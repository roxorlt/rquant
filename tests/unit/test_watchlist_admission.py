from __future__ import annotations

import http.client
import json
import os
import socket
import sqlite3
import threading
from datetime import UTC, datetime
from decimal import Decimal
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rquant.alert_ack_admission import AckAdmission, build_ack_admission_server
from rquant.manual_watchlist import ManualWatchlistRepository, ManualWatchlistUpsert
from rquant.page_control import (
    AckAlert,
    AddWatchlistItem,
    PageControlConsumer,
    PageControlOutbox,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
    RemoveWatchlistItem,
)
from rquant.page_control_service import handler_for
from rquant.watchlist_admission import (
    WatchlistAdmission,
    WatchlistAdmissionClient,
    WatchlistAdmissionRejectedError,
    WatchlistAdmissionUnavailableError,
    build_watchlist_admission_server,
)

NOW = datetime(2026, 9, 28, 4, 0, tzinfo=UTC)
SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"


def _service(tmp_path: Path, *, activated: bool = True) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    if activated:
        outbox.activate_manual_watchlist(NOW)
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: NOW,
            consumer_id="admission-test",
        ),
    )


def _add(
    command_id: str = "watch-1",
    *,
    owner_id: str = "alice",
    code: str = "600000.SH",
    expected_version: int | None = None,
    price: Decimal = Decimal("10.00"),
) -> AddWatchlistItem:
    return AddWatchlistItem(
        command_id=command_id,
        requested_at=NOW,
        item=ManualWatchlistUpsert(
            owner_id=owner_id,
            ts_code=code,
            expected_version=expected_version,
            source="detail",
            price_levels=(price,),
        ),
    )


def _serve(server: object) -> threading.Thread:
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    return worker


def _stop(server: object, worker: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    worker.join(timeout=2)
    assert not worker.is_alive()


def _count(path: Path) -> int:
    with sqlite3.connect(path) as connection:
        return connection.execute("SELECT COUNT(*) FROM manual_watchlist").fetchone()[0]


def test_private_socket_is_opt_in_and_rejects_unsafe_paths(tmp_path: Path) -> None:
    admission = WatchlistAdmission(_service(tmp_path))
    assert build_watchlist_admission_server(admission, socket_path=None) is None
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        private = Path(directory)
        path = private / "watch.sock"
        os.chmod(private, 0o750)
        with pytest.raises(ValueError, match="0700"):
            build_watchlist_admission_server(admission, socket_path=path)
        os.chmod(private, 0o700)
        path.write_text("unsafe")
        with pytest.raises(ValueError, match="exists|unsafe"):
            build_watchlist_admission_server(admission, socket_path=path)
        path.unlink()
        path.symlink_to(private / "missing")
        with pytest.raises(ValueError, match="exists|unsafe"):
            build_watchlist_admission_server(admission, socket_path=path)
        path.unlink()
        server = build_watchlist_admission_server(admission, socket_path=path)
        assert server is not None
        try:
            assert path.stat().st_mode & 0o777 == 0o600
            assert private.stat().st_mode & 0o777 == 0o700
        finally:
            server.server_close()
        assert not path.exists()


def test_watchlist_and_ack_sockets_can_be_managed_independently(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        private = Path(directory)
        watch_path = private / "watch.sock"
        ack_path = private / "ack.sock"
        watch = build_watchlist_admission_server(
            WatchlistAdmission(service), socket_path=watch_path
        )
        ack = build_ack_admission_server(
            AckAdmission(service, tmp_path / "serving"), socket_path=ack_path
        )
        assert watch is not None and ack is not None
        assert watch_path.exists() and ack_path.exists()
        watch.server_close()
        assert not watch_path.exists() and ack_path.exists()
        ack.server_close()
        assert not ack_path.exists()


def test_private_client_exact_retry_conflict_owner_and_tcp_rejection(tmp_path: Path) -> None:
    service = _service(tmp_path)
    admission = WatchlistAdmission(service)
    command = _add()
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(admission, socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            client = WatchlistAdmissionClient(path)
            first = client.submit(command, authenticated_owner_id="alice")
            assert first.status is PageControlStatus.SUCCEEDED
            assert first.result == {
                "ts_code": "600000.SH",
                "action": "add",
                "version": 1,
                "state": "active",
            }
            assert client.submit(command, authenticated_owner_id="alice") == first
            with pytest.raises(WatchlistAdmissionRejectedError, match="command_conflict"):
                client.submit(_add(price=Decimal("11.00")), authenticated_owner_id="alice")
            with pytest.raises(WatchlistAdmissionRejectedError, match="owner_mismatch"):
                client.submit(_add("cross-user"), authenticated_owner_id="bob")
            assert service.outbox.receipt("cross-user") is None
            removed = client.submit(
                RemoveWatchlistItem(
                    command_id="remove-watch",
                    requested_at=NOW,
                    item={
                        "owner_id": "alice",
                        "ts_code": "600000.SH",
                        "expected_version": 1,
                    },
                ),
                authenticated_owner_id="alice",
            )
            assert removed.status is PageControlStatus.SUCCEEDED
            assert removed.result["version"] == 2
            with pytest.raises(ValueError, match="trusted"):
                service.submit(_add("legacy-tcp"))
            assert service.outbox.receipt("legacy-tcp") is None
            assert _count(service.outbox.path) == 1
        finally:
            _stop(server, worker)


def test_wrong_peer_uid_is_rejected_before_json_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _service(tmp_path)
    checks: list[str] = []

    def wrong_uid(_connection: socket.socket) -> int:
        checks.append("peer")
        return os.geteuid() + 1

    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(
            WatchlistAdmission(service), socket_path=path, peer_uid=wrong_uid
        )
        assert server is not None
        worker = _serve(server)
        try:
            import rquant.watchlist_admission as module

            def parsed(_body: bytes) -> object:
                raise AssertionError("untrusted request was parsed")

            monkeypatch.setattr(module, "strict_json_loads", parsed)
            with pytest.raises(WatchlistAdmissionUnavailableError):
                WatchlistAdmissionClient(path).submit(_add(), authenticated_owner_id="alice")
            assert checks == ["peer"]
            assert _count(service.outbox.path) == 0
        finally:
            _stop(server, worker)


def test_client_rejects_unsafe_socket_permissions_and_wrong_server_uid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.watchlist_admission as module

    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        private = Path(directory)
        path = private / "watch.sock"
        server = build_watchlist_admission_server(WatchlistAdmission(service), socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            client = WatchlistAdmissionClient(path)
            os.chmod(path, 0o660)
            with pytest.raises(WatchlistAdmissionUnavailableError):
                client.submit(_add("bad-socket-mode"), authenticated_owner_id="alice")
            os.chmod(path, 0o600)
            os.chmod(private, 0o750)
            with pytest.raises(WatchlistAdmissionUnavailableError):
                client.submit(_add("bad-parent-mode"), authenticated_owner_id="alice")
            os.chmod(private, 0o700)
            monkeypatch.setattr(module, "_peer_uid", lambda _connection: os.geteuid() + 1)
            with pytest.raises(WatchlistAdmissionUnavailableError):
                client.submit(_add("wrong-server-uid"), authenticated_owner_id="alice")
            assert service.outbox.receipt("bad-socket-mode") is None
            assert service.outbox.receipt("bad-parent-mode") is None
            assert service.outbox.receipt("wrong-server-uid") is None
        finally:
            os.chmod(private, 0o700)
            _stop(server, worker)


def test_response_loss_recovers_same_durable_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _service(tmp_path)
    original = service._submit_trusted_watchlist
    calls = 0

    def commit_then_lose(
        command: AddWatchlistItem | RemoveWatchlistItem, *, authenticated_owner_id: str
    ):
        nonlocal calls
        receipt = original(command, authenticated_owner_id=authenticated_owner_id)
        calls += 1
        if calls == 1:
            raise ConnectionResetError("response lost after commit")
        return receipt

    monkeypatch.setattr(service, "_submit_trusted_watchlist", commit_then_lose)
    command = _add("response-loss")
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(WatchlistAdmission(service), socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            client = WatchlistAdmissionClient(path)
            with pytest.raises(WatchlistAdmissionUnavailableError):
                client.submit(command, authenticated_owner_id="alice")
            assert _count(service.outbox.path) == 1
            persisted = client.lookup(command, authenticated_owner_id="alice")
            assert persisted is not None
            assert persisted.status is PageControlStatus.SUCCEEDED
            recovered = client.resume(command, authenticated_owner_id="alice")
            assert recovered.status is PageControlStatus.SUCCEEDED
            assert recovered.result["version"] == 1
            assert _count(service.outbox.path) == 1
        finally:
            _stop(server, worker)


def test_cas_capacity_and_disabled_state_reuse_pagecontrol_receipts(tmp_path: Path) -> None:
    service = _service(tmp_path)
    admission = WatchlistAdmission(service)
    assert (
        admission.admit(_add(), authenticated_owner_id="alice").status
        is PageControlStatus.SUCCEEDED
    )
    stale = admission.admit(_add("stale"), authenticated_owner_id="alice")
    assert stale.status is PageControlStatus.FAILED
    assert stale.result["code"] == "version_conflict"
    with sqlite3.connect(service.outbox.path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        store = ManualWatchlistRepository(connection)
        for number in range(499):
            store.upsert(
                ManualWatchlistUpsert(
                    owner_id="alice", ts_code=f"{number:06d}.SH", source="detail"
                ),
                now=NOW,
            )
    full = admission.admit(_add("full", code="600001.SH"), authenticated_owner_id="alice")
    assert full.status is PageControlStatus.FAILED
    assert full.result["code"] == "capacity_exceeded"

    disabled = _service(tmp_path / "disabled", activated=False)
    with pytest.raises(ValueError, match="activated"):
        WatchlistAdmission(disabled).admit(_add("disabled"), authenticated_owner_id="alice")
    assert disabled.outbox.receipt("disabled") is None


def test_private_route_rejects_other_command_kind(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(WatchlistAdmission(service), socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            connection = http.client.HTTPConnection("localhost", timeout=1)
            connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.sock.connect(str(path))
            command = AckAlert(
                command_id="not-watchlist",
                requested_at=NOW,
                generation_id="a" * 64,
                alert_id="b" * 64,
                actor_id="alice",
            )
            connection.request(
                "POST",
                "/v1/watchlist-admission",
                body=json.dumps(
                    {"authenticated_owner_id": "alice", "command": command.model_dump(mode="json")}
                ),
                headers={"Content-Type": "application/json"},
            )
            assert connection.getresponse().status == 400
            connection.close()
            assert service.outbox.receipt("not-watchlist") is None
        finally:
            _stop(server, worker)


def test_client_rejects_response_for_another_command(tmp_path: Path) -> None:
    service = _service(tmp_path)
    admission = WatchlistAdmission(service)
    command = _add("expected-id")

    def wrong_receipt(
        _command: AddWatchlistItem | RemoveWatchlistItem, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        assert authenticated_owner_id == "alice"
        return PageControlReceipt(
            command_id="other-id",
            status=PageControlStatus.SUCCEEDED,
            enqueued_at=NOW,
            completed_at=NOW,
            result={"ts_code": "600000.SH", "action": "add", "version": 1, "state": "active"},
        )

    admission.admit = wrong_receipt  # type: ignore[method-assign]
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(admission, socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            with pytest.raises(WatchlistAdmissionUnavailableError, match="another command"):
                WatchlistAdmissionClient(path).submit(command, authenticated_owner_id="alice")
        finally:
            _stop(server, worker)


def test_client_bounds_response_even_from_private_peer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.watchlist_admission as module

    class OversizedResponse:
        status = 200

        def read(self, size: int) -> bytes:
            assert size == 16 * 1024 + 1
            return b"x" * size

    class Connection:
        def request(self, *_args: object, **_kwargs: object) -> None:
            return

        def getresponse(self) -> OversizedResponse:
            return OversizedResponse()

        def close(self) -> None:
            return

    monkeypatch.setattr(module, "_UnixHTTPConnection", lambda *_args, **_kwargs: Connection())
    with pytest.raises(WatchlistAdmissionUnavailableError, match="exceeds bound"):
        WatchlistAdmissionClient(tmp_path / "private.sock").submit(
            _add("response-bound"), authenticated_owner_id="alice"
        )


def test_exact_lookup_is_read_only_and_rejects_changed_or_cross_owner_command(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    command = _add("lookup-original")
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(WatchlistAdmission(service), socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            client = WatchlistAdmissionClient(path)
            assert client.lookup(command, authenticated_owner_id="alice") is None
            assert service.outbox.receipt(command.command_id) is None
            with pytest.raises(WatchlistAdmissionRejectedError, match="not_found"):
                client.resume(command, authenticated_owner_id="alice")
            assert service.outbox.receipt(command.command_id) is None

            first = client.submit(command, authenticated_owner_id="alice")
            assert client.lookup(command, authenticated_owner_id="alice") == first
            removed = client.submit(
                RemoveWatchlistItem(
                    command_id="newer-removal",
                    requested_at=NOW,
                    item={
                        "owner_id": "alice",
                        "ts_code": "600000.SH",
                        "expected_version": 1,
                    },
                ),
                authenticated_owner_id="alice",
            )
            assert removed.result["version"] == 2
            assert client.lookup(command, authenticated_owner_id="alice") == first
            assert client.resume(command, authenticated_owner_id="alice") == first
            with pytest.raises(WatchlistAdmissionRejectedError, match="command_conflict"):
                client.lookup(
                    _add("lookup-original", price=Decimal("11.00")), authenticated_owner_id="alice"
                )
            with pytest.raises(WatchlistAdmissionRejectedError, match="owner_mismatch"):
                client.lookup(command, authenticated_owner_id="bob")
            assert service.outbox.receipt(command.command_id) == first
            assert service.outbox.receipt("newer-removal") == removed
        finally:
            _stop(server, worker)


def test_resume_settles_only_a_persisted_pending_command(tmp_path: Path) -> None:
    service = _service(tmp_path)
    command = _add("pending-original")
    pending = service.outbox.enqueue_trusted_watchlist(command)
    assert pending.status is PageControlStatus.PENDING
    assert _count(service.outbox.path) == 0
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        path = Path(directory) / "watch.sock"
        server = build_watchlist_admission_server(WatchlistAdmission(service), socket_path=path)
        assert server is not None
        worker = _serve(server)
        try:
            client = WatchlistAdmissionClient(path)
            assert client.lookup(command, authenticated_owner_id="alice") == pending
            assert service.outbox.receipt(command.command_id) == pending
            assert _count(service.outbox.path) == 0
            settled = client.resume(command, authenticated_owner_id="alice")
            assert settled.status is PageControlStatus.SUCCEEDED
            assert settled.result["version"] == 1
            assert client.resume(command, authenticated_owner_id="alice") == settled
            assert _count(service.outbox.path) == 1
        finally:
            _stop(server, worker)


def test_loopback_tcp_still_rejects_watchlist_commands(tmp_path: Path) -> None:
    service = _service(tmp_path)
    tcp = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(service))
    worker = _serve(tcp)
    try:
        command = _add("tcp-rejected")
        connection = http.client.HTTPConnection("127.0.0.1", tcp.server_port, timeout=1)
        connection.request(
            "POST",
            "/v1/commands",
            body=command.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        assert connection.getresponse().status == 400
        connection.close()
        assert service.outbox.receipt(command.command_id) is None
    finally:
        _stop(tcp, worker)
