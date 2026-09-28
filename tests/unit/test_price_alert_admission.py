from __future__ import annotations

import http.client
import json
import os
import socket
import sqlite3
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import rquant.page_control_service as page_control_service
from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import ManualWatchlistRepository, ManualWatchlistUpsert
from rquant.page_control import (
    DeletePriceAlertRule,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SavePriceAlertRule,
    SetPriceAlertRuleEnabled,
)
from rquant.page_control_service import build_parser, handler_for
from rquant.price_alert_admission import (
    PriceAlertAdmission,
    PriceAlertAdmissionClient,
    PriceAlertAdmissionRejectedError,
    PriceAlertAdmissionUnavailableError,
    build_price_alert_admission_server,
)
from rquant.price_alert_rule_store import PriceAlertRuleKey, PriceAlertRuleRepository

NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)
CODE = "600001.SH"
SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"


def _service(tmp_path: Path, *, activated: bool = True) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_manual_watchlist(NOW)
    if activated:
        outbox.activate_price_alert_rules(NOW)
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: NOW,
            consumer_id="price-private-test",
        ),
    )


def _member(path: Path, owner: str, *, expires_at: datetime | None = None) -> None:
    with sqlite3.connect(path, isolation_level=None) as connection:
        connection.execute("BEGIN IMMEDIATE")
        ManualWatchlistRepository(connection).upsert(
            ManualWatchlistUpsert(
                owner_id=owner, ts_code=CODE, source="detail", expires_at=expires_at
            ),
            now=NOW,
        )
        connection.commit()


def _rule(rule_id: str = "threshold-a") -> PriceAlertRule:
    return PriceAlertRule(
        rule_id=rule_id,
        name="到价提醒",
        priority="P2",
        enabled=True,
        comparison="gte",
        threshold=Decimal("10.00"),
        valid_from=time(9, 30),
        valid_until=time(14, 57),
    )


def _save(
    command_id: str = "price-1", *, rule_id: str = "threshold-a", membership_version: int = 1
) -> SavePriceAlertRule:
    return SavePriceAlertRule(
        command_id=command_id,
        requested_at=NOW,
        ts_code=CODE,
        membership_version=membership_version,
        rule=_rule(rule_id),
    )


def _entry(path: Path, owner: str) -> object:
    with sqlite3.connect(path) as connection:
        return PriceAlertRuleRepository(connection).get(
            PriceAlertRuleKey(owner_id=owner, rule_id="threshold-a")
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


def _simulated_client(path: Path) -> PriceAlertAdmissionClient:
    # The two UIDs are simulated on this host; deployment still needs a Linux dual-UID test.
    return PriceAlertAdmissionClient(
        path,
        expected_service_uid=os.geteuid(),
        shared_gid=os.getegid(),
        client_uid=lambda: os.geteuid() + 1,
    )


def _simulated_server(path: Path, service: PageControlService, *, peer_uid=None) -> object:
    return build_price_alert_admission_server(
        PriceAlertAdmission(service),
        socket_path=path,
        trusted_web_uid=os.geteuid() + 1,
        shared_gid=os.getegid(),
        peer_uid=peer_uid or (lambda _connection: os.geteuid() + 1),
    )


def test_private_listener_requires_explicit_distinct_uid_and_gid(tmp_path: Path) -> None:
    admission = PriceAlertAdmission(_service(tmp_path))
    assert build_price_alert_admission_server(admission, socket_path=None) is None
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        for options in (
            {"trusted_web_uid": os.geteuid() + 1},
            {"shared_gid": os.getegid()},
            {"trusted_web_uid": os.geteuid(), "shared_gid": os.getegid()},
        ):
            with pytest.raises(ValueError):
                build_price_alert_admission_server(admission, socket_path=path, **options)
            assert not path.exists()
        server = _simulated_server(path, admission.service)
        assert server is not None
        try:
            assert path.parent.stat().st_mode & 0o777 == 0o710
            assert path.stat().st_mode & 0o777 == 0o660
            assert path.parent.stat().st_gid == os.getegid()
            assert path.stat().st_gid == os.getegid()
        finally:
            server.server_close()
        assert not path.exists()


def test_simulated_distinct_uid_exact_owner_retry_and_tcp_rejection(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _member(service.outbox.path, "alice")
    _member(service.outbox.path, "bob")
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            client = _simulated_client(path)
            command = _save()
            assert client.lookup(command, authenticated_owner_id="alice") is None
            assert service.outbox.receipt(command.command_id) is None
            receipt = client.submit(command, authenticated_owner_id="alice")
            assert receipt.status is PageControlStatus.SUCCEEDED
            assert receipt.result["action"] == "save"
            assert client.lookup(command, authenticated_owner_id="alice") == receipt
            assert client.resume(command, authenticated_owner_id="alice") == receipt
            assert client.submit(command, authenticated_owner_id="alice") == receipt
            assert _entry(service.outbox.path, "alice").version == 1
            assert _entry(service.outbox.path, "bob") is None
            for owner, other in (("bob", command), ("alice", _save(membership_version=2))):
                for operation in (client.submit, client.lookup, client.resume):
                    with pytest.raises(PriceAlertAdmissionRejectedError, match="command_conflict"):
                        operation(other, authenticated_owner_id=owner)
            with pytest.raises(PriceAlertAdmissionRejectedError, match="not_found"):
                client.resume(_save("missing"), authenticated_owner_id="alice")
            assert service.outbox.receipt("missing") is None
            bob = client.submit(_save("bob-own"), authenticated_owner_id="bob")
            assert bob.status is PageControlStatus.SUCCEEDED
            assert _entry(service.outbox.path, "bob").version == 1
            with pytest.raises(ValueError, match="price rule|trusted"):
                service.submit(_save("old-tcp"))
            assert service.outbox.receipt("old-tcp") is None
        finally:
            _stop(server, worker)


def test_real_same_uid_peer_is_rejected_before_body_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.price_alert_admission as module

    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = build_price_alert_admission_server(
            PriceAlertAdmission(service),
            socket_path=path,
            trusted_web_uid=os.geteuid() + 1,
            shared_gid=os.getegid(),
        )
        assert server is not None
        worker = _serve(server)
        try:
            monkeypatch.setattr(
                module,
                "strict_json_loads",
                lambda *_args, **_kwargs: pytest.fail("untrusted body was parsed"),
            )
            payload = json.dumps(
                {"authenticated_owner_id": "bob", "command": _save().model_dump(mode="json")}
            ).encode()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.connect(str(path))
                with suppress(BrokenPipeError):
                    connection.sendall(
                        b"POST /v1/price-alert-admission HTTP/1.1\r\n"
                        + f"Content-Length: {len(payload)}\r\n".encode()
                        + b"Content-Type: application/json\r\n\r\n"
                        + payload
                    )
                try:
                    response = connection.recv(1)
                except ConnectionResetError:
                    # Linux may reset a rejected peer with unread request bytes.
                    response = b""
                assert response == b""
            assert service.outbox.receipt("price-1") is None
        finally:
            _stop(server, worker)


def test_same_uid_client_refuses_endpoint_before_transmission(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            with pytest.raises(ValueError, match="distinct|different"):
                PriceAlertAdmissionClient(
                    path, expected_service_uid=os.geteuid(), shared_gid=os.getegid()
                )
            assert service.outbox.receipt("price-1") is None
        finally:
            _stop(server, worker)


def test_opt_in_cli_has_no_price_listener_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = build_parser()
    base = [
        "--manifest",
        "/private/tmp/manifest.json",
        "--control-root",
        "/private/tmp/control",
        "--expected-commit",
        "a" * 40,
        "--expected-generation",
        "b" * 64,
    ]
    assert parser.parse_args(base).price_rule_socket is None
    parsed = parser.parse_args(
        [
            *base,
            "--price-rule-socket",
            "/private/tmp/price.sock",
            "--price-rule-web-uid",
            "12345",
            "--price-rule-shared-gid",
            "12346",
        ]
    )
    assert parsed.price_rule_socket == Path("/private/tmp/price.sock")
    assert parsed.price_rule_web_uid == 12345
    assert parsed.price_rule_shared_gid == 12346
    passed: list[dict[str, object]] = []
    monkeypatch.setattr(page_control_service, "_serve", lambda **kwargs: passed.append(kwargs))
    page_control_service.main(base)
    page_control_service.main(
        [
            *base,
            "--price-rule-socket",
            "/private/tmp/price.sock",
            "--price-rule-web-uid",
            "12345",
            "--price-rule-shared-gid",
            "12346",
        ]
    )
    assert passed[0]["price_rule_socket_path"] is None
    assert passed[0]["price_rule_web_uid"] is None
    assert passed[0]["price_rule_shared_gid"] is None
    assert passed[1]["price_rule_socket_path"] == Path("/private/tmp/price.sock")
    assert passed[1]["price_rule_web_uid"] == 12345
    assert passed[1]["price_rule_shared_gid"] == 12346


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        (b'{"authenticated_owner_id":"alice","authenticated_owner_id":"bob","command":{}}', None),
        (None, {"Content-Type": "text/plain"}),
        (None, {"Transfer-Encoding": "chunked"}),
        (None, {"Content-Length": "9000"}),
        (None, {"Content-Length": "0"}),
        (None, {"Content-Length": "-1"}),
        (b"{", {"Content-Length": "2"}),
        (b'{"authenticated_owner_id":"alice","command":{},"extra":1}', None),
    ],
)
def test_raw_socket_rejects_invalid_framing_before_enqueue(
    tmp_path: Path, body: bytes | None, headers: dict[str, str] | None
) -> None:
    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            payload = (
                body
                or json.dumps(
                    {
                        "authenticated_owner_id": "alice",
                        "command": _save().model_dump(mode="json"),
                    }
                ).encode()
            )
            fields = {"Content-Type": "application/json", "Content-Length": str(len(payload))}
            fields.update(headers or {})
            raw = (
                "POST /v1/price-alert-admission HTTP/1.1\r\nHost: localhost\r\n"
                + "".join(f"{key}: {value}\r\n" for key, value in fields.items())
                + "\r\n"
            ).encode() + payload
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(3)
                connection.connect(str(path))
                connection.sendall(raw)
                connection.shutdown(socket.SHUT_WR)
                response = connection.recv(1024)
            assert response.startswith(b"HTTP/1.0 400")
            assert service.outbox.receipt("price-1") is None
        finally:
            _stop(server, worker)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: {**body, "owner_id": "bob"},
        lambda body: {**body, "kind": "add_watchlist_item"},
        lambda body: {**body, "extra": 1},
    ],
)
def test_raw_socket_rejects_owner_wrong_kind_and_extra_fields(
    tmp_path: Path, mutate: Callable[[dict[str, object]], dict[str, object]]
) -> None:
    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            payload = json.dumps(
                {
                    "authenticated_owner_id": "alice",
                    "command": mutate(_save().model_dump(mode="json")),
                }
            ).encode()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(3)
                connection.connect(str(path))
                connection.sendall(
                    b"POST /v1/price-alert-admission HTTP/1.1\r\n"
                    + f"Content-Length: {len(payload)}\r\n".encode()
                    + b"Content-Type: application/json\r\n\r\n"
                    + payload
                )
                assert connection.recv(1024).startswith(b"HTTP/1.0 400")
            assert service.outbox.receipt("price-1") is None
        finally:
            _stop(server, worker)


def test_duplicate_content_length_is_rejected(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            payload = json.dumps(
                {"authenticated_owner_id": "alice", "command": _save().model_dump(mode="json")}
            ).encode()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(3)
                connection.connect(str(path))
                connection.sendall(
                    b"POST /v1/price-alert-admission HTTP/1.1\r\n"
                    + f"Content-Length: {len(payload)}\r\n".encode() * 2
                    + b"Content-Type: application/json\r\n\r\n"
                    + payload
                )
                assert connection.recv(1024).startswith(b"HTTP/1.0 400")
            assert service.outbox.receipt("price-1") is None
        finally:
            _stop(server, worker)


def test_endpoint_path_and_peer_validation_prevents_untrusted_transmission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.price_alert_admission as module

    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        path.parent.mkdir(mode=0o700)
        with pytest.raises(ValueError, match="0710"):
            _simulated_server(path, service)
        path.parent.rmdir()
        path.parent.symlink_to(Path(root))
        with pytest.raises(ValueError, match="symlink"):
            _simulated_server(path, service)
        path.parent.unlink()
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            client = _simulated_client(path)
            os.chmod(path, 0o666)
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                client.submit(_save("bad-socket"), authenticated_owner_id="alice")
            os.chmod(path, 0o660)
            os.chmod(path.parent, 0o700)
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                client.submit(_save("bad-parent"), authenticated_owner_id="alice")
            os.chmod(path.parent, 0o710)
            monkeypatch.setattr(module, "_peer_uid", lambda _connection: os.geteuid() + 2)
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                client.submit(_save("bad-peer"), authenticated_owner_id="alice")
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                PriceAlertAdmissionClient(
                    path,
                    expected_service_uid=os.geteuid(),
                    shared_gid=os.getegid() + 1,
                    client_uid=lambda: os.geteuid() + 1,
                ).submit(_save("bad-gid"), authenticated_owner_id="alice")
            for command_id in ("bad-socket", "bad-parent", "bad-peer", "bad-gid"):
                assert service.outbox.receipt(command_id) is None
            moved = path.with_suffix(".old")
            path.rename(moved)
            path.write_text("replacement")
        finally:
            _stop(server, worker)
        assert path.read_text() == "replacement"
        moved.unlink()
        with pytest.raises(ValueError, match="exists"):
            _simulated_server(path, service)
        assert path.read_text() == "replacement"


def test_path_change_during_connection_rejects_before_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.price_alert_admission as module

    service = _service(tmp_path)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            original = module._assert_endpoint
            checks = 0

            def changed(path: Path, uid: int, gid: int) -> tuple[int, int, int, int]:
                nonlocal checks
                checks += 1
                identity = original(path, uid, gid)
                return identity if checks == 1 else (*identity[:3], identity[3] + 1)

            monkeypatch.setattr(module, "_assert_endpoint", changed)
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                _simulated_client(path).submit(
                    _save("replaced-during-connect"), authenticated_owner_id="alice"
                )
            assert checks == 2
            assert service.outbox.receipt("replaced-during-connect") is None
        finally:
            _stop(server, worker)


def test_set_enabled_and_delete_routes_check_effect_identity(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _member(service.outbox.path, "alice")
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            client = _simulated_client(path)
            saved = client.submit(_save(), authenticated_owner_id="alice")
            assert saved.result["version"] == 1
            disabled = client.submit(
                SetPriceAlertRuleEnabled(
                    command_id="disable",
                    requested_at=NOW,
                    rule_id="threshold-a",
                    expected_version=1,
                    enabled=False,
                ),
                authenticated_owner_id="alice",
            )
            assert disabled.status is PageControlStatus.SUCCEEDED
            assert disabled.result == {
                "rule_id": "threshold-a",
                "action": "set_enabled",
                "version": 2,
                "deleted": False,
                "enabled": False,
            }
            deleted = client.submit(
                DeletePriceAlertRule(
                    command_id="delete",
                    requested_at=NOW,
                    rule_id="threshold-a",
                    expected_version=2,
                ),
                authenticated_owner_id="alice",
            )
            assert deleted.status is PageControlStatus.SUCCEEDED
            assert deleted.result["action"] == "delete"
            assert deleted.result["version"] == 3
            assert deleted.result["deleted"] is True
            assert client.lookup(_save(), authenticated_owner_id="alice") == saved
        finally:
            _stop(server, worker)


def test_pending_resume_lookup_is_read_only_and_response_loss_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _service(tmp_path)
    _member(service.outbox.path, "alice")
    command = _save("pending-resume")
    original_drain = service.consumer.drain_price_rule_command
    monkeypatch.setattr(
        service.consumer,
        "drain_price_rule_command",
        lambda _owned: (_ for _ in ()).throw(ConnectionResetError("after enqueue")),
    )
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            client = _simulated_client(path)
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                client.submit(command, authenticated_owner_id="alice")
            assert service.outbox.receipt(command.command_id).status is PageControlStatus.PENDING
            assert client.lookup(command, authenticated_owner_id="alice").status is (
                PageControlStatus.PENDING
            )
            assert _entry(service.outbox.path, "alice") is None
            monkeypatch.setattr(service.consumer, "drain_price_rule_command", original_drain)
            finished = client.resume(command, authenticated_owner_id="alice")
            assert finished.status is PageControlStatus.SUCCEEDED
            assert client.submit(command, authenticated_owner_id="alice") == finished
            with sqlite3.connect(service.outbox.path) as connection:
                assert (
                    connection.execute(
                        "SELECT COUNT(*) FROM page_control_effect WHERE command_id = ?",
                        (command.command_id,),
                    ).fetchone()[0]
                    == 1
                )
        finally:
            _stop(server, worker)


def test_marker_cas_scope_and_concurrent_same_command(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _member(service.outbox.path, "alice")
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            client = _simulated_client(path)
            with sqlite3.connect(service.outbox.path) as connection:
                connection.execute(
                    "DELETE FROM page_control_protocol_activation WHERE marker_name = ?",
                    ("price-alert-rule/v1",),
                )
            with pytest.raises(PriceAlertAdmissionRejectedError, match="rejected"):
                client.submit(_save("unactivated"), authenticated_owner_id="alice")
            assert service.outbox.receipt("unactivated") is None
            service.outbox.activate_price_alert_rules(NOW)
            with ThreadPoolExecutor(max_workers=6) as pool:
                receipts = list(
                    pool.map(
                        lambda _index: _simulated_client(path).submit(
                            _save("raced"), authenticated_owner_id="alice"
                        ),
                        range(6),
                    )
                )
            assert len(set(receipt.model_dump_json() for receipt in receipts)) == 1
            assert receipts[0].status is PageControlStatus.SUCCEEDED
            failed_cas = client.submit(
                SetPriceAlertRuleEnabled(
                    command_id="wrong-cas",
                    requested_at=NOW,
                    rule_id="threshold-a",
                    expected_version=99,
                    enabled=False,
                ),
                authenticated_owner_id="alice",
            )
            assert failed_cas.status is PageControlStatus.FAILED
            assert failed_cas.result["code"] == "version_conflict"
            invalid_scope = client.submit(
                _save("bad-scope", rule_id="threshold-b", membership_version=2),
                authenticated_owner_id="alice",
            )
            assert invalid_scope.status is PageControlStatus.FAILED
            assert invalid_scope.result["code"] == "scope_invalid"
            assert _entry(service.outbox.path, "alice").version == 1
        finally:
            _stop(server, worker)


@pytest.mark.parametrize("corruption", ["not_activated", "bad_marker", "bad_schema"])
def test_protocol_absence_or_corruption_cannot_enqueue(tmp_path: Path, corruption: str) -> None:
    service = _service(tmp_path, activated=corruption != "not_activated")
    _member(service.outbox.path, "alice")
    if corruption == "bad_marker":
        with sqlite3.connect(service.outbox.path) as connection:
            connection.execute(
                "UPDATE page_control_protocol_activation SET activated_at = 'malformed' "
                "WHERE marker_name = 'price-alert-rule/v1'"
            )
    if corruption == "bad_schema":
        with sqlite3.connect(service.outbox.path) as connection:
            connection.execute("DROP TABLE price_alert_rule")
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            expected_error = (
                PriceAlertAdmissionRejectedError
                if corruption in {"not_activated", "bad_marker"}
                else PriceAlertAdmissionUnavailableError
            )
            with pytest.raises(expected_error):
                _simulated_client(path).submit(_save(), authenticated_owner_id="alice")
            assert service.outbox.receipt("price-1") is None
        finally:
            _stop(server, worker)


def test_receipt_loss_after_committed_effect_recovers_with_same_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _service(tmp_path)
    _member(service.outbox.path, "alice")
    original = service._submit_trusted_price_rule
    calls = 0

    def commit_then_lose(command: SavePriceAlertRule, *, authenticated_owner_id: str):
        nonlocal calls
        receipt = original(command, authenticated_owner_id=authenticated_owner_id)
        calls += 1
        if calls == 1:
            raise ConnectionResetError("lost after commit")
        return receipt

    monkeypatch.setattr(service, "_submit_trusted_price_rule", commit_then_lose)
    with TemporaryDirectory(prefix="rqp-", dir=SHORT_TMP) as root:
        path = Path(root) / "private" / "price.sock"
        server = _simulated_server(path, service)
        assert server is not None
        worker = _serve(server)
        try:
            client = _simulated_client(path)
            command = _save("lost-effect")
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                client.submit(command, authenticated_owner_id="alice")
            recovered = client.lookup(command, authenticated_owner_id="alice")
            assert recovered.status is PageControlStatus.SUCCEEDED
            assert client.resume(command, authenticated_owner_id="alice") == recovered
            assert client.submit(command, authenticated_owner_id="alice") == recovered
            with sqlite3.connect(service.outbox.path) as connection:
                assert (
                    connection.execute(
                        "SELECT COUNT(*) FROM page_control_effect WHERE command_id = ?",
                        (command.command_id,),
                    ).fetchone()[0]
                    == 1
                )
        finally:
            _stop(server, worker)


@pytest.mark.parametrize("corruption", ["command_id", "requested_at", "result", "action"])
def test_client_rejects_mismatched_success_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    import rquant.price_alert_admission as module

    service = _service(tmp_path)
    _member(service.outbox.path, "alice")
    command = _save("forged-response")
    receipt = service._submit_trusted_price_rule(command, authenticated_owner_id="alice")
    payload = receipt.model_dump(mode="json")
    if corruption == "command_id":
        payload["command_id"] = "another-command"
    elif corruption == "requested_at":
        payload["enqueued_at"] = "2026-09-29T02:00:01Z"
    elif corruption == "result":
        payload["result"] = {**payload["result"], "rule_id": "another-rule"}
    else:
        payload["result"] = {**payload["result"], "action": "delete"}
    encoded = json.dumps(payload).encode()

    class FakeResponse:
        status = 200

        def getheaders(self) -> list[tuple[str, str]]:
            return [("Content-Length", str(len(encoded))), ("Content-Type", "application/json")]

        def getheader(self, name: str) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return encoded

    class FakeConnection:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def request(self, *_args: object, **_kwargs: object) -> None:
            pass

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(module, "_UnixHTTPConnection", FakeConnection)
    with pytest.raises(PriceAlertAdmissionUnavailableError):
        _simulated_client(Path("/private/tmp/nonexistent.sock")).submit(
            command, authenticated_owner_id="alice"
        )


@pytest.mark.parametrize(
    "corruption", ["duplicate_length", "oversize", "wrong_type", "truncated", "duplicate_key"]
)
def test_client_rejects_invalid_response_framing_and_json(
    monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    import rquant.price_alert_admission as module

    body = b'{"found":false,"found":false}' if corruption == "duplicate_key" else b'{"found":false}'
    length = len(body) + 1 if corruption == "truncated" else len(body)
    if corruption == "oversize":
        length = 16 * 1024 + 1
    headers = [("Content-Length", str(length)), ("Content-Type", "application/json")]
    if corruption == "duplicate_length":
        headers.append(("Content-Length", str(length)))
    if corruption == "wrong_type":
        headers[1] = ("Content-Type", "text/plain")

    class FakeResponse:
        status = 200

        def getheaders(self) -> list[tuple[str, str]]:
            return headers

        def getheader(self, _name: str) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return body

    class FakeConnection:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def request(self, *_args: object, **_kwargs: object) -> None:
            pass

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(module, "_UnixHTTPConnection", FakeConnection)
    with pytest.raises(PriceAlertAdmissionUnavailableError):
        _simulated_client(Path("/private/tmp/nonexistent.sock")).lookup(
            _save(), authenticated_owner_id="alice"
        )


def test_direct_tcp_command_stays_closed(tmp_path: Path) -> None:
    from http.server import ThreadingHTTPServer

    service = _service(tmp_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(service))
    worker = _serve(server)
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        connection.request(
            "POST",
            "/v1/commands",
            body=_save("tcp-price").model_dump_json().encode(),
            headers={"Content-Type": "application/json"},
        )
        assert connection.getresponse().status == 400
        connection.close()
        assert service.outbox.receipt("tcp-price") is None
    finally:
        _stop(server, worker)
