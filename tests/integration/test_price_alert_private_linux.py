"""Exact Linux kernel-credential gate for the private price-rule Unix socket."""

from __future__ import annotations

import grp
import importlib
import json
import os
import pwd
import select
import shutil
import signal
import socket
import sqlite3
import stat
import sys
import tempfile
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from contextlib import closing, suppress
from ctypes import c_uint
from datetime import UTC, datetime
from datetime import time as clock_time
from decimal import Decimal
from multiprocessing.sharedctypes import RawValue
from pathlib import Path

import pytest

import rquant.price_alert_admission as admission_module
from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import ManualWatchlistRepository, ManualWatchlistUpsert
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SavePriceAlertRule,
    _owned_price_rule_command,
)
from rquant.price_alert_admission import (
    PriceAlertAdmission,
    PriceAlertAdmissionClient,
    PriceAlertAdmissionRejectedError,
    PriceAlertAdmissionUnavailableError,
    build_price_alert_admission_server,
)

pytestmark = pytest.mark.linux_exact

PAGE_UID = 64011
WEB_UID = 64012
OTHER_UID = 64013
SHARED_GID = 64014
NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)
CODE = "600001.SH"


def _save(command_id: str, *, rule_id: str = "threshold-a") -> SavePriceAlertRule:
    return SavePriceAlertRule(
        command_id=command_id,
        requested_at=NOW,
        ts_code=CODE,
        membership_version=1,
        rule=PriceAlertRule(
            rule_id=rule_id,
            name="到价提醒",
            priority="P2",
            enabled=True,
            comparison="gte",
            threshold=Decimal("10.00"),
            valid_from=clock_time(9, 30),
            valid_until=clock_time(14, 57),
        ),
    )


def _drop_to(uid: int) -> None:
    os.setgroups([SHARED_GID])
    os.setgid(SHARED_GID)
    os.setuid(uid)
    assert os.geteuid() == uid and os.getegid() == SHARED_GID


def _wait_exit(pid: int) -> int:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        observed, status = os.waitpid(pid, os.WNOHANG)
        if observed == pid:
            return status
        time.sleep(0.02)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise AssertionError(f"isolated UID process {pid} did not exit")


def _read_pipe(fd: int) -> bytes:
    ready, _, _ = select.select([fd], [], [], 10)
    if not ready:
        raise AssertionError("isolated UID process did not report within ten seconds")
    return os.read(fd, 8192)


class _Authority:
    def __init__(self, root: Path, pid: int, stop_fd: int, parses: c_uint) -> None:
        self.root = root
        self.pid = pid
        self.stop_fd = stop_fd
        self.parses = parses
        self.socket_path = root / "ingress" / "private" / "price.sock"
        self.db_path = root / "control" / "control.sqlite3"

    def run_as(self, uid: int, operation: Callable[[], dict[str, object]]) -> dict[str, object]:
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            os.close(self.stop_fd)
            try:
                _drop_to(uid)
                result = operation()
                message = b"OK:" + json.dumps(result, ensure_ascii=True).encode()
                status = 0
            except BaseException:
                message = b"ERROR:" + traceback.format_exc().encode()[:7000]
                status = 1
            with suppress(OSError):
                os.write(write_fd, message)
            os.close(write_fd)
            os._exit(status)
        os.close(write_fd)
        try:
            message = _read_pipe(read_fd)
            status = _wait_exit(pid)
        except BaseException:
            with suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
            with suppress(ChildProcessError):
                os.waitpid(pid, 0)
            raise
        finally:
            os.close(read_fd)
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, message.decode()
        assert message.startswith(b"OK:"), message.decode()
        return json.loads(message[3:])

    def counts(self) -> tuple[int, int]:
        with closing(sqlite3.connect(self.db_path)) as connection:
            commands = connection.execute("SELECT COUNT(*) FROM page_control_command").fetchone()[0]
            effects = connection.execute("SELECT COUNT(*) FROM page_control_effect").fetchone()[0]
        return commands, effects


def _serve_child(root: Path, ready_fd: int, stop_fd: int, parses: c_uint) -> None:
    try:
        # Load the Outbox's late schema imports while the CI checkout is still readable.
        for module in ("rquant.screen.alert_draft", "rquant.ai_usage"):
            importlib.import_module(module)
        _drop_to(PAGE_UID)
        control = root / "control"
        outbox = PageControlOutbox(control / "control.sqlite3")
        outbox.activate_manual_watchlist(NOW)
        outbox.activate_price_alert_rules(NOW)
        with closing(sqlite3.connect(outbox.path, isolation_level=None)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            for owner in ("alice", "bob"):
                ManualWatchlistRepository(connection).upsert(
                    ManualWatchlistUpsert(owner_id=owner, ts_code=CODE, source="detail"),
                    now=NOW,
                )
            connection.commit()
        pending = _owned_price_rule_command(
            _save("pending", rule_id="pending-rule"), authenticated_owner_id="alice"
        )
        outbox.enqueue_trusted_price_rule(pending)
        service = PageControlService(
            outbox=outbox,
            consumer=PageControlConsumer(
                outbox=outbox,
                data_dir=control / "data",
                log_dir=control / "logs",
                clock=lambda: NOW,
                consumer_id="price-linux-exact",
            ),
        )
        original_decode = admission_module._decode_request

        def counted_decode(body: bytes):
            parses.value += 1
            return original_decode(body)

        admission_module._decode_request = counted_decode
        path = root / "ingress" / "private" / "price.sock"
        server = build_price_alert_admission_server(
            PriceAlertAdmission(service),
            socket_path=path,
            trusted_web_uid=WEB_UID,
            shared_gid=SHARED_GID,
        )
        assert server is not None
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        os.write(ready_fd, b"READY")
        os.close(ready_fd)
        os.read(stop_fd, 1)
        server.shutdown()
        worker.join(timeout=3)
        assert not worker.is_alive()
        server.server_close()
        os.close(stop_fd)
        os._exit(0)
    except BaseException:
        with suppress(OSError):
            os.write(ready_fd, b"ERROR:" + traceback.format_exc().encode()[:7000])
        os._exit(1)


@pytest.fixture
def authority() -> Iterator[_Authority]:
    if sys.platform != "linux":
        pytest.skip("real two-UID peer credentials require Linux")
    assert os.geteuid() == 0, "the exact Linux gate must run as root"
    assert hasattr(socket, "SO_PEERCRED"), "Linux peer credentials are unavailable"
    assert len({os.geteuid(), PAGE_UID, WEB_UID, OTHER_UID}) == 4
    for uid in (PAGE_UID, WEB_UID, OTHER_UID):
        with pytest.raises(KeyError):
            pwd.getpwuid(uid)
    with pytest.raises(KeyError):
        grp.getgrgid(SHARED_GID)
    root = Path(tempfile.mkdtemp(prefix="rqp-linux-", dir="/tmp"))
    os.chmod(root, 0o711)
    control = root / "control"
    ingress = root / "ingress"
    control.mkdir(mode=0o700)
    ingress.mkdir(mode=0o711)
    for directory, mode in ((control, 0o700), (ingress, 0o711)):
        os.chown(directory, PAGE_UID, SHARED_GID)
        os.chmod(directory, mode)
    ready_read, ready_write = os.pipe()
    stop_read, stop_write = os.pipe()
    parses = RawValue(c_uint, 0)
    pid = os.fork()
    if pid == 0:
        os.close(ready_read)
        os.close(stop_write)
        _serve_child(root, ready_write, stop_read, parses)
        os._exit(1)
    os.close(ready_write)
    os.close(stop_read)
    ready_complete = False
    try:
        message = _read_pipe(ready_read)
        assert message == b"READY", message.decode(errors="replace")
        assert os.stat(root / "ingress" / "private").st_uid == PAGE_UID
        ready_complete = True
        yield _Authority(root, pid, stop_write, parses)
    finally:
        os.close(ready_read)
        with suppress(OSError):
            os.write(stop_write, b"x")
        os.close(stop_write)
        try:
            status = _wait_exit(pid)
        finally:
            shutil.rmtree(root)
        if ready_complete:
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0


def _client(path: Path) -> PriceAlertAdmissionClient:
    return PriceAlertAdmissionClient(path, expected_service_uid=PAGE_UID, shared_gid=SHARED_GID)


def test_real_web_uid_submits_looks_up_and_resumes_exact_command(authority: _Authority) -> None:
    socket_info = authority.socket_path.lstat()
    parent_info = authority.socket_path.parent.lstat()
    assert (socket_info.st_uid, socket_info.st_gid, stat.S_IMODE(socket_info.st_mode)) == (
        PAGE_UID,
        SHARED_GID,
        0o660,
    )
    assert (parent_info.st_uid, parent_info.st_gid, stat.S_IMODE(parent_info.st_mode)) == (
        PAGE_UID,
        SHARED_GID,
        0o710,
    )
    assert authority.counts() == (1, 0)

    def lookup_only() -> dict[str, object]:
        client = _client(authority.socket_path)
        pending = client.lookup(
            _save("pending", rule_id="pending-rule"), authenticated_owner_id="alice"
        )
        assert pending is not None and pending.status is PageControlStatus.PENDING
        assert client.lookup(_save("missing"), authenticated_owner_id="alice") is None
        return {"uid": os.geteuid(), "pending": pending.status.value}

    assert authority.run_as(WEB_UID, lookup_only) == {"uid": WEB_UID, "pending": "pending"}
    assert authority.counts() == (1, 0)

    def write_flow() -> dict[str, object]:
        client = _client(authority.socket_path)
        pending = _save("pending", rule_id="pending-rule")
        settled = client.resume(pending, authenticated_owner_id="alice")
        assert settled.status is PageControlStatus.SUCCEEDED
        assert client.resume(pending, authenticated_owner_id="alice") == settled
        assert client.lookup(pending, authenticated_owner_id="alice") == settled
        fresh = _save("fresh")
        saved = client.submit(fresh, authenticated_owner_id="alice")
        assert saved.status is PageControlStatus.SUCCEEDED
        assert client.submit(fresh, authenticated_owner_id="alice") == saved
        assert client.lookup(fresh, authenticated_owner_id="alice") == saved
        for operation in (client.submit, client.lookup, client.resume):
            with pytest.raises(PriceAlertAdmissionRejectedError, match="command_conflict"):
                operation(fresh, authenticated_owner_id="bob")
        bob = client.submit(_save("bob-own"), authenticated_owner_id="bob")
        assert bob.status is PageControlStatus.SUCCEEDED
        with pytest.raises(PriceAlertAdmissionRejectedError, match="not_found"):
            client.resume(_save("absent"), authenticated_owner_id="alice")
        return {
            "uid": os.geteuid(),
            "versions": [settled.result["version"], saved.result["version"], bob.result["version"]],
        }

    assert authority.run_as(WEB_UID, write_flow) == {
        "uid": WEB_UID,
        "versions": [1, 1, 1],
    }
    assert authority.counts() == (3, 3)


def test_same_uid_and_wrong_uid_are_rejected_before_parsing(authority: _Authority) -> None:
    before = authority.parses.value

    def same_uid() -> dict[str, object]:
        with pytest.raises(ValueError, match="distinct"):
            _client(authority.socket_path)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(2)
            connection.connect(str(authority.socket_path))
            with suppress(BrokenPipeError):
                connection.sendall(b"POST /v1/price-alert-admission HTTP/1.1\r\n")
            try:
                observed = connection.recv(1)
            except ConnectionResetError:
                observed = b""
        assert observed == b""
        return {"uid": os.geteuid(), "rejected": True}

    assert authority.run_as(PAGE_UID, same_uid) == {"uid": PAGE_UID, "rejected": True}

    def wrong_uid() -> dict[str, object]:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(2)
            connection.connect(str(authority.socket_path))
            with suppress(BrokenPipeError):
                connection.sendall(b"POST /v1/price-alert-admission HTTP/1.1\r\n")
            try:
                observed = connection.recv(1)
            except ConnectionResetError:
                observed = b""
        assert observed == b""
        with pytest.raises(PriceAlertAdmissionUnavailableError):
            _client(authority.socket_path).submit(_save("wrong-uid"), authenticated_owner_id="bob")
        return {"uid": os.geteuid(), "rejected": True}

    assert authority.run_as(OTHER_UID, wrong_uid) == {"uid": OTHER_UID, "rejected": True}
    assert authority.parses.value == before
    assert authority.counts() == (1, 0)


def test_web_uid_rejects_permissions_symlink_and_false_server_peer(authority: _Authority) -> None:
    path = authority.socket_path
    baseline = authority.parses.value

    def refused() -> dict[str, object]:
        with pytest.raises(PriceAlertAdmissionUnavailableError):
            _client(path).submit(_save("unsafe-path"), authenticated_owner_id="alice")
        return {"uid": os.geteuid(), "rejected": True}

    os.chmod(path, 0o666)
    try:
        assert authority.run_as(WEB_UID, refused)["rejected"] is True
    finally:
        os.chmod(path, 0o660)
    os.chmod(path.parent, 0o750)
    try:
        assert authority.run_as(WEB_UID, refused)["rejected"] is True
    finally:
        os.chmod(path.parent, 0o710)
    moved = path.with_suffix(".old")
    path.rename(moved)
    path.symlink_to(moved)
    try:
        assert authority.run_as(WEB_UID, refused)["rejected"] is True
    finally:
        path.unlink()
        moved.rename(path)

    spoof_dir = authority.root / "ingress" / "spoof"
    spoof_dir.mkdir(mode=0o710)
    os.chown(spoof_dir, PAGE_UID, SHARED_GID)
    os.chmod(spoof_dir, 0o710)
    spoof = spoof_dir / "false.sock"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as false_server:
        false_server.bind(str(spoof))
        false_server.listen(1)
        os.chown(spoof, PAGE_UID, SHARED_GID)
        os.chmod(spoof, 0o660)

        def false_peer() -> dict[str, object]:
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                _client(spoof).submit(_save("false-peer"), authenticated_owner_id="alice")
            return {"uid": os.geteuid(), "rejected": True}

        assert authority.run_as(WEB_UID, false_peer)["rejected"] is True
    spoof.unlink()
    assert authority.parses.value == baseline
    assert authority.counts() == (1, 0)
