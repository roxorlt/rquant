from __future__ import annotations

import os
import socket
import stat
import struct
import threading
import time
from datetime import UTC, datetime
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rquant.strict_json import canonical_json_bytes, strict_json_loads
from rquant.unit_log_reader import (
    JournalCursorError,
    JournalEntry,
    JournalPage,
    JournalRequestError,
    JournalUnavailableError,
)
from rquant.unit_log_service import UnitLogClient, UnitLogService, UnitLogServiceError

SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
SINCE = datetime(2026, 9, 28, 4, 0, tzinfo=UTC)
UNIT = "rquant-daily.service"
TOKEN = "Bearer secret-key=123456"


class FakeReader:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.result = JournalPage(
            service_label="每日任务",
            entries=(JournalEntry(at=SINCE, level="信息", text="任务已开始"),),
        )
        self.error: Exception | None = None

    def read(self, **kwargs: object) -> JournalPage:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result


def _private_path(root: Path) -> Path:
    directory = root / "private"
    directory.mkdir()
    os.chown(directory, -1, os.getegid())
    os.chmod(directory, 0o710)
    return directory / "logs.sock"


def _request(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "version": 1,
        "op": "read",
        "unit": UNIT,
        "since": SINCE.isoformat(),
        "level": None,
        "page_size": 20,
        "cursor": None,
    }
    value.update(overrides)
    return value


def _receive_exact(connection: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise EOFError
        data.extend(chunk)
    return bytes(data)


def _wire(path: Path, payload: bytes) -> dict[str, object]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(3)
        connection.connect(str(path))
        connection.sendall(struct.pack("!I", len(payload)) + payload)
        length = struct.unpack("!I", _receive_exact(connection, 4))[0]
        assert 0 < length <= 256 * 1024
        result = strict_json_loads(_receive_exact(connection, length))
    assert type(result) is dict
    return result


def _call(path: Path, **overrides: object) -> dict[str, object]:
    return _wire(path, canonical_json_bytes(_request(**overrides)))


class RunningService:
    def __init__(
        self,
        path: Path,
        reader: FakeReader,
        *,
        peer_uid: int | None = None,
        min_interval: float = 1.0,
        clock: object | None = None,
    ) -> None:
        self.path = path
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.service = UnitLogService(
            socket_path=path,
            web_group_gid=os.getegid(),
            web_uid=os.geteuid(),
            reader=reader,
            peer_uid=(lambda _connection: peer_uid) if peer_uid is not None else None,
            min_interval_seconds=min_interval,
            monotonic=clock if clock is not None else count(100).__next__,
        )
        self.thread = threading.Thread(
            target=self.service.serve,
            kwargs={"stop": self.stop, "ready": self.ready},
            daemon=True,
        )

    def __enter__(self) -> RunningService:
        self.thread.start()
        assert self.ready.wait(2), "service did not bind"
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop.set()
        self.thread.join(7)
        assert not self.thread.is_alive()


def test_valid_request_round_trip_uses_private_socket_and_fixed_fields() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        with RunningService(path, reader):
            state = path.lstat()
            assert stat.S_ISSOCK(state.st_mode)
            assert (state.st_uid, state.st_gid, stat.S_IMODE(state.st_mode)) == (
                os.geteuid(),
                os.getegid(),
                0o660,
            )
            result = _call(path)
            assert set(result) == {"status", "page"}
            assert result["status"] == "ok"
            assert result["page"] == reader.result.model_dump(mode="json")
            client = UnitLogClient(
                socket_path=path,
                service_uid=os.geteuid(),
                web_group_gid=os.getegid(),
            )
            assert client.read(unit=UNIT, since=SINCE, page_size=20) == reader.result
            assert (
                reader.calls
                == [{"unit": UNIT, "since": SINCE, "level": None, "page_size": 20, "cursor": None}]
                * 2
            )
        assert not path.exists()


@pytest.mark.parametrize("directory_mode", [0o700, 0o750, 0o777])
def test_service_refuses_unsafe_directory_before_binding(directory_mode: int) -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        os.chmod(path.parent, directory_mode)
        service = UnitLogService(
            socket_path=path,
            web_group_gid=os.getegid(),
            web_uid=os.geteuid(),
            reader=FakeReader(),
        )
        with pytest.raises(ValueError, match="0710"):
            service.serve(stop=threading.Event())
        assert not path.exists()


def test_service_refuses_preexisting_socket_path_without_unlinking() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        path.write_text("keep")
        service = UnitLogService(
            socket_path=path,
            web_group_gid=os.getegid(),
            web_uid=os.geteuid(),
            reader=FakeReader(),
        )
        with pytest.raises(ValueError, match="already exists"):
            service.serve(stop=threading.Event())
        assert path.read_text() == "keep"


def test_service_cannot_disable_fixed_rate_limit() -> None:
    with pytest.raises(ValueError, match="configuration"):
        UnitLogService(
            socket_path=Path("/private/tmp/logs.sock"),
            web_group_gid=os.getegid(),
            web_uid=os.geteuid(),
            reader=FakeReader(),
            min_interval_seconds=0,
        )


def test_wrong_peer_uid_is_rejected_before_parsing_or_reading() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        with RunningService(path, reader, peer_uid=os.geteuid() + 1):
            response = _wire(path, canonical_json_bytes(_request(authorization=TOKEN)))
        assert response == {
            "status": "error",
            "code": "forbidden",
            "message": "无权查看运行日志",
        }
        assert not reader.calls
        assert TOKEN not in str(response)


@pytest.mark.parametrize(
    "payload",
    [
        canonical_json_bytes(_request(user="admin")),
        canonical_json_bytes(_request(headers={"x-user": "admin"})),
        canonical_json_bytes(_request(unit="../../secret")),
        canonical_json_bytes(_request(unit="--unit=ssh.service")),
        canonical_json_bytes(_request(path="/tmp/secret")),
        canonical_json_bytes(_request(page_size=True)),
        canonical_json_bytes(_request(since="2026-09-28T04:00:00")),
        b'{"version":1,"version":1}',
        b"\xff",
        b"{not json}",
    ],
)
def test_malformed_or_spoofed_requests_never_reach_reader(payload: bytes) -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        with RunningService(path, reader):
            response = _wire(path, payload)
        assert response == {
            "status": "error",
            "code": "invalid_request",
            "message": "日志请求无效",
        }
        assert not reader.calls


def test_slow_partial_request_has_one_second_total_deadline() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        with (
            RunningService(path, reader),
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection,
        ):
            connection.settimeout(2)
            connection.connect(str(path))
            connection.sendall(struct.pack("!I", 8))
            for _ in range(3):
                time.sleep(0.25)
                connection.sendall(b"x")
            time.sleep(0.4)
            connection.settimeout(0.4)
            length = struct.unpack("!I", _receive_exact(connection, 4))[0]
            response = strict_json_loads(_receive_exact(connection, length))
        assert response == {
            "status": "error",
            "code": "invalid_request",
            "message": "日志请求无效",
        }
        assert not reader.calls
        assert TOKEN not in str(response)


def test_oversized_frame_is_rejected_after_length_header() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        with (
            RunningService(path, reader),
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection,
        ):
            connection.settimeout(3)
            connection.connect(str(path))
            connection.sendall(struct.pack("!I", 8193))
            length = struct.unpack("!I", _receive_exact(connection, 4))[0]
            response = strict_json_loads(_receive_exact(connection, length))
        assert response == {
            "status": "error",
            "code": "invalid_request",
            "message": "日志请求无效",
        }
        assert not reader.calls


@pytest.mark.parametrize(
    ("error", "code", "message"),
    [
        (JournalRequestError(), "invalid_request", "日志请求无效"),
        (JournalCursorError(), "cursor_changed", "日志已更新，请重新查看"),
        (JournalUnavailableError(), "unavailable", "运行日志暂不可用"),
        (RuntimeError(TOKEN), "unavailable", "运行日志暂不可用"),
    ],
)
def test_reader_errors_have_closed_messages_and_release_capacity(
    error: Exception, code: str, message: str
) -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        reader.error = error
        with RunningService(path, reader):
            response = _call(path)
            assert response == {"status": "error", "code": code, "message": message}
            assert TOKEN not in str(response)
            reader.error = None
            assert _call(path)["status"] == "ok"


def test_one_journal_call_at_a_time_and_reject_overload() -> None:
    class BlockingReader(FakeReader):
        entered = threading.Event()
        release = threading.Event()

        def read(self, **kwargs: object) -> JournalPage:
            self.entered.set()
            assert self.release.wait(3)
            return super().read(**kwargs)

    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = BlockingReader()
        first: list[dict[str, object]] = []
        with RunningService(path, reader):
            thread = threading.Thread(target=lambda: first.append(_call(path)))
            thread.start()
            assert reader.entered.wait(2)
            started = time.monotonic()
            assert _call(path) == {
                "status": "error",
                "code": "busy",
                "message": "请求较多，请稍后重试",
            }
            assert time.monotonic() - started < 1
            reader.release.set()
            thread.join(3)
            assert first[0]["status"] == "ok"
            assert len(reader.calls) == 1
            assert _call(path)["status"] == "ok"


def test_fixed_rate_rejects_early_request_then_recovers() -> None:
    ticks = [100.0]
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        with RunningService(path, reader, min_interval=1.0, clock=lambda: ticks[0]):
            assert _call(path)["status"] == "ok"
            assert _call(path)["code"] == "busy"
            ticks[0] = 101.0
            assert _call(path)["status"] == "ok"
            assert len(reader.calls) == 2


def test_client_rejects_untrusted_path_and_server_error() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        client = UnitLogClient(
            socket_path=path,
            service_uid=os.geteuid(),
            web_group_gid=os.getegid(),
        )
        reader = FakeReader()
        with RunningService(path, reader):
            reader.error = JournalCursorError()
            with pytest.raises(UnitLogServiceError) as exc:
                client.read(unit=UNIT, since=SINCE)
            assert exc.value.code == "cursor_changed"
            assert str(exc.value) == "日志已更新，请重新查看"
            os.chmod(path, 0o666)
            with pytest.raises(UnitLogServiceError, match="运行日志暂不可用"):
                client.read(unit=UNIT, since=SINCE)


def test_client_rejects_wrong_server_peer_uid_before_sending_request() -> None:
    with TemporaryDirectory(prefix="rql-", dir=SHORT_TMP) as directory:
        path = _private_path(Path(directory))
        reader = FakeReader()
        client = UnitLogClient(
            socket_path=path,
            service_uid=os.geteuid(),
            web_group_gid=os.getegid(),
            peer_uid=lambda _connection: os.geteuid() + 1,
        )
        with (
            RunningService(path, reader),
            pytest.raises(UnitLogServiceError, match="运行日志暂不可用"),
        ):
            client.read(unit=UNIT, since=SINCE)
        assert not reader.calls
