"""Bounded, read-only journal projection for signed installed services."""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import socket
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import Field, StrictStr

from rquant.ops_status import _bounded_proc_read, _run_bounded, load_signed_ops_manifest
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, strict_json_loads

LIFECYCLE_MESSAGE_ID = "4d46a7d8b26c4e0ea93d681a62f1ad70"
LIFECYCLE_MESSAGE = "rquant.lifecycle"

_BOOT_PATH = "/proc/sys/kernel/random/boot_id"
_BOOT_ID = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
_JOURNAL_CURSOR = re.compile(r"^[\x21-\x7e]{1,1024}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_PRIORITY = {
    "emerg": 0,
    "alert": 1,
    "crit": 2,
    "err": 3,
    "warning": 4,
    "notice": 5,
    "info": 6,
    "debug": 7,
}
_LEVEL_TEXT = ("紧急", "警报", "严重", "错误", "警告", "注意", "信息", "调试")
_EVENT_TEXT = {
    "run_started": "任务已开始",
    "run_succeeded": "任务已完成",
    "run_failed": "任务未完成",
}
_STRUCTURED_UNITS = frozenset({"rquant-daily.service", "rquant-backup.service"})
_JOURNAL_FIELDS = frozenset(
    {
        "__CURSOR",
        "__REALTIME_TIMESTAMP",
        "__MONOTONIC_TIMESTAMP",
        "_BOOT_ID",
        "_SYSTEMD_UNIT",
        "_SYSTEMD_INVOCATION_ID",
        "_PID",
        "_UID",
        "_GID",
        "_COMM",
        "_EXE",
        "_CMDLINE",
        "_CAP_EFFECTIVE",
        "_SELINUX_CONTEXT",
        "_SYSTEMD_CGROUP",
        "_SYSTEMD_SLICE",
        "_SYSTEMD_USER_SLICE",
        "_SYSTEMD_SESSION",
        "_SYSTEMD_OWNER_UID",
        "_HOSTNAME",
        "_MACHINE_ID",
        "_TRANSPORT",
        "_STREAM_ID",
        "_LINE_BREAK",
        "_SOURCE_REALTIME_TIMESTAMP",
        "SYSLOG_IDENTIFIER",
        "SYSLOG_FACILITY",
        "SYSLOG_PID",
        "PRIORITY",
        "MESSAGE",
        "MESSAGE_ID",
        "RQUANT_EVENT",
        "RQUANT_DURATION_MS",
        "RQUANT_EXIT_CODE",
    }
)
_MAX_ROWS = 500
_MAX_PAGE_SIZE = _MAX_ROWS - 2
_MAX_BYTES = 256 * 1024
_MAX_MESSAGE_BYTES = 4 * 1024
_MAX_SECONDS = 5.0
_SEVEN_DAYS = timedelta(days=7)
_SCOPE = "本机本次开机以来的服务日志（含手动运行）"


class JournalRequestError(ValueError):
    """The request is outside the fixed log-read scope."""

    def __init__(self) -> None:
        super().__init__("日志请求无效")


class JournalCursorError(ValueError):
    """The page cursor no longer matches its source and filters."""

    def __init__(self) -> None:
        super().__init__("日志已更新，请重新查看")


class JournalUnavailableError(RuntimeError):
    """The source could not provide a trustworthy bounded page."""

    def __init__(self) -> None:
        super().__init__("运行日志暂不可用")


class JournalEntry(RuntimeContractModel):
    at: AwareUtcDatetime
    level: Literal["紧急", "警报", "严重", "错误", "警告", "注意", "信息", "调试"]
    text: Literal["任务已开始", "任务已完成", "任务未完成", "该条内容暂不可显示"]


class JournalPage(RuntimeContractModel):
    service_label: StrictStr = Field(min_length=1, max_length=40)
    scope: Literal["本机本次开机以来的服务日志（含手动运行）"] = _SCOPE
    entries: tuple[JournalEntry, ...] = Field(max_length=_MAX_PAGE_SIZE)
    next_cursor: StrictStr | None = None


CommandRunner = Callable[[tuple[str, ...], float, int], bytes]
BootReader = Callable[[], bytes]


def _default_boot_reader() -> bytes:
    return _bounded_proc_read(_BOOT_PATH, 128)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise JournalCursorError
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, base64.binascii.Error):
        raise JournalCursorError from None
    if _b64(decoded) != value:
        raise JournalCursorError
    return decoded


def _boot_id(payload: bytes) -> str:
    if len(payload) > 128:
        raise ValueError
    boot = payload.decode("ascii").strip()
    if _BOOT_ID.fullmatch(boot) is None:
        raise ValueError
    return boot


def _bounded_number(value: object, *, maximum: int) -> int | None:
    if type(value) is not str or re.fullmatch(r"(?:0|[1-9][0-9]{0,18})", value) is None:
        return None
    parsed = int(value)
    return parsed if parsed <= maximum else None


def _event_text(unit: str, row: dict[str, object]) -> str:
    hidden = "该条内容暂不可显示"
    if unit not in _STRUCTURED_UNITS or not set(row).issubset(_JOURNAL_FIELDS):
        return hidden
    if row.get("MESSAGE_ID") != LIFECYCLE_MESSAGE_ID or row.get("MESSAGE") != LIFECYCLE_MESSAGE:
        return hidden
    event = row.get("RQUANT_EVENT")
    if type(event) is not str or event not in _EVENT_TEXT:
        return hidden
    duration = row.get("RQUANT_DURATION_MS")
    exit_code = row.get("RQUANT_EXIT_CODE")
    if (
        "RQUANT_DURATION_MS" in row
        and _bounded_number(duration, maximum=7 * 24 * 60 * 60 * 1000) is None
    ):
        return hidden
    if "RQUANT_EXIT_CODE" in row and _bounded_number(exit_code, maximum=255) is None:
        return hidden
    if event == "run_started" and (duration is not None or exit_code is not None):
        return hidden
    if event == "run_succeeded" and exit_code not in (None, "0"):
        return hidden
    return _EVENT_TEXT[event]


def _parse_rows(
    payload: bytes,
    *,
    unit: str,
    boot: str,
    since: datetime | None,
    level: str | None,
    max_rows: int,
) -> list[tuple[str, JournalEntry]]:
    if len(payload) > _MAX_BYTES or (payload and not payload.endswith(b"\n")):
        raise ValueError
    text = payload.decode("utf-8")
    lines = text.splitlines()
    if len(lines) > max_rows or any(not line for line in lines):
        raise ValueError
    rows: list[tuple[str, JournalEntry]] = []
    seen: set[str] = set()
    previous_at: datetime | None = None
    for line in lines:
        parsed = strict_json_loads(
            line,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if type(parsed) is not dict:
            raise ValueError
        row: dict[str, object] = parsed
        cursor = row.get("__CURSOR")
        if (
            type(cursor) is not str
            or _JOURNAL_CURSOR.fullmatch(cursor) is None
            or cursor in seen
            or row.get("_BOOT_ID") != boot
            or row.get("_SYSTEMD_UNIT") != unit
        ):
            raise ValueError
        seen.add(cursor)
        micros = _bounded_number(row.get("__REALTIME_TIMESTAMP"), maximum=2**63 - 1)
        priority = _bounded_number(row.get("PRIORITY"), maximum=7)
        if micros is None or priority is None:
            raise ValueError
        at = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)
        if (since is not None and at < since) or (previous_at is not None and at > previous_at):
            raise ValueError
        if level is not None and priority > _PRIORITY[level]:
            raise ValueError
        previous_at = at
        message = row.get("MESSAGE")
        if "MESSAGE" in row and (
            type(message) is not str or len(message.encode("utf-8")) > _MAX_MESSAGE_BYTES
        ):
            raise ValueError
        rows.append(
            (
                cursor,
                JournalEntry(at=at, level=_LEVEL_TEXT[priority], text=_event_text(unit, row)),
            )
        )
    return rows


class JournalLogReader:
    def __init__(
        self,
        *,
        manifest_path: Path,
        public_key_pem: bytes,
        expected_host: str,
        cursor_secret: bytes,
        command_runner: CommandRunner = _run_bounded,
        boot_reader: BootReader = _default_boot_reader,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        host_name: Callable[[], str] = socket.gethostname,
    ) -> None:
        if len(cursor_secret) < 32:
            raise ValueError("journal cursor signing key is too short")
        self.manifest_path = manifest_path
        self.public_key_pem = public_key_pem
        self.expected_host = expected_host
        self.cursor_secret = cursor_secret
        self.command_runner = command_runner
        self.boot_reader = boot_reader
        self.clock = clock
        self.monotonic = monotonic
        self.host_name = host_name

    def _encode_cursor(self, binding: dict[str, str]) -> str:
        body = canonical_json_bytes(binding)
        tag = hmac.new(self.cursor_secret, body, hashlib.sha256).digest()
        return _b64(body) + "." + _b64(tag)

    def _decode_cursor(self, token: str) -> dict[str, str]:
        if len(token) > 4096 or token.count(".") != 1:
            raise JournalCursorError
        body_part, tag_part = token.split(".")
        body, tag = _unb64(body_part), _unb64(tag_part)
        expected = hmac.new(self.cursor_secret, body, hashlib.sha256).digest()
        if not hmac.compare_digest(tag, expected):
            raise JournalCursorError
        try:
            decoded = strict_json_loads(body)
            if (
                type(decoded) is not dict
                or set(decoded) != {"unit", "boot", "manifest", "host", "since", "level", "journal"}
                or any(type(value) is not str for value in decoded.values())
                or canonical_json_bytes(decoded) != body
                or _JOURNAL_CURSOR.fullmatch(decoded["journal"]) is None
                or _BOOT_ID.fullmatch(decoded["boot"]) is None
                or _DIGEST.fullmatch(decoded["manifest"]) is None
            ):
                raise ValueError
        except (UnicodeError, ValueError, TypeError):
            raise JournalCursorError from None
        return decoded

    def read(
        self,
        *,
        unit: str,
        since: datetime,
        level: str | None = None,
        page_size: int = 100,
        cursor: str | None = None,
    ) -> JournalPage:
        now = self.clock()
        if (
            type(unit) is not str
            or type(since) is not datetime
            or since.tzinfo is None
            or since.utcoffset() is None
            or now.tzinfo is None
            or now.utcoffset() is None
            or (level is not None and (type(level) is not str or level not in _PRIORITY))
            or type(page_size) is not int
            or not 1 <= page_size <= _MAX_PAGE_SIZE
            or (cursor is not None and type(cursor) is not str)
        ):
            raise JournalRequestError
        if not now - _SEVEN_DAYS <= since <= now:
            if cursor is not None:
                raise JournalCursorError
            raise JournalRequestError
        since_utc = since.astimezone(UTC)
        prior = self._decode_cursor(cursor) if cursor is not None else None
        started = self.monotonic()
        try:
            manifest, digest = load_signed_ops_manifest(
                self.manifest_path,
                public_key_pem=self.public_key_pem,
                expected_host=self.expected_host,
            )
            if self.host_name() != self.expected_host:
                raise ValueError
            installed = next((item for item in manifest.units if item.service == unit), None)
            if installed is None:
                if prior is None:
                    raise JournalRequestError
                raise JournalCursorError
            boot = _boot_id(self.boot_reader())
            binding = {
                "unit": unit,
                "boot": boot,
                "manifest": digest,
                "host": self.expected_host,
                "since": since_utc.isoformat(),
                "level": level or "",
            }
            if prior is not None and any(prior[key] != value for key, value in binding.items()):
                raise JournalCursorError
            remaining = _MAX_SECONDS - (self.monotonic() - started)
            if remaining <= 0:
                raise JournalUnavailableError
            limit = page_size + 2
            argv = (
                "/usr/bin/journalctl",
                "--no-pager",
                "--quiet",
                "--output=json",
                "--reverse",
                f"--boot={boot}",
                *((f"--since={since_utc.isoformat()}",) if prior is None else ()),
                *((f"--priority={level}",) if level is not None else ()),
                f"--lines={limit}",
                *((f"--cursor={prior['journal']}",) if prior is not None else ()),
                f"_SYSTEMD_UNIT={unit}",
            )
            try:
                payload = self.command_runner(argv, remaining, _MAX_BYTES)
                if type(payload) is not bytes:
                    raise ValueError
                rows = _parse_rows(
                    payload,
                    unit=unit,
                    boot=boot,
                    since=since_utc if prior is None else None,
                    level=level,
                    max_rows=limit,
                )
            except Exception:
                raise JournalUnavailableError from None
            if _boot_id(self.boot_reader()) != boot:
                if prior is not None:
                    raise JournalCursorError
                raise JournalUnavailableError
            _, final_digest = load_signed_ops_manifest(
                self.manifest_path,
                public_key_pem=self.public_key_pem,
                expected_host=self.expected_host,
            )
            if final_digest != digest:
                if prior is not None:
                    raise JournalCursorError
                raise JournalUnavailableError
            if self.monotonic() - started > _MAX_SECONDS:
                raise JournalUnavailableError
            if prior is not None:
                if not rows or rows[0][0] != prior["journal"]:
                    raise JournalCursorError
                rows = rows[1:]
                rows = [row for row in rows if row[1].at >= since_utc]
            visible = rows[:page_size]
            has_more = len(rows) > page_size
            next_cursor = (
                self._encode_cursor(binding | {"journal": visible[-1][0]})
                if has_more and visible
                else None
            )
            return JournalPage(
                service_label=installed.label,
                entries=tuple(entry for _, entry in visible),
                next_cursor=next_cursor,
            )
        except (JournalRequestError, JournalCursorError, JournalUnavailableError):
            raise
        except Exception:
            if prior is not None:
                raise JournalCursorError from None
            raise JournalUnavailableError from None


__all__ = [
    "LIFECYCLE_MESSAGE",
    "LIFECYCLE_MESSAGE_ID",
    "JournalCursorError",
    "JournalEntry",
    "JournalLogReader",
    "JournalPage",
    "JournalRequestError",
    "JournalUnavailableError",
]
