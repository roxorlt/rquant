from __future__ import annotations

import base64
import json
import shutil
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.ops_status import (
    STATIC_TIMER_STEMS,
    OpsInstallManifest,
    OpsUnitInstall,
    SignedOpsInstallManifest,
)
from rquant.unit_log_reader import (
    LIFECYCLE_MESSAGE,
    LIFECYCLE_MESSAGE_ID,
    JournalCursorError,
    JournalLogReader,
    JournalRequestError,
    JournalUnavailableError,
)

NOW = datetime(2026, 9, 28, 4, 0, tzinfo=UTC)
SINCE = NOW - timedelta(days=1)
BOOT = "12345678-1234-1234-1234-123456789abc"
JOURNAL_BOOT = BOOT.replace("-", "")
DAILY = "rquant-daily.service"
BACKUP = "rquant-backup.service"


def _manifest() -> OpsInstallManifest:
    return OpsInstallManifest(
        version=1,
        host_name="rquant-test",
        units=tuple(
            OpsUnitInstall(
                timer=f"rquant-{stem}.timer",
                service=f"rquant-{stem}.service",
                label="每日任务" if stem == "daily" else "备份" if stem == "backup" else "其他任务",
                expected_enabled=True,
                session="all",
                resource_group="maintenance",
            )
            for stem in STATIC_TIMER_STEMS
        ),
    )


@pytest.fixture
def signed_manifest(tmp_path: Path) -> tuple[Path, bytes]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is required for signed manifest tests")
    private = tmp_path / "private.pem"
    public = tmp_path / "public.pem"
    payload = tmp_path / "manifest.payload"
    signature = tmp_path / "manifest.signature"
    subprocess.run(
        (openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)),
        check=True,
        capture_output=True,
    )
    manifest = _manifest()
    payload.write_bytes(manifest.signing_bytes())
    subprocess.run(
        (
            openssl,
            "pkeyutl",
            "-sign",
            "-inkey",
            str(private),
            "-rawin",
            "-in",
            str(payload),
            "-out",
            str(signature),
        ),
        check=True,
        capture_output=True,
    )
    signed = SignedOpsInstallManifest(
        manifest=manifest,
        signature=base64.b64encode(signature.read_bytes()).decode("ascii"),
    )
    path = tmp_path / "manifest.json"
    path.write_bytes(signed.canonical_bytes())
    return path, public.read_bytes()


def _row(
    cursor: str,
    *,
    timestamp: int = 1_790_566_800_000_000,
    unit: str = DAILY,
    boot: str = JOURNAL_BOOT,
    priority: str = "6",
    event: str = "run_started",
    message: str = LIFECYCLE_MESSAGE,
    **extras: object,
) -> dict[str, object]:
    return {
        "__CURSOR": cursor,
        "__REALTIME_TIMESTAMP": str(timestamp),
        "_SYSTEMD_UNIT": unit,
        "_BOOT_ID": boot,
        "PRIORITY": priority,
        "MESSAGE_ID": LIFECYCLE_MESSAGE_ID,
        "MESSAGE": message,
        "RQUANT_EVENT": event,
        **extras,
    }


def _json_lines(*rows: dict[str, object]) -> bytes:
    return b"".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        for row in rows
    )


def _reader(
    signed_manifest: tuple[Path, bytes],
    runner: Callable[[tuple[str, ...], float, int], bytes],
    *,
    boot_reader: Callable[[], bytes] = lambda: (BOOT + "\n").encode(),
    monotonic: Callable[[], float] = lambda: 1.0,
) -> JournalLogReader:
    path, public = signed_manifest
    return JournalLogReader(
        manifest_path=path,
        public_key_pem=public,
        expected_host="rquant-test",
        cursor_secret=b"fixed-test-secret-longer-than-32-bytes",
        command_runner=runner,
        boot_reader=boot_reader,
        clock=lambda: NOW,
        monotonic=monotonic,
        host_name=lambda: "rquant-test",
    )


def test_linux_boot_uuid_is_compacted_for_journal_and_page_cursor(
    signed_manifest: tuple[Path, bytes],
) -> None:
    compact_boot = BOOT.replace("-", "")
    records = (
        _row("s=first", boot=compact_boot),
        _row("s=last", boot=compact_boot),
    )
    argv_calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], _timeout: float, _max_bytes: int) -> bytes:
        argv_calls.append(argv)
        return _json_lines(*records)

    reader = _reader(signed_manifest, runner)
    first = reader.read(unit=DAILY, since=SINCE, page_size=1)
    assert len(first.entries) == 1 and first.next_cursor is not None
    second = reader.read(unit=DAILY, since=SINCE, page_size=1, cursor=first.next_cursor)
    assert len(second.entries) == 1 and second.next_cursor is None
    assert all(f"--boot={compact_boot}" in argv for argv in argv_calls)

    token_body = first.next_cursor.split(".")[0]
    decoded = json.loads(base64.urlsafe_b64decode(token_body + "=" * (-len(token_body) % 4)))
    assert decoded["boot"] == compact_boot
    old_format_cursor = reader._encode_cursor(decoded | {"boot": BOOT})
    with pytest.raises(JournalCursorError, match="日志已更新"):
        reader.read(unit=DAILY, since=SINCE, page_size=1, cursor=old_format_cursor)

    with pytest.raises(JournalUnavailableError):
        _reader(
            signed_manifest,
            runner,
            boot_reader=lambda: (compact_boot + "\n").encode(),
        ).read(unit=DAILY, since=SINCE)


def test_linux_standard_journal_metadata_keeps_registered_event_visible(
    signed_manifest: tuple[Path, bytes],
) -> None:
    row = _row(
        "s=metadata",
        _RUNTIME_SCOPE="system",
        __SEQNUM="319",
        __SEQNUM_ID="a" * 32,
    )
    page = _reader(signed_manifest, lambda *_args: _json_lines(row)).read(unit=DAILY, since=SINCE)
    assert [item.text for item in page.entries] == ["任务已开始"]


def test_exact_signed_service_and_fixed_command_projection(
    signed_manifest: tuple[Path, bytes],
) -> None:
    calls: list[tuple[tuple[str, ...], float, int]] = []

    def runner(argv: tuple[str, ...], timeout: float, max_bytes: int) -> bytes:
        calls.append((argv, timeout, max_bytes))
        return _json_lines(
            _row("s=first", event="run_succeeded", RQUANT_DURATION_MS="920"),
            _row(
                "s=second",
                timestamp=1_790_566_799_000_000,
                event="run_failed",
                RQUANT_EXIT_CODE="2",
            ),
        )

    reader = _reader(signed_manifest, runner)
    page = reader.read(unit=DAILY, since=SINCE, level="info", page_size=20)
    assert page.service_label == "每日任务"
    assert page.scope == "本机本次开机以来的服务日志（含手动运行）"
    assert [item.text for item in page.entries] == ["任务已完成", "任务未完成"]
    assert all(item.level == "信息" for item in page.entries)
    assert page.next_cursor is None
    assert len(calls) == 1
    argv, timeout, max_bytes = calls[0]
    assert argv[0] == "/usr/bin/journalctl"
    assert "--no-pager" in argv and "--output=json" in argv and "--reverse" in argv
    assert f"--boot={JOURNAL_BOOT}" in argv
    assert f"--since={SINCE.isoformat()}" in argv
    assert "--priority=info" in argv
    assert "--lines=22" in argv
    assert argv[-1] == f"_SYSTEMD_UNIT={DAILY}"
    assert 0 < timeout <= 5 and max_bytes == 256 * 1024

    with pytest.raises(JournalRequestError):
        reader.read(unit="rquant-daily.service;id", since=SINCE)
    with pytest.raises(JournalRequestError):
        reader.read(unit="ssh.service", since=SINCE)
    with pytest.raises(JournalRequestError):
        reader.read(unit=DAILY, since=SINCE - timedelta(days=8))
    with pytest.raises(JournalRequestError):
        reader.read(unit=DAILY, since=SINCE.replace(tzinfo=None))
    with pytest.raises(JournalRequestError):
        reader.read(unit=DAILY, since=SINCE, level="info;id")
    with pytest.raises(JournalRequestError):
        reader.read(unit=DAILY, since=SINCE, level=[])
    with pytest.raises(JournalRequestError):
        reader.read(unit=DAILY, since=SINCE, page_size=499)
    assert len(calls) == 1


def test_same_microsecond_cursor_paging_is_stable_and_bound(
    signed_manifest: tuple[Path, bytes],
) -> None:
    rows = [_row(f"s={index}") for index in range(5)]
    observed: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], _timeout: float, _max_bytes: int) -> bytes:
        observed.append(argv)
        cursor_arg = next((arg for arg in argv if arg.startswith("--cursor=")), None)
        offset = (
            0
            if cursor_arg is None
            else next(index for index, row in enumerate(rows) if row["__CURSOR"] == cursor_arg[9:])
        )
        limit = int(next(arg for arg in argv if arg.startswith("--lines="))[8:])
        return _json_lines(*rows[offset : offset + limit])

    reader = _reader(signed_manifest, runner)
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    second = reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)
    third = reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=second.next_cursor)
    assert [len(page.entries) for page in (first, second, third)] == [2, 2, 1]
    assert first.next_cursor and second.next_cursor and third.next_cursor is None
    assert f"--since={SINCE.isoformat()}" in observed[0]
    assert not any(arg.startswith("--cursor=") for arg in observed[0])
    assert "--cursor=s=1" in observed[1]
    assert "--cursor=s=3" in observed[2]
    assert not any(arg.startswith("--since=") for arg in observed[1])
    assert not any(arg.startswith("--since=") for arg in observed[2])
    with pytest.raises(JournalCursorError):
        reader.read(unit=BACKUP, since=SINCE, page_size=2, cursor=first.next_cursor)
    with pytest.raises(JournalCursorError):
        reader.read(
            unit=DAILY, since=SINCE + timedelta(seconds=1), page_size=2, cursor=first.next_cursor
        )
    with pytest.raises(JournalCursorError):
        reader.read(unit=DAILY, since=SINCE, level="err", page_size=2, cursor=first.next_cursor)
    with pytest.raises(JournalCursorError):
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor[:-1] + "x")
    assert len(observed) == 3


def test_cursor_page_stops_at_signed_since_without_treating_older_rows_as_damage(
    signed_manifest: tuple[Path, bytes],
) -> None:
    since_us = int(SINCE.timestamp() * 1_000_000)
    rows = (
        _row("s=latest"),
        _row("s=boundary-before", timestamp=since_us + 1),
        _row("s=boundary", timestamp=since_us),
        _row("s=older", timestamp=since_us - 1),
        _row("s=oldest", timestamp=since_us - 2),
    )
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], _timeout: float, _max_bytes: int) -> bytes:
        calls.append(argv)
        has_since = any(arg.startswith("--since=") for arg in argv)
        has_cursor = any(arg.startswith("--cursor=") for arg in argv)
        if has_since and has_cursor:
            raise ValueError("journalctl rejects --since with --cursor")
        if has_cursor:
            return _json_lines(*rows[1:])
        return _json_lines(*rows[:3])

    reader = _reader(signed_manifest, runner)
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    assert first.next_cursor is not None
    second = reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)
    assert len(second.entries) == 1
    assert second.entries[0].at == SINCE
    assert second.next_cursor is None
    assert len(calls) == 2


def test_rotation_and_boot_change_invalidate_cursor(signed_manifest: tuple[Path, bytes]) -> None:
    outputs = iter((_json_lines(*[_row(f"s={i}") for i in range(3)]), _json_lines(_row("s=older"))))
    reader = _reader(signed_manifest, lambda *_args: next(outputs))
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    with pytest.raises(JournalCursorError):
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)

    boots = iter(
        ((BOOT + "\n").encode(), (BOOT + "\n").encode(), b"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n")
    )

    def boot_reader() -> bytes:
        return next(boots)

    reader = _reader(
        signed_manifest,
        lambda *_args: _json_lines(*[_row(f"s={i}") for i in range(3)]),
        boot_reader=boot_reader,
    )
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    with pytest.raises(JournalCursorError):
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)


def test_expired_time_window_invalidates_existing_cursor(
    signed_manifest: tuple[Path, bytes],
) -> None:
    reader = _reader(
        signed_manifest,
        lambda *_args: _json_lines(*[_row(f"s={index}") for index in range(3)]),
    )
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    reader.clock = lambda: NOW + timedelta(days=8)
    with pytest.raises(JournalCursorError):
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)


@pytest.mark.parametrize(
    "bad_output",
    [
        _json_lines(_row("s=x", unit=BACKUP)),
        _json_lines(_row("s=x", boot="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
        _json_lines(_row("s=x", boot=BOOT)),
        _json_lines(_row("s=x", priority="8")),
        _json_lines(_row("s=x", timestamp=1)),
        b'{"__CURSOR":"x","__CURSOR":"y"}\n',
        b"\xff\n",
        b"{}",
        b"[1,2]\n",
        _json_lines(_row("s=x"), _row("s=x")),
        _json_lines(_row("s=old", timestamp=1_790_566_799_000_000), _row("s=new")),
        _json_lines(_row("s=x", message="x" * 4097)),
        _json_lines(_row("s=x", MESSAGE=None)),
    ],
)
def test_bad_journal_page_is_atomic_unavailable(
    signed_manifest: tuple[Path, bytes], bad_output: bytes
) -> None:
    reader = _reader(signed_manifest, lambda *_args: bad_output)
    with pytest.raises(JournalUnavailableError, match="运行日志暂不可用"):
        reader.read(unit=DAILY, since=SINCE)


def test_only_registered_events_produce_fixed_text(signed_manifest: tuple[Path, bytes]) -> None:
    token = "Bearer secret-key=123456"
    output = _json_lines(
        _row("s=one", event="run_started"),
        _row("s=two", message=token),
        _row("s=three", event="run_failed", RQUANT_ERROR=token),
        _row("s=four", event="run_succeeded", RQUANT_DURATION_MS="999999999999999999"),
        _row("s=five", event="run_failed", RQUANT_EXIT_CODE="-1"),
        _row("s=six", event="unknown"),
        _row("s=seven", message="rquant.lifecycle\n" + token),
        _row("s=eight", event="run_succeeded", MESSAGE_ID="0" * 32),
        _row("s=nine", event="run_succeeded", RQUANT_DURATION_MS=None),
    )
    page = _reader(signed_manifest, lambda *_args: output).read(unit=DAILY, since=SINCE)
    assert [item.text for item in page.entries] == [
        "任务已开始",
        *(["该条内容暂不可显示"] * 8),
    ]
    dumped = page.model_dump_json()
    assert token not in dumped
    assert "RQUANT_ERROR" not in dumped
    assert "s=one" not in dumped


def test_backup_is_registered_but_other_installed_units_remain_hidden(
    signed_manifest: tuple[Path, bytes],
) -> None:
    backup = _reader(
        signed_manifest, lambda *_args: _json_lines(_row("s=backup", unit=BACKUP))
    ).read(unit=BACKUP, since=SINCE)
    assert [item.text for item in backup.entries] == ["任务已开始"]

    monitor = "rquant-monitor.service"
    other = _reader(
        signed_manifest, lambda *_args: _json_lines(_row("s=monitor", unit=monitor))
    ).read(unit=monitor, since=SINCE)
    assert [item.text for item in other.entries] == ["该条内容暂不可显示"]


def test_manual_invocations_are_not_attributed_to_timer(
    signed_manifest: tuple[Path, bytes],
) -> None:
    page = _reader(
        signed_manifest,
        lambda *_args: _json_lines(
            _row("s=first", _SYSTEMD_INVOCATION_ID="a" * 32),
            _row("s=second", _SYSTEMD_INVOCATION_ID="b" * 32),
        ),
    ).read(unit=DAILY, since=SINCE)
    assert len(page.entries) == 2
    assert not hasattr(page.entries[0], "timer_result")
    assert not hasattr(page.entries[0], "invocation_id")


def test_output_and_time_budgets_fail_closed(signed_manifest: tuple[Path, bytes]) -> None:
    oversized = b"x" * (256 * 1024 + 1)
    with pytest.raises(JournalUnavailableError):
        _reader(signed_manifest, lambda *_args: oversized).read(unit=DAILY, since=SINCE)
    times = iter((1.0, 7.0))
    with pytest.raises(JournalUnavailableError):
        _reader(signed_manifest, lambda *_args: b"", monotonic=lambda: next(times)).read(
            unit=DAILY, since=SINCE
        )


def test_paginated_timeout_and_malformed_data_are_source_failures(
    signed_manifest: tuple[Path, bytes],
) -> None:
    outputs = iter(
        (
            _json_lines(*[_row(f"s={index}") for index in range(3)]),
            b"Bearer secret-key=123456\n",
        )
    )
    reader = _reader(signed_manifest, lambda *_args: next(outputs))
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    with pytest.raises(JournalUnavailableError):
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)

    def fail(*_args: object) -> bytes:
        raise TimeoutError("Bearer secret-key=123456")

    reader = _reader(signed_manifest, fail)
    first = _reader(
        signed_manifest,
        lambda *_args: _json_lines(*[_row(f"s={index}") for index in range(3)]),
    ).read(unit=DAILY, since=SINCE, page_size=2)
    with pytest.raises(JournalUnavailableError) as exc:
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)
    assert "secret-key" not in str(exc.value)


def test_empty_page_and_command_failure_never_include_stderr(
    signed_manifest: tuple[Path, bytes],
) -> None:
    empty = _reader(signed_manifest, lambda *_args: b"").read(unit=DAILY, since=SINCE)
    assert empty.entries == () and empty.next_cursor is None

    def fail(*_args: object) -> bytes:
        raise RuntimeError("Bearer secret-key=123456")

    with pytest.raises(JournalUnavailableError) as exc:
        _reader(signed_manifest, fail).read(unit=DAILY, since=SINCE)
    assert "secret-key" not in str(exc.value)


def test_maximum_page_never_requests_more_than_500_records(
    signed_manifest: tuple[Path, bytes],
) -> None:
    observed: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], _timeout: float, _max_bytes: int) -> bytes:
        observed.append(argv)
        return b""

    page = _reader(signed_manifest, runner).read(unit=DAILY, since=SINCE, page_size=498)
    assert page.entries == ()
    assert "--lines=500" in observed[0]

    rows = _json_lines(*[_row(f"s={index}") for index in range(500)])
    full = _reader(signed_manifest, lambda *_args: rows).read(
        unit=DAILY, since=SINCE, page_size=498
    )
    assert len(full.entries) == 498 and full.next_cursor


def test_manifest_change_invalidates_cursor(signed_manifest: tuple[Path, bytes]) -> None:
    path, _public = signed_manifest
    reader = _reader(
        signed_manifest,
        lambda *_args: _json_lines(*[_row(f"s={index}") for index in range(3)]),
    )
    first = reader.read(unit=DAILY, since=SINCE, page_size=2)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(JournalCursorError):
        reader.read(unit=DAILY, since=SINCE, page_size=2, cursor=first.next_cursor)


def test_manifest_change_during_command_invalidates_whole_page(
    signed_manifest: tuple[Path, bytes],
) -> None:
    path, _public = signed_manifest

    def runner(*_args: object) -> bytes:
        path.write_bytes(path.read_bytes() + b" ")
        return _json_lines(_row("s=first"))

    with pytest.raises(JournalUnavailableError):
        _reader(signed_manifest, runner).read(unit=DAILY, since=SINCE)
