"""The old per-target JSONL must cross the page boundary only as safe facts."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant import serving_page_projection_source as source_module
from rquant.serving_page_projection_source import PageProjectionSourceIntegrityError

NOW = datetime(2026, 9, 24, 8, 0, tzinfo=UTC)
SECRET = "SECRET-CANARY-not-for-serving"


def _line(
    *,
    sent_at: str = "2026-09-24T09:47:00.123456",
    scene: str = "price_level",
    channel: str = "pushdeer",
    success: bool = True,
) -> str:
    return json.dumps(
        {
            "sent_at": sent_at,
            "scene": scene,
            "channel": channel,
            "target": SECRET,
            "success": success,
            "error_msg": SECRET,
            "title": SECRET,
        },
        ensure_ascii=False,
    ) + "\n"


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    stamp = (NOW - timedelta(seconds=2)).timestamp()
    os.utime(path, (stamp, stamp))


def test_reader_whitelists_fields_and_tracks_unknown_source_rows(tmp_path: Path) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(
        path,
        _line() + _line(channel="pushplus", success=False) + _line(scene="unknown-secret"),
    )

    projections = source_module._read_legacy_notification_projections(path, observed=NOW)

    assert projections is not None
    records, status = projections
    assert len(records.rows) == 2
    assert status.rows == ({"snapshot_key": "current", "state": "partial", "skipped": 1},)
    assert {row["channel_label"] for row in records.rows} == {"PushDeer", "PushPlus"}
    assert {row["submitted"] for row in records.rows} == {True, False}
    assert {row["sent_at"] for row in records.rows} == {"2026-09-24T01:47:00.123456+00:00"}
    assert len({row["record_key"] for row in records.rows}) == 2
    assert SECRET not in repr(projections)
    assert "unknown-secret" not in repr(projections)
    assert not any("scene" in row or "channel" in row for row in records.rows)


@pytest.mark.parametrize(
    "content",
    [
        _line()[:-1],
        _line() + "{broken}\n",
        _line(sent_at="2026-09-25T09:47:00"),
        _line(sent_at="invalid"),
        _line(sent_at="0001-01-01T00:00:00"),
    ],
    ids=("half-line", "invalid-json", "future-time", "invalid-time", "overflow-time"),
)
def test_incomplete_invalid_or_future_file_is_not_published(tmp_path: Path, content: str) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(path, content)

    with pytest.raises(PageProjectionSourceIntegrityError) as error:
        source_module._read_legacy_notification_projections(path, observed=NOW)

    assert SECRET not in str(error.value)
    assert content not in str(error.value)


def test_missing_file_and_directory_are_not_published(tmp_path: Path) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    assert source_module._read_legacy_notification_projections(path, observed=NOW) is None
    path.parent.mkdir()
    assert source_module._read_legacy_notification_projections(path, observed=NOW) is None


def test_symlink_and_size_bound_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    target = tmp_path / "elsewhere.jsonl"
    _write(target, _line())
    path.parent.mkdir()
    path.symlink_to(target)
    with pytest.raises(PageProjectionSourceIntegrityError):
        source_module._read_legacy_notification_projections(path, observed=NOW)
    path.unlink()
    _write(path, _line())
    monkeypatch.setattr(source_module, "_MAX_LEGACY_NOTIFICATION_BYTES", 32)
    with pytest.raises(PageProjectionSourceIntegrityError):
        source_module._read_legacy_notification_projections(path, observed=NOW)
    monkeypatch.setattr(source_module, "_MAX_LEGACY_NOTIFICATION_BYTES", 8 * 1024 * 1024)
    monkeypatch.setattr(source_module, "_MAX_EVENT_ROWS", 1)
    _write(path, _line() + _line())
    with pytest.raises(PageProjectionSourceIntegrityError, match="row bound"):
        source_module._read_legacy_notification_projections(path, observed=NOW)


def test_same_inode_rewrite_with_restored_mtime_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(path, _line())
    stamp = (NOW - timedelta(seconds=2)).timestamp()
    original = source_module._read_bound_optional_file

    def rewrite_after_read(*args: object, **kwargs: object) -> object:
        result = original(*args, **kwargs)
        path.write_text(_line(success=False), encoding="utf-8")
        os.utime(path, (stamp, stamp))
        return result

    monkeypatch.setattr(source_module, "_read_bound_optional_file", rewrite_after_read)
    with pytest.raises(PageProjectionSourceIntegrityError):
        source_module._read_legacy_notification_projections(path, observed=NOW)


def test_append_after_read_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(path, _line())
    original = source_module._read_bound_optional_file

    def append_after_read(*args: object, **kwargs: object) -> object:
        result = original(*args, **kwargs)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(_line(success=False))
        return result

    monkeypatch.setattr(source_module, "_read_bound_optional_file", append_after_read)
    with pytest.raises(PageProjectionSourceIntegrityError, match="changed while read"):
        source_module._read_legacy_notification_projections(path, observed=NOW)


def test_duplicate_submission_attempts_have_distinct_stable_keys(tmp_path: Path) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(path, _line() + _line())
    first = source_module._read_legacy_notification_projections(path, observed=NOW)
    second = source_module._read_legacy_notification_projections(path, observed=NOW)
    assert first is not None and second is not None
    keys = [row["record_key"] for row in first[0].rows]
    assert len(set(keys)) == 2
    assert keys == [row["record_key"] for row in second[0].rows]


def test_failed_source_logs_no_raw_values(tmp_path: Path) -> None:
    from loguru import logger

    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(path, "{SECRET-CANARY-not-for-serving}\n")
    messages: list[str] = []
    handler = logger.add(lambda message: messages.append(message.record["message"]))
    try:
        source = source_module.DuckDBSignalPageProjectionSource(
            tmp_path / "unused.duckdb", notification_log_path=path
        )
        records, status = source.legacy_notification_projections(NOW)
    finally:
        logger.remove(handler)
    assert records is None
    assert status is not None and status.rows[0]["state"] == "unavailable"
    assert SECRET not in repr(status)
    assert SECRET not in "\n".join(messages)


def test_shanghai_natural_day_window_excludes_prior_local_day(tmp_path: Path) -> None:
    path = tmp_path / "logs" / "notification_log.jsonl"
    _write(
        path,
        _line(sent_at="2026-08-25T23:59:59")
        + _line(sent_at="2026-08-26T00:00:00")
        + _line(sent_at="2026-09-24T15:59:59"),
    )
    records, status = source_module._read_legacy_notification_projections(path, observed=NOW)
    assert len(records.rows) == 2
    assert status.rows[0]["state"] == "complete"
    assert {row["sent_at"] for row in records.rows} == {
        "2026-08-25T16:00:00+00:00",
        "2026-09-24T07:59:59+00:00",
    }
