from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

import rquant.runtime_market_session as market_session
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_market_session import (
    MarketCalendarAuthority,
    MarketSessionCalendarError,
    MarketSessionDecision,
    MarketSessionPhase,
    decide_market_session,
    load_market_calendar_authority,
)

COMMIT = "a" * 40
SHANGHAI = ZoneInfo("Asia/Shanghai")
OPEN_DATE = date(2026, 7, 31)


def _authority() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 30),
        coverage_end=date(2026, 8, 3),
        open_dates=(date(2026, 7, 30), OPEN_DATE, date(2026, 8, 3)),
        generated_at=datetime(2026, 7, 29, 16, 0, tzinfo=SHANGHAI),
    )


def _write_authority(path: Path, authority: MarketCalendarAuthority | None = None) -> Path:
    payload = (authority or _authority()).model_dump(mode="json")
    path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _local(hour: int, minute: int, second: int = 0, microsecond: int = 0) -> datetime:
    return datetime(
        OPEN_DATE.year,
        OPEN_DATE.month,
        OPEN_DATE.day,
        hour,
        minute,
        second,
        microsecond,
        tzinfo=SHANGHAI,
    )


def _tree_state(root: Path) -> tuple[tuple[str, int, int, int, int], ...]:
    return tuple(
        sorted(
            (
                str(path.relative_to(root)),
                path.lstat().st_mode,
                path.lstat().st_size,
                path.lstat().st_mtime_ns,
                path.lstat().st_ctime_ns,
            )
            for path in root.rglob("*")
        )
    )


def test_cross_process_contracts_are_frozen_runtime_models() -> None:
    assert issubclass(MarketCalendarAuthority, RuntimeContractModel)
    assert issubclass(MarketSessionDecision, RuntimeContractModel)

    authority = _authority()
    decision = decide_market_session(authority, _local(9, 30))

    with pytest.raises(ValidationError, match="frozen"):
        decision.phase = MarketSessionPhase.CLOSED  # type: ignore[misc]


def test_authority_normalizes_generated_at_and_binds_canonical_content() -> None:
    authority = _authority()

    assert authority.generated_at == datetime(2026, 7, 29, 8, 0, tzinfo=UTC)
    assert len(authority.content_sha256) == 64
    assert authority.exchange == "SSE"

    payload = authority.model_dump(mode="python")
    payload["coverage_end"] = date(2026, 8, 4)
    with pytest.raises(ValidationError, match="content_sha256"):
        MarketCalendarAuthority.model_validate(payload)


@pytest.mark.parametrize(
    ("open_dates", "message"),
    [
        ((OPEN_DATE, OPEN_DATE), "strictly increasing"),
        ((OPEN_DATE, date(2026, 7, 30)), "strictly increasing"),
        ((date(2026, 7, 29), OPEN_DATE), "coverage"),
    ],
)
def test_authority_rejects_duplicate_unsorted_or_out_of_coverage_open_dates(
    open_dates: tuple[date, ...], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        MarketCalendarAuthority.create(
            schema_version=1,
            exchange="SSE",
            producer_commit=COMMIT,
            coverage_start=date(2026, 7, 30),
            coverage_end=date(2026, 8, 3),
            open_dates=open_dates,
            generated_at=datetime(2026, 7, 29, 8, 0, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("observed_at", "phase", "may_fetch"),
    [
        (_local(0, 0), MarketSessionPhase.PRE_OPEN, False),
        (_local(9, 29, 59, 999999), MarketSessionPhase.PRE_OPEN, False),
        (_local(9, 30), MarketSessionPhase.MORNING, True),
        (_local(11, 30), MarketSessionPhase.MORNING, True),
        (_local(11, 30, 0, 1), MarketSessionPhase.LUNCH, False),
        (_local(12, 59, 59, 999999), MarketSessionPhase.LUNCH, False),
        (_local(13, 0), MarketSessionPhase.AFTERNOON, True),
        (_local(15, 0), MarketSessionPhase.AFTERNOON, True),
        (_local(15, 0, 0, 1), MarketSessionPhase.CLOSED, False),
        (_local(23, 59, 59, 999999), MarketSessionPhase.CLOSED, False),
    ],
)
def test_market_session_phase_has_explicit_microsecond_boundaries(
    observed_at: datetime,
    phase: MarketSessionPhase,
    may_fetch: bool,
) -> None:
    decision = decide_market_session(_authority(), observed_at)

    assert decision.observed_at == observed_at.astimezone(UTC)
    assert decision.local_trade_date == OPEN_DATE
    assert decision.phase is phase
    assert decision.is_open_date is True
    assert decision.may_fetch_market_minute is may_fetch


def test_utc_input_is_converted_to_shanghai_before_decision() -> None:
    decision = decide_market_session(_authority(), datetime(2026, 7, 31, 1, 30, tzinfo=UTC))

    assert decision.local_trade_date == OPEN_DATE
    assert decision.phase is MarketSessionPhase.MORNING
    assert decision.may_fetch_market_minute is True


@pytest.mark.parametrize("closed_date", [date(2026, 8, 1), date(2026, 8, 2)])
def test_non_open_dates_are_always_closed(closed_date: date) -> None:
    decision = decide_market_session(
        _authority(), datetime.combine(closed_date, datetime.min.time(), SHANGHAI).replace(hour=10)
    )

    assert decision.phase is MarketSessionPhase.CLOSED
    assert decision.is_open_date is False
    assert decision.may_fetch_market_minute is False


def test_naive_observation_and_calendar_range_miss_fail_closed() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        decide_market_session(_authority(), datetime(2026, 7, 31, 9, 30))

    with pytest.raises(MarketSessionCalendarError, match="coverage"):
        decide_market_session(_authority(), datetime(2026, 8, 4, 10, 0, tzinfo=SHANGHAI))
    with pytest.raises(MarketSessionCalendarError, match="coverage"):
        decide_market_session(_authority(), datetime(2026, 7, 29, 10, 0, tzinfo=SHANGHAI))


def test_future_generated_calendar_and_inconsistent_decision_fail_closed() -> None:
    future = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=OPEN_DATE,
        coverage_end=OPEN_DATE,
        open_dates=(OPEN_DATE,),
        generated_at=_local(10, 0),
    )
    with pytest.raises(MarketSessionCalendarError, match="generated after"):
        decide_market_session(future, _local(9, 30))

    with pytest.raises(ValidationError, match="conflicts"):
        MarketSessionDecision(
            observed_at=_local(10, 0),
            local_trade_date=OPEN_DATE,
            phase=MarketSessionPhase.MORNING,
            is_open_date=True,
            may_fetch_market_minute=False,
        )
    with pytest.raises(ValidationError, match="local_trade_date"):
        MarketSessionDecision(
            observed_at=_local(10, 0),
            local_trade_date=OPEN_DATE.replace(day=30),
            phase=MarketSessionPhase.MORNING,
            is_open_date=True,
            may_fetch_market_minute=True,
        )
    with pytest.raises(ValidationError, match="phase"):
        MarketSessionDecision(
            observed_at=_local(10, 0),
            local_trade_date=OPEN_DATE,
            phase=MarketSessionPhase.AFTERNOON,
            is_open_date=True,
            may_fetch_market_minute=True,
        )


def test_loader_accepts_only_absolute_normalized_private_owned_regular_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_authority(tmp_path / "calendar.json")

    loaded = load_market_calendar_authority(path, expected_commit=COMMIT)
    assert loaded == _authority()

    with pytest.raises(ValueError, match="absolute normalized"):
        load_market_calendar_authority(Path("calendar.json"), expected_commit=COMMIT)
    with pytest.raises(ValueError, match="absolute normalized"):
        load_market_calendar_authority(
            tmp_path / "nested" / ".." / "calendar.json",
            expected_commit=COMMIT,
        )
    with pytest.raises(ValueError, match="40-character"):
        load_market_calendar_authority(path, expected_commit="bad")

    path.chmod(0o644)
    with pytest.raises(MarketSessionCalendarError, match="0600"):
        load_market_calendar_authority(path, expected_commit=COMMIT)
    path.chmod(0o600)

    actual_euid = os.geteuid()
    monkeypatch.setattr(market_session.os, "geteuid", lambda: actual_euid + 1)
    with pytest.raises(MarketSessionCalendarError, match="owner"):
        load_market_calendar_authority(path, expected_commit=COMMIT)


def test_loader_rejects_missing_symlink_and_ancestor_symlink(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(MarketSessionCalendarError, match="unavailable"):
        load_market_calendar_authority(missing, expected_commit=COMMIT)

    target = _write_authority(tmp_path / "target.json")
    link = tmp_path / "calendar.json"
    link.symlink_to(target)
    with pytest.raises(MarketSessionCalendarError, match="symlink"):
        load_market_calendar_authority(link, expected_commit=COMMIT)

    physical = tmp_path / "physical"
    physical.mkdir()
    nested = _write_authority(physical / "calendar.json")
    assert nested.exists()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(physical, target_is_directory=True)
    with pytest.raises(MarketSessionCalendarError, match="symlink"):
        load_market_calendar_authority(linked_parent / "calendar.json", expected_commit=COMMIT)


def test_loader_rejects_non_regular_hardlinked_and_oversized_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "calendar-dir"
    directory.mkdir()
    with pytest.raises(MarketSessionCalendarError, match="regular file"):
        load_market_calendar_authority(directory, expected_commit=COMMIT)

    path = _write_authority(tmp_path / "calendar.json")
    hardlink = tmp_path / "calendar-copy.json"
    os.link(path, hardlink)
    with pytest.raises(MarketSessionCalendarError, match="hard link"):
        load_market_calendar_authority(path, expected_commit=COMMIT)
    hardlink.unlink()

    monkeypatch.setattr(market_session, "_MAX_CALENDAR_BYTES", 16)
    with pytest.raises(MarketSessionCalendarError, match="size limit"):
        load_market_calendar_authority(path, expected_commit=COMMIT)


def test_loader_rejects_duplicate_json_keys_hash_tamper_and_commit_drift(
    tmp_path: Path,
) -> None:
    path = tmp_path / "calendar.json"
    path.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(MarketSessionCalendarError, match="duplicate JSON key"):
        load_market_calendar_authority(path, expected_commit=COMMIT)

    for malformed in ("", "{", "[]"):
        path.write_text(malformed, encoding="utf-8")
        with pytest.raises(MarketSessionCalendarError, match="invalid calendar"):
            load_market_calendar_authority(path, expected_commit=COMMIT)

    authority = _authority()
    payload = authority.model_dump(mode="json")
    payload["open_dates"] = ["2026-07-30", "2026-08-03"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MarketSessionCalendarError, match="content_sha256"):
        load_market_calendar_authority(path, expected_commit=COMMIT)

    _write_authority(path, authority)
    with pytest.raises(MarketSessionCalendarError, match="producer_commit"):
        load_market_calendar_authority(path, expected_commit="b" * 40)


def test_loader_detects_file_change_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_authority(tmp_path / "calendar.json")
    original_read = market_session.os.read
    changed = False

    def mutating_read(descriptor: int, size: int) -> bytes:
        nonlocal changed
        chunk = original_read(descriptor, size)
        if chunk and not changed:
            changed = True
            replacement = chunk.replace(b'"SSE"', b'"SZSE"', 1)
            writer = os.open(path, os.O_WRONLY)
            try:
                os.pwrite(writer, replacement, 0)
                os.fsync(writer)
            finally:
                os.close(writer)
        return chunk

    monkeypatch.setattr(market_session.os, "read", mutating_read)
    with pytest.raises(MarketSessionCalendarError, match="changed while reading"):
        load_market_calendar_authority(path, expected_commit=COMMIT)


def test_loader_wraps_authority_unlink_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_authority(tmp_path / "calendar.json")
    original_read = market_session.os.read
    removed = False

    def removing_read(descriptor: int, size: int) -> bytes:
        nonlocal removed
        chunk = original_read(descriptor, size)
        if chunk and not removed:
            removed = True
            path.unlink()
        return chunk

    monkeypatch.setattr(market_session.os, "read", removing_read)
    with pytest.raises(MarketSessionCalendarError, match="changed while reading"):
        load_market_calendar_authority(path, expected_commit=COMMIT)


def test_loader_lifecycle_performs_zero_filesystem_writes(tmp_path: Path) -> None:
    path = _write_authority(tmp_path / "calendar.json")
    before = _tree_state(tmp_path)

    authority = load_market_calendar_authority(path, expected_commit=COMMIT)
    decide_market_session(authority, _local(10, 7))

    assert _tree_state(tmp_path) == before
