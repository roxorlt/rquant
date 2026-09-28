"""Manual systemd run admission is a deterministic, closed decision."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.unit_run_admission import (
    UnitEffect,
    UnitRunPolicy,
    UnitRunRefusal,
    UnitRunSnapshot,
    admit_unit_run,
)

SHANGHAI = timezone(timedelta(hours=8))
SESSION = date(2026, 9, 29)
UNIT = "rquant-daily.service"


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 29, hour, minute, second, tzinfo=SHANGHAI)


def _policy(**updates: object) -> UnitRunPolicy:
    fields: dict[str, object] = {
        "unit_name": UNIT,
        "enabled": True,
        "effect": UnitEffect.WRITES_PRIMARY,
        "window_start": time(8),
        "window_end": time(18),
        "min_interval_seconds": 300,
    }
    fields.update(updates)
    return UnitRunPolicy(**fields)


def _snapshot(**updates: object) -> UnitRunSnapshot:
    fields: dict[str, object] = {
        "requested_unit": UNIT,
        "policy": _policy(),
        "evaluated_at": _at(8, 30),
        "sse_session_date": SESSION,
        "is_sse_trading_day": True,
        "systemd_active": False,
        "systemd_running": False,
        "last_start_evidence_available": True,
        "last_started_at": _at(8, 20),
        "writer_mutex_held": False,
    }
    fields.update(updates)
    return UnitRunSnapshot(**fields)


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"requested_unit": "arbitrary.service"}, UnitRunRefusal.UNIT_NOT_ALLOWED),
        ({"requested_unit": "rquant-daily"}, UnitRunRefusal.UNIT_NOT_ALLOWED),
        ({"requested_unit": "rquant-daily-alias.service"}, UnitRunRefusal.UNIT_NOT_ALLOWED),
        ({"policy": None}, UnitRunRefusal.UNIT_NOT_ALLOWED),
        ({"policy": _policy(enabled=False)}, UnitRunRefusal.POLICY_DISABLED),
        ({"evaluated_at": None}, UnitRunRefusal.CLOCK_UNAVAILABLE),
        ({"sse_session_date": None}, UnitRunRefusal.CALENDAR_UNAVAILABLE),
        ({"is_sse_trading_day": None}, UnitRunRefusal.CALENDAR_UNAVAILABLE),
        ({"sse_session_date": date(2026, 9, 28)}, UnitRunRefusal.CALENDAR_MISMATCH),
        ({"systemd_active": True, "systemd_running": True}, UnitRunRefusal.UNIT_ACTIVE),
        ({"systemd_active": True, "systemd_running": False}, UnitRunRefusal.UNIT_ACTIVE),
        ({"systemd_active": None}, UnitRunRefusal.UNIT_STATE_UNKNOWN),
        ({"systemd_running": None}, UnitRunRefusal.UNIT_STATE_UNKNOWN),
        (
            {"systemd_active": False, "systemd_running": True},
            UnitRunRefusal.UNIT_STATE_CONFLICT,
        ),
        ({"evaluated_at": _at(7, 59, 59)}, UnitRunRefusal.OUTSIDE_WINDOW),
        ({"evaluated_at": _at(18, 0)}, UnitRunRefusal.OUTSIDE_WINDOW),
        (
            {"last_start_evidence_available": False, "last_started_at": None},
            UnitRunRefusal.LAST_START_UNKNOWN,
        ),
        (
            {"last_start_evidence_available": None, "last_started_at": None},
            UnitRunRefusal.LAST_START_UNKNOWN,
        ),
        (
            {"last_start_evidence_available": False, "last_started_at": _at(8, 20)},
            UnitRunRefusal.LAST_START_CONFLICT,
        ),
        ({"last_started_at": _at(8, 31)}, UnitRunRefusal.LAST_START_IN_FUTURE),
        ({"last_started_at": _at(8, 25, 1)}, UnitRunRefusal.MIN_INTERVAL),
        ({"writer_mutex_held": None}, UnitRunRefusal.WRITER_STATE_UNKNOWN),
        ({"writer_mutex_held": True}, UnitRunRefusal.WRITER_BUSY),
        ({"evaluated_at": _at(9, 15)}, UnitRunRefusal.TRADING_SESSION_WRITE_BLOCKED),
        ({"evaluated_at": _at(15, 10)}, UnitRunRefusal.TRADING_SESSION_WRITE_BLOCKED),
    ],
)
def test_closed_refusals(updates: dict[str, object], reason: UnitRunRefusal) -> None:
    decision = admit_unit_run(_snapshot(**updates))

    assert decision.allowed is False
    assert decision.reason is reason
    assert decision.model_dump(mode="json") == {"allowed": False, "reason": reason.value}


@pytest.mark.parametrize(
    "updates",
    [
        {},
        {"evaluated_at": _at(8, 0), "last_started_at": None},
        {"evaluated_at": _at(18, 0) - timedelta(microseconds=1)},
        {"last_started_at": _at(8, 25)},
        {"evaluated_at": _at(9, 14, 59)},
        {"evaluated_at": _at(15, 10, 1)},
        {"evaluated_at": _at(9, 15), "is_sse_trading_day": False},
        {"evaluated_at": _at(15, 10), "is_sse_trading_day": False},
        {"policy": _policy(effect=UnitEffect.READ_ONLY), "writer_mutex_held": None},
        {"evaluated_at": _at(8, 30).astimezone(UTC)},
    ],
)
def test_allowed_boundaries_and_read_only(updates: dict[str, object]) -> None:
    decision = admit_unit_run(_snapshot(**updates))

    assert decision.allowed is True
    assert decision.reason is None
    assert decision.model_dump(mode="json") == {"allowed": True, "reason": None}


def test_same_snapshot_has_same_result_and_utc_normalization() -> None:
    snapshot = _snapshot(evaluated_at=_at(8, 30))

    assert snapshot.evaluated_at == _at(8, 30).astimezone(UTC)
    assert snapshot.evaluated_at.tzinfo is UTC
    assert admit_unit_run(snapshot) == admit_unit_run(snapshot)
    assert admit_unit_run(snapshot) == admit_unit_run(
        _snapshot(evaluated_at=_at(8, 30).astimezone(UTC))
    )


def test_read_only_still_requires_known_systemd_state() -> None:
    snapshot = _snapshot(
        policy=_policy(effect=UnitEffect.READ_ONLY),
        evaluated_at=_at(9, 15),
        systemd_running=None,
        writer_mutex_held=None,
    )

    assert admit_unit_run(snapshot).reason is UnitRunRefusal.UNIT_STATE_UNKNOWN
    assert admit_unit_run(
        _snapshot(
            policy=_policy(effect=UnitEffect.READ_ONLY),
            evaluated_at=_at(9, 15),
            writer_mutex_held=None,
        )
    ).allowed


def test_known_nontrading_day_still_obeys_own_window() -> None:
    nontrading_day = date(2026, 9, 27)
    in_window = _snapshot(
        evaluated_at=datetime(2026, 9, 27, 10, tzinfo=SHANGHAI),
        sse_session_date=nontrading_day,
        is_sse_trading_day=False,
        last_started_at=None,
    )
    outside_window = _snapshot(
        evaluated_at=datetime(2026, 9, 27, 18, tzinfo=SHANGHAI),
        sse_session_date=nontrading_day,
        is_sse_trading_day=False,
        last_started_at=None,
    )

    assert admit_unit_run(in_window).allowed
    assert admit_unit_run(outside_window).reason is UnitRunRefusal.OUTSIDE_WINDOW


@pytest.mark.parametrize(
    "updates",
    [
        {"unit_name": "rquant-daily"},
        {"unit_name": "../rquant-daily.service"},
        {"unit_name": "rquant-daily.service/other"},
        {"window_start": time(18), "window_end": time(8)},
        {"window_start": time(8), "window_end": time(8)},
        {"window_start": time(8, tzinfo=SHANGHAI)},
        {"min_interval_seconds": 0},
        {"min_interval_seconds": True},
    ],
)
def test_policy_rejects_unbounded_or_invalid_terms(updates: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _policy(**updates)


def test_snapshot_rejects_naive_clock_and_policy_cannot_be_mutated() -> None:
    with pytest.raises(ValidationError):
        _snapshot(evaluated_at=datetime(2026, 9, 29, 8, 30))
    policy = _policy()
    with pytest.raises(ValidationError):
        policy.enabled = False
