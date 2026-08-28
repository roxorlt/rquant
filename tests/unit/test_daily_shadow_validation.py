from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

NOW = datetime(2026, 8, 3, 10, 0, tzinfo=UTC)
TRADE_DATE = date(2026, 8, 3)
COMMIT = "a" * 40
PROFILE = "b" * 64
GENERATION = "c" * 64


def _calendar(*, open_dates: tuple[date, ...]):
    from rquant.runtime_market_session import MarketCalendarAuthority

    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=open_dates,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )


def _snapshot(
    *,
    source: str,
    value: str = "same",
    available_at: datetime = NOW - timedelta(minutes=1),
    revises_content_hash: str | None = None,
    trade_date: date = TRADE_DATE,
):
    from rquant.daily_shadow_validation import (
        DailyShadowDataset,
        DailyShadowRecord,
        DailyShadowSnapshot,
    )

    return DailyShadowSnapshot(
        source=source,
        trade_date=trade_date,
        dataset=DailyShadowDataset.DAILY_BAR,
        source_generation_id=GENERATION,
        available_at=available_at,
        records=(
            DailyShadowRecord(
                key=("600000.SH", trade_date.isoformat()),
                content_hash={"same": "a", "a": "a", "b": "b"}[value] * 64,
                available_at=available_at,
                revises_content_hash=revises_content_hash,
            ),
        ),
    )


def _session(*, origin: str = "production", snapshots=()):
    from rquant.daily_shadow_validation import DailyShadowSession

    return DailyShadowSession(
        evidence_origin=origin,
        trade_date=TRADE_DATE,
        captured_at=NOW,
        session_closed_at=NOW - timedelta(hours=2),
        deadline_at=NOW + timedelta(minutes=30),
        code_commit=COMMIT,
        legacy_profile_hash=PROFILE,
        dag_profile_hash=PROFILE,
        legacy_data_generation=GENERATION,
        dag_data_generation=GENERATION,
        snapshots=tuple(snapshots),
    )


def _signer():
    from rquant.daily_shadow_validation import DailyShadowHmacSigner

    return DailyShadowHmacSigner(key_id="test-shadow", secret=b"x" * 32)


def _comparator():
    from rquant.daily_shadow_validation import DailyShadowComparator, DailyShadowDataset

    return DailyShadowComparator(required_datasets=(DailyShadowDataset.DAILY_BAR,))


def test_comparator_produces_signed_immutable_report_without_future_rows(tmp_path: Path) -> None:
    from rquant.daily_shadow_validation import DailyShadowReportStore

    report = _comparator().compare(
        _session(snapshots=(_snapshot(source="legacy"), _snapshot(source="dag")))
    )
    store = DailyShadowReportStore(tmp_path, signer=_signer())

    stored = store.publish(report)

    assert stored.passed is True
    assert store.load(TRADE_DATE) == stored
    assert store.load(TRADE_DATE).signature == stored.signature


def test_signed_shadow_report_rejects_signature_tampering(tmp_path: Path) -> None:
    from rquant.daily_shadow_validation import (
        DailyShadowReportStore,
        DailyShadowValidationError,
    )

    store = DailyShadowReportStore(tmp_path, signer=_signer())
    store.publish(
        _comparator().compare(
            _session(snapshots=(_snapshot(source="legacy"), _snapshot(source="dag")))
        )
    )
    path = tmp_path / TRADE_DATE.isoformat() / "report.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["signature"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DailyShadowValidationError, match="signature"):
        store.load(TRADE_DATE)


def test_comparator_classifies_delay_revision_and_unknown_differences() -> None:

    legacy = _snapshot(source="legacy", value="a")
    delayed = _snapshot(source="dag", value="a", available_at=NOW)
    delay_report = _comparator().compare(_session(snapshots=(legacy, delayed)))
    assert delay_report.discrepancies[0].category == "data_delay"

    revision = _snapshot(
        source="dag",
        value="b",
        revises_content_hash=("a" * 64),
    )
    revision_report = _comparator().compare(_session(snapshots=(legacy, revision)))
    assert revision_report.discrepancies[0].category == "legitimate_revision"

    missing_report = _comparator().compare(_session(snapshots=(legacy,)))
    assert missing_report.discrepancies[0].category == "unknown"
    assert missing_report.passed is False


def test_future_input_is_rejected_before_comparison() -> None:
    from rquant.daily_shadow_validation import DailyShadowValidationError

    future = _snapshot(source="dag", available_at=NOW + timedelta(seconds=1))
    with pytest.raises(DailyShadowValidationError, match="future"):
        _comparator().compare(_session(snapshots=(_snapshot(source="legacy"), future)))


def test_production_comparator_requires_every_daily_stage_snapshot() -> None:
    from rquant.daily_shadow_validation import DailyShadowComparator

    report = DailyShadowComparator().compare(
        _session(snapshots=(_snapshot(source="legacy"), _snapshot(source="dag")))
    )

    assert report.passed is False
    assert report.discrepancy_counts["unknown"] == 5


def test_retirement_gate_requires_ten_consecutive_real_frozen_days(tmp_path: Path) -> None:
    from rquant.daily_shadow_validation import (
        DailyRetirementGate,
        DailyRetirementGateConfig,
        DailyShadowReportStore,
        DailyShadowSession,
    )

    store = DailyShadowReportStore(tmp_path, signer=_signer())
    days = (
        date(2026, 7, 20),
        date(2026, 7, 21),
        date(2026, 7, 22),
        date(2026, 7, 23),
        date(2026, 7, 24),
        date(2026, 7, 27),
        date(2026, 7, 28),
        date(2026, 7, 29),
        date(2026, 7, 30),
        date(2026, 7, 31),
    )
    for day in days:
        session = DailyShadowSession(
            evidence_origin="production",
            trade_date=day,
            captured_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC)
            + timedelta(hours=10),
            session_closed_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC)
            + timedelta(hours=8),
            deadline_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC)
            + timedelta(hours=12),
            code_commit=COMMIT,
            legacy_profile_hash=PROFILE,
            dag_profile_hash=PROFILE,
            legacy_data_generation=GENERATION,
            dag_data_generation=GENERATION,
            snapshots=(
                _snapshot(
                    source="legacy",
                    trade_date=day,
                    available_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC)
                    + timedelta(hours=9),
                ),
                _snapshot(
                    source="dag",
                    trade_date=day,
                    available_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC)
                    + timedelta(hours=9),
                ),
            ),
        )
        store.publish(_comparator().compare(session))

    decision = DailyRetirementGate(
        DailyRetirementGateConfig(minimum_real_trading_days=10)
    ).evaluate(store, expected_trade_dates=days, calendar=_calendar(open_dates=days))

    assert decision.eligible is True
    assert decision.counted_trade_dates == days


def test_retirement_gate_rejects_non_trading_calendar_inputs(tmp_path: Path) -> None:
    from rquant.daily_shadow_validation import DailyRetirementGate, DailyShadowReportStore

    calendar = _calendar(
        open_dates=(
            date(2026, 7, 20),
            date(2026, 7, 21),
            date(2026, 7, 22),
            date(2026, 7, 23),
            date(2026, 7, 27),
            date(2026, 7, 28),
            date(2026, 7, 29),
            date(2026, 7, 30),
            date(2026, 7, 31),
        )
    )

    decision = DailyRetirementGate().evaluate(
        DailyShadowReportStore(tmp_path, signer=_signer()),
        expected_trade_dates=(date(2026, 7, 24),) * 10,
        calendar=calendar,
    )

    assert decision.eligible is False
    assert decision.reasons == ("non_sse_open_date:2026-07-24",)


def test_retirement_gate_rejects_missing_sessions_and_frozen_identity_drift(tmp_path: Path) -> None:
    from rquant.daily_shadow_validation import (
        DailyRetirementGate,
        DailyRetirementGateConfig,
        DailyShadowReportStore,
        DailyShadowSession,
    )

    days = (
        date(2026, 7, 20),
        date(2026, 7, 21),
        date(2026, 7, 22),
        date(2026, 7, 23),
        date(2026, 7, 24),
        date(2026, 7, 27),
        date(2026, 7, 28),
        date(2026, 7, 29),
        date(2026, 7, 30),
        date(2026, 7, 31),
    )

    def publish(store: DailyShadowReportStore, day: date, *, code_commit: str = COMMIT) -> None:
        opened = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
        session = DailyShadowSession(
            evidence_origin="production",
            trade_date=day,
            captured_at=opened + timedelta(hours=10),
            session_closed_at=opened + timedelta(hours=8),
            deadline_at=opened + timedelta(hours=12),
            code_commit=code_commit,
            legacy_profile_hash=PROFILE,
            dag_profile_hash=PROFILE,
            legacy_data_generation=GENERATION,
            dag_data_generation=GENERATION,
            snapshots=(
                _snapshot(
                    source="legacy", trade_date=day, available_at=opened + timedelta(hours=9)
                ),
                _snapshot(source="dag", trade_date=day, available_at=opened + timedelta(hours=9)),
            ),
        )
        store.publish(_comparator().compare(session))

    gate = DailyRetirementGate(DailyRetirementGateConfig(minimum_real_trading_days=10))
    missing_store = DailyShadowReportStore(tmp_path / "missing", signer=_signer())
    for day in (*days[:4], *days[5:]):
        publish(missing_store, day)
    assert gate.evaluate(
        missing_store, expected_trade_dates=days, calendar=_calendar(open_dates=days)
    ).reasons == ("missing_report:2026-07-24",)

    drift_store = DailyShadowReportStore(tmp_path / "drift", signer=_signer())
    for day in days:
        publish(drift_store, day, code_commit="d" * 40 if day == days[-1] else COMMIT)
    drift = gate.evaluate(
        drift_store, expected_trade_dates=days, calendar=_calendar(open_dates=days)
    )

    assert drift.eligible is False
    assert drift.reasons == ("frozen_identity_changed:2026-07-31",)


def test_fixture_and_duplicate_or_late_reports_cannot_count_toward_retirement(
    tmp_path: Path,
) -> None:
    from rquant.daily_shadow_validation import (
        DailyRetirementGate,
        DailyRetirementGateConfig,
        DailyShadowComparator,
        DailyShadowReportConflictError,
        DailyShadowReportStore,
        DailyShadowSession,
        DailyShadowValidationError,
    )

    store = DailyShadowReportStore(tmp_path, signer=_signer())
    fixture = _comparator().compare(
        _session(
            origin="historical_fixture",
            snapshots=(_snapshot(source="legacy"), _snapshot(source="dag")),
        )
    )
    store.publish(fixture)
    with pytest.raises(DailyShadowReportConflictError):
        store.publish(fixture)
    decision = DailyRetirementGate(
        DailyRetirementGateConfig(minimum_real_trading_days=10)
    ).evaluate(
        store,
        expected_trade_dates=(TRADE_DATE,),
        calendar=_calendar(open_dates=(TRADE_DATE,)),
    )
    assert decision.eligible is False
    assert decision.counted_trade_dates == ()

    late_session = _session(snapshots=(_snapshot(source="legacy"), _snapshot(source="dag")))
    payload = late_session.model_dump(mode="python")
    payload["captured_at"] = late_session.deadline_at + timedelta(seconds=1)
    with pytest.raises(DailyShadowValidationError, match="deadline"):
        DailyShadowComparator().compare(DailyShadowSession.model_validate(payload))
