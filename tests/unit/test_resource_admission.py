from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.research_run_spec import ResourceClass
from rquant.resource_admission import (
    AdmissionDecision,
    AdmissionOutcome,
    AdmissionPolicy,
    AdmissionRequest,
    ResourceSnapshot,
    SourceQuotaLease,
    TradingSession,
    evaluate_admission,
)

OBSERVED_AT = datetime(2026, 7, 31, 2, 0, tzinfo=UTC)


def _snapshot(**overrides: object) -> ResourceSnapshot:
    payload: dict[str, object] = {
        "observed_at": OBSERVED_AT,
        "session": TradingSession.POST_MARKET,
        "live_backlog_age_seconds": 1.0,
        "live_p95_latency_seconds": 0.5,
        "available_memory_bytes": 8_000,
        "available_disk_bytes": 80_000,
        "io_pressure_pct": 10.0,
        "cpu_load_pct": 20.0,
        "source_quota_remaining": 100,
        "live_healthy": True,
    }
    payload.update(overrides)
    return ResourceSnapshot(**payload)


def _policy(**overrides: object) -> AdmissionPolicy:
    payload: dict[str, object] = {
        "allow_live_session": False,
        "max_live_backlog_age_seconds": 10.0,
        "max_live_p95_latency_seconds": 5.0,
        "min_available_memory_bytes": 1_000,
        "min_available_disk_bytes": 10_000,
        "max_io_pressure_pct": 80.0,
        "max_cpu_load_pct": 85.0,
        "max_expected_memory_bytes": 4_000,
        "max_expected_disk_bytes": 40_000,
        "max_expected_quota_units": 50,
        "retry_delay_seconds": 60,
    }
    payload.update(overrides)
    return AdmissionPolicy(**payload)


def _request(**overrides: object) -> AdmissionRequest:
    payload: dict[str, object] = {
        "job_id": "job-1",
        "resource_class": ResourceClass.STANDARD,
        "expected_memory_bytes": 2_000,
        "expected_disk_bytes": 20_000,
        "expected_quota_units": 10,
        "source": "tushare",
        "preemptible": True,
        "read_only": True,
        "deadline": OBSERVED_AT + timedelta(hours=1),
    }
    payload.update(overrides)
    return AdmissionRequest(**payload)


def _lease(**overrides: object) -> SourceQuotaLease:
    payload: dict[str, object] = {
        "source": "tushare",
        "owner": "job-1",
        "units": 10,
        "granted_at": OBSERVED_AT - timedelta(seconds=5),
        "expires_at": OBSERVED_AT + timedelta(minutes=5),
        "quota_reset_at": OBSERVED_AT + timedelta(hours=1),
    }
    payload.update(overrides)
    return SourceQuotaLease(**payload)


def test_quota_lease_identity_is_deterministic_and_verified() -> None:
    first = _lease()
    local_tz = timezone(timedelta(hours=8))
    equivalent = _lease(
        granted_at=first.granted_at.astimezone(local_tz),
        expires_at=first.expires_at.astimezone(local_tz),
        quota_reset_at=first.quota_reset_at.astimezone(local_tz),
    )

    assert first.lease_id == equivalent.lease_id
    assert len(first.lease_id) == 64
    assert _lease(lease_id=first.lease_id) == first
    with pytest.raises(ValidationError, match="lease_id does not match"):
        _lease(lease_id="0" * 64)
    with pytest.raises(ValidationError, match="quota_reset_at cannot precede expires_at"):
        _lease(quota_reset_at=OBSERVED_AT)
    with pytest.raises(ValidationError, match="released_at cannot precede granted_at"):
        _lease(released_at=OBSERVED_AT - timedelta(minutes=1))


def test_admission_fails_closed_for_non_readonly_work() -> None:
    decision = evaluate_admission(
        _request(read_only=False),
        _snapshot(),
        _policy(),
        quota_lease=_lease(),
    )

    assert decision.outcome is AdmissionOutcome.REJECTED
    assert decision.reason_codes == ("non_read_only",)
    assert decision.retry_at is None


@pytest.mark.parametrize(
    ("session", "expected_reason"),
    [
        (TradingSession.PRE_MARKET, "live_session_blocked"),
        (TradingSession.MORNING, "live_session_blocked"),
        (TradingSession.LUNCH, "live_session_blocked"),
        (TradingSession.AFTERNOON, "live_session_blocked"),
    ],
)
def test_live_sessions_are_deferred_with_retry(
    session: TradingSession,
    expected_reason: str,
) -> None:
    decision = evaluate_admission(
        _request(),
        _snapshot(session=session),
        _policy(),
        quota_lease=_lease(),
    )

    assert decision.outcome is AdmissionOutcome.DEFERRED
    assert expected_reason in decision.reason_codes
    assert decision.retry_at == OBSERVED_AT + timedelta(seconds=60)


def test_resource_health_cost_quota_and_deadline_gates_are_fail_closed() -> None:
    decision = evaluate_admission(
        _request(
            expected_memory_bytes=5_000,
            expected_disk_bytes=50_000,
            expected_quota_units=60,
            deadline=OBSERVED_AT,
        ),
        _snapshot(
            live_backlog_age_seconds=11,
            live_p95_latency_seconds=6,
            available_memory_bytes=5_500,
            available_disk_bytes=55_000,
            io_pressure_pct=81,
            cpu_load_pct=86,
            source_quota_remaining=5,
            live_healthy=False,
        ),
        _policy(),
    )

    assert decision.outcome is AdmissionOutcome.DEFERRED
    assert decision.reason_codes == tuple(sorted(set(decision.reason_codes)))
    assert {
        "deadline_expired",
        "expected_disk_cost_exceeded",
        "expected_memory_cost_exceeded",
        "expected_quota_cost_exceeded",
        "insufficient_disk",
        "insufficient_memory",
        "insufficient_source_quota",
        "io_pressure_high",
        "live_backlog_stale",
        "live_latency_high",
        "live_unhealthy",
        "quota_lease_missing",
        "cpu_load_high",
    } <= set(decision.reason_codes)
    assert decision.retry_at == OBSERVED_AT + timedelta(seconds=60)


def test_admission_requires_an_active_owner_bound_quota_lease() -> None:
    missing = evaluate_admission(_request(), _snapshot(), _policy())
    wrong_owner = evaluate_admission(
        _request(),
        _snapshot(),
        _policy(),
        quota_lease=_lease(owner="other-job"),
    )
    expired_lease = _lease(
        expires_at=OBSERVED_AT,
        quota_reset_at=OBSERVED_AT + timedelta(minutes=30),
    )
    expired = evaluate_admission(
        _request(),
        _snapshot(),
        _policy(),
        quota_lease=expired_lease,
    )

    assert missing.reason_codes == ("quota_lease_missing",)
    assert wrong_owner.reason_codes == ("quota_lease_owner_mismatch",)
    assert expired.reason_codes == ("quota_lease_expired",)
    assert expired.retry_at == expired_lease.quota_reset_at

    wrong_source = evaluate_admission(
        _request(),
        _snapshot(),
        _policy(),
        quota_lease=_lease(source="other-source"),
    )
    assert wrong_source.reason_codes == ("quota_lease_source_mismatch",)


def test_quota_consuming_request_must_declare_its_source() -> None:
    with pytest.raises(ValidationError, match="source"):
        _request(source=None)


def test_healthy_readonly_request_with_quota_lease_is_admitted() -> None:
    lease = _lease()
    decision = evaluate_admission(
        _request(),
        _snapshot(),
        _policy(),
        quota_lease=lease,
    )

    assert decision == AdmissionDecision(
        outcome=AdmissionOutcome.ADMITTED,
        reason_codes=(),
        observed_at=OBSERVED_AT,
        quota_lease=lease,
    )


def test_decision_enforces_outcome_retry_semantics_and_unique_reasons() -> None:
    with pytest.raises(ValidationError, match="admitted decision cannot have reasons or retry_at"):
        AdmissionDecision(
            outcome=AdmissionOutcome.ADMITTED,
            reason_codes=("unexpected",),
            observed_at=OBSERVED_AT,
        )
    with pytest.raises(ValidationError, match="deferred decision requires retry_at"):
        AdmissionDecision(
            outcome=AdmissionOutcome.DEFERRED,
            reason_codes=("busy",),
            observed_at=OBSERVED_AT,
        )
    with pytest.raises(ValidationError, match="rejected decision cannot have retry_at"):
        AdmissionDecision(
            outcome=AdmissionOutcome.REJECTED,
            reason_codes=("unsafe",),
            observed_at=OBSERVED_AT,
            retry_at=OBSERVED_AT + timedelta(seconds=1),
        )
    with pytest.raises(ValidationError, match="reason_codes must be unique"):
        AdmissionDecision(
            outcome=AdmissionOutcome.DEFERRED,
            reason_codes=("busy", "busy"),
            observed_at=OBSERVED_AT,
            retry_at=OBSERVED_AT + timedelta(seconds=1),
        )


def test_resource_contracts_reject_naive_time_and_out_of_range_pressure() -> None:
    with pytest.raises(ValidationError):
        _snapshot(observed_at=datetime(2026, 7, 31, 10, 0))
    with pytest.raises(ValidationError):
        _snapshot(io_pressure_pct=100.1)
    with pytest.raises(ValidationError):
        _request(unexpected=True)
