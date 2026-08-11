from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.research_run_spec import ResourceClass
from rquant.resource_admission import (
    LAB_ADMISSION_COST_PROFILES,
    AdmissionDecision,
    AdmissionOutcome,
    AdmissionPolicy,
    AdmissionRequest,
    ResearchAdapterSourceUsage,
    ResearchAdapterSourceUsageError,
    ResourceSnapshot,
    SourceQuotaLease,
    TradingSession,
    derive_lab_admission_request,
    evaluate_admission,
    require_research_adapter_source_usage,
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


def test_lab_admission_request_is_deterministic_and_uses_work_plan_costs() -> None:
    from tests.unit.test_strategy_job_adapters import _nshape_compare_spec

    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"resource_class": ResourceClass.STANDARD}
    )
    work_plan = LabShardWorkPlan(
        phase="replay",
        work_unit_name="candidate",
        work_units=2_001,
        static_duration_ms=3_600_001,
    )

    first = derive_lab_admission_request(
        job_id="job-1",
        spec=spec,
        work_plan=work_plan,
    )
    second = derive_lab_admission_request(
        job_id="job-1",
        spec=spec,
        work_plan=work_plan,
    )
    profile = LAB_ADMISSION_COST_PROFILES[ResourceClass.STANDARD]

    assert first == second
    assert first.resource_class is ResourceClass.STANDARD
    assert first.expected_memory_bytes > profile.base_memory_bytes
    assert first.expected_disk_bytes > profile.base_disk_bytes
    assert first.expected_memory_bytes <= profile.max_memory_bytes
    assert first.expected_disk_bytes <= profile.max_disk_bytes
    assert first.expected_quota_units == 0
    assert first.source is None
    assert first.preemptible is True
    assert first.read_only is True
    assert first.deadline == spec.deadline


def test_lab_admission_request_requires_a_work_plan() -> None:
    from tests.unit.test_strategy_job_adapters import _nshape_compare_spec

    with pytest.raises(ValueError, match="work plan"):
        derive_lab_admission_request(
            job_id="job-1",
            spec=_nshape_compare_spec(hold_days=(1,)),
            work_plan=None,
        )


def test_local_research_adapter_declares_immutable_zero_quota_usage() -> None:
    usage = ResearchAdapterSourceUsage(
        adapter_id="snapshot-replay-v1",
        external=False,
        immutable_snapshot=True,
        expected_calls=0,
        actual_calls=0,
    )

    assert usage.source is None
    assert usage.quota_lease is None


def test_external_research_adapter_requires_a_bound_source_quota_lease() -> None:
    usage = ResearchAdapterSourceUsage(
        adapter_id="tushare-backfill-v1",
        external=True,
        immutable_snapshot=False,
        source="tushare.pro_bar",
        expected_calls=2,
        actual_calls=1,
        quota_lease=_lease(source="tushare.pro_bar", units=2),
    )

    assert usage.quota_lease is not None
    assert usage.actual_calls == 1


def test_external_research_adapter_without_usage_is_refused() -> None:
    with pytest.raises(ResearchAdapterSourceUsageError, match="source usage is required"):
        require_research_adapter_source_usage(adapter_id="external-undeclared-v1", usage=None)


@pytest.mark.parametrize(
    "payload",
    (
        {
            "adapter_id": "external-undeclared-v1",
            "external": True,
            "immutable_snapshot": False,
            "expected_calls": 1,
            "actual_calls": 0,
        },
        {
            "adapter_id": "local-with-quota-v1",
            "external": False,
            "immutable_snapshot": True,
            "source": "tushare",
            "expected_calls": 1,
            "actual_calls": 0,
        },
    ),
)
def test_research_adapter_usage_fails_closed_for_undeclared_external_or_nonlocal_quota(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        ResearchAdapterSourceUsage(**payload)


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
            session=TradingSession.MORNING,
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


def test_non_live_session_does_not_require_inapplicable_live_slo() -> None:
    decision = evaluate_admission(
        _request(expected_quota_units=0, source=None),
        _snapshot(
            session=TradingSession.POST_MARKET,
            live_slo_applicable=False,
            live_backlog_age_seconds=999,
            live_p95_latency_seconds=999,
            live_healthy=False,
        ),
        _policy(),
    )

    assert decision.outcome is AdmissionOutcome.ADMITTED


def test_live_session_cannot_disable_live_slo_gate() -> None:
    with pytest.raises(ValidationError, match="live_slo_applicable"):
        _snapshot(
            session=TradingSession.MORNING,
            live_slo_applicable=False,
        )


def test_live_session_requires_short_preemptible_shard() -> None:
    decision = evaluate_admission(
        _request(
            expected_quota_units=0,
            source=None,
            expected_duration_ms=5_001,
        ),
        _snapshot(session=TradingSession.LUNCH),
        _policy(allow_live_session=True, max_live_shard_duration_ms=5_000),
    )

    assert decision.outcome is AdmissionOutcome.DEFERRED
    assert "live_duration_exceeded" in decision.reason_codes


def test_live_shard_duration_equal_to_policy_limit_is_admitted() -> None:
    decision = evaluate_admission(
        _request(
            expected_quota_units=0,
            source=None,
            expected_duration_ms=5_000,
        ),
        _snapshot(session=TradingSession.LUNCH),
        _policy(allow_live_session=True, max_live_shard_duration_ms=5_000),
    )

    assert decision.outcome is AdmissionOutcome.ADMITTED


def test_admission_requires_expected_duration_before_deadline() -> None:
    decision = evaluate_admission(
        _request(
            expected_quota_units=0,
            source=None,
            expected_duration_ms=60_001,
            deadline=OBSERVED_AT + timedelta(minutes=1),
        ),
        _snapshot(),
        _policy(),
    )

    assert decision.outcome is AdmissionOutcome.DEFERRED
    assert "deadline_insufficient" in decision.reason_codes


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


@pytest.mark.parametrize("field", ("available_memory_bytes", "available_disk_bytes"))
def test_resource_snapshot_rejects_unbounded_capacity(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        _snapshot(**{field: 1 << 63})


@pytest.mark.parametrize(
    ("factory", "field"),
    (
        (_snapshot, "available_memory_bytes"),
        (_snapshot, "available_disk_bytes"),
        (_snapshot, "source_quota_remaining"),
        (_policy, "max_live_shard_duration_ms"),
        (_policy, "min_available_memory_bytes"),
        (_policy, "max_expected_quota_units"),
        (_policy, "retry_delay_seconds"),
        (_request, "expected_memory_bytes"),
        (_request, "expected_quota_units"),
        (_request, "expected_duration_ms"),
        (_lease, "units"),
    ),
)
@pytest.mark.parametrize("value", (True, "1", 1 << 63))
def test_resource_integer_contracts_reject_noncanonical_or_unbounded_values(
    factory: object,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError, match=field):
        factory(**{field: value})


@pytest.mark.parametrize("value", (True, "1", 1 << 63))
def test_resource_duration_alias_rejects_noncanonical_or_unbounded_values(
    value: object,
) -> None:
    with pytest.raises((ValidationError, ValueError), match="live_backlog_age_seconds"):
        _snapshot(live_backlog_age_seconds=value)


def test_admission_policy_requires_a_strict_positive_snapshot_age_limit() -> None:
    assert _policy(max_snapshot_age_seconds=1.5).max_snapshot_age_seconds == 1.5

    with pytest.raises(ValidationError, match="max_snapshot_age_microseconds"):
        _policy(max_snapshot_age_seconds=0)


def test_admission_time_rules_normalize_seconds_to_integer_microseconds() -> None:
    snapshot = _snapshot(
        session=TradingSession.MORNING,
        live_backlog_age_seconds=0.00018000000000000004,
        live_p95_latency_seconds=0.00018000000000000004,
    )
    policy = _policy(
        allow_live_session=True,
        max_snapshot_age_seconds=0.00018000000000000004,
        max_live_backlog_age_seconds=0.00018,
        max_live_p95_latency_seconds=0.00018,
    )

    assert snapshot.live_backlog_age_microseconds == 180
    assert snapshot.live_p95_latency_microseconds == 180
    assert policy.max_snapshot_age_microseconds == 180
    assert policy.max_live_backlog_age_microseconds == 180
    assert policy.max_live_p95_latency_microseconds == 180
    assert (
        evaluate_admission(
            _request(expected_quota_units=0, source=None),
            snapshot,
            policy,
        ).outcome
        is AdmissionOutcome.ADMITTED
    )


@pytest.mark.parametrize(
    ("factory", "field"),
    (
        (_snapshot, "live_slo_applicable"),
        (_snapshot, "live_healthy"),
        (_policy, "allow_live_session"),
        (_request, "preemptible"),
        (_request, "read_only"),
    ),
)
@pytest.mark.parametrize("value", (0, 1, "true", "false"))
def test_resource_boolean_contracts_reject_noncanonical_values(
    factory: object,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError, match=field):
        factory(**{field: value})


@pytest.mark.parametrize(
    ("factory", "field"),
    (
        (_snapshot, "io_pressure_pct"),
        (_snapshot, "cpu_load_pct"),
        (_policy, "max_io_pressure_pct"),
        (_policy, "max_cpu_load_pct"),
    ),
)
@pytest.mark.parametrize("value", (True, "10.0"))
def test_resource_percentage_contracts_reject_noncanonical_values(
    factory: object,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError, match=field):
        factory(**{field: value})


def test_cost_profile_boolean_contract_is_strict() -> None:
    from rquant.resource_admission import LabAdmissionCostProfile

    payload = LAB_ADMISSION_COST_PROFILES[ResourceClass.STANDARD].model_dump(mode="python")
    payload["preemptible"] = "false"

    with pytest.raises(ValidationError, match="preemptible"):
        LabAdmissionCostProfile.model_validate(payload)


def test_admission_duration_and_retry_have_business_safe_bounds() -> None:
    with pytest.raises(ValidationError, match="expected_duration_ms"):
        _request(expected_duration_ms=2_592_000_001)
    with pytest.raises(ValidationError, match="max_live_shard_duration_ms"):
        _policy(max_live_shard_duration_ms=2_592_000_001)
    with pytest.raises(ValidationError, match="retry_delay_seconds"):
        _policy(retry_delay_seconds=86_401)


def test_valid_resource_snapshot_timestamp_cannot_overflow_admission_arithmetic() -> None:
    with pytest.raises(ValidationError, match="observed_at"):
        _snapshot(observed_at=datetime.max.replace(tzinfo=UTC))
