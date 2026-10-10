"""Runtime step for the single market-minute source gateway."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from rquant.live_contracts import BatchQualityStatus
from rquant.market_minute_gateway import MarketMinuteCapture, MarketMinuteGateway
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_health_details import (
    RuntimeHealthAsOfValidity,
    RuntimeHealthMetric,
    RuntimeHealthMinuteScope,
)
from rquant.runtime_service_control import RuntimeStepResult


def capture_market_minute_step(
    gateway: MarketMinuteGateway,
    *,
    received_at: datetime,
    quota_cost_units: int | None = None,
    health_metrics_enabled: bool = False,
    expected_codes: tuple[str, ...] | None = None,
) -> RuntimeStepResult:
    capture = gateway.capture_once(
        received_at=received_at,
        quota_cost_units=quota_cost_units,
        include_health_material=health_metrics_enabled,
        expected_codes=expected_codes,
    )
    degraded_reasons: tuple[str, ...] = ()
    if capture.pointer.quality_status in {
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
    }:
        degraded_reasons = tuple(
            f"market_minute:{capture.pointer.quality_status.value}:{reason}"
            for reason in gateway.spool.list_after(
                capture.pointer.channel,
                sequence=capture.pointer.sequence - 1,
            )[0].envelope.degraded_reasons
        )
    return RuntimeStepResult(
        output_sequence=capture.pointer.sequence,
        processed_count=int(capture.published),
        source_generations={
            capture.pointer.channel.value: capture.pointer.source_generation_id,
        },
        degraded_reasons=degraded_reasons,
        health_metrics=(None if not health_metrics_enabled else _minute_metrics(capture)),
    )


def _minute_metrics(capture: MarketMinuteCapture) -> tuple[RuntimeHealthMetric, ...]:
    material = capture.health_material
    if material is None:
        raise ValueError("enabled minute detail requires its actual captured material")
    envelope = material.envelope
    expected = material.expected_codes
    complete = expected is not None and set(material.accepted_codes) <= set(expected)
    usable = envelope.quality_status is BatchQualityStatus.PUBLISHED and envelope.row_count > 0
    scope = RuntimeHealthMinuteScope(
        batch_id=envelope.batch_id,
        expected_universe_identity=None if expected is None else canonical_sha256(expected),
        scope_complete=complete,
    )
    common = dict(
        owner_dataset_id="market_minute",
        source_generation_id=material.source_generation_id,
        source_identity=material.identity,
        scope=scope,
        event_time_start=envelope.event_time_start,
        event_time_end=envelope.event_time_end,
        available_at=envelope.available_at,
        observed_at=envelope.available_at,
    )
    validity = RuntimeHealthAsOfValidity(
        basis="batch",
        basis_identity=envelope.batch_id,
        as_of=envelope.available_at,
        scope_identity=canonical_sha256(scope),
        **{key: value for key, value in common.items() if key not in {"scope", "observed_at"}},
    )
    elapsed = envelope.available_at - envelope.event_time_end
    seconds = (
        Decimal(elapsed.days * 86400 + elapsed.seconds) + Decimal(elapsed.microseconds) / 1_000_000
    )
    return (
        RuntimeHealthMetric(
            metric_id="minute_delay",
            unit="seconds",
            **common,
            validity=validity,
            completeness="complete" if usable else "unavailable",
            value=seconds if usable else None,
            verdict="unassessed" if usable else "unavailable",
            reason_code="latest_minute_batch" if usable else "minute_batch_unavailable",
        ),
        RuntimeHealthMetric(
            metric_id="minute_missing_codes",
            unit="count",
            **common,
            validity=validity,
            completeness="complete" if usable and complete else "unavailable",
            value=len(set(expected) - set(material.accepted_codes))
            if usable and complete
            else None,
            verdict="unassessed" if usable and complete else "unavailable",
            reason_code="latest_minute_batch"
            if usable and complete
            else "minute_expected_scope_unavailable",
        ),
    )


__all__ = ["capture_market_minute_step"]
