"""Runtime step for the single market-minute source gateway."""

from __future__ import annotations

from datetime import datetime

from rquant.live_contracts import BatchQualityStatus
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_service_control import RuntimeStepResult


def capture_market_minute_step(
    gateway: MarketMinuteGateway,
    *,
    received_at: datetime,
    quota_cost_units: int | None = None,
) -> RuntimeStepResult:
    capture = gateway.capture_once(
        received_at=received_at,
        quota_cost_units=quota_cost_units,
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
    )


__all__ = ["capture_market_minute_step"]
