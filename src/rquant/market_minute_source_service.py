"""Runtime step for the single market-minute source gateway."""

from __future__ import annotations

from datetime import datetime

from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_service_control import RuntimeStepResult


def capture_market_minute_step(
    gateway: MarketMinuteGateway,
    *,
    received_at: datetime,
) -> RuntimeStepResult:
    capture = gateway.capture_once(received_at=received_at)
    records = gateway.spool.list_after(
        LiveChannel.MARKET_MINUTE,
        sequence=capture.pointer.sequence - 1,
    )
    if len(records) != 1:
        raise RuntimeError("published market-minute batch cannot be resolved")
    envelope = records[0].envelope
    degraded_reasons: tuple[str, ...] = ()
    if envelope.quality_status in {
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
    }:
        degraded_reasons = tuple(
            f"market_minute:{envelope.quality_status.value}:{reason}"
            for reason in envelope.degraded_reasons
        )
    return RuntimeStepResult(
        output_sequence=capture.pointer.sequence,
        processed_count=int(capture.published),
        source_generations={LiveChannel.MARKET_MINUTE.value: capture.pointer.source_generation_id},
        degraded_reasons=degraded_reasons,
    )


__all__ = ["capture_market_minute_step"]
