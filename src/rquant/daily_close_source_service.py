"""Side-effect-limited runtime step for daily-close raw capture."""

from __future__ import annotations

from datetime import date, datetime

from rquant.daily_close_gateway import DailyCloseGateway
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.runtime_service_control import RuntimeStepResult


def capture_daily_close_step(
    gateway: DailyCloseGateway,
    *,
    trade_date: date,
    observed_at: datetime,
    refresh: bool = False,
) -> RuntimeStepResult:
    capture = gateway.capture_once(
        trade_date=trade_date,
        observed_at=observed_at,
        refresh=refresh,
    )
    descriptor = gateway.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
    degraded_reasons: tuple[str, ...] = ()
    if capture.quality_status in {
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
        BatchQualityStatus.QUARANTINED,
    }:
        degraded_reasons = tuple(
            f"daily_close:{capture.quality_status.value}:{reason}"
            for reason in capture.degraded_reasons
        )
    return RuntimeStepResult(
        output_sequence=descriptor.high_watermark,
        processed_count=int(capture.published or capture.quarantined),
        source_generations={LiveChannel.DAILY_CLOSE.value: descriptor.generation_id},
        degraded_reasons=degraded_reasons,
    )


__all__ = ["capture_daily_close_step"]
