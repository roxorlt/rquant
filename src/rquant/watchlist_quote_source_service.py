"""Runtime step for the standalone watchlist quote source."""

from __future__ import annotations

from datetime import date, datetime

from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.runtime_service_control import RuntimeStepResult
from rquant.watchlist_quote_gateway import WatchlistQuoteGateway


def capture_watchlist_quote_step(
    gateway: WatchlistQuoteGateway,
    *,
    codes: tuple[str, ...],
    scheduled_at: datetime,
    universe_as_of: datetime,
    trade_date: date,
) -> RuntimeStepResult:
    capture = gateway.capture_once(
        codes=codes,
        scheduled_at=scheduled_at,
        universe_as_of=universe_as_of,
        trade_date=trade_date,
    )
    records = gateway.spool.list_after(
        LiveChannel.WATCHLIST_QUOTE,
        sequence=capture.pointer.sequence - 1,
    )
    if len(records) != 1:
        raise RuntimeError("published watchlist quote batch cannot be resolved")
    envelope = records[0].envelope
    reasons = ()
    if envelope.quality_status in {BatchQualityStatus.DEGRADED, BatchQualityStatus.STALE}:
        reasons = tuple(
            f"watchlist_quote:{envelope.quality_status.value}:{reason}"
            for reason in envelope.degraded_reasons
        )
    return RuntimeStepResult(
        output_sequence=capture.pointer.sequence,
        processed_count=int(capture.published),
        source_generations={
            LiveChannel.WATCHLIST_QUOTE.value: capture.pointer.source_generation_id,
        },
        degraded_reasons=reasons,
    )


__all__ = ["capture_watchlist_quote_step"]
