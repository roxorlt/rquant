"""Runtime step for the isolated opening-auction source gateway."""

from __future__ import annotations

from rquant.auction_match_gateway import AuctionMatchCapture, AuctionMatchGateway
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.runtime_service_control import RuntimeStepResult


def capture_auction_match_step(
    gateway: AuctionMatchGateway,
    *,
    capture: AuctionMatchCapture,
) -> RuntimeStepResult:
    records = gateway.spool.list_after(
        LiveChannel.AUCTION_MATCH,
        sequence=capture.pointer.sequence - 1,
    )
    if len(records) != 1:
        raise RuntimeError("published auction-match batch cannot be resolved")
    envelope = records[0].envelope
    degraded_reasons: tuple[str, ...] = ()
    if envelope.quality_status in {
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
    }:
        degraded_reasons = tuple(
            f"auction_match:{envelope.quality_status.value}:{reason}"
            for reason in envelope.degraded_reasons
        )
    return RuntimeStepResult(
        output_sequence=capture.pointer.sequence,
        processed_count=int(capture.published),
        source_generations={
            LiveChannel.AUCTION_MATCH.value: capture.pointer.source_generation_id,
        },
        degraded_reasons=degraded_reasons,
    )


__all__ = ["capture_auction_match_step"]
