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
    #: 丢行这件事说在心跳上，不写进批次信封：`BatchEnvelope` 的不变量是「非降级状态禁止
    #: 携带 degraded_reasons」，把这条记进信封就等于把批次判成 DEGRADED，而
    #: `candidate.auction_gap` 只认 PUBLISHED——那正是 2026-09-23 要修的那件事本身。
    #: 心跳是落盘的（`control/<桶>/<实例>/heartbeats/*.json`），所以这条记录不是只活在内存里。
    if capture.rows_dropped_non_finite:
        degraded_reasons = (
            *degraded_reasons,
            f"auction_match:rows_dropped_non_finite:{capture.rows_dropped_non_finite}",
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
