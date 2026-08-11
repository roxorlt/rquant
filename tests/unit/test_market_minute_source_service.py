from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.market_minute_source_service import capture_market_minute_step

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-07-31 09:40:00",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 1_000.0,
                "amount": 10_100.0,
            }
        ]
    )


def _gateway(root: Path, fetcher) -> MarketMinuteGateway:
    return MarketMinuteGateway(
        spool=LiveBatchSpool(root),
        fetcher=fetcher,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
    )


def test_step_publishes_source_watermark_and_deduplicates_unchanged_capture(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, _frame)

    first = capture_market_minute_step(gateway, received_at=NOW)
    retry = capture_market_minute_step(gateway, received_at=NOW)

    minute_descriptor = gateway.spool.source_descriptor(LiveChannel.MARKET_MINUTE)
    assert first.processed_count == 1
    assert retry.processed_count == 0
    assert first.output_sequence == retry.output_sequence == 0
    assert first.source_generations == {"market_minute": minute_descriptor.generation_id}
    assert first.degraded_reasons == ()


def test_step_persists_source_failure_as_stale_degraded_watermark(tmp_path: Path) -> None:
    gateway = _gateway(
        tmp_path,
        lambda: (_ for _ in ()).throw(TimeoutError("source down")),
    )

    result = capture_market_minute_step(gateway, received_at=NOW)

    assert result.processed_count == 1
    assert result.output_sequence == 0
    assert result.degraded_reasons == ("market_minute:stale:source_error:TimeoutError",)


def test_market_minute_source_does_not_own_watchlist_quote_capture() -> None:
    source = Path(__file__).resolve().parents[2] / "src/rquant/market_minute_source_service.py"

    assert "watchlist_quote" not in source.read_text(encoding="utf-8")
