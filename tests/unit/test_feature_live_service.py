from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from rquant.feature_contracts import FeatureAvailability
from rquant.feature_live_service import run_feature_live_batch
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig

SHANGHAI = timezone(timedelta(hours=8))
RECEIVED = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


def _raw_frame(*, minute: int = 40, amount: float = 10_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": f"2026-07-31 09:{minute:02d}:00",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": amount / 10.1,
                "amount": amount,
            }
        ]
    )


def _history() -> pd.DataFrame:
    rows = []
    for day, amount in ((29, 4_000.0), (30, 6_000.0)):
        rows.append(
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 7, day, 9, 40, tzinfo=SHANGHAI),
                "available_at": datetime(2026, 7, day, 9, 40, 2, tzinfo=SHANGHAI),
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "vol": amount / 10.0,
                "amount": amount,
            }
        )
    return pd.DataFrame(rows)


def _gateway(root: Path, frames: list[pd.DataFrame]) -> MarketMinuteGateway:
    return MarketMinuteGateway(
        spool=LiveBatchSpool(root / "live"),
        fetcher=lambda: frames.pop(0),
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
    )


def _config() -> IntradayFeatureConfig:
    return IntradayFeatureConfig(
        lookback_sessions=2,
        opening_acceleration_block_minutes=3,
        producer_commit="b" * 40,
    )


def test_service_consumes_raw_once_and_publishes_pit_feature_batch(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED)
    features = FeatureBatchSpool(tmp_path / "features")

    summary = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    assert summary.processed_count == 1
    assert summary.last_raw_sequence == 0
    record = features.list_after(sequence=-1, through_sequence=0, limit=1)[0]
    result = features.read_result(record)
    assert result.frame.iloc[0]["rel_same_minute"] == pytest.approx(2.0)
    assert result.envelope.input_batch_ids == tuple(
        sorted(("history-20260730", gateway.spool.current(LiveChannel.MARKET_MINUTE).batch_id))
    )


def test_crash_after_feature_publish_replays_without_duplicate_batch(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED)
    features = FeatureBatchSpool(tmp_path / "features")

    def fail(stage: str) -> None:
        if stage == "after_feature_publish":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected crash"):
        run_feature_live_batch(
            raw_spool=gateway.spool,
            feature_spool=features,
            historical_minutes=_history(),
            historical_snapshot_id="history-20260730",
            config=_config(),
            observed_at=RECEIVED,
            limit=10,
            fault_hook=fail,
        )

    assert gateway.spool.load_cursor("feature-live", LiveChannel.MARKET_MINUTE) is None
    recovered = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED + timedelta(seconds=1),
        limit=10,
    )
    assert recovered.processed_count == 1
    assert len(features.list_after(sequence=-1, through_sequence=0, limit=10)) == 1


def test_service_does_not_consume_future_raw_batch(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED + timedelta(minutes=1))

    summary = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=FeatureBatchSpool(tmp_path / "features"),
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    assert summary.processed_count == 0
    assert summary.has_deferred_batches is True


def test_stale_raw_batch_becomes_explicit_stale_empty_feature_batch(tmp_path: Path) -> None:
    gateway = MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=lambda: (_ for _ in ()).throw(TimeoutError("source down")),
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
    )
    capture = gateway.capture_once(received_at=RECEIVED)
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    features = FeatureBatchSpool(tmp_path / "features")

    run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    result = features.read_result(features.list_after(sequence=-1, through_sequence=0, limit=1)[0])
    assert result.frame.empty
    assert result.envelope.row_count == 0
    assert all(
        status.status is FeatureAvailability.STALE for status in result.envelope.field_statuses
    )


def test_published_empty_raw_batch_is_consumed_as_unavailable_feature_batch(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [pd.DataFrame(columns=_raw_frame().columns)])
    capture = gateway.capture_once(received_at=RECEIVED)
    assert capture.pointer.quality_status is BatchQualityStatus.PUBLISHED
    features = FeatureBatchSpool(tmp_path / "features")

    summary = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    result = features.read_result(features.list_after(sequence=-1, through_sequence=0, limit=1)[0])
    assert summary.processed_count == 1
    assert result.frame.empty
    assert all(
        status.status is FeatureAvailability.UNAVAILABLE and status.reason == "source_empty"
        for status in result.envelope.field_statuses
    )
