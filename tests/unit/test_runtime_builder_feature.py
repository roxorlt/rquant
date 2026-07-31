from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.feature_contracts import FeatureAvailability
from rquant.feature_spool import FeatureBatchSpool
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.runtime_builder_feature import FeatureRuntimeConfig, feature_live_builder
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

NOW = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
COMMIT = "a" * 40


def test_feature_runtime_config_defaults_to_contract_v3() -> None:
    config = FeatureRuntimeConfig()

    assert config.contract_version == 3
    assert config.schema_version == 2
    with pytest.raises(ValidationError):
        FeatureRuntimeConfig(contract_version=2)


def _minute_frame(*, trade_time: str, amount: float = 20_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": trade_time,
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 2_000.0,
                "amount": amount,
            }
        ]
    )


def _history() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for trade_time in ("2026-07-29 09:40:00+08:00", "2026-07-30 09:40:00+08:00"):
        rows.append(
            {
                "ts_code": "600000.SH",
                "trade_time": trade_time,
                "available_at": trade_time,
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "vol": 1_000.0,
                "amount": 10_000.0,
            }
        )
    return pd.DataFrame(rows)


def _write_snapshot(path: Path) -> str:
    _history().to_parquet(path, index=False)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest(
    tmp_path: Path,
    *,
    snapshot_id: str,
    limit: int = 10,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="feature.intraday-pit",
        service_kind=RuntimeServiceKind.FEATURE_LIVE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings={
            "raw_spool_root": str(tmp_path / "raw"),
            "feature_spool_root": str(tmp_path / "features"),
            "historical_minutes_snapshot_path": str(tmp_path / "history.parquet"),
            "historical_snapshot_id": snapshot_id,
            "limit": limit,
            "consumer_id": "feature-live-test",
            "feature_config": {
                "lookback_sessions": 2,
                "opening_acceleration_block_minutes": 3,
                "contract_id": "intraday-pit",
                "contract_version": 3,
                "schema_version": 2,
            },
        },
    )


def _gateway(tmp_path: Path, frames: list[pd.DataFrame]) -> MarketMinuteGateway:
    pending = iter(frames)
    return MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "raw"),
        fetcher=lambda: next(pending),
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="b" * 40,
        ),
    )


def test_builder_runs_persistent_feature_batch_with_manifest_bound_config(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [_minute_frame(trade_time="2026-07-31 09:40:00")])
    gateway.capture_once(received_at=NOW)
    snapshot_path = tmp_path / "history.parquet"
    snapshot_id = _write_snapshot(snapshot_path)

    step = feature_live_builder(clock=lambda: NOW)(_manifest(tmp_path, snapshot_id=snapshot_id))
    snapshot_path.unlink()
    result = step()

    assert result.input_sequence == 0
    assert result.output_sequence == 0
    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert result.degraded_reasons == ()
    assert result.source_generations[LiveChannel.MARKET_MINUTE.value] == (
        gateway.spool.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id
    )
    features = FeatureBatchSpool(tmp_path / "features")
    assert result.source_generations["intraday_feature"] == (
        features.source_descriptor().generation_id
    )
    record = features.list_after(sequence=-1, through_sequence=0, limit=1)[0]
    stored = features.read_result(record)
    assert stored.envelope.producer_commit == COMMIT
    assert stored.frame.iloc[0]["rel_same_minute"] == pytest.approx(2.0)


def test_builder_maps_limit_backlog_and_stale_source_to_runtime_degradation(
    tmp_path: Path,
) -> None:
    gateway = MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "raw"),
        fetcher=lambda: (_ for _ in ()).throw(TimeoutError("source down")),
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="b" * 40,
        ),
    )
    gateway.capture_once(received_at=NOW)
    healthy = _gateway(
        tmp_path,
        [_minute_frame(trade_time="2026-07-31 09:41:00", amount=30_000.0)],
    )
    healthy.capture_once(received_at=NOW + timedelta(minutes=1))
    snapshot_path = tmp_path / "history.parquet"
    snapshot_id = _write_snapshot(snapshot_path)

    result = feature_live_builder(clock=lambda: NOW + timedelta(minutes=1))(
        _manifest(tmp_path, snapshot_id=snapshot_id, limit=1)
    )()

    assert result.input_sequence == 0
    assert result.output_sequence == 0
    assert result.processed_count == 1
    assert result.backlog_count == 1
    assert result.degraded_reasons == (
        "feature_live:backlog_deferred",
        "feature_live:stale_source_batches:1",
    )
    stored = FeatureBatchSpool(tmp_path / "features").read_result(
        FeatureBatchSpool(tmp_path / "features").list_after(
            sequence=-1,
            through_sequence=0,
            limit=1,
        )[0]
    )
    assert all(
        status.status is FeatureAvailability.STALE for status in stored.envelope.field_statuses
    )


def test_builder_rejects_wrong_service_plane_paths_snapshot_and_feature_config(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "history.parquet"
    snapshot_id = _write_snapshot(snapshot_path)
    manifest = _manifest(tmp_path, snapshot_id=snapshot_id)
    builder = feature_live_builder(clock=lambda: NOW)

    wrong_kind = RuntimeServiceManifest.model_validate(
        {**manifest.model_dump(mode="json"), "service_kind": "strategy_live"}
    )
    with pytest.raises(ValueError, match="kind"):
        builder(wrong_kind)

    wrong_plane = RuntimeServiceManifest.model_validate(
        {**manifest.model_dump(mode="json"), "plane": "serving"}
    )
    with pytest.raises(ValueError, match="live plane"):
        builder(wrong_plane)

    settings = manifest.model_dump(mode="json")["settings"]
    relative = RuntimeServiceManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "settings": {**settings, "raw_spool_root": "relative/raw"},
        }
    )
    with pytest.raises(ValidationError, match="absolute"):
        builder(relative)

    wrong_snapshot = RuntimeServiceManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "settings": {**settings, "historical_snapshot_id": "0" * 64},
        }
    )
    with pytest.raises(ValueError, match="snapshot"):
        builder(wrong_snapshot)

    config_with_commit = dict(settings["feature_config"])
    config_with_commit["producer_commit"] = "c" * 40
    unbound_config = RuntimeServiceManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "settings": {**settings, "feature_config": config_with_commit},
        }
    )
    with pytest.raises(ValidationError, match="producer_commit"):
        builder(unbound_config)

    bool_limit = RuntimeServiceManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "settings": {**settings, "limit": True},
        }
    )
    with pytest.raises(ValidationError, match="limit"):
        builder(bool_limit)
