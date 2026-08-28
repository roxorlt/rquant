"""Runtime builder for the isolated point-in-time feature service."""

from __future__ import annotations

import hashlib
import io
import os
import stat
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import pandas as pd
from pydantic import Field, StrictInt, StrictStr, StringConstraints, field_validator

from rquant.feature_live_service import FeatureLiveBatchSummary, run_feature_live_batch
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)

SnapshotId = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class FeatureRuntimeConfig(RuntimeContractModel):
    """Feature parameters whose producer commit is supplied only by the manifest."""

    lookback_sessions: StrictInt = Field(default=20, ge=1)
    opening_acceleration_block_minutes: StrictInt = Field(default=3, ge=0, le=30)
    bar_timestamp_semantics: Literal["bar_end"] = "bar_end"
    contract_id: StrictStr = Field(default="intraday-pit", min_length=1)
    contract_version: Literal[3] = 3
    schema_version: StrictInt = Field(default=2, ge=2)

    def bind_to_manifest(self, manifest: RuntimeServiceManifest) -> IntradayFeatureConfig:
        return IntradayFeatureConfig(
            **self.model_dump(mode="python"),
            producer_commit=manifest.producer_commit,
        )


class FeatureLiveRuntimeSettings(RuntimeContractModel):
    raw_spool_root: Path
    feature_spool_root: Path
    historical_minutes_snapshot_path: Path
    historical_snapshot_id: SnapshotId
    limit: StrictInt = Field(gt=0)
    consumer_id: StrictStr = Field(default="feature-live", min_length=1)
    feature_config: FeatureRuntimeConfig

    @field_validator(
        "raw_spool_root",
        "feature_spool_root",
        "historical_minutes_snapshot_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("feature-live runtime paths must be absolute")
        return value

    @field_validator("historical_minutes_snapshot_path")
    @classmethod
    def require_parquet_snapshot(cls, value: Path) -> Path:
        if value.suffix.lower() != ".parquet":
            raise ValueError("historical minute snapshot must be a parquet file")
        return value


def _read_immutable_parquet(path: Path, *, snapshot_id: str) -> pd.DataFrame:
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("historical minute snapshot must be a regular parquet file")
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = -1
            payload = stream.read()
    except OSError as exc:
        raise ValueError("historical minute snapshot is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    if hashlib.sha256(payload).hexdigest() != snapshot_id:
        raise ValueError("historical minute snapshot id does not match parquet content")
    try:
        return pd.read_parquet(io.BytesIO(payload))
    except (OSError, TypeError, ValueError) as exc:
        raise ValueError("historical minute snapshot parquet is invalid") from exc


def _runtime_result(
    summary: FeatureLiveBatchSummary,
    *,
    feature_spool: FeatureBatchSpool,
) -> RuntimeStepResult:
    backlog = max(summary.source_high_watermark - summary.last_raw_sequence, 0)
    degraded: list[str] = []
    if summary.has_deferred_batches:
        degraded.append("feature_live:backlog_deferred")
    if summary.stale_count:
        degraded.append(f"feature_live:stale_source_batches:{summary.stale_count}")
    return RuntimeStepResult(
        input_sequence=summary.last_raw_sequence,
        output_sequence=summary.feature_high_watermark,
        processed_count=summary.processed_count,
        backlog_count=backlog,
        source_generations={
            LiveChannel.MARKET_MINUTE.value: summary.source_generation_id,
            "intraday_feature": feature_spool.source_descriptor().generation_id,
        },
        degraded_reasons=tuple(degraded),
    )


def feature_live_builder(*, clock: Callable[[], datetime]) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.FEATURE_LIVE:
            raise ValueError("runtime service kind must be feature_live")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("feature-live service must run on the live plane")

        settings = FeatureLiveRuntimeSettings.model_validate(dict(manifest.settings))
        historical_minutes = _read_immutable_parquet(
            settings.historical_minutes_snapshot_path,
            snapshot_id=settings.historical_snapshot_id,
        )
        raw_spool = LiveBatchSpool(settings.raw_spool_root)
        feature_spool = FeatureBatchSpool(settings.feature_spool_root)
        config = settings.feature_config.bind_to_manifest(manifest)

        def step() -> RuntimeStepResult:
            summary = run_feature_live_batch(
                raw_spool=raw_spool,
                feature_spool=feature_spool,
                historical_minutes=historical_minutes,
                historical_snapshot_id=settings.historical_snapshot_id,
                config=config,
                observed_at=clock(),
                limit=settings.limit,
                consumer_id=settings.consumer_id,
            )
            return _runtime_result(summary, feature_spool=feature_spool)

        return step

    return build


__all__ = [
    "FeatureLiveRuntimeSettings",
    "FeatureRuntimeConfig",
    "feature_live_builder",
]
