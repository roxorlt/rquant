"""Single-fetch market-minute gateway publishing immutable live batches."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from io import BytesIO
from typing import Annotated

import numpy as np
import pandas as pd
from pydantic import Field, StringConstraints

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    CurrentPointer,
    LiveChannel,
)
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import (
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

MARKET_MINUTE_COLUMNS = (
    "ts_code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)


class MarketMinuteValidationError(ValueError):
    pass


class MarketMinuteGatewayConfig(RuntimeContractModel):
    source: str = Field(default="tushare.rt_min", min_length=1)
    dataset_id: str = Field(default="market_minute", min_length=1)
    producer_version: str = Field(min_length=1)
    producer_commit: CommitSha


class MarketMinuteCapture(RuntimeContractModel):
    pointer: CurrentPointer
    published: bool


class MarketMinuteGateway:
    """The only owner of a market-minute source request and its live spool."""

    def __init__(
        self,
        *,
        spool: LiveBatchSpool,
        fetcher: Callable[[], pd.DataFrame],
        config: MarketMinuteGatewayConfig,
    ) -> None:
        self.spool = spool
        self._fetcher = fetcher
        self.config = config

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ts_code": pd.Series(dtype="string"),
                "trade_time": pd.Series(dtype="datetime64[ns, UTC]"),
                **{
                    column: pd.Series(dtype="float64")
                    for column in MARKET_MINUTE_COLUMNS
                    if column not in {"ts_code", "trade_time"}
                },
            }
        )

    @staticmethod
    def normalize_frame(raw: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(raw, pd.DataFrame):
            raise MarketMinuteValidationError("source result must be a DataFrame")
        missing = sorted(set(MARKET_MINUTE_COLUMNS) - set(raw.columns))
        if missing:
            raise MarketMinuteValidationError(f"missing columns: {missing}")
        frame = raw.loc[:, MARKET_MINUTE_COLUMNS].copy()
        if frame.empty:
            return MarketMinuteGateway._empty_frame()
        frame["ts_code"] = frame["ts_code"].astype("string").str.strip()
        if frame["ts_code"].isna().any() or (frame["ts_code"] == "").any():
            raise MarketMinuteValidationError("ts_code cannot be empty")
        try:
            trade_time = pd.to_datetime(frame["trade_time"], errors="raise")
            if trade_time.dt.tz is None:
                trade_time = trade_time.dt.tz_localize(
                    "Asia/Shanghai",
                    ambiguous="raise",
                    nonexistent="raise",
                )
            frame["trade_time"] = trade_time.dt.tz_convert(UTC)
            for column in MARKET_MINUTE_COLUMNS[2:]:
                frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
        except (TypeError, ValueError) as exc:
            raise MarketMinuteValidationError("invalid market-minute value") from exc
        numeric = frame.loc[:, MARKET_MINUTE_COLUMNS[2:]].to_numpy(dtype="float64")
        if not np.isfinite(numeric).all():
            raise MarketMinuteValidationError("market-minute numeric values must be finite")
        if frame.duplicated(subset=["ts_code", "trade_time"]).any():
            raise MarketMinuteValidationError("duplicate ts_code and trade_time rows")
        return frame.sort_values(["trade_time", "ts_code"], kind="stable").reset_index(drop=True)

    @staticmethod
    def encode_payload(frame: pd.DataFrame) -> bytes:
        output = BytesIO()
        frame.to_parquet(output, index=False)
        return output.getvalue()

    @staticmethod
    def decode_payload(payload: bytes) -> pd.DataFrame:
        frame = pd.read_parquet(BytesIO(payload))
        if "trade_time" in frame:
            frame["trade_time"] = pd.to_datetime(frame["trade_time"], utc=True)
        return frame

    def _latest_envelope(self) -> BatchEnvelope | None:
        current = self.spool.current(LiveChannel.MARKET_MINUTE)
        if current is None:
            return None
        records = self.spool.list_after(
            LiveChannel.MARKET_MINUTE,
            sequence=current.sequence - 1,
        )
        if len(records) != 1:
            raise MarketMinuteValidationError("current live batch cannot be resolved")
        return records[0].envelope

    def capture_once(self, *, received_at: datetime) -> MarketMinuteCapture:
        received = normalize_aware_utc(received_at)
        quality = BatchQualityStatus.PUBLISHED
        degraded_reasons: tuple[str, ...] = ()
        try:
            frame = self.normalize_frame(self._fetcher())
        except MarketMinuteValidationError:
            raise
        except Exception as exc:
            frame = self._empty_frame()
            quality = BatchQualityStatus.STALE
            degraded_reasons = (f"source_error:{type(exc).__name__}",)

        payload = self.encode_payload(frame)
        content_hash = hashlib.sha256(payload).hexdigest()
        if frame.empty:
            event_start = event_end = received
            source_time = received
        else:
            event_start = frame["trade_time"].min().to_pydatetime()
            event_end = frame["trade_time"].max().to_pydatetime()
            source_time = event_end

        latest = self._latest_envelope()
        if (
            latest is not None
            and latest.content_sha256 == content_hash
            and latest.event_time_start == event_start
            and latest.event_time_end == event_end
            and latest.quality_status is quality
            and latest.degraded_reasons == degraded_reasons
        ):
            pointer = self.spool.current(LiveChannel.MARKET_MINUTE)
            if pointer is None:
                raise MarketMinuteValidationError("live current pointer disappeared")
            return MarketMinuteCapture(pointer=pointer, published=False)

        sequence = 0 if latest is None else latest.sequence + 1
        same_market_time = (
            latest is not None
            and latest.event_time_start == event_start
            and latest.event_time_end == event_end
        )
        revision = latest.revision + 1 if same_market_time and latest is not None else 1
        revises_batch_id = latest.batch_id if revision > 1 and latest is not None else None
        identity = {
            "channel": LiveChannel.MARKET_MINUTE,
            "sequence": sequence,
            "revision": revision,
            "event_time_start": event_start,
            "event_time_end": event_end,
            "content_sha256": content_hash,
        }
        batch_id = canonical_sha256(identity)
        envelope = BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.MARKET_MINUTE,
            dataset_id=self.config.dataset_id,
            source=self.config.source,
            source_request_id=canonical_sha256(
                {
                    "source": self.config.source,
                    "event_time_end": event_end,
                    "received_at": received,
                    "content_sha256": content_hash,
                }
            ),
            batch_id=batch_id,
            sequence=sequence,
            revision=revision,
            revises_batch_id=revises_batch_id,
            event_time_start=event_start,
            event_time_end=event_end,
            source_time=source_time,
            received_at=received,
            available_at=max(received, event_end),
            row_count=len(frame),
            content_sha256=content_hash,
            quality_status=quality,
            degraded_reasons=degraded_reasons,
            producer_version=self.config.producer_version,
            producer_commit=self.config.producer_commit,
        )
        pointer = self.spool.publish(envelope, payload)
        return MarketMinuteCapture(pointer=pointer, published=True)
