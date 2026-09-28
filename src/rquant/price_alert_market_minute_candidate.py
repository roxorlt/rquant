"""Pure price quote candidates from a sealed market-minute batch and its payload."""

from __future__ import annotations

import hashlib
from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field, StrictInt, TypeAdapter, ValidationError

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    CommitSha,
    LiveChannel,
)
from rquant.manual_watchlist import TsCode
from rquant.market_minute_gateway import MARKET_MINUTE_COLUMNS
from rquant.price_alert_minute_quote_source import price_quote_evidence_from_rt_min
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.serving_price_alert_evaluation import PriceQuoteEvidence

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODES = TypeAdapter(tuple[TsCode, ...])
_MAX_REQUEST_CODES = 1000
_MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
_MAX_BATCH_ROWS = 10_000


class MarketMinuteQuoteCandidateError(ValueError):
    """A sealed batch cannot establish a trustworthy minute quote candidate."""


class MarketMinuteQuoteCandidateConfig(RuntimeContractModel):
    expected_producer_commit: CommitSha
    max_payload_bytes: StrictInt = Field(default=_MAX_PAYLOAD_BYTES, ge=1, le=_MAX_PAYLOAD_BYTES)
    max_rows: StrictInt = Field(default=_MAX_BATCH_ROWS, ge=1, le=_MAX_BATCH_ROWS)


def _requested_codes(codes: tuple[TsCode, ...]) -> tuple[str, ...]:
    if not 1 <= len(codes) <= _MAX_REQUEST_CODES:
        raise ValueError("requested_codes must contain 1 to 1000 codes")
    try:
        validated = _CODES.validate_python(codes)
    except ValidationError as exc:
        raise ValueError("requested_codes contain an invalid code") from exc
    if len(set(validated)) != len(validated):
        raise ValueError("requested_codes must be distinct")
    return validated


def _decode_bounded_payload(
    payload: bytes, *, envelope: BatchEnvelope, config: MarketMinuteQuoteCandidateConfig
) -> pd.DataFrame:
    if type(payload) is not bytes or not 0 < len(payload) <= config.max_payload_bytes:
        raise MarketMinuteQuoteCandidateError("market-minute payload bytes exceed bound")
    if envelope.row_count > config.max_rows:
        raise MarketMinuteQuoteCandidateError("market-minute rows exceed bound")
    if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
        raise MarketMinuteQuoteCandidateError("market-minute payload digest mismatch")
    try:
        parquet = pq.ParquetFile(BytesIO(payload))
    except Exception as exc:
        raise MarketMinuteQuoteCandidateError("market-minute parquet cannot be decoded") from exc
    if (
        parquet.metadata.num_rows != envelope.row_count
        or parquet.metadata.num_rows > config.max_rows
    ):
        raise MarketMinuteQuoteCandidateError("market-minute parquet rows disagree with envelope")
    if tuple(parquet.schema_arrow.names) != MARKET_MINUTE_COLUMNS:
        raise MarketMinuteQuoteCandidateError("market-minute parquet lacks required columns")
    schema = parquet.schema_arrow
    code_type = schema.field("ts_code").type
    if not (pa.types.is_string(code_type) or pa.types.is_large_string(code_type)) or any(
        not pa.types.is_float64(schema.field(column).type) for column in MARKET_MINUTE_COLUMNS[2:]
    ):
        raise MarketMinuteQuoteCandidateError("market-minute parquet numeric schema differs")
    try:
        frame = parquet.read(columns=list(MARKET_MINUTE_COLUMNS), use_threads=False).to_pandas()
    except Exception as exc:
        raise MarketMinuteQuoteCandidateError("market-minute parquet cannot be decoded") from exc
    if len(frame) != envelope.row_count or tuple(frame.columns) != MARKET_MINUTE_COLUMNS:
        raise MarketMinuteQuoteCandidateError("market-minute decoded rows or columns disagree")
    return frame


def price_quotes_from_market_minute_batch(
    envelope: BatchEnvelope,
    payload: bytes,
    *,
    requested_codes: tuple[TsCode, ...],
    evaluated_at: datetime,
    config: MarketMinuteQuoteCandidateConfig,
) -> tuple[PriceQuoteEvidence, ...]:
    """Validate the whole published batch, then return requested quotes in request order.

    The producer commit must be approved by deployment configuration: the payload has no
    frequency field, so the source label alone cannot establish one-minute semantics.
    """
    requested = _requested_codes(requested_codes)
    evaluated = normalize_aware_utc(evaluated_at)
    if (
        envelope.schema_version != 1
        or envelope.channel is not LiveChannel.MARKET_MINUTE
        or envelope.dataset_id != "market_minute"
        or envelope.source != "tushare.rt_min"
        or envelope.quality_status is not BatchQualityStatus.PUBLISHED
        or envelope.producer_commit != config.expected_producer_commit
    ):
        raise MarketMinuteQuoteCandidateError("market-minute envelope identity is untrusted")
    if envelope.available_at > evaluated:
        raise MarketMinuteQuoteCandidateError("market-minute batch is not yet available")
    trade_date = evaluated.astimezone(_SHANGHAI).date()
    if envelope.available_at.astimezone(_SHANGHAI).date() != trade_date:
        raise MarketMinuteQuoteCandidateError("market-minute batch is from another trade date")

    frame = _decode_bounded_payload(payload, envelope=envelope, config=config)
    if (
        not isinstance(frame["trade_time"].dtype, pd.DatetimeTZDtype)
        or str(frame["trade_time"].dtype.tz) != "UTC"
    ):
        raise MarketMinuteQuoteCandidateError("market-minute source times must be UTC aware")
    try:
        numeric = frame.loc[:, MARKET_MINUTE_COLUMNS[2:]].apply(pd.to_numeric, errors="raise")
        finite = np.isfinite(numeric.to_numpy(dtype="float64")).all()
    except (TypeError, ValueError, OverflowError) as exc:
        raise MarketMinuteQuoteCandidateError("market-minute values must be finite") from exc
    if not finite:
        raise MarketMinuteQuoteCandidateError("market-minute values must be finite")
    try:
        all_codes = _CODES.validate_python(tuple(frame["ts_code"]))
    except ValidationError as exc:
        raise MarketMinuteQuoteCandidateError("market-minute batch has an invalid code") from exc
    if len(set(all_codes)) != len(all_codes):
        raise MarketMinuteQuoteCandidateError("market-minute batch has a duplicate code")
    if not all_codes:
        return ()

    normalized = frame.loc[:, ["ts_code", "trade_time", "close"]].copy()
    normalized["freq"] = "1min"
    normalized["source"] = "tushare_rt"
    evidence: list[PriceQuoteEvidence] = []
    for start in range(0, len(frame), _MAX_REQUEST_CODES):
        end = start + _MAX_REQUEST_CODES
        chunk = normalized.iloc[start:end]
        try:
            # The envelope's received_at is request start; available_at bounds completed data.
            converted = price_quote_evidence_from_rt_min(
                chunk,
                trade_date=trade_date,
                received_at=envelope.available_at,
                requested_codes=all_codes[start:end],
            )
        except ValueError as exc:
            raise MarketMinuteQuoteCandidateError(
                "market-minute batch has invalid quote facts"
            ) from exc
        if len(converted) != len(chunk):
            raise MarketMinuteQuoteCandidateError("market-minute batch has invalid quote facts")
        evidence.extend(converted)

    observed = tuple(item.quote.observed_at for item in evidence)
    if (
        min(observed) != envelope.event_time_start
        or max(observed) != envelope.event_time_end
        or envelope.source_time != envelope.event_time_end
        or envelope.event_time_end > envelope.available_at
    ):
        raise MarketMinuteQuoteCandidateError("market-minute source time or event window differs")
    by_code = {item.quote.ts_code: item for item in evidence}
    return tuple(by_code[code] for code in requested if code in by_code)
