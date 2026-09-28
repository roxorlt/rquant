from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.alert_price_rule import MarketDayEvidence
from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.price_alert_market_minute_candidate import (
    MarketMinuteQuoteCandidateConfig,
    MarketMinuteQuoteCandidateError,
    price_quotes_from_market_minute_batch,
)
from rquant.serving_price_alert_evaluation import (
    PriceAlertEvaluationInputs,
    PriceQuoteEvidence,
    evaluate_price_alert_batch,
)
from tests.unit.test_web_price_alert_rules import _head, _member

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 25)
CODE = "600001.SH"
OTHER = "000001.SZ"
MISSING = "000002.SZ"
COMMIT = "a" * 40


def _at(hour: int, minute: int, second: int = 0, *, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=SHANGHAI).astimezone(
        UTC
    )


RECEIVED = _at(10, 0, 5)
AVAILABLE = _at(10, 0, 10)
EVALUATED = _at(10, 0, 30)


def _frame(*rows: tuple[str, datetime, object]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_time": source_time,
                "open": 10.0,
                "high": 10.6,
                "low": 9.9,
                "close": close,
                "vol": 100.0,
                "amount": 1050.0,
            }
            for code, source_time, close in rows
        ]
    )


def _payload(frame: pd.DataFrame) -> bytes:
    payload = frame.to_parquet(index=False)
    assert isinstance(payload, bytes)
    return payload


def _batch(
    frame: pd.DataFrame,
    *,
    received_at: datetime = RECEIVED,
    available_at: datetime = AVAILABLE,
) -> tuple[BatchEnvelope, bytes]:
    payload = _payload(frame)
    start = received_at if len(frame) == 0 else min(frame["trade_time"])
    end = received_at if len(frame) == 0 else max(frame["trade_time"])
    return (
        BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.MARKET_MINUTE,
            dataset_id="market_minute",
            source="tushare.rt_min",
            source_request_id="offline-request",
            batch_id="offline-batch",
            sequence=1,
            revision=1,
            event_time_start=start,
            event_time_end=end,
            source_time=end,
            received_at=received_at,
            available_at=available_at,
            row_count=len(frame),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            quality_status=BatchQualityStatus.PUBLISHED,
            producer_version="offline-market-minute-v1",
            producer_commit=COMMIT,
        ),
        payload,
    )


def _changed(envelope: BatchEnvelope, **updates: object) -> BatchEnvelope:
    return BatchEnvelope.model_validate({**envelope.model_dump(mode="python"), **updates})


def _convert(
    envelope: BatchEnvelope,
    payload: bytes,
    *,
    requested_codes: tuple[str, ...] = (CODE,),
    evaluated_at: datetime = EVALUATED,
    config: MarketMinuteQuoteCandidateConfig | None = None,
) -> tuple[PriceQuoteEvidence, ...]:
    return price_quotes_from_market_minute_batch(
        envelope,
        payload,
        requested_codes=requested_codes,
        evaluated_at=evaluated_at,
        config=config or MarketMinuteQuoteCandidateConfig(expected_producer_commit=COMMIT),
    )


def test_pinned_batch_yields_stable_source_time_for_requested_codes_only() -> None:
    envelope, payload = _batch(_frame((OTHER, _at(9, 58), 8.25), (CODE, _at(9, 59), 10.5)))

    quotes = _convert(envelope, payload, requested_codes=(CODE, MISSING, OTHER))

    assert [item.quote.ts_code for item in quotes] == [CODE, OTHER]
    assert [item.quote.observed_at for item in quotes] == [_at(9, 59), _at(9, 58)]
    assert all(item.source_timestamp_provenance == "provider_source_timestamp" for item in quotes)
    assert _convert(envelope, payload, requested_codes=(CODE, MISSING, OTHER)) == quotes


def test_gateway_normalized_payload_shape_is_accepted_without_fetch_or_spool() -> None:
    normalized = MarketMinuteGateway.normalize_frame(_frame((CODE, _at(9, 59), 10.5)))
    envelope, payload = _batch(normalized)
    assert _convert(envelope, payload)[0].quote.observed_at == _at(9, 59)


def test_full_market_batch_over_one_thousand_rows_still_reuses_last_requested_quote() -> None:
    last_code = "001000.SH"
    frame = _frame(*[(f"{number:06d}.SH", _at(9, 59), 10.5) for number in range(1001)])
    envelope, payload = _batch(frame)
    quotes = _convert(envelope, payload, requested_codes=(last_code,))
    assert [item.quote.ts_code for item in quotes] == [last_code]


def test_one_candidate_quote_is_shared_by_two_rules_and_missing_code_stays_unavailable() -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), 10.5)))
    quotes = _convert(envelope, payload, requested_codes=(CODE, MISSING))
    inputs = PriceAlertEvaluationInputs(
        availability="ready",
        generation_id="generation",
        source_generation_id="source-generation",
        evaluated_at=EVALUATED,
        available_at=AVAILABLE,
        rules=(
            _head("alice", "a", CODE, 1),
            _head("bob", "b", CODE, 1),
            _head("alice", "c", MISSING, 1),
        ),
        members=(
            _member("alice", CODE, 1),
            _member("bob", CODE, 1),
            _member("alice", MISSING, 1),
        ),
    )

    result = evaluate_price_alert_batch(
        inputs,
        market=MarketDayEvidence(trade_date=DAY, is_trading_day=True),
        quotes=quotes,
        max_quote_age_seconds=120,
    )

    assert result.delivery_eligible is False
    assert [(row.owner_id, row.rule_id, row.state, row.reason) for row in result.results] == [
        ("alice", "a", "triggered", "threshold_reached"),
        ("bob", "b", "triggered", "threshold_reached"),
        ("alice", "c", "unavailable", "quote_missing"),
    ]


@pytest.mark.parametrize(
    "updates",
    [
        {"channel": LiveChannel.WATCHLIST_QUOTE, "dataset_id": "watchlist_quote"},
        {"dataset_id": "wrong_dataset"},
        {"source": "other.rt_min"},
        {"producer_commit": "b" * 40},
        {"quality_status": BatchQualityStatus.DEGRADED, "degraded_reasons": ("offline",)},
        {"schema_version": 2},
        {"available_at": EVALUATED + timedelta(seconds=1)},
    ],
)
def test_wrong_envelope_identity_or_future_availability_rejects_whole_batch(
    updates: dict[str, object],
) -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), 10.5)))
    with pytest.raises(MarketMinuteQuoteCandidateError):
        _convert(_changed(envelope, **updates), payload)


def test_payload_digest_or_parquet_decode_failure_rejects_whole_batch() -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), 10.5)))
    with pytest.raises(MarketMinuteQuoteCandidateError, match="digest"):
        _convert(envelope, payload + b"changed")
    corrupt = b"not a parquet payload"
    with pytest.raises(MarketMinuteQuoteCandidateError, match="parquet"):
        _convert(_changed(envelope, content_sha256=hashlib.sha256(corrupt).hexdigest()), corrupt)


@pytest.mark.parametrize(
    "updates",
    [
        {"row_count": 2},
        {"source_time": _at(9, 58)},
        {"event_time_start": _at(9, 58)},
        {"event_time_end": _at(10, 0)},
    ],
)
def test_row_count_or_source_window_mismatch_rejects_whole_batch(
    updates: dict[str, object],
) -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), 10.5)))
    with pytest.raises(MarketMinuteQuoteCandidateError):
        _convert(_changed(envelope, **updates), payload)


def test_bytes_and_rows_are_bounded_before_accepting_batch() -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), 10.5), (OTHER, _at(9, 58), 8.25)))
    with pytest.raises(MarketMinuteQuoteCandidateError, match="bytes"):
        _convert(
            envelope,
            payload,
            config=MarketMinuteQuoteCandidateConfig(
                expected_producer_commit=COMMIT, max_payload_bytes=len(payload) - 1
            ),
        )
    with pytest.raises(MarketMinuteQuoteCandidateError, match="rows"):
        _convert(
            envelope,
            payload,
            config=MarketMinuteQuoteCandidateConfig(expected_producer_commit=COMMIT, max_rows=1),
        )


def test_missing_required_column_or_nonfinite_numeric_rejects_even_extra_stock() -> None:
    complete = _frame((CODE, _at(9, 59), 10.5), (OTHER, _at(9, 58), 8.25))
    for malformed in (
        complete.drop(columns=["close"]),
        complete.assign(open=[10.0, float("nan")]),
    ):
        envelope, payload = _batch(malformed)
        with pytest.raises(MarketMinuteQuoteCandidateError):
            _convert(envelope, payload, requested_codes=(CODE,))


def test_numeric_strings_cannot_masquerade_as_gateway_normalized_floats() -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), "10.5")))
    with pytest.raises(MarketMinuteQuoteCandidateError, match="numeric"):
        _convert(envelope, payload)


@pytest.mark.parametrize(
    "frame",
    [
        _frame((CODE, _at(9, 58), 10.5), (CODE, _at(9, 59), 10.5)),
        _frame((CODE, _at(9, 59), 10.5), (OTHER, _at(9, 58), 0)),
        _frame((CODE, _at(9, 59), 10.5), (OTHER, _at(12, 0), 8.25)),
        _frame((CODE, _at(9, 59), 10.5), (OTHER, _at(9, 58, day=DAY - timedelta(days=1)), 8.25)),
        _frame((CODE, _at(9, 59), 10.5), (OTHER, _at(10, 1), 8.25)),
    ],
)
def test_duplicate_bad_price_lunch_wrong_day_or_future_extra_stock_rejects_entire_batch(
    frame: pd.DataFrame,
) -> None:
    envelope, payload = _batch(frame)
    with pytest.raises(MarketMinuteQuoteCandidateError):
        _convert(envelope, payload, requested_codes=(CODE,))


def test_unrequested_lunch_minute_rejects_batch_even_after_lunch() -> None:
    envelope, payload = _batch(
        _frame((CODE, _at(11, 29), 10.5), (OTHER, _at(12, 0), 8.25)),
        received_at=_at(13, 0, 5),
        available_at=_at(13, 0, 10),
    )
    with pytest.raises(MarketMinuteQuoteCandidateError, match="quote facts"):
        _convert(envelope, payload, evaluated_at=_at(13, 0, 30), requested_codes=(CODE,))


def test_request_codes_and_evaluation_time_must_be_explicit_and_bounded() -> None:
    envelope, payload = _batch(_frame((CODE, _at(9, 59), 10.5)))
    with pytest.raises(ValueError, match="requested_codes"):
        _convert(envelope, payload, requested_codes=(CODE, CODE))
    with pytest.raises(ValueError, match="requested_codes"):
        _convert(envelope, payload, requested_codes=tuple(f"{n:06d}.SH" for n in range(1001)))
    with pytest.raises(ValueError, match="aware"):
        _convert(envelope, payload, evaluated_at=datetime(2026, 9, 25, 10, 0, 30))


def test_valid_empty_batch_returns_no_quote() -> None:
    frame = _frame((CODE, _at(9, 59), 10.5)).iloc[:0].copy()
    envelope, payload = _batch(frame)
    assert _convert(envelope, payload) == ()
