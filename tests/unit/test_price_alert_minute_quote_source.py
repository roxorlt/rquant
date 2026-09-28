from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.adapter.tushare import TushareAdapter
from rquant.alert_price_rule import MarketDayEvidence
from rquant.price_alert_minute_quote_source import price_quote_evidence_from_rt_min
from rquant.serving_price_alert_evaluation import (
    PriceQuoteEvidence,
    evaluate_price_alert_batch,
    read_price_alert_evaluation_inputs,
)
from tests.unit.test_web_price_alert_rules import _head, _member, _publish

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 25)
CODE = "600001.SH"
SECOND_CODE = "000001.SZ"
RECEIVED = datetime(2026, 9, 25, 10, 0, 45, tzinfo=SHANGHAI)


def _frame(*rows: tuple[str, object, object, object, object]) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        rows, columns=["ts_code", "trade_time", "close", "freq", "source"]
    )


def _bar(
    code: str = CODE,
    trade_time: object = datetime(2026, 9, 25, 9, 59),
    close: object = 10.5,
    freq: object = "1min",
    source: object = "tushare_rt",
) -> tuple[str, object, object, object, object]:
    return code, trade_time, close, freq, source


def _convert(
    frame: pd.DataFrame,
    *,
    received_at: datetime = RECEIVED,
    requested_codes: tuple[str, ...] = (CODE,),
    trade_date: date = DAY,
) -> tuple[PriceQuoteEvidence, ...]:
    return price_quote_evidence_from_rt_min(
        frame,
        trade_date=trade_date,
        received_at=received_at,
        requested_codes=requested_codes,
    )


def test_source_minute_is_shanghai_local_not_response_time() -> None:
    frame = _frame(
        _bar(SECOND_CODE, pd.Timestamp("2026-09-25 01:58:00+00:00"), "8.25"),
        _bar(CODE, pd.Timestamp("2026-09-25 09:59:00"), 10.5),
    )

    quotes = _convert(frame, requested_codes=(CODE, SECOND_CODE))

    assert [item.quote.ts_code for item in quotes] == [CODE, SECOND_CODE]
    assert [item.quote.observed_at for item in quotes] == [
        datetime(2026, 9, 25, 1, 59, tzinfo=UTC),
        datetime(2026, 9, 25, 1, 58, tzinfo=UTC),
    ]
    assert [item.quote.price for item in quotes] == [Decimal("10.5"), Decimal("8.25")]
    assert all(item.quote.trade_date == DAY for item in quotes)
    assert all(item.source_timestamp_provenance == "provider_source_timestamp" for item in quotes)


def test_existing_tushare_normalization_feeds_quote_projection_without_network() -> None:
    class OfflinePro:
        def rt_min(self, *, ts_code: str, freq: str) -> pd.DataFrame:
            assert (ts_code, freq) == (CODE, "1MIN")
            return pd.DataFrame(
                [
                    {
                        "ts_code": CODE,
                        "time": "2026-09-25 09:59:00",
                        "open": 10.2,
                        "high": 10.6,
                        "low": 10.1,
                        "close": 10.5,
                        "vol": 100,
                        "amount": 1050,
                    }
                ]
            )

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = OfflinePro()
    normalized = adapter.rt_min([CODE], freq="1min")

    assert list(normalized["trade_time"]) == [pd.Timestamp("2026-09-25 09:59:00")]
    assert normalized.loc[0, "source"] == "tushare_rt"
    assert _convert(normalized)[0].quote.observed_at == datetime(2026, 9, 25, 1, 59, tzinfo=UTC)


def test_empty_response_and_missing_requested_code_produce_no_invented_quote() -> None:
    assert _convert(pd.DataFrame(), requested_codes=(CODE, SECOND_CODE)) == ()
    quotes = _convert(_frame(_bar()), requested_codes=(CODE, SECOND_CODE))
    assert [item.quote.ts_code for item in quotes] == [CODE]


def test_nonzero_rows_without_quote_columns_are_malformed() -> None:
    with pytest.raises(ValueError, match="columns"):
        _convert(pd.DataFrame(index=[0]))


@pytest.mark.parametrize("close", [None, 0, -0.01, float("nan"), float("inf"), "bad", True])
def test_invalid_close_cannot_become_quote(close: object) -> None:
    assert _convert(_frame(_bar(close=close))) == ()


@pytest.mark.parametrize(
    "source_time",
    [
        None,
        pd.NaT,
        "2026-09-25 09:59:00",
        datetime(2026, 9, 25, 10, 0, 30),
        datetime(2026, 9, 25, 10, 1),
        datetime(2026, 9, 24, 9, 59),
        datetime(2026, 9, 25, 9, 29),
        datetime(2026, 9, 25, 11, 30),
        datetime(2026, 9, 25, 12, 59),
        datetime(2026, 9, 25, 14, 57),
    ],
)
def test_missing_invalid_future_wrong_day_or_outside_session_time_cannot_become_quote(
    source_time: object,
) -> None:
    assert _convert(_frame(_bar(trade_time=source_time))) == ()


@pytest.mark.parametrize("minute", [(9, 30), (11, 29), (13, 0), (14, 56)])
def test_continuous_session_minute_boundaries_are_accepted(minute: tuple[int, int]) -> None:
    received = datetime(2026, 9, 25, 15, 0, tzinfo=SHANGHAI)
    source_time = datetime(2026, 9, 25, *minute)
    assert len(_convert(_frame(_bar(trade_time=source_time)), received_at=received)) == 1


@pytest.mark.parametrize(
    "bar", [_bar(freq="5min"), _bar(source="tushare"), _bar(freq=pd.NA), _bar(source=pd.NA)]
)
def test_wrong_frequency_or_source_cannot_be_labelled_trusted(
    bar: tuple[str, object, object, object, object],
) -> None:
    with pytest.raises(ValueError, match="source|freq"):
        _convert(_frame(_bar(SECOND_CODE), bar), requested_codes=(CODE, SECOND_CODE))


@pytest.mark.parametrize("bar", [_bar(freq=pd.NA), _bar(source=pd.NA)])
def test_missing_source_marker_is_rejected_as_invalid_batch(
    bar: tuple[str, object, object, object, object],
) -> None:
    with pytest.raises(ValueError, match="source|freq"):
        _convert(_frame(bar))


@pytest.mark.parametrize(
    "frame",
    [
        _frame(_bar(), _bar(close=11)),
        _frame(_bar(), _bar(SECOND_CODE)),
    ],
)
def test_duplicate_or_unrequested_code_rejects_entire_response(frame: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="duplicate|unrequested"):
        _convert(frame)


def test_request_requires_distinct_bounded_codes_and_aware_receive_time() -> None:
    with pytest.raises(ValueError, match="requested_codes"):
        _convert(_frame(_bar()), requested_codes=(CODE, CODE))
    with pytest.raises(ValueError, match="requested_codes"):
        _convert(
            pd.DataFrame(), requested_codes=tuple(f"{number:06d}.SH" for number in range(1001))
        )
    with pytest.raises(ValueError, match="aware"):
        _convert(_frame(_bar()), received_at=datetime(2026, 9, 25, 10, 0, 45))


def test_quote_is_reused_across_owners_while_missing_code_stays_unavailable(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(
            _head("alice", "a", CODE, 1),
            _head("bob", "b", CODE, 1),
            _head("alice", "c", SECOND_CODE, 1),
        ),
        members=(
            _member("alice", CODE, 1),
            _member("bob", CODE, 1),
            _member("alice", SECOND_CODE, 1),
        ),
    )
    inputs = read_price_alert_evaluation_inputs(
        root, evaluated_at=RECEIVED, max_generation_age=timedelta(days=1)
    )
    quotes = _convert(_frame(_bar()), requested_codes=(CODE, SECOND_CODE))

    batch = evaluate_price_alert_batch(
        inputs,
        market=MarketDayEvidence(trade_date=DAY, is_trading_day=True),
        quotes=quotes,
        max_quote_age_seconds=120,
    )

    assert batch.delivery_eligible is False
    assert [(item.owner_id, item.rule_id, item.state, item.reason) for item in batch.results] == [
        ("alice", "a", "triggered", "threshold_reached"),
        ("alice", "c", "unavailable", "quote_missing"),
        ("bob", "b", "triggered", "threshold_reached"),
    ]


def test_evaluator_rejects_quote_across_lunch_and_regressed_source_time(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rules=(_head("alice", "a", CODE, 1),), members=(_member("alice", CODE, 1),))
    market = MarketDayEvidence(trade_date=DAY, is_trading_day=True)
    after_lunch = datetime(2026, 9, 25, 13, 0, tzinfo=SHANGHAI)
    lunch_quote = _convert(
        _frame(_bar(trade_time=datetime(2026, 9, 25, 11, 29))), received_at=after_lunch
    )
    lunch_inputs = read_price_alert_evaluation_inputs(
        root, evaluated_at=after_lunch, max_generation_age=timedelta(days=1)
    )
    lunch_batch = evaluate_price_alert_batch(
        lunch_inputs, market=market, quotes=lunch_quote, max_quote_age_seconds=7200
    )
    assert (lunch_batch.results[0].state, lunch_batch.results[0].reason) == (
        "unavailable",
        "quote_outside_continuous_session",
    )

    quote = _convert(_frame(_bar()))[0]
    regressed = PriceQuoteEvidence(
        quote=quote.quote,
        source_timestamp_provenance=quote.source_timestamp_provenance,
        previous_source_observed_at=datetime(2026, 9, 25, 10, 0, tzinfo=SHANGHAI),
    )
    morning_inputs = read_price_alert_evaluation_inputs(
        root, evaluated_at=RECEIVED, max_generation_age=timedelta(days=1)
    )
    morning_batch = evaluate_price_alert_batch(
        morning_inputs, market=market, quotes=(regressed,), max_quote_age_seconds=120
    )
    assert (morning_batch.results[0].state, morning_batch.results[0].reason) == (
        "unavailable",
        "quote_source_time_regressed",
    )
