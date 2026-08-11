from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from rquant.feature_contracts import FeatureAvailability
from rquant.intraday_feature_engine import (
    STATUS_COLUMNS,
    FeatureComputationMode,
    IntradayFeatureConfig,
    IntradayFeatureValidationError,
    _as_shanghai_timestamp,
    live_compute,
    replay_compute,
)

SHANGHAI = timezone(timedelta(hours=8))
PRODUCER_COMMIT = "a" * 40


def _config(
    *,
    lookback_sessions: int = 2,
    opening_acceleration_block_minutes: int = 3,
) -> IntradayFeatureConfig:
    return IntradayFeatureConfig(
        lookback_sessions=lookback_sessions,
        opening_acceleration_block_minutes=opening_acceleration_block_minutes,
        producer_commit=PRODUCER_COMMIT,
    )


def _minute(
    ts_code: str,
    trade_time: datetime,
    *,
    open_: float,
    close: float,
    vol: float,
    amount: float,
    high: float | None = None,
    low: float | None = None,
    available_at: datetime | None = None,
) -> dict[str, object]:
    local_bar_end = trade_time.replace(tzinfo=trade_time.tzinfo or SHANGHAI)
    return {
        "ts_code": ts_code,
        "trade_time": trade_time,
        "available_at": available_at or local_bar_end + timedelta(seconds=2),
        "open": open_,
        "high": max(open_, close) if high is None else high,
        "low": min(open_, close) if low is None else low,
        "close": close,
        "vol": vol,
        "amount": amount,
    }


def _current_minutes(*, ts_code: str = "600000.SH") -> pd.DataFrame:
    closes = (10.0, 11.0, 10.5, 12.0, 12.0, 11.0, 12.0, 13.0, 12.0, 13.0, 14.0)
    return pd.DataFrame(
        [
            _minute(
                ts_code,
                datetime(2026, 7, 31, 9, 30 + offset),
                open_=closes[offset - 1] if offset else closes[0],
                close=close,
                vol=100.0,
                amount=float((offset + 1) * 1_000),
            )
            for offset, close in enumerate(closes)
        ]
    )


def _historical_minutes(*, ts_code: str = "600000.SH") -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day, scale in ((29, 500.0), (30, 1_000.0)):
        for offset in range(11):
            close = 10.0 + offset * 0.1
            amount = scale * (offset + 1)
            rows.append(
                _minute(
                    ts_code,
                    datetime(2026, 7, day, 9, 30 + offset),
                    open_=close,
                    close=close,
                    vol=amount / close,
                    amount=amount,
                )
            )
    return pd.DataFrame(rows)


def _compute(
    current: pd.DataFrame | None = None,
    historical: pd.DataFrame | None = None,
    *,
    decision_time: datetime = datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI),
    input_available_at: datetime | None = None,
    config: IntradayFeatureConfig | None = None,
):
    return live_compute(
        _current_minutes() if current is None else current,
        _historical_minutes() if historical is None else historical,
        decision_time=decision_time,
        input_available_at=input_available_at or decision_time,
        input_batch_ids=("history-0002", "current-0007"),
        sequence=7,
        config=config or _config(),
    )


def test_computes_only_closed_pit_bars_and_explicit_tick_rule_proxies() -> None:
    result = _compute()
    row = result.frame.iloc[0]

    assert result.mode is FeatureComputationMode.LIVE
    assert row["feature_time"] == pd.Timestamp("2026-07-31 01:40:00+00:00")
    assert row["minute_amount"] == pytest.approx(11_000.0)
    assert row["cumulative_amount"] == pytest.approx(66_000.0)
    assert row["hist_same_minute_amount_median"] == pytest.approx(8_250.0)
    assert row["hist_cumulative_amount_median"] == pytest.approx(49_500.0)
    assert row["rel_same_minute"] == pytest.approx(11_000.0 / 8_250.0)
    assert row["rel_cumulative"] == pytest.approx(66_000.0 / 49_500.0)
    assert row["amount_accel_5m"] == pytest.approx(11_000.0 / 8_000.0)
    assert row["amount_accel_10m"] == pytest.approx(11_000.0 / 5_500.0)
    assert row["tick_rule_buy_volume_proxy"] == pytest.approx(700.0)
    assert row["tick_rule_sell_volume_proxy"] == pytest.approx(400.0)
    assert row["tick_rule_buy_sell_ratio_proxy"] == pytest.approx(1.75)
    assert row["tick_rule_proxy_method"] == "minute_close_vs_previous_close"
    assert row["tick_rule_proxy_quality"] == "proxy_not_order_flow"
    assert not {"outer_volume", "inner_volume", "outer_inner_ratio"} & set(result.frame.columns)


def test_feature_availability_is_scoped_to_candidate_event_time() -> None:
    decision = datetime(2026, 7, 31, 9, 31, 2, tzinfo=SHANGHAI)
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                close=10.1,
                vol=100.0,
                amount=1_000.0,
            ),
            _minute(
                "600001.SH",
                datetime(2026, 7, 31, 9, 31),
                open_=20.0,
                close=20.1,
                vol=100.0,
                amount=2_000.0,
            ),
        ]
    )
    historical = pd.concat(
        [_historical_minutes(), _historical_minutes(ts_code="600001.SH")],
        ignore_index=True,
    )

    result = _compute(
        current=current,
        historical=historical,
        decision_time=decision,
        input_available_at=decision,
    )

    older = result.envelope.field_status("latest_close", candidate_id="600000.SH")
    newer = result.envelope.field_status("latest_close", candidate_id="600001.SH")
    assert older is not None and newer is not None
    assert older.source_event_time == datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
    assert older.actual_delay_seconds == pytest.approx(62.0)
    assert newer.source_event_time == datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    assert newer.actual_delay_seconds == pytest.approx(2.0)


def test_computes_price_and_volume_geometry_from_visible_minute_prefix() -> None:
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                high=10.4,
                low=9.8,
                close=10.2,
                vol=100.0,
                amount=1_000.0,
            ),
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 31),
                open_=10.2,
                high=10.7,
                low=10.1,
                close=10.6,
                vol=250.0,
                amount=2_500.0,
            ),
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 32),
                open_=10.6,
                high=10.8,
                low=9.7,
                close=9.9,
                vol=50.0,
                amount=500.0,
            ),
        ]
    )

    result = _compute(
        current=current,
        decision_time=datetime(2026, 7, 31, 9, 32, 2, tzinfo=SHANGHAI),
    )
    row = result.frame.iloc[0]

    expected = {
        "latest_open": 10.6,
        "latest_high": 10.8,
        "latest_low": 9.7,
        "latest_close": 9.9,
        "minute_volume": 50.0,
        "cumulative_volume": 400.0,
        "session_open": 10.0,
        "session_high": 10.8,
        "session_low": 9.7,
        "opening_bar_open": 10.0,
        "opening_bar_high": 10.4,
        "opening_bar_low": 9.8,
        "opening_bar_close": 10.2,
    }
    for name, value in expected.items():
        assert row[name] == pytest.approx(value)
        status = result.envelope.field_status(name)
        assert status is not None
        assert status.status is FeatureAvailability.AVAILABLE
        assert status.reason is None


def test_future_bars_do_not_change_prior_decision_payload_or_envelope() -> None:
    decision = datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI)
    prefix = _current_minutes()
    future = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 41),
                open_=14.0,
                high=99.0,
                low=1.0,
                close=15.0,
                vol=999_999.0,
                amount=9_999_999.0,
            )
        ]
    )

    before = _compute(current=prefix, decision_time=decision)
    after = _compute(
        current=pd.concat([prefix, future], ignore_index=True),
        decision_time=decision,
    )

    assert after.payload_bytes == before.payload_bytes
    assert after.envelope == before.envelope


def test_next_trade_date_bars_do_not_change_prior_decision_prefix() -> None:
    decision = datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI)
    prefix = _current_minutes()
    next_trade_date = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 8, 3, 9, 30),
                open_=50.0,
                high=80.0,
                low=1.0,
                close=60.0,
                vol=999_999.0,
                amount=9_999_999.0,
            )
        ]
    )
    next_trade_date.loc[0, "available_at"] = decision - timedelta(days=1)

    before = _compute(current=prefix, decision_time=decision)
    after = _compute(
        current=pd.concat([prefix, next_trade_date], ignore_index=True),
        decision_time=decision,
    )

    assert after.payload_bytes == before.payload_bytes
    assert after.envelope == before.envelope


def test_rejects_closed_current_rows_not_yet_available() -> None:
    unavailable = _current_minutes()
    unavailable.loc[unavailable.index[-1], "available_at"] = datetime(
        2026, 7, 31, 9, 40, 3, tzinfo=SHANGHAI
    )
    with pytest.raises(IntradayFeatureValidationError, match="not available"):
        _compute(current=unavailable)

    with pytest.raises(IntradayFeatureValidationError, match="input_available_at"):
        _compute(input_available_at=datetime(2026, 7, 31, 9, 40, 3, tzinfo=SHANGHAI))


def test_rejects_subminute_bars_even_when_natural_minute_already_exists() -> None:
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                close=10.1,
                vol=100.0,
                amount=1_000.0,
            ),
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30, 30),
                open_=10.1,
                close=10.2,
                vol=100.0,
                amount=1_000.0,
            ),
        ]
    )

    with pytest.raises(IntradayFeatureValidationError, match="whole-minute"):
        _compute(
            current=current,
            decision_time=datetime(2026, 7, 31, 9, 31, tzinfo=SHANGHAI),
        )


@pytest.mark.parametrize("target", ["current", "historical"])
def test_rejects_bars_outside_continuous_auction_sessions(target: str) -> None:
    lunch_bar = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 30 if target == "historical" else 31, 12, 0),
                open_=10.0,
                close=10.1,
                vol=100.0,
                amount=1_000.0,
            )
        ]
    )
    current = lunch_bar if target == "current" else _current_minutes()
    historical = lunch_bar if target == "historical" else _historical_minutes()
    decision = (
        datetime(2026, 7, 31, 12, 0, 2, tzinfo=SHANGHAI)
        if target == "current"
        else datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI)
    )

    with pytest.raises(IntradayFeatureValidationError, match="continuous auction session"):
        _compute(
            current=current,
            historical=historical,
            decision_time=decision,
        )


@pytest.mark.parametrize(
    ("high", "low", "close"),
    [
        (10.0, 9.8, 10.1),
        (10.2, 10.0, 9.9),
    ],
)
@pytest.mark.parametrize("target", ["current", "historical"])
def test_rejects_invalid_ohlc_geometry(
    target: str,
    high: float,
    low: float,
    close: float,
) -> None:
    day = 30 if target == "historical" else 31
    bad_bar = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, day, 9, 40),
                open_=10.0,
                high=high,
                low=low,
                close=close,
                vol=100.0,
                amount=1_000.0,
            )
        ]
    )
    current = bad_bar if target == "current" else _current_minutes()
    historical = bad_bar if target == "historical" else _historical_minutes()
    with pytest.raises(IntradayFeatureValidationError, match="OHLC geometry"):
        _compute(current=current, historical=historical)


@pytest.mark.parametrize("price", [0.0, -1.0])
@pytest.mark.parametrize("target", ["current", "historical"])
def test_rejects_nonpositive_ohlc_prices(target: str, price: float) -> None:
    day = 30 if target == "historical" else 31
    bad_bar = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, day, 9, 40),
                open_=price,
                high=price,
                low=price,
                close=price,
                vol=100.0,
                amount=1_000.0,
            )
        ]
    )

    with pytest.raises(IntradayFeatureValidationError, match="strictly positive"):
        _compute(
            current=bad_bar if target == "current" else _current_minutes(),
            historical=bad_bar if target == "historical" else _historical_minutes(),
        )


@pytest.mark.parametrize(
    "case",
    [
        "empty_code",
        "bad_numeric",
        "bad_ohlc",
        "outside_session",
        "subminute",
        "bad_available",
    ],
)
def test_future_current_row_contents_do_not_change_visible_prefix(case: str) -> None:
    decision = datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI)
    prefix = _current_minutes()
    future = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 8, 3, 9, 30),
                open_=50.0,
                high=50.2,
                low=49.8,
                close=50.1,
                vol=100.0,
                amount=5_000.0,
            )
        ],
        dtype="object",
    )
    if case == "empty_code":
        future.loc[0, "ts_code"] = ""
    elif case == "bad_numeric":
        future.loc[0, "vol"] = "not-a-number"
    elif case == "bad_ohlc":
        future.loc[0, "high"] = 49.0
    elif case == "outside_session":
        future.loc[0, "trade_time"] = datetime(2026, 8, 3, 12, 0)
    elif case == "subminute":
        future.loc[0, "trade_time"] = datetime(2026, 8, 3, 9, 30, 30)
    elif case == "bad_available":
        future.loc[0, "available_at"] = "not-a-timestamp"

    before = _compute(current=prefix, decision_time=decision)
    after = _compute(
        current=pd.concat([prefix, future], ignore_index=True),
        decision_time=decision,
    )

    assert after.payload_bytes == before.payload_bytes
    assert after.envelope == before.envelope


def test_unparseable_future_current_trade_time_fails_closed() -> None:
    future = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 8, 3, 9, 30),
                open_=50.0,
                close=50.1,
                vol=100.0,
                amount=5_000.0,
            )
        ],
        dtype="object",
    )
    future.loc[0, "trade_time"] = "not-a-timestamp"

    with pytest.raises(IntradayFeatureValidationError, match="invalid current_minutes value"):
        _compute(current=pd.concat([_current_minutes(), future], ignore_index=True))


@pytest.mark.parametrize("invalid_time", [pd.NaT, None, ""])
@pytest.mark.parametrize("target", ["current", "historical"])
def test_nat_like_trade_time_values_fail_closed(
    target: str,
    invalid_time: object,
) -> None:
    bad_bar = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 30 if target == "historical" else 31, 9, 40),
                open_=10.0,
                close=10.1,
                vol=100.0,
                amount=1_000.0,
            )
        ],
        dtype="object",
    )
    bad_bar.loc[0, "trade_time"] = invalid_time

    with pytest.raises(
        IntradayFeatureValidationError,
        match=f"invalid {target}_minutes value",
    ):
        _compute(
            current=bad_bar if target == "current" else _current_minutes(),
            historical=bad_bar if target == "historical" else _historical_minutes(),
        )


@pytest.mark.parametrize("invalid_time", [pd.NaT, None, ""])
def test_timestamp_parser_explicitly_rejects_nat_like_values(invalid_time: object) -> None:
    with pytest.raises(IntradayFeatureValidationError, match="invalid probe"):
        _as_shanghai_timestamp(invalid_time, field_name="probe")


def test_rejects_current_rows_from_before_decision_trade_date() -> None:
    prior_day = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 30, 15, 0),
                open_=50.0,
                high=80.0,
                low=1.0,
                close=60.0,
                vol=10_000.0,
                amount=600_000.0,
            )
        ]
    )
    today = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                high=10.3,
                low=9.9,
                close=10.2,
                vol=100.0,
                amount=1_000.0,
            )
        ]
    )

    with pytest.raises(IntradayFeatureValidationError, match="before decision trade date"):
        _compute(
            current=pd.concat([prior_day, today], ignore_index=True),
            decision_time=datetime(2026, 7, 31, 9, 30, 2, tzinfo=SHANGHAI),
        )


def test_price_geometry_keeps_morning_prefix_across_lunch_break() -> None:
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                high=10.3,
                low=9.8,
                close=10.1,
                vol=100.0,
                amount=1_000.0,
            ),
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 11, 30),
                open_=10.1,
                high=10.8,
                low=10.0,
                close=10.7,
                vol=200.0,
                amount=2_000.0,
            ),
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 13, 0),
                open_=10.6,
                high=10.7,
                low=9.7,
                close=9.9,
                vol=300.0,
                amount=3_000.0,
            ),
        ]
    )

    row = _compute(
        current=current,
        decision_time=datetime(2026, 7, 31, 13, 0, 2, tzinfo=SHANGHAI),
    ).frame.iloc[0]

    assert row["cumulative_volume"] == pytest.approx(600.0)
    assert row["session_open"] == pytest.approx(10.0)
    assert row["session_high"] == pytest.approx(10.8)
    assert row["session_low"] == pytest.approx(9.7)
    assert row["opening_bar_open"] == pytest.approx(10.0)
    assert row["opening_bar_high"] == pytest.approx(10.3)
    assert row["opening_bar_low"] == pytest.approx(9.8)
    assert row["opening_bar_close"] == pytest.approx(10.1)


def test_missing_opening_bar_reports_candidate_scoped_unavailability_for_mixed_codes() -> None:
    decision_time = datetime(2026, 7, 31, 9, 31, 2, tzinfo=SHANGHAI)
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                high=10.2,
                low=9.9,
                close=10.1,
                vol=100.0,
                amount=1_000.0,
            ),
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 31),
                open_=10.1,
                high=10.4,
                low=10.0,
                close=10.3,
                vol=150.0,
                amount=1_500.0,
            ),
            _minute(
                "600001.SH",
                datetime(2026, 7, 31, 9, 31),
                open_=20.0,
                high=20.5,
                low=19.8,
                close=20.4,
                vol=200.0,
                amount=4_000.0,
            ),
        ]
    )
    historical = pd.concat(
        [_historical_minutes(), _historical_minutes(ts_code="600001.SH")],
        ignore_index=True,
    )

    result = _compute(
        current=current,
        historical=historical,
        decision_time=decision_time,
    )
    with_future = _compute(
        current=pd.concat(
            [
                current,
                pd.DataFrame(
                    [
                        _minute(
                            "600001.SH",
                            datetime(2026, 7, 31, 9, 32),
                            open_=99.0,
                            high=100.0,
                            low=1.0,
                            close=50.0,
                            vol=999_999.0,
                            amount=9_999_999.0,
                        )
                    ]
                ),
            ],
            ignore_index=True,
        ),
        historical=historical,
        decision_time=decision_time,
    )
    missing = result.frame.loc[lambda frame: frame["ts_code"] == "600001.SH"].iloc[0]

    assert result.payload_bytes == with_future.payload_bytes
    assert result.envelope == with_future.envelope
    expected_event_time = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    expected_available_at = datetime(2026, 7, 31, 1, 31, 2, tzinfo=UTC)

    for name in (
        "session_open",
        "opening_bar_open",
        "opening_bar_high",
        "opening_bar_low",
        "opening_bar_close",
    ):
        assert pd.isna(missing[name])
        assert result.envelope.field_status(name) is None
        missing_status = result.envelope.field_status(name, candidate_id="600001.SH")
        available_status = result.envelope.field_status(name, candidate_id="600000.SH")
        assert missing_status is not None and available_status is not None
        assert missing_status.status is FeatureAvailability.UNAVAILABLE
        assert missing_status.reason == "missing_opening_bar"
        assert available_status.status is FeatureAvailability.AVAILABLE
        assert available_status.reason is None
        for status, candidate_id in (
            (missing_status, "600001.SH"),
            (available_status, "600000.SH"),
        ):
            assert status.candidate_id == candidate_id
            assert status.source_event_time == expected_event_time
            assert status.available_at == expected_available_at
            assert status.decision_cutoff == expected_available_at
            assert status.actual_delay_seconds == pytest.approx(2.0)
    assert missing["latest_close"] == pytest.approx(20.4)
    assert missing["minute_volume"] == pytest.approx(200.0)
    assert missing["cumulative_volume"] == pytest.approx(200.0)
    assert missing["session_high"] == pytest.approx(20.5)
    assert missing["session_low"] == pytest.approx(19.8)


def test_missing_opening_bar_is_unavailable_when_all_codes_lack_0930() -> None:
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 31),
                open_=10.0,
                high=10.3,
                low=9.8,
                close=10.2,
                vol=100.0,
                amount=1_000.0,
            )
        ]
    )

    result = _compute(
        current=current,
        decision_time=datetime(2026, 7, 31, 9, 31, 2, tzinfo=SHANGHAI),
    )

    for name in (
        "session_open",
        "opening_bar_open",
        "opening_bar_high",
        "opening_bar_low",
        "opening_bar_close",
    ):
        status = result.envelope.field_status(name)
        assert status is not None
        assert status.status is FeatureAvailability.UNAVAILABLE
        assert status.reason == "missing_opening_bar"
    for name in (
        "latest_open",
        "latest_high",
        "latest_low",
        "latest_close",
        "minute_volume",
        "cumulative_volume",
        "session_high",
        "session_low",
    ):
        status = result.envelope.field_status(name)
        assert status is not None
        assert status.status is FeatureAvailability.AVAILABLE


def test_zero_minute_volume_is_available_and_keeps_geometry_finite() -> None:
    current = pd.DataFrame(
        [
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 9, 30),
                open_=10.0,
                close=10.0,
                vol=0.0,
                amount=0.0,
            )
        ]
    )

    result = _compute(
        current=current,
        decision_time=datetime(2026, 7, 31, 9, 30, 2, tzinfo=SHANGHAI),
    )
    row = result.frame.iloc[0]

    assert row["minute_volume"] == pytest.approx(0.0)
    assert row["cumulative_volume"] == pytest.approx(0.0)
    assert result.envelope.field_status("minute_volume").status is FeatureAvailability.AVAILABLE
    assert result.envelope.field_status("cumulative_volume").status is FeatureAvailability.AVAILABLE


def test_rejects_historical_revisions_that_were_unknown_at_decision_time() -> None:
    revised_later = _historical_minutes()
    revised_later.loc[revised_later.index[0], "available_at"] = datetime(
        2026, 7, 31, 9, 41, tzinfo=SHANGHAI
    )

    with pytest.raises(IntradayFeatureValidationError, match="historical.*not available"):
        _compute(historical=revised_later)

    missing_pit = _historical_minutes().drop(columns=["available_at"])
    with pytest.raises(IntradayFeatureValidationError, match="available_at"):
        _compute(historical=missing_pit)


@pytest.mark.parametrize("minute", [30, 31, 32])
def test_parameterized_opening_segment_makes_acceleration_unavailable(minute: int) -> None:
    decision = datetime(2026, 7, 31, 9, minute, 2, tzinfo=SHANGHAI)
    current = _current_minutes().loc[lambda frame: frame["trade_time"].dt.minute <= minute]
    result = _compute(current=current, decision_time=decision)

    for name in ("amount_accel_5m", "amount_accel_10m"):
        status = result.envelope.field_status(name)
        assert status is not None
        assert status.status is FeatureAvailability.UNAVAILABLE
        assert status.reason == "opening_segment"


def test_acceleration_requires_full_contiguous_window() -> None:
    decision = datetime(2026, 7, 31, 9, 36, 2, tzinfo=SHANGHAI)
    current = _current_minutes().iloc[:7]
    result = _compute(current=current, decision_time=decision)

    assert result.frame.iloc[0]["amount_accel_5m"] == pytest.approx(7_000.0 / 4_000.0)
    assert pd.isna(result.frame.iloc[0]["amount_accel_10m"])
    assert result.envelope.field_status("amount_accel_10m").reason == "insufficient_prior_minutes"

    missing_minute = current.drop(index=current.index[3])
    missing = _compute(current=missing_minute, decision_time=decision)
    assert pd.isna(missing.frame.iloc[0]["amount_accel_5m"])
    assert missing.envelope.field_status("amount_accel_5m").reason == "non_contiguous_minutes"


def test_acceleration_is_unavailable_across_lunch_session_boundary() -> None:
    rows = []
    for minute in range(26, 31):
        rows.append(
            _minute(
                "600000.SH",
                datetime(2026, 7, 31, 11, minute),
                open_=10.0,
                close=10.0,
                vol=100.0,
                amount=1_000.0,
            )
        )
    rows.append(
        _minute(
            "600000.SH",
            datetime(2026, 7, 31, 13, 0),
            open_=10.0,
            close=10.1,
            vol=100.0,
            amount=2_000.0,
        )
    )
    result = _compute(
        current=pd.DataFrame(rows),
        decision_time=datetime(2026, 7, 31, 13, 0, 2, tzinfo=SHANGHAI),
    )

    assert pd.isna(result.frame.iloc[0]["amount_accel_5m"])
    assert result.envelope.field_status("amount_accel_5m").reason == "session_break"


def test_opening_gate_configuration_is_part_of_batch_identity() -> None:
    blocked = _compute(config=_config(opening_acceleration_block_minutes=3))
    unblocked = _compute(config=_config(opening_acceleration_block_minutes=0))

    assert blocked.payload_bytes == unblocked.payload_bytes
    assert blocked.envelope.batch_id != unblocked.envelope.batch_id


def test_uses_latest_n_distinct_prior_sessions_for_historical_baselines() -> None:
    older = _historical_minutes().copy()
    older["trade_time"] = pd.to_datetime(older["trade_time"]) - pd.Timedelta(days=2)
    older["available_at"] = pd.to_datetime(older["available_at"]) - pd.Timedelta(days=2)
    older["amount"] = 100_000.0
    historical = pd.concat([older, _historical_minutes()], ignore_index=True)

    row = _compute(historical=historical, config=_config(lookback_sessions=2)).frame.iloc[0]

    assert row["hist_same_minute_amount_median"] == pytest.approx(8_250.0)
    assert row["hist_cumulative_amount_median"] == pytest.approx(49_500.0)
    assert row["historical_sessions"] == 2


def test_live_and_replay_share_one_semantic_core() -> None:
    kwargs = {
        "decision_time": datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI),
        "input_available_at": datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC),
        "input_batch_ids": ("current-0007", "history-0002"),
        "sequence": 7,
        "config": _config(),
    }

    live = live_compute(_current_minutes(), _historical_minutes(), **kwargs)
    replay = replay_compute(_current_minutes(), _historical_minutes(), **kwargs)

    assert live.mode is FeatureComputationMode.LIVE
    assert replay.mode is FeatureComputationMode.REPLAY
    assert live.payload_bytes == replay.payload_bytes
    assert live.envelope == replay.envelope
    assert_frame_equal(live.frame, replay.frame)


@pytest.mark.parametrize("minute", range(30, 41))
def test_each_opening_prefix_is_live_replay_equal_and_future_invariant(minute: int) -> None:
    current = pd.concat(
        [_current_minutes(), _current_minutes(ts_code="600001.SH")],
        ignore_index=True,
    )
    historical = pd.concat(
        [_historical_minutes(), _historical_minutes(ts_code="600001.SH")],
        ignore_index=True,
    )
    decision = datetime(2026, 7, 31, 9, minute, 2, tzinfo=SHANGHAI)
    prefix = current.loc[pd.to_datetime(current["trade_time"]).dt.minute <= minute]
    kwargs = {
        "decision_time": decision,
        "input_available_at": decision,
        "input_batch_ids": ("current-opening", "history-opening"),
        "sequence": minute - 30,
        "config": _config(),
    }

    live_prefix = live_compute(prefix, historical, **kwargs)
    replay_prefix = replay_compute(prefix, historical, **kwargs)
    live_with_future = live_compute(current, historical, **kwargs)

    assert live_prefix.payload_bytes == replay_prefix.payload_bytes
    assert live_prefix.envelope == replay_prefix.envelope
    assert live_prefix.payload_bytes == live_with_future.payload_bytes
    assert live_prefix.envelope == live_with_future.envelope


@pytest.mark.parametrize(
    "decision",
    [
        datetime(2026, 7, 31, 11, 30, 2, tzinfo=SHANGHAI),
        datetime(2026, 7, 31, 13, 0, 2, tzinfo=SHANGHAI),
    ],
)
def test_lunch_boundary_prefixes_are_live_replay_equal_and_future_invariant(
    decision: datetime,
) -> None:
    rows: list[dict[str, object]] = []
    for ts_code, base in (("600000.SH", 10.0), ("600001.SH", 20.0)):
        rows.extend(
            [
                _minute(
                    ts_code,
                    datetime(2026, 7, 31, 9, 30),
                    open_=base,
                    close=base + 0.1,
                    vol=100.0,
                    amount=1_000.0,
                ),
                _minute(
                    ts_code,
                    datetime(2026, 7, 31, 11, 30),
                    open_=base + 0.1,
                    close=base + 0.2,
                    vol=200.0,
                    amount=2_000.0,
                ),
                _minute(
                    ts_code,
                    datetime(2026, 7, 31, 13, 0),
                    open_=base + 0.2,
                    close=base + 0.3,
                    vol=300.0,
                    amount=3_000.0,
                ),
            ]
        )
    current = pd.DataFrame(rows)
    historical = pd.concat(
        [_historical_minutes(), _historical_minutes(ts_code="600001.SH")],
        ignore_index=True,
    )
    decision_utc = pd.Timestamp(decision).tz_convert(UTC)
    prefix = current.loc[
        pd.to_datetime(current["trade_time"]).dt.tz_localize(SHANGHAI).dt.tz_convert(UTC)
        <= decision_utc
    ]
    kwargs = {
        "decision_time": decision,
        "input_available_at": decision,
        "input_batch_ids": ("current-lunch", "history-lunch"),
        "sequence": 0 if decision.hour == 11 else 1,
        "config": _config(),
    }

    live_prefix = live_compute(prefix, historical, **kwargs)
    replay_prefix = replay_compute(prefix, historical, **kwargs)
    replay_with_future = replay_compute(current, historical, **kwargs)

    assert live_prefix.payload_bytes == replay_prefix.payload_bytes
    assert live_prefix.envelope == replay_prefix.envelope
    assert live_prefix.payload_bytes == replay_with_future.payload_bytes
    assert live_prefix.envelope == replay_with_future.envelope


def test_all_242_continuous_auction_minutes_are_live_replay_and_future_invariant() -> None:
    legal_minutes = list(pd.date_range("2026-07-31 09:30", "2026-07-31 11:30", freq="1min")) + list(
        pd.date_range("2026-07-31 13:00", "2026-07-31 15:00", freq="1min")
    )
    assert len(legal_minutes) == 242
    historical = _historical_minutes().iloc[0:0]
    rows: list[dict[str, object]] = []
    for sequence, minute in enumerate(legal_minutes):
        open_price = 10.0 + (sequence % 23) * 0.01
        close_price = open_price + ((sequence % 5) - 2) * 0.01
        volume = 100.0 + (sequence % 17) * 10.0
        rows.append(
            _minute(
                "600000.SH",
                minute.to_pydatetime(),
                open_=open_price,
                high=max(open_price, close_price) + 0.02,
                low=min(open_price, close_price) - 0.02,
                close=close_price,
                vol=volume,
                amount=volume * ((open_price + close_price) / 2),
            )
        )
    full_day = pd.DataFrame(rows)

    for sequence, minute in enumerate(legal_minutes):
        prefix = full_day.iloc[: sequence + 1].copy()
        decision = minute.to_pydatetime().replace(tzinfo=SHANGHAI) + timedelta(seconds=2)
        kwargs = {
            "decision_time": decision,
            "input_available_at": decision,
            "input_batch_ids": ("current-all-minutes", "history-empty"),
            "sequence": sequence,
            "config": _config(),
        }

        live = live_compute(prefix, historical, **kwargs)
        replay = replay_compute(prefix, historical, **kwargs)
        with_future = live_compute(full_day, historical, **kwargs)

        assert live.payload_bytes == replay.payload_bytes
        assert live.envelope == replay.envelope
        assert live.payload_bytes == with_future.payload_bytes
        assert live.envelope == with_future.envelope


def test_payload_order_hash_and_batch_identity_are_deterministic() -> None:
    current = pd.concat(
        [_current_minutes(ts_code="600001.SH"), _current_minutes()], ignore_index=True
    )
    history = pd.concat(
        [_historical_minutes(ts_code="600001.SH"), _historical_minutes()], ignore_index=True
    )

    left = _compute(current=current, historical=history)
    right = _compute(
        current=current.sample(frac=1.0, random_state=17),
        historical=history.sample(frac=1.0, random_state=23),
    )

    assert left.payload_bytes == right.payload_bytes
    assert left.envelope == right.envelope
    assert list(left.frame["ts_code"]) == ["600000.SH", "600001.SH"]


def test_geometry_is_deterministic_for_unordered_input() -> None:
    ordered = _compute()
    shuffled = _compute(
        current=_current_minutes().sample(frac=1.0, random_state=31),
        historical=_historical_minutes().sample(frac=1.0, random_state=29),
    )

    geometry = [
        "latest_open",
        "latest_high",
        "latest_low",
        "latest_close",
        "minute_volume",
        "cumulative_volume",
        "session_open",
        "session_high",
        "session_low",
        "opening_bar_open",
        "opening_bar_high",
        "opening_bar_low",
        "opening_bar_close",
    ]
    assert ordered.payload_bytes == shuffled.payload_bytes
    assert ordered.envelope == shuffled.envelope
    assert ordered.frame.loc[:, geometry].equals(shuffled.frame.loc[:, geometry])


def test_contract_v3_is_required_and_batch_identity_is_deterministic() -> None:
    default = _config()

    first = _compute(config=default)
    repeated = _compute(config=_config())

    assert default.contract_version == 3
    assert first.envelope.contract_version == 3
    assert first.envelope.schema_version == 2
    assert first.envelope.batch_id == repeated.envelope.batch_id
    assert set(STATUS_COLUMNS) == {status.name for status in first.envelope.field_statuses}
    with pytest.raises(ValidationError):
        IntradayFeatureConfig(
            lookback_sessions=2,
            opening_acceleration_block_minutes=3,
            contract_version=2,
            producer_commit=PRODUCER_COMMIT,
        )


def test_config_and_result_contracts_are_frozen_and_forbid_unknown_fields() -> None:
    config = _config()
    result = _compute(config=config)

    with pytest.raises(ValidationError):
        config.lookback_sessions = 99
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        IntradayFeatureConfig(producer_commit=PRODUCER_COMMIT, future_option=True)
    with pytest.raises(ValidationError):
        result.mode = FeatureComputationMode.REPLAY
