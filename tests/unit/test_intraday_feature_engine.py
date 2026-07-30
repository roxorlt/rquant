from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from rquant.feature_contracts import FeatureAvailability
from rquant.intraday_feature_engine import (
    FeatureComputationMode,
    IntradayFeatureConfig,
    IntradayFeatureValidationError,
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
    available_at: datetime | None = None,
) -> dict[str, object]:
    local_bar_end = trade_time.replace(tzinfo=trade_time.tzinfo or SHANGHAI)
    return {
        "ts_code": ts_code,
        "trade_time": trade_time,
        "available_at": available_at or local_bar_end + timedelta(seconds=2),
        "open": open_,
        "high": max(open_, close),
        "low": min(open_, close),
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


def test_rejects_unclosed_current_bar_and_rows_not_yet_available() -> None:
    future_bar = pd.concat(
        [
            _current_minutes(),
            pd.DataFrame(
                [
                    _minute(
                        "600000.SH",
                        datetime(2026, 7, 31, 9, 41),
                        open_=14.0,
                        close=15.0,
                        vol=100.0,
                        amount=12_000.0,
                    )
                ]
            ),
        ],
        ignore_index=True,
    )
    with pytest.raises(IntradayFeatureValidationError, match="unclosed bar"):
        _compute(current=future_bar)

    unavailable = _current_minutes()
    unavailable.loc[unavailable.index[-1], "available_at"] = datetime(
        2026, 7, 31, 9, 40, 3, tzinfo=SHANGHAI
    )
    with pytest.raises(IntradayFeatureValidationError, match="not available"):
        _compute(current=unavailable)

    with pytest.raises(IntradayFeatureValidationError, match="input_available_at"):
        _compute(input_available_at=datetime(2026, 7, 31, 9, 40, 3, tzinfo=SHANGHAI))


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


def test_config_and_result_contracts_are_frozen_and_forbid_unknown_fields() -> None:
    config = _config()
    result = _compute(config=config)

    with pytest.raises(ValidationError):
        config.lookback_sessions = 99
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        IntradayFeatureConfig(producer_commit=PRODUCER_COMMIT, future_option=True)
    with pytest.raises(ValidationError):
        result.mode = FeatureComputationMode.REPLAY
