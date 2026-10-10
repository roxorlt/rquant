from datetime import date, datetime, time

import pandas as pd
import pytest

from rquant.auction_gap_strategy import (
    AuctionGapConfig, AuctionGapMinuteReplayConfig, auction_candidate_mask,
    auction_morning_vwap_break, resolve_auction_hold_policy,
)
from rquant.growth_board_surge_strategy import (
    GrowthBoardSurgeConfig, passes_growth_board_filter, passes_growth_fresh_filter,
    passes_growth_listing_filter, prior_days_had_surge,
)
from rquant.paper import PaperPosition


def test_auction_original_mask_and_causal_mask_have_explicit_next_open_difference() -> None:
    frame = pd.DataFrame({"entry_price": [10.3, 10.3, 10.3, 11.0, 10.3],
        "pre_close": [10.0]*5, "pre_high": [10.4, 10.2, 10.2, 10.2, 10.2],
        "auction_vol_ratio_5d": [0.15, 0.15, 5.0, 1.0, 1.0],
        "limit_up_price": [11.0]*5, "is_st": [False, False, False, False, True],
        "name": ["样本", "样本", "样本", "样本", "ST样本"],
        "next_open": [None, 10.5, None, 11.0, 10.5]})
    assert auction_candidate_mask(frame, AuctionGapConfig(start_date="2025-01-02", end_date="2025-01-02")).tolist() == [False, True, False, False, False]
    causal = AuctionGapConfig(start_date="2025-01-02", end_date="2025-01-02", require_next_day=False)
    assert auction_candidate_mask(frame.drop(columns="next_open"), causal).tolist() == [True, True, True, False, False]
    assert auction_candidate_mask(frame, causal.model_copy(update={"gap_mode": "strict_high"})).tolist() == [False, True, True, False, False]
    assert auction_candidate_mask(frame, causal.model_copy(update={"st_filter": "literal_lower"})).iloc[-1]


@pytest.mark.parametrize("official,expected", [
    (None, ("seal_hold", 3)),
    (pd.Series({"open_times": 1, "fd_amount": 20.0, "float_mv": 100.0}), ("t1", 1)),
    (pd.Series({"open_times": 0, "fd_amount": 0.5, "float_mv": 100.0}), ("t1", 1)),
    (pd.Series({"open_times": 0, "fd_amount": 1.0, "float_mv": 100.0}), ("seal_hold", 3)),
    (pd.Series({"open_times": None, "fd_amount": None, "float_mv": 100.0}), ("t1", 1)),
])
def test_original_seal_hold_missing_official_and_exact_ratio_boundary(official: pd.Series | None, expected: tuple[str, int]) -> None:
    config = AuctionGapMinuteReplayConfig(start_date="2025-01-02", end_date="2025-01-02", seal_hold_enabled=True, seal_hold_max_days=3,
        seal_hold_max_open_times=0, seal_hold_min_fd_to_circ_pct=1.0)
    assert resolve_auction_hold_policy({"b_close_at_limit_up": True, "b_open_times": 0},
        config=config, official=official) == expected
    assert resolve_auction_hold_policy({"b_close_at_limit_up": False}, config=config, official=official) == ("t1", 1)


def test_original_morning_vwap_gate_keeps_t_plus_one_and_strict_price_boundary() -> None:
    position = PaperPosition(position_id="synthetic-position", entry_signal="attack",
        stop_loss_basis="percent", stop_loss_pct=0.05, take_profit_pct=0.1,
        ts_code="600001.SH", name="样本", pool="pool1",
        trade_date=date(2025, 1, 2), entry_time=datetime(2025, 1, 2, 10), entry_price=10.0,
        earliest_exit_date=date(2025, 1, 3), stop_loss_price=9.5, take_profit_price=11.0,
        max_price_seen=10.0, trailing_stop_pct=0.03)
    config = AuctionGapMinuteReplayConfig(start_date="2025-01-02", end_date="2025-01-02",
        next_morning_exit_until=time(10), next_morning_vwap_break_buffer_pct=0.01)
    assert not auction_morning_vwap_break(position, quote_time=datetime(2025, 1, 2, 9, 45),
        price=9.8, day_vwap=10.0, config=config)
    assert not auction_morning_vwap_break(position, quote_time=datetime(2025, 1, 3, 10),
        price=9.9, day_vwap=10.0, config=config)
    assert auction_morning_vwap_break(position, quote_time=datetime(2025, 1, 3, 10),
        price=9.89, day_vwap=10.0, config=config)
    assert not auction_morning_vwap_break(position, quote_time=datetime(2025, 1, 3, 10, 1),
        price=9.89, day_vwap=10.0, config=config)


def test_original_growth_nonmissing_history_and_static_gate_boundaries() -> None:
    assert prior_days_had_surge((1.0,), lookback_days=2, max_prior_volume_ratio=2.0) is None
    assert prior_days_had_surge((1.0, 2.0), lookback_days=2, max_prior_volume_ratio=2.0) is True
    assert prior_days_had_surge((1.0, 1.99), lookback_days=2, max_prior_volume_ratio=2.0) is False
    config = GrowthBoardSurgeConfig(min_listing_trading_days=60, require_fresh_surge=True,
        require_board_favor=True, min_board_gap_up_ratio=0.6, min_board_auction_amount_ratio=2.0)
    assert not passes_growth_listing_filter(59, config)
    assert passes_growth_listing_filter(60, config)
    assert passes_growth_fresh_filter(None, config)
    assert not passes_growth_fresh_filter(True, config)
    assert not passes_growth_board_filter(None, config)
    assert not passes_growth_board_filter({"board_gap_up_ratio": 0.6, "board_auction_amount_ratio": None}, config)
    assert passes_growth_board_filter({"board_gap_up_ratio": 0.6, "board_auction_amount_ratio": 2.0}, config)
