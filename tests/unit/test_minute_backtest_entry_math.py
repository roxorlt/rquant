from datetime import datetime, time

import pytest

from rquant.auction_gap_strategy import AuctionGapMinuteReplayConfig, evaluate_auction_gap_entry
from rquant.minute_replay import MinuteReplayConfig, evaluate_n_shape_entry
from rquant.growth_board_surge_strategy import GrowthBoardSurgeConfig, evaluate_growth_entry


def _n_facts() -> dict[str, object]:
    return {
        "quote_time": datetime(2025, 1, 2, 10, 30), "latest_price": 10.5, "bar_low": 10.15,
        "session_low": 10.0, "session_high": 10.6, "t_close": 10.0, "t_high": 10.2,
        "vwap": 10.3, "minute_amount": 250.0, "prior_amounts": (100.0, 100.0),
        "first_signal_time": datetime(2025, 1, 2, 9, 45), "static_factors": {},
    }


@pytest.mark.parametrize("mode", ["first_break", "break_retest", "late_confirm", "vwap_confirm", "amount_surge", "factor_confirm"])
def test_original_six_mode_gates_accept_their_observed_boundary(mode: str) -> None:
    config = MinuteReplayConfig(entry_mode=mode, factor_score_threshold=0)
    decision = evaluate_n_shape_entry(config=config, **_n_facts())
    assert decision.eligible
    assert decision.signal_time == datetime(2025, 1, 2, 9, 45)


def test_first_break_keeps_original_vwap_independence() -> None:
    facts = {**_n_facts(), "vwap": 11.0}
    assert evaluate_n_shape_entry(config=MinuteReplayConfig(entry_mode="first_break"), **facts).eligible
    assert not evaluate_n_shape_entry(config=MinuteReplayConfig(entry_mode="vwap_confirm"), **facts).eligible


def test_retest_requires_a_later_bar_and_preserves_inclusive_tolerance() -> None:
    facts = {**_n_facts(), "bar_low": 10.2 * 1.005}
    config = MinuteReplayConfig(entry_mode="break_retest")
    assert evaluate_n_shape_entry(config=config, **facts).eligible
    assert not evaluate_n_shape_entry(config=config, **{**facts, "bar_low": 10.2 * 1.005 + 0.001}).eligible
    assert not evaluate_n_shape_entry(config=config, **{**facts, "first_signal_time": facts["quote_time"]}).eligible


def test_late_confirmation_can_follow_a_previously_seen_break_without_strong_carry() -> None:
    config = MinuteReplayConfig(entry_mode="late_confirm", late_confirm_at=time(10, 30))
    facts = {**_n_facts(), "session_low": 9.9}
    assert evaluate_n_shape_entry(config=config, **facts).eligible
    assert not evaluate_n_shape_entry(config=config, **{**facts, "quote_time": datetime(2025, 1, 2, 10, 29)}).eligible


def test_amount_surge_only_uses_the_last_prior_positive_observations() -> None:
    config = MinuteReplayConfig(entry_mode="amount_surge", amount_surge_lookback=3)
    facts = {**_n_facts(), "prior_amounts": (10000.0, 0.0, 100.0, 100.0), "minute_amount": 200.0}
    assert evaluate_n_shape_entry(config=config, **facts).eligible
    assert not evaluate_n_shape_entry(config=config, **{**facts, "minute_amount": 199.99}).eligible
    assert not evaluate_n_shape_entry(config=config, **{**facts, "prior_amounts": (0.0, 100.0)}).eligible


def test_no_future_first_signal_or_execution_open_can_enter_the_pure_gate() -> None:
    with pytest.raises(ValueError, match="future"):
        evaluate_n_shape_entry(config=MinuteReplayConfig(), **{**_n_facts(), "first_signal_time": datetime(2025, 1, 2, 11, 0)})
    with pytest.raises(TypeError):
        evaluate_n_shape_entry(config=MinuteReplayConfig(), next_open=10.5, **_n_facts())


def test_auction_original_support_vwap_and_progress_boundaries() -> None:
    config = AuctionGapMinuteReplayConfig(start_date="2025-01-02", end_date="2025-01-03", min_limit_progress_pct=0.25)
    facts = {
        "quote_time": datetime(2025, 1, 2, 9, 31), "latest_price": 10.1,
        "session_low": 9.8, "session_high": 10.25, "auction_price": 10.0,
        "limit_up_price": 11.0, "vwap": 10.05,
    }
    assert evaluate_auction_gap_entry(config=config, **facts).eligible
    assert not evaluate_auction_gap_entry(config=config, **{**facts, "session_low": 9.799}).eligible
    assert not evaluate_auction_gap_entry(config=config, **{**facts, "latest_price": 10.049}).eligible
    assert not evaluate_auction_gap_entry(config=config, **{**facts, "quote_time": datetime(2025, 1, 2, 9, 30)}).eligible
    assert not evaluate_auction_gap_entry(config=config, **{**facts, "limit_up_price": 10.0}).eligible


def _growth_facts() -> dict[str, object]:
    return {
        "quote_time": datetime(2025, 1, 2, 9, 40), "latest_price": 10.5,
        "limit_up_price": 12.0, "vwap": 10.4,
        "features": {"hist_intraday_days": 20, "signal_rel_cum_amount_asof": 2.0,
                     "signal_rel_amount_same_minute": 3.0, "signal_amount_accel_5m": 3.0},
        "inner_outer_ratio": 0.5, "static_factors": {"large_net_vol_t1": 2.0},
        "board_strength": None,
    }


def test_growth_gate_keeps_original_short_circuit_and_missing_static_rejection() -> None:
    facts = _growth_facts()
    config = GrowthBoardSurgeConfig(require_large_net_vol=True)
    assert evaluate_growth_entry(config=config, **facts).eligible
    missing = evaluate_growth_entry(config=config, **{**facts, "static_factors": {}})
    assert missing.abort_candidate and not missing.eligible
    early = evaluate_growth_entry(config=config, **{**facts, "quote_time": datetime(2025, 1, 2, 9, 29), "static_factors": None})
    assert not early.eligible and not early.needs_static_factors and not early.abort_candidate
    lazy = evaluate_growth_entry(config=config, **{**facts, "static_factors": None})
    assert lazy.needs_static_factors and not lazy.eligible


def test_growth_gate_uses_original_ablation_and_weighted_score_owner() -> None:
    facts = _growth_facts()
    config = GrowthBoardSurgeConfig(enable_factor_confirm=True, factor_score_threshold=0.0)
    scored = evaluate_growth_entry(config=config, **facts)
    assert scored.eligible and scored.factor_score is not None
    assert not evaluate_growth_entry(config=config.model_copy(update={"factor_score_threshold": 1000.0}), **facts).eligible
    weak = {**facts, "features": {**facts["features"], "signal_rel_amount_same_minute": None, "signal_amount_accel_5m": None}}
    assert not evaluate_growth_entry(config=GrowthBoardSurgeConfig(), **weak).eligible
    assert evaluate_growth_entry(config=GrowthBoardSurgeConfig(use_same_minute_surge=False, use_accel_surge=False), **weak).eligible
    assert not evaluate_growth_entry(config=GrowthBoardSurgeConfig(require_inner_outer=True), **{**facts, "inner_outer_ratio": 1.0}).eligible
