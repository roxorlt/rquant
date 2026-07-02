"""模拟盘入场、止损与退出逻辑测试。"""

from __future__ import annotations

from datetime import date, datetime

import pytest


def _item(
    *,
    t_close: float = 9.85,
    t_high: float = 10.20,
    stop_weak: float = 9.75,
):
    from rquant.monitor import WatchItem

    return WatchItem(
        ts_code="002415.SZ",
        pool="pool2",
        limit_up_date=date(2026, 6, 24),
        body_upper=10.30,
        body_lower=9.60,
        body=0.70,
        level_40=9.88,
        level_30=9.81,
        level_20=9.74,
        stop_strong=9.60,
        stop_weak=stop_weak,
        name="海康威视",
        entry_date=date(2026, 6, 24),
        reference_date=date(2026, 6, 24),
        t_high=t_high,
        t_close=t_close,
        limit_up_price_next=10.84,
    )


def _quote(price: float, low: float, high: float | None = None):
    from rquant.monitor import RealtimeQuote

    return RealtimeQuote(
        ts_code="002415.SZ",
        price=price,
        low=low,
        open=9.90,
        high=high or price,
        pre_close=9.80,
        pct_chg=2.04,
        source="test",
    )


def test_open_position_from_attack_signal_freezes_entry_fields() -> None:
    from rquant.paper import PaperTradeConfig, open_position_from_signal

    now = datetime(2026, 6, 25, 9, 41)
    signal = {
        "level": "attack_break_high",
        "trigger_price": 10.25,
        "level_price": 10.20,
        "trigger_type": "break_t_high",
    }

    position = open_position_from_signal(
        _item(),
        _quote(price=10.22, low=9.86, high=10.25),
        signal,
        now,
        PaperTradeConfig(candidate_id="baseline-smoke"),
    )

    assert position.position_id == "20260625-002415.SZ-attack_break_high"
    assert position.ts_code == "002415.SZ"
    assert position.name == "海康威视"
    assert position.pool == "pool2"
    assert position.entry_time == now
    assert position.entry_price == 10.22
    assert position.entry_price_raw == 10.22
    assert position.entry_signal == "attack_break_high"
    assert position.candidate_id == "baseline-smoke"
    assert position.entry_level_price == 10.20
    assert position.entry_t_date == date(2026, 6, 24)
    assert position.t_close == 9.85
    assert position.t_high == 10.20
    assert position.limit_up_price_next == 10.84
    assert position.take_profit_price == 10.73
    assert position.trailing_stop_pct == 0.025
    assert position.trailing_stop_price is None
    assert position.status == "open"


def test_stop_loss_prefers_tighter_structural_floor() -> None:
    from rquant.paper import PaperTradeConfig, calculate_initial_stop_loss

    stop, basis = calculate_initial_stop_loss(
        _item(t_close=9.85, stop_weak=9.75),
        _quote(price=10.00, low=9.80),
        10.00,
        PaperTradeConfig(stop_loss_pct=0.03),
    )

    assert stop == 9.85
    assert basis == "t_close"


def test_stop_loss_falls_back_to_percent_floor_when_structure_is_too_far() -> None:
    from rquant.paper import PaperTradeConfig, calculate_initial_stop_loss

    stop, basis = calculate_initial_stop_loss(
        _item(t_close=9.10, stop_weak=8.80),
        _quote(price=10.00, low=9.00),
        10.00,
        PaperTradeConfig(stop_loss_pct=0.03),
    )

    assert stop == 9.70
    assert basis == "pct_3"


def test_stop_loss_is_capped_below_entry_price() -> None:
    from rquant.paper import PaperTradeConfig, calculate_initial_stop_loss

    stop, basis = calculate_initial_stop_loss(
        _item(t_close=9.99, stop_weak=9.80),
        _quote(price=10.00, low=9.99),
        10.00,
        PaperTradeConfig(stop_loss_pct=0.03),
    )

    assert stop == 9.95
    assert basis == "entry_buffer_cap"


def test_profile_risk_plan_overrides_intraday_low_stop() -> None:
    from rquant.paper import PaperRiskPlan, PaperTradeConfig, calculate_initial_stop_loss

    stop, basis = calculate_initial_stop_loss(
        _item(t_close=10.00),
        _quote(price=10.24, low=10.15),
        10.24,
        PaperTradeConfig(stop_loss_pct=0.03),
        PaperRiskPlan(
            stop_loss_price=9.97,
            stop_loss_basis="vp30_poc",
        ),
    )

    assert stop == 10.00
    assert basis == "t_close"


def test_entry_day_stop_loss_is_blocked_by_a_share_t1_rule() -> None:
    from rquant.paper import PaperTradeConfig, check_position_exit, open_position_from_signal

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.85),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(),
    )

    exit_ = check_position_exit(
        opened,
        _quote(price=9.80, low=9.78, high=9.83),
        datetime(2026, 6, 25, 13, 4),
        PaperTradeConfig(),
    )

    assert opened.earliest_exit_date == date(2026, 6, 26)
    assert exit_ is None


def test_position_exits_at_stop_loss_when_quote_low_crosses_stop() -> None:
    from rquant.paper import PaperTradeConfig, check_position_exit, open_position_from_signal

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.85),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(),
    )

    exit_ = check_position_exit(
        opened,
        _quote(price=9.90, low=9.84, high=9.96),
        datetime(2026, 6, 26, 9, 36),
        PaperTradeConfig(),
    )

    assert exit_ is not None
    assert exit_.position_id == opened.position_id
    assert exit_.exit_price == opened.stop_loss_price
    assert exit_.exit_reason == "stop_loss"
    assert exit_.pnl_pct == pytest.approx(-1.5)


def test_position_exits_at_quote_price_when_gap_below_stop() -> None:
    from rquant.paper import PaperTradeConfig, check_position_exit, open_position_from_signal

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.86),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(),
    )

    exit_ = check_position_exit(
        opened,
        _quote(price=9.80, low=9.78, high=9.83),
        datetime(2026, 6, 26, 9, 31),
        PaperTradeConfig(),
    )

    assert exit_ is not None
    assert exit_.exit_price == 9.80
    assert exit_.exit_reason == "gap_stop"
    assert exit_.pnl_pct == pytest.approx(-2.0)


def test_take_profit_activation_updates_trailing_stop_without_immediate_exit() -> None:
    from rquant.paper import (
        PaperTradeConfig,
        check_position_exit,
        mark_position_to_quote,
        open_position_from_signal,
    )

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.85),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )

    marked = mark_position_to_quote(
        opened,
        _quote(price=10.52, low=10.45, high=10.55),
    )
    exit_ = check_position_exit(
        marked,
        _quote(price=10.52, low=10.45, high=10.55),
        datetime(2026, 6, 25, 10, 10),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )

    assert marked.max_price_seen == 10.55
    assert marked.trailing_stop_price == 10.28
    assert exit_ is None


def test_trailing_take_profit_exits_on_next_day_pullback() -> None:
    from rquant.paper import (
        PaperTradeConfig,
        check_position_exit,
        mark_position_to_quote,
        open_position_from_signal,
    )

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.85),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )
    marked = mark_position_to_quote(opened, _quote(price=10.57, low=10.50, high=10.60))

    exit_ = check_position_exit(
        marked,
        _quote(price=10.35, low=10.32, high=10.36),
        datetime(2026, 6, 26, 10, 40),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )

    assert exit_ is not None
    assert marked.trailing_stop_price == 10.33
    assert exit_.exit_price == 10.33
    assert exit_.exit_reason == "take_profit_trailing"
    assert exit_.pnl_pct == pytest.approx(3.3)


def test_trailing_take_profit_gap_exits_at_current_quote_price() -> None:
    from rquant.paper import (
        PaperTradeConfig,
        check_position_exit,
        mark_position_to_quote,
        open_position_from_signal,
    )

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.85),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )
    marked = mark_position_to_quote(opened, _quote(price=10.57, low=10.50, high=10.60))

    exit_ = check_position_exit(
        marked,
        _quote(price=10.20, low=10.18, high=10.22),
        datetime(2026, 6, 26, 9, 31),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )

    assert exit_ is not None
    assert exit_.exit_price == 10.20
    assert exit_.exit_reason == "take_profit_gap"
    assert exit_.pnl_pct == pytest.approx(2.0)


def test_gap_below_stop_keeps_stop_reason_even_when_trailing_exists() -> None:
    from rquant.paper import (
        PaperTradeConfig,
        check_position_exit,
        mark_position_to_quote,
        open_position_from_signal,
    )

    opened = open_position_from_signal(
        _item(t_close=9.85),
        _quote(price=10.00, low=9.85),
        {"level": "attack_strong_carry", "level_price": 9.85},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )
    marked = mark_position_to_quote(opened, _quote(price=10.57, low=10.50, high=10.60))

    exit_ = check_position_exit(
        marked,
        _quote(price=9.60, low=9.50, high=9.70),
        datetime(2026, 6, 26, 9, 31),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )

    assert exit_ is not None
    assert exit_.exit_price == 9.60
    assert exit_.exit_reason == "gap_stop"
    assert exit_.pnl_pct == pytest.approx(-4.0)


def test_adjust_open_position_price_basis_scales_cost_and_risk_lines() -> None:
    from rquant.paper import (
        PaperTradeConfig,
        adjust_open_position_price_basis,
        mark_position_to_quote,
        open_position_from_signal,
    )

    opened = open_position_from_signal(
        _item(t_close=150.80, t_high=153.60),
        _quote(price=138.00, low=138.00, high=153.60),
        {"level": "attack_strong_carry", "level_price": 150.80},
        datetime(2026, 6, 12, 9, 30),
        PaperTradeConfig(take_profit_pct=0.05, trailing_stop_pct=0.025),
    )
    marked = mark_position_to_quote(opened, _quote(price=150.80, low=138.00, high=153.60))

    adjusted = adjust_open_position_price_basis(
        marked,
        previous_close=150.80,
        current_pre_close=115.70,
    )

    ratio = 115.70 / 150.80
    assert adjusted.entry_price_raw == 138.00
    assert adjusted.entry_price == pytest.approx(138.00 * ratio)
    assert adjusted.stop_loss_price == pytest.approx(marked.stop_loss_price * ratio)
    assert adjusted.take_profit_price == pytest.approx(marked.take_profit_price * ratio)
    assert adjusted.trailing_stop_price == pytest.approx(
        marked.trailing_stop_price * ratio
    )
    assert adjusted.t_close == pytest.approx(115.70)
    assert adjusted.t_high == pytest.approx(153.60 * ratio)


def test_same_stock_cannot_open_twice_on_same_trading_day() -> None:
    from rquant.paper import PaperTradeConfig, can_open_position, open_position_from_signal

    opened = open_position_from_signal(
        _item(),
        _quote(price=10.00, low=9.86),
        {"level": "attack_break_high", "level_price": 10.20},
        datetime(2026, 6, 25, 9, 35),
        PaperTradeConfig(),
    )

    assert not can_open_position([opened], "002415.SZ", date(2026, 6, 25))
    assert can_open_position([opened], "002415.SZ", date(2026, 6, 26))
    assert can_open_position([opened], "300001.SZ", date(2026, 6, 25))
