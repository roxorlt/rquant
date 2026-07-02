"""风控参数搜索测试。"""

from __future__ import annotations

import pandas as pd


def test_build_risk_grid_names_candidate_profiles() -> None:
    from rquant.risk_search import build_risk_grid

    grid = build_risk_grid(
        stop_loss_pcts=[0.03],
        take_profit_pcts=[0.05, 0.08],
        trailing_stop_pcts=[0.02],
    )

    assert [config.candidate_id for config in grid] == [
        "sl3_tp5_tr2",
        "sl3_tp8_tr2",
    ]


def test_run_risk_grid_search_ranks_cached_replays() -> None:
    from rquant.risk_search import build_risk_grid, run_risk_grid_search

    def replay_loader(config):
        if config.take_profit_pct == 0.08:
            return pd.DataFrame([
                {"ts_code": "000001.SZ", "ret_pct": 6.0, "exit_reason": "take_profit"},
                {"ts_code": "000002.SZ", "ret_pct": -2.0, "exit_reason": "stop_loss"},
            ])
        return pd.DataFrame([
            {"ts_code": "000001.SZ", "ret_pct": 3.0, "exit_reason": "take_profit"},
            {"ts_code": "000002.SZ", "ret_pct": -2.0, "exit_reason": "stop_loss"},
        ])

    result = run_risk_grid_search(
        build_risk_grid(
            stop_loss_pcts=[0.03],
            take_profit_pcts=[0.05, 0.08],
            trailing_stop_pcts=[0.02],
        ),
        replay_loader=replay_loader,
        min_trades=1,
    )

    assert result.iloc[0]["paper_candidate_id"] == "sl3_tp8_tr2"
    assert result.iloc[0]["mean_ret_pct"] == 2.0
    assert result.iloc[0]["win_rate_pct"] == 50.0


def test_run_risk_grid_search_from_entry_cache_loads_snapshots_once() -> None:
    from datetime import date, datetime

    from rquant.replay_entry_cache import (
        ReplayEntrySnapshot,
        ReplayWatchItem,
        RiskReplayQuote,
    )
    from rquant.risk_search import (
        build_risk_grid,
        run_risk_grid_search_from_entry_cache,
    )

    calls = 0

    def snapshot_loader():
        nonlocal calls
        calls += 1
        return [
            ReplayEntrySnapshot(
                item=ReplayWatchItem(
                    ts_code="600000.SH",
                    pool="pool1",
                    name="浦发银行",
                    entry_date=date(2026, 6, 24),
                    reference_date=date(2026, 6, 24),
                    limit_up_date=date(2026, 6, 24),
                    t_close=10.0,
                    t_high=10.2,
                    limit_up_price_next=None,
                    stop_weak=0.0,
                ),
                execution_quote=RiskReplayQuote(
                    ts_code="600000.SH",
                    trade_time=datetime(2026, 6, 25, 9, 33),
                    price=10.0,
                    low=10.0,
                    high=10.0,
                ),
                signal={
                    "level": "minute_vwap_confirm",
                    "trigger_type": "minute_vwap_confirm",
                    "level_price": 10.2,
                    "signal_price": 10.24,
                },
                entry_time=datetime(2026, 6, 25, 9, 33),
                earliest_exit_date=date(2026, 6, 26),
                window_dates=(date(2026, 6, 25), date(2026, 6, 26)),
                exit_quotes=(
                    RiskReplayQuote(
                        ts_code="600000.SH",
                        trade_time=datetime(2026, 6, 25, 9, 33),
                        price=10.4,
                        low=10.0,
                        high=10.6,
                    ),
                    RiskReplayQuote(
                        ts_code="600000.SH",
                        trade_time=datetime(2026, 6, 26, 9, 31),
                        price=10.4,
                        low=10.35,
                        high=10.42,
                    ),
                ),
            )
        ]

    result = run_risk_grid_search_from_entry_cache(
        build_risk_grid(
            stop_loss_pcts=[0.03],
            take_profit_pcts=[0.05, 0.08],
            trailing_stop_pcts=[0.02],
        ),
        snapshot_loader=snapshot_loader,
        min_trades=1,
    )

    assert calls == 1
    assert result.iloc[0]["paper_candidate_id"] == "sl3_tp8_tr2"
    assert result.iloc[0]["mean_ret_pct"] == 4.0
