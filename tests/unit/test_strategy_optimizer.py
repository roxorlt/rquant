"""策略自动优化器测试。"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_market_temperature(
    store: DuckDBStore,
    trade_date: date,
    above_ma20_ratio_pct: float,
) -> None:
    store._conn.execute(
        """
        INSERT OR REPLACE INTO market_sentiment_daily
        (trade_date, stock_count, up_count, down_count, flat_count,
         limit_up_count, first_limit_up_count, limit_down_count, yiziban_count,
         max_consecutive_limit_ups, high_board_count, up_ratio_pct,
         limit_up_ratio_pct, avg_pct_chg, median_pct_chg, total_amount,
         high_60d_ratio_pct, above_ma20_ratio_pct)
        VALUES (?, 100, 40, 50, 10, 3, 2, 1, 0, 2, 1, 40.0, 3.0,
                -0.5, -0.3, 1000000000.0, 2.0, ?)
        """,
        [trade_date, above_ma20_ratio_pct],
    )


def test_strategy_optimizer_ranks_candidate_configs(store: DuckDBStore) -> None:
    from rquant.strategy_optimizer import run_strategy_optimization

    _seed_daily_and_screen(store)
    _seed_minutes(store)

    result = run_strategy_optimization(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_modes=["first_break"],
        profile_variants=["baseline"],
        max_hold_days_options=[1],
        validation_ratio=0.0,
        min_trades=1,
    )

    assert result.rankings.shape[0] == 1
    row = result.rankings.iloc[0]
    assert row["rank"] == 1
    assert row["entry_mode"] == "first_break"
    assert row["profile_variant"] == "baseline"
    assert row["max_hold_days"] == 1
    assert row["train_trades"] == 1
    assert row["robust_score"] is not None


def test_strategy_optimizer_includes_topn_rankings(store: DuckDBStore) -> None:
    from rquant.strategy_optimizer import run_strategy_optimization

    _seed_daily_and_screen(store)
    _seed_minutes(store)

    result = run_strategy_optimization(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_modes=["first_break"],
        profile_variants=["baseline"],
        max_hold_days_options=[1],
        validation_ratio=0.0,
        min_trades=1,
        top_n_options=[1],
        score_profile_names=["v1", "no_intraday"],
    )

    assert not result.topn_rankings.empty
    assert set(result.topn_rankings["score_profile"]) == {"v1", "no_intraday"}
    row = result.topn_rankings.sort_values("score_profile").iloc[0]
    assert row["selection"] == "top1"
    assert row["candidate_id"] == "first_break|baseline|h1|no_intraday|top1"
    assert row["train_trades"] == 1
    assert row["test_trades"] == 1


def test_attach_market_temperature_maps_signal_date(store: DuckDBStore) -> None:
    from rquant.strategy_optimizer import _attach_market_temperature

    _seed_market_temperature(store, date(2026, 6, 24), 25.0)
    trades = pd.DataFrame([
        {"ts_code": "600000.SH", "signal_date": date(2026, 6, 24), "ret_pct": 1.0},
        {"ts_code": "600001.SH", "signal_date": date(2026, 6, 25), "ret_pct": 2.0},
    ])

    out = _attach_market_temperature(store, trades)

    assert out.iloc[0]["market_above_ma20_ratio_pct"] == pytest.approx(25.0)
    assert pd.isna(out.iloc[1]["market_above_ma20_ratio_pct"])
    # 原 frame 不被就地修改
    assert "market_above_ma20_ratio_pct" not in trades.columns


def test_strategy_optimizer_supports_v2_profiles_and_env_gate(
    store: DuckDBStore,
) -> None:
    from rquant.strategy_optimizer import run_strategy_optimization

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    # signal_date = 2026-06-24，温度 25 < 30 → 门控画像应把当日交易全部跳过
    _seed_market_temperature(store, date(2026, 6, 24), 25.0)

    result = run_strategy_optimization(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_modes=["first_break"],
        profile_variants=["baseline"],
        max_hold_days_options=[1],
        validation_ratio=0.0,
        min_trades=1,
        top_n_options=[1],
        score_profile_names=["v1", "v2_low_position", "v2_momentum", "v2_env_gate"],
    )

    assert set(result.topn_rankings["score_profile"]) == {
        "v1",
        "v2_low_position",
        "v2_momentum",
        "v2_env_gate",
    }
    assert "market_above_ma20_ratio_pct" in result.trades.columns

    by_profile = result.topn_rankings.set_index("score_profile")
    assert by_profile.loc["v1", "test_trades"] == 1
    assert by_profile.loc["v2_low_position", "test_trades"] == 1
    assert by_profile.loc["v2_env_gate", "test_trades"] == 0
