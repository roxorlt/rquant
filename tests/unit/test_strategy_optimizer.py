"""策略自动优化器测试。"""

from __future__ import annotations

import pytest

from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


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
