"""分钟策略版本对比测试。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _memory_store(*, bound: bool, operational: bool = False) -> SimpleNamespace:
    connection = duckdb.connect(":memory:")
    if bound:
        connection.execute(
            """
            CREATE TABLE strategy_eligibility (
                eligibility_id VARCHAR PRIMARY KEY,
                strategy_id VARCHAR NOT NULL,
                ts_code VARCHAR NOT NULL,
                eligibility_date DATE NOT NULL,
                entry_date DATE NOT NULL,
                variant VARCHAR NOT NULL
            )
            """
        )
    if operational:
        connection.execute(
            """
            CREATE TABLE screen_result (
                trade_date DATE NOT NULL,
                preset_name VARCHAR NOT NULL,
                ts_code VARCHAR NOT NULL
            )
            """
        )
    return SimpleNamespace(_conn=connection)


def _insert_bound_rows(store: SimpleNamespace) -> None:
    store._conn.execute(
        """
        INSERT INTO strategy_eligibility VALUES
            ('e1', 'n_shape', '000001.SZ', DATE '2026-07-14', DATE '2026-07-15', 'pool1'),
            ('e2', 'n_shape', '000001.SZ', DATE '2026-07-14', DATE '2026-07-15', 'pool2'),
            ('e3', 'n_shape', '000002.SZ', DATE '2026-07-14', DATE '2026-07-15', 'pool1'),
            ('e4', 'n_shape', '000001.SZ', DATE '2026-07-15', DATE '2026-07-16', 'pool2')
        """
    )


def _candidate_count(store: SimpleNamespace, preset_name: str) -> int:
    from rquant.strategy_compare import _candidate_count as count

    return count(store, date(2026, 7, 14), date(2026, 7, 15), preset_name)


def test_bound_empty_eligibility_without_screen_result_counts_zero() -> None:
    store = _memory_store(bound=True)
    try:
        assert _candidate_count(store, "n-shape-combined") == 0
    finally:
        store._conn.close()


def test_bound_empty_eligibility_ignores_conflicting_operational_rows() -> None:
    store = _memory_store(bound=True, operational=True)
    try:
        store._conn.execute(
            "INSERT INTO screen_result VALUES (DATE '2026-07-14', 'n-shape-pool1', 'attacker')"
        )

        assert _candidate_count(store, "n-shape-pool1") == 0
        assert _candidate_count(store, "n-shape-combined") == 0
    finally:
        store._conn.close()


def test_bound_eligibility_counts_pool_variants_and_deduplicates_combined() -> None:
    store = _memory_store(bound=True)
    try:
        _insert_bound_rows(store)

        assert _candidate_count(store, "n-shape-pool1") == 2
        assert _candidate_count(store, "n-shape-pool2") == 2
        assert _candidate_count(store, "n-shape-combined") == 3
    finally:
        store._conn.close()


def test_bound_eligibility_counts_ignore_conflicting_operational_rows() -> None:
    store = _memory_store(bound=True, operational=True)
    try:
        _insert_bound_rows(store)
        store._conn.execute(
            """
            INSERT INTO screen_result VALUES
                (DATE '2026-07-14', 'n-shape-pool1', 'attacker-1'),
                (DATE '2026-07-14', 'n-shape-pool2', 'attacker-2'),
                (DATE '2026-07-15', 'n-shape-pool2', 'attacker-3')
            """
        )

        assert _candidate_count(store, "n-shape-pool1") == 2
        assert _candidate_count(store, "n-shape-pool2") == 2
        assert _candidate_count(store, "n-shape-combined") == 3
    finally:
        store._conn.close()


def test_legacy_candidate_count_without_bound_table_is_unchanged() -> None:
    store = _memory_store(bound=False, operational=True)
    try:
        store._conn.execute(
            """
            INSERT INTO screen_result VALUES
                (DATE '2026-07-14', 'n-shape-pool1', '000001.SZ'),
                (DATE '2026-07-14', 'n-shape-pool2', '000001.SZ'),
                (DATE '2026-07-15', 'n-shape-pool2', '000002.SZ')
            """
        )

        assert _candidate_count(store, "n-shape-pool1") == 1
        assert _candidate_count(store, "n-shape-pool2") == 2
        assert _candidate_count(store, "n-shape-combined") == 2
    finally:
        store._conn.close()


def test_comparison_with_empty_bound_eligibility_uses_zero_denominator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_compare as strategy_compare

    store = _memory_store(bound=True)
    monkeypatch.setattr(
        strategy_compare,
        "run_minute_strong_carry_replay",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    try:
        result = strategy_compare.run_entry_mode_comparison(
            store,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 15),
            entry_modes=["first_break"],
            profile_variants=["baseline"],
            preset_name="n-shape-combined",
        )
    finally:
        store._conn.close()

    assert result.candidates_count == 0
    assert result.summary.to_dict(orient="records") == [
        {
            "entry_mode": "first_break",
            "profile_variant": "baseline",
            "candidates": 0,
            "trades": 0,
            "trigger_rate_pct": 0.0,
            "mean_ret_pct": None,
            "median_ret_pct": None,
            "win_rate_pct": None,
            "best_ret_pct": None,
            "worst_ret_pct": None,
            "gap_stop_rate_pct": None,
        }
    ]


def test_run_entry_mode_comparison_returns_summary_and_trades(
    store: DuckDBStore,
) -> None:
    from rquant.strategy_compare import run_entry_mode_comparison

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_minute_bars(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_time": datetime(2026, 6, 25, 10, 30),
                    "freq": "1min",
                    "open": 10.30,
                    "high": 10.40,
                    "low": 10.25,
                    "close": 10.35,
                    "vol": 10000,
                    "amount": 103500,
                    "source": "tushare",
                },
                {
                    "ts_code": "600000.SH",
                    "trade_time": datetime(2026, 6, 25, 10, 31),
                    "freq": "1min",
                    "open": 10.36,
                    "high": 10.42,
                    "low": 10.35,
                    "close": 10.40,
                    "vol": 10000,
                    "amount": 104000,
                    "source": "tushare",
                },
            ]
        )
    )

    result = run_entry_mode_comparison(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_modes=["first_break", "late_confirm"],
        max_hold_days=1,
    )

    assert set(result.summary["entry_mode"]) == {"first_break", "late_confirm"}
    assert len(result.trades) == 2
    assert result.candidates_count == 1
    assert result.summary.set_index("entry_mode").loc["first_break", "trades"] == 1
    assert result.summary.set_index("entry_mode").loc["late_confirm", "trades"] == 1
    assert set(result.trades["entry_mode"]) == {"first_break", "late_confirm"}


def test_run_entry_mode_comparison_supports_combined_pool(
    store: DuckDBStore,
) -> None:
    from rquant.strategy_compare import run_entry_mode_comparison

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_screen_result(
        pd.DataFrame(
            [
                {
                    "trade_date": datetime(2026, 6, 24).date(),
                    "preset_name": "n-shape-pool2",
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "close": 10.00,
                    "pct_chg": 2.04,
                    "extra": None,
                }
            ]
        )
    )

    result = run_entry_mode_comparison(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_modes=["first_break"],
        preset_name="n-shape-combined",
        max_hold_days=1,
    )

    assert result.candidates_count == 1
    assert len(result.trades) == 1
    assert result.trades.iloc[0]["pool"] == "pool2"


def test_run_entry_mode_comparison_can_compare_profile_variants(
    store: DuckDBStore,
) -> None:
    from rquant.strategy_compare import run_entry_mode_comparison

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_daily(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": datetime(2026, 6, day).date(),
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.0,
                    "pre_close": 10.0,
                    "change": 0.0,
                    "pct_chg": 0.0,
                    "vol": 1,
                    "amount": 1,
                }
                for day in [21, 22, 23]
            ]
        )
    )
    store.upsert_minute_bars(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_time": datetime(2026, 6, day, 9, 30),
                    "freq": "1min",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "vol": 10000,
                    "amount": 100000,
                    "source": "tushare",
                }
                for day in [21, 22, 23]
            ]
        )
    )
    store.upsert_adj_factor(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": datetime(2026, 6, day).date(),
                    "adj_factor": 1.0,
                }
                for day in [21, 22, 23, 24]
            ]
        )
    )

    result = run_entry_mode_comparison(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_modes=["first_break"],
        profile_variants=["baseline", "vp_risk_only", "vp_90"],
        max_hold_days=1,
    )

    assert set(result.summary["profile_variant"]) == {
        "baseline", "vp_risk_only", "vp_90"
    }
    assert set(result.trades["profile_variant"]) == {
        "baseline", "vp_risk_only", "vp_90"
    }


def test_comparison_rejects_two_nonzero_slippage_owners_before_replay() -> None:
    from rquant.paper import PaperTradeConfig
    from rquant.research_run_spec import ExecutionCostSpec
    from rquant.strategy_compare import run_entry_mode_comparison

    with pytest.raises(ValueError, match="slippage.*owner"):
        run_entry_mode_comparison(
            object(),  # type: ignore[arg-type]
            start_date="2026-06-24",
            end_date="2026-06-24",
            entry_modes=["first_break"],
            paper_config=PaperTradeConfig(entry_slippage_pct=0.001),
            execution_costs=ExecutionCostSpec(
                commission_bps=Decimal("0"),
                stamp_duty_bps=Decimal("0"),
                transfer_fee_bps=Decimal("0"),
                slippage_bps=Decimal("10"),
            ),
        )
