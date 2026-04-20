"""pipeline 流水线单测 —— mock screen() 和 PRESET_SCREENS。"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from rquant.pipeline import (
    _get_prev_trading_date,
    _resolve_execution_order,
    _to_screen_result_df,
    run_daily_pipeline,
)
from rquant.presets import ScreenPreset
from rquant.screen.rules import not_st
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestToScreenResultDf:
    def test_converts_screen_output(self) -> None:
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
            "CIRC_MV[0]": [80000.0],
        })
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert len(result) == 1
        assert result.iloc[0]["trade_date"] == "2026-04-18"
        assert result.iloc[0]["preset_name"] == "pool1"
        assert result.iloc[0]["ts_code"] == "000001.SZ"
        assert result.iloc[0]["close"] == 10.5
        assert result.iloc[0]["pct_chg"] == 5.0
        assert "CIRC_MV" in result.iloc[0]["extra"]

    def test_empty_input(self) -> None:
        df = pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert result.empty

    def test_no_extra_columns(self) -> None:
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
        })
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert result.iloc[0]["extra"] is None


class TestResolveExecutionOrder:
    def test_no_dep_first(self) -> None:
        presets = {
            "child": ScreenPreset(
                name="child", description="", rules=[],
                depends_on="parent", offset_days=1,
            ),
            "parent": ScreenPreset(
                name="parent", description="", rules=[],
            ),
        }
        order = _resolve_execution_order(presets)
        assert order.index("parent") < order.index("child")

    def test_filter_by_names(self) -> None:
        presets = {
            "a": ScreenPreset(name="a", description="", rules=[]),
            "b": ScreenPreset(name="b", description="", rules=[]),
        }
        order = _resolve_execution_order(presets, names=["a"])
        assert order == ["a"]


class TestGetPrevTradingDate:
    def test_returns_previous_date(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        assert _get_prev_trading_date(store, "2026-04-18", 1) == "2026-04-17"

    def test_returns_none_when_no_data(self, store: DuckDBStore) -> None:
        assert _get_prev_trading_date(store, "2026-04-18", 1) is None

    def test_offset_2(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-04-16', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        assert _get_prev_trading_date(store, "2026-04-18", 2) == "2026-04-16"


class TestRunDailyPipeline:
    def test_skips_non_trading_day(self, store: DuckDBStore) -> None:
        result = run_daily_pipeline("2026-04-20", store=store)
        assert result == {}

    def test_runs_preset_and_persists(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
        })
        test_presets = {
            "test-pool": ScreenPreset(
                name="test-pool", description="test", rules=[not_st()],
            ),
        }
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline("2026-04-18", store=store)
        assert result == {"test-pool": 1}
        sr = store.query_screen_result("2026-04-18", "test-pool")
        assert len(sr) == 1
        assert sr.iloc[0]["ts_code"] == "000001.SZ"

    def test_specific_preset_only(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
        })
        test_presets = {
            "a": ScreenPreset(name="a", description="", rules=[not_st()]),
            "b": ScreenPreset(name="b", description="", rules=[not_st()]),
        }
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline(
                "2026-04-18", preset_names=["a"], store=store
            )
        assert "a" in result
        assert "b" not in result

    def test_child_uses_parent_whitelist(self, store: DuckDBStore) -> None:
        # T-1 and T data
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('000001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0),"
            "('300001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        # Parent results on T-1
        parent_sr = pd.DataFrame({
            "trade_date": ["2026-04-17"],
            "preset_name": ["parent"],
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "close": [10.0],
            "pct_chg": [5.0],
            "extra": [None],
        })
        store.upsert_screen_result(parent_sr)

        child_preset = ScreenPreset(
            name="child", description="", rules=[not_st()],
            depends_on="parent", offset_days=1,
        )
        empty_df = pd.DataFrame(
            columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"child": child_preset}),
            patch("rquant.pipeline.screen", return_value=empty_df) as mock_scr,
        ):
            run_daily_pipeline("2026-04-18", store=store)
            kw = mock_scr.call_args.kwargs
            assert kw["ts_code_whitelist"] == ["000001.SZ"]

    def test_child_skips_when_parent_empty(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('000001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        child_preset = ScreenPreset(
            name="child", description="", rules=[not_st()],
            depends_on="parent", offset_days=1,
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"child": child_preset}),
            patch("rquant.pipeline.screen") as mock_scr,
        ):
            result = run_daily_pipeline("2026-04-18", store=store)
            mock_scr.assert_not_called()
            assert result == {"child": 0}
