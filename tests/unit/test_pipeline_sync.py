"""pipeline pool2_watch 同步测试。"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from rquant.pipeline import (
    _compute_levels,
    _sync_pool2_watch,
)
from rquant.presets import ScreenPreset
from rquant.screen.rules import not_st
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestComputeLevels:
    def test_basic_computation(self) -> None:
        levels = _compute_levels(body_upper=13.20, body_lower=11.80)
        body = 13.20 - 11.80  # 1.40
        assert levels["level_40"] == pytest.approx(11.80 + body * 0.4)
        assert levels["level_30"] == pytest.approx(11.80 + body * 0.3)
        assert levels["level_20"] == pytest.approx(11.80 + body * 0.2)
        assert levels["stop_strong"] == pytest.approx(11.80)
        assert levels["stop_weak"] == pytest.approx(11.80 - body * 0.2)

    def test_yiziban_zero_body(self) -> None:
        """一字板 body=0 时所有档位 = body_lower。"""
        levels = _compute_levels(body_upper=10.0, body_lower=10.0)
        assert levels["level_40"] == 10.0
        assert levels["stop_weak"] == 10.0


class TestSyncPool2Watch:
    def test_new_stock_added(self, store: DuckDBStore) -> None:
        # Pool 2 screen_result
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool2",
            "ts_code": "002415.SZ",
            "name": "海康威视",
            "close": 12.50,
            "pct_chg": -2.0,
            "extra": None,
        }])
        store.upsert_screen_result(sr)

        # daily_state: limit-up day's body
        store._conn.execute(
            """
            INSERT INTO daily_state VALUES
            ('002415.SZ', '2026-04-18', false, false, 'main', 0.10,
             13.20, 10.80, true, false, true, false, 1, 13.20, 11.80),
            ('002415.SZ', '2026-04-19', false, false, 'main', 0.10,
             13.20, 10.80, false, false, false, false, 0, 12.50, 12.00)
            """
        )

        # daily_bar for prev trading date lookup
        store._conn.execute(
            """
            INSERT INTO daily_bar VALUES
            ('002415.SZ', '2026-04-18', 12,13,11,12,11,1,5,1000,10000),
            ('002415.SZ', '2026-04-19', 12,13,11,12,11,1,5,1000,10000),
            ('002415.SZ', '2026-04-20', 12,13,11,12,11,1,5,1000,10000),
            ('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)
            """
        )

        _sync_pool2_watch(store, "2026-04-21")

        active = store.query_pool2_active()
        assert len(active) == 1
        assert active.iloc[0]["ts_code"] == "002415.SZ"
        assert active.iloc[0]["body_upper"] == 13.20
        assert active.iloc[0]["body_lower"] == 11.80

    def test_existing_active_not_duplicated(self, store: DuckDBStore) -> None:
        # Already in pool2_watch
        existing = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 20),
            "limit_up_date": date(2026, 4, 18),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(existing)

        # Same stock in today's Pool 2
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool2",
            "ts_code": "002415.SZ",
            "name": "海康威视",
            "close": 12.50, "pct_chg": -2.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        _sync_pool2_watch(store, "2026-04-21")

        active = store.query_pool2_active()
        assert len(active) == 1
        # entry_date stays original (DuckDB returns date as datetime, compare via .date())
        entry = active.iloc[0]["entry_date"]
        if hasattr(entry, "date"):
            entry = entry.date()
        assert entry == date(2026, 4, 20)
