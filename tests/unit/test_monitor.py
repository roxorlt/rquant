"""monitor 模块单测。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestBuildWatchlist:
    def test_loads_pool2_active(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        items = build_watchlist(store)
        assert len(items) == 1
        assert items[0].ts_code == "002415.SZ"
        assert items[0].pool == "pool2"
        assert items[0].level_40 == 12.36

    def test_loads_pool1_with_level_calc(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        # Pool 1 screen_result from yesterday
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool1",
            "ts_code": "300001.SZ",
            "name": "特锐德", "close": 15.0, "pct_chg": 3.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        # daily_bar (so _get_latest_screen_date can find it)
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('300001.SZ', '2026-04-21', 14,16,13,15,14,1,3,500,5000)"
        )

        # daily_state: limit-up was 04-20
        store._conn.execute(
            """
            INSERT INTO daily_state VALUES
            ('300001.SZ', '2026-04-20', false, false, 'gem', 0.20,
             18.0, 12.0, true, false, true, false, 1, 16.50, 14.80)
            """
        )

        items = build_watchlist(store, screen_date="2026-04-21")
        assert len(items) == 1
        assert items[0].pool == "pool1"
        body = 16.50 - 14.80
        assert items[0].level_40 == pytest.approx(14.80 + body * 0.4)

    def test_dedup_pool2_wins(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        # Same stock in both Pool 1 and Pool 2
        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool1",
            "ts_code": "002415.SZ",
            "name": "海康威视", "close": 12.50, "pct_chg": -2.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)"
        )

        items = build_watchlist(store, screen_date="2026-04-21")
        assert len(items) == 1
        assert items[0].pool == "pool2"
