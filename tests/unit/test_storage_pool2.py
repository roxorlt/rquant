"""pool2_watch + monitor_event 存储层测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestPool2Watch:
    def test_upsert_and_query_active(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20,
            "body_lower": 11.80,
            "level_40": 12.36,
            "level_30": 12.22,
            "level_20": 12.08,
            "stop_strong": 11.80,
            "stop_weak": 11.52,
            "status": "active",
        }])
        count = store.upsert_pool2_watch(df)
        assert count == 1

        active = store.query_pool2_active()
        assert len(active) == 1
        assert active.iloc[0]["ts_code"] == "002415.SZ"
        assert active.iloc[0]["level_40"] == 12.36

    def test_exited_not_in_active(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.update_pool2_exit("002415.SZ", date(2026, 4, 21), "expired")

        active = store.query_pool2_active()
        assert len(active) == 0

    def test_update_exit(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.update_pool2_exit("002415.SZ", date(2026, 4, 21), "breakdown")

        all_rows = store.query("SELECT * FROM pool2_watch")
        assert all_rows.iloc[0]["status"] == "exited"
        assert all_rows.iloc[0]["exit_reason"] == "breakdown"

    def test_remove(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.remove_pool2("002415.SZ")
        assert len(store.query("SELECT * FROM pool2_watch")) == 0

    def test_reentry_after_exit(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.update_pool2_exit("002415.SZ", date(2026, 4, 21), "expired")

        # Re-enter with new data
        df2 = df.copy()
        df2["entry_date"] = date(2026, 4, 22)
        df2["status"] = "active"
        store.upsert_pool2_watch(df2)

        active = store.query_pool2_active()
        assert len(active) == 1
        assert active.iloc[0]["status"] == "active"


class TestMonitorEvent:
    def test_upsert_and_query(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "trade_date": date(2026, 4, 21),
            "ts_code": "002415.SZ",
            "level": "30",
            "trigger_price": 12.35,
            "level_price": 12.22,
            "trigger_time": datetime(2026, 4, 21, 10, 23, 15),
            "trigger_type": "realtime",
            "pool": "pool2",
            "body_upper": 13.20,
            "body_lower": 11.80,
        }])
        count = store.upsert_monitor_event(df)
        assert count == 1

        events = store.query_monitor_events("2026-04-21")
        assert len(events) == 1
        assert events.iloc[0]["level"] == "30"

    def test_dedup_same_level(self, store: DuckDBStore) -> None:
        row = {
            "trade_date": date(2026, 4, 21),
            "ts_code": "002415.SZ",
            "level": "30",
            "trigger_price": 12.35,
            "level_price": 12.22,
            "trigger_time": datetime(2026, 4, 21, 10, 23, 15),
            "trigger_type": "realtime",
            "pool": "pool2",
            "body_upper": 13.20,
            "body_lower": 11.80,
        }
        store.upsert_monitor_event(pd.DataFrame([row]))
        row["trigger_price"] = 12.30
        store.upsert_monitor_event(pd.DataFrame([row]))

        events = store.query_monitor_events("2026-04-21")
        assert len(events) == 1

    def test_query_events_for_stock(self, store: DuckDBStore) -> None:
        rows = [
            {
                "trade_date": date(2026, 4, 21), "ts_code": "002415.SZ",
                "level": "40", "trigger_price": 12.50, "level_price": 12.36,
                "trigger_time": datetime(2026, 4, 21, 10, 0),
                "trigger_type": "realtime", "pool": "pool2",
                "body_upper": 13.20, "body_lower": 11.80,
            },
            {
                "trade_date": date(2026, 4, 21), "ts_code": "002415.SZ",
                "level": "30", "trigger_price": 12.20, "level_price": 12.22,
                "trigger_time": datetime(2026, 4, 21, 10, 30),
                "trigger_type": "realtime", "pool": "pool2",
                "body_upper": 13.20, "body_lower": 11.80,
            },
        ]
        store.upsert_monitor_event(pd.DataFrame(rows))
        events = store.query_monitor_events("2026-04-21", ts_code="002415.SZ")
        assert len(events) == 2
