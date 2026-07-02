"""分钟线、特征快照、模拟盘存储测试。"""

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


def test_init_creates_intraday_tables(store: DuckDBStore) -> None:
    tables = set(store.query("SHOW TABLES")["name"].tolist())

    assert "minute_bar" in tables
    assert "auction_bar" in tables
    assert "intraday_feature_snapshot" in tables
    assert "paper_position" in tables
    assert "paper_position_event" in tables


def test_upsert_and_query_minute_bars(store: DuckDBStore) -> None:
    df = pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2023, 8, 25, 9, 31),
            "freq": "1min",
            "open": 6.99,
            "high": 7.02,
            "low": 6.97,
            "close": 7.02,
            "vol": 807500.0,
            "amount": 5649956.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2023, 8, 25, 9, 30),
            "freq": "1min",
            "open": 6.99,
            "high": 6.99,
            "low": 6.99,
            "close": 6.99,
            "vol": 103700.0,
            "amount": 724863.0,
            "source": "tushare",
        },
    ])

    assert store.upsert_minute_bars(df) == 2
    assert store.upsert_minute_bars(df) == 2

    out = store.query_minute_bars(
        "600000.SH",
        datetime(2023, 8, 25, 9, 30),
        datetime(2023, 8, 25, 9, 31),
    )
    assert len(out) == 2
    assert out["trade_time"].tolist() == [
        pd.Timestamp("2023-08-25 09:30:00"),
        pd.Timestamp("2023-08-25 09:31:00"),
    ]
    assert out.iloc[0]["source"] == "tushare"


def test_query_minute_bars_dedups_across_sources(store: DuckDBStore) -> None:
    """C4：同一分钟盘中 rt_min（tushare_rt）与日终 stk_mins（tushare）双写时，
    查询只返回权威 tushare 行，避免研究/回测把成交量翻倍计。"""
    df = pd.DataFrame([
        {  # 盘中实时快照
            "ts_code": "600000.SH",
            "trade_time": datetime(2023, 8, 25, 9, 30),
            "freq": "1min",
            "open": 6.98, "high": 7.00, "low": 6.98, "close": 7.00,
            "vol": 100000.0, "amount": 700000.0,
            "source": "tushare_rt",
        },
        {  # 同一分钟的日终权威 bar → 应胜出
            "ts_code": "600000.SH",
            "trade_time": datetime(2023, 8, 25, 9, 30),
            "freq": "1min",
            "open": 6.99, "high": 6.99, "low": 6.99, "close": 6.99,
            "vol": 103700.0, "amount": 724863.0,
            "source": "tushare",
        },
        {  # 只有实时源的分钟 → 无权威行时保留 rt 行
            "ts_code": "600000.SH",
            "trade_time": datetime(2023, 8, 25, 9, 31),
            "freq": "1min",
            "open": 6.99, "high": 7.02, "low": 6.97, "close": 7.02,
            "vol": 807500.0, "amount": 5649956.0,
            "source": "tushare_rt",
        },
    ])
    assert store.upsert_minute_bars(df) == 3

    out = store.query_minute_bars(
        "600000.SH",
        datetime(2023, 8, 25, 9, 30),
        datetime(2023, 8, 25, 9, 31),
    )
    assert len(out) == 2
    row_930 = out.iloc[0]
    assert row_930["trade_time"] == pd.Timestamp("2023-08-25 09:30:00")
    assert row_930["source"] == "tushare"
    assert row_930["close"] == 6.99
    assert row_930["vol"] == 103700.0
    assert out.iloc[1]["source"] == "tushare_rt"


def test_upsert_and_query_auction_bars(store: DuckDBStore) -> None:
    df = pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2025, 2, 18),
            "auction_type": "open_realtime",
            "price": 9.81,
            "vol": 28355900.0,
            "amount": 278113479.0,
            "turnover_rate": 0.1,
            "volume_ratio": 3.2,
            "source": "tushare",
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2025, 2, 18),
            "auction_type": "open_realtime",
            "price": 11.23,
            "vol": 1200000.0,
            "amount": 13476000.0,
            "turnover_rate": 0.03,
            "volume_ratio": 1.1,
            "source": "tushare",
        },
    ])

    assert store.upsert_auction_bars(df) == 2
    assert store.upsert_auction_bars(df) == 2

    out = store.query_auction_bars(date(2025, 2, 18))
    assert len(out) == 2
    assert out["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert out.iloc[0]["auction_type"] == "open_realtime"
    assert out.iloc[1]["amount"] == 278113479.0


def test_upsert_intraday_feature_snapshot(store: DuckDBStore) -> None:
    df = pd.DataFrame([{
        "snapshot_id": "20260825-600000.SH-baseline",
        "ts_code": "600000.SH",
        "trade_date": date(2023, 8, 25),
        "as_of_time": datetime(2023, 8, 25, 17, 0),
        "feature_set": "pool1_context_v1",
        "lookback_days": 30,
        "payload": '{"vwap_30d": 7.01, "volume_nodes": [6.99, 7.05]}',
        "source": "tushare_1min",
    }])

    assert store.upsert_intraday_feature_snapshot(df) == 1
    out = store.query(
        "SELECT snapshot_id, feature_set, lookback_days FROM intraday_feature_snapshot"
    )
    assert out.iloc[0]["snapshot_id"] == "20260825-600000.SH-baseline"
    assert out.iloc[0]["feature_set"] == "pool1_context_v1"
    assert out.iloc[0]["lookback_days"] == 30


def test_upsert_paper_position_and_events(store: DuckDBStore) -> None:
    position = pd.DataFrame([{
        "position_id": "20260826-600000.SH-attack",
        "trade_date": date(2026, 8, 26),
        "ts_code": "600000.SH",
        "name": "浦发银行",
        "pool": "pool1",
        "entry_time": datetime(2026, 8, 26, 9, 40),
        "entry_price": 7.10,
        "entry_price_raw": 7.10,
        "entry_signal": "attack_break_high",
        "candidate_id": "baseline",
        "entry_level_price": 7.08,
        "entry_t_date": date(2026, 8, 25),
        "earliest_exit_date": date(2026, 8, 27),
        "t_close": 7.00,
        "t_high": 7.08,
        "limit_up_price_next": 7.70,
        "stop_loss_price": 6.95,
        "stop_loss_basis": "t_close",
        "stop_loss_pct": 0.03,
        "take_profit_price": 7.46,
        "take_profit_pct": 0.05,
        "trailing_stop_pct": 0.025,
        "trailing_stop_price": None,
        "status": "open",
        "max_price_seen": 7.12,
        "max_drawdown_pct": None,
        "feature_snapshot_id": "20260825-600000.SH-baseline",
        "param_payload": '{"stop_loss_pct": 0.03}',
    }])
    event = pd.DataFrame([{
        "event_id": "20260826-600000.SH-attack-entry",
        "position_id": "20260826-600000.SH-attack",
        "event_time": datetime(2026, 8, 26, 9, 40),
        "event_type": "entry",
        "price": 7.10,
        "size_pct": 1.0,
        "payload": '{"signal": "attack_break_high"}',
    }])

    assert store.upsert_paper_position(position) == 1
    assert store.upsert_paper_position_event(event) == 1

    active = store.query_active_paper_positions("2026-08-26")
    assert len(active) == 1
    assert active.iloc[0]["position_id"] == "20260826-600000.SH-attack"
    assert active.iloc[0]["entry_price_raw"] == 7.10

    events = store.query_paper_position_events("20260826-600000.SH-attack")
    assert len(events) == 1
    assert events.iloc[0]["event_type"] == "entry"
