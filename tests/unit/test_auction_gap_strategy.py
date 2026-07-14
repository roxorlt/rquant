"""集合竞价跳空策略回测测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime

import pandas as pd
import pytest

from rquant.security_status import SHANGHAI, SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _status_row(
    ts_code: str,
    trade_date: date,
    *,
    name: str | None,
    is_st: bool | None,
    available_at: datetime | None = None,
    conflict_reason: str | None = None,
) -> SecurityStatusDaily:
    return SecurityStatusDaily(
        ts_code=ts_code,
        trade_date=trade_date,
        name=name,
        is_st=is_st,
        name_source="test_name" if conflict_reason is None else "conflict",
        st_source="test_st" if is_st is not None else None,
        available_at=available_at,
        ingested_at=datetime(2026, 7, 1, tzinfo=UTC),
        conflict_reason=conflict_reason,
    )


def _seed_auction_gap_case(store: DuckDBStore) -> None:
    daily_rows = []
    dates = [
        date(2026, 6, 18),
        date(2026, 6, 19),
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
        date(2026, 6, 26),
    ]
    for i, trade_date in enumerate(dates):
        daily_rows.append({
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "open": 10.0 + i * 0.1,
            "high": 10.5 + i * 0.1,
            "low": 9.8 + i * 0.1,
            "close": 10.0 + i * 0.1,
            "pre_close": 9.9 + i * 0.1,
            "change": 0.1,
            "pct_chg": 1.0,
            "vol": 1000.0,
            "amount": 10000.0,
        })
        daily_rows.append({
            "ts_code": "000001.SZ",
            "trade_date": trade_date,
            "open": 20.0,
            "high": 20.5,
            "low": 19.8,
            "close": 20.0,
            "pre_close": 19.9,
            "change": 0.1,
            "pct_chg": 0.5,
            "vol": 1000.0,
            "amount": 20000.0,
        })
    store.upsert_daily(pd.DataFrame(daily_rows))
    store.upsert_stock_basic(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "symbol": "600000",
            "name": "*ST当前浦发",
            "area": "上海",
            "industry": "银行",
            "list_date": "19991110",
            "market": "主板",
        },
        {
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "name": "当前平安",
            "area": "深圳",
            "industry": "测试",
            "list_date": "19910403",
            "market": "主板",
        },
    ]))
    store.upsert_stock_status(tuple(
        _status_row(
            ts_code,
            trade_date,
            name=name,
            is_st=is_st,
            available_at=datetime(
                trade_date.year,
                trade_date.month,
                trade_date.day,
                9,
                25,
                tzinfo=SHANGHAI,
            ),
        )
        for trade_date in dates
        for ts_code, name, is_st in (
            ("600000.SH", "历史浦发", False),
            ("000001.SZ", "*ST历史样本", True),
        )
    ))
    store.upsert_state(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.55,
            "limit_down_price": 9.45,
            "is_limit_up": True,
            "is_limit_down": False,
            "is_first_limit_up": True,
            "is_yiziban": False,
            "consecutive_limit_ups": 1,
            "body_upper": 10.5,
            "body_lower": 10.5,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.66,
            "limit_down_price": 9.54,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 10.6,
            "body_lower": 10.6,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2026, 6, 25),
            "is_st": True,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.05,
            "limit_up_price": 21.00,
            "limit_down_price": 19.00,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 20.0,
            "body_lower": 20.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2026, 6, 26),
            "is_st": True,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.05,
            "limit_up_price": 21.00,
            "limit_down_price": 19.00,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 20.0,
            "body_lower": 20.0,
        },
    ]))
    store.upsert_auction_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "auction_type": "open_realtime",
            "price": 10.7,
            "vol": 18000.0,
            "amount": 192600.0,
            "turnover_rate": 0.1,
            "volume_ratio": 1.0,
            "source": "tushare",
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2026, 6, 25),
            "auction_type": "open_realtime",
            "price": 20.5,
            "vol": 18000.0,
            "amount": 369000.0,
            "turnover_rate": 0.1,
            "volume_ratio": 1.0,
            "source": "tushare",
        },
    ]))


def test_auction_gap_replay_uses_share_volume_and_next_open_exit(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    row = trades.iloc[0]
    assert row["auction_vol_ratio_5d"] == pytest.approx(0.18)
    assert bool(row["hit_limit_up_today"])
    assert row["next_open_ret_pct"] == pytest.approx((10.6 / 10.7 - 1) * 100)
    assert row["name"] == "历史浦发"


def test_auction_gap_replay_literal_lower_only_filters_lowercase_historical_name(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="literal_lower",
        ),
    )

    assert trades["ts_code"].tolist() == ["000001.SZ", "600000.SH"]

    signal_date = date(2026, 6, 25)
    store._conn.execute(
        "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
        ["000001.SZ", signal_date],
    )
    store.upsert_stock_status((
        _status_row(
            "000001.SZ",
            signal_date,
            name="*st历史样本",
            is_st=True,
            available_at=datetime(2026, 6, 25, 9, 25, tzinfo=SHANGHAI),
        ),
    ))

    lowercase_filtered = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="literal_lower",
        ),
    )

    assert lowercase_filtered["ts_code"].tolist() == ["600000.SH"]


def test_auction_gap_replay_none_st_filter_allows_known_true_and_false(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="none",
        ),
    )

    assert trades["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert trades.set_index("ts_code").loc["000001.SZ", "name"] == "*ST历史样本"


def test_auction_gap_replay_none_rejects_exact_nullable_unknown_status(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    signal_date = date(2026, 6, 25)
    store._conn.execute(
        "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
        ["600000.SH", signal_date],
    )
    store.upsert_stock_status((
        _status_row(
            "600000.SH",
            signal_date,
            name=None,
            is_st=None,
        ),
    ))

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="none",
        ),
    )

    assert trades["ts_code"].tolist() == ["000001.SZ"]


@pytest.mark.parametrize("status_case", ["missing", "adjacent_only", "conflict", "future"])
def test_auction_gap_replay_rejects_unknown_point_in_time_status(
    store: DuckDBStore,
    status_case: str,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store._conn.execute(
        "UPDATE stock_basic SET name = '当前普通名' WHERE ts_code = '600000.SH'"
    )
    signal_date = date(2026, 6, 25)
    if status_case == "missing":
        store._conn.execute(
            "DELETE FROM stock_status_daily WHERE ts_code = ?",
            ["600000.SH"],
        )
    else:
        store._conn.execute(
            "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", signal_date],
        )
        if status_case == "conflict":
            store.upsert_stock_status((
                _status_row(
                    "600000.SH",
                    signal_date,
                    name=None,
                    is_st=None,
                    conflict_reason="test_conflict",
                ),
            ))
        elif status_case == "future":
            store.upsert_stock_status((
                _status_row(
                    "600000.SH",
                    signal_date,
                    name="历史浦发",
                    is_st=False,
                    available_at=datetime(2026, 6, 25, 9, 25, 1, tzinfo=SHANGHAI),
                ),
            ))

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    assert trades.empty


@pytest.mark.parametrize("empty_case", ["no_auction", "all_status_unknown"])
def test_auction_gap_replay_empty_result_keeps_minimum_columns(
    store: DuckDBStore,
    empty_case: str,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    if empty_case == "all_status_unknown":
        _seed_auction_gap_case(store)
        store._conn.execute("DELETE FROM stock_status_daily")

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="none",
        ),
    )

    assert trades.empty
    assert {"signal_date", "ts_code", "name"}.issubset(trades.columns)


def test_auction_gap_replay_uses_pit_status_when_daily_state_disagrees(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store._conn.execute(
        """
        UPDATE daily_state
        SET is_st = TRUE,
            limit_pct = 0.05,
            limit_up_price = 10.92,
            is_limit_up = TRUE,
            is_yiziban = TRUE
        WHERE ts_code = '600000.SH'
          AND trade_date = DATE '2026-06-25'
        """
    )

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    row = trades.iloc[0]
    assert row["name"] == "历史浦发"
    assert not bool(row["is_st"])
    assert row["limit_pct"] == pytest.approx(0.10)
    assert row["limit_up_price"] == pytest.approx(11.44)
    assert not bool(row["hit_limit_up_today"])
    assert pd.isna(row["hit_yiziban_today"])


def test_auction_gap_replay_calculates_limit_price_from_final_state_pct(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store._conn.execute(
        """
        UPDATE daily_state
        SET limit_pct = 0.07,
            limit_up_price = NULL
        WHERE ts_code = '600000.SH'
          AND trade_date = DATE '2026-06-25'
        """
    )

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    row = trades.set_index("ts_code").loc["600000.SH"]
    assert row["limit_pct"] == pytest.approx(0.07)
    assert row["limit_up_price"] == pytest.approx(11.13)


def test_auction_gap_replay_uses_historical_gem_limit_pct_before_2020_reform(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    dates = [
        date(2020, 8, 14),
        date(2020, 8, 17),
        date(2020, 8, 18),
        date(2020, 8, 19),
        date(2020, 8, 20),
        date(2020, 8, 21),
        date(2020, 8, 24),
    ]
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "300001.SZ",
            "trade_date": trade_date,
            "open": 10.0,
            "high": 10.8,
            "low": 9.8,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1000.0,
            "amount": 10000.0,
        }
        for trade_date in dates
    ]))
    signal_date = date(2020, 8, 21)
    store.upsert_stock_status((
        _status_row(
            "300001.SZ",
            signal_date,
            name="历史创业板",
            is_st=False,
            available_at=datetime(2020, 8, 21, 9, 25, tzinfo=SHANGHAI),
        ),
    ))
    store.upsert_auction_bars(pd.DataFrame([{
        "ts_code": "300001.SZ",
        "trade_date": signal_date,
        "auction_type": "open_realtime",
        "price": 10.5,
        "vol": 18000.0,
        "amount": 189000.0,
        "turnover_rate": 0.1,
        "volume_ratio": 1.0,
        "source": "tushare",
    }]))

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2020-08-21",
            end_date="2020-08-21",
            st_filter="case_insensitive",
        ),
    )

    row = trades.iloc[0]
    assert row["limit_pct"] == pytest.approx(0.10)
    assert row["limit_up_price"] == pytest.approx(11.00)


def test_auction_gap_replay_prefers_tushare_auction_over_minute_fallback(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store.upsert_auction_bars(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 25),
        "auction_type": "open_realtime",
        "price": 10.8,
        "vol": 20000.0,
        "amount": 216000.0,
        "turnover_rate": None,
        "volume_ratio": None,
        "source": "minute_0930_fallback",
    }]))

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    assert trades.iloc[0]["entry_price"] == pytest.approx(10.7)


def test_auction_gap_replay_strict_high_gap_mode_requires_gap_above_prior_high(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            gap_mode="strict_high",
            st_filter="case_insensitive",
        ),
    )

    assert trades.empty


def test_auction_gap_replay_can_generate_live_candidate_without_signal_day_daily(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store._conn.execute(
        "DELETE FROM daily_bar WHERE trade_date = DATE '2026-06-25'"
    )
    store._conn.execute(
        "DELETE FROM daily_state WHERE trade_date = DATE '2026-06-25'"
    )

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
            require_next_day=False,
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    row = trades.iloc[0]
    assert row["prev_trade_date"] == pd.Timestamp("2026-06-24")
    assert row["pre_close"] == pytest.approx(10.4)
    assert row["auction_vol_ratio_5d"] == pytest.approx(0.18)
    assert row["limit_up_price"] == pytest.approx(11.44)
    assert pd.isna(row["day_high"])
    assert pd.isna(row["next_open"])
