"""T 日板块集合竞价强度因子测试（无未来函数）。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import pandas as pd
import pytest

from rquant.board_auction_strength import board_auction_strength
from rquant.storage.duckdb import DuckDBStore

_SIGNAL = date(2026, 2, 3)
_PREV = date(2026, 1, 30)   # T-1（daily_bar 昨收）
_HIST = [date(2026, 1, 29), date(2026, 1, 30)]  # 历史竞价额窗口
_MEMBER_DATE = date(2026, 2, 2)  # ≤ 信号日的最近打点日


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "board.duckdb")
    yield s
    s.close()


def _member(board: str, con: str, trade_date: date) -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "board_code": board,
        "board_name": f"题材{board}",
        "con_code": con,
        "con_name": f"股{con[:6]}",
        "hot_num": 100,
    }


def _daily(ts_code: str, trade_date: date, close: float) -> dict[str, object]:
    return {"ts_code": ts_code, "trade_date": trade_date, "close": close}


def _auction(
    ts_code: str, trade_date: date, price: float, amount: float
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "auction_type": "open_realtime",
        "price": price,
        "vol": amount / price,
        "amount": amount,
        "turnover_rate": 1.0,
        "volume_ratio": 1.0,
        "source": "tushare",
    }


def _seed_two_boards(store: DuckDBStore) -> None:
    """题材 A（3 成分）与题材 B（2 成分），候选 X 同时属于两者。

    A：竞价资金 6000 vs 历史中位 2500 → ratio 2.4；高开 2/3；
    B：竞价资金 4000 vs 历史中位 4000 → ratio 1.0；高开 1/2。
    取最强 = A（ratio 2.4）。
    """
    x, a1, a2, b1 = "300001.SZ", "300010.SZ", "300011.SZ", "300020.SZ"
    members = [
        _member("000100.KP", x, _MEMBER_DATE),
        _member("000100.KP", a1, _MEMBER_DATE),
        _member("000100.KP", a2, _MEMBER_DATE),
        _member("000200.KP", x, _MEMBER_DATE),
        _member("000200.KP", b1, _MEMBER_DATE),
        # 更早一天的旧打点（成分不同）——不应被采用（用最近打点日）
        _member("000100.KP", x, date(2026, 2, 1)),
        _member("000100.KP", "300099.SZ", date(2026, 2, 1)),
    ]
    store.upsert_dataset("kpl_concept_member_daily", pd.DataFrame(members))

    # T-1 昨收：全部 10.0
    store.upsert_daily(pd.DataFrame([
        {**_daily(ts, _PREV, 10.0), "open": 10.0, "high": 10.0, "low": 10.0,
         "pre_close": 10.0, "change": 0.0, "pct_chg": 0.0, "vol": 1.0,
         "amount": 1.0}
        for ts in (x, a1, a2, b1)
    ]))

    auctions: list[dict[str, object]] = []
    # 信号日竞价（价 vs 昨收 10）：A 高开 2/3（a2 低开），B 高开 1/2（b1 低开）
    auctions += [
        _auction(x, _SIGNAL, 11.0, 3000.0),
        _auction(a1, _SIGNAL, 10.5, 2000.0),
        _auction(a2, _SIGNAL, 9.5, 1000.0),   # 低开
        _auction(b1, _SIGNAL, 9.0, 1000.0),   # 低开
    ]
    # 历史 A 竞价额：1/30 合计 3000、1/29 合计 2000 → 中位 2500
    auctions += [
        _auction(x, _HIST[1], 10.0, 1000.0),
        _auction(a1, _HIST[1], 10.0, 1000.0),
        _auction(a2, _HIST[1], 10.0, 1000.0),
        _auction(x, _HIST[0], 10.0, 1000.0),
        _auction(a1, _HIST[0], 10.0, 500.0),
        _auction(a2, _HIST[0], 10.0, 500.0),
    ]
    # 历史 B 竞价额：两日各 4000 → 中位 4000（signal B=4000 → ratio 1.0）
    auctions += [
        _auction(b1, _HIST[1], 10.0, 3000.0),
        _auction(b1, _HIST[0], 10.0, 3000.0),
    ]
    store.upsert_auction_bars(pd.DataFrame(auctions))


def test_board_auction_strength_picks_strongest_board(store: DuckDBStore) -> None:
    _seed_two_boards(store)

    result = board_auction_strength(store, "300001.SZ", _SIGNAL)

    assert result is not None
    # 取 board_auction_amount_ratio 最大的题材 A
    assert result["board_code"] == "000100.KP"
    assert result["board_member_count"] == 3
    assert result["board_gap_up_ratio"] == pytest.approx(0.6667, abs=1e-4)
    assert result["board_auction_amount_ratio"] == pytest.approx(2.4)


def test_board_auction_strength_uses_latest_membership_stamp(
    store: DuckDBStore,
) -> None:
    """成分按 ≤T 最近打点日还原：2/2 的 3 成分，而非 2/1 旧打点。"""
    _seed_two_boards(store)
    result = board_auction_strength(store, "300001.SZ", _SIGNAL)
    assert result is not None
    # 2/2 的题材 A 成分数 3（旧打点 2/1 的 300099.SZ 不计入）
    assert result["board_member_count"] == 3


def test_board_auction_strength_no_membership_returns_none(
    store: DuckDBStore,
) -> None:
    _seed_two_boards(store)
    # 窗口内无打点的票 → None（因子缺失，不判定）
    assert board_auction_strength(store, "301999.SZ", _SIGNAL) is None


def test_board_auction_strength_membership_out_of_window_returns_none(
    store: DuckDBStore,
) -> None:
    _seed_two_boards(store)
    # lookback 太短，最近打点 2/2 落在窗口外 → None
    assert board_auction_strength(
        store, "300001.SZ", _SIGNAL, membership_lookback_days=0
    ) is None


def test_board_auction_strength_small_board_member_count(
    store: DuckDBStore,
) -> None:
    """成分太少的题材：member_count 如实返回，供调用方过滤。"""
    store.upsert_dataset("kpl_concept_member_daily", pd.DataFrame([
        _member("000300.KP", "300055.SZ", _MEMBER_DATE),
    ]))
    store.upsert_daily(pd.DataFrame([{
        **_daily("300055.SZ", _PREV, 10.0), "open": 10.0, "high": 10.0,
        "low": 10.0, "pre_close": 10.0, "change": 0.0, "pct_chg": 0.0,
        "vol": 1.0, "amount": 1.0,
    }]))
    store.upsert_auction_bars(pd.DataFrame([
        _auction("300055.SZ", _SIGNAL, 11.0, 2000.0),
        _auction("300055.SZ", _HIST[1], 10.0, 1000.0),
    ]))

    result = board_auction_strength(store, "300055.SZ", _SIGNAL)
    assert result is not None
    assert result["board_member_count"] == 1
    assert result["board_gap_up_ratio"] == pytest.approx(1.0)
    # 单日历史 → 中位 1000，signal 2000 → ratio 2.0
    assert result["board_auction_amount_ratio"] == pytest.approx(2.0)


def test_board_auction_strength_missing_auction_history_ratio_none(
    store: DuckDBStore,
) -> None:
    """无历史竞价额 → amount_ratio 为 None（缺数据不判定）。"""
    store.upsert_dataset("kpl_concept_member_daily", pd.DataFrame([
        _member("000400.KP", "300066.SZ", _MEMBER_DATE),
    ]))
    store.upsert_daily(pd.DataFrame([{
        **_daily("300066.SZ", _PREV, 10.0), "open": 10.0, "high": 10.0,
        "low": 10.0, "pre_close": 10.0, "change": 0.0, "pct_chg": 0.0,
        "vol": 1.0, "amount": 1.0,
    }]))
    # 只有信号日竞价，无历史
    store.upsert_auction_bars(pd.DataFrame([
        _auction("300066.SZ", _SIGNAL, 11.0, 2000.0),
    ]))
    result = board_auction_strength(store, "300066.SZ", _SIGNAL)
    assert result is not None
    assert result["board_auction_amount_ratio"] is None
    assert result["board_gap_up_ratio"] == pytest.approx(1.0)


def test_board_auction_strength_hist_days_truncates_window(
    store: DuckDBStore,
) -> None:
    """hist_days 只取最近 N 个历史竞价日：短窗口抓当下、长窗口摊平。

    历史三日竞价额 5000/3000/1000（由近及远），信号日 4000：
    - hist_days=1 → 中位 5000 → ratio 0.8（近端强，相对显弱）
    - hist_days=3 → 中位 3000 → ratio 1.333（长窗口摊平）
    """
    ts = "300077.SZ"
    store.upsert_dataset("kpl_concept_member_daily", pd.DataFrame([
        _member("000500.KP", ts, _MEMBER_DATE),
    ]))
    store.upsert_daily(pd.DataFrame([{
        **_daily(ts, _PREV, 10.0), "open": 10.0, "high": 10.0, "low": 10.0,
        "pre_close": 10.0, "change": 0.0, "pct_chg": 0.0, "vol": 1.0,
        "amount": 1.0,
    }]))
    store.upsert_auction_bars(pd.DataFrame([
        _auction(ts, _SIGNAL, 11.0, 4000.0),
        _auction(ts, date(2026, 2, 2), 10.0, 5000.0),   # 最近
        _auction(ts, date(2026, 1, 30), 10.0, 3000.0),
        _auction(ts, date(2026, 1, 29), 10.0, 1000.0),  # 最远
    ]))

    near = board_auction_strength(store, ts, _SIGNAL, hist_days=1)
    far = board_auction_strength(store, ts, _SIGNAL, hist_days=3)
    assert near is not None and far is not None
    assert near["board_auction_amount_ratio"] == pytest.approx(0.8)
    assert far["board_auction_amount_ratio"] == pytest.approx(4000 / 3000, abs=1e-4)
