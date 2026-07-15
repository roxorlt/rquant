"""T 日板块集合竞价强度因子（无未来函数）。

候选票所在题材（按 ≤T 最近一次打点还原成分）在信号日集合竞价的整体强度，
用作 growth_board_surge 的候选级闸门与评分观察因子。全程只用 ≤signal_date
数据：题材成分取信号日前最近一次打点（kpl_concept_member_daily）、昨收取
daily_bar 的 T-1 收盘、竞价价/额按信号时刻执行 PIT 可见性过滤。

一票多题材（kpl 一只股票可挂多个题材）时取「板块竞价资金相对历史」最强的题材。
"""

from __future__ import annotations

import statistics
from datetime import date, datetime, time, timedelta

import pandas as pd
from loguru import logger

from rquant.data_contracts import EXCHANGE_TIMEZONE
from rquant.pit_visibility import (
    VisibilityQueryScope,
    query_visible_rows,
)
from rquant.storage.duckdb import DuckDBStore

_AUCTION_TYPE = "open_realtime"


def _visible_auction_rows(
    store: DuckDBStore,
    members: list[str],
    signal_date: date,
    decision_at: datetime,
) -> pd.DataFrame:
    return query_visible_rows(
        store,
        "auction_bar",
        decision_at,
        scope=VisibilityQueryScope(
            ts_codes=tuple(members),
            end_date=signal_date,
            columns=(
                "ts_code",
                "trade_date",
                "auction_type",
                "price",
                "amount",
                "source",
            ),
        ),
    )


def _latest_membership(
    store: DuckDBStore,
    ts_code: str,
    signal_date: date,
    membership_lookback_days: int,
    decision_at: datetime,
) -> tuple[date, list[tuple[str, str]]] | None:
    """[signal_date-lookback, signal_date] 内该票最近一次打点日及其所属题材。

    返回 (打点日, [(board_code, board_name), ...])；窗口内无打点 → None。
    """
    lower = signal_date - timedelta(days=membership_lookback_days)
    visible = query_visible_rows(
        store,
        "kpl_concept_daily",
        decision_at,
        scope=VisibilityQueryScope(
            start_date=lower,
            end_date=signal_date,
            columns=("board_code", "board_name", "con_code", "trade_date"),
        ),
    )
    visible = visible.loc[visible["con_code"] == ts_code]
    if visible.empty:
        return None
    visible["trade_date"] = pd.to_datetime(visible["trade_date"]).dt.date
    latest = max(visible["trade_date"])
    boards = [
        (str(row.board_code), str(row.board_name))
        for row in visible.loc[visible["trade_date"] == latest].itertuples()
    ]
    return latest, boards


def _board_members(
    store: DuckDBStore,
    board_code: str,
    membership_date: date,
    decision_at: datetime,
) -> list[str]:
    visible = query_visible_rows(
        store,
        "kpl_concept_daily",
        decision_at,
        scope=VisibilityQueryScope(
            start_date=membership_date,
            end_date=membership_date,
            columns=("board_code", "con_code", "trade_date"),
        ),
    )
    return visible.loc[
        visible["board_code"] == board_code,
        "con_code",
    ].astype(str).tolist()


def _board_gap_up_ratio(
    store: DuckDBStore,
    members: list[str],
    signal_date: date,
    prev_date: date | None,
    decision_at: datetime,
) -> float | None:
    """题材内竞价价 > 昨收的家数占比（高开占比）。昨收缺失或无竞价 → None。"""
    if prev_date is None or not members:
        return None
    auctions = _visible_auction_rows(store, members, signal_date, decision_at)
    if auctions.empty:
        return None
    auctions = auctions.loc[
        (pd.to_datetime(auctions["trade_date"]).dt.date == signal_date)
        & (auctions["auction_type"] == _AUCTION_TYPE)
    ]
    closes = store._conn.execute(
        "SELECT ts_code, close FROM daily_bar "
        "WHERE trade_date = ? AND ts_code IN (SELECT UNNEST(?))",
        [prev_date, members],
    ).fetchdf()
    joined = auctions.merge(closes, on="ts_code", how="inner")
    valid = [
        (float(row.price), float(row.close))
        for row in joined.itertuples()
        if pd.notna(row.price) and pd.notna(row.close) and float(row.close) > 0
    ]
    if not valid:
        return None
    gap_up = sum(1 for price, pre_close in valid if price > pre_close)
    return round(gap_up / len(valid), 4)


def _board_auction_amount_ratio(
    store: DuckDBStore,
    members: list[str],
    signal_date: date,
    hist_days: int,
    decision_at: datetime,
) -> float | None:
    """题材当日竞价总额 / 该题材过去 hist_days 竞价总额中位（板块资金青睐度）。

    历史成分固定用当前成分集（无未来函数），逐历史日汇总题材竞价额取中位。
    """
    if not members:
        return None
    auctions = _visible_auction_rows(store, members, signal_date, decision_at)
    if auctions.empty:
        return None
    auctions = auctions.loc[auctions["auction_type"] == _AUCTION_TYPE].copy()
    auctions["trade_date"] = pd.to_datetime(auctions["trade_date"]).dt.date
    signal_amount = auctions.loc[
        auctions["trade_date"] == signal_date,
        "amount",
    ].sum(min_count=1)
    if pd.isna(signal_amount):
        return None
    hist_amounts = (
        auctions.loc[auctions["trade_date"] < signal_date]
        .groupby("trade_date")["amount"]
        .sum(min_count=1)
        .sort_index(ascending=False)
        .head(hist_days)
        .dropna()
        .astype(float)
        .tolist()
    )
    if not hist_amounts:
        return None
    median_hist = statistics.median(hist_amounts)
    if median_hist <= 0:
        return None
    return round(float(signal_amount) / median_hist, 4)


def board_auction_strength(
    store: DuckDBStore,
    ts_code: str,
    signal_date: date,
    *,
    membership_lookback_days: int = 30,
    hist_days: int = 20,
    decision_at: datetime | None = None,
) -> dict[str, object] | None:
    """信号日板块集合竞价强度：候选票所在题材的整体竞价青睐度。

    a. 取 [signal_date-lookback, signal_date] 内该票最近一次打点所属题材；
       无题材归属 → None（因子缺失，不判定）。
    b. 逐题材算板块级竞价指标：高开占比 board_gap_up_ratio、竞价资金相对历史
       board_auction_amount_ratio、成分数 board_member_count。
    c. 一票多题材时取 board_auction_amount_ratio 最大的题材（缺则并列取首个）。

    昨收从 daily_bar 的 T-1 取；竞价源按 decision_at 执行 PIT 过滤。默认以
    09:30 决策，因此 09:31 才可用的分钟回补竞价不会提前参与。
    """
    resolved_decision_at = decision_at or datetime.combine(
        signal_date,
        time(9, 30),
        tzinfo=EXCHANGE_TIMEZONE,
    )
    if resolved_decision_at.astimezone(EXCHANGE_TIMEZONE).date() != signal_date:
        raise ValueError("board auction decision_at must be on signal_date")
    membership = _latest_membership(
        store,
        ts_code,
        signal_date,
        membership_lookback_days,
        resolved_decision_at,
    )
    if membership is None:
        return None
    membership_date, boards = membership

    prev_row = store._conn.execute(
        "SELECT MAX(trade_date) FROM daily_bar WHERE trade_date < ?",
        [signal_date],
    ).fetchone()
    prev_date = prev_row[0] if prev_row else None

    metrics: list[dict[str, object]] = []
    for board_code, board_name in boards:
        members = _board_members(
            store,
            board_code,
            membership_date,
            resolved_decision_at,
        )
        if not members:
            continue
        metrics.append({
            "board_code": board_code,
            "board_name": board_name,
            "board_gap_up_ratio": _board_gap_up_ratio(
                store,
                members,
                signal_date,
                prev_date,
                resolved_decision_at,
            ),
            "board_auction_amount_ratio": _board_auction_amount_ratio(
                store,
                members,
                signal_date,
                hist_days,
                resolved_decision_at,
            ),
            "board_member_count": len(members),
        })
    if not metrics:
        return None

    def _rank(item: dict[str, object]) -> float:
        ratio = item["board_auction_amount_ratio"]
        return float(ratio) if ratio is not None else float("-inf")

    strongest = max(metrics, key=_rank)
    logger.debug(
        f"board_auction_strength {ts_code}@{signal_date}: "
        f"board={strongest['board_code']} "
        f"gap_up={strongest['board_gap_up_ratio']} "
        f"amt_ratio={strongest['board_auction_amount_ratio']} "
        f"members={strongest['board_member_count']}"
    )
    return strongest
