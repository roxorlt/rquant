"""T 日板块集合竞价强度因子（无未来函数）。

候选票所在题材（按 ≤T 最近一次打点还原成分）在信号日集合竞价的整体强度，
用作 growth_board_surge 的候选级闸门与评分观察因子。全程只用 ≤signal_date
数据：题材成分取信号日前最近一次打点（kpl_concept_member_daily）、昨收取
daily_bar 的 T-1 收盘、竞价价/额取 auction_bar 的信号日 09:25 集合竞价快照。

一票多题材（kpl 一只股票可挂多个题材）时取「板块竞价资金相对历史」最强的题材。
"""

from __future__ import annotations

import statistics
from datetime import date, timedelta

from loguru import logger

from rquant.storage.duckdb import DuckDBStore

# auction_bar 只用开盘集合竞价快照（9:25 已知，无未来函数）
_AUCTION_TYPE = "open_realtime"


def _placeholders(n: int) -> str:
    return ", ".join("?" for _ in range(n))


def _latest_membership(
    store: DuckDBStore,
    ts_code: str,
    signal_date: date,
    membership_lookback_days: int,
) -> tuple[date, list[tuple[str, str]]] | None:
    """[signal_date-lookback, signal_date] 内该票最近一次打点日及其所属题材。

    返回 (打点日, [(board_code, board_name), ...])；窗口内无打点 → None。
    """
    lower = signal_date - timedelta(days=membership_lookback_days)
    rows = store._conn.execute(
        """
        SELECT board_code, board_name, trade_date
        FROM kpl_concept_member_daily
        WHERE con_code = ? AND trade_date >= ? AND trade_date <= ?
        """,
        [ts_code, lower, signal_date],
    ).fetchall()
    if not rows:
        return None
    latest = max(r[2] for r in rows)
    boards = [(r[0], r[1]) for r in rows if r[2] == latest]
    return latest, boards


def _board_members(
    store: DuckDBStore, board_code: str, membership_date: date
) -> list[str]:
    rows = store._conn.execute(
        """
        SELECT con_code FROM kpl_concept_member_daily
        WHERE board_code = ? AND trade_date = ?
        """,
        [board_code, membership_date],
    ).fetchall()
    return [r[0] for r in rows]


def _board_gap_up_ratio(
    store: DuckDBStore,
    members: list[str],
    signal_date: date,
    prev_date: date | None,
) -> float | None:
    """题材内竞价价 > 昨收的家数占比（高开占比）。昨收缺失或无竞价 → None。"""
    if prev_date is None or not members:
        return None
    ph = _placeholders(len(members))
    rows = store._conn.execute(
        f"""
        SELECT ab.price, db.close
        FROM auction_bar ab
        JOIN daily_bar db
          ON ab.ts_code = db.ts_code AND db.trade_date = ?
        WHERE ab.trade_date = ?
          AND ab.auction_type = ?
          AND ab.ts_code IN ({ph})
        """,
        [prev_date, signal_date, _AUCTION_TYPE, *members],
    ).fetchall()
    valid = [
        (float(p), float(pc))
        for p, pc in rows
        if p is not None and pc is not None and float(pc) > 0
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
) -> float | None:
    """题材当日竞价总额 / 该题材过去 hist_days 竞价总额中位（板块资金青睐度）。

    历史成分固定用当前成分集（无未来函数），逐历史日汇总题材竞价额取中位。
    """
    if not members:
        return None
    ph = _placeholders(len(members))
    signal_amount = store._conn.execute(
        f"""
        SELECT SUM(amount) FROM auction_bar
        WHERE trade_date = ? AND auction_type = ?
          AND ts_code IN ({ph})
        """,
        [signal_date, _AUCTION_TYPE, *members],
    ).fetchone()[0]
    if signal_amount is None:
        return None
    hist_rows = store._conn.execute(
        f"""
        SELECT SUM(amount) AS board_amount
        FROM auction_bar
        WHERE trade_date < ? AND auction_type = ?
          AND ts_code IN ({ph})
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [signal_date, _AUCTION_TYPE, *members, hist_days],
    ).fetchall()
    hist_amounts = [float(r[0]) for r in hist_rows if r[0] is not None]
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
) -> dict[str, object] | None:
    """信号日板块集合竞价强度：候选票所在题材的整体竞价青睐度。

    a. 取 [signal_date-lookback, signal_date] 内该票最近一次打点所属题材；
       无题材归属 → None（因子缺失，不判定）。
    b. 逐题材算板块级竞价指标：高开占比 board_gap_up_ratio、竞价资金相对历史
       board_auction_amount_ratio、成分数 board_member_count。
    c. 一票多题材时取 board_auction_amount_ratio 最大的题材（缺则并列取首个）。

    昨收从 daily_bar 的 T-1 取、竞价价/额从 auction_bar 的 signal_date 取，
    全程 ≤signal_date（无未来函数）。
    """
    membership = _latest_membership(
        store, ts_code, signal_date, membership_lookback_days
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
        members = _board_members(store, board_code, membership_date)
        if not members:
            continue
        metrics.append({
            "board_code": board_code,
            "board_name": board_name,
            "board_gap_up_ratio": _board_gap_up_ratio(
                store, members, signal_date, prev_date
            ),
            "board_auction_amount_ratio": _board_auction_amount_ratio(
                store, members, signal_date, hist_days
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
