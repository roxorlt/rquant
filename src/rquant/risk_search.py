"""止损/止盈/移动止盈参数搜索。"""

from __future__ import annotations

from collections.abc import Callable, Iterable

import pandas as pd

from rquant.paper import PaperTradeConfig
from rquant.replay_entry_cache import (
    ReplayEntrySnapshot,
    replay_entry_snapshots_to_trades,
)


def _pct_label(value: float) -> str:
    pct = value * 100
    if pct.is_integer():
        return str(int(pct))
    return f"{pct:g}"


def build_risk_grid(
    *,
    stop_loss_pcts: Iterable[float],
    take_profit_pcts: Iterable[float],
    trailing_stop_pcts: Iterable[float],
    entry_buffer_pct: float = 0.005,
    entry_slippage_pct: float = 0.0,
) -> list[PaperTradeConfig]:
    """生成待比较的风控参数组合。"""
    configs: list[PaperTradeConfig] = []
    for stop_loss_pct in stop_loss_pcts:
        for take_profit_pct in take_profit_pcts:
            for trailing_stop_pct in trailing_stop_pcts:
                candidate_id = (
                    f"sl{_pct_label(stop_loss_pct)}_"
                    f"tp{_pct_label(take_profit_pct)}_"
                    f"tr{_pct_label(trailing_stop_pct)}"
                )
                configs.append(
                    PaperTradeConfig(
                        candidate_id=candidate_id,
                        stop_loss_pct=stop_loss_pct,
                        entry_buffer_pct=entry_buffer_pct,
                        entry_slippage_pct=entry_slippage_pct,
                        take_profit_pct=take_profit_pct,
                        trailing_stop_pct=trailing_stop_pct,
                    )
                )
    return configs


def _summary_row(
    config: PaperTradeConfig,
    trades: pd.DataFrame,
    *,
    min_trades: int,
) -> dict[str, object]:
    if trades.empty:
        return {
            "paper_candidate_id": config.candidate_id,
            "stop_loss_pct": config.stop_loss_pct,
            "take_profit_pct": config.take_profit_pct,
            "trailing_stop_pct": config.trailing_stop_pct,
            "trades": 0,
            "mean_ret_pct": None,
            "median_ret_pct": None,
            "win_rate_pct": None,
            "best_ret_pct": None,
            "worst_ret_pct": None,
            "robust_score": -999.0,
        }

    mean_ret = float(trades["ret_pct"].mean())
    win_rate = float((trades["ret_pct"] > 0).mean() * 100)
    worst_ret = float(trades["ret_pct"].min())
    low_sample_penalty = max(min_trades - len(trades), 0) * 2.0
    robust_score = (
        mean_ret
        + (win_rate - 50.0) * 0.02
        - abs(min(worst_ret, 0.0)) * 0.15
        - low_sample_penalty
    )
    return {
        "paper_candidate_id": config.candidate_id,
        "stop_loss_pct": config.stop_loss_pct,
        "take_profit_pct": config.take_profit_pct,
        "trailing_stop_pct": config.trailing_stop_pct,
        "trades": len(trades),
        "mean_ret_pct": round(mean_ret, 4),
        "median_ret_pct": round(float(trades["ret_pct"].median()), 4),
        "win_rate_pct": round(win_rate, 2),
        "best_ret_pct": round(float(trades["ret_pct"].max()), 4),
        "worst_ret_pct": round(worst_ret, 4),
        "robust_score": round(robust_score, 4),
    }


def run_risk_grid_search(
    configs: list[PaperTradeConfig],
    *,
    replay_loader: Callable[[PaperTradeConfig], pd.DataFrame],
    min_trades: int = 5,
) -> pd.DataFrame:
    """对一组风控参数进行排序。"""
    rows = [
        _summary_row(config, replay_loader(config), min_trades=min_trades)
        for config in configs
    ]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["robust_score", "mean_ret_pct", "win_rate_pct", "trades"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def run_risk_grid_search_from_entry_cache(
    configs: list[PaperTradeConfig],
    *,
    snapshot_loader: Callable[[], Iterable[ReplayEntrySnapshot]],
    min_trades: int = 5,
) -> pd.DataFrame:
    """基于同一批入场快照比较风控参数。"""
    snapshots = list(snapshot_loader())
    return run_risk_grid_search(
        configs,
        replay_loader=lambda config: replay_entry_snapshots_to_trades(
            snapshots,
            config,
        ),
        min_trades=min_trades,
    )
