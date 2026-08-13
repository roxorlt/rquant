"""分钟策略版本对比。"""

from __future__ import annotations

from datetime import date
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict

from rquant.minute_replay import (
    DEFAULT_FACTOR_SCORE_THRESHOLD,
    EntryMode,
    MinuteFreq,
    run_minute_strong_carry_replay,
)
from rquant.paper import PaperTradeConfig
from rquant.research_run_spec import ExecutionCostSpec
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_execution_costs import apply_round_trip_execution_costs
from rquant.volume_profile import VolumeProfileRuleConfig

ProfileVariant = Literal["baseline", "vp_risk_only", "vp_90"]


class StrategyComparisonResult(BaseModel):
    """分钟策略版本对比结果。"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    candidates_count: int
    trades: pd.DataFrame
    summary: pd.DataFrame


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value[:10])


def _candidate_count(
    store: DuckDBStore,
    start_date: str | date,
    end_date: str | date,
    preset_name: str,
) -> int:
    if preset_name == "n-shape-combined":
        row = store._conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT trade_date, ts_code
                FROM screen_result
                WHERE trade_date >= ?
                  AND trade_date <= ?
                  AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
            )
            """,
            [_parse_date(start_date), _parse_date(end_date)],
        ).fetchone()
        return int(row[0]) if row else 0
    row = store._conn.execute(
        """
        SELECT COUNT(*)
        FROM screen_result
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND preset_name = ?
        """,
        [_parse_date(start_date), _parse_date(end_date), preset_name],
    ).fetchone()
    return int(row[0]) if row else 0


def _volume_profile_config(variant: ProfileVariant) -> VolumeProfileRuleConfig:
    if variant == "vp_risk_only":
        return VolumeProfileRuleConfig(
            enabled=True,
            filter_entry=False,
            require_profile=False,
            lookback_days=(90,),
        )
    if variant == "vp_90":
        return VolumeProfileRuleConfig(enabled=True, lookback_days=(90,))
    return VolumeProfileRuleConfig(enabled=False)


def _summary_row(
    mode: EntryMode,
    variant: ProfileVariant,
    trades: pd.DataFrame,
    candidates_count: int,
) -> dict[str, object]:
    if trades.empty:
        return {
            "entry_mode": mode,
            "profile_variant": variant,
            "candidates": candidates_count,
            "trades": 0,
            "trigger_rate_pct": 0.0,
            "mean_ret_pct": None,
            "median_ret_pct": None,
            "win_rate_pct": None,
            "best_ret_pct": None,
            "worst_ret_pct": None,
            "gap_stop_rate_pct": None,
        }

    return {
        "entry_mode": mode,
        "profile_variant": variant,
        "candidates": candidates_count,
        "trades": len(trades),
        "trigger_rate_pct": round(len(trades) / candidates_count * 100, 2)
        if candidates_count
        else None,
        "mean_ret_pct": round(float(trades["ret_pct"].mean()), 4),
        "median_ret_pct": round(float(trades["ret_pct"].median()), 4),
        "win_rate_pct": round(float((trades["ret_pct"] > 0).mean() * 100), 2),
        "best_ret_pct": round(float(trades["ret_pct"].max()), 4),
        "worst_ret_pct": round(float(trades["ret_pct"].min()), 4),
        "gap_stop_rate_pct": round(
            float((trades["exit_reason"] == "gap_stop").mean() * 100),
            2,
        ),
    }


def run_entry_mode_comparison(
    store: DuckDBStore,
    *,
    start_date: str | date,
    end_date: str | date,
    entry_modes: list[EntryMode],
    profile_variants: list[ProfileVariant] | None = None,
    preset_name: str = "n-shape-pool1",
    max_hold_days: int = 1,
    freq: MinuteFreq = "1min",
    paper_config: PaperTradeConfig | None = None,
    factor_score_threshold: float = DEFAULT_FACTOR_SCORE_THRESHOLD,
    execution_costs: ExecutionCostSpec | None = None,
) -> StrategyComparisonResult:
    """按多个入场模式跑分钟 replay，并生成对比表。"""
    candidates_count = _candidate_count(store, start_date, end_date, preset_name)
    trade_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []
    variants = profile_variants or ["baseline"]

    for mode in entry_modes:
        for variant in variants:
            trades = run_minute_strong_carry_replay(
                store,
                start_date=start_date,
                end_date=end_date,
                preset_name=preset_name,
                freq=freq,
                entry_mode=mode,
                max_hold_days=max_hold_days,
                paper_config=paper_config,
                volume_profile_config=_volume_profile_config(variant),
                factor_score_threshold=factor_score_threshold,
            )
            if execution_costs is not None:
                trades = apply_round_trip_execution_costs(trades, execution_costs)
            if not trades.empty:
                trades = trades.copy()
                trades.insert(0, "profile_variant", variant)
                trades.insert(0, "entry_mode", mode)
                trade_frames.append(trades)
            summary_rows.append(_summary_row(mode, variant, trades, candidates_count))

    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    return StrategyComparisonResult(
        candidates_count=candidates_count,
        trades=all_trades,
        summary=summary,
    )
