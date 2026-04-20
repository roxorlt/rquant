"""每日全流水线：检查数据 → 遍历预设 → 落库结果。"""

from __future__ import annotations

import json

import pandas as pd
from loguru import logger

from rquant.presets import PRESET_SCREENS, ScreenPreset
from rquant.screen.core import screen
from rquant.storage.duckdb import DuckDBStore


def _get_prev_trading_date(
    store: DuckDBStore, trade_date: str, n: int = 1
) -> str | None:
    """trade_date 前第 n 个交易日（n=1 = 前一天）。"""
    row = store._conn.execute(
        """
        SELECT strftime(trade_date, '%Y-%m-%d') AS d
        FROM (
            SELECT DISTINCT trade_date FROM daily_bar
            WHERE trade_date < ?
            ORDER BY trade_date DESC
            LIMIT 1 OFFSET ?
        )
        """,
        [trade_date, n - 1],
    ).fetchone()
    return row[0] if row else None


def _to_screen_result_df(
    screen_df: pd.DataFrame,
    trade_date: str,
    preset_name: str,
) -> pd.DataFrame:
    """将 screen() 返回的 DataFrame 转为 screen_result 表格式。"""
    if screen_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date", "preset_name", "ts_code",
                "name", "close", "pct_chg", "extra",
            ]
        )

    base = {"ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"}
    extra_cols = [c for c in screen_df.columns if c not in base]

    result = pd.DataFrame({
        "trade_date": trade_date,
        "preset_name": preset_name,
        "ts_code": screen_df["ts_code"].values,
        "name": screen_df.get("name"),
        "close": screen_df.get("CLOSE[0]"),
        "pct_chg": screen_df.get("PCT_CHG[0]"),
    })

    if extra_cols:
        result["extra"] = screen_df[extra_cols].apply(
            lambda row: json.dumps(
                {k: v for k, v in row.items() if pd.notna(v)},
                ensure_ascii=False,
            ),
            axis=1,
        )
    else:
        result["extra"] = None

    return result


def _resolve_execution_order(
    presets: dict[str, ScreenPreset],
    names: list[str] | None = None,
) -> list[str]:
    """按依赖拓扑排序：无 depends_on 的先跑。"""
    if names:
        selected = {n: presets[n] for n in names if n in presets}
    else:
        selected = presets

    no_dep = [n for n, p in selected.items() if p.depends_on is None]
    has_dep = [n for n, p in selected.items() if p.depends_on is not None]
    return no_dep + has_dep


def run_daily_pipeline(
    trade_date: str,
    preset_names: list[str] | None = None,
    store: DuckDBStore | None = None,
) -> dict[str, int]:
    """遍历预设筛选并落库，返回 {preset_name: 命中数}。

    前置条件：trade_date 的 daily_bar / daily_indicator / daily_state
    数据已通过 ingest_daily.py 入库。
    """
    owns_store = store is None
    store = store or DuckDBStore()

    try:
        # 检查是否有数据
        count = store._conn.execute(
            "SELECT COUNT(*) FROM daily_bar WHERE trade_date = ?",
            [trade_date],
        ).fetchone()[0]
        if count == 0:
            logger.warning(f"{trade_date} 无 daily_bar 数据，跳过")
            return {}

        order = _resolve_execution_order(PRESET_SCREENS, preset_names)
        summary: dict[str, int] = {}

        for name in order:
            preset = PRESET_SCREENS[name]
            ts_whitelist: list[str] | None = None

            if preset.depends_on:
                parent_date = _get_prev_trading_date(
                    store, trade_date, preset.offset_days
                )
                if parent_date is None:
                    logger.warning(
                        f"{name}: 找不到 {preset.offset_days} "
                        f"个交易日前的日期，跳过"
                    )
                    summary[name] = 0
                    continue

                parent_df = store.query_screen_result(
                    parent_date, preset.depends_on
                )
                ts_whitelist = (
                    parent_df["ts_code"].tolist()
                    if not parent_df.empty
                    else []
                )
                if not ts_whitelist:
                    logger.info(
                        f"{name}: 父预设 {preset.depends_on} "
                        f"在 {parent_date} 无命中，跳过"
                    )
                    summary[name] = 0
                    continue

            result_df = screen(
                trade_date=trade_date,
                rules=preset.rules,
                include_columns=preset.include_columns or None,
                store=store,
                ts_code_whitelist=ts_whitelist,
            )

            sr_df = _to_screen_result_df(result_df, trade_date, name)
            store.upsert_screen_result(sr_df)

            hit_count = len(result_df)
            summary[name] = hit_count
            logger.info(f"  {name}: {hit_count} 命中")

        logger.info(f"流水线完成 {trade_date}: {summary}")
        return summary
    finally:
        if owns_store:
            store.close()
