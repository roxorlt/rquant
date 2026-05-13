"""Week 7.5 B 阶段：Pool 的 per-rule diagnostic 漏斗 + 命中标的查询。

跟 `llm/dispatch.py:screen_with_plan_diagnostic` 设计同源，但输入是 ScreenPreset
而不是 ScreenPlan，且不重复跑 N 次 screen() —— 只 load_universe 一次，规则在
内存里 incremental 应用，性能从 O(N) SQL → 1 次 SQL + O(N) pandas mask。

设计要点：
- 走 read_only=True 的 DuckDBStore（跟 monitor 共存）
- depends_on 链：先跑上游 pool 拿 ts_codes，下游用 ts_whitelist
- offset_days 简化：上游用当前 trade_date 跑（不像 pipeline 那样合并 T-1~T-N）。
  画布是当前状态预览，不是 production 流水线。
"""

from __future__ import annotations

import pandas as pd

from rquant.presets import PRESET_SCREENS, ScreenPreset
from rquant.screen.core import BASE_COLUMNS, _collect_aggregates, _infer_lookback
from rquant.screen.loader import load_universe
from rquant.storage.duckdb import DuckDBStore


def _rule_display_name(rule) -> str:
    """优先用 rules.py 显式挂的 __rquant_name__（含参数），否则取 __qualname__ 第一段。

    背景：screen/rules.py 里的 factory 大多直接 def _rule(): ...，外层名直接可读
    （如 `gt._rule` → "gt"）；少数 factory 转调内部工厂 `_bool_state_rule`，导致
    `__qualname__` 失效（变成 `_bool_state_rule.<locals>._rule`）—— 这种情况由
    factory 显式挂 __rquant_name__ 提供 friendly 名。
    """
    friendly = getattr(rule, "__rquant_name__", None)
    if friendly:
        return friendly
    qualname = getattr(rule, "__qualname__", "")
    if qualname:
        return qualname.split(".")[0]
    return getattr(rule, "__name__", "rule")


def _run_rules_incremental(
    df: pd.DataFrame,
    rules: list,
) -> tuple[pd.Series, list[tuple[str, int]]]:
    """对预加载的 df 逐条应用 rules，返回最终 mask + diagnostic 序列。

    diagnostic 序列开头是 ('(初始)', len(df))，之后每条规则一项。
    """
    mask = pd.Series(True, index=df.index)
    diagnostics: list[tuple[str, int]] = [("(初始)", int(mask.sum()))]
    for rule in rules:
        mask &= rule(df).reindex(df.index, fill_value=False)
        diagnostics.append((_rule_display_name(rule), int(mask.sum())))
    return mask, diagnostics


def _project_result(df: pd.DataFrame, mask: pd.Series, include_columns: list[str]) -> pd.DataFrame:
    cols = list(BASE_COLUMNS)
    if include_columns:
        cols += [c for c in include_columns if c not in cols]
    cols = [c for c in cols if c in df.columns]
    return df.loc[mask, cols].sort_values("ts_code").reset_index(drop=True)


def diagnose_preset(
    preset_name: str,
    trade_date: str,
    *,
    store: DuckDBStore,
) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """跑单个 preset 的 diagnostic：

    返回 (final_hits_df, [(rule_name, count_after_this_rule), ...])

    - 有 depends_on：先跑父 preset 拿 ts_codes，传给当前 preset 作 ts_whitelist
    - 无 depends_on：universe 全市场
    - 父 preset 跑出来空 → 当前 preset 也是空，diagnostic 只有 ('(初始)', 0)
    """
    if preset_name not in PRESET_SCREENS:
        raise KeyError(f"未知 preset: {preset_name}")
    preset: ScreenPreset = PRESET_SCREENS[preset_name]

    # 上游 ts_whitelist
    ts_whitelist: list[str] | None = None
    if preset.depends_on and preset.depends_on in PRESET_SCREENS:
        parent_final, _ = diagnose_preset(preset.depends_on, trade_date, store=store)
        ts_whitelist = parent_final["ts_code"].tolist() if not parent_final.empty else []
        if not ts_whitelist:
            return parent_final.iloc[0:0], [("(初始-父预设空)", 0)]

    # 一次 load_universe（基于 rules 推断 lookback + aggregates）
    lookback = _infer_lookback(preset.rules)
    aggregates = _collect_aggregates(preset.rules)
    df = load_universe(
        trade_date, lookback=lookback, store=store, aggregate_requests=aggregates
    )
    if ts_whitelist is not None:
        df = df[df["ts_code"].isin(ts_whitelist)].reset_index(drop=True)

    if df.empty or not preset.rules:
        return _project_result(df, pd.Series(True, index=df.index), preset.include_columns or []), [
            ("(初始)", int(len(df)))
        ]

    mask, diagnostics = _run_rules_incremental(df, preset.rules)
    final = _project_result(df, mask, preset.include_columns or [])
    return final, diagnostics


def latest_trade_date(store: DuckDBStore) -> str:
    """返回 daily_bar 中最大 trade_date（YYYY-MM-DD）。无数据返回今天。"""
    row = store._conn.execute(
        "SELECT MAX(trade_date) FROM daily_bar"
    ).fetchone()
    if row and row[0]:
        return row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])
    return pd.Timestamp.today().strftime("%Y-%m-%d")
