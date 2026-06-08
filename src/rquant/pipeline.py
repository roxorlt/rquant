"""每日全流水线：检查数据 → 遍历预设 → 落库结果。"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
from loguru import logger

from rquant.presets import PRESET_SCREENS, ScreenPreset
from rquant.risk.blacklist import load_active_blacklist
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
    selected = (
        {n: presets[n] for n in names if n in presets} if names else presets
    )

    no_dep = [n for n, p in selected.items() if p.depends_on is None]
    has_dep = [n for n, p in selected.items() if p.depends_on is not None]
    return no_dep + has_dep


def _compute_levels(body_upper: float, body_lower: float) -> dict[str, float]:
    """根据涨停日实体算 5 个档位价。"""
    body = body_upper - body_lower
    return {
        "level_40": body_lower + body * 0.4,
        "level_30": body_lower + body * 0.3,
        "level_20": body_lower + body * 0.2,
        "stop_strong": body_lower,
        "stop_weak": body_lower - body * 0.2,
    }


def _sync_pool2_watch(store: DuckDBStore, trade_date: str) -> None:
    """将今日 Pool 2 screen_result 同步到 pool2_watch 持久池。

    只添加新票（pool2_watch 中不存在或已 exited 的重新激活）。
    """
    pool2_sr = store.query_screen_result(trade_date, "n-shape-pool2")
    if pool2_sr.empty:
        return

    existing = store.query_pool2_active()
    existing_codes = set(existing["ts_code"].tolist()) if not existing.empty else set()

    new_rows = []
    for _, row in pool2_sr.iterrows():
        code = row["ts_code"]
        if code in existing_codes:
            continue

        # 找涨停日：Pool 2 筛选已保证涨停在近几日内，无需额外日期窗口
        state_df = store._conn.execute(
            """
            SELECT trade_date, body_upper, body_lower
            FROM daily_state
            WHERE ts_code = ? AND is_first_limit_up = true
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [code],
        ).fetchdf()

        if state_df.empty:
            logger.warning(f"pool2_watch 同步跳过 {code}：找不到涨停日")
            continue

        limit_up_date = state_df.iloc[0]["trade_date"]
        bu = float(state_df.iloc[0]["body_upper"])
        bl = float(state_df.iloc[0]["body_lower"])
        levels = _compute_levels(bu, bl)

        new_rows.append({
            "ts_code": code,
            "entry_date": date.fromisoformat(trade_date),
            "limit_up_date": limit_up_date,
            "body_upper": bu,
            "body_lower": bl,
            **levels,
            "status": "active",
        })

    if new_rows:
        store.upsert_pool2_watch(pd.DataFrame(new_rows))
        logger.info(f"pool2_watch 新增 {len(new_rows)} 只")


def _notify_error_safe(component: str, exc: BaseException) -> None:
    """推 error 通知，自身失败也不抛——避免"错误处理再出错"二次中断流程。"""
    try:
        from rquant.notify import notify

        notify("error", component=component, exc=exc)
    except Exception:
        logger.exception(f"notify error 失败 ({component})")


def run_daily_pipeline(
    trade_date: str,
    preset_names: list[str] | None = None,
    store: DuckDBStore | None = None,
) -> dict[str, int]:
    """遍历预设筛选并落库，返回 {preset_name: 命中数}。

    前置条件：trade_date 的 daily_bar / daily_indicator / daily_state
    数据已通过 ingest_daily.py 入库。
    """
    import time as _time

    owns_store = store is None
    store = store or DuckDBStore()
    started_at = _time.time()

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

        # 黑名单：在每个 preset upsert 前过滤新推荐（保留 pool2_watch 中已存在条目）
        blacklist = load_active_blacklist(store)
        if blacklist:
            logger.info(
                f"风险黑名单 active: {len(blacklist)} 只，"
                f"将过滤所有 preset 新推荐"
            )

        for name in order:
            # 单个 preset 故障隔离：某 preset 抛异常（规则引用缺失列、聚合 SQL 报错、
            # user preset 规则坏掉等）不应拖垮其他 preset，更不能跳过后面的
            # _sync_pool2_watch / check_exits（那会连带打掉 check_exits 兜底）。
            try:
                preset = PRESET_SCREENS[name]
                ts_whitelist: list[str] | None = None

                if preset.depends_on:
                    # offset_days=N 表示合并 T-1 到 T-N 的父预设结果
                    ts_whitelist = []
                    parent_dates = []
                    for offset in range(1, preset.offset_days + 1):
                        parent_date = _get_prev_trading_date(
                            store, trade_date, offset
                        )
                        if parent_date is None:
                            continue
                        parent_df = store.query_screen_result(
                            parent_date, preset.depends_on
                        )
                        if not parent_df.empty:
                            ts_whitelist.extend(parent_df["ts_code"].tolist())
                            parent_dates.append(parent_date)

                    ts_whitelist = list(set(ts_whitelist))  # 去重

                    if not ts_whitelist:
                        logger.info(
                            f"{name}: 父预设 {preset.depends_on} "
                            f"在 T-1~T-{preset.offset_days} 无命中，跳过"
                        )
                        summary[name] = 0
                        continue

                    logger.info(
                        f"{name}: 从 {parent_dates} 合并 "
                        f"{len(ts_whitelist)} 只白名单"
                    )

                result_df = screen(
                    trade_date=trade_date,
                    rules=preset.rules,
                    include_columns=preset.include_columns or None,
                    store=store,
                    ts_code_whitelist=ts_whitelist,
                )

                sr_df = _to_screen_result_df(result_df, trade_date, name)

                if blacklist and not sr_df.empty:
                    hit_mask = sr_df["ts_code"].isin(blacklist.keys())
                    if hit_mask.any():
                        removed = sr_df.loc[hit_mask, "ts_code"].tolist()
                        sr_df = sr_df.loc[~hit_mask].reset_index(drop=True)
                        logger.warning(
                            f"  {name}: 黑名单过滤剔除 {len(removed)} 只 → {removed}"
                        )

                store.upsert_screen_result(sr_df)

                hit_count = len(sr_df)
                summary[name] = hit_count
                logger.info(f"  {name}: {hit_count} 命中")
            except Exception as e:
                summary[name] = -1  # -1 标记该 preset 失败，区别于 0 命中
                logger.exception(f"preset {name} 执行失败，跳过，继续后续 preset")
                _notify_error_safe(f"pipeline:preset:{name}", e)
                continue

        # 同步 Pool 2 到持久池（新涨停的票入池）— 独立隔离
        try:
            _sync_pool2_watch(store, trade_date)
        except Exception as e:
            logger.exception("_sync_pool2_watch 失败")
            _notify_error_safe("pipeline:sync_pool2", e)

        # Pool 2 退出检查（兜底）：原本由 monitor 15:00 收盘后跑，但 monitor 在盘中
        # 被 deploy / watchdog 等 restart 时 SIGTERM 中断会跳过 check_exits，导致
        # aged_out（超过 pool2_max_age_days）和 breakdown（跌破止损）的票留在池里。
        # daily 17:00 是 oneshot timer 必跑，作为可靠兜底。重复执行无副作用：
        # 已 exited 的不会再出现在 active，check_exits 内部按 active 池过滤。
        try:
            from datetime import date as _date

            from rquant.monitor import check_exits

            check_exits(store, _date.fromisoformat(trade_date))
        except Exception as e:
            logger.exception("check_exits 失败")
            _notify_error_safe("pipeline:check_exits", e)

        elapsed = _time.time() - started_at
        logger.info(f"流水线完成 {trade_date}: {summary} (耗时 {elapsed:.1f}s)")

        # 推送失败不应影响 pipeline 退出码（数据已落库，推送是旁路）
        try:
            _push_daily_summary(store, trade_date, elapsed)
        except Exception as e:
            logger.exception("_push_daily_summary 失败")
            _notify_error_safe("pipeline:push_summary", e)

        return summary
    finally:
        if owns_store:
            store.close()


def _push_daily_summary(
    store: DuckDBStore,
    trade_date: str,
    duration_seconds: float,
) -> None:
    """流水线完成后推 C 类汇总。"""
    from rquant.notify import notify

    blacklist = load_active_blacklist(store)

    pool1_df = store.query_screen_result(trade_date, "n-shape-pool1")
    pool1_hits = [
        {
            "ts_code": row["ts_code"],
            "name": row.get("name", ""),
            "close": float(row["close"]),
            "blacklist_label": (
                blacklist[row["ts_code"]].list_label
                if row["ts_code"] in blacklist
                else None
            ),
        }
        for _, row in pool1_df.iterrows()
    ]

    pool2_df = store.query_pool2_active()
    pool2_active: list[dict] = []
    if not pool2_df.empty:
        # 批量取股票名
        codes = pool2_df["ts_code"].tolist()
        placeholders = ",".join("?" * len(codes))
        name_df = store._conn.execute(
            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
            codes,
        ).fetchdf()
        name_map = dict(zip(name_df["ts_code"], name_df["name"]))

        from datetime import date as _date
        today = _date.fromisoformat(trade_date)
        for _, row in pool2_df.iterrows():
            raw_entry = row["entry_date"]
            entry_d = raw_entry.date() if hasattr(raw_entry, "date") else raw_entry
            days = _count_trading_days_since(store, entry_d, today)
            code = row["ts_code"]
            pool2_active.append({
                "ts_code": code,
                "name": name_map.get(code, ""),
                "entry_date": entry_d,
                "days_in_pool": days,
                "blacklist_label": (
                    blacklist[code].list_label if code in blacklist else None
                ),
            })

    notify(
        "daily_summary",
        trade_date=trade_date,
        pool1_hits=pool1_hits,
        pool2_active=pool2_active,
        duration_seconds=duration_seconds,
    )


def _count_trading_days_since(store: DuckDBStore, entry_date, today) -> int:
    """entry_date 到 today 之间有多少个交易日（含两端）。"""
    row = store._conn.execute(
        "SELECT COUNT(DISTINCT trade_date) FROM daily_bar "
        "WHERE trade_date >= ? AND trade_date <= ?",
        [entry_date, today],
    ).fetchone()
    return row[0] if row else 0
