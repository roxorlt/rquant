"""每日全流水线：检查数据 → 遍历预设 → 落库结果。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from loguru import logger
from pydantic import Field

from rquant.builtin_presets import builtin_definition_version
from rquant.pool_result_receipt import ScreenRunReceipt, member_set_digest
from rquant.presets import PRESET_SCREENS, ScreenPreset, load_user_presets
from rquant.risk.blacklist import load_active_blacklist
from rquant.runtime_contracts import RuntimeContractModel
from rquant.screen.core import screen
from rquant.storage.duckdb import DuckDBStore


class DailyScreenPipelineResult(RuntimeContractModel):
    """Durable screen-stage payload; errors are delegated to the notification outbox."""

    preset_hits: Mapping[str, int] = Field(default_factory=dict)
    errors: tuple[str, ...] = ()


class DailyPoolPipelineResult(RuntimeContractModel):
    """Pool mutations performed by the single daily downstream writer."""

    pool2_added: int = Field(ge=0)
    pool2_exited: int = Field(ge=0)
    pool2_active_count: int = Field(ge=0)
    errors: tuple[str, ...] = ()


class ParentMarketDataGapError(LookupError):
    """A trusted parent trading day has no daily market rows."""


class ParentRunEvidenceError(LookupError):
    """A parent result cannot be tied to its definition and exact members."""


def _definition_version(preset: ScreenPreset) -> str:
    return preset.definition_version or builtin_definition_version(preset)


def _verified_parent_run(
    store: DuckDBStore,
    presets: Mapping[str, ScreenPreset],
    trade_date: str,
    name: str,
    *,
    seen: frozenset[tuple[str, str]] = frozenset(),
) -> tuple[list[str], ScreenRunReceipt]:
    key = (trade_date, name)
    if key in seen:
        raise ParentRunEvidenceError("pool result lineage contains a cycle")
    preset = presets.get(name)
    receipt = store.query_screen_run_receipt(trade_date, name)
    if preset is None or receipt is None:
        raise ParentRunEvidenceError(f"parent run receipt missing: {name}/{trade_date}")
    codes = store.query_screen_result(trade_date, name)["ts_code"].tolist()
    if (
        receipt.definition_version != _definition_version(preset)
        or receipt.hit_count != len(codes)
        or receipt.member_digest != member_set_digest(codes)
    ):
        raise ParentRunEvidenceError(f"parent run differs from definition or members: {name}")
    if preset.delay_days is not None and preset.depends_on is not None:
        expected_date = _get_exact_delayed_trading_date(
            store, trade_date, preset.delay_days
        )
        if receipt.parent_trade_date != date.fromisoformat(expected_date):
            raise ParentRunEvidenceError(f"parent run has a stale ancestor date: {name}")
        _, ancestor = _verified_parent_run(
            store,
            presets,
            expected_date,
            preset.depends_on,
            seen=seen | {key},
        )
        if receipt.parent_result_version != ancestor.result_version:
            raise ParentRunEvidenceError(f"parent run has a stale ancestor version: {name}")
    elif preset.depends_on is None and receipt.parent_result_version is not None:
        raise ParentRunEvidenceError(f"root pool run has unexpected parent: {name}")
    return codes, receipt


def _get_prev_trading_date(store: DuckDBStore, trade_date: str, n: int = 1) -> str | None:
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


def _get_exact_delayed_trading_date(
    store: DuckDBStore, trade_date: str, delay_days: int
) -> str:
    anchor = date.fromisoformat(trade_date)
    if not store.is_trading_day("SSE", anchor):
        raise ValueError(f"v2 screen date is not an open trading day: {trade_date}")
    for _ in range(delay_days):
        anchor = store.previous_trading_day(anchor)
    available = store._conn.execute(
        "SELECT EXISTS(SELECT 1 FROM daily_bar WHERE trade_date = ?)",
        [anchor],
    ).fetchone()[0]
    if not available:
        raise ParentMarketDataGapError(
            f"daily_bar missing for exact parent trading day {anchor.isoformat()}"
        )
    return anchor.isoformat()


def _to_screen_result_df(
    screen_df: pd.DataFrame,
    trade_date: str,
    preset_name: str,
) -> pd.DataFrame:
    """将 screen() 返回的 DataFrame 转为 screen_result 表格式。"""
    if screen_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "preset_name",
                "ts_code",
                "name",
                "close",
                "pct_chg",
                "extra",
            ]
        )

    base = {"ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"}
    extra_cols = [c for c in screen_df.columns if c not in base]

    result = pd.DataFrame(
        {
            "trade_date": trade_date,
            "preset_name": preset_name,
            "ts_code": screen_df["ts_code"].values,
            "name": screen_df.get("name"),
            "close": screen_df.get("CLOSE[0]"),
            "pct_chg": screen_df.get("PCT_CHG[0]"),
        }
    )

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
    """按依赖拓扑排序，等价节点保持注册表声明顺序。"""
    selected = {n: presets[n] for n in names if n in presets} if names is not None else presets
    state: dict[str, int] = {}
    ordered: list[str] = []
    for name in selected:
        chain: list[str] = []
        current = name
        while current in selected and state.get(current) != 2:
            if state.get(current) == 1:
                raise ValueError("pool dependency cycle")
            state[current] = 1
            chain.append(current)
            parent = selected[current].depends_on
            if parent is None:
                break
            current = parent
        for item in reversed(chain):
            state[item] = 2
            ordered.append(item)
    return ordered


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


def _sync_pool2_watch(store: DuckDBStore, trade_date: str) -> int:
    """将今日 Pool 2 screen_result 同步到 pool2_watch 持久池。

    只添加新票（pool2_watch 中不存在或已 exited 的重新激活）。
    """
    pool2_sr = store.query_screen_result(trade_date, "n-shape-pool2")
    if pool2_sr.empty:
        return 0

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

        new_rows.append(
            {
                "ts_code": code,
                "entry_date": date.fromisoformat(trade_date),
                "limit_up_date": limit_up_date,
                "body_upper": bu,
                "body_lower": bl,
                **levels,
                "status": "active",
            }
        )

    if new_rows:
        store.upsert_pool2_watch(pd.DataFrame(new_rows))
        logger.info(f"pool2_watch 新增 {len(new_rows)} 只")
    return len(new_rows)


def run_daily_screen_stage(
    trade_date: str,
    *,
    preset_names: list[str] | None = None,
    store: DuckDBStore,
    preset_directory: Path | None = None,
    transaction_open: bool = False,
) -> DailyScreenPipelineResult:
    """Run only screen materialization. Notification is deliberately out of band."""
    count = store._conn.execute(
        "SELECT COUNT(*) FROM daily_bar WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    if count == 0:
        logger.warning(f"{trade_date} 无 daily_bar 数据，跳过")
        return DailyScreenPipelineResult(preset_hits={}, errors=())

    from rquant.config import settings

    directory = (
        Path(settings.data_dir) / "user_presets"
        if preset_directory is None
        else preset_directory
    )
    presets = {
        name: preset
        for name, preset in PRESET_SCREENS.items()
        if not name.startswith("user/")
    }
    presets.update(load_user_presets(directory))
    order = _resolve_execution_order(presets, preset_names)
    summary: dict[str, int] = {}
    errors: list[str] = []
    blacklist = load_active_blacklist(store)
    if blacklist:
        logger.info(f"风险黑名单 active: {len(blacklist)} 只，将过滤所有 preset 新推荐")

    for name in order:
        writing = False
        transaction_started = False
        try:
            if not transaction_open:
                store._conn.execute("BEGIN")
                transaction_started = True
            preset = presets[name]
            ts_whitelist: list[str] | None = None
            parent_trade_date: date | None = None
            parent_result_version: str | None = None
            lineage_complete = preset.depends_on is None
            if preset.depends_on:
                ts_whitelist = []
                parent_dates = []
                # v2 延后只取 T-N；旧 offset_days 保持 T-1..T-N 回看窗口。
                if preset.delay_days is not None:
                    exact_date = _get_exact_delayed_trading_date(
                        store, trade_date, preset.delay_days
                    )
                    parent_codes, parent_receipt = _verified_parent_run(
                        store, presets, exact_date, preset.depends_on
                    )
                    ts_whitelist = parent_codes
                    parent_trade_date = date.fromisoformat(exact_date)
                    parent_result_version = parent_receipt.result_version
                    lineage_complete = parent_receipt.lineage_complete
                else:
                    candidate_dates = tuple(
                        _get_prev_trading_date(store, trade_date, offset)
                        for offset in range(1, preset.offset_days + 1)
                    )
                    for parent_date in candidate_dates:
                        if parent_date is None:
                            continue
                        parent_df = store.query_screen_result(parent_date, preset.depends_on)
                        if not parent_df.empty:
                            ts_whitelist.extend(parent_df["ts_code"].tolist())
                            parent_dates.append(parent_date)
                ts_whitelist = sorted(set(ts_whitelist))
                if not ts_whitelist:
                    lookback = (
                        f"T-{preset.delay_days}"
                        if preset.delay_days is not None
                        else f"T-1~T-{preset.offset_days}"
                    )
                    logger.info(
                        f"{name}: 父预设 {preset.depends_on} "
                        f"在 {lookback} 无命中，跳过"
                    )
                    empty = _to_screen_result_df(pd.DataFrame(), trade_date, name)
                    receipt = ScreenRunReceipt(
                        trade_date=date.fromisoformat(trade_date),
                        preset_name=name,
                        definition_version=_definition_version(preset),
                        parent_trade_date=parent_trade_date,
                        parent_result_version=parent_result_version,
                        hit_count=0,
                        member_digest=member_set_digest([]),
                        lineage_complete=lineage_complete,
                        completed_at=datetime.now(UTC),
                    )
                    writing = True
                    store.replace_screen_result_with_receipt(
                        trade_date, name, empty, receipt,
                        manage_transaction=False,
                    )
                    writing = False
                    if transaction_started:
                        store._conn.execute("COMMIT")
                        transaction_started = False
                    summary[name] = 0
                    continue
                dates = [parent_trade_date.isoformat()] if parent_trade_date else parent_dates
                logger.info(f"{name}: 从 {dates} 合并 {len(ts_whitelist)} 只白名单")

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
                    logger.warning(f"  {name}: 黑名单过滤剔除 {len(removed)} 只 → {removed}")
            receipt = ScreenRunReceipt(
                trade_date=date.fromisoformat(trade_date),
                preset_name=name,
                definition_version=_definition_version(preset),
                parent_trade_date=parent_trade_date,
                parent_result_version=parent_result_version,
                hit_count=len(sr_df),
                member_digest=member_set_digest(sr_df["ts_code"].tolist()),
                lineage_complete=lineage_complete,
                completed_at=datetime.now(UTC),
            )
            writing = True
            store.replace_screen_result_with_receipt(
                trade_date, name, sr_df, receipt,
                manage_transaction=False,
            )
            writing = False
            if transaction_started:
                store._conn.execute("COMMIT")
                transaction_started = False
            summary[name] = len(sr_df)
            logger.info(f"  {name}: {len(sr_df)} 命中")
        except Exception as exc:
            if transaction_started:
                store._conn.execute("ROLLBACK")
            if writing and transaction_open:
                raise
            summary[name] = -1
            errors.append(f"screen:{name}:{type(exc).__name__}")
            logger.exception(f"preset {name} 执行失败，跳过，继续后续 preset")
    return DailyScreenPipelineResult(preset_hits=summary, errors=tuple(sorted(errors)))


def _check_pool2_exits_without_notification(store: DuckDBStore, today: date) -> int:
    """Apply the established Pool 2 exit rules without bypassing the outbox."""
    from rquant.config import settings

    active = store.query_pool2_active()
    if active.empty:
        return 0
    kicked = 0
    for _, row in active.iterrows():
        code = row["ts_code"]
        close_row = store._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            [code, today],
        ).fetchone()
        if close_row is None:
            continue
        raw_entry = row["entry_date"]
        entry_date = raw_entry.date() if hasattr(raw_entry, "date") else raw_entry
        days = _count_trading_days_since(store, entry_date, today)
        close = float(close_row[0])
        if close < float(row["stop_strong"]):
            store.update_pool2_exit(code, today, "breakdown")
            kicked += 1
        elif days > settings.pool2_max_age_days:
            store.update_pool2_exit(code, today, "aged_out")
            kicked += 1
    return kicked


def run_daily_pool_stage(trade_date: str, *, store: DuckDBStore) -> DailyPoolPipelineResult:
    """Run only Pool 2 persistence and exit rules; it never sends a notification."""
    errors: list[str] = []
    try:
        added = _sync_pool2_watch(store, trade_date)
    except Exception as exc:
        logger.exception("daily Pool 2 sync failed")
        added = 0
        errors.append(f"pool:sync:{type(exc).__name__}")
    try:
        exited = _check_pool2_exits_without_notification(store, date.fromisoformat(trade_date))
    except Exception as exc:
        logger.exception("daily Pool 2 exit stage failed")
        exited = 0
        errors.append(f"pool:exits:{type(exc).__name__}")
    try:
        active = store.query_pool2_active()
    except Exception as exc:
        logger.exception("daily Pool 2 active-count query failed")
        active = pd.DataFrame()
        errors.append(f"pool:active_count:{type(exc).__name__}")
    return DailyPoolPipelineResult(
        pool2_added=added,
        pool2_exited=exited,
        pool2_active_count=len(active),
        errors=tuple(sorted(errors)),
    )


def _run_minute_context_backfill(
    store: DuckDBStore,
    trade_date: str,
    *,
    lookback_days: int = 90,
    freq: str = "1min",
) -> None:
    """日终筛选完成后回补当日 Pool1 的 90 日分钟上下文。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_pool1_minute_context

    summary = backfill_pool1_minute_context(
        store,
        TushareAdapter(),
        screen_date=trade_date,
        lookback_days=lookback_days,
        freq=freq,
        preset_name="n-shape-pool1",
    )
    logger.info(f"日终分钟上下文回补完成: {summary.model_dump()}")


def run_daily_pipeline(
    trade_date: str,
    preset_names: list[str] | None = None,
    store: DuckDBStore | None = None,
    minute_backfill: bool | None = None,
    minute_backfill_lookback_days: int = 90,
    minute_backfill_freq: str = "1min",
) -> dict[str, int]:
    """遍历预设筛选并落库，返回 {preset_name: 命中数}。

    前置条件：trade_date 的 daily_bar / daily_indicator / daily_state
    数据已通过 ingest_daily.py 入库。
    """
    import time as _time

    owns_store = store is None
    store = store or DuckDBStore()
    should_minute_backfill = owns_store if minute_backfill is None else minute_backfill
    started_at = _time.time()

    try:
        screen_result = run_daily_screen_stage(trade_date, preset_names=preset_names, store=store)
        summary = screen_result.preset_hits

        run_daily_pool_stage(trade_date, store=store)

        if should_minute_backfill:
            try:
                _run_minute_context_backfill(
                    store,
                    trade_date,
                    lookback_days=minute_backfill_lookback_days,
                    freq=minute_backfill_freq,
                )
            except Exception:
                logger.exception("_run_minute_context_backfill 失败")

        elapsed = _time.time() - started_at
        logger.info(f"流水线完成 {trade_date}: {summary} (耗时 {elapsed:.1f}s)")

        return summary
    finally:
        if owns_store:
            store.close()


def _count_trading_days_since(store: DuckDBStore, entry_date, today) -> int:
    """entry_date 到 today 之间有多少个交易日（含两端）。"""
    row = store._conn.execute(
        "SELECT COUNT(DISTINCT trade_date) FROM daily_bar "
        "WHERE trade_date >= ? AND trade_date <= ?",
        [entry_date, today],
    ).fetchone()
    return row[0] if row else 0
