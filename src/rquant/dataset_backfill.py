"""统一「按日数据集回补」层：Tushare 板块/资金流/龙虎榜/榜单等接口注册表。

12+ 个同构接口（按 trade_date 全市场取数）共用一条回补管线：
trade_cal 日历 → 单日故障隔离 → normalize（列名映射 + 类型归一）→ 幂等
upsert（限频退避在 adapter._call_with_backoff）。板块列表/成分/游资名录这类
低频快照走 snapshot 模式：一次拉全量 + 整表替换（成分调出即删）。

所有表本地回补权威、云端没有 → 全部注册进 research_sync.MERGE_TABLES，
否则日终合并会把回补历史整表抹掉（同 market_backfill 的教训）。

字段口径：各 normalize 函数注释里的字段清单为 2026-07-01 实测返回
（探测记录见 adapter/tushare.py 对应薄方法 docstring）。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Literal, Protocol

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict

from rquant.storage.duckdb import DuckDBStore

# Tushare 接口限流间隔（对齐 market_backfill._API_SLEEP）
_API_SLEEP = 0.35


class DatasetBackfillAdapter(Protocol):
    """fetch 闭包按数据集各自调 adapter 薄方法，Protocol 只锁公共的日历依赖。"""

    def trade_cal(self, start: date, end: date) -> list[date]:
        ...


class DatasetSpec(BaseModel):
    """一个可按日回补（或快照刷新）的 Tushare 数据集。"""

    model_config = ConfigDict(frozen=True)

    name: str                      # 注册名 = Tushare 接口名（CLI --dataset 用）
    table: str                     # 目标表（DDL 必须在 schema.ALL_DDL）
    mode: Literal["by_date", "snapshot"]
    fetch: Callable[[DatasetBackfillAdapter, date], pd.DataFrame]
    normalize: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    requests_per_day: int = 1      # 预计请求数口径（index_daily 逐指数 5 次/日）


def _parse_date(value: str | date) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


# ── normalize：列名映射 + 类型归一（实测字段见各函数注释） ──────────────────


def _norm_ths_index(df: pd.DataFrame) -> pd.DataFrame:
    """ths_index 实测字段：ts_code, name, count, exchange, list_date, type。
    count/type 是 SQL 关键字 → member_count/board_type；list_date 转 DATE。"""
    out = df.rename(columns={"count": "member_count", "type": "board_type"})
    out["member_count"] = pd.to_numeric(
        out["member_count"], errors="coerce"
    ).astype("Int64")
    out["list_date"] = pd.to_datetime(
        out["list_date"], format="%Y%m%d", errors="coerce"
    ).dt.date
    return out


def _norm_ths_member(df: pd.DataFrame) -> pd.DataFrame:
    """ths_member 实测字段：ts_code, con_code, con_name。
    源 ts_code 是板块代码 → board_code（与个股代码语义区分）。"""
    return df.rename(columns={"ts_code": "board_code"})


def _norm_dc_index(df: pd.DataFrame) -> pd.DataFrame:
    """dc_index 实测字段：ts_code, trade_date, name, leading, leading_code,
    pct_change, leading_pct, total_mv, turnover_rate, up_num, down_num,
    idx_type, level。leading 是 DuckDB 保留字 → leading_name。"""
    return df.rename(columns={"leading": "leading_name"})


def _norm_dc_member(df: pd.DataFrame) -> pd.DataFrame:
    """dc_member 实测字段：trade_date, ts_code, con_code, name。
    ts_code → board_code，name → con_name（对齐 ths_board_member）。"""
    return df.rename(columns={"ts_code": "board_code", "name": "con_name"})


def _norm_moneyflow(df: pd.DataFrame) -> pd.DataFrame:
    """moneyflow 实测 20 字段（sm/md/lg/elg 买卖 vol+amount + net_mf_*）。
    net_mf_vol/net_mf_amount → large_net_vol/large_net_amount（对齐
    moneyflow_daily 既有列名），补 source='tushare'（表主键含 source）。"""
    out = df.rename(
        columns={
            "net_mf_vol": "large_net_vol",
            "net_mf_amount": "large_net_amount",
        }
    )
    out["source"] = "tushare"
    return out


def _norm_moneyflow_ind_dc(df: pd.DataFrame) -> pd.DataFrame:
    """moneyflow_ind_dc 实测 18 字段。rank 是 SQL 关键字 → net_amount_rank。"""
    return df.rename(columns={"rank": "net_amount_rank"})


def _norm_top_list(df: pd.DataFrame) -> pd.DataFrame:
    """top_list 实测 15 字段。reason 进主键，NULL 填空串。"""
    out = df.copy()
    out["reason"] = out["reason"].fillna("")
    return out


def _norm_top_inst(df: pd.DataFrame) -> pd.DataFrame:
    """top_inst 实测 10 字段（exalter/side/reason 进主键，NULL 填空串）。"""
    out = df.copy()
    for col in ("exalter", "side", "reason"):
        out[col] = out[col].fillna("")
    return out


# kpl_list 实测里这些列为 object（含 None），落 DOUBLE 前强制数值化
_KPL_NUMERIC_COLS = (
    "bid_amount", "bid_change", "bid_turnover", "lu_bid_vol",
    "pct_chg", "bid_pct_chg", "rt_pct_chg",
)


def _norm_kpl_list(df: pd.DataFrame) -> pd.DataFrame:
    """kpl_list 实测 24 字段。tag 进主键 NULL 填空串；竞价类字段源里
    混 None/字符串，to_numeric 强转（转不动的置 NULL）。"""
    out = df.copy()
    out["tag"] = out["tag"].fillna("")
    for col in _KPL_NUMERIC_COLS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


# kpl_concept_cons 快照回看窗口：源按「题材当日活跃」增量打点（2026-07-03 实测
# 每天只写 3-8 个题材的全量成分，约 250-660 行/日），单日快照拼不出题材库，
# 全量历史又深不见底（offset=100000 仍有 2025-12 数据）→ 拉窗口后每题材只留
# 最近一次打点。30 天实测 ≈ 25 个题材 / 4.5 万行 / 15 页
_KPL_CONS_WINDOW_DAYS = 30
_KPL_CONS_PAGE_SIZE = 3000   # 实测单页上限 3000（limit=10000 也只回 3000）
_KPL_CONS_FIELDS = "ts_code,name,con_name,con_code,trade_date,desc,hot_num"


def _fetch_kpl_concept_cons_day(
    adapter: DatasetBackfillAdapter, day: date
) -> pd.DataFrame:
    """开盘啦题材成分：单交易日分页拉取（快照窗口与日度回补共用）。

    该接口 offset > 100,000 直接报「查询数据失败，请确认参数」（2026-07-03
    实测撞墙），故按交易日逐日拉（每天约 3,400 行、2 页封顶），永远到不了深
    offset。非交易日源空返回无害。
    """
    return adapter._dataset_paged(  # type: ignore[attr-defined]
        "kpl_concept_cons",
        fields=_KPL_CONS_FIELDS,
        page_size=_KPL_CONS_PAGE_SIZE,
        trade_date=day.strftime("%Y%m%d"),
    )


def _fetch_kpl_concept_cons(
    adapter: DatasetBackfillAdapter, as_of: date
) -> pd.DataFrame:
    """题材成分快照：拉 30 天窗口后每题材留最新打点（见 _norm_kpl_concept）。"""
    frames: list[pd.DataFrame] = []
    for offset_days in range(_KPL_CONS_WINDOW_DAYS, -1, -1):
        day = as_of - timedelta(days=offset_days)
        if day.weekday() >= 5:
            continue
        df = _fetch_kpl_concept_cons_day(adapter, day)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _norm_kpl_concept(df: pd.DataFrame) -> pd.DataFrame:
    """kpl_concept_cons 实测字段：ts_code(题材代码), name, con_name, con_code,
    trade_date, desc, hot_num。ts_code → board_code、name → board_name（对齐
    成分表惯例）、desc 是 SQL 关键字 → description（沿用 hm_list 惯例）。

    热门题材每天重复打点 → 每题材只留最近一次打点日的全量成分（旧打点里
    已调出的成分不残留），PK (board_code, con_code) 落库。
    """
    out = df.rename(
        columns={
            "ts_code": "board_code",
            "name": "board_name",
            "desc": "description",
        }
    )
    out["hot_num"] = pd.to_numeric(out["hot_num"], errors="coerce")
    latest = out.groupby("board_code")["trade_date"].transform("max")
    return out[out["trade_date"] == latest].reset_index(drop=True)


def _norm_kpl_concept_daily(df: pd.DataFrame) -> pd.DataFrame:
    """kpl_concept 日度：列映射同快照表，但**不做每题材留最新折叠**——日度
    表按 (trade_date, board_code, con_code) 存每天的打点，回测才能按 ≤T 最近
    一次打点还原信号日的题材成分。desc/description 不落日度表（列交集自动丢弃）。
    """
    out = df.rename(columns={"ts_code": "board_code", "name": "board_name"})
    out["hot_num"] = pd.to_numeric(out["hot_num"], errors="coerce")
    return out


def _norm_daily_info(df: pd.DataFrame) -> pd.DataFrame:
    """daily_info 实测 14 字段。trans_count 源里为 object（含 None）→ 数值化。"""
    out = df.copy()
    out["trans_count"] = pd.to_numeric(out["trans_count"], errors="coerce")
    return out


def _norm_hm_list(df: pd.DataFrame) -> pd.DataFrame:
    """hm_list 实测字段：name, desc, orgs。desc 是 SQL 关键字 → description。"""
    return df.rename(columns={"desc": "description"})


# ── 注册表 ──────────────────────────────────────────────────────────────────

DATASETS: dict[str, DatasetSpec] = {
    spec.name: spec
    for spec in [
        # 板块日行情
        DatasetSpec(
            name="ths_daily", table="ths_index_daily", mode="by_date",
            fetch=lambda a, d: a.ths_daily_by_date(d),
        ),
        DatasetSpec(
            name="dc_daily", table="dc_index_daily", mode="by_date",
            fetch=lambda a, d: a.dc_daily_by_date(d),
        ),
        DatasetSpec(
            name="adj_factor", table="adj_factor", mode="by_date",
            fetch=lambda a, d: a.adj_factor_by_date(d),
        ),
        # 板块静态 / 成分（低频快照）
        DatasetSpec(
            name="ths_index", table="ths_board", mode="snapshot",
            fetch=lambda a, d: a.ths_index_snapshot(),
            normalize=_norm_ths_index,
        ),
        DatasetSpec(
            name="ths_member", table="ths_board_member", mode="snapshot",
            fetch=lambda a, d: a.ths_member_snapshot(),
            normalize=_norm_ths_member,
        ),
        DatasetSpec(
            name="dc_index", table="dc_board", mode="snapshot",
            fetch=lambda a, d: a.dc_index_snapshot(d),
            normalize=_norm_dc_index,
        ),
        DatasetSpec(
            name="dc_member", table="dc_board_member", mode="snapshot",
            fetch=lambda a, d: a.dc_member_snapshot(d),
            normalize=_norm_dc_member,
        ),
        # 资金流
        DatasetSpec(
            name="moneyflow", table="moneyflow_daily", mode="by_date",
            fetch=lambda a, d: a.moneyflow_full_by_date(d),
            normalize=_norm_moneyflow,
        ),
        DatasetSpec(
            name="moneyflow_dc", table="moneyflow_dc_daily", mode="by_date",
            fetch=lambda a, d: a.moneyflow_dc_by_date(d),
        ),
        DatasetSpec(
            name="moneyflow_ths", table="moneyflow_ths_daily", mode="by_date",
            fetch=lambda a, d: a.moneyflow_ths_by_date(d),
        ),
        DatasetSpec(
            name="moneyflow_ind_ths", table="moneyflow_ind_ths_daily",
            mode="by_date",
            fetch=lambda a, d: a.moneyflow_ind_ths_by_date(d),
        ),
        DatasetSpec(
            name="moneyflow_ind_dc", table="moneyflow_ind_dc_daily",
            mode="by_date",
            fetch=lambda a, d: a.moneyflow_ind_dc_by_date(d),
            normalize=_norm_moneyflow_ind_dc,
        ),
        DatasetSpec(
            name="moneyflow_cnt_ths", table="moneyflow_cnt_ths_daily",
            mode="by_date",
            fetch=lambda a, d: a.moneyflow_cnt_ths_by_date(d),
        ),
        DatasetSpec(
            name="moneyflow_mkt_dc", table="moneyflow_mkt_daily",
            mode="by_date",
            fetch=lambda a, d: a.moneyflow_mkt_dc_by_date(d),
        ),
        # 龙虎榜
        DatasetSpec(
            name="top_list", table="top_list_daily", mode="by_date",
            fetch=lambda a, d: a.top_list_by_date(d),
            normalize=_norm_top_list,
        ),
        DatasetSpec(
            name="top_inst", table="top_inst_daily", mode="by_date",
            fetch=lambda a, d: a.top_inst_by_date(d),
            normalize=_norm_top_inst,
        ),
        # 其他
        DatasetSpec(
            name="kpl_list", table="kpl_list_daily", mode="by_date",
            fetch=lambda a, d: a.kpl_list_by_date(d),
            normalize=_norm_kpl_list,
        ),
        DatasetSpec(
            name="kpl_concept", table="kpl_concept_member", mode="snapshot",
            fetch=_fetch_kpl_concept_cons,
            normalize=_norm_kpl_concept,
        ),
        # 日度题材成分（逐日落库，不折叠）：回测按 ≤T 最近打点还原当日成分
        DatasetSpec(
            name="kpl_concept_daily", table="kpl_concept_member_daily",
            mode="by_date",
            fetch=_fetch_kpl_concept_cons_day,
            normalize=_norm_kpl_concept_daily,
        ),
        DatasetSpec(
            name="daily_info", table="market_daily_info", mode="by_date",
            fetch=lambda a, d: a.daily_info_by_date(d),
            normalize=_norm_daily_info,
        ),
        DatasetSpec(
            name="hm_list", table="hm_list", mode="snapshot",
            fetch=lambda a, d: a.hm_list_snapshot(),
            normalize=_norm_hm_list,
        ),
        DatasetSpec(
            name="index_daily", table="index_daily_bar", mode="by_date",
            fetch=lambda a, d: a.index_daily_major_by_date(d),
            requests_per_day=5,   # 实测不支持多代码合并，逐指数各打 1 次
        ),
    ]
}


# ── 通用回补 ────────────────────────────────────────────────────────────────


def backfill_dataset(
    name: str,
    start_date: str | date,
    end_date: str | date,
    store: DuckDBStore,
    adapter: DatasetBackfillAdapter | None = None,
    *,
    dry_run: bool = False,
    api_sleep: float = _API_SLEEP,
) -> dict:
    """按注册表回补一个数据集。

    by_date：trade_cal 日历循环，单日失败 warning + 记入 failed_dates 后继续
    （故障隔离，upsert 幂等，事后可对 failed_dates 重跑）。snapshot：解析到
    end_date 往前最近交易日，一次拉全量整表替换；失败同样落 failed_dates。
    dry_run 只报告计划（by_date 会打 1 次 trade_cal），不调数据接口、不写库。
    """
    spec = DATASETS.get(name)
    if spec is None:
        raise ValueError(
            f"未知数据集：{name}（可用：{', '.join(sorted(DATASETS))}）"
        )
    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)
    if start_d > end_d:
        raise ValueError("start_date must be <= end_date")

    if adapter is None:
        from rquant.adapter.tushare import TushareAdapter

        adapter = TushareAdapter()

    if spec.mode == "snapshot":
        return _run_snapshot(spec, end_d, store, adapter, dry_run=dry_run)
    return _run_by_date(
        spec, start_d, end_d, store, adapter, dry_run=dry_run, api_sleep=api_sleep
    )


def _base_summary(spec: DatasetSpec, start_d: date, end_d: date, dry_run: bool) -> dict:
    return {
        "dataset": spec.name,
        "table": spec.table,
        "mode": spec.mode,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "trading_dates_count": 0,
        "planned_requests": 0,
        "executed_dates": 0,
        "rows": 0,
        "failed_dates": [],
        "dry_run": dry_run,
    }


def _run_by_date(
    spec: DatasetSpec,
    start_d: date,
    end_d: date,
    store: DuckDBStore,
    adapter: DatasetBackfillAdapter,
    *,
    dry_run: bool,
    api_sleep: float,
) -> dict:
    dates = adapter.trade_cal(start_d, end_d)
    summary = _base_summary(spec, start_d, end_d, dry_run)
    summary["trading_dates_count"] = len(dates)
    summary["planned_requests"] = len(dates) * spec.requests_per_day
    logger.info(
        f"数据集回补计划 [{spec.name} → {spec.table}]: {start_d} ~ {end_d}, "
        f"交易日 {len(dates)} 个, 预计请求 {summary['planned_requests']} 次, "
        f"dry_run={dry_run}"
    )
    if dry_run:
        return summary

    for i, trading_date in enumerate(dates):
        summary["executed_dates"] += 1
        try:
            df = spec.fetch(adapter, trading_date)
            time.sleep(api_sleep)
            if spec.normalize is not None and not df.empty:
                df = spec.normalize(df)
            rows = store.upsert_dataset(spec.table, df)
        except Exception as e:
            summary["failed_dates"].append(trading_date.isoformat())
            logger.warning(
                f"数据集 {spec.name} 单日失败，跳过: date={trading_date} err={e}"
            )
            continue
        summary["rows"] += rows
        logger.info(
            f"{trading_date} {spec.name} 回补完成 "
            f"({i + 1}/{len(dates)}): rows={rows}"
        )

    logger.info(
        f"数据集回补结束 [{spec.name}]: rows={summary['rows']:,} "
        f"failed={len(summary['failed_dates'])}"
    )
    return summary


def _latest_trading_day(adapter: DatasetBackfillAdapter, end_d: date) -> date:
    """快照类（dc_index/dc_member）需要交易日参数：end_d 往前找最近交易日。

    14 天窗口覆盖春节最长连休；日历失败/为空退回 end_d（fetch 空返回时
    snapshot 会记失败，不会污染现有快照）。
    """
    try:
        dates = adapter.trade_cal(end_d - timedelta(days=14), end_d)
    except Exception as e:
        logger.warning(f"trade_cal 失败，快照参考日回退 {end_d}: {e}")
        return end_d
    return dates[-1] if dates else end_d


def _run_snapshot(
    spec: DatasetSpec,
    end_d: date,
    store: DuckDBStore,
    adapter: DatasetBackfillAdapter,
    *,
    dry_run: bool,
) -> dict:
    as_of = _latest_trading_day(adapter, end_d)
    summary = _base_summary(spec, as_of, end_d, dry_run)
    summary["trading_dates_count"] = 1
    summary["planned_requests"] = 1
    logger.info(
        f"数据集快照计划 [{spec.name} → {spec.table}]: as_of={as_of}, "
        f"dry_run={dry_run}"
    )
    if dry_run:
        return summary

    summary["executed_dates"] = 1
    try:
        df = spec.fetch(adapter, as_of)
        if spec.normalize is not None and not df.empty:
            df = spec.normalize(df)
        if df.empty:
            raise RuntimeError("快照返回空，保留现有数据不替换")
        summary["rows"] = store.replace_dataset(spec.table, df)
    except Exception as e:
        summary["failed_dates"].append(as_of.isoformat())
        logger.warning(f"数据集快照失败: {spec.name} as_of={as_of} err={e}")

    logger.info(
        f"数据集快照结束 [{spec.name}]: rows={summary['rows']:,} "
        f"failed={len(summary['failed_dates'])}"
    )
    return summary
