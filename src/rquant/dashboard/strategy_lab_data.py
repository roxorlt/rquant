"""策略实验室数据辅助函数。"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, time, timedelta
from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from pydantic import BaseModel, Field

from rquant.config import settings
from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
from rquant.strategy_replay_metrics import (
    auction_gap_metric_rows as auction_gap_metric_rows,
)
from rquant.strategy_replay_metrics import (
    growth_board_metric_rows as growth_board_metric_rows,
)

TUSHARE_STAGE_LABELS: dict[str, str] = {
    "stage_0_integrated": "0 已接入",
    "stage_1_realtime": "1 盘中/竞价",
    "stage_2_strategy_features": "2 策略特征",
    "stage_3_context_filters": "3 环境/过滤",
    "stage_4_reference": "4 参考观察",
}

TUSHARE_PERMISSION_LABELS: dict[str, str] = {
    "available": "已可用",
    "official_permission": "正式权限",
    "points_required": "积分要求",
    "unknown_or_included": "未知/可能已含",
}

TUSHARE_STATUS_LABELS: dict[str, str] = {
    "already_integrated": "已接入",
    "recommended": "建议接入",
    "candidate": "候选",
    "watchlist": "观察",
}

_POINTS_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*积分")
SHANGHAI = ZoneInfo("Asia/Shanghai")


class StrategyOptimizationWorkloadEstimate(BaseModel):
    """Strategy Lab 自动优化的开跑前工作量估算。"""

    candidate_count: int = Field(ge=0)
    entry_mode_count: int = Field(ge=0)
    profile_variant_count: int = Field(ge=0)
    hold_count: int = Field(ge=0)
    topn_count: int = Field(ge=0)
    score_profile_count: int = Field(ge=0)
    walk_forward_folds: int = Field(ge=0)
    replay_runs: int = Field(ge=0)
    replay_candidate_passes: int = Field(ge=0)
    topn_combinations: int = Field(ge=0)
    walk_forward_topn_combinations: int = Field(ge=0)
    estimated_seconds: float = Field(ge=0.0)
    estimated_label: str


class GrowthBoardWorkloadEstimate(BaseModel):
    """科创/创业放量 replay 的开跑前工作量估算。"""

    candidate_count: int = Field(ge=0)
    variant_count: int = Field(ge=0)
    candidate_passes: int = Field(ge=0)
    estimated_seconds: float = Field(ge=0.0)
    estimated_label: str


class GrowthBoardAblationSpec(BaseModel):
    """科创/创业放量策略的一个消融版本。"""

    key: str
    label: str
    description: str
    require_vwap_strength: bool = True
    use_same_minute_surge: bool = True
    use_accel_surge: bool = True


class TushareMetadataState(StrEnum):
    MISSING = "missing"
    CORRUPT = "corrupt"
    EMPTY = "empty"
    READY = "ready"


@dataclass(frozen=True)
class TushareMetadataResult:
    state: TushareMetadataState
    detail: str
    frame: pd.DataFrame


def query_growth_board_candidates(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_date: date,
    end_date: date,
    min_signal_time: time = time(9, 30),
) -> pd.DataFrame:
    """批量读取 Lab 工作量分母，并按盘中决策时点校验历史证券状态。"""
    raw = conn.execute(
        """
        SELECT daily.ts_code,
               daily.trade_date,
               status.name,
               status.is_st,
               status.available_at,
               status.conflict_reason
        FROM daily_bar AS daily
        LEFT JOIN stock_status_daily AS status
          ON daily.ts_code = status.ts_code
         AND daily.trade_date = status.trade_date
        WHERE daily.trade_date >= ?
          AND daily.trade_date <= ?
          AND (
              daily.ts_code LIKE '300%.SZ'
              OR daily.ts_code LIKE '301%.SZ'
              OR daily.ts_code LIKE '688%.SH'
          )
          AND daily.pre_close > 0
        ORDER BY daily.trade_date, daily.ts_code
        """,
        [start_date, end_date],
    ).fetchdf()
    if raw.empty:
        return raw[["ts_code", "trade_date", "name"]]

    signal_dates = pd.to_datetime(raw["trade_date"])
    decision_at = signal_dates.dt.tz_localize(SHANGHAI) + timedelta(
        hours=min_signal_time.hour,
        minutes=min_signal_time.minute,
        seconds=min_signal_time.second,
        microseconds=min_signal_time.microsecond,
    )
    available_at = pd.to_datetime(raw["available_at"], utc=True)
    known_non_st = (
        raw["conflict_reason"].isna()
        & raw["is_st"].notna()
        & raw["is_st"].eq(False)
        & available_at.notna()
        & available_at.le(decision_at)
    ).fillna(False)
    candidates = raw.loc[known_non_st, ["ts_code", "trade_date", "name"]].copy()
    candidates["name"] = (
        candidates["name"]
        .astype("string")
        .str.strip()
        .fillna(candidates["ts_code"].astype("string"))
    )
    return candidates.reset_index(drop=True)


def _duration_label(seconds: float) -> str:
    if seconds < 60:
        return f"约 {int(round(seconds))} 秒"
    minutes = seconds / 60
    if minutes < 10:
        return f"约 {minutes:.1f} 分钟"
    if minutes < 60:
        return f"约 {int(round(minutes))} 分钟"
    return f"约 {minutes / 60:.1f} 小时"


def estimate_strategy_optimization_workload(
    *,
    candidate_count: int,
    entry_mode_count: int,
    profile_variant_count: int,
    hold_count: int,
    topn_count: int,
    score_profile_count: int,
    walk_forward_folds: int,
    seconds_per_candidate_pass: float = 0.012,
    seconds_per_replay_run: float = 0.25,
    seconds_per_topn_combination: float = 0.003,
) -> StrategyOptimizationWorkloadEstimate:
    """按当前自动优化器复杂度估算运行成本。"""
    counts = [
        candidate_count,
        entry_mode_count,
        profile_variant_count,
        hold_count,
        topn_count,
        score_profile_count,
        walk_forward_folds,
    ]
    if any(value < 0 for value in counts):
        msg = "工作量估算参数不能为负数"
        raise ValueError(msg)
    timings = [
        seconds_per_candidate_pass,
        seconds_per_replay_run,
        seconds_per_topn_combination,
    ]
    if any(value < 0 for value in timings):
        msg = "工作量估算耗时参数不能为负数"
        raise ValueError(msg)

    strategy_count = entry_mode_count * profile_variant_count
    has_walk_forward = walk_forward_folds > 0
    replay_runs = hold_count * strategy_count * (2 + int(has_walk_forward))
    replay_candidate_passes = (
        candidate_count
        * hold_count
        * strategy_count
        * (1 + int(has_walk_forward))
    )
    topn_combinations = (
        hold_count
        * strategy_count
        * 2
        * topn_count
        * score_profile_count
    )
    walk_forward_topn_combinations = (
        hold_count
        * walk_forward_folds
        * strategy_count
        * 2
        * topn_count
        * score_profile_count
    )
    estimated_seconds = round(
        replay_candidate_passes * seconds_per_candidate_pass
        + replay_runs * seconds_per_replay_run
        + (topn_combinations + walk_forward_topn_combinations)
        * seconds_per_topn_combination,
        3,
    )
    return StrategyOptimizationWorkloadEstimate(
        candidate_count=candidate_count,
        entry_mode_count=entry_mode_count,
        profile_variant_count=profile_variant_count,
        hold_count=hold_count,
        topn_count=topn_count,
        score_profile_count=score_profile_count,
        walk_forward_folds=walk_forward_folds,
        replay_runs=replay_runs,
        replay_candidate_passes=replay_candidate_passes,
        topn_combinations=topn_combinations,
        walk_forward_topn_combinations=walk_forward_topn_combinations,
        estimated_seconds=estimated_seconds,
        estimated_label=_duration_label(estimated_seconds),
    )


def estimate_growth_board_workload(
    *,
    candidate_count: int,
    variant_count: int,
    seconds_per_candidate_pass: float = 0.018,
    seconds_per_variant: float = 0.5,
) -> GrowthBoardWorkloadEstimate:
    """按候选数量和消融版本数估算科创/创业放量 replay 耗时。"""
    if candidate_count < 0 or variant_count < 0:
        msg = "候选数和版本数不能为负数"
        raise ValueError(msg)
    if seconds_per_candidate_pass < 0 or seconds_per_variant < 0:
        msg = "耗时参数不能为负数"
        raise ValueError(msg)
    candidate_passes = candidate_count * variant_count
    estimated_seconds = round(
        candidate_passes * seconds_per_candidate_pass
        + variant_count * seconds_per_variant,
        3,
    )
    return GrowthBoardWorkloadEstimate(
        candidate_count=candidate_count,
        variant_count=variant_count,
        candidate_passes=candidate_passes,
        estimated_seconds=estimated_seconds,
        estimated_label=_duration_label(estimated_seconds),
    )


def growth_board_ablation_specs() -> list[GrowthBoardAblationSpec]:
    """返回默认消融版本，顺序固定用于页面对比。"""
    return [
        GrowthBoardAblationSpec(
            key="full",
            label="完整策略",
            description="累计放量 + 同刻放量或5分加速 + VWAP强度",
        ),
        GrowthBoardAblationSpec(
            key="no_vwap",
            label="去掉VWAP",
            description="不要求信号价强于当日VWAP",
            require_vwap_strength=False,
        ),
        GrowthBoardAblationSpec(
            key="no_same_minute",
            label="去掉同刻放量",
            description="只保留累计放量与5分加速",
            use_same_minute_surge=False,
        ),
        GrowthBoardAblationSpec(
            key="no_accel_5m",
            label="去掉5分加速",
            description="只保留累计放量与同刻放量",
            use_accel_surge=False,
        ),
        GrowthBoardAblationSpec(
            key="cum_only",
            label="只看累计放量",
            description="同刻放量、5分加速、VWAP都不参与过滤",
            require_vwap_strength=False,
            use_same_minute_surge=False,
            use_accel_surge=False,
        ),
    ]


def safe_replay_end_date(
    trading_calendar: Sequence[date],
    pool_max_date: date,
    *,
    max_hold_days: int,
) -> date | None:
    """返回具备完整 B 日买入 + T+N 卖出窗口的最晚 Pool 日期。"""
    if max_hold_days < 1:
        msg = "max_hold_days 必须 >= 1"
        raise ValueError(msg)
    required_tail = max_hold_days + 2
    if len(trading_calendar) < required_tail:
        return None
    calendar_safe_end = trading_calendar[-required_tail]
    return min(pool_max_date, calendar_safe_end)


def _pct_mean(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return round(float(clean.mean()), 4)


def _pct_median(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return round(float(clean.median()), 4)


def _win_rate(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return round(float((clean > 0).mean() * 100), 4)


def _bool_rate(series: pd.Series) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    return round(float(clean.astype(bool).mean() * 100), 4)


def auction_gap_metric_rows(
    baseline: pd.DataFrame,
    minute_trades: pd.DataFrame,
) -> pd.DataFrame:
    """汇总集合竞价基准与分钟 B/S replay，用于 Strategy Lab 对比。"""
    candidates_count = len(baseline)
    baseline_ret = (
        baseline["next_open_ret_pct"]
        if "next_open_ret_pct" in baseline.columns
        else pd.Series(dtype=float)
    )
    minute_ret = (
        minute_trades["ret_pct"]
        if "ret_pct" in minute_trades.columns
        else pd.Series(dtype=float)
    )
    baseline_hit = (
        baseline["hit_limit_up_today"]
        if "hit_limit_up_today" in baseline.columns
        else pd.Series(dtype=bool)
    )
    baseline_high_ret = (
        baseline["intraday_high_ret_pct"]
        if "intraday_high_ret_pct" in baseline.columns
        else pd.Series(dtype=float)
    )
    baseline_close_ret = (
        baseline["day_close_ret_pct"]
        if "day_close_ret_pct" in baseline.columns
        else pd.Series(dtype=float)
    )
    minute_hit = (
        minute_trades["b_hit_limit_up_today"]
        if "b_hit_limit_up_today" in minute_trades.columns
        else pd.Series(dtype=bool)
    )
    weak_exit_rate = None
    if not minute_trades.empty and "exit_reason" in minute_trades.columns:
        weak_exit_rate = round(
            float(minute_trades["exit_reason"].fillna("").eq("next_auction_weak").mean() * 100),
            4,
        )
    return pd.DataFrame([
        {
            "策略": "竞价直接B/次日开盘S",
            "候选": candidates_count,
            "交易": candidates_count,
            "触发率%": 100.0 if candidates_count else None,
            "当日上板率%": _bool_rate(baseline_hit),
            "当日最高均值%": _pct_mean(baseline_high_ret),
            "当日收盘均值%": _pct_mean(baseline_close_ret),
            "平均收益%": _pct_mean(baseline_ret),
            "中位收益%": _pct_median(baseline_ret),
            "胜率%": _win_rate(baseline_ret),
            "弱竞价退出%": None,
        },
        {
            "策略": "竞价候选/分钟B/S",
            "候选": candidates_count,
            "交易": len(minute_trades),
            "触发率%": round(len(minute_trades) / candidates_count * 100, 4)
            if candidates_count
            else None,
            "当日上板率%": _bool_rate(minute_hit),
            "当日最高均值%": None,
            "当日收盘均值%": None,
            "平均收益%": _pct_mean(minute_ret),
            "中位收益%": _pct_median(minute_ret),
            "胜率%": _win_rate(minute_ret),
            "弱竞价退出%": weak_exit_rate,
        },
    ])


def growth_board_metric_rows(
    trades: pd.DataFrame,
    *,
    strategy_name: str = "科创/创业放量追击",
) -> pd.DataFrame:
    """汇总科创/创业板分钟放量 replay，用于 Strategy Lab 对比。"""
    ret = trades["ret_pct"] if "ret_pct" in trades.columns else pd.Series(dtype=float)
    hit = (
        trades["hit_limit_up_today"]
        if "hit_limit_up_today" in trades.columns
        else pd.Series(dtype=bool)
    )
    return pd.DataFrame([
        {
            "策略": strategy_name,
            "交易": len(trades),
            "当日上板率%": _bool_rate(hit),
            "平均收益%": _pct_mean(ret),
            "中位收益%": _pct_median(ret),
            "胜率%": _win_rate(ret),
        }
    ])


def dataframe_preview(
    df: pd.DataFrame,
    *,
    max_rows: int = 500,
) -> tuple[pd.DataFrame, bool]:
    """返回适合浏览器渲染的 DataFrame 预览和是否截断。"""
    if max_rows < 1:
        msg = "max_rows 必须 >= 1"
        raise ValueError(msg)
    truncated = len(df) > max_rows
    if not truncated:
        return df.copy(), False
    return df.head(max_rows).copy(), True


def _decode_json_list(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [value]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [str(parsed)]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def _extract_required_points(*texts: object) -> float | None:
    values: list[float] = []
    for text in texts:
        if _is_missing(text):
            continue
        values.extend(float(match) for match in _POINTS_PATTERN.findall(str(text)))
    return max(values) if values else None


def _point_unit_price(goods: pd.DataFrame) -> float | None:
    if goods.empty:
        return None
    candidates = goods[
        (goods["good_type"] == 1)
        & (
            goods["name"].astype(str).str.contains("自定义", na=False)
            | (goods["good_id"] == 7)
        )
    ]
    if candidates.empty or pd.isna(candidates.iloc[0]["price"]):
        return None
    return float(candidates.iloc[0]["price"])


def _row_dict(row: pd.Series) -> dict[str, object]:
    return {key: row[key] for key in row.index}


def _enrich_catalog_with_purchase_data(
    catalog: pd.DataFrame,
    goods: pd.DataFrame,
    *,
    current_points: float | None,
) -> pd.DataFrame:
    out = catalog.copy()
    for column in [
        "permission_good_id",
        "permission_good_name",
        "permission_price",
        "permission_duration_unit",
        "permission_api_count",
        "permission_api_names",
        "required_points",
        "current_points",
        "points_gap",
        "estimated_points_cost",
    ]:
        out[column] = None

    permission_goods = goods[goods["good_type"] == 2].copy() if not goods.empty else goods
    by_api: dict[str, dict[str, object]] = {}
    by_doc_id: dict[int, dict[str, object]] = {}
    for _, good in permission_goods.iterrows():
        good_dict = _row_dict(good)
        for api_name in _decode_json_list(good.get("api_names")):
            by_api.setdefault(api_name, good_dict)
        doc_ids = [
            int(value)
            for value in _decode_json_list(good.get("api_doc_ids"))
            if str(value).isdigit()
        ]
        if not _is_missing(good.get("doc_id")):
            doc_ids.append(int(good["doc_id"]))
        for doc_id in doc_ids:
            by_doc_id.setdefault(doc_id, good_dict)

    unit_price = _point_unit_price(goods)
    for index, row in out.iterrows():
        matched = None
        api_name = row.get("api_name")
        if not _is_missing(api_name):
            matched = by_api.get(str(api_name))
        if matched is None and not _is_missing(row.get("doc_id")):
            matched = by_doc_id.get(int(row["doc_id"]))
        if matched:
            out.at[index, "permission_good_id"] = matched.get("good_id")
            out.at[index, "permission_good_name"] = matched.get("name")
            out.at[index, "permission_price"] = matched.get("price")
            out.at[index, "permission_duration_unit"] = matched.get("duration_unit")
            out.at[index, "permission_api_count"] = matched.get("api_count")
            out.at[index, "permission_api_names"] = matched.get("api_names")

        required_points = _extract_required_points(
            row.get("limit_note"),
            row.get("permission_note"),
        )
        out.at[index, "required_points"] = required_points
        out.at[index, "current_points"] = current_points
        if required_points is not None and current_points is not None:
            gap = max(required_points - current_points, 0)
            out.at[index, "points_gap"] = gap
            out.at[index, "estimated_points_cost"] = (
                gap * unit_price if unit_price is not None and gap > 0 else 0.0
            )
    return out


def _latest_account_points(conn: duckdb.DuckDBPyConnection) -> float | None:
    if not _table_exists(conn, "tushare_account_score"):
        return None
    row = conn.execute(
        """
        SELECT scores
        FROM tushare_account_score
        ORDER BY fetched_at DESC NULLS LAST
        LIMIT 1
        """
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def load_tushare_purchase_goods(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    with _open_tushare_metadata_catalog(db_path) as catalog:
        conn = catalog.connection
        if not _table_exists(conn, "tushare_purchase_goods"):
            return pd.DataFrame()
        return conn.execute(
            """
            SELECT good_type, good_id, name, description, price, duration_unit,
                   doc_url, doc_id, api_count, api_names, api_doc_ids, fetched_at
            FROM tushare_purchase_goods
            ORDER BY good_type, good_id
            """
        ).fetchdf()


def load_tushare_activity_packages(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    with _open_tushare_metadata_catalog(db_path) as catalog:
        conn = catalog.connection
        if not _table_exists(conn, "tushare_activity_packages"):
            return pd.DataFrame()
        return conn.execute(
            """
            SELECT package_id, name, description, price, duration_unit, fetched_at
            FROM tushare_activity_packages
            ORDER BY package_id
            """
        ).fetchdf()


def load_tushare_interface_catalog(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    with _open_tushare_metadata_catalog(db_path) as metadata:
        conn = metadata.connection
        if not _table_exists(conn, "tushare_interface_catalog"):
            return pd.DataFrame()
        catalog_frame = conn.execute(
            """
            SELECT doc_id, title, api_name, priority, integration_status,
                   integration_stage, update_cadence, target_table_hint,
                   permission_level, strategy_value, category_path,
                   capability_tags, limit_note, permission_note,
                   history_coverage_type, history_start, history_coverage_note,
                   doc_url
            FROM tushare_interface_catalog
            ORDER BY integration_stage, priority, doc_id
            """
        ).fetchdf()
        if catalog_frame.empty:
            return catalog_frame
        goods = (
            conn.execute(
                """
                SELECT good_type, good_id, name, price, duration_unit, doc_id,
                       api_count, api_names, api_doc_ids
                FROM tushare_purchase_goods
                """
            ).fetchdf()
            if _table_exists(conn, "tushare_purchase_goods")
            else pd.DataFrame()
        )
        return _enrich_catalog_with_purchase_data(
            catalog_frame,
            goods,
            current_points=_latest_account_points(conn),
        )


def _load_tushare_metadata_state(
    db_path: Path,
    loader: Callable[[Path], pd.DataFrame],
    *,
    label: str,
) -> TushareMetadataResult:
    if not db_path.exists():
        return TushareMetadataResult(
            state=TushareMetadataState.MISSING,
            detail=f"{label}元数据目录不存在",
            frame=pd.DataFrame(),
        )
    try:
        frame = loader(db_path)
    except (duckdb.Error, OSError, RuntimeError, ValueError) as exc:
        return TushareMetadataResult(
            state=TushareMetadataState.CORRUPT,
            detail=f"{label}元数据损坏或不可验证：{type(exc).__name__}: {exc}",
            frame=pd.DataFrame(),
        )
    if frame.empty:
        return TushareMetadataResult(
            state=TushareMetadataState.EMPTY,
            detail=f"{label}元数据可读，但当前没有记录",
            frame=frame,
        )
    return TushareMetadataResult(
        state=TushareMetadataState.READY,
        detail=f"{label}元数据可用，共 {len(frame)} 条",
        frame=frame,
    )


def load_tushare_interface_catalog_state(db_path: Path) -> TushareMetadataResult:
    return _load_tushare_metadata_state(
        db_path,
        load_tushare_interface_catalog,
        label="Tushare 接口",
    )


def load_tushare_purchase_goods_state(db_path: Path) -> TushareMetadataResult:
    return _load_tushare_metadata_state(
        db_path,
        load_tushare_purchase_goods,
        label="Tushare 权限商品",
    )


def load_tushare_activity_packages_state(db_path: Path) -> TushareMetadataResult:
    return _load_tushare_metadata_state(
        db_path,
        load_tushare_activity_packages,
        label="Tushare 套餐",
    )


def _open_tushare_metadata_catalog(db_path: Path) -> ImmutableDuckDBMetadataCatalog:
    forbidden = tuple(
        path
        for path in (
            settings.duckdb_path,
            settings.duckdb_readonly_path,
            settings.research_db_path_resolved,
            settings.research_readonly_db_path_resolved,
        )
        if path is not None
    )
    return ImmutableDuckDBMetadataCatalog.open(
        db_path.resolve(strict=False),
        forbidden_paths=forbidden,
    )


def _format_price(value: object) -> str:
    if _is_missing(value):
        return ""
    price = float(value)
    return f"¥{price:,.0f}" if price.is_integer() else f"¥{price:,.2f}"


def _format_quote(row: pd.Series) -> str:
    name = row.get("permission_good_name")
    price = row.get("permission_price")
    if _is_missing(name) or _is_missing(price):
        return ""
    duration = row.get("permission_duration_unit")
    count = row.get("permission_api_count")
    suffix = f"/{duration}" if not _is_missing(duration) else ""
    count_text = f"（{int(count)}个接口）" if not _is_missing(count) else ""
    return f"{name} {_format_price(price)}{suffix}{count_text}"


def _format_points_gap(row: pd.Series) -> str:
    required = row.get("required_points")
    current = row.get("current_points")
    gap = row.get("points_gap")
    cost = row.get("estimated_points_cost")
    if _is_missing(required) or _is_missing(current) or _is_missing(gap):
        return ""
    if float(gap) <= 0:
        return f"够用（需 {float(required):,.0f}，当前 {float(current):,.0f}）"
    if _is_missing(cost):
        return f"缺 {float(gap):,.0f}"
    return f"缺 {float(gap):,.0f}（约 ¥{float(cost):,.2f}）"


def format_tushare_catalog_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = df.copy()
    for column in [
        "limit_note",
        "permission_note",
        "doc_url",
        "permission_good_name",
        "permission_price",
        "permission_duration_unit",
        "permission_api_count",
        "permission_api_names",
        "required_points",
        "current_points",
        "points_gap",
        "estimated_points_cost",
        "history_coverage_type",
        "history_start",
        "history_coverage_note",
    ]:
        if column not in out.columns:
            out[column] = ""
    out["阶段"] = out["integration_stage"].map(TUSHARE_STAGE_LABELS).fillna(
        out["integration_stage"]
    )
    out["状态"] = out["integration_status"].map(TUSHARE_STATUS_LABELS).fillna(
        out["integration_status"]
    )
    out["权限"] = out["permission_level"].map(TUSHARE_PERMISSION_LABELS).fillna(
        out["permission_level"]
    )
    out["路径"] = out["category_path"].apply(lambda value: " > ".join(_decode_json_list(value)))
    out["能力"] = out["capability_tags"].apply(lambda value: "、".join(_decode_json_list(value)))
    out["报价"] = out.apply(_format_quote, axis=1)
    out["积分缺口"] = out.apply(_format_points_gap, axis=1)
    out["覆盖API"] = out["permission_api_names"].apply(
        lambda value: "、".join(_decode_json_list(value))
    )

    return out.rename(
        columns={
            "doc_id": "doc_id",
            "title": "标题",
            "api_name": "API",
            "priority": "优先级",
            "update_cadence": "更新节奏",
            "target_table_hint": "建议表",
            "strategy_value": "策略价值",
            "limit_note": "限量",
            "permission_note": "权限说明",
            "history_coverage_type": "历史覆盖",
            "history_start": "历史起始",
            "history_coverage_note": "历史说明",
            "doc_url": "文档",
        }
    )[
        [
            "阶段",
            "状态",
            "权限",
            "报价",
            "积分缺口",
            "优先级",
            "doc_id",
            "标题",
            "API",
            "覆盖API",
            "路径",
            "能力",
            "更新节奏",
            "建议表",
            "策略价值",
            "历史覆盖",
            "历史起始",
            "历史说明",
            "限量",
            "权限说明",
            "文档",
        ]
    ]
