"""rQuant 策略实验室 — 独立于健康看板的 Streamlit 应用。

启动方式（本地开发）：
    streamlit run src/rquant/dashboard/strategy_lab.py --server.port 8504 \\
        --server.address 0.0.0.0 --server.headless true

健康看板跑在 8501（30s 自动刷新）。本页不自动刷新，适合分钟 replay、
价量分布覆盖率检查和模拟盘参数对比。
"""

from __future__ import annotations

import json
import time as time_module
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from rquant.config import settings
from rquant.dashboard.strategy_lab_data import (
    TUSHARE_PERMISSION_LABELS,
    TUSHARE_STAGE_LABELS,
    TUSHARE_STATUS_LABELS,
    auction_gap_metric_rows,
    dataframe_preview,
    estimate_growth_board_workload,
    estimate_strategy_optimization_workload,
    format_tushare_catalog_display,
    growth_board_ablation_specs,
    growth_board_metric_rows,
    load_tushare_activity_packages,
    load_tushare_interface_catalog,
    load_tushare_purchase_goods,
    query_growth_board_candidates,
    safe_replay_end_date,
)
from rquant.dashboard.strategy_lab_runs import (
    RUN_TYPE_LABELS,
    build_strategy_lab_run,
    list_strategy_lab_runs,
    save_strategy_lab_run,
)
from rquant.dashboard.strategy_lab_worker import (
    cancel_background_run,
    launch_background_run,
    list_run_statuses,
)
from rquant.research_gate import (
    ResearchGateDecision,
    ResearchGateFailure,
    ResearchGateRequest,
    build_gate_research_manifest,
    evaluate_store_research_gate,
)
from rquant.research_manifest import (
    CURRENT_RESEARCH_NOTICES,
    RESEARCH_STATUS_LABELS,
    detect_code_commit,
)
from rquant.storage.duckdb import open_readonly_connection, open_readonly_store
from rquant.topn_selection import default_score_profiles
from rquant.volume_profile import calculate_volume_profile

CST = timezone(timedelta(hours=8))

ENTRY_LABELS: dict[str, str] = {
    "first_break": "第一次突破",
    "break_retest": "突破回踩确认",
    "late_confirm": "10:30后确认",
    "vwap_confirm": "VWAP确认",
    "amount_surge": "成交额突增",
    "factor_confirm": "多因子确认",
}
VARIANT_LABELS: dict[str, str] = {
    "baseline": "Baseline",
    "vp_risk_only": "90日价量动态风控",
    "vp_90": "90日价量过滤+风控",
}
SCORE_PROFILE_LABELS: dict[str, str] = {
    profile.name: profile.label for profile in default_score_profiles()
}

st.set_page_config(
    page_title="rQuant Strategy Lab",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    html, body, [class*="st-"], .stMarkdown, .stText { font-size: 13px; }
    .block-container { padding-top: 1.25rem !important; max-width: 1500px; }
    h1, .stMarkdown h1 { font-size: 1.55rem !important; margin-bottom: .15rem !important; }
    h2, .stMarkdown h2 {
        font-size: .8rem !important;
        color: #6b7280 !important;
        letter-spacing: .08em !important;
        text-transform: uppercase !important;
        border-top: 1px solid #e5e7eb !important;
        padding-top: .7rem !important;
        margin-top: 1.4rem !important;
    }
    h3, .stMarkdown h3 { font-size: .9rem !important; color: #374151 !important; }
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; }
    [data-testid="stMetricLabel"] {
        font-size: .7rem !important;
        color: #6b7280 !important;
        letter-spacing: .04em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stVerticalBlockBorderWrapper"] {
        border-radius: 6px !important;
        border-color: #e5e7eb !important;
        background: #fbfbfb !important;
    }
    [data-testid="stDataFrame"] { font-size: .78rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=120)
def query_duckdb(sql: str, params: tuple[object, ...] = ()) -> pd.DataFrame | None:
    """只读查询 DuckDB；写锁冲突时让页面降级。"""
    try:
        conn = open_readonly_connection()
    except duckdb.IOException as e:
        if "Conflicting lock" in str(e):
            return None
        raise
    try:
        return conn.execute(sql, list(params)).fetchdf()
    finally:
        conn.close()


@st.cache_data(ttl=60)
def current_research_gate(
    mode: str,
    strategy_name: str,
    start_date: date,
    end_date: date,
) -> tuple[ResearchGateRequest, ResearchGateDecision]:
    request = ResearchGateRequest(
        mode=mode,
        strategy_name=strategy_name,
        start_date=start_date,
        end_date=end_date,
        code_commit=detect_code_commit(),
    )
    try:
        with open_readonly_store() as store:
            decision = evaluate_store_research_gate(store, request)
    except Exception as exc:
        failure = ResearchGateFailure(
            code="metadata_unavailable",
            message=f"研究门元数据不可用: {type(exc).__name__}: {exc}",
        )
        decision = ResearchGateDecision(
            allowed=mode == "exploratory",
            research_status="exploratory",
            audit_run_id=None,
            dataset_snapshot_id=None,
            coverage_ratios={},
            coverage_counts={},
            failures=(failure,),
        )
    return request, decision


def _render_research_gate(decision: ResearchGateDecision) -> None:
    if decision.allowed and decision.research_status == "comparable":
        st.success("正式研究门已通过")
        return
    if not decision.failures:
        st.info("当前为探索性试跑")
        return
    st.warning("；".join(failure.message for failure in decision.failures))


@st.cache_data(ttl=300)
def screen_bounds(preset_name: str) -> tuple[date | None, date | None, int]:
    if preset_name == "n-shape-combined":
        df = query_duckdb(
            """
            SELECT MIN(trade_date) AS min_date,
                   MAX(trade_date) AS max_date,
                   COUNT(*) AS candidates
            FROM (
                SELECT DISTINCT trade_date, ts_code
                FROM screen_result
                WHERE preset_name IN ('n-shape-pool1', 'n-shape-pool2')
            )
            """
        )
        if df is None or df.empty or pd.isna(df.iloc[0]["min_date"]):
            return None, None, 0
        row = df.iloc[0]
        min_date = (
            row["min_date"].date()
            if hasattr(row["min_date"], "date")
            else row["min_date"]
        )
        max_date = (
            row["max_date"].date()
            if hasattr(row["max_date"], "date")
            else row["max_date"]
        )
        return min_date, max_date, int(row["candidates"])
    df = query_duckdb(
        """
        SELECT MIN(trade_date) AS min_date,
               MAX(trade_date) AS max_date,
               COUNT(*) AS candidates
        FROM screen_result
        WHERE preset_name = ?
        """,
        (preset_name,),
    )
    if df is None or df.empty or pd.isna(df.iloc[0]["min_date"]):
        return None, None, 0
    row = df.iloc[0]
    min_date = row["min_date"].date() if hasattr(row["min_date"], "date") else row["min_date"]
    max_date = row["max_date"].date() if hasattr(row["max_date"], "date") else row["max_date"]
    return min_date, max_date, int(row["candidates"])


@st.cache_data(ttl=300)
def screen_candidate_count(
    start_date: str,
    end_date: str,
    preset_name: str,
) -> int | None:
    if preset_name == "n-shape-combined":
        df = query_duckdb(
            """
            SELECT COUNT(*) AS candidates
            FROM (
                SELECT DISTINCT trade_date, ts_code
                FROM screen_result
                WHERE trade_date >= ?
                  AND trade_date <= ?
                  AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
            )
            """,
            (date.fromisoformat(start_date), date.fromisoformat(end_date)),
        )
    else:
        df = query_duckdb(
            """
            SELECT COUNT(*) AS candidates
            FROM screen_result
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND preset_name = ?
            """,
            (date.fromisoformat(start_date), date.fromisoformat(end_date), preset_name),
        )
    if df is None:
        return None
    if df.empty:
        return 0
    return int(df.iloc[0]["candidates"])


@st.cache_data(ttl=300)
def trading_calendar() -> list[date]:
    df = query_duckdb(
        """
        SELECT DISTINCT trade_date
        FROM daily_bar
        ORDER BY trade_date
        """
    )
    if df is None or df.empty:
        return []
    out: list[date] = []
    for value in df["trade_date"].tolist():
        out.append(value.date() if hasattr(value, "date") else value)
    return out


@st.cache_data(ttl=300)
def minute_overview() -> pd.DataFrame | None:
    return query_duckdb(
        """
        SELECT COUNT(*) AS rows_count,
               COUNT(DISTINCT ts_code) AS codes_count,
               COUNT(DISTINCT CAST(trade_time AS DATE)) AS trade_dates,
               MIN(trade_time) AS min_time,
               MAX(trade_time) AS max_time
        FROM minute_bar
        WHERE freq = '1min'
        """
    )


@st.cache_data(ttl=300)
def minute_source_overview() -> pd.DataFrame | None:
    return query_duckdb(
        """
        SELECT source,
               COUNT(*) AS rows_count,
               COUNT(DISTINCT ts_code) AS codes_count,
               COUNT(DISTINCT CAST(trade_time AS DATE)) AS trade_dates,
               MIN(trade_time) AS min_time,
               MAX(trade_time) AS max_time
        FROM minute_bar
        WHERE freq = '1min'
        GROUP BY source
        ORDER BY rows_count DESC
        """
    )


@st.cache_data(ttl=900)
def tushare_interface_catalog_cached() -> pd.DataFrame:
    try:
        return load_tushare_interface_catalog(
            settings.data_dir / "tushare_interface_audit.duckdb"
        )
    except duckdb.Error:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def tushare_purchase_goods_cached() -> pd.DataFrame:
    try:
        return load_tushare_purchase_goods(
            settings.data_dir / "tushare_interface_audit.duckdb"
        )
    except duckdb.Error:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def tushare_activity_packages_cached() -> pd.DataFrame:
    try:
        return load_tushare_activity_packages(
            settings.data_dir / "tushare_interface_audit.duckdb"
        )
    except duckdb.Error:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def strategy_compare_cached(
    start_date: str,
    end_date: str,
    entry_modes: tuple[str, ...],
    profile_variants: tuple[str, ...],
    max_hold_days: int,
    preset_name: str,
    factor_score_threshold: float = 35.0,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    from rquant.strategy_compare import run_entry_mode_comparison

    with open_readonly_store() as store:
        result = run_entry_mode_comparison(
            store,
            start_date=start_date,
            end_date=end_date,
            entry_modes=list(entry_modes),
            profile_variants=list(profile_variants),
            max_hold_days=max_hold_days,
            preset_name=preset_name,
            factor_score_threshold=factor_score_threshold,
        )
    return result.summary, result.trades, result.candidates_count


@st.cache_data(ttl=1800)
def strategy_optimize_cached(
    start_date: str,
    end_date: str,
    entry_modes: tuple[str, ...],
    profile_variants: tuple[str, ...],
    hold_options: tuple[int, ...],
    top_n_options: tuple[int, ...],
    score_profile_names: tuple[str, ...],
    walk_forward_folds: int,
    preset_name: str,
    validation_ratio: float,
    min_trades: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from rquant.strategy_optimizer import run_strategy_optimization

    with open_readonly_store() as store:
        result = run_strategy_optimization(
            store,
            start_date=start_date,
            end_date=end_date,
            entry_modes=list(entry_modes),
            profile_variants=list(profile_variants),
            max_hold_days_options=list(hold_options),
            top_n_options=list(top_n_options),
            score_profile_names=list(score_profile_names),
            walk_forward_folds=walk_forward_folds,
            preset_name=preset_name,
            validation_ratio=validation_ratio,
            min_trades=min_trades,
        )
    return (
        result.rankings,
        result.trades,
        result.topn_rankings,
        result.topn_trades,
        result.walk_forward_rankings,
        result.walk_forward_trades,
    )


@st.cache_data(ttl=1800)
def auction_gap_minute_cached(
    start_date: str,
    end_date: str,
    gap_mode: str,
    st_filter: str,
    min_ratio: float,
    max_ratio: float,
    max_hold_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
    )

    config = AuctionGapMinuteReplayConfig(
        start_date=start_date,
        end_date=end_date,
        gap_mode=gap_mode,
        st_filter=st_filter,
        min_auction_vol_ratio_5d=min_ratio,
        max_auction_vol_ratio_5d=max_ratio,
        max_hold_days=max_hold_days,
    )
    with open_readonly_store(
        required_tables=[
            "auction_bar",
            "daily_bar",
            "daily_state",
            "minute_bar",
            "stock_status_daily",
        ]
    ) as store:
        baseline = run_auction_gap_replay(store, config.auction_config())
        minute_trades = run_auction_gap_minute_replay(store, config)
    return auction_gap_metric_rows(baseline, minute_trades), baseline, minute_trades


@st.cache_data(ttl=1800)
def growth_board_surge_cached(
    start_date: str,
    end_date: str,
    lookback_days: int,
    min_hist_days: int,
    min_cum_amount_ratio: float,
    min_same_minute_amount_ratio: float,
    min_amount_accel_5m: float,
    max_hold_days: int,
    require_vwap_strength: bool,
    use_same_minute_surge: bool = True,
    use_accel_surge: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    config = GrowthBoardSurgeConfig(
        lookback_days=lookback_days,
        min_hist_days=min_hist_days,
        min_cum_amount_ratio=min_cum_amount_ratio,
        min_same_minute_amount_ratio=min_same_minute_amount_ratio,
        min_amount_accel_5m=min_amount_accel_5m,
        max_hold_days=max_hold_days,
        require_vwap_strength=require_vwap_strength,
        use_same_minute_surge=use_same_minute_surge,
        use_accel_surge=use_accel_surge,
    )
    with open_readonly_store(
        required_tables=["daily_bar", "daily_state", "minute_bar", "stock_status_daily"]
    ) as store:
        trades = run_growth_board_surge_replay(
            store,
            start_date=start_date,
            end_date=end_date,
            config=config,
        )
    return growth_board_metric_rows(trades), trades


@st.cache_data(ttl=1800)
def growth_board_ablation_cached(
    start_date: str,
    end_date: str,
    lookback_days: int,
    min_hist_days: int,
    min_cum_amount_ratio: float,
    min_same_minute_amount_ratio: float,
    min_amount_accel_5m: float,
    max_hold_days: int,
    spec_keys: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_specs = {spec.key: spec for spec in growth_board_ablation_specs()}
    metric_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    for key in spec_keys:
        spec = all_specs[key]
        metrics, trades = growth_board_surge_cached(
            start_date,
            end_date,
            lookback_days,
            min_hist_days,
            min_cum_amount_ratio,
            min_same_minute_amount_ratio,
            min_amount_accel_5m,
            max_hold_days,
            spec.require_vwap_strength,
            spec.use_same_minute_surge,
            spec.use_accel_surge,
        )
        metrics = growth_board_metric_rows(trades, strategy_name=spec.label)
        metrics["说明"] = spec.description
        metric_frames.append(metrics)
        if not trades.empty:
            with_label = trades.copy()
            with_label.insert(0, "ablation_key", spec.key)
            with_label.insert(1, "ablation_label", spec.label)
            trade_frames.append(with_label)
    metric_df = pd.concat(metric_frames, ignore_index=True) if metric_frames else pd.DataFrame()
    trade_df = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    if not metric_df.empty:
        full_rows = metric_df[metric_df["策略"] == "完整策略"]
        if not full_rows.empty:
            base_mean = full_rows.iloc[0]["平均收益%"]
            base_win = full_rows.iloc[0]["胜率%"]
            metric_df["较完整均值差%"] = metric_df["平均收益%"].apply(
                lambda value: (
                    round(float(value) - float(base_mean), 4)
                    if pd.notna(value) and pd.notna(base_mean)
                    else None
                )
            )
            metric_df["较完整胜率差%"] = metric_df["胜率%"].apply(
                lambda value: (
                    round(float(value) - float(base_win), 4)
                    if pd.notna(value) and pd.notna(base_win)
                    else None
                )
            )
    return metric_df, trade_df


@st.cache_data(ttl=300)
def growth_board_candidate_count_cached(
    start_date: str,
    end_date: str,
) -> int:
    with open_readonly_store(
        required_tables=["daily_bar", "stock_status_daily"]
    ) as store:
        candidates = query_growth_board_candidates(
            store._conn,
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
        )
    return len(candidates)


@st.cache_data(ttl=300)
def auction_candidate_count_cached(
    start_date: str,
    end_date: str,
) -> int:
    try:
        df = query_duckdb(
            """
            SELECT COUNT(*) AS candidates
            FROM auction_bar
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND auction_type = 'open_realtime'
              AND price IS NOT NULL
              AND vol > 0
            """,
            (date.fromisoformat(start_date), date.fromisoformat(end_date)),
        )
    except duckdb.Error:
        return 0
    if df is None or df.empty:
        return 0
    return int(df.iloc[0]["candidates"])


@st.cache_data(ttl=1800)
def profile_coverage_cached(
    start_date: str,
    end_date: str,
    preset_name: str,
    lookback_days: int,
) -> tuple[int, int, pd.DataFrame, pd.DataFrame]:
    with open_readonly_store() as store:
        candidates = store._conn.execute(
            """
            SELECT trade_date, ts_code, name
            FROM screen_result
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND preset_name = ?
            ORDER BY trade_date, ts_code
            """,
            [date.fromisoformat(start_date), date.fromisoformat(end_date), preset_name],
        ).fetchdf()
        missing: list[dict[str, object]] = []
        samples: list[dict[str, object]] = []
        ok = 0
        for row in candidates.itertuples(index=False):
            ref_date = row.trade_date.date() if hasattr(row.trade_date, "date") else row.trade_date
            profile = calculate_volume_profile(
                store,
                row.ts_code,
                reference_date=ref_date,
                lookback_days=lookback_days,
            )
            if profile is None:
                missing.append({
                    "trade_date": ref_date,
                    "ts_code": row.ts_code,
                    "name": row.name,
                })
                continue
            ok += 1
            if len(samples) < 20:
                samples.append({
                    "trade_date": ref_date,
                    "ts_code": profile.ts_code,
                    "lookback_days": profile.lookback_days,
                    "start_date": profile.start_date,
                    "end_date": profile.end_date,
                    "rows_count": profile.rows_count,
                    "poc_price": profile.poc_price,
                    "value_area_low": profile.value_area_low,
                    "value_area_high": profile.value_area_high,
                    "vwap": profile.vwap,
                    "total_amount": profile.total_amount,
                })
    return ok, len(candidates), pd.DataFrame(missing), pd.DataFrame(samples)


def _label_to_key(labels: list[str], mapping: dict[str, str]) -> tuple[str, ...]:
    return tuple(key for key, label in mapping.items() if label in labels)


def _display_summary(summary: pd.DataFrame) -> pd.DataFrame:
    display = summary.copy()
    display["entry_mode"] = display["entry_mode"].map(ENTRY_LABELS)
    display["profile_variant"] = display["profile_variant"].map(VARIANT_LABELS)
    return display.rename(columns={
        "entry_mode": "入场模式",
        "profile_variant": "风控版本",
        "candidates": "候选",
        "trades": "交易",
        "trigger_rate_pct": "触发率%",
        "mean_ret_pct": "平均收益%",
        "median_ret_pct": "中位收益%",
        "win_rate_pct": "胜率%",
        "best_ret_pct": "最佳%",
        "worst_ret_pct": "最差%",
        "gap_stop_rate_pct": "跳空止损%",
    })


def _display_trades(trades: pd.DataFrame) -> pd.DataFrame:
    display = trades.copy()
    display["entry_mode"] = display["entry_mode"].map(ENTRY_LABELS)
    display["profile_variant"] = display["profile_variant"].map(VARIANT_LABELS)
    cols = [
        "split",
        "max_hold_days",
        "selection",
        "score_profile_label",
        "entry_mode",
        "profile_variant",
        "feature_rank",
        "feature_score",
        "signal_date",
        "ts_code",
        "name",
        "entry_time",
        "entry_price_raw",
        "entry_price",
        "stop_loss_price",
        "stop_loss_basis",
        "take_profit_price",
        "take_profit_basis",
        "market_limit_up_count",
        "market_first_limit_up_count",
        "market_limit_down_count",
        "market_max_consecutive_limit_ups",
        "market_high_board_count",
        "market_up_ratio_pct",
        "index_sse_pct_chg",
        "index_szse_pct_chg",
        "index_chinext_pct_chg",
        "index_csi300_pct_chg",
        "index_csi500_pct_chg",
        "index_csi1000_pct_chg",
        "price_position_90d_pct",
        "price_position_120d_pct",
        "price_position_250d_pct",
        "distance_to_high_90d_pct",
        "accum_obv_change_20d_pct",
        "accum_ad_flow_20d_pct",
        "accum_up_down_amount_ratio_20d",
        "accum_heavy_no_drop_days_20d",
        "signal_rel_amount_same_minute_20d",
        "signal_rel_cum_amount_asof_20d",
        "signal_opening_segment",
        "signal_opening_segment_amount",
        "signal_amount_accel_5m",
        "signal_amount_accel_10m",
        "hist_intraday_days_20d",
        "volume_profile_lookbacks",
        "volume_profile_rr",
        "exit_time",
        "exit_price",
        "exit_reason",
        "holding_trading_days",
        "ret_pct",
    ]
    return display[[c for c in cols if c in display.columns]].rename(columns={
        "split": "样本",
        "max_hold_days": "持有期",
        "selection": "特征选择",
        "score_profile_label": "评分画像",
        "entry_mode": "入场模式",
        "profile_variant": "风控版本",
        "feature_rank": "特征排名",
        "feature_score": "特征分",
        "signal_date": "信号日",
        "ts_code": "代码",
        "name": "名称",
        "entry_time": "买入时间",
        "entry_price_raw": "原始买价",
        "entry_price": "买入价",
        "stop_loss_price": "止损价",
        "stop_loss_basis": "止损依据",
        "take_profit_price": "止盈价",
        "take_profit_basis": "止盈依据",
        "market_limit_up_count": "涨停家数",
        "market_first_limit_up_count": "首板家数",
        "market_limit_down_count": "跌停家数",
        "market_max_consecutive_limit_ups": "最高连板",
        "market_high_board_count": "3板+家数",
        "market_up_ratio_pct": "上涨占比%",
        "index_sse_pct_chg": "上证%",
        "index_szse_pct_chg": "深成%",
        "index_chinext_pct_chg": "创业板%",
        "index_csi300_pct_chg": "沪深300%",
        "index_csi500_pct_chg": "中证500%",
        "index_csi1000_pct_chg": "中证1000%",
        "price_position_90d_pct": "90日位置%",
        "price_position_120d_pct": "120日位置%",
        "price_position_250d_pct": "250日位置%",
        "distance_to_high_90d_pct": "距90日高%",
        "accum_obv_change_20d_pct": "OBV变化%",
        "accum_ad_flow_20d_pct": "A/D流%",
        "accum_up_down_amount_ratio_20d": "涨跌量额比",
        "accum_heavy_no_drop_days_20d": "放量不跌天",
        "signal_rel_amount_same_minute_20d": "同刻放量倍",
        "signal_rel_cum_amount_asof_20d": "累计放量倍",
        "signal_opening_segment": "开盘段",
        "signal_opening_segment_amount": "开盘段成交额",
        "signal_amount_accel_5m": "5分加速度",
        "signal_amount_accel_10m": "10分加速度",
        "hist_intraday_days_20d": "分时基准日",
        "volume_profile_lookbacks": "价量窗口",
        "volume_profile_rr": "收益风险比",
        "exit_time": "卖出时间",
        "exit_price": "卖出价",
        "exit_reason": "卖出原因",
        "holding_trading_days": "持股天数",
        "ret_pct": "收益%",
    })


def _display_growth_board_trades(trades: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "ablation_label",
        "signal_date",
        "ts_code",
        "name",
        "board_type",
        "entry_time",
        "entry_price_raw",
        "entry_price",
        "limit_up_price",
        "entry_to_limit_room_pct",
        "hit_limit_up_today",
        "signal_rel_amount_same_minute",
        "signal_rel_cum_amount_asof",
        "signal_amount_accel_5m",
        "signal_amount_accel_10m",
        "hist_intraday_days",
        "signal_vwap",
        "signal_limit_progress_pct",
        "exit_time",
        "exit_price",
        "exit_reason",
        "holding_trading_days",
        "ret_pct",
    ]
    return trades[[c for c in cols if c in trades.columns]].rename(columns={
        "ablation_label": "消融版本",
        "signal_date": "信号日",
        "ts_code": "代码",
        "name": "名称",
        "board_type": "板块",
        "entry_time": "买入时间",
        "entry_price_raw": "原始买价",
        "entry_price": "买入价",
        "limit_up_price": "涨停价",
        "entry_to_limit_room_pct": "距涨停空间%",
        "hit_limit_up_today": "当日上板",
        "signal_rel_amount_same_minute": "同刻放量倍",
        "signal_rel_cum_amount_asof": "累计放量倍",
        "signal_amount_accel_5m": "5分加速度",
        "signal_amount_accel_10m": "10分加速度",
        "hist_intraday_days": "分时基准日",
        "signal_vwap": "信号VWAP",
        "signal_limit_progress_pct": "涨停进度%",
        "exit_time": "卖出时间",
        "exit_price": "卖出价",
        "exit_reason": "卖出原因",
        "holding_trading_days": "持股天数",
        "ret_pct": "收益%",
    })


def _display_topn_rankings(rankings: pd.DataFrame) -> pd.DataFrame:
    display = rankings.copy()
    display["entry_mode"] = display["entry_mode"].map(ENTRY_LABELS)
    display["profile_variant"] = display["profile_variant"].map(VARIANT_LABELS)
    cols = [
        "rank",
        "candidate_id",
        "entry_mode",
        "profile_variant",
        "score_profile_label",
        "selection",
        "max_hold_days",
        "robust_score",
        "train_trades",
        "train_mean_ret_pct",
        "train_median_ret_pct",
        "train_win_rate_pct",
        "train_mean_delta_vs_all_pct",
        "test_trades",
        "test_mean_ret_pct",
        "test_median_ret_pct",
        "test_win_rate_pct",
        "test_mean_delta_vs_all_pct",
        "test_avg_feature_score",
    ]
    return display[[c for c in cols if c in display.columns]].rename(columns={
        "rank": "排名",
        "candidate_id": "组合ID",
        "entry_mode": "入场模式",
        "profile_variant": "风控版本",
        "score_profile_label": "评分画像",
        "selection": "特征选择",
        "max_hold_days": "持有期",
        "robust_score": "稳健分",
        "train_trades": "训练交易",
        "train_mean_ret_pct": "训练均值%",
        "train_median_ret_pct": "训练中位%",
        "train_win_rate_pct": "训练胜率%",
        "train_mean_delta_vs_all_pct": "训练较全量%",
        "test_trades": "验证交易",
        "test_mean_ret_pct": "验证均值%",
        "test_median_ret_pct": "验证中位%",
        "test_win_rate_pct": "验证胜率%",
        "test_mean_delta_vs_all_pct": "验证较全量%",
        "test_avg_feature_score": "验证特征分",
    })


def _display_walk_forward_rankings(rankings: pd.DataFrame) -> pd.DataFrame:
    display = rankings.copy()
    display["entry_mode"] = display["entry_mode"].map(ENTRY_LABELS)
    display["profile_variant"] = display["profile_variant"].map(VARIANT_LABELS)
    cols = [
        "rank",
        "candidate_id",
        "entry_mode",
        "profile_variant",
        "score_profile_label",
        "selection",
        "max_hold_days",
        "robust_score",
        "folds",
        "train_folds",
        "train_trades",
        "train_mean_ret_pct",
        "test_trades",
        "test_mean_ret_pct",
        "test_median_ret_pct",
        "test_win_rate_pct",
        "test_worst_ret_pct",
        "test_avg_feature_score",
    ]
    return display[[c for c in cols if c in display.columns]].rename(columns={
        "rank": "排名",
        "candidate_id": "组合ID",
        "entry_mode": "入场模式",
        "profile_variant": "风控版本",
        "score_profile_label": "评分画像",
        "selection": "特征选择",
        "max_hold_days": "持有期",
        "robust_score": "稳健分",
        "folds": "验证折数",
        "train_folds": "训练折数",
        "train_trades": "训练交易",
        "train_mean_ret_pct": "训练均值%",
        "test_trades": "验证交易",
        "test_mean_ret_pct": "验证均值%",
        "test_median_ret_pct": "验证中位%",
        "test_win_rate_pct": "验证胜率%",
        "test_worst_ret_pct": "验证最差%",
        "test_avg_feature_score": "验证特征分",
    })


def _json_list(value: object) -> list[str]:
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


def _show_dataframe(
    df: pd.DataFrame,
    *,
    max_rows: int = 500,
    hide_index: bool = True,
) -> None:
    preview, truncated = dataframe_preview(df, max_rows=max_rows)
    if truncated:
        st.caption(f"仅渲染前 {max_rows:,} 行，完整结果已在历史记录/导出文件中保存。")
    st.dataframe(preview, hide_index=hide_index, width="stretch")


def _format_seconds(seconds: float) -> str:
    if seconds < 60:
        return f"{max(0, int(round(seconds)))}秒"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}分钟"
    return f"{minutes / 60:.1f}小时"


def _run_with_countdown(
    label: str,
    estimated_seconds: float,
    work: Callable[[], Any],
) -> Any:
    estimated = max(float(estimated_seconds), 1.0)
    progress = st.progress(
        0.0,
        text=f"{label} · 预计 {_format_seconds(estimated)}",
    )
    start = time_module.monotonic()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(work)
        while not future.done():
            elapsed = time_module.monotonic() - start
            remaining = estimated - elapsed
            if remaining > 0:
                text = (
                    f"{label} · 已用 {_format_seconds(elapsed)} · "
                    f"预计剩余 {_format_seconds(remaining)}"
                )
            else:
                text = f"{label} · 已用 {_format_seconds(elapsed)} · 已超过预估，仍在运行"
            progress.progress(min(0.95, elapsed / estimated), text=text)
            time_module.sleep(0.5)
        result = future.result()
    elapsed = time_module.monotonic() - start
    progress.progress(1.0, text=f"{label} · 完成 · 实际 {_format_seconds(elapsed)}")
    return result


st.markdown("# 🧪 rQuant Strategy Lab")
st.caption(f"渲染时间 {datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')}")
with st.expander("研究可信度警示", expanded=True):
    st.caption(
        "收益排行不等于可用策略。当前记录必须先通过数据覆盖、无未来函数、可成交性和"
        "严格样本外验证，才能进入模拟盘。基线见 "
        "docs/analysis/2026-07-13-research-trust-baseline.md。"
    )
    for notice in CURRENT_RESEARCH_NOTICES:
        message = f"**{notice.title}**：{notice.body}"
        if notice.severity == "error":
            st.error(message)
        elif notice.severity == "warning":
            st.warning(message)
        else:
            st.info(message)

research_mode_label = st.sidebar.radio(
    "研究用途",
    ["探索性试跑", "正式回测"],
    horizontal=True,
    key="research_mode",
)
research_mode = "formal" if research_mode_label == "正式回测" else "exploratory"

PRESET_OPTIONS = ["n-shape-combined", "n-shape-pool1", "n-shape-pool2"]

# 收益对比结果落盘时每张表最多保留的行数；超出部分只存在于当前会话内存
COMPARE_SAVE_MAX_ROWS = 2000

# form 内控件提交前不触发 rerun，日期边界按上一次提交（或默认）的池子/持有期先算
_prev_preset = str(st.session_state.get("compare_preset", PRESET_OPTIONS[0]))
_prev_hold = int(st.session_state.get("compare_hold_days", 1))

bounds_min, bounds_max, total_candidates = screen_bounds(_prev_preset)
calendar = trading_calendar()
if bounds_min is None or bounds_max is None or not calendar:
    st.warning("暂无可回测数据")
    st.stop()

safe_end = safe_replay_end_date(
    calendar,
    bounds_max,
    max_hold_days=_prev_hold,
)
if safe_end is None or safe_end < bounds_min:
    st.warning("可用交易日不足")
    st.stop()

default_end = safe_end
default_start = max(bounds_min, default_end - timedelta(days=30))

# 换池子/持有期后旧日期可能越界，重置回默认，避免 date_input 抛越界异常
_dates_were_reset = False
for _date_key in ("compare_start", "compare_end"):
    _picked = st.session_state.get(_date_key)
    if isinstance(_picked, date) and not (bounds_min <= _picked <= safe_end):
        del st.session_state[_date_key]
        _dates_were_reset = True

with st.sidebar.form("compare_form"):
    preset_name = st.selectbox("候选池", PRESET_OPTIONS, key="compare_preset")
    hold_days = st.number_input(
        "持有交易日",
        min_value=1,
        max_value=10,
        value=1,
        step=1,
        key="compare_hold_days",
    )
    start_value = st.date_input(
        "开始日期",
        value=default_start,
        min_value=bounds_min,
        max_value=safe_end,
        key="compare_start",
    )
    end_value = st.date_input(
        "结束日期",
        value=default_end,
        min_value=bounds_min,
        max_value=safe_end,
        key="compare_end",
    )
    selected_mode_labels = st.multiselect(
        "入场模式",
        list(ENTRY_LABELS.values()),
        default=[ENTRY_LABELS["first_break"]],
        key="compare_modes",
    )
    factor_score_threshold = st.number_input(
        "多因子确认阈值（仅「多因子确认」模式生效）",
        min_value=0.0,
        max_value=100.0,
        value=35.0,
        step=1.0,
        key="compare_factor_threshold",
    )
    selected_variant_labels = st.multiselect(
        "风控版本",
        list(VARIANT_LABELS.values()),
        default=[
            VARIANT_LABELS["baseline"],
            VARIANT_LABELS["vp_risk_only"],
            VARIANT_LABELS["vp_90"],
        ],
        key="compare_variants",
    )
    n_gate_request, n_gate_decision = current_research_gate(
        research_mode,
        "n_shape",
        start_value,
        end_value,
    )
    run_clicked = st.form_submit_button(
        "运行对比",
        type="primary",
        width="stretch",
        disabled=research_mode == "formal" and not n_gate_decision.allowed,
    )

n_gate_request = n_gate_request.model_copy(
    update={
        "audit_run_id": n_gate_decision.audit_run_id,
        "dataset_snapshot_id": n_gate_decision.dataset_snapshot_id,
    }
)
with st.sidebar.expander("正式研究门", expanded=research_mode == "formal"):
    _render_research_gate(n_gate_decision)

# 日期被重置时不静默执行：让用户看清新区间后再点一次，避免以为跑的是自己选的区间
if _dates_were_reset:
    st.warning(
        f"所选日期超出当前候选池/持有期的可用范围，已重置为默认 "
        f"{default_start:%Y-%m-%d} ~ {default_end:%Y-%m-%d}。"
        "本次提交未执行回测，请确认日期后重新点「运行对比」。"
    )
    run_clicked = False

selected_modes = _label_to_key(selected_mode_labels, ENTRY_LABELS)
selected_variants = _label_to_key(selected_variant_labels, VARIANT_LABELS)

overview = minute_overview()
source_overview = minute_source_overview()
metric_cols = st.columns(5)
metric_cols[0].metric("候选总数", f"{total_candidates:,}")
metric_cols[1].metric("Pool 日期", f"{bounds_min:%m-%d} → {bounds_max:%m-%d}")
metric_cols[2].metric("安全截止", f"{safe_end:%m-%d}")
if overview is None or overview.empty:
    metric_cols[3].metric("分钟线", "—")
    metric_cols[4].metric("覆盖股票", "—")
else:
    overview_row = overview.iloc[0]
    metric_cols[3].metric("分钟线", f"{int(overview_row['rows_count']):,}")
    metric_cols[4].metric("覆盖股票", f"{int(overview_row['codes_count']):,}")

if start_value > end_value:
    st.warning("开始日期不能晚于结束日期")
    st.stop()
if not selected_modes or not selected_variants:
    st.warning("至少选择一个入场模式和一个风控版本")
    st.stop()

(
    tab_compare,
    tab_optimize,
    tab_trades,
    tab_auction,
    tab_growth,
    tab_coverage,
    tab_exits,
    tab_interfaces,
    tab_history,
) = st.tabs(
    [
        "收益对比",
        "自动优化",
        "交易明细",
        "集合竞价跳空",
        "科创/创业放量",
        "价量分布",
        "退出原因",
        "数据接口",
        "历史记录",
    ]
)

if run_clicked:
    compare_candidate_count = screen_candidate_count(
        start_value.isoformat(),
        end_value.isoformat(),
        preset_name,
    ) or 0
    compare_variants = max(1, len(selected_modes) * len(selected_variants))
    compare_estimated_seconds = max(2.0, compare_candidate_count * compare_variants * 0.012)
    with st.spinner("正在回放分钟策略..."):
        summary, trades, candidates_count = _run_with_countdown(
            "N字分钟回放",
            compare_estimated_seconds,
            lambda: strategy_compare_cached(
                start_value.isoformat(),
                end_value.isoformat(),
                selected_modes,
                selected_variants,
                int(hold_days),
                preset_name,
                float(factor_score_threshold),
            ),
        )
    compare_saved = save_strategy_lab_run(
        build_strategy_lab_run(
            run_type="n_shape_compare",
            title=f"N字收益对比 {start_value:%Y-%m-%d} 至 {end_value:%Y-%m-%d}",
            params={
                "候选池": preset_name,
                "开始日期": start_value,
                "结束日期": end_value,
                "入场模式": list(selected_modes),
                "风控版本": list(selected_variants),
                "持有交易日": int(hold_days),
                "多因子确认阈值": float(factor_score_threshold),
            },
            metrics={
                "区间候选": candidates_count,
                "触发交易": int(summary["trades"].sum()) if not summary.empty else 0,
                "平均收益%": (
                    round(float(trades["ret_pct"].mean()), 4) if not trades.empty else None
                ),
                "胜率%": (
                    round(float((trades["ret_pct"] > 0).mean() * 100), 2)
                    if not trades.empty
                    else None
                ),
            },
            tables={
                "策略对比汇总": summary,
                "交易明细": trades,
            },
            max_rows_per_table=COMPARE_SAVE_MAX_ROWS,
            manifest=build_gate_research_manifest(n_gate_request, n_gate_decision),
        )
    )
    # 结果进 session_state：切 tab / 改控件触发 rerun 不再丢结果
    st.session_state["compare_result"] = {
        "summary": summary,
        "trades": trades,
        "candidates_count": candidates_count,
        "run_id": compare_saved.run_id,
        "title": compare_saved.title,
        "markdown": compare_saved.markdown,
        "markdown_path": str(compare_saved.markdown_path),
        "json_path": str(compare_saved.json_path),
    }

with tab_compare:
    compare_state = st.session_state.get("compare_result")
    if compare_state is None:
        st.info("选择参数后运行对比")
    else:
        summary = compare_state["summary"]
        trades = compare_state["trades"]
        st.caption(
            f"当前结果：{compare_state['title']} · 记录 `{compare_state['run_id']}`"
            "（已落盘，切 tab / 改参数不丢；重跑覆盖显示）"
        )
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("区间候选", int(compare_state["candidates_count"]))
        k2.metric("触发交易", int(summary["trades"].sum()) if not summary.empty else 0)
        if trades.empty:
            k3.metric("平均收益", "—")
            k4.metric("胜率", "—")
        else:
            k3.metric("平均收益", f"{trades['ret_pct'].mean():.2f}%")
            k4.metric("胜率", f"{(trades['ret_pct'] > 0).mean() * 100:.1f}%")

        _show_dataframe(_display_summary(summary))
        st.download_button(
            "导出本次结果 Markdown",
            data=compare_state["markdown"],
            file_name=f"{compare_state['run_id']}.md",
            mime="text/markdown",
            width="stretch",
            key=f"compare_download_{compare_state['run_id']}",
        )
        st.caption(
            f"Markdown: {compare_state['markdown_path']} · JSON: {compare_state['json_path']}"
        )
        if len(trades) > COMPARE_SAVE_MAX_ROWS:
            st.caption(
                f"注意：本次交易明细共 {len(trades):,} 行，历史记录落盘截断至前 "
                f"{COMPARE_SAVE_MAX_ROWS:,} 行；完整明细仅保留在当前会话内。"
            )
        if not summary.empty:
            chart_df = summary.copy()
            chart_df["entry_label"] = chart_df["entry_mode"].map(ENTRY_LABELS)
            chart_df["variant_label"] = chart_df["profile_variant"].map(VARIANT_LABELS)
            chart_df["strategy"] = chart_df["entry_label"] + " / " + chart_df["variant_label"]
            chart = (
                alt.Chart(chart_df)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("strategy:N", title=None, sort=None, axis=alt.Axis(labelAngle=-25)),
                    y=alt.Y("mean_ret_pct:Q", title="平均收益%"),
                    color=alt.condition(
                        alt.datum.mean_ret_pct >= 0,
                        alt.value("#0f766e"),
                        alt.value("#b91c1c"),
                    ),
                    tooltip=[
                        alt.Tooltip("entry_label:N", title="入场"),
                        alt.Tooltip("variant_label:N", title="风控"),
                        alt.Tooltip("trades:Q", title="交易"),
                        alt.Tooltip("mean_ret_pct:Q", title="平均收益%"),
                        alt.Tooltip("median_ret_pct:Q", title="中位收益%"),
                        alt.Tooltip("win_rate_pct:Q", title="胜率%"),
                    ],
                )
                .properties(height=260)
            )
            st.altair_chart(chart, width="stretch")

with tab_trades:
    compare_state = st.session_state.get("compare_result")
    if compare_state is None:
        st.info("暂无回放结果")
    elif compare_state["trades"].empty:
        st.info("所选区间没有触发交易")
    else:
        st.caption(f"来自：{compare_state['title']} · 记录 `{compare_state['run_id']}`")
        if len(compare_state["trades"]) > COMPARE_SAVE_MAX_ROWS:
            st.caption(
                f"注意：本次交易明细共 {len(compare_state['trades']):,} 行，"
                f"历史记录落盘截断至前 {COMPARE_SAVE_MAX_ROWS:,} 行；"
                "完整明细仅保留在当前会话内。"
            )
        _show_dataframe(_display_trades(compare_state["trades"]))

with tab_optimize:
    with st.expander("自动优化说明书", expanded=False):
        st.markdown(
            """
            你可以把自动优化理解成“让电脑帮你试菜谱”：

            - 入场模式：什么时候买。比如第一次突破就买，还是等 VWAP 确认再买。
            - 风控版本：买进去以后怎么设止损止盈。Baseline 是朴素版本，
              90日价量版本会参考过去筹码位置。
            - 持有期：最多拿几天。选 1、2、3 天，就是让电脑分别试三套卖出窗口。
            - 特征 topN：如果一天触发很多只票，只挑评分最高的前 N 只，看会不会比全买更好。
            - 评分画像：给不同指标不同权重。比如更看重放量，或者更看重位置低不低。
            - Walk-forward：更严格的复盘方式。它只用更早的数据挑规则，再拿后面的日期验证。

            为什么会跑很久：每多选一个持有期、入场模式、风控版本，电脑就要重新把分钟数据
            从头扫一遍。topN 和评分画像比较便宜，但选很多也会增加后处理时间。

            建议先小跑：入场模式选 1-2 个，风控版本选 1-2 个，持有期只选 1/2/3，
            topN 选 1/2/3，评分画像选 1-2 个，Walk-forward 先设 0。看到有希望的方向后，
            再把范围扩大。当前默认值就是这套“先小跑”配置（持有期 1/2/3 +
            V1 基础评分 + Walk-forward 0）。

            预计超过 10 分钟的组合建议点「后台运行」：任务在独立进程里跑，
            关掉页面也不中断，进度和取消入口在「历史记录」tab 顶部。
            """
        )

    st.caption(
        f"生效的 sidebar 参数：候选池 `{preset_name}` · 区间 "
        f"{start_value:%Y-%m-%d} ~ {end_value:%Y-%m-%d} · "
        f"入场 {'、'.join(selected_mode_labels)} · 风控 {'、'.join(selected_variant_labels)}。"
        "在 sidebar 修改参数后需先点「运行对比」提交，这里才会用新参数。"
    )
    with st.form("optimize_form"):
        oc1, oc2, oc3, oc4 = st.columns([1.2, 1.1, 1.0, 1.0])
        hold_selection = oc1.multiselect(
            "持有期集合",
            list(range(1, 11)),
            default=[1, 2, 3],
            key="opt_hold_options",
        )
        topn_selection = oc2.multiselect(
            "特征 topN",
            [1, 2, 3, 5, 10],
            default=[1, 2, 3],
            key="opt_topn_options",
        )
        validation_ratio = oc3.slider(
            "验证区间占比",
            min_value=0.0,
            max_value=0.5,
            value=0.3,
            step=0.1,
            key="opt_validation_ratio",
        )
        min_trades = oc4.number_input(
            "最少交易数",
            min_value=1,
            max_value=50,
            value=5,
            step=1,
            key="opt_min_trades",
        )
        pc1, pc2 = st.columns([3.0, 1.0])
        score_profile_selection = pc1.multiselect(
            "评分画像",
            list(SCORE_PROFILE_LABELS.keys()),
            default=list(SCORE_PROFILE_LABELS.keys())[:1],
            format_func=lambda value: SCORE_PROFILE_LABELS.get(value, value),
            key="opt_score_profiles",
        )
        walk_forward_folds = pc2.number_input(
            "Walk-forward折数",
            min_value=0,
            max_value=8,
            value=0,
            step=1,
            key="opt_walk_forward_folds",
        )
        ob1, ob2 = st.columns(2)
        optimize_clicked = ob1.form_submit_button(
            "生成排行榜",
            type="primary",
            width="stretch",
            disabled=research_mode == "formal" and not n_gate_decision.allowed,
        )
        optimize_background_clicked = ob2.form_submit_button(
            "后台运行",
            width="stretch",
            disabled=research_mode == "formal" and not n_gate_decision.allowed,
        )

    opt_candidate_count = screen_candidate_count(
        start_value.isoformat(),
        end_value.isoformat(),
        preset_name,
    )
    workload = estimate_strategy_optimization_workload(
        candidate_count=opt_candidate_count or 0,
        entry_mode_count=len(selected_modes),
        profile_variant_count=len(selected_variants),
        hold_count=len(hold_selection),
        topn_count=len(topn_selection),
        score_profile_count=len(score_profile_selection),
        walk_forward_folds=int(walk_forward_folds),
    )
    wc1, wc2, wc3, wc4, wc5 = st.columns(5)
    wc1.metric("区间候选", "—" if opt_candidate_count is None else f"{opt_candidate_count:,}")
    wc2.metric("分钟 replay", f"{workload.replay_runs:,} 次")
    wc3.metric("候选扫描量", f"{workload.replay_candidate_passes:,}")
    wc4.metric("topN 组合", f"{workload.topn_combinations:,}")
    wc5.metric("预计耗时", workload.estimated_label)
    st.caption(
        "粗估公式：训练/验证 replay 扫描一次全区间候选；开启 Walk-forward 后再加一次全区间扫描。"
        "命中 Streamlit 缓存、分钟覆盖不足或机器负载变化时，实际耗时会有偏差。"
    )
    if workload.estimated_seconds >= 1800:
        st.warning("当前组合预计超过 30 分钟，建议先缩小持有期、评分画像或关闭 Walk-forward。")
    elif workload.estimated_seconds >= 600:
        st.info("当前组合预计超过 10 分钟，适合放后台跑；快速探索时建议先减少组合数。")

    if not hold_selection:
        st.warning("至少选择一个持有期")
    elif not topn_selection:
        st.warning("至少选择一个特征 topN")
    elif not score_profile_selection:
        st.warning("至少选择一个评分画像")
    elif optimize_background_clicked:
        # 重复提交防护：已有同类型任务在跑时不再派生（连点会产生多个全量优化进程）
        _running_same_type = [
            item
            for item in list_run_statuses()
            if item.get("state") == "running" and item.get("run_type") == "n_shape_optimize"
        ]
        if _running_same_type:
            st.warning(
                f"已有同类型后台任务运行中：`{_running_same_type[0]['run_id']}`，"
                "本次未启动新任务。可在「历史记录」tab 顶部取消它，或等它跑完再提交。"
            )
        else:
            bg_run_id = launch_background_run({
                "run_type": "n_shape_optimize",
                "params": {
                    "start_date": start_value.isoformat(),
                    "end_date": end_value.isoformat(),
                    "preset_name": preset_name,
                    "entry_modes": list(selected_modes),
                    "profile_variants": list(selected_variants),
                    "hold_options": [int(v) for v in hold_selection],
                    "topn_options": [int(v) for v in topn_selection],
                    "score_profile_names": list(score_profile_selection),
                    "walk_forward_folds": int(walk_forward_folds),
                    "validation_ratio": float(validation_ratio),
                    "min_trades": int(min_trades),
                    "research_gate": n_gate_request.model_dump(mode="json"),
                },
            })
            st.success(
                f"后台任务已启动：`{bg_run_id}`。可以关闭页面；"
                "进度与取消入口在「历史记录」tab 顶部，跑完结果自动进历史记录。"
            )
    elif optimize_clicked:
        with st.spinner("正在自动枚举并排序策略组合..."):
            (
                rankings,
                opt_trades,
                topn_rankings,
                topn_trades,
                walk_forward_rankings,
                walk_forward_trades,
            ) = _run_with_countdown(
                "自动优化",
                workload.estimated_seconds,
                lambda: strategy_optimize_cached(
                    start_value.isoformat(),
                    end_value.isoformat(),
                    selected_modes,
                    selected_variants,
                    tuple(int(v) for v in hold_selection),
                    tuple(int(v) for v in topn_selection),
                    tuple(score_profile_selection),
                    int(walk_forward_folds),
                    preset_name,
                    float(validation_ratio),
                    int(min_trades),
                ),
            )
        saved_run = save_strategy_lab_run(
            build_strategy_lab_run(
                run_type="n_shape_optimize",
                title=f"N字自动优化 {start_value:%Y-%m-%d} 至 {end_value:%Y-%m-%d}",
                params={
                    "候选池": preset_name,
                    "开始日期": start_value,
                    "结束日期": end_value,
                    "入场模式": list(selected_modes),
                    "风控版本": list(selected_variants),
                    "持有期集合": [int(v) for v in hold_selection],
                    "特征topN": [int(v) for v in topn_selection],
                    "评分画像": list(score_profile_selection),
                    "验证区间占比": float(validation_ratio),
                    "最少交易数": int(min_trades),
                    "Walk-forward折数": int(walk_forward_folds),
                },
                metrics={
                    "区间候选": opt_candidate_count,
                    "分钟replay次数": workload.replay_runs,
                    "候选扫描量": workload.replay_candidate_passes,
                    "topN组合": workload.topn_combinations,
                    "Walk-forward topN组合": workload.walk_forward_topn_combinations,
                    "预计耗时秒": workload.estimated_seconds,
                    "策略排行数": len(rankings),
                    "交易样本数": len(opt_trades),
                    "topN排行数": len(topn_rankings),
                    "Walk-forward排行数": len(walk_forward_rankings),
                },
                tables={
                    "策略排行": rankings,
                    "自动优化交易样本": opt_trades,
                    "特征topN排行": topn_rankings,
                    "特征topN入选样本": topn_trades,
                    "Walk-forward排行": walk_forward_rankings,
                    "Walk-forward入选样本": walk_forward_trades,
                },
                manifest=build_gate_research_manifest(
                    n_gate_request,
                    n_gate_decision,
                ),
            )
        )
        st.session_state["optimize_result"] = {
            "rankings": rankings,
            "trades": opt_trades,
            "topn_rankings": topn_rankings,
            "topn_trades": topn_trades,
            "walk_forward_rankings": walk_forward_rankings,
            "walk_forward_trades": walk_forward_trades,
            "walk_forward_folds": int(walk_forward_folds),
            "run_id": saved_run.run_id,
            "title": saved_run.title,
            "markdown": saved_run.markdown,
            "markdown_path": str(saved_run.markdown_path),
            "json_path": str(saved_run.json_path),
        }

    optimize_state = st.session_state.get("optimize_result")
    if optimize_state is None:
        st.info(
            "暂无自动优化结果。设置参数后点「生成排行榜」前台运行；"
            "「后台运行」的结果在「历史记录」tab 查看。"
        )
    if optimize_state is not None:
        rankings = optimize_state["rankings"]
        opt_trades = optimize_state["trades"]
        topn_rankings = optimize_state["topn_rankings"]
        topn_trades = optimize_state["topn_trades"]
        walk_forward_rankings = optimize_state["walk_forward_rankings"]
        walk_forward_trades = optimize_state["walk_forward_trades"]
        st.success(
            f"研究记录已保存：{optimize_state['run_id']}（切 tab 不丢；重跑覆盖显示）"
        )
        st.download_button(
            "导出本次结果 Markdown",
            data=optimize_state["markdown"],
            file_name=f"{optimize_state['run_id']}.md",
            mime="text/markdown",
            width="stretch",
            key=f"download_{optimize_state['run_id']}",
        )
        st.caption(
            f"Markdown: {optimize_state['markdown_path']} · "
            f"JSON: {optimize_state['json_path']}"
        )
        if rankings.empty:
            st.info("没有可排序的策略结果")
        else:
            top = rankings.head(20).copy()
            _show_dataframe(top)
            chart = (
                alt.Chart(top)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("candidate_id:N", title=None, sort=None, axis=alt.Axis(labelAngle=-30)),
                    y=alt.Y("robust_score:Q", title="稳健分"),
                    color=alt.value("#0f766e"),
                    tooltip=[
                        alt.Tooltip("rank:Q", title="排名"),
                        alt.Tooltip("candidate_id:N", title="策略"),
                        alt.Tooltip("train_mean_ret_pct:Q", title="训练均值%"),
                        alt.Tooltip("test_mean_ret_pct:Q", title="验证均值%"),
                        alt.Tooltip("train_trades:Q", title="训练交易"),
                        alt.Tooltip("test_trades:Q", title="验证交易"),
                    ],
                )
                .properties(height=260)
            )
            st.altair_chart(chart, width="stretch")
            with st.expander("自动优化交易样本", expanded=False):
                if opt_trades.empty:
                    st.info("暂无交易样本")
                else:
                    _show_dataframe(opt_trades)
        st.markdown("### 特征 topN 排行")
        if topn_rankings.empty:
            st.info("没有可排序的特征 topN 结果")
        else:
            topn_top = topn_rankings.head(20).copy()
            _show_dataframe(_display_topn_rankings(topn_top))
            topn_chart = (
                alt.Chart(topn_top)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("candidate_id:N", title=None, sort=None, axis=alt.Axis(labelAngle=-30)),
                    y=alt.Y("robust_score:Q", title="稳健分"),
                    color=alt.Color("selection:N", title="topN"),
                    tooltip=[
                        alt.Tooltip("rank:Q", title="排名"),
                        alt.Tooltip("candidate_id:N", title="组合"),
                        alt.Tooltip("score_profile_label:N", title="评分画像"),
                        alt.Tooltip("selection:N", title="特征选择"),
                        alt.Tooltip("train_mean_ret_pct:Q", title="训练均值%"),
                        alt.Tooltip("train_mean_delta_vs_all_pct:Q", title="训练较全量%"),
                        alt.Tooltip("test_mean_ret_pct:Q", title="验证均值%"),
                        alt.Tooltip("test_mean_delta_vs_all_pct:Q", title="验证较全量%"),
                        alt.Tooltip("test_trades:Q", title="验证交易"),
                    ],
                )
                .properties(height=260)
            )
            st.altair_chart(topn_chart, width="stretch")
            with st.expander("特征 topN 入选样本", expanded=False):
                if topn_trades.empty:
                    st.info("暂无 topN 样本")
                else:
                    _show_dataframe(_display_trades(topn_trades))
        if int(optimize_state["walk_forward_folds"]) > 0:
            st.markdown("### Walk-forward 出样本排行")
            if walk_forward_rankings.empty:
                st.info("没有可排序的 walk-forward 结果")
            else:
                wf_top = walk_forward_rankings.head(20).copy()
                _show_dataframe(_display_walk_forward_rankings(wf_top))
                wf_chart = (
                    alt.Chart(wf_top)
                    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                    .encode(
                        x=alt.X(
                            "candidate_id:N",
                            title=None,
                            sort=None,
                            axis=alt.Axis(labelAngle=-30),
                        ),
                        y=alt.Y("robust_score:Q", title="出样本稳健分"),
                        color=alt.Color("score_profile_label:N", title="评分画像"),
                        tooltip=[
                            alt.Tooltip("rank:Q", title="排名"),
                            alt.Tooltip("candidate_id:N", title="组合"),
                            alt.Tooltip("folds:Q", title="验证折数"),
                            alt.Tooltip("test_trades:Q", title="验证交易"),
                            alt.Tooltip("test_mean_ret_pct:Q", title="验证均值%"),
                            alt.Tooltip("test_win_rate_pct:Q", title="验证胜率%"),
                        ],
                    )
                    .properties(height=260)
                )
                st.altair_chart(wf_chart, width="stretch")
                with st.expander("Walk-forward 入选样本", expanded=False):
                    if walk_forward_trades.empty:
                        st.info("暂无 walk-forward 样本")
                    else:
                        _show_dataframe(_display_trades(walk_forward_trades))

with tab_auction:
    st.caption(
        f"生效的 sidebar 区间：{start_value:%Y-%m-%d} ~ {end_value:%Y-%m-%d}"
        "（本 tab 是全市场策略，不受候选池影响）。"
        "在 sidebar 修改区间后需先点「运行对比」提交，这里才会用新区间。"
    )
    with st.form("auction_gap_form"):
        ac1, ac2, ac3, ac4, ac5 = st.columns([1.0, 1.2, 1.0, 1.0, 1.1])
        auction_gap_mode = ac1.selectbox(
            "跳空口径",
            ["close", "strict_high"],
            format_func=lambda value: "高于昨收" if value == "close" else "高于昨高",
            key="auction_gap_mode",
        )
        auction_st_filter = ac2.selectbox(
            "ST过滤",
            ["case_insensitive", "literal_lower", "none"],
            format_func=lambda value: {
                "case_insensitive": "严格过滤ST/*ST",
                "literal_lower": "近似同花顺小写st",
                "none": "不过滤",
            }.get(value, value),
            key="auction_st_filter",
        )
        auction_min_ratio = ac3.number_input(
            "量比下限",
            min_value=0.0,
            max_value=5.0,
            value=0.15,
            step=0.05,
            key="auction_min_ratio",
        )
        auction_max_ratio = ac4.number_input(
            "量比上限",
            min_value=0.1,
            max_value=10.0,
            value=5.0,
            step=0.1,
            key="auction_max_ratio",
        )
        auction_hold_days = ac5.number_input(
            "持有交易日",
            min_value=1,
            max_value=10,
            value=1,
            step=1,
            key="auction_hold_days",
        )
        auction_gate_request, auction_gate_decision = current_research_gate(
            research_mode,
            "auction_gap",
            start_value,
            end_value,
        )
        run_auction_clicked = st.form_submit_button(
            "运行集合竞价策略",
            type="primary",
            width="stretch",
            disabled=(
                research_mode == "formal" and not auction_gate_decision.allowed
            ),
        )

    auction_gate_request = auction_gate_request.model_copy(
        update={
            "audit_run_id": auction_gate_decision.audit_run_id,
            "dataset_snapshot_id": auction_gate_decision.dataset_snapshot_id,
        }
    )
    if research_mode == "formal":
        _render_research_gate(auction_gate_decision)

    auction_candidate_count = auction_candidate_count_cached(
        start_value.isoformat(),
        end_value.isoformat(),
    )
    auction_workload = estimate_growth_board_workload(
        candidate_count=auction_candidate_count,
        variant_count=2,
        seconds_per_candidate_pass=0.01,
        seconds_per_variant=0.5,
    )
    aw1, aw2, aw3 = st.columns(3)
    aw1.metric("粗估竞价行", f"{auction_candidate_count:,}")
    aw2.metric("预计扫描", f"{auction_workload.candidate_passes:,}")
    aw3.metric("预计耗时", auction_workload.estimated_label)

    if run_auction_clicked:
        with st.spinner("正在回放集合竞价跳空策略..."):
            auction_metrics, auction_baseline, auction_minute_trades = (
                _run_with_countdown(
                    "集合竞价回放",
                    auction_workload.estimated_seconds,
                    lambda: auction_gap_minute_cached(
                        start_value.isoformat(),
                        end_value.isoformat(),
                        str(auction_gap_mode),
                        str(auction_st_filter),
                        float(auction_min_ratio),
                        float(auction_max_ratio),
                        int(auction_hold_days),
                    ),
                )
            )
        baseline_row = auction_metrics.iloc[0]
        minute_row = auction_metrics.iloc[1]
        saved_run = save_strategy_lab_run(
            build_strategy_lab_run(
                run_type="auction_gap",
                title=f"集合竞价跳空 {start_value:%Y-%m-%d} 至 {end_value:%Y-%m-%d}",
                params={
                    "开始日期": start_value,
                    "结束日期": end_value,
                    "跳空口径": str(auction_gap_mode),
                    "ST过滤": str(auction_st_filter),
                    "量比下限": float(auction_min_ratio),
                    "量比上限": float(auction_max_ratio),
                    "持有交易日": int(auction_hold_days),
                },
                metrics={
                    "竞价候选": baseline_row["候选"],
                    "竞价当日上板率%": baseline_row["当日上板率%"],
                    "竞价当日最高均值%": baseline_row["当日最高均值%"],
                    "竞价当日收盘均值%": baseline_row["当日收盘均值%"],
                    "竞价次开平均收益%": baseline_row["平均收益%"],
                    "竞价次开胜率%": baseline_row["胜率%"],
                    "分钟B交易": minute_row["交易"],
                    "分钟B触发率%": minute_row["触发率%"],
                    "分钟B当日上板率%": minute_row["当日上板率%"],
                    "分钟B/S平均收益%": minute_row["平均收益%"],
                    "分钟B/S胜率%": minute_row["胜率%"],
                    "分钟B/S弱竞价退出%": minute_row["弱竞价退出%"],
                },
                tables={
                    "策略对比": auction_metrics,
                    "分钟B/S明细": auction_minute_trades,
                    "竞价基准明细": auction_baseline,
                },
                manifest=build_gate_research_manifest(
                    auction_gate_request,
                    auction_gate_decision,
                ),
            )
        )
        st.session_state["auction_result"] = {
            "metrics": auction_metrics,
            "baseline": auction_baseline,
            "minute_trades": auction_minute_trades,
            "run_id": saved_run.run_id,
            "title": saved_run.title,
            "markdown": saved_run.markdown,
            "markdown_path": str(saved_run.markdown_path),
            "json_path": str(saved_run.json_path),
        }

    auction_state = st.session_state.get("auction_result")
    if auction_state is None:
        st.info("选择参数后运行集合竞价策略")
    else:
        auction_metrics = auction_state["metrics"]
        auction_baseline = auction_state["baseline"]
        auction_minute_trades = auction_state["minute_trades"]
        baseline_row = auction_metrics.iloc[0]
        minute_row = auction_metrics.iloc[1]
        st.caption(
            f"当前结果：{auction_state['title']} · 记录 `{auction_state['run_id']}`"
            "（已落盘，切 tab 不丢；重跑覆盖显示）"
        )

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("竞价候选", int(minute_row["候选"]))
        m2.metric(
            "当日上板率",
            "—"
            if pd.isna(baseline_row["当日上板率%"])
            else f"{baseline_row['当日上板率%']:.1f}%",
        )
        m3.metric(
            "竞价→次开均值",
            "—"
            if pd.isna(baseline_row["平均收益%"])
            else f"{baseline_row['平均收益%']:.2f}%",
        )
        m4.metric(
            "分钟B/S均值",
            "—"
            if pd.isna(minute_row["平均收益%"])
            else f"{minute_row['平均收益%']:.2f}%",
        )

        st.download_button(
            "导出本次结果 Markdown",
            data=auction_state["markdown"],
            file_name=f"{auction_state['run_id']}.md",
            mime="text/markdown",
            width="stretch",
            key=f"download_{auction_state['run_id']}",
        )
        st.caption(
            f"Markdown: {auction_state['markdown_path']} · JSON: {auction_state['json_path']}"
        )

        _show_dataframe(auction_metrics)
        auction_chart_df = auction_metrics.rename(columns={"平均收益%": "mean_ret_pct"})
        chart = (
            alt.Chart(auction_chart_df)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("策略:N", title=None, sort=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("mean_ret_pct:Q", title="平均收益%"),
                color=alt.condition(
                    alt.datum.mean_ret_pct >= 0,
                    alt.value("#0f766e"),
                    alt.value("#b91c1c"),
                ),
                tooltip=[
                    alt.Tooltip("策略:N", title="策略"),
                    alt.Tooltip("候选:Q", title="候选"),
                    alt.Tooltip("交易:Q", title="交易"),
                    alt.Tooltip("触发率%:Q", title="触发率%"),
                    alt.Tooltip("当日上板率%:Q", title="当日上板率%"),
                    alt.Tooltip("当日最高均值%:Q", title="当日最高均值%"),
                    alt.Tooltip("当日收盘均值%:Q", title="当日收盘均值%"),
                    alt.Tooltip("mean_ret_pct:Q", title="平均收益%"),
                    alt.Tooltip("中位收益%:Q", title="中位收益%"),
                    alt.Tooltip("胜率%:Q", title="胜率%"),
                    alt.Tooltip("弱竞价退出%:Q", title="弱竞价退出%"),
                ],
            )
            .properties(height=240)
        )
        st.altair_chart(chart, width="stretch")

        detail_tab, baseline_tab = st.tabs(["分钟B/S明细", "竞价基准明细"])
        with detail_tab:
            if auction_minute_trades.empty:
                st.info("所选区间没有分钟 B 交易")
            else:
                _show_dataframe(auction_minute_trades)
        with baseline_tab:
            if auction_baseline.empty:
                st.info("所选区间没有竞价候选")
            else:
                cols = [
                    "signal_date", "ts_code", "name", "entry_price",
                    "auction_vol_ratio_5d", "gap_pct_close", "entry_to_limit_up_pct",
                    "hit_limit_up_today", "next_open_ret_pct",
                ]
                _show_dataframe(
                    auction_baseline[[c for c in cols if c in auction_baseline.columns]]
                )

with tab_growth:
    growth_safe_end = safe_replay_end_date(calendar, calendar[-1], max_hold_days=10)
    if growth_safe_end is None:
        st.info("可用交易日不足，暂时无法回放科创/创业放量策略")
    else:
        st.info(
            "左侧“候选池”只影响 N字策略的收益对比、自动优化、交易明细、价量分布和退出原因。"
            "本 tab 是独立的全市场策略：从科创板/创业板里按条件找票，不读取 pool1/pool2。"
        )
        with st.expander("使用说明书：这些参数到底是什么意思", expanded=False):
            st.markdown(
                """
                这套策略可以理解成：先在全市场里找“科创/创业板、不是 ST、不是一字板、均线多头”的票，
                然后等盘中出现真正放量，再在下一分钟模拟买入。

                - 分时基准天数：拿过去多少个交易日的 1 分钟数据做参照。
                  比如 20 天，就是比较过去 20 天。
                - 最少基准日：历史分钟数据至少有多少天才允许判断，避免样本太少。
                - 累计放量倍：截至当前分钟，今天累计成交额 / 过去 N 天同一时刻累计成交额中位数。
                  它回答“今天到现在是不是整体明显更热”。
                - 同刻放量倍：当前这一分钟成交额 / 过去 N 天同一时刻这一分钟成交额中位数。
                  它回答“这一分钟是不是比历史同一时间突然更强”。
                - 5分加速度：当前这一分钟成交额 / 当天前 5 个普通分钟成交额中位数。
                  它回答“刚刚是不是突然提速”。9:30-9:32 单独视为开盘段，不参与这个滚动比较。
                - VWAP：当天到当前为止的成交额 / 成交量，可以近似理解成“今天已成交资金的平均成本”。
                  要求强于 VWAP，就是避免价格还压在当日平均成本下方时追进去。

                消融对比的读法：每次只拿掉一个过滤条件，观察交易数、平均收益、胜率怎么变。
                如果拿掉某个条件后收益变差，说明它可能是有效过滤；如果拿掉后收益反而变好，
                说明它可能太保守或方向不对。
                """
            )
        growth_default_end = growth_safe_end
        growth_default_start = max(calendar[0], growth_default_end - timedelta(days=30))
        ablation_specs = growth_board_ablation_specs()
        spec_labels = [spec.label for spec in ablation_specs]
        spec_by_label = {spec.label: spec for spec in ablation_specs}
        with st.form("growth_form"):
            gd1, gd2, gd3 = st.columns([1.0, 1.0, 1.0])
            growth_start = gd1.date_input(
                "开始日期",
                value=growth_default_start,
                min_value=calendar[0],
                max_value=growth_safe_end,
                key="growth_start_date",
            )
            growth_end = gd2.date_input(
                "结束日期",
                value=growth_default_end,
                min_value=calendar[0],
                max_value=growth_safe_end,
                key="growth_end_date",
            )
            growth_hold_days = gd3.number_input(
                "持有交易日",
                min_value=1,
                max_value=10,
                value=1,
                step=1,
                key="growth_hold_days",
            )
            gf1, gf2, gf3, gf4, gf5 = st.columns([1.0, 1.0, 1.0, 1.0, 1.0])
            growth_lookback = gf1.number_input(
                "分时基准天数",
                min_value=5,
                max_value=90,
                value=20,
                step=5,
                key="growth_lookback_days",
            )
            growth_min_hist = gf2.number_input(
                "最少基准日",
                min_value=3,
                max_value=90,
                value=10,
                step=1,
                key="growth_min_hist_days",
            )
            growth_min_cum = gf3.number_input(
                "累计放量倍",
                min_value=0.5,
                max_value=10.0,
                value=1.4,
                step=0.1,
                key="growth_min_cum_amount_ratio",
            )
            growth_min_same = gf4.number_input(
                "同刻放量倍",
                min_value=0.5,
                max_value=20.0,
                value=2.0,
                step=0.1,
                key="growth_min_same_minute_ratio",
            )
            growth_min_accel = gf5.number_input(
                "5分加速度",
                min_value=0.5,
                max_value=20.0,
                value=2.0,
                step=0.1,
                key="growth_min_accel_5m",
            )
            growth_require_vwap = st.checkbox(
                "单策略：要求信号价强于当日 VWAP",
                value=True,
                key="growth_require_vwap",
            )
            growth_run_mode = st.radio(
                "运行模式",
                ["单策略回测", "消融对比"],
                horizontal=True,
                key="growth_run_mode",
            )
            growth_ablation_labels = st.multiselect(
                "消融版本",
                spec_labels,
                default=spec_labels,
                key="growth_ablation_labels",
            )
            growth_gate_request, growth_gate_decision = current_research_gate(
                research_mode,
                "growth_board_surge",
                growth_start,
                growth_end,
            )
            st.caption("单策略回测只使用上方参数；消融对比会按所选版本自动改写过滤条件。")
            run_growth_clicked = st.form_submit_button(
                "运行科创/创业放量策略",
                type="primary",
                width="stretch",
                disabled=(
                    research_mode == "formal" and not growth_gate_decision.allowed
                ),
            )

        growth_gate_request = growth_gate_request.model_copy(
            update={
                "audit_run_id": growth_gate_decision.audit_run_id,
                "dataset_snapshot_id": growth_gate_decision.dataset_snapshot_id,
            }
        )
        if research_mode == "formal":
            _render_research_gate(growth_gate_decision)

        st.caption(
            "当前可回测部分：过滤 ST/一字板、科创/创业板、均线多头排列、分钟级爆量 B、"
            "T+1 起按止盈止损/持有期 S。外盘/内盘与大单净量缺少可回放的历史订单流数据，"
            "暂不参与过滤。"
        )

        growth_spec_keys: tuple[str, ...] = ()
        if growth_start > growth_end:
            st.warning("科创/创业放量策略的开始日期不能晚于结束日期")
        else:
            if growth_run_mode == "消融对比":
                selected_ablation_labels = list(growth_ablation_labels)
                if "完整策略" not in selected_ablation_labels:
                    selected_ablation_labels.insert(0, "完整策略")
                growth_spec_keys = tuple(
                    spec_by_label[label].key
                    for label in selected_ablation_labels
                    if label in spec_by_label
                )
            else:
                growth_spec_keys = ("full",)

            growth_candidate_count = growth_board_candidate_count_cached(
                growth_start.isoformat(),
                growth_end.isoformat(),
            )
            growth_workload = estimate_growth_board_workload(
                candidate_count=growth_candidate_count,
                variant_count=len(growth_spec_keys),
            )
            gw1, gw2, gw3, gw4 = st.columns(4)
            gw1.metric("粗估候选", f"{growth_candidate_count:,}")
            gw2.metric("策略版本", len(growth_spec_keys))
            gw3.metric("预计扫描", f"{growth_workload.candidate_passes:,}")
            gw4.metric("预计耗时", growth_workload.estimated_label)

        if growth_start > growth_end:
            pass
        elif not growth_spec_keys:
            st.warning("至少选择一个消融版本")
        elif run_growth_clicked:
            with st.spinner("正在回放科创/创业板分钟放量策略..."):
                if growth_run_mode == "消融对比":
                    growth_metrics, growth_trades = _run_with_countdown(
                        "科创/创业放量消融",
                        growth_workload.estimated_seconds,
                        lambda: growth_board_ablation_cached(
                            growth_start.isoformat(),
                            growth_end.isoformat(),
                            int(growth_lookback),
                            int(growth_min_hist),
                            float(growth_min_cum),
                            float(growth_min_same),
                            float(growth_min_accel),
                            int(growth_hold_days),
                            growth_spec_keys,
                        ),
                    )
                else:
                    growth_metrics, growth_trades = _run_with_countdown(
                        "科创/创业放量回放",
                        growth_workload.estimated_seconds,
                        lambda: growth_board_surge_cached(
                            growth_start.isoformat(),
                            growth_end.isoformat(),
                            int(growth_lookback),
                            int(growth_min_hist),
                            float(growth_min_cum),
                            float(growth_min_same),
                            float(growth_min_accel),
                            int(growth_hold_days),
                            bool(growth_require_vwap),
                            True,
                            True,
                        ),
                    )

            row = growth_metrics.iloc[0]
            saved_run = save_strategy_lab_run(
                build_strategy_lab_run(
                    run_type="growth_board_surge",
                    title=f"科创/创业放量 {growth_start:%Y-%m-%d} 至 {growth_end:%Y-%m-%d}",
                    params={
                        "开始日期": growth_start,
                        "结束日期": growth_end,
                        "持有交易日": int(growth_hold_days),
                        "分时基准天数": int(growth_lookback),
                        "最少基准日": int(growth_min_hist),
                        "累计放量倍": float(growth_min_cum),
                        "同刻放量倍": float(growth_min_same),
                        "5分加速度": float(growth_min_accel),
                        "要求强于VWAP": bool(growth_require_vwap),
                        "运行模式": growth_run_mode,
                        "消融版本": list(growth_spec_keys),
                    },
                    metrics={
                        "触发交易": row["交易"],
                        "当日上板率%": row["当日上板率%"],
                        "平均收益%": row["平均收益%"],
                        "中位收益%": row["中位收益%"],
                        "胜率%": row["胜率%"],
                    },
                    tables={
                        "策略汇总": growth_metrics,
                        "交易明细": growth_trades,
                    },
                    manifest=build_gate_research_manifest(
                        growth_gate_request,
                        growth_gate_decision,
                    ),
                )
            )
            st.session_state["growth_result"] = {
                "metrics": growth_metrics,
                "trades": growth_trades,
                "run_id": saved_run.run_id,
                "title": saved_run.title,
                "markdown": saved_run.markdown,
                "markdown_path": str(saved_run.markdown_path),
                "json_path": str(saved_run.json_path),
            }

        growth_state = st.session_state.get("growth_result")
        if growth_state is None:
            st.info("选择参数后运行科创/创业放量策略")
        else:
            growth_metrics = growth_state["metrics"]
            growth_trades = growth_state["trades"]
            row = growth_metrics.iloc[0]
            st.caption(
                f"当前结果：{growth_state['title']} · 记录 `{growth_state['run_id']}`"
                "（已落盘，切 tab 不丢；重跑覆盖显示）"
            )
            gk1, gk2, gk3, gk4 = st.columns(4)
            gk1.metric("触发交易", int(row["交易"]))
            gk2.metric(
                "当日上板率",
                "—" if pd.isna(row["当日上板率%"]) else f"{row['当日上板率%']:.1f}%",
            )
            gk3.metric(
                "平均收益",
                "—" if pd.isna(row["平均收益%"]) else f"{row['平均收益%']:.2f}%",
            )
            gk4.metric(
                "胜率",
                "—" if pd.isna(row["胜率%"]) else f"{row['胜率%']:.1f}%",
            )
            st.download_button(
                "导出本次结果 Markdown",
                data=growth_state["markdown"],
                file_name=f"{growth_state['run_id']}.md",
                mime="text/markdown",
                width="stretch",
                key=f"download_{growth_state['run_id']}",
            )
            st.caption(
                f"Markdown: {growth_state['markdown_path']} · "
                f"JSON: {growth_state['json_path']}"
            )

            _show_dataframe(growth_metrics)
            if growth_trades.empty:
                st.info("所选区间没有触发交易")
            else:
                _show_dataframe(_display_growth_board_trades(growth_trades))

with tab_coverage:
    if overview is not None and not overview.empty:
        row = overview.iloc[0]
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("分钟线行数", f"{int(row['rows_count']):,}")
        d2.metric("覆盖股票", f"{int(row['codes_count']):,}")
        d3.metric("交易日", f"{int(row['trade_dates']):,}")
        d4.metric(
            "时间范围",
            f"{row['min_time']:%m-%d} → {row['max_time']:%m-%d}"
            if pd.notna(row["min_time"]) else "—",
        )
        if source_overview is not None and not source_overview.empty:
            with st.expander("分钟数据来源拆分", expanded=False):
                _show_dataframe(source_overview, max_rows=50)
    st.caption(
        f"生效的 sidebar 参数：候选池 `{preset_name}` · 区间 "
        f"{start_value:%Y-%m-%d} ~ {end_value:%Y-%m-%d}。"
        "在 sidebar 修改参数后需先点「运行对比」提交，这里才会用新参数。"
    )
    if st.button("计算当前区间覆盖率", width="stretch"):
        coverage_candidate_count = screen_candidate_count(
            start_value.isoformat(),
            end_value.isoformat(),
            preset_name,
        ) or 0
        coverage_estimated_seconds = max(1.0, coverage_candidate_count * 0.02)
        with st.spinner("正在计算 90 日价量分布覆盖率..."):
            ok, total, missing_df, samples_df = _run_with_countdown(
                "90日价量覆盖率",
                coverage_estimated_seconds,
                lambda: profile_coverage_cached(
                    start_value.isoformat(),
                    end_value.isoformat(),
                    preset_name,
                    90,
                ),
            )
        rate = ok / total * 100 if total else 0
        st.metric("90日价量分布覆盖率", f"{rate:.2f}%", delta=f"{ok}/{total}")
        if not missing_df.empty:
            _show_dataframe(missing_df)
        if not samples_df.empty:
            _show_dataframe(samples_df)

with tab_exits:
    compare_state = st.session_state.get("compare_result")
    if compare_state is None or compare_state["trades"].empty:
        st.info("暂无退出统计")
    else:
        st.caption(f"来自：{compare_state['title']} · 记录 `{compare_state['run_id']}`")
        exits = (
            compare_state["trades"].groupby(["entry_mode", "profile_variant", "exit_reason"])
            .agg(n=("ts_code", "count"), mean_ret=("ret_pct", "mean"))
            .reset_index()
        )
        exits["entry_mode"] = exits["entry_mode"].map(ENTRY_LABELS)
        exits["profile_variant"] = exits["profile_variant"].map(VARIANT_LABELS)
        _show_dataframe(
            exits.rename(columns={
                "entry_mode": "入场模式",
                "profile_variant": "风控版本",
                "exit_reason": "退出原因",
                "n": "次数",
                "mean_ret": "平均收益%",
            })
        )

with tab_interfaces:
    catalog = tushare_interface_catalog_cached()
    if catalog.empty:
        st.info("暂无 Tushare 接口目录，请先运行 scripts/audit_tushare_docs.py")
    else:
        goods = tushare_purchase_goods_cached()
        packages = tushare_activity_packages_cached()
        current_points = None
        if "current_points" in catalog.columns:
            point_values = catalog["current_points"].dropna()
            if not point_values.empty:
                current_points = float(point_values.iloc[0])

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("接口总数", f"{len(catalog):,}")
        c2.metric("盘中/竞价", int((catalog["integration_stage"] == "stage_1_realtime").sum()))
        c3.metric(
            "策略特征",
            int((catalog["integration_stage"] == "stage_2_strategy_features").sum()),
        )
        c4.metric(
            "当前积分",
            f"{current_points:,.0f}" if current_points is not None else "未同步",
        )
        c5.metric(
            "独立权限报价",
            int((goods["good_type"] == 2).sum()) if not goods.empty else 0,
        )

        f1, f2, f3, f4 = st.columns([1.1, 1.0, 1.0, 1.4])
        stage_labels = list(TUSHARE_STAGE_LABELS.values())
        selected_stage_labels = f1.multiselect(
            "接入阶段",
            stage_labels,
            default=stage_labels,
            key="tushare_stage_filter",
        )
        status_labels = list(TUSHARE_STATUS_LABELS.values())
        selected_status_labels = f2.multiselect(
            "接入状态",
            status_labels,
            default=status_labels,
            key="tushare_status_filter",
        )
        permission_labels = list(TUSHARE_PERMISSION_LABELS.values())
        selected_permission_labels = f3.multiselect(
            "权限",
            permission_labels,
            default=permission_labels,
            key="tushare_permission_filter",
        )
        tag_options = sorted({
            tag
            for value in catalog["capability_tags"].tolist()
            for tag in _json_list(value)
        })
        selected_tags = f4.multiselect(
            "能力标签",
            tag_options,
            default=[],
            key="tushare_tag_filter",
        )

        stage_keys = {
            key for key, label in TUSHARE_STAGE_LABELS.items()
            if label in selected_stage_labels
        }
        status_keys = {
            key for key, label in TUSHARE_STATUS_LABELS.items()
            if label in selected_status_labels
        }
        permission_keys = {
            key for key, label in TUSHARE_PERMISSION_LABELS.items()
            if label in selected_permission_labels
        }

        filtered = catalog[
            catalog["integration_stage"].isin(stage_keys)
            & catalog["integration_status"].isin(status_keys)
            & catalog["permission_level"].isin(permission_keys)
        ].copy()
        if selected_tags:
            selected_tag_set = set(selected_tags)
            filtered = filtered[
                filtered["capability_tags"].apply(
                    lambda value: bool(selected_tag_set & set(_json_list(value)))
                )
            ]

        _show_dataframe(
            format_tushare_catalog_display(filtered),
            max_rows=300,
        )

        if not goods.empty:
            with st.expander("购买页报价原表", expanded=False):
                goods_display = goods.copy()
                goods_display["类型"] = goods_display["good_type"].map({
                    1: "积分购买",
                    2: "独立权限",
                }).fillna(goods_display["good_type"])
                goods_display["覆盖API"] = goods_display["api_names"].apply(
                    lambda value: "、".join(_json_list(value))
                )

                def _goods_quote(row: pd.Series) -> str:
                    if pd.isna(row.get("price")):
                        return ""
                    price = float(row["price"])
                    price_text = f"¥{price:,.0f}" if price.is_integer() else f"¥{price:,.2f}"
                    if pd.isna(row.get("duration_unit")):
                        return price_text
                    return f"{price_text}/{row['duration_unit']}"

                goods_display["报价"] = goods_display.apply(_goods_quote, axis=1)
                _show_dataframe(
                    goods_display.rename(columns={
                        "good_id": "商品ID",
                        "name": "名称",
                        "description": "说明",
                        "doc_id": "doc_id",
                        "api_count": "接口数",
                        "doc_url": "文档",
                    })[
                        [
                            "类型",
                            "商品ID",
                            "名称",
                            "报价",
                            "说明",
                            "doc_id",
                            "接口数",
                            "覆盖API",
                            "文档",
                        ]
                    ],
                    max_rows=300,
                )

        if not packages.empty:
            with st.expander("套餐报价", expanded=False):
                package_display = packages.copy()
                package_display["报价"] = package_display.apply(
                    lambda row: (
                        f"¥{float(row['price']):,.0f}/{row['duration_unit']}"
                        if pd.notna(row.get("price")) and pd.notna(row.get("duration_unit"))
                        else ""
                    ),
                    axis=1,
                )
                _show_dataframe(
                    package_display.rename(columns={
                        "package_id": "套餐ID",
                        "name": "名称",
                        "description": "说明",
                    })[
                        [
                            "套餐ID",
                            "名称",
                            "报价",
                            "说明",
                        ]
                    ],
                    max_rows=300,
                )

with tab_history:
    st.markdown("### 后台任务")
    bg_statuses = list_run_statuses()
    if not bg_statuses:
        st.caption("暂无后台任务。自动优化 tab 的「后台运行」按钮会把任务放到这里。")
    else:
        if st.button("刷新后台任务状态", key="bg_status_refresh"):
            st.rerun()
        _BG_STATE_LABELS = {
            "running": "🟢 运行中",
            "done": "✅ 已完成",
            "error": "❌ 失败",
            "cancelled": "⏹ 已取消",
        }
        for bg in bg_statuses[:20]:
            bc1, bc2, bc3 = st.columns([2.8, 1.0, 1.6])
            bg_started = str(bg.get("started_at") or "")[:16].replace("T", " ")
            bc1.markdown(f"`{bg['run_id']}`")
            bc1.caption(
                f"{RUN_TYPE_LABELS.get(bg['run_type'], bg['run_type'])} · 启动 {bg_started}"
            )
            bc2.markdown(_BG_STATE_LABELS.get(bg["state"], bg["state"]))
            if bg["state"] == "running":
                if bc3.button("取消", key=f"bg_cancel_{bg['run_id']}"):
                    cancel_background_run(bg["run_id"])
                    st.rerun()
            elif bg["state"] == "done":
                bc3.caption("结果已入下方历史记录")
            elif bg.get("error"):
                bc3.caption(str(bg["error"]))

    st.markdown("### 历史记录")
    runs = list_strategy_lab_runs(limit=50)
    if not runs:
        st.info("暂无历史记录。运行一次自动优化或集合竞价跳空回测后，这里会自动出现。")
    else:
        def _history_label(index: int) -> str:
            run = runs[index]
            created = run.created_at.astimezone(CST).strftime("%m-%d %H:%M")
            run_type = RUN_TYPE_LABELS.get(run.run_type, run.run_type)
            status = RESEARCH_STATUS_LABELS[run.manifest.research_status]
            return f"{created} · [{status}] {run_type} · {run.title}"

        selected_history = st.selectbox(
            "历史记录",
            list(range(len(runs))),
            format_func=_history_label,
            key="strategy_lab_history_select",
        )
        history_run = runs[int(selected_history)]
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("记录类型", RUN_TYPE_LABELS.get(history_run.run_type, history_run.run_type))
        h2.metric(
            "可信度",
            RESEARCH_STATUS_LABELS[history_run.manifest.research_status],
        )
        h3.metric("表格数", len(history_run.tables))
        h4.metric(
            "生成时间",
            history_run.created_at.astimezone(CST).strftime("%m-%d %H:%M"),
        )
        if history_run.manifest.research_status == "exploratory":
            st.warning(history_run.manifest.status_reason)
        st.download_button(
            "下载 Markdown",
            data=history_run.markdown,
            file_name=f"{history_run.run_id}.md",
            mime="text/markdown",
            width="stretch",
            key=f"history_download_{history_run.run_id}",
        )
        st.caption(f"Markdown: {history_run.markdown_path} · JSON: {history_run.json_path}")
        with st.expander("Markdown 预览", expanded=True):
            st.markdown(history_run.markdown)
        for table in history_run.tables:
            with st.expander(f"{table.name}（{len(table.rows)}/{table.total_rows} 行）"):
                if table.rows:
                    _show_dataframe(pd.DataFrame(table.rows))
                else:
                    st.info("无数据")
