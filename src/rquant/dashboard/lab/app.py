"""Streamlit Strategy Lab backed only by the durable Job Center."""

from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Final
from uuid import UUID, uuid4

import pandas as pd
import streamlit as st

from rquant.config import settings
from rquant.dashboard.lab.job_center import (
    StrategyLabJobCenterController,
    StrategyLabSubmissionContext,
)
from rquant.dashboard.strategy_lab_data import (
    TUSHARE_PERMISSION_LABELS,
    TUSHARE_STAGE_LABELS,
    TUSHARE_STATUS_LABELS,
    dataframe_preview,
    format_tushare_catalog_display,
    growth_board_ablation_specs,
    load_tushare_activity_packages,
    load_tushare_interface_catalog,
    load_tushare_purchase_goods,
    safe_replay_end_date,
)
from rquant.dashboard.strategy_lab_runs import (
    RUN_TYPE_LABELS,
    list_strategy_lab_runs,
)
from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_job_center import (
    AuctionGapRunInput,
    CommandSubmissionResult,
    GrowthBoardSurgeRunInput,
    LabCommandSubmissionFacade,
    NShapeComparisonRunInput,
    NShapeOptimizationRunInput,
    ResearchRunInput,
)
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import (
    JobStatus,
    LabJobDetail,
    LabJobListFilters,
    LabJobReader,
)
from rquant.research_gate import (
    ResearchGateDecision,
    ResearchGateFailure,
    ResearchGateRequest,
    evaluate_store_research_gate,
    research_gate_metadata_ready,
)
from rquant.research_manifest import (
    CURRENT_RESEARCH_NOTICES,
    RESEARCH_STATUS_LABELS,
    detect_verified_code_commit,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    ResearchJobType,
    ResourceClass,
)
from rquant.storage.duckdb import open_readonly_connection, open_readonly_store
from rquant.strategy_job_adapters import (
    AuctionGapParameters,
    GrowthBoardSurgeParameters,
    NShapeCompareParameters,
    NShapeOptimizeParameters,
)
from rquant.topn_selection import default_score_profiles

CST = timezone(timedelta(hours=8))
_EXACT_SHA: Final = re.compile(r"^[0-9a-f]{40}$")
_ACTIVE_JOB_STATUSES: Final = {
    JobStatus.QUEUED,
    JobStatus.RUNNING,
    JobStatus.CHECKPOINTED,
}
_ENTRY_LABELS: Final = {
    "first_break": "第一次突破",
    "break_retest": "突破回踩确认",
    "late_confirm": "10:30 后确认",
    "vwap_confirm": "VWAP 确认",
    "amount_surge": "成交额突增",
    "factor_confirm": "多因子确认",
}
_VARIANT_LABELS: Final = {
    "baseline": "基线",
    "vp_risk_only": "90 日价量动态风控",
    "vp_90": "90 日价量过滤及风控",
}
_JOB_STATUS_LABELS: Final = {
    JobStatus.QUEUED: "排队",
    JobStatus.RUNNING: "运行中",
    JobStatus.CHECKPOINTED: "已暂停",
    JobStatus.SUCCEEDED: "成功",
    JobStatus.FAILED: "失败",
    JobStatus.CANCELLED: "已取消",
}
_JOB_TYPE_LABELS: Final = {
    ResearchJobType.STRATEGY_REPLAY: "策略回放",
    ResearchJobType.PARAMETER_SEARCH: "参数搜索",
    ResearchJobType.ABLATION: "消融实验",
}
_RESOURCE_LABELS: Final = {
    ResourceClass.INTERACTIVE: "交互",
    ResourceClass.STANDARD: "标准",
    ResourceClass.HEAVY: "重型",
}


@dataclass(frozen=True)
class _JobCenterRuntime:
    controller: StrategyLabJobCenterController
    artifact_store: LabJobArtifactStore


@dataclass(frozen=True)
class _ResearchUiSettings:
    mode: str
    code_sha: str | None
    execution_costs: ExecutionCostSpec
    random_seed: int
    resource_class: ResourceClass
    deadline_hours: int
    max_attempts: int


def _configure_page() -> None:
    st.set_page_config(
        page_title="rQuant Strategy Lab",
        page_icon=None,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
        html, body, [class*="st-"] { font-size: 13px; }
        .block-container { max-width: 1480px; padding-top: 1.1rem; }
        h1 { font-size: 1.55rem !important; margin-bottom: .2rem !important; }
        h2 { font-size: 1.05rem !important; margin-top: 1.35rem !important; }
        h3 { font-size: .9rem !important; }
        [data-testid="stMetricValue"] { font-size: 1.18rem !important; }
        [data-testid="stMetricLabel"] { color: #56616f; font-size: .72rem !important; }
        [data-testid="stDataFrame"] { border: 1px solid #dfe3e8; }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def _job_center_runtime() -> _JobCenterRuntime:
    _ensure_private_runtime_directory(settings.lab_runtime_dir_resolved)
    reader = LabJobReader(settings.lab_jobs_path_resolved)
    commands = LabCommandSubmissionFacade(
        reader=reader,
        spool=LabCommandSpool(settings.lab_job_command_dir_resolved),
    )
    artifact_root = settings.lab_final_artifact_dir_resolved
    artifact_store = LabJobArtifactStore(artifact_root)
    export_root = settings.lab_runtime_dir_resolved / "exports"
    export_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    export_root.chmod(0o700)
    controller = StrategyLabJobCenterController(
        reader=reader,
        commands=commands,
        preview_reader=ArtifactPreviewReader(
            reader=reader,
            artifact_root=artifact_root,
        ),
        zip_exports=LabJobZipExportFacade(
            reader=reader,
            artifact_store=artifact_store,
            export_root=export_root,
        ),
    )
    return _JobCenterRuntime(controller=controller, artifact_store=artifact_store)


def _ensure_private_runtime_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    observed = path.lstat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.geteuid():
        raise RuntimeError("lab runtime root must be an owned physical directory")
    if stat.S_IMODE(observed.st_mode) != 0o700:
        path.chmod(0o700, follow_symlinks=False)
        tightened = path.lstat()
        if (
            not stat.S_ISDIR(tightened.st_mode)
            or tightened.st_uid != os.geteuid()
            or stat.S_IMODE(tightened.st_mode) != 0o700
        ):
            raise RuntimeError("lab runtime root permissions could not be tightened")


@st.cache_data(ttl=300)
def _query_readonly(sql: str, params: tuple[object, ...] = ()) -> pd.DataFrame | None:
    try:
        connection = open_readonly_connection()
    except Exception:
        return None
    try:
        return connection.execute(sql, list(params)).fetchdf()
    except Exception:
        return None
    finally:
        connection.close()


@st.cache_data(ttl=300)
def _trading_calendar() -> tuple[date, ...]:
    frame = _query_readonly("SELECT DISTINCT trade_date FROM daily_bar ORDER BY trade_date")
    if frame is None or frame.empty:
        return ()
    return tuple(
        value.date() if hasattr(value, "date") else value for value in frame["trade_date"].tolist()
    )


@st.cache_data(ttl=300)
def _screen_bounds(preset_name: str) -> tuple[date | None, date | None, int]:
    if preset_name == "n-shape-combined":
        frame = _query_readonly(
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
    else:
        frame = _query_readonly(
            """
            SELECT MIN(trade_date) AS min_date,
                   MAX(trade_date) AS max_date,
                   COUNT(*) AS candidates
            FROM screen_result
            WHERE preset_name = ?
            """,
            (preset_name,),
        )
    if frame is None or frame.empty or pd.isna(frame.iloc[0]["min_date"]):
        return None, None, 0
    row = frame.iloc[0]
    minimum = row["min_date"].date() if hasattr(row["min_date"], "date") else row["min_date"]
    maximum = row["max_date"].date() if hasattr(row["max_date"], "date") else row["max_date"]
    return minimum, maximum, int(row["candidates"])


@st.cache_data(ttl=300)
def _minute_overview() -> pd.DataFrame | None:
    return _query_readonly(
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
def _minute_source_overview() -> pd.DataFrame | None:
    return _query_readonly(
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
def _tushare_catalog() -> pd.DataFrame:
    try:
        return load_tushare_interface_catalog(settings.data_dir / "tushare_interface_audit.duckdb")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def _tushare_goods() -> pd.DataFrame:
    try:
        return load_tushare_purchase_goods(settings.data_dir / "tushare_interface_audit.duckdb")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def _tushare_packages() -> pd.DataFrame:
    try:
        return load_tushare_activity_packages(settings.data_dir / "tushare_interface_audit.duckdb")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def _verified_code_sha() -> str | None:
    detected = detect_verified_code_commit(
        trusted_git_path=settings.lab_trusted_git_path,
    )
    return detected if detected and _EXACT_SHA.fullmatch(detected) else None


@st.cache_data(ttl=60)
def _current_research_gate(
    mode: str,
    strategy_name: str,
    start_date: date,
    end_date: date,
    code_sha: str | None,
) -> tuple[ResearchGateRequest, ResearchGateDecision]:
    request = ResearchGateRequest(
        mode=mode,
        strategy_name=strategy_name,
        start_date=start_date,
        end_date=end_date,
        code_commit=code_sha,
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
            dataset_binding_hash=None,
            coverage_ratios={},
            coverage_counts={},
            failures=(failure,),
        )
    return request, decision


def _bound_gate_request(
    request: ResearchGateRequest,
    decision: ResearchGateDecision,
) -> ResearchGateRequest:
    return request.model_copy(
        update={
            "audit_run_id": decision.audit_run_id,
            "dataset_snapshot_id": decision.dataset_snapshot_id,
            "dataset_binding_hash": decision.dataset_binding_hash,
        }
    )


def _submission_gate(
    request: ResearchGateRequest,
    decision: ResearchGateDecision,
) -> ResearchGateDecision:
    if request.mode == "exploratory":
        return decision
    if not research_gate_metadata_ready(decision):
        raise PermissionError("formal research metadata gate is not ready")
    # The worker reopens the immutable binding and performs the authoritative
    # file verification before executing any shard. The UI only queues that
    # formal intent so a large snapshot can never block the Streamlit process.
    return decision.model_copy(
        update={
            "allowed": True,
            "research_status": "comparable",
        }
    )


def _render_gate(decision: ResearchGateDecision) -> None:
    if decision.allowed and decision.research_status == "comparable":
        st.success("正式研究门已通过")
    elif research_gate_metadata_ready(decision):
        st.info("元数据已通过；提交时将校验不可变数据文件")
    elif decision.failures:
        st.warning("；".join(item.message for item in decision.failures))
    else:
        st.info("探索性试跑")


def _research_settings() -> _ResearchUiSettings:
    st.sidebar.markdown("### 研究设置")
    mode_label = st.sidebar.radio(
        "研究用途",
        ("探索性试跑", "正式回测"),
        horizontal=True,
        key="lab_research_mode",
    )
    resource_class = st.sidebar.selectbox(
        "资源等级",
        list(ResourceClass),
        index=1,
        format_func=lambda item: _RESOURCE_LABELS[item],
        key="lab_resource_class",
    )
    c1, c2 = st.sidebar.columns(2)
    deadline_hours = int(
        c1.number_input(
            "截止小时",
            min_value=1,
            max_value=168,
            value=24,
            step=1,
            key="lab_deadline_hours",
        )
    )
    max_attempts = int(
        c2.number_input(
            "最多尝试",
            min_value=1,
            max_value=10,
            value=3,
            step=1,
            key="lab_max_attempts",
        )
    )
    random_seed = int(
        st.sidebar.number_input(
            "随机种子",
            min_value=0,
            max_value=2**31 - 1,
            value=20260731,
            step=1,
            key="lab_random_seed",
        )
    )
    with st.sidebar.expander("交易成本", expanded=False):
        commission = st.number_input(
            "佣金 bps",
            min_value=0.0,
            max_value=100.0,
            value=2.5,
            step=0.1,
            key="lab_commission_bps",
        )
        stamp = st.number_input(
            "卖出印花税 bps",
            min_value=0.0,
            max_value=100.0,
            value=5.0,
            step=0.1,
            key="lab_stamp_bps",
        )
        transfer = st.number_input(
            "过户费 bps",
            min_value=0.0,
            max_value=100.0,
            value=0.1,
            step=0.1,
            key="lab_transfer_bps",
        )
        slippage = st.number_input(
            "单边滑点 bps",
            min_value=0.0,
            max_value=100.0,
            value=3.0,
            step=0.1,
            key="lab_slippage_bps",
        )
    return _ResearchUiSettings(
        mode="formal" if mode_label == "正式回测" else "exploratory",
        code_sha=_verified_code_sha(),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal(str(commission)),
            stamp_duty_bps=Decimal(str(stamp)),
            transfer_fee_bps=Decimal(str(transfer)),
            slippage_bps=Decimal(str(slippage)),
        ),
        random_seed=random_seed,
        resource_class=resource_class,
        deadline_hours=deadline_hours,
        max_attempts=max_attempts,
    )


def _submission_context(
    ui: _ResearchUiSettings,
    request: ResearchGateRequest,
    decision: ResearchGateDecision,
) -> StrategyLabSubmissionContext:
    if ui.code_sha is None:
        raise ValueError("当前代码不是可验证的干净精确提交，不能创建持久研究任务")
    verified = _submission_gate(request, decision)
    snapshot = None
    if verified.research_status != "exploratory":
        if not all(
            (
                verified.dataset_snapshot_id,
                verified.dataset_binding_hash,
                verified.audit_run_id,
            )
        ):
            raise ValueError("正式研究门缺少数据快照、绑定或审计身份")
        snapshot = DatasetSnapshotIdentity(
            snapshot_id=verified.dataset_snapshot_id,
            binding_hash=verified.dataset_binding_hash,
            audit_run_id=verified.audit_run_id,
        )
    return StrategyLabSubmissionContext(
        gate_decision=verified,
        code_sha=ui.code_sha,
        dataset_snapshot=snapshot,
        execution_costs=ui.execution_costs,
        random_seed=ui.random_seed,
        resource_class=ui.resource_class,
        deadline=datetime.now(UTC) + timedelta(hours=ui.deadline_hours),
        max_attempts=ui.max_attempts,
    )


def _render_submission_result(result: CommandSubmissionResult) -> None:
    if result.result == "submitted":
        st.session_state["lab_selected_job_id"] = str(result.job_id)
        st.success(f"任务已提交：{result.job_id}")
        return
    if result.result == "stale":
        st.warning(
            f"任务状态已变化，当前版本 {result.authoritative_version}，"
            f"状态 {_JOB_STATUS_LABELS[result.authoritative_status]}"
        )
        return
    if result.result == "unavailable":
        st.warning(f"当前状态 {_JOB_STATUS_LABELS[result.authoritative_status]} 不支持该操作")
        return
    st.warning(f"命令未接受：{result.reason}")


def _submit_run(
    controller: StrategyLabJobCenterController,
    run_input: ResearchRunInput,
    *,
    ui: _ResearchUiSettings,
    request: ResearchGateRequest,
    decision: ResearchGateDecision,
    form_key: str,
) -> None:
    try:
        context = _submission_context(ui, request, decision)
        job_id = uuid4()
        result = controller.submit(
            run_input,
            context=context,
            interaction_key=f"{form_key}:{job_id}",
            job_id=job_id,
        )
    except Exception as exc:
        st.error(f"提交失败：{type(exc).__name__}: {exc}")
        return
    _render_submission_result(result)


def _default_calendar_range(max_hold_days: int) -> tuple[date, date] | None:
    calendar = _trading_calendar()
    if not calendar:
        return None
    safe_end = safe_replay_end_date(
        calendar,
        calendar[-1],
        max_hold_days=max_hold_days,
    )
    if safe_end is None:
        return None
    return max(calendar[0], safe_end - timedelta(days=30)), safe_end


def _render_n_shape(
    controller: StrategyLabJobCenterController,
    ui: _ResearchUiSettings,
) -> None:
    st.header("N 字策略")
    mode = st.segmented_control(
        "实验类型",
        ("收益对比", "自动优化"),
        default="收益对比",
        key="n_shape_experiment_type",
    )
    presets = ("n-shape-combined", "n-shape-pool1", "n-shape-pool2")
    if mode == "自动优化":
        _render_n_shape_optimize(controller, ui, presets)
    else:
        _render_n_shape_compare(controller, ui, presets)


def _n_shape_date_defaults(preset: str, max_hold_days: int) -> tuple[date, date, date, int] | None:
    minimum, maximum, count = _screen_bounds(preset)
    calendar = _trading_calendar()
    if minimum is None or maximum is None or not calendar:
        return None
    safe_end = safe_replay_end_date(calendar, maximum, max_hold_days=max_hold_days)
    if safe_end is None or safe_end < minimum:
        return None
    return minimum, max(minimum, safe_end - timedelta(days=30)), safe_end, count


def _render_n_shape_compare(
    controller: StrategyLabJobCenterController,
    ui: _ResearchUiSettings,
    presets: tuple[str, ...],
) -> None:
    prior_preset = str(st.session_state.get("n_compare_preset", presets[0]))
    defaults = _n_shape_date_defaults(prior_preset, 10)
    if defaults is None:
        st.warning("暂无具备完整卖出窗口的 N 字候选数据")
        return
    minimum, default_start, default_end, count = defaults
    st.caption(f"候选记录 {count:,} 条 · 可用区间 {minimum} 至 {default_end}")
    with st.form("n_shape_compare_form"):
        c1, c2, c3, c4 = st.columns(4)
        preset = c1.selectbox("候选池", presets, key="n_compare_preset")
        start = c2.date_input(
            "开始日期",
            default_start,
            min_value=minimum,
            max_value=default_end,
            key="n_compare_start",
        )
        end = c3.date_input(
            "结束日期",
            default_end,
            min_value=minimum,
            max_value=default_end,
            key="n_compare_end",
        )
        hold_days = c4.multiselect(
            "持有交易日",
            list(range(1, 11)),
            default=[1],
            key="n_compare_hold_days",
        )
        selected_entries = st.multiselect(
            "入场模式",
            list(_ENTRY_LABELS),
            default=["first_break"],
            format_func=lambda item: _ENTRY_LABELS[item],
            key="n_compare_entries",
        )
        selected_variants = st.multiselect(
            "风控版本",
            list(_VARIANT_LABELS),
            default=list(_VARIANT_LABELS),
            format_func=lambda item: _VARIANT_LABELS[item],
            key="n_compare_variants",
        )
        threshold = st.number_input(
            "多因子确认阈值",
            min_value=0.0,
            max_value=100.0,
            value=35.0,
            step=1.0,
            key="n_compare_threshold",
        )
        request, decision = _current_research_gate(
            ui.mode,
            "n_shape",
            start,
            end,
            ui.code_sha,
        )
        submitted = st.form_submit_button(
            "提交收益对比",
            type="primary",
            width="stretch",
            disabled=(
                not hold_days
                or not selected_entries
                or not selected_variants
                or start > end
                or ui.code_sha is None
                or (ui.mode == "formal" and not research_gate_metadata_ready(decision))
            ),
        )
    _render_gate(decision)
    if submitted:
        run_input = NShapeComparisonRunInput(
            start_date=start,
            end_date=end,
            parameters=NShapeCompareParameters(
                hold_days=tuple(int(item) for item in hold_days),
                entry_modes=tuple(selected_entries),
                profile_variants=tuple(selected_variants),
                preset_name=preset,
                factor_score_threshold=Decimal(str(threshold)),
            ),
        )
        _submit_run(
            controller,
            run_input,
            ui=ui,
            request=request,
            decision=decision,
            form_key="n-shape-compare",
        )


def _render_n_shape_optimize(
    controller: StrategyLabJobCenterController,
    ui: _ResearchUiSettings,
    presets: tuple[str, ...],
) -> None:
    prior_preset = str(st.session_state.get("n_opt_preset", presets[0]))
    defaults = _n_shape_date_defaults(prior_preset, 10)
    if defaults is None:
        st.warning("暂无具备完整卖出窗口的 N 字候选数据")
        return
    minimum, default_start, default_end, count = defaults
    score_profiles = {item.name: item.label for item in default_score_profiles()}
    st.caption(f"候选记录 {count:,} 条 · 参数搜索由后台分片执行")
    with st.form("n_shape_optimize_form"):
        c1, c2, c3 = st.columns(3)
        preset = c1.selectbox("候选池", presets, key="n_opt_preset")
        start = c2.date_input(
            "开始日期",
            default_start,
            min_value=minimum,
            max_value=default_end,
            key="n_opt_start",
        )
        end = c3.date_input(
            "结束日期",
            default_end,
            min_value=minimum,
            max_value=default_end,
            key="n_opt_end",
        )
        c4, c5, c6 = st.columns(3)
        hold_days = c4.multiselect(
            "持有期集合",
            list(range(1, 11)),
            default=[1, 2, 3],
            key="n_opt_holds",
        )
        top_n = c5.multiselect(
            "特征 topN",
            [1, 2, 3, 5, 10],
            default=[1, 2, 3],
            key="n_opt_topn",
        )
        folds = int(
            c6.number_input(
                "Walk-forward 折数",
                min_value=0,
                max_value=8,
                value=0,
                step=1,
                key="n_opt_folds",
            )
        )
        selected_entries = st.multiselect(
            "入场模式",
            list(_ENTRY_LABELS),
            default=["first_break"],
            format_func=lambda item: _ENTRY_LABELS[item],
            key="n_opt_entries",
        )
        selected_variants = st.multiselect(
            "风控版本",
            list(_VARIANT_LABELS),
            default=list(_VARIANT_LABELS),
            format_func=lambda item: _VARIANT_LABELS[item],
            key="n_opt_variants",
        )
        selected_profiles = st.multiselect(
            "评分画像",
            list(score_profiles),
            default=list(score_profiles)[:1],
            format_func=lambda item: score_profiles[item],
            key="n_opt_profiles",
        )
        c7, c8 = st.columns(2)
        validation_ratio = c7.slider(
            "验证区间占比",
            min_value=0.0,
            max_value=0.5,
            value=0.3,
            step=0.1,
            key="n_opt_validation",
        )
        min_trades = int(
            c8.number_input(
                "最少交易数",
                min_value=1,
                max_value=100,
                value=5,
                step=1,
                key="n_opt_min_trades",
            )
        )
        request, decision = _current_research_gate(
            ui.mode,
            "n_shape",
            start,
            end,
            ui.code_sha,
        )
        submitted = st.form_submit_button(
            "提交自动优化",
            type="primary",
            width="stretch",
            disabled=(
                not hold_days
                or not top_n
                or not selected_entries
                or not selected_variants
                or not selected_profiles
                or start > end
                or ui.code_sha is None
                or (ui.mode == "formal" and not research_gate_metadata_ready(decision))
            ),
        )
    _render_gate(decision)
    if submitted:
        run_input = NShapeOptimizationRunInput(
            start_date=start,
            end_date=end,
            parameters=NShapeOptimizeParameters(
                hold_days=tuple(int(item) for item in hold_days),
                entry_modes=tuple(selected_entries),
                profile_variants=tuple(selected_variants),
                preset_name=preset,
                validation_ratio=Decimal(str(validation_ratio)),
                min_trades=min_trades,
                top_n_options=tuple(int(item) for item in top_n),
                score_profile_names=tuple(selected_profiles),
                walk_forward_folds=folds,
            ),
        )
        _submit_run(
            controller,
            run_input,
            ui=ui,
            request=request,
            decision=decision,
            form_key="n-shape-optimize",
        )


def _render_auction_gap(
    controller: StrategyLabJobCenterController,
    ui: _ResearchUiSettings,
) -> None:
    st.header("集合竞价跳空")
    defaults = _default_calendar_range(10)
    if defaults is None:
        st.warning("暂无具备完整卖出窗口的交易日数据")
        return
    default_start, default_end = defaults
    calendar = _trading_calendar()
    with st.form("auction_gap_job_form"):
        c1, c2, c3 = st.columns(3)
        start = c1.date_input(
            "开始日期",
            default_start,
            min_value=calendar[0],
            max_value=default_end,
            key="auction_job_start",
        )
        end = c2.date_input(
            "结束日期",
            default_end,
            min_value=calendar[0],
            max_value=default_end,
            key="auction_job_end",
        )
        hold_days = int(
            c3.number_input(
                "持有交易日",
                min_value=1,
                max_value=10,
                value=1,
                step=1,
                key="auction_job_hold",
            )
        )
        c4, c5, c6, c7 = st.columns(4)
        gap_mode = c4.selectbox(
            "跳空口径",
            ("close", "strict_high"),
            format_func=lambda item: "高于昨收" if item == "close" else "高于昨高",
            key="auction_job_gap_mode",
        )
        st_filter = c5.selectbox(
            "ST 过滤",
            ("case_insensitive", "literal_lower", "none"),
            format_func=lambda item: {
                "case_insensitive": "严格过滤",
                "literal_lower": "小写 st 近似",
                "none": "不过滤",
            }[item],
            key="auction_job_st_filter",
        )
        min_ratio = c6.number_input(
            "竞价量比下限",
            min_value=0.0,
            max_value=5.0,
            value=0.15,
            step=0.05,
            key="auction_job_min_ratio",
        )
        max_ratio = c7.number_input(
            "竞价量比上限",
            min_value=0.1,
            max_value=10.0,
            value=5.0,
            step=0.1,
            key="auction_job_max_ratio",
        )
        request, decision = _current_research_gate(
            ui.mode,
            "auction_gap",
            start,
            end,
            ui.code_sha,
        )
        submitted = st.form_submit_button(
            "提交集合竞价回测",
            type="primary",
            width="stretch",
            disabled=(
                start > end
                or min_ratio > max_ratio
                or ui.code_sha is None
                or (ui.mode == "formal" and not research_gate_metadata_ready(decision))
            ),
        )
    _render_gate(decision)
    if submitted:
        run_input = AuctionGapRunInput(
            start_date=start,
            end_date=end,
            parameters=AuctionGapParameters(
                max_hold_days=hold_days,
                gap_mode=gap_mode,
                min_auction_vol_ratio_5d=Decimal(str(min_ratio)),
                max_auction_vol_ratio_5d=Decimal(str(max_ratio)),
                st_filter=st_filter,
            ),
        )
        _submit_run(
            controller,
            run_input,
            ui=ui,
            request=request,
            decision=decision,
            form_key="auction-gap",
        )


def _render_growth_board(
    controller: StrategyLabJobCenterController,
    ui: _ResearchUiSettings,
) -> None:
    st.header("科创及创业板放量")
    defaults = _default_calendar_range(10)
    if defaults is None:
        st.warning("暂无具备完整卖出窗口的交易日数据")
        return
    default_start, default_end = defaults
    calendar = _trading_calendar()
    variants = growth_board_ablation_specs()
    variant_labels = {item.key: item.label for item in variants}
    with st.form("growth_board_job_form"):
        c1, c2, c3 = st.columns(3)
        start = c1.date_input(
            "开始日期",
            default_start,
            min_value=calendar[0],
            max_value=default_end,
            key="growth_job_start",
        )
        end = c2.date_input(
            "结束日期",
            default_end,
            min_value=calendar[0],
            max_value=default_end,
            key="growth_job_end",
        )
        hold_days = int(
            c3.number_input(
                "持有交易日",
                min_value=1,
                max_value=10,
                value=1,
                step=1,
                key="growth_job_hold",
            )
        )
        selected_variants = st.multiselect(
            "策略及消融版本",
            list(variant_labels),
            default=["full"],
            format_func=lambda item: variant_labels[item],
            key="growth_job_variants",
        )
        c4, c5, c6, c7, c8 = st.columns(5)
        lookback = int(
            c4.number_input(
                "分时基准天数",
                min_value=5,
                max_value=90,
                value=20,
                step=5,
                key="growth_job_lookback",
            )
        )
        min_history = int(
            c5.number_input(
                "最少基准日",
                min_value=3,
                max_value=90,
                value=10,
                step=1,
                key="growth_job_min_history",
            )
        )
        min_cumulative = c6.number_input(
            "累计放量倍",
            min_value=0.5,
            max_value=10.0,
            value=1.4,
            step=0.1,
            key="growth_job_cumulative",
        )
        min_same_minute = c7.number_input(
            "同刻放量倍",
            min_value=0.5,
            max_value=20.0,
            value=2.0,
            step=0.1,
            key="growth_job_same_minute",
        )
        min_acceleration = c8.number_input(
            "5 分钟加速度",
            min_value=0.5,
            max_value=20.0,
            value=2.0,
            step=0.1,
            key="growth_job_acceleration",
        )
        require_vwap = st.checkbox(
            "完整策略要求信号价强于当日 VWAP",
            value=True,
            key="growth_job_vwap",
        )
        request, decision = _current_research_gate(
            ui.mode,
            "growth_board_surge",
            start,
            end,
            ui.code_sha,
        )
        submitted = st.form_submit_button(
            "提交放量策略回测",
            type="primary",
            width="stretch",
            disabled=(
                not selected_variants
                or start > end
                or min_history > lookback
                or ui.code_sha is None
                or (ui.mode == "formal" and not research_gate_metadata_ready(decision))
            ),
        )
    _render_gate(decision)
    if submitted:
        run_input = GrowthBoardSurgeRunInput(
            start_date=start,
            end_date=end,
            parameters=GrowthBoardSurgeParameters(
                variants=tuple(selected_variants),
                max_hold_days=hold_days,
                lookback_days=lookback,
                min_hist_days=min_history,
                min_cum_amount_ratio=Decimal(str(min_cumulative)),
                min_same_minute_amount_ratio=Decimal(str(min_same_minute)),
                min_amount_accel_5m=Decimal(str(min_acceleration)),
                require_vwap_strength=require_vwap,
            ),
        )
        _submit_run(
            controller,
            run_input,
            ui=ui,
            request=request,
            decision=decision,
            form_key="growth-board-surge",
        )


def _format_seconds(milliseconds: float) -> str:
    seconds = max(0, int(round(milliseconds / 1000)))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}小时{minutes}分"
    if minutes:
        return f"{minutes}分{seconds}秒"
    return f"{seconds}秒"


def _render_control_result(result: CommandSubmissionResult) -> None:
    _render_submission_result(result)


def _render_job_detail(
    controller: StrategyLabJobCenterController,
    detail: LabJobDetail,
) -> None:
    job = detail.job
    st.subheader(f"{job.spec.parameters.strategy_name} · {job.job_id}")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("状态", _JOB_STATUS_LABELS[job.status])
    m2.metric("阶段", detail.progress.phase or "等待")
    m3.metric("分片", f"{detail.progress.terminal_shards}/{detail.progress.total_shards}")
    m4.metric("尝试", f"{job.attempt_count}/{job.max_attempts}")
    if detail.eta and detail.eta.remaining_duration:
        m5.metric("预计剩余", _format_seconds(detail.eta.remaining_duration.center_ms))
    else:
        m5.metric("预计剩余", "-")
    st.progress(detail.progress.fraction)
    if detail.heartbeat.stale:
        st.warning("运行分片心跳已超时，scheduler 将按租约恢复")
    if detail.first_failure is not None:
        st.error(
            f"首个失败分片 #{detail.first_failure.shard_index}："
            f"{detail.first_failure.failure.error_type} · "
            f"{detail.first_failure.failure.message}"
        )

    availability = detail.command_availability
    b1, b2, b3, b4, b5 = st.columns(5)
    if b1.button(
        "暂停",
        disabled=not availability.pause,
        width="stretch",
        key=f"pause-{job.job_id}-{job.version}",
    ):
        _render_control_result(
            controller.pause(
                job.job_id,
                expected_version=job.version,
                reason="strategy-lab-ui",
                interaction_key=f"pause:{job.job_id}:{job.version}",
            )
        )
    if b2.button(
        "恢复",
        disabled=not availability.resume,
        width="stretch",
        key=f"resume-{job.job_id}-{job.version}",
    ):
        _render_control_result(
            controller.resume(
                job.job_id,
                expected_version=job.version,
                reason="strategy-lab-ui",
                interaction_key=f"resume:{job.job_id}:{job.version}",
            )
        )
    if b3.button(
        "取消",
        disabled=not availability.cancel,
        width="stretch",
        key=f"cancel-{job.job_id}-{job.version}",
    ):
        _render_control_result(
            controller.cancel(
                job.job_id,
                expected_version=job.version,
                reason="strategy-lab-ui",
                interaction_key=f"cancel:{job.job_id}:{job.version}",
            )
        )
    if b4.button(
        "重试",
        disabled=not availability.retry,
        width="stretch",
        key=f"retry-{job.job_id}-{job.version}",
    ):
        _render_control_result(
            controller.retry(
                job.job_id,
                expected_version=job.version,
                reason="strategy-lab-ui",
                interaction_key=f"retry:{job.job_id}:{job.version}",
            )
        )
    if b5.button(
        "重新运行",
        width="stretch",
        key=f"rerun-{job.job_id}-{job.version}",
    ):
        _render_control_result(
            controller.rerun(
                job.job_id,
                max_attempts=job.max_attempts,
                interaction_key=f"rerun:{job.job_id}:{job.version}",
            )
        )

    with st.expander("运行参数", expanded=False):
        st.json(job.spec.model_dump(mode="json"))
    if detail.events:
        with st.expander(
            f"事件 {detail.event_count} 条"
            + ("（仅显示最近部分）" if detail.events_truncated else ""),
            expanded=False,
        ):
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "时间": item.created_at.astimezone(CST),
                            "事件": item.event_type,
                            "状态": _JOB_STATUS_LABELS[item.new_status],
                            "版本": item.job_version,
                            "原因": item.reason,
                        }
                        for item in detail.events
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
    if detail.result_evidence is not None and job.status is JobStatus.SUCCEEDED:
        _render_artifact(controller, job.job_id)


def _render_artifact(
    controller: StrategyLabJobCenterController,
    job_id: UUID,
) -> None:
    st.subheader("结果")
    try:
        initial = controller.preview_artifact(job_id)
        selected_table = st.selectbox(
            "结果表",
            initial.available_tables,
            key=f"artifact-table-{job_id}",
        )
        preview = (
            initial
            if initial.table is not None and initial.table.table_name == selected_table
            else controller.preview_artifact(job_id, table_name=selected_table)
        )
    except Exception as exc:
        st.error(f"结果校验失败：{type(exc).__name__}: {exc}")
        return
    with st.expander("研究报告", expanded=True):
        st.markdown(preview.report_markdown)
    with st.expander("完整指标", expanded=False):
        st.json(preview.metrics)
    if preview.table is not None:
        st.caption(
            f"{preview.table.table_name} · "
            f"{preview.table.total_rows:,} 行 × {preview.table.total_columns:,} 列"
        )
        st.dataframe(
            pd.DataFrame(preview.table.rows, columns=preview.table.columns),
            width="stretch",
            hide_index=True,
        )
    if st.button("生成完整 ZIP", key=f"export-{job_id}"):
        receipt = None
        try:
            receipt = controller.export_zip(job_id)
            with receipt.path.open("rb") as handle:
                st.download_button(
                    "下载完整结果包",
                    data=handle,
                    file_name=f"{job_id}.zip",
                    mime="application/zip",
                    width="stretch",
                    key=f"download-export-{receipt.request_id}",
                )
            st.caption(f"SHA256 `{receipt.sha256}` · {receipt.byte_size:,} 字节")
        except Exception as exc:
            st.error(f"导出失败：{type(exc).__name__}: {exc}")
        finally:
            if receipt is not None:
                try:
                    controller.discard_zip(receipt)
                except Exception as exc:
                    st.warning(f"临时 ZIP 清理失败：{type(exc).__name__}: {exc}")


def _render_job_center(controller: StrategyLabJobCenterController) -> None:
    st.header("任务中心")
    with st.form("job_filters"):
        f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.2, 1.5])
        statuses = f1.multiselect(
            "状态",
            list(JobStatus),
            format_func=lambda item: _JOB_STATUS_LABELS[item],
            key="job_filter_statuses",
        )
        job_types = f2.multiselect(
            "类型",
            list(ResearchJobType),
            format_func=lambda item: _JOB_TYPE_LABELS[item],
            key="job_filter_types",
        )
        resources = f3.multiselect(
            "资源",
            list(ResourceClass),
            format_func=lambda item: _RESOURCE_LABELS[item],
            key="job_filter_resources",
        )
        keyword = f4.text_input("关键词或任务 ID", key="job_filter_keyword")
        page_size = st.segmented_control(
            "每页",
            (20, 25),
            default=25,
            key="job_page_size",
        )
        filters_submitted = st.form_submit_button("应用筛选", width="stretch")
    if filters_submitted:
        st.session_state["lab_job_cursor"] = None
    filters = LabJobListFilters(
        statuses=tuple(statuses),
        job_types=tuple(job_types),
        resource_classes=tuple(resources),
        keyword=keyword.strip() or None,
    )
    cursor = st.session_state.get("lab_job_cursor")
    try:
        page = controller.list_jobs(
            filters=filters,
            page_size=int(page_size or 25),
            cursor=str(cursor) if cursor else None,
        )
    except Exception as exc:
        st.warning(f"任务账本尚不可用：{type(exc).__name__}: {exc}")
        return
    st.caption(f"匹配 {page.total_count:,} 个任务")
    if not page.items:
        st.info("当前筛选没有任务")
        return
    rows = pd.DataFrame(
        [
            {
                "任务 ID": str(item.job_id),
                "策略": item.strategy_name,
                "类型": _JOB_TYPE_LABELS[item.job_type],
                "状态": _JOB_STATUS_LABELS[item.status],
                "阶段": item.progress.phase or "",
                "进度": f"{item.progress.fraction:.1%}",
                "更新时间": item.updated_at.astimezone(CST),
            }
            for item in page.items
        ]
    )
    st.dataframe(rows, width="stretch", hide_index=True)
    n1, n2 = st.columns([4, 1])
    labels = {
        str(item.job_id): (
            f"{item.strategy_name} · {_JOB_STATUS_LABELS[item.status]} · {item.job_id}"
        )
        for item in page.items
    }
    current_selection = st.session_state.get("lab_selected_job_id")
    if current_selection not in labels:
        st.session_state["lab_selected_job_id"] = next(iter(labels))
    selected = n1.selectbox(
        "查看任务",
        list(labels),
        format_func=lambda item: labels[item],
        key="lab_selected_job_id",
    )
    if n2.button(
        "下一页",
        disabled=not page.has_more,
        width="stretch",
        key="job_next_page",
    ):
        st.session_state["lab_job_cursor"] = page.next_cursor
        st.rerun()
    job_id = UUID(str(selected))
    try:
        initial = controller.get_job_detail(job_id, as_of=datetime.now(UTC))
    except Exception as exc:
        st.error(f"读取任务失败：{type(exc).__name__}: {exc}")
        return
    if initial is None:
        st.warning("任务不存在或已迁移")
        return
    if initial.job.status in _ACTIVE_JOB_STATUSES:

        @st.fragment(run_every="4s")
        def _active_job_fragment() -> None:
            current = controller.get_job_detail(job_id, as_of=datetime.now(UTC))
            if current is None:
                return
            if current.job.status not in _ACTIVE_JOB_STATUSES:
                st.rerun(scope="app")
            _render_job_detail(controller, current)

        _active_job_fragment()
    else:
        _render_job_detail(controller, initial)


def _render_data_coverage() -> None:
    st.header("数据覆盖")
    overview = _minute_overview()
    if overview is None or overview.empty or pd.isna(overview.iloc[0]["rows_count"]):
        st.warning("分钟数据副本不可用")
        return
    row = overview.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("1 分钟行数", f"{int(row['rows_count']):,}")
    c2.metric("股票数", f"{int(row['codes_count']):,}")
    c3.metric("交易日", f"{int(row['trade_dates']):,}")
    c4.metric(
        "时间范围",
        f"{row['min_time']:%Y-%m-%d} 至 {row['max_time']:%Y-%m-%d}"
        if pd.notna(row["min_time"])
        else "-",
    )
    sources = _minute_source_overview()
    if sources is not None and not sources.empty:
        st.dataframe(sources, width="stretch", hide_index=True)
    st.info("90 日价量分布由后台数据任务计算；本页面不再现场扫描分钟数据。")


def _render_data_interfaces() -> None:
    st.header("数据接口")
    catalog = _tushare_catalog()
    if catalog.empty:
        st.info("暂无 Tushare 接口目录")
        return
    f1, f2, f3 = st.columns(3)
    stages = f1.multiselect(
        "接入阶段",
        list(TUSHARE_STAGE_LABELS),
        default=list(TUSHARE_STAGE_LABELS),
        format_func=lambda item: TUSHARE_STAGE_LABELS[item],
        key="catalog_stages",
    )
    statuses = f2.multiselect(
        "接入状态",
        list(TUSHARE_STATUS_LABELS),
        default=list(TUSHARE_STATUS_LABELS),
        format_func=lambda item: TUSHARE_STATUS_LABELS[item],
        key="catalog_statuses",
    )
    permissions = f3.multiselect(
        "权限",
        list(TUSHARE_PERMISSION_LABELS),
        default=list(TUSHARE_PERMISSION_LABELS),
        format_func=lambda item: TUSHARE_PERMISSION_LABELS[item],
        key="catalog_permissions",
    )
    filtered = catalog[
        catalog["integration_stage"].isin(stages)
        & catalog["integration_status"].isin(statuses)
        & catalog["permission_level"].isin(permissions)
    ]
    st.dataframe(
        format_tushare_catalog_display(filtered),
        width="stretch",
        hide_index=True,
    )
    goods = _tushare_goods()
    if not goods.empty:
        with st.expander("独立权限及积分商品", expanded=False):
            st.dataframe(goods, width="stretch", hide_index=True)
    packages = _tushare_packages()
    if not packages.empty:
        with st.expander("套餐", expanded=False):
            st.dataframe(packages, width="stretch", hide_index=True)


def _render_legacy_history() -> None:
    st.header("旧版历史")
    runs = list_strategy_lab_runs(limit=50)
    if not runs:
        st.info("暂无旧版研究记录")
        return
    labels = {
        index: (
            f"{run.created_at.astimezone(CST):%m-%d %H:%M} · "
            f"{RUN_TYPE_LABELS.get(run.run_type, run.run_type)} · {run.title}"
        )
        for index, run in enumerate(runs)
    }
    selected = st.selectbox(
        "记录",
        list(labels),
        format_func=lambda item: labels[item],
        key="legacy_run_index",
    )
    run = runs[int(selected)]
    c1, c2, c3 = st.columns(3)
    c1.metric("类型", RUN_TYPE_LABELS.get(run.run_type, run.run_type))
    c2.metric("可信度", RESEARCH_STATUS_LABELS[run.manifest.research_status])
    c3.metric("表格数", len(run.tables))
    st.download_button(
        "下载旧版 Markdown",
        data=run.markdown,
        file_name=f"{run.run_id}.md",
        mime="text/markdown",
        key=f"legacy-download-{run.run_id}",
    )
    st.json({"参数": run.params, "指标": run.metrics})
    for table in run.tables:
        with st.expander(
            f"{table.name} · {table.total_rows:,} 行"
            + ("（旧记录已截断）" if table.truncated else ""),
            expanded=False,
        ):
            frame, _ = dataframe_preview(pd.DataFrame(table.rows), max_rows=500)
            st.dataframe(frame, width="stretch", hide_index=True)


def run_strategy_lab_app() -> None:
    _configure_page()
    st.title("rQuant Strategy Lab")
    for notice in CURRENT_RESEARCH_NOTICES:
        if notice.severity == "error":
            st.error(f"{notice.title}：{notice.body}")
        elif notice.severity == "warning":
            st.warning(f"{notice.title}：{notice.body}")
    try:
        runtime = _job_center_runtime()
    except Exception as exc:
        st.error(f"Job Center 初始化失败：{type(exc).__name__}: {exc}")
        return
    view = st.sidebar.radio(
        "视图",
        (
            "任务中心",
            "N 字策略",
            "集合竞价跳空",
            "科创及创业板放量",
            "数据覆盖",
            "数据接口",
            "旧版历史",
        ),
        key="strategy_lab_view",
    )
    ui = _research_settings()
    if ui.code_sha is None:
        st.sidebar.warning("代码目录不是干净的精确提交，任务提交已禁用")
    if view == "任务中心":
        _render_job_center(runtime.controller)
    elif view == "N 字策略":
        _render_n_shape(runtime.controller, ui)
    elif view == "集合竞价跳空":
        _render_auction_gap(runtime.controller, ui)
    elif view == "科创及创业板放量":
        _render_growth_board(runtime.controller, ui)
    elif view == "数据覆盖":
        _render_data_coverage()
    elif view == "数据接口":
        _render_data_interfaces()
    else:
        _render_legacy_history()
