"""Fail-closed evidence gate for formal Strategy Lab research."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import (
    DataAuditRun,
    DataQualityIssue,
    DatasetCoverage,
    DatasetSnapshot,
)
from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION
from rquant.research_manifest import ResearchManifest, ResearchStatus

ResearchMode = Literal["exploratory", "formal"]

MIN_COVERAGE_BY_SCOPE: dict[str, float] = {
    "eligibility": 0.99,
    "baseline": 0.95,
    "entry": 0.99,
    "exit": 0.99,
}


class GateModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ResearchGateRequest(GateModel):
    mode: ResearchMode
    strategy_name: str = Field(min_length=1)
    start_date: date
    end_date: date
    audit_run_id: str | None = None
    dataset_snapshot_id: str | None = None
    code_commit: str | None = None

    @model_validator(mode="after")
    def validate_range(self) -> ResearchGateRequest:
        if self.start_date > self.end_date:
            raise ValueError("research start_date cannot be after end_date")
        return self


class ResearchGateFailure(GateModel):
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)


class ResearchGateDecision(GateModel):
    allowed: bool
    research_status: ResearchStatus
    audit_run_id: str | None
    dataset_snapshot_id: str | None
    coverage_ratios: dict[str, float | None]
    coverage_counts: dict[str, tuple[int, int]]
    failures: tuple[ResearchGateFailure, ...]


def _failure(code: str, message: str) -> ResearchGateFailure:
    return ResearchGateFailure(code=code, message=message)


def evaluate_research_gate(
    request: ResearchGateRequest,
    *,
    audit_run: DataAuditRun | None,
    snapshot: DatasetSnapshot | None,
    coverages: Sequence[DatasetCoverage],
    open_p0_issues: Sequence[DataQualityIssue],
) -> ResearchGateDecision:
    failures: list[ResearchGateFailure] = []

    if not request.code_commit:
        failures.append(_failure("code_commit_missing", "没有记录当前代码提交"))
    elif request.code_commit.endswith("-dirty"):
        failures.append(_failure("code_commit_dirty", "正式回测不能使用脏工作树代码"))

    if audit_run is None:
        failures.append(_failure("audit_missing", "没有覆盖本次区间的已完成数据审计"))
    else:
        if request.audit_run_id and audit_run.audit_run_id != request.audit_run_id:
            failures.append(_failure("audit_mismatch", "数据审计 ID 与提交请求不一致"))
        if audit_run.status != "completed":
            failures.append(_failure("audit_incomplete", "数据审计尚未成功完成"))
        if audit_run.rule_set_version != STAGE1_AUDIT_RULE_SET_VERSION:
            failures.append(
                _failure(
                    "audit_rule_set",
                    "数据审计规则版本已过期："
                    f"{audit_run.rule_set_version}，当前要求 "
                    f"{STAGE1_AUDIT_RULE_SET_VERSION}",
                )
            )
        if (
            audit_run.range_start > request.start_date
            or audit_run.range_end < request.end_date
        ):
            failures.append(_failure("audit_range", "数据审计未覆盖完整回测日期区间"))
        if audit_run.as_of_date < request.end_date:
            failures.append(_failure("audit_as_of", "数据审计时点早于回测结束日期"))
        if audit_run.p0_count:
            failures.append(
                _failure("audit_p0", f"本次数据审计发现 {audit_run.p0_count} 个 P0 问题")
            )

    if open_p0_issues:
        failures.append(
            _failure("open_p0", f"当前仍有 {len(open_p0_issues)} 个未解决 P0 问题")
        )

    if snapshot is None:
        failures.append(_failure("snapshot_missing", "没有可用的 ready 数据快照"))
    else:
        if request.dataset_snapshot_id and snapshot.snapshot_id != request.dataset_snapshot_id:
            failures.append(_failure("snapshot_mismatch", "数据快照 ID 与提交请求不一致"))
        if snapshot.status != "ready":
            failures.append(_failure("snapshot_not_ready", "数据快照尚未完成"))
        if snapshot.strategy_name != request.strategy_name:
            failures.append(_failure("snapshot_strategy", "数据快照不属于当前策略"))
        if snapshot.as_of_time.date() < request.end_date:
            failures.append(_failure("snapshot_as_of", "数据快照时点早于回测结束日期"))
        manifest_start = snapshot.table_watermarks.get("manifest_start_date")
        manifest_end = snapshot.table_watermarks.get("manifest_end_date")
        if (
            manifest_start is None
            or manifest_end is None
            or manifest_start > request.start_date.isoformat()
            or manifest_end < request.end_date.isoformat()
        ):
            failures.append(_failure("snapshot_range", "数据快照未覆盖完整回测日期区间"))
        failures.append(
            _failure(
                "snapshot_execution_unbound",
                "正式回测尚未绑定不可变计算数据，当前只能探索性试跑",
            )
        )
        if not snapshot.manifest_id:
            failures.append(_failure("manifest_missing", "数据快照未绑定不可变回补清单"))
        if request.code_commit and snapshot.code_commit != request.code_commit:
            failures.append(_failure("code_commit_mismatch", "当前代码提交与数据快照不一致"))

    by_scope: dict[str, DatasetCoverage] = {}
    for coverage in coverages:
        if coverage.coverage_scope in by_scope:
            failures.append(
                _failure(
                    f"coverage_{coverage.coverage_scope}_duplicate",
                    f"覆盖范围 {coverage.coverage_scope} 出现重复记录",
                )
            )
            continue
        by_scope[coverage.coverage_scope] = coverage
        if snapshot is not None and coverage.snapshot_id != snapshot.snapshot_id:
            failures.append(
                _failure(
                    f"coverage_{coverage.coverage_scope}_snapshot",
                    f"覆盖范围 {coverage.coverage_scope} 不属于所选快照",
                )
            )

    coverage_ratios: dict[str, float | None] = {}
    coverage_counts: dict[str, tuple[int, int]] = {}
    for scope, threshold in MIN_COVERAGE_BY_SCOPE.items():
        coverage = by_scope.get(scope)
        if coverage is None:
            coverage_ratios[scope] = None
            coverage_counts[scope] = (0, 0)
            failures.append(
                _failure(f"coverage_{scope}_missing", f"缺少 {scope} 覆盖率凭证")
            )
            continue
        ratio = coverage.coverage_ratio
        coverage_ratios[scope] = ratio
        coverage_counts[scope] = (coverage.available_count, coverage.expected_count)
        if ratio is None:
            failures.append(
                _failure(f"coverage_{scope}_empty", f"{scope} 覆盖率分母为零")
            )
        elif ratio < threshold:
            failures.append(
                _failure(
                    f"coverage_{scope}_low",
                    f"{scope} 覆盖率 {ratio:.2%}，要求至少 {threshold:.0%}",
                )
            )

    formal_passed = not failures
    allowed = request.mode == "exploratory" or formal_passed
    return ResearchGateDecision(
        allowed=allowed,
        research_status=(
            "comparable"
            if request.mode == "formal" and formal_passed
            else "exploratory"
        ),
        audit_run_id=None if audit_run is None else audit_run.audit_run_id,
        dataset_snapshot_id=None if snapshot is None else snapshot.snapshot_id,
        coverage_ratios=coverage_ratios,
        coverage_counts=coverage_counts,
        failures=tuple(failures),
    )


def evaluate_store_research_gate(
    store: object,
    request: ResearchGateRequest,
) -> ResearchGateDecision:
    audit_run = (
        store.get_data_audit_run(request.audit_run_id)
        if request.audit_run_id
        else store.latest_completed_data_audit_run(
            as_of_date=request.end_date,
            range_start=request.start_date,
            range_end=request.end_date,
        )
    )
    snapshot = (
        store.get_dataset_snapshot(request.dataset_snapshot_id)
        if request.dataset_snapshot_id
        else store.latest_ready_dataset_snapshot(
            strategy_name=request.strategy_name,
            as_of_date=request.end_date,
            range_start=request.start_date,
            range_end=request.end_date,
        )
    )
    coverages = () if snapshot is None else store.list_dataset_coverages(snapshot.snapshot_id)
    open_p0 = store.list_open_data_quality_issues(severities=("P0",))
    return evaluate_research_gate(
        request,
        audit_run=audit_run,
        snapshot=snapshot,
        coverages=coverages,
        open_p0_issues=open_p0,
    )


def build_gate_research_manifest(
    request: ResearchGateRequest,
    decision: ResearchGateDecision,
) -> ResearchManifest:
    warnings = [failure.message for failure in decision.failures]
    if decision.research_status == "exploratory":
        return ResearchManifest(
            research_status="exploratory",
            status_reason=(
                "探索性试跑；正式研究门未通过"
                if warnings
                else "用户选择探索性试跑"
            ),
            code_commit=request.code_commit,
            warnings=warnings
            or ["当前结果只能形成研究假设，不能用于本金增长推算或自动发布到 live。"],
        )
    numerator = sum(item[0] for item in decision.coverage_counts.values())
    denominator = sum(item[1] for item in decision.coverage_counts.values())
    return ResearchManifest(
        research_status="comparable",
        status_reason="已通过 Stage-1 数据审计、PIT 与覆盖率正式研究门",
        code_commit=request.code_commit,
        dataset_snapshot_id=decision.dataset_snapshot_id,
        coverage_numerator=numerator,
        coverage_denominator=denominator,
        data_start_date=request.start_date,
        data_end_date=request.end_date,
        universe_definition=request.strategy_name,
        execution_model_version="minute-replay-tplus1-v1",
        cost_model_version="a-share-cost-v1",
        warnings=(),
    )
