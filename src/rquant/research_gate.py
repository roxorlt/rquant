"""Fail-closed evidence gate for formal Strategy Lab research."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import (
    DataAuditRun,
    DataQualityIssue,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotBinding,
)
from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION
from rquant.research_manifest import ResearchManifest, ResearchStatus
from rquant.runtime_code_attestation import CodeTrustEvidence

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
    dataset_binding_hash: str | None = None
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
    dataset_binding_hash: str | None = None
    coverage_ratios: dict[str, float | None]
    coverage_counts: dict[str, tuple[int, int]]
    failures: tuple[ResearchGateFailure, ...]


def _failure(code: str, message: str) -> ResearchGateFailure:
    return ResearchGateFailure(code=code, message=message)


def research_gate_metadata_ready(
    decision: ResearchGateDecision,
) -> bool:
    """Return whether only execution-time artifact verification remains."""
    return not any(failure.code != "snapshot_artifacts_unverified" for failure in decision.failures)


def evaluate_research_gate(
    request: ResearchGateRequest,
    *,
    audit_run: DataAuditRun | None,
    snapshot: DatasetSnapshot | None,
    binding: DatasetSnapshotBinding | None = None,
    binding_verified: bool = False,
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
        if audit_run.range_start > request.start_date or audit_run.range_end < request.end_date:
            failures.append(_failure("audit_range", "数据审计未覆盖完整回测日期区间"))
        if audit_run.as_of_date < request.end_date:
            failures.append(_failure("audit_as_of", "数据审计时点早于回测结束日期"))
        if audit_run.p0_count:
            failures.append(
                _failure("audit_p0", f"本次数据审计发现 {audit_run.p0_count} 个 P0 问题")
            )

    if open_p0_issues:
        failures.append(_failure("open_p0", f"当前仍有 {len(open_p0_issues)} 个未解决 P0 问题"))

    if snapshot is None:
        failures.append(_failure("snapshot_missing", "没有可用的 ready 数据快照"))
        if binding is not None:
            failures.append(
                _failure(
                    "snapshot_binding_orphan",
                    "执行数据绑定没有对应的数据快照",
                )
            )
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
        eligibility_resolution_hash = snapshot.table_watermarks.get("eligibility_resolution_hash")
        if eligibility_resolution_hash is None:
            failures.append(
                _failure(
                    "snapshot_eligibility_resolution_missing",
                    "数据快照没有记录资格解析 hash",
                )
            )
        if binding is None:
            failures.append(
                _failure(
                    "snapshot_execution_unbound",
                    "正式回测尚未绑定不可变计算数据，当前只能探索性试跑",
                )
            )
        else:
            if request.dataset_binding_hash and (
                binding.binding_hash != request.dataset_binding_hash
            ):
                failures.append(
                    _failure(
                        "snapshot_binding_hash_mismatch",
                        "执行数据绑定 hash 与提交请求不一致",
                    )
                )
            if binding.snapshot_id != snapshot.snapshot_id:
                failures.append(
                    _failure(
                        "snapshot_binding_mismatch",
                        "执行数据绑定不属于所选数据快照",
                    )
                )
            if binding.status != "ready":
                failures.append(
                    _failure(
                        "snapshot_binding_not_ready",
                        "执行数据绑定尚未完成",
                    )
                )
            manifest = binding.manifest
            if (
                manifest.strategy_name != snapshot.strategy_name
                or manifest.code_commit != snapshot.code_commit
                or manifest.as_of_time != snapshot.as_of_time
            ):
                failures.append(
                    _failure(
                        "snapshot_binding_identity",
                        "执行数据绑定与快照身份不一致",
                    )
                )
            eligibility_artifacts = tuple(
                artifact
                for artifact in manifest.artifacts
                if artifact.dataset_id == "strategy_eligibility"
                and artifact.table_name == "strategy_eligibility"
            )
            if (
                eligibility_resolution_hash is None
                or manifest.eligibility_resolution_hash != eligibility_resolution_hash
                or len(eligibility_artifacts) != 1
                or eligibility_artifacts[0].artifact_key
                != f"strategy_eligibility:{eligibility_resolution_hash}"
            ):
                failures.append(
                    _failure(
                        "snapshot_binding_eligibility",
                        "执行数据绑定与快照资格解析不是同一数据代际",
                    )
                )
            if manifest.start_date > request.start_date or manifest.end_date < request.end_date:
                failures.append(
                    _failure(
                        "snapshot_binding_range",
                        "执行数据绑定未覆盖完整回测日期区间",
                    )
                )
            if not binding_verified:
                failures.append(
                    _failure(
                        "snapshot_artifacts_unverified",
                        "执行数据工件尚未在本次会话中完成哈希校验",
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
            failures.append(_failure(f"coverage_{scope}_missing", f"缺少 {scope} 覆盖率凭证"))
            continue
        ratio = coverage.coverage_ratio
        coverage_ratios[scope] = ratio
        coverage_counts[scope] = (coverage.available_count, coverage.expected_count)
        if ratio is None:
            failures.append(_failure(f"coverage_{scope}_empty", f"{scope} 覆盖率分母为零"))
        elif ratio < threshold:
            failures.append(
                _failure(
                    f"coverage_{scope}_low",
                    f"{scope} 覆盖率 {ratio:.2%}，要求至少 {threshold:.0%}",
                )
            )
    eligibility_coverage = by_scope.get("eligibility")
    if (
        binding is not None
        and eligibility_coverage is not None
        and (
            binding.manifest.eligibility_expected_dates != eligibility_coverage.expected_count
            or binding.manifest.eligibility_complete_dates != eligibility_coverage.available_count
        )
    ):
        failures.append(
            _failure(
                "snapshot_binding_eligibility_counts",
                "执行数据绑定的资格日期计数与覆盖凭证不一致",
            )
        )

    formal_passed = not failures
    allowed = request.mode == "exploratory" or formal_passed
    return ResearchGateDecision(
        allowed=allowed,
        research_status=(
            "comparable" if request.mode == "formal" and formal_passed else "exploratory"
        ),
        audit_run_id=None if audit_run is None else audit_run.audit_run_id,
        dataset_snapshot_id=None if snapshot is None else snapshot.snapshot_id,
        dataset_binding_hash=(None if binding is None else binding.binding_hash),
        coverage_ratios=coverage_ratios,
        coverage_counts=coverage_counts,
        failures=tuple(failures),
    )


def evaluate_store_research_gate(
    store: object,
    request: ResearchGateRequest,
    *,
    binding_verified: bool = False,
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
    binding = None if snapshot is None else store.get_dataset_snapshot_binding(snapshot.snapshot_id)
    open_p0 = store.list_open_data_quality_issues(severities=("P0",))
    return evaluate_research_gate(
        request,
        audit_run=audit_run,
        snapshot=snapshot,
        binding=binding,
        binding_verified=binding_verified,
        coverages=coverages,
        open_p0_issues=open_p0,
    )


@contextmanager
def open_gated_research_store(
    request: ResearchGateRequest,
    *,
    metadata_store_factory: (Callable[[], AbstractContextManager[object]] | None) = None,
    execution_session_factory: (
        Callable[
            [DatasetSnapshotBinding, Path],
            AbstractContextManager[object],
        ]
        | None
    ) = None,
    lake_root: Path | None = None,
) -> Iterator[tuple[object, ResearchGateDecision]]:
    """Evaluate the gate and run formal compute on its exact verified binding."""
    if metadata_store_factory is None:
        from rquant.storage.duckdb import open_readonly_store

        metadata_store_factory = open_readonly_store
    with metadata_store_factory() as metadata_store:
        decision = evaluate_store_research_gate(metadata_store, request)
        blocking_failures = tuple(
            failure
            for failure in decision.failures
            if failure.code != "snapshot_artifacts_unverified"
        )
        if request.mode == "formal" and blocking_failures:
            reasons = "; ".join(failure.message for failure in blocking_failures)
            raise PermissionError(f"正式研究门未通过: {reasons}")
        if request.mode == "exploratory":
            yield metadata_store, decision
            return

        snapshot_id = decision.dataset_snapshot_id
        if snapshot_id is None:
            raise PermissionError("正式研究门没有解析出数据快照")
        binding = metadata_store.get_dataset_snapshot_binding(snapshot_id)
        if binding is None or binding.status != "ready":
            raise PermissionError("正式研究门没有可用的执行数据绑定")
        if (
            decision.dataset_binding_hash is None
            or binding.binding_hash != decision.dataset_binding_hash
        ):
            raise PermissionError("正式研究门执行数据绑定发生变化")

        if lake_root is None:
            from rquant.config import get_settings

            lake_root = get_settings().research_lake_dir_resolved
        if execution_session_factory is None:
            from rquant.research_snapshot import ResearchExecutionSession

            def open_execution_session(
                selected: DatasetSnapshotBinding,
                root: Path,
            ) -> AbstractContextManager[object]:
                return ResearchExecutionSession(
                    binding=selected,
                    lake_root=root,
                )

            execution_session_factory = open_execution_session
        with execution_session_factory(binding, lake_root) as execution_store:
            verified_decision = evaluate_store_research_gate(
                metadata_store,
                request,
                binding_verified=True,
            )
            if not verified_decision.allowed:
                reasons = "; ".join(failure.message for failure in verified_decision.failures)
                raise PermissionError(f"正式研究门未通过: {reasons}")
            if verified_decision.dataset_binding_hash != binding.binding_hash:
                raise PermissionError("正式研究门执行数据绑定发生变化")
            yield execution_store, verified_decision


def build_gate_research_manifest(
    request: ResearchGateRequest,
    decision: ResearchGateDecision,
    *,
    code_trust_evidence: CodeTrustEvidence | None = None,
    strategy_spec_hash: str | None = None,
    result_hash: str | None = None,
) -> ResearchManifest:
    warnings = [failure.message for failure in decision.failures]
    numerator = sum(item[0] for item in decision.coverage_counts.values())
    denominator = sum(item[1] for item in decision.coverage_counts.values())
    if decision.research_status == "exploratory":
        return ResearchManifest(
            schema_version=2,
            research_status="exploratory",
            status_reason=("探索性试跑；正式研究门未通过" if warnings else "用户选择探索性试跑"),
            code_commit=request.code_commit,
            dataset_snapshot_id=decision.dataset_snapshot_id,
            dataset_binding_hash=decision.dataset_binding_hash,
            coverage_numerator=(numerator if denominator else None),
            coverage_denominator=(denominator if denominator else None),
            data_start_date=request.start_date,
            data_end_date=request.end_date,
            universe_definition=request.strategy_name,
            warnings=warnings
            or ["当前结果只能形成研究假设，不能用于本金增长推算或自动发布到 live。"],
        )
    if code_trust_evidence is None:
        raise PermissionError("正式研究 manifest 缺少不可变代码 generation 证据")
    if request.code_commit != code_trust_evidence.provenance_commit:
        raise PermissionError("正式研究请求与不可变代码 generation 证据不一致")
    return ResearchManifest(
        schema_version=3,
        research_status="comparable",
        status_reason="已通过 Stage-1 数据审计、PIT 与覆盖率正式研究门",
        code_commit=code_trust_evidence.provenance_commit,
        code_trust_evidence=code_trust_evidence,
        dataset_snapshot_id=decision.dataset_snapshot_id,
        dataset_binding_hash=decision.dataset_binding_hash,
        strategy_spec_hash=strategy_spec_hash,
        result_hash=result_hash,
        coverage_numerator=numerator,
        coverage_denominator=denominator,
        data_start_date=request.start_date,
        data_end_date=request.end_date,
        universe_definition=request.strategy_name,
        execution_model_version="minute-replay-tplus1-v1",
        cost_model_version="a-share-cost-v1",
        warnings=(),
    )
