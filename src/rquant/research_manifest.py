"""研究结果的可信度证据与当前风险提示。"""

from __future__ import annotations

import os
import re
import stat
import subprocess
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, computed_field, model_validator

from rquant.contained_subprocess import run_contained

_IGNORED_NATIVE_CODE_SUFFIXES = frozenset({".so", ".dylib", ".pyd"})
_IGNORED_SOURCE_CODE_SUFFIXES = frozenset({".py", ".pyw"})
_IGNORED_LEGACY_BYTECODE_SUFFIXES = frozenset({".pyc", ".pyo"})
_DEFAULT_TRUSTED_GIT_PATH = Path("/usr/bin/git")

ResearchStatus = Literal[
    "exploratory",
    "comparable",
    "paper_candidate",
    "monitor_approved",
]

RESEARCH_STATUS_LABELS: dict[ResearchStatus, str] = {
    "exploratory": "探索性",
    "comparable": "可比较",
    "paper_candidate": "模拟候选",
    "monitor_approved": "监控通过",
}


@dataclass(frozen=True)
class TrustedGitExecutable:
    path: Path
    device: int
    inode: int
    mode: int
    owner: int
    links: int


def bind_trusted_git_executable(path: Path | None = None) -> TrustedGitExecutable:
    raw = path or Path(os.environ.get("RQUANT_TRUSTED_GIT_PATH", str(_DEFAULT_TRUSTED_GIT_PATH)))
    candidate = Path(raw)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError("trusted Git path must be absolute and canonical")
    try:
        if candidate.resolve(strict=True) != candidate:
            raise ValueError("trusted Git path must be physical")
        for parent in candidate.parents:
            parent_stat = parent.lstat()
            if (
                not stat.S_ISDIR(parent_stat.st_mode)
                or stat.S_ISLNK(parent_stat.st_mode)
                or parent_stat.st_uid != 0
                or parent_stat.st_mode & 0o022
            ):
                raise ValueError("trusted Git parent path is unsafe")
        observed = candidate.lstat()
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise ValueError("trusted Git executable is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != 0
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
        or (observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid)
        != (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid)
    ):
        raise ValueError("trusted Git executable is unsafe")
    return TrustedGitExecutable(
        path=candidate,
        device=observed.st_dev,
        inode=observed.st_ino,
        mode=observed.st_mode,
        owner=observed.st_uid,
        links=observed.st_nlink,
    )


def _run_trusted_git(
    binding: TrustedGitExecutable,
    arguments: list[str],
    *,
    cwd: Path,
    text: bool = True,
    deadline_monotonic: float | None = None,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    if bind_trusted_git_executable(binding.path) != binding:
        raise ValueError("trusted Git executable identity changed")
    result = run_contained(
        [str(binding.path), *arguments],
        cwd=cwd,
        text=text,
        deadline_monotonic=(
            deadline_monotonic if deadline_monotonic is not None else time.monotonic() + 3
        ),
        check=False,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        may_spawn_background_descendants=False,
    )
    if bind_trusted_git_executable(binding.path) != binding:
        raise ValueError("trusted Git executable identity changed")
    return result


class ResearchNotice(BaseModel):
    """Strategy Lab 首页展示的一条当前研究风险。"""

    severity: Literal["info", "warning", "error"]
    title: str
    body: str
    affected_run_types: tuple[str, ...]


CURRENT_RESEARCH_NOTICES: tuple[ResearchNotice, ...] = (
    ResearchNotice(
        severity="error",
        title="科创/创业旧收益不可用于决策",
        body=(
            "资格候选日的分钟覆盖率基线仅 2.2969%，旧回放存在严重选择偏差；"
            "补齐资格全集前只能作为探索线索。"
        ),
        affected_run_types=("growth_board_surge",),
    ),
    ResearchNotice(
        severity="warning",
        title="N字结果仍是小样本",
        body=(
            "旧回放尚未完整处理涨停不可成交、账户资金和 live/replay 组合信号一致性，"
            "不能据此推算本金增长。"
        ),
        affected_run_types=("n_shape_compare", "n_shape_optimize"),
    ),
    ResearchNotice(
        severity="warning",
        title="集合竞价跳空停止作为独立B策略优化",
        body="现有大样本独立策略平均收益为负；当前只保留竞价强度作为候选和排序因子。",
        affected_run_types=("auction_gap",),
    ),
)


class ResearchManifest(BaseModel):
    """一条研究结果可复现、可晋级所需的最小证据。"""

    schema_version: Literal[1, 2] = 1
    research_status: ResearchStatus = "exploratory"
    status_reason: str = Field(min_length=1)
    code_commit: str | None = None
    dataset_snapshot_id: str | None = None
    dataset_binding_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    strategy_spec_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    result_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    coverage_numerator: int | None = Field(default=None, ge=0)
    coverage_denominator: int | None = Field(default=None, gt=0)
    coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    data_start_date: date | None = None
    data_end_date: date | None = None
    universe_definition: str | None = None
    execution_model_version: str | None = None
    cost_model_version: str | None = None
    validation_method: str | None = None
    out_of_sample_trades: int | None = Field(default=None, ge=0)
    forward_validation_days: int | None = Field(default=None, ge=0)
    forward_filled_trades: int | None = Field(default=None, ge=0)
    warnings: list[str] = Field(default_factory=list)

    @computed_field
    @property
    def missing_evidence(self) -> list[str]:
        missing: list[str] = []
        if not self.code_commit:
            missing.append("code_commit")
        if not self.dataset_snapshot_id:
            missing.append("dataset_snapshot_id")
        if self.schema_version >= 2 and not self.dataset_binding_hash:
            missing.append("dataset_binding_hash")
        if self.schema_version >= 2 and not self.strategy_spec_hash:
            missing.append("strategy_spec_hash")
        if self.schema_version >= 2 and not self.result_hash:
            missing.append("result_hash")
        if self.coverage_numerator is None or self.coverage_denominator is None:
            missing.append("coverage_counts")
        if self.data_start_date is None or self.data_end_date is None:
            missing.append("data_range")
        if not self.universe_definition:
            missing.append("universe_definition")
        if not self.execution_model_version:
            missing.append("execution_model_version")
        if not self.cost_model_version:
            missing.append("cost_model_version")
        return missing

    @model_validator(mode="after")
    def validate_evidence(self) -> ResearchManifest:
        numerator_set = self.coverage_numerator is not None
        denominator_set = self.coverage_denominator is not None
        if numerator_set != denominator_set:
            raise ValueError("coverage_numerator 与 coverage_denominator 必须同时提供")
        if numerator_set and denominator_set:
            assert self.coverage_numerator is not None
            assert self.coverage_denominator is not None
            if self.coverage_numerator > self.coverage_denominator:
                raise ValueError("coverage_numerator 不能大于 coverage_denominator")
            computed = self.coverage_numerator / self.coverage_denominator
            if self.coverage_ratio is not None and abs(self.coverage_ratio - computed) > 1e-9:
                raise ValueError("coverage_ratio 与覆盖计数不一致")
            self.coverage_ratio = computed

        start_set = self.data_start_date is not None
        end_set = self.data_end_date is not None
        if start_set != end_set:
            raise ValueError("data_start_date 与 data_end_date 必须同时提供")
        if (
            self.data_start_date is not None
            and self.data_end_date is not None
            and self.data_start_date > self.data_end_date
        ):
            raise ValueError("data_start_date 不能晚于 data_end_date")

        if self.research_status != "exploratory" and self.missing_evidence:
            missing = ", ".join(self.missing_evidence)
            raise ValueError(f"{self.research_status} 缺少证据: {missing}")
        if self.research_status != "exploratory" and str(self.code_commit).endswith("-dirty"):
            raise ValueError(f"{self.research_status} 不能使用脏工作树代码")
        if (
            self.research_status in {"paper_candidate", "monitor_approved"}
            and not self.validation_method
        ):
            raise ValueError(f"{self.research_status} 缺少证据: validation_method")
        if self.research_status in {"paper_candidate", "monitor_approved"} and (
            self.out_of_sample_trades is None or self.out_of_sample_trades < 100
        ):
            raise ValueError(f"{self.research_status} 至少需要 100 笔严格样本外成交")
        if self.research_status == "monitor_approved" and (
            not self.forward_validation_days or self.forward_validation_days < 20
        ):
            raise ValueError("monitor_approved 至少需要 20 个前瞻验证交易日")
        if self.research_status == "monitor_approved" and (
            self.forward_filled_trades is None or self.forward_filled_trades < 30
        ):
            raise ValueError("monitor_approved 至少需要 30 笔前瞻成交")
        return self


def detect_code_commit(
    repo_root: Path | None = None,
    *,
    trusted_git_path: Path | None = None,
    deadline_monotonic: float | None = None,
) -> str | None:
    """优先读取部署注入值；本地开发时回退到 git HEAD。"""
    injected = os.getenv("RQUANT_CODE_COMMIT", "").strip()
    if injected:
        return injected
    cwd = repo_root or Path(__file__).resolve().parents[2]
    try:
        git = bind_trusted_git_executable(trusted_git_path)
        result = _run_trusted_git(
            git,
            ["rev-parse", "HEAD"],
            cwd=cwd,
            deadline_monotonic=deadline_monotonic,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    commit = result.stdout.strip()
    if result.returncode != 0 or not commit:
        return None
    try:
        status = _run_trusted_git(
            git,
            ["status", "--porcelain", "--untracked-files=normal"],
            cwd=cwd,
            deadline_monotonic=deadline_monotonic,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return f"{commit}-dirty"
    if status.returncode != 0 or status.stdout.strip():
        return f"{commit}-dirty"
    return commit


def detect_verified_code_commit(
    repo_root: Path | None = None,
    *,
    trusted_git_path: Path | None = None,
    deadline_monotonic: float | None = None,
) -> str | None:
    """Resolve a formal-run commit from the real clean Git checkout."""
    cwd = repo_root or Path(__file__).resolve().parents[2]
    try:
        git = bind_trusted_git_executable(trusted_git_path)
        head = _run_trusted_git(
            git,
            ["rev-parse", "HEAD"],
            cwd=cwd,
            deadline_monotonic=deadline_monotonic,
        )
        checkout = _run_trusted_git(
            git,
            ["rev-parse", "--show-toplevel"],
            cwd=cwd,
            deadline_monotonic=deadline_monotonic,
        )
        status = _run_trusted_git(
            git,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
            cwd=cwd,
            text=False,
            deadline_monotonic=deadline_monotonic,
        )
        ignored_source_artifacts = _run_trusted_git(
            git,
            [
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "-z",
                "--",
                ":(top)src/rquant",
            ],
            cwd=cwd,
            text=False,
            deadline_monotonic=deadline_monotonic,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    commit = head.stdout.strip()
    if (
        head.returncode != 0
        or re.fullmatch(r"[0-9a-f]{40}", commit) is None
        or checkout.returncode != 0
        or status.returncode != 0
        or ignored_source_artifacts.returncode != 0
    ):
        return None
    injected = os.getenv("RQUANT_CODE_COMMIT", "").strip()
    if injected and injected != commit:
        return None
    checkout_root = Path(checkout.stdout.strip())
    if not checkout_root.is_absolute():
        return None
    if status.stdout:
        return f"{commit}-dirty"
    for raw_path in ignored_source_artifacts.stdout.split(b"\0"):
        if not raw_path:
            continue
        artifact = Path(os.fsdecode(raw_path))
        try:
            artifact_identity = (checkout_root / artifact).lstat()
        except OSError:
            return f"{commit}-dirty"
        if stat.S_ISLNK(artifact_identity.st_mode):
            return f"{commit}-dirty"
        suffix = artifact.suffix.lower()
        if (
            suffix in _IGNORED_NATIVE_CODE_SUFFIXES
            or suffix in _IGNORED_SOURCE_CODE_SUFFIXES
            or suffix in _IGNORED_LEGACY_BYTECODE_SUFFIXES
        ):
            return f"{commit}-dirty"
    return commit


def new_exploratory_manifest(run_type: str) -> ResearchManifest:
    return ResearchManifest(
        research_status="exploratory",
        status_reason=f"{run_type} 尚未提供完整数据、覆盖率与执行模型证据",
        code_commit=detect_code_commit(),
        warnings=["当前结果只能形成研究假设，不能用于本金增长推算或自动发布到 live。"],
    )


def legacy_exploratory_manifest(run_type: str) -> ResearchManifest:
    return ResearchManifest(
        research_status="exploratory",
        status_reason=f"旧记录 {run_type} 未保存可信度 manifest，已自动降级",
        warnings=["原始 JSON 未改写；需要重跑才能补齐数据快照和执行证据。"],
    )
