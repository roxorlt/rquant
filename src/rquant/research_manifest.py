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
from rquant.runtime_code_attestation import CodeTrustEvidence

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


@dataclass(frozen=True)
class _TrustedGitPathIdentity:
    path: Path
    device: int
    inode: int
    mode: int
    owner: int
    links: int


@dataclass(frozen=True)
class TrustedGitRepository:
    checkout_root: _TrustedGitPathIdentity
    git_dir: _TrustedGitPathIdentity
    common_dir: _TrustedGitPathIdentity
    object_directory: _TrustedGitPathIdentity
    index_file: _TrustedGitPathIdentity


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


def _capture_git_path_identity(
    path: Path,
    *,
    expect_directory: bool,
) -> _TrustedGitPathIdentity:
    try:
        physical = path.resolve(strict=True)
        observed = physical.lstat()
    except OSError as exc:
        raise ValueError("trusted Git repository path is unavailable") from exc
    expected_type = stat.S_ISDIR if expect_directory else stat.S_ISREG
    if not expected_type(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise ValueError("trusted Git repository path has unsafe type")
    return _TrustedGitPathIdentity(
        path=physical,
        device=observed.st_dev,
        inode=observed.st_ino,
        mode=observed.st_mode,
        owner=observed.st_uid,
        links=observed.st_nlink,
    )


def _assert_trusted_git_repository_identity(repository: TrustedGitRepository) -> None:
    paths = (
        (repository.checkout_root, True),
        (repository.git_dir, True),
        (repository.common_dir, True),
        (repository.object_directory, True),
        (repository.index_file, False),
    )
    for expected, expect_directory in paths:
        if (
            _capture_git_path_identity(
                expected.path,
                expect_directory=expect_directory,
            )
            != expected
        ):
            raise ValueError("trusted Git repository identity changed")


def _trusted_git_environment(repository: TrustedGitRepository | None) -> dict[str, str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "LC_ALL": "C",
        }
    )
    if repository is not None:
        environment.update(
            {
                "GIT_COMMON_DIR": str(repository.common_dir.path),
                "GIT_DIR": str(repository.git_dir.path),
                "GIT_INDEX_FILE": str(repository.index_file.path),
                "GIT_OBJECT_DIRECTORY": str(repository.object_directory.path),
                "GIT_WORK_TREE": str(repository.checkout_root.path),
            }
        )
    return environment


def _run_trusted_git(
    binding: TrustedGitExecutable,
    arguments: list[str],
    *,
    cwd: Path,
    text: bool = True,
    deadline_monotonic: float | None = None,
    repository: TrustedGitRepository | None = None,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    try:
        physical_cwd = cwd.resolve(strict=True)
    except OSError as exc:
        raise ValueError("trusted Git checkout is unavailable") from exc
    if not physical_cwd.is_dir():
        raise ValueError("trusted Git checkout is not a directory")
    if repository is not None:
        _assert_trusted_git_repository_identity(repository)
        if physical_cwd != repository.checkout_root.path:
            raise ValueError("trusted Git checkout binding mismatch")
    if bind_trusted_git_executable(binding.path) != binding:
        raise ValueError("trusted Git executable identity changed")
    result = run_contained(
        [
            str(binding.path),
            "--no-pager",
            "--no-replace-objects",
            "-C",
            str(physical_cwd),
            *arguments,
        ],
        cwd=physical_cwd,
        text=text,
        deadline_monotonic=(
            deadline_monotonic if deadline_monotonic is not None else time.monotonic() + 3
        ),
        check=False,
        env=_trusted_git_environment(repository),
        may_spawn_background_descendants=False,
    )
    if bind_trusted_git_executable(binding.path) != binding:
        raise ValueError("trusted Git executable identity changed")
    if repository is not None:
        _assert_trusted_git_repository_identity(repository)
    return result


def _physical_git_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        raise ValueError("trusted Git repository path is not absolute")
    try:
        return candidate.resolve(strict=True)
    except OSError as exc:
        raise ValueError("trusted Git repository path is unavailable") from exc


def bind_trusted_git_repository(
    executable: TrustedGitExecutable,
    checkout_root: Path,
    *,
    deadline_monotonic: float | None = None,
) -> TrustedGitRepository:
    checkout_identity = _capture_git_path_identity(checkout_root, expect_directory=True)
    metadata = _run_trusted_git(
        executable,
        [
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--absolute-git-dir",
            "--git-common-dir",
            "--git-path",
            "objects",
            "--git-path",
            "index",
        ],
        cwd=checkout_identity.path,
        deadline_monotonic=deadline_monotonic,
    )
    if metadata.returncode != 0 or not isinstance(metadata.stdout, str):
        raise ValueError("trusted Git repository metadata is unavailable")
    metadata_paths = metadata.stdout.splitlines()
    if len(metadata_paths) != 5 or not all(metadata_paths):
        raise ValueError("trusted Git repository metadata is malformed")
    top_level = _physical_git_path(metadata_paths[0])
    if top_level != checkout_identity.path:
        raise ValueError("trusted Git top-level does not match checkout")
    git_dir, common_dir, object_directory, index_file = map(
        _physical_git_path,
        metadata_paths[1:],
    )
    git_dir_identity = _capture_git_path_identity(git_dir, expect_directory=True)
    common_dir_identity = _capture_git_path_identity(common_dir, expect_directory=True)
    object_identity = _capture_git_path_identity(object_directory, expect_directory=True)
    index_identity = _capture_git_path_identity(index_file, expect_directory=False)
    if object_identity.path != (common_dir_identity.path / "objects").resolve(strict=True):
        raise ValueError("trusted Git object directory is outside repository metadata")
    if index_identity.path != (git_dir_identity.path / "index").resolve(strict=True):
        raise ValueError("trusted Git index is outside worktree metadata")
    marker = checkout_identity.path / ".git"
    try:
        marker_identity = marker.lstat()
    except OSError as exc:
        raise ValueError("trusted Git checkout marker is unavailable") from exc
    if stat.S_ISDIR(marker_identity.st_mode):
        if (
            marker.resolve(strict=True) != git_dir_identity.path
            or git_dir_identity.path != common_dir_identity.path
        ):
            raise ValueError("trusted Git checkout metadata mismatch")
    elif stat.S_ISREG(marker_identity.st_mode) and not stat.S_ISLNK(marker_identity.st_mode):
        linked_metadata_root = common_dir_identity.path / "worktrees"
        if (
            git_dir_identity.path == common_dir_identity.path
            or not git_dir_identity.path.is_relative_to(linked_metadata_root)
        ):
            raise ValueError("trusted Git linked worktree metadata mismatch")
    else:
        raise ValueError("trusted Git checkout marker has unsafe type")
    repository = TrustedGitRepository(
        checkout_root=checkout_identity,
        git_dir=git_dir_identity,
        common_dir=common_dir_identity,
        object_directory=object_identity,
        index_file=index_identity,
    )
    _assert_trusted_git_repository_identity(repository)
    return repository


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

    schema_version: Literal[1, 2, 3] = 1
    research_status: ResearchStatus = "exploratory"
    status_reason: str = Field(min_length=1)
    code_commit: str | None = None
    code_trust_evidence: CodeTrustEvidence | None = None
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
        if self.schema_version < 3 and not self.code_commit:
            missing.append("code_commit")
        if self.schema_version >= 3 and self.code_trust_evidence is None:
            missing.append("code_trust_evidence")
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
        if self.code_trust_evidence is not None:
            if self.schema_version < 3:
                raise ValueError("code_trust_evidence requires research manifest schema 3")
            if self.code_commit not in {None, self.code_trust_evidence.provenance_commit}:
                raise ValueError("code_commit conflicts with signed code trust evidence")
            self.code_commit = self.code_trust_evidence.provenance_commit
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


def require_formal_research_manifest(
    manifest: ResearchManifest,
    *,
    capability: object,
) -> ResearchManifest:
    """Bind a formal research record to one live immutable generation."""

    from rquant.runtime_code_generation import RuntimeCodeGenerationCapability

    if not isinstance(capability, RuntimeCodeGenerationCapability):
        raise ValueError("formal research requires an attested generation capability")
    capability.require_live()
    if (
        manifest.schema_version != 3
        or manifest.research_status == "exploratory"
        or manifest.code_trust_evidence != capability.evidence
    ):
        raise ValueError("formal research manifest code evidence is invalid")
    return manifest


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
        repository = bind_trusted_git_repository(
            git,
            cwd,
            deadline_monotonic=deadline_monotonic,
        )
        result = _run_trusted_git(
            git,
            ["rev-parse", "--verify", "HEAD^{commit}"],
            cwd=repository.checkout_root.path,
            deadline_monotonic=deadline_monotonic,
            repository=repository,
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
            cwd=repository.checkout_root.path,
            deadline_monotonic=deadline_monotonic,
            repository=repository,
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
        repository = bind_trusted_git_repository(
            git,
            cwd,
            deadline_monotonic=deadline_monotonic,
        )
        head = _run_trusted_git(
            git,
            ["rev-parse", "--verify", "HEAD^{commit}"],
            cwd=repository.checkout_root.path,
            deadline_monotonic=deadline_monotonic,
            repository=repository,
        )
        status = _run_trusted_git(
            git,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
            cwd=repository.checkout_root.path,
            text=False,
            deadline_monotonic=deadline_monotonic,
            repository=repository,
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
            cwd=repository.checkout_root.path,
            text=False,
            deadline_monotonic=deadline_monotonic,
            repository=repository,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    commit = head.stdout.strip()
    if (
        head.returncode != 0
        or re.fullmatch(r"[0-9a-f]{40}", commit) is None
        or status.returncode != 0
        or ignored_source_artifacts.returncode != 0
    ):
        return None
    injected = os.getenv("RQUANT_CODE_COMMIT", "").strip()
    if injected and injected != commit:
        return None
    checkout_root = repository.checkout_root.path
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
