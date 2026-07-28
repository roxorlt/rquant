"""研究可信度 manifest 行为测试。"""

from __future__ import annotations

import importlib.util
import marshal
import os
import struct
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError


def test_exploratory_manifest_allows_unknown_evidence() -> None:
    from rquant.research_manifest import ResearchManifest

    manifest = ResearchManifest(
        research_status="exploratory",
        status_reason="旧结果缺少数据快照",
    )

    assert manifest.coverage_ratio is None
    assert manifest.code_commit is None
    assert manifest.missing_evidence == [
        "code_commit",
        "dataset_snapshot_id",
        "coverage_counts",
        "data_range",
        "universe_definition",
        "execution_model_version",
        "cost_model_version",
    ]


def test_comparable_manifest_requires_all_core_evidence() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="comparable 缺少证据"):
        ResearchManifest(
            research_status="comparable",
            status_reason="准备横向比较",
            code_commit="abc123",
        )


def test_manifest_computes_coverage_ratio_from_counts() -> None:
    from rquant.research_manifest import ResearchManifest

    manifest = ResearchManifest(
        research_status="comparable",
        status_reason="资格全集和执行模型均已冻结",
        code_commit="abc123",
        dataset_snapshot_id="snapshot-20260713",
        coverage_numerator=99,
        coverage_denominator=100,
        data_start_date=date(2025, 1, 1),
        data_end_date=date(2026, 6, 30),
        universe_definition="创业板和科创板均线多头资格全集 v1",
        execution_model_version="execution-v1",
        cost_model_version="cost-cn-a-v1",
    )

    assert manifest.coverage_ratio == pytest.approx(0.99)
    assert manifest.missing_evidence == []


def test_manifest_v2_requires_and_preserves_execution_hashes() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="dataset_binding_hash"):
        ResearchManifest(
            schema_version=2,
            research_status="comparable",
            status_reason="绑定执行数据",
            code_commit="abc123",
            dataset_snapshot_id="snapshot-20260713",
            coverage_numerator=100,
            coverage_denominator=100,
            data_start_date=date(2025, 1, 1),
            data_end_date=date(2026, 6, 30),
            universe_definition="资格全集 v1",
            execution_model_version="execution-v1",
            cost_model_version="cost-v1",
        )

    with pytest.raises(ValidationError, match="strategy_spec_hash, result_hash"):
        ResearchManifest(
            schema_version=2,
            research_status="comparable",
            status_reason="绑定执行数据",
            code_commit="abc123",
            dataset_snapshot_id="snapshot-20260713",
            dataset_binding_hash="b" * 64,
            coverage_numerator=100,
            coverage_denominator=100,
            data_start_date=date(2025, 1, 1),
            data_end_date=date(2026, 6, 30),
            universe_definition="资格全集 v1",
            execution_model_version="execution-v1",
            cost_model_version="cost-v1",
        )

    manifest = ResearchManifest(
        schema_version=2,
        research_status="comparable",
        status_reason="绑定执行数据",
        code_commit="abc123",
        dataset_snapshot_id="snapshot-20260713",
        dataset_binding_hash="b" * 64,
        coverage_numerator=100,
        coverage_denominator=100,
        data_start_date=date(2025, 1, 1),
        data_end_date=date(2026, 6, 30),
        universe_definition="资格全集 v1",
        execution_model_version="execution-v1",
        cost_model_version="cost-v1",
        strategy_spec_hash="c" * 64,
        result_hash="d" * 64,
    )

    assert manifest.schema_version == 2
    assert manifest.dataset_binding_hash == "b" * 64
    assert manifest.strategy_spec_hash == "c" * 64
    assert manifest.result_hash == "d" * 64
    assert manifest.missing_evidence == []


def test_comparable_manifest_does_not_accept_ratio_without_counts() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="coverage_counts"):
        ResearchManifest(
            research_status="comparable",
            status_reason="不能只填一个比例",
            code_commit="abc123",
            dataset_snapshot_id="snapshot-20260713",
            coverage_ratio=1.0,
            data_start_date=date(2025, 1, 1),
            data_end_date=date(2026, 6, 30),
            universe_definition="资格全集 v1",
            execution_model_version="execution-v1",
            cost_model_version="cost-v1",
        )


def test_paper_candidate_requires_enough_out_of_sample_trades() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="至少需要 100 笔"):
        ResearchManifest(
            research_status="paper_candidate",
            status_reason="样本外候选",
            code_commit="abc123",
            dataset_snapshot_id="snapshot-20260713",
            coverage_numerator=100,
            coverage_denominator=100,
            data_start_date=date(2025, 1, 1),
            data_end_date=date(2026, 6, 30),
            universe_definition="资格全集 v1",
            execution_model_version="execution-v1",
            cost_model_version="cost-v1",
            validation_method="nested-walk-forward-v1",
            out_of_sample_trades=99,
        )


def test_monitor_approved_requires_forward_days_and_fills() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="至少需要 30 笔前瞻成交"):
        ResearchManifest(
            research_status="monitor_approved",
            status_reason="前瞻观察结束",
            code_commit="abc123",
            dataset_snapshot_id="snapshot-20260713",
            coverage_numerator=100,
            coverage_denominator=100,
            data_start_date=date(2025, 1, 1),
            data_end_date=date(2026, 6, 30),
            universe_definition="资格全集 v1",
            execution_model_version="execution-v1",
            cost_model_version="cost-v1",
            validation_method="nested-walk-forward-v1",
            out_of_sample_trades=120,
            forward_validation_days=20,
            forward_filled_trades=29,
        )


def test_non_exploratory_manifest_rejects_dirty_commit() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="脏工作树"):
        ResearchManifest(
            research_status="comparable",
            status_reason="不应晋级",
            code_commit="abc123-dirty",
            dataset_snapshot_id="snapshot-20260713",
            coverage_numerator=100,
            coverage_denominator=100,
            data_start_date=date(2025, 1, 1),
            data_end_date=date(2026, 6, 30),
            universe_definition="资格全集 v1",
            execution_model_version="execution-v1",
            cost_model_version="cost-v1",
        )


def test_detect_code_commit_marks_dirty_worktree(tmp_path) -> None:
    from rquant.research_manifest import detect_code_commit

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    tracked = repo / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )

    clean_commit = detect_code_commit(repo)
    tracked.write_text("dirty\n", encoding="utf-8")
    dirty_commit = detect_code_commit(repo)

    assert clean_commit is not None and not clean_commit.endswith("-dirty")
    assert dirty_commit == f"{clean_commit}-dirty"


def test_detect_code_commit_ignores_project_runtime_backup_directory(
    tmp_path: Path,
) -> None:
    from rquant.research_manifest import detect_code_commit

    project_root = Path(__file__).resolve().parents[2]
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / ".gitignore").write_text(
        (project_root / ".gitignore").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    tracked = repo / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", ".gitignore", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    clean_commit = detect_code_commit(repo)
    backup_dir = repo / "backup"
    backup_dir.mkdir()
    (backup_dir / "snapshot.duckdb.gz").write_bytes(b"runtime backup")

    observed_commit = detect_code_commit(repo)

    assert clean_commit is not None
    assert observed_commit == clean_commit


def test_detect_verified_code_commit_rejects_injected_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    tracked = repo / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    monkeypatch.setenv("RQUANT_CODE_COMMIT", "f" * 40)
    assert detect_verified_code_commit(repo) is None

    monkeypatch.setenv("RQUANT_CODE_COMMIT", head)
    assert detect_verified_code_commit(repo) == head

    tracked.write_text("dirty\n", encoding="utf-8")
    assert detect_verified_code_commit(repo) == f"{head}-dirty"


def test_detect_verified_code_commit_uses_explicit_trusted_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    repo = tmp_path / "repo"
    fake_bin = repo / ".venv" / "bin"
    fake_bin.mkdir(parents=True)
    subprocess.run(["/usr/bin/git", "init", "-q"], cwd=repo, check=True)
    tracked = repo / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["/usr/bin/git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "/usr/bin/git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    marker = tmp_path / "fake-git-ran"
    fake_git = fake_bin / "git"
    fake_git.write_text(f"#!/bin/sh\ntouch {marker!s}\nexit 0\n", encoding="utf-8")
    fake_git.chmod(0o700)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ.get('PATH', '')}")
    tracked.write_text("dirty\n", encoding="utf-8")

    observed = detect_verified_code_commit(
        repo,
        trusted_git_path=Path("/usr/bin/git"),
    )

    assert observed is not None and observed.endswith("-dirty")
    assert not marker.exists()


def test_research_manifest_readonly_git_preserves_index_and_disables_optional_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.research_manifest as module

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["/usr/bin/git", "init", "-q"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["/usr/bin/git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "/usr/bin/git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    index = repo / ".git" / "index"
    before = (index.read_bytes(), index.stat())
    original_run = subprocess.run
    environments: list[dict[str, str]] = []

    def capture_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        command = args[0]
        if isinstance(command, list) and command and command[0] == "/usr/bin/git":
            environment = kwargs.get("env")
            assert isinstance(environment, dict)
            environments.append(environment)
        return original_run(*args, **kwargs)

    monkeypatch.setattr(module.subprocess, "run", capture_run)

    assert module.detect_verified_code_commit(repo, trusted_git_path=Path("/usr/bin/git"))
    after = index.stat()
    assert environments
    assert all(environment["GIT_OPTIONAL_LOCKS"] == "0" for environment in environments)
    assert index.read_bytes() == before[0]
    assert (after.st_ino, after.st_mtime_ns) == (before[1].st_ino, before[1].st_mtime_ns)


def test_trusted_git_binding_rejects_symlink(tmp_path: Path) -> None:
    from rquant.research_manifest import bind_trusted_git_executable

    linked_git = tmp_path / "linked-git"
    linked_git.symlink_to("/usr/bin/git")

    with pytest.raises(ValueError, match="physical"):
        bind_trusted_git_executable(linked_git)


def test_detect_verified_code_commit_rejects_unignored_worktree_venv_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    worktree = tmp_path / "worktree"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    (repo / ".venv").mkdir(mode=0o755)
    subprocess.run(
        ["git", "worktree", "add", "-q", "-b", "fixture-worktree", str(worktree)],
        cwd=repo,
        check=True,
    )
    (worktree / ".venv").symlink_to(repo / ".venv", target_is_directory=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert detect_verified_code_commit(worktree) == f"{head}-dirty"


def test_detect_verified_code_commit_uses_precise_gitignore_for_worktree_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    worktree = tmp_path / "worktree"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / ".gitignore").write_text("/.venv\n", encoding="utf-8")
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", ".gitignore", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    (repo / ".venv").mkdir(mode=0o755)
    subprocess.run(
        ["git", "worktree", "add", "-q", "-b", "fixture-worktree", str(worktree)],
        cwd=repo,
        check=True,
    )
    (worktree / ".venv").symlink_to(repo / ".venv", target_is_directory=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert detect_verified_code_commit(worktree) == head


@pytest.mark.parametrize(
    "artifact_name",
    ["trusted.so", "native.dylib", "native.pyd", "legacy.pyc", "legacy.pyo"],
)
def test_detect_verified_code_commit_rejects_ignored_loadable_source_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_name: str,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    package = repo / "src" / "rquant"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / ".gitignore").write_text("*.so\n*.dylib\n*.pyd\n*.pyc\n*.pyo\n", encoding="utf-8")
    (package / "trusted.py").write_text("VALUE = 'tracked'\n", encoding="utf-8")
    subprocess.run(["git", "add", ".gitignore", "src/rquant/trusted.py"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (package / artifact_name).write_bytes(b"ignored executable payload")

    assert detect_verified_code_commit(repo) == f"{head}-dirty"


def test_detect_verified_code_commit_rejects_executable_timestamp_bytecode_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    package = repo / "src" / "rquant"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / ".gitignore").write_text("__pycache__/\n*.pyc\n*.pyo\n", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    source = package / "timestamp_payload.py"
    source.write_text("VALUE = 'safe'\n", encoding="utf-8")
    subprocess.run(
        [
            "git",
            "add",
            ".gitignore",
            "src/rquant/__init__.py",
            "src/rquant/timestamp_payload.py",
        ],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    source_stat = source.stat()
    cache = Path(importlib.util.cache_from_source(str(source)))
    cache.parent.mkdir()
    malicious = compile("VALUE = 'evil'\n", str(source), "exec")
    cache.write_bytes(
        importlib.util.MAGIC_NUMBER
        + struct.pack(
            "<III",
            0,
            int(source_stat.st_mtime) & 0xFFFF_FFFF,
            source_stat.st_size,
        )
        + marshal.dumps(malicious)
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repo / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    imported = subprocess.run(
        [
            sys.executable,
            "-S",
            "-c",
            "from rquant import timestamp_payload; print(timestamp_payload.VALUE)",
        ],
        cwd=repo,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert imported.stdout.strip() == "evil"
    assert (
        subprocess.run(
            ["git", "status", "--porcelain=v1"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        == ""
    )
    assert detect_verified_code_commit(repo) == f"{head}-dirty"


def test_detect_verified_code_commit_rejects_ignored_package_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    package = repo / "src" / "rquant"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / ".gitignore").write_text("/src/rquant/plugged\n", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    subprocess.run(["git", "add", ".gitignore", "src/rquant/__init__.py"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    payload = tmp_path / "payload-package"
    payload.mkdir()
    (payload / "__init__.py").write_text("VALUE = 'ignored'\n", encoding="utf-8")
    (package / "plugged").symlink_to(payload, target_is_directory=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert detect_verified_code_commit(repo) == f"{head}-dirty"


@pytest.mark.parametrize(
    ("name", "executable"),
    [("payload.py", False), ("runtime-tool", True), ("notes.txt", False)],
)
def test_detect_verified_code_commit_rejects_untrusted_untracked_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    executable: bool,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    payload = repo / name
    payload.write_text("untrusted\n", encoding="utf-8")
    if executable:
        payload.chmod(0o755)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert detect_verified_code_commit(repo) == f"{head}-dirty"


def test_detect_verified_code_commit_rejects_venv_symlink_to_unapproved_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    approved = repo / ".venv"
    approved.mkdir(mode=0o755)
    worktree = tmp_path / "worktree"
    subprocess.run(
        ["git", "worktree", "add", "-q", "-b", "fixture-worktree", str(worktree)],
        cwd=repo,
        check=True,
    )
    unapproved = tmp_path / "other-venv"
    unapproved.mkdir(mode=0o755)
    (worktree / ".venv").symlink_to(unapproved, target_is_directory=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert detect_verified_code_commit(worktree) == f"{head}-dirty"


def test_detect_verified_code_commit_rejects_symlinked_authoritative_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    monkeypatch.delenv("RQUANT_CODE_COMMIT", raising=False)
    repo = tmp_path / "repo"
    worktree = tmp_path / "worktree"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rquant-ci",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=repo,
        check=True,
    )
    actual = tmp_path / "shared-venv"
    actual.mkdir(mode=0o755)
    (repo / ".venv").symlink_to(actual, target_is_directory=True)
    subprocess.run(
        ["git", "worktree", "add", "-q", "-b", "fixture-worktree", str(worktree)],
        cwd=repo,
        check=True,
    )
    (worktree / ".venv").symlink_to(repo / ".venv", target_is_directory=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert detect_verified_code_commit(worktree) == f"{head}-dirty"


def test_manifest_rejects_covered_count_above_denominator() -> None:
    from rquant.research_manifest import ResearchManifest

    with pytest.raises(ValidationError, match="不能大于"):
        ResearchManifest(
            research_status="exploratory",
            status_reason="坏数据",
            coverage_numerator=101,
            coverage_denominator=100,
        )


def test_current_notices_cover_all_untrusted_strategy_families() -> None:
    from rquant.research_manifest import CURRENT_RESEARCH_NOTICES

    covered = {
        run_type for notice in CURRENT_RESEARCH_NOTICES for run_type in notice.affected_run_types
    }

    assert {
        "n_shape_compare",
        "n_shape_optimize",
        "growth_board_surge",
        "auction_gap",
    } <= covered
