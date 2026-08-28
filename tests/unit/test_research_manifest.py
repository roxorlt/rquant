"""研究可信度 manifest 行为测试。"""

from __future__ import annotations

import os
import subprocess
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError


def _commit_fixture(repo: Path, *, message: str) -> str:
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("same checkout contents\n", encoding="utf-8")
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
            message,
        ],
        cwd=repo,
        check=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


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


def test_trusted_git_subprocess_scrubs_git_environment_and_binds_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import research_manifest as manifest_module

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    trusted_git_path = Path("/trusted/git")
    binding = SimpleNamespace(path=trusted_git_path)
    captured: dict[str, object] = {}
    poisoned = {
        "GIT_DIR": "/poison/repo.git",
        "GIT_WORK_TREE": "/poison/worktree",
        "GIT_INDEX_FILE": "/poison/index",
        "GIT_OBJECT_DIRECTORY": "/poison/objects",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES": "/poison/alternate-objects",
        "GIT_COMMON_DIR": "/poison/common",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.filemode",
        "GIT_CONFIG_VALUE_0": "false",
        "GIT_CONFIG_GLOBAL": "/poison/global-config",
        "GIT_CONFIG_NOSYSTEM": "0",
        "GIT_CONFIG_SYSTEM": "/poison/system-config",
        "GIT_CEILING_DIRECTORIES": "/poison/ceiling",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM": "1",
        "GIT_EXEC_PATH": "/poison/exec-path",
        "GIT_ATTR_NOSYSTEM": "0",
        "GIT_OPTIONAL_LOCKS": "1",
        "GIT_REPLACE_REF_BASE": "refs/replace-poison",
        "GIT_TERMINAL_PROMPT": "1",
        "GIT_LITERAL_PATHSPECS": "1",
        "GIT_POISON_FUTURE_BEHAVIOR": "1",
    }
    for key, value in poisoned.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        manifest_module,
        "bind_trusted_git_executable",
        lambda path: binding,
    )

    def run_probe(command: list[str], **kwargs: object) -> SimpleNamespace:
        captured["command"] = command
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(manifest_module, "run_contained", run_probe)

    manifest_module._run_trusted_git(
        binding,
        ["rev-parse", "--show-toplevel"],
        cwd=checkout,
        deadline_monotonic=42.0,
    )

    command = captured["command"]
    assert isinstance(command, list)
    assert command[0] == str(trusted_git_path)
    assert "--no-replace-objects" in command
    assert command[command.index("-C") + 1] == str(checkout.resolve())
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert environment["GIT_CONFIG_NOSYSTEM"] == "1"
    assert environment["GIT_CONFIG_GLOBAL"] == os.devnull
    assert environment["GIT_CONFIG_SYSTEM"] == os.devnull
    assert environment["GIT_ATTR_NOSYSTEM"] == "1"
    assert environment["GIT_OPTIONAL_LOCKS"] == "0"
    assert environment["GIT_TERMINAL_PROMPT"] == "0"
    safe_overrides = {
        "GIT_ATTR_NOSYSTEM",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_SYSTEM",
        "GIT_OPTIONAL_LOCKS",
        "GIT_TERMINAL_PROMPT",
    }
    assert not ((set(poisoned) - safe_overrides) & set(environment))


def test_detect_verified_code_commit_rejects_inherited_git_routing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    expected = tmp_path / "expected"
    alternate = tmp_path / "alternate"
    expected_head = _commit_fixture(expected, message="expected")
    alternate_head = _commit_fixture(alternate, message="alternate")
    assert alternate_head != expected_head
    alternate_config = alternate / ".git" / "config"
    poisoned = {
        "GIT_DIR": str(alternate / ".git"),
        "GIT_WORK_TREE": str(expected),
        "GIT_INDEX_FILE": str(alternate / ".git" / "index"),
        "GIT_OBJECT_DIRECTORY": str(alternate / ".git" / "objects"),
        "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(expected / ".git" / "objects"),
        "GIT_COMMON_DIR": str(alternate / ".git"),
        "GIT_CONFIG_GLOBAL": str(alternate_config),
        "GIT_CONFIG_SYSTEM": str(alternate_config),
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.filemode",
        "GIT_CONFIG_VALUE_0": "false",
        "GIT_CEILING_DIRECTORIES": str(tmp_path),
        "GIT_DISCOVERY_ACROSS_FILESYSTEM": "1",
        "GIT_EXEC_PATH": subprocess.run(
            ["git", "--exec-path"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "GIT_REPLACE_REF_BASE": "refs/replace-poison",
    }
    for key, value in poisoned.items():
        monkeypatch.setenv(key, value)

    assert detect_verified_code_commit(expected) == expected_head


def test_detect_verified_code_commit_rejects_another_absolute_top_level(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import research_manifest as manifest_module

    expected = tmp_path / "expected"
    other = tmp_path / "other"
    expected.mkdir()
    other.mkdir()
    trusted_git_path = Path("/trusted/git")
    binding = SimpleNamespace(path=trusted_git_path)

    monkeypatch.setattr(
        manifest_module,
        "bind_trusted_git_executable",
        lambda path: binding,
    )

    def probe(
        received_binding: object,
        arguments: list[str],
        *,
        cwd: Path,
        text: bool = True,
        deadline_monotonic: float | None = None,
    ) -> SimpleNamespace:
        del text, deadline_monotonic
        assert received_binding is binding
        assert cwd == expected
        if arguments[:2] == ["rev-parse", "--path-format=absolute"]:
            return SimpleNamespace(
                returncode=0,
                stdout=f"{other}\nunused-git-dir\nunused-common\nunused-objects\nunused-index\n",
            )
        raise AssertionError(f"unexpected trusted Git probe: {arguments}")

    monkeypatch.setattr(manifest_module, "_run_trusted_git", probe)

    assert (
        manifest_module.detect_verified_code_commit(
            expected,
            trusted_git_path=trusted_git_path,
        )
        is None
    )


def test_detect_verified_code_commit_supports_linked_worktree(tmp_path: Path) -> None:
    from rquant.research_manifest import detect_verified_code_commit

    primary = tmp_path / "primary"
    linked = tmp_path / "linked"
    head = _commit_fixture(primary, message="linked fixture")
    subprocess.run(
        ["git", "worktree", "add", "--detach", "-q", str(linked), "HEAD"],
        cwd=primary,
        check=True,
    )

    assert detect_verified_code_commit(linked) == head


def test_detect_verified_code_commit_uses_trusted_git_contract_for_ignored_native(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import research_manifest as manifest_module

    checkout = tmp_path / "checkout"
    artifact = checkout / "src" / "rquant" / "runtime.so"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"native")
    trusted_git_path = Path("/trusted/git")
    binding = SimpleNamespace(path=trusted_git_path)
    repository_binding = SimpleNamespace(checkout_root=SimpleNamespace(path=checkout))
    code_sha = "a" * 40
    calls: list[tuple[tuple[str, ...], bool, float | None]] = []
    repository_deadlines: list[float | None] = []

    monkeypatch.setattr(
        manifest_module,
        "bind_trusted_git_executable",
        lambda path: binding,
    )

    def bind_repository(
        received_binding: object,
        received_checkout: Path,
        *,
        deadline_monotonic: float | None = None,
    ) -> object:
        assert received_binding is binding
        assert received_checkout == checkout
        repository_deadlines.append(deadline_monotonic)
        return repository_binding

    monkeypatch.setattr(manifest_module, "bind_trusted_git_repository", bind_repository)

    def probe(
        received_binding: object,
        arguments: list[str],
        *,
        cwd: Path,
        text: bool = True,
        deadline_monotonic: float | None = None,
        repository: object | None = None,
    ) -> SimpleNamespace:
        assert received_binding is binding
        assert cwd == checkout
        assert repository is repository_binding
        calls.append((tuple(arguments), text, deadline_monotonic))
        if arguments == ["rev-parse", "--verify", "HEAD^{commit}"]:
            return SimpleNamespace(returncode=0, stdout=f"{code_sha}\n")
        if arguments == ["status", "--porcelain=v1", "-z", "--untracked-files=all"]:
            return SimpleNamespace(returncode=0, stdout=b"")
        if arguments == [
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
            "--",
            ":(top)src/rquant",
        ]:
            return SimpleNamespace(returncode=0, stdout=b"src/rquant/runtime.so\0")
        raise AssertionError(f"unexpected trusted Git probe: {arguments}")

    monkeypatch.setattr(manifest_module, "_run_trusted_git", probe)

    assert (
        manifest_module.detect_verified_code_commit(
            checkout,
            trusted_git_path=trusted_git_path,
            deadline_monotonic=42.0,
        )
        == f"{code_sha}-dirty"
    )
    assert repository_deadlines == [42.0]
    assert calls == [
        (("rev-parse", "--verify", "HEAD^{commit}"), True, 42.0),
        (("status", "--porcelain=v1", "-z", "--untracked-files=all"), False, 42.0),
        (
            (
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "-z",
                "--",
                ":(top)src/rquant",
            ),
            False,
            42.0,
        ),
    ]


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
