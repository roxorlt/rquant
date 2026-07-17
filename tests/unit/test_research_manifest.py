"""研究可信度 manifest 行为测试。"""

from __future__ import annotations

import subprocess
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
        run_type
        for notice in CURRENT_RESEARCH_NOTICES
        for run_type in notice.affected_run_types
    }

    assert {
        "n_shape_compare",
        "n_shape_optimize",
        "growth_board_surge",
        "auction_gap",
    } <= covered
