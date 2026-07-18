"""Strategy Lab 研究记录持久化测试。"""

from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd
import pytest


def test_save_and_load_strategy_lab_run_with_markdown_export(tmp_path) -> None:
    from rquant.dashboard.strategy_lab_runs import (
        build_strategy_lab_run,
        list_strategy_lab_runs,
        load_strategy_lab_run,
        save_strategy_lab_run,
    )

    run = build_strategy_lab_run(
        run_type="n_shape_optimize",
        title="N字自动优化",
        params={
            "start_date": date(2026, 6, 1),
            "end_date": date(2026, 6, 24),
            "entry_modes": ["first_break"],
        },
        metrics={"candidate_count": 42, "estimated_seconds": 12.345},
        tables={
            "策略排行": pd.DataFrame({
                "candidate_id": ["first_break|baseline|h1"],
                "test_mean_ret_pct": [3.2],
                "note": ["可继续观察"],
            }),
        },
        max_rows_per_table=50,
    )

    saved = save_strategy_lab_run(run, base_dir=tmp_path)
    loaded = load_strategy_lab_run(saved.run_id, base_dir=tmp_path)
    runs = list_strategy_lab_runs(base_dir=tmp_path)

    assert loaded.run_id == saved.run_id
    assert loaded.params["start_date"] == "2026-06-01"
    assert loaded.metrics["estimated_seconds"] == 12.345
    assert loaded.tables[0].total_rows == 1
    assert loaded.tables[0].rows[0]["candidate_id"] == "first_break|baseline|h1"
    assert "| candidate_id | test_mean_ret_pct | note |" in loaded.markdown
    assert runs[0].run_id == saved.run_id
    assert saved.json_path.exists()
    assert saved.markdown_path.exists()
    assert loaded.manifest.research_status == "exploratory"
    assert "## 研究可信度" in loaded.markdown
    assert "探索性" in loaded.markdown


def test_run_ids_are_unique_when_two_runs_finish_in_the_same_second(
    tmp_path,
    monkeypatch,
) -> None:
    import rquant.dashboard.strategy_lab_runs as runs_module

    fixed_now = datetime(
        2026,
        7,
        18,
        22,
        0,
        0,
        123456,
        tzinfo=runs_module.CST,
    )

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    monkeypatch.setattr(runs_module, "datetime", FrozenDateTime)
    first = runs_module.build_strategy_lab_run(
        run_type="n_shape_compare",
        title="same second",
        params={},
        metrics={},
        tables={},
    )
    second = runs_module.build_strategy_lab_run(
        run_type="n_shape_compare",
        title="same second",
        params={},
        metrics={},
        tables={},
    )

    assert first.created_at == second.created_at
    assert first.run_id != second.run_id
    runs_module.save_strategy_lab_run(first, base_dir=tmp_path)
    runs_module.save_strategy_lab_run(second, base_dir=tmp_path)
    with pytest.raises(FileExistsError):
        runs_module.save_strategy_lab_run(first, base_dir=tmp_path)


def test_build_strategy_lab_run_truncates_large_tables(tmp_path) -> None:
    from rquant.dashboard.strategy_lab_runs import build_strategy_lab_run, save_strategy_lab_run

    df = pd.DataFrame({"rank": [1, 2, 3], "score": [1.1, None, -0.2]})

    run = build_strategy_lab_run(
        run_type="auction_gap",
        title="集合竞价跳空",
        params={"gap_mode": "close"},
        metrics={"candidate_count": 3},
        tables={"候选明细": df},
        max_rows_per_table=2,
    )
    saved = save_strategy_lab_run(run, base_dir=tmp_path)

    assert saved.tables[0].total_rows == 3
    assert len(saved.tables[0].rows) == 2
    assert saved.tables[0].truncated is True
    assert "| 2 |  |" in saved.markdown


def test_strategy_lab_run_roundtrips_explicit_research_manifest(tmp_path) -> None:
    from rquant.dashboard.strategy_lab_runs import (
        build_strategy_lab_run,
        load_strategy_lab_run,
        save_strategy_lab_run,
    )
    from rquant.research_manifest import ResearchManifest

    manifest = ResearchManifest(
        research_status="comparable",
        status_reason="完整资格全集上的横向比较",
        code_commit="abc123",
        dataset_snapshot_id="snapshot-1",
        dataset_binding_hash="b" * 64,
        coverage_numerator=100,
        coverage_denominator=100,
        data_start_date=date(2025, 1, 1),
        data_end_date=date(2026, 6, 30),
        universe_definition="N字 Pool1+Pool2 资格全集 v1",
        execution_model_version="execution-v1",
        cost_model_version="cost-v1",
    )
    run = build_strategy_lab_run(
        run_type="n_shape_compare",
        title="可信度记录",
        params={},
        metrics={},
        tables={},
        manifest=manifest,
    )

    saved = save_strategy_lab_run(run, base_dir=tmp_path)
    loaded = load_strategy_lab_run(saved.run_id, base_dir=tmp_path)

    assert loaded.manifest.research_status == "comparable"
    assert loaded.manifest.coverage_ratio == 1.0
    assert "可比较" in loaded.markdown
    assert "snapshot-1" in loaded.markdown
    assert len(loaded.manifest.strategy_spec_hash or "") == 64
    assert len(loaded.manifest.result_hash or "") == 64
    assert loaded.manifest.strategy_spec_hash in loaded.markdown
    assert loaded.manifest.result_hash in loaded.markdown


def test_strategy_and_result_hashes_change_only_with_their_own_payloads() -> None:
    from rquant.dashboard.strategy_lab_runs import build_strategy_lab_run

    base = build_strategy_lab_run(
        run_type="auction_gap",
        title="hash",
        params={"hold_days": 1},
        metrics={"mean_ret": 1.0},
        tables={"trades": pd.DataFrame({"ts_code": ["000001.SZ"]})},
    )
    changed_result = build_strategy_lab_run(
        run_type="auction_gap",
        title="hash",
        params={"hold_days": 1},
        metrics={"mean_ret": 2.0},
        tables={"trades": pd.DataFrame({"ts_code": ["000001.SZ"]})},
    )
    changed_spec = build_strategy_lab_run(
        run_type="auction_gap",
        title="hash",
        params={"hold_days": 2},
        metrics={"mean_ret": 1.0},
        tables={"trades": pd.DataFrame({"ts_code": ["000001.SZ"]})},
    )

    assert base.manifest.strategy_spec_hash == changed_result.manifest.strategy_spec_hash
    assert base.manifest.result_hash != changed_result.manifest.result_hash
    assert base.manifest.strategy_spec_hash != changed_spec.manifest.strategy_spec_hash
    assert base.manifest.result_hash == changed_spec.manifest.result_hash


def test_strategy_and_result_hashes_preserve_sub_six_decimal_precision() -> None:
    from rquant.dashboard.strategy_lab_runs import build_strategy_lab_run

    first = build_strategy_lab_run(
        run_type="auction_gap",
        title="precision",
        params={"threshold": 1.0000001},
        metrics={"mean_ret": 2.0000001},
        tables={"trades": pd.DataFrame({"ret": [3.0000001]})},
    )
    changed_spec = build_strategy_lab_run(
        run_type="auction_gap",
        title="precision",
        params={"threshold": 1.0000002},
        metrics={"mean_ret": 2.0000001},
        tables={"trades": pd.DataFrame({"ret": [3.0000001]})},
    )
    changed_metric = build_strategy_lab_run(
        run_type="auction_gap",
        title="precision",
        params={"threshold": 1.0000001},
        metrics={"mean_ret": 2.0000002},
        tables={"trades": pd.DataFrame({"ret": [3.0000001]})},
    )
    changed_table = build_strategy_lab_run(
        run_type="auction_gap",
        title="precision",
        params={"threshold": 1.0000001},
        metrics={"mean_ret": 2.0000001},
        tables={"trades": pd.DataFrame({"ret": [3.0000002]})},
    )

    assert first.manifest.strategy_spec_hash != changed_spec.manifest.strategy_spec_hash
    assert first.manifest.result_hash != changed_metric.manifest.result_hash
    assert first.manifest.result_hash != changed_table.manifest.result_hash


def test_legacy_run_without_manifest_loads_as_exploratory(tmp_path) -> None:
    from rquant.dashboard.strategy_lab_runs import load_strategy_lab_run

    run_id = "20260626-legacy"
    out_dir = tmp_path / "strategy_lab_runs"
    out_dir.mkdir(parents=True)
    (out_dir / f"{run_id}.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "run_type": "auction_gap",
                "title": "旧集合竞价记录",
                "created_at": "2026-06-26T10:00:00+08:00",
                "params": {},
                "metrics": {},
                "tables": [],
                "markdown": "# 旧记录\n",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (out_dir / f"{run_id}.md").write_text(
        "# 旧集合竞价记录\n\n## 手工备注\n\n这段历史结论必须保留。\n",
        encoding="utf-8",
    )

    loaded = load_strategy_lab_run(run_id, base_dir=tmp_path)

    assert loaded.manifest.research_status == "exploratory"
    assert "旧记录" in loaded.manifest.status_reason
    assert "## 研究可信度" in loaded.markdown
    assert "这段历史结论必须保留。" in loaded.markdown
