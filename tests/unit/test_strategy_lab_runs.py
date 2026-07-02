"""Strategy Lab 研究记录持久化测试。"""

from __future__ import annotations

from datetime import date

import pandas as pd


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
