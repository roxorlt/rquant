from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
GUIDE = ROOT / "docs" / "strategy-lab-auto-optimization-guide.md"
README = ROOT / "README.md"


def test_strategy_lab_guide_documents_the_durable_job_center_flow() -> None:
    guide = GUIDE.read_text(encoding="utf-8")

    for term in (
        "Job Center",
        "typed job",
        "scheduler",
        "worker",
        "checkpoint",
        "ETA",
        "result/artifact catalog",
        "Serving 投影",
        "页面关闭",
        "切换页签",
    ):
        assert term in guide


def test_strategy_lab_guide_separates_new_jobs_from_read_only_legacy_history() -> None:
    guide = GUIDE.read_text(encoding="utf-8")

    for stale_promise in (
        "每次在 Lab 里跑完自动优化或集合竞价跳空，系统都会保存一条研究记录",
        "每条记录会有两个文件",
        ".json`：结构化数据",
        ".md`：人能直接看的复盘报告",
    ):
        assert stale_promise not in guide

    assert "新 Job Center 任务不会写入 `data/strategy_lab_runs/`" in guide
    assert "旧版历史" in guide
    assert "迁移" in guide
    assert "只读" in guide
    assert "Lab Job 账本" in guide
    assert "完整 ZIP" in guide
    assert "SHA256" in guide


def test_readme_keeps_the_public_launcher_and_names_its_job_center() -> None:
    readme = README.read_text(encoding="utf-8")
    launcher = ".venv/bin/streamlit run src/rquant/dashboard/strategy_lab.py --server.port 8504"

    assert launcher in readme
    launch_context = readme[max(0, readme.index(launcher) - 160) : readme.index(launcher) + 160]
    assert "Job Center" in launch_context
