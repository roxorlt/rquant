from __future__ import annotations

import shlex
from pathlib import Path


def test_paper_sqlite_image_linux_contract() -> None:
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    job = workflow.split("  paper-sqlite-image-linux:\n", maxsplit=1)[1]
    assert "runs-on: ubuntu-24.04" in job
    assert 'python-version: ["3.11", "3.12"]' in job
    report = "test-results/paper-sqlite-image-${{ matrix.python-version }}.xml"
    nodes = (
        "tests/unit/test_paper_sqlite_image.py::test_cpython_memory_deserialize_query_only",
        "tests/unit/test_paper_sqlite_image.py::test_memory_adapter_unavailable_is_closed",
        "tests/unit/test_paper_sqlite_image.py::test_memory_adapter_deserialize_raise_is_closed",
    )
    command = job.split("uv run pytest", maxsplit=1)[1].split("\n\n", maxsplit=1)[0]
    argv = shlex.split("uv run pytest" + command.replace("\\\n", " "))
    assert argv == [
        "uv",
        "run",
        "pytest",
        "-o",
        "addopts=",
        "-rA",
        f"--junitxml={report}",
        *nodes,
    ]
    assert job.count(report) == 3
    assert "--suites 1 --tests 3 --failures 0 --errors 0 --skipped 0 --cases 3" in job
    assert "if-no-files-found: error" in job
    assert "+" not in argv
    assert "P" + "YVER" not in job
