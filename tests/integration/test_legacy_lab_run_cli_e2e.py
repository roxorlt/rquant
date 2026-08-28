from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_legacy_lab_run_cli_rejects_unbound_research_spec(tmp_path: Path) -> None:
    """The public legacy CLI must fail before it can choose a main/replica store."""
    spec = tmp_path / "strategy_lab_runs" / "legacy-unbound.spec.json"
    spec.parent.mkdir()
    spec.write_text(
        json.dumps(
            {
                "run_type": "n_shape_optimize",
                "run_id": "legacy-unbound",
                "base_dir": str(tmp_path),
                "params": {"start_date": "2026-07-01", "end_date": "2026-07-31"},
            }
        ),
        encoding="utf-8",
    )
    source_root = Path(__file__).resolve().parents[2] / "src"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = (
        f"{source_root}{os.pathsep}{environment['PYTHONPATH']}"
        if environment.get("PYTHONPATH")
        else str(source_root)
    )

    result = subprocess.run(
        [sys.executable, "-m", "rquant.cli", "lab-run", "--spec", str(spec)],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 1
    status = json.loads((spec.parent / "legacy-unbound.status.json").read_text(encoding="utf-8"))
    assert status["state"] == "error"
    assert "immutable research snapshot" in status["error"]
