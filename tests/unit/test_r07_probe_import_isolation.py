"""Parent/child import isolation contracts for the executable R07 probe harness."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_probe_runner_disables_dotenv_before_any_rquant_import(
    tmp_path: Path,
) -> None:
    policy_path = ROOT / "tests/fixtures/r07_differential_gate/policy-v1.json"
    probe_path = tmp_path / "probe"
    code = f"""
import json
import os
import sys
from pathlib import Path

before_environment = tuple(os.environ.items())
before_rquant_modules = {{
    name
    for name in sys.modules
    if name == "rquant" or name.startswith("rquant.")
}}
import tests.r07_differential_probe_runner as runner
assert tuple(os.environ.items()) == before_environment
after_rquant_modules = {{
    name
    for name in sys.modules
    if name == "rquant" or name.startswith("rquant.")
}}
assert after_rquant_modules == before_rquant_modules
result = runner.run_boundary_probe_subprocess(
    policy_path=Path({str(policy_path)!r}),
    inventory_id="R07-B01",
    tmp_path=Path({str(probe_path)!r}),
    fail_on_dotenv_read=True,
)
assert tuple(os.environ.items()) == before_environment
assert result["inventory_id"] == "R07-B01"
assert result["passed"] is True
print(json.dumps({{"status": "parent-unchanged-child-isolated"}}, sort_keys=True))
"""
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(tmp_path / "home"),
        "TMPDIR": str(tmp_path / "tmp"),
        "TMP": str(tmp_path / "tmp"),
        "TEMP": str(tmp_path / "tmp"),
        "PYTHONPATH": str(ROOT),
        "PYTHONNOUSERSITE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "DATA_DIR": str(tmp_path / "data"),
        "DUCKDB_PATH": str(tmp_path / "data" / "probe.duckdb"),
        "DUCKDB_READONLY_PATH": str(tmp_path / "data" / "probe-ro.duckdb"),
        "PARQUET_DIR": str(tmp_path / "parquet"),
        "LOG_DIR": str(tmp_path / "logs"),
        "TUSHARE_TOKEN_MAIN": "0" * 32,
        "NOTIFY_ENABLED": "false",
        "PUSHDEER_KEYS": "must-be-cleared-before-rquant-import",
    }
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert json.loads(completed.stdout) == {"status": "parent-unchanged-child-isolated"}
