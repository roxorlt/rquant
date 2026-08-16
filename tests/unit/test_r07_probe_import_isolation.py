"""Pre-import isolation contracts for the executable R07 probe harness."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_probe_runner_disables_dotenv_before_any_rquant_import(tmp_path: Path) -> None:
    code = """
import os
from pydantic_settings.sources import DotEnvSettingsSource

def fail_dotenv(*_args, **_kwargs):
    raise AssertionError("dotenv loader reached before probe isolation")

DotEnvSettingsSource.__call__ = fail_dotenv
import tests.r07_differential_probe_runner as runner
assert runner.PREIMPORT_ISOLATED is True
assert os.environ["RQUANT_DISABLE_DOTENV"] == "1"
assert "PUSHDEER_KEYS" not in os.environ
print("preimport-isolated")
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
    assert completed.stdout.strip() == "preimport-isolated"
