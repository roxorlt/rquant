"""Stdlib-only parent facade for isolated R07 boundary probe subprocesses."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).parents[1]

_SENSITIVE_CHILD_ENVIRONMENT = frozenset(
    {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "DEEPSEEK_API_KEY",
        "PANORAMA_COOKIE_SECRET",
        "PANORAMA_GATE_TOKEN",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "RQUANT_PANORAMA_GATE_TOKEN",
        "TUSHARE_TOKEN_BACKUP",
        "TUSHARE_TOKEN_MAIN",
    }
)


def _child_environment(environment_root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    for name in _SENSITIVE_CHILD_ENVIRONMENT:
        environment.pop(name, None)
    roots = {
        "HOME": environment_root / "home",
        "TMPDIR": environment_root / "tmp",
        "TMP": environment_root / "tmp",
        "TEMP": environment_root / "tmp",
        "DATA_DIR": environment_root / "data",
        "PARQUET_DIR": environment_root / "parquet",
        "LOG_DIR": environment_root / "logs",
    }
    for path in set(roots.values()):
        path.mkdir(parents=True, exist_ok=True)
    existing_pythonpath = environment.get("PYTHONPATH", "")
    environment.update(
        {
            **{name: str(path) for name, path in roots.items()},
            "PYTHONPATH": os.pathsep.join(
                path for path in (str(ROOT), existing_pythonpath) if path
            ),
            "PYTHONNOUSERSITE": "1",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            "RQUANT_DISABLE_DOTENV": "1",
            "DUCKDB_PATH": str(environment_root / "data" / "probe.duckdb"),
            "DUCKDB_READONLY_PATH": str(environment_root / "data" / "probe-ro.duckdb"),
            "TUSHARE_TOKEN_MAIN": "0" * 32,
            "NOTIFY_ENABLED": "false",
        }
    )
    return environment


def run_boundary_probe_subprocess(
    *,
    policy_path: Path,
    inventory_id: str,
    tmp_path: Path,
    fail_on_dotenv_read: bool = False,
) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="rquant-r07-probe-env-", dir=tmp_path.parent) as directory:
        environment_root = Path(directory)
        environment = _child_environment(environment_root)
        if fail_on_dotenv_read:
            environment["RQUANT_R07_FAIL_DOTENV_READ"] = "1"
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.r07_differential_probe_child",
                "--inventory-id",
                inventory_id,
                "--tmp-path",
                str(tmp_path),
                "--policy-path",
                str(policy_path),
            ],
            cwd=environment_root,
            env=environment,
            capture_output=True,
            check=False,
            text=True,
        )
    if completed.returncode != 0:
        raise AssertionError(completed.stdout + completed.stderr)
    payload = json.loads(completed.stdout)
    if type(payload) is not dict:
        raise AssertionError("probe child result must be a JSON object")
    return payload
