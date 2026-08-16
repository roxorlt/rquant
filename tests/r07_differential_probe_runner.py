"""Stdlib-only parent facade for isolated R07 boundary probe subprocesses."""

from __future__ import annotations

import atexit  # noqa: F401  # preload before the per-probe parent snapshot
import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).parents[1]


def _parent_snapshot() -> tuple[
    tuple[tuple[str, str], ...], tuple[str, ...], tuple[tuple[str, int], ...]
]:
    return (
        tuple(os.environ.items()),
        tuple(sys.path),
        tuple((name, id(module)) for name, module in sys.modules.items()),
    )


def _child_environment(environment_root: Path, *, candidate_root: Path = ROOT) -> dict[str, str]:
    environment_root = environment_root.resolve()
    candidate_root = candidate_root.resolve(strict=True)
    candidate_src = (candidate_root / "src").resolve(strict=True)
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
    return {
        "LANG": "C",
        "LC_ALL": "C",
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
        "PYTHONPATH": os.pathsep.join((str(candidate_src), str(candidate_root))),
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTEST_ADDOPTS": "",
        **{name: str(path) for name, path in roots.items()},
        "DUCKDB_PATH": str(environment_root / "data" / "probe.duckdb"),
        "DUCKDB_READONLY_PATH": str(environment_root / "data" / "probe-ro.duckdb"),
        "RQUANT_DISABLE_DOTENV": "1",
        "TUSHARE_TOKEN_MAIN": "0" * 32,
        "NOTIFY_ENABLED": "false",
    }


def run_boundary_probe_subprocess(
    *,
    policy_path: Path,
    candidate_root: Path = ROOT,
    inventory_id: str,
    tmp_path: Path,
    fail_on_dotenv_read: bool = False,
) -> dict[str, object]:
    before = _parent_snapshot()
    tmp_path = tmp_path.resolve()
    tmp_path.mkdir(parents=True, exist_ok=True)
    executable = Path(sys.executable).resolve(strict=True)
    site_packages = (
        Path(sys.prefix)
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    bootstrap = (
        "import runpy, sys; "
        f"site_packages = {str(site_packages)!r}; "
        "sys.path.append(site_packages) if site_packages not in sys.path else None; "
        'entrypoint = "tests.r07_differential_probe_child"; '
        "runpy.run_module(entrypoint, run_name='__main__')"
    )
    try:
        with TemporaryDirectory(prefix="rquant-r07-probe-env-", dir=tmp_path.parent) as directory:
            environment_root = Path(directory).resolve(strict=True)
            environment = _child_environment(environment_root, candidate_root=candidate_root)
            if fail_on_dotenv_read:
                environment["RQUANT_R07_FAIL_DOTENV_READ"] = "1"
            completed = subprocess.run(
                [
                    str(executable),
                    "-c",
                    bootstrap,
                    "--inventory-id",
                    inventory_id,
                    "--tmp-path",
                    str(tmp_path),
                    "--policy-path",
                    str(policy_path.resolve(strict=True)),
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
    finally:
        after = _parent_snapshot()
        if after != before:
            raise AssertionError("parent probe baseline changed around child launch")
