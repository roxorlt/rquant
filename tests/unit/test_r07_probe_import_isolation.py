"""Parent/child import isolation contracts for the executable R07 probe harness."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_child_environment_is_empty_derived_and_has_fixed_two_entry_pythonpath(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from tests.r07_differential_probe_runner import _child_environment

    monkeypatch.setenv("PATH", "/poisoned/path")
    monkeypatch.setenv("UNKNOWN_SECRET", "must-not-cross")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-cross")
    monkeypatch.setenv("RQUANT_PRODUCTION_SENTINEL", "must-not-cross")
    monkeypatch.setenv("LANG", "poisoned-locale")
    candidate_root = (tmp_path / "candidate").resolve()
    (candidate_root / "src").mkdir(parents=True)
    environment_root = tmp_path / "child"

    environment = _child_environment(environment_root, candidate_root=candidate_root)

    assert "PATH" not in environment
    assert "UNKNOWN_SECRET" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "RQUANT_PRODUCTION_SENTINEL" not in environment
    assert environment["LANG"] == "C"
    assert environment["LC_ALL"] == "C"
    assert environment["PYTHONPATH"] == os.pathsep.join(
        (str(candidate_root / "src"), str(candidate_root))
    )
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert environment["PYTHONDONTWRITEBYTECODE"] == "1"


def test_probe_runner_disables_dotenv_before_any_rquant_import(
    tmp_path: Path,
) -> None:
    policy_path = ROOT / "tests/fixtures/r07_differential_gate/policy-v1.json"
    probe_path = tmp_path / "probe"
    code = f"""
import importlib.util
import json
import os
import sys
from pathlib import Path

facade_name = "tests.r07_differential_probe_runner"
facade_spec = importlib.util.find_spec(facade_name)
assert facade_spec is not None
assert facade_spec.loader is not None
runner = importlib.util.module_from_spec(facade_spec)
before_environment = tuple(os.environ.items())
before_path = tuple(sys.path)
before_module_keys = frozenset(sys.modules)
facade_spec.loader.exec_module(runner)
assert tuple(os.environ.items()) == before_environment
assert tuple(sys.path) == before_path
assert frozenset(sys.modules) == before_module_keys
before_modules = tuple((name, id(module)) for name, module in sys.modules.items())
result = runner.run_boundary_probe_subprocess(
    policy_path=Path({str(policy_path)!r}),
    inventory_id="R07-B01",
    tmp_path=Path({str(probe_path)!r}),
    fail_on_dotenv_read=True,
)
assert tuple(os.environ.items()) == before_environment
assert tuple(sys.path) == before_path
assert tuple((name, id(module)) for name, module in sys.modules.items()) == before_modules
assert result["inventory_id"] == "R07-B01"
assert result["passed"] is True
try:
    runner.run_boundary_probe_subprocess(
        policy_path=Path({str(policy_path)!r}),
        inventory_id="R07-B99",
        tmp_path=Path({str(probe_path)!r}),
    )
except AssertionError:
    pass
else:
    raise AssertionError("invalid inventory must fail")
assert tuple(os.environ.items()) == before_environment
assert tuple(sys.path) == before_path
assert tuple((name, id(module)) for name, module in sys.modules.items()) == before_modules
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
