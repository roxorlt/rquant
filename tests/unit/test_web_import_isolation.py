"""The web API package reads Serving and nothing else, and runs without configuration.

Static half: no module under ``src/rquant/web/`` imports ``rquant.config`` (constructing it
reads ``.env``), ``rquant.storage`` (the main database and its replica) or ``duckdb``
(every DuckDB connection goes through ``ServingReader``). Runtime half: importing the app
and driving ``rquant web-openapi`` through ``rquant.cli.main`` in an environment with no
configuration at all never loads those modules.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_PACKAGE = REPO_ROOT / "src" / "rquant" / "web"
FORBIDDEN_PREFIXES = ("rquant.config", "rquant.storage", "duckdb")
CONFIGURATION_FREE_ENVIRONMENT = {
    "PATH": os.environ.get("PATH", os.defpath),
    "LANG": "C",
    "PYTHONPATH": str(REPO_ROOT / "src"),
    "RQUANT_DISABLE_DOTENV": "1",
}


def _modules() -> list[Path]:
    return sorted(WEB_PACKAGE.rglob("*.py"))


def _imported_names(tree: ast.AST) -> list[str]:
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            names.append(node.module)
            names.extend(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def test_the_package_has_modules_to_check() -> None:
    assert {path.name for path in _modules()} >= {"app.py", "serving.py", "cli.py", "meta.py"}


@pytest.mark.parametrize(
    "module", _modules(), ids=lambda path: path.relative_to(WEB_PACKAGE).as_posix()
)
def test_no_web_module_imports_configuration_storage_or_duckdb(module: Path) -> None:
    tree = ast.parse(module.read_text(encoding="utf-8"), filename=str(module))
    offending = [
        name
        for name in _imported_names(tree)
        if any(name == prefix or name.startswith(prefix + ".") for prefix in FORBIDDEN_PREFIXES)
    ]
    assert offending == []
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "connect"
    ]
    assert calls == []


def _run(program: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, "-c", program),
        cwd=str(REPO_ROOT),
        env=dict(CONFIGURATION_FREE_ENVIRONMENT),
        capture_output=True,
        text=True,
        check=False,
    )


def test_importing_the_app_loads_no_configuration_or_storage_module() -> None:
    result = _run(
        "import sys\n"
        "import rquant.web.app, rquant.web.cli\n"
        "bad = sorted(m for m in sys.modules\n"
        "             if m == 'rquant.config' or m.startswith('rquant.storage'))\n"
        "print(bad)\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"


def test_rquant_web_openapi_runs_through_the_main_cli_without_configuration() -> None:
    result = _run(
        "import sys\n"
        "sys.argv = ['rquant', 'web-openapi']\n"
        "from rquant.cli import main\n"
        "code = main()\n"
        "assert 'rquant.config' not in sys.modules, 'web-openapi built Settings'\n"
        "raise SystemExit(code)\n"
    )
    assert result.returncode == 0, result.stderr
    document = json.loads(result.stdout)
    assert document["info"]["title"] == "rQuant Web API"
    assert "/api/v1/meta" in document["paths"]


def test_web_serve_self_check_reports_a_missing_root_without_configuration(
    tmp_path: Path,
) -> None:
    environment = {**CONFIGURATION_FREE_ENVIRONMENT, "RQUANT_SERVING_ROOT": str(tmp_path / "x")}
    result = subprocess.run(
        (
            sys.executable,
            "-c",
            "import sys; sys.argv = ['rquant', 'web-serve', '--self-check'];"
            " from rquant.cli import main; raise SystemExit(main())",
        ),
        cwd=str(REPO_ROOT),
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1, result.stderr
    report = json.loads(result.stdout)
    assert report["ok"] is False
    assert "serving 指针不可读" in report["detail"]
