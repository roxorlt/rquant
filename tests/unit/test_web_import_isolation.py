"""Cold web commands load definitions, without settings or storage operations.

Two web readers may import only DuckDB's Error type. The existing writer-config field
loads the storage package's definitions; loading them must not connect, migrate, inspect
writer paths or acquire a lease. All other storage imports remain forbidden.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_PACKAGE = REPO_ROOT / "src" / "rquant" / "web"
FORBIDDEN_PREFIXES = ("rquant.config", "rquant.storage", "duckdb")
ERROR_IMPORT_MODULES = frozenset(
    {WEB_PACKAGE / "condition_alert_read.py", WEB_PACKAGE / "screen_service.py"}
)
ALLOWED_STORAGE_MODULES = frozenset(
    {
        "rquant.storage",
        "rquant.storage.duckdb",
        "rquant.storage.migrations",
        "rquant.storage.primary_writer_gate",
        "rquant.storage.schema",
    }
)
_BLOCKED_PYTHON_CALLS = frozenset(
    {
        ("rquant.config", "<module>"),
        ("rquant.storage.duckdb", "DuckDBStore.__init__"),
        ("rquant.storage.duckdb", "open_readonly_store"),
        ("rquant.storage.duckdb", "open_readonly_connection"),
        ("rquant.storage.migrations", "initialize_schema"),
        ("rquant.storage.migrations", "_apply_migration"),
        ("rquant.storage.primary_writer_gate", "PrimaryWriterGate.acquire"),
        ("rquant.storage.primary_writer_gate", "PrimaryWriterGateConfig.capture"),
        ("rquant.storage.primary_writer_gate", "configured_primary_gate"),
    }
)
_REPORT_PREFIX = "IMPORT_ISOLATION_REPORT "
CONFIGURATION_FREE_ENVIRONMENT = {
    "PATH": os.environ.get("PATH", os.defpath),
    "LANG": "C",
    "PYTHONPATH": str(REPO_ROOT / "src"),
    "RQUANT_DISABLE_DOTENV": "1",
    "PYTHONDONTWRITEBYTECODE": "1",
}


def _modules() -> list[Path]:
    return sorted(WEB_PACKAGE.rglob("*.py"))


def _imported_names(tree: ast.AST, module: Path) -> list[str]:
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            if (
                module in ERROR_IMPORT_MODULES
                and node.module == "duckdb"
                and len(node.names) == 1
                and node.names[0].name == "Error"
                and node.names[0].asname == "DuckDBError"
            ):
                continue
            names.append(node.module)
            names.extend(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def test_the_package_has_modules_to_check() -> None:
    assert {path.name for path in _modules()} >= {"app.py", "serving.py", "cli.py", "meta.py"}


def _assert_static_import_boundary(module: Path, tree: ast.AST) -> None:
    offending = [
        name
        for name in _imported_names(tree, module)
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


@pytest.mark.parametrize(
    "module", _modules(), ids=lambda path: path.relative_to(WEB_PACKAGE).as_posix()
)
def test_no_web_module_imports_configuration_storage_or_duckdb(module: Path) -> None:
    tree = ast.parse(module.read_text(encoding="utf-8"), filename=str(module))
    _assert_static_import_boundary(module, tree)


def _run(
    program: str, *, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    guard = f"""
import json, sys, threading
_allowed_storage = {tuple(sorted(ALLOWED_STORAGE_MODULES))!r}
_blocked_python = {tuple(sorted(_BLOCKED_PYTHON_CALLS))!r}
_violations = []
class StorageAccessRejected(BaseException):
    pass
def _guard(frame, event, arg):
    if event == 'call':
        target = (frame.f_globals.get('__name__'), frame.f_code.co_qualname)
        if target not in _blocked_python:
            return
        name = ':'.join(target)
    elif event == 'c_call':
        if (getattr(arg, '__module__', None) not in ('duckdb', '_duckdb')
                or getattr(arg, '__name__', None) != 'connect'):
            return
        name = 'duckdb.connect'
    else:
        return
    _violations.append(name)
    raise StorageAccessRejected(name)
sys.setprofile(_guard)
threading.setprofile(_guard)
try:
"""
    finish = f"""
finally:
    _active = sys.getprofile() is _guard
    sys.setprofile(None)
    threading.setprofile(None)
    _storage = sorted(m for m in sys.modules
                      if m == 'rquant.storage' or m.startswith('rquant.storage.'))
    _report = {{
        'storage_modules': _storage,
        'unknown_storage': sorted(set(_storage) - set(_allowed_storage)),
        'configuration_loaded': 'rquant.config' in sys.modules,
        'forbidden_calls': _violations,
        'hook_active_until_exit': _active,
        'hook_removed': sys.getprofile() is None,
        'future_hook_removed': threading.getprofile() is None,
        'extra_threads': len(threading.enumerate()) - 1,
    }}
    sys.stderr.write({_REPORT_PREFIX!r} + json.dumps(_report, sort_keys=True) + '\\n')
"""
    guarded_program = guard + textwrap.indent(program, "    ") + "\n" + finish
    return subprocess.run(
        (sys.executable, "-c", guarded_program),
        cwd=str(REPO_ROOT),
        env=dict(CONFIGURATION_FREE_ENVIRONMENT if environment is None else environment),
        capture_output=True,
        text=True,
        check=False,
    )


def _storage_report(result: subprocess.CompletedProcess[str]) -> dict[str, object]:
    reports = [
        line.removeprefix(_REPORT_PREFIX)
        for line in result.stderr.splitlines()
        if line.startswith(_REPORT_PREFIX)
    ]
    assert len(reports) == 1, result.stderr
    return json.loads(reports[0])


def _assert_no_storage_access(result: subprocess.CompletedProcess[str]) -> None:
    report = _storage_report(result)
    assert report["unknown_storage"] == []
    assert report["configuration_loaded"] is False
    assert report["forbidden_calls"] == []
    assert report["hook_active_until_exit"] is True
    assert report["hook_removed"] is True
    assert report["future_hook_removed"] is True
    assert report["extra_threads"] == 0
    assert set(report["storage_modules"]) == ALLOWED_STORAGE_MODULES
    print(_REPORT_PREFIX + json.dumps(report, sort_keys=True))


def test_importing_the_app_loads_no_configuration_or_storage_module() -> None:
    result = _run(
        "import sys\n"
        "import rquant.web.app, rquant.web.cli\n"
        "bad = sorted(m for m in sys.modules\n"
        "             if m == 'rquant.config' or (m.startswith('rquant.storage')\n"
        f"             and m not in {tuple(sorted(ALLOWED_STORAGE_MODULES))!r}))\n"
        "print(bad)\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"
    _assert_no_storage_access(result)


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
    _assert_no_storage_access(result)


def test_web_serve_self_check_reports_a_missing_root_without_configuration(
    tmp_path: Path,
) -> None:
    environment = {**CONFIGURATION_FREE_ENVIRONMENT, "RQUANT_SERVING_ROOT": str(tmp_path / "x")}
    result = _run(
        "import sys; sys.argv = ['rquant', 'web-serve', '--self-check'];"
        " from rquant.cli import main; raise SystemExit(main())",
        environment=environment,
    )
    assert result.returncode == 1, result.stderr
    report = json.loads(result.stdout)
    assert report["ok"] is False
    assert "serving 指针不可读" in report["detail"]
    _assert_no_storage_access(result)


def test_static_import_guard_rejects_access() -> None:
    for filename, source in (
        ("screen_service.py", "import duckdb"),
        ("screen_service.py", "from duckdb import connect"),
        ("screen_service.py", "from duckdb import Error as DuckDBError, connect"),
        ("screen_service.py", "from duckdb import Error as OtherError"),
        ("another_reader.py", "from duckdb import Error as DuckDBError"),
        ("nested/screen_service.py", "from duckdb import Error as DuckDBError"),
        ("screen_service.py", "from rquant.storage.primary_writer_gate import PrimaryWriterGateConfig"),
        ("screen_service.py", "from rquant.config import Settings"),
        ("screen_service.py", "source.connect()"),
        ("screen_service.py", "from duckdb import Error as DuckDBError\nDuckDBError.connect()"),
    ):
        with pytest.raises(AssertionError):
            _assert_static_import_boundary(WEB_PACKAGE / filename, ast.parse(source))


def test_runtime_import_guard_blocks_storage() -> None:
    for program, blocked in (
        ("import duckdb; duckdb.connect(':memory:')", "duckdb.connect"),
        (
            "from rquant.storage.duckdb import DuckDBStore; DuckDBStore()",
            "rquant.storage.duckdb:DuckDBStore.__init__",
        ),
        (
            "from rquant.storage.duckdb import open_readonly_store\nwith open_readonly_store(): pass",
            "rquant.storage.duckdb:open_readonly_store",
        ),
        (
            "from rquant.storage.duckdb import open_readonly_connection; open_readonly_connection()",
            "rquant.storage.duckdb:open_readonly_connection",
        ),
        (
            "from rquant.storage.primary_writer_gate import PrimaryWriterGate\n"
            "PrimaryWriterGate.__new__(PrimaryWriterGate).acquire()",
            "rquant.storage.primary_writer_gate:PrimaryWriterGate.acquire",
        ),
        (
            "from pathlib import Path\n"
            "from rquant.storage.primary_writer_gate import PrimaryWriterGateConfig\n"
            "PrimaryWriterGateConfig.capture(primary_path=Path('/nonexistent/primary'), lock_path=Path('/nonexistent/lock'))",
            "rquant.storage.primary_writer_gate:PrimaryWriterGateConfig.capture",
        ),
        (
            "from pathlib import Path\n"
            "from rquant.storage.primary_writer_gate import configured_primary_gate\n"
            "configured_primary_gate(Path('/nonexistent/primary'), Path('/nonexistent/profile'))",
            "rquant.storage.primary_writer_gate:configured_primary_gate",
        ),
        (
            "from rquant.storage.migrations import initialize_schema; initialize_schema(None)",
            "rquant.storage.migrations:initialize_schema",
        ),
        (
            "from rquant.storage.migrations import _apply_migration; _apply_migration(None, None)",
            "rquant.storage.migrations:_apply_migration",
        ),
    ):
        result = _run(program)
        assert result.returncode != 0
        report = _storage_report(result)
        assert report["forbidden_calls"] == [blocked]
        assert report["configuration_loaded"] is False
        assert report["unknown_storage"] == []
        assert report["hook_removed"] is True
        assert report["future_hook_removed"] is True
        assert report["extra_threads"] == 0
