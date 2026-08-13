from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

_ALLOWED_TOP_LEVEL_IMPORTS = frozenset({"collections", "os"})
_DYNAMIC_IMPORTERS = frozenset(
    {
        "__import__",
        "builtins.__import__",
        "import_module",
        "importlib.import_module",
    }
)


def _top_level_imports(module: ast.Module) -> set[str]:
    imported_modules = {
        alias.name.split(".")[0]
        for node in ast.walk(module)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_modules.update(
        node.module.split(".")[0]
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    return imported_modules


def _import_aliases(module: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                aliases[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            for alias in node.names:
                aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return aliases


def _literal_dynamic_imports(module: ast.Module) -> set[str]:
    aliases = _import_aliases(module)
    imported: set[str] = set()
    for node in ast.walk(module):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        argument = node.args[0]
        if not isinstance(argument, ast.Constant) or not isinstance(argument.value, str):
            continue
        importer: str | None = None
        if isinstance(node.func, ast.Name):
            importer = aliases.get(node.func.id, node.func.id)
        elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            base = aliases.get(node.func.value.id, node.func.value.id)
            importer = f"{base}.{node.func.attr}"
        if importer in _DYNAMIC_IMPORTERS:
            imported.add(argument.value)
    return imported


def _assert_fd_exec_import_graph(source: str) -> None:
    module = ast.parse(source)
    violations: list[str] = []
    unexpected_imports = _top_level_imports(module) - _ALLOWED_TOP_LEVEL_IMPORTS - {"__future__"}
    if unexpected_imports:
        violations.append(f"disallowed top-level imports: {sorted(unexpected_imports)}")
    dynamic_imports = _literal_dynamic_imports(module)
    if dynamic_imports:
        violations.append(f"dynamic literal imports: {sorted(dynamic_imports)}")
    assert not violations, "; ".join(violations)


def test_fd_exec_import_graph_is_stdlib_only() -> None:
    module_path = Path(__file__).parents[2] / "src" / "rquant" / "fd_exec.py"
    _assert_fd_exec_import_graph(module_path.read_text(encoding="utf-8"))

    source_root = module_path.parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-S",
            "-c",
            (
                "import sys\n"
                "assert 'site' not in sys.modules\n"
                "try:\n"
                "    import pydantic\n"
                "except ModuleNotFoundError:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('pydantic must not be importable')\n"
                f"sys.path.insert(0, {str(source_root)!r})\n"
                "import rquant.fd_exec\n"
                "forbidden = (\n"
                "    'pydantic', 'pydantic_core', 'pydantic_settings',\n"
                "    'rquant.config', 'rquant.runtime_code_attestation',\n"
                "    'rquant.runtime_code_generation',\n"
                ")\n"
                "assert not any(\n"
                "    name == blocked or name.startswith(f'{blocked}.')\n"
                "    for blocked in forbidden for name in sys.modules\n"
                ")\n"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "source",
    [
        "__import__('pydantic')",
        "import importlib\nimportlib.import_module('pydantic')",
        "import importlib as loader\nloader.import_module('pydantic')",
        "from importlib import import_module as load\nload('pydantic')",
        "import builtins as loader\nloader.__import__('pydantic')",
    ],
)
def test_fd_exec_import_graph_rejects_literal_dynamic_imports(source: str) -> None:
    with pytest.raises(AssertionError, match="dynamic literal import"):
        _assert_fd_exec_import_graph(source)


def test_runtime_fd_exec_ci_job_pins_official_actions() -> None:
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    job = workflow.split("  runtime-fd-exec-linux:\n", maxsplit=1)[1]

    assert "name: Linux FD-exec exact (${{ matrix.python-version }})" in job
    assert (
        "uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # actions/checkout@v4"
    ) in job
    assert (
        "uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065"
        " # actions/setup-python@v5"
    ) in job
    assert (
        "uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
        " # actions/upload-artifact@v4"
    ) in job
    assert "uv sync --frozen" in job
    assert "--junitxml=" in job
    assert "if-no-files-found: error" in job
