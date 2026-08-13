from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path


def test_fd_exec_import_graph_is_stdlib_only() -> None:
    module_path = Path(__file__).parents[2] / "src" / "rquant" / "fd_exec.py"
    module = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
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

    forbidden = {
        "pydantic",
        "pydantic_core",
        "pydantic_settings",
        "config",
        "runtime_code_attestation",
        "runtime_code_generation",
    }
    assert not imported_modules & forbidden
    assert not any(name.startswith("rquant") for name in imported_modules)

    source_root = module_path.parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            (f"import sys; sys.path.insert(0, {str(source_root)!r}); import rquant.fd_exec"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
