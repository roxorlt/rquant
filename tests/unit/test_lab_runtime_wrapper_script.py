from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
import sys
import venv
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "scripts" / "run-lab-daemon.py"
PREFLIGHT = ROOT / "scripts" / "preflight-lab-runtime.py"


def _runtime_checkout(tmp_path: Path) -> tuple[Path, Path, Path]:
    checkout = tmp_path / "checkout"
    scripts = checkout / "scripts"
    package = checkout / "src" / "rquant"
    scripts.mkdir(parents=True)
    package.mkdir(parents=True)
    shutil.copy2(WRAPPER, scripts / WRAPPER.name)
    shutil.copy2(PREFLIGHT, scripts / PREFLIGHT.name)
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    (checkout / ".gitignore").write_text(
        "/.venv\n__pycache__/\n*.pyc\n*.pyo\n*.so\n*.dylib\n*.pyd\n",
        encoding="utf-8",
    )
    marker = checkout / "daemon.json"
    (package / "__init__.py").write_text(
        "from __future__ import annotations\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "def main():\n"
        "    Path(os.environ['LAB_WRAPPER_MARKER']).write_text("
        "json.dumps(sys.argv[1:]), encoding='utf-8')\n"
        "    print('fake daemon executed', flush=True)\n",
        encoding="utf-8",
    )
    venv.EnvBuilder(with_pip=False, symlinks=True).create(checkout / ".venv")
    python = checkout / ".venv" / "bin" / "python"
    executable = checkout / ".venv" / "bin" / "rquant"
    executable.write_text(
        f"#!{python}\n"
        "import sys\n"
        f"sys.path.insert(0, {str(checkout / 'src')!r})\n"
        "from rquant import main\n"
        "main()\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    return checkout, executable, marker


def _run_wrapper(
    checkout: Path,
    executable: Path,
    marker: Path,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        environment.pop(variable, None)
    environment["LAB_WRAPPER_MARKER"] = str(marker)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            str(checkout / ".venv" / "bin" / "python"),
            "-I",
            "-S",
            str(checkout / "scripts" / WRAPPER.name),
            "--expected-checkout-root",
            str(checkout),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )


def test_lab_runtime_wrapper_runs_preflight_before_daemon_exec(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.index("Lab runtime preflight") < result.stdout.index(
        "fake daemon executed"
    )
    assert json.loads(marker.read_text(encoding="utf-8"))[:2] == [
        "lab-worker",
        "--expected-checkout-root",
    ]


@pytest.mark.parametrize("suffix", [".pyc", ".pyo", ".so", ".dylib", ".pyd"])
def test_lab_runtime_wrapper_never_imports_rquant_when_preflight_fails(
    tmp_path: Path,
    suffix: str,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    artifact = checkout / "src" / "rquant" / f"untrusted{suffix}"
    artifact.write_bytes(b"untrusted executable")

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert "preflight failed" in result.stderr.lower()
    assert not marker.exists()
    assert artifact.read_bytes() == b"untrusted executable"


def test_lab_runtime_wrapper_rejects_package_symlink_before_import(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    external = tmp_path / "external-package"
    external.mkdir()
    (external / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (checkout / "src" / "rquant" / "external").symlink_to(external)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert not marker.exists()
    assert (external / "__init__.py").read_text(encoding="utf-8") == "VALUE = 1\n"


def test_lab_runtime_wrapper_rejects_mismatched_daemon_root(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    environment = os.environ.copy()
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        environment.pop(variable, None)
    environment["LAB_WRAPPER_MARKER"] = str(marker)

    result = subprocess.run(
        [
            str(checkout / ".venv" / "bin" / "python"),
            "-I",
            "-S",
            str(checkout / "scripts" / WRAPPER.name),
            "--expected-checkout-root",
            str(checkout),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(other),
            "--once",
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode != 0
    assert "checkout root" in result.stderr.lower()
    assert not marker.exists()


def test_lab_runtime_wrapper_source_is_stdlib_only() -> None:
    source = WRAPPER.read_text(encoding="utf-8")
    imported_roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", maxsplit=1)[0])

    assert imported_roots <= sys.stdlib_module_names | {"__future__"}
