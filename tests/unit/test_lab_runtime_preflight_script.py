from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "preflight-lab-runtime.py"


def _checkout(tmp_path: Path) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    package = checkout / "src" / "rquant"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    (checkout / ".gitignore").write_text("__pycache__/\n*.pyc\n*.pyo\n", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    return checkout, package


def _run(checkout: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--checkout-root",
            str(checkout),
            *arguments,
        ],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )


def test_lab_runtime_preflight_rejects_bytecode_without_mutation(tmp_path: Path) -> None:
    checkout, package = _checkout(tmp_path)
    cache = package / "__pycache__" / "payload.cpython-312.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"malicious bytecode")

    result = _run(checkout)

    assert result.returncode == 1
    assert "ignored Python bytecode" in result.stderr
    assert cache.read_bytes() == b"malicious bytecode"


def test_lab_runtime_preflight_explicitly_cleans_private_regular_bytecode(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    caches = (
        package / "__pycache__" / "payload.cpython-312.pyc",
        package / "legacy.pyo",
    )
    for cache in caches:
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(b"stale bytecode")

    result = _run(checkout, "--clean-bytecode")

    assert result.returncode == 0, result.stderr
    assert not any(cache.exists() for cache in caches)
    assert (package / "__init__.py").is_file()


def test_lab_runtime_preflight_refuses_to_clean_hardlinked_bytecode(tmp_path: Path) -> None:
    checkout, package = _checkout(tmp_path)
    external = tmp_path / "external.pyc"
    external.write_bytes(b"shared bytecode")
    cache = package / "__pycache__" / "payload.cpython-312.pyc"
    cache.parent.mkdir()
    os.link(external, cache)

    result = _run(checkout, "--clean-bytecode")

    assert result.returncode == 1
    assert "unsafe bytecode" in result.stderr
    assert cache.exists()
    assert external.read_bytes() == b"shared bytecode"
