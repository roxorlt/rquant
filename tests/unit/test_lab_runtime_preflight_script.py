from __future__ import annotations

import os
import runpy
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "preflight-lab-runtime.py"
TRUSTED_GIT = Path("/usr/bin/git")


def _checkout(tmp_path: Path) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    package = checkout / "src" / "rquant"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    (checkout / ".gitignore").write_text(
        "__pycache__/\n*.pyc\n*.pyo\n*.so\n*.dylib\n*.pyd\n",
        encoding="utf-8",
    )
    (package / "__init__.py").write_text("", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "test fixture",
        ],
        cwd=checkout,
        check=True,
    )
    return checkout, package


def _run(checkout: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    expected_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--checkout-root",
            str(checkout),
            "--expected-commit",
            expected_commit,
            "--trusted-git-path",
            str(TRUSTED_GIT),
            *arguments,
        ],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )


def test_lab_runtime_preflight_rejects_dirty_tracked_package_source(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    tracked = package / "__init__.py"
    tracked.write_text("UNTRUSTED = True\n", encoding="utf-8")

    result = _run(checkout)

    assert result.returncode == 1
    assert "tracked" in result.stderr.lower()
    assert tracked.read_text(encoding="utf-8") == "UNTRUSTED = True\n"


def test_lab_runtime_preflight_uses_explicit_trusted_git_not_path(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    fake_bin = checkout / ".venv" / "bin"
    fake_bin.mkdir(parents=True)
    marker = tmp_path / "fake-git-ran"
    fake_git = fake_bin / "git"
    fake_git.write_text(
        f"#!/bin/sh\ntouch {marker!s}\nexit 0\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o700)
    (package / "__init__.py").write_text("UNTRUSTED = True\n", encoding="utf-8")
    expected_commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}:{environment.get('PATH', '')}"

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--checkout-root",
            str(checkout),
            "--expected-commit",
            expected_commit,
            "--trusted-git-path",
            str(TRUSTED_GIT),
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "tracked" in result.stderr.lower()
    assert not marker.exists()


def test_lab_runtime_preflight_rejects_symlinked_trusted_git(tmp_path: Path) -> None:
    checkout, _package = _checkout(tmp_path)
    linked_git = tmp_path / "linked-git"
    linked_git.symlink_to(TRUSTED_GIT)
    expected_commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--checkout-root",
            str(checkout),
            "--expected-commit",
            expected_commit,
            "--trusted-git-path",
            str(linked_git),
        ],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "physical" in result.stderr


def test_lab_runtime_preflight_rejects_expected_commit_mismatch(tmp_path: Path) -> None:
    checkout, _package = _checkout(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--checkout-root",
            str(checkout),
            "--expected-commit",
            "0" * 40,
            "--trusted-git-path",
            str(TRUSTED_GIT),
        ],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "commit" in result.stderr.lower()


@pytest.mark.parametrize("suffix", [".pyc", ".pyo", ".so", ".dylib", ".pyd"])
def test_lab_runtime_preflight_rejects_executable_artifacts_without_mutation(
    tmp_path: Path,
    suffix: str,
) -> None:
    checkout, package = _checkout(tmp_path)
    artifact = package / f"payload{suffix}"
    artifact.write_bytes(b"untrusted executable artifact")

    result = _run(checkout)

    assert result.returncode == 1
    assert "executable artifact" in result.stderr
    assert artifact.read_bytes() == b"untrusted executable artifact"


def test_lab_runtime_preflight_rejects_package_symlink_without_mutation(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    external = tmp_path / "external-package"
    external.mkdir()
    external_init = external / "__init__.py"
    external_init.write_text("VALUE = 'external'\n", encoding="utf-8")
    package_link = package / "external_package"
    package_link.symlink_to(external)

    result = _run(checkout)

    assert result.returncode == 1
    assert "package symlink" in result.stderr
    assert package_link.is_symlink()
    assert external_init.read_text(encoding="utf-8") == "VALUE = 'external'\n"


def test_lab_runtime_preflight_has_no_automatic_cleanup_mode(tmp_path: Path) -> None:
    checkout, package = _checkout(tmp_path)
    cache = package / "__pycache__" / "payload.cpython-312.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"stale bytecode")

    result = _run(checkout, "--clean-bytecode")

    assert result.returncode != 0
    assert cache.read_bytes() == b"stale bytecode"


def test_lab_runtime_preflight_symlink_swap_never_deletes_external_bytecode(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    external = tmp_path / "external-cache"
    external.mkdir()
    victim = external / "payload.cpython-312.pyc"
    victim.write_bytes(b"external bytecode must survive")
    os.symlink(external, package / "__pycache__")

    result = _run(checkout)

    assert result.returncode == 1
    assert "manual" in result.stderr.lower()
    assert (package / "__pycache__").is_symlink()
    assert victim.read_bytes() == b"external bytecode must survive"


def test_lab_runtime_preflight_detect_only_scan_survives_mid_walk_symlink_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, package = _checkout(tmp_path)
    cache = package / "__pycache__"
    cache.mkdir()
    repository_bytecode = cache / "payload.cpython-312.pyc"
    repository_bytecode.write_bytes(b"repository bytecode")
    external = tmp_path / "external-cache"
    external.mkdir()
    victim = external / repository_bytecode.name
    victim.write_bytes(b"external bytecode must survive")
    displaced = tmp_path / "displaced-cache"
    namespace = runpy.run_path(str(SCRIPT))

    def swapping_walk(
        *_args: object,
        **_kwargs: object,
    ) -> Iterator[tuple[str, list[str], list[str]]]:
        yield str(package), [cache.name], []
        cache.rename(displaced)
        cache.symlink_to(external, target_is_directory=True)
        yield str(cache), [], [victim.name]

    monkeypatch.setattr(os, "walk", swapping_walk)

    artifacts = namespace["_runtime_artifacts"](checkout)

    assert cache / victim.name in artifacts
    assert cache.is_symlink()
    assert victim.read_bytes() == b"external bytecode must survive"
    assert (displaced / repository_bytecode.name).read_bytes() == b"repository bytecode"
