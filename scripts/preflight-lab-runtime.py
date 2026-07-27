#!/usr/bin/env python3
"""Detect unsafe runtime artifacts before Lab daemon rollout."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

EXECUTABLE_SUFFIXES = frozenset({".pyc", ".pyo", ".so", ".dylib", ".pyd"})
GIT_TIMEOUT_SECONDS = 5


class PreflightError(RuntimeError):
    pass


def _git_command(
    checkout: Path,
    arguments: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=checkout,
            check=False,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PreflightError("Git checkout verification failed closed") from exc
    if check and result.returncode != 0:
        raise PreflightError("Git checkout verification failed closed")
    return result


def _checkout_root(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PreflightError("checkout root must be an absolute canonical path")
    top_level = _git_command(path, ["rev-parse", "--show-toplevel"]).stdout.strip()
    if Path(top_level) != path:
        raise PreflightError("checkout root does not match Git top-level")
    package_root = path / "src" / "rquant"
    try:
        package_root.lstat()
    except OSError as exc:
        raise PreflightError("src/rquant is unavailable") from exc
    if not package_root.is_dir() or package_root.is_symlink():
        raise PreflightError("src/rquant must be a physical directory")
    return path


def _verify_tracked_checkout(checkout: Path, *, expected_commit: str) -> None:
    if len(expected_commit) != 40 or any(
        character not in "0123456789abcdef" for character in expected_commit
    ):
        raise PreflightError("expected commit must be a lowercase full Git SHA")
    observed_commit = _git_command(
        checkout,
        ["rev-parse", "--verify", "HEAD^{commit}"],
    ).stdout.strip()
    if observed_commit != expected_commit:
        raise PreflightError("checkout HEAD does not match expected commit")
    status = _git_command(
        checkout,
        ["status", "--porcelain=v1", "--untracked-files=no"],
    )
    diff_index = _git_command(
        checkout,
        ["diff-index", "--quiet", "HEAD", "--"],
        check=False,
    )
    if status.stdout or diff_index.returncode != 0:
        raise PreflightError("tracked checkout is dirty")


def _runtime_artifacts(checkout: Path) -> tuple[Path, ...]:
    package_root = checkout / "src" / "rquant"
    found: list[Path] = []
    try:
        for current_root, directory_names, file_names in os.walk(
            package_root,
            topdown=True,
            followlinks=False,
        ):
            current = Path(current_root)
            physical_directories: list[str] = []
            for name in directory_names:
                child = current / name
                if child.is_symlink():
                    found.append(child)
                else:
                    physical_directories.append(name)
            directory_names[:] = physical_directories
            for name in file_names:
                child = current / name
                if child.is_symlink() or child.suffix.lower() in EXECUTABLE_SUFFIXES:
                    found.append(child)
    except OSError as exc:
        raise PreflightError("runtime artifact scan failed closed") from exc
    return tuple(sorted(set(found)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout-root", required=True)
    parser.add_argument("--expected-commit", required=True)
    args = parser.parse_args(argv)
    try:
        checkout = _checkout_root(args.checkout_root)
        _verify_tracked_checkout(checkout, expected_commit=args.expected_commit)
        artifacts = _runtime_artifacts(checkout)
        if not artifacts:
            print("Lab runtime preflight: no executable artifacts or package symlinks")
            return 0
        preview = ", ".join(str(path.relative_to(checkout)) for path in artifacts[:20])
        if len(artifacts) > 20:
            preview = f"{preview}, ..."
        raise PreflightError(
            "ignored executable artifacts or package symlinks block formal runtime: "
            f"{len(artifacts)} artifact(s); manually verify and remove only these "
            f"repository entries, then rerun: {preview}"
        )
    except PreflightError as exc:
        print(f"Lab runtime preflight failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
