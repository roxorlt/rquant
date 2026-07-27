#!/usr/bin/env python3
"""Detect unsafe runtime artifacts before Lab daemon rollout."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

BYTECODE_SUFFIXES = frozenset({".pyc", ".pyo"})


class PreflightError(RuntimeError):
    pass


def _checkout_root(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PreflightError("checkout root must be an absolute canonical path")
    try:
        top_level = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=path,
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise PreflightError("checkout root is not a readable Git worktree") from exc
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
                if child.is_symlink() or child.suffix.lower() in BYTECODE_SUFFIXES:
                    found.append(child)
    except OSError as exc:
        raise PreflightError("runtime artifact scan failed closed") from exc
    return tuple(sorted(set(found)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout-root", required=True)
    args = parser.parse_args(argv)
    try:
        checkout = _checkout_root(args.checkout_root)
        artifacts = _runtime_artifacts(checkout)
        if not artifacts:
            print("Lab runtime preflight: no Python bytecode or package symlinks")
            return 0
        preview = ", ".join(str(path.relative_to(checkout)) for path in artifacts[:20])
        if len(artifacts) > 20:
            preview = f"{preview}, ..."
        raise PreflightError(
            "ignored Python bytecode or package symlinks block formal runtime: "
            f"{len(artifacts)} artifact(s); manually verify and remove only these "
            f"repository entries, then rerun: {preview}"
        )
    except PreflightError as exc:
        print(f"Lab runtime preflight failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
