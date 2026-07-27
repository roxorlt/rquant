#!/usr/bin/env python3
"""Reject or explicitly remove ignored bytecode before Lab daemon rollout."""

from __future__ import annotations

import argparse
import os
import stat
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
        package_identity = package_root.lstat()
    except OSError as exc:
        raise PreflightError("src/rquant is unavailable") from exc
    if not stat.S_ISDIR(package_identity.st_mode) or stat.S_ISLNK(package_identity.st_mode):
        raise PreflightError("src/rquant must be a physical directory")
    return path


def _ignored_bytecode(checkout: Path) -> tuple[Path, ...]:
    try:
        result = subprocess.run(
            [
                "git",
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "-z",
                "--",
                ":(top)src/rquant",
            ],
            cwd=checkout,
            check=True,
            capture_output=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PreflightError("ignored runtime artifact scan failed") from exc
    found: list[Path] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        if relative.suffix.lower() not in BYTECODE_SUFFIXES:
            continue
        candidate = checkout / relative
        if not candidate.is_relative_to(checkout / "src" / "rquant"):
            raise PreflightError("ignored bytecode escaped src/rquant")
        found.append(candidate)
    return tuple(sorted(found))


def _safe_bytecode_identities(paths: tuple[Path, ...]) -> dict[Path, tuple[int, int]]:
    identities: dict[Path, tuple[int, int]] = {}
    for path in paths:
        try:
            observed = path.lstat()
        except OSError as exc:
            raise PreflightError(f"unsafe bytecode changed before cleanup: {path}") from exc
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
        ):
            raise PreflightError(f"unsafe bytecode cannot be cleaned: {path}")
        identities[path] = (observed.st_dev, observed.st_ino)
    return identities


def _clean_bytecode(checkout: Path, paths: tuple[Path, ...]) -> None:
    identities = _safe_bytecode_identities(paths)
    for path, expected in identities.items():
        observed = path.lstat()
        if (observed.st_dev, observed.st_ino) != expected or observed.st_nlink != 1:
            raise PreflightError(f"unsafe bytecode changed during cleanup: {path}")
    for path in identities:
        path.unlink()
    package_root = checkout / "src" / "rquant"
    cache_dirs = sorted(
        {path.parent for path in identities if path.parent.name == "__pycache__"},
        key=lambda item: len(item.parts),
        reverse=True,
    )
    for directory in cache_dirs:
        if not directory.is_relative_to(package_root):
            raise PreflightError("bytecode cache directory escaped src/rquant")
        try:
            observed = directory.lstat()
            if (
                stat.S_ISDIR(observed.st_mode)
                and not stat.S_ISLNK(observed.st_mode)
                and observed.st_uid == os.getuid()
            ):
                directory.rmdir()
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout-root", required=True)
    parser.add_argument("--clean-bytecode", action="store_true")
    args = parser.parse_args(argv)
    try:
        checkout = _checkout_root(args.checkout_root)
        bytecode = _ignored_bytecode(checkout)
        if not bytecode:
            print("Lab runtime preflight: no ignored Python bytecode")
            return 0
        if not args.clean_bytecode:
            raise PreflightError(
                f"ignored Python bytecode blocks formal runtime: {len(bytecode)} file(s)"
            )
        _clean_bytecode(checkout, bytecode)
        remaining = _ignored_bytecode(checkout)
        if remaining:
            raise PreflightError(
                f"ignored Python bytecode remains after cleanup: {len(remaining)} file(s)"
            )
        print(f"Lab runtime preflight: removed {len(bytecode)} private bytecode file(s)")
        return 0
    except PreflightError as exc:
        print(f"Lab runtime preflight failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
