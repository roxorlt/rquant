#!/usr/bin/env python3
"""Fail-closed, versioned SourceBroker V2 affected-boundary test entry."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

SUITE_ID = "source_broker_v2_frozen"
SCHEMA_VERSION = 1
DEFAULT_MANIFEST = Path("tests/manifests/source_broker_v2_frozen.json")
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "suite_id",
        "expected_test_count",
        "expected_nodeids_sha256",
        "modules",
        "pressure_gate",
    }
)
_MODULE_FIELDS = frozenset({"path", "boundary"})
_PRESSURE_FIELDS = frozenset({"rounds", "nodeids"})
_FORBIDDEN_PATH_CHARACTERS = frozenset("*?[]{}")


class FrozenSuiteError(RuntimeError):
    """The frozen suite definition or collected boundary is not trustworthy."""


@dataclass(frozen=True)
class FrozenModule:
    path: str
    boundary: str


@dataclass(frozen=True)
class PressureGate:
    rounds: int
    nodeids: tuple[str, ...]


@dataclass(frozen=True)
class FrozenManifest:
    expected_test_count: int
    expected_nodeids_sha256: str
    modules: tuple[FrozenModule, ...]
    pressure_gate: PressureGate

    @property
    def module_paths(self) -> tuple[str, ...]:
        return tuple(module.path for module in self.modules)


@dataclass(frozen=True)
class FrozenRunResult:
    collected_count: int
    collected_nodeids_sha256: str
    pressure_rounds: int


SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]


def _strict_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FrozenSuiteError(f"manifest contains duplicate JSON key: {key}")
        result[key] = value
    return result


def _require_exact_fields(value: dict[str, Any], expected: frozenset[str], *, label: str) -> None:
    observed = frozenset(value)
    if observed != expected:
        missing = sorted(expected - observed)
        unknown = sorted(observed - expected)
        raise FrozenSuiteError(f"{label} fields are invalid: missing={missing}, unknown={unknown}")


def _require_relative_module_path(value: object) -> str:
    if type(value) is not str or not value:
        raise FrozenSuiteError("frozen module path must be nonempty text")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or ".." in path.parts
        or path.suffix != ".py"
        or not path.parts
        or path.parts[0] != "tests"
        or any(character in value for character in _FORBIDDEN_PATH_CHARACTERS)
        or str(path) != value
    ):
        raise FrozenSuiteError(f"frozen module path is unsafe: {value}")
    return value


def _nodeid_digest(nodeids: Sequence[str]) -> str:
    payload = ("\n".join(sorted(nodeids)) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_manifest(path: Path) -> FrozenManifest:
    try:
        raw = path.read_text(encoding="utf-8")
        decoded = json.loads(raw, object_pairs_hook=_strict_object)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrozenSuiteError("frozen manifest is unavailable or invalid JSON") from exc
    if type(decoded) is not dict:
        raise FrozenSuiteError("frozen manifest must be one JSON object")
    _require_exact_fields(decoded, _TOP_LEVEL_FIELDS, label="manifest")
    if decoded["schema_version"] != SCHEMA_VERSION or decoded["suite_id"] != SUITE_ID:
        raise FrozenSuiteError("frozen manifest schema or suite identity is invalid")
    expected_count = decoded["expected_test_count"]
    expected_digest = decoded["expected_nodeids_sha256"]
    if type(expected_count) is not int or expected_count < 1:
        raise FrozenSuiteError("frozen expected test count is invalid")
    if (
        type(expected_digest) is not str
        or len(expected_digest) != 64
        or any(character not in "0123456789abcdef" for character in expected_digest)
    ):
        raise FrozenSuiteError("frozen expected nodeid digest is invalid")

    raw_modules = decoded["modules"]
    if type(raw_modules) is not list or not raw_modules:
        raise FrozenSuiteError("frozen module list must be nonempty")
    modules: list[FrozenModule] = []
    for raw_module in raw_modules:
        if type(raw_module) is not dict:
            raise FrozenSuiteError("frozen module entry must be an object")
        _require_exact_fields(raw_module, _MODULE_FIELDS, label="module")
        module_path = _require_relative_module_path(raw_module["path"])
        boundary = raw_module["boundary"]
        if type(boundary) is not str or not boundary.strip():
            raise FrozenSuiteError(f"frozen module boundary is missing: {module_path}")
        modules.append(FrozenModule(path=module_path, boundary=boundary.strip()))
    module_paths = tuple(module.path for module in modules)
    if len(module_paths) != len(set(module_paths)):
        raise FrozenSuiteError("frozen manifest contains duplicate module path")

    raw_pressure = decoded["pressure_gate"]
    if type(raw_pressure) is not dict:
        raise FrozenSuiteError("pressure gate must be an object")
    _require_exact_fields(raw_pressure, _PRESSURE_FIELDS, label="pressure gate")
    rounds = raw_pressure["rounds"]
    raw_nodeids = raw_pressure["nodeids"]
    if type(rounds) is not int or not 2 <= rounds <= 10:
        raise FrozenSuiteError("pressure gate rounds must be between 2 and 10")
    if type(raw_nodeids) is not list or not raw_nodeids:
        raise FrozenSuiteError("pressure gate nodeids must be nonempty")
    if any(type(nodeid) is not str or "::" not in nodeid for nodeid in raw_nodeids):
        raise FrozenSuiteError("pressure gate nodeid is invalid")
    pressure_nodeids = tuple(raw_nodeids)
    if len(pressure_nodeids) != len(set(pressure_nodeids)):
        raise FrozenSuiteError("pressure gate contains duplicate nodeid")
    if any(
        not any(nodeid.startswith(f"{module_path}::") for module_path in module_paths)
        for nodeid in pressure_nodeids
    ):
        raise FrozenSuiteError("pressure gate nodeid is outside the frozen modules")
    return FrozenManifest(
        expected_test_count=expected_count,
        expected_nodeids_sha256=expected_digest,
        modules=tuple(modules),
        pressure_gate=PressureGate(rounds=rounds, nodeids=pressure_nodeids),
    )


def _validate_paths(*, repo_root: Path, manifest: FrozenManifest) -> None:
    try:
        root_metadata = os.lstat(repo_root)
        resolved_root = repo_root.resolve(strict=True)
    except OSError as exc:
        raise FrozenSuiteError("repository root is unavailable") from exc
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise FrozenSuiteError("repository root must be a canonical directory")
    if resolved_root != repo_root:
        raise FrozenSuiteError("repository root must be canonical")

    for relative in manifest.module_paths:
        parts = PurePosixPath(relative).parts
        candidate = repo_root
        for index, part in enumerate(parts):
            candidate /= part
            try:
                metadata = os.lstat(candidate)
            except OSError as exc:
                raise FrozenSuiteError(f"unknown module path: {relative}") from exc
            if stat.S_ISLNK(metadata.st_mode):
                raise FrozenSuiteError(
                    "frozen module path contains symlink component: "
                    f"{relative} (component={candidate.relative_to(repo_root).as_posix()})"
                )
            is_final = index == len(parts) - 1
            if not is_final and not stat.S_ISDIR(metadata.st_mode):
                raise FrozenSuiteError(
                    f"frozen module path ancestor is not a directory: {relative}"
                )
            if is_final and not stat.S_ISREG(metadata.st_mode):
                raise FrozenSuiteError(f"frozen module path must be a regular file: {relative}")

        try:
            resolved_path = candidate.resolve(strict=True)
            resolved_relative = resolved_path.relative_to(resolved_root)
        except ValueError as exc:
            raise FrozenSuiteError(
                f"frozen module path resolves outside repository root: {relative}"
            ) from exc
        except OSError as exc:
            raise FrozenSuiteError(f"unknown module path: {relative}") from exc
        if resolved_relative.as_posix() != relative or resolved_path != candidate:
            raise FrozenSuiteError(f"frozen module path is not canonical: {relative}")


def _run(
    command: Sequence[str],
    *,
    repo_root: Path,
    manifest: FrozenManifest,
    capture_output: bool,
    subprocess_runner: SubprocessRunner,
) -> subprocess.CompletedProcess[str]:
    # This is an execution-boundary recheck, not a claim that pathname TOCTOU is eliminated.
    _validate_paths(repo_root=repo_root, manifest=manifest)
    try:
        completed = subprocess_runner(
            tuple(command),
            cwd=repo_root,
            check=False,
            capture_output=capture_output,
            text=True,
        )
    except OSError as exc:
        detail = exc.strerror or type(exc).__name__
        raise FrozenSuiteError(f"frozen pytest command could not start: {detail}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "pytest failed").strip()
        raise FrozenSuiteError(f"frozen pytest command failed: {detail}")
    return completed


def _collect_nodeids(
    *,
    repo_root: Path,
    python_path: Path,
    manifest: FrozenManifest,
    subprocess_runner: SubprocessRunner,
) -> tuple[str, ...]:
    completed = _run(
        (
            str(python_path),
            "-m",
            "pytest",
            "-o",
            "addopts=",
            "--collect-only",
            "-q",
            *manifest.module_paths,
        ),
        repo_root=repo_root,
        manifest=manifest,
        capture_output=True,
        subprocess_runner=subprocess_runner,
    )
    prefixes = tuple(f"{module_path}::" for module_path in manifest.module_paths)
    nodeids = tuple(line.strip() for line in (completed.stdout or "").splitlines() if "::" in line)
    if any(not nodeid.startswith(prefixes) for nodeid in nodeids):
        raise FrozenSuiteError("collect returned nodeid outside the frozen modules")
    return nodeids


def _validate_collected_nodeids(
    *,
    manifest: FrozenManifest,
    nodeids: Sequence[str],
) -> str:
    if len(nodeids) != len(set(nodeids)):
        raise FrozenSuiteError("collect returned duplicate nodeid")
    if len(nodeids) != manifest.expected_test_count:
        raise FrozenSuiteError(
            "frozen collect count changed: "
            f"expected={manifest.expected_test_count}, observed={len(nodeids)}"
        )
    digest = _nodeid_digest(nodeids)
    if digest != manifest.expected_nodeids_sha256:
        raise FrozenSuiteError(
            "frozen collect nodeid digest changed: "
            f"expected={manifest.expected_nodeids_sha256}, observed={digest}"
        )
    missing_pressure = sorted(set(manifest.pressure_gate.nodeids) - set(nodeids))
    if missing_pressure:
        raise FrozenSuiteError(f"pressure gate nodeids are no longer collected: {missing_pressure}")
    return digest


def run_frozen_suite(
    *,
    repo_root: Path,
    manifest_path: Path,
    python_path: Path,
    collect_only: bool = False,
    subprocess_runner: SubprocessRunner = subprocess.run,
) -> FrozenRunResult:
    try:
        root = repo_root.resolve(strict=True)
    except OSError as exc:
        raise FrozenSuiteError("repository root is unavailable") from exc
    manifest = load_manifest(manifest_path)
    nodeids = _collect_nodeids(
        repo_root=root,
        python_path=python_path,
        manifest=manifest,
        subprocess_runner=subprocess_runner,
    )
    digest = _validate_collected_nodeids(manifest=manifest, nodeids=nodeids)
    if collect_only:
        return FrozenRunResult(
            collected_count=len(nodeids),
            collected_nodeids_sha256=digest,
            pressure_rounds=0,
        )

    _run(
        (
            str(python_path),
            "-m",
            "pytest",
            "-o",
            "addopts=",
            "-q",
            "-m",
            "not network",
            *manifest.module_paths,
            *(f"--deselect={nodeid}" for nodeid in manifest.pressure_gate.nodeids),
        ),
        repo_root=root,
        manifest=manifest,
        capture_output=False,
        subprocess_runner=subprocess_runner,
    )
    for _round in range(manifest.pressure_gate.rounds):
        _run(
            (
                str(python_path),
                "-m",
                "pytest",
                "-o",
                "addopts=",
                "-q",
                "-m",
                "not network",
                *manifest.pressure_gate.nodeids,
            ),
            repo_root=root,
            manifest=manifest,
            capture_output=False,
            subprocess_runner=subprocess_runner,
        )
    return FrozenRunResult(
        collected_count=len(nodeids),
        collected_nodeids_sha256=digest,
        pressure_rounds=manifest.pressure_gate.rounds,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = args.manifest or repo_root / DEFAULT_MANIFEST
    try:
        result = run_frozen_suite(
            repo_root=repo_root,
            manifest_path=manifest_path,
            python_path=args.python,
            collect_only=args.collect_only,
        )
    except FrozenSuiteError as exc:
        print(f"{SUITE_ID}: FAIL: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "collected_count": result.collected_count,
                "collected_nodeids_sha256": result.collected_nodeids_sha256,
                "pressure_rounds": result.pressure_rounds,
                "suite_id": SUITE_ID,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
