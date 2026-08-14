"""Generate and execute the versioned, fail-closed full-suite CI shards."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
SHARD_COUNT = 4
INDEX_NAME = "index.json"

# These are conservative historical weights for files that dominated the former
# monolithic CI job. Every other file still contributes its case count.
STATIC_FILE_WEIGHTS = {
    "tests/unit/test_runtime_production_profile.py": 4_000,
    "tests/unit/test_runtime_recovery_coordinator.py": 3_800,
    "tests/unit/test_runtime_recovery_artifacts.py": 2_600,
    "tests/unit/test_runtime_recovery_service.py": 2_400,
    "tests/unit/test_runtime_recovery_backup.py": 1_600,
    "tests/integration/test_production_artifact_terminal_lifecycle.py": 2_200,
}


class ContractError(ValueError):
    """Raised when checked-in CI selection evidence cannot be trusted."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def nodeid_digest(nodeids: Sequence[str]) -> str:
    unique = sorted(set(nodeids))
    if len(unique) != len(nodeids):
        raise ContractError("nodeid digest input contains duplicates")
    return hashlib.sha256("".join(f"{nodeid}\n" for nodeid in unique).encode("utf-8")).hexdigest()


def _file_for_nodeid(nodeid: str) -> str:
    path, separator, _ = nodeid.partition("::")
    if not separator or not path.endswith(".py"):
        raise ContractError(f"invalid pytest nodeid: {nodeid!r}")
    return path


def plan_shards(
    nodeids: Sequence[str], *, shard_count: int = SHARD_COUNT
) -> tuple[tuple[str, ...], ...]:
    """Use deterministic LPT on test-file history, not directory or case count alone."""
    if shard_count != SHARD_COUNT:
        raise ContractError(f"the full-suite contract requires exactly {SHARD_COUNT} shards")
    files: dict[str, list[str]] = defaultdict(list)
    for nodeid in sorted(nodeids):
        files[_file_for_nodeid(nodeid)].append(nodeid)
    weighted_files = sorted(
        files.items(),
        key=lambda item: (-max(len(item[1]), STATIC_FILE_WEIGHTS.get(item[0], 0)), item[0]),
    )
    loads = [0] * shard_count
    shards: list[list[str]] = [[] for _ in range(shard_count)]
    for path, file_nodeids in weighted_files:
        shard_id = min(range(shard_count), key=lambda candidate: (loads[candidate], candidate))
        shards[shard_id].extend(file_nodeids)
        loads[shard_id] += max(len(file_nodeids), STATIC_FILE_WEIGHTS.get(path, 0))
    return tuple(tuple(sorted(shard)) for shard in shards)


def _jsonl_path(root: Path, shard_id: int) -> Path:
    return root / f"shard-{shard_id}.jsonl"


def write_manifest_bundle(
    root: Path,
    *,
    selector: Sequence[str],
    shard_nodeids: Sequence[Sequence[str]],
    expected_skips: int,
) -> dict[str, Any]:
    if len(shard_nodeids) != SHARD_COUNT:
        raise ContractError(f"expected {SHARD_COUNT} shard nodeid lists")
    if expected_skips < 0:
        raise ContractError("expected skips must be nonnegative")
    root.mkdir(parents=True, exist_ok=True)
    normalized = tuple(tuple(sorted(group)) for group in shard_nodeids)
    full_nodeids = tuple(nodeid for group in normalized for nodeid in group)
    full_digest = nodeid_digest(full_nodeids)
    shards = []
    for shard_id, nodeids in enumerate(normalized):
        path = _jsonl_path(root, shard_id)
        path.write_text(
            "".join(_canonical_json({"nodeid": nodeid}) + "\n" for nodeid in nodeids),
            encoding="utf-8",
        )
        shards.append(
            {
                "id": shard_id,
                "path": path.name,
                "count": len(nodeids),
                "sha256": nodeid_digest(nodeids),
            }
        )
    index: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "selector": list(selector),
        "partition": {"algorithm": "lpt-file-static-v1", "shard_count": SHARD_COUNT},
        "full_suite": {"cases": len(full_nodeids), "skips": expected_skips, "sha256": full_digest},
        "shards": shards,
    }
    (root / INDEX_NAME).write_text(_canonical_json(index) + "\n", encoding="utf-8")
    return index


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read manifest JSON {path}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"manifest JSON is not an object: {path}")
    return value


def _read_jsonl(path: Path) -> tuple[str, ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ContractError(f"cannot read shard manifest {path}") from exc
    nodeids: list[str] = []
    for line_number, line in enumerate(lines, start=1):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContractError(f"invalid JSONL at {path}:{line_number}") from exc
        if (
            not isinstance(entry, dict)
            or set(entry) != {"nodeid"}
            or not isinstance(entry["nodeid"], str)
        ):
            raise ContractError(f"invalid shard nodeid record at {path}:{line_number}")
        _file_for_nodeid(entry["nodeid"])
        nodeids.append(entry["nodeid"])
    if len(set(nodeids)) != len(nodeids):
        raise ContractError(f"duplicate nodeid inside shard manifest {path}")
    return tuple(nodeids)


def load_manifest(root: Path) -> tuple[dict[str, Any], tuple[tuple[str, ...], ...]]:
    index = _read_json(root / INDEX_NAME)
    if index.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("unsupported full-suite manifest schema")
    selector = index.get("selector")
    if not isinstance(selector, list) or not all(isinstance(item, str) for item in selector):
        raise ContractError("manifest selector is invalid")
    partition = index.get("partition")
    if not isinstance(partition, dict) or partition.get("shard_count") != SHARD_COUNT:
        raise ContractError("manifest does not define four shards")
    full = index.get("full_suite")
    if (
        not isinstance(full, dict)
        or not all(
            isinstance(full.get(field), int) and full[field] >= 0 for field in ("cases", "skips")
        )
        or not isinstance(full.get("sha256"), str)
    ):
        raise ContractError("manifest full-suite contract is invalid")
    shard_entries = index.get("shards")
    if not isinstance(shard_entries, list) or len(shard_entries) != SHARD_COUNT:
        raise ContractError("manifest shard index is invalid")
    nodeid_groups: list[tuple[str, ...]] = []
    for shard_id, entry in enumerate(shard_entries):
        if not isinstance(entry, dict):
            raise ContractError("manifest shard entry is invalid")
        if entry.get("id") != shard_id or entry.get("path") != f"shard-{shard_id}.jsonl":
            raise ContractError("manifest shard identity is invalid")
        if not isinstance(entry.get("count"), int) or entry["count"] < 0:
            raise ContractError("manifest shard count is invalid")
        if not isinstance(entry.get("sha256"), str):
            raise ContractError("manifest shard digest is invalid")
        nodeids = _read_jsonl(root / entry["path"])
        if len(nodeids) != entry["count"] or nodeid_digest(nodeids) != entry["sha256"]:
            raise ContractError(f"manifest shard {shard_id} digest or count differs")
        nodeid_groups.append(nodeids)
    all_nodeids = tuple(nodeid for group in nodeid_groups for nodeid in group)
    if len(set(all_nodeids)) != len(all_nodeids):
        raise ContractError("manifest shards contain duplicate nodeids")
    if len(all_nodeids) != full["cases"] or nodeid_digest(all_nodeids) != full["sha256"]:
        raise ContractError("manifest full-suite digest or count differs")
    return index, tuple(nodeid_groups)


def validate_manifest(
    root: Path, collected_nodeids: Sequence[str]
) -> tuple[dict[str, Any], tuple[tuple[str, ...], ...]]:
    index, groups = load_manifest(root)
    expected = tuple(sorted(collected_nodeids))
    actual = tuple(sorted(nodeid for group in groups for nodeid in group))
    if len(set(expected)) != len(expected):
        raise ContractError("full-suite collection contains duplicate nodeids")
    if actual != expected:
        actual_set = set(actual)
        expected_set = set(expected)
        missing = len(expected_set - actual_set)
        extra = len(actual_set - expected_set)
        raise ContractError(f"full-suite collection differs: missing={missing} extra={extra}")
    return index, groups


def _collection_path() -> Path | None:
    raw = os.environ.get("RQUANT_FULL_SUITE_COLLECTION_PATH")
    return Path(raw) if raw else None


def pytest_collection_finish(session: Any) -> None:
    """Pytest plugin hook used by :func:`collect_nodeids`, never by normal runs."""
    output = _collection_path()
    if output is None:
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        _canonical_json({"nodeids": [item.nodeid for item in session.items]}) + "\n",
        encoding="utf-8",
    )


def collect_nodeids(selector: Sequence[str]) -> tuple[str, ...]:
    with tempfile.TemporaryDirectory(prefix="rquant-full-suite-collect-") as directory:
        output = Path(directory) / "collection.json"
        environment = os.environ.copy()
        environment.pop("PYTEST_ADDOPTS", None)
        repository_root = str(Path(__file__).resolve().parent.parent)
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            repository_root
            if not existing_pythonpath
            else os.pathsep.join((repository_root, existing_pythonpath))
        )
        environment["RQUANT_FULL_SUITE_COLLECTION_PATH"] = str(output)
        command = [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "scripts.full_suite_shards",
            *selector,
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        if completed.returncode != 0:
            details = (completed.stderr + completed.stdout)[-1_600:]
            raise ContractError(f"pytest collection failed: {details}")
        payload = _read_json(output)
    nodeids = payload.get("nodeids")
    if not isinstance(nodeids, list) or not all(isinstance(nodeid, str) for nodeid in nodeids):
        raise ContractError("pytest collection hook emitted invalid nodeids")
    if len(set(nodeids)) != len(nodeids):
        raise ContractError("pytest collection emitted duplicate nodeids")
    return tuple(sorted(nodeids))


def parse_argsfile_line(line: str) -> str:
    if not line or any(character in line for character in "\x00\r\n"):
        raise ContractError("pytest argsfile line is not a valid nodeid")
    return line


@contextmanager
def argsfile_for_nodeids(nodeids: Sequence[str], *, directory: Path) -> Iterator[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    for nodeid in nodeids:
        if not nodeid or any(character in nodeid for character in "\x00\r\n"):
            raise ContractError("nodeid cannot be encoded safely in a pytest argsfile")
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="full-suite-shard-",
        suffix=".args",
        dir=directory,
        delete=False,
    ) as handle:
        path = Path(handle.name)
        for nodeid in nodeids:
            # Pytest treats each @argsfile line as one literal argument.
            handle.write(nodeid + "\n")
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


def _selection_evidence(index: dict[str, Any], shard_id: int) -> dict[str, Any]:
    full = index["full_suite"]
    shard = index["shards"][shard_id]
    return {
        "schema_version": SCHEMA_VERSION,
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
        "shard": shard_id,
        "full_count": full["cases"],
        "full_digest": full["sha256"],
        "shard_count": shard["count"],
        "shard_digest": shard["sha256"],
    }


def _write_evidence(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(_canonical_json(payload) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_shard(
    *,
    manifest_root: Path,
    shard_id: int,
    mode: str,
    junitxml: Path | None,
    selection_evidence: Path | None,
    basetemp: Path | None,
) -> int:
    index, groups = load_manifest(manifest_root)
    selector = tuple(index["selector"])
    full_nodeids = collect_nodeids(selector)
    index, groups = validate_manifest(manifest_root, full_nodeids)
    if not 0 <= shard_id < SHARD_COUNT:
        raise ContractError(f"shard must be between 0 and {SHARD_COUNT - 1}")
    nodeids = groups[shard_id]
    with argsfile_for_nodeids(nodeids, directory=manifest_root) as argsfile:
        selected_nodeids = collect_nodeids((f"@{argsfile}",))
        if tuple(sorted(nodeids)) != selected_nodeids:
            raise ContractError(f"shard {shard_id} collection differs from its manifest")
        if selection_evidence is not None:
            _write_evidence(selection_evidence, _selection_evidence(index, shard_id))
        if mode == "check":
            return 0
        if junitxml is None or basetemp is None:
            raise ContractError("run mode requires JUnit and basetemp paths")
        junitxml.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            f"@{argsfile}",
            f"--junitxml={junitxml}",
            f"--basetemp={basetemp}",
        ]
        completed = subprocess.run(command, check=False)
    return completed.returncode


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate")
    generate.add_argument("--manifest-dir", type=Path, required=True)
    generate.add_argument("--expected-skips", type=int, required=True)
    run = commands.add_parser("run")
    check = commands.add_parser("check")
    for command in (run, check):
        command.add_argument("--manifest-dir", type=Path, required=True)
        command.add_argument("--shard", type=int, required=True)
    run.add_argument("--junitxml", type=Path, required=True)
    run.add_argument("--selection-evidence", type=Path, required=True)
    run.add_argument("--basetemp", type=Path, required=True)
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    if args.command == "generate":
        nodeids = collect_nodeids(())
        write_manifest_bundle(
            args.manifest_dir,
            selector=(),
            shard_nodeids=plan_shards(nodeids),
            expected_skips=args.expected_skips,
        )
        return 0
    return run_shard(
        manifest_root=args.manifest_dir,
        shard_id=args.shard,
        mode=args.command,
        junitxml=getattr(args, "junitxml", None),
        selection_evidence=getattr(args, "selection_evidence", None),
        basetemp=getattr(args, "basetemp", None),
    )


if __name__ == "__main__":
    raise SystemExit(main())
