"""Generate and execute the versioned, fail-closed full-suite CI shards."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import unicodedata
from collections import defaultdict
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA_VERSION = 1
SHARD_COUNT = 4
INDEX_NAME = "index.json"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MAX_NODEID_BYTES = 1_100_000
MAX_JSONL_LINE_BYTES = MAX_NODEID_BYTES + 32
MAX_INDEX_BYTES = 64 * 1024
MAX_SHARD_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_TOTAL_BYTES = 4 * 1024 * 1024
MAX_COLLECTION_BYTES = MAX_MANIFEST_TOTAL_BYTES
CI_DUMMY_TUSHARE_TOKEN = "0" * 32

_INDEX_FIELDS = frozenset({"schema_version", "selector", "partition", "full_suite", "shards"})
_PARTITION_FIELDS = frozenset({"algorithm", "shard_count"})
_FULL_SUITE_FIELDS = frozenset({"cases", "skips", "sha256"})
_SHARD_FIELDS = frozenset({"id", "path", "count", "sha256"})
_NODEID_FIELDS = frozenset({"nodeid"})
_COLLECTION_FIELDS = frozenset({"nodeids"})
_CI_PRIVATE_DIRECTORIES = ("tmp", "data", "parquet", "logs")
_SENSITIVE_ENVIRONMENT_SUFFIXES = (
    "_API_KEY",
    "_CREDENTIAL",
    "_CREDENTIALS",
    "_PASSWORD",
    "_SECRET",
    "_TOKEN",
    "_TOKENS",
)

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


def _canonical_directory(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise ContractError(f"{label} must be absolute")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ContractError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ContractError(f"{label} must be a non-symlink directory: {path}")
    if resolved != path:
        raise ContractError(f"{label} must be canonical: {path}")
    return resolved


def _private_ci_environment(root: Path) -> dict[str, str]:
    canonical_root = _canonical_directory(root, label="full-suite CI root")
    canonical_root.chmod(0o700)
    directories: dict[str, Path] = {}
    for name in _CI_PRIVATE_DIRECTORIES:
        path = canonical_root / name
        try:
            path.mkdir(mode=0o700)
            path.chmod(0o700)
        except OSError as exc:
            raise ContractError(f"cannot create private full-suite directory: {path}") from exc
        directories[name] = _canonical_directory(path, label="full-suite CI directory")
    return {
        "RQUANT_CI_ROOT": str(canonical_root),
        "TMPDIR": str(directories["tmp"]),
        "TMP": str(directories["tmp"]),
        "TEMP": str(directories["tmp"]),
        "DATA_DIR": str(directories["data"]),
        "DUCKDB_PATH": str(directories["data"] / "test.duckdb"),
        "DUCKDB_READONLY_PATH": str(directories["data"] / "test_ro.duckdb"),
        "PARQUET_DIR": str(directories["parquet"]),
        "LOG_DIR": str(directories["logs"]),
        "RQUANT_DISABLE_DOTENV": "1",
        "TUSHARE_TOKEN_MAIN": CI_DUMMY_TUSHARE_TOKEN,
        "NOTIFY_ENABLED": "false",
    }


def _create_private_ci_environment(base_dir: Path, *, label: str) -> dict[str, str]:
    if re.fullmatch(r"[A-Za-z0-9.-]+", label) is None:
        raise ContractError("full-suite CI environment label is invalid")
    canonical_base = _canonical_directory(base_dir, label="full-suite CI base directory")
    try:
        root = Path(
            tempfile.mkdtemp(
                prefix=f"rqci.full-suite.{label}.",
                dir=canonical_base,
            )
        ).resolve(strict=True)
    except OSError as exc:
        raise ContractError("cannot create private full-suite CI root") from exc
    return _private_ci_environment(root)


def _append_github_environment(path: Path, environment: dict[str, str]) -> None:
    if "\n" in str(path) or "\r" in str(path):
        raise ContractError("GITHUB_ENV path contains a line break")
    try:
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            for name, value in environment.items():
                if any(character in value for character in "\r\n"):
                    raise ContractError(f"full-suite environment value is invalid: {name}")
                handle.write(f"{name}={value}\n")
    except OSError as exc:
        raise ContractError(f"cannot write GITHUB_ENV: {path}") from exc


def _without_inherited_credentials(environment: dict[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in environment.items()
        if not name.upper().endswith(_SENSITIVE_ENVIRONMENT_SUFFIXES)
    }


def _is_line_control(character: str) -> bool:
    return unicodedata.category(character) in {"Cc", "Zl", "Zp"}


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ContractError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _require_exact_fields(value: dict[str, Any], expected: frozenset[str], *, label: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ContractError(f"{label} fields differ: missing={missing} extra={extra}")


def _decode_canonical_object(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        decoded = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except ContractError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"{label} is not valid UTF-8 JSON") from exc
    if type(decoded) is not dict:
        raise ContractError(f"{label} is not a JSON object")
    try:
        canonical = (_canonical_json(decoded) + "\n").encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ContractError(f"{label} cannot be encoded canonically") from exc
    if raw != canonical:
        raise ContractError(f"{label} is not canonical JSON with one LF newline")
    return decoded


def _read_bounded_regular_file(path: Path, *, maximum: int, label: str) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ContractError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ContractError(f"{label} must be a regular non-symlink file: {path}")
    if metadata.st_size > maximum:
        raise ContractError(f"{label} exceeds size limit {maximum}: {path}")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ContractError(f"{label} cannot be read: {path}") from exc
    if len(raw) > maximum:
        raise ContractError(f"{label} exceeds size limit {maximum}: {path}")
    return raw


def _validate_repository_root(repository_root: Path) -> Path:
    try:
        metadata = repository_root.lstat()
        resolved = repository_root.resolve(strict=True)
    except OSError as exc:
        raise ContractError("repository root is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ContractError("repository root must be a non-symlink directory")
    if resolved != repository_root:
        raise ContractError("repository root must be an absolute canonical path")
    return resolved


def _validate_test_file(relative: str, *, repository_root: Path) -> None:
    resolved_root = _validate_repository_root(repository_root)
    candidate = repository_root
    parts = PurePosixPath(relative).parts
    for index, part in enumerate(parts):
        candidate /= part
        try:
            metadata = candidate.lstat()
        except OSError as exc:
            raise ContractError(f"nodeid test file is unavailable: {relative}") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise ContractError(f"nodeid test path contains a symlink: {relative}")
        final = index == len(parts) - 1
        if final and not stat.S_ISREG(metadata.st_mode):
            raise ContractError(f"nodeid test path is not a regular file: {relative}")
        if not final and not stat.S_ISDIR(metadata.st_mode):
            raise ContractError(f"nodeid test path ancestor is not a directory: {relative}")
    try:
        resolved = candidate.resolve(strict=True)
        resolved_relative = resolved.relative_to(resolved_root).as_posix()
    except (OSError, ValueError) as exc:
        raise ContractError(
            f"nodeid test path resolves outside the repository: {relative}"
        ) from exc
    if resolved != candidate or resolved_relative != relative:
        raise ContractError(f"nodeid test path is not canonical: {relative}")


def nodeid_digest(nodeids: Sequence[str]) -> str:
    unique = sorted(set(nodeids))
    if len(unique) != len(nodeids):
        raise ContractError("nodeid digest input contains duplicates")
    return hashlib.sha256("".join(f"{nodeid}\n" for nodeid in unique).encode("utf-8")).hexdigest()


def _valid_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _file_for_nodeid(
    nodeid: str,
    *,
    repository_root: Path = REPOSITORY_ROOT,
    validated_files: set[str] | None = None,
) -> str:
    if type(nodeid) is not str or not nodeid:
        raise ContractError("pytest nodeid must be a nonempty string")
    try:
        encoded = nodeid.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ContractError("pytest nodeid is not valid UTF-8") from exc
    if len(encoded) > MAX_NODEID_BYTES:
        raise ContractError(f"pytest nodeid exceeds size limit {MAX_NODEID_BYTES}")
    if any(_is_line_control(character) for character in nodeid):
        raise ContractError("pytest nodeid contains a control character")
    path, separator, selection = nodeid.partition("::")
    if not separator or not selection:
        raise ContractError(f"invalid pytest nodeid: {nodeid!r}")
    if nodeid.startswith("-") or path.startswith("-"):
        raise ContractError("pytest nodeid cannot be option-prefixed")
    pure_path = PurePosixPath(path)
    raw_parts = path.split("/")
    if (
        pure_path.is_absolute()
        or path != pure_path.as_posix()
        or "\\" in path
        or any(part in {"", ".", ".."} for part in raw_parts)
        or len(pure_path.parts) < 2
        or pure_path.parts[0] != "tests"
        or pure_path.suffix != ".py"
    ):
        raise ContractError(f"pytest nodeid path is not canonical under tests/: {path!r}")
    if validated_files is None or path not in validated_files:
        _validate_test_file(path, repository_root=repository_root)
        if validated_files is not None:
            validated_files.add(path)
    return path


def plan_shards(
    nodeids: Sequence[str],
    *,
    shard_count: int = SHARD_COUNT,
    repository_root: Path = REPOSITORY_ROOT,
) -> tuple[tuple[str, ...], ...]:
    """Use deterministic LPT on test-file history, not directory or case count alone."""
    if shard_count != SHARD_COUNT:
        raise ContractError(f"the full-suite contract requires exactly {SHARD_COUNT} shards")
    files: dict[str, list[str]] = defaultdict(list)
    validated_files: set[str] = set()
    for nodeid in sorted(nodeids):
        files[
            _file_for_nodeid(
                nodeid,
                repository_root=repository_root,
                validated_files=validated_files,
            )
        ].append(nodeid)
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
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, Any]:
    if type(selector) not in {list, tuple} or selector:
        raise ContractError("v1 manifest selector must be exactly empty")
    if len(shard_nodeids) != SHARD_COUNT:
        raise ContractError(f"expected {SHARD_COUNT} shard nodeid lists")
    if type(expected_skips) is not int or expected_skips < 0:
        raise ContractError("expected skips must be nonnegative")
    normalized = tuple(tuple(sorted(group)) for group in shard_nodeids)
    validated_files: set[str] = set()
    for group in normalized:
        for nodeid in group:
            _file_for_nodeid(
                nodeid,
                repository_root=repository_root,
                validated_files=validated_files,
            )
    full_nodeids = tuple(nodeid for group in normalized for nodeid in group)
    full_digest = nodeid_digest(full_nodeids)
    shard_payloads: list[bytes] = []
    shards: list[dict[str, Any]] = []
    for shard_id, nodeids in enumerate(normalized):
        lines = tuple(
            (_canonical_json({"nodeid": nodeid}) + "\n").encode("utf-8") for nodeid in nodeids
        )
        if any(len(line) > MAX_JSONL_LINE_BYTES for line in lines):
            raise ContractError(f"shard {shard_id} JSONL line exceeds size limit")
        payload = b"".join(lines)
        if len(payload) > MAX_SHARD_MANIFEST_BYTES:
            raise ContractError(f"shard {shard_id} manifest exceeds size limit")
        shard_payloads.append(payload)
        shards.append(
            {
                "id": shard_id,
                "path": f"shard-{shard_id}.jsonl",
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
    index_payload = (_canonical_json(index) + "\n").encode("utf-8")
    if len(index_payload) > MAX_INDEX_BYTES:
        raise ContractError("manifest index exceeds size limit")
    if len(index_payload) + sum(map(len, shard_payloads)) > MAX_MANIFEST_TOTAL_BYTES:
        raise ContractError("manifest total size exceeds limit")
    root.mkdir(parents=True, exist_ok=True)
    (root / INDEX_NAME).write_bytes(index_payload)
    for shard_id, payload in enumerate(shard_payloads):
        _jsonl_path(root, shard_id).write_bytes(payload)
    return index


def _read_json(path: Path, *, maximum: int, label: str) -> dict[str, Any]:
    raw = _read_bounded_regular_file(path, maximum=maximum, label=label)
    return _decode_canonical_object(raw, label=label)


def _read_jsonl(path: Path, *, repository_root: Path) -> tuple[str, ...]:
    raw = _read_bounded_regular_file(
        path,
        maximum=MAX_SHARD_MANIFEST_BYTES,
        label="shard manifest",
    )
    lines = raw.splitlines(keepends=True)
    nodeids: list[str] = []
    validated_files: set[str] = set()
    for line_number, raw_line in enumerate(lines, start=1):
        if len(raw_line) > MAX_JSONL_LINE_BYTES:
            raise ContractError(f"shard manifest line exceeds size limit at {path}:{line_number}")
        entry = _decode_canonical_object(
            raw_line,
            label=f"shard manifest line {path}:{line_number}",
        )
        _require_exact_fields(entry, _NODEID_FIELDS, label="shard nodeid record")
        if type(entry["nodeid"]) is not str:
            raise ContractError(f"invalid shard nodeid record at {path}:{line_number}")
        _file_for_nodeid(
            entry["nodeid"],
            repository_root=repository_root,
            validated_files=validated_files,
        )
        nodeids.append(entry["nodeid"])
    if len(set(nodeids)) != len(nodeids):
        raise ContractError(f"duplicate nodeid inside shard manifest {path}")
    return tuple(nodeids)


def load_manifest(
    root: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> tuple[dict[str, Any], tuple[tuple[str, ...], ...]]:
    manifest_paths = (
        root / INDEX_NAME,
        *(_jsonl_path(root, shard) for shard in range(SHARD_COUNT)),
    )
    total_size = 0
    for path in manifest_paths:
        try:
            total_size += path.lstat().st_size
        except OSError as exc:
            raise ContractError(f"manifest file is unavailable: {path}") from exc
    if total_size > MAX_MANIFEST_TOTAL_BYTES:
        raise ContractError("manifest total size exceeds limit")
    index = _read_json(root / INDEX_NAME, maximum=MAX_INDEX_BYTES, label="manifest index")
    _require_exact_fields(index, _INDEX_FIELDS, label="manifest index")
    if type(index["schema_version"]) is not int or index["schema_version"] != SCHEMA_VERSION:
        raise ContractError("unsupported full-suite manifest schema")
    if type(index["selector"]) is not list or index["selector"] != []:
        raise ContractError("v1 manifest selector must be exactly []")
    partition = index["partition"]
    if type(partition) is not dict:
        raise ContractError("manifest partition is invalid")
    _require_exact_fields(partition, _PARTITION_FIELDS, label="manifest partition")
    if (
        partition["algorithm"] != "lpt-file-static-v1"
        or type(partition["shard_count"]) is not int
        or partition["shard_count"] != SHARD_COUNT
    ):
        raise ContractError("manifest does not define four shards")
    full = index["full_suite"]
    if type(full) is not dict:
        raise ContractError("manifest full-suite contract is invalid")
    _require_exact_fields(full, _FULL_SUITE_FIELDS, label="manifest full-suite contract")
    if not all(
        type(full[field]) is int and full[field] >= 0 for field in ("cases", "skips")
    ) or not _valid_digest(full["sha256"]):
        raise ContractError("manifest full-suite contract is invalid")
    shard_entries = index["shards"]
    if type(shard_entries) is not list or len(shard_entries) != SHARD_COUNT:
        raise ContractError("manifest shard index is invalid")
    nodeid_groups: list[tuple[str, ...]] = []
    for shard_id, entry in enumerate(shard_entries):
        if type(entry) is not dict:
            raise ContractError("manifest shard entry is invalid")
        _require_exact_fields(entry, _SHARD_FIELDS, label="manifest shard entry")
        if (
            type(entry["id"]) is not int
            or entry["id"] != shard_id
            or type(entry["path"]) is not str
            or entry["path"] != f"shard-{shard_id}.jsonl"
        ):
            raise ContractError("manifest shard identity is invalid")
        if type(entry["count"]) is not int or entry["count"] < 0:
            raise ContractError("manifest shard count is invalid")
        if not _valid_digest(entry["sha256"]):
            raise ContractError("manifest shard digest is invalid")
        nodeids = _read_jsonl(root / entry["path"], repository_root=repository_root)
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
    root: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> tuple[dict[str, Any], tuple[tuple[str, ...], ...]]:
    index, groups = load_manifest(root, repository_root=repository_root)
    expected = tuple(sorted(collect_nodeids((), repository_root=repository_root)))
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


def collect_nodeids(
    selector: Sequence[str],
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> tuple[str, ...]:
    with tempfile.TemporaryDirectory(prefix="rquant-full-suite-collect-") as directory:
        private_root = Path(directory).resolve(strict=True)
        output = private_root / "collection.json"
        environment = _without_inherited_credentials(os.environ.copy())
        environment.update(_private_ci_environment(private_root))
        environment.pop("PYTEST_ADDOPTS", None)
        pythonpath_root = str(Path(__file__).resolve().parent.parent)
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            pythonpath_root
            if not existing_pythonpath
            else os.pathsep.join((pythonpath_root, existing_pythonpath))
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
            cwd=repository_root,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        if completed.returncode != 0:
            details = (completed.stderr + completed.stdout)[-1_600:]
            raise ContractError(f"pytest collection failed: {details}")
        payload = _read_json(
            output,
            maximum=MAX_COLLECTION_BYTES,
            label="pytest collection evidence",
        )
        _require_exact_fields(payload, _COLLECTION_FIELDS, label="pytest collection evidence")
    nodeids = payload["nodeids"]
    if type(nodeids) is not list or not all(type(nodeid) is str for nodeid in nodeids):
        raise ContractError("pytest collection hook emitted invalid nodeids")
    if len(set(nodeids)) != len(nodeids):
        raise ContractError("pytest collection emitted duplicate nodeids")
    validated_files: set[str] = set()
    for nodeid in nodeids:
        _file_for_nodeid(
            nodeid,
            repository_root=repository_root,
            validated_files=validated_files,
        )
    return tuple(sorted(nodeids))


def parse_argsfile_line(line: str) -> str:
    if not line or any(_is_line_control(character) for character in line):
        raise ContractError("pytest argsfile line is not a valid nodeid")
    if len(line.encode("utf-8")) > MAX_NODEID_BYTES:
        raise ContractError("pytest argsfile nodeid exceeds size limit")
    return line


@contextmanager
def argsfile_for_nodeids(
    nodeids: Sequence[str],
    *,
    directory: Path,
    repository_root: Path = REPOSITORY_ROOT,
) -> Iterator[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    validated_files: set[str] = set()
    for nodeid in nodeids:
        _file_for_nodeid(
            nodeid,
            repository_root=repository_root,
            validated_files=validated_files,
        )
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
    index, groups = validate_manifest(manifest_root)
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
    prepare_environment = commands.add_parser("prepare-environment")
    prepare_environment.add_argument("--github-env", type=Path, required=True)
    prepare_environment.add_argument("--base-dir", type=Path, required=True)
    prepare_environment.add_argument("--label", required=True)
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
    if args.command == "prepare-environment":
        environment = _create_private_ci_environment(args.base_dir, label=args.label)
        _append_github_environment(args.github_env, environment)
        return 0
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
