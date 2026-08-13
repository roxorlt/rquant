from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
import signal
import stat
import subprocess
import sys
import sysconfig
import tarfile
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from xml.etree import ElementTree

import pytest
from pydantic import ConfigDict, Field

from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootRequest,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.external_monotonic_root_service import (
    ExternalMonotonicRootUnixService,
    ExternalRootServiceConfiguration,
    ExternalRootStoredState,
    PersistentExternalMonotonicRootBackend,
)
from rquant.formal_runtime_composition import FormalRuntimeBootstrapConfiguration
from rquant.formal_smoke_protocol import FormalSmokeExecutionReceipt, FormalSmokeStrategy
from rquant.runtime_code_attestation import (
    CodeTrustEvidence,
    RuntimeCodeAttestation,
    RuntimeCodeBundleEntry,
    RuntimeCodeExecutionSpec,
    RuntimeCodeFile,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strict_json import (
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)
from tests.runtime_code_e2e_support import (
    RuntimeCodeTestPackage,
    build_test_package,
    install_test_package,
)

_TRADE_DATE = date(2026, 7, 14)
_AS_OF = datetime(2026, 7, 14, 9, tzinfo=UTC)
_MAX_CLOSURE_BYTES = 512 * 1024 * 1024
_EXACT_CASE_COUNT = 2
_HASH_PATTERN = r"^[0-9a-f]{64}$"
PRODUCT_IMPORT_ROOTS = ("release/runtime-site-packages", "release/src")
_EXACT_TEST_NAMES = frozenset(
    {
        "test_checkout_b_executes_real_generation_a_and_publishes_bound_artifacts",
        "test_real_generation_business_gate_rejects_unknown_audit_and_snapshot",
    }
)
_PROVENANCE_MODULE_PATHS = {
    "rquant": "release/src/rquant/__init__.py",
    "rquant.cli": "release/src/rquant/cli.py",
    "rquant.formal_smoke_runtime_entry": ("release/src/rquant/formal_smoke_runtime_entry.py"),
    "rquant.formal_smoke_replay": "release/src/rquant/formal_smoke_replay.py",
    "rquant.strategy_compare": "release/src/rquant/strategy_compare.py",
}
_RUNTIME_DISTRIBUTIONS = (
    "annotated-types",
    "duckdb",
    "loguru",
    "numpy",
    "pandas",
    "pydantic",
    "pydantic-core",
    "pydantic-settings",
    "python-dateutil",
    "python-dotenv",
    "pytz",
    "six",
    "ta",
    "typing-extensions",
    "typing-inspection",
)
_CHILD_ENVIRONMENT_NAMES = tuple(
    sorted(
        (
            "DATA_DIR",
            "DUCKDB_PATH",
            "DUCKDB_READONLY_PATH",
            "LOG_DIR",
            "NOTIFY_ENABLED",
            "PARQUET_DIR",
            "RESEARCH_DB_PATH",
            "RESEARCH_LAKE_DIR",
            "RESEARCH_READONLY_DB_PATH",
            "RESEARCH_STAGING_DIR",
            "RQUANT_DISABLE_DOTENV",
            "TUSHARE_TOKEN_MAIN",
        )
    )
)


class FormalSmokeInput(RuntimeContractModel):
    strategy: FormalSmokeStrategy = "n_shape"
    start_date: date
    end_date: date
    audit_run_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class RedactedArtifactDigests(RuntimeContractModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        serialize_by_alias=True,
        validate_by_alias=True,
        validate_by_name=False,
    )

    json_sha256: str = Field(alias="json", serialization_alias="json", pattern=_HASH_PATTERN)
    markdown_sha256: str = Field(
        alias="markdown",
        serialization_alias="markdown",
        pattern=_HASH_PATTERN,
    )


class RedactedExactFacts(RuntimeContractModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    python_version: str = Field(pattern=r"^[0-9]+\.[0-9]+\.[0-9]+$")
    generation_id: str = Field(pattern=_HASH_PATTERN)
    content_root_sha256: str = Field(pattern=_HASH_PATTERN)
    receipt_digest: str = Field(pattern=_HASH_PATTERN)
    artifact_digests: RedactedArtifactDigests


class ModuleProvenance(RuntimeContractModel):
    name: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    sha256: str = Field(pattern=_HASH_PATTERN)


class GenerationProvenanceProbe(RuntimeContractModel):
    modules: tuple[ModuleProvenance, ...]


@dataclass(frozen=True)
class SealedFormalSmokeData:
    catalog_path: Path
    lake_root: Path
    code_commit: str
    formal_input: FormalSmokeInput


@dataclass(frozen=True)
class RealFormalSmokeGeneration:
    package: RuntimeCodeTestPackage
    trusted_base: Path
    runtime_root: Path
    generation_root: Path
    child_environment: Mapping[str, str]
    formal_data: SealedFormalSmokeData
    code_trust_evidence: CodeTrustEvidence
    import_roots: tuple[str, ...]
    provenance_probe: GenerationProvenanceProbe


@dataclass(frozen=True)
class CliInvocation:
    exit_code: int
    stdout: str
    stderr: str


def _source_commit(source_root: Path) -> str:
    head = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=source_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if head.returncode != 0:
        raise RuntimeError("real generation source commit is unavailable")
    commit = head.stdout.strip().lower()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("real generation source commit is invalid")
    _require_clean_rquant_source(source_root)
    return commit


def _run_git(source_root: Path, *arguments: str) -> bytes:
    result = subprocess.run(
        ("git", *arguments),
        cwd=source_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError("real generation HEAD source is unavailable")
    return result.stdout


def verified_head_rquant_sources(source_root: Path) -> dict[PurePosixPath, bytes]:
    _require_clean_rquant_source(source_root)
    archive = _run_git(
        source_root,
        "archive",
        "--format=tar",
        "HEAD",
        "src/rquant",
    )
    prefix = PurePosixPath("src/rquant")
    sources: dict[PurePosixPath, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tree:
        for member in tree.getmembers():
            if member.isdir():
                continue
            if not member.isfile():
                raise RuntimeError("real generation HEAD source is not a regular file")
            tracked_path = PurePosixPath(member.name)
            try:
                relative = tracked_path.relative_to(prefix)
            except ValueError as exc:
                raise RuntimeError("real generation HEAD source path escapes rQuant") from exc
            extracted = tree.extractfile(member)
            if extracted is None:
                raise RuntimeError("real generation HEAD source bytes are unavailable")
            head_bytes = extracted.read()
            source = source_root.joinpath(*tracked_path.parts)
            observed = source.lstat()
            if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
                raise RuntimeError("real generation HEAD source is not a regular file")
            if source.read_bytes() != head_bytes:
                raise RuntimeError("real generation rQuant source differs from HEAD")
            sources[relative] = head_bytes
    if PurePosixPath("__init__.py") not in sources:
        raise RuntimeError("real generation HEAD source has no rQuant package")
    return sources


def _require_clean_rquant_source(source_root: Path) -> None:
    status = _run_git(
        source_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--",
        "src/rquant",
    )
    if status:
        raise RuntimeError("real generation rQuant source differs from HEAD")


def _seed_formal_source(store: object) -> None:
    store._conn.execute(
        """
        INSERT INTO trade_calendar
        (exchange, cal_date, is_open, pretrade_date, source, updated_at)
        VALUES (
            'SSE', DATE '2026-07-14', TRUE, DATE '2026-07-13',
            'exact-gate', TIMESTAMPTZ '2026-07-14 08:00:00+00'
        );

        INSERT INTO minute_bar
        (ts_code, trade_time, freq, open, high, low, close, vol, amount,
         source, created_at)
        VALUES (
            '000001.SZ', TIMESTAMP '2026-07-14 09:30:00', '1min',
            10, 10.2, 9.9, 10.1, 1000, 10100, 'tushare',
            TIMESTAMP '2026-07-14 16:00:00'
        );

        INSERT INTO auction_bar
        (ts_code, trade_date, auction_type, price, vol, amount,
         turnover_rate, volume_ratio, source, created_at)
        VALUES (
            '000001.SZ', DATE '2026-07-14', 'open', 10, 1000, 10000,
            0.1, 1.5, 'tushare', TIMESTAMP '2026-07-14 09:26:00'
        );
        """
    )


def build_sealed_formal_smoke_data(
    root: Path,
    *,
    source_root: Path,
) -> SealedFormalSmokeData:
    from rquant.backfill_manifest import EligibilityResolution
    from rquant.data_metadata import (
        DataAuditRun,
        DataAuditRunFinalization,
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
    )
    from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_lake import export_research_dataset
    from rquant.research_snapshot import build_dataset_snapshot_binding
    from rquant.storage.duckdb import DuckDBStore

    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    root.chmod(0o700)
    lake_root = root / "lake"
    lake_root.mkdir(mode=0o700)
    code_commit = _source_commit(source_root)
    catalog_path = root / "formal-readonly.duckdb"
    research_catalog = ResearchCatalog(root / "partition-catalog.duckdb")
    eligibility = EligibilityResolution(
        strategy_id="n_shape",
        strategy_version="v1",
        requested_dates=(_TRADE_DATE,),
        evaluated_dates=(_TRADE_DATE,),
        complete_dates=(_TRADE_DATE,),
        records=(),
    )

    with DuckDBStore(catalog_path) as store:
        _seed_formal_source(store)
        for dataset in ("minute_bar", "auction_bar"):
            export_research_dataset(
                store._conn,
                catalog=research_catalog,
                lake_root=lake_root,
                dataset=dataset,
                start_date=_TRADE_DATE,
                end_date=_TRADE_DATE,
                code_commit=code_commit,
                now=lambda: _AS_OF - timedelta(minutes=5),
            )
        pending_audit = DataAuditRun.create(
            as_of_date=_TRADE_DATE,
            range_start=_TRADE_DATE,
            range_end=_TRADE_DATE,
            rule_set_version=STAGE1_AUDIT_RULE_SET_VERSION,
            observed_at=_AS_OF,
        )
        store.begin_data_audit_run(pending_audit)
        audit = store.finalize_data_audit_run(
            pending_audit.audit_run_id,
            DataAuditRunFinalization(
                p0_count=0,
                completed_at=_AS_OF + timedelta(minutes=1),
            ),
        )
        pending_snapshot = DatasetSnapshot.create(
            strategy_name="n_shape",
            manifest_id="a" * 64,
            as_of_time=_AS_OF,
            code_commit=code_commit,
            origin="formal_smoke_real_generation_exact",
            created_at=_AS_OF,
        )
        store.begin_dataset_snapshot(pending_snapshot)
        for scope in ("eligibility", "baseline", "entry", "exit"):
            store.upsert_dataset_coverage(
                DatasetCoverage(
                    snapshot_id=pending_snapshot.snapshot_id,
                    dataset_id=("strategy_eligibility" if scope == "eligibility" else "minute_bar"),
                    coverage_scope=scope,
                    table_name=("backfill_manifest" if scope == "eligibility" else "minute_bar"),
                    expected_count=1,
                    available_count=1,
                )
            )
        snapshot = store.finalize_dataset_snapshot(
            pending_snapshot.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks={
                    "manifest_start_date": _TRADE_DATE.isoformat(),
                    "manifest_end_date": _TRADE_DATE.isoformat(),
                    "eligibility_resolution_hash": eligibility.resolution_hash,
                },
                completed_at=_AS_OF + timedelta(minutes=2),
            ),
        )
        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=research_catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=_TRADE_DATE,
            end_date=_TRADE_DATE,
            ts_codes=("000001.SZ",),
            eligibility_resolution=eligibility,
            now=lambda: _AS_OF + timedelta(minutes=3),
        )

    catalog_path.chmod(0o444)
    for path in lake_root.rglob("*"):
        if path.is_symlink():
            raise RuntimeError("sealed formal data contains a symlink")
        if path.is_file():
            path.chmod(0o444)
    return SealedFormalSmokeData(
        catalog_path=catalog_path,
        lake_root=lake_root,
        code_commit=code_commit,
        formal_input=FormalSmokeInput(
            start_date=_TRADE_DATE,
            end_date=_TRADE_DATE,
            audit_run_id=audit.audit_run_id,
            dataset_snapshot_id=snapshot.snapshot_id,
            dataset_binding_hash=binding.binding_hash,
        ),
    )


def _regular_entry(
    *, source: Path, target: PurePosixPath, mode: int = 0o444
) -> RuntimeCodeBundleEntry:
    observed = source.lstat()
    if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise RuntimeError(f"real generation closure source is not regular: {target}")
    return RuntimeCodeBundleEntry(path=target.as_posix(), mode=mode, content=source.read_bytes())


def _add_entry(
    entries: dict[str, RuntimeCodeBundleEntry],
    entry: RuntimeCodeBundleEntry,
) -> None:
    existing = entries.get(entry.path)
    if existing is not None and existing != entry:
        raise RuntimeError(f"real generation closure path collision: {entry.path}")
    entries[entry.path] = entry


def _source_entries(
    sources: Mapping[PurePosixPath, bytes],
) -> tuple[RuntimeCodeBundleEntry, ...]:
    entries: dict[str, RuntimeCodeBundleEntry] = {}
    for relative, content in sorted(sources.items(), key=lambda item: item[0].as_posix()):
        if relative == PurePosixPath("__init__.py"):
            continue
        _add_entry(
            entries,
            RuntimeCodeBundleEntry(
                path=(PurePosixPath("release/src/rquant") / relative).as_posix(),
                mode=0o444,
                content=content,
            ),
        )
    return tuple(entries[path] for path in sorted(entries))


def require_no_higher_priority_rquant_provider(
    entries: tuple[RuntimeCodeBundleEntry, ...],
    *,
    import_roots: tuple[str, ...],
) -> None:
    if import_roots != PRODUCT_IMPORT_ROOTS:
        raise RuntimeError("real generation import roots do not use product order")
    source_index = import_roots.index("release/src")
    for root_value in import_roots[:source_index]:
        root = PurePosixPath(root_value)
        for entry in entries:
            path = PurePosixPath(entry.path)
            try:
                relative = path.relative_to(root)
            except ValueError:
                continue
            first = relative.parts[0].casefold() if relative.parts else ""
            if first == "rquant" or (
                len(relative.parts) == 1 and relative.name.casefold() == "rquant.py"
            ):
                raise RuntimeError(
                    f"real generation higher-priority rQuant provider is forbidden: {entry.path}"
                )


def _stdlib_entries() -> tuple[RuntimeCodeBundleEntry, ...]:
    stdlib_root = Path(sysconfig.get_path("stdlib")).absolute()
    target_root = (
        PurePosixPath("release/lib") / f"python{sys.version_info.major}.{sys.version_info.minor}"
    )
    entries: dict[str, RuntimeCodeBundleEntry] = {}
    for source in sorted(stdlib_root.rglob("*")):
        relative = source.relative_to(stdlib_root)
        if (
            "site-packages" in relative.parts
            or "__pycache__" in relative.parts
            or source.suffix == ".pyc"
        ):
            continue
        if source.is_symlink():
            raise RuntimeError(f"CPython stdlib contains a symlink: {relative.as_posix()}")
        if source.is_file():
            _add_entry(
                entries,
                _regular_entry(source=source, target=target_root / relative.as_posix()),
            )
    library_root = Path(sys.base_prefix) / "lib"
    for source in sorted(library_root.glob("libpython*")):
        if source.is_symlink() or not source.is_file():
            continue
        _add_entry(
            entries,
            _regular_entry(
                source=source,
                target=PurePosixPath("release/lib") / source.name,
            ),
        )
    return tuple(entries[path] for path in sorted(entries))


def _distribution_entries(venv_root: Path) -> tuple[RuntimeCodeBundleEntry, ...]:
    if Path(sys.prefix).resolve() != venv_root.resolve():
        raise RuntimeError("real generation distribution closure must use the active locked venv")
    site_root = Path(sysconfig.get_path("purelib")).absolute()
    entries: dict[str, RuntimeCodeBundleEntry] = {}
    for name in _RUNTIME_DISTRIBUTIONS:
        distribution = importlib.metadata.distribution(name)
        files = distribution.files
        if files is None:
            raise RuntimeError(f"real generation distribution has no file table: {name}")
        for distribution_record in files:
            relative = PurePosixPath(distribution_record.as_posix())
            if relative.is_absolute() or ".." in relative.parts:
                continue
            source = site_root.joinpath(*relative.parts)
            if "__pycache__" in relative.parts or source.suffix == ".pyc":
                continue
            if source.is_symlink():
                raise RuntimeError(
                    f"real generation distribution contains a symlink: {name}/{relative.as_posix()}"
                )
            if not source.is_file():
                continue
            _add_entry(
                entries,
                _regular_entry(
                    source=source,
                    target=PurePosixPath("release/runtime-site-packages") / relative.as_posix(),
                ),
            )
    return tuple(entries[path] for path in sorted(entries))


def _child_environment(root: Path, formal_data: SealedFormalSmokeData) -> dict[str, str]:
    data = root / "child-data"
    logs = root / "child-logs"
    parquet = root / "child-parquet"
    research_staging = root / "child-research-staging"
    for path in (data, logs, parquet, research_staging):
        path.mkdir(mode=0o700)
    values = {
        "DATA_DIR": os.fspath(data),
        "DUCKDB_PATH": os.fspath(data / "unused-main.duckdb"),
        "DUCKDB_READONLY_PATH": os.fspath(formal_data.catalog_path),
        "LOG_DIR": os.fspath(logs),
        "NOTIFY_ENABLED": "false",
        "PARQUET_DIR": os.fspath(parquet),
        "RESEARCH_DB_PATH": os.fspath(data / "unused-research.duckdb"),
        "RESEARCH_LAKE_DIR": os.fspath(formal_data.lake_root),
        "RESEARCH_READONLY_DB_PATH": os.fspath(data / "unused-research-ro.duckdb"),
        "RESEARCH_STAGING_DIR": os.fspath(research_staging),
        "RQUANT_DISABLE_DOTENV": "1",
        "TUSHARE_TOKEN_MAIN": "0" * 32,
    }
    if tuple(sorted(values)) != _CHILD_ENVIRONMENT_NAMES:
        raise RuntimeError("real generation child environment differs from its allowlist")
    if any(not Path(value).is_absolute() for name, value in values.items() if name.endswith("DIR")):
        raise RuntimeError("real generation child directory is not absolute")
    return values


_PROVENANCE_PROBE = "\n".join(
    (
        "import hashlib, importlib, json, os, sys",
        "generation = os.path.realpath(sys.argv[1])",
        "roots = json.loads(sys.argv[2])",
        "names = json.loads(sys.argv[3])",
        "sys.path[:0] = roots",
        "modules = []",
        "for name in names:",
        "    module = importlib.import_module(name)",
        "    path = os.path.realpath(module.__file__)",
        "    with open(path, 'rb') as source:",
        "        digest = hashlib.sha256(source.read()).hexdigest()",
        "    modules.append({'name': name, 'relative_path': os.path.relpath(path, generation), "
        "'sha256': digest})",
        "sys.stdout.write(json.dumps({'modules': modules}, sort_keys=True, separators=(',', ':')))",
    )
)


def probe_installed_generation_provenance(
    *,
    generation_root: Path,
    execution_spec: RuntimeCodeExecutionSpec,
    attested_files: tuple[RuntimeCodeFile, ...],
    environment: Mapping[str, str],
) -> GenerationProvenanceProbe:
    if execution_spec.import_roots != PRODUCT_IMPORT_ROOTS:
        raise RuntimeError("real generation provenance probe import roots are invalid")
    if any(name.startswith(("GIT_", "PYTHON", "DYLD_", "LD_")) for name in environment):
        raise RuntimeError("real generation provenance probe has a routing environment")
    interpreter = generation_root.joinpath(*PurePosixPath(execution_spec.interpreter_path).parts)
    absolute_roots = tuple(
        os.fspath(generation_root.joinpath(*PurePosixPath(root).parts))
        for root in execution_spec.import_roots
    )
    result = subprocess.run(
        (
            os.fspath(interpreter),
            "-I",
            "-B",
            "-S",
            "-c",
            _PROVENANCE_PROBE,
            os.fspath(generation_root),
            json.dumps(absolute_roots, separators=(",", ":")),
            json.dumps(tuple(_PROVENANCE_MODULE_PATHS), separators=(",", ":")),
        ),
        cwd=generation_root / "release",
        env=dict(environment),
        check=False,
        capture_output=True,
        timeout=30,
    )
    if result.returncode != 0 or result.stderr:
        raise RuntimeError("real generation provenance probe failed")
    probe = strict_model_validate_canonical_json(GenerationProvenanceProbe, result.stdout)
    expected_names = tuple(_PROVENANCE_MODULE_PATHS)
    if tuple(module.name for module in probe.modules) != expected_names:
        raise RuntimeError("real generation provenance probe module set is invalid")
    by_path = {file.path: file for file in attested_files}
    source_root = PurePosixPath("release/src/rquant")
    for module in probe.modules:
        expected_path = _PROVENANCE_MODULE_PATHS[module.name]
        path = PurePosixPath(module.relative_path)
        if (
            path.as_posix() != module.relative_path
            or path != PurePosixPath(expected_path)
            or not path.is_relative_to(source_root)
        ):
            raise RuntimeError("real generation provenance module escaped reviewed source")
        descriptor = by_path.get(module.relative_path)
        if descriptor is None or descriptor.sha256 != module.sha256:
            raise RuntimeError("real generation provenance module is not attested")
    return probe


def build_real_formal_smoke_generation(
    root: Path,
    *,
    source_root: Path,
    venv_root: Path,
    formal_data: SealedFormalSmokeData,
    now: datetime,
) -> RealFormalSmokeGeneration:
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    root.chmod(0o700)
    if formal_data.code_commit != _source_commit(source_root):
        raise RuntimeError("sealed formal data commit differs from generation source")
    launcher_path = venv_root / "bin" / "rquant"
    launcher_bytes = launcher_path.read_bytes()
    if (
        b"from rquant.cli import main" not in launcher_bytes
        or b"formal-smoke-runtime-execute" in launcher_bytes
        or b"execution_receipt" in launcher_bytes
    ):
        raise RuntimeError("installed rQuant launcher is not the ordinary console entry")
    interpreter = Path(sys.executable).resolve()
    python_abi = sys.implementation.cache_tag
    if python_abi is None:
        raise RuntimeError("active Python ABI tag is unavailable")
    source_files = verified_head_rquant_sources(source_root)
    import_roots = PRODUCT_IMPORT_ROOTS
    execution_spec = RuntimeCodeExecutionSpec(
        launcher_path="release/bin/rquant",
        working_directory="release",
        import_roots=import_roots,
        interpreter_path="release/bin/python",
        interpreter_sha256=hashlib.sha256(interpreter.read_bytes()).hexdigest(),
        python_abi=python_abi,
        environment_allowlist=_CHILD_ENVIRONMENT_NAMES,
    )
    dependencies = (*_stdlib_entries(), *_distribution_entries(venv_root))
    require_no_higher_priority_rquant_provider(dependencies, import_roots=import_roots)
    closure = (
        *_source_entries(source_files),
        *dependencies,
    )
    closure_bytes = sum(len(entry.content) for entry in closure)
    if not 0 < closure_bytes <= _MAX_CLOSURE_BYTES:
        raise RuntimeError(f"real generation closure size is invalid: {closure_bytes}")
    package = build_test_package(
        root / "signed-package",
        source=source_files[PurePosixPath("__init__.py")],
        source_path="release/src/rquant/__init__.py",
        provenance_commit=formal_data.code_commit,
        extra_entries=tuple(closure),
        environment_allowlist=_CHILD_ENVIRONMENT_NAMES,
        interpreter_bytes=interpreter.read_bytes(),
        launcher_bytes=launcher_bytes,
        import_roots=import_roots,
        python_abi=python_abi,
        now=now,
    )
    trusted_base, runtime_root, _installer = install_test_package(root, package)
    generation_root = runtime_root / "generations" / package.receipt.generation_id
    evidence = CodeTrustEvidence(
        generation_id=package.receipt.generation_id,
        attestation_sha256=package.receipt.attestation_sha256,
        content_root_sha256=package.receipt.content_root_sha256,
        promotion_sequence=package.receipt.promotion_sequence,
        provenance_commit=formal_data.code_commit,
    )
    child_environment = _child_environment(root, formal_data)
    attestation = strict_model_validate_canonical_json(
        RuntimeCodeAttestation,
        package.attestation_bytes,
    )
    if attestation.execution_spec != execution_spec:
        raise RuntimeError("real generation signed execution spec changed")
    provenance_probe = probe_installed_generation_provenance(
        generation_root=generation_root,
        execution_spec=attestation.execution_spec,
        attested_files=attestation.files,
        environment=child_environment,
    )
    return RealFormalSmokeGeneration(
        package=package,
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        generation_root=generation_root,
        child_environment=child_environment,
        formal_data=formal_data,
        code_trust_evidence=evidence,
        import_roots=import_roots,
        provenance_probe=provenance_probe,
    )


class _PromotionReceiptHandler:
    def response_json(
        self,
        _request: object,
        state: ExternalRootStoredState | None,
    ) -> str | None:
        return None if state is None else state.checkpoint_json


class _PromotionProbeSigner:
    signature_algorithm = "ed25519"

    def __init__(self, signer: object) -> None:
        self.issuer = signer.issuer
        self.key_id = signer.key_id
        self.key_purpose = signer.key_purpose
        self.public_key_fingerprint = signer.public_key_fingerprint
        self._signer = signer

    def sign(self, *, namespace: str, payload: bytes) -> str:
        return self._signer.sign(namespace=namespace, payload=payload)


@contextmanager
def real_promotion_authority(
    root: Path,
    *,
    generation: RealFormalSmokeGeneration,
) -> Iterator[Path]:
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    receipt = generation.package.receipt
    short_parent = Path(__file__).resolve().parents[1] / ".s"
    short_parent.mkdir(mode=0o700, exist_ok=True)
    short_parent.chmod(0o700)
    short_root = short_parent / hashlib.sha256(os.fspath(root).encode()).hexdigest()[:12]
    short_root.mkdir(mode=0o700)
    socket_path = short_root / "p.sock"
    transport = UnixSocketExternalMonotonicRootManifest(
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
        rollback_domain_id=receipt.rollback_domain_id,
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        socket_mode=0o600,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=2_000,
        max_response_bytes=1024 * 1024,
    )
    service_configuration = ExternalRootServiceConfiguration(
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        service_uid=os.getuid(),
        service_gid=os.getgid(),
        allowed_peer_uid=os.getuid(),
        allowed_peer_gid=os.getgid(),
        socket_mode=0o600,
        socket_directory_mode=0o700,
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
        rollback_domain_id=receipt.rollback_domain_id,
        transport_manifest_hash=transport.manifest_hash,
    )
    backend = PersistentExternalMonotonicRootBackend(
        root / "promotion.sqlite3",
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
    )
    checkpoint = receipt.model_dump(mode="json")
    backend.apply(
        ExternalMonotonicRootRequest.close(
            kind="pin",
            role=receipt.role,
            root_authority_id=receipt.root_authority_id,
            root_store_id=receipt.root_store_id,
            subject_authority_id="installation-a-test-platform",
            challenge_nonce="9" * 64,
            operation_id="8" * 64,
            previous_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
            checkpoint_contract=receipt.contract,
            checkpoint_hash=canonical_sha256(checkpoint),
            checkpoint_json=generation.package.receipt_bytes.decode("utf-8"),
        )
    )
    service = ExternalMonotonicRootUnixService(
        configuration=service_configuration,
        backend=backend,
        handler=_PromotionReceiptHandler(),
        probe_signer=_PromotionProbeSigner(generation.package.authorities[6]),
    )
    stop = threading.Event()
    thread = threading.Thread(
        target=service.serve_forever,
        kwargs={"stop": stop},
        daemon=True,
    )
    thread.start()
    if not service.ready.wait(timeout=5):
        raise RuntimeError("real promotion authority did not become ready")
    promotion_config = ExternalMonotonicRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=transport.manifest_hash,
        role=receipt.role,
        root_authority_id=receipt.root_authority_id,
        root_store_id=receipt.root_store_id,
        root_issuer=receipt.issuer,
        root_key_id=receipt.key_id,
        root_key_purpose=receipt.key_purpose,
        root_receipt_namespace=receipt.namespace,
        root_public_key_fingerprint=receipt.public_key_fingerprint,
        witness_rollback_domain_id=receipt.rollback_domain_id,
        local_rollback_domain_id="local-runtime-code-domain",
    )
    configuration = FormalRuntimeBootstrapConfiguration(
        runtime_root=generation.runtime_root,
        trusted_base=generation.trusted_base,
        expected_material_uid=os.getuid(),
        expected_material_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        expected_python_abi=sys.implementation.cache_tag or "invalid",
        root_keys=(generation.package.authorities[1],),
        runtime_keys=(generation.package.authorities[4],),
        promotion_key=generation.package.authorities[7],
        promotion_config=promotion_config,
        promotion_transport=transport,
        promotion_subject_authority_id="installation-a-test-platform",
    )
    configuration_name = f"runtime-code-bootstrap-{transport.manifest_hash[:16]}.json"
    configuration_path = generation.trusted_base / configuration_name
    configuration_path.write_bytes(canonical_model_json_bytes(configuration))
    configuration_path.chmod(0o444)
    try:
        yield configuration_path
    finally:
        stop.set()
        service.wake()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("real promotion authority did not stop")
        short_root.rmdir()


def invoke_outer_formal_smoke_cli_from_checkout_b(
    *,
    bootstrap_config: Path,
    trusted_base: Path,
    output: Path,
    formal_input: FormalSmokeInput,
    child_environment: Mapping[str, str],
    timeout_seconds: float,
) -> CliInvocation:
    from rquant.cli import main

    arguments = [
        "rquant",
        "formal-smoke-replay",
        "--strategy",
        formal_input.strategy,
        "--start-date",
        formal_input.start_date.isoformat(),
        "--end-date",
        formal_input.end_date.isoformat(),
        "--audit-run-id",
        formal_input.audit_run_id,
        "--snapshot-id",
        formal_input.dataset_snapshot_id,
        "--binding-hash",
        formal_input.dataset_binding_hash,
        "--output-dir",
        os.fspath(output),
        "--execution-timeout-seconds",
        str(timeout_seconds),
        "--runtime-code-config",
        os.fspath(bootstrap_config),
        "--runtime-code-trusted-base",
        os.fspath(trusted_base),
        "--runtime-code-authority-uid",
        str(os.getuid()),
        "--runtime-code-authority-gid",
        str(os.getgid()),
    ]
    old_argv = sys.argv
    old_environment = {name: os.environ.get(name) for name in child_environment}
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        sys.argv = arguments
        os.environ.update(child_environment)
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main()
    finally:
        sys.argv = old_argv
        for name, value in old_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    return CliInvocation(
        exit_code=exit_code,
        stdout=stdout.getvalue().strip(),
        stderr=stderr.getvalue(),
    )


def write_redacted_exact_facts_if_requested(
    *,
    generation: RealFormalSmokeGeneration,
    receipt: FormalSmokeExecutionReceipt,
    receipt_digest: str,
) -> None:
    raw_path = os.getenv("RQUANT_FORMAL_SMOKE_EXACT_FACTS_PATH", "").strip()
    if not raw_path:
        return
    path = Path(raw_path)
    if not path.is_absolute() or path.parent.resolve(strict=True) != path.parent:
        raise RuntimeError("real generation facts path is not canonical absolute")
    artifact_digests = {artifact.kind: artifact.sha256 for artifact in receipt.artifacts}
    facts = RedactedExactFacts(
        python_version=(
            f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        ),
        generation_id=generation.code_trust_evidence.generation_id,
        content_root_sha256=generation.code_trust_evidence.content_root_sha256,
        receipt_digest=receipt_digest,
        artifact_digests=RedactedArtifactDigests.model_validate(artifact_digests),
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = canonical_model_json_bytes(facts)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise RuntimeError("real generation facts write failed")
            view = view[written:]
    finally:
        os.close(descriptor)


def verify_exact_junit(path: Path) -> None:
    root = ElementTree.parse(path).getroot()
    if root.tag not in {"testsuite", "testsuites"}:
        raise ValueError("real generation JUnit has no test suite")
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    if not suites or any(suite.tag != "testsuite" for suite in suites):
        raise ValueError("real generation JUnit has no test suite")
    if any(suite.findall(".//testsuite") for suite in suites):
        raise ValueError("real generation JUnit contains nested suites")
    cases = [case for suite in suites for case in suite.findall("testcase")]
    if len(root.findall(".//testcase")) != len(cases):
        raise ValueError("real generation JUnit testcase nesting is invalid")

    totals = {field: 0 for field in ("tests", "failures", "errors", "skipped", "deselected")}
    for suite in suites:
        suite_cases = suite.findall("testcase")
        observed = {
            "tests": len(suite_cases),
            "failures": sum(bool(case.findall("failure")) for case in suite_cases),
            "errors": sum(bool(case.findall("error")) for case in suite_cases),
            "skipped": sum(bool(case.findall("skipped")) for case in suite_cases),
            "deselected": 0,
        }
        for field, value in observed.items():
            try:
                declared = int(suite.attrib.get(field, "0"))
            except ValueError as exc:
                raise ValueError("real generation JUnit summary is invalid") from exc
            if declared < 0 or declared != value:
                raise ValueError(f"real generation JUnit {field} summary mismatch")
            totals[field] += value

    if root.tag == "testsuites":
        for field, value in totals.items():
            if field not in root.attrib:
                continue
            try:
                declared = int(root.attrib[field])
            except ValueError as exc:
                raise ValueError("real generation JUnit root summary is invalid") from exc
            if declared < 0 or declared != value:
                raise ValueError(f"real generation JUnit root {field} summary mismatch")

    if totals["tests"] != _EXACT_CASE_COUNT or len(cases) != _EXACT_CASE_COUNT:
        raise ValueError("real generation JUnit must contain exactly 2 cases")
    for outcome, field in (
        ("failure", "failures"),
        ("error", "errors"),
        ("skipped", "skipped"),
    ):
        if totals[field]:
            raise ValueError(f"real generation JUnit contains {outcome}")
    if totals["deselected"]:
        raise ValueError("real generation JUnit contains deselected")
    names = tuple(case.attrib.get("name", "") for case in cases)
    if len(set(names)) != _EXACT_CASE_COUNT or frozenset(names) != _EXACT_TEST_NAMES:
        raise ValueError("real generation JUnit testcase set is invalid")


class ExactNodeTimeoutError(TimeoutError):
    pass


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "exact_timeout(seconds): fail a complete exact test protocol after a wall-clock bound",
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(
    item: pytest.Item,
    nextitem: pytest.Item | None,
) -> Iterator[None]:
    del nextitem
    marker = item.get_closest_marker("exact_timeout")
    if marker is None:
        yield
        return
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("exact node timeout requires the main thread")
    seconds = float(marker.args[0]) if marker.args else 0.0
    if not 0 < seconds <= 180:
        raise RuntimeError("exact node timeout must be in (0, 180]")
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    if previous_timer != (0.0, 0.0):
        raise RuntimeError("exact node timeout cannot replace an active timer")
    previous_handler = signal.getsignal(signal.SIGALRM)

    def expire(_signum: int, _frame: object) -> None:
        raise ExactNodeTimeoutError(f"exact node exceeded {seconds:g} seconds")

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def verify_ci_evidence(
    *,
    junit: Path,
    facts_path: Path,
    expected_python: str,
) -> None:
    verify_exact_junit(junit)
    facts = strict_model_validate_canonical_json(
        RedactedExactFacts,
        facts_path.read_bytes(),
    )
    if not facts.python_version.startswith(expected_python + "."):
        raise ValueError("real generation facts Python version mismatch")


def _main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("verify-ci-evidence")
    verify.add_argument("--junit", type=Path, required=True)
    verify.add_argument("--facts", type=Path, required=True)
    verify.add_argument("--expected-python", required=True)
    args = parser.parse_args()
    verify_ci_evidence(
        junit=args.junit,
        facts_path=args.facts,
        expected_python=args.expected_python,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
