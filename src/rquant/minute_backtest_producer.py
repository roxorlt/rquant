"""Trusted publication, complete independent receipts and the original snapshot gate."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Literal, Self

import duckdb
import pyarrow.parquet as parquet
from pydantic import Field, model_validator

from rquant.data_metadata import (
    DataAuditRun, DataAuditRunFinalization, DatasetCoverage, DatasetSnapshot,
    DatasetSnapshotBinding, DatasetSnapshotFinalization,
)
from rquant.live_contracts import BatchEnvelope, CurrentPointer
from rquant.live_spool import _SpoolCompletionReceipt
from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, MAX_WORK_UNITS, MinuteReplayModel, MinuteReplayWork, MinuteRuntimeSourceReceipt, Sha256
from rquant.minute_backtest_publication_contracts import (
    MAX_MINUTE_CONTROL_BYTES, MINUTE_AUDIT_RULE, MINUTE_FORMAL_CONTRACT, MINUTE_FORMAL_TABLE,
    FrozenMinuteResearchInput, MinuteDerivation, MinuteFormalWork, MinuteOriginMaterial,
    MinuteProvenance, MinuteSourceContentSeed, MinuteVisibilityPolicy,
)
from rquant.minute_backtest_source import restore_minute_runtime_source
from rquant.paper_execution_constraints import PaperExecutionConstraintPointer
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import ResearchGateDecision, ResearchGateRequest
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.research_snapshot import ResearchExecutionSession, build_dataset_snapshot_binding
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshot
from rquant.strict_json import strict_json_loads


def _reject_constant(value: str) -> object:
    raise ValueError("minute JSON contains a non-finite number: " + value)


def _strict_json(data: bytes | str) -> object:
    try:
        return strict_json_loads(data, parse_constant=_reject_constant)
    except (RecursionError, UnicodeError) as exc:
        raise ValueError("minute JSON is invalid or exceeds the parser nesting limit") from exc


def _payload(value: MinuteReplayModel) -> bytes:
    return value.model_dump_json(exclude_computed_fields=True).encode("utf-8")


def _discard_private_tree(path: Path) -> None:
    if path.is_symlink() or path.lstat().st_uid != os.getuid():
        raise PermissionError("minute temporary tree ownership changed")
    for parent, dirs, _files in os.walk(path, followlinks=False):
        Path(parent).chmod(0o700)
        for name in dirs:
            child = Path(parent) / name
            info = child.lstat()
            if stat.S_ISDIR(info.st_mode):
                if info.st_uid != os.getuid():
                    raise PermissionError("minute temporary directory ownership changed")
                child.chmod(0o700)
    shutil.rmtree(path)


@contextmanager
def private_minute_workspace() -> Iterator[Path]:
    temporary_parent = Path(tempfile.gettempdir()).resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix="minute-formal-", dir=temporary_parent))
    root.chmod(0o700)
    try:
        yield root
    finally:
        _discard_private_tree(root)


def _origin_rows(item: MinuteOriginMaterial) -> int:
    data = item.payload()
    observed_format = ("sqlite" if data.startswith(b"SQLite format 3\x00") else
        "parquet" if data.startswith(b"PAR1") else None)
    if observed_format is None and item.format == "bytes":
        try:
            structured = _strict_json(data)
        except ValueError:
            structured = None
        if isinstance(structured, (dict, list)):
            observed_format = "json"
    if observed_format is not None and observed_format != item.format:
        raise PermissionError("minute original format cannot hide physical dataset rows")
    if item.format == "parquet":
        rows = parquet.ParquetFile(BytesIO(data)).metadata.num_rows
    elif item.format == "json":
        value = _strict_json(data)
        if isinstance(value, list):
            rows = len(value)
        elif isinstance(value, dict):
            containers = [value.get(key) for key in ("rows", "records") if isinstance(value.get(key), list)]
            rows = sum(len(x) for x in containers) if containers else 1
        else:
            raise ValueError("minute original JSON must contain an object or array")
    elif item.format == "sqlite":
        with private_minute_workspace() as root:
            path = root / "origin.sqlite3"
            path.write_bytes(data)
            path.chmod(0o600)
            with sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True) as connection:
                names = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
                rows = 0
                for (name,) in names:
                    rows += connection.execute('SELECT COUNT(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
                    if rows > MAX_WORK_UNITS:
                        raise ValueError("minute original SQLite rows exceed work budget")
    else:
        rows = 1
    if rows > MAX_WORK_UNITS:
        raise ValueError("minute original physical rows exceed work budget")
    return rows


def measure_minute_formal_work(
    runtime_work: MinuteReplayWork, *, origins: tuple[MinuteOriginMaterial, ...],
    provenance: MinuteProvenance, derivations: tuple[MinuteDerivation, ...],
) -> MinuteFormalWork:
    rows = 0
    for item in origins:
        rows += _origin_rows(item)
        if rows > MAX_WORK_UNITS:
            raise ValueError("minute original rows exceed total work budget")
    records = (1 + len(origins) + len(provenance.capture_lineage) + len(provenance.publication_evidence)
        + len(provenance.code_files) + len(derivations) + int(provenance.visibility_policy is not None))
    return MinuteFormalWork(runtime_work=runtime_work, origin_physical_rows=rows, provenance_record_count=records)


def _minute_source_contract(value: MinuteReplayModel) -> tuple[str, str, str]:
    if value.contract == MINUTE_FORMAL_CONTRACT:
        return "minute_runtime_replay", MINUTE_FORMAL_CONTRACT, MINUTE_FORMAL_TABLE
    from rquant.minute_backtest_parameter_contracts import PARAMETER_RESEARCH_KEY, PARAMETER_SOURCE_CONTRACT, PARAMETER_SOURCE_TABLE

    if value.contract == PARAMETER_SOURCE_CONTRACT:
        return PARAMETER_RESEARCH_KEY, PARAMETER_SOURCE_CONTRACT, PARAMETER_SOURCE_TABLE
    raise PermissionError("minute source has no exact installed typed contract")


def _read_complete_minute_source(connection: duckdb.DuckDBPyConnection, value: MinuteReplayModel) -> MinuteReplayModel:
    if value.contract == MINUTE_FORMAL_CONTRACT:
        return read_minute_formal_input_table(connection)
    _minute_source_contract(value)
    from rquant.minute_backtest_parameter_source import read_minute_parameter_input_table

    return read_minute_parameter_input_table(connection)


def _write_complete_minute_source(connection: duckdb.DuckDBPyConnection, value: MinuteReplayModel) -> None:
    if value.contract == MINUTE_FORMAL_CONTRACT:
        write_minute_formal_input_table(connection, value)
        return
    _minute_source_contract(value)
    from rquant.minute_backtest_parameter_source import write_minute_parameter_input_table

    write_minute_parameter_input_table(connection, value)


def _verify_publications(value: FrozenMinuteResearchInput) -> None:
    material = {x.relative_path: x for x in value.runtime.materials}
    origins = {x.object_key: x for x in value.origin_materials}
    expected_paths = {key for key in material if (key.startswith("market/batches/") and key.endswith(".json"))
        or key.startswith("constraint-publications/") or key.startswith("candidates/generations/")}
    proofs = value.provenance.publication_evidence
    if {x.material_path for x in proofs} != expected_paths or len(proofs) != len(expected_paths):
        raise PermissionError("minute source lacks complete per-publication evidence")
    for proof in proofs:
        data = material[proof.material_path].payload()
        if value.provenance.source_kind == "captured" and data != origins[proof.origin_object_key].payload():
            raise PermissionError("captured publication changed original bytes")
        if proof.kind == "market":
            envelope = BatchEnvelope.model_validate_json(data)
            if (proof.sequence, proof.published_at) != (envelope.sequence, envelope.available_at):
                raise PermissionError("minute market publication time differs from exact envelope")
            if value.provenance.source_kind == "captured":
                pointer = CurrentPointer.model_validate_json(origins[proof.pointer_object_key].payload())
                receipt = _SpoolCompletionReceipt.model_validate_json(origins[proof.completion_receipt_object_key].payload())
                actual_pointer = CurrentPointer(channel=envelope.channel, source_generation_id=pointer.source_generation_id,
                    batch_id=envelope.batch_id, sequence=envelope.sequence, revision=envelope.revision,
                    content_sha256=envelope.content_sha256, quality_status=envelope.quality_status, published_at=envelope.available_at)
                if pointer != actual_pointer or (receipt.channel, receipt.sequence, receipt.source_generation_id,
                    receipt.pointer_identity_sha256, receipt.envelope_identity_sha256, receipt.batch_id,
                    receipt.revision, receipt.content_sha256, receipt.quality_status, receipt.producer_commit,
                    receipt.source_time, receipt.received_at, receipt.visible_at) != (
                    envelope.channel, envelope.sequence, pointer.source_generation_id, pointer.identity_sha256,
                    envelope.identity_sha256, envelope.batch_id, envelope.revision, envelope.content_sha256,
                    envelope.quality_status, envelope.producer_commit, envelope.source_time, envelope.received_at, envelope.available_at):
                    raise PermissionError("captured minute completion proof differs from original pointer/envelope")
                if not envelope.event_time_end <= envelope.source_time <= receipt.completed_at <= envelope.available_at:
                    raise PermissionError("captured minute completion proof has impossible times")
        elif proof.kind == "constraint":
            pointer = PaperExecutionConstraintPointer.model_validate_json(data)
            if (proof.sequence, proof.published_at) != (pointer.sequence, pointer.published_at):
                raise PermissionError("minute constraint publication evidence time differs")
        else:
            snapshot = StrategyCandidateSnapshot.model_validate_json(data)
            if (proof.sequence, proof.published_at) != (snapshot.sequence, snapshot.captured_at):
                raise PermissionError("minute candidate publication evidence time differs")
        if proof.published_at > value.provenance.extracted_at:
            raise PermissionError("minute publication evidence is after actual extraction")


def core_source_receipt(value: FrozenMinuteResearchInput) -> MinuteRuntimeSourceReceipt:
    native = value.runtime
    return MinuteRuntimeSourceReceipt(source_key=native.source_key, source_version=native.source_version,
        owner_id=native.owner_id, input_hash=native.input_hash, producer_commit=native.producer_commit,
        start_date=native.start_date, end_date=native.end_date, audit_run_id=native.audit_run_id,
        dataset_snapshot_id=native.dataset_snapshot_id, work=native.work, result_budget=native.result_budget,
        profile_hash=native.execution_profile.profile_hash, strategy_id=native.strategy.strategy_id,
        strategy_version=native.strategy.strategy_version)


def verify_minute_source_content(value: FrozenMinuteResearchInput, *, installed_policies: tuple[MinuteVisibilityPolicy, ...]) -> None:
    value = FrozenMinuteResearchInput.model_validate(value.model_dump(mode="python"))
    _verify_complete_minute_source_content(value, installed_policies=installed_policies)


def _verify_complete_minute_source_content(value: MinuteReplayModel, *, installed_policies: tuple[MinuteVisibilityPolicy, ...]) -> None:
    _minute_source_contract(value)
    if value.provenance.source_kind == "reconstructed" and value.provenance.visibility_policy not in installed_policies:
        raise PermissionError("minute reconstructed source policy is not installed by the trusted producer")
    actual = measure_minute_formal_work(value.runtime.work, origins=value.origin_materials,
        provenance=value.provenance, derivations=value.derivations)
    if (actual.runtime_work, actual.origin_physical_rows, actual.provenance_record_count) != (
            value.formal_work.runtime_work, value.formal_work.origin_physical_rows, value.formal_work.provenance_record_count):
        raise PermissionError("minute formal source understates physical work")
    _verify_publications(value)
    with private_minute_workspace() as root:
        if value.contract == MINUTE_FORMAL_CONTRACT:
            restored = restore_minute_runtime_source(value.runtime, expected=core_source_receipt(value), research_root=root / "validation")
        else:
            from rquant.minute_backtest_parameter_contracts import MinuteParameterRuntimeReceipt
            from rquant.minute_backtest_parameter_source import restore_minute_parameter_source

            restored = restore_minute_parameter_source(value.runtime,
                expected=MinuteParameterRuntimeReceipt(frozen=value.runtime), research_root=root / "validation")
        if restored.work != value.formal_work.runtime_work:
            raise PermissionError("minute native work differs from complete physical archive")


def read_minute_formal_input_table(connection: duckdb.DuckDBPyConnection) -> FrozenMinuteResearchInput:
    if connection.execute("SHOW TABLES").fetchall() != [(MINUTE_FORMAL_TABLE,)]:
        raise PermissionError("minute formal source requires exactly one input table")
    schema = connection.execute("PRAGMA table_info('minute_runtime_replay_input')").fetchall()
    if [(x[1], x[2], x[3], x[5]) for x in schema] != [("input_hash", "VARCHAR", True, True), ("payload", "VARCHAR", True, False)]:
        raise PermissionError("minute formal table lacks exact PK/NOT NULL schema")
    rows = connection.execute("SELECT input_hash, payload FROM minute_runtime_replay_input").fetchmany(2)
    if len(rows) != 1 or len(rows[0][1].encode("utf-8")) > MAX_INPUT_BYTES:
        raise PermissionError("minute formal source requires one bounded complete payload")
    _strict_json(rows[0][1])
    value = FrozenMinuteResearchInput.model_validate_json(rows[0][1])
    if rows[0][0] != value.full_input_hash:
        raise PermissionError("minute complete source hash differs from payload")
    return value


def write_minute_formal_input_table(connection: duckdb.DuckDBPyConnection, value: FrozenMinuteResearchInput) -> None:
    if connection.execute("SHOW TABLES").fetchall():
        raise PermissionError("minute publication requires an empty new source")
    connection.execute("CREATE TABLE minute_runtime_replay_input (input_hash VARCHAR PRIMARY KEY, payload VARCHAR NOT NULL)")
    connection.execute("INSERT INTO minute_runtime_replay_input VALUES (?, ?)", [value.full_input_hash, _payload(value).decode()])


def verify_minute_snapshot_source(
    connection: duckdb.DuckDBPyConnection, *, code_sha: str, start_date: date, end_date: date,
    full_input_hash: str, seed_hash: str, core_input_hash: str, as_of: datetime,
) -> None:
    value = read_minute_formal_input_table(connection)
    if (value.runtime.producer_commit, value.runtime.start_date, value.runtime.end_date, value.full_input_hash,
        value.source_content_seed.seed_hash, value.core_input_hash, value.provenance.published_at) != (
        code_sha, start_date, end_date, full_input_hash, seed_hash, core_input_hash, as_of):
        raise PermissionError("minute snapshot source differs from exact typed identity")


class MinutePublicationReceipt(MinuteReplayModel):
    seed: MinuteSourceContentSeed
    frozen: FrozenMinuteResearchInput
    audit: DataAuditRun
    snapshot: DatasetSnapshot
    binding: DatasetSnapshotBinding
    coverages: tuple[DatasetCoverage, ...]
    source_file_bytes: int = Field(ge=1, le=MAX_INPUT_BYTES)
    snapshot_artifact_bytes: int = Field(ge=1, le=MAX_INPUT_BYTES)

    @model_validator(mode="after")
    def exact_publication(self) -> Self:
        if self.seed != self.frozen.source_content_seed:
            raise ValueError("minute publication receipt seed differs from complete frozen payload")
        frozen = self.frozen
        expected_audit, expected_snapshot = minute_metadata_identities(self.seed)
        if (self.audit.audit_run_id, self.snapshot.snapshot_id, frozen.runtime.audit_run_id,
            frozen.runtime.dataset_snapshot_id) != (expected_audit.audit_run_id, expected_snapshot.snapshot_id,
                expected_audit.audit_run_id, expected_snapshot.snapshot_id):
            raise ValueError("minute publication receipt differs from original Metadata identities")
        if self.audit != expected_audit.finalize(DataAuditRunFinalization(p0_count=0, completed_at=self.seed.provenance.published_at)):
            raise ValueError("minute complete audit differs from source receipt")
        if self.snapshot.status != "ready" or self.snapshot.table_watermarks != minute_watermarks(frozen):
            raise ValueError("minute complete snapshot differs from source receipt")
        if self.snapshot != expected_snapshot.finalize(DatasetSnapshotFinalization(table_watermarks=minute_watermarks(frozen), completed_at=self.seed.provenance.published_at)):
            raise ValueError("minute complete snapshot time or identity changed")
        m = self.binding.manifest
        strategy_name, source_contract, table_name = _minute_source_contract(frozen)
        if (self.binding.status, self.binding.snapshot_id, m.snapshot_id, m.strategy_name, m.code_commit,
            m.as_of_time, m.start_date, m.end_date, m.dependency_contract_version, m.eligibility_resolution_hash) != (
            "ready", self.snapshot.snapshot_id, self.snapshot.snapshot_id, strategy_name, frozen.runtime.producer_commit,
            frozen.provenance.published_at, frozen.runtime.start_date, frozen.runtime.end_date, source_contract, None):
            raise ValueError("minute exact binding receipt differs")
        if len(m.artifacts) != 1 or (m.artifacts[0].artifact_type, m.artifacts[0].dataset_id, m.artifacts[0].table_name,
            m.artifacts[0].row_count, m.artifacts[0].primary_key) != ("materialized_table", table_name, table_name, 1, ("input_hash",)):
            raise ValueError("minute receipt requires its exact single artifact")
        if self.coverages != minute_coverages(frozen):
            raise ValueError("minute complete receipt coverage/work differs")
        return self


class MinutePrivateFileReference(MinuteReplayModel):
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size_bytes: int = Field(ge=1, le=MAX_INPUT_BYTES)
    mtime_ns: int = Field(ge=0)
    owner_uid: int = Field(ge=0)
    mode: Literal[384] = 0o600
    link_count: Literal[1] = 1
    content_sha256: Sha256

    @model_validator(mode="after")
    def normalized(self) -> Self:
        if not self.path.is_absolute() or self.path != Path(os.path.abspath(self.path)):
            raise ValueError("minute installed authority path is not absolute and normalized")
        return self


def _secure_private_bytes(path: Path, expected: MinutePrivateFileReference | None = None) -> tuple[bytes, MinutePrivateFileReference]:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PermissionError("minute receipt path is not a normalized installed path")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory = os.open(path.anchor, flags)
    fd = -1
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        parent = os.fstat(directory)
        if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) != 0o700:
            raise PermissionError("minute installed source parent must be private owned 0700")
        fd = os.open(path.name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or before.st_nlink != 1 or stat.S_IMODE(before.st_mode) != 0o600 or before.st_size > MAX_INPUT_BYTES:
            raise PermissionError("minute receipt/source is not an independent private owned bounded file")
        data = bytearray()
        while chunk := os.read(fd, min(65536, MAX_INPUT_BYTES + 1 - len(data))):
            data.extend(chunk)
            if len(data) > MAX_INPUT_BYTES:
                raise PermissionError("minute private authority exceeds bytes")
        after, named = os.fstat(fd), os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        attributes = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_uid", "st_mode", "st_nlink")
        if any(getattr(before, x) != getattr(after, x) or getattr(before, x) != getattr(named, x) for x in attributes):
            raise PermissionError("minute private authority changed during complete read")
        reference = MinutePrivateFileReference(path=path, device=before.st_dev, inode=before.st_ino,
            size_bytes=before.st_size, mtime_ns=before.st_mtime_ns, owner_uid=before.st_uid,
            content_sha256=hashlib.sha256(data).hexdigest())
        if expected is not None and reference != expected:
            raise PermissionError("minute installed file identity/hash differs")
        return bytes(data), reference
    finally:
        if fd >= 0:
            os.close(fd)
        os.close(directory)


class MinutePublicationReference(MinuteReplayModel):
    source_key: str
    source_version: int = Field(ge=1)
    owner_id: str
    receipt: MinutePrivateFileReference
    source: MinutePrivateFileReference

    def _receipt_model(self) -> type[MinutePublicationReceipt]:
        return MinutePublicationReceipt

    def load(self, *, installed_policies: tuple[MinuteVisibilityPolicy, ...]) -> MinutePublicationReceipt:
        data, _ = _secure_private_bytes(self.receipt.path, self.receipt)
        _strict_json(data)
        publication = self._receipt_model().model_validate_json(data)
        source_data, _ = _secure_private_bytes(self.source.path, self.source)
        if len(source_data) + len(data) + publication.snapshot_artifact_bytes > MAX_INPUT_BYTES:
            raise PermissionError("minute receipt/source/artifact actual storage exceeds input budget")
        if publication.source_file_bytes != len(source_data):
            raise PermissionError("minute receipt source byte accounting differs")
        runtime = publication.frozen.runtime
        if (self.source_key, self.source_version, self.owner_id) != (runtime.source_key, runtime.source_version, runtime.owner_id):
            raise PermissionError("minute complete receipt source key/version/owner differs")
        with private_minute_workspace() as root:
            path = root / "input.duckdb"
            path.write_bytes(source_data)
            path.chmod(0o600)
            with duckdb.connect(str(path), read_only=True) as connection:
                if _read_complete_minute_source(connection, publication.frozen) != publication.frozen:
                    raise PermissionError("minute independent full receipt differs from original source table")
        _verify_complete_minute_source_content(publication.frozen, installed_policies=installed_policies)
        _secure_private_bytes(self.receipt.path, self.receipt)
        _secure_private_bytes(self.source.path, self.source)
        return publication


class MinuteReplayCatalog(MinuteReplayModel):
    entries: tuple[MinutePublicationReference, ...] = Field(min_length=1, max_length=100)
    installed_policies: tuple[MinuteVisibilityPolicy, ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def closed_budget(self) -> Self:
        if len({(x.source_key, x.source_version, x.owner_id) for x in self.entries}) != len(self.entries):
            raise ValueError("minute installed source references repeat")
        if len({(x.policy_id, x.version) for x in self.installed_policies}) != len(self.installed_policies):
            raise ValueError("minute installed modeling policies repeat")
        if len(_payload(self)) > MAX_MINUTE_CONTROL_BYTES:
            raise ValueError("minute catalog exceeds the original 1 MiB control budget")
        return self

    def resolve(self, *, source_key: str, source_version: int, owner_id: str) -> MinutePublicationReceipt:
        entries = [x for x in self.entries if (x.source_key, x.source_version, x.owner_id) == (source_key, source_version, owner_id)]
        if len(entries) != 1:
            raise PermissionError("minute source has no exact installed independent receipt")
        return entries[0].load(installed_policies=self.installed_policies)


class PublishedMinuteInput(MinuteReplayModel):
    receipt: MinutePublicationReceipt
    reference: MinutePublicationReference
    identity: DatasetSnapshotIdentity
    gate_decision: ResearchGateDecision


def minute_metadata_identities(seed: MinuteSourceContentSeed) -> tuple[DataAuditRun, DatasetSnapshot]:
    now = seed.provenance.published_at
    strategy_name, _, _ = _minute_source_contract(seed)
    audit = DataAuditRun.create(as_of_date=now.date(), range_start=seed.runtime.start_date,
        range_end=seed.runtime.end_date, observed_at=now, rule_set_version=f"{MINUTE_AUDIT_RULE}:{seed.seed_hash}")
    snapshot = DatasetSnapshot.create(strategy_name=strategy_name, manifest_id=seed.seed_hash,
        as_of_time=now, code_commit=seed.runtime.producer_commit,
        origin="trusted-minute-producer/v2" if strategy_name == "minute_runtime_replay" else "trusted-minute-parameter-producer/v1", created_at=now)
    return audit, snapshot


def minute_watermarks(value: FrozenMinuteResearchInput) -> dict[str, str]:
    result = {"manifest_start_date": value.runtime.start_date.isoformat(), "manifest_end_date": value.runtime.end_date.isoformat(),
        "minute_seed_hash": value.source_content_seed.seed_hash, "minute_full_input_hash": value.full_input_hash,
        "minute_core_input_hash": value.core_input_hash, "minute_audit_id": value.runtime.audit_run_id,
        "minute_work_units": str(value.formal_work.work_units), "minute_origin_rows": str(value.formal_work.origin_physical_rows),
        "minute_provenance_records": str(value.formal_work.provenance_record_count), "minute_market_batches": str(value.runtime.work.market_batches),
        "minute_daily_observations": str(value.runtime.work.daily_observations)}
    if value.contract != MINUTE_FORMAL_CONTRACT:
        _minute_source_contract(value)
        result.update({"minute_parameter_hash": value.runtime.parameters.fingerprint,
            **{"minute_parameter_" + name: str(getattr(value.runtime.parameter_work, name))
                for name in ("prefix_rows", "history_rows", "derived_rows", "lifecycle_rows", "session_fact_rows")}})
    return result


def minute_coverages(value: FrozenMinuteResearchInput) -> tuple[DatasetCoverage, ...]:
    counts = {"input": 1, "work": value.formal_work.work_units, "origin_rows": value.formal_work.origin_physical_rows,
        "provenance_records": value.formal_work.provenance_record_count, "market_batches": value.runtime.work.market_batches,
        "daily_observations": value.runtime.work.daily_observations}
    _, _, table_name = _minute_source_contract(value)
    if value.contract != MINUTE_FORMAL_CONTRACT:
        counts.update({"parameter_" + name: getattr(value.runtime.parameter_work, name)
            for name in ("prefix_rows", "history_rows", "derived_rows", "lifecycle_rows", "session_fact_rows")})
    return tuple(DatasetCoverage(snapshot_id=value.runtime.dataset_snapshot_id, dataset_id=table_name,
        table_name=table_name, coverage_scope=scope, expected_count=count, available_count=count,
        created_at=value.provenance.published_at) for scope, count in sorted(counts.items()))


def verify_bound_minute_input(store: DuckDBStore, request: ResearchGateRequest,
    session: ResearchExecutionSession, expected: MinutePublicationReceipt) -> FrozenMinuteResearchInput:
    value = _read_complete_minute_source(session._conn, expected.frozen)
    if value != expected.frozen or value.source_content_seed != expected.seed:
        raise PermissionError("minute bound full source differs from complete independent publication")
    snapshot = store.get_dataset_snapshot(request.dataset_snapshot_id)
    audit = store.get_data_audit_run(request.audit_run_id)
    binding = store.get_dataset_snapshot_binding(request.dataset_snapshot_id)
    coverage = tuple(sorted(store.list_dataset_coverages(request.dataset_snapshot_id), key=lambda x: x.coverage_scope))
    if (snapshot, audit, binding, coverage) != (expected.snapshot, expected.audit, expected.binding, expected.coverages):
        raise PermissionError("minute full Metadata/binding/work differs from independent publication")
    if (request.mode, request.strategy_name, request.code_commit, request.start_date, request.end_date,
        request.audit_run_id, request.dataset_snapshot_id, request.dataset_binding_hash) != (
        "formal", _minute_source_contract(value)[0], value.runtime.producer_commit, value.runtime.start_date, value.runtime.end_date,
        value.runtime.audit_run_id, value.runtime.dataset_snapshot_id, expected.binding.binding_hash):
        raise PermissionError("minute formal gate request differs from complete source receipt")
    if store.list_open_data_quality_issues(severities=("P0",)):
        raise PermissionError("minute source gate has an unresolved original P0 issue")
    return value


def _allowed_decision(receipt: MinutePublicationReceipt) -> ResearchGateDecision:
    counts = {x.coverage_scope: (x.available_count, x.expected_count) for x in receipt.coverages}
    return ResearchGateDecision(allowed=True, research_status="comparable", audit_run_id=receipt.audit.audit_run_id,
        dataset_snapshot_id=receipt.snapshot.snapshot_id, dataset_binding_hash=receipt.binding.binding_hash,
        coverage_counts=counts, coverage_ratios={k: a / e if e else None for k, (a, e) in counts.items()}, failures=())


@contextmanager
def open_gated_minute_store(request: ResearchGateRequest, *, metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
    lake_root: Path, catalog: MinuteReplayCatalog, source_key: str, source_version: int, owner_id: str
) -> Iterator[tuple[ResearchExecutionSession, ResearchGateDecision]]:
    expected = catalog.resolve(source_key=source_key, source_version=source_version, owner_id=owner_id)
    with metadata_store_factory() as store:
        binding = store.get_dataset_snapshot_binding(request.dataset_snapshot_id)
        if binding != expected.binding:
            raise PermissionError("minute original snapshot binding changed before gate")
        with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
            verify_bound_minute_input(store, request, session, expected)
            after = catalog.resolve(source_key=source_key, source_version=source_version, owner_id=owner_id)
            if after != expected:
                raise PermissionError("minute complete source authority changed during gate")
            session._minute_gate_receipt = expected
            yield session, _allowed_decision(expected)


def _record_minute_publication(seed: MinuteSourceContentSeed, frozen: FrozenMinuteResearchInput,
    audit: DataAuditRun, snapshot: DatasetSnapshot, *, metadata_store: DuckDBStore, source_path: Path,
    catalog: ResearchCatalog, lake_root: Path, now: AwareUtcDatetime,
) -> MinutePublicationReceipt:
    with duckdb.connect(str(source_path), read_only=True) as connection:
        if _read_complete_minute_source(connection, frozen) != frozen:
            raise PermissionError("minute original source round trip differs")
        metadata_store.begin_data_audit_run(audit)
        metadata_store.begin_dataset_snapshot(snapshot)
        coverages = minute_coverages(frozen)
        for coverage in coverages:
            metadata_store.upsert_dataset_coverage(coverage)
        metadata_store.finalize_dataset_snapshot(snapshot.snapshot_id,
            DatasetSnapshotFinalization(table_watermarks=minute_watermarks(frozen), completed_at=now))
        binding = build_dataset_snapshot_binding(metadata_store=metadata_store, source_connection=connection,
            catalog=catalog, lake_root=lake_root, snapshot_id=snapshot.snapshot_id, start_date=frozen.runtime.start_date,
            end_date=frozen.runtime.end_date, now=lambda: now)
    metadata_store.finalize_data_audit_run(audit.audit_run_id, DataAuditRunFinalization(p0_count=0, completed_at=now))
    receipt_type = MinutePublicationReceipt
    if frozen.contract != MINUTE_FORMAL_CONTRACT:
        from rquant.minute_backtest_parameter_producer import MinuteParameterPublicationReceipt

        receipt_type = MinuteParameterPublicationReceipt
    return receipt_type(seed=seed, frozen=frozen, audit=metadata_store.get_data_audit_run(audit.audit_run_id),
        snapshot=metadata_store.get_dataset_snapshot(snapshot.snapshot_id), binding=binding, coverages=coverages,
        source_file_bytes=source_path.stat().st_size, snapshot_artifact_bytes=sum(x.file_size for x in binding.manifest.artifacts))


def publish_minute_input(seed: MinuteSourceContentSeed, *, metadata_store: DuckDBStore, source_path: Path,
    receipt_path: Path, catalog: ResearchCatalog, lake_root: Path,
    installed_policies: tuple[MinuteVisibilityPolicy, ...], now: AwareUtcDatetime,
) -> PublishedMinuteInput:
    seed = MinuteSourceContentSeed.model_validate(seed.model_dump(mode="python"))
    return _publish_complete_minute_input(seed, metadata_store=metadata_store, source_path=source_path,
        receipt_path=receipt_path, catalog=catalog, lake_root=lake_root, installed_policies=installed_policies, now=now)


def _publish_complete_minute_input(seed: MinuteReplayModel, *, metadata_store: DuckDBStore, source_path: Path,
    receipt_path: Path, catalog: ResearchCatalog, lake_root: Path,
    installed_policies: tuple[MinuteVisibilityPolicy, ...], now: AwareUtcDatetime,
) -> PublishedMinuteInput:
    strategy_name, _, _ = _minute_source_contract(seed)
    if now != seed.provenance.published_at:
        raise PermissionError("minute actual publication clock differs from complete source")
    audit, snapshot = minute_metadata_identities(seed)
    frozen = seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)
    if len(_payload(seed)) + len(_payload(frozen)) > MAX_INPUT_BYTES:
        raise PermissionError("minute complete publication receipt exceeds the original input budget")
    _verify_complete_minute_source_content(frozen, installed_policies=installed_policies)
    for path in (source_path, receipt_path):
        if path.exists() or path.is_symlink() or path.parent.is_symlink():
            raise PermissionError("minute publication requires a new installed private path")
        if not path.is_absolute() or path.parent.lstat().st_uid != os.getuid() or stat.S_IMODE(path.parent.lstat().st_mode) != 0o700:
            raise PermissionError("minute publication parent must be private owned 0700")
    # The original materializer determines compressed sizes. Measure its actual
    # complete receipt in a private stage before installing source or Metadata.
    with private_minute_workspace() as stage:
        staged_source = stage / "input.duckdb"
        with duckdb.connect(str(staged_source)) as connection:
            staged_source.chmod(0o600)
            _write_complete_minute_source(connection, frozen)
        with DuckDBStore(stage / "metadata.duckdb") as staging_metadata:
            staged = _record_minute_publication(seed, frozen, audit, snapshot, metadata_store=staging_metadata,
                source_path=staged_source, catalog=ResearchCatalog(stage / "catalog.duckdb"), lake_root=stage / "lake", now=now)
        data = _payload(staged)
        if len(data) + staged.source_file_bytes + staged.snapshot_artifact_bytes > MAX_INPUT_BYTES:
            raise PermissionError("minute complete receipt/source/artifact storage exceeds input budget")
        source_data, _ = _secure_private_bytes(staged_source)
        descriptor = os.open(source_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(source_data)
            stream.flush()
            os.fsync(stream.fileno())
        receipt = _record_minute_publication(seed, frozen, audit, snapshot, metadata_store=metadata_store,
            source_path=source_path, catalog=catalog, lake_root=lake_root, now=now)
        if receipt != staged or _payload(receipt) != data:
            raise PermissionError("minute actual publication differs from its complete private preflight")
    binding = receipt.binding
    descriptor = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    _, receipt_ref = _secure_private_bytes(receipt_path)
    _, source_ref = _secure_private_bytes(source_path)
    reference_type, catalog_type, published_type = MinutePublicationReference, MinuteReplayCatalog, PublishedMinuteInput
    if seed.contract != MINUTE_FORMAL_CONTRACT:
        from rquant.minute_backtest_parameter_producer import (
            MinuteParameterPublicationReference, MinuteParameterReplayCatalog, PublishedMinuteParameterInput,
        )

        reference_type, catalog_type, published_type = MinuteParameterPublicationReference, MinuteParameterReplayCatalog, PublishedMinuteParameterInput
    reference = reference_type(source_key=frozen.runtime.source_key, source_version=frozen.runtime.source_version,
        owner_id=frozen.runtime.owner_id, receipt=receipt_ref, source=source_ref)
    installed = catalog_type(entries=(reference,), installed_policies=installed_policies)
    request = ResearchGateRequest(mode="formal", strategy_name=strategy_name, start_date=frozen.runtime.start_date,
        end_date=frozen.runtime.end_date, code_commit=frozen.runtime.producer_commit, audit_run_id=audit.audit_run_id,
        dataset_snapshot_id=snapshot.snapshot_id, dataset_binding_hash=binding.binding_hash)
    with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
        verify_bound_minute_input(metadata_store, request, session, installed.resolve(source_key=reference.source_key,
            source_version=reference.source_version, owner_id=reference.owner_id))
    return published_type(receipt=receipt, reference=reference, identity=DatasetSnapshotIdentity(
        snapshot_id=snapshot.snapshot_id, binding_hash=binding.binding_hash, audit_run_id=audit.audit_run_id),
        gate_decision=_allowed_decision(receipt))
