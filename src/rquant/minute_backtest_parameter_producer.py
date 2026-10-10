"""Parameter-only envelopes over the original complete minute publisher and gate."""

from __future__ import annotations

import os
import hashlib
import inspect
import json
import stat
import sys
from dataclasses import dataclass, fields, is_dataclass, replace
from enum import Enum
from types import CodeType, FunctionType, MappingProxyType
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID
from pathlib import Path
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Literal, Self

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_contracts import _ParameterContentGuard, _ParameterReadUnitContentEntries

import duckdb
from pydantic import BaseModel, Field, SerializerFunctionWrapHandler, model_serializer, model_validator

from rquant.minute_backtest_parameter_contracts import (
    FrozenMinuteParameterResearchInput, MinuteParameterSourceSeed,
)
from rquant.minute_backtest_parameter_source import read_minute_parameter_input_table
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_producer import (
    MinutePublicationReceipt, MinutePublicationReference, MinuteReplayCatalog,
    PublishedMinuteInput, _publish_complete_minute_input,
    _verify_complete_minute_source_content,
    _secure_private_bytes, verify_bound_minute_input,
)
from rquant.minute_backtest_publication_contracts import MinuteVisibilityPolicy
from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, MAX_WORK_UNITS, Sha256
from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog, MetadataCatalogDescriptor
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import ResearchGateRequest
from rquant.research_snapshot import ResearchExecutionSession
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.storage.duckdb import DuckDBStore

from rquant.executable_dependencies import (
    ExecutableBinding, ExecutableDependencyError, ExecutableDependencyGuard,
    capture_executable_dependency_guard,
)
from rquant.minute_backtest_contracts import MinuteReplayModel
from rquant.strict_json import canonical_json_bytes
from rquant.data_metadata import DataQualityIssue


class MinuteParameterPublicationReceipt(MinutePublicationReceipt):
    contract: Literal["minute-parameter-publication-receipt/v1"] = "minute-parameter-publication-receipt/v1"
    seed: MinuteParameterSourceSeed
    frozen: FrozenMinuteParameterResearchInput


class MinuteParameterPublicationReference(MinutePublicationReference):
    contract: Literal["minute-parameter-publication-reference/v1"] = "minute-parameter-publication-reference/v1"

    def _receipt_model(self) -> type[MinuteParameterPublicationReceipt]:
        return MinuteParameterPublicationReceipt

    def load(self, *, installed_policies: tuple[MinuteVisibilityPolicy, ...]) -> MinuteParameterPublicationReceipt:
        return super().load(installed_policies=installed_policies)


class MinuteParameterFactIdentity(MinuteParameterPublicationReference):
    """Full baseline reference, not an unverified browser key or hash."""

    full_input_hash: Sha256


class MinuteParameterFactSourceReference(MinuteParameterPublicationReference):
    full_input_hash: Sha256
    metadata_identity: MetadataCatalogDescriptor
    display_name: str = Field(min_length=1, max_length=128)
    source_nature: Literal["real_retained", "historical_reconstruction", "synthetic_validation"]
    supported_parameter_names: tuple[str, ...] = Field(min_length=1, max_length=256)

    @property
    def fact_identity(self) -> MinuteParameterFactIdentity:
        return MinuteParameterFactIdentity(source_key=self.source_key, source_version=self.source_version,
            owner_id=self.owner_id, source=self.source, receipt=self.receipt, full_input_hash=self.full_input_hash)


class MinuteParameterPreparedPublication(MinutePublicationReference):
    contract: Literal["minute-parameter-prepared-publication/v1"] = "minute-parameter-prepared-publication/v1"
    baseline: MinuteParameterFactIdentity
    metadata_identity: MetadataCatalogDescriptor
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    parameter_hash: Sha256
    work_units: int = Field(ge=1, le=MAX_WORK_UNITS)
    loaded_bytes: int = Field(ge=1, le=MAX_INPUT_BYTES)
    study_binding: MinuteParameterStudyBinding | None = None

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study_binding is None:
            value.pop("study_binding", None)
        return value

    def _receipt_model(self) -> type[MinuteParameterPublicationReceipt]:
        return MinuteParameterPublicationReceipt

    @classmethod
    def from_published(cls, value: PublishedMinuteParameterInput, *,
        baseline_reference: MinuteParameterFactSourceReference,
        baseline_receipt: MinuteParameterPublicationReceipt,
        metadata_identity: MetadataCatalogDescriptor,
    ) -> MinuteParameterPreparedPublication:
        frozen = value.receipt.frozen
        if baseline_receipt.frozen.full_input_hash != baseline_reference.full_input_hash:
            raise PermissionError("parameter prepared baseline differs from its full reference")
        loaded_bytes = sum((value.reference.source.size_bytes, value.reference.receipt.size_bytes,
            metadata_identity.size_bytes, value.receipt.snapshot_artifact_bytes,
            baseline_reference.source.size_bytes, baseline_reference.receipt.size_bytes,
            baseline_reference.metadata_identity.size_bytes, baseline_receipt.snapshot_artifact_bytes))
        return cls(source_key=value.reference.source_key, source_version=value.reference.source_version,
            owner_id=value.reference.owner_id, source=value.reference.source, receipt=value.reference.receipt,
            baseline=baseline_reference.fact_identity, metadata_identity=metadata_identity,
            full_input_hash=frozen.full_input_hash, core_input_hash=frozen.core_input_hash,
            seed_hash=frozen.source_content_seed.seed_hash, parameter_hash=frozen.runtime.parameters.fingerprint,
            work_units=frozen.formal_work.work_units+baseline_receipt.frozen.formal_work.work_units,
            loaded_bytes=loaded_bytes, study_binding=frozen.runtime.study_binding)


class MinuteParameterReplayCatalog(MinuteReplayCatalog):
    contract: Literal["minute-parameter-replay-catalog/v1"] = "minute-parameter-replay-catalog/v1"
    entries: tuple[MinuteParameterPublicationReference, ...] = Field(default=(), max_length=100)
    fact_sources: tuple[MinuteParameterFactSourceReference, ...] = Field(default=(), max_length=100)
    prepared_root: Path | None = None
    snapshot_root: Path | None = None
    research_lake_root: Path | None = None
    forbidden_paths: tuple[Path, ...] = ()

    @model_validator(mode="after")
    def complete_fact_installation(self) -> Self:
        if not self.entries and not self.fact_sources:
            raise ValueError("parameter catalog needs a complete publication or installed fact source")
        keys = tuple((item.source_key, item.source_version, item.owner_id) for item in self.fact_sources)
        if len(set(keys)) != len(keys):
            raise ValueError("parameter installed full fact references repeat")
        if self.fact_sources and any(value is None for value in (self.prepared_root, self.snapshot_root, self.research_lake_root)):
            raise ValueError("parameter fact catalog requires its private producer and original snapshot roots")
        for path in (*self.forbidden_paths, self.prepared_root, self.snapshot_root, self.research_lake_root):
            if path is not None and (not path.is_absolute() or path != Path(os.path.abspath(path))):
                raise ValueError("parameter catalog private roots must be normalized absolute paths")
        return self

    def resolve(self, *, source_key: str, source_version: int, owner_id: str) -> MinuteParameterPublicationReceipt:
        return super().resolve(source_key=source_key, source_version=source_version, owner_id=owner_id)

    def resolve_fact(self, *, source_key: str, source_version: int, owner_id: str,
        full_input_hash: str) -> MinuteParameterPublicationReceipt:
        matches = [item for item in self.fact_sources if (item.source_key, item.source_version,
            item.owner_id, item.full_input_hash) == (source_key, source_version, owner_id, full_input_hash)]
        if len(matches) != 1:
            raise PermissionError("parameter baseline has no exact installed full fact authority")
        reference = matches[0]
        receipt = reference.load(installed_policies=self.installed_policies)
        if receipt.frozen.full_input_hash != full_input_hash:
            raise PermissionError("parameter installed baseline full input changed")
        with self._metadata_gate(reference.metadata_identity, receipt):
            pass
        return receipt

    @contextmanager
    def _metadata_gate(self, identity: MetadataCatalogDescriptor,
        expected: MinuteParameterPublicationReceipt) -> Iterator[ResearchExecutionSession]:
        if self.research_lake_root is None:
            raise PermissionError("parameter prepared gate lacks its original research lake root")
        _metadata_bytes, before = _secure_private_bytes(identity.source_path)
        with ImmutableDuckDBMetadataCatalog.open(identity.source_path,
            forbidden_paths=self.forbidden_paths, snapshot_root=self.snapshot_root) as catalog:
            if catalog.descriptor != identity:
                raise PermissionError("parameter complete Metadata physical identity changed")
            with DuckDBStore(catalog.snapshot_path, read_only=True) as metadata:
                with ResearchExecutionSession(binding=expected.binding, lake_root=self.research_lake_root) as session:
                    value = expected.frozen
                    request = ResearchGateRequest(mode="formal", strategy_name="minute_parameter_replay",
                        start_date=value.runtime.start_date, end_date=value.runtime.end_date,
                        code_commit=value.runtime.producer_commit, audit_run_id=expected.audit.audit_run_id,
                        dataset_snapshot_id=expected.snapshot.snapshot_id, dataset_binding_hash=expected.binding.binding_hash)
                    verify_bound_minute_input(metadata, request, session, expected)
                    session._minute_gate_receipt = expected
                    yield session
        _secure_private_bytes(identity.source_path, before)

    def resolve_prepared(self, prepared: MinuteParameterPreparedPublication) -> MinuteParameterPublicationReceipt:
        prepared = MinuteParameterPreparedPublication.model_validate(prepared.model_dump(mode="python"))
        if self.prepared_root is None:
            raise PermissionError("parameter dynamic publication needs its installed private producer root")
        parent = prepared.source.path.parent
        if (parent.parent != self.prepared_root or prepared.receipt.path.parent != parent
                or prepared.metadata_identity.source_path.parent != parent):
            raise PermissionError("parameter prepared reference is outside its installed producer root")
        baseline = self.resolve_fact(source_key=prepared.baseline.source_key,
            source_version=prepared.baseline.source_version, owner_id=prepared.baseline.owner_id,
            full_input_hash=prepared.baseline.full_input_hash)
        matches = [item for item in self.fact_sources if item.fact_identity == prepared.baseline]
        if len(matches) != 1:
            raise PermissionError("parameter baseline full physical reference differs from its installation")
        reference = matches[0]
        if reference.owner_id != prepared.owner_id:
            raise PermissionError("parameter prepared baseline owner differs")
        receipt = prepared.load(installed_policies=self.installed_policies)
        value = receipt.frozen
        if prepared.study_binding != value.runtime.study_binding:
            raise PermissionError("parameter prepared study differs from the complete independent source")
        if (prepared.full_input_hash, prepared.core_input_hash, prepared.seed_hash, prepared.parameter_hash) != (
            value.full_input_hash, value.core_input_hash, value.source_content_seed.seed_hash, value.runtime.parameters.fingerprint):
            raise PermissionError("parameter prepared complete identity or recipe changed")
        from rquant.minute_backtest_parameter_fact_sources import verify_minute_parameter_derivation

        verify_minute_parameter_derivation(value, baseline.frozen)
        expected_work = value.formal_work.work_units + baseline.frozen.formal_work.work_units
        loaded = sum((prepared.source.size_bytes, prepared.receipt.size_bytes,
            prepared.metadata_identity.size_bytes, receipt.snapshot_artifact_bytes,
            reference.source.size_bytes, reference.receipt.size_bytes,
            reference.metadata_identity.size_bytes, baseline.snapshot_artifact_bytes))
        if (prepared.work_units, prepared.loaded_bytes) != (expected_work, loaded):
            raise PermissionError("parameter prepared source and baseline work/loaded bytes differ")
        if expected_work > MAX_WORK_UNITS or loaded > MAX_INPUT_BYTES:
            raise PermissionError("parameter complete source and baseline exceed original admission budgets")
        with self._metadata_gate(prepared.metadata_identity, receipt):
            pass
        return receipt

    @contextmanager
    def open_prepared(self, prepared: MinuteParameterPreparedPublication) -> Iterator[ResearchExecutionSession]:
        expected = self.resolve_prepared(prepared)
        with self._metadata_gate(prepared.metadata_identity, expected) as session:
            yield session
        if self.resolve_prepared(prepared) != expected:
            raise PermissionError("parameter prepared full authority changed during original gate")


class PublishedMinuteParameterInput(PublishedMinuteInput):
    receipt: MinuteParameterPublicationReceipt
    reference: MinuteParameterPublicationReference


@dataclass(frozen=True, slots=True)
class _ResolvedReadFile:
    path: Path
    identity: tuple[int, ...]
    parents: tuple[tuple[str, tuple[int, ...]], ...]
    sha256: str


_MinuteStudyInputRole = Literal[
    "prepared_source", "prepared_receipt", "prepared_metadata",
    "prepared_manifest", "prepared_artifact", "baseline_source",
    "baseline_receipt", "baseline_metadata", "baseline_manifest", "baseline_artifact",
]


class _MinuteStudyInputPath(MinuteReplayModel):
    role: _MinuteStudyInputRole
    path: Path


class _MinuteStudyInputDirectoryEvidence(MinuteReplayModel):
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    owner_uid: int = Field(ge=0)
    mode: int = Field(ge=0)


class _MinuteStudyInputFileEvidence(MinuteReplayModel):
    role: _MinuteStudyInputRole
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    owner_uid: int = Field(ge=0)
    mode: int = Field(ge=0)
    link_count: int = Field(ge=1)
    size_bytes: int = Field(ge=1, le=MAX_INPUT_BYTES)
    mtime_ns: int
    ctime_ns: int
    content_sha256: Sha256
    parents: tuple[_MinuteStudyInputDirectoryEvidence, ...]


def _capture_minute_study_input_files(
    paths: tuple[_MinuteStudyInputPath, ...],
) -> tuple[_MinuteStudyInputFileEvidence, ...]:
    evidence = []
    for item in paths:
        if type(item) is not _MinuteStudyInputPath:
            raise TypeError("minute input files require their actual typed role/path inventory")
        original = _resolved_read_file(item.path)
        device, inode, uid, mode, links, size, mtime, ctime = original.identity
        evidence.append(_MinuteStudyInputFileEvidence(role=item.role, path=original.path,
            device=device, inode=inode, owner_uid=uid, mode=mode, link_count=links,
            size_bytes=size, mtime_ns=mtime, ctime_ns=ctime, content_sha256=original.sha256,
            parents=tuple(_MinuteStudyInputDirectoryEvidence(path=Path(path),
                device=identity[0], inode=identity[1], owner_uid=identity[2], mode=identity[3])
                for path, identity in original.parents)))
    return tuple(evidence)


def _verify_minute_study_input_files(
    paths: tuple[_MinuteStudyInputPath, ...], *,
    expected: tuple[_MinuteStudyInputFileEvidence, ...],
) -> None:
    if (any(type(item) is not _MinuteStudyInputFileEvidence for item in expected)
            or tuple((item.role, item.path) for item in paths)
            != tuple((item.role, item.path) for item in expected)):
        raise PermissionError("minute input proof differs from its complete role/path inventory")
    if _capture_minute_study_input_files(paths) != expected:
        raise PermissionError("minute input proof physical identity/bytes changed")


def _resolved_read_file(path: Path, *, expected: _ResolvedReadFile | None = None) -> _ResolvedReadFile:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PermissionError("minute resolved source path is not normalized")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open(path.anchor, directory_flags)
    opened: list[tuple[Path, int, os.stat_result]] = []
    current = Path(path.anchor)
    fd = -1
    directory_attributes = ("st_dev", "st_ino", "st_uid", "st_mode")
    attributes = (*directory_attributes, "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, directory_flags, dir_fd=directory)
            opened.append((current, directory, os.fstat(directory)))
            current = current / part
            directory = child
        opened.append((current, directory, os.fstat(directory)))
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or stat.S_IMODE(before.st_mode) & 0o022
                or not 0 < before.st_size <= MAX_INPUT_BYTES):
            raise PermissionError("minute resolved source is not an independent owned bounded file")
        digest = hashlib.sha256()
        count = 0
        while chunk := os.read(fd, 65_536):
            count += len(chunk)
            if count > MAX_INPUT_BYTES:
                raise PermissionError("minute resolved source bytes exceed original budget")
            digest.update(chunk)
        after, named = os.fstat(fd), os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if any(getattr(before, name) != getattr(after, name) or getattr(before, name) != getattr(named, name)
               for name in attributes) or count != before.st_size:
            raise PermissionError("minute resolved source changed during complete FD read")
        parents = []
        for name, handle, prior in opened:
            now, named_directory = os.fstat(handle), os.stat(name, follow_symlinks=False)
            if any(getattr(prior, attr) != getattr(now, attr) or getattr(prior, attr) != getattr(named_directory, attr)
                   for attr in directory_attributes):
                raise PermissionError("minute resolved source directory changed during read")
            parents.append((str(name), tuple(getattr(prior, attr) for attr in directory_attributes)))
        result = _ResolvedReadFile(path, tuple(getattr(before, attr) for attr in attributes), tuple(parents), digest.hexdigest())
        if expected is not None and result != expected:
            raise PermissionError("minute resolved source physical identity/bytes changed")
        return result
    finally:
        if fd >= 0:
            os.close(fd)
        closed = {handle for _, handle, _ in opened}
        for _, handle, _ in reversed(opened):
            os.close(handle)
        if directory not in closed:
            os.close(directory)


class _MinuteParameterResolvedContent(MinuteReplayModel):
    catalog: MinuteParameterReplayCatalog
    prepared: MinuteParameterPreparedPublication
    expected: MinuteParameterPublicationReceipt
    gate_request_policy: ResearchGateRequest | None = None
    quality_issue_policy: DataQualityIssue | None = None


def _resolved_read_content(value: object) -> str:
    references: dict[int, int] = {}
    nodes = 0

    def part(item: object, *, fields_set: bool = True) -> object:
        nonlocal nodes
        prior = nodes
        nodes = 0
        try:
            return visit(item, fields_set=fields_set)
        finally:
            nodes = prior

    def visit(item: object, depth: int = 0, *, fields_set: bool = True) -> object:
        nonlocal nodes
        nodes += 1
        if nodes > 8192 or depth > 64:
            raise ExecutableDependencyError("minute resolved content node/depth limit")
        if item is None or type(item) in (str, int, bool):
            return (type(item).__name__, item)
        if type(item) in (float, bytes):
            return (type(item).__name__, repr(item))
        if isinstance(item, Enum):
            return (type(item).__module__, type(item).__qualname__, item.name, visit(item.value, depth + 1, fields_set=fields_set))
        if type(item) in (date, datetime, time, timedelta, Decimal, UUID) or isinstance(item, Path):
            return (type(item).__module__, type(item).__qualname__, str(item))
        identity = id(item)
        if identity in references:
            return ("reference", references[identity])
        references[identity] = len(references)
        if isinstance(item, BaseModel):
            if type(item) is MinuteParameterPublicationReceipt:
                # These are the original separately validated complete source
                # units. Keep aliases across them and one total byte/retention
                # budget; never split an oversized seed or frozen input.
                body = tuple((visit(key, depth + 1),
                    part(nested) if key in ("seed", "frozen") else visit(nested, depth + 1))
                    for key, nested in item.__dict__.items())
                data = ("dict", body)
            else:
                data = visit(item.__dict__, depth + 1, fields_set=fields_set)
            return (type(item).__module__, type(item).__qualname__, data,
                visit(item.__pydantic_fields_set__, depth + 1) if fields_set else None,
                visit(item.__pydantic_extra__, depth + 1, fields_set=fields_set),
                visit(item.__pydantic_private__, depth + 1, fields_set=fields_set))
        if type(item) in (dict, MappingProxyType):
            return (type(item).__name__, tuple((visit(key, depth + 1, fields_set=fields_set), visit(nested, depth + 1, fields_set=fields_set)) for key, nested in item.items()))
        if type(item) in (tuple, list):
            return (type(item).__name__, tuple(visit(nested, depth + 1, fields_set=fields_set) for nested in item))
        if type(item) in (set, frozenset) and all(type(nested) is str for nested in item):
            return (type(item).__name__, tuple(sorted(item)))
        raise ExecutableDependencyError(f"minute resolved content contains an opaque live value: {type(item).__module__}.{type(item).__qualname__}")

    if type(value) is _MinuteParameterResolvedContent:
        # The original adapter revalidates controls and fills their fields_set.
        # Its complete field content/type/alias structure is the bound input;
        # the retained receipt also keeps its original private parse state.
        parts = []
        for item, fields_set in ((value.catalog, False), (value.prepared, False), (value.expected, True),
                (value.gate_request_policy, True), (value.quality_issue_policy, True)):
            parts.append(part(item, fields_set=fields_set))
        encoded = canonical_json_bytes(parts)
    else:
        encoded = canonical_json_bytes(visit(value))
    if len(encoded) > MAX_INPUT_BYTES:
        raise ExecutableDependencyError("minute resolved content byte limit")
    return hashlib.sha256(encoded).hexdigest()


def _resolved_read_functions() -> tuple[FunctionType, ...]:
    from rquant import minute_backtest_producer as producer
    from rquant import minute_backtest_parameter_fact_sources as facts
    from rquant import minute_backtest_parameter_source as source
    from rquant import research_snapshot as snapshots
    from rquant.storage import duckdb as storage
    from rquant.minute_backtest_parameter_study_projection import _ProjectionSemanticSnapshot
    from rquant.minute_backtest_parameter_contracts import (
        _ParameterReadUnitContentEntries, _parameter_read_unit_capacity,
        _parameter_read_unit_content_entries, _parameter_read_unit_retention,
    )
    from rquant.minute_backtest_parameter_definition import _minute_parameter_read_validation_entries

    return (
        MinuteParameterReplayCatalog.resolve_prepared, MinuteParameterReplayCatalog.resolve_fact,
        MinuteParameterReplayCatalog._metadata_gate, MinuteParameterPublicationReference.load,
        MinutePublicationReference.load, MinuteParameterPublicationReference._receipt_model,
        producer._secure_private_bytes, producer._read_complete_minute_source,
        producer._minute_source_contract, producer._verify_complete_minute_source_content,
        producer._verify_publications, producer.measure_minute_formal_work, producer.verify_bound_minute_input,
        facts.verify_minute_parameter_derivation, facts._candidate_facts,
        facts._parameter_execution_profile, facts._derived_profile,
        source.read_minute_parameter_input_table, source.restore_minute_parameter_source,
        source.measure_minute_parameter_work, source._measure_minute_parameter_work,
        source.parameter_archive_projections,
        ImmutableDuckDBMetadataCatalog.open.__func__, ImmutableDuckDBMetadataCatalog._identity,
        ImmutableDuckDBMetadataCatalog._reject_operational_alias,
        ImmutableDuckDBMetadataCatalog.__init__, ImmutableDuckDBMetadataCatalog.__enter__,
        ImmutableDuckDBMetadataCatalog.__exit__, ImmutableDuckDBMetadataCatalog.close,
        snapshots.ResearchExecutionSession.__init__, snapshots.ResearchExecutionSession._open_verified_views,
        snapshots.ResearchExecutionSession.__enter__, snapshots.ResearchExecutionSession.__exit__, snapshots.ResearchExecutionSession.close,
        snapshots.verify_materialized_table_artifact, snapshots._file_sha256, snapshots._logical_content_hash,
        DuckDBStore.__init__, DuckDBStore.__enter__, DuckDBStore.__exit__, DuckDBStore.close,
        DuckDBStore.get_dataset_snapshot, DuckDBStore.get_data_audit_run,
        DuckDBStore.get_dataset_snapshot_binding, DuckDBStore.list_dataset_coverages,
        DuckDBStore.list_open_data_quality_issues,
        storage._data_audit_run_from_row, storage._snapshot_from_row,
        storage._snapshot_binding_from_row, storage._coverage_from_row, storage._quality_issue_from_row,
        _ProjectionSemanticSnapshot.__init__, _ProjectionSemanticSnapshot.part, _ProjectionSemanticSnapshot.digest,
        _ProjectionSemanticSnapshot.value, _ProjectionSemanticSnapshot.mapping,
        _resolved_read_content, _resolved_read_file,
        _resolved_read_retained_bytes, _resolved_read_functions, _resolved_read_wrapper_policy,
        _resolved_read_unit_fee, _resolved_read_guard_budget, _resolved_minute_parameter_read_unit,
        _ParameterReadUnitContentEntries.__init__, _ParameterReadUnitContentEntries.release_new_content,
        _parameter_read_unit_capacity, _parameter_read_unit_content_entries, _parameter_read_unit_retention,
        _minute_parameter_read_validation_entries, _resolved_read_function_guard,
        MinuteParameterResolvedReadUnit.__init__, MinuteParameterResolvedReadUnit._check, MinuteParameterResolvedReadUnit.resolve,
        MinuteParameterResolvedReadUnit.close, resolved_minute_parameter_read_unit,
    )


def _resolved_read_wrapper_policy(function: FunctionType) -> str:
    from rquant.minute_backtest_parameter_study_projection import _ProjectionSemanticSnapshot

    snapshot = _ProjectionSemanticSnapshot()
    return snapshot.digest(snapshot.part(function, globals=False))


def _resolved_read_retained_bytes(value: object) -> int:
    seen: set[int] = set()

    def retained(item: object) -> int:
        if id(item) in seen:
            return 0
        seen.add(id(item))
        size = sys.getsizeof(item)
        if isinstance(item, BaseModel):
            nested = (item.__dict__, item.__pydantic_fields_set__, item.__pydantic_extra__, item.__pydantic_private__)
        elif is_dataclass(item) and not isinstance(item, type):
            nested = tuple(getattr(item, field.name) for field in fields(item))
        elif isinstance(item, dict) or type(item) is MappingProxyType:
            if type(item) is MappingProxyType:
                size += sys.getsizeof(dict(item))
            nested = tuple(child for pair in item.items() for child in pair)
        elif isinstance(item, (tuple, list, set, frozenset)):
            nested = tuple(item)
        elif isinstance(item, CodeType):
            nested = tuple(getattr(item, name) for name in ("co_code", "co_consts", "co_names", "co_varnames",
                "co_freevars", "co_cellvars", "co_filename", "co_name", "co_qualname", "co_linetable", "co_exceptiontable"))
        else:
            nested = ()
        return size + sum(retained(child) for child in nested)

    return retained(value)


class MinuteParameterResolvedReadUnit:
    """One charged trial lease; physical evidence and current policy are always read."""

    def __init__(self, *, content: _MinuteParameterResolvedContent, files: tuple[_ResolvedReadFile, ...],
        model_guard: object, code_guard: ExecutableDependencyGuard,
        functions: tuple[tuple[FunctionType, tuple[object, ...]], ...],
        wrappers: tuple[tuple[FunctionType, str], ...], retained_bytes: int) -> None:
        self._content = content
        self._files = files
        self._model_guard = model_guard
        self._code_guard = code_guard
        self._functions = functions
        self._wrappers = wrappers
        self._content_fingerprint = _resolved_read_content(content)
        self.retained_bytes = retained_bytes
        self._closed = False
        from rquant.minute_backtest_parameter_contracts import _PARAMETER_CONTENT_STATE
        self._scope_owner = _PARAMETER_CONTENT_STATE.get()

    def _check(self, catalog: MinuteParameterReplayCatalog, prepared: MinuteParameterPreparedPublication) -> None:
        from rquant.minute_backtest_parameter_contracts import _PARAMETER_CONTENT_STATE, _parameter_function_policy

        if self._closed or self._scope_owner is None or _PARAMETER_CONTENT_STATE.get() is not self._scope_owner:
            raise PermissionError("minute resolved trial unit is closed")
        self._code_guard.assert_unchanged()
        self._model_guard.assert_unchanged()
        if any(_parameter_function_policy(function, code_paths=self._model_guard.code_paths) != policy
                for function, policy in self._functions):
            raise PermissionError("minute resolved complete source live policy changed")
        if any(_resolved_read_wrapper_policy(function) != policy for function, policy in self._wrappers):
            raise PermissionError("minute resolved complete source wrapper changed")
        live = _MinuteParameterResolvedContent.model_construct(catalog=catalog, prepared=prepared, expected=self._content.expected)
        if type(catalog) is not MinuteParameterReplayCatalog or type(prepared) is not MinuteParameterPreparedPublication or _resolved_read_content(live) != self._content_fingerprint:
            raise PermissionError("minute resolved complete catalog/prepared/content changed")
        for original in self._files:
            _resolved_read_file(original.path, expected=original)
        for identity in (prepared.metadata_identity, *(item.metadata_identity for item in catalog.fact_sources
                if item.fact_identity == prepared.baseline)):
            ImmutableDuckDBMetadataCatalog._reject_operational_alias(
                os.stat(identity.source_path, follow_symlinks=False), catalog.forbidden_paths)

    def resolve(self, catalog: MinuteParameterReplayCatalog,
        prepared: MinuteParameterPreparedPublication) -> MinuteParameterPublicationReceipt:
        from rquant.minute_backtest_parameter_contracts import _copy_parameter_content

        self._check(catalog, prepared)
        result = _copy_parameter_content(self._content.expected)
        self._check(catalog, prepared)
        return result

    def close(self) -> None:
        self._closed = True
        self._content = None
        self._files = ()
        self._model_guard = None
        self._code_guard = None
        self._functions = ()
        self._wrappers = ()
        self._scope_owner = None


@contextmanager
def resolved_minute_parameter_read_unit(catalog: MinuteParameterReplayCatalog,
    prepared: MinuteParameterPreparedPublication) -> Iterator[MinuteParameterResolvedReadUnit | None]:
    from rquant.minute_backtest_parameter_definition import _minute_parameter_read_validation_entries

    with _minute_parameter_read_validation_entries() as content_entries:
        yield from _resolved_minute_parameter_read_unit(catalog, prepared, content_entries=content_entries)


def _resolved_read_unit_fee(parts: tuple[object, ...]) -> int:
    fee = _resolved_read_retained_bytes(parts)
    fee += sys.getsizeof(MinuteParameterResolvedReadUnit.__new__(MinuteParameterResolvedReadUnit))
    fee += sys.getsizeof(dict.fromkeys(("_content", "_files", "_model_guard", "_code_guard", "_functions",
        "_wrappers", "_content_fingerprint", "retained_bytes", "_closed", "_scope_owner")))
    return fee


def _resolved_read_guard_budget(guard: _ParameterContentGuard, *, retained_parts: tuple[object, ...], allowance: int) -> _ParameterContentGuard | None:
    from rquant.minute_backtest_parameter_contracts import _parameter_code_paths_retained_bytes

    raw = tuple(dependency.with_compiled_code_plan(max_retained_bytes=0) for dependency in guard.guards)
    policy_bytes = _parameter_code_paths_retained_bytes(guard.code_paths, guard.model_policy_probe)
    guard = replace(guard, guards=raw, code_paths_bytes=policy_bytes)
    mandatory_fee = _resolved_read_unit_fee((*retained_parts, guard))
    if mandatory_fee > allowance:
        return None
    remaining = allowance - mandatory_fee
    adopted = []
    for dependency in raw:
        current = dependency.with_compiled_code_plan(max_retained_bytes=remaining)
        remaining -= current.code_plan_retained_bytes
        policy_bytes += current.code_plan_retained_bytes
        adopted.append(current)
    return replace(guard, guards=tuple(adopted), code_paths_bytes=policy_bytes)


def _resolved_read_function_guard(
    guard: _ParameterContentGuard, *, functions: tuple[FunctionType, ...],
    retained_parts: tuple[object, ...], allowance: int,
) -> tuple[_ParameterContentGuard | None, tuple[tuple[FunctionType, tuple[object, ...]], ...]]:
    from rquant.minute_backtest_parameter_contracts import _parameter_code_paths, _parameter_function_policy

    paths = dict(guard.code_paths)
    paths.update(_parameter_code_paths((), functions))
    planned = replace(guard, code_paths=MappingProxyType(paths))
    policies = tuple((function, _parameter_function_policy(function, code_paths=planned.code_paths))
        for function in functions)
    complete_parts = (*retained_parts, policies)
    adopted = _resolved_read_guard_budget(planned, retained_parts=complete_parts, allowance=allowance)
    if adopted is None:
        adopted = _resolved_read_guard_budget(guard, retained_parts=complete_parts, allowance=allowance)
    return adopted, policies


def _resolved_minute_parameter_read_unit(catalog: MinuteParameterReplayCatalog,
    prepared: MinuteParameterPreparedPublication, *, content_entries: _ParameterReadUnitContentEntries | None) -> Iterator[MinuteParameterResolvedReadUnit | None]:
    from rquant.minute_backtest_parameter_contracts import (
        _copy_parameter_content, _parameter_content_guard, _parameter_function_policy,
        _parameter_read_unit_capacity, _parameter_read_unit_retention,
    )
    from rquant.research_snapshot import DatasetSnapshotBinding

    allowance = _parameter_read_unit_capacity()
    if allowance is None or allowance <= 0:
        yield None
        return
    if type(catalog) is not MinuteParameterReplayCatalog or type(prepared) is not MinuteParameterPreparedPublication:
        raise PermissionError("minute resolved unit requires its exact original installed models")
    unit = None
    try:
        receipt_bytes, _ = _secure_private_bytes(prepared.receipt.path, prepared.receipt)
        candidate = MinuteParameterPublicationReceipt.model_validate_json(receipt_bytes)
        matches = [item for item in catalog.fact_sources if item.fact_identity == prepared.baseline]
        if len(matches) != 1:
            raise PermissionError("minute resolved unit has no complete baseline reference")
        reference = matches[0]
        baseline_bytes, _ = _secure_private_bytes(reference.receipt.path, reference.receipt)
        # resolve_prepared below performs the full gate. These exact physical
        # bytes and bindings only let that finite verified unit be reused.
        baseline_binding = DatasetSnapshotBinding.model_validate(json.loads(baseline_bytes)["binding"])
        paths = [prepared.source.path, prepared.receipt.path, prepared.metadata_identity.source_path,
            reference.source.path, reference.receipt.path, reference.metadata_identity.source_path]
        if catalog.research_lake_root is None:
            raise PermissionError("minute resolved unit lacks its original lake")
        for binding in (candidate.binding, baseline_binding):
            paths.append(catalog.research_lake_root / binding.manifest_relative_path)
            paths.extend(catalog.research_lake_root / artifact.relative_path for artifact in binding.manifest.artifacts)
        original_files = tuple(_resolved_read_file(path) for path in dict.fromkeys(paths))
        content = _MinuteParameterResolvedContent.model_construct(catalog=_copy_parameter_content(catalog),
            prepared=_copy_parameter_content(prepared), expected=_copy_parameter_content(candidate))
        content_hash = _resolved_read_content(content)
        if content_entries is not None:
            content_entries.release_new_content()
        current_capacity = _parameter_read_unit_capacity()
        guard = _parameter_content_guard(content, output_model=_MinuteParameterResolvedContent,
            derive=_resolved_read_content, remaining_bytes=0 if current_capacity is None else current_capacity)
        if guard.model_policy_probe is None:
            yield None
            return
        functions = _resolved_read_functions()
        bindings = []
        wrappers = []
        for function in functions:
            binding = ExecutableBinding.from_callable(function)
            wrapped = inspect.getattr_static(function, "__wrapped__", None)
            if isinstance(wrapped, FunctionType):
                wrappers.append((function, _resolved_read_wrapper_policy(function)))
                binding = ExecutableBinding(owner_module=binding.owner_module,
                    binding_path=(*binding.binding_path, "__wrapped__"), implementation=wrapped)
            bindings.append(binding)
        code_guard = capture_executable_dependency_guard(tuple(bindings),
            contract="minute-parameter-resolved-read-unit/v1", include_global_dependencies=False)
        wrappers = tuple(wrappers)
        if content_entries is not None:
            content_entries.release_new_content()
        current_capacity = _parameter_read_unit_capacity()
        if current_capacity is None:
            yield None
            return
        retained_parts = (content, content_hash, original_files, code_guard, wrappers)
        guard, policies = _resolved_read_function_guard(guard, functions=functions,
            retained_parts=retained_parts, allowance=current_capacity)
        if guard is None:
            yield None
            return
        fee = _resolved_read_unit_fee((*retained_parts, policies, guard))
        if fee > current_capacity:
            yield None
            return
    except (ExecutableDependencyError, TypeError, ValueError):
        yield None
        return
    with _parameter_read_unit_retention(retained_bytes=fee) as retained:
        if not retained:
            yield None
            return
        try:
            actual = catalog.resolve_prepared(prepared)
            if actual != candidate:
                raise PermissionError("minute resolved trial differs from the original complete gate")
            unit = MinuteParameterResolvedReadUnit(content=content, files=original_files, model_guard=guard,
                code_guard=code_guard, functions=policies, wrappers=wrappers, retained_bytes=fee)
            unit._check(catalog, prepared)
            yield unit
            unit._check(catalog, prepared)
        finally:
            if unit is not None:
                unit.close()


def publish_minute_parameter_input(
    seed: MinuteParameterSourceSeed, *, metadata_store: DuckDBStore, source_path: Path,
    receipt_path: Path, catalog: ResearchCatalog, lake_root: Path,
    installed_policies: tuple[MinuteVisibilityPolicy, ...], now: AwareUtcDatetime,
) -> PublishedMinuteParameterInput:
    checked = MinuteParameterSourceSeed.model_validate(seed.model_dump(mode="python"))
    return _publish_complete_minute_input(checked, metadata_store=metadata_store,
        source_path=source_path, receipt_path=receipt_path, catalog=catalog,
        lake_root=lake_root, installed_policies=installed_policies, now=now)


def verify_minute_parameter_source_content(
    value: FrozenMinuteParameterResearchInput, *, installed_policies: tuple[MinuteVisibilityPolicy, ...],
) -> None:
    checked = FrozenMinuteParameterResearchInput.model_validate(value.model_dump(mode="python"))
    _verify_complete_minute_source_content(checked, installed_policies=installed_policies)


def verify_minute_parameter_snapshot_source(
    connection: duckdb.DuckDBPyConnection, *, code_sha: str, start_date: date,
    end_date: date, full_input_hash: str, seed_hash: str, core_input_hash: str,
    as_of: datetime,
) -> None:
    value = read_minute_parameter_input_table(connection)
    if (value.runtime.producer_commit, value.runtime.start_date, value.runtime.end_date,
        value.full_input_hash, value.source_content_seed.seed_hash, value.core_input_hash,
        value.provenance.published_at) != (
        code_sha, start_date, end_date, full_input_hash, seed_hash, core_input_hash, as_of):
        raise PermissionError("parameter snapshot source differs from the complete typed identity")
