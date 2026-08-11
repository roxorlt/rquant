"""Typed adapters for restoring real rQuant production artifacts."""

from __future__ import annotations

import binascii
import fcntl
import hashlib
import json
import multiprocessing
import os
import shutil
import signal
import sqlite3
import stat
import struct
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from multiprocessing.connection import Connection
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Annotated, Literal, Protocol, Self

import duckdb
from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.data_contracts import research_dataset_contract, research_export_schema
from rquant.reference_data_registry import ReadonlyReferenceRegistry, ReferenceDataset
from rquant.research_lake import ResearchPartitionManifest, partition_version_relative_path
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.serving_contracts import ServingCurrentPointer, ServingGenerationManifest
from rquant.serving_publisher import ServingReader
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

_CHUNK_SIZE = 1024 * 1024
_MAX_JSON_BYTES = 16 * 1024 * 1024
_PRIVATE_DIR_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_IMMUTABLE_DIR_MODE = 0o500
_IMMUTABLE_FILE_MODE = 0o400
_RELATION_HASH_CONTRACT = "rquant-recovery-relation-stream-sha256/v1"
_SQLITE_BLOB_PREFIX = b'{"type":"blob","value":"'
_SQLITE_TEXT_PREFIX = b'{"type":"text","value":'
_SQLITE_INT_PREFIX = b'{"type":"int","value":'
_SQLITE_FLOAT_PREFIX = b'{"type":"float","value":'
_SQLITE_NULL_FRAGMENT = b'{"type":"null","value":null}'
_SQLITE_CELL_SUFFIX = b"}"
_SQLITE_KEYSET_BATCH = 1024
_SQLITE_BLOB_CHUNK_BYTES = 64 * 1024
_SQLITE_STORAGE_TYPES = frozenset({"null", "integer", "real", "text", "blob"})


class RealRecoveryIntegrityError(RuntimeError):
    """A real recovery contract, artifact, or publication failed closed."""


class RecoveryVerificationBudget(RuntimeContractModel):
    """Hard resource bounds shared by fast verification and full rehearsal."""

    max_artifacts: int = Field(default=4096, ge=1, le=100_000)
    max_total_bytes: int = Field(default=256 * 1024**3, ge=1)
    max_relation_rows: int = Field(default=100_000_000, ge=1)
    max_relation_bytes: int = Field(default=64 * 1024**3, ge=1)
    max_row_bytes: int = Field(default=8 * 1024**2, ge=1)
    max_json_bytes: int = Field(default=_MAX_JSON_BYTES, ge=1, le=_MAX_JSON_BYTES)
    duckdb_memory_bytes: int = Field(default=256 * 1024**2, ge=16 * 1024**2)
    duckdb_temp_bytes: int = Field(default=4 * 1024**3, ge=16 * 1024**2)
    deadline_seconds: float = Field(default=6 * 60 * 60, gt=0, le=7 * 24 * 60 * 60)


class _VerificationMeter:
    def __init__(
        self,
        budget: RecoveryVerificationBudget,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        deadline: float | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        self.budget = budget
        self._monotonic = monotonic
        self._deadline = (
            monotonic() + budget.deadline_seconds if deadline is None else float(deadline)
        )
        self._cancelled = cancelled or (lambda: False)
        self.artifact_bytes = 0
        self.relation_rows = 0
        self.relation_bytes = 0

    def check_deadline(self) -> None:
        if self._cancelled():
            raise RealRecoveryIntegrityError("recovery operation cancelled")
        if self._monotonic() > self._deadline:
            raise RealRecoveryIntegrityError("recovery verification deadline exceeded")

    def remaining_seconds(self) -> float:
        remaining = self._deadline - self._monotonic()
        if remaining <= 0:
            raise RealRecoveryIntegrityError("recovery verification deadline exceeded")
        return remaining

    def add_artifact_bytes(self, size: int) -> None:
        self.artifact_bytes += size
        if self.artifact_bytes > self.budget.max_total_bytes:
            raise RealRecoveryIntegrityError("recovery artifact byte total exceeds budget")
        self.check_deadline()

    def add_relation_row(self, payload: bytes) -> None:
        self.add_relation_row_size(len(payload))

    def add_relation_row_size(self, size: int) -> None:
        if size > self.budget.max_row_bytes:
            raise RealRecoveryIntegrityError("recovery relation row exceeds byte budget")
        self.relation_rows += 1
        self.relation_bytes += size
        if self.relation_rows > self.budget.max_relation_rows:
            raise RealRecoveryIntegrityError("recovery relation row total exceeds budget")
        if self.relation_bytes > self.budget.max_relation_bytes:
            raise RealRecoveryIntegrityError("recovery relation byte total exceeds budget")
        self.check_deadline()

    def require_relation_capacity(
        self,
        *,
        rows: int,
        bytes_count: int,
        largest_row_bytes: int,
    ) -> None:
        if largest_row_bytes > self.budget.max_row_bytes:
            raise RealRecoveryIntegrityError("recovery relation row exceeds byte budget")
        if self.relation_rows + rows > self.budget.max_relation_rows:
            raise RealRecoveryIntegrityError("recovery relation row total exceeds budget")
        if self.relation_bytes + bytes_count > self.budget.max_relation_bytes:
            raise RealRecoveryIntegrityError("recovery relation byte total exceeds budget")
        self.check_deadline()

    def check_json_bytes(self, payload: bytes) -> None:
        if len(payload) > self.budget.max_json_bytes:
            raise RealRecoveryIntegrityError("recovery JSON exceeds its byte budget")
        self.check_deadline()


@contextmanager
def _interrupt_on_deadline(seconds: float):
    """Interrupt a blocking verifier on POSIX main threads; callers still check after."""

    if threading.current_thread() is not threading.main_thread() or not hasattr(
        signal, "setitimer"
    ):
        raise RealRecoveryIntegrityError(
            "recovery fixed replay cannot enforce a continuous deadline"
        )
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)

    def expired(_signum: int, _frame: object) -> None:
        raise RealRecoveryIntegrityError("recovery fixed replay deadline exceeded")

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, max(0.001, seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)


def _fixed_replay_process_entry(
    sender: Connection,
    verifier: FixedReplayVerifier,
    target_root: Path,
    dataset_path: Path,
) -> None:
    try:
        receipts = verifier.verify(target_root=target_root, dataset_path=dataset_path)
        payload = {
            "status": "succeeded",
            "receipts": [item.model_dump(mode="json") for item in receipts],
        }
    except BaseException as exc:
        payload = {
            "status": "failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc) or type(exc).__name__,
        }
    try:
        sender.send_bytes(canonical_json_bytes(payload))
    finally:
        sender.close()


def _run_fixed_replay_in_subprocess(
    *,
    verifier: FixedReplayVerifier,
    target_root: Path,
    dataset_path: Path,
    seconds: float,
) -> tuple[FixedReplayReceipt, ...]:
    try:
        context = multiprocessing.get_context("spawn")
    except ValueError as exc:
        raise RealRecoveryIntegrityError(
            "recovery fixed replay cannot enforce a continuous deadline"
        ) from exc
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_fixed_replay_process_entry,
        args=(sender, verifier, target_root, dataset_path),
        daemon=True,
    )
    process.start()
    sender.close()
    try:
        if not receiver.poll(seconds):
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
            raise RealRecoveryIntegrityError("recovery fixed replay deadline exceeded")
        try:
            raw = receiver.recv_bytes(_MAX_JSON_BYTES)
        except (EOFError, OSError) as exc:
            raise RealRecoveryIntegrityError(
                "recovery fixed replay process returned invalid evidence"
            ) from exc
    finally:
        receiver.close()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
    if process.exitcode != 0:
        raise RealRecoveryIntegrityError("recovery fixed replay process failed")
    try:
        payload = strict_canonical_json_loads(raw)
        if not isinstance(payload, dict) or payload.get("status") not in {
            "succeeded",
            "failed",
        }:
            raise ValueError("invalid replay response")
        if payload["status"] == "failed":
            raise RealRecoveryIntegrityError(
                "recovery fixed replay failed: "
                f"{payload.get('error_type')}: {payload.get('error_message')}"
            )
        receipts = tuple(FixedReplayReceipt.model_validate(item) for item in payload["receipts"])
    except RealRecoveryIntegrityError:
        raise
    except Exception as exc:
        raise RealRecoveryIntegrityError(
            "recovery fixed replay process returned invalid evidence"
        ) from exc
    return receipts


def _run_fixed_replay_with_deadline(
    *,
    verifier: FixedReplayVerifier,
    target_root: Path,
    dataset_path: Path,
    meter: _VerificationMeter,
) -> tuple[FixedReplayReceipt, ...]:
    meter.check_deadline()
    remaining = meter.remaining_seconds()
    if threading.current_thread() is threading.main_thread() and hasattr(signal, "setitimer"):
        with _interrupt_on_deadline(remaining):
            receipts = verifier.verify(target_root=target_root, dataset_path=dataset_path)
    else:
        receipts = _run_fixed_replay_in_subprocess(
            verifier=verifier,
            target_root=target_root,
            dataset_path=dataset_path,
            seconds=remaining,
        )
    meter.check_deadline()
    return receipts


class RealRecoveryArtifactKind(StrEnum):
    PRODUCTION_DUCKDB = "production_duckdb"
    STATE_SQLITE = "state_sqlite"
    RESEARCH_CATALOG = "research_catalog"
    RESEARCH_CATALOG_READONLY = "research_catalog_readonly"
    RESEARCH_LAKE_MANIFEST = "research_lake_manifest"
    RESEARCH_LAKE_OBJECT = "research_lake_object"
    LAB_ARTIFACT_MANIFEST = "lab_artifact_manifest"
    LAB_ARTIFACT_OBJECT = "lab_artifact_object"
    SERVING_CURRENT = "serving_current"
    SERVING_MANIFEST = "serving_manifest"
    SERVING_DATABASE = "serving_database"
    REFERENCE_SLOW_SQLITE = "reference_slow_sqlite"


def _safe_relative_path(value: str) -> str:
    if "\\" in value:
        raise ValueError("artifact path must be canonical relative POSIX")
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or str(path) != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("artifact path must be canonical relative POSIX")
    return value


def _canonical_absolute_path(value: Path | str, *, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError(f"{label} must be an absolute canonical path")
    return path


class RealRecoveryArtifactSpec(RuntimeContractModel):
    logical_role: str = Field(min_length=1, max_length=256)
    kind: RealRecoveryArtifactKind
    source_path: str
    restore_path: str
    generation_id: str = Field(min_length=1, max_length=512)
    schema_version: str = Field(min_length=1, max_length=128)
    available_at: AwareUtcDatetime | None = None
    price_basis: Literal["raw_session", "forward_adjusted", "backward_adjusted"] | None = None
    relations: tuple[str, ...] = ()
    references: Mapping[str, str] = Field(default_factory=dict)

    @field_validator("source_path", "restore_path")
    @classmethod
    def validate_paths(cls, value: str) -> str:
        return _safe_relative_path(value)

    @field_validator("relations")
    @classmethod
    def canonicalize_relations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item for item in value) or len(value) != len(set(value)):
            raise ValueError("artifact relations must be unique nonempty names")
        return tuple(sorted(value))

    @field_validator("references")
    @classmethod
    def canonicalize_references(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not key or not role for key, role in value.items()):
            raise ValueError("artifact references must have nonempty keys and roles")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("references")
    def serialize_references(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)


class RelationEvidence(RuntimeContractModel):
    relation_name: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    schema_sha256: Sha256
    content_sha256: Sha256


class RealRecoveryArtifact(RuntimeContractModel):
    logical_role: str = Field(min_length=1, max_length=256)
    kind: RealRecoveryArtifactKind
    source_path: str
    restore_path: str
    size_bytes: int = Field(ge=0)
    sha256: Sha256
    generation_id: str = Field(min_length=1, max_length=512)
    schema_version: str = Field(min_length=1, max_length=128)
    available_at: AwareUtcDatetime | None = None
    price_basis: Literal["raw_session", "forward_adjusted", "backward_adjusted"] | None = None
    relations: tuple[RelationEvidence, ...] = ()
    references: Mapping[str, str] = Field(default_factory=dict)
    contract_identity: Mapping[str, JsonValue]
    contract_sha256: Sha256

    @field_validator("source_path", "restore_path")
    @classmethod
    def validate_paths(cls, value: str) -> str:
        return _safe_relative_path(value)

    @field_validator("relations")
    @classmethod
    def canonicalize_relations(
        cls,
        value: tuple[RelationEvidence, ...],
    ) -> tuple[RelationEvidence, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.relation_name))
        names = tuple(item.relation_name for item in ordered)
        if len(names) != len(set(names)):
            raise ValueError("artifact relation evidence must be unique")
        return ordered

    @field_validator("references")
    @classmethod
    def canonicalize_references(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("references")
    def serialize_references(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("contract_identity")
    @classmethod
    def canonicalize_contract_identity(
        cls,
        value: Mapping[str, JsonValue],
    ) -> Mapping[str, JsonValue]:
        normalized = strict_canonical_json_loads(canonical_json_bytes(dict(value)))
        if not isinstance(normalized, dict):
            raise ValueError("artifact contract identity must be an object")
        return MappingProxyType(dict(sorted(normalized.items())))

    @field_serializer("contract_identity")
    def serialize_contract_identity(
        self,
        value: Mapping[str, JsonValue],
    ) -> dict[str, JsonValue]:
        return dict(value)

    @model_validator(mode="after")
    def validate_contract_hash(self) -> Self:
        expected = canonical_sha256(self.contract_identity)
        if self.contract_sha256 != expected:
            raise ValueError("artifact contract_sha256 does not bind contract_identity")
        return self


class FixedReplayReceipt(RuntimeContractModel):
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"]
    replay_fingerprint: Sha256


REQUIRED_RECOVERY_ARTIFACT_KINDS = frozenset(
    {
        RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
        RealRecoveryArtifactKind.STATE_SQLITE,
        RealRecoveryArtifactKind.RESEARCH_CATALOG,
        RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
        RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST,
        RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT,
        RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST,
        RealRecoveryArtifactKind.LAB_ARTIFACT_OBJECT,
        RealRecoveryArtifactKind.SERVING_CURRENT,
        RealRecoveryArtifactKind.SERVING_MANIFEST,
        RealRecoveryArtifactKind.SERVING_DATABASE,
        RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
    }
)


def validate_complete_recovery_artifact_graph(
    artifacts: Sequence[RealRecoveryArtifact | RealRecoveryArtifactSpec],
    *,
    production_artifact_role: str,
    paper_ledger_artifact_role: str,
) -> None:
    """Validate the one canonical production recovery role graph."""

    roles = tuple(item.logical_role for item in artifacts)
    if len(roles) != len(set(roles)):
        raise ValueError("recovery target artifact roles must be unique")
    by_role = {item.logical_role: item for item in artifacts}
    unknown_references = {
        role for item in artifacts for role in item.references.values() if role not in by_role
    }
    if unknown_references:
        raise ValueError(
            f"recovery target role references are unknown: {sorted(unknown_references)}"
        )
    production = by_role.get(production_artifact_role)
    paper = by_role.get(paper_ledger_artifact_role)
    if production is None or production.kind is not RealRecoveryArtifactKind.PRODUCTION_DUCKDB:
        raise ValueError("recovery target production role is invalid")
    if paper is None or paper.kind is not RealRecoveryArtifactKind.STATE_SQLITE:
        raise ValueError("recovery target paper ledger role is invalid")

    by_kind = {
        kind: tuple(item for item in artifacts if item.kind is kind)
        for kind in REQUIRED_RECOVERY_ARTIFACT_KINDS
    }
    missing = REQUIRED_RECOVERY_ARTIFACT_KINDS - {item.kind for item in artifacts}
    state_roles = tuple(
        item.logical_role
        for item in artifacts
        if item.kind is RealRecoveryArtifactKind.STATE_SQLITE
    )
    if missing or len(state_roles) < 2 or paper_ledger_artifact_role not in state_roles:
        raise ValueError(
            "recovery target requires complete production role inventory with distinct "
            "paper ledger and runtime state SQLite: "
            f"missing={sorted(item.value for item in missing)}"
        )
    singleton_kinds = (
        RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
        RealRecoveryArtifactKind.RESEARCH_CATALOG,
        RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
        RealRecoveryArtifactKind.SERVING_CURRENT,
        RealRecoveryArtifactKind.SERVING_MANIFEST,
        RealRecoveryArtifactKind.SERVING_DATABASE,
        RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
    )
    if any(len(by_kind[kind]) != 1 for kind in singleton_kinds):
        raise ValueError("recovery target requires one complete singleton role inventory")

    def require_reference(
        artifact: RealRecoveryArtifact | RealRecoveryArtifactSpec,
        name: str,
        expected_kind: RealRecoveryArtifactKind,
    ) -> str:
        role = artifact.references.get(name)
        target = None if role is None else by_role.get(role)
        if target is None or target.kind is not expected_kind:
            raise ValueError(
                "recovery target required role reference is incomplete: "
                f"{artifact.logical_role}.{name}"
            )
        return role

    lake_manifests = by_kind[RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST]
    lake_objects = by_kind[RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT]
    for manifest in lake_manifests:
        require_reference(
            manifest,
            "parquet",
            RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT,
        )
    for item in lake_objects:
        require_reference(
            item,
            "manifest",
            RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST,
        )
    catalog = by_kind[RealRecoveryArtifactKind.RESEARCH_CATALOG][0]
    catalog_lake_roles = {
        role
        for role in catalog.references.values()
        if by_role[role].kind is RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST
    }
    if catalog_lake_roles != {item.logical_role for item in lake_manifests}:
        raise ValueError("recovery target research catalog/lake inventory is incomplete")
    readonly_catalog = by_kind[RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY][0]
    if (
        require_reference(
            readonly_catalog,
            "authority",
            RealRecoveryArtifactKind.RESEARCH_CATALOG,
        )
        != catalog.logical_role
    ):
        raise ValueError("recovery target readonly catalog authority differs")

    serving_pointer = by_kind[RealRecoveryArtifactKind.SERVING_CURRENT][0]
    serving_manifest = by_kind[RealRecoveryArtifactKind.SERVING_MANIFEST][0]
    serving_database = by_kind[RealRecoveryArtifactKind.SERVING_DATABASE][0]
    reference = by_kind[RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE][0]
    if (
        require_reference(
            serving_pointer,
            "manifest",
            RealRecoveryArtifactKind.SERVING_MANIFEST,
        )
        != serving_manifest.logical_role
        or require_reference(
            serving_manifest,
            "database",
            RealRecoveryArtifactKind.SERVING_DATABASE,
        )
        != serving_database.logical_role
        or require_reference(
            serving_manifest,
            "reference",
            RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
        )
        != reference.logical_role
        or require_reference(
            serving_database,
            "manifest",
            RealRecoveryArtifactKind.SERVING_MANIFEST,
        )
        != serving_manifest.logical_role
    ):
        raise ValueError("recovery target serving role inventory is incomplete")

    lab_manifests = by_kind[RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST]
    lab_objects = by_kind[RealRecoveryArtifactKind.LAB_ARTIFACT_OBJECT]
    referenced_lab_objects = {
        role
        for manifest in lab_manifests
        for name, role in manifest.references.items()
        if name.startswith("file:")
        and by_role[role].kind is RealRecoveryArtifactKind.LAB_ARTIFACT_OBJECT
    }
    if referenced_lab_objects != {item.logical_role for item in lab_objects}:
        raise ValueError("recovery target lab artifact inventory is incomplete")
    for item in lab_objects:
        require_reference(
            item,
            "manifest",
            RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST,
        )


class RealRecoveryTargetManifest(RuntimeContractModel):
    schema_version: Literal[2] = 2
    manifest_id: Sha256 | None = None
    target_commit: CommitSha
    target_profile_generation: Sha256
    as_of: AwareUtcDatetime
    production_artifact_role: str = Field(min_length=1, max_length=256)
    paper_ledger_artifact_role: str = Field(min_length=1, max_length=256)
    artifacts: tuple[RealRecoveryArtifact, ...] = Field(min_length=1, max_length=4096)
    external_attestations: Mapping[str, Sha256] = Field(default_factory=dict)

    @field_validator("external_attestations")
    @classmethod
    def canonicalize_external_attestations(
        cls,
        value: Mapping[str, Sha256],
    ) -> Mapping[str, Sha256]:
        if any(not key or len(key) > 128 for key in value):
            raise ValueError("external attestation names must be nonempty and bounded")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("external_attestations")
    def serialize_external_attestations(
        self,
        value: Mapping[str, Sha256],
    ) -> dict[str, Sha256]:
        return dict(value)

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        ordered = tuple(sorted(self.artifacts, key=lambda item: item.logical_role))
        roles = tuple(item.logical_role for item in ordered)
        paths = tuple(item.restore_path for item in ordered)
        if len(roles) != len(set(roles)) or len(paths) != len(set(paths)):
            raise ValueError("recovery artifact roles and restore paths must be unique")
        role_set = set(roles)
        for artifact in ordered:
            missing = set(artifact.references.values()) - role_set
            if missing:
                raise ValueError(
                    f"artifact {artifact.logical_role} references unknown roles: {sorted(missing)}"
                )
            if artifact.available_at is not None and artifact.available_at > self.as_of:
                raise ValueError("recovery artifact is not visible at target as_of")
        validate_complete_recovery_artifact_graph(
            ordered,
            production_artifact_role=self.production_artifact_role,
            paper_ledger_artifact_role=self.paper_ledger_artifact_role,
        )
        if "paper_ledger" not in self.external_attestations:
            raise ValueError("recovery target paper ledger external head is missing")
        object.__setattr__(self, "artifacts", ordered)
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"manifest_id"}))
        if self.manifest_id is not None and self.manifest_id != expected:
            raise ValueError("manifest_id does not bind recovery target")
        object.__setattr__(self, "manifest_id", expected)
        return self


class RecoveryToolVerifierBundle(RuntimeContractModel):
    schema_version: Literal[1] = 1
    verifier_id: Literal["rquant-real-recovery-verifier"] = "rquant-real-recovery-verifier"
    verifier_version: int = Field(default=1, ge=1)
    verifier_commit: CommitSha
    executable_fingerprint: Sha256
    target_manifest_id: Sha256
    target_profile_generation: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=128 * 1024)
    bundle_id: Sha256 | None = None

    def signing_payload(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", exclude={"signature", "bundle_id"})
        )

    @model_validator(mode="after")
    def validate_bundle_id(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"bundle_id"}))
        if self.bundle_id is not None and self.bundle_id != expected:
            raise ValueError("bundle_id does not bind recovery verifier bundle")
        object.__setattr__(self, "bundle_id", expected)
        return self


class RecoveryCurrentPointer(RuntimeContractModel):
    schema_version: Literal[1] = 1
    generation_id: Sha256
    generation_path: str
    target_commit: CommitSha
    target_profile_generation: Sha256
    previous_generation_id: Sha256 | None = None
    published_at: AwareUtcDatetime

    @field_validator("generation_path")
    @classmethod
    def validate_generation_path(cls, value: str) -> str:
        return _safe_relative_path(value)

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        if self.previous_generation_id == self.generation_id:
            raise ValueError("previous generation must differ from current generation")
        return self


class _RecoveryPublicationIntent(RuntimeContractModel):
    schema_version: Literal[1] = 1
    operation_id: str = Field(min_length=32, max_length=64)
    manifest_id: Sha256
    previous_pointer: RecoveryCurrentPointer | None = None
    created_at: AwareUtcDatetime


class RealRecoveryReceipt(RuntimeContractModel):
    schema_version: Literal[1] = 1
    receipt_id: Sha256 | None = None
    operation_id: str = Field(min_length=32, max_length=64)
    status: Literal["succeeded", "failed"]
    manifest_id: Sha256
    tool_bundle_id: Sha256
    target_commit: CommitSha
    target_profile_generation: Sha256
    previous_generation_id: Sha256 | None = None
    published_generation_id: Sha256 | None = None
    fixed_replays: tuple[FixedReplayReceipt, ...] = ()
    started_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    error_type: str | None = None
    error_message: str | None = None

    @model_validator(mode="after")
    def validate_receipt(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("recovery receipt completion precedes start")
        if self.status == "succeeded":
            if self.published_generation_id != self.manifest_id:
                raise ValueError("successful recovery must publish its manifest generation")
            if len(self.fixed_replays) != 3 or {
                item.strategy_id for item in self.fixed_replays
            } != {"n_shape", "growth_board_surge", "auction_gap"}:
                raise ValueError("successful recovery requires all fixed strategy replays")
            if self.error_type is not None or self.error_message is not None:
                raise ValueError("successful recovery cannot carry an error")
        elif (
            not self.error_type
            or not self.error_message
            or self.published_generation_id is not None
        ):
            raise ValueError("failed recovery requires an error and cannot publish")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is not None and self.receipt_id != expected:
            raise ValueError("receipt_id does not bind immutable recovery result")
        object.__setattr__(self, "receipt_id", expected)
        return self


class RecoveryPayloadSigner(Protocol):
    key_id: str

    def sign(self, payload: bytes) -> str: ...


class RecoveryPayloadVerifier(Protocol):
    key_id: str

    def verify(self, payload: bytes, signature: str) -> bool: ...


class FixedReplayVerifier(Protocol):
    fingerprint: str

    def verify(
        self,
        *,
        target_root: Path,
        dataset_path: Path,
    ) -> tuple[FixedReplayReceipt, ...]: ...


class _DigestWriter(Protocol):
    def update(self, payload: bytes) -> object: ...


def _hash_descriptor(
    descriptor: int,
    *,
    max_bytes: int | None = None,
    check: Callable[[], None] | None = None,
) -> tuple[int, str]:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    size = 0
    while True:
        if check is not None:
            check()
        chunk = os.read(descriptor, _CHUNK_SIZE)
        if not chunk:
            break
        size += len(chunk)
        if max_bytes is not None and size > max_bytes:
            raise RealRecoveryIntegrityError("artifact size exceeds its byte budget")
        digest.update(chunk)
    return size, digest.hexdigest()


def _regular_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _directory_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


def _assert_safe_ancestors(root: Path, relative: str) -> Path:
    current = root
    root_stat = os.lstat(root)
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise RealRecoveryIntegrityError("artifact root is missing or unsafe")
    for part in PurePosixPath(relative).parts[:-1]:
        current /= part
        observed = os.lstat(current)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise RealRecoveryIntegrityError("artifact ancestor contains a symlink")
    return root / relative


def _assert_no_unsealed_database_sidecars(
    root: Path,
    *,
    relative: str,
    kind: RealRecoveryArtifactKind,
) -> None:
    path = _assert_safe_ancestors(root, relative)
    suffixes: tuple[str, ...] = ()
    if kind in {
        RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
        RealRecoveryArtifactKind.RESEARCH_CATALOG,
        RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
        RealRecoveryArtifactKind.SERVING_DATABASE,
    }:
        suffixes = (".wal",)
    elif kind in {
        RealRecoveryArtifactKind.STATE_SQLITE,
        RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
    }:
        suffixes = ("-wal", "-shm")
    for suffix in suffixes:
        sidecar = path.with_name(f"{path.name}{suffix}")
        try:
            os.lstat(sidecar)
        except FileNotFoundError:
            continue
        raise RealRecoveryIntegrityError(
            f"database artifact has an unsealed sidecar: {sidecar.name}"
        )


@contextmanager
def _open_safe_regular(root: Path, relative: str):
    path = _assert_safe_ancestors(root, relative)
    parent_before = os.lstat(path.parent)
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        named = os.lstat(path)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise RealRecoveryIntegrityError("artifact is not a private regular file or has links")
        yield descriptor, path, opened
        after = os.fstat(descriptor)
        named_after = os.lstat(path)
        parent_after = os.lstat(path.parent)
        if (
            _regular_stat_identity(opened) != _regular_stat_identity(after)
            or (after.st_dev, after.st_ino) != (named_after.st_dev, named_after.st_ino)
            or _directory_stat_identity(parent_before) != _directory_stat_identity(parent_after)
        ):
            raise RealRecoveryIntegrityError("artifact identity changed while reading")
    except OSError as exc:
        raise RealRecoveryIntegrityError("artifact path is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _quote_identifier(value: str) -> str:
    if not value or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
        for character in value
    ):
        raise RealRecoveryIntegrityError("relation name is not a canonical identifier")
    return '"' + value + '"'


def _update_length_prefixed(digest: _DigestWriter, payload: bytes) -> None:
    digest.update(len(payload).to_bytes(8, byteorder="big", signed=False))
    digest.update(payload)


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _update_research_logical_hash(digest: _DigestWriter, value: object) -> None:
    if value is None:
        payload = b""
        marker = b"N"
    elif isinstance(value, bool):
        payload = b"1" if value else b"0"
        marker = b"B"
    elif isinstance(value, int):
        payload = str(value).encode("ascii")
        marker = b"I"
    elif isinstance(value, float):
        payload = struct.pack(">d", value)
        marker = b"F"
    elif isinstance(value, datetime):
        payload = value.isoformat(timespec="microseconds").encode("ascii")
        marker = b"T"
    elif isinstance(value, date):
        payload = value.isoformat().encode("ascii")
        marker = b"D"
    elif isinstance(value, str):
        payload = value.encode("utf-8")
        marker = b"S"
    else:
        raise RealRecoveryIntegrityError(
            f"research partition contains unsupported value: {type(value).__name__}"
        )
    digest.update(marker)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _duckdb_deadline_timer(
    connection: duckdb.DuckDBPyConnection,
    meter: _VerificationMeter,
) -> tuple[threading.Timer, threading.Event]:
    interrupted = threading.Event()

    def interrupt() -> None:
        interrupted.set()
        with suppress(Exception):
            connection.interrupt()

    timer = threading.Timer(meter.remaining_seconds(), interrupt)
    timer.daemon = True
    timer.start()
    return timer, interrupted


def _stop_deadline_timer(timer: threading.Timer) -> None:
    timer.cancel()
    timer.join(timeout=1)


def _install_sqlite_deadline(
    connection: sqlite3.Connection,
    meter: _VerificationMeter,
) -> threading.Event:
    interrupted = threading.Event()

    def progress() -> int:
        try:
            meter.check_deadline()
        except RealRecoveryIntegrityError:
            interrupted.set()
            return 1
        return 0

    connection.set_progress_handler(progress, 10_000)
    return interrupted


def _research_partition_predicate(manifest: ResearchPartitionManifest) -> str:
    contract = research_dataset_contract(manifest.dataset)
    day = _quote_literal(manifest.partition.trade_date.isoformat())
    if manifest.dataset == "minute_bar":
        event_column = _quote_identifier(str(contract.event_time_column))
        return (
            f"CAST({event_column} AS DATE) = DATE {day} "
            f"AND {_quote_identifier('freq')} = {_quote_literal(str(manifest.partition.freq))}"
        )
    event_column = _quote_identifier(str(contract.event_date_column))
    return f"{event_column} = DATE {day}"


def _research_event_is_after_as_of(
    value: datetime | date,
    *,
    as_of: datetime,
) -> bool:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise RealRecoveryIntegrityError("recovery as_of must be timezone-aware")
    from zoneinfo import ZoneInfo

    market_zone = ZoneInfo("Asia/Shanghai")
    if isinstance(value, datetime):
        event_time = (
            value.replace(tzinfo=market_zone)
            if value.tzinfo is None or value.utcoffset() is None
            else value
        )
        return event_time.astimezone(UTC) > as_of.astimezone(UTC)
    return value > as_of.astimezone(market_zone).date()


def _verify_bounded_research_partition(
    *,
    path: Path,
    manifest: ResearchPartitionManifest,
    as_of: datetime,
    meter: _VerificationMeter,
) -> None:
    """Verify the canonical lake contract with bounded external ordering."""

    contract = research_dataset_contract(manifest.dataset)
    columns = research_export_schema(manifest.dataset)
    expected_schema_hash = hashlib.sha256(
        json.dumps(columns, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    expected_relative = partition_version_relative_path(
        manifest.partition,
        manifest.file_hash,
    )
    expected_source = manifest.sources[0] if len(manifest.sources) == 1 else "mixed"
    if (
        manifest.dataset != manifest.partition.dataset
        or Path(manifest.relative_path) != expected_relative
        or manifest.primary_key != contract.physical_primary_key
        or manifest.schema_hash != expected_schema_hash
        or not set(manifest.sources) <= set(contract.sources)
        or manifest.source != expected_source
        or (
            manifest.earliest_time.date()
            if isinstance(manifest.earliest_time, datetime)
            else manifest.earliest_time
        )
        != manifest.partition.trade_date
        or (
            manifest.latest_time.date()
            if isinstance(manifest.latest_time, datetime)
            else manifest.latest_time
        )
        != manifest.partition.trade_date
    ):
        raise RealRecoveryIntegrityError("research partition manifest binding differs")
    if manifest.row_count > meter.budget.max_relation_rows:
        raise RealRecoveryIntegrityError("recovery relation row total exceeds budget")

    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or before.st_nlink != 1:
        raise RealRecoveryIntegrityError("research partition object is unsafe")
    reader = f"read_parquet({_quote_literal(str(path))}, hive_partitioning = false)"
    with TemporaryDirectory(prefix="rquant-recovery-lake-") as temporary:
        try:
            connection = duckdb.connect(
                config={
                    "temp_directory": temporary,
                    "threads": "1",
                    "memory_limit": f"{meter.budget.duckdb_memory_bytes}B",
                    "max_temp_directory_size": f"{meter.budget.duckdb_temp_bytes}B",
                    "preserve_insertion_order": "false",
                }
            )
        except (duckdb.Error, OSError) as exc:
            raise RealRecoveryIntegrityError(
                "research partition verifier cannot initialize"
            ) from exc
        deadline_timer, deadline_interrupted = _duckdb_deadline_timer(connection, meter)
        try:
            described = connection.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
            actual_columns = tuple((str(row[0]), str(row[1])) for row in described)
            if actual_columns != columns:
                raise RealRecoveryIntegrityError("research partition schema differs")
            budget_row = connection.execute(
                f"""
                SELECT COUNT(*)::UBIGINT,
                       COALESCE(SUM(octet_length(encode(row_json))), 0)::HUGEINT,
                       COALESCE(MAX(octet_length(encode(row_json))), 0)::UBIGINT
                FROM (SELECT to_json(t) AS row_json FROM {reader} AS t)
                """
            ).fetchone()
            assert budget_row is not None
            actual_rows = int(budget_row[0])
            meter.require_relation_capacity(
                rows=actual_rows,
                bytes_count=int(budget_row[1]),
                largest_row_bytes=int(budget_row[2]),
            )
            if actual_rows != manifest.row_count:
                raise RealRecoveryIntegrityError("research partition row count differs")

            keys = ", ".join(_quote_identifier(column) for column in contract.physical_primary_key)
            duplicate = connection.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT {keys} FROM {reader}
                    GROUP BY {keys} HAVING COUNT(*) > 1
                )
                """
            ).fetchone()
            if duplicate is not None and int(duplicate[0]) > 0:
                raise RealRecoveryIntegrityError("research partition has duplicate primary keys")
            mismatched = connection.execute(
                f"SELECT COUNT(*) FROM {reader} "
                f"WHERE NOT ({_research_partition_predicate(manifest)})"
            ).fetchone()
            if mismatched is not None and int(mismatched[0]) > 0:
                raise RealRecoveryIntegrityError("research partition dimensions differ")

            event_name = contract.event_time_column or contract.event_date_column
            event = _quote_identifier(str(event_name))
            bounds = connection.execute(
                f"SELECT MIN({event}), MAX({event}) FROM {reader}"
            ).fetchone()
            if (
                bounds is None
                or bounds[0] is None
                or bounds[1] is None
                or bounds[0] != manifest.earliest_time
                or bounds[1] != manifest.latest_time
            ):
                raise RealRecoveryIntegrityError("research partition event bounds differ")
            if _research_event_is_after_as_of(bounds[1], as_of=as_of):
                raise RealRecoveryIntegrityError("research partition contains future data")

            selected = ", ".join(_quote_identifier(column) for column, _ in columns)
            cursor = connection.execute(
                f"SELECT {selected}, to_json(t) FROM {reader} AS t ORDER BY {keys}"
            )
            digest = hashlib.sha256()
            row_count = 0
            while rows := cursor.fetchmany(1024):
                meter.check_deadline()
                for row in rows:
                    payload = str(row[-1]).encode("utf-8")
                    meter.add_relation_row(payload)
                    digest.update(b"R")
                    for value in row[:-1]:
                        _update_research_logical_hash(digest, value)
                    row_count += 1
            if row_count != manifest.row_count or digest.hexdigest() != manifest.content_hash:
                raise RealRecoveryIntegrityError("research partition content hash differs")
        except duckdb.Error as exc:
            if deadline_interrupted.is_set():
                raise RealRecoveryIntegrityError(
                    "research partition verification deadline exceeded"
                ) from exc
            raise RealRecoveryIntegrityError("research partition verification failed") from exc
        finally:
            _stop_deadline_timer(deadline_timer)
            connection.close()
    after = os.lstat(path)
    if _regular_stat_identity(before) != _regular_stat_identity(after):
        raise RealRecoveryIntegrityError("research partition changed during verification")


def _sqlite_row_bytes(row: Sequence[object]) -> bytes:
    encoded: list[Mapping[str, JsonValue]] = []
    for value in row:
        if value is None:
            encoded.append({"type": "null", "value": None})
        elif isinstance(value, bool):
            encoded.append({"type": "bool", "value": value})
        elif isinstance(value, int):
            encoded.append({"type": "int", "value": value})
        elif isinstance(value, float):
            encoded.append({"type": "float", "value": repr(value)})
        elif isinstance(value, str):
            encoded.append({"type": "text", "value": value})
        elif isinstance(value, bytes):
            encoded.append({"type": "blob", "value": value.hex()})
        else:
            raise RealRecoveryIntegrityError("SQLite relation contains an unsupported value")
    return canonical_json_bytes(encoded)


def _configure_sqlite_verifier(
    connection: sqlite3.Connection,
    *,
    path: Path,
    meter: _VerificationMeter,
) -> threading.Event:
    if path.stat().st_size > meter.budget.max_total_bytes:
        raise RealRecoveryIntegrityError("recovery SQLite file exceeds byte budget")
    interrupted = _install_sqlite_deadline(connection, meter)
    cache_kib = max(
        64,
        min(meter.budget.duckdb_memory_bytes // 1024, 64 * 1024),
    )
    try:
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA temp_store = FILE")
        connection.execute(f"PRAGMA cache_size = -{cache_kib}")
        connection.execute("PRAGMA mmap_size = 0")
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    except sqlite3.Error as exc:
        raise RealRecoveryIntegrityError("SQLite verifier limits cannot be installed") from exc
    if page_count < 0 or page_size <= 0 or page_count * page_size > meter.budget.max_total_bytes:
        raise RealRecoveryIntegrityError("recovery SQLite page inventory exceeds byte budget")
    meter.check_deadline()
    return interrupted


@dataclass(frozen=True)
class _SQLiteRowLocator:
    rowid: int | None
    rowid_alias: str | None
    key: tuple[object, ...]
    storage_types: tuple[str, ...]
    byte_lengths: tuple[int, ...]


def _sqlite_octet_length_sql(identifier: str) -> str:
    return (
        f"CASE typeof({identifier}) "
        f"WHEN 'text' THEN length(CAST({identifier} AS BLOB)) "
        f"WHEN 'blob' THEN length({identifier}) "
        f"WHEN 'null' THEN 0 ELSE length(CAST({identifier} AS TEXT)) END"
    )


def _sqlite_sql_max(expressions: Sequence[str]) -> str:
    if not expressions:
        return "0"
    if len(expressions) == 1:
        return expressions[0]
    return f"max({', '.join(expressions)})"


def _sqlite_internal_rowid_alias(
    connection: sqlite3.Connection,
    *,
    quoted_relation: str,
    relation_type: str,
    columns: tuple[str, ...],
) -> str | None:
    if relation_type != "table":
        return None
    business_columns = {column.casefold() for column in columns}
    for alias in ("rowid", "_rowid_", "oid"):
        if alias.casefold() in business_columns:
            continue
        try:
            connection.execute(f"SELECT {_quote_identifier(alias)} FROM {quoted_relation} LIMIT 0")
        except sqlite3.Error:
            continue
        return alias
    return None


def _sqlite_cell_encoded_bound_sql(identifier: str, *, memory_bytes: int) -> str:
    invalid = memory_bytes + 1
    return (
        f"CASE typeof({identifier}) "
        f"WHEN 'null' THEN {len(_SQLITE_NULL_FRAGMENT)} "
        f"WHEN 'integer' THEN {len(_SQLITE_INT_PREFIX) + len(_SQLITE_CELL_SUFFIX)} "
        f"+ length(CAST({identifier} AS TEXT)) "
        f"WHEN 'real' THEN {len(_SQLITE_FLOAT_PREFIX) + len(_SQLITE_CELL_SUFFIX) + 34} "
        f"WHEN 'text' THEN {len(_SQLITE_TEXT_PREFIX) + len(_SQLITE_CELL_SUFFIX)} "
        f"+ length(CAST(json_quote({identifier}) AS BLOB)) "
        f"WHEN 'blob' THEN {len(_SQLITE_BLOB_PREFIX) + len(_SQLITE_CELL_SUFFIX)} "
        f"+ (2 * length({identifier})) ELSE {invalid} END"
    )


def _sqlite_preflight_relation(
    connection: sqlite3.Connection,
    *,
    quoted_relation: str,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
    rowid_alias: str | None,
    meter: _VerificationMeter,
) -> int:
    """Reject unsafe row widths before payload access.

    Python's SQLite API exposes incremental BLOB handles but no equivalent TEXT
    handle. TEXT is therefore capped at memory/32 (and 1 MiB) before json_quote
    or Python value conversion; BLOBs on rowid tables are streamed later.
    """

    quoted_columns = tuple(_quote_identifier(column) for column in columns)
    text_lengths = tuple(
        f"CASE WHEN typeof({column}) = 'text' THEN length(CAST({column} AS BLOB)) ELSE 0 END"
        for column in quoted_columns
    )
    key_lengths = tuple(
        f"CASE WHEN typeof({_quote_identifier(column)}) IN ('text', 'blob') "
        f"THEN {_sqlite_octet_length_sql(_quote_identifier(column))} ELSE 0 END"
        for column in primary_key
    )
    unstreamable_blob_lengths = (
        ()
        if rowid_alias is not None
        else tuple(
            f"CASE WHEN typeof({column}) = 'blob' THEN length({column}) ELSE 0 END"
            for column in quoted_columns
        )
    )
    raw_budget = connection.execute(
        f"""
        SELECT COUNT(*),
               COALESCE(MAX({_sqlite_sql_max(text_lengths)}), 0),
               COALESCE(MAX({_sqlite_sql_max(key_lengths)}), 0),
               COALESCE(MAX({_sqlite_sql_max(unstreamable_blob_lengths)}), 0)
        FROM {quoted_relation}
        """
    ).fetchone()
    if raw_budget is None:
        raise RealRecoveryIntegrityError("SQLite relation length inventory is unavailable")
    expected_rows = int(raw_budget[0])
    meter.require_relation_capacity(
        rows=expected_rows,
        bytes_count=0,
        largest_row_bytes=0,
    )
    text_hard_limit = max(
        1,
        min(
            meter.budget.max_row_bytes,
            meter.budget.duckdb_memory_bytes // 32,
            1024 * 1024,
        ),
    )
    key_hard_limit = max(
        1,
        min(
            text_hard_limit,
            meter.budget.duckdb_memory_bytes // (2 * _SQLITE_KEYSET_BATCH),
        ),
    )
    if int(raw_budget[1]) > text_hard_limit:
        raise RealRecoveryIntegrityError(
            "SQLite TEXT exceeds the non-incremental verifier hard limit"
        )
    if int(raw_budget[2]) > key_hard_limit:
        raise RealRecoveryIntegrityError("SQLite keyset value exceeds its memory budget")
    if int(raw_budget[3]) > text_hard_limit:
        raise RealRecoveryIntegrityError(
            "SQLite BLOB without rowid exceeds the incremental verifier hard limit"
        )

    row_overhead = 2 + max(0, len(columns) - 1)
    row_bound = str(row_overhead)
    if quoted_columns:
        row_bound += " + " + " + ".join(
            _sqlite_cell_encoded_bound_sql(
                column,
                memory_bytes=meter.budget.duckdb_memory_bytes,
            )
            for column in quoted_columns
        )
    bound_row = connection.execute(
        f"SELECT COALESCE(MAX({row_bound}), 0) FROM {quoted_relation}"
    ).fetchone()
    largest_row = 0 if bound_row is None else int(bound_row[0])
    if largest_row > meter.budget.max_row_bytes:
        raise RealRecoveryIntegrityError("recovery relation row exceeds byte budget")
    if largest_row > meter.budget.duckdb_memory_bytes:
        raise RealRecoveryIntegrityError("SQLite relation row exceeds verifier memory budget")
    meter.require_relation_capacity(
        rows=expected_rows,
        bytes_count=0,
        largest_row_bytes=largest_row,
    )
    return expected_rows


def _sqlite_keyset_rows(
    connection: sqlite3.Connection,
    *,
    relation: str,
    relation_type: str,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
    rowid_alias: str | None,
    meter: _VerificationMeter,
) -> Iterator[_SQLiteRowLocator]:
    quoted = _quote_identifier(relation)
    if not primary_key and rowid_alias is None:
        raise RealRecoveryIntegrityError(
            "SQLite relation has no usable primary key or internal rowid alias"
        )
    quoted_rowid = None if rowid_alias is None else _quote_identifier(rowid_alias)
    quoted_primary = tuple(_quote_identifier(value) for value in primary_key)
    metadata = tuple(
        item
        for column in columns
        for item in (
            f"typeof({_quote_identifier(column)})",
            _sqlite_octet_length_sql(_quote_identifier(column)),
        )
    )
    selected = (
        *((f"{quoted_rowid} AS __recovery_rowid__",) if quoted_rowid else ()),
        *quoted_primary,
        *metadata,
    )
    last_key: tuple[object, ...] | None = None
    while True:
        meter.check_deadline()
        if primary_key:
            order = ", ".join(quoted_primary)
            if last_key is None:
                where = ""
                parameters: tuple[object, ...] = (_SQLITE_KEYSET_BATCH,)
            else:
                placeholders = ", ".join("?" for _ in primary_key)
                where = f"WHERE ({order}) > ({placeholders})"
                parameters = (*last_key, _SQLITE_KEYSET_BATCH)
        else:
            assert quoted_rowid is not None
            order = quoted_rowid
            if last_key is None:
                where = ""
                parameters = (_SQLITE_KEYSET_BATCH,)
            else:
                where = f"WHERE {quoted_rowid} > ?"
                parameters = (last_key[0], _SQLITE_KEYSET_BATCH)
        query = f"SELECT {', '.join(selected)} FROM {quoted} {where} ORDER BY {order} LIMIT ?"
        plan = connection.execute(f"EXPLAIN QUERY PLAN {query}", parameters).fetchall()
        if any("USE TEMP B-TREE" in str(row[3]).upper() for row in plan):
            raise RealRecoveryIntegrityError(
                "SQLite keyset traversal would require an unbounded temporary sort"
            )
        cursor = connection.execute(query, parameters)
        observed = 0
        next_key: tuple[object, ...] | None = None
        while row := cursor.fetchone():
            meter.check_deadline()
            observed += 1
            offset = 0
            rowid: int | None = None
            if quoted_rowid is not None:
                observed_rowid = row[offset]
                if type(observed_rowid) is not int:
                    raise RealRecoveryIntegrityError(
                        "SQLite internal rowid locator is not an integer"
                    )
                rowid = observed_rowid
                offset += 1
            key = tuple(row[offset : offset + len(primary_key)])
            offset += len(primary_key)
            storage_types = tuple(str(row[offset + index * 2]) for index in range(len(columns)))
            byte_lengths = tuple(int(row[offset + index * 2 + 1]) for index in range(len(columns)))
            if any(value not in _SQLITE_STORAGE_TYPES for value in storage_types):
                raise RealRecoveryIntegrityError("SQLite relation contains an unsupported value")
            if any(value < 0 for value in byte_lengths):
                raise RealRecoveryIntegrityError("SQLite relation has an invalid value length")
            next_key = key if primary_key else (rowid,)
            if any(value is None for value in next_key):
                raise RealRecoveryIntegrityError("SQLite keyset contains a null key")
            yield _SQLiteRowLocator(
                rowid=rowid,
                rowid_alias=rowid_alias,
                key=key,
                storage_types=storage_types,
                byte_lengths=byte_lengths,
            )
        if observed < _SQLITE_KEYSET_BATCH:
            return
        if next_key is None or next_key == last_key:
            raise RealRecoveryIntegrityError("SQLite keyset traversal did not advance")
        last_key = next_key


def _sqlite_scalar_fragment(value: object, *, storage_type: str) -> bytes:
    if storage_type == "null" and value is None:
        return _SQLITE_NULL_FRAGMENT
    if storage_type == "integer" and isinstance(value, int):
        return canonical_json_bytes({"type": "int", "value": value})
    if storage_type == "real" and isinstance(value, float):
        return canonical_json_bytes({"type": "float", "value": repr(value)})
    if storage_type == "text" and isinstance(value, str):
        return canonical_json_bytes({"type": "text", "value": value})
    if storage_type == "blob" and isinstance(value, bytes):
        return canonical_json_bytes({"type": "blob", "value": value.hex()})
    raise RealRecoveryIntegrityError("SQLite value differs from its length inventory")


def _sqlite_stream_blob(
    connection: sqlite3.Connection,
    *,
    relation: str,
    column: str,
    rowid: int,
    expected_bytes: int,
    digest: _DigestWriter,
    meter: _VerificationMeter,
) -> None:
    digest.update(_SQLITE_BLOB_PREFIX)
    try:
        blob = connection.blobopen(relation, column, rowid, readonly=True, name="main")
    except sqlite3.Error as exc:
        raise RealRecoveryIntegrityError("SQLite BLOB cannot be opened incrementally") from exc
    try:
        if len(blob) != expected_bytes:
            raise RealRecoveryIntegrityError("SQLite BLOB length changed inside snapshot")
        remaining = expected_bytes
        chunk_size = max(
            4096,
            min(_SQLITE_BLOB_CHUNK_BYTES, meter.budget.duckdb_memory_bytes // 256),
        )
        while remaining:
            meter.check_deadline()
            chunk = blob.read(min(chunk_size, remaining))
            if not chunk:
                raise RealRecoveryIntegrityError("SQLite BLOB ended before its declared length")
            remaining -= len(chunk)
            digest.update(binascii.hexlify(chunk))
    finally:
        blob.close()
    digest.update(b'"}')


def _sqlite_digest_row(
    connection: sqlite3.Connection,
    *,
    relation: str,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
    locator: _SQLiteRowLocator,
    digest: _DigestWriter,
    meter: _VerificationMeter,
) -> None:
    quoted_relation = _quote_identifier(relation)
    selected = tuple(
        (
            f"CASE WHEN typeof({_quote_identifier(column)}) = 'blob' "
            f"THEN NULL ELSE {_quote_identifier(column)} END"
            if locator.rowid is not None
            else _quote_identifier(column)
        )
        for column in columns
    )
    if locator.rowid is not None:
        if locator.rowid_alias is None:
            raise RealRecoveryIntegrityError("SQLite rowid locator alias is missing")
        where = f"{_quote_identifier(locator.rowid_alias)} = ?"
        parameters: tuple[object, ...] = (locator.rowid,)
    else:
        where = " AND ".join(f"{_quote_identifier(column)} = ?" for column in primary_key)
        parameters = locator.key
    cursor = connection.execute(
        f"SELECT {', '.join(selected)} FROM {quoted_relation} WHERE {where} LIMIT 2",
        parameters,
    )
    values = cursor.fetchone()
    if values is None or cursor.fetchone() is not None:
        raise RealRecoveryIntegrityError("SQLite row locator is not unique inside snapshot")

    fragments: list[bytes | None] = []
    row_size = 2 + max(0, len(columns) - 1)
    for value, storage_type, byte_length in zip(
        values,
        locator.storage_types,
        locator.byte_lengths,
        strict=True,
    ):
        if storage_type == "blob" and locator.rowid is not None:
            fragments.append(None)
            row_size += len(_SQLITE_BLOB_PREFIX) + 2 * byte_length + 2
            continue
        fragment = _sqlite_scalar_fragment(value, storage_type=storage_type)
        fragments.append(fragment)
        row_size += len(fragment)
    meter.add_relation_row_size(row_size)
    digest.update(row_size.to_bytes(8, byteorder="big", signed=False))
    digest.update(b"[")
    for index, (column, fragment) in enumerate(zip(columns, fragments, strict=True)):
        if index:
            digest.update(b",")
        if fragment is not None:
            digest.update(fragment)
            continue
        assert locator.rowid is not None
        _sqlite_stream_blob(
            connection,
            relation=relation,
            column=column,
            rowid=locator.rowid,
            expected_bytes=locator.byte_lengths[index],
            digest=digest,
            meter=meter,
        )
    digest.update(b"]")


def _duckdb_relation_evidence(
    path: Path,
    relations: Sequence[str],
    *,
    meter: _VerificationMeter,
) -> tuple[RelationEvidence, ...]:
    evidence: list[RelationEvidence] = []
    with TemporaryDirectory(prefix="rquant-recovery-duckdb-") as temporary:
        try:
            connection = duckdb.connect(
                str(path),
                read_only=True,
                config={
                    "temp_directory": temporary,
                    "threads": "1",
                    "memory_limit": f"{meter.budget.duckdb_memory_bytes}B",
                    "max_temp_directory_size": f"{meter.budget.duckdb_temp_bytes}B",
                    "preserve_insertion_order": "false",
                },
            )
        except (duckdb.Error, OSError) as exc:
            raise RealRecoveryIntegrityError("DuckDB artifact cannot be opened read-only") from exc
        deadline_timer, deadline_interrupted = _duckdb_deadline_timer(connection, meter)
        try:
            for relation in sorted(relations):
                meter.check_deadline()
                quoted = _quote_identifier(relation)
                column_cursor = connection.execute(
                    """
                    SELECT column_name, data_type, is_nullable, COALESCE(column_default, '')
                    FROM information_schema.columns
                    WHERE table_catalog = current_database() AND table_schema = 'main'
                      AND table_name = ?
                    ORDER BY ordinal_position
                    """,
                    [relation],
                )
                columns: list[tuple[str, str, str, str]] = []
                while rows := column_cursor.fetchmany(256):
                    for row in rows:
                        columns.append(tuple(str(value) for value in row))
                        if len(columns) > 4096:
                            raise RealRecoveryIntegrityError(
                                "DuckDB relation column inventory exceeds budget"
                            )
                if not columns:
                    raise RealRecoveryIntegrityError(f"DuckDB relation is missing: {relation}")
                budget_row = connection.execute(
                    f"""
                    SELECT COUNT(*)::UBIGINT,
                           COALESCE(SUM(octet_length(encode(row_json))), 0)::HUGEINT,
                           COALESCE(MAX(octet_length(encode(row_json))), 0)::UBIGINT
                    FROM (SELECT to_json(t) AS row_json FROM {quoted} AS t)
                    """
                ).fetchone()
                assert budget_row is not None
                meter.require_relation_capacity(
                    rows=int(budget_row[0]),
                    bytes_count=int(budget_row[1]),
                    largest_row_bytes=int(budget_row[2]),
                )
                primary = connection.execute(
                    """
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name = 'main' AND table_name = ?
                      AND constraint_type = 'PRIMARY KEY'
                    LIMIT 2
                    """,
                    [relation],
                ).fetchall()
                if len(primary) > 1:
                    raise RealRecoveryIntegrityError("DuckDB relation has ambiguous primary keys")
                if primary:
                    order_columns = tuple(str(value) for value in primary[0][0])
                    order_clause = ", ".join(_quote_identifier(value) for value in order_columns)
                    query = (
                        f"SELECT to_json(t) AS row_json FROM {quoted} AS t ORDER BY {order_clause}"
                    )
                else:
                    query = f"SELECT to_json(t) AS row_json FROM {quoted} AS t ORDER BY row_json"
                cursor = connection.execute(query)
                content_digest = hashlib.sha256()
                content_digest.update(_RELATION_HASH_CONTRACT.encode("ascii"))
                row_count = 0
                while rows := cursor.fetchmany(1024):
                    meter.check_deadline()
                    for row in rows:
                        payload = str(row[0]).encode("utf-8")
                        meter.add_relation_row(payload)
                        _update_length_prefixed(content_digest, payload)
                        row_count += 1
                evidence.append(
                    RelationEvidence(
                        relation_name=relation,
                        row_count=row_count,
                        schema_sha256=canonical_sha256(tuple(columns)),
                        content_sha256=content_digest.hexdigest(),
                    )
                )
        except duckdb.Error as exc:
            if deadline_interrupted.is_set():
                raise RealRecoveryIntegrityError(
                    "recovery DuckDB relation verification deadline exceeded"
                ) from exc
            raise RealRecoveryIntegrityError("DuckDB relation verification failed") from exc
        finally:
            _stop_deadline_timer(deadline_timer)
            connection.close()
    return tuple(evidence)


def _sqlite_relation_evidence(
    path: Path,
    relations: Sequence[str],
    *,
    meter: _VerificationMeter,
) -> tuple[RelationEvidence, ...]:
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or before.st_nlink != 1:
        raise RealRecoveryIntegrityError("SQLite artifact is unsafe")
    try:
        connection = sqlite3.connect(
            f"file:{path}?mode=ro&immutable=1",
            uri=True,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        deadline_interrupted = _configure_sqlite_verifier(
            connection,
            path=path,
            meter=meter,
        )
        connection.execute("BEGIN")
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise RealRecoveryIntegrityError("SQLite integrity_check failed")
        evidence: list[RelationEvidence] = []
        for relation in sorted(relations):
            meter.check_deadline()
            quoted = _quote_identifier(relation)
            schema = connection.execute(
                "SELECT type, name, sql FROM sqlite_master WHERE name = ?",
                (relation,),
            ).fetchone()
            if schema is None:
                raise RealRecoveryIntegrityError(f"SQLite relation is missing: {relation}")
            table_columns = connection.execute(f"PRAGMA table_info({quoted})").fetchall()
            columns = tuple(str(row[1]) for row in table_columns)
            if not columns:
                raise RealRecoveryIntegrityError("SQLite relation column inventory is empty")
            for column in columns:
                _quote_identifier(column)
            primary = tuple(
                str(row[1])
                for row in sorted(table_columns, key=lambda row: int(row[5]) or 10_000)
                if int(row[5]) > 0
            )
            rowid_alias = _sqlite_internal_rowid_alias(
                connection,
                quoted_relation=quoted,
                relation_type=str(schema[0]),
                columns=columns,
            )
            expected_rows = _sqlite_preflight_relation(
                connection,
                quoted_relation=quoted,
                columns=columns,
                primary_key=primary,
                rowid_alias=rowid_alias,
                meter=meter,
            )
            content_digest = hashlib.sha256()
            content_digest.update(_RELATION_HASH_CONTRACT.encode("ascii"))
            row_count = 0
            for locator in _sqlite_keyset_rows(
                connection,
                relation=relation,
                relation_type=str(schema[0]),
                columns=columns,
                primary_key=primary,
                rowid_alias=rowid_alias,
                meter=meter,
            ):
                _sqlite_digest_row(
                    connection,
                    relation=relation,
                    columns=columns,
                    primary_key=primary,
                    locator=locator,
                    digest=content_digest,
                    meter=meter,
                )
                row_count += 1
            if row_count != expected_rows:
                raise RealRecoveryIntegrityError("SQLite relation changed during digest")
            evidence.append(
                RelationEvidence(
                    relation_name=relation,
                    row_count=row_count,
                    schema_sha256=canonical_sha256(tuple(schema)),
                    content_sha256=content_digest.hexdigest(),
                )
            )
        connection.execute("ROLLBACK")
        after = os.lstat(path)
        if _regular_stat_identity(before) != _regular_stat_identity(after):
            raise RealRecoveryIntegrityError("SQLite artifact changed during snapshot verification")
        return tuple(evidence)
    except sqlite3.Error as exc:
        if "deadline_interrupted" in locals() and deadline_interrupted.is_set():
            raise RealRecoveryIntegrityError(
                "recovery SQLite relation verification deadline exceeded"
            ) from exc
        raise RealRecoveryIntegrityError("SQLite artifact verification failed") from exc
    finally:
        with suppress(UnboundLocalError):
            connection.close()


def _read_bounded_json(
    path: Path,
    *,
    max_bytes: int = _MAX_JSON_BYTES,
    meter: _VerificationMeter | None = None,
) -> bytes:
    parent_before = os.lstat(path.parent)
    if stat.S_ISLNK(parent_before.st_mode) or not stat.S_ISDIR(parent_before.st_mode):
        raise RealRecoveryIntegrityError("artifact manifest parent is unsafe")
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        named = os.lstat(path)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or opened.st_size > max_bytes
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise RealRecoveryIntegrityError("artifact manifest is unsafe")
        chunks: list[bytes] = []
        size = 0
        while chunk := os.read(descriptor, _CHUNK_SIZE):
            size += len(chunk)
            if size > max_bytes:
                raise RealRecoveryIntegrityError("artifact manifest exceeds its JSON byte budget")
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        named_after = os.lstat(path)
        parent_after = os.lstat(path.parent)
        if (
            len(raw) != opened.st_size
            or _regular_stat_identity(opened) != _regular_stat_identity(after)
            or (after.st_dev, after.st_ino) != (named_after.st_dev, named_after.st_ino)
            or _directory_stat_identity(parent_before) != _directory_stat_identity(parent_after)
        ):
            raise RealRecoveryIntegrityError("artifact manifest changed while reading")
        if meter is not None:
            meter.check_json_bytes(raw)
        return raw
    except OSError as exc:
        raise RealRecoveryIntegrityError("artifact manifest is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _contract_for_path(
    *,
    path: Path,
    kind: RealRecoveryArtifactKind,
    relations: Sequence[str],
    generation_id: str,
    price_basis: str | None,
    meter: _VerificationMeter,
) -> tuple[tuple[RelationEvidence, ...], Mapping[str, JsonValue]]:
    relation_evidence: tuple[RelationEvidence, ...] = ()
    identity: dict[str, JsonValue] = {"kind": kind.value}
    if kind in {
        RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
        RealRecoveryArtifactKind.RESEARCH_CATALOG,
        RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
        RealRecoveryArtifactKind.SERVING_DATABASE,
    }:
        relation_evidence = _duckdb_relation_evidence(path, relations, meter=meter)
        identity.update(
            {
                "generation_id": generation_id,
                "relation_hash_contract": _RELATION_HASH_CONTRACT,
                "relations": [item.model_dump(mode="json") for item in relation_evidence],
            }
        )
    elif kind is RealRecoveryArtifactKind.STATE_SQLITE:
        relation_evidence = _sqlite_relation_evidence(path, relations, meter=meter)
        connection = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        deadline_interrupted = _install_sqlite_deadline(connection, meter)
        try:
            identity.update(
                {
                    "application_id": int(
                        connection.execute("PRAGMA application_id").fetchone()[0]
                    ),
                    "user_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
                    "relation_hash_contract": _RELATION_HASH_CONTRACT,
                    "relations": [item.model_dump(mode="json") for item in relation_evidence],
                }
            )
        except sqlite3.Error as exc:
            if deadline_interrupted.is_set():
                raise RealRecoveryIntegrityError(
                    "recovery SQLite identity verification deadline exceeded"
                ) from exc
            raise
        finally:
            connection.close()
    elif kind is RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST:
        try:
            manifest = ResearchPartitionManifest.model_validate_json(
                _read_bounded_json(
                    path,
                    max_bytes=meter.budget.max_json_bytes,
                    meter=meter,
                )
            )
        except Exception as exc:
            raise RealRecoveryIntegrityError("research partition manifest is invalid") from exc
        identity.update(
            {
                "partition_id": manifest.partition.partition_id,
                "relative_path": manifest.relative_path,
                "row_count": manifest.row_count,
                "schema_hash": manifest.schema_hash,
                "content_hash": manifest.content_hash,
                "file_hash": manifest.file_hash,
                "file_size": manifest.file_size,
                "created_at": manifest.created_at.isoformat(),
            }
        )
    elif kind is RealRecoveryArtifactKind.SERVING_CURRENT:
        try:
            pointer = ServingCurrentPointer.model_validate_json(
                _read_bounded_json(
                    path,
                    max_bytes=meter.budget.max_json_bytes,
                    meter=meter,
                )
            )
        except Exception as exc:
            raise RealRecoveryIntegrityError("serving current pointer is invalid") from exc
        identity.update(pointer.model_dump(mode="json"))
    elif kind is RealRecoveryArtifactKind.SERVING_MANIFEST:
        try:
            manifest = ServingGenerationManifest.model_validate_json(
                _read_bounded_json(
                    path,
                    max_bytes=meter.budget.max_json_bytes,
                    meter=meter,
                )
            )
        except Exception as exc:
            raise RealRecoveryIntegrityError("serving generation manifest is invalid") from exc
        identity.update(manifest.model_dump(mode="json"))
        identity["price_basis"] = price_basis
    elif kind is RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE:
        connection = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        inventory_interrupted = _install_sqlite_deadline(connection, meter)
        try:
            cursor = connection.execute("SELECT payload_json FROM reference_record")
            while rows := cursor.fetchmany(1024):
                meter.check_deadline()
                for row in rows:
                    meter.check_json_bytes(str(row[0]).encode("utf-8"))
        except sqlite3.Error as exc:
            if inventory_interrupted.is_set():
                raise RealRecoveryIntegrityError(
                    "reference slow inventory deadline exceeded"
                ) from exc
            raise RealRecoveryIntegrityError("reference slow payload inventory is invalid") from exc
        finally:
            connection.close()
        try:
            registry = ReadonlyReferenceRegistry(path)
            pointer = registry.current_pointer()
            manifest = registry.current_manifest()
        except Exception as exc:
            raise RealRecoveryIntegrityError("reference slow registry is invalid") from exc
        if pointer.generation_id != generation_id or manifest.generation_id != generation_id:
            raise RealRecoveryIntegrityError("reference slow generation differs from manifest")
        connection = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        payload_interrupted = _install_sqlite_deadline(connection, meter)
        try:
            cursor = connection.execute(
                "SELECT payload_json FROM reference_record WHERE dataset_id = ?",
                (ReferenceDataset.ADJUSTMENT_FACTOR.value,),
            )
            row_count = 0
            observed_price_basis: set[object] = set()
            while rows := cursor.fetchmany(1024):
                meter.check_deadline()
                for row in rows:
                    row_count += 1
                    raw = str(row[0]).encode("utf-8")
                    meter.check_json_bytes(raw)
                    try:
                        payload = strict_canonical_json_loads(raw)
                    except Exception as exc:
                        raise RealRecoveryIntegrityError(
                            "reference slow payload JSON is invalid"
                        ) from exc
                    if not isinstance(payload, dict):
                        raise RealRecoveryIntegrityError(
                            "reference slow payload JSON is not an object"
                        )
                    observed_price_basis.add(payload.get("price_basis"))
        except sqlite3.Error as exc:
            if payload_interrupted.is_set():
                raise RealRecoveryIntegrityError(
                    "reference slow payload deadline exceeded"
                ) from exc
            raise RealRecoveryIntegrityError("reference slow payload inventory is invalid") from exc
        finally:
            connection.close()
        if row_count == 0 or observed_price_basis != {price_basis}:
            raise RealRecoveryIntegrityError("reference slow price basis is inconsistent")
        identity.update(
            {
                "generation_id": manifest.generation_id,
                "manifest_sha256": manifest.manifest_sha256,
                "row_count": manifest.row_count,
                "dataset_counts": dict(manifest.dataset_counts),
                "published_at": manifest.published_at.isoformat(),
                "price_basis": price_basis,
            }
        )
    elif kind is RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST:
        raw = _read_bounded_json(
            path,
            max_bytes=meter.budget.max_json_bytes,
            meter=meter,
        )
        parsed = strict_canonical_json_loads(raw)
        if not isinstance(parsed, dict) or "schema_version" not in parsed:
            raise RealRecoveryIntegrityError("lab artifact manifest is invalid")
        try:
            from rquant.lab_artifacts import LabJobArtifactManifest
            from rquant.lab_worker import LabShardResultManifest

            if "claim_token" in parsed:
                manifest = LabShardResultManifest.model_validate(parsed)
                identity.update(
                    {
                        "manifest_type": "lab-shard-result",
                        "manifest_hash": manifest.manifest_hash,
                        "files": [item.file_name for item in manifest.artifacts],
                    }
                )
            else:
                manifest = LabJobArtifactManifest.model_validate(parsed)
                identity.update(
                    {
                        "manifest_type": "lab-job-artifact",
                        "manifest_hash": manifest.manifest_hash,
                        "files": [item.relative_path for item in manifest.files],
                    }
                )
        except Exception as exc:
            raise RealRecoveryIntegrityError("lab artifact manifest contract is invalid") from exc
    else:
        identity["generation_id"] = generation_id
    normalized = strict_canonical_json_loads(canonical_json_bytes(identity))
    assert isinstance(normalized, dict)
    return relation_evidence, MappingProxyType(dict(sorted(normalized.items())))


def _isolated_contract_for_path(
    *,
    path: Path,
    kind: RealRecoveryArtifactKind,
    relations: Sequence[str],
    generation_id: str,
    price_basis: str | None,
    meter: _VerificationMeter,
) -> tuple[tuple[RelationEvidence, ...], Mapping[str, JsonValue]]:
    contract_path = path
    temporary: TemporaryDirectory[str] | None = None
    if kind is RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE:
        temporary = TemporaryDirectory(prefix="rquant-reference-recovery-verify-")
        contract_path = Path(temporary.name) / "reference.sqlite3"
        shutil.copyfile(path, contract_path)
        os.chmod(contract_path, _PRIVATE_FILE_MODE)
    try:
        return _contract_for_path(
            path=contract_path,
            kind=kind,
            relations=relations,
            generation_id=generation_id,
            price_basis=price_basis,
            meter=meter,
        )
    finally:
        if temporary is not None:
            temporary.cleanup()


def build_real_recovery_target(
    *,
    source_root: Path,
    target_commit: str,
    target_profile_generation: str,
    as_of: datetime,
    production_artifact_role: str,
    paper_ledger_artifact_role: str,
    artifacts: Sequence[RealRecoveryArtifactSpec],
    external_attestations: Mapping[str, Sha256] = MappingProxyType({}),
    verification_budget: RecoveryVerificationBudget | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> RealRecoveryTargetManifest:
    """Capture exact real artifact contracts from an immutable backup root."""

    root = _canonical_absolute_path(source_root, label="source_root")
    if not root.is_dir() or root.is_symlink():
        raise RealRecoveryIntegrityError("source_root must be a physical directory")
    ordered_specs = tuple(sorted(artifacts, key=lambda item: item.logical_role))
    if not ordered_specs:
        raise ValueError("recovery target requires artifacts")
    budget = verification_budget or RecoveryVerificationBudget()
    if len(ordered_specs) > budget.max_artifacts:
        raise RealRecoveryIntegrityError("recovery artifact count exceeds budget")
    meter = _VerificationMeter(budget, monotonic=monotonic)
    captured: list[RealRecoveryArtifact] = []
    for spec in ordered_specs:
        meter.check_deadline()
        _assert_no_unsealed_database_sidecars(
            root,
            relative=spec.source_path,
            kind=spec.kind,
        )
        with _open_safe_regular(root, spec.source_path) as (descriptor, path, opened):
            size, digest = _hash_descriptor(descriptor, check=meter.check_deadline)
            if size != opened.st_size:
                raise RealRecoveryIntegrityError("artifact size changed while capturing")
            meter.add_artifact_bytes(size)
            first_identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        relation_evidence, identity = _isolated_contract_for_path(
            path=path,
            kind=spec.kind,
            relations=spec.relations,
            generation_id=spec.generation_id,
            price_basis=spec.price_basis,
            meter=meter,
        )
        with _open_safe_regular(root, spec.source_path) as (descriptor, _path, reopened):
            second_size, second_digest = _hash_descriptor(
                descriptor,
                check=meter.check_deadline,
            )
            second_identity = (
                reopened.st_dev,
                reopened.st_ino,
                reopened.st_size,
                reopened.st_mtime_ns,
            )
        if second_identity != first_identity or (second_size, second_digest) != (size, digest):
            raise RealRecoveryIntegrityError("artifact changed across contract verification")
        captured.append(
            RealRecoveryArtifact(
                logical_role=spec.logical_role,
                kind=spec.kind,
                source_path=spec.source_path,
                restore_path=spec.restore_path,
                size_bytes=size,
                sha256=digest,
                generation_id=spec.generation_id,
                schema_version=spec.schema_version,
                available_at=spec.available_at,
                price_basis=spec.price_basis,
                relations=relation_evidence,
                references=spec.references,
                contract_identity=identity,
                contract_sha256=canonical_sha256(identity),
            )
        )
    return RealRecoveryTargetManifest(
        target_commit=target_commit,
        target_profile_generation=target_profile_generation,
        as_of=as_of,
        production_artifact_role=production_artifact_role,
        paper_ledger_artifact_role=paper_ledger_artifact_role,
        artifacts=tuple(captured),
        external_attestations=external_attestations,
    )


def seal_recovery_tool_bundle(
    *,
    target: RealRecoveryTargetManifest,
    verifier_commit: str,
    executable_fingerprint: str,
    key_id: str,
    signer: RecoveryPayloadSigner,
) -> RecoveryToolVerifierBundle:
    if signer.key_id != key_id:
        raise ValueError("recovery signer key_id differs")
    unsigned = RecoveryToolVerifierBundle(
        verifier_commit=verifier_commit,
        executable_fingerprint=executable_fingerprint,
        target_manifest_id=str(target.manifest_id),
        target_profile_generation=target.target_profile_generation,
        key_id=key_id,
        signature="pending",
    )
    signature = signer.sign(unsigned.signing_payload())
    return RecoveryToolVerifierBundle(
        **unsigned.model_dump(mode="python", exclude={"signature", "bundle_id"}),
        signature=signature,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_atomic(path: Path, payload: bytes, *, mode: int = _PRIVATE_FILE_MODE) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        mode,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


class RealRecoveryRestorer:
    """Restore a signed real-artifact target into an isolated atomic generation."""

    def __init__(
        self,
        *,
        backup_root: Path,
        restore_root: Path,
        signature_verifier: RecoveryPayloadVerifier,
        fixed_replay_verifier: FixedReplayVerifier,
        max_artifacts: int = 4096,
        max_total_bytes: int = 256 * 1024**3,
        deadline_seconds: float = 6 * 60 * 60,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        if type(max_artifacts) is not int or max_artifacts < 1:
            raise ValueError("max_artifacts must be a positive integer")
        if type(max_total_bytes) is not int or max_total_bytes < 1:
            raise ValueError("max_total_bytes must be a positive integer")
        if isinstance(deadline_seconds, bool) or deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")
        self.backup_root = _canonical_absolute_path(backup_root, label="backup_root")
        self.restore_root = _canonical_absolute_path(restore_root, label="restore_root")
        if (
            self.backup_root == self.restore_root
            or self.backup_root.is_relative_to(self.restore_root)
            or self.restore_root.is_relative_to(self.backup_root)
        ):
            raise ValueError("backup and restore roots must be physically isolated")
        self.signature_verifier = signature_verifier
        self.fixed_replay_verifier = fixed_replay_verifier
        self.max_artifacts = max_artifacts
        self.max_total_bytes = max_total_bytes
        self.deadline_seconds = float(deadline_seconds)
        self.cancelled = cancelled or (lambda: False)
        self.generations_root = self.restore_root / "generations"
        self.candidates_root = self.restore_root / ".candidates"
        self.failed_root = self.restore_root / ".failed"
        self.audits_root = self.restore_root / "audits"
        self.receipts_root = self.restore_root / "receipts"
        self.lock_path = self.restore_root / ".recovery.lock"
        self.current_path = self.restore_root / "current.json"
        self.intent_path = self.restore_root / ".publication-intent.json"
        self._prepare_layout()
        with self._lock():
            self._recover_interrupted_publication()

    def _prepare_layout(self) -> None:
        if self.restore_root.is_symlink() or not self.restore_root.is_dir():
            raise RealRecoveryIntegrityError("restore root must be a physical existing directory")
        if os.lstat(self.restore_root).st_uid != os.geteuid():
            raise RealRecoveryIntegrityError("restore root owner is unsafe")
        for path in (
            self.generations_root,
            self.candidates_root,
            self.failed_root,
            self.audits_root,
            self.receipts_root,
        ):
            try:
                path.mkdir(mode=_PRIVATE_DIR_MODE)
                _fsync_directory(path.parent)
            except FileExistsError:
                pass
            observed = os.lstat(path)
            if (
                stat.S_ISLNK(observed.st_mode)
                or not stat.S_ISDIR(observed.st_mode)
                or observed.st_uid != os.geteuid()
            ):
                raise RealRecoveryIntegrityError("restore managed directory is unsafe")

    @contextmanager
    def _lock(self):
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
        )
        try:
            opened = os.fstat(descriptor)
            named = os.lstat(self.lock_path)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.geteuid()
                or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            ):
                raise RealRecoveryIntegrityError("recovery lock file is unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _current(self) -> RecoveryCurrentPointer | None:
        if not self.current_path.exists():
            return None
        try:
            return RecoveryCurrentPointer.model_validate_json(_read_bounded_json(self.current_path))
        except Exception as exc:
            raise RealRecoveryIntegrityError("recovery current pointer is invalid") from exc

    def _remove_durable(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            return
        _fsync_directory(path.parent)

    def _has_success_receipt(self, intent: _RecoveryPublicationIntent) -> bool:
        receipt_paths = tuple(self.receipts_root.glob("*.json"))
        if len(receipt_paths) > 4096:
            raise RealRecoveryIntegrityError("recovery receipt inventory exceeds budget")
        for path in receipt_paths:
            try:
                receipt = RealRecoveryReceipt.model_validate_json(_read_bounded_json(path))
            except Exception as exc:
                raise RealRecoveryIntegrityError("immutable recovery receipt is invalid") from exc
            if (
                receipt.operation_id == intent.operation_id
                and receipt.status == "succeeded"
                and receipt.published_generation_id == intent.manifest_id
            ):
                return True
        return False

    def _recover_interrupted_publication(self) -> None:
        if not self.intent_path.exists():
            return
        try:
            intent = _RecoveryPublicationIntent.model_validate_json(
                _read_bounded_json(self.intent_path)
            )
        except Exception as exc:
            raise RealRecoveryIntegrityError("recovery publication intent is invalid") from exc
        current = self._current()
        if self._has_success_receipt(intent):
            if current is None or current.generation_id != intent.manifest_id:
                raise RealRecoveryIntegrityError(
                    "committed recovery receipt and current generation differ"
                )
            self._remove_durable(self.intent_path)
            return
        if current is not None and current.generation_id == intent.manifest_id:
            previous = intent.previous_pointer
            if previous is None:
                self._remove_durable(self.current_path)
            else:
                previous_path = self.restore_root / previous.generation_path
                if (
                    not previous_path.is_dir()
                    or previous_path.is_symlink()
                    or previous_path.name != previous.generation_id
                ):
                    raise RealRecoveryIntegrityError(
                        "interrupted recovery previous generation is unavailable"
                    )
                _write_atomic(
                    self.current_path,
                    canonical_json_bytes(previous.model_dump(mode="json")),
                )
        self._remove_durable(self.intent_path)

    def _validate_bundle(
        self,
        *,
        target: RealRecoveryTargetManifest,
        tool_bundle: RecoveryToolVerifierBundle,
    ) -> None:
        try:
            validate_complete_recovery_artifact_graph(
                target.artifacts,
                production_artifact_role=target.production_artifact_role,
                paper_ledger_artifact_role=target.paper_ledger_artifact_role,
            )
            if "paper_ledger" not in target.external_attestations:
                raise ValueError("paper ledger external head is missing")
        except ValueError as exc:
            raise RealRecoveryIntegrityError(
                "recovery target complete production role graph is invalid"
            ) from exc
        if len(target.artifacts) > self.max_artifacts:
            raise RealRecoveryIntegrityError("recovery artifact count exceeds budget")
        if sum(item.size_bytes for item in target.artifacts) > self.max_total_bytes:
            raise RealRecoveryIntegrityError("recovery byte total exceeds budget")
        if (
            tool_bundle.target_manifest_id != target.manifest_id
            or tool_bundle.target_profile_generation != target.target_profile_generation
        ):
            raise RealRecoveryIntegrityError("recovery tool target profile or manifest differs")
        if (
            tool_bundle.key_id != self.signature_verifier.key_id
            or not self.signature_verifier.verify(
                tool_bundle.signing_payload(),
                tool_bundle.signature,
            )
        ):
            raise RealRecoveryIntegrityError("recovery tool bundle signature is invalid")
        if tool_bundle.executable_fingerprint != self.fixed_replay_verifier.fingerprint:
            raise RealRecoveryIntegrityError("recovery verifier executable fingerprint differs")

    def _check_deadline(self, deadline: float) -> None:
        if self.cancelled():
            raise RealRecoveryIntegrityError("recovery operation cancelled")
        if time.monotonic() > deadline:
            raise RealRecoveryIntegrityError("recovery deadline exceeded")

    def _preflight_sources(
        self,
        *,
        target: RealRecoveryTargetManifest,
        deadline: float,
    ) -> None:
        for artifact in target.artifacts:
            self._check_deadline(deadline)
            _assert_no_unsealed_database_sidecars(
                self.backup_root,
                relative=artifact.source_path,
                kind=artifact.kind,
            )
            with _open_safe_regular(self.backup_root, artifact.source_path) as (
                descriptor,
                _path,
                _opened,
            ):
                observed = _hash_descriptor(
                    descriptor,
                    max_bytes=artifact.size_bytes,
                    check=lambda: self._check_deadline(deadline),
                )
            if observed != (artifact.size_bytes, artifact.sha256):
                raise RealRecoveryIntegrityError(
                    f"backup artifact hash or size differs: {artifact.logical_role}"
                )

    def _copy_artifact(
        self,
        *,
        artifact: RealRecoveryArtifact,
        candidate: Path,
        deadline: float,
    ) -> None:
        destination = candidate.joinpath(*PurePosixPath(artifact.restore_path).parts)
        destination.parent.mkdir(mode=_PRIVATE_DIR_MODE, parents=True, exist_ok=True)
        if destination.exists() or destination.is_symlink():
            raise RealRecoveryIntegrityError("restore candidate contains a duplicate artifact")
        with _open_safe_regular(self.backup_root, artifact.source_path) as (
            source_descriptor,
            _source_path,
            source_identity,
        ):
            destination_descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
            )
            digest = hashlib.sha256()
            size = 0
            try:
                while chunk := os.read(source_descriptor, _CHUNK_SIZE):
                    self._check_deadline(deadline)
                    size += len(chunk)
                    if size > artifact.size_bytes:
                        raise RealRecoveryIntegrityError("restore source exceeds declared size")
                    digest.update(chunk)
                    view = memoryview(chunk)
                    while view:
                        written = os.write(destination_descriptor, view)
                        view = view[written:]
                os.fsync(destination_descriptor)
            finally:
                os.close(destination_descriptor)
            if (
                size != artifact.size_bytes
                or digest.hexdigest() != artifact.sha256
                or os.fstat(source_descriptor).st_ino != source_identity.st_ino
            ):
                raise RealRecoveryIntegrityError("restore source hash or size differs")
        _fsync_directory(destination.parent)

    @staticmethod
    def _verify_artifact_contracts(
        *,
        target: RealRecoveryTargetManifest,
        candidate: Path,
        meter: _VerificationMeter,
    ) -> None:
        by_role = {item.logical_role: item for item in target.artifacts}
        for artifact in target.artifacts:
            meter.check_deadline()
            path = candidate.joinpath(*PurePosixPath(artifact.restore_path).parts)
            with _open_safe_regular(candidate, artifact.restore_path) as (
                descriptor,
                _opened_path,
                opened,
            ):
                size, digest = _hash_descriptor(
                    descriptor,
                    max_bytes=artifact.size_bytes,
                    check=meter.check_deadline,
                )
                first_identity = (
                    opened.st_dev,
                    opened.st_ino,
                    opened.st_size,
                    opened.st_mtime_ns,
                    opened.st_ctime_ns,
                )
            if (size, digest) != (artifact.size_bytes, artifact.sha256):
                raise RealRecoveryIntegrityError("restored artifact hash or size differs")
            meter.add_artifact_bytes(size)
            relations, identity = _isolated_contract_for_path(
                path=path,
                kind=artifact.kind,
                relations=tuple(item.relation_name for item in artifact.relations),
                generation_id=artifact.generation_id,
                price_basis=artifact.price_basis,
                meter=meter,
            )
            with _open_safe_regular(candidate, artifact.restore_path) as (
                descriptor,
                _reopened_path,
                reopened,
            ):
                second_size, second_digest = _hash_descriptor(
                    descriptor,
                    max_bytes=artifact.size_bytes,
                    check=meter.check_deadline,
                )
                second_identity = (
                    reopened.st_dev,
                    reopened.st_ino,
                    reopened.st_size,
                    reopened.st_mtime_ns,
                    reopened.st_ctime_ns,
                )
            if second_identity != first_identity or (second_size, second_digest) != (
                artifact.size_bytes,
                artifact.sha256,
            ):
                raise RealRecoveryIntegrityError(
                    "restored artifact changed across contract verification"
                )
            if (
                relations != artifact.relations
                or canonical_sha256(identity) != artifact.contract_sha256
            ):
                raise RealRecoveryIntegrityError("restored artifact contract identity differs")

        lake_manifests = tuple(
            item
            for item in target.artifacts
            if item.kind is RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST
        )
        for artifact in lake_manifests:
            meter.check_deadline()
            manifest_path = candidate / artifact.restore_path
            manifest = ResearchPartitionManifest.model_validate_json(
                _read_bounded_json(
                    manifest_path,
                    max_bytes=meter.budget.max_json_bytes,
                    meter=meter,
                )
            )
            lake_root = candidate / "research" / "lake"
            object_role = artifact.references.get("parquet")
            if object_role is None:
                raise RealRecoveryIntegrityError("lake manifest does not bind its Parquet object")
            lake_object = by_role[object_role]
            expected_object_path = Path("research/lake") / manifest.relative_path
            if (
                lake_object.kind is not RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT
                or Path(lake_object.restore_path) != expected_object_path
                or lake_object.sha256 != manifest.file_hash
                or lake_object.size_bytes != manifest.file_size
            ):
                raise RealRecoveryIntegrityError("lake manifest does not bind its Parquet object")
            _verify_bounded_research_partition(
                path=lake_root / manifest.relative_path,
                manifest=manifest,
                as_of=target.as_of,
                meter=meter,
            )

        lab_manifests = tuple(
            item
            for item in target.artifacts
            if item.kind is RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST
        )
        for artifact in lab_manifests:
            meter.check_deadline()
            raw = _read_bounded_json(
                candidate / artifact.restore_path,
                max_bytes=meter.budget.max_json_bytes,
                meter=meter,
            )
            parsed = strict_canonical_json_loads(raw)
            try:
                from rquant.lab_artifacts import LabJobArtifactManifest
                from rquant.lab_worker import LabShardResultManifest

                if not isinstance(parsed, dict):
                    raise ValueError("manifest root is not an object")
                if "claim_token" in parsed:
                    manifest = LabShardResultManifest.model_validate(parsed)
                    declared = {
                        item.file_name: (item.file_size, item.file_sha256)
                        for item in manifest.artifacts
                    }
                else:
                    manifest = LabJobArtifactManifest.model_validate(parsed)
                    declared = {
                        item.relative_path: (item.size, item.sha256) for item in manifest.files
                    }
            except Exception as exc:
                raise RealRecoveryIntegrityError(
                    "lab artifact manifest contract is invalid"
                ) from exc
            referenced = {
                key.removeprefix("file:"): by_role[role]
                for key, role in artifact.references.items()
                if key.startswith("file:")
            }
            if set(referenced) != set(declared):
                raise RealRecoveryIntegrityError("lab artifact manifest inventory differs")
            for name, expected in declared.items():
                item = referenced[name]
                if (item.size_bytes, item.sha256) != expected:
                    raise RealRecoveryIntegrityError("lab artifact object differs from manifest")

        catalog_artifacts = tuple(
            item
            for item in target.artifacts
            if item.kind
            in {
                RealRecoveryArtifactKind.RESEARCH_CATALOG,
                RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
            }
        )
        for artifact in catalog_artifacts:
            meter.check_deadline()
            path = candidate / artifact.restore_path
            with TemporaryDirectory(prefix="rquant-recovery-catalog-") as temporary:
                connection = duckdb.connect(
                    str(path),
                    read_only=True,
                    config={
                        "temp_directory": temporary,
                        "threads": "1",
                        "memory_limit": f"{meter.budget.duckdb_memory_bytes}B",
                        "max_temp_directory_size": f"{meter.budget.duckdb_temp_bytes}B",
                    },
                )
                deadline_timer, deadline_interrupted = _duckdb_deadline_timer(connection, meter)
                try:
                    catalog_count = int(
                        connection.execute("SELECT COUNT(*) FROM research_partition").fetchone()[0]
                    )
                    if catalog_count > meter.budget.max_relation_rows:
                        raise RealRecoveryIntegrityError(
                            "research catalog row count exceeds budget"
                        )
                    referenced_roles = {
                        role
                        for role in artifact.references.values()
                        if by_role[role].kind is RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST
                    }
                    if referenced_roles and catalog_count != len(referenced_roles):
                        raise RealRecoveryIntegrityError(
                            "research catalog and lake manifest inventory differ"
                        )
                    for role in sorted(referenced_roles):
                        meter.check_deadline()
                        expected = ResearchPartitionManifest.model_validate_json(
                            _read_bounded_json(
                                candidate / by_role[role].restore_path,
                                max_bytes=meter.budget.max_json_bytes,
                                meter=meter,
                            )
                        )
                        row = connection.execute(
                            """
                            SELECT manifest_json FROM research_partition
                            WHERE partition_id = ?
                            """,
                            [expected.partition.partition_id],
                        ).fetchone()
                        if row is None:
                            raise RealRecoveryIntegrityError(
                                "research catalog is missing a lake partition"
                            )
                        raw = str(row[0]).encode("utf-8")
                        meter.check_json_bytes(raw)
                        observed = ResearchPartitionManifest.model_validate_json(raw)
                        if observed != expected:
                            raise RealRecoveryIntegrityError(
                                "research catalog manifest differs from lake authority"
                            )
                except duckdb.Error as exc:
                    if deadline_interrupted.is_set():
                        raise RealRecoveryIntegrityError(
                            "research catalog relation deadline exceeded"
                        ) from exc
                    raise RealRecoveryIntegrityError(
                        "research catalog relation verification failed"
                    ) from exc
                finally:
                    _stop_deadline_timer(deadline_timer)
                    connection.close()
        for artifact in catalog_artifacts:
            authority = artifact.references.get("authority")
            if authority is not None and artifact.relations != by_role[authority].relations:
                raise RealRecoveryIntegrityError("readonly research catalog differs from authority")

        serving_pointer = next(
            (
                item
                for item in target.artifacts
                if item.kind is RealRecoveryArtifactKind.SERVING_CURRENT
            ),
            None,
        )
        if serving_pointer is not None:
            meter.check_deadline()
            serving_root = (candidate / serving_pointer.restore_path).parent
            with ServingReader(serving_root).acquire_generation() as lease:
                manifest_role = serving_pointer.references.get("manifest")
                if (
                    manifest_role is None
                    or lease.manifest.generation_id != by_role[manifest_role].generation_id
                ):
                    raise RealRecoveryIntegrityError("serving pointer and manifest differ")
                manifest_artifact = by_role[manifest_role]
                reference_role = manifest_artifact.references.get("reference")
                if reference_role is None:
                    raise RealRecoveryIntegrityError("serving manifest has no reference relation")
                expected_reference = by_role[reference_role]
                if (
                    lease.manifest.source_generations.get("reference_slow")
                    != expected_reference.generation_id
                    or manifest_artifact.price_basis != expected_reference.price_basis
                ):
                    raise RealRecoveryIntegrityError(
                        "serving/reference generation or price basis differs"
                    )

    def _append_receipt(self, receipt: RealRecoveryReceipt) -> None:
        assert receipt.receipt_id is not None
        payload = canonical_json_bytes(receipt.model_dump(mode="json"))
        for root in (self.receipts_root, self.audits_root):
            path = root / f"{receipt.receipt_id}.json"
            if path.exists():
                if path.read_bytes() != payload:
                    raise RealRecoveryIntegrityError("immutable recovery receipt conflicts")
                continue
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
            )
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(descriptor, view)
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.chmod(path, _IMMUTABLE_FILE_MODE)
            _fsync_directory(root)

    def restore(
        self,
        *,
        target: RealRecoveryTargetManifest,
        tool_bundle: RecoveryToolVerifierBundle,
        fault_hook: Callable[[str], None] | None = None,
        publication_fence: Callable[[str], None] | None = None,
    ) -> RealRecoveryReceipt:
        started_at = datetime.now(UTC)
        started = time.monotonic()
        deadline = started + self.deadline_seconds
        operation_id = uuid.uuid4().hex
        previous: RecoveryCurrentPointer | None = None
        candidate: Path | None = None

        def check_fence(stage: str) -> None:
            self._check_deadline(deadline)
            if publication_fence is not None:
                publication_fence(stage)

        with self._lock():
            try:
                self._validate_bundle(target=target, tool_bundle=tool_bundle)
                self._preflight_sources(target=target, deadline=deadline)
                previous = self._current()
                existing = self.generations_root / str(target.manifest_id)
                if existing.exists():
                    if existing.is_symlink() or not existing.is_dir():
                        raise RealRecoveryIntegrityError("existing recovery generation is unsafe")
                    candidate = existing
                    meter = _VerificationMeter(
                        RecoveryVerificationBudget(
                            max_artifacts=self.max_artifacts,
                            max_total_bytes=self.max_total_bytes,
                            deadline_seconds=max(0.001, deadline - time.monotonic()),
                        ),
                        deadline=deadline,
                        cancelled=self.cancelled,
                    )
                    self._verify_artifact_contracts(
                        target=target,
                        candidate=candidate,
                        meter=meter,
                    )
                else:
                    candidate = self.candidates_root / operation_id
                    candidate.mkdir(mode=_PRIVATE_DIR_MODE)
                    for artifact in target.artifacts:
                        self._copy_artifact(
                            artifact=artifact, candidate=candidate, deadline=deadline
                        )
                    if fault_hook is not None:
                        fault_hook("after_copy")
                    meter = _VerificationMeter(
                        RecoveryVerificationBudget(
                            max_artifacts=self.max_artifacts,
                            max_total_bytes=self.max_total_bytes,
                            deadline_seconds=max(0.001, deadline - time.monotonic()),
                        ),
                        deadline=deadline,
                        cancelled=self.cancelled,
                    )
                    self._verify_artifact_contracts(
                        target=target,
                        candidate=candidate,
                        meter=meter,
                    )
                    if fault_hook is not None:
                        fault_hook("after_verify")
                production = next(
                    item
                    for item in target.artifacts
                    if item.kind is RealRecoveryArtifactKind.PRODUCTION_DUCKDB
                )
                generation_manifest_path = candidate / "recovery-generation-manifest.json"
                generation_manifest_payload = canonical_json_bytes(target.model_dump(mode="json"))
                if generation_manifest_path.exists():
                    if (
                        _read_bounded_json(
                            generation_manifest_path,
                            max_bytes=meter.budget.max_json_bytes,
                            meter=meter,
                        )
                        != generation_manifest_payload
                    ):
                        raise RealRecoveryIntegrityError(
                            "recovery generation manifest differs from target"
                        )
                else:
                    _write_atomic(generation_manifest_path, generation_manifest_payload)
                fixed_replays = _run_fixed_replay_with_deadline(
                    verifier=self.fixed_replay_verifier,
                    target_root=candidate,
                    dataset_path=candidate / production.restore_path,
                    meter=meter,
                )
                if {item.strategy_id for item in fixed_replays} != {
                    "n_shape",
                    "growth_board_surge",
                    "auction_gap",
                }:
                    raise RealRecoveryIntegrityError("fixed replay did not cover all strategies")
                if candidate.parent == self.candidates_root:
                    check_fence("before_generation_publish")
                    os.replace(candidate, existing)
                    candidate = existing
                    for directory, subdirectories, files in os.walk(candidate, topdown=False):
                        for name in files:
                            os.chmod(Path(directory) / name, _IMMUTABLE_FILE_MODE)
                        for name in subdirectories:
                            os.chmod(Path(directory) / name, _IMMUTABLE_DIR_MODE)
                    os.chmod(candidate, _IMMUTABLE_DIR_MODE)
                    _fsync_directory(self.generations_root)
                    check_fence("after_generation_publish")
                if fault_hook is not None:
                    fault_hook("before_current")
                pointer = RecoveryCurrentPointer(
                    generation_id=str(target.manifest_id),
                    generation_path=f"generations/{target.manifest_id}",
                    target_commit=target.target_commit,
                    target_profile_generation=target.target_profile_generation,
                    previous_generation_id=(
                        previous.generation_id
                        if previous is not None and previous.generation_id != target.manifest_id
                        else None
                    ),
                    published_at=datetime.now(UTC),
                )
                intent = _RecoveryPublicationIntent(
                    operation_id=operation_id,
                    manifest_id=str(target.manifest_id),
                    previous_pointer=previous,
                    created_at=datetime.now(UTC),
                )
                _write_atomic(
                    self.intent_path,
                    canonical_json_bytes(intent.model_dump(mode="json")),
                )
                check_fence("before_current_publish")
                _write_atomic(
                    self.current_path,
                    canonical_json_bytes(pointer.model_dump(mode="json")),
                )
                check_fence("after_current_publish")
                if fault_hook is not None:
                    fault_hook("after_current")
                receipt = RealRecoveryReceipt(
                    operation_id=operation_id,
                    status="succeeded",
                    manifest_id=str(target.manifest_id),
                    tool_bundle_id=str(tool_bundle.bundle_id),
                    target_commit=target.target_commit,
                    target_profile_generation=target.target_profile_generation,
                    previous_generation_id=previous.generation_id if previous is not None else None,
                    published_generation_id=str(target.manifest_id),
                    fixed_replays=fixed_replays,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                )
                check_fence("before_completion_receipt")
                self._append_receipt(receipt)
                self._remove_durable(self.intent_path)
                return receipt
            except BaseException as exc:
                with suppress(Exception):
                    self._recover_interrupted_publication()
                if (
                    candidate is not None
                    and candidate.parent == self.candidates_root
                    and candidate.exists()
                ):
                    failed = self.failed_root / operation_id
                    with suppress(OSError):
                        os.replace(candidate, failed)
                        _fsync_directory(self.failed_root)
                receipt = RealRecoveryReceipt(
                    operation_id=operation_id,
                    status="failed",
                    manifest_id=str(target.manifest_id),
                    tool_bundle_id=str(tool_bundle.bundle_id),
                    target_commit=target.target_commit,
                    target_profile_generation=target.target_profile_generation,
                    previous_generation_id=previous.generation_id if previous is not None else None,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                    error_type=type(exc).__name__,
                    error_message=str(exc) or type(exc).__name__,
                )
                if publication_fence is None:
                    self._append_receipt(receipt)
                else:
                    with suppress(Exception):
                        publication_fence("before_failure_receipt")
                        self._append_receipt(receipt)
                if isinstance(exc, RealRecoveryIntegrityError):
                    raise
                raise RealRecoveryIntegrityError(f"real recovery failed: {exc}") from exc


def load_verified_real_recovery_receipt(
    *,
    restore_root: Path,
    receipt_id: str,
    target: RealRecoveryTargetManifest,
    verification_budget: RecoveryVerificationBudget | None = None,
) -> tuple[RecoveryCurrentPointer, RealRecoveryReceipt]:
    """Fast verify current bytes/contracts and existing fixed-replay evidence.

    Fast verification re-hashes every artifact and recomputes schema/relation/reference
    contracts under hard budgets. It validates the three fixed replay receipts already
    committed by a full rehearsal, but deliberately does not execute the strategies.
    """

    current, receipt, _generation, _meter = _load_verified_generation(
        restore_root=restore_root,
        receipt_id=receipt_id,
        target=target,
        verification_budget=verification_budget,
    )
    return current, receipt


def _load_verified_generation(
    *,
    restore_root: Path,
    receipt_id: str,
    target: RealRecoveryTargetManifest,
    verification_budget: RecoveryVerificationBudget | None,
) -> tuple[RecoveryCurrentPointer, RealRecoveryReceipt, Path, _VerificationMeter]:
    try:
        validate_complete_recovery_artifact_graph(
            target.artifacts,
            production_artifact_role=target.production_artifact_role,
            paper_ledger_artifact_role=target.paper_ledger_artifact_role,
        )
        if "paper_ledger" not in target.external_attestations:
            raise ValueError("paper ledger external head is missing")
    except ValueError as exc:
        raise RealRecoveryIntegrityError(
            "recovery target complete production role graph is invalid"
        ) from exc
    budget = verification_budget or RecoveryVerificationBudget()
    meter = _VerificationMeter(budget)

    root = Path(restore_root)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("restore_root must be an absolute canonical path")
    try:
        current = RecoveryCurrentPointer.model_validate_json(
            _read_bounded_json(
                root / "current.json",
                max_bytes=budget.max_json_bytes,
                meter=meter,
            )
        )
        receipt = RealRecoveryReceipt.model_validate_json(
            _read_bounded_json(
                root / "receipts" / f"{receipt_id}.json",
                max_bytes=budget.max_json_bytes,
                meter=meter,
            )
        )
    except Exception as exc:
        raise RealRecoveryIntegrityError("published recovery receipt is invalid") from exc
    if (
        receipt.status != "succeeded"
        or receipt.receipt_id != receipt_id
        or receipt.published_generation_id != current.generation_id
        or receipt.manifest_id != current.generation_id
        or receipt.target_commit != current.target_commit
        or receipt.target_profile_generation != current.target_profile_generation
        or target.manifest_id != current.generation_id
        or target.target_commit != current.target_commit
        or target.target_profile_generation != current.target_profile_generation
    ):
        raise RealRecoveryIntegrityError(
            "published recovery receipt differs from current generation"
        )
    generation = root.joinpath(*PurePosixPath(current.generation_path).parts)
    observed = os.lstat(generation)
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != _IMMUTABLE_DIR_MODE
    ):
        raise RealRecoveryIntegrityError("published recovery generation path is unsafe")
    if len(target.artifacts) > budget.max_artifacts:
        raise RealRecoveryIntegrityError("recovery artifact count exceeds budget")
    expected_files = {item.restore_path for item in target.artifacts}
    expected_files.add("recovery-generation-manifest.json")
    observed_files: set[str] = set()
    for directory, directories, files in os.walk(generation, followlinks=False):
        meter.check_deadline()
        directory_path = Path(directory)
        directory_stat = os.lstat(directory_path)
        if (
            stat.S_ISLNK(directory_stat.st_mode)
            or not stat.S_ISDIR(directory_stat.st_mode)
            or directory_stat.st_uid != os.geteuid()
            or stat.S_IMODE(directory_stat.st_mode) != _IMMUTABLE_DIR_MODE
        ):
            raise RealRecoveryIntegrityError("published recovery generation tree is unsafe")
        for name in directories:
            child = os.lstat(directory_path / name)
            if (
                stat.S_ISLNK(child.st_mode)
                or not stat.S_ISDIR(child.st_mode)
                or child.st_uid != os.geteuid()
                or stat.S_IMODE(child.st_mode) != _IMMUTABLE_DIR_MODE
            ):
                raise RealRecoveryIntegrityError("recovery generation contains an unsafe directory")
        for name in files:
            child_path = directory_path / name
            child = os.lstat(child_path)
            if (
                stat.S_ISLNK(child.st_mode)
                or not stat.S_ISREG(child.st_mode)
                or child.st_uid != os.geteuid()
                or child.st_nlink != 1
                or stat.S_IMODE(child.st_mode) != _IMMUTABLE_FILE_MODE
            ):
                raise RealRecoveryIntegrityError("recovery generation contains an unsafe file")
            relative = child_path.relative_to(generation).as_posix()
            observed_files.add(relative)
            if len(observed_files) > budget.max_artifacts + 1:
                raise RealRecoveryIntegrityError(
                    "recovery generation file inventory exceeds budget"
                )
    if observed_files != expected_files:
        raise RealRecoveryIntegrityError("recovery generation artifact inventory differs")
    generation_manifest = _read_bounded_json(
        generation / "recovery-generation-manifest.json",
        max_bytes=budget.max_json_bytes,
        meter=meter,
    )
    if generation_manifest != canonical_json_bytes(target.model_dump(mode="json")):
        raise RealRecoveryIntegrityError("recovery generation manifest differs from target")
    RealRecoveryRestorer._verify_artifact_contracts(
        target=target,
        candidate=generation,
        meter=meter,
    )
    if {item.strategy_id for item in receipt.fixed_replays} != {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }:
        raise RealRecoveryIntegrityError("recovery fixed replay evidence is incomplete")
    meter.check_deadline()
    return current, receipt, generation, meter


def load_full_verified_current_recovery_receipt(
    *,
    restore_root: Path,
    receipt_id: str,
    target: RealRecoveryTargetManifest,
    fixed_replay_verifier: FixedReplayVerifier,
    verification_budget: RecoveryVerificationBudget | None = None,
) -> tuple[RecoveryCurrentPointer, RealRecoveryReceipt]:
    """Full current-generation proof required by retention and periodic rehearsal.

    This includes fast byte/contract verification and a fresh execution of all three
    fixed strategy replays under the same hard deadline.
    """

    current, receipt, generation, meter = _load_verified_generation(
        restore_root=restore_root,
        receipt_id=receipt_id,
        target=target,
        verification_budget=verification_budget,
    )
    production = next(
        (
            item
            for item in target.artifacts
            if item.kind is RealRecoveryArtifactKind.PRODUCTION_DUCKDB
        ),
        None,
    )
    if production is None:
        raise RealRecoveryIntegrityError("recovery production DuckDB artifact is missing")
    observed = _run_fixed_replay_with_deadline(
        verifier=fixed_replay_verifier,
        target_root=generation,
        dataset_path=generation / production.restore_path,
        meter=meter,
    )
    if observed != receipt.fixed_replays:
        raise RealRecoveryIntegrityError("recovery fixed replay result differs from receipt")
    return current, receipt
