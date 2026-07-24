"""Filesystem-fenced background worker for Strategy Lab shard claims."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import signal
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotBinding
from rquant.lab_job_protocol import InvalidCommandEnvelopeError
from rquant.lab_shard_protocol import (
    LabClaimAlreadyConsumedError,
    LabClaimNotConsumedError,
    LabClaimRevokedError,
    LabClaimSpool,
    LabClaimSupersededError,
    LabReportReceipt,
    LabReportSpool,
    LabReportSpoolEntry,
    LabShardClaim,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerReport,
    LabWorkerStopped,
)
from rquant.research_gate import ResearchGateRequest, open_gated_research_store
from rquant.research_run_spec import ResearchRunSpec
from rquant.research_snapshot import ResearchExecutionSession
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    LabShardMetric,
    StrategyJobAdapterRegistry,
    default_strategy_job_adapter_registry,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_GARBAGE_LEDGER_NAME = re.compile(
    r"(?P<garbage_id>[0-9a-f]{32})-(?P<sequence>[0-2])-"
    r"(?P<state>prepared|quarantined|deferred_gc)\.json"
)
_GARBAGE_STATE_SEQUENCE = {
    "prepared": 0,
    "quarantined": 1,
    "deferred_gc": 2,
}
_GARBAGE_INTENT_NAME = re.compile(r"(?P<garbage_id>[0-9a-f]{32})-prepared-intent-v1\.json")
_GARBAGE_INTENT_TEMP_NAME = re.compile(
    r"\.prepared-intent-tmp-v1-(?P<garbage_id>[0-9a-f]{32})-[0-9a-f]{32}\.tmp"
)
_GARBAGE_DERIVED_TEMP_NAME = re.compile(r"\.derived-json-tmp-v1-[0-9a-f]{32}\.tmp")
_GARBAGE_ORPHAN_METADATA_TEMP_NAME = re.compile(
    r"\.orphan-metadata-tmp-v1-(?P<metadata_hash>[0-9a-f]{64})-[0-9a-f]{32}\.tmp"
)
_LEGACY_EMPTY_STAGING_ORPHAN_NAME = re.compile(
    r"legacy-empty-staging-(?P<staging_id>[0-9a-f]{32})"
    r"(?:-(?P<orphan_token>[0-9a-f]{32}))?"
)


def _system_clock() -> datetime:
    return datetime.now(UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("worker clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _canonical_frame_json(frame: pd.DataFrame) -> str:
    if any(not isinstance(column, str) for column in frame.columns):
        raise ValueError("artifact DataFrame columns must be strings")
    raw = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )
    payload = json.loads(raw)
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


class LabWorkerModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


class LabShardArtifactManifest(LabWorkerModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    file_name: str = Field(pattern=r"^[0-9]{3}-[a-z][a-z0-9_]*\.parquet$")
    format: Literal["parquet"] = "parquet"
    row_count: int = Field(ge=0)
    columns: tuple[str, ...]
    file_size: int = Field(gt=0)
    file_sha256: str = Field(pattern=_HASH_PATTERN)
    content_sha256: str = Field(pattern=_HASH_PATTERN)


class LabShardResultManifest(LabWorkerModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    shard_id: UUID
    claim_token: UUID
    claim_generation: int = Field(ge=1)
    scheduler_fencing_token: int = Field(ge=1)
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    artifacts: tuple[LabShardArtifactManifest, ...]
    metrics: tuple[LabShardMetric, ...] = ()

    @model_validator(mode="after")
    def validate_artifacts(self) -> LabShardResultManifest:
        names = tuple(artifact.name for artifact in self.artifacts)
        files = tuple(artifact.file_name for artifact in self.artifacts)
        if not names:
            raise ValueError("result manifest requires at least one artifact")
        if len(names) != len(set(names)) or len(files) != len(set(files)):
            raise ValueError("result manifest artifact identities must be unique")
        return self

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @property
    def manifest_hash(self) -> str:
        return _sha256_bytes(self.canonical_json().encode("utf-8"))


class LabWorkerFailure(LabWorkerModel):
    phase: Literal["claim", "session", "execute", "deadline", "fence", "seal"]
    error_type: str = Field(min_length=1)
    message: str = Field(min_length=1)

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class LabWorkerTickResult(LabWorkerModel):
    status: Literal[
        "idle",
        "succeeded",
        "failed",
        "stopped",
        "reported",
        "awaiting_receipt",
        "unknown",
    ]
    claim_token: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    report_id: UUID | None = None


class LabPreparedFileIdentity(LabWorkerModel):
    file_name: str = Field(pattern=r"^(?:manifest\.json|[0-9]{3}-[a-z][a-z0-9_]*\.parquet)$")
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)


class LabPreparedShardBundle(LabWorkerModel):
    temporary: Path | None
    manifest: LabShardResultManifest
    file_identities: tuple[LabPreparedFileIdentity, ...]
    reuses_existing: bool = False
    existing_device: int | None = Field(default=None, ge=0)
    existing_inode: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_existing_identity(self) -> LabPreparedShardBundle:
        has_identity = self.existing_device is not None and self.existing_inode is not None
        if self.reuses_existing != has_identity or self.reuses_existing != (self.temporary is None):
            raise ValueError("prepared bundle reuse identity is inconsistent")
        expected_files = {"manifest.json"} | {
            artifact.file_name for artifact in self.manifest.artifacts
        }
        observed_files = tuple(item.file_name for item in self.file_identities)
        if observed_files != tuple(sorted(observed_files)) or set(observed_files) != expected_files:
            raise ValueError("prepared bundle file identities are incomplete")
        return self


class LabReclaimInventoryEntry(LabWorkerModel):
    relative_path: str = Field(pattern=r"^(?:manifest\.json|[0-9]{3}-[a-z][a-z0-9_]*\.parquet)$")
    file_type: Literal["regular"] = "regular"
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    sha256: str = Field(pattern=_HASH_PATTERN)


class LabRegularFileIdentity(LabWorkerModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    sha256: str = Field(pattern=_HASH_PATTERN)


class LabGarbageInventoryEntry(LabWorkerModel):
    relative_path: str = Field(min_length=1)
    file_type: Literal["directory", "regular"]
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_file_identity(self) -> LabGarbageInventoryEntry:
        parts = self.relative_path.split("/")
        if self.relative_path != "." and (
            self.relative_path.startswith("/") or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("garbage inventory path is unsafe")
        has_content = self.size is not None and self.sha256 is not None
        if has_content != (self.file_type == "regular"):
            raise ValueError("garbage regular inventory requires size and hash")
        return self


class LabGarbageOwner(LabWorkerModel):
    schema_version: Literal[1] = 1
    garbage_id: UUID = UUID(int=0)
    purpose: str = Field(min_length=1)
    original_relative_path: str = Field(min_length=1)
    protocol_phase: Literal["source_identified"] = "source_identified"
    source_device: int = Field(default=0, ge=0)
    source_inode: int = Field(default=0, ge=0)
    payload_type: Literal["directory", "regular"]
    inventory: tuple[LabGarbageInventoryEntry, ...]
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabGarbageOwner:
        path_parts = self.original_relative_path.split("/")
        if self.original_relative_path.startswith("/") or any(
            part in {"", ".", ".."} for part in path_parts
        ):
            raise ValueError("garbage original path is unsafe")
        paths = tuple(entry.relative_path for entry in self.inventory)
        if not paths or paths[0] != "." or paths != tuple(sorted(paths)):
            raise ValueError("garbage inventory must be non-empty and sorted")
        if len(paths) != len(set(paths)):
            raise ValueError("garbage inventory paths must be unique")
        if self.inventory[0].file_type != self.payload_type:
            raise ValueError("garbage root inventory type conflicts with payload")
        source = self.inventory[0]
        if self.source_device and self.source_device != source.device:
            raise ValueError("garbage owner source device conflicts with inventory")
        if self.source_inode and self.source_inode != source.inode:
            raise ValueError("garbage owner source inode conflicts with inventory")
        object.__setattr__(self, "source_device", source.device)
        object.__setattr__(self, "source_inode", source.inode)
        canonical = json.dumps(
            {
                "inventory": [entry.model_dump(mode="json") for entry in self.inventory],
                "original_relative_path": self.original_relative_path,
                "payload_type": self.payload_type,
                "protocol_phase": self.protocol_phase,
                "purpose": self.purpose,
                "schema_version": self.schema_version,
                "source_device": source.device,
                "source_inode": source.inode,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        content_hash = _sha256_bytes(canonical.encode("utf-8"))
        garbage_id = uuid5(NAMESPACE_URL, f"rquant:lab-garbage:{content_hash}")
        if self.content_hash and self.content_hash != content_hash:
            raise ValueError("garbage owner content_hash conflicts with inventory")
        if self.garbage_id.int and self.garbage_id != garbage_id:
            raise ValueError("garbage_id conflicts with deterministic inventory")
        object.__setattr__(self, "content_hash", content_hash)
        object.__setattr__(self, "garbage_id", garbage_id)
        return self

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class LabGarbagePreparedIntent(LabWorkerModel):
    schema_version: Literal[1] = 1
    state: Literal["prepared"] = "prepared"
    source_relative_path: str = Field(min_length=1)
    staging_relative_path: str = Field(min_length=1)
    owner: LabGarbageOwner
    intent_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabGarbagePreparedIntent:
        expected_staging = f".garbage-v1/staging/{self.owner.garbage_id.hex}"
        if self.source_relative_path != self.owner.original_relative_path:
            raise ValueError("prepared intent source conflicts with owner")
        if self.staging_relative_path != expected_staging:
            raise ValueError("prepared intent staging conflicts with owner")
        canonical = json.dumps(
            {
                "owner": self.owner.model_dump(mode="json"),
                "schema_version": self.schema_version,
                "source_relative_path": self.source_relative_path,
                "staging_relative_path": self.staging_relative_path,
                "state": self.state,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        intent_hash = _sha256_bytes(canonical.encode("utf-8"))
        if self.intent_hash and self.intent_hash != intent_hash:
            raise ValueError("prepared intent hash conflicts with canonical content")
        object.__setattr__(self, "intent_hash", intent_hash)
        return self

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class LabGarbageOrphanMetadata(LabWorkerModel):
    schema_version: Literal[1] = 1
    reason: Literal["no_proven_source"] = "no_proven_source"
    staging_id: UUID
    orphan_token: UUID | None = None
    original_staging_relative_path: str
    orphan_relative_path: str
    expected_device: int | None = Field(default=None, ge=0)
    expected_inode: int | None = Field(default=None, ge=0)
    expected_file_type: Literal["directory"] | None = None
    expected_nlink: int | None = Field(default=None, ge=1)
    expected_empty: Literal[True] | None = None
    metadata_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabGarbageOrphanMetadata:
        expected_source = f".garbage-v1/staging/{self.staging_id.hex}"
        token_suffix = f"-{self.orphan_token.hex}" if self.orphan_token is not None else ""
        expected_orphan = (
            f".garbage-v1/intent_orphans/legacy-empty-staging-{self.staging_id.hex}{token_suffix}"
        )
        if self.original_staging_relative_path != expected_source:
            raise ValueError("orphan metadata source conflicts with staging identity")
        if self.orphan_relative_path != expected_orphan:
            raise ValueError("orphan metadata target conflicts with staging identity")
        identity_fields = (
            self.expected_device,
            self.expected_inode,
            self.expected_file_type,
            self.expected_nlink,
            self.expected_empty,
        )
        if any(value is not None for value in identity_fields) and any(
            value is None for value in identity_fields
        ):
            raise ValueError("orphan metadata expected identity is incomplete")
        canonical_payload: dict[str, object] = {
            "orphan_relative_path": self.orphan_relative_path,
            "original_staging_relative_path": self.original_staging_relative_path,
            "reason": self.reason,
            "schema_version": self.schema_version,
            "staging_id": str(self.staging_id),
        }
        if self.orphan_token is not None:
            canonical_payload["orphan_token"] = str(self.orphan_token)
        if self.expected_device is not None:
            canonical_payload.update(
                {
                    "expected_device": self.expected_device,
                    "expected_empty": self.expected_empty,
                    "expected_file_type": self.expected_file_type,
                    "expected_inode": self.expected_inode,
                    "expected_nlink": self.expected_nlink,
                }
            )
        canonical = json.dumps(
            canonical_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        metadata_hash = _sha256_bytes(canonical.encode("utf-8"))
        if self.metadata_hash and self.metadata_hash != metadata_hash:
            raise ValueError("orphan metadata hash conflicts with canonical content")
        object.__setattr__(self, "metadata_hash", metadata_hash)
        return self

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json", exclude_none=True),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class LabGarbageLedger(LabWorkerModel):
    schema_version: Literal[1] = 1
    state: Literal["prepared", "quarantined", "deferred_gc"]
    owner: LabGarbageOwner

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class LabQuarantineEntry(LabWorkerModel):
    state: Literal["prepared", "quarantined", "deferred_gc"]
    owner: LabGarbageOwner
    ledger_paths: tuple[Path, ...]
    bundle_path: Path
    retained_bytes: int = Field(ge=0)


class LabQuarantineSummary(LabWorkerModel):
    bundle_count: int = Field(ge=0)
    retained_bytes: int = Field(ge=0)


class LabReclaimLedger(LabWorkerModel):
    schema_version: Literal[2] = 2
    state: Literal["prepared", "isolated", "deferred_gc"]
    current_claim: LabShardClaim
    obsolete_claim: LabShardClaim
    manifest: LabShardResultManifest
    inventory: tuple[LabReclaimInventoryEntry, ...]
    source_name: str = Field(min_length=1)
    tombstone_name: str = Field(min_length=1)
    source_device: int = Field(ge=0)
    source_inode: int = Field(ge=1)
    quarantine_id: UUID | None = None

    @model_validator(mode="after")
    def validate_inventory(self) -> LabReclaimLedger:
        paths = tuple(entry.relative_path for entry in self.inventory)
        expected = ("manifest.json",) + tuple(
            artifact.file_name for artifact in self.manifest.artifacts
        )
        if paths != tuple(sorted(paths)) or set(paths) != set(expected):
            raise ValueError("reclaim inventory must exactly cover manifest files")
        if len(paths) != len(set(paths)):
            raise ValueError("reclaim inventory paths must be unique")
        if self.state == "deferred_gc" and self.quarantine_id is None:
            raise ValueError("deferred reclaim ledger requires quarantine identity")
        return self

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class LabSealedShardBundle(LabWorkerModel):
    path: Path
    manifest: LabShardResultManifest
    created: bool
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabPendingSuccess(LabWorkerModel):
    claim: LabShardClaim
    report: LabWorkerReport
    bundle: LabSealedShardBundle
    receipt_state: Literal["reported", "awaiting_receipt", "unknown"]

    @model_validator(mode="after")
    def validate_success_identity(self) -> LabPendingSuccess:
        if not isinstance(self.report.body, LabShardSucceeded):
            raise ValueError("pending success must contain shard_succeeded report")
        if self.report.body.result_manifest_hash != self.bundle.manifest.manifest_hash:
            raise ValueError("pending success manifest hash does not match sealed bundle")
        return self


class LabArtifactConflictError(RuntimeError):
    """A sealed shard bundle exists but is not the expected immutable result."""


class LabStopSignal:
    """Signal-handler-safe cooperative stop flag with bounded polling waits."""

    def __init__(self) -> None:
        self._requested = False

    def request(self) -> None:
        self._requested = True

    def is_set(self) -> bool:
        return self._requested

    def wait(self, timeout_seconds: float) -> bool:
        if self._requested:
            return True
        time.sleep(max(0.0, timeout_seconds))
        return self._requested


StoreFactory = Callable[[], AbstractContextManager[object]]
ReceiptWaiter = Callable[[LabWorkerReport, float, LabStopSignal], LabReportReceipt]
CodeShaProvider = Callable[[], str | None]


class LabWorker:
    def __init__(
        self,
        *,
        worker_id: str,
        claim_spool: LabClaimSpool,
        report_spool: LabReportSpool,
        artifact_root: Path,
        adapter_registry: StrategyJobAdapterRegistry | None = None,
        exploratory_store_factory: StoreFactory | None = None,
        metadata_store_factory: StoreFactory | None = None,
        research_lake_root: Path | None = None,
        heartbeat_interval_seconds: float = 30.0,
        lease_extension_seconds: int = 120,
        poll_interval_ms: int = 250,
        receipt_timeout_seconds: float = 30.0,
        receipt_waiter: ReceiptWaiter | None = None,
        verified_code_sha_provider: CodeShaProvider | None = None,
        clock: Callable[[], datetime] = _system_clock,
    ) -> None:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id must not be empty")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if lease_extension_seconds < 1 or lease_extension_seconds > 3_600:
            raise ValueError("lease_extension_seconds must be from 1 through 3600")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        if receipt_timeout_seconds <= 0:
            raise ValueError("receipt_timeout_seconds must be positive")
        self.worker_id = normalized_worker_id
        self.claim_spool = claim_spool
        self.report_spool = report_spool
        self.artifact_root = Path(artifact_root).resolve()
        self.adapter_registry = adapter_registry or default_strategy_job_adapter_registry()
        self.exploratory_store_factory = exploratory_store_factory
        self.metadata_store_factory = metadata_store_factory
        self.research_lake_root = (
            None if research_lake_root is None else Path(research_lake_root).resolve()
        )
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.lease_extension_seconds = lease_extension_seconds
        self.poll_interval_ms = poll_interval_ms
        self.receipt_timeout_seconds = receipt_timeout_seconds
        self.receipt_waiter = receipt_waiter or self._wait_for_receipt
        self.verified_code_sha_provider = verified_code_sha_provider
        self.clock = clock
        self.artifact_reclaimer = LabArtifactReclaimer(
            artifact_root=self.artifact_root,
            report_spool=self.report_spool,
        )
        self.claim_spool.set_claim_advance_hook(self.artifact_reclaimer.reclaim)
        self._stop = LabStopSignal()
        self._terminal_lock = threading.Lock()
        self._pending_success: LabPendingSuccess | None = None

    def request_stop(self) -> None:
        self._stop.request()

    def sealed_bundle_path(self, claim: LabShardClaim) -> Path:
        shard_root = (
            self.artifact_root / "jobs" / str(claim.job_id) / "shards" / str(claim.shard_id)
        )
        if not all(
            hasattr(claim, field)
            for field in (
                "scheduler_fencing_token",
                "claim_generation",
                "claim_token",
            )
        ):
            return shard_root / "accepted"
        return shard_root / "attempts" / self._attempt_name(claim)

    @staticmethod
    def _attempt_name(claim: LabShardClaim) -> str:
        return (
            f"{claim.scheduler_fencing_token:020d}-"
            f"{claim.claim_generation:020d}-{claim.claim_token}"
        )

    def _temporary_bundle_path(self, claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / ".tmp"
            / str(claim.job_id)
            / str(claim.shard_id)
            / self._attempt_name(claim)
        )

    @staticmethod
    def _parse_attempt_name(name: str) -> tuple[int, int, UUID]:
        parts = name.split("-", 2)
        if (
            len(parts) != 3
            or len(parts[0]) != 20
            or len(parts[1]) != 20
            or not parts[0].isdigit()
            or not parts[1].isdigit()
        ):
            raise LabArtifactConflictError(f"invalid temporary attempt identity: {name}")
        try:
            token = UUID(parts[2])
        except ValueError as exc:
            raise LabArtifactConflictError(f"invalid temporary attempt token: {name}") from exc
        return int(parts[0]), int(parts[1]), token

    @staticmethod
    def _assert_safe_temporary_tree(path: Path) -> None:
        root = path.lstat()
        if not stat.S_ISDIR(root.st_mode) or path.is_symlink():
            raise LabArtifactConflictError(
                f"obsolete temporary attempt is a symlink or not a directory: {path.name}"
            )
        for root, directories, files in os.walk(path, followlinks=False):
            for name in directories:
                child = Path(root) / name
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains an unsafe directory: {name}"
                    )
            for name in files:
                child = Path(root) / name
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISREG(observed.st_mode):
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains an unsafe file: {name}"
                    )
                if observed.st_nlink != 1:
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains a hard link: {name}"
                    )

    def _assert_safe_artifact_ancestors(self, path: Path) -> None:
        try:
            relative = path.relative_to(self.artifact_root)
        except ValueError as exc:
            raise LabArtifactConflictError("artifact path escapes configured root") from exc
        current = self.artifact_root
        for part in relative.parts:
            if part in {"", ".", ".."}:
                raise LabArtifactConflictError("artifact path contains traversal components")
            current /= part
            if current.is_symlink():
                raise LabArtifactConflictError(f"artifact path ancestor is a symlink: {part}")
            if os.path.lexists(current) and not current.is_dir():
                raise LabArtifactConflictError(f"artifact path ancestor is not a directory: {part}")

    def _reclaim_current_candidate_directories(
        self,
        attempt_root: Path,
        _shard_root: Path,
        claim: LabShardClaim,
    ) -> None:
        self._assert_safe_temporary_tree(attempt_root)
        for child in tuple(attempt_root.iterdir()):
            try:
                candidate_id = UUID(child.name)
            except ValueError:
                continue
            if candidate_id.hex != child.name:
                continue
            self.artifact_reclaimer.logical_delete_temporary_tree(
                child,
                current_claim=claim,
            )

    def _reclaim_obsolete_temporaries(self, claim: LabShardClaim) -> None:
        self.artifact_reclaimer.collect_garbage()
        current_root = self._temporary_bundle_path(claim)
        shard_root = current_root.parent
        self._assert_safe_artifact_ancestors(shard_root)
        if not shard_root.exists():
            return
        if shard_root.is_symlink() or not shard_root.is_dir():
            raise LabArtifactConflictError("temporary shard root is unsafe")
        for candidate in tuple(shard_root.iterdir()):
            fence, generation, token = self._parse_attempt_name(candidate.name)
            if generation > claim.claim_generation:
                continue
            if generation == claim.claim_generation:
                if (
                    fence,
                    token,
                ) != (
                    claim.scheduler_fencing_token,
                    claim.claim_token,
                ):
                    raise LabArtifactConflictError(
                        "same-generation temporary attempt has conflicting identity"
                    )
                if not self.claim_spool.is_current(claim):
                    raise LabArtifactConflictError(
                        "current temporary attempt is no longer the claim high-water"
                    )
                self._reclaim_current_candidate_directories(candidate, shard_root, claim)
                continue
            self.artifact_reclaimer.logical_delete_temporary_tree(
                candidate,
                current_claim=claim,
            )

    @staticmethod
    def _validate_receipt_identity(
        report: LabWorkerReport,
        receipt: LabReportReceipt,
    ) -> None:
        if (
            receipt.report_id != report.report_id
            or receipt.content_hash != report.content_hash
            or receipt.job_id != report.job_id
            or receipt.shard_id != report.shard_id
        ):
            raise ValueError("report receipt identity does not match published report")

    def _make_report(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    ) -> LabWorkerReport:
        return LabWorkerReport.from_claim(
            claim,
            report_id=uuid4(),
            reported_at=_utc(self.clock()),
            body=body,
        )

    def _publish_report(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    ) -> LabWorkerReport:
        report = self._make_report(claim, body)
        self.report_spool.publish(report)
        return report

    def _wait_for_receipt(
        self,
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: LabStopSignal,
    ) -> LabReportReceipt:
        timeout_at = time.monotonic() + timeout_seconds
        receipt_path = self.report_spool.ack_dir / f"{report.report_id}.json"
        while True:
            if os.path.lexists(receipt_path):
                receipt = self.report_spool.load_receipt(receipt_path)
                if (
                    receipt.report_id != report.report_id
                    or receipt.content_hash != report.content_hash
                    or receipt.job_id != report.job_id
                    or receipt.shard_id != report.shard_id
                ):
                    raise ValueError("report receipt identity does not match published report")
                return receipt
            if stop.is_set():
                raise InterruptedError("worker stop requested while waiting for report receipt")
            remaining = timeout_at - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"report receipt timed out: {report.report_id}")
            stop.wait(min(0.05, remaining))

    def _publish_and_wait(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded,
        *,
        stop: LabStopSignal,
    ) -> LabReportReceipt:
        report = self._publish_report(claim, body)
        receipt = self.receipt_waiter(report, self.receipt_timeout_seconds, stop)
        self._validate_receipt_identity(report, receipt)
        if receipt.status != "accepted":
            raise PermissionError(f"worker report rejected: {receipt.reason}")
        return receipt

    def _best_effort_report(
        self,
        claim: LabShardClaim,
        body: LabShardFailed | LabWorkerStopped,
    ) -> bool:
        try:
            self._publish_report(claim, body)
        except Exception:
            return False
        return True

    def _next_owned_claim(self) -> LabShardClaim | None:
        now = _utc(self.clock())
        for path in self.claim_spool.pending_paths():
            try:
                entry = self.claim_spool.load(path)
            except InvalidCommandEnvelopeError:
                continue
            claim = entry.claim
            try:
                marker = self.claim_spool.current(claim.job_id, claim.shard_id)
            except InvalidCommandEnvelopeError:
                continue
            if marker.claim != claim:
                if (
                    claim.worker_id == self.worker_id
                    and claim.claim_generation <= marker.claim.claim_generation
                ):
                    with suppress(InvalidCommandEnvelopeError):
                        self.claim_spool.quarantine(
                            entry,
                            reason="superseded_by_current_claim_marker",
                        )
                continue
            if claim.lease_expires_at <= now:
                continue
            if claim.worker_id != self.worker_id:
                continue
            try:
                return self.claim_spool.consume(entry)
            except (
                InvalidCommandEnvelopeError,
                LabClaimAlreadyConsumedError,
                LabClaimRevokedError,
                LabClaimSupersededError,
                OSError,
            ):
                continue
        return None

    @contextmanager
    def _open_store(self, spec: ResearchRunSpec) -> Iterator[object]:
        if spec.research_status == "exploratory":
            if self.exploratory_store_factory is None:
                raise PermissionError(
                    "exploratory worker execution requires an explicit read-only store factory"
                )
            with self.exploratory_store_factory() as store:
                yield store
            return

        identity = spec.dataset_snapshot
        if identity is None:
            raise PermissionError("formal worker execution requires dataset_snapshot")
        if self.metadata_store_factory is None or self.research_lake_root is None:
            raise PermissionError(
                "formal worker execution requires metadata store and research lake"
            )
        if self.verified_code_sha_provider is None:
            raise PermissionError("formal worker execution requires verified runtime code SHA")
        runtime_code_sha = self.verified_code_sha_provider()
        if (
            runtime_code_sha is None
            or len(runtime_code_sha) != 40
            or any(character not in "0123456789abcdef" for character in runtime_code_sha)
            or runtime_code_sha != spec.code_sha
        ):
            raise PermissionError("formal runtime clean code SHA does not match ResearchRunSpec")
        adapter = self.adapter_registry.for_spec(spec)
        request = ResearchGateRequest(
            mode="formal",
            strategy_name=adapter.snapshot_strategy_name,
            start_date=spec.parameters.start_date,
            end_date=spec.parameters.end_date,
            audit_run_id=identity.audit_run_id,
            dataset_snapshot_id=identity.snapshot_id,
            dataset_binding_hash=identity.binding_hash,
            code_commit=runtime_code_sha,
        )

        def execution_session_factory(
            binding: DatasetSnapshotBinding,
            lake_root: Path,
        ) -> AbstractContextManager[object]:
            return ResearchExecutionSession(
                binding=binding,
                lake_root=lake_root,
            )

        with open_gated_research_store(
            request,
            metadata_store_factory=self.metadata_store_factory,
            execution_session_factory=execution_session_factory,
            lake_root=self.research_lake_root,
        ) as (execution_store, _decision):
            yield execution_store

    def _heartbeat_loop(
        self,
        claim: LabShardClaim,
        finished: threading.Event,
        errors: list[Exception],
    ) -> None:
        while not finished.wait(self.heartbeat_interval_seconds):
            try:
                self._publish_report(
                    claim,
                    LabShardHeartbeat(
                        lease_extension_seconds=self.lease_extension_seconds,
                    ),
                )
            except Exception as exc:
                errors.append(exc)
                finished.set()

    def _check_deadline(self, spec: ResearchRunSpec) -> None:
        if _utc(self.clock()) >= spec.deadline:
            raise TimeoutError("ResearchRunSpec deadline reached")

    @staticmethod
    def _validate_result_identity(
        claim: LabShardClaim,
        result: LabShardExecutionResult,
    ) -> None:
        expected = (
            claim.shard_id,
            claim.spec_hash,
            claim.payload_hash,
            claim.plan_hash,
            claim.definition.adapter_id,
            claim.definition.adapter_version,
        )
        actual = (
            result.shard_id,
            result.spec_hash,
            result.payload_hash,
            result.plan_hash,
            result.adapter_id,
            result.adapter_version,
        )
        if actual != expected:
            raise ValueError("adapter result identity does not match claim")

    def _write_bundle(
        self,
        temporary: Path,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
    ) -> LabShardResultManifest:
        temporary.mkdir(parents=True, exist_ok=False)
        artifacts: list[LabShardArtifactManifest] = []
        for index, table in enumerate(result.tables):
            file_name = f"{index:03d}-{table.name}.parquet"
            path = temporary / file_name
            table.frame.to_parquet(path, index=False)
            _fsync_file(path)
            persisted = pd.read_parquet(path)
            if len(persisted) != len(table.frame) or tuple(persisted.columns) != tuple(
                table.frame.columns
            ):
                raise ValueError(f"artifact round-trip shape changed: {table.name}")
            canonical_frame = _canonical_frame_json(persisted)
            artifacts.append(
                LabShardArtifactManifest(
                    name=table.name,
                    file_name=file_name,
                    row_count=len(persisted),
                    columns=tuple(persisted.columns),
                    file_size=path.stat().st_size,
                    file_sha256=_file_sha256(path),
                    content_sha256=_sha256_bytes(canonical_frame.encode("utf-8")),
                )
            )
        manifest = LabShardResultManifest(
            job_id=claim.job_id,
            shard_id=claim.shard_id,
            claim_token=claim.claim_token,
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            spec_hash=claim.spec_hash,
            payload_hash=claim.payload_hash,
            plan_hash=claim.plan_hash,
            adapter_id=claim.definition.adapter_id,
            adapter_version=claim.definition.adapter_version,
            artifacts=tuple(artifacts),
            metrics=result.metrics,
        )
        manifest_path = temporary / "manifest.json"
        with manifest_path.open("x", encoding="utf-8", newline="") as stream:
            stream.write(manifest.canonical_json())
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(temporary)
        return manifest

    @staticmethod
    def _expected_manifest_identity(
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> bool:
        return (
            manifest.job_id,
            manifest.shard_id,
            manifest.claim_token,
            manifest.claim_generation,
            manifest.scheduler_fencing_token,
            manifest.spec_hash,
            manifest.payload_hash,
            manifest.plan_hash,
            manifest.adapter_id,
            manifest.adapter_version,
        ) == (
            claim.job_id,
            claim.shard_id,
            claim.claim_token,
            claim.claim_generation,
            claim.scheduler_fencing_token,
            claim.spec_hash,
            claim.payload_hash,
            claim.plan_hash,
            claim.definition.adapter_id,
            claim.definition.adapter_version,
        )

    def _validate_bundle(
        self,
        bundle: Path,
        claim: LabShardClaim,
    ) -> LabShardResultManifest:
        try:
            bundle_before = bundle.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("sealed shard bundle is missing") from exc
        if not stat.S_ISDIR(bundle_before.st_mode) or bundle.is_symlink():
            raise LabArtifactConflictError("sealed shard bundle is not a regular directory")
        manifest_path = bundle / "manifest.json"
        try:
            manifest_before = manifest_path.lstat()
            if not stat.S_ISREG(manifest_before.st_mode):
                raise LabArtifactConflictError("sealed result manifest is not regular")
            if manifest_before.st_nlink != 1:
                raise LabArtifactConflictError("sealed result manifest has an external hard link")
            raw = manifest_path.read_text(encoding="utf-8")
            manifest_after = manifest_path.lstat()
            if (
                manifest_after.st_dev,
                manifest_after.st_ino,
                manifest_after.st_size,
                manifest_after.st_nlink,
            ) != (
                manifest_before.st_dev,
                manifest_before.st_ino,
                manifest_before.st_size,
                1,
            ):
                raise LabArtifactConflictError("sealed result manifest changed while validating")
            manifest = LabShardResultManifest.model_validate_json(raw)
        except LabArtifactConflictError:
            raise
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid sealed result manifest: {exc}") from exc
        if raw != manifest.canonical_json():
            raise LabArtifactConflictError("sealed result manifest is not canonical JSON")
        if not self._expected_manifest_identity(claim, manifest):
            raise LabArtifactConflictError("sealed result manifest identity conflicts with claim")
        expected_files = {"manifest.json"} | {artifact.file_name for artifact in manifest.artifacts}
        actual_files = {child.name for child in bundle.iterdir()}
        if actual_files != expected_files:
            unexpected = sorted(actual_files - expected_files)
            missing = sorted(expected_files - actual_files)
            raise LabArtifactConflictError(
                f"sealed bundle has unexpected={unexpected} missing={missing} files"
            )
        for artifact in manifest.artifacts:
            path = bundle / artifact.file_name
            if path.parent != bundle or path.is_symlink() or not path.is_file():
                raise LabArtifactConflictError(
                    f"sealed artifact path is missing or unsafe: {artifact.file_name}"
                )
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise LabArtifactConflictError(
                    f"sealed artifact has an external hard link: {artifact.file_name}"
                )
            if before.st_size != artifact.file_size or _file_sha256(path) != artifact.file_sha256:
                raise LabArtifactConflictError(
                    f"sealed artifact bytes conflict: {artifact.file_name}"
                )
            frame = pd.read_parquet(path)
            if len(frame) != artifact.row_count or tuple(frame.columns) != artifact.columns:
                raise LabArtifactConflictError(
                    f"sealed artifact shape conflicts: {artifact.file_name}"
                )
            content_hash = _sha256_bytes(_canonical_frame_json(frame).encode("utf-8"))
            if content_hash != artifact.content_sha256:
                raise LabArtifactConflictError(
                    f"sealed artifact content conflicts: {artifact.file_name}"
                )
            after = path.lstat()
            if (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_nlink,
            ) != (before.st_dev, before.st_ino, before.st_size, 1):
                raise LabArtifactConflictError(
                    f"sealed artifact changed while validating: {artifact.file_name}"
                )
        bundle_after = bundle.lstat()
        if (bundle_after.st_dev, bundle_after.st_ino) != (
            bundle_before.st_dev,
            bundle_before.st_ino,
        ):
            raise LabArtifactConflictError("sealed shard bundle changed while validating")
        return manifest

    def _cleanup_temporary(self, temporary: Path) -> None:
        if not os.path.lexists(temporary):
            return
        self.artifact_reclaimer.logical_quarantine_tree(
            temporary,
            purpose="worker candidate temporary cleanup",
        )

    @staticmethod
    def _prepared_file_identities(
        bundle: Path,
        manifest: LabShardResultManifest,
    ) -> tuple[LabPreparedFileIdentity, ...]:
        names = sorted(
            ("manifest.json",) + tuple(artifact.file_name for artifact in manifest.artifacts)
        )
        identities: list[LabPreparedFileIdentity] = []
        for name in names:
            path = bundle / name
            observed = path.lstat()
            if not stat.S_ISREG(observed.st_mode) or path.is_symlink():
                raise LabArtifactConflictError(f"prepared bundle file is unsafe: {name}")
            if observed.st_nlink != 1:
                raise LabArtifactConflictError(
                    f"prepared bundle file has an external hard link: {name}"
                )
            identities.append(
                LabPreparedFileIdentity(
                    file_name=name,
                    device=observed.st_dev,
                    inode=observed.st_ino,
                    size=observed.st_size,
                )
            )
        actual = {child.name for child in bundle.iterdir()}
        if actual != set(names):
            raise LabArtifactConflictError("prepared bundle file inventory changed")
        return tuple(identities)

    @staticmethod
    def _bundle_file_identity(path: Path) -> tuple[int, int]:
        file_stat = os.lstat(path)
        if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISDIR(file_stat.st_mode):
            raise LabArtifactConflictError("sealed shard bundle is not a regular directory")
        return file_stat.st_dev, file_stat.st_ino

    def _prepare_result(
        self,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
    ) -> LabPreparedShardBundle:
        self._validate_result_identity(claim, result)
        sealed = self.sealed_bundle_path(claim)
        temporary_root = self._temporary_bundle_path(claim)
        self._assert_safe_artifact_ancestors(temporary_root)
        self._assert_safe_artifact_ancestors(sealed.parent)
        sealed.parent.mkdir(parents=True, exist_ok=True)
        _fsync_directory(sealed.parent)
        temporary = temporary_root / uuid4().hex
        try:
            self._write_bundle(temporary, claim, result)
            candidate = self._validate_bundle(temporary, claim)
            candidate_files = self._prepared_file_identities(temporary, candidate)
            if sealed.exists() or sealed.is_symlink():
                existing = self._validate_bundle(sealed, claim)
                if existing.manifest_hash != candidate.manifest_hash:
                    raise LabArtifactConflictError(
                        "same attempt produced a conflicting result manifest"
                    )
                device, inode = self._bundle_file_identity(sealed)
                existing_files = self._prepared_file_identities(sealed, existing)
                self._cleanup_temporary(temporary)
                return LabPreparedShardBundle(
                    temporary=None,
                    manifest=existing,
                    file_identities=existing_files,
                    reuses_existing=True,
                    existing_device=device,
                    existing_inode=inode,
                )
            return LabPreparedShardBundle(
                temporary=temporary,
                manifest=candidate,
                file_identities=candidate_files,
            )
        except BaseException:
            self._cleanup_temporary(temporary)
            raise

    def _discard_prepared(self, prepared: LabPreparedShardBundle | None) -> None:
        if prepared is not None and prepared.temporary is not None:
            self._cleanup_temporary(prepared.temporary)

    def _assert_publish_boundary(
        self,
        claim: LabShardClaim,
        *,
        deadline: datetime | None,
        effective_expiry: datetime | None,
        require_current_claim: bool,
    ) -> None:
        if self._stop.is_set():
            raise InterruptedError("worker stop requested before success point-of-no-return")
        now = _utc(self.clock())
        if deadline is not None and now >= deadline:
            raise TimeoutError("ResearchRunSpec deadline reached before success publish")
        if effective_expiry is not None and now >= effective_expiry:
            raise PermissionError("accepted heartbeat lease expired before success publish")
        if require_current_claim and not self.claim_spool.is_current(claim):
            raise PermissionError("claim is no longer the durable shard high-water")

    def _rollback_sealed(
        self,
        claim: LabShardClaim,
        bundle: LabSealedShardBundle,
    ) -> None:
        if not bundle.created or not os.path.lexists(bundle.path):
            return
        self._validate_bundle(bundle.path, claim)
        device, inode = self._bundle_file_identity(bundle.path)
        if (device, inode) != (bundle.device, bundle.inode):
            raise LabArtifactConflictError(
                "sealed bundle changed identity before compensating rollback"
            )
        self.artifact_reclaimer.logical_quarantine_tree(
            bundle.path,
            purpose=(
                "sealed rollback "
                f"job={claim.job_id} shard={claim.shard_id} "
                f"generation={claim.claim_generation} token={claim.claim_token}"
            ),
        )

    def _publish_candidate(
        self,
        claim: LabShardClaim,
        prepared: LabPreparedShardBundle,
        *,
        deadline: datetime | None,
        effective_expiry: datetime | None,
        validate_concurrent_race: bool,
    ) -> LabSealedShardBundle:
        sealed = self.sealed_bundle_path(claim)
        self._assert_publish_boundary(
            claim,
            deadline=deadline,
            effective_expiry=effective_expiry,
            require_current_claim=effective_expiry is not None,
        )
        if prepared.reuses_existing:
            if (
                self._prepared_file_identities(sealed, prepared.manifest)
                != prepared.file_identities
            ):
                raise LabArtifactConflictError(
                    "sealed bundle files changed after candidate validation"
                )
            device, inode = self._bundle_file_identity(sealed)
            if (device, inode) != (prepared.existing_device, prepared.existing_inode):
                raise LabArtifactConflictError("sealed bundle changed after candidate validation")
            self._assert_publish_boundary(
                claim,
                deadline=deadline,
                effective_expiry=effective_expiry,
                require_current_claim=effective_expiry is not None,
            )
            return LabSealedShardBundle(
                path=sealed,
                manifest=prepared.manifest,
                created=False,
                device=device,
                inode=inode,
            )

        temporary = prepared.temporary
        if temporary is None:  # pragma: no cover - enforced by prepared model
            raise RuntimeError("new prepared bundle has no temporary path")
        created_bundle: LabSealedShardBundle | None = None
        try:
            if (
                self._prepared_file_identities(temporary, prepared.manifest)
                != prepared.file_identities
            ):
                raise LabArtifactConflictError(
                    "prepared bundle files changed before atomic publish"
                )
            try:
                os.rename(temporary, sealed)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                if not validate_concurrent_race:
                    raise LabArtifactConflictError(
                        "sealed bundle appeared after final fence confirmation"
                    ) from exc
                existing = self._validate_bundle(sealed, claim)
                if existing.manifest_hash != prepared.manifest.manifest_hash:
                    raise LabArtifactConflictError(
                        "concurrent attempt produced a conflicting result manifest"
                    ) from exc
                device, inode = self._bundle_file_identity(sealed)
                return LabSealedShardBundle(
                    path=sealed,
                    manifest=existing,
                    created=False,
                    device=device,
                    inode=inode,
                )
            if (
                self._prepared_file_identities(sealed, prepared.manifest)
                != prepared.file_identities
            ):
                raise LabArtifactConflictError(
                    "prepared bundle files changed during atomic publish"
                )
            device, inode = self._bundle_file_identity(sealed)
            created_bundle = LabSealedShardBundle(
                path=sealed,
                manifest=prepared.manifest,
                created=True,
                device=device,
                inode=inode,
            )
            _fsync_directory(sealed.parent)
            self._assert_publish_boundary(
                claim,
                deadline=deadline,
                effective_expiry=effective_expiry,
                require_current_claim=effective_expiry is not None,
            )
            return created_bundle
        except BaseException:
            if created_bundle is not None:
                self._rollback_sealed(claim, created_bundle)
            raise
        finally:
            self._cleanup_temporary(temporary)

    def _seal_result(
        self,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
        *,
        deadline: datetime | None = None,
    ) -> LabShardResultManifest:
        prepared = self._prepare_result(claim, result)
        bundle = self._publish_candidate(
            claim,
            prepared,
            deadline=deadline,
            effective_expiry=None,
            validate_concurrent_race=True,
        )
        return bundle.manifest

    def _reuse_sealed(self, claim: LabShardClaim) -> LabShardResultManifest | None:
        sealed = self.sealed_bundle_path(claim)
        if not sealed.exists() and not sealed.is_symlink():
            return None
        return self._validate_bundle(sealed, claim)

    @staticmethod
    def _pending_tick_result(pending: LabPendingSuccess) -> LabWorkerTickResult:
        return LabWorkerTickResult(
            status=pending.receipt_state,
            claim_token=pending.claim.claim_token,
            manifest_hash=pending.bundle.manifest.manifest_hash,
            report_id=pending.report.report_id,
        )

    def _set_pending_receipt_state(
        self,
        state: Literal["reported", "awaiting_receipt", "unknown"],
    ) -> LabWorkerTickResult:
        pending = self._pending_success
        if pending is None:  # pragma: no cover - internal state invariant
            raise RuntimeError("worker has no pending success report")
        pending = pending.model_copy(update={"receipt_state": state})
        self._pending_success = pending
        return self._pending_tick_result(pending)

    def _await_pending_success(self) -> LabWorkerTickResult:
        pending = self._pending_success
        if pending is None:  # pragma: no cover - guarded by caller
            raise RuntimeError("worker has no pending success report")
        try:
            self.report_spool.publish(pending.report)
        except Exception:
            return self._set_pending_receipt_state("unknown")
        try:
            receipt = self.receipt_waiter(
                pending.report,
                self.receipt_timeout_seconds,
                self._stop,
            )
            self._validate_receipt_identity(pending.report, receipt)
        except TimeoutError:
            return self._set_pending_receipt_state("awaiting_receipt")
        except InterruptedError:
            return self._set_pending_receipt_state("reported")
        except Exception:
            return self._set_pending_receipt_state("unknown")
        if receipt.status == "rejected":
            self._rollback_sealed(pending.claim, pending.bundle)
            self._pending_success = None
            return LabWorkerTickResult(
                status="failed",
                claim_token=pending.claim.claim_token,
                manifest_hash=pending.bundle.manifest.manifest_hash,
                report_id=pending.report.report_id,
            )
        self._pending_success = None
        return LabWorkerTickResult(
            status="succeeded",
            claim_token=pending.claim.claim_token,
            manifest_hash=pending.bundle.manifest.manifest_hash,
            report_id=pending.report.report_id,
        )

    def _failure_result(
        self,
        claim: LabShardClaim,
        *,
        phase: Literal["claim", "session", "execute", "deadline", "fence", "seal"],
        error: Exception,
    ) -> LabWorkerTickResult:
        failure = LabWorkerFailure(
            phase=phase,
            error_type=type(error).__name__,
            message=str(error) or type(error).__name__,
        )
        self._best_effort_report(
            claim,
            LabShardFailed(failure_json=failure.canonical_json()),
        )
        return LabWorkerTickResult(status="failed", claim_token=claim.claim_token)

    def _stopped_result(self, claim: LabShardClaim, *, reason: str) -> LabWorkerTickResult:
        self._best_effort_report(claim, LabWorkerStopped(reason=reason))
        return LabWorkerTickResult(status="stopped", claim_token=claim.claim_token)

    def run_once(self) -> LabWorkerTickResult:
        if self._pending_success is not None:
            return self._await_pending_success()
        if self._stop.is_set():
            return LabWorkerTickResult(status="stopped")
        claim = self._next_owned_claim()
        if claim is None:
            return LabWorkerTickResult(status="idle")
        if self._stop.is_set():
            return self._stopped_result(
                claim,
                reason="worker stop requested before shard execution",
            )

        try:
            self._reclaim_obsolete_temporaries(claim)
            self.artifact_reclaimer.reclaim(claim)
        except Exception as exc:
            return self._failure_result(claim, phase="claim", error=exc)

        try:
            validated = self.adapter_registry.validate_claim(claim)
        except Exception as exc:
            return self._failure_result(claim, phase="claim", error=exc)

        try:
            self._check_deadline(validated.spec)
        except Exception as exc:
            return self._failure_result(claim, phase="deadline", error=exc)

        try:
            self.claim_spool.admit_execution(claim)
        except (
            LabClaimNotConsumedError,
            LabClaimRevokedError,
            LabClaimSupersededError,
        ):
            return self._stopped_result(
                claim,
                reason="claim revoked or superseded before shard execution",
            )
        except Exception as exc:
            return self._failure_result(claim, phase="claim", error=exc)

        finished = threading.Event()
        heartbeat_errors: list[Exception] = []
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(claim, finished, heartbeat_errors),
            name=f"lab-heartbeat-{claim.claim_token}",
            daemon=True,
        )
        heartbeat.start()
        prepared: LabPreparedShardBundle | None = None
        operation_error: Exception | None = None
        operation_phase: Literal["session", "execute", "deadline", "seal"] = "execute"
        stop_reason: str | None = None
        try:
            try:
                with self._open_store(validated.spec) as store:
                    result = self.adapter_registry.execute_shard(validated, store)
            except PermissionError as exc:
                operation_phase = "session"
                operation_error = exc
            except Exception as exc:
                operation_phase = "execute"
                operation_error = exc
            if operation_error is None:
                if self._stop.is_set():
                    stop_reason = "worker stop requested after shard execution"
                else:
                    try:
                        self._check_deadline(validated.spec)
                    except Exception as exc:
                        operation_phase = "deadline"
                        operation_error = exc
            if operation_error is None and stop_reason is None:
                try:
                    prepared = self._prepare_result(claim, result)
                    self._check_deadline(validated.spec)
                except TimeoutError as exc:
                    operation_phase = "deadline"
                    operation_error = exc
                except Exception as exc:
                    operation_phase = "seal"
                    operation_error = exc
                if self._stop.is_set():
                    stop_reason = "worker stop requested after candidate serialization"
        finally:
            finished.set()
            heartbeat.join()

        if stop_reason is not None:
            self._discard_prepared(prepared)
            return self._stopped_result(claim, reason=stop_reason)
        if operation_error is not None:
            self._discard_prepared(prepared)
            return self._failure_result(
                claim,
                phase=operation_phase,
                error=operation_error,
            )
        if heartbeat_errors:
            self._discard_prepared(prepared)
            return self._failure_result(
                claim,
                phase="fence",
                error=heartbeat_errors[0],
            )
        if self._stop.is_set():
            self._discard_prepared(prepared)
            return self._stopped_result(
                claim,
                reason="worker stop requested after candidate serialization",
            )
        if prepared is None:  # pragma: no cover - operation state invariant
            return self._failure_result(
                claim,
                phase="seal",
                error=RuntimeError("worker did not prepare a shard result"),
            )

        try:
            self._assert_publish_boundary(
                claim,
                deadline=validated.spec.deadline,
                effective_expiry=None,
                require_current_claim=True,
            )
            receipt = self._publish_and_wait(
                claim,
                LabShardHeartbeat(
                    lease_extension_seconds=self.lease_extension_seconds,
                ),
                stop=self._stop,
            )
            effective_expiry = receipt.accepted_at + timedelta(seconds=self.lease_extension_seconds)
            self._assert_publish_boundary(
                claim,
                deadline=validated.spec.deadline,
                effective_expiry=effective_expiry,
                require_current_claim=True,
            )
        except InterruptedError:
            self._discard_prepared(prepared)
            return self._stopped_result(
                claim,
                reason="worker stop requested while confirming final shard fence",
            )
        except Exception as exc:
            self._discard_prepared(prepared)
            return self._failure_result(claim, phase="fence", error=exc)

        try:
            bundle = self._publish_candidate(
                claim,
                prepared,
                deadline=validated.spec.deadline,
                effective_expiry=effective_expiry,
                validate_concurrent_race=False,
            )
        except InterruptedError:
            return self._stopped_result(
                claim,
                reason="worker stop requested at atomic shard publish boundary",
            )
        except TimeoutError as exc:
            return self._failure_result(claim, phase="deadline", error=exc)
        except Exception as exc:
            return self._failure_result(claim, phase="seal", error=exc)

        try:
            with self._terminal_lock:
                self._assert_publish_boundary(
                    claim,
                    deadline=validated.spec.deadline,
                    effective_expiry=effective_expiry,
                    require_current_claim=True,
                )
                report = self._make_report(
                    claim,
                    LabShardSucceeded(
                        result_manifest_hash=bundle.manifest.manifest_hash,
                    ),
                )
                self._pending_success = LabPendingSuccess(
                    claim=claim,
                    report=report,
                    bundle=bundle,
                    receipt_state="reported",
                )
                self.report_spool.publish(report)
        except InterruptedError:
            self._rollback_sealed(claim, bundle)
            return self._stopped_result(
                claim,
                reason="worker stop requested before success point-of-no-return",
            )
        except TimeoutError as exc:
            self._rollback_sealed(claim, bundle)
            return self._failure_result(claim, phase="deadline", error=exc)
        except Exception as exc:
            if self._pending_success is None:
                self._rollback_sealed(claim, bundle)
                return self._failure_result(claim, phase="fence", error=exc)
            return self._set_pending_receipt_state("unknown")
        return self._await_pending_success()

    def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        previous_handler: object | None = None

        def handle_stop(_signum: int, _frame: FrameType | None) -> None:
            self.request_stop()

        if install_signal_handlers and threading.current_thread() is threading.main_thread():
            previous_handler = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, handle_stop)
        try:
            while True:
                result = self.run_once()
                if result.status == "stopped" or (
                    self._stop.is_set()
                    and result.status in {"idle", "reported", "awaiting_receipt", "unknown"}
                ):
                    return
                self._stop.wait(self.poll_interval_ms / 1_000)
        finally:
            if previous_handler is not None:
                signal.signal(signal.SIGTERM, previous_handler)


class LabArtifactReclaimer:
    """Quarantine superseded bundles; physical deletion belongs to the later lifecycle GC."""

    _TOMBSTONE_NAME = re.compile(
        r"\.reclaim-v1-"
        r"(?P<fence>[0-9]{20})-"
        r"(?P<generation>[0-9]{20})-"
        r"(?P<token>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})-"
        r"(?P<manifest_hash>[0-9a-f]{64})"
    )
    _LEDGER_TEMP_NAME = re.compile(r"\.reclaim-ledger-tmp-v1-[0-9a-f]{32}\.tmp")

    def __init__(
        self,
        *,
        artifact_root: Path,
        report_spool: LabReportSpool,
    ) -> None:
        self.artifact_root = Path(artifact_root).resolve()
        self.report_spool = report_spool
        self.garbage_root = self.artifact_root / ".garbage-v1"
        self.garbage_intent_dir = self.garbage_root / "prepared_intents"
        self.garbage_intent_temp_dir = self.garbage_root / "intent_temporary"
        self.garbage_intent_orphan_dir = self.garbage_root / "intent_orphans"
        self.garbage_orphan_metadata_dir = self.garbage_root / "intent_orphans_metadata"
        self.garbage_owner_dir = self.garbage_root / "owners"
        self.garbage_ledger_dir = self.garbage_root / "ledger"
        self.garbage_staging_dir = self.garbage_root / "staging"
        self.garbage_deferred_dir = self.garbage_root / "deferred_gc"
        self.garbage_pending_dir = self.garbage_deferred_dir
        for directory in (
            self.garbage_intent_dir,
            self.garbage_intent_temp_dir,
            self.garbage_intent_orphan_dir,
            self.garbage_orphan_metadata_dir,
            self.garbage_owner_dir,
            self.garbage_ledger_dir,
            self.garbage_staging_dir,
            self.garbage_deferred_dir,
        ):
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise LabArtifactConflictError("garbage quarantine directory is unsafe")
            directory.chmod(0o700)

    @staticmethod
    def _attempt_name(claim: LabShardClaim) -> str:
        return LabWorker._attempt_name(claim)

    @staticmethod
    def _parse_attempt_name(name: str) -> tuple[int, int, UUID]:
        return LabWorker._parse_attempt_name(name)

    @staticmethod
    def _expected_manifest_identity(
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> bool:
        return LabWorker._expected_manifest_identity(claim, manifest)

    @staticmethod
    def _assert_safe_temporary_tree(path: Path) -> None:
        LabWorker._assert_safe_temporary_tree(path)

    def _assert_safe_artifact_ancestors(self, path: Path) -> None:
        LabWorker._assert_safe_artifact_ancestors(self, path)

    def _validate_bundle(
        self,
        bundle: Path,
        claim: LabShardClaim,
    ) -> LabShardResultManifest:
        return LabWorker._validate_bundle(self, bundle, claim)

    def sealed_bundle_path(self, claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / "jobs"
            / str(claim.job_id)
            / "shards"
            / str(claim.shard_id)
            / "attempts"
            / self._attempt_name(claim)
        )

    @staticmethod
    def _report_matches_attempt(
        report: LabWorkerReport,
        claim: LabShardClaim,
    ) -> bool:
        return (
            report.job_id,
            report.shard_id,
            report.claim_token,
            report.claim_generation,
            report.scheduler_fencing_token,
        ) == (
            claim.job_id,
            claim.shard_id,
            claim.claim_token,
            claim.claim_generation,
            claim.scheduler_fencing_token,
        )

    @staticmethod
    def _receipt_matches_attempt(
        receipt: LabReportReceipt,
        claim: LabShardClaim,
    ) -> bool:
        return (
            receipt.job_id,
            receipt.shard_id,
            receipt.claim_token,
            receipt.claim_generation,
            receipt.scheduler_fencing_token,
        ) == (
            claim.job_id,
            claim.shard_id,
            claim.claim_token,
            claim.claim_generation,
            claim.scheduler_fencing_token,
        )

    @classmethod
    def _tombstone_name(
        cls,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> str:
        return (
            f".reclaim-v1-{claim.scheduler_fencing_token:020d}-"
            f"{claim.claim_generation:020d}-{claim.claim_token}-"
            f"{manifest.manifest_hash}"
        )

    @classmethod
    def _parse_tombstone_name(cls, name: str) -> tuple[int, int, UUID, str]:
        match = cls._TOMBSTONE_NAME.fullmatch(name)
        if match is None:
            raise LabArtifactConflictError(f"invalid reclaim tombstone identity: {name}")
        return (
            int(match.group("fence")),
            int(match.group("generation")),
            UUID(match.group("token")),
            match.group("manifest_hash"),
        )

    def _ledger_dir(self, current_claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / ".reclaim-ledger"
            / str(current_claim.job_id)
            / str(current_claim.shard_id)
        )

    def _ledger_path(self, current_claim: LabShardClaim, tombstone_name: str) -> Path:
        return self._ledger_dir(current_claim) / f"{tombstone_name}.json"

    def _write_ledger(self, ledger: LabReclaimLedger) -> Path:
        directory = self._ledger_dir(ledger.current_claim)
        self._assert_safe_artifact_ancestors(directory)
        directory.mkdir(parents=True, exist_ok=True)
        target = self._ledger_path(ledger.current_claim, ledger.tombstone_name)
        if os.path.lexists(target):
            self._load_ledger(target)
        temporary = directory / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(ledger.canonical_json().encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            _fsync_directory(directory)
        finally:
            if os.path.lexists(temporary):
                identity = self._regular_file_identity(
                    temporary,
                    label="reclaim ledger temporary file",
                )
                self._safe_remove_regular_child(
                    temporary,
                    expected=identity,
                    label="reclaim ledger temporary file",
                )
        return target

    def _load_ledger(self, path: Path) -> LabReclaimLedger:
        try:
            before = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("reclaim ledger is missing or unsafe") from exc
        if not stat.S_ISREG(before.st_mode) or path.is_symlink():
            raise LabArtifactConflictError("reclaim ledger is missing or unsafe")
        if before.st_nlink != 1:
            raise LabArtifactConflictError("reclaim ledger has an external hard link")
        try:
            raw = path.read_text(encoding="utf-8")
            after = path.lstat()
            if (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_nlink,
            ) != (before.st_dev, before.st_ino, before.st_size, 1):
                raise LabArtifactConflictError("reclaim ledger changed while validating")
            ledger = LabReclaimLedger.model_validate_json(raw)
        except LabArtifactConflictError:
            raise
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid reclaim ledger: {exc}") from exc
        if raw != ledger.canonical_json():
            raise LabArtifactConflictError("reclaim ledger is not canonical JSON")
        if path != self._ledger_path(ledger.current_claim, ledger.tombstone_name):
            raise LabArtifactConflictError("reclaim ledger path conflicts with its identity")
        return ledger

    def _validate_ledger(
        self,
        ledger: LabReclaimLedger,
        *,
        current_claim: LabShardClaim,
        obsolete_claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> None:
        isolation_claim = ledger.current_claim
        same_shard_plan = (
            isolation_claim.job_id,
            isolation_claim.shard_id,
            isolation_claim.spec_hash,
            isolation_claim.definition,
        ) == (
            current_claim.job_id,
            current_claim.shard_id,
            current_claim.spec_hash,
            current_claim.definition,
        )
        monotonic_high_water = (
            obsolete_claim.claim_generation
            < isolation_claim.claim_generation
            <= current_claim.claim_generation
            and obsolete_claim.scheduler_fencing_token
            <= isolation_claim.scheduler_fencing_token
            <= current_claim.scheduler_fencing_token
        )
        exact_if_same_generation = (
            isolation_claim.claim_generation != current_claim.claim_generation
            or isolation_claim == current_claim
        )
        same_obsolete_attempt = (
            ledger.obsolete_claim.job_id,
            ledger.obsolete_claim.shard_id,
            ledger.obsolete_claim.spec_hash,
            ledger.obsolete_claim.definition,
            self._attempt_identity(ledger.obsolete_claim),
        ) == (
            obsolete_claim.job_id,
            obsolete_claim.shard_id,
            obsolete_claim.spec_hash,
            obsolete_claim.definition,
            self._attempt_identity(obsolete_claim),
        )
        if (
            not same_shard_plan
            or not monotonic_high_water
            or not exact_if_same_generation
            or not same_obsolete_attempt
            or ledger.manifest != manifest
            or ledger.source_name != self._attempt_name(obsolete_claim)
            or ledger.tombstone_name != self._tombstone_name(obsolete_claim, manifest)
        ):
            raise LabArtifactConflictError("reclaim ledger identity conflicts with artifact")

    @staticmethod
    def _regular_file_identity(path: Path, *, label: str) -> LabRegularFileIdentity:
        try:
            before = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(f"{label} is missing or unsafe") from exc
        if not stat.S_ISREG(before.st_mode) or path.is_symlink():
            raise LabArtifactConflictError(f"{label} is not a regular file")
        if before.st_nlink != 1:
            raise LabArtifactConflictError(f"{label} has an external hard link")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise LabArtifactConflictError(f"{label} changed while opening") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError(f"{label} changed while opening")
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            after_open = os.fstat(descriptor)
            try:
                after_path = path.lstat()
            except OSError as exc:
                raise LabArtifactConflictError(f"{label} changed while validating") from exc
            if (
                after_open.st_dev,
                after_open.st_ino,
                after_open.st_size,
                after_open.st_nlink,
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                1,
                before.st_dev,
                before.st_ino,
                before.st_size,
                1,
            ):
                raise LabArtifactConflictError(f"{label} changed while validating")
        finally:
            os.close(descriptor)
        return LabRegularFileIdentity(
            device=before.st_dev,
            inode=before.st_ino,
            size=before.st_size,
            sha256=digest.hexdigest(),
        )

    def _garbage_relative_path(self, path: Path) -> str:
        try:
            relative = path.relative_to(self.artifact_root)
        except ValueError as exc:
            raise LabArtifactConflictError("garbage source escapes artifact root") from exc
        if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            raise LabArtifactConflictError("garbage source path is unsafe")
        return relative.as_posix()

    @staticmethod
    def _inventory_regular(
        path: Path,
        *,
        relative_path: str,
        label: str,
    ) -> LabGarbageInventoryEntry:
        identity = LabArtifactReclaimer._regular_file_identity(path, label=label)
        return LabGarbageInventoryEntry(
            relative_path=relative_path,
            file_type="regular",
            device=identity.device,
            inode=identity.inode,
            size=identity.size,
            sha256=identity.sha256,
        )

    def _garbage_inventory(self, path: Path) -> tuple[LabGarbageInventoryEntry, ...]:
        root = path.lstat()
        if stat.S_ISREG(root.st_mode):
            if root.st_nlink != 1 or path.is_symlink():
                raise LabArtifactConflictError("garbage regular payload is unsafe")
            return (
                self._inventory_regular(
                    path,
                    relative_path=".",
                    label="garbage regular payload",
                ),
            )
        if not stat.S_ISDIR(root.st_mode) or path.is_symlink():
            raise LabArtifactConflictError("garbage directory payload is unsafe")
        entries: list[LabGarbageInventoryEntry] = [
            LabGarbageInventoryEntry(
                relative_path=".",
                file_type="directory",
                device=root.st_dev,
                inode=root.st_ino,
            )
        ]
        for current, directories, files in os.walk(path, followlinks=False):
            directories.sort()
            files.sort()
            current_path = Path(current)
            for name in directories:
                child = current_path / name
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                    raise LabArtifactConflictError(
                        f"garbage tree contains unsafe directory: {name}"
                    )
                entries.append(
                    LabGarbageInventoryEntry(
                        relative_path=child.relative_to(path).as_posix(),
                        file_type="directory",
                        device=observed.st_dev,
                        inode=observed.st_ino,
                    )
                )
            for name in files:
                child = current_path / name
                entries.append(
                    self._inventory_regular(
                        child,
                        relative_path=child.relative_to(path).as_posix(),
                        label=f"garbage tree file {name}",
                    )
                )
        return tuple(sorted(entries, key=lambda entry: entry.relative_path))

    def _garbage_owner(
        self,
        path: Path,
        *,
        purpose: str,
        inventory: tuple[LabGarbageInventoryEntry, ...] | None = None,
    ) -> LabGarbageOwner:
        observed = inventory or self._garbage_inventory(path)
        return LabGarbageOwner(
            purpose=" ".join(purpose.split()),
            original_relative_path=self._garbage_relative_path(path),
            payload_type=observed[0].file_type,
            inventory=observed,
        )

    @staticmethod
    def _garbage_bundle_name(owner: LabGarbageOwner) -> str:
        return owner.garbage_id.hex

    def _prepared_intent(self, owner: LabGarbageOwner) -> LabGarbagePreparedIntent:
        return LabGarbagePreparedIntent(
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )

    def _prepared_intent_path(self, garbage_id: UUID) -> Path:
        return self.garbage_intent_dir / f"{garbage_id.hex}-prepared-intent-v1.json"

    @staticmethod
    def _read_prepared_intent_file(
        path: Path,
        *,
        allowed_links: frozenset[int] = frozenset({1}),
    ) -> LabGarbagePreparedIntent:
        try:
            before = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent is missing or unsafe") from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink not in allowed_links
        ):
            raise LabArtifactConflictError("prepared intent is not an owned regular file")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent changed while opening") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink not in allowed_links
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError("prepared intent changed while opening")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after_open = os.fstat(descriptor)
            after_path = path.lstat()
            if (
                after_open.st_dev,
                after_open.st_ino,
                after_open.st_size,
                after_open.st_nlink,
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
            ):
                raise LabArtifactConflictError("prepared intent changed while validating")
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent changed while validating") from exc
        finally:
            os.close(descriptor)
        try:
            raw = b"".join(chunks).decode("utf-8")
            intent = LabGarbagePreparedIntent.model_validate_json(raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid prepared intent: {exc}") from exc
        if raw != intent.canonical_json():
            raise LabArtifactConflictError("prepared intent is not canonical JSON")
        return intent

    def _load_prepared_intent(self, path: Path) -> LabGarbagePreparedIntent:
        match = _GARBAGE_INTENT_NAME.fullmatch(path.name)
        if match is None:
            raise LabArtifactConflictError("prepared intent name is invalid")
        intent = self._read_prepared_intent_file(path)
        if intent.owner.garbage_id.hex != match.group("garbage_id"):
            raise LabArtifactConflictError("prepared intent path conflicts with owner")
        return intent

    def _isolate_intent_temporary(self, temporary: Path) -> None:
        target = self.garbage_intent_orphan_dir / temporary.name
        if os.path.lexists(target):
            raise LabArtifactConflictError("prepared intent temporary orphan conflicts")
        os.rename(temporary, target)
        _fsync_directory(self.garbage_intent_temp_dir)
        _fsync_directory(self.garbage_intent_orphan_dir)

    def _drop_published_intent_temporary(self, temporary: Path, target: Path) -> None:
        temporary_stat = temporary.lstat()
        target_stat = target.lstat()
        if (
            temporary.is_symlink()
            or target.is_symlink()
            or not stat.S_ISREG(temporary_stat.st_mode)
            or not stat.S_ISREG(target_stat.st_mode)
            or (temporary_stat.st_dev, temporary_stat.st_ino)
            != (target_stat.st_dev, target_stat.st_ino)
            or temporary_stat.st_nlink != 2
            or target_stat.st_nlink != 2
        ):
            raise LabArtifactConflictError("published intent temporary identity conflicts")
        os.unlink(temporary)
        _fsync_directory(self.garbage_intent_temp_dir)
        if target.lstat().st_nlink != 1:
            raise LabArtifactConflictError("prepared intent retained an unexpected hard link")

    def _write_prepared_intent(self, intent: LabGarbagePreparedIntent) -> Path:
        target = self._prepared_intent_path(intent.owner.garbage_id)
        if os.path.lexists(target):
            existing = self._load_prepared_intent(target)
            if existing != intent:
                raise LabArtifactConflictError("prepared intent conflicts with durable intent")
            return target
        temporary = self.garbage_intent_temp_dir / (
            f".prepared-intent-tmp-v1-{intent.owner.garbage_id.hex}-{uuid4().hex}.tmp"
        )
        linked = False
        try:
            with temporary.open("xb") as stream:
                stream.write(intent.canonical_json().encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            _fsync_directory(self.garbage_intent_temp_dir)
            try:
                os.link(temporary, target, follow_symlinks=False)
                linked = True
                _fsync_directory(self.garbage_intent_dir)
            except FileExistsError as exc:
                existing = self._load_prepared_intent(target)
                if existing != intent:
                    raise LabArtifactConflictError(
                        "prepared intent no-clobber publication found conflicting content"
                    ) from exc
            if linked:
                self._drop_published_intent_temporary(temporary, target)
            else:
                self._isolate_intent_temporary(temporary)
            if self._load_prepared_intent(target) != intent:
                raise LabArtifactConflictError("prepared intent publication changed content")
        except BaseException:
            if os.path.lexists(temporary):
                self._isolate_intent_temporary(temporary)
            raise
        return target

    def _legacy_empty_staging_orphan_metadata(
        self,
        staging_id: UUID,
        orphan_token: UUID | None,
        expected_identity: tuple[int, int, int, int] | None = None,
    ) -> LabGarbageOrphanMetadata:
        token_suffix = f"-{orphan_token.hex}" if orphan_token is not None else ""
        if expected_identity is not None and (
            expected_identity[2] != stat.S_IFDIR or expected_identity[3] < 1
        ):
            raise LabArtifactConflictError("legacy empty staging expected identity is unsafe")
        return LabGarbageOrphanMetadata(
            staging_id=staging_id,
            orphan_token=orphan_token,
            original_staging_relative_path=f".garbage-v1/staging/{staging_id.hex}",
            orphan_relative_path=(
                f".garbage-v1/intent_orphans/legacy-empty-staging-{staging_id.hex}{token_suffix}"
            ),
            expected_device=(expected_identity[0] if expected_identity is not None else None),
            expected_inode=(expected_identity[1] if expected_identity is not None else None),
            expected_file_type=("directory" if expected_identity is not None else None),
            expected_nlink=(expected_identity[3] if expected_identity is not None else None),
            expected_empty=(True if expected_identity is not None else None),
        )

    @staticmethod
    def _read_garbage_orphan_metadata_file(
        marker: Path,
        *,
        allowed_links: frozenset[int] = frozenset({1}),
    ) -> LabGarbageOrphanMetadata:
        try:
            before = marker.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("garbage orphan metadata is missing") from exc
        if (
            marker.is_symlink()
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink not in allowed_links
        ):
            raise LabArtifactConflictError("garbage orphan metadata is unsafe")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(marker, flags)
        except OSError as exc:
            raise LabArtifactConflictError("garbage orphan metadata changed while opening") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink not in allowed_links
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError("garbage orphan metadata changed while opening")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after_open = os.fstat(descriptor)
            after_path = marker.lstat()
            if (
                after_open.st_dev,
                after_open.st_ino,
                after_open.st_size,
                after_open.st_nlink,
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
            ):
                raise LabArtifactConflictError("garbage orphan metadata changed while validating")
        except OSError as exc:
            raise LabArtifactConflictError(
                "garbage orphan metadata changed while validating"
            ) from exc
        finally:
            os.close(descriptor)
        try:
            raw = b"".join(chunks).decode("utf-8")
            metadata = LabGarbageOrphanMetadata.model_validate_json(raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid garbage orphan metadata: {exc}") from exc
        if raw != metadata.canonical_json():
            raise LabArtifactConflictError("garbage orphan metadata is not canonical")
        return metadata

    def _load_garbage_orphan_metadata(self, marker: Path) -> LabGarbageOrphanMetadata:
        return self._read_garbage_orphan_metadata_file(marker)

    def _external_orphan_metadata_path(self, metadata: LabGarbageOrphanMetadata) -> Path:
        return self.garbage_orphan_metadata_dir / (
            f"{Path(metadata.orphan_relative_path).name}.json"
        )

    @staticmethod
    def _has_external_orphan_identity(metadata: LabGarbageOrphanMetadata) -> bool:
        return all(
            value is not None
            for value in (
                metadata.expected_device,
                metadata.expected_inode,
                metadata.expected_file_type,
                metadata.expected_nlink,
                metadata.expected_empty,
            )
        )

    def _load_external_orphan_metadata(self, marker: Path) -> LabGarbageOrphanMetadata:
        metadata = self._load_garbage_orphan_metadata(marker)
        if not self._has_external_orphan_identity(
            metadata
        ) or marker != self._external_orphan_metadata_path(metadata):
            raise LabArtifactConflictError("external orphan metadata identity conflicts")
        return metadata

    def _assert_external_orphan_identity(
        self,
        orphan: Path,
        metadata: LabGarbageOrphanMetadata,
    ) -> None:
        if not self._has_external_orphan_identity(metadata):
            raise LabArtifactConflictError("external orphan metadata has no expected identity")
        expected = (
            metadata.expected_device,
            metadata.expected_inode,
            stat.S_IFDIR,
            metadata.expected_nlink,
        )
        try:
            entry = orphan.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(
                "legacy empty staging orphan identity conflicts"
            ) from exc
        if (
            orphan.parent != self.garbage_intent_orphan_dir
            or stat.S_ISLNK(entry.st_mode)
            or not stat.S_ISDIR(entry.st_mode)
            or self._directory_identity(entry) != expected
        ):
            raise LabArtifactConflictError("legacy empty staging orphan identity conflicts")
        descriptor: int | None = None
        try:
            descriptor = os.open(
                orphan,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(descriptor)
            opened_identity = self._directory_identity(opened)
            if (
                stat.S_ISLNK(opened.st_mode)
                or not stat.S_ISDIR(opened.st_mode)
                or opened_identity != expected
            ):
                raise LabArtifactConflictError("legacy empty staging orphan identity conflicts")
            children = os.listdir(descriptor)
            exit_opened = os.fstat(descriptor)
            exit_path = orphan.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(
                "legacy empty staging orphan identity conflicts"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError as exc:
                    raise LabArtifactConflictError(
                        "legacy empty staging orphan identity conflicts"
                    ) from exc
        exit_opened_identity = self._directory_identity(exit_opened)
        if (
            children
            or stat.S_ISLNK(exit_opened.st_mode)
            or not stat.S_ISDIR(exit_opened.st_mode)
            or exit_opened_identity != expected
            or exit_opened_identity != opened_identity
            or stat.S_ISLNK(exit_path.st_mode)
            or not stat.S_ISDIR(exit_path.st_mode)
            or self._directory_identity(exit_path) != expected
        ):
            raise LabArtifactConflictError("legacy empty staging orphan identity conflicts")

    def _drop_published_orphan_metadata_temporary(
        self,
        temporary: Path,
        target: Path,
    ) -> None:
        temporary_stat = temporary.lstat()
        target_stat = target.lstat()
        if (
            temporary.is_symlink()
            or target.is_symlink()
            or not stat.S_ISREG(temporary_stat.st_mode)
            or not stat.S_ISREG(target_stat.st_mode)
            or (temporary_stat.st_dev, temporary_stat.st_ino)
            != (target_stat.st_dev, target_stat.st_ino)
            or temporary_stat.st_nlink != 2
            or target_stat.st_nlink != 2
        ):
            raise LabArtifactConflictError("published orphan metadata temporary conflicts")
        os.unlink(temporary)
        _fsync_directory(self.garbage_orphan_metadata_dir)
        if target.lstat().st_nlink != 1:
            raise LabArtifactConflictError("orphan metadata retained an unexpected hard link")

    def _isolate_orphan_metadata_temporary(self, temporary: Path) -> None:
        target = self.garbage_intent_orphan_dir / f".derived-json-tmp-v1-{uuid4().hex}.tmp"
        if os.path.lexists(target):
            raise LabArtifactConflictError("orphan metadata temporary isolation conflicts")
        os.rename(temporary, target)
        _fsync_directory(self.garbage_orphan_metadata_dir)
        _fsync_directory(self.garbage_intent_orphan_dir)

    def _write_external_orphan_metadata(self, metadata: LabGarbageOrphanMetadata) -> Path:
        if not self._has_external_orphan_identity(metadata):
            raise LabArtifactConflictError("external orphan metadata has no expected identity")
        target = self._external_orphan_metadata_path(metadata)
        if os.path.lexists(target):
            if self._load_external_orphan_metadata(target) != metadata:
                raise LabArtifactConflictError("external orphan metadata conflicts")
            return target
        temporary = self.garbage_orphan_metadata_dir / (
            f".orphan-metadata-tmp-v1-{metadata.metadata_hash}-{uuid4().hex}.tmp"
        )
        with temporary.open("xb") as stream:
            stream.write(metadata.canonical_json().encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(self.garbage_orphan_metadata_dir)
        try:
            os.link(temporary, target, follow_symlinks=False)
            _fsync_directory(self.garbage_orphan_metadata_dir)
        except FileExistsError as exc:
            if self._load_external_orphan_metadata(target) != metadata:
                raise LabArtifactConflictError(
                    "external orphan metadata no-clobber publication conflicts"
                ) from exc
            self._isolate_orphan_metadata_temporary(temporary)
        else:
            self._drop_published_orphan_metadata_temporary(temporary, target)
        if self._load_external_orphan_metadata(target) != metadata:
            raise LabArtifactConflictError("external orphan metadata changed after publication")
        return target

    def _ensure_legacy_empty_staging_orphan_metadata(
        self,
        orphan: Path,
        staging_id: UUID,
        orphan_token: UUID | None,
        expected_identity: tuple[int, int, int, int],
    ) -> None:
        expected = self._legacy_empty_staging_orphan_metadata(
            staging_id,
            orphan_token,
            expected_identity,
        )
        self._assert_external_orphan_identity(orphan, expected)
        self._write_external_orphan_metadata(expected)
        self._assert_external_orphan_identity(orphan, expected)

    def _reconcile_orphan_metadata_temporaries_locked(self) -> None:
        for temporary in tuple(sorted(self.garbage_orphan_metadata_dir.iterdir())):
            match = _GARBAGE_ORPHAN_METADATA_TEMP_NAME.fullmatch(temporary.name)
            if match is None:
                continue
            observed = temporary.lstat()
            if (
                temporary.is_symlink()
                or not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink not in {1, 2}
            ):
                raise LabArtifactConflictError("orphan metadata temporary is unsafe")
            metadata = self._read_garbage_orphan_metadata_file(
                temporary,
                allowed_links=frozenset({observed.st_nlink}),
            )
            if metadata.metadata_hash != match.group(
                "metadata_hash"
            ) or not self._has_external_orphan_identity(metadata):
                raise LabArtifactConflictError("orphan metadata temporary identity conflicts")
            target = self._external_orphan_metadata_path(metadata)
            if observed.st_nlink == 2:
                if not os.path.lexists(target):
                    raise LabArtifactConflictError("linked orphan metadata temporary has no target")
                target_stat = target.lstat()
                if (target_stat.st_dev, target_stat.st_ino) != (
                    observed.st_dev,
                    observed.st_ino,
                ):
                    raise LabArtifactConflictError(
                        "linked orphan metadata temporary conflicts with target"
                    )
                self._drop_published_orphan_metadata_temporary(temporary, target)
                continue
            if os.path.lexists(target):
                if self._load_external_orphan_metadata(target) != metadata:
                    raise LabArtifactConflictError(
                        "orphan metadata temporary conflicts with durable target"
                    )
                self._isolate_orphan_metadata_temporary(temporary)
                continue
            os.link(temporary, target, follow_symlinks=False)
            _fsync_directory(self.garbage_orphan_metadata_dir)
            self._drop_published_orphan_metadata_temporary(temporary, target)

    def _reconcile_intent_temporaries_locked(self) -> None:
        self._reconcile_orphan_metadata_temporaries_locked()
        external_metadata: dict[str, LabGarbageOrphanMetadata] = {}
        for marker in tuple(sorted(self.garbage_orphan_metadata_dir.iterdir())):
            if _GARBAGE_ORPHAN_METADATA_TEMP_NAME.fullmatch(marker.name) is not None:
                raise LabArtifactConflictError("orphan metadata temporary remained unreconciled")
            metadata = self._load_external_orphan_metadata(marker)
            orphan_name = Path(metadata.orphan_relative_path).name
            if orphan_name in external_metadata:
                raise LabArtifactConflictError("duplicate external orphan metadata")
            external_metadata[orphan_name] = metadata
        for orphan in tuple(sorted(self.garbage_intent_orphan_dir.iterdir())):
            observed = orphan.lstat()
            if orphan.is_symlink():
                raise LabArtifactConflictError("prepared intent orphan is unsafe")
            if stat.S_ISDIR(observed.st_mode):
                match = _LEGACY_EMPTY_STAGING_ORPHAN_NAME.fullmatch(orphan.name)
                if match is None:
                    raise LabArtifactConflictError("prepared intent orphan directory is unsafe")
                names = {child.name for child in orphan.iterdir()}
                if names == {"orphan.json"}:
                    legacy = self._load_garbage_orphan_metadata(orphan / "orphan.json")
                    expected_legacy = self._legacy_empty_staging_orphan_metadata(
                        UUID(hex=match.group("staging_id")),
                        (
                            UUID(hex=match.group("orphan_token"))
                            if match.group("orphan_token") is not None
                            else None
                        ),
                    )
                    if legacy != expected_legacy:
                        raise LabArtifactConflictError(
                            "legacy empty staging orphan metadata conflicts"
                        )
                    continue
                metadata = external_metadata.pop(orphan.name, None)
                if metadata is not None:
                    self._assert_external_orphan_identity(orphan, metadata)
                    continue
                if names:
                    raise LabArtifactConflictError(
                        "prepared intent orphan directory has unexpected metadata"
                    )
                raise LabArtifactConflictError(
                    "legacy empty staging orphan has no external metadata"
                )
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or (
                    _GARBAGE_INTENT_TEMP_NAME.fullmatch(orphan.name) is None
                    and _GARBAGE_DERIVED_TEMP_NAME.fullmatch(orphan.name) is None
                )
            ):
                raise LabArtifactConflictError("prepared intent orphan file is unsafe")
        if external_metadata:
            raise LabArtifactConflictError("external orphan metadata has no matching orphan")
        for temporary in tuple(sorted(self.garbage_intent_temp_dir.iterdir())):
            match = _GARBAGE_INTENT_TEMP_NAME.fullmatch(temporary.name)
            if match is None or temporary.is_symlink():
                raise LabArtifactConflictError("unknown prepared intent temporary")
            observed = temporary.lstat()
            if not stat.S_ISREG(observed.st_mode) or observed.st_nlink not in {1, 2}:
                raise LabArtifactConflictError("prepared intent temporary is unsafe")
            target = self.garbage_intent_dir / (
                f"{match.group('garbage_id')}-prepared-intent-v1.json"
            )
            if observed.st_nlink == 2:
                if not os.path.lexists(target):
                    raise LabArtifactConflictError("linked intent temporary has no target")
                intent = self._read_prepared_intent_file(
                    temporary,
                    allowed_links=frozenset({2}),
                )
                target_intent = self._read_prepared_intent_file(
                    target,
                    allowed_links=frozenset({2}),
                )
                if intent != target_intent or intent.owner.garbage_id.hex != match.group(
                    "garbage_id"
                ):
                    raise LabArtifactConflictError("linked intent temporary conflicts with target")
                self._drop_published_intent_temporary(temporary, target)
                continue
            try:
                intent = self._read_prepared_intent_file(temporary)
            except LabArtifactConflictError:
                self._isolate_intent_temporary(temporary)
                continue
            if intent.owner.garbage_id.hex != match.group("garbage_id"):
                raise LabArtifactConflictError("intent temporary name conflicts with content")
            if os.path.lexists(target):
                if self._load_prepared_intent(target) != intent:
                    raise LabArtifactConflictError("intent temporary conflicts with durable target")
                self._isolate_intent_temporary(temporary)
                continue
            os.link(temporary, target, follow_symlinks=False)
            _fsync_directory(self.garbage_intent_dir)
            self._drop_published_intent_temporary(temporary, target)

    def _write_derived_canonical_file(self, target: Path, payload: str) -> None:
        if os.path.lexists(target):
            raise LabArtifactConflictError("derived garbage metadata target already exists")
        temporary = self.garbage_intent_orphan_dir / (f".derived-json-tmp-v1-{uuid4().hex}.tmp")
        with temporary.open("xb") as stream:
            stream.write(payload.encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(self.garbage_intent_orphan_dir)
        if os.path.lexists(target):
            return
        os.rename(temporary, target)
        _fsync_directory(target.parent)
        _fsync_directory(self.garbage_intent_orphan_dir)

    def _garbage_ledger_path(
        self,
        owner: LabGarbageOwner,
        state: Literal["prepared", "quarantined", "deferred_gc"],
    ) -> Path:
        sequence = _GARBAGE_STATE_SEQUENCE[state]
        return self.garbage_ledger_dir / (f"{owner.garbage_id.hex}-{sequence}-{state}.json")

    def _write_garbage_ledger(
        self,
        owner: LabGarbageOwner,
        state: Literal["prepared", "quarantined", "deferred_gc"],
    ) -> Path:
        ledger = LabGarbageLedger(state=state, owner=owner)
        target = self._garbage_ledger_path(owner, state)
        if os.path.lexists(target):
            existing = self._load_garbage_ledger(target)
            if existing != ledger:
                raise LabArtifactConflictError("garbage ledger state conflicts")
            return target
        self._write_derived_canonical_file(target, ledger.canonical_json())
        if self._load_garbage_ledger(target) != ledger:
            raise LabArtifactConflictError("derived garbage ledger changed after publication")
        return target

    def _load_garbage_ledger(self, path: Path) -> LabGarbageLedger:
        identity = self._regular_file_identity(path, label="garbage ledger")
        match = _GARBAGE_LEDGER_NAME.fullmatch(path.name)
        if match is None:
            raise LabArtifactConflictError("garbage ledger name is invalid")
        try:
            raw = path.read_text(encoding="utf-8")
            ledger = LabGarbageLedger.model_validate_json(raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid garbage ledger: {exc}") from exc
        after = self._regular_file_identity(path, label="garbage ledger")
        expected_state = match.group("state")
        if (
            after != identity
            or raw != ledger.canonical_json()
            or ledger.owner.garbage_id.hex != match.group("garbage_id")
            or ledger.state != expected_state
            or _GARBAGE_STATE_SEQUENCE[ledger.state] != int(match.group("sequence"))
        ):
            raise LabArtifactConflictError("garbage ledger identity is not canonical")
        return ledger

    def _garbage_ledgers(self, owner: LabGarbageOwner) -> tuple[Path, ...]:
        paths = tuple(sorted(self.garbage_ledger_dir.glob(f"{owner.garbage_id.hex}-*.json")))
        if not paths:
            raise LabArtifactConflictError("garbage owner has no durable ledger")
        ledgers = tuple(self._load_garbage_ledger(path) for path in paths)
        for ledger in ledgers:
            if ledger.owner != owner:
                raise LabArtifactConflictError("garbage ledger owner conflicts")
        sequences = tuple(_GARBAGE_STATE_SEQUENCE[ledger.state] for ledger in ledgers)
        if sequences != tuple(range(max(sequences) + 1)):
            raise LabArtifactConflictError("garbage ledger state history is incomplete")
        return paths

    def _latest_garbage_ledger(self, owner: LabGarbageOwner) -> LabGarbageLedger:
        paths = self._garbage_ledgers(owner)
        return self._load_garbage_ledger(paths[-1])

    def _write_garbage_owner(self, bundle: Path, owner: LabGarbageOwner) -> None:
        self._ensure_global_garbage_owner(owner)
        self._ensure_bundle_garbage_owner(bundle, owner)
        self._write_garbage_ledger(owner, "prepared")

    def _ensure_global_garbage_owner(self, owner: LabGarbageOwner) -> Path:
        marker = self.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
        if os.path.lexists(marker):
            if self._load_garbage_owner(marker) != owner:
                raise LabArtifactConflictError("global garbage owner marker conflicts")
            return marker
        self._write_derived_canonical_file(marker, owner.canonical_json())
        if self._load_garbage_owner(marker) != owner:
            raise LabArtifactConflictError("global garbage owner changed after publication")
        return marker

    def _ensure_garbage_staging(self, intent: LabGarbagePreparedIntent) -> Path:
        staging = self.artifact_root / intent.staging_relative_path
        expected = self.garbage_staging_dir / intent.owner.garbage_id.hex
        if staging != expected:
            raise LabArtifactConflictError("prepared intent staging path is unsafe")
        if not os.path.lexists(staging):
            staging.mkdir(mode=0o700)
            _fsync_directory(self.garbage_staging_dir)
        observed = staging.lstat()
        if staging.is_symlink() or not stat.S_ISDIR(observed.st_mode):
            raise LabArtifactConflictError("garbage staging bundle is unsafe")
        names = {child.name for child in staging.iterdir()}
        if not names.issubset({"owner.json", "payload"}):
            raise LabArtifactConflictError(
                f"garbage staging bundle has unexpected entries: {sorted(names)}"
            )
        return staging

    def _ensure_bundle_garbage_owner(
        self,
        staging: Path,
        owner: LabGarbageOwner,
    ) -> Path:
        marker = staging / "owner.json"
        if os.path.lexists(marker):
            if self._load_garbage_owner(marker) != owner:
                raise LabArtifactConflictError("bundle garbage owner marker conflicts")
            return marker
        payload = staging / "payload"
        if os.path.lexists(payload) and self._garbage_inventory(payload) != owner.inventory:
            raise LabArtifactConflictError("staged payload conflicts with prepared intent")
        self._write_derived_canonical_file(marker, owner.canonical_json())
        if self._load_garbage_owner(marker) != owner:
            raise LabArtifactConflictError("bundle garbage owner changed after publication")
        return marker

    def _load_garbage_owner(self, marker: Path) -> LabGarbageOwner:
        identity = self._regular_file_identity(marker, label="garbage owner marker")
        try:
            raw = marker.read_text(encoding="utf-8")
            owner = LabGarbageOwner.model_validate_json(raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid garbage owner marker: {exc}") from exc
        after = self._regular_file_identity(marker, label="garbage owner marker")
        if after != identity or raw != owner.canonical_json():
            raise LabArtifactConflictError("garbage owner marker changed or is not canonical")
        return owner

    def _load_garbage_owner_ledger(self, garbage_id: UUID) -> LabGarbageOwner:
        marker = self.garbage_owner_dir / f"{garbage_id.hex}.json"
        owner = self._load_garbage_owner(marker)
        if owner.garbage_id != garbage_id:
            raise LabArtifactConflictError("garbage owner ledger identity conflicts")
        return owner

    def _validate_garbage_bundle(self, bundle: Path) -> LabGarbageOwner:
        root = bundle.lstat()
        if bundle.is_symlink() or not stat.S_ISDIR(root.st_mode):
            raise LabArtifactConflictError("garbage bundle is unsafe")
        try:
            bundle_id = UUID(hex=bundle.name)
        except ValueError as exc:
            raise LabArtifactConflictError("garbage bundle name is invalid") from exc
        names = {child.name for child in bundle.iterdir()}
        if names != {"owner.json", "payload"}:
            raise LabArtifactConflictError(
                f"garbage bundle has unexpected entries: {sorted(names)}"
            )
        owner = self._load_garbage_owner(bundle / "owner.json")
        if owner.garbage_id != bundle_id:
            raise LabArtifactConflictError("garbage bundle name conflicts with owner")
        intent = self._load_prepared_intent(self._prepared_intent_path(bundle_id))
        if intent.owner != owner:
            raise LabArtifactConflictError("garbage bundle conflicts with prepared intent")
        if self._load_garbage_owner_ledger(bundle_id) != owner:
            raise LabArtifactConflictError("garbage bundle owner conflicts with ledger")
        observed = self._garbage_inventory(bundle / "payload")
        if observed != owner.inventory:
            raise LabArtifactConflictError("garbage payload conflicts with owner inventory")
        after = bundle.lstat()
        if (after.st_dev, after.st_ino) != (root.st_dev, root.st_ino):
            raise LabArtifactConflictError("garbage bundle changed while validating")
        return owner

    def _promote_garbage_bundle(self, source: Path, target: Path) -> None:
        if os.path.lexists(target):
            raise LabArtifactConflictError("garbage state has duplicate bundle ownership")
        os.rename(source, target)
        _fsync_directory(source.parent)
        _fsync_directory(target.parent)
        if os.path.lexists(source):
            raise LabArtifactConflictError("garbage source path was replaced during promotion")

    def _staging_owner(self, staging: Path) -> LabGarbageOwner:
        root = staging.lstat()
        if staging.is_symlink() or not stat.S_ISDIR(root.st_mode):
            raise LabArtifactConflictError("garbage staging bundle is unsafe")
        names = {child.name for child in staging.iterdir()}
        if names not in ({"owner.json"}, {"owner.json", "payload"}):
            raise LabArtifactConflictError(
                f"garbage staging bundle has unexpected entries: {sorted(names)}"
            )
        owner = self._load_garbage_owner(staging / "owner.json")
        if staging.name != owner.garbage_id.hex:
            raise LabArtifactConflictError("garbage staging name conflicts with owner")
        if self._load_garbage_owner_ledger(owner.garbage_id) != owner:
            raise LabArtifactConflictError("garbage staging owner conflicts with ledger")
        self._garbage_ledgers(owner)
        return owner

    def _source_path_for_owner(self, owner: LabGarbageOwner) -> Path:
        source = self.artifact_root / owner.original_relative_path
        if source.parent == source or not source.is_relative_to(self.artifact_root):
            raise LabArtifactConflictError("garbage owner source path escapes artifact root")
        self._assert_safe_artifact_ancestors(source.parent)
        return source

    def _reconcile_staging_bundle(self, staging: Path) -> LabGarbageOwner:
        owner = self._staging_owner(staging)
        intent_path = self._prepared_intent_path(owner.garbage_id)
        if not os.path.lexists(intent_path):
            self._write_prepared_intent(self._prepared_intent(owner))
        intent = self._load_prepared_intent(intent_path)
        if intent.owner != owner:
            raise LabArtifactConflictError("staging owner conflicts with prepared intent")
        self._reconcile_prepared_intent(intent)
        return owner

    @staticmethod
    def _register_legacy_owner(
        owners: dict[UUID, LabGarbageOwner],
        owner: LabGarbageOwner,
    ) -> None:
        existing = owners.get(owner.garbage_id)
        if existing is not None and existing != owner:
            raise LabArtifactConflictError("legacy garbage owner identities conflict")
        owners[owner.garbage_id] = owner

    def _prepared_intents_locked(self) -> dict[UUID, LabGarbagePreparedIntent]:
        intents: dict[UUID, LabGarbagePreparedIntent] = {}
        for path in sorted(self.garbage_intent_dir.iterdir()):
            if path.is_symlink() or not path.is_file():
                raise LabArtifactConflictError("prepared intent namespace is unsafe")
            intent = self._load_prepared_intent(path)
            if intent.owner.garbage_id in intents:
                raise LabArtifactConflictError("duplicate prepared intent identity")
            intents[intent.owner.garbage_id] = intent
        return intents

    @staticmethod
    def _directory_identity(observed: os.stat_result) -> tuple[int, int, int, int]:
        return (
            observed.st_dev,
            observed.st_ino,
            stat.S_IFMT(observed.st_mode),
            observed.st_nlink,
        )

    def _restore_changed_staging_from_orphan(
        self,
        *,
        staging: Path,
        orphan: Path,
        moved: os.stat_result,
    ) -> None:
        if os.path.lexists(staging):
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "original path is occupied and both paths were preserved"
            )
        if orphan.is_symlink() or not stat.S_ISDIR(moved.st_mode):
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "isolated replacement is unsafe and was preserved"
            )
        try:
            os.rename(orphan, staging)
        except OSError as exc:
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "replacement could not be restored and both paths were preserved"
            ) from exc
        _fsync_directory(self.garbage_intent_orphan_dir)
        _fsync_directory(self.garbage_staging_dir)
        restored = staging.lstat()
        if (
            staging.is_symlink()
            or not stat.S_ISDIR(restored.st_mode)
            or self._directory_identity(restored) != self._directory_identity(moved)
            or os.path.lexists(orphan)
        ):
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "replacement restore identity conflicts"
            )

    def _orphan_legacy_empty_staging_locked(self, staging: Path) -> None:
        try:
            legacy_id = UUID(hex=staging.name)
        except ValueError as exc:
            raise LabArtifactConflictError("legacy empty staging name is invalid") from exc
        expected = staging.lstat()
        if (
            staging.is_symlink()
            or not stat.S_ISDIR(expected.st_mode)
            or expected.st_nlink < 1
            or any(staging.iterdir())
        ):
            raise LabArtifactConflictError("legacy empty staging is unsafe")
        orphan_token = uuid4()
        orphan = self.garbage_intent_orphan_dir / (
            f"legacy-empty-staging-{legacy_id.hex}-{orphan_token.hex}"
        )
        if os.path.lexists(orphan):
            raise LabArtifactConflictError("legacy empty staging orphan already exists")
        os.rename(staging, orphan)
        _fsync_directory(self.garbage_staging_dir)
        _fsync_directory(self.garbage_intent_orphan_dir)
        moved = orphan.lstat()
        moved_is_empty = not any(orphan.iterdir())
        source_is_absent = not os.path.lexists(staging)
        if (
            orphan.is_symlink()
            or not stat.S_ISDIR(moved.st_mode)
            or self._directory_identity(moved) != self._directory_identity(expected)
            or not moved_is_empty
            or not source_is_absent
        ):
            if source_is_absent:
                self._restore_changed_staging_from_orphan(
                    staging=staging,
                    orphan=orphan,
                    moved=moved,
                )
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "successful orphan metadata was not written"
            )
        self._ensure_legacy_empty_staging_orphan_metadata(
            orphan,
            legacy_id,
            orphan_token,
            self._directory_identity(expected),
        )

    def _migrate_legacy_prepared_state_locked(self) -> None:
        intents = self._prepared_intents_locked()
        owners: dict[UUID, LabGarbageOwner] = {}
        empty_staging: list[Path] = []
        for marker in sorted(self.garbage_owner_dir.iterdir()):
            if marker.is_symlink() or not marker.is_file() or marker.suffix != ".json":
                raise LabArtifactConflictError("garbage owner namespace is unsafe")
            try:
                garbage_id = UUID(hex=marker.stem)
            except ValueError as exc:
                raise LabArtifactConflictError("garbage owner ledger name is invalid") from exc
            owner = self._load_garbage_owner_ledger(garbage_id)
            self._register_legacy_owner(owners, owner)
        for ledger_path in sorted(self.garbage_ledger_dir.iterdir()):
            if ledger_path.is_symlink() or not ledger_path.is_file():
                raise LabArtifactConflictError("garbage ledger namespace is unsafe")
            ledger = self._load_garbage_ledger(ledger_path)
            self._register_legacy_owner(owners, ledger.owner)
        for staging in sorted(self.garbage_staging_dir.iterdir()):
            observed = staging.lstat()
            if staging.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                raise LabArtifactConflictError("garbage staging namespace is unsafe")
            try:
                staging_id = UUID(hex=staging.name)
            except ValueError as exc:
                raise LabArtifactConflictError("garbage staging name is invalid") from exc
            names = {child.name for child in staging.iterdir()}
            if not names:
                if staging_id not in intents and staging_id not in owners:
                    empty_staging.append(staging)
                continue
            if not names.issubset({"owner.json", "payload"}):
                raise LabArtifactConflictError("legacy staging contains unknown derived state")
            if "owner.json" in names:
                owner = self._load_garbage_owner(staging / "owner.json")
                if owner.garbage_id != staging_id:
                    raise LabArtifactConflictError("legacy staging owner conflicts with directory")
                self._register_legacy_owner(owners, owner)
            elif staging_id not in owners and staging_id not in intents:
                raise LabArtifactConflictError("legacy staged payload has no provable owner")
        for deferred in sorted(self.garbage_deferred_dir.iterdir()):
            observed = deferred.lstat()
            if deferred.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                raise LabArtifactConflictError("deferred garbage namespace is unsafe")
            names = {child.name for child in deferred.iterdir()}
            if names != {"owner.json", "payload"}:
                raise LabArtifactConflictError("legacy deferred bundle inventory is unsafe")
            owner = self._load_garbage_owner(deferred / "owner.json")
            if deferred.name != owner.garbage_id.hex:
                raise LabArtifactConflictError("legacy deferred owner conflicts with directory")
            if self._garbage_inventory(deferred / "payload") != owner.inventory:
                raise LabArtifactConflictError("legacy deferred payload conflicts with owner")
            self._register_legacy_owner(owners, owner)
        for garbage_id, owner in sorted(owners.items(), key=lambda item: item[0].hex):
            existing = intents.get(garbage_id)
            expected = self._prepared_intent(owner)
            if existing is not None:
                if existing != expected:
                    raise LabArtifactConflictError("legacy owner conflicts with prepared intent")
                continue
            self._write_prepared_intent(expected)
            intents[garbage_id] = expected
        for staging in empty_staging:
            self._orphan_legacy_empty_staging_locked(staging)

    def _reconcile_prepared_intent(self, intent: LabGarbagePreparedIntent) -> None:
        owner = intent.owner
        if self._load_prepared_intent(self._prepared_intent_path(owner.garbage_id)) != intent:
            raise LabArtifactConflictError("prepared intent changed before reconciliation")
        source = self._source_path_for_owner(owner)
        staging = self.artifact_root / intent.staging_relative_path
        deferred = self.garbage_deferred_dir / owner.garbage_id.hex
        source_exists = os.path.lexists(source)
        staging_exists = os.path.lexists(staging)
        deferred_exists = os.path.lexists(deferred)
        if deferred_exists:
            if source_exists or staging_exists:
                raise LabArtifactConflictError("deferred quarantine conflicts with active source")
            self._ensure_global_garbage_owner(owner)
            if self._validate_garbage_bundle(deferred) != owner:
                raise LabArtifactConflictError("deferred garbage conflicts with prepared intent")
            for state in ("prepared", "quarantined", "deferred_gc"):
                self._write_garbage_ledger(owner, state)
            return
        staging = self._ensure_garbage_staging(intent)
        self._ensure_global_garbage_owner(owner)
        self._ensure_bundle_garbage_owner(staging, owner)
        self._write_garbage_ledger(owner, "prepared")
        payload = staging / "payload"
        source_exists = os.path.lexists(source)
        payload_exists = os.path.lexists(payload)
        if source_exists and payload_exists:
            raise LabArtifactConflictError("garbage source and staged payload both exist")
        if not source_exists and not payload_exists:
            raise LabArtifactConflictError("prepared intent has neither source nor staged payload")
        if source_exists:
            if self._garbage_inventory(source) != owner.inventory:
                raise LabArtifactConflictError(
                    "garbage source conflicts with owner inventory and prepared intent"
                )
            os.rename(source, payload)
            _fsync_directory(source.parent)
            _fsync_directory(staging)
            if os.path.lexists(source):
                raise LabArtifactConflictError("garbage source was replaced during isolation")
        if self._validate_garbage_bundle(staging) != owner:
            raise LabArtifactConflictError("staged garbage conflicts with prepared intent")
        self._write_garbage_ledger(owner, "quarantined")
        self._promote_garbage_bundle(staging, deferred)
        if self._validate_garbage_bundle(deferred) != owner:
            raise LabArtifactConflictError("promoted garbage conflicts with prepared intent")
        self._write_garbage_ledger(owner, "deferred_gc")

    def _collect_garbage_locked(self) -> None:
        self._reconcile_intent_temporaries_locked()
        self._migrate_legacy_prepared_state_locked()
        intents = self._prepared_intents_locked()
        for intent in sorted(intents.values(), key=lambda item: item.owner.garbage_id.hex):
            self._reconcile_prepared_intent(intent)
        if any(self.garbage_staging_dir.iterdir()):
            raise LabArtifactConflictError("garbage staging remained after intent reconciliation")

    def collect_garbage(self) -> None:
        """Reconcile durable quarantine state without physically deleting retained bytes."""
        with self.report_spool.evidence_lock():
            self._collect_garbage_locked()

    def quarantine_entries(self) -> tuple[LabQuarantineEntry, ...]:
        with self.report_spool.evidence_lock():
            self._collect_garbage_locked()
            entries: list[LabQuarantineEntry] = []
            for bundle in sorted(self.garbage_deferred_dir.iterdir()):
                owner = self._validate_garbage_bundle(bundle)
                latest = self._latest_garbage_ledger(owner)
                if latest.state != "deferred_gc":
                    raise LabArtifactConflictError("deferred quarantine has no deferred_gc ledger")
                retained_bytes = sum(
                    entry.size or 0 for entry in owner.inventory if entry.file_type == "regular"
                )
                entries.append(
                    LabQuarantineEntry(
                        state=latest.state,
                        owner=owner,
                        ledger_paths=self._garbage_ledgers(owner),
                        bundle_path=bundle,
                        retained_bytes=retained_bytes,
                    )
                )
            return tuple(entries)

    def quarantine_summary(self) -> LabQuarantineSummary:
        """Expose retained P1.3 bytes for the later exclusive-window lifecycle GC."""
        entries = self.quarantine_entries()
        return LabQuarantineSummary(
            bundle_count=len(entries),
            retained_bytes=sum(entry.retained_bytes for entry in entries),
        )

    def _logical_delete(
        self,
        path: Path,
        *,
        owner: LabGarbageOwner,
    ) -> bool:
        intent = self._prepared_intent(owner)
        self._write_prepared_intent(intent)
        self._reconcile_prepared_intent(intent)
        return True

    def logical_quarantine_tree(
        self,
        path: Path,
        *,
        purpose: str,
    ) -> bool:
        if not os.path.lexists(path):
            return False
        self._assert_safe_artifact_ancestors(path.parent)
        inventory = self._garbage_inventory(path)
        owner = self._garbage_owner(
            path,
            purpose=purpose,
            inventory=inventory,
        )
        with self.report_spool.evidence_lock():
            return self._logical_delete(path, owner=owner)

    def logical_delete_temporary_tree(
        self,
        path: Path,
        *,
        current_claim: LabShardClaim,
    ) -> bool:
        self._assert_safe_temporary_tree(path)
        return self.logical_quarantine_tree(
            path,
            purpose=(
                "crash temporary cleanup "
                f"job={current_claim.job_id} shard={current_claim.shard_id} "
                f"generation={current_claim.claim_generation} "
                f"token={current_claim.claim_token}"
            ),
        )

    def _safe_remove_regular_child(
        self,
        path: Path,
        *,
        expected: LabRegularFileIdentity,
        label: str,
    ) -> bool:
        parent = path.parent
        self._assert_safe_artifact_ancestors(parent)
        if path != parent / path.name or path.name in {"", ".", ".."}:
            raise LabArtifactConflictError(f"{label} path is unsafe")
        if parent.is_symlink() or not parent.is_dir():
            raise LabArtifactConflictError(f"{label} parent is unsafe")
        try:
            observed = self._regular_file_identity(path, label=label)
        except LabArtifactConflictError:
            if not os.path.lexists(path):
                return False
            raise
        if observed != expected:
            raise LabArtifactConflictError(f"{label} changed before deletion")
        inventory = (
            LabGarbageInventoryEntry(
                relative_path=".",
                file_type="regular",
                device=expected.device,
                inode=expected.inode,
                size=expected.size,
                sha256=expected.sha256,
            ),
        )
        owner = self._garbage_owner(path, purpose=label, inventory=inventory)
        return self._logical_delete(path, owner=owner)

    def _remove_ledger(self, path: Path) -> None:
        if not os.path.lexists(path):
            return
        expected = self._regular_file_identity(path, label="reclaim ledger")
        self._load_ledger(path)
        self._safe_remove_regular_child(
            path,
            expected=expected,
            label="reclaim ledger",
        )

    @staticmethod
    def _inventory_entry(path: Path, *, relative_path: str) -> LabReclaimInventoryEntry:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or path.is_symlink():
            raise LabArtifactConflictError(f"reclaim inventory path is unsafe: {relative_path}")
        if before.st_nlink != 1:
            raise LabArtifactConflictError(
                f"reclaim inventory file has an external hard link: {relative_path}"
            )
        digest = _file_sha256(path)
        after = path.lstat()
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_nlink,
        ) != (before.st_dev, before.st_ino, before.st_size, 1):
            raise LabArtifactConflictError(
                f"reclaim inventory file changed while hashing: {relative_path}"
            )
        return LabReclaimInventoryEntry(
            relative_path=relative_path,
            device=before.st_dev,
            inode=before.st_ino,
            size=before.st_size,
            sha256=digest,
        )

    def _build_inventory(
        self,
        bundle: Path,
        manifest: LabShardResultManifest,
    ) -> tuple[LabReclaimInventoryEntry, ...]:
        names = sorted(
            ("manifest.json",) + tuple(artifact.file_name for artifact in manifest.artifacts)
        )
        return tuple(self._inventory_entry(bundle / name, relative_path=name) for name in names)

    def _sealed_quarantine_owner(
        self,
        tombstone: Path,
        *,
        obsolete_claim: LabShardClaim,
        source_device: int,
        source_inode: int,
        inventory: tuple[LabReclaimInventoryEntry, ...],
    ) -> LabGarbageOwner:
        garbage_inventory = (
            LabGarbageInventoryEntry(
                relative_path=".",
                file_type="directory",
                device=source_device,
                inode=source_inode,
            ),
            *(
                LabGarbageInventoryEntry(
                    relative_path=entry.relative_path,
                    file_type="regular",
                    device=entry.device,
                    inode=entry.inode,
                    size=entry.size,
                    sha256=entry.sha256,
                )
                for entry in inventory
            ),
        )
        return LabGarbageOwner(
            purpose=(
                "obsolete sealed attempt "
                f"job={obsolete_claim.job_id} shard={obsolete_claim.shard_id} "
                f"generation={obsolete_claim.claim_generation} "
                f"token={obsolete_claim.claim_token}"
            ),
            original_relative_path=self._garbage_relative_path(tombstone),
            payload_type="directory",
            inventory=tuple(sorted(garbage_inventory, key=lambda entry: entry.relative_path)),
        )

    def _reclaim_quarantine_owner(
        self,
        tombstone: Path,
        ledger: LabReclaimLedger,
    ) -> LabGarbageOwner:
        owner = self._sealed_quarantine_owner(
            tombstone,
            obsolete_claim=ledger.obsolete_claim,
            source_device=ledger.source_device,
            source_inode=ledger.source_inode,
            inventory=ledger.inventory,
        )
        if ledger.quarantine_id is not None and ledger.quarantine_id != owner.garbage_id:
            raise LabArtifactConflictError("reclaim quarantine identity conflicts with ledger")
        return owner

    def _validate_isolated_tree(
        self,
        path: Path,
        ledger: LabReclaimLedger,
    ) -> None:
        root_before = path.lstat()
        if path.is_symlink() or not stat.S_ISDIR(root_before.st_mode):
            raise LabArtifactConflictError("reclaim tombstone is unsafe")
        if (root_before.st_dev, root_before.st_ino) != (
            ledger.source_device,
            ledger.source_inode,
        ):
            raise LabArtifactConflictError("reclaim tombstone inode conflicts with durable ledger")
        expected = {entry.relative_path: entry for entry in ledger.inventory}
        actual = {candidate.name: candidate for candidate in path.iterdir()}
        unknown = set(actual) - set(expected)
        if unknown:
            raise LabArtifactConflictError(
                f"reclaim tombstone contains unknown paths: {sorted(unknown)}"
            )
        if ledger.state == "prepared" and set(actual) != set(expected):
            raise LabArtifactConflictError("prepared reclaim tombstone is incomplete")
        for name, candidate in actual.items():
            observed = self._inventory_entry(candidate, relative_path=name)
            if observed != expected[name]:
                raise LabArtifactConflictError(
                    f"reclaim tombstone inventory identity conflicts: {name}"
                )
        root_after = path.lstat()
        if (root_after.st_dev, root_after.st_ino) != (
            root_before.st_dev,
            root_before.st_ino,
        ):
            raise LabArtifactConflictError("reclaim tombstone changed while validating")

    def _delete_inventory_entry(
        self,
        directory: Path,
        entry: LabReclaimInventoryEntry,
    ) -> None:
        target = directory / entry.relative_path
        if not os.path.lexists(target):
            return
        if self._inventory_entry(target, relative_path=entry.relative_path) != entry:
            raise LabArtifactConflictError(
                f"reclaim inventory changed before deletion: {entry.relative_path}"
            )
        self._safe_remove_regular_child(
            target,
            expected=LabRegularFileIdentity(
                device=entry.device,
                inode=entry.inode,
                size=entry.size,
                sha256=entry.sha256,
            ),
            label=f"reclaim inventory {entry.relative_path}",
        )

    def _delete_isolated_tombstone(
        self,
        tombstone: Path,
        ledger: LabReclaimLedger,
    ) -> LabGarbageOwner:
        self._validate_isolated_tree(tombstone, ledger)
        owner = self._reclaim_quarantine_owner(tombstone, ledger)
        self._logical_delete(tombstone, owner=owner)
        return owner

    def _cleanup_ledger_temporaries(self, directory: Path) -> None:
        if not directory.exists():
            return
        if directory.is_symlink() or not directory.is_dir():
            raise LabArtifactConflictError("reclaim ledger directory is unsafe")
        for candidate in sorted(directory.iterdir(), key=lambda path: path.name):
            if not candidate.name.endswith(".tmp"):
                continue
            if self._LEDGER_TEMP_NAME.fullmatch(candidate.name) is None:
                raise LabArtifactConflictError("unknown reclaim ledger temporary file")
            expected = self._regular_file_identity(
                candidate,
                label="reclaim ledger temporary file",
            )
            self._safe_remove_regular_child(
                candidate,
                expected=expected,
                label="reclaim ledger temporary file",
            )

    def _assert_no_terminal_success_evidence_from(
        self,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
        current_claim: LabShardClaim,
        *,
        pending: tuple[LabReportSpoolEntry, ...],
        receipt_paths: tuple[Path, ...],
    ) -> None:
        for entry in pending:
            report = entry.report
            if not self._report_matches_attempt(report, claim):
                continue
            if not isinstance(report.body, LabShardSucceeded):
                continue
            if claim.claim_generation < current_claim.claim_generation:
                continue
            if report.body.result_manifest_hash != manifest.manifest_hash:
                raise LabArtifactConflictError(
                    "pending success manifest conflicts with sealed attempt"
                )
            raise LabArtifactConflictError(
                "pending success may already be committed before receipt ack"
            )

        for path in receipt_paths:
            receipt = self.report_spool.load_receipt(path)
            if (receipt.job_id, receipt.shard_id) != (claim.job_id, claim.shard_id):
                continue
            if receipt.claim_token is None:
                if receipt.status == "accepted":
                    raise LabArtifactConflictError(
                        "legacy accepted receipt cannot prove a safe attempt deletion"
                    )
                continue
            if not self._receipt_matches_attempt(receipt, claim):
                continue
            if receipt.report_type != "shard_succeeded":
                continue
            if receipt.result_manifest_hash != manifest.manifest_hash:
                raise LabArtifactConflictError(
                    "success receipt manifest conflicts with sealed attempt"
                )
            if receipt.status == "accepted":
                raise LabArtifactConflictError(
                    "accepted success receipt protects terminal artifact"
                )

    def _assert_no_terminal_success_evidence(
        self,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
        current_claim: LabShardClaim,
    ) -> None:
        self._assert_no_terminal_success_evidence_from(
            claim,
            manifest,
            current_claim,
            pending=self.report_spool.pending(),
            receipt_paths=tuple(sorted(self.report_spool.ack_dir.glob("*.json"))),
        )

    def _assert_no_terminal_success_evidence_locked(
        self,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
        current_claim: LabShardClaim,
    ) -> None:
        self._assert_no_terminal_success_evidence_from(
            claim,
            manifest,
            current_claim,
            pending=self.report_spool.pending_locked(),
            receipt_paths=self.report_spool.receipt_paths_locked(),
        )

    @staticmethod
    def _obsolete_claim(
        current_claim: LabShardClaim,
        *,
        fence: int,
        generation: int,
        token: UUID,
    ) -> LabShardClaim:
        if generation >= current_claim.claim_generation:
            raise LabArtifactConflictError(
                "reclaim identity is not older than durable claim high-water"
            )
        if fence > current_claim.scheduler_fencing_token:
            raise LabArtifactConflictError("obsolete attempt has a future scheduler fencing token")
        return current_claim.model_copy(
            update={
                "claim_token": token,
                "claim_generation": generation,
                "scheduler_fencing_token": fence,
            }
        )

    def _validate_tombstone(
        self,
        path: Path,
        current_claim: LabShardClaim,
    ) -> tuple[LabShardClaim, LabShardResultManifest]:
        fence, generation, token, expected_hash = self._parse_tombstone_name(path.name)
        obsolete_claim = self._obsolete_claim(
            current_claim,
            fence=fence,
            generation=generation,
            token=token,
        )
        ledger = self._load_ledger(self._ledger_path(current_claim, path.name))
        manifest = ledger.manifest
        self._validate_ledger(
            ledger,
            current_claim=current_claim,
            obsolete_claim=obsolete_claim,
            manifest=manifest,
        )
        if manifest.manifest_hash != expected_hash:
            raise LabArtifactConflictError(
                "reclaim tombstone manifest does not match its durable identity"
            )
        self._validate_isolated_tree(path, ledger)
        return obsolete_claim, manifest

    def _classify_attempt(
        self,
        candidate: Path,
        current_claim: LabShardClaim,
    ) -> tuple[LabShardClaim, LabShardResultManifest] | None:
        fence, generation, token = self._parse_attempt_name(candidate.name)
        candidate_identity = (fence, generation, token)
        current_identity = (
            current_claim.scheduler_fencing_token,
            current_claim.claim_generation,
            current_claim.claim_token,
        )
        if generation > current_claim.claim_generation:
            raise LabArtifactConflictError(
                "future sealed attempt conflicts with durable claim high-water"
            )
        if generation == current_claim.claim_generation:
            if candidate_identity != current_identity:
                raise LabArtifactConflictError(
                    "current-generation sealed attempt has conflicting identity"
                )
            return None
        obsolete_claim = self._obsolete_claim(
            current_claim,
            fence=fence,
            generation=generation,
            token=token,
        )
        if self.sealed_bundle_path(obsolete_claim) != candidate:
            raise LabArtifactConflictError(
                "sealed attempt directory does not match parsed identity"
            )
        return obsolete_claim, self._validate_bundle(candidate, obsolete_claim)

    @staticmethod
    def _attempt_identity(claim: LabShardClaim) -> tuple[int, int, UUID]:
        return (
            claim.scheduler_fencing_token,
            claim.claim_generation,
            claim.claim_token,
        )

    def _inventory(
        self,
        current_claim: LabShardClaim,
        attempts_root: Path,
    ) -> tuple[
        tuple[tuple[Path, LabShardClaim, LabShardResultManifest], ...],
        tuple[tuple[Path, LabShardClaim, LabShardResultManifest], ...],
    ]:
        sources: dict[
            tuple[int, int, UUID],
            tuple[Path, LabShardClaim, LabShardResultManifest],
        ] = {}
        tombstones: dict[
            tuple[int, int, UUID],
            tuple[Path, LabShardClaim, LabShardResultManifest],
        ] = {}
        candidates = tuple(sorted(attempts_root.iterdir(), key=lambda path: path.name))
        source_names: set[tuple[int, int, UUID]] = set()
        tombstone_names: set[tuple[int, int, UUID]] = set()
        for candidate in candidates:
            if candidate.name.startswith(".reclaim-"):
                fence, generation, token, _manifest_hash = self._parse_tombstone_name(
                    candidate.name
                )
                identity = (fence, generation, token)
                if identity in tombstone_names:
                    raise LabArtifactConflictError(
                        "multiple tombstones claim the same attempt identity"
                    )
                tombstone_names.add(identity)
            else:
                identity = self._parse_attempt_name(candidate.name)
                if identity in source_names:
                    raise LabArtifactConflictError(
                        "multiple sources claim the same attempt identity"
                    )
                source_names.add(identity)
        if source_names & tombstone_names:
            raise LabArtifactConflictError(
                "source and tombstone coexist for the same attempt identity"
            )

        for candidate in candidates:
            if candidate.is_symlink() or not candidate.is_dir():
                raise LabArtifactConflictError(
                    f"sealed attempt is a symlink or not a directory: {candidate.name}"
                )
            if candidate.name.startswith(".reclaim-"):
                obsolete_claim, manifest = self._validate_tombstone(
                    candidate,
                    current_claim,
                )
                identity = self._attempt_identity(obsolete_claim)
                if identity in tombstones:
                    raise LabArtifactConflictError(
                        "multiple tombstones claim the same attempt identity"
                    )
                tombstones[identity] = (candidate, obsolete_claim, manifest)
                continue
            classified = self._classify_attempt(candidate, current_claim)
            if classified is None:
                continue
            obsolete_claim, manifest = classified
            identity = self._attempt_identity(obsolete_claim)
            if identity in sources:
                raise LabArtifactConflictError("multiple sources claim the same attempt identity")
            sources[identity] = (candidate, obsolete_claim, manifest)
        return tuple(sources.values()), tuple(tombstones.values())

    def _preflight(self, current_claim: LabShardClaim, attempts_root: Path) -> None:
        sources, tombstones = self._inventory(current_claim, attempts_root)
        for _candidate, obsolete_claim, manifest in sources:
            self._assert_no_terminal_success_evidence(
                obsolete_claim,
                manifest,
                current_claim,
            )
        for tombstone, obsolete_claim, manifest in tombstones:
            ledger_path = self._ledger_path(current_claim, tombstone.name)
            ledger = self._load_ledger(ledger_path)
            self._validate_ledger(
                ledger,
                current_claim=current_claim,
                obsolete_claim=obsolete_claim,
                manifest=manifest,
            )
            self._validate_isolated_tree(tombstone, ledger)
            self._assert_no_terminal_success_evidence(
                obsolete_claim,
                manifest,
                current_claim,
            )

    def _reclaim_locked(self, current_claim: LabShardClaim, attempts_root: Path) -> None:
        sources, tombstones = self._inventory(current_claim, attempts_root)
        for tombstone, obsolete_claim, manifest in tombstones:
            ledger_path = self._ledger_path(current_claim, tombstone.name)
            ledger = self._load_ledger(ledger_path)
            self._validate_ledger(
                ledger,
                current_claim=current_claim,
                obsolete_claim=obsolete_claim,
                manifest=manifest,
            )
            self._validate_isolated_tree(tombstone, ledger)
            self._assert_no_terminal_success_evidence_locked(
                obsolete_claim,
                manifest,
                current_claim,
            )
            if ledger.state == "prepared":
                ledger = ledger.model_copy(update={"state": "isolated"})
                self._write_ledger(ledger)
            owner = self._delete_isolated_tombstone(tombstone, ledger)
            ledger = ledger.model_copy(
                update={
                    "state": "deferred_gc",
                    "quarantine_id": owner.garbage_id,
                }
            )
            self._write_ledger(ledger)

        for candidate, obsolete_claim, manifest in sources:
            self._assert_no_terminal_success_evidence_locked(
                obsolete_claim,
                manifest,
                current_claim,
            )
            tombstone = attempts_root / self._tombstone_name(obsolete_claim, manifest)
            source_identity = LabWorker._bundle_file_identity(candidate)
            inventory = self._build_inventory(candidate, manifest)
            owner = self._sealed_quarantine_owner(
                tombstone,
                obsolete_claim=obsolete_claim,
                source_device=source_identity[0],
                source_inode=source_identity[1],
                inventory=inventory,
            )
            ledger_path = self._ledger_path(current_claim, tombstone.name)
            if os.path.lexists(ledger_path):
                stale = self._load_ledger(ledger_path)
                self._validate_ledger(
                    stale,
                    current_claim=current_claim,
                    obsolete_claim=obsolete_claim,
                    manifest=manifest,
                )
                if (
                    stale.state != "prepared"
                    or source_identity
                    != (
                        stale.source_device,
                        stale.source_inode,
                    )
                    or stale.inventory != inventory
                ):
                    raise LabArtifactConflictError(
                        "stale reclaim ledger conflicts with live source"
                    )
                if stale.quarantine_id not in {None, owner.garbage_id}:
                    raise LabArtifactConflictError(
                        "stale reclaim ledger quarantine identity conflicts"
                    )
                ledger = stale
            else:
                ledger = LabReclaimLedger(
                    state="prepared",
                    current_claim=current_claim,
                    obsolete_claim=obsolete_claim,
                    manifest=manifest,
                    inventory=inventory,
                    source_name=candidate.name,
                    tombstone_name=tombstone.name,
                    source_device=source_identity[0],
                    source_inode=source_identity[1],
                    quarantine_id=owner.garbage_id,
                )
                self._write_ledger(ledger)
            try:
                os.rename(candidate, tombstone)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                if not tombstone.is_dir() or candidate.exists():
                    raise LabArtifactConflictError(
                        "reclaim tombstone conflicts with sealed attempt"
                    ) from exc
                self._validate_isolated_tree(tombstone, ledger)
            _fsync_directory(attempts_root)
            if LabWorker._bundle_file_identity(tombstone) != source_identity:
                if os.path.lexists(candidate):
                    raise LabArtifactConflictError(
                        "sealed attempt was replaced during isolation and cannot be restored"
                    )
                os.rename(tombstone, candidate)
                _fsync_directory(attempts_root)
                raise LabArtifactConflictError("sealed attempt was replaced during isolation")
            self._validate_isolated_tree(tombstone, ledger)
            ledger = ledger.model_copy(update={"state": "isolated"})
            self._write_ledger(ledger)
            self._assert_no_terminal_success_evidence_locked(
                obsolete_claim,
                manifest,
                current_claim,
            )
            quarantined_owner = self._delete_isolated_tombstone(tombstone, ledger)
            ledger = ledger.model_copy(
                update={
                    "state": "deferred_gc",
                    "quarantine_id": quarantined_owner.garbage_id,
                }
            )
            self._write_ledger(ledger)

    def _reconcile_orphan_ledgers(self, current_claim: LabShardClaim) -> None:
        directory = self._ledger_dir(current_claim)
        if not directory.exists():
            return
        attempts_root = self.sealed_bundle_path(current_claim).parent
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.name.endswith(".tmp"):
                raise LabArtifactConflictError("reclaim ledger temporary remained after cleanup")
            if path.suffix != ".json":
                raise LabArtifactConflictError("unknown reclaim ledger file")
            ledger = self._load_ledger(path)
            source = attempts_root / ledger.source_name
            tombstone = attempts_root / ledger.tombstone_name
            if os.path.lexists(source) or os.path.lexists(tombstone):
                continue
            self._validate_ledger(
                ledger,
                current_claim=current_claim,
                obsolete_claim=ledger.obsolete_claim,
                manifest=ledger.manifest,
            )
            self._assert_no_terminal_success_evidence_locked(
                ledger.obsolete_claim,
                ledger.manifest,
                current_claim,
            )
            owner = self._reclaim_quarantine_owner(tombstone, ledger)
            if ledger.quarantine_id not in {None, owner.garbage_id}:
                raise LabArtifactConflictError(
                    "orphan reclaim ledger quarantine identity conflicts"
                )
            self._collect_garbage_locked()
            deferred = self.garbage_deferred_dir / owner.garbage_id.hex
            if not os.path.lexists(deferred):
                raise LabArtifactConflictError(
                    "reclaim ledger has no source, tombstone, or deferred quarantine"
                )
            if self._validate_garbage_bundle(deferred) != owner:
                raise LabArtifactConflictError("deferred reclaim quarantine conflicts")
            if ledger.state != "deferred_gc":
                ledger = ledger.model_copy(
                    update={
                        "state": "deferred_gc",
                        "quarantine_id": owner.garbage_id,
                    }
                )
                self._write_ledger(ledger)

    def reclaim(self, current_claim: LabShardClaim) -> None:
        validated = LabShardClaim.model_validate(current_claim)
        attempts_root = self.sealed_bundle_path(validated).parent
        ledger_dir = self._ledger_dir(validated)
        self._assert_safe_artifact_ancestors(attempts_root)
        self._assert_safe_artifact_ancestors(ledger_dir)
        if not attempts_root.exists() and not ledger_dir.exists():
            return
        if attempts_root.exists() and (attempts_root.is_symlink() or not attempts_root.is_dir()):
            raise LabArtifactConflictError("sealed attempts root is unsafe")
        if attempts_root.exists():
            self._preflight(validated, attempts_root)
        with self.report_spool.evidence_lock():
            self._cleanup_ledger_temporaries(ledger_dir)
            if attempts_root.exists():
                self._reclaim_locked(validated, attempts_root)
            self._reconcile_orphan_ledgers(validated)
