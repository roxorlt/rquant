"""Filesystem-fenced background worker for Strategy Lab shard claims."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import signal
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Literal
from uuid import UUID, uuid4

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotBinding
from rquant.lab_job_protocol import InvalidCommandEnvelopeError
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabClaimSupersededError,
    LabReportReceipt,
    LabReportSpool,
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
    status: Literal["idle", "succeeded", "failed", "stopped"]
    claim_token: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)


class LabArtifactConflictError(RuntimeError):
    """A sealed shard bundle exists but is not the expected immutable result."""


StoreFactory = Callable[[], AbstractContextManager[object]]
ReceiptWaiter = Callable[[LabWorkerReport, float, threading.Event], LabReportReceipt]
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
        self._stop = threading.Event()

    def request_stop(self) -> None:
        self._stop.set()

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
            raise LabArtifactConflictError(
                f"invalid temporary attempt token: {name}"
            ) from exc
        return int(parts[0]), int(parts[1]), token

    @staticmethod
    def _assert_safe_temporary_tree(path: Path) -> None:
        if path.is_symlink() or not path.is_dir():
            raise LabArtifactConflictError(
                f"obsolete temporary attempt is a symlink or not a directory: {path.name}"
            )
        for root, directories, files in os.walk(path, followlinks=False):
            for name in (*directories, *files):
                child = Path(root) / name
                if child.is_symlink():
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains a symlink: {name}"
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
                raise LabArtifactConflictError(
                    f"artifact path ancestor is a symlink: {part}"
                )
            if os.path.lexists(current) and not current.is_dir():
                raise LabArtifactConflictError(
                    f"artifact path ancestor is not a directory: {part}"
                )

    def _reclaim_obsolete_temporaries(self, claim: LabShardClaim) -> None:
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
                continue
            self._assert_safe_temporary_tree(candidate)
            reclaimed = shard_root / f".reclaim-{candidate.name}-{uuid4().hex}"
            os.rename(candidate, reclaimed)
            _fsync_directory(shard_root)
            shutil.rmtree(reclaimed)
            _fsync_directory(shard_root)

    def _publish_report(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    ) -> LabWorkerReport:
        report = LabWorkerReport.from_claim(
            claim,
            report_id=uuid4(),
            reported_at=_utc(self.clock()),
            body=body,
        )
        self.report_spool.publish(report)
        return report

    def _wait_for_receipt(
        self,
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: threading.Event,
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
        stop: threading.Event,
    ) -> LabReportReceipt:
        report = self._publish_report(claim, body)
        receipt = self.receipt_waiter(report, self.receipt_timeout_seconds, stop)
        if (
            receipt.report_id != report.report_id
            or receipt.content_hash != report.content_hash
            or receipt.job_id != report.job_id
            or receipt.shard_id != report.shard_id
        ):
            raise ValueError("report receipt identity does not match published report")
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
                if claim.worker_id == self.worker_id:
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
            except (InvalidCommandEnvelopeError, LabClaimSupersededError):
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
            raise PermissionError(
                "formal runtime clean code SHA does not match ResearchRunSpec"
            )
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
        if bundle.is_symlink() or not bundle.is_dir():
            raise LabArtifactConflictError("sealed shard bundle is not a regular directory")
        manifest_path = bundle / "manifest.json"
        try:
            raw = manifest_path.read_text(encoding="utf-8")
            manifest = LabShardResultManifest.model_validate_json(raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid sealed result manifest: {exc}") from exc
        if raw != manifest.canonical_json():
            raise LabArtifactConflictError("sealed result manifest is not canonical JSON")
        if not self._expected_manifest_identity(claim, manifest):
            raise LabArtifactConflictError("sealed result manifest identity conflicts with claim")
        expected_files = {"manifest.json"} | {
            artifact.file_name for artifact in manifest.artifacts
        }
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
            if (
                path.stat().st_size != artifact.file_size
                or _file_sha256(path) != artifact.file_sha256
            ):
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
        return manifest

    def _cleanup_temporary(self, temporary: Path) -> None:
        if temporary.is_symlink():
            raise LabArtifactConflictError("temporary shard bundle is a symlink")
        if temporary.exists():
            for child in temporary.iterdir():
                if child.is_symlink() or not child.is_file():
                    raise LabArtifactConflictError(
                        f"temporary shard bundle contains unsafe path: {child.name}"
                    )
                child.unlink()
            temporary.rmdir()
        stop = self.artifact_root / ".tmp"
        parent = temporary.parent
        while parent != stop.parent and parent != self.artifact_root:
            try:
                parent.rmdir()
            except OSError:
                break
            if parent == stop:
                break
            parent = parent.parent

    def _seal_result(
        self,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
        *,
        deadline: datetime | None = None,
    ) -> LabShardResultManifest:
        self._validate_result_identity(claim, result)
        sealed = self.sealed_bundle_path(claim)
        temporary_root = self._temporary_bundle_path(claim)
        self._assert_safe_artifact_ancestors(temporary_root)
        self._assert_safe_artifact_ancestors(sealed.parent)
        temporary = temporary_root / uuid4().hex
        try:
            self._write_bundle(temporary, claim, result)
            candidate = self._validate_bundle(temporary, claim)
            if self._stop.is_set():
                raise InterruptedError("worker stop requested before atomic seal")
            if deadline is not None and _utc(self.clock()) >= deadline:
                raise TimeoutError("ResearchRunSpec deadline reached before atomic seal")
            if sealed.exists() or sealed.is_symlink():
                existing = self._validate_bundle(sealed, claim)
                if existing.manifest_hash != candidate.manifest_hash:
                    raise LabArtifactConflictError(
                        "same attempt produced a conflicting result manifest"
                    )
                return existing
            sealed.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.rename(temporary, sealed)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                existing = self._validate_bundle(sealed, claim)
                if existing.manifest_hash != candidate.manifest_hash:
                    raise LabArtifactConflictError(
                        "concurrent attempt produced a conflicting result manifest"
                    ) from exc
                return existing
            _fsync_directory(sealed.parent)
            return candidate
        finally:
            self._cleanup_temporary(temporary)

    def _reuse_sealed(self, claim: LabShardClaim) -> LabShardResultManifest | None:
        sealed = self.sealed_bundle_path(claim)
        if not sealed.exists() and not sealed.is_symlink():
            return None
        return self._validate_bundle(sealed, claim)

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

        finished = threading.Event()
        heartbeat_errors: list[Exception] = []
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(claim, finished, heartbeat_errors),
            name=f"lab-heartbeat-{claim.claim_token}",
            daemon=True,
        )
        heartbeat.start()
        try:
            with self._open_store(validated.spec) as store:
                result = self.adapter_registry.execute_shard(validated, store)
        except PermissionError as exc:
            return self._failure_result(claim, phase="session", error=exc)
        except Exception as exc:
            return self._failure_result(claim, phase="execute", error=exc)
        finally:
            finished.set()
            heartbeat.join()

        if heartbeat_errors:
            return self._failure_result(
                claim,
                phase="fence",
                error=heartbeat_errors[0],
            )
        try:
            self._check_deadline(validated.spec)
        except Exception as exc:
            return self._failure_result(claim, phase="deadline", error=exc)
        if self._stop.is_set():
            return self._stopped_result(
                claim,
                reason="worker stop requested after shard execution",
            )

        try:
            self._check_deadline(validated.spec)
            receipt = self._publish_and_wait(
                claim,
                LabShardHeartbeat(
                    lease_extension_seconds=self.lease_extension_seconds,
                ),
                stop=self._stop,
            )
            effective_expiry = receipt.accepted_at + timedelta(
                seconds=self.lease_extension_seconds
            )
            if _utc(self.clock()) >= effective_expiry:
                raise PermissionError("accepted heartbeat lease expired before seal")
            self._check_deadline(validated.spec)
        except InterruptedError:
            return self._stopped_result(
                claim,
                reason="worker stop requested while confirming final shard fence",
            )
        except Exception as exc:
            return self._failure_result(claim, phase="fence", error=exc)

        try:
            manifest = self._seal_result(
                claim,
                result,
                deadline=validated.spec.deadline,
            )
        except InterruptedError:
            return self._stopped_result(
                claim,
                reason="worker stop requested before atomic shard seal",
            )
        except Exception as exc:
            return self._failure_result(claim, phase="seal", error=exc)

        try:
            self._publish_and_wait(
                claim,
                LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
                stop=threading.Event(),
            )
        except Exception as exc:
            return self._failure_result(claim, phase="fence", error=exc)
        return LabWorkerTickResult(
            status="succeeded",
            claim_token=claim.claim_token,
            manifest_hash=manifest.manifest_hash,
        )

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
                if result.status == "stopped" or (self._stop.is_set() and result.status == "idle"):
                    return
                self._stop.wait(self.poll_interval_ms / 1_000)
        finally:
            if previous_handler is not None:
                signal.signal(signal.SIGTERM, previous_handler)
