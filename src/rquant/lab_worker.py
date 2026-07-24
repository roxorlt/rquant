"""Filesystem-fenced background worker for Strategy Lab shard claims."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Literal
from uuid import UUID, uuid4

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.lab_job_protocol import InvalidCommandEnvelopeError
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportSpool,
    LabShardClaim,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerReport,
    LabWorkerStopped,
)
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
    phase: Literal["claim", "session", "execute", "seal"]
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
        self.clock = clock
        self._stop = threading.Event()

    def request_stop(self) -> None:
        self._stop.set()

    def sealed_bundle_path(self, claim: LabShardClaim) -> Path:
        return self.artifact_root / "jobs" / str(claim.job_id) / "shards" / str(claim.shard_id)

    def _temporary_bundle_path(self, claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / ".tmp"
            / str(claim.job_id)
            / str(claim.shard_id)
            / f"{claim.claim_generation:020d}-{claim.claim_token}"
        )

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

    def _next_owned_claim(self) -> LabShardClaim | None:
        now = _utc(self.clock())
        entries = []
        for path in self.claim_spool.pending_paths():
            try:
                entry = self.claim_spool.load(path)
            except InvalidCommandEnvelopeError:
                continue
            claim = entry.claim
            if claim.lease_expires_at <= now:
                continue
            entries.append(entry)
        latest_by_shard = {}
        for entry in entries:
            claim = entry.claim
            key = (claim.job_id, claim.shard_id)
            identity = (
                claim.scheduler_fencing_token,
                claim.claim_generation,
                claim.claimed_at,
                claim.claim_token.int,
            )
            current = latest_by_shard.get(key)
            if current is None or identity > current[0]:
                latest_by_shard[key] = (identity, entry)
        latest_entries = {id(value[1]) for value in latest_by_shard.values()}
        for entry in entries:
            if id(entry) not in latest_entries or entry.claim.worker_id != self.worker_id:
                continue
            try:
                return self.claim_spool.consume(entry)
            except InvalidCommandEnvelopeError:
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
        with self.metadata_store_factory() as metadata_store:
            snapshot = metadata_store.get_dataset_snapshot(identity.snapshot_id)
            if (
                snapshot is None
                or snapshot.snapshot_id != identity.snapshot_id
                or snapshot.status != "ready"
            ):
                raise PermissionError(
                    "formal dataset snapshot identity is unavailable or not ready"
                )
            binding = metadata_store.get_dataset_snapshot_binding(identity.snapshot_id)
            if (
                binding is None
                or binding.snapshot_id != identity.snapshot_id
                or binding.binding_hash != identity.binding_hash
                or binding.status != "ready"
            ):
                raise PermissionError("formal dataset binding identity is unavailable or changed")
            if identity.audit_run_id is not None:
                audit = metadata_store.get_data_audit_run(identity.audit_run_id)
                if (
                    audit is None
                    or audit.audit_run_id != identity.audit_run_id
                    or audit.status != "completed"
                ):
                    raise PermissionError("formal data audit identity is unavailable or incomplete")
            with ResearchExecutionSession(
                binding=binding,
                lake_root=self.research_lake_root,
            ) as session:
                yield session

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
            manifest.spec_hash,
            manifest.payload_hash,
            manifest.plan_hash,
            manifest.adapter_id,
            manifest.adapter_version,
        ) == (
            claim.job_id,
            claim.shard_id,
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
        if temporary.exists():
            for child in temporary.iterdir():
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
    ) -> LabShardResultManifest:
        self._validate_result_identity(claim, result)
        sealed = self.sealed_bundle_path(claim)
        if sealed.exists() or sealed.is_symlink():
            return self._validate_bundle(sealed, claim)
        temporary = self._temporary_bundle_path(claim)
        try:
            self._write_bundle(temporary, claim, result)
            manifest = self._validate_bundle(temporary, claim)
            sealed.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.rename(temporary, sealed)
            except FileExistsError:
                return self._validate_bundle(sealed, claim)
            _fsync_directory(sealed.parent)
            return manifest
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
        phase: Literal["claim", "session", "execute", "seal"],
        error: Exception,
    ) -> LabWorkerTickResult:
        failure = LabWorkerFailure(
            phase=phase,
            error_type=type(error).__name__,
            message=str(error) or type(error).__name__,
        )
        self._publish_report(
            claim,
            LabShardFailed(failure_json=failure.canonical_json()),
        )
        return LabWorkerTickResult(status="failed", claim_token=claim.claim_token)

    def run_once(self) -> LabWorkerTickResult:
        claim = self._next_owned_claim()
        if claim is None:
            return LabWorkerTickResult(status="idle")
        if self._stop.is_set():
            self._publish_report(
                claim,
                LabWorkerStopped(reason="worker stop requested before shard execution"),
            )
            return LabWorkerTickResult(status="stopped", claim_token=claim.claim_token)

        try:
            validated = self.adapter_registry.validate_claim(claim)
        except Exception as exc:
            return self._failure_result(claim, phase="claim", error=exc)

        try:
            reused = self._reuse_sealed(claim)
        except Exception as exc:
            return self._failure_result(claim, phase="seal", error=exc)
        if reused is not None:
            self._publish_report(
                claim,
                LabShardSucceeded(result_manifest_hash=reused.manifest_hash),
            )
            return LabWorkerTickResult(
                status="succeeded",
                claim_token=claim.claim_token,
                manifest_hash=reused.manifest_hash,
            )

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
            try:
                with self._open_store(validated.spec) as store:
                    result = self.adapter_registry.execute_shard(validated, store)
            except PermissionError as exc:
                return self._failure_result(claim, phase="session", error=exc)
            except Exception as exc:
                return self._failure_result(claim, phase="execute", error=exc)
            if heartbeat_errors:
                raise heartbeat_errors[0]
            try:
                manifest = self._seal_result(claim, result)
            except Exception as exc:
                return self._failure_result(claim, phase="seal", error=exc)
        finally:
            finished.set()
            heartbeat.join()

        self._publish_report(
            claim,
            LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
        )
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
