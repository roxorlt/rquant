"""Read-only Strategy Lab result finalization and durable commit publication."""

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
)
from rquant.lab_artifacts import (
    LabArtifactError,
    LabArtifactRecoveryAuthority,
    LabArtifactRecoveryRecord,
    LabJobArtifactCandidate,
    LabJobArtifactPlan,
    LabJobArtifactStore,
    LabSealedJobArtifact,
)
from rquant.lab_jobs import (
    COMPLETE_RESULT_CONTRACT_VERSION,
    LabFinalizationReadyEpoch,
    LabFinalizationShardEvidence,
    LabFinalizationSnapshot,
    LabJobReader,
)
from rquant.lab_shard_protocol import LabShardSucceeded
from rquant.lab_worker import LabShardResultManifest, canonical_shard_frame_json
from rquant.strategy_job_adapters import (
    LabJobExecutionResult,
    LabShardExecutionResult,
    LabShardMetric,
    LabShardTable,
    StrategyJobAdapterRegistry,
    default_strategy_job_adapter_registry,
)


class LabFinalizationError(RuntimeError):
    """Base error for independent complete-result finalization."""


class LabFinalizationIntegrityError(LabFinalizationError):
    """Accepted ledger evidence or immutable artifact bytes failed validation."""


class LabFinalizerModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=False,
    )


class LabFinalizerShardSummary(LabFinalizerModel):
    shard_index: int = Field(ge=0)
    shard_id: UUID
    result_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    metrics: tuple[LabShardMetric, ...]


class LabFinalizerTableSummary(LabFinalizerModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    row_count: int = Field(ge=0)
    columns: tuple[str, ...]


class LabFinalizerMetrics(LabFinalizerModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    result_contract_version: str = Field(min_length=1)
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    shard_count: int = Field(ge=1)
    shards: tuple[LabFinalizerShardSummary, ...]
    tables: tuple[LabFinalizerTableSummary, ...]

    @model_validator(mode="after")
    def validate_summary(self) -> LabFinalizerMetrics:
        if self.shard_count != len(self.shards):
            raise ValueError("shard_count does not match shard summaries")
        if tuple(item.shard_index for item in self.shards) != tuple(range(self.shard_count)):
            raise ValueError("shard summaries must be complete and ordered")
        if not self.tables:
            raise ValueError("finalizer metrics require complete result tables")
        return self


class LabFinalizerResult(LabFinalizerModel):
    status: Literal["not_ready", "published", "acknowledged", "rejected"]
    job_id: UUID
    request_id: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    complete_result_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    rejection_reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_result_identity(self) -> LabFinalizerResult:
        identities = (self.request_id, self.manifest_hash, self.complete_result_hash)
        if self.status == "not_ready" and any(value is not None for value in identities):
            raise ValueError("not_ready result cannot claim an artifact identity")
        if self.status != "not_ready" and any(value is None for value in identities):
            raise ValueError("published result requires a complete artifact identity")
        if (self.status == "rejected") != (self.rejection_reason is not None):
            raise ValueError("only rejected results may contain a rejection reason")
        return self


@dataclass(frozen=True)
class _PathBinding:
    parent_descriptor: int
    name: str
    descriptor: int
    observation: tuple[int, int, int, int, int, int, int]


def _observation(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_descriptor(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


class LabSealedShardBundleReader:
    """Load one exact accepted worker attempt without trusting mutable paths."""

    def __init__(self, artifact_root: Path) -> None:
        self.artifact_root = Path(artifact_root).resolve()

    @staticmethod
    def _after_file_read(_name: str) -> None:
        """Fault hook; every pathname is rebound to its read inode before return."""

    @staticmethod
    def _attempt_name(evidence: LabFinalizationShardEvidence) -> str:
        report = evidence.accepted_success.report
        return (
            f"{report.scheduler_fencing_token:020d}-"
            f"{report.claim_generation:020d}-{report.claim_token}"
        )

    @staticmethod
    def _open_directory(parent_descriptor: int, name: str) -> tuple[int, tuple[int, ...]]:
        if not name or name in {".", ".."} or "/" in name or "\x00" in name:
            raise LabFinalizationIntegrityError("unsafe shard artifact path segment")
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode):
                raise LabFinalizationIntegrityError("shard artifact ancestor is not a directory")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_descriptor,
            )
            opened = os.fstat(descriptor)
        except LabFinalizationIntegrityError:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            raise LabFinalizationIntegrityError(
                "accepted shard artifact path is unavailable"
            ) from exc
        if _observation(before) != _observation(opened):
            os.close(descriptor)
            raise LabFinalizationIntegrityError("shard artifact ancestor changed while opening")
        return descriptor, _observation(opened)

    @staticmethod
    def _read_regular_file(
        bundle_descriptor: int,
        name: str,
    ) -> tuple[bytes, tuple[int, int, int, int, int, int, int]]:
        if not name or name in {".", ".."} or "/" in name or "\x00" in name:
            raise LabFinalizationIntegrityError("unsafe shard artifact file name")
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=bundle_descriptor, follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise LabFinalizationIntegrityError("shard artifact is not a private regular file")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=bundle_descriptor,
            )
            opened = os.fstat(descriptor)
            if _observation(before) != _observation(opened):
                raise LabFinalizationIntegrityError("shard artifact changed while opening")
            payload = _read_descriptor(descriptor)
            after = os.fstat(descriptor)
            linked = os.stat(name, dir_fd=bundle_descriptor, follow_symlinks=False)
            if not (
                _observation(opened) == _observation(after) == _observation(linked)
                and len(payload) == opened.st_size
            ):
                raise LabFinalizationIntegrityError("shard artifact changed while reading")
            return payload, _observation(opened)
        except LabFinalizationIntegrityError:
            raise
        except OSError as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard artifact file is unavailable"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def read(self, evidence: LabFinalizationShardEvidence) -> LabShardExecutionResult:
        report = evidence.accepted_success.report
        body = report.body
        if not isinstance(body, LabShardSucceeded):
            raise LabFinalizationIntegrityError("accepted attempt is not shard_succeeded")
        segments = (
            "jobs",
            str(report.job_id),
            "shards",
            str(report.shard_id),
            "attempts",
            self._attempt_name(evidence),
        )
        descriptors: list[int] = []
        bindings: list[_PathBinding] = []
        file_observations: dict[str, tuple[int, int, int, int, int, int, int]] = {}
        try:
            try:
                root_descriptor = os.open(
                    self.artifact_root,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                )
            except OSError as exc:
                raise LabFinalizationIntegrityError("shard artifact root is unavailable") from exc
            descriptors.append(root_descriptor)
            root_observation = _observation(os.fstat(root_descriptor))
            parent = root_descriptor
            for segment in segments:
                child, observed = self._open_directory(parent, segment)
                descriptors.append(child)
                bindings.append(
                    _PathBinding(
                        parent_descriptor=parent,
                        name=segment,
                        descriptor=child,
                        observation=observed,
                    )
                )
                parent = child
            bundle_descriptor = descriptors[-1]
            manifest_bytes, manifest_observed = self._read_regular_file(
                bundle_descriptor,
                "manifest.json",
            )
            file_observations["manifest.json"] = manifest_observed
            self._after_file_read("manifest.json")
            try:
                manifest = LabShardResultManifest.model_validate_json(manifest_bytes)
            except Exception as exc:
                raise LabFinalizationIntegrityError("accepted shard manifest is invalid") from exc
            if manifest_bytes != manifest.canonical_json().encode("utf-8"):
                raise LabFinalizationIntegrityError("accepted shard manifest is not canonical JSON")
            expected_manifest_identity = (
                report.job_id,
                report.shard_id,
                report.claim_token,
                report.claim_generation,
                report.scheduler_fencing_token,
                report.spec_hash,
                report.payload_hash,
                evidence.shard.plan_hash,
                evidence.shard.adapter_id,
                evidence.shard.adapter_version,
            )
            actual_manifest_identity = (
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
            )
            if actual_manifest_identity != expected_manifest_identity:
                raise LabFinalizationIntegrityError(
                    "accepted shard manifest conflicts with ledger attempt identity"
                )
            if manifest.manifest_hash != body.result_manifest_hash:
                raise LabFinalizationIntegrityError(
                    "accepted shard manifest hash conflicts with success evidence"
                )
            expected_names = {"manifest.json"} | {
                artifact.file_name for artifact in manifest.artifacts
            }
            try:
                actual_names = set(os.listdir(bundle_descriptor))
            except OSError as exc:
                raise LabFinalizationIntegrityError(
                    "accepted shard inventory is unavailable"
                ) from exc
            if actual_names != expected_names:
                raise LabFinalizationIntegrityError("accepted shard bundle inventory conflicts")

            tables: list[LabShardTable] = []
            for index, artifact in enumerate(manifest.artifacts):
                if artifact.file_name != f"{index:03d}-{artifact.name}.parquet":
                    raise LabFinalizationIntegrityError("accepted shard artifact order is invalid")
                payload, observed = self._read_regular_file(
                    bundle_descriptor,
                    artifact.file_name,
                )
                file_observations[artifact.file_name] = observed
                self._after_file_read(artifact.file_name)
                if (
                    len(payload) != artifact.file_size
                    or observed[4] != artifact.file_size
                    or hashlib.sha256(payload).hexdigest() != artifact.file_sha256
                ):
                    raise LabFinalizationIntegrityError("accepted shard artifact bytes conflict")
                try:
                    frame = pd.read_parquet(io.BytesIO(payload))
                except Exception as exc:
                    raise LabFinalizationIntegrityError(
                        "accepted shard Parquet is invalid"
                    ) from exc
                if len(frame) != artifact.row_count or tuple(frame.columns) != artifact.columns:
                    raise LabFinalizationIntegrityError("accepted shard Parquet shape conflicts")
                content_hash = hashlib.sha256(
                    canonical_shard_frame_json(frame).encode("utf-8")
                ).hexdigest()
                if content_hash != artifact.content_sha256:
                    raise LabFinalizationIntegrityError("accepted shard Parquet content conflicts")
                tables.append(LabShardTable(name=artifact.name, frame=frame))

            if set(os.listdir(bundle_descriptor)) != expected_names:
                raise LabFinalizationIntegrityError(
                    "accepted shard inventory changed while reading"
                )
            for name in sorted(expected_names):
                try:
                    linked = os.stat(
                        name,
                        dir_fd=bundle_descriptor,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise LabFinalizationIntegrityError(
                        "accepted shard file identity changed while reading"
                    ) from exc
                if _observation(linked) != file_observations[name]:
                    raise LabFinalizationIntegrityError(
                        "accepted shard file identity changed while reading"
                    )
            root_path = os.lstat(self.artifact_root)
            if _observation(root_path) != root_observation:
                raise LabFinalizationIntegrityError("shard artifact root changed while reading")
            for binding in bindings:
                linked = os.stat(
                    binding.name,
                    dir_fd=binding.parent_descriptor,
                    follow_symlinks=False,
                )
                if (
                    _observation(linked) != binding.observation
                    or _observation(os.fstat(binding.descriptor)) != binding.observation
                ):
                    raise LabFinalizationIntegrityError("shard artifact path changed while reading")
            return LabShardExecutionResult(
                shard_id=manifest.shard_id,
                spec_hash=manifest.spec_hash,
                payload_hash=manifest.payload_hash,
                plan_hash=manifest.plan_hash,
                adapter_id=manifest.adapter_id,
                adapter_version=manifest.adapter_version,
                tables=tuple(tables),
                metrics=manifest.metrics,
            )
        except LabFinalizationIntegrityError:
            raise
        except Exception as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard bundle could not be safely reconstructed"
            ) from exc
        finally:
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)


class LabFinalizer:
    """Finalize one ready job without ever opening a writable SQLite connection."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        shard_artifact_root: Path,
        artifact_store: LabJobArtifactStore,
        commit_spool: LabArtifactCommitSpool,
        adapter_registry: StrategyJobAdapterRegistry | None = None,
    ) -> None:
        self.reader = reader
        self.bundle_reader = LabSealedShardBundleReader(shard_artifact_root)
        self.artifact_store = artifact_store
        self.commit_spool = commit_spool
        self.adapter_registry = adapter_registry or default_strategy_job_adapter_registry()

    @staticmethod
    def _after_candidate_prepared(_candidate: LabJobArtifactCandidate) -> None:
        """Fault-injection boundary after durable candidate publication."""

    @staticmethod
    def _after_artifact_sealed(_sealed: LabSealedJobArtifact) -> None:
        """Fault-injection boundary after immutable job artifact sealing."""

    @staticmethod
    def _after_commit_published(
        _published: LabArtifactCommitSpoolEntry | LabAcknowledgedArtifactCommit,
    ) -> None:
        """Fault-injection boundary after durable commit-spool publication."""

    @staticmethod
    def _metrics(
        snapshot: LabFinalizationSnapshot,
        result: LabJobExecutionResult,
        shard_results: tuple[LabShardExecutionResult, ...],
    ) -> LabFinalizerMetrics:
        first = snapshot.shards[0].shard
        return LabFinalizerMetrics(
            job_id=snapshot.job.job_id,
            spec_hash=snapshot.job.spec_hash,
            plan_hash=first.plan_hash,
            adapter_id=first.adapter_id,
            adapter_version=first.adapter_version,
            result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
            result_hash=result.result_hash,
            shard_count=len(snapshot.shards),
            shards=tuple(
                LabFinalizerShardSummary(
                    shard_index=evidence.shard.shard_index,
                    shard_id=evidence.shard.shard_id,
                    result_manifest_hash=evidence.shard.result_manifest_hash or "",
                    metrics=shard_result.metrics,
                )
                for evidence, shard_result in zip(
                    snapshot.shards,
                    shard_results,
                    strict=True,
                )
            ),
            tables=tuple(
                LabFinalizerTableSummary(
                    name=table.name,
                    row_count=len(table.frame),
                    columns=tuple(table.frame.columns),
                )
                for table in result.tables
            ),
        )

    @staticmethod
    def _report(metrics: LabFinalizerMetrics) -> str:
        lines = [
            "# Strategy Lab Complete Result",
            "",
            f"- Job: `{metrics.job_id}`",
            f"- Spec: `{metrics.spec_hash}`",
            f"- Plan: `{metrics.plan_hash}`",
            f"- Adapter: `{metrics.adapter_id}@{metrics.adapter_version}`",
            f"- Result contract: `{metrics.result_contract_version}`",
            f"- Result hash: `{metrics.result_hash}`",
            f"- Shards: {metrics.shard_count}",
            "",
            "## Tables",
            "",
        ]
        lines.extend(
            f"- `{table.name}`: {table.row_count} rows; columns="
            f"{json.dumps(table.columns, ensure_ascii=True, separators=(',', ':'))}"
            for table in metrics.tables
        )
        lines.extend(["", "## Shard Metrics", ""])
        for shard in metrics.shards:
            rendered = json.dumps(
                [metric.model_dump(mode="json") for metric in shard.metrics],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            lines.append(
                f"- {shard.shard_index} `{shard.shard_id}` "
                f"manifest=`{shard.result_manifest_hash}` metrics={rendered}"
            )
        return "\n".join(lines) + "\n"

    @staticmethod
    def _authority(plan: LabJobArtifactPlan) -> LabArtifactRecoveryAuthority:
        manifest = plan.manifest
        return LabArtifactRecoveryAuthority(
            job_id=manifest.job_id,
            spec_hash=manifest.spec_hash,
            plan_hash=manifest.plan_hash,
            adapter_id=manifest.adapter_id,
            adapter_version=manifest.adapter_version,
            result_contract_version=manifest.result_contract_version,
            code_sha=manifest.code_sha,
            dataset_snapshot=manifest.dataset_snapshot,
            expected_manifest_hash=plan.manifest_hash,
        )

    @staticmethod
    def _related_recovery_records(
        plan: LabJobArtifactPlan,
        records: tuple[LabArtifactRecoveryRecord, ...],
    ) -> tuple[LabArtifactRecoveryRecord, ...]:
        prefix = f"{plan.job_id.hex}-"
        related = tuple(
            record
            for record in records
            if record.status != "quarantined"
            and (record.job_id == plan.job_id or record.path.name.startswith(prefix))
        )
        for record in related:
            if record.status in {"recoverable", "needs_authority", "recoverable_torn"} and (
                record.job_id != plan.job_id or record.manifest_hash != plan.manifest_hash
            ):
                raise LabFinalizationIntegrityError(
                    "job candidate recovery conflicts with current aggregate result"
                )
        priority = {"recoverable_torn": 0, "recoverable": 1, "needs_authority": 2}
        return tuple(
            sorted(
                related,
                key=lambda record: (
                    priority.get(record.status, 3),
                    record.path.name,
                ),
            )
        )

    def _verify_or_recover_sealed(self, plan: LabJobArtifactPlan) -> LabSealedJobArtifact | None:
        target = self.artifact_store.sealed_root / plan.job_id.hex
        if not os.path.lexists(target):
            return None
        try:
            sealed = self.artifact_store.recover_interrupted_seal(target)
        except LabArtifactError as interrupted_error:
            try:
                sealed = self.artifact_store.verify_sealed(target)
            except LabArtifactError as verify_error:
                raise LabFinalizationIntegrityError(
                    "existing sealed job artifact is neither complete nor recoverable"
                ) from ExceptionGroup(
                    "sealed artifact recovery and verification failed",
                    [interrupted_error, verify_error],
                )
        if sealed.manifest != plan.manifest or sealed.manifest_hash != plan.manifest_hash:
            raise LabFinalizationIntegrityError(
                "existing sealed artifact conflicts with deterministic finalization output"
            )
        return sealed

    def _recover_candidate_from_plan(
        self,
        plan: LabJobArtifactPlan,
    ) -> LabSealedJobArtifact | None:
        authority = self._authority(plan)
        records = self._related_recovery_records(
            plan,
            self.artifact_store.list_candidate_recovery(),
        )

        if any(record.status == "invalid" for record in records):
            raise LabFinalizationIntegrityError(
                "job candidate recovery contains invalid filesystem evidence"
            )
        recoverable = tuple(
            record
            for record in records
            if record.status in {"recoverable", "needs_authority", "recoverable_torn"}
        )
        if recoverable:
            primary, *redundant = recoverable
            try:
                sealed = self.artifact_store.recover_candidate(
                    primary,
                    authority=authority,
                )
            except LabArtifactError as exc:
                raise LabFinalizationIntegrityError(
                    "matching job candidate could not be safely recovered"
                ) from exc
            for record in redundant:
                try:
                    self.artifact_store.quarantine_recovery_record(
                        record,
                        reason="redundant deterministic candidate after recovery",
                    )
                except LabArtifactError as exc:
                    raise LabFinalizationIntegrityError(
                        "redundant matching candidate could not be safely isolated"
                    ) from exc
            if sealed.manifest != plan.manifest or sealed.manifest_hash != plan.manifest_hash:
                raise LabFinalizationIntegrityError(
                    "recovered job artifact conflicts with deterministic finalization output"
                )
            return sealed
        return None

    def _prepare_and_seal(self, plan: LabJobArtifactPlan) -> LabSealedJobArtifact:
        try:
            candidate = self.artifact_store.prepare_candidate_from_plan(plan)
        except LabArtifactError as exc:
            raise LabFinalizationIntegrityError(
                "complete result candidate could not be prepared"
            ) from exc
        self._after_candidate_prepared(candidate)
        try:
            sealed = self.artifact_store.seal_candidate(candidate)
        except LabArtifactError as exc:
            primary_error = LabFinalizationIntegrityError("job artifact could not be sealed")
            primary_error.__cause__ = exc
            try:
                self._isolate_owned_candidate(candidate)
            except Exception as cleanup_error:
                raise ExceptionGroup(
                    "finalization failed and owned candidate isolation failed",
                    [primary_error, cleanup_error],
                ) from None
            raise primary_error from exc
        if sealed.manifest != plan.manifest or sealed.manifest_hash != plan.manifest_hash:
            raise LabFinalizationIntegrityError(
                "sealed job artifact conflicts with deterministic finalization output"
            )
        return sealed

    def _recover_or_prepare(self, plan: LabJobArtifactPlan) -> LabSealedJobArtifact:
        sealed = self._verify_or_recover_sealed(plan)
        if sealed is not None:
            return sealed
        sealed = self._recover_candidate_from_plan(plan)
        if sealed is not None:
            return sealed
        return self._prepare_and_seal(plan)

    def _isolate_owned_candidate(self, candidate: LabJobArtifactCandidate) -> None:
        if not os.path.lexists(candidate.path):
            return
        matching = tuple(
            record
            for record in self.artifact_store.list_candidate_recovery()
            if (
                record.path == candidate.path
                and record.device == candidate.device
                and record.inode == candidate.inode
            )
        )
        if len(matching) != 1:
            raise LabFinalizationIntegrityError(
                "owned finalization candidate cannot be uniquely bound for isolation"
            )
        self.artifact_store.quarantine_recovery_record(
            matching[0],
            reason="owned candidate isolated after finalization conflict",
        )

    @staticmethod
    def _envelope(
        sealed: LabSealedJobArtifact,
        ready_epoch: LabFinalizationReadyEpoch,
    ) -> LabArtifactCommitEnvelope:
        manifest = sealed.manifest
        commit = LabArtifactCommit(
            job_id=manifest.job_id,
            spec_hash=manifest.spec_hash,
            plan_hash=manifest.plan_hash,
            adapter_id=manifest.adapter_id,
            adapter_version=manifest.adapter_version,
            result_contract_version=manifest.result_contract_version,
            code_sha=manifest.code_sha,
            dataset_snapshot=manifest.dataset_snapshot,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=manifest.complete_result_hash,
            sealed_path=sealed.path,
        )
        commit_identity = hashlib.sha256(commit.canonical_json_bytes()).hexdigest()
        request_id = uuid5(
            NAMESPACE_URL,
            "rquant:lab-artifact-commit:v2:"
            f"{manifest.job_id}:{ready_epoch.job_version}:"
            f"{ready_epoch.event.event_id}:{commit_identity}",
        )
        return LabArtifactCommitEnvelope(request_id=request_id, commit=commit)

    def finalize(self, job_id: UUID) -> LabFinalizerResult:
        snapshot = self.reader.get_finalization_snapshot(job_id)
        if snapshot is None:
            return LabFinalizerResult(status="not_ready", job_id=job_id)
        try:
            shard_results = tuple(self.bundle_reader.read(evidence) for evidence in snapshot.shards)
            result = self.adapter_registry.aggregate_results(snapshot.job.spec, shard_results)
        except LabFinalizationIntegrityError:
            raise
        except Exception as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard results could not be aggregated"
            ) from exc
        metrics = self._metrics(snapshot, result, shard_results)
        first = snapshot.shards[0].shard
        try:
            plan = self.artifact_store.preview_candidate(
                job_id=snapshot.job.job_id,
                spec=snapshot.job.spec,
                plan_hash=first.plan_hash,
                adapter_id=first.adapter_id,
                adapter_version=first.adapter_version,
                result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
                metrics=metrics.model_dump(mode="json"),
                report_markdown=self._report(metrics),
                tables={table.name: table.frame for table in result.tables},
            )
        except LabArtifactError as exc:
            raise LabFinalizationIntegrityError(
                "complete result candidate could not be previewed"
            ) from exc
        sealed = self._recover_or_prepare(plan)
        self._after_artifact_sealed(sealed)
        envelope = self._envelope(sealed, snapshot.ready_epoch)
        published = self.commit_spool.publish(envelope)
        self._after_commit_published(published)
        rejected = (
            isinstance(published, LabAcknowledgedArtifactCommit)
            and published.receipt.status == "rejected"
        )
        return LabFinalizerResult(
            status=(
                "rejected"
                if rejected
                else (
                    "acknowledged"
                    if isinstance(published, LabAcknowledgedArtifactCommit)
                    else "published"
                )
            ),
            job_id=job_id,
            request_id=envelope.request_id,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            rejection_reason=published.receipt.reason if rejected else None,
        )
