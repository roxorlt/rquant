"""Publish bounded point-in-time Lab Jobs serving source generations."""

from __future__ import annotations

import hashlib
import io
import math
import os
import sqlite3
import stat
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import quote
from uuid import UUID
from zoneinfo import ZoneInfo

import pandas as pd

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    OwnerTerminalReleaseReceipt,
)
from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import (
    ExperimentRegistry,
    ExperimentRegistryReadonlyReader,
    ExperimentStatus,
)
from rquant.lab_artifact_catalog import LabArtifactDurableOwners
from rquant.lab_artifacts import LabJobArtifactStore, LabSealedJobArtifact
from rquant.lab_jobs import (
    LAB_ETA_COMPLETED_LIMIT_MAX,
    LAB_JOB_LIST_LIMIT_MAX,
    JobStatus,
    LabJobPage,
    LabJobReader,
    LabJobRecord,
    LabJobSummary,
    LabResultState,
)
from rquant.lab_worker import LabShardResultManifest
from rquant.research_run_spec import ResearchExperimentIdentity, StrategyExecutionIdentity
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
)
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    LabJobsPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingLabJobRecord, ServingProjectionPayload

StrategyProjectionReader = Callable[
    [tuple[LabJobSummary, ...], datetime],
    tuple[ServingProjectionPayload, ...],
]
PageProjectionReader = Callable[[datetime], tuple[ServingProjectionPayload, ...]]

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_PROJECTED_PARQUET_BYTES = 64 * 1024 * 1024
_SUMMARY_COLUMNS = (
    "run_id",
    "computed_at",
    "start_date",
    "end_date",
    "max_hold_days",
    "entry_mode",
    "profile_variant",
    "candidates",
    "trades",
    "trigger_rate_pct",
    "mean_ret_pct",
    "median_ret_pct",
    "win_rate_pct",
    "best_ret_pct",
    "worst_ret_pct",
    "gap_stop_rate_pct",
)
_TRADE_COLUMNS = (
    "run_id",
    "trade_id",
    "entry_mode",
    "profile_variant",
    "signal_date",
    "ts_code",
    "name",
    "entry_time",
    "entry_price_raw",
    "entry_price",
    "stop_loss_basis",
    "take_profit_basis",
    "volume_profile_lookbacks",
    "volume_profile_rr",
    "exit_time",
    "exit_price",
    "exit_reason",
    "ret_pct",
)


class LabTerminalAuthorityIntegrityError(RuntimeError):
    """Terminal Lab evidence cannot safely own a serving or retention action."""


class _FormalJobAuthority:
    def __init__(
        self,
        *,
        reader: LabJobReader,
        experiment_registry: ExperimentRegistry | ExperimentRegistryReadonlyReader,
        definition_registry: ImmutableDefinitionRegistry,
    ) -> None:
        self.reader = reader
        self.experiment_registry = experiment_registry
        self.definition_registry = definition_registry

    def succeeded_job(self, job_id: UUID, *, observed_at: datetime) -> LabJobRecord:
        observed = normalize_aware_utc(observed_at)
        job = self.reader.get_job(job_id)
        if job is None:
            raise LabTerminalAuthorityIntegrityError("terminal Lab job is missing")
        if (
            job.status is not JobStatus.SUCCEEDED
            or job.result_state is not LabResultState.SEALED
            or job.updated_at > observed
        ):
            raise LabTerminalAuthorityIntegrityError(
                "terminal authority requires a PIT succeeded sealed Lab job"
            )
        execution = job.spec.strategy_execution
        experiment = job.spec.experiment
        if (
            job.spec.schema_version != 3
            or not job.spec.catalog_owner_eligible
            or not isinstance(execution, StrategyExecutionIdentity)
            or not isinstance(experiment, ResearchExperimentIdentity)
            or experiment.schema_version != 2
            or experiment.formal_plan_id is None
        ):
            raise LabTerminalAuthorityIntegrityError(
                "terminal authority requires a current formal v3 execution identity"
            )
        attempt = self.experiment_registry.get_attempt(experiment.experiment_id)
        if attempt.spec != experiment.spec or attempt.status not in {
            ExperimentStatus.EXECUTED,
            ExperimentStatus.SUCCEEDED,
        }:
            raise LabTerminalAuthorityIntegrityError(
                "terminal Experiment attempt is absent or not accepted"
            )
        if attempt.completed_at is None or attempt.completed_at > observed:
            raise LabTerminalAuthorityIntegrityError(
                "terminal Experiment evidence is unavailable at the PIT boundary"
            )
        plan = self.experiment_registry.resolve_formal_plan_by_id(
            experiment.formal_plan_id,
            as_of=observed,
        )
        if (
            plan.schema_version != 2
            or plan.plan_id != experiment.formal_plan_id
            or plan.spec != experiment.spec
            or plan.hypothesis_variant != experiment.hypothesis_variant
            or plan.strategy_definition_fingerprint != execution.strategy_definition_fingerprint
            or plan.definition_registration_record_hash
            != execution.definition_registration_record_hash
        ):
            raise LabTerminalAuthorityIntegrityError(
                "formal Experiment plan conflicts with the Lab execution identity"
            )
        registration = self.definition_registry.read_strategy_spec(
            execution.strategy_definition_fingerprint,
            as_of=observed,
        )
        if registration is None or (
            registration.logical_id,
            registration.version,
            registration.spec.spec_fingerprint,
            registration.fingerprint,
            registration.executable_fingerprint,
            registration.candidate_schema_fingerprint,
            registration.record_hash,
            registration.registered_at,
            registration.available_at,
            registration.producer_commit,
        ) != (
            execution.strategy_id,
            execution.strategy_version,
            execution.strategy_spec_fingerprint,
            execution.strategy_definition_fingerprint,
            execution.strategy_executable_fingerprint,
            execution.candidate_schema_fingerprint,
            execution.definition_registration_record_hash,
            execution.definition_registered_at,
            execution.definition_available_at,
            execution.producer_code_commit,
        ):
            raise LabTerminalAuthorityIntegrityError(
                "Definition Registry receipt conflicts with the Lab execution identity"
            )
        return job


class TrustedLabStrategyProjectionReader:
    """Project only exact formal terminal artifacts into bounded Serving rows."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        artifact_store: LabJobArtifactStore,
        experiment_registry: ExperimentRegistry,
        definition_registry: ImmutableDefinitionRegistry,
        max_projection_jobs: int = 10,
    ) -> None:
        if not 1 <= max_projection_jobs <= LAB_JOB_LIST_LIMIT_MAX:
            raise ValueError("max_projection_jobs is outside the Lab Jobs serving bound")
        self.authority = _FormalJobAuthority(
            reader=reader,
            experiment_registry=experiment_registry,
            definition_registry=definition_registry,
        )
        self.reader = reader
        self.artifact_store = artifact_store
        self.max_projection_jobs = max_projection_jobs

    def __call__(
        self,
        summaries: tuple[LabJobSummary, ...],
        observed_at: datetime,
    ) -> tuple[ServingProjectionPayload, ...]:
        observed = normalize_aware_utc(observed_at)
        summary_rows: list[dict[str, object]] = []
        trade_rows: list[dict[str, object]] = []
        available_times: list[datetime] = []
        selected = 0
        for item in summaries:
            if item.status is not JobStatus.SUCCEEDED:
                continue
            job = self.reader.get_job(item.job_id)
            if job is None or not job.spec.catalog_owner_eligible:
                continue
            job = self.authority.succeeded_job(item.job_id, observed_at=observed)
            if job.spec.parameters.strategy_name != "n_shape":
                continue
            result = self.reader.get_result_artifact(job.job_id)
            if result is None:
                raise LabTerminalAuthorityIntegrityError(
                    "succeeded Lab job is missing final artifact evidence"
                )
            sealed = self.artifact_store.verify_sealed(result.sealed_path)
            if (
                sealed.manifest.job_id,
                sealed.manifest.spec_hash,
                sealed.manifest.code_sha,
                sealed.manifest.dataset_snapshot,
                sealed.manifest_hash,
                sealed.manifest.complete_result_hash,
                sealed.device,
                sealed.inode,
            ) != (
                job.job_id,
                job.spec_hash,
                job.spec.code_sha,
                job.spec.dataset_snapshot,
                result.manifest_hash,
                result.complete_result_hash,
                result.bundle_device,
                result.bundle_inode,
            ):
                raise LabTerminalAuthorityIntegrityError(
                    "sealed result conflicts with its accepted Lab artifact index"
                )
            frames = self._read_projection_tables(sealed)
            if set(frames) != {"summary", "trades"}:
                raise LabTerminalAuthorityIntegrityError(
                    "N-shape terminal artifact lacks exact summary/trades tables"
                )
            projected_summary, projected_trades = _nshape_projection_rows(
                job,
                summary=frames["summary"],
                trades=frames["trades"],
            )
            if len(summary_rows) + len(projected_summary) > 2_000:
                break
            if len(trade_rows) + len(projected_trades) > 10_000:
                break
            summary_rows.extend(projected_summary)
            trade_rows.extend(projected_trades)
            available_times.append(job.updated_at)
            selected += 1
            if selected >= self.max_projection_jobs:
                break
        if not available_times:
            return ()
        available_at = max(available_times)
        return (
            ServingProjectionPayload(
                table_name="strategy_summary",
                available_at=available_at,
                rows=tuple(summary_rows),
            ),
            ServingProjectionPayload(
                table_name="strategy_trade",
                available_at=available_at,
                rows=tuple(trade_rows),
            ),
        )

    def _read_projection_tables(
        self,
        sealed: LabSealedJobArtifact,
    ) -> dict[str, pd.DataFrame]:
        selected: dict[str, pd.DataFrame] = {}
        for name in ("summary", "trades"):
            relative_path = f"tables/{name}.parquet"
            manifest_file = next(
                (item for item in sealed.manifest.files if item.relative_path == relative_path),
                None,
            )
            if manifest_file is None:
                continue
            if manifest_file.size > _MAX_PROJECTED_PARQUET_BYTES:
                raise LabTerminalAuthorityIntegrityError(
                    f"{name} Parquet exceeds the bounded Serving projection budget"
                )
            identity = next(
                item for item in sealed.file_identities if item.relative_path == relative_path
            )
            selected[name] = _read_verified_parquet(
                sealed.path,
                relative_path=relative_path,
                expected_size=manifest_file.size,
                expected_sha256=manifest_file.sha256,
                expected_identity=(identity.device, identity.inode),
            )
        repeated = self.artifact_store.verify_sealed(sealed.path)
        if repeated != sealed:
            raise LabTerminalAuthorityIntegrityError(
                "sealed result generation changed while building projections"
            )
        return selected


class _TerminalReceiptJournal:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lab_job_terminal_release (
                    job_id TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    receipt_id TEXT NOT NULL,
                    PRIMARY KEY(job_id, content_sha256),
                    UNIQUE(receipt_id)
                )
                """
            )
        self.path.chmod(0o600)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(
            f"file:{quote(str(self.path), safe='/')}?mode=rwc",
            uri=True,
            timeout=30,
            isolation_level=None,
        )

    def prepare(
        self,
        *,
        job_id: UUID,
        content_sha256: str,
        receipt: OwnerTerminalReleaseReceipt,
    ) -> OwnerTerminalReleaseReceipt:
        payload = receipt.model_dump_json()
        assert receipt.receipt_id is not None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT receipt_json FROM lab_job_terminal_release
                WHERE job_id = ? AND content_sha256 = ?
                """,
                (str(job_id), content_sha256),
            ).fetchone()
            if row is not None:
                existing = OwnerTerminalReleaseReceipt.model_validate_json(row[0])
                if existing != receipt:
                    connection.rollback()
                    raise LabTerminalAuthorityIntegrityError(
                        "terminal release journal contains conflicting immutable evidence"
                    )
                connection.commit()
                return existing
            connection.execute(
                """
                INSERT INTO lab_job_terminal_release(
                    job_id, content_sha256, receipt_json, receipt_id
                ) VALUES (?, ?, ?, ?)
                """,
                (str(job_id), content_sha256, payload, receipt.receipt_id),
            )
            connection.commit()
        return receipt

    def get(
        self,
        *,
        job_id: UUID,
        content_sha256: str,
    ) -> OwnerTerminalReleaseReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT receipt_json FROM lab_job_terminal_release
                WHERE job_id = ? AND content_sha256 = ?
                """,
                (str(job_id), content_sha256),
            ).fetchone()
        return OwnerTerminalReleaseReceipt.model_validate_json(row[0]) if row is not None else None


class LabArtifactTerminalReleaseCoordinator:
    """Release only the Job owner after durable exact terminal evidence exists."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        experiment_registry: ExperimentRegistry | ExperimentRegistryReadonlyReader,
        definition_registry: ImmutableDefinitionRegistry,
        reference_store: ArtifactReferenceStore,
        journal_path: Path | None = None,
    ) -> None:
        self.authority = _FormalJobAuthority(
            reader=reader,
            experiment_registry=experiment_registry,
            definition_registry=definition_registry,
        )
        self.reference_store = reference_store
        self.journal = (
            _TerminalReceiptJournal(
                journal_path or reference_store.path.parent / "lab-job-terminal-release.sqlite3"
            )
            if isinstance(reference_store, ArtifactReferenceStore)
            else None
        )

    def __call__(
        self,
        manifest: LabShardResultManifest,
        owners: LabArtifactDurableOwners,
        observed_at: datetime,
    ) -> OwnerTerminalReleaseReceipt:
        observed = normalize_aware_utc(observed_at)
        job = self.authority.succeeded_job(owners.job_id, observed_at=observed)
        if (
            owners.job_id,
            owners.spec_hash,
            owners.plan_hash,
            manifest.job_id,
            manifest.spec_hash,
            manifest.plan_hash,
        ) != (
            job.job_id,
            job.spec_hash,
            manifest.plan_hash,
            job.job_id,
            job.spec_hash,
            owners.plan_hash,
        ):
            raise LabTerminalAuthorityIntegrityError(
                "catalog owner evidence conflicts with terminal Lab job"
            )
        evidence_sha256 = self._evidence_hash(job, manifest, owners)
        journal = self.journal
        stored = (
            None
            if journal is None
            else journal.get(
                job_id=job.job_id,
                content_sha256=manifest.manifest_hash,
            )
        )
        if stored is not None:
            if (
                stored.owner_type,
                stored.owner_id,
                stored.content_sha256,
                stored.terminal_state,
                stored.lifecycle_revision,
                stored.evidence_sha256,
            ) != (
                "job",
                str(job.job_id),
                manifest.manifest_hash,
                "succeeded",
                job.version,
                evidence_sha256,
            ):
                raise LabTerminalAuthorityIntegrityError(
                    "stored terminal receipt conflicts with current authority evidence"
                )
            self.reference_store.release_owner_terminal(stored)
            return stored
        references = tuple(
            reference
            for reference in self.reference_store.list_active_references(manifest.manifest_hash)
            if (reference.owner_type, reference.owner_id) == ("job", str(job.job_id))
        )
        if len(references) != 1:
            raise LabTerminalAuthorityIntegrityError(
                "catalog must contain exactly one active Job owner before release"
            )
        reference = references[0]
        if reference.created_at > observed:
            raise LabTerminalAuthorityIntegrityError(
                "catalog Job owner evidence is from the future"
            )
        receipt = OwnerTerminalReleaseReceipt(
            reference_id=reference.reference_id,
            owner_type="job",
            owner_id=str(job.job_id),
            content_sha256=manifest.manifest_hash,
            terminal_state="succeeded",
            lifecycle_revision=job.version,
            evidence_sha256=evidence_sha256,
            released_at=observed,
        )
        prepared = (
            receipt
            if journal is None
            else journal.prepare(
                job_id=job.job_id,
                content_sha256=manifest.manifest_hash,
                receipt=receipt,
            )
        )
        self._after_receipt_prepared(prepared)
        self.reference_store.release_owner_terminal(prepared)
        self._after_owner_released(prepared)
        return prepared

    @staticmethod
    def _evidence_hash(
        job: LabJobRecord,
        manifest: LabShardResultManifest,
        owners: LabArtifactDurableOwners,
    ) -> str:
        return canonical_sha256(
            {
                "contract": "lab-job-terminal-owner-release/v1",
                "job_id": str(job.job_id),
                "job_version": job.version,
                "job_status": job.status.value,
                "result_state": job.result_state.value,
                "spec_hash": job.spec_hash,
                "execution_identity": job.spec.strategy_execution,
                "experiment_identity": job.spec.experiment,
                "shard_manifest_hash": manifest.manifest_hash,
                "owners": owners,
            }
        )

    @staticmethod
    def _after_receipt_prepared(_receipt: OwnerTerminalReleaseReceipt) -> None:
        """Fault-injection boundary after durable intent and before owner release."""

    @staticmethod
    def _after_owner_released(_receipt: OwnerTerminalReleaseReceipt) -> None:
        """Fault-injection boundary after retention commit for crash recovery tests."""


def _read_verified_parquet(
    bundle_path: Path,
    *,
    relative_path: str,
    expected_size: int,
    expected_sha256: str,
    expected_identity: tuple[int, int],
) -> pd.DataFrame:
    parts = relative_path.split("/")
    if parts != ["tables", Path(relative_path).name]:
        raise LabTerminalAuthorityIntegrityError("unsafe projection artifact path")
    bundle_fd = os.open(
        bundle_path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    tables_fd = -1
    file_fd = -1
    try:
        tables_fd = os.open(
            "tables",
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=bundle_fd,
        )
        file_fd = os.open(
            parts[1],
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=tables_fd,
        )
        before = os.fstat(file_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or (before.st_dev, before.st_ino) != expected_identity
            or before.st_size != expected_size
        ):
            raise LabTerminalAuthorityIntegrityError(
                "projection Parquet physical identity conflicts with sealed evidence"
            )
        payload = bytearray()
        digest = hashlib.sha256()
        while True:
            chunk = os.read(file_fd, min(1024 * 1024, expected_size - len(payload) + 1))
            if not chunk:
                break
            payload.extend(chunk)
            digest.update(chunk)
            if len(payload) > expected_size:
                raise LabTerminalAuthorityIntegrityError(
                    "projection Parquet exceeds its sealed byte identity"
                )
        after = os.fstat(file_fd)
        if (
            _content_file_identity(before) != _content_file_identity(after)
            or len(payload) != expected_size
            or digest.hexdigest() != expected_sha256
        ):
            raise LabTerminalAuthorityIntegrityError("projection Parquet changed while reading")
        return pd.read_parquet(io.BytesIO(payload))
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        if tables_fd >= 0:
            os.close(tables_fd)
        os.close(bundle_fd)


def _content_file_identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_mode,
        observed.st_ino,
        observed.st_dev,
        observed.st_nlink,
        observed.st_uid,
        observed.st_gid,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _nshape_projection_rows(
    job: LabJobRecord,
    *,
    summary: pd.DataFrame,
    trades: pd.DataFrame,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    arguments = {item.name: item.value for item in job.spec.parameters.arguments}
    hold_days = tuple(arguments.get("hold_days", ()))
    if not hold_days or any(type(value) is not int for value in hold_days):
        raise LabTerminalAuthorityIntegrityError(
            "N-shape formal result lacks exact hold-day parameters"
        )
    required_summary = set(_SUMMARY_COLUMNS) - {
        "run_id",
        "computed_at",
        "start_date",
        "end_date",
        "max_hold_days",
    }
    if set(summary.columns) != required_summary:
        raise LabTerminalAuthorityIntegrityError(
            "N-shape summary schema cannot be projected exactly"
        )
    required_trades = set(_TRADE_COLUMNS) - {"run_id", "trade_id"}
    if not required_trades.issubset(trades.columns):
        raise LabTerminalAuthorityIntegrityError("N-shape trade schema cannot be projected exactly")
    occurrences: dict[tuple[str, str], int] = {}
    summary_rows: list[dict[str, object]] = []
    for raw in summary.to_dict(orient="records"):
        key = (str(raw["entry_mode"]), str(raw["profile_variant"]))
        occurrence = occurrences.get(key, 0)
        if occurrence >= len(hold_days):
            raise LabTerminalAuthorityIntegrityError(
                "N-shape summary rows exceed preregistered hold-day variants"
            )
        occurrences[key] = occurrence + 1
        row = {
            **raw,
            "run_id": str(job.job_id),
            "computed_at": job.updated_at.isoformat(),
            "start_date": job.spec.parameters.start_date.isoformat(),
            "end_date": job.spec.parameters.end_date.isoformat(),
            "max_hold_days": hold_days[occurrence],
        }
        summary_rows.append(_projection_row(row, columns=_SUMMARY_COLUMNS))
    trade_rows: list[dict[str, object]] = []
    for index, raw in enumerate(trades.to_dict(orient="records")):
        selected = {column: raw.get(column) for column in required_trades}
        selected.update(
            {
                "run_id": str(job.job_id),
                "trade_id": canonical_sha256(
                    {
                        "contract": "lab-strategy-trade-row/v1",
                        "job_id": str(job.job_id),
                        "row_index": index,
                        "row": selected,
                    }
                ),
            }
        )
        trade_rows.append(_projection_row(selected, columns=_TRADE_COLUMNS))
    return summary_rows, trade_rows


def _projection_row(
    raw: dict[str, object],
    *,
    columns: tuple[str, ...],
) -> dict[str, object]:
    return {column: _projection_scalar(column, raw[column]) for column in columns}


def _projection_scalar(column: str, value: object) -> object:
    if value is None or value is pd.NA:
        return None
    if hasattr(value, "item") and not isinstance(value, (str, bytes, date, datetime)):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    if column in {"start_date", "end_date", "signal_date"}:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value)[:10]).isoformat()
    if column in {"computed_at", "entry_time", "exit_time"}:
        parsed = value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
        if not isinstance(parsed, datetime):
            parsed = datetime.fromisoformat(str(parsed).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=_SHANGHAI)
        return parsed.astimezone(UTC).isoformat()
    if column in {"max_hold_days", "candidates", "trades"}:
        return int(value)
    if column in {
        "trigger_rate_pct",
        "mean_ret_pct",
        "median_ret_pct",
        "win_rate_pct",
        "best_ret_pct",
        "worst_ret_pct",
        "gap_stop_rate_pct",
        "entry_price_raw",
        "entry_price",
        "stop_loss_basis",
        "take_profit_basis",
        "volume_profile_rr",
        "exit_price",
        "ret_pct",
    }:
        return float(value)
    return str(value)


class LabJobsServingAuthorityIntegrityError(RuntimeError):
    """The Lab Jobs read cannot be represented as a trustworthy PIT source."""


class LabJobsServingSourceReader:
    """Read a bounded, stable Lab Jobs projection without mutating its SQLite store."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        max_jobs: int = LAB_JOB_LIST_LIMIT_MAX,
        eta_completed_limit: int = LAB_ETA_COMPLETED_LIMIT_MAX,
        strategy_projection_reader: StrategyProjectionReader | None = None,
        page_projection_reader: PageProjectionReader | None = None,
    ) -> None:
        if not isinstance(reader, LabJobReader):
            raise TypeError("reader must be LabJobReader")
        if not 1 <= max_jobs <= LAB_JOB_LIST_LIMIT_MAX:
            raise ValueError(f"max_jobs must be between 1 and {LAB_JOB_LIST_LIMIT_MAX}")
        if not 3 <= eta_completed_limit <= LAB_ETA_COMPLETED_LIMIT_MAX:
            raise ValueError(
                f"eta_completed_limit must be between 3 and {LAB_ETA_COMPLETED_LIMIT_MAX}"
            )
        self.reader = reader
        self.max_jobs = max_jobs
        self.eta_completed_limit = eta_completed_limit
        self.strategy_projection_reader = strategy_projection_reader
        self.page_projection_reader = page_projection_reader

    def __call__(self, observed_at: datetime, /) -> SourceReadResult:
        observed = normalize_aware_utc(observed_at)
        first_page = self.reader.list_jobs(limit=self.max_jobs)
        self._validate_summaries(first_page, observed_at=observed)

        records = tuple(
            ServingLabJobRecord(
                summary=summary,
                eta=self.reader.estimate_eta(
                    summary.job_id,
                    as_of=observed,
                    completed_limit=self.eta_completed_limit,
                ),
            )
            for summary in first_page.items
        )
        self._validate_eta(records, observed_at=observed)
        strategy_projections = (
            self.strategy_projection_reader(first_page.items, observed)
            if self.strategy_projection_reader is not None
            else ()
        )
        page_projections = (
            self.page_projection_reader(observed) if self.page_projection_reader is not None else ()
        )
        projections = strategy_projections + page_projections

        second_page = self.reader.list_jobs(limit=self.max_jobs)
        if second_page != first_page:
            raise LabJobsServingAuthorityIntegrityError(
                "lab jobs changed while building serving source"
            )
        repeated_strategy_projections = (
            self.strategy_projection_reader(second_page.items, observed)
            if self.strategy_projection_reader is not None
            else ()
        )
        repeated_page_projections = (
            self.page_projection_reader(observed) if self.page_projection_reader is not None else ()
        )
        if repeated_strategy_projections != strategy_projections:
            raise LabJobsServingAuthorityIntegrityError(
                "strategy projection authority changed while building serving source"
            )
        if repeated_page_projections != page_projections:
            raise LabJobsServingAuthorityIntegrityError(
                "page projection authority changed while building serving source"
            )

        payload = LabJobsPayload(
            lab_jobs=tuple(
                sorted(
                    records,
                    key=lambda record: (
                        record.summary.created_at,
                        str(record.summary.job_id),
                    ),
                    reverse=True,
                )
            ),
            projections=projections,
        )
        values: dict[str, object] = {
            "dataset_id": LAB_JOBS_DATASET_ID,
            "sequence": _sequence_for(observed),
            "event_time": max(
                (
                    timestamp
                    for record in payload.lab_jobs
                    for timestamp in (
                        record.summary.updated_at,
                        record.eta.as_of if record.eta is not None else None,
                    )
                    if timestamp is not None
                ),
                default=observed,
            ),
            "published_at": observed,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": payload,
        }
        values["generation_id"] = canonical_sha256(values)
        return SourceReadResult.model_validate(values)

    @staticmethod
    def _validate_summaries(page: LabJobPage, *, observed_at: datetime) -> None:
        if any(
            summary.created_at > observed_at or summary.updated_at > observed_at
            for summary in page.items
        ):
            raise LabJobsServingAuthorityIntegrityError("lab job summary contains future evidence")

    @staticmethod
    def _validate_eta(
        records: tuple[ServingLabJobRecord, ...],
        *,
        observed_at: datetime,
    ) -> None:
        if any(record.eta is not None and record.eta.as_of > observed_at for record in records):
            raise LabJobsServingAuthorityIntegrityError("lab job ETA contains future evidence")


class LabJobsServingAuthorityPublisher:
    """Publish one verified Lab Jobs projection through its owner authority."""

    def __init__(
        self,
        *,
        reader: LabJobsServingSourceReader,
        publisher: ServingSourceAuthorityPublisher,
    ) -> None:
        if not isinstance(reader, LabJobsServingSourceReader):
            raise TypeError("reader must be LabJobsServingSourceReader")
        if not isinstance(publisher, ServingSourceAuthorityPublisher):
            raise TypeError("publisher must be ServingSourceAuthorityPublisher")
        if publisher.dataset_id != LAB_JOBS_DATASET_ID:
            raise ValueError("publisher must own the lab_jobs dataset")
        if publisher.payload_kind != "lab_jobs":
            raise ValueError("publisher must own the lab_jobs payload kind")
        self.reader = reader
        self.publisher = publisher

    def publish(self, observed_at: datetime) -> ServingSourceAuthorityPointer:
        return self.publisher.publish(self.reader(observed_at))


def _sequence_for(observed_at: datetime) -> int:
    elapsed = observed_at - datetime(1970, 1, 1, tzinfo=UTC)
    sequence = elapsed.days * 86_400_000_000 + elapsed.seconds * 1_000_000 + elapsed.microseconds
    if sequence < 0:
        raise ValueError("observed_at must not precede the Unix epoch")
    return sequence


__all__ = [
    "LabArtifactTerminalReleaseCoordinator",
    "LabJobsServingAuthorityIntegrityError",
    "LabJobsServingAuthorityPublisher",
    "LabJobsServingSourceReader",
    "LabTerminalAuthorityIntegrityError",
    "TrustedLabStrategyProjectionReader",
]
