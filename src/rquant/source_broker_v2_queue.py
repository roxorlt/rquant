"""Narrow scheduler-facing queue for already-authorized SourceBroker v2 work."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker_v2 import (
    SourceBrokerV2ClaimOnceResponse,
    SourceBrokerV2DispatchRequest,
    SourceBrokerV2DispatchResponse,
    SourceBrokerV2FinalizeRequest,
    SourceBrokerV2FinalizeResponse,
    SourceBrokerV2OutboxPhase,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeEnvelope,
    SourceBrokerV2NativeEvidence,
    canonical_job_model_bytes,
    canonical_job_sha256,
    parse_job_intent,
    parse_job_outcome,
)
from rquant.source_broker_v2_runner import (
    SourceBrokerV2JobRunnerState,
    SourceBrokerV2StoreConfigError,
    load_source_broker_v2_job_store_config,
    open_source_broker_v2_job_storage_connection,
    source_broker_v2_published_commit_hash,
)
from rquant.source_operation_contracts import require_authorized_source_broker_v2_job_intent
from rquant.strict_json import strict_canonical_json_loads, strict_model_validate_canonical_json


class SourceBrokerV2SchedulerQueueError(RuntimeError):
    """Base failure for the narrow scheduler queue."""


class SourceBrokerV2SchedulerQueueConflictError(SourceBrokerV2SchedulerQueueError):
    """The operation id is already durably bound to different intent bytes."""


class SourceBrokerV2SchedulerQueueBackpressureError(SourceBrokerV2SchedulerQueueError):
    """The executor-owned inbox is at its configured active-work capacity."""


class SourceBrokerV2SchedulerQueueIntegrityError(SourceBrokerV2SchedulerQueueError):
    """Shared durable state cannot be parsed or is not internally bound."""


def _require_stage_store(stage_store: object) -> object:
    from rquant.lab_source_stage import LabSourceStageStore

    if type(stage_store) is not LabSourceStageStore:
        raise TypeError("queue requires an exact LabSourceStageStore authority")
    return stage_store


class SourceBrokerV2SchedulerQueue:
    """Exact non-executor access to canonical intent enqueue and published outcomes.

    This object deliberately owns neither provider registrations nor an execution loop.
    It only shares the runner's SQLite table and accepts the same active-inbox limit.
    """

    __slots__ = (
        "_authorization_keyring",
        "_busy_timeout_ms",
        "_manifest_keyring",
        "_stage_store",
        "_store_config",
        "db_path",
    )

    def __init__(
        self,
        db_path: Path,
        *,
        manifest_keyring: VerifyOnlyEd25519Keyring,
        authorization_keyring: VerifyOnlyEd25519Keyring,
        stage_store: object,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if not 1 <= busy_timeout_ms <= 120_000:
            raise ValueError("busy_timeout_ms must be in [1, 120000]")
        self.db_path = Path(db_path)
        if not isinstance(manifest_keyring, VerifyOnlyEd25519Keyring):
            raise TypeError("queue requires a verify-only manifest keyring")
        if not isinstance(authorization_keyring, VerifyOnlyEd25519Keyring):
            raise TypeError("queue requires a verify-only authorization keyring")
        self._manifest_keyring = manifest_keyring
        self._authorization_keyring = authorization_keyring
        self._stage_store = _require_stage_store(stage_store)
        self._busy_timeout_ms = busy_timeout_ms
        if not self.db_path.is_file():
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "runner store has not been initialized by an executor"
            )
        try:
            with open_source_broker_v2_job_storage_connection(
                self.db_path,
                busy_timeout_ms=self._busy_timeout_ms,
            ) as connection:
                self._store_config = load_source_broker_v2_job_store_config(connection)
        except (sqlite3.Error, SourceBrokerV2StoreConfigError) as exc:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "runner store configuration is unavailable"
            ) from exc

    def enqueue_intent(self, intent: SourceBrokerV2JobIntentEnvelope) -> str:
        return self.enqueue_intent_bytes(canonical_job_model_bytes(intent))

    def enqueue_intent_bytes(self, payload: bytes) -> str:
        try:
            intent = parse_job_intent(payload)
        except Exception as exc:
            raise SourceBrokerV2SchedulerQueueConflictError(
                "job intent bytes are malformed or conflicting"
            ) from exc
        try:
            require_authorized_source_broker_v2_job_intent(
                intent,
                manifest_keyring=self._manifest_keyring,
                authorization_keyring=self._authorization_keyring,
                now=datetime.now(UTC),
            )
            stage_authority_hash, stage_record_commitment = self._require_stage_intent(
                intent,
                now=datetime.now(UTC),
            )
        except Exception as exc:
            raise SourceBrokerV2SchedulerQueueConflictError(
                "job intent authorization is invalid or expired"
            ) from exc
        now = datetime.now(UTC).isoformat(timespec="microseconds")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT intent FROM source_broker_v2_jobs WHERE operation_id = ?",
                (intent.operation_id,),
            ).fetchone()
            if existing is not None:
                if bytes(existing["intent"]) == payload:
                    return intent.operation_id
                raise SourceBrokerV2SchedulerQueueConflictError(
                    "operation_id is already bound to another intent"
                )
            active = connection.execute(
                """
                SELECT COUNT(*) AS count FROM source_broker_v2_jobs
                WHERE state IN ('NEW', 'CLAIMED', 'DISPATCHING', 'RECONCILE_REQUIRED')
                """
            ).fetchone()
            if active is None or int(active["count"]) >= self._store_config.max_inbox:
                raise SourceBrokerV2SchedulerQueueBackpressureError("runner inbox is full")
            connection.execute(
                """
                INSERT INTO source_broker_v2_jobs (
                    operation_id, intent, intent_hash, source_id, operation_hash, request_hash,
                    deadline_at, stage_authority_hash, stage_record_commitment,
                    state, lease_generation, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW', 0, ?, ?)
                """,
                (
                    intent.operation_id,
                    payload,
                    intent.intent_hash,
                    intent.source_id,
                    intent.operation_hash,
                    intent.request_hash,
                    intent.deadline.isoformat(timespec="microseconds"),
                    stage_authority_hash,
                    stage_record_commitment,
                    now,
                    now,
                ),
            )
        return intent.operation_id

    def get_state(self, operation_id: str) -> SourceBrokerV2JobRunnerState:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM source_broker_v2_jobs WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        if row is None:
            raise KeyError(operation_id)
        try:
            self._require_stored_stage_intent(row, now=datetime.now(UTC))
        except Exception as exc:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "stored source-stage execution proof is unavailable or conflicts"
            ) from exc
        try:
            return SourceBrokerV2JobRunnerState(str(row["state"]))
        except ValueError as exc:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "stored job state is not recognized"
            ) from exc

    def get_verified_published_outcome(
        self,
        operation_id: str,
    ) -> SourceBrokerV2JobOutcomeEnvelope:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM source_broker_v2_jobs WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()
        if row is None or str(row["state"]) != SourceBrokerV2JobRunnerState.PUBLISHED.value:
            raise KeyError(operation_id)
        if row["outcome"] is None:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published job has no terminal outcome"
            )
        try:
            stored_commit_hash = str(row["published_commit_hash"])
            expected_commit_hash = source_broker_v2_published_commit_hash(dict(row))
            if stored_commit_hash != expected_commit_hash:
                raise ValueError("published row commitment mismatch")
            intent = parse_job_intent(bytes(row["intent"]))
            require_authorized_source_broker_v2_job_intent(
                intent,
                manifest_keyring=self._manifest_keyring,
                authorization_keyring=self._authorization_keyring,
                now=datetime.now(UTC),
            )
            self._require_stored_stage_intent(row, now=datetime.now(UTC))
            outcome = parse_job_outcome(bytes(row["outcome"]))
            dispatch = _parse_canonical(
                SourceBrokerV2DispatchResponse,
                row["dispatch_receipt"],
                label="dispatch receipt",
            )
            source_evidence = _parse_canonical(
                SourceBrokerV2NativeEvidence,
                row["source_evidence"],
                label="source evidence",
            )
            claim_evidence = _parse_canonical(
                SourceBrokerV2NativeEvidence,
                row["claim_evidence"],
                label="claim evidence",
            )
            quota_evidence = _parse_canonical(
                SourceBrokerV2NativeEvidence,
                row["quota_evidence"],
                label="quota evidence",
            )
            lineage_evidence = _parse_canonical(
                SourceBrokerV2NativeEvidence,
                row["lineage_evidence"],
                label="lineage evidence",
            )
            finalize = _parse_canonical(
                SourceBrokerV2FinalizeResponse,
                row["finalize_receipt"],
                label="finalize receipt",
            )
            claim_receipt = (
                None
                if row["claim_receipt"] is None
                else _parse_canonical(
                    SourceBrokerV2ClaimOnceResponse,
                    row["claim_receipt"],
                    label="claim receipt",
                )
            )
        except Exception as exc:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published job contains malformed canonical data"
            ) from exc
        self._require_stored_intent_binding(row, intent, requested_operation_id=operation_id)
        self._require_outcome_binding(
            intent,
            outcome,
            dispatch=dispatch,
            source_evidence=source_evidence,
            claim_evidence=claim_evidence,
            quota_evidence=quota_evidence,
            lineage_evidence=lineage_evidence,
            finalize=finalize,
            claim_receipt=claim_receipt,
        )
        return outcome

    def _require_stage_intent(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        now: datetime,
    ) -> tuple[str, str]:
        return self._stage_store.require_execution_intent(intent, now=now)

    def _require_stored_stage_intent(self, row: sqlite3.Row, *, now: datetime) -> None:
        intent = parse_job_intent(bytes(row["intent"]))
        authority_hash, record_commitment = self._stage_store.require_execution_intent(
            intent,
            now=now,
            allow_ready=True,
        )
        if (
            row["stage_authority_hash"] != authority_hash
            or row["stage_record_commitment"] != record_commitment
        ):
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "stored source-stage execution proof conflicts with current authority"
            )

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        with open_source_broker_v2_job_storage_connection(
            self.db_path,
            busy_timeout_ms=self._busy_timeout_ms,
        ) as connection:
            try:
                observed = load_source_broker_v2_job_store_config(connection)
            except SourceBrokerV2StoreConfigError as exc:
                raise SourceBrokerV2SchedulerQueueIntegrityError(
                    "runner store configuration is unavailable"
                ) from exc
            if observed != self._store_config:
                raise SourceBrokerV2SchedulerQueueIntegrityError(
                    "runner store configuration changed after queue initialization"
                )
            yield connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")

    @staticmethod
    def _require_stored_intent_binding(
        row: object,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        requested_operation_id: str,
    ) -> None:
        try:
            expected = (
                intent.operation_id,
                intent.intent_hash,
                intent.source_id,
                intent.operation_hash,
                intent.request_hash,
            )
            actual = (
                requested_operation_id,
                str(row["intent_hash"]),  # type: ignore[index]
                str(row["source_id"]),  # type: ignore[index]
                str(row["operation_hash"]),  # type: ignore[index]
                str(row["request_hash"]),  # type: ignore[index]
            )
        except (KeyError, TypeError) as exc:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "stored intent row is incomplete"
            ) from exc
        if expected != actual:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "stored intent denormalization conflicts with canonical bytes"
            )

    @staticmethod
    def _require_outcome_binding(
        intent: SourceBrokerV2JobIntentEnvelope,
        outcome: SourceBrokerV2JobOutcomeEnvelope,
        *,
        dispatch: SourceBrokerV2DispatchResponse,
        source_evidence: SourceBrokerV2NativeEvidence,
        claim_evidence: SourceBrokerV2NativeEvidence,
        quota_evidence: SourceBrokerV2NativeEvidence,
        lineage_evidence: SourceBrokerV2NativeEvidence,
        finalize: SourceBrokerV2FinalizeResponse,
        claim_receipt: SourceBrokerV2ClaimOnceResponse | None,
    ) -> None:
        expected = (
            intent.source_id,
            intent.operation_id,
            intent.operation_hash,
            intent.source_authority,
            intent.claim,
            intent.quota,
            intent.fence,
            intent.lineage,
        )
        actual = (
            outcome.source_id,
            outcome.operation_id,
            outcome.operation_hash,
            outcome.source_authority,
            outcome.claim,
            outcome.quota,
            outcome.fence,
            outcome.lineage,
        )
        if expected != actual:
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published outcome conflicts with canonical source intent"
            )
        dispatch_request = _dispatch_request(intent)
        if (
            dispatch.saga_id != intent.claim.saga_id
            or dispatch.operation_id != intent.operation_id
            or dispatch.call_id != intent.quota.parent_id
            or dispatch.request_hash != dispatch_request.request_hash
            or dispatch.response != outcome.response
            or dispatch.response_hash != outcome.response_hash
            or dispatch.outcome.value != outcome.status.value
        ):
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published dispatch receipt conflicts with outcome"
            )
        if (
            canonical_job_model_bytes(source_evidence)
            != canonical_job_model_bytes(outcome.source_evidence)
            or canonical_job_model_bytes(claim_evidence)
            != canonical_job_model_bytes(outcome.claim_evidence)
            or canonical_job_model_bytes(quota_evidence)
            != canonical_job_model_bytes(outcome.quota_evidence)
            or canonical_job_model_bytes(lineage_evidence)
            != canonical_job_model_bytes(outcome.lineage_evidence)
        ):
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published native evidence columns conflict with outcome"
            )
        finalize_request = _finalize_request(intent, dispatch)
        if (
            finalize.saga_id != intent.claim.saga_id
            or finalize.operation_id != finalize_request.operation_id
            or finalize.request_hash != finalize_request.request_hash
        ):
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published finalize receipt conflicts with source operation"
            )
        if claim_receipt is not None and (
            claim_receipt.saga_id != intent.claim.saga_id
            or claim_receipt.operation_id != intent.operation_id
            or claim_receipt.phase is not SourceBrokerV2OutboxPhase.DISPATCH
        ):
            raise SourceBrokerV2SchedulerQueueIntegrityError(
                "published claim receipt conflicts with source operation"
            )


def _parse_canonical(model: type[object], value: object, *, label: str):
    if not isinstance(value, bytes | bytearray | memoryview):
        raise ValueError(f"published {label} is missing")
    return strict_model_validate_canonical_json(model, bytes(value))


def _dispatch_request(intent: SourceBrokerV2JobIntentEnvelope) -> SourceBrokerV2DispatchRequest:
    return SourceBrokerV2DispatchRequest(
        saga_id=intent.claim.saga_id,
        operation_id=intent.operation_id,
        call_id=intent.quota.parent_id,
        attempt_identity_hash=intent.claim.attempt_identity_hash,
        claim_plan_hash=intent.claim.claim_plan_hash,
        claim_binding_hash=intent.claim.claim_binding_hash,
        manifest_hash=intent.claim.manifest_hash,
        payload=intent.request,
        claim_payload_hash=intent.claim.claim_payload_hash,
        dispatch_payload_hash=canonical_sha256(strict_canonical_json_loads(intent.request)),
    )


def _finalize_request(
    intent: SourceBrokerV2JobIntentEnvelope,
    dispatch: SourceBrokerV2DispatchResponse,
) -> SourceBrokerV2FinalizeRequest:
    return SourceBrokerV2FinalizeRequest(
        saga_id=intent.claim.saga_id,
        operation_id=canonical_job_sha256(
            {
                "contract": "rquant-source-broker-v2-job-finalize-operation/v2",
                "job_operation_id": intent.operation_id,
                "operation_hash": intent.operation_hash,
            }
        ),
        dispatch_evidence_hash=dispatch.evidence_hash,
        claim_binding_hash=intent.claim.claim_binding_hash,
    )
