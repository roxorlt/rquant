from __future__ import annotations

import json
import re
import sqlite3
from base64 import b64encode
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from typing import cast
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import rquant.lab_jobs as lab_jobs
from rquant.adapter_manifest import AdapterManifest, PydanticModelSchema
from rquant.experiment_registry import DateRange, ExperimentSpec
from rquant.lab_artifact_protocol import LabArtifactCommitReceipt
from rquant.lab_claim_publication import (
    ClaimPublicationStatus,
)
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    LabCommandReceipt,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    COMPLETE_RESULT_CONTRACT_VERSION,
    CancelConfirmationRequiredError,
    ControlIntent,
    FormalSubmissionAuthorityError,
    InvalidJobTransitionError,
    InvalidStoredJobError,
    JobStatus,
    LabArtifactRecord,
    LabCommandRecord,
    LabDatabaseIdentityError,
    LabEventRecord,
    LabJobReader,
    LabJobRecord,
    LabJobStore,
    LabLeaseRecord,
    LabResultState,
    LabShardRecord,
    SchedulerLeaseFencedError,
    SchedulerLeaseUnavailableError,
    StaleJobVersionError,
)
from rquant.lab_shard_protocol import (
    LabShardClaimV2,
    LabShardDefinition,
    LabShardWorkPlan,
    StrategyShardPayloadV2,
)
from rquant.lab_source_stage import LabSourceStageState, LabSourceStageStore
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchExperimentIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
    StrategyExecutionIdentity,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker_v2_job_protocol import canonical_job_sha256
from rquant.source_operation_contracts import (
    SourceBrokerV2SchedulerIntentTemplate,
    SourceIntentV2,
    SourceOperationContractError,
    SourceResourceRequestV2,
    build_source_broker_v2_scheduler_intent,
    issue_scheduler_intent_authorization_v1,
)
from rquant.strict_json import canonical_json_bytes, canonical_model_json_bytes
from tests.unit.source_broker_v2_authorized_intent_fixture import authorized_payload_and_claim
from tests.unit.test_adapter_manifest import Authorities, create_test_authorities

NOW = datetime(2026, 7, 24, 1, 0, tzinfo=UTC)
OLD_V1_SPEC_JSON = (
    '{"schema_version":1,"job_type":"strategy_replay","parameters":{"strategy_name":'
    '"n_shape","start_date":"2026-04-01","end_date":"2026-07-14","arguments":[]},'
    '"code_sha":"1111111111111111111111111111111111111111","dataset_snapshot":'
    '{"snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    '"binding_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},'
    '"feature_contract":{"contract_id":"intraday-core","contract_version":"v1",'
    '"contract_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},'
    '"execution_costs":{"commission_bps":"2.5","stamp_duty_bps":"5",'
    '"transfer_fee_bps":"0.1","slippage_bps":"3"},"random_seed":20260724,'
    '"resource_class":"standard","deadline":"2026-07-25T02:00:00Z",'
    '"research_status":"comparable"}'
)
OLD_V1_SPEC_HASH = "bab8a079dd4cbad1a7e8343d2872d0f87707945f416af1e3eb088af13c367f3b"
OLD_V1_COMMAND_HASH = "65c3859a9f38541641cf9b87093042ed863c451c5bb69a0bfd0053b07d86eace"


def _assert_current_schema_rejection(
    action: Callable[[], object],
    *,
    cause_match: str,
) -> None:
    with pytest.raises(LabDatabaseIdentityError, match="v16 current schema is invalid") as raised:
        action()
    cause = raised.value.__cause__
    assert isinstance(cause, LabDatabaseIdentityError)
    assert re.search(cause_match, str(cause))


class _StagedLifecycleConnection:
    def __init__(
        self,
        *,
        commit_error: BaseException | None = None,
        rollback_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.commit_error = commit_error
        self.rollback_error = rollback_error
        self.close_error = close_error
        self.calls: list[str] = []

    def commit(self) -> None:
        self.calls.append("commit")
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.calls.append("rollback")
        if self.rollback_error is not None:
            raise self.rollback_error

    def close(self) -> None:
        self.calls.append("close")
        if self.close_error is not None:
            raise self.close_error


class _FinalizationSnapshotFaultCursor:
    def __init__(self, row: object | None) -> None:
        self.row = row

    def fetchone(self) -> object | None:
        return self.row


class _FinalizationSnapshotFaultConnection:
    def __init__(
        self,
        *,
        query_error: BaseException | None = None,
        rollback_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.query_error = query_error
        self.rollback_error = rollback_error
        self.close_error = close_error
        self.in_transaction = False
        self.calls: list[str] = []

    def execute(self, statement: str, _parameters: object = ()) -> object:
        normalized = " ".join(statement.split())
        if normalized == "BEGIN":
            self.calls.append("begin")
            self.in_transaction = True
            return _FinalizationSnapshotFaultCursor(None)
        if normalized.startswith("SELECT * FROM lab_job"):
            self.calls.append("query")
            if self.query_error is not None:
                raise self.query_error
            return _FinalizationSnapshotFaultCursor(None)
        if normalized == "COMMIT":
            self.calls.append("commit")
            self.in_transaction = False
            return _FinalizationSnapshotFaultCursor(None)
        raise AssertionError(f"unexpected SQL: {normalized}")

    def rollback(self) -> None:
        self.calls.append("rollback")
        if self.rollback_error is not None:
            raise self.rollback_error
        self.in_transaction = False

    def close(self) -> None:
        self.calls.append("close")
        if self.close_error is not None:
            raise self.close_error


class _ArtifactCommitFaultConnection:
    def __init__(
        self,
        *,
        query_error: BaseException | None = None,
        rollback_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.query_error = query_error
        self.rollback_error = rollback_error
        self.close_error = close_error
        self.in_transaction = False
        self.calls: list[str] = []

    def execute(self, statement: str, _parameters: object = ()) -> object:
        normalized = " ".join(statement.split())
        if normalized == "BEGIN":
            self.calls.append("begin")
            self.in_transaction = True
            return _FinalizationSnapshotFaultCursor(None)
        if normalized.startswith("SELECT * FROM lab_artifact_commit"):
            self.calls.append("query")
            if self.query_error is not None:
                raise self.query_error
            return _FinalizationSnapshotFaultCursor(None)
        if normalized == "COMMIT":
            self.calls.append("commit")
            self.in_transaction = False
            return _FinalizationSnapshotFaultCursor(None)
        raise AssertionError(f"unexpected SQL: {normalized}")

    def rollback(self) -> None:
        self.calls.append("rollback")
        if self.rollback_error is not None:
            raise self.rollback_error
        self.in_transaction = False

    def close(self) -> None:
        self.calls.append("close")
        if self.close_error is not None:
            raise self.close_error


def _staged_receipt() -> LabArtifactCommitReceipt:
    return LabArtifactCommitReceipt(
        request_id=uuid4(),
        content_hash="a" * 64,
        job_id=uuid4(),
        status="accepted",
        reason="artifact_committed",
        accepted_at=NOW,
        job_version=1,
    )


def _flatten_exception_messages(exc: BaseException) -> tuple[str, ...]:
    if isinstance(exc, BaseExceptionGroup):
        return tuple(
            message for nested in exc.exceptions for message in _flatten_exception_messages(nested)
        )
    return (str(exc),)


def _spec(
    *,
    job_type: ResearchJobType = ResearchJobType.STRATEGY_REPLAY,
    resource_class: ResourceClass = ResourceClass.STANDARD,
) -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=job_type,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
            audit_run_id="d" * 64,
        ),
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="c" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
        ),
        random_seed=20260724,
        resource_class=resource_class,
        deadline=datetime(2026, 7, 25, 2, tzinfo=UTC),
        research_status="exploratory",
    )


def _formal_v2_spec() -> ResearchRunSpec:
    return _spec().model_copy(update={"research_status": "comparable"})


def _formal_v3_spec() -> ResearchRunSpec:
    base = _formal_v2_spec()
    execution = StrategyExecutionIdentity(
        strategy_id=base.parameters.strategy_name,
        strategy_version=1,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        strategy_spec_fingerprint="2" * 64,
        strategy_definition_fingerprint="3" * 64,
        strategy_executable_fingerprint="4" * 64,
        candidate_schema_fingerprint="5" * 64,
        definition_registration_record_hash="6" * 64,
        definition_registered_at=NOW - timedelta(days=2),
        definition_available_at=NOW - timedelta(days=1),
        producer_code_commit=base.code_sha,
    )
    assert base.dataset_snapshot is not None
    experiment_spec = ExperimentSpec(
        strategy_spec_fingerprint=execution.strategy_spec_fingerprint,
        strategy_executable_fingerprint=execution.strategy_executable_fingerprint,
        candidate_schema_fingerprint=execution.candidate_schema_fingerprint,
        dataset_snapshot_id=base.dataset_snapshot.snapshot_id,
        code_commit=base.code_sha,
        parameter_fingerprint=canonical_sha256(base.parameters),
        hypothesis_family="lab-jobs-v3",
        metric_definition_fingerprint="7" * 64,
        train_range=DateRange(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 6, 30),
        ),
        validation_range=DateRange(
            start_date=date(2025, 7, 1),
            end_date=date(2025, 12, 31),
        ),
        frozen_outer_test_range=DateRange(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 31),
        ),
        cost_model_fingerprint=canonical_sha256(base.execution_costs),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": execution.adapter_id,
                "adapter_version": execution.adapter_version,
                "feature_contract": base.feature_contract,
            }
        ),
        seed=base.random_seed,
    )
    assert experiment_spec.experiment_id is not None
    return base.model_copy(
        update={
            "schema_version": 3,
            "strategy_execution": execution,
            "experiment": ResearchExperimentIdentity(
                schema_version=2,
                spec=experiment_spec,
                experiment_id=experiment_spec.experiment_id,
                hypothesis_family=experiment_spec.hypothesis_family,
                hypothesis_variant="baseline",
                formal_plan_id="8" * 64,
            ),
        }
    )


def _v1_spec() -> ResearchRunSpec:
    return ResearchRunSpec.model_validate_json(OLD_V1_SPEC_JSON)


def _hidden_audit_v1_spec() -> ResearchRunSpec:
    base = _v1_spec()
    assert base.dataset_snapshot is not None
    hidden_snapshot = DatasetSnapshotIdentity.model_construct(
        snapshot_id=base.dataset_snapshot.snapshot_id,
        binding_hash=base.dataset_snapshot.binding_hash,
        audit_run_id="e" * 64,
        _fields_set={"snapshot_id", "binding_hash"},
    )
    values = {name: getattr(base, name) for name in type(base).model_fields}
    values["dataset_snapshot"] = hidden_snapshot
    return ResearchRunSpec.model_construct(
        **values,
        _fields_set=set(base.model_fields_set),
    )


def _submit(
    *,
    request_id: UUID | None = None,
    job_id: UUID | None = None,
    spec: ResearchRunSpec | None = None,
    max_attempts: int = 3,
) -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=request_id or uuid4(),
        command=SubmitJobCommand(
            job_id=job_id or uuid4(),
            spec=spec or _spec(),
            max_attempts=max_attempts,
        ),
    )


def _store(tmp_path: Path, *, timeout: int = 1_234) -> LabJobStore:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3", busy_timeout_ms=timeout)
    store.initialize()
    return store


def _register_unprivileged_job_functions(connection: sqlite3.Connection) -> None:
    connection.create_function(
        lab_jobs._ARTIFACT_SUCCESS_AUTH_FUNCTION,
        5,
        lambda *_args: 0,
    )
    connection.create_function(
        lab_jobs._RETRY_AUTH_FUNCTION,
        3,
        lambda *_args: 0,
    )
    connection.create_function(
        lab_jobs._READY_TERMINAL_AUTH_FUNCTION,
        6,
        lambda *_args: 0,
    )


def _lease(
    store: LabJobStore,
    *,
    owner: str = "scheduler-a",
    now: datetime = NOW,
    seconds: int = 60,
) -> LabLeaseRecord:
    return store.acquire_scheduler_lease(
        owner_id=owner,
        lease_seconds=seconds,
        now=now,
    )


def _v2_definition() -> LabShardDefinition:
    schema = PydanticModelSchema(
        model_name="rquant.test.SourcePayload",
        schema_hash="a" * 64,
    )
    manifest = AdapterManifest(
        issuer="test-release-authority",
        key_id="test-manifest-v2",
        signature=b64encode(b"x" * 64).decode("ascii"),
        adapter_id="research.daily-bars",
        adapter_version="2.1.0",
        adapter_code_hash="b" * 64,
        network="provider",
        source="test-source",
        operation="daily-bars",
        cost_per_call=1,
        max_calls=1,
        request_schema=schema,
        response_schema=schema,
    )
    source_intent = SourceIntentV2.from_manifest(
        manifest,
        resource_request=SourceResourceRequestV2.from_manifest(manifest, requested_calls=1),
    )
    payload = StrategyShardPayloadV2.from_source_intent(
        adapter_id=manifest.adapter_id,
        adapter_version=manifest.adapter_version,
        payload_json='{"partition":"2026-07-24"}',
        source_intent=source_intent,
    )
    return LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id=payload.adapter_id,
        adapter_version=payload.adapter_version,
        plan_hash="c" * 64,
        payload_json=payload.model_dump_json(round_trip=True),
        work_plan=LabShardWorkPlan(
            phase="strategy_replay",
            work_unit_name="symbol",
            work_units=1,
            static_duration_ms=1_000,
        ),
    )


def _real_v2_definition(
    tmp_path: Path,
    *,
    payload_transform: Callable[[StrategyShardPayloadV2, Authorities], StrategyShardPayloadV2]
    | None = None,
) -> tuple[LabShardDefinition, Authorities]:
    """Create a production-shaped signed v2 payload without a pre-bound claim."""

    authorities = create_test_authorities(tmp_path / "preclaim-authorities")
    payload, _claim = authorized_payload_and_claim(
        now=NOW + timedelta(seconds=2),
        authority_set=authorities,
        plan_hash="c" * 64,
        shard_index=0,
        payload_json='{"partition":"2026-07-24"}',
    )
    if payload_transform is not None:
        payload = payload_transform(payload, authorities)
    return (
        LabShardDefinition.from_payload(
            shard_index=0,
            adapter_id=payload.adapter_id,
            adapter_version=payload.adapter_version,
            plan_hash="c" * 64,
            payload_json=payload.model_dump_json(round_trip=True),
            work_plan=LabShardWorkPlan(
                phase="strategy_replay",
                work_unit_name="symbol",
                work_units=1,
                static_duration_ms=1_000,
            ),
        ),
        authorities,
    )


def _reissue_scheduler_authorization(
    payload: StrategyShardPayloadV2,
    authorities: Authorities,
    *,
    valid_from: datetime,
    expires_at: datetime,
) -> StrategyShardPayloadV2:
    unsigned = payload.model_copy(update={"scheduler_intent_authorization": None})
    return unsigned.with_scheduler_intent_authorization(
        issue_scheduler_intent_authorization_v1(
            unsigned,
            signer=authorities.scheduler_intent,
            valid_from=valid_from,
            expires_at=expires_at,
        )
    )


def _real_v2_preclaim(
    authorities: Authorities,
) -> Callable[[StrategyShardPayloadV2, LabShardClaimV2, datetime], None]:
    def verify(
        payload: StrategyShardPayloadV2,
        claim: LabShardClaimV2,
        now: datetime,
    ) -> None:
        build_source_broker_v2_scheduler_intent(
            payload,
            claim=claim,
            manifest_keyring=authorities.authorization_keyring,
            authorization_keyring=authorities.authorization_keyring,
            deadline=now + timedelta(seconds=60),
            now=now,
        )

    return verify


def _invalid_manifest_signature(
    payload: StrategyShardPayloadV2,
    _authorities: Authorities,
) -> StrategyShardPayloadV2:
    source_intent = payload.source_intent.model_copy(
        update={
            "manifest": payload.source_intent.manifest.model_copy(
                update={"signature": b64encode(b"z" * 64).decode("ascii")}
            )
        }
    )
    template = payload.scheduler_intent_template
    assert template is not None
    replacement_template = SourceBrokerV2SchedulerIntentTemplate.from_source_intent(
        source_intent=source_intent,
        source_id=template.source_id,
        request=template.request,
        deadline_offset_seconds=template.deadline_offset_seconds,
        saga_id=template.saga_id,
        source_authority=template.source_authority,
        claim_authority=template.claim_authority,
        quota_parent_id=template.quota_parent_id,
        quota_authority=template.quota_authority,
        lineage_id=template.lineage_id,
        lineage_authority=template.lineage_authority,
        fence_external_root_hash=template.fence_external_root_hash,
    )
    altered = StrategyShardPayloadV2.from_source_intent(
        adapter_id=payload.adapter_id,
        adapter_version=payload.adapter_version,
        payload_json=payload.payload_json,
        source_intent=source_intent,
        scheduler_intent_template=replacement_template,
    )
    return _reissue_scheduler_authorization(
        altered,
        _authorities,
        valid_from=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(minutes=5),
    )


def _expired_scheduler_authorization(
    payload: StrategyShardPayloadV2,
    authorities: Authorities,
) -> StrategyShardPayloadV2:
    return _reissue_scheduler_authorization(
        payload,
        authorities,
        valid_from=NOW - timedelta(minutes=2),
        expires_at=NOW + timedelta(seconds=1),
    )


def _not_yet_valid_scheduler_authorization(
    payload: StrategyShardPayloadV2,
    authorities: Authorities,
) -> StrategyShardPayloadV2:
    return _reissue_scheduler_authorization(
        payload,
        authorities,
        valid_from=NOW + timedelta(seconds=3),
        expires_at=NOW + timedelta(minutes=5),
    )


def _legacy_missing_scheduler_authorization(
    payload: StrategyShardPayloadV2,
    _authorities: Authorities,
) -> StrategyShardPayloadV2:
    return payload.model_copy(update={"scheduler_intent_authorization": None})


def _missing_scheduler_template(
    payload: StrategyShardPayloadV2,
    _authorities: Authorities,
) -> StrategyShardPayloadV2:
    return payload.model_copy(
        update={
            "scheduler_intent_authorization": None,
            "scheduler_intent_template": None,
        }
    )


def _future_signed_availability_request(
    payload: StrategyShardPayloadV2,
    authorities: Authorities,
) -> StrategyShardPayloadV2:
    template = payload.scheduler_intent_template
    assert template is not None
    future = (NOW + timedelta(days=1)).date()
    future_request = template.request.model_copy(update={"requested_end": future, "as_of": future})
    altered = payload.model_copy(
        update={
            "scheduler_intent_authorization": None,
            "scheduler_intent_template": template.model_copy(
                update={
                    "request": future_request,
                    "request_hash": canonical_job_sha256(future_request.canonical_bytes),
                }
            ),
        }
    )
    return _reissue_scheduler_authorization(
        altered,
        authorities,
        valid_from=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(minutes=5),
    )


def _v2_definitions(count: int) -> tuple[LabShardDefinition, ...]:
    definition = _v2_definition()
    return tuple(
        LabShardDefinition.from_payload(
            shard_index=index,
            adapter_id=definition.adapter_id,
            adapter_version=definition.adapter_version,
            plan_hash=definition.plan_hash,
            payload_json=definition.payload_json,
            work_plan=definition.work_plan,
        )
        for index in range(count)
    )


def _v1_definitions(count: int) -> tuple[LabShardDefinition, ...]:
    return tuple(
        LabShardDefinition.from_payload(
            shard_index=index,
            adapter_id="research.local",
            adapter_version="1.0.0",
            plan_hash="3" * 64,
            payload_json=json.dumps({"partition": f"2026-07-{index + 1:02d}"}),
            work_plan=LabShardWorkPlan(
                phase="strategy_replay",
                work_unit_name="symbol",
                work_units=1,
                static_duration_ms=1_000,
            ),
        )
        for index in range(count)
    )


def _source_stage_store(tmp_path: Path) -> LabSourceStageStore:
    tmp_path.mkdir(parents=True, exist_ok=True)
    queue_path = tmp_path / "source-broker-v2.sqlite3"
    store_id = canonical_sha256({"path": str(queue_path.resolve())})
    config_hash = canonical_sha256(
        {
            "contract": "rquant-source-broker-v2-job-store-config/v2",
            "max_inbox": 100,
            "schema_version": 2,
            "store_id": store_id,
        }
    )
    with sqlite3.connect(queue_path) as connection:
        connection.execute(
            """
            CREATE TABLE source_broker_v2_store_config (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                store_id TEXT NOT NULL,
                max_inbox INTEGER NOT NULL,
                config_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE source_broker_v2_jobs (
                operation_id TEXT PRIMARY KEY NOT NULL,
                intent BLOB NOT NULL,
                intent_hash TEXT NOT NULL,
                source_id TEXT NOT NULL,
                operation_hash TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                deadline_at TEXT NOT NULL,
                state TEXT NOT NULL,
                lease_generation INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_broker_v2_store_config (
                singleton, schema_version, store_id, max_inbox, config_hash, created_at
            ) VALUES (1, 2, ?, 100, ?, ?)
            """,
            (store_id, config_hash, NOW.isoformat()),
        )
    return LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
    )


def _plan_v2_job(store: LabJobStore, lease: LabLeaseRecord) -> LabJobRecord:
    job = _submit_job(store, lease)
    store.plan_job(
        job.job_id,
        (_v2_definition(),),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    return job


def test_claim_next_shard_skips_rejected_v2_candidate_for_later_v1_in_same_tick(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    v2 = _v2_definition()
    v1_template = _v1_definitions(2)[1]
    v1 = LabShardDefinition.from_payload(
        shard_index=v1_template.shard_index,
        adapter_id=v1_template.adapter_id,
        adapter_version=v1_template.adapter_version,
        plan_hash=v2.plan_hash,
        payload_json=v1_template.payload_json,
        work_plan=v1_template.work_plan,
    )
    store.plan_job(
        job.job_id,
        (v2, v1),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    calls: list[UUID] = []

    def reject_v2(
        _payload: StrategyShardPayloadV2,
        claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        calls.append(claim.shard_id)
        raise SourceOperationContractError("signature is invalid")

    selection = store.claim_next_shard(
        worker_id="mixed-worker",
        shard_lease_seconds=90,
        source_stage_store=_source_stage_store(tmp_path),
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=reject_v2,
        include_diagnostics=True,
    )

    assert isinstance(selection, lab_jobs.LabClaimSelection)
    assert isinstance(selection.claim, lab_jobs.LabShardClaim)
    assert selection.claim.definition.shard_id == v1.shard_id
    assert calls == [v2.shard_id]
    assert len(selection.rejections) == 1
    assert selection.rejections[0].shard_id == v2.shard_id
    shards = LabJobReader(store.path).list_shards(job.job_id)
    rejected = next(shard for shard in shards if shard.shard_id == v2.shard_id)
    assert rejected.status is lab_jobs.ShardStatus.QUEUED
    assert rejected.attempt_count == 0


def test_claim_next_shard_returns_bounded_rejections_without_mutating_all_blocked_batch(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    definitions = _v2_definitions(lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE + 1)
    store.plan_job(job.job_id, definitions, lease=lease, now=NOW + timedelta(seconds=1))
    calls: list[UUID] = []

    def reject_v2(
        _payload: StrategyShardPayloadV2,
        claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        calls.append(claim.shard_id)
        raise ValueError("expired")

    source_stage_store = _source_stage_store(tmp_path)
    selection = store.claim_next_shard(
        worker_id="blocked-worker",
        shard_lease_seconds=90,
        source_stage_store=source_stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=reject_v2,
        include_diagnostics=True,
    )

    assert isinstance(selection, lab_jobs.LabClaimSelection)
    assert selection.claim is None
    assert len(calls) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE
    assert len(selection.rejections) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE
    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted is not None and persisted.status is JobStatus.QUEUED
    shards = LabJobReader(store.path).list_shards(job.job_id)
    assert all(shard.status is lab_jobs.ShardStatus.QUEUED for shard in shards)
    assert all(shard.attempt_count == 0 for shard in shards)

    later_job = _submit_job(store, lease)
    store.plan_job(
        later_job.job_id,
        _v1_definitions(1),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    later = store.claim_next_shard(
        worker_id="fair-worker",
        shard_lease_seconds=30,
        source_stage_store=source_stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=4),
        v2_precondition=reject_v2,
    )
    assert isinstance(later, lab_jobs.LabShardClaim)
    assert later.job_id == later_job.job_id


@pytest.mark.parametrize("failure", [RuntimeError("unexpected"), KeyboardInterrupt()])
def test_claim_next_shard_rolls_back_unexpected_v2_precondition_failures(
    tmp_path: Path,
    failure: BaseException,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _plan_v2_job(store, lease)

    def fail_v2(
        _payload: StrategyShardPayloadV2,
        _claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        raise failure

    with pytest.raises(type(failure)):
        store.claim_next_shard(
            worker_id="failure-worker",
            shard_lease_seconds=90,
            source_stage_store=_source_stage_store(tmp_path),
            source_wait_deadline=NOW + timedelta(seconds=30),
            publication_deadline=NOW + timedelta(seconds=60),
            lease=lease,
            now=NOW + timedelta(seconds=2),
            v2_precondition=fail_v2,
        )

    shard = LabJobReader(store.path).list_shards(job.job_id)[0]
    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert shard.status is lab_jobs.ShardStatus.QUEUED
    assert shard.attempt_count == 0
    assert persisted is not None and persisted.status is JobStatus.QUEUED


@pytest.mark.parametrize(
    ("variant", "payload_transform"),
    (
        pytest.param(
            "invalid_manifest_signature",
            _invalid_manifest_signature,
            id="invalid_manifest_signature",
        ),
        pytest.param(
            "expired_authorization",
            _expired_scheduler_authorization,
            id="expired_authorization",
        ),
        pytest.param(
            "not_yet_valid_authorization",
            _not_yet_valid_scheduler_authorization,
            id="not_yet_valid_authorization",
        ),
        pytest.param(
            "legacy_missing_authorization",
            _legacy_missing_scheduler_authorization,
            id="legacy_missing_authorization",
        ),
        pytest.param(
            "missing_scheduler_template",
            _missing_scheduler_template,
            id="missing_scheduler_template",
        ),
        pytest.param(
            "future_signed_availability",
            _future_signed_availability_request,
            id="future_signed_availability",
        ),
    ),
)
def test_real_b2a_preclaim_rejection_preserves_attempt_and_claims_later_v1_same_tick(
    tmp_path: Path,
    variant: str,
    payload_transform: Callable[[StrategyShardPayloadV2, Authorities], StrategyShardPayloadV2],
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    rejected_definition, authorities = _real_v2_definition(
        tmp_path,
        payload_transform=payload_transform,
    )
    v1_template = _v1_definitions(2)[1]
    later_v1 = LabShardDefinition.from_payload(
        shard_index=v1_template.shard_index,
        adapter_id=v1_template.adapter_id,
        adapter_version=v1_template.adapter_version,
        plan_hash=rejected_definition.plan_hash,
        payload_json=v1_template.payload_json,
        work_plan=v1_template.work_plan,
    )
    store.plan_job(
        job.job_id,
        (rejected_definition, later_v1),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    reader = LabJobReader(store.path)
    events_before = reader.list_events(job.job_id)
    with sqlite3.connect(store.path) as connection:
        ledger_entries_before = connection.execute(
            "SELECT COUNT(*) FROM lab_ledger_chain_entry"
        ).fetchone()[0]

    selection = store.claim_next_shard(
        worker_id=f"preclaim-{variant}",
        shard_lease_seconds=90,
        source_stage_store=_source_stage_store(tmp_path),
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=_real_v2_preclaim(authorities),
        include_diagnostics=True,
    )

    assert isinstance(selection, lab_jobs.LabClaimSelection)
    assert isinstance(selection.claim, lab_jobs.LabShardClaim)
    assert selection.claim.definition.shard_id == later_v1.shard_id
    assert len(selection.rejections) == 1
    assert selection.rejections[0].shard_id == rejected_definition.shard_id
    rejected = next(
        shard
        for shard in reader.list_shards(job.job_id)
        if shard.shard_id == rejected_definition.shard_id
    )
    assert rejected.status is lab_jobs.ShardStatus.QUEUED
    assert rejected.attempt_count == 0
    assert rejected.claim_generation == 0
    events_after = reader.list_events(job.job_id)
    assert events_after[: len(events_before)] == events_before
    assert all("preclaim" not in event.reason for event in events_after[len(events_before) :])
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_claim_publication").fetchone()[0] == 0
        assert (
            connection.execute("SELECT COUNT(*) FROM lab_ledger_chain_entry").fetchone()[0]
            > ledger_entries_before
        )


def test_concurrent_schedulers_cas_claim_one_v2_held_attempt_without_consuming_loser(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path, timeout=5_000)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    definition, authorities = _real_v2_definition(tmp_path)
    store.plan_job(job.job_id, (definition,), lease=lease, now=NOW + timedelta(seconds=1))
    stage_store = _source_stage_store(tmp_path)
    barrier = Barrier(2)

    def claim(worker_id: str) -> LabShardClaimV2 | None:
        barrier.wait()
        candidate = LabJobStore(store.path, busy_timeout_ms=5_000).claim_next_shard(
            worker_id=worker_id,
            shard_lease_seconds=90,
            source_stage_store=stage_store,
            source_wait_deadline=NOW + timedelta(seconds=30),
            publication_deadline=NOW + timedelta(seconds=60),
            lease=lease,
            now=NOW + timedelta(seconds=2),
            v2_precondition=_real_v2_preclaim(authorities),
        )
        return candidate if isinstance(candidate, LabShardClaimV2) else None

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(claim, ("scheduler-a", "scheduler-b")))

    winners = tuple(item for item in outcomes if item is not None)
    assert len(winners) == 1
    shard = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert shard.attempt_count == 1
    assert shard.claim_generation == 1
    publication = store.get_claim_publication(winners[0].claim_token)
    assert publication is not None and publication.status is ClaimPublicationStatus.HELD_SOURCE


def test_v2_precondition_runs_before_the_preclaim_write_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    definition, authorities = _real_v2_definition(tmp_path)
    store.plan_job(job.job_id, (definition,), lease=lease, now=NOW + timedelta(seconds=1))
    active_write_transaction = False
    original_transaction = LabJobStore._transaction

    @contextmanager
    def traced_transaction(self: LabJobStore):
        nonlocal active_write_transaction
        with original_transaction(self) as connection:
            active_write_transaction = True
            try:
                yield connection
            finally:
                active_write_transaction = False

    monkeypatch.setattr(LabJobStore, "_transaction", traced_transaction)

    def precondition(
        payload: StrategyShardPayloadV2,
        claim: LabShardClaimV2,
        current: datetime,
    ) -> None:
        assert not active_write_transaction
        _real_v2_preclaim(authorities)(payload, claim, current)

    claim = store.claim_next_shard(
        worker_id="scheduler-a",
        shard_lease_seconds=90,
        source_stage_store=_source_stage_store(tmp_path),
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=precondition,
    )
    assert isinstance(claim, LabShardClaimV2)


def test_preclaim_cursor_revalidates_33_keyring_candidates_after_recovery(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    definition, authorities = _real_v2_definition(tmp_path)
    definitions = tuple(
        LabShardDefinition.from_payload(
            shard_index=index,
            adapter_id=definition.adapter_id,
            adapter_version=definition.adapter_version,
            plan_hash=definition.plan_hash,
            payload_json=definition.payload_json,
            work_plan=definition.work_plan,
        )
        for index in range(lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE + 1)
    )
    store.plan_job(job.job_id, definitions, lease=lease, now=NOW + timedelta(seconds=1))
    stage_store = _source_stage_store(tmp_path)

    def old_keyring(
        _payload: StrategyShardPayloadV2,
        _claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        raise SourceOperationContractError("unknown signing key")

    blocked = store.claim_next_shard(
        worker_id="scheduler-a",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=old_keyring,
        include_diagnostics=True,
    )
    assert isinstance(blocked, lab_jobs.LabClaimSelection)
    assert blocked.claim is None
    assert len(blocked.rejections) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE

    recovered = store.claim_next_shard(
        worker_id="scheduler-b",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=3),
        v2_precondition=_real_v2_preclaim(authorities),
    )
    assert isinstance(recovered, LabShardClaimV2)
    assert recovered.definition.shard_id == definitions[-1].shard_id

    wrapped = store.claim_next_shard(
        worker_id="scheduler-c",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=4),
        v2_precondition=_real_v2_preclaim(authorities),
    )
    assert isinstance(wrapped, LabShardClaimV2)
    assert wrapped.definition.shard_id == definitions[0].shard_id
    first = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert first.attempt_count == 1 and first.claim_generation == 1


def test_preclaim_cursor_reaches_same_job_v1_after_32_rejected_v2_candidates(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    template, _authorities = _real_v2_definition(tmp_path)
    rejected = tuple(
        LabShardDefinition.from_payload(
            shard_index=index,
            adapter_id=template.adapter_id,
            adapter_version=template.adapter_version,
            plan_hash=template.plan_hash,
            payload_json=template.payload_json,
            work_plan=template.work_plan,
        )
        for index in range(lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE)
    )
    v1_template = _v1_definitions(1)[0]
    later_v1 = LabShardDefinition.from_payload(
        shard_index=lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE,
        adapter_id=v1_template.adapter_id,
        adapter_version=v1_template.adapter_version,
        plan_hash=template.plan_hash,
        payload_json=v1_template.payload_json,
        work_plan=v1_template.work_plan,
    )
    store.plan_job(job.job_id, (*rejected, later_v1), lease=lease, now=NOW + timedelta(seconds=1))
    stage_store = _source_stage_store(tmp_path)

    def reject_v2(
        _payload: StrategyShardPayloadV2,
        _claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        raise ValueError("keyring unavailable")

    first = store.claim_next_shard(
        worker_id="cursor-a",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=reject_v2,
        include_diagnostics=True,
    )
    assert isinstance(first, lab_jobs.LabClaimSelection)
    assert first.claim is None
    assert len(first.rejections) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE

    second = store.claim_next_shard(
        worker_id="cursor-b",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=3),
        v2_precondition=reject_v2,
    )
    assert isinstance(second, lab_jobs.LabShardClaim)
    assert second.definition.shard_id == later_v1.shard_id


def test_preclaim_cursor_reaches_same_job_v1_after_64_rejected_v2_candidates(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    template, _authorities = _real_v2_definition(tmp_path)
    rejected = tuple(
        LabShardDefinition.from_payload(
            shard_index=index,
            adapter_id=template.adapter_id,
            adapter_version=template.adapter_version,
            plan_hash=template.plan_hash,
            payload_json=template.payload_json,
            work_plan=template.work_plan,
        )
        for index in range(lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE * 2)
    )
    v1_template = _v1_definitions(1)[0]
    later_v1 = LabShardDefinition.from_payload(
        shard_index=len(rejected),
        adapter_id=v1_template.adapter_id,
        adapter_version=v1_template.adapter_version,
        plan_hash=template.plan_hash,
        payload_json=v1_template.payload_json,
        work_plan=v1_template.work_plan,
    )
    store.plan_job(job.job_id, (*rejected, later_v1), lease=lease, now=NOW + timedelta(seconds=1))
    stage_store = _source_stage_store(tmp_path)

    def reject_v2(
        _payload: StrategyShardPayloadV2,
        _claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        raise ValueError("keyring unavailable")

    for offset in range(2):
        selection = store.claim_next_shard(
            worker_id=f"cursor-{offset}",
            shard_lease_seconds=90,
            source_stage_store=stage_store,
            source_wait_deadline=NOW + timedelta(seconds=30),
            publication_deadline=NOW + timedelta(seconds=60),
            lease=lease,
            now=NOW + timedelta(seconds=2 + offset),
            v2_precondition=reject_v2,
            include_diagnostics=True,
        )
        assert isinstance(selection, lab_jobs.LabClaimSelection)
        assert selection.claim is None
        assert len(selection.rejections) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE

    claimed = store.claim_next_shard(
        worker_id="cursor-final",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=4),
        v2_precondition=reject_v2,
    )
    assert isinstance(claimed, lab_jobs.LabShardClaim)
    assert claimed.definition.shard_id == later_v1.shard_id


def test_preclaim_fair_cursor_rechecks_old_recovered_v2_despite_newer_candidates(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    old_job = _submit_job(store, lease)
    template, authorities = _real_v2_definition(tmp_path)
    old_definitions = tuple(
        LabShardDefinition.from_payload(
            shard_index=index,
            adapter_id=template.adapter_id,
            adapter_version=template.adapter_version,
            plan_hash=template.plan_hash,
            payload_json=template.payload_json,
            work_plan=template.work_plan,
        )
        for index in range(lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE)
    )
    store.plan_job(old_job.job_id, old_definitions, lease=lease, now=NOW + timedelta(seconds=1))
    stage_store = _source_stage_store(tmp_path)
    recovered = False

    def precondition(
        _payload: StrategyShardPayloadV2,
        claim: LabShardClaimV2,
        _now: datetime,
    ) -> None:
        if recovered and claim.definition.shard_id == old_definitions[0].shard_id:
            _real_v2_preclaim(authorities)(_payload, claim, _now)
            return
        raise ValueError("keyring unavailable")

    first = store.claim_next_shard(
        worker_id="fair-old",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
        v2_precondition=precondition,
        include_diagnostics=True,
    )
    assert isinstance(first, lab_jobs.LabClaimSelection)
    assert len(first.rejections) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE

    for index in range(2):
        envelope = _submit()
        store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=10 + index))
        newer = LabJobReader(store.path).get_job(envelope.command.job_id)
        assert newer is not None
        store.plan_job(
            newer.job_id,
            tuple(
                LabShardDefinition.from_payload(
                    shard_index=shard_index,
                    adapter_id=template.adapter_id,
                    adapter_version=template.adapter_version,
                    plan_hash=template.plan_hash,
                    payload_json=template.payload_json,
                    work_plan=template.work_plan,
                )
                for shard_index in range(lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE)
            ),
            lease=lease,
            now=NOW + timedelta(seconds=12 + index),
        )
        blocked = store.claim_next_shard(
            worker_id=f"fair-new-{index}",
            shard_lease_seconds=90,
            source_stage_store=stage_store,
            source_wait_deadline=NOW + timedelta(seconds=30),
            publication_deadline=NOW + timedelta(seconds=60),
            lease=lease,
            now=NOW + timedelta(seconds=14 + index),
            v2_precondition=precondition,
            include_diagnostics=True,
        )
        assert isinstance(blocked, lab_jobs.LabClaimSelection)
        assert len(blocked.rejections) == lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE

    recovered = True
    claimed = store.claim_next_shard(
        worker_id="fair-recovered",
        shard_lease_seconds=90,
        source_stage_store=stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=20),
        v2_precondition=precondition,
    )
    assert isinstance(claimed, LabShardClaimV2)
    assert claimed.definition.shard_id == old_definitions[0].shard_id


def test_initialize_migrates_v13_preclaim_cursor_to_shard_position(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        for column in (
            "claim_cursor_sequence",
            "claim_cursor_shard_id",
            "claim_cursor_shard_index",
        ):
            connection.execute(f"ALTER TABLE lab_scheduler_state DROP COLUMN {column}")
        connection.execute("PRAGMA user_version = 13")

    LabJobStore(store.path).initialize()

    with sqlite3.connect(store.path) as connection:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(lab_scheduler_state)").fetchall()
        }
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
    assert {
        "claim_cursor_shard_index",
        "claim_cursor_shard_id",
        "claim_cursor_sequence",
    } <= columns


def test_preclaim_candidate_query_uses_protocol_index_without_temp_sort(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    store.plan_job(
        job.job_id,
        _v2_definitions(2),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )

    with store._read_transaction() as connection:
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM lab_shard INDEXED BY ix_lab_shard_preclaim_candidate
            WHERE job_id = ? AND status = 'queued'
              AND payload_protocol_version IN (?, ?)
              AND attempt_count < max_attempts
            ORDER BY shard_index, shard_id
            LIMIT ?
            """,
            (str(job.job_id), 1, 2, lab_jobs.PRECLAIM_CANDIDATE_BATCH_SIZE),
        ).fetchall()

    details = "\n".join(str(row[3]) for row in plan).upper()
    assert "IX_LAB_SHARD_PRECLAIM_CANDIDATE" in details
    assert "JSON" not in details
    assert "TEMP B-TREE" not in details


def test_v2_claim_atomically_creates_held_publication_and_stays_invisible(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    _plan_v2_job(store, lease)
    source_stage_store = _source_stage_store(tmp_path)

    claim = store.claim_next_shard(
        worker_id="source-worker",
        shard_lease_seconds=90,
        source_stage_store=source_stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )

    assert isinstance(claim, LabShardClaimV2)
    assert claim.source_use_plan is None
    publication = store.get_claim_publication(claim.claim_token)
    assert publication is not None
    assert publication.status is ClaimPublicationStatus.HELD_SOURCE
    assert publication.claim_preimage_bytes == canonical_model_json_bytes(claim)
    assert publication.source_stage_authority_bytes == canonical_model_json_bytes(
        source_stage_store.authority
    )
    assert (
        store.list_active_claims(
            lease,
            now=NOW + timedelta(seconds=3),
            initial_lease_seconds=90,
        )
        == ()
    )


@pytest.mark.parametrize("failure", ["missing_store", "held_write"])
def test_v2_claim_rolls_back_without_consuming_attempt_on_held_failure(
    tmp_path: Path,
    failure: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _plan_v2_job(store, lease)
    source_stage_store = _source_stage_store(tmp_path)
    arguments: dict[str, object] = {
        "worker_id": "source-worker",
        "shard_lease_seconds": 90,
        "source_wait_deadline": NOW + timedelta(seconds=30),
        "publication_deadline": NOW + timedelta(seconds=60),
        "lease": lease,
        "now": NOW + timedelta(seconds=2),
    }
    if failure == "missing_store":
        expected = "source_stage_store"
    else:
        arguments["source_stage_store"] = source_stage_store
        expected = "held publication fault"

    if failure == "missing_store":
        with pytest.raises(ValueError, match=expected):
            store.claim_next_shard(**arguments)  # type: ignore[arg-type]
    else:
        with (
            patch.object(
                store,
                "_create_held_claim_publication_in_transaction",
                side_effect=RuntimeError(expected),
            ),
            pytest.raises(RuntimeError, match=expected),
        ):
            store.claim_next_shard(**arguments)  # type: ignore[arg-type]

    shard = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert shard.status.value == "queued"
    assert shard.attempt_count == 0
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_claim_publication").fetchone()[0] == 0
    successful = store.claim_next_shard(
        worker_id="source-worker",
        shard_lease_seconds=90,
        source_stage_store=source_stage_store,
        source_wait_deadline=NOW + timedelta(seconds=30),
        publication_deadline=NOW + timedelta(seconds=60),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert isinstance(successful, LabShardClaimV2)
    assert successful.claim_generation == 1


def test_v2_claim_deadline_must_fit_its_explicit_publication_window(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _plan_v2_job(store, lease)

    with pytest.raises(ValueError, match="publication_deadline"):
        store.claim_next_shard(
            worker_id="source-worker",
            shard_lease_seconds=30,
            source_stage_store=_source_stage_store(tmp_path),
            source_wait_deadline=NOW + timedelta(seconds=33),
            publication_deadline=NOW + timedelta(seconds=33),
            lease=lease,
            now=NOW + timedelta(seconds=2),
        )

    shard = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert shard.status.value == "queued"
    assert shard.attempt_count == 0


def _publication_at_status(
    tmp_path: Path,
    status: ClaimPublicationStatus,
) -> tuple[LabJobStore, LabLeaseRecord, LabShardClaimV2, LabSourceStageStore]:
    from tests.unit import test_lab_claim_publication as publication_test

    store, lease, _claim, preimage, held, authorities = publication_test._claimed_attempt(tmp_path)
    source_store = publication_test._source_stage_store(tmp_path)
    if status is ClaimPublicationStatus.HELD_SOURCE:
        return store, lease, preimage, source_store
    queue = publication_test._queue_binding(preimage)
    _queued, writer = publication_test._queue(store, lease, held, queue, source_store)
    if status is ClaimPublicationStatus.SOURCE_QUEUED:
        return store, lease, preimage, source_store
    signed_plan, final_claim = publication_test._ready_inputs(
        preimage,
        queue,
        authorities,
        tmp_path,
        source_store,
        writer,
    )
    publication_test._ready(
        store,
        lease,
        held,
        signed_plan,
        final_claim,
        authorities,
        tmp_path,
    )
    if status is ClaimPublicationStatus.READY_TO_PUBLISH:
        return store, lease, preimage, source_store
    if status is ClaimPublicationStatus.PUBLISHED:
        store.publish_claim_publication(
            held.identity,
            publication_test._typed_receipt(tmp_path, final_claim),
            current_claim_authority=publication_test._current_claim_authority(
                tmp_path, authorities
            ),
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool_receipt_verifier=publication_test._typed_receipt_verifier(tmp_path),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
        return store, lease, preimage, source_store
    if status is ClaimPublicationStatus.ABORTED:
        store.abort_claim_publication(
            held.identity,
            terminal_reason="test_abort",
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
        return store, lease, preimage, source_store
    raise AssertionError(status)


@pytest.mark.parametrize(
    ("status", "visible"),
    [
        (ClaimPublicationStatus.HELD_SOURCE, False),
        (ClaimPublicationStatus.SOURCE_QUEUED, False),
        (ClaimPublicationStatus.ABORTED, False),
        (ClaimPublicationStatus.READY_TO_PUBLISH, True),
        (ClaimPublicationStatus.PUBLISHED, True),
    ],
)
def test_v2_active_claim_visibility_uses_real_final_ledger_claim_bytes(
    tmp_path: Path,
    status: ClaimPublicationStatus,
    visible: bool,
) -> None:
    store, lease, claim, _source_store = _publication_at_status(tmp_path, status)
    publication = store.get_claim_publication(claim.claim_token)
    assert publication is not None and publication.status is status

    active = store.list_active_claims(
        lease,
        now=NOW + timedelta(seconds=6),
        initial_lease_seconds=120,
    )

    if not visible:
        assert active == ()
        return
    assert publication.final_claim_bytes is not None
    exact_final_claim = LabShardClaimV2.model_validate_json(
        publication.final_claim_bytes,
        strict=True,
    )
    assert active == (exact_final_claim,)


def test_active_v2_claims_use_one_deferred_joined_publication_query(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease)
    store.plan_job(
        job.job_id,
        _v2_definitions(3),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    source_stage_store = _source_stage_store(tmp_path)
    for index in range(3):
        claim = store.claim_next_shard(
            worker_id=f"source-worker-{index}",
            shard_lease_seconds=90,
            source_stage_store=source_stage_store,
            source_wait_deadline=NOW + timedelta(seconds=30),
            publication_deadline=NOW + timedelta(seconds=60),
            lease=lease,
            now=NOW + timedelta(seconds=2),
        )
        assert isinstance(claim, LabShardClaimV2)

    statements: list[str] = []
    connect = store._connect

    def traced_connect(*, validate_identity: bool = True) -> sqlite3.Connection:
        connection = connect(validate_identity=validate_identity)
        connection.set_trace_callback(statements.append)
        return connection

    with patch.object(store, "_connect", side_effect=traced_connect):
        assert (
            store.list_active_claims(
                lease,
                now=NOW + timedelta(seconds=3),
                initial_lease_seconds=90,
            )
            == ()
        )

    publication_reads = [
        statement
        for statement in statements
        if "FROM LAB_CLAIM_PUBLICATION" in statement.upper()
        or "JOIN LAB_CLAIM_PUBLICATION" in statement.upper()
    ]
    assert len(publication_reads) == 1
    assert "LEFT JOIN LAB_CLAIM_PUBLICATION" in publication_reads[0].upper()
    assert "BEGIN DEFERRED" in {statement.upper() for statement in statements}
    assert "BEGIN IMMEDIATE" not in {statement.upper() for statement in statements}


def test_stale_recovery_is_bounded_and_leaves_v2_publication_fenced(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    v1_envelope = _submit()
    assert store.apply_command(v1_envelope, lease=lease, now=NOW).status == "applied"
    v1_job = LabJobReader(store.path).get_job(v1_envelope.command.job_id)
    assert v1_job is not None
    definition_count = lab_jobs.STALE_RECOVERY_BATCH_SIZE + 1
    store.plan_job(
        v1_job.job_id,
        _v1_definitions(definition_count),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    for index in range(definition_count):
        claim = store.claim_next_shard(
            worker_id=f"legacy-worker-{index}",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=2),
        )
        assert isinstance(claim, lab_jobs.LabShardClaim)

    v2_envelope = _submit()
    assert (
        store.apply_command(v2_envelope, lease=lease, now=NOW + timedelta(seconds=3)).status
        == "applied"
    )
    v2_job = LabJobReader(store.path).get_job(v2_envelope.command.job_id)
    assert v2_job is not None
    store.plan_job(
        v2_job.job_id,
        (_v2_definition(),),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    source_stage_store = _source_stage_store(tmp_path)
    v2_claim = store.claim_next_shard(
        worker_id="held-v2-worker",
        shard_lease_seconds=30,
        source_stage_store=source_stage_store,
        source_wait_deadline=NOW + timedelta(seconds=20),
        publication_deadline=NOW + timedelta(seconds=25),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert isinstance(v2_claim, LabShardClaimV2)

    statements: list[str] = []
    connect = store._connect

    def traced_connect(*, validate_identity: bool = True) -> sqlite3.Connection:
        connection = connect(validate_identity=validate_identity)
        connection.set_trace_callback(statements.append)
        return connection

    expired_at = NOW + timedelta(seconds=36)
    with patch.object(store, "_connect", side_effect=traced_connect):
        assert store.recover_stale_shards(lease, now=expired_at) == (v1_job.job_id,)
        first_batch = LabJobReader(store.path).list_shards(v1_job.job_id)
        assert sum(shard.status.value == "queued" for shard in first_batch) == (
            lab_jobs.STALE_RECOVERY_BATCH_SIZE
        )
        assert sum(shard.status.value == "running" for shard in first_batch) == 1
        assert store.recover_stale_shards(lease, now=expired_at) == (v1_job.job_id,)
        assert all(
            shard.status.value == "queued"
            for shard in LabJobReader(store.path).list_shards(v1_job.job_id)
        )
        assert store.recover_stale_shards(lease, now=expired_at) == ()

    assert LabJobReader(store.path).list_shards(v2_job.job_id)[0].status.value == "running"
    assert not any("FROM LAB_CLAIM_PUBLICATION" in statement.upper() for statement in statements)
    assert not any("OFFSET" in statement.upper() for statement in statements)
    reclaimed = store.claim_next_shard(
        worker_id="legacy-retry-worker",
        shard_lease_seconds=30,
        lease=lease,
        now=expired_at,
    )
    assert isinstance(reclaimed, lab_jobs.LabShardClaim)
    assert reclaimed.claim_generation == 2


def test_stale_recovery_uses_v1_protocol_index_with_v2_majority(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    v1_job = _submit_job(store, lease)
    v1_count = lab_jobs.STALE_RECOVERY_BATCH_SIZE + 1
    store.plan_job(
        v1_job.job_id,
        _v1_definitions(v1_count),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    for index in range(v1_count):
        assert isinstance(
            store.claim_next_shard(
                worker_id=f"legacy-plan-worker-{index}",
                shard_lease_seconds=30,
                lease=lease,
                now=NOW + timedelta(seconds=2),
            ),
            lab_jobs.LabShardClaim,
        )

    v2_job = _submit_job(store, lease)
    v2_count = lab_jobs.STALE_RECOVERY_BATCH_SIZE * 2
    store.plan_job(
        v2_job.job_id,
        _v2_definitions(v2_count),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    source_stage_store = _source_stage_store(tmp_path)
    for index in range(v2_count):
        assert isinstance(
            store.claim_next_shard(
                worker_id=f"v2-plan-worker-{index}",
                shard_lease_seconds=90,
                source_stage_store=source_stage_store,
                source_wait_deadline=NOW + timedelta(seconds=30),
                publication_deadline=NOW + timedelta(seconds=60),
                lease=lease,
                now=NOW + timedelta(seconds=3),
            ),
            LabShardClaimV2,
        )

    with store._read_transaction() as connection:
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT s.shard_id FROM lab_shard AS s
            JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE s.status = ?
              AND j.status = ?
              AND s.payload_protocol_version = 1
              AND (
                s.scheduler_fencing_token IS NULL
                OR s.scheduler_fencing_token <> ?
                OR s.lease_expires_at IS NULL
                OR s.lease_expires_at <= ?
              )
            ORDER BY s.job_id, s.shard_index, s.shard_id
            LIMIT ?
            """,
            (
                lab_jobs.ShardStatus.RUNNING.value,
                JobStatus.RUNNING.value,
                lease.fencing_token,
                (NOW + timedelta(seconds=40)).isoformat(timespec="microseconds"),
                lab_jobs.STALE_RECOVERY_BATCH_SIZE,
            ),
        ).fetchall()
    details = "\n".join(str(row[3]) for row in plan).upper()
    assert "IX_LAB_SHARD_STALE_RECOVERY" in details
    assert "JSON" not in details
    assert "TEMP B-TREE" not in details

    expired_at = NOW + timedelta(seconds=40)
    assert store.recover_stale_shards(lease, now=expired_at) == (v1_job.job_id,)
    assert (
        sum(
            shard.status is lab_jobs.ShardStatus.QUEUED
            for shard in LabJobReader(store.path).list_shards(v1_job.job_id)
        )
        == lab_jobs.STALE_RECOVERY_BATCH_SIZE
    )
    assert store.recover_stale_shards(lease, now=expired_at) == (v1_job.job_id,)
    assert all(
        shard.status is lab_jobs.ShardStatus.QUEUED
        for shard in LabJobReader(store.path).list_shards(v1_job.job_id)
    )
    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM lab_shard WHERE job_id = ? AND attempt_count = 1",
                (str(v2_job.job_id),),
            ).fetchone()[0]
            == v2_count
        )


def test_exhausted_and_idle_recovery_queries_are_indexed_and_bounded(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    recovery_count = lab_jobs.STALE_RECOVERY_BATCH_SIZE + 1
    exhausted_jobs: list[LabJobRecord] = []
    idle_jobs: list[LabJobRecord] = []

    for _ in range(recovery_count):
        idle = _submit_job(store, lease)
        store.plan_job(
            idle.job_id,
            _v1_definitions(1),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )
        idle_jobs.append(idle)

    for index in range(recovery_count):
        assert isinstance(
            store.claim_next_shard(
                worker_id=f"idle-control-worker-{index}",
                shard_lease_seconds=90,
                lease=lease,
                now=NOW + timedelta(seconds=2),
            ),
            lab_jobs.LabShardClaim,
        )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            UPDATE lab_shard
            SET status = 'queued', worker_id = NULL,
                scheduler_fencing_token = NULL, claim_token = NULL,
                claimed_at = NULL, heartbeat_at = NULL, lease_expires_at = NULL
            WHERE job_id IN ({})
            """.format(", ".join("?" for _ in idle_jobs)),
            tuple(str(job.job_id) for job in idle_jobs),
        )
    with store._transaction() as connection:
        for idle in idle_jobs:
            row = store._load_job_row(connection, str(idle.job_id))
            assert row is not None and JobStatus(str(row["status"])) is JobStatus.RUNNING
            store._set_control_intent_in_transaction(
                connection,
                row,
                control_intent=ControlIntent.PAUSE_REQUESTED,
                lease=lease,
                reason="idle-control backlog setup",
                now=NOW + timedelta(seconds=3),
                request_id=uuid4(),
            )

    for _ in range(recovery_count):
        exhausted = _submit_job(store, lease, max_attempts=1)
        store.plan_job(
            exhausted.job_id,
            _v1_definitions(1),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )
        exhausted_jobs.append(exhausted)

    v2_job = _submit_job(store, lease)
    store.plan_job(
        v2_job.job_id,
        _v2_definitions(lab_jobs.STALE_RECOVERY_BATCH_SIZE * 2),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_shard SET attempt_count = max_attempts WHERE job_id IN ({})".format(
                ", ".join("?" for _ in exhausted_jobs)
            ),
            tuple(str(job.job_id) for job in exhausted_jobs),
        )

    with store._read_transaction() as connection:
        exhausted_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT s.job_id, s.shard_id
            FROM lab_shard AS s INDEXED BY ix_lab_shard_exhausted_queued_v1_recovery
            CROSS JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE j.status IN (?, ?, ?) AND j.control_intent <> ?
              AND s.status = 'queued'
              AND s.attempt_count >= s.max_attempts
              AND s.payload_protocol_version = 1
            ORDER BY s.job_id, s.shard_index, s.shard_id
            LIMIT ?
            """,
            (
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                JobStatus.CHECKPOINTED.value,
                ControlIntent.CANCEL_REQUESTED.value,
                lab_jobs.STALE_RECOVERY_BATCH_SIZE,
            ),
        ).fetchall()
        checkpointed_exhausted_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT s.job_id, s.shard_id
            FROM lab_shard AS s
            INDEXED BY ix_lab_shard_exhausted_checkpointed_v1_recovery
            CROSS JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE j.status IN (?, ?, ?) AND j.control_intent <> ?
              AND s.status = 'checkpointed'
              AND s.attempt_count >= s.max_attempts
              AND s.payload_protocol_version = 1
            ORDER BY s.job_id, s.shard_index, s.shard_id
            LIMIT ?
            """,
            (
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                JobStatus.CHECKPOINTED.value,
                ControlIntent.CANCEL_REQUESTED.value,
                lab_jobs.STALE_RECOVERY_BATCH_SIZE,
            ),
        ).fetchall()
        idle_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT j.job_id, j.control_intent, j.created_at FROM lab_job AS j
            INDEXED BY ix_lab_job_idle_control_recovery
            WHERE j.status = 'running'
              AND j.control_intent IN ('pause_requested', 'cancel_requested')
              AND EXISTS (
                  SELECT 1 FROM lab_shard AS planned
                  INDEXED BY ix_lab_shard_idle_control_eligibility
                  WHERE planned.job_id = j.job_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM lab_shard AS active
                  INDEXED BY ix_lab_shard_idle_control_eligibility
                  WHERE active.job_id = j.job_id AND active.status = 'running'
              )
              AND (j.created_at > ? OR (j.created_at = ? AND j.job_id > ?))
            ORDER BY j.created_at, j.job_id
            LIMIT ?
            """,
            (
                lab_jobs._dump_time(NOW),
                lab_jobs._dump_time(NOW),
                str(uuid4()),
                lab_jobs.IDLE_CONTROL_AFTER_BATCH_SIZE,
            ),
        ).fetchall()
        idle_before_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT j.job_id, j.control_intent, j.created_at FROM lab_job AS j
            INDEXED BY ix_lab_job_idle_control_recovery
            WHERE j.status = 'running'
              AND j.control_intent IN ('pause_requested', 'cancel_requested')
              AND EXISTS (
                  SELECT 1 FROM lab_shard AS planned
                  INDEXED BY ix_lab_shard_idle_control_eligibility
                  WHERE planned.job_id = j.job_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM lab_shard AS active
                  INDEXED BY ix_lab_shard_idle_control_eligibility
                  WHERE active.job_id = j.job_id AND active.status = 'running'
              )
              AND (j.created_at < ? OR (j.created_at = ? AND j.job_id <= ?))
            ORDER BY j.created_at, j.job_id
            LIMIT ?
            """,
            (
                lab_jobs._dump_time(NOW),
                lab_jobs._dump_time(NOW),
                str(uuid4()),
                lab_jobs.IDLE_CONTROL_BEFORE_BATCH_SIZE,
            ),
        ).fetchall()
    exhausted_details = "\n".join(str(row[3]) for row in exhausted_plan).upper()
    checkpointed_exhausted_details = "\n".join(
        str(row[3]) for row in checkpointed_exhausted_plan
    ).upper()
    idle_details = "\n".join(str(row[3]) for row in idle_plan).upper()
    idle_before_details = "\n".join(str(row[3]) for row in idle_before_plan).upper()
    assert "IX_LAB_SHARD_EXHAUSTED_QUEUED_V1_RECOVERY" in exhausted_details
    assert "TEMP B-TREE" not in exhausted_details
    assert "IX_LAB_SHARD_EXHAUSTED_CHECKPOINTED_V1_RECOVERY" in checkpointed_exhausted_details
    assert "TEMP B-TREE" not in checkpointed_exhausted_details
    assert "IX_LAB_JOB_IDLE_CONTROL_RECOVERY" in idle_details
    assert "IX_LAB_SHARD_IDLE_CONTROL_ELIGIBILITY" in idle_details
    assert "TEMP B-TREE" not in idle_details
    assert "IX_LAB_JOB_IDLE_CONTROL_RECOVERY" in idle_before_details
    assert "IX_LAB_SHARD_IDLE_CONTROL_ELIGIBILITY" in idle_before_details
    assert "TEMP B-TREE" not in idle_before_details

    first = store.recover_stale_shards(lease, now=NOW + timedelta(seconds=5))
    assert len(first) <= lab_jobs.STALE_RECOVERY_BATCH_SIZE * 2
    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM lab_job WHERE status = 'failed'").fetchone()[0]
            == lab_jobs.STALE_RECOVERY_BATCH_SIZE
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM lab_job WHERE status = 'checkpointed'"
            ).fetchone()[0]
            == lab_jobs.IDLE_CONTROL_AFTER_BATCH_SIZE
        )

    second = store.recover_stale_shards(lease, now=NOW + timedelta(seconds=6))
    assert len(second) == lab_jobs.IDLE_CONTROL_AFTER_BATCH_SIZE + 1
    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM lab_job WHERE status = 'failed'").fetchone()[0]
            == recovery_count
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM lab_job WHERE status = 'checkpointed'"
            ).fetchone()[0]
            == lab_jobs.IDLE_CONTROL_AFTER_BATCH_SIZE * 2
        )
    third = store.recover_stale_shards(lease, now=NOW + timedelta(seconds=7))
    assert len(third) == 1
    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM lab_job WHERE status = 'checkpointed'"
            ).fetchone()[0]
            == recovery_count
        )
    assert store.recover_stale_shards(lease, now=NOW + timedelta(seconds=8)) == ()
    assert all(
        shard.attempt_count == 0 for shard in LabJobReader(store.path).list_shards(v2_job.job_id)
    )


def test_idle_control_recovery_prioritizes_older_eligible_requests_over_newer_backlog(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=600)

    def prepare_running_job(
        *,
        created_at: datetime,
        control_intent: ControlIntent = ControlIntent.NONE,
    ) -> LabJobRecord:
        job = _submit_job(store, lease)
        store.plan_job(
            job.job_id,
            _v1_definitions(1),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )
        with store._transaction() as connection:
            row = store._load_job_row(connection, str(job.job_id))
            assert row is not None
            running = store._transition_in_transaction(
                connection,
                row,
                target_status=JobStatus.RUNNING,
                lease=lease,
                reason="idle-control fairness setup",
                now=NOW + timedelta(seconds=2),
                request_id=None,
                recoverable=None,
                event_type="job_transitioned",
            )
            if control_intent is not ControlIntent.NONE:
                store._set_control_intent_in_transaction(
                    connection,
                    running,
                    control_intent=control_intent,
                    lease=lease,
                    reason="idle-control fairness setup",
                    now=NOW + timedelta(seconds=3),
                    request_id=uuid4(),
                )
        with store._transaction() as connection:
            connection.execute(
                "UPDATE lab_job SET created_at = ? WHERE job_id = ?",
                (lab_jobs._dump_time(created_at), str(job.job_id)),
            )
        return job

    older_jobs = [
        prepare_running_job(
            created_at=NOW - timedelta(seconds=40 - index),
        )
        for index in range(lab_jobs.STALE_RECOVERY_BATCH_SIZE // 2 + 1)
    ]
    cursor_marker = prepare_running_job(created_at=NOW)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            INSERT INTO lab_recovery_cursor (
                cursor_key, cursor_created_at, cursor_job_id, updated_at
            ) VALUES ('idle_control', ?, ?, ?)
            """,
            (
                lab_jobs._dump_time(NOW),
                str(cursor_marker.job_id),
                lab_jobs._dump_time(NOW),
            ),
        )
    with store._transaction() as connection:
        for index, older_job in enumerate(older_jobs):
            row = store._load_job_row(connection, str(older_job.job_id))
            assert row is not None
            store._set_control_intent_in_transaction(
                connection,
                row,
                control_intent=(
                    ControlIntent.PAUSE_REQUESTED
                    if index % 2 == 0
                    else ControlIntent.CANCEL_REQUESTED
                ),
                lease=lease,
                reason="older job became idle after cursor",
                now=NOW + timedelta(seconds=4),
                request_id=uuid4(),
            )

    store = LabJobStore(store.path)
    store.initialize()

    ordinary_jobs = [
        prepare_running_job(created_at=NOW + timedelta(seconds=100 + index))
        for index in range(lab_jobs.STALE_RECOVERY_BATCH_SIZE)
    ]
    for index in range(lab_jobs.STALE_RECOVERY_BATCH_SIZE):
        prepare_running_job(
            created_at=NOW + timedelta(seconds=200 + index),
            control_intent=ControlIntent.PAUSE_REQUESTED,
        )

    first = store.recover_stale_shards(lease, now=NOW + timedelta(seconds=300))
    assert len(first) <= lab_jobs.STALE_RECOVERY_BATCH_SIZE
    first_old_statuses = {
        job.job_id: LabJobReader(store.path).get_job(job.job_id).status for job in older_jobs
    }
    assert (
        list(first_old_statuses.values()).count(JobStatus.CHECKPOINTED)
        + list(first_old_statuses.values()).count(JobStatus.CANCELLED)
        == lab_jobs.STALE_RECOVERY_BATCH_SIZE // 2
    )
    assert list(first_old_statuses.values()).count(JobStatus.RUNNING) == 1
    assert all(
        LabJobReader(store.path).get_job(job.job_id).status is JobStatus.RUNNING
        for job in ordinary_jobs
    )

    for index in range(lab_jobs.STALE_RECOVERY_BATCH_SIZE):
        prepare_running_job(
            created_at=NOW + timedelta(seconds=300 + index),
            control_intent=ControlIntent.CANCEL_REQUESTED,
        )

    second = store.recover_stale_shards(lease, now=NOW + timedelta(seconds=400))
    assert len(second) <= lab_jobs.STALE_RECOVERY_BATCH_SIZE
    assert all(
        LabJobReader(store.path).get_job(job.job_id).status
        in {JobStatus.CHECKPOINTED, JobStatus.CANCELLED}
        for job in older_jobs
    )
    with sqlite3.connect(store.path) as connection:
        cursor = connection.execute(
            "SELECT cursor_created_at, cursor_job_id FROM lab_recovery_cursor "
            "WHERE cursor_key = 'idle_control'"
        ).fetchone()
    assert cursor is not None
    assert store.recover_stale_shards(lease, now=NOW + timedelta(seconds=401)) != ()


def test_exhausted_v1_sibling_does_not_terminalize_mixed_v2_held_job(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease, max_attempts=1)
    v2_definition = _v2_definitions(2)[1]
    v1_template = _v1_definitions(1)[0]
    v1_definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id=v1_template.adapter_id,
        adapter_version=v1_template.adapter_version,
        plan_hash=v2_definition.plan_hash,
        payload_json=v1_template.payload_json,
        work_plan=v1_template.work_plan,
    )
    store.plan_job(
        job.job_id,
        (v1_definition, v2_definition),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    v1_claim = store.claim_next_shard(
        worker_id="legacy-worker",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    assert isinstance(v1_claim, lab_jobs.LabShardClaim)
    source_stage_store = _source_stage_store(tmp_path)
    v2_claim = store.claim_next_shard(
        worker_id="held-v2-worker",
        shard_lease_seconds=30,
        source_stage_store=source_stage_store,
        source_wait_deadline=NOW + timedelta(seconds=20),
        publication_deadline=NOW + timedelta(seconds=25),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    assert isinstance(v2_claim, LabShardClaimV2)
    expired_at = NOW + timedelta(seconds=33)

    with store._transaction() as connection:
        assert store._jobs_requiring_v2_reconciliation(connection, (job.job_id,)) == {job.job_id}

    assert store.recover_stale_shards(lease, now=expired_at) == ()
    assert store.recover_stale_shards(lease, now=expired_at) == ()

    persisted_job = LabJobReader(store.path).get_job(job.job_id)
    shards = LabJobReader(store.path).list_shards(job.job_id)
    publication = store.get_claim_publication(v2_claim.claim_token)
    assert persisted_job is not None and persisted_job.status is JobStatus.RUNNING
    assert tuple(shard.status.value for shard in shards) == ("running", "running")
    assert tuple(shard.attempt_count for shard in shards) == (1, 1)
    assert publication is not None and publication.status is ClaimPublicationStatus.HELD_SOURCE
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_claim_publication").fetchone()[0] == 1
    with sqlite3.connect(source_stage_store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_source_stage").fetchone()[0] == 0


def test_exhausted_v1_only_job_keeps_generic_failure_tree_recovery(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store, seconds=120)
    job = _submit_job(store, lease, max_attempts=1)
    store.plan_job(
        job.job_id,
        _v1_definitions(1),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="legacy-worker",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    assert isinstance(claim, lab_jobs.LabShardClaim)

    assert store.recover_stale_shards(lease, now=NOW + timedelta(seconds=33)) == (job.job_id,)
    persisted_job = LabJobReader(store.path).get_job(job.job_id)
    shard = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert persisted_job is not None and persisted_job.status is JobStatus.FAILED
    assert shard.status.value == "failed"
    assert shard.failure_json == '{"reason":"attempts_exhausted"}'


@pytest.mark.parametrize(
    "status",
    [
        ClaimPublicationStatus.HELD_SOURCE,
        ClaimPublicationStatus.SOURCE_QUEUED,
        ClaimPublicationStatus.READY_TO_PUBLISH,
    ],
)
def test_expired_nonterminal_v2_publication_never_reclaims_a_second_attempt(
    tmp_path: Path,
    status: ClaimPublicationStatus,
) -> None:
    store, lease, claim, source_store = _publication_at_status(tmp_path, status)
    recovered = LabJobStore(store.path, busy_timeout_ms=5_000)
    recovered.initialize()
    expired_at = claim.lease_expires_at + timedelta(seconds=1)

    assert recovered.recover_stale_shards(lease, now=expired_at) == ()
    assert (
        recovered.claim_next_shard(
            worker_id="recovery-worker",
            shard_lease_seconds=120,
            source_stage_store=source_store,
            source_wait_deadline=expired_at + timedelta(seconds=30),
            publication_deadline=expired_at + timedelta(seconds=60),
            lease=lease,
            now=expired_at,
        )
        is None
    )
    shard = LabJobReader(store.path).list_shards(claim.job_id)[0]
    publication = recovered.get_claim_publication(claim.claim_token)
    assert shard.attempt_count == 1
    assert shard.claim_generation == 1
    assert publication is not None and publication.status is status
    with sqlite3.connect(source_store.path) as connection:
        operation_count = connection.execute("SELECT COUNT(*) FROM lab_source_stage").fetchone()[0]
    assert operation_count == (0 if status is ClaimPublicationStatus.HELD_SOURCE else 1)


def test_expired_aborted_v2_publication_stays_fenced_for_explicit_reconciliation(
    tmp_path: Path,
) -> None:
    store, lease, claim, source_store = _publication_at_status(
        tmp_path,
        ClaimPublicationStatus.ABORTED,
    )
    expired_at = claim.lease_expires_at + timedelta(seconds=1)
    reopened = LabJobStore(store.path, busy_timeout_ms=5_000)
    reopened.initialize()

    assert reopened.recover_stale_shards(lease, now=expired_at) == ()
    assert reopened.recover_stale_shards(lease, now=expired_at) == ()
    assert (
        reopened.claim_next_shard(
            worker_id="recovery-worker",
            shard_lease_seconds=120,
            source_stage_store=source_store,
            source_wait_deadline=expired_at + timedelta(seconds=30),
            publication_deadline=expired_at + timedelta(seconds=60),
            lease=lease,
            now=expired_at,
        )
        is None
    )
    shard = LabJobReader(store.path).list_shards(claim.job_id)[0]
    prior = reopened.get_claim_publication(claim.claim_token)
    assert shard.claim_token == claim.claim_token
    assert shard.claim_generation == claim.claim_generation
    assert shard.attempt_count == 1
    assert prior is not None and prior.status is ClaimPublicationStatus.ABORTED


@pytest.mark.parametrize(
    ("advance_to_pending", "expected_stage_state"),
    [(False, LabSourceStageState.QUEUED), (True, LabSourceStageState.PENDING)],
)
def test_aborted_queued_v2_source_operation_never_generates_a_second_attempt(
    tmp_path: Path,
    advance_to_pending: bool,
    expected_stage_state: LabSourceStageState,
) -> None:
    from tests.unit import test_lab_claim_publication as publication_test

    store, lease, _claim, preimage, held, _authorities = publication_test._claimed_attempt(tmp_path)
    source_store = publication_test._source_stage_store(tmp_path)
    queue = publication_test._queue_binding(preimage)
    _queued, writer = publication_test._queue(store, lease, held, queue, source_store)
    binding = publication_test._stage_binding(preimage)
    if advance_to_pending:
        source_store.begin_external(
            binding,
            publication_test._intent(preimage),
            lease=writer,
            now=NOW + timedelta(seconds=3),
        )
    aborted = store.abort_claim_publication(
        held.identity,
        terminal_reason="source_operation_cancelled",
        lease=lease,
        now=NOW + timedelta(seconds=4),
    ).record
    expired_at = preimage.lease_expires_at + timedelta(seconds=1)
    reopened = LabJobStore(store.path, busy_timeout_ms=5_000)
    reopened.initialize()

    assert reopened.recover_stale_shards(lease, now=expired_at) == ()
    assert reopened.recover_stale_shards(lease, now=expired_at) == ()
    assert (
        reopened.claim_next_shard(
            worker_id="recovery-worker",
            shard_lease_seconds=120,
            source_stage_store=source_store,
            source_wait_deadline=expired_at + timedelta(seconds=30),
            publication_deadline=expired_at + timedelta(seconds=60),
            lease=lease,
            now=expired_at,
        )
        is None
    )

    shard = LabJobReader(store.path).list_shards(preimage.job_id)[0]
    stage_record = source_store.get(binding)
    persisted = reopened.get_claim_publication(preimage.claim_token)
    assert aborted.status is ClaimPublicationStatus.ABORTED
    assert persisted is not None and persisted.terminal_reason == "source_operation_cancelled"
    assert stage_record is not None and stage_record.state is expected_stage_state
    assert (shard.claim_token, shard.claim_generation, shard.attempt_count) == (
        preimage.claim_token,
        preimage.claim_generation,
        1,
    )
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_claim_publication").fetchone()[0] == 1
    with sqlite3.connect(source_store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_source_stage").fetchone()[0] == 1


def test_store_internal_mutation_fence_rolls_back_before_sqlite_commit(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    calls = 0

    def mutation_guard() -> str:
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise RuntimeError("runtime drifted before SQLite commit")
        return "1" * 40

    store.mutation_guard = mutation_guard
    mutation_guard()

    with pytest.raises(RuntimeError, match="before SQLite commit"):
        store.apply_command(envelope, lease=lease, now=NOW)

    assert LabJobReader(store.path).get_job(envelope.command.job_id) is None


def test_initialize_guards_wal_and_schema_commit_as_separate_persistent_boundaries(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab-jobs.sqlite3"
    calls = 0

    def mutation_guard() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("runtime drifted before schema commit")
        return "1" * 40

    store = LabJobStore(path, mutation_guard=mutation_guard)

    with pytest.raises(RuntimeError, match="before schema commit"):
        store.initialize()

    assert calls == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == 0
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'lab_%'"
            ).fetchone()[0]
            == 0
        )


def test_initialize_guards_persistent_wal_mutation_after_schema_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    real_connect = sqlite3.connect

    class TrackingConnection(lab_jobs._LabJobStoreConnection):
        def execute(self, sql: str, parameters: object = (), /):  # type: ignore[override]
            if sql.strip().upper() == "PRAGMA JOURNAL_MODE = WAL":
                events.append("wal")
            return super().execute(sql, parameters)

        def commit(self) -> None:
            events.append("commit")
            super().commit()

    def tracking_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = TrackingConnection
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(lab_jobs.sqlite3, "connect", tracking_connect)

    def mutation_guard() -> str:
        events.append("guard")
        return "1" * 40

    LabJobStore(tmp_path / "lab-jobs.sqlite3", mutation_guard=mutation_guard).initialize()

    assert events == ["guard", "commit", "guard", "wal"]


def test_initialize_rejects_runtime_drift_between_schema_commit_and_wal(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab-jobs.sqlite3"
    calls = 0

    def mutation_guard() -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("runtime drifted after schema commit")
        return "1" * 40

    with pytest.raises(RuntimeError, match="after schema commit"):
        LabJobStore(path, mutation_guard=mutation_guard).initialize()

    assert calls == 2
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == lab_jobs._APPLICATION_ID
        assert connection.execute("PRAGMA user_version").fetchone()[0] == lab_jobs._SCHEMA_VERSION
        assert str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "wal"


def test_staged_commit_validation_failure_rolls_back_and_closes(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection()

    def reject_precommit(_lease: LabLeaseRecord, _now: datetime) -> None:
        raise RuntimeError("precommit failed")

    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=reject_precommit,
    )

    with pytest.raises(RuntimeError, match="precommit failed"):
        staged.commit(lease=lease, now=NOW)

    assert connection.calls == ["rollback", "close"]
    with pytest.raises(RuntimeError, match="already closed"):
        staged.commit(lease=lease, now=NOW)


def test_staged_commit_rejects_changed_lease_fence_before_validation(
    tmp_path: Path,
) -> None:
    lease = _lease(_store(tmp_path))
    replacement = lease.model_copy(
        update={"fencing_token": lease.fencing_token + 1},
    )
    connection = _StagedLifecycleConnection()
    validator_called = False

    def validate(_lease: LabLeaseRecord, _now: datetime) -> None:
        nonlocal validator_called
        validator_called = True

    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=validate,
    )

    with pytest.raises(SchedulerLeaseFencedError, match="identity changed"):
        staged.commit(lease=replacement, now=NOW)

    assert validator_called is False
    assert connection.calls == ["rollback", "close"]


def test_staged_commit_preserves_commit_rollback_and_close_errors(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection(
        commit_error=OSError("commit failed"),
        rollback_error=OSError("rollback failed"),
        close_error=OSError("close failed"),
    )
    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=lambda _lease, _now: None,
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        staged.commit(lease=lease, now=NOW)

    assert connection.calls == ["commit", "rollback", "close"]
    assert _flatten_exception_messages(raised.value) == (
        "commit failed",
        "rollback failed",
        "close failed",
    )


def test_staged_commit_reports_close_error_after_successful_commit(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection(close_error=OSError("close failed"))
    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=lambda _lease, _now: None,
    )

    with pytest.raises(OSError, match="close failed"):
        staged.commit(lease=lease, now=NOW)

    assert connection.calls == ["commit", "close"]


def test_finalization_snapshot_preserves_query_rollback_and_close_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FinalizationSnapshotFaultConnection(
        query_error=OSError("snapshot query failed"),
        rollback_error=OSError("snapshot rollback failed"),
        close_error=OSError("snapshot close failed"),
    )
    reader = LabJobReader(tmp_path / "lab.sqlite3")
    monkeypatch.setattr(reader, "_connect", lambda: connection)

    with pytest.raises(BaseExceptionGroup) as raised:
        reader.get_finalization_snapshot(uuid4())

    assert connection.calls == ["begin", "query", "rollback", "close"]
    assert _flatten_exception_messages(raised.value) == (
        "snapshot query failed",
        "snapshot rollback failed",
        "snapshot close failed",
    )


def test_finalization_snapshot_reports_close_error_after_successful_missing_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FinalizationSnapshotFaultConnection(
        close_error=OSError("snapshot close failed"),
    )
    reader = LabJobReader(tmp_path / "lab.sqlite3")
    monkeypatch.setattr(reader, "_connect", lambda: connection)

    with pytest.raises(OSError, match="snapshot close failed"):
        reader.get_finalization_snapshot(uuid4())

    assert connection.calls == ["begin", "query", "commit", "close"]


def test_artifact_commit_read_preserves_query_rollback_and_close_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _ArtifactCommitFaultConnection(
        query_error=OSError("artifact commit query failed"),
        rollback_error=OSError("artifact commit rollback failed"),
        close_error=OSError("artifact commit close failed"),
    )
    reader = LabJobReader(tmp_path / "lab.sqlite3")
    monkeypatch.setattr(reader, "_connect", lambda: connection)

    with pytest.raises(BaseExceptionGroup) as raised:
        reader.get_artifact_commit(uuid4())

    assert connection.calls == ["begin", "query", "rollback", "close"]
    assert _flatten_exception_messages(raised.value) == (
        "artifact commit query failed",
        "artifact commit rollback failed",
        "artifact commit close failed",
    )


def test_artifact_commit_read_explicitly_closes_after_missing_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_ArtifactCommitFaultConnection] = []
    reader = LabJobReader(tmp_path / "lab.sqlite3")

    def connect() -> _ArtifactCommitFaultConnection:
        connection = _ArtifactCommitFaultConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(reader, "_connect", connect)

    for _ in range(20):
        assert reader.get_artifact_commit(uuid4()) is None

    assert len(connections) == 20
    assert all(item.calls == ["begin", "query", "commit", "close"] for item in connections)


def test_staged_rollback_preserves_rollback_and_close_errors(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection(
        rollback_error=OSError("rollback failed"),
        close_error=OSError("close failed"),
    )
    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=lambda _lease, _now: None,
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        staged.rollback()

    assert connection.calls == ["rollback", "close"]
    assert _flatten_exception_messages(raised.value) == (
        "rollback failed",
        "close failed",
    )


def test_staged_context_rolls_back_when_commit_is_forgotten(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection()
    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=lambda _lease, _now: None,
    )

    with staged as entered:
        assert entered is staged

    assert connection.calls == ["rollback", "close"]
    staged.rollback()
    staged.close()
    assert connection.calls == ["rollback", "close"]
    with pytest.raises(RuntimeError, match="already closed"):
        staged.commit(lease=lease, now=NOW)


def test_staged_context_rolls_back_on_caller_exception(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection()
    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=lambda _lease, _now: None,
    )

    with pytest.raises(RuntimeError, match="caller failed"), staged:
        raise RuntimeError("caller failed")

    assert connection.calls == ["rollback", "close"]


def test_staged_context_commit_and_close_are_idempotently_closed(tmp_path: Path) -> None:
    lease = _lease(_store(tmp_path))
    connection = _StagedLifecycleConnection()
    staged = lab_jobs._LabStagedArtifactCommit(
        cast(sqlite3.Connection, connection),
        _staged_receipt(),
        lease=lease,
        precommit_validator=lambda _lease, _now: None,
    )

    with staged:
        receipt = staged.commit(lease=lease, now=NOW)

    assert receipt == staged.receipt
    assert connection.calls == ["commit", "close"]
    staged.rollback()
    staged.close()
    assert connection.calls == ["commit", "close"]
    with pytest.raises(RuntimeError, match="already closed"):
        staged.commit(lease=lease, now=NOW)


def test_connection_authority_is_exact_and_cleared_after_exception(tmp_path: Path) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    other_job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        authority = connection.write_authorization
        with (
            pytest.raises(RuntimeError, match="simulated write failure"),
            authority.authorize_submit(job_id, spec_json),
        ):
            assert authority.submit_authorized(str(job_id), spec_json) == 1
            assert authority.submit_authorized(str(other_job_id), spec_json) == 0
            raise RuntimeError("simulated write failure")

        assert authority.submit_authorized(str(job_id), spec_json) == 0
        connection.rollback()


def test_store_connection_context_closes_without_identity_authority(tmp_path: Path) -> None:
    store = _store(tmp_path)
    connection = store._connect()

    with connection:
        assert connection.execute("SELECT 1").fetchone()[0] == 1

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")


@pytest.mark.parametrize("boundary", ["commit", "rollback"])
def test_connection_authority_expires_on_explicit_transaction_boundary(
    tmp_path: Path,
    boundary: str,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        authority = connection.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            assert authority.submit_authorized(str(job_id), spec_json) == 1
            getattr(connection, boundary)()
            assert authority.submit_authorized(str(job_id), spec_json) == 0
            connection.execute("BEGIN IMMEDIATE")
            assert authority.submit_authorized(str(job_id), spec_json) == 0
            connection.rollback()


@pytest.mark.parametrize("statement", ["COMMIT", "ROLLBACK"])
def test_connection_authority_expires_on_sql_transaction_boundary(
    tmp_path: Path,
    statement: str,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        authority = connection.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            assert authority.submit_authorized(str(job_id), spec_json) == 1
            connection.execute(statement)
            assert authority.submit_authorized(str(job_id), spec_json) == 0
            connection.execute("BEGIN IMMEDIATE")
            assert authority.submit_authorized(str(job_id), spec_json) == 0
            connection.rollback()


def test_connection_authority_expires_on_executescript_implicit_commit(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        authority = connection.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            assert authority.submit_authorized(str(job_id), spec_json) == 1
            connection.executescript("SELECT 1;")
            assert authority.submit_authorized(str(job_id), spec_json) == 0


def test_connection_authority_cannot_revive_after_implicit_conflict_rollback(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("CREATE TEMP TABLE auth_probe (value INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO auth_probe VALUES (1)")
        connection.execute("BEGIN IMMEDIATE")
        authority = connection.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute("INSERT OR ROLLBACK INTO auth_probe VALUES (1)")
            assert connection.in_transaction is False
            assert authority.submit_authorized(str(job_id), spec_json) == 0
            connection.execute("BEGIN IMMEDIATE")
            assert authority.submit_authorized(str(job_id), spec_json) == 0
            connection.rollback()


@pytest.mark.parametrize(
    "executor",
    ["connection-execute", "connection-executemany", "cursor-execute", "cursor-executemany"],
)
def test_connection_authority_expires_after_statement_abort(
    tmp_path: Path,
    executor: str,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("CREATE TEMP TABLE auth_probe (value INTEGER PRIMARY KEY)")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO auth_probe VALUES (1)")
        authority = connection.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            with pytest.raises(sqlite3.IntegrityError):
                if executor == "connection-execute":
                    connection.execute("INSERT OR ABORT INTO auth_probe VALUES (1)")
                elif executor == "connection-executemany":
                    connection.executemany(
                        "INSERT OR ABORT INTO auth_probe VALUES (?)",
                        [(1,)],
                    )
                elif executor == "cursor-execute":
                    connection.cursor().execute("INSERT OR ABORT INTO auth_probe VALUES (1)")
                else:
                    connection.cursor().executemany(
                        "INSERT OR ABORT INTO auth_probe VALUES (?)",
                        [(1,)],
                    )
            assert connection.in_transaction is True
            assert authority.submit_authorized(str(job_id), spec_json) == 0
        connection.rollback()


def test_connection_authority_cannot_bypass_statement_abort_with_bare_cursor_factory(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'

    with store._connect() as connection:
        connection.execute("CREATE TEMP TABLE auth_probe (value INTEGER PRIMARY KEY)")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO auth_probe VALUES (1)")
        authority = connection.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            with pytest.raises(TypeError, match="cursor factory"):
                connection.cursor(sqlite3.Cursor)
            assert authority.submit_authorized(str(job_id), spec_json) == 0
        connection.rollback()


def test_connection_authority_requires_active_transaction(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with (
        store._connect() as connection,
        pytest.raises(RuntimeError, match="transaction"),
        connection.write_authorization.authorize_submit(
            uuid4(),
            '{"schema_version":2}',
        ),
    ):
        pass


def test_connection_authority_isolated_by_connection_and_rejects_nesting(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    job_id = uuid4()
    spec_json = '{"schema_version":2}'
    with store._connect() as first, store._connect() as second:
        first.execute("BEGIN IMMEDIATE")
        authority = first.write_authorization
        with authority.authorize_submit(job_id, spec_json):
            assert authority.submit_authorized(str(job_id), spec_json) == 1
            assert second.write_authorization.submit_authorized(str(job_id), spec_json) == 0
            with (
                pytest.raises(RuntimeError, match="already active"),
                authority.authorize_submit(job_id, spec_json),
            ):
                pass
        first.rollback()


def _submit_job(
    store: LabJobStore,
    lease: LabLeaseRecord,
    *,
    max_attempts: int = 3,
) -> LabJobRecord:
    envelope = _submit(max_attempts=max_attempts)
    receipt = store.apply_command(envelope, lease=lease, now=NOW)
    assert receipt.status == "applied"
    job = LabJobReader(store.path).get_job(envelope.command.job_id)
    assert job is not None
    return job


def _transition_to(
    store: LabJobStore,
    lease: LabLeaseRecord,
    target: JobStatus,
) -> LabJobRecord:
    job = _submit_job(store, lease)
    if target is JobStatus.QUEUED:
        return job
    job = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="worker started",
        now=NOW + timedelta(seconds=1),
    )
    if target is JobStatus.RUNNING:
        return job
    if target is JobStatus.CHECKPOINTED:
        return store.transition_job(
            job.job_id,
            expected_version=job.version,
            target_status=target,
            lease=lease,
            reason="checkpoint",
            now=NOW + timedelta(seconds=2),
        )
    if target is JobStatus.CANCELLED:
        cancel = LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=job.job_id,
                expected_version=job.version,
                reason="cancel",
            ),
        )
        store.apply_command(cancel, lease=lease, now=NOW + timedelta(seconds=2))
        requested = LabJobReader(store.path).get_job(job.job_id)
        assert requested is not None
        return store.confirm_cancelled_job(
            job.job_id,
            expected_version=requested.version,
            lease=lease,
            reason="claim invalidated",
            now=NOW + timedelta(seconds=3),
        )
    return store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=target,
        lease=lease,
        reason="terminal",
        recoverable=target is JobStatus.FAILED,
        now=NOW + timedelta(seconds=2),
    )


def _count(path: Path, table: str) -> int:
    with sqlite3.connect(path) as connection:
        row = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])


def _create_609c599_v1_fixture(
    path: Path,
) -> tuple[tuple[LabCommandEnvelope, LabCommandReceipt], ...]:
    applied = _submit()
    applied_receipt = LabCommandReceipt(
        request_id=applied.request_id,
        content_hash=applied.content_hash,
        job_id=applied.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    rejected = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=uuid4(),
            expected_version=0,
            reason="missing job",
        ),
    )
    rejected_receipt = LabCommandReceipt(
        request_id=rejected.request_id,
        content_hash=rejected.content_hash,
        job_id=rejected.command.job_id,
        status="rejected",
        reason="job_not_found",
        job_version=None,
    )
    rows = ((applied, applied_receipt), (rejected, rejected_receipt))
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE lab_command (
                request_id TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                command_type TEXT NOT NULL,
                job_id TEXT NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('applied', 'rejected')),
                reason TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                received_at TEXT NOT NULL,
                applied_at TEXT NOT NULL
            )
            """
        )
        for offset, (envelope, receipt) in enumerate(rows):
            timestamp = (NOW + timedelta(seconds=offset)).isoformat(timespec="microseconds")
            connection.execute(
                """
                INSERT INTO lab_command (
                    request_id, content_hash, command_type, job_id, command_json,
                    status, reason, receipt_json, received_at, applied_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    envelope.command.command_type,
                    str(envelope.command.job_id),
                    envelope.model_dump_json(),
                    receipt.status,
                    receipt.reason,
                    receipt.model_dump_json(),
                    timestamp,
                    timestamp,
                ),
            )
        connection.execute(f"PRAGMA application_id = {LabJobStore.APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 1")
    return rows


def test_initialize_creates_v16_strict_schema_and_required_pragmas(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        application_id = connection.execute("PRAGMA application_id").fetchone()[0]
        user_version = connection.execute("PRAGMA user_version").fetchone()[0]
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
        table_sql = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            )
        }
        schema_sql = " ".join(table_sql.values()).upper()

    assert {
        "lab_command",
        "lab_job",
        "lab_shard",
        "lab_event",
        "lab_lease",
        "lab_artifact",
        "lab_artifact_commit",
        "lab_job_result_artifact",
        "lab_ledger_epoch",
        "lab_claim_publication",
        "lab_claim_publication_audit",
        "lab_recovery_cursor",
        "lab_preclaim_fair_cursor",
        "lab_claim_publication_finalizer_lease",
        "lab_claim_publication_finalizer_root_anchor",
        "lab_claim_publication_finalizer_observation",
        "lab_claim_publication_finalizer_attestation",
        "lab_claim_publication_finalizer_trust_cache",
        "lab_claim_publication_finalizer_observation_degradation",
    } <= tables
    assert application_id == LabJobStore.APPLICATION_ID
    assert user_version == LabJobStore.SCHEMA_VERSION
    assert str(journal_mode).lower() == "wal"
    assert synchronous == 2
    strict_tables = {
        "lab_claim_publication_finalizer_root_anchor",
        "lab_claim_publication_finalizer_attestation",
        "lab_claim_publication_finalizer_trust_cache",
        "lab_claim_publication_finalizer_observation_degradation",
    }
    assert all(table_sql[table].upper().rstrip().endswith("STRICT") for table in strict_tables)
    assert "TYPEOF(RECEIPT_JOB_VERSION) = 'INTEGER'" in " ".join(schema_sql.split())
    normalized_schema = " ".join(schema_sql.split())
    assert "CHECK (SOURCE_WAIT_DEADLINE <= PUBLICATION_DEADLINE)" in normalized_schema
    for deadline in ("SOURCE_WAIT_DEADLINE", "PUBLICATION_DEADLINE"):
        assert f"LENGTH({deadline}) = 32" in normalized_schema
        assert f"{deadline} GLOB" in normalized_schema
        assert f"JULIANDAY({deadline}) IS NOT NULL" in normalized_schema

    pragmas = store.connection_pragmas()
    assert pragmas.journal_mode == "wal"
    assert pragmas.synchronous == 2
    assert pragmas.foreign_keys == 1
    assert pragmas.busy_timeout_ms == 1_234


def test_v10_reopen_refuses_claim_publication_schema_without_canonical_deadline_checks(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'lab_claim_publication'"
        ).fetchone()
        assert row is not None and row[0] is not None
        schema_sql = " ".join(str(row[0]).split())
        source_deadline_check = (
            "source_wait_deadline TEXT NOT NULL CHECK ( "
            "typeof(source_wait_deadline) = 'text' "
            "AND length(source_wait_deadline) = 32 "
            f"AND source_wait_deadline GLOB {lab_jobs._CANONICAL_UTC_TIMESTAMP_GLOB} "
            "AND julianday(source_wait_deadline) IS NOT NULL "
            "AND substr(source_wait_deadline, 1, 10) = "
            "strftime('%Y-%m-%d', source_wait_deadline, '+0 days') "
            "AND substr(source_wait_deadline, 12, 8) = "
            "strftime('%H:%M:%S', source_wait_deadline, '+0 seconds') )"
        )
        assert source_deadline_check in schema_sql
        schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        connection.execute("PRAGMA writable_schema = ON")
        connection.execute(
            "UPDATE sqlite_schema SET sql = ? "
            "WHERE type = 'table' AND name = 'lab_claim_publication'",
            (
                schema_sql.replace(
                    source_deadline_check,
                    "source_wait_deadline TEXT NOT NULL "
                    "CHECK (typeof(source_wait_deadline) = 'text')",
                    1,
                ),
            ),
        )
        connection.execute(f"PRAGMA schema_version = {schema_version + 1}")
        connection.execute("PRAGMA writable_schema = OFF")

    with pytest.raises(LabDatabaseIdentityError, match="v5 table.*invalid constraints"):
        LabJobStore(store.path).initialize()
    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="v5 table.*invalid constraints",
    )


def test_initialize_migrates_v6_chain_and_read_summaries_idempotently(tmp_path: Path) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    with sqlite3.connect(path) as connection:
        for statement in lab_jobs._V6_SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.execute(f"PRAGMA application_id = {LabJobStore.APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 6")

    store = LabJobStore(path)
    store.initialize()
    store.initialize()

    with sqlite3.connect(path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        chain = connection.execute(
            "SELECT chain_generation, head_hash FROM lab_ledger_chain WHERE singleton = 1"
        ).fetchone()
        entries = connection.execute("SELECT COUNT(*) FROM lab_ledger_chain_entry").fetchone()[0]
        totals = connection.execute(
            "SELECT total_count FROM lab_job_list_summary WHERE singleton = 1"
        ).fetchone()[0]

    assert version == LabJobStore.SCHEMA_VERSION
    assert chain == (0, lab_jobs._ledger_chain_step(lab_jobs._LEDGER_CHAIN_GENESIS_HASH, 0, 0))
    assert entries == 1
    assert totals == 0


def test_v6_to_v7_migration_preserves_historical_job_after_crash_and_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use a real pre-v7 ledger, not a schema-version relabelled empty database."""

    store = _store(tmp_path)
    envelope = _submit()
    lease = store.acquire_scheduler_lease(owner_id="migration", lease_seconds=60, now=NOW)
    store.apply_command(envelope, lease=lease, now=NOW)
    path = store.path
    with sqlite3.connect(path) as connection:
        for table in (
            "lab_claim_publication_audit",
            "lab_claim_publication",
        ):
            connection.execute(f"DROP TABLE {table}")
        for name in (
            "trg_lab_job_list_summary_insert",
            "trg_lab_job_list_summary_delete",
            "trg_lab_job_list_summary_update",
            "trg_lab_shard_payload_protocol_insert",
            "trg_lab_shard_payload_protocol_update",
        ):
            connection.execute(f"DROP TRIGGER {name}")
        for name in (
            "ix_lab_shard_stale_recovery",
            "ix_lab_shard_v2_reconciliation",
            "ix_lab_shard_preclaim_candidate",
            "ix_lab_shard_exhausted_queued_v1_recovery",
            "ix_lab_shard_exhausted_checkpointed_v1_recovery",
            "ix_lab_job_idle_control_recovery",
            "ix_lab_shard_idle_control_eligibility",
        ):
            connection.execute(f"DROP INDEX {name}")
        connection.execute("DROP TABLE lab_recovery_cursor")
        connection.execute("ALTER TABLE lab_shard DROP COLUMN payload_protocol_version")
        for table in (
            "lab_finalization_candidate_summary",
            "lab_job_list_summary",
            "lab_ledger_chain_entry",
            "lab_ledger_chain",
        ):
            connection.execute(f"DROP TABLE {table}")
        connection.execute("PRAGMA user_version = 6")

    original = lab_jobs._migrate_v6_to_v7

    def crash_after_v7_objects(connection: sqlite3.Connection) -> None:
        original(connection)
        raise RuntimeError("simulated v6 migration crash")

    monkeypatch.setattr(lab_jobs, "_migrate_v6_to_v7", crash_after_v7_objects)
    with pytest.raises(RuntimeError, match="simulated v6 migration crash"):
        LabJobStore(path).initialize()
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        assert connection.execute(
            "SELECT job_id FROM lab_job WHERE job_id = ?", (str(envelope.command.job_id),)
        ).fetchone() == (str(envelope.command.job_id),)
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE name = 'lab_ledger_chain'"
            ).fetchone()
            is None
        )

    monkeypatch.setattr(lab_jobs, "_migrate_v6_to_v7", original)
    LabJobStore(path).initialize()
    migrated = LabJobReader(path).get_job(envelope.command.job_id)
    assert migrated is not None
    assert migrated.spec_hash == envelope.command.spec.spec_hash


def test_initialize_refuses_other_sqlite_without_overwriting_identity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "other.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA application_id = 12345")
        connection.execute("CREATE TABLE other_data (value TEXT)")

    with pytest.raises(LabDatabaseIdentityError, match="application_id"):
        LabJobStore(path).initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == 12345
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name = 'other_data'"
        ).fetchone() == ("other_data",)


def test_initialize_refuses_unclaimed_nonempty_sqlite(tmp_path: Path) -> None:
    path = tmp_path / "unclaimed.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE unrelated (value TEXT)")

    with pytest.raises(LabDatabaseIdentityError, match="not empty"):
        LabJobStore(path).initialize()


@pytest.mark.parametrize("version", [0, 2, 99])
def test_store_and_reader_fail_closed_on_unknown_schema_version(
    tmp_path: Path,
    version: int,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute(f"PRAGMA user_version = {version}")

    with pytest.raises(LabDatabaseIdentityError, match="user_version|unexpectedly"):
        store.initialize()
    with pytest.raises(LabDatabaseIdentityError, match="user_version"):
        LabJobReader(store.path).get_job(uuid4())


def test_reader_rejects_same_name_structurally_wrong_v5_trigger(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER trg_lab_result_artifact_no_delete")
        connection.execute(
            """
            CREATE TRIGGER trg_lab_result_artifact_no_delete
            BEFORE DELETE ON lab_job_result_artifact
            BEGIN
                SELECT 1;
            END
            """
        )

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="trigger.*structure",
    )


def test_sql_ddl_equivalence_preserves_quoted_literal_bytes_and_escapes() -> None:
    expected = "SELECT 'it''s ready', X'AB', \"MiXeD\" FROM jobs WHERE state = 'ready'"
    equivalent = (
        " select /* spacing */ 'it''s ready' , x'AB', \"MiXeD\" "
        "from JOBS -- line comment\n where STATE='ready' "
    )
    carriage_return_comment = equivalent.replace(
        "-- line comment\n",
        "-- old-mac line comment\r",
    )

    assert lab_jobs._sql_ddl_equivalent(expected, equivalent)
    assert lab_jobs._sql_ddl_equivalent(expected, carriage_return_comment)
    assert not lab_jobs._sql_ddl_equivalent(
        expected,
        equivalent.replace("'it''s ready'", "'it''s READY'"),
    )
    assert not lab_jobs._sql_ddl_equivalent(
        expected,
        equivalent.replace("x'AB'", "x'ab'"),
    )
    assert not lab_jobs._sql_ddl_equivalent(
        expected,
        equivalent.replace('"MiXeD"', '"MIXED"'),
    )


def test_v5_trigger_validator_accepts_keyword_case_and_spacing(tmp_path: Path) -> None:
    store = _store(tmp_path)
    trigger = "trg_lab_result_artifact_no_delete"
    with sqlite3.connect(store.path) as connection:
        connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute(
            f"""
            create   trigger if not exists {trigger}
            before delete on lab_job_result_artifact
            begin
                select raise ( abort,
                    'complete result artifact index is immutable' );
            end
            """
        )

    assert LabJobReader(store.path).get_job(uuid4()) is None


def test_v5_trigger_validator_rejects_string_literal_case_change(tmp_path: Path) -> None:
    store = _store(tmp_path)
    trigger = "trg_lab_complete_result_shard_no_update"
    changed = lab_jobs._V5_COMPLETE_RESULT_SHARD_NO_UPDATE_TRIGGER.replace(
        "'ready'",
        "'READY'",
        1,
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute(changed)

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="trigger.*structure",
    )


def test_v5_schema_rejects_unexpected_persistent_trigger(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER trg_lab_unexpected_review_probe
            AFTER INSERT ON lab_event
            BEGIN
                SELECT 1;
            END
            """
        )

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="unexpected.*trigger|trigger.*set",
    )
    _assert_current_schema_rejection(
        store.connection_pragmas,
        cause_match="unexpected.*trigger|trigger.*set",
    )


def test_v12_schema_identity_ignores_connection_local_temp_trigger(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            CREATE TEMP TRIGGER trg_lab_temp_review_probe
            AFTER INSERT ON main.lab_event
            BEGIN
                SELECT 1;
            END
            """
        )
        assert connection.execute(
            "SELECT name FROM sqlite_temp_master WHERE type = 'trigger'"
        ).fetchall() == [("trg_lab_temp_review_probe",)]
        lab_jobs._validate_v12_schema(connection)


def test_v5_schema_rejects_missing_persistent_trigger(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER trg_lab_result_artifact_no_delete")

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="missing.*trigger",
    )


@pytest.mark.parametrize(
    "trigger",
    [
        "trg_lab_complete_result_job_no_delete",
        "trg_lab_job_existing_key_no_insert",
        "trg_lab_job_id_immutable",
        "trg_lab_complete_result_ready_job_update",
        "trg_lab_complete_result_sealed_job_no_update",
    ],
)
def test_v5_schema_requires_exact_complete_result_parent_guards(
    tmp_path: Path,
    trigger: str,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?",
            (trigger,),
        ).fetchone()
        assert row is not None and row[0] is not None, f"missing required trigger {trigger}"
        connection.execute(f'DROP TRIGGER "{trigger}"')
        operation = (
            "DELETE"
            if trigger.endswith("no_delete")
            else "INSERT"
            if trigger.endswith("no_insert")
            else "UPDATE"
        )
        connection.execute(
            f"""
            CREATE TRIGGER "{trigger}"
            BEFORE {operation} ON lab_job
            BEGIN
                SELECT 1;
            END
            """
        )

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="trigger.*structure",
    )
    _assert_current_schema_rejection(
        store.connection_pragmas,
        cause_match="trigger.*structure",
    )


@pytest.mark.parametrize(
    "status",
    [
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.CHECKPOINTED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    ],
)
@pytest.mark.parametrize("uuid_style", ["uppercase", "braces", "urn", "whitespace", "other"])
def test_v5_job_id_is_immutable_in_every_application_state_with_foreign_keys_off(
    tmp_path: Path,
    status: JobStatus,
    uuid_style: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, status)
    canonical = str(job.job_id)
    replacement = {
        "uppercase": canonical.upper(),
        "braces": f"{{{canonical}}}",
        "urn": f"urn:uuid:{canonical}",
        "whitespace": f" {canonical}",
        "other": str(UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")),
    }[uuid_style]
    if replacement == canonical:
        replacement = f"{{{canonical}}}"
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        _register_unprivileged_job_functions(connection)
        with pytest.raises(sqlite3.IntegrityError, match="job_id.*immutable"):
            connection.execute(
                "UPDATE lab_job SET job_id = ? WHERE job_id = ?",
                (replacement, canonical),
            )

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted is not None and persisted.status is status
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_event WHERE job_id <> ?",
            (canonical,),
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_shard WHERE job_id <> ?",
            (canonical,),
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    "status",
    [
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.CHECKPOINTED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    ],
)
def test_v5_job_id_guard_preserves_legitimate_lifecycle_transitions(
    tmp_path: Path,
    status: JobStatus,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)

    transitioned = _transition_to(store, lease, status)

    assert transitioned.status is status
    assert LabJobReader(store.path).get_job(transitioned.job_id) == transitioned


def test_existing_job_key_insert_guard_does_not_depend_on_authorization_udf(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            """
            SELECT sql FROM sqlite_schema
            WHERE type = 'trigger' AND name = 'trg_lab_job_existing_key_no_insert'
            """
        ).fetchone()

    assert row is not None and row[0] is not None
    sql = str(row[0])
    assert "EXISTS" in sql.upper()
    assert "lab_job" in sql
    assert "authorized" not in sql.lower()


@pytest.mark.parametrize(
    ("trigger", "operation"),
    [
        ("trg_lab_complete_result_shard_no_insert", "INSERT"),
        ("trg_lab_complete_result_shard_no_update", "UPDATE"),
    ],
)
def test_v5_schema_requires_exact_shard_parent_guards(
    tmp_path: Path,
    trigger: str,
    operation: str,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute(
            f"""
            CREATE TRIGGER "{trigger}"
            BEFORE {operation} ON lab_shard
            BEGIN
                SELECT 1;
            END
            """
        )

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="trigger.*structure",
    )
    _assert_current_schema_rejection(
        store.connection_pragmas,
        cause_match="trigger.*structure",
    )


@pytest.mark.parametrize(
    ("trigger", "authorization_function"),
    [
        (
            "trg_lab_job_complete_result_insert",
            lab_jobs._SUBMIT_AUTH_FUNCTION,
        ),
        (
            "trg_lab_job_complete_result_update",
            lab_jobs._ARTIFACT_SUCCESS_AUTH_FUNCTION,
        ),
        (
            "trg_lab_artifact_commit_insert",
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
        ),
    ],
)
def test_v5_reader_rejects_trigger_with_replaced_authorization_udf(
    tmp_path: Path,
    trigger: str,
    authorization_function: str,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?",
            (trigger,),
        ).fetchone()
        assert row is not None and row[0] is not None
        original = str(row[0])
        assert authorization_function in original
        connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute(
            original.replace(
                authorization_function,
                f"{authorization_function}_weakened",
                1,
            )
        )

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="trigger.*structure",
    )
    _assert_current_schema_rejection(
        store.connection_pragmas,
        cause_match="trigger.*structure",
    )


def _replace_empty_v5_table_with_weakened_ddl(
    path: Path,
    *,
    table: str,
    old: str,
    new: str,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        assert row is not None and row[0] is not None
        original_sql = str(row[0])
        assert old in original_sql
        weakened_sql = original_sql.replace(old, new, 1)
        trigger_sql = tuple(
            str(trigger[0])
            for trigger in connection.execute(
                """
                SELECT sql FROM sqlite_schema
                WHERE type = 'trigger' AND tbl_name = ?
                ORDER BY name
                """,
                (table,),
            ).fetchall()
        )
        connection.execute(f'DROP TABLE "{table}"')
        connection.execute(weakened_sql)
        for statement in trigger_sql:
            connection.execute(statement)


@pytest.mark.parametrize(
    ("table", "old", "new"),
    [
        (
            "lab_job_result_artifact",
            "job_id TEXT PRIMARY KEY REFERENCES lab_job(job_id) ON DELETE RESTRICT",
            "job_id TEXT REFERENCES lab_job(job_id) ON DELETE RESTRICT",
        ),
        (
            "lab_job_result_artifact",
            "commit_request_id TEXT NOT NULL UNIQUE",
            "commit_request_id TEXT NOT NULL",
        ),
        (
            "lab_job_result_artifact",
            "job_id TEXT PRIMARY KEY REFERENCES lab_job(job_id) ON DELETE RESTRICT",
            "job_id TEXT PRIMARY KEY",
        ),
        (
            "lab_job_result_artifact",
            "REFERENCES lab_artifact_commit(request_id) ON DELETE RESTRICT",
            "",
        ),
        (
            "lab_job_result_artifact",
            "AND manifest_hash NOT GLOB '*[^0-9a-f]*'",
            "",
        ),
        (
            "lab_job_result_artifact",
            "AND json_valid(evidence_json)",
            "",
        ),
        (
            "lab_artifact_commit",
            "request_id TEXT PRIMARY KEY CHECK",
            "request_id TEXT CHECK",
        ),
        (
            "lab_artifact_commit",
            "AND content_hash NOT GLOB '*[^0-9a-f]*'",
            "",
        ),
        (
            "lab_artifact_commit",
            "AND json_valid(commit_json)",
            "",
        ),
    ],
    ids=[
        "result-job-primary-key",
        "result-commit-unique",
        "result-job-foreign-key",
        "result-commit-foreign-key",
        "result-hash-check",
        "result-evidence-check",
        "commit-request-primary-key",
        "commit-content-hash-check",
        "commit-envelope-check",
    ],
)
def test_v5_reader_rejects_same_columns_with_weakened_table_constraints(
    tmp_path: Path,
    table: str,
    old: str,
    new: str,
) -> None:
    store = _store(tmp_path)
    _replace_empty_v5_table_with_weakened_ddl(
        store.path,
        table=table,
        old=old,
        new=new,
    )

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="v5.*constraint|primary|unique|foreign",
    )


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            "CHECK (result_state IN ('pending','ready','sealed','legacy_unsealed'))",
            "",
        ),
        (
            "CHECK ( typeof(requires_complete_result) = 'integer' "
            "AND requires_complete_result IN (0, 1) )",
            "",
        ),
    ],
    ids=["result-state-check", "complete-result-marker-check"],
)
def test_v5_reader_rejects_weakened_job_checks_in_sqlite_schema(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'lab_job'"
        ).fetchone()
        assert row is not None and row[0] is not None
        compact = " ".join(str(row[0]).split())
        assert old in compact
        weakened = compact.replace(old, new, 1)
        schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        connection.execute("PRAGMA writable_schema = ON")
        connection.execute(
            "UPDATE sqlite_schema SET sql = ? WHERE type = 'table' AND name = 'lab_job'",
            (weakened,),
        )
        connection.execute(f"PRAGMA schema_version = {schema_version + 1}")
        connection.execute("PRAGMA writable_schema = OFF")

    _assert_current_schema_rejection(
        lambda: LabJobReader(store.path).get_job(uuid4()),
        cause_match="v5.*constraint",
    )


def test_v5_store_and_reader_reopen_exact_schema(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.initialize()

    assert LabJobReader(store.path).get_job(uuid4()) is None


def test_complete_result_contract_cannot_enter_legacy_unsealed_state(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="authorized|function|consistent"),
    ):
        connection.execute(
            """
            UPDATE lab_job
            SET result_contract_version = ?, result_state = ?
            WHERE job_id = ?
            """,
            (
                COMPLETE_RESULT_CONTRACT_VERSION,
                LabResultState.LEGACY_UNSEALED.value,
                str(job.job_id),
            ),
        )


def test_unplanned_v5_job_cannot_succeed_through_public_transition(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    queued = _submit_job(store, lease)
    running = store.transition_job(
        queued.job_id,
        expected_version=queued.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="start without plan",
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(InvalidJobTransitionError, match="artifact commit"):
        store.transition_job(
            running.job_id,
            expected_version=running.version,
            target_status=JobStatus.SUCCEEDED,
            lease=lease,
            reason="unsafe direct success",
            now=NOW + timedelta(seconds=2),
        )

    unchanged = LabJobReader(store.path).get_job(running.job_id)
    assert unchanged == running


def test_combined_contract_downgrade_and_legacy_success_is_blocked(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    queued = _submit_job(store, lease)
    running = store.transition_job(
        queued.job_id,
        expected_version=queued.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="start",
        now=NOW + timedelta(seconds=1),
    )

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="authorized|function|consistent"),
    ):
        connection.execute(
            """
            UPDATE lab_job
            SET result_contract_version = NULL,
                result_state = 'legacy_unsealed', status = 'succeeded'
            WHERE job_id = ?
            """,
            (str(running.job_id),),
        )


def test_requires_complete_result_marker_cannot_be_downgraded(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="immutable|function|authorized"),
    ):
        connection.execute(
            "UPDATE lab_job SET requires_complete_result = 0 WHERE job_id = ?",
            (str(job.job_id),),
        )


def test_v5_schema_rejects_forged_legacy_success_insert(tmp_path: Path) -> None:
    store = _store(tmp_path)
    spec = _spec()
    timestamp = NOW.isoformat(timespec="microseconds")

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="submit|function|authorized"),
    ):
        connection.execute(
            """
            INSERT INTO lab_job (
                job_id, spec_json, spec_hash, job_type, resource_class,
                deadline, status, control_intent, version, attempt_count,
                max_attempts, recoverable, scheduler_fencing_token,
                created_at, updated_at, result_contract_version,
                result_state, requires_complete_result
            ) VALUES (?, ?, ?, ?, ?, ?, 'succeeded', 'none', 0, 0, 3, 0,
                      NULL, ?, ?, NULL, 'legacy_unsealed', 0)
            """,
            (
                str(uuid4()),
                spec.model_dump_json(round_trip=True),
                spec.spec_hash,
                spec.job_type.value,
                spec.resource_class.value,
                spec.deadline.isoformat(timespec="microseconds"),
                timestamp,
                timestamp,
            ),
        )


def test_external_sql_cannot_forge_zero_shard_artifact_success(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    queued = _submit_job(store, lease)
    running = store.transition_job(
        queued.job_id,
        expected_version=queued.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="start without a plan",
        now=NOW + timedelta(seconds=1),
    )
    request_id = uuid4()
    timestamp = NOW.isoformat(timespec="microseconds")

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="authorized|function|artifact"),
    ):
        connection.execute(
            """
            INSERT INTO lab_artifact_commit (
                request_id, content_hash, job_id, commit_json, status, reason,
                receipt_json, receipt_job_version, received_at, applied_at
            ) VALUES (?, ?, ?, '{}', 'accepted', 'forged', '{}', ?, ?, ?)
            """,
            (
                str(request_id),
                "a" * 64,
                str(running.job_id),
                running.version + 1,
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO lab_job_result_artifact (
                job_id, commit_request_id, sealed_path, manifest_hash,
                complete_result_hash, bundle_device, bundle_inode,
                evidence_json, indexed_at
            ) VALUES (?, ?, '/does/not/exist', ?, ?, 0, 1, '{}', ?)
            """,
            (
                str(running.job_id),
                str(request_id),
                "b" * 64,
                "c" * 64,
                timestamp,
            ),
        )
        connection.execute(
            """
            UPDATE lab_job
            SET status = 'succeeded', result_state = 'sealed',
                result_contract_version = ?
            WHERE job_id = ?
            """,
            (COMPLETE_RESULT_CONTRACT_VERSION, str(running.job_id)),
        )


def test_external_sql_cannot_insert_running_job_without_submit_authority(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    spec = _spec()
    timestamp = NOW.isoformat(timespec="microseconds")

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="authorized|function|submit"),
    ):
        connection.execute(
            """
            INSERT INTO lab_job (
                job_id, spec_json, spec_hash, job_type, resource_class,
                deadline, status, control_intent, version, attempt_count,
                max_attempts, recoverable, scheduler_fencing_token,
                created_at, updated_at, result_contract_version,
                result_state, requires_complete_result
            ) VALUES (?, ?, ?, ?, ?, ?, 'running', 'none', 1, 1, 3, 0,
                      1, ?, ?, NULL, 'pending', 1)
            """,
            (
                str(uuid4()),
                spec.model_dump_json(round_trip=True),
                spec.spec_hash,
                spec.job_type.value,
                spec.resource_class.value,
                spec.deadline.isoformat(timespec="microseconds"),
                timestamp,
                timestamp,
            ),
        )


def test_external_sql_cannot_insert_legacy_source_even_in_submit_initial_state(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    spec = _spec()
    timestamp = NOW.isoformat(timespec="microseconds")

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="submit|function|authorized"),
    ):
        connection.execute(
            """
            INSERT INTO lab_job (
                job_id, spec_json, spec_hash, job_type, resource_class,
                deadline, status, control_intent, version, attempt_count,
                max_attempts, recoverable, scheduler_fencing_token,
                created_at, updated_at, result_contract_version,
                result_state, requires_complete_result
            ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 'none', 0, 0, 3, 0,
                      NULL, ?, ?, NULL, 'pending', 0)
            """,
            (
                str(uuid4()),
                spec.model_dump_json(round_trip=True),
                spec.spec_hash,
                spec.job_type.value,
                spec.resource_class.value,
                spec.deadline.isoformat(timespec="microseconds"),
                timestamp,
                timestamp,
            ),
        )


def test_store_test_connection_has_no_submit_authority(tmp_path: Path) -> None:
    store = _store(tmp_path)
    spec = _spec()
    timestamp = NOW.isoformat(timespec="microseconds")
    statement = f"""
        INSERT INTO lab_job (
            job_id, spec_json, spec_hash, job_type, resource_class,
            deadline, status, control_intent, version, attempt_count,
            max_attempts, recoverable, scheduler_fencing_token,
            created_at, updated_at, result_contract_version,
            result_state, requires_complete_result
        ) VALUES (
            '{uuid4()}', '{spec.model_dump_json(round_trip=True)}',
            '{spec.spec_hash}', '{spec.job_type.value}',
            '{spec.resource_class.value}',
            '{spec.deadline.isoformat(timespec="microseconds")}',
            'queued', 'none', 0, 0, 3, 0, NULL,
            '{timestamp}', '{timestamp}', NULL, 'pending', 1
        )
    """

    with pytest.raises(sqlite3.DatabaseError, match="authorized|submit"):
        store.execute_for_test(statement)


def test_external_sql_cannot_retry_failed_job_without_retry_authority(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    failed = _transition_to(store, lease, JobStatus.FAILED)

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="authorized|function|retry"),
    ):
        connection.execute(
            """
            UPDATE lab_job
            SET status = 'queued', control_intent = 'none',
                version = version + 1, recoverable = 0,
                scheduler_fencing_token = NULL, result_state = 'pending',
                updated_at = ?
            WHERE job_id = ?
            """,
            (NOW.isoformat(timespec="microseconds"), str(failed.job_id)),
        )


def test_reader_refuses_wrong_application_id(tmp_path: Path) -> None:
    path = tmp_path / "other.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA application_id = 9876")
        connection.execute("PRAGMA user_version = 1")
        connection.execute("CREATE TABLE lab_job (job_id TEXT)")

    with pytest.raises(LabDatabaseIdentityError, match="application_id"):
        LabJobReader(path).get_job(uuid4())


def test_initialize_migrates_609c599_v1_fixture_and_preserves_commands(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    fixture = _create_609c599_v1_fixture(path)
    with pytest.raises(LabDatabaseIdentityError, match="user_version"):
        LabJobReader(path).get_command(fixture[0][0].request_id)

    store = LabJobStore(path)
    store.initialize()

    with sqlite3.connect(path) as connection:
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(lab_command)").fetchall()
        }
        migrated = tuple(
            connection.execute(
                """
                SELECT request_id, content_hash, status, reason,
                       receipt_job_version, typeof(receipt_job_version)
                FROM lab_command ORDER BY applied_at
                """
            ).fetchall()
        )
        migrated_schema = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'lab_command'"
        ).fetchone()[0]
    assert user_version == LabJobStore.SCHEMA_VERSION
    assert "receipt_job_version" in columns
    assert migrated == (
        (
            str(fixture[0][0].request_id),
            fixture[0][0].content_hash,
            "applied",
            "submitted",
            0,
            "integer",
        ),
        (
            str(fixture[1][0].request_id),
            fixture[1][0].content_hash,
            "rejected",
            "job_not_found",
            None,
            "null",
        ),
    )
    assert "typeof(receipt_job_version) = 'integer'" in migrated_schema
    reader = LabJobReader(path)
    assert reader.get_command(fixture[0][0].request_id).receipt_job_version == 0
    assert reader.get_command(fixture[1][0].request_id).receipt_job_version is None

    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=60, now=NOW)
    new_command = _submit()
    new_receipt = store.apply_command(new_command, lease=lease, now=NOW)
    assert new_receipt.job_version == 0
    assert reader.get_command(new_command.request_id).receipt_job_version == 0


def test_v1_migration_fault_rolls_back_schema_rows_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    fixture = _create_609c599_v1_fixture(path)
    original = LabCommandReceipt.model_validate_json
    calls = 0

    def crash_on_second_receipt(payload: str) -> int | None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated migration crash")
        return original(payload).job_version

    monkeypatch.setattr(
        lab_jobs,
        "_receipt_job_version_from_json",
        crash_on_second_receipt,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="migration crash"):
        LabJobStore(path).initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(lab_command)").fetchall()
        }
        rows = tuple(
            connection.execute(
                "SELECT request_id, receipt_json FROM lab_command ORDER BY applied_at"
            ).fetchall()
        )
    assert "receipt_job_version" not in columns
    assert rows == tuple(
        (str(envelope.request_id), receipt.model_dump_json()) for envelope, receipt in fixture
    )


def _create_v4_job_fixture(path: Path, *, status: JobStatus) -> UUID:
    job_id = uuid4()
    spec = _spec()
    timestamp = NOW.isoformat(timespec="microseconds")
    with sqlite3.connect(path) as connection:
        for statement in lab_jobs._V4_SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.execute(
            """
            INSERT INTO lab_job (
                job_id, spec_json, spec_hash, job_type, resource_class,
                deadline, status, control_intent, version, attempt_count,
                max_attempts, recoverable, scheduler_fencing_token,
                created_at, updated_at, result_contract_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 2, 1, 3, 0, ?, ?, ?, ?)
            """,
            (
                str(job_id),
                spec.model_dump_json(round_trip=True),
                spec.spec_hash,
                spec.job_type.value,
                spec.resource_class.value,
                spec.deadline.isoformat(timespec="microseconds"),
                status.value,
                ControlIntent.NONE.value,
                1 if status is JobStatus.RUNNING else None,
                timestamp,
                timestamp,
                lab_jobs.RESULT_CONTRACT_VERSION,
            ),
        )
        connection.execute(f"PRAGMA application_id = {LabJobStore.APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 4")
    return job_id


@pytest.mark.parametrize(
    ("status", "expected_result_state"),
    [
        (JobStatus.SUCCEEDED, "legacy_unsealed"),
        (JobStatus.RUNNING, "pending"),
        (JobStatus.FAILED, "pending"),
    ],
)
def test_v4_migration_preserves_legacy_contract_without_faking_sealed_result(
    tmp_path: Path,
    status: JobStatus,
    expected_result_state: str,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id = _create_v4_job_fixture(path, status=status)

    LabJobStore(path).initialize()

    migrated = LabJobReader(path).get_job(job_id)
    assert migrated is not None
    assert migrated.status is status
    assert migrated.result_contract_version == lab_jobs.RESULT_CONTRACT_VERSION
    assert migrated.result_state.value == expected_result_state
    assert migrated.requires_complete_result is False
    assert LabJobReader(path).get_result_artifact(job_id) is None
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION


def test_reader_is_readonly_does_not_create_missing_database(tmp_path: Path) -> None:
    path = tmp_path / "missing.sqlite3"
    reader = LabJobReader(path)

    with pytest.raises(sqlite3.OperationalError):
        reader.get_job(uuid4())

    assert not path.exists()


def test_submit_roundtrips_validated_spec_and_typed_empty_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    spec = _spec(
        job_type=ResearchJobType.ABLATION,
        resource_class=ResourceClass.HEAVY,
    )
    envelope = _submit(spec=spec)

    receipt = store.apply_command(envelope, lease=lease, now=NOW)
    reader = LabJobReader(store.path)
    job = reader.get_job(envelope.command.job_id)

    assert receipt.status == "applied"
    assert receipt.job_version == 0
    assert isinstance(job, LabJobRecord)
    assert job is not None
    assert job.spec == spec
    assert job.spec.spec_hash == spec.spec_hash
    assert job.requires_complete_result is True
    assert job.job_type is ResearchJobType.ABLATION
    assert job.result_state.value == "pending"
    assert job.resource_class is ResourceClass.HEAVY
    assert job.status is JobStatus.QUEUED
    assert job.control_intent is ControlIntent.NONE
    assert job.attempt_count == 0
    command_record = reader.get_command(envelope.request_id)
    assert isinstance(command_record, LabCommandRecord)
    assert command_record is not None
    assert command_record.envelope == envelope
    assert command_record.receipt == receipt
    assert all(isinstance(row, LabEventRecord) for row in reader.list_events(job.job_id))
    assert reader.list_shards(job.job_id) == ()
    assert reader.list_artifacts(job.job_id) == ()
    assert isinstance(reader.list_leases()[0], LabLeaseRecord)
    assert LabShardRecord.model_fields
    assert LabArtifactRecord.model_fields


def test_get_job_uses_shard_aggregates_without_loading_twenty_thousand_models(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    timestamp = NOW.isoformat(timespec="microseconds")
    with sqlite3.connect(store.path) as connection:
        connection.executemany(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'queued', 0, 0, 3, ?, ?)
            """,
            (
                (str(UUID(int=index + 1)), str(job.job_id), index, timestamp, timestamp)
                for index in range(20_000)
            ),
        )

    statements: list[str] = []

    class TracingLabJobReader(LabJobReader):
        def _connect(self) -> sqlite3.Connection:
            connection = super()._connect()
            connection.set_trace_callback(statements.append)
            return connection

    def forbid_shard_model_construction(
        _cls: type[LabJobReader],
        _row: sqlite3.Row,
    ) -> LabShardRecord:
        raise AssertionError("get_job must not construct shard models")

    monkeypatch.setattr(
        LabJobReader,
        "_shard_from_row",
        classmethod(forbid_shard_model_construction),
    )
    reader = TracingLabJobReader(store.path)

    persisted = reader.get_job(job.job_id)

    assert persisted == job
    shard_queries = [
        " ".join(statement.split()).lower()
        for statement in statements
        if "from lab_shard" in statement.lower()
    ]
    assert len(shard_queries) == 1
    assert "count(" in shard_queries[0]
    assert "rquant_lab_shard_row_valid" in shard_queries[0]
    assert "select *" not in shard_queries[0]
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        reader.list_shards(job.job_id)


@pytest.mark.parametrize(
    ("mutation", "parameters"),
    [
        ("shard_id = ?", ("not-a-uuid",)),
        ("shard_id = ?", (str(UUID(int=0)),)),
        ("shard_id = upper(shard_id)", ()),
        ("shard_id = '{' || shard_id || '}'", ()),
        ("shard_id = 'urn:uuid:' || shard_id", ()),
        ("shard_id = ' ' || shard_id", ()),
        ("shard_id = ?", ("00000000-0000-4000-8000-000000000001",)),
        ("payload_json = ?", ('{"fraction":1.5}',)),
        ("payload_hash = ?", ("f" * 64,)),
        ("plan_hash = ?", ("g" * 64,)),
        ("adapter_id = ?", ("",)),
        ("phase = NULL", ()),
        ("status = 'bogus'", ()),
        ("status = 'running'", ()),
        ("claimed_at = ?", ("not-a-time",)),
        ("version = ?", (1.5,)),
        ("result_manifest_hash = ?", ("not-a-hash",)),
        ("attempt_count = max_attempts", ()),
        (
            "status = 'succeeded', duration_ms = 1000, "
            "throughput_units_per_second = 999, completion_sequence = 1, "
            "result_manifest_hash = ?, finished_at = ?",
            ("9" * 64, NOW.isoformat(timespec="microseconds")),
        ),
        (
            "status = 'cancelled', finished_at = ?, worker_id = 'stale-worker'",
            (NOW.isoformat(timespec="microseconds"),),
        ),
    ],
)
def test_get_job_and_list_shards_reject_the_same_corrupt_shard_rows_without_models(
    tmp_path: Path,
    mutation: str,
    parameters: tuple[object, ...],
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash="a" * 64,
        payload_json='{"hold_days":1}',
        work_plan=LabShardWorkPlan(
            phase="strategy_replay",
            work_unit_name="parameter_case",
            work_units=1,
            static_duration_ms=1_000,
        ),
    )
    planned = store.plan_job(
        job.job_id,
        (definition,),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    assert len(planned) == 1
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            f"UPDATE lab_shard SET {mutation} WHERE job_id = ? AND shard_id = ?",
            (*parameters, str(job.job_id), str(planned[0].shard_id)),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError):
        reader.get_job(job.job_id)
    with pytest.raises(InvalidStoredJobError):
        reader.list_shards(job.job_id)


@pytest.mark.parametrize(
    ("mutation", "parameters"),
    [
        ("payload_json = ?", ('{"hold_days":"bad","hold_days":1}',)),
        (
            "status = 'failed', failure_json = ?, finished_at = ?",
            ('{"reason":false,"reason":"failed"}', NOW.isoformat(timespec="microseconds")),
        ),
        (
            "status = 'checkpointed', checkpoint_json = ?",
            ('{"cursor":{"page":false,"page":1}}',),
        ),
    ],
)
def test_shard_udf_and_readers_reject_duplicate_persisted_json_keys(
    tmp_path: Path,
    mutation: str,
    parameters: tuple[object, ...],
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash="a" * 64,
        payload_json='{"hold_days":1}',
        work_plan=LabShardWorkPlan(
            phase="strategy_replay",
            work_unit_name="parameter_case",
            work_units=1,
            static_duration_ms=1_000,
        ),
    )
    shard = store.plan_job(
        job.job_id,
        (definition,),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )[0]
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            f"UPDATE lab_shard SET {mutation} WHERE shard_id = ?",
            (*parameters, str(shard.shard_id)),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError):
        reader.get_job(job.job_id)
    with pytest.raises(InvalidStoredJobError):
        reader.list_shards(job.job_id)


def test_job_reader_rejects_duplicate_spec_keys_even_when_second_value_is_valid(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    valid = job.spec.model_dump_json(round_trip=True)
    duplicate = valid.replace(
        '"schema_version":2',
        '"schema_version":false,"schema_version":2',
        1,
    )
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (duplicate, str(job.job_id)),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.get_job(job.job_id)
    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.list_jobs()


@pytest.mark.parametrize("column", ("command_json", "receipt_json"))
def test_command_reader_and_replay_reject_nested_duplicate_persisted_json(
    tmp_path: Path,
    column: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)
    with sqlite3.connect(store.path) as connection:
        stored = str(
            connection.execute(
                f"SELECT {column} FROM lab_command WHERE request_id = ?",
                (str(envelope.request_id),),
            ).fetchone()[0]
        )
        if column == "command_json":
            duplicate = stored.replace(
                '"schema_version":1',
                '"schema_version":false,"schema_version":1',
                1,
            )
        else:
            duplicate = stored.replace(
                '"job_version":0',
                '"job_version":false,"job_version":0',
                1,
            )
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            f"UPDATE lab_command SET {column} = ? WHERE request_id = ?",
            (duplicate, str(envelope.request_id)),
        )

    with pytest.raises(InvalidStoredJobError, match="stored lab command"):
        LabJobReader(store.path).get_command(envelope.request_id)
    with pytest.raises(InvalidStoredJobError, match="stored lab command"):
        store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))


def test_shard_udf_and_readers_reject_noncanonical_payload_bytes(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash="a" * 64,
        payload_json='{"hold_days":1}',
        work_plan=LabShardWorkPlan(
            phase="strategy_replay",
            work_unit_name="parameter_case",
            work_units=1,
            static_duration_ms=1_000,
        ),
    )
    shard = store.plan_job(job.job_id, (definition,), lease=lease, now=NOW)[0]
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_shard SET payload_json = ? WHERE shard_id = ?",
            ('{ "hold_days": 1 }', str(shard.shard_id)),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError):
        reader.get_job(job.job_id)
    with pytest.raises(InvalidStoredJobError):
        reader.list_shards(job.job_id)


def test_new_v1_submit_is_durably_rejected_and_replays_same_receipt(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit(spec=_v1_spec())

    first = store.apply_command(envelope, lease=lease, now=NOW)
    replayed = store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))

    assert first.status == "rejected"
    assert first.reason == "unsupported_spec_version"
    assert first.job_version is None
    assert replayed == first
    reader = LabJobReader(store.path)
    assert reader.get_job(envelope.command.job_id) is None
    command_record = reader.get_command(envelope.request_id)
    assert command_record is not None
    assert command_record.receipt == first
    assert _count(store.path, "lab_command") == 1
    assert _count(store.path, "lab_job") == 0


def test_new_formal_v3_submit_without_authority_writes_nothing(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    spec = _formal_v3_spec()
    envelope = _submit(spec=spec)

    with pytest.raises(FormalSubmissionAuthorityError, match="authoritative"):
        store.apply_command(envelope, lease=lease, now=NOW)

    reader = LabJobReader(store.path)
    assert reader.get_job(envelope.command.job_id) is None
    assert reader.get_command(envelope.request_id) is None
    assert _count(store.path, "lab_job") == 0
    assert _count(store.path, "lab_command") == 0


def test_new_formal_v3_submit_persists_after_authoritative_validation(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    spec = _formal_v3_spec()
    envelope = _submit(spec=spec)
    validated: list[tuple[LabCommandEnvelope, datetime]] = []

    receipt = store.apply_command(
        envelope,
        lease=lease,
        now=NOW,
        submission_authority=lambda submitted, observed_at: validated.append(
            (submitted, observed_at)
        ),
    )
    job = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert validated == [(envelope, NOW)]
    assert receipt.status == "applied"
    assert receipt.reason == "submitted_v3_owned"
    assert job is not None
    assert job.spec == spec
    assert job.spec.catalog_owner_eligible
    assert job.spec.experiment is not None
    assert job.spec.experiment.experiment_id == spec.experiment.experiment_id


def test_new_exploratory_v2_submit_is_explicitly_a_non_owner(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit(spec=_spec())

    receipt = store.apply_command(envelope, lease=lease, now=NOW)
    job = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert receipt.status == "applied"
    assert receipt.reason == "submitted_legacy_v2_exploratory_non_owner"
    assert job is not None
    assert job.spec.schema_version == 2
    assert not job.spec.catalog_owner_eligible


def test_new_formal_v2_submit_is_explicitly_non_owner_and_requires_migration(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit(spec=_formal_v2_spec())

    first = store.apply_command(envelope, lease=lease, now=NOW)
    replayed = store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))
    job = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert first.status == "rejected"
    assert first.reason == "v2_formal_requires_exploratory_migration"
    assert replayed == first
    assert job is None
    assert _count(store.path, "lab_command") == 1
    assert _count(store.path, "lab_job") == 0


def test_reader_and_exactly_once_replay_accept_real_legacy_v1_ledger(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    spec = _v1_spec()
    request_id = UUID("00000000-0000-0000-0000-000000000011")
    job_id = UUID("00000000-0000-0000-0000-000000000012")
    envelope = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=job_id, spec=spec, max_attempts=3),
    )
    assert envelope.content_hash == OLD_V1_COMMAND_HASH
    receipt = LabCommandReceipt(
        request_id=request_id,
        content_hash=OLD_V1_COMMAND_HASH,
        job_id=job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    timestamp = NOW.isoformat(timespec="microseconds")
    deadline = spec.deadline.isoformat(timespec="microseconds")
    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._SUBMIT_AUTH_FUNCTION,
            2,
            lambda candidate_job_id, candidate_spec_json: int(
                (candidate_job_id, candidate_spec_json) == (str(job_id), OLD_V1_SPEC_JSON)
            ),
        )
        connection.execute(
            """
                INSERT INTO lab_job (
                    job_id, spec_json, spec_hash, job_type, resource_class,
                    deadline, status, control_intent, version, attempt_count,
                    max_attempts, recoverable, scheduler_fencing_token,
                    created_at, updated_at, result_state,
                    requires_complete_result
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 3, 0, NULL, ?, ?,
                          'pending', 1)
            """,
            (
                str(job_id),
                OLD_V1_SPEC_JSON,
                OLD_V1_SPEC_HASH,
                "strategy_replay",
                "standard",
                deadline,
                "queued",
                "none",
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO lab_command (
                request_id, content_hash, command_type, job_id, command_json,
                status, reason, receipt_json, receipt_job_version,
                received_at, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(request_id),
                OLD_V1_COMMAND_HASH,
                "submit",
                str(job_id),
                canonical_model_json_bytes(envelope).decode("utf-8"),
                "applied",
                "submitted",
                canonical_model_json_bytes(receipt).decode("utf-8"),
                0,
                timestamp,
                timestamp,
            ),
        )

    reader = LabJobReader(store.path)
    job = reader.get_job(job_id)
    stored_command = reader.get_command(request_id)
    lease = _lease(store)
    replayed = store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))

    assert spec.spec_hash == OLD_V1_SPEC_HASH
    assert job is not None
    assert job.spec.schema_version == 1
    assert job.spec.spec_hash == OLD_V1_SPEC_HASH
    assert stored_command is not None
    assert stored_command.envelope.command.spec.spec_hash == OLD_V1_SPEC_HASH
    assert replayed == receipt

    unsafe_command = SubmitJobCommand.model_construct(
        command_type="submit",
        job_id=job_id,
        spec=_hidden_audit_v1_spec(),
        max_attempts=3,
    )
    unsafe_replay = LabCommandEnvelope.model_construct(
        schema_version=1,
        request_id=request_id,
        command=unsafe_command,
        content_hash=OLD_V1_COMMAND_HASH,
    )
    with pytest.raises(ValidationError, match="v1.*audit_run_id"):
        store.apply_command(
            unsafe_replay,
            lease=lease,
            now=NOW + timedelta(seconds=2),
        )

    assert _count(store.path, "lab_command") == 1
    assert _count(store.path, "lab_job") == 1


def test_receipt_job_version_column_roundtrips_applied_rejected_and_null(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    submit = _submit()
    submitted = store.apply_command(submit, lease=lease, now=NOW)
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=submit.command.job_id,
            expected_version=0,
            reason="cancel queued job",
        ),
    )
    cancelled = store.apply_command(cancel, lease=lease, now=NOW + timedelta(seconds=1))
    missing = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=uuid4(),
            expected_version=0,
            reason="missing job",
        ),
    )
    missing_rejection = store.apply_command(
        missing,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    stale = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=submit.command.job_id,
            expected_version=0,
            reason="stale control",
        ),
    )
    stale_rejection = store.apply_command(
        stale,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert (
        submitted.job_version,
        cancelled.job_version,
        missing_rejection.job_version,
        stale_rejection.job_version,
    ) == (0, 1, None, 1)
    reader = LabJobReader(store.path)
    records = tuple(
        reader.get_command(envelope.request_id) for envelope in (submit, cancel, missing, stale)
    )
    assert tuple(record.receipt_job_version for record in records if record is not None) == (
        0,
        1,
        None,
        1,
    )
    with sqlite3.connect(store.path) as connection:
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(lab_command)").fetchall()
        }
        stored_versions = tuple(
            row[0]
            for row in connection.execute(
                "SELECT receipt_job_version FROM lab_command ORDER BY applied_at"
            ).fetchall()
        )
    assert "receipt_job_version" in columns
    assert stored_versions == (0, 1, None, 1)


@pytest.mark.parametrize(
    "replacement",
    [pytest.param(0.5, id="real"), pytest.param(sqlite3.Binary(b"0"), id="blob")],
)
def test_receipt_job_version_schema_rejects_noninteger_storage(
    tmp_path: Path,
    replacement: object,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)

    with sqlite3.connect(store.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            connection.execute(
                "UPDATE lab_command SET receipt_job_version = ? WHERE request_id = ?",
                (replacement, str(envelope.request_id)),
            )
        stored = connection.execute(
            "SELECT receipt_job_version, typeof(receipt_job_version) "
            "FROM lab_command WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()

    assert stored == (0, "integer")


@pytest.mark.parametrize(
    ("target", "replacement"),
    [
        ("column", 9),
        ("column", None),
        ("receipt_json", 9),
        ("receipt_json", None),
    ],
)
def test_reader_and_replay_fail_closed_on_receipt_job_version_tamper(
    tmp_path: Path,
    target: str,
    replacement: int | None,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)
    with sqlite3.connect(store.path) as connection:
        if target == "column":
            connection.execute(
                "UPDATE lab_command SET receipt_job_version = ? WHERE request_id = ?",
                (replacement, str(envelope.request_id)),
            )
        else:
            row = connection.execute(
                "SELECT receipt_json FROM lab_command WHERE request_id = ?",
                (str(envelope.request_id),),
            ).fetchone()
            payload = json.loads(str(row[0]))
            payload["job_version"] = replacement
            connection.execute(
                "UPDATE lab_command SET receipt_json = ? WHERE request_id = ?",
                (canonical_json_bytes(payload).decode("utf-8"), str(envelope.request_id)),
            )

    with pytest.raises(InvalidStoredJobError, match="job version mismatch"):
        LabJobReader(store.path).get_command(envelope.request_id)
    with pytest.raises(InvalidStoredJobError, match="job version mismatch"):
        store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))


@pytest.mark.parametrize(
    "replacement",
    [
        pytest.param(0.5, id="fractional-half"),
        pytest.param(7.5, id="version-plus-half"),
        pytest.param(sqlite3.Binary(b"0"), id="quoted-zero-noninteger-storage"),
    ],
)
def test_reader_and_replay_reject_noninteger_receipt_job_version_storage(
    tmp_path: Path,
    replacement: object,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_command SET receipt_job_version = ? WHERE request_id = ?",
            (replacement, str(envelope.request_id)),
        )
        stored_type = connection.execute(
            "SELECT typeof(receipt_job_version) FROM lab_command WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()[0]

    assert stored_type in {"real", "blob"}
    with pytest.raises(InvalidStoredJobError, match="SQLite integer"):
        LabJobReader(store.path).get_command(envelope.request_id)
    with pytest.raises(InvalidStoredJobError, match="SQLite integer"):
        store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))


@pytest.mark.parametrize("replacement", [True, False, 0.0, "0"])
def test_strict_sqlite_integer_helper_rejects_bool_real_and_text(
    replacement: object,
) -> None:
    with pytest.raises(InvalidStoredJobError, match="SQLite integer"):
        lab_jobs._strict_sqlite_int(replacement, field="test.value")


@pytest.mark.parametrize(
    ("table", "column", "replacement", "reader_name"),
    [
        ("lab_job", "version", 1.5, "job"),
        ("lab_job", "attempt_count", "0_1", "job"),
        ("lab_job", "max_attempts", 3.5, "job"),
        ("lab_job", "scheduler_fencing_token", "0_1", "job"),
        ("lab_shard", "shard_index", 0.5, "shard"),
        ("lab_shard", "version", "0_0", "shard"),
        ("lab_shard", "attempt_count", 0.5, "shard"),
        ("lab_shard", "max_attempts", "0_3", "shard"),
        ("lab_shard", "scheduler_fencing_token", 1.5, "shard"),
        ("lab_event", "job_version", 0.5, "event"),
        ("lab_event", "scheduler_fencing_token", "0_1", "event"),
        ("lab_lease", "fencing_token", 1.5, "lease"),
    ],
)
def test_typed_row_readers_reject_noninteger_version_count_and_fence_columns(
    tmp_path: Path,
    table: str,
    column: str,
    replacement: object,
    reader_name: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    job = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="seed numeric rows",
        now=NOW + timedelta(seconds=1),
    )
    shard_id = uuid4()
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version, attempt_count,
                max_attempts, worker_id, scheduler_fencing_token,
                checkpoint_json, created_at, updated_at
            ) VALUES (?, ?, 0, 'running', 0, 1, 3, 'worker-a', ?, NULL, ?, ?)
            """,
            (
                str(shard_id),
                str(job.job_id),
                lease.fencing_token,
                NOW.isoformat(timespec="microseconds"),
                NOW.isoformat(timespec="microseconds"),
            ),
        )
        connection.execute("PRAGMA ignore_check_constraints = ON")
        where = {
            "lab_job": ("job_id", str(job.job_id)),
            "lab_shard": ("shard_id", str(shard_id)),
            "lab_event": (
                "event_id",
                connection.execute("SELECT MIN(event_id) FROM lab_event").fetchone()[0],
            ),
            "lab_lease": ("lease_id", lease.lease_id),
        }[table]
        connection.execute(
            f"UPDATE {table} SET {column} = ? WHERE {where[0]} = ?",
            (replacement, where[1]),
        )

    reader = LabJobReader(store.path)
    read = {
        "job": lambda: reader.get_job(job.job_id),
        "shard": lambda: reader.list_shards(job.job_id),
        "event": lambda: reader.list_events(job.job_id),
        "lease": reader.list_leases,
    }[reader_name]
    with pytest.raises(InvalidStoredJobError, match="SQLite integer"):
        read()


def test_same_request_and_hash_is_exactly_once_without_second_event(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()

    first = store.apply_command(envelope, lease=lease, now=NOW)
    event_count = _count(store.path, "lab_event")
    second = store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))

    assert second == first
    assert _count(store.path, "lab_command") == 1
    assert _count(store.path, "lab_job") == 1
    assert _count(store.path, "lab_event") == event_count


def test_same_request_with_different_hash_conflicts_with_zero_modification(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    request_id = uuid4()
    first = _submit(request_id=request_id)
    store.apply_command(first, lease=lease, now=NOW)
    before = tuple(_count(store.path, table) for table in ("lab_command", "lab_job", "lab_event"))

    with pytest.raises(RequestContentConflictError):
        store.apply_command(
            _submit(request_id=request_id),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )

    assert (
        tuple(_count(store.path, table) for table in ("lab_command", "lab_job", "lab_event"))
        == before
    )


def test_reused_job_id_is_durably_rejected_and_replayed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job_id = uuid4()
    store.apply_command(_submit(job_id=job_id), lease=lease, now=NOW)
    reused = _submit(job_id=job_id)

    first = store.apply_command(reused, lease=lease, now=NOW + timedelta(seconds=1))
    event_count = _count(store.path, "lab_event")
    replay = store.apply_command(reused, lease=lease, now=NOW + timedelta(seconds=2))

    assert first.status == "rejected"
    assert first.reason == "job_id_reused"
    assert replay == first
    assert _count(store.path, "lab_job") == 1
    assert _count(store.path, "lab_event") == event_count


def test_illegal_cancel_is_durably_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, JobStatus.FAILED)
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="too late",
        ),
    )

    first = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=3))
    replay = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=4))

    assert first.status == "rejected"
    assert first.reason == "invalid_state:failed"
    assert replay == first
    assert LabJobReader(store.path).get_job(job.job_id).status is JobStatus.FAILED


def test_stale_command_version_is_durably_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    event_count = _count(store.path, "lab_event")
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version + 1,
            reason="stale UI",
        ),
    )

    first = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=1))
    replay = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=2))

    assert first.status == "rejected"
    assert first.reason == f"stale_version:{job.version}"
    assert replay == first
    assert _count(store.path, "lab_event") == event_count


def test_pause_and_resume_commands_are_persistent_and_exactly_once(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="free resources",
        ),
    )

    paused_receipt = store.apply_command(
        pause,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    paused = LabJobReader(store.path).get_job(running.job_id)
    assert paused is not None
    assert paused_receipt.status == "applied"
    assert paused_receipt.reason == "pause_requested"
    assert paused.status is JobStatus.RUNNING
    assert paused.control_intent is ControlIntent.PAUSE_REQUESTED
    event_count = _count(store.path, "lab_event")
    assert (
        store.apply_command(
            pause,
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
        == paused_receipt
    )
    assert _count(store.path, "lab_event") == event_count

    checkpointed = store.transition_job(
        paused.job_id,
        expected_version=paused.version,
        target_status=JobStatus.CHECKPOINTED,
        lease=lease,
        reason="worker reached safe point",
        now=NOW + timedelta(seconds=5),
    )
    assert checkpointed.control_intent is ControlIntent.NONE

    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(
            job_id=checkpointed.job_id,
            expected_version=checkpointed.version,
            reason="capacity restored",
        ),
    )
    resumed_receipt = store.apply_command(
        resume,
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    resumed = LabJobReader(store.path).get_job(paused.job_id)
    assert resumed is not None
    assert resumed_receipt.status == "applied"
    assert resumed.status is JobStatus.RUNNING
    assert resumed.control_intent is ControlIntent.NONE


def test_resume_can_withdraw_unacknowledged_pause_intent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="pause",
        ),
    )
    store.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=3))
    paused = LabJobReader(store.path).get_job(running.job_id)
    assert paused is not None
    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(
            job_id=paused.job_id,
            expected_version=paused.version,
            reason="withdraw pause",
        ),
    )

    receipt = store.apply_command(
        resume,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    resumed = LabJobReader(store.path).get_job(running.job_id)

    assert receipt.status == "applied"
    assert receipt.reason == "pause_withdrawn"
    assert resumed is not None
    assert resumed.status is JobStatus.RUNNING
    assert resumed.control_intent is ControlIntent.NONE


def test_running_cancel_records_intent_before_worker_terminal_ack(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="operator cancel",
        ),
    )

    receipt = store.apply_command(
        cancel,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    requested = LabJobReader(store.path).get_job(running.job_id)

    assert receipt.status == "applied"
    assert receipt.reason == "cancel_requested"
    assert requested is not None
    assert requested.status is JobStatus.RUNNING
    assert requested.control_intent is ControlIntent.CANCEL_REQUESTED

    cancelled = store.confirm_cancelled_job(
        requested.job_id,
        expected_version=requested.version,
        lease=lease,
        reason="worker invalidated claim",
        now=NOW + timedelta(seconds=4),
    )
    assert cancelled.status is JobStatus.CANCELLED
    assert cancelled.control_intent is ControlIntent.NONE


def test_running_cancel_terminal_requires_explicit_requested_confirmation(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)

    with pytest.raises(CancelConfirmationRequiredError):
        store.transition_job(
            running.job_id,
            expected_version=running.version,
            target_status=JobStatus.CANCELLED,
            lease=lease,
            reason="unsafe direct cancel",
            now=NOW + timedelta(seconds=3),
        )
    with pytest.raises(CancelConfirmationRequiredError):
        store.confirm_cancelled_job(
            running.job_id,
            expected_version=running.version,
            lease=lease,
            reason="missing request",
            now=NOW + timedelta(seconds=4),
        )


@pytest.mark.parametrize(
    "late_status",
    [JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CHECKPOINTED],
)
def test_cancel_requested_blocks_late_lifecycle_until_explicit_confirmation(
    tmp_path: Path,
    late_status: JobStatus,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="cancel first",
        ),
    )
    store.apply_command(cancel, lease=lease, now=NOW + timedelta(seconds=3))
    requested = LabJobReader(store.path).get_job(running.job_id)
    assert requested is not None
    event_count = _count(store.path, "lab_event")

    with pytest.raises(CancelConfirmationRequiredError):
        store.transition_job(
            requested.job_id,
            expected_version=requested.version,
            target_status=late_status,
            lease=lease,
            reason="late worker result",
            recoverable=late_status is JobStatus.FAILED,
            now=NOW + timedelta(seconds=4),
        )

    unchanged = LabJobReader(store.path).get_job(requested.job_id)
    assert unchanged is not None
    assert unchanged.status is JobStatus.RUNNING
    assert unchanged.control_intent is ControlIntent.CANCEL_REQUESTED
    assert _count(store.path, "lab_event") == event_count

    confirmed = store.confirm_cancelled_job(
        requested.job_id,
        expected_version=requested.version,
        lease=lease,
        reason="worker claim invalidated",
        now=NOW + timedelta(seconds=5),
    )
    assert confirmed.status is JobStatus.CANCELLED
    assert confirmed.control_intent is ControlIntent.NONE


def test_terminal_failure_committed_before_cancel_keeps_terminal_state(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    terminal = store.transition_job(
        running.job_id,
        expected_version=running.version,
        target_status=JobStatus.FAILED,
        lease=lease,
        reason="failure first",
        recoverable=False,
        now=NOW + timedelta(seconds=3),
    )
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=terminal.job_id,
            expected_version=terminal.version,
            reason="late cancel",
        ),
    )

    receipt = store.apply_command(
        cancel,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_state:failed"
    stored = LabJobReader(store.path).get_job(terminal.job_id)
    assert stored is not None
    assert stored.status is JobStatus.FAILED
    assert terminal.control_intent is ControlIntent.NONE


def test_takeover_finalizes_cancel_requested_and_cannot_resume(tmp_path: Path) -> None:
    store = _store(tmp_path)
    old = _lease(store, owner="scheduler-old", seconds=10)
    running = _transition_to(store, old, JobStatus.RUNNING)
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="cancel before crash",
        ),
    )
    store.apply_command(cancel, lease=old, now=NOW + timedelta(seconds=2))
    takeover_at = NOW + timedelta(seconds=11)
    new = _lease(store, owner="scheduler-new", now=takeover_at, seconds=60)

    recovered = store.recover_expired_jobs(new, now=takeover_at)

    assert recovered[0].status is JobStatus.CANCELLED
    assert recovered[0].control_intent is ControlIntent.NONE
    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(
            job_id=running.job_id,
            expected_version=recovered[0].version,
            reason="must not revive",
        ),
    )
    receipt = store.apply_command(
        resume,
        lease=new,
        now=takeover_at + timedelta(seconds=1),
    )
    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_state:cancelled"


def test_pause_in_wrong_state_is_durably_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    queued = _submit_job(store, lease)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=queued.job_id,
            expected_version=queued.version,
            reason="not running",
        ),
    )

    receipt = store.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))

    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_state:queued"


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.CHECKPOINTED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.CHECKPOINTED, JobStatus.RUNNING),
        (JobStatus.CHECKPOINTED, JobStatus.CANCELLED),
    ],
)
def test_complete_state_matrix_allows_only_documented_edges(
    tmp_path: Path,
    source: JobStatus,
    target: JobStatus,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, source)

    transitioned = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=target,
        lease=lease,
        reason="matrix",
        recoverable=target is JobStatus.FAILED,
        now=NOW + timedelta(seconds=10),
    )

    assert transitioned.status is target
    assert transitioned.version == job.version + 1


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (JobStatus.QUEUED, JobStatus.SUCCEEDED),
        (JobStatus.QUEUED, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.QUEUED),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.CHECKPOINTED, JobStatus.SUCCEEDED),
        (JobStatus.FAILED, JobStatus.RUNNING),
        (JobStatus.CANCELLED, JobStatus.QUEUED),
    ],
)
def test_complete_state_matrix_rejects_undocumented_edges(
    tmp_path: Path,
    source: JobStatus,
    target: JobStatus,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, source)

    with pytest.raises(InvalidJobTransitionError):
        store.transition_job(
            job.job_id,
            expected_version=job.version,
            target_status=target,
            lease=lease,
            reason="invalid",
            now=NOW + timedelta(seconds=10),
        )


def test_transition_rejects_stale_version_without_event(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    event_count = _count(store.path, "lab_event")

    with pytest.raises(StaleJobVersionError):
        store.transition_job(
            job.job_id,
            expected_version=job.version + 1,
            target_status=JobStatus.RUNNING,
            lease=lease,
            reason="stale",
            now=NOW + timedelta(seconds=1),
        )

    assert _count(store.path, "lab_event") == event_count


def test_failed_job_retries_only_explicitly_when_attempt_budget_remains(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    failed = _transition_to(store, lease, JobStatus.FAILED)
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=RetryJobCommand(
            job_id=failed.job_id,
            expected_version=failed.version,
            reason="source recovered",
        ),
    )

    receipt = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=3))
    retried = LabJobReader(store.path).get_job(failed.job_id)

    assert receipt.status == "applied"
    assert retried is not None
    assert retried.status is JobStatus.QUEUED
    assert retried.version == failed.version + 1


def test_retry_is_durably_rejected_after_attempt_budget_exhausted(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease, max_attempts=1)
    running = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="start",
        now=NOW + timedelta(seconds=1),
    )
    failed = store.transition_job(
        job.job_id,
        expected_version=running.version,
        target_status=JobStatus.FAILED,
        lease=lease,
        reason="failed",
        recoverable=True,
        now=NOW + timedelta(seconds=2),
    )
    retry = LabCommandEnvelope(
        request_id=uuid4(),
        command=RetryJobCommand(
            job_id=job.job_id,
            expected_version=failed.version,
            reason="again",
        ),
    )

    receipt = store.apply_command(retry, lease=lease, now=NOW + timedelta(seconds=3))

    assert receipt.status == "rejected"
    assert receipt.reason == "attempts_exhausted"


def test_active_scheduler_lease_rejects_second_owner(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _lease(store, owner="scheduler-a")

    with pytest.raises(SchedulerLeaseUnavailableError):
        _lease(store, owner="scheduler-b", now=NOW + timedelta(seconds=30))

    assert first.released_at is None
    assert len(LabJobReader(store.path).list_leases()) == 1


def test_expired_lease_is_released_before_fenced_takeover(tmp_path: Path) -> None:
    store = _store(tmp_path)
    old = _lease(store, owner="scheduler-a", seconds=10)
    new = _lease(
        store,
        owner="scheduler-b",
        now=NOW + timedelta(seconds=11),
        seconds=20,
    )
    leases = LabJobReader(store.path).list_leases()

    assert new.fencing_token == old.fencing_token + 1
    assert leases[0].released_at == NOW + timedelta(seconds=11)
    assert leases[1] == new


def test_old_owner_and_public_api_cannot_complete_after_takeover(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    old = _lease(store, owner="scheduler-a", seconds=10)
    running = _transition_to(store, old, JobStatus.RUNNING)
    takeover_at = NOW + timedelta(seconds=11)
    new = _lease(store, owner="scheduler-b", now=takeover_at, seconds=60)
    recovered = store.recover_expired_jobs(new, now=takeover_at)

    assert recovered[0].status is JobStatus.CHECKPOINTED
    with pytest.raises(SchedulerLeaseFencedError):
        store.transition_job(
            running.job_id,
            expected_version=running.version,
            target_status=JobStatus.SUCCEEDED,
            lease=old,
            reason="late completion",
            now=takeover_at,
        )

    resumed = store.transition_job(
        running.job_id,
        expected_version=recovered[0].version,
        target_status=JobStatus.RUNNING,
        lease=new,
        reason="resume",
        now=takeover_at + timedelta(seconds=1),
    )
    with pytest.raises(InvalidJobTransitionError, match="artifact commit"):
        store.transition_job(
            running.job_id,
            expected_version=resumed.version,
            target_status=JobStatus.SUCCEEDED,
            lease=new,
            reason="complete",
            now=takeover_at + timedelta(seconds=2),
        )
    assert LabJobReader(store.path).get_job(running.job_id) == resumed


def test_heartbeat_renews_without_appending_event(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    event_count = _count(store.path, "lab_event")

    renewed = store.renew_scheduler_lease(
        lease,
        lease_seconds=60,
        now=NOW + timedelta(seconds=20),
    )

    assert renewed.heartbeat_at == NOW + timedelta(seconds=20)
    assert renewed.expires_at == NOW + timedelta(seconds=80)
    assert _count(store.path, "lab_event") == event_count


@pytest.mark.parametrize(
    ("pragma", "tampered_value"),
    [("user_version", 2), ("application_id", 12_345)],
)
def test_writer_mutation_fails_closed_after_database_identity_tamper(
    tmp_path: Path,
    pragma: str,
    tampered_value: int,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    with sqlite3.connect(store.path) as connection:
        before = connection.execute(
            """
            SELECT owner_id, token, fencing_token, acquired_at, heartbeat_at,
                   expires_at, released_at
            FROM lab_lease
            WHERE lease_id = ?
            """,
            (lease.lease_id,),
        ).fetchone()
        connection.execute(f"PRAGMA {pragma} = {tampered_value}")

    with pytest.raises(LabDatabaseIdentityError, match=pragma):
        store.renew_scheduler_lease(
            lease,
            lease_seconds=60,
            now=NOW + timedelta(seconds=20),
        )

    with sqlite3.connect(store.path) as connection:
        after = connection.execute(
            """
            SELECT owner_id, token, fencing_token, acquired_at, heartbeat_at,
                   expires_at, released_at
            FROM lab_lease
            WHERE lease_id = ?
            """,
            (lease.lease_id,),
        ).fetchone()
        persisted_pragma = connection.execute(f"PRAGMA {pragma}").fetchone()[0]

    assert after == before
    assert persisted_pragma == tampered_value


def test_submit_transaction_rolls_back_when_event_insert_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)

    def reject_event(
        _connection: sqlite3.Connection,
        **_kwargs: object,
    ) -> None:
        raise sqlite3.IntegrityError("event rejected")

    monkeypatch.setattr(store, "_insert_event", reject_event)
    envelope = _submit()

    with pytest.raises(sqlite3.IntegrityError, match="event rejected"):
        store.apply_command(envelope, lease=lease, now=NOW)

    assert _count(store.path, "lab_command") == 0
    assert _count(store.path, "lab_job") == 0
    assert _count(store.path, "lab_event") == 0


def test_readonly_reader_sees_committed_wal_data(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)

    reader = LabJobReader(store.path)
    assert reader.get_job(envelope.command.job_id) is not None
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        reader.execute_for_test("DELETE FROM lab_job")


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("request_id", str(UUID("00000000-0000-0000-0000-000000000001"))),
        ("content_hash", "f" * 64),
        ("job_id", str(UUID("00000000-0000-0000-0000-000000000002"))),
        ("status", "rejected"),
        ("reason", "tampered"),
    ],
)
def test_reader_and_replay_share_full_receipt_consistency_validation(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT receipt_json FROM lab_command WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()
        payload = json.loads(str(row[0]))
        payload[field] = replacement
        connection.execute(
            "UPDATE lab_command SET receipt_json = ? WHERE request_id = ?",
            (canonical_json_bytes(payload).decode("utf-8"), str(envelope.request_id)),
        )

    with pytest.raises(InvalidStoredJobError):
        LabJobReader(store.path).get_command(envelope.request_id)
    with pytest.raises(InvalidStoredJobError):
        store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))


@pytest.mark.parametrize(
    ("column", "replacement"),
    [
        ("content_hash", "f" * 64),
        ("command_type", "cancel"),
        ("job_id", str(UUID("00000000-0000-0000-0000-000000000003"))),
        ("status", "rejected"),
        ("reason", "tampered"),
    ],
)
def test_reader_and_replay_share_full_command_column_consistency_validation(
    tmp_path: Path,
    column: str,
    replacement: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            f"UPDATE lab_command SET {column} = ? WHERE request_id = ?",
            (replacement, str(envelope.request_id)),
        )

    with pytest.raises(InvalidStoredJobError):
        LabJobReader(store.path).get_command(envelope.request_id)
    with pytest.raises(InvalidStoredJobError):
        store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))


@pytest.mark.parametrize(
    "column",
    ["spec_json", "spec_hash", "job_type", "resource_class", "deadline"],
)
def test_reader_fails_closed_on_tampered_spec_or_denormalized_columns(
    tmp_path: Path,
    column: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    replacements = {
        "spec_json": "{}",
        "spec_hash": "f" * 64,
        "job_type": ResearchJobType.ABLATION.value,
        "resource_class": ResourceClass.HEAVY.value,
        "deadline": "2030-01-01T00:00:00+00:00",
    }
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute(
            f"UPDATE lab_job SET {column} = ? WHERE job_id = ?",
            (replacements[column], str(job.job_id)),
        )

    with pytest.raises(InvalidStoredJobError):
        LabJobReader(store.path).get_job(job.job_id)
