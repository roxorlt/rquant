from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import rquant.lab_jobs as lab_jobs
from rquant.lab_artifact_protocol import LabArtifactCommitReceipt
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
from rquant.lab_shard_protocol import LabShardDefinition, LabShardWorkPlan
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)

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
        research_status="comparable",
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


def test_initialize_creates_v5_schema_and_required_pragmas(
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
        schema_sql = " ".join(
            str(row[0])
            for row in connection.execute("SELECT sql FROM sqlite_master WHERE type = 'table'")
        ).upper()

    assert {
        "lab_command",
        "lab_job",
        "lab_shard",
        "lab_event",
        "lab_lease",
        "lab_artifact",
        "lab_artifact_commit",
        "lab_job_result_artifact",
    } <= tables
    assert application_id == LabJobStore.APPLICATION_ID
    assert user_version == 5
    assert str(journal_mode).lower() == "wal"
    assert synchronous == 2
    assert ") STRICT" not in schema_sql
    assert "TYPEOF(RECEIPT_JOB_VERSION) = 'INTEGER'" in " ".join(schema_sql.split())

    pragmas = store.connection_pragmas()
    assert pragmas.journal_mode == "wal"
    assert pragmas.synchronous == 2
    assert pragmas.foreign_keys == 1
    assert pragmas.busy_timeout_ms == 1_234


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

    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        LabJobReader(store.path).get_job(uuid4())


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

    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        LabJobReader(store.path).get_job(uuid4())


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

    with pytest.raises(LabDatabaseIdentityError, match="unexpected.*trigger|trigger.*set"):
        LabJobReader(store.path).get_job(uuid4())
    with pytest.raises(LabDatabaseIdentityError, match="unexpected.*trigger|trigger.*set"):
        store.connection_pragmas()


def test_v5_schema_identity_ignores_connection_local_temp_trigger(tmp_path: Path) -> None:
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
        lab_jobs._validate_v5_schema(connection)


def test_v5_schema_rejects_missing_persistent_trigger(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER trg_lab_result_artifact_no_delete")

    with pytest.raises(LabDatabaseIdentityError, match="missing.*trigger"):
        LabJobReader(store.path).get_job(uuid4())


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

    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        LabJobReader(store.path).get_job(uuid4())
    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        store.connection_pragmas()


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

    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        LabJobReader(store.path).get_job(uuid4())
    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        store.connection_pragmas()


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

    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        LabJobReader(store.path).get_job(uuid4())
    with pytest.raises(LabDatabaseIdentityError, match="trigger.*structure"):
        store.connection_pragmas()


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

    with pytest.raises(LabDatabaseIdentityError, match="v5.*constraint|primary|unique|foreign"):
        LabJobReader(store.path).get_job(uuid4())


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

    with pytest.raises(LabDatabaseIdentityError, match="v5.*constraint"):
        LabJobReader(store.path).get_job(uuid4())


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
    assert user_version == 5
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
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5


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
    command_payload = envelope.model_dump(mode="json")
    command = command_payload["command"]
    assert isinstance(command, dict)
    command["spec"] = json.loads(OLD_V1_SPEC_JSON)
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
                json.dumps(command_payload, separators=(",", ":")),
                "applied",
                "submitted",
                receipt.model_dump_json(),
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
                (json.dumps(payload), str(envelope.request_id)),
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
            (json.dumps(payload), str(envelope.request_id)),
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
