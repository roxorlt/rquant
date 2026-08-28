from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from rquant.lab_eta import (
    LabEtaCompletedShard,
    LabEtaFinishWindow,
    LabEtaInput,
    LabEtaProjectionError,
    LabEtaRemainingShard,
    estimate_lab_eta,
)
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    PauseJobCommand,
    ResumeJobCommand,
)
from rquant.lab_jobs import (
    MAX_JOB_SHARDS,
    ControlIntent,
    InvalidStoredJobError,
    JobStatus,
    LabJobReader,
    LabJobStore,
    ShardStatus,
)
from rquant.lab_shard_protocol import LabShardSucceeded, LabShardTelemetry, LabShardWorkPlan

from .test_lab_jobs import NOW, _lease, _submit_job
from .test_lab_shard_control_plane import _claim, _report, _setup

AS_OF = datetime(2026, 7, 24, 4, 0, tzinfo=UTC)
_EXTREME_OFFSET_TIMES = (
    pytest.param(
        datetime.min.replace(tzinfo=timezone(timedelta(hours=14))),
        id="datetime-min-plus-14",
    ),
    pytest.param(
        datetime.max.replace(tzinfo=timezone(-timedelta(hours=12))),
        id="datetime-max-minus-12",
    ),
)
_NEAR_BOUND_OFFSET_TIMES = (
    pytest.param(
        (datetime.min + timedelta(hours=14)).replace(tzinfo=timezone(timedelta(hours=14))),
        datetime.min.replace(tzinfo=UTC),
        id="utc-min",
    ),
    pytest.param(
        (datetime.max - timedelta(hours=12)).replace(tzinfo=timezone(-timedelta(hours=12))),
        datetime.max.replace(tzinfo=UTC),
        id="utc-max",
    ),
)


def _plan(
    *,
    phase: str = "scan",
    unit: str = "trading_day",
    work_units: int = 2,
    static_duration_ms: int = 1_000,
) -> LabShardWorkPlan:
    return LabShardWorkPlan(
        phase=phase,
        work_unit_name=unit,
        work_units=work_units,
        static_duration_ms=static_duration_ms,
    )


def _completed(
    sequence: int,
    ms_per_unit: float,
    *,
    phase: str = "scan",
    unit: str = "trading_day",
    work_units: int = 2,
) -> LabEtaCompletedShard:
    duration_ms = ms_per_unit * work_units
    return LabEtaCompletedShard(
        shard_id=UUID(int=sequence),
        completion_sequence=sequence,
        telemetry=LabShardTelemetry(
            phase=phase,
            work_unit_name=unit,
            work_units=work_units,
            static_duration_ms=1_000,
            duration_ms=duration_ms,
            throughput_units_per_second=work_units / (duration_ms / 1_000),
        ),
    )


def _remaining(
    value: int,
    *,
    phase: str = "scan",
    unit: str = "trading_day",
    work_units: int = 2,
    static_duration_ms: int = 1_000,
) -> LabEtaRemainingShard:
    return LabEtaRemainingShard(
        shard_id=UUID(int=100 + value),
        work_plan=_plan(
            phase=phase,
            unit=unit,
            work_units=work_units,
            static_duration_ms=static_duration_ms,
        ),
    )


def _input(
    *,
    status: str = "running",
    completed: tuple[LabEtaCompletedShard, ...] = (),
    remaining: tuple[LabEtaRemainingShard, ...] = (_remaining(1),),
    as_of: datetime = AS_OF,
) -> LabEtaInput:
    return LabEtaInput(
        job_id=UUID(int=999),
        status=status,
        as_of=as_of,
        completed=completed,
        remaining=remaining,
    )


def test_eta_uses_documented_static_interval_before_three_completed_shards() -> None:
    estimate = estimate_lab_eta(
        _input(
            completed=(_completed(1, 10), _completed(2, 10_000)),
            remaining=(
                _remaining(1, static_duration_ms=1_000),
                _remaining(2, static_duration_ms=3_000),
            ),
        )
    )

    assert estimate.estimator == "static"
    assert estimate.completed_telemetry_shards == 2
    assert estimate.remaining_duration is not None
    assert estimate.remaining_duration.center_ms == 4_000
    assert estimate.remaining_duration.low_ms == 3_000
    assert estimate.remaining_duration.high_ms == 6_000


@pytest.mark.parametrize("status", ["checkpointed", "paused"])
def test_eta_has_no_prediction_while_execution_is_paused(status: str) -> None:
    estimate = estimate_lab_eta(_input(status=status))

    assert estimate.estimator == "unavailable"
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None


def test_eta_switches_at_exactly_three_and_uses_deterministic_ewma_variance() -> None:
    estimate = estimate_lab_eta(
        _input(
            completed=(
                _completed(1, 100),
                _completed(2, 200),
                _completed(3, 300),
            )
        )
    )

    assert estimate.estimator == "ewma"
    assert estimate.remaining_duration is not None
    assert estimate.remaining_duration.center_ms == pytest.approx(450)
    expected_sd = 6_875**0.5
    assert estimate.remaining_duration.low_ms == pytest.approx(
        max(1, 0.25 * 225, 225 - 1.645 * expected_sd) * 2
    )
    assert estimate.remaining_duration.high_ms == pytest.approx(
        min(4 * 225, 225 + 1.645 * expected_sd) * 2
    )


def test_eta_aggregates_phases_and_falls_back_static_for_unsampled_phase() -> None:
    estimate = estimate_lab_eta(
        _input(
            completed=(
                _completed(1, 100, phase="scan"),
                _completed(2, 100, phase="scan"),
                _completed(3, 100, phase="scan"),
            ),
            remaining=(
                _remaining(1, phase="scan", work_units=2),
                _remaining(
                    2,
                    phase="aggregate",
                    unit="candidate",
                    work_units=4,
                    static_duration_ms=5_000,
                ),
            ),
        )
    )

    assert estimate.estimator == "mixed"
    assert estimate.remaining_duration is not None
    assert estimate.remaining_duration.center_ms == pytest.approx(5_200)
    assert estimate.remaining_duration.low_ms == pytest.approx(3_950)
    assert estimate.remaining_duration.high_ms == pytest.approx(7_700)


def test_eta_is_independent_of_input_order() -> None:
    completed = (
        _completed(3, 500),
        _completed(1, 100),
        _completed(4, 50, phase="aggregate", unit="candidate"),
        _completed(2, 200),
    )
    remaining = (
        _remaining(2, phase="aggregate", unit="candidate"),
        _remaining(1),
    )

    forward = estimate_lab_eta(_input(completed=completed, remaining=remaining))
    reverse = estimate_lab_eta(
        _input(completed=tuple(reversed(completed)), remaining=tuple(reversed(remaining)))
    )

    assert reverse == forward


@pytest.mark.parametrize("status", ["queued", "running"])
def test_active_eta_projects_finish_window_from_explicit_as_of(status: str) -> None:
    estimate = estimate_lab_eta(_input(status=status))

    assert estimate.finish_at is not None
    assert estimate.finish_at.center == AS_OF + timedelta(seconds=1)
    assert estimate.finish_at.low == AS_OF + timedelta(milliseconds=750)
    assert estimate.finish_at.high == AS_OF + timedelta(milliseconds=1_500)


def test_paused_eta_keeps_progress_counts_but_no_prediction_interval() -> None:
    estimate = estimate_lab_eta(
        _input(
            status="paused",
            completed=(_completed(1, 100),),
            remaining=(_remaining(1), _remaining(2)),
        )
    )

    assert estimate.completed_telemetry_shards == 1
    assert estimate.remaining_shards == 2
    assert estimate.estimator == "unavailable"
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None


def test_succeeded_eta_is_zero_at_as_of() -> None:
    estimate = estimate_lab_eta(_input(status="succeeded"))

    assert estimate.estimator == "terminal"
    assert estimate.remaining_duration is not None
    assert estimate.remaining_duration.center_ms == 0
    assert estimate.finish_at is not None
    assert estimate.finish_at.low == estimate.finish_at.center == estimate.finish_at.high == AS_OF


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_failed_and_cancelled_eta_are_null(status: str) -> None:
    estimate = estimate_lab_eta(_input(status=status))

    assert estimate.estimator == "unavailable"
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None


def test_legacy_remaining_without_work_plan_is_deterministically_unknown() -> None:
    estimate = estimate_lab_eta(
        _input(remaining=(LabEtaRemainingShard(shard_id=UUID(int=123), work_plan=None),))
    )

    assert estimate.estimator == "unknown"
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None


def test_eta_requires_aware_as_of_and_normalizes_an_offset_timezone() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _input(as_of=datetime(2026, 7, 24, 4, 0))
    with pytest.raises(ValidationError, match="ETA finish timestamps must be timezone-aware"):
        naive = datetime(2026, 7, 24, 4, 0)
        LabEtaFinishWindow(low=naive, center=naive, high=naive)

    offset = timezone(timedelta(hours=8))
    estimate = estimate_lab_eta(_input(as_of=datetime(2026, 7, 24, 12, 0, tzinfo=offset)))

    assert estimate.as_of == AS_OF
    assert estimate.finish_at is not None
    assert estimate.finish_at.center.tzinfo is UTC


@pytest.mark.parametrize("as_of", _EXTREME_OFFSET_TIMES)
def test_eta_input_extreme_offset_raises_typed_validation_error(as_of: datetime) -> None:
    with pytest.raises(ValidationError, match="outside the UTC datetime domain"):
        _input(as_of=as_of)


@pytest.mark.parametrize("as_of", _EXTREME_OFFSET_TIMES)
def test_estimate_revalidation_extreme_offset_raises_typed_error(as_of: datetime) -> None:
    unvalidated = LabEtaInput.model_construct(
        job_id=UUID(int=999),
        status="running",
        as_of=as_of,
        completed=(),
        remaining=(),
    )

    with pytest.raises(ValidationError, match="outside the UTC datetime domain"):
        estimate_lab_eta(unvalidated)


@pytest.mark.parametrize("value", _EXTREME_OFFSET_TIMES)
def test_finish_window_extreme_offset_raises_typed_validation_error(value: datetime) -> None:
    with pytest.raises(ValidationError, match="outside the UTC datetime domain"):
        LabEtaFinishWindow(low=value, center=value, high=value)


@pytest.mark.parametrize("as_of", _EXTREME_OFFSET_TIMES)
def test_eta_reader_rejects_extreme_offset_before_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    as_of: datetime,
) -> None:
    reader = LabJobReader(tmp_path / "lab_jobs.sqlite3")

    def fail_if_sql_is_opened() -> None:
        raise AssertionError("UTC normalization must happen before SQL")

    monkeypatch.setattr(reader, "_connect", fail_if_sql_is_opened)

    with pytest.raises(ValueError, match="outside the UTC datetime domain"):
        reader.get_eta_input(UUID(int=1), as_of=as_of)


@pytest.mark.parametrize(("value", "expected"), _NEAR_BOUND_OFFSET_TIMES)
def test_near_bound_offsets_normalize_across_eta_models_and_reader(
    tmp_path: Path,
    value: datetime,
    expected: datetime,
) -> None:
    eta_input = _input(status="paused", remaining=(), as_of=value)
    estimate = estimate_lab_eta(eta_input)
    finish = LabEtaFinishWindow(low=value, center=value, high=value)
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    projection = LabJobReader(store.path).get_eta_input(job.job_id, as_of=value)

    assert eta_input.as_of == expected
    assert estimate.as_of == expected
    assert finish.low == finish.center == finish.high == expected
    assert projection is not None
    assert projection.as_of == expected


def _insert_completed_eta_shards(store: LabJobStore, job_id: UUID, count: int) -> None:
    timestamp = AS_OF.isoformat(timespec="microseconds")
    rows = tuple(
        (
            str(UUID(int=index + 1)),
            str(job_id),
            index,
            "succeeded",
            1,
            1,
            3,
            "4" * 64,
            "history-fixture",
            "v1",
            "{}",
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "history",
            "item",
            1,
            1_000,
            1_000.0,
            1.0,
            index + 1,
            "6" * 64,
            timestamp,
            timestamp,
            timestamp,
        )
        for index in range(count)
    )
    with sqlite3.connect(store.path) as connection:
        connection.executemany(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, plan_hash, adapter_id,
                adapter_version, payload_json, payload_hash,
                phase, work_unit_name, work_units, static_duration_ms,
                duration_ms, throughput_units_per_second, completion_sequence,
                result_manifest_hash, finished_at, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            rows,
        )


def _insert_queued_eta_shard(store: LabJobStore, job_id: UUID, index: int) -> None:
    timestamp = AS_OF.isoformat(timespec="microseconds")
    with sqlite3.connect(store.path, timeout=5) as connection:
        connection.execute(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'queued', 0, 0, 3, ?, ?)
            """,
            (
                str(UUID(int=100_000 + index)),
                str(job_id),
                index,
                timestamp,
                timestamp,
            ),
        )


class _ProbeBarrierCursor:
    def __init__(
        self, cursor: sqlite3.Cursor, *, release_writer: Event, writer_done: Event
    ) -> None:
        self._cursor = cursor
        self._release_writer = release_writer
        self._writer_done = writer_done

    def fetchall(self) -> list[sqlite3.Row]:
        rows = self._cursor.fetchall()
        self._release_writer.set()
        assert self._writer_done.wait(timeout=5), "concurrent writer did not commit"
        return rows


class _ProbeBarrierConnection:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        release_writer: Event,
        writer_done: Event,
    ) -> None:
        self._connection = connection
        self._release_writer = release_writer
        self._writer_done = writer_done

    @property
    def in_transaction(self) -> bool:
        return self._connection.in_transaction

    def __enter__(self) -> _ProbeBarrierConnection:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        return False

    def execute(self, statement: str, parameters: Any = ()) -> sqlite3.Cursor | _ProbeBarrierCursor:
        cursor = self._connection.execute(statement, parameters)
        if "SELECT 1 FROM lab_shard" in " ".join(statement.split()):
            return _ProbeBarrierCursor(
                cursor,
                release_writer=self._release_writer,
                writer_done=self._writer_done,
            )
        return cursor

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class _FailingEtaConnection:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.calls: list[str] = []

    @property
    def in_transaction(self) -> bool:
        return self._connection.in_transaction

    def __enter__(self) -> _FailingEtaConnection:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        return False

    def execute(self, statement: str, parameters: Any = ()) -> sqlite3.Cursor:
        normalized = " ".join(statement.split())
        if normalized == "BEGIN":
            self.calls.append("begin")
        if "completion_sequence FROM lab_shard" in normalized:
            self.calls.append("fault")
            raise KeyboardInterrupt("eta sample interrupted")
        return self._connection.execute(statement, parameters)

    def rollback(self) -> None:
        self.calls.append("rollback")
        self._connection.rollback()

    def close(self) -> None:
        self.calls.append("close")
        self._connection.close()


def test_eta_reader_uses_one_wal_snapshot_across_concurrent_shard_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    _insert_completed_eta_shards(store, job.job_id, MAX_JOB_SHARDS - 1)
    _insert_queued_eta_shard(store, job.job_id, MAX_JOB_SHARDS - 1)
    reader = LabJobReader(store.path)
    original_connect = reader._connect
    release_writer = Event()
    writer_done = Event()

    def connect_with_probe_barrier() -> _ProbeBarrierConnection:
        return _ProbeBarrierConnection(
            original_connect(),
            release_writer=release_writer,
            writer_done=writer_done,
        )

    def insert_after_probe() -> None:
        assert release_writer.wait(timeout=5), "reader did not finish the shard probe"
        try:
            _insert_queued_eta_shard(store, job.job_id, MAX_JOB_SHARDS)
        finally:
            writer_done.set()

    monkeypatch.setattr(reader, "_connect", connect_with_probe_barrier)
    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = executor.submit(insert_after_probe)
        projection = reader.get_eta_input(job.job_id, as_of=AS_OF)
        writer.result(timeout=5)

    assert projection is not None
    assert len(projection.completed) + len(projection.remaining) == MAX_JOB_SHARDS
    with sqlite3.connect(store.path) as connection:
        persisted_count = connection.execute(
            "SELECT COUNT(*) FROM lab_shard WHERE job_id = ?", (str(job.job_id),)
        ).fetchone()
    assert persisted_count is not None and persisted_count[0] == MAX_JOB_SHARDS + 1


def test_eta_reader_rolls_back_and_closes_on_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    raw_connection = LabJobReader(store.path)._connect()
    connection = _FailingEtaConnection(raw_connection)
    reader = LabJobReader(store.path)
    monkeypatch.setattr(reader, "_connect", lambda: connection)

    with pytest.raises(KeyboardInterrupt, match="eta sample interrupted"):
        reader.get_eta_input(job.job_id, as_of=AS_OF)

    assert connection.calls == ["begin", "fault", "rollback", "close"]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        raw_connection.execute("SELECT 1")


def test_eta_reader_rejects_10k_completed_graph_before_sampling(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    _insert_completed_eta_shards(store, job.job_id, 10_000)
    reader = LabJobReader(store.path)
    statements: list[str] = []
    original_connect = reader._connect

    def traced_connect():  # type: ignore[no-untyped-def]
        connection = original_connect()
        connection.set_trace_callback(
            lambda statement: statements.append(" ".join(statement.split()))
        )
        return connection

    reader._connect = traced_connect  # type: ignore[method-assign]

    with pytest.raises(InvalidStoredJobError, match="shard count"):
        reader.get_eta_input(job.job_id, as_of=AS_OF, completed_limit=128)

    assert any(
        "FROM lab_shard" in statement and "LIMIT 129" in statement for statement in statements
    )
    assert not any("completion_sequence FROM lab_shard" in statement for statement in statements)


def test_eta_reader_bounds_valid_completed_history_and_uses_completion_index(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    _insert_completed_eta_shards(store, job.job_id, MAX_JOB_SHARDS)
    with sqlite3.connect(store.path) as connection:
        query_plan = tuple(
            str(row[3])
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT shard_id, phase, work_unit_name, work_units,
                       static_duration_ms, duration_ms,
                       throughput_units_per_second, completion_sequence
                FROM lab_shard
                WHERE job_id = ? AND status = 'succeeded'
                  AND completion_sequence IS NOT NULL
                ORDER BY completion_sequence DESC
                LIMIT ?
                """,
                (str(job.job_id), 128),
            )
        )

    projection = LabJobReader(store.path).get_eta_input(
        job.job_id,
        as_of=AS_OF,
        completed_limit=128,
    )

    assert projection is not None
    assert len(projection.completed) == MAX_JOB_SHARDS
    assert projection.completed[0].completion_sequence == 1
    assert projection.completed[-1].completion_sequence == MAX_JOB_SHARDS
    assert any("ix_lab_shard_job_completion_sequence" in step for step in query_plan)


def _seed_eta_job_with_terminal_shards(
    tmp_path: Path,
    *,
    job_status: str,
) -> tuple[LabJobReader, UUID]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    target = JobStatus(job_status)
    if target is JobStatus.CANCELLED:
        job = store.transition_job(
            job.job_id,
            expected_version=job.version,
            target_status=target,
            lease=lease,
            reason="eta terminal fixture",
            now=job.created_at,
        )
    else:
        job = store.transition_job(
            job.job_id,
            expected_version=job.version,
            target_status=JobStatus.RUNNING,
            lease=lease,
            reason="eta running fixture",
            now=job.created_at,
        )
        if target is not JobStatus.RUNNING:
            job = store.transition_job(
                job.job_id,
                expected_version=job.version,
                target_status=target,
                lease=lease,
                reason="eta terminal fixture",
                now=job.updated_at,
                recoverable=False if target is JobStatus.FAILED else None,
            )
    timestamp = AS_OF.isoformat(timespec="microseconds")
    shard_rows = tuple(
        (
            str(UUID(int=20_000 + index)),
            str(job.job_id),
            index,
            shard_status,
            "terminal-filter-fixture",
            static_duration_ms,
            timestamp,
            timestamp,
        )
        for index, (shard_status, static_duration_ms) in enumerate(
            (("queued", 1_000), ("failed", 50_000), ("cancelled", 60_000))
        )
    )
    with sqlite3.connect(store.path) as connection:
        connection.executemany(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, plan_hash, adapter_id,
                adapter_version, payload_json, payload_hash,
                phase, work_unit_name, work_units, static_duration_ms,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, 0, 0, 3, ?, 'eta-fixture', 'v1', '{}',
                '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
                'scan', 'trading_day', 1, ?, ?, ?
            )
            """,
            shard_rows,
        )
    return LabJobReader(store.path), job.job_id


@pytest.mark.parametrize("job_status", ["running", "checkpointed"])
def test_running_and_checkpointed_eta_ignore_terminal_shards_without_paused_prediction(
    tmp_path: Path,
    job_status: str,
) -> None:
    reader, job_id = _seed_eta_job_with_terminal_shards(
        tmp_path,
        job_status=job_status,
    )

    projection = reader.get_eta_input(job_id, as_of=AS_OF)
    estimate = reader.estimate_eta(job_id, as_of=AS_OF)

    assert projection is not None
    assert tuple(item.shard_id for item in projection.remaining) == (UUID(int=20_000),)
    assert estimate is not None
    assert estimate.remaining_shards == 1
    if job_status == "running":
        assert estimate.remaining_duration is not None
        assert estimate.remaining_duration.center_ms == 1_000
    else:
        assert estimate.estimator == "unavailable"
        assert estimate.remaining_duration is None
        assert estimate.finish_at is None


def test_active_pause_hides_eta_for_running_and_queued_shards_until_withdrawn(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2, with_work_plan=True)
    _claim(store, lease)
    reader = LabJobReader(store.path)
    running = reader.get_job(job_id)
    assert running is not None
    assert {shard.status for shard in reader.list_shards(job_id)} == {
        ShardStatus.QUEUED,
        ShardStatus.RUNNING,
    }

    pause = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=PauseJobCommand(
                job_id=job_id,
                expected_version=running.version,
                reason="pause active job",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    paused = reader.get_job(job_id)
    assert pause.status == "applied"
    assert paused is not None
    assert paused.status is JobStatus.RUNNING
    assert paused.control_intent is ControlIntent.PAUSE_REQUESTED

    projection = reader.get_eta_input(job_id, as_of=AS_OF)
    estimate = reader.estimate_eta(job_id, as_of=AS_OF)
    detail = reader.get_job_detail(job_id, as_of=AS_OF)

    assert projection is not None
    assert projection.status == "paused"
    assert len(projection.remaining) == 2
    assert estimate is not None
    assert estimate.status == "paused"
    assert estimate.estimator == "unavailable"
    assert estimate.remaining_shards == 2
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None
    assert detail is not None
    assert detail.progress.total_shards == 2
    assert detail.progress.terminal_shards == 0
    assert detail.eta == estimate

    resume = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=paused.version,
                reason="withdraw pause",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    resumed = reader.get_job(job_id)
    assert resume.status == "applied"
    assert resume.reason == "pause_withdrawn"
    assert resumed is not None
    assert resumed.status is JobStatus.RUNNING
    assert resumed.control_intent is ControlIntent.NONE

    restored_projection = reader.get_eta_input(job_id, as_of=AS_OF)
    restored_estimate = reader.estimate_eta(job_id, as_of=AS_OF)
    restored_detail = reader.get_job_detail(job_id, as_of=AS_OF)
    assert restored_projection is not None
    assert restored_projection.status == "running"
    assert restored_estimate is not None
    assert restored_estimate.status == "running"
    assert restored_estimate.remaining_duration is not None
    assert restored_estimate.finish_at is not None
    assert restored_detail is not None
    assert restored_detail.eta == restored_estimate


def test_checkpointed_job_has_no_eta_prediction_at_idle_boundary(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    first = _claim(store, lease)
    success = store.apply_worker_report(
        _report(first, LabShardSucceeded(result_manifest_hash="6" * 64), offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    reader = LabJobReader(store.path)
    running = reader.get_job(job_id)
    assert success.status == "accepted"
    assert running is not None
    assert running.status is JobStatus.RUNNING
    pause = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=PauseJobCommand(
                job_id=job_id,
                expected_version=running.version,
                reason="pause at idle boundary",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    projection = reader.get_eta_input(job_id, as_of=AS_OF)
    estimate = reader.estimate_eta(job_id, as_of=AS_OF)
    detail = reader.get_job_detail(job_id, as_of=AS_OF)

    assert pause.status == "applied"
    assert pause.reason == "checkpointed"
    assert projection is not None
    assert projection.status == "checkpointed"
    assert len(projection.remaining) == 1
    assert estimate is not None
    assert estimate.estimator == "unavailable"
    assert estimate.remaining_shards == 1
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None
    assert detail is not None
    assert detail.progress.total_shards == 2
    assert detail.progress.terminal_shards == 1
    assert detail.eta == estimate


def test_cancel_intent_does_not_masquerade_as_paused_eta(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=1, with_work_plan=True)
    _claim(store, lease)
    reader = LabJobReader(store.path)
    running = reader.get_job(job_id)
    assert running is not None
    cancel = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=job_id,
                expected_version=running.version,
                reason="cancel active job",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    requested = reader.get_job(job_id)
    assert cancel.status == "applied"
    assert cancel.reason == "cancel_requested"
    assert requested is not None
    assert requested.status is JobStatus.RUNNING
    assert requested.control_intent is ControlIntent.CANCEL_REQUESTED

    projection = reader.get_eta_input(job_id, as_of=AS_OF)
    estimate = reader.estimate_eta(job_id, as_of=AS_OF)
    detail = reader.get_job_detail(job_id, as_of=AS_OF)

    assert projection is not None
    assert projection.status == "running"
    assert estimate is not None
    assert estimate.estimator == "static"
    assert estimate.remaining_duration is not None
    assert estimate.finish_at is not None
    assert detail is not None
    assert detail.eta == estimate


@pytest.mark.parametrize("job_status", ["failed", "cancelled"])
def test_terminal_job_eta_is_unavailable_regardless_of_shard_rows(
    tmp_path: Path,
    job_status: str,
) -> None:
    reader, job_id = _seed_eta_job_with_terminal_shards(
        tmp_path,
        job_status=job_status,
    )

    estimate = reader.estimate_eta(job_id, as_of=AS_OF)

    assert estimate is not None
    assert estimate.estimator == "unavailable"
    assert estimate.remaining_duration is None
    assert estimate.finish_at is None


@pytest.mark.parametrize("completed_limit", [2, 257, 10_000])
def test_eta_reader_rejects_completed_limit_outside_hard_bounds_before_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    completed_limit: int,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    reader = LabJobReader(store.path)

    def fail_if_sql_is_opened() -> None:
        raise AssertionError("completed_limit validation must happen before SQL")

    monkeypatch.setattr(reader, "_connect", fail_if_sql_is_opened)

    with pytest.raises(ValueError, match="between 3 and 256"):
        reader.get_eta_input(UUID(int=1), as_of=AS_OF, completed_limit=completed_limit)


@pytest.mark.parametrize("completed_limit", [3, 256])
def test_eta_reader_accepts_completed_limit_boundaries(
    tmp_path: Path,
    completed_limit: int,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)

    projection = LabJobReader(store.path).get_eta_input(
        job.job_id,
        as_of=AS_OF,
        completed_limit=completed_limit,
    )

    assert projection is not None


def test_eta_huge_static_duration_raises_typed_projection_error() -> None:
    with pytest.raises(LabEtaProjectionError):
        estimate_lab_eta(
            _input(remaining=(_remaining(1, work_units=1, static_duration_ms=10**18),))
        )


def test_eta_multiple_legal_shards_with_overflowing_total_raise_projection_error() -> None:
    static_duration_ms = 150_000_000_000_000
    with pytest.raises(LabEtaProjectionError):
        estimate_lab_eta(
            _input(
                as_of=datetime.min.replace(tzinfo=UTC),
                remaining=(
                    _remaining(1, work_units=1, static_duration_ms=static_duration_ms),
                    _remaining(2, work_units=1, static_duration_ms=static_duration_ms),
                ),
            )
        )


def test_eta_near_datetime_max_raises_projection_error_without_raw_overflow() -> None:
    with pytest.raises(LabEtaProjectionError):
        estimate_lab_eta(
            _input(
                as_of=datetime.max.replace(tzinfo=UTC),
                remaining=(_remaining(1, work_units=1, static_duration_ms=1),),
            )
        )


def test_eta_near_datetime_min_projects_small_duration() -> None:
    estimate = estimate_lab_eta(
        _input(
            as_of=datetime.min.replace(tzinfo=UTC),
            remaining=(_remaining(1, work_units=1, static_duration_ms=1),),
        )
    )

    assert estimate.finish_at is not None
    assert estimate.finish_at.center == datetime.min.replace(tzinfo=UTC) + timedelta(milliseconds=1)
