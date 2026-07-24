from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.lab_eta import (
    LabEtaCompletedShard,
    LabEtaInput,
    LabEtaRemainingShard,
    estimate_lab_eta,
)
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_shard_protocol import LabShardTelemetry, LabShardWorkPlan

from .test_lab_jobs import _lease, _submit_job

AS_OF = datetime(2026, 7, 24, 4, 0, tzinfo=UTC)


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


@pytest.mark.parametrize("status", ["queued", "running", "checkpointed"])
def test_active_eta_projects_finish_window_from_explicit_as_of(status: str) -> None:
    estimate = estimate_lab_eta(_input(status=status))

    assert estimate.finish_at is not None
    assert estimate.finish_at.center == AS_OF + timedelta(seconds=1)
    assert estimate.finish_at.low == AS_OF + timedelta(milliseconds=750)
    assert estimate.finish_at.high == AS_OF + timedelta(milliseconds=1_500)


def test_paused_eta_keeps_duration_but_has_no_finish_time() -> None:
    estimate = estimate_lab_eta(_input(status="paused"))

    assert estimate.remaining_duration is not None
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

    offset = timezone(timedelta(hours=8))
    estimate = estimate_lab_eta(_input(as_of=datetime(2026, 7, 24, 12, 0, tzinfo=offset)))

    assert estimate.as_of == AS_OF
    assert estimate.finish_at is not None
    assert estimate.finish_at.center.tzinfo is UTC


def test_eta_reader_bounds_10k_completed_history_and_uses_completion_index(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    timestamp = AS_OF.isoformat(timespec="microseconds")
    rows = tuple(
        (
            str(UUID(int=index + 1)),
            str(job.job_id),
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
        for index in range(10_000)
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
        remaining_query_plan = tuple(
            str(row[3])
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT shard_id, phase, work_unit_name, work_units,
                       static_duration_ms
                FROM lab_shard INDEXED BY ix_lab_shard_job_status_index
                WHERE job_id = ?
                  AND status IN ('queued', 'running', 'checkpointed', 'failed', 'cancelled')
                ORDER BY shard_index, shard_id
                """,
                (str(job.job_id),),
            )
        )
        sequence_query_plan = tuple(
            str(row[3])
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT MAX(completion_sequence) FROM lab_shard
                WHERE job_id = ? AND status = 'succeeded'
                  AND completion_sequence IS NOT NULL
                """,
                (str(job.job_id),),
            )
        )

    projection = LabJobReader(store.path).get_eta_input(
        job.job_id,
        as_of=AS_OF,
        completed_limit=128,
    )

    assert projection is not None
    assert len(projection.completed) == 128
    assert projection.completed[0].completion_sequence == 9_873
    assert projection.completed[-1].completion_sequence == 10_000
    assert any("ix_lab_shard_job_completion_sequence" in step for step in query_plan)
    assert any("ix_lab_shard_job_status_index" in step for step in remaining_query_plan)
    assert any("ix_lab_shard_job_completion_sequence" in step for step in sequence_query_plan)
