from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest

from rquant.runtime_service_control import (
    RuntimeServiceAlreadyRunningError,
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
    RuntimeStepResult,
    inspect_runtime_health,
    run_service_loop,
)

NOW = datetime(2026, 7, 31, 2, 0, tzinfo=UTC)


def _spec(service_id: str = "feature-live") -> RuntimeServiceSpec:
    return RuntimeServiceSpec(
        service_id=service_id,
        plane=RuntimeServicePlane.LIVE,
        stale_after=timedelta(seconds=10),
        producer_commit="a" * 40,
    )


def test_singleton_authority_and_restart_generation_are_persistent(tmp_path: Path) -> None:
    first = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    second = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)

    started = first.start()
    with pytest.raises(RuntimeServiceAlreadyRunningError):
        second.start()

    first.stop(reason="planned restart")
    restarted = second.start()

    assert restarted.generation == started.generation + 1
    assert restarted.run_id != started.run_id
    assert restarted.status is RuntimeServiceStatus.STARTING
    second.stop(reason="test complete")


def test_success_and_failure_heartbeats_preserve_monotonic_watermarks(tmp_path: Path) -> None:
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    running = control.record_success(
        RuntimeStepResult(
            input_sequence=4,
            output_sequence=3,
            processed_count=12,
            backlog_count=1,
            source_generations={"market-minute": "b" * 64},
        )
    )
    degraded = control.record_failure(RuntimeError("provider unavailable"))

    assert running.status is RuntimeServiceStatus.RUNNING
    assert running.input_sequence == 4
    assert running.output_sequence == 3
    assert degraded.status is RuntimeServiceStatus.DEGRADED
    assert degraded.consecutive_failures == 1
    assert degraded.last_error == "RuntimeError: provider unavailable"
    assert degraded.last_success_at == NOW
    with pytest.raises(ValueError, match="regress"):
        control.record_success(RuntimeStepResult(input_sequence=3, output_sequence=3))
    control.stop(reason="test complete")


def test_step_latency_window_is_bounded_and_persists_nearest_rank_p95(
    tmp_path: Path,
) -> None:
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()

    for duration in range(1, 26):
        heartbeat = control.record_success(
            RuntimeStepResult(),
            duration_seconds=float(duration),
        )

    assert heartbeat.last_step_duration_seconds == 25.0
    assert heartbeat.recent_step_durations_seconds == tuple(
        float(duration) for duration in range(6, 26)
    )
    assert heartbeat.p95_step_duration_seconds == 24.0
    persisted = RuntimeServiceControl.read_heartbeat(tmp_path, _spec())
    assert persisted == heartbeat
    control.stop(reason="test complete")


def test_successful_degraded_step_keeps_watermarks_and_health_reason(tmp_path: Path) -> None:
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()

    degraded = control.record_success(
        RuntimeStepResult(
            input_sequence=3,
            output_sequence=4,
            processed_count=1,
            degraded_reasons=("source_stale:TimeoutError",),
        )
    )
    recovered = control.record_success(
        RuntimeStepResult(input_sequence=4, output_sequence=5, processed_count=1)
    )

    assert degraded.status is RuntimeServiceStatus.DEGRADED
    assert degraded.degraded_reasons == ("source_stale:TimeoutError",)
    assert degraded.input_sequence == 3
    assert degraded.output_sequence == 4
    assert degraded.total_successes == 1
    assert degraded.total_failures == 0
    assert recovered.status is RuntimeServiceStatus.RUNNING
    assert recovered.degraded_reasons == ()
    control.stop(reason="test complete")


def test_loop_isolates_ordinary_step_failure_and_recovers(tmp_path: Path) -> None:
    ticks = iter(
        (
            RuntimeError("temporary"),
            RuntimeStepResult(input_sequence=1, output_sequence=1, processed_count=1),
        )
    )
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)

    def step() -> RuntimeStepResult:
        outcome = next(ticks)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    final = run_service_loop(
        control,
        step=step,
        stop_event=Event(),
        interval_seconds=0,
        max_iterations=2,
    )

    assert final.status is RuntimeServiceStatus.STOPPED
    assert final.consecutive_failures == 0
    assert final.total_failures == 1
    assert final.total_successes == 1
    assert final.input_sequence == 1


def test_loop_measures_success_and_failure_step_latency(tmp_path: Path) -> None:
    ticks = iter((RuntimeError("temporary"), RuntimeStepResult()))
    monotonic_ticks = iter((10.0, 10.1, 20.0, 20.4))
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)

    def step() -> RuntimeStepResult:
        outcome = next(ticks)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    final = run_service_loop(
        control,
        step=step,
        stop_event=Event(),
        interval_seconds=0,
        max_iterations=2,
        monotonic_clock=lambda: next(monotonic_ticks),
    )

    assert final.recent_step_durations_seconds == pytest.approx((0.1, 0.4))
    assert final.last_step_duration_seconds == pytest.approx(0.4)
    assert final.p95_step_duration_seconds == pytest.approx(0.4)


def test_health_reader_marks_stale_without_writing_service_state(tmp_path: Path) -> None:
    feature = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    notifier = RuntimeServiceControl(
        tmp_path,
        spec=_spec("notifier").model_copy(update={"plane": RuntimeServicePlane.SERVING}),
        clock=lambda: NOW,
    )
    feature.start()
    feature.record_success(RuntimeStepResult(input_sequence=2, output_sequence=2))
    notifier.start()
    notifier.record_failure(TimeoutError("push timeout"))

    health = inspect_runtime_health(
        tmp_path,
        specs=(feature.spec, notifier.spec),
        observed_at=NOW + timedelta(seconds=11),
    )

    assert {item.service_id for item in health} == {"feature-live", "notifier"}
    assert all(item.stale for item in health)
    assert next(item for item in health if item.service_id == "notifier").status is (
        RuntimeServiceStatus.DEGRADED
    )
    feature.stop(reason="test complete")
    notifier.stop(reason="test complete")


def test_base_exception_escapes_loop_after_stopped_heartbeat(tmp_path: Path) -> None:
    class SimulatedCrash(BaseException):
        pass

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)

    with pytest.raises(SimulatedCrash):
        run_service_loop(
            control,
            step=lambda: (_ for _ in ()).throw(SimulatedCrash()),
            stop_event=Event(),
            interval_seconds=0,
            max_iterations=1,
        )

    heartbeat = RuntimeServiceControl.read_heartbeat(tmp_path, _spec())
    assert heartbeat is not None
    assert heartbeat.status is RuntimeServiceStatus.STOPPED
    assert heartbeat.last_error == "SimulatedCrash"
