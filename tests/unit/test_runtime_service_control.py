from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest

from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError
from rquant.runtime_service_control import (
    RuntimeServiceAlreadyRunningError,
    RuntimeServiceControl,
    RuntimeServiceHealth,
    RuntimeServiceHeartbeat,
    RuntimeServiceHeartbeatProjection,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
    RuntimeStepResult,
    inspect_runtime_health,
    project_heartbeat,
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


# ---------------------------------------------------------------------------------------
# How long a service has been waiting on one peer artifact (#231, #232, #220)
# ---------------------------------------------------------------------------------------


def _waiting(path: str) -> PeerArtifactUnavailableError:
    return PeerArtifactUnavailableError(
        reader="signal_router",
        artifact="runner source",
        path=Path(path),
    )


def test_a_wait_on_one_peer_artifact_is_timed_from_the_iteration_that_named_it(
    tmp_path: Path,
) -> None:
    """The live plane deliberately has no failure threshold any more, so it needs a clock.

    A role whose peer never starts now stays alive and DEGRADED for as long as that
    lasts, which is the point -- an absent producer stopped taking the process down and
    firing `OnFailure`. The cost is that "waiting" and "wedged" look identical on a
    dashboard, and `consecutive_failures` cannot tell them apart either once a service
    has waited on two different peers in one run. These three fields can.
    """

    moment = [NOW]
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: moment[0])
    control.start()

    first = control.record_failure(_waiting("/runtime/live/strategies/svc-1/runner.sqlite3"))
    assert first.waiting_for == "/runtime/live/strategies/svc-1/runner.sqlite3"
    assert first.waiting_since == NOW
    assert first.waited_seconds == 0.0

    moment[0] = NOW + timedelta(minutes=7)
    same = control.record_failure(_waiting("/runtime/live/strategies/svc-1/runner.sqlite3"))
    assert same.waiting_since == NOW
    assert same.waited_seconds == 420.0
    assert same.consecutive_failures == 2

    control.stop(reason="test complete")


def test_waiting_on_a_different_artifact_or_failing_otherwise_restarts_the_clock(
    tmp_path: Path,
) -> None:
    moment = [NOW]
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: moment[0])
    control.start()
    control.record_failure(_waiting("/runtime/live/strategies/svc-1/runner.sqlite3"))

    moment[0] = NOW + timedelta(minutes=3)
    moved = control.record_failure(_waiting("/runtime/live/signal-bus/signal_bus.sqlite3"))
    assert moved.waiting_for == "/runtime/live/signal-bus/signal_bus.sqlite3"
    assert moved.waiting_since == moment[0]
    assert moved.waited_seconds == 0.0

    #: a failure that is not a wait says nothing about how long a wait has lasted
    other = control.record_failure(RuntimeError("provider unavailable"))
    assert other.waiting_for is None
    assert other.waiting_since is None
    assert other.waited_seconds is None

    control.stop(reason="test complete")


def test_one_successful_iteration_clears_the_wait(tmp_path: Path) -> None:
    moment = [NOW]
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: moment[0])
    control.start()
    control.record_failure(_waiting("/runtime/live/strategies/svc-1/runner.sqlite3"))

    moment[0] = NOW + timedelta(minutes=1)
    running = control.record_success(RuntimeStepResult())

    assert running.status is RuntimeServiceStatus.RUNNING
    assert running.waiting_for is None
    assert running.waiting_since is None
    assert running.waited_seconds is None

    control.stop(reason="test complete")


def test_a_stopped_service_keeps_the_wait_and_the_seconds_stay_consistent(
    tmp_path: Path,
) -> None:
    """`stop()` moves `heartbeat_at`, and the two fields have to move with it."""

    moment = [NOW]
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: moment[0])
    control.start()
    control.record_failure(_waiting("/runtime/live/strategies/svc-1/runner.sqlite3"))

    moment[0] = NOW + timedelta(minutes=2)
    stopped = control.stop(reason="loop completed")

    assert stopped.waiting_for == "/runtime/live/strategies/svc-1/runner.sqlite3"
    assert stopped.waiting_since == NOW
    assert stopped.waited_seconds == 120.0


def test_the_three_waiting_fields_are_published_as_one_group(tmp_path: Path) -> None:
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    published = control.start()
    control.stop(reason="test complete")
    payload = published.model_dump(mode="json")

    for partial in (
        {"waiting_for": "/runtime/live/strategies/svc-1/runner.sqlite3"},
        {"waiting_since": NOW.isoformat().replace("+00:00", "Z")},
        {"waited_seconds": 3.0},
    ):
        with pytest.raises(ValueError, match="one group"):
            RuntimeServiceHeartbeat.model_validate({**payload, **partial})

    with pytest.raises(ValueError, match="waited_seconds must equal"):
        RuntimeServiceHeartbeat.model_validate(
            {
                **payload,
                "waiting_for": "/runtime/live/strategies/svc-1/runner.sqlite3",
                "waiting_since": NOW.isoformat().replace("+00:00", "Z"),
                "waited_seconds": 3.0,
            }
        )


#: The field set `runtime.serving.runtime-health` published in v0.33.1, in order. The
#: release snapshot gate hashes it; this list says the same thing where a reader of the
#: model will look. Adding a name here without bumping the channel's schema version and
#: refreshing the snapshot is #237 happening again.
PUBLISHED_HEARTBEAT_FIELDS = (
    "service_id",
    "spec_fingerprint",
    "run_id",
    "generation",
    "status",
    "started_at",
    "heartbeat_at",
    "last_success_at",
    "stopped_at",
    "input_sequence",
    "output_sequence",
    "processed_count",
    "backlog_count",
    "consecutive_failures",
    "total_failures",
    "total_successes",
    "last_step_duration_seconds",
    "p95_step_duration_seconds",
    "recent_step_durations_seconds",
    "source_generations",
    "degraded_reasons",
    "last_error",
    "stop_reason",
)


def _waiting_heartbeat() -> RuntimeServiceHeartbeat:
    """Every field populated, including the three the serving payload must not carry."""

    return RuntimeServiceHeartbeat(
        service_id="feature-live",
        spec_fingerprint="b" * 64,
        run_id="c" * 64,
        generation=3,
        status=RuntimeServiceStatus.DEGRADED,
        started_at=NOW - timedelta(minutes=5),
        heartbeat_at=NOW,
        last_success_at=NOW - timedelta(minutes=1),
        input_sequence=11,
        output_sequence=9,
        processed_count=7,
        backlog_count=2,
        consecutive_failures=4,
        total_failures=6,
        total_successes=8,
        last_step_duration_seconds=0.5,
        p95_step_duration_seconds=0.5,
        recent_step_durations_seconds=(0.1, 0.5),
        source_generations={"upstream": "d" * 64},
        degraded_reasons=("peer-artifact-missing",),
        last_error="PeerArtifactUnavailableError: runner.sqlite3",
        waiting_for="/runtime/live/strategies/svc-1/runner.sqlite3",
        waiting_since=NOW - timedelta(seconds=90),
        waited_seconds=90.0,
    )


def test_the_serving_projection_declares_the_published_field_set() -> None:
    assert tuple(RuntimeServiceHeartbeatProjection.model_fields) == PUBLISHED_HEARTBEAT_FIELDS


def test_projecting_a_heartbeat_carries_every_published_value_and_drops_the_rest() -> None:
    heartbeat = _waiting_heartbeat()

    projected = project_heartbeat(heartbeat)

    assert projected is not None
    for name in PUBLISHED_HEARTBEAT_FIELDS:
        assert getattr(projected, name) == getattr(heartbeat, name), name
    dumped = projected.model_dump(mode="json")
    assert set(dumped) == set(PUBLISHED_HEARTBEAT_FIELDS)
    assert not {"waiting_for", "waiting_since", "waited_seconds"} & set(dumped)


def test_projecting_nothing_stays_nothing() -> None:
    assert project_heartbeat(None) is None


def test_serving_health_refuses_the_heartbeat_file_model() -> None:
    with pytest.raises(ValueError, match="project_heartbeat"):
        RuntimeServiceHealth(
            service_id="feature-live",
            plane=RuntimeServicePlane.LIVE,
            status=RuntimeServiceStatus.DEGRADED,
            stale=False,
            observed_at=NOW,
            heartbeat=_waiting_heartbeat(),
        )


def test_health_reader_publishes_the_projection_not_the_file_model(tmp_path: Path) -> None:
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    control.record_failure(_waiting(str(tmp_path / "runner.sqlite3")))

    health = inspect_runtime_health(
        tmp_path,
        specs=(control.spec,),
        observed_at=NOW + timedelta(seconds=1),
    )
    control.stop(reason="test complete")

    published = health[0].heartbeat
    assert isinstance(published, RuntimeServiceHeartbeatProjection)
    assert not isinstance(published, RuntimeServiceHeartbeat)
    on_disk = RuntimeServiceControl.read_heartbeat(tmp_path, control.spec)
    assert on_disk is not None
    assert on_disk.waiting_for is not None
