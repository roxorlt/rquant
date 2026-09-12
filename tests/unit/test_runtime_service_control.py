from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest

from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError
from rquant.runtime_service_control import (
    MAX_FAILURE_BACKOFF_SECONDS,
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


# ---------------------------------------------------------------------------------------
# #254 follow-up: a DEGRADED loop must be cheap, and a stop must never need SIGKILL
# ---------------------------------------------------------------------------------------


def _record_delays(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Every delay the loop asks to wait, without waiting any of them.

    The loop's own wait is sliced so a stop is never late (see `_wait_for_stop`), so
    counting slices would measure the slicing rather than the backoff. What matters here
    is the delay the loop *chose*.
    """

    from rquant import runtime_service_control as control_module

    delays: list[float] = []

    def record(stop_event: Event, delay: float, **_kwargs: object) -> bool:
        delays.append(delay)
        return stop_event.is_set()

    monkeypatch.setattr(control_module, "_wait_for_stop", record)
    return delays


def _failing_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    error: Callable[[int], Exception],
    iterations: int,
    interval_seconds: float = 2.0,
    max_failure_backoff_seconds: float = MAX_FAILURE_BACKOFF_SECONDS,
) -> tuple[RuntimeServiceHeartbeat, list[float]]:
    delays = _record_delays(monkeypatch)
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    attempt = iter(range(1, iterations + 1))

    def step() -> RuntimeStepResult:
        raise error(next(attempt))

    final = run_service_loop(
        control,
        step=step,
        stop_event=Event(),
        interval_seconds=interval_seconds,
        max_iterations=iterations,
        max_failure_backoff_seconds=max_failure_backoff_seconds,
    )
    return final, delays


def test_the_same_failure_backs_off_instead_of_retrying_every_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#254's second cost: the failing loop itself, not the failure.

    `market-minute.source.v1` and `watchlist-quote.source.v1` re-walked and re-hashed a
    candidate store they could not read every two seconds for a whole morning. A 4-vCPU
    host sat at load 11-12 with ~47% system time, the 15-minute backups went from 8 to 14
    minutes, and the monitor watchdog timed out once -- one more real push. The first
    failure of a kind is still free; from the second it doubles, up to a minute.
    """

    final, delays = _failing_loop(
        tmp_path,
        monkeypatch,
        error=lambda _attempt: RuntimeError("candidate store is damaged"),
        iterations=8,
    )

    #: the first failure waits the plain interval, then 4, 8, 16, and the 20 s cap
    assert delays[:4] == [2.0, 4.0, 8.0, 16.0]
    assert all(delay == MAX_FAILURE_BACKOFF_SECONDS for delay in delays[5:])
    assert final.failure_kind == "builtins.RuntimeError"


def test_a_different_failure_resets_the_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A loop alternating between two faults is not idle and must not be slowed like one."""

    _final, delays = _failing_loop(
        tmp_path,
        monkeypatch,
        error=lambda attempt: (
            RuntimeError("one") if attempt % 2 else ValueError("another")
        ),
        iterations=5,
    )

    assert delays == [2.0, 2.0, 2.0, 2.0]


def test_a_peer_wait_is_never_backed_off(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A role waiting for a peer is not burning the host, and slowing it costs a cold start.

    With `broker -> strategy -> router` no longer an ordered start (#252), the chain's
    worst-case convergence is a sum of these waits. Backing each one off to twenty seconds
    would trade a fixed start order for a slow one. What #254 is about is the other kind
    of failure -- one whose path re-walks and re-hashes a store every iteration.
    """

    _final, delays = _failing_loop(
        tmp_path,
        monkeypatch,
        error=lambda _attempt: PeerArtifactUnavailableError(
            reader="strategy_live",
            artifact="paper broker ledger",
            path=Path("/runtime/live/paper-brokers/svc/broker.sqlite3"),
        ),
        iterations=6,
    )

    assert delays == [2.0, 2.0, 2.0, 2.0, 2.0]


def test_the_backoff_cap_stays_under_the_tightest_stale_after_in_the_profile() -> None:
    """A heartbeat is written once per failure, so the backoff *is* the gap between them.

    A cap above a role's `stale_after` would put it on the health plane as `stale` while
    it is doing exactly what it was told to do -- a second, invented symptom on top of the
    real one. The tightest value in the production profile is read out of the profile
    itself rather than copied here, so raising the cap without looking at it fails.
    """

    import re

    from rquant.runtime_service_control import MAX_FAILURE_BACKOFF_SECONDS

    profile = Path("src/rquant/runtime_production_profile.py").read_text()
    stale_after = [
        float(value.replace("_", ""))
        for value in re.findall(r"stale_after_seconds=([0-9_]+)\b", profile)
    ]
    assert stale_after, "the profile must declare stale_after_seconds"
    assert min(stale_after) > MAX_FAILURE_BACKOFF_SECONDS, (
        min(stale_after),
        MAX_FAILURE_BACKOFF_SECONDS,
    )


def test_two_peers_waited_on_are_two_kinds_even_at_the_same_exception(
    tmp_path: Path,
) -> None:
    """`PeerArtifactUnavailableError` for two different files is two waits, not one."""

    from rquant.runtime_service_control import failure_kind_of

    first = PeerArtifactUnavailableError(
        reader="strategy_live",
        artifact="paper broker ledger",
        path=Path("/runtime/live/paper-brokers/a/broker.sqlite3"),
    )
    second = PeerArtifactUnavailableError(
        reader="strategy_live",
        artifact="paper broker ledger",
        path=Path("/runtime/live/paper-brokers/b/broker.sqlite3"),
    )
    assert failure_kind_of(first) != failure_kind_of(second)
    assert failure_kind_of(first).endswith("/a/broker.sqlite3")


def test_one_success_clears_the_backoff_from_the_heartbeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The field says what this iteration is doing, so a recovery has to erase it."""

    delays = _record_delays(monkeypatch)
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    outcomes: list[object] = [
        RuntimeError("again"),
        RuntimeError("again"),
        RuntimeStepResult(input_sequence=1, output_sequence=1),
    ]
    ticks = iter(outcomes)

    def step() -> RuntimeStepResult:
        outcome = next(ticks)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, RuntimeStepResult)
        return outcome

    final = run_service_loop(
        control,
        step=step,
        stop_event=Event(),
        interval_seconds=2.0,
        max_iterations=3,
    )

    assert delays == [2.0, 4.0]
    assert final.failure_backoff_seconds is None
    assert final.failure_kind is None
    assert final.total_successes == 1


def test_the_backoff_is_a_file_field_and_reaches_no_published_payload() -> None:
    """#237's line: adding to the heartbeat file model must not add to what serving publishes."""

    assert "failure_backoff_seconds" in RuntimeServiceHeartbeat.model_fields
    assert "failure_kind" in RuntimeServiceHeartbeat.model_fields
    assert "failure_backoff_seconds" not in RuntimeServiceHeartbeatProjection.model_fields
    assert "failure_kind" not in RuntimeServiceHeartbeatProjection.model_fields


def test_the_replica_cost_is_a_file_field_and_reaches_no_published_payload() -> None:
    """#237's line again, for the two fields #256 adds."""

    assert "replica_opened" in RuntimeServiceHeartbeat.model_fields
    assert "replica_read_bytes" in RuntimeServiceHeartbeat.model_fields
    assert "replica_opened" not in RuntimeServiceHeartbeatProjection.model_fields
    assert "replica_read_bytes" not in RuntimeServiceHeartbeatProjection.model_fields


def test_a_successful_iteration_carries_what_it_did_with_the_replica(tmp_path: Path) -> None:
    """An iteration that opened the database, then one that recognised the generation."""

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    try:
        opened = control.record_success(
            RuntimeStepResult(replica_opened=True, replica_read_bytes=4096)
        )
        reused = control.record_success(
            RuntimeStepResult(replica_opened=False, replica_read_bytes=0)
        )
    finally:
        control.stop(reason="test complete")

    assert (opened.replica_opened, opened.replica_read_bytes) == (True, 4096)
    assert (reused.replica_opened, reused.replica_read_bytes) == (False, 0)


def test_a_role_that_reads_no_replica_reports_nothing_rather_than_zero(
    tmp_path: Path,
) -> None:
    """Twenty-one of the twenty-five roles never open it; "0 bytes" would be a claim."""

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    try:
        heartbeat = control.record_success(RuntimeStepResult())
    finally:
        control.stop(reason="test complete")

    assert heartbeat.replica_opened is None
    assert heartbeat.replica_read_bytes is None


def test_a_failed_iteration_does_not_keep_the_previous_read_s_numbers(
    tmp_path: Path,
) -> None:
    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    try:
        control.record_success(RuntimeStepResult(replica_opened=True, replica_read_bytes=4096))
        failed = control.record_failure(RuntimeError("the replica moved under the read"))
    finally:
        control.stop(reason="test complete")

    assert failed.replica_opened is None
    assert failed.replica_read_bytes is None


def test_a_failed_iteration_reports_what_it_did_with_the_replica_before_it_raised(
    tmp_path: Path,
) -> None:
    """#260: the iteration that fails is not the iteration that read nothing.

    The notifier publishes its page projection before it touches the serving authority, so
    on every one of the eighth window's failing iterations the replica had been opened and
    read -- and the heartbeat said `null`, which is "cannot say" and was wrong. The role
    that can say hands the loop its gate's own summary, and the same MF-1 rule applies to a
    failed round as to a successful one.
    """

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    try:
        opened = control.record_failure(
            RuntimeError("current pointer producer_commit does not match expected commit"),
            replica_cost=(True, 43_790_567),
        )
        never_asked = control.record_failure(
            RuntimeError("stopped before the read"),
            replica_cost=(False, 0),
        )
    finally:
        control.stop(reason="test complete")

    assert (opened.replica_opened, opened.replica_read_bytes) == (True, 43_790_567)
    assert (never_asked.replica_opened, never_asked.replica_read_bytes) == (False, 0)
    assert opened.last_error is not None
    assert "producer_commit" in opened.last_error


def test_the_loop_takes_the_failed_iteration_s_replica_cost_off_the_step(
    tmp_path: Path,
) -> None:
    """The wiring, through the loop, the way `generation_events` is taken off the step."""

    reported: list[str] = []

    def step() -> RuntimeStepResult:
        reported.append("iteration")
        raise RuntimeError("the serving authority refused")

    step.replica_iteration_summary = lambda: (True, 2048)  # type: ignore[attr-defined]

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    final = run_service_loop(
        control,
        step=step,
        stop_event=Event(),
        interval_seconds=0,
        max_iterations=1,
    )

    assert reported == ["iteration"]
    assert final.total_failures == 1
    assert (final.replica_opened, final.replica_read_bytes) == (True, 2048)


def test_a_step_that_cannot_say_still_reports_neither_on_failure(tmp_path: Path) -> None:
    """Twenty-one roles read no replica, and a probe that raises is not an answer either.

    Both halves are the same rule: the heartbeat's cost is a diagnostic and the failure is
    the news, so anything short of a real summary leaves the two fields `null` rather than
    inventing a zero or replacing the error being recorded.
    """

    def silent() -> RuntimeStepResult:
        raise RuntimeError("this role reads no replica")

    def broken() -> RuntimeStepResult:
        raise RuntimeError("this role reads a replica and failed")

    def malformed() -> RuntimeStepResult:
        raise RuntimeError("this role reads a replica and failed differently")

    broken.replica_iteration_summary = _raising_summary  # type: ignore[attr-defined]
    #: not a pair -- the probe runs inside the loop's own except handler, so a summary
    #: this shape must read as "cannot say" rather than take the loop down with it
    malformed.replica_iteration_summary = lambda: "opened"  # type: ignore[attr-defined]

    for step in (silent, broken, malformed):
        control = RuntimeServiceControl(tmp_path / step.__name__, spec=_spec(), clock=lambda: NOW)
        final = run_service_loop(
            control,
            step=step,
            stop_event=Event(),
            interval_seconds=0,
            max_iterations=1,
        )
        assert final.total_failures == 1, step.__name__
        assert final.replica_opened is None, step.__name__
        assert final.replica_read_bytes is None, step.__name__
        #: the failure itself is recorded unchanged, which is the half that matters
        assert final.last_error is not None
        assert "this role reads" in final.last_error, step.__name__


def _raising_summary() -> tuple[bool, int | None]:
    raise OSError("the gate itself is unusable")


def test_a_role_with_no_replica_still_writes_both_keys_as_null(tmp_path: Path) -> None:
    """Review MF-3: this is why the rollback moves **every** role's heartbeat, not four.

    Heartbeats are serialized with a plain `model_dump(mode="json")` -- no `exclude_none`
    -- so the two fields appear in the file for all 25 roles, as `null` for the 21 that
    never touch the replica. The file model is `extra="forbid"` and `read_heartbeat`
    raises rather than degrades, so a binary from before this package refuses every one of
    those files, not just the four read-side ones. `DEPLOY.md`'s D-2 step moves them all.
    """

    control = RuntimeServiceControl(
        tmp_path, spec=_spec("market-minute.source.v1"), clock=lambda: NOW
    )
    control.start()
    try:
        control.record_success(RuntimeStepResult(processed_count=1))
    finally:
        control.stop(reason="test complete")

    written = next((tmp_path / "heartbeats").glob("*.json"))
    payload = json.loads(written.read_text(encoding="utf-8"))

    assert "replica_opened" in payload
    assert "replica_read_bytes" in payload
    assert payload["replica_opened"] is None
    assert payload["replica_read_bytes"] is None


def test_a_negative_read_is_refused() -> None:
    with pytest.raises(ValueError, match="greater than or equal to 0"):
        RuntimeStepResult(replica_opened=True, replica_read_bytes=-1)


def test_a_long_wait_is_cut_short_by_a_stop_within_one_poll_slice() -> None:
    """The wait itself, at the 60-second cap, stopped from another thread.

    `#254`'s third cost was that stopping `watchlist-quote` inside its failing loop
    exceeded `TimeoutStopSec`, took `SIGKILL`, and left the unit `failed` -- one more real
    push. Slicing the wait bounds the stop latency whether or not `Event.set()` wakes the
    wait: the handler that sets it runs on this very thread, and having to take the
    event's own lock to hand the news over is exactly how a stop ends up late.
    """

    import threading
    import time as real_time

    from rquant.runtime_service_control import MAX_FAILURE_BACKOFF_SECONDS, _wait_for_stop

    stop = Event()
    threading.Timer(0.05, stop.set).start()
    started = real_time.monotonic()
    stopped = _wait_for_stop(
        stop,
        MAX_FAILURE_BACKOFF_SECONDS,
        monotonic_clock=real_time.monotonic,
    )
    elapsed = real_time.monotonic() - started

    assert stopped is True
    assert elapsed < 1.0, elapsed


def test_no_single_wait_blocks_longer_than_the_poll_slice() -> None:
    """The property that bounds the stop latency without depending on the event at all.

    `Event.set()` from another *thread* wakes `Event.wait()` at once, so a test that stops
    the loop that way cannot tell a sliced wait from a single 60-second one. What
    production does is different: the handler runs on the waiting thread, and it has to
    take the event's own lock to hand the news over. So the contract asserted here is the
    one that holds either way -- the loop never blocks longer than one slice without
    looking at the stop event again.
    """

    import time as real_time

    from rquant.runtime_service_control import _STOP_POLL_SECONDS, _wait_for_stop

    class _RecordingEvent(Event):
        def __init__(self) -> None:
            super().__init__()
            self.timeouts: list[float | None] = []

        def wait(self, timeout: float | None = None) -> bool:
            self.timeouts.append(timeout)
            return False

    stop = _RecordingEvent()
    clock = iter([0.0, 0.0, 0.25, 0.5, 0.75, 1.0, 60.0])
    assert _wait_for_stop(stop, 60.0, monotonic_clock=lambda: next(clock)) is False
    assert stop.timeouts, "the wait has to consult the stop event"
    assert all(
        timeout is not None and timeout <= _STOP_POLL_SECONDS for timeout in stop.timeouts
    ), stop.timeouts
    #: and a delay inside one slice is still handed over whole, so a short interval is
    #: driven exactly as it was before
    short = _RecordingEvent()
    assert _wait_for_stop(short, 0.05, monotonic_clock=real_time.monotonic) is False
    assert short.timeouts == [0.05]


def test_a_real_sigterm_during_a_backoff_stops_the_process_without_a_kill(
    tmp_path: Path,
) -> None:
    """End to end, the way `systemd` does it: SIGTERM to a process inside its backoff.

    This is the failure the window actually saw -- `watchlist-quote` exceeded
    `TimeoutStopSec`, was killed, and left its unit `failed`. The child runs the real loop
    with a failing step and a 60-second cap, and has to be gone well inside any stop
    timeout, with a clean exit rather than a signal.
    """

    import subprocess
    import sys
    import time as real_time

    program = f"""
import os, signal, sys, time
from pathlib import Path
from threading import Event
from datetime import UTC, datetime, timedelta
from rquant.runtime_service_control import (
    RuntimeServiceControl, RuntimeServicePlane, RuntimeServiceSpec, RuntimeStepResult,
    run_service_loop,
)
spec = RuntimeServiceSpec(
    service_id="watchlist-quote",
    plane=RuntimeServicePlane.LIVE,
    stale_after=timedelta(seconds=10),
    producer_commit="a" * 40,
)
control = RuntimeServiceControl(Path({str(tmp_path)!r}), spec=spec)
stop = Event()
def request_stop(_signum, _frame):
    stop.set()
signal.signal(signal.SIGTERM, request_stop)
attempts = 0
def step():
    global attempts
    attempts += 1
    if attempts == 2:
        print("backing-off", flush=True)
    raise RuntimeError("candidate store is damaged")
run_service_loop(
    control, step=step, stop_event=stop, interval_seconds=30.0,
    max_failure_backoff_seconds=60.0,
)
print("clean-exit", flush=True)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", program],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        #: the first failure's own wait is 30 s, which is already long enough to measure
        real_time.sleep(0.5)
        started = real_time.monotonic()
        child.terminate()
        stdout, stderr = child.communicate(timeout=15)
        elapsed = real_time.monotonic() - started
    finally:
        if child.poll() is None:  # pragma: no cover - only on a failure
            child.kill()
            child.communicate()

    assert child.returncode == 0, (child.returncode, stderr)
    assert "clean-exit" in stdout, stdout
    assert elapsed < 5.0, elapsed


def test_a_stop_during_the_loop_s_own_wait_never_needs_to_be_killed(
    tmp_path: Path,
) -> None:
    """The same thing through the loop: a 30-second wait, and a stop that lands in it.

    The delay the loop hands that wait is the backoff whenever one is in force -- which is
    what `test_the_same_failure_backs_off_instead_of_retrying_every_interval` pins -- so a
    stop during a 60-second backoff comes out of exactly this path.
    """

    import threading
    import time as real_time

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    stop = Event()
    waiting = Event()
    attempts = 0

    def step() -> RuntimeStepResult:
        nonlocal attempts
        attempts += 1
        waiting.set()
        raise RuntimeError("candidate store is damaged")

    def stopper() -> None:
        assert waiting.wait(10.0)
        real_time.sleep(0.05)
        stop.set()

    watcher = threading.Thread(target=stopper)
    watcher.start()
    started = real_time.monotonic()
    final = run_service_loop(
        control,
        step=step,
        stop_event=stop,
        interval_seconds=30.0,
    )
    elapsed = real_time.monotonic() - started
    watcher.join(5.0)

    assert final.status is RuntimeServiceStatus.STOPPED
    assert final.stop_reason == "loop completed"
    assert attempts == 1, attempts
    assert elapsed < 3.0, elapsed
