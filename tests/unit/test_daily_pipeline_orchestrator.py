from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunState,
    DailyStageState,
    LeaseLost,
    StageResult,
)
from rquant.runtime_contracts import canonical_sha256

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)
TRADE_DATE = date(2026, 8, 3)
SHA = "a" * 64
COMMIT = "b" * 40


class RecordingAdapter:
    def __init__(
        self,
        stage_id: str,
        calls: list[str],
        *,
        ready: bool = True,
        crash_once: bool = False,
        estimated_memory_mb: int | None = None,
        estimated_io_bytes: int | None = None,
    ) -> None:
        self.stage_id = stage_id
        self._calls = calls
        self._ready = ready
        self._crash_once = crash_once
        self._estimated_memory_mb = estimated_memory_mb
        self._estimated_io_bytes = estimated_io_bytes

    def health(self, _context):
        from rquant.daily_pipeline_orchestrator import DailyStageHealth

        return DailyStageHealth(
            ready=self._ready,
            detail="ready" if self._ready else "busy",
            estimated_memory_mb=self._estimated_memory_mb,
            estimated_io_bytes=self._estimated_io_bytes,
        )

    def run(self, context):
        self._calls.append(context.attempt.stage_id)
        if self._crash_once:
            self._crash_once = False
            raise SystemExit("simulated process exit")
        return StageResult(
            content_hash=("a" * 64),
            evidence_hash=("b" * 64),
        )


class StaticSourceResolver:
    """Fixture-only current source authority for legacy in-process stage tests."""

    def resolve(self, run):
        from rquant.daily_pipeline_orchestrator import DailySourceIdentity

        return DailySourceIdentity(
            source_generation_id=run.spec.source_generation_id,
            source_content_hash=run.spec.source_content_hash,
        )


def _definition():
    from rquant.daily_pipeline_orchestrator import (
        DailyPipelineDefinition,
        DailyStageBudget,
        DailyStageRuntimeSpec,
    )

    return DailyPipelineDefinition(
        stages=(
            DailyStageRuntimeSpec(
                stage_id="raw_capture",
                budget=DailyStageBudget(max_wall_seconds=30),
            ),
            DailyStageRuntimeSpec(
                stage_id="validate_candidate",
                depends_on=("raw_capture",),
                budget=DailyStageBudget(max_wall_seconds=30),
            ),
            DailyStageRuntimeSpec(
                stage_id="canonical_publish",
                depends_on=("validate_candidate",),
                budget=DailyStageBudget(max_wall_seconds=30),
            ),
            DailyStageRuntimeSpec(
                stage_id="screen",
                depends_on=("canonical_publish",),
                budget=DailyStageBudget(max_wall_seconds=30),
            ),
            DailyStageRuntimeSpec(
                stage_id="pool",
                depends_on=("screen",),
                budget=DailyStageBudget(max_wall_seconds=30),
            ),
            DailyStageRuntimeSpec(
                stage_id="summary",
                depends_on=("screen", "pool"),
                budget=DailyStageBudget(max_wall_seconds=30),
            ),
        )
    )


def _orchestrator(tmp_path: Path, adapters: tuple[RecordingAdapter, ...]):
    from rquant.daily_pipeline_orchestrator import DailyPipelineOrchestrator

    profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )
    ledger = DailyPipelineLedger(
        storage_profile=profile,
        service_owner="daily-shadow",
    )
    return DailyPipelineOrchestrator(
        ledger=ledger,
        service_owner="daily-shadow",
        definition=_definition(),
        adapters=adapters,
        source_resolver=StaticSourceResolver(),
        clock=lambda: NOW,
        execution_mode="test_fixture",
    )


def _create_run(orchestrator):
    return orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=TRADE_DATE,
        source_generation_id=SHA,
        source_content_hash="c" * 64,
        command_manifest_hash="e" * 64,
        code_commit=COMMIT,
        profile_hash="d" * 64,
        now=NOW,
    )


class ProcessAdapter:
    stage_id = "capture"

    def __init__(self, marker: Path, *, spawn_descendant: bool = False) -> None:
        self.marker = marker
        self.command_calls = 0
        self._spawn_descendant = spawn_descendant

    def health(self, _context):
        from rquant.daily_pipeline_orchestrator import DailyStageHealth

        return DailyStageHealth(ready=True, detail="ready")

    def prepare(self, context):
        from rquant.daily_pipeline_ledger import DailyStageEffectIntent

        return DailyStageEffectIntent(
            mode=context.run.spec.mode,
            idempotency_key=canonical_sha256(
                {
                    "contract": "daily-stage-idempotency-key/v3",
                    "mode": context.run.spec.mode,
                    "run_id": context.run.run_id,
                    "stage_id": context.attempt.stage_id,
                    "input_identity": context.run.input_identity,
                    "command_manifest_hash": context.run.spec.command_manifest_hash,
                }
            ),
            command_manifest_hash=context.run.spec.command_manifest_hash,
            adapter_identity="unit-process-adapter/v1",
            receipt_locator=str(self.marker.with_suffix(".receipt")),
        )

    def command(self, _context, _effect):
        from rquant.daily_pipeline_orchestrator import DailyStageProcessSpec

        self.command_calls += 1
        child_body = "time.sleep(60)"
        if self._spawn_descendant:
            descendant_marker = self.marker.with_suffix(".descendant")
            descendant_body = (
                "from pathlib import Path; import os, time; "
                f"target = Path({str(descendant_marker)!r}); "
                "pending = target.with_name(target.name + '.tmp'); "
                "pending.write_text("
                "str(os.getpid()) + ':' + str(os.getpgrp())); "
                "os.replace(pending, target); "
                "time.sleep(60)"
            )
            child_body = (
                f"descendant_marker = Path({str(descendant_marker)!r}); "
                f"subprocess.Popen((sys.executable, '-c', {descendant_body!r})); "
                "deadline = time.monotonic() + 1.0; "
                'exec("while not descendant_marker.exists():\\n'
                "    if time.monotonic() >= deadline:\\n"
                "        raise TimeoutError('descendant pid marker missing')\\n"
                '    time.sleep(0.001)")'
            )
        return DailyStageProcessSpec(
            argv=(
                sys.executable,
                "-c",
                "from pathlib import Path; import os, subprocess, sys, time; "
                f"Path({str(self.marker)!r}).write_text(str(os.getpid())); {child_body}",
            )
        )

    def reconcile(self, _context, _effect):
        return None


def _process_orchestrator(
    tmp_path: Path,
    adapter: ProcessAdapter,
    *,
    monotonic_clock: Callable[[], float] = time.monotonic,
):
    from rquant.daily_pipeline_orchestrator import (
        DailyPipelineDefinition,
        DailyPipelineOrchestrator,
        DailyStageBudget,
        DailyStageRuntimeSpec,
    )

    profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )
    return DailyPipelineOrchestrator(
        ledger=DailyPipelineLedger(storage_profile=profile, service_owner="daily-shadow"),
        service_owner="daily-shadow",
        definition=DailyPipelineDefinition(
            stages=(
                DailyStageRuntimeSpec(
                    stage_id="capture",
                    budget=DailyStageBudget(max_wall_seconds=30),
                ),
            )
        ),
        adapters=(adapter,),
        source_resolver=StaticSourceResolver(),
        clock=lambda: NOW,
        monotonic_clock=monotonic_clock,
    )


def _create_process_run(orchestrator):
    return orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=TRADE_DATE,
        source_generation_id=SHA,
        source_content_hash="c" * 64,
        command_manifest_hash="e" * 64,
        code_commit=COMMIT,
        profile_hash="d" * 64,
        now=NOW,
    )


def _assert_running_without_terminal_mutation(orchestrator, run_id: str) -> None:
    stage = orchestrator.ledger.stage(run_id, "capture")
    assert stage.state is DailyStageState.RUNNING
    assert stage.terminal_receipt_id is None
    assert stage.last_failure is None
    assert orchestrator.ledger.run(run_id).state is DailyRunState.RUNNING


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_spawn_boundary_lease_loss_does_not_run_command_or_fail_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = ProcessAdapter(tmp_path / "child-pid")
    orchestrator = _process_orchestrator(tmp_path, adapter)
    run = _create_process_run(orchestrator)
    lease_loss = LeaseLost("writer lease is stale")
    adapter_error = RuntimeError("adapter failed during authority loss")
    authority_group = ExceptionGroup(
        "heartbeat authority loss",
        [lease_loss, adapter_error],
    )

    def lose_authority_as_group(_lease, _attempt):
        raise authority_group

    with monkeypatch.context() as lease_loss_patch:
        lease_loss_patch.setattr(
            orchestrator,
            "_heartbeat",
            lose_authority_as_group,
        )

        with pytest.raises(ExceptionGroup, match="heartbeat authority loss") as raised:
            orchestrator.advance(run.run_id, now=NOW)

    assert raised.value is authority_group
    assert raised.value.exceptions == (lease_loss, adapter_error)
    assert adapter.command_calls == 0
    _assert_running_without_terminal_mutation(orchestrator, run.run_id)
    _assert_exception_group_without_lease_loss_is_adapter_failure(
        tmp_path / "ordinary-exception-group",
        monkeypatch,
    )
    _assert_injected_monotonic_clock_caps_heartbeat_wait_and_enforces_deadline(
        tmp_path / "monotonic-clock",
        monkeypatch,
    )


def test_live_child_lease_loss_terminates_and_reaps_its_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.daily_pipeline_orchestrator as orchestrator_module

    adapter = ProcessAdapter(tmp_path / "child-pid")
    orchestrator = _process_orchestrator(tmp_path, adapter)
    run = _create_process_run(orchestrator)
    original_heartbeat = orchestrator._heartbeat
    heartbeat_calls = 0
    started_processes: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def capture_popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        started_processes.append(process)
        return process

    def lose_after_child_started(lease, attempt):
        nonlocal heartbeat_calls
        heartbeat_calls += 1
        if heartbeat_calls == 3:
            raise LeaseLost("writer lease is stale")
        return original_heartbeat(lease, attempt)

    monkeypatch.setattr(orchestrator_module, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(orchestrator_module.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(orchestrator, "_heartbeat", lose_after_child_started)

    with pytest.raises(LeaseLost, match="writer lease"):
        orchestrator.advance(run.run_id, now=NOW)

    assert adapter.command_calls == 1
    assert len(started_processes) == 1
    process = started_processes[0]
    assert process.poll() is not None
    with pytest.raises(ProcessLookupError):
        os.killpg(process.pid, 0)
    _assert_running_without_terminal_mutation(orchestrator, run.run_id)


def test_lease_loss_cleanup_failure_remains_a_typed_authority_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.daily_pipeline_orchestrator import DailyPipelineOrchestratorError

    adapter = ProcessAdapter(tmp_path / "child-pid")
    orchestrator = _process_orchestrator(tmp_path, adapter)
    run = _create_process_run(orchestrator)
    original_heartbeat = orchestrator._heartbeat
    original_terminate = orchestrator._terminate_process_group
    heartbeat_calls = 0

    def lose_after_child_started(lease, attempt):
        nonlocal heartbeat_calls
        heartbeat_calls += 1
        if heartbeat_calls == 3:
            raise LeaseLost("writer lease is stale")
        return original_heartbeat(lease, attempt)

    def terminate_then_report_failure(process):
        original_terminate(process)
        raise DailyPipelineOrchestratorError("simulated process-group verification failure")

    import rquant.daily_pipeline_orchestrator as orchestrator_module

    monkeypatch.setattr(orchestrator_module, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(orchestrator, "_heartbeat", lose_after_child_started)
    monkeypatch.setattr(orchestrator, "_terminate_process_group", terminate_then_report_failure)

    with pytest.raises(LeaseLost, match="cleanup failed") as raised:
        orchestrator.advance(run.run_id, now=NOW)

    assert isinstance(raised.value.__cause__, DailyPipelineOrchestratorError)
    _assert_running_without_terminal_mutation(orchestrator, run.run_id)


def test_normal_leader_exit_cleans_descendants_before_reporting_child_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.daily_pipeline_orchestrator as orchestrator_module

    adapter = ProcessAdapter(tmp_path / "child-pid", spawn_descendant=True)
    orchestrator = _process_orchestrator(tmp_path, adapter)
    run = _create_process_run(orchestrator)
    started_processes: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen
    descendant_marker = adapter.marker.with_suffix(".descendant")
    descendant_pid: int | None = None

    def capture_popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        started_processes.append(process)
        return process

    monkeypatch.setattr(orchestrator_module.subprocess, "Popen", capture_popen)

    try:
        outcome = orchestrator.advance(run.run_id, now=NOW)

        assert outcome is not None
        assert outcome.disposition == "retry_wait"
        assert len(started_processes) == 1
        descendant_pid_text, descendant_pgid_text = descendant_marker.read_text().split(":")
        descendant_pid = int(descendant_pid_text)
        descendant_pgid = int(descendant_pgid_text)
        leader = started_processes[0]
        assert descendant_pgid == leader.pid
        assert leader.poll() is not None
        assert not _process_exists(descendant_pid)
        with pytest.raises(ProcessLookupError):
            os.killpg(leader.pid, 0)
    finally:
        for process in started_processes:
            if process.poll() is None:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1.0)
        if descendant_pid is None and descendant_marker.exists():
            descendant_pid = int(descendant_marker.read_text().split(":", maxsplit=1)[0])
        if descendant_pid is not None and _process_exists(descendant_pid):
            with suppress(ProcessLookupError):
                os.kill(descendant_pid, signal.SIGKILL)


def _assert_exception_group_without_lease_loss_is_adapter_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tmp_path.mkdir()
    adapter = ProcessAdapter(tmp_path / "child-pid")
    orchestrator = _process_orchestrator(tmp_path, adapter)
    run = _create_process_run(orchestrator)
    ordinary_group = ExceptionGroup(
        "ordinary adapter errors",
        [RuntimeError("first"), ValueError("second")],
    )

    def raise_ordinary_group(_lease, _attempt):
        raise ordinary_group

    with monkeypatch.context() as ordinary_group_patch:
        ordinary_group_patch.setattr(orchestrator, "_heartbeat", raise_ordinary_group)
        outcome = orchestrator.advance(run.run_id, now=NOW)

    stage = orchestrator.ledger.stage(run.run_id, "capture")
    assert outcome is not None
    assert outcome.disposition == "retry_wait"
    assert outcome.failure_code == "adapter_exception"
    assert stage.state is DailyStageState.RETRY_WAIT
    assert stage.terminal_receipt_id is None
    assert stage.last_failure is not None
    assert stage.last_failure.error_code == "adapter_exception"


def _assert_injected_monotonic_clock_caps_heartbeat_wait_and_enforces_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.daily_pipeline_orchestrator as orchestrator_module

    tmp_path.mkdir()
    monotonic_values = [
        100.0,
        200.0,
        200.0,
        229.75,
        230.000001,
        230.000001,
    ]
    observed_monotonic: list[float] = []
    values = iter(monotonic_values)

    def monotonic_clock() -> float:
        value = next(values)
        observed_monotonic.append(value)
        return value

    class TimeoutProcess:
        pid = 12345
        args = ("controlled-child",)
        returncode = None

        def __init__(self) -> None:
            self.wait_timeouts: list[float] = []

        def wait(self, timeout: float) -> int:
            self.wait_timeouts.append(timeout)
            raise subprocess.TimeoutExpired(self.args, timeout)

    adapter = ProcessAdapter(tmp_path / "child-pid")
    orchestrator = _process_orchestrator(
        tmp_path,
        adapter,
        monotonic_clock=monotonic_clock,
    )
    run = _create_process_run(orchestrator)
    process = TimeoutProcess()
    termination_calls: list[TimeoutProcess] = []
    heartbeat_calls = 0
    original_heartbeat = orchestrator._heartbeat

    def record_heartbeat(lease, attempt):
        nonlocal heartbeat_calls
        heartbeat_calls += 1
        return original_heartbeat(lease, attempt)

    monkeypatch.setattr(
        orchestrator_module.subprocess,
        "Popen",
        lambda *args, **kwargs: process,
    )
    monkeypatch.setattr(
        orchestrator,
        "_terminate_process_group",
        lambda observed: termination_calls.append(observed),
    )
    monkeypatch.setattr(orchestrator, "_heartbeat", record_heartbeat)

    outcome = orchestrator.advance(run.run_id, now=NOW)

    deadline = monotonic_values[1] + 30
    assert orchestrator_module._HEARTBEAT_INTERVAL_SECONDS == 2.0
    assert orchestrator.definition.runtime_spec("capture").budget.max_wall_seconds == 30
    assert monotonic_values[3] < deadline < monotonic_values[4]
    assert process.wait_timeouts == pytest.approx([2.0, deadline - monotonic_values[3]])
    assert observed_monotonic == monotonic_values
    assert heartbeat_calls == 4
    assert termination_calls == [process]
    assert outcome is not None
    assert outcome.disposition == "failed"
    assert outcome.failure_code == "resource_budget_exceeded"


def test_daily_definition_has_the_isolated_a_to_d_stage_order() -> None:
    definition = _definition()

    assert definition.stage_ids == (
        "raw_capture",
        "validate_candidate",
        "canonical_publish",
        "screen",
        "pool",
        "summary",
    )
    assert definition.runtime_spec("summary").depends_on == ("screen", "pool")
    assert (
        definition.to_run_spec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=SHA,
            source_content_hash="c" * 64,
            command_manifest_hash="e" * 64,
            code_commit=COMMIT,
            profile_hash="d" * 64,
        )
        .stages[-1]
        .stage_id
        == "summary"
    )


def test_advance_runs_one_ready_stage_then_binds_dependency_receipts(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    run = _create_run(orchestrator)

    first = orchestrator.advance(run.run_id, now=NOW)
    assert first.stage_id == "raw_capture"
    assert calls == ["raw_capture"]

    second = orchestrator.advance(run.run_id, now=NOW + timedelta(seconds=1))
    assert second.stage_id == "validate_candidate"
    assert second.dependency_receipt_ids == (first.receipt_id,)


def test_unhealthy_stage_is_recorded_as_retryable_without_calling_adapter(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(
        RecordingAdapter(stage.stage_id, calls, ready=stage.stage_id != "raw_capture")
        for stage in _definition().stages
    )
    orchestrator = _orchestrator(tmp_path, adapters)
    run = _create_run(orchestrator)

    outcome = orchestrator.advance(run.run_id, now=NOW)

    assert outcome.stage_id == "raw_capture"
    assert outcome.disposition == "retry_wait"
    assert calls == []
    assert orchestrator.status(run.run_id).next_stage_id is None


def test_stage_budget_rejects_unhealthy_resource_estimate_without_side_effects(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    adapters = tuple(
        RecordingAdapter(
            stage.stage_id,
            calls,
            estimated_memory_mb=513 if stage.stage_id == "raw_capture" else None,
        )
        for stage in _definition().stages
    )
    orchestrator = _orchestrator(tmp_path, adapters)
    run = _create_run(orchestrator)

    outcome = orchestrator.advance(run.run_id, now=NOW)

    assert outcome is not None
    assert outcome.disposition == "failed"
    assert outcome.failure_code == "resource_budget_exceeded"
    assert calls == []


def test_crash_restart_recovers_without_reexecuting_a_prepared_stage(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    run = _create_run(orchestrator)
    ledger = orchestrator.ledger
    lease = ledger.acquire_writer(
        owner="daily-shadow",
        now=NOW,
        lease_for=timedelta(seconds=1),
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    prepared = ledger.prepare_success(
        lease,
        attempt,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW,
    )
    assert prepared.stage_id == "raw_capture"

    recovery = orchestrator.recover(now=NOW + timedelta(seconds=2))

    assert recovery.finalized_receipt_ids == (prepared.receipt_id,)
    assert calls == []
    second = orchestrator.advance(run.run_id, now=NOW + timedelta(seconds=3))
    assert second.stage_id == "validate_candidate"
    assert calls == ["validate_candidate"]


def test_duplicate_advance_is_exactly_once_and_completes_the_dag(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    run = _create_run(orchestrator)

    while orchestrator.advance(run.run_id, now=NOW) is not None:
        pass
    duplicate = orchestrator.advance(run.run_id, now=NOW)

    assert duplicate is None
    assert calls == list(_definition().stage_ids)
    assert orchestrator.status(run.run_id).state is DailyRunState.SUCCEEDED


def test_input_revision_cannot_continue_an_old_run(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    first = _create_run(orchestrator)
    second = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=TRADE_DATE,
        source_generation_id="e" * 64,
        source_content_hash="f" * 64,
        command_manifest_hash="e" * 64,
        code_commit=COMMIT,
        profile_hash="d" * 64,
        now=NOW,
    )

    assert first.run_id != second.run_id

    class RevisedSourceResolver:
        def resolve(self, run):
            from rquant.daily_pipeline_orchestrator import DailySourceIdentity

            if run.run_id == first.run_id:
                return DailySourceIdentity(
                    source_generation_id="e" * 64,
                    source_content_hash="f" * 64,
                )
            return StaticSourceResolver().resolve(run)

    object.__setattr__(orchestrator, "_source_resolver", RevisedSourceResolver())
    with pytest.raises(ValueError, match="source identity"):
        orchestrator.advance(first.run_id, now=NOW)


def test_cancelled_run_cannot_continue(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    run = _create_run(orchestrator)

    cancelled = orchestrator.cancel(run.run_id, reason="newer_source_revision", now=NOW)

    assert cancelled.state is DailyRunState.CANCELLED
    assert orchestrator.advance(run.run_id, now=NOW) is None
    assert calls == []


def test_expired_run_deadline_is_not_claimed(tmp_path: Path) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=TRADE_DATE,
        source_generation_id=SHA,
        source_content_hash="c" * 64,
        command_manifest_hash="e" * 64,
        code_commit=COMMIT,
        profile_hash="d" * 64,
        deadline_at=NOW,
        now=NOW,
    )

    assert orchestrator.advance(run.run_id, now=NOW) is None
    assert orchestrator.status(run.run_id).state is DailyRunState.FAILED
    assert calls == []


def test_advance_claims_only_the_explicit_requested_run(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)
    orchestrator = _orchestrator(tmp_path, adapters)
    first = _create_run(orchestrator)
    second = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=TRADE_DATE,
        source_generation_id="e" * 64,
        source_content_hash="f" * 64,
        command_manifest_hash="e" * 64,
        code_commit=COMMIT,
        profile_hash="d" * 64,
        now=NOW + timedelta(seconds=1),
    )

    outcome = orchestrator.advance(second.run_id, now=NOW + timedelta(seconds=2))
    assert outcome is not None
    assert outcome.run_id == second.run_id
    assert orchestrator.ledger.stage(first.run_id, "raw_capture").attempts == 0
    assert orchestrator.ledger.stage(first.run_id, "raw_capture").state.value == "pending"

    outcome = orchestrator.advance(first.run_id, now=NOW + timedelta(seconds=3))

    assert outcome is not None
    assert outcome.stage_id == "raw_capture"
    assert calls == ["raw_capture", "raw_capture"]


def test_create_run_rejects_source_identity_that_is_not_current(tmp_path: Path) -> None:
    from rquant.daily_pipeline_orchestrator import DailyPipelineOrchestrator

    calls: list[str] = []
    adapters = tuple(RecordingAdapter(stage.stage_id, calls) for stage in _definition().stages)

    class RevisedSourceResolver:
        def resolve(self, _run):
            from rquant.daily_pipeline_orchestrator import DailySourceIdentity

            return DailySourceIdentity(
                source_generation_id="e" * 64,
                source_content_hash="f" * 64,
            )

    profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )
    ledger = DailyPipelineLedger(
        storage_profile=profile,
        service_owner="daily-shadow",
    )
    orchestrator = DailyPipelineOrchestrator(
        ledger=ledger,
        service_owner="daily-shadow",
        definition=_definition(),
        adapters=adapters,
        source_resolver=RevisedSourceResolver(),
        clock=lambda: NOW,
        execution_mode="test_fixture",
    )

    with pytest.raises(ValueError, match="source identity"):
        orchestrator.create_run(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=SHA,
            source_content_hash="c" * 64,
            command_manifest_hash="e" * 64,
            code_commit=COMMIT,
            profile_hash="d" * 64,
            now=NOW,
        )
