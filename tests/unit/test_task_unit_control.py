from __future__ import annotations

import base64
import os
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.unit.test_ops_status import _manifest

if TYPE_CHECKING:
    from rquant.task_unit_control import TaskUnitRunPolicy, TaskUnitRuntimeState

NOW = datetime(2026, 10, 6, 1, 14, tzinfo=UTC)
BOOT = "12345678-1234-1234-1234-123456789abc"
UNIT = "rquant-backup.service"
REQUEST = "c2b8d5ac-dc25-4c55-af11-cb217465b0b4"


def policy(*, mode: str = "readonly", enabled: bool = True) -> TaskUnitRunPolicy:
    from rquant.task_unit_control import TaskUnitRunPolicy

    return TaskUnitRunPolicy.model_validate({
        "version": 1, "host_name": "rquant-test", "manifest_digest": _manifest().digest,
        "enabled": enabled, "units": [{"unit": UNIT, "mode": mode, "enabled": enabled}],
    })


def state(**updates: object) -> TaskUnitRuntimeState:
    from rquant.task_unit_control import TaskUnitRuntimeState

    return TaskUnitRuntimeState.model_validate({
        "host_name": "rquant-test", "boot_id": BOOT, "unit": UNIT,
        "load_state": "loaded", "active_state": "inactive", "invocation_id": None,
        "start_jobs": [], "observed_at": NOW,
    } | updates)


def signed_policy(tmp_path: Path) -> tuple[Path, bytes]:
    from rquant.task_unit_control import SignedTaskUnitRunPolicy

    openssl = shutil.which("openssl")
    assert openssl is not None
    private, public = tmp_path / "synthetic-private.pem", tmp_path / "public.pem"
    body, signature = tmp_path / "policy.body", tmp_path / "policy.signature"
    subprocess.run((openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private)), check=True, capture_output=True)
    subprocess.run((openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)), check=True, capture_output=True)
    body.write_bytes(policy().signing_bytes())
    subprocess.run((openssl, "pkeyutl", "-sign", "-inkey", str(private), "-rawin", "-in", str(body), "-out", str(signature)), check=True, capture_output=True)
    signed = SignedTaskUnitRunPolicy(policy=policy(), signature=base64.b64encode(signature.read_bytes()).decode())
    path = tmp_path / "policy.json"
    path.write_bytes(signed.canonical_bytes())
    return path, public.read_bytes()


def test_tsc_05_signed_policy_exact_manifest_and_host(tmp_path: Path) -> None:
    from rquant.task_unit_control import load_task_unit_policy

    path, key = signed_policy(tmp_path)
    assert load_task_unit_policy(path, public_key_pem=key, manifest=_manifest()) == policy()
    raw = path.read_bytes()
    path.write_bytes(raw.replace(b"readonly", b"writer"))
    with pytest.raises(ValueError, match="signature"):
        load_task_unit_policy(path, public_key_pem=key, manifest=_manifest())
    path.write_bytes(raw)
    wrong_host = _manifest().model_dump() | {"host_name": "other"}
    with pytest.raises(ValueError, match="host|manifest"):
        load_task_unit_policy(path, public_key_pem=key, manifest=type(_manifest()).model_validate(wrong_host))
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises((OSError, ValueError)):
        load_task_unit_policy(link, public_key_pem=key, manifest=_manifest())


@pytest.mark.parametrize("unit", ["rquant-backup@x.service", "rquant-backup.service;id", "rquant-unknown.service"])
def test_tsc_05_no_prefix_unit_authority(unit: str) -> None:
    from rquant.task_unit_control import guard_task_unit_run

    with pytest.raises(ValueError, match="exact|allowlist|policy"):
        guard_task_unit_run(policy(), manifest=_manifest(), state=state(), unit=unit, now=NOW)


def test_tsc_05_default_off_and_extra_fields() -> None:
    from rquant.task_unit_control import TaskUnitRunPolicy, guard_task_unit_run

    default = TaskUnitRunPolicy(version=1, host_name="rquant-test", manifest_digest=_manifest().digest, units=())
    assert default.enabled is False
    with pytest.raises(ValueError, match="disabled"):
        guard_task_unit_run(default, manifest=_manifest(), state=state(), unit=UNIT, now=NOW)
    with pytest.raises(ValueError):
        TaskUnitRunPolicy.model_validate(policy().model_dump() | {"command": "systemctl"})
    raw = policy().model_dump()
    raw["units"] = tuple(raw["units"]) * 2
    with pytest.raises(ValueError, match="duplicate"):
        TaskUnitRunPolicy.model_validate(raw)


@pytest.mark.parametrize("active", ["active", "activating", "reloading", "deactivating"])
def test_tsc_05_running_exclusion(active: str) -> None:
    from rquant.task_unit_control import guard_task_unit_run

    with pytest.raises(ValueError, match="running"):
        guard_task_unit_run(policy(), manifest=_manifest(), state=state(active_state=active), unit=UNIT, now=NOW)


def test_tsc_05_pending_job_or_stale_state_closed() -> None:
    from rquant.task_unit_control import guard_task_unit_run

    for raw in (state(start_jobs=("/org/freedesktop/systemd1/job/15",)), state(observed_at=NOW - timedelta(seconds=120)), state(host_name="elsewhere")):
        with pytest.raises(ValueError):
            guard_task_unit_run(policy(), manifest=_manifest(), state=raw, unit=UNIT, now=NOW)


@pytest.mark.parametrize(("clock", "allowed"), [
    (datetime(2026, 10, 6, 1, 14, 59, tzinfo=UTC), True),
    (datetime(2026, 10, 6, 1, 15, tzinfo=UTC), False),
    (datetime(2026, 10, 6, 7, 10, tzinfo=UTC), False),
    (datetime(2026, 10, 6, 7, 10, 0, 1, tzinfo=UTC), True),
    (datetime(2026, 10, 10, 1, 30, tzinfo=UTC), True),
])
def test_tsc_05_server_shanghai_window(clock: datetime, allowed: bool) -> None:
    from rquant.task_unit_control import guard_task_unit_run

    if allowed:
        assert guard_task_unit_run(policy(mode="writer"), manifest=_manifest(), state=state(observed_at=clock), unit=UNIT, now=clock).mode == "writer"
    else:
        with pytest.raises(ValueError, match="readonly"):
            guard_task_unit_run(policy(mode="writer"), manifest=_manifest(), state=state(observed_at=clock), unit=UNIT, now=clock)
        assert guard_task_unit_run(policy(), manifest=_manifest(), state=state(observed_at=clock), unit=UNIT, now=clock).mode == "readonly"


def run_material(**updates: object) -> dict[str, object]:
    result = {
        "origin": "manual", "host_name": "rquant-test", "boot_id": BOOT,
        "unit": UNIT, "manifest_digest": _manifest().digest, "invocation_id": "a" * 32,
        "request_id": REQUEST, "request_hash": "b" * 64,
        "started_at": NOW, "started_monotonic_ns": 1_000_000_000,
        "ended_at": NOW + timedelta(seconds=2), "ended_monotonic_ns": 3_000_000_000,
        "result": "success", "exec_status": 0, "observed_at": NOW + timedelta(seconds=3), "observed_monotonic_ns": 4_000_000_000,
        "job_witness": {
            "host_name": "rquant-test", "boot_id": BOOT, "unit": UNIT,
            "job_path": "/org/freedesktop/systemd1/job/15", "job_id": 15,
            "caller_pid": 200, "sender": ":1.17", "call_serial": 10, "reply_serial": 10,
            "call_monotonic_ns": 900_000_000, "job_new_monotonic_ns": 950_000_000,
            "reply_monotonic_ns": 980_000_000,
            "timer_trigger_before_usec": 50, "timer_trigger_after_usec": 50,
            "competing_calls": (), "invocation_id": "a" * 32,
        },
        "completion_witness": {"host_name": "rquant-test", "boot_id": BOOT, "unit": UNIT, "invocation_id": "a" * 32,
                               "job_path": "/org/freedesktop/systemd1/job/15", "job_id": 15,
                               "job_removed_monotonic_ns": 3_100_000_000, "job_result": "done"},
    } | updates
    if result["ended_at"] is None:
        result["completion_witness"] = None
    return result


def test_tsc_07_real_same_invocation_and_monotonic_duration() -> None:
    from rquant.task_unit_control import TaskUnitRunEvidence

    run = TaskUnitRunEvidence.model_validate(run_material())
    assert run.status == "succeeded"
    assert run.duration_ns == 2_000_000_000
    failed = TaskUnitRunEvidence.model_validate(run_material(result="exit-code", exec_status=7))
    assert failed.status == "failed"
    assert TaskUnitRunEvidence.model_validate(run_material(ended_at=None, ended_monotonic_ns=None, result=None, exec_status=None)).status == "started"
    with pytest.raises(ValueError, match="end|invocation"):
        TaskUnitRunEvidence.model_validate(run_material(ended_at=None))


@pytest.mark.parametrize("field", ["job_path", "reply_serial", "invocation_id", "timer_trigger_after_usec", "competing_calls"])
def test_tsc_07_ambiguous_job_not_success(field: str) -> None:
    from rquant.task_unit_control import TaskUnitRunEvidence

    raw = run_material()
    witness = dict(raw["job_witness"])
    witness[field] = {"job_path": "/org/freedesktop/systemd1/job/16", "reply_serial": 11, "invocation_id": "c" * 32, "timer_trigger_after_usec": 60, "competing_calls": (":1.18",)}[field]
    raw["job_witness"] = witness
    with pytest.raises(ValueError):
        TaskUnitRunEvidence.model_validate(raw)


def test_tsc_07_future_end_clock_rollback_or_unbound_manual_rejected() -> None:
    from rquant.task_unit_control import TaskUnitRunEvidence

    for update in (
        {"observed_at": NOW + timedelta(seconds=1)}, {"ended_monotonic_ns": 500_000_000},
        {"ended_at": NOW - timedelta(seconds=1)}, {"job_witness": None}, {"request_id": None},
        {"origin": "timer"}, {"exec_status": 2**63}, {"result": "success", "exec_status": 2},
    ):
        with pytest.raises(ValueError):
            TaskUnitRunEvidence.model_validate(run_material(**update))


def test_tsc_07_timer_show_success_without_trigger_job_not_a_run_result() -> None:
    from rquant.task_unit_control import TaskUnitRunEvidence

    raw = run_material(origin="timer", request_id=None, request_hash=None, job_witness=None)
    with pytest.raises(ValueError, match="witness|trigger|job"):
        TaskUnitRunEvidence.model_validate(raw)


def test_tsc_07_show_end_without_same_job_removed_not_complete() -> None:
    from rquant.task_unit_control import TaskUnitRunEvidence

    with pytest.raises(ValueError, match="witness|removed|completion"):
        TaskUnitRunEvidence.model_validate(run_material(completion_witness=None))


def systemd_window(**updates: object) -> dict[str, object]:
    return {
        "host_name": "rquant-test", "boot_id": BOOT, "unit": UNIT, "manifest_digest": _manifest().digest,
        "request_id": REQUEST, "request_hash": "b" * 64, "caller_pid": 200, "systemd_sender": ":1.1",
        "previous_invocation_id": None, "timer_before_usec": 50, "timer_after_usec": 50,
        "observed_at": NOW + timedelta(seconds=3), "observed_monotonic_ns": 4_000_000_000,
        "calls": ({"sender": ":1.17", "pid": 200, "serial": 10, "unit": UNIT, "mode": "fail", "monotonic_ns": 900_000_000},),
        "replies": ({"sender": ":1.1", "destination": ":1.17", "reply_serial": 10, "job_path": "/org/freedesktop/systemd1/job/15", "monotonic_ns": 980_000_000},),
        "events": (
            {"sender": ":1.1", "member": "JobNew", "unit": UNIT, "job_id": 15, "job_path": "/org/freedesktop/systemd1/job/15", "monotonic_ns": 950_000_000},
            {"sender": ":1.1", "member": "JobRemoved", "unit": UNIT, "job_id": 15, "job_path": "/org/freedesktop/systemd1/job/15", "monotonic_ns": 3_100_000_000, "result": "done"},
        ),
        "invocation": {"host_name": "rquant-test", "boot_id": BOOT, "unit": UNIT, "invocation_id": "a" * 32,
                       "started_at": NOW, "started_monotonic_ns": 1_000_000_000, "ended_at": NOW + timedelta(seconds=2),
                       "ended_monotonic_ns": 3_000_000_000, "result": "success", "exec_status": 0},
    } | updates


def test_tsc_07_producer_binds_actual_call_reply_events_and_invocation() -> None:
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run

    run = bind_systemd_unit_run(TaskSystemdRunWindow.model_validate(systemd_window()))
    assert run.request_id == REQUEST and run.job_witness.caller_pid == 200
    assert run.status == "succeeded" and run.duration_ns == 2_000_000_000


@pytest.mark.parametrize("change", ["caller", "reply", "external", "boot"])
def test_tsc_07_producer_cannot_claim_an_external_or_mixed_run(change: str) -> None:
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run

    raw = systemd_window()
    if change == "caller":
        raw["caller_pid"] = 201
    elif change == "reply":
        raw["replies"] = tuple(dict(row) | {"destination": ":1.18"} for row in raw["replies"])
    elif change == "external":
        raw["calls"] = tuple(raw["calls"]) + (dict(raw["calls"][0]) | {"sender": ":1.18", "pid": 201},)
    else:
        raw["invocation"] = dict(raw["invocation"]) | {"boot_id": "22222222-1234-1234-1234-123456789abc"}
    with pytest.raises(ValueError, match="caller|reply|ambiguous|identity|invocation"):
        bind_systemd_unit_run(TaskSystemdRunWindow.model_validate(raw))


def test_tsc_05_actual_leaf_has_fixed_start_argv_and_refuses_missing_monitor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_unit_control import SystemdUnitRunExecutor

    executor = SystemdUnitRunExecutor(manifest_path=tmp_path / "manifest.json", policy_path=tmp_path / "policy.json", manifest_public_key_pem=b"public", policy_public_key_pem=b"public")
    assert executor.start_argv(UNIT) == ("/usr/bin/busctl", "--system", "--json=short", "--timeout=5", "--allow-interactive-authorization=no", "call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "StartUnit", "ss", UNIT, "fail")
    with pytest.raises(ValueError):
        executor.start_argv("rquant-backup.service;id")
    monkeypatch.setattr(executor, "configuration", lambda: (_manifest(), policy()))
    monkeypatch.setattr(executor, "read_state", lambda *_: state())
    calls: list[tuple[str, ...]] = []

    def forbidden_monitor(*args: object, **kwargs: object) -> object:
        calls.append(tuple(args[0]))
        raise PermissionError("synthetic monitor Access denied before start")

    monkeypatch.setattr("rquant.task_unit_control.subprocess.Popen", forbidden_monitor)
    with pytest.raises(PermissionError, match="monitor"):
        with executor.prepare(UNIT, now=NOW):
            pytest.fail("unavailable monitor cannot give a start permit")
    assert len(calls) == 1 and "monitor" in calls[0] and "StartUnit" not in calls[0]


@pytest.mark.parametrize("changed", ["invocation", "active", "job"])
def test_tsc_05_fixed_leaf_rechecks_the_complete_accepted_runtime_before_start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str) -> None:
    from rquant.task_unit_control import SystemdUnitRunExecutor, _TaskSystemdMonitor

    writes: list[int] = []
    starts: list[tuple[str, ...]] = []

    class MonitorProcess:
        def __init__(self) -> None:
            out, write = os.pipe()
            err, error_write = os.pipe()
            writes.extend((write, error_write))
            self.stdout, self.stderr = os.fdopen(out, "rb"), os.fdopen(err, "rb")
            self.returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9

        def wait(self, timeout: float) -> int:
            assert self.returncode == -9
            return self.returncode

    def launch(argv: tuple[str, ...], **kwargs: object) -> MonitorProcess:
        if "monitor" not in argv:
            starts.append(argv)
            raise AssertionError("changed original state cannot launch StartUnit")
        return MonitorProcess()

    monkeypatch.setattr("rquant.task_unit_control.subprocess.Popen", launch)
    executor = SystemdUnitRunExecutor(manifest_path=tmp_path / "manifest", policy_path=tmp_path / "policy", manifest_public_key_pem=b"test", policy_public_key_pem=b"test", clock=lambda: NOW)
    monkeypatch.setattr(executor, "configuration", lambda: (_manifest(), policy()))
    updates = {"invocation": {"invocation_id": "a" * 32}, "active": {"active_state": "failed"}, "job": {"start_jobs": ("/org/freedesktop/systemd1/job/16",)}}[changed]
    monkeypatch.setattr(executor, "_timer_counter", lambda *_args, **_kwargs: 50)
    monkeypatch.setattr(executor, "read_state", lambda *_args, **_kwargs: state(**updates))
    session = _TaskSystemdMonitor(UNIT)
    try:
        with pytest.raises(ValueError, match="runtime|running|pending"):
            executor.invoke(command_id=REQUEST, request_hash="b" * 64, context_host="rquant-test", context_boot=BOOT,
                manifest_digest=_manifest().digest, policy_digest=policy().digest, expected_state=state(), session=session)
        assert starts == []
    finally:
        session.close()
        for descriptor in writes:
            os.close(descriptor)
    assert session.closed and session.process.returncode == -9


def test_tsc_05_fixed_leaf_queries_share_one_deadline_and_byte_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_unit_control import _TaskSystemdBudget

    clock = [10.0]
    calls: list[tuple[float, int]] = []
    monkeypatch.setattr("rquant.task_unit_control.monotonic_clock.monotonic", lambda: clock[0])

    def read(argv: tuple[str, ...], timeout: float, max_bytes: int) -> bytes:
        calls.append((timeout, max_bytes))
        clock[0] += timeout
        return b"x" * max_bytes

    monkeypatch.setattr("rquant.task_unit_control._run_bounded", read)
    budget = _TaskSystemdBudget()
    assert len(budget.query(("fixed",), 4096)) == 4096
    assert len(budget.query(("fixed",), 16 * 1024)) == 12 * 1024
    with pytest.raises(ValueError, match="budget"):
        budget.query(("fixed",), 1)
    assert calls == [(1.0, 4096), (1.0, 12 * 1024)]
    later = _TaskSystemdBudget()
    clock[0] += 4.75
    assert later.query(("fixed",), 1) == b"x"
    assert calls[-1] == (.25, 1)
    with pytest.raises(TimeoutError, match="deadline"):
        later.query(("fixed",), 1)
