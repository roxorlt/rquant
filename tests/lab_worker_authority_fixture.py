from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rquant.lab_daemon import LabDaemonConfigurationError

NOW = datetime(2026, 7, 24, 0, 1, tzinfo=UTC)


def _counter_increment(path_value: object) -> int:
    path = Path(str(path_value))
    current = int(path.read_text(encoding="ascii")) if path.exists() else 0
    current += 1
    path.write_text(str(current), encoding="ascii")
    return current


def _wait_for_release(path_value: object, *, timeout_seconds: float = 10) -> None:
    path = Path(str(path_value))
    deadline = time.monotonic() + timeout_seconds
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        raise TimeoutError("test authority fixture was not released")


def _spawn_descendant(path_value: object) -> int:
    path = Path(str(path_value))
    child_pid = os.fork()
    if child_pid == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
        os._exit(1)
    deadline = time.monotonic() + 2
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        raise TimeoutError("test authority descendant did not start")
    return child_pid


def evaluate_lab_authority_fixture(
    configuration: object,
    *,
    operation: str,
    spec: object | None,
    admission_request: object | None,
    snapshot: object | None,
    authority_state: object | None,
) -> dict[str, object]:
    from rquant.lab_worker import LabSnapshotAuthorityState, _AuthorityWireResult
    from rquant.resource_admission import (
        AdmissionRequest,
        ResourceSnapshot,
        SourceQuotaLease,
    )

    assert isinstance(configuration, dict)
    if operation == "admission":
        policy_result = _AuthorityWireResult.model_validate(
            evaluate_lab_authority_fixture(
                configuration,
                operation="policy",
                spec=spec,
                admission_request=admission_request,
                snapshot=snapshot,
                authority_state=authority_state,
            )
        )
        snapshot_result = _AuthorityWireResult.model_validate(
            evaluate_lab_authority_fixture(
                configuration,
                operation="snapshot",
                spec=spec,
                admission_request=admission_request,
                snapshot=snapshot,
                authority_state=authority_state,
            )
        )
        quota_lease = None
        if (
            admission_request is not None
            and AdmissionRequest.model_validate(admission_request).expected_quota_units > 0
        ):
            quota_result = _AuthorityWireResult.model_validate(
                evaluate_lab_authority_fixture(
                    configuration,
                    operation="quota",
                    spec=spec,
                    admission_request=admission_request,
                    snapshot=snapshot_result.snapshot,
                    authority_state=snapshot_result.authority_state,
                )
            )
            quota_lease = quota_result.quota_lease
        return _AuthorityWireResult(
            operation="admission",
            policy=policy_result.policy,
            snapshot=snapshot_result.snapshot,
            quota_lease=quota_lease,
            authority_state=snapshot_result.authority_state,
        ).model_dump(mode="python")

    del spec, snapshot, authority_state
    component = configuration[operation]
    assert isinstance(component, dict)
    kind = component["kind"]
    if kind == "unregistered":
        raise LabDaemonConfigurationError(str(component["message"]))
    if operation == "policy":
        if kind in {"block", "recursive-block"}:
            Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            if "entered_path" in component:
                Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
            if kind == "recursive-block":
                _spawn_descendant(component["descendant_pid_path"])
            _wait_for_release(component["release_path"])
        elif kind == "block-after-first":
            call = _counter_increment(component["counter_path"])
            if call > 1:
                Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
                _wait_for_release(component["release_path"])
        elif kind == "failure":
            raise RuntimeError(str(component["message"]))
        elif kind == "slow":
            call = _counter_increment(component["counter_path"])
            if call > 1:
                Path(str(component["second_call_entered_path"])).write_text(
                    "entered", encoding="ascii"
                )
            time.sleep(float(component["delay_seconds"]))
        if kind == "sequence":
            call = _counter_increment(component["counter_path"])
            policies = component["policies"]
            assert isinstance(policies, list)
            policy = policies[min(call - 1, len(policies) - 1)]
        else:
            policy = component["policy"]
        return _AuthorityWireResult(operation="policy", policy=policy).model_dump(mode="python")

    if operation == "snapshot":
        if kind == "failure":
            raise RuntimeError(str(component["message"]))
        if kind == "recursive-block":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            _spawn_descendant(component["descendant_pid_path"])
            threading.Event().wait()
        if kind == "block-after-first":
            marker = Path(str(component["marker_path"]))
            if marker.exists():
                threading.Event().wait()
            marker.write_text("first", encoding="ascii")
        if kind == "block-after-calls" and _counter_increment(component["counter_path"]) > int(
            component["block_after_calls"]
        ):
            threading.Event().wait()
        if kind == "adversarial-hook":
            Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
            if component["behavior"] == "exception":
                raise RuntimeError(f"{component['hook']} exploded")
            if component["behavior"] == "recursive":
                _spawn_descendant(component["descendant_pid_path"])
            _wait_for_release(component["release_path"])
        if kind == "reject-state":
            raise LabDaemonConfigurationError("resource snapshot authority state was not accepted")
        if kind == "sequence":
            call = _counter_increment(component["counter_path"])
            snapshots = component["snapshots"]
            assert isinstance(snapshots, list)
            raw_snapshot = snapshots[min(call - 1, len(snapshots) - 1)]
        elif kind == "selected":
            selection_path = Path(str(component["selection_path"]))
            index = (
                int(selection_path.read_text(encoding="ascii")) if selection_path.exists() else 0
            )
            snapshots = component["snapshots"]
            assert isinstance(snapshots, list)
            raw_snapshot = snapshots[index]
        else:
            raw_snapshot = component["snapshot"]
        state = None
        if kind in {"exporting", "reject-state", "adversarial-hook"}:
            raw_state = component.get("state", {"sequence": 1})
            state = LabSnapshotAuthorityState(
                state_kind="test-fixture",
                state_json=json.dumps(
                    raw_state,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            )
        return _AuthorityWireResult(
            operation="snapshot",
            snapshot=ResourceSnapshot.model_validate(raw_snapshot),
            authority_state=state,
        ).model_dump(mode="python")

    if kind == "none":
        return _AuthorityWireResult(operation="quota").model_dump(mode="python")
    if kind in {"block", "block-after-first"}:
        should_block = True
        if kind == "block-after-first":
            should_block = _counter_increment(component["counter_path"]) > 1
        if should_block:
            if "pid_path" in component:
                Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
            _wait_for_release(component["release_path"])
    if kind == "failure":
        raise RuntimeError(str(component["message"]))
    request = AdmissionRequest.model_validate(admission_request)
    lease = SourceQuotaLease(
        source=request.source or "test-source",
        owner=request.job_id,
        units=request.expected_quota_units,
        granted_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        quota_reset_at=NOW + timedelta(minutes=2),
    )
    return _AuthorityWireResult(operation="quota", quota_lease=lease).model_dump(mode="python")
