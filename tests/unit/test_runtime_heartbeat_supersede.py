"""#216: a stopped instance's heartbeat must not block the next generation's spec.

`read_heartbeat` refused any heartbeat file whose `spec_fingerprint` differed from the spec
being asked about. That is right while the other instance is still running and wrong once it
has stopped, and every generation change that alters a service spec — settings, plane —
leaves exactly such a file behind. Publishing sequence 3 in the first Route A window
therefore left `runtime_health_publisher` and `serving_publisher` unable to start at all:

    ValueError: runtime heartbeat does not match the requested service spec

with `stopped_at 2026-09-06T22:01:46Z` in the stale files. Moving them aside by hand let both
roles enter their loop (runbook R-14).

The heartbeat records no pid, so liveness is asked of the service's own singleton lock —
the same lock `start()` takes, so "nobody holds it" is exactly "no process is running this
service", and it cannot go stale the way a written-down pid would. Supersede therefore needs
all three of: the record says stopped, it carries `stopped_at`, and the lock is free.
"""

from __future__ import annotations

import fcntl
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
)

NOW = datetime(2026, 9, 6, 22, 1, 46, tzinfo=UTC)
SERVICE_ID = "runtime-health.publisher"


def _spec(*, stale_after_seconds: int = 10) -> RuntimeServiceSpec:
    """Two specs for one service: the second is what a settings change publishes."""

    return RuntimeServiceSpec(
        service_id=SERVICE_ID,
        plane=RuntimeServicePlane.SERVING,
        stale_after=timedelta(seconds=stale_after_seconds),
        producer_commit="a" * 40,
    )


def _stopped_under_the_old_spec(root: Path) -> RuntimeServiceControl:
    """Run the previous generation's instance to completion, exactly as a stop does."""

    control = RuntimeServiceControl(root, spec=_spec(), clock=lambda: NOW)
    control.start()
    stopped = control.stop(reason="planned restart")
    assert stopped.status is RuntimeServiceStatus.STOPPED
    assert stopped.stopped_at is not None
    return control


def _heartbeat_document(root: Path, spec: RuntimeServiceSpec) -> dict[str, object]:
    path = RuntimeServiceControl._path_for(root, spec)
    return json.loads(path.read_text(encoding="utf-8"))


def test_a_stopped_instances_heartbeat_is_superseded_not_a_conflict(tmp_path: Path) -> None:
    """The exact #216 shape: stopped file, new spec, nobody running."""

    old = _stopped_under_the_old_spec(tmp_path)
    new_spec = _spec(stale_after_seconds=30)
    assert new_spec.identity != old.spec.identity

    assert RuntimeServiceControl.read_heartbeat(tmp_path, new_spec) is None


def test_the_new_generation_starts_and_overwrites_the_stale_file(tmp_path: Path) -> None:
    """What the operator had to do by hand now happens on the next start."""

    _stopped_under_the_old_spec(tmp_path)
    new_spec = _spec(stale_after_seconds=30)

    control = RuntimeServiceControl(tmp_path, spec=new_spec, clock=lambda: NOW)
    started = control.start()
    try:
        assert started.spec_fingerprint == new_spec.identity
        assert started.status is RuntimeServiceStatus.STARTING
        document = _heartbeat_document(tmp_path, new_spec)
        assert document["spec_fingerprint"] == new_spec.identity
        assert document["stopped_at"] is None
        assert RuntimeServiceControl.read_heartbeat(tmp_path, new_spec) == started
    finally:
        control.stop(reason="test complete")


def test_a_live_instance_under_another_spec_is_still_refused(tmp_path: Path) -> None:
    """The other half of the rule: a running writer's heartbeat is nobody else's to replace."""

    running = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    running.start()
    try:
        with pytest.raises(ValueError, match="does not match the requested service spec"):
            RuntimeServiceControl.read_heartbeat(tmp_path, _spec(stale_after_seconds=30))
    finally:
        running.stop(reason="test complete")


def test_a_heartbeat_that_never_reached_stop_is_still_refused(tmp_path: Path) -> None:
    """A killed or crashed instance leaves a non-stopped record; that still wants a human."""

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    control.start()
    control.stop(reason="planned restart")
    path = RuntimeServiceControl._path_for(tmp_path, _spec())
    document = json.loads(path.read_text(encoding="utf-8"))
    document.update({"status": "degraded", "stopped_at": None, "stop_reason": None})
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match the requested service spec"):
        RuntimeServiceControl.read_heartbeat(tmp_path, _spec(stale_after_seconds=30))


def test_a_stopped_record_whose_lock_is_still_held_is_refused(tmp_path: Path) -> None:
    """`stopped_at` alone is not the rule; the lock is what says the writer is gone.

    This is the mutation the fix has to survive: dropping the liveness half would let a
    process that is holding the singleton lock — whatever its heartbeat happens to say — have
    its file replaced out from under it.
    """

    _stopped_under_the_old_spec(tmp_path)
    lock_path = RuntimeServiceControl._lock_path_for(tmp_path, _spec())
    descriptor = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="does not match the requested service spec"):
            RuntimeServiceControl.read_heartbeat(tmp_path, _spec(stale_after_seconds=30))
    finally:
        os.close(descriptor)


def test_a_matching_spec_is_returned_whatever_the_lock_says(tmp_path: Path) -> None:
    """The supersede branch is only ever reached by a fingerprint mismatch."""

    control = RuntimeServiceControl(tmp_path, spec=_spec(), clock=lambda: NOW)
    started = control.start()
    try:
        assert RuntimeServiceControl.read_heartbeat(tmp_path, _spec()) == started
    finally:
        control.stop(reason="test complete")


def test_a_heartbeat_for_another_service_is_never_superseded(tmp_path: Path) -> None:
    """Identity paths are per-service, so this can only be a corrupted or planted file."""

    _stopped_under_the_old_spec(tmp_path)
    other = _spec()
    path = RuntimeServiceControl._path_for(tmp_path, other)
    document = json.loads(path.read_text(encoding="utf-8"))
    document["service_id"] = "someone-else"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match the requested service spec"):
        RuntimeServiceControl.read_heartbeat(tmp_path, other)


def test_the_liveness_probe_leaves_the_lock_free(tmp_path: Path) -> None:
    """Reading must not become a way to take the singleton lock, even briefly, and keep it."""

    _stopped_under_the_old_spec(tmp_path)
    new_spec = _spec(stale_after_seconds=30)

    assert RuntimeServiceControl.read_heartbeat(tmp_path, new_spec) is None
    assert not RuntimeServiceControl._service_lock_is_held(tmp_path, new_spec)

    control = RuntimeServiceControl(tmp_path, spec=new_spec, clock=lambda: NOW)
    control.start()
    try:
        assert RuntimeServiceControl._service_lock_is_held(tmp_path, new_spec)
    finally:
        control.stop(reason="test complete")


def test_an_unreadable_lock_counts_as_held(tmp_path: Path, monkeypatch) -> None:
    """A probe that cannot answer must never answer "free"."""

    _stopped_under_the_old_spec(tmp_path)
    new_spec = _spec(stale_after_seconds=30)

    real_open = os.open

    def refuse(path, flags, *args, **kwargs):  # type: ignore[no-untyped-def]
        if str(path).endswith(".lock"):
            raise PermissionError(str(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", refuse)

    assert RuntimeServiceControl._service_lock_is_held(tmp_path, new_spec)
    with pytest.raises(ValueError, match="does not match the requested service spec"):
        RuntimeServiceControl.read_heartbeat(tmp_path, new_spec)
