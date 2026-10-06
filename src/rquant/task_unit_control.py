"""Exact signed unit policy and attributable systemd run facts."""

from __future__ import annotations

import base64
import os
import re
import stat
import socket
import subprocess
import selectors
import time as monotonic_clock
from contextlib import contextmanager
from collections.abc import Callable, Iterator
from datetime import datetime, time
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, StrictBool, StrictInt, StrictStr, TypeAdapter, model_validator

from rquant.backtest.contracts import Sha256
from rquant.ed25519_verify import verify_ed25519_signature
from rquant.ops_status import OpsInstallManifest, _bounded_proc_read, _run_bounded, _timestamp, load_signed_ops_manifest
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads

UnitName = Annotated[StrictStr, Field(pattern=r"^[A-Za-z0-9_.@-]{1,128}\.service$")]
BootId = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")]
InvocationId = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{32}$")]
Counter = Annotated[StrictInt, Field(ge=0, le=2**63 - 1)]
JobPath = Annotated[StrictStr, Field(pattern=r"^/org/freedesktop/systemd1/job/[1-9][0-9]{0,9}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class TaskUnitPolicyEntry(RuntimeContractModel):
    unit: UnitName
    mode: Literal["readonly", "writer"]
    enabled: StrictBool = False
    contract: Literal["task-unit-run/v1"] = "task-unit-run/v1"


class TaskUnitRunPolicy(RuntimeContractModel):
    version: Literal[1]
    host_name: StrictStr = Field(min_length=1, max_length=253)
    manifest_digest: Sha256
    enabled: StrictBool = False
    units: tuple[TaskUnitPolicyEntry, ...] = Field(max_length=32)

    @model_validator(mode="after")
    def validate_exact_set(self) -> TaskUnitRunPolicy:
        names = tuple(entry.unit for entry in self.units)
        if len(names) != len(set(names)):
            raise ValueError("task unit policy contains duplicate exact units")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))

    def bind_manifest(self, manifest: OpsInstallManifest) -> None:
        if self.host_name != manifest.host_name or self.manifest_digest != manifest.digest:
            raise ValueError("unit policy host/manifest binding differs")
        exact = {entry.service for entry in manifest.units}
        if any(entry.unit not in exact for entry in self.units):
            raise ValueError("unit policy is outside the exact installed manifest")


class SignedTaskUnitRunPolicy(RuntimeContractModel):
    policy: TaskUnitRunPolicy
    signature: StrictStr = Field(min_length=88, max_length=88)

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


def load_task_unit_policy(
    path: Path, *, public_key_pem: bytes, manifest: OpsInstallManifest,
) -> TaskUnitRunPolicy:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > 64 * 1024:
            raise ValueError("unit policy requires a bounded regular file")
        payload = os.read(descriptor, 64 * 1024 + 1)
        after = os.fstat(descriptor)
        if len(payload) > 64 * 1024 or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("unit policy changed during read")
    finally:
        os.close(descriptor)
    signed = SignedTaskUnitRunPolicy.model_validate(strict_canonical_json_loads(payload))
    if payload != signed.canonical_bytes():
        raise ValueError("unit policy requires canonical JSON")
    try:
        signature = base64.b64decode(signed.signature, validate=True)
    except ValueError as exc:
        raise ValueError("unit policy signature is invalid") from exc
    if not verify_ed25519_signature(public_key_pem=public_key_pem, message=signed.policy.signing_bytes(), signature=signature):
        raise ValueError("unit policy signature is invalid")
    signed.policy.bind_manifest(manifest)
    return signed.policy


class TaskUnitRuntimeState(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    load_state: Literal["loaded", "not-found", "error", "masked", "merged", "stub"]
    active_state: Literal["inactive", "failed", "active", "activating", "deactivating", "reloading", "maintenance", "unknown"]
    invocation_id: InvocationId | None
    start_jobs: tuple[JobPath, ...] = Field(max_length=32)
    observed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_jobs(self) -> TaskUnitRuntimeState:
        if len(set(self.start_jobs)) != len(self.start_jobs):
            raise ValueError("unit state contains duplicate jobs")
        return self


def writer_window_allows(now: datetime) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("unit admission requires server UTC time")
    local = now.astimezone(_SHANGHAI)
    return not (local.weekday() < 5 and time(9, 15) <= local.time() <= time(15, 10))


def guard_task_unit_run(
    policy: TaskUnitRunPolicy, *, manifest: OpsInstallManifest,
    state: TaskUnitRuntimeState, unit: str, now: datetime,
) -> TaskUnitPolicyEntry:
    policy = TaskUnitRunPolicy.model_validate(policy)
    state = TaskUnitRuntimeState.model_validate(state)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("unit admission requires server UTC time")
    policy.bind_manifest(manifest)
    if not policy.enabled:
        raise ValueError("unit running is disabled")
    entry = next((entry for entry in policy.units if entry.unit == unit), None)
    if entry is None or not entry.enabled:
        raise ValueError("exact unit is absent or disabled in signed policy")
    if state.unit != unit or state.host_name != policy.host_name:
        raise ValueError("unit state differs from exact policy identity")
    age = (now - state.observed_at).total_seconds()
    if not 0 <= age < 120:
        raise ValueError("unit state is future or stale")
    if state.load_state != "loaded":
        raise ValueError("exact unit is not loaded")
    if state.active_state not in ("inactive", "failed") or state.start_jobs:
        raise ValueError("unit is running or has a pending start job")
    if not writer_window_allows(now) and entry.mode != "readonly":
        raise ValueError("market window admits readonly unit policy only")
    return entry


class TaskUnitJobWitness(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    job_path: JobPath
    job_id: StrictInt = Field(ge=1, le=2**32 - 1)
    caller_pid: StrictInt = Field(ge=1, le=2**31 - 1)
    sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    call_serial: StrictInt = Field(ge=1, le=2**32 - 1)
    reply_serial: StrictInt = Field(ge=1, le=2**32 - 1)
    call_monotonic_ns: Counter
    job_new_monotonic_ns: Counter
    reply_monotonic_ns: Counter
    timer_trigger_before_usec: Counter
    timer_trigger_after_usec: Counter
    competing_calls: tuple[StrictStr, ...] = Field(max_length=32)
    invocation_id: InvocationId

    @model_validator(mode="after")
    def validate_unique_start(self) -> TaskUnitJobWitness:
        if self.job_path != f"/org/freedesktop/systemd1/job/{self.job_id}" or self.call_serial != self.reply_serial:
            raise ValueError("StartUnit reply/job path differs from exact request witness")
        if not self.call_monotonic_ns <= self.job_new_monotonic_ns <= self.reply_monotonic_ns:
            raise ValueError("job creation is outside the observed StartUnit call")
        if self.timer_trigger_before_usec != self.timer_trigger_after_usec or self.competing_calls:
            raise ValueError("timer or external start makes invocation attribution ambiguous")
        return self


class TaskUnitAutomaticJobWitness(RuntimeContractModel):
    origin: Literal["timer", "external"]
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    job_path: JobPath
    job_id: StrictInt = Field(ge=1, le=2**32 - 1)
    job_new_monotonic_ns: Counter
    invocation_id: InvocationId
    timer_unit: StrictStr | None = Field(default=None, max_length=128)
    trigger_at: AwareUtcDatetime | None = None
    trigger_monotonic_ns: Counter | None = None

    @model_validator(mode="after")
    def validate_trigger(self) -> TaskUnitAutomaticJobWitness:
        if self.job_path != f"/org/freedesktop/systemd1/job/{self.job_id}":
            raise ValueError("automatic job path differs from original JobNew")
        if self.origin == "timer":
            if self.timer_unit != self.unit.removesuffix(".service") + ".timer" or self.trigger_at is None or self.trigger_monotonic_ns is None or self.trigger_monotonic_ns > self.job_new_monotonic_ns:
                raise ValueError("timer job requires its exact trigger witness")
        elif any(value is not None for value in (self.timer_unit, self.trigger_at, self.trigger_monotonic_ns)):
            raise ValueError("external job cannot claim a timer trigger")
        return self


class TaskUnitCompletionWitness(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    invocation_id: InvocationId
    job_path: JobPath
    job_id: StrictInt = Field(ge=1, le=2**32 - 1)
    job_removed_monotonic_ns: Counter
    job_result: Literal["done", "failed", "timeout", "canceled", "dependency", "skipped", "invalid", "assert", "unsupported", "collected", "once"]

    @model_validator(mode="after")
    def validate_path(self) -> TaskUnitCompletionWitness:
        if self.job_path != f"/org/freedesktop/systemd1/job/{self.job_id}":
            raise ValueError("completion witness differs from exact JobRemoved path")
        return self


class TaskUnitRunEvidence(RuntimeContractModel):
    origin: Literal["manual", "timer", "external"]
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    manifest_digest: Sha256
    invocation_id: InvocationId
    request_id: StrictStr | None = None
    request_hash: Sha256 | None = None
    job_witness: TaskUnitJobWitness | None = None
    automatic_witness: TaskUnitAutomaticJobWitness | None = None
    completion_witness: TaskUnitCompletionWitness | None = None
    started_at: AwareUtcDatetime
    started_monotonic_ns: Counter
    ended_at: AwareUtcDatetime | None = None
    ended_monotonic_ns: Counter | None = None
    result: Literal["success", "exit-code", "signal", "core-dump", "timeout", "watchdog", "start-limit-hit", "resources", "protocol", "oom-kill"] | None = None
    exec_status: StrictInt | None = Field(default=None, ge=0, le=255)
    observed_at: AwareUtcDatetime
    observed_monotonic_ns: Counter

    @model_validator(mode="after")
    def validate_same_invocation(self) -> TaskUnitRunEvidence:
        if self.origin == "manual":
            if self.request_id is None or self.request_hash is None or self.job_witness is None:
                raise ValueError("manual invocation requires the original request and job witness")
            if str(UUID(self.request_id)) != self.request_id:
                raise ValueError("manual request requires a canonical UUID")
            witness = self.job_witness
            if (self.host_name, self.boot_id, self.unit, self.invocation_id) != (witness.host_name, witness.boot_id, witness.unit, witness.invocation_id):
                raise ValueError("run invocation differs from original job witness")
            if self.started_monotonic_ns < witness.job_new_monotonic_ns:
                raise ValueError("run start precedes its job witness")
            if self.automatic_witness is not None:
                raise ValueError("manual run cannot reuse an automatic trigger witness")
        elif self.request_id is not None or self.request_hash is not None or self.job_witness is not None:
            raise ValueError("timer/external origin cannot reuse manual request witness")
        else:
            automatic = self.automatic_witness
            if automatic is None or (automatic.origin, automatic.host_name, automatic.boot_id, automatic.unit, automatic.invocation_id) != (self.origin, self.host_name, self.boot_id, self.unit, self.invocation_id):
                raise ValueError("automatic run requires its exact original trigger/job witness")
            if self.started_monotonic_ns < automatic.job_new_monotonic_ns or automatic.trigger_at is not None and automatic.trigger_at > self.started_at:
                raise ValueError("automatic run precedes its original trigger/job")
        fields = (self.ended_at, self.ended_monotonic_ns, self.result, self.exec_status)
        if any(value is not None for value in fields) and any(value is None for value in fields):
            raise ValueError("run end requires same-invocation exit facts")
        if self.started_at > self.observed_at:
            raise ValueError("run start is later than observation")
        if self.started_monotonic_ns > self.observed_monotonic_ns:
            raise ValueError("run start has a future monotonic fact")
        if self.ended_at is not None and self.ended_monotonic_ns is not None:
            completion = self.completion_witness
            original_job = self.job_witness if self.origin == "manual" else self.automatic_witness
            if completion is None or original_job is None or (completion.host_name, completion.boot_id, completion.unit, completion.invocation_id, completion.job_path, completion.job_id) != (self.host_name, self.boot_id, self.unit, self.invocation_id, original_job.job_path, original_job.job_id):
                raise ValueError("run completion requires the same original JobRemoved witness")
            if not self.ended_monotonic_ns <= completion.job_removed_monotonic_ns <= self.observed_monotonic_ns:
                raise ValueError("JobRemoved completion precedes the same-invocation exit")
            if self.ended_at < self.started_at or self.ended_at > self.observed_at or self.ended_monotonic_ns < self.started_monotonic_ns:
                raise ValueError("run end has a future fact or clock rollback")
            if self.result == "success" and self.exec_status != 0:
                raise ValueError("success result disagrees with process exit status")
            if self.result == "success" and completion.job_result != "done":
                raise ValueError("successful invocation disagrees with original job completion")
        elif self.completion_witness is not None:
            raise ValueError("completion witness has no same-invocation exit facts")
        return self

    @property
    def duration_ns(self) -> int | None:
        return None if self.ended_monotonic_ns is None else self.ended_monotonic_ns - self.started_monotonic_ns

    @property
    def status(self) -> Literal["started", "succeeded", "failed"]:
        if self.ended_at is None:
            return "started"
        return "succeeded" if self.result == "success" and self.exec_status == 0 else "failed"

    @property
    def material_hash(self) -> str:
        return canonical_sha256(self)


class TaskSystemdStartCall(RuntimeContractModel):
    sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    pid: StrictInt = Field(ge=1, le=2**31 - 1)
    serial: StrictInt = Field(ge=1, le=2**32 - 1)
    unit: UnitName
    mode: Literal["fail"]
    monotonic_ns: Counter


class TaskSystemdStartReply(RuntimeContractModel):
    sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    destination: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    reply_serial: StrictInt = Field(ge=1, le=2**32 - 1)
    job_path: JobPath
    monotonic_ns: Counter


class TaskSystemdJobEvent(RuntimeContractModel):
    sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    member: Literal["JobNew", "JobRemoved"]
    unit: UnitName
    job_id: StrictInt = Field(ge=1, le=2**32 - 1)
    job_path: JobPath
    monotonic_ns: Counter
    result: StrictStr | None = Field(default=None, max_length=32)

    @model_validator(mode="after")
    def validate_job(self) -> TaskSystemdJobEvent:
        if self.job_path != f"/org/freedesktop/systemd1/job/{self.job_id}":
            raise ValueError("systemd event path differs from its job ID")
        if (self.member == "JobRemoved") != (self.result is not None):
            raise ValueError("systemd JobRemoved requires its real result")
        return self


class TaskSystemdInvocationFacts(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    invocation_id: InvocationId
    started_at: AwareUtcDatetime
    started_monotonic_ns: Counter
    ended_at: AwareUtcDatetime | None = None
    ended_monotonic_ns: Counter | None = None
    result: StrictStr | None = Field(default=None, max_length=32)
    exec_status: StrictInt | None = Field(default=None, ge=0, le=255)


class TaskSystemdRunWindow(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: UnitName
    manifest_digest: Sha256
    request_id: StrictStr
    request_hash: Sha256
    caller_pid: StrictInt = Field(ge=1, le=2**31 - 1)
    systemd_sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    previous_invocation_id: InvocationId | None
    timer_before_usec: Counter
    timer_after_usec: Counter
    calls: tuple[TaskSystemdStartCall, ...] = Field(max_length=32)
    replies: tuple[TaskSystemdStartReply, ...] = Field(max_length=32)
    events: tuple[TaskSystemdJobEvent, ...] = Field(max_length=64)
    invocation: TaskSystemdInvocationFacts
    observed_at: AwareUtcDatetime
    observed_monotonic_ns: Counter

    @model_validator(mode="after")
    def validate_budget(self) -> TaskSystemdRunWindow:
        if len(self.model_dump_json().encode()) > 16 * 1024:
            raise ValueError("systemd witness exceeds 16 KiB")
        if str(UUID(self.request_id)) != self.request_id:
            raise ValueError("systemd witness requires its canonical original UUID")
        if any(item.monotonic_ns > self.observed_monotonic_ns for items in (self.calls, self.replies, self.events) for item in items):
            raise ValueError("systemd witness contains future raw events")
        return self


def bind_systemd_unit_run(window: TaskSystemdRunWindow) -> TaskUnitRunEvidence:
    window = TaskSystemdRunWindow.model_validate(window)
    facts = window.invocation
    if (facts.host_name, facts.boot_id, facts.unit) != (window.host_name, window.boot_id, window.unit) or facts.invocation_id == window.previous_invocation_id:
        raise ValueError("systemd invocation identity did not change in this exact window")
    calls = tuple(call for call in window.calls if call.unit == window.unit)
    if len(calls) != 1 or calls[0].pid != window.caller_pid:
        raise ValueError("systemd caller is absent or external start is ambiguous")
    call = calls[0]
    replies = tuple(reply for reply in window.replies if reply.sender == window.systemd_sender and reply.destination == call.sender and reply.reply_serial == call.serial)
    if len(replies) != 1:
        raise ValueError("systemd reply does not belong to the actual caller")
    reply = replies[0]
    events = tuple(event for event in window.events if event.sender == window.systemd_sender and event.unit == window.unit)
    created = tuple(event for event in events if event.member == "JobNew")
    if len(created) != 1 or created[0].job_path != reply.job_path:
        raise ValueError("systemd JobNew and caller reply are ambiguous")
    event = created[0]
    witness = TaskUnitJobWitness(host_name=window.host_name, boot_id=window.boot_id, unit=window.unit, job_path=event.job_path,
        job_id=event.job_id, caller_pid=call.pid, sender=call.sender, call_serial=call.serial, reply_serial=reply.reply_serial,
        call_monotonic_ns=call.monotonic_ns, job_new_monotonic_ns=event.monotonic_ns, reply_monotonic_ns=reply.monotonic_ns,
        timer_trigger_before_usec=window.timer_before_usec, timer_trigger_after_usec=window.timer_after_usec, competing_calls=(), invocation_id=facts.invocation_id)
    completion = None
    if facts.ended_at is not None:
        removed = tuple(item for item in events if item.member == "JobRemoved")
        if len(removed) != 1 or removed[0].job_path != event.job_path:
            raise ValueError("systemd completion lacks the same JobRemoved witness")
        completion = TaskUnitCompletionWitness(host_name=window.host_name, boot_id=window.boot_id, unit=window.unit,
            invocation_id=facts.invocation_id, job_path=event.job_path, job_id=event.job_id,
            job_removed_monotonic_ns=removed[0].monotonic_ns, job_result=removed[0].result)
    return TaskUnitRunEvidence(origin="manual", manifest_digest=window.manifest_digest, request_id=window.request_id,
        request_hash=window.request_hash, job_witness=witness, completion_witness=completion,
        observed_at=window.observed_at, observed_monotonic_ns=window.observed_monotonic_ns, **facts.model_dump())


class SystemdUnitAttempt(RuntimeContractModel):
    stage: Literal["unknown", "started", "completed"]
    job_path: JobPath | None = None
    run: TaskUnitRunEvidence | None = None
    window: TaskSystemdRunWindow | None = None
    reason: StrictStr | None = Field(default=None, max_length=256)

    @model_validator(mode="after")
    def validate_attempt(self) -> SystemdUnitAttempt:
        if self.stage == "unknown":
            if self.run is not None or self.window is not None or self.reason is None:
                raise ValueError("unknown unit attempt cannot claim invocation facts")
        elif self.run is None or self.window is None or bind_systemd_unit_run(self.window) != self.run or self.job_path != self.run.job_witness.job_path or (self.stage == "completed") != (self.run.ended_at is not None):
            raise ValueError("unit attempt differs from complete producer evidence")
        return self


def validate_task_window_extension(original: TaskSystemdRunWindow, current: TaskSystemdRunWindow) -> None:
    fixed = {"observed_at", "observed_monotonic_ns", "events", "invocation"}
    ending = {"ended_at", "ended_monotonic_ns", "result", "exec_status"}
    if (
        original.model_dump(exclude=fixed) != current.model_dump(exclude=fixed)
        or original.invocation.model_dump(exclude=ending) != current.invocation.model_dump(exclude=ending)
        or current.events[:len(original.events)] != original.events
        or current.observed_at < original.observed_at
        or current.observed_monotonic_ns < original.observed_monotonic_ns
        or original.invocation.ended_at is not None and original.invocation != current.invocation
    ):
        raise ValueError("later observation cannot replace original start facts or witness")


class _TaskSystemdMonitor:
    def __init__(self, unit: str) -> None:
        TypeAdapter(UnitName).validate_python(unit)
        matches = (
            f"--match=type='method_call',interface='org.freedesktop.systemd1.Manager',member='StartUnit',arg0='{unit}'",
            "--match=type='method_return',sender='org.freedesktop.systemd1'",
            *(f"--match=type='signal',sender='org.freedesktop.systemd1',interface='org.freedesktop.systemd1.Manager',member='{member}',arg2='{unit}'" for member in ("JobNew", "JobRemoved")),
        )
        self.unit = unit
        self.process = subprocess.Popen(("/usr/bin/busctl", "--system", "--json=short", "--augment-creds=yes", *matches, "monitor", "org.freedesktop.systemd1"), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={"LC_ALL": "C", "TZ": "UTC", "SYSTEMD_PAGER": ""})
        self.selector = selectors.DefaultSelector()
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.lines: list[bytes] = []
        self.byte_count = 0
        self.closed = False
        for stream in (self.process.stdout, self.process.stderr):
            assert stream is not None
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ)

    def drain(self, seconds: float) -> None:
        if self.closed:
            raise ValueError("systemd witness session is closed")
        for key, _mask in self.selector.select(max(0, seconds)):
            chunk = os.read(key.fd, 4096)
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            self.byte_count += len(chunk)
            if self.byte_count > 16 * 1024:
                raise ValueError("systemd witness exceeds 16 KiB")
            target = self.stdout if key.fileobj is self.process.stdout else self.stderr
            target.extend(chunk)
        while b"\n" in self.stdout:
            line, _, rest = self.stdout.partition(b"\n")
            self.stdout[:] = rest
            if line.strip():
                self.lines.append(bytes(line))
            if len(self.lines) > 64:
                raise ValueError("systemd witness exceeds finite message budget")

    def wait_ready(self) -> None:
        deadline = monotonic_clock.monotonic() + 1
        while monotonic_clock.monotonic() < deadline:
            self.drain(min(.05, deadline - monotonic_clock.monotonic()))
            if self.process.poll() is not None:
                raise PermissionError("systemd monitor unavailable before start")
            if b"Monitoring bus message stream." in self.stderr:
                return
        raise PermissionError("systemd monitor has no verified ready witness")

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.selector.close()
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=1)
        for stream in (self.process.stdout, self.process.stderr):
            if stream is not None:
                stream.close()


class _TaskSystemdBudget:
    def __init__(self) -> None:
        self.deadline = monotonic_clock.monotonic() + 5
        self.bytes = 0

    def remaining(self) -> float:
        remaining = self.deadline - monotonic_clock.monotonic()
        if remaining <= 0:
            raise TimeoutError("unit witness deadline expired")
        return remaining

    def account(self, count: int) -> None:
        self.bytes += count
        if self.bytes > 16 * 1024:
            raise ValueError("unit queries, call and witness exceed byte budget")

    def query(self, argv: tuple[str, ...], max_bytes: int) -> bytes:
        remaining_bytes = 16 * 1024 - self.bytes
        if remaining_bytes <= 0:
            raise ValueError("unit query byte budget expired")
        payload = _run_bounded(argv, min(1.0, self.remaining()), min(max_bytes, remaining_bytes))
        self.account(len(payload))
        return payload


class SystemdUnitRunExecutor:
    """Only the fixed busctl leaf; unrecognized witness material stays unknown."""

    def __init__(self, *, manifest_path: Path, policy_path: Path, manifest_public_key_pem: bytes,
                 policy_public_key_pem: bytes, clock: Callable[[], datetime] = lambda: datetime.now().astimezone()) -> None:
        self.manifest_path, self.policy_path = Path(manifest_path), Path(policy_path)
        self.manifest_public_key_pem, self.policy_public_key_pem = manifest_public_key_pem, policy_public_key_pem
        self.clock = clock

    def configuration(self) -> tuple[OpsInstallManifest, TaskUnitRunPolicy]:
        manifest, _digest = load_signed_ops_manifest(self.manifest_path, public_key_pem=self.manifest_public_key_pem, expected_host=socket.gethostname())
        policy = load_task_unit_policy(self.policy_path, public_key_pem=self.policy_public_key_pem, manifest=manifest)
        return manifest, policy

    @staticmethod
    def start_argv(unit: str) -> tuple[str, ...]:
        TypeAdapter(UnitName).validate_python(unit)
        return ("/usr/bin/busctl", "--system", "--json=short", "--timeout=5", "--allow-interactive-authorization=no", "call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "StartUnit", "ss", unit, "fail")

    def _show(self, unit: str, *, budget: _TaskSystemdBudget | None = None) -> dict[str, str]:
        TypeAdapter(UnitName).validate_python(unit)
        names = ("Id", "LoadState", "ActiveState", "InvocationID", "Job", "Result", "ExecMainStatus", "ExecMainStartTimestamp", "ExecMainExitTimestamp", "ExecMainStartTimestampMonotonic", "ExecMainExitTimestampMonotonic")
        argv = ("/usr/bin/systemctl", "show", unit, "--no-pager", "--property=" + ",".join(names))
        body = _run_bounded(argv, 1, 4096) if budget is None else budget.query(argv, 4096)
        result: dict[str, str] = {}
        for line in body.decode("utf-8").splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in names or key in result:
                raise ValueError("unit show requires exact complete fixed properties")
            result[key] = value
        if set(result) != set(names) or result["Id"] != unit:
            raise ValueError("unit show exact identity or properties are absent")
        return result

    def read_state(self, unit: str, *, budget: _TaskSystemdBudget | None = None) -> TaskUnitRuntimeState:
        manifest, policy = self.configuration()
        if unit not in {item.unit for item in policy.units}:
            raise ValueError("unit is outside the exact signed policy")
        boot = _bounded_proc_read("/proc/sys/kernel/random/boot_id", 128).decode("ascii").strip()
        props = self._show(unit, budget=budget)
        argv = ("/usr/bin/busctl", "--system", "--json=short", "--timeout=1", "call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "ListJobs")
        payload = strict_json_loads(_run_bounded(argv, 1, 16 * 1024) if budget is None else budget.query(argv, 16 * 1024))
        if not isinstance(payload, dict) or payload.get("type") != "a(usssoo)" or not isinstance(payload.get("data"), list) or len(payload["data"]) != 1 or not isinstance(payload["data"][0], list) or len(payload["data"][0]) > 128:
            raise ValueError("systemd ListJobs format or finite budget is unavailable")
        starts: list[str] = []
        for row in payload["data"][0]:
            if not isinstance(row, list) or len(row) != 6 or type(row[0]) is not int or not 1 <= row[0] <= 2**32 - 1 or not all(isinstance(item, str) for item in row[1:]):
                raise ValueError("systemd ListJobs contains invalid raw facts")
            if row[1] == unit and row[2] in ("start", "restart", "reload-or-start", "try-restart", "reload-or-restart"):
                starts.append(row[4])
        if boot != _bounded_proc_read("/proc/sys/kernel/random/boot_id", 128).decode("ascii").strip() or socket.gethostname() != manifest.host_name:
            raise ValueError("unit host/boot changed during read")
        return TaskUnitRuntimeState(host_name=manifest.host_name, boot_id=boot, unit=unit, load_state=props["LoadState"], active_state=props["ActiveState"], invocation_id=props["InvocationID"] or None, start_jobs=tuple(starts), observed_at=self.clock())

    @contextmanager
    def prepare(self, unit: str, *, now: datetime) -> Iterator[_TaskSystemdMonitor]:
        manifest, policy = self.configuration()
        guard_task_unit_run(policy, manifest=manifest, state=self.read_state(unit), unit=unit, now=now)
        session = _TaskSystemdMonitor(unit)
        try:
            session.wait_ready()
            yield session
        finally:
            session.close()

    def _timer_counter(self, unit: str, *, budget: _TaskSystemdBudget | None = None) -> int:
        timer = unit.removesuffix(".service") + ".timer"
        encoded = "".join(character if character.isascii() and character.isalnum() else f"_{ord(character):02x}" for character in timer)
        argv = ("/usr/bin/busctl", "--system", "--json=short", "--timeout=1", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/" + encoded, "org.freedesktop.systemd1.Timer", "LastTriggerUSecMonotonic")
        value = strict_json_loads(_run_bounded(argv, 1, 4096) if budget is None else budget.query(argv, 4096))
        if not isinstance(value, dict) or value.get("type") != "t" or not isinstance(value.get("data"), list) or len(value["data"]) != 1:
            raise ValueError("timer trigger counter is unavailable")
        return TypeAdapter(Counter).validate_python(value["data"][0])

    def invoke(self, *, command_id: str, request_hash: str, context_host: str, context_boot: str,
               manifest_digest: str, policy_digest: str, expected_state: TaskUnitRuntimeState,
               session: _TaskSystemdMonitor) -> SystemdUnitAttempt:
        if type(session) is not _TaskSystemdMonitor or session.closed:
            raise TypeError("start requires the same concrete live witness session")
        budget = _TaskSystemdBudget()
        budget.account(session.byte_count)
        accounted_monitor_bytes = session.byte_count
        manifest, policy = self.configuration()
        timer_before = self._timer_counter(session.unit, budget=budget)
        before = self.read_state(session.unit, budget=budget)
        guard_task_unit_run(policy, manifest=manifest, state=before, unit=session.unit, now=self.clock())
        if (manifest.host_name, before.boot_id, manifest.digest, policy.digest) != (context_host, context_boot, manifest_digest, policy_digest):
            raise ValueError("unit source/policy identity changed immediately before start")
        if before.model_dump(exclude={"observed_at"}) != TaskUnitRuntimeState.model_validate(expected_state).model_dump(exclude={"observed_at"}):
            raise ValueError("original complete runtime changed immediately before start")
        budget.remaining()
        process = subprocess.Popen(self.start_argv(session.unit), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env={"LC_ALL": "C", "TZ": "UTC", "SYSTEMD_PAGER": ""})
        assert process.stdout is not None
        os.set_blocking(process.stdout.fileno(), False)
        deadline = budget.deadline
        output = bytearray()
        job_path = None
        try:
            while monotonic_clock.monotonic() < deadline:
                session.drain(min(.05, max(0, deadline - monotonic_clock.monotonic())))
                budget.account(session.byte_count - accounted_monitor_bytes)
                accounted_monitor_bytes = session.byte_count
                try:
                    chunk = os.read(process.stdout.fileno(), 4096)
                except BlockingIOError:
                    chunk = b""
                output.extend(chunk)
                budget.account(len(chunk))
                if process.poll() is None:
                    continue
                if process.returncode != 0:
                    return SystemdUnitAttempt(stage="unknown", reason="start_reply_unavailable")
                reply = strict_json_loads(bytes(output))
                if not isinstance(reply, dict) or reply.get("type") != "o" or not isinstance(reply.get("data"), list) or len(reply["data"]) != 1:
                    raise ValueError("StartUnit has no exact job-path reply")
                job_path = TypeAdapter(JobPath).validate_python(reply["data"][0])
                props = self._show(session.unit, budget=budget)
                if not props["InvocationID"] or props["InvocationID"] == before.invocation_id:
                    continue
                after = self.read_state(session.unit, budget=budget)
                facts = self._invocation_facts(props, after)
                calls, replies, events = self._messages(session)
                if not calls or not replies or not events:
                    continue
                window = TaskSystemdRunWindow(host_name=manifest.host_name, boot_id=after.boot_id, unit=session.unit, manifest_digest=manifest.digest,
                    request_id=command_id, request_hash=request_hash, caller_pid=process.pid, systemd_sender=events[0].sender,
                    previous_invocation_id=before.invocation_id, timer_before_usec=timer_before, timer_after_usec=self._timer_counter(session.unit, budget=budget),
                    calls=calls, replies=replies, events=events, invocation=facts, observed_at=self.clock(), observed_monotonic_ns=monotonic_clock.monotonic_ns())
                run = bind_systemd_unit_run(window)
                budget.remaining()
                if run.job_witness.job_path != job_path:
                    raise ValueError("actual StartUnit reply differs from invocation producer")
                return SystemdUnitAttempt(stage="completed" if run.ended_at is not None else "started", job_path=job_path, run=run, window=window)
            return SystemdUnitAttempt(stage="unknown", job_path=job_path, reason="witness_deadline")
        except (OSError, ValueError, TimeoutError):
            return SystemdUnitAttempt(stage="unknown", job_path=job_path, reason="witness_unavailable")
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=1)
            process.stdout.close()

    def observe(self, original: TaskSystemdRunWindow, *, policy_digest: str) -> SystemdUnitAttempt:
        original = TaskSystemdRunWindow.model_validate(original)
        bind_systemd_unit_run(original)
        budget = _TaskSystemdBudget()
        budget.account(len(original.model_dump_json().encode()))
        session = None
        latest: SystemdUnitAttempt | None = None
        try:
            manifest, policy = self.configuration()
            if (manifest.host_name, manifest.digest, policy.digest) != (original.host_name, original.manifest_digest, policy_digest) or original.unit not in {item.unit for item in policy.units}:
                raise ValueError("original unit observation configuration differs")
            session = _TaskSystemdMonitor(original.unit)
            session.wait_ready()
            accounted = 0
            while monotonic_clock.monotonic() < budget.deadline:
                session.drain(min(.05, budget.remaining()))
                budget.account(session.byte_count - accounted)
                accounted = session.byte_count
                props = self._show(original.unit, budget=budget)
                current = self.read_state(original.unit, budget=budget)
                if (current.host_name, current.boot_id, current.invocation_id) != (original.host_name, original.boot_id, original.invocation.invocation_id):
                    raise ValueError("original unit invocation changed during observation")
                facts = self._invocation_facts(props, current)
                _calls, _replies, events = self._messages(session)
                if _calls or any(event.member == "JobNew" and event.unit == original.unit for event in events):
                    raise ValueError("original unit observation has a competing start")
                terminal = tuple(event for event in events if event.member == "JobRemoved" and event.unit == original.unit)
                window = TaskSystemdRunWindow.model_validate(original.model_dump() | {
                    "events": original.events + terminal, "invocation": facts,
                    "timer_after_usec": self._timer_counter(original.unit, budget=budget),
                    "observed_at": self.clock(), "observed_monotonic_ns": monotonic_clock.monotonic_ns()})
                validate_task_window_extension(original, window)
                run = bind_systemd_unit_run(window)
                latest = SystemdUnitAttempt(stage="completed" if run.ended_at is not None else "started",
                    job_path=run.job_witness.job_path, run=run, window=window)
                if latest.stage == "completed":
                    return latest
            return latest or SystemdUnitAttempt(stage="unknown", reason="original_observation_unavailable")
        except (OSError, ValueError, TimeoutError, PermissionError):
            return SystemdUnitAttempt(stage="unknown", reason="original_observation_unavailable")
        finally:
            if session is not None:
                session.close()

    @staticmethod
    def _invocation_facts(props: dict[str, str], state: TaskUnitRuntimeState) -> TaskSystemdInvocationFacts:
        started, ended = _timestamp(props["ExecMainStartTimestamp"]), _timestamp(props["ExecMainExitTimestamp"])
        def counter(name: str) -> int:
            text = props[name]
            if not re.fullmatch(r"[0-9]{1,16}", text):
                raise ValueError("unit monotonic timestamp is unavailable")
            return TypeAdapter(Counter).validate_python(int(text) * 1000)

        if started is None or state.invocation_id != props["InvocationID"]:
            raise ValueError("unit start/show invocation identity differs")
        exit_text = props["ExecMainStatus"]
        if ended is not None and not re.fullmatch(r"[0-9]{1,3}", exit_text):
            raise ValueError("unit process exit status is unavailable")
        return TaskSystemdInvocationFacts(host_name=state.host_name, boot_id=state.boot_id, unit=state.unit, invocation_id=state.invocation_id,
            started_at=started, started_monotonic_ns=counter("ExecMainStartTimestampMonotonic"), ended_at=ended,
            ended_monotonic_ns=None if ended is None else counter("ExecMainExitTimestampMonotonic"), result=None if ended is None else props["Result"], exec_status=None if ended is None else int(exit_text))

    @staticmethod
    def _messages(session: _TaskSystemdMonitor) -> tuple[tuple[TaskSystemdStartCall, ...], tuple[TaskSystemdStartReply, ...], tuple[TaskSystemdJobEvent, ...]]:
        calls: list[TaskSystemdStartCall] = []
        replies: list[TaskSystemdStartReply] = []
        events: list[TaskSystemdJobEvent] = []
        for line in session.lines:
            raw = strict_json_loads(line)
            if not isinstance(raw, dict) or not isinstance(raw.get("payload"), dict):
                raise ValueError("systemd monitor JSON format is unverified")
            payload = raw["payload"]
            data = payload.get("data")
            stamp = raw.get("monotonic_usec")
            if type(stamp) is not int or not 0 <= stamp <= (2**63 - 1) // 1000 or not isinstance(data, list):
                raise ValueError("systemd monitor lacks original monotonic metadata")
            common = {"sender": raw.get("sender"), "monotonic_ns": stamp * 1000}
            if raw.get("type") == "method_call":
                creds = raw.get("credentials")
                if raw.get("member") != "StartUnit" or raw.get("interface") != "org.freedesktop.systemd1.Manager" or payload.get("type") != "ss" or len(data) != 2 or not isinstance(creds, dict):
                    raise ValueError("systemd call lacks the exact caller credential witness")
                calls.append(TaskSystemdStartCall(**common, pid=creds.get("pid"), serial=raw.get("cookie"), unit=data[0], mode=data[1]))
            elif raw.get("type") == "method_return":
                if payload.get("type") != "o" or len(data) != 1 or not isinstance(data[0], str) or not data[0].startswith("/org/freedesktop/systemd1/job/"):
                    continue
                replies.append(TaskSystemdStartReply(**common, destination=raw.get("destination"), reply_serial=raw.get("reply_cookie"), job_path=data[0]))
            elif raw.get("type") == "signal":
                member = raw.get("member")
                if raw.get("interface") != "org.freedesktop.systemd1.Manager" or member not in ("JobNew", "JobRemoved") or payload.get("type") != ("uos" if member == "JobNew" else "uoss") or len(data) != (3 if member == "JobNew" else 4):
                    raise ValueError("systemd job signal lacks exact bounded metadata")
                events.append(TaskSystemdJobEvent(**common, member=member, job_id=data[0], job_path=data[1], unit=data[2], result=None if member == "JobNew" else data[3]))
            else:
                raise ValueError("systemd monitor contains an unsupported message")
        return tuple(calls), tuple(replies), tuple(events)
