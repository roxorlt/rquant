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
from rquant.notifier_operator import (
    MANUAL_SERVICE, MANUAL_TIMER, SERVICE_PROPERTIES, TIMER_PROPERTIES,
    ManualServiceReadReceipt, NotifierManualServiceBinding,
    capture_manual_service_receipt, guard_manual_service_run, load_notifier_manual_install,
)

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


class ManualServiceJobWitness(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: Literal["rquant-notify-test.service"] = MANUAL_SERVICE
    job_path: JobPath
    job_id: StrictInt = Field(ge=1, le=2**32 - 1)
    caller_pid: StrictInt = Field(ge=1, le=2**31 - 1)
    sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    call_serial: StrictInt = Field(ge=1, le=2**32 - 1)
    reply_serial: StrictInt = Field(ge=1, le=2**32 - 1)
    call_monotonic_ns: Counter
    job_new_monotonic_ns: Counter
    reply_monotonic_ns: Counter
    invocation_id: InvocationId
    installation_digest: Sha256
    before_material_sha256: Sha256
    after_material_sha256: Sha256

    @model_validator(mode="after")
    def exact_start(self) -> ManualServiceJobWitness:
        if self.job_path != f"/org/freedesktop/systemd1/job/{self.job_id}" or self.call_serial != self.reply_serial:
            raise ValueError("manual reply and original job identity differ")
        if not self.call_monotonic_ns <= self.job_new_monotonic_ns <= self.reply_monotonic_ns:
            raise ValueError("manual job is outside the original call/reply window")
        return self


class ManualServiceRunEvidence(TaskUnitRunEvidence):
    contract: Literal["rquant.manual-service-run/v1"] = "rquant.manual-service-run/v1"
    origin: Literal["manual"] = "manual"
    unit: Literal["rquant-notify-test.service"] = MANUAL_SERVICE
    job_witness: ManualServiceJobWitness
    automatic_witness: None = None


class ManualServiceRunWindow(RuntimeContractModel):
    contract: Literal["rquant.manual-service-window/v1"] = "rquant.manual-service-window/v1"
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    unit: Literal["rquant-notify-test.service"] = MANUAL_SERVICE
    manifest_digest: Sha256
    request_id: StrictStr
    request_hash: Sha256
    caller_pid: StrictInt = Field(ge=1, le=2**31 - 1)
    systemd_sender: StrictStr = Field(pattern=r"^:[0-9]{1,10}\.[0-9]{1,10}$")
    previous_invocation_id: InvocationId | None
    before_receipt: ManualServiceReadReceipt
    after_receipt: ManualServiceReadReceipt
    calls: tuple[TaskSystemdStartCall, ...] = Field(max_length=32)
    replies: tuple[TaskSystemdStartReply, ...] = Field(max_length=32)
    events: tuple[TaskSystemdJobEvent, ...] = Field(max_length=64)
    invocation: TaskSystemdInvocationFacts
    observed_at: AwareUtcDatetime
    observed_monotonic_ns: Counter

    @model_validator(mode="after")
    def bounded_window(self) -> ManualServiceRunWindow:
        if str(UUID(self.request_id)) != self.request_id or len(self.model_dump_json().encode()) > 16 * 1024:
            raise ValueError("manual witness requires canonical UUID and original 16 KiB budget")
        if any(item.monotonic_ns > self.observed_monotonic_ns for items in (self.calls, self.replies, self.events) for item in items):
            raise ValueError("manual witness has future raw events")
        return self


def bind_manual_service_run(window: ManualServiceRunWindow) -> ManualServiceRunEvidence:
    window = ManualServiceRunWindow.model_validate(window)
    before, after, facts = window.before_receipt, window.after_receipt, window.invocation
    guard_manual_service_run(before, now=window.observed_at)
    identity = (window.host_name, window.boot_id, window.unit, window.manifest_digest)
    for receipt in (before, after):
        state, material = receipt.runtime_state, receipt.material
        if identity != (state.host_name, state.boot_id, state.unit, material.ops_manifest_digest):
            raise ValueError("manual original receipt differs from the run identity")
    fixed = {"service_show", "list_jobs", "observed_at", "observed_monotonic_ns"}
    if before.material.model_dump(exclude=fixed) != after.material.model_dump(exclude=fixed):
        raise ValueError("manual installation or actual timer absence changed during start")
    if (window.previous_invocation_id != before.runtime_state.invocation_id
            or facts.invocation_id == window.previous_invocation_id
            or (facts.host_name, facts.boot_id, facts.unit, facts.invocation_id) != (
                window.host_name, window.boot_id, window.unit, after.runtime_state.invocation_id)):
        raise ValueError("manual invocation differs from the original complete read")
    if (after.material.observed_at > window.observed_at
            or not before.material.observed_monotonic_ns <= after.material.observed_monotonic_ns <= window.observed_monotonic_ns):
        raise ValueError("manual receipts have future or reversed times")
    props = {}
    for line in after.material.service_show.splitlines():
        key, _, value = line.partition("=")
        props[key] = value
    if SystemdUnitRunExecutor._invocation_facts(props, after.runtime_state) != facts:
        raise ValueError("manual invocation fields differ from the actual same-read manager values")
    calls = tuple(call for call in window.calls if call.unit == window.unit)
    if len(calls) != 1 or len(window.calls) != 1 or calls[0].pid != window.caller_pid:
        raise ValueError("manual caller is absent or competing")
    call = calls[0]
    if call.monotonic_ns < before.material.observed_monotonic_ns:
        raise ValueError("manual call precedes its actual idle/absence read")
    replies = tuple(reply for reply in window.replies if reply.sender == window.systemd_sender
                    and reply.destination == call.sender and reply.reply_serial == call.serial)
    created = tuple(event for event in window.events if event.member == "JobNew")
    if len(replies) != 1 or len(created) != 1 or any(event.sender != window.systemd_sender or event.unit != window.unit for event in window.events):
        raise ValueError("manual reply or JobNew is ambiguous")
    reply, event = replies[0], created[0]
    if event.job_path != reply.job_path:
        raise ValueError("manual reply and JobNew differ")
    witness = ManualServiceJobWitness(host_name=window.host_name, boot_id=window.boot_id, unit=window.unit,
        job_path=event.job_path, job_id=event.job_id, caller_pid=call.pid, sender=call.sender,
        call_serial=call.serial, reply_serial=reply.reply_serial, call_monotonic_ns=call.monotonic_ns,
        job_new_monotonic_ns=event.monotonic_ns, reply_monotonic_ns=reply.monotonic_ns,
        invocation_id=facts.invocation_id, installation_digest=before.install_digest,
        before_material_sha256=before.material_sha256, after_material_sha256=after.material_sha256)
    completion = None
    removed = tuple(item for item in window.events if item.member == "JobRemoved")
    if facts.ended_at is not None:
        if len(removed) != 1 or removed[0].job_path != event.job_path:
            raise ValueError("manual completion lacks the original JobRemoved")
        completion = TaskUnitCompletionWitness(host_name=window.host_name, boot_id=window.boot_id, unit=window.unit,
            invocation_id=facts.invocation_id, job_path=event.job_path, job_id=event.job_id,
            job_removed_monotonic_ns=removed[0].monotonic_ns, job_result=removed[0].result)
    elif removed:
        raise ValueError("manual completion has no corresponding process exit")
    return ManualServiceRunEvidence(manifest_digest=window.manifest_digest, request_id=window.request_id,
        request_hash=window.request_hash, job_witness=witness, completion_witness=completion,
        observed_at=window.observed_at, observed_monotonic_ns=window.observed_monotonic_ns, **facts.model_dump())


def bind_unit_run(window: TaskSystemdRunWindow | ManualServiceRunWindow) -> TaskUnitRunEvidence | ManualServiceRunEvidence:
    return bind_manual_service_run(window) if isinstance(window, ManualServiceRunWindow) else bind_systemd_unit_run(window)


class SystemdUnitAttempt(RuntimeContractModel):
    stage: Literal["unknown", "started", "completed"]
    job_path: JobPath | None = None
    run: TaskUnitRunEvidence | ManualServiceRunEvidence | None = None
    window: TaskSystemdRunWindow | ManualServiceRunWindow | None = None
    reason: StrictStr | None = Field(default=None, max_length=256)

    @model_validator(mode="after")
    def validate_attempt(self) -> SystemdUnitAttempt:
        if self.stage == "unknown":
            if self.run is not None or self.window is not None or self.reason is None:
                raise ValueError("unknown unit attempt cannot claim invocation facts")
        elif self.run is None or self.window is None or bind_unit_run(self.window) != self.run or self.job_path != self.run.job_witness.job_path or (self.stage == "completed") != (self.run.ended_at is not None):
            raise ValueError("unit attempt differs from complete producer evidence")
        return self


def validate_task_window_extension(original: TaskSystemdRunWindow | ManualServiceRunWindow,
                                   current: TaskSystemdRunWindow | ManualServiceRunWindow) -> None:
    if type(original) is not type(current):
        raise ValueError("later observation cannot change the original run variant")
    fixed = {"observed_at", "observed_monotonic_ns", "events", "invocation"}
    if isinstance(original, ManualServiceRunWindow):
        fixed.add("after_receipt")
        previous, latest = original.after_receipt.material, current.after_receipt.material
        fields = {"service_show", "list_jobs", "observed_at", "observed_monotonic_ns"}
        if previous.model_dump(exclude=fields) != latest.model_dump(exclude=fields):
            raise ValueError("manual observation cannot replace installation/absence facts")
        if latest.observed_at < previous.observed_at or latest.observed_monotonic_ns < previous.observed_monotonic_ns:
            raise ValueError("manual observation cannot reverse its actual read time")
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
    def __init__(self, unit: str, *, manual: bool = False) -> None:
        TypeAdapter(UnitName).validate_python(unit)
        matches = (
            f"--match=type='method_call',interface='org.freedesktop.systemd1.Manager',member='StartUnit',arg0='{unit}'",
            "--match=type='method_return',sender='org.freedesktop.systemd1'",
            *(f"--match=type='signal',sender='org.freedesktop.systemd1',interface='org.freedesktop.systemd1.Manager',member='{member}',arg2='{unit}'" for member in ("JobNew", "JobRemoved")),
        )
        if manual:
            if unit != MANUAL_SERVICE:
                raise ValueError("manual witness requires the exact signed notification service")
            matches = (
                "--match=type='method_call',interface='org.freedesktop.systemd1.Manager'",
                "--match=type='method_return',sender='org.freedesktop.systemd1'",
                "--match=type='signal',sender='org.freedesktop.systemd1',interface='org.freedesktop.systemd1.Manager'",
            )
        self.manual = manual
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
                 policy_public_key_pem: bytes, clock: Callable[[], datetime] = lambda: datetime.now().astimezone(),
                 manual_binding: NotifierManualServiceBinding | None = None) -> None:
        self.manifest_path, self.policy_path = Path(manifest_path), Path(policy_path)
        self.manifest_public_key_pem, self.policy_public_key_pem = manifest_public_key_pem, policy_public_key_pem
        self.clock = clock
        if manual_binding is not None and type(manual_binding) is not NotifierManualServiceBinding:
            raise TypeError("manual unit binding must come from the original bootstrap")
        self.manual_binding = manual_binding

    def configuration(self) -> tuple[OpsInstallManifest, TaskUnitRunPolicy]:
        manifest, _digest = load_signed_ops_manifest(self.manifest_path, public_key_pem=self.manifest_public_key_pem, expected_host=socket.gethostname())
        policy = load_task_unit_policy(self.policy_path, public_key_pem=self.policy_public_key_pem, manifest=manifest)
        return manifest, policy

    def manual_configuration(self) -> tuple[OpsInstallManifest, object]:
        if self.manual_binding is None:
            raise ValueError("manual notification service is not configured")
        manifest, _policy = self.configuration()
        binding = self.manual_binding
        install, _source = load_notifier_manual_install(binding.install_path, public_key_pem=binding.public_key_pem,
            manifest=manifest, expected_profile_sha256=binding.profile_sha256,
            expected_runtime_commit=binding.runtime_commit, expected_uid=binding.expected_uid)
        return manifest, install

    def read_manual_state(self, *, budget: _TaskSystemdBudget | None = None) -> ManualServiceReadReceipt:
        budget = budget or _TaskSystemdBudget()
        boot = _bounded_proc_read("/proc/sys/kernel/random/boot_id", 128).decode("ascii").strip()
        manifest, _install = self.manual_configuration()
        binding = self.manual_binding
        assert binding is not None
        show = budget.query(("/usr/bin/systemctl", "show", MANUAL_SERVICE, "--no-pager",
                             "--property=" + ",".join(SERVICE_PROPERTIES)), 4096)
        timer = budget.query(("/usr/bin/systemctl", "show", MANUAL_TIMER, "--no-pager",
                              "--property=" + ",".join(TIMER_PROPERTIES)), 1024)
        jobs = budget.query(("/usr/bin/busctl", "--system", "--json=short", "--timeout=1", "call",
                             "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
                             "org.freedesktop.systemd1.Manager", "ListJobs"), 16 * 1024)
        after = _bounded_proc_read("/proc/sys/kernel/random/boot_id", 128).decode("ascii").strip()
        receipt = capture_manual_service_receipt(install_path=binding.install_path, public_key_pem=binding.public_key_pem,
            manifest=manifest, expected_profile_sha256=binding.profile_sha256,
            expected_runtime_commit=binding.runtime_commit, expected_host=socket.gethostname(),
            boot_before=boot, boot_after=after, service_show=show, timer_show=timer, list_jobs=jobs,
            observed_at=self.clock(), observed_monotonic_ns=monotonic_clock.monotonic_ns(), expected_uid=binding.expected_uid)
        budget.account(receipt.material.install_source.size + receipt.material.fragment_source.size)
        budget.remaining()
        return receipt

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
        if unit == MANUAL_SERVICE:
            return self.read_manual_state(budget=budget).runtime_state
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
        if unit == MANUAL_SERVICE:
            guard_manual_service_run(self.read_manual_state(), now=now)
            session = _TaskSystemdMonitor(unit, manual=True)
        else:
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
               manifest_digest: str, policy_digest: str, expected_state: TaskUnitRuntimeState | ManualServiceReadReceipt,
               session: _TaskSystemdMonitor) -> SystemdUnitAttempt:
        if type(session) is not _TaskSystemdMonitor or session.closed:
            raise TypeError("start requires the same concrete live witness session")
        budget = _TaskSystemdBudget()
        budget.account(session.byte_count)
        accounted_monitor_bytes = session.byte_count
        manifest, policy = self.configuration()
        manual = session.unit == MANUAL_SERVICE
        before_receipt = self.read_manual_state(budget=budget) if manual else None
        timer_before = None if manual else self._timer_counter(session.unit, budget=budget)
        before = before_receipt.runtime_state if manual else self.read_state(session.unit, budget=budget)
        if manual:
            if not session.manual or type(expected_state) is not ManualServiceReadReceipt:
                raise ValueError("manual start requires the original complete receipt and monitor")
            install = guard_manual_service_run(before_receipt, now=self.clock())
            actual_policy_digest = install.digest
            fields = {"service_show", "list_jobs", "observed_at", "observed_monotonic_ns"}
            if (before_receipt.material.model_dump(exclude=fields) != expected_state.material.model_dump(exclude=fields)
                    or before.model_dump(exclude={"observed_at"}) != expected_state.runtime_state.model_dump(exclude={"observed_at"})
                    or before_receipt.main_pid != expected_state.main_pid):
                raise ValueError("manual installation or runtime changed immediately before start")
        else:
            guard_task_unit_run(policy, manifest=manifest, state=before, unit=session.unit, now=self.clock())
            actual_policy_digest = policy.digest
        if (manifest.host_name, before.boot_id, manifest.digest, actual_policy_digest) != (context_host, context_boot, manifest_digest, policy_digest):
            raise ValueError("unit source/policy identity changed immediately before start")
        if not manual and before.model_dump(exclude={"observed_at"}) != TaskUnitRuntimeState.model_validate(expected_state).model_dump(exclude={"observed_at"}):
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
                after_receipt = self.read_manual_state(budget=budget) if manual else None
                props = dict(line.split("=", 1) for line in after_receipt.material.service_show.splitlines()) if manual else self._show(session.unit, budget=budget)
                if not props["InvocationID"] or props["InvocationID"] == before.invocation_id:
                    continue
                after = after_receipt.runtime_state if manual else self.read_state(session.unit, budget=budget)
                facts = self._invocation_facts(props, after)
                calls, replies, events = self._messages(session)
                if not calls or not replies or not events:
                    continue
                common = dict(host_name=manifest.host_name, boot_id=after.boot_id, unit=session.unit, manifest_digest=manifest.digest,
                    request_id=command_id, request_hash=request_hash, caller_pid=process.pid, systemd_sender=events[0].sender,
                    previous_invocation_id=before.invocation_id,
                    calls=calls, replies=replies, events=events, invocation=facts, observed_at=self.clock(), observed_monotonic_ns=monotonic_clock.monotonic_ns())
                window = (ManualServiceRunWindow(**common, before_receipt=before_receipt, after_receipt=after_receipt) if manual
                    else TaskSystemdRunWindow(**common, timer_before_usec=timer_before, timer_after_usec=self._timer_counter(session.unit, budget=budget)))
                run = bind_unit_run(window)
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

    def observe(self, original: TaskSystemdRunWindow | ManualServiceRunWindow, *, policy_digest: str) -> SystemdUnitAttempt:
        manual = isinstance(original, ManualServiceRunWindow)
        original = (ManualServiceRunWindow if manual else TaskSystemdRunWindow).model_validate(original)
        bind_unit_run(original)
        budget = _TaskSystemdBudget()
        budget.account(len(original.model_dump_json().encode()))
        session = None
        latest: SystemdUnitAttempt | None = None
        try:
            manifest, policy = self.configuration()
            if manual:
                _actual_manifest, install = self.manual_configuration()
                binding = self.manual_binding
                assert binding is not None
                for receipt in (original.before_receipt, original.after_receipt):
                    receipt.verify_source(public_key_pem=binding.public_key_pem, manifest=manifest,
                        profile_sha256=binding.profile_sha256, runtime_commit=binding.runtime_commit)
                if (manifest.host_name, manifest.digest, install.digest) != (original.host_name, original.manifest_digest, policy_digest):
                    raise ValueError("original manual installation observation differs")
            elif (manifest.host_name, manifest.digest, policy.digest) != (original.host_name, original.manifest_digest, policy_digest) or original.unit not in {item.unit for item in policy.units}:
                raise ValueError("original unit observation configuration differs")
            session = _TaskSystemdMonitor(original.unit, manual=True) if manual else _TaskSystemdMonitor(original.unit)
            session.wait_ready()
            accounted = 0
            while monotonic_clock.monotonic() < budget.deadline:
                session.drain(min(.05, budget.remaining()))
                budget.account(session.byte_count - accounted)
                accounted = session.byte_count
                receipt = self.read_manual_state(budget=budget) if manual else None
                props = dict(line.split("=", 1) for line in receipt.material.service_show.splitlines()) if manual else self._show(original.unit, budget=budget)
                current = receipt.runtime_state if manual else self.read_state(original.unit, budget=budget)
                if (current.host_name, current.boot_id, current.invocation_id) != (original.host_name, original.boot_id, original.invocation.invocation_id):
                    raise ValueError("original unit invocation changed during observation")
                facts = self._invocation_facts(props, current)
                _calls, _replies, events = self._messages(session)
                if _calls or any(event.member == "JobNew" and event.unit == original.unit for event in events):
                    raise ValueError("original unit observation has a competing start")
                terminal = tuple(event for event in events if event.member == "JobRemoved" and event.unit == original.unit)
                changes = {
                    "events": original.events + terminal, "invocation": facts,
                    "observed_at": self.clock(), "observed_monotonic_ns": monotonic_clock.monotonic_ns()}
                if manual:
                    changes["after_receipt"] = receipt
                    window = ManualServiceRunWindow.model_validate(original.model_dump() | changes)
                else:
                    changes["timer_after_usec"] = self._timer_counter(original.unit, budget=budget)
                    window = TaskSystemdRunWindow.model_validate(original.model_dump() | changes)
                validate_task_window_extension(original, window)
                run = bind_unit_run(window)
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
                if getattr(session, "manual", False) and raw.get("interface") == "org.freedesktop.systemd1.Manager":
                    if raw.get("member") in {"GetUnit", "GetUnitByPID", "GetUnitFileState", "ListJobs", "ListUnits", "ListUnitFiles"}:
                        continue
                    if raw.get("member") != "StartUnit" or not data or data[0] != MANUAL_SERVICE:
                        raise ValueError("manual window has a competing or unverified Manager mutation")
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
                if getattr(session, "manual", False) and member in ("JobNew", "JobRemoved") and len(data) >= 3 and data[2] != MANUAL_SERVICE:
                    continue
                if raw.get("interface") != "org.freedesktop.systemd1.Manager" or member not in ("JobNew", "JobRemoved") or payload.get("type") != ("uos" if member == "JobNew" else "uoss") or len(data) != (3 if member == "JobNew" else 4):
                    raise ValueError("systemd job signal lacks exact bounded metadata")
                events.append(TaskSystemdJobEvent(**common, member=member, job_id=data[0], job_path=data[1], unit=data[2], result=None if member == "JobNew" else data[3]))
            else:
                raise ValueError("systemd monitor contains an unsupported message")
        return tuple(calls), tuple(replies), tuple(events)
