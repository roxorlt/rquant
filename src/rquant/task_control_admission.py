"""One verified Serving generation feeds the private task admission boundary."""

from __future__ import annotations

import re
import socket
from collections.abc import Callable
from datetime import datetime
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from threading import Lock
from typing import Annotated, Literal

from pydantic import Field, StrictBool, StrictStr, model_validator

from rquant.lab_scheduling_control import LabSchedulingControlState
from rquant.delivery_contracts import NotificationRuntimeWindow
from rquant.ops_status import OpsSnapshot
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_publisher import ServingReader
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.task_center_projection import TaskOpsEvidence, TaskOpsSample, scheduling_projection, task_ops_projections
from rquant.task_cpu import SLICES, TaskCpuResult, parse_cpu_list
from rquant.task_unit_control import TaskUnitRunEvidence
from rquant.web.serving import BorrowedGeneration
from rquant.page_control import PageControlReceipt, PageControlService, PageControlStatus, PageControlCommandConflictError
from rquant.task_control import TaskControlPageControlBackend, TaskControlSubmissionReference, TaskUnitConfirmation, TaskUnitEffect
from rquant.task_control_commands import (OwnedTaskControl, OwnedPrepareUnitRun, OwnedRequestUnitRun, OwnedSetLabSchedulingPaused, TaskControlRequest, TaskControlIdentity,
    OwnedPrepareNotifierDeliveryMode, OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled, NotifierModeConfirmation)
from rquant.notifier_operator import NotifierModeState, MonitorBuiltinControlState, inspect_monitor_control_installation, read_monitor_control_state
from rquant.lab_scheduling_control import LabSchedulingSubmission
from rquant.factor_definition_admission import FactorDefinitionAdmissionClient, FactorDefinitionAdmissionServer, _ACTOR_ADAPTER, _UnixHTTPConnection, _peer_uid, build_factor_definition_admission_server
from rquant.strict_json import canonical_json_bytes, strict_json_loads


class TaskCenterServingView(RuntimeContractModel):
    generation_id: StrictStr = Field(min_length=1, max_length=128)
    ops_generation_id: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot: OpsSnapshot
    cpu: tuple[TaskCpuResult, ...] = Field(min_length=5, max_length=5)
    runs: tuple[TaskUnitRunEvidence, ...] = Field(max_length=32)
    scheduling_control: LabSchedulingControlState | None = None
    notification_runtime: NotificationRuntimeWindow | None = None

    @property
    def material_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json", exclude={"notification_runtime"})
            if self.notification_runtime is None else self)


def _task_projection(borrowed: BorrowedGeneration, name: Literal["ops_task_cpu", "ops_task_runs", "lab_scheduler_control"]) -> ServingProjectionPayload | None:
    contract = PAGE_PROJECTION_CONTRACTS[name]
    rows = borrowed.cursor.execute("SELECT available,row_count,owner_dataset_id,owner_generation_id,available_at FROM projection_status WHERE table_name=? LIMIT 2", (name,)).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError("task projection has duplicate status")
    available, count, owner, generation, at = rows[0]
    if type(available) is not bool or type(count) is not int or not 0 <= count <= contract.max_rows or owner != contract.owner_dataset_id or count != borrowed.manifest.row_counts.get(name):
        raise ValueError("task projection original owner or row budget differs")
    if not available:
        if count or generation is not None or at is not None:
            raise ValueError("unavailable task projection contains facts")
        return None
    watermark = next((item for item in borrowed.manifest.watermarks if item.dataset_id == owner), None)
    if watermark is None or watermark.status is not FreshnessStatus.FRESH or generation != watermark.generation_id or at is None or at > borrowed.manifest.built_at:
        raise ValueError("task projection generation or cutoff differs")
    physical = borrowed.cursor.execute(f"SELECT {', '.join(contract.column_names)} FROM {name} ORDER BY {', '.join(contract.sort_keys)} LIMIT ?", (contract.max_rows + 1,)).fetchall()
    if len(physical) != count:
        raise ValueError("task projection physical rows differ")
    return ServingProjectionPayload(table_name=name, available_at=at, rows=tuple({
        key: normalize_aware_utc(value).isoformat() if isinstance(value, datetime) else value
        for key, value in zip(contract.column_names, row, strict=True)
    } for row in physical))


def read_task_center_view(borrowed: BorrowedGeneration) -> TaskCenterServingView | None:
    from rquant.web.task_overview import _mark, _ops_sample

    mark = _mark(borrowed, "ops_status")
    if mark is None or mark.status is not FreshnessStatus.FRESH:
        return None
    snapshot = _ops_sample(borrowed, mark)
    if snapshot is None:
        raise ValueError("task original ops snapshot is incomplete")
    cpu = _task_projection(borrowed, "ops_task_cpu")
    runs = _task_projection(borrowed, "ops_task_runs")
    if cpu is None and runs is None:
        return None
    if cpu is None or runs is None or cpu.available_at != runs.available_at or cpu.available_at != snapshot.sampled_at:
        raise ValueError("task ops projection pair or cutoff differs")
    if len(cpu.rows) != 5 or {row["slice_name"] for row in cpu.rows} != set(SLICES):
        raise ValueError("task CPU projection requires the exact five groups")
    hashes = {row["material_hash"] for row in cpu.rows}
    if len(hashes) != 1 or not all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes):
        raise ValueError("task CPU projection material identity differs")
    by_slice: dict[str, TaskCpuResult] = {}
    for row in cpu.rows:
        if (row["host_name"], row["boot_id"], row["manifest_digest"], row["observed_at"]) != (snapshot.host_name, snapshot.boot_id, snapshot.manifest_digest, snapshot.sampled_at.isoformat()):
            raise ValueError("task CPU projection differs from original ops identity")
        group = TaskCpuResult(slice_name=row["slice_name"], percent=row["percent"], reason=row["reason"], capacity=row["capacity"], effective_cpus=() if not row["effective_cpus"] else parse_cpu_list(row["effective_cpus"]))
        by_slice[group.slice_name] = group
    evidence = tuple(TaskUnitRunEvidence.model_validate_json(row["evidence_json"]) for row in runs.rows)
    sample = TaskOpsSample(snapshot=snapshot, evidence=TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable", runs=evidence))
    if runs != task_ops_projections(sample)[1]:
        raise ValueError("task run projection does not match complete invocation witnesses")
    control = _task_projection(borrowed, "lab_scheduler_control")
    state = None
    if control is not None:
        if len(control.rows) != 1:
            raise ValueError("scheduler projection requires one original control row")
        state = LabSchedulingControlState.model_validate_json(control.rows[0]["state_json"])
        if scheduling_projection(state, cutoff=control.available_at) != control:
            raise ValueError("scheduler projection does not match original state")
    from rquant.condition_alert_runtime_projection import read_monitor_runtime

    monitor = read_monitor_runtime(borrowed, now=borrowed.manifest.built_at)
    return TaskCenterServingView(generation_id=borrowed.manifest.generation_id, ops_generation_id=mark.generation_id,
        snapshot=snapshot, cpu=tuple(by_slice[name] for name in SLICES), runs=evidence, scheduling_control=state,
        notification_runtime=None if monitor is None else monitor.notification_window)


class TaskCenterServingSource:
    def __init__(self, root: Path, *, clock: Callable[[], datetime]) -> None:
        self.root, self.clock = Path(root), clock

    def read(self, *, generation_id: str) -> TaskCenterServingView:
        reader = ServingReader(self.root)
        with reader.acquire_generation() as lease:
            if lease.pointer is None or lease.manifest.generation_id != generation_id:
                raise ValueError("task admission generation changed")
            cursor = lease.connection.cursor()
            try:
                view = read_task_center_view(BorrowedGeneration(manifest=lease.manifest, pointer=lease.pointer, cursor=cursor, fallback_detail=None))
            finally:
                cursor.close()
            if view is None:
                raise ValueError("task complete source is unavailable")
            now = normalize_aware_utc(self.clock())
            if lease.manifest.built_at > now or not 0 <= (now - view.snapshot.sampled_at).total_seconds() < 120:
                raise ValueError("task admission source is future or stale")
            if view.scheduling_control is not None and not 0 <= (now - view.scheduling_control.observed_at).total_seconds() < 120:
                raise ValueError("task scheduling source is future or stale")
            pointer = reader.current_pointer()
            if (pointer.generation_id, pointer.manifest_sha256) != (lease.pointer.generation_id, lease.pointer.manifest_sha256):
                raise ValueError("task source generation changed during verification")
            return view


class TaskControlAdmissionUnavailableError(RuntimeError):
    pass


class TaskControlAdmissionRejectedError(ValueError):
    pass


class TaskControlAdmissionNotFoundError(KeyError):
    pass


class TaskUnitRunCapability(RuntimeContractModel):
    unit: StrictStr = Field(min_length=1, max_length=128)
    mode: Literal["readonly", "writer"]
    can_request: StrictBool
    requires_confirmation: StrictBool
    reason: Literal["available", "disabled", "busy", "window_closed", "source_unavailable", "cooldown"]


class ManualTaskUnitRunCapability(TaskUnitRunCapability):
    contract: Literal["rquant.manual-service-capability/v1"] = "rquant.manual-service-capability/v1"
    unit: Literal["rquant-notify-test.service"] = "rquant-notify-test.service"
    mode: Literal["readonly"] = "readonly"
    requires_confirmation: Literal[True] = True
    next_allowed_at: AwareUtcDatetime | None = None


class TaskControlCapabilities(RuntimeContractModel):
    owner_id: StrictStr = Field(min_length=1, max_length=256)
    generation_id: StrictStr = Field(min_length=1, max_length=128)
    source_payload_hash: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    metadata_identity: TaskControlIdentity
    enabled: StrictBool
    units: tuple[TaskUnitRunCapability | ManualTaskUnitRunCapability, ...] = Field(max_length=32)
    can_control_scheduling: StrictBool
    can_recover_units: StrictBool
    can_recover_scheduling: StrictBool
    scheduling_control: LabSchedulingControlState | None
    notifier_mode: NotifierModeState | None = None
    builtin_controls: tuple[MonitorBuiltinControlState, ...] = Field(default=(), max_length=4)
    can_control_notifier_mode: StrictBool = False
    can_set_notifier_live: StrictBool = False
    can_control_builtins: StrictBool = False

    @model_validator(mode="after")
    def validate_unique(self) -> TaskControlCapabilities:
        if len({item.unit for item in self.units}) != len(self.units):
            raise ValueError("task capability exact unit set differs")
        if not self.enabled and (any(item.can_request for item in self.units) or self.can_control_scheduling or self.can_control_notifier_mode or self.can_control_builtins):
            raise ValueError("disabled task capability cannot permit fresh effects")
        return self


class TaskControlAdmissionResult(RuntimeContractModel):
    original_request: Annotated[TaskControlRequest, Field(discriminator="kind")]
    owner_id: StrictStr
    accepted_command: Annotated[OwnedTaskControl, Field(discriminator="kind")]
    metadata_identity: TaskControlIdentity
    receipt: PageControlReceipt
    confirmation: TaskUnitConfirmation | None = None
    unit_effect: TaskUnitEffect | None = None
    scheduling_submission: LabSchedulingSubmission | None = None
    notifier_confirmation: NotifierModeConfirmation | None = None
    monitor_state: NotifierModeState | MonitorBuiltinControlState | None = None

    @model_validator(mode="after")
    def validate_original(self) -> TaskControlAdmissionResult:
        command = self.accepted_command
        if command.original() != self.original_request or (command.owner_id, command.metadata_identity, command.command_id, command.accepted_at) != (self.owner_id, self.metadata_identity, self.receipt.command_id, self.receipt.enqueued_at):
            raise ValueError("task private response differs from original actor/request/identity")
        if self.receipt.status is PageControlStatus.SUCCEEDED:
            reference = TaskControlSubmissionReference.model_validate(self.receipt.result)
            if reference.model_dump(mode="json") != TaskControlPageControlBackend.reference(command):
                raise ValueError("task response receipt differs from exact original accepted command")
        if type(command) in (OwnedPrepareNotifierDeliveryMode, OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled):
            if any(value is not None for value in (self.confirmation, self.unit_effect, self.scheduling_submission)):
                raise ValueError("monitor response contains another original effect")
            if type(command) is OwnedPrepareNotifierDeliveryMode:
                value = self.notifier_confirmation
                if self.monitor_state is not None or self.receipt.status is PageControlStatus.SUCCEEDED and value is None:
                    raise ValueError("mode preparation has no original confirmation")
                if value is not None and (value.prepare_id, value.owner_id, value.run, value.context) != (command.command_id, command.owner_id, command.run, command.context):
                    raise ValueError("mode preparation differs from its full original draft/source")
            else:
                value = self.monitor_state
                if self.notifier_confirmation is not None or self.receipt.status is PageControlStatus.SUCCEEDED and value is None:
                    raise ValueError("monitor operation has no exact original state")
                if value is not None and (value.command_id, value.accepted_at, value.installation_sha256, value.revision) != (
                        command.command_id, command.accepted_at, command.context.installation_sha256, command.expected_revision + 1):
                    raise ValueError("monitor operation differs from its original UUID/revision")
                if value is not None and (type(command) is OwnedSetNotifierDeliveryMode and (type(value) is not NotifierModeState or value.mode != command.mode or value.actor_id != command.owner_id)
                        or type(command) is OwnedSetMonitorBuiltinEnabled and (type(value) is not MonitorBuiltinControlState or (value.owner_id, value.builtin_id, value.definition.enabled) != (command.owner_id, command.builtin_id, command.enabled))):
                    raise ValueError("monitor state differs from its protected owner/kind")
        elif self.notifier_confirmation is not None or self.monitor_state is not None:
            raise ValueError("original task response contains another monitor effect")
        elif type(command) is OwnedPrepareUnitRun:
            if self.unit_effect is not None or self.scheduling_submission is not None or self.receipt.status is PageControlStatus.SUCCEEDED and self.confirmation is None:
                raise ValueError("unit prepare response contains another effect")
            if self.confirmation is not None and (self.confirmation.prepare_id, self.confirmation.owner_id, self.confirmation.run, self.confirmation.context) != (command.command_id, command.owner_id, command.run, command.context):
                raise ValueError("prepare response differs from original full confirmation")
        elif type(command) is OwnedRequestUnitRun:
            if self.confirmation is not None or self.scheduling_submission is not None or self.receipt.status is PageControlStatus.SUCCEEDED and self.unit_effect is None:
                raise ValueError("unit run response contains another effect")
            if self.unit_effect is not None and self.unit_effect.command != command:
                raise ValueError("unit effect differs from original accepted request")
        elif self.unit_effect is not None or self.confirmation is not None:
            raise ValueError("scheduler response contains a unit effect")
        elif self.scheduling_submission is not None and (str(self.scheduling_submission.request_id), self.scheduling_submission.content_hash, self.scheduling_submission.queue_identity) != (command.command_id, command.envelope.content_hash, command.envelope.command.queue_identity):
            raise ValueError("scheduler response differs from exact original scope")
        if len(self.model_dump_json().encode()) > 64 * 1024:
            raise ValueError("task private response exceeds 64 KiB")
        return self


class TaskControlAdmission:
    def __init__(self, service: PageControlService, *, backend: TaskControlPageControlBackend) -> None:
        if type(service) is not PageControlService or type(backend) is not TaskControlPageControlBackend or service.consumer.task_control_backend is not backend:
            raise TypeError("task admission requires the original concrete journal/backend")
        self.service, self.backend, self._lock = service, backend, Lock()

    @property
    def editor_users(self) -> frozenset[str]:
        owners = () if self.backend.monitor_control is None else tuple(row.owner_id for row in
            inspect_monitor_control_installation(self.backend.monitor_control, runtime_root=self.backend.monitor_runtime_root).builtin_definitions)
        return frozenset((*self.backend.operators, *self.backend.scheduling_admins, *self.backend.notifier_admins, *owners))

    def capabilities(self, *, authenticated_actor_id: str, generation_id: str) -> TaskControlCapabilities:
        from rquant.task_unit_control import writer_window_allows

        actor = authenticated_actor_id
        operator, admin = actor in self.backend.operators, actor in self.backend.scheduling_admins
        if actor not in self.editor_users:
            raise PermissionError("task capability is unavailable for current role")
        view = self.backend.source.read(generation_id=generation_id)
        entries: list[TaskUnitRunCapability] = []
        if operator:
            manifest, policy = self.backend.executor.configuration()
            if (manifest.host_name, manifest.digest) != (view.snapshot.host_name, view.snapshot.manifest_digest):
                raise ValueError("task capability source differs from signed exact policy")
            by_unit = {item.service: item for item in view.snapshot.units}
            for entry in policy.units:
                published = by_unit.get(entry.unit)
                reason = "disabled" if not (self.backend.enabled and policy.enabled and entry.enabled) else "source_unavailable" if published is None else "busy" if published.service_active_state in ("active", "activating", "reloading", "deactivating") or self.backend.journal.unit_unresolved(entry.unit, view.snapshot.boot_id) else "window_closed" if entry.mode == "writer" and not writer_window_allows(self.backend.clock()) else "available"
                entries.append(TaskUnitRunCapability(unit=entry.unit, mode=entry.mode, can_request=reason == "available", requires_confirmation=entry.mode == "writer", reason=reason))
            if self.backend.executor.manual_binding is not None:
                from rquant.notifier_operator import MANUAL_SERVICE, guard_manual_service_run

                next_allowed = self.backend.journal.manual_next_allowed_at()
                reason = "source_unavailable"
                try:
                    receipt = self.backend.executor.read_manual_state()
                    if (receipt.runtime_state.host_name, receipt.runtime_state.boot_id, receipt.material.ops_manifest_digest) != (
                            view.snapshot.host_name, view.snapshot.boot_id, view.snapshot.manifest_digest):
                        raise ValueError("manual capability differs from the original Ops source")
                    if not (self.backend.enabled and receipt.material.install.enabled):
                        reason = "disabled"
                    elif self.backend.journal.unit_unresolved(MANUAL_SERVICE, view.snapshot.boot_id):
                        reason = "busy"
                    elif next_allowed is not None and self.backend.clock() < next_allowed:
                        reason = "cooldown"
                    else:
                        guard_manual_service_run(receipt, now=self.backend.clock())
                        reason = "available"
                except (OSError, ValueError, PermissionError, TimeoutError):
                    pass
                entries.append(ManualTaskUnitRunCapability(can_request=reason == "available", reason=reason,
                    next_allowed_at=next_allowed))
        mode, builtins, monitor_ready, live_ready = None, (), False, False
        if self.backend.monitor_control is not None:
            try:
                self.backend.monitor_context(view, actor=actor, now=self.backend.clock())
                actual = read_monitor_control_state(self.backend.monitor_control, runtime_root=self.backend.monitor_runtime_root, now=self.backend.clock())
                mode = actual.mode if actor in self.backend.notifier_admins else None
                builtins = tuple(row for row in actual.builtins if row.owner_id == actor)
                monitor_ready = self.backend.enabled
                from rquant.notifier_operator import require_monitor_live_capability

                if monitor_ready and actor in self.backend.notifier_admins:
                    try:
                        require_monitor_live_capability(view.notification_runtime, installed=actual.installation,
                            current_mode=actual.mode, now=self.backend.clock())
                        live_ready = True
                    except ValueError:
                        pass
            except (OSError, ValueError, PermissionError, TimeoutError):
                pass
        return TaskControlCapabilities(owner_id=actor, generation_id=view.generation_id, source_payload_hash=view.material_hash,
            metadata_identity=self.backend.journal.identity(), enabled=self.backend.enabled, units=tuple(entries),
            can_control_scheduling=bool(admin and self.backend.enabled and self.backend.lab_facade is not None and view.scheduling_control is not None),
            can_recover_units=operator, can_recover_scheduling=admin, scheduling_control=view.scheduling_control if admin else None,
            notifier_mode=mode, builtin_controls=builtins, can_control_notifier_mode=monitor_ready and mode is not None,
            can_set_notifier_live=live_ready,
            can_control_builtins=monitor_ready and bool(builtins))

    def _bound(self, request: TaskControlRequest, actor: str, receipt: PageControlReceipt) -> TaskControlAdmissionResult:
        match = self.service.outbox.lookup_task_control_command(request, authenticated_actor_id=actor)
        if match is None or match[1] != receipt:
            raise ValueError("task original journal changed during private admission")
        command = match[0]
        self.backend.validate(command)
        challenge = None
        effect = None
        scheduling = None
        notifier_confirmation, monitor_state = None, None
        if type(command) is OwnedPrepareUnitRun and self.backend.has_effect(command):
            challenge = self.backend.journal.prepare_confirmation(command)
        elif type(command) is OwnedRequestUnitRun:
            effect = self.backend.journal.run_effect(command)
        elif type(command) is OwnedSetLabSchedulingPaused and self.backend.has_effect(command):
            scheduling = self.backend.lab_facade.submit_scheduling_control(command.envelope)
        elif type(command) in (OwnedPrepareNotifierDeliveryMode, OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled):
            operation = self.backend.journal.monitor_operation(command)
            if type(command) is OwnedPrepareNotifierDeliveryMode:
                notifier_confirmation = operation
            else:
                monitor_state = operation
        return TaskControlAdmissionResult(original_request=request, owner_id=actor, accepted_command=command,
            metadata_identity=command.metadata_identity, receipt=receipt, confirmation=challenge, unit_effect=effect, scheduling_submission=scheduling,
            notifier_confirmation=notifier_confirmation, monitor_state=monitor_state)

    def lookup(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> TaskControlAdmissionResult | None:
        with self._lock:
            receipt = self.service._lookup_trusted_task_control(request, authenticated_actor_id=authenticated_actor_id)
            return None if receipt is None else self._bound(request, authenticated_actor_id, receipt)

    def resume(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> TaskControlAdmissionResult:
        with self._lock:
            try:
                receipt = self.service._resume_trusted_task_control(request, authenticated_actor_id=authenticated_actor_id)
            except KeyError as exc:
                raise TaskControlAdmissionNotFoundError("original_not_found") from exc
            return self._bound(request, authenticated_actor_id, receipt)

    def submit(self, request: TaskControlRequest, *, authenticated_actor_id: str, verified_metadata_identity: TaskControlIdentity) -> TaskControlAdmissionResult:
        with self._lock:
            receipt = self.service._submit_trusted_task_control(request, authenticated_actor_id=authenticated_actor_id, verified_metadata_identity=verified_metadata_identity)
            return self._bound(request, authenticated_actor_id, receipt)


class TaskPrivateRequest(RuntimeContractModel):
    authenticated_actor_id: StrictStr = Field(min_length=1, max_length=256)
    command: Annotated[TaskControlRequest, Field(discriminator="kind")]
    verified_metadata_identity: TaskControlIdentity | None = None


_TASK_PREFIX = "/v1/task-control-admission"


def _task_handler() -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def _json(self, status: int, payload: object) -> None:
            body = canonical_json_bytes(payload)
            if len(body) > 64 * 1024:
                status, body = 503, b'{"error":"unavailable"}'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            mode = self.path.removeprefix(_TASK_PREFIX + "/")
            if self.path != _TASK_PREFIX + "/" + mode or mode not in {"capabilities", "lookup", "resume", "submit"}:
                self._json(404, {"error": "not_found"})
                return
            try:
                lengths = self.headers.get_all("Content-Length", [])
                if len(lengths) != 1 or not lengths[0].isdecimal() or not 1 <= int(lengths[0]) <= 8 * 1024 or self.headers.get_all("Content-Type", []) != ["application/json"] or self.headers.get("Transfer-Encoding") is not None or self.headers.get("Content-Encoding") is not None:
                    raise ValueError("task private framing differs")
                body = self.rfile.read(int(lengths[0]))
                if len(body) != int(lengths[0]):
                    raise ValueError("task private body is truncated")
                raw = strict_json_loads(body)
                if not isinstance(raw, dict):
                    raise ValueError("task private envelope differs")
                actor = _ACTOR_ADAPTER.validate_python(raw.get("authenticated_actor_id"))
                if mode == "capabilities":
                    if set(raw) != {"authenticated_actor_id", "generation_id"} or not isinstance(raw["generation_id"], str) or not 1 <= len(raw["generation_id"]) <= 128:
                        raise ValueError("task capability envelope differs")
                else:
                    parsed = TaskPrivateRequest.model_validate(raw)
                    if (mode == "submit") != (parsed.verified_metadata_identity is not None):
                        raise ValueError("task original lookup cannot provide fresh metadata")
            except (ValueError, KeyError, TypeError):
                self._json(400, {"error": "invalid_command"})
                return
            try:
                admission = self.server.admission
                if mode == "capabilities":
                    result = admission.capabilities(authenticated_actor_id=actor, generation_id=raw["generation_id"])
                elif mode == "lookup":
                    result = admission.lookup(parsed.command, authenticated_actor_id=actor)
                elif mode == "resume":
                    result = admission.resume(parsed.command, authenticated_actor_id=actor)
                else:
                    result = admission.submit(parsed.command, authenticated_actor_id=actor, verified_metadata_identity=parsed.verified_metadata_identity)
            except PermissionError:
                self._json(403, {"error": "actor_forbidden"})
            except TaskControlAdmissionNotFoundError:
                self._json(404, {"error": "original_not_found"})
            except (ValueError, KeyError, PageControlCommandConflictError):
                self._json(409, {"error": "rejected"})
            except Exception:
                self._json(503, {"error": "unavailable"})
            else:
                self._json(200, {"found": result is not None, "result": None if result is None else result.model_dump(mode="json")} if mode == "lookup" else result.model_dump(mode="json"))
    return Handler


def build_task_control_admission_server(admission: TaskControlAdmission, *, socket_path: Path | None,
        trusted_web_uid: int | None = None, shared_gid: int | None = None, peer_uid: Callable[[socket.socket], int] = _peer_uid) -> FactorDefinitionAdmissionServer | None:
    if type(admission) is not TaskControlAdmission:
        raise TypeError("task private server requires the original concrete admission")
    return build_factor_definition_admission_server(admission, socket_path=socket_path, trusted_web_uid=trusted_web_uid,
        shared_gid=shared_gid, peer_uid=peer_uid, _handler_type=_task_handler())


class TaskControlAdmissionClient(FactorDefinitionAdmissionClient):
    def _call(self, mode: str, raw: dict[str, object]) -> object:
        _ACTOR_ADAPTER.validate_python(raw.get("authenticated_actor_id"))
        body = canonical_json_bytes(raw)
        if len(body) > 8 * 1024:
            raise TaskControlAdmissionRejectedError("task private request budget")
        connection = _UnixHTTPConnection(self.socket_path, timeout_seconds=self.timeout_seconds, expected_service_uid=self.expected_service_uid, shared_gid=self.shared_gid, client_uid=self.client_uid)
        try:
            connection.request("POST", _TASK_PREFIX + "/" + mode, body=body, headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            if response.status in (400, 403, 409):
                raise TaskControlAdmissionRejectedError("task private admission rejected")
            if response.status == 404:
                raise TaskControlAdmissionNotFoundError("original_not_found")
            lengths = response.headers.get_all("Content-Length", [])
            if response.status != 200 or len(lengths) != 1 or not lengths[0].isdecimal() or not 1 <= int(lengths[0]) <= 64 * 1024 or response.headers.get_all("Content-Type", []) != ["application/json"] or response.getheader("Transfer-Encoding") is not None or response.getheader("Content-Encoding") is not None:
                raise ValueError("task private response framing differs")
            content = response.read(64 * 1024 + 1)
            if len(content) != int(lengths[0]):
                raise ValueError("task private response length differs")
            return strict_json_loads(content)
        except (TaskControlAdmissionRejectedError, TaskControlAdmissionNotFoundError):
            raise
        except (OSError, HTTPException, ValueError) as exc:
            raise TaskControlAdmissionUnavailableError("原请求结果暂无法核验。") from exc
        finally:
            connection.close()

    def _result(self, raw: object, request: TaskControlRequest, actor: str) -> TaskControlAdmissionResult:
        try:
            result = TaskControlAdmissionResult.model_validate(raw)
            if result.original_request != request or result.owner_id != actor:
                raise ValueError("task original response identity differs")
            return result
        except ValueError as exc:
            raise TaskControlAdmissionUnavailableError("原请求结果暂无法核验。") from exc

    def capabilities(self, *, authenticated_actor_id: str, generation_id: str) -> TaskControlCapabilities:
        try:
            result = TaskControlCapabilities.model_validate(self._call("capabilities", {"authenticated_actor_id": authenticated_actor_id, "generation_id": generation_id}))
            if (result.owner_id, result.generation_id) != (authenticated_actor_id, generation_id):
                raise ValueError("task capability identity differs")
            return result
        except ValueError as exc:
            raise TaskControlAdmissionUnavailableError("任务权限暂无法核验。") from exc

    def lookup(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> TaskControlAdmissionResult | None:
        raw = self._call("lookup", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json")})
        if not isinstance(raw, dict) or set(raw) != {"found", "result"} or type(raw["found"]) is not bool or raw["found"] != (raw["result"] is not None):
            raise TaskControlAdmissionUnavailableError("原请求结果暂无法核验。")
        return self._result(raw["result"], request, authenticated_actor_id) if raw["found"] else None

    def resume(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> TaskControlAdmissionResult:
        return self._result(self._call("resume", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json")}), request, authenticated_actor_id)

    def submit(self, request: TaskControlRequest, *, authenticated_actor_id: str, verified_metadata_identity: TaskControlIdentity) -> TaskControlAdmissionResult:
        return self._result(self._call("submit", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json"), "verified_metadata_identity": verified_metadata_identity.model_dump(mode="json")}), request, authenticated_actor_id)
