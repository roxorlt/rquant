"""Data-only original requests and private PageControl task bindings."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from rquant.backtest.contracts import Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.task_unit_control import BootId, UnitName, TaskUnitRuntimeState
from rquant.lab_scheduling_control import LabSchedulingCommandEnvelope, PauseSchedulingCommand
from rquant.monitor_builtin_contracts import BuiltinId, MonitorBuiltinDefinition

_OWNED_FIELDS = {"owner_id", "accepted_at", "metadata_identity", "original_request_hash", "context", "envelope"}


def _uuid(value: str) -> str:
    if str(UUID(value)) != value:
        raise ValueError("task command requires a canonical UUID")
    return value


class TaskUnitRunDraft(RuntimeContractModel):
    command_id: StrictStr
    requested_at: AwareUtcDatetime
    generation_id: StrictStr = Field(min_length=1, max_length=128)
    unit: UnitName

    @field_validator("command_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        return _uuid(value)

    @field_validator("requested_at", mode="before")
    @classmethod
    def bound_timestamp(cls, value: object) -> object:
        if not isinstance(value, (str, datetime)) or isinstance(value, str) and len(value) > 64:
            raise ValueError("task timestamp requires a bounded aware ISO value")
        return value

    @property
    def draft_hash(self) -> str:
        return canonical_sha256(TaskUnitRunDraft.model_validate(self.model_dump(exclude={"kind", "confirmation_id"} | _OWNED_FIELDS)))


class RequestUnitRun(TaskUnitRunDraft):
    kind: Literal["request_unit_run"] = "request_unit_run"
    confirmation_id: StrictStr | None = None

    @field_validator("confirmation_id")
    @classmethod
    def validate_confirmation(cls, value: str | None) -> str | None:
        return None if value is None else _uuid(value)

    @model_validator(mode="after")
    def validate_budget(self) -> RequestUnitRun:
        if len(self.model_dump_json(exclude=_OWNED_FIELDS).encode()) > 4096:
            raise ValueError("unit request exceeds 4 KiB")
        return self

    @property
    def request_hash(self) -> str:
        return canonical_sha256(RequestUnitRun.model_validate(self.model_dump(exclude=_OWNED_FIELDS)))


class _TaskRequest(RuntimeContractModel):
    command_id: StrictStr
    requested_at: AwareUtcDatetime
    generation_id: StrictStr = Field(min_length=1, max_length=128)

    @field_validator("command_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        return _uuid(value)

    @field_validator("requested_at", mode="before")
    @classmethod
    def bound_timestamp(cls, value: object) -> object:
        return TaskUnitRunDraft.bound_timestamp(value)

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self.model_dump(exclude=_OWNED_FIELDS))


class PrepareUnitRun(_TaskRequest):
    kind: Literal["prepare_unit_run"] = "prepare_unit_run"
    run: TaskUnitRunDraft

    @model_validator(mode="after")
    def validate_run_binding(self) -> PrepareUnitRun:
        if self.command_id == self.run.command_id or self.generation_id != self.run.generation_id:
            raise ValueError("prepare requires its own UUID and the same complete run generation")
        if len(self.model_dump_json(exclude=_OWNED_FIELDS).encode()) > 4096:
            raise ValueError("unit prepare exceeds 4 KiB")
        return self


class SetLabSchedulingPaused(_TaskRequest):
    kind: Literal["set_lab_scheduling_paused"] = "set_lab_scheduling_paused"
    expected_version: StrictInt = Field(ge=0, le=2**63 - 1)
    paused: StrictBool

    @model_validator(mode="after")
    def validate_budget(self) -> SetLabSchedulingPaused:
        if len(self.model_dump_json(exclude=_OWNED_FIELDS).encode()) > 1024:
            raise ValueError("scheduler control exceeds 1 KiB")
        return self


class NotifierModeDraft(_TaskRequest):
    mode: Literal["shadow", "live"]
    expected_revision: StrictInt = Field(ge=0, le=2**63 - 1)

    @property
    def draft_hash(self) -> str:
        return canonical_sha256(NotifierModeDraft.model_validate(self.model_dump(exclude={"kind", "confirmation_id"} | _OWNED_FIELDS)))


class PrepareNotifierDeliveryMode(_TaskRequest):
    kind: Literal["prepare_notifier_delivery_mode"] = "prepare_notifier_delivery_mode"
    run: NotifierModeDraft

    @model_validator(mode="after")
    def exact_draft(self) -> PrepareNotifierDeliveryMode:
        if self.command_id == self.run.command_id or self.generation_id != self.run.generation_id:
            raise ValueError("mode prepare needs two original UUIDs and the same generation")
        if len(self.model_dump_json(exclude=_OWNED_FIELDS).encode()) > 4096:
            raise ValueError("mode preparation exceeds 4 KiB")
        return self


class SetNotifierDeliveryMode(NotifierModeDraft):
    kind: Literal["set_notifier_delivery_mode"] = "set_notifier_delivery_mode"
    confirmation_id: StrictStr

    @field_validator("confirmation_id")
    @classmethod
    def exact_confirmation(cls, value: str) -> str:
        return _uuid(value)

    @model_validator(mode="after")
    def bounded_request(self) -> SetNotifierDeliveryMode:
        if len(self.model_dump_json(exclude=_OWNED_FIELDS).encode()) > 4096:
            raise ValueError("mode command exceeds 4 KiB")
        return self


class SetMonitorBuiltinEnabled(_TaskRequest):
    kind: Literal["set_monitor_builtin_enabled"] = "set_monitor_builtin_enabled"
    builtin_id: BuiltinId
    expected_revision: StrictInt = Field(ge=0, le=2**63 - 1)
    enabled: StrictBool

    @model_validator(mode="after")
    def bounded_request(self) -> SetMonitorBuiltinEnabled:
        if len(self.model_dump_json(exclude=_OWNED_FIELDS).encode()) > 4096:
            raise ValueError("builtin command exceeds 4 KiB")
        return self


class TaskControlIdentity(RuntimeContractModel):
    path: StrictStr = Field(min_length=1, max_length=4096)
    device: StrictInt = Field(ge=0, le=2**63 - 1)
    inode: StrictInt = Field(ge=1, le=2**63 - 1)
    instance_id: StrictStr = Field(pattern=r"^[0-9a-f]{32}$")

    @field_validator("path")
    @classmethod
    def validate_absolute_path(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError("task journal identity requires its original absolute path")
        return value


class TaskUnitAcceptedContext(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    manifest_digest: Sha256
    policy_digest: Sha256
    source_payload_hash: Sha256
    generation_id: StrictStr = Field(min_length=1, max_length=128)
    observed_at: AwareUtcDatetime
    mode: Literal["readonly", "writer"]
    runtime_state: TaskUnitRuntimeState

    @model_validator(mode="after")
    def validate_runtime_identity(self) -> TaskUnitAcceptedContext:
        if (self.host_name, self.boot_id) != (self.runtime_state.host_name, self.runtime_state.boot_id):
            raise ValueError("unit context differs from its complete runtime identity")
        return self


class ManualServiceAcceptedContext(TaskUnitAcceptedContext):
    contract: Literal["rquant.manual-service-context/v1"] = "rquant.manual-service-context/v1"
    mode: Literal["readonly"] = "readonly"
    manual_identity_sha256: Sha256

    @model_validator(mode="after")
    def exact_manual_identity(self) -> ManualServiceAcceptedContext:
        if self.runtime_state.unit != "rquant-notify-test.service":
            raise ValueError("manual context requires the exact installed notification service")
        return self


class NotifierControlAcceptedContext(RuntimeContractModel):
    contract: Literal["rquant.notifier-control-context/v1"] = "rquant.notifier-control-context/v1"
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: BootId
    manifest_digest: Sha256
    installation_sha256: Sha256
    notifier_manifest_sha256: Sha256
    producer_manifest_sha256: Sha256
    source_payload_hash: Sha256
    generation_id: StrictStr = Field(min_length=1, max_length=128)
    observed_at: AwareUtcDatetime
    initial_mode: Literal["shadow", "live"]
    builtin_definitions: tuple[MonitorBuiltinDefinition, ...] = Field(max_length=4)

    @model_validator(mode="after")
    def exact_owner_scope(self) -> NotifierControlAcceptedContext:
        keys = tuple((row.owner_id, row.builtin_id) for row in self.builtin_definitions)
        if keys != tuple(sorted(set(keys))) or len({row.owner_id for row in self.builtin_definitions}) > 1:
            raise ValueError("notifier control must keep the exact actor's installed builtin scope")
        return self


class NotifierModeConfirmation(RuntimeContractModel):
    contract: Literal["rquant.notifier-mode-confirmation/v1"] = "rquant.notifier-mode-confirmation/v1"
    confirmation_id: StrictStr
    prepare_id: StrictStr
    owner_id: StrictStr
    run: NotifierModeDraft
    draft_hash: Sha256
    context: NotifierControlAcceptedContext
    accepted_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    consumed_by: StrictStr | None = None

    @model_validator(mode="after")
    def exact_original(self) -> NotifierModeConfirmation:
        if (self.draft_hash != self.run.draft_hash or self.expires_at != self.accepted_at + timedelta(minutes=5)
                or self.consumed_by not in {None, self.run.command_id}):
            raise ValueError("mode confirmation differs from its original UUID/draft/expiry")
        return self


class TaskSchedulingAcceptedContext(RuntimeContractModel):
    store_id: StrictStr = Field(min_length=1, max_length=128)
    queue_binding_fingerprint: Sha256
    source_payload_hash: Sha256
    generation_id: StrictStr = Field(min_length=1, max_length=128)
    schema_version: Literal[17]
    observed_at: AwareUtcDatetime
    spool_path: StrictStr = Field(min_length=1, max_length=4096)
    spool_generation: tuple[StrictInt, StrictInt]

    @model_validator(mode="after")
    def validate_spool_identity(self) -> TaskSchedulingAcceptedContext:
        if not Path(self.spool_path).is_absolute() or any(value < 0 for value in self.spool_generation):
            raise ValueError("scheduler context requires its original physical spool")
        return self


class _OwnedTaskBinding(RuntimeContractModel):
    owner_id: StrictStr = Field(min_length=1, max_length=256)
    accepted_at: AwareUtcDatetime
    metadata_identity: TaskControlIdentity
    original_request_hash: Sha256


class OwnedPrepareUnitRun(PrepareUnitRun, _OwnedTaskBinding):
    context: TaskUnitAcceptedContext | ManualServiceAcceptedContext

    def original(self) -> PrepareUnitRun:
        return PrepareUnitRun.model_validate(self.model_dump(exclude=_OWNED_FIELDS))

    @model_validator(mode="after")
    def validate_original(self) -> OwnedPrepareUnitRun:
        _bind(self.original(), self)
        return self


class OwnedRequestUnitRun(RequestUnitRun, _OwnedTaskBinding):
    context: TaskUnitAcceptedContext | ManualServiceAcceptedContext

    def original(self) -> RequestUnitRun:
        return RequestUnitRun.model_validate(self.model_dump(exclude=_OWNED_FIELDS))

    @model_validator(mode="after")
    def validate_original(self) -> OwnedRequestUnitRun:
        _bind(self.original(), self)
        return self


class OwnedSetLabSchedulingPaused(SetLabSchedulingPaused, _OwnedTaskBinding):
    context: TaskSchedulingAcceptedContext
    envelope: LabSchedulingCommandEnvelope

    def original(self) -> SetLabSchedulingPaused:
        return SetLabSchedulingPaused.model_validate(self.model_dump(exclude=_OWNED_FIELDS | {"envelope"}))

    @model_validator(mode="after")
    def validate_original(self) -> OwnedSetLabSchedulingPaused:
        _bind(self.original(), self)
        action = self.envelope.command
        if (str(self.envelope.request_id), action.expected_version, isinstance(action, PauseSchedulingCommand), action.accepted_at,
            action.queue_identity.store_id, action.queue_identity.fingerprint) != (self.command_id, self.expected_version, self.paused,
            self.accepted_at, self.context.store_id, self.context.queue_binding_fingerprint):
            raise ValueError("scheduler owned command differs from its exact original frozen envelope")
        return self


class OwnedPrepareNotifierDeliveryMode(PrepareNotifierDeliveryMode, _OwnedTaskBinding):
    context: NotifierControlAcceptedContext

    def original(self) -> PrepareNotifierDeliveryMode:
        return PrepareNotifierDeliveryMode.model_validate(self.model_dump(exclude=_OWNED_FIELDS))

    @model_validator(mode="after")
    def exact_original(self) -> OwnedPrepareNotifierDeliveryMode:
        _bind(self.original(), self)
        return self


class OwnedSetNotifierDeliveryMode(SetNotifierDeliveryMode, _OwnedTaskBinding):
    context: NotifierControlAcceptedContext

    def original(self) -> SetNotifierDeliveryMode:
        return SetNotifierDeliveryMode.model_validate(self.model_dump(exclude=_OWNED_FIELDS))

    @model_validator(mode="after")
    def exact_original(self) -> OwnedSetNotifierDeliveryMode:
        _bind(self.original(), self)
        return self


class OwnedSetMonitorBuiltinEnabled(SetMonitorBuiltinEnabled, _OwnedTaskBinding):
    context: NotifierControlAcceptedContext

    def original(self) -> SetMonitorBuiltinEnabled:
        return SetMonitorBuiltinEnabled.model_validate(self.model_dump(exclude=_OWNED_FIELDS))

    @model_validator(mode="after")
    def exact_original(self) -> OwnedSetMonitorBuiltinEnabled:
        _bind(self.original(), self)
        if not any((row.owner_id, row.builtin_id) == (self.owner_id, self.builtin_id) for row in self.context.builtin_definitions):
            raise ValueError("builtin toggle requires its exact installed owner definition")
        return self


TaskControlRequest = PrepareUnitRun | RequestUnitRun | SetLabSchedulingPaused | PrepareNotifierDeliveryMode | SetNotifierDeliveryMode | SetMonitorBuiltinEnabled
OwnedTaskControl = OwnedPrepareUnitRun | OwnedRequestUnitRun | OwnedSetLabSchedulingPaused | OwnedPrepareNotifierDeliveryMode | OwnedSetNotifierDeliveryMode | OwnedSetMonitorBuiltinEnabled
TASK_CONTROL_KINDS = frozenset({"prepare_unit_run", "request_unit_run", "set_lab_scheduling_paused", "prepare_notifier_delivery_mode", "set_notifier_delivery_mode", "set_monitor_builtin_enabled"})
TASK_CONTROL_OWNED_TYPES = (OwnedPrepareUnitRun, OwnedRequestUnitRun, OwnedSetLabSchedulingPaused, OwnedPrepareNotifierDeliveryMode, OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled)
TASK_CONTROL_PUBLIC_TYPES = (PrepareUnitRun, RequestUnitRun, SetLabSchedulingPaused, PrepareNotifierDeliveryMode, SetNotifierDeliveryMode, SetMonitorBuiltinEnabled)


def _bind(request: TaskControlRequest, owned: OwnedTaskControl) -> None:
    if owned.original_request_hash != request.request_hash or owned.context.generation_id != request.generation_id:
        raise ValueError("owned task binding differs from complete original request")
    if owned.context.observed_at > owned.accepted_at:
        raise ValueError("owned task acceptance precedes its source observation")
    if isinstance(owned.context, TaskUnitAcceptedContext) and (owned.context.runtime_state.unit != (request.run.unit if isinstance(request, PrepareUnitRun) else request.unit) or owned.context.runtime_state.observed_at > owned.accepted_at):
        raise ValueError("owned unit request differs from actual accepted runtime facts")
