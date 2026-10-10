"""Short, typed task controls without private journal or policy locations."""

from typing import Annotated, Literal

from pydantic import Field, StrictBool, StrictInt, StrictStr

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.task_control_commands import TaskControlRequest
from rquant.monitor_builtin_contracts import BuiltinId


class TaskUnitControlChoice(RuntimeContractModel):
    unit: StrictStr = Field(min_length=1, max_length=128)
    can_request: StrictBool
    requires_confirmation: StrictBool
    reason: StrictStr = Field(min_length=1, max_length=80)
    next_allowed_at: AwareUtcDatetime | None = None


class NotifierModeControlView(RuntimeContractModel):
    available: StrictBool = False
    mode: Literal["shadow", "live"] | None = None
    revision: StrictInt | None = Field(default=None, ge=0)
    installation_sha256: StrictStr | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    can_request: StrictBool = False
    can_set_live: StrictBool = False
    note: StrictStr = "通知模式暂无法核验。"


class MonitorBuiltinControlView(RuntimeContractModel):
    builtin_id: BuiltinId
    label: StrictStr = Field(min_length=1, max_length=8)
    enabled: StrictBool
    revision: StrictInt = Field(ge=0)
    installation_sha256: StrictStr | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    can_request: StrictBool = False


class TaskSchedulingView(RuntimeContractModel):
    available: StrictBool = False
    desired_version: StrictInt | None = Field(default=None, ge=0)
    applied_version: StrictInt | None = Field(default=None, ge=0)
    desired_paused: StrictBool | None = None
    applied_paused: StrictBool | None = None
    draining_count: StrictInt | None = Field(default=None, ge=0, le=64)
    accepted_at: AwareUtcDatetime | None = None
    applied_at: AwareUtcDatetime | None = None
    note: StrictStr = "调度状态尚未发布。"


class TaskControlCapabilitiesData(RuntimeContractModel):
    generation_id: StrictStr | None = None
    units: tuple[TaskUnitControlChoice, ...] = Field(default=(), max_length=32)
    can_control_scheduling: StrictBool = False
    can_recover_units: StrictBool = False
    can_recover_scheduling: StrictBool = False
    scheduling: TaskSchedulingView = TaskSchedulingView()
    notifier_mode: NotifierModeControlView = NotifierModeControlView()
    monitor_builtins: tuple[MonitorBuiltinControlView, ...] = Field(default=(), max_length=4)
    note: StrictStr = "任务操作尚未开放。"


class TaskControlCommandData(RuntimeContractModel):
    command_id: StrictStr
    original_request: Annotated[TaskControlRequest, Field(discriminator="kind")]
    status: Literal["not_found", "pending", "prepared", "submitted", "started", "succeeded", "failed", "unknown", "rejected"]
    message: StrictStr = Field(min_length=1, max_length=80)
    can_resume: StrictBool = False
    confirmation_id: StrictStr | None = None
    confirmation_expires_at: AwareUtcDatetime | None = None
    started_at: AwareUtcDatetime | None = None
    ended_at: AwareUtcDatetime | None = None
    invocation_id: StrictStr | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    duration_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    desired_version: StrictInt | None = Field(default=None, ge=0)
    desired_revision: StrictInt | None = Field(default=None, ge=0)
    desired_installation_sha256: StrictStr | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
