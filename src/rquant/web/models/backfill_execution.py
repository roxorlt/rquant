"""Public two-step maintenance commands and bounded original execution views."""
from __future__ import annotations
from datetime import date
from typing import Annotated,Literal,Self
from pydantic import BaseModel,ConfigDict,Field,model_validator
from rquant.backfill_execute_contracts import ExecutionConfirmation,MaintenanceExecutionStatus,ExecutionStatus
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.backfill_execute_projection import ExecutionEventProjectionRow


class _Command(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    command_id: str = Field(min_length=1,max_length=128)
    requested_at: AwareUtcDatetime


class PrepareBackfillRequest(_Command):
    kind: Literal['prepare_backfill_execution']
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan_hash: str = Field(pattern=r'^[0-9a-f]{64}$')


class ExecuteBackfillRequest(_Command):
    kind: Literal['execute_backfill_plan']
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    intent_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    prepare_command_id: str = Field(min_length=1,max_length=128)
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    exact_dates_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    confirmed: Literal[True]


class PrepareFinancialRequest(_Command):
    kind: Literal['prepare_financial_collection']
    audit_report_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    security_scope: Literal['available_securities','selected_securities'] = 'available_securities'
    selected_securities: tuple[str,...] = Field(default=(),max_length=250)
    start_date: date
    end_date: date
    report_periods: tuple[date,...] = Field(min_length=1,max_length=41)


class ExecuteFinancialRequest(_Command):
    kind: Literal['execute_financial_collection']
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    intent_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    prepare_command_id: str = Field(min_length=1,max_length=128)
    plan_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    confirmed: Literal[True]


class ExecutionControlRequest(_Command):
    kind: Literal['pause_data_center_execution','resume_data_center_execution']
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_sequence: int = Field(strict=True,ge=1)


DataCenterCommandRequest=Annotated[PrepareBackfillRequest|ExecuteBackfillRequest|PrepareFinancialRequest|ExecuteFinancialRequest|ExecutionControlRequest,Field(discriminator='kind')]


class ExecutionView(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    execution_id: str
    kind: Literal['backfill','financial']
    name: str
    status: ExecutionStatus
    status_label: str
    control_sequence: int
    pause_requested: bool
    pause_applied: bool
    completed_tasks: int
    total_tasks: int
    current_date: date | None
    updated_at: AwareUtcDatetime
    can_pause: bool
    can_resume: bool
    completion_verified: bool
    audit_report_hash: str | None

    @classmethod
    def from_original(cls,value: MaintenanceExecutionStatus) -> Self:
        from rquant.web.labels import DATA_CENTER_EXECUTION_LABELS
        return cls(execution_id=value.execution_id,kind=value.kind,name='日线回补' if value.kind=='backfill' else '财务采集',
            status=value.status,status_label='正在暂停' if value.pause_requested and not value.pause_applied else DATA_CENTER_EXECUTION_LABELS[value.status],
            control_sequence=value.control_sequence,pause_requested=value.pause_requested,pause_applied=value.pause_applied,
            completed_tasks=value.completed_tasks,total_tasks=value.total_tasks,current_date=value.current_date,updated_at=value.updated_at,
            can_pause=value.status in {'queued','running','verifying'} and not value.pause_requested,
            can_resume=value.status in {'paused','partial'},completion_verified=value.status=='completed',audit_report_hash=value.audit_report_sha256)


class ExecutionEventView(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    event_id: str
    execution_id: str
    name: str
    status_label: str
    occurred_at: AwareUtcDatetime
    detail: str | None = None

    @classmethod
    def from_original(cls,value: ExecutionEventProjectionRow) -> Self:
        from rquant.web.labels import data_center_record_labels
        name,label,detail=data_center_record_labels(value.event_type,value.task_id,value.task_status,value.failure_code)
        return cls(event_id=value.event_id,execution_id=value.execution_id,name=name,status_label=label,
            occurred_at=value.occurred_at,detail=detail)


class DataCenterCommandReceipt(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    command_id: str
    status: Literal['prepared','queued','control_accepted','pending','processing','failed','ambiguous']
    message: str
    confirmation: ExecutionConfirmation | None = None
    execution_id: str | None = None
    execution: ExecutionView | None = None


class ExecutionIndexData(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    status: Literal['ready','not_published','unavailable']
    configured: bool = False
    backfill_enabled: bool = False
    financial_enabled: bool = False
    may_start: bool = False
    executions: tuple[ExecutionView,...] = Field(default=(),max_length=50)
    events: tuple[ExecutionEventView,...] = Field(default=(),max_length=20)
