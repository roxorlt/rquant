"""Finite execution, current entitlement and original transaction receipt contracts."""
from __future__ import annotations

from datetime import UTC,date,datetime,time,timedelta
from pathlib import Path
from typing import Literal,Self
from zoneinfo import ZoneInfo

from pydantic import Field,JsonValue,model_validator

from rquant.backfill_plan_core import DailyBarBackfillPlan
from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity,CanonicalTableWatermark
from rquant.data_collection_contracts import AuditCollectionReference,SourceObservation,Sha256,CommitSha
from rquant.runtime_contracts import AwareUtcDatetime,RuntimeContractModel,canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.source_quota_transport import SourceTransportCallReceipt
from rquant.storage.primary_writer_gate import PrimaryWriterGateConfig

MARKET_EXECUTE_APIS=('daily','daily_basic','adj_factor','namechange','stock_st')
FINANCIAL_EXECUTE_APIS=('fina_indicator','income','balancesheet','cashflow','forecast','express','dividend')
ExecutionStatus=Literal['queued','running','paused','partial','verifying','failed','completed']
_SHANGHAI=ZoneInfo('Asia/Shanghai')


class ApiEntitlementEvidence(RuntimeContractModel):
    api_name: str = Field(min_length=1,max_length=80)
    status: Literal['verified','unknown','unavailable'] = 'unknown'
    source_account_sha256: Sha256
    proof_source: Literal['supplier_account_response','offline_fixture'] | None = None
    evidence_path: Path | None = None
    evidence_sha256: Sha256 | None = None
    valid_from: AwareUtcDatetime | None = None
    expires_at: AwareUtcDatetime | None = None
    allowed_parameters: tuple[str,...] = Field(default=(),max_length=16)
    scope_start: date | None = None
    scope_end: date | None = None
    allowed_symbols: tuple[str,...] = Field(default=(),max_length=8000)
    full_market: bool = False
    provider_row_limit: int | None = Field(default=None,strict=True,ge=1,le=8000)

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.status=='verified' and any(value is None for value in (self.proof_source,self.evidence_path,
            self.evidence_sha256,self.valid_from,self.expires_at,self.scope_start,self.scope_end,self.provider_row_limit)):
            raise ValueError('verified entitlement requires actual source and scope evidence')
        if self.valid_from is not None and self.expires_at is not None and self.valid_from>=self.expires_at:
            raise ValueError('entitlement proof range is reversed')
        if self.scope_start is not None and self.scope_end is not None and self.scope_start>self.scope_end:
            raise ValueError('entitlement date scope is reversed')
        return self

    def require_current(self,*,parameters: dict[str,str|None],now: datetime) -> None:
        if self.status!='verified':
            raise ValueError('source entitlement is unknown or unavailable')
        if not self.valid_from<=now<self.expires_at:
            raise ValueError('source entitlement expired')
        if not set(parameters)<=set(self.allowed_parameters):
            raise ValueError('source request exceeds entitled parameters')
        for key in ('trade_date','start_date','end_date','period','ann_date'):
            value=parameters.get(key)
            if value is not None:
                parsed=datetime.strptime(value,'%Y%m%d').date()
                if not self.scope_start<=parsed<=self.scope_end:
                    raise ValueError('source request exceeds entitled date scope')
        symbol=parameters.get('ts_code')
        if symbol and symbol not in self.allowed_symbols and not self.full_market:
            raise ValueError('source request exceeds entitled securities')
        if symbol is None and not self.full_market:
            raise ValueError('full-market source entitlement is unknown')


class DataCenterExecutionPolicy(RuntimeContractModel):
    contract: Literal['data-center-execution-policy/v1'] = 'data-center-execution-policy/v1'
    policy_generation: Sha256 | None = None
    environment: Literal['installed_runtime','offline_fixture'] = 'installed_runtime'
    backfill_execute_enabled: bool = False
    financial_collect_enabled: bool = False
    code_commit: CommitSha
    primary_writer_gate: PrimaryWriterGateConfig
    original_state_path: Path
    original_state_device: int = Field(strict=True,ge=0)
    original_state_inode: int = Field(strict=True,gt=0)
    quota_ledger_path: Path
    quota_ledger_device: int = Field(strict=True,ge=0)
    quota_ledger_inode: int = Field(strict=True,gt=0)
    quota_source: str = Field(min_length=1,max_length=128)
    source_account_sha256: Sha256
    quota_units_per_window: int = Field(strict=True,ge=1)
    quota_window_kind: Literal['day','minute']
    writer_installation_evidence_path: Path | None = None
    writer_installation_evidence_sha256: Sha256 | None = None
    entitlement_evidence: tuple[ApiEntitlementEvidence,...] = Field(default=(),max_length=16)
    source_material_directory: Path
    valid_from: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> Self:
        paths=(self.original_state_path,self.quota_ledger_path,self.source_material_directory)
        if any(not path.is_absolute() or path.resolve(strict=False)!=path for path in paths):
            raise ValueError('execution policy paths must be absolute and canonical')
        if self.valid_from>=self.expires_at:
            raise ValueError('execution policy is expired or reversed')
        if len({item.api_name for item in self.entitlement_evidence})!=len(self.entitlement_evidence):
            raise ValueError('execution entitlement APIs are duplicated')
        if any(item.source_account_sha256!=self.source_account_sha256 for item in self.entitlement_evidence):
            raise ValueError('execution entitlement account differs from original quota scope')
        digest=canonical_sha256(self.model_dump(mode='python',exclude={'policy_generation'}))
        if self.policy_generation is not None and self.policy_generation!=digest:
            raise ValueError('execution policy content changed')
        object.__setattr__(self,'policy_generation',digest)
        return self


class BackfillSourceRequestBinding(RuntimeContractModel):
    contract: Literal['backfill-source-request/v1'] = 'backfill-source-request/v1'
    logical_request_id: Sha256 | None = None
    owner: str = Field(min_length=1,max_length=256)
    execution_id: Sha256
    manifest_id: Sha256
    plan_sha256: Sha256
    scope_sha256: Sha256
    api_name: str = Field(min_length=1,max_length=80)
    parameters: dict[str,str|None]
    source_account_sha256: Sha256
    quota_source: str = Field(min_length=1,max_length=128)
    quota_ledger_device: int = Field(strict=True,ge=0)
    quota_ledger_inode: int = Field(strict=True,gt=0)
    max_sdk_dispatches: Literal[6] = 6
    source_normalization_version: Literal['tushare-nullable-v1'] | None = Field(default=None,exclude_if=lambda value:value is None)

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.api_name not in (*MARKET_EXECUTE_APIS,*FINANCIAL_EXECUTE_APIS,'trade_cal'):
            raise ValueError('source request API is outside original execution whitelist')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'logical_request_id'}))
        if self.logical_request_id is not None and self.logical_request_id!=expected:
            raise ValueError('source request immutable identity changed')
        object.__setattr__(self,'logical_request_id',expected)
        return self


class MaintenanceWindow(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    may_start_day: bool
    may_hold_writer: bool
    stop_new_day_at: AwareUtcDatetime
    interrupt_at: AwareUtcDatetime
    terminate_at: AwareUtcDatetime
    release_at: AwareUtcDatetime


def maintenance_window(now: datetime) -> MaintenanceWindow:
    local=now.astimezone(_SHANGHAI)
    weekday=local.weekday()<5
    clock=local.time()
    may_start=not weekday or clock<time(8,20) or clock>=time(17,50)
    may_hold=not weekday or clock<time(8,30) or clock>=time(17,50)
    cutoff_day=local.date() if weekday and clock<time(8,30) else local.date()+timedelta(days=1)
    while cutoff_day.weekday()>=5:
        cutoff_day+=timedelta(days=1)
    at=lambda value:datetime.combine(cutoff_day,value,_SHANGHAI).astimezone(UTC)
    return MaintenanceWindow(observed_at=now,may_start_day=may_start,may_hold_writer=may_hold,
        stop_new_day_at=at(time(8,20)),interrupt_at=at(time(8,28)),terminate_at=at(time(8,29,30)),
        release_at=at(time(8,30)))


class BackfillExecutionIntent(RuntimeContractModel):
    intent_id: Sha256 | None = None
    execution_id: Sha256
    owner: str = Field(min_length=1,max_length=256)
    prepare_command_id: str = Field(min_length=1,max_length=128)
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan_sha256: Sha256
    exact_dates_sha256: Sha256
    source_reference: AuditCollectionReference
    source_generation_id: Sha256
    calendar_sha256: Sha256
    primary_identity: CanonicalDatabaseIdentity
    policy_generation: Sha256
    nonce_sha256: Sha256
    issued_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.expires_at-self.issued_at!=timedelta(minutes=5):
            raise ValueError('execution confirmation must expire after five minutes')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'intent_id'}))
        if self.intent_id is not None and self.intent_id!=expected:
            raise ValueError('execution intent binding changed')
        object.__setattr__(self,'intent_id',expected)
        return self


class BackfillExecutionSpec(RuntimeContractModel):
    execution_id: Sha256
    owner: str = Field(min_length=1,max_length=256)
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan: DailyBarBackfillPlan
    intent: BackfillExecutionIntent
    execute_command_id: str = Field(min_length=1,max_length=128)
    manifest_id: Sha256
    admission_policy: DataCenterExecutionPolicy | None = Field(default=None,exclude_if=lambda value:value is None)

    @model_validator(mode='after')
    def bind(self) -> Self:
        if (self.execution_id,self.owner,self.plan_task_id,self.plan.content_sha256,canonical_sha256(self.plan.missing_dates))!=(
            self.intent.execution_id,self.intent.owner,self.intent.plan_task_id,self.intent.plan_sha256,self.intent.exact_dates_sha256):
            raise ValueError('execution differs from original owner, plan or exact dates')
        if not self.plan.missing_dates:
            raise ValueError('execution requires a nonempty original gap plan')
        if self.admission_policy is not None and self.admission_policy.policy_generation!=self.intent.policy_generation:
            raise ValueError('immutable admission policy differs from confirmation')
        return self


class BackfillDayCommitReceipt(RuntimeContractModel):
    contract: Literal['backfill-day-commit-receipt/v1'] = 'backfill-day-commit-receipt/v1'
    receipt_id: Sha256 | None = None
    execution_id: Sha256
    owner: str = Field(min_length=1,max_length=256)
    manifest_id: Sha256
    task_id: str = Field(min_length=1,max_length=128)
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan_sha256: Sha256
    trade_date: date
    committed_claim_token: str = Field(min_length=1,max_length=128)
    committed_attempt: int = Field(strict=True,ge=1)
    commit_protection: Literal['original_sqlite_begin_immediate'] = 'original_sqlite_begin_immediate'
    control_sequence: int = Field(strict=True,ge=1)
    policy_generation: Sha256
    source_requests: tuple[BackfillSourceRequestBinding,...] = Field(min_length=1,max_length=32)
    source_observations: tuple[SourceObservation,...] = Field(min_length=1,max_length=32)
    dispatch_receipts: tuple[SourceTransportCallReceipt,...] = Field(min_length=1,max_length=192)
    primary_identity: CanonicalDatabaseIdentity
    calendar: MarketCalendarAuthority
    inserted_rows: int = Field(strict=True,ge=0)
    preserved_rows: int = Field(strict=True,ge=0)
    watermarks: tuple[CanonicalTableWatermark,...] = Field(min_length=1,max_length=24)
    committed_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.trade_date not in self.calendar.open_dates or any(item.trade_date!=self.trade_date for item in self.watermarks):
            raise ValueError('backfill day receipt differs from original calendar/day scope')
        ids={item.logical_request_id for item in self.source_requests}
        if any(item.logical_request_id not in ids for item in self.dispatch_receipts):
            raise ValueError('backfill dispatch receipt is outside immutable source requests')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'receipt_id'}))
        if self.receipt_id is not None and self.receipt_id!=expected:
            raise ValueError('backfill day receipt content changed')
        object.__setattr__(self,'receipt_id',expected)
        return self


class MaintenanceExecutionStatus(RuntimeContractModel):
    execution_id: Sha256
    owner: str = Field(min_length=1,max_length=256)
    kind: Literal['backfill','financial']
    manifest_id: Sha256
    plan_task_id: str | None = None
    plan_sha256: Sha256
    source_generation_id: Sha256
    policy_generation: Sha256
    status: ExecutionStatus
    control_sequence: int = Field(strict=True,ge=1)
    pause_requested: bool = False
    pause_applied: bool = False
    total_tasks: int = Field(strict=True,ge=0,le=4096)
    completed_tasks: int = Field(strict=True,ge=0,le=4096)
    current_date: date | None = None
    failure_code: str | None = Field(default=None,max_length=80)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    completion_sha256: Sha256 | None = None
    audit_task_id: str | None = None
    audit_report_sha256: Sha256 | None = None

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.completed_tasks>self.total_tasks:
            raise ValueError('execution completed count exceeds fixed manifest')
        if (self.status=='completed')!=(self.completion_sha256 is not None):
            raise ValueError('only verified completion may claim a completion receipt')
        return self


class ExecutionConfirmation(RuntimeContractModel):
    kind: Literal['backfill','financial']
    execution_id: Sha256
    intent_id: Sha256
    prepare_command_id: str
    plan_hash: Sha256
    plan_task_id: str | None = None
    exact_dates_sha256: Sha256 | None = None
    start_date: date
    end_date: date
    missing_date_count: int | None = Field(default=None,strict=True,ge=1,le=3660)
    security_count: int | None = Field(default=None,strict=True,ge=1,le=8000)
    query_count: int | None = Field(default=None,strict=True,ge=1,le=100000)
    report_periods: tuple[date,...] = Field(default=(),max_length=41)
    expires_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.start_date>self.end_date:
            raise ValueError('confirmation date range is reversed')
        if self.kind=='backfill':
            if any(value is None for value in (self.plan_task_id,self.exact_dates_sha256,self.missing_date_count)) or any(
                    value is not None for value in (self.security_count,self.query_count)) or self.report_periods:
                raise ValueError('backfill confirmation differs from exact original plan')
        elif self.plan_task_id is not None or self.exact_dates_sha256 is not None or self.missing_date_count is not None or not self.report_periods or self.security_count is None or self.query_count is None:
            raise ValueError('financial confirmation differs from fixed original scope')
        return self
