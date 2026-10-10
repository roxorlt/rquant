"""Execute immutable gap scopes through original quota and backfill state stores."""
from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import tempfile
from collections.abc import Callable,Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC,date,datetime
import re
import time
from threading import Event,Thread
from pathlib import Path
from typing import TYPE_CHECKING,TypeVar,Literal

import pandas as pd
from pydantic import Field,JsonValue,model_validator

from rquant.backfill_execute_contracts import (
    DataCenterExecutionPolicy,BackfillSourceRequestBinding,ApiEntitlementEvidence,
    MARKET_EXECUTE_APIS,FINANCIAL_EXECUTE_APIS,maintenance_window,
    MaintenanceExecutionStatus,
)
from rquant.runtime_contracts import AwareUtcDatetime,RuntimeContractModel,canonical_sha256
from rquant.data_collection_contracts import SourceObservation
from rquant.source_quota_store import SourceQuotaAttemptOutcome,SourceQuotaConflictError,SourceQuotaExhaustedError
from rquant.source_quota_transport import QuotaBoundTransportObserver,SourceTransportCallReceipt

if TYPE_CHECKING:
    from rquant.backfill_execute_contracts import BackfillDayCommitReceipt,BackfillExecutionSpec
    from rquant.backfill_state import BackfillStateStore,ClaimedBackfillTask
    from rquant.market_backfill import PreparedMarketFrames
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.security_status import SecurityStatusDaily
    from rquant.storage.duckdb import DuckDBStore
    from rquant.storage.primary_writer_gate import PrimaryWriterLease
    from rquant.backfill_plan_artifact import DailyBarBackfillPlan

_T=TypeVar('_T')
_REQUIRED_WRITERS={'daily','monitor','research_sync','backup','replica_sync','controlled_maintenance'}


def _read_private_json(path: Path,*,expected_sha256: str | None,max_bytes: int) -> object:
    if path.suffix!='.json' or not path.is_absolute() or path.resolve(strict=True)!=path:
        raise ValueError('trusted evidence must be a canonical JSON file')
    fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(fd,'rb') as handle:
        before=os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid()
                or before.st_mode&0o077 or not 0<before.st_size<=max_bytes):
            raise ValueError('trusted evidence identity or capacity is invalid')
        payload=handle.read(max_bytes+1)
        after=os.fstat(handle.fileno())
        if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(
            after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
            raise ValueError('trusted evidence changed during read')
    if len(payload)!=before.st_size or (expected_sha256 is not None and hashlib.sha256(payload).hexdigest()!=expected_sha256):
        raise ValueError('trusted evidence content changed')
    return json.loads(payload)


def load_execution_policy(path: Path) -> DataCenterExecutionPolicy:
    return DataCenterExecutionPolicy.model_validate(_read_private_json(path,expected_sha256=None,max_bytes=2*1024*1024))


def require_stable_execution_policy(policy: DataCenterExecutionPolicy,spec: BackfillExecutionSpec) -> None:
    original=spec.admission_policy
    if original is None:
        if policy.environment!='offline_fixture' or policy.policy_generation!=spec.intent.policy_generation:
            raise ValueError('immutable original execution policy is unavailable')
        return
    immutable=('environment','code_commit','primary_writer_gate','original_state_path','original_state_device',
        'original_state_inode','quota_ledger_path','quota_ledger_device','quota_ledger_inode','quota_source',
        'source_account_sha256','quota_units_per_window','quota_window_kind','source_material_directory')
    if any(getattr(policy,name)!=getattr(original,name) for name in immutable):
        raise ValueError('current policy changed immutable original execution/source scope')


def require_execution_policy(policy: DataCenterExecutionPolicy,*,kind: str,now: datetime,
                             api_name: str | None = None,parameters: dict[str,str|None] | None = None) -> ApiEntitlementEvidence | None:
    policy=DataCenterExecutionPolicy.model_validate_json(policy.model_dump_json())
    if kind not in {'backfill','financial'} or not getattr(policy,'backfill_execute_enabled' if kind=='backfill' else 'financial_collect_enabled'):
        raise ValueError('controlled execution is disabled')
    if not policy.valid_from<=now<policy.expires_at:
        raise ValueError('controlled execution policy expired')
    for path,device,inode in ((policy.original_state_path,policy.original_state_device,policy.original_state_inode),
                              (policy.quota_ledger_path,policy.quota_ledger_device,policy.quota_ledger_inode)):
        observed=path.stat(follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode) or path.resolve(strict=True)!=path or (observed.st_dev,observed.st_ino)!=(device,inode):
            raise ValueError('original state or quota ledger physical identity changed')
    if policy.writer_installation_evidence_path is None or policy.writer_installation_evidence_sha256 is None:
        raise ValueError('writer participation is unknown')
    installed=_read_private_json(policy.writer_installation_evidence_path,
        expected_sha256=policy.writer_installation_evidence_sha256,max_bytes=64*1024)
    if (not isinstance(installed,dict) or installed.get('kind')!='primary-writer-participation/v1'
            or installed.get('primary_generation')!=canonical_sha256({'canonical_path':str(policy.primary_writer_gate.primary_path),
                'device':policy.primary_writer_gate.primary_device,'inode':policy.primary_writer_gate.primary_inode})
            or set(installed.get('writers',()))!=_REQUIRED_WRITERS
            or installed.get('environment')!=policy.environment
            or installed.get('valid_until') is None or datetime.fromisoformat(installed['valid_until'])<=now):
        raise ValueError('installed writer evidence is incomplete or expired')
    if api_name is None:
        return None
    return require_current_entitlement(policy,api_name=api_name,parameters=parameters or {},now=now)


def require_current_entitlement(policy: DataCenterExecutionPolicy,*,api_name: str,
        parameters: dict[str,str|None],now: datetime) -> ApiEntitlementEvidence:
    entitlement=next((item for item in policy.entitlement_evidence if item.api_name==api_name),None)
    if entitlement is None:
        raise ValueError('source entitlement is unknown')
    entitlement.require_current(parameters=parameters,now=now)
    if policy.environment=='installed_runtime' and entitlement.proof_source!='supplier_account_response':
        raise ValueError('installed runtime requires actual supplier entitlement evidence')
    body=_read_private_json(entitlement.evidence_path,expected_sha256=entitlement.evidence_sha256,max_bytes=64*1024)
    expected=entitlement.model_dump(mode='json',exclude={'evidence_path','evidence_sha256'})
    if not isinstance(body,dict) or body.get('kind')!='source-entitlement-evidence/v1' or body.get('proof')!=expected:
        raise ValueError('current entitlement evidence differs from policy scope')
    return entitlement


def require_market_plan_policy(plan: DailyBarBackfillPlan,policy: DataCenterExecutionPolicy,*,now: datetime) -> None:
    from rquant.security_status import _add_years
    for day in (plan.missing_dates[0],plan.missing_dates[-1]):
        for api in ('daily','daily_basic','adj_factor','stock_st'):
            parameters={'trade_date':day.strftime('%Y%m%d')}
            if api=='daily_basic':
                parameters['fields']='ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,pe_ttm,pb,dv_ttm'
            elif api=='stock_st':
                parameters['fields']='ts_code,name,trade_date,type,type_name'
            require_execution_policy(policy,kind='backfill',now=now,api_name=api,parameters=parameters)
    start=plan.estimate.assumptions.status_namechange_start
    end=plan.estimate.assumptions.status_source_as_of
    while start<=end:
        window_end=min(_add_years(start,3),end)
        require_execution_policy(policy,kind='backfill',now=now,api_name='namechange',parameters={
            'start_date':start.strftime('%Y%m%d'),'end_date':window_end.strftime('%Y%m%d'),
            'fields':'ts_code,name,start_date,end_date,ann_date,change_reason'})
        if window_end==end:
            break
        start=window_end


class SupplierFailureEvidence(RuntimeContractModel):
    api_name: str
    source_account_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    supplier_response_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    refusal: str = Field(min_length=1,max_length=80)
    observed_at: AwareUtcDatetime


class DefinitiveSupplierFailure(Exception):
    """Only a retained actual supplier refusal can authorize original backoff."""
    def __init__(self,evidence: SupplierFailureEvidence) -> None:
        self.evidence=SupplierFailureEvidence.model_validate(evidence)
        super().__init__(self.evidence.refusal)


class _SourceMaterial(RuntimeContractModel):
    binding: BackfillSourceRequestBinding
    ordinal: int = Field(strict=True,ge=1,le=6)
    observation: SourceObservation
    columns: tuple[str,...] = Field(max_length=256)
    rows: tuple[tuple[JsonValue,...],...] = Field(max_length=8000)
    content_sha256: str | None = Field(default=None,pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def bind(self) -> _SourceMaterial:
        if len(self.columns)>(256 if self.binding.api_name in FINANCIAL_EXECUTE_APIS else 128):
            raise ValueError('source material exceeds original interface column capacity')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'content_sha256'}))
        if self.content_sha256 is not None and self.content_sha256!=expected:
            raise ValueError('source material content changed')
        object.__setattr__(self,'content_sha256',expected)
        return self

    def frame(self) -> pd.DataFrame:
        frame=pd.DataFrame.from_records(self.rows,columns=self.columns)
        for index,column in enumerate(self.columns):
            if any(row[index] is None for row in self.rows):
                frame[column]=pd.Series([row[index] for row in self.rows],dtype=object)
        observation=SourceObservation.from_frame(self.binding.api_name,self.binding.parameters,frame,
            observed_at=self.observation.observed_at,source_normalization_version=self.binding.source_normalization_version,
            possibly_truncated=self.observation.possibly_truncated)
        if observation!=self.observation:
            raise ValueError('cached source response differs from actual observed bytes')
        return frame


class ControlledTransportObserver:
    """Check current rights before every actual SDK call; retain the original ledger."""
    def __init__(self,original: QuotaBoundTransportObserver,*,policy: Callable[[],DataCenterExecutionPolicy],
        kind: str,material_directory: Path,clock: Callable[[],datetime],
        claim_guard: Callable[[],None]) -> None:
        self.original=original
        self.policy=policy
        self.kind=kind
        self.material_directory=material_directory
        self.clock=clock
        self.claim_guard=claim_guard
        self._binding: ContextVar[BackfillSourceRequestBinding | None]=ContextVar('controlled-source-binding',default=None)
        self.observations: list[SourceObservation]=[]
        self.dispatch_receipts: list[SourceTransportCallReceipt]=[]

    def _path(self,binding: BackfillSourceRequestBinding,ordinal: int,kind: str) -> Path:
        return self.material_directory/f'{binding.logical_request_id}-{ordinal}.{kind}.json'

    def _store(self,path: Path,payload: object) -> None:
        if self.material_directory.resolve(strict=False)!=self.material_directory or self.material_directory.is_symlink():
            raise ValueError('source material directory is not canonical')
        self.material_directory.mkdir(mode=0o700,parents=True,exist_ok=True)
        if self.material_directory.stat().st_mode&0o077:
            raise ValueError('source material directory must be private')
        data=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        if len(data)>32*1024*1024:
            raise ValueError('source material exceeds day capacity')
        total=0
        with os.scandir(self.material_directory) as entries:
            for index,entry in enumerate(entries):
                if index>=256 or entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                    raise ValueError('source material directory exceeds bounded recovery capacity')
                total+=entry.stat(follow_symlinks=False).st_size
        if total+len(data)>32*1024*1024:
            raise ValueError('prepared source material exceeds day capacity')
        fd,name=tempfile.mkstemp(prefix='.source-',dir=self.material_directory)
        try:
            with os.fdopen(fd,'wb') as handle:
                handle.write(data)
                handle.flush()
                os.fchmod(handle.fileno(),0o400)
                os.fsync(handle.fileno())
            try:
                os.link(name,path,follow_symlinks=False)
            except FileExistsError:
                if _read_private_json(path,expected_sha256=hashlib.sha256(data).hexdigest(),max_bytes=32*1024*1024)!=payload:
                    raise ValueError('same source identity has conflicting material')
            parent_fd=os.open(self.material_directory,os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        finally:
            Path(name).unlink(missing_ok=True)

    def request_attempts(self,logical_request_id: str) -> tuple:
        return self.original.request_attempts(logical_request_id)

    def request_outcome(self,logical_request_id: str) -> SourceQuotaAttemptOutcome | None:
        return self.original.request_outcome(logical_request_id)

    def current_receipts(self) -> tuple[SourceTransportCallReceipt,...]:
        return tuple(self.dispatch_receipts)

    @contextmanager
    def request(self,binding: BackfillSourceRequestBinding) -> Iterator[None]:
        if self._binding.get() is not None:
            raise SourceQuotaConflictError('controlled source scopes cannot be nested')
        attempts=self.original.request_attempts(binding.logical_request_id)
        if len(attempts)>6:
            raise SourceQuotaConflictError('source dispatch limit already exceeded')
        success=None
        for ordinal in range(1,len(attempts)+1):
            attempt=self.original.get_call_attempt(logical_request_id=binding.logical_request_id,api_name=binding.api_name,call_ordinal=ordinal)
            if attempt is None or attempt.outcome in {SourceQuotaAttemptOutcome.PENDING,SourceQuotaAttemptOutcome.UNKNOWN}:
                raise SourceQuotaConflictError('source response is uncertain; original identity is paused')
            if attempt.outcome is SourceQuotaAttemptOutcome.SUCCESS:
                if ordinal!=len(attempts) or not self._path(binding,ordinal,'response').exists():
                    raise SourceQuotaConflictError('successful source dispatch lacks durable response material')
                success=ordinal
            elif not self._path(binding,ordinal,'failure').exists():
                raise SourceQuotaConflictError('failed source dispatch has no definitive supplier evidence')
            else:
                evidence=_read_private_json(self._path(binding,ordinal,'failure'),expected_sha256=None,max_bytes=64*1024)
                if evidence.get('attempt_id')!=attempt.attempt_id or evidence.get('binding')!=binding.model_dump(mode='json'):
                    raise SourceQuotaConflictError('supplier failure evidence differs from original quota attempt')
                failure=SupplierFailureEvidence.model_validate(evidence['evidence'])
                if (failure.api_name,failure.source_account_sha256)!=(binding.api_name,binding.source_account_sha256):
                    raise SourceQuotaConflictError('supplier refusal has another source binding')
            self._record_usage(binding,ordinal)
        token=self._binding.set(binding)
        try:
            if success is not None:
                yield
            else:
                if len(attempts)>=6:
                    raise SourceQuotaConflictError('original source dispatch limit is exhausted')
                with self.original.scope(logical_request_id=binding.logical_request_id,observed_at=self.clock(),
                    next_call_ordinal=len(attempts)+1,resume_api_name=binding.api_name if attempts else None):
                    yield
        finally:
            self._binding.reset(token)

    def observe(self,api_name: str,call: Callable[[],_T]) -> _T:
        binding=self._binding.get()
        if binding is None or binding.api_name!=api_name:
            raise SourceQuotaConflictError('SDK call differs from immutable request scope')
        try:
            self.claim_guard()
            current=self.policy()
            entitlement=require_execution_policy(current,kind=self.kind,now=self.clock(),api_name=api_name,parameters=binding.parameters)
        except Exception as error:
            # Local admission failures must not enter the adapter's SDK backoff.
            raise SourceQuotaConflictError(f'current execution policy refused dispatch: {error}') from error
        if (current.quota_source,current.source_account_sha256,current.quota_ledger_device,current.quota_ledger_inode)!=(
            binding.quota_source,binding.source_account_sha256,binding.quota_ledger_device,binding.quota_ledger_inode):
            raise SourceQuotaConflictError('current quota/account scope differs from immutable request')
        if (self.original.source,self.original.quota_units_per_window,self.original.window_kind)!=(
            current.quota_source,current.quota_units_per_window,current.quota_window_kind):
            raise SourceQuotaConflictError('original quota observer differs from current policy')
        attempts=self.original.request_attempts(binding.logical_request_id)
        ordinal=len(attempts)+1
        if attempts and attempts[-1].outcome is SourceQuotaAttemptOutcome.SUCCESS:
            ordinal=len(attempts)
            material=_SourceMaterial.model_validate(_read_private_json(self._path(binding,ordinal,'response'),
                expected_sha256=None,max_bytes=32*1024*1024))
            if material.binding!=binding or material.ordinal!=ordinal:
                raise SourceQuotaConflictError('durable source response scope changed')
            self.observations.append(material.observation)
            self._record_usage(binding,ordinal)
            return material.frame()
        if ordinal>6:
            raise SourceQuotaConflictError('original source dispatch limit is exhausted')
        def dispatch() -> _T:
            result=call()
            if not isinstance(result,pd.DataFrame):
                raise ValueError('SDK source response is not a table')
            if (self.kind=='backfill' and len(result)>=entitlement.provider_row_limit
                    or self.kind=='financial' and len(result)>entitlement.provider_row_limit):
                raise ValueError('SDK response may be truncated')
            observation=SourceObservation.from_frame(api_name,binding.parameters,result,observed_at=self.clock(),
                source_normalization_version=binding.source_normalization_version,
                possibly_truncated=self.kind=='financial' and len(result)>=entitlement.provider_row_limit)
            rows=tuple(tuple(value.item() if hasattr(value,'item') and not isinstance(value,str) else value for value in row)
                       for row in result.itertuples(index=False,name=None))
            material=_SourceMaterial(binding=binding,ordinal=ordinal,observation=observation,
                columns=tuple(str(column) for column in result.columns),rows=rows)
            self._store(self._path(binding,ordinal,'response'),material.model_dump(mode='json'))
            self.observations.append(observation)
            return result
        try:
            result=self.original.observe(api_name,dispatch)
        except DefinitiveSupplierFailure as error:
            if (error.evidence.api_name,error.evidence.source_account_sha256)!=(api_name,binding.source_account_sha256):
                raise SourceQuotaConflictError('supplier refusal has another source') from error
            attempt=self.original.get_call_attempt(logical_request_id=binding.logical_request_id,api_name=api_name,call_ordinal=ordinal)
            self._store(self._path(binding,ordinal,'failure'),{'binding':binding.model_dump(mode='json'),
                'attempt_id':attempt.attempt_id,'evidence':error.evidence.model_dump(mode='json')})
            self._record_usage(binding,ordinal)
            raise
        except (SourceQuotaConflictError,SourceQuotaExhaustedError):
            raise
        except Exception as error:
            raise SourceQuotaConflictError('source dispatch result is uncertain; retain charge and original identity') from error
        self._record_usage(binding,ordinal)
        return result

    def _record_usage(self,binding: BackfillSourceRequestBinding,ordinal: int) -> None:
        attempt=self.original.get_call_attempt(logical_request_id=binding.logical_request_id,api_name=binding.api_name,call_ordinal=ordinal)
        if attempt is None or attempt.dispatched_at is None or attempt.committed_at is None:
            raise SourceQuotaConflictError('source dispatch ledger receipt is incomplete')
        if any(item.attempt_id==attempt.attempt_id for item in self.dispatch_receipts):
            return
        self.dispatch_receipts.append(SourceTransportCallReceipt(source=attempt.source,logical_request_id=binding.logical_request_id,
            api_name=binding.api_name,call_ordinal=ordinal,attempt_id=attempt.attempt_id,outcome=attempt.outcome,
            dispatched_at=attempt.dispatched_at,committed_at=attempt.committed_at))


class ControlledMarketAdapter:
    """Bind exact original adapter calls to stable finite request identities."""
    def __init__(self,adapter: object,observer: ControlledTransportObserver,*,spec: object,policy: DataCenterExecutionPolicy) -> None:
        from rquant.adapter.tushare import TushareAdapter
        from rquant.backfill_execute_contracts import BackfillExecutionSpec
        if not isinstance(adapter,TushareAdapter) or getattr(adapter,'_transport_observer',None) is not observer or getattr(adapter,'_backup_token',None):
            raise ValueError('controlled market requires the original bound adapter without backup token')
        self.adapter=adapter
        self.observer=observer
        self.spec=BackfillExecutionSpec.model_validate(spec)
        require_stable_execution_policy(policy,self.spec)
        self.policy_at_binding=policy
        self.bindings: list[BackfillSourceRequestBinding]=[]

    def _fetch(self,api: str,parameters: dict[str,str|None],call: Callable[[],pd.DataFrame]) -> pd.DataFrame:
        binding=BackfillSourceRequestBinding(owner=self.spec.owner,execution_id=self.spec.execution_id,
            manifest_id=self.spec.manifest_id,plan_sha256=self.spec.plan.content_sha256,
            scope_sha256=canonical_sha256({'api_name':api,'parameters':parameters}),api_name=api,parameters=parameters,
            source_account_sha256=self.policy_at_binding.source_account_sha256,quota_source=self.policy_at_binding.quota_source,
            quota_ledger_device=self.policy_at_binding.quota_ledger_device,quota_ledger_inode=self.policy_at_binding.quota_ledger_inode,
            source_normalization_version=getattr(self.adapter,'_sdk_null_normalization',None))
        if binding not in self.bindings:
            self.bindings.append(binding)
        with self.observer.request(binding):
            return call()

    def daily_by_date(self,trade_date: date) -> pd.DataFrame:
        return self._fetch('daily',{'trade_date':trade_date.strftime('%Y%m%d')},lambda:self.adapter.daily_by_date(trade_date))

    def daily_basic_by_date(self,trade_date: date) -> pd.DataFrame:
        return self._fetch('daily_basic',{'trade_date':trade_date.strftime('%Y%m%d'),
            'fields':'ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,pe_ttm,pb,dv_ttm'},
            lambda:self.adapter.daily_basic_by_date(trade_date))

    def adj_factor_by_date(self,trade_date: date) -> pd.DataFrame:
        return self._fetch('adj_factor',{'trade_date':trade_date.strftime('%Y%m%d')},lambda:self.adapter.adj_factor_by_date(trade_date))

    def namechange_raw(self,start_date: date,end_date: date,ts_code: str | None = None) -> pd.DataFrame:
        parameters={'start_date':start_date.strftime('%Y%m%d'),'end_date':end_date.strftime('%Y%m%d'),
            'fields':'ts_code,name,start_date,end_date,ann_date,change_reason'}
        if ts_code is not None:
            parameters['ts_code']=ts_code
        return self._fetch('namechange',parameters,lambda:self.adapter.namechange_raw(start_date,end_date,ts_code))

    def stock_st_raw(self,trade_date: date) -> pd.DataFrame:
        return self._fetch('stock_st',{'trade_date':trade_date.strftime('%Y%m%d'),'fields':'ts_code,name,trade_date,type,type_name'},
            lambda:self.adapter.stock_st_raw(trade_date))


def cleanup_verified_source_material(binding: BackfillSourceRequestBinding, *, directory: Path,
        observer: QuotaBoundTransportObserver) -> int:
    attempts=observer.request_attempts(binding.logical_request_id)
    if not attempts or len(attempts)>6 or attempts[-1].outcome is not SourceQuotaAttemptOutcome.SUCCESS:
        raise ValueError('only confirmed original dispatch results may release source material')
    removed=0
    for ordinal,attempt in enumerate(attempts,1):
        if attempt.outcome not in {SourceQuotaAttemptOutcome.SUCCESS,SourceQuotaAttemptOutcome.FAILURE}:
            raise ValueError('uncertain source dispatch material must remain available')
        suffix='response' if attempt.outcome is SourceQuotaAttemptOutcome.SUCCESS else 'failure'
        path=directory/f'{binding.logical_request_id}-{ordinal}.{suffix}.json'
        if not path.exists():
            continue
        before=path.stat(follow_symlinks=False)
        payload=_read_private_json(path,expected_sha256=None,max_bytes=32*1024*1024 if suffix=='response' else 64*1024)
        if suffix=='response':
            material=_SourceMaterial.model_validate(payload)
            material.frame()
            if material.binding!=binding or material.ordinal!=ordinal:
                raise ValueError('source material cleanup scope changed')
        elif payload.get('binding')!=binding.model_dump(mode='json') or payload.get('attempt_id')!=attempt.attempt_id:
            raise ValueError('supplier refusal cleanup scope changed')
        after=path.stat(follow_symlinks=False)
        if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(
                after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
            raise ValueError('source material changed before release')
        path.unlink()
        removed+=1
    if removed:
        descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return removed


_RAW_DAY_TABLES={'daily_bar','daily_basic','adj_factor','stock_status_daily'}


def _verify_prepared_day_material(spec: BackfillExecutionSpec,policy: DataCenterExecutionPolicy,*,day: date,
        frames: PreparedMarketFrames,status_rows: tuple[SecurityStatusDaily,...],
        source_requests: tuple[BackfillSourceRequestBinding,...],source_observations: tuple[SourceObservation,...],
        dispatch_receipts: tuple[SourceTransportCallReceipt,...],observer: QuotaBoundTransportObserver,
        now: datetime) -> tuple[PreparedMarketFrames,tuple[SecurityStatusDaily,...],pd.DataFrame]:
    from rquant.market_backfill import PreparedMarketFrames
    from rquant.security_status import (DailySecurityKey,SecurityStatusDaily,normalize_namechange_history,
        normalize_stock_st_history,materialize_security_status,_add_years)
    bindings=tuple(BackfillSourceRequestBinding.model_validate_json(row.model_dump_json()) for row in source_requests)
    if len({row.logical_request_id for row in bindings})!=len(bindings) or not 1<=len(bindings)<=32:
        raise ValueError('actual source requests have duplicate identities or excess scope')
    materials: dict[str,list[_SourceMaterial]]={api:[] for api in MARKET_EXECUTE_APIS}
    expected_observations=[]
    expected_dispatches=[]
    for binding in bindings:
        if binding.api_name not in materials or (binding.owner,binding.execution_id,binding.manifest_id,binding.plan_sha256)!=(
                spec.owner,spec.execution_id,spec.manifest_id,spec.plan.content_sha256):
            raise ValueError('actual source material has another execution scope')
        if (binding.source_account_sha256,binding.quota_source,binding.quota_ledger_device,binding.quota_ledger_inode)!=(
                policy.source_account_sha256,policy.quota_source,policy.quota_ledger_device,policy.quota_ledger_inode):
            raise ValueError('actual source material has another account or ledger scope')
        require_execution_policy(policy,kind='backfill',now=now,api_name=binding.api_name,parameters=binding.parameters)
        attempts=observer.request_attempts(binding.logical_request_id)
        if not 1<=len(attempts)<=6:
            raise ValueError('actual source dispatch sequence is missing or unbounded')
        for ordinal in range(1,len(attempts)+1):
            saved=observer.get_call_attempt(logical_request_id=binding.logical_request_id,api_name=binding.api_name,call_ordinal=ordinal)
            if saved is None or saved.dispatched_at is None or saved.committed_at is None:
                raise ValueError('actual source dispatch receipt is missing')
            expected_dispatches.append(SourceTransportCallReceipt(source=saved.source,logical_request_id=binding.logical_request_id,
                api_name=binding.api_name,call_ordinal=ordinal,attempt_id=saved.attempt_id,outcome=saved.outcome,
                dispatched_at=saved.dispatched_at,committed_at=saved.committed_at))
            if ordinal==len(attempts):
                if saved.outcome is not SourceQuotaAttemptOutcome.SUCCESS:
                    raise ValueError('actual source response has no successful dispatch')
            elif saved.outcome is not SourceQuotaAttemptOutcome.FAILURE:
                raise ValueError('actual source dispatch recovery has an uncertain attempt')
        path=policy.source_material_directory/f'{binding.logical_request_id}-{len(attempts)}.response.json'
        material=_SourceMaterial.model_validate(_read_private_json(path,expected_sha256=None,max_bytes=32*1024*1024))
        if material.binding!=binding or material.ordinal!=len(attempts) or material.observation.observed_at>now:
            raise ValueError('actual source response material differs from immutable scope')
        material.frame()
        materials[binding.api_name].append(material)
        expected_observations.append(material.observation)
    if sorted(row.model_dump_json() for row in expected_dispatches)!=sorted(row.model_dump_json() for row in dispatch_receipts):
        raise ValueError('actual source dispatch receipts differ from original ledger')
    if sorted(row.model_dump_json() for row in expected_observations)!=sorted(row.model_dump_json() for row in source_observations):
        raise ValueError('actual source observations differ from retained response material')
    verified=[]
    for api,supplied in (('daily',frames.daily),('daily_basic',frames.daily_basic),('adj_factor',frames.adj_factor)):
        if len(materials[api])!=1:
            raise ValueError('exact-day source response scope is missing or duplicated')
        material=materials[api][0]
        expected_parameters={'trade_date':day.strftime('%Y%m%d')}
        if api=='daily_basic':
            expected_parameters['fields']='ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,pe_ttm,pb,dv_ttm'
        if material.binding.parameters!=expected_parameters:
            raise ValueError('exact-day source request parameters changed')
        expected=material.frame()
        if api=='adj_factor':
            expected=expected.loc[:,['ts_code','trade_date','adj_factor']].copy()
        expected['trade_date']=pd.to_datetime(expected['trade_date'],format='%Y%m%d',errors='raise').dt.date
        expected=expected.sort_values('ts_code').reset_index(drop=True)
        copied=supplied.copy(deep=True).sort_values('ts_code').reset_index(drop=True)
        if expected.empty or set(expected['trade_date'])!={day} or set(copied['trade_date'])!={day}:
            raise ValueError('exact-day source response differs from the sealed day scope')
        if tuple(copied.columns)!=tuple(expected.columns) or SourceObservation.from_frame(api,expected_parameters,copied,
                observed_at=now)!=SourceObservation.from_frame(api,expected_parameters,expected,observed_at=now):
            raise ValueError('prepared facts differ from actual source response material')
        verified.append(copied)
    codes=set(verified[0]['ts_code'])
    if any(set(frame['ts_code'])!=codes for frame in verified[1:]):
        raise ValueError('original source responses have different security scopes')
    names=materials['namechange']
    start=spec.plan.estimate.assumptions.status_namechange_start
    end=spec.plan.estimate.assumptions.status_source_as_of
    windows=[]
    while start<=end:
        window_end=min(_add_years(start,3),end)
        windows.append({'start_date':start.strftime('%Y%m%d'),'end_date':window_end.strftime('%Y%m%d'),
            'fields':'ts_code,name,start_date,end_date,ann_date,change_reason'})
        if window_end==end:
            break
        start=window_end
    if sorted(row.binding.parameters.items() for row in names)!=sorted(row.items() for row in windows):
        raise ValueError('original name history source scope differs from sealed plan')
    if len(materials['stock_st'])!=1 or materials['stock_st'][0].binding.parameters!={
            'trade_date':day.strftime('%Y%m%d'),'fields':'ts_code,name,trade_date,type,type_name'}:
        raise ValueError('original security status source scope differs from exact day')
    st=normalize_stock_st_history(materials['stock_st'][0].frame(),requested_trade_date=day)
    if not st.is_complete:
        raise ValueError('original security status source response is incomplete')
    copied_status=tuple(SecurityStatusDaily.model_validate_json(row.model_dump_json()) for row in status_rows)
    clocks={row.ingested_at for row in copied_status}
    if len(clocks)!=1 or not copied_status or max(row.observation.observed_at for row in (*names,materials['stock_st'][0]))>next(iter(clocks)) or next(iter(clocks))>now:
        raise ValueError('original security status source observation clock is invalid')
    expected_status=materialize_security_status(tuple(DailySecurityKey(ts_code=code,trade_date=day) for code in sorted(codes)),
        normalize_namechange_history(pd.concat([row.frame() for row in names],ignore_index=True)),st,
        ingested_at=next(iter(clocks)))
    if sorted(row.model_dump_json() for row in expected_status)!=sorted(row.model_dump_json() for row in copied_status):
        raise ValueError('prepared security status differs from original source material')
    return PreparedMarketFrames(trade_date=day,daily=verified[0],daily_basic=verified[1],adj_factor=verified[2]),copied_status,materials['daily_basic'][0].frame()


def _missing_market_rows(store: DuckDBStore,table: str,frame: pd.DataFrame,columns: tuple[str,...]) -> tuple[pd.DataFrame,int]:
    if table not in {'daily_bar','daily_basic','adj_factor'} or set(columns)-set(frame.columns):
        raise ValueError('original market columns are missing')
    if frame.empty or frame.duplicated(['ts_code','trade_date']).any():
        raise ValueError('exact missing-day response is empty or has duplicate keys')
    if (len(frame)>8000 or len(frame.columns)>128 or frame['ts_code'].isna().any()
            or any(re.fullmatch(r'[0-9]{6}\.(SH|SZ|BJ)',str(value)) is None for value in frame['ts_code'])
            or any(value is None or pd.isna(value) for value in frame['trade_date'])):
        raise ValueError('exact-day source keys or capacity are invalid')
    for column in columns[2:]:
        for value in frame[column]:
            if value is not None and (isinstance(value,(str,bool)) or not isinstance(value,(int,float)) or not math.isfinite(value)):
                raise ValueError('exact-day source has invalid or nonfinite numeric facts')
    if table in {'daily_bar','adj_factor'} and frame.loc[:,list(columns[2:])].isna().any().any():
        raise ValueError('critical exact-day numeric fact is missing')
    stage='_rquant_exact_gap_compare'
    store._conn.register(stage,frame.loc[:,list(columns)])
    try:
        comparisons=' OR '.join(f'stage.{column} IS DISTINCT FROM target.{column}' for column in columns)
        conflict=store._conn.execute(f'SELECT stage.ts_code FROM {stage} AS stage JOIN {table} AS target USING(ts_code,trade_date) '
            f'WHERE {comparisons} LIMIT 1').fetchone()
        if conflict is not None:
            raise ValueError(f'existing {table} row conflicts with sealed source')
        missing=store._conn.execute(f'SELECT stage.* FROM {stage} AS stage LEFT JOIN {table} AS target USING(ts_code,trade_date) '
            'WHERE target.ts_code IS NULL ORDER BY stage.ts_code').df()
        return missing,len(frame)-len(missing)
    finally:
        store._conn.unregister(stage)


def _missing_status_rows(store: DuckDBStore,rows: tuple[SecurityStatusDaily,...],day: date,
                         codes: set[str]) -> tuple[tuple[SecurityStatusDaily,...],int]:
    from rquant.security_status import SecurityStatusDaily
    if len(rows)!=len(codes) or {row.ts_code for row in rows}!=codes:
        raise ValueError('original security status does not cover exact source securities')
    if any(row.trade_date!=day or row.is_st is None or row.conflict_reason is not None or row.available_at is None for row in rows):
        raise ValueError('critical original security status source is unknown or conflicting')
    existing={row.ts_code:row for row in store.list_stock_status(day,day)}
    missing=[]
    for supplied in rows:
        row=SecurityStatusDaily.model_validate_json(supplied.model_dump_json())
        previous=existing.get(row.ts_code)
        if previous is None:
            missing.append(row)
        elif row.model_dump(exclude={'ingested_at'})!=previous.model_dump(exclude={'ingested_at'}):
            raise ValueError('existing security status conflicts with exact source')
    return tuple(missing),len(rows)-len(missing)


def verify_backfill_day_receipt(store: DuckDBStore,spec: BackfillExecutionSpec,*,task_id: str,
                               policy: DataCenterExecutionPolicy,
                               quota_observer: QuotaBoundTransportObserver | None = None) -> BackfillDayCommitReceipt | None:
    from rquant.backfill_execute_contracts import BackfillDayCommitReceipt
    from rquant.daily_canonical_publisher import DailyCanonicalPublisher
    from rquant.data_collection_authority import _calendar_facts
    from rquant.source_quota_store import SourceQuotaStore
    row=store._conn.execute('SELECT receipt_id,trade_date,payload_json FROM backfill_day_commit_receipt WHERE execution_id=? AND task_id=?',
        [spec.execution_id,task_id]).fetchone()
    if row is None:
        return None
    receipt=BackfillDayCommitReceipt.model_validate_json(row[2])
    if (receipt.execution_id,receipt.owner,receipt.manifest_id,receipt.task_id,receipt.plan_task_id,receipt.plan_sha256,
        receipt.receipt_id,receipt.trade_date)!=(spec.execution_id,spec.owner,spec.manifest_id,task_id,spec.plan_task_id,
            spec.plan.content_sha256,row[0],row[1]) or receipt.trade_date not in spec.plan.missing_dates:
        raise ValueError('original day receipt differs from immutable owner/manifest/plan/day')
    if receipt.primary_identity!=DailyCanonicalPublisher.database_identity(store) or receipt.primary_identity!=spec.intent.primary_identity:
        raise ValueError('original committed day belongs to another physical primary')
    _calendar_facts(store,receipt.calendar,spec.plan.audit_start,spec.plan.completed_through)
    current=tuple(mark for mark in DailyCanonicalPublisher.collect_table_watermarks(store,receipt.trade_date) if mark.table_name in _RAW_DAY_TABLES)
    if current!=receipt.watermarks or sum(mark.row_count for mark in current if mark.table_name=='daily_bar')==0:
        raise ValueError('original committed day facts differ from durable receipt')
    observer=quota_observer
    if observer is None:
        quota=SourceQuotaStore(policy.quota_ledger_path)
        observer=QuotaBoundTransportObserver(store=quota,source=policy.quota_source,quota_units_per_window=policy.quota_units_per_window,
            window_kind=policy.quota_window_kind,clock=lambda:receipt.committed_at)
    bindings={binding.logical_request_id:binding for binding in receipt.source_requests}
    for saved in receipt.dispatch_receipts:
        binding=bindings[saved.logical_request_id]
        if (binding.source_account_sha256,binding.quota_source,binding.quota_ledger_device,binding.quota_ledger_inode)!=(
            policy.source_account_sha256,policy.quota_source,policy.quota_ledger_device,policy.quota_ledger_inode):
            raise ValueError('original day quota/account binding differs')
        actual=observer.get_call_attempt(logical_request_id=binding.logical_request_id,api_name=binding.api_name,call_ordinal=saved.call_ordinal)
        if actual is None or (actual.attempt_id,actual.source,actual.outcome,actual.dispatched_at,actual.committed_at)!=(
            saved.attempt_id,saved.source,saved.outcome,saved.dispatched_at,saved.committed_at):
            raise ValueError('original dispatch receipt is missing or changed')
    from rquant.data_collection_authority import CollectionCommitRecorder
    from rquant.data_collection_contracts import IngestionCommitReceipt
    collected=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt '
        'WHERE collector_id=? AND run_id=? AND trade_date=?',
        ['controlled_backfill',spec.execution_id+':'+receipt.trade_date.isoformat(),receipt.trade_date]).fetchall()
    if len(collected)!=1:
        raise ValueError('original day collection receipt is missing or duplicated')
    ingestion=CollectionCommitRecorder.verify_receipt(store,IngestionCommitReceipt.model_validate_json(collected[0][0]))
    if (ingestion.owner,ingestion.source_generation_id,ingestion.observations)!=(spec.owner,spec.intent.source_generation_id,receipt.source_observations):
        raise ValueError('original day valuation collection source changed')
    # A successor claim may verify a committed old token. It never rewrites these facts.
    return receipt


def commit_exact_backfill_day(state: BackfillStateStore,claim: ClaimedBackfillTask,*,spec: BackfillExecutionSpec,
        policy: Callable[[],DataCenterExecutionPolicy],control_sequence: int,calendar: MarketCalendarAuthority,
        frames: PreparedMarketFrames | None,status_rows: tuple[SecurityStatusDaily,...] = (),
        source_requests: tuple[BackfillSourceRequestBinding,...] = (),source_observations: tuple[SourceObservation,...] = (),
        dispatch_receipts: tuple[SourceTransportCallReceipt,...] = (),clock: Callable[[],datetime],
        writer_factory: Callable[[PrimaryWriterLease],DuckDBStore] | None = None) -> BackfillDayCommitReceipt | None:
    from rquant.backfill_execute_contracts import BackfillDayCommitReceipt
    from rquant.backfill_state import BackfillTaskMetrics
    from rquant.daily_canonical_publisher import DailyCanonicalPublisher
    from rquant.data_collection_authority import CollectionCommitRecorder,_calendar_facts
    from rquant.data_collection_contracts import CollectionRecorderConfig
    from rquant.market_backfill import _require_state_predecessor_coverage
    from rquant.storage.duckdb import DuckDBStore
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    from rquant.source_quota_store import SourceQuotaStore
    current=policy()
    require_execution_policy(current,kind='backfill',now=clock())
    require_stable_execution_policy(current,spec)
    active=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    if current.original_state_path!=state.path or current.policy_generation!=active.policy_generation:
        raise ValueError('original state or current execution policy changed')
    if claim.task_id not in {f'day-{day.isoformat()}' for day in spec.plan.missing_dates}:
        raise ValueError('original claim is outside exact immutable missing dates')
    day=date.fromisoformat(claim.task_id[4:])
    if claim.payload.get('trade_date')!=day.isoformat() or (frames is not None and frames.trade_date!=day):
        raise ValueError('original day task or prepared scope differs')
    began=clock()
    quota_observer=QuotaBoundTransportObserver(store=SourceQuotaStore(current.quota_ledger_path),source=current.quota_source,
        quota_units_per_window=current.quota_units_per_window,window_kind=current.quota_window_kind,clock=clock)
    valuation_response=None
    if frames is not None:
        frames,status_rows,valuation_response=_verify_prepared_day_material(spec,current,day=day,frames=frames,status_rows=status_rows,
            source_requests=source_requests,source_observations=source_observations,dispatch_receipts=dispatch_receipts,
            observer=quota_observer,now=clock())
    with PrimaryWriterGate(current.primary_writer_gate).acquire() as lease:
        def guard(connection: object,observed: datetime) -> None:
            lease.verify()
            status=state.verify_maintenance_claim(connection,claim,execution_id=spec.execution_id,owner=spec.owner,
                expected_sequence=control_sequence,now=observed)
            live=policy()
            require_execution_policy(live,kind='backfill',now=observed)
            window=maintenance_window(observed)
            if (live.policy_generation!=current.policy_generation or status.policy_generation!=current.policy_generation
                    or status.source_generation_id!=spec.intent.source_generation_id or not window.may_hold_writer
                    or observed>=window.interrupt_at):
                raise ValueError('current policy, source or production commit window changed')
        with state.commit_claim(claim,now=clock(),guard=guard) as protected:
            factory=writer_factory or (lambda borrowed:DuckDBStore(current.primary_writer_gate.primary_path,primary_writer_lease=borrowed))
            with factory(lease) as store:
                if DailyCanonicalPublisher.database_identity(store)!=spec.intent.primary_identity:
                    raise ValueError('current writer belongs to another physical primary')
                restored=verify_backfill_day_receipt(store,spec,task_id=claim.task_id,policy=current,quota_observer=quota_observer)
                if restored is not None:
                    protected.succeed(duration_seconds=max(0,(clock()-began).total_seconds()),now=clock())
                    return restored
                if frames is None:
                    return None
                if store._conn.execute('SELECT COUNT(*) FROM daily_bar WHERE trade_date=?',[day]).fetchone()[0]:
                    raise ValueError('planned whole-day gap was filled without this execution receipt')
                transaction_open=False
                try:
                    store._conn.execute('BEGIN')
                    transaction_open=True
                    _calendar_facts(store,calendar,spec.plan.audit_start,spec.plan.completed_through)
                    codes=set(frames.daily['ts_code'].astype(str))
                    _require_state_predecessor_coverage(store,trade_date=spec.plan.missing_dates[0],affected_codes=codes)
                    inserted=preserved=0
                    for table,frame,columns,write in (
                        ('daily_bar',frames.daily,('ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'),store.upsert_daily),
                        ('daily_basic',frames.daily_basic,('ts_code','trade_date','turnover_rate','volume_ratio','total_mv','circ_mv'),store.upsert_daily_basic),
                        ('adj_factor',frames.adj_factor,('ts_code','trade_date','adj_factor'),store.upsert_adj_factor)):
                        missing,kept=_missing_market_rows(store,table,frame,columns)
                        inserted+=write(missing)
                        preserved+=kept
                    missing_status,kept=_missing_status_rows(store,status_rows,day,codes)
                    inserted+=store.upsert_stock_status(missing_status,transaction_mode='existing',require_daily_keys=True)
                    preserved+=kept
                    for table in ('daily_indicator','daily_state'):
                        store._conn.execute(f'DELETE FROM {table} WHERE ts_code=ANY(?) AND trade_date>=?',[sorted(codes),day])
                    marks=tuple(mark for mark in DailyCanonicalPublisher.collect_table_watermarks(store,day) if mark.table_name in _RAW_DAY_TABLES)
                    if {item.api_name for item in source_requests}!=set(MARKET_EXECUTE_APIS):
                        raise ValueError('exact day is missing a critical original source request')
                    for binding in source_requests:
                        if (binding.owner,binding.execution_id,binding.manifest_id,binding.plan_sha256)!=(
                            spec.owner,spec.execution_id,spec.manifest_id,spec.plan.content_sha256):
                            raise ValueError('original source request belongs to another execution')
                        if binding.api_name!='namechange' and binding.parameters.get('trade_date')!=day.strftime('%Y%m%d'):
                            raise ValueError('original source request differs from exact day')
                        if not any(item.api_name==binding.api_name and item.request_sha256==canonical_sha256(binding.parameters)
                            for item in source_observations):
                            raise ValueError('actual source response observation is missing')
                    if any(item.observed_at>clock() for item in source_observations):
                        raise ValueError('actual source observation is future')
                    receipt=BackfillDayCommitReceipt(execution_id=spec.execution_id,owner=spec.owner,manifest_id=spec.manifest_id,
                        task_id=claim.task_id,plan_task_id=spec.plan_task_id,plan_sha256=spec.plan.content_sha256,trade_date=day,
                        committed_claim_token=claim.claim_token,committed_attempt=claim.attempt,control_sequence=control_sequence,
                        policy_generation=current.policy_generation,source_requests=source_requests,source_observations=source_observations,
                        dispatch_receipts=dispatch_receipts,primary_identity=spec.intent.primary_identity,calendar=calendar,
                        inserted_rows=inserted,preserved_rows=preserved,watermarks=marks,committed_at=clock())
                    if len(receipt.model_dump_json().encode())>256*1024:
                        raise ValueError('original day receipt exceeds bounded capacity')
                    store._conn.execute('INSERT INTO backfill_day_commit_receipt VALUES (?,?,?,?,?,?)',
                        [spec.execution_id,claim.task_id,receipt.receipt_id,day,receipt.model_dump_json(),receipt.committed_at])
                    recorder=CollectionCommitRecorder(CollectionRecorderConfig(collector_id='controlled_backfill',run_id=spec.execution_id+':'+day.isoformat(),
                        owner=spec.owner,code_commit=current.code_commit,source_generation_id=spec.intent.source_generation_id,calendar=calendar),clock=clock)
                    recorder.record_daily(store,day,observations=source_observations,daily_basic_response=valuation_response)
                    verify_backfill_day_receipt(store,spec,task_id=claim.task_id,policy=current,quota_observer=quota_observer)
                    protected.verify(now=clock())
                    store._conn.execute('COMMIT')
                    transaction_open=False
                except BaseException as primary:
                    if transaction_open:
                        try:
                            store._conn.execute('ROLLBACK')
                        except BaseException as cleanup:
                            raise BaseExceptionGroup('fact commit response and rollback cleanup both failed',[primary,cleanup]) from None
                    raise
                protected.succeed(duration_seconds=max(0,(clock()-began).total_seconds()),metrics=BackfillTaskMetrics(
                    request_count=len(dispatch_receipts),returned_rows=sum(item.row_count for item in source_observations),
                    written_rows=inserted,covered_sessions=1),now=clock())
                return receipt


class BackfillExecutionStep(RuntimeContractModel):
    outcome: Literal['idle','day_committed','derived_committed','completion_waiting','completed','paused','partial']
    execution: MaintenanceExecutionStatus
    task_id: str | None = None


class BackfillExecutionWorker:
    """Advance one original finite manifest task, with its original lease and quota."""
    def __init__(self,state: BackfillStateStore,*,policy: Callable[[],DataCenterExecutionPolicy],
            adapter_factory: Callable[[ControlledTransportObserver],object],
            calendar: Callable[[BackfillExecutionSpec],MarketCalendarAuthority],
            clock: Callable[[],datetime] | None = None,monotonic: Callable[[],float] = time.monotonic,
            derived_step: Callable[[BackfillExecutionSpec,ClaimedBackfillTask,int],bool] | None = None,
            completion_step: Callable[[BackfillExecutionSpec,ClaimedBackfillTask,int],bool] | None = None,
            stopped: Event | None = None) -> None:
        if not state.maintenance_enabled:
            raise ValueError('worker requires original controlled maintenance state')
        self.state,self.policy,self.adapter_factory,self.calendar=state,policy,adapter_factory,calendar
        self.clock=clock or (lambda:datetime.now(UTC))
        self.monotonic=monotonic
        self.derived_step,self.completion_step=derived_step,completion_step
        self.completion_runtime=None
        self.stopped=stopped or Event()
        self._active_store: DuckDBStore | None = None
        self._kind: Literal['backfill','financial']='backfill'

    def request_stop(self) -> None:
        import duckdb
        self.stopped.set()
        if self._active_store is not None:
            try:
                self._active_store._conn.interrupt()
            except duckdb.ConnectionException:
                # A stop may race with normal connection cleanup. The durable stop remains set.
                pass

    def _guard(self,spec: BackfillExecutionSpec,claim: ClaimedBackfillTask,sequence: int,deadline: float) -> None:
        window=maintenance_window(self.clock())
        if self.stopped.is_set() or self.monotonic()>=deadline or not window.may_hold_writer or self.clock()>=window.interrupt_at:
            raise ValueError('maintenance boundary requested stop')
        connection=self.state._connect()
        try:
            self.state.verify_maintenance_claim(connection,claim,execution_id=spec.execution_id,owner=spec.owner,
                expected_sequence=sequence,now=self.clock())
        finally:
            connection.close()
        current=self.policy()
        require_stable_execution_policy(current,spec)
        require_execution_policy(current,kind=self._kind,now=self.clock())
        if current.policy_generation!=self.state.get_maintenance_status(spec.execution_id,owner=spec.owner).policy_generation:
            raise ValueError('maintenance policy changed before dispatch')

    @contextmanager
    def _heartbeat(self,spec: BackfillExecutionSpec,claim: ClaimedBackfillTask,sequence: int,
                   deadline: float) -> Iterator[Callable[[],None]]:
        finished=Event()
        failures: list[BaseException]=[]
        def renew() -> None:
            while not finished.wait(40):
                try:
                    def guard(connection: object,now: datetime) -> None:
                        if self.stopped.is_set() or self.monotonic()>=deadline:
                            raise ValueError('maintenance heartbeat reached stop boundary')
                        self.state.verify_maintenance_claim(connection,claim,execution_id=spec.execution_id,
                            owner=spec.owner,expected_sequence=sequence,now=now)
                    self.state.renew_task_claim(claim,lease_seconds=120,now=self.clock(),guard=guard)
                except BaseException as error:
                    failures.append(error)
                    return
        thread=Thread(target=renew,name='data-center-original-claim-heartbeat',daemon=False)
        thread.start()
        def stop() -> None:
            finished.set()
            thread.join(timeout=6)
            if thread.is_alive():
                raise RuntimeError('original claim heartbeat did not stop')
            if failures:
                raise ValueError('original claim heartbeat failed') from failures[0]
        try:
            yield stop
        finally:
            stop()

    def _pause(self,execution_id: str,owner: str,*,code: str,task_id: str | None = None,
               claim: ClaimedBackfillTask | None = None) -> BackfillExecutionStep:
        if claim is not None:
            task=self.state.get_task(claim.manifest_id,claim.task_id)
            if task.status=='running' and task.claim_token==claim.claim_token:
                self.state.release_task_claim(claim,now=self.clock())
        current=self.state.get_maintenance_status(execution_id,owner=owner)
        state='paused' if current.pause_requested or code=='maintenance_window_closed' else 'partial'
        if current.status not in {'completed','failed','paused','partial'}:
            self.state.release_expired_maintenance_claims(execution_id,owner=owner,expected_sequence=current.control_sequence,now=self.clock())
            if self.state.get_manifest_status(current.manifest_id).running:
                return BackfillExecutionStep(outcome='idle',execution=current,task_id=task_id)
            current=self.state.transition_maintenance(execution_id,owner=owner,expected_sequence=current.control_sequence,
                status=state,failure_code=code,now=self.clock())
        return BackfillExecutionStep(outcome=state,execution=current,task_id=task_id)

    def _writer(self,lease: PrimaryWriterLease) -> DuckDBStore:
        from rquant.storage.duckdb import DuckDBStore
        if self.stopped.is_set():
            raise ValueError('maintenance writer requested stop')
        store=DuckDBStore(lease.config.primary_path,primary_writer_lease=lease)
        self._active_store=store
        if self.stopped.is_set():
            store._conn.interrupt()
        return store

    def _reader(self,path: Path) -> DuckDBStore:
        from rquant.storage.duckdb import DuckDBStore
        if self.stopped.is_set():
            raise ValueError('maintenance reader requested stop')
        store=DuckDBStore(path,read_only=True)
        self._active_store=store
        if self.stopped.is_set():
            store._conn.interrupt()
        return store

    def run_one(self,execution_id: str,*,owner: str) -> BackfillExecutionStep:
        import duckdb
        from rquant.backfill_execute_page_backend import build_backfill_execution_manifest
        from rquant.backfill_state import StaleTaskClaimError
        from rquant.market_backfill import prepare_market_frames
        from rquant.security_status import DailySecurityKey,SecurityStatusDaily,prefetch_namechange_context,prefetch_security_status_for_date
        from rquant.source_quota_store import SourceQuotaStore
        from rquant.storage.primary_writer_gate import PrimaryWriterBusy
        spec=self.state.get_backfill_execution_spec(execution_id,owner=owner)
        if spec is None:
            raise ValueError('owned original execution is unavailable')
        if self.state.load_manifest(spec.manifest_id)!=build_backfill_execution_manifest(spec):
            raise ValueError('original execution manifest differs from sealed exact tasks')
        current=self.state.get_maintenance_status(execution_id,owner=owner)
        if current.status in {'completed','failed','paused','partial'}:
            return BackfillExecutionStep(outcome='idle',execution=current)
        if current.pause_requested or self.stopped.is_set():
            return self._pause(execution_id,owner,code='pause_requested')
        if not maintenance_window(self.clock()).may_start_day:
            return self._pause(execution_id,owner,code='maintenance_window_closed')
        try:
            policy=self.policy()
            require_execution_policy(policy,kind='backfill',now=self.clock())
            require_stable_execution_policy(policy,spec)
            if policy.policy_generation!=current.policy_generation or policy.original_state_path!=self.state.path:
                raise ValueError('current original execution policy changed')
            require_market_plan_policy(spec.plan,policy,now=self.clock())
            quota=SourceQuotaStore(policy.quota_ledger_path)
            window_id,start,end=quota._quota_window(self.clock().astimezone(UTC),window_kind=policy.quota_window_kind)
            quota.declare_window(source=policy.quota_source,window_id=window_id,starts_at=start,resets_at=end,total_units=policy.quota_units_per_window)
            original=QuotaBoundTransportObserver(store=quota,source=policy.quota_source,
                quota_units_per_window=policy.quota_units_per_window,window_kind=policy.quota_window_kind,clock=self.clock)
            original.remaining(now=self.clock())
        except (OSError,ValueError,SourceQuotaConflictError,SourceQuotaExhaustedError) as error:
            code='source_quota_exhausted' if isinstance(error,SourceQuotaExhaustedError) else 'current_execution_policy_unavailable'
            return self._pause(execution_id,owner,code=code)
        if current.status=='queued':
            current=self.state.transition_maintenance(execution_id,owner=owner,expected_sequence=current.control_sequence,status='running',now=self.clock())
        claim=self.state.claim_task(spec.manifest_id,worker_id='controlled-maintenance',lease_seconds=120,
            now=self.clock(),maintenance_execution_id=execution_id)
        if claim is None:
            return BackfillExecutionStep(outcome='idle',execution=self.state.get_maintenance_status(execution_id,owner=owner))
        deadline=min(self.monotonic()+1800,self.monotonic()+max(0,(maintenance_window(self.clock()).interrupt_at-self.clock()).total_seconds()))
        try:
            guard=lambda:self._guard(spec,claim,current.control_sequence,deadline)
            if claim.task_id=='tail-derived' or claim.task_id=='verify-completion':
                action=self.derived_step if claim.task_id=='tail-derived' else self.completion_step
                if action is None and claim.task_id=='tail-derived':
                    from rquant.data_center_maintenance_runtime import run_derived_tail
                    done=run_derived_tail(self.state,claim,spec=spec,policy=self.policy,
                        control_sequence=current.control_sequence,clock=self.clock,guard=guard,stopped=self.stopped,
                        prepare_context=lambda:self._heartbeat(spec,claim,current.control_sequence,deadline),writer_factory=self._writer,reader_factory=self._reader)
                    if not done:
                        return self._pause(execution_id,owner,code='maintenance_window_closed',task_id=claim.task_id,claim=claim)
                elif action is None and self.completion_runtime is not None:
                    done=self.completion_runtime.run(spec,claim,current.control_sequence,
                        prepare_context=lambda:self._heartbeat(spec,claim,current.control_sequence,deadline),guard=guard,
                        stopped=self.stopped,writer_factory=self._writer,reader_factory=self._reader)
                elif action is None:
                    raise ValueError('original maintenance continuation is not installed')
                else:
                    done=action(spec,claim,current.control_sequence)
                return BackfillExecutionStep(outcome=('completed' if claim.task_id=='verify-completion' else 'derived_committed') if done else 'completion_waiting',
                    execution=self.state.get_maintenance_status(execution_id,owner=owner),task_id=claim.task_id)
            guard()
            restored=commit_exact_backfill_day(self.state,claim,spec=spec,policy=self.policy,
                control_sequence=current.control_sequence,calendar=self.calendar(spec),frames=None,clock=self.clock,writer_factory=self._writer)
            if restored is None:
                observer=ControlledTransportObserver(original,policy=self.policy,kind='backfill',
                    material_directory=policy.source_material_directory,clock=self.clock,claim_guard=guard)
                bound=ControlledMarketAdapter(self.adapter_factory(observer),observer,spec=spec,policy=policy)
                with self._heartbeat(spec,claim,current.control_sequence,deadline) as stop_heartbeat:
                    names=prefetch_namechange_context(bound,start=spec.plan.estimate.assumptions.status_namechange_start,
                        source_as_of=spec.plan.estimate.assumptions.status_source_as_of)
                    day=date.fromisoformat(claim.task_id[4:])
                    frames=prepare_market_frames(bound,day,strict_date_scope=True)
                    status=prefetch_security_status_for_date(bound,tuple(DailySecurityKey(ts_code=code,trade_date=day)
                        for code in sorted(set(frames.daily['ts_code']))),namechange_context=names,
                        ingested_at=self.clock(),strict_stock_st_crosscheck=True)
                    # Local ingestion follows the original SDK observations.
                    ingested_at=self.clock()
                    status_rows=tuple(SecurityStatusDaily.model_validate_json(row.model_copy(
                        update={'ingested_at':ingested_at}).model_dump_json()) for row in status.rows)
                    stop_heartbeat()
                    guard()
                    self.state.renew_task_claim(claim,lease_seconds=120,now=self.clock(),guard=lambda connection,now:
                        self.state.verify_maintenance_claim(connection,claim,execution_id=spec.execution_id,owner=owner,
                            expected_sequence=current.control_sequence,now=now))
                    restored=commit_exact_backfill_day(self.state,claim,spec=spec,policy=self.policy,
                        control_sequence=current.control_sequence,calendar=self.calendar(spec),frames=frames,status_rows=status_rows,
                        source_requests=tuple(bound.bindings),source_observations=tuple(observer.observations),
                        dispatch_receipts=observer.current_receipts(),clock=self.clock,writer_factory=self._writer)
            if restored is not None:
                for binding in restored.source_requests:
                    if binding.api_name!='namechange':
                        cleanup_verified_source_material(binding,directory=policy.source_material_directory,observer=original)
            return BackfillExecutionStep(outcome='day_committed',execution=self.state.get_maintenance_status(execution_id,owner=owner),task_id=claim.task_id)
        except PrimaryWriterBusy:
            task=self.state.get_task(claim.manifest_id,claim.task_id)
            if task.status=='running' and task.claim_token==claim.claim_token:
                self.state.release_task_claim(claim,now=self.clock())
            return BackfillExecutionStep(outcome='idle',execution=self.state.get_maintenance_status(execution_id,owner=owner),task_id=claim.task_id)
        except StaleTaskClaimError:
            task=self.state.get_task(claim.manifest_id,claim.task_id)
            if task.claim_token!=claim.claim_token:
                return BackfillExecutionStep(outcome='idle',execution=self.state.get_maintenance_status(execution_id,owner=owner),task_id=claim.task_id)
            return self._pause(execution_id,owner,code='execution_control_changed',task_id=claim.task_id,claim=claim)
        except (OSError,ValueError,RuntimeError,duckdb.Error,ExceptionGroup,SourceQuotaConflictError,SourceQuotaExhaustedError) as error:
            code='source_quota_exhausted' if isinstance(error,SourceQuotaExhaustedError) else 'source_or_committed_result_unconfirmed'
            return self._pause(execution_id,owner,code=code,task_id=claim.task_id,claim=claim)
        finally:
            self._active_store=None
