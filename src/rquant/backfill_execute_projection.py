"""Bounded, read-only publication of original maintenance and quota facts."""
from __future__ import annotations

import os
import sqlite3
import stat
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC,datetime
from pathlib import Path
from typing import Literal

from pydantic import Field,model_validator

from rquant.backfill_execute_contracts import FINANCIAL_EXECUTE_APIS,MaintenanceExecutionStatus,maintenance_window
from rquant.backfill_state import BackfillStateStore
from rquant.data_audit_report_job_projection import _file_identity,_sidecar_state
from rquant.runtime_contracts import RuntimeContractModel,AwareUtcDatetime,canonical_sha256
from rquant.serving_read_models import ServingProjectionPayload

DATA_CENTER_EXECUTION_TABLES=frozenset({'data_center_execution','data_center_execution_state','data_center_financial_source','data_center_execution_event'})


class ExecutionProjectionRow(RuntimeContractModel):
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    owner: str = Field(min_length=1,max_length=256)
    status_json: str = Field(min_length=1,max_length=8192)


class ExecutionStateProjectionRow(RuntimeContractModel):
    configured: bool
    backfill_enabled: bool
    financial_enabled: bool
    may_start: bool
    observed_at: AwareUtcDatetime


class ExecutionEventProjectionRow(RuntimeContractModel):
    event_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    owner: str = Field(min_length=1,max_length=256)
    event_type: Literal['task_started','task_finished','pause_requested','resume_requested']
    occurred_at: AwareUtcDatetime
    task_id: str | None = Field(default=None,max_length=256)
    task_status: Literal['running','succeeded','failed','skipped'] | None = None
    attempts: int | None = Field(default=None,ge=1,strict=True)
    control_sequence: int | None = Field(default=None,ge=2,strict=True)
    failure_code: str | None = Field(default=None,max_length=80)

    @model_validator(mode='after')
    def bind(self) -> ExecutionEventProjectionRow:
        if self.event_id!=canonical_sha256(self.model_dump(mode='python',exclude={'event_id'})):
            raise ValueError('original execution record content changed')
        task=self.event_type.startswith('task_')
        if task:
            if self.task_id is None or self.task_status is None or self.attempts is None or self.control_sequence is not None:
                raise ValueError('original task record lacks its original state')
            if (self.event_type=='task_started')!=(self.task_status=='running'):
                raise ValueError('original task record disagrees with its state')
        elif self.control_sequence is None or any(value is not None for value in (self.task_id,self.task_status,self.attempts,self.failure_code)):
            raise ValueError('original control record mixes task facts')
        return self


def _execution_record(**values: object) -> ExecutionEventProjectionRow:
    return ExecutionEventProjectionRow(event_id=canonical_sha256(values),**values)


def _recent_execution_records(connection: sqlite3.Connection,statuses: tuple[MaintenanceExecutionStatus,...],*,observed: datetime) -> tuple[ExecutionEventProjectionRow,...]:
    if not statuses:
        return ()
    known={item.execution_id:item for item in statuses}
    placeholders=','.join('?' for _ in known)
    rows=connection.execute(f'SELECT e.execution_id,e.owner,t.task_id,t.status,t.attempts,t.claimed_at,t.finished_at,t.failure_json '
        'FROM backfill_task t JOIN data_center_maintenance_execution e ON e.manifest_id=t.manifest_id '
        f'WHERE e.execution_id IN ({placeholders}) AND (t.claimed_at IS NOT NULL OR t.finished_at IS NOT NULL) '
        'ORDER BY COALESCE(t.finished_at,t.claimed_at) DESC,t.ordinal DESC LIMIT 20',tuple(known)).fetchall()
    records=[]
    for row in rows:
        if row['status'] not in {'running','succeeded','failed','skipped'}:
            continue
        stamp=row['claimed_at'] if row['status']=='running' else row['finished_at']
        if stamp is None:
            continue
        failure=None
        if row['failure_json'] is not None:
            from rquant.backfill_state import BackfillFailure
            if len(row['failure_json'].encode())>4096:
                raise ValueError('original task failure record exceeds capacity')
            failure=BackfillFailure.model_validate_json(row['failure_json']).code
        records.append(_execution_record(execution_id=row['execution_id'],owner=row['owner'],
            event_type='task_started' if row['status']=='running' else 'task_finished',occurred_at=datetime.fromisoformat(stamp),
            task_id=row['task_id'],task_status=row['status'],attempts=row['attempts'],control_sequence=None,failure_code=failure))
    if connection.execute('SELECT COUNT(*) FROM data_center_maintenance_control').fetchone()[0]>8192:
        raise ValueError('original control record history exceeds capacity')
    controls=connection.execute(f'SELECT execution_id,payload_json,result_json FROM data_center_maintenance_control '
        f'WHERE execution_id IN ({placeholders}) ORDER BY rowid DESC LIMIT 20',tuple(known)).fetchall()
    for row in controls:
        if len(row['payload_json'].encode())>4096 or len(row['result_json'].encode())>8192:
            raise ValueError('original control record exceeds capacity')
        payload=json.loads(row['payload_json'])
        result=MaintenanceExecutionStatus.model_validate_json(row['result_json'])
        current=known[row['execution_id']]
        if (not isinstance(payload,dict) or set(payload)!={'execution_id','owner','expected_sequence','action'} or
                (payload['execution_id'],payload['owner'],payload['expected_sequence']+1)!=(result.execution_id,result.owner,result.control_sequence) or
                (result.execution_id,result.owner,result.manifest_id)!=(current.execution_id,current.owner,current.manifest_id) or
                result.control_sequence>current.control_sequence or payload['action'] not in {'pause','resume'}):
            raise ValueError('original control record owner or result changed')
        records.append(_execution_record(execution_id=result.execution_id,owner=result.owner,
            event_type='pause_requested' if payload['action']=='pause' else 'resume_requested',occurred_at=result.updated_at,
            task_id=None,task_status=None,attempts=None,control_sequence=result.control_sequence,failure_code=None))
    if any(item.occurred_at>observed for item in records):
        raise ValueError('original execution record is newer than publication')
    return tuple(sorted(records,key=lambda item:(item.occurred_at,item.event_id))[-20:])


class FinancialSourceProjectionRow(RuntimeContractModel):
    api_name: Literal['fina_indicator','income','balancesheet','cashflow','forecast','express','dividend']
    permission_status: Literal['verified','unknown','unavailable']
    evidence_source: Literal['supplier_account_response','offline_fixture'] | None = None
    scope_start: str | None = None
    scope_end: str | None = None
    expires_at: str | None = None
    remaining_units: int | None = Field(default=None,ge=0)
    total_units: int | None = Field(default=None,ge=0)
    resets_at: str | None = None


@contextmanager
def _readonly_state(path: Path,*,max_bytes: int=64*1024*1024) -> Iterator[sqlite3.Connection]:
    if not path.is_absolute() or path.resolve(strict=False)!=path:
        raise ValueError('original state path must be canonical')
    before=os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_size>max_bytes:
        raise ValueError('original state file exceeds bounded regular-file contract')
    sidecars,_=_sidecar_state(path)
    uri=path.as_uri()+'?mode=ro'+('&immutable=1' if sidecars==(None,None) else '')
    connection=sqlite3.connect(uri,uri=True,timeout=5)
    connection.row_factory=sqlite3.Row
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        yield connection
        connection.rollback()
    finally:
        connection.close()
    after=os.lstat(path)
    after_sidecars,_=_sidecar_state(path)
    if (_file_identity(before)!=_file_identity(after) or sidecars!=after_sidecars
            or sidecars==(None,None) and before.st_ctime_ns!=after.st_ctime_ns):
        raise ValueError('original state rotated while read')


def read_execution_projection_rows(rows: tuple[dict[str,object],...]) -> tuple[MaintenanceExecutionStatus,...]:
    if len(rows)>50:
        raise ValueError('execution index exceeds bounded capacity')
    result=[]
    for item in rows:
        row=ExecutionProjectionRow.model_validate(item)
        value=MaintenanceExecutionStatus.model_validate_json(row.status_json)
        if (row.execution_id,row.owner)!=(value.execution_id,value.owner) or row.status_json!=value.model_dump_json():
            raise ValueError('execution publication differs from original owner/status')
        result.append(value)
    if len({item.execution_id for item in result})!=len(result):
        raise ValueError('execution publication duplicates identities')
    return tuple(result)


def validate_execution_projections(projections: dict[str,ServingProjectionPayload]) -> None:
    from rquant.serving_read_models import _projection_json_bytes
    if not DATA_CENTER_EXECUTION_TABLES<=projections.keys() or len({projections[name].available_at for name in DATA_CENTER_EXECUTION_TABLES})!=1:
        raise ValueError('execution/source projection group is incomplete')
    bounded_names=DATA_CENTER_EXECUTION_TABLES|{'data_collection_dataset'}
    if sum(_projection_json_bytes(projections[name].rows) for name in bounded_names if name in projections)>512*1024:
        raise ValueError('combined data center projection capacity exceeds 512 KiB')
    statuses=read_execution_projection_rows(projections['data_center_execution'].rows)
    known={(item.execution_id,item.owner) for item in statuses}
    events=tuple(ExecutionEventProjectionRow.model_validate(row) for row in projections['data_center_execution_event'].rows)
    if (len(events)>20 or len({item.event_id for item in events})!=len(events) or
            any((item.execution_id,item.owner) not in known or item.occurred_at>projections['data_center_execution_event'].available_at for item in events)):
        raise ValueError('execution records differ from original owner, time or capacity')
    state=projections['data_center_execution_state']
    if len(state.rows)!=1:
        raise ValueError('execution state must have exactly one bounded row')
    value=ExecutionStateProjectionRow.model_validate(state.rows[0])
    if value.observed_at!=state.available_at:
        raise ValueError('execution state observation differs from its publication')
    sources=tuple(FinancialSourceProjectionRow.model_validate(row) for row in projections['data_center_financial_source'].rows)
    if tuple(row.api_name for row in sources)!=FINANCIAL_EXECUTE_APIS:
        raise ValueError('financial sources omit or duplicate an original interface')


def project_data_center_execution(*,state_path: Path | None,policy_path: Path | None,
        observed_at: datetime) -> tuple[ServingProjectionPayload,...]:
    observed=observed_at.astimezone(UTC)
    rows=[]
    records=()
    if state_path is not None and state_path.exists():
        with _readonly_state(state_path) as connection:
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='data_center_maintenance_execution'").fetchone():
                if connection.execute('SELECT COUNT(*) FROM data_center_maintenance_execution').fetchone()[0]>4096:
                    raise ValueError('original execution history exceeds fixed capacity')
                identifiers=connection.execute('SELECT execution_id,owner FROM data_center_maintenance_execution '
                    'ORDER BY rowid DESC LIMIT 50').fetchall()
                for item in identifiers:
                    value=BackfillStateStore._maintenance_status_on(connection,item['execution_id'],owner=item['owner'])
                    if value.updated_at>observed:
                        raise ValueError('original execution state is newer than publication')
                    rows.append(ExecutionProjectionRow(execution_id=value.execution_id,owner=value.owner,status_json=value.model_dump_json()).model_dump(mode='json'))
                records=_recent_execution_records(connection,read_execution_projection_rows(tuple(rows)),observed=observed)
    sources=tuple(FinancialSourceProjectionRow(api_name=api,permission_status='unknown') for api in FINANCIAL_EXECUTE_APIS)
    enabled_backfill=enabled_financial=False
    configured=state_path is not None and policy_path is not None
    if configured:
        from rquant.backfill_execute import load_execution_policy,require_execution_policy,require_current_entitlement
        from rquant.source_quota_store import SourceQuotaStore,SourceQuotaExhaustedError
        policy=load_execution_policy(policy_path)
        if policy.original_state_path!=state_path:
            raise ValueError('execution source differs from original state')
        for kind in ('backfill','financial'):
            try:
                require_execution_policy(policy,kind=kind,now=observed)
            except (ValueError,OSError):
                continue
            if kind=='backfill':
                enabled_backfill=True
            else:
                enabled_financial=True
        remaining=total=reset=None
        if policy.quota_ledger_path.exists():
            current=policy.quota_ledger_path.stat()
            if (current.st_dev,current.st_ino)!=(policy.quota_ledger_device,policy.quota_ledger_inode):
                raise ValueError('original quota source identity changed')
            with _readonly_state(policy.quota_ledger_path) as connection:
                try:
                    window=SourceQuotaStore._active_window(connection,policy.quota_source,observed)
                except SourceQuotaExhaustedError:
                    pass
                else:
                    remaining=max(0,SourceQuotaStore._remaining_in_window(connection,window,observed))
                    total,reset=window['total_units'],window['resets_at']
        values=[]
        for api in FINANCIAL_EXECUTE_APIS:
            right=next((entry for entry in policy.entitlement_evidence if entry.api_name==api),None)
            status='unknown'
            if right is not None:
                status=right.status
                if status=='verified':
                    try:
                        require_current_entitlement(policy,api_name=api,
                            parameters={} if right.full_market else {'ts_code':right.allowed_symbols[0]},now=observed)
                    except (ValueError,OSError,IndexError):
                        status='unknown'
            values.append(FinancialSourceProjectionRow(api_name=api,permission_status=status,
                evidence_source=None if right is None else right.proof_source,
                scope_start=None if right is None or right.scope_start is None else right.scope_start.isoformat(),
                scope_end=None if right is None or right.scope_end is None else right.scope_end.isoformat(),
                expires_at=None if right is None or right.expires_at is None else right.expires_at.isoformat(),
                remaining_units=remaining,total_units=total,resets_at=reset))
        sources=tuple(values)
    state=ExecutionStateProjectionRow(configured=configured,backfill_enabled=enabled_backfill,
        financial_enabled=enabled_financial,may_start=maintenance_window(observed).may_start_day,observed_at=observed)
    return tuple(sorted((ServingProjectionPayload(table_name='data_center_execution',available_at=observed,rows=tuple(rows)),
        ServingProjectionPayload(table_name='data_center_execution_event',available_at=observed,rows=tuple({
            **item.model_dump(mode='json'),
            'occurred_at':item.occurred_at.isoformat(timespec='microseconds'),
        } for item in records)),
        ServingProjectionPayload(table_name='data_center_execution_state',available_at=observed,rows=(state.model_dump(mode='json'),)),
        ServingProjectionPayload(table_name='data_center_financial_source',available_at=observed,rows=tuple(row.model_dump(mode='json') for row in sources))),key=lambda p:p.table_name))
