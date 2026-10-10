from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest


@pytest.mark.parametrize('action',['pause','resume'])
def test_real_control_public_receipt_and_original_lookup_bind_both_execution_ids(tmp_path: Path,action: str) -> None:
    from tests.unit.test_backfill_execute_admission import _state_execution
    from rquant.web.routes.backfill_execution import _public_receipt
    from rquant.web.models.backfill_execution import ExecutionControlRequest
    from rquant.web.backfill_plan_command_gateway import BackfillPlanWireReceipt
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    current=state.admit_backfill_execution(spec,manifest,now=now)
    if action=='resume':
        current=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=current.control_sequence,status='paused',now=now)
    body=ExecutionControlRequest(kind=f'{action}_data_center_execution',command_id=f'actual-{action}',requested_at=now,
        execution_id=spec.execution_id,expected_sequence=current.control_sequence)
    accepted=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id=body.command_id,
        expected_sequence=body.expected_sequence,action=action,now=now)
    wire=BackfillPlanWireReceipt(command_id=body.command_id,status='succeeded',enqueued_at=now,completed_at=now,
        result={'outcome':'control_accepted','execution':accepted.model_dump(mode='json')})
    before=wire.model_dump_json()
    public=_public_receipt(body,wire,owner=spec.owner)
    retry=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id=body.command_id,
        expected_sequence=body.expected_sequence,action=action,now=now+timedelta(seconds=1))
    assert retry==accepted and _public_receipt(body,wire,owner=spec.owner)==public
    assert wire.model_dump_json()==before
    assert public.status=='control_accepted' and public.command_id==body.command_id
    assert public.execution_id==public.execution.execution_id==spec.execution_id
    assert public.execution.control_sequence==body.expected_sequence+1
    from rquant.web.backfill_plan_command_gateway import BackfillPlanCommandInvalidReceiptError
    with pytest.raises(BackfillPlanCommandInvalidReceiptError):
        _public_receipt(body,wire,owner='another-owner')


def test_financial_prepare_recovery_binds_entire_original_command(tmp_path: Path,monkeypatch) -> None:
    from tests.unit.test_financial_runtime import _financial_runtime_setup,NOW
    from rquant.backfill_execute_page_backend import BackfillExecutePageBackend,BackfillExecutePageBackendConfig
    from rquant.page_control import PrepareFinancialCollection
    from rquant.financial_runtime import FinancialExecutionIntent,FinancialCollectionPlan
    from rquant.runtime_contracts import canonical_sha256
    state,spec,current,archive,_,_=_financial_runtime_setup(tmp_path,monkeypatch)
    command=PrepareFinancialCollection(command_id='new-financial-prepare',requested_at=NOW,actor_id=spec.owner,
        audit_report_hash='a'*64,security_scope='selected_securities',selected_securities=('600000.SH',),
        start_date=spec.plan.start_date,end_date=spec.plan.end_date,report_periods=spec.plan.report_periods)
    plan=FinancialCollectionPlan.create(**{**spec.plan.scope_body(),'prepare_command_id':command.command_id})
    intent=FinancialExecutionIntent(execution_id=plan.execution_id,owner=plan.owner,prepare_command_id=command.command_id,
        plan=plan,plan_sha256=plan.content_sha256,source_generation_id=plan.source_generation_id,primary_identity=plan.primary_identity,
        policy_generation=current[0].policy_generation,nonce_sha256='b'*64,issued_at=NOW,expires_at=NOW+timedelta(minutes=5),
        prepare_request_sha256=canonical_sha256(command.model_dump(mode='python')))
    state.persist_maintenance_intent(intent)
    config=BackfillExecutePageBackendConfig(policy_path=tmp_path/'policy.json',original_state_path=state.path,
        plan_state_path=tmp_path/'plans.sqlite',plan_directory=tmp_path/'plans',audit_state_path=tmp_path/'audit.sqlite',
        audit_directory=tmp_path/'audit',collection_directory=tmp_path/'collection',allowed_owners=(spec.owner,))
    backend=BackfillExecutePageBackend(config,clock=lambda:NOW)
    result=backend.recover(command)
    assert result['outcome']=='execution_prepared'
    assert result['confirmation']['security_count']==1
    assert 'archive_path' not in str(result) and 'queries' not in str(result)
    for change in ({'audit_report_hash':'c'*64},{'security_scope':'available_securities','selected_securities':()},
            {'selected_securities':('600001.SH',)},{'requested_at':NOW+timedelta(seconds=1)}):
        with pytest.raises(ValueError,match='different|differs|changed'):
            backend.recover(command.model_copy(update=change))


def test_financial_browser_scope_rejects_unsorted_symbols_and_nonquarter_periods() -> None:
    from rquant.page_control import PrepareFinancialCollection
    from datetime import date,datetime,UTC
    values=dict(command_id='prepare',requested_at=datetime(2026,10,5,10,tzinfo=UTC),actor_id='fixture',
        audit_report_hash='a'*64,security_scope='selected_securities',selected_securities=('600000.SH',),
        start_date=date(2026,10,1),end_date=date(2026,10,5),report_periods=(date(2026,6,30),))
    for change in ({'selected_securities':('600001.SH','600000.SH')},{'report_periods':(date(2026,6,29),)},
            {'security_scope':'available_securities'},{'start_date':date(2026,10,6)}):
        with pytest.raises(ValueError):
            PrepareFinancialCollection(**{**values,**change})


def test_original_execution_projection_is_readonly_and_keeps_owner_and_counts(tmp_path: Path) -> None:
    import hashlib
    from tests.unit.test_backfill_execute_admission import _state_execution
    from rquant.backfill_execute_projection import project_data_center_execution,read_execution_projection_rows
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.glob('state.sqlite3*')}
    projections=project_data_center_execution(state_path=state.path,policy_path=None,observed_at=now+timedelta(days=1))
    index=next(p for p in projections if p.table_name=='data_center_execution')
    statuses=read_execution_projection_rows(index.rows)
    assert len(statuses)==1 and statuses[0].owner==spec.owner
    assert statuses[0].total_tasks==1 and statuses[0].completed_tasks==0
    assert before=={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.glob('state.sqlite3*')}
    source=next(p for p in projections if p.table_name=='data_center_financial_source')
    assert len(source.rows)==7 and all(p['permission_status']=='unknown' and p['remaining_units'] is None for p in source.rows)
    from rquant.serving_page_projection_source import LabPageProjectionSnapshot
    assert LabPageProjectionSnapshot.create(available_at=now+timedelta(days=1),data_center_execution_projections=projections)


def test_original_execution_recent_records_bind_original_task_control_owner_time_and_twenty_limit(tmp_path: Path) -> None:
    import json
    from tests.unit.test_backfill_execute_admission import _state_execution
    from rquant.backfill_execute_projection import project_data_center_execution,ExecutionEventProjectionRow
    from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
    from rquant.web.models.backfill_execution import ExecutionView
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    status=state.admit_backfill_execution(spec,manifest,now=now)
    status=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=status.control_sequence,status='running',now=now)
    claim=state.claim_task(spec.manifest_id,worker_id='original-records',lease_seconds=120,now=now,maintenance_execution_id=spec.execution_id)
    state.mark_task_succeeded(claim,duration_seconds=1,now=now+timedelta(seconds=1))
    status=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=status.control_sequence,status='paused',now=now+timedelta(seconds=2))
    for sequence in range(12):
        status=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id=f'original-resume-{sequence}',
            expected_sequence=status.control_sequence,action='resume',now=now+timedelta(seconds=sequence*2+3))
        status=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id=f'original-pause-{sequence}',
            expected_sequence=status.control_sequence,action='pause',now=now+timedelta(seconds=sequence*2+4))
        status=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=status.control_sequence,status='paused',
            now=now+timedelta(seconds=sequence*2+4))
    projections=project_data_center_execution(state_path=state.path,policy_path=None,observed_at=now+timedelta(minutes=1))
    rows=next(p for p in projections if p.table_name=='data_center_execution_event').rows
    records=tuple(ExecutionEventProjectionRow.model_validate(row) for row in rows)
    assert len(records)==20 and len({item.event_id for item in records})==20
    assert records==tuple(sorted(records,key=lambda item:(item.occurred_at,item.event_id)))
    assert all(item.owner==spec.owner and item.execution_id==spec.execution_id and item.occurred_at<=now+timedelta(minutes=1) for item in records)
    assert records[-1].occurred_at==status.updated_at and records[-1].event_type=='pause_requested'
    failed=MaintenanceExecutionStatus.model_validate(status.model_dump(mode='python')|{'status':'failed','failure_code':'source_unconfirmed'})
    assert not ExecutionView.from_original(failed).can_resume
    with state._write_transaction() as connection:
        row=connection.execute('SELECT command_id,result_json FROM data_center_maintenance_control ORDER BY rowid DESC LIMIT 1').fetchone()
        result=json.loads(row['result_json']);result['owner']='another-owner'
        connection.execute('UPDATE data_center_maintenance_control SET result_json=? WHERE command_id=?',[json.dumps(result),row['command_id']])
    with pytest.raises(ValueError,match='record|owner|control'):
        project_data_center_execution(state_path=state.path,policy_path=None,observed_at=now+timedelta(minutes=1))


def test_new_projection_group_keeps_the_frozen_combined_512_kib_limit(tmp_path: Path) -> None:
    from tests.unit.test_backfill_execute_admission import _state_execution
    from rquant.backfill_execute_projection import (ExecutionProjectionRow,project_data_center_execution,
        read_execution_projection_rows,validate_execution_projections)
    from rquant.serving_read_models import ServingProjectionPayload,_projection_json_bytes
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    group={item.table_name:item for item in project_data_center_execution(state_path=state.path,
        policy_path=None,observed_at=now)}
    validate_execution_projections(group)
    status=read_execution_projection_rows(group['data_center_execution'].rows)[0]
    rows=[]
    for index in range(50):
        bounded=status.model_copy(update={'execution_id':f'{index+1:064x}','audit_task_id':'a'*6000})
        rows.append(ExecutionProjectionRow(execution_id=bounded.execution_id,owner=bounded.owner,
            status_json=bounded.model_dump_json()).model_dump(mode='json'))
    group['data_center_execution']=ServingProjectionPayload(table_name='data_center_execution',
        available_at=now,rows=tuple(rows))
    # Each table is below its own limit; their combined UTF-8 material exceeds the frozen group limit.
    group['data_collection_dataset']=ServingProjectionPayload(table_name='data_collection_dataset',
        available_at=now,rows=tuple({'report_hash':'1'*64,'dataset_id':str(index),
            'source_binding_sha256':'2'*64,'evidence_json':'x'*7400} for index in range(24)))
    assert sum(_projection_json_bytes(item.rows) for item in group.values())>512*1024
    with pytest.raises(ValueError,match='512 KiB|combined.*capacity'):
        validate_execution_projections(group)


def test_two_step_http_command_keeps_original_actor_and_confirmation_on_retry(tmp_path: Path) -> None:
    from datetime import datetime,UTC
    from tests.support.web_proxy_identity import create_private_test_app,ProofTestClient
    from rquant.web.settings import WebSettings
    from rquant.page_control import parse_page_control_command,PrepareBackfillExecution
    now=datetime(2026,10,5,10,tzinfo=UTC)
    body=dict(kind='prepare_backfill_execution',command_id='prepare-original',requested_at=now.isoformat(),plan_task_id='a'*32,plan_hash='b'*64)
    calls=[]
    def transport(payload):
        calls.append(payload)
        command=parse_page_control_command(payload)
        assert isinstance(command,PrepareBackfillExecution) and command.actor_id=='researcher'
        return dict(command_id=command.command_id,status='succeeded',enqueued_at=now,completed_at=now,
            result={'outcome':'execution_prepared','confirmation':dict(kind='backfill',execution_id='c'*64,intent_id='d'*64,
                prepare_command_id=command.command_id,plan_hash=command.plan_hash,plan_task_id=command.plan_task_id,
                exact_dates_sha256='e'*64,start_date='2026-10-01',end_date='2026-10-01',missing_date_count=1,
                expires_at=(now+timedelta(minutes=5)).isoformat())},error=None)
    app=create_private_test_app(WebSettings(serving_root=tmp_path/'serving'),clock=lambda:now,background=False,backfill_plan_command_transport=transport)
    headers={'x-rquant-user':'researcher','x-rquant-csrf':'1','origin':'http://testserver'}
    with ProofTestClient(app) as client:
        first=client.post('/api/v1/data/executions/commands',json=body,headers=headers)
        retry=client.post('/api/v1/data/executions/commands',json=body,headers=headers)
        denied=client.post('/api/v1/data/executions/commands',json={**body,'actor_id':'another'},headers=headers)
        cross=client.post('/api/v1/data/executions/commands',json=body,headers={**headers,'origin':'https://another.example'})
    assert first.status_code==retry.status_code==200,first.text
    assert first.json()==retry.json() and first.json()['status']=='prepared'
    assert len(calls)==2 and denied.status_code==422 and cross.status_code==403


def test_actual_original_execution_serving_http_filters_owner_and_keeps_default_closed(tmp_path: Path) -> None:
    from tests.unit.test_backfill_execute_admission import _state_execution
    from tests.support.web_serving_fixture import build_web_fixture,FIXTURE_BUILT_AT
    from tests.support.web_proxy_identity import create_private_test_app,ProofTestClient
    from rquant.web.settings import WebSettings
    from rquant.backfill_execute_projection import project_data_center_execution
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    observed=now+timedelta(days=1)
    sequence=int((observed-FIXTURE_BUILT_AT).total_seconds()//60)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='actual-owner-pause',expected_sequence=1,action='pause',now=now)
    projections=project_data_center_execution(state_path=state.path,policy_path=None,observed_at=observed)
    build_web_fixture(tmp_path/'serving','baseline',audit_report_projections=projections,sequence=sequence)
    app=create_private_test_app(WebSettings(serving_root=tmp_path/'serving'),clock=lambda:observed,background=False)
    with ProofTestClient(app) as client:
        own=client.get('/api/v1/data/executions',headers={'x-rquant-user':spec.owner})
        other=client.get('/api/v1/data/executions',headers={'x-rquant-user':'another'})
        source=client.get('/api/v1/data/financial-sources',headers={'x-rquant-user':spec.owner})
    assert own.status_code==200,own.text
    assert len(own.json()['data']['events'])==1 and own.json()['data']['events'][0]['name']=='暂停'
    assert other.json()['data']['events']==[]
    assert own.json()['data']['executions'][0]['status']=='queued'
    assert own.json()['data']['backfill_enabled'] is False
    assert other.json()['data']['executions']==[]
    assert source.status_code==200,source.text
    assert len(source.json()['data']['sources'])==7
    assert all(item['permission_status']=='unknown' for item in source.json()['data']['sources'])


def test_actual_execution_serving_preserves_mixed_original_record_precision(tmp_path: Path) -> None:
    from tests.unit.test_backfill_execute_admission import _state_execution
    from tests.support.web_serving_fixture import build_web_fixture,FIXTURE_BUILT_AT
    from tests.support.web_proxy_identity import create_private_test_app,ProofTestClient
    from rquant.web.settings import WebSettings
    from rquant.backfill_execute_projection import project_data_center_execution,ExecutionEventProjectionRow
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    running=state.transition_maintenance(spec.execution_id,owner=spec.owner,
        expected_sequence=1,status='running',now=now)
    claim=state.claim_task(spec.manifest_id,worker_id='original-precise-record',lease_seconds=120,
        now=now,maintenance_execution_id=spec.execution_id)
    finished=now+timedelta(seconds=1,microseconds=137)
    state.mark_task_succeeded(claim,duration_seconds=1,now=finished)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='original-exact-second-pause',
        expected_sequence=running.control_sequence,action='pause',now=now+timedelta(seconds=2))
    observed=now+timedelta(days=1)
    projections=project_data_center_execution(state_path=state.path,policy_path=None,observed_at=observed)
    original=next(item for item in projections if item.table_name=='data_center_execution_event')
    records=tuple(ExecutionEventProjectionRow.model_validate(row) for row in original.rows)
    assert len(records)==2 and {row.occurred_at.microsecond for row in records}=={0,137}
    build_web_fixture(tmp_path/'serving','baseline',audit_report_projections=projections,
        sequence=int((observed-FIXTURE_BUILT_AT).total_seconds()//60))
    app=create_private_test_app(WebSettings(serving_root=tmp_path/'serving'),clock=lambda:observed,background=False)
    with ProofTestClient(app) as client:
        response=client.get('/api/v1/data/executions',headers={'x-rquant-user':spec.owner})
    assert response.status_code==200,response.text
    events=response.json()['data']['events']
    assert {row['event_id'] for row in events}=={row.event_id for row in records}
    from datetime import datetime
    assert {datetime.fromisoformat(row['occurred_at']) for row in events}=={row.occurred_at for row in records}


def test_actual_v3_collection_serving_http_keeps_partial_scope_and_global_unknown(tmp_path: Path) -> None:
    from tests.unit.test_data_collection_bridge import _chain
    from tests.support.web_serving_fixture import build_web_fixture,FIXTURE_BUILT_AT
    from tests.support.web_proxy_identity import create_private_test_app,ResearcherTestClient
    from rquant.data_audit_report_jobs import DataAuditReportJobWorker
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from rquant.data_audit_report_projection import project_data_audit_report
    from rquant.web.settings import WebSettings
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    bridge.run_one()
    task=DataAuditReportJobWorker(jobs).run_one()
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,task.report_hash))
    observed=report.datasets[0].as_of+timedelta(minutes=1)
    sequence=int((observed-FIXTURE_BUILT_AT).total_seconds()//60)
    projections=project_data_audit_report(report,available_at=observed)
    build_web_fixture(tmp_path/'serving','baseline',audit_report_projections=projections,sequence=sequence)
    app=create_private_test_app(WebSettings(serving_root=tmp_path/'serving'),clock=lambda:observed,background=False)
    with ResearcherTestClient(app) as client:
        response=client.get('/api/v1/data/collection')
    assert response.status_code==200,response.text
    value=response.json()['data']
    assert value['report_hash']==task.report_hash and len(value['datasets'])==24
    daily=next(item for item in value['datasets'] if item['dataset_id']=='daily_bar')
    minute=next(item for item in value['datasets'] if item['dataset_id']=='minute_bar')
    assert daily['status']=='partial' and daily['scopes'][0]['row_count']==1
    assert minute['status']=='unconfirmed' and minute['scopes']==[]
    assert value['coverage_label']=='全市场覆盖尚未核验'


def test_financial_source_projection_uses_original_current_quota_and_expires_rights(tmp_path: Path,monkeypatch) -> None:
    from tests.unit.test_financial_runtime import _financial_runtime_setup,NOW
    from rquant.backfill_execute_projection import project_data_center_execution
    from rquant.source_quota_store import SourceQuotaStore
    state,spec,current,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    worker.run_one(spec.execution_id,owner=spec.owner)
    path=tmp_path/'policy.json'
    path.write_text(current[0].model_dump_json())
    path.chmod(0o600)
    observed=NOW+timedelta(seconds=10)
    remaining=SourceQuotaStore(current[0].quota_ledger_path).remaining(current[0].quota_source,now=observed)
    projections=project_data_center_execution(state_path=state.path,policy_path=path,observed_at=observed)
    sources=next(p for p in projections if p.table_name=='data_center_financial_source')
    assert len(calls)==7 and all(row['remaining_units']==remaining and row['permission_status']=='verified' for row in sources.rows)
    later=project_data_center_execution(state_path=state.path,policy_path=path,observed_at=NOW+timedelta(hours=2))
    assert all(row['permission_status']=='unknown' for row in next(p for p in later if p.table_name=='data_center_financial_source').rows)
