from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from rquant.data_collection_authority import CollectionCommitRecorder
from rquant.data_collection_contracts import CollectionRecorderConfig, IngestionCommitReceipt
from rquant.ingest import ingest_daily
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay

DAY = date(2024, 1, 2)
NOW = datetime(2024, 1, 2, 8, tzinfo=UTC)


class DailySource:
    def stock_basic(self, **kwargs: object) -> pd.DataFrame:
        return pd.DataFrame([dict(ts_code='600000.SH',symbol='600000',name='浦发银行',
            area='上海',industry='银行',list_date='19991110',market='主板')])

    def daily(self, **kwargs: object) -> pd.DataFrame:
        assert kwargs == {'trade_date':'20240102'}
        return pd.DataFrame([dict(ts_code='600000.SH',trade_date='20240102',open=10.0,
            high=10.5,low=9.5,close=10.0,pre_close=10.0,change=0.0,pct_chg=0.0,
            vol=100.0,amount=1000.0)])

    def index_daily(self, **kwargs: object) -> pd.DataFrame:
        return pd.DataFrame()

    def adj_factor(self, **kwargs: object) -> pd.DataFrame:
        return pd.DataFrame([dict(ts_code='600000.SH',trade_date='20240102',adj_factor=1.0)])

    def daily_basic(self, **kwargs: object) -> pd.DataFrame:
        return pd.DataFrame()

    def namechange_raw(self, start_date: date, end_date: date,
                       ts_code: str | None = None) -> pd.DataFrame:
        return pd.DataFrame([dict(ts_code='600000.SH',name='浦发银行',start_date='19991110',
            end_date=None,ann_date='19991110',change_reason='上市')])

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        return pd.DataFrame(columns=['ts_code','name','trade_date','type','type_name'])

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
        return pd.DataFrame(columns=['ts_code','trade_date','suspend_timing','suspend_type'])


def recorder() -> CollectionCommitRecorder:
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit='a'*40,
        coverage_start=DAY,coverage_end=DAY,open_dates=(DAY,),generated_at=NOW)
    return CollectionCommitRecorder(CollectionRecorderConfig(collector_id='legacy_daily',
        run_id='actual-original-daily',owner='test-owner',code_commit='a'*40,
        source_generation_id='b'*64,calendar=calendar),clock=lambda:NOW)


@pytest.mark.parametrize('fail_receipt',[False,True])
def test_original_ingest_records_actual_responses_in_fact_transaction(tmp_path: Path,
                                                                   fail_receipt: bool) -> None:
    assert 'completion_recorder' in inspect.signature(ingest_daily).parameters
    primary=tmp_path/'primary.duckdb'
    with DuckDBStore(primary) as store:
        store.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=DAY,is_open=True,
            pretrade_date=date(2023,12,29),updated_at=NOW),))
    source=DailySource()
    completion=recorder()
    if fail_receipt:
        def failure(*args: object, **kwargs: object) -> None:
            raise OSError('receipt I/O failure')
        completion.record_daily=failure  # type: ignore[method-assign]
    call=lambda:ingest_daily('2024-01-02',pro=source,status_adapter=source,
        indicator_reader_factory=lambda:DuckDBStore(primary,read_only=True),
        writer_factory=lambda:DuckDBStore(primary),ingested_at=NOW,api_sleep=0,
        sleep=lambda _:None,completion_recorder=completion)
    if fail_receipt:
        with pytest.raises(OSError,match='receipt I/O'):
            call()
    else:
        assert call()==1
    with DuckDBStore(primary,read_only=True) as store:
        count=store.count_daily()
        payloads=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt').fetchall()
        assert count==(0 if fail_receipt else 1)
        assert len(payloads)==(0 if fail_receipt else 1)
        if not fail_receipt:
            receipt=IngestionCommitReceipt.model_validate_json(payloads[0][0])
            assert {'daily','namechange','stock_st','suspend_d','adj_factor'} <= {
                row.api_name for row in receipt.observations}
            assert {'daily_bar','stock_status_daily','stock_suspend_coverage','adj_factor'} <= {
                row.dataset_id for row in receipt.datasets}
            assert all(not row.coverage_complete for row in receipt.datasets)
            completion.verify_receipt(store,receipt)


def test_original_v3_audit_projections_keep_real_scopes_and_old_global_unknown(tmp_path: Path) -> None:
    from tests.unit.test_data_collection_bridge import _chain
    from rquant.data_audit_report_jobs import DataAuditReportJobWorker
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from rquant.data_audit_report_projection import project_data_audit_report
    from rquant.serving_page_projection_source import LabPageProjectionSnapshot
    from rquant.data_collection_projection import read_data_collection_projection_rows
    from datetime import timedelta
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    bridge.run_one()
    succeeded=DataAuditReportJobWorker(jobs).run_one()
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,succeeded.report_hash))
    available=report.datasets[0].as_of+timedelta(seconds=1)
    projections=project_data_audit_report(report,available_at=available)
    snapshot=LabPageProjectionSnapshot.create(available_at=available,audit_report_projections=projections)
    rows=next(item.rows for item in snapshot.projections if item.table_name=='data_collection_dataset')
    evidence=read_data_collection_projection_rows(tuple(dict(row) for row in rows),report_hash=report.content_hash)
    assert len(evidence)==24
    assert next(item for item in evidence if item.dataset_id=='daily_bar').status=='partial'
    assert next(item for item in evidence if item.dataset_id=='minute_bar').status=='unconfirmed'
    assert report.collection_reference==reference and report.coverage_conclusion=='unconfirmed'


@pytest.mark.parametrize('case',['actual_chain','calendar_generation','calendar_producer','calendar_as_of','physical_source'])
def test_original_canonical_receipt_is_bound_to_actual_proof_and_calendar(tmp_path: Path,case: str) -> None:
    from shutil import copyfile
    from datetime import timedelta
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from rquant.data_audit_report_jobs import DataAuditReportJobStore,DataAuditReportJobWorker
    from rquant.data_audit_evidence import DailyBarNullFieldSpec
    from rquant.data_collection_authority import seal_data_collection_proof
    from rquant.data_collection_bridge import DataCollectionBridge
    from rquant.data_collection_contracts import AuditCollectionReference,CollectionReceiptReference
    from rquant.replica_generation import capture_database_watermark,replica_generation_path,write_replica_generation_metadata
    from rquant.storage.primary_writer_gate import PrimaryWriterGate,PrimaryWriterGateConfig
    from tests.unit.test_daily_canonical_publisher import _candidate,_publisher,_attempt,COMMITTED_AT,LEDGER_INPUT
    from tests.unit.test_daily_close_validation import _calendar,TRADE_DATE
    gateway,candidates,candidate=_candidate(tmp_path)
    primary=tmp_path/'canonical.duckdb'
    calendar=_calendar()
    with DuckDBStore(primary) as store:
        store.upsert_stock_basic(DailySource().stock_basic())
        store.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=TRADE_DATE,is_open=True,
            pretrade_date=None,updated_at=calendar.generated_at),))
    receipt=_publisher(candidates,primary,gateway.spool).publish(candidate.generation_id,
        attempt=_attempt(),ledger_input_identity=LEDGER_INPUT,committed_at=COMMITTED_AT)
    assert receipt.calendar_generation_id==calendar.content_sha256
    assert receipt.expected_ledger_receipt.run_id==_attempt().run_id
    assert receipt.ledger_fencing_token==_attempt().fencing_token
    if case=='physical_source':
        replacement=tmp_path/'other-primary.duckdb'
        copyfile(primary,replacement)
        primary=replacement
    if case.startswith('calendar_'):
        values=calendar.model_dump(exclude={'content_sha256'})
        if case=='calendar_generation':
            values['coverage_start']=calendar.coverage_start-timedelta(days=1)
        elif case=='calendar_producer':
            values['producer_commit']='b'*40
        else:
            values['generated_at']=calendar.generated_at-timedelta(microseconds=1)
        calendar=MarketCalendarAuthority.create(**values)
    replica=tmp_path/'replica.duckdb'
    watermark=capture_database_watermark(primary)
    copyfile(primary,replica)
    write_replica_generation_metadata(primary_path=primary,replica_path=replica,
        output_path=replica_generation_path(replica),source_before=watermark)
    lock=tmp_path/'canonical-primary.lock'
    lock.touch(mode=0o600)
    gate=PrimaryWriterGate(PrimaryWriterGateConfig.capture(primary_path=primary,lock_path=lock))
    observed=COMMITTED_AT+timedelta(seconds=1)
    jobs=DataAuditReportJobStore(state_path=tmp_path/'canonical-audit.sqlite',report_directory=tmp_path/'reports',
        collection_directory=tmp_path/'collection',clock=lambda:observed)
    with gate.acquire() as lease:
        def seal() -> AuditCollectionReference:
            return seal_data_collection_proof(jobs,primary_path=primary,replica_path=replica,calendar=calendar,
                references=(CollectionReceiptReference(kind='canonical',receipt_id=receipt.receipt_id,
                    dataset_ids=('daily_bar',)),),audit_start=TRADE_DATE,observed_through=TRADE_DATE,
                primary_writer_lease=lease,clock=lambda:observed)
        if case!='actual_chain':
            with pytest.raises(ValueError,match='canonical'):
                seal()
            with jobs._connect() as connection:
                assert connection.execute('SELECT COUNT(*) FROM data_collection_source').fetchone()[0]==0
                assert connection.execute('SELECT COUNT(*) FROM data_audit_report_job').fetchone()[0]==0
            assert not tuple(jobs.collection_directory.glob('pin-*.duckdb'))
            return
        reference=seal()
    admitted=DataCollectionBridge(jobs,null_fields=(DailyBarNullFieldSpec(field_name='close',
        max_null_numerator=0,max_null_denominator=1),)).run_one()
    assert admitted is not None and admitted.status=='queued'
    succeeded=DataAuditReportJobWorker(jobs).run_one()
    assert succeeded.task_id==admitted.task_id and succeeded.status=='succeeded'
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,succeeded.report_hash))
    assert report.schema_version==3 and report.collection_reference==reference
    daily=next(row for row in report.collection_datasets if row.dataset_id=='daily_bar')
    original=next(row for row in receipt.watermarks if row.table_name=='daily_bar')
    assert daily.status=='partial' and daily.receipt_ids==(receipt.receipt_id,)
    assert len(daily.scopes)==1 and daily.scopes[0].row_count==original.row_count
    assert daily.scopes[0].content_sha256==original.content_sha256
    assert daily.scopes[0].trade_date==TRADE_DATE
    assert next(row for row in report.collection_datasets if row.dataset_id=='minute_bar').status=='unconfirmed'
    assert report.coverage_conclusion=='unconfirmed' and report.collection_completed_through is None


@pytest.mark.parametrize('case',['historical','twenty_one_queries','seventeen_source_batches'])
def test_original_financial_completion_keeps_all_observations_and_historical_pit(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    from tests.unit.test_financial_runtime import _financial_runtime_setup,NOW,APIS
    from rquant.data_center_maintenance_runtime import DataCenterCompletionRuntime,DataCenterMaintenanceRuntimeConfig
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from datetime import timedelta
    historical=case=='historical'
    day=NOW.date()-timedelta(days=1) if historical else NOW.date()
    codes=tuple(f'{600000+i:06d}.SH' for i in range(75 if case=='seventeen_source_batches' else 1 if historical else 3))
    state,spec,current,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch,start_date=day,end_date=day,
        securities=codes,quota_units=len(codes)*7)
    runtime=DataCenterCompletionRuntime(state,config=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'replica.duckdb',
        audit_state_path=tmp_path/'audit.sqlite',audit_directory=tmp_path/'audit',collection_directory=tmp_path/'collection'),
        policy=lambda:current[0],clock=worker.clock)
    worker.completion_runtime=runtime
    manifest=state.load_manifest(spec.manifest_id)
    raw_tasks=tuple(task for task in manifest.tasks if task.payload['kind']=='original_financial_query_group')
    derived_tasks=tuple(task for task in manifest.tasks if task.payload['kind']=='original_fundamental_group')
    for task in raw_tasks:
        assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='query_committed'
    assert calls==list(APIS)*len(codes)
    originals=tuple(archive.receipt(query.request_id) for query in spec.plan.queries)
    assert all(item.observed_at.date()==NOW.date() for item in originals)
    for task in derived_tasks:
        assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='fundamentals_committed'
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='completed',result.model_dump_json()
    report=load_data_audit_report(data_audit_report_path(runtime.jobs.report_directory,result.execution.audit_report_sha256))
    summary=next(item for item in report.collection_proof.receipt_manifest.datasets if item.dataset_id=='financial_observation')
    assert summary.scope_count==len(spec.plan.queries)
    assert summary.receipt_count==len(raw_tasks)
    assert summary.first_date==summary.last_date==NOW.date()
    assert report.audit_start==report.observed_through==day
    assert report.collection_status=='collection_partial' and report.collection_completed_through is None
    assert calls==list(APIS)*len(codes)
    from rquant.data_collection_manifest import iter_receipt_manifest
    entries=tuple(iter_receipt_manifest(runtime.jobs.collection_directory,report.collection_proof.receipt_manifest))
    assert tuple(item.task_id for item in entries)==tuple(task.task_id for task in manifest.tasks[:-1])
    assert report.collection_proof.receipt_manifest.request_count==len(calls)==len(spec.plan.queries)
    if case=='seventeen_source_batches':
        assert len(raw_tasks)>16 and len(entries)>16 and len(report.collection_proof.receipt_manifest.pages)>1
        versions=next(item for item in report.collection_proof.receipt_manifest.datasets if item.dataset_id=='fundamental_daily_version')
        assert versions.scope_count==75 and versions.receipt_count==1
        assert len(report.collection_datasets)==24
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        from rquant.financial_runtime import _load_runtime_receipt
        bindings=tuple(binding for item in entries for binding in _load_runtime_receipt(reader,receipt_id=item.task_receipt_id).source_requests)
        assert len({item.logical_request_id for item in bindings})==len(spec.plan.queries)
        from rquant.runtime_contracts import canonical_sha256
        assert tuple(binding.scope_sha256 for binding in bindings)==tuple(canonical_sha256(query.model_dump(mode='python')) for query in spec.plan.queries)
        times=reader._conn.execute('SELECT DISTINCT observed_at FROM financial_observation').fetchall()
        assert {value[0] for value in times}=={item.observed_at for item in originals}
        assert originals==tuple(archive.receipt(query.request_id) for query in spec.plan.queries)
        if historical:
            values=reader._conn.execute('SELECT pe_ttm,pb,dv_ttm,roe,or_yoy,netprofit_yoy FROM fundamental_daily_version').fetchall()
            assert values and all(all(value is None for value in row) for row in values)


def _financial_page_source(tmp_path: Path,monkeypatch: pytest.MonkeyPatch):
    from tests.unit.test_financial_runtime import _financial_runtime_setup
    from tests.unit.test_data_collection_bridge import _chain
    from rquant.backfill_execute_contracts import DataCenterExecutionPolicy
    from rquant.backfill_execute_page_backend import BackfillExecutePageBackend,BackfillExecutePageBackendConfig
    from rquant.backfill_state import BackfillStateStore
    from rquant.data_audit_report_jobs import DataAuditReportJobWorker
    state,spec,current,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    # Both existing fixture builders use one SSE day. Align their supplied previous-day fact.
    with DuckDBStore(tmp_path/'primary.duckdb') as seed:
        seed._conn.execute("UPDATE trade_calendar SET pretrade_date=DATE '2026-09-30' WHERE exchange='SSE' AND cal_date=DATE '2026-10-05'")
    original= DuckDBStore.upsert_daily
    def seed_available(store: DuckDBStore,frame: pd.DataFrame) -> int:
        store.upsert_stock_basic(pd.DataFrame([dict(ts_code='600000.SH',symbol='600000',name='浦发银行',
            area='上海',industry='银行',list_date=date(1999,11,10),market='主板')]))
        return original(store,frame)
    with monkeypatch.context() as local:
        local.setattr(DuckDBStore,'upsert_daily',seed_available)
        _,replica,reference,jobs,bridge,*_=_chain(tmp_path)
    bridge.run_one()
    audited=DataAuditReportJobWorker(jobs).run_one()
    assert audited.status=='succeeded'
    page_state=BackfillStateStore(tmp_path/'page-original-state.sqlite3',maintenance_enabled=True)
    page_identity=page_state.path.stat()
    current[0]=DataCenterExecutionPolicy.model_validate_json(current[0].model_copy(update={
        'policy_generation':None,'original_state_path':page_state.path,
        'original_state_device':page_identity.st_dev,'original_state_inode':page_identity.st_ino}).model_dump_json())
    policy_path=tmp_path/'page-policy.json'
    policy_path.write_text(current[0].model_dump_json())
    policy_path.chmod(0o600)
    backend=BackfillExecutePageBackend(BackfillExecutePageBackendConfig(policy_path=policy_path,
        original_state_path=current[0].original_state_path,plan_state_path=tmp_path/'page-plan.sqlite3',
        plan_directory=tmp_path/'plans',audit_state_path=jobs.state_path,audit_directory=jobs.report_directory,
        collection_directory=jobs.collection_directory,financial_archive_path=archive.root,
        replica_path=replica,allowed_owners=(spec.owner,)),clock=worker.clock)
    from rquant.data_collection_authority import restore_collection_report_replica
    restore_collection_report_replica(jobs,audited.task_id,replica_path=replica,
        primary_writer_gate=current[0].primary_writer_gate)
    return backend,spec,replica,audited,calls


def test_original_seventeen_day_completion_keeps_every_original_receipt_and_daily_scope(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import timedelta
    from tests.unit.test_data_collection_bridge import _chain,NOW,DAY
    from tests.unit.test_backfill_execute import _controlled_source
    from rquant.data_collection_authority import load_collection_proof
    from rquant.backfill_execute_contracts import DataCenterExecutionPolicy,BackfillExecutionIntent,BackfillExecutionSpec
    from rquant.backfill_execute_page_backend import build_backfill_execution_manifest
    from rquant.backfill_plan_core import build_daily_bar_backfill_plan,BackfillEstimateAssumptions
    from rquant.data_center_maintenance_runtime import DataCenterCompletionRuntime,DataCenterMaintenanceRuntimeConfig
    from rquant.backfill_state import BackfillStateStore
    from rquant.backfill_execute import BackfillExecutionWorker
    from rquant.adapter.tushare import TushareAdapter
    from rquant.runtime_contracts import canonical_sha256
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from rquant.data_audit_report_jobs import DataAuditReportJobWorker
    from rquant.data_collection_manifest import iter_receipt_manifest
    import hashlib,json
    primary,replica,source,jobs,bridge,gate,calendar,_=_chain(tmp_path,first_day=DAY-timedelta(days=17))
    bridge.run_one()
    original_audit=DataAuditReportJobWorker(jobs).run_one()
    assert original_audit.status=='succeeded'
    proof=load_collection_proof(jobs.collection_directory,source)
    with DuckDBStore(proof.fixed_replica_path,read_only=True) as reader:
        plan=build_daily_bar_backfill_plan(reader._conn,snapshot_label=f'sha256:{proof.replica_sha256}',
            snapshot_file_sha256=proof.replica_sha256,evidence_code_revision='offline-actual-original-source',
            audit_start=calendar.coverage_start,completed_through=DAY,observed_at=NOW,
            assumptions=BackfillEstimateAssumptions(status_namechange_start=date(2026,1,1),status_source_as_of=DAY,
                status_window_years=3,adapter_seconds_per_operation=1,market_throttle_seconds_per_operation=0,
                status_throttle_seconds_per_operation=0,retry_allowance_seconds_per_operation=0))
    assert len(plan.missing_dates)==17
    _,_,current=_controlled_source(tmp_path)
    rights=[]
    for api in ('daily','daily_basic','adj_factor','namechange','stock_st'):
        right=current[0].entitlement_evidence[0].model_copy(update={'api_name':api,'scope_start':date(2026,1,1),
            'allowed_parameters':('trade_date','start_date','end_date','fields','ts_code'),'evidence_path':tmp_path/f'{api}-rights.json'})
        payload=json.dumps({'kind':'source-entitlement-evidence/v1','proof':right.model_dump(mode='json',
            exclude={'evidence_path','evidence_sha256'})},sort_keys=True,separators=(',',':')).encode()
        right.evidence_path.write_bytes(payload)
        right.evidence_path.chmod(0o400)
        rights.append(right.model_copy(update={'evidence_sha256':hashlib.sha256(payload).hexdigest()}))
    current[0]=DataCenterExecutionPolicy.model_validate_json(current[0].model_copy(update={'policy_generation':None,
        'entitlement_evidence':tuple(rights),'quota_units_per_window':100}).model_dump_json())
    execution=canonical_sha256(('actual-seventeen-day',plan.content_sha256))
    intent=BackfillExecutionIntent(execution_id=execution,owner='fixture-owner',prepare_command_id='prepare-seventeen-days',
        plan_task_id='e'*32,plan_sha256=plan.content_sha256,exact_dates_sha256=canonical_sha256(plan.missing_dates),
        source_reference=source,source_generation_id=source.binding_sha256,calendar_sha256=plan.evidence.calendar_sha256,
        primary_identity=proof.primary_identity,policy_generation=current[0].policy_generation,nonce_sha256='f'*64,
        issued_at=NOW,expires_at=NOW+timedelta(minutes=5))
    spec=BackfillExecutionSpec(execution_id=execution,owner=intent.owner,plan_task_id=intent.plan_task_id,plan=plan,intent=intent,
        execute_command_id='execute-seventeen-days',manifest_id=canonical_sha256(('seventeen-days',execution)),admission_policy=current[0])
    state=BackfillStateStore(current[0].original_state_path,maintenance_enabled=True)
    state.persist_maintenance_intent(intent)
    manifest=build_backfill_execution_manifest(spec)
    state.admit_backfill_execution(spec,manifest,now=NOW)
    calls=[]
    class SDK:
        def daily(self,**kw):
            calls.append(('daily',kw['trade_date']))
            return pd.DataFrame([dict(ts_code='600000.SH',trade_date=kw['trade_date'],open=10.0,high=10.5,low=9.5,
                close=10.0,pre_close=10.0,change=0.0,pct_chg=0.0,vol=100.0,amount=1000.0)])
        def daily_basic(self,**kw):
            calls.append(('daily_basic',kw['trade_date']))
            return pd.DataFrame([dict(ts_code='600000.SH',trade_date=kw['trade_date'],turnover_rate=1.0,volume_ratio=1.0,
                total_mv=10000.0,circ_mv=8000.0,pe_ttm=10.0,pb=1.0,dv_ttm=None)])
        def adj_factor(self,**kw):
            calls.append(('adj_factor',kw['trade_date']))
            return pd.DataFrame([dict(ts_code='600000.SH',trade_date=kw['trade_date'],adj_factor=1.0)])
        def namechange(self,**kw):
            calls.append(('namechange',kw['start_date']))
            return pd.DataFrame([dict(ts_code='600000.SH',name='浦发银行',start_date='20260101',end_date=None,
                ann_date='20260101',change_reason='更名')])
        def stock_st(self,**kw):
            calls.append(('stock_st',kw['trade_date']))
            return pd.DataFrame([dict(ts_code='600001.SH',name='ST样本',trade_date=kw['trade_date'],type='ST',type_name='特别处理')])
    def adapter_factory(observer):
        adapter=TushareAdapter.__new__(TushareAdapter)
        adapter._pro=SDK()
        adapter._backup_token=''
        adapter.bind_transport_observer(observer)
        adapter.bind_sdk_null_normalization('tushare-nullable-v1')
        return adapter
    observed=[NOW]
    worker=BackfillExecutionWorker(state,policy=lambda:current[0],adapter_factory=adapter_factory,calendar=lambda _:calendar,clock=lambda:observed[0])
    import sys
    failures=[]
    pause=worker._pause
    def capture_pause(*args,**kw):
        failures.append(repr(sys.exception()))
        return pause(*args,**kw)
    monkeypatch.setattr(worker,'_pause',capture_pause)
    runtime=DataCenterCompletionRuntime(state,config=DataCenterMaintenanceRuntimeConfig(replica_path=replica,
        audit_state_path=jobs.state_path,audit_directory=jobs.report_directory,collection_directory=jobs.collection_directory),
        policy=lambda:current[0],clock=worker.clock)
    worker.completion_runtime=runtime
    for day in plan.missing_dates:
        step=worker.run_one(execution,owner=spec.owner)
        assert step.outcome=='day_committed' and step.task_id==f'day-{day.isoformat()}',(step.model_dump_json(),failures)
    derived=worker.run_one(execution,owner=spec.owner)
    assert derived.outcome=='derived_committed',(derived.model_dump_json(),failures)
    observed[0]=NOW+timedelta(seconds=601)
    completed=worker.run_one(execution,owner=spec.owner)
    assert completed.execution.status=='completed',(completed.model_dump_json(),failures)
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,completed.execution.audit_report_sha256))
    entries=tuple(iter_receipt_manifest(jobs.collection_directory,report.collection_proof.receipt_manifest))
    assert tuple(item.task_id for item in entries)==tuple(task.task_id for task in manifest.tasks[:-1])
    daily=next(item for item in report.collection_datasets if item.dataset_id=='daily_bar')
    assert daily.receipt_set.receipt_count==daily.receipt_set.scope_count==17
    assert daily.receipt_set.first_date==plan.missing_dates[0] and daily.receipt_set.last_date==plan.missing_dates[-1]
    assert daily.receipt_ids==() and len(daily.scopes)==1 and daily.scopes[0].row_count==17
    assert {value for api,value in calls if api=='daily'}=={day.strftime('%Y%m%d') for day in plan.missing_dates}


@pytest.mark.parametrize('scope',['available_securities','selected_securities'])
def test_financial_page_prepares_from_actual_original_audit_and_securities_without_sdk(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,scope: str) -> None:
    from rquant.page_control import PrepareFinancialCollection
    from tests.unit.test_financial_runtime import NOW as financial_now
    backend,spec,replica,audited,calls=_financial_page_source(tmp_path,monkeypatch)
    command=PrepareFinancialCollection(command_id='actual-finance-preview',actor_id=spec.owner,
        requested_at=financial_now,audit_report_hash=audited.report_hash,security_scope=scope,
        selected_securities=() if scope=='available_securities' else ('600000.SH',),
        start_date=financial_now.date(),end_date=financial_now.date(),report_periods=(date(2026,6,30),))
    prepared=backend.submit(command)
    assert prepared['outcome']=='execution_prepared'
    assert prepared['confirmation']['security_count']==1
    assert prepared['confirmation']['query_count']==7
    assert backend.submit(command)==prepared and calls==[]
    intent=backend.state.financial_intent_by_command(command.command_id,owner=spec.owner)
    assert intent.plan.securities==('600000.SH',)


def test_financial_page_rejects_live_replica_rotation_before_scope_admission(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from shutil import copyfile
    from rquant.page_control import PrepareFinancialCollection
    from tests.unit.test_financial_runtime import NOW as financial_now
    backend,spec,replica,audited,calls=_financial_page_source(tmp_path,monkeypatch)
    copyfile(replica,tmp_path/'replacement.duckdb')
    (tmp_path/'replacement.duckdb').replace(replica)
    command=PrepareFinancialCollection(command_id='rotated-finance-preview',actor_id=spec.owner,
        requested_at=financial_now,audit_report_hash=audited.report_hash,security_scope='available_securities',
        selected_securities=(),start_date=financial_now.date(),end_date=financial_now.date(),report_periods=(date(2026,6,30),))
    with pytest.raises(ValueError,match='replica|source'):
        backend.submit(command)
    assert backend.state.financial_intent_by_command(command.command_id,owner=spec.owner) is None
    assert calls==[]


@pytest.mark.parametrize('case',['primary','sidecar','during_sidecar'])
def test_financial_cold_prepare_keeps_physical_primary_and_exact_sealed_sidecar_binding(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    import json,os
    from shutil import copyfile
    from rquant.page_control import PrepareFinancialCollection
    from rquant.replica_generation import replica_generation_path
    import rquant.financial_runtime as financial
    from tests.unit.test_financial_runtime import NOW as observed
    backend,spec,replica,audited,calls=_financial_page_source(tmp_path,monkeypatch)
    policy=backend._policy(kind='financial')
    sidecar=replica_generation_path(replica)
    if case=='primary':
        other=tmp_path/'replacement-primary.duckdb'
        copyfile(policy.primary_writer_gate.primary_path,other)
        other.replace(policy.primary_writer_gate.primary_path)
    elif case=='sidecar':
        body=json.loads(sidecar.read_text())
        body['source_before']['main']['mtime_ns']-=1
        body['source_after']['main']['mtime_ns']-=1
        sidecar.write_text(json.dumps(body))
    else:
        original=financial.require_financial_plan_policy
        def move_sidecar(plan: object,current: object,*,now: datetime) -> None:
            original(plan,current,now=now)
            identity=sidecar.stat()
            os.utime(sidecar,ns=(identity.st_atime_ns,identity.st_mtime_ns+1))
        monkeypatch.setattr(financial,'require_financial_plan_policy',move_sidecar)
    command=PrepareFinancialCollection(command_id='bound-finance-preview',actor_id=spec.owner,
        requested_at=observed,audit_report_hash=audited.report_hash,security_scope='available_securities',
        selected_securities=(),start_date=observed.date(),end_date=observed.date(),report_periods=(date(2026,6,30),))
    with pytest.raises((ValueError,RuntimeError,OSError)):
        backend.submit(command)
    assert backend.state.financial_intent_by_command(command.command_id,owner=spec.owner) is None
    assert calls==[]


def _profile_plan_source(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,*,different_source: bool=False) -> tuple:
    from rquant.data_collection_authority import restore_collection_report_replica
    from rquant.data_audit_report_jobs import DataAuditReportJobStore
    from rquant.data_center_maintenance_runtime import DataCenterRuntimeProfile,DataCenterMaintenanceRuntimeConfig,build_data_center_plan_backend
    from rquant.backfill_plan_core import BackfillEstimateAssumptions
    from rquant.page_control import SubmitBackfillPlan
    from rquant.screen.replica_source import VerifiedReplicaScreenSource
    from tests.unit.test_financial_runtime import NOW as observed
    backend,spec,replica,audited,calls=_financial_page_source(tmp_path,monkeypatch)
    policy=backend._policy(kind='financial')
    jobs=DataAuditReportJobStore(state_path=backend.config.audit_state_path,report_directory=backend.config.audit_directory,
        collection_directory=backend.config.collection_directory,clock=lambda:observed)
    restore_collection_report_replica(jobs,audited.task_id,replica_path=replica,primary_writer_gate=policy.primary_writer_gate)
    source=VerifiedReplicaScreenSource(primary_path=policy.primary_writer_gate.primary_path,replica_path=replica)
    assert source.generation_identity()
    profile=DataCenterRuntimeProfile(policy_path=backend.config.policy_path,
        maintenance=DataCenterMaintenanceRuntimeConfig(replica_path=replica,audit_state_path=jobs.state_path,
            audit_directory=jobs.report_directory,collection_directory=jobs.collection_directory),
        plan_state_path=tmp_path/'profile-plans.sqlite',plan_directory=backend.config.plan_directory,allowed_owners=(spec.owner,),
        backfill_plan_assumptions=BackfillEstimateAssumptions(adapter_seconds_per_operation=1,
            market_throttle_seconds_per_operation=0,status_throttle_seconds_per_operation=0,
            retry_allowance_seconds_per_operation=0,status_namechange_start=date(2026,1,1),
            status_source_as_of=observed.date(),status_window_years=3))
    path=tmp_path/'plan-runtime.json';path.write_text(profile.model_dump_json());path.chmod(0o600)
    plans=build_data_center_plan_backend(path,clock=lambda:observed)
    if different_source:
        from shutil import copyfile
        alternate=tmp_path/'same-bytes-different-source.duckdb'
        copyfile(replica,alternate)
        alternate.replace(replica)
    admitted=plans.submit(SubmitBackfillPlan(command_id='original-pin-plan',actor_id=spec.owner,requested_at=observed,
        audit_start=observed.date(),completed_through=observed.date()))
    return path,profile,plans,admitted,source,jobs,audited,observed,calls


def test_original_profile_plan_pin_keeps_same_source_financial_read_available(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.data_center_maintenance_runtime import build_data_center_plan_worker
    from rquant.data_audit_report import data_audit_report_path,load_data_audit_report
    from rquant.replica_generation import replica_generation_path
    path,profile,plans,admitted,source,jobs,audited,observed,calls=_profile_plan_source(tmp_path,monkeypatch)
    before_summary=source.fundamental_summary(now=observed).summary
    report_path=data_audit_report_path(jobs.report_directory,audited.report_hash)
    report_bytes=report_path.read_bytes()
    proof_path=jobs.collection_directory/load_data_audit_report(report_path).collection_reference.relative_proof_name
    proof_bytes=proof_path.read_bytes()
    sidecar_path=replica_generation_path(profile.maintenance.replica_path)
    sidecar_bytes=sidecar_path.read_bytes()
    done=build_data_center_plan_worker(path,clock=lambda:observed).run_one()
    assert done.task_id==admitted['task_id'] and done.status=='succeeded'
    assert source.generation_identity()
    assert source.fundamental_summary(now=observed).summary==before_summary
    assert report_path.read_bytes()==report_bytes and proof_path.read_bytes()==proof_bytes
    assert sidecar_path.read_bytes()==sidecar_bytes
    assert calls==[]


@pytest.mark.parametrize('case',['owner','code','profile','missing_report','different_source'])
def test_profile_plan_cannot_restore_without_its_original_scope_and_collection_proof(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    from rquant.backfill_execute import load_execution_policy
    from rquant.backfill_execute_contracts import DataCenterExecutionPolicy
    from rquant.data_audit_report import data_audit_report_path
    from rquant.data_center_maintenance_runtime import build_data_center_plan_worker
    from rquant.screen.replica_source import ScreenReplicaUnavailableError
    from rquant.replica_generation import replica_generation_path
    path,profile,plans,admitted,source,jobs,audited,observed,calls=_profile_plan_source(
        tmp_path,monkeypatch,different_source=case=='different_source')
    worker=build_data_center_plan_worker(path,clock=lambda:observed)
    if case=='owner':
        owner_profile=profile.model_copy(update={'allowed_owners':('another-owner',)})
        path.write_text(owner_profile.model_dump_json())
        worker=build_data_center_plan_worker(path,clock=lambda:observed)
    elif case=='code':
        policy=load_execution_policy(profile.policy_path)
        changed=DataCenterExecutionPolicy.model_validate_json(policy.model_copy(update={
            'code_commit':'f'*40,'policy_generation':None}).model_dump_json())
        profile.policy_path.write_text(changed.model_dump_json())
    elif case=='profile':
        path.write_text(profile.model_copy(update={'plan_directory':tmp_path/'another-directory'}).model_dump_json())
    elif case=='missing_report':
        data_audit_report_path(jobs.report_directory,audited.report_hash).unlink()
    sidecar_path=replica_generation_path(profile.maintenance.replica_path)
    sidecar_before=sidecar_path.stat()
    done=worker.run_one()
    assert done.task_id==admitted['task_id'] and done.status=='failed'
    assert sidecar_path.stat()==sidecar_before
    with pytest.raises(ScreenReplicaUnavailableError):
        source.generation_identity()
    assert calls==[]


@pytest.mark.parametrize('case',['explicit','state_mismatch','directory_mismatch'])
def test_original_plan_runner_profile_matches_exact_state_and_directory_before_claim(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    from rquant.backfill_plan_runner import main
    from rquant.backfill_plan_jobs import BackfillPlanJobWorker
    import rquant.data_center_maintenance_runtime as runtime
    path,profile,plans,admitted,source,jobs,audited,observed,calls=_profile_plan_source(tmp_path,monkeypatch)
    original_factory=runtime.build_data_center_plan_worker
    def fixed_clock_factory(profile_path: Path,*,stop_requested: Callable[[],bool] | None=None) -> BackfillPlanJobWorker:
        return original_factory(profile_path,clock=lambda:observed,stop_requested=stop_requested)
    monkeypatch.setattr(runtime,'build_data_center_plan_worker',fixed_clock_factory)
    arguments=['--state-path',str(profile.plan_state_path if case!='state_mismatch' else tmp_path/'other.sqlite'),
        '--plan-directory',str(profile.plan_directory if case!='directory_mismatch' else tmp_path/'other-plans'),
        '--runtime-profile',str(path),'--once']
    assert main(arguments)==(0 if case=='explicit' else 2)
    assert plans.store.status(admitted['task_id']).status==('succeeded' if case=='explicit' else 'queued')
    assert source.generation_identity() and calls==[]


@pytest.mark.parametrize('lose_bridge_response',[False,True])
def test_explicit_normal_daily_profile_uses_original_ingest_and_resumes_original_receipt_without_sdk(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,lose_bridge_response: bool) -> None:
    from rquant.data_center_maintenance_runtime import (DataCenterRuntimeProfile,DataCenterMaintenanceRuntimeConfig,
        collect_daily_from_profile)
    from rquant.data_collection_bridge import DataCollectionBridge
    from rquant.data_audit_report_jobs import DataAuditReportJobStore
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from tests.unit.test_backfill_execute import _controlled_source,NOW as observed
    with DuckDBStore(tmp_path/'primary.duckdb'):
        pass
    _,_,policies=_controlled_source(tmp_path)
    policy=policies[0]
    with DuckDBStore(policy.primary_writer_gate.primary_path) as seed:
        seed.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=DAY,is_open=True,
            pretrade_date=date(2023,12,29),updated_at=observed),))
    daily=recorder().config.model_copy(update={'code_commit':policy.code_commit,
        'calendar':recorder().config.calendar.model_copy(update={'generated_at':NOW})})
    daily=CollectionRecorderConfig.model_validate(daily.model_dump(mode='python'))
    profile=DataCenterRuntimeProfile(policy_path=tmp_path/'normal-policy.json',
        maintenance=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'normal-replica.duckdb',
            audit_state_path=tmp_path/'normal-audit.sqlite',audit_directory=tmp_path/'normal-reports',
            collection_directory=tmp_path/'normal-collection'),plan_state_path=tmp_path/'normal-plans.sqlite3',
        plan_directory=tmp_path/'normal-plans',allowed_owners=(daily.owner,),daily_collection=daily)
    profile.policy_path.write_text(policy.model_dump_json());profile.policy_path.chmod(0o600)
    path=tmp_path/'normal-profile.json';path.write_text(profile.model_dump_json());path.chmod(0o600)
    sdk=[]
    class Source(DailySource):
        def daily(self,**kwargs: object) -> pd.DataFrame:
            sdk.append(kwargs)
            return super().daily(**kwargs)
    source=Source()
    def original_ingest(trade_date: str,**kwargs: object) -> int:
        return ingest_daily(trade_date,pro=source,status_adapter=source,api_sleep=0,sleep=lambda _:None,
            ingested_at=observed,**kwargs)
    monkeypatch.setattr('rquant.ingest.ingest_daily',original_ingest)
    original_bridge=DataCollectionBridge.run_one
    if lose_bridge_response:
        def lost_response(bridge: DataCollectionBridge):
            original_bridge(bridge)
            raise OSError('original bridge acknowledgement response lost')
        monkeypatch.setattr(DataCollectionBridge,'run_one',lost_response)
        with pytest.raises(OSError,match='acknowledgement'):
            collect_daily_from_profile(path,DAY.isoformat(),clock=lambda:observed)
        monkeypatch.setattr(DataCollectionBridge,'run_one',original_bridge)
    assert collect_daily_from_profile(path,DAY.isoformat(),clock=lambda:observed)==1
    from rquant.screen.replica_source import VerifiedReplicaScreenSource
    source=VerifiedReplicaScreenSource(primary_path=policy.primary_writer_gate.primary_path,
        replica_path=profile.maintenance.replica_path)
    assert source.generation_identity()
    from rquant.replica_generation import replica_generation_path,capture_file_watermark
    watermark=capture_file_watermark(profile.maintenance.replica_path)
    sidecar_bytes=replica_generation_path(profile.maintenance.replica_path).read_bytes()
    assert collect_daily_from_profile(path,DAY.isoformat(),clock=lambda:observed)==1
    assert source.generation_identity()
    assert capture_file_watermark(profile.maintenance.replica_path)==watermark
    assert replica_generation_path(profile.maintenance.replica_path).read_bytes()==sidecar_bytes
    assert sdk==[{'trade_date':'20240102'}]
    jobs=DataAuditReportJobStore(state_path=profile.maintenance.audit_state_path,
        report_directory=profile.maintenance.audit_directory,collection_directory=profile.maintenance.collection_directory,
        clock=lambda:observed)
    task=jobs.latest_success()
    assert task is not None
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,task.report_hash))
    assert report.schema_version==3 and report.collection_completed_through is None
    with jobs._transaction() as state:
        assert state.execute('SELECT COUNT(*) FROM data_audit_report_job').fetchone()[0]==1
    with DuckDBStore(policy.primary_writer_gate.primary_path,read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(1,)


@pytest.mark.parametrize('case',['closed','missing','unknown','contradictory','future','outside','open_empty'])
def test_normal_daily_profile_only_trusted_complete_calendar_can_end_as_closed(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    from argparse import Namespace
    from datetime import timedelta
    from rquant import cli,config as config_module
    from rquant.data_center_maintenance_runtime import DataCenterRuntimeProfile,DataCenterMaintenanceRuntimeConfig,collect_daily_from_profile
    from tests.unit.test_backfill_execute import _controlled_source,NOW as observed
    with DuckDBStore(tmp_path/'primary.duckdb'):
        pass
    _,_,policies=_controlled_source(tmp_path)
    policy=policies[0]
    if case!='missing':
        with DuckDBStore(policy.primary_writer_gate.primary_path) as seed:
            seed.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=DAY,
                is_open=case in {'contradictory','open_empty'},source='other' if case=='unknown' else 'tushare',
                pretrade_date=date(2023,12,29),updated_at=observed),))
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit=policy.code_commit,
        coverage_start=DAY+timedelta(days=1) if case=='outside' else DAY,
        coverage_end=DAY+timedelta(days=1) if case=='outside' else DAY,
        open_dates=(DAY,) if case=='open_empty' else (),
        generated_at=observed+timedelta(seconds=1) if case=='future' else NOW)
    config=recorder().config.model_copy(update={'calendar':calendar,'code_commit':policy.code_commit})
    profile=DataCenterRuntimeProfile(policy_path=tmp_path/'normal-policy.json',
        maintenance=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'normal-replica.duckdb',
            audit_state_path=tmp_path/'normal-audit.sqlite',audit_directory=tmp_path/'normal-reports',
            collection_directory=tmp_path/'normal-collection'),plan_state_path=tmp_path/'normal-plans.sqlite3',
        plan_directory=tmp_path/'normal-plans',allowed_owners=(config.owner,),daily_collection=config)
    profile.policy_path.write_text(policy.model_dump_json());profile.policy_path.chmod(0o600)
    path=tmp_path/'normal-profile.json';path.write_text(profile.model_dump_json());path.chmod(0o600)
    calls=[]
    def no_original_sdk(trade_date: str,**kwargs: object) -> int:
        calls.append(trade_date)
        assert case=='open_empty', 'SDK ingestion cannot run when the calendar is closed or unknown'
        return 0
    monkeypatch.setattr('rquant.ingest.ingest_daily',no_original_sdk)
    if case=='closed':
        assert collect_daily_from_profile(path,DAY.isoformat(),clock=lambda:observed) is None
        monkeypatch.setattr(config_module.settings,'data_center_runtime_profile_path',path)
        monkeypatch.setattr('rquant.data_center_maintenance_runtime.collect_daily_from_profile',
            lambda profile_path,trade_date:collect_daily_from_profile(profile_path,trade_date,clock=lambda:observed))
        monkeypatch.setattr(cli,'setup_logging',lambda:None)
        monkeypatch.setattr(cli.time,'sleep',lambda _:pytest.fail('known closed day must not retry'))
        monkeypatch.setattr('rquant.pipeline.run_daily_pipeline',lambda *args,**kwargs:pytest.fail('closed day cannot screen'))
        assert cli.cmd_ingest(Namespace(date=DAY.isoformat()))==0
        assert cli.cmd_run_daily(Namespace(date=DAY.isoformat(),preset=None,no_ingest=False))==0
        class Day(date):
            @classmethod
            def today(cls) -> date:
                return DAY
        class Scheduler:
            def scheduled_job(self,*args: object,**kwargs: object):
                def bind(function):
                    self.job=function
                    return function
                return bind
            def start(self) -> None:
                self.job()
            def shutdown(self,**kwargs: object) -> None:
                pass
        monkeypatch.setattr(cli,'date',Day)
        monkeypatch.setattr(cli,'_bridge_apscheduler_logging',lambda:None)
        monkeypatch.setattr(cli.signal,'signal',lambda *args:None)
        monkeypatch.setattr('apscheduler.schedulers.blocking.BlockingScheduler',Scheduler)
        assert cli.cmd_serve(Namespace(hour=17))==0
    elif case=='open_empty':
        assert collect_daily_from_profile(path,DAY.isoformat(),clock=lambda:observed)==0
        assert calls==[DAY.isoformat()]
    else:
        with pytest.raises(ValueError,match='calendar|authority'):
            collect_daily_from_profile(path,DAY.isoformat(),clock=lambda:observed)
    if case!='open_empty':
        assert calls==[]
    with DuckDBStore(policy.primary_writer_gate.primary_path,read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(0,)
        assert reader.count_daily()==0
    assert not profile.maintenance.audit_state_path.exists()


def test_normal_daily_none_profile_keeps_original_literal_ingest_call(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.data_center_maintenance_runtime import DataCenterRuntimeProfile,DataCenterMaintenanceRuntimeConfig,collect_daily_from_profile
    profile=DataCenterRuntimeProfile(policy_path=tmp_path/'policy-not-read.json',
        maintenance=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'replica.duckdb',audit_state_path=tmp_path/'audit.sqlite',
            audit_directory=tmp_path/'reports',collection_directory=tmp_path/'collection'),plan_state_path=tmp_path/'plans.sqlite3',
        plan_directory=tmp_path/'plans',allowed_owners=('owner',))
    path=tmp_path/'profile.json';path.write_text(profile.model_dump_json());path.chmod(0o600)
    calls=[]
    def original(trade_date: str) -> int:
        calls.append(trade_date)
        return 17
    monkeypatch.setattr('rquant.ingest.ingest_daily',original)
    assert collect_daily_from_profile(path,DAY.isoformat())==17
    assert calls==[DAY.isoformat()] and 'daily_collection' not in profile.model_dump_json()


def test_explicit_profile_connects_original_plan_and_audit_backends_without_new_queue(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import config as config_module
    from rquant.data_center_maintenance_runtime import DataCenterRuntimeProfile,DataCenterMaintenanceRuntimeConfig,build_data_center_plan_backend,build_data_center_audit_backend
    from rquant.backfill_plan_core import BackfillEstimateAssumptions
    from rquant.page_control_service import build_page_control_service_with_dependencies
    backend,spec,replica,audited,calls=_financial_page_source(tmp_path,monkeypatch)
    assumptions=BackfillEstimateAssumptions(status_namechange_start=date(2026,1,1),status_source_as_of=date(2026,10,5),
        status_window_years=3,adapter_seconds_per_operation=1,market_throttle_seconds_per_operation=0,
        status_throttle_seconds_per_operation=0,retry_allowance_seconds_per_operation=0)
    profile=DataCenterRuntimeProfile(policy_path=backend.config.policy_path,
        maintenance=DataCenterMaintenanceRuntimeConfig(replica_path=replica,audit_state_path=backend.config.audit_state_path,
            audit_directory=backend.config.audit_directory,collection_directory=backend.config.collection_directory),
        plan_state_path=backend.config.plan_state_path,plan_directory=backend.config.plan_directory,
        financial_archive_path=backend.config.financial_archive_path,allowed_owners=(spec.owner,),backfill_plan_assumptions=assumptions)
    path=tmp_path/'runtime-profile.json';path.write_text(profile.model_dump_json());path.chmod(0o600)
    plan=build_data_center_plan_backend(path,clock=backend.clock)
    audit=build_data_center_audit_backend(path,clock=backend.clock)
    assert plan.config.assumptions==assumptions and plan.store.state_path==profile.plan_state_path
    assert audit.store.state_path==profile.maintenance.audit_state_path and audit.store.status(audited.task_id).status=='succeeded'
    monkeypatch.setattr(config_module.settings,'data_center_runtime_profile_path',path)
    service=build_page_control_service_with_dependencies(outbox_path=tmp_path/'page-outbox.sqlite3',data_dir=tmp_path,
        log_dir=tmp_path/'logs',allowed_lab_export_roots=(tmp_path/'exports',),load_default_lab_backend=False,clock=backend.clock)
    assert service.consumer.backfill_plan_backend is not None and service.consumer.data_audit_report_backend is not None
    assert service.consumer.data_center_execution_backend is not None and calls==[]
    legacy=profile.model_copy(update={'backfill_plan_assumptions':None})
    path.write_text(legacy.model_dump_json())
    assert build_data_center_plan_backend(path) is None
