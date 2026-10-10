from __future__ import annotations

import json
from datetime import UTC,date,datetime,timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest

from rquant.adapter.tushare import TushareAdapter,normalize_sdk_nullable_response
from rquant.financial_pit_acquisition import FinancialArchive,FinancialQuery,acquire_financial_batches
from rquant.source_quota_transport import QuotaBoundTransportObserver

NOW=datetime(2026,10,5,10,tzinfo=UTC)
APIS=('fina_indicator','income','balancesheet','cashflow','forecast','express','dividend')


def test_seven_actual_sdk_methods_enter_original_archive_and_resume_original_queries(tmp_path: Path,monkeypatch) -> None:
    from tushare.pro.client import DataApi
    calls=[]
    class HTTPResponse:
        def __init__(self,row: dict) -> None:
            self.text=json.dumps({'code':0,'data':{'fields':list(row),'items':[list(row.values())]}})
    def http(*args,**kwargs):
        api=kwargs['json']['api_name']
        calls.append(api)
        row={'ts_code':'600000.SH','ann_date':'20260930','end_date':'20260630'}
        if api in {'income','balancesheet','cashflow'}:
            row.update(f_ann_date=None,report_type='1')
        if api=='forecast':
            row['type']='预增'
        if api=='dividend':
            row['div_proc']='实施'
        if api=='fina_indicator':
            row.update(roe=None,or_yoy=10.0,netprofit_yoy=20.0)
        return HTTPResponse(row)
    monkeypatch.setattr('tushare.pro.client.requests.post',http)
    observer=QuotaBoundTransportObserver(path=tmp_path/'quota.sqlite',source='fixture.financial',
        quota_units_per_window=10,window_kind='day',clock=lambda:NOW)
    adapter=TushareAdapter.__new__(TushareAdapter)
    adapter._pro=DataApi(token='dummy-offline-token')
    adapter._backup_token=''
    adapter.bind_transport_observer(observer)
    adapter.bind_sdk_null_normalization('tushare-nullable-v1')
    archive=FinancialArchive(tmp_path/'archive')
    queries=[]
    at=NOW
    for ordinal,api in enumerate(APIS,1):
        params={'request_id':UUID(int=ordinal),'api':api,'ts_code':'600000.SH'}
        if api=='fina_indicator':
            params['period']=date(2026,6,30)
        elif api=='dividend':
            params['ann_date']=date(2026,9,30)
        else:
            params.update(start_date=date(2026,9,1),end_date=date(2026,9,30))
        query=FinancialQuery(**params)
        queries.append(query)
        at=NOW+timedelta(seconds=ordinal)
        with observer.scope(logical_request_id=str(query.request_id),observed_at=NOW):
            receipt=acquire_financial_batches(adapter,archive,(query,),run_day=NOW.date(),clock=lambda:at)[0]
        assert receipt.observed_at==at and receipt.status=='observed'
    first=archive.read_batch(queries[0].request_id)
    assert first.rows[0].values['roe'] is None
    assert first.rows[0].values['or_yoy']==10.0
    with observer.scope(logical_request_id='unused-restart-scope',observed_at=NOW):
        restored=acquire_financial_batches(adapter,archive,queries,run_day=NOW.date(),clock=lambda:pytest.fail('first observed clock reset'))
    assert len(restored)==7 and calls==list(APIS) and observer.remaining(now=NOW)==3


def test_nullable_financial_boundary_keeps_missing_unknown_and_rejects_decimal_nonfinite() -> None:
    frame=pd.DataFrame({'roe':pd.Series([pd.NA,None,float('nan'),12.5],dtype=object),'label':['a','b','c','d']})
    result=normalize_sdk_nullable_response('fina_indicator',frame)
    assert result['roe'].tolist()==[None,None,None,12.5]
    assert result['label'].tolist()==frame['label'].tolist()
    with pytest.raises(ValueError,match='Decimal'):
        normalize_sdk_nullable_response('fina_indicator',pd.DataFrame([{'roe':Decimal('NaN')}]))
    unchanged=normalize_sdk_nullable_response('fina_indicator',pd.DataFrame([{'roe':'invalid-number','ann_date':'bad-date'}]))
    assert unchanged.loc[0,'roe']=='invalid-number' and unchanged.loc[0,'ann_date']=='bad-date'


def test_invalid_financial_dates_still_fail_original_archive_without_replacing_missing(tmp_path: Path) -> None:
    query=FinancialQuery(request_id=UUID(int=9),api='fina_indicator',ts_code='600000.SH',period=date(2026,6,30))
    class Client:
        def fina_indicator(self,**kwargs: str) -> pd.DataFrame:
            return normalize_sdk_nullable_response('fina_indicator',pd.DataFrame([{
                'ts_code':'600000.SH','ann_date':'bad-date','end_date':'20260630','roe':None}]))
    archive=FinancialArchive(tmp_path/'archive')
    with pytest.raises(ValueError,match='date'):
        acquire_financial_batches(Client(),archive,(query,),run_day=NOW.date(),clock=lambda:NOW)
    assert archive._read_receipt(query.request_id) is None


def test_original_documented_financial_limit_is_retained_as_possibly_truncated_and_recovered_without_sdk(tmp_path: Path) -> None:
    query=FinancialQuery(request_id=UUID(int=17),api='fina_indicator',ts_code='600000.SH',period=date(2026,6,30))
    calls=[]
    class Client:
        def fina_indicator(self,**parameters: str) -> pd.DataFrame:
            calls.append(parameters)
            return pd.DataFrame([{'ts_code':'600000.SH','ann_date':'20260930','end_date':'20260630','roe':None}
                for _ in range(100)])
    archive=FinancialArchive(tmp_path/'archive')
    receipt=acquire_financial_batches(Client(),archive,(query,),run_day=NOW.date(),clock=lambda:NOW)[0]
    assert receipt.status=='possibly_truncated' and receipt.row_count==100
    assert archive.read_batch(query.request_id).status=='possibly_truncated'
    recovered=acquire_financial_batches(Client(),archive,(query,),run_day=NOW.date(),
        clock=lambda:pytest.fail('verified archive recovery cannot reset observation time'))[0]
    assert recovered==receipt and len(calls)==1


def test_original_financial_page_existing_mode_requires_outer_transaction_and_rolls_back_cursor(tmp_path: Path) -> None:
    from tests.unit.test_financial_pit_facts import _observe,_row
    from rquant.financial_pit_facts import _import_page
    from rquant.storage.duckdb import DuckDBStore
    archive=FinancialArchive(tmp_path/'archive')
    _observe(archive,NOW,[_row()])
    page=archive.committed_page(limit=32)
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        with pytest.raises(ValueError,match='outer transaction'):
            _import_page(store._conn,page,None,transaction_mode='existing')
        store._conn.execute('BEGIN')
        _import_page(store._conn,page,None,transaction_mode='existing')
        store._conn.execute('ROLLBACK')
        assert store._conn.execute('SELECT COUNT(*) FROM financial_observation').fetchone()[0]==0
        assert store._conn.execute('SELECT COUNT(*) FROM financial_import_cursor').fetchone()[0]==0


def test_original_six_fields_prepared_outside_writer_keep_version_and_outer_atomicity(tmp_path: Path) -> None:
    from tests.unit.test_fundamental_daily import _conn,_finance,_valuation,MONDAY,SYMBOL
    from rquant.fundamental_daily import prepare_fundamental_daily,derive_fundamental_daily,read_fundamental_daily,FundamentalDailyQuery
    with _conn() as conn:
        archive=FinancialArchive(tmp_path/'archive')
        _finance(conn,archive)
        _valuation(conn)
        query=FundamentalDailyQuery(ts_code=SYMBOL,trade_date=MONDAY)
        prepared=prepare_fundamental_daily(conn,query)
        assert read_fundamental_daily(conn,query) is None
        with pytest.raises(ValueError,match='outer transaction'):
            derive_fundamental_daily(conn,query,transaction_mode='existing',prepared=prepared)
        conn.execute('BEGIN')
        actual=derive_fundamental_daily(conn,query,transaction_mode='existing',prepared=prepared)
        assert actual==prepared.version
        conn.execute('ROLLBACK')
        assert read_fundamental_daily(conn,query) is None
        default=derive_fundamental_daily(conn,query)
        assert default==prepared.version
        assert [default.fields[name].value for name in ('pe_ttm','pb','dv_ttm','roe','or_yoy','netprofit_yoy')]==[10,2,1.5,12,18,20]


def test_original_prepared_fundamentals_refuse_source_or_head_changes(tmp_path: Path) -> None:
    from tests.unit.test_fundamental_daily import _conn,_finance,_valuation,MONDAY,SYMBOL
    from rquant.fundamental_daily import prepare_fundamental_daily,derive_fundamental_daily,FundamentalDailyQuery
    with _conn() as conn:
        archive=FinancialArchive(tmp_path/'archive')
        _finance(conn,archive)
        _valuation(conn)
        query=FundamentalDailyQuery(ts_code=SYMBOL,trade_date=MONDAY)
        prepared=prepare_fundamental_daily(conn,query)
        _valuation(conn,revision=2,observed_at=datetime(2026,9,26,8,tzinfo=UTC),pe_ttm=12.0)
        conn.execute('BEGIN')
        with pytest.raises(ValueError,match='source|head'):
            derive_fundamental_daily(conn,query,transaction_mode='existing',prepared=prepared)
        conn.execute('ROLLBACK')
        assert conn.execute('SELECT COUNT(*) FROM fundamental_daily_version').fetchone()[0]==0


def test_financial_fixed_scope_uses_original_queries_and_bounded_original_manifest(tmp_path: Path) -> None:
    from rquant.financial_runtime import FinancialCollectionPlan,build_financial_manifest
    from tests.unit.test_backfill_execute import _exact_day_setup
    _,_,spec,current,_,args=_exact_day_setup(tmp_path)
    plan=FinancialCollectionPlan.create(owner=spec.owner,prepare_command_id='finance-scope',
        source_generation_id=spec.intent.source_generation_id,primary_identity=spec.intent.primary_identity,
        calendar=args['calendar'],archive_path=tmp_path/'financial-archive',archive_id='a'*32,
        securities=('600000.SH',),start_date=NOW.date(),end_date=NOW.date(),report_periods=(date(2026,6,30),),
        code_commit=current[0].code_commit)
    assert len(plan.queries)==7 and {query.api for query in plan.queries}==set(APIS)
    assert build_financial_manifest(plan).tasks[0].payload['query_start']==0
    assert len(build_financial_manifest(plan).tasks)==3
    assert FinancialCollectionPlan.model_validate_json(plan.model_dump_json())==plan
    assert len({query.request_id for query in plan.queries})==7
    changed=plan.model_copy(update={'owner':'other-owner'})
    with pytest.raises(ValueError,match='identity|content'):
        FinancialCollectionPlan.model_validate_json(changed.model_dump_json())
    with pytest.raises(ValueError,match='capacity|request|range'):
        FinancialCollectionPlan.create(owner=spec.owner,prepare_command_id='too-many',
            source_generation_id=spec.intent.source_generation_id,primary_identity=spec.intent.primary_identity,
            calendar=args['calendar'],archive_path=tmp_path/'financial-archive',archive_id='a'*32,
            securities=tuple(f'{number:06d}.SH' for number in range(1,801)),start_date=date(2020,1,1),
            end_date=NOW.date(),report_periods=(date(2026,6,30),),code_commit=current[0].code_commit)


def _financial_runtime_setup(tmp_path: Path,monkeypatch,*,start_date: date | None=None,end_date: date | None=None,
        securities: tuple[str,...]=('600000.SH',),quota_units: int | None=None):
    import hashlib
    from tests.unit.test_backfill_execute_admission import _state_execution
    from tests.unit.test_backfill_execute import _controlled_source
    from rquant.backfill_execute_contracts import ApiEntitlementEvidence,DataCenterExecutionPolicy
    from rquant.backfill_state import BackfillStateStore
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.financial_runtime import FinancialCollectionPlan,FinancialExecutionIntent,FinancialExecutionSpec,FinancialRuntimeWorker,build_financial_manifest
    from rquant.runtime_contracts import canonical_sha256
    from rquant.storage.duckdb import DuckDBStore
    from tushare.pro.client import DataApi
    _,old,_,_= _state_execution(tmp_path)
    with DuckDBStore(tmp_path/'primary.duckdb'):
        pass
    _,_,current=_controlled_source(tmp_path)
    rights=[]
    for api in APIS:
        entitlement=ApiEntitlementEvidence(api_name=api,status='verified',source_account_sha256=current[0].source_account_sha256,
            proof_source='offline_fixture',evidence_path=tmp_path/(api+'-rights.json'),evidence_sha256='0'*64,
            valid_from=NOW-timedelta(minutes=1),expires_at=NOW+timedelta(hours=1),allowed_parameters=('ts_code','period','start_date','end_date','ann_date'),
            scope_start=date(2020,1,1),scope_end=NOW.date(),allowed_symbols=securities,provider_row_limit=5000)
        data=json.dumps({'kind':'source-entitlement-evidence/v1','proof':entitlement.model_dump(mode='json',
            exclude={'evidence_path','evidence_sha256'})},sort_keys=True,separators=(',',':')).encode()
        entitlement.evidence_path.write_bytes(data)
        entitlement.evidence_path.chmod(0o400)
        rights.append(entitlement.model_copy(update={'evidence_sha256':hashlib.sha256(data).hexdigest()}))
    current[0]=DataCenterExecutionPolicy.model_validate_json(current[0].model_copy(update={
        'policy_generation':None,'backfill_execute_enabled':False,'financial_collect_enabled':True,'entitlement_evidence':tuple(rights),
        'quota_units_per_window':quota_units or current[0].quota_units_per_window}).model_dump_json())
    archive=FinancialArchive(tmp_path/'financial-archive')
    metadata=archive.committed_page(limit=1)
    start_date=start_date or NOW.date()
    end_date=end_date or NOW.date()
    dates=tuple(start_date+timedelta(days=i) for i in range((end_date-start_date).days+1))
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit=current[0].code_commit,
        coverage_start=start_date,coverage_end=end_date,open_dates=dates,generated_at=NOW)
    if start_date!=NOW.date() or end_date!=NOW.date():
        from rquant.trade_calendar import TradeCalendarDay
        with DuckDBStore(tmp_path/'primary.duckdb') as store:
            store.upsert_trade_calendar(tuple(TradeCalendarDay(exchange='SSE',cal_date=day,is_open=True,
                pretrade_date=day-timedelta(days=1),updated_at=NOW) for day in dates))
    plan=FinancialCollectionPlan.create(owner=old.owner,prepare_command_id='prepare-finance',source_generation_id=old.intent.source_generation_id,
        primary_identity=old.intent.primary_identity,calendar=calendar,archive_path=archive.root,archive_id=metadata.archive_id,
        securities=securities,start_date=start_date,end_date=end_date,report_periods=(date(2026,6,30),),code_commit=current[0].code_commit)
    intent=FinancialExecutionIntent(execution_id=plan.execution_id,owner=plan.owner,prepare_command_id=plan.prepare_command_id,
        plan=plan,plan_sha256=plan.content_sha256,source_generation_id=plan.source_generation_id,primary_identity=plan.primary_identity,
        policy_generation=current[0].policy_generation,nonce_sha256=canonical_sha256(('financial-confirmation',plan.execution_id)),
        issued_at=NOW,expires_at=NOW+timedelta(minutes=5))
    spec=FinancialExecutionSpec(execution_id=plan.execution_id,owner=plan.owner,manifest_id=plan.manifest_id,plan=plan,
        intent=intent,execute_command_id='execute-finance',admission_policy=current[0])
    state=BackfillStateStore(tmp_path/'state.sqlite3',maintenance_enabled=True,busy_timeout_ms=30)
    state.persist_maintenance_intent(intent)
    state.admit_financial_execution(spec,build_financial_manifest(plan),now=NOW)
    ticks=[0]
    def clock():
        ticks[0]+=1
        return NOW+timedelta(microseconds=ticks[0])
    calls=[]
    class HTTPResponse:
        def __init__(self,row):
            self.text=json.dumps({'code':0,'data':{'fields':list(row),'items':[list(row.values())]}})
    def http(*args,**kwargs):
        api=kwargs['json']['api_name']
        calls.append(api)
        row={'ts_code':kwargs['json']['params']['ts_code'],'ann_date':end_date.strftime('%Y%m%d'),'end_date':'20260630'}
        if api in {'income','balancesheet','cashflow'}:
            row.update(f_ann_date=None,report_type='1')
        if api=='forecast':
            row['type']='预增'
        if api=='dividend':
            row['div_proc']='实施'
        if api=='fina_indicator':
            row.update(roe=None,or_yoy=10.0,netprofit_yoy=20.0)
        return HTTPResponse(row)
    monkeypatch.setattr('tushare.pro.client.requests.post',http)
    def adapter_factory(observer):
        adapter=TushareAdapter.__new__(TushareAdapter)
        adapter._pro=DataApi(token='dummy-offline-token')
        adapter._backup_token=''
        adapter.bind_transport_observer(observer)
        adapter.bind_sdk_null_normalization('tushare-nullable-v1')
        return adapter
    worker=FinancialRuntimeWorker(state,policy=lambda:current[0],adapter_factory=adapter_factory,clock=clock)
    return state,spec,current,archive,worker,calls


def test_actual_financial_worker_archives_imports_seven_interfaces_and_derives_original_six_fields(tmp_path: Path,monkeypatch) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.financial_runtime import verify_financial_runtime_receipt
    state,spec,_,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='query_committed' and calls==list(APIS)
    first_times=tuple(archive.receipt(query.request_id).observed_at for query in spec.plan.queries)
    assert len(set(first_times))==7
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='fundamentals_committed' and calls==list(APIS)
    assert state.get_task(spec.manifest_id,'verify-completion').status=='pending'
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM financial_observation').fetchone()[0]==7
        assert reader._conn.execute('SELECT COUNT(*) FROM fundamental_daily_version').fetchone()[0]==1
        raw=reader._conn.execute("SELECT receipt_id FROM data_center_financial_runtime_receipt WHERE task_id='financial-raw-0000'").fetchone()[0]
        claims=verify_financial_runtime_receipt(reader,raw,as_of=worker.clock())
        assert len(claims)==7 and all(not claim.coverage_complete for claim in claims)


@pytest.mark.parametrize('failure',['import_receipt','task_ack'])
def test_financial_original_archive_and_receipts_resume_after_actual_commit_failure_without_sdk_or_clock_reset(tmp_path: Path,monkeypatch,failure: str) -> None:
    from rquant import financial_runtime
    from rquant.storage.duckdb import DuckDBStore
    state,spec,_,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    if failure=='import_receipt':
        original=financial_runtime._write_runtime_receipt
        def broken(store,receipt):
            if receipt.kind=='import_page':
                raise OSError('financial import receipt I/O failure')
            return original(store,receipt)
        monkeypatch.setattr(financial_runtime,'_write_runtime_receipt',broken)
    else:
        original=state._succeed_in_transaction
        def broken(*args,**kwargs):
            raise OSError('original task success acknowledgement lost')
        monkeypatch.setattr(state,'_succeed_in_transaction',broken)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='partial' and calls==list(APIS)
    first=tuple(archive.receipt(query.request_id) for query in spec.plan.queries)
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM financial_observation').fetchone()[0]==(0 if failure=='import_receipt' else 7)
    if failure=='import_receipt':
        monkeypatch.setattr(financial_runtime,'_write_runtime_receipt',original)
    else:
        monkeypatch.setattr(state,'_succeed_in_transaction',original)
    current=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume-finance',expected_sequence=current.control_sequence,
        action='resume',now=worker.clock())
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='query_committed' and calls==list(APIS)
    assert tuple(archive.receipt(query.request_id) for query in spec.plan.queries)==first


@pytest.mark.parametrize('columns',[129,257])
def test_actual_controlled_financial_sdk_preserves_original_256_column_limit(tmp_path: Path,monkeypatch,columns: int) -> None:
    state,spec,_,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    original_http=__import__('tushare.pro.client',fromlist=['requests']).requests.post
    def wide_response(*args,**kwargs):
        response=original_http(*args,**kwargs)
        body=json.loads(response.text)
        fields=body['data']['fields']
        while len(fields)<columns:
            fields.append(f'original_numeric_{len(fields)}')
            body['data']['items'][0].append(1.0)
        response.text=json.dumps(body)
        return response
    monkeypatch.setattr('tushare.pro.client.requests.post',wide_response)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome==('query_committed' if columns==129 else 'partial')
    if columns==129:
        assert calls==list(APIS)
        assert len(archive.read_batch(spec.plan.queries[0].request_id).rows[0].values)==129
    else:
        assert calls==['fina_indicator']
        assert archive._read_receipt(spec.plan.queries[0].request_id) is None


@pytest.mark.parametrize('ledger_failure',['missing','unknown'])
def test_committed_financial_task_recovery_requires_original_dispatch_proof(tmp_path: Path,monkeypatch,ledger_failure: str) -> None:
    import sqlite3
    state,spec,current,archive,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    original=state._succeed_in_transaction
    monkeypatch.setattr(state,'_succeed_in_transaction',lambda *args,**kwargs:(_ for _ in ()).throw(OSError('lost acknowledgement')))
    assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='partial'
    first=tuple(archive.receipt(query.request_id) for query in spec.plan.queries)
    monkeypatch.setattr(state,'_succeed_in_transaction',original)
    with sqlite3.connect(current[0].quota_ledger_path) as connection:
        if ledger_failure=='missing':
            connection.execute('DELETE FROM quota_attempt WHERE attempt_id=(SELECT attempt_id FROM quota_attempt ORDER BY attempt_id LIMIT 1)')
        else:
            connection.execute("UPDATE quota_attempt SET outcome='unknown' WHERE attempt_id=(SELECT attempt_id FROM quota_attempt ORDER BY attempt_id LIMIT 1)")
    status=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume-unproved-finance',
        expected_sequence=status.control_sequence,action='resume',now=worker.clock())
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='partial' and calls==list(APIS)
    assert state.get_task(spec.manifest_id,'financial-raw-0000').status!='succeeded'
    assert tuple(archive.receipt(query.request_id) for query in spec.plan.queries)==first
