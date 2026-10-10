from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import inspect
from pathlib import Path

import pytest

from rquant.backfill_state import (
    BackfillManifestInput, BackfillStateStore, BackfillTaskInput, StaleTaskClaimError,
)

NOW = datetime(2026,10,5,10,tzinfo=UTC)


def _claimed(tmp_path: Path):
    state = BackfillStateStore(tmp_path/'state.sqlite3',busy_timeout_ms=30)
    state.persist_manifest(BackfillManifestInput(
        manifest_id='owned-plan',payload={'kind':'exact-day-test'},
        tasks=(BackfillTaskInput(task_id='day-1',payload={'day':'2026-10-01'}),),
        eligibility=(),
    ),now=NOW)
    claim=state.claim_task('owned-plan',worker_id='a',lease_seconds=120,now=NOW)
    assert claim is not None
    return state,claim


def test_commit_protection_holds_original_claim_through_fact_commit(tmp_path: Path) -> None:
    state,claim=_claimed(tmp_path)
    assert hasattr(state,'commit_claim')
    competing=BackfillStateStore(state.path,busy_timeout_ms=30)
    with state.commit_claim(claim,now=NOW) as protected:
        # This is a distinct SQLite connection and an expired-time claimant.
        with pytest.raises(Exception,match='locked|busy'):
            competing.claim_task('owned-plan',worker_id='b',lease_seconds=120,now=NOW+timedelta(seconds=121))
        protected.verify(now=NOW+timedelta(seconds=1))
        protected.succeed(duration_seconds=1,now=NOW+timedelta(seconds=1))
    assert state.get_task('owned-plan','day-1').status=='succeeded'


def test_stale_or_expired_claim_cannot_enter_commit_protection(tmp_path: Path) -> None:
    state,claim=_claimed(tmp_path)
    assert hasattr(state,'commit_claim')
    with pytest.raises(StaleTaskClaimError):
        with state.commit_claim(claim,now=NOW+timedelta(seconds=121)):
            pytest.fail('expired claim entered commit protection')
    replacement=state.claim_task('owned-plan',worker_id='b',lease_seconds=120,now=NOW+timedelta(seconds=121))
    assert replacement is not None
    with pytest.raises(StaleTaskClaimError):
        with state.commit_claim(claim,now=NOW+timedelta(seconds=122)):
            pytest.fail('replaced token entered commit protection')


def test_failure_in_protected_fact_commit_keeps_original_task_recoverable(tmp_path: Path) -> None:
    state,claim=_claimed(tmp_path)
    assert hasattr(state,'commit_claim')
    with pytest.raises(RuntimeError,match='fact'):
        with state.commit_claim(claim,now=NOW):
            raise RuntimeError('fact commit response lost')
    assert state.get_task('owned-plan','day-1').status=='running'
    recovered=state.claim_task('owned-plan',worker_id='b',lease_seconds=120,now=NOW+timedelta(seconds=121))
    assert recovered is not None and recovered.claim_token!=claim.claim_token


def test_original_observer_resumes_confirmed_failure_with_stable_ordinal(tmp_path: Path) -> None:
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    assert 'next_call_ordinal' in inspect.signature(QuotaBoundTransportObserver.scope).parameters
    observer=QuotaBoundTransportObserver(path=tmp_path/'quota.sqlite',source='test.source',
        quota_units_per_window=8,window_kind='day',clock=lambda: NOW)
    with observer.scope(logical_request_id='stable-original-request',observed_at=NOW):
        with pytest.raises(RuntimeError):
            observer.observe('daily',lambda: (_ for _ in ()).throw(RuntimeError('known refusal')))
    first=observer.get_call_attempt(logical_request_id='stable-original-request',api_name='daily')
    assert first is not None
    with observer.scope(logical_request_id='stable-original-request',observed_at=NOW,
                        next_call_ordinal=2,resume_api_name='daily'):
        assert observer.observe('daily',lambda: 'actual-response')=='actual-response'
        receipt=observer.current_receipts()[0]
        assert receipt.call_ordinal==2 and receipt.logical_request_id=='stable-original-request'
    assert observer.remaining(now=NOW)==6


def test_original_observer_rejects_skipped_or_unknown_resume(tmp_path: Path) -> None:
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    from rquant.source_quota_store import SourceQuotaConflictError
    assert 'next_call_ordinal' in inspect.signature(QuotaBoundTransportObserver.scope).parameters
    observer=QuotaBoundTransportObserver(path=tmp_path/'quota.sqlite',source='test.source',
        quota_units_per_window=8,window_kind='day',clock=lambda: NOW)
    for ordinal,api in ((2,'daily'),(3,'daily'),(2,'adj_factor')):
        with pytest.raises(SourceQuotaConflictError):
            with observer.scope(logical_request_id='never-dispatched',observed_at=NOW,
                                next_call_ordinal=ordinal,resume_api_name=api):
                pytest.fail('scope skipped durable attempts')


def test_actual_calendar_path_obeys_bound_observer_without_switching_token(tmp_path: Path) -> None:
    from rquant.adapter.tushare import TushareAdapter
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    from rquant.source_quota_store import SourceQuotaExhaustedError
    import pandas as pd
    calls: list[str]=[]
    class ActualSDK:
        def trade_cal(self,**kwargs: str) -> pd.DataFrame:
            calls.append('calendar')
            return pd.DataFrame([{'exchange':'SSE','cal_date':'20261001','is_open':0,'pretrade_date':'20260930'}])
    observer=QuotaBoundTransportObserver(path=tmp_path/'quota.sqlite',source='test.calendar',
        quota_units_per_window=1,window_kind='day',clock=lambda: NOW)
    adapter=TushareAdapter.__new__(TushareAdapter)
    adapter._pro=ActualSDK()
    adapter._backup_token=''
    adapter._using_backup=False
    adapter.bind_transport_observer(observer)
    with observer.scope(logical_request_id='calendar-1',observed_at=NOW):
        adapter.trade_cal(NOW.date(),NOW.date())
    with observer.scope(logical_request_id='calendar-2',observed_at=NOW):
        with pytest.raises(SourceQuotaExhaustedError):
            adapter.trade_cal(NOW.date(),NOW.date())
    assert calls==['calendar']


def _controlled_source(tmp_path: Path):
    import hashlib
    import json
    from rquant.backfill_execute import ControlledTransportObserver
    from rquant.backfill_execute_contracts import ApiEntitlementEvidence, DataCenterExecutionPolicy, BackfillSourceRequestBinding
    from rquant.runtime_contracts import canonical_sha256
    from rquant.source_quota_store import SourceQuotaStore
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    from rquant.storage.primary_writer_gate import PrimaryWriterGateConfig
    state=BackfillStateStore(tmp_path/'state.sqlite3')
    quota=SourceQuotaStore(tmp_path/'quota.sqlite3',boot_id='offline-fixture')
    primary=tmp_path/'primary.duckdb'
    if not primary.exists():
        primary.write_bytes(b'physical-offline-primary')
    lock=tmp_path/'primary.lock'
    lock.write_bytes(b'')
    lock.chmod(0o600)
    gate=PrimaryWriterGateConfig.capture(primary_path=primary,lock_path=lock)
    def write(path: Path,payload: object) -> str:
        data=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
        path.write_bytes(data)
        path.chmod(0o400)
        return hashlib.sha256(data).hexdigest()
    rights_path=tmp_path/'rights.json'
    entitlement=ApiEntitlementEvidence(api_name='daily',status='verified',source_account_sha256='a'*64,
        proof_source='offline_fixture',evidence_path=rights_path,evidence_sha256='0'*64,
        valid_from=NOW-timedelta(minutes=1),expires_at=NOW+timedelta(hours=1),
        allowed_parameters=('trade_date',),scope_start=NOW.date()-timedelta(days=10),scope_end=NOW.date(),
        full_market=True,provider_row_limit=8000)
    rights_hash=write(rights_path,{'kind':'source-entitlement-evidence/v1','proof':
        entitlement.model_dump(mode='json',exclude={'evidence_path','evidence_sha256'})})
    entitlement=entitlement.model_copy(update={'evidence_sha256':rights_hash})
    installed_path=tmp_path/'writers.json'
    installed_hash=write(installed_path,{'kind':'primary-writer-participation/v1',
        'primary_generation':canonical_sha256({'canonical_path':str(primary),'device':gate.primary_device,'inode':gate.primary_inode}),
        'writers':['daily','monitor','research_sync','backup','replica_sync','controlled_maintenance'],
        'environment':'offline_fixture','valid_until':(NOW+timedelta(hours=1)).isoformat()})
    state_stat=state.path.stat()
    quota_stat=quota.path.stat()
    policy=DataCenterExecutionPolicy(environment='offline_fixture',backfill_execute_enabled=True,code_commit='b'*40,
        primary_writer_gate=gate,original_state_path=state.path,original_state_device=state_stat.st_dev,
        original_state_inode=state_stat.st_ino,quota_ledger_path=quota.path,quota_ledger_device=quota_stat.st_dev,
        quota_ledger_inode=quota_stat.st_ino,quota_source='fixture.tushare',source_account_sha256='a'*64,
        quota_units_per_window=10,quota_window_kind='day',writer_installation_evidence_path=installed_path,
        writer_installation_evidence_sha256=installed_hash,entitlement_evidence=(entitlement,),
        source_material_directory=tmp_path/'source',valid_from=NOW-timedelta(minutes=1),expires_at=NOW+timedelta(hours=1))
    current=[policy]
    original=QuotaBoundTransportObserver(store=quota,source=policy.quota_source,quota_units_per_window=10,
        window_kind='day',clock=lambda: NOW)
    source=ControlledTransportObserver(original,policy=lambda:current[0],kind='backfill',
        material_directory=policy.source_material_directory,clock=lambda:NOW,claim_guard=lambda:None)
    binding=BackfillSourceRequestBinding(owner='fixture-owner',execution_id='c'*64,manifest_id='d'*64,
        plan_sha256='e'*64,scope_sha256='f'*64,api_name='daily',parameters={'trade_date':NOW.strftime('%Y%m%d')},
        source_account_sha256=policy.source_account_sha256,quota_source=policy.quota_source,
        quota_ledger_device=quota_stat.st_dev,quota_ledger_inode=quota_stat.st_ino)
    return source,binding,current


def _source_adapter(source,call):
    from rquant.adapter.tushare import TushareAdapter
    class SDK:
        def daily(self,**kwargs):
            return call()
    adapter=TushareAdapter.__new__(TushareAdapter)
    adapter._pro=SDK()
    adapter._backup_token=''
    adapter._using_backup=False
    adapter.bind_transport_observer(source)
    return adapter


def test_controlled_source_checks_actual_rights_before_sdk_and_backoff(tmp_path: Path,monkeypatch) -> None:
    from rquant.source_quota_store import SourceQuotaConflictError
    source,binding,current=_controlled_source(tmp_path)
    current[0]=current[0].model_copy(update={'entitlement_evidence':(), 'policy_generation':None})
    calls=[]
    monkeypatch.setattr('rquant.adapter.tushare.time.sleep',lambda value:pytest.fail('local refusal entered SDK backoff'))
    adapter=_source_adapter(source,lambda:calls.append('called'))
    with source.request(binding),pytest.raises(SourceQuotaConflictError,match='entitlement|policy'):
        adapter.daily_by_date(NOW.date())
    assert calls==[] and source.original.request_attempts(binding.logical_request_id)==()


def test_controlled_source_timeout_retains_original_charge_and_pauses_restart(tmp_path: Path,monkeypatch) -> None:
    from rquant.source_quota_store import SourceQuotaConflictError
    source,binding,_=_controlled_source(tmp_path)
    calls=[]
    def unknown():
        calls.append('called')
        raise TimeoutError('response not received')
    adapter=_source_adapter(source,unknown)
    monkeypatch.setattr('rquant.adapter.tushare.time.sleep',lambda value:pytest.fail('unknown response retried'))
    with source.request(binding),pytest.raises(SourceQuotaConflictError,match='uncertain'):
        adapter.daily_by_date(NOW.date())
    with pytest.raises(SourceQuotaConflictError,match='definitive supplier'):
        with source.request(binding):
            adapter.daily_by_date(NOW.date())
    assert calls==['called'] and source.original.remaining(now=NOW)==9


def test_controlled_source_replays_successful_material_without_sdk(tmp_path: Path) -> None:
    import pandas as pd
    from rquant.backfill_execute import ControlledTransportObserver
    source,binding,current=_controlled_source(tmp_path)
    calls=[]
    def success():
        calls.append('called')
        return pd.DataFrame([{'ts_code':'000001.SZ','trade_date':NOW.strftime('%Y%m%d'),'close':10.5}])
    adapter=_source_adapter(source,success)
    with source.request(binding):
        first=adapter.daily_by_date(NOW.date())
    restarted=ControlledTransportObserver(source.original,policy=lambda:current[0],kind='backfill',
        material_directory=current[0].source_material_directory,clock=lambda:NOW,claim_guard=lambda:None)
    with restarted.request(binding):
        second=_source_adapter(restarted,lambda:pytest.fail('durable source dispatch repeated')).daily_by_date(NOW.date())
    pd.testing.assert_frame_equal(first,second)
    assert calls==['called'] and source.original.remaining(now=NOW)==9
    assert restarted.current_receipts()[0].attempt_id==source.current_receipts()[0].attempt_id


def test_controlled_source_resumes_definitive_refusal_with_bounded_original_ordinals(tmp_path: Path,monkeypatch) -> None:
    import pandas as pd
    from rquant.backfill_execute import DefinitiveSupplierFailure,SupplierFailureEvidence
    source,binding,_=_controlled_source(tmp_path)
    calls=[]
    def dispatch():
        calls.append('called')
        if len(calls)==1:
            raise DefinitiveSupplierFailure(SupplierFailureEvidence(api_name='daily',source_account_sha256='a'*64,
                supplier_response_sha256='1'*64,refusal='频率超限',observed_at=NOW))
        return pd.DataFrame([{'ts_code':'000001.SZ','trade_date':NOW.strftime('%Y%m%d'),'close':10.5}])
    monkeypatch.setattr('rquant.adapter.tushare.time.sleep',lambda value:None)
    with source.request(binding):
        result=_source_adapter(source,dispatch).daily_by_date(NOW.date())
    assert len(result)==1 and len(calls)==2 and source.original.remaining(now=NOW)==8
    assert [item.call_ordinal for item in source.current_receipts()]==[1,2]


def test_controlled_source_current_entitlement_is_rechecked_for_original_retry(tmp_path: Path,monkeypatch) -> None:
    from rquant.backfill_execute import DefinitiveSupplierFailure,SupplierFailureEvidence
    from rquant.source_quota_store import SourceQuotaConflictError
    source,binding,current=_controlled_source(tmp_path)
    calls=[]
    def denied_after_dispatch():
        calls.append('called')
        current[0]=current[0].model_copy(update={'entitlement_evidence':(), 'policy_generation':None})
        raise DefinitiveSupplierFailure(SupplierFailureEvidence(api_name='daily',source_account_sha256='a'*64,
            supplier_response_sha256='1'*64,refusal='频率超限',observed_at=NOW))
    monkeypatch.setattr('rquant.adapter.tushare.time.sleep',lambda value:None)
    with source.request(binding),pytest.raises(SourceQuotaConflictError,match='entitlement|policy'):
        _source_adapter(source,denied_after_dispatch).daily_by_date(NOW.date())
    assert calls==['called'] and source.original.remaining(now=NOW)==9


def test_actual_sdk_json_null_is_normalized_only_at_opted_in_adapter(tmp_path: Path,monkeypatch) -> None:
    import json
    import math
    from tushare.pro.client import DataApi
    from rquant.adapter.tushare import TushareAdapter
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    class ActualHTTPResponse:
        text=json.dumps({'code':0,'data':{'fields':['ts_code','trade_date','pe_ttm'],
            'items':[['000001.SZ','20261005',None],['600000.SH','20261005',10.5]]}})
    calls=[]
    def http(*args,**kwargs):
        calls.append('mocked-http')
        return ActualHTTPResponse()
    monkeypatch.setattr('tushare.pro.client.requests.post',http)
    observer=QuotaBoundTransportObserver(path=tmp_path/'quota.sqlite',source='sdk-fixture',quota_units_per_window=3,
        window_kind='day',clock=lambda:NOW)
    adapter=TushareAdapter.__new__(TushareAdapter)
    adapter._pro=DataApi(token='dummy-offline-token')
    adapter._backup_token=''
    adapter.bind_transport_observer(observer)
    with observer.scope(logical_request_id='old-none',observed_at=NOW):
        old=adapter.daily_basic_by_date(NOW.date())
    assert math.isnan(old.loc[0,'pe_ttm'])
    adapter.bind_sdk_null_normalization('tushare-nullable-v1')
    with observer.scope(logical_request_id='opted-in',observed_at=NOW):
        result=adapter.daily_basic_by_date(NOW.date())
    assert result.loc[0,'pe_ttm'] is None and result.loc[1,'pe_ttm']==10.5
    assert result['ts_code'].tolist()==old['ts_code'].tolist() and len(calls)==2


@pytest.mark.parametrize('api,field,value',[('daily','close',float('nan')),('adj_factor','adj_factor',float('nan')),
    ('daily_basic','pe_ttm',float('inf')),('daily_basic','pe_ttm',float('-inf')),('daily_basic','unlisted',float('nan'))])
def test_nullable_sdk_boundary_keeps_critical_and_unlisted_nonfinite_rejection(api: str,field: str,value: float) -> None:
    import pandas as pd
    from rquant.adapter.tushare import normalize_sdk_nullable_response
    with pytest.raises(ValueError,match='missing|infinity'):
        normalize_sdk_nullable_response(api,pd.DataFrame([{field:value}]))


def test_optional_normalization_does_not_change_old_request_bytes_or_identity(tmp_path: Path) -> None:
    import pandas as pd
    from rquant.data_collection_contracts import SourceObservation
    from rquant.runtime_contracts import canonical_sha256
    source,binding,_=_controlled_source(tmp_path)
    old=binding.model_dump(mode='python',exclude={'logical_request_id'})
    assert 'source_normalization_version' not in old and binding.logical_request_id==canonical_sha256(old)
    observation=SourceObservation.from_frame('daily',binding.parameters,pd.DataFrame([{'close':10.0}]),observed_at=NOW)
    assert 'source_normalization_version' not in observation.model_dump()
    opted=binding.model_copy(update={'logical_request_id':None,'source_normalization_version':'tushare-nullable-v1'})
    assert type(binding).model_validate_json(opted.model_dump_json()).logical_request_id!=binding.logical_request_id


def _exact_day_setup(tmp_path: Path,*,prepare: bool=True):
    import hashlib
    import json
    import pandas as pd
    from tests.unit.test_backfill_execute_admission import _state_execution
    from rquant.backfill_execute_contracts import ApiEntitlementEvidence,DataCenterExecutionPolicy,BackfillExecutionSpec,BackfillExecutionIntent
    from rquant.backfill_execute import ControlledMarketAdapter
    from rquant.market_backfill import prepare_market_frames
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.security_status import prefetch_namechange_context,prefetch_security_status_for_date,DailySecurityKey
    from rquant.storage.duckdb import DuckDBStore
    state,spec,manifest,now=_state_execution(tmp_path)
    with DuckDBStore(tmp_path/'primary.duckdb'):
        pass
    source,_,current=_controlled_source(tmp_path)
    entitlements=[]
    for api in ('daily','daily_basic','adj_factor','namechange','stock_st'):
        original=current[0].entitlement_evidence[0]
        entitlement=original.model_copy(update={'api_name':api,'allowed_parameters':('trade_date','start_date','end_date','fields','ts_code'),
            'scope_start':date(2026,1,1),'evidence_path':tmp_path/(api+'-rights.json')})
        data=json.dumps({'kind':'source-entitlement-evidence/v1','proof':entitlement.model_dump(mode='json',
            exclude={'evidence_path','evidence_sha256'})},sort_keys=True,separators=(',',':')).encode()
        entitlement.evidence_path.write_bytes(data)
        entitlement.evidence_path.chmod(0o400)
        entitlements.append(entitlement.model_copy(update={'evidence_sha256':hashlib.sha256(data).hexdigest()}))
    current[0]=DataCenterExecutionPolicy.model_validate_json(current[0].model_copy(update={
        'policy_generation':None,'entitlement_evidence':tuple(entitlements)}).model_dump_json())
    intent=BackfillExecutionIntent.model_validate_json(spec.intent.model_copy(update={
        'intent_id':None,'policy_generation':current[0].policy_generation}).model_dump_json())
    spec=BackfillExecutionSpec.model_validate_json(spec.model_copy(update={'intent':intent}).model_dump_json())
    spec=BackfillExecutionSpec.model_validate_json(spec.model_copy(update={'admission_policy':current[0]}).model_dump_json())
    from rquant.backfill_execute_page_backend import build_backfill_execution_manifest
    manifest=build_backfill_execution_manifest(spec)
    state.persist_maintenance_intent(intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    running=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=1,status='running',now=now)
    claim=state.claim_task(spec.manifest_id,worker_id='actual-fixture',lease_seconds=120,now=now,
        maintenance_execution_id=spec.execution_id)
    class SDK:
        def daily(self,**kwargs):
            return pd.DataFrame([dict(ts_code='600000.SH',trade_date='20261005',open=10.0,high=10.5,low=9.5,
                close=10.0,pre_close=10.0,change=0.0,pct_chg=0.0,vol=100.0,amount=1000.0)])
        def daily_basic(self,**kwargs):
            return pd.DataFrame([dict(ts_code='600000.SH',trade_date='20261005',turnover_rate=1.0,volume_ratio=1.0,
                total_mv=10000.0,circ_mv=8000.0,pe_ttm=10.0,pb=1.0,dv_ttm=None)])
        def adj_factor(self,**kwargs):
            return pd.DataFrame([dict(ts_code='600000.SH',trade_date='20261005',adj_factor=1.0)])
        def namechange(self,**kwargs):
            return pd.DataFrame([dict(ts_code='600000.SH',name='浦发银行',start_date='20260101',
                end_date=None,ann_date='20260101',change_reason='更名')])
        def stock_st(self,**kwargs):
            return pd.DataFrame([dict(ts_code='600001.SH',name='ST样本',trade_date='20261005',
                type='ST',type_name='特别处理')])
    from rquant.adapter.tushare import TushareAdapter
    adapter=TushareAdapter.__new__(TushareAdapter)
    adapter._pro=SDK()
    adapter._backup_token=''
    adapter.bind_transport_observer(source)
    adapter.bind_sdk_null_normalization('tushare-nullable-v1')
    bound=ControlledMarketAdapter(adapter,source,spec=spec,policy=current[0])
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit=current[0].code_commit,
        coverage_start=NOW.date(),coverage_end=NOW.date(),open_dates=(NOW.date(),),generated_at=NOW)
    if not prepare:
        return state,claim,spec,current,None,dict(adapter=adapter,calendar=calendar)
    names=prefetch_namechange_context(bound,start=date(2026,1,1),source_as_of=NOW.date(),request_interval_seconds=0,sleep=lambda _:None)
    frames=prepare_market_frames(bound,NOW.date(),strict_date_scope=True,api_sleep=0,sleep=lambda _:None)
    status=prefetch_security_status_for_date(bound,(DailySecurityKey(ts_code='600000.SH',trade_date=NOW.date()),),
        namechange_context=names,ingested_at=NOW,strict_stock_st_crosscheck=True,request_interval_seconds=0,sleep=lambda _:None)
    arguments=dict(spec=spec,policy=lambda:current[0],control_sequence=running.control_sequence,calendar=calendar,
        frames=frames,status_rows=status.rows,source_requests=tuple(bound.bindings),source_observations=tuple(source.observations),
        dispatch_receipts=source.current_receipts(),clock=lambda:NOW)
    return state,claim,spec,current,frames,arguments


def test_actual_exact_day_preserves_equal_associated_rows_and_commits_original_receipts(tmp_path: Path) -> None:
    from rquant.backfill_execute import commit_exact_backfill_day,verify_backfill_day_receipt
    from rquant.storage.duckdb import DuckDBStore
    state,claim,spec,current,frames,args=_exact_day_setup(tmp_path)
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        store.upsert_adj_factor(frames.adj_factor)
        before=store._conn.execute('SELECT * FROM adj_factor').fetchall()
    receipt=commit_exact_backfill_day(state,claim,**args)
    assert receipt.preserved_rows==1 and receipt.inserted_rows==3
    assert state.get_task(spec.manifest_id,claim.task_id).status=='succeeded'
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as store:
        assert store._conn.execute('SELECT * FROM adj_factor').fetchall()==before
        assert store._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()[0]==1
        assert verify_backfill_day_receipt(store,spec,task_id=claim.task_id,policy=current[0])==receipt


@pytest.mark.parametrize('failure',['conflicting-associated','receipt-write','filled-gap'])
def test_actual_exact_day_refuses_conflict_and_receipt_failure_without_partial_facts(tmp_path: Path,monkeypatch,failure: str) -> None:
    from rquant.backfill_execute import commit_exact_backfill_day
    from rquant.data_collection_authority import CollectionCommitRecorder
    from rquant.storage.duckdb import DuckDBStore
    state,claim,spec,_,frames,args=_exact_day_setup(tmp_path)
    if failure=='conflicting-associated':
        with DuckDBStore(tmp_path/'primary.duckdb') as store:
            store.upsert_adj_factor(frames.adj_factor.assign(adj_factor=2.0))
    elif failure=='filled-gap':
        with DuckDBStore(tmp_path/'primary.duckdb') as store:
            store.upsert_daily(frames.daily)
    else:
        def broken(*args,**kwargs):
            raise OSError('actual receipt write failed')
        monkeypatch.setattr(CollectionCommitRecorder,'record_daily',broken)
    with pytest.raises((OSError,ValueError),match='conflict|receipt|without this execution'):
        commit_exact_backfill_day(state,claim,**args)
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as store:
        assert store.count_daily()==(1 if failure=='filled-gap' else 0)
        assert store._conn.execute('SELECT COUNT(*) FROM backfill_day_commit_receipt').fetchone()[0]==0
        assert store._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()[0]==0


def test_actual_fact_commit_lost_state_ack_restores_old_token_under_new_claim(tmp_path: Path,monkeypatch) -> None:
    from rquant.backfill_execute import commit_exact_backfill_day
    state,claim,spec,_,_,args=_exact_day_setup(tmp_path)
    original=state._succeed_in_transaction
    def lost(*args,**kwargs):
        raise OSError('state completion response lost')
    monkeypatch.setattr(state,'_succeed_in_transaction',lost)
    with pytest.raises(OSError,match='state completion'):
        commit_exact_backfill_day(state,claim,**args)
    assert state.get_task(spec.manifest_id,claim.task_id).status=='running'
    monkeypatch.setattr(state,'_succeed_in_transaction',original)
    later=NOW+timedelta(seconds=121)
    replacement=state.claim_task(spec.manifest_id,worker_id='successor',lease_seconds=120,now=later,
        maintenance_execution_id=spec.execution_id)
    assert replacement.claim_token!=claim.claim_token
    restored=commit_exact_backfill_day(state,replacement,**{**args,'clock':lambda:later,'frames':None,
        'source_requests':(),'source_observations':(),'dispatch_receipts':()})
    assert restored.committed_claim_token==claim.claim_token
    assert state.get_task(spec.manifest_id,replacement.task_id).status=='succeeded'


@pytest.mark.parametrize('change',['price','date','security','status','observation','dispatch','material'])
def test_actual_exact_day_binds_prepared_facts_to_retained_original_sdk_material(tmp_path: Path,change: str) -> None:
    from rquant.backfill_execute import commit_exact_backfill_day
    from rquant.storage.duckdb import DuckDBStore
    state,claim,_,current,frames,args=_exact_day_setup(tmp_path)
    if change=='price':
        frames.daily.loc[0,'close']=12.0
    elif change=='date':
        frames.daily_basic.loc[0,'trade_date']=date(2026,10,6)
    elif change=='security':
        frames.adj_factor.loc[0,'ts_code']='600002.SH'
    elif change=='status':
        args['status_rows']=(args['status_rows'][0].model_copy(update={'is_st':True}),)
    elif change=='observation':
        args['source_observations']=tuple(row for row in args['source_observations'] if row.api_name!='daily')
    elif change=='dispatch':
        args['dispatch_receipts']=tuple(row for row in args['dispatch_receipts'] if row.api_name!='daily_basic')
    else:
        binding=next(row for row in args['source_requests'] if row.api_name=='adj_factor')
        (current[0].source_material_directory/f'{binding.logical_request_id}-1.response.json').unlink()
    with pytest.raises((ValueError,OSError),match='source|scope|material|response|observation|dispatch'):
        commit_exact_backfill_day(state,claim,**args)
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as store:
        assert store.count_daily()==0
        assert store._conn.execute('SELECT COUNT(*) FROM backfill_day_commit_receipt').fetchone()[0]==0


def test_controlled_heartbeat_cannot_revive_expired_original_claim(tmp_path: Path) -> None:
    state,claim,spec,_,_,args=_exact_day_setup(tmp_path)
    def guard(connection,now):
        state.verify_maintenance_claim(connection,claim,execution_id=spec.execution_id,owner=spec.owner,
            expected_sequence=args['control_sequence'],now=now)
    renewed=state.renew_task_claim(claim,lease_seconds=120,now=NOW+timedelta(seconds=40),guard=guard)
    assert renewed.lease_expires_at==NOW+timedelta(seconds=160)
    with pytest.raises(StaleTaskClaimError,match='expired'):
        state.renew_task_claim(claim,lease_seconds=120,now=NOW+timedelta(seconds=161),guard=guard)


def _worker_setup(tmp_path: Path):
    from rquant.backfill_execute import BackfillExecutionWorker
    from rquant.adapter.tushare import TushareAdapter
    state,claim,spec,current,_,args=_exact_day_setup(tmp_path)
    state.release_task_claim(claim,now=NOW)
    calls=[]
    class SDK:
        def __getattr__(self,name):
            def forbidden(**kwargs):
                calls.append(name)
                raise AssertionError('durable response should prevent another actual SDK dispatch')
            return forbidden
    def adapter_factory(observer):
        adapter=TushareAdapter.__new__(TushareAdapter)
        adapter._pro=SDK()
        adapter._backup_token=''
        adapter.bind_transport_observer(observer)
        adapter.bind_sdk_null_normalization('tushare-nullable-v1')
        return adapter
    worker=BackfillExecutionWorker(state,policy=lambda:current[0],adapter_factory=adapter_factory,
        calendar=lambda value:args['calendar'],clock=lambda:NOW)
    return state,spec,current,worker,calls


def test_original_worker_restores_prepared_sdk_material_and_commits_one_exact_day(tmp_path: Path) -> None:
    state,spec,_,worker,calls=_worker_setup(tmp_path)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='day_committed' and result.task_id=='day-2026-10-05'
    assert calls==[] and state.get_task(spec.manifest_id,result.task_id).status=='succeeded'
    assert state.get_task(spec.manifest_id,'tail-derived').status=='pending'


def test_original_worker_stamps_status_after_actual_source_observation(tmp_path: Path) -> None:
    from rquant.backfill_execute import BackfillExecutionWorker,verify_backfill_day_receipt
    from rquant.storage.duckdb import DuckDBStore
    state,claim,spec,current,_,prepared=_exact_day_setup(tmp_path,prepare=False)
    state.release_task_claim(claim,now=NOW)
    ticks=[0]
    def clock() -> datetime:
        ticks[0]+=1
        return NOW+timedelta(microseconds=ticks[0])
    def adapter_factory(observer):
        adapter=prepared['adapter']
        adapter.bind_transport_observer(observer)
        return adapter
    worker=BackfillExecutionWorker(state,policy=lambda:current[0],adapter_factory=adapter_factory,
        calendar=lambda _:prepared['calendar'],clock=clock)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='day_committed',result
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as store:
        receipt=verify_backfill_day_receipt(store,spec,task_id='day-2026-10-05',policy=current[0])
        observed=max(row.observed_at for row in receipt.source_observations
            if row.api_name in {'namechange','stock_st'})
        ingested=store._conn.execute('SELECT ingested_at FROM stock_status_daily').fetchone()[0]
        assert ingested>=observed
    assert state.get_task(spec.manifest_id,'day-2026-10-05').status=='succeeded'


def test_original_worker_applies_pending_pause_before_claim_or_sdk(tmp_path: Path) -> None:
    state,spec,_,worker,calls=_worker_setup(tmp_path)
    current=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='pause-before-dispatch',
        expected_sequence=current.control_sequence,action='pause',now=NOW)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='paused' and result.execution.pause_applied and calls==[]
    assert state.get_task(spec.manifest_id,'day-2026-10-05').status=='pending'


def test_original_worker_does_not_take_new_task_after_shanghai_stop_boundary(tmp_path: Path) -> None:
    state,spec,_,worker,calls=_worker_setup(tmp_path)
    worker.clock=lambda:datetime(2026,10,6,0,20,tzinfo=UTC)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='paused' and result.execution.failure_code=='maintenance_window_closed' and calls==[]
    assert state.get_task(spec.manifest_id,'day-2026-10-05').status=='pending'


@pytest.mark.parametrize('expired',[False,True])
def test_pause_waits_for_live_original_claim_and_releases_only_expired_claim(tmp_path: Path,expired: bool) -> None:
    state,spec,_,worker,calls=_worker_setup(tmp_path)
    held=state.claim_task(spec.manifest_id,worker_id='original-worker',lease_seconds=120,now=NOW,
        maintenance_execution_id=spec.execution_id)
    current=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='request-pause',
        expected_sequence=current.control_sequence,action='pause',now=NOW)
    if expired:
        worker.clock=lambda:NOW+timedelta(seconds=121)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert calls==[] and result.execution.pause_applied==expired
    if expired:
        assert result.outcome=='paused' and state.get_task(spec.manifest_id,held.task_id).status=='pending'
    else:
        assert result.outcome=='idle' and state.get_task(spec.manifest_id,held.task_id).claim_token==held.claim_token


def test_original_worker_recovers_actual_success_material_with_exhausted_quota_without_sdk(tmp_path: Path) -> None:
    import pandas as pd
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    state,spec,current,worker,calls=_worker_setup(tmp_path)
    observer=QuotaBoundTransportObserver(path=current[0].quota_ledger_path,source=current[0].quota_source,
        quota_units_per_window=current[0].quota_units_per_window,window_kind='day',clock=lambda:NOW)
    for index in range(5):
        with observer.scope(logical_request_id=f'other-original-operation-{index}',observed_at=NOW):
            observer.observe('daily',lambda:pd.DataFrame())
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='day_committed' and calls==[]
    assert state.get_task(spec.manifest_id,'day-2026-10-05').status=='succeeded'


def test_original_worker_runs_original_derived_tail_without_extra_sdk_or_completion_claim(tmp_path: Path) -> None:
    state,spec,_,worker,calls=_worker_setup(tmp_path)
    assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='day_committed'
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='derived_committed' and calls==[]
    assert state.get_task(spec.manifest_id,'tail-derived').status=='succeeded'
    assert state.get_task(spec.manifest_id,'verify-completion').status=='pending'


def test_original_worker_releases_claim_after_actual_primary_gate_busy(tmp_path: Path) -> None:
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    state,spec,current,worker,calls=_worker_setup(tmp_path)
    with PrimaryWriterGate(current[0].primary_writer_gate).acquire():
        result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.outcome=='idle' and calls==[]
    assert state.get_task(spec.manifest_id,'day-2026-10-05').status=='pending'
