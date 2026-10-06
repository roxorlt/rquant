from __future__ import annotations

import importlib.util
import inspect
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import copyfile

import pandas as pd
import pytest

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import load_data_audit_report, data_audit_report_path
from rquant.data_audit_report_jobs import DataAuditReportJobStore, DataAuditReportJobWorker
from rquant.data_collection_authority import CollectionCommitRecorder
from rquant.data_collection_contracts import CollectionRecorderConfig, CollectionReceiptReference, SourceObservation
from rquant.replica_generation import capture_database_watermark, replica_generation_path, write_replica_generation_metadata
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.primary_writer_gate import PrimaryWriterGate, PrimaryWriterGateConfig
from rquant.trade_calendar import TradeCalendarDay

DAY=date(2026,10,5)
NOW=datetime(2026,10,5,10,tzinfo=UTC)


@pytest.mark.parametrize('case',['repaired','changed_source','exhausted','active','changed_binding'])
def test_original_failed_collection_job_retries_same_bound_request_with_bounded_attempts(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    import rquant.data_audit_report_jobs as job_module
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    admitted=bridge.run_one()
    original=job_module.create_and_publish_data_audit_report
    monkeypatch.setattr(job_module,'create_and_publish_data_audit_report',
        lambda **kwargs:(_ for _ in ()).throw(OSError('original report publication I/O failed')))
    failed=DataAuditReportJobWorker(jobs).run_one()
    assert failed.task_id==admitted.task_id and failed.status=='failed'
    with pytest.raises(ValueError,match='explicit'):
        jobs.retry_failed(failed.task_id,expected_collection_reference=reference)
    before=jobs.admission_by_key(reference.event_id)
    monkeypatch.setattr(job_module,'create_and_publish_data_audit_report',original)
    if case=='changed_binding':
        with pytest.raises(ValueError,match='binding'):
            jobs.retry_failed(failed.task_id,expected_collection_reference=reference.model_copy(update={'event_id':'f'*64}),maintenance_recovery=True)
        assert jobs.status(failed.task_id).status=='failed' and jobs.admission_by_key(reference.event_id)==before
        return
    if case=='active':
        from datetime import timedelta
        from rquant.data_audit_report import capture_data_audit_replica_identity
        jobs.clock=lambda:NOW+timedelta(minutes=10)
        request=before[0]
        other=jobs.submit(request.model_copy(update={'idempotency_key':'other-active-audit-request','collection_reference':None,
            'replica_file_identity':capture_data_audit_replica_identity(request.primary_path,request.replica_path)}))
        with pytest.raises(ValueError,match='active'):
            jobs.retry_failed(failed.task_id,expected_collection_reference=reference,maintenance_recovery=True)
        assert jobs.status(failed.task_id).status=='failed' and jobs.status(other.task_id).status=='queued'
        return
    if case=='exhausted':
        with jobs._transaction() as connection:
            connection.execute('UPDATE data_audit_report_job SET attempts=6 WHERE task_id=?',[failed.task_id])
        with pytest.raises(ValueError,match='attempt'):
            jobs.retry_failed(failed.task_id,expected_collection_reference=reference,maintenance_recovery=True)
        assert jobs.status(failed.task_id).status=='failed'
        return
    queued=jobs.retry_failed(failed.task_id,expected_collection_reference=reference,maintenance_recovery=True)
    assert queued.task_id==failed.task_id and queued.status=='queued' and queued.attempts==1
    assert jobs.retry_failed(failed.task_id,expected_collection_reference=reference,maintenance_recovery=True)==queued
    assert jobs.admission_by_key(reference.event_id)==before
    if case=='changed_source':
        before[0].replica_path.write_bytes(b'changed original source')
    recovered=DataAuditReportJobWorker(jobs).run_one()
    assert recovered.task_id==failed.task_id and recovered.attempts==2
    assert recovered.status==('failed' if case=='changed_source' else 'succeeded')
    assert bridge.lookup(reference).task_id==failed.task_id
    with jobs._transaction() as connection:
        assert connection.execute('SELECT COUNT(*) FROM data_audit_report_job').fetchone()[0]==1
    if case=='repaired':
        report=load_data_audit_report(data_audit_report_path(jobs.report_directory,recovered.report_hash))
        assert report.collection_reference==reference and report.schema_version==3


def test_original_finite_runner_bridges_only_explicit_profile_and_processes_one_source(tmp_path: Path) -> None:
    from rquant.data_audit_report_runner import main
    from rquant.data_center_maintenance_runtime import DataCenterRuntimeProfile,DataCenterMaintenanceRuntimeConfig
    _,replica,reference,jobs,bridge,*_=_chain(tmp_path)
    from tests.unit.test_backfill_execute import _controlled_source
    _,_,policies=_controlled_source(tmp_path)
    policy_path=tmp_path/'runner-policy.json'
    policy_path.write_text(policies[0].model_dump_json());policy_path.chmod(0o600)
    profile=DataCenterRuntimeProfile(policy_path=policy_path,maintenance=DataCenterMaintenanceRuntimeConfig(
        replica_path=replica,audit_state_path=jobs.state_path,audit_directory=jobs.report_directory,collection_directory=jobs.collection_directory),
        plan_state_path=tmp_path/'plans.sqlite3',plan_directory=tmp_path/'plans',allowed_owners=('test-owner',))
    path=tmp_path/'profile.json';path.write_text(profile.model_dump_json());path.chmod(0o600)
    arguments=['--state-path',str(jobs.state_path),'--report-directory',str(jobs.report_directory),'--once']
    assert main(arguments)==0 and bridge.lookup(reference) is None
    assert main([*arguments,'--runtime-profile',str(path),'--bridge'])==0
    result=bridge.lookup(reference)
    assert result is not None and result.status=='succeeded'
    assert main([*arguments,'--runtime-profile',str(path),'--bridge'])==0
    assert bridge.lookup(reference).task_id==result.task_id


def _chain(tmp_path: Path,*,first_day: date=DAY) -> tuple:
    assert importlib.util.find_spec('rquant.data_collection_bridge') is not None
    from rquant.data_collection_bridge import DataCollectionBridge
    from rquant.data_collection_authority import seal_data_collection_proof
    primary=tmp_path/'primary.duckdb'
    replica=tmp_path/'replica.duckdb'
    dates=tuple(first_day+timedelta(days=i) for i in range((DAY-first_day).days+1))
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit='a'*40,
        coverage_start=first_day,coverage_end=DAY,open_dates=dates,generated_at=NOW)
    recorder=CollectionCommitRecorder(CollectionRecorderConfig(collector_id='legacy_daily',run_id='original-1',
        owner='test-owner',code_commit='a'*40,source_generation_id='b'*64,calendar=calendar),clock=lambda:NOW)
    with DuckDBStore(primary) as store:
        store.upsert_trade_calendar(tuple(TradeCalendarDay(exchange='SSE',cal_date=day,is_open=True,
            pretrade_date=date(2026,9,30) if first_day==DAY else day-timedelta(days=1),updated_at=NOW) for day in dates))
        frame=pd.DataFrame([dict(ts_code='600000.SH',trade_date=first_day,open=10.0,high=10.5,
            low=9.5,close=10.0,pre_close=10.0,change=0.0,pct_chg=0.0,vol=100.0,amount=1000.0)])
        store._conn.execute('BEGIN')
        store.upsert_daily(frame)
        observations=(SourceObservation.from_frame('daily',{'trade_date':first_day.strftime('%Y%m%d')},frame,observed_at=NOW),)
        if first_day!=DAY:
            basic=pd.DataFrame([dict(ts_code='600000.SH',trade_date=first_day,turnover_rate=1.0,volume_ratio=1.0,
                total_mv=10000.0,circ_mv=8000.0)])
            factors=pd.DataFrame([dict(ts_code='600000.SH',trade_date=first_day,adj_factor=1.0)])
            store.upsert_daily_basic(basic)
            store.upsert_adj_factor(factors)
            observations+=tuple(SourceObservation.from_frame(api,{'trade_date':first_day.strftime('%Y%m%d')},response,
                observed_at=NOW) for api,response in (('daily_basic',basic),('adj_factor',factors)))
            from rquant.data_collection_authority import ObservedSecurityClient,CollectionObservationBatch
            from rquant.security_status import DailySecurityKey,prefetch_namechange_context,prefetch_security_status_for_date
            class Source:
                def namechange_raw(self,*args,**kw):
                    return pd.DataFrame([dict(ts_code='600000.SH',name='浦发银行',start_date='20260101',end_date=None,
                        ann_date='20260101',change_reason='更名')])
                def stock_st_raw(self,day):
                    return pd.DataFrame([dict(ts_code='600001.SH',name='ST样本',trade_date=day.strftime('%Y%m%d'),type='ST',type_name='特别处理')])
            batch=CollectionObservationBatch(NOW)
            observed=ObservedSecurityClient(Source(),batch)
            names=prefetch_namechange_context(observed,start=date(2026,1,1),source_as_of=DAY,request_interval_seconds=0,sleep=lambda _:None)
            status=prefetch_security_status_for_date(observed,(DailySecurityKey(ts_code='600000.SH',trade_date=first_day),),
                namechange_context=names,ingested_at=NOW,strict_stock_st_crosscheck=True,request_interval_seconds=0,sleep=lambda _:None)
            store.upsert_stock_status(status.rows,transaction_mode='existing',require_daily_keys=True)
            observations+=tuple(batch.observations)
        receipt=recorder.record_daily(store,first_day,observations=observations)
        store._conn.execute('COMMIT')
        if first_day!=DAY:
            from rquant.market_backfill import recompute_daily_state
            recompute_daily_state(store,start_date=first_day,status_mode='verified_no_fetch')
    watermark=capture_database_watermark(primary)
    copyfile(primary,replica)
    write_replica_generation_metadata(primary_path=primary,replica_path=replica,
        output_path=replica_generation_path(replica),source_before=watermark)
    lock=tmp_path/'primary.lock'
    lock.touch(mode=0o600)
    gate=PrimaryWriterGate(PrimaryWriterGateConfig.capture(primary_path=primary,lock_path=lock))
    jobs=DataAuditReportJobStore(state_path=tmp_path/'audit.sqlite',report_directory=tmp_path/'reports',
        collection_directory=tmp_path/'collection',clock=lambda:NOW)
    with gate.acquire() as lease:
        reference=seal_data_collection_proof(jobs,primary_path=primary,replica_path=replica,calendar=calendar,
            references=(CollectionReceiptReference(kind='ingestion',receipt_id=receipt.receipt_id,
                dataset_ids=('daily_bar',)),),audit_start=first_day,observed_through=DAY,
            primary_writer_lease=lease,clock=lambda:NOW)
    bridge=DataCollectionBridge(jobs,null_fields=(DailyBarNullFieldSpec(field_name='close',max_null_numerator=0,max_null_denominator=1),))
    return primary,replica,reference,jobs,bridge,gate,calendar,receipt


def test_real_commit_proof_uses_original_job_and_v3_report(tmp_path: Path) -> None:
    primary,replica,reference,jobs,bridge,*_= _chain(tmp_path)
    accepted=bridge.run_one()
    assert accepted.status=='queued'
    assert bridge.run_one() is None
    succeeded=DataAuditReportJobWorker(jobs).run_one()
    assert succeeded.status=='succeeded'
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,succeeded.report_hash))
    assert report.schema_version==3
    assert report.collection_reference==reference
    evidence={item.dataset_id:item for item in report.collection_datasets}
    assert evidence['daily_bar'].status=='partial'
    assert evidence['minute_bar'].status=='unconfirmed'
    assert report.collection_completed_through is None
    assert report.coverage_conclusion=='unconfirmed'
    assert jobs.status(accepted.task_id)==succeeded


def test_lost_acceptance_recovers_original_key_and_pinned_inode(tmp_path: Path) -> None:
    primary,replica,reference,jobs,bridge,*_= _chain(tmp_path)
    accepted=bridge.run_one()
    copyfile(primary,tmp_path/'new.duckdb')
    (tmp_path/'new.duckdb').replace(replica)
    assert bridge.lookup(reference)==accepted
    assert DataAuditReportJobWorker(jobs).run_one().status=='succeeded'


def test_source_bytes_change_rejects_verified_completion(tmp_path: Path) -> None:
    primary,replica,reference,jobs,bridge,*_= _chain(tmp_path)
    from rquant.data_collection_authority import load_collection_proof
    proof=load_collection_proof(jobs.collection_directory,reference)
    with DuckDBStore(proof.fixed_replica_path) as store:
        store._conn.execute("UPDATE daily_bar SET close=11 WHERE trade_date=?",[DAY])
    with pytest.raises(ValueError):
        bridge.run_one()


def test_old_request_defaults_remain_exact_bytes() -> None:
    from rquant.data_audit_report_jobs import DataAuditReportJobRequest, _canonical_request
    assert 'collection_reference' in DataAuditReportJobRequest.model_fields
    assert DataAuditReportJobRequest.model_fields['collection_reference'].default is None
    assert 'collection_directory' in inspect.signature(DataAuditReportJobStore).parameters


def test_original_security_inventory_is_bound_and_old_none_keeps_its_exact_payload(tmp_path: Path) -> None:
    import json
    from rquant.data_collection_authority import load_collection_proof,verify_collection_source
    from rquant.data_collection_contracts import DataCollectionProofV2
    _,_,reference,jobs,*_=_chain(tmp_path)
    proof=load_collection_proof(jobs.collection_directory,reference)
    assert proof.available_securities==()
    payload=proof.model_dump(mode='python',exclude={'binding_sha256','available_securities'})
    old=DataCollectionProofV2.model_validate(payload)
    assert 'available_securities' not in json.loads(old.model_dump_json())
    assert DataCollectionProofV2.model_validate_json(old.model_dump_json())==old
    declared={**payload,'available_securities':('600000.SH',)}
    bound=DataCollectionProofV2.model_validate(declared)
    assert bound.binding_sha256!=old.binding_sha256
    with DuckDBStore(proof.fixed_replica_path,read_only=True) as reader:
        with pytest.raises(ValueError,match='inventory'):
            verify_collection_source(reader._conn,bound)
        verify_collection_source(reader._conn,old)
    for inventory in (('600000.SH','600000.SH'),('600000.SH','000001.SZ'),('invalid',),
            tuple(f'{number:06d}.SH' for number in range(1,8002))):
        with pytest.raises(ValueError):
            DataCollectionProofV2.model_validate({**payload,'available_securities':inventory})


def test_changed_event_binding_cannot_replace_original_admission(tmp_path: Path) -> None:
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    original=bridge.run_one()
    changed=reference.model_copy(update={'binding_sha256':'f'*64})
    with pytest.raises(ValueError,match='conflicting'):
        bridge.lookup(changed)
    assert bridge.lookup(reference)==original


def test_source_scan_occurs_outside_original_write_transaction(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager
    import rquant.data_collection_bridge as module
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    original_transaction=jobs._transaction
    active=False
    @contextmanager
    def observed_transaction():
        nonlocal active
        with original_transaction() as connection:
            active=True
            try:
                yield connection
            finally:
                active=False
    original_capture=module.capture_verified_collection_identity
    def capture(*args: object,**kwargs: object):
        assert not active
        return original_capture(*args,**kwargs)
    monkeypatch.setattr(jobs,'_transaction',observed_transaction)
    monkeypatch.setattr(module,'capture_verified_collection_identity',capture)
    accepted=bridge.run_one()
    assert accepted.status=='queued'
    with pytest.raises(TypeError):
        jobs.submit(jobs.admission_by_key(reference.event_id)[0],verified_collection_file=object())


def test_mutation_after_scan_rolls_back_original_job_and_cursor(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.data_collection_authority import load_collection_proof
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    pin=load_collection_proof(jobs.collection_directory,reference).fixed_replica_path
    original_submit=jobs._submit_on
    def mutated_submit(connection: object,request: object,**kwargs: object) -> str:
        with pin.open('r+b') as handle:
            handle.seek(-1,2)
            handle.write(b'X')
        return original_submit(connection,request,**kwargs)
    monkeypatch.setattr(jobs,'_submit_on',mutated_submit)
    with pytest.raises(ValueError,match='identity changed'):
        bridge.run_one()
    assert jobs.admission_by_key(reference.event_id) is None
    with jobs._transaction() as connection:
        assert connection.execute('SELECT COUNT(*) FROM data_collection_cursor').fetchone()[0]==0


def test_admitted_source_mutation_cannot_produce_verified_success(tmp_path: Path) -> None:
    from rquant.data_collection_authority import load_collection_proof
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    bridge.run_one()
    pin=load_collection_proof(jobs.collection_directory,reference).fixed_replica_path
    with pin.open('r+b') as handle:
        handle.seek(-1,2)
        handle.write(b'X')
    result=DataAuditReportJobWorker(jobs).run_one()
    assert result.status=='failed'
    assert result.report_hash is None


def test_source_scan_honors_deadline_and_stop(tmp_path: Path) -> None:
    from datetime import timedelta
    from rquant.data_collection_authority import load_collection_proof,capture_verified_collection_identity
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    proof=load_collection_proof(jobs.collection_directory,reference)
    with pytest.raises(InterruptedError):
        capture_verified_collection_identity(proof,reference,deadline=datetime.now(UTC)-timedelta(seconds=1))
    with pytest.raises(InterruptedError):
        capture_verified_collection_identity(proof,reference,deadline=datetime.now(UTC)+timedelta(minutes=1),
            stop_requested=lambda:True)


def test_source_sealing_full_scan_occurs_outside_audit_write_transaction(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager
    from rquant import data_collection_authority as module
    primary,replica,reference,jobs,_,gate,calendar,receipt=_chain(tmp_path)
    active=False
    original_transaction=jobs._transaction
    @contextmanager
    def observed_transaction():
        nonlocal active
        with original_transaction() as connection:
            active=True
            try:
                yield connection
            finally:
                active=False
    original_hash=module._file_sha
    def bounded_hash(*args,**kwargs):
        assert not active
        return original_hash(*args,**kwargs)
    monkeypatch.setattr(jobs,'_transaction',observed_transaction)
    monkeypatch.setattr(module,'_file_sha',bounded_hash)
    with gate.acquire() as lease:
        assert module.seal_data_collection_proof(jobs,primary_path=primary,replica_path=replica,calendar=calendar,
            references=(CollectionReceiptReference(kind='ingestion',receipt_id=receipt.receipt_id,dataset_ids=('daily_bar',)),),
            audit_start=DAY,observed_through=DAY,primary_writer_lease=lease,clock=lambda:NOW)==reference


def test_only_readable_success_with_no_original_claim_releases_pin_and_keeps_history(tmp_path: Path) -> None:
    from rquant.data_collection_authority import load_collection_proof,release_completed_collection_pins
    _,_,reference,jobs,bridge,*_=_chain(tmp_path)
    proof=load_collection_proof(jobs.collection_directory,reference)
    accepted=bridge.run_one()
    assert release_completed_collection_pins(jobs)==0 and proof.fixed_replica_path.exists()
    succeeded=DataAuditReportJobWorker(jobs).run_one()
    path=data_audit_report_path(jobs.report_directory,succeeded.report_hash)
    payload=path.read_bytes()
    path.unlink()
    assert release_completed_collection_pins(jobs)==0 and proof.fixed_replica_path.exists()
    path.write_bytes(payload)
    with jobs._transaction() as connection:
        connection.execute('UPDATE data_audit_report_job SET lease_token=? WHERE task_id=?',('unexpected-live-claim',accepted.task_id))
    assert release_completed_collection_pins(jobs)==0 and proof.fixed_replica_path.exists()
    with jobs._transaction() as connection:
        connection.execute('UPDATE data_audit_report_job SET lease_token=NULL WHERE task_id=?',(accepted.task_id,))
    assert release_completed_collection_pins(jobs)==1
    assert not proof.fixed_replica_path.exists()
    assert load_collection_proof(jobs.collection_directory,reference)==proof
    assert bridge.lookup(reference)==succeeded


def test_unknown_and_failed_pins_count_towards_capacity(tmp_path: Path) -> None:
    from rquant import data_collection_authority as module
    primary,replica,_,jobs,bridge,gate,calendar,receipt=_chain(tmp_path)
    bridge.run_one()
    with jobs._transaction() as connection:
        connection.execute("UPDATE data_audit_report_job SET status='failed',error_code='internal_error' WHERE status='queued'")
    (jobs.collection_directory/('pin-'+'f'*64+'.duckdb')).write_bytes(b'unknown incomplete source')
    # A different legitimate collector identity is a distinct finite source event.
    with DuckDBStore(primary) as store:
        updated=CollectionCommitRecorder(CollectionRecorderConfig(collector_id='legacy_daily',run_id='original-2',
            owner='test-owner',code_commit='a'*40,source_generation_id='b'*64,calendar=calendar),clock=lambda:NOW)
        store._conn.execute('BEGIN')
        second=updated.record_daily(store,DAY,observations=receipt.observations)
        store._conn.execute('COMMIT')
    watermark=capture_database_watermark(primary)
    copyfile(primary,tmp_path/'new.duckdb')
    (tmp_path/'new.duckdb').replace(replica)
    write_replica_generation_metadata(primary_path=primary,replica_path=replica,output_path=replica_generation_path(replica),source_before=watermark)
    with gate.acquire() as lease,pytest.raises(ValueError,match='capacity'):
        module.seal_data_collection_proof(jobs,primary_path=primary,replica_path=replica,calendar=calendar,
            references=(CollectionReceiptReference(kind='ingestion',receipt_id=second.receipt_id,dataset_ids=('daily_bar',)),),
            audit_start=DAY,observed_through=DAY,primary_writer_lease=lease,clock=lambda:NOW)
