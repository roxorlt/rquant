from __future__ import annotations

import importlib.util
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay

if TYPE_CHECKING:
    from rquant.data_audit_report_jobs import DataAuditReportJobStore
    from rquant.data_center_maintenance_runtime import MaintenanceDomainEvidence
    from rquant.backfill_execute import BackfillExecutionWorker
    from rquant.backfill_execute_contracts import DataCenterExecutionPolicy
    from rquant.data_collection_contracts import AuditCollectionReference

DAY = date(2026,10,5)
NOW = datetime(2026,10,5,10,tzinfo=UTC)


def test_complete_manifest_admission_does_not_publish_material_when_two_unknown_pins_fill_capacity(tmp_path: Path,monkeypatch) -> None:
    from tests.unit.test_financial_runtime import _financial_runtime_setup
    from rquant.data_center_maintenance_runtime import _maintenance_domain
    from rquant.data_collection_authority import seal_data_collection_proof
    from rquant.data_audit_report_jobs import DataAuditReportJobStore
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    state,spec,current,_,worker,_=_financial_runtime_setup(tmp_path,monkeypatch)
    assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='query_committed'
    assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='fundamentals_committed'
    jobs=DataAuditReportJobStore(state_path=tmp_path/'audit.sqlite',report_directory=tmp_path/'reports',
        collection_directory=tmp_path/'collection',clock=worker.clock)
    for name in ('a','b'):
        (jobs.collection_directory/f'pin-{name*64}.duckdb').write_bytes(b'unknown-material')
    before=set(jobs.collection_directory.iterdir())
    with PrimaryWriterGate(current[0].primary_writer_gate).acquire() as lease:
        with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
            domain=_maintenance_domain(state,reader,spec,policy=current[0],clock=worker.clock)
        with pytest.raises(ValueError,match='capacity'):
            seal_data_collection_proof(jobs,primary_path=tmp_path/'primary.duckdb',replica_path=tmp_path/'replica.duckdb',
                calendar=domain.calendar,references=domain.references,audit_start=domain.audit_start,observed_through=domain.observed_through,
                primary_writer_lease=lease,clock=worker.clock,receipt_manifest=domain.receipt_manifest,receipt_pages=domain.receipt_pages)
    assert set(jobs.collection_directory.iterdir())==before
    assert all(path.read_bytes()==b'unknown-material' for path in before)


def _complete_financial_domain(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> tuple:
    from tests.unit.test_financial_runtime import _financial_runtime_setup
    from rquant.data_center_maintenance_runtime import _maintenance_domain
    from rquant.data_audit_report_jobs import DataAuditReportJobStore
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    state,spec,current,_,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='query_committed'
    assert worker.run_one(spec.execution_id,owner=spec.owner).outcome=='fundamentals_committed'
    jobs=DataAuditReportJobStore(state_path=tmp_path/'audit.sqlite',report_directory=tmp_path/'reports',
        collection_directory=tmp_path/'collection',clock=worker.clock)
    from rquant.research_sync import refresh_readonly_replica
    with PrimaryWriterGate(current[0].primary_writer_gate).acquire() as lease:
        with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
            domain=_maintenance_domain(state,reader,spec,policy=current[0],clock=worker.clock)
        ok,detail=refresh_readonly_replica(tmp_path/'primary.duckdb',tmp_path/'replica.duckdb',primary_writer_lease=lease)
        assert ok,detail
    return jobs,domain,current[0],worker,calls


def _seal_complete_financial_domain(tmp_path: Path,jobs: DataAuditReportJobStore,domain: MaintenanceDomainEvidence,
        policy: DataCenterExecutionPolicy,worker: BackfillExecutionWorker) -> AuditCollectionReference:
    from rquant.data_collection_authority import seal_data_collection_proof
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    with PrimaryWriterGate(policy.primary_writer_gate).acquire() as lease:
        return seal_data_collection_proof(jobs,primary_path=tmp_path/'primary.duckdb',replica_path=tmp_path/'replica.duckdb',
            calendar=domain.calendar,references=domain.references,audit_start=domain.audit_start,
            observed_through=domain.observed_through,primary_writer_lease=lease,clock=worker.clock,
            receipt_manifest=domain.receipt_manifest,receipt_pages=domain.receipt_pages)


@pytest.mark.parametrize('unknown_material',[False,True])
def test_complete_manifest_failed_publication_reconciles_only_owned_material(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,
        unknown_material: bool) -> None:
    import rquant.data_collection_manifest as material
    jobs,domain,policy,worker,calls=_complete_financial_domain(tmp_path,monkeypatch)
    observed_calls=tuple(calls)
    original=material._write_once
    def interrupted(path: Path,payload: bytes) -> None:
        original(path,payload)
        if unknown_material:
            (path.parent/'unknown.json').write_bytes(b'unknown')
        raise OSError('publication response lost')
    monkeypatch.setattr(material,'_write_once',interrupted)
    with pytest.raises((OSError,ValueError),match='publication response|unknown material'):
        _seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker)
    with jobs._connect() as connection:
        assert tuple(connection.execute('SELECT COUNT(*) FROM data_collection_source').fetchone())==(0,)
    pins=tuple(jobs.collection_directory.glob('pin-*.duckdb'))
    manifests=tuple(jobs.collection_directory.glob('receipts-*'))
    if unknown_material:
        assert len(pins)==len(manifests)==1
        assert (manifests[0]/'unknown.json').read_bytes()==b'unknown'
        with pytest.raises(ValueError,match='unknown collection pin'):
            _seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker)
    else:
        assert tuple(jobs.collection_directory.iterdir())==()
        monkeypatch.setattr(material,'_write_once',original)
        assert _seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker).sequence==1
    assert tuple(calls)==observed_calls


def test_complete_manifest_lost_commit_response_recovers_exact_existing_material_without_sdk(tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager
    from rquant.data_collection_contracts import AuditCollectionReference
    import rquant.data_collection_manifest as material
    jobs,domain,policy,worker,calls=_complete_financial_domain(tmp_path,monkeypatch)
    observed_calls=tuple(calls)
    original=jobs._transaction
    lost=[False]
    @contextmanager
    def lost_response() -> Iterator[object]:
        with original() as connection:
            yield connection
        if not lost[0]:
            lost[0]=True
            raise OSError('original SQLite commit response lost')
    monkeypatch.setattr(jobs,'_transaction',lost_response)
    with pytest.raises(OSError,match='commit response lost'):
        _seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker)
    with jobs._connect() as connection:
        rows=connection.execute('SELECT reference_json FROM data_collection_source').fetchall()
    assert len(rows)==1
    original_reference=AuditCollectionReference.model_validate_json(rows[0][0])
    before={path:path.read_bytes() for path in jobs.collection_directory.rglob('*') if path.is_file()}
    def forbidden_publication(*args: object,**kwargs: object) -> None:
        pytest.fail('accepted immutable material must never be published again')
    monkeypatch.setattr(material,'publish_receipt_manifest',forbidden_publication)
    recovered=_seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker)
    assert recovered==original_reference
    assert {path:path.read_bytes() for path in jobs.collection_directory.rglob('*') if path.is_file()}==before
    assert tuple(calls)==observed_calls


def test_complete_manifest_accepted_missing_page_is_refused_without_recreation(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.data_collection_manifest as material
    jobs,domain,policy,worker,calls=_complete_financial_domain(tmp_path,monkeypatch)
    observed_calls=tuple(calls)
    accepted=_seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker)
    directory=material.receipt_manifest_directory(jobs.collection_directory,domain.receipt_manifest)
    missing=directory/domain.receipt_manifest.pages[0].relative_name
    missing.unlink()
    before={path:path.read_bytes() for path in jobs.collection_directory.rglob('*') if path.is_file()}
    def forbidden_publication(*args: object,**kwargs: object) -> None:
        pytest.fail('missing accepted page must not be recreated')
    monkeypatch.setattr(material,'publish_receipt_manifest',forbidden_publication)
    with pytest.raises(ValueError,match='missing artifacts'):
        _seal_complete_financial_domain(tmp_path,jobs,domain,policy,worker)
    assert not missing.exists()
    assert {path:path.read_bytes() for path in jobs.collection_directory.rglob('*') if path.is_file()}==before
    with jobs._connect() as connection:
        assert tuple(connection.execute('SELECT reference_json FROM data_collection_source').fetchone())==(accepted.model_dump_json(),)
    assert tuple(calls)==observed_calls


def _recorder():
    assert importlib.util.find_spec('rquant.data_collection_authority') is not None
    from rquant.data_collection_authority import CollectionCommitRecorder
    from rquant.data_collection_contracts import CollectionRecorderConfig
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit='a'*40,
        coverage_start=DAY,coverage_end=DAY,open_dates=(DAY,),generated_at=NOW)
    return CollectionCommitRecorder(CollectionRecorderConfig(collector_id='legacy_daily',
        run_id='original-ingestion-1',owner='test-owner',code_commit='a'*40,
        source_generation_id='b'*64,calendar=calendar),clock=lambda: NOW)


def _seed(store: DuckDBStore) -> None:
    store.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=DAY,
        is_open=True,pretrade_date=date(2026,9,30),updated_at=NOW),))


def test_fact_and_completion_receipt_share_original_transaction(tmp_path: Path) -> None:
    recorder=_recorder()
    from rquant.data_collection_contracts import SourceObservation
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        _seed(store)
        frame=pd.DataFrame([{'ts_code':'600000.SH','trade_date':DAY,'open':10.0,'high':10.5,
            'low':9.5,'close':10.0,'pre_close':10.0,'change':0.0,'pct_chg':0.0,'vol':100.0,'amount':1000.0}])
        observation=SourceObservation.from_frame('daily',{'trade_date':'20261005'},frame,observed_at=NOW)
        store._conn.execute('BEGIN')
        store.upsert_daily(frame)
        receipt=recorder.record_daily(store,DAY,observations=(observation,))
        store._conn.execute('ROLLBACK')
        assert store.count_daily()==0
        assert store._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(0,)
        store._conn.execute('BEGIN')
        store.upsert_daily(frame)
        receipt=recorder.record_daily(store,DAY,observations=(observation,))
        store._conn.execute('COMMIT')
        assert recorder.verify_receipt(store,receipt)==receipt
        assert {claim.dataset_id for claim in receipt.datasets}=={'daily_bar'}


def test_shadow_source_cannot_claim_production_collection() -> None:
    assert importlib.util.find_spec('rquant.data_collection_contracts') is not None
    from rquant.data_collection_contracts import CollectionRecorderConfig
    config=_recorder().config.model_dump(mode='python')
    config['collector_id']='shadow'
    with pytest.raises(ValueError):
        CollectionRecorderConfig.model_validate(config)


def test_same_commit_identity_cannot_change_content(tmp_path: Path) -> None:
    recorder=_recorder()
    from rquant.data_collection_contracts import SourceObservation
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        _seed(store)
        observation=SourceObservation.from_frame('daily',{'trade_date':'20261005'},pd.DataFrame(),observed_at=NOW)
        store._conn.execute('BEGIN')
        first=recorder.record_daily(store,DAY,observations=(observation,))
        store._conn.execute('COMMIT')
        store._conn.execute('BEGIN')
        changed=observation.model_copy(update={'response_sha256':'f'*64})
        with pytest.raises(ValueError,match='conflict'):
            recorder.record_daily(store,DAY,observations=(changed,))
        store._conn.execute('ROLLBACK')
        assert recorder.verify_receipt(store,first)==first


def test_collection_response_capacity_and_nonfinite_values_fail_closed() -> None:
    assert importlib.util.find_spec('rquant.data_collection_contracts') is not None
    from rquant.data_collection_contracts import SourceObservation
    with pytest.raises(ValueError):
        SourceObservation.from_frame('daily',{},pd.DataFrame({'value':[float('inf')]}),observed_at=NOW)
    with pytest.raises(ValueError):
        SourceObservation.from_frame('daily',{},pd.DataFrame({'value':range(8001)}),observed_at=NOW)


@pytest.mark.parametrize('receipt_failure',[False,True])
def test_original_dataset_snapshot_facts_and_receipt_are_atomic(tmp_path: Path,receipt_failure: bool) -> None:
    import inspect
    from rquant.dataset_backfill import backfill_dataset
    assert 'completion_recorder' in inspect.signature(backfill_dataset).parameters
    recorder=_recorder()
    config=recorder.config.model_dump(mode='python')
    config['collector_id']='dataset_backfill'
    from rquant.data_collection_contracts import CollectionRecorderConfig
    recorder.config=CollectionRecorderConfig.model_validate(config)
    class Source:
        def trade_cal(self,start: date,end: date) -> list[date]:
            return [DAY]
        def ths_index_snapshot(self) -> pd.DataFrame:
            return pd.DataFrame([dict(ts_code='885001.TI',name='新板块',count=1,exchange='A',
                list_date='20261005',type='N')])
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        _seed(store)
        original=pd.DataFrame([dict(ts_code='885002.TI',name='旧板块',member_count=2,
            exchange='A',list_date=DAY,board_type='N')])
        store.replace_dataset('ths_board',original)
        if receipt_failure:
            def fail(*args: object,**kwargs: object) -> None:
                raise OSError('receipt unavailable')
            recorder.record_dataset=fail
        result=backfill_dataset('ths_index',DAY,DAY,store,Source(),api_sleep=0,
            completion_recorder=recorder)
        assert bool(result['failed_dates'])==receipt_failure
        codes=store._conn.execute('SELECT ts_code FROM ths_board').fetchall()
        assert codes==[('885002.TI' if receipt_failure else '885001.TI',)]
        assert store._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(0 if receipt_failure else 1,)


def test_snapshot_existing_mode_requires_actual_outer_transaction(tmp_path: Path) -> None:
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        frame=pd.DataFrame([dict(ts_code='885001.TI',name='板块')])
        with pytest.raises(ValueError,match='transaction'):
            store.replace_dataset('ths_board',frame,transaction_mode='existing')


def _valuation_source():
    from rquant.data_collection_contracts import SourceObservation
    params={'trade_date':'20261005','fields':'ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,pe_ttm,pb,dv_ttm'}
    raw=pd.DataFrame([['600000.SH','20261005',1.0,2.0,100.0,50.0,10.0,None,2.0]],columns=params['fields'].split(','),dtype=object)
    observation=SourceObservation.from_frame('daily_basic',params,raw,observed_at=NOW,
        source_normalization_version='tushare-nullable-v1')
    return raw,observation


def test_collected_valuations_use_original_pit_and_same_fact_receipt_transaction(tmp_path: Path) -> None:
    from rquant.data_collection_authority import verify_collected_daily_valuation
    from rquant.daily_valuation_pit import DailyValuationPITQuery,query_daily_valuation_pit
    recorder=_recorder()
    raw,observation=_valuation_source()
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        _seed(store)
        store.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=date(2026,10,6),is_open=True,
            pretrade_date=DAY,updated_at=NOW),))
        frame=raw.copy()
        frame['trade_date']=DAY
        for commit in (False,True):
            store._conn.execute('BEGIN')
            store.upsert_daily_basic(frame)
            receipt=recorder.record_daily(store,DAY,observations=(observation,),daily_basic_response=raw)
            assert verify_collected_daily_valuation(store,receipt)
            store._conn.execute('COMMIT' if commit else 'ROLLBACK')
            assert store._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(int(commit),)
            assert store._conn.execute('SELECT COUNT(*) FROM daily_basic_valuation_batch').fetchone()==(int(commit),)
        selected=query_daily_valuation_pit(store._conn,DailyValuationPITQuery(ts_code='600000.SH',decision_date=date(2026,10,6)))
        assert selected.status=='selected' and (selected.pe_ttm,selected.pb,selected.dv_ttm)==(10.0,None,2.0)
        assert selected.first_observed_at==NOW and selected.observed_at==NOW
        store._conn.execute('BEGIN')
        assert recorder.record_daily(store,DAY,observations=(observation,),daily_basic_response=raw)==receipt
        store._conn.execute('COMMIT')
        assert store._conn.execute('SELECT COUNT(*) FROM daily_basic_valuation_observation').fetchone()==(1,)
        store._conn.execute('UPDATE daily_basic_valuation_observation SET pe_ttm=999')
        with pytest.raises(ValueError,match='valuation'):
            verify_collected_daily_valuation(store,receipt)


def test_collected_valuations_refuse_unbound_sdk_content_and_future_observation(tmp_path: Path) -> None:
    recorder=_recorder()
    raw,observation=_valuation_source()
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        _seed(store)
        for changed,source in ((raw.assign(pe_ttm=11.0),observation),(raw,observation.model_copy(update={'observed_at':NOW.replace(hour=11)}))):
            store._conn.execute('BEGIN')
            with pytest.raises(ValueError):
                recorder.record_daily(store,DAY,observations=(source,),daily_basic_response=changed)
            store._conn.execute('ROLLBACK')
        assert store._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(0,)
        assert store._conn.execute('SELECT COUNT(*) FROM daily_basic_valuation_batch').fetchone()==(0,)


@pytest.mark.parametrize('record',[False,True])
def test_original_daily_ingest_requests_valuations_only_with_explicit_recorder(tmp_path: Path,record: bool) -> None:
    from tests.unit.test_ingest import _FakeDailyPro,_StatusAdapter,_ReaderFactory,_WriterFactory,INGESTED_AT
    from rquant.ingest import ingest_daily
    from rquant.data_collection_authority import CollectionCommitRecorder
    from rquant.data_collection_contracts import CollectionRecorderConfig
    day=date(2024,1,2)
    primary=tmp_path/'primary.duckdb'
    calendar=MarketCalendarAuthority.create(schema_version=1,exchange='SSE',producer_commit='a'*40,
        coverage_start=day,coverage_end=day,open_dates=(day,),generated_at=INGESTED_AT)
    recorder=CollectionCommitRecorder(CollectionRecorderConfig(collector_id='legacy_daily',run_id='legacy-actual-entry',
        owner='test-owner',code_commit='a'*40,source_generation_id='b'*64,calendar=calendar),clock=lambda:INGESTED_AT)
    with DuckDBStore(primary) as store:
        store.upsert_trade_calendar((TradeCalendarDay(exchange='SSE',cal_date=day,is_open=True,
            pretrade_date=date(2023,12,29),updated_at=INGESTED_AT),))
    requested=[]
    class Pro(_FakeDailyPro):
        def daily_basic(self,**kwargs: object) -> pd.DataFrame:
            requested.append(kwargs['fields'])
            raw=super().daily_basic(**kwargs)
            if record:
                raw=raw.assign(pe_ttm=10.0,pb=float('nan'),dv_ttm=2.0)
            return raw
    assert ingest_daily(day.isoformat(),pro=Pro(include_daily_basic=True),status_adapter=_StatusAdapter(primary),
        indicator_reader_factory=_ReaderFactory(primary),writer_factory=_WriterFactory(primary),ingested_at=INGESTED_AT,
        api_sleep=0,sleep=lambda _:None,completion_recorder=recorder if record else None)==1
    assert requested==['ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv'+(',pe_ttm,pb,dv_ttm' if record else '')]
    with DuckDBStore(primary,read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM ingestion_commit_receipt').fetchone()==(int(record),)
        assert reader._conn.execute('SELECT COUNT(*) FROM daily_basic_valuation_batch').fetchone()==(int(record),)
        if record:
            assert reader._conn.execute('SELECT pe_ttm,pb,dv_ttm,first_observed_at FROM daily_basic_valuation_observation').fetchone()==(10.0,None,2.0,INGESTED_AT)


def _audited_collection(tmp_path: Path) -> tuple:
    from tests.unit.test_data_collection_bridge import _chain
    from rquant.data_audit_report_jobs import DataAuditReportJobWorker
    from rquant.data_collection_authority import load_collection_proof
    chain=_chain(tmp_path)
    primary,replica,reference,jobs,bridge,gate,*_=chain
    bridge.run_one()
    succeeded=DataAuditReportJobWorker(jobs).run_one()
    assert succeeded.status=='succeeded'
    proof=load_collection_proof(jobs.collection_directory,reference)
    return primary,replica,reference,jobs,gate,succeeded,proof


def test_real_audit_and_legal_unpin_restore_exact_original_sidecar_without_rebinding(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import data_collection_authority as module
    from rquant.screen.replica_source import VerifiedReplicaScreenSource,ScreenReplicaUnavailableError
    from rquant.replica_generation import replica_generation_path
    from rquant.data_audit_report import data_audit_report_path
    primary,replica,reference,jobs,gate,succeeded,proof=_audited_collection(tmp_path)
    sidecar=replica_generation_path(replica)
    original=sidecar.read_bytes()
    report_bytes=data_audit_report_path(jobs.report_directory,succeeded.report_hash).read_bytes()
    source=VerifiedReplicaScreenSource(primary_path=primary,replica_path=replica)
    with pytest.raises(ScreenReplicaUnavailableError):
        source.generation_identity()
    scanned=[]
    original_hash=module._file_sha
    def observed_hash(path: Path,**kwargs: object) -> str:
        scanned.append(path)
        return original_hash(path,**kwargs)
    monkeypatch.setattr(module,'_file_sha',observed_hash)
    assert module.restore_collection_report_replica(jobs,succeeded.task_id,replica_path=replica,primary_writer_gate=gate.config)
    first=source.generation_identity()
    published=sidecar.stat()
    assert sidecar.read_bytes()==original==proof.original_sidecar_bytes.encode()
    assert not module.restore_collection_report_replica(jobs,succeeded.task_id,replica_path=replica,primary_writer_gate=gate.config)
    assert sidecar.stat()==published and source.generation_identity()==first and scanned==[replica]
    with gate.acquire() as lease:
        assert module.release_completed_collection_pins(jobs,replica_path=replica,primary_writer_lease=lease)==1
    assert source.generation_identity()!=first  # the legal unlink advances the original consumer identity
    assert scanned==[replica,replica] and sidecar.read_bytes()==original
    assert not proof.fixed_replica_path.exists()
    assert module.load_collection_proof(jobs.collection_directory,reference)==proof
    assert data_audit_report_path(jobs.report_directory,succeeded.report_hash).read_bytes()==report_bytes
    assert jobs.status(succeeded.task_id)==succeeded


@pytest.mark.parametrize('case',['bytes','sidecar','inode','scan_inode','scan_sidecar','publish_bytes','stop','deadline','busy'])
def test_sidecar_restoration_rejects_changed_or_unavailable_original_material(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    import os,json
    from shutil import copyfile
    from datetime import timedelta
    from rquant import data_collection_authority as module,research_sync
    from rquant.replica_generation import replica_generation_path
    from rquant.screen.replica_source import VerifiedReplicaScreenSource,ScreenReplicaUnavailableError
    from rquant.storage.primary_writer_gate import PrimaryWriterBusy
    primary,replica,reference,jobs,gate,succeeded,proof=_audited_collection(tmp_path)
    sidecar=replica_generation_path(replica)
    original_sidecar=sidecar.read_bytes()
    published=[]
    publish=research_sync._publish_replica_generation_sidecar
    def record_publish(*args: Path) -> None:
        published.append(args)
        publish(*args)
    monkeypatch.setattr(research_sync,'_publish_replica_generation_sidecar',record_publish)
    def change_bytes() -> None:
        before=replica.stat()
        with replica.open('r+b') as source:
            source.seek(-1,2)
            old=source.read(1)
            source.seek(-1,2)
            source.write(bytes([old[0]^1]))
        os.utime(replica,ns=(before.st_atime_ns,before.st_mtime_ns))
        after=replica.stat()
        assert (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)==(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)
    if case=='bytes':
        change_bytes()
    elif case=='sidecar':
        value=json.loads(original_sidecar)
        value['source_before']['main']['mtime_ns']-=1
        value['source_after']=value['source_before']
        from rquant.replica_generation import ReplicaGenerationMetadata
        ReplicaGenerationMetadata.model_validate(value)
        sidecar.write_text(json.dumps(value))
    elif case=='inode':
        before=replica.stat()
        replacement=tmp_path/'replacement.duckdb'
        copyfile(replica,replacement)
        os.utime(replacement,ns=(before.st_atime_ns,before.st_mtime_ns))
        replacement.replace(replica)
    elif case.startswith('scan_'):
        original_hash=module._file_sha
        def moved_hash(path: Path,**kwargs: object) -> str:
            result=original_hash(path,**kwargs)
            target=replica if case=='scan_inode' else sidecar
            replacement=tmp_path/'moved-source'
            copyfile(target,replacement)
            replacement.replace(target)
            return result
        monkeypatch.setattr(module,'_file_sha',moved_hash)
    elif case=='publish_bytes':
        def raced_publish(*args: Path) -> None:
            change_bytes()
            record_publish(*args)
        monkeypatch.setattr(research_sync,'_publish_replica_generation_sidecar',raced_publish)
    if case=='busy':
        with gate.acquire(),pytest.raises(PrimaryWriterBusy):
            module.restore_collection_report_replica(jobs,succeeded.task_id,replica_path=replica,primary_writer_gate=gate.config)
    else:
        with gate.acquire() as lease,pytest.raises((ValueError,InterruptedError)):
            module.republish_verified_collection_sidecar(proof,replica_path=replica,primary_writer_lease=lease,
                stop_requested=(lambda:True) if case=='stop' else None,
                deadline=datetime.now(UTC)-timedelta(seconds=1) if case=='deadline' else None)
    if case=='publish_bytes':
        assert len(published)==1 and not sidecar.exists()
    else:
        assert published==[]
        if case!='sidecar':
            assert sidecar.read_bytes()==original_sidecar
    if case not in {'sidecar','scan_sidecar'}:
        with pytest.raises(ScreenReplicaUnavailableError):
            VerifiedReplicaScreenSource(primary_path=primary,replica_path=replica).generation_identity()
    assert module.load_collection_proof(jobs.collection_directory,reference)==proof
    assert not tuple(sidecar.parent.glob('.collection-generation-*'))


@pytest.mark.parametrize('case',['queued','failed','missing_proof'])
def test_sidecar_restoration_needs_readable_original_success_and_proof(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,case: str) -> None:
    from tests.unit.test_data_collection_bridge import _chain
    from rquant import data_collection_authority as module,data_audit_report_jobs as audit
    from rquant.replica_generation import replica_generation_path
    primary,replica,reference,jobs,bridge,gate,*_=_chain(tmp_path)
    accepted=bridge.run_one()
    if case=='failed':
        def failed(**kwargs: object) -> None:
            raise OSError('original report I/O failed')
        monkeypatch.setattr(audit,'create_and_publish_data_audit_report',failed)
        assert audit.DataAuditReportJobWorker(jobs).run_one().status=='failed'
    elif case=='missing_proof':
        assert audit.DataAuditReportJobWorker(jobs).run_one().status=='succeeded'
        (jobs.collection_directory/reference.relative_proof_name).unlink()
    sidecar=replica_generation_path(replica)
    before=sidecar.stat(),sidecar.read_bytes()
    with pytest.raises((ValueError,OSError)):
        module.restore_collection_report_replica(jobs,accepted.task_id,replica_path=replica,primary_writer_gate=gate.config)
    assert (sidecar.stat(),sidecar.read_bytes())==before
