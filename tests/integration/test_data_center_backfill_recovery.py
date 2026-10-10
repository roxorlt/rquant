from __future__ import annotations

from datetime import date,timedelta
from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.parametrize('kind',['backfill','financial'])
def test_original_maintenance_finishes_only_after_real_replica_and_original_audit(tmp_path: Path,monkeypatch,kind: str) -> None:
    from rquant.data_center_maintenance_runtime import DataCenterCompletionRuntime,DataCenterMaintenanceRuntimeConfig
    from rquant.data_audit_report import load_data_audit_report,data_audit_report_path
    from rquant.data_audit_report_jobs import DataAuditReportJobStore
    if kind=='backfill':
        from tests.unit.test_backfill_execute import _worker_setup
        state,spec,current,worker,calls=_worker_setup(tmp_path)
    else:
        from tests.unit.test_financial_runtime import _financial_runtime_setup
        state,spec,current,_,worker,calls=_financial_runtime_setup(tmp_path,monkeypatch)
    config=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'replica.duckdb',audit_state_path=tmp_path/'audit.sqlite',
        audit_directory=tmp_path/'reports',collection_directory=tmp_path/'collection')
    runtime=DataCenterCompletionRuntime(state,config=config,policy=lambda:current[0],clock=worker.clock)
    worker.completion_runtime=runtime
    assert worker.run_one(spec.execution_id,owner=spec.owner).execution.status=='running'
    assert worker.run_one(spec.execution_id,owner=spec.owner).execution.status=='running'
    before=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    assert before.completed_tasks==2 and before.completion_sha256 is None
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.execution.status=='completed' and result.execution.completed_tasks==result.execution.total_tasks
    jobs=DataAuditReportJobStore(state_path=config.audit_state_path,report_directory=config.audit_directory,
        collection_directory=config.collection_directory,clock=worker.clock)
    receipt=jobs.status(result.execution.audit_task_id)
    assert receipt.status=='succeeded' and receipt.report_hash==result.execution.audit_report_sha256
    report=load_data_audit_report(data_audit_report_path(config.audit_directory,receipt.report_hash))
    assert report.schema_version==3 and report.collection_completed_through is None
    assert config.replica_path.is_file()
    from rquant.screen.replica_source import VerifiedReplicaScreenSource
    source=VerifiedReplicaScreenSource(primary_path=current[0].primary_writer_gate.primary_path,
        replica_path=config.replica_path)
    assert source.generation_identity()


def test_original_completion_report_read_failure_keeps_claim_unfinished_and_resumes_same_job(tmp_path: Path,monkeypatch) -> None:
    from rquant.data_center_maintenance_runtime import DataCenterCompletionRuntime,DataCenterMaintenanceRuntimeConfig
    from rquant import data_center_maintenance_runtime as runtime_module
    from tests.unit.test_backfill_execute import _worker_setup,NOW
    state,spec,current,worker,calls=_worker_setup(tmp_path)
    config=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'replica.duckdb',audit_state_path=tmp_path/'audit.sqlite',
        audit_directory=tmp_path/'reports',collection_directory=tmp_path/'collection')
    runtime=DataCenterCompletionRuntime(state,config=config,policy=lambda:current[0],clock=worker.clock)
    worker.completion_runtime=runtime
    worker.run_one(spec.execution_id,owner=spec.owner)
    worker.run_one(spec.execution_id,owner=spec.owner)
    original=runtime_module._load_completion_report
    monkeypatch.setattr(runtime_module,'_load_completion_report',lambda *args,**kwargs:(_ for _ in ()).throw(OSError('report read failed')))
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.execution.status=='partial' and result.execution.completion_sha256 is None
    task=runtime.jobs.latest_success()
    assert task is not None
    monkeypatch.setattr(runtime_module,'_load_completion_report',original)
    status=state.get_maintenance_status(spec.execution_id,owner=spec.owner)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume-real-completion',expected_sequence=status.control_sequence,
        action='resume',now=NOW)
    result=worker.run_one(spec.execution_id,owner=spec.owner)
    assert result.execution.status=='completed' and result.execution.audit_task_id==task.task_id and calls==[]


@pytest.mark.parametrize('failure',['sqlite_ack','audit_publish'])
def test_original_completion_recovers_exact_committed_receipt_or_failed_audit_without_sdk(tmp_path: Path,monkeypatch,failure: str) -> None:
    from rquant.data_center_maintenance_runtime import DataCenterCompletionRuntime,DataCenterMaintenanceRuntimeConfig
    from rquant.storage.duckdb import DuckDBStore
    import rquant.data_audit_report_jobs as job_module
    from tests.unit.test_backfill_execute import _worker_setup,NOW
    state,spec,current,worker,calls=_worker_setup(tmp_path)
    config=DataCenterMaintenanceRuntimeConfig(replica_path=tmp_path/'replica.duckdb',audit_state_path=tmp_path/'audit.sqlite',
        audit_directory=tmp_path/'reports',collection_directory=tmp_path/'collection')
    runtime=DataCenterCompletionRuntime(state,config=config,policy=lambda:current[0],clock=worker.clock)
    worker.completion_runtime=runtime
    worker.run_one(spec.execution_id,owner=spec.owner)
    worker.run_one(spec.execution_id,owner=spec.owner)
    original_ack=state._succeed_in_transaction
    original_publish=job_module.create_and_publish_data_audit_report
    if failure=='sqlite_ack':
        def lose_ack(connection,claim,**kwargs: object) -> None:
            if claim.task_id=='verify-completion':
                raise OSError('original SQLite completion acknowledgement lost after DuckDB COMMIT')
            original_ack(connection,claim,**kwargs)
        monkeypatch.setattr(state,'_succeed_in_transaction',lose_ack)
    else:
        monkeypatch.setattr(job_module,'create_and_publish_data_audit_report',
            lambda **kwargs:(_ for _ in ()).throw(OSError('original audit publication failed')))
    partial=worker.run_one(spec.execution_id,owner=spec.owner).execution
    assert partial.status=='partial' and partial.completion_sha256 is None
    task=runtime.jobs.latest()
    assert task is not None and task.status==('succeeded' if failure=='sqlite_ack' else 'failed')
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        before=reader._conn.execute("SELECT receipt_id,payload_json FROM backfill_day_commit_receipt WHERE task_id='verify-completion'").fetchall()
    assert len(before)==(1 if failure=='sqlite_ack' else 0)
    monkeypatch.setattr(state,'_succeed_in_transaction',original_ack)
    monkeypatch.setattr(job_module,'create_and_publish_data_audit_report',original_publish)
    state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume-exact-completion-'+failure,
        expected_sequence=partial.control_sequence,action='resume',now=NOW)
    completed=worker.run_one(spec.execution_id,owner=spec.owner).execution
    assert completed.status=='completed' and completed.audit_task_id==task.task_id and calls==[]
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        after=reader._conn.execute("SELECT receipt_id,payload_json FROM backfill_day_commit_receipt WHERE task_id='verify-completion'").fetchall()
    if failure=='sqlite_ack':
        assert after==before
    assert len(after)==1 and completed.completion_sha256==after[0][0]
    with runtime.jobs._transaction() as connection:
        assert connection.execute('SELECT COUNT(*) FROM data_audit_report_job').fetchone()[0]==1


def test_original_state_tail_preparation_keeps_math_and_joins_outer_receipt_transaction(tmp_path: Path) -> None:
    from tests.unit.test_backfill_execute import _exact_day_setup
    from rquant.backfill_execute import commit_exact_backfill_day
    from rquant.market_backfill import prepare_daily_state_tail,recompute_daily_state
    from rquant.storage.duckdb import DuckDBStore
    state,claim,_,_,_,args=_exact_day_setup(tmp_path)
    commit_exact_backfill_day(state,claim,**args)
    day=date(2026,10,5)
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        assert recompute_daily_state(store,['600000.SH'],start_date=day,status_mode='verified_no_fetch')==1
        expected=store._conn.execute('SELECT * FROM daily_state ORDER BY ts_code,trade_date').fetchall()
        store._conn.execute('DELETE FROM daily_state')
        prepared=prepare_daily_state_tail(store,['600000.SH'],start_date=day,end_date=day)
        with pytest.raises(ValueError,match='outer transaction'):
            recompute_daily_state(store,['600000.SH'],start_date=day,status_mode='verified_no_fetch',
                transaction_mode='existing',prepared_tail=prepared)
        store._conn.execute('BEGIN')
        recompute_daily_state(store,['600000.SH'],start_date=day,status_mode='verified_no_fetch',
            transaction_mode='existing',prepared_tail=prepared)
        store._conn.execute('ROLLBACK')
        assert store._conn.execute('SELECT COUNT(*) FROM daily_state').fetchone()[0]==0
        store._conn.execute('BEGIN')
        recompute_daily_state(store,['600000.SH'],start_date=day,status_mode='verified_no_fetch',
            transaction_mode='existing',prepared_tail=prepared)
        store._conn.execute('COMMIT')
        assert store._conn.execute('SELECT * FROM daily_state ORDER BY ts_code,trade_date').fetchall()==expected


def test_original_state_tail_refuses_source_changed_after_preparation(tmp_path: Path) -> None:
    from tests.unit.test_backfill_execute import _exact_day_setup
    from rquant.backfill_execute import commit_exact_backfill_day
    from rquant.market_backfill import prepare_daily_state_tail,recompute_daily_state
    from rquant.storage.duckdb import DuckDBStore
    state,claim,_,_,_,args=_exact_day_setup(tmp_path)
    commit_exact_backfill_day(state,claim,**args)
    day=date(2026,10,5)
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        prepared=prepare_daily_state_tail(store,['600000.SH'],start_date=day,end_date=day)
        store._conn.execute('UPDATE daily_bar SET close=12 WHERE ts_code=?',['600000.SH'])
        store._conn.execute('BEGIN')
        with pytest.raises(ValueError,match='source'):
            recompute_daily_state(store,['600000.SH'],start_date=day,status_mode='verified_no_fetch',
                transaction_mode='existing',prepared_tail=prepared)
        store._conn.execute('ROLLBACK')
        assert store._conn.execute('SELECT COUNT(*) FROM daily_state').fetchone()[0]==0


def test_original_bounded_indicator_range_equals_original_causal_daily_results(tmp_path: Path) -> None:
    from rquant.indicator_backfill import derive_daily_indicators,derive_target_daily_indicators
    from rquant.storage.duckdb import DuckDBStore
    start=date(2026,1,1)
    daily=[]
    factors=[]
    for offset in range(65):
        day=start+timedelta(days=offset)
        for code,base in (('600000.SH',10.0),('000001.SZ',20.0)):
            price=base+offset*0.05+(offset%3)*0.02
            daily.append(dict(ts_code=code,trade_date=day,open=price-0.1,high=price+0.2,low=price-0.2,
                close=price,pre_close=price-0.05,change=0.05,pct_chg=0.5,vol=100.0,amount=1000.0))
            factors.append(dict(ts_code=code,trade_date=day,adj_factor=1+offset*0.005))
    daily_frame=pd.DataFrame(daily)
    factor_frame=pd.DataFrame(factors)
    with DuckDBStore(tmp_path/'primary.duckdb') as store:
        store.upsert_daily(daily_frame)
        store.upsert_adj_factor(factor_frame)
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as store:
        actual=derive_daily_indicators(store,start_date=start+timedelta(days=40),end_date=start+timedelta(days=64),
            ts_codes=('600000.SH','000001.SZ'),batch_size=250)
        pieces=[]
        for offset in range(40,65):
            day=start+timedelta(days=offset)
            pieces.append(derive_target_daily_indicators(store,target_date=day,daily_rows=daily_frame.loc[daily_frame.trade_date==day],
                factor_rows=factor_frame.loc[factor_frame.trade_date==day],batch_size=250))
    expected=pd.concat(pieces,ignore_index=True).sort_values(['ts_code','trade_date'],kind='stable').reset_index(drop=True)
    pd.testing.assert_frame_equal(actual,expected)


def _derived_tail_setup(tmp_path: Path):
    from tests.unit.test_backfill_execute import _exact_day_setup
    from rquant.backfill_execute import commit_exact_backfill_day
    state,day_claim,spec,current,_,args=_exact_day_setup(tmp_path)
    commit_exact_backfill_day(state,day_claim,**args)
    claim=state.claim_task(spec.manifest_id,worker_id='tail-fixture',lease_seconds=120,
        now=args['clock'](),maintenance_execution_id=spec.execution_id)
    assert claim.task_id=='tail-derived'
    return state,claim,spec,current,args


def test_original_tail_batch_receipt_and_math_commit_together_and_recover_without_recompute(tmp_path: Path,monkeypatch) -> None:
    from rquant.data_center_maintenance_runtime import prepare_derived_tail_batch,commit_derived_tail_batch,verify_derived_tail_batch
    from rquant.storage.duckdb import DuckDBStore
    state,claim,spec,current,args=_derived_tail_setup(tmp_path)
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        prepared=prepare_derived_tail_batch(reader,spec,('600000.SH',),batch_index=0,universe=('600000.SH',))
    receipt=commit_derived_tail_batch(state,claim,spec=spec,prepared=prepared,policy=lambda:current[0],
        control_sequence=args['control_sequence'],clock=args['clock'])
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM daily_state').fetchone()[0]==1
        assert reader._conn.execute('SELECT COUNT(*) FROM daily_indicator').fetchone()[0]==1
        assert verify_derived_tail_batch(reader,spec,receipt.task_id)==receipt
    def should_not_write(*args,**kwargs):
        pytest.fail('verified committed original tail must not be recomputed or written')
    monkeypatch.setattr('rquant.data_center_maintenance_runtime.recompute_daily_state',should_not_write)
    assert commit_derived_tail_batch(state,claim,spec=spec,prepared=prepared,policy=lambda:current[0],
        control_sequence=args['control_sequence'],clock=args['clock'])==receipt


@pytest.mark.parametrize('failure',['receipt','source'])
def test_original_tail_batch_failure_rolls_back_both_derived_tables(tmp_path: Path,monkeypatch,failure: str) -> None:
    from rquant.data_center_maintenance_runtime import prepare_derived_tail_batch,commit_derived_tail_batch
    from rquant.storage.duckdb import DuckDBStore
    state,claim,spec,current,args=_derived_tail_setup(tmp_path)
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        prepared=prepare_derived_tail_batch(reader,spec,('600000.SH',),batch_index=0,universe=('600000.SH',))
    if failure=='source':
        with DuckDBStore(tmp_path/'primary.duckdb') as writer:
            writer._conn.execute('UPDATE daily_bar SET close=12')
    else:
        def broken(*args,**kwargs):
            raise OSError('tail receipt I/O failure')
        monkeypatch.setattr('rquant.data_center_maintenance_runtime._write_tail_receipt',broken)
    with pytest.raises((ValueError,OSError),match='source|receipt'):
        commit_derived_tail_batch(state,claim,spec=spec,prepared=prepared,policy=lambda:current[0],
            control_sequence=args['control_sequence'],clock=args['clock'])
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        assert reader._conn.execute('SELECT COUNT(*) FROM daily_state').fetchone()[0]==0
        assert reader._conn.execute('SELECT COUNT(*) FROM daily_indicator').fetchone()[0]==0
        assert reader._conn.execute("SELECT COUNT(*) FROM backfill_day_commit_receipt WHERE task_id LIKE 'tail-derived%'").fetchone()[0]==0


def test_original_tail_worker_completes_only_after_all_derived_batch_receipts(tmp_path: Path) -> None:
    from rquant.data_center_maintenance_runtime import run_derived_tail
    from rquant.storage.duckdb import DuckDBStore
    state,claim,spec,current,args=_derived_tail_setup(tmp_path)
    assert run_derived_tail(state,claim,spec=spec,policy=lambda:current[0],control_sequence=args['control_sequence'],
        clock=args['clock']) is True
    assert state.get_task(spec.manifest_id,'tail-derived').status=='succeeded'
    assert state.get_task(spec.manifest_id,'verify-completion').status=='pending'
    assert state.get_maintenance_status(spec.execution_id,owner=spec.owner).status=='running'
    with DuckDBStore(tmp_path/'primary.duckdb',read_only=True) as reader:
        assert reader._conn.execute("SELECT COUNT(*) FROM backfill_day_commit_receipt WHERE task_id LIKE 'tail-derived%'").fetchone()[0]==2
