from __future__ import annotations

import importlib.util
from datetime import UTC,date,datetime
from datetime import timedelta
from pathlib import Path

import pytest


def test_new_runtime_settings_keep_none_false_and_require_canonical_explicit_paths(tmp_path: Path) -> None:
    from rquant.config import Settings
    values=dict(_env_file=None,tushare_token_main='dummy-offline-token-'*4,data_dir=tmp_path,
        duckdb_path=tmp_path/'primary.duckdb',parquet_dir=tmp_path/'parquet',log_dir=tmp_path/'logs')
    names=('primary_writer_gate_path','data_center_execution_policy_path','data_center_runtime_profile_path','data_center_financial_archive_path')
    original=Settings(**values)
    assert all(getattr(original,name) is None for name in names)
    assert not original.data_center_backfill_execute_enabled and not original.data_center_financial_collect_enabled
    for name in names:
        assert getattr(Settings(**values,**{name:''}),name) is None
        assert getattr(Settings(**values,**{name:tmp_path/name}),name)==tmp_path/name
        with pytest.raises(ValueError,match='absolute|canonical'):
            Settings(**values,**{name:'relative-path'})


def test_unknown_entitlement_cannot_admit_execute_or_actual_dispatch() -> None:
    assert importlib.util.find_spec('rquant.backfill_execute_contracts') is not None
    from rquant.backfill_execute_contracts import ApiEntitlementEvidence
    unknown=ApiEntitlementEvidence(api_name='daily',status='unknown',source_account_sha256='a'*64)
    with pytest.raises(ValueError,match='unknown'):
        unknown.require_current(parameters={'trade_date':'20261005'},now=datetime(2026,10,5,10,tzinfo=UTC))


def test_shanghai_maintenance_window_and_new_day_cutoff() -> None:
    assert importlib.util.find_spec('rquant.backfill_execute_contracts') is not None
    from rquant.backfill_execute_contracts import maintenance_window
    assert not maintenance_window(datetime(2026,10,6,9,49,tzinfo=UTC)).may_start_day
    assert maintenance_window(datetime(2026,10,6,9,50,tzinfo=UTC)).may_start_day
    assert maintenance_window(datetime(2026,10,5,0,19,tzinfo=UTC)).may_start_day
    assert not maintenance_window(datetime(2026,10,5,0,20,tzinfo=UTC)).may_start_day
    assert not maintenance_window(datetime(2026,10,5,0,30,tzinfo=UTC)).may_hold_writer
    assert maintenance_window(datetime(2026,10,10,4,tzinfo=UTC)).may_start_day


def test_source_request_identity_is_stable_across_claims_and_quota_windows() -> None:
    assert importlib.util.find_spec('rquant.backfill_execute_contracts') is not None
    from rquant.backfill_execute_contracts import BackfillSourceRequestBinding
    parameters=dict(owner='test-owner',execution_id='a'*64,manifest_id='b'*64,
        plan_sha256='c'*64,scope_sha256='d'*64,api_name='daily',parameters={'trade_date':'20261005'},
        source_account_sha256='e'*64,quota_source='tushare-account',quota_ledger_device=1,
        quota_ledger_inode=2)
    first=BackfillSourceRequestBinding(**parameters)
    assert first.logical_request_id==BackfillSourceRequestBinding.model_validate_json(first.model_dump_json()).logical_request_id
    for name in ('claim_token','boot_id','quota_window','observed_at'):
        with pytest.raises(ValueError):
            BackfillSourceRequestBinding(**parameters,**{name:'new'})
    assert BackfillSourceRequestBinding(**{**parameters,'parameters':{'trade_date':'20261006'}}).logical_request_id!=first.logical_request_id


def _state_execution(tmp_path: Path,*,owner: str = 'fixture-owner',tag: str = 'a'):
    import duckdb
    from rquant.backfill_state import BackfillStateStore,BackfillManifestInput,BackfillTaskInput
    from rquant.backfill_plan_core import BackfillEstimateAssumptions,build_daily_bar_backfill_plan
    from rquant.backfill_execute_contracts import BackfillExecutionIntent,BackfillExecutionSpec
    from rquant.data_collection_contracts import AuditCollectionReference
    from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity
    from rquant.runtime_contracts import canonical_sha256
    from rquant.storage.schema import DAILY_BAR_DDL,TRADE_CALENDAR_DDL
    observed=datetime(2026,10,5,10,tzinfo=UTC)
    path=tmp_path/'primary.duckdb'
    with duckdb.connect(str(path)) as connection:
        connection.execute(DAILY_BAR_DDL)
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute("INSERT INTO trade_calendar(exchange,cal_date,is_open,source,updated_at) VALUES ('SSE',DATE '2026-10-05',TRUE,'tushare',?) ON CONFLICT DO NOTHING",[observed])
    with duckdb.connect(str(path),read_only=True) as connection:
        plan=build_daily_bar_backfill_plan(connection,snapshot_label='offline-fixture',snapshot_file_sha256='b'*64,
            evidence_code_revision='fixture',audit_start=date(2026,10,5),completed_through=date(2026,10,5),observed_at=observed,
            assumptions=BackfillEstimateAssumptions(status_namechange_start=date(2026,1,1),status_source_as_of=observed.date(),
                status_window_years=3,adapter_seconds_per_operation=1,market_throttle_seconds_per_operation=0,
                status_throttle_seconds_per_operation=0,retry_allowance_seconds_per_operation=0))
    physical=path.stat()
    identity=CanonicalDatabaseIdentity(canonical_path=str(path),device=physical.st_dev,inode=physical.st_ino)
    reference=AuditCollectionReference(event_id='b'*64,binding_sha256='c'*64,sequence=1,proof_sha256='d'*64,
        relative_proof_name='collection-'+('d'*64)+'.json',byte_count=100)
    intent=BackfillExecutionIntent(execution_id=tag*64,owner=owner,prepare_command_id='prepare-'+tag,
        plan_task_id='e'*32,plan_sha256=plan.content_sha256,exact_dates_sha256=canonical_sha256(plan.missing_dates),
        source_reference=reference,source_generation_id='f'*64,calendar_sha256=plan.evidence.calendar_sha256,
        primary_identity=identity,policy_generation='1'*64,nonce_sha256='2'*64,
        issued_at=observed,expires_at=observed+timedelta(minutes=5))
    spec=BackfillExecutionSpec(execution_id=intent.execution_id,owner=owner,plan_task_id=intent.plan_task_id,plan=plan,
        intent=intent,execute_command_id='execute-'+tag,manifest_id=canonical_sha256({'execution':intent.execution_id}))
    manifest=BackfillManifestInput(manifest_id=spec.manifest_id,payload={'execution_id':spec.execution_id},
        tasks=(BackfillTaskInput(task_id='day-2026-10-05',payload={'trade_date':'2026-10-05'}),),eligibility=())
    state=BackfillStateStore(tmp_path/'state.sqlite3',maintenance_enabled=True,busy_timeout_ms=30)
    return state,spec,manifest,observed


def test_confirmation_and_original_manifest_are_atomic_and_repeatable(tmp_path: Path) -> None:
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    accepted=state.admit_backfill_execution(spec,manifest,now=now)
    assert accepted.status=='queued' and accepted.total_tasks==1
    assert state.load_manifest(spec.manifest_id)==manifest
    assert state.admit_backfill_execution(spec,manifest,now=now+timedelta(minutes=8))==accepted
    assert state.get_backfill_execution_spec(spec.execution_id,owner=spec.owner)==spec
    with pytest.raises(ValueError,match='owner|owned'):
        state.get_backfill_execution_spec(spec.execution_id,owner='another-owner')


def test_confirmation_expiry_or_other_active_execution_never_leaves_manifest(tmp_path: Path) -> None:
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    with pytest.raises(ValueError,match='expired'):
        state.admit_backfill_execution(spec,manifest,now=now+timedelta(minutes=5))
    assert state.load_manifest(spec.manifest_id) is None
    state.admit_backfill_execution(spec,manifest,now=now)
    _,second,second_manifest,_=_state_execution(tmp_path,tag='c')
    state.persist_maintenance_intent(second.intent)
    with pytest.raises(ValueError,match='another maintenance'):
        state.admit_backfill_execution(second,second_manifest,now=now)
    assert state.load_manifest(second.manifest_id) is None


def test_pause_ack_is_distinct_from_applied_pause_and_resumes_same_manifest(tmp_path: Path) -> None:
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    running=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=1,status='running',now=now)
    claim=state.claim_task(spec.manifest_id,worker_id='fixture',lease_seconds=120,now=now,
        maintenance_execution_id=spec.execution_id)
    assert claim is not None
    from rquant.backfill_state import StaleTaskClaimError
    with pytest.raises(StaleTaskClaimError):
        state.claim_task(spec.manifest_id,worker_id='legacy',lease_seconds=120,now=now)
    assert state.claim_task(spec.manifest_id,worker_id='other',lease_seconds=120,now=now,
        maintenance_execution_id=spec.execution_id) is None
    pending=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='pause',expected_sequence=running.control_sequence,
        action='pause',now=now)
    assert pending.pause_requested and not pending.pause_applied and pending.status=='running'
    with pytest.raises(ValueError,match='claim must be released'):
        state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=pending.control_sequence,status='paused',now=now)
    state.release_task_claim(claim,now=now)
    paused=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=pending.control_sequence,status='paused',now=now)
    resumed=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume',expected_sequence=paused.control_sequence,
        action='resume',now=now)
    assert resumed.manifest_id==spec.manifest_id and resumed.status=='queued' and not resumed.pause_requested
    assert state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume',expected_sequence=paused.control_sequence,
        action='resume',now=now)==resumed


def test_resume_refreshes_effective_policy_without_rebinding_original_execution(tmp_path: Path) -> None:
    state,spec,manifest,now=_state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec,manifest,now=now)
    paused=state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=1,status='paused',now=now)
    resumed=state.maintenance_control(spec.execution_id,owner=spec.owner,command_id='resume-current-policy',
        expected_sequence=paused.control_sequence,action='resume',policy_generation='3'*64,now=now)
    assert resumed.policy_generation=='3'*64
    assert state.get_backfill_execution_spec(spec.execution_id,owner=spec.owner)==spec
    assert state.load_manifest(spec.manifest_id)==manifest
    assert state.maintenance_control_by_command('resume-current-policy',execution_id=spec.execution_id,
        owner=spec.owner,expected_sequence=paused.control_sequence,action='resume')==resumed
