"""Two PageControl steps bind the original plan, owner, source and state manifest."""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC,datetime,timedelta
from pathlib import Path
import stat
from typing import TYPE_CHECKING

from pydantic import Field

from rquant.backfill_execute import load_execution_policy,require_execution_policy,require_stable_execution_policy
from rquant.backfill_execute_contracts import BackfillExecutionIntent,BackfillExecutionSpec,DataCenterExecutionPolicy,MARKET_EXECUTE_APIS,ExecutionConfirmation
from rquant.backfill_plan_artifact import load_daily_bar_backfill_plan
from rquant.backfill_plan_jobs import BackfillPlanJobStore
from rquant.backfill_state import BackfillStateStore,BackfillManifestInput,BackfillTaskInput
from rquant.data_audit_report import CollectionDataAuditReport,data_audit_report_path,load_data_audit_report
from rquant.data_audit_report_jobs import DataAuditReportJobStore
from rquant.page_control import (DataCenterExecutionCommand,PrepareBackfillExecution,ExecuteBackfillPlan,
    PauseDataCenterExecution,ResumeDataCenterExecution,PrepareFinancialCollection,ExecuteFinancialCollection)
from rquant.runtime_contracts import RuntimeContractModel,canonical_sha256
from rquant.source_quota_store import SourceQuotaStore,SourceQuotaExhaustedError
from rquant.storage.primary_writer_gate import PrimaryWriterGate

if TYPE_CHECKING:
    from rquant.financial_runtime import FinancialExecutionIntent


class BackfillExecutePageBackendConfig(RuntimeContractModel):
    policy_path: Path
    original_state_path: Path
    plan_state_path: Path
    plan_directory: Path
    audit_state_path: Path
    audit_directory: Path
    collection_directory: Path
    collection_snapshot_root: Path | None = None
    allowed_owners: tuple[str,...] = Field(min_length=1,max_length=32)
    financial_archive_path: Path | None = None
    replica_path: Path | None = None


def build_backfill_execution_manifest(spec: BackfillExecutionSpec) -> BackfillManifestInput:
    return BackfillManifestInput(manifest_id=spec.manifest_id,payload={
        'contract':'controlled-daily-gap-manifest/v1','execution_id':spec.execution_id,
        'owner':spec.owner,'plan_task_id':spec.plan_task_id,'plan_sha256':spec.plan.content_sha256,
        'exact_dates_sha256':spec.intent.exact_dates_sha256,
    },tasks=tuple(BackfillTaskInput(task_id=f'day-{day.isoformat()}',payload={
        'kind':'exact_missing_day','trade_date':day.isoformat(),'execution_id':spec.execution_id,
    },max_attempts=6) for day in spec.plan.missing_dates)+(BackfillTaskInput(
        task_id='tail-derived',payload={'kind':'original_derived_tail','execution_id':spec.execution_id},max_attempts=6),
        BackfillTaskInput(task_id='verify-completion',payload={'kind':'replica_and_original_audit','execution_id':spec.execution_id},max_attempts=6)),
        eligibility=())


class BackfillExecutePageBackend:
    def __init__(self,config: BackfillExecutePageBackendConfig,*,clock: Callable[[],datetime] | None = None) -> None:
        self.config=BackfillExecutePageBackendConfig.model_validate(config)
        self.clock=clock or (lambda:datetime.now(UTC))
        for path in (config.policy_path,config.original_state_path,config.plan_state_path,config.plan_directory,
            config.audit_state_path,config.audit_directory,config.collection_directory,config.financial_archive_path,config.replica_path,
            config.collection_snapshot_root):
            if path is None:
                continue
            if not path.is_absolute() or path.resolve(strict=False)!=path or path.is_symlink():
                raise ValueError('execution backend paths must be canonical and trusted')
        self.state=BackfillStateStore(config.original_state_path,maintenance_enabled=True)
        self.plans=BackfillPlanJobStore(state_path=config.plan_state_path,plan_directory=config.plan_directory,clock=self.clock)
        self.audits=DataAuditReportJobStore(state_path=config.audit_state_path,report_directory=config.audit_directory,clock=self.clock,
            collection_directory=config.collection_directory,collection_snapshot_root=config.collection_snapshot_root)

    def authorize(self,owner: str) -> None:
        if owner not in self.config.allowed_owners:
            raise ValueError('data center execution owner is not authorized')

    def _policy(self,*,kind: str='backfill',require_balance: bool=True) -> DataCenterExecutionPolicy:
        policy=load_execution_policy(self.config.policy_path)
        require_execution_policy(policy,kind=kind,now=self.clock())
        if policy.original_state_path!=self.state.path:
            raise ValueError('execution must use the original configured state file')
        # Nonblocking, before any original state write transaction.
        with PrimaryWriterGate(policy.primary_writer_gate).acquire():
            pass
        quota=SourceQuotaStore(policy.quota_ledger_path)
        identifier,start,end=quota._quota_window(self.clock().astimezone(UTC),window_kind=policy.quota_window_kind)
        quota.declare_window(source=policy.quota_source,window_id=identifier,starts_at=start,resets_at=end,
            total_units=policy.quota_units_per_window)
        if require_balance and quota.remaining(policy.quota_source,now=self.clock())<1:
            raise SourceQuotaExhaustedError('current original source quota is exhausted')
        return policy

    def _plan(self,task_id: str,plan_hash: str,*,owner: str):
        request=self.plans.request_for_task(task_id)
        if request.owner!=owner or request.page_command_id is None:
            raise ValueError('original plan has no current owner proof')
        receipt=self.plans.status(task_id)
        if receipt.status!='succeeded' or receipt.plan_hash!=plan_hash:
            raise ValueError('original successful plan task or hash differs')
        return load_daily_bar_backfill_plan(self.config.plan_directory/f'daily-bar-backfill-plan-v1-{plan_hash}.json')

    def _prepared(self,intent: BackfillExecutionIntent) -> dict:
        plan=self._plan(intent.plan_task_id,intent.plan_sha256,owner=intent.owner)
        confirmation=ExecutionConfirmation(kind='backfill',execution_id=intent.execution_id,intent_id=intent.intent_id,
            prepare_command_id=intent.prepare_command_id,plan_hash=intent.plan_sha256,plan_task_id=intent.plan_task_id,
            exact_dates_sha256=intent.exact_dates_sha256,start_date=min(plan.missing_dates),end_date=max(plan.missing_dates),
            missing_date_count=len(plan.missing_dates),expires_at=intent.expires_at)
        return {'outcome':'execution_prepared','confirmation':confirmation.model_dump(mode='json')}

    @staticmethod
    def _financial_prepared(intent: FinancialExecutionIntent) -> dict:
        plan=intent.plan
        confirmation=ExecutionConfirmation(kind='financial',execution_id=intent.execution_id,intent_id=intent.intent_id,
            prepare_command_id=intent.prepare_command_id,plan_hash=plan.content_sha256,start_date=plan.start_date,end_date=plan.end_date,
            security_count=len(plan.securities),query_count=len(plan.queries),report_periods=plan.report_periods,expires_at=intent.expires_at)
        return {'outcome':'execution_prepared','confirmation':confirmation.model_dump(mode='json')}

    def recover(self,command: DataCenterExecutionCommand) -> dict | None:
        self.authorize(command.actor_id)
        if isinstance(command,PrepareFinancialCollection):
            intent=self.state.financial_intent_by_command(command.command_id,owner=command.actor_id)
            if intent is None:
                return None
            if intent.prepare_request_sha256!=canonical_sha256(command.model_dump(mode='python')):
                raise ValueError('same financial prepare command has different content')
            return self._financial_prepared(intent)
        if isinstance(command,ExecuteFinancialCollection):
            spec=self.state.get_financial_execution_spec(command.execution_id,owner=command.actor_id)
            if spec is None:
                return None
            if (spec.execute_command_id,spec.intent.intent_id,spec.intent.prepare_command_id,spec.plan.content_sha256)!=(
                    command.command_id,command.intent_id,command.prepare_command_id,command.plan_hash):
                raise ValueError('financial execute command differs from original admission')
            return {'outcome':'execution_queued','execution_id':spec.execution_id,'manifest_id':spec.manifest_id}
        if isinstance(command,PrepareBackfillExecution):
            intent=self.state.maintenance_intent_by_command(command.command_id,owner=command.actor_id)
            if intent is None:
                return None
            if (intent.plan_task_id,intent.plan_sha256)!=(command.plan_task_id,command.plan_hash):
                raise ValueError('prepared plan command has different content')
            return self._prepared(intent)
        if isinstance(command,ExecuteBackfillPlan):
            spec=self.state.get_backfill_execution_spec(command.execution_id,owner=command.actor_id)
            if spec is None:
                return None
            if (spec.execute_command_id,spec.intent.intent_id,spec.intent.prepare_command_id,
                spec.plan_task_id,spec.plan.content_sha256,spec.intent.exact_dates_sha256)!=(
                command.command_id,command.intent_id,command.prepare_command_id,command.plan_task_id,
                command.plan_hash,command.exact_dates_sha256):
                raise ValueError('execute command differs from original admission')
            return {'outcome':'execution_queued','execution_id':spec.execution_id,'manifest_id':spec.manifest_id}
        action='pause' if isinstance(command,PauseDataCenterExecution) else 'resume'
        result=self.state.maintenance_control_by_command(command.command_id,execution_id=command.execution_id,
            owner=command.actor_id,expected_sequence=command.expected_sequence,action=action)
        return None if result is None else {'outcome':'control_accepted','execution':result.model_dump(mode='json')}

    def submit(self,command: DataCenterExecutionCommand) -> dict:
        previous=self.recover(command)
        if previous is not None:
            return previous
        if isinstance(command,PauseDataCenterExecution):
            result=self.state.maintenance_control(command.execution_id,owner=command.actor_id,command_id=command.command_id,
                expected_sequence=command.expected_sequence,action='pause',now=self.clock())
            return {'outcome':'control_accepted','execution':result.model_dump(mode='json')}
        if isinstance(command,(PrepareFinancialCollection,ExecuteFinancialCollection)):
            return self._submit_financial(command)
        if isinstance(command,ResumeDataCenterExecution):
            status=self.state.get_maintenance_status(command.execution_id,owner=command.actor_id)
            policy=self._policy(kind=status.kind,require_balance=False)
            spec=(self.state.get_financial_execution_spec(command.execution_id,owner=command.actor_id) if status.kind=='financial'
                else self.state.get_backfill_execution_spec(command.execution_id,owner=command.actor_id))
            if spec is None:
                raise ValueError('original execution is unavailable')
            require_stable_execution_policy(policy,spec)
            if status.kind=='financial':
                from rquant.financial_runtime import require_financial_plan_policy
                require_financial_plan_policy(spec.plan,policy,now=self.clock())
            else:
                from rquant.backfill_execute import require_market_plan_policy
                require_market_plan_policy(spec.plan,policy,now=self.clock())
            result=self.state.maintenance_control(command.execution_id,owner=command.actor_id,command_id=command.command_id,
                expected_sequence=command.expected_sequence,action='resume',now=self.clock(),policy_generation=policy.policy_generation)
            return {'outcome':'control_accepted','execution':result.model_dump(mode='json')}
        policy=self._policy()
        plan=self._plan(command.plan_task_id,command.plan_hash,owner=command.actor_id)
        if not plan.missing_dates:
            raise ValueError('original plan has no whole-day gaps')
        from rquant.backfill_execute import require_market_plan_policy
        require_market_plan_policy(plan,policy,now=self.clock())
        if isinstance(command,PrepareBackfillExecution):
            latest=self.audits.latest_success()
            if latest is None or latest.report_hash is None:
                raise ValueError('original verified collection audit is unavailable')
            report=load_data_audit_report(data_audit_report_path(self.config.audit_directory,latest.report_hash))
            if not isinstance(report,CollectionDataAuditReport) or report.collection_proof.replica_sha256!=plan.source.claimed_file_sha256:
                raise ValueError('plan is not bound to the original verified collection source')
            proof=report.collection_proof
            gate=policy.primary_writer_gate
            if (proof.primary_identity.canonical_path,proof.primary_identity.device,proof.primary_identity.inode)!=(
                    str(gate.primary_path),gate.primary_device,gate.primary_inode):
                raise ValueError('plan source differs from current physical primary')
            if proof.audit_start!=plan.audit_start or proof.observed_through!=plan.completed_through:
                raise ValueError('plan and original collection ranges differ')
            for item in plan.evidence.calendar.days:
                if item.is_open!=(item.day in proof.calendar.open_dates):
                    raise ValueError('plan differs from original SSE calendar')
            execution_id=canonical_sha256({'contract':'controlled-backfill-execution/v1','owner':command.actor_id,
                'prepare_command_id':command.command_id,'plan_task_id':command.plan_task_id,'plan_sha256':command.plan_hash,
                'source_binding':proof.binding_sha256})
            intent=BackfillExecutionIntent(execution_id=execution_id,owner=command.actor_id,prepare_command_id=command.command_id,
                plan_task_id=command.plan_task_id,plan_sha256=command.plan_hash,exact_dates_sha256=canonical_sha256(plan.missing_dates),
                source_reference=report.collection_reference,source_generation_id=proof.binding_sha256,
                calendar_sha256=plan.evidence.calendar_sha256,primary_identity=proof.primary_identity,
                policy_generation=policy.policy_generation,nonce_sha256=canonical_sha256({'execution_id':execution_id,'step':'confirm_missing_days'}),
                issued_at=self.clock(),expires_at=self.clock()+timedelta(minutes=5))
            return self._prepared(self.state.persist_maintenance_intent(intent))
        intent=self.state.maintenance_intent_by_command(command.prepare_command_id,owner=command.actor_id)
        if intent is None or (intent.execution_id,intent.intent_id,intent.plan_task_id,intent.plan_sha256,intent.exact_dates_sha256,
            intent.policy_generation)!=(command.execution_id,command.intent_id,command.plan_task_id,command.plan_hash,
                command.exact_dates_sha256,policy.policy_generation):
            raise ValueError('confirmation, current policy or exact plan differs')
        spec=BackfillExecutionSpec(execution_id=intent.execution_id,owner=command.actor_id,plan_task_id=command.plan_task_id,
            plan=plan,intent=intent,execute_command_id=command.command_id,
            manifest_id=canonical_sha256({'contract':'controlled-gap-manifest/v1','execution_id':intent.execution_id}),admission_policy=policy)
        self.state.admit_backfill_execution(spec,build_backfill_execution_manifest(spec),now=self.clock())
        return {'outcome':'execution_queued','execution_id':spec.execution_id,'manifest_id':spec.manifest_id}

    def _submit_financial(self,command: PrepareFinancialCollection | ExecuteFinancialCollection) -> dict:
        from rquant.financial_runtime import FinancialCollectionPlan,FinancialExecutionIntent,FinancialExecutionSpec,build_financial_manifest,require_financial_plan_policy
        from rquant.financial_pit_acquisition import FinancialArchive
        policy=self._policy(kind='financial')
        if self.config.financial_archive_path is None or self.config.replica_path is None:
            raise ValueError('original financial archive and replica are not configured')
        if isinstance(command,ExecuteFinancialCollection):
            intent=self.state.financial_intent_by_command(command.prepare_command_id,owner=command.actor_id)
            if intent is None or (intent.execution_id,intent.intent_id,intent.plan.content_sha256,intent.policy_generation)!=(
                    command.execution_id,command.intent_id,command.plan_hash,policy.policy_generation):
                raise ValueError('financial fixed scope or current confirmation policy changed')
            require_financial_plan_policy(intent.plan,policy,now=self.clock())
            spec=FinancialExecutionSpec(execution_id=intent.execution_id,owner=command.actor_id,manifest_id=intent.plan.manifest_id,
                plan=intent.plan,intent=intent,execute_command_id=command.command_id,admission_policy=policy)
            self.state.admit_financial_execution(spec,build_financial_manifest(spec.plan),now=self.clock())
            return {'outcome':'execution_queued','execution_id':spec.execution_id,'manifest_id':spec.manifest_id}
        latest=self.audits.latest_success()
        if latest is None or latest.report_hash!=command.audit_report_hash:
            raise ValueError('financial preparation requires the current original collection audit')
        report=load_data_audit_report(data_audit_report_path(self.config.audit_directory,latest.report_hash))
        if not isinstance(report,CollectionDataAuditReport) or report.collection_proof.primary_identity.generation_id!=canonical_sha256({
                'canonical_path':str(policy.primary_writer_gate.primary_path),'device':policy.primary_writer_gate.primary_device,'inode':policy.primary_writer_gate.primary_inode}):
            raise ValueError('financial preparation source differs from the actual primary')
        proof=report.collection_proof
        from rquant.screen.replica_source import VerifiedReplicaScreenSource,ScreenReplicaUnavailableError
        primary_path=policy.primary_writer_gate.primary_path
        def primary_identity() -> tuple[int,int]:
            observed=primary_path.stat(follow_symlinks=False)
            if (not stat.S_ISREG(observed.st_mode) or primary_path.resolve(strict=True)!=primary_path
                    or primary_path.is_symlink() or (observed.st_dev,observed.st_ino)!=(
                        proof.primary_identity.device,proof.primary_identity.inode)):
                raise ValueError('financial actual primary physical source changed')
            return observed.st_dev,observed.st_ino
        primary_before=primary_identity()
        source=VerifiedReplicaScreenSource(primary_path=primary_path,replica_path=self.config.replica_path)
        try:
            generation=source._verify()
        except ScreenReplicaUnavailableError as error:
            raise ValueError('financial sealed replica source is unavailable') from error
        before=self.config.replica_path.stat(follow_symlinks=False)
        if (generation.sidecar_sha256!=proof.original_sidecar_sha256
                or generation.replica!=(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
                or generation.replica[:4]!=(proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns)):
            raise ValueError('financial available security source changed after the original audit')
        # The original audited proof seals this bounded inventory between complete file hashes.
        # Later same-inode primary commits are allowed; current content is not reread or rehashed.
        available=proof.available_securities
        if not available or len(available)>8000:
            raise ValueError('original available security inventory is unverified or empty')
        if command.security_scope=='available_securities':
            if command.selected_securities:
                raise ValueError('available security scope cannot carry another selected scope')
            securities=available
        else:
            securities=tuple(sorted(set(command.selected_securities)))
            if not securities or securities!=command.selected_securities or not set(securities)<=set(available):
                raise ValueError('financial selected securities differ from the original available scope')
        archive=FinancialArchive(self.config.financial_archive_path)
        archive_id=archive.committed_page(limit=1).archive_id
        plan=FinancialCollectionPlan.create(owner=command.actor_id,prepare_command_id=command.command_id,
            source_generation_id=report.collection_reference.binding_sha256,primary_identity=report.collection_proof.primary_identity,
            calendar=report.collection_proof.calendar,archive_path=self.config.financial_archive_path,archive_id=archive_id,securities=securities,
            start_date=command.start_date,end_date=command.end_date,report_periods=command.report_periods,code_commit=policy.code_commit)
        require_financial_plan_policy(plan,policy,now=self.clock())
        after=self.config.replica_path.stat(follow_symlinks=False)
        try:
            after_generation=source._verify()
        except ScreenReplicaUnavailableError as error:
            raise ValueError('financial sealed replica source moved during preparation') from error
        if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(
                after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns) or (
                    after_generation!=generation or primary_identity()!=primary_before):
            raise ValueError('financial available security source rotated during preparation')
        issued=self.clock()
        intent=FinancialExecutionIntent(execution_id=plan.execution_id,owner=command.actor_id,prepare_command_id=command.command_id,
            plan=plan,plan_sha256=plan.content_sha256,source_generation_id=plan.source_generation_id,primary_identity=plan.primary_identity,
            policy_generation=policy.policy_generation,nonce_sha256=canonical_sha256(('confirm-financial',plan.execution_id)),
            issued_at=issued,expires_at=issued+timedelta(minutes=5),
            prepare_request_sha256=canonical_sha256(command.model_dump(mode='python')))
        return self._financial_prepared(self.state.persist_maintenance_intent(intent))
