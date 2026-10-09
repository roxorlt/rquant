"""Only the original installed source gate and complete sealed reader supply views."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from uuid import UUID
from typing import TYPE_CHECKING

from rquant.lab_artifact_preview import ArtifactPreviewReader, ArtifactPreviewUnavailableError
from rquant.lab_jobs import JobStatus, LabJobListFilters
from rquant.minute_backtest_artifact import MINUTE_RESULT_TABLE_NAMES, MinuteSealedReplayReader, MinuteSealedReplayResult
from rquant.minute_backtest_formal_adapter import MinuteFormalReplayAdapter
from rquant.minute_backtest_installation import InstalledMinuteReplay, load_minute_replay_installation
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter
from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader, MinuteParameterSealedReplayResult
from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy
from rquant.web.models.collaboration import MinuteStudyJournalFact
from rquant.minute_backtest_parameter_producer import (
    MinuteParameterFactSourceReference, MinuteParameterPreparedPublication, MinuteParameterPublicationReceipt,
)
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_request
from rquant.minute_backtest_producer import MinutePublicationReceipt
from rquant.minute_backtest_performance import build_minute_performance
from rquant.minute_backtest_report import MinuteHtmlReport
from rquant.research_run_spec import ResearchRunSpec
from rquant.strategy_catalog_source import _NAMES
from rquant.web.collaboration_gateway import CollaborationGateway
from rquant.web.models.minute_backtests import (
    MinuteCapabilities, MinuteJob, MinuteJobsData, MinuteNavData, MinuteNavPoint, MinutePriceTime, MinuteResultRow,
    MinuteRowsData, MinuteSourceOption, MinuteSourceProvenance, MinuteSourcesData, MinuteSummaryData, MinuteTableName,
    MinuteParameterCapability, MinuteParameterFactSourceOption, MinuteParameterJob,
    MinuteParameterResultSource, MinuteParameterSourcesData,
    MinuteStudiesData, MinuteStudyCapabilitiesData, MinuteStudySourceCapability, MinuteStudyScoreProfile,
    MinuteStudyListItem, MinuteStudyCreateRequest, MinuteStudyResultData, MinuteStudyTrialData,
    MinuteStudyWindowData, MinuteStudyHeatmapData,
)


if TYPE_CHECKING:
    from rquant.minute_backtest_native_report_runtime import MinuteNativeReportRuntime
    from rquant.minute_backtest_export import MinuteReportReader
    from rquant.minute_backtest_parameter_study_execution import (
        MinuteParameterStudyExecutionEffect, MinuteParameterStudyExecutionResult, MinuteParameterStudyWindowObservation,
    )


def public_study_request(command: SubmitMinuteParameterStudy) -> MinuteStudyCreateRequest:
    from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy

    if type(command) is not SubmitMinuteParameterStudy:
        raise TypeError("public study requires its complete original parent command")
    value = command.request
    return MinuteStudyCreateRequest(command_id=value.request_id, requested_at=value.requested_at,
        source_key=value.source_key, source_version=value.source_version, full_input_hash=value.full_input_hash,
        parameters=value.parameters, protocol=value.formal_protocol, settings=value.settings,
        random_seed=value.random_seed, deadline=value.deadline, mode=value.mode, search=value.search,
        walk_forward=value.walk_forward)


def project_study_result(effect: MinuteParameterStudyExecutionEffect, result: MinuteParameterStudyExecutionResult, *, parent_status: str) -> MinuteStudyResultData:
    from rquant.minute_backtest_commands import _minute_config_hash
    from rquant.minute_backtest_parameter_study_execution import (
        MinuteParameterStudyExecutionEffect, MinuteParameterStudyExecutionResult,
    )

    if type(effect) is not MinuteParameterStudyExecutionEffect or type(result) is not MinuteParameterStudyExecutionResult:
        raise TypeError("study projection requires the original complete typed effect and read")
    if effect.plan != result.plan:
        raise PermissionError("study complete read belongs to another original parent")
    prepared = {item.trial.index: item for item in effect.prepared}
    sealed = {item.prepared.trial.index: item for item in result.results}
    ranks = {item.study_id: item for item in result.training_ranks}
    states = {item.index: item for item in result.trial_states}

    def window(value: MinuteParameterStudyWindowObservation) -> MinuteStudyWindowData:
        return MinuteStudyWindowData(window=value.window, status=value.status, summary=value.summary,
            daily=value.daily, cross_window_trades=value.cross_window_trades,
            unavailable_reasons=value.unavailable_reasons)

    trials = []
    for trial in result.plan.trials:
        ref, value, state = prepared.get(trial.index), sealed.get(trial.index), states[trial.index]
        binding = None if ref is None else ref.binding
        trials.append(MinuteStudyTrialData(index=trial.index, fold=trial.fold, label=trial.label,
            job_id=trial.job_id, state=state.state, parameters=trial.command.config.parameters,
            settings=trial.command.config.study, protocol=trial.command.config.protocol,
            request_hash=_minute_config_hash(trial.command.config), study_id=None if binding is None else binding.study_id,
            full_input_hash=None if ref is None else ref.full_input_hash,
            core_input_hash=None if ref is None else ref.core_input_hash, seed_hash=None if ref is None else ref.seed_hash,
            profile_hash=None if ref is None else ref.profile_hash,
            spec_hash=None if value is None else value.spec_hash, manifest_hash=None if value is None else value.manifest_hash,
            complete_result_hash=None if value is None else value.complete_result_hash,
            result_hash=None if value is None else value.result_hash,
            completed_at=None if value is None else value.completed_at,
            training=None if value is None else window(value.training),
            validation=None if value is None else window(value.validation),
            out_of_sample=None if value is None else window(value.independent_test),
            training_rank=None if binding is None else ranks.get(binding.study_id),
            unavailable_reasons=() if state.reason is None else (state.reason,)))
    return MinuteStudyResultData(command_id=UUID(effect.command.command_id), request=public_study_request(effect.command),
        status="failed" if parent_status == "failed" else result.state,
        plan_id=result.plan.plan_id, trial_count=result.plan.trial_count, read_at=result.read_at,
        trials=tuple(trials), missing_trial_indices=result.missing_trial_indices,
        unavailable_reasons=result.unavailable_reasons,
        message="部分结果尚未封存。" if result.missing_trial_indices else None)


def source_option(receipt: MinutePublicationReceipt) -> MinuteSourceOption:
    value, source = receipt.frozen, receipt.frozen.provenance
    runtime, policy = value.runtime, source.visibility_policy
    return MinuteSourceOption(source_key=runtime.source_key, source_version=runtime.source_version,
        full_input_hash=value.full_input_hash, core_input_hash=value.core_input_hash, seed_hash=receipt.seed.seed_hash,
        native_id=runtime.strategy.strategy_id, native_name=_NAMES[runtime.strategy.strategy_id], native_version=runtime.strategy.strategy_version,
        native_registration_hash=value.native_registration.record_hash,
        native_executable_fingerprint=value.native_registration.executable_fingerprint,
        wrapper_registration_hash=value.wrapper_registration.record_hash, profile_hash=runtime.execution_profile.profile_hash,
        dataset_snapshot_id=runtime.dataset_snapshot_id, start_date=runtime.start_date, end_date=runtime.end_date,
        work_units=value.formal_work.work_units, provenance=MinuteSourceProvenance(source_kind=source.source_kind,
            extracted_at=source.extracted_at, published_at=source.published_at,
            replay_start=source.replay_start, replay_end=source.replay_end,
            acquisition_commits=tuple(sorted({x.acquisition_commit for x in source.capture_lineage})),
            real_capture_times=tuple(sorted({x.captured_at for x in source.capture_lineage if x.captured_at is not None})),
            research_code_commit=source.research_code_commit, visibility_policy_id=None if policy is None else policy.policy_id,
            visibility_policy_version=None if policy is None else policy.version,
            visibility_limitations=None if policy is None else policy.limitations))


def parameter_source_option(receipt: MinuteParameterPublicationReceipt,
    prepared: MinuteParameterPreparedPublication, baseline: MinuteParameterFactSourceReference,
) -> MinuteParameterResultSource:
    value, runtime = receipt.frozen, receipt.frozen.runtime
    if (prepared.source_key, prepared.source_version, prepared.owner_id, prepared.full_input_hash,
        prepared.core_input_hash, prepared.seed_hash, prepared.parameter_hash) != (
        runtime.source_key, runtime.source_version, runtime.owner_id, value.full_input_hash,
        value.core_input_hash, receipt.seed.seed_hash, runtime.parameters.fingerprint):
        raise PermissionError("parameter result source differs from its full prepared publication")
    if baseline.fact_identity != prepared.baseline:
        raise PermissionError("parameter result baseline differs from its complete installed reference")
    source, policy = value.provenance, value.provenance.visibility_policy
    return MinuteParameterResultSource(source_key=runtime.source_key, source_version=runtime.source_version,
        full_input_hash=value.full_input_hash, core_input_hash=value.core_input_hash, seed_hash=receipt.seed.seed_hash,
        native_id=runtime.strategy.strategy_id, native_name=_NAMES[runtime.parameters.parameters.family],
        native_version=runtime.strategy.strategy_version, family=runtime.parameters.parameters.family,
        parameters=runtime.parameters, parameter_hash=runtime.parameters.fingerprint,
        native_registration_hash=value.native_registration.record_hash,
        native_executable_fingerprint=value.native_registration.executable_fingerprint,
        wrapper_registration_hash=value.wrapper_registration.record_hash, profile_hash=runtime.execution_profile.profile_hash,
        dataset_snapshot_id=runtime.dataset_snapshot_id, start_date=runtime.start_date, end_date=runtime.end_date,
        work_units=prepared.work_units, baseline_source_key=prepared.baseline.source_key,
        baseline_source_version=prepared.baseline.source_version, baseline_full_input_hash=prepared.baseline.full_input_hash,
        source_nature=baseline.source_nature,
        provenance=MinuteSourceProvenance(source_kind=source.source_kind, extracted_at=source.extracted_at,
            published_at=source.published_at, replay_start=source.replay_start, replay_end=source.replay_end,
            acquisition_commits=tuple(sorted({x.acquisition_commit for x in source.capture_lineage})),
            real_capture_times=tuple(sorted({x.captured_at for x in source.capture_lineage if x.captured_at is not None})),
            research_code_commit=source.research_code_commit, visibility_policy_id=None if policy is None else policy.policy_id,
            visibility_policy_version=None if policy is None else policy.version,
            visibility_limitations=None if policy is None else policy.limitations))


class _InstalledStudyReplayReader(MinuteParameterSealedReplayReader):
    def __init__(self, installation: InstalledMinuteReplay, initial_spec: ResearchRunSpec, *,
        defer_facade: bool = False) -> None:
        self.installation = installation
        if defer_facade:
            self.reader = installation.reader
            self.artifact_reader = ArtifactPreviewReader(reader=installation.reader,
                artifact_root=installation.authority.final_artifact_root)
            self.catalog = self._catalog_model().model_validate(
                installation.profile.parameter_catalog.model_dump(mode="python"))
            self.submission_facade = None
            return
        super().__init__(reader=installation.reader,
            artifact_reader=ArtifactPreviewReader(reader=installation.reader,
                artifact_root=installation.authority.final_artifact_root),
            submission_facade=installation.parameter_submission_facade(initial_spec),
            catalog=installation.profile.parameter_catalog)

    def read(self, job_id: UUID, *, owner_id: str, native_id: str,
        native_version: int, as_of: datetime,
    ) -> MinuteParameterSealedReplayResult | None:
        context = self.reader.get_command_context(job_id)
        if context is None:
            return None
        # Each complete recipe owns a different executable registration. Resolve
        # its original facade from the actual Lab spec before the full reader.
        reader = MinuteParameterSealedReplayReader(reader=self.reader,
            artifact_reader=self.artifact_reader,
            submission_facade=self.installation.parameter_submission_facade(context.job.spec),
            catalog=self.catalog)
        return reader.read(job_id, owner_id=owner_id, native_id=native_id,
            native_version=native_version, as_of=as_of)


class MinuteWebService:
    def __init__(self, installation: InstalledMinuteReplay | None, *,
        native_report_runtime: MinuteNativeReportRuntime | None = None,
        study_projection_authority: Path | None = None, study_projection_expected_sha256: str | None = None,
    ) -> None:
        if installation is None and native_report_runtime is None:
            raise TypeError("minute views require an original installed or native report authority")
        self.installation = installation
        if (study_projection_authority is None) != (study_projection_expected_sha256 is None):
            raise ValueError("minute projection requires its exact configured authority SHA")
        if study_projection_authority is not None and installation is None:
            raise ValueError("minute projection requires its original installation")
        self.study_projection_authority = study_projection_authority
        self.study_projection_expected_sha256 = study_projection_expected_sha256
        self.native_report_runtime = native_report_runtime
        self.reader = installation.reader if installation is not None else native_report_runtime.reader
        self.results = None if installation is None or installation.profile.catalog is None else MinuteSealedReplayReader(reader=self.reader,
            artifact_reader=ArtifactPreviewReader(reader=self.reader, artifact_root=installation.authority.final_artifact_root),
            submission_facade=installation.commands, catalog=installation.profile.catalog)

    @minute_parameter_validation_request
    def sources(self, *, owner_id: str) -> MinuteSourcesData:
        if self.installation is None:
            self.native_report_runtime.verify_current()
            return MinuteSourcesData(available=False, message="当前仅提供已封存分钟研究报告。")
        self.installation.verify_current()
        sources, unavailable = [], 0
        catalog = self.installation.profile.catalog
        for reference in (() if catalog is None else catalog.entries):
            if reference.owner_id != owner_id:
                continue
            try:
                published = self.installation.publication(source_key=reference.source_key,
                    source_version=reference.source_version, owner_id=owner_id)
                sources.append(source_option(published.receipt))
            except (OSError, PermissionError, ValueError, RuntimeError):
                unavailable += 1
        self.installation.verify_current()
        return MinuteSourcesData(available=True, sources=tuple(sources), unavailable_count=unavailable,
            message=None if sources else "尚无可用分钟来源，请先准备完整发布资料。")

    @minute_parameter_validation_request
    def capabilities(self, *, owner_id: str, can_write: bool) -> MinuteCapabilities:
        if self.installation is None:
            self.native_report_runtime.verify_current()
            return MinuteCapabilities(available=True, can_run=False, can_export=can_write,
                source_count=0, source_unavailable_count=0, message="当前仅提供已封存分钟研究报告。")
        sources = self.sources(owner_id=owner_id)
        parameters = self.parameter_sources(owner_id=owner_id)
        count = len(sources.sources) + len(parameters.sources)
        return MinuteCapabilities(available=sources.available or parameters.available, can_run=can_write and count > 0,
            can_export=can_write, source_count=count,
            source_unavailable_count=sources.unavailable_count + parameters.unavailable_count,
            message=None if count else sources.message)

    @minute_parameter_validation_request
    def parameter_sources(self, *, owner_id: str) -> MinuteParameterSourcesData:
        if self.installation is None:
            self.native_report_runtime.verify_current()
            return MinuteParameterSourcesData(available=False, message="尚无已安装的完整参数研究来源。")
        self.installation.verify_current()
        catalog = self.installation.profile.parameter_catalog
        if catalog is None:
            return MinuteParameterSourcesData(available=False, message="尚无已安装的完整参数研究来源。")
        sources, unavailable = [], 0
        for reference in catalog.fact_sources:
            if reference.owner_id != owner_id:
                continue
            try:
                receipt = catalog.resolve_fact(source_key=reference.source_key, source_version=reference.source_version,
                    owner_id=owner_id, full_input_hash=reference.full_input_hash)
                runtime, provenance = receipt.frozen.runtime, receipt.frozen.provenance
                if runtime.producer_commit != self.installation.profile.code_sha or provenance.published_at > self.installation.clock():
                    raise PermissionError("parameter installed fact code or actual publication time differs")
                policy = provenance.visibility_policy
                sources.append(MinuteParameterFactSourceOption(source_key=reference.source_key,
                    source_version=reference.source_version, full_input_hash=reference.full_input_hash,
                    display_name=reference.display_name, start_date=runtime.start_date, end_date=runtime.end_date,
                    frequency=runtime.source_frequency, source_nature=reference.source_nature,
                    capabilities=(MinuteParameterCapability(family=runtime.parameters.parameters.family,
                        display_name=_NAMES[runtime.parameters.parameters.family], default_parameters=runtime.parameters,
                        supported_parameter_names=reference.supported_parameter_names),),
                    provenance=MinuteSourceProvenance(source_kind=provenance.source_kind,
                        extracted_at=provenance.extracted_at, published_at=provenance.published_at,
                        replay_start=provenance.replay_start, replay_end=provenance.replay_end,
                        acquisition_commits=tuple(sorted({x.acquisition_commit for x in provenance.capture_lineage})),
                        real_capture_times=tuple(sorted({x.captured_at for x in provenance.capture_lineage if x.captured_at is not None})),
                        research_code_commit=provenance.research_code_commit, visibility_policy_id=None if policy is None else policy.policy_id,
                        visibility_policy_version=None if policy is None else policy.version,
                        visibility_limitations=None if policy is None else policy.limitations)))
            except (OSError, PermissionError, ValueError, RuntimeError):
                unavailable += 1
        self.installation.verify_current()
        return MinuteParameterSourcesData(available=True, sources=tuple(sources), unavailable_count=unavailable,
            message=None if sources else "尚无可用的完整参数研究来源。")

    @minute_parameter_validation_request
    def study_capabilities(self, *, owner_id: str, can_write: bool) -> MinuteStudyCapabilitiesData:
        if self.installation is None:
            self.native_report_runtime.verify_current()
            return MinuteStudyCapabilitiesData(available=False, can_run=False,
                message="尚无已安装的完整参数研究来源。")
        from rquant.minute_backtest_parameter_fact_sources import _candidate_facts
        from rquant.minute_backtest_parameter_search import _axis_adapter
        from rquant.minute_backtest_parameter_study_features import _DYNAMIC_FEATURES
        from rquant.topn_selection import default_score_profiles

        choices = self.parameter_sources(owner_id=owner_id)
        catalog = self.installation.profile.parameter_catalog
        if catalog is None:
            return MinuteStudyCapabilitiesData(available=False, can_run=False, message=choices.message)
        sources = []
        for choice in choices.sources:
            receipt = catalog.resolve_fact(source_key=choice.source_key, source_version=choice.source_version,
                owner_id=owner_id, full_input_hash=choice.full_input_hash)
            candidates = tuple(_candidate_facts(receipt.frozen).values())
            known = set.intersection(*(set(value.static_factors) for value in candidates)) if candidates else set()
            profiles = []
            for profile in default_score_profiles():
                required = {term.name for term in profile.terms}
                if profile.env_gate is not None:
                    required.add(profile.env_gate.feature)
                missing = tuple(sorted(required - known - _DYNAMIC_FEATURES))
                profiles.append(MinuteStudyScoreProfile(name=profile.name, label=profile.label,
                    available=bool(candidates) and not missing, missing_features=missing))
            capability = choice.capabilities[0]
            searchable, heatmap = [], []
            for name in capability.supported_parameter_names:
                try:
                    _axis_adapter(capability.default_parameters, name)
                except ValueError:
                    continue
                searchable.append(name)
                value = capability.default_parameters.parameters
                for part in name.split("."):
                    value = getattr(value, part)
                if type(value) in (bool, int, float):
                    heatmap.append(name)
            modes = ("single", "grid", "random", "walk_forward")
            if capability.family == "growth_board_surge":
                modes += ("ablation",)
            sources.append(MinuteStudySourceCapability(source=choice, modes=modes,
                score_profiles=tuple(profiles), searchable_parameter_names=tuple(searchable),
                heatmap_parameter_names=tuple(heatmap),
                unavailable_reasons=() if any(p.available for p in profiles) else ("score_features_unavailable",)))
        self.installation.verify_current()
        return MinuteStudyCapabilitiesData(available=choices.available,
            can_run=can_write and any(not item.unavailable_reasons for item in sources), sources=tuple(sources),
            source_unavailable_count=choices.unavailable_count, message=choices.message)

    @minute_parameter_validation_request
    def studies(self, *, owner_id: str, limit: int, cursor: str | None,
        collaboration: CollaborationGateway) -> MinuteStudiesData:
        from rquant.command_audit_projection import CommandAuditQuery
        from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudySubmissionReceipt

        if self.installation is None:
            self.native_report_runtime.verify_current()
            return MinuteStudiesData(available=False, message="尚无已安装的完整参数研究来源。")
        self.installation.verify_current()
        page = collaboration.audit(owner_id, CommandAuditQuery(limit=limit, actor_id=owner_id,
            command_kind="submit_minute_parameter_study", cursor=cursor))
        items = []
        for audit in page.items:
            fact = collaboration.study_journal(owner_id, command_id=UUID(audit.command_id), include_admission=False)
            if (fact.command_hash, fact.command.actor_id) != (audit.command_hash, owner_id):
                raise PermissionError("study list differs from its complete original owner journal")
            value = fact.command.request
            receipt = None if fact.status != "succeeded" else MinuteParameterStudySubmissionReceipt.model_validate_json(
                json.dumps(fact.result))
            if receipt is not None and receipt.parent_command_id != fact.command.command_id:
                raise PermissionError("study list terminal receipt belongs to another parent")
            items.append(MinuteStudyListItem(command_id=value.request_id, mode=value.mode,
                family=value.parameters.parameters.family, display_name=_NAMES[value.parameters.parameters.family],
                source_key=value.source_key, source_version=value.source_version, full_input_hash=value.full_input_hash,
                status=(receipt.state if receipt is not None else
                    {"ambiguous": "unknown"}.get(fact.status, fact.status)),
                requested_at=value.requested_at, completed_at=fact.completed_at,
                plan_id=None if receipt is None else receipt.plan_id,
                trial_count=None if receipt is None else len(receipt.receipts),
                submitted_count=0 if receipt is None else len(receipt.receipts)))
        self.installation.verify_current()
        return MinuteStudiesData(available=True, studies=tuple(items), next_cursor=page.next_cursor)

    def _study_execution(self, command_id: UUID, *, owner_id: str,
        collaboration: CollaborationGateway) -> tuple[MinuteStudyJournalFact, MinuteParameterStudyExecutionEffect | None, MinuteParameterStudyExecutionResult | None]:
        from rquant.minute_backtest_commands import MinuteCommandWriter
        from rquant.minute_backtest_parameter_study_execution import (
            MinuteParameterStudyExecutionEffect, read_minute_parameter_study_execution,
        )
        from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudyCommandWriter
        from rquant.strict_json import strict_model_validate_json

        if self.installation is None:
            raise LookupError("original installed parameter study authority is unavailable")
        self.installation.verify_current()
        fact = collaboration.study_journal(owner_id, command_id=command_id)
        if fact.admission_json is None:
            self.installation.verify_current()
            return fact, None, None
        effect = strict_model_validate_json(MinuteParameterStudyExecutionEffect, fact.admission_json)
        if effect.command != fact.command:
            raise PermissionError("study original admission belongs to another complete parent")
        fresh = load_minute_replay_installation(self.installation.reference.path,
            expected_code_sha=self.installation.profile.code_sha, clock=self.installation.clock)
        if (fresh.profile, fresh.reference, fresh.authority) != (
            self.installation.profile, self.installation.reference, self.installation.authority):
            raise PermissionError("study complete installed authority changed")
        # Reuse the original source verifier with a read-only installation. Neither
        # writer constructor opens an ArtifactStore, creates directories, or submits.
        MinuteParameterStudyCommandWriter(MinuteCommandWriter(fresh))._verify_baseline(effect.plan)
        if effect.prepared:
            reader = (_InstalledStudyReplayReader(fresh, effect.prepared[0].marker.command.spec)
                if self.study_projection_authority is None else
                _InstalledStudyReplayReader(fresh, effect.prepared[0].marker.command.spec, defer_facade=True))
            if self.study_projection_authority is None:
                result = read_minute_parameter_study_execution(effect.plan, prepared=effect.prepared,
                    reader=reader, as_of=self.installation.clock())
            else:
                from rquant.minute_backtest_parameter_study_projection import load_minute_study_projection

                with load_minute_study_projection(fresh, self.study_projection_authority,
                    expected_sha256=self.study_projection_expected_sha256, writable=False) as projection:
                    result = read_minute_parameter_study_execution(effect.plan, prepared=effect.prepared,
                        reader=reader, as_of=self.installation.clock(), projection=projection)
        else:
            result = None
        after = collaboration.study_journal(owner_id, command_id=command_id)
        if after != fact:
            raise ValueError("study original journal changed during the complete result read")
        self.installation.verify_current()
        return fact, effect, result

    @minute_parameter_validation_request
    def study(self, command_id: UUID, *, owner_id: str,
        collaboration: CollaborationGateway) -> MinuteStudyResultData:
        fact, effect, result = self._study_execution(command_id, owner_id=owner_id, collaboration=collaboration)
        if result is not None:
            return project_study_result(effect, result, parent_status=fact.status)
        status = {"ambiguous": "unknown", "succeeded": "submitted"}.get(fact.status, fact.status)
        if effect is not None and effect.plan.state == "unavailable":
            status = "unavailable"
        return MinuteStudyResultData(command_id=command_id, request=public_study_request(fact.command),
            status=status, plan_id=None if effect is None else effect.plan.plan_id,
            trial_count=None if effect is None else effect.plan.trial_count,
            unavailable_reasons=() if effect is None else effect.plan.unavailable_reasons,
            message="研究请求未完成。" if status == "failed" else "研究正在准备或等待原任务结果。")

    @minute_parameter_validation_request
    def study_heatmap(self, command_id: UUID, *, owner_id: str, current_trial_index: int,
        x_parameter: str, y_parameter: str, collaboration: CollaborationGateway) -> MinuteStudyHeatmapData:
        from rquant.minute_backtest_parameter_study_execution import build_minute_parameter_study_execution_heatmap

        fact, effect, result = self._study_execution(command_id, owner_id=owner_id, collaboration=collaboration)
        if result is None or effect is None or fact.status != "succeeded":
            raise ArtifactPreviewUnavailableError("study original complete results are unavailable")
        heatmap = build_minute_parameter_study_execution_heatmap(result, current_trial_index=current_trial_index,
            x_parameter=x_parameter, y_parameter=y_parameter)
        return MinuteStudyHeatmapData(command_id=command_id, plan_id=effect.plan.plan_id,
            read_at=result.read_at, heatmap=heatmap)

    @minute_parameter_validation_request
    def job(self, job_id: UUID, *, owner_id: str) -> MinuteJob | MinuteParameterJob:
        if self.installation is None:
            raise LookupError("original installed minute run authority is unavailable")
        self.installation.verify_current()
        context = self.reader.get_command_context(job_id)
        if context is None or context.job.spec.parameters.strategy_name not in {"minute_runtime_replay", "minute_parameter_replay"}:
            raise LookupError("minute job is unavailable")
        job = context.job
        arguments = {item.name: item.value for item in job.spec.parameters.arguments}
        if arguments.get("owner_id") != owner_id:
            raise LookupError("minute job owner differs")
        is_parameter = job.spec.parameters.strategy_name == "minute_parameter_replay"
        if is_parameter:
            if self.installation.profile.parameter_catalog is None:
                raise LookupError("minute parameter catalog is unavailable")
            parameters = MinuteParameterFormalReplayAdapter(self.installation.profile.parameter_catalog).parameters(job.spec)
            recipe = MinuteParameterSet.model_validate_json(parameters.parameter_set_json)
            model, extra = MinuteParameterJob, {"family": recipe.parameters.family, "parameters": recipe,
                "parameter_hash": recipe.fingerprint}
            name = _NAMES[recipe.parameters.family]
        else:
            if self.installation.profile.catalog is None:
                raise LookupError("minute native run catalog is unavailable")
            parameters = MinuteFormalReplayAdapter(self.installation.profile.catalog).parameters(job.spec)
            model, extra = MinuteJob, {}
            name = _NAMES[parameters.native_strategy_id]
        authority = self.reader.get_artifact_preview_authority(job_id)
        status = ("completed" if authority is not None else "sealing") if job.status is JobStatus.SUCCEEDED else {
            JobStatus.QUEUED: "queued", JobStatus.RUNNING: "running", JobStatus.CHECKPOINTED: "paused",
            JobStatus.CANCELLED: "cancelled", JobStatus.FAILED: "failed"}[job.status]
        self.installation.verify_current()
        return model(job_id=job_id, status=status, version=job.version, created_at=job.created_at,
            updated_at=job.updated_at, spec_hash=job.spec_hash, source_key=parameters.source_key,
            source_version=parameters.source_version, full_input_hash=parameters.full_input_hash,
            native_id=parameters.native_strategy_id, native_name=name, native_version=parameters.native_strategy_version,
            start_date=job.spec.parameters.start_date, end_date=job.spec.parameters.end_date,
            result_hash=None if authority is None else authority.evidence.complete_result_hash, **extra)

    @minute_parameter_validation_request
    def jobs(self, *, owner_id: str, limit: int, cursor: str | None) -> MinuteJobsData:
        if self.installation is None:
            self.native_report_runtime.verify_current()
            return MinuteJobsData(available=False, message="当前仅提供已封存分钟研究报告。")
        self.installation.verify_current()
        page = self.reader.list_jobs(filters=LabJobListFilters(keyword="minute_"), limit=limit, cursor=cursor)
        selected = []
        for item in page.items:
            try:
                selected.append(self.job(item.job_id, owner_id=owner_id))
            except LookupError:
                continue
        return MinuteJobsData(available=True, jobs=tuple(selected), next_cursor=page.next_cursor)

    @minute_parameter_validation_request
    def read_result(self, job_id: UUID, *, owner_id: str,
        result_hash: str | None = None,
    ) -> MinuteSealedReplayResult | MinuteParameterSealedReplayResult:
        job = self.job(job_id, owner_id=owner_id)
        if result_hash is not None and job.result_hash != result_hash:
            raise ValueError("minute selected sealed result changed")
        # A queued request can precede legitimate original registry/WAL writes.
        # Bind the current read from the same complete installed authority rather
        # than retaining a directory generation from before that writer finished.
        fresh = load_minute_replay_installation(self.installation.reference.path,
            expected_code_sha=self.installation.profile.code_sha, clock=self.installation.clock)
        if (fresh.profile, fresh.reference, fresh.authority) != (
            self.installation.profile, self.installation.reference, self.installation.authority):
            raise PermissionError("minute installed complete authority changed before sealed read")
        artifacts = ArtifactPreviewReader(reader=fresh.reader, artifact_root=fresh.authority.final_artifact_root)
        if type(job) is MinuteParameterJob:
            context = fresh.reader.get_command_context(job_id)
            if context is None:
                raise LookupError("minute parameter original job is unavailable")
            results = MinuteParameterSealedReplayReader(reader=fresh.reader, artifact_reader=artifacts,
                submission_facade=fresh.parameter_submission_facade(context.job.spec), catalog=fresh.profile.parameter_catalog)
        else:
            results = MinuteSealedReplayReader(reader=fresh.reader, artifact_reader=artifacts,
                submission_facade=fresh.commands, catalog=fresh.profile.catalog)
        read = results.read(job_id, owner_id=owner_id, native_id=job.native_id, native_version=job.native_version,
            as_of=self.installation.clock())
        if read is None:
            raise ArtifactPreviewUnavailableError("minute original finalizer seal is unavailable")
        if job.result_hash != read.complete_result_hash:
            raise ValueError("minute complete result changed during read")
        self.installation.verify_current()
        return read

    @minute_parameter_validation_request
    def summary(self, job_id: UUID, *, owner_id: str) -> MinuteSummaryData:
        job = self.job(job_id, owner_id=owner_id)
        if type(job) is MinuteParameterJob:
            context = self.reader.get_command_context(job_id)
            if context is None or self.installation.profile.parameter_catalog is None:
                raise LookupError("minute parameter original job or catalog is unavailable")
            adapter = MinuteParameterFormalReplayAdapter(self.installation.profile.parameter_catalog)
            parameters = adapter.parameters(context.job.spec)
            if parameters.prepared_publication_json is None:
                raise PermissionError("parameter result has no complete baseline/prepared publication binding")
            prepared = MinuteParameterPreparedPublication.model_validate_json(parameters.prepared_publication_json)
            receipt = adapter.expected(parameters)
            baselines = tuple(item for item in self.installation.profile.parameter_catalog.fact_sources
                if item.fact_identity == prepared.baseline)
            if len(baselines) != 1:
                raise PermissionError("parameter selected result has no exact installed baseline")
            selected_source = parameter_source_option(receipt, prepared, baselines[0])
        else:
            receipt = self.installation.publication(source_key=job.source_key, source_version=job.source_version,
                owner_id=owner_id, full_input_hash=job.full_input_hash).receipt
            selected_source = source_option(receipt)
        if job.status != "completed":
            return MinuteSummaryData(job=job, source=selected_source, message="结果尚未保存完成。")
        read = self.read_result(job_id, owner_id=owner_id, result_hash=job.result_hash)
        replay = read.result.replay
        if read.result.publication != receipt:
            raise PermissionError("minute complete publication changed during selected result read")
        return MinuteSummaryData(job=job, source=selected_source, result_hash=read.complete_result_hash,
            daily_status=replay.daily_status, signal_count=len(replay.signals), order_count=len(replay.orders),
            fill_count=len(replay.fills), queue_count=len(replay.queue_records), execution_profile=replay.execution_profile,
            performance=build_minute_performance(replay, runtime=read.result.publication.frozen.runtime), can_report=True,
            tables=MINUTE_RESULT_TABLE_NAMES, message=None if replay.daily_status == "complete" else "部分交易日缺少可验证估值，净值有缺口。")

    @minute_parameter_validation_request
    def report(self, job_id: UUID, *, owner_id: str, result_hash: str,
        collaboration: CollaborationGateway) -> MinuteHtmlReport:
        return self._report_reader(job_id, owner_id=owner_id, collaboration=collaboration).read(
            job_id, owner_id=owner_id, expected_result_hash=result_hash).report

    def _report_reader(self, job_id: UUID, *, owner_id: str,
        collaboration: CollaborationGateway,
    ) -> MinuteReportReader:
        from rquant.minute_backtest_export import MinuteReportReader
        from rquant.minute_experiment_result_owner import MinuteExperimentProvenance
        from rquant.web.models.collaboration import ResultOwnerProof

        native = self.native_report_runtime
        if native is None:
            if self.installation is None:
                raise LookupError("original minute report authority is unavailable")
            return MinuteReportReader(self.installation, owner_authority=collaboration)
        if self.installation is not None:
            original_context = self.installation.reader.get_command_context(job_id)
            if original_context is not None:
                try:
                    proof = collaboration.result_owner(owner_id, domain="minute", job_id=str(job_id),
                        spec_hash=original_context.job.spec_hash)
                except LookupError:
                    pass
                else:
                    if type(proof) is not ResultOwnerProof:
                        raise PermissionError("original submitted minute report lacks its own proof")
                    return MinuteReportReader(self.installation, owner_authority=collaboration)
        native.verify_current()
        context = native.reader.get_command_context(job_id)
        if context is None:
            raise LookupError("original native report job is unavailable")
        proof = collaboration.minute_report_owner(owner_id, job_id=str(job_id), spec_hash=context.job.spec_hash)
        if type(proof) is MinuteExperimentProvenance:
            return MinuteReportReader(None, owner_authority=collaboration, native_reader=native.replay_reader(job_id))
        if type(proof) is ResultOwnerProof and self.installation is not None:
            raise PermissionError("original submitted minute report is absent from its own Lab")
        raise PermissionError("minute report has no matching independent original authority")

    @minute_parameter_validation_request
    def export_bytes(self, job_id: UUID, *, request_id: UUID, owner_id: str, result_hash: str,
        collaboration: CollaborationGateway) -> bytes:
        from rquant.minute_backtest_export import MinuteZipExportFacade
        # GET binds only the already installed private directory, with no artifact
        # writer, namespace recovery, directory creation or temporary publication.
        report_reader = self._report_reader(job_id, owner_id=owner_id, collaboration=collaboration)
        export_root = (self.installation.profile.runtime_root / "exports" / "minute-reports"
            if report_reader.installation is not None else self.native_report_runtime.export_root)
        exports = MinuteZipExportFacade(reader=report_reader.reader, artifact_store=None,
            read_only=True, report_reader=report_reader, export_root=export_root)
        receipt = exports.recover_minute(job_id, request_id=request_id, owner_id=owner_id, expected_result_hash=result_hash)
        if receipt is None:
            raise LookupError("minute complete ZIP is not yet published")
        return exports.read_bytes(receipt, owner_id=owner_id)

    def nav(self, job_id: UUID, *, owner_id: str, result_hash: str) -> MinuteNavData:
        read = self.read_result(job_id, owner_id=owner_id, result_hash=result_hash)
        replay = read.result.replay
        return MinuteNavData(job_id=job_id, result_hash=result_hash, daily_status=replay.daily_status,
            points=tuple(MinuteNavPoint(trade_date=item.trade_date, as_of=item.as_of, basis=item.basis,
                status=item.status, nav=None if item.account is None else item.account.nav,
                cash=None if item.account is None else item.account.cash,
                account_snapshot_id=None if item.account is None else item.account.snapshot_id,
                profile_hash=item.profile_hash, price_times=tuple(MinutePriceTime(code=proof.quote.ts_code,
                    event_time=proof.quote.event_time, available_at=proof.quote.available_at,
                    quote_snapshot_id=proof.quote.snapshot_id) for proof in item.price_proofs),
                unavailable_reasons=item.unavailable_reasons) for item in replay.daily_valuations),
            message="按15:00已确认的行情估值；真实报价时间见逐日凭据。")

    def rows(self, job_id: UUID, *, owner_id: str, result_hash: str, table: MinuteTableName,
        offset: int, limit: int) -> MinuteRowsData:
        if not 0 <= offset < 80_000 or not 1 <= limit <= 50:
            raise ValueError("minute public rows exceed original page budget")
        read = self.read_result(job_id, owner_id=owner_id, result_hash=result_hash)
        replay = read.result.replay
        selected = {"signals": replay.signals, "orders": replay.orders, "fills": replay.fills,
            "paper_queue": replay.queue_records, "account": (replay.account,), "daily_valuations": replay.daily_valuations,
            "execution_profile": (replay.execution_profile,), "replay_summary": (read.result,)}[table]
        stop = min(offset + limit, len(selected))
        return MinuteRowsData(job_id=job_id, result_hash=result_hash, table=table,
            rows=tuple(MinuteResultRow(sequence=index, payload=selected[index].model_dump(
                mode="json", exclude_computed_fields=True)) for index in range(offset, stop)), total=len(selected),
            next_offset=stop if stop < len(selected) else None)


class LazyMinuteWebService:
    def __init__(self, path: Path | None, *, expected_code_sha: str | None, clock: Callable[[], datetime],
        native_report_path: Path | None = None, native_expected_code_sha: str | None = None,
        study_projection_authority: Path | None = None, study_projection_expected_sha256: str | None = None,
    ) -> None:
        if (path is None) != (expected_code_sha is None) or (
            native_report_path is None) != (native_expected_code_sha is None):
            raise ValueError("minute private paths require their exact actual runtime code")
        if path is None and native_report_path is None:
            raise ValueError("minute private report or run authority is unavailable")
        self.path, self.expected_code_sha, self.clock = path, expected_code_sha, clock
        self.native_report_path, self.native_expected_code_sha = native_report_path, native_expected_code_sha
        if (study_projection_authority is None) != (study_projection_expected_sha256 is None) or study_projection_authority is not None and path is None:
            raise ValueError("minute projection requires its pinned authority and original installation")
        self.study_projection_authority, self.study_projection_expected_sha256 = study_projection_authority, study_projection_expected_sha256
        self._service: MinuteWebService | None = None
        self._lock = threading.Lock()

    def load(self) -> MinuteWebService:
        with self._lock:
            if self._service is None:
                installed = None if self.path is None else load_minute_replay_installation(self.path,
                    expected_code_sha=self.expected_code_sha, clock=self.clock)
                native = None
                if self.native_report_path is not None:
                    from rquant.minute_backtest_native_report_runtime import load_minute_native_report_runtime

                    native = load_minute_native_report_runtime(self.native_report_path,
                        expected_code_sha=self.native_expected_code_sha, clock=self.clock)
                self._service = MinuteWebService(installed, native_report_runtime=native,
                    study_projection_authority=self.study_projection_authority,
                    study_projection_expected_sha256=self.study_projection_expected_sha256)
            if self._service.installation is not None:
                self._service.installation.verify_current()
            if self._service.native_report_runtime is not None:
                self._service.native_report_runtime.verify_current()
            return self._service
