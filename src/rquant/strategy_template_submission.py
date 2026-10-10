"""Prepare one trusted source and submit its frozen plan to the original Lab."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from rquant.experiment_registry import (
    ExperimentRegistry,
    ExperimentSpec,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
    IncompleteHypothesisFamilyError,
)
from rquant.lab_job_center import (
    CommandSubmissionReceipt,
    LabCommandSubmissionFacade,
    ResearchJobSubmission,
    _research_parameter,
    build_research_job_submission,
)
from rquant.lab_job_protocol import LabCommandEnvelope, SubmitJobCommand
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol
from rquant.research_catalog import ResearchCatalog
from rquant.research_run_spec import ResearchRunParameters, ResourceClass
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_job_adapters import build_adapter_execution_contract
from rquant.strategy_template_adapter import (
    StrategyTemplateAdapterCatalog,
    StrategyTemplateRunInput,
    StrategyTemplateRunParameters,
    build_strategy_template_adapter_catalog,
    template_adapter_id,
)
from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
from rquant.strategy_template_run_commands import (
    AcceptedStrategyTemplateRun,
    OwnedRunStrategyTemplate,
    RunStrategyTemplate,
    StrategyTemplateRunReceipt,
)
from rquant.strategy_template_source import (
    StrategyTemplateSourceData,
    freeze_strategy_template_source,
    publish_strategy_template_input,
)


class StrategyTemplateRunPreparer:
    def __init__(
        self,
        *,
        source_provider: Callable[
            [str, str, StrategyTemplateExecutionVersion], StrategyTemplateSourceData
        ],
        metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
        catalog: ResearchCatalog,
        lake_root: Path,
        input_root: Path,
        experiments: ExperimentRegistry,
        protocol: PortfolioExperimentProtocol,
        code_commit: str,
        clock: Callable[[], datetime],
        max_task_seconds: int = 3600,
    ) -> None:
        observed = input_root.lstat()
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o700
            or observed.st_uid != os.geteuid()
            or input_root.resolve() != input_root
        ):
            raise PermissionError("template input root must be private and producer-owned")
        if (
            re.fullmatch(r"[0-9a-f]{40}", code_commit) is None
            or type(max_task_seconds) is not int
            or not 1 <= max_task_seconds <= 86400
        ):
            raise ValueError("template installed producer identity or deadline differs")
        self.source_provider, self.metadata_store_factory = source_provider, metadata_store_factory
        self.catalog, self.lake_root, self.input_root = catalog, lake_root, input_root
        self.experiments, self.protocol = experiments, protocol
        self.code_commit, self.clock, self.max_task_seconds = code_commit, clock, max_task_seconds

    def prepare(
        self,
        version: StrategyTemplateExecutionVersion,
        request: RunStrategyTemplate,
        *,
        catalog: StrategyTemplateAdapterCatalog,
    ) -> tuple[ResearchJobSubmission, FormalExperimentPlan]:
        source = self.source_provider(version.owner_id, request.generation_id, version)
        if type(source) is not StrategyTemplateSourceData:
            raise TypeError("template producer requires the typed complete source")
        value = freeze_strategy_template_source(source, version, request)
        if value.request.producer_commit != self.code_commit:
            raise PermissionError("template source differs from the installed producer")
        now = self.clock()
        directory = self.input_root / uuid4().hex
        directory.mkdir(mode=0o700)
        with self.metadata_store_factory() as metadata:
            publication = publish_strategy_template_input(
                value,
                metadata_store=metadata,
                source_path=directory / "input.duckdb",
                catalog=self.catalog,
                lake_root=self.lake_root,
                version=version,
                now=now,
            )
        parameters = StrategyTemplateRunParameters.from_input(value, request_id=request.command_id)
        run = StrategyTemplateRunInput(
            start_date=request.start_date, end_date=request.end_date, parameters=parameters
        )
        run_parameters = ResearchRunParameters(
            strategy_name=version.strategy_id,
            start_date=request.start_date,
            end_date=request.end_date,
            arguments=tuple(
                _research_parameter(name, getattr(parameters, name))
                for name in type(parameters).model_fields
            ),
        )
        adapter_id = template_adapter_id(version.strategy_id)
        contract = build_adapter_execution_contract(adapter_id, "1", self.code_commit)
        definition = version.definition
        family = "template:" + canonical_sha256(
            {"owner": version.owner_id, "input": value.input_hash, "protocol": self.protocol}
        )
        spec = ExperimentSpec(
            strategy_spec_fingerprint=definition.spec.spec_fingerprint,
            strategy_executable_fingerprint=definition.executable_fingerprint,
            candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
            dataset_snapshot_id=publication.identity.snapshot_id,
            code_commit=self.code_commit,
            parameter_fingerprint=canonical_sha256(run_parameters),
            hypothesis_family=family,
            metric_definition_fingerprint=canonical_sha256(
                {"contract": "strategy-template-performance/v1", "overfit": "not_evaluated"}
            ),
            train_range=self.protocol.train_range,
            validation_range=self.protocol.validation_range,
            frozen_outer_test_range=self.protocol.frozen_outer_test_range,
            cost_model_fingerprint=canonical_sha256(value.request.execution_cost_spec),
            execution_model_fingerprint=canonical_sha256(
                {
                    "contract": "lab-adapter-execution/v1",
                    "adapter_id": adapter_id,
                    "adapter_version": "1",
                    "feature_contract": contract,
                }
            ),
            seed=0,
        )
        try:
            plan = self.experiments.resolve_formal_plan(
                strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
                strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
                candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
                dataset_snapshot_id=spec.dataset_snapshot_id,
                code_commit=spec.code_commit,
                parameter_fingerprint=spec.parameter_fingerprint,
                cost_model_fingerprint=spec.cost_model_fingerprint,
                execution_model_fingerprint=spec.execution_model_fingerprint,
                seed=spec.seed,
                as_of=now,
            )
        except IncompleteHypothesisFamilyError:
            plan = None
        if plan is None:
            plan = FormalExperimentPlan(
                schema_version=2,
                spec=spec,
                hypothesis_variant="controlled-template",
                strategy_definition_fingerprint=definition.fingerprint,
                definition_registration_record_hash=definition.record_hash,
                preregistered_at=now,
            )
            self.experiments.register_formal_plan(
                plan,
                family_manifest=HypothesisFamilyManifest(
                    hypothesis_family=family,
                    experiment_ids=(spec.experiment_id,),
                    search_space_fingerprint=canonical_sha256(
                        {"rules": value.rules, "input": value.input_hash}
                    ),
                    metric_definition_fingerprint=spec.metric_definition_fingerprint,
                    preregistered_at=now,
                ),
            )
        elif plan.spec != spec or (
            plan.strategy_definition_fingerprint,
            plan.definition_registration_record_hash,
        ) != (definition.fingerprint, definition.record_hash):
            raise PermissionError("template original experiment plan differs")
        submission: ResearchJobSubmission = build_research_job_submission(
            run,
            gate_decision=publication.gate_decision,
            code_sha=self.code_commit,
            dataset_snapshot=publication.identity,
            feature_contract=contract,
            execution_costs=value.request.execution_cost_spec,
            random_seed=0,
            resource_class=ResourceClass.STANDARD,
            deadline=now + timedelta(seconds=self.max_task_seconds),
            job_id=UUID(request.command_id),
            max_attempts=2,
            trusted_strategy_registration=definition,
            formal_experiment_plan=plan,
            template_catalog=catalog,
        )
        return submission, plan


class StrategyTemplateRunBackend:
    def __init__(
        self,
        store: StrategyAuthoringStore,
        *,
        facade: LabCommandSubmissionFacade,
        preparer: StrategyTemplateRunPreparer,
        expected_identity: StrategyAuthoringIdentity,
    ) -> None:
        if (
            type(store) is not StrategyAuthoringStore
            or type(facade) is not LabCommandSubmissionFacade
            or type(preparer) is not StrategyTemplateRunPreparer
        ):
            raise TypeError("template runs require the concrete installed original services")
        directory = facade.template_directory
        if (
            directory is None
            or directory.store is not store
            or directory.expected_identity != expected_identity
            or store.identity() != expected_identity
            or facade.experiment_registry is not preparer.experiments
        ):
            raise ValueError("template run services do not share the original private authority")
        self.store, self.facade, self.preparer, self.expected_identity = (
            store,
            facade,
            preparer,
            expected_identity,
        )

    def compile(
        self,
        request: RunStrategyTemplate,
        *,
        owner_id: str,
        expected_identity: StrategyAuthoringIdentity,
    ) -> OwnedRunStrategyTemplate:
        if self.expected_identity != expected_identity:
            raise PermissionError("template run private metadata identity differs")
        accepted = self.store.accepted_run(
            request, owner_id=owner_id, expected_identity=expected_identity
        )
        if accepted is None:
            self.store.verify_new_run(
                request, owner_id=owner_id, expected_identity=expected_identity
            )
            catalog = build_strategy_template_adapter_catalog(
                self.store,
                expected_identity=expected_identity,
                selected_keys=((request.strategy_id, request.head.version),),
            )
            version = catalog.versions[0]
            if version.owner_id != owner_id or version.head != request.head:
                raise PermissionError("selected template original version differs")
            submission, plan = self.preparer.prepare(version, request, catalog=catalog)
            accepted = self.store.commit_run_admission(
                AcceptedStrategyTemplateRun(
                    owner_id=owner_id,
                    request=request,
                    metadata_identity=expected_identity,
                    accepted_at=self.store._now(),
                    spec=submission.spec,
                    plan=plan,
                )
            )
        return OwnedRunStrategyTemplate(
            **request.model_dump(mode="python"),
            owner_id=owner_id,
            metadata_identity=expected_identity,
            accepted=accepted,
        )

    def submit(self, command: OwnedRunStrategyTemplate) -> StrategyTemplateRunReceipt:
        original = command.original()
        accepted = self.store.accepted_run(
            original, owner_id=command.owner_id, expected_identity=command.metadata_identity
        )
        if accepted != command.accepted:
            raise PermissionError("template original accepted plan differs")
        old = self.store.lookup_command(
            original, owner_id=command.owner_id, expected_identity=command.metadata_identity
        )
        if old is not None:
            return old
        submit = SubmitJobCommand(
            job_id=UUID(original.command_id), spec=accepted.spec, max_attempts=2
        )
        interaction = "strategy-template:" + command.owner_id + ":" + original.command_id
        envelope = LabCommandEnvelope(
            request_id=self.facade._request_id(interaction), command=submit
        )
        existing_job = self.facade.reader.get_job(submit.job_id)
        if existing_job is not None:
            intent = self.facade.experiment_registry.get_submission_intent_for_job(submit.job_id)
            if (
                existing_job.spec != accepted.spec
                or intent != self.facade._experiment_submission_intent(envelope)
            ):
                raise PermissionError("template original Lab publication identity differs")
            request_id, content_hash = intent.request_id, intent.command_content_hash
        else:
            result = self.facade.submit_create(submit, interaction_key=interaction)
            if not isinstance(result, CommandSubmissionReceipt):
                raise RuntimeError("template original Lab submission is not confirmed")
            if (
                result.request_id != envelope.request_id
                or result.spool.content_hash != envelope.content_hash
                or result.job_id != submit.job_id
            ):
                raise PermissionError("template original Lab receipt differs")
            request_id, content_hash = result.request_id, result.spool.content_hash
        receipt = StrategyTemplateRunReceipt(
            owner_id=command.owner_id,
            command_id=original.command_id,
            strategy_id=original.strategy_id,
            head=original.head,
            original_request_hash=original.request_hash,
            job_id=submit.job_id,
            lab_request_id=request_id,
            lab_content_hash=content_hash,
            spec_hash=accepted.spec.spec_hash,
            completed_at=self.store._now(),
        )
        return self.store.complete_run_receipt(accepted, receipt)
