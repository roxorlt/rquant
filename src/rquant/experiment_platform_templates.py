"""The original C5 store saves exact private variants; M8 retains every slot."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from rquant.experiment_platform_template_models import (
    ExperimentTemplateBaseline,
    ExperimentTemplatePublication,
    PreparedExperimentTemplate,
)
from rquant.research_run_spec import ResearchRunSpec
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_authoring import StrategyAuthoringConflict, StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_authoring_source import StrategySourceCatalog
from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
from rquant.strategy_template_source import StrategyTemplateSourceData

if TYPE_CHECKING:
    from rquant.experiment_platform import (
        ExperimentFamilyRecord,
        ExperimentPhaseRead,
        ExperimentPlatformStore,
        ExperimentPreparationReservation,
        ExperimentSearchRequest,
        ExperimentSourceProfile,
    )
    from rquant.experiment_platform_commands import ExperimentFamilyPreparer
    from rquant.portfolio_backtest_models import PortfolioBacktestConfig
    from rquant.strategy_template_adapter import StrategyTemplateAdapterCatalog
    from rquant.strategy_template_run import FrozenStrategyTemplateInput
    from rquant.strategy_template_source import PublishedStrategyTemplateInput


class ExperimentTemplateBinding:
    def __init__(
        self,
        *,
        original: StrategyAuthoringStore,
        expected_original_identity: StrategyAuthoringIdentity,
        private: StrategyAuthoringStore,
        expected_private_identity: StrategyAuthoringIdentity,
        catalogs: tuple[StrategySourceCatalog, ...],
        phase_provider: Callable[
            [ExperimentPhaseRead, StrategyTemplateExecutionVersion], StrategyTemplateSourceData
        ]
        | None = None,
    ) -> None:
        if (
            type(original) is not StrategyAuthoringStore
            or type(private) is not StrategyAuthoringStore
        ):
            raise TypeError("template binding needs both concrete original C5 stores")
        if (
            original.identity() != expected_original_identity
            or private.identity() != expected_private_identity
        ):
            raise PermissionError("installed original template metadata identity differs")
        if (expected_original_identity.st_dev, expected_original_identity.st_ino) == (
            expected_private_identity.st_dev,
            expected_private_identity.st_ino,
        ):
            raise ValueError("experiment variants require a separate installed private store")
        if len(catalogs) > 100 or len({c.owner_id for c in catalogs}) != len(catalogs):
            raise ValueError("template source catalogs repeat or exceed their budget")
        self.original, self.private = original, private
        self.expected_original_identity, self.expected_private_identity = (
            expected_original_identity,
            expected_private_identity,
        )
        self.catalogs = tuple(
            StrategySourceCatalog.model_validate(c.model_dump(mode="python")) for c in catalogs
        )
        self.phase_provider = phase_provider
        self.directory = StrategyTemplateRuntimeDirectory(
            private, expected_identity=expected_private_identity
        )

    def _catalog(self, owner: str) -> StrategySourceCatalog:
        matches = tuple(c for c in self.catalogs if c.owner_id == owner)
        if len(matches) != 1:
            raise PermissionError("original template source catalog is unavailable")
        return matches[0]

    def baseline(
        self, *, owner: str, request: ExperimentSearchRequest
    ) -> ExperimentTemplateBaseline | None:
        if request.template is None:
            return None
        if self.original.identity() != self.expected_original_identity:
            raise PermissionError("original template metadata identity changed")
        selection = request.template
        current = self.original.get_current(selection.strategy_id, owner_id=owner)
        metadata = self.original.get_version(
            selection.strategy_id, selection.head.version, owner_id=owner
        )
        if current.archived or metadata.head != selection.head:
            raise ValueError("original template head is archived or differs")
        definition = self.original.definition_registry(metadata.strategy_id).read_strategy_spec(
            metadata.head.registration_fingerprint
        )
        if definition is None:
            raise ValueError("original template definition is unavailable")
        version = StrategyTemplateExecutionVersion(
            owner_id=owner,
            strategy_id=metadata.strategy_id,
            head=metadata.head,
            rules=metadata.rules,
            definition=definition,
        )
        if (request.base_config.weight_rule, request.base_config.rebalance_rule) != (
            version.rules.weight_rule,
            version.rules.rebalance_rule,
        ):
            raise ValueError("template search baseline parameters differ from original rules")
        catalog = self._catalog(owner)
        catalog.validate_rules(version.rules, owner_id=owner, generation_id=catalog.generation_id)
        if self.original.identity() != self.expected_original_identity:
            raise PermissionError("original template metadata changed during baseline read")
        return ExperimentTemplateBaseline(
            metadata_identity=self.expected_original_identity,
            version=version,
            name=metadata.name,
            catalog_hash=canonical_sha256(catalog),
            generation_id=catalog.generation_id,
        )

    def prepare_definitions(
        self, store: ExperimentPlatformStore, record: ExperimentFamilyRecord
    ) -> tuple[StrategyTemplateExecutionVersion, ...]:
        if (
            store.get_family(record.owner, record.family_id) != record
            or record.template_baseline is None
        ):
            raise ValueError("derived preparation needs its exact admitted template baseline")
        if self.private.identity() != self.expected_private_identity:
            raise PermissionError("installed private original metadata identity changed")
        baseline = record.template_baseline
        catalog = self._catalog(record.owner)
        if (canonical_sha256(catalog), catalog.generation_id) != (
            baseline.catalog_hash,
            baseline.generation_id,
        ):
            raise PermissionError("original admitted source catalog changed")
        slots = store.template_slots(record.owner, record.family_id)
        if any(s.state in ("failed", "cancelled") for s in slots):
            raise ValueError("original planned template preparation failed or was cancelled")
        with self.private._connection(
            expected_identity=self.expected_private_identity
        ) as connection:
            pending = (
                "receipt IS NULL AND json_extract(frozen,'$.request.kind')='save_strategy_template'"
            )
            logical = connection.execute(
                "SELECT COUNT(*) FROM (SELECT strategy_id FROM heads UNION "
                f"SELECT strategy_id FROM command_refs WHERE {pending})"
            ).fetchone()[0]
            versions = connection.execute("SELECT COUNT(*) FROM versions").fetchone()[0]
            accepted = connection.execute(
                f"SELECT COUNT(*) FROM command_refs WHERE {pending}"
            ).fetchone()[0]
            needed = sum(
                connection.execute(
                    "SELECT 1 FROM command_refs WHERE command_id=?", (s.request.command_id,)
                ).fetchone()
                is None
                for s in slots
            )
        if logical + needed > 500 or versions + accepted + needed > 4096:
            for slot in slots:
                if slot.state == "pending":
                    store.save_template_slot(slot, failure="capacity")
            raise StrategyAuthoringConflict(
                "complete planned template family exceeds original capacity"
            )
        versions_out = []
        for slot in slots:
            if slot.state != "saved":
                try:
                    receipt = self.private.lookup_command(
                        slot.request,
                        owner_id=record.owner,
                        expected_identity=self.expected_private_identity,
                    )
                    if receipt is None:
                        accepted_command = self.private.accept(
                            slot.request,
                            owner_id=record.owner,
                            catalog=catalog,
                            expected_identity=self.expected_private_identity,
                        )
                        receipt = self.private.complete_save(
                            accepted_command, expected_identity=self.expected_private_identity
                        )
                    slot = store.save_template_slot(slot, receipt=receipt)
                except StrategyAuthoringConflict:
                    store.save_template_slot(slot, failure="capacity")
                    raise
                except ValueError:
                    store.save_template_slot(slot, failure="invalid_definition")
                    raise
            metadata = self.private.get_version(slot.receipt.strategy_id, 1, owner_id=record.owner)
            if metadata.head != slot.receipt.head or metadata.rules != slot.request.rules:
                raise ValueError("derived original immutable definition differs from its slot")
            definition = self.private.definition_registry(metadata.strategy_id).read_strategy_spec(
                metadata.head.registration_fingerprint
            )
            if definition is None:
                raise ValueError("derived original definition is unavailable")
            versions_out.append(
                StrategyTemplateExecutionVersion(
                    owner_id=record.owner,
                    strategy_id=metadata.strategy_id,
                    head=metadata.head,
                    rules=metadata.rules,
                    definition=definition,
                )
            )
        if self.private.identity() != self.expected_private_identity:
            raise PermissionError("private original metadata changed during derived preparation")
        return tuple(versions_out)

    def prepare_family(
        self,
        preparer: ExperimentFamilyPreparer,
        record: ExperimentFamilyRecord,
        *,
        read: ExperimentPhaseRead,
        profile: ExperimentSourceProfile,
    ) -> ExperimentFamilyRecord:
        from rquant.experiment_platform import (
            MAX_FAMILY_INPUT_BYTES,
            ExperimentChildRegistration,
            ExperimentPreparationReceipt,
            ExperimentPreparationReservation,
            stable_experiment_interaction,
            stable_experiment_job,
        )
        from rquant.experiment_platform_commands import _input_digest
        from rquant.lab_job_center import LabCommandSubmissionFacade
        from rquant.lab_job_protocol import LabCommandEnvelope
        from rquant.lab_worker import build_builtin_shard_runtime_manifest
        from rquant.portfolio_backtest_source import freeze_portfolio_config
        from rquant.strategy_template_adapter import build_strategy_template_adapter_catalog
        from rquant.strategy_template_run_commands import RunStrategyTemplate
        from rquant.strategy_template_source import (
            freeze_strategy_template_source,
            publish_strategy_template_input,
        )

        versions = self.prepare_definitions(preparer.store, record)
        source = None
        children = []
        total_bytes = 0
        expected_dates = tuple(
            d for d in profile.calendar.dates if read.window.start_date <= d <= read.window.end_date
        )
        for index, (cfg, version) in enumerate(
            zip(record.actual_configurations, versions, strict=True)
        ):
            previous = preparer.store.preparation(record.owner, record.family_id, index)
            if previous is not None:
                if not isinstance(previous.prepared, PreparedExperimentTemplate):
                    raise ValueError("template child has another original preparation type")
                identity, digest = _input_digest(Path(previous.source_path))
                if (previous.source_identity, identity, digest) != (
                    profile.source_identity,
                    previous.file_identity,
                    previous.file_sha256,
                ):
                    raise ValueError("original prepared template source changed")
                prepared = previous.prepared
            else:
                reservation = preparer.store.preparation_reservation(
                    record.owner, record.family_id, index
                )
                if (
                    reservation is not None
                    and reservation.source_identity != profile.source_identity
                ):
                    raise PermissionError("original reserved template source identity changed")
                if reservation is not None and Path(reservation.source_path).exists():
                    frozen, publication = self._recover_input(
                        preparer, record, version, reservation
                    )
                    directory = Path(reservation.source_path).parent
                    benchmark_closes = reservation.template_benchmark_closes
                else:
                    if source is None:
                        if self.phase_provider is None:
                            raise ValueError(
                                "original typed template phase source is not installed"
                            )
                        actual = self.phase_provider(read, version)
                        if type(actual) is not StrategyTemplateSourceData:
                            raise TypeError(
                                "template phase needs complete typed original source facts"
                            )
                        source = StrategyTemplateSourceData.model_validate(
                            actual.model_dump(mode="python")
                        )
                        portfolio = source.portfolio
                        if (
                            source.owner_id,
                            canonical_sha256(source.catalog),
                            portfolio.source_key,
                            portfolio.source_version,
                            portfolio.sources,
                            portfolio.template.producer_commit,
                            portfolio.template.calendar,
                            tuple(d.trade_date for d in portfolio.template.days),
                        ) != (
                            record.owner,
                            record.template_baseline.catalog_hash,
                            profile.source_key,
                            profile.source_version,
                            profile.sources,
                            profile.producer_commit,
                            profile.calendar,
                            expected_dates,
                        ):
                            raise PermissionError(
                                "template phase provider exposed another source or unbounded rows"
                            )
                        first = profile.calendar.dates.index(read.window.start_date)
                        if first == 0:
                            raise ValueError("template phase has no actual preceding trading date")
                        baseline = profile.calendar.dates[first - 1]
                        if any(
                            d < baseline or d > read.window.end_date
                            for rows in portfolio.benchmarks.values()
                            for d, _ in rows
                        ):
                            raise PermissionError(
                                "template phase provider exposed out-of-phase benchmark prices"
                            )
                        generation = canonical_sha256(
                            {
                                "contract": "private-template-phase/v1",
                                "source": profile.source_identity,
                                "owner": record.owner,
                                "family": record.family_id,
                                "phase": record.phase,
                                "window": read.window,
                            }
                        )
                        portfolio = type(portfolio).model_validate(
                            portfolio.model_dump(mode="python")
                            | {
                                "template": portfolio.template.model_dump(mode="python")
                                | {"input_generation_id": generation},
                                "material_hash": None,
                            }
                        )
                        source = StrategyTemplateSourceData.model_validate(
                            source.model_dump(mode="python")
                            | {"portfolio": portfolio, "material_hash": None}
                        )
                    run = RunStrategyTemplate(
                        command_id=str(
                            stable_experiment_job(record.owner, record.request_id, index)
                        ),
                        requested_at=record.registered_at,
                        generation_id=record.template_baseline.generation_id,
                        strategy_id=version.strategy_id,
                        head=version.head,
                        expected_head=version.head,
                        start_date=cfg.start_date,
                        end_date=cfg.end_date,
                        initial_cash=cfg.initial_cash,
                    )
                    frozen = freeze_strategy_template_source(source, version, run)
                    benchmark_closes = freeze_portfolio_config(
                        source.portfolio, cfg
                    ).benchmark_closes
                    # Original source facts fix all non-search risk/cost fields.
                    observed = (
                        type(cfg)
                        .from_request(frozen.request)
                        .model_dump(exclude={"source_key", "source_version", "benchmark_code"})
                    )
                    if observed != cfg.model_dump(
                        exclude={"source_key", "source_version", "benchmark_code"}
                    ):
                        raise ValueError(
                            "template actual risk/cost/input differs from planned configuration"
                        )
                    if reservation is None:
                        directory = preparer.input_root / uuid4().hex
                        reservation = preparer.store.reserve_preparation(
                            ExperimentPreparationReservation(
                                owner=record.owner,
                                family_id=record.family_id,
                                index=index,
                                source_identity=profile.source_identity,
                                source_path=str(directory / "input.duckdb"),
                                input_hash=frozen.input_hash,
                                created_at=record.registered_at,
                                template_benchmark_closes=benchmark_closes,
                            )
                        )
                    else:
                        directory = Path(reservation.source_path).parent
                        if reservation.input_hash != frozen.input_hash:
                            raise ValueError("reserved original template input changed")
                        if reservation.template_benchmark_closes != benchmark_closes:
                            raise ValueError("reserved original template benchmark changed")
                    directory.mkdir(mode=0o700, exist_ok=True)
                    with preparer.metadata_store_factory() as metadata:
                        publication = publish_strategy_template_input(
                            frozen,
                            metadata_store=metadata,
                            source_path=directory / "input.duckdb",
                            catalog=preparer.catalog,
                            lake_root=preparer.lake_root,
                            version=version,
                            now=record.registered_at,
                        )
                catalog = build_strategy_template_adapter_catalog(
                    self.private,
                    expected_identity=self.expected_private_identity,
                    selected_keys=((version.strategy_id, version.head.version),),
                )
                prepared = _prepare_template_plan(
                    record,
                    index,
                    cfg,
                    version,
                    frozen,
                    publication,
                    catalog,
                    deadline=record.registered_at + timedelta(seconds=preparer.max_task_seconds),
                    benchmark_closes=benchmark_closes,
                )
                with preparer.metadata_store_factory() as metadata:
                    manifest = build_builtin_shard_runtime_manifest(
                        catalog_path=Path(metadata.path),
                        forbidden_paths=(),
                        snapshot_root=preparer.input_root / "worker-copies",
                        research_lake_root=preparer.lake_root,
                    )
                self.directory.manifest_for_spec(prepared.spec, manifest)
                identity, digest = _input_digest(directory / "input.duckdb")
                total_bytes += identity[2]
                if total_bytes > MAX_FAMILY_INPUT_BYTES:
                    raise ValueError("complete family input exceeds 512 MiB")
                preparer.store.save_preparation(
                    ExperimentPreparationReceipt(
                        owner=record.owner,
                        family_id=record.family_id,
                        index=index,
                        source_identity=profile.source_identity,
                        source_path=str(directory / "input.duckdb"),
                        file_identity=identity,
                        file_sha256=digest,
                        prepared=prepared,
                    )
                )
            if previous is not None:
                total_bytes += identity[2]
            if total_bytes > MAX_FAMILY_INPUT_BYTES:
                raise ValueError("complete family input exceeds 512 MiB")
            envelope = LabCommandEnvelope(
                request_id=LabCommandSubmissionFacade._request_id(
                    stable_experiment_interaction(record.owner, record.request_id, index)
                ),
                command=prepared.submission(
                    job_id=stable_experiment_job(record.owner, record.request_id, index)
                ).command,
            )
            intent = LabCommandSubmissionFacade._experiment_submission_intent(envelope)
            if intent is None:
                raise ValueError("template original formal intent is unavailable")
            children.append(
                ExperimentChildRegistration(
                    config=cfg,
                    plan=prepared.formal_plan,
                    intent=intent,
                    published=prepared.published,
                )
            )
        return preparer.store.register_family_submission(
            owner=record.owner, request_id=record.request_id, children=tuple(children)
        )

    def _recover_input(
        self,
        preparer: ExperimentFamilyPreparer,
        record: ExperimentFamilyRecord,
        version: StrategyTemplateExecutionVersion,
        reservation: ExperimentPreparationReservation,
    ) -> tuple[FrozenStrategyTemplateInput, PublishedStrategyTemplateInput]:
        from rquant.data_metadata import DataAuditRun, DatasetSnapshot
        from rquant.experiment_platform_commands import _input_digest
        from rquant.research_gate import ResearchGateRequest
        from rquant.research_run_spec import DatasetSnapshotIdentity
        from rquant.research_snapshot import ResearchExecutionSession
        from rquant.strategy_template_source import (
            PublishedStrategyTemplateInput,
            require_template_gate,
            verify_bound_template_input,
        )

        path = Path(reservation.source_path)
        if (
            path.parent.parent != preparer.input_root
            or path.name != "input.duckdb"
            or len(path.parent.name) != 32
        ):
            raise PermissionError("reserved template input is outside the installed private root")
        parent = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
        ):
            raise PermissionError("reserved input directory is not private and owned")
        _input_digest(path)
        cfg = record.actual_configurations[reservation.index]
        snapshot = DatasetSnapshot.create(
            strategy_name=version.strategy_id,
            manifest_id=reservation.input_hash,
            as_of_time=record.registered_at,
            code_commit=version.definition.producer_commit,
            origin="trusted-template-producer",
            created_at=record.registered_at,
        )
        audit = DataAuditRun.create(
            as_of_date=record.registered_at.date(),
            range_start=cfg.start_date,
            range_end=cfg.end_date,
            observed_at=record.registered_at,
            rule_set_version=f"strategy-template-source/v1:{reservation.input_hash}",
        )
        with preparer.metadata_store_factory() as metadata:
            binding = metadata.get_dataset_snapshot_binding(snapshot.snapshot_id)
            if binding is None:
                raise RuntimeError("interrupted original template publication is incomplete")
            request = ResearchGateRequest(
                mode="exploratory",
                strategy_name=version.strategy_id,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                code_commit=version.definition.producer_commit,
                audit_run_id=audit.audit_run_id,
                dataset_snapshot_id=snapshot.snapshot_id,
                dataset_binding_hash=binding.binding_hash,
            )
            with ResearchExecutionSession(binding=binding, lake_root=preparer.lake_root) as session:
                # The original publisher seals its temporary table in the bound lake.
                frozen = verify_bound_template_input(metadata, request, session, version=version)
            if (
                frozen.input_hash != reservation.input_hash
                or frozen.definition != version.definition
                or frozen.owner_id != record.owner
            ):
                raise ValueError("interrupted original template input differs")
            gate = require_template_gate(metadata, request, version=version, binding_verified=True)
        return frozen, PublishedStrategyTemplateInput(
            input_hash=frozen.input_hash,
            identity=DatasetSnapshotIdentity(
                snapshot_id=snapshot.snapshot_id,
                binding_hash=binding.binding_hash,
                audit_run_id=audit.audit_run_id,
            ),
            request=request,
            gate_decision=gate,
        )


def _prepare_template_plan(
    record: ExperimentFamilyRecord,
    index: int,
    cfg: PortfolioBacktestConfig,
    version: StrategyTemplateExecutionVersion,
    frozen: FrozenStrategyTemplateInput,
    publication: PublishedStrategyTemplateInput,
    catalog: StrategyTemplateAdapterCatalog,
    *,
    deadline: datetime,
    benchmark_closes: tuple[tuple[date, float], ...] | None = None,
) -> PreparedExperimentTemplate:
    from rquant.experiment_platform import stable_experiment_job
    from rquant.experiment_registry import ExperimentSpec, FormalExperimentPlan
    from rquant.lab_job_center import _research_parameter, build_research_job_submission
    from rquant.research_run_spec import ResearchRunParameters, ResourceClass
    from rquant.strategy_job_adapters import build_adapter_execution_contract
    from rquant.strategy_template_adapter import (
        StrategyTemplateRunInput,
        StrategyTemplateRunParameters,
        template_adapter_id,
    )

    parameters = StrategyTemplateRunParameters.from_input(
        frozen, request_id=str(stable_experiment_job(record.owner, record.request_id, index))
    )
    run = StrategyTemplateRunInput(
        start_date=cfg.start_date, end_date=cfg.end_date, parameters=parameters
    )
    raw_parameters = ResearchRunParameters(
        strategy_name=version.strategy_id,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        arguments=tuple(
            _research_parameter(name, getattr(parameters, name))
            for name in type(parameters).model_fields
        ),
    )
    adapter_id = template_adapter_id(version.strategy_id)
    contract = build_adapter_execution_contract(adapter_id, "1", frozen.request.producer_commit)
    definition = version.definition
    spec = ExperimentSpec(
        strategy_spec_fingerprint=definition.spec.spec_fingerprint,
        strategy_executable_fingerprint=definition.executable_fingerprint,
        candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
        dataset_snapshot_id=publication.identity.snapshot_id,
        code_commit=frozen.request.producer_commit,
        parameter_fingerprint=canonical_sha256(raw_parameters),
        hypothesis_family=record.family_id,
        metric_definition_fingerprint=canonical_sha256(
            {"contract": "strategy-template-performance/v1", "overfit": "not_evaluated"}
        ),
        train_range=record.request.protocol.train_range,
        validation_range=record.request.protocol.validation_range,
        frozen_outer_test_range=record.request.protocol.frozen_outer_test_range,
        cost_model_fingerprint=canonical_sha256(frozen.request.execution_cost_spec),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": adapter_id,
                "adapter_version": "1",
                "feature_contract": contract,
            }
        ),
        seed=record.request.seed,
    )
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=spec,
        hypothesis_variant=f"configuration-{index}",
        strategy_definition_fingerprint=definition.fingerprint,
        definition_registration_record_hash=definition.record_hash,
        preregistered_at=record.registered_at,
    )
    submission = build_research_job_submission(
        run,
        gate_decision=publication.gate_decision,
        code_sha=frozen.request.producer_commit,
        dataset_snapshot=publication.identity,
        feature_contract=contract,
        execution_costs=frozen.request.execution_cost_spec,
        random_seed=record.request.seed,
        resource_class=ResourceClass.STANDARD,
        deadline=deadline,
        job_id=stable_experiment_job(record.owner, record.request_id, index),
        max_attempts=2,
        trusted_strategy_registration=definition,
        formal_experiment_plan=plan,
        template_catalog=catalog,
    )
    return PreparedExperimentTemplate(
        configuration=cfg,
        frozen=frozen,
        published=ExperimentTemplatePublication(
            **publication.model_dump(mode="python"), config_hash=cfg.config_hash
        ),
        registration=definition,
        formal_plan=plan,
        spec=submission.spec,
        catalog=catalog,
        benchmark_closes=benchmark_closes,
    )


class ExperimentTemplateRuntimeBinding:
    """Installed exact-child selector; it never accepts a browser permit or path."""

    def __init__(
        self, *, store: ExperimentPlatformStore, binding: ExperimentTemplateBinding
    ) -> None:
        from rquant.experiment_platform import ExperimentPlatformStore

        if (
            type(store) is not ExperimentPlatformStore
            or type(binding) is not ExperimentTemplateBinding
        ):
            raise TypeError(
                "private template runtime needs the installed concrete original authorities"
            )
        self.store, self.binding = store, binding

    def directory_for_job(
        self, job_id: UUID, spec: ResearchRunSpec
    ) -> StrategyTemplateRuntimeDirectory | None:
        from rquant.experiment_platform import PRIVATE_FAMILY_PREFIXES, stable_experiment_job

        family_id = None if spec.experiment is None else spec.experiment.hypothesis_family
        if family_id is None or not family_id.startswith(PRIVATE_FAMILY_PREFIXES):
            return None
        child = self.store.child(job_id)
        if (
            child is None
            or child.family_id != family_id
            or child.experiment_id != spec.experiment.experiment_id
        ):
            raise PermissionError("private template runtime has no exact persisted child")
        record = self.store.get_family(child.owner, family_id)
        if record.state != "ready":
            raise PermissionError("private template family is not fully ready")
        prepared = next(
            (
                p
                for i in range(len(record.actual_configurations))
                if (p := self.store.preparation(record.owner, family_id, i)) is not None
                and p.prepared.formal_plan.spec.experiment_id == child.experiment_id
            ),
            None,
        )
        if (
            prepared is None
            or prepared.prepared.submission(job_id=job_id).spec != spec
            or (
                prepared.owner,
                prepared.family_id,
                stable_experiment_job(record.owner, record.request_id, prepared.index),
                prepared.configuration,
            )
            != (child.owner, family_id, job_id, record.actual_configurations[prepared.index])
        ):
            raise PermissionError("private template runtime differs from its original job or input")
        if record.template_baseline is None:
            if isinstance(prepared.prepared, PreparedExperimentTemplate):
                raise PermissionError(
                    "ordinary experiment cannot select a private template directory"
                )
            return None
        if not isinstance(prepared.prepared, PreparedExperimentTemplate):
            raise PermissionError("private template preparation has another kind")
        slots = self.store.template_slots(record.owner, record.family_id)
        slot = slots[prepared.index]
        value = prepared.prepared
        if (
            slot.state != "saved"
            or slot.receipt is None
            or (slot.receipt.strategy_id, slot.receipt.head, slot.request.rules)
            != (value.registration.logical_id, value.catalog.versions[0].head, value.frozen.rules)
        ):
            raise PermissionError("private template runtime differs from its original save receipt")
        if self.binding.private.identity() != self.binding.expected_private_identity:
            raise PermissionError("installed private original directory identity changed")
        self.binding.directory.catalog_for_spec(spec)
        return self.binding.directory


def require_experiment_template_runtime_binding(
    value: ExperimentTemplateRuntimeBinding | None,
) -> ExperimentTemplateRuntimeBinding | None:
    if value is not None and type(value) is not ExperimentTemplateRuntimeBinding:
        raise TypeError("private template runtime selector must be the installed concrete binding")
    return value
