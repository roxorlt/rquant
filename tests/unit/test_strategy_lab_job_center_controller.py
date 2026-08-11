from __future__ import annotations

import ast
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentSpec,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.lab_artifact_export import LabJobZipExportReceipt
from rquant.lab_artifact_preview import ArtifactPreview
from rquant.lab_job_center import (
    AuctionGapRunInput,
    CommandSubmissionConflict,
    CommandSubmissionReceipt,
    GrowthBoardSurgeRunInput,
    NShapeComparisonRunInput,
    NShapeOptimizationRunInput,
)
from rquant.lab_jobs import LabJobListFilters
from rquant.research_gate import ResearchGateDecision
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    ResourceClass,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_job_adapters import (
    AuctionGapParameters,
    GrowthBoardSurgeParameters,
    NShapeCompareParameters,
    NShapeOptimizeParameters,
    build_adapter_execution_contract,
)

CODE_SHA = "1" * 40
NOW = datetime(2026, 7, 31, 8, tzinfo=UTC)
JOB_ID = UUID(int=101)
SOURCE_JOB_ID = UUID(int=102)


def _gate(*, formal: bool) -> ResearchGateDecision:
    return ResearchGateDecision(
        allowed=True,
        research_status="comparable" if formal else "exploratory",
        audit_run_id="d" * 64 if formal else None,
        dataset_snapshot_id="a" * 64 if formal else None,
        dataset_binding_hash="b" * 64 if formal else None,
        coverage_ratios={},
        coverage_counts={},
        failures=(),
    )


def _snapshot() -> DatasetSnapshotIdentity:
    return DatasetSnapshotIdentity(
        snapshot_id="a" * 64,
        binding_hash="b" * 64,
        audit_run_id="d" * 64,
    )


def _costs() -> ExecutionCostSpec:
    return ExecutionCostSpec(
        commission_bps=Decimal("2.5"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("0.1"),
        slippage_bps=Decimal("3"),
    )


RUN_INPUTS = (
    (
        NShapeComparisonRunInput(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 20),
            parameters=NShapeCompareParameters(
                hold_days=(1, 3),
                entry_modes=("first_break",),
            ),
        ),
        "nshape-compare",
    ),
    (
        NShapeOptimizationRunInput(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 20),
            parameters=NShapeOptimizeParameters(
                hold_days=(1, 3),
                entry_modes=("first_break",),
                profile_variants=("baseline",),
            ),
        ),
        "nshape-optimize",
    ),
    (
        AuctionGapRunInput(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 20),
            parameters=AuctionGapParameters(max_hold_days=2),
        ),
        "auction-gap",
    ),
    (
        GrowthBoardSurgeRunInput(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 20),
            parameters=GrowthBoardSurgeParameters(
                variants=("full", "no_vwap"),
                max_hold_days=2,
            ),
        ),
        "growth-board-surge",
    ),
)


class _CommandFacadeSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _record(self, name: str, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        self.calls.append((name, args, kwargs))
        argument = args[0] if args else JOB_ID
        job_id = kwargs.get("new_job_id", getattr(argument, "job_id", argument))
        return CommandSubmissionConflict(
            request_id=UUID(int=len(self.calls)),
            job_id=job_id,
            reason="job_not_found",
        )

    def submit_create(self, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        return self._record("create", *args, **kwargs)

    def submit_pause(self, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        return self._record("pause", *args, **kwargs)

    def submit_resume(self, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        return self._record("resume", *args, **kwargs)

    def submit_cancel(self, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        return self._record("cancel", *args, **kwargs)

    def submit_retry(self, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        return self._record("retry", *args, **kwargs)

    def submit_rerun(self, *args: Any, **kwargs: Any) -> CommandSubmissionConflict:
        return self._record("rerun", *args, **kwargs)


class _ReaderSpy:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, Any]] = []
        self.detail_calls: list[tuple[UUID, dict[str, Any]]] = []

    def list_jobs(self, **kwargs: Any) -> Any:
        self.list_calls.append(kwargs)
        return "typed-page"

    def get_job_detail(self, job_id: UUID, **kwargs: Any) -> Any:
        self.detail_calls.append((job_id, kwargs))
        return "typed-detail"


class _PreviewSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, dict[str, Any]]] = []

    def preview(self, job_id: UUID, **kwargs: Any) -> ArtifactPreview:
        self.calls.append((job_id, kwargs))
        return ArtifactPreview(
            job_id=job_id,
            spec_hash="2" * 64,
            manifest_hash="3" * 64,
            complete_result_hash="4" * 64,
            report_markdown="ok",
            metrics={},
            available_tables=(),
            table=None,
        )


class _ExportSpy:
    def __init__(self) -> None:
        self.calls: list[UUID] = []
        self.discarded: list[LabJobZipExportReceipt] = []

    def export(self, job_id: UUID) -> LabJobZipExportReceipt:
        self.calls.append(job_id)
        return LabJobZipExportReceipt(
            request_id=UUID(int=901),
            job_id=job_id,
            path=Path("/tmp/export.zip"),
            byte_size=10,
            sha256="5" * 64,
        )

    def discard(self, receipt: LabJobZipExportReceipt) -> None:
        self.discarded.append(receipt)


def _controller() -> tuple[Any, _ReaderSpy, _CommandFacadeSpy, _PreviewSpy, _ExportSpy]:
    from rquant.dashboard.lab.job_center import StrategyLabJobCenterController

    reader = _ReaderSpy()
    commands = _CommandFacadeSpy()
    preview = _PreviewSpy()
    exports = _ExportSpy()
    return (
        StrategyLabJobCenterController(
            reader=reader,
            commands=commands,
            preview_reader=preview,
            zip_exports=exports,
        ),
        reader,
        commands,
        preview,
        exports,
    )


def _context(*, formal: bool = False) -> Any:
    from rquant.dashboard.lab.job_center import StrategyLabSubmissionContext

    return StrategyLabSubmissionContext(
        gate_decision=_gate(formal=formal),
        code_sha=CODE_SHA,
        dataset_snapshot=_snapshot() if formal else None,
        execution_costs=_costs(),
        random_seed=7,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        max_attempts=3,
    )


def _definition_registry(tmp_path: Path) -> ImmutableDefinitionRegistry:
    root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=CODE_SHA)
    bootstrap_builtin_definitions(
        root,
        producer_commit=CODE_SHA,
        registered_at=NOW - timedelta(days=2),
        available_at=NOW - timedelta(days=1),
        expected_plan_id=plan.plan_id,
    )
    return ImmutableDefinitionRegistry(
        root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=CODE_SHA
        ).trusted_executable_registry(),
    )


@pytest.mark.parametrize(("run_input", "adapter_id"), RUN_INPUTS)
def test_submit_maps_all_inputs_through_the_canonical_factory(
    run_input: Any,
    adapter_id: str,
) -> None:
    controller, _, commands, _, _ = _controller()

    result = controller.submit(
        run_input,
        context=_context(),
        interaction_key="form-submit-1",
        job_id=JOB_ID,
    )

    assert isinstance(result, CommandSubmissionConflict)
    name, args, kwargs = commands.calls[-1]
    assert name == "create"
    command = args[0]
    assert command.job_id == JOB_ID
    assert command.max_attempts == 3
    assert command.spec.feature_contract == build_adapter_execution_contract(
        adapter_id,
        "1",
        CODE_SHA,
    )
    assert command.spec.parameters.start_date == run_input.start_date
    assert kwargs == {"interaction_key": "form-submit-1"}


def test_page_controller_can_build_submission_without_writer_facades() -> None:
    from rquant.dashboard.lab.job_center import StrategyLabJobCenterController

    controller = StrategyLabJobCenterController(
        reader=_ReaderSpy(),
        preview_reader=_PreviewSpy(),
    )

    command = controller.build_submission_command(
        RUN_INPUTS[0][0],
        context=_context(),
        job_id=JOB_ID,
        as_of=NOW,
    )

    assert command.job_id == JOB_ID
    assert command.command_type == "submit"
    assert command.spec.parameters.strategy_name == "n_shape"


def test_submission_context_is_strict_frozen_and_requires_formal_snapshot() -> None:
    context = _context(formal=True)
    assert context.dataset_snapshot == _snapshot()
    assert context.model_config["frozen"] is True
    assert context.model_config["strict"] is True

    with pytest.raises(ValidationError, match="immutable dataset snapshot"):
        type(context)(
            gate_decision=_gate(formal=True),
            code_sha=CODE_SHA,
            dataset_snapshot=None,
            execution_costs=_costs(),
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            max_attempts=1,
        )
    with pytest.raises(ValidationError, match="code_sha"):
        type(context)(
            gate_decision=_gate(formal=False),
            code_sha="dirty",
            dataset_snapshot=None,
            execution_costs=_costs(),
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            max_attempts=1,
        )


def test_formal_submit_fails_closed_without_trusted_ownership_resolvers() -> None:
    controller, _, commands, _, _ = _controller()

    with pytest.raises(RuntimeError, match="Definition Registry|formal ownership"):
        controller.submit(
            RUN_INPUTS[0][0],
            context=_context(formal=True),
            interaction_key="formal-missing-trust",
            job_id=JOB_ID,
        )

    assert commands.calls == []


def test_formal_submit_resolves_v3_identity_and_atomically_registers_attempt(
    tmp_path: Path,
) -> None:
    from rquant.dashboard.lab.job_center import (
        RegistryBackedFormalExperimentResolver,
        StrategyLabFormalExperimentBinding,
        StrategyLabFormalResolutionRequest,
        StrategyLabJobCenterController,
    )
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore

    definitions = _definition_registry(tmp_path)
    experiments = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    requests: list[StrategyLabFormalResolutionRequest] = []
    trusted_resolver = RegistryBackedFormalExperimentResolver(experiments)

    def resolve_experiment(
        registration: object,
        request: StrategyLabFormalResolutionRequest,
    ) -> StrategyLabFormalExperimentBinding:
        requests.append(request)
        experiment = ExperimentSpec(
            strategy_spec_fingerprint=registration.spec.spec_fingerprint,  # type: ignore[attr-defined]
            strategy_executable_fingerprint=registration.executable_fingerprint,  # type: ignore[attr-defined]
            candidate_schema_fingerprint=registration.candidate_schema_fingerprint,  # type: ignore[attr-defined]
            dataset_snapshot_id=request.dataset_snapshot.snapshot_id,
            code_commit=request.code_sha,
            parameter_fingerprint=canonical_sha256(request.parameters),
            hypothesis_family="dashboard-formal",
            metric_definition_fingerprint="9" * 64,
            train_range=DateRange(
                start_date=date(2025, 1, 1),
                end_date=date(2025, 6, 30),
            ),
            validation_range=DateRange(
                start_date=date(2025, 7, 1),
                end_date=date(2025, 12, 31),
            ),
            frozen_outer_test_range=DateRange(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
            ),
            cost_model_fingerprint=canonical_sha256(request.execution_costs),
            execution_model_fingerprint=canonical_sha256(
                {
                    "contract": "lab-adapter-execution/v1",
                    "adapter_id": request.adapter_id,
                    "adapter_version": request.adapter_version,
                    "feature_contract": request.feature_contract,
                }
            ),
            seed=request.random_seed,
        )
        assert experiment.experiment_id is not None
        manifest = HypothesisFamilyManifest(
            hypothesis_family=experiment.hypothesis_family,
            experiment_ids=(experiment.experiment_id,),
            search_space_fingerprint="8" * 64,
            metric_definition_fingerprint=experiment.metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=1),
        )
        experiments.register_formal_plan(
            FormalExperimentPlan(
                schema_version=2,
                spec=experiment,
                hypothesis_variant="baseline",
                strategy_definition_fingerprint=registration.fingerprint,  # type: ignore[attr-defined]
                definition_registration_record_hash=registration.record_hash,  # type: ignore[attr-defined]
                preregistered_at=manifest.preregistered_at,
            ),
            family_manifest=manifest,
        )
        return trusted_resolver(registration, request)  # type: ignore[arg-type]

    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    reader = LabJobReader(store.path)
    commands = LabCommandSubmissionFacade(
        reader=reader,
        spool=spool,
        experiment_registry=experiments,
        definition_registry=definitions,
        clock=lambda: NOW,
    )
    controller = StrategyLabJobCenterController(
        reader=reader,
        commands=commands,
        preview_reader=_PreviewSpy(),
        zip_exports=_ExportSpy(),
        definition_registry=definitions,
        formal_experiment_resolver=resolve_experiment,
        clock=lambda: NOW,
    )

    receipt = controller.submit(
        RUN_INPUTS[0][0],
        context=_context(formal=True),
        interaction_key="dashboard-formal-v3",
        job_id=JOB_ID,
    )

    assert isinstance(receipt, CommandSubmissionReceipt)
    assert len(requests) == 1
    envelope = spool.pending()[0].envelope
    spec = envelope.command.spec
    assert spec.schema_version == 3
    assert spec.catalog_owner_eligible
    assert spec.strategy_execution is not None
    assert spec.experiment is not None
    assert spec.strategy_execution.definition_registration_record_hash
    assert experiments.get_attempt(spec.experiment.experiment_id).spec == spec.experiment.spec
    assert experiments.list_pending_submissions(limit=10) == ()
    assert not {
        "strategy_spec_fingerprint",
        "strategy_executable_fingerprint",
        "candidate_schema_fingerprint",
    }.intersection(argument.name for argument in spec.parameters.arguments)


def test_submit_is_exactly_once_and_defaults_to_a_fresh_job_id(tmp_path: Path) -> None:
    from rquant.dashboard.lab.job_center import StrategyLabJobCenterController
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore

    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    reader = LabJobReader(store.path)
    controller = StrategyLabJobCenterController(
        reader=reader,
        commands=LabCommandSubmissionFacade(reader=reader, spool=spool),
        preview_reader=_PreviewSpy(),
        zip_exports=_ExportSpy(),
    )
    run_input = RUN_INPUTS[0][0]

    first = controller.submit(run_input, context=_context(), interaction_key="stable-form")
    repeated = controller.submit(run_input, context=_context(), interaction_key="stable-form")

    assert isinstance(first, CommandSubmissionReceipt)
    assert repeated == first
    assert first.job_id.int != 0
    assert len(spool.pending()) == 1


def test_submission_estimate_uses_the_same_plan_without_publishing_a_command() -> None:
    controller, _, commands, _, _ = _controller()

    estimate = controller.estimate_submission(
        RUN_INPUTS[0][0],
        context=_context(),
        as_of=NOW,
    )

    assert estimate.estimator == "static"
    assert estimate.remaining_shards == 2
    assert estimate.remaining_duration is not None
    assert estimate.remaining_duration.low_ms == 45_000
    assert estimate.remaining_duration.center_ms == 60_000
    assert estimate.remaining_duration.high_ms == 90_000
    assert commands.calls == []


@pytest.mark.parametrize("page_size", [20, 25])
def test_list_jobs_allows_only_ui_page_sizes(page_size: int) -> None:
    controller, reader, _, _, _ = _controller()
    filters = LabJobListFilters(keyword="n_shape")

    result = controller.list_jobs(filters=filters, page_size=page_size, cursor="next")

    assert result == "typed-page"
    assert reader.list_calls == [{"filters": filters, "limit": page_size, "cursor": "next"}]


@pytest.mark.parametrize("page_size", [1, 24, 26, True])
def test_list_jobs_rejects_non_ui_page_sizes(page_size: object) -> None:
    controller, reader, _, _, _ = _controller()

    with pytest.raises(ValueError, match="20 or 25"):
        controller.list_jobs(page_size=page_size)  # type: ignore[arg-type]
    assert reader.list_calls == []


def test_detail_uses_controller_bound_limits_and_dynamic_as_of() -> None:
    from rquant.dashboard.lab.job_center import (
        LAB_UI_ARTIFACT_LIMIT,
        LAB_UI_COMPLETED_TELEMETRY_LIMIT,
        LAB_UI_EVENT_LIMIT,
        LAB_UI_SHARD_LIMIT,
    )

    controller, reader, _, _, _ = _controller()

    result = controller.get_job_detail(JOB_ID, as_of=NOW)

    assert result == "typed-detail"
    assert reader.detail_calls == [
        (
            JOB_ID,
            {
                "as_of": NOW,
                "shard_limit": LAB_UI_SHARD_LIMIT,
                "event_limit": LAB_UI_EVENT_LIMIT,
                "artifact_limit": LAB_UI_ARTIFACT_LIMIT,
                "completed_telemetry_limit": LAB_UI_COMPLETED_TELEMETRY_LIMIT,
            },
        )
    ]


@pytest.mark.parametrize("operation", ["pause", "resume", "cancel", "retry"])
def test_control_methods_are_typed_thin_facade_calls(operation: str) -> None:
    controller, _, commands, _, _ = _controller()

    result = getattr(controller, operation)(
        JOB_ID,
        expected_version=4,
        reason=f"user {operation}",
        interaction_key=f"{operation}-1",
    )

    assert isinstance(result, CommandSubmissionConflict)
    assert commands.calls == [
        (
            operation,
            (JOB_ID,),
            {
                "expected_version": 4,
                "reason": f"user {operation}",
                "interaction_key": f"{operation}-1",
            },
        )
    ]


def test_rerun_uses_a_new_injectable_job_identity() -> None:
    controller, _, commands, _, _ = _controller()

    result = controller.rerun(
        SOURCE_JOB_ID,
        new_job_id=JOB_ID,
        max_attempts=2,
        interaction_key="rerun-1",
    )

    assert isinstance(result, CommandSubmissionConflict)
    assert commands.calls == [
        (
            "rerun",
            (SOURCE_JOB_ID,),
            {
                "new_job_id": JOB_ID,
                "max_attempts": 2,
                "interaction_key": "rerun-1",
            },
        )
    ]


def test_preview_and_export_accept_only_job_identity_and_bounded_table_name() -> None:
    from inspect import signature

    from rquant.dashboard.lab.job_center import (
        LAB_UI_PREVIEW_COLUMN_LIMIT,
        LAB_UI_PREVIEW_ROW_LIMIT,
    )

    controller, _, _, preview, exports = _controller()

    preview_result = controller.preview_artifact(JOB_ID, table_name="trades")
    export_result = controller.export_zip(JOB_ID)
    controller.discard_zip(export_result)

    assert isinstance(preview_result, ArtifactPreview)
    assert isinstance(export_result, LabJobZipExportReceipt)
    assert preview.calls == [
        (
            JOB_ID,
            {
                "table_name": "trades",
                "row_limit": LAB_UI_PREVIEW_ROW_LIMIT,
                "column_limit": LAB_UI_PREVIEW_COLUMN_LIMIT,
            },
        )
    ]
    assert exports.calls == [JOB_ID]
    assert exports.discarded == [export_result]
    assert tuple(signature(controller.export_zip).parameters) == ("job_id",)
    assert tuple(signature(controller.discard_zip).parameters) == ("receipt",)
    assert tuple(signature(controller.preview_artifact).parameters) == (
        "job_id",
        "table_name",
    )


def test_missing_job_results_remain_typed_or_none(tmp_path: Path) -> None:
    from rquant.dashboard.lab.job_center import StrategyLabJobCenterController
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore

    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    reader = LabJobReader(store.path)
    controller = StrategyLabJobCenterController(
        reader=reader,
        commands=LabCommandSubmissionFacade(
            reader=reader,
            spool=LabCommandSpool(tmp_path / "commands"),
        ),
        preview_reader=_PreviewSpy(),
        zip_exports=_ExportSpy(),
    )

    assert controller.get_job_detail(UUID(int=999), as_of=NOW) is None
    result = controller.pause(
        UUID(int=999),
        expected_version=0,
        reason="missing",
        interaction_key="missing-pause",
    )
    assert isinstance(result, CommandSubmissionConflict)
    assert result.reason == "job_not_found"


def test_controller_source_has_no_ui_runtime_or_unbounded_dependencies() -> None:
    source_path = (
        Path(__file__).parents[2] / "src" / "rquant" / "dashboard" / "lab" / "job_center.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    called_attributes: set[str] = set()
    called_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            called_attributes.add(node.func.attr)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            called_names.add(node.func.id)

    forbidden_modules = {
        "streamlit",
        "duckdb",
        "subprocess",
        "concurrent",
        "concurrent.futures",
        "rquant.dashboard.strategy_lab",
        "rquant.dashboard.strategy_lab_worker",
        "rquant.strategy_compare",
        "rquant.auction_gap_strategy",
        "rquant.growth_board_surge_strategy",
        "rquant.minute_replay",
        "rquant.optimizer",
        "rquant.settings",
    }
    assert not (imported & forbidden_modules)
    assert not any(name.startswith("rquant.adapter") for name in imported)
    assert "list_events" not in called_attributes
    assert "list_shards" not in called_attributes
    assert "list_artifacts" not in called_attributes
    assert "build_research_job_submission" in called_names
