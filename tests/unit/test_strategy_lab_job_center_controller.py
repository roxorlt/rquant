from __future__ import annotations

import ast
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

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

    def export(self, job_id: UUID) -> LabJobZipExportReceipt:
        self.calls.append(job_id)
        return LabJobZipExportReceipt(
            request_id=UUID(int=901),
            job_id=job_id,
            path=Path("/tmp/export.zip"),
            byte_size=10,
            sha256="5" * 64,
        )


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
    assert tuple(signature(controller.export_zip).parameters) == ("job_id",)
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
