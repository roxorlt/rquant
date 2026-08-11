from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from rquant.experiment_registry import DateRange, ExperimentSpec, FormalExperimentPlan
from rquant.lab_job_center import (
    AuctionGapRunInput,
    GrowthBoardSurgeRunInput,
    NShapeComparisonRunInput,
    NShapeOptimizationRunInput,
    build_research_job_submission,
)
from rquant.research_gate import ResearchGateDecision, ResearchGateFailure
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResourceClass,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_job_adapters import (
    AuctionGapParameters,
    GrowthBoardSurgeParameters,
    NShapeCompareParameters,
    NShapeOptimizeParameters,
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)

from .test_lab_job_center import _v3_strategy_registration


def _gate(*, formal: bool, allowed: bool = True) -> ResearchGateDecision:
    return ResearchGateDecision(
        allowed=allowed,
        research_status="comparable" if formal else "exploratory",
        audit_run_id="d" * 64 if formal else None,
        dataset_snapshot_id="a" * 64 if formal else None,
        dataset_binding_hash="b" * 64 if formal else None,
        coverage_ratios={},
        coverage_counts={},
        failures=(
            () if allowed else (ResearchGateFailure(code="blocked", message="gate rejected"),)
        ),
    )


def _snapshot() -> DatasetSnapshotIdentity:
    return DatasetSnapshotIdentity(
        snapshot_id="a" * 64,
        binding_hash="b" * 64,
        audit_run_id="d" * 64,
    )


def _contract(run_input: object = None, code_sha: str = "1" * 40) -> FeatureContractIdentity:
    adapter_id = {
        NShapeComparisonRunInput: "nshape-compare",
        NShapeOptimizationRunInput: "nshape-optimize",
        AuctionGapRunInput: "auction-gap",
        GrowthBoardSurgeRunInput: "growth-board-surge",
    }.get(type(run_input), "nshape-compare")
    return build_adapter_execution_contract(adapter_id, "1", code_sha)


def _costs() -> ExecutionCostSpec:
    return ExecutionCostSpec(
        commission_bps=Decimal("2.5"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("0.1"),
        slippage_bps=Decimal("3"),
    )


RUN_INPUTS = (
    NShapeComparisonRunInput(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 2, 1),
        parameters=NShapeCompareParameters(
            hold_days=(1, 3),
            entry_modes=("first_break",),
        ),
    ),
    NShapeOptimizationRunInput(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 2, 1),
        parameters=NShapeOptimizeParameters(
            hold_days=(1, 3),
            entry_modes=("first_break",),
            profile_variants=("baseline",),
        ),
    ),
    AuctionGapRunInput(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 2, 1),
        parameters=AuctionGapParameters(max_hold_days=2),
    ),
    GrowthBoardSurgeRunInput(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 2, 1),
        parameters=GrowthBoardSurgeParameters(
            variants=("full", "no_vwap"),
            max_hold_days=2,
        ),
    ),
)


def _build(run_input: object, *, resource_class: ResourceClass = ResourceClass.STANDARD):
    return build_research_job_submission(
        run_input,
        gate_decision=_gate(formal=False),
        code_sha="1" * 40,
        dataset_snapshot=None,
        feature_contract=_contract(run_input),
        execution_costs=_costs(),
        random_seed=7,
        resource_class=resource_class,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID(int=99),
    )


@pytest.mark.parametrize("run_input", RUN_INPUTS)
def test_factory_builds_canonical_adapter_compatible_spec_for_all_typed_inputs(
    run_input: object,
) -> None:
    built = build_research_job_submission(
        run_input,
        gate_decision=_gate(formal=False),
        code_sha="1" * 40,
        dataset_snapshot=None,
        feature_contract=_contract(run_input),
        execution_costs=_costs(),
        random_seed=7,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID(int=10),
        max_attempts=3,
    )

    assert built.command.job_id == UUID(int=10)
    assert built.command.spec == built.spec
    assert built.command.max_attempts == 3
    assert built.spec.research_status == "exploratory"
    assert built.spec.dataset_snapshot is None
    assert built.spec.spec_hash == built.command.spec.spec_hash
    assert default_strategy_job_adapter_registry().plan(built.spec)


def test_factory_preflights_all_four_inputs_with_canonical_adapter_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = default_strategy_job_adapter_registry()
    original_plan = registry.plan
    planned_hashes: list[str] = []

    def record_plan(spec):  # type: ignore[no-untyped-def]
        planned_hashes.append(spec.spec_hash)
        return original_plan(spec)

    monkeypatch.setattr(registry, "plan", record_plan)

    built = tuple(_build(run_input) for run_input in RUN_INPUTS)

    assert planned_hashes == [item.spec.spec_hash for item in built]


@pytest.mark.parametrize(
    "run_input",
    [
        NShapeComparisonRunInput(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 2, 1),
            parameters=NShapeCompareParameters(
                hold_days=(1,),
                entry_modes=("first_break",) * 1_000,
            ),
        ),
        NShapeOptimizationRunInput(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 2, 1),
            parameters=NShapeOptimizeParameters(
                hold_days=(1,),
                entry_modes=("first_break",),
                profile_variants=("baseline",),
                walk_forward_folds=2**63,
            ),
        ),
        AuctionGapRunInput(
            start_date=date(2010, 1, 1),
            end_date=date(2026, 1, 1),
            parameters=AuctionGapParameters(max_hold_days=2),
        ),
        GrowthBoardSurgeRunInput(
            start_date=date(2024, 1, 1),
            end_date=date(2026, 1, 1),
            parameters=GrowthBoardSurgeParameters(
                variants=("full", "no_vwap", "no_same_minute", "no_accel_5m", "cum_only"),
                max_hold_days=2,
            ),
        ),
    ],
    ids=["comparison-cardinality", "optimization-folds", "auction-range", "growth-shards"],
)
def test_factory_rejects_unplannable_resource_inputs_with_typed_error(
    run_input: object,
) -> None:
    with pytest.raises(ValueError, match="submission preflight") as exc_info:
        _build(run_input, resource_class=ResourceClass.HEAVY)

    assert type(exc_info.value).__name__ == "ResearchJobSubmissionError"
    assert exc_info.value.code in {  # type: ignore[attr-defined]
        "input_bounds",
        "adapter_plan",
        "shard_budget",
        "resource_budget",
    }


def test_factory_rejects_plan_that_exceeds_resource_work_budget() -> None:
    run_input = NShapeOptimizationRunInput(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 2, 1),
        parameters=NShapeOptimizeParameters(
            hold_days=tuple(range(1, 21)),
            entry_modes=(
                "first_break",
                "break_retest",
                "late_confirm",
                "vwap_confirm",
                "amount_surge",
                "factor_confirm",
            ),
            profile_variants=("baseline", "vp_risk_only", "vp_90"),
            top_n_options=tuple(range(1, 33)),
            score_profile_names=(
                "v1",
                "no_intraday",
                "no_accumulation",
                "no_position",
                "no_market",
                "intraday_heavy",
                "accumulation_heavy",
                "position_heavy",
                "v2_low_position",
                "v2_momentum",
                "v2_env_gate",
            ),
            walk_forward_folds=64,
        ),
    )

    with pytest.raises(ValueError, match="submission preflight") as exc_info:
        _build(run_input, resource_class=ResourceClass.HEAVY)

    assert type(exc_info.value).__name__ == "ResearchJobSubmissionError"
    assert exc_info.value.code == "resource_budget"  # type: ignore[attr-defined]


def test_factory_accepts_formal_only_with_exact_trusted_ownership(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="trusted strategy registration"):
        build_research_job_submission(
            RUN_INPUTS[0],
            gate_decision=_gate(formal=True),
            code_sha="1" * 40,
            dataset_snapshot=_snapshot(),
            feature_contract=_contract(RUN_INPUTS[0]),
            execution_costs=_costs(),
            random_seed=11,
            resource_class=ResourceClass.HEAVY,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            job_id=UUID(int=11),
        )

    provisional = build_research_job_submission(
        RUN_INPUTS[0],
        gate_decision=_gate(formal=False),
        code_sha="1" * 40,
        dataset_snapshot=_snapshot(),
        feature_contract=_contract(RUN_INPUTS[0]),
        execution_costs=_costs(),
        random_seed=11,
        resource_class=ResourceClass.HEAVY,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID(int=11),
    )
    registration = _v3_strategy_registration(tmp_path)
    experiment = ExperimentSpec(
        strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        dataset_snapshot_id=_snapshot().snapshot_id,
        code_commit="1" * 40,
        parameter_fingerprint=canonical_sha256(provisional.spec.parameters),
        hypothesis_family="factory-formal",
        metric_definition_fingerprint="e" * 64,
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
        cost_model_fingerprint=canonical_sha256(_costs()),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": "nshape-compare",
                "adapter_version": "1",
                "feature_contract": _contract(RUN_INPUTS[0]),
            }
        ),
        seed=11,
    )
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=experiment,
        hypothesis_variant="baseline",
        strategy_definition_fingerprint=registration.fingerprint,
        definition_registration_record_hash=registration.record_hash,
        preregistered_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    built = build_research_job_submission(
        RUN_INPUTS[0],
        gate_decision=_gate(formal=True),
        code_sha="1" * 40,
        dataset_snapshot=_snapshot(),
        feature_contract=_contract(RUN_INPUTS[0]),
        execution_costs=_costs(),
        random_seed=11,
        resource_class=ResourceClass.HEAVY,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID(int=11),
        trusted_strategy_registration=registration,
        formal_experiment_plan=plan,
    )

    assert built.spec.schema_version == 3
    assert built.spec.catalog_owner_eligible
    assert built.spec.research_status == "comparable"
    assert built.spec.dataset_snapshot == _snapshot()
    assert built.spec.code_sha == "1" * 40

    with pytest.raises(ValueError, match="snapshot"):
        build_research_job_submission(
            RUN_INPUTS[0],
            gate_decision=_gate(formal=True),
            code_sha="f" * 40,
            dataset_snapshot=None,
            feature_contract=_contract(),
            execution_costs=_costs(),
            random_seed=11,
            resource_class=ResourceClass.HEAVY,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            job_id=UUID(int=11),
        )


@pytest.mark.parametrize("code_sha", ["1" * 39, "1" * 40 + "-dirty", "abc-dirty"])
def test_factory_rejects_short_or_dirty_sha_without_truncating_it(code_sha: str) -> None:
    with pytest.raises(ValueError, match="code SHA"):
        build_research_job_submission(
            RUN_INPUTS[0],
            gate_decision=_gate(formal=False),
            code_sha=code_sha,
            dataset_snapshot=None,
            feature_contract=_contract(),
            execution_costs=_costs(),
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            job_id=UUID(int=12),
        )


def test_factory_rejects_denied_or_mismatched_formal_gate() -> None:
    denied = _gate(formal=True, allowed=False)
    with pytest.raises(ValueError, match="gate"):
        build_research_job_submission(
            RUN_INPUTS[0],
            gate_decision=denied,
            code_sha="1" * 40,
            dataset_snapshot=_snapshot(),
            feature_contract=_contract(),
            execution_costs=_costs(),
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            job_id=UUID(int=12),
        )

    mismatched = _snapshot().model_copy(update={"binding_hash": "e" * 64})
    with pytest.raises(ValueError, match="binding"):
        build_research_job_submission(
            RUN_INPUTS[0],
            gate_decision=_gate(formal=True),
            code_sha="1" * 40,
            dataset_snapshot=mismatched,
            feature_contract=_contract(),
            execution_costs=_costs(),
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            job_id=UUID(int=12),
        )
