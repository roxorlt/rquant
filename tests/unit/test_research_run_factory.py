from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

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
from rquant.strategy_job_adapters import (
    AuctionGapParameters,
    GrowthBoardSurgeParameters,
    NShapeCompareParameters,
    NShapeOptimizeParameters,
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)


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


def test_factory_accepts_formal_only_with_exact_gate_snapshot_and_audit_evidence() -> None:
    built = build_research_job_submission(
        RUN_INPUTS[0],
        gate_decision=_gate(formal=True),
        code_sha="f" * 40,
        dataset_snapshot=_snapshot(),
        feature_contract=_contract(RUN_INPUTS[0], "f" * 40),
        execution_costs=_costs(),
        random_seed=11,
        resource_class=ResourceClass.HEAVY,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID(int=11),
    )

    assert built.spec.research_status == "comparable"
    assert built.spec.dataset_snapshot == _snapshot()
    assert built.spec.code_sha == "f" * 40

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
