from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ParameterKind,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)


def _parameters(*arguments: ResearchParameter) -> ResearchRunParameters:
    return ResearchRunParameters(
        strategy_name="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 14),
        arguments=arguments,
    )


def _snapshot() -> DatasetSnapshotIdentity:
    return DatasetSnapshotIdentity(
        snapshot_id="a" * 64,
        binding_hash="b" * 64,
    )


def _feature_contract() -> FeatureContractIdentity:
    return FeatureContractIdentity(
        contract_id="intraday-core",
        contract_version="v1",
        contract_hash="c" * 64,
    )


def _costs() -> ExecutionCostSpec:
    return ExecutionCostSpec(
        commission_bps=Decimal("2.5"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("0.1"),
        slippage_bps=Decimal("3"),
    )


def _spec(**overrides: object) -> ResearchRunSpec:
    values: dict[str, object] = {
        "job_type": ResearchJobType.STRATEGY_REPLAY,
        "parameters": _parameters(
            ResearchParameter(name="hold_days", kind=ParameterKind.INTEGER, value=3),
            ResearchParameter(
                name="vp_risk_only",
                kind=ParameterKind.BOOLEAN,
                value=True,
            ),
        ),
        "code_sha": "1" * 40,
        "dataset_snapshot": _snapshot(),
        "feature_contract": _feature_contract(),
        "execution_costs": _costs(),
        "random_seed": 20260724,
        "resource_class": ResourceClass.STANDARD,
        "deadline": datetime(2026, 7, 25, 2, tzinfo=UTC),
        "research_status": "comparable",
    }
    values.update(overrides)
    return ResearchRunSpec.model_validate(values)


def test_valid_spec_freezes_reproducibility_inputs() -> None:
    spec = _spec()

    assert spec.job_type is ResearchJobType.STRATEGY_REPLAY
    assert spec.code_sha == "1" * 40
    assert spec.dataset_snapshot == _snapshot()
    assert spec.feature_contract == _feature_contract()
    assert spec.execution_costs.slippage_bps == Decimal("3")
    assert spec.random_seed == 20260724
    assert spec.resource_class is ResourceClass.STANDARD
    assert spec.deadline == datetime(2026, 7, 25, 2, tzinfo=UTC)
    assert len(spec.spec_hash) == 64
    with pytest.raises(ValidationError, match="frozen"):
        spec.random_seed = 7  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("job_type", "unknown", "job_type"),
        ("code_sha", "1" * 39, "code_sha"),
        ("code_sha", "G" * 40, "code_sha"),
        ("random_seed", -1, "random_seed"),
        ("random_seed", 2**63, "random_seed"),
        ("resource_class", "unbounded", "resource_class"),
        ("deadline", datetime(2026, 7, 25, 2), "timezone-aware"),
    ],
)
def test_spec_rejects_invalid_contract_values(
    field: str,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        _spec(**{field: value})


def test_hash_is_stable_for_mapping_order_parameter_order_and_timezone() -> None:
    shanghai = timezone(timedelta(hours=8))
    first = _spec(
        parameters=_parameters(
            ResearchParameter(
                name="threshold",
                kind=ParameterKind.DECIMAL,
                value=Decimal("1.5000"),
            ),
            ResearchParameter(name="hold_days", kind=ParameterKind.INTEGER, value=3),
        ),
        deadline=datetime(2026, 7, 25, 10, tzinfo=shanghai),
    )
    reordered = ResearchRunSpec.model_validate(
        {
            "research_status": "comparable",
            "deadline": datetime(2026, 7, 25, 2, tzinfo=UTC),
            "resource_class": "standard",
            "random_seed": 20260724,
            "execution_costs": {
                "slippage_bps": "3.000",
                "transfer_fee_bps": "0.10",
                "stamp_duty_bps": "5.0",
                "commission_bps": "2.500",
            },
            "feature_contract": {
                "contract_hash": "c" * 64,
                "contract_version": "v1",
                "contract_id": "intraday-core",
            },
            "dataset_snapshot": {
                "binding_hash": "b" * 64,
                "snapshot_id": "a" * 64,
            },
            "code_sha": "1" * 40,
            "parameters": {
                "arguments": [
                    {"value": 3, "kind": "integer", "name": "hold_days"},
                    {"value": "1.5", "kind": "decimal", "name": "threshold"},
                ],
                "end_date": "2026-07-14",
                "start_date": "2026-04-01",
                "strategy_name": "n_shape",
            },
            "job_type": "strategy_replay",
        }
    )

    assert first.canonical_json() == reordered.canonical_json()
    assert first.spec_hash == reordered.spec_hash


def test_hash_changes_when_a_reproducibility_input_changes() -> None:
    base = _spec()
    changed_snapshot = DatasetSnapshotIdentity.model_validate(
        {
            **_snapshot().model_dump(mode="python"),
            "binding_hash": "d" * 64,
        }
    )

    assert _spec(random_seed=base.random_seed + 1).spec_hash != base.spec_hash
    assert _spec(code_sha="2" * 40).spec_hash != base.spec_hash
    assert _spec(dataset_snapshot=changed_snapshot).spec_hash != base.spec_hash


def test_spec_model_copy_revalidates_snapshot_grade_gate() -> None:
    comparable = _spec()

    with pytest.raises(ValidationError, match="immutable dataset snapshot"):
        comparable.model_copy(update={"dataset_snapshot": None})


def test_spec_model_copy_rejects_unvalidated_parameter_mapping() -> None:
    with pytest.raises(ValidationError, match="parameters"):
        _spec().model_copy(update={"parameters": {"strategy_name": "n_shape"}})


def test_spec_model_validate_revalidates_nested_model_instances() -> None:
    invalid_snapshot = _snapshot().model_copy(update={"snapshot_id": "bad"})
    payload = _spec().model_dump(mode="python")
    payload["dataset_snapshot"] = invalid_snapshot

    with pytest.raises(ValidationError, match="snapshot_id"):
        ResearchRunSpec.model_validate(payload)


def test_snapshot_gate_allows_only_exploratory_without_immutable_snapshot() -> None:
    exploratory = _spec(dataset_snapshot=None, research_status="exploratory")

    assert exploratory.dataset_snapshot is None
    with pytest.raises(ValidationError, match="immutable dataset snapshot"):
        _spec(dataset_snapshot=None, research_status="comparable")


@pytest.mark.parametrize("status", ["comparable", "paper_candidate", "monitor_approved"])
def test_immutable_snapshot_allows_higher_research_status(status: str) -> None:
    assert _spec(research_status=status).research_status == status


@pytest.mark.parametrize(
    "bad_value",
    [Decimal("NaN"), Decimal("Infinity"), float("nan"), float("inf")],
)
def test_numeric_inputs_must_be_finite(bad_value: object) -> None:
    with pytest.raises(ValidationError, match="finite"):
        _spec(
            execution_costs=ExecutionCostSpec(
                commission_bps=bad_value,
                stamp_duty_bps=0,
                transfer_fee_bps=0,
                slippage_bps=0,
            )
        )


def test_costs_and_parameter_values_reject_invalid_numeric_boundaries() -> None:
    with pytest.raises(ValidationError, match="commission_bps"):
        ExecutionCostSpec(
            commission_bps=Decimal("-0.01"),
            stamp_duty_bps=0,
            transfer_fee_bps=0,
            slippage_bps=0,
        )
    with pytest.raises(ValidationError, match="scalar"):
        ResearchParameter(name="weights", kind="decimal", value={"volume": 1})
    with pytest.raises(ValidationError, match="unique"):
        _parameters(
            ResearchParameter(name="hold_days", kind="integer", value=3),
            ResearchParameter(name="hold_days", kind="integer", value=5),
        )


def test_parameter_datetime_must_be_timezone_aware_and_is_canonical() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        ResearchParameter(
            name="as_of",
            kind="datetime",
            value=datetime(2026, 7, 24, 9, 30),
        )

    utc_value = ResearchParameter(
        name="as_of",
        kind="datetime",
        value=datetime(2026, 7, 24, 1, 30, tzinfo=UTC),
    )
    cst_value = ResearchParameter(
        name="as_of",
        kind="datetime",
        value=datetime(
            2026,
            7,
            24,
            9,
            30,
            tzinfo=timezone(timedelta(hours=8)),
        ),
    )

    assert (
        _spec(parameters=_parameters(utc_value)).spec_hash
        == _spec(parameters=_parameters(cst_value)).spec_hash
    )
