from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from rquant.experiment_registry import DateRange, ExperimentSpec
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ParameterKind,
    ResearchExperimentIdentity,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
    StrategyExecutionIdentity,
)
from rquant.runtime_contracts import canonical_sha256


def _parameters(*arguments: ResearchParameter) -> ResearchRunParameters:
    return ResearchRunParameters(
        strategy_name="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 14),
        arguments=arguments,
    )


def _snapshot(
    *,
    audit_run_id: str | None = "d" * 64,
    include_audit: bool = True,
) -> DatasetSnapshotIdentity:
    values: dict[str, object] = {
        "snapshot_id": "a" * 64,
        "binding_hash": "b" * 64,
    }
    if include_audit:
        values["audit_run_id"] = audit_run_id
    return DatasetSnapshotIdentity.model_validate(values)


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


def _strategy_execution_identity() -> StrategyExecutionIdentity:
    return StrategyExecutionIdentity(
        strategy_id="n_shape",
        strategy_version=1,
        adapter_id="nshape-compare",
        adapter_version="1",
        strategy_spec_fingerprint="2" * 64,
        strategy_definition_fingerprint="3" * 64,
        strategy_executable_fingerprint="4" * 64,
        candidate_schema_fingerprint="5" * 64,
        definition_registration_record_hash="6" * 64,
        definition_registered_at=datetime(2026, 7, 20, tzinfo=UTC),
        definition_available_at=datetime(2026, 7, 20, tzinfo=UTC),
        producer_code_commit="1" * 40,
    )


def _experiment_identity() -> ResearchExperimentIdentity:
    parameters = _parameters(
        ResearchParameter(name="hold_days", kind=ParameterKind.INTEGER, value=3),
        ResearchParameter(
            name="vp_risk_only",
            kind=ParameterKind.BOOLEAN,
            value=True,
        ),
    )
    execution_model = {
        "contract": "lab-adapter-execution/v1",
        "adapter_id": "nshape-compare",
        "adapter_version": "1",
        "feature_contract": _feature_contract(),
    }
    spec = ExperimentSpec(
        strategy_spec_fingerprint="2" * 64,
        strategy_executable_fingerprint="4" * 64,
        candidate_schema_fingerprint="5" * 64,
        dataset_snapshot_id="a" * 64,
        code_commit="1" * 40,
        parameter_fingerprint=canonical_sha256(parameters),
        hypothesis_family="n-shape-hold-days",
        metric_definition_fingerprint="8" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 3, 31)),
        cost_model_fingerprint=canonical_sha256(_costs()),
        execution_model_fingerprint=canonical_sha256(execution_model),
        seed=20260724,
    )
    assert spec.experiment_id is not None
    return ResearchExperimentIdentity(
        spec=spec,
        experiment_id=spec.experiment_id,
        hypothesis_family="n-shape-hold-days",
        hypothesis_variant="hold-3",
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


def _decimal_parameter_spec(value: Decimal) -> ResearchRunSpec:
    return _spec(
        parameters=_parameters(
            ResearchParameter(name="threshold", kind="decimal", value=value),
        )
    )


def _v1_spec(**overrides: object) -> ResearchRunSpec:
    values = _spec().model_dump(mode="python", round_trip=True)
    values["schema_version"] = 1
    snapshot = values["dataset_snapshot"]
    assert isinstance(snapshot, dict)
    snapshot.pop("audit_run_id")
    values.update(overrides)
    return ResearchRunSpec.model_validate(values)


def _hidden_audit_v1_spec() -> ResearchRunSpec:
    base = _v1_spec()
    assert base.dataset_snapshot is not None
    hidden_snapshot = DatasetSnapshotIdentity.model_construct(
        snapshot_id=base.dataset_snapshot.snapshot_id,
        binding_hash=base.dataset_snapshot.binding_hash,
        audit_run_id="e" * 64,
        _fields_set={"snapshot_id", "binding_hash"},
    )
    values = {name: getattr(base, name) for name in type(base).model_fields}
    values["dataset_snapshot"] = hidden_snapshot
    return ResearchRunSpec.model_construct(
        **values,
        _fields_set=set(base.model_fields_set),
    )


def test_valid_spec_freezes_reproducibility_inputs() -> None:
    spec = _spec()

    assert spec.schema_version == 2
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


def test_v3_requires_first_class_strategy_and_experiment_identity() -> None:
    values = _spec().model_dump(mode="python", round_trip=True)
    values["schema_version"] = 3

    with pytest.raises(ValidationError, match="strategy_execution"):
        ResearchRunSpec.model_validate(values)

    values["strategy_execution"] = _strategy_execution_identity()
    with pytest.raises(ValidationError, match="experiment"):
        ResearchRunSpec.model_validate(values)

    values["experiment"] = _experiment_identity()
    spec = ResearchRunSpec.model_validate(values)

    assert spec.schema_version == 3
    assert spec.strategy_execution == _strategy_execution_identity()
    assert spec.experiment == _experiment_identity()
    assert not spec.catalog_owner_eligible

    current_experiment = ResearchExperimentIdentity.model_validate(
        {
            **_experiment_identity().model_dump(
                mode="python",
                exclude={"attempt_identity"},
            ),
            "schema_version": 2,
            "formal_plan_id": "9" * 64,
        }
    )
    values["experiment"] = current_experiment
    current = ResearchRunSpec.model_validate(values)

    assert current.catalog_owner_eligible


def test_legacy_run_specs_are_never_catalog_owners() -> None:
    assert not _spec().catalog_owner_eligible
    assert not _v1_spec().catalog_owner_eligible


def test_v3_strategy_execution_identity_must_match_run_and_code() -> None:
    values = _spec().model_dump(mode="python", round_trip=True)
    wrong_strategy = _strategy_execution_identity().model_dump(
        mode="python", exclude={"identity_hash"}
    )
    wrong_strategy["strategy_id"] = "auction_gap"
    values.update(
        schema_version=3,
        strategy_execution=wrong_strategy,
        experiment=_experiment_identity(),
    )

    with pytest.raises(ValidationError, match="strategy_id"):
        ResearchRunSpec.model_validate(values)

    wrong_commit = _strategy_execution_identity().model_dump(
        mode="python", exclude={"identity_hash"}
    )
    wrong_commit["producer_code_commit"] = "9" * 40
    values["strategy_execution"] = wrong_commit
    with pytest.raises(ValidationError, match="producer_code_commit"):
        ResearchRunSpec.model_validate(values)


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


@pytest.mark.parametrize(
    "schema_version",
    [0, 4, True, False, 1.0, 2.0, Decimal("1"), Decimal("2"), "1", "2"],
)
def test_schema_version_requires_exact_supported_integer(schema_version: object) -> None:
    with pytest.raises(
        ValidationError,
        match="schema_version must be integer 1, 2, or 3",
    ):
        _spec(schema_version=schema_version)


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
                "audit_run_id": "d" * 64,
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


def test_decimal_canonicalization_ignores_active_context_precision() -> None:
    spec = _decimal_parameter_spec(Decimal("123456789.123456789"))

    with localcontext() as context:
        context.prec = 3
        low_precision_json = spec.canonical_json()
        low_precision_hash = spec.spec_hash
    with localcontext() as context:
        context.prec = 50
        high_precision_json = spec.canonical_json()
        high_precision_hash = spec.spec_hash

    assert low_precision_json == high_precision_json
    assert low_precision_hash == high_precision_hash
    assert '"$decimal":"123456789.123456789"' in low_precision_json


def test_decimal_canonicalization_does_not_collapse_distinct_values() -> None:
    left = _decimal_parameter_spec(Decimal("1.2341"))
    right = _decimal_parameter_spec(Decimal("1.2342"))

    with localcontext() as context:
        context.prec = 4
        assert left.spec_hash != right.spec_hash


def test_decimal_canonicalization_normalizes_negative_zero() -> None:
    assert (
        _decimal_parameter_spec(Decimal("-0.000")).spec_hash
        == _decimal_parameter_spec(Decimal("0")).spec_hash
    )


def test_v2_canonical_json_and_hash_match_cross_python_golden_vector() -> None:
    expected_json = (
        '{"code_sha":"1111111111111111111111111111111111111111",'
        '"dataset_snapshot":{"audit_run_id":"dddddddddddddddddddddddddddddddddddddddd'
        'dddddddddddddddddddddddd","binding_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        'bbbbbbbbbbbbbbbbbbbbbbbb","snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaaaaaaaaaa"},"deadline":{"$datetime":"2026-07-25T02:00:00.'
        '000000Z"},"execution_costs":{"commission_bps":{"$decimal":"2.5"},'
        '"slippage_bps":{"$decimal":"3"},"stamp_duty_bps":{"$decimal":"5"},'
        '"transfer_fee_bps":{"$decimal":"0.1"}},"feature_contract":{"contract_hash":'
        '"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
        '"contract_id":"intraday-core","contract_version":"v1"},"job_type":'
        '"strategy_replay","parameters":{"arguments":[{"kind":"integer","name":'
        '"hold_days","value":3},{"kind":"boolean","name":"vp_risk_only","value":true}],'
        '"end_date":{"$date":"2026-07-14"},"start_date":{"$date":"2026-04-01"},'
        '"strategy_name":"n_shape"},"random_seed":20260724,"research_status":'
        '"comparable","resource_class":"standard","schema_version":2}'
    )

    assert _spec().canonical_json() == expected_json
    assert _spec().spec_hash == "2d8fa8e3f3e7a7aa5e43397c8f8fe99b2f37cc1076c915885ebbc0b215f7345f"


def test_v1_comparable_canonical_json_and_hash_match_9fd159e_golden() -> None:
    expected_json = (
        '{"code_sha":"1111111111111111111111111111111111111111",'
        '"dataset_snapshot":{"binding_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        'bbbbbbbbbbbbbbbbbbbbbbbb","snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaaaaaaaaaa"},"deadline":{"$datetime":"2026-07-25T02:00:00.'
        '000000Z"},"execution_costs":{"commission_bps":{"$decimal":"2.5"},'
        '"slippage_bps":{"$decimal":"3"},"stamp_duty_bps":{"$decimal":"5"},'
        '"transfer_fee_bps":{"$decimal":"0.1"}},"feature_contract":{"contract_hash":'
        '"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
        '"contract_id":"intraday-core","contract_version":"v1"},"job_type":'
        '"strategy_replay","parameters":{"arguments":[{"kind":"integer","name":'
        '"hold_days","value":3},{"kind":"boolean","name":"vp_risk_only","value":true}],'
        '"end_date":{"$date":"2026-07-14"},"start_date":{"$date":"2026-04-01"},'
        '"strategy_name":"n_shape"},"random_seed":20260724,"research_status":'
        '"comparable","resource_class":"standard","schema_version":1}'
    )

    spec = _v1_spec()

    assert spec.canonical_json() == expected_json
    assert "audit_run_id" not in spec.canonical_json()
    assert "audit_run_id" not in spec.model_dump_json(round_trip=True)
    assert spec.spec_hash == "5dfdef4d16e812237792efdff0613b551d4830793af8b828bc7aa1122cda6557"


def test_v1_exploratory_without_snapshot_keeps_9fd159e_golden_hash() -> None:
    spec = _v1_spec(dataset_snapshot=None, research_status="exploratory")

    assert spec.spec_hash == "99ed983adf2b06360aa278a1391041d5d2926babbb2bf94bfc071dceea55b495"


@pytest.mark.parametrize("value", [Decimal("1E+1000000"), Decimal("1E-1000000")])
def test_decimal_parameter_rejects_extreme_exponents(value: Decimal) -> None:
    with pytest.raises(ValidationError, match="exponent"):
        ResearchParameter(name="threshold", kind="decimal", value=value)


@pytest.mark.parametrize("value", [Decimal("1E+1000000"), Decimal("1E-1000000")])
def test_execution_cost_rejects_extreme_exponents(value: Decimal) -> None:
    with pytest.raises(ValidationError, match="exponent"):
        ExecutionCostSpec(
            commission_bps=value,
            stamp_duty_bps=0,
            transfer_fee_bps=0,
            slippage_bps=0,
        )


@pytest.mark.parametrize("target", ["parameter", "execution_cost"])
def test_decimal_inputs_reject_oversized_coefficients(target: str) -> None:
    value = Decimal("1" * 129)

    with pytest.raises(ValidationError, match="coefficient digits"):
        if target == "parameter":
            ResearchParameter(name="threshold", kind="decimal", value=value)
        else:
            ExecutionCostSpec(
                commission_bps=value,
                stamp_duty_bps=0,
                transfer_fee_bps=0,
                slippage_bps=0,
            )


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

    changed_audit = _snapshot(audit_run_id="e" * 64)
    assert _spec(dataset_snapshot=changed_audit).spec_hash != base.spec_hash


def test_spec_model_copy_revalidates_snapshot_grade_gate() -> None:
    comparable = _spec()

    with pytest.raises(ValidationError, match="immutable dataset snapshot"):
        comparable.model_copy(update={"dataset_snapshot": None})

    with pytest.raises(ValidationError, match="audit_run_id"):
        comparable.model_copy(update={"dataset_snapshot": _snapshot(audit_run_id=None)})


def test_v1_model_copy_preserves_legacy_canonical_shape() -> None:
    spec = _v1_spec()

    copied = spec.model_copy(update={"random_seed": 7})

    assert copied.schema_version == 1
    assert "audit_run_id" not in copied.canonical_json()
    assert copied.random_seed == 7


def test_v1_revalidation_rejects_hidden_audit_actual_value() -> None:
    unsafe = _hidden_audit_v1_spec()
    assert unsafe.dataset_snapshot is not None
    assert unsafe.dataset_snapshot.audit_run_id == "e" * 64
    assert "audit_run_id" not in unsafe.dataset_snapshot.model_fields_set

    with pytest.raises(ValidationError, match="v1.*audit_run_id"):
        ResearchRunSpec.model_validate(unsafe)

    base = _v1_spec()
    with pytest.raises(ValidationError, match="v1.*audit_run_id"):
        base.model_copy(update={"dataset_snapshot": unsafe.dataset_snapshot})


def test_hidden_v1_audit_cannot_use_legacy_spec_hash_collision() -> None:
    base = _v1_spec()
    unsafe = _hidden_audit_v1_spec()

    assert unsafe.spec_hash == base.spec_hash
    with pytest.raises(ValidationError, match="v1.*audit_run_id"):
        ResearchRunSpec.model_validate(unsafe)


@pytest.mark.parametrize(
    "schema_version",
    [True, False, 1.0, 2.0, Decimal("1"), Decimal("2"), "1", "2"],
)
def test_model_construct_revalidation_rejects_noninteger_schema_version(
    schema_version: object,
) -> None:
    base = _spec()
    values = {name: getattr(base, name) for name in type(base).model_fields}
    values["schema_version"] = schema_version
    unsafe = ResearchRunSpec.model_construct(
        **values,
        _fields_set=set(base.model_fields_set),
    )

    with pytest.raises(
        ValidationError,
        match="schema_version must be integer 1, 2, or 3",
    ):
        ResearchRunSpec.model_validate(unsafe)


def test_spec_model_copy_rejects_unvalidated_parameter_mapping() -> None:
    with pytest.raises(ValidationError, match="parameters"):
        _spec().model_copy(update={"parameters": {"strategy_name": "n_shape"}})


def test_spec_model_copy_preserves_shallow_and_deep_identity() -> None:
    spec = _spec()

    shallow = spec.model_copy(update={"random_seed": 7})
    deep = spec.model_copy(update={"random_seed": 7}, deep=True)

    assert shallow.parameters is spec.parameters
    assert shallow.dataset_snapshot is spec.dataset_snapshot
    assert deep.parameters is not spec.parameters
    assert deep.dataset_snapshot is not spec.dataset_snapshot


def test_spec_model_copy_preserves_fields_set_and_exclude_unset() -> None:
    payload = _spec().model_dump(mode="python")
    payload.pop("schema_version")
    payload.pop("research_status")
    spec = ResearchRunSpec.model_validate(payload)

    copied = spec.model_copy(update={"research_status": "comparable"})

    assert copied.model_fields_set == spec.model_fields_set | {"research_status"}
    assert "schema_version" not in copied.model_dump(exclude_unset=True)
    assert copied.model_dump(exclude_unset=True)["research_status"] == "comparable"


def test_spec_model_validate_revalidates_nested_model_instances() -> None:
    invalid_snapshot = _snapshot().model_copy(update={"snapshot_id": "bad"})
    payload = _spec().model_dump(mode="python")
    payload["dataset_snapshot"] = invalid_snapshot

    with pytest.raises(ValidationError, match="snapshot_id"):
        ResearchRunSpec.model_validate(payload)


def test_spec_model_validate_rejects_tampered_nested_audit_identity() -> None:
    invalid_snapshot = _snapshot().model_copy(update={"audit_run_id": "bad"})
    payload = _spec().model_dump(mode="python")
    payload["dataset_snapshot"] = invalid_snapshot

    with pytest.raises(ValidationError, match="audit_run_id"):
        ResearchRunSpec.model_validate(payload)


def test_snapshot_gate_allows_only_exploratory_without_immutable_snapshot() -> None:
    exploratory = _spec(dataset_snapshot=None, research_status="exploratory")

    assert exploratory.dataset_snapshot is None
    with pytest.raises(ValidationError, match="immutable dataset snapshot"):
        _spec(dataset_snapshot=None, research_status="comparable")


def test_comparable_research_requires_snapshot_audit_identity() -> None:
    with pytest.raises(ValidationError, match="audit_run_id"):
        _spec(
            dataset_snapshot=_snapshot(audit_run_id=None),
            research_status="comparable",
        )


def test_v1_comparable_accepts_snapshot_without_audit_identity() -> None:
    spec = _v1_spec()

    assert spec.schema_version == 1
    assert spec.research_status == "comparable"
    assert spec.dataset_snapshot is not None
    assert spec.dataset_snapshot.audit_run_id is None


def test_v1_keeps_legacy_snapshot_gate_above_exploratory() -> None:
    with pytest.raises(ValidationError, match="immutable dataset snapshot"):
        _v1_spec(dataset_snapshot=None, research_status="comparable")


@pytest.mark.parametrize("audit_run_id", [None, "d" * 64])
def test_v1_rejects_explicit_audit_field(audit_run_id: str | None) -> None:
    values = _spec().model_dump(mode="python", round_trip=True)
    values["schema_version"] = 1
    snapshot = values["dataset_snapshot"]
    assert isinstance(snapshot, dict)
    snapshot["audit_run_id"] = audit_run_id

    with pytest.raises(ValidationError, match="v1.*audit_run_id"):
        ResearchRunSpec.model_validate(values)


def test_exploratory_research_accepts_snapshot_without_audit_identity() -> None:
    spec = _spec(
        dataset_snapshot=_snapshot(audit_run_id=None),
        research_status="exploratory",
    )

    assert spec.dataset_snapshot is not None
    assert spec.dataset_snapshot.audit_run_id is None


@pytest.mark.parametrize("audit_run_id", ["d" * 63, "D" * 64, "g" * 64, "audit-latest"])
def test_snapshot_rejects_invalid_audit_run_id(audit_run_id: str) -> None:
    with pytest.raises(ValidationError, match="audit_run_id"):
        _snapshot(audit_run_id=audit_run_id)


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


def test_list_parameters_are_typed_unique_and_canonical() -> None:
    first = ResearchParameter(
        name="hold_days",
        kind="integer_list",
        value=[5, 1, 3],
    )
    second = ResearchParameter(
        name="hold_days",
        kind="integer_list",
        value=(3, 5, 1),
    )

    assert first.value == (1, 3, 5)
    assert first == second

    with pytest.raises(ValidationError, match="unique"):
        ResearchParameter(
            name="hold_days",
            kind="integer_list",
            value=[1, 1],
        )
    with pytest.raises(ValidationError, match="integer"):
        ResearchParameter(
            name="hold_days",
            kind="integer_list",
            value=[1, True],
        )
    with pytest.raises(ValidationError, match="string"):
        ResearchParameter(
            name="variants",
            kind="text_list",
            value=["full", 1],
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


@pytest.mark.parametrize("value", [0, 0.5, date(2026, 7, 25), "2026-07-25"])
def test_deadline_rejects_non_datetime_inputs(value: object) -> None:
    with pytest.raises(ValidationError, match="ISO datetime"):
        _spec(deadline=value)


def test_temporal_iso_strings_are_parsed_and_normalized() -> None:
    parameters = ResearchRunParameters.model_validate(
        {
            "strategy_name": "n_shape",
            "start_date": "2026-04-01",
            "end_date": "2026-07-14",
            "arguments": [
                {
                    "name": "as_of",
                    "kind": "datetime",
                    "value": "2026-07-24T09:30:00+08:00",
                },
                {"name": "signal_date", "kind": "date", "value": "2026-07-24"},
            ],
        }
    )
    spec = _spec(
        parameters=parameters,
        deadline="2026-07-25T10:00:00+08:00",
    )

    assert spec.deadline == datetime(2026, 7, 25, 2, tzinfo=UTC)
    assert parameters.start_date == date(2026, 4, 1)
    assert parameters.arguments[0].value == datetime(2026, 7, 24, 1, 30, tzinfo=UTC)
    assert parameters.arguments[1].value == date(2026, 7, 24)


@pytest.mark.parametrize("field", ["start_date", "end_date"])
@pytest.mark.parametrize("value", [0, 0.5, datetime(2026, 4, 1, tzinfo=UTC)])
def test_research_date_range_rejects_non_civil_dates(field: str, value: object) -> None:
    values: dict[str, object] = {
        "strategy_name": "n_shape",
        "start_date": date(2026, 4, 1),
        "end_date": date(2026, 7, 14),
    }
    values[field] = value

    with pytest.raises(ValidationError, match="civil date"):
        ResearchRunParameters.model_validate(values)


@pytest.mark.parametrize(
    ("kind", "value", "message"),
    [
        ("datetime", 0, "datetime"),
        ("datetime", 0.5, "datetime"),
        ("date", 0, "civil date"),
        ("date", datetime(2026, 7, 24, tzinfo=UTC), "civil date"),
    ],
)
def test_typed_temporal_parameters_reject_numeric_and_cross_type_inputs(
    kind: str,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        ResearchParameter(name="as_of", kind=kind, value=value)
