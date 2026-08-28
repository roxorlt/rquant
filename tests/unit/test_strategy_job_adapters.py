from __future__ import annotations

import hashlib
import json
import tracemalloc
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.lab_shard_protocol import (
    LabClaimDeliveryReceipt,
    LabClaimHighWater,
    LabShardClaim,
    LabShardDefinition,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)

_P13_PLAN_HASH = "dbab9770704e67d6ca06c73cc59e02fc7fd1430ce6ea26be00eff4e624543a49"
_P13_SHARD_ID = UUID("688270f0-2359-5276-b9f3-7338a0c3254e")
_P13_ENVELOPE_HASH = "723899d882f2f4bfda6b335d17b4a16c62a920f985cb8ecd72177686c8ac6cb1"
_P13_CLAIM_JSON = r"""{"schema_version":1,"job_id":"11111111-2222-3333-4444-555555555555","spec_hash":"452390fb85bd62aac6b02eb89b45fe1773c0b916cdd992b20ef5750d322bef7c","definition":{"schema_version":1,"shard_id":"688270f0-2359-5276-b9f3-7338a0c3254e","shard_index":0,"adapter_id":"nshape-compare","adapter_version":"1","plan_hash":"dbab9770704e67d6ca06c73cc59e02fc7fd1430ce6ea26be00eff4e624543a49","payload_json":"{\"adapter_id\":\"nshape-compare\",\"adapter_version\":\"1\",\"schema_version\":1,\"shard\":{\"hold_days\":1,\"kind\":\"hold_days\"},\"spec\":{\"code_sha\":\"1111111111111111111111111111111111111111\",\"dataset_snapshot\":null,\"deadline\":\"2026-08-01T00:00:00Z\",\"execution_costs\":{\"commission_bps\":\"0\",\"slippage_bps\":\"0\",\"stamp_duty_bps\":\"0\",\"transfer_fee_bps\":\"0\"},\"feature_contract\":{\"contract_hash\":\"ae9dfef2a24213b119b3b71e1a63b05b62bf0df1083660e8ea52edfd42095daf\",\"contract_id\":\"strategy-adapter-execution\",\"contract_version\":\"p13b-adapter-v1\"},\"job_type\":\"strategy_replay\",\"parameters\":{\"arguments\":[{\"kind\":\"text_list\",\"name\":\"entry_modes\",\"value\":[\"first_break\",\"late_confirm\"]},{\"kind\":\"integer_list\",\"name\":\"hold_days\",\"value\":[1]},{\"kind\":\"text_list\",\"name\":\"profile_variants\",\"value\":[\"baseline\"]}],\"end_date\":\"2026-02-10\",\"start_date\":\"2026-01-01\",\"strategy_name\":\"n_shape\"},\"random_seed\":20260724,\"research_status\":\"exploratory\",\"resource_class\":\"standard\",\"schema_version\":2}}","payload_hash":"054ccf2d5b6423c8791553d7e16de6b948ffe792e6ddee8a73936f1a18f3c45c"},"worker_id":"worker-legacy","claim_token":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","claim_generation":1,"scheduler_fencing_token":7,"claimed_at":"2026-07-24T00:00:00Z","lease_expires_at":"2026-07-24T00:05:00Z"}"""  # noqa: E501


def test_job_result_hash_streaming_matches_legacy_bytes_and_bounds_memory() -> None:
    from rquant.strategy_job_adapters import LabJobExecutionResult, LabShardTable

    frame = pd.DataFrame(
        {
            "code": [f"{index:06d}.SZ" for index in range(100_000)],
            "ret_pct": [index / 1000 for index in range(100_000)],
        }
    )
    result = LabJobExecutionResult(
        spec_hash="1" * 64,
        plan_hash="2" * 64,
        adapter_id="streaming-test",
        adapter_version="1",
        tables=(LabShardTable(name="trades", frame=frame),),
    )
    small = frame.iloc[:3]
    small_result = result.model_copy(
        update={"tables": (LabShardTable(name="trades", frame=small),)}
    )
    legacy_payload = {
        "adapter_id": small_result.adapter_id,
        "adapter_version": small_result.adapter_version,
        "plan_hash": small_result.plan_hash,
        "spec_hash": small_result.spec_hash,
        "tables": [
            {
                "frame": small.to_json(
                    orient="split",
                    date_format="iso",
                    date_unit="us",
                    double_precision=15,
                    force_ascii=True,
                    index=False,
                ),
                "name": "trades",
            }
        ],
    }
    expected = hashlib.sha256(
        json.dumps(
            legacy_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert small_result.result_hash == expected

    frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())
    tracemalloc.start()
    _ = result.result_hash
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak <= max(8 * 1024 * 1024, int(frame_bytes * 1.75))


def test_job_result_hash_bounds_wide_string_scratch() -> None:
    from rquant.strategy_job_adapters import LabJobExecutionResult, LabShardTable

    value = "x" * (64 * 1024)
    frame = pd.DataFrame({"wide": [value] * 1024})
    result = LabJobExecutionResult(
        spec_hash="1" * 64,
        plan_hash="2" * 64,
        adapter_id="wide-streaming-test",
        adapter_version="1",
        tables=(LabShardTable(name="trades", frame=frame),),
    )

    tracemalloc.start()
    digest = result.result_hash
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest) == 64
    assert peak <= 8 * 1024 * 1024


@pytest.mark.parametrize(
    "categorical",
    [
        pd.Categorical(pd.Series([1, 2, 1], dtype="Int64")),
        pd.Categorical(pd.Series([True, False, True], dtype="boolean")),
        pd.Categorical(pd.Series([1.25, 2.5, 1.25], dtype="Float64")),
        pd.Categorical(pd.to_datetime(["2026-01-01", "2026-01-02"])),
    ],
    ids=["integer", "boolean", "float", "timestamp"],
)
def test_job_result_hash_preserves_categorical_scalar_semantics(
    categorical: pd.Categorical,
) -> None:
    from rquant.strategy_job_adapters import LabJobExecutionResult, LabShardTable

    frame = pd.DataFrame({"value": categorical})
    result = LabJobExecutionResult(
        spec_hash="1" * 64,
        plan_hash="2" * 64,
        adapter_id="categorical-streaming-test",
        adapter_version="1",
        tables=(LabShardTable(name="trades", frame=frame),),
    )
    legacy_payload = {
        "adapter_id": result.adapter_id,
        "adapter_version": result.adapter_version,
        "plan_hash": result.plan_hash,
        "spec_hash": result.spec_hash,
        "tables": [
            {
                "frame": frame.to_json(
                    orient="split",
                    date_format="iso",
                    date_unit="us",
                    double_precision=15,
                    force_ascii=True,
                    index=False,
                ),
                "name": "trades",
            }
        ],
    }
    expected = hashlib.sha256(
        json.dumps(
            legacy_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()

    assert result.result_hash == expected


def _p13_frozen_claim() -> LabShardClaim:
    return LabShardClaim.model_validate_json(_P13_CLAIM_JSON)


def _parameter(name: str, kind: str, value: object) -> ResearchParameter:
    return ResearchParameter(name=name, kind=kind, value=value)


def _spec(
    strategy_name: str,
    *arguments: ResearchParameter,
    job_type: ResearchJobType = ResearchJobType.STRATEGY_REPLAY,
    start_date: date = date(2026, 1, 1),
    end_date: date = date(2026, 2, 10),
) -> ResearchRunSpec:
    from rquant.strategy_job_adapters import build_adapter_execution_contract

    adapter_id = {
        ("n_shape", ResearchJobType.STRATEGY_REPLAY): "nshape-compare",
        ("n_shape", ResearchJobType.PARAMETER_SEARCH): "nshape-optimize",
        ("auction_gap", ResearchJobType.STRATEGY_REPLAY): "auction-gap",
        ("growth_board_surge", ResearchJobType.STRATEGY_REPLAY): "growth-board-surge",
    }[(strategy_name, job_type)]
    return ResearchRunSpec(
        job_type=job_type,
        parameters=ResearchRunParameters(
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            arguments=arguments,
        ),
        code_sha="1" * 40,
        dataset_snapshot=None,
        feature_contract=build_adapter_execution_contract(adapter_id, "1", "1" * 40),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("0"),
            stamp_duty_bps=Decimal("0"),
            transfer_fee_bps=Decimal("0"),
            slippage_bps=Decimal("0"),
        ),
        random_seed=20260724,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        research_status="exploratory",
    )


def _claim(spec: ResearchRunSpec, shard_index: int = 0) -> LabShardClaim:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    definition = default_strategy_job_adapter_registry().plan(spec)[shard_index]
    claimed_at = datetime(2026, 7, 24, tzinfo=UTC)
    return LabShardClaim(
        job_id=uuid4(),
        spec_hash=spec.spec_hash,
        definition=definition,
        worker_id="worker-a",
        claim_token=uuid4(),
        claim_generation=1,
        scheduler_fencing_token=7,
        claimed_at=claimed_at,
        lease_expires_at=claimed_at + timedelta(minutes=5),
    )


def _nshape_compare_spec(*, hold_days: tuple[int, ...] = (1, 3, 5)) -> ResearchRunSpec:
    return _spec(
        "n_shape",
        _parameter("hold_days", "integer_list", hold_days),
        _parameter("entry_modes", "text_list", ("late_confirm", "first_break")),
        _parameter("profile_variants", "text_list", ("baseline",)),
    )


def _nshape_optimize_spec(*, hold_days: tuple[int, ...] = (1, 3, 5)) -> ResearchRunSpec:
    return _spec(
        "n_shape",
        _parameter("hold_days", "integer_list", hold_days),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        _parameter("top_n_options", "integer_list", (1,)),
        _parameter("score_profile_names", "text_list", ("v1",)),
        job_type=ResearchJobType.PARAMETER_SEARCH,
    )


def _auction_spec() -> ResearchRunSpec:
    return _spec(
        "auction_gap",
        _parameter("max_hold_days", "integer", 1),
    )


def _growth_spec(*, variants: tuple[str, ...] = ("no_vwap", "full")) -> ResearchRunSpec:
    return _spec(
        "growth_board_surge",
        _parameter("variants", "text_list", variants),
        _parameter("max_hold_days", "integer", 1),
    )


def _nonzero_costs() -> ExecutionCostSpec:
    return ExecutionCostSpec(
        commission_bps=Decimal("10"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("1"),
        slippage_bps=Decimal("2"),
    )


def _notional_costs() -> ExecutionCostSpec:
    return ExecutionCostSpec(
        schema_version=2,
        commission_bps=Decimal("10"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("1"),
        slippage_bps=Decimal("2"),
        minimum_commission=Decimal("5"),
        research_notional_per_trade=Decimal("100000"),
    )


def _legacy_strategy_spec(spec: ResearchRunSpec, strategy_name: str) -> ResearchRunSpec:
    return spec.model_copy(
        update={"parameters": spec.parameters.model_copy(update={"strategy_name": strategy_name})}
    )


@pytest.mark.parametrize(
    ("spec", "expected_adapter"),
    [
        (_nshape_compare_spec(), "nshape-compare"),
        (_nshape_optimize_spec(), "nshape-optimize"),
        (_auction_spec(), "auction-gap"),
        (_growth_spec(), "growth-board-surge"),
    ],
)
def test_registry_plans_all_supported_strategy_jobs(
    spec: ResearchRunSpec,
    expected_adapter: str,
) -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    definitions = default_strategy_job_adapter_registry().plan(spec)

    assert definitions
    assert {item.adapter_id for item in definitions} == {expected_adapter}
    assert [item.shard_index for item in definitions] == list(range(len(definitions)))
    assert len({item.plan_hash for item in definitions}) == 1


@pytest.mark.parametrize(
    ("spec", "phase", "work_unit_name", "expected_units"),
    [
        (_nshape_compare_spec(hold_days=(1,)), "nshape_compare", "parameter_case", 2),
        (_nshape_optimize_spec(hold_days=(1,)), "nshape_optimize", "parameter_case", 1),
        (_auction_spec(), "auction_gap_replay", "calendar_day", 20),
        (_growth_spec(variants=("full",)), "growth_board_surge_replay", "calendar_day", 20),
    ],
)
def test_registry_maps_all_contract_jobs_to_explicit_work_plans(
    spec: ResearchRunSpec,
    phase: str,
    work_unit_name: str,
    expected_units: int,
) -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    definitions = default_strategy_job_adapter_registry().plan(spec)

    assert definitions
    assert all(item.work_plan is not None for item in definitions)
    first = definitions[0].work_plan
    assert first is not None
    assert first.phase == phase
    assert first.work_unit_name == work_unit_name
    assert first.work_units == expected_units
    assert first.static_duration_ms > 0


@pytest.mark.parametrize(
    ("spec", "legacy_name"),
    [
        (_nshape_compare_spec(hold_days=(1,)), "NShapeCompare"),
        (_nshape_optimize_spec(hold_days=(1,)), "NShapeOptimize"),
        (_auction_spec(), "AuctionGap"),
        (_growth_spec(variants=("full",)), "GrowthBoardSurge"),
    ],
)
def test_legacy_aliases_keep_the_same_typed_work_plan_mapping(
    spec: ResearchRunSpec,
    legacy_name: str,
) -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    registry = default_strategy_job_adapter_registry()

    legacy = registry.plan(_legacy_strategy_spec(spec, legacy_name))
    canonical = registry.plan(spec)

    assert tuple(item.work_plan for item in legacy) == tuple(item.work_plan for item in canonical)


def test_registry_selects_n_shape_adapter_by_job_type_and_plans_formal_spec() -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    registry = default_strategy_job_adapter_registry()
    compare = _nshape_compare_spec(hold_days=(1,))
    optimize = _nshape_optimize_spec(hold_days=(1,))
    formal = compare.model_copy(
        update={
            "dataset_snapshot": DatasetSnapshotIdentity(
                snapshot_id="a" * 64,
                binding_hash="b" * 64,
                audit_run_id="c" * 64,
            ),
            "research_status": "comparable",
        }
    )

    assert registry.for_spec(compare).adapter_id == "nshape-compare"
    assert registry.for_spec(optimize).adapter_id == "nshape-optimize"
    assert registry.plan(formal)


@pytest.mark.parametrize(
    ("spec", "snapshot_strategy"),
    [
        (_nshape_compare_spec(), "n_shape"),
        (_nshape_optimize_spec(), "n_shape"),
        (_auction_spec(), "auction_gap"),
        (_growth_spec(), "growth_board_surge"),
    ],
)
def test_adapter_declares_snapshot_strategy_mapping(
    spec: ResearchRunSpec,
    snapshot_strategy: str,
) -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    assert (
        default_strategy_job_adapter_registry().for_spec(spec).snapshot_strategy_name
        == snapshot_strategy
    )


@pytest.mark.parametrize(
    ("canonical", "legacy_name", "adapter_id", "snapshot_strategy"),
    [
        (_nshape_compare_spec(), "NShapeCompare", "nshape-compare", "n_shape"),
        (_nshape_optimize_spec(), "NShapeOptimize", "nshape-optimize", "n_shape"),
        (_auction_spec(), "AuctionGap", "auction-gap", "auction_gap"),
        (
            _growth_spec(),
            "GrowthBoardSurge",
            "growth-board-surge",
            "growth_board_surge",
        ),
    ],
)
def test_registry_supports_versioned_legacy_strategy_aliases(
    canonical: ResearchRunSpec,
    legacy_name: str,
    adapter_id: str,
    snapshot_strategy: str,
) -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    legacy = _legacy_strategy_spec(canonical, legacy_name)
    original_hash = legacy.spec_hash
    registry = default_strategy_job_adapter_registry()

    adapter = registry.for_spec(legacy)
    definitions = registry.plan(legacy)
    formal = legacy.model_copy(
        update={
            "dataset_snapshot": DatasetSnapshotIdentity(
                snapshot_id="a" * 64,
                binding_hash="b" * 64,
                audit_run_id="c" * 64,
            ),
            "research_status": "comparable",
        }
    )

    assert adapter.adapter_id == adapter_id
    assert adapter.snapshot_strategy_name == snapshot_strategy
    assert definitions
    assert legacy.spec_hash == original_hash
    assert registry.for_spec(formal) is adapter
    assert registry.plan(formal)


def test_legacy_inflight_claim_rebuilds_and_executes_without_spec_rewrite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.strategy_job_adapters import (
        LabShardExecutionResult,
        LabShardTable,
        NShapeCompareAdapter,
        ValidatedStrategyShard,
        default_strategy_job_adapter_registry,
    )

    legacy = _legacy_strategy_spec(_nshape_compare_spec(hold_days=(1,)), "NShapeCompare")
    claim = _claim(legacy)
    registry = default_strategy_job_adapter_registry()
    executions = 0

    def execute_fixture(
        _self: NShapeCompareAdapter,
        validated: ValidatedStrategyShard,
        _store: object,
    ) -> LabShardExecutionResult:
        nonlocal executions
        executions += 1
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(LabShardTable(name="trades", frame=pd.DataFrame([{"value": 1}])),),
        )

    monkeypatch.setattr(NShapeCompareAdapter, "execute_shard", execute_fixture)

    validated = registry.validate_claim(claim)
    result = registry.execute_shard(validated, object())

    assert validated.spec == legacy
    assert validated.claim.spec_hash == legacy.spec_hash
    assert result.spec_hash == legacy.spec_hash
    assert executions == 1


def test_legacy_strategy_alias_rejects_unknown_contract_version() -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    spec = _legacy_strategy_spec(_nshape_compare_spec(), "NShapeCompare")
    unsupported = spec.feature_contract.model_copy(update={"contract_version": "p13b-adapter-v0"})

    with pytest.raises(ValueError, match="legacy execution contract version"):
        default_strategy_job_adapter_registry().plan(
            spec.model_copy(update={"feature_contract": unsupported})
        )


def test_adapter_execution_contract_mismatch_fails_closed() -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    spec = _nshape_compare_spec()
    bad_contract = spec.feature_contract.model_copy(update={"contract_hash": "f" * 64})

    with pytest.raises(ValueError, match="execution contract"):
        default_strategy_job_adapter_registry().plan(
            spec.model_copy(update={"feature_contract": bad_contract})
        )


def test_hold_day_plan_is_unique_sorted_and_input_order_independent() -> None:
    from rquant.strategy_job_adapters import (
        HoldDaysShardInput,
        StrategyShardPayload,
        default_strategy_job_adapter_registry,
    )

    registry = default_strategy_job_adapter_registry()
    first = registry.plan(_nshape_compare_spec(hold_days=(5, 1, 3)))
    second = registry.plan(_nshape_compare_spec(hold_days=(3, 5, 1)))
    first_payloads = tuple(
        StrategyShardPayload.model_validate_json(item.payload_json) for item in first
    )

    assert first == second
    assert [payload.shard.hold_days for payload in first_payloads] == [1, 3, 5]
    assert all(isinstance(payload.shard, HoldDaysShardInput) for payload in first_payloads)


@pytest.mark.parametrize(
    "frames",
    [
        (
            pd.DataFrame({"left": pd.Series([1], dtype="int64")}),
            pd.DataFrame({"right": pd.Series([2], dtype="int64")}),
        ),
        (
            pd.DataFrame(
                {
                    "left": pd.Series([1], dtype="int64"),
                    "right": pd.Series([2], dtype="int64"),
                }
            ),
            pd.DataFrame(
                {
                    "right": pd.Series([2], dtype="int64"),
                    "left": pd.Series([1], dtype="int64"),
                }
            ),
        ),
        (
            pd.DataFrame({"value": pd.Series([1], dtype="int64")}),
            pd.DataFrame({"value": pd.Series([1.0], dtype="float64")}),
        ),
        (
            pd.DataFrame({"value": pd.Series([], dtype="int64")}),
            pd.DataFrame({"value": pd.Series([1.0], dtype="float64")}),
        ),
    ],
)
def test_aggregate_rejects_column_order_and_dtype_conflicts(
    frames: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    from rquant.strategy_job_adapters import _concat_shard_frames

    with pytest.raises(ValueError, match="schema"):
        _concat_shard_frames(frames)

    with pytest.raises(ValidationError, match="unique"):
        _nshape_compare_spec(hold_days=(1, 1))


@pytest.mark.parametrize(
    "frames",
    [
        (
            pd.DataFrame(
                {
                    "value": pd.Series(
                        ["a"],
                        dtype=pd.CategoricalDtype(["a", "b"], ordered=False),
                    )
                }
            ),
            pd.DataFrame(
                {
                    "value": pd.Series(
                        ["a"],
                        dtype=pd.CategoricalDtype(["a", "c"], ordered=False),
                    )
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "value": pd.Series(
                        ["a"],
                        dtype=pd.CategoricalDtype(["a", "b"], ordered=False),
                    )
                }
            ),
            pd.DataFrame(
                {
                    "value": pd.Series(
                        ["a"],
                        dtype=pd.CategoricalDtype(["a", "b"], ordered=True),
                    )
                }
            ),
        ),
        (
            pd.DataFrame({"value": pd.Series(pd.to_datetime(["2026-01-01"], utc=True))}),
            pd.DataFrame(
                {
                    "value": pd.Series(
                        pd.to_datetime(["2026-01-01"], utc=True).tz_convert("Asia/Shanghai")
                    )
                }
            ),
        ),
        (
            pd.DataFrame({"value": pd.Series([1], dtype="Int64")}),
            pd.DataFrame({"value": pd.Series([1], dtype="UInt64")}),
        ),
    ],
    ids=("categorical-values", "categorical-ordered", "timezone", "nullable"),
)
def test_aggregate_rejects_full_dtype_identity_conflicts(
    frames: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    from rquant.strategy_job_adapters import _concat_shard_frames

    with pytest.raises(ValueError, match="schema"):
        _concat_shard_frames(frames)


def test_aggregate_rejects_concat_output_dtype_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_job_adapters as adapters

    original_concat = pd.concat

    def drift_dtype(*args: object, **kwargs: object) -> pd.DataFrame:
        result = original_concat(*args, **kwargs)
        result["value"] = result["value"].astype("float64")
        return result

    monkeypatch.setattr(adapters.pd, "concat", drift_dtype)
    frames = (
        pd.DataFrame({"value": pd.Series([1], dtype="int64")}),
        pd.DataFrame({"value": pd.Series([2], dtype="int64")}),
    )

    with pytest.raises(ValueError, match="output schema"):
        adapters._concat_shard_frames(frames)


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        (
            _spec(
                "n_shape",
                _parameter("hold_days", "integer_list", (1,)),
                _parameter("entry_modes", "text_list", ("first_break",)),
                _parameter("mystery", "text", "x"),
            ),
            "mystery",
        ),
        (
            _spec(
                "n_shape",
                _parameter("entry_modes", "text_list", ("first_break",)),
                _parameter("profile_variants", "text_list", ("baseline",)),
                job_type=ResearchJobType.PARAMETER_SEARCH,
            ),
            "hold_days",
        ),
        (
            _spec("auction_gap", _parameter("max_hold_days", "text", "1")),
            "max_hold_days",
        ),
        (
            _spec(
                "growth_board_surge",
                _parameter("variants", "text_list", ("unknown",)),
                _parameter("max_hold_days", "integer", 1),
            ),
            "variant",
        ),
    ],
)
def test_adapter_parameters_fail_closed(spec: ResearchRunSpec, message: str) -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    with pytest.raises(ValueError, match=message):
        default_strategy_job_adapter_registry().plan(spec)


def test_date_buckets_are_inclusive_fixed_and_reproducible() -> None:
    from rquant.strategy_job_adapters import (
        DateBucketShardInput,
        GrowthDateVariantShardInput,
        StrategyShardPayload,
        default_strategy_job_adapter_registry,
    )

    registry = default_strategy_job_adapter_registry()
    auction = [
        StrategyShardPayload.model_validate_json(item.payload_json).shard
        for item in registry.plan(_auction_spec())
    ]
    growth = [
        StrategyShardPayload.model_validate_json(item.payload_json).shard
        for item in registry.plan(_growth_spec())
    ]

    assert auction == [
        DateBucketShardInput(start_date=date(2026, 1, 1), end_date=date(2026, 1, 20)),
        DateBucketShardInput(start_date=date(2026, 1, 21), end_date=date(2026, 2, 9)),
        DateBucketShardInput(start_date=date(2026, 2, 10), end_date=date(2026, 2, 10)),
    ]
    assert growth == [
        GrowthDateVariantShardInput(
            start_date=bucket_start,
            end_date=bucket_end,
            variant=variant,
        )
        for bucket_start, bucket_end in (
            (date(2026, 1, 1), date(2026, 1, 20)),
            (date(2026, 1, 21), date(2026, 2, 9)),
            (date(2026, 2, 10), date(2026, 2, 10)),
        )
        for variant in ("full", "no_vwap")
    ]


def test_claim_validation_rebuilds_the_full_identity() -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    registry = default_strategy_job_adapter_registry()
    spec = _nshape_compare_spec()
    claim = _claim(spec, shard_index=1)

    validated = registry.validate_claim(claim)

    assert validated.spec == spec
    assert validated.claim == claim
    assert validated.shard.hold_days == 3

    with pytest.raises(ValueError, match="spec_hash"):
        registry.validate_claim(claim.model_copy(update={"spec_hash": "f" * 64}))
    with pytest.raises(ValueError, match="definition"):
        registry.validate_claim(
            claim.model_copy(
                update={"definition": claim.definition.model_copy(update={"plan_hash": "f" * 64})}
            )
        )


def test_frozen_p13_claim_preserves_plan_shard_and_envelope_identity() -> None:
    claim = _p13_frozen_claim()

    assert claim.definition.work_plan is None
    assert claim.plan_hash == _P13_PLAN_HASH
    assert claim.shard_id == _P13_SHARD_ID
    assert LabClaimHighWater(claim=claim).content_hash == _P13_ENVELOPE_HASH
    assert LabClaimDeliveryReceipt(claim=claim).content_hash == _P13_ENVELOPE_HASH


def test_claim_validation_accepts_only_the_exact_regenerated_p13_definition() -> None:
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    registry = default_strategy_job_adapter_registry()
    claim = _p13_frozen_claim()

    validated = registry.validate_claim(claim)

    assert validated.claim == claim
    assert validated.shard.hold_days == 1

    forged_definitions = (
        LabShardDefinition.from_payload(
            shard_index=claim.shard_index,
            adapter_id=claim.definition.adapter_id,
            adapter_version=claim.definition.adapter_version,
            plan_hash="f" * 64,
            payload_json=claim.definition.payload_json,
        ),
        LabShardDefinition.from_payload(
            shard_index=1,
            adapter_id=claim.definition.adapter_id,
            adapter_version=claim.definition.adapter_version,
            plan_hash=claim.plan_hash,
            payload_json=claim.definition.payload_json,
        ),
        LabShardDefinition.from_payload(
            shard_index=claim.shard_index,
            adapter_id=claim.definition.adapter_id,
            adapter_version=claim.definition.adapter_version,
            plan_hash=claim.plan_hash,
            payload_json=claim.definition.payload_json.replace(
                '"hold_days":1',
                '"hold_days":2',
                1,
            ),
        ),
    )
    for forged in forged_definitions:
        with pytest.raises(ValueError):
            registry.validate_claim(claim.model_copy(update={"definition": forged}))


def test_current_claim_cannot_downgrade_to_missing_or_changed_work_plan() -> None:
    from rquant.lab_shard_protocol import LabShardWorkPlan
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    registry = default_strategy_job_adapter_registry()
    current = _claim(_nshape_compare_spec(hold_days=(1,)))
    plan = current.definition.work_plan
    assert plan is not None
    missing = LabShardDefinition.from_payload(
        shard_index=current.shard_index,
        adapter_id=current.definition.adapter_id,
        adapter_version=current.definition.adapter_version,
        plan_hash=current.plan_hash,
        payload_json=current.definition.payload_json,
    )
    changed = LabShardDefinition.from_payload(
        shard_index=current.shard_index,
        adapter_id=current.definition.adapter_id,
        adapter_version=current.definition.adapter_version,
        plan_hash=current.plan_hash,
        payload_json=current.definition.payload_json,
        work_plan=LabShardWorkPlan(
            phase=plan.phase,
            work_unit_name=plan.work_unit_name,
            work_units=plan.work_units + 1,
            static_duration_ms=plan.static_duration_ms,
        ),
    )

    for forged in (missing, changed):
        with pytest.raises(ValueError, match="definition"):
            registry.validate_claim(current.model_copy(update={"definition": forged}))


def _result_table(result: object, name: str) -> pd.DataFrame:
    return next(table.frame for table in result.tables if table.name == name)


def test_nshape_optimize_executes_only_the_claimed_hold_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_optimizer as optimizer
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    captured: list[list[int]] = []

    def fake_optimize(store: object, **kwargs: object) -> optimizer.StrategyOptimizationResult:
        del store
        captured.append(kwargs["max_hold_days_options"])
        return optimizer.StrategyOptimizationResult(
            rankings=pd.DataFrame(),
            trades=pd.DataFrame(),
        )

    monkeypatch.setattr(optimizer, "run_strategy_optimization", fake_optimize)
    registry = default_strategy_job_adapter_registry()
    validated = registry.validate_claim(_claim(_nshape_optimize_spec(), shard_index=1))

    registry.execute_shard(validated, object())

    assert captured == [[3]]


def test_nshape_compare_adapter_assigns_slippage_only_to_execution_cost_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_compare as compare
    from rquant.paper import PaperTradeConfig
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    captured: dict[str, object] = {}

    def fake_compare(
        store: object,
        **kwargs: object,
    ) -> compare.StrategyComparisonResult:
        del store
        captured.update(kwargs)
        return compare.StrategyComparisonResult(
            candidates_count=0,
            trades=pd.DataFrame(),
            summary=pd.DataFrame(),
        )

    monkeypatch.setattr(compare, "run_entry_mode_comparison", fake_compare)
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"execution_costs": _notional_costs()}
    )
    registry = default_strategy_job_adapter_registry()

    registry.execute_shard(registry.validate_claim(_claim(spec)), object())

    paper_config = captured["paper_config"]
    assert isinstance(paper_config, PaperTradeConfig)
    assert paper_config.entry_slippage_pct == 0
    assert captured["execution_costs"] == spec.execution_costs


def test_nshape_compare_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_compare import run_entry_mode_comparison
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes

    spec = _spec(
        "n_shape",
        _parameter("hold_days", "integer_list", (1,)),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    )
    with DuckDBStore(tmp_path / "compare.duckdb") as store:
        _seed_daily_and_screen(store)
        _seed_minutes(store)
        expected = run_entry_mode_comparison(
            store,
            start_date=date(2026, 6, 24),
            end_date=date(2026, 6, 24),
            entry_modes=["first_break"],
            profile_variants=["baseline"],
            max_hold_days=1,
        )
        registry = default_strategy_job_adapter_registry()
        actual = registry.execute_shard(registry.validate_claim(_claim(spec)), store)
        costly_spec = spec.model_copy(update={"execution_costs": _nonzero_costs()})
        costly = registry.execute_shard(
            registry.validate_claim(_claim(costly_spec)),
            store,
        )

    pd.testing.assert_frame_equal(_result_table(actual, "summary"), expected.summary)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected.trades)
    costly_trades = _result_table(costly, "trades")
    costly_summary = _result_table(costly, "summary")
    assert not costly_trades["ret_pct"].equals(expected.trades["ret_pct"])
    assert costly_trades["gross_ret_pct"].tolist() == expected.trades["ret_pct"].tolist()
    assert costly_summary.iloc[0]["mean_ret_pct"] == round(
        float(costly_trades["ret_pct"].mean()), 4
    )


def test_nshape_compare_nonzero_execution_costs_match_legacy_direct_and_reduce_returns(
    tmp_path: Path,
) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_compare import run_entry_mode_comparison
    from rquant.strategy_job_adapters import (
        LabShardExecutionWireResult,
        StrategyShardPayload,
        default_strategy_job_adapter_registry,
    )
    from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes

    spec = _spec(
        "n_shape",
        _parameter("hold_days", "integer_list", (1,)),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    ).model_copy(update={"execution_costs": _notional_costs()})
    with DuckDBStore(tmp_path / "compare-costs.duckdb") as store:
        _seed_daily_and_screen(store)
        _seed_minutes(store)
        expected = run_entry_mode_comparison(
            store,
            start_date=spec.parameters.start_date,
            end_date=spec.parameters.end_date,
            entry_modes=["first_break"],
            profile_variants=["baseline"],
            max_hold_days=1,
            execution_costs=spec.execution_costs,
        )
        registry = default_strategy_job_adapter_registry()
        definition = registry.plan(spec)[0]
        actual = registry.execute_shard(
            registry.validate_claim(_claim(spec)),
            store,
        )
        restored = LabShardExecutionWireResult.from_result(actual).to_result()

    actual_summary = _result_table(actual, "summary")
    actual_trades = _result_table(actual, "trades")
    payload = StrategyShardPayload.model_validate_json(definition.payload_json)
    pd.testing.assert_frame_equal(actual_summary, expected.summary)
    pd.testing.assert_frame_equal(actual_trades, expected.trades)
    pd.testing.assert_frame_equal(_result_table(restored, "trades"), actual_trades)
    assert payload.spec.execution_costs == spec.execution_costs
    assert actual_trades.iloc[0]["execution_cost_schema_version"] == 2
    assert actual_trades.iloc[0]["minimum_commission"] == 5.0
    assert actual_trades.iloc[0]["research_notional_per_trade"] == 100000.0
    assert actual_trades["ret_pct"].lt(actual_trades["gross_ret_pct"]).all()
    assert actual_summary.iloc[0]["mean_ret_pct"] < actual_trades["gross_ret_pct"].mean()


def test_nshape_optimize_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_optimizer import run_strategy_optimization
    from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes

    spec = _spec(
        "n_shape",
        _parameter("hold_days", "integer_list", (1,)),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        _parameter("top_n_options", "integer_list", (1,)),
        _parameter("score_profile_names", "text_list", ("v1",)),
        _parameter("validation_ratio", "decimal", Decimal("0")),
        _parameter("min_trades", "integer", 1),
        job_type=ResearchJobType.PARAMETER_SEARCH,
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    )
    with DuckDBStore(tmp_path / "optimize.duckdb") as store:
        _seed_daily_and_screen(store)
        _seed_minutes(store)
        expected = run_strategy_optimization(
            store,
            start_date=date(2026, 6, 24),
            end_date=date(2026, 6, 24),
            entry_modes=["first_break"],
            profile_variants=["baseline"],
            max_hold_days_options=[1],
            validation_ratio=0.0,
            min_trades=1,
            top_n_options=[1],
            score_profile_names=["v1"],
        )
        registry = default_strategy_job_adapter_registry()
        actual = registry.execute_shard(registry.validate_claim(_claim(spec)), store)
        costly_spec = spec.model_copy(update={"execution_costs": _notional_costs()})
        costly_expected = run_strategy_optimization(
            store,
            start_date=date(2026, 6, 24),
            end_date=date(2026, 6, 24),
            entry_modes=["first_break"],
            profile_variants=["baseline"],
            max_hold_days_options=[1],
            validation_ratio=0.0,
            min_trades=1,
            top_n_options=[1],
            score_profile_names=["v1"],
            execution_costs=costly_spec.execution_costs,
        )
        costly = registry.execute_shard(
            registry.validate_claim(_claim(costly_spec)),
            store,
        )

    pd.testing.assert_frame_equal(_result_table(actual, "rankings"), expected.rankings)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected.trades)
    pd.testing.assert_frame_equal(_result_table(actual, "topn_rankings"), expected.topn_rankings)
    pd.testing.assert_frame_equal(_result_table(costly, "rankings"), costly_expected.rankings)
    pd.testing.assert_frame_equal(_result_table(costly, "trades"), costly_expected.trades)
    pd.testing.assert_frame_equal(
        _result_table(costly, "topn_rankings"),
        costly_expected.topn_rankings,
    )
    costly_trades = _result_table(costly, "trades")
    costly_rankings = _result_table(costly, "rankings")
    assert "gross_ret_pct" in costly_trades.columns
    assert costly_rankings.iloc[0]["train_mean_ret_pct"] == round(
        float(costly_trades.loc[costly_trades["split"] == "train", "ret_pct"].mean()),
        4,
    )
    assert costly_rankings.iloc[0]["robust_score"] != expected.rankings.iloc[0]["robust_score"]


def test_auction_gap_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_replay_metrics import auction_gap_metric_rows
    from tests.unit.test_auction_gap_minute_replay import _seed_base

    spec = _spec(
        "auction_gap",
        _parameter("max_hold_days", "integer", 1),
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
    )
    with DuckDBStore(tmp_path / "auction.duckdb") as store:
        _seed_base(store)
        config = AuctionGapMinuteReplayConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            max_hold_days=1,
        )
        candidates = run_auction_gap_replay(store, config.auction_config())
        expected = run_auction_gap_minute_replay(
            store,
            config,
            candidates=candidates,
        )
        registry = default_strategy_job_adapter_registry()
        actual = registry.execute_shard(registry.validate_claim(_claim(spec)), store)
        costly_spec = spec.model_copy(update={"execution_costs": _nonzero_costs()})
        costly = registry.execute_shard(
            registry.validate_claim(_claim(costly_spec)),
            store,
        )

    pd.testing.assert_frame_equal(_result_table(actual, "candidates"), candidates)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected)
    pd.testing.assert_frame_equal(
        _result_table(actual, "summary"),
        auction_gap_metric_rows(candidates, expected),
    )
    costly_trades = _result_table(costly, "trades")
    pd.testing.assert_frame_equal(
        _result_table(costly, "summary"),
        auction_gap_metric_rows(candidates, costly_trades),
    )
    assert costly_trades["gross_ret_pct"].tolist() == expected["ret_pct"].tolist()
    assert not costly_trades["ret_pct"].equals(expected["ret_pct"])


def test_growth_board_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_replay_metrics import growth_board_metric_rows
    from tests.unit.test_growth_board_surge_strategy import (
        _seed_base_market,
        _seed_volume_surge_minutes,
    )

    spec = _spec(
        "growth_board_surge",
        _parameter("variants", "text_list", ("full",)),
        _parameter("max_hold_days", "integer", 1),
        _parameter("lookback_days", "integer", 2),
        _parameter("min_hist_days", "integer", 2),
        _parameter("min_cum_amount_ratio", "decimal", Decimal("1.4")),
        _parameter("min_same_minute_amount_ratio", "decimal", Decimal("2")),
        _parameter("min_amount_accel_5m", "decimal", Decimal("2")),
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
    )
    with DuckDBStore(tmp_path / "growth.duckdb") as store:
        _seed_base_market(store)
        _seed_volume_surge_minutes(store)
        expected = run_growth_board_surge_replay(
            store,
            start_date=date(2026, 6, 25),
            end_date=date(2026, 6, 25),
            config=GrowthBoardSurgeConfig(
                lookback_days=2,
                min_hist_days=2,
                min_cum_amount_ratio=1.4,
                min_same_minute_amount_ratio=2.0,
                min_amount_accel_5m=2.0,
                max_hold_days=1,
            ),
        )
        registry = default_strategy_job_adapter_registry()
        actual = registry.execute_shard(registry.validate_claim(_claim(spec)), store)
        costly_spec = spec.model_copy(update={"execution_costs": _nonzero_costs()})
        costly = registry.execute_shard(
            registry.validate_claim(_claim(costly_spec)),
            store,
        )

    adapter_trades = _result_table(actual, "trades").drop(columns="variant")
    pd.testing.assert_frame_equal(adapter_trades, expected)
    expected_summary = growth_board_metric_rows(
        _result_table(actual, "trades"),
        strategy_name="full",
    )
    expected_summary.insert(0, "variant", "full")
    pd.testing.assert_frame_equal(_result_table(actual, "summary"), expected_summary)
    costly_trades = _result_table(costly, "trades")
    costly_summary = growth_board_metric_rows(costly_trades, strategy_name="full")
    costly_summary.insert(0, "variant", "full")
    pd.testing.assert_frame_equal(_result_table(costly, "summary"), costly_summary)
    assert costly_trades["gross_ret_pct"].tolist() == expected["ret_pct"].tolist()
    assert not costly_trades["ret_pct"].equals(expected["ret_pct"])


def test_auction_and_growth_adapters_emit_deterministic_empty_summaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.auction_gap_strategy as auction_strategy
    import rquant.growth_board_surge_strategy as growth_strategy
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_replay_metrics import (
        auction_gap_metric_rows,
        growth_board_metric_rows,
    )

    def empty_frame(*_args: object, **_kwargs: object) -> pd.DataFrame:
        return pd.DataFrame()

    monkeypatch.setattr(auction_strategy, "run_auction_gap_replay", empty_frame)
    monkeypatch.setattr(auction_strategy, "run_auction_gap_minute_replay", empty_frame)
    monkeypatch.setattr(growth_strategy, "run_growth_board_surge_replay", empty_frame)
    registry = default_strategy_job_adapter_registry()

    auction_result = registry.execute_shard(
        registry.validate_claim(_claim(_auction_spec())),
        object(),
    )
    growth_result = registry.execute_shard(
        registry.validate_claim(_claim(_growth_spec(variants=("no_vwap",)))),
        object(),
    )
    auction_summary = _result_table(auction_result, "summary")
    growth_summary = _result_table(growth_result, "summary")
    expected_growth = growth_board_metric_rows(
        pd.DataFrame(),
        strategy_name="no_vwap",
    )
    expected_growth.insert(0, "variant", "no_vwap")

    pd.testing.assert_frame_equal(
        auction_summary,
        auction_gap_metric_rows(pd.DataFrame(), pd.DataFrame()),
    )
    pd.testing.assert_frame_equal(growth_summary, expected_growth)
    assert growth_summary.iloc[0]["variant"] == "no_vwap"

    auction_spec = _auction_spec()
    auction_results = tuple(
        registry.execute_shard(registry.validate_claim(_claim(auction_spec, index)), object())
        for index in range(len(registry.plan(auction_spec)))
    )
    aggregated_auction = registry.aggregate_results(auction_spec, auction_results)
    pd.testing.assert_frame_equal(
        _result_table(aggregated_auction, "summary"),
        auction_gap_metric_rows(pd.DataFrame(), pd.DataFrame()),
    )

    growth_spec = _growth_spec(variants=("cum_only", "no_vwap"))
    growth_variants = next(
        parameter.value
        for parameter in growth_spec.parameters.arguments
        if parameter.name == "variants"
    )
    growth_results = tuple(
        registry.execute_shard(registry.validate_claim(_claim(growth_spec, index)), object())
        for index in range(len(registry.plan(growth_spec)))
    )
    aggregated_growth = registry.aggregate_results(growth_spec, growth_results)
    expected_empty_growth: list[pd.DataFrame] = []
    for variant in growth_variants:
        summary = growth_board_metric_rows(pd.DataFrame(), strategy_name=variant)
        summary.insert(0, "variant", variant)
        expected_empty_growth.append(summary)
    pd.testing.assert_frame_equal(
        _result_table(aggregated_growth, "summary"),
        pd.concat(expected_empty_growth, ignore_index=True),
    )
    reversed_growth = registry.aggregate_results(growth_spec, tuple(reversed(growth_results)))
    assert aggregated_growth.result_hash == reversed_growth.result_hash


def test_nshape_optimize_multi_hold_aggregate_matches_legacy_global_result(
    tmp_path: Path,
) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_optimizer import run_strategy_optimization
    from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes

    spec = _spec(
        "n_shape",
        _parameter("hold_days", "integer_list", (1, 3)),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        _parameter("top_n_options", "integer_list", (1,)),
        _parameter("score_profile_names", "text_list", ("v1",)),
        _parameter("validation_ratio", "decimal", Decimal("0")),
        _parameter("min_trades", "integer", 1),
        job_type=ResearchJobType.PARAMETER_SEARCH,
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    ).model_copy(update={"execution_costs": _notional_costs()})
    registry = default_strategy_job_adapter_registry()
    with DuckDBStore(tmp_path / "aggregate-optimize.duckdb") as store:
        _seed_daily_and_screen(store)
        _seed_minutes(store)
        expected = run_strategy_optimization(
            store,
            start_date=spec.parameters.start_date,
            end_date=spec.parameters.end_date,
            entry_modes=["first_break"],
            profile_variants=["baseline"],
            max_hold_days_options=[1, 3],
            validation_ratio=0.0,
            min_trades=1,
            top_n_options=[1],
            score_profile_names=["v1"],
            execution_costs=spec.execution_costs,
        )
        results = tuple(
            registry.execute_shard(registry.validate_claim(_claim(spec, index)), store)
            for index in range(2)
        )
        actual = registry.aggregate_results(spec, results)

    pd.testing.assert_frame_equal(_result_table(actual, "rankings"), expected.rankings)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected.trades)
    pd.testing.assert_frame_equal(_result_table(actual, "topn_rankings"), expected.topn_rankings)


def test_auction_cross_bucket_aggregate_matches_legacy_fixture(tmp_path: Path) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_replay_metrics import auction_gap_metric_rows
    from tests.unit.test_auction_gap_minute_replay import _seed_base

    spec = _spec(
        "auction_gap",
        _parameter("max_hold_days", "integer", 1),
        start_date=date(2026, 6, 5),
        end_date=date(2026, 6, 25),
    )
    registry = default_strategy_job_adapter_registry()
    with DuckDBStore(tmp_path / "aggregate-auction.duckdb") as store:
        _seed_base(store)
        config = AuctionGapMinuteReplayConfig(
            start_date=spec.parameters.start_date.isoformat(),
            end_date=spec.parameters.end_date.isoformat(),
            max_hold_days=1,
        )
        candidates = run_auction_gap_replay(store, config.auction_config())
        expected = run_auction_gap_minute_replay(store, config, candidates=candidates)
        definitions = registry.plan(spec)
        results = tuple(
            registry.execute_shard(registry.validate_claim(_claim(spec, index)), store)
            for index in range(len(definitions))
        )
        actual = registry.aggregate_results(spec, tuple(reversed(results)))
        ordered = registry.aggregate_results(spec, results)

    pd.testing.assert_frame_equal(_result_table(actual, "candidates"), candidates)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected)
    pd.testing.assert_frame_equal(
        _result_table(actual, "summary"),
        auction_gap_metric_rows(candidates, expected),
    )
    assert actual.result_hash == ordered.result_hash
    with pytest.raises(ValueError, match="complete shard plan"):
        registry.aggregate_results(spec, results[:-1])
    with pytest.raises(ValueError, match="unique"):
        registry.aggregate_results(spec, (results[0], results[0]))


def test_growth_cross_bucket_aggregate_matches_legacy_fixture(tmp_path: Path) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_replay_metrics import growth_board_metric_rows
    from tests.unit.test_growth_board_surge_strategy import (
        _seed_base_market,
        _seed_volume_surge_minutes,
    )

    spec = _spec(
        "growth_board_surge",
        _parameter("variants", "text_list", ("no_vwap", "full")),
        _parameter("max_hold_days", "integer", 1),
        _parameter("lookback_days", "integer", 2),
        _parameter("min_hist_days", "integer", 2),
        start_date=date(2026, 6, 5),
        end_date=date(2026, 6, 25),
    )
    typed_variants = next(
        parameter.value for parameter in spec.parameters.arguments if parameter.name == "variants"
    )
    registry = default_strategy_job_adapter_registry()
    with DuckDBStore(tmp_path / "aggregate-growth.duckdb") as store:
        _seed_base_market(store)
        _seed_volume_surge_minutes(store)
        expected = run_growth_board_surge_replay(
            store,
            start_date=spec.parameters.start_date,
            end_date=spec.parameters.end_date,
            config=GrowthBoardSurgeConfig(
                lookback_days=2,
                min_hist_days=2,
                max_hold_days=1,
            ),
        )
        definitions = registry.plan(spec)
        results = tuple(
            registry.execute_shard(registry.validate_claim(_claim(spec, index)), store)
            for index in range(len(definitions))
        )
        actual = registry.aggregate_results(spec, tuple(reversed(results)))

    aggregated_trades = _result_table(actual, "trades")
    full_trades = aggregated_trades.loc[aggregated_trades["variant"] == "full"].drop(
        columns="variant"
    )
    pd.testing.assert_frame_equal(full_trades.reset_index(drop=True), expected)
    expected_summaries: list[pd.DataFrame] = []
    for variant in typed_variants:
        variant_trades = aggregated_trades.loc[aggregated_trades["variant"] == variant]
        summary = growth_board_metric_rows(variant_trades, strategy_name=variant)
        summary.insert(0, "variant", variant)
        expected_summaries.append(summary)
    expected_summary = pd.concat(expected_summaries, ignore_index=True)
    pd.testing.assert_frame_equal(_result_table(actual, "summary"), expected_summary)
    assert _result_table(actual, "summary")["variant"].tolist() == list(typed_variants)


def test_scheduler_registry_plans_unplanned_submissions_after_restart(tmp_path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    spec = _nshape_compare_spec()
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2),
    )
    spool.publish(command)
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: datetime(2026, 7, 24, 1, tzinfo=UTC),
    )

    result = scheduler.run_once()
    scheduler.release()
    restarted = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: datetime(2026, 7, 24, 1, 1, tzinfo=UTC),
    )
    restarted.run_once()
    shards = LabJobReader(store.path).list_shards(command.command.job_id)

    assert result.plans_created == 1
    assert [shard.shard_index for shard in shards] == [0, 1, 2]
    assert len({shard.plan_hash for shard in shards}) == 1


def test_scheduler_plans_legacy_queued_spec_without_rewriting_hash(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    spec = _legacy_strategy_spec(
        _nshape_compare_spec(hold_days=(1, 2)),
        "NShapeCompare",
    )
    job_id = uuid4()
    spool.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(job_id=job_id, spec=spec, max_attempts=2),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: datetime(2026, 7, 24, 1, tzinfo=UTC),
    )

    result = scheduler.run_once()
    reader = LabJobReader(store.path)
    persisted = reader.get_job(job_id)

    assert result.plans_created == 1
    assert result.plans_failed == 0
    assert persisted is not None and persisted.status is JobStatus.QUEUED
    assert persisted.spec == spec
    assert persisted.spec_hash == spec.spec_hash
    assert len(reader.list_shards(job_id)) == 2


def test_scheduler_persists_first_adapter_plan_failure(tmp_path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    spec = _nshape_compare_spec()
    bad = spec.model_copy(
        update={
            "feature_contract": spec.feature_contract.model_copy(update={"contract_hash": "f" * 64})
        }
    )
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(job_id=uuid4(), spec=bad, max_attempts=2),
    )
    spool.publish(command)
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: datetime(2026, 7, 24, 1, tzinfo=UTC),
    )

    first = scheduler.run_once()
    second = scheduler.run_once()
    reader = LabJobReader(store.path)
    job = reader.get_job(command.command.job_id)

    assert first.plans_failed == 1
    assert second.plans_failed == 0
    assert job is not None and job.status is JobStatus.FAILED
    assert reader.list_shards(command.command.job_id) == ()
    assert "execution contract" in reader.list_events(command.command.job_id)[-1].reason


def test_scheduler_persists_runtime_plan_failure_and_continues_next_job(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    class RuntimeFailingRegistry:
        def __init__(self) -> None:
            self.delegate = default_strategy_job_adapter_registry()

        def plan(self, spec: ResearchRunSpec):
            if spec.random_seed == 1:
                raise RuntimeError("fixture plan exploded\nunsafe detail")
            return self.delegate.plan(spec)

    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    bad_job_id = UUID(int=1)
    good_job_id = UUID(int=2)
    for job_id, seed in ((bad_job_id, 1), (good_job_id, 2)):
        spool.publish(
            LabCommandEnvelope(
                request_id=uuid4(),
                command=SubmitJobCommand(
                    job_id=job_id,
                    spec=_nshape_compare_spec(hold_days=(1,)).model_copy(
                        update={"random_seed": seed}
                    ),
                    max_attempts=2,
                ),
            )
        )
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=RuntimeFailingRegistry(),
        clock=lambda: datetime(2026, 7, 24, 1, tzinfo=UTC),
    )

    result = scheduler.run_once()
    reader = LabJobReader(store.path)
    bad = reader.get_job(bad_job_id)
    good = reader.get_job(good_job_id)
    reason = reader.list_events(bad_job_id)[-1].reason

    assert result.plans_failed == 1
    assert result.plans_created == 1
    assert bad is not None and bad.status is JobStatus.FAILED
    assert good is not None and good.status is JobStatus.QUEUED
    assert reader.list_shards(good_job_id)
    assert "RuntimeError: fixture plan exploded unsafe detail" in reason
    assert "\n" not in reason


def test_scheduler_deadline_terminalizes_running_job_and_shards(tmp_path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, ShardStatus
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabClaimSpool
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    clock = [datetime(2026, 7, 24, 1, tzinfo=UTC)]
    deadline = clock[0] + timedelta(seconds=30)
    spec = _nshape_compare_spec().model_copy(update={"deadline": deadline})
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    claims = LabClaimSpool(tmp_path / "claims")
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2),
    )
    spool.publish(command)
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=default_strategy_job_adapter_registry(),
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=30,
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    clock[0] = deadline

    result = scheduler.run_once()
    reader = LabJobReader(store.path)
    job = reader.get_job(command.command.job_id)
    shards = reader.list_shards(command.command.job_id)

    assert result.deadlines_expired == 1
    assert job is not None and job.status is JobStatus.FAILED
    assert {shard.status for shard in shards} == {ShardStatus.FAILED}
    assert all(shard.failure_json == '{"reason":"deadline_exceeded"}' for shard in shards)
    assert result.claims_revoked == 1
    assert claims.pending() == ()


def test_scheduler_terminalizes_deadline_at_claim_boundary(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, ShardStatus
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabClaimSpool
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    now = datetime(2026, 7, 24, 1, tzinfo=UTC)
    deadline = now + timedelta(seconds=5)
    moments = iter((now, now, now, now, deadline))
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(update={"deadline": deadline})
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    claims = LabClaimSpool(tmp_path / "claims")
    job_id = uuid4()
    spool.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(job_id=job_id, spec=spec, max_attempts=2),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        adapter_registry=default_strategy_job_adapter_registry(),
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=30,
        clock=lambda: next(moments),
    )

    result = scheduler.run_once()
    reader = LabJobReader(store.path)
    job = reader.get_job(job_id)

    assert result.deadlines_expired == 1
    assert result.claims_published == 0
    assert claims.pending() == ()
    assert job is not None and job.status is JobStatus.FAILED
    assert {shard.status for shard in reader.list_shards(job_id)} == {ShardStatus.FAILED}


def test_shard_wire_result_round_trips_parquet_bytes_without_python_objects() -> None:
    import pandas as pd

    from rquant.strategy_job_adapters import (
        LabShardExecutionResult,
        LabShardExecutionWireResult,
        LabShardTable,
        default_strategy_job_adapter_registry,
    )

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = default_strategy_job_adapter_registry().validate_claim(claim)
    result = LabShardExecutionResult.from_validated(
        validated,
        tables=(
            LabShardTable(
                name="trades",
                frame=pd.DataFrame({"code": ["000001.SZ"], "ret_pct": [1.25], "hold_days": [1]}),
            ),
        ),
    )

    wire = LabShardExecutionWireResult.from_result(result)
    restored = wire.to_result()

    assert restored.model_dump(exclude={"tables"}) == result.model_dump(exclude={"tables"})
    pd.testing.assert_frame_equal(restored.tables[0].frame, result.tables[0].frame)


def test_shard_wire_result_rejects_tampered_parquet_hash() -> None:
    import pandas as pd

    from rquant.strategy_job_adapters import (
        LabShardExecutionResult,
        LabShardExecutionWireResult,
        LabShardTable,
        default_strategy_job_adapter_registry,
    )

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = default_strategy_job_adapter_registry().validate_claim(claim)
    result = LabShardExecutionResult.from_validated(
        validated,
        tables=(LabShardTable(name="trades", frame=pd.DataFrame({"value": [1]})),),
    )
    wire = LabShardExecutionWireResult.from_result(result)
    tampered = wire.model_copy(
        update={"tables": (wire.tables[0].model_copy(update={"sha256": "f" * 64}),)}
    )

    with pytest.raises(ValueError, match="hash mismatch"):
        tampered.to_result()


def test_shard_wire_capacity_boundaries_fit_the_80_mib_receive_limit() -> None:
    from rquant.strategy_job_adapters import (
        MAX_AGGREGATE_PARQUET_BYTES,
        MAX_PARQUET_BYTES_PER_TABLE,
        MAX_RESULT_JSON_OVERHEAD_BYTES,
        MAX_RESULT_WIRE_BYTES,
        MAX_TABLES,
        shard_wire_base64_size,
        validate_shard_wire_capacity,
    )

    assert MAX_RESULT_WIRE_BYTES == 80 * 1024 * 1024
    assert (
        shard_wire_base64_size(MAX_AGGREGATE_PARQUET_BYTES)
        + 4 * (MAX_TABLES - 1)
        + MAX_RESULT_JSON_OVERHEAD_BYTES
        <= MAX_RESULT_WIRE_BYTES
    )
    remainder = MAX_AGGREGATE_PARQUET_BYTES - MAX_PARQUET_BYTES_PER_TABLE
    validate_shard_wire_capacity((MAX_PARQUET_BYTES_PER_TABLE, remainder))
    validate_shard_wire_capacity((1,) * MAX_TABLES)

    with pytest.raises(ValueError, match="at most"):
        validate_shard_wire_capacity((1,) * (MAX_TABLES + 1))
    with pytest.raises(ValueError, match="per-table"):
        validate_shard_wire_capacity((MAX_PARQUET_BYTES_PER_TABLE + 1,))
    with pytest.raises(ValueError, match="aggregate"):
        validate_shard_wire_capacity(
            (
                MAX_PARQUET_BYTES_PER_TABLE,
                remainder + 1,
            )
        )


def test_shard_wire_rejects_too_many_tables_before_parquet_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.strategy_job_adapters import (
        MAX_TABLES,
        LabShardExecutionResult,
        LabShardExecutionWireResult,
        LabShardTable,
        default_strategy_job_adapter_registry,
    )

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = default_strategy_job_adapter_registry().validate_claim(claim)
    result = LabShardExecutionResult.from_validated(
        validated,
        tables=tuple(
            LabShardTable(name=f"table_{index}", frame=pd.DataFrame({"value": [index]}))
            for index in range(MAX_TABLES + 1)
        ),
    )
    serialized = False

    def unexpected_to_parquet(*_args: object, **_kwargs: object) -> None:
        nonlocal serialized
        serialized = True

    monkeypatch.setattr(pd.DataFrame, "to_parquet", unexpected_to_parquet)

    with pytest.raises(ValueError, match="at most"):
        LabShardExecutionWireResult.from_result(result)
    assert not serialized


def test_shard_wire_model_rejects_noncanonical_base64_length() -> None:
    from rquant.strategy_job_adapters import LabShardWireTable

    with pytest.raises(ValidationError, match="base64 length"):
        LabShardWireTable(
            name="trades",
            parquet_base64="YQ==",
            byte_size=4,
            sha256=hashlib.sha256(b"a").hexdigest(),
        )
