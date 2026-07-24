from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.lab_shard_protocol import LabShardClaim
from rquant.research_run_spec import (
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)


def _parameter(name: str, kind: str, value: object) -> ResearchParameter:
    return ResearchParameter(name=name, kind=kind, value=value)


def _spec(
    strategy_name: str,
    *arguments: ResearchParameter,
    start_date: date = date(2026, 1, 1),
    end_date: date = date(2026, 2, 10),
) -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=(
            ResearchJobType.PARAMETER_SEARCH
            if strategy_name == "NShapeOptimize"
            else ResearchJobType.STRATEGY_REPLAY
        ),
        parameters=ResearchRunParameters(
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            arguments=arguments,
        ),
        code_sha="1" * 40,
        dataset_snapshot=None,
        feature_contract=FeatureContractIdentity(
            contract_id="strategy-lab",
            contract_version="v1",
            contract_hash="2" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
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
        "NShapeCompare",
        _parameter("hold_days", "integer_list", hold_days),
        _parameter("entry_modes", "text_list", ("late_confirm", "first_break")),
        _parameter("profile_variants", "text_list", ("baseline",)),
    )


def _nshape_optimize_spec(*, hold_days: tuple[int, ...] = (1, 3, 5)) -> ResearchRunSpec:
    return _spec(
        "NShapeOptimize",
        _parameter("hold_days", "integer_list", hold_days),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        _parameter("top_n_options", "integer_list", (1,)),
        _parameter("score_profile_names", "text_list", ("v1",)),
    )


def _auction_spec() -> ResearchRunSpec:
    return _spec(
        "AuctionGap",
        _parameter("max_hold_days", "integer", 1),
    )


def _growth_spec(*, variants: tuple[str, ...] = ("no_vwap", "full")) -> ResearchRunSpec:
    return _spec(
        "GrowthBoardSurge",
        _parameter("variants", "text_list", variants),
        _parameter("max_hold_days", "integer", 1),
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

    with pytest.raises(ValidationError, match="unique"):
        _nshape_compare_spec(hold_days=(1, 1))


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        (
            _spec(
                "NShapeCompare",
                _parameter("hold_days", "integer_list", (1,)),
                _parameter("entry_modes", "text_list", ("first_break",)),
                _parameter("mystery", "text", "x"),
            ),
            "mystery",
        ),
        (
            _spec(
                "NShapeOptimize",
                _parameter("entry_modes", "text_list", ("first_break",)),
                _parameter("profile_variants", "text_list", ("baseline",)),
            ),
            "hold_days",
        ),
        (
            _spec("AuctionGap", _parameter("max_hold_days", "text", "1")),
            "max_hold_days",
        ),
        (
            _spec(
                "GrowthBoardSurge",
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


def test_nshape_compare_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_compare import run_entry_mode_comparison
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes

    spec = _spec(
        "NShapeCompare",
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

    pd.testing.assert_frame_equal(_result_table(actual, "summary"), expected.summary)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected.trades)


def test_nshape_optimize_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.strategy_optimizer import run_strategy_optimization
    from tests.unit.test_minute_replay import _seed_daily_and_screen, _seed_minutes

    spec = _spec(
        "NShapeOptimize",
        _parameter("hold_days", "integer_list", (1,)),
        _parameter("entry_modes", "text_list", ("first_break",)),
        _parameter("profile_variants", "text_list", ("baseline",)),
        _parameter("top_n_options", "integer_list", (1,)),
        _parameter("score_profile_names", "text_list", ("v1",)),
        _parameter("validation_ratio", "decimal", Decimal("0")),
        _parameter("min_trades", "integer", 1),
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

    pd.testing.assert_frame_equal(_result_table(actual, "rankings"), expected.rankings)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected.trades)
    pd.testing.assert_frame_equal(_result_table(actual, "topn_rankings"), expected.topn_rankings)


def test_auction_gap_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from tests.unit.test_auction_gap_minute_replay import _seed_base

    spec = _spec(
        "AuctionGap",
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

    pd.testing.assert_frame_equal(_result_table(actual, "candidates"), candidates)
    pd.testing.assert_frame_equal(_result_table(actual, "trades"), expected)


def test_growth_board_adapter_matches_legacy_fixture(tmp_path) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from tests.unit.test_growth_board_surge_strategy import (
        _seed_base_market,
        _seed_volume_surge_minutes,
    )

    spec = _spec(
        "GrowthBoardSurge",
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

    adapter_trades = _result_table(actual, "trades").drop(columns="variant")
    pd.testing.assert_frame_equal(adapter_trades, expected)


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
