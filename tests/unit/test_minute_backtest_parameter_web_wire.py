from datetime import UTC, date, datetime, time, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from rquant.minute_backtest_commands import MinuteParameterRunConfig, SubmitMinuteReplay
from rquant.minute_backtest_parameters import (
    MinuteAuctionGapParameters, MinuteGrowthParameters, MinuteNShapeParameters, MinuteParameterSet,
)
from rquant.web.models.minute_backtests import MinuteCreateRequest, MinuteJob, MinuteJobsData, MinuteParameterJob
from tests.unit.test_minute_backtest_commands import configuration


def test_parameter_wire_keeps_every_original_parameter_and_exact_float() -> None:
    old = configuration()
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(entry_mode="amount_surge", max_hold_days=20,
        paper={"stop_loss_pct": 0.012345, "take_profit_pct": 0.123456}))
    config = MinuteParameterRunConfig(source_key="verified.complete-facts", source_version=1,
        full_input_hash="a" * 64, parameters=parameters, protocol=old.protocol,
        deadline=datetime(2027, 1, 1, tzinfo=UTC), random_seed=52)
    requested = datetime(2026, 1, 1, tzinfo=UTC)
    command = SubmitMinuteReplay(command_id=str(uuid4()), requested_at=requested, actor_id="researcher", config=config)
    assert SubmitMinuteReplay.model_validate_json(command.model_dump_json()) == command
    request = MinuteCreateRequest(command_id=uuid4(), requested_at=requested, config=config)
    assert MinuteCreateRequest.model_validate_json(request.model_dump_json()) == request
    assert MinuteCreateRequest.model_validate(request.model_dump(mode="json")) == request
    assert request.config.parameters.model_dump_json() == parameters.model_dump_json()
    assert request.config.parameters.parameters.paper.stop_loss_pct == 0.012345
    original = MinuteCreateRequest(command_id=uuid4(), requested_at=requested, config=old)
    assert original.config == old and original.config.model_dump_json() == old.model_dump_json()


@pytest.mark.parametrize("family", ("auction_gap", "growth_board_surge"))
def test_remaining_family_public_wire_keeps_complete_advanced_nested_recipe(family: str) -> None:
    risk = {"candidate_id": "explicit-family-wire", "stop_loss_pct": 0.012345,
        "take_profit_pct": 0.123456, "trailing_stop_pct": 0.034567,
        "entry_buffer_pct": 0.004567, "entry_slippage_pct": 0.000234}
    if family == "auction_gap":
        owner = MinuteAuctionGapParameters(start_date="2026-07-31", end_date="2026-08-04",
            freq="15min", gap_mode="strict_high", st_filter="literal_lower",
            min_auction_vol_ratio_5d=0.3, max_auction_vol_ratio_5d=2.1,
            entry_start_time=time(9, 35), entry_pullback_tolerance_pct=0.03,
            entry_vwap_buffer_pct=0.02, min_limit_progress_pct=0.6, max_hold_days=10,
            next_auction_weak_gap_pct=-0.025, strong_seal_min_close_minutes=12,
            strong_seal_weak_gap_pct=-0.04, next_morning_exit_until=time(10, 15),
            next_morning_vwap_break_buffer_pct=0.009, seal_hold_enabled=True,
            seal_hold_max_days=8, seal_hold_max_open_times=2, seal_hold_min_fd_to_circ_pct=0.12,
            factor_score_threshold=65.0, price_tol=0.02, paper=risk)
    else:
        owner = MinuteGrowthParameters(freq="60min", min_signal_time=time(9, 45),
            lookback_days=90, min_hist_days=30, min_cum_amount_ratio=2.7,
            min_same_minute_amount_ratio=3.9, min_amount_accel_5m=4.2,
            require_vwap_strength=False, use_same_minute_surge=False, use_accel_surge=True,
            vwap_buffer_pct=0.01, require_inner_outer=True, max_inner_outer_ratio=0.7,
            require_large_net_vol=True, min_large_net_vol=500.0, require_fresh_surge=True,
            fresh_lookback_days=20, fresh_max_prior_volume_ratio=1.5, min_listing_trading_days=60,
            require_board_favor=True, min_board_gap_up_ratio=0.7, min_board_auction_amount_ratio=2.5,
            board_hist_days=7, enable_factor_confirm=True, factor_score_threshold=60.0,
            max_hold_days=10, price_tol=0.02, paper=risk)
    parameters = MinuteParameterSet(parameters=owner)
    old = configuration()
    config = MinuteParameterRunConfig(source_key="verified.complete-family-facts", source_version=1,
        full_input_hash="a" * 64, parameters=parameters, protocol=old.protocol,
        deadline=datetime(2027, 1, 1, tzinfo=UTC), random_seed=52)
    requested = datetime(2026, 10, 8, tzinfo=UTC)
    request = MinuteCreateRequest(command_id=uuid4(), requested_at=requested, config=config)
    restored = MinuteCreateRequest.model_validate_json(request.model_dump_json())
    assert restored == request
    assert MinuteCreateRequest.model_validate(request.model_dump(mode="json")) == request
    assert restored.config.parameters.model_dump_json() == parameters.model_dump_json()
    assert restored.config.parameters.parameters.owner_config().model_dump(mode="json") == owner.model_dump(
        mode="json", exclude={"family"})
    command = SubmitMinuteReplay(command_id=str(request.command_id), requested_at=requested,
        actor_id="researcher", config=restored.config)
    assert SubmitMinuteReplay.model_validate_json(command.model_dump_json()) == command
    job = MinuteParameterJob(job_id=uuid4(), status="completed", version=1,
        created_at=requested, updated_at=requested, spec_hash="b" * 64,
        source_key=config.source_key, source_version=config.source_version, full_input_hash=config.full_input_hash,
        native_id=parameters.definition_id, native_name=family, native_version=1,
        family=family, parameters=parameters, parameter_hash=parameters.fingerprint,
        start_date=date(2026, 7, 31), end_date=date(2026, 8, 4), result_hash="c" * 64)
    restored_job, = MinuteJobsData.model_validate_json(
        MinuteJobsData(available=True, jobs=(job,)).model_dump_json()).jobs
    assert type(restored_job) is MinuteParameterJob and restored_job == job
    assert restored_job.parameters.model_dump_json() == parameters.model_dump_json()
    assert restored_job.native_id.startswith("ap." if family == "auction_gap" else "gp.")
    assert restored_job.evaluator_semantic_version == "2.0.0"


@pytest.mark.parametrize("field", ["path", "native_id", "native_version", "native_head", "trusted", "owner_id"])
def test_parameter_wire_rejects_browser_authority_and_old_selector_fields(field: str) -> None:
    old = configuration()
    data = {"kind": "minute_parameter_replay", "source_key": "verified.complete-facts", "source_version": 1,
        "full_input_hash": "a" * 64, "parameters": MinuteParameterSet(parameters=MinuteNShapeParameters()).model_dump(mode="json"),
        "protocol": old.protocol.model_dump(mode="json"), "deadline": old.deadline.isoformat(), "random_seed": 0,
        field: "cannot-grant-authority"}
    with pytest.raises(ValidationError):
        MinuteParameterRunConfig.model_validate(data)


def test_parameter_job_wire_preserves_complete_recipe_and_fresh_semantic_identity() -> None:
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(entry_mode="amount_surge",
        max_hold_days=20, paper={"stop_loss_pct": 0.012345}))
    now = datetime(2026, 10, 7, tzinfo=UTC)
    common = dict(job_id=uuid4(), status="completed", version=1, created_at=now, updated_at=now,
        spec_hash="a" * 64, source_key="prepared.opaque", source_version=1, full_input_hash="b" * 64,
        native_name="N字反包", native_version=1, start_date=date(2026, 7, 31), end_date=date(2026, 8, 4),
        result_hash="c" * 64)
    job = MinuteParameterJob(**common, native_id=parameters.definition_id, family="n_shape",
        parameters=parameters, parameter_hash=parameters.fingerprint)
    restored = MinuteJobsData.model_validate_json(MinuteJobsData(available=True, jobs=(job,)).model_dump_json())
    assert type(restored.jobs[0]) is MinuteParameterJob
    assert restored.jobs[0] == job
    assert restored.jobs[0].parameters.parameters.paper.stop_loss_pct == 0.012345
    original = MinuteJob(**common, native_id="n_shape")
    assert type(MinuteJobsData(available=True, jobs=(original,)).jobs[0]) is MinuteJob
    assert "kind" not in original.model_dump()
    for changed in ({"parameter_hash": "d" * 64}, {"family": "auction_gap"},
        {"native_id": "np." + "a" * 52}, {"evaluator_semantic_version": "1.0.0"}):
        with pytest.raises(ValidationError):
            MinuteParameterJob.model_validate(job.model_dump(mode="python") | changed)
