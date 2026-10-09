from datetime import time

import pytest
from pydantic import ValidationError

from rquant.auction_gap_strategy import AuctionGapMinuteReplayConfig
from rquant.growth_board_surge_strategy import GrowthBoardSurgeConfig
from rquant.minute_backtest_parameters import (
    MinuteAuctionGapParameters,
    MinuteGrowthParameters,
    MinuteNShapeParameters,
    MinuteParameterSet,
    n_shape_volume_profile_parameters,
)
from rquant.minute_replay import MinuteReplayConfig
from rquant.strategy_compare import _volume_profile_config


def test_complete_default_configs_keep_the_original_math_owner_values() -> None:
    pairs = (
        (MinuteNShapeParameters(), MinuteReplayConfig()),
        (
            MinuteAuctionGapParameters(start_date="2025-01-02", end_date="2025-01-03"),
            AuctionGapMinuteReplayConfig(start_date="2025-01-02", end_date="2025-01-03"),
        ),
        (MinuteGrowthParameters(), GrowthBoardSurgeConfig()),
    )
    for parameters, original in pairs:
        assert parameters.owner_config().model_dump(mode="json") == original.model_dump(mode="json")
        assert set(type(original).model_fields) <= set(type(parameters).model_fields)


@pytest.mark.parametrize(
    "mode", ["first_break", "break_retest", "late_confirm", "vwap_confirm", "amount_surge", "factor_confirm"]
)
def test_six_entry_modes_are_explicit_parameters(mode: str) -> None:
    parameters = MinuteNShapeParameters(entry_mode=mode, max_hold_days=20)
    assert parameters.owner_config().entry_mode == mode
    assert parameters.owner_config().max_hold_days == 20
    assert MinuteParameterSet(parameters=parameters).definition_version == 1
    assert MinuteParameterSet(parameters=parameters).evaluator_semantic_version == "2.0.0"


@pytest.mark.parametrize("variant", ["baseline", "vp_risk_only", "vp_90"])
def test_volume_profile_presets_call_the_original_owner(variant: str) -> None:
    profile = n_shape_volume_profile_parameters(variant)
    assert profile.model_dump(mode="json") == _volume_profile_config(variant).model_dump(mode="json")
    parameters = MinuteNShapeParameters(volume_profile=profile)
    assert parameters.owner_config().volume_profile == _volume_profile_config(variant)


@pytest.mark.parametrize("frequency", ["1min", "5min", "15min", "30min", "60min"])
def test_all_original_frequencies_are_bound_to_each_family(frequency: str) -> None:
    for parameters in (
        MinuteNShapeParameters(freq=frequency),
        MinuteAuctionGapParameters(start_date="2025-01-02", end_date="2025-01-03", freq=frequency),
        MinuteGrowthParameters(freq=frequency),
    ):
        assert parameters.owner_config().freq == frequency


def test_advanced_fields_are_not_discarded_during_owner_conversion() -> None:
    n_shape = MinuteNShapeParameters(
        preset_name="n-shape-combined", entry_mode="amount_surge", carry_low_ratio=0.98,
        carry_close_ratio=1.03, break_high_ratio=1.02, retest_tolerance_pct=0.013,
        late_confirm_at=time(11, 5), vwap_buffer_pct=0.01, amount_surge_lookback=30,
        amount_surge_min_prior_minutes=10, amount_surge_ratio=3.3,
        factor_score_threshold=57.0, price_discontinuity_pct=0.025,
        paper={"candidate_id": "explicit-study", "stop_loss_pct": 0.07, "entry_buffer_pct": 0.012,
               "entry_slippage_pct": 0.015, "take_profit_pct": 0.09, "trailing_stop_pct": 0.04},
        volume_profile={"enabled": True, "filter_entry": False, "require_profile": False,
                        "lookback_days": (30, 90), "min_reward_risk": 2.3, "bin_ratio": 0.012},
    )
    auction = MinuteAuctionGapParameters(
        start_date="2025-01-02", end_date="2025-02-03", gap_mode="strict_high",
        min_auction_vol_ratio_5d=0.3, max_auction_vol_ratio_5d=2.1, st_filter="literal_lower",
        max_hold_days=10, entry_start_time=time(9, 35), entry_pullback_tolerance_pct=0.03,
        entry_vwap_buffer_pct=0.02, min_limit_progress_pct=0.6, next_auction_weak_gap_pct=-0.025,
        strong_seal_min_close_minutes=12, strong_seal_weak_gap_pct=-0.04,
        next_morning_exit_until=time(10, 15), next_morning_vwap_break_buffer_pct=0.009,
        seal_hold_enabled=True, seal_hold_max_days=8, seal_hold_max_open_times=2,
        seal_hold_min_fd_to_circ_pct=0.12, factor_score_threshold=65.0, price_tol=0.02,
    )
    growth = MinuteGrowthParameters(
        min_signal_time=time(9, 45), lookback_days=90, min_hist_days=30,
        min_cum_amount_ratio=2.7, min_same_minute_amount_ratio=3.9, min_amount_accel_5m=4.2,
        require_vwap_strength=False, use_same_minute_surge=False, use_accel_surge=True,
        vwap_buffer_pct=0.01, require_inner_outer=True, max_inner_outer_ratio=0.7,
        require_large_net_vol=True, min_large_net_vol=500.0, require_fresh_surge=True,
        fresh_lookback_days=20, fresh_max_prior_volume_ratio=1.5, min_listing_trading_days=60,
        require_board_favor=True, min_board_gap_up_ratio=0.7, min_board_auction_amount_ratio=2.5,
        board_hist_days=7, enable_factor_confirm=True, factor_score_threshold=60.0,
        max_hold_days=10, price_tol=0.02,
    )
    for parameters in (n_shape, auction, growth):
        wire = parameters.model_dump(mode="json", exclude={"family"})
        assert parameters.owner_config().model_dump(mode="json") == wire
        assert MinuteParameterSet.model_validate_json(
            MinuteParameterSet(parameters=parameters).model_dump_json()
        ).parameters == parameters


@pytest.mark.parametrize(
    "model, values",
    [
        (MinuteNShapeParameters, {"max_hold_days": 21}),
        (MinuteNShapeParameters, {"freq": "2min"}),
        (MinuteNShapeParameters, {"preset_name": "arbitrary-source-path"}),
        (MinuteNShapeParameters, {"paper": {"stop_loss_pct": float("nan")}}),
        (MinuteNShapeParameters, {"volume_profile": {"lookback_days": (-1,)}}),
        (MinuteNShapeParameters, {"paper": {"ignored_exit": True}}),
        (MinuteGrowthParameters, {"lookback_days": 91}),
        (MinuteGrowthParameters, {"min_hist_days": 0}),
        (MinuteGrowthParameters, {"max_hold_days": 11}),
        (MinuteGrowthParameters, {"min_large_net_vol": float("inf")}),
        (MinuteAuctionGapParameters, {"start_date": "2025-02-02", "end_date": "2025-01-01"}),
        (MinuteAuctionGapParameters, {"start_date": "2025-01-01", "end_date": "2025-01-02", "max_hold_days": 11}),
        (MinuteAuctionGapParameters, {"start_date": "2025-01-01", "end_date": "2025-01-02", "min_auction_vol_ratio_5d": 3.0, "max_auction_vol_ratio_5d": 2.0}),
    ],
)
def test_invalid_or_unhandled_parameter_values_reject(model: type, values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(values)


def test_nested_parameters_are_immutable_and_owner_copies_do_not_change_identity() -> None:
    parameters = MinuteNShapeParameters()
    recipe = MinuteParameterSet(parameters=parameters)
    before = recipe.fingerprint
    with pytest.raises(ValidationError):
        parameters.paper.stop_loss_pct = 0.08
    with pytest.raises(ValidationError):
        parameters.volume_profile.filter_entry = False
    owner = parameters.owner_config()
    owner.paper.stop_loss_pct = 0.08
    assert recipe.fingerprint == before
    assert parameters.paper.stop_loss_pct == 0.03


def test_parameter_identity_includes_complete_nested_math_config_and_new_namespace() -> None:
    baseline = MinuteParameterSet(parameters=MinuteNShapeParameters())
    changed = MinuteParameterSet(parameters=MinuteNShapeParameters(
        volume_profile=n_shape_volume_profile_parameters("vp_risk_only"),
        paper={"entry_slippage_pct": 0.01},
    ))
    assert changed.fingerprint != baseline.fingerprint
    import base64

    assert baseline.definition_id.startswith("np.") and len(baseline.definition_id) == 55
    encoded = baseline.definition_id.split(".", 1)[1].upper()
    assert base64.b32decode(encoded + "=" * (-len(encoded) % 8)).hex() == baseline.fingerprint
    assert baseline.definition_id != baseline.parameters.family
    assert baseline.definition_version == 1
    with pytest.raises(ValidationError):
        MinuteParameterSet.model_validate({"parameters": {"family": "n_shape", "unhandled_legacy_option": True}})
