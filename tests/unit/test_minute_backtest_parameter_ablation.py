from __future__ import annotations

from importlib import import_module, util

import pytest

from rquant.dashboard.strategy_lab_data import growth_board_ablation_specs
from rquant.minute_backtest_parameters import (
    MinuteGrowthParameters,
    MinuteNShapeParameters,
    MinuteParameterSet,
)


def test_all_five_variants_use_original_flags_and_keep_the_complete_advanced_recipe() -> None:
    assert util.find_spec("rquant.minute_backtest_parameter_ablation") is not None, (
        "typed growth ablation mapping is not implemented"
    )
    api = import_module("rquant.minute_backtest_parameter_ablation")
    original = MinuteParameterSet(
        parameters=MinuteGrowthParameters(
            require_inner_outer=True,
            max_inner_outer_ratio=0.7,
            require_large_net_vol=True,
            min_large_net_vol=500.0,
            require_fresh_surge=True,
            fresh_lookback_days=7,
            fresh_max_prior_volume_ratio=1.5,
            min_listing_trading_days=60,
            require_board_favor=True,
            min_board_gap_up_ratio=0.7,
            min_board_auction_amount_ratio=2.5,
            enable_factor_confirm=True,
            factor_score_threshold=60.0,
            max_hold_days=8,
            paper={
                "candidate_id": "growth-advanced",
                "stop_loss_pct": 0.06,
                "take_profit_pct": 0.11,
                "trailing_stop_pct": 0.04,
                "entry_slippage_pct": 0.012,
            },
        )
    )
    before = original.model_dump(mode="json")
    variants = api.growth_board_parameter_ablation(original)
    specs = growth_board_ablation_specs()
    assert [item.key for item in variants] == [
        "full",
        "no_vwap",
        "no_same_minute",
        "no_accel_5m",
        "cum_only",
    ]
    for variant, spec in zip(variants, specs, strict=True):
        expected = original.model_dump(mode="python")
        expected["parameters"].update(
            {
                name: getattr(spec, name)
                for name in ("require_vwap_strength", "use_same_minute_surge", "use_accel_surge")
            }
        )
        assert variant.parameters == MinuteParameterSet.model_validate(expected)
        assert variant.label == spec.label and variant.description == spec.description
        assert variant.parameters.parameters.owner_config().model_dump(
            mode="json"
        ) == variant.parameters.parameters.model_dump(mode="json", exclude={"family"})
    assert len({item.parameters.fingerprint for item in variants}) == 5
    assert original.model_dump(mode="json") == before


def test_non_growth_recipe_is_not_coerced_into_growth_parameters() -> None:
    assert util.find_spec("rquant.minute_backtest_parameter_ablation") is not None, (
        "typed growth ablation mapping is not implemented"
    )
    with pytest.raises(ValueError):
        import_module("rquant.minute_backtest_parameter_ablation").growth_board_parameter_ablation(
            MinuteParameterSet(parameters=MinuteNShapeParameters())
        )
