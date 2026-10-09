from __future__ import annotations

import random
from datetime import time

import pytest
from pydantic import ValidationError

from rquant.minute_backtest_parameter_search import (
    MinuteParameterSearchAxis,
    MinuteParameterSearchPlan,
    MinuteParameterSearchRequest,
    build_minute_parameter_search_plan,
)
from rquant.minute_backtest_parameters import (
    MinuteAuctionGapParameters,
    MinuteGrowthParameters,
    MinuteNShapeParameters,
    MinutePaperParameters,
    MinuteParameterSet,
    MinuteVolumeProfileParameters,
)


def original_base() -> MinuteParameterSet:
    return MinuteParameterSet(
        parameters=MinuteNShapeParameters(
            preset_name="n-shape-pool2",
            freq="5min",
            late_confirm_at=time(14, 37),
            carry_close_ratio=1.25,
            paper=MinutePaperParameters(candidate_id="research-original", stop_loss_pct=0.04),
            volume_profile=MinuteVolumeProfileParameters(enabled=True, lookback_days=(20, 60)),
        )
    )


def axis(path: str, *values: object) -> MinuteParameterSearchAxis:
    return MinuteParameterSearchAxis.model_validate({"path": path, "values": values})


def test_grid_is_exact_cartesian_order_and_retains_full_original_recipe() -> None:
    base = original_base()
    request = MinuteParameterSearchRequest(
        base=base,
        axes=(axis("max_hold_days", 1, 3), axis("paper.take_profit_pct", 0.03, 0.05, 0.07)),
        mode="grid",
        seed=7,
    )
    plan = build_minute_parameter_search_plan(request)
    assert (plan.space_size, plan.trial_count, plan.mode, plan.seed) == (6, 6, "grid", 7)
    assert [
        (p.parameters.max_hold_days, p.parameters.paper.take_profit_pct) for p in plan.trials
    ] == [
        (1, 0.03),
        (1, 0.05),
        (1, 0.07),
        (3, 0.03),
        (3, 0.05),
        (3, 0.07),
    ]
    for recipe in plan.trials:
        expected = base.model_dump(mode="json")
        expected["parameters"]["max_hold_days"] = recipe.parameters.max_hold_days
        expected["parameters"]["paper"]["take_profit_pct"] = recipe.parameters.paper.take_profit_pct
        assert recipe == MinuteParameterSet.model_validate(expected)
        assert recipe.parameters.owner_config().preset_name == "n-shape-pool2"
    assert base.parameters.max_hold_days == 5
    assert plan.request == request


def test_nested_boolean_and_integer_tuple_values_use_original_owner_validation() -> None:
    plan = build_minute_parameter_search_plan(
        MinuteParameterSearchRequest(
            base=original_base(),
            axes=(
                axis("volume_profile.filter_entry", False, True),
                axis("volume_profile.lookback_days", (5, 20), (30, 90)),
            ),
            mode="grid",
            seed=0,
            requested_trials=4,
        )
    )
    assert [
        (p.parameters.volume_profile.filter_entry, p.parameters.volume_profile.lookback_days)
        for p in plan.trials
    ] == [(False, (5, 20)), (False, (30, 90)), (True, (5, 20)), (True, (30, 90))]
    assert all(p.parameters.paper == original_base().parameters.paper for p in plan.trials)


def test_random_is_local_reproducible_unique_and_seed_bound() -> None:
    kwargs = {
        "base": original_base(),
        "axes": (
            axis("max_hold_days", 1, 2, 3, 4, 5),
            axis("paper.take_profit_pct", 0.03, 0.05, 0.07),
        ),
        "mode": "random",
        "requested_trials": 7,
    }
    global_state = random.getstate()
    first = build_minute_parameter_search_plan(MinuteParameterSearchRequest(seed=11, **kwargs))
    again = build_minute_parameter_search_plan(MinuteParameterSearchRequest(seed=11, **kwargs))
    different = build_minute_parameter_search_plan(MinuteParameterSearchRequest(seed=19, **kwargs))
    assert first == again
    assert first.plan_hash == again.plan_hash
    assert first.trials != different.trials
    assert first.plan_hash != different.plan_hash
    assert len({p.fingerprint for p in first.trials}) == first.trial_count == 7
    assert first.space_size == 15
    assert random.getstate() == global_state
    assert all(
        p.parameters.freq == "5min" and p.parameters.paper.candidate_id == "research-original"
        for p in first.trials
    )


def test_random_can_cover_whole_space_without_duplicates() -> None:
    plan = build_minute_parameter_search_plan(
        MinuteParameterSearchRequest(
            base=original_base(),
            axes=(axis("max_hold_days", 1, 2, 3),),
            mode="random",
            seed=2**63 - 1,
            requested_trials=3,
        )
    )
    assert sorted(p.parameters.max_hold_days for p in plan.trials) == [1, 2, 3]


@pytest.mark.parametrize(
    "parameters",
    [
        MinuteAuctionGapParameters(start_date="2026-01-05", end_date="2026-04-30", freq="15min"),
        MinuteGrowthParameters(freq="30min", require_inner_outer=True),
    ],
)
def test_original_families_keep_identity_dates_frequency_and_all_other_fields(
    parameters: object,
) -> None:
    base = MinuteParameterSet(parameters=parameters)
    plan = build_minute_parameter_search_plan(
        MinuteParameterSearchRequest(
            base=base,
            axes=(axis("max_hold_days", 1, 2),),
            mode="grid",
            seed=4,
        )
    )
    for recipe in plan.trials:
        expected = base.model_dump(mode="json")
        expected["parameters"]["max_hold_days"] = recipe.parameters.max_hold_days
        assert recipe.model_dump(mode="json") == expected
        assert type(recipe.parameters) is type(base.parameters)


@pytest.mark.parametrize(
    "path",
    [
        "family",
        "preset_name",
        "freq",
        "entry_mode",
        "late_confirm_at",
        "paper.candidate_id",
        "start_date",
        "end_date",
        "parameters.max_hold_days",
        "volume_profile",
        "unknown",
        "paper.__class__",
        "paper.stop_loss_pct.extra",
        "kind",
        "schema_version",
    ],
)
def test_unknown_identity_and_source_boundary_axes_are_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        build_minute_parameter_search_plan(
            MinuteParameterSearchRequest(
                base=original_base(),
                axes=(axis(path, 1),),
                mode="grid",
                seed=0,
            )
        )


@pytest.mark.parametrize(
    ("path", "values"),
    [
        ("max_hold_days", (0,)),
        ("max_hold_days", (21,)),
        ("max_hold_days", (True,)),
        ("max_hold_days", (1.0,)),
        ("max_hold_days", ("3",)),
        ("paper.stop_loss_pct", (0.0,)),
        ("paper.stop_loss_pct", (True,)),
        ("paper.stop_loss_pct", (float("inf"),)),
        ("paper.stop_loss_pct", (float("nan"),)),
        ("volume_profile.enabled", (0,)),
        ("volume_profile.lookback_days", ((0, 20),)),
        ("volume_profile.lookback_days", ((20, 20),)),
        ("volume_profile.lookback_days", ((),)),
    ],
)
def test_invalid_axis_values_are_not_coerced_or_silently_skipped(
    path: str, values: tuple[object, ...]
) -> None:
    with pytest.raises(ValueError):
        build_minute_parameter_search_plan(
            MinuteParameterSearchRequest(
                base=original_base(),
                axes=(axis(path, *values),),
                mode="grid",
                seed=0,
            )
        )


def test_optional_numeric_axis_and_equivalent_duplicate_values() -> None:
    base = MinuteParameterSet(
        parameters=MinuteAuctionGapParameters(
            start_date="2026-01-05",
            end_date="2026-04-30",
        )
    )
    plan = build_minute_parameter_search_plan(
        MinuteParameterSearchRequest(
            base=base,
            axes=(axis("factor_score_threshold", None, 0.5),),
            mode="grid",
            seed=0,
        )
    )
    assert [p.parameters.factor_score_threshold for p in plan.trials] == [None, 0.5]
    with pytest.raises(ValueError, match="duplicate"):
        build_minute_parameter_search_plan(
            MinuteParameterSearchRequest(
                base=original_base(),
                axes=(axis("carry_low_ratio", 1, 1.0),),
                mode="grid",
                seed=0,
            )
        )


def test_signed_zero_axis_values_are_equivalent_duplicates() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        build_minute_parameter_search_plan(
            MinuteParameterSearchRequest(
                base=original_base(),
                axes=(axis("volume_profile.support_buffer_pct", 0.0, -0.0),),
                mode="grid",
                seed=0,
            )
        )


@pytest.mark.parametrize("mode", ["grid", "random"])
def test_one_invalid_combination_rejects_entire_space_even_if_random_sample_would_miss_it(
    mode: str,
) -> None:
    base = MinuteParameterSet(
        parameters=MinuteAuctionGapParameters(
            start_date="2026-01-05",
            end_date="2026-04-30",
        )
    )
    with pytest.raises(ValueError, match="volume ratio"):
        build_minute_parameter_search_plan(
            MinuteParameterSearchRequest(
                base=base,
                axes=(
                    axis("min_auction_vol_ratio_5d", 0.5, 2.0),
                    axis("max_auction_vol_ratio_5d", 1.0, 3.0),
                ),
                mode=mode,
                seed=0,
                requested_trials=1 if mode == "random" else None,
            )
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"axes": ()},
        {"axes": (axis("max_hold_days", 1), axis("max_hold_days", 2))},
        {"axes": ({"path": "max_hold_days", "values": ()},)},
        {"mode": "random"},
        {"mode": "random", "requested_trials": 0},
        {"mode": "random", "requested_trials": 3},
        {"requested_trials": 1},
        {"seed": -1},
        {"seed": 2**63},
        {"seed": True},
        {"seed": 1.5},
    ],
)
def test_empty_duplicate_or_inconsistent_search_requests_fail(kwargs: dict[str, object]) -> None:
    payload = {
        "base": original_base(),
        "axes": (axis("max_hold_days", 1, 2),),
        "mode": "grid",
        "seed": 0,
    }
    payload.update(kwargs)
    with pytest.raises(ValueError):
        build_minute_parameter_search_plan(MinuteParameterSearchRequest.model_validate(payload))


def test_theoretical_space_limit_is_checked_before_any_recipe_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_validation(*args: object, **kwargs: object) -> None:
        pytest.fail("over-limit request must not generate recipes")

    base = original_base()
    # Each axis is finite and legal in isolation; the product is 20,010.
    monkeypatch.setattr(MinuteParameterSet, "model_validate", forbidden_validation)
    with pytest.raises(ValueError, match="space.*budget|space.*20"):
        MinuteParameterSearchRequest(
            base=base,
            axes=(
                axis("carry_low_ratio", *(1.0 + x / 10000 for x in range(145))),
                axis("carry_close_ratio", *(1.0 + x / 10000 for x in range(138))),
            ),
            mode="random",
            seed=0,
            requested_trials=1,
        )


def test_control_byte_limit_is_checked_before_recipes_and_small_random_subset_is_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    axes = (
        axis("max_hold_days", *range(1, 21)),
        axis("carry_low_ratio", *(1.0 + i / 100 for i in range(100))),
    )
    random_request = MinuteParameterSearchRequest(
        base=original_base(), axes=axes, mode="random", seed=3, requested_trials=2
    )
    assert build_minute_parameter_search_plan(random_request).trial_count == 2
    grid_request = MinuteParameterSearchRequest(
        base=original_base(), axes=axes, mode="grid", seed=3
    )

    def forbidden_validation(*args: object, **kwargs: object) -> None:
        pytest.fail("over-byte plan must not construct recipes")

    monkeypatch.setattr(MinuteParameterSet, "model_validate", forbidden_validation)
    with pytest.raises(ValueError, match="byte|control"):
        build_minute_parameter_search_plan(grid_request)


def test_plan_is_immutable_roundtrips_and_binding_covers_full_recipe_and_mode() -> None:
    request = MinuteParameterSearchRequest(
        base=original_base(), axes=(axis("max_hold_days", 1, 2),), mode="grid", seed=8
    )
    plan = build_minute_parameter_search_plan(request)
    assert MinuteParameterSearchPlan.model_validate_json(plan.model_dump_json()) == plan
    assert len(plan.plan_hash) == 64
    for instance, field, replacement in [
        (plan, "seed", 9),
        (plan.trials[0].parameters, "freq", "1min"),
    ]:
        with pytest.raises(ValidationError, match="frozen"):
            setattr(instance, field, replacement)
    other_base = MinuteParameterSet(parameters=MinuteNShapeParameters(carry_close_ratio=1.125))
    other = build_minute_parameter_search_plan(
        MinuteParameterSearchRequest(
            base=other_base,
            axes=request.axes,
            mode="grid",
            seed=8,
        )
    )
    assert plan.plan_hash != other.plan_hash


@pytest.mark.parametrize(
    "field", ["trial_count", "space_size", "plan_hash", "mode", "seed", "trials"]
)
def test_persisted_plan_cannot_forge_counts_identity_selection_or_trial_recipe(field: str) -> None:
    plan = build_minute_parameter_search_plan(
        MinuteParameterSearchRequest(
            base=original_base(),
            axes=(axis("max_hold_days", 1, 2),),
            mode="grid",
            seed=8,
        )
    )
    data = plan.model_dump(mode="json")
    if field == "trials":
        data[field][0]["parameters"]["paper"]["candidate_id"] = "other-owner-profile"
    else:
        data[field] = {
            "trial_count": 1,
            "space_size": 3,
            "plan_hash": "a" * 64,
            "mode": "random",
            "seed": 9,
        }[field]
    with pytest.raises(ValueError):
        MinuteParameterSearchPlan.model_validate(data)
