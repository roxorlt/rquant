from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib import import_module, util
from types import ModuleType
from typing import TYPE_CHECKING

import pytest
from tests.unit.test_minute_backtest_study_protocols import auction_study, study_protocol

from rquant.minute_backtest_parameter_optimizer import MinuteStudyTrainingObservation
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.runtime_contracts import canonical_sha256

if TYPE_CHECKING:
    from rquant.minute_backtest_study_protocols import MinuteStudyProtocol

CUTOFF = datetime(2026, 10, 7, tzinfo=UTC)


def heatmap_api() -> ModuleType:
    assert util.find_spec("rquant.minute_backtest_parameter_heatmap") is not None, (
        "two-parameter training heatmap is not implemented"
    )
    return import_module("rquant.minute_backtest_parameter_heatmap")


def set_parameter(body: dict[str, object], path: str, value: object) -> None:
    branch = body
    parts = path.split(".")
    for part in parts[:-1]:
        child = branch[part]
        assert isinstance(child, dict)
        branch = child
    branch[parts[-1]] = value


def trial(
    base: MinuteStudyProtocol | None = None, **parameter_updates: object
) -> MinuteStudyProtocol:
    original = base or study_protocol()
    body = original.model_dump(mode="python")
    for path, value in parameter_updates.items():
        set_parameter(body["parameters"]["parameters"], path, value)
    recipe = MinuteParameterSet.model_validate(body["parameters"])
    body["parameters"] = recipe
    body["head"].update(
        definition_id=recipe.definition_id,
        parameter_fingerprint=recipe.fingerprint,
        registration_fingerprint=canonical_sha256({"registration": recipe.fingerprint}),
        spec_fingerprint=canonical_sha256({"spec": recipe.fingerprint}),
        executable_fingerprint=canonical_sha256({"executable": recipe.fingerprint}),
    )
    return type(original).model_validate(body)


def observed(
    protocol: MinuteStudyProtocol, *, mean: float = 1.0, trades: int = 8
) -> MinuteStudyTrainingObservation:
    return MinuteStudyTrainingObservation(
        study_id=protocol.study_id,
        source=protocol.source,
        head=protocol.head,
        parameter_fingerprint=protocol.parameters.fingerprint,
        train_start=protocol.split.train_start,
        train_end=protocol.split.train_end,
        result_hash=canonical_sha256({"sealed_fixture": protocol.study_id}),
        available_at=CUTOFF,
        summary={
            "trades": trades,
            "mean_ret_pct": mean if trades else None,
            "win_rate_pct": 60.0 if trades else None,
            "worst_ret_pct": -2.0 if trades else None,
            "gap_stop_rate_pct": 0.0 if trades else None,
        },
    )


def square() -> tuple[list[MinuteStudyProtocol], list[MinuteStudyTrainingObservation]]:
    protocols = [
        trial(max_hold_days=x, **{"paper.stop_loss_pct": y})
        for y in (0.01, 0.02, 0.03)
        for x in (2, 3, 4)
    ]
    return protocols, [observed(p, mean=float(i)) for i, p in enumerate(protocols)]


def project(
    protocols: list[MinuteStudyProtocol],
    observations: list[MinuteStudyTrainingObservation],
    **updates: object,
) -> object:
    values = {
        "selection_cutoff": CUTOFF,
        "x_parameter": "max_hold_days",
        "y_parameter": "paper.stop_loss_pct",
        "current_study_id": protocols[min(4, len(protocols) - 1)].study_id,
    }
    values.update(updates)
    return heatmap_api().build_minute_study_heatmap(protocols, observations, **values)


def test_hand_calculated_center_scores_and_one_cell_ring_exclude_the_center() -> None:
    protocols, observations = square()
    result = project(protocols[::-1], observations[::-1])
    assert result.x_axis.parameter_name == "max_hold_days"
    assert result.x_axis.values == (2, 3, 4)
    assert result.y_axis.values == (0.01, 0.02, 0.03)
    # Original owner: mean + (60-50)*.02 - abs(-2)*.15 = mean - .1.
    assert [cell.training_score for cell in result.cells] == pytest.approx(
        [-0.1, 0.9, 1.9, 2.9, 3.9, 4.9, 5.9, 6.9, 7.9]
    )
    center = result.cells[4]
    assert center.is_current is True
    assert sum(cell.is_current for cell in result.cells) == 1
    assert center.protocol == protocols[4]
    assert center.observation == observations[4]
    assert center.neighborhood.status == "available"
    assert center.neighborhood.minimum_score == -0.1
    assert (1, 1) not in center.neighborhood.coordinates
    assert len(center.neighborhood.coordinates) == 8
    assert result.trial_set_hash == project(protocols, observations).trial_set_hash
    assert heatmap_api().MinuteStudyHeatmap.model_validate_json(result.model_dump_json()) == result


def test_corner_uses_only_three_actual_adjacent_cells_and_preserves_zero_score() -> None:
    protocols, observations = square()
    observations[0] = observed(protocols[0], mean=0.1)
    result = project(protocols, observations, current_study_id=protocols[0].study_id)
    corner = result.cells[0]
    assert corner.training_score == 0.0
    assert corner.status == "available"
    assert corner.neighborhood.coordinates == ((1, 0), (0, 1), (1, 1))
    assert corner.neighborhood.minimum_score == 0.9


@pytest.mark.parametrize("trades", [0, 4])
def test_insufficient_trades_keep_cell_and_neighbor_minimum_unavailable(trades: int) -> None:
    protocols, observations = square()
    observations[0] = observed(protocols[0], trades=trades)
    result = project(protocols, observations, current_study_id=protocols[0].study_id)
    corner, center = result.cells[0], result.cells[4]
    assert corner.protocol == protocols[0]
    assert corner.study_id == protocols[0].study_id
    assert corner.observation.summary.trades == trades
    assert corner.status == "insufficient_trades"
    assert corner.training_score is None
    assert center.neighborhood.status == "unavailable"
    assert center.neighborhood.minimum_score is None
    assert center.neighborhood.unavailable_coordinates == ((0, 0),)


def test_sparse_random_trials_do_not_fill_missing_neighbors_or_affect_a_distant_corner() -> None:
    protocols, observations = square()
    current = protocols[4].study_id
    result = project(protocols[1:], observations[1:], current_study_id=current)
    missing, center, far_corner = result.cells[0], result.cells[4], result.cells[8]
    assert missing.status == "missing_trial"
    assert missing.study_id is None and missing.protocol is None and missing.observation is None
    assert missing.training_score is None
    assert center.neighborhood.status == "unavailable"
    assert center.neighborhood.unavailable_coordinates == ((0, 0),)
    assert far_corner.neighborhood.minimum_score == 3.9


def test_numeric_and_nested_boolean_axes_have_natural_order_and_exact_recipe_identity() -> None:
    protocols = [
        trial(**{"volume_profile.enabled": enabled, "paper.stop_loss_pct": stop})
        for stop in (0.02, 0.01)
        for enabled in (True, False)
    ]
    result = project(
        protocols,
        [observed(p) for p in protocols],
        x_parameter="volume_profile.enabled",
        current_study_id=protocols[0].study_id,
    )
    assert result.x_axis.values == (False, True)
    assert all(type(value) is bool for value in result.x_axis.values)
    assert result.y_axis.values == (0.01, 0.02)
    for cell in result.cells:
        assert cell.protocol.parameters.parameters.volume_profile.enabled is cell.x_value
        assert cell.protocol.parameters.parameters.paper.stop_loss_pct == cell.y_value
        assert cell.observation.parameter_fingerprint == cell.protocol.parameters.fingerprint


def test_original_coupled_parameter_validation_excludes_illegal_neighbor_combination() -> None:
    base = auction_study()
    protocols = [
        trial(base, min_auction_vol_ratio_5d=x, max_auction_vol_ratio_5d=y)
        for x, y in ((2.0, 3.0), (2.0, 9.0), (8.0, 9.0))
    ]
    result = project(
        protocols,
        [observed(p, mean=float(i + 1)) for i, p in enumerate(protocols)],
        x_parameter="min_auction_vol_ratio_5d",
        y_parameter="max_auction_vol_ratio_5d",
        current_study_id=protocols[-1].study_id,
    )
    assert result.cells[1].status == "invalid_parameters"
    assert result.cells[1].protocol is None
    assert result.cells[3].neighborhood.coordinates == ((0, 0), (0, 1))
    assert result.cells[3].neighborhood.minimum_score == 0.9


@pytest.mark.parametrize(
    "x,y",
    [
        ("unknown", "paper.stop_loss_pct"),
        ("max_hold_days", "max_hold_days"),
        ("freq", "paper.stop_loss_pct"),
        ("family", "paper.stop_loss_pct"),
        ("random_seed", "paper.stop_loss_pct"),
        ("volume_profile.lookback_days", "paper.stop_loss_pct"),
        ("late_confirm_at", "paper.stop_loss_pct"),
        ("parameters.max_hold_days", "paper.stop_loss_pct"),
        ("__class__", "paper.stop_loss_pct"),
    ],
)
def test_unknown_same_nonscalar_or_nonparameter_axis_is_rejected(x: str, y: str) -> None:
    protocols, observations = square()
    with pytest.raises(ValueError):
        project(protocols, observations, x_parameter=x, y_parameter=y)


def test_constant_axis_is_not_a_two_parameter_grid() -> None:
    protocols, observations = square()
    with pytest.raises(ValueError, match="distinct"):
        project(protocols[:3], observations[:3], current_study_id=protocols[0].study_id)


@pytest.mark.parametrize(
    "field,value",
    [
        ("score_profile", "v2_low_position"),
        ("top_n", 3),
        ("min_trades", 6),
        ("random_seed", 18),
    ],
)
def test_selection_dimensions_must_match_outside_the_two_axes(field: str, value: object) -> None:
    protocols, observations = square()
    protocols[0] = type(protocols[0]).model_validate(
        {**protocols[0].model_dump(mode="python"), field: value}
    )
    observations[0] = observed(protocols[0])
    with pytest.raises(ValueError, match="non-axis"):
        project(protocols, observations)


def test_actual_distinct_preparation_timestamps_preserve_each_trial_and_score() -> None:
    original, _ = square()
    protocols = [
        type(p).model_validate(
            {
                **p.model_dump(mode="python"),
                "requested_at": CUTOFF - timedelta(seconds=len(original) - index),
            }
        )
        for index, p in enumerate(original)
    ]
    observations = [observed(p, mean=float(index)) for index, p in enumerate(protocols)]
    result = project(protocols, observations)
    assert [cell.protocol for cell in result.cells] == protocols
    assert [cell.observation for cell in result.cells] == observations
    assert [cell.training_score for cell in result.cells] == pytest.approx(
        [-0.1, 0.9, 1.9, 2.9, 3.9, 4.9, 5.9, 6.9, 7.9]
    )


def test_trial_requested_after_selection_cutoff_remains_rejected() -> None:
    protocols, observations = square()
    protocols[0] = type(protocols[0]).model_validate(
        {**protocols[0].model_dump(mode="python"), "requested_at": CUTOFF + timedelta(seconds=1)}
    )
    observations[0] = observed(protocols[0])
    with pytest.raises(ValueError, match="not available at the selection cutoff"):
        project(protocols, observations)


def test_distinct_preparation_clocks_do_not_allow_a_different_fixed_seed() -> None:
    protocols, observations = square()
    protocols[0] = type(protocols[0]).model_validate(
        {
            **protocols[0].model_dump(mode="python"),
            "requested_at": CUTOFF - timedelta(seconds=1),
            "random_seed": 18,
        }
    )
    observations[0] = observed(protocols[0])
    with pytest.raises(ValueError, match="non-axis"):
        project(protocols, observations)


def test_other_paper_term_cannot_silently_mix_into_the_grid() -> None:
    protocols, observations = square()
    protocols[0] = trial(
        max_hold_days=2, **{"paper.stop_loss_pct": 0.01, "paper.entry_buffer_pct": 0.01}
    )
    observations[0] = observed(protocols[0])
    with pytest.raises(ValueError, match="non-axis"):
        project(protocols, observations)


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", "foreign-owner"),
        ("source_key", "foreign-source"),
        ("source_version", 2),
        ("full_input_hash", "8" * 64),
        ("dataset_snapshot_id", "9" * 64),
    ],
)
def test_mixed_source_or_owner_is_rejected(field: str, value: object) -> None:
    protocols, observations = square()
    body = protocols[0].model_dump(mode="python")
    body["source"][field] = value
    protocols[0] = type(protocols[0]).model_validate(body)
    observations[0] = observed(protocols[0])
    with pytest.raises(ValueError):
        project(protocols, observations)


@pytest.mark.parametrize("field", ["head", "study_id", "source", "train_end"])
def test_each_observation_is_bound_to_its_original_recipe_head_and_window(field: str) -> None:
    protocols, observations = square()
    body = observations[0].model_dump(mode="python")
    if field == "head":
        body["head"]["registration_fingerprint"] = "b" * 64
    elif field == "study_id":
        body["study_id"] = "c" * 64
    elif field == "source":
        body["source"]["owner_id"] = "foreign-owner"
    else:
        body["train_end"] -= timedelta(days=1)
    observations[0] = MinuteStudyTrainingObservation.model_validate(body)
    with pytest.raises(ValueError):
        project(protocols, observations)


def test_missing_future_or_repeated_observation_is_rejected_even_in_sparse_grid() -> None:
    protocols, observations = square()
    with pytest.raises(ValueError):
        project(protocols, observations[:-1])
    body = observations[0].model_dump(mode="python")
    body["available_at"] = CUTOFF + timedelta(seconds=1)
    with pytest.raises(ValueError):
        project(protocols, [MinuteStudyTrainingObservation.model_validate(body), *observations[1:]])
    with pytest.raises(ValueError):
        project(protocols, [*observations, observations[0]])


def test_duplicate_coordinate_is_rejected_even_when_both_distinct_heads_are_bound() -> None:
    protocols, observations = square()
    body = protocols[0].model_dump(mode="python")
    body["head"]["spec_fingerprint"] = "d" * 64
    duplicate = type(protocols[0]).model_validate(body)
    with pytest.raises(ValueError, match="duplicate.*cell"):
        project([*protocols, duplicate], [*observations, observed(duplicate)])


def test_current_point_and_empty_input_must_reference_an_actual_trial() -> None:
    protocols, observations = square()
    with pytest.raises(ValueError):
        project(protocols, observations, current_study_id="e" * 64)
    with pytest.raises(ValueError):
        heatmap_api().build_minute_study_heatmap(
            [], [], selection_cutoff=CUTOFF,
            x_parameter="max_hold_days", y_parameter="paper.stop_loss_pct",
            current_study_id="e" * 64,
        )


def test_original_score_owner_is_called_once_per_trial_and_no_test_returns_enter_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import strategy_optimizer

    protocols, observations = square()
    actual = strategy_optimizer._score_row
    calls = []

    def original(row: object, *, min_trades: int) -> float:
        calls.append(min_trades)
        return actual(row, min_trades=min_trades)

    monkeypatch.setattr(strategy_optimizer, "_score_row", original)
    result = project(protocols, observations)
    assert calls == [5] * 9
    assert result.selection_cutoff == CUTOFF
    assert result.cells[4].training_score == 3.9


def test_original_trial_and_dense_grid_work_budget_are_not_expanded() -> None:
    from rquant.minute_backtest_contracts import MAX_WORK_UNITS

    protocols, observations = square()
    with pytest.raises(ValueError, match="budget"):
        project([protocols[0]] * (MAX_WORK_UNITS + 1), observations)
    sparse = [
        trial(carry_low_ratio=1.0 + index / 1000, **{"paper.stop_loss_pct": index / 1000})
        for index in range(1, 143)
    ]
    assert MAX_WORK_UNITS < 142 * 142
    with pytest.raises(ValueError, match="grid.*budget"):
        project(
            sparse, [observed(p) for p in sparse], x_parameter="carry_low_ratio",
            current_study_id=sparse[0].study_id,
        )


def test_input_byte_budget_is_applied_before_scoring(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import strategy_optimizer
    from rquant.minute_backtest_contracts import MAX_INPUT_BYTES

    api = heatmap_api()
    assert api.MAX_INPUT_BYTES == MAX_INPUT_BYTES
    monkeypatch.setattr(api, "MAX_INPUT_BYTES", 1)

    def unexpected_score(*args: object, **kwargs: object) -> float:
        pytest.fail("over-budget material reached the score owner")

    monkeypatch.setattr(strategy_optimizer, "_score_row", unexpected_score)
    protocols, observations = square()
    with pytest.raises(ValueError, match="byte budget"):
        project(protocols, observations)
