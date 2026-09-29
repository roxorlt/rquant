"""CSCV/PBO uses complete aligned period returns, never summary-only estimates."""

from __future__ import annotations

from datetime import date, timedelta
from math import comb, log, sqrt
from random import Random

import pytest
from pydantic import ValidationError


def _hand_case() -> dict[str, object]:
    starts = (0.02, 0.02, -0.01, -0.01)
    rows = []
    for first_mean in starts:
        for offset in range(15):
            noise = 0 if offset == 14 else (-1 if offset % 2 == 0 else 1)
            rows.append((first_mean + 0.01 * noise, 0.005 + 0.01 * noise))
    return {
        "candidate_ids": ("A", "B"),
        "period_end_dates": tuple(date(2026, 1, 1) + timedelta(days=i) for i in range(60)),
        "returns_by_observation": tuple(rows),
        "slice_count": 4,
    }


def test_four_slice_hand_case_has_exact_complements_ranks_logits_and_pbo() -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    inputs = CSCVInput(**_hand_case())
    result = calculate_cscv_pbo(inputs)

    assert result.inputs == inputs
    assert result.split_count == 6
    assert result.below_median_count == 2
    assert result.probability_of_backtest_overfitting == 2 / 6
    assert [split.in_sample_slices for split in result.splits] == [
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 2),
        (1, 3),
        (2, 3),
    ]
    assert [split.out_of_sample_slices for split in result.splits] == [
        (2, 3),
        (1, 3),
        (1, 2),
        (0, 3),
        (0, 2),
        (0, 1),
    ]
    assert [split.selected_candidate_id for split in result.splits] == [
        "A",
        "B",
        "B",
        "B",
        "B",
        "B",
    ]
    assert [split.out_of_sample_rank for split in result.splits] == [1, 2, 2, 2, 2, 1]
    assert [split.out_of_sample_relative_rank for split in result.splits] == [
        1 / 3,
        2 / 3,
        2 / 3,
        2 / 3,
        2 / 3,
        1 / 3,
    ]
    assert [split.logit for split in result.splits] == [
        -log(2),
        log(2),
        log(2),
        log(2),
        log(2),
        -log(2),
    ]
    base_sd = 0.01 * sqrt(28 / 29)
    first = result.splits[0]
    assert abs(first.in_sample_sharpe_by_candidate[0] - 0.02 / base_sd) < 1e-12
    assert abs(first.in_sample_sharpe_by_candidate[1] - 0.005 / base_sd) < 1e-12
    assert abs(first.out_of_sample_sharpe_by_candidate[0] + 0.01 / base_sd) < 1e-12
    assert abs(first.out_of_sample_sharpe_by_candidate[1] - 0.005 / base_sd) < 1e-12


def test_input_rejects_incomplete_unaligned_or_over_budget_trial_families() -> None:
    from rquant.overfit_pbo import CSCVInput

    valid = _hand_case()
    rows = valid["returns_by_observation"]
    days = valid["period_end_dates"]
    assert isinstance(rows, tuple)
    assert isinstance(days, tuple)
    cases = (
        {"candidate_ids": ("A",)},
        {"candidate_ids": ("A", "A")},
        {"candidate_ids": ("A", " ")},
        {"candidate_ids": tuple(f"C{i}" for i in range(65))},
        {"period_end_dates": days[:-1]},
        {"period_end_dates": (days[0], days[0], *days[2:])},
        {"period_end_dates": (days[1], days[0], *days[2:])},
        {"returns_by_observation": rows[:-1]},
        {"returns_by_observation": ((0.1,), *rows[1:])},
        {"returns_by_observation": ((0.1, 0.2, 0.3), *rows[1:])},
        {"returns_by_observation": ((True, 0.1), *rows[1:])},
        {"returns_by_observation": ((float("nan"), 0.1), *rows[1:])},
        {"returns_by_observation": ((float("inf"), 0.1), *rows[1:])},
        {"returns_by_observation": (("0.1", 0.1), *rows[1:])},
        {"slice_count": 3},
        {"slice_count": 8},
        {"slice_count": True},
        {"slice_count": "4"},
        {"period_end_dates": days[:58], "returns_by_observation": rows[:58]},
        {
            "period_end_dates": tuple(date(2020, 1, 1) + timedelta(days=i) for i in range(4097)),
            "returns_by_observation": rows * 68 + rows[:17],
        },
    )
    for overrides in cases:
        with pytest.raises(ValidationError):
            CSCVInput(**(valid | overrides))


def test_undefined_sharpe_or_tied_is_oos_ranks_rejects_the_whole_result() -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    valid = _hand_case()
    rows = valid["returns_by_observation"]
    assert isinstance(rows, tuple)
    cases = (
        tuple((first, 0.005) for first, _second in rows),
        tuple((first, first) for first, _second in rows),
        tuple(
            (first, second if index < 30 else first) for index, (first, second) in enumerate(rows)
        ),
    )
    for candidate_rows in cases:
        with pytest.raises(ValueError):
            calculate_cscv_pbo(CSCVInput(**(valid | {"returns_by_observation": candidate_rows})))


def test_finite_returns_with_unrepresentable_sample_variance_are_rejected() -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    valid = _hand_case()
    rows = valid["returns_by_observation"]
    assert isinstance(rows, tuple)
    huge_rows = tuple(
        (1e308 if index % 2 == 0 else -1e308, second) for index, (_, second) in enumerate(rows)
    )
    with pytest.raises(ValueError, match="finite numeric range"):
        calculate_cscv_pbo(CSCVInput(**(valid | {"returns_by_observation": huge_rows})))


def test_column_permutation_preserves_candidate_identity_and_result_is_deeply_immutable() -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    valid = _hand_case()
    rows = valid["returns_by_observation"]
    assert isinstance(rows, tuple)
    first_input = CSCVInput(**valid)
    first = calculate_cscv_pbo(first_input)
    permuted = calculate_cscv_pbo(
        CSCVInput(
            **(
                valid
                | {
                    "candidate_ids": ("B", "A"),
                    "returns_by_observation": tuple((second, first) for first, second in rows),
                }
            )
        )
    )

    assert first == calculate_cscv_pbo(first_input)
    assert first.probability_of_backtest_overfitting == (
        permuted.probability_of_backtest_overfitting
    )
    assert [split.selected_candidate_id for split in first.splits] == [
        split.selected_candidate_id for split in permuted.splits
    ]
    assert [split.out_of_sample_rank for split in first.splits] == [
        split.out_of_sample_rank for split in permuted.splits
    ]
    with pytest.raises(ValidationError):
        first_input.slice_count = 6
    with pytest.raises(ValidationError):
        first.probability_of_backtest_overfitting = 0.0
    with pytest.raises(ValidationError):
        first.splits[0].out_of_sample_rank = 2
    with pytest.raises(TypeError):
        first_input.returns_by_observation[0][0] = 1.0


@pytest.mark.parametrize(
    ("slice_count", "observations"),
    ((4, 60), (6, 60), (8, 64), (10, 60)),
)
def test_supported_slice_counts_enumerate_every_balanced_split_exactly_once(
    slice_count: int, observations: int
) -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    random = Random(1024 + slice_count)
    rows = tuple(tuple(random.gauss(0.0, 0.01) for _ in range(3)) for _ in range(observations))
    result = calculate_cscv_pbo(
        CSCVInput(
            candidate_ids=("A", "B", "C"),
            period_end_dates=tuple(
                date(2026, 1, 1) + timedelta(days=i) for i in range(observations)
            ),
            returns_by_observation=rows,
            slice_count=slice_count,
        )
    )

    expected_count = comb(slice_count, slice_count // 2)
    assert result.split_count == expected_count
    assert len({split.in_sample_slices for split in result.splits}) == expected_count
    for split in result.splits:
        assert len(split.in_sample_slices) == slice_count // 2
        assert len(split.out_of_sample_slices) == slice_count // 2
        assert set(split.in_sample_slices).isdisjoint(split.out_of_sample_slices)
        assert sorted((*split.in_sample_slices, *split.out_of_sample_slices)) == list(
            range(slice_count)
        )


def test_calculation_revalidates_an_input_copy_with_bypassed_model_validation() -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    valid = CSCVInput(**_hand_case())
    malformed = valid.model_copy(update={"returns_by_observation": ((0.01,),)})

    with pytest.raises(ValidationError):
        calculate_cscv_pbo(malformed)


def test_fixed_seed_noise_family_is_near_half_and_stable_winner_is_lower() -> None:
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo

    dates = tuple(date(2026, 1, 1) + timedelta(days=i) for i in range(60))
    noise_counts = []
    stable_counts = []
    for seed in range(20):
        random = Random(20260929 + seed)
        rows = tuple(tuple(random.gauss(0.0, 0.01) for _ in range(5)) for _ in dates)
        noise = calculate_cscv_pbo(
            CSCVInput(
                candidate_ids=("A", "B", "C", "D", "E"),
                period_end_dates=dates,
                returns_by_observation=rows,
                slice_count=4,
            )
        )
        stable_rows = tuple((first + 0.06, *others) for first, *others in rows)
        stable = calculate_cscv_pbo(
            CSCVInput(
                candidate_ids=("A", "B", "C", "D", "E"),
                period_end_dates=dates,
                returns_by_observation=stable_rows,
                slice_count=4,
            )
        )
        noise_counts.append(noise.below_median_count)
        stable_counts.append(stable.below_median_count)

    noise_probability = sum(noise_counts) / (20 * 6)
    stable_probability = sum(stable_counts) / (20 * 6)
    assert 0.3 <= noise_probability <= 0.7
    assert stable_probability < noise_probability
