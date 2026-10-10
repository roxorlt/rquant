"""Deflated Sharpe uses a verified independent-trial count and single-period units."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase, main

from pydantic import ValidationError

if TYPE_CHECKING:
    from rquant.overfit import DeflatedSharpeInput, SinglePeriodSharpeInput


class DeflatedSharpeTests(TestCase):
    def _selected(self, **overrides: object) -> SinglePeriodSharpeInput:
        from rquant.overfit import SinglePeriodSharpeInput

        values: dict[str, object] = {
            "observed_sharpe_per_period": 0.3,
            "benchmark_sharpe_per_period": 0.0,
            "skewness": 0.0,
            "pearson_kurtosis": 3.0,
            "independent_observations": 100,
        }
        values.update(overrides)
        return SinglePeriodSharpeInput(**values)

    def _request(self, **overrides: object) -> DeflatedSharpeInput:
        from rquant.overfit import DeflatedSharpeInput

        values: dict[str, object] = {
            "selected_strategy": self._selected(),
            "independent_trial_count": 10,
            "family_sharpe_std_per_period": 0.08,
        }
        values.update(overrides)
        return DeflatedSharpeInput(**values)

    def test_single_trial_is_zero_threshold_psr_without_mutating_selected_input(self) -> None:
        from rquant.overfit import (
            deflated_sharpe_ratio_per_period,
            probabilistic_sharpe_ratio_per_period,
        )

        request = self._request(independent_trial_count=1)
        original = request.selected_strategy.model_dump()

        result = deflated_sharpe_ratio_per_period(request)

        self.assertEqual(result.inputs, request)
        self.assertEqual(result.expected_max_noise_sharpe_per_period, 0.0)
        self.assertEqual(
            result.probability,
            probabilistic_sharpe_ratio_per_period(request.selected_strategy).probability,
        )
        self.assertEqual(request.selected_strategy.model_dump(), original)

    def test_zero_family_dispersion_is_zero_threshold_for_multiple_trials(self) -> None:
        from rquant.overfit import (
            deflated_sharpe_ratio_per_period,
            probabilistic_sharpe_ratio_per_period,
        )

        request = self._request(independent_trial_count=100, family_sharpe_std_per_period=0.0)
        result = deflated_sharpe_ratio_per_period(request)

        self.assertEqual(result.expected_max_noise_sharpe_per_period, 0.0)
        self.assertEqual(
            result.probability,
            probabilistic_sharpe_ratio_per_period(request.selected_strategy).probability,
        )

    def test_expected_noise_threshold_and_probability_match_fixed_references(self) -> None:
        from rquant.overfit import deflated_sharpe_ratio_per_period

        references = (
            (2, 0.041580427542447514, 0.9940529611172677),
            (10, 0.12596786410765998, 0.9548582769549131),
            (100, 0.2024482314561348, 0.8288166085204746),
        )
        previous_threshold = 0.0
        previous_probability = 1.0
        for count, expected_threshold, expected_probability in references:
            with self.subTest(independent_trial_count=count):
                result = deflated_sharpe_ratio_per_period(
                    self._request(independent_trial_count=count)
                )
                self.assertAlmostEqual(
                    result.expected_max_noise_sharpe_per_period,
                    expected_threshold,
                    places=14,
                )
                self.assertAlmostEqual(result.probability, expected_probability, places=14)
                self.assertGreater(result.expected_max_noise_sharpe_per_period, previous_threshold)
                self.assertLess(result.probability, previous_probability)
                previous_threshold = result.expected_max_noise_sharpe_per_period
                previous_probability = result.probability

    def test_doubling_family_dispersion_doubles_threshold_and_reduces_probability(self) -> None:
        from rquant.overfit import deflated_sharpe_ratio_per_period

        first = deflated_sharpe_ratio_per_period(self._request())
        doubled = deflated_sharpe_ratio_per_period(self._request(family_sharpe_std_per_period=0.16))

        self.assertAlmostEqual(doubled.expected_max_noise_sharpe_per_period, 0.25193572821531995)
        self.assertAlmostEqual(
            doubled.expected_max_noise_sharpe_per_period,
            2 * first.expected_max_noise_sharpe_per_period,
            places=14,
        )
        self.assertAlmostEqual(doubled.probability, 0.6800445103092748, places=14)
        self.assertLess(doubled.probability, first.probability)

    def test_request_rejects_nonzero_psr_threshold_and_invalid_trial_evidence(self) -> None:
        invalid_cases = (
            {"selected_strategy": self._selected(benchmark_sharpe_per_period=0.1)},
            {"independent_trial_count": 0},
            {"independent_trial_count": True},
            {"independent_trial_count": 1.5},
            {"independent_trial_count": "10"},
            {"family_sharpe_std_per_period": -0.01},
            {"family_sharpe_std_per_period": True},
            {"family_sharpe_std_per_period": float("nan")},
            {"family_sharpe_std_per_period": float("inf")},
        )
        for values in invalid_cases:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self._request(**values)

    def test_nonrepresentable_noise_threshold_is_rejected(self) -> None:
        from rquant.overfit import deflated_sharpe_ratio_per_period

        for values in (
            {"independent_trial_count": 100, "family_sharpe_std_per_period": 1e308},
            {"independent_trial_count": 10**20},
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                deflated_sharpe_ratio_per_period(self._request(**values))

    def test_result_is_immutable_and_contract_names_independent_single_period_inputs(self) -> None:
        from rquant.overfit import DeflatedSharpeInput, deflated_sharpe_ratio_per_period

        result = deflated_sharpe_ratio_per_period(self._request())
        self.assertIn("independent_trial_count", DeflatedSharpeInput.model_fields)
        self.assertIn("family_sharpe_std_per_period", DeflatedSharpeInput.model_fields)
        self.assertIn("annualized", (DeflatedSharpeInput.__doc__ or "").lower())
        with self.assertRaises(ValidationError):
            result.probability = 1.0


if __name__ == "__main__":
    main()
