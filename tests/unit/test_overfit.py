"""PSR and minimum track record length use single-period Sharpe inputs."""

from __future__ import annotations

from math import sqrt
from typing import TYPE_CHECKING
from unittest import TestCase, main

from pydantic import ValidationError

if TYPE_CHECKING:
    from rquant.overfit import SinglePeriodSharpeInput


class OverfitCoreTests(TestCase):
    def _inputs(self, **overrides: object) -> SinglePeriodSharpeInput:
        from rquant.overfit import SinglePeriodSharpeInput

        values: dict[str, object] = {
            "observed_sharpe_per_period": 0.2,
            "benchmark_sharpe_per_period": 0.1,
            "skewness": 0.0,
            "pearson_kurtosis": 3.0,
            "independent_observations": 100,
        }
        values.update(overrides)
        return SinglePeriodSharpeInput(**values)

    def test_paper_monthly_example_requires_sixty_observations_at_95_percent(self) -> None:
        from rquant.overfit import (
            minimum_track_record_length_per_period,
            probabilistic_sharpe_ratio_per_period,
        )

        inputs = self._inputs(
            observed_sharpe_per_period=2 / sqrt(12),
            benchmark_sharpe_per_period=1 / sqrt(12),
            skewness=-0.72,
            pearson_kurtosis=5.78,
            independent_observations=60,
        )

        required = minimum_track_record_length_per_period(inputs, confidence=0.95)
        probability = probabilistic_sharpe_ratio_per_period(inputs)

        self.assertEqual(required.status, "reachable")
        self.assertAlmostEqual(required.estimated_observations, 59.8950986865, places=8)
        self.assertEqual(required.minimum_observations, 60)
        self.assertEqual(required.inputs, inputs)
        self.assertEqual(probability.inputs, inputs)
        self.assertAlmostEqual(probability.estimation_variance_factor, 1.81402552715, places=10)
        self.assertGreaterEqual(probability.probability, 0.95)
        self.assertLessEqual(probability.probability, 1.0)

    def test_normal_reference_has_known_probability_and_required_length(self) -> None:
        from rquant.overfit import (
            minimum_track_record_length_per_period,
            probabilistic_sharpe_ratio_per_period,
        )

        inputs = self._inputs(
            observed_sharpe_per_period=0.0,
            benchmark_sharpe_per_period=-0.1,
            skewness=0.0,
            pearson_kurtosis=3.0,
            independent_observations=101,
        )

        probability = probabilistic_sharpe_ratio_per_period(inputs)
        required = minimum_track_record_length_per_period(inputs, confidence=0.8413447460685429)

        self.assertEqual(probability.estimation_variance_factor, 1.0)
        self.assertAlmostEqual(probability.probability, 0.8413447460685429, places=14)
        self.assertAlmostEqual(required.estimated_observations, 101.0, places=10)
        self.assertEqual(required.minimum_observations, 101)

    def test_equal_threshold_has_half_probability_and_unreachable_length(self) -> None:
        from rquant.overfit import (
            minimum_track_record_length_per_period,
            probabilistic_sharpe_ratio_per_period,
        )

        inputs = self._inputs(benchmark_sharpe_per_period=0.2)

        self.assertEqual(probabilistic_sharpe_ratio_per_period(inputs).probability, 0.5)
        required = minimum_track_record_length_per_period(inputs, confidence=0.95)
        self.assertEqual(required.status, "unreachable")
        self.assertIsNone(required.estimated_observations)
        self.assertIsNone(required.minimum_observations)

    def test_below_threshold_is_unreachable_even_for_long_observation(self) -> None:
        from rquant.overfit import minimum_track_record_length_per_period

        inputs = self._inputs(
            observed_sharpe_per_period=-0.1,
            benchmark_sharpe_per_period=0.2,
            independent_observations=100_000,
        )

        required = minimum_track_record_length_per_period(inputs, confidence=0.9)
        self.assertEqual(required.status, "unreachable")
        self.assertIsNone(required.estimated_observations)
        self.assertIsNone(required.minimum_observations)

    def test_formula_below_thirty_retains_estimate_but_reports_thirty(self) -> None:
        from rquant.overfit import minimum_track_record_length_per_period

        inputs = self._inputs(
            observed_sharpe_per_period=1.0,
            benchmark_sharpe_per_period=0.0,
        )

        required = minimum_track_record_length_per_period(inputs, confidence=0.8413447460685429)
        self.assertAlmostEqual(required.estimated_observations, 2.5, places=12)
        self.assertEqual(required.minimum_observations, 30)

    def test_nonfinite_boolean_and_too_few_observations_are_rejected(self) -> None:
        invalid_cases = (
            {"observed_sharpe_per_period": float("nan")},
            {"benchmark_sharpe_per_period": float("inf")},
            {"skewness": float("-inf")},
            {"pearson_kurtosis": float("nan")},
            {"observed_sharpe_per_period": True},
            {"skewness": False},
            {"independent_observations": True},
            {"independent_observations": 29},
        )
        for values in invalid_cases:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self._inputs(**values)

    def test_impossible_moments_and_nonpositive_variance_are_rejected(self) -> None:
        invalid_cases = (
            {"skewness": 0.0, "pearson_kurtosis": 0.9},
            {"skewness": 2.0, "pearson_kurtosis": 3.0},
            {
                "observed_sharpe_per_period": 1.0,
                "skewness": 2.0,
                "pearson_kurtosis": 5.0,
            },
            {
                "observed_sharpe_per_period": 1e308,
                "pearson_kurtosis": 3.0,
            },
        )
        for values in invalid_cases:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self._inputs(**values)

    def test_confidence_must_be_finite_strict_and_between_half_and_one(self) -> None:
        from rquant.overfit import minimum_track_record_length_per_period

        inputs = self._inputs()
        for confidence in (
            False,
            0.5,
            1.0,
            -0.1,
            float("nan"),
            float("inf"),
            "0.95",
        ):
            with self.subTest(confidence=confidence), self.assertRaises(ValueError):
                minimum_track_record_length_per_period(inputs, confidence=confidence)

    def test_result_and_inputs_are_immutable(self) -> None:
        from rquant.overfit import probabilistic_sharpe_ratio_per_period

        inputs = self._inputs()
        result = probabilistic_sharpe_ratio_per_period(inputs)

        with self.assertRaises(ValidationError):
            inputs.independent_observations = 200
        with self.assertRaises(ValidationError):
            result.probability = 1.0

    def test_input_contract_names_single_period_not_annualized_sharpe(self) -> None:
        from rquant.overfit import SinglePeriodSharpeInput

        self.assertIn("observed_sharpe_per_period", SinglePeriodSharpeInput.model_fields)
        self.assertIn("benchmark_sharpe_per_period", SinglePeriodSharpeInput.model_fields)
        self.assertIn("annualized", (SinglePeriodSharpeInput.__doc__ or "").lower())


if __name__ == "__main__":
    main()
