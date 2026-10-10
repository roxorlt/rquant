"""Frozen day-major sampler matches an independent exact rational reference."""

from decimal import Decimal
from fractions import Fraction
import json
from pathlib import Path

import pytest


def test_fixed_seed_full_hand_distribution_matches_independent_reference() -> None:
    from rquant.paper_portfolio_band import bootstrap_daily_band

    reference = json.loads((Path(__file__).resolve().parents[2] / "data/verification/paper-portfolio-completion-20261005/bootstrap-independent-reference.json").read_text())
    result = bootstrap_daily_band((Decimal("-.01"), Decimal("0"), Decimal(".02")), days=8)
    assert len(result) == 8
    for actual, expected in zip(result, reference["days"], strict=True):
        for observed, key in ((actual.lower, "lower_fraction"), (actual.upper, "upper_fraction")):
            exact = Fraction(expected[key])
            assert abs(Fraction(observed) - exact) <= Fraction(1, 10**28)
    assert bootstrap_daily_band((Decimal("0"),), days=3)[-1].lower == 1
    assert bootstrap_daily_band((Decimal("0"),), days=3)[-1].upper == 1


@pytest.mark.parametrize("returns,days", [(("NaN",), 1), (("Infinity",), 1), (("-1.0001",), 1), (("1e1000000000",), 1), (("0",), 2521), ((), 1)])
def test_bad_distribution_or_day_budget_refused_before_sampling(returns, days, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.paper_portfolio_band as product

    monkeypatch.setattr(product, "_draw", lambda *_: pytest.fail("invalid input reached PRNG work"))
    with pytest.raises(ValueError):
        product.bootstrap_daily_band(tuple(Decimal(value) for value in returns), days=days)
