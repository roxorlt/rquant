"""价格基准调整工具测试。"""

from __future__ import annotations

import pytest


def test_resolve_price_basis_adjustment_detects_discontinuity() -> None:
    from rquant.price_adjustment import resolve_price_basis_adjustment

    adjustment = resolve_price_basis_adjustment(
        previous_close=150.80,
        current_pre_close=115.70,
    )

    assert adjustment.adjusted is True
    assert adjustment.ratio == pytest.approx(115.70 / 150.80)
    assert adjustment.adjust(150.80) == pytest.approx(115.70)


def test_resolve_price_basis_adjustment_ignores_small_rounding_noise() -> None:
    from rquant.price_adjustment import resolve_price_basis_adjustment

    adjustment = resolve_price_basis_adjustment(
        previous_close=10.00,
        current_pre_close=10.01,
        threshold_pct=0.02,
    )

    assert adjustment.adjusted is False
    assert adjustment.ratio == 1.0
    assert adjustment.adjust(9.50) == 9.50
