"""Reject small representations that could expand in the original arithmetic."""

from __future__ import annotations

from decimal import Decimal

import pytest

from rquant.strategy_authoring_commands import SaveStrategyTemplate
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_template_run import FrozenStrategyTemplateInput
from tests.unit.test_strategy_authoring import draft
from tests.unit.test_strategy_template import template_payload
from tests.unit.test_strategy_template_run import frozen


class UnsafeArithmeticReached(RuntimeError):
    pass


def test_save_rejects_expanding_money_before_original_fraction(monkeypatch) -> None:
    import rquant.portfolio.weights as weights

    original = weights.Fraction

    def intercept(value, *args, **kwargs):
        if isinstance(value, Decimal) and abs(value.as_tuple().exponent) > 384:
            raise UnsafeArithmeticReached("huge rational allocation was intercepted")
        return original(value, *args, **kwargs)

    monkeypatch.setattr(weights, "Fraction", intercept)
    payload = draft().model_dump(mode="python")
    payload["rules"]["weight_rule"]["min_target_amount"] = "1e1000000000"
    with pytest.raises(ValueError, match="exponent"):
        SaveStrategyTemplate.model_validate(payload)


@pytest.mark.parametrize("field", ["stop_loss", "take_profit", "trailing_profit"])
def test_exit_rate_representation_is_bounded_before_arithmetic(field: str) -> None:
    with pytest.raises(ValueError, match="exponent"):
        StrategyTemplate.model_validate({**template_payload(), "exit": {field: "1e-1000000000"}})


def test_valid_tail_zero_money_and_rates_keep_original_values() -> None:
    payload = template_payload()
    payload["weight_rule"]["min_target_amount"] = "1.230000"
    payload["exit"] = {"stop_loss": "0.1000000"}
    value = StrategyTemplate.model_validate(payload)
    assert value.weight_rule.min_target_amount == Decimal("1.23")
    assert value.exit.stop_loss == Decimal("0.1")


def test_run_cash_rejected_before_old_cent_underflow(tmp_path) -> None:
    value = frozen(tmp_path)
    payload = value.model_dump(mode="python")
    payload["request"]["initial_cash"] = "1e-1000000000"
    payload["input_hash"] = None
    with pytest.raises(ValueError, match="exponent"):
        FrozenStrategyTemplateInput.model_validate(payload)


@pytest.mark.parametrize("source", ["ranking", "entry_rows", "entry_pool", "entry_signals"])
def test_run_code_budget_includes_unselected_original_facts(tmp_path, source: str) -> None:
    value = frozen(tmp_path)
    payload = value.model_dump(mode="python")
    payload["input_hash"] = None
    codes = tuple(f"{600001 + index}.SH" for index in range(501))
    if source == "ranking":
        payload["request"]["days"][0]["ranking"]["candidates"] = tuple({"ts_code": code, "rank_score": "0"} for code in codes)
    else:
        raw = payload["days"][0]["entry"]["evidence"]
        if source == "entry_rows":
            raw["rows"] = tuple({"ts_code": code, "is_st": True} for code in codes)
        elif source == "entry_pool":
            raw["pool_codes"] = codes
        else:
            raw["signals"] = tuple({"strategy_id": "n_shape", "version": 1, "action": "watch", "ts_code": code, "observed_at": raw["observed_at"], "source_hash": raw["source_hash"]} for code in codes)
    with pytest.raises(ValueError, match="code.*budget"):
        FrozenStrategyTemplateInput.model_validate(payload)
