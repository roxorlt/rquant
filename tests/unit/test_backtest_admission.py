from __future__ import annotations

import json
from decimal import Decimal, localcontext
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

import rquant.portfolio.weights as weights
from rquant.portfolio_backtest_models import FrozenPortfolioInput, PortfolioBacktestConfig
from rquant.web.models.backtests import PortfolioCreateRequest, PortfolioEditableConfig
from tests.unit.test_backtest_platform import config, frozen


def _admit(payload: dict[str, Any], entry: str) -> Any:
    if entry == "domain":
        return PortfolioBacktestConfig.model_validate(payload)
    if entry == "editable":
        return PortfolioEditableConfig.model_validate(payload)
    wire = {
        "command_id": str(UUID(int=1)),
        "requested_at": "2026-10-05T00:00:00Z",
        "config": payload,
    }
    return PortfolioCreateRequest.model_validate_json(json.dumps(wire)).config


@pytest.mark.parametrize("entry", ["domain", "editable", "wire"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("initial_cash", "1e-1000000000"),
        ("initial_cash", "1e1000000000"),
        ("initial_cash", "1." + "0" * 128),
        ("initial_cash", "0.001"),
        ("initial_cash", "NaN"),
        ("initial_cash", "Infinity"),
        ("initial_cash", True),
        ("initial_cash", None),
        ("initial_cash", "invalid"),
        ("min_target_amount", "1e1000000000"),
        ("min_target_amount", "1e-1000000000"),
        ("min_target_amount", "1e10000"),
        ("min_target_amount", "1e385"),
        ("min_target_amount", "1." + "0" * 128),
        ("min_target_amount", "0.001"),
        ("max_stock_weight", "1e-1000000000"),
        ("max_industry_weight", "1e-1000000000"),
        ("cash_reserve", "1e-1000000000"),
    ],
)
def test_pb_final_02_invalid_representation_is_rejected_before_fraction(
    monkeypatch: pytest.MonkeyPatch, entry: str, field: str, value: object
) -> None:
    payload = config().model_dump(mode="json")
    if field == "initial_cash":
        payload[field] = value
    else:
        payload["weight_rule"][field] = value
    calls: list[str] = []

    def blocked_fraction(number: Decimal) -> Any:
        calls.append(str(number))
        raise ValueError("test interception before any rational allocation")

    monkeypatch.setattr(weights, "Fraction", blocked_fraction)
    with pytest.raises(ValidationError):
        _admit(payload, entry)
    assert calls == [], "invalid new-entry value reached the shared Fraction allocator"


def test_pb_final_02_cent_check_does_not_round_in_decimal_context() -> None:
    payload = config().model_dump(mode="json")
    with localcontext() as context:
        context.prec = 1
        with pytest.raises(ValidationError, match="cent"):
            PortfolioBacktestConfig.model_validate(payload | {"initial_cash": "1.001"})
        accepted = PortfolioBacktestConfig.model_validate(payload | {"initial_cash": "3000.00"})
        assert accepted.initial_cash.as_tuple() == Decimal("3000.00").as_tuple()


@pytest.mark.parametrize("cash", ["3000.00", "3000.0000", "0.0100", "1000000000000"])
@pytest.mark.parametrize("entry", ["domain", "editable", "wire"])
def test_pb_final_02_legal_cash_and_weight_trailing_zeros_are_unchanged(
    cash: str, entry: str
) -> None:
    payload = config().model_dump(mode="json")
    payload["initial_cash"] = cash
    payload["weight_rule"]["min_target_amount"] = "0.0000"
    value = _admit(payload, entry)
    assert value.initial_cash.as_tuple() == Decimal(cash).as_tuple()
    assert value.weight_rule.min_target_amount.as_tuple() == Decimal("0.0000").as_tuple()
    assert value.model_dump(mode="json")["initial_cash"] == cash
    assert value.model_dump(mode="json")["weight_rule"]["min_target_amount"] == "0.0000"


def test_pb_final_02_representation_boundary_and_typed_weight_are_checked() -> None:
    payload = config().model_dump(mode="python")
    weight = payload["weight_rule"]
    weight["min_target_amount"] = Decimal("1e384")
    value = PortfolioBacktestConfig.model_validate(payload)
    assert value.weight_rule.min_target_amount == Decimal("1e384")
    payload["initial_cash"] = Decimal("1." + "0" * 127)
    assert PortfolioBacktestConfig.model_validate(payload).initial_cash == Decimal(1)
    payload["weight_rule"] = value.weight_rule.model_copy(
        update={"min_target_amount": Decimal("1e1000000000")}
    )
    with pytest.raises(ValidationError, match="exponent"):
        PortfolioBacktestConfig.model_validate(payload)


def _candidate_input(code_groups: tuple[tuple[str, ...], ...]) -> dict[str, Any]:
    payload = frozen().model_dump(mode="python")
    payload["input_hash"] = None
    payload["sources"]["ranking_hash"] = "4" * 64
    for day, codes in zip(payload["request"]["days"], code_groups, strict=True):
        template = day["ranking"]["candidates"][0]
        day["ranking"]["candidates"] = tuple(template | {"ts_code": code} for code in codes)
    return payload


@pytest.mark.parametrize("count", [500, 501])
def test_pb_final_03_candidate_code_budget_includes_ranking(count: int) -> None:
    codes = ("600000.SH", "000001.SZ", *(f"{700000 + index:06d}.SH" for index in range(count - 2)))
    payload = _candidate_input((codes, ("000001.SZ",)))
    if count == 501:
        with pytest.raises(ValidationError, match="candidate.*budget"):
            FrozenPortfolioInput.model_validate(payload)
    else:
        value = FrozenPortfolioInput.model_validate(payload)
        assert (
            len({item.ts_code for day in value.request.days for item in day.ranking.candidates})
            == 500
        )
        assert len({item.ts_code for day in value.request.days for item in day.instruments}) == 3


@pytest.mark.parametrize("total", [500, 501])
def test_pb_final_03_candidate_budget_counts_union_across_days(total: int) -> None:
    codes = tuple(f"{700000 + index:06d}.SH" for index in range(total))
    payload = _candidate_input((codes[:250], codes[250:]))
    if total == 501:
        with pytest.raises(ValidationError, match="candidate.*budget"):
            FrozenPortfolioInput.model_validate(payload)
    else:
        value = FrozenPortfolioInput.model_validate(payload)
        assert (
            len({item.ts_code for day in value.request.days for item in day.ranking.candidates})
            == 500
        )


def test_pb_final_03_same_candidates_on_two_days_are_counted_once() -> None:
    codes = tuple(f"{700000 + index:06d}.SH" for index in range(500))
    value = FrozenPortfolioInput.model_validate(_candidate_input((codes, codes)))
    assert sum(len(day.ranking.candidates) for day in value.request.days) == 1000
