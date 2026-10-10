from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.alert_price_rule import (
    MarketDayEvidence,
    ObservedPriceQuote,
    PriceAlertEvaluationContext,
    PriceAlertRule,
    PriceRuleDecision,
    ScopeMembershipEvidence,
    evaluate_price_rule,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 28)
CODE = "600000.SH"
OTHER = "000001.SZ"


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 28, hour, minute, second, tzinfo=SHANGHAI)


def _rule(**changes: object) -> PriceAlertRule:
    values: dict[str, object] = {
        "rule_id": "price-rule-1",
        "name": "价格到 10 元",
        "priority": "P2",
        "enabled": True,
        "comparison": "gte",
        "threshold": Decimal("10.00"),
        "valid_from": time(9, 30),
        "valid_until": time(14, 57),
    }
    return PriceAlertRule.model_validate({**values, **changes})


def _quote(**changes: object) -> ObservedPriceQuote:
    values: dict[str, object] = {
        "ts_code": CODE,
        "price": Decimal("10.00"),
        "observed_at": _at(10, 0),
        "trade_date": TRADE_DATE,
    }
    return ObservedPriceQuote.model_validate({**values, **changes})


def _context(**changes: object) -> PriceAlertEvaluationContext:
    values: dict[str, object] = {
        "ts_code": CODE,
        "quote": _quote(),
        "evaluated_at": _at(10, 0),
        "market": MarketDayEvidence(trade_date=TRADE_DATE, is_trading_day=True),
        "scope": ScopeMembershipEvidence(member_codes=frozenset({CODE})),
        "max_quote_age_seconds": 60,
    }
    return PriceAlertEvaluationContext.model_validate({**values, **changes})


@pytest.mark.parametrize(
    ("comparison", "price", "expected"),
    [
        ("gte", "10.00", "triggered"),
        ("gte", "9.99", "not_triggered"),
        ("lte", "10.00", "triggered"),
        ("lte", "10.01", "not_triggered"),
    ],
)
def test_price_boundary_comparisons(comparison: str, price: str, expected: str) -> None:
    result = evaluate_price_rule(
        _rule(comparison=comparison), _context(quote=_quote(price=Decimal(price)))
    )
    assert isinstance(result, PriceRuleDecision)
    assert result.state == expected
    assert result.reason == (
        "threshold_reached" if expected == "triggered" else "threshold_not_reached"
    )
    assert result.rule_id == "price-rule-1"
    assert result.ts_code == CODE


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        (9, 29, "not_triggered"),
        (9, 30, "triggered"),
        (11, 29, "triggered"),
        (11, 30, "not_triggered"),
        (12, 59, "not_triggered"),
        (13, 0, "triggered"),
        (14, 56, "triggered"),
        (14, 57, "not_triggered"),
        (15, 0, "not_triggered"),
    ],
)
def test_only_continuous_auction_half_open_sessions(hour: int, minute: int, expected: str) -> None:
    at = _at(hour, minute)
    result = evaluate_price_rule(_rule(), _context(evaluated_at=at, quote=_quote(observed_at=at)))
    assert result.state == expected
    assert result.reason == (
        "threshold_reached" if expected == "triggered" else "outside_continuous_session"
    )


def test_rule_window_uses_shanghai_wall_clock_even_with_utc_input() -> None:
    rule = _rule(valid_from=time(10, 0), valid_until=time(10, 30))
    at = _at(10, 30).astimezone(ZoneInfo("UTC"))
    result = evaluate_price_rule(rule, _context(evaluated_at=at, quote=_quote(observed_at=at)))
    assert result.state == "not_triggered"
    assert result.reason == "outside_rule_window"


def test_quote_age_accepts_exact_boundary_then_fails_closed() -> None:
    at = _at(10, 1)
    exact = evaluate_price_rule(_rule(), _context(evaluated_at=at))
    late = evaluate_price_rule(_rule(), _context(evaluated_at=at + timedelta(seconds=1)))
    assert (exact.state, exact.reason) == ("triggered", "threshold_reached")
    assert (late.state, late.reason) == ("unavailable", "stale_quote")


def test_fresh_preopen_quote_cannot_trigger_at_continuous_open() -> None:
    result = evaluate_price_rule(
        _rule(),
        _context(
            evaluated_at=_at(9, 30),
            quote=_quote(observed_at=_at(9, 29, 45)),
        ),
    )
    assert (result.state, result.reason) == (
        "unavailable",
        "quote_outside_continuous_session",
    )


def test_morning_quote_cannot_trigger_after_afternoon_open_even_with_long_age_limit() -> None:
    result = evaluate_price_rule(
        _rule(),
        _context(
            evaluated_at=_at(13, 0),
            quote=_quote(observed_at=_at(11, 29)),
            max_quote_age_seconds=7200,
        ),
    )
    assert (result.state, result.reason) == (
        "unavailable",
        "quote_outside_continuous_session",
    )


@pytest.mark.parametrize(
    ("quote_changes", "reason"),
    [
        ({"observed_at": _at(10, 0, 1)}, "future_quote"),
        ({"trade_date": date(2026, 9, 25)}, "wrong_quote_day"),
        ({"ts_code": OTHER}, "wrong_quote_code"),
    ],
)
def test_quote_identity_or_time_mismatch_is_unavailable(
    quote_changes: dict[str, object], reason: str
) -> None:
    result = evaluate_price_rule(_rule(), _context(quote=_quote(**quote_changes)))
    assert (result.state, result.reason) == ("unavailable", reason)


@pytest.mark.parametrize(
    ("context_changes", "state", "reason"),
    [
        ({"quote": None}, "unavailable", "quote_missing"),
        (
            {"market": MarketDayEvidence(trade_date=TRADE_DATE, is_trading_day=None)},
            "unavailable",
            "market_day_unknown",
        ),
        (
            {"market": MarketDayEvidence(trade_date=None, is_trading_day=True)},
            "unavailable",
            "market_day_unknown",
        ),
        (
            {"market": MarketDayEvidence(trade_date=date(2026, 9, 25), is_trading_day=True)},
            "unavailable",
            "wrong_market_day",
        ),
        (
            {"market": MarketDayEvidence(trade_date=date(2026, 9, 25), is_trading_day=False)},
            "unavailable",
            "wrong_market_day",
        ),
        (
            {"market": MarketDayEvidence(trade_date=None, is_trading_day=False)},
            "unavailable",
            "market_day_unknown",
        ),
        (
            {"scope": ScopeMembershipEvidence(member_codes=None)},
            "unavailable",
            "scope_unknown",
        ),
        (
            {"scope": ScopeMembershipEvidence(member_codes=frozenset({OTHER}))},
            "not_triggered",
            "out_of_scope",
        ),
        (
            {"market": MarketDayEvidence(trade_date=TRADE_DATE, is_trading_day=False)},
            "not_triggered",
            "market_closed",
        ),
    ],
)
def test_market_and_scope_evidence_distinguishes_known_absence_from_unknown(
    context_changes: dict[str, object], state: str, reason: str
) -> None:
    result = evaluate_price_rule(_rule(), _context(**context_changes))
    assert (result.state, result.reason) == (state, reason)


def test_disabled_rule_is_not_triggered_even_when_quote_is_missing() -> None:
    result = evaluate_price_rule(_rule(enabled=False), _context(quote=None))
    assert (result.state, result.reason) == ("not_triggered", "disabled")


@pytest.mark.parametrize("invalid", [Decimal("NaN"), Decimal("Infinity"), Decimal("0")])
def test_nonfinite_or_nonpositive_price_is_rejected_at_input_boundary(
    invalid: Decimal,
) -> None:
    with pytest.raises(ValidationError):
        _quote(price=invalid)
    with pytest.raises(ValidationError):
        _rule(threshold=invalid)


def test_invalid_clock_code_and_rule_window_are_rejected() -> None:
    with pytest.raises(ValidationError):
        _quote(observed_at=datetime(2026, 9, 28, 10, 0))
    with pytest.raises(ValidationError):
        _context(evaluated_at=datetime(2026, 9, 28, 10, 0))
    with pytest.raises(ValidationError):
        _context(ts_code="bad")
    with pytest.raises(ValidationError):
        _rule(valid_from=time(10, 30), valid_until=time(10, 30))
    with pytest.raises(ValidationError):
        _rule(valid_from=time(10, tzinfo=SHANGHAI))
    with pytest.raises(ValidationError):
        _context(max_quote_age_seconds=0)
