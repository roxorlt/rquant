from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.alert_price_rule import MarketDayEvidence, ObservedPriceQuote
from rquant.serving_contracts import ServingCurrentPointer
from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
from rquant.serving_price_alert_evaluation import (
    PriceAlertBatch,
    PriceAlertEvaluationInputs,
    PriceQuoteEvidence,
    QuoteProvenance,
    evaluate_price_alert_batch,
    read_price_alert_evaluation_inputs,
)
from rquant.serving_price_alert_rule_read import read_price_alert_rules
from rquant.serving_publisher import ServingReader
from tests.unit.test_web_price_alert_rules import _head, _member, _publish

SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 25, 10, 0, tzinfo=SHANGHAI)
CODE = "600001.SH"


def _inputs(root: Path, *, at: datetime = NOW) -> PriceAlertEvaluationInputs:
    return read_price_alert_evaluation_inputs(
        root, evaluated_at=at, max_generation_age=timedelta(days=1)
    )


def _quote(
    *,
    at: datetime = NOW - timedelta(seconds=1),
    provenance: QuoteProvenance = "provider_source_timestamp",
    previous: datetime | None = None,
) -> PriceQuoteEvidence:
    return PriceQuoteEvidence(
        quote=ObservedPriceQuote(
            ts_code=CODE, price=Decimal("10.50"), observed_at=at, trade_date=date(2026, 9, 25)
        ),
        source_timestamp_provenance=provenance,
        previous_source_observed_at=previous,
    )


def _evaluate(
    inputs: PriceAlertEvaluationInputs, *, quotes: tuple[PriceQuoteEvidence, ...] = ()
) -> PriceAlertBatch:
    return evaluate_price_alert_batch(
        inputs,
        market=MarketDayEvidence(trade_date=date(2026, 9, 25), is_trading_day=True),
        quotes=quotes,
        max_quote_age_seconds=60,
    )


def test_same_generation_read_and_owner_scope_keep_versions(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(
            _head("alice", "shared-rule", CODE, 2, version=3),
            _head("bob", "shared-rule", CODE, 1, version=7),
        ),
        members=(_member("alice", CODE, 2), _member("bob", CODE, 1)),
    )
    inputs = _inputs(root)
    assert inputs.availability == "ready"
    assert len(inputs.rules) == len(inputs.members) == 2
    batch = _evaluate(inputs, quotes=(_quote(),))
    assert batch.availability == "ready"
    assert batch.delivery_eligible is False
    assert [
        (item.owner_id, item.rule_version, item.membership_version, item.state)
        for item in batch.results
    ] == [
        ("alice", 3, 2, "triggered"),
        ("bob", 7, 1, "triggered"),
    ]
    assert all(item.source_generation_id == inputs.source_generation_id for item in batch.results)


def test_trusted_zero_differs_from_unavailable_and_stale(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    ready = _inputs(root)
    assert ready.availability == "ready"
    assert ready.rules == ()
    assert _evaluate(ready).results == ()
    assert _inputs(root, at=NOW + timedelta(days=2)).availability == "unavailable"

    other = tmp_path / "other"
    _publish(other, activated=False, member_activated=False)
    assert _inputs(other).availability == "not_ready"
    missing = tmp_path / "missing"
    assert _inputs(missing).availability == "unavailable"


def test_pointer_change_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "serving"
    _publish(root, rules=(_head("alice", "rule-a", CODE, 1),), members=(_member("alice", CODE, 1),))
    original = ServingReader.current_pointer
    checks = 0

    def switched(self: ServingReader) -> ServingCurrentPointer:
        nonlocal checks
        checks += 1
        pointer = original(self)
        if checks > 1:
            return pointer.model_copy(update={"generation_id": "f" * 64})
        return pointer

    monkeypatch.setattr(ServingReader, "current_pointer", switched)
    assert _inputs(root).availability == "unavailable"


def test_source_generation_and_digest_mismatches_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rules=(_head("alice", "rule-a", CODE, 1),), members=(_member("alice", CODE, 1),))

    class CorruptRuleStateCursor:
        def __init__(self, actual: object) -> None:
            self.actual = actual
            self.corrupt = False

        def execute(self, query: str, parameters: object = None) -> CorruptRuleStateCursor:
            self.corrupt = "FROM price_alert_rule_state " in query
            if parameters is None:
                self.actual.execute(query)
            else:
                self.actual.execute(query, parameters)
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            rows = self.actual.fetchall()
            return [(*row[:4], "0" * 64) for row in rows] if self.corrupt else rows

    with ServingReader(root).acquire_generation() as lease:
        cursor = lease.connection.cursor()
        try:
            wrong_source = lease.manifest.model_copy(
                update={"source_generations": {"signals": "f" * 64}}
            )
            with pytest.raises(ValueError, match="source generation"):
                read_price_alert_rules(wrong_source, cursor, now=NOW)
            with pytest.raises(ValueError, match="digest mismatch"):
                read_price_alert_rules(lease.manifest, CorruptRuleStateCursor(cursor), now=NOW)
        finally:
            cursor.close()


def test_ready_inputs_require_explicit_generation_and_availability_evidence() -> None:
    with pytest.raises(ValidationError):
        PriceAlertEvaluationInputs(
            availability="ready",
            generation_id=None,
            source_generation_id=None,
            evaluated_at=NOW,
        )


def test_batch_cannot_override_read_time_to_reuse_expired_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rules=(_head("alice", "rule-a", CODE, 1),), members=(_member("alice", CODE, 1),))
    inputs = _inputs(root)
    later = NOW + timedelta(days=7)
    assert _inputs(root, at=later).availability == "unavailable"
    later_quote = PriceQuoteEvidence(
        quote=ObservedPriceQuote(
            ts_code=CODE,
            price=Decimal("10.50"),
            observed_at=later - timedelta(seconds=1),
            trade_date=later.date(),
        ),
        source_timestamp_provenance="provider_source_timestamp",
    )
    with pytest.raises(TypeError, match="evaluated_at"):
        evaluate_price_alert_batch(
            inputs,
            market=MarketDayEvidence(trade_date=later.date(), is_trading_day=True),
            quotes=(later_quote,),
            max_quote_age_seconds=60,
            evaluated_at=later,
        )


@pytest.mark.parametrize(
    ("quote", "expected_state", "expected_reason"),
    [
        (None, "unavailable", "quote_missing"),
        (
            _quote(provenance="response_received_at_fallback"),
            "unavailable",
            "quote_source_untrusted",
        ),
        (_quote(previous=NOW), "unavailable", "quote_source_time_regressed"),
        (_quote(at=NOW - timedelta(minutes=2)), "unavailable", "stale_quote"),
        (_quote(), "triggered", "threshold_reached"),
    ],
)
def test_quote_evidence_is_required_before_a_rule_can_trigger(
    tmp_path: Path, quote: PriceQuoteEvidence | None, expected_state: str, expected_reason: str
) -> None:
    root = tmp_path / "serving"
    _publish(root, rules=(_head("alice", "rule-a", CODE, 1),), members=(_member("alice", CODE, 1),))
    batch = _evaluate(_inputs(root), quotes=() if quote is None else (quote,))
    assert (batch.results[0].state, batch.results[0].reason) == (
        expected_state,
        expected_reason,
    )


@pytest.mark.parametrize(
    ("member", "expected_reason"),
    [
        (None, "member_missing"),
        (_member("alice", CODE, 2), "membership_changed"),
        (_member("alice", CODE, 1, expiry=NOW - timedelta(seconds=1)), "member_expired"),
    ],
)
def test_member_binding_fails_closed(
    tmp_path: Path, member: ManualWatchlistProjectionRow | None, expected_reason: str
) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(_head("alice", "rule-a", CODE, 1),),
        members=() if member is None else (member,),
    )
    batch = _evaluate(_inputs(root), quotes=(_quote(),))
    assert (batch.results[0].state, batch.results[0].reason) == ("not_triggered", expected_reason)


@pytest.mark.parametrize(
    ("at", "market", "expected_state", "expected_reason"),
    [
        (
            datetime(2026, 9, 25, 12, 0, tzinfo=SHANGHAI),
            True,
            "not_triggered",
            "outside_continuous_session",
        ),
        (NOW, False, "not_triggered", "market_closed"),
    ],
)
def test_session_and_market_evidence_keep_pure_rule_reasons(
    tmp_path: Path, at: datetime, market: bool, expected_state: str, expected_reason: str
) -> None:
    root = tmp_path / "serving"
    _publish(root, rules=(_head("alice", "rule-a", CODE, 1),), members=(_member("alice", CODE, 1),))
    batch = evaluate_price_alert_batch(
        _inputs(root, at=at),
        market=MarketDayEvidence(trade_date=date(2026, 9, 25), is_trading_day=market),
        quotes=(_quote(),),
        max_quote_age_seconds=60,
    )
    assert (batch.results[0].state, batch.results[0].reason) == (expected_state, expected_reason)


def test_deleted_and_disabled_rules_never_trigger(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(
            _head("alice", "deleted", None, None, deleted=True),
            _head("alice", "disabled", CODE, 1, enabled=False),
        ),
        members=(_member("alice", CODE, 1),),
    )
    batch = _evaluate(_inputs(root), quotes=(_quote(),))
    assert [(item.rule_id, item.state, item.reason) for item in batch.results] == [
        ("disabled", "not_triggered", "disabled")
    ]
