from datetime import timedelta
from pathlib import Path

import pytest

from rquant.price_alert_runtime import evaluate_price_alert_round
from rquant.price_alert_runtime_source import (
    PriceAlertScopeSnapshot,
    PriceQuoteFact,
    PriceQuoteSnapshot,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleAuthoritySnapshot,
    PriceAlertRuleProjectionRow,
)
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_runtime_store import store_fixture


def inputs(at=AT, *, enabled=True, membership=1, is_open=True, quote_at=None):
    quote_at = at if quote_at is None else quote_at
    row = PriceAlertRuleProjectionRow(
        owner_id="alice",
        rule_id="r1",
        version=1,
        deleted=False,
        ts_code="600000.SH",
        membership_version=1,
        name="到价提醒",
        priority="P2",
        enabled=enabled,
        comparison="gte",
        threshold="10.000000000000000000001",
        valid_from="09:30:00",
        valid_until="14:57:00",
        updated_at=at - timedelta(seconds=1),
    )
    member = ManualWatchlistProjectionRow(
        owner_id="alice",
        ts_code="600000.SH",
        version=membership,
        deleted=False,
        source="detail",
        price_levels_json="[]",
        expires_at=None,
        updated_at=at - timedelta(seconds=1),
    )
    scope = PriceAlertScopeSnapshot(
        generation_id="4" * 64,
        manifest_sha256="5" * 64,
        source_generation_id="6" * 64,
        source_sequence=1,
        built_at=at,
        available_at=at,
        inspected_at=at,
        rules=(row,),
        members=(member,),
        rule_rows_sha256=PriceAlertRuleAuthoritySnapshot.digest((row,)),
        member_rows_sha256=ManualWatchlistAuthoritySnapshot.digest((member,)),
    )
    quotes = PriceQuoteSnapshot(
        scope_generation_id=scope.generation_id,
        scope_manifest_sha256=scope.manifest_sha256,
        source_generation_id="7" * 64,
        batch_id="8" * 64,
        sequence=0,
        revision=1,
        payload_sha256="9" * 64,
        request_binding_sha256="a" * 64,
        available_at=at,
        inspected_at=at,
        requested_codes=("600000.SH",),
        quotes=(
            PriceQuoteFact(
                ts_code="600000.SH",
                price="10.000000000000000000002",
                observed_at=quote_at,
                trade_date=at.date(),
                source_timestamp_provenance="provider_source_timestamp",
            ),
        ),
    )
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="b" * 40,
        coverage_start=at.date(),
        coverage_end=at.date(),
        open_dates=(at.date(),) if is_open else (),
        generated_at=at - timedelta(days=1),
    )
    return scope, quotes, calendar


def test_original_three_state_precise_compare_and_sealed_event(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    scope, quotes, calendar = inputs()
    value = evaluate_price_alert_round(
        activation=activation,
        scope=scope,
        quotes=quotes,
        calendar=calendar,
        evaluated_at=AT,
        policy=policy,
    )
    assert value.records[0].state == "triggered"
    assert value.records[0].event.price == "10.000000000000000000002"
    assert value.records[0].event.expires_at == AT + timedelta(seconds=120)
    assert len(store.commit_round(value, policy=policy, current_scope=lambda: True).events) == 1
    missing = evaluate_price_alert_round(
        activation=activation,
        scope=scope,
        quotes=None,
        calendar=calendar,
        evaluated_at=AT,
        policy=policy,
    )
    assert missing.records[0].state == "unavailable"
    assert missing.records[0].reason == "quote_missing"
    store.close()


@pytest.mark.parametrize(
    "hour,minute,second,state,reason",
    [
        (1, 29, 59, "not_triggered", "outside_continuous_session"),
        (1, 30, 0, "triggered", "threshold_reached"),
        (3, 29, 59, "triggered", "threshold_reached"),
        (3, 30, 0, "not_triggered", "outside_continuous_session"),
        (5, 0, 0, "triggered", "threshold_reached"),
        (6, 57, 0, "not_triggered", "outside_continuous_session"),
    ],
)
def test_original_sse_continuous_boundaries_and_expiry_clip(
    tmp_path: Path, hour: int, minute: int, second: int, state: str, reason: str
) -> None:
    store, activation, policy = store_fixture(tmp_path)
    at = AT.replace(hour=hour, minute=minute, second=second)
    scope, quotes, calendar = inputs(at)
    value = evaluate_price_alert_round(
        activation=activation,
        scope=scope,
        quotes=quotes,
        calendar=calendar,
        evaluated_at=at,
        policy=policy,
    )
    assert (value.records[0].state, value.records[0].reason) == (state, reason)
    if hour == 3 and state == "triggered":
        assert value.records[0].event.expires_at == at + timedelta(seconds=1)
    store.close()


@pytest.mark.parametrize(
    "change,reason",
    [
        ("disabled", "disabled"),
        ("new_member", "out_of_scope"),
        ("holiday", "market_closed"),
        ("unknown_day", "market_day_unknown"),
        ("lunch_quote", "quote_outside_continuous_session"),
    ],
)
def test_actual_scope_and_calendar_no_implicit_open(
    tmp_path: Path, change: str, reason: str
) -> None:
    store, activation, policy = store_fixture(tmp_path)
    at = AT.replace(hour=5) if change == "lunch_quote" else AT
    scope, quotes, calendar = inputs(
        at,
        enabled=change != "disabled",
        membership=2 if change == "new_member" else 1,
        is_open=change != "holiday",
        quote_at=at - timedelta(seconds=1) if change == "lunch_quote" else at,
    )
    if change == "unknown_day":
        calendar = None
    if change in {"new_member", "disabled"}:
        quotes = None
    value = evaluate_price_alert_round(
        activation=activation,
        scope=scope,
        quotes=quotes,
        calendar=calendar,
        evaluated_at=at,
        policy=policy,
    )
    assert value.records[0].reason == reason
    assert value.records[0].event is None
    store.close()
