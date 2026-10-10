from datetime import UTC, datetime, timedelta
from decimal import localcontext
from hashlib import sha256

import pytest

from rquant.price_alert_runtime_contracts import (
    PriceAlertEventEnvelope,
    PriceAlertFrequencyPolicy,
    decimal_text,
    parse_price_alert_event,
)
from rquant.strict_json import canonical_json_bytes

AT = datetime(2026, 10, 5, 2, 0, tzinfo=UTC)


def event(**changes: object) -> PriceAlertEventEnvelope:
    facts = dict(
        owner_id="alice",
        rule_id="r1",
        rule_version=1,
        membership_version=1,
        ts_code="600000.SH",
        trade_date=AT.date(),
        rule_body_sha256="1" * 64,
        member_binding_sha256="2" * 64,
        frequency_policy_sha256="3" * 64,
        comparison="gte",
        threshold="10.000000000000000000001",
        price="10.000000000000000000002",
        rule_name="价格提醒",
        priority="P2",
        scope_generation_id="4" * 64,
        scope_manifest_sha256="5" * 64,
        calendar_content_sha256="6" * 64,
        quote_source_generation_id="7" * 64,
        quote_batch_id="8" * 64,
        quote_sequence=1,
        quote_revision=1,
        quote_payload_sha256="9" * 64,
        quote_request_binding_sha256="a" * 64,
        quote_observed_at=AT,
        source_timestamp_provenance="provider_source_timestamp",
        quote_available_at=AT,
        evaluated_at=AT,
        available_at=AT,
        expires_at=AT + timedelta(seconds=120),
        producer_manifest_sha256="b" * 64,
        producer_commit="c" * 40,
        source_epoch="d" * 64,
    )
    facts.update(changes)
    return PriceAlertEventEnvelope.create(**facts)


def test_identity_has_exact_named_canonical_preimage() -> None:
    item = event()
    observation = {
        "quote_source_generation_id": item.quote_source_generation_id,
        "ts_code": item.ts_code,
        "trade_date": "2026-10-05",
        "quote_observed_at": "2026-10-05T02:00:00.000000Z",
        "price": item.price,
        "source_timestamp_provenance": item.source_timestamp_provenance,
    }
    expected = {
        "envelope_schema": "rquant.price-alert-event/v1",
        "owner_id": "alice",
        "rule_id": "r1",
        "rule_version": 1,
        "membership_version": 1,
        "ts_code": "600000.SH",
        "trade_date": "2026-10-05",
        "frequency_policy_sha256": "3" * 64,
        "rule_body_sha256": "1" * 64,
        "observation_key": sha256(canonical_json_bytes(observation)).hexdigest(),
    }
    assert item.event_id == sha256(canonical_json_bytes(expected)).hexdigest()
    later = event(
        evaluated_at=AT + timedelta(seconds=1),
        available_at=AT + timedelta(seconds=1),
        scope_generation_id="e" * 64,
        quote_sequence=17,
    )
    assert later.event_id == item.event_id
    assert later.wire_bytes() != item.wire_bytes()


def test_precise_decimal_ignores_context_and_keeps_tail_zeros() -> None:
    with localcontext() as context:
        context.prec = 4
        assert event().price == "10.000000000000000000002"
        assert decimal_text("1.2300") == "1.2300"


@pytest.mark.parametrize(
    "changes",
    [
        {"producer_manifest_sha256": "0" * 64},
        {"price": "NaN"},
        {"price": "1e1000000"},
        {"price": "0"},
        {"quote_sequence": True},
        {"rule_version": 1.0},
        {"quote_observed_at": AT + timedelta(seconds=1)},
        {"available_at": AT - timedelta(seconds=1)},
        {"expires_at": AT + timedelta(seconds=121)},
        {"expires_at": AT},
    ],
)
def test_event_rejects_invalid_facts(changes: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        event(**changes)


def test_exact_wire_and_family_reject_duplicate_extra_id_and_subclass() -> None:
    item = event()
    assert parse_price_alert_event(item.wire_bytes()) == item
    body = item.model_dump(mode="json")
    for changed in ({**body, "event_id": "a" * 64}, {**body, "strategy_id": "fake"}):
        with pytest.raises((TypeError, ValueError)):
            parse_price_alert_event(canonical_json_bytes(changed))
    with pytest.raises(ValueError):
        parse_price_alert_event(
            item.wire_bytes().replace(b'"owner_id":', b'"owner_id":"bob","owner_id":')
        )

    class Substitute(PriceAlertEventEnvelope):
        pass

    with pytest.raises((TypeError, ValueError)):
        parse_price_alert_event(Substitute.model_validate_json(item.wire_bytes()))


@pytest.mark.parametrize("seconds", [True, 59, 3601, 300.0])
def test_cooldown_policy_strict_bounds(seconds: object) -> None:
    with pytest.raises(ValueError):
        PriceAlertFrequencyPolicy(cooldown_seconds=seconds)


def test_default_cooldown_has_frozen_identity() -> None:
    policy = PriceAlertFrequencyPolicy()
    assert policy.cooldown_seconds == 300
    assert (
        policy.sha256
        == sha256(
            canonical_json_bytes(
                {
                    "policy_schema": "price-alert-frequency/v1",
                    "mode": "per_rule_cooldown",
                    "cooldown_seconds": 300,
                }
            )
        ).hexdigest()
    )
