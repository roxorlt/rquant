"""Adapt verified scope and quotes to the existing pure price-rule evaluator."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from zoneinfo import ZoneInfo

from rquant.alert_price_rule import (
    MarketDayEvidence,
    ObservedPriceQuote,
    PriceAlertEvaluationContext,
    ScopeMembershipEvidence,
    evaluate_price_rule,
)
from rquant.price_alert_runtime_contracts import (
    PriceAlertEventEnvelope,
    PriceAlertFrequencyPolicy,
    PriceAlertRuntimeActivation,
    require_price_alert_activation,
)
from rquant.price_alert_runtime_source import (
    PriceAlertScopeSnapshot,
    PriceQuoteSnapshot,
    original_price_rule,
)
from rquant.price_alert_runtime_store import PriceEvaluationRecord, PriceRoundInput
from rquant.runtime_contracts import normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strict_json import canonical_json_bytes

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def evaluate_price_alert_round(
    *,
    activation: PriceAlertRuntimeActivation,
    scope: PriceAlertScopeSnapshot,
    quotes: PriceQuoteSnapshot | None,
    calendar: MarketCalendarAuthority | None,
    evaluated_at: datetime,
    policy: PriceAlertFrequencyPolicy,
) -> PriceRoundInput:
    binding = require_price_alert_activation(activation, "evaluation")
    if type(scope) is not PriceAlertScopeSnapshot or type(policy) is not PriceAlertFrequencyPolicy:
        raise TypeError("price evaluation requires exact verified domain types")
    scope = PriceAlertScopeSnapshot.model_validate(scope)
    policy = PriceAlertFrequencyPolicy.model_validate(policy)
    now = normalize_aware_utc(evaluated_at)
    if scope.inspected_at > now or now - scope.built_at > timedelta(seconds=30):
        raise ValueError("price scope was not visible or is stale")
    if policy.sha256 != binding.frequency_policy_sha256:
        raise ValueError("price evaluation frequency differs from the actual manifest")
    if quotes is not None:
        if type(quotes) is not PriceQuoteSnapshot:
            raise TypeError("price quotes must use the exact verified snapshot type")
        quotes = PriceQuoteSnapshot.model_validate(quotes)
        if (quotes.scope_generation_id, quotes.scope_manifest_sha256, quotes.requested_codes) != (
            scope.generation_id,
            scope.manifest_sha256,
            scope.codes,
        ) or not quotes.available_at <= quotes.inspected_at <= now:
            raise ValueError("price quotes were requested for another rule domain")
        codes = tuple(item.ts_code for item in quotes.quotes)
        if (
            len(codes) > 500
            or codes != tuple(sorted(set(codes)))
            or not set(codes) <= set(scope.codes)
        ):
            raise ValueError("price quotes exceed the exact original domain")
    local = now.astimezone(_SHANGHAI)
    if calendar is not None:
        if type(calendar) is not MarketCalendarAuthority:
            raise TypeError("price calendar must use the actual SSE authority type")
        calendar = MarketCalendarAuthority.model_validate(calendar)
    known_day = (
        calendar is not None
        and calendar.generated_at <= now
        and calendar.coverage_start <= local.date() <= calendar.coverage_end
    )
    market = MarketDayEvidence(
        trade_date=local.date() if known_day else None,
        is_trading_day=(local.date() in calendar.open_dates) if known_day else None,
    )
    members = {(row.owner_id, row.ts_code): row for row in scope.members}
    by_code = {} if quotes is None else {item.ts_code: item for item in quotes.quotes}
    records = []
    for row in scope.rules:
        if row.deleted:
            continue
        rule = original_price_rule(row)
        member = members.get((row.owner_id, row.ts_code))
        active = (
            member is not None
            and not member.deleted
            and member.version == row.membership_version
            and (member.expires_at is None or member.expires_at > now)
        )
        fact = by_code.get(row.ts_code)
        quote = (
            None
            if fact is None
            else ObservedPriceQuote(
                ts_code=fact.ts_code,
                price=Decimal(fact.price),
                observed_at=fact.observed_at,
                trade_date=fact.trade_date,
            )
        )
        decision = evaluate_price_rule(
            rule,
            PriceAlertEvaluationContext(
                ts_code=row.ts_code,
                quote=quote,
                evaluated_at=now,
                market=market,
                scope=ScopeMembershipEvidence(
                    member_codes=frozenset({row.ts_code}) if active else frozenset()
                ),
                max_quote_age_seconds=15,
            ),
        )
        event = None
        if decision.state == "triggered":
            wall = local.time()
            session_end = time(11, 30) if wall < time(11, 30) else time(14, 57)
            expires = min(
                now + timedelta(seconds=120),
                datetime.combine(local.date(), rule.valid_until, tzinfo=_SHANGHAI),
                datetime.combine(local.date(), session_end, tzinfo=_SHANGHAI),
            )
            event = PriceAlertEventEnvelope.create(
                owner_id=row.owner_id,
                rule_id=row.rule_id,
                rule_version=row.version,
                membership_version=row.membership_version,
                ts_code=row.ts_code,
                trade_date=local.date(),
                comparison=row.comparison,
                threshold=row.threshold,
                price=fact.price,
                rule_name=row.name,
                priority=row.priority,
                rule_body_sha256=sha256(
                    canonical_json_bytes(rule.model_dump(mode="json"))
                ).hexdigest(),
                member_binding_sha256=sha256(
                    canonical_json_bytes(member.projection_row())
                ).hexdigest(),
                frequency_policy_sha256=policy.sha256,
                scope_generation_id=scope.generation_id,
                scope_manifest_sha256=scope.manifest_sha256,
                calendar_content_sha256=calendar.content_sha256,
                quote_source_generation_id=quotes.source_generation_id,
                quote_batch_id=quotes.batch_id,
                quote_sequence=quotes.sequence,
                quote_revision=quotes.revision,
                quote_payload_sha256=quotes.payload_sha256,
                quote_request_binding_sha256=quotes.request_binding_sha256,
                quote_observed_at=fact.observed_at,
                source_timestamp_provenance=fact.source_timestamp_provenance,
                quote_available_at=quotes.available_at,
                evaluated_at=now,
                available_at=now,
                expires_at=expires,
                producer_manifest_sha256=binding.producer_manifest_sha256,
                producer_commit=binding.producer_commit,
                source_epoch=binding.source_epoch,
            )
        records.append(
            PriceEvaluationRecord(
                owner_id=row.owner_id,
                rule_id=row.rule_id,
                rule_version=row.version,
                membership_version=row.membership_version,
                ts_code=row.ts_code,
                state=decision.state,
                reason=decision.reason.value,
                event=event,
            )
        )
    return PriceRoundInput(
        evaluated_at=now,
        scope_generation_id=scope.generation_id,
        scope_manifest_sha256=scope.manifest_sha256,
        scope_source_generation_id=scope.source_generation_id,
        scope_source_sequence=scope.source_sequence,
        rule_rows_sha256=scope.rule_rows_sha256,
        member_rows_sha256=scope.member_rows_sha256,
        scope_built_at=scope.built_at,
        quote_available_at=None if quotes is None else quotes.available_at,
        quote_source_generation_id=None if quotes is None else quotes.source_generation_id,
        quote_batch_id=None if quotes is None else quotes.batch_id,
        requested_codes=len(scope.codes),
        valid_quotes=0 if quotes is None else len(quotes.quotes),
        records=tuple(records),
    )
