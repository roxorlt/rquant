"""Read verified price-rule inputs and evaluate them without delivery side effects."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, StrictInt, model_validator

from rquant.alert_price_rule import (
    MarketDayEvidence,
    ObservedPriceQuote,
    PriceAlertEvaluationContext,
    PriceAlertRule,
    PriceRuleDecision,
    PriceRuleReason,
    ScopeMembershipEvidence,
    evaluate_price_rule,
)
from rquant.manual_watchlist import OwnerId, TsCode
from rquant.price_alert_rule_store import RuleId
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
from rquant.serving_price_alert_rule_projection import PriceAlertRuleProjectionRow
from rquant.serving_price_alert_rule_read import read_price_alert_rules
from rquant.serving_publisher import ServingReader

QuoteProvenance = Literal["provider_source_timestamp", "response_received_at_fallback"]
BatchReason = (
    PriceRuleReason
    | Literal[
        "member_missing",
        "member_expired",
        "membership_changed",
        "quote_source_untrusted",
        "quote_source_time_regressed",
        "quote_ambiguous",
    ]
)


class PriceAlertEvaluationInputs(RuntimeContractModel):
    availability: Literal["ready", "not_ready", "unavailable"]
    generation_id: str | None
    source_generation_id: str | None
    evaluated_at: AwareUtcDatetime
    available_at: AwareUtcDatetime | None = None
    rules: tuple[PriceAlertRuleProjectionRow, ...] = ()
    members: tuple[ManualWatchlistProjectionRow, ...] = ()

    @model_validator(mode="after")
    def require_explicit_authority(self) -> Self:
        if self.availability == "ready":
            if not all((self.generation_id, self.source_generation_id, self.available_at)):
                raise ValueError("ready price inputs require generation and source evidence")
        elif self.rules or self.members or self.available_at is not None:
            raise ValueError("unready price inputs cannot carry facts")
        return self


class PriceQuoteEvidence(RuntimeContractModel):
    quote: ObservedPriceQuote
    source_timestamp_provenance: QuoteProvenance
    previous_source_observed_at: AwareUtcDatetime | None = None


class PriceAlertBatchResult(RuntimeContractModel):
    owner_id: OwnerId
    rule_id: RuleId
    rule_version: StrictInt = Field(ge=1)
    ts_code: TsCode
    membership_version: StrictInt = Field(ge=1)
    source_generation_id: str
    state: Literal["triggered", "not_triggered", "unavailable"]
    reason: BatchReason
    quote_source_observed_at: AwareUtcDatetime | None
    quote_timestamp_provenance: QuoteProvenance | None


class PriceAlertBatch(RuntimeContractModel):
    delivery_eligible: Literal[False] = False
    availability: Literal["ready", "not_ready", "unavailable"]
    generation_id: str | None
    source_generation_id: str | None
    evaluated_at: AwareUtcDatetime
    market: MarketDayEvidence
    results: tuple[PriceAlertBatchResult, ...]


def read_price_alert_evaluation_inputs(
    reader_or_root: ServingReader | str | Path,
    *,
    evaluated_at: datetime,
    max_generation_age: timedelta,
) -> PriceAlertEvaluationInputs:
    """Borrow one current generation and fail closed on stale or changing authority."""
    now = normalize_aware_utc(evaluated_at)
    if max_generation_age <= timedelta(0):
        raise ValueError("max_generation_age must be positive")
    generation_id: str | None = None

    def unavailable() -> PriceAlertEvaluationInputs:
        return PriceAlertEvaluationInputs(
            availability="unavailable",
            generation_id=generation_id,
            source_generation_id=None,
            evaluated_at=now,
        )

    try:
        reader = (
            reader_or_root
            if isinstance(reader_or_root, ServingReader)
            else ServingReader(reader_or_root)
        )
        with reader.acquire_generation() as lease:
            generation_id = lease.manifest.generation_id
            if (
                lease.pointer is None
                or lease.pointer.generation_id != generation_id
                or lease.manifest.built_at > now
                or now - lease.manifest.built_at > max_generation_age
            ):
                return unavailable()
            cursor = lease.connection.cursor()
            try:
                read = read_price_alert_rules(lease.manifest, cursor, now=now)
            finally:
                cursor.close()
            if reader.current_pointer() != lease.pointer:
                return unavailable()
            return PriceAlertEvaluationInputs(
                availability=read.availability,
                generation_id=generation_id,
                source_generation_id=lease.manifest.source_generations["signals"],
                evaluated_at=now,
                available_at=read.available_at,
                rules=read.rules,
                members=read.members,
            )
    except Exception:
        return unavailable()


def _rule(row: PriceAlertRuleProjectionRow) -> PriceAlertRule:
    assert row.ts_code is not None
    assert row.membership_version is not None
    assert row.name is not None
    assert row.priority is not None
    assert row.enabled is not None
    assert row.comparison is not None
    assert row.threshold is not None
    assert row.valid_from is not None
    assert row.valid_until is not None
    return PriceAlertRule(
        rule_id=row.rule_id,
        name=row.name,
        priority=row.priority,
        enabled=row.enabled,
        comparison=row.comparison,
        threshold=row.threshold,
        valid_from=time.fromisoformat(row.valid_from),
        valid_until=time.fromisoformat(row.valid_until),
    )


def _decision(
    row: PriceAlertRuleProjectionRow,
    *,
    source_generation_id: str,
    decision: PriceRuleDecision | None = None,
    state: Literal["triggered", "not_triggered", "unavailable"] | None = None,
    reason: BatchReason | None = None,
    quote: PriceQuoteEvidence | None = None,
) -> PriceAlertBatchResult:
    assert row.ts_code is not None
    assert row.membership_version is not None
    assert decision is not None or (state is not None and reason is not None)
    return PriceAlertBatchResult(
        owner_id=row.owner_id,
        rule_id=row.rule_id,
        rule_version=row.version,
        ts_code=row.ts_code,
        membership_version=row.membership_version,
        source_generation_id=source_generation_id,
        state=decision.state if decision is not None else state,
        reason=decision.reason if decision is not None else reason,
        quote_source_observed_at=None if quote is None else quote.quote.observed_at,
        quote_timestamp_provenance=None if quote is None else quote.source_timestamp_provenance,
    )


def evaluate_price_alert_batch(
    inputs: PriceAlertEvaluationInputs,
    *,
    market: MarketDayEvidence,
    quotes: tuple[PriceQuoteEvidence, ...],
    max_quote_age_seconds: int,
) -> PriceAlertBatch:
    """Preserve every rule decision and provenance; never create a signal or notification."""
    at = inputs.evaluated_at
    if type(max_quote_age_seconds) is not int or max_quote_age_seconds <= 0:
        raise ValueError("max_quote_age_seconds must be a positive integer")
    common = {
        "availability": inputs.availability,
        "generation_id": inputs.generation_id,
        "source_generation_id": inputs.source_generation_id,
        "evaluated_at": at,
        "market": market,
    }
    if inputs.availability != "ready" or inputs.source_generation_id is None:
        return PriceAlertBatch(**common, results=())

    members = {(row.owner_id, row.ts_code): row for row in inputs.members}
    quote_by_code: dict[str, PriceQuoteEvidence] = {}
    ambiguous: set[str] = set()
    for item in quotes:
        code = item.quote.ts_code
        if code in quote_by_code:
            ambiguous.add(code)
        quote_by_code[code] = item

    results: list[PriceAlertBatchResult] = []
    for row in inputs.rules:
        if row.deleted:
            continue
        assert row.ts_code is not None
        assert row.membership_version is not None
        candidate = quote_by_code.get(row.ts_code)
        if not row.enabled:
            reason = "disabled"
        else:
            member = members.get((row.owner_id, row.ts_code))
            if member is None or member.deleted:
                reason = "member_missing"
            elif member.version != row.membership_version:
                reason = "membership_changed"
            elif member.expires_at is not None and member.expires_at <= at:
                reason = "member_expired"
            else:
                reason = None
        if reason is not None:
            results.append(
                _decision(
                    row,
                    source_generation_id=inputs.source_generation_id,
                    state="not_triggered",
                    reason=reason,
                    quote=candidate,
                )
            )
            continue

        blocked_quote: BatchReason | None = None
        if row.ts_code in ambiguous:
            blocked_quote = "quote_ambiguous"
        elif candidate is not None:
            if candidate.source_timestamp_provenance != "provider_source_timestamp":
                blocked_quote = "quote_source_untrusted"
            elif (
                candidate.previous_source_observed_at is not None
                and candidate.quote.observed_at < candidate.previous_source_observed_at
            ):
                blocked_quote = "quote_source_time_regressed"
        usable_quote = None if blocked_quote is not None or candidate is None else candidate.quote
        decision = evaluate_price_rule(
            _rule(row),
            PriceAlertEvaluationContext(
                ts_code=row.ts_code,
                quote=usable_quote,
                evaluated_at=at,
                market=market,
                scope=ScopeMembershipEvidence(member_codes=frozenset({row.ts_code})),
                max_quote_age_seconds=max_quote_age_seconds,
            ),
        )
        if blocked_quote is not None and decision.reason is PriceRuleReason.QUOTE_MISSING:
            results.append(
                _decision(
                    row,
                    source_generation_id=inputs.source_generation_id,
                    state="unavailable",
                    reason=blocked_quote,
                    quote=candidate,
                )
            )
        else:
            results.append(
                _decision(
                    row,
                    source_generation_id=inputs.source_generation_id,
                    decision=decision,
                    quote=candidate,
                )
            )
    return PriceAlertBatch(**common, results=tuple(results))
