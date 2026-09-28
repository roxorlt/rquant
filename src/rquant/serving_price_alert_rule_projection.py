"""Bounded price-rule heads for one read-only Serving generation."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import OwnerId, TsCode
from rquant.price_alert_rule_store import MAX_ACTIVE_RULES, PriceAlertRuleEntry, RuleId
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistProjectionRow,
    validate_manual_watchlist_projections,
)
from rquant.serving_publisher import ServingGenerationLease
from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload

MAX_PRICE_ALERT_RULE_ROWS = 10_000
_UNAVAILABLE_AT = datetime(1970, 1, 1, tzinfo=UTC)


def _bounded_threshold(value: Decimal) -> str:
    if not value.is_finite() or abs(value.adjusted()) > 64 or len(value.as_tuple().digits) > 64:
        raise ValueError("price rule threshold exceeds its projection bound")
    return format(value, "f")


class PriceAlertRuleProjectionRow(RuntimeContractModel):
    owner_id: OwnerId
    rule_id: RuleId
    version: StrictInt = Field(ge=1)
    deleted: StrictBool
    ts_code: TsCode | None
    membership_version: StrictInt | None = Field(ge=1)
    name: StrictStr | None = Field(default=None, min_length=1, max_length=80)
    priority: Literal["P0", "P1", "P2", "P3"] | None
    enabled: StrictBool | None
    comparison: Literal["gte", "lte"] | None
    threshold: StrictStr | None = Field(default=None, max_length=128)
    valid_from: StrictStr | None = Field(default=None, max_length=32)
    valid_until: StrictStr | None = Field(default=None, max_length=32)
    updated_at: AwareUtcDatetime

    @classmethod
    def from_entry(cls, entry: PriceAlertRuleEntry) -> PriceAlertRuleProjectionRow:
        rule = entry.rule
        return cls(
            owner_id=entry.owner_id,
            rule_id=entry.rule_id,
            version=entry.version,
            deleted=entry.deleted,
            ts_code=entry.ts_code,
            membership_version=entry.membership_version,
            name=None if rule is None else rule.name,
            priority=None if rule is None else rule.priority,
            enabled=None if rule is None else rule.enabled,
            comparison=None if rule is None else rule.comparison,
            threshold=None if rule is None else _bounded_threshold(rule.threshold),
            valid_from=None if rule is None else rule.valid_from.isoformat(),
            valid_until=None if rule is None else rule.valid_until.isoformat(),
            updated_at=entry.updated_at,
        )

    @model_validator(mode="after")
    def validate_rule(self) -> Self:
        facts = (
            self.ts_code,
            self.membership_version,
            self.name,
            self.priority,
            self.enabled,
            self.comparison,
            self.threshold,
            self.valid_from,
            self.valid_until,
        )
        if self.deleted:
            if any(value is not None for value in facts):
                raise ValueError("price rule tombstone carries live facts")
            return self
        if any(value is None for value in facts):
            raise ValueError("live price rule lacks facts or scope binding")
        try:
            rule = PriceAlertRule(
                rule_id=self.rule_id,
                name=self.name,
                priority=self.priority,
                enabled=self.enabled,
                comparison=self.comparison,
                threshold=self.threshold,
                valid_from=time.fromisoformat(self.valid_from),
                valid_until=time.fromisoformat(self.valid_until),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("price rule facts are invalid") from exc
        if (
            _bounded_threshold(rule.threshold) != self.threshold
            or rule.valid_from.isoformat() != self.valid_from
            or rule.valid_until.isoformat() != self.valid_until
        ):
            raise ValueError("price rule facts are not canonical")
        return self

    def projection_row(self) -> dict[str, object]:
        return {
            "owner_id": self.owner_id,
            "rule_id": self.rule_id,
            "version": self.version,
            "deleted": self.deleted,
            "ts_code": self.ts_code,
            "membership_version": self.membership_version,
            "name": self.name,
            "priority": self.priority,
            "enabled": self.enabled,
            "comparison": self.comparison,
            "threshold": self.threshold,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
            "updated_at": self.updated_at.isoformat(),
        }


class PriceAlertRuleAuthoritySnapshot(RuntimeContractModel):
    activated_at: AwareUtcDatetime
    rows: tuple[PriceAlertRuleProjectionRow, ...] = ()
    row_count: StrictInt = Field(ge=0, le=MAX_PRICE_ALERT_RULE_ROWS)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @staticmethod
    def digest(rows: Iterable[PriceAlertRuleProjectionRow]) -> str:
        return canonical_sha256(
            {
                "contract": "price-alert-rule-snapshot/v1",
                "rows": tuple(row.projection_row() for row in rows),
            }
        )

    @classmethod
    def create(
        cls, *, activated_at: datetime, rows: Iterable[PriceAlertRuleProjectionRow]
    ) -> PriceAlertRuleAuthoritySnapshot:
        ordered = tuple(sorted(rows, key=lambda row: (row.owner_id, row.rule_id)))
        return cls(
            activated_at=activated_at,
            rows=ordered,
            row_count=len(ordered),
            rows_sha256=cls.digest(ordered),
        )

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        identities = tuple((row.owner_id, row.rule_id) for row in self.rows)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("price rule identities must be sorted and unique")
        if self.row_count != len(self.rows) or self.rows_sha256 != self.digest(self.rows):
            raise ValueError("price rule count or digest mismatch")
        live_counts = Counter(row.owner_id for row in self.rows if not row.deleted)
        if any(count > MAX_ACTIVE_RULES for count in live_counts.values()):
            raise ValueError("owner has too many live price rules")
        return self


def build_price_alert_rule_projections(
    snapshot: PriceAlertRuleAuthoritySnapshot | None,
    *,
    observed_at: datetime,
    unavailable: bool = False,
) -> tuple[ServingProjectionPayload, ...]:
    observed = normalize_aware_utc(observed_at)
    if snapshot is None:
        return (
            ServingProjectionPayload(
                table_name="price_alert_rule_state",
                available_at=_UNAVAILABLE_AT,
                rows=(
                    {
                        "snapshot_key": "current",
                        "state": "unavailable" if unavailable else "not_activated",
                        "activated_at": None,
                        "row_count": None,
                        "rows_sha256": None,
                    },
                ),
            ),
        )
    if unavailable:
        raise ValueError("ready price rule snapshot cannot be unavailable")
    if snapshot.activated_at > observed or any(row.updated_at > observed for row in snapshot.rows):
        raise ValueError("price rule snapshot contains future evidence")
    available = max((snapshot.activated_at, *(row.updated_at for row in snapshot.rows)))
    return (
        ServingProjectionPayload(
            table_name="price_alert_rule_state",
            available_at=available,
            rows=(
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "activated_at": snapshot.activated_at.isoformat(),
                    "row_count": snapshot.row_count,
                    "rows_sha256": snapshot.rows_sha256,
                },
            ),
        ),
        ServingProjectionPayload(
            table_name="price_alert_rule",
            available_at=available,
            rows=tuple(row.projection_row() for row in snapshot.rows),
        ),
    )


def validate_price_alert_rule_projections(
    projections: Mapping[str, ServingProjectionPayload | ServingProjectionInput],
) -> None:
    state = projections.get("price_alert_rule_state")
    facts = projections.get("price_alert_rule")
    if state is None:
        if facts is not None:
            raise ValueError("price rule facts lack an authority state")
        return
    if len(state.rows) != 1 or state.rows[0]["snapshot_key"] != "current":
        raise ValueError("price rule authority state is incomplete")
    status = state.rows[0]
    if status["state"] in {"not_activated", "unavailable"}:
        if facts is not None or any(
            status[field] is not None for field in ("activated_at", "row_count", "rows_sha256")
        ):
            raise ValueError("unavailable price rule source carries authority")
        return
    if status["state"] != "ready" or facts is None:
        raise ValueError("ready price rule state lacks facts")
    if state.available_at != facts.available_at:
        raise ValueError("price rule state and facts have different source times")
    if (
        isinstance(state, ServingProjectionInput)
        and isinstance(facts, ServingProjectionInput)
        and state.owner_generation_id != facts.owner_generation_id
    ):
        raise ValueError("price rule state and facts belong to different generations")
    snapshot = PriceAlertRuleAuthoritySnapshot(
        activated_at=status["activated_at"],
        rows=tuple(PriceAlertRuleProjectionRow.model_validate(row) for row in facts.rows),
        row_count=status["row_count"],
        rows_sha256=status["rows_sha256"],
    )
    if snapshot.activated_at > state.available_at or any(
        row.updated_at > state.available_at for row in snapshot.rows
    ):
        raise ValueError("price rule source time precedes its facts")


def price_rule_scope_status(
    projections: Mapping[str, ServingProjectionInput | ServingProjectionPayload],
    *,
    lease: ServingGenerationLease,
    max_generation_age: timedelta,
    owner_id: str,
    rule_id: str,
    at: datetime,
) -> Literal["active", "inactive", "unavailable"]:
    """Resolve scope only through a fresh, verified current Serving lease."""
    observed = normalize_aware_utc(at)
    built_at = lease.manifest.built_at
    if (
        lease.closed
        or lease.pointer is None
        or lease.pointer.generation_id != lease.manifest.generation_id
        or max_generation_age <= timedelta(0)
        or not built_at <= observed <= built_at + max_generation_age
    ):
        return "unavailable"
    names = (
        "price_alert_rule_state",
        "price_alert_rule",
        "manual_watchlist_state",
        "manual_watchlist",
    )
    selected = tuple(projections.get(name) for name in names)
    if not all(isinstance(item, ServingProjectionInput) for item in selected):
        return "unavailable"
    if any(item.available_at > observed for item in selected):
        return "unavailable"
    generations = {item.owner_generation_id for item in selected}
    if len(generations) != 1 or lease.manifest.source_generations.get("signals") not in generations:
        return "unavailable"
    try:
        validate_price_alert_rule_projections(projections)
        validate_manual_watchlist_projections(projections)
        if (
            projections["price_alert_rule_state"].rows[0]["state"] != "ready"
            or projections["manual_watchlist_state"].rows[0]["state"] != "ready"
        ):
            return "unavailable"
        rules = tuple(
            PriceAlertRuleProjectionRow.model_validate(row)
            for row in projections["price_alert_rule"].rows
        )
        members = tuple(
            ManualWatchlistProjectionRow.model_validate(row)
            for row in projections["manual_watchlist"].rows
        )
    except (KeyError, TypeError, ValueError):
        return "unavailable"
    rule = next((row for row in rules if row.owner_id == owner_id and row.rule_id == rule_id), None)
    if rule is None or rule.deleted or not rule.enabled:
        return "inactive"
    member = next(
        (row for row in members if row.owner_id == owner_id and row.ts_code == rule.ts_code),
        None,
    )
    if (
        member is None
        or member.deleted
        or member.version != rule.membership_version
        or (member.expires_at is not None and member.expires_at <= observed)
    ):
        return "inactive"
    return "active"
