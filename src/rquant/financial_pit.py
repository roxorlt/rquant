"""Pure financial fact visibility and revision selection at a decision instant."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, StrictBool

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_OPEN_TIME = time(9, 30)


class _PITModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class FinancialFact(_PITModel):
    """One supplier-observed version of one financial field."""

    source_api: str = Field(min_length=1)
    field: str = Field(min_length=1)
    ts_code: str = Field(min_length=1)
    report_period: date
    report_type: str = Field(min_length=1)
    ann_date: date | None
    f_ann_date: date | None = None
    first_observed_at: datetime | None
    value: Decimal | None
    update_flag: str | None = None
    block_reason: str | None = None

    @property
    def logical_key(self) -> tuple[str, str, str, date, str]:
        return (
            self.source_api,
            self.field,
            self.ts_code,
            self.report_period,
            self.report_type,
        )


class SSECalendarDay(_PITModel):
    day: date
    is_open: StrictBool


class SSECalendar(_PITModel):
    """A declared inclusive SSE coverage interval with one row per civil day."""

    exchange: Literal["SSE"] = "SSE"
    coverage_start: date
    coverage_end: date
    days: tuple[SSECalendarDay, ...]


class FinancialPITSelection(_PITModel):
    status: Literal["selected", "unknown"]
    reason: str
    fact: FinancialFact | None = None
    next_open_at: datetime | None = None
    content_sha256: str | None = None


def _unknown(reason: str) -> FinancialPITSelection:
    return FinancialPITSelection(status="unknown", reason=reason)


def _aware_utc(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is None:
        return None
    try:
        if value.utcoffset() is None:
            return None
        return value.astimezone(UTC)
    except (OverflowError, TypeError, ValueError):
        return None


def _calendar_days(calendar: SSECalendar) -> dict[date, bool] | None:
    if calendar.coverage_start > calendar.coverage_end:
        return None
    expected_count = (calendar.coverage_end - calendar.coverage_start).days + 1
    if len(calendar.days) != expected_count:
        return None
    rows: dict[date, bool] = {}
    for row in calendar.days:
        if row.day < calendar.coverage_start or row.day > calendar.coverage_end:
            return None
        if row.day in rows:
            return None
        rows[row.day] = row.is_open
    return rows


def _next_open_at(
    *,
    publication_date: date,
    calendar: SSECalendar,
    calendar_days: dict[date, bool],
) -> datetime | None:
    if publication_date < calendar.coverage_start or publication_date > calendar.coverage_end:
        return None
    for day in sorted(calendar_days):
        if day > publication_date and calendar_days[day]:
            return datetime.combine(day, _OPEN_TIME, tzinfo=_SHANGHAI)
    return None


def _content_sha256(
    fact: FinancialFact,
    *,
    first_observed_at: datetime,
    next_open_at: datetime,
) -> str:
    value = fact.value
    if value is None:
        raise ValueError("selected financial fact requires a value")
    parts = value.as_tuple()
    digits = list(parts.digits)
    exponent = int(parts.exponent)
    if all(digit == 0 for digit in digits):
        digits = [0]
        exponent = 0
        sign = 0
    else:
        while digits[-1] == 0:
            digits.pop()
            exponent += 1
        sign = parts.sign
    payload = {
        "source_api": fact.source_api,
        "field": fact.field,
        "ts_code": fact.ts_code,
        "report_period": fact.report_period.isoformat(),
        "report_type": fact.report_type,
        "ann_date": fact.ann_date.isoformat() if fact.ann_date is not None else None,
        "f_ann_date": fact.f_ann_date.isoformat() if fact.f_ann_date is not None else None,
        "first_observed_at": first_observed_at.isoformat(timespec="microseconds"),
        "next_open_at": next_open_at.isoformat(timespec="microseconds"),
        "value": [sign, "".join(str(digit) for digit in digits), exponent],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def select_financial_fact(
    facts: Sequence[FinancialFact],
    *,
    as_of: datetime,
    calendar: SSECalendar,
) -> FinancialPITSelection:
    """Return the latest proven version of one logical field, or an explicit unknown."""

    as_of_utc = _aware_utc(as_of)
    if as_of_utc is None:
        return _unknown("invalid_as_of")
    calendar_days = _calendar_days(calendar)
    if calendar_days is None:
        return _unknown("invalid_calendar")
    try:
        as_of_local = as_of_utc.astimezone(_SHANGHAI)
    except (OverflowError, ValueError):
        return _unknown("invalid_as_of")
    if not calendar.coverage_start <= as_of_local.date() <= calendar.coverage_end:
        return _unknown("outside_calendar")
    if not facts:
        return _unknown("no_facts")
    key = facts[0].logical_key
    if any(fact.logical_key != key for fact in facts[1:]):
        return _unknown("mixed_logical_keys")

    eligible: list[tuple[datetime, str, FinancialFact, datetime]] = []
    latest_blocker: tuple[datetime, str] | None = None
    unavailable_reason = "no_visible_version"
    for fact in facts:
        observed_utc = _aware_utc(fact.first_observed_at)
        if observed_utc is None:
            return _unknown("missing_observation_time")
        if as_of_utc <= observed_utc:
            continue
        # A keyed revision cannot resolve an observation with no report identity.
        if fact.block_reason == "unkeyed_observation":
            return _unknown("unkeyed_observation")
        block_reason = (
            fact.block_reason
            or ("missing_announcement_date" if fact.ann_date is None else None)
            or ("missing_value" if fact.value is None else None)
        )
        if block_reason is not None:
            if latest_blocker is None or observed_utc >= latest_blocker[0]:
                latest_blocker = (observed_utc, block_reason)
            continue
        assert fact.ann_date is not None
        publication_date = max(fact.ann_date, fact.f_ann_date or fact.ann_date)
        if publication_date > as_of_local.date():
            continue
        if not calendar.coverage_start <= publication_date <= calendar.coverage_end:
            unavailable_reason = "outside_calendar"
            continue
        next_open_at = _next_open_at(
            publication_date=publication_date,
            calendar=calendar,
            calendar_days=calendar_days,
        )
        if next_open_at is None:
            unavailable_reason = "no_next_open_day"
            continue
        if as_of_local < next_open_at:
            continue
        digest = _content_sha256(
            fact,
            first_observed_at=observed_utc,
            next_open_at=next_open_at,
        )
        eligible.append((observed_utc, digest, fact, next_open_at))

    if not eligible:
        return _unknown(latest_blocker[1] if latest_blocker is not None else unavailable_reason)
    latest_observation = max(item[0] for item in eligible)
    if latest_blocker is not None and latest_blocker[0] >= latest_observation:
        return _unknown(latest_blocker[1])
    latest = [item for item in eligible if item[0] == latest_observation]
    if len({item[1] for item in latest}) != 1:
        return _unknown("conflicting_versions")
    _, digest, fact, next_open_at = latest[0]
    return FinancialPITSelection(
        status="selected",
        reason="latest_visible_version",
        fact=fact,
        next_open_at=next_open_at,
        content_sha256=digest,
    )
