"""Bounded, read-only financial summary over one already pinned replica connection."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb

from rquant.fundamental_receipts import (
    FIELD_NAMES,
    RECEIPT_JOIN,
    RECEIPT_SELECT,
    checked_fundamental_version,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_CALENDAR_DAYS = 32
_MAX_HEADS = 8_000
_CATEGORIES = (
    "尚无来源记录",
    "披露尚未可见",
    "字段缺值",
    "来源证据不足",
    "日历待核验",
    "候选数量超限",
    "数值不可用",
    "其他原因",
)
_REASONS = {
    "no_financial_archive": "尚无来源记录",
    "no_period_evidence": "尚无来源记录",
    "no_observed_batch": "尚无来源记录",
    "missing_symbol_row": "尚无来源记录",
    "no_facts": "尚无来源记录",
    "no_visible_version": "披露尚未可见",
    "missing_announcement_date": "披露尚未可见",
    "observed_at_decision_boundary": "披露尚未可见",
    "missing_value": "字段缺值",
    "missing_field": "字段缺值",
    "invalid_import_cursor": "来源证据不足",
    "mixed_logical_keys": "来源证据不足",
    "unkeyed_observation": "来源证据不足",
    "unusable_observation": "来源证据不足",
    "conflicting_versions": "来源证据不足",
    "conflicted_observation": "来源证据不足",
    "invalid_observation_evidence": "来源证据不足",
    "valuation_not_observed": "来源证据不足",
    "missing_observation_time": "来源证据不足",
    "future_report_period": "来源证据不足",
    "invalid_calendar": "日历待核验",
    "outside_calendar": "日历待核验",
    "incomplete_calendar": "日历待核验",
    "no_previous_session": "日历待核验",
    "decision_not_open": "日历待核验",
    "before_next_session": "日历待核验",
    "no_next_open_day": "日历待核验",
    "candidate_limit": "候选数量超限",
    "period_candidate_limit": "候选数量超限",
    "unrepresentable_numeric": "数值不可用",
}


class FinancialSummaryBudgetError(ValueError):
    """The requested day's heads exceed the fixed read budget."""


@dataclass(frozen=True, slots=True)
class ReasonCount:
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class FieldCount:
    key: str
    known_count: int
    unknown_count: int
    reasons: tuple[ReasonCount, ...]


@dataclass(frozen=True, slots=True)
class FundamentalSummary:
    status: Literal["ready", "calendar_unavailable", "no_records"]
    decision_date: date | None
    waiting_for_today: bool
    record_count: int | None
    fields: tuple[FieldCount, ...]


def _decision_day(connection: duckdb.DuckDBPyConnection, now: datetime) -> tuple[date | None, bool]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("financial summary clock must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    today = local.date()
    start = today - timedelta(days=_MAX_CALENDAR_DAYS - 1)
    rows = connection.execute(
        "SELECT cal_date, is_open, pretrade_date, source FROM trade_calendar "
        "WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ? ORDER BY cal_date",
        [start, today],
    ).fetchall()
    if not rows or rows[-1][0] != today:
        return None, False
    cutoff = today if local.time() >= time(17) else today - timedelta(days=1)
    openings = [day for day, is_open, *_ in rows if is_open and day <= cutoff]
    if len(openings) < 2:
        return None, False
    decision, previous = openings[-1], openings[-2]
    window = [row for row in rows if row[0] >= previous]
    if len(window) != (today - previous).days + 1:
        return None, False
    prior_open = window[0][2]
    if prior_open is None or prior_open >= previous:
        return None, False
    for offset, (day, is_open, pretrade_date, source) in enumerate(window):
        if (
            day != previous + timedelta(days=offset)
            or source != "tushare"
            or pretrade_date != prior_open
        ):
            return None, False
        if is_open:
            prior_open = day
    return decision, bool(rows[-1][1] and local.time() < time(17))


def _reason_counts(raw: Counter[str]) -> tuple[ReasonCount, ...]:
    grouped: Counter[str] = Counter()
    for reason, count in raw.items():
        grouped[_REASONS.get(reason, "其他原因")] += count
    order = {category: index for index, category in enumerate(_CATEGORIES)}
    if len(grouped) > 4:
        specific = sorted(
            (label for label in grouped if label != "其他原因"),
            key=lambda label: (-grouped[label], order[label]),
        )
        keep = set(specific[:3])
        grouped = Counter(
            {label: grouped[label] for label in keep}
            | {"其他原因": sum(count for label, count in grouped.items() if label not in keep)}
        )
    return tuple(
        ReasonCount(label=label, count=grouped[label])
        for label in sorted(grouped, key=lambda label: (-grouped[label], order[label]))
    )


def read_fundamental_summary(
    connection: duckdb.DuckDBPyConnection, *, now: datetime
) -> FundamentalSummary:
    decision, waiting = _decision_day(connection, now)
    if decision is None:
        return FundamentalSummary("calendar_unavailable", None, False, None, ())
    rows = connection.execute(
        f"SELECT {RECEIPT_SELECT} {RECEIPT_JOIN} WHERE h.trade_date = ? ORDER BY h.ts_code LIMIT ?",
        [decision, _MAX_HEADS + 1],
    ).fetchall()
    if len(rows) > _MAX_HEADS:
        raise FinancialSummaryBudgetError("financial summary head limit exceeded")
    if not rows:
        return FundamentalSummary("no_records", decision, waiting, None, ())
    seen: set[str] = set()
    known: Counter[str] = Counter()
    unknown: dict[str, Counter[str]] = {name: Counter() for name in FIELD_NAMES}
    for row in rows:
        version = checked_fundamental_version(row, expected_date=decision)
        if version.ts_code in seen:
            raise ValueError("financial summary has duplicate heads")
        seen.add(version.ts_code)
        for name in FIELD_NAMES:
            field = version.fields[name]
            if field.status == "selected":
                known[name] += 1
            else:
                unknown[name][field.reason] += 1
    fields = tuple(
        FieldCount(
            key=name,
            known_count=known[name],
            unknown_count=sum(unknown[name].values()),
            reasons=_reason_counts(unknown[name]),
        )
        for name in FIELD_NAMES
    )
    return FundamentalSummary("ready", decision, waiting, len(rows), fields)
