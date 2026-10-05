"""Bounded catalog facts; date presence never proves a complete security population."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_audit_contracts import MAX_AUDIT_DAYS
from rquant.data_audit_coverage import (
    CalendarEvidence,
    ClosedDayRows,
    DailyBarCount,
    DailyBarCountEvidence,
    DailyBarCoverageRequest,
    MonthlyCoverage,
    TradingDayGap,
    audit_daily_bar_coverage,
)
from rquant.data_catalog.build import CATALOG_CONTRACTS
from rquant.data_contracts import EXCHANGE_TIMEZONE, DatasetContract, VisibilityRule

DATASET_AUDIT_RULE_VERSION = "catalog-dataset-audit-v1"
MAX_DATASET_FIELDS = 128
MAX_ROW_CHANGES = 128
MAX_INDEXED_DATASET_DATES = 128
MAX_DIMENSIONS = 64
MAX_DATASET_RESULT_BYTES = 48 * 1024
_COUNT = 1_000_000_000_000

SourceState = Literal["ready", "missing_source", "schema_mismatch"]
AssessmentState = Literal[
    "measured",
    "delayed",
    "not_applicable",
    "missing_expected_scope",
    "missing_source",
    "not_evaluated",
]
AuditReason = Literal[
    "date_presence_only",
    "population_unknown",
    "minute_grid_unknown",
    "named_partitions_only",
    "current_snapshot",
    "event_driven",
    "not_required_daily",
    "visibility_unknown",
    "source_missing",
    "contract_columns_missing",
    "no_observations",
    "no_visible_observations",
    "no_completed_sessions",
    "outside_contract_history",
    "session_grid_unknown",
    "null_counts_only",
    "declared_keys",
    "declared_sources",
    "declared_frequencies",
    "not_minute",
    "no_ingestion_clock",
    "observations_only",
    "no_source_column",
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class DatasetDayCount(_Model):
    day: date
    row_count: int = Field(ge=0, le=_COUNT, strict=True)
    visible_rows: int = Field(ge=0, le=_COUNT, strict=True)

    @model_validator(mode="after")
    def _counts(self) -> DatasetDayCount:
        if self.visible_rows > self.row_count:
            raise ValueError("visible rows exceed observed rows")
        return self


class DatasetFieldNulls(_Model):
    field_name: str = Field(min_length=1, max_length=64)
    observed_rows: int = Field(ge=0, le=_COUNT, strict=True)
    null_rows: int = Field(ge=0, le=_COUNT, strict=True)
    required_key: bool

    @model_validator(mode="after")
    def _counts(self) -> DatasetFieldNulls:
        if self.null_rows > self.observed_rows:
            raise ValueError("null rows exceed observed rows")
        return self

    @property
    def null_ratio(self) -> float | None:
        return self.null_rows / self.observed_rows if self.observed_rows else None


class DatasetFrequencyCount(_Model):
    frequency: str = Field(min_length=1, max_length=32)
    row_count: int = Field(ge=0, le=_COUNT, strict=True)
    visible_rows: int = Field(ge=0, le=_COUNT, strict=True)

    @model_validator(mode="after")
    def _counts(self) -> DatasetFrequencyCount:
        if self.visible_rows > self.row_count:
            raise ValueError("visible frequency rows exceed observed rows")
        return self


class DatasetAuditEvidence(_Model):
    dataset_id: str = Field(min_length=1, max_length=64)
    source_id: str = Field(min_length=1, max_length=128)
    source_kind: Literal["fixed_replica", "named_lake"] = "fixed_replica"
    source_state: SourceState
    source_column_present: bool = False
    audit_start: date
    observed_through: date
    as_of: datetime
    missing_columns: tuple[str, ...] = Field(default=(), max_length=MAX_DATASET_FIELDS)
    observed_rows: int = Field(default=0, ge=0, le=_COUNT, strict=True)
    visible_rows: int = Field(default=0, ge=0, le=_COUNT, strict=True)
    recorded_after_as_of_rows: int = Field(default=0, ge=0, le=_COUNT, strict=True)
    null_event_rows: int = Field(default=0, ge=0, le=_COUNT, strict=True)
    unknown_source_rows: int = Field(default=0, ge=0, le=_COUNT, strict=True)
    unknown_frequency_rows: int = Field(default=0, ge=0, le=_COUNT, strict=True)
    latest_visible_date: date | None = None
    latest_visible_time: datetime | None = None
    latest_ingested_at: datetime | None = None
    days: tuple[DatasetDayCount, ...] = Field(default=(), max_length=MAX_AUDIT_DAYS)
    fields: tuple[DatasetFieldNulls, ...] = Field(default=(), max_length=MAX_DATASET_FIELDS)
    frequencies: tuple[DatasetFrequencyCount, ...] = Field(default=(), max_length=MAX_DIMENSIONS)

    @model_validator(mode="after")
    def _range(self) -> DatasetAuditEvidence:
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("audit observation time must be timezone-aware")
        if not 1 <= (self.observed_through - self.audit_start).days + 1 <= MAX_AUDIT_DAYS:
            raise ValueError("invalid bounded audit date range")
        dates = tuple(d.day for d in self.days)
        if dates != tuple(sorted(set(dates))) or any(
            not self.audit_start <= d <= self.observed_through for d in dates
        ):
            raise ValueError("audit day counts are duplicated or out of range")
        names = tuple(f.field_name for f in self.fields)
        if names != tuple(sorted(set(names))):
            raise ValueError("audit fields must be unique and ordered")
        frequencies = tuple(f.frequency for f in self.frequencies)
        if frequencies != tuple(sorted(set(frequencies))):
            raise ValueError("audit frequencies must be unique and ordered")
        if any(
            c > self.observed_rows
            for c in (
                self.visible_rows,
                self.recorded_after_as_of_rows,
                self.null_event_rows,
                self.unknown_source_rows,
                self.unknown_frequency_rows,
            )
        ) or any(f.observed_rows != self.observed_rows for f in self.fields):
            raise ValueError("audit counts disagree with observed rows")
        if self.source_state != "ready" and (
            self.observed_rows or self.days or self.fields or self.frequencies
        ):
            raise ValueError("missing source cannot carry measured observations")
        return self


class DatasetRowChange(_Model):
    day: date
    row_count: int = Field(ge=0, le=_COUNT, strict=True)
    previous_rows: int | None = Field(default=None, ge=0, le=_COUNT)
    change_rows: int | None = None

    @model_validator(mode="after")
    def _delta(self) -> DatasetRowChange:
        expected = None if self.previous_rows is None else self.row_count - self.previous_rows
        if self.change_rows != expected:
            raise ValueError("row change disagrees with observed counts")
        return self


class DatasetAuditRule(_Model):
    rule_id: Literal[
        "date_presence",
        "freshness",
        "required_keys",
        "field_nulls",
        "known_sources",
        "known_frequency",
        "closed_day_rows",
        "row_count_change",
        "observation_cutoff",
    ]
    state: AssessmentState
    reason: AuditReason
    checked_rows: int = Field(ge=0, le=_COUNT, strict=True)
    issue_count: int = Field(ge=0, le=_COUNT, strict=True)


class DatasetAuditResult(_Model):
    dataset_id: str = Field(min_length=1, max_length=64)
    source_id: str = Field(min_length=1, max_length=128)
    source_kind: Literal["fixed_replica", "named_lake"]
    source_state: SourceState
    source_column_present: bool
    scope: Literal["audit_range", "current_snapshot", "named_partitions"]
    audit_start: date
    observed_through: date
    as_of: datetime
    contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rule_version: Literal["catalog-dataset-audit-v1"] = DATASET_AUDIT_RULE_VERSION
    visibility: VisibilityRule
    coverage_state: AssessmentState
    coverage_reason: AuditReason
    completeness_state: Literal["missing_expected_scope", "not_applicable", "not_evaluated"]
    freshness_state: AssessmentState
    freshness_reason: AuditReason
    freshness_lag_sessions: int | None = Field(default=None, ge=0, le=MAX_AUDIT_DAYS)
    observed_age_seconds: float | None = Field(default=None, ge=0)
    latest_visible_date: date | None
    latest_visible_time: datetime | None
    latest_ingested_at: datetime | None
    observed_rows: int = Field(ge=0, le=_COUNT)
    visible_rows: int = Field(ge=0, le=_COUNT)
    pending_rows: int = Field(ge=0, le=_COUNT)
    recorded_after_as_of_rows: int = Field(ge=0, le=_COUNT)
    unknown_source_rows: int = Field(ge=0, le=_COUNT)
    unknown_frequency_rows: int = Field(ge=0, le=_COUNT)
    expected_open_days: int | None = Field(default=None, ge=0, le=MAX_AUDIT_DAYS)
    covered_open_days: int | None = Field(default=None, ge=0, le=MAX_AUDIT_DAYS)
    monthly: tuple[MonthlyCoverage, ...] = Field(max_length=122)
    gaps: tuple[TradingDayGap, ...] = Field(max_length=MAX_INDEXED_DATASET_DATES)
    omitted_gap_count: int = Field(default=0, ge=0, le=MAX_AUDIT_DAYS)
    omitted_gap_open_days: int = Field(default=0, ge=0, le=MAX_AUDIT_DAYS)
    closed_day_rows: tuple[ClosedDayRows, ...] = Field(max_length=MAX_INDEXED_DATASET_DATES)
    omitted_closed_day_count: int = Field(default=0, ge=0, le=MAX_AUDIT_DAYS)
    omitted_closed_day_rows: int = Field(default=0, ge=0, le=_COUNT)
    fields: tuple[DatasetFieldNulls, ...] = Field(max_length=MAX_DATASET_FIELDS)
    frequencies: tuple[DatasetFrequencyCount, ...] = Field(max_length=MAX_DIMENSIONS)
    row_changes: tuple[DatasetRowChange, ...] = Field(max_length=MAX_ROW_CHANGES)
    omitted_row_changes: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    rules: tuple[DatasetAuditRule, ...] = Field(min_length=9, max_length=9)
    conclusion: Literal["not_fully_assessed", "issues_observed", "no_issues_observed"]

    @model_validator(mode="after")
    def _counts(self) -> DatasetAuditResult:
        if self.pending_rows != self.observed_rows - self.visible_rows:
            raise ValueError("audit visible and pending counts disagree")
        if (self.expected_open_days is None) != (self.covered_open_days is None):
            raise ValueError("audit coverage denominator is missing")
        if self.expected_open_days is not None and (
            self.covered_open_days is None
            or self.covered_open_days > self.expected_open_days
            or sum(m.expected_open_days for m in self.monthly) != self.expected_open_days
            or sum(m.covered_open_days for m in self.monthly) != self.covered_open_days
            or sum(g.missing_open_days for g in self.gaps) + self.omitted_gap_open_days
            != self.expected_open_days - self.covered_open_days
        ):
            raise ValueError("audit monthly coverage or gaps disagree")
        if len({r.rule_id for r in self.rules}) != 9:
            raise ValueError("audit rules are missing or duplicated")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("audit observation time must be timezone-aware")
        raw = self.model_dump_json().encode()
        if len(raw) > MAX_DATASET_RESULT_BYTES:
            raise ValueError("dataset audit exceeds result byte budget")
        return self


def contract_sha256(contract: DatasetContract) -> str:
    return hashlib.sha256(
        json.dumps(
            contract.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def catalog_contract_sha256() -> str:
    return hashlib.sha256(
        json.dumps(
            {c.dataset_id: contract_sha256(c) for c in CATALOG_CONTRACTS},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def audit_dataset_evidence(
    evidence: DatasetAuditEvidence,
    *,
    calendar: CalendarEvidence,
    contract: DatasetContract,
) -> DatasetAuditResult:
    """Apply only declared policies, with the existing date-presence calculation."""
    if evidence.dataset_id != contract.dataset_id or calendar.snapshot_id != evidence.source_id:
        raise ValueError("audit evidence and contract/source identity disagree")
    span = (evidence.observed_through - evidence.audit_start).days + 1
    if len(calendar.days) != span or any(
        d.day != evidence.audit_start + timedelta(days=i) for i, d in enumerate(calendar.days)
    ):
        raise ValueError("audit calendar must contain every date exactly once")
    ready = evidence.source_state == "ready"
    as_of = evidence.as_of.astimezone(EXCHANGE_TIMEZONE)
    by_date = {d.day: d for d in evidence.days}
    historical = contract.historized and contract.freshness.required_on_open_day
    eligible = tuple(
        d.day
        for d in calendar.days
        if d.is_open
        and d.day < as_of.date()
        and (contract.earliest_date is None or d.day >= contract.earliest_date)
    )
    monthly: tuple[MonthlyCoverage, ...] = ()
    gaps: tuple[TradingDayGap, ...] = ()
    closed: tuple[ClosedDayRows, ...] = ()
    changes: list[DatasetRowChange] = []
    expected = covered = None
    state: AssessmentState = "not_evaluated"
    reason: AuditReason = "no_completed_sessions"
    if not ready:
        state = "missing_source" if evidence.source_state == "missing_source" else "not_evaluated"
        reason = (
            "source_missing"
            if evidence.source_state == "missing_source"
            else "contract_columns_missing"
        )
    elif not contract.historized:
        state, reason = "not_applicable", "current_snapshot"
    elif contract.freshness.event_driven:
        state, reason = "not_applicable", "event_driven"
    elif not contract.freshness.required_on_open_day:
        state, reason = "not_applicable", "not_required_daily"
    elif eligible:
        start = max(evidence.audit_start, contract.earliest_date or evidence.audit_start)
        end = eligible[-1]
        presence = audit_daily_bar_coverage(
            DailyBarCoverageRequest(
                audit_start=start,
                completed_through=end,
                calendar=CalendarEvidence(
                    snapshot_id=evidence.source_id,
                    exchange="SSE",
                    days=tuple(d for d in calendar.days if start <= d.day <= end),
                ),
                daily_bar_counts=DailyBarCountEvidence(
                    snapshot_id=evidence.source_id,
                    days=tuple(
                        DailyBarCount(day=d.day, row_count=d.visible_rows)
                        for d in evidence.days
                        if start <= d.day <= end
                    ),
                ),
            )
        )
        monthly, gaps = presence.monthly, presence.gaps
        expected = sum(m.expected_open_days for m in monthly)
        covered = sum(m.covered_open_days for m in monthly)
        state, reason = "measured", "date_presence_only"
        prior = None
        for day in eligible:
            count = by_date[day].visible_rows if day in by_date else 0
            changes.append(
                DatasetRowChange(
                    day=day,
                    row_count=count,
                    previous_rows=prior,
                    change_rows=None if prior is None else count - prior,
                )
            )
            prior = count
    if ready and historical:
        closed = tuple(
            ClosedDayRows(day=d.day, row_count=by_date[d.day].row_count)
            for d in calendar.days
            if not d.is_open and d.day in by_date and by_date[d.day].row_count
        )
    completeness: Literal["missing_expected_scope", "not_applicable", "not_evaluated"] = (
        "not_evaluated"
        if not ready
        else "missing_expected_scope"
        if historical
        else "not_applicable"
    )
    lag = None
    age = None
    fresh: AssessmentState = "not_evaluated"
    fresh_reason: AuditReason = "visibility_unknown"
    if not ready:
        fresh, fresh_reason = state, reason
    elif contract.freshness.event_driven:
        fresh, fresh_reason = "not_applicable", "event_driven"
    elif not evidence.visible_rows:
        fresh, fresh_reason = "not_evaluated", "no_visible_observations"
    elif contract.freshness.max_wall_clock_lag is not None:
        fresh, fresh_reason = "missing_expected_scope", "session_grid_unknown"
        if evidence.latest_visible_time is not None:
            latest = evidence.latest_visible_time
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=EXCHANGE_TIMEZONE)
            age = max(0.0, (as_of - latest).total_seconds())
    elif (
        contract.freshness.max_trading_session_lag is not None
        and evidence.latest_visible_date is not None
    ):
        reference_dates = list(eligible)
        if (
            contract.visibility == VisibilityRule.AUCTION_0925
            and any(d.is_open and d.day == as_of.date() for d in calendar.days)
            and all(
                contract.is_visible(as_of_time=as_of, event_date=as_of.date(), source=s)
                for s in contract.sources
            )
        ):
            reference_dates.append(as_of.date())
        lag = sum(d > evidence.latest_visible_date for d in reference_dates)
        fresh = "delayed" if lag > contract.freshness.max_trading_session_lag else "measured"
        fresh_reason = "observations_only"
    key_nulls = sum(f.null_rows for f in evidence.fields if f.required_key)
    generic: AssessmentState = "measured" if ready and evidence.observed_rows else "not_evaluated"
    generic_reason: AuditReason = "no_observations" if ready else reason

    def rule(
        rule_id: str, rule_state: AssessmentState, why: AuditReason, count: int = 0
    ) -> DatasetAuditRule:
        return DatasetAuditRule(
            rule_id=rule_id,
            state=rule_state,
            reason=why,
            checked_rows=evidence.observed_rows if ready else 0,
            issue_count=count,
        )

    rules = (
        rule("date_presence", state, reason, 0 if expected is None else expected - (covered or 0)),
        rule("freshness", fresh, fresh_reason, int(fresh == "delayed")),
        rule(
            "required_keys",
            generic,
            "declared_keys" if generic == "measured" else generic_reason,
            key_nulls,
        ),
        rule(
            "field_nulls", generic, "null_counts_only" if generic == "measured" else generic_reason
        ),
        rule(
            "known_sources",
            generic if evidence.source_column_present else "not_evaluated",
            "declared_sources"
            if evidence.source_column_present and generic == "measured"
            else generic_reason
            if evidence.source_column_present
            else "no_source_column",
            evidence.unknown_source_rows,
        ),
        rule(
            "known_frequency",
            generic if contract.dataset_id == "minute_bar" else "not_applicable",
            "declared_frequencies"
            if contract.dataset_id == "minute_bar" and generic == "measured"
            else generic_reason
            if contract.dataset_id == "minute_bar"
            else "not_minute",
            evidence.unknown_frequency_rows,
        ),
        rule(
            "closed_day_rows",
            generic if historical else "not_applicable",
            "observations_only"
            if historical and generic == "measured"
            else generic_reason
            if historical
            else reason,
            sum(d.row_count for d in closed),
        ),
        rule(
            "row_count_change",
            "measured" if changes else "not_applicable" if not historical else "not_evaluated",
            "observations_only" if changes else reason,
        ),
        rule(
            "observation_cutoff",
            generic if contract.ingested_at_column else "not_applicable",
            "observations_only"
            if contract.ingested_at_column and generic == "measured"
            else generic_reason
            if contract.ingested_at_column
            else "no_ingestion_clock",
        ),
    )
    conclusion = (
        "issues_observed"
        if evidence.observed_rows and any(r.issue_count for r in rules)
        else "not_fully_assessed"
    )
    return DatasetAuditResult(
        **evidence.model_dump(exclude={"days", "missing_columns", "null_event_rows"}),
        contract_sha256=contract_sha256(contract),
        visibility=contract.visibility,
        scope="named_partitions"
        if evidence.source_kind == "named_lake"
        else "audit_range"
        if contract.historized
        else "current_snapshot",
        coverage_state=state,
        coverage_reason=reason,
        completeness_state=completeness,
        freshness_state=fresh,
        freshness_reason=fresh_reason,
        freshness_lag_sessions=lag,
        observed_age_seconds=age,
        pending_rows=evidence.observed_rows - evidence.visible_rows,
        expected_open_days=expected,
        covered_open_days=covered,
        monthly=monthly,
        gaps=gaps[:MAX_INDEXED_DATASET_DATES],
        omitted_gap_count=max(0, len(gaps) - MAX_INDEXED_DATASET_DATES),
        omitted_gap_open_days=sum(g.missing_open_days for g in gaps[MAX_INDEXED_DATASET_DATES:]),
        closed_day_rows=closed[:MAX_INDEXED_DATASET_DATES],
        omitted_closed_day_count=max(0, len(closed) - MAX_INDEXED_DATASET_DATES),
        omitted_closed_day_rows=sum(d.row_count for d in closed[MAX_INDEXED_DATASET_DATES:]),
        row_changes=tuple(changes[-MAX_ROW_CHANGES:]),
        omitted_row_changes=max(0, len(changes) - MAX_ROW_CHANGES),
        rules=rules,
        conclusion=conclusion,
    )
