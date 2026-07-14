"""Reusable data-quality audit contracts and orchestration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    computed_field,
    field_validator,
    model_validator,
)

from rquant.data_metadata import (
    DataQualityIssue,
    QualityEvidence,
    QualitySeverity,
    normalize_utc_datetime,
    stable_sha256,
    utc_now,
)
from rquant.storage.duckdb import DuckDBStore

_READ_ONLY_ACCESS_MODE = "read_only"
_WRITABLE_ACCESS_MODES = frozenset({"automatic", "read_write"})
_LIMIT_UP_POOL_REPAIR_ACTION_ID = "limit-up-pool-closed-day-cleanup/v1"
_LIMIT_UP_POOL_DATASET_ID = "limit_up_pool_daily"
_LIMIT_UP_POOL_TARGET_TABLE = "limit_up_pool_daily"
_LIMIT_UP_POOL_KEY_COLUMNS = ("ts_code", "trade_date", "source")


class QualityModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class MinuteSessionWindow(QualityModel):
    start: time
    end: time
    step_minutes: StrictInt = Field(gt=0)

    @model_validator(mode="after")
    def validate_window(self) -> MinuteSessionWindow:
        if self.start.tzinfo is not None or self.end.tzinfo is not None:
            raise ValueError("minute session windows must use local naive times")
        if any(
            value != 0
            for value in (
                self.start.second,
                self.start.microsecond,
                self.end.second,
                self.end.microsecond,
            )
        ):
            raise ValueError("minute session windows must align to whole minutes")
        start_at = datetime.combine(date.min, self.start)
        end_at = datetime.combine(date.min, self.end)
        if start_at > end_at:
            raise ValueError("minute session window start must not be after end")
        duration_minutes = int((end_at - start_at).total_seconds() // 60)
        if duration_minutes % self.step_minutes != 0:
            raise ValueError("minute session window end must align to step_minutes")
        return self

    def expected_times(self) -> tuple[time, ...]:
        current = datetime.combine(date.min, self.start)
        end_at = datetime.combine(date.min, self.end)
        values: list[time] = []
        while current <= end_at:
            values.append(current.time())
            current += timedelta(minutes=self.step_minutes)
        return tuple(values)


class MinuteSourceSessionSpec(QualityModel):
    source: str = Field(min_length=1)
    freq: str = Field(min_length=1)
    timestamp_semantics: Literal["bar_start", "bar_end", "provider_snapshot"]
    windows: tuple[MinuteSessionWindow, ...] = Field(min_length=1)
    authoritative: bool
    required_for_daily_coverage: bool
    require_full_session: bool

    @model_validator(mode="after")
    def validate_spec(self) -> MinuteSourceSessionSpec:
        if self.required_for_daily_coverage and not self.authoritative:
            raise ValueError("required_for_daily_coverage requires an authoritative source")
        if self.require_full_session and not self.authoritative:
            raise ValueError("require_full_session requires an authoritative source")
        starts = tuple(window.start for window in self.windows)
        if starts != tuple(sorted(starts)):
            raise ValueError("minute session windows must be in chronological order")
        for previous, current in zip(self.windows, self.windows[1:], strict=False):
            if current.start <= previous.end:
                raise ValueError("minute session windows must not overlap")
        expected = tuple(value for window in self.windows for value in window.expected_times())
        if len(expected) != len(set(expected)):
            raise ValueError("minute session windows must not overlap")
        return self

    def expected_times(self) -> tuple[time, ...]:
        return tuple(value for window in self.windows for value in window.expected_times())


class MinuteSessionGapSample(QualityModel):
    ts_code: str = Field(min_length=1)
    trade_date: date
    source: str = Field(min_length=1)
    freq: str = Field(min_length=1)
    timestamp_semantics: Literal["bar_start", "bar_end", "provider_snapshot"]
    missing_count: StrictInt = Field(ge=0)
    missing_times: tuple[str, ...]
    extra_count: StrictInt = Field(ge=0)
    extra_times: tuple[str, ...]


class DailyMinuteDateSample(QualityModel):
    ts_code: str = Field(min_length=1)
    trade_date: date


class RequiredMinuteCoverageSample(DailyMinuteDateSample):
    source: str = Field(min_length=1)
    freq: str = Field(min_length=1)


class MinuteWithoutDailySample(DailyMinuteDateSample):
    source: str = Field(min_length=1)
    freq: str = Field(min_length=1)
    minute_count: StrictInt = Field(gt=0)


class UnknownMinuteSemanticsSample(DailyMinuteDateSample):
    raw_source: str | None
    raw_freq: str | None
    minute_count: StrictInt = Field(gt=0)


class MinuteOverlapSample(QualityModel):
    ts_code: str = Field(min_length=1)
    trade_time: datetime
    freq: str = Field(min_length=1)
    sources: tuple[str, ...] = Field(min_length=2)
    distinct_payload_count: StrictInt = Field(gt=0)


class MinuteOverlapAuditSummary(QualityModel):
    category: Literal["exact", "conflict"]
    finding_count: StrictInt = Field(gt=0)
    samples: tuple[MinuteOverlapSample, ...]


def _display_minute_time(value: str) -> str:
    return value[:-3] if value.endswith(":00") else value


def _minute_frequency_minutes(freq: str) -> int:
    suffix = "min"
    magnitude = freq[: -len(suffix)] if freq.endswith(suffix) else ""
    if not magnitude.isdecimal() or int(magnitude) < 1:
        raise ValueError(f"minute frequency cannot be normalized: {freq}")
    return int(magnitude)


DEFAULT_MINUTE_SOURCE_SESSION_SPECS = (
    MinuteSourceSessionSpec(
        source="tushare",
        freq="1min",
        timestamp_semantics="bar_end",
        windows=(
            MinuteSessionWindow(start=time(9, 30), end=time(11, 30), step_minutes=1),
            MinuteSessionWindow(start=time(13, 1), end=time(15, 0), step_minutes=1),
        ),
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=True,
    ),
    MinuteSourceSessionSpec(
        source="tushare_rt",
        freq="1min",
        timestamp_semantics="provider_snapshot",
        windows=(
            MinuteSessionWindow(start=time(9, 30), end=time(11, 30), step_minutes=1),
            MinuteSessionWindow(start=time(13, 0), end=time(15, 0), step_minutes=1),
        ),
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    ),
    MinuteSourceSessionSpec(
        source="tushare_rt_daily",
        freq="1min",
        timestamp_semantics="provider_snapshot",
        windows=(
            MinuteSessionWindow(start=time(9, 30), end=time(11, 30), step_minutes=1),
            MinuteSessionWindow(start=time(13, 0), end=time(15, 0), step_minutes=1),
        ),
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    ),
)


class LimitUpPoolRepairKey(QualityModel):
    ts_code: str = Field(min_length=1)
    trade_date: date
    source: str = Field(min_length=1)


class LimitUpPoolRepairPlanPayload(QualityModel):
    action_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    target_table: str = Field(min_length=1)
    key_columns: tuple[str, ...] = Field(min_length=1)
    candidate_keys: tuple[LimitUpPoolRepairKey, ...]
    before_count: StrictInt = Field(ge=0)


class LimitUpPoolRepairPlan(QualityModel):
    status: Literal["ready", "blocked"]
    severity: QualitySeverity | None = None
    action_id: str = _LIMIT_UP_POOL_REPAIR_ACTION_ID
    dataset_id: str = _LIMIT_UP_POOL_DATASET_ID
    target_table: str = _LIMIT_UP_POOL_TARGET_TABLE
    key_columns: tuple[str, ...] = _LIMIT_UP_POOL_KEY_COLUMNS
    candidate_keys: tuple[LimitUpPoolRepairKey, ...] = ()
    unknown_dates: tuple[date, ...] = ()
    plan_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @computed_field
    @property
    def before_count(self) -> int:
        return len(self.candidate_keys)

    @model_validator(mode="after")
    def validate_status(self) -> LimitUpPoolRepairPlan:
        if self.status == "ready":
            if self.plan_id is None:
                raise ValueError("ready repair plan requires plan_id")
            if self.severity is not None or self.unknown_dates:
                raise ValueError("ready repair plan cannot be blocked")
        else:
            if self.plan_id is not None or self.candidate_keys:
                raise ValueError("blocked repair plan cannot be executable")
            if self.severity != "P0" or not self.unknown_dates:
                raise ValueError("blocked repair plan requires P0 unknown dates")
        return self


class LimitUpPoolRepairApplyRequest(QualityModel):
    expected_plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class DataRepairAudit(QualityModel):
    audit_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    action_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    target_table: str = Field(min_length=1)
    key_columns: tuple[str, ...] = Field(min_length=1)
    candidate_keys: tuple[LimitUpPoolRepairKey, ...]
    before_count: StrictInt = Field(ge=0)
    deleted_count: StrictInt = Field(ge=0)
    after_count: StrictInt = Field(ge=0)
    applied_at: datetime

    @field_validator("applied_at")
    @classmethod
    def validate_applied_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_counts(self) -> DataRepairAudit:
        if self.after_count > self.before_count:
            raise ValueError("after_count cannot exceed before_count")
        if self.deleted_count != self.before_count - self.after_count:
            raise ValueError("deleted_count must equal before_count - after_count")
        if self.before_count != len(self.candidate_keys):
            raise ValueError("before_count must match candidate_keys")
        return self


class LimitUpPoolRepairApplyResult(QualityModel):
    audit_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    action_id: str = Field(min_length=1)
    before_count: StrictInt = Field(ge=0)
    deleted_count: StrictInt = Field(ge=0)
    after_count: StrictInt = Field(ge=0)
    applied_at: datetime

    @field_validator("applied_at")
    @classmethod
    def validate_applied_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)


class LimitUpPoolRepairBlockedError(RuntimeError):
    plan: LimitUpPoolRepairPlan

    def __init__(self, plan: LimitUpPoolRepairPlan) -> None:
        self.plan = plan
        rendered = ", ".join(day.isoformat() for day in plan.unknown_dates)
        super().__init__(f"unknown trade calendar dates block repair: {rendered}")


class LimitUpPoolRepairPlanMismatchError(RuntimeError):
    expected_plan_id: str
    current_plan_id: str

    def __init__(self, expected_plan_id: str, current_plan_id: str) -> None:
        self.expected_plan_id = expected_plan_id
        self.current_plan_id = current_plan_id
        super().__init__(
            f"repair plan id mismatch: expected={expected_plan_id}, current={current_plan_id}"
        )


class LimitUpPoolRepairNoOpError(RuntimeError):
    plan: LimitUpPoolRepairPlan

    def __init__(self, plan: LimitUpPoolRepairPlan) -> None:
        self.plan = plan
        super().__init__("limit-up-pool repair plan has no candidates")


def _load_limit_up_pool_repair_plan(
    store: DuckDBStore,
) -> LimitUpPoolRepairPlan:
    rows = store._conn.execute(  # noqa: SLF001
        """
        SELECT pool.ts_code, pool.trade_date, pool.source, calendar.is_open
        FROM limit_up_pool_daily AS pool
        LEFT JOIN trade_calendar AS calendar
          ON calendar.exchange = 'SSE'
         AND calendar.cal_date = pool.trade_date
        ORDER BY pool.trade_date, pool.ts_code, pool.source
        """
    ).fetchall()
    unknown_dates = tuple(sorted({row[1] for row in rows if row[3] is None}))
    if unknown_dates:
        return LimitUpPoolRepairPlan(
            status="blocked",
            severity="P0",
            unknown_dates=unknown_dates,
        )

    candidate_keys = tuple(
        sorted(
            {
                LimitUpPoolRepairKey(
                    ts_code=str(row[0]),
                    trade_date=row[1],
                    source=str(row[2]),
                )
                for row in rows
                if row[3] is False
            },
            key=lambda key: (key.ts_code, key.trade_date, key.source),
        )
    )
    payload = LimitUpPoolRepairPlanPayload(
        action_id=_LIMIT_UP_POOL_REPAIR_ACTION_ID,
        dataset_id=_LIMIT_UP_POOL_DATASET_ID,
        target_table=_LIMIT_UP_POOL_TARGET_TABLE,
        key_columns=_LIMIT_UP_POOL_KEY_COLUMNS,
        candidate_keys=candidate_keys,
        before_count=len(candidate_keys),
    )
    canonical = json.dumps(
        payload.model_dump(mode="json"),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    plan_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return LimitUpPoolRepairPlan(
        status="ready",
        candidate_keys=candidate_keys,
        plan_id=plan_id,
    )


def build_limit_up_pool_closed_day_repair_plan(
    store: DuckDBStore,
) -> LimitUpPoolRepairPlan:
    """Build an executable plan from the writable target database itself."""
    _require_writable_store(store, operation="build repair plan")
    return _load_limit_up_pool_repair_plan(store)


def _delete_limit_up_pool_repair_candidates(
    store: DuckDBStore,
    candidate_keys: tuple[LimitUpPoolRepairKey, ...],
) -> tuple[LimitUpPoolRepairKey, ...]:
    if not candidate_keys:
        return ()
    placeholders = ", ".join("(?, ?, ?)" for _ in candidate_keys)
    parameters = [
        value for key in candidate_keys for value in (key.ts_code, key.trade_date, key.source)
    ]
    rows = store._conn.execute(  # noqa: SLF001
        f"""
        DELETE FROM limit_up_pool_daily
        WHERE (ts_code, trade_date, source) IN (VALUES {placeholders})
          AND EXISTS (
              SELECT 1
              FROM trade_calendar AS calendar
              WHERE calendar.exchange = 'SSE'
                AND calendar.cal_date = limit_up_pool_daily.trade_date
                AND calendar.is_open = FALSE
          )
        RETURNING ts_code, trade_date, source
        """,
        parameters,
    ).fetchall()
    return tuple(
        sorted(
            (
                LimitUpPoolRepairKey(
                    ts_code=str(row[0]),
                    trade_date=row[1],
                    source=str(row[2]),
                )
                for row in rows
            ),
            key=lambda key: (key.ts_code, key.trade_date, key.source),
        )
    )


def _insert_data_repair_audit(
    store: DuckDBStore,
    audit: DataRepairAudit,
) -> None:
    store._conn.execute(  # noqa: SLF001
        """
        INSERT INTO data_repair_audit
        (audit_id, plan_id, action_id, dataset_id, target_table,
         key_columns, candidate_keys, before_count, deleted_count,
         after_count, applied_at)
        VALUES (?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?)
        """,
        [
            audit.audit_id,
            audit.plan_id,
            audit.action_id,
            audit.dataset_id,
            audit.target_table,
            json.dumps(audit.key_columns, ensure_ascii=True),
            json.dumps(
                [key.model_dump(mode="json") for key in audit.candidate_keys],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ),
            audit.before_count,
            audit.deleted_count,
            audit.after_count,
            audit.applied_at,
        ],
    )


def _acquire_limit_up_pool_repair_write_fence(store: DuckDBStore) -> None:
    # DuckDB 1.5 optimizes `SET is_open = is_open` and takes no write conflict.
    # Two real writes restore the fact before it is read while fencing that column.
    store._conn.execute(  # noqa: SLF001
        "UPDATE trade_calendar SET is_open = NOT is_open WHERE exchange = 'SSE'"
    )
    store._conn.execute(  # noqa: SLF001
        "UPDATE trade_calendar SET is_open = NOT is_open WHERE exchange = 'SSE'"
    )
    guard = store._conn.execute(  # noqa: SLF001
        """
        UPDATE limit_up_pool_write_guard
        SET generation = generation + 1
        WHERE guard_id = 'limit_up_pool_daily'
        RETURNING generation
        """
    ).fetchone()
    if guard is None:
        raise RuntimeError("limit-up-pool write guard row is missing")


def apply_limit_up_pool_closed_day_repair(
    store: DuckDBStore,
    expected_plan_id: str,
    *,
    applied_at: datetime | None = None,
) -> LimitUpPoolRepairApplyResult:
    """CAS-apply a closed-day cleanup and its audit in one transaction."""
    _require_writable_store(store, operation="apply repair plan")
    request = LimitUpPoolRepairApplyRequest(
        expected_plan_id=expected_plan_id,
    )
    applied_time = normalize_utc_datetime(utc_now() if applied_at is None else applied_at)
    transaction_open = False
    try:
        store._conn.execute("BEGIN")  # noqa: SLF001
        transaction_open = True
        _acquire_limit_up_pool_repair_write_fence(store)
        current = _load_limit_up_pool_repair_plan(store)
        if current.status == "blocked":
            raise LimitUpPoolRepairBlockedError(current)
        if current.plan_id != request.expected_plan_id:
            assert current.plan_id is not None
            raise LimitUpPoolRepairPlanMismatchError(
                request.expected_plan_id,
                current.plan_id,
            )
        if current.before_count == 0:
            raise LimitUpPoolRepairNoOpError(current)

        deleted_keys = _delete_limit_up_pool_repair_candidates(
            store,
            current.candidate_keys,
        )
        if deleted_keys != current.candidate_keys:
            raise RuntimeError("deleted repair keys do not match the approved candidate keys")
        after = _load_limit_up_pool_repair_plan(store)
        if after.status == "blocked":
            raise LimitUpPoolRepairBlockedError(after)
        if after.before_count != 0:
            raise RuntimeError(
                f"closed-day repair after_count must be zero: after_count={after.before_count}"
            )

        audit_id = stable_sha256(
            "data_repair_audit",
            {
                "plan_id": request.expected_plan_id,
                "action_id": current.action_id,
                "applied_at": applied_time,
            },
        )
        audit = DataRepairAudit(
            audit_id=audit_id,
            plan_id=request.expected_plan_id,
            action_id=current.action_id,
            dataset_id=current.dataset_id,
            target_table=current.target_table,
            key_columns=current.key_columns,
            candidate_keys=current.candidate_keys,
            before_count=current.before_count,
            deleted_count=len(deleted_keys),
            after_count=after.before_count,
            applied_at=applied_time,
        )
        _insert_data_repair_audit(store, audit)
        result = LimitUpPoolRepairApplyResult(
            audit_id=audit.audit_id,
            plan_id=audit.plan_id,
            action_id=audit.action_id,
            before_count=audit.before_count,
            deleted_count=audit.deleted_count,
            after_count=audit.after_count,
            applied_at=audit.applied_at,
        )
        store._conn.execute("COMMIT")  # noqa: SLF001
        transaction_open = False
        return result
    except BaseException as original_error:
        if transaction_open:
            try:
                store._conn.execute("ROLLBACK")  # noqa: SLF001
            except BaseException as rollback_error:
                raise BaseExceptionGroup(
                    "repair failed and rollback failed; database state uncertain",
                    [original_error, rollback_error],
                ) from None
        raise


class AuditFinding(QualityModel):
    rule_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    severity: QualitySeverity
    scope_key: str = Field(min_length=1)
    message: str = Field(min_length=1)
    evidence: QualityEvidence = Field(default_factory=dict)

    @computed_field
    @property
    def issue_id(self) -> str:
        return stable_sha256(
            "data_quality_issue",
            {
                "rule_id": self.rule_id,
                "dataset_id": self.dataset_id,
                "scope_key": self.scope_key,
            },
        )

    @computed_field
    @property
    def is_blocking(self) -> bool:
        return self.severity == "P0"

    def to_issue(self, *, observed_at: datetime | None = None) -> DataQualityIssue:
        return DataQualityIssue.detected(
            rule_id=self.rule_id,
            dataset_id=self.dataset_id,
            severity=self.severity,
            scope_key=self.scope_key,
            message=self.message,
            evidence=self.evidence,
            observed_at=observed_at,
        )


AuditCheck = Callable[[DuckDBStore], Sequence[AuditFinding]]


class AuditRule(QualityModel):
    rule_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    severity: QualitySeverity
    description: str = Field(min_length=1)
    check: AuditCheck = Field(exclude=True, repr=False)


class AuditReport(QualityModel):
    observed_at: datetime
    rule_ids: tuple[str, ...] = Field(min_length=1)
    findings: tuple[AuditFinding, ...] = ()

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_unique_ids(self) -> AuditReport:
        _reject_duplicates(self.rule_ids, label="rule_id")
        _reject_duplicates(self.issue_ids, label="finding issue_id")
        rule_ids = set(self.rule_ids)
        for finding in self.findings:
            if finding.rule_id not in rule_ids:
                raise ValueError(f"finding rule_id is not in rule_ids: {finding.rule_id}")
        return self

    @computed_field
    @property
    def finding_count(self) -> int:
        return len(self.findings)

    @computed_field
    @property
    def is_blocked(self) -> bool:
        return any(finding.is_blocking for finding in self.findings)

    @computed_field
    @property
    def issue_ids(self) -> tuple[str, ...]:
        return tuple(finding.issue_id for finding in self.findings)


def historical_security_status_audit_rules(
    start: date,
    end: date,
    *,
    sample_limit: int = 20,
) -> tuple[AuditRule, ...]:
    """Build blocking coverage rules for daily_bar-eligible historical status."""
    if start > end:
        raise ValueError("security status audit start must not be after end")
    if sample_limit < 1:
        raise ValueError("sample_limit must be positive")

    def coverage_check(store: DuckDBStore) -> tuple[AuditFinding, ...]:
        coverage = store.stock_status_coverage(
            start,
            end,
            sample_limit=sample_limit,
        )
        categories = (
            (
                "missing",
                coverage.missing_count,
                coverage.missing_samples,
                "daily_bar eligibility keys have no historical security status",
            ),
            (
                "unknown",
                coverage.unknown_count,
                coverage.unknown_samples,
                "historical ST status is unknown",
            ),
            (
                "conflict",
                coverage.conflict_count,
                coverage.conflict_samples,
                "historical security status sources conflict",
            ),
            (
                "invalid",
                coverage.invalid_count,
                coverage.invalid_samples,
                "historical security status violates fact invariants",
            ),
        )
        return tuple(
            AuditFinding(
                rule_id="stock-status-coverage",
                dataset_id="stock_status_daily",
                severity="P0",
                scope_key=(f"{category}/{start.isoformat()}/{end.isoformat()}"),
                message=message,
                evidence={
                    "count": count,
                    "samples": [
                        f"{sample.ts_code}/{sample.trade_date.isoformat()}" for sample in samples
                    ],
                },
            )
            for category, count, samples, message in categories
            if count > 0
        )

    return (
        AuditRule(
            rule_id="stock-status-coverage",
            dataset_id="stock_status_daily",
            severity="P0",
            description="Aggregate blocking historical status coverage gaps",
            check=coverage_check,
        ),
    )


def daily_minute_consistency_audit_rules(
    start: date,
    end: date,
    *,
    source_specs: Sequence[MinuteSourceSessionSpec] = (DEFAULT_MINUTE_SOURCE_SESSION_SPECS),
    sample_limit: int = 20,
) -> tuple[AuditRule, ...]:
    """Build daily/minute consistency rules for explicitly declared sources."""
    if start > end:
        raise ValueError("daily/minute audit start must not be after end")
    if sample_limit < 1:
        raise ValueError("sample_limit must be positive")
    specs = tuple(source_specs)
    if not specs:
        raise ValueError("daily/minute audit requires source specs")
    identities = tuple((spec.source, spec.freq) for spec in specs)
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate minute source/freq spec")
    if not any(spec.authoritative for spec in specs):
        raise ValueError("daily/minute audit requires an authoritative spec")
    range_start = datetime.combine(start, time.min)
    range_end = datetime.combine(end, time.min) + timedelta(days=1)

    coverage_specs = tuple(
        spec for spec in specs if spec.authoritative and spec.required_for_daily_coverage
    )
    if not coverage_specs:
        raise ValueError("daily/minute audit requires an authoritative coverage spec")

    def minute_without_daily_check(
        store: DuckDBStore,
    ) -> tuple[AuditFinding, ...]:
        rows = store._conn.execute(  # noqa: SLF001
            """
            WITH missing_daily AS (
                SELECT
                    m.ts_code,
                    CAST(m.trade_time AS DATE) AS trade_date,
                    m.source,
                    m.freq,
                    count(*) AS minute_count
                FROM minute_bar AS m
                LEFT JOIN daily_bar AS d
                  ON d.ts_code = m.ts_code
                 AND d.trade_date = CAST(m.trade_time AS DATE)
                WHERE m.trade_time >= ?
                  AND m.trade_time < ?
                  AND d.ts_code IS NULL
                GROUP BY
                    m.ts_code,
                    CAST(m.trade_time AS DATE),
                    m.source,
                    m.freq
            )
            SELECT *, count(*) OVER () AS finding_count
            FROM missing_daily
            ORDER BY trade_date, ts_code, source, freq
            LIMIT ?
            """,
            [range_start, range_end, sample_limit],
        ).fetchall()
        if not rows:
            return ()
        samples = tuple(
            MinuteWithoutDailySample(
                ts_code=ts_code,
                trade_date=trade_date_value,
                source=source,
                freq=freq,
                minute_count=minute_count,
            )
            for (
                ts_code,
                trade_date_value,
                source,
                freq,
                minute_count,
                _finding_count,
            ) in rows
        )
        return (
            AuditFinding(
                rule_id="minute-without-daily",
                dataset_id="minute_bar",
                severity="P1",
                scope_key=f"{start.isoformat()}/{end.isoformat()}",
                message="Minute bars exist without a matching daily bar",
                evidence={
                    "count": rows[0][-1],
                    "samples": [sample.model_dump(mode="json") for sample in samples],
                },
            ),
        )

    def eligible_daily_without_minute_check(
        store: DuckDBStore,
    ) -> tuple[AuditFinding, ...]:
        coverage_values = ", ".join("(?, ?)" for _spec in coverage_specs)
        parameters: list[object] = [
            value for spec in coverage_specs for value in (spec.source, spec.freq)
        ]
        parameters.extend((start, end))
        rows = store._conn.execute(  # noqa: SLF001
            f"""
            WITH required_source(source, freq) AS (
                VALUES {coverage_values}
            ),
            eligible_daily AS (
                SELECT d.ts_code, d.trade_date
                FROM daily_bar AS d
                WHERE d.trade_date BETWEEN ? AND ?
                  AND (
                        coalesce(d.vol, 0) > 0
                     OR coalesce(d.amount, 0) > 0
                  )
            ),
            missing_required AS (
                SELECT
                    r.source,
                    r.freq,
                    d.ts_code,
                    d.trade_date
                FROM eligible_daily AS d
                CROSS JOIN required_source AS r
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM minute_bar AS m
                    WHERE m.ts_code = d.ts_code
                      AND m.source = r.source
                      AND m.freq = r.freq
                      AND m.trade_time >= CAST(d.trade_date AS TIMESTAMP)
                      AND m.trade_time < CAST(d.trade_date AS TIMESTAMP)
                                         + INTERVAL 1 DAY
                )
            ),
            ranked AS (
                SELECT
                    *,
                    count(*) OVER (
                        PARTITION BY source, freq
                    ) AS finding_count,
                    row_number() OVER (
                        PARTITION BY source, freq
                        ORDER BY trade_date, ts_code
                    ) AS sample_rank
                FROM missing_required
            )
            SELECT source, freq, ts_code, trade_date, finding_count
            FROM ranked
            WHERE sample_rank <= ?
            ORDER BY source, freq, trade_date, ts_code
            """,
            [*parameters, sample_limit],
        ).fetchall()
        if not rows:
            return ()
        grouped: dict[
            tuple[str, str],
            tuple[int, list[RequiredMinuteCoverageSample]],
        ] = {}
        for source, freq, ts_code, trade_date_value, finding_count in rows:
            key = (source, freq)
            if key not in grouped:
                grouped[key] = (finding_count, [])
            grouped[key][1].append(
                RequiredMinuteCoverageSample(
                    ts_code=ts_code,
                    trade_date=trade_date_value,
                    source=source,
                    freq=freq,
                )
            )
        return tuple(
            AuditFinding(
                rule_id="eligible-daily-without-authoritative-minute",
                dataset_id="minute_bar",
                severity="P1",
                scope_key=(f"{start.isoformat()}/{end.isoformat()}/{spec.source}/{spec.freq}"),
                message=(
                    "Eligible daily bars have no minute bars from required "
                    f"authoritative source {spec.source}/{spec.freq}"
                ),
                evidence={
                    "count": grouped[(spec.source, spec.freq)][0],
                    "samples": [
                        sample.model_dump(mode="json")
                        for sample in grouped[(spec.source, spec.freq)][1]
                    ],
                },
            )
            for spec in coverage_specs
            if (spec.source, spec.freq) in grouped
        )

    declared_parameters: list[object] = [
        value for spec in specs for value in (spec.source, spec.freq)
    ]

    def unknown_semantics_check(
        store: DuckDBStore,
    ) -> tuple[AuditFinding, ...]:
        parameters = [*declared_parameters, range_start, range_end]
        declared_values_with_marker = ", ".join("(?, ?, TRUE)" for _spec in specs)
        rows = store._conn.execute(  # noqa: SLF001
            f"""
            WITH declared(source, freq, is_declared) AS (
                VALUES {declared_values_with_marker}
            ),
            unknown_rows AS (
                SELECT
                    m.ts_code,
                    CAST(m.trade_time AS DATE) AS trade_date,
                    m.source,
                    m.freq,
                    count(*) AS minute_count
                FROM minute_bar AS m
                LEFT JOIN declared AS d
                  ON d.source IS NOT DISTINCT FROM m.source
                 AND d.freq IS NOT DISTINCT FROM m.freq
                WHERE m.trade_time >= ?
                  AND m.trade_time < ?
                  AND d.is_declared IS NULL
                GROUP BY
                    m.ts_code,
                    CAST(m.trade_time AS DATE),
                    m.source,
                    m.freq
            )
            SELECT *, count(*) OVER () AS finding_count
            FROM unknown_rows
            ORDER BY trade_date, ts_code, source, freq
            LIMIT ?
            """,
            [*parameters, sample_limit],
        ).fetchall()
        if not rows:
            return ()
        samples = tuple(
            UnknownMinuteSemanticsSample(
                ts_code=ts_code,
                trade_date=trade_date_value,
                raw_source=source,
                raw_freq=freq,
                minute_count=minute_count,
            )
            for (
                ts_code,
                trade_date_value,
                source,
                freq,
                minute_count,
                _finding_count,
            ) in rows
        )
        return (
            AuditFinding(
                rule_id="unknown-source-or-freq-semantics",
                dataset_id="minute_bar",
                severity="P0",
                scope_key=f"{start.isoformat()}/{end.isoformat()}",
                message=(
                    "Minute bars use a source/frequency pair with undeclared timestamp semantics"
                ),
                evidence={
                    "count": rows[0][-1],
                    "samples": [sample.model_dump(mode="json") for sample in samples],
                },
            ),
        )

    overlap_cache_store: DuckDBStore | None = None
    overlap_cache: tuple[MinuteOverlapAuditSummary, ...] = ()
    overlap_declared_values = ", ".join("(?, ?, ?, ?)" for _spec in specs)
    overlap_declared_parameters: list[object] = [
        value
        for spec in specs
        for value in (
            spec.source,
            spec.freq,
            spec.timestamp_semantics,
            _minute_frequency_minutes(spec.freq),
        )
    ]

    def load_overlap_summaries(
        store: DuckDBStore,
    ) -> tuple[MinuteOverlapAuditSummary, ...]:
        nonlocal overlap_cache_store, overlap_cache
        if overlap_cache_store is store:
            return overlap_cache
        rows = store._conn.execute(  # noqa: SLF001
            f"""
            WITH declared(source, freq, timestamp_semantics, bar_minutes) AS (
                VALUES {overlap_declared_values}
            ),
            normalized_rows AS (
                SELECT
                    m.*,
                    CASE
                        WHEN d.timestamp_semantics = 'bar_end'
                            THEN m.trade_time - d.bar_minutes * INTERVAL 1 MINUTE
                        ELSE m.trade_time
                    END AS logical_bar_start
                FROM minute_bar AS m
                JOIN declared AS d
                  ON d.source = m.source
                 AND d.freq = m.freq
                WHERE m.trade_time >= ?
                  AND m.trade_time < ?
            ),
            overlap_rows AS (
                SELECT
                    m.ts_code,
                    m.logical_bar_start AS trade_time,
                    m.freq,
                    list(DISTINCT m.source ORDER BY m.source) AS sources,
                    count(DISTINCT struct_pack(
                        open_value := m.open,
                        high_value := m.high,
                        low_value := m.low,
                        close_value := m.close,
                        vol_value := m.vol,
                        amount_value := m.amount
                    )) AS distinct_payload_count
                FROM normalized_rows AS m
                GROUP BY m.ts_code, m.logical_bar_start, m.freq
                HAVING count(DISTINCT m.source) > 1
            ),
            classified AS (
                SELECT
                    *,
                    CASE
                        WHEN distinct_payload_count = 1 THEN 'exact'
                        ELSE 'conflict'
                    END AS category
                FROM overlap_rows
            ),
            ranked AS (
                SELECT
                    *,
                    count(*) OVER (
                        PARTITION BY category
                    ) AS finding_count,
                    row_number() OVER (
                        PARTITION BY category
                        ORDER BY trade_time, ts_code, freq
                    ) AS sample_rank
                FROM classified
            )
            SELECT
                category,
                ts_code,
                trade_time,
                freq,
                sources,
                distinct_payload_count,
                finding_count
            FROM ranked
            WHERE sample_rank <= ?
            ORDER BY category, trade_time, ts_code, freq
            """,
            [
                *overlap_declared_parameters,
                range_start,
                range_end,
                sample_limit,
            ],
        ).fetchall()
        grouped: dict[
            Literal["exact", "conflict"],
            tuple[int, list[MinuteOverlapSample]],
        ] = {}
        for (
            category,
            ts_code,
            trade_time_value,
            freq,
            sources,
            distinct_payload_count,
            finding_count,
        ) in rows:
            if category not in grouped:
                grouped[category] = (finding_count, [])
            grouped[category][1].append(
                MinuteOverlapSample(
                    ts_code=ts_code,
                    trade_time=trade_time_value,
                    freq=freq,
                    sources=tuple(sources),
                    distinct_payload_count=distinct_payload_count,
                )
            )
        overlap_cache_store = store
        overlap_cache = tuple(
            MinuteOverlapAuditSummary(
                category=category,
                finding_count=grouped[category][0],
                samples=tuple(grouped[category][1]),
            )
            for category in ("exact", "conflict")
            if category in grouped
        )
        return overlap_cache

    def make_overlap_check(
        *,
        rule_id: Literal[
            "cross-source-exact-overlap",
            "cross-source-conflicting-overlap",
        ],
        severity: Literal["P2", "P3"],
        exact: bool,
    ) -> AuditCheck:
        def check(store: DuckDBStore) -> tuple[AuditFinding, ...]:
            category = "exact" if exact else "conflict"
            summary = next(
                (item for item in load_overlap_summaries(store) if item.category == category),
                None,
            )
            if summary is None:
                return ()
            return (
                AuditFinding(
                    rule_id=rule_id,
                    dataset_id="minute_bar",
                    severity=severity,
                    scope_key=f"{start.isoformat()}/{end.isoformat()}",
                    message=(
                        "Multiple sources contain identical logical minute bars"
                        if exact
                        else ("Multiple sources disagree on OHLCV for the same logical minute bar")
                    ),
                    evidence={
                        "count": summary.finding_count,
                        "samples": [sample.model_dump(mode="json") for sample in summary.samples],
                    },
                ),
            )

        return check

    def no_findings(_store: DuckDBStore) -> tuple[AuditFinding, ...]:
        return ()

    full_session_specs = tuple(
        spec for spec in specs if spec.authoritative and spec.require_full_session
    )
    full_session_by_identity = {(spec.source, spec.freq): spec for spec in full_session_specs}
    session_cache_store: DuckDBStore | None = None
    session_cache: tuple[AuditFinding, ...] = ()

    def session_check(store: DuckDBStore) -> tuple[AuditFinding, ...]:
        nonlocal session_cache_store, session_cache
        if session_cache_store is store:
            return session_cache
        if not full_session_specs:
            session_cache_store = store
            session_cache = ()
            return session_cache
        expected_grid_rows = tuple(
            (spec.source, spec.freq, value.isoformat(timespec="seconds"))
            for spec in full_session_specs
            for value in spec.expected_times()
        )
        expected_grid_values = ", ".join("(?, ?, ?)" for _row in expected_grid_rows)
        expected_grid_parameters: list[object] = [
            value for row in expected_grid_rows for value in row
        ]
        rows = store._conn.execute(  # noqa: SLF001
            f"""
            WITH expected_grid(source, freq, expected_time) AS (
                VALUES {expected_grid_values}
            ),
            expected_sessions AS (
                SELECT
                    source,
                    freq,
                    list(expected_time ORDER BY expected_time) AS expected_times,
                    count(*) AS expected_count
                FROM expected_grid
                GROUP BY source, freq
            ),
            sessions AS (
                SELECT
                    m.source,
                    m.freq,
                    m.ts_code,
                    CAST(m.trade_time AS DATE) AS trade_date,
                    list(
                        DISTINCT strftime(m.trade_time, '%H:%M:%S')
                        ORDER BY strftime(m.trade_time, '%H:%M:%S')
                    ) AS actual_times
                FROM minute_bar AS m
                JOIN expected_sessions AS e
                  ON e.source = m.source
                 AND e.freq = m.freq
                WHERE m.trade_time >= ?
                  AND m.trade_time < ?
                GROUP BY
                    m.source,
                    m.freq,
                    m.ts_code,
                    CAST(m.trade_time AS DATE)
            ),
            mismatches AS (
                SELECT
                    s.source,
                    s.freq,
                    s.ts_code,
                    s.trade_date,
                    s.actual_times,
                    e.expected_times,
                    e.expected_count
                FROM sessions AS s
                JOIN expected_sessions AS e
                  ON e.source = s.source
                 AND e.freq = s.freq
                WHERE actual_times IS DISTINCT FROM expected_times
            ),
            ranked AS (
                SELECT
                    *,
                    count(*) OVER (
                        PARTITION BY source, freq
                    ) AS finding_count,
                    row_number() OVER (
                        PARTITION BY source, freq
                        ORDER BY trade_date, ts_code
                    ) AS sample_rank
                FROM mismatches
            )
            SELECT
                source,
                freq,
                ts_code,
                trade_date,
                actual_times,
                expected_times,
                expected_count,
                finding_count
            FROM ranked
            WHERE sample_rank <= ?
            ORDER BY source, freq, trade_date, ts_code
            """,
            [
                *expected_grid_parameters,
                range_start,
                range_end,
                sample_limit,
            ],
        ).fetchall()
        grouped: dict[
            tuple[str, str],
            tuple[int, list[MinuteSessionGapSample]],
        ] = {}
        for (
            source,
            freq,
            ts_code,
            trade_date_value,
            actual_values,
            expected_values,
            _expected_count,
            finding_count,
        ) in rows:
            spec = full_session_by_identity[(source, freq)]
            expected = set(expected_values)
            actual = set(actual_values)
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            identity = (source, freq)
            if identity not in grouped:
                grouped[identity] = (finding_count, [])
            grouped[identity][1].append(
                MinuteSessionGapSample(
                    ts_code=ts_code,
                    trade_date=trade_date_value,
                    source=source,
                    freq=freq,
                    timestamp_semantics=spec.timestamp_semantics,
                    missing_count=len(missing),
                    missing_times=tuple(_display_minute_time(value) for value in missing),
                    extra_count=len(extra),
                    extra_times=tuple(_display_minute_time(value) for value in extra),
                )
            )
        session_cache_store = store
        session_cache = tuple(
            AuditFinding(
                rule_id="incomplete-authoritative-session",
                dataset_id="minute_bar",
                severity="P1",
                scope_key=(f"{start.isoformat()}/{end.isoformat()}/{spec.source}/{spec.freq}"),
                message=(
                    "Authoritative minute sessions do not match their configured timestamp grid"
                ),
                evidence={
                    "count": grouped[(spec.source, spec.freq)][0],
                    "expected_count": len(spec.expected_times()),
                    "samples": [
                        sample.model_dump(mode="json")
                        for sample in grouped[(spec.source, spec.freq)][1]
                    ],
                },
            )
            for spec in full_session_specs
            if (spec.source, spec.freq) in grouped
        )
        return session_cache

    definitions = (
        (
            "minute-without-daily",
            "P1",
            "Minute bars exist without a matching daily bar",
        ),
        (
            "eligible-daily-without-authoritative-minute",
            "P1",
            "Eligible daily bars have no authoritative minute bars",
        ),
        (
            "incomplete-authoritative-session",
            "P1",
            "Authoritative minute sessions do not match their configured grid",
        ),
        (
            "unknown-source-or-freq-semantics",
            "P0",
            "Minute bars use an undeclared source/frequency semantic",
        ),
        (
            "cross-source-exact-overlap",
            "P3",
            "Multiple sources contain the same logical minute bar",
        ),
        (
            "cross-source-conflicting-overlap",
            "P2",
            "Multiple sources disagree on the same logical minute bar",
        ),
    )
    checks: dict[str, AuditCheck] = {
        "minute-without-daily": minute_without_daily_check,
        "eligible-daily-without-authoritative-minute": (eligible_daily_without_minute_check),
        "incomplete-authoritative-session": session_check,
        "unknown-source-or-freq-semantics": unknown_semantics_check,
        "cross-source-exact-overlap": make_overlap_check(
            rule_id="cross-source-exact-overlap",
            severity="P3",
            exact=True,
        ),
        "cross-source-conflicting-overlap": make_overlap_check(
            rule_id="cross-source-conflicting-overlap",
            severity="P2",
            exact=False,
        ),
    }
    return tuple(
        AuditRule(
            rule_id=rule_id,
            dataset_id="minute_bar",
            severity=severity,
            description=description,
            check=checks.get(rule_id, no_findings),
        )
        for rule_id, severity, description in definitions
    )


def run_audit(
    store: DuckDBStore,
    rules: Sequence[AuditRule],
    *,
    observed_at: datetime | None = None,
) -> AuditReport:
    _require_read_only_store(store, operation="run_audit")
    rule_ids = tuple(rule.rule_id for rule in rules)
    _reject_duplicates(rule_ids, label="rule_id")
    audit_time = normalize_utc_datetime(utc_now() if observed_at is None else observed_at)
    findings: list[AuditFinding] = []
    finding_ids: set[str] = set()
    for rule in rules:
        for finding in rule.check(store):
            if (
                finding.rule_id,
                finding.dataset_id,
                finding.severity,
            ) != (rule.rule_id, rule.dataset_id, rule.severity):
                raise ValueError(f"audit finding does not match rule identity: {rule.rule_id}")
            if finding.issue_id in finding_ids:
                raise ValueError(f"duplicate finding issue_id: {finding.issue_id}")
            finding_ids.add(finding.issue_id)
            findings.append(finding)
    return AuditReport(
        observed_at=audit_time,
        rule_ids=rule_ids,
        findings=tuple(findings),
    )


def record_audit_report(
    store: DuckDBStore,
    report: AuditReport,
) -> tuple[DataQualityIssue, ...]:
    _require_writable_store(store, operation="record_audit_report")
    return tuple(
        store.record_data_quality_issue(finding.to_issue(observed_at=report.observed_at))
        for finding in report.findings
    )


def resolve_audit_issues(
    store: DuckDBStore,
    issue_ids: Sequence[str],
    *,
    resolved_at: datetime | None = None,
) -> tuple[DataQualityIssue, ...]:
    _require_writable_store(store, operation="resolve_audit_issues")
    return tuple(
        store.resolve_data_quality_issue(issue_id, resolved_at=resolved_at)
        for issue_id in dict.fromkeys(issue_ids)
    )


RepairCount = Callable[[DuckDBStore], int]
RepairMutation = Callable[[DuckDBStore], None]


class RepairAction(QualityModel):
    action_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    count_affected: RepairCount = Field(exclude=True, repr=False)
    apply: RepairMutation = Field(exclude=True, repr=False)


class RepairReport(QualityModel):
    action_id: str = Field(min_length=1)
    dry_run: bool
    before_count: StrictInt = Field(ge=0)
    after_count: StrictInt | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_counts_for_mode(self) -> RepairReport:
        if self.dry_run and self.after_count is not None:
            raise ValueError("dry-run repair cannot have after_count")
        if not self.dry_run and self.after_count is None:
            raise ValueError("applied repair requires after_count")
        if self.after_count is not None and self.after_count > self.before_count:
            raise ValueError("after_count cannot exceed before_count")
        return self

    @computed_field
    @property
    def changed_count(self) -> int | None:
        if self.after_count is None:
            return None
        return self.before_count - self.after_count


def run_repair(
    store: DuckDBStore,
    action: RepairAction,
    *,
    dry_run: bool = True,
) -> RepairReport:
    """Run a repair; callbacks must not manage DuckDB transactions themselves."""
    if dry_run:
        _require_read_only_store(store, operation="run_repair dry-run")
        before_count = _validate_count(
            "before_count",
            action.count_affected(store),
        )
        return RepairReport(
            action_id=action.action_id,
            dry_run=True,
            before_count=before_count,
        )

    _require_writable_store(store, operation="run_repair apply")
    transaction_open = False
    try:
        store._conn.execute("BEGIN")  # noqa: SLF001
        transaction_open = True
        before_count = _validate_count(
            "before_count",
            action.count_affected(store),
        )
        action.apply(store)
        after_count = _validate_count(
            "after_count",
            action.count_affected(store),
        )
        if after_count > before_count:
            raise ValueError("after_count cannot exceed before_count")
        report = RepairReport(
            action_id=action.action_id,
            dry_run=False,
            before_count=before_count,
            after_count=after_count,
        )
        store._conn.execute("COMMIT")  # noqa: SLF001
        transaction_open = False
        return report
    except BaseException as original_error:
        if transaction_open:
            try:
                store._conn.execute("ROLLBACK")  # noqa: SLF001
            except BaseException as rollback_error:
                transaction_open = False
                raise BaseExceptionGroup(
                    "repair failed and rollback failed; database state uncertain",
                    [original_error, rollback_error],
                ) from None
            transaction_open = False
        raise


def _duckdb_access_mode(store: DuckDBStore) -> str:
    try:
        row = store._conn.execute(  # noqa: SLF001
            "SELECT current_setting('access_mode')"
        ).fetchone()
    except Exception as exc:
        raise ValueError("cannot verify DuckDBStore access_mode") from exc
    if row is None or not isinstance(row[0], str):
        raise ValueError("cannot verify DuckDBStore access_mode")
    return row[0].lower()


def _require_read_only_store(store: DuckDBStore, *, operation: str) -> None:
    access_mode = _duckdb_access_mode(store)
    if access_mode != _READ_ONLY_ACCESS_MODE:
        raise ValueError(f"{operation} requires a read-only DuckDBStore; access_mode={access_mode}")


def _require_writable_store(store: DuckDBStore, *, operation: str) -> None:
    access_mode = _duckdb_access_mode(store)
    if access_mode not in _WRITABLE_ACCESS_MODES:
        raise ValueError(f"{operation} requires a writable DuckDBStore; access_mode={access_mode}")


def _reject_duplicates(values: Sequence[str], *, label: str) -> None:
    seen: set[str] = set()
    for value in values:
        if value in seen:
            raise ValueError(f"duplicate {label}: {value}")
        seen.add(value)


def _validate_count(name: str, value: object) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be a strict int")
    if value < 0:
        raise ValueError(f"{name} cannot be negative")
    return value
