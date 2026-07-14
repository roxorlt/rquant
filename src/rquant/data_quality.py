"""Reusable data-quality audit contracts and orchestration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from datetime import date, datetime
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
            "repair plan id mismatch: "
            f"expected={expected_plan_id}, current={current_plan_id}"
        )


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
    unknown_dates = tuple(
        sorted({row[1] for row in rows if row[3] is None})
    )
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
        value
        for key in candidate_keys
        for value in (key.ts_code, key.trade_date, key.source)
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
    applied_time = normalize_utc_datetime(
        utc_now() if applied_at is None else applied_at
    )
    transaction_open = False
    try:
        store._conn.execute("BEGIN")  # noqa: SLF001
        transaction_open = True
        current = _load_limit_up_pool_repair_plan(store)
        if current.status == "blocked":
            raise LimitUpPoolRepairBlockedError(current)
        if current.plan_id != request.expected_plan_id:
            assert current.plan_id is not None
            raise LimitUpPoolRepairPlanMismatchError(
                request.expected_plan_id,
                current.plan_id,
            )

        deleted_keys = _delete_limit_up_pool_repair_candidates(
            store,
            current.candidate_keys,
        )
        if deleted_keys != current.candidate_keys:
            raise RuntimeError(
                "deleted repair keys do not match the approved candidate keys"
            )
        after = _load_limit_up_pool_repair_plan(store)
        if after.status == "blocked":
            raise LimitUpPoolRepairBlockedError(after)
        if after.before_count != 0:
            raise RuntimeError(
                "closed-day repair after_count must be zero: "
                f"after_count={after.before_count}"
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
                raise ValueError(
                    f"finding rule_id is not in rule_ids: {finding.rule_id}"
                )
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
                scope_key=(
                    f"{category}/{start.isoformat()}/{end.isoformat()}"
                ),
                message=message,
                evidence={
                    "count": count,
                    "samples": [
                        f"{sample.ts_code}/{sample.trade_date.isoformat()}"
                        for sample in samples
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
                raise ValueError(
                    f"audit finding does not match rule identity: {rule.rule_id}"
                )
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
        store.record_data_quality_issue(
            finding.to_issue(observed_at=report.observed_at)
        )
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
        raise ValueError(
            f"{operation} requires a read-only DuckDBStore; "
            f"access_mode={access_mode}"
        )


def _require_writable_store(store: DuckDBStore, *, operation: str) -> None:
    access_mode = _duckdb_access_mode(store)
    if access_mode not in _WRITABLE_ACCESS_MODES:
        raise ValueError(
            f"{operation} requires a writable DuckDBStore; "
            f"access_mode={access_mode}"
        )


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
