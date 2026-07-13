"""Reusable data-quality audit contracts and orchestration."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime

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


class QualityModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


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
