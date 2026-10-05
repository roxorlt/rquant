"""Bounded, content-addressed offline daily-bar audit reports."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Annotated, BinaryIO, Literal, Self

import duckdb
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from rquant.data_audit_contracts import MAX_REPORT_ISSUES
from rquant.data_audit_coverage import (
    ClosedDayRows,
    DailyBarCoverageReport,
    MonthlyCoverage,
    TradingDayGap,
)
from rquant.data_audit_datasets import DatasetAuditResult
from rquant.data_audit_evidence import (
    MAX_AUDIT_DAYS,
    DailyBarNullFieldSpec,
    audit_daily_bar_coverage_from_connection,
    audit_daily_bar_quality_from_connection,
)
from rquant.data_audit_quality import DailyBarQualityIssue

MAX_REPORT_BYTES = 8_000_000
_MAX_NULL_FIELDS = 9
_REPORT_VERSION = 1
_RULE_VERSION = "daily-bar-quality-v1"
_RULE_CLOSE = "daily_bar.close_limit"
_RULE_VOLUME = "daily_bar.zero_volume"
_RULE_NULL = "daily_bar.field_null_ratio"
_RuleKey = tuple[str, str | None]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class AuditReplicaFileIdentity(_Contract):
    """O(1) file evidence, not a verified production data generation."""

    device: int = Field(ge=0, strict=True)
    inode: int = Field(ge=0, strict=True)
    size: int = Field(gt=0, strict=True)
    mtime_ns: int = Field(gt=0, strict=True)
    ctime_ns: int = Field(gt=0, strict=True)


class DataAuditReplicaChangedError(ValueError):
    """The submitted read-only source cannot be used for this report."""


class AuditReportSource(_Contract):
    """A caller label is not a verified production replica generation."""

    mode: Literal["synthetic_test", "production_unverified"]
    namespace: Literal["test", "production"]
    snapshot_label: str = Field(min_length=1, max_length=128)
    replica_generation_id: str | None = Field(default=None, max_length=128)
    source_generation_id: str | None = Field(default=None, max_length=128)
    source_content_hash: Sha256 | None = None
    daily_run_id: str | None = Field(default=None, max_length=128)
    ingestion_commit_id: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        if not self.snapshot_label.strip():
            raise ValueError("snapshot_label must be non-empty")
        if self.mode == "synthetic_test":
            if self.namespace != "test":
                raise ValueError("synthetic report must use the test namespace")
            if (
                self.replica_generation_id is None
                or not self.replica_generation_id.startswith("test:")
                or not self.replica_generation_id.removeprefix("test:").strip()
            ):
                raise ValueError("synthetic replica identity must start with test:")
        elif self.namespace != "production" or any(
            value is not None
            for value in (
                self.replica_generation_id,
                self.source_generation_id,
                self.source_content_hash,
                self.daily_run_id,
                self.ingestion_commit_id,
            )
        ):
            raise ValueError(
                "unverified production evidence cannot claim a completed identity chain"
            )
        return self


class RuleUnassessedDay(_Contract):
    day: date
    reason: Literal[
        "no_daily_bar",
        "close_missing",
        "limits_unavailable",
        "volume_missing",
        "suspension_unknown",
        "no_observations",
    ]
    affected_rows: int = Field(ge=0, le=10_000, strict=True)


class CoverageFacts(_Contract):
    """Observed date presence without claiming collection completion."""

    snapshot_label: str = Field(min_length=1, max_length=128)
    exchange: Literal["SSE"]
    audit_start: date
    observed_through: date
    monthly: tuple[MonthlyCoverage, ...] = Field(max_length=122)
    gaps: tuple[TradingDayGap, ...] = Field(max_length=MAX_AUDIT_DAYS)
    closed_day_rows: tuple[ClosedDayRows, ...] = Field(max_length=MAX_AUDIT_DAYS)


class QualityRuleEvidence(_Contract):
    rule_id: Literal["daily_bar.close_limit", "daily_bar.zero_volume", "daily_bar.field_null_ratio"]
    field_name: str | None = Field(default=None, max_length=32)
    expected_days: tuple[date, ...] = Field(max_length=MAX_AUDIT_DAYS)
    checked_days: tuple[date, ...] = Field(max_length=MAX_AUDIT_DAYS)
    assessed_days: tuple[date, ...] = Field(max_length=MAX_AUDIT_DAYS)
    unassessed: tuple[RuleUnassessedDay, ...] = Field(max_length=MAX_AUDIT_DAYS * 4)
    issue_count: int = Field(ge=0, le=MAX_REPORT_ISSUES, strict=True)

    @model_validator(mode="after")
    def validate_day_sets(self) -> Self:
        if (self.rule_id == _RULE_NULL) != (self.field_name is not None):
            raise ValueError("NULL ratio rule requires one field name")
        for name, days in (
            ("expected", self.expected_days),
            ("checked", self.checked_days),
            ("assessed", self.assessed_days),
        ):
            if tuple(sorted(set(days))) != days:
                raise ValueError(f"{name} dates must be unique and increasing")
        expected, checked, assessed = (
            set(self.expected_days),
            set(self.checked_days),
            set(self.assessed_days),
        )
        if not checked <= expected or not assessed <= checked:
            raise ValueError("checked and assessed dates must belong to expected dates")
        unassessed: set[date] = set()
        reasons: set[tuple[date, str]] = set()
        previous_reason: tuple[date, str] | None = None
        for item in self.unassessed:
            if item.day not in expected or (item.day, item.reason) in reasons:
                raise ValueError("unassessed dates and reasons must be unique expected dates")
            if previous_reason is not None and (item.day, item.reason) <= previous_reason:
                raise ValueError("unassessed dates and reasons must be increasing")
            if item.reason == "no_daily_bar":
                if item.day in checked or item.affected_rows != 0:
                    raise ValueError("no_daily_bar means no quality check ran")
            elif item.day not in checked or item.affected_rows == 0:
                raise ValueError("quality fact gaps need a checked date and affected rows")
            reasons.add((item.day, item.reason))
            previous_reason = (item.day, item.reason)
            unassessed.add(item.day)
        if assessed & unassessed or expected != assessed | unassessed:
            raise ValueError("every expected date must be assessed or explicitly unassessed")
        return self


class _ReportBody(_Contract):
    schema_version: Literal[1] = _REPORT_VERSION
    rule_version: Literal["daily-bar-quality-v1"] = _RULE_VERSION
    run_status: Literal["completed"] = "completed"
    collection_status: Literal["collection_unconfirmed"] = "collection_unconfirmed"
    collection_completed_through: None = None
    coverage_conclusion: Literal["unconfirmed"] = "unconfirmed"
    quality_conclusion: Literal["not_fully_assessed", "issues_observed", "no_issues_observed"]
    source: AuditReportSource
    audit_start: date
    observed_through: date
    null_fields: tuple[DailyBarNullFieldSpec, ...] = Field(
        min_length=1, max_length=_MAX_NULL_FIELDS
    )
    coverage: CoverageFacts
    quality_rules: tuple[QualityRuleEvidence, ...] = Field(
        min_length=3, max_length=2 + _MAX_NULL_FIELDS
    )
    issues: tuple[DailyBarQualityIssue, ...] = Field(max_length=MAX_REPORT_ISSUES)

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.audit_start > self.observed_through:
            raise ValueError("report range is reversed")
        if (self.observed_through - self.audit_start).days + 1 > MAX_AUDIT_DAYS:
            raise ValueError("report range exceeds audit bound")
        if (
            self.coverage.snapshot_label != self.source.snapshot_label
            or self.coverage.audit_start != self.audit_start
            or self.coverage.observed_through != self.observed_through
        ):
            raise ValueError("coverage source or range disagrees with report")
        fields = [field.field_name for field in self.null_fields]
        if fields != sorted(set(fields)):
            raise ValueError("NULL field specs must be unique and sorted")
        keys = [(rule.rule_id, rule.field_name) for rule in self.quality_rules]
        required = [(_RULE_CLOSE, None), (_RULE_VOLUME, None)] + [
            (_RULE_NULL, name) for name in fields
        ]
        if keys != required:
            raise ValueError("quality rule set differs from selected rules")
        expected = self.quality_rules[0].expected_days
        if not expected or expected[-1] != self.observed_through:
            raise ValueError("quality range lacks its final open SSE day")
        if any(
            rule.expected_days != expected
            or any(
                not self.audit_start <= day <= self.observed_through for day in rule.expected_days
            )
            for rule in self.quality_rules
        ):
            raise ValueError("quality rules disagree on expected dates or range")
        expected_months: list[date] = []
        month = self.audit_start.replace(day=1)
        last_month = self.observed_through.replace(day=1)
        while month <= last_month:
            expected_months.append(month)
            month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
        if [item.month for item in self.coverage.monthly] != expected_months:
            raise ValueError("monthly coverage is incomplete or duplicated")
        missing = {
            day
            for day in expected
            if any(gap.start <= day <= gap.end for gap in self.coverage.gaps)
        }
        if any(
            gap.start not in expected
            or gap.end not in expected
            or gap.start > gap.end
            or sum(gap.start <= day <= gap.end for day in expected) != gap.missing_open_days
            for gap in self.coverage.gaps
        ) or len(missing) != sum(gap.missing_open_days for gap in self.coverage.gaps):
            raise ValueError("coverage gaps disagree with open SSE dates")
        expected_gap_days: list[tuple[date, date, int]] = []
        gap_start: date | None = None
        gap_end: date | None = None
        gap_count = 0
        for day in expected:
            if day in missing:
                gap_start = day if gap_start is None else gap_start
                gap_end = day
                gap_count += 1
            elif gap_start is not None:
                assert gap_end is not None
                expected_gap_days.append((gap_start, gap_end, gap_count))
                gap_start = gap_end = None
                gap_count = 0
        if gap_start is not None:
            assert gap_end is not None
            expected_gap_days.append((gap_start, gap_end, gap_count))
        if [(gap.start, gap.end, gap.missing_open_days) for gap in self.coverage.gaps] != (
            expected_gap_days
        ):
            raise ValueError("coverage gap sequence disagrees with open SSE dates")
        for monthly in self.coverage.monthly:
            open_days = sum(
                day.year == monthly.month.year and day.month == monthly.month.month
                for day in expected
            )
            covered_days = sum(
                day.year == monthly.month.year
                and day.month == monthly.month.month
                and day not in missing
                for day in expected
            )
            ratio = (
                (Decimal(covered_days) / Decimal(open_days)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )
                if open_days
                else None
            )
            if (
                monthly.expected_open_days != open_days
                or monthly.covered_open_days != covered_days
                or monthly.coverage_ratio != ratio
                or monthly.status != ("measured" if open_days else "no_expected_sessions")
            ):
                raise ValueError("monthly coverage counts disagree with open SSE dates")
        dirty_dates = [item.day for item in self.coverage.closed_day_rows]
        if dirty_dates != sorted(set(dirty_dates)) or any(
            not self.audit_start <= day <= self.observed_through or day in expected
            for day in dirty_dates
        ):
            raise ValueError("closed-day rows contain duplicate or open dates")
        expected_checked = tuple(day for day in expected if day not in missing)
        for rule in self.quality_rules:
            if (
                rule.checked_days != expected_checked
                or {item.day for item in rule.unassessed if item.reason == "no_daily_bar"}
                != missing
            ):
                raise ValueError("quality checked dates disagree with daily-bar coverage")
        issue_counts: dict[_RuleKey, int] = dict.fromkeys(keys, 0)
        seen_issues: set[str] = set()
        for item in self.issues:
            key = _issue_key(item)
            if key not in issue_counts or item.trade_date not in expected_checked:
                raise ValueError("quality issue falls outside selected rule and dates")
            identity = item.model_dump_json()
            if identity in seen_issues:
                raise ValueError("quality issue is duplicated")
            seen_issues.add(identity)
            issue_counts[key] += 1
        if any(
            rule.issue_count != issue_counts[(rule.rule_id, rule.field_name)]
            for rule in self.quality_rules
        ):
            raise ValueError("quality issue counts disagree with issue rows")
        expected_conclusion = (
            "not_fully_assessed"
            if any(rule.assessed_days != rule.expected_days for rule in self.quality_rules)
            else "issues_observed"
            if self.issues
            else "no_issues_observed"
        )
        if self.quality_conclusion != expected_conclusion:
            raise ValueError("quality conclusion disagrees with per-rule evidence")
        return self


class DataAuditReport(_ReportBody):
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_digest(self) -> Self:
        if self.content_hash != _payload_hash(
            self.model_dump(mode="json", exclude={"content_hash"})
        ):
            raise ValueError("report content hash mismatch")
        return self


class CatalogDataAuditReport(DataAuditReport):
    """V2 adds catalog facts while retaining the daily V1 result unchanged."""

    schema_version: Literal[2] = 2
    dataset_rule_version: Literal["catalog-dataset-audit-v1"] = "catalog-dataset-audit-v1"
    dataset_contract_sha256: Sha256
    audit_as_of: datetime
    datasets: tuple[DatasetAuditResult, ...] = Field(min_length=24, max_length=24)

    @model_validator(mode="after")
    def validate_dataset_binding(self) -> Self:
        from rquant.data_catalog.build import CATALOG_CONTRACTS

        ids = tuple(item.dataset_id for item in self.datasets)
        if ids != tuple(sorted(c.dataset_id for c in CATALOG_CONTRACTS)):
            raise ValueError("catalog audit must include every dataset exactly once")
        if self.audit_as_of.tzinfo is None or self.audit_as_of.utcoffset() is None:
            raise ValueError("catalog audit requires an aware observation time")
        if any(
            item.source_id != self.source.snapshot_label
            or item.source_kind != "fixed_replica"
            or item.audit_start != self.audit_start
            or item.observed_through != self.observed_through
            or item.as_of != self.audit_as_of
            or item.rule_version != self.dataset_rule_version
            for item in self.datasets
        ):
            raise ValueError("catalog audit source, range or rule version differs")
        if self.dataset_contract_sha256 != _payload_hash(
            {item.dataset_id: item.contract_sha256 for item in self.datasets}
        ):
            raise ValueError("catalog audit contract summary differs")
        return self


def validate_data_audit_report(report: DataAuditReport) -> DataAuditReport:
    model = CatalogDataAuditReport if report.schema_version == 2 else DataAuditReport
    return model.model_validate(report.model_dump(mode="python"))


def data_audit_report_path(directory: Path, report_hash: str) -> Path:
    """Resolve two explicit artifact versions without directory scans or fallback on corruption."""
    if len(report_hash) != 64 or any(c not in "0123456789abcdef" for c in report_hash):
        raise ValueError("invalid audit report digest")
    current = directory / f"data-audit-v2-{report_hash}.json"
    return current if current.exists() else directory / f"data-audit-v1-{report_hash}.json"


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _payload_hash(payload: dict[str, object]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _issue_key(issue: DailyBarQualityIssue) -> _RuleKey:
    if issue.rule_id in {"daily_bar.close_above_limit", "daily_bar.close_below_limit"}:
        return (_RULE_CLOSE, None)
    if issue.rule_id == "daily_bar.zero_volume_unsuspended":
        return (_RULE_VOLUME, None)
    return (_RULE_NULL, issue.field_name)


def _open_days(
    connection: duckdb.DuckDBPyConnection,
    audit_start: date,
    observed_through: date,
    coverage: DailyBarCoverageReport,
) -> tuple[date, ...]:
    try:
        rows = connection.execute(
            "SELECT cal_date FROM trade_calendar WHERE exchange = 'SSE' AND is_open "
            "AND cal_date BETWEEN ? AND ? ORDER BY cal_date LIMIT ?",
            (audit_start, observed_through, MAX_AUDIT_DAYS + 1),
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("SSE calendar evidence unavailable") from exc
    days = tuple(row[0] for row in rows)
    if len(days) > MAX_AUDIT_DAYS or tuple(sorted(set(days))) != days:
        raise ValueError("SSE calendar open dates are duplicated or over budget")
    month_counts = {
        month.month: sum(
            day.year == month.month.year and day.month == month.month.month for day in days
        )
        for month in coverage.monthly
    }
    if any(month_counts[month.month] != month.expected_open_days for month in coverage.monthly):
        raise ValueError("SSE open dates disagree with coverage evidence")
    if not days or days[-1] != observed_through:
        raise ValueError("SSE final open date is absent")
    return days


def build_data_audit_report(
    connection: duckdb.DuckDBPyConnection,
    *,
    source: AuditReportSource,
    audit_start: date,
    observed_through: date,
    null_fields: tuple[DailyBarNullFieldSpec, ...],
) -> DataAuditReport:
    """Audit all selected open dates from one caller-fixed read-only connection.

    The production completion chain is not implemented here. Even when the
    observed dates have rows, the report keeps collection completion unconfirmed.
    """
    source = AuditReportSource.model_validate(source)
    null_fields = tuple(sorted(null_fields, key=lambda item: item.field_name))
    if not null_fields or len(null_fields) > _MAX_NULL_FIELDS:
        raise ValueError("null_fields must select one to nine supported fields")
    if len({item.field_name for item in null_fields}) != len(null_fields):
        raise ValueError("null_fields must be unique")
    coverage = audit_daily_bar_coverage_from_connection(
        connection,
        snapshot_id=source.snapshot_label,
        audit_start=audit_start,
        completed_through=observed_through,
    )
    days = _open_days(connection, audit_start, observed_through, coverage)
    missing = {day for day in days if any(gap.start <= day <= gap.end for gap in coverage.gaps)}
    if len(missing) != sum(gap.missing_open_days for gap in coverage.gaps):
        raise ValueError("coverage gaps disagree with open SSE dates")
    keys: tuple[_RuleKey, ...] = (
        (_RULE_CLOSE, None),
        (_RULE_VOLUME, None),
        *((_RULE_NULL, item.field_name) for item in null_fields),
    )
    checked: dict[_RuleKey, list[date]] = {key: [] for key in keys}
    assessed: dict[_RuleKey, list[date]] = {key: [] for key in keys}
    unassessed: dict[_RuleKey, list[RuleUnassessedDay]] = {key: [] for key in keys}
    issues: list[DailyBarQualityIssue] = []
    issue_counts: dict[_RuleKey, int] = dict.fromkeys(keys, 0)

    for day in days:
        if day in missing:
            for key in keys:
                unassessed[key].append(
                    RuleUnassessedDay(day=day, reason="no_daily_bar", affected_rows=0)
                )
            continue
        daily = audit_daily_bar_quality_from_connection(
            connection,
            snapshot_id=source.snapshot_label,
            completed_trade_date=day,
            null_fields=null_fields,
        )
        if daily.snapshot_id != coverage.snapshot_id or daily.trade_date != day:
            raise ValueError("quality evidence disagrees with fixed report source or date")
        day_unassessed: set[_RuleKey] = set()
        for item in daily.unassessed:
            key = (item.rule_id, item.field_name)
            if key not in unassessed:
                raise ValueError("quality reported an unselected rule")
            unassessed[key].append(
                RuleUnassessedDay(day=day, reason=item.reason, affected_rows=item.count)
            )
            day_unassessed.add(key)
        for key in keys:
            checked[key].append(day)
            if key not in day_unassessed:
                assessed[key].append(day)
        for item in daily.issues:
            key = _issue_key(item)
            if key not in issue_counts:
                raise ValueError("quality reported an unselected issue")
            issues.append(item)
            issue_counts[key] += 1
            if len(issues) > MAX_REPORT_ISSUES:
                raise ValueError("quality issue count exceeds report limit")

    rules = tuple(
        QualityRuleEvidence(
            rule_id=key[0],
            field_name=key[1],
            expected_days=days,
            checked_days=tuple(checked[key]),
            assessed_days=tuple(assessed[key]),
            unassessed=tuple(unassessed[key]),
            issue_count=issue_counts[key],
        )
        for key in keys
    )
    quality_conclusion = (
        "not_fully_assessed"
        if any(rule.assessed_days != days for rule in rules)
        else "issues_observed"
        if issues
        else "no_issues_observed"
    )
    body = _ReportBody(
        quality_conclusion=quality_conclusion,
        source=source,
        audit_start=audit_start,
        observed_through=observed_through,
        null_fields=null_fields,
        coverage=CoverageFacts(
            snapshot_label=coverage.snapshot_id,
            exchange=coverage.exchange,
            audit_start=coverage.audit_start,
            observed_through=coverage.completed_through,
            monthly=coverage.monthly,
            gaps=coverage.gaps,
            closed_day_rows=coverage.closed_day_rows,
        ),
        quality_rules=rules,
        issues=tuple(issues),
    )
    data = body.model_dump(mode="json")
    report = DataAuditReport.model_validate({**data, "content_hash": _payload_hash(data)})
    if len(_canonical_bytes(report.model_dump(mode="json"))) > MAX_REPORT_BYTES:
        raise ValueError("audit report exceeds byte limit")
    return report


def build_catalog_data_audit_report(
    connection: duckdb.DuckDBPyConnection,
    *,
    source: AuditReportSource,
    audit_start: date,
    observed_through: date,
    null_fields: tuple[DailyBarNullFieldSpec, ...],
    as_of: datetime,
) -> CatalogDataAuditReport:
    from rquant.data_audit_dataset_evidence import read_catalog_audit_from_connection
    from rquant.data_audit_datasets import catalog_contract_sha256

    daily = build_data_audit_report(
        connection,
        source=source,
        audit_start=audit_start,
        observed_through=observed_through,
        null_fields=null_fields,
    )
    datasets = read_catalog_audit_from_connection(
        connection,
        source_id=source.snapshot_label,
        audit_start=audit_start,
        observed_through=observed_through,
        as_of=as_of,
    )
    data = {
        **daily.model_dump(mode="json", exclude={"content_hash"}),
        "schema_version": 2,
        "dataset_rule_version": "catalog-dataset-audit-v1",
        "dataset_contract_sha256": catalog_contract_sha256(),
        "audit_as_of": datasets[0].model_dump(mode="json")["as_of"],
        "datasets": [r.model_dump(mode="json") for r in datasets],
    }
    report = CatalogDataAuditReport.model_validate({**data, "content_hash": _payload_hash(data)})
    if len(_canonical_bytes(report.model_dump(mode="json"))) > MAX_REPORT_BYTES:
        raise ValueError("audit report exceeds byte limit")
    return report


def _decode_report(data: bytes) -> DataAuditReport:
    if not data or len(data) > MAX_REPORT_BYTES:
        raise ValueError("audit report exceeds byte limit or is empty")
    try:
        header = json.loads(data)
        if not isinstance(header, dict):
            raise ValueError("audit report must be an object")
        model = CatalogDataAuditReport if header.get("schema_version") == 2 else DataAuditReport
        report = model.model_validate_json(data)
    except ValueError as exc:
        raise ValueError("audit report is invalid or its digest mismatches") from exc
    if data != _canonical_bytes(report.model_dump(mode="json")):
        raise ValueError("audit report is not canonical")
    return report


def parse_data_audit_report_bytes(data: bytes, *, filename: str) -> DataAuditReport:
    """Validate the whole immutable artifact, including its content-addressed name."""
    report = _decode_report(data)
    if filename != f"data-audit-v{report.schema_version}-{report.content_hash}.json":
        raise ValueError("audit report filename disagrees with content hash")
    return report


def load_data_audit_report(path: Path) -> DataAuditReport:
    """Reject oversized, noncanonical, renamed, symlinked, or corrupt artifacts."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        observed = os.fstat(handle.fileno())
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("audit report must be a regular file")
        size = observed.st_size
        if size <= 0 or size > MAX_REPORT_BYTES:
            raise ValueError("audit report exceeds byte limit or is empty")
        data = handle.read(MAX_REPORT_BYTES + 1)
        if os.fstat(handle.fileno()).st_size != size or len(data) != size:
            raise ValueError("audit report changed during read")
    return parse_data_audit_report_bytes(data, filename=path.name)


def publish_data_audit_report(report: DataAuditReport, directory: Path) -> Path:
    """Validate, fsync, then atomically add one immutable content-addressed file."""
    report = validate_data_audit_report(report)
    data = _canonical_bytes(report.model_dump(mode="json"))
    _decode_report(data)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"data-audit-v{report.schema_version}-{report.content_hash}.json"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".data-audit-", dir=directory)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fchmod(handle.fileno(), 0o444)
            os.fsync(handle.fileno())
        _decode_report(temporary.read_bytes())
        try:
            os.link(str(temporary), str(destination))
        except FileExistsError:
            existing = load_data_audit_report(destination)
            if existing != report:
                raise ValueError("existing audit report differs from content identity") from None
        else:
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return destination
    finally:
        temporary.unlink(missing_ok=True)


def _file_identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _replica_identity(observed: os.stat_result) -> AuditReplicaFileIdentity:
    return AuditReplicaFileIdentity(
        device=observed.st_dev,
        inode=observed.st_ino,
        size=observed.st_size,
        mtime_ns=observed.st_mtime_ns,
        ctime_ns=observed.st_ctime_ns,
    )


def _require_explicit_path(path: Path) -> None:
    if (
        not path.is_absolute()
        or path != Path(os.path.abspath(path))
        or path.parent.resolve(strict=False) != path.parent
    ):
        raise ValueError("audit paths must be absolute and canonical")


def _require_fixed_replica(primary_path: Path, replica_path: Path, opened: os.stat_result) -> None:
    try:
        primary = primary_path.stat(follow_symlinks=False)
        current = replica_path.stat(follow_symlinks=False)
    except OSError as exc:
        raise DataAuditReplicaChangedError("read-only replica path changed during audit") from exc
    if not stat.S_ISREG(primary.st_mode) or not stat.S_ISREG(current.st_mode):
        raise DataAuditReplicaChangedError("primary and read-only replica must be regular files")
    if (primary.st_dev, primary.st_ino) == (opened.st_dev, opened.st_ino):
        raise DataAuditReplicaChangedError("read-only replica aliases the primary")
    if _file_identity(current) != _file_identity(opened):
        raise DataAuditReplicaChangedError("read-only replica changed during audit")
    if os.path.lexists(f"{replica_path}.wal"):
        raise DataAuditReplicaChangedError("read-only replica has an unsealed DuckDB WAL")
    if os.path.lexists(f"{replica_path}.shm"):
        raise DataAuditReplicaChangedError("read-only replica has an unsealed SHM sidecar")


def capture_data_audit_replica_identity(
    primary_path: Path, replica_path: Path
) -> AuditReplicaFileIdentity:
    """Bind an explicit sealed replica without scanning bytes or connecting to DuckDB."""
    primary_path, replica_path = Path(primary_path), Path(replica_path)
    _require_explicit_path(primary_path)
    _require_explicit_path(replica_path)
    descriptor = os.open(replica_path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size <= 0:
            raise DataAuditReplicaChangedError("read-only replica must be a nonempty regular file")
        _require_fixed_replica(primary_path, replica_path, opened)
        return _replica_identity(opened)


def _file_sha256(handle: BinaryIO) -> str:
    handle.seek(0)
    digest = hashlib.sha256()
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def create_and_publish_data_audit_report(
    *,
    primary_path: Path,
    replica_path: Path,
    audit_start: date,
    observed_through: date,
    null_fields: tuple[DailyBarNullFieldSpec, ...],
    directory: Path,
    expected_file_identity: AuditReplicaFileIdentity | None = None,
    expected_file_sha256: str | None = None,
    on_replica_sha256: Callable[[str], None] | None = None,
    include_catalog: bool = False,
) -> Path:
    """Seal one trusted local replica read into an unverified production report.

    The file digest binds the observed bytes, not a production generation or
    collection-completion authority. The caller selects paths and dates outside Web.
    """
    primary_path, replica_path, directory = (
        Path(primary_path),
        Path(replica_path),
        Path(directory),
    )
    for path in (primary_path, replica_path, directory):
        _require_explicit_path(path)
    if expected_file_identity is not None:
        expected_file_identity = AuditReplicaFileIdentity.model_validate(expected_file_identity)
    if expected_file_sha256 is not None and (
        len(expected_file_sha256) != 64
        or any(char not in "0123456789abcdef" for char in expected_file_sha256)
    ):
        raise ValueError("expected replica SHA256 is invalid")
    if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
        raise ValueError("audit report directory must be a real directory")
    descriptor = os.open(replica_path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size <= 0:
            raise DataAuditReplicaChangedError("read-only replica must be a nonempty regular file")
        _require_fixed_replica(primary_path, replica_path, opened)
        if expected_file_identity is not None:
            actual = _replica_identity(opened)
            if expected_file_sha256 is None:
                if actual != expected_file_identity:
                    raise DataAuditReplicaChangedError("submitted replica file identity changed")
            elif (
                actual.device,
                actual.inode,
                actual.size,
                actual.mtime_ns,
            ) != (
                expected_file_identity.device,
                expected_file_identity.inode,
                expected_file_identity.size,
                expected_file_identity.mtime_ns,
            ):
                raise DataAuditReplicaChangedError("submitted replica file identity changed")

        digest = _file_sha256(handle)
        _require_fixed_replica(primary_path, replica_path, opened)
        if expected_file_sha256 is not None and digest != expected_file_sha256:
            raise DataAuditReplicaChangedError("submitted replica SHA256 changed")
        if on_replica_sha256 is not None:
            on_replica_sha256(digest)

        directory.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".audit-source-", dir=directory.parent) as private:
            pinned = Path(private) / "replica.duckdb"
            try:
                os.link(replica_path, pinned, follow_symlinks=False)
            except OSError as exc:
                raise ValueError("cannot pin read-only replica inode") from exc
            pinned_source = os.fstat(handle.fileno())
            pinned_stat = pinned.stat(follow_symlinks=False)
            if (
                not stat.S_ISREG(pinned_stat.st_mode)
                or (pinned_stat.st_dev, pinned_stat.st_ino) != (opened.st_dev, opened.st_ino)
                or _file_identity(pinned_stat) != _file_identity(pinned_source)
                or pinned_source.st_size != opened.st_size
                or pinned_source.st_mtime_ns != opened.st_mtime_ns
            ):
                raise DataAuditReplicaChangedError("read-only replica changed before pinning")
            _require_fixed_replica(primary_path, replica_path, pinned_source)
            with duckdb.connect(str(pinned), read_only=True) as connection:
                _require_fixed_replica(primary_path, replica_path, pinned_source)
                builder = (
                    build_catalog_data_audit_report if include_catalog else build_data_audit_report
                )
                observation = {}
                if include_catalog:
                    from rquant.data_contracts import EXCHANGE_TIMEZONE

                    # Keep retry digests stable; this cutoff is not the actual execution clock.
                    observation["as_of"] = datetime.combine(
                        observed_through + timedelta(days=1), time.min, EXCHANGE_TIMEZONE
                    )
                report = builder(
                    connection,
                    source=AuditReportSource(
                        mode="production_unverified",
                        namespace="production",
                        snapshot_label=f"sha256:{digest}",
                    ),
                    audit_start=audit_start,
                    observed_through=observed_through,
                    null_fields=null_fields,
                    **observation,
                )
                _require_fixed_replica(primary_path, replica_path, pinned_source)
            if _file_sha256(handle) != digest:
                raise DataAuditReplicaChangedError("read-only replica digest changed during audit")
            _require_fixed_replica(primary_path, replica_path, pinned_source)
    return publish_data_audit_report(report, directory)
