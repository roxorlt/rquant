"""Bounded catalog and daily Serving rows derived from one verified audit artifact."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime

from rquant.data_audit_contracts import MAX_INDEXED_ISSUES
from rquant.data_audit_datasets import DatasetAuditResult
from rquant.data_audit_report import (
    CatalogDataAuditReport,
    DataAuditReport,
    validate_data_audit_report,
)
from rquant.serving_read_models import ServingProjectionPayload


def project_data_audit_report(
    report: DataAuditReport, *, available_at: datetime
) -> tuple[ServingProjectionPayload, ...]:
    """Retain exact totals while limiting the first-page issue index."""
    report = validate_data_audit_report(report)
    monthly = report.coverage.monthly
    indexed = report.issues[:MAX_INDEXED_ISSUES]
    expected_days = sum(item.expected_open_days for item in monthly)
    covered_days = sum(item.covered_open_days for item in monthly)
    unassessed_rule_days = sum(
        len({item.day for item in rule.unassessed}) for rule in report.quality_rules
    )
    overview = {
        "report_hash": report.content_hash,
        "schema_version": report.schema_version,
        "rule_version": report.rule_version,
        "run_status": report.run_status,
        "collection_status": report.collection_status,
        "collection_completed_through": report.collection_completed_through,
        "coverage_conclusion": report.coverage_conclusion,
        "quality_conclusion": report.quality_conclusion,
        "current": False,
        "source_mode": report.source.mode,
        "source_namespace": report.source.namespace,
        "replica_generation_id": report.source.replica_generation_id,
        "audit_start": report.audit_start.isoformat(),
        "observed_through": report.observed_through.isoformat(),
        "expected_open_days": expected_days,
        "covered_open_days": covered_days,
        "missing_open_days": expected_days - covered_days,
        "gap_count": len(report.coverage.gaps),
        "longest_gap_open_days": max(
            (item.missing_open_days for item in report.coverage.gaps), default=0
        ),
        "closed_day_count": len(report.coverage.closed_day_rows),
        "monthly_count": len(monthly),
        "rule_count": len(report.quality_rules),
        "quality_issue_count": len(report.issues),
        "indexed_issue_count": len(indexed),
        "omitted_issue_count": len(report.issues) - len(indexed),
        "unassessed_rule_days": unassessed_rule_days,
    }
    month_rows = tuple(
        {
            "report_hash": report.content_hash,
            "month": item.month.isoformat(),
            "expected_open_days": item.expected_open_days,
            "covered_open_days": item.covered_open_days,
            "coverage_ratio": (None if item.coverage_ratio is None else float(item.coverage_ratio)),
            "status": item.status,
        }
        for item in monthly
    )
    rule_rows = []
    for rule in report.quality_rules:
        reasons = Counter(item.reason for item in rule.unassessed)
        rule_rows.append(
            {
                "report_hash": report.content_hash,
                "rule_id": rule.rule_id,
                "field_name": rule.field_name or "",
                "expected_days": len(rule.expected_days),
                "checked_days": len(rule.checked_days),
                "assessed_days": len(rule.assessed_days),
                "unassessed_days": len({item.day for item in rule.unassessed}),
                "first_assessed_date": (
                    None if not rule.assessed_days else rule.assessed_days[0].isoformat()
                ),
                "last_assessed_date": (
                    None if not rule.assessed_days else rule.assessed_days[-1].isoformat()
                ),
                "assessment_complete": rule.assessed_days == rule.expected_days,
                "unassessed_reasons_json": json.dumps(
                    dict(sorted(reasons.items())), separators=(",", ":"), sort_keys=True
                ),
                "issue_count": rule.issue_count,
            }
        )
    issue_rows = tuple(
        {
            "report_hash": report.content_hash,
            "issue_index": index,
            "trade_date": item.trade_date.isoformat(),
            "rule_id": item.rule_id,
            "ts_code": item.ts_code,
            "field_name": item.field_name,
            "observed_value": (None if item.observed_value is None else str(item.observed_value)),
            "reference_value": (
                None if item.reference_value is None else str(item.reference_value)
            ),
            "null_rows": item.null_rows,
            "observed_rows": item.observed_rows,
        }
        for index, item in enumerate(indexed)
    )
    rows = {
        "audit_report_overview": (overview,),
        "audit_report_month": month_rows,
        "audit_report_rule": tuple(rule_rows),
        "audit_report_issue": issue_rows,
    }
    if isinstance(report, CatalogDataAuditReport):
        rows["audit_report_dataset"] = tuple(
            {
                "report_hash": report.content_hash,
                "dataset_id": result.dataset_id,
                "result_json": result.model_dump_json(),
            }
            for result in report.datasets
        )
    return tuple(
        ServingProjectionPayload(table_name=name, available_at=available_at, rows=rows[name])
        for name in sorted(rows)
    )


def read_catalog_audit_projection_rows(
    rows: tuple[dict[str, object], ...],
    *,
    report_hash: str,
) -> tuple[DatasetAuditResult, ...]:
    """Validate typed, canonical summaries and one source/range/observation binding."""
    from rquant.data_catalog.build import CATALOG_CONTRACTS

    if len(rows) != len(CATALOG_CONTRACTS):
        raise ValueError("catalog audit projection lacks datasets")
    results = []
    for row in rows:
        raw = row.get("result_json")
        if row.get("report_hash") != report_hash or not isinstance(raw, str):
            raise ValueError("catalog audit projection mixes reports")
        result = DatasetAuditResult.model_validate_json(raw)
        if row.get("dataset_id") != result.dataset_id or raw != result.model_dump_json():
            raise ValueError("catalog audit projection is noncanonical or mislabelled")
        results.append(result)
    ids = tuple(r.dataset_id for r in results)
    if ids != tuple(sorted(c.dataset_id for c in CATALOG_CONTRACTS)):
        raise ValueError("catalog audit projection is missing or duplicating datasets")
    bindings = {
        (r.source_id, r.source_kind, r.audit_start, r.observed_through, r.as_of, r.rule_version)
        for r in results
    }
    if len(bindings) != 1:
        raise ValueError("catalog audit projection mixes sources or observation times")
    return tuple(results)
