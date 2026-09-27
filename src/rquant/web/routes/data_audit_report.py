"""One-generation read of the bounded offline daily-bar audit report."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Literal, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from rquant.data_audit_report_projection import REPORT_PROJECTION_TABLES
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from rquant.web.envelope import Envelope
from rquant.web.models.data_audit_report import (
    AuditReportIssue,
    AuditReportMonth,
    AuditReportOverview,
    AuditReportRule,
    AuditReportUnassessedReason,
    DataAuditReportData,
    IssueRuleId,
    ReportIssueRow,
    ReportMonthRow,
    ReportOverviewRow,
    ReportRuleRow,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/data")
_TABLES = tuple(sorted(REPORT_PROJECTION_TABLES))
_CHANGED = "审计报告已更新，请重新查看。"
_UNREADABLE = "审计报告暂时无法读取，请稍后重试。"
_RULE_NAMES = {
    "daily_bar.close_limit": "收盘价上下限",
    "daily_bar.zero_volume": "零成交量",
    "daily_bar.field_null_ratio": "字段空值比例",
}
_ISSUE_NAMES = {
    "daily_bar.close_above_limit": "收盘价高于涨停价",
    "daily_bar.close_below_limit": "收盘价低于跌停价",
    "daily_bar.zero_volume_unsuspended": "未停牌但零成交量",
    "daily_bar.field_null_ratio": "字段空值比例",
}
_FIELD_NAMES = {
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "pre_close": "昨收价",
    "change": "涨跌额",
    "pct_chg": "涨跌幅",
    "vol": "成交量",
    "amount": "成交额",
}
_REASON_NAMES = {
    "no_daily_bar": "缺少日线",
    "close_missing": "缺少收盘价",
    "limits_unavailable": "涨跌停价未确认",
    "volume_missing": "缺少成交量",
    "suspension_unknown": "停牌状态未确认",
    "no_observations": "没有可检查的数据",
}
_ISSUE_TO_RULE = {
    "daily_bar.close_above_limit": "daily_bar.close_limit",
    "daily_bar.close_below_limit": "daily_bar.close_limit",
    "daily_bar.zero_volume_unsuspended": "daily_bar.zero_volume",
    "daily_bar.field_null_ratio": "daily_bar.field_null_ratio",
}
_RowT = TypeVar("_RowT", ReportOverviewRow, ReportMonthRow, ReportRuleRow, ReportIssueRow)


def _source_state(
    borrowed: BorrowedGeneration | None,
) -> Literal["ready", "not_published", "unavailable"]:
    if borrowed is None:
        return "unavailable"
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        "WHERE table_name IN (?, ?, ?, ?) ORDER BY table_name LIMIT 5",
        _TABLES,
    ).fetchall()
    if not marks:
        if any(borrowed.manifest.row_counts.get(name, 0) for name in _TABLES):
            raise ValueError("report table is missing from projection status")
        return "not_published"
    if len(marks) != len(_TABLES) or tuple(row[0] for row in marks) != _TABLES:
        raise ValueError("report projection status is incomplete")
    if any(
        type(available) is not bool or type(count) is not int for _, available, count, *_ in marks
    ):
        raise ValueError("report projection status has invalid types")
    if not any(row[1] for row in marks):
        if any(
            count or borrowed.manifest.row_counts.get(str(name), 0) for name, _, count, *_ in marks
        ):
            raise ValueError("unpublished report has rows")
        return "not_published"
    watermark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == "lab_jobs"), None
    )
    if watermark is None:
        raise ValueError("report owner watermark is absent")
    available_at = marks[0][5]
    if any(
        not available
        or owner != "lab_jobs"
        or generation != watermark.generation_id
        or at != available_at
        or at is None
        or at > borrowed.manifest.built_at
        or type(count) is not int
        or count != borrowed.manifest.row_counts.get(str(name))
        for name, available, count, owner, generation, at in marks
    ):
        raise ValueError("report projection status disagrees with the generation")
    return "ready"


def _rows(
    borrowed: BorrowedGeneration,
    table: str,
    model: type[_RowT],
    expected: int,
) -> list[_RowT]:
    contract = PAGE_PROJECTION_CONTRACTS[table]
    if not 0 <= expected <= contract.max_rows:
        raise ValueError("report table exceeds its row budget")
    columns = contract.column_names
    selected = borrowed.cursor.execute(
        f"SELECT {', '.join(columns)} FROM {table} "
        f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
        (contract.max_rows + 1,),
    ).fetchall()
    if len(selected) != expected:
        raise ValueError("report table count differs from manifest")
    return [model.model_validate(dict(zip(columns, row, strict=True))) for row in selected]


def _month_sequence(start: date, end: date) -> list[date]:
    months: list[date] = []
    month = start.replace(day=1)
    last = end.replace(day=1)
    while month <= last:
        months.append(month)
        month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
    return months


def _reasons(rule: ReportRuleRow) -> list[AuditReportUnassessedReason]:
    raw = rule.unassessed_reasons_json
    reasons = json.loads(raw)
    if (
        not isinstance(reasons, dict)
        or json.dumps(reasons, sort_keys=True, separators=(",", ":")) != raw
    ):
        raise ValueError("report unassessed reasons are not canonical")
    if set(reasons) - _REASON_NAMES.keys() or any(
        type(days) is not int or not 1 <= days <= rule.unassessed_days for days in reasons.values()
    ):
        raise ValueError("report unassessed reasons are invalid")
    count = sum(reasons.values())
    if (rule.unassessed_days == 0) != (count == 0) or count < rule.unassessed_days:
        raise ValueError("report unassessed reasons do not cover missing days")
    return [
        AuditReportUnassessedReason(reason=reason, name=_REASON_NAMES[reason], days=days)
        for reason, days in reasons.items()
    ]


def _rule_key(rule_id: IssueRuleId, field_name: str | None) -> tuple[str, str]:
    return _ISSUE_TO_RULE[rule_id], field_name or ""


def _read_report(borrowed: BorrowedGeneration) -> DataAuditReportData:
    counts = borrowed.manifest.row_counts
    overview_rows = _rows(
        borrowed, "audit_report_overview", ReportOverviewRow, counts["audit_report_overview"]
    )
    if len(overview_rows) != 1:
        raise ValueError("report overview is not singular")
    overview = overview_rows[0]
    months = _rows(borrowed, "audit_report_month", ReportMonthRow, counts["audit_report_month"])
    rules = _rows(borrowed, "audit_report_rule", ReportRuleRow, counts["audit_report_rule"])
    issues = _rows(borrowed, "audit_report_issue", ReportIssueRow, counts["audit_report_issue"])
    if any(row.report_hash != overview.report_hash for row in (*months, *rules, *issues)):
        raise ValueError("report rows belong to different reports")
    if (
        len(months) != overview.monthly_count
        or len(rules) != overview.rule_count
        or len(issues) != overview.indexed_issue_count
        or [item.month for item in months]
        != _month_sequence(overview.audit_start, overview.observed_through)
    ):
        raise ValueError("report projected rows are incomplete")
    if any(
        item.covered_open_days > item.expected_open_days
        or item.status != ("measured" if item.expected_open_days else "no_expected_sessions")
        or (
            item.coverage_ratio is None
            if item.expected_open_days
            else item.coverage_ratio is not None
        )
        or (
            item.expected_open_days > 0
            and Decimal(str(item.coverage_ratio)).quantize(Decimal("0.0001"))
            != (Decimal(item.covered_open_days) / Decimal(item.expected_open_days)).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )
        )
        for item in months
    ):
        raise ValueError("report monthly coverage is inconsistent")
    if (
        sum(item.expected_open_days for item in months) != overview.expected_open_days
        or sum(item.covered_open_days for item in months) != overview.covered_open_days
    ):
        raise ValueError("report monthly totals disagree with overview")
    rule_keys = [(item.rule_id, item.field_name) for item in rules]
    if len(set(rule_keys)) != len(rules) or set(rule_keys) != {
        ("daily_bar.close_limit", ""),
        ("daily_bar.zero_volume", ""),
        *(("daily_bar.field_null_ratio", item.field_name) for item in rules if item.field_name),
    }:
        raise ValueError("report rule set is invalid")
    if any(
        item.expected_days != overview.expected_open_days
        or (
            item.first_assessed_date is not None
            and not overview.audit_start <= item.first_assessed_date <= overview.observed_through
        )
        or (
            item.last_assessed_date is not None
            and not overview.audit_start <= item.last_assessed_date <= overview.observed_through
        )
        for item in rules
    ):
        raise ValueError("report rule day range is invalid")
    reasons = {key: _reasons(rule) for key, rule in zip(rule_keys, rules, strict=True)}
    if (
        sum(item.unassessed_days for item in rules) != overview.unassessed_rule_days
        or sum(item.issue_count for item in rules) != overview.quality_issue_count
    ):
        raise ValueError("report rule totals disagree with overview")
    conclusion = (
        "not_fully_assessed"
        if overview.unassessed_rule_days
        else "issues_observed"
        if overview.quality_issue_count
        else "no_issues_observed"
    )
    if overview.quality_conclusion != conclusion:
        raise ValueError("report quality conclusion disagrees with rule evidence")
    indexed_counts: Counter[tuple[str, str]] = Counter()
    for index, issue in enumerate(issues):
        key = _rule_key(issue.rule_id, issue.field_name)
        if (
            issue.issue_index != index
            or not overview.audit_start <= issue.trade_date <= overview.observed_through
            or key not in rule_keys
            or (issue.rule_id == "daily_bar.field_null_ratio") != bool(issue.field_name)
            or (
                issue.null_rows is not None
                and issue.observed_rows is not None
                and issue.null_rows > issue.observed_rows
            )
        ):
            raise ValueError("report issue index is invalid")
        indexed_counts[key] += 1
    if any(
        indexed_counts[key] > rule.issue_count for key, rule in zip(rule_keys, rules, strict=True)
    ):
        raise ValueError("report issue counts exceed rule totals")
    return DataAuditReportData(
        source_state="ready",
        overview=AuditReportOverview.model_validate(
            {
                **overview.model_dump(),
                "collection_label": "采集未确认",
                "coverage_label": "覆盖情况待确认",
                "quality_label": {
                    "not_fully_assessed": "尚未完整检查",
                    "issues_observed": "发现问题",
                    "no_issues_observed": "已检查范围无问题",
                }[overview.quality_conclusion],
            }
        ),
        months=[
            AuditReportMonth(
                **item.model_dump(exclude={"report_hash"}),
                status_label="已统计" if item.status == "measured" else "无交易日",
            )
            for item in months
        ],
        rules=[
            AuditReportRule(
                **item.model_dump(exclude={"report_hash", "unassessed_reasons_json", "field_name"}),
                name=_RULE_NAMES[item.rule_id],
                field_name=item.field_name or None,
                field_label=_FIELD_NAMES.get(item.field_name, "检查字段")
                if item.field_name
                else None,
                unassessed_reasons=reasons[key],
            )
            for key, item in zip(rule_keys, rules, strict=True)
        ],
        issues=[
            AuditReportIssue(
                number=item.issue_index + 1,
                trade_date=item.trade_date,
                rule_id=item.rule_id,
                name=_ISSUE_NAMES[item.rule_id],
                ts_code=item.ts_code,
                field_name=item.field_name,
                field_label=_FIELD_NAMES.get(item.field_name, "检查字段")
                if item.field_name
                else None,
                observed_value=item.observed_value,
                reference_value=item.reference_value,
                null_rows=item.null_rows,
                observed_rows=item.observed_rows,
            )
            for item in issues
        ],
    )


def _snapshot(borrowed: BorrowedGeneration | None) -> DataAuditReportData:
    try:
        state = _source_state(borrowed)
        if state != "ready" or borrowed is None:
            return DataAuditReportData(
                source_state=state, overview=None, months=[], rules=[], issues=[]
            )
        return _read_report(borrowed)
    except Exception as error:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from error


@router.get("/report", response_model=Envelope[DataAuditReportData], summary="日线数据审计报告")
def get_data_audit_report(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    generation: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
) -> Envelope[DataAuditReportData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation is not None and generation != meta.generation_id:
            raise HTTPException(status_code=409, detail=_CHANGED)
        data = _snapshot(None if meta.state == "unavailable" else borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[DataAuditReportData](data=data, serving=meta)
