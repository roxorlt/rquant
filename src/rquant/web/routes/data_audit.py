"""Read bounded audit results from one verified Serving generation."""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from rquant.data_audit_projection import DataAuditIssueProjectionRow, DataAuditStatusProjectionRow
from rquant.serving_contracts import FreshnessStatus
from rquant.web.envelope import Envelope
from rquant.web.labels import audit_rule_label
from rquant.web.models.data_audit import (
    AuditAttempt,
    AuditSuccess,
    DataAuditHealthData,
    DataAuditIssueItem,
    DataAuditIssuesData,
)
from rquant.web.routes.catalog import _catalog
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/data")
_MAX_ISSUES = 256
_CHANGED = "审计数据已更新，请重新查看。"
_UNREADABLE = "审计结果暂时无法读取，请稍后重试。"
_TABLES = ("data_audit_issue", "data_audit_status")
_ATTEMPT_LABELS = {"running": "审计中", "completed": "已完成", "failed": "审计失败"}


def _source_state(
    borrowed: BorrowedGeneration | None,
) -> Literal["ready", "not_published", "unavailable"]:
    if borrowed is None:
        return "unavailable"
    mark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == "lab_jobs"), None
    )
    if mark is None:
        return "not_published"
    if mark.status is not FreshnessStatus.FRESH:
        return "unavailable"
    rows = borrowed.cursor.execute(
        "SELECT table_name, available, row_count FROM projection_status "
        "WHERE table_name IN (?, ?) ORDER BY table_name LIMIT 3",
        _TABLES,
    ).fetchall()
    if len(rows) != 2:
        return "not_published" if not rows else "unavailable"
    if not any(bool(row[1]) for row in rows):
        return "not_published"
    if not all(bool(row[1]) for row in rows):
        return "unavailable"
    for name, _available, count in rows:
        if borrowed.manifest.row_counts.get(str(name)) != int(count):
            raise ValueError("audit projection count differs from manifest")
    return "ready"


def _read(
    borrowed: BorrowedGeneration,
) -> tuple[DataAuditStatusProjectionRow, tuple[DataAuditIssueProjectionRow, ...]]:
    expected_status = borrowed.manifest.row_counts["data_audit_status"]
    expected_issues = borrowed.manifest.row_counts["data_audit_issue"]
    if expected_status != 1 or not 0 <= expected_issues <= _MAX_ISSUES:
        raise ValueError("audit row budget differs")
    status_rows = borrowed.cursor.execute(
        "SELECT latest_status, latest_observed_at, latest_completed_at, "
        "successful_audit_id, successful_as_of_date, successful_range_start, "
        "successful_range_end, successful_completed_at, finding_count, p0_count "
        "FROM data_audit_status WHERE status_key = 'current' LIMIT 2"
    ).fetchall()
    issue_rows = borrowed.cursor.execute(
        "SELECT audit_run_id, issue_id, dataset_id, rule_id, severity, status "
        "FROM data_audit_issue ORDER BY audit_run_id, issue_id LIMIT ?",
        (_MAX_ISSUES + 1,),
    ).fetchall()
    if len(status_rows) != expected_status or len(issue_rows) != expected_issues:
        raise ValueError("audit rows differ from manifest")
    status = DataAuditStatusProjectionRow.model_validate(
        dict(
            zip(
                (
                    "latest_status",
                    "latest_observed_at",
                    "latest_completed_at",
                    "successful_audit_id",
                    "successful_as_of_date",
                    "successful_range_start",
                    "successful_range_end",
                    "successful_completed_at",
                    "finding_count",
                    "p0_count",
                ),
                status_rows[0],
                strict=True,
            )
        )
    )
    issues = tuple(
        DataAuditIssueProjectionRow.model_validate(
            dict(
                zip(
                    ("audit_run_id", "issue_id", "dataset_id", "rule_id", "severity", "status"),
                    row,
                    strict=True,
                )
            )
        )
        for row in issue_rows
    )
    if len(issues) != status.finding_count:
        raise ValueError("audit issue count differs")
    if any(item.audit_run_id != status.successful_audit_id for item in issues):
        raise ValueError("audit issues belong to another run")
    if len({item.issue_id for item in issues}) != len(issues):
        raise ValueError("audit issues are duplicated")
    return status, issues


def _snapshot(
    borrowed: BorrowedGeneration | None,
) -> tuple[
    Literal["ready", "not_published", "unavailable"],
    DataAuditStatusProjectionRow | None,
    tuple[DataAuditIssueProjectionRow, ...],
]:
    try:
        state = _source_state(borrowed)
        if state != "ready" or borrowed is None:
            return state, None, ()
        status, issues = _read(borrowed)
        return state, status, issues
    except Exception as error:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from error


def _health(
    state: Literal["ready", "not_published", "unavailable"],
    status: DataAuditStatusProjectionRow | None,
) -> DataAuditHealthData:
    if status is None:
        return DataAuditHealthData(source_state=state, latest_attempt=None, latest_success=None)
    attempt = (
        None
        if status.latest_status == "never_run"
        else AuditAttempt(
            status=status.latest_status,
            label=_ATTEMPT_LABELS[status.latest_status],
            observed_at=status.latest_observed_at,
            completed_at=status.latest_completed_at,
        )
    )
    success = (
        None
        if status.successful_audit_id is None
        else AuditSuccess(
            as_of_date=status.successful_as_of_date,
            range_start=status.successful_range_start,
            range_end=status.successful_range_end,
            completed_at=status.successful_completed_at,
            finding_count=status.finding_count,
            p0_count=status.p0_count,
        )
    )
    return DataAuditHealthData(source_state=state, latest_attempt=attempt, latest_success=success)


@router.get("/health", response_model=Envelope[DataAuditHealthData], summary="数据审计状态")
def get_data_health(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[DataAuditHealthData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        state, status, _issues = _snapshot(None if meta.state == "unavailable" else borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[DataAuditHealthData](data=_health(state, status), serving=meta)


@router.get("/issues", response_model=Envelope[DataAuditIssuesData], summary="数据审计问题")
def get_data_issues(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    dataset: Annotated[str, Query(min_length=1, max_length=128)],
    generation: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
) -> Envelope[DataAuditIssuesData]:
    found = next((item for item in _catalog().datasets if item.dataset_id == dataset), None)
    if found is None:
        raise HTTPException(status_code=404, detail="找不到这个数据集")
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
        state, _status, issues = _snapshot(None if meta.state == "unavailable" else borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    selected = [item for item in issues if item.dataset_id == dataset]
    data = DataAuditIssuesData(
        source_state=state,
        dataset_name=found.name,
        total_count=len(selected),
        partial=False,
        issues=[
            DataAuditIssueItem(
                number=index,
                name=audit_rule_label(item.rule_id),
                severity=item.severity,
                status="待处理" if item.status == "open" else "已处理",
            )
            for index, item in enumerate(selected, start=1)
        ],
    )
    return Envelope[DataAuditIssuesData](data=data, serving=meta)
