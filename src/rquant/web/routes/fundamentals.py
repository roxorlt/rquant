"""Independent replica-bound financial summary for the data center."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from rquant.screen.replica_source import (
    ScreenReplicaBudgetError,
    ScreenReplicaChangedError,
    ScreenReplicaDataError,
    ScreenReplicaUnavailableError,
)
from rquant.web.models.fundamentals import (
    FinancialFieldCount,
    FinancialReasonCount,
    FinancialSummarySource,
    FundamentalSummaryData,
)
from rquant.web.security import current_user

router = APIRouter(prefix="/data/fundamentals")
_COVERAGE_NOTE = "全市场覆盖尚未核验"
_FIELDS = (
    ("pe_ttm", "市盈率", "倍"),
    ("pb", "市净率", "倍"),
    ("dv_ttm", "股息率", "%"),
    ("roe", "净资产收益率", "%"),
    ("or_yoy", "营收同比", "%"),
    ("netprofit_yoy", "归母净利同比", "%"),
)
_CHANGED = "财务数据已更新，请刷新后重试。"
_UNAVAILABLE = "财务数据暂时无法读取，请稍后重试。"


@router.get("/summary", response_model=FundamentalSummaryData, summary="财务数据概况")
def get_fundamental_summary(
    request: Request,
    _viewer: Annotated[str | None, Depends(current_user)],
    expected_identity: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> FundamentalSummaryData:
    web = request.app.state.web
    replica = web.screen_service.replica
    if replica is None:
        if expected_identity is not None:
            raise HTTPException(status_code=409, detail=_CHANGED)
        return FundamentalSummaryData(
            status="not_configured",
            decision_date=None,
            waiting_for_today=False,
            source=None,
            record_count=None,
            fields=[
                FinancialFieldCount(
                    key=key,
                    label=label,
                    unit=unit,
                    known_count=None,
                    unknown_count=None,
                    reasons=[],
                )
                for key, label, unit in _FIELDS
            ],
            coverage_note=_COVERAGE_NOTE,
        )
    try:
        snapshot = replica.fundamental_summary(now=web.clock(), expected_identity=expected_identity)
    except ScreenReplicaChangedError as error:
        raise HTTPException(status_code=409, detail=_CHANGED) from error
    except (
        ScreenReplicaUnavailableError,
        ScreenReplicaDataError,
        ScreenReplicaBudgetError,
        ValueError,
    ) as error:
        raise HTTPException(status_code=503, detail=_UNAVAILABLE) from error
    summary = snapshot.summary
    counts = {field.key: field for field in summary.fields}
    return FundamentalSummaryData(
        status=summary.status,
        decision_date=summary.decision_date,
        waiting_for_today=summary.waiting_for_today,
        source=FinancialSummarySource(identity=snapshot.identity, updated_at=snapshot.updated_at),
        record_count=summary.record_count,
        fields=[
            FinancialFieldCount(
                key=key,
                label=label,
                unit=unit,
                known_count=counts[key].known_count if key in counts else None,
                unknown_count=counts[key].unknown_count if key in counts else None,
                reasons=[
                    FinancialReasonCount(label=reason.label, count=reason.count)
                    for reason in counts[key].reasons
                ]
                if key in counts
                else [],
            )
            for key, label, unit in _FIELDS
        ],
        coverage_note=_COVERAGE_NOTE,
    )
