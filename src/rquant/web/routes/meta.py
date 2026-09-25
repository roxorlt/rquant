"""``GET /api/v1/meta``: the generation marker, dataset watermarks and market phase.

The front end polls this every 15 seconds; a changed ``generation.generation_id`` makes
it invalidate every other query. It is also the release script's liveness check.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, ConfigDict

from rquant.web.calendar import calendar_day
from rquant.web.envelope import Envelope
from rquant.web.labels import dataset_label
from rquant.web.market import PHASE_LABELS, MarketPhase, market_phase, shanghai_trade_date
from rquant.web.models.common import StatusInfo
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.status import watermark_status

router = APIRouter()

_MAX_PROJECTIONS = 256


class GenerationInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    generation_id: str
    built_at: datetime
    published_at: datetime | None
    previous_generation_id: str | None
    producer_commit: str
    schema_version: int
    age_seconds: float


class DatasetWatermarkInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_id: str
    #: Plain name and user-level status for the data chip's tooltip.
    name: str
    user_status: StatusInfo
    status: Literal["fresh", "stale", "degraded", "unavailable"]
    event_time: datetime
    published_at: datetime
    sequence: int
    reason: str | None


class ProjectionInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str
    available: bool
    reason: str | None
    owner_dataset_id: str | None
    available_at: datetime | None


class MarketInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_date: date
    phase: MarketPhase
    phase_label: str
    #: None when the Serving trade calendar is missing or does not cover the date.
    is_trading_day: bool | None
    #: Open days either side of ``trade_date`` from the same calendar; None when unknown.
    previous_trading_day: date | None
    next_trading_day: date | None


class MetaData(BaseModel):
    model_config = ConfigDict(frozen=True)

    server_time: datetime
    viewer: str | None
    generation: GenerationInfo | None
    datasets: list[DatasetWatermarkInfo]
    projections: list[ProjectionInfo]
    market: MarketInfo


def _utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError("serving timestamp columns must be TIMESTAMPTZ")
    return value.astimezone(UTC)


def _projections(borrowed: BorrowedGeneration) -> list[ProjectionInfo]:
    rows = borrowed.cursor.execute(
        "SELECT table_name, available, reason, owner_dataset_id, available_at "
        "FROM projection_status ORDER BY table_name"
    ).fetchmany(_MAX_PROJECTIONS)
    return [
        ProjectionInfo(
            table_name=str(row[0]),
            available=bool(row[1]),
            reason=None if row[2] is None else str(row[2]),
            owner_dataset_id=None if row[3] is None else str(row[3]),
            available_at=_utc(row[4]),
        )
        for row in rows
    ]


def build_meta(
    borrowed: BorrowedGeneration | None,
    *,
    now: datetime,
    viewer: str | None,
) -> MetaData:
    trade_date = shanghai_trade_date(now)
    if borrowed is None:
        return MetaData(
            server_time=now,
            viewer=viewer,
            generation=None,
            datasets=[],
            projections=[],
            market=MarketInfo(
                trade_date=trade_date,
                phase=MarketPhase.UNKNOWN,
                phase_label=PHASE_LABELS[MarketPhase.UNKNOWN],
                is_trading_day=None,
                previous_trading_day=None,
                next_trading_day=None,
            ),
        )
    manifest = borrowed.manifest
    pointer = borrowed.pointer
    projections = _projections(borrowed)
    day = calendar_day(borrowed.cursor, trade_date)
    phase = market_phase(now, day.is_trading_day)
    return MetaData(
        server_time=now,
        viewer=viewer,
        generation=GenerationInfo(
            generation_id=manifest.generation_id,
            built_at=manifest.built_at,
            published_at=None if pointer is None else pointer.published_at,
            previous_generation_id=None if pointer is None else pointer.previous_generation_id,
            producer_commit=manifest.producer_commit,
            schema_version=manifest.schema_version,
            age_seconds=max((now - manifest.built_at).total_seconds(), 0.0),
        ),
        datasets=[
            DatasetWatermarkInfo(
                dataset_id=item.dataset_id,
                name=dataset_label(item.dataset_id),
                user_status=StatusInfo.of(watermark_status(item)),
                status=item.status.value,
                event_time=item.event_time,
                published_at=item.published_at,
                sequence=item.sequence,
                reason=item.reason,
            )
            for item in manifest.watermarks
        ],
        projections=projections,
        market=MarketInfo(
            trade_date=trade_date,
            phase=phase,
            phase_label=PHASE_LABELS[phase],
            is_trading_day=day.is_trading_day,
            previous_trading_day=day.previous_trading_day,
            next_trading_day=day.next_trading_day,
        ),
    )


@router.get("/meta", response_model=Envelope[MetaData], summary="数据代、数据集水位与市场阶段")
def get_meta(
    request: Request,
    response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[MetaData]:
    context = request.app.state.web
    now = context.clock()
    with context.tracker.borrow() as borrowed:
        data = build_meta(borrowed, now=now, viewer=viewer)
        meta = serving_meta(
            borrowed,
            now=now,
            stale_after=context.settings.stale_after,
            failure=context.tracker.failure,
        )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    response.headers["Cache-Control"] = "no-store"
    return Envelope[MetaData](data=data, serving=meta)
