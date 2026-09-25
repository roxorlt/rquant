"""The response shell every API endpoint returns: ``{"data": ..., "serving": ...}``.

``serving.state`` uses the four state names of the Streamlit pages' ``ServingFrameResult``,
but it describes the generation being served, not every dataset watermark inside it (see
``rquant.web.serving.serving_meta``): per-dataset freshness is data, shown where it matters.
``message`` is the one short sentence the page banner shows (None when there is nothing
to say); ``detail`` is the technical reason, for tooltips and logs.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict

DataT = TypeVar("DataT")


class ServingState(StrEnum):
    READY = "ready"
    STALE = "stale"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ServingMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    generation_id: str | None
    built_at: datetime | None
    #: Seconds since ``built_at`` at the time of the request; None without a generation.
    age_seconds: float | None
    state: ServingState
    message: str | None
    detail: str


class Envelope(BaseModel, Generic[DataT]):
    model_config = ConfigDict(frozen=True)

    data: DataT
    serving: ServingMeta
