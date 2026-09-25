"""The response shell every API endpoint returns: ``{"data": ..., "serving": ...}``.

``serving.state`` uses the four states of the Streamlit pages' ``ServingFrameResult``
so the front end's ``ServingBanner`` and ``render_serving_state_banner`` agree.
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
    state: ServingState
    detail: str


class Envelope(BaseModel, Generic[DataT]):
    model_config = ConfigDict(frozen=True)

    data: DataT
    serving: ServingMeta
