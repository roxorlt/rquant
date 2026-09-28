"""Private manual watchlist response contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.manual_watchlist import WatchlistSource


class _PrivateModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ManualWatchlistItemData(_PrivateModel):
    ts_code: str
    version: int = Field(ge=1)
    source: WatchlistSource
    price_levels: list[str] = Field(max_length=8)
    expires_at: datetime | None
    updated_at: datetime


class ManualWatchlistListData(_PrivateModel):
    availability: Literal["ready", "unavailable"]
    message: str
    available_at: datetime | None
    items: list[ManualWatchlistItemData] = Field(max_length=500)


class ManualWatchlistExactData(_PrivateModel):
    availability: Literal["ready", "unavailable"]
    message: str
    available_at: datetime | None
    ts_code: str
    status: Literal["active", "expired", "deleted", "absent"] | None
    version: int | None
    source: WatchlistSource | None
    price_levels: list[str] = Field(max_length=8)
    expires_at: datetime | None
    updated_at: datetime | None
