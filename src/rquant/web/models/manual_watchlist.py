"""Private manual watchlist response contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

from rquant.manual_watchlist import PriceLevel, TsCode, WatchlistSource
from rquant.runtime_contracts import AwareUtcDatetime


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


class ManualWatchlistCommandRequest(_PrivateModel):
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    ts_code: TsCode
    action: Literal["add", "remove"]
    expected_version: StrictInt | None = Field(default=None, ge=1)
    source: WatchlistSource | None = None
    price_levels: tuple[PriceLevel, ...] | None = Field(default=None, max_length=8)
    expires_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def require_action_fields(self) -> ManualWatchlistCommandRequest:
        if self.action == "remove":
            if (
                self.expected_version is None
                or {"source", "price_levels", "expires_at"} & self.model_fields_set
            ):
                raise ValueError("remove requires a version and no add-only fields")
        elif self.price_levels is None:
            if "price_levels" in self.model_fields_set:
                raise ValueError("price levels cannot be null")
        elif any(
            left >= right
            for left, right in zip(self.price_levels, self.price_levels[1:], strict=False)
        ):
            raise ValueError("price levels must be ascending and distinct")
        return self


class ManualWatchlistCommandReceipt(_PrivateModel):
    command_id: str
    ts_code: TsCode
    action: Literal["add", "remove"]
    status: Literal[
        "pending",
        "processing",
        "saved_syncing",
        "published",
        "conflict",
        "capacity",
        "failed",
        "uncertain",
    ]
    version: int | None = Field(default=None, ge=1)
    message: str
