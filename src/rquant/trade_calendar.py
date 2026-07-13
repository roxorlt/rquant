"""Typed authoritative trade-calendar normalization and refresh contracts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from numbers import Integral
from typing import Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("updated_at must be timezone-aware")
    return value.astimezone(UTC)


class TradeCalendarDay(BaseModel):
    """One known exchange-local civil date in an authoritative calendar."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    exchange: str = Field(min_length=1)
    cal_date: date
    is_open: StrictBool
    pretrade_date: date | None = None
    source: str = Field(default="tushare", min_length=1)
    updated_at: datetime = Field(default_factory=_utc_now)

    @field_validator("updated_at")
    @classmethod
    def normalize_updated_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @model_validator(mode="after")
    def validate_pretrade_date(self) -> TradeCalendarDay:
        if self.pretrade_date is not None and self.pretrade_date >= self.cal_date:
            raise ValueError("pretrade_date must be strictly before cal_date")
        return self


class TradeCalendarRefreshResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange: str = Field(min_length=1)
    start: date
    end: date
    requested_days: int = Field(ge=1)
    fetched_days: int = Field(ge=1)
    upserted_days: int = Field(ge=0)


class TradeCalendarGapError(LookupError):
    """Calendar coverage is unknown, as distinct from a stored closed day."""

    exchange: str
    missing_dates: tuple[date, ...]

    def __init__(
        self,
        exchange: str,
        missing_dates: Sequence[date] = (),
        *,
        detail: str | None = None,
    ) -> None:
        self.exchange = exchange
        self.missing_dates = tuple(sorted(set(missing_dates)))
        if detail is None:
            rendered = ", ".join(day.isoformat() for day in self.missing_dates)
            detail = f"missing trade calendar data for {exchange}: {rendered}"
        super().__init__(detail)


class TradeCalendarConflictError(ValueError):
    """Two observations claim different business facts at the same time."""

    exchange: str
    cal_date: date
    updated_at: datetime

    def __init__(
        self,
        exchange: str,
        cal_date: date,
        updated_at: datetime,
    ) -> None:
        self.exchange = exchange
        self.cal_date = cal_date
        self.updated_at = updated_at
        super().__init__(
            "trade calendar conflict at equal updated_at for "
            f"{exchange} {cal_date.isoformat()} ({updated_at.isoformat()})"
        )


class TradeCalendarAdapter(Protocol):
    def trade_cal_raw(
        self, start: date, end: date, exchange: str = "SSE"
    ) -> pd.DataFrame: ...


class TradeCalendarStore(Protocol):
    def upsert_trade_calendar(self, rows: Sequence[TradeCalendarDay]) -> int: ...


def trade_calendar_business_facts(
    row: TradeCalendarDay,
) -> tuple[bool, date | None]:
    return row.is_open, row.pretrade_date


def deduplicate_trade_calendar_rows(
    rows: Sequence[TradeCalendarDay],
) -> list[TradeCalendarDay]:
    """Select the newest observation per key and reject equal-time fact conflicts."""
    observations: dict[tuple[str, date, datetime], TradeCalendarDay] = {}
    for row in rows:
        observation_key = (row.exchange, row.cal_date, row.updated_at)
        current = observations.get(observation_key)
        if current is None:
            observations[observation_key] = row
            continue
        if trade_calendar_business_facts(row) != trade_calendar_business_facts(
            current
        ):
            raise TradeCalendarConflictError(
                row.exchange,
                row.cal_date,
                row.updated_at,
            )
        observations[observation_key] = min(
            current,
            row,
            key=lambda item: item.source,
        )

    selected: dict[tuple[str, date], TradeCalendarDay] = {}
    for row in observations.values():
        key = (row.exchange, row.cal_date)
        current = selected.get(key)
        if current is None or row.updated_at > current.updated_at:
            selected[key] = row
    return [selected[key] for key in sorted(selected)]


def _parse_civil_date(value: object, *, field_name: str) -> date:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            raise ValueError(f"{field_name} is required")
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if len(stripped) == 8 and stripped.isdigit():
            return datetime.strptime(stripped, "%Y%m%d").date()
    raise ValueError(
        f"{field_name} must be YYYYMMDD, date, or pandas Timestamp; got {value!r}"
    )


def _parse_optional_civil_date(value: object) -> date | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return _parse_civil_date(value, field_name="pretrade_date")


def _parse_is_open(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, Integral) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip() in ("0", "1"):
        return value.strip() == "1"
    raise ValueError(f"is_open must be bool, 0, 1, '0', or '1'; got {value!r}")


def normalize_trade_calendar(
    frame: pd.DataFrame,
    *,
    source: str = "tushare",
    updated_at: datetime | None = None,
) -> list[TradeCalendarDay]:
    """Normalize provider rows without using Python truthiness for open status."""
    if frame.empty:
        return []
    required = {"exchange", "cal_date", "is_open"}
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "trade calendar provider data missing columns: "
            + ", ".join(missing_columns)
        )
    normalized_at = _normalize_utc(updated_at or _utc_now())
    rows: list[TradeCalendarDay] = []
    for provider_row in frame.to_dict(orient="records"):
        rows.append(
            TradeCalendarDay(
                exchange=provider_row["exchange"],
                cal_date=_parse_civil_date(
                    provider_row["cal_date"], field_name="cal_date"
                ),
                is_open=_parse_is_open(provider_row["is_open"]),
                pretrade_date=_parse_optional_civil_date(
                    provider_row.get("pretrade_date")
                ),
                source=source,
                updated_at=normalized_at,
            )
        )
    return deduplicate_trade_calendar_rows(rows)


def _civil_dates(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def refresh_trade_calendar(
    adapter: TradeCalendarAdapter,
    store: TradeCalendarStore,
    *,
    exchange: str,
    start: date,
    end: date,
    updated_at: datetime | None = None,
) -> TradeCalendarRefreshResult:
    """Fetch and persist a complete requested civil range or raise a typed gap."""
    if start > end:
        raise ValueError("trade calendar refresh start must not be after end")
    frame = adapter.trade_cal_raw(start, end, exchange=exchange)
    if frame.empty:
        raise TradeCalendarGapError(
            exchange,
            _civil_dates(start, end),
            detail=(
                f"trade calendar provider returned no calendar rows for "
                f"{exchange} {start.isoformat()}..{end.isoformat()}"
            ),
        )
    rows = normalize_trade_calendar(
        frame,
        source="tushare",
        updated_at=updated_at,
    )
    wrong_exchange = sorted({row.exchange for row in rows if row.exchange != exchange})
    if wrong_exchange:
        raise ValueError(
            f"trade calendar provider returned unexpected exchanges: {wrong_exchange}"
        )
    outside_dates = sorted(
        {row.cal_date for row in rows if row.cal_date < start or row.cal_date > end}
    )
    if outside_dates:
        rendered = ", ".join(day.isoformat() for day in outside_dates)
        raise ValueError(
            "trade calendar provider returned dates outside requested range: "
            f"{rendered}"
        )
    expected_dates = _civil_dates(start, end)
    fetched_dates = {row.cal_date for row in rows}
    missing_dates = [day for day in expected_dates if day not in fetched_dates]
    if missing_dates:
        raise TradeCalendarGapError(exchange, missing_dates)
    upserted = store.upsert_trade_calendar(rows)
    return TradeCalendarRefreshResult(
        exchange=exchange,
        start=start,
        end=end,
        requested_days=len(expected_dates),
        fetched_days=len(rows),
        upserted_days=upserted,
    )
