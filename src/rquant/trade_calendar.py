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


class TradeCalendarConnection(Protocol):
    def execute(
        self,
        query: str,
        parameters: Sequence[object] | None = None,
    ) -> TradeCalendarConnection: ...

    def executemany(
        self,
        query: str,
        parameters: Sequence[Sequence[object]],
    ) -> TradeCalendarConnection: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class TradeCalendarStore(Protocol):
    _conn: TradeCalendarConnection

    def upsert_trade_calendar(self, rows: Sequence[TradeCalendarDay]) -> int: ...

    def list_trade_calendar(
        self, exchange: str, start: date, end: date
    ) -> list[TradeCalendarDay]: ...

    def missing_trade_calendar_dates(
        self, exchange: str, start: date, end: date
    ) -> list[date]: ...


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


def _validate_authoritative_rows(
    rows: Sequence[TradeCalendarDay],
    *,
    exchange: str,
    start: date,
    end: date,
) -> list[TradeCalendarDay]:
    if start > end:
        raise ValueError("trade calendar refresh start must not be after end")
    selected = deduplicate_trade_calendar_rows(rows)
    wrong_exchange = sorted({row.exchange for row in selected if row.exchange != exchange})
    if wrong_exchange:
        raise ValueError(
            f"trade calendar provider returned unexpected exchanges: {wrong_exchange}"
        )
    outside_dates = sorted(
        {row.cal_date for row in selected if row.cal_date < start or row.cal_date > end}
    )
    if outside_dates:
        rendered = ", ".join(day.isoformat() for day in outside_dates)
        raise ValueError(
            "trade calendar provider returned dates outside requested range: "
            f"{rendered}"
        )
    expected_dates = _civil_dates(start, end)
    fetched_dates = {row.cal_date for row in selected}
    missing_dates = [day for day in expected_dates if day not in fetched_dates]
    if missing_dates:
        raise TradeCalendarGapError(exchange, missing_dates)

    first_pretrade_date = selected[0].pretrade_date
    if first_pretrade_date is None:
        raise ValueError(
            f"trade calendar pretrade_date is required for {selected[0].cal_date}"
        )
    if first_pretrade_date >= start:
        raise ValueError(
            "trade calendar first pretrade_date must be before requested range: "
            f"{first_pretrade_date} >= {start}"
        )

    last_open = first_pretrade_date
    for row in selected:
        if row.pretrade_date is None:
            raise ValueError(
                f"trade calendar pretrade_date is required for {row.cal_date}"
            )
        if row.pretrade_date != last_open:
            raise ValueError(
                "trade calendar pretrade_date chain mismatch for "
                f"{row.cal_date}: expected {last_open}, got {row.pretrade_date}"
            )
        if row.is_open:
            last_open = row.cal_date
    return selected


def fetch_trade_calendar_rows(
    adapter: TradeCalendarAdapter,
    *,
    exchange: str,
    start: date,
    end: date,
    updated_at: datetime | None = None,
) -> list[TradeCalendarDay]:
    """Fetch, normalize, and validate a complete authoritative civil range."""
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
    required_columns = {"exchange", "cal_date", "is_open", "pretrade_date"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "trade calendar provider data missing columns: "
            + ", ".join(missing_columns)
        )
    rows = normalize_trade_calendar(
        frame,
        source="tushare",
        updated_at=updated_at,
    )
    return _validate_authoritative_rows(
        rows,
        exchange=exchange,
        start=start,
        end=end,
    )


def persist_verified_trade_calendar(
    store: TradeCalendarStore,
    rows: Sequence[TradeCalendarDay],
    *,
    exchange: str,
    start: date,
    end: date,
) -> TradeCalendarRefreshResult:
    """Persist validated rows and verify stored business facts day by day."""
    authoritative_rows = _validate_authoritative_rows(
        rows,
        exchange=exchange,
        start=start,
        end=end,
    )
    connection = store._conn
    connection.execute("BEGIN")
    try:
        for incoming in authoritative_rows:
            existing = connection.execute(
                """
                SELECT is_open, pretrade_date
                FROM trade_calendar
                WHERE exchange = ? AND cal_date = ? AND updated_at = ?
                """,
                [incoming.exchange, incoming.cal_date, incoming.updated_at],
            ).fetchone()
            if existing is not None and (
                bool(existing[0]), existing[1]
            ) != trade_calendar_business_facts(incoming):
                raise TradeCalendarConflictError(
                    incoming.exchange,
                    incoming.cal_date,
                    incoming.updated_at,
                )

        connection.executemany(
            """
            INSERT INTO trade_calendar
            (exchange, cal_date, is_open, pretrade_date, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (exchange, cal_date) DO UPDATE SET
                is_open = excluded.is_open,
                pretrade_date = excluded.pretrade_date,
                source = excluded.source,
                updated_at = excluded.updated_at
            WHERE excluded.updated_at > trade_calendar.updated_at
            """,
            [
                [
                    row.exchange,
                    row.cal_date,
                    row.is_open,
                    row.pretrade_date,
                    row.source,
                    row.updated_at,
                ]
                for row in authoritative_rows
            ],
        )

        stored_rows = connection.execute(
            """
            SELECT cal_date, is_open, pretrade_date
            FROM trade_calendar
            WHERE exchange = ? AND cal_date BETWEEN ? AND ?
            ORDER BY cal_date
            """,
            [exchange, start, end],
        ).fetchall()
        stored_by_date = {
            row[0]: (bool(row[1]), row[2])
            for row in stored_rows
        }
        missing_dates = [
            row.cal_date
            for row in authoritative_rows
            if row.cal_date not in stored_by_date
        ]
        if missing_dates:
            raise TradeCalendarGapError(exchange, missing_dates)

        mismatches = [
            row.cal_date
            for row in authoritative_rows
            if stored_by_date[row.cal_date] != trade_calendar_business_facts(row)
        ]
        if mismatches:
            rendered = ", ".join(day.isoformat() for day in mismatches)
            raise ValueError(
                "trade calendar post-write verification mismatch for "
                f"{exchange}: {rendered}"
            )
        result = TradeCalendarRefreshResult(
            exchange=exchange,
            start=start,
            end=end,
            requested_days=len(_civil_dates(start, end)),
            fetched_days=len(authoritative_rows),
            upserted_days=len(authoritative_rows),
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return result


def refresh_trade_calendar(
    adapter: TradeCalendarAdapter,
    store: TradeCalendarStore,
    *,
    exchange: str,
    start: date,
    end: date,
    updated_at: datetime | None = None,
) -> TradeCalendarRefreshResult:
    """Compatibility wrapper for fetch then persist with post-write verification."""
    rows = fetch_trade_calendar_rows(
        adapter,
        exchange=exchange,
        start=start,
        end=end,
        updated_at=updated_at,
    )
    return persist_verified_trade_calendar(
        store,
        rows,
        exchange=exchange,
        start=start,
        end=end,
    )
