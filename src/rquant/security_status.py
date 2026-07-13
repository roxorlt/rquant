"""Typed historical security-name and ST-status facts."""

from __future__ import annotations

import re
import time as time_module
import unicodedata
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, time
from typing import Protocol
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SHANGHAI = ZoneInfo("Asia/Shanghai")
STATUS_VISIBLE_TIME = time(9, 25)
NAMECHANGE_PROVIDER_LIMIT = 10_000
NAMECHANGE_EARLIEST_DATE = date(1990, 1, 1)
STOCK_ST_PROVIDER_LIMIT = 1_000
STOCK_ST_AVAILABLE_FROM = date(2016, 1, 1)
DEFAULT_REQUEST_INTERVAL_SECONDS = 60 / 480
SOURCE_ISSUE_DATE_SAMPLE_LIMIT = 20
_NAMECHANGE_COLUMNS = (
    "ts_code",
    "name",
    "start_date",
    "end_date",
    "ann_date",
    "change_reason",
)
_STOCK_ST_COLUMNS = ("ts_code", "name", "trade_date", "type", "type_name")
_ST_PREFIX = re.compile(r"^(?:S\*ST|\*ST|SST|ST)", re.IGNORECASE)


class SecurityStatusModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class DailySecurityKey(SecurityStatusModel):
    ts_code: str = Field(min_length=1)
    trade_date: date


class NameChangeInterval(SecurityStatusModel):
    ts_code: str = Field(min_length=1)
    name: str = Field(min_length=1)
    start_date: date
    end_date: date | None = None
    ann_date: date | None = None
    change_reason: str | None = None

    @model_validator(mode="after")
    def validate_interval(self) -> NameChangeInterval:
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        return self


class StockSTObservation(SecurityStatusModel):
    ts_code: str = Field(min_length=1)
    name: str = Field(min_length=1)
    trade_date: date
    type: str | None = None
    type_name: str | None = None


class SecurityStatusSourceIssue(SecurityStatusModel):
    source: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    blocking: bool = True
    ts_code: str | None = None
    trade_date: date | None = None


class NameChangeHistory(SecurityStatusModel):
    intervals: tuple[NameChangeInterval, ...] = ()
    issues: tuple[SecurityStatusSourceIssue, ...] = ()


class StockSTHistory(SecurityStatusModel):
    observations: tuple[StockSTObservation, ...] = ()
    issues: tuple[SecurityStatusSourceIssue, ...] = ()
    is_complete: bool = True


class SecurityStatusDaily(SecurityStatusModel):
    ts_code: str = Field(min_length=1)
    trade_date: date
    name: str | None = Field(default=None, min_length=1)
    is_st: bool | None = None
    name_source: str = Field(min_length=1)
    st_source: str | None = Field(default=None, min_length=1)
    available_at: datetime | None = None
    ingested_at: datetime
    conflict_reason: str | None = Field(default=None, min_length=1)

    @field_validator("available_at")
    @classmethod
    def validate_available_at(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _require_aware(value, field_name="available_at")

    @field_validator("ingested_at")
    @classmethod
    def validate_ingested_at(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="ingested_at").astimezone(UTC)

    @model_validator(mode="after")
    def validate_conflict(self) -> SecurityStatusDaily:
        if self.conflict_reason is not None and (
            self.name is not None or self.is_st is not None
        ):
            raise ValueError("conflicted status must keep name and is_st unknown")
        if self.is_st is not None:
            invalid_fields: list[str] = []
            if self.name is None:
                invalid_fields.append("name")
            if self.available_at is None:
                invalid_fields.append("available_at")
            if self.name_source.casefold() in {"unknown", "conflict"}:
                invalid_fields.append("name_source")
            if self.st_source is None or self.st_source.casefold() in {
                "unknown",
                "conflict",
            }:
                invalid_fields.append("st_source")
            if invalid_fields:
                invalid = ", ".join(invalid_fields)
                raise ValueError(
                    f"known is_st requires complete valid facts; invalid: {invalid}"
                )
        return self


class SecurityStatusCoverage(SecurityStatusModel):
    start: date
    end: date
    expected_count: int = Field(ge=0)
    persisted_count: int = Field(ge=0)
    missing_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    conflict_count: int = Field(ge=0)
    invalid_count: int = Field(ge=0)
    missing_samples: tuple[DailySecurityKey, ...] = ()
    unknown_samples: tuple[DailySecurityKey, ...] = ()
    conflict_samples: tuple[DailySecurityKey, ...] = ()
    invalid_samples: tuple[DailySecurityKey, ...] = ()

    @model_validator(mode="after")
    def validate_counts(self) -> SecurityStatusCoverage:
        if self.persisted_count + self.missing_count != self.expected_count:
            raise ValueError("persisted_count + missing_count must equal expected_count")
        if self.unknown_count > self.persisted_count:
            raise ValueError("unknown_count cannot exceed persisted_count")
        if self.conflict_count > self.unknown_count:
            raise ValueError("conflict_count cannot exceed unknown_count")
        if self.invalid_count > self.persisted_count:
            raise ValueError("invalid_count cannot exceed persisted_count")
        return self


class SecurityStatusBackfillResult(SecurityStatusModel):
    start: date
    end: date
    source_as_of: date
    eligible_count: int = Field(ge=0)
    upserted_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    conflict_count: int = Field(ge=0)
    source_issue_count: int = Field(ge=0)
    source_issue_dates: tuple[date, ...] = ()


class NameChangeTruncatedError(RuntimeError):
    """A provider window reached the hard row cap and may be incomplete."""


class StockSTTruncatedError(RuntimeError):
    """A stock_st response reached its hard row cap and is incomplete."""

    def __init__(self, trade_date: date, row_limit: int) -> None:
        self.trade_date = trade_date
        self.row_limit = row_limit
        super().__init__(
            "Tushare stock_st response reached provider limit "
            f"{row_limit} for {trade_date.isoformat()}"
        )


class StockSTIncompleteError(RuntimeError):
    """Strict cross-checking rejected an incomplete stock_st response."""

    def __init__(
        self,
        trade_date: date,
        issues: Sequence[SecurityStatusSourceIssue],
    ) -> None:
        self.trade_date = trade_date
        self.issues = tuple(issues)
        reasons = ", ".join(issue.reason for issue in issues) or "unknown"
        super().__init__(
            f"stock_st response is incomplete for {trade_date.isoformat()}: {reasons}"
        )


class SecurityStatusWriteConflictError(RuntimeError):
    """Equal-time observations disagree on historical business facts."""

    def __init__(
        self,
        ts_code: str,
        trade_date: date,
        ingested_at: datetime,
    ) -> None:
        self.ts_code = ts_code
        self.trade_date = trade_date
        self.ingested_at = ingested_at
        super().__init__(
            "historical security status conflict at equal ingested_at for "
            f"{ts_code} {trade_date.isoformat()} ({ingested_at.isoformat()})"
        )


class SecurityStatusConcurrentWriteError(RuntimeError):
    """A concurrent commit conflict requires retrying the status batch."""


def security_status_business_facts(
    row: SecurityStatusDaily,
) -> tuple[object, ...]:
    return (
        row.name,
        row.is_st,
        row.name_source,
        row.st_source,
        row.available_at,
        row.conflict_reason,
    )


def deduplicate_security_status_rows(
    rows: Sequence[SecurityStatusDaily],
) -> list[SecurityStatusDaily]:
    observations: dict[
        tuple[str, date, datetime], SecurityStatusDaily
    ] = {}
    for row in rows:
        observation_key = (row.ts_code, row.trade_date, row.ingested_at)
        current = observations.get(observation_key)
        if current is not None and security_status_business_facts(
            current
        ) != security_status_business_facts(row):
            raise SecurityStatusWriteConflictError(*observation_key)
        observations[observation_key] = row

    selected: dict[tuple[str, date], SecurityStatusDaily] = {}
    for row in observations.values():
        key = (row.ts_code, row.trade_date)
        current = selected.get(key)
        if current is None or row.ingested_at > current.ingested_at:
            selected[key] = row
    return [selected[key] for key in sorted(selected)]


class SecurityStatusAdapter(Protocol):
    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame: ...

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame: ...


class SecurityStatusStore(Protocol):
    def list_daily_security_dates(self, start: date, end: date) -> list[date]: ...

    def list_incomplete_stock_status_dates(
        self, start: date, end: date
    ) -> list[date]: ...

    def list_daily_security_keys(
        self, start: date, end: date
    ) -> list[DailySecurityKey]: ...

    def upsert_stock_status(self, rows: Sequence[SecurityStatusDaily]) -> int: ...


def normalize_name(value: object) -> tuple[str | None, bool | None]:
    """Normalize width/whitespace and identify historical ST name prefixes."""
    if not isinstance(value, str):
        return None, None
    compact = "".join(unicodedata.normalize("NFKC", value).split())
    if not compact:
        return None, None
    return compact, bool(_ST_PREFIX.match(compact))


def _parse_date(value: object, *, field_name: str, optional: bool = False) -> date | None:
    if value is None:
        if optional:
            return None
        raise ValueError(f"{field_name} is required")
    try:
        if bool(pd.isna(value)):
            if optional:
                return None
            raise ValueError(f"{field_name} is required")
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        for format_string in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(stripped, format_string).date()
            except ValueError:
                continue
    raise ValueError(f"{field_name} is not a valid civil date: {value!r}")


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if not isinstance(value, str):
        raise ValueError(f"expected text, got {value!r}")
    stripped = value.strip()
    return stripped or None


def _source_code(value: object) -> str | None:
    try:
        return _optional_text(value)
    except ValueError:
        return None


def normalize_namechange_history(frame: pd.DataFrame) -> NameChangeHistory:
    """Normalize and exactly deduplicate overlapping namechange result windows."""
    intervals: dict[tuple[object, ...], NameChangeInterval] = {}
    issues: list[SecurityStatusSourceIssue] = []
    for raw in frame.to_dict(orient="records"):
        ts_code = _source_code(raw.get("ts_code"))
        try:
            if ts_code is None:
                raise ValueError("ts_code is required")
            name, _ = normalize_name(raw.get("name"))
            if name is None:
                raise ValueError("name is required")
            start_date = _parse_date(
                raw.get("start_date"), field_name="start_date"
            )
            assert start_date is not None
            end_date = _parse_date(
                raw.get("end_date"), field_name="end_date", optional=True
            )
            ann_date = _parse_date(
                raw.get("ann_date"), field_name="ann_date", optional=True
            )
            interval = NameChangeInterval(
                ts_code=ts_code,
                name=name,
                start_date=start_date,
                end_date=end_date,
                ann_date=ann_date,
                change_reason=_optional_text(raw.get("change_reason")),
            )
        except (TypeError, ValueError) as exc:
            issues.append(
                SecurityStatusSourceIssue(
                    source="tushare.namechange",
                    ts_code=ts_code,
                    reason=str(exc),
                )
            )
            continue
        key = (
            interval.ts_code,
            interval.name,
            interval.start_date,
            interval.end_date,
            interval.ann_date,
            interval.change_reason,
        )
        intervals[key] = interval
    selected = tuple(
        sorted(
            intervals.values(),
            key=lambda row: (
                row.ts_code,
                row.start_date,
                row.end_date or date.max,
                row.name,
                row.ann_date or date.min,
                row.change_reason or "",
            ),
        )
    )
    return NameChangeHistory(intervals=selected, issues=tuple(issues))


def normalize_stock_st_history(
    frame: pd.DataFrame,
    *,
    requested_trade_date: date | None = None,
) -> StockSTHistory:
    """Normalize stock_st positive observations without inferring negative rows."""
    observations: dict[tuple[object, ...], StockSTObservation] = {}
    issues: list[SecurityStatusSourceIssue] = []
    for raw in frame.to_dict(orient="records"):
        ts_code = _source_code(raw.get("ts_code"))
        trade_date: date | None = None
        try:
            parsed_date = _parse_date(
                raw.get("trade_date"), field_name="trade_date"
            )
            assert parsed_date is not None
            trade_date = parsed_date
            if ts_code is None:
                raise ValueError("ts_code is required")
            name, _ = normalize_name(raw.get("name"))
            if name is None:
                raise ValueError("name is required")
            observation = StockSTObservation(
                ts_code=ts_code,
                name=name,
                trade_date=trade_date,
                type=_optional_text(raw.get("type")),
                type_name=_optional_text(raw.get("type_name")),
            )
        except (TypeError, ValueError) as exc:
            issues.append(
                SecurityStatusSourceIssue(
                    source="tushare.stock_st",
                    ts_code=ts_code,
                    trade_date=trade_date,
                    reason=str(exc),
                )
            )
            continue
        key = (
            observation.ts_code,
            observation.trade_date,
            observation.name,
            observation.type,
            observation.type_name,
        )
        observations[key] = observation
    if frame.empty and requested_trade_date is not None:
        reason = (
            "stock_st_unavailable_before_2016"
            if requested_trade_date < STOCK_ST_AVAILABLE_FROM
            else "empty_stock_st_response"
        )
        issues.append(
            SecurityStatusSourceIssue(
                source="tushare.stock_st",
                reason=reason,
                blocking=False,
                trade_date=requested_trade_date,
            )
        )
    selected = tuple(
        sorted(
            observations.values(),
            key=lambda row: (
                row.ts_code,
                row.trade_date,
                row.name,
                row.type or "",
                row.type_name or "",
            ),
        )
    )
    return StockSTHistory(
        observations=selected,
        issues=tuple(issues),
        is_complete=not issues,
    )


def _visible_at(effective_date: date) -> datetime:
    return datetime.combine(effective_date, STATUS_VISIBLE_TIME, tzinfo=SHANGHAI)


def _interval_visible_at(
    key: DailySecurityKey,
    intervals: Sequence[NameChangeInterval],
) -> datetime:
    visible_dates = [
        max(key.trade_date, interval.ann_date or interval.start_date)
        for interval in intervals
    ]
    return _visible_at(max(visible_dates))


def _unknown_status(
    key: DailySecurityKey,
    *,
    ingested_at: datetime,
    conflict_reason: str | None = None,
) -> SecurityStatusDaily:
    return SecurityStatusDaily(
        ts_code=key.ts_code,
        trade_date=key.trade_date,
        name=None,
        is_st=None,
        name_source="conflict" if conflict_reason else "unknown",
        st_source=None,
        available_at=None,
        ingested_at=ingested_at,
        conflict_reason=conflict_reason,
    )


def materialize_security_status(
    keys: Sequence[DailySecurityKey],
    namechanges: NameChangeHistory,
    stock_st: StockSTHistory,
    *,
    ingested_at: datetime,
) -> list[SecurityStatusDaily]:
    """Materialize provider facts only for actual daily_bar eligibility keys."""
    _require_aware(ingested_at, field_name="ingested_at")
    intervals_by_code: dict[str, list[NameChangeInterval]] = {}
    for interval in namechanges.intervals:
        intervals_by_code.setdefault(interval.ts_code, []).append(interval)
    stock_by_key: dict[tuple[str, date], list[StockSTObservation]] = {}
    for observation in stock_st.observations:
        stock_by_key.setdefault(
            (observation.ts_code, observation.trade_date), []
        ).append(observation)

    invalid_name_codes = {
        issue.ts_code for issue in namechanges.issues if issue.ts_code is not None
    }
    invalid_name_global = any(
        issue.ts_code is None for issue in namechanges.issues
    )
    invalid_stock_codes = {
        issue.ts_code
        for issue in stock_st.issues
        if issue.blocking
        and issue.ts_code is not None
        and issue.trade_date is None
    }
    invalid_stock_keys = {
        (issue.ts_code, issue.trade_date)
        for issue in stock_st.issues
        if issue.blocking
        and issue.ts_code is not None
        and issue.trade_date is not None
    }
    invalid_stock_dates = {
        issue.trade_date
        for issue in stock_st.issues
        if issue.blocking
        and issue.ts_code is None
        and issue.trade_date is not None
    }
    invalid_stock_global = any(
        issue.blocking and issue.ts_code is None and issue.trade_date is None
        for issue in stock_st.issues
    )

    rows: list[SecurityStatusDaily] = []
    for key in sorted(set(keys), key=lambda item: (item.ts_code, item.trade_date)):
        if invalid_name_global or key.ts_code in invalid_name_codes:
            rows.append(
                _unknown_status(
                    key,
                    ingested_at=ingested_at,
                    conflict_reason="invalid_namechange_fields",
                )
            )
            continue
        if (
            invalid_stock_global
            or key.trade_date in invalid_stock_dates
            or key.ts_code in invalid_stock_codes
            or (key.ts_code, key.trade_date) in invalid_stock_keys
        ):
            rows.append(
                _unknown_status(
                    key,
                    ingested_at=ingested_at,
                    conflict_reason="invalid_stock_st_fields",
                )
            )
            continue

        covering = [
            interval
            for interval in intervals_by_code.get(key.ts_code, ())
            if interval.start_date <= key.trade_date
            and (interval.end_date is None or key.trade_date <= interval.end_date)
        ]
        name_facts = {normalize_name(interval.name) for interval in covering}
        if len(name_facts) > 1:
            rows.append(
                _unknown_status(
                    key,
                    ingested_at=ingested_at,
                    conflict_reason="overlapping_namechange_intervals",
                )
            )
            continue

        if name_facts:
            name, is_st = next(iter(name_facts))
            if name is None or is_st is None:
                rows.append(
                    _unknown_status(
                        key,
                        ingested_at=ingested_at,
                        conflict_reason="invalid_namechange_fields",
                    )
                )
                continue
            row_name = name
            row_is_st = is_st
            name_source = "tushare.namechange"
            st_source: str | None = "tushare.namechange"
            available_at: datetime | None = _interval_visible_at(key, covering)
        else:
            row_name = None
            row_is_st = None
            name_source = "unknown"
            st_source = None
            available_at = None

        positives = stock_by_key.get((key.ts_code, key.trade_date), [])
        stock_names = {normalize_name(item.name)[0] for item in positives}
        if len(stock_names) > 1:
            rows.append(
                _unknown_status(
                    key,
                    ingested_at=ingested_at,
                    conflict_reason="conflicting_stock_st_observations",
                )
            )
            continue
        if positives:
            stock_name = next(iter(stock_names))
            if row_name is None:
                row_name = stock_name
                row_is_st = True
                name_source = "tushare.stock_st"
                st_source = "tushare.stock_st"
                available_at = _visible_at(key.trade_date)
            elif stock_name != row_name or row_is_st is not True:
                rows.append(
                    _unknown_status(
                        key,
                        ingested_at=ingested_at,
                        conflict_reason="stock_st_name_conflict",
                    )
                )
                continue
            else:
                row_is_st = True
                st_source = "tushare.namechange+tushare.stock_st"
                stock_available_at = _visible_at(key.trade_date)
                available_at = max(available_at, stock_available_at)

        rows.append(
            SecurityStatusDaily(
                ts_code=key.ts_code,
                trade_date=key.trade_date,
                name=row_name,
                is_st=row_is_st,
                name_source=name_source,
                st_source=st_source,
                available_at=available_at,
                ingested_at=ingested_at,
            )
        )
    return rows


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def fetch_namechange_history(
    adapter: SecurityStatusAdapter,
    *,
    start: date,
    end: date,
    ts_code: str | None = None,
    window_years: int = 3,
    row_limit: int = NAMECHANGE_PROVIDER_LIMIT,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time_module.sleep,
) -> NameChangeHistory:
    """Fetch bounded windows and fail closed if any one may be truncated."""
    if start > end:
        raise ValueError("namechange start must not be after end")
    if window_years < 1:
        raise ValueError("window_years must be positive")
    if row_limit < 1:
        raise ValueError("row_limit must be positive")
    if request_interval_seconds < 0:
        raise ValueError("request_interval_seconds must not be negative")

    frames: list[pd.DataFrame] = []
    window_start = start
    while window_start <= end:
        window_end = min(_add_years(window_start, window_years), end)
        frame = adapter.namechange_raw(window_start, window_end, ts_code=ts_code)
        sleep(request_interval_seconds)
        if len(frame) >= row_limit:
            raise NameChangeTruncatedError(
                "Tushare namechange window reached provider limit "
                f"{row_limit}: {window_start.isoformat()}..{window_end.isoformat()}"
            )
        frames.append(frame.reindex(columns=_NAMECHANGE_COLUMNS))
        if window_end >= end:
            break
        window_start = window_end
    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=_NAMECHANGE_COLUMNS)
    )
    return normalize_namechange_history(combined)


def fetch_stock_st_history(
    adapter: SecurityStatusAdapter,
    trade_date: date,
    *,
    row_limit: int = STOCK_ST_PROVIDER_LIMIT,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time_module.sleep,
) -> StockSTHistory:
    """Fetch one stock_st day, preserving explicit source completeness."""
    if row_limit < 1:
        raise ValueError("stock_st row_limit must be positive")
    if request_interval_seconds < 0:
        raise ValueError("request_interval_seconds must not be negative")
    frame = adapter.stock_st_raw(trade_date)
    sleep(request_interval_seconds)
    if len(frame) >= row_limit:
        raise StockSTTruncatedError(trade_date, row_limit)
    return normalize_stock_st_history(
        frame.reindex(columns=_STOCK_ST_COLUMNS),
        requested_trade_date=trade_date,
    )


def backfill_historical_security_status(
    adapter: SecurityStatusAdapter,
    store: SecurityStatusStore,
    *,
    start: date,
    end: date,
    ingested_at: datetime,
    source_as_of: date | None = None,
    namechange_start: date = NAMECHANGE_EARLIEST_DATE,
    window_years: int = 3,
    missing_only: bool = True,
    strict_stock_st_crosscheck: bool = False,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time_module.sleep,
) -> SecurityStatusBackfillResult:
    """Backfill actual daily_bar keys; provider completeness is checked before writes."""
    if start > end:
        raise ValueError("security-status backfill start must not be after end")
    _require_aware(ingested_at, field_name="ingested_at")
    resolved_source_as_of = source_as_of or ingested_at.astimezone(SHANGHAI).date()
    if resolved_source_as_of < end:
        raise ValueError("source_as_of must not be before eligibility end")
    trade_dates = (
        store.list_incomplete_stock_status_dates(start, end)
        if missing_only
        else store.list_daily_security_dates(start, end)
    )
    if not trade_dates:
        return SecurityStatusBackfillResult(
            start=start,
            end=end,
            source_as_of=resolved_source_as_of,
            eligible_count=0,
            upserted_count=0,
            unknown_count=0,
            conflict_count=0,
            source_issue_count=0,
            source_issue_dates=(),
        )
    namechanges = fetch_namechange_history(
        adapter,
        start=min(namechange_start, start),
        end=resolved_source_as_of,
        window_years=window_years,
        request_interval_seconds=request_interval_seconds,
        sleep=sleep,
    )
    eligible_count = 0
    upserted_count = 0
    unknown_count = 0
    conflict_count = 0
    source_issue_count = len(namechanges.issues)
    source_issue_dates = {
        issue.trade_date
        for issue in namechanges.issues
        if issue.trade_date is not None
    }
    for trade_date in trade_dates:
        keys = store.list_daily_security_keys(trade_date, trade_date)
        stock_st = fetch_stock_st_history(
            adapter,
            trade_date,
            request_interval_seconds=request_interval_seconds,
            sleep=sleep,
        )
        if strict_stock_st_crosscheck and not stock_st.is_complete:
            raise StockSTIncompleteError(trade_date, stock_st.issues)
        source_issue_count += len(stock_st.issues)
        source_issue_dates.update(
            issue.trade_date
            for issue in stock_st.issues
            if issue.trade_date is not None
        )
        rows = materialize_security_status(
            keys,
            namechanges,
            stock_st,
            ingested_at=ingested_at,
        )
        eligible_count += len(keys)
        upserted_count += store.upsert_stock_status(rows)
        unknown_count += sum(row.is_st is None for row in rows)
        conflict_count += sum(row.conflict_reason is not None for row in rows)
    return SecurityStatusBackfillResult(
        start=start,
        end=end,
        source_as_of=resolved_source_as_of,
        eligible_count=eligible_count,
        upserted_count=upserted_count,
        unknown_count=unknown_count,
        conflict_count=conflict_count,
        source_issue_count=source_issue_count,
        source_issue_dates=tuple(
            sorted(source_issue_dates, reverse=True)[
                :SOURCE_ISSUE_DATE_SAMPLE_LIMIT
            ]
        ),
    )
