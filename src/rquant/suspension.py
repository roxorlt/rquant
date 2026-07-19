"""Authoritative Tushare suspend_d facts and exact query coverage."""

from __future__ import annotations

import hashlib
import json
import time as time_module
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Literal, Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.storage.duckdb import DuckDBStore

SessionScope = Literal["full_day", "partial", "unknown"]
CoverageState = Literal["complete", "unverified_empty", "unsupported"]
TransactionMode = Literal["managed", "existing"]

_FULL_DAY_TIMINGS = frozenset(
    {
        "全天",
        "全日",
        "09:30-15:00",
        "09:30-11:30,13:00-15:00",
        "09:30-11:30，13:00-15:00",
    }
)


class SuspensionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class SuspensionEvent(SuspensionModel):
    source: str = Field(default="tushare", min_length=1)
    ts_code: str = Field(min_length=1)
    trade_date: date
    suspend_type: Literal["S", "R"]
    suspend_timing: str
    session_scope: SessionScope
    available_at: datetime
    ingested_at: datetime

    @field_validator("available_at", "ingested_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("suspension timestamps must be timezone-aware")
        return value.astimezone(UTC)


class SuspensionCoverage(SuspensionModel):
    source: str = Field(default="tushare", min_length=1)
    trade_date: date
    coverage_state: CoverageState
    row_count: int = Field(ge=0)
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    queried_at: datetime

    @field_validator("queried_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("queried_at must be timezone-aware")
        return value.astimezone(UTC)


class SuspensionSnapshot(SuspensionModel):
    coverage: SuspensionCoverage
    events: tuple[SuspensionEvent, ...] = ()

    @model_validator(mode="after")
    def validate_snapshot(self) -> SuspensionSnapshot:
        if self.coverage.row_count != len(self.events):
            raise ValueError("suspension coverage row_count must match events")
        if any(
            event.source != self.coverage.source
            or event.trade_date != self.coverage.trade_date
            for event in self.events
        ):
            raise ValueError("suspension events must match snapshot source/date")
        return self


class SuspensionAdapter(Protocol):
    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame: ...


class SuspensionBackfillResult(SuspensionModel):
    start: date
    end: date
    open_date_count: int = Field(ge=0)
    requested_date_count: int = Field(ge=0)
    persisted_date_count: int = Field(ge=0)
    event_count: int = Field(ge=0)


class SuspensionBackfillPlan(SuspensionModel):
    start: date
    end: date
    missing_only: bool
    open_dates: tuple[date, ...]
    requested_dates: tuple[date, ...]


def plan_suspension_backfill(
    *,
    store_factory: Callable[[], DuckDBStore],
    start: date,
    end: date,
    missing_only: bool = True,
) -> SuspensionBackfillPlan:
    """Resolve authoritative open dates and the exact read-only refresh target."""
    if start > end:
        raise ValueError("suspension backfill start must not be after end")
    with store_factory() as planning_store:
        missing_calendar = planning_store.missing_trade_calendar_dates(
            "SSE",
            start,
            end,
        )
        if missing_calendar:
            from rquant.trade_calendar import TradeCalendarGapError

            raise TradeCalendarGapError("SSE", missing_calendar)
        open_dates = tuple(
            row.cal_date
            for row in planning_store.list_trade_calendar("SSE", start, end)
            if row.is_open
        )
        covered_dates = {
            row[0]
            for row in planning_store._conn.execute(  # noqa: SLF001
                """
                SELECT trade_date
                FROM stock_suspend_coverage
                WHERE source = 'tushare'
                  AND coverage_state = 'complete'
                  AND trade_date BETWEEN ? AND ?
                """,
                [start, end],
            ).fetchall()
        }
    requested_dates = tuple(
        trading_date
        for trading_date in open_dates
        if not missing_only or trading_date not in covered_dates
    )
    return SuspensionBackfillPlan(
        start=start,
        end=end,
        missing_only=missing_only,
        open_dates=open_dates,
        requested_dates=requested_dates,
    )


def _parse_trade_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text)


def _session_scope(suspend_type: str, timing: str) -> SessionScope:
    if suspend_type != "S":
        return "unknown"
    if not timing:
        return "full_day"
    compact = timing.replace(" ", "")
    if compact in _FULL_DAY_TIMINGS:
        return "full_day"
    if any(marker in compact for marker in ("-", "至", ":")):
        return "partial"
    return "unknown"


def _snapshot_hash(events: tuple[SuspensionEvent, ...]) -> str:
    payload = [
        {
            "source": event.source,
            "ts_code": event.ts_code,
            "trade_date": event.trade_date.isoformat(),
            "suspend_type": event.suspend_type,
            "suspend_timing": event.suspend_timing,
            "session_scope": event.session_scope,
        }
        for event in events
    ]
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_suspend_d_snapshot(
    frame: pd.DataFrame,
    *,
    trade_date: date,
    queried_at: datetime,
    source: str = "tushare",
) -> SuspensionSnapshot:
    if queried_at.tzinfo is None or queried_at.utcoffset() is None:
        raise ValueError("queried_at must be timezone-aware")
    required = {"ts_code", "trade_date", "suspend_timing", "suspend_type"}
    if not frame.empty and not required <= set(frame.columns):
        raise ValueError(
            "suspend_d response missing columns: "
            + ", ".join(sorted(required - set(frame.columns)))
        )
    events: list[SuspensionEvent] = []
    for row in frame.to_dict("records"):
        row_date = _parse_trade_date(row["trade_date"])
        if row_date != trade_date:
            raise ValueError("suspend_d response contains a different trade_date")
        suspend_type = str(row["suspend_type"]).strip().upper()
        timing_value = row.get("suspend_timing")
        timing = "" if timing_value is None or pd.isna(timing_value) else str(timing_value).strip()
        events.append(
            SuspensionEvent(
                source=source,
                ts_code=str(row["ts_code"]),
                trade_date=row_date,
                suspend_type=suspend_type,
                suspend_timing=timing,
                session_scope=_session_scope(suspend_type, timing),
                available_at=queried_at,
                ingested_at=queried_at,
            )
        )
    selected = tuple(
        sorted(
            set(events),
            key=lambda item: (
                item.ts_code,
                item.suspend_type,
                item.suspend_timing,
            ),
        )
    )
    coverage = SuspensionCoverage(
        source=source,
        trade_date=trade_date,
        coverage_state="complete",
        row_count=len(selected),
        snapshot_hash=_snapshot_hash(selected),
        queried_at=queried_at,
    )
    return SuspensionSnapshot(coverage=coverage, events=selected)


def persist_suspension_snapshot(
    store: DuckDBStore,
    snapshot: SuspensionSnapshot,
    *,
    transaction_mode: TransactionMode = "managed",
) -> None:
    coverage = snapshot.coverage
    managed = transaction_mode == "managed"
    if managed:
        store._conn.execute("BEGIN")  # noqa: SLF001
    try:
        current = store._conn.execute(  # noqa: SLF001
            """
            SELECT snapshot_hash, queried_at
            FROM stock_suspend_coverage
            WHERE source = ? AND trade_date = ?
            """,
            [coverage.source, coverage.trade_date],
        ).fetchone()
        if current is not None and current[1] > coverage.queried_at:
            if managed:
                store._conn.execute("COMMIT")  # noqa: SLF001
            return
        if (
            current is not None
            and current[1] == coverage.queried_at
            and current[0] != coverage.snapshot_hash
        ):
            raise ValueError(
                "conflicting suspension snapshots at the same queried_at"
            )
        store._conn.execute(  # noqa: SLF001
            "DELETE FROM stock_suspend_event WHERE source = ? AND trade_date = ?",
            [coverage.source, coverage.trade_date],
        )
        if snapshot.events:
            store._conn.executemany(  # noqa: SLF001
                """
                INSERT INTO stock_suspend_event
                (source, ts_code, trade_date, suspend_type, suspend_timing,
                 session_scope, available_at, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        event.source,
                        event.ts_code,
                        event.trade_date,
                        event.suspend_type,
                        event.suspend_timing,
                        event.session_scope,
                        event.available_at,
                        event.ingested_at,
                    )
                    for event in snapshot.events
                ],
            )
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO stock_suspend_coverage
            (source, trade_date, coverage_state, row_count, snapshot_hash, queried_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, trade_date) DO UPDATE SET
                coverage_state = excluded.coverage_state,
                row_count = excluded.row_count,
                snapshot_hash = excluded.snapshot_hash,
                queried_at = excluded.queried_at
            WHERE excluded.queried_at >= stock_suspend_coverage.queried_at
            """,
            [
                coverage.source,
                coverage.trade_date,
                coverage.coverage_state,
                coverage.row_count,
                coverage.snapshot_hash,
                coverage.queried_at,
            ],
        )
        if managed:
            store._conn.execute("COMMIT")  # noqa: SLF001
    except Exception:
        if managed:
            store._conn.execute("ROLLBACK")  # noqa: SLF001
        raise


def backfill_suspension_facts(
    adapter: SuspensionAdapter,
    *,
    store_factory: Callable[[], DuckDBStore],
    start: date,
    end: date,
    queried_at: datetime,
    missing_only: bool = True,
    request_interval_seconds: float = 0.15,
    sleep: Callable[[float], None] = time_module.sleep,
) -> SuspensionBackfillResult:
    """Fetch exact full-market snapshots without holding a DuckDB connection."""
    if queried_at.tzinfo is None or queried_at.utcoffset() is None:
        raise ValueError("queried_at must be timezone-aware")
    if request_interval_seconds < 0:
        raise ValueError("request_interval_seconds must not be negative")

    plan = plan_suspension_backfill(
        store_factory=store_factory,
        start=start,
        end=end,
        missing_only=missing_only,
    )
    snapshots: list[SuspensionSnapshot] = []
    for index, trading_date in enumerate(plan.requested_dates):
        frame = adapter.suspend_d_raw(trading_date)
        snapshots.append(
            normalize_suspend_d_snapshot(
                frame,
                trade_date=trading_date,
                queried_at=queried_at,
            )
        )
        if index + 1 < len(plan.requested_dates) and request_interval_seconds:
            sleep(request_interval_seconds)

    if snapshots:
        with store_factory() as writer:
            writer._conn.execute("BEGIN")  # noqa: SLF001
            try:
                for snapshot in snapshots:
                    persist_suspension_snapshot(
                        writer,
                        snapshot,
                        transaction_mode="existing",
                    )
                writer._conn.execute("COMMIT")  # noqa: SLF001
            except Exception:
                writer._conn.execute("ROLLBACK")  # noqa: SLF001
                raise
    return SuspensionBackfillResult(
        start=start,
        end=end,
        open_date_count=len(plan.open_dates),
        requested_date_count=len(plan.requested_dates),
        persisted_date_count=len(snapshots),
        event_count=sum(len(snapshot.events) for snapshot in snapshots),
    )
