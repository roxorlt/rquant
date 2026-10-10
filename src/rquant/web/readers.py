"""Fixed, bounded queries on one borrowed generation cursor.

Only tables the Serving generation already publishes are read; nothing here adds a
projection. A page table whose ``projection_status`` says it is unpublished reads as
empty, so a missing source shows as a placeholder, never as an error.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from rquant.dashboard.runtime_console_data import (
    ConsoleLimits,
    RuntimeConsoleSections,
    read_runtime_console_sections,
)
from rquant.web.calendar import CALENDAR_EXCHANGE

#: The console's per-section ceilings, raised to what one trading day can need.
CONSOLE_LIMITS = ConsoleLimits(
    services=200,
    signals=500,
    deliveries=500,
    paper_accounts=20,
    paper_holdings=500,
    lab_jobs=1,
    promotions=1,
)
_MAX_NAMES = 1_000
_MAX_CANVAS_HITS = 500


@dataclass(frozen=True)
class TableState:
    table_name: str
    available: bool
    row_count: int
    available_at: datetime | None


def sections(cursor: Any) -> RuntimeConsoleSections:
    return read_runtime_console_sections(cursor, limits=CONSOLE_LIMITS)


def table_states(cursor: Any) -> dict[str, TableState]:
    rows = cursor.execute(
        "SELECT table_name, available, row_count, available_at FROM projection_status "
        "ORDER BY table_name LIMIT 256"
    ).fetchall()
    return {
        str(name): TableState(
            table_name=str(name),
            available=bool(available),
            row_count=int(row_count or 0),
            available_at=None if available_at is None else available_at.astimezone(UTC),
        )
        for name, available, row_count, available_at in rows
    }


def _readable(tables: Mapping[str, TableState], name: str) -> bool:
    state = tables.get(name)
    return state is not None and state.available


def stock_names(
    cursor: Any, tables: Mapping[str, TableState], codes: Iterable[str]
) -> dict[str, str]:
    wanted = sorted(set(codes))[:_MAX_NAMES]
    if not wanted or not _readable(tables, "stock_basic"):
        return {}
    marks = ",".join("?" for _ in wanted)
    rows = cursor.execute(
        f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({marks})",
        tuple(wanted),
    ).fetchall()
    return {str(code): str(name) for code, name in rows if name}


def stock_count(cursor: Any, tables: Mapping[str, TableState]) -> int | None:
    if not _readable(tables, "stock_basic"):
        return None
    row = cursor.execute("SELECT count(*) FROM stock_basic").fetchone()
    return None if row is None else int(row[0])


@dataclass(frozen=True)
class CanvasHit:
    trade_date: date
    preset_name: str
    ts_code: str
    name: str | None
    close: float | None
    pct_chg: float | None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def canvas_hits(cursor: Any, tables: Mapping[str, TableState]) -> list[CanvasHit]:
    """The latest daily screen's members (``canvas_hit`` holds the latest date only)."""

    if not _readable(tables, "canvas_hit"):
        return []
    rows = cursor.execute(
        "SELECT trade_date, preset_name, ts_code, row_json FROM canvas_hit "
        "WHERE trade_date = (SELECT max(trade_date) FROM canvas_hit) "
        "ORDER BY preset_name, ts_code LIMIT ?",
        (_MAX_CANVAS_HITS,),
    ).fetchall()
    hits: list[CanvasHit] = []
    for trade_date, preset_name, ts_code, row_json in rows:
        try:
            payload = json.loads(row_json) if row_json else {}
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        name = payload.get("name")
        hits.append(
            CanvasHit(
                trade_date=trade_date,
                preset_name=str(preset_name),
                ts_code=str(ts_code),
                name=str(name) if isinstance(name, str) and name else None,
                close=_number(payload.get("close")),
                pct_chg=_number(payload.get("pct_chg")),
            )
        )
    return hits


def minute_latest(cursor: Any, tables: Mapping[str, TableState]) -> datetime | None:
    if not _readable(tables, "minute_coverage"):
        return None
    row = cursor.execute("SELECT max(max_time) FROM minute_coverage WHERE is_total").fetchone()
    return None if row is None or row[0] is None else row[0].astimezone(UTC)


def daily_latest(cursor: Any, tables: Mapping[str, TableState]) -> date | None:
    """Latest daily bar: the operations summary's, else the chart projection's."""

    if _readable(tables, "dashboard_summary"):
        row = cursor.execute(
            "SELECT latest_daily_bar FROM dashboard_summary WHERE snapshot_key = 'current'"
        ).fetchone()
        if row is not None and row[0] is not None:
            return row[0]
    if _readable(tables, "daily_bar"):
        row = cursor.execute("SELECT max(trade_date) FROM daily_bar").fetchone()
        if row is not None and row[0] is not None:
            return row[0]
    return None


def screen_latest(cursor: Any, tables: Mapping[str, TableState]) -> date | None:
    if not _readable(tables, "canvas_latest_trade_date"):
        return None
    row = cursor.execute(
        "SELECT trade_date FROM canvas_latest_trade_date WHERE snapshot_key = 'current'"
    ).fetchone()
    return None if row is None else row[0]


def calendar_last_date(cursor: Any, tables: Mapping[str, TableState]) -> date | None:
    if not _readable(tables, "trade_calendar"):
        return None
    row = cursor.execute(
        "SELECT max(trade_date) FROM trade_calendar WHERE exchange = ? AND is_open",
        (CALENDAR_EXCHANGE,),
    ).fetchone()
    return None if row is None else row[0]


def trading_days_after(
    cursor: Any,
    tables: Mapping[str, TableState],
    start: date,
    end: date,
) -> int | None:
    """Open days in ``(start, end]``."""

    if not _readable(tables, "trade_calendar"):
        return None
    row = cursor.execute(
        "SELECT count(*) FROM trade_calendar "
        "WHERE exchange = ? AND is_open AND trade_date > ? AND trade_date <= ?",
        (CALENDAR_EXCHANGE, start, end),
    ).fetchone()
    return None if row is None else int(row[0])


__all__ = [
    "CONSOLE_LIMITS",
    "CanvasHit",
    "TableState",
    "calendar_last_date",
    "canvas_hits",
    "daily_latest",
    "minute_latest",
    "screen_latest",
    "sections",
    "stock_count",
    "stock_names",
    "table_states",
    "trading_days_after",
]
