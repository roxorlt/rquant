"""Bind historical screen candidates to adjacent SSE days in one frozen transaction."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
from pydantic import model_validator

from rquant.backtest.calendar_source import (
    RetrospectiveSSECalendarSnapshot,
    verify_retrospective_sse_calendar,
)
from rquant.backtest.screen_source import (
    VerifiedScreenCandidateSnapshot,
    verify_screen_candidates,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

MAX_PRESET_NAME_LENGTH = 256


class ScreenCalendarSourceError(ValueError):
    """One frozen file cannot bind a screen receipt to the decision day's SSE predecessor."""


class VerifiedScreenCalendarCandidates(RuntimeContractModel):
    """A candidate slice only; ranking, execution, and first visibility remain unproven."""

    source_mode: Literal["screen_receipt_with_retrospective_sse_calendar"] = (
        "screen_receipt_with_retrospective_sse_calendar"
    )
    screen: VerifiedScreenCandidateSnapshot
    calendar: RetrospectiveSSECalendarSnapshot

    @model_validator(mode="after")
    def validate_binding(self) -> Self:
        decision = self.screen.decision_trade_date
        if self.calendar.requested_start != decision or self.calendar.requested_end != decision:
            raise ValueError("SSE calendar must be requested for the decision day")
        dates = self.calendar.calendar.dates
        if decision not in dates:
            raise ValueError("decision day must be SSE open")
        index = dates.index(decision)
        if index == 0 or dates[index - 1] != self.screen.source_trade_date:
            raise ValueError("screen source must be the previous SSE open day")
        return self

    @property
    def source_identity(self) -> str:
        return canonical_sha256(
            {
                "source_mode": self.source_mode,
                "calendar_identity": self.calendar.source_identity,
                "screen_result_version": self.screen.result_version,
                "source_trade_date": self.screen.source_trade_date,
                "decision_trade_date": self.screen.decision_trade_date,
                "preset_name": self.screen.preset_name,
            }
        )


def _check_input(source_trade_date: date, decision_trade_date: date, preset_name: str) -> None:
    if type(source_trade_date) is not date or type(decision_trade_date) is not date:
        raise ScreenCalendarSourceError("source and decision must be civil dates")
    if source_trade_date >= decision_trade_date:
        raise ScreenCalendarSourceError("decision date must follow source date")
    if not isinstance(preset_name, str) or not (
        0 < len(preset_name.strip()) <= MAX_PRESET_NAME_LENGTH
    ):
        raise ScreenCalendarSourceError(
            "screen preset name is required and must be at most 256 chars"
        )


def verify_screen_calendar_candidates(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedScreenCalendarCandidates:
    """Verify both sources using the caller's existing frozen read-only transaction."""
    _check_input(source_trade_date, decision_trade_date, preset_name)
    calendar = verify_retrospective_sse_calendar(
        connection, start=decision_trade_date, end=decision_trade_date
    )
    dates = calendar.calendar.dates
    index = dates.index(decision_trade_date)
    if index == 0 or dates[index - 1] != source_trade_date:
        raise ScreenCalendarSourceError("screen source must be the previous SSE open day")
    screen = verify_screen_candidates(
        connection, source_trade_date, decision_trade_date, preset_name
    )
    return VerifiedScreenCalendarCandidates(screen=screen, calendar=calendar)


def load_screen_calendar_candidates(
    frozen_path: Path,
    *,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedScreenCalendarCandidates:
    """Use one read-only DuckDB transaction for the screen and retrospective calendar."""
    _check_input(source_trade_date, decision_trade_date, preset_name)
    if not frozen_path.is_file():
        raise ScreenCalendarSourceError("an existing frozen DuckDB file is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise ScreenCalendarSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            evidence = verify_screen_calendar_candidates(
                connection,
                source_trade_date=source_trade_date,
                decision_trade_date=decision_trade_date,
                preset_name=preset_name,
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return evidence
    except duckdb.Error as exc:
        raise ScreenCalendarSourceError(
            "frozen candidate or calendar tables are unavailable"
        ) from exc
    finally:
        connection.close()
