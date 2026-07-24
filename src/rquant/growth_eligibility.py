"""Shared structural eligibility facts for the growth-board strategy."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict

from rquant.storage.duckdb import DuckDBStore
from rquant.suspension_evidence import suspension_session_evidence_sql

_GROWTH_STRUCTURE_BATCH_SIZE = 8


class GrowthOpeningStructure(BaseModel):
    """One authoritative structural exclusion at the opening decision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    target_date: date
    previous_date: date
    ts_code: str
    reason: Literal[
        "insufficient_possible_sessions",
        "previous_full_day_suspension",
        "suspension_input_conflict",
        "listing_fact_unavailable",
        "listing_fact_conflict",
    ]

    @property
    def is_deterministic_non_candidate(self) -> bool:
        return self.reason in {
            "insufficient_possible_sessions",
            "previous_full_day_suspension",
        }


def classify_growth_opening_structure(
    store: DuckDBStore,
    date_pairs: Mapping[date, date],
    *,
    batch_size: int = _GROWTH_STRUCTURE_BATCH_SIZE,
) -> tuple[GrowthOpeningStructure, ...]:
    """Classify structural non-candidates and suspension input conflicts."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not date_pairs:
        return ()
    ordered_pairs = sorted(date_pairs.items())
    facts: list[GrowthOpeningStructure] = []
    for start in range(0, len(ordered_pairs), batch_size):
        facts.extend(
            _classify_growth_opening_structure_batch(
                store,
                dict(ordered_pairs[start : start + batch_size]),
            )
        )
    return tuple(
        sorted(
            facts,
            key=lambda fact: (fact.target_date, fact.ts_code, fact.reason),
        )
    )


def _classify_growth_opening_structure_batch(
    store: DuckDBStore,
    date_pairs: Mapping[date, date],
) -> tuple[GrowthOpeningStructure, ...]:
    """Classify one bounded target-date batch with unchanged PIT semantics."""
    values = ", ".join("(?, ?)" for _ in date_pairs)
    parameters = [
        value
        for target_date, previous_date in sorted(date_pairs.items())
        for value in (target_date, previous_date)
    ]
    parameters.append(max(date_pairs.values()))
    try:
        store._conn.execute("SELECT 1 FROM stock_suspend_session_evidence LIMIT 0")
    except duckdb.CatalogException:
        evidence_sql = suspension_session_evidence_sql(
            "suspension.source = 'tushare' AND suspension.trade_date <= ?"
        )
    else:
        evidence_sql = """
            SELECT source, ts_code, trade_date, evidence_state
            FROM stock_suspend_session_evidence AS suspension
            WHERE suspension.source = 'tushare'
              AND suspension.trade_date <= ?
        """
    rows = store._conn.execute(
        f"""
        WITH requested(target_date, previous_date) AS (
            VALUES {values}
        ),
        suspension_evidence AS (
            {evidence_sql}
        ),
        full_day_suspension AS (
            SELECT suspension.ts_code, suspension.trade_date
            FROM suspension_evidence AS suspension
            WHERE suspension.source = 'tushare'
              AND suspension.evidence_state = 'full_day'
        ),
        suspension_conflict AS (
            SELECT suspension.ts_code, suspension.trade_date
            FROM suspension_evidence AS suspension
            WHERE suspension.source = 'tushare'
              AND suspension.evidence_state = 'conflict'
        ),
        universe AS (
            SELECT requested.target_date,
                   requested.previous_date,
                   basic.ts_code
            FROM requested
            JOIN stock_basic AS basic
              ON basic.list_date IS NOT NULL
             AND basic.list_date <= requested.previous_date
             AND (
                    basic.ts_code LIKE '300%.SZ'
                 OR basic.ts_code LIKE '301%.SZ'
                 OR basic.ts_code LIKE '688%.SH'
                 OR basic.ts_code LIKE '689%.SH'
             )
            UNION
            SELECT requested.target_date,
                   requested.previous_date,
                   status.ts_code
            FROM requested
            JOIN stock_status_daily AS status
              ON status.trade_date = requested.previous_date
             AND (
                    status.ts_code LIKE '300%.SZ'
                 OR status.ts_code LIKE '301%.SZ'
                 OR status.ts_code LIKE '688%.SH'
                 OR status.ts_code LIKE '689%.SH'
             )
            UNION
            SELECT requested.target_date,
                   requested.previous_date,
                   bar.ts_code
            FROM requested
            JOIN daily_bar AS bar
              ON bar.trade_date = requested.previous_date
             AND (
                    bar.ts_code LIKE '300%.SZ'
                 OR bar.ts_code LIKE '301%.SZ'
                 OR bar.ts_code LIKE '688%.SH'
                 OR bar.ts_code LIKE '689%.SH'
             )
        ),
        facts AS (
            SELECT universe.target_date,
                   universe.previous_date,
                   universe.ts_code,
                   basic.list_date,
                   suspension.ts_code AS suspended_ts_code,
                   conflict.ts_code AS suspension_conflict_ts_code,
                   EXISTS (
                       SELECT 1
                       FROM suspension_conflict AS historical_suspension
                       WHERE historical_suspension.ts_code = universe.ts_code
                         AND historical_suspension.trade_date
                             BETWEEN basic.list_date AND universe.previous_date
                   ) AS has_historical_suspension_conflict
            FROM universe
            LEFT JOIN stock_basic AS basic
              ON basic.ts_code = universe.ts_code
            LEFT JOIN full_day_suspension AS suspension
              ON suspension.ts_code = universe.ts_code
             AND suspension.trade_date = universe.previous_date
            LEFT JOIN suspension_conflict AS conflict
              ON conflict.ts_code = universe.ts_code
             AND conflict.trade_date = universe.previous_date
        ),
        classified AS (
            SELECT target_date,
                   previous_date,
                   ts_code,
                   CASE
                       WHEN suspension_conflict_ts_code IS NOT NULL
                       THEN 'suspension_input_conflict'
                       WHEN has_historical_suspension_conflict
                       THEN 'suspension_input_conflict'
                       WHEN suspended_ts_code IS NOT NULL
                       THEN 'previous_full_day_suspension'
                       WHEN list_date IS NULL
                       THEN 'listing_fact_unavailable'
                       WHEN list_date > previous_date
                       THEN 'listing_fact_conflict'
                       WHEN list_date IS NOT NULL
                        AND list_date <= previous_date
                        AND (
                            date_diff('day', list_date, previous_date) + 1 < 60
                            OR (
                                (
                                    SELECT count(*)
                                    FROM trade_calendar AS civil_calendar
                                    WHERE civil_calendar.exchange = 'SSE'
                                      AND civil_calendar.cal_date BETWEEN
                                          facts.list_date AND facts.previous_date
                                ) = (
                                    date_diff(
                                        'day',
                                        list_date,
                                        previous_date
                                    ) + 1
                                )
                                AND (
                                    SELECT count(*)
                                    FROM trade_calendar AS listing_calendar
                                    WHERE listing_calendar.exchange = 'SSE'
                                      AND listing_calendar.is_open = TRUE
                                      AND listing_calendar.cal_date BETWEEN
                                          facts.list_date AND facts.previous_date
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM full_day_suspension
                                              AS listing_suspension
                                          WHERE listing_suspension.ts_code =
                                                facts.ts_code
                                            AND listing_suspension.trade_date =
                                                listing_calendar.cal_date
                                      )
                                ) < 60
                            )
                        )
                       THEN 'insufficient_possible_sessions'
                   END AS reason
            FROM facts
        )
        SELECT target_date, previous_date, ts_code, reason
        FROM classified
        WHERE reason IS NOT NULL
        ORDER BY target_date, ts_code
        """,
        parameters,
    ).fetchall()
    return tuple(
        GrowthOpeningStructure(
            target_date=target_date,
            previous_date=previous_date,
            ts_code=str(ts_code),
            reason=reason,
        )
        for target_date, previous_date, ts_code, reason in rows
    )
