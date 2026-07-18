"""Shared structural eligibility facts for the growth-board strategy."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.storage.duckdb import DuckDBStore


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
) -> tuple[GrowthOpeningStructure, ...]:
    """Classify structural non-candidates and suspension input conflicts."""
    if not date_pairs:
        return ()
    values = ", ".join("(?, ?)" for _ in date_pairs)
    parameters = [
        value
        for target_date, previous_date in sorted(date_pairs.items())
        for value in (target_date, previous_date)
    ]
    rows = store._conn.execute(
        f"""
        WITH requested(target_date, previous_date) AS (
            VALUES {values}
        ),
        full_day_suspension AS (
            SELECT suspension.ts_code, suspension.trade_date
            FROM stock_suspend_event AS suspension
            JOIN stock_suspend_coverage AS coverage
              ON coverage.source = suspension.source
             AND coverage.trade_date = suspension.trade_date
             AND coverage.coverage_state = 'complete'
            WHERE suspension.source = 'tushare'
            GROUP BY suspension.ts_code, suspension.trade_date
            HAVING count(*) FILTER (
                       WHERE suspension.suspend_type = 'S'
                         AND suspension.session_scope = 'full_day'
                   ) > 0
               AND count(*) FILTER (
                       WHERE suspension.suspend_type <> 'S'
                          OR suspension.session_scope <> 'full_day'
                   ) = 0
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
                   bar.ts_code AS daily_ts_code,
                   indicator.ts_code AS indicator_ts_code,
                   suspension.ts_code AS suspended_ts_code,
                   EXISTS (
                       SELECT 1
                       FROM full_day_suspension AS historical_suspension
                       WHERE historical_suspension.ts_code = universe.ts_code
                         AND historical_suspension.trade_date
                             BETWEEN basic.list_date AND universe.previous_date
                         AND (
                             EXISTS (
                                 SELECT 1
                                 FROM daily_bar AS conflicting_daily
                                 WHERE conflicting_daily.ts_code =
                                       historical_suspension.ts_code
                                   AND conflicting_daily.trade_date =
                                       historical_suspension.trade_date
                             )
                             OR EXISTS (
                                 SELECT 1
                                 FROM daily_indicator AS conflicting_indicator
                                 WHERE conflicting_indicator.ts_code =
                                       historical_suspension.ts_code
                                   AND conflicting_indicator.trade_date =
                                       historical_suspension.trade_date
                             )
                         )
                   ) AS has_historical_suspension_conflict
            FROM universe
            LEFT JOIN stock_basic AS basic
              ON basic.ts_code = universe.ts_code
            LEFT JOIN daily_bar AS bar
              ON bar.ts_code = universe.ts_code
             AND bar.trade_date = universe.previous_date
            LEFT JOIN daily_indicator AS indicator
              ON indicator.ts_code = universe.ts_code
             AND indicator.trade_date = universe.previous_date
            LEFT JOIN full_day_suspension AS suspension
              ON suspension.ts_code = universe.ts_code
             AND suspension.trade_date = universe.previous_date
        ),
        classified AS (
            SELECT target_date,
                   previous_date,
                   ts_code,
                   CASE
                       WHEN suspended_ts_code IS NOT NULL
                        AND (
                            daily_ts_code IS NOT NULL
                            OR indicator_ts_code IS NOT NULL
                        )
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
