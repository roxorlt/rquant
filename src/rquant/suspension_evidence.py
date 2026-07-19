"""Shared SQL relation for authoritative suspension-session evidence."""


def suspension_session_evidence_sql(
    event_filter: str = "TRUE",
) -> str:
    """Build a scoped evidence relation from internal SQL predicates."""
    if not event_filter.strip():
        raise ValueError("suspension evidence event filter must not be empty")
    return f"""
WITH grouped AS (
    SELECT suspension.source,
           suspension.ts_code,
           suspension.trade_date,
           count(*) FILTER (
               WHERE suspension.suspend_type = 'S'
                 AND suspension.session_scope = 'full_day'
           ) AS full_day_event_count,
           count(*) FILTER (
               WHERE suspension.suspend_type <> 'S'
                  OR suspension.session_scope <> 'full_day'
           ) AS contradictory_event_count
    FROM stock_suspend_event AS suspension
    JOIN stock_suspend_coverage AS coverage
      ON coverage.source = suspension.source
     AND coverage.trade_date = suspension.trade_date
     AND coverage.coverage_state = 'complete'
    WHERE {event_filter}
    GROUP BY suspension.source, suspension.ts_code, suspension.trade_date
),
classified AS (
    SELECT grouped.*,
           EXISTS (
               SELECT 1
               FROM daily_bar AS daily
               WHERE daily.ts_code = grouped.ts_code
                 AND daily.trade_date = grouped.trade_date
           )
           OR EXISTS (
               SELECT 1
               FROM minute_bar AS minute
               WHERE minute.ts_code = grouped.ts_code
                 AND minute.trade_time >=
                     CAST(grouped.trade_date AS TIMESTAMP)
                 AND minute.trade_time <
                     CAST(grouped.trade_date AS TIMESTAMP) + INTERVAL 1 DAY
                 AND (
                     coalesce(minute.vol, 0) > 0
                     OR coalesce(minute.amount, 0) > 0
                 )
           ) AS has_positive_trading_evidence
    FROM grouped
)
SELECT source,
       ts_code,
       trade_date,
       CASE
           WHEN full_day_event_count > 0
            AND contradictory_event_count = 0
            AND NOT has_positive_trading_evidence
           THEN 'full_day'
           WHEN full_day_event_count > 0
            AND (
                contradictory_event_count > 0
                OR has_positive_trading_evidence
            )
           THEN 'conflict'
           ELSE 'unknown'
       END AS evidence_state
FROM classified
"""
