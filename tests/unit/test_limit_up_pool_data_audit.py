"""Stage-1 audit must block closed-day limit-up-pool contamination."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from rquant.data_quality import limit_up_pool_calendar_audit_rules, run_audit
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay


def test_closed_day_pool_rows_are_p0_audit_findings(tmp_path: Path) -> None:
    database_path = tmp_path / "pool-audit.duckdb"
    closed = date(2026, 7, 12)
    with DuckDBStore(database_path) as store:
        store.upsert_trade_calendar(
            (
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=closed,
                    is_open=False,
                    source="test",
                    updated_at=datetime(2026, 7, 13, tzinfo=UTC),
                ),
            )
        )
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO limit_up_pool_daily (ts_code, trade_date, source)
            VALUES ('600000.SH', ?, 'eastmoney')
            """,
            [closed],
        )

    with DuckDBStore(database_path, read_only=True) as store:
        report = run_audit(
            store,
            limit_up_pool_calendar_audit_rules(closed, closed),
        )

    assert len(report.findings) == 1
    assert report.findings[0].rule_id == "limit-up-pool-calendar-coverage"
    assert report.findings[0].severity == "P0"
    assert report.findings[0].evidence["count"] == 1
    assert report.findings[0].evidence["samples"] == [
        {
            "ts_code": "600000.SH",
            "trade_date": "2026-07-12",
            "source": "eastmoney",
        }
    ]
