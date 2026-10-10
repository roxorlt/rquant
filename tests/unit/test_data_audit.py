from __future__ import annotations

from datetime import date

import duckdb

from rquant.data_catalog.audit import audit, read_report, write_report
from rquant.storage.migrations import initialize_schema


def test_missing_open_day_and_latest_date(tmp_path) -> None:
    con = duckdb.connect(":memory:")
    initialize_schema(con)
    con.execute("INSERT INTO trade_calendar VALUES ('SSE', '2026-01-05', true, NULL, 't', now()), "
                "('SSE', '2026-01-06', true, NULL, 't', now()), "
                "('SSE', '2026-01-07', false, NULL, 't', now())")
    con.execute("INSERT INTO daily_bar (ts_code, trade_date, close) "
                "VALUES ('600000.SH', '2026-01-05', 10)")
    report = audit(con, date(2026, 1, 7), days=5)
    daily = next(d for d in report.datasets if d.dataset_id == "daily_bar")
    assert daily.latest_date == date(2026, 1, 5)
    assert daily.missing_open_days == [date(2026, 1, 6)]
    assert report.open_days == 2
    write_report(report, tmp_path)
    assert read_report(tmp_path) == report
