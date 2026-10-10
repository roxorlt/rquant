"""The replay's pre-open replica keeps historical monitor events without future evidence."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb

from rquant.storage.schema import MONITOR_EVENT_DDL
from scripts.route_a_day_replay import ProductionAudit, extract_replica

TRADE_DATE = date(2026, 7, 31)
PRIOR_DATE = TRADE_DATE - timedelta(days=1)


def _extract(tmp_path: Path, *, source_has_monitor_event: bool) -> tuple[Path, dict[str, object]]:
    source = tmp_path / "source.duckdb"
    with duckdb.connect(str(source)) as connection:
        if source_has_monitor_event:
            connection.execute(MONITOR_EVENT_DDL)
            connection.executemany(
                "INSERT INTO monitor_event "
                "(trade_date, ts_code, level, trigger_time) VALUES (?, ?, ?, ?)",
                [
                    (
                        PRIOR_DATE - timedelta(days=1),
                        "600001.SH",
                        "older",
                        datetime.combine(PRIOR_DATE - timedelta(days=1), time(10, 0)),
                    ),
                    (
                        PRIOR_DATE,
                        "600002.SH",
                        "historical",
                        datetime.combine(PRIOR_DATE, time(14, 0)),
                    ),
                    (
                        PRIOR_DATE,
                        "600003.SH",
                        "future_trigger",
                        datetime.combine(TRADE_DATE, time(0, 1)),
                    ),
                    (
                        TRADE_DATE,
                        "600004.SH",
                        "before_open",
                        datetime.combine(TRADE_DATE, time(9, 0)),
                    ),
                    (TRADE_DATE, "600005.SH", "future", datetime.combine(TRADE_DATE, time(10, 0))),
                    (
                        TRADE_DATE + timedelta(days=1),
                        "600006.SH",
                        "next_day",
                        datetime.combine(TRADE_DATE + timedelta(days=1), time(10, 0)),
                    ),
                ],
            )
            connection.execute(
                "UPDATE monitor_event SET trigger_price = 12.3, level_price = 12.0, "
                "trigger_type = 'attack', pool = 'pool2', body_upper = 12.5, body_lower = 11.5 "
                "WHERE ts_code = '600002.SH'"
            )
    before = source.stat()
    target = tmp_path / "target.duckdb"
    result = extract_replica(
        replica=source,
        target=target,
        minutes_target=tmp_path / "minutes.parquet",
        trade_date=TRADE_DATE,
        calendar=SimpleNamespace(open_dates=(PRIOR_DATE, TRADE_DATE)),
        daily_sessions=1,
        minute_sessions=1,
        synced_at=datetime.combine(TRADE_DATE, time(9, 15), tzinfo=UTC),
        audit=ProductionAudit((source.parent,)),
    )
    after = source.stat()
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
    return target, result


def _canonical_schema() -> list[tuple[object, ...]]:
    with duckdb.connect(":memory:") as expected:
        expected.execute(MONITOR_EVENT_DDL)
        return expected.execute("PRAGMA table_info('monitor_event')").fetchall()


def test_extract_keeps_only_pre_open_historical_monitor_events_with_canonical_schema(
    tmp_path: Path,
) -> None:
    target, result = _extract(tmp_path, source_has_monitor_event=True)

    with duckdb.connect(str(target), read_only=True) as connection:
        assert (
            connection.execute("PRAGMA table_info('monitor_event')").fetchall()
            == _canonical_schema()
        )
        assert connection.execute(
            "SELECT trade_date, ts_code, level, trigger_time FROM monitor_event ORDER BY trade_date"
        ).fetchall() == [
            (
                PRIOR_DATE - timedelta(days=1),
                "600001.SH",
                "older",
                datetime.combine(PRIOR_DATE - timedelta(days=1), time(10, 0)),
            ),
            (PRIOR_DATE, "600002.SH", "historical", datetime.combine(PRIOR_DATE, time(14, 0))),
        ]
        assert connection.execute(
            "SELECT trigger_price, level_price, trigger_type, pool, body_upper, body_lower "
            "FROM monitor_event WHERE ts_code = '600002.SH'"
        ).fetchone() == (12.3, 12.0, "attack", "pool2", 12.5, 11.5)
    assert result["rows"]["monitor_event"] == 2
    assert "monitor_event" in result["tables_on_host"]
    assert "monitor_event" not in result["empty_tables_created"]


def test_extract_creates_and_reports_empty_canonical_monitor_event_without_source_table(
    tmp_path: Path,
) -> None:
    target, result = _extract(tmp_path, source_has_monitor_event=False)

    with duckdb.connect(str(target), read_only=True) as connection:
        assert (
            connection.execute("PRAGMA table_info('monitor_event')").fetchall()
            == _canonical_schema()
        )
        assert connection.execute("SELECT count(*) FROM monitor_event").fetchone() == (0,)
    assert result["rows"]["monitor_event"] == 0
    assert "monitor_event" not in result["tables_on_host"]
    assert "monitor_event" in result["empty_tables_created"]
