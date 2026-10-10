"""A fixed read-only DuckDB generation supplies one day's quality facts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant import data_audit_evidence as evidence
from rquant.storage.schema import (
    DAILY_BAR_DDL,
    DAILY_STATE_DDL,
    STOCK_SUSPEND_COVERAGE_DDL,
    STOCK_SUSPEND_EVENT_DDL,
    TRADE_CALENDAR_DDL,
)
from rquant.suspension import normalize_suspend_d_snapshot

DAY = date(2026, 9, 25)
QUERIED_AT = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
CODE = "600000.SH"


def _database(
    path: Path,
    bars: tuple[tuple[str, float | None, float | None], ...],
    *,
    calendar_open: bool = True,
    suspension_tables: bool = True,
) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.execute(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, ?, 'synthetic', ?)",
            (DAY, calendar_open, QUERIED_AT),
        )
        if bars:
            connection.executemany(
                "INSERT INTO daily_bar (ts_code, trade_date, close, vol) VALUES (?, ?, ?, ?)",
                [(code, DAY, close, volume) for code, close, volume in bars],
            )
        if suspension_tables:
            connection.execute(STOCK_SUSPEND_EVENT_DDL)
            connection.execute(STOCK_SUSPEND_COVERAGE_DDL)
    return path


def _suspension_snapshot(
    path: Path,
    *,
    events: tuple[tuple[str, str, str], ...] = (),
    coverage_state: str = "complete",
    corrupt: str | None = None,
    queried_at: datetime = QUERIED_AT,
) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_date": DAY,
                "suspend_type": kind,
                "suspend_timing": timing,
            }
            for code, kind, timing in events
        ]
    )
    snapshot = normalize_suspend_d_snapshot(frame, trade_date=DAY, queried_at=queried_at)
    with duckdb.connect(str(path)) as connection:
        if snapshot.events:
            connection.executemany(
                "INSERT INTO stock_suspend_event "
                "(source, ts_code, trade_date, suspend_type, suspend_timing, session_scope, "
                "available_at, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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
        connection.execute(
            "INSERT INTO stock_suspend_coverage "
            "(source, trade_date, coverage_state, row_count, snapshot_hash, queried_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "tushare",
                DAY,
                coverage_state,
                snapshot.coverage.row_count + (1 if corrupt == "count" else 0),
                "0" * 64 if corrupt == "hash" else snapshot.coverage.snapshot_hash,
                queried_at,
            ),
        )


def _audit(path: Path) -> evidence.DailyBarQualityReport:
    with duckdb.connect(str(path), read_only=True) as connection:
        return evidence.audit_daily_bar_quality_from_connection(
            connection,
            snapshot_id="verified-generation-1",
            completed_trade_date=DAY,
            null_fields=(
                evidence.DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
                evidence.DailyBarNullFieldSpec(
                    field_name="vol", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )


def _unassessed(report: evidence.DailyBarQualityReport) -> set[tuple[str, str, str | None, int]]:
    return {(item.rule_id, item.reason, item.field_name, item.count) for item in report.unassessed}


def test_real_sql_reports_null_ratio_zero_volume_and_unproven_limits(tmp_path: Path) -> None:
    path = _database(tmp_path / "quality.duckdb", ((CODE, 12.0, 0.0), ("000001.SZ", None, 10.0)))
    _suspension_snapshot(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(DAILY_STATE_DDL)
        connection.execute(
            "INSERT INTO daily_state (ts_code, trade_date, limit_up_price, limit_down_price) "
            "VALUES (?, ?, 11, 9)",
            (CODE, DAY),
        )

    report = _audit(path)

    assert report.snapshot_id == "verified-generation-1"
    assert report.trade_date == DAY
    assert {(issue.rule_id, issue.ts_code, issue.field_name) for issue in report.issues} == {
        ("daily_bar.zero_volume_unsuspended", CODE, None),
        ("daily_bar.field_null_ratio", None, "close"),
    }
    null_issue = next(issue for issue in report.issues if issue.field_name == "close")
    assert (null_issue.null_rows, null_issue.observed_rows) == (1, 2)
    assert _unassessed(report) == {
        ("daily_bar.close_limit", "close_missing", None, 1),
        ("daily_bar.close_limit", "limits_unavailable", None, 1),
    }
    assert all(item.rule_id != "daily_bar.close_above_limit" for item in report.issues)


def test_complete_empty_suspend_snapshot_proves_not_suspended(tmp_path: Path) -> None:
    path = _database(tmp_path / "empty-suspend.duckdb", ((CODE, 10.0, 0.0),))
    _suspension_snapshot(path)

    report = _audit(path)

    assert [item.rule_id for item in report.issues] == ["daily_bar.zero_volume_unsuspended"]
    assert ("daily_bar.zero_volume", "suspension_unknown", None, 1) not in _unassessed(report)


def test_preclose_suspend_snapshot_cannot_prove_full_day_absence(tmp_path: Path) -> None:
    path = _database(tmp_path / "early-suspend.duckdb", ((CODE, 10.0, 0.0),))
    _suspension_snapshot(path, queried_at=datetime(2026, 9, 25, 6, 0, tzinfo=UTC))

    report = _audit(path)

    assert report.issues == ()
    assert ("daily_bar.zero_volume", "suspension_unknown", None, 1) in _unassessed(report)


@pytest.mark.parametrize(
    ("coverage_state", "corrupt", "events"),
    [
        (None, None, ()),
        ("unverified_empty", None, ()),
        ("complete", "count", ()),
        ("complete", "hash", ((CODE, "S", "全天"),)),
        ("complete", None, ((CODE, "S", "全天"),)),
        ("complete", None, ((CODE, "S", "09:30-10:30"),)),
    ],
)
def test_missing_or_conflicting_suspension_proof_stays_unassessed(
    tmp_path: Path,
    coverage_state: str | None,
    corrupt: str | None,
    events: tuple[tuple[str, str, str], ...],
) -> None:
    path = _database(tmp_path / "unknown-suspend.duckdb", ((CODE, 10.0, 0.0),))
    if coverage_state is not None:
        _suspension_snapshot(path, coverage_state=coverage_state, corrupt=corrupt, events=events)

    report = _audit(path)

    assert report.issues == ()
    assert ("daily_bar.zero_volume", "suspension_unknown", None, 1) in _unassessed(report)


def test_missing_suspension_tables_do_not_block_null_counting(tmp_path: Path) -> None:
    path = _database(tmp_path / "legacy.duckdb", ((CODE, None, 0.0),), suspension_tables=False)

    report = _audit(path)

    assert ("daily_bar.field_null_ratio", "close") in {
        (item.rule_id, item.field_name) for item in report.issues
    }
    assert ("daily_bar.zero_volume", "suspension_unknown", None, 1) in _unassessed(report)


def test_no_day_rows_returns_explicit_no_observations(tmp_path: Path) -> None:
    path = _database(tmp_path / "no-bars.duckdb", ())
    _suspension_snapshot(path)

    report = _audit(path)

    assert report.issues == ()
    assert _unassessed(report) == {
        ("daily_bar.field_null_ratio", "no_observations", "close", 1),
        ("daily_bar.field_null_ratio", "no_observations", "vol", 1),
    }


def test_more_than_ten_thousand_bars_is_rejected_before_partial_quality(tmp_path: Path) -> None:
    path = _database(tmp_path / "too-many.duckdb", ())
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close, vol) "
            "SELECT lpad(i::VARCHAR, 6, '0') || '.SH', ?, 10, 100 "
            "FROM range(10001) AS t(i)",
            (DAY,),
        )

    with pytest.raises(ValueError, match="10,000|10000|limit"):
        _audit(path)


def test_duplicate_daily_code_and_nonfinite_value_fail_closed(tmp_path: Path) -> None:
    path = _database(tmp_path / "corrupt.duckdb", ((CODE, 10.0, 100.0),))
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE no_key AS SELECT * FROM daily_bar")
        connection.execute("DROP TABLE daily_bar")
        connection.execute("ALTER TABLE no_key RENAME TO daily_bar")
        connection.execute("INSERT INTO daily_bar SELECT * FROM daily_bar")
    with pytest.raises(ValueError, match="unique|duplicate"):
        _audit(path)

    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM daily_bar WHERE rowid = (SELECT MAX(rowid) FROM daily_bar)")
        connection.execute("UPDATE daily_bar SET close = 'NaN'::DOUBLE")
    with pytest.raises(ValueError, match="finite|nan|number"):
        _audit(path)


@pytest.mark.parametrize("bad_open", [float("nan"), float("inf")])
def test_selected_null_field_nonfinite_value_fails_closed(tmp_path: Path, bad_open: float) -> None:
    path = _database(tmp_path / "nonfinite-open.duckdb", ((CODE, 10.0, 100.0),))
    with duckdb.connect(str(path)) as connection:
        connection.execute("UPDATE daily_bar SET open = ? WHERE ts_code = ?", (bad_open, CODE))

    with (
        duckdb.connect(str(path), read_only=True) as connection,
        pytest.raises(ValueError, match="non-finite|invalid"),
    ):
        evidence.audit_daily_bar_quality_from_connection(
            connection,
            snapshot_id="verified-generation-1",
            completed_trade_date=DAY,
            null_fields=(
                evidence.DailyBarNullFieldSpec(
                    field_name="open", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )


def test_requires_open_sse_day_read_only_connection_and_explicit_fields(tmp_path: Path) -> None:
    path = _database(tmp_path / "wrong-connection.duckdb", ((CODE, 10.0, 100.0),))
    with duckdb.connect(str(path)) as connection, pytest.raises(ValueError, match="read-only"):
        evidence.audit_daily_bar_quality_from_connection(
            connection,
            snapshot_id="verified-generation-1",
            completed_trade_date=DAY,
            null_fields=(
                evidence.DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )
    with duckdb.connect(str(path), read_only=True) as connection:
        with pytest.raises(ValueError, match="snapshot_id"):
            evidence.audit_daily_bar_quality_from_connection(
                connection,
                snapshot_id="  ",
                completed_trade_date=DAY,
                null_fields=(
                    evidence.DailyBarNullFieldSpec(
                        field_name="close", max_null_numerator=0, max_null_denominator=1
                    ),
                ),
            )
        with pytest.raises(ValueError, match="null_fields"):
            evidence.audit_daily_bar_quality_from_connection(
                connection,
                snapshot_id="verified-generation-1",
                completed_trade_date=DAY,
                null_fields=(),
            )
    with duckdb.connect(str(path)) as connection:
        connection.execute("UPDATE trade_calendar SET is_open = FALSE WHERE cal_date = ?", (DAY,))
    with pytest.raises(ValueError, match="SSE|open"):
        _audit(path)


def test_rejects_unsupported_null_field_spec() -> None:
    with pytest.raises(ValueError):
        evidence.DailyBarNullFieldSpec(
            field_name="ts_code); DROP TABLE daily_bar;--",
            max_null_numerator=0,
            max_null_denominator=1,
        )


def test_optional_field_threshold_counts_use_exact_integer_rule(tmp_path: Path) -> None:
    path = _database(
        tmp_path / "threshold.duckdb",
        ((CODE, None, 100.0), ("000001.SZ", 10.0, 100.0)),
    )
    with duckdb.connect(str(path), read_only=True) as connection:
        report = evidence.audit_daily_bar_quality_from_connection(
            connection,
            snapshot_id="verified-generation-1",
            completed_trade_date=DAY,
            null_fields=(
                evidence.DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=1, max_null_denominator=2
                ),
            ),
        )

    assert all(item.rule_id != "daily_bar.field_null_ratio" for item in report.issues)
    assert ("daily_bar.close_limit", "close_missing", None, 1) in _unassessed(report)
