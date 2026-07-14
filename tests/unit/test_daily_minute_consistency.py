from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.data_quality import (
    DEFAULT_MINUTE_SOURCE_SESSION_SPECS,
    AuditReport,
    MinuteSessionWindow,
    MinuteSourceSessionSpec,
    daily_minute_consistency_audit_rules,
    run_audit,
)
from rquant.storage.duckdb import DuckDBStore


def _minute_times(start: time, end: time) -> tuple[time, ...]:
    current = datetime.combine(date(2026, 6, 26), start)
    final = datetime.combine(date(2026, 6, 26), end)
    values: list[time] = []
    while current <= final:
        values.append(current.time())
        current += timedelta(minutes=1)
    return tuple(values)


def _insert_eligible_daily(
    store: DuckDBStore,
    *,
    ts_code: str,
    trade_date: date,
) -> None:
    store._conn.execute(  # noqa: SLF001
        """
        INSERT INTO daily_bar (ts_code, trade_date, close, vol, amount)
        VALUES (?, ?, 10.0, 1000.0, 10000.0)
        """,
        [ts_code, trade_date],
    )


def _insert_minutes(
    store: DuckDBStore,
    *,
    ts_code: str,
    trade_date: date,
    source: str,
    times: tuple[time, ...],
) -> None:
    rows = [
        (
            ts_code,
            datetime.combine(trade_date, trade_time),
            "1min",
            10.0,
            10.1,
            9.9,
            10.0,
            100.0,
            1000.0,
            source,
        )
        for trade_time in times
    ]
    store._conn.executemany(  # noqa: SLF001
        """
        INSERT INTO minute_bar (
            ts_code, trade_time, freq, open, high, low, close, vol, amount, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _single_minute_spec(
    *,
    source: str = "feed",
    authoritative: bool = True,
    required_for_daily_coverage: bool = True,
    require_full_session: bool = True,
) -> MinuteSourceSessionSpec:
    return MinuteSourceSessionSpec(
        source=source,
        freq="1min",
        timestamp_semantics="bar_start",
        windows=(
            MinuteSessionWindow(
                start=time(9, 30), end=time(9, 30), step_minutes=1
            ),
        ),
        authoritative=authoritative,
        required_for_daily_coverage=required_for_daily_coverage,
        require_full_session=require_full_session,
    )


def _insert_minute_row(
    store: DuckDBStore,
    *,
    ts_code: str,
    trade_time: datetime,
    source: str,
    freq: str = "1min",
    high: float | None = 10.1,
    vol: float | None = 100.0,
    amount: float | None = 1000.0,
) -> None:
    store._conn.execute(  # noqa: SLF001
        """
        INSERT INTO minute_bar (
            ts_code, trade_time, freq, open, high, low, close, vol, amount, source
        ) VALUES (?, ?, ?, 10.0, ?, 9.9, 10.0, ?, ?, ?)
        """,
        [ts_code, trade_time, freq, high, vol, amount, source],
    )


def _run_consistency_audit(
    database_path: Path,
    *,
    start: date,
    end: date,
    specs: tuple[MinuteSourceSessionSpec, ...],
) -> AuditReport:
    rules = daily_minute_consistency_audit_rules(
        start,
        end,
        source_specs=specs,
    )
    with DuckDBStore(database_path, read_only=True) as store:
        return run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        )


def test_tushare_241_bar_end_grid_passes_full_session_audit(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "tushare-241.duckdb"
    trade_date = date(2026, 6, 26)
    tushare_spec = next(
        spec
        for spec in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if spec.source == "tushare" and spec.freq == "1min"
    )
    expected_times = tushare_spec.expected_times()

    assert tushare_spec.timestamp_semantics == "bar_end"
    assert len(expected_times) == 241
    assert expected_times[0] == time(9, 30)
    assert expected_times[-1] == time(15, 0)

    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source="tushare",
            times=expected_times,
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=DEFAULT_MINUTE_SOURCE_SESSION_SPECS,
    )

    assert report.findings == ()


def test_configured_240_bar_start_grid_passes_same_audit_engine(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "start-grid-240.duckdb"
    trade_date = date(2026, 6, 26)
    spec = MinuteSourceSessionSpec(
        source="start_feed",
        freq="1min",
        timestamp_semantics="bar_start",
        windows=(
            MinuteSessionWindow(
                start=time(9, 30), end=time(11, 29), step_minutes=1
            ),
            MinuteSessionWindow(
                start=time(13, 0), end=time(14, 59), step_minutes=1
            ),
        ),
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=True,
    )
    expected_times = spec.expected_times()

    assert len(expected_times) == 240

    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="000001.SZ", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="000001.SZ",
            trade_date=trade_date,
            source=spec.source,
            times=expected_times,
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    assert report.findings == ()


def test_wrong_timestamp_grid_reports_missing_and_extra_times(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "wrong-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    tushare_spec = next(
        spec
        for spec in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if spec.source == "tushare" and spec.freq == "1min"
    )
    start_grid = (
        *_minute_times(time(9, 30), time(11, 29)),
        *_minute_times(time(13, 0), time(14, 59)),
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source="tushare",
            times=start_grid,
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(tushare_spec,),
    )

    finding = next(
        item
        for item in report.findings
        if item.rule_id == "incomplete-authoritative-session"
    )
    assert finding.severity == "P1"
    assert finding.scope_key == "2026-06-26/2026-06-26/tushare/1min"
    assert finding.evidence == {
        "count": 1,
        "expected_count": 241,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "source": "tushare",
                "freq": "1min",
                "timestamp_semantics": "bar_end",
                "missing_count": 2,
                "missing_times": ["11:30", "15:00"],
                "extra_count": 1,
                "extra_times": ["13:00"],
            }
        ],
    }


def test_daily_minute_audit_configuration_fails_fast() -> None:
    authoritative = MinuteSourceSessionSpec(
        source="feed",
        freq="1min",
        timestamp_semantics="bar_start",
        windows=(
            MinuteSessionWindow(
                start=time(9, 30), end=time(9, 31), step_minutes=1
            ),
        ),
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=True,
    )
    non_authoritative = authoritative.model_copy(
        update={
            "source": "snapshot",
            "authoritative": False,
            "required_for_daily_coverage": False,
            "require_full_session": False,
        }
    )

    with pytest.raises(ValueError, match="start must not be after end"):
        daily_minute_consistency_audit_rules(
            date(2026, 6, 27),
            date(2026, 6, 26),
            source_specs=(authoritative,),
        )
    with pytest.raises(ValueError, match="sample_limit must be positive"):
        daily_minute_consistency_audit_rules(
            date(2026, 6, 26),
            date(2026, 6, 26),
            source_specs=(authoritative,),
            sample_limit=0,
        )
    with pytest.raises(ValueError, match="duplicate minute source/freq spec"):
        daily_minute_consistency_audit_rules(
            date(2026, 6, 26),
            date(2026, 6, 26),
            source_specs=(authoritative, authoritative),
        )
    with pytest.raises(ValueError, match="requires an authoritative spec"):
        daily_minute_consistency_audit_rules(
            date(2026, 6, 26),
            date(2026, 6, 26),
            source_specs=(non_authoritative,),
        )


@pytest.mark.parametrize(
    ("start", "end", "step_minutes", "message"),
    (
        (time(10, 0), time(9, 59), 1, "start must not be after end"),
        (time(9, 30), time(9, 31), 0, "greater than 0"),
        (time(9, 30), time(9, 33), 2, "align to step_minutes"),
    ),
)
def test_minute_session_window_rejects_invalid_grid(
    start: time,
    end: time,
    step_minutes: int,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        MinuteSessionWindow(
            start=start,
            end=end,
            step_minutes=step_minutes,
        )


def test_minute_session_window_requires_explicit_step() -> None:
    with pytest.raises(
        ValidationError,
        match=r"(?s)step_minutes.*Field required",
    ):
        MinuteSessionWindow.model_validate(
            {"start": time(9, 30), "end": time(9, 31)}
        )


def test_minute_source_spec_rejects_overlapping_windows() -> None:
    with pytest.raises(ValidationError, match="must not overlap"):
        MinuteSourceSessionSpec(
            source="feed",
            freq="1min",
            timestamp_semantics="bar_start",
            windows=(
                MinuteSessionWindow(
                    start=time(9, 30), end=time(9, 31), step_minutes=1
                ),
                MinuteSessionWindow(
                    start=time(9, 31), end=time(9, 32), step_minutes=1
                ),
            ),
            authoritative=True,
            required_for_daily_coverage=True,
            require_full_session=True,
        )


def test_minute_source_spec_rejects_out_of_order_windows() -> None:
    with pytest.raises(ValidationError, match="chronological order"):
        MinuteSourceSessionSpec(
            source="feed",
            freq="1min",
            timestamp_semantics="bar_start",
            windows=(
                MinuteSessionWindow(
                    start=time(13, 0), end=time(15, 0), step_minutes=1
                ),
                MinuteSessionWindow(
                    start=time(9, 30), end=time(11, 30), step_minutes=1
                ),
            ),
            authoritative=True,
            required_for_daily_coverage=True,
            require_full_session=True,
        )


def test_only_trading_daily_rows_require_authoritative_minutes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "eligibility.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO daily_bar (ts_code, trade_date, close, vol, amount)
            VALUES ('000001.SZ', ?, 10.0, 0.0, 0.0)
            """,
            [trade_date],
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    assert tuple(item.rule_id for item in report.findings) == (
        "eligible-daily-without-authoritative-minute",
    )
    assert report.findings[0].evidence == {
        "count": 1,
        "samples": [
            {"ts_code": "600000.SH", "trade_date": "2026-06-26"}
        ],
    }


def test_minute_without_daily_is_reported_separately(tmp_path: Path) -> None:
    database_path = tmp_path / "minute-without-daily.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source=spec.source,
            times=(time(9, 30),),
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item for item in report.findings if item.rule_id == "minute-without-daily"
    )
    assert finding.severity == "P1"
    assert finding.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
                "minute_count": 1,
            }
        ],
    }


def test_zero_volume_daily_and_no_rows_do_not_create_missing_findings(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "allowed-missing.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO daily_bar (ts_code, trade_date, close, vol, amount)
            VALUES ('000001.SZ', ?, 10.0, 0.0, 0.0)
            """,
            [trade_date],
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    assert report.findings == ()


def test_unknown_source_or_frequency_semantics_is_blocking(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "unknown-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 30)),
            source="mystery",
            freq="5min",
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item
        for item in report.findings
        if item.rule_id == "unknown-source-or-freq-semantics"
    )
    assert finding.severity == "P0"
    assert finding.is_blocking is True
    assert finding.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "source": "mystery",
                "freq": "5min",
                "minute_count": 1,
            }
        ],
    }


def test_cross_source_exact_and_conflicting_overlaps_are_separate_and_null_safe(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "cross-source.duckdb"
    trade_date = date(2026, 6, 26)
    primary = _single_minute_spec(require_full_session=False)
    mirror = _single_minute_spec(
        source="mirror",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source in (primary.source, mirror.source):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, time(9, 30)),
                source=source,
                vol=None,
                amount=None,
            )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 31)),
            source=primary.source,
            vol=None,
        )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 31)),
            source=mirror.source,
            vol=100.0,
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(primary, mirror),
    )

    exact = next(
        item
        for item in report.findings
        if item.rule_id == "cross-source-exact-overlap"
    )
    conflict = next(
        item
        for item in report.findings
        if item.rule_id == "cross-source-conflicting-overlap"
    )
    assert exact.severity == "P3"
    assert exact.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:30:00",
                "freq": "1min",
                "sources": ["feed", "mirror"],
                "distinct_payload_count": 1,
            }
        ],
    }
    assert conflict.severity == "P2"
    assert conflict.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:31:00",
                "freq": "1min",
                "sources": ["feed", "mirror"],
                "distinct_payload_count": 2,
            }
        ],
    }


def test_audit_uses_closed_date_range_for_all_rules(tmp_path: Path) -> None:
    database_path = tmp_path / "date-range.duckdb"
    in_range = date(2026, 6, 26)
    outside = date(2026, 6, 25)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=in_range)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=in_range,
            source=spec.source,
            times=spec.expected_times(),
        )
        _insert_eligible_daily(store, ts_code="000001.SZ", trade_date=outside)
        _insert_minute_row(
            store,
            ts_code="300001.SZ",
            trade_time=datetime.combine(outside, time(9, 30)),
            source="unknown-outside-range",
        )

    report = _run_consistency_audit(
        database_path,
        start=in_range,
        end=in_range,
        specs=(spec,),
    )

    assert report.findings == ()


def test_findings_are_stable_aggregates_with_bounded_deterministic_samples(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "stable-samples.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        for ts_code in ("600002.SH", "600000.SH", "600001.SH"):
            _insert_eligible_daily(store, ts_code=ts_code, trade_date=trade_date)

    rules = daily_minute_consistency_audit_rules(
        trade_date,
        trade_date,
        source_specs=(spec,),
        sample_limit=2,
    )
    with DuckDBStore(database_path, read_only=True) as store:
        first = run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        )
    with DuckDBStore(database_path, read_only=True) as store:
        second = run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 15, tzinfo=UTC),
        )

    first_finding = next(
        item
        for item in first.findings
        if item.rule_id == "eligible-daily-without-authoritative-minute"
    )
    second_finding = next(
        item
        for item in second.findings
        if item.rule_id == "eligible-daily-without-authoritative-minute"
    )
    assert first_finding.issue_id == second_finding.issue_id
    assert first_finding.scope_key == "2026-06-26/2026-06-26"
    assert first_finding.evidence == second_finding.evidence
    assert first_finding.evidence == {
        "count": 3,
        "samples": [
            {"ts_code": "600000.SH", "trade_date": "2026-06-26"},
            {"ts_code": "600001.SH", "trade_date": "2026-06-26"},
        ],
    }


def test_consistency_audit_is_readonly_and_does_not_record_issues(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "readonly.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    assert report.finding_count == 1
    with DuckDBStore(database_path) as store:
        issue_count = store._conn.execute(  # noqa: SLF001
            "SELECT count(*) FROM data_quality_issue"
        ).fetchone()[0]
    assert issue_count == 0
