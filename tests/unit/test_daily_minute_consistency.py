from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal, cast

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.data_quality import (
    DEFAULT_MINUTE_SOURCE_SESSION_SPECS,
    AuditReport,
    MinuteSessionWindow,
    MinuteSourceSessionSpec,
    UnknownMinuteSemanticsSample,
    daily_minute_consistency_audit_rules,
    run_audit,
)
from rquant.storage.duckdb import DuckDBStore


class _CountingConnection:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection
        self.overlap_aggregate_queries = 0
        self.session_aggregate_queries = 0
        self.session_sample_queries = 0
        self.executed_queries: list[str] = []

    def execute(
        self,
        query: str,
        parameters: object | None = None,
    ) -> duckdb.DuckDBPyConnection:
        self.executed_queries.append(query)
        if "distinct_payload_count" in query and "struct_pack" in query:
            self.overlap_aggregate_queries += 1
        if "session_shape AS" in query and "AS actual_count" in query:
            self.session_aggregate_queries += 1
        if "sampled_sessions" in query and "AS actual_time" in query:
            self.session_sample_queries += 1
        if parameters is None:
            return self._connection.execute(query)
        return self._connection.execute(query, parameters)

    def close(self) -> None:
        self._connection.close()


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
    timestamp_semantics: Literal["bar_start", "bar_end", "provider_snapshot"] = ("bar_start"),
    authoritative: bool = True,
    required_for_daily_coverage: bool = True,
    require_full_session: bool = True,
) -> MinuteSourceSessionSpec:
    return MinuteSourceSessionSpec(
        source=source,
        freq="1min",
        timestamp_semantics=timestamp_semantics,
        windows=(MinuteSessionWindow(start=time(9, 30), end=time(9, 30), step_minutes=1),),
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
            MinuteSessionWindow(start=time(9, 30), end=time(11, 29), step_minutes=1),
            MinuteSessionWindow(start=time(13, 0), end=time(14, 59), step_minutes=1),
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
        item for item in report.findings if item.rule_id == "incomplete-authoritative-session"
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
        windows=(MinuteSessionWindow(start=time(9, 30), end=time(9, 31), step_minutes=1),),
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
        MinuteSessionWindow.model_validate({"start": time(9, 30), "end": time(9, 31)})


def test_minute_source_spec_rejects_overlapping_windows() -> None:
    with pytest.raises(ValidationError, match="must not overlap"):
        MinuteSourceSessionSpec(
            source="feed",
            freq="1min",
            timestamp_semantics="bar_start",
            windows=(
                MinuteSessionWindow(start=time(9, 30), end=time(9, 31), step_minutes=1),
                MinuteSessionWindow(start=time(9, 31), end=time(9, 32), step_minutes=1),
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
                MinuteSessionWindow(start=time(13, 0), end=time(15, 0), step_minutes=1),
                MinuteSessionWindow(start=time(9, 30), end=time(11, 30), step_minutes=1),
            ),
            authoritative=True,
            required_for_daily_coverage=True,
            require_full_session=True,
        )


def test_minute_source_spec_rejects_staggered_step_interval_overlap() -> None:
    with pytest.raises(ValidationError, match="must not overlap"):
        MinuteSourceSessionSpec(
            source="feed",
            freq="1min",
            timestamp_semantics="bar_start",
            windows=(
                MinuteSessionWindow(start=time(9, 30), end=time(9, 34), step_minutes=4),
                MinuteSessionWindow(start=time(9, 31), end=time(9, 33), step_minutes=2),
            ),
            authoritative=True,
            required_for_daily_coverage=True,
            require_full_session=True,
        )


def test_full_session_requirement_requires_authoritative_source() -> None:
    with pytest.raises(
        ValidationError,
        match="require_full_session requires an authoritative source",
    ):
        _single_minute_spec(
            source="snapshot",
            authoritative=False,
            required_for_daily_coverage=False,
            require_full_session=True,
        )


def test_all_daily_rows_require_minutes_without_authoritative_suspension(
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
        "count": 2,
        "samples": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
            },
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
            }
        ],
    }


def test_required_authoritative_sources_use_all_of_coverage_semantics(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "all-of-coverage.duckdb"
    trade_date = date(2026, 6, 26)
    primary = _single_minute_spec(require_full_session=False)
    secondary = _single_minute_spec(
        source="secondary",
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source=primary.source,
            times=(time(9, 30),),
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(primary, secondary),
    )

    findings = tuple(
        item
        for item in report.findings
        if item.rule_id == "eligible-daily-without-authoritative-minute"
    )
    assert len(findings) == 1
    assert findings[0].scope_key == ("2026-06-26/2026-06-26/secondary/1min")
    assert findings[0].evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "source": "secondary",
                "freq": "1min",
            }
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

    finding = next(item for item in report.findings if item.rule_id == "minute-without-daily")
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


def test_zero_volume_daily_needs_authoritative_suspension_fact(
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

    assert report.findings[0].rule_id == (
        "eligible-daily-without-authoritative-minute"
    )
    assert report.findings[0].evidence["count"] == 1


def test_zero_volume_daily_row_conflicts_with_full_day_suspension(
    tmp_path: Path,
) -> None:
    from rquant.suspension import (
        normalize_suspend_d_snapshot,
        persist_suspension_snapshot,
    )

    database_path = tmp_path / "suspended.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    snapshot = normalize_suspend_d_snapshot(
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260626",
                    "suspend_timing": "全天",
                    "suspend_type": "S",
                }
            ]
        ),
        trade_date=trade_date,
        queried_at=datetime(2026, 6, 27, tzinfo=UTC),
    )
    with DuckDBStore(database_path) as store:
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO daily_bar (ts_code, trade_date, close, vol, amount)
            VALUES ('000001.SZ', ?, 10.0, 0.0, 0.0)
            """,
            [trade_date],
        )
        persist_suspension_snapshot(store, snapshot)

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item
        for item in report.findings
        if item.rule_id == "eligible-daily-without-authoritative-minute"
    )
    assert finding.evidence["count"] == 1


def test_positive_daily_evidence_blocks_full_day_suspension_exemption(
    tmp_path: Path,
) -> None:
    from rquant.suspension import (
        normalize_suspend_d_snapshot,
        persist_suspension_snapshot,
    )

    database_path = tmp_path / "suspended-with-trading.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    snapshot = normalize_suspend_d_snapshot(
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260626",
                    "suspend_timing": None,
                    "suspend_type": "S",
                }
            ]
        ),
        trade_date=trade_date,
        queried_at=datetime(2026, 6, 27, tzinfo=UTC),
    )
    with DuckDBStore(database_path) as store:
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO daily_bar (ts_code, trade_date, close, vol, amount)
            VALUES ('000001.SZ', ?, 10.0, 100.0, 1000.0)
            """,
            [trade_date],
        )
        persist_suspension_snapshot(store, snapshot)

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item
        for item in report.findings
        if item.rule_id == "eligible-daily-without-authoritative-minute"
    )
    assert finding.evidence["count"] == 1


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
        item for item in report.findings if item.rule_id == "unknown-source-or-freq-semantics"
    )
    assert finding.severity == "P0"
    assert finding.is_blocking is True
    assert finding.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "raw_source": "mystery",
                "raw_freq": "5min",
                "minute_count": 1,
            }
        ],
    }


def test_unknown_semantics_preserves_blank_source_and_frequency(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "blank-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 30)),
            source="",
            freq="",
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item for item in report.findings if item.rule_id == "unknown-source-or-freq-semantics"
    )
    assert finding.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "raw_source": "",
                "raw_freq": "",
                "minute_count": 1,
            }
        ],
    }


def test_unknown_semantics_sample_preserves_legacy_null_values() -> None:
    sample = UnknownMinuteSemanticsSample(
        ts_code="600000.SH",
        trade_date=date(2026, 6, 26),
        raw_source=None,
        raw_freq=None,
        minute_count=1,
    )

    assert sample.model_dump(mode="json") == {
        "ts_code": "600000.SH",
        "trade_date": "2026-06-26",
        "raw_source": None,
        "raw_freq": None,
        "minute_count": 1,
    }


@pytest.mark.parametrize(
    ("source", "freq"),
    (
        (None, "1min"),
        ("feed", None),
    ),
)
def test_current_minute_bar_schema_rejects_null_source_or_frequency(
    tmp_path: Path,
    source: str | None,
    freq: str | None,
) -> None:
    database_path = tmp_path / "null-semantics.duckdb"
    with (
        DuckDBStore(database_path) as store,
        pytest.raises(duckdb.ConstraintException),
    ):
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO minute_bar (
                ts_code,
                trade_time,
                freq,
                open,
                high,
                low,
                close,
                source
            ) VALUES ('600000.SH', '2026-06-26 09:30:00', ?, 10, 10, 10, 10, ?)
            """,
            [freq, source],
        )


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

    exact = next(item for item in report.findings if item.rule_id == "cross-source-exact-overlap")
    conflict = next(
        item for item in report.findings if item.rule_id == "cross-source-conflicting-overlap"
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


def test_overlap_normalizes_mixed_timestamp_semantics_to_same_logical_bar(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "mixed-overlap-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    start_feed = _single_minute_spec(require_full_session=False)
    end_feed = _single_minute_spec(
        source="end_feed",
        timestamp_semantics="bar_end",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    snapshot_feed = _single_minute_spec(
        source="snapshot_feed",
        timestamp_semantics="provider_snapshot",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source, trade_time in (
            (start_feed.source, time(9, 30)),
            (end_feed.source, time(9, 31)),
            (snapshot_feed.source, time(9, 30)),
        ):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, trade_time),
                source=source,
            )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(start_feed, end_feed, snapshot_feed),
    )

    exact = next(item for item in report.findings if item.rule_id == "cross-source-exact-overlap")
    assert exact.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:30:00",
                "freq": "1min",
                "sources": ["end_feed", "feed", "snapshot_feed"],
                "distinct_payload_count": 1,
            }
        ],
    }
    assert all(item.rule_id != "cross-source-conflicting-overlap" for item in report.findings)


def test_overlap_normalization_keeps_cross_midnight_logical_bar_together(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "cross-midnight-overlap.duckdb"
    first_date = date(2026, 6, 25)
    second_date = date(2026, 6, 26)
    start_feed = _single_minute_spec(require_full_session=False)
    end_feed = _single_minute_spec(
        source="end_feed",
        timestamp_semantics="bar_end",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(first_date, time(23, 59)),
            source=start_feed.source,
        )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(second_date, time(0, 0)),
            source=end_feed.source,
        )

    report = _run_consistency_audit(
        database_path,
        start=first_date,
        end=second_date,
        specs=(start_feed, end_feed),
    )

    exact = next(item for item in report.findings if item.rule_id == "cross-source-exact-overlap")
    assert exact.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-25T23:59:00",
                "freq": "1min",
                "sources": ["end_feed", "feed"],
                "distinct_payload_count": 1,
            }
        ],
    }


def test_overlap_normalization_does_not_merge_adjacent_logical_bars(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "adjacent-overlap-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    start_feed = _single_minute_spec(require_full_session=False)
    end_feed = _single_minute_spec(
        source="end_feed",
        timestamp_semantics="bar_end",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source in (start_feed.source, end_feed.source):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, time(9, 31)),
                source=source,
            )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(start_feed, end_feed),
    )

    assert all(not item.rule_id.startswith("cross-source-") for item in report.findings)


def test_overlap_normalization_detects_shifted_conflict_without_exact_false_positive(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shifted-conflict-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    start_feed = _single_minute_spec(require_full_session=False)
    end_feed = _single_minute_spec(
        source="end_feed",
        timestamp_semantics="bar_end",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    snapshot_feed = _single_minute_spec(
        source="snapshot_feed",
        timestamp_semantics="provider_snapshot",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source in (start_feed.source, snapshot_feed.source):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, time(9, 30)),
                source=source,
            )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 31)),
            source=end_feed.source,
            high=10.2,
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(start_feed, end_feed, snapshot_feed),
    )

    conflict = next(
        item for item in report.findings if item.rule_id == "cross-source-conflicting-overlap"
    )
    assert conflict.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:30:00",
                "freq": "1min",
                "sources": ["end_feed", "feed", "snapshot_feed"],
                "distinct_payload_count": 2,
            }
        ],
    }
    assert all(item.rule_id != "cross-source-exact-overlap" for item in report.findings)


def test_unknown_semantics_fails_closed_without_joining_known_overlap(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "unknown-mixed-overlap-semantics.duckdb"
    trade_date = date(2026, 6, 26)
    start_feed = _single_minute_spec(require_full_session=False)
    end_feed = _single_minute_spec(
        source="end_feed",
        timestamp_semantics="bar_end",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source, trade_time in (
            (start_feed.source, time(9, 30)),
            (end_feed.source, time(9, 31)),
            ("mystery", time(9, 30)),
        ):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, trade_time),
                source=source,
            )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(start_feed, end_feed),
    )

    unknown = next(
        item for item in report.findings if item.rule_id == "unknown-source-or-freq-semantics"
    )
    exact = next(item for item in report.findings if item.rule_id == "cross-source-exact-overlap")
    assert report.is_blocked is True
    assert unknown.severity == "P0"
    assert exact.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:30:00",
                "freq": "1min",
                "sources": ["end_feed", "feed"],
                "distinct_payload_count": 1,
            }
        ],
    }


def test_overlap_and_full_session_rules_share_one_aggregate_query_per_store(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shared-aggregate.duckdb"
    trade_date = date(2026, 6, 26)
    primary = _single_minute_spec()
    secondary = _single_minute_spec(
        source="secondary",
        authoritative=True,
        required_for_daily_coverage=False,
        require_full_session=True,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source in (primary.source, secondary.source):
            _insert_minutes(
                store,
                ts_code="600000.SH",
                trade_date=trade_date,
                source=source,
                times=(time(9, 30),),
            )

    rules = daily_minute_consistency_audit_rules(
        trade_date,
        trade_date,
        source_specs=(primary, secondary),
    )
    with DuckDBStore(database_path, read_only=True) as store:
        counter = _CountingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(duckdb.DuckDBPyConnection, counter)  # noqa: SLF001
        run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        )

    assert counter.overlap_aggregate_queries == 1
    assert counter.session_aggregate_queries == 1
    assert counter.session_sample_queries == 0
    session_query = next(query for query in counter.executed_queries if "session_shape AS" in query)
    assert "list(" not in session_query
    assert "DISTINCT strftime" not in session_query
    assert "actual_count <> expected_count" in session_query
    assert "extra_count > 0" in session_query
    assert "WHERE sample_rank <= ?" in session_query


def test_overlap_audit_scans_each_populated_trade_date_independently(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "partitioned-overlap.duckdb"
    first_date = date(2026, 6, 25)
    second_date = date(2026, 6, 26)
    primary = _single_minute_spec(require_full_session=False)
    secondary = _single_minute_spec(
        source="secondary",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    with DuckDBStore(database_path) as store:
        for trade_date in (first_date, second_date):
            _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
            for source in (primary.source, secondary.source):
                _insert_minutes(
                    store,
                    ts_code="600000.SH",
                    trade_date=trade_date,
                    source=source,
                    times=(time(9, 30),),
                )

    rules = daily_minute_consistency_audit_rules(
        first_date,
        second_date,
        source_specs=(primary, secondary),
        sample_limit=1,
    )
    with DuckDBStore(database_path, read_only=True) as store:
        counter = _CountingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(duckdb.DuckDBPyConnection, counter)  # noqa: SLF001
        report = run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        )

    exact = next(
        item for item in report.findings if item.rule_id == "cross-source-exact-overlap"
    )
    assert counter.overlap_aggregate_queries == 2
    assert exact.evidence["count"] == 2
    assert [sample["trade_time"] for sample in exact.evidence["samples"]] == [
        "2026-06-25T09:30:00",
    ]


def test_full_session_query_returns_bounded_mismatches_and_total_count(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "bounded-session-mismatches.duckdb"
    trade_date = date(2026, 6, 26)
    spec = MinuteSourceSessionSpec(
        source="feed",
        freq="1min",
        timestamp_semantics="bar_start",
        windows=(MinuteSessionWindow(start=time(9, 30), end=time(9, 31), step_minutes=1),),
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=True,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source=spec.source,
            times=spec.expected_times(),
        )
        for ts_code in ("600004.SH", "600002.SH", "600001.SH", "600003.SH"):
            _insert_eligible_daily(store, ts_code=ts_code, trade_date=trade_date)
            _insert_minutes(
                store,
                ts_code=ts_code,
                trade_date=trade_date,
                source=spec.source,
                times=(time(9, 30),),
            )

    rules = daily_minute_consistency_audit_rules(
        trade_date,
        trade_date,
        source_specs=(spec,),
        sample_limit=2,
    )
    with DuckDBStore(database_path, read_only=True) as store:
        counter = _CountingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(duckdb.DuckDBPyConnection, counter)  # noqa: SLF001
        report = run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        )

    finding = next(
        item for item in report.findings if item.rule_id == "incomplete-authoritative-session"
    )
    assert counter.session_aggregate_queries == 1
    assert counter.session_sample_queries == 1
    assert finding.evidence == {
        "count": 4,
        "expected_count": 2,
        "samples": [
            {
                "ts_code": "600001.SH",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
                "timestamp_semantics": "bar_start",
                "missing_count": 1,
                "missing_times": ["09:31"],
                "extra_count": 0,
                "extra_times": [],
            },
            {
                "ts_code": "600002.SH",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
                "timestamp_semantics": "bar_start",
                "missing_count": 1,
                "missing_times": ["09:31"],
                "extra_count": 0,
                "extra_times": [],
            },
        ],
    }


def test_full_session_detects_missing_and_extra_times_when_count_is_unchanged(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "equal-count-session-mismatch.duckdb"
    trade_date = date(2026, 6, 26)
    spec = MinuteSourceSessionSpec(
        source="feed",
        freq="1min",
        timestamp_semantics="bar_start",
        windows=(MinuteSessionWindow(start=time(9, 30), end=time(9, 31), step_minutes=1),),
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=True,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source=spec.source,
            times=(time(9, 30), time(9, 32)),
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item for item in report.findings if item.rule_id == "incomplete-authoritative-session"
    )
    assert finding.evidence["count"] == 1
    assert finding.evidence["samples"][0]["missing_times"] == ["09:31"]
    assert finding.evidence["samples"][0]["extra_times"] == ["09:32"]


def test_full_session_preserves_subsecond_duplicate_as_extra_evidence(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "subsecond-session-duplicate.duckdb"
    trade_date = date(2026, 6, 26)
    spec = MinuteSourceSessionSpec(
        source="feed",
        freq="1min",
        timestamp_semantics="bar_start",
        windows=(MinuteSessionWindow(start=time(9, 30), end=time(9, 31), step_minutes=1),),
        authoritative=True,
        required_for_daily_coverage=True,
        require_full_session=True,
    )
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 30)),
            source=spec.source,
        )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 30, 0, 500_000)),
            source=spec.source,
        )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(spec,),
    )

    finding = next(
        item for item in report.findings if item.rule_id == "incomplete-authoritative-session"
    )
    assert finding.evidence["samples"][0]["missing_times"] == ["09:31"]
    assert finding.evidence["samples"][0]["extra_times"] == ["09:30:00.500000"]


def test_all_minute_range_scans_use_raw_half_open_timestamps(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "raw-range-filter.duckdb"
    trade_date = date(2026, 6, 26)
    spec = _single_minute_spec()
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        _insert_minutes(
            store,
            ts_code="600000.SH",
            trade_date=trade_date,
            source=spec.source,
            times=spec.expected_times(),
        )

    rules = daily_minute_consistency_audit_rules(
        trade_date,
        trade_date,
        source_specs=(spec,),
    )
    with DuckDBStore(database_path, read_only=True) as store:
        counter = _CountingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(duckdb.DuckDBPyConnection, counter)  # noqa: SLF001
        run_audit(
            store,
            rules,
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        )

    for cte_name in ("missing_daily", "unknown_rows"):
        query = next(query for query in counter.executed_queries if cte_name in query)
        assert "m.trade_time >= ?" in query
        assert "m.trade_time < ?" in query
        assert "CAST(m.trade_time AS DATE) BETWEEN" not in query


def test_overlap_groups_handle_nan_null_and_three_source_payloads(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "overlap-nan.duckdb"
    trade_date = date(2026, 6, 26)
    primary = _single_minute_spec(require_full_session=False)
    secondary = _single_minute_spec(
        source="secondary",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    tertiary = _single_minute_spec(
        source="tertiary",
        authoritative=False,
        required_for_daily_coverage=False,
        require_full_session=False,
    )
    nan = float("nan")
    with DuckDBStore(database_path) as store:
        _insert_eligible_daily(store, ts_code="600000.SH", trade_date=trade_date)
        for source in (primary.source, secondary.source):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, time(9, 30)),
                source=source,
                high=nan,
            )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 31)),
            source=primary.source,
            high=nan,
        )
        _insert_minute_row(
            store,
            ts_code="600000.SH",
            trade_time=datetime.combine(trade_date, time(9, 31)),
            source=secondary.source,
            high=None,
        )
        for source, high in (
            (primary.source, 10.1),
            (secondary.source, 10.1),
            (tertiary.source, 10.2),
        ):
            _insert_minute_row(
                store,
                ts_code="600000.SH",
                trade_time=datetime.combine(trade_date, time(9, 32)),
                source=source,
                high=high,
            )

    report = _run_consistency_audit(
        database_path,
        start=trade_date,
        end=trade_date,
        specs=(primary, secondary, tertiary),
    )

    exact = next(item for item in report.findings if item.rule_id == "cross-source-exact-overlap")
    conflict = next(
        item for item in report.findings if item.rule_id == "cross-source-conflicting-overlap"
    )
    assert exact.evidence == {
        "count": 1,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:30:00",
                "freq": "1min",
                "sources": ["feed", "secondary"],
                "distinct_payload_count": 1,
            }
        ],
    }
    assert conflict.evidence == {
        "count": 2,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:31:00",
                "freq": "1min",
                "sources": ["feed", "secondary"],
                "distinct_payload_count": 2,
            },
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-06-26T09:32:00",
                "freq": "1min",
                "sources": ["feed", "secondary", "tertiary"],
                "distinct_payload_count": 2,
            },
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
    assert first_finding.scope_key == "2026-06-26/2026-06-26/feed/1min"
    assert first_finding.evidence == second_finding.evidence
    assert first_finding.evidence == {
        "count": 3,
        "samples": [
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
            },
            {
                "ts_code": "600001.SH",
                "trade_date": "2026-06-26",
                "source": "feed",
                "freq": "1min",
            },
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
