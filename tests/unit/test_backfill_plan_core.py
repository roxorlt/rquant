"""A proposed daily-bar repair plan is based on one offline, read-only snapshot."""

import json
import socket
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backfill_plan_core import (
    BackfillEstimateAssumptions,
    DailyBarBackfillPlan,
    build_daily_bar_backfill_plan,
)
from rquant.storage.schema import DAILY_BAR_DDL, TRADE_CALENDAR_DDL


def _database(
    path: Path,
    start: date,
    end: date,
    bars: list[tuple[str, date]],
) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.executemany(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, ?, 'synthetic', ?)",
            [
                (day, day.weekday() < 5, datetime(2026, 2, 6, tzinfo=UTC))
                for offset in range((end - start).days + 1)
                for day in [start + timedelta(days=offset)]
            ],
        )
        if bars:
            connection.executemany(
                "INSERT INTO daily_bar (ts_code, trade_date) VALUES (?, ?)", bars
            )
    return path


def _assumptions() -> BackfillEstimateAssumptions:
    return BackfillEstimateAssumptions(
        status_namechange_start=date(2026, 1, 1),
        status_source_as_of=date(2026, 2, 5),
        status_window_years=3,
        adapter_seconds_per_operation=Decimal("1"),
        market_throttle_seconds_per_operation=Decimal("0.5"),
        status_throttle_seconds_per_operation=Decimal("0.25"),
        retry_allowance_seconds_per_operation=Decimal("0.1"),
    )


def _plan(
    connection: duckdb.DuckDBPyConnection,
    *,
    start: date = date(2026, 1, 29),
    end: date = date(2026, 2, 5),
    snapshot_file_sha256: str = "a" * 64,
    evidence_code_revision: str = "synthetic-revision-1",
    observed_at: datetime = datetime(2026, 2, 6, 1, tzinfo=UTC),
    assumptions: BackfillEstimateAssumptions | None = None,
) -> DailyBarBackfillPlan:
    return build_daily_bar_backfill_plan(
        connection,
        snapshot_label="snapshot-1",
        snapshot_file_sha256=snapshot_file_sha256,
        evidence_code_revision=evidence_code_revision,
        audit_start=start,
        completed_through=end,
        observed_at=observed_at,
        assumptions=assumptions or _assumptions(),
    )


def _rehash(payload: dict[str, object]) -> dict[str, object]:
    body = {key: value for key, value in payload.items() if key != "content_sha256"}
    content = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {**body, "content_sha256": sha256(content.encode("utf-8")).hexdigest()}


def test_plan_uses_only_missing_open_days_and_one_exact_batch(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(
        tmp_path / "replica.duckdb",
        start,
        end,
        [
            ("600000.SH", start),
            ("600000.SH", date(2026, 2, 3)),
            ("600000.SH", end),
        ],
    )

    with duckdb.connect(str(path), read_only=True) as connection:
        plan = build_daily_bar_backfill_plan(
            connection,
            snapshot_label="synthetic-fixed-replica",
            snapshot_file_sha256="a" * 64,
            evidence_code_revision="synthetic-revision-1",
            audit_start=start,
            completed_through=end,
            observed_at=datetime(2026, 2, 6, 1, tzinfo=UTC),
            assumptions=_assumptions(),
        )

    assert plan.missing_dates == (
        date(2026, 1, 30),
        date(2026, 2, 2),
        date(2026, 2, 4),
    )
    assert [(item.start, item.end, item.missing_open_days) for item in plan.gaps] == [
        (date(2026, 1, 30), date(2026, 2, 2), 2),
        (date(2026, 2, 4), date(2026, 2, 4), 1),
    ]
    assert [(item.month, item.missing_open_days) for item in plan.monthly] == [
        (date(2026, 1, 1), 1),
        (date(2026, 2, 1), 2),
    ]
    assert plan.estimate.logical_operations.daily == 3
    assert plan.estimate.logical_operations.daily_basic == 3
    assert plan.estimate.logical_operations.adj_factor == 3
    assert plan.estimate.logical_operations.namechange_windows == 1
    assert plan.estimate.logical_operations.stock_st_upper_bound == 3
    assert plan.estimate.logical_operations.trade_cal == 0
    assert plan.estimate.logical_operations.total == 13
    assert plan.estimate.estimated_seconds == Decimal("19.8")
    assert plan.source.mode == "production_unverified"
    assert plan.source.identity_verified is False
    assert plan.source.collection_complete_verified is False
    assert plan.executable is False
    assert plan.estimate.quota_status == "unverified"
    assert len(plan.content_sha256) == 64


def test_plan_keeps_verifiable_calendar_evidence_and_rejects_changed_dates(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(tmp_path / "replica.duckdb", start, end, [])
    with duckdb.connect(str(path), read_only=True) as connection:
        plan = build_daily_bar_backfill_plan(
            connection,
            snapshot_label="snapshot-1",
            snapshot_file_sha256="a" * 64,
            evidence_code_revision="synthetic-revision-1",
            audit_start=start,
            completed_through=end,
            observed_at=datetime(2026, 2, 6, 1, tzinfo=UTC),
            assumptions=_assumptions(),
        )

    assert [item.day for item in plan.evidence.calendar.days] == [
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    ]
    payload = plan.model_dump(mode="json")
    payload["missing_dates"][0] = "2026-01-31"  # a closed Saturday
    with pytest.raises(ValidationError, match="missing_dates|calendar"):
        type(plan).model_validate(payload)


def test_no_whole_day_gap_means_zero_tasks_and_no_quota_claim(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    open_days = [
        start + timedelta(days=offset)
        for offset in range((end - start).days + 1)
        if (start + timedelta(days=offset)).weekday() < 5
    ]
    path = _database(
        tmp_path / "covered.duckdb",
        start,
        end,
        [("600000.SH", day) for day in open_days],
    )
    before = sha256(path.read_bytes()).hexdigest()

    with duckdb.connect(str(path), read_only=True) as connection:
        plan = _plan(connection)

    assert plan.missing_dates == ()
    assert plan.gaps == ()
    assert plan.estimate.logical_operations.total == 0
    assert plan.estimate.logical_operations.namechange_context_batches == 0
    assert plan.estimate.estimated_seconds == 0
    assert plan.estimate.quota_status == "unverified"
    assert plan.source.collection_complete_verified is False
    assert sha256(path.read_bytes()).hexdigest() == before


def test_weekday_sse_holiday_is_not_a_backfill_task(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 2)
    path = _database(tmp_path / "holiday.duckdb", start, end, [])
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE trade_calendar SET is_open = FALSE WHERE exchange = 'SSE' AND cal_date = ?",
            [date(2026, 1, 30)],
        )

    with duckdb.connect(str(path), read_only=True) as connection:
        plan = _plan(connection, start=start, end=end)

    assert plan.missing_dates == (date(2026, 1, 29), date(2026, 2, 2))


def test_hash_tracks_source_and_estimate_assumptions(tmp_path: Path) -> None:
    path = _database(
        tmp_path / "stable.duckdb",
        date(2026, 1, 29),
        date(2026, 2, 5),
        [],
    )
    with duckdb.connect(str(path), read_only=True) as connection:
        first = _plan(connection)
        repeated = _plan(connection)
        new_source = _plan(connection, snapshot_file_sha256="b" * 64)
        changed_assumptions = _plan(
            connection,
            assumptions=_assumptions().model_copy(
                update={"retry_allowance_seconds_per_operation": Decimal("1")}
            ),
        )

    assert first.content_sha256 == repeated.content_sha256
    assert first.content_sha256 != new_source.content_sha256
    assert first.content_sha256 != changed_assumptions.content_sha256
    with duckdb.connect(str(path), read_only=True) as connection:
        changed_code = _plan(connection, evidence_code_revision="synthetic-revision-2")
    assert first.content_sha256 != changed_code.content_sha256
    with pytest.raises(ValidationError, match="hash"):
        type(first).model_validate({**first.model_dump(mode="json"), "content_sha256": "0" * 64})


def test_current_unclosed_cutoff_and_writable_connection_are_rejected(tmp_path: Path) -> None:
    path = _database(tmp_path / "cutoff.duckdb", date(2026, 1, 29), date(2026, 2, 5), [])
    with (
        duckdb.connect(str(path), read_only=True) as connection,
        pytest.raises(ValueError, match="market close"),
    ):
        _plan(connection, observed_at=datetime(2026, 2, 5, 6, 59, tzinfo=UTC))
    with duckdb.connect(str(path)) as connection, pytest.raises(ValueError, match="read-only"):
        _plan(connection)


@pytest.mark.parametrize("calendar_problem", ["missing", "duplicate"])
def test_incomplete_or_duplicate_sse_day_refuses_plan(
    tmp_path: Path, calendar_problem: str
) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(tmp_path / "calendar.duckdb", start, end, [])
    with duckdb.connect(str(path)) as connection:
        if calendar_problem == "missing":
            connection.execute(
                "DELETE FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ?",
                [date(2026, 2, 1)],
            )
        else:
            connection.execute("CREATE TABLE no_key AS SELECT * FROM trade_calendar")
            connection.execute("DROP TABLE trade_calendar")
            connection.execute("ALTER TABLE no_key RENAME TO trade_calendar")
            connection.execute(
                "INSERT INTO trade_calendar SELECT * FROM trade_calendar WHERE cal_date = ?",
                [date(2026, 1, 29)],
            )

    with (
        duckdb.connect(str(path), read_only=True) as connection,
        pytest.raises(ValueError, match="calendar"),
    ):
        _plan(connection)


def test_range_over_3660_natural_days_is_rejected_before_data_read(tmp_path: Path) -> None:
    path = _database(tmp_path / "bounded.duckdb", date(2026, 1, 29), date(2026, 2, 5), [])
    with (
        duckdb.connect(str(path), read_only=True) as connection,
        pytest.raises(ValueError, match="range|limit"),
    ):
        _plan(connection, start=date(2010, 1, 1))


def test_plan_uses_only_given_connection_without_reopening_or_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _database(tmp_path / "isolated.duckdb", date(2026, 1, 29), date(2026, 2, 5), [])

    def forbid(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("planning must not open storage or a network connection")

    with duckdb.connect(str(path), read_only=True) as connection:
        monkeypatch.setattr(duckdb, "connect", forbid)
        monkeypatch.setattr(socket.socket, "connect", forbid)
        plan = _plan(connection)

    assert len(plan.missing_dates) == 6


@pytest.mark.parametrize("change", ["unclosed_cutoff", "incorrect_estimate"])
def test_plan_reload_rejects_inconsistent_cutoff_or_estimate(tmp_path: Path, change: str) -> None:
    path = _database(tmp_path / "validated.duckdb", date(2026, 1, 29), date(2026, 2, 5), [])
    with duckdb.connect(str(path), read_only=True) as connection:
        plan = _plan(connection)
    payload = plan.model_dump(mode="json")
    if change == "unclosed_cutoff":
        payload["cutoff_observed_at_utc"] = "2026-02-05T06:59:00Z"
    else:
        payload["estimate"]["estimated_seconds"] = "1"
    with pytest.raises(ValidationError, match="cutoff|estimate|market close"):
        type(plan).model_validate(_rehash(payload))
