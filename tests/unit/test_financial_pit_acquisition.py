"""Offline acquisition and immutable observation behavior for seven Tushare APIs."""

from __future__ import annotations

import json
import multiprocessing
import os
import sqlite3
import stat
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.financial_pit import (
    FinancialFact,
    SSECalendar,
    SSECalendarDay,
    select_financial_fact,
)
from rquant.financial_pit_acquisition import (
    AcquisitionLimits,
    FinancialArchive,
    FinancialObservedVersion,
    FinancialQuery,
    acquire_financial_batches,
    observed_versions,
)


class FakeTushare:
    def __init__(self, response: pd.DataFrame | Exception) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __getattr__(self, name: str) -> Any:
        def invoke(**kwargs: str) -> pd.DataFrame:
            self.calls.append((name, kwargs))
            if isinstance(self.response, Exception):
                raise self.response
            return self.response

        return invoke


def _query(api: str = "income", **changes: object) -> FinancialQuery:
    fields: dict[str, object] = {
        "request_id": uuid4(),
        "api": api,
        "ts_code": "600000.SH",
    }
    if api == "fina_indicator":
        fields["period"] = date(2025, 12, 31)
    elif api == "dividend":
        fields["ann_date"] = date(2026, 4, 20)
    else:
        fields["start_date"] = date(2026, 4, 1)
        fields["end_date"] = date(2026, 5, 1)
    fields.update(changes)
    return FinancialQuery(**fields)


def _row(api: str = "income", **changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ts_code": "600000.SH",
        "ann_date": "20260420",
        "end_date": "20251231",
        "value": 100.0,
    }
    if api in {"income", "balancesheet", "cashflow"}:
        row.update(f_ann_date="20260421", report_type="1")
    elif api == "forecast":
        row.update(type="预增", first_ann_date="20260418")
    elif api == "dividend":
        row.update(div_proc="预案", pay_date="20260601")
    row.update(changes)
    return row


def _clock(at: datetime) -> Callable[[], datetime]:
    return lambda: at


def _acquire(
    root: Path,
    query: FinancialQuery,
    response: pd.DataFrame | Exception,
    *,
    at: datetime = datetime(2026, 9, 28, 12, tzinfo=UTC),
    limits: AcquisitionLimits | None = None,
) -> tuple[FakeTushare, FinancialArchive]:
    client = FakeTushare(response)
    archive = FinancialArchive(root, limits=limits or AcquisitionLimits())
    acquire_financial_batches(
        client, archive, (query,), run_day=date(2026, 9, 28), clock=_clock(at)
    )
    return client, archive


@pytest.mark.parametrize(
    ("api", "expected"),
    [
        ("fina_indicator", {"ts_code": "600000.SH", "period": "20251231"}),
        (
            "income",
            {"ts_code": "600000.SH", "start_date": "20260401", "end_date": "20260501"},
        ),
        (
            "balancesheet",
            {"ts_code": "600000.SH", "start_date": "20260401", "end_date": "20260501"},
        ),
        (
            "cashflow",
            {"ts_code": "600000.SH", "start_date": "20260401", "end_date": "20260501"},
        ),
        (
            "forecast",
            {"ts_code": "600000.SH", "start_date": "20260401", "end_date": "20260501"},
        ),
        (
            "express",
            {"ts_code": "600000.SH", "start_date": "20260401", "end_date": "20260501"},
        ),
        ("dividend", {"ts_code": "600000.SH", "ann_date": "20260420"}),
    ],
)
def test_seven_apis_use_their_actual_date_filter_and_preserve_old_period(
    tmp_path: Path, api: str, expected: dict[str, str]
) -> None:
    query = _query(api)
    client, archive = _acquire(tmp_path / "archive", query, pd.DataFrame([_row(api)]))

    assert client.calls == [(api, expected)]
    batch = archive.read_batch(query.request_id)
    assert batch.query == query
    assert batch.rows[0].values["end_date"] == "20251231"
    assert batch.rows[0].values["ann_date"] == "20260420"
    assert batch.observed_at == datetime(2026, 9, 28, 12, tzinfo=UTC)
    assert batch.status == "observed"


@pytest.mark.parametrize(
    "changes",
    [
        {"start_date": date(2026, 3, 31)},
        {"end_date": date(2026, 9, 29)},
        {"ts_code": ""},
        {"period": date(2025, 12, 31)},
    ],
)
def test_invalid_or_unbounded_query_never_calls_supplier(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    client = FakeTushare(pd.DataFrame([_row()]))
    archive = FinancialArchive(tmp_path / "archive")
    query = _query().model_copy(update=changes)

    with pytest.raises(ValueError):
        acquire_financial_batches(
            client, archive, (query,), run_day=date(2026, 9, 28), clock=_clock(datetime.now(UTC))
        )
    assert client.calls == []
    assert archive.list_committed() == ()


def test_explicit_request_and_symbol_caps_are_checked_before_fetch(tmp_path: Path) -> None:
    client = FakeTushare(pd.DataFrame([_row()]))
    archive = FinancialArchive(tmp_path / "archive", limits=AcquisitionLimits(max_requests=1))
    with pytest.raises(ValueError):
        acquire_financial_batches(
            client,
            archive,
            (_query(), _query()),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime.now(UTC)),
        )
    assert client.calls == []

    archive = FinancialArchive(tmp_path / "archive2", limits=AcquisitionLimits(max_symbols=1))
    with pytest.raises(ValueError):
        acquire_financial_batches(
            client,
            archive,
            (_query(), _query(ts_code="000001.SZ")),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime.now(UTC)),
        )
    assert client.calls == []


@pytest.mark.parametrize(
    "bad_row",
    [
        _row(ts_code="000001.SZ"),
        _row(ann_date="20260331"),
        _row(end_date="20251331"),
        _row(f_ann_date="20260432"),
        _row(report_type=1),
        _row(value=float("nan")),
        _row(value=float("inf")),
        _row(value=object()),
        _row(value="x" * 100),
    ],
)
def test_bad_second_row_rejects_whole_response_before_observation_clock(
    tmp_path: Path, bad_row: dict[str, object]
) -> None:
    archive = FinancialArchive(tmp_path / "archive", limits=AcquisitionLimits(max_field_chars=64))
    client = FakeTushare(pd.DataFrame([_row(), bad_row]))
    ticks: list[int] = []

    def clock() -> datetime:
        ticks.append(1)
        return datetime(2026, 9, 28, 12, tzinfo=UTC)

    with pytest.raises((ValueError, TypeError)):
        acquire_financial_batches(
            client, archive, (_query(),), run_day=date(2026, 9, 28), clock=clock
        )
    assert ticks == []
    assert archive.list_committed() == ()


def test_missing_required_response_column_rejects_entire_batch(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    client = FakeTushare(pd.DataFrame([{"ts_code": "600000.SH", "ann_date": "20260420"}]))
    with pytest.raises(ValueError):
        acquire_financial_batches(
            client,
            archive,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime.now(UTC)),
        )
    assert archive.list_committed() == ()


def test_missing_announcement_is_sealed_but_not_pit_usable(tmp_path: Path) -> None:
    query = _query()
    _client, archive = _acquire(tmp_path / "archive", query, pd.DataFrame([_row(ann_date=None)]))
    batch = archive.read_batch(query.request_id)
    assert batch.rows[0].values["ann_date"] is None
    assert batch.rows[0].pit_usable is False


def test_cap_touch_is_possible_truncation_and_empty_is_only_an_observation(tmp_path: Path) -> None:
    query = _query("fina_indicator")
    rows = [_row("fina_indicator", value=float(index)) for index in range(100)]
    _client, archive = _acquire(tmp_path / "archive", query, pd.DataFrame(rows))
    assert archive.read_batch(query.request_id).status == "possibly_truncated"

    empty_query = _query("income")
    empty_columns = list(_row().keys())
    _client, archive = _acquire(
        tmp_path / "archive",
        empty_query,
        pd.DataFrame(columns=empty_columns),
        at=datetime(2026, 9, 28, 12, 0, 1, tzinfo=UTC),
    )
    assert archive.read_batch(empty_query.request_id).status == "empty"
    assert archive.read_batch(empty_query.request_id).rows == ()


def test_supplier_error_does_not_look_like_empty_success(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    with pytest.raises(PermissionError):
        acquire_financial_batches(
            FakeTushare(PermissionError("quota or permission")),
            archive,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert archive.list_committed() == ()


def test_reopen_idempotency_and_persistent_high_water_reject_rollback(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    query = _query()
    _client, archive = _acquire(root, query, pd.DataFrame([_row(value=100.0)]))
    original = archive.receipt(query.request_id)
    assert original.observed_at == datetime(2026, 9, 28, 12, tzinfo=UTC)

    reopened = FinancialArchive(root)
    retry_client = FakeTushare(PermissionError("retry must not fetch"))
    retried = acquire_financial_batches(
        retry_client,
        reopened,
        (query,),
        run_day=date(2026, 9, 28),
        clock=_clock(datetime(2026, 9, 28, 11, tzinfo=UTC)),
    )
    assert retried == (original,)
    assert retry_client.calls == []

    with pytest.raises(ValueError):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row(value=120.0)])),
            reopened,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 11, tzinfo=UTC)),
        )
    with pytest.raises(ValueError):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row(value=120.0)])),
            reopened,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert len(reopened.list_committed()) == 1


def test_request_id_reuse_with_other_query_is_a_conflict(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    query = _query()
    _client, archive = _acquire(root, query, pd.DataFrame([_row()]))
    changed = query.model_copy(update={"ts_code": "000001.SZ"})
    with pytest.raises(ValueError):
        acquire_financial_batches(
            FakeTushare(PermissionError("must not fetch")),
            archive,
            (changed,),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 13, tzinfo=UTC)),
        )


def test_utc_normalization_and_naive_time_rejection(tmp_path: Path) -> None:
    query = _query()
    offset_time = datetime(2026, 9, 28, 20, tzinfo=ZoneInfo("Asia/Shanghai"))
    _client, archive = _acquire(tmp_path / "archive", query, pd.DataFrame([_row()]), at=offset_time)
    assert archive.receipt(query.request_id).observed_at == datetime(2026, 9, 28, 12, tzinfo=UTC)

    with pytest.raises(ValueError):
        _acquire(
            tmp_path / "archive",
            _query(),
            pd.DataFrame([_row()]),
            at=datetime(2026, 9, 28, 13),
        )
    assert len(archive.list_committed()) == 1


def test_revision_a_b_a_has_three_distinct_first_observations_and_pit_gate(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    times = [datetime(2026, 9, 28, hour, tzinfo=UTC) for hour in (1, 2, 3)]
    queries = [_query() for _ in times]
    for query, value, at in zip(queries, (100.0, 120.0, 100.0), times, strict=True):
        _acquire(root, query, pd.DataFrame([_row(value=value)]), at=at)

    archive = FinancialArchive(root)
    versions = observed_versions(archive.list_committed())
    assert [item.query.api for item in versions] == ["income"] * 3
    assert [item.first_response_status for item in versions] == ["observed"] * 3
    assert [item.first_observed_at for item in versions] == times
    assert [item.row.values["value"] for item in versions] == [100.0, 120.0, 100.0]
    assert versions[0].row.content_sha256 == versions[2].row.content_sha256

    def fact(version: FinancialObservedVersion) -> FinancialFact:
        return FinancialFact(
            source_api="income",
            field="value",
            ts_code="600000.SH",
            report_period=date(2025, 12, 31),
            report_type="1",
            ann_date=date(2026, 4, 20),
            f_ann_date=date(2026, 4, 21),
            first_observed_at=version.first_observed_at,
            value=Decimal(str(version.row.values["value"])),
        )

    start = date(2026, 4, 20)
    end = date(2026, 9, 29)
    days = tuple(
        SSECalendarDay(day=start + timedelta(days=i), is_open=True)
        for i in range((end - start).days + 1)
    )
    calendar = SSECalendar(coverage_start=start, coverage_end=end, days=days)
    facts = tuple(map(fact, versions))
    past = select_financial_fact(facts, as_of=times[0] + timedelta(minutes=1), calendar=calendar)
    after_b = select_financial_fact(facts, as_of=times[1] + timedelta(minutes=1), calendar=calendar)
    after_a = select_financial_fact(facts, as_of=times[2] + timedelta(minutes=1), calendar=calendar)
    assert past.fact is not None and past.fact.value == Decimal("100.0")
    assert after_b.fact is not None and after_b.fact.value == Decimal("120.0")
    assert after_a.fact is not None and after_a.fact.value == Decimal("100.0")


def test_file_readback_detects_tamper_and_truncation(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    query = _query()
    _client, archive = _acquire(root, query, pd.DataFrame([_row()]))
    receipt = archive.receipt(query.request_id)
    target = root / receipt.relative_path
    original = target.read_bytes()
    assert json.loads(original)["row_count"] == 1

    target.write_bytes(original.replace(b"100.0", b"101.0"))
    with pytest.raises(ValueError):
        archive.read_batch(query.request_id)
    target.write_bytes(original[: len(original) // 2])
    with pytest.raises(ValueError):
        archive.read_batch(query.request_id)


def test_symlinked_batch_directory_is_rejected_without_external_write(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "batches").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError):
        FinancialArchive(root)
    assert list(outside.iterdir()) == []


def test_interruption_after_file_publish_leaves_only_an_uncommitted_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    query = _query()
    publish = archive._publish_file

    def interrupt(root_fd: int, request_id: UUID, data: bytes) -> None:
        publish(root_fd, request_id, data)
        raise RuntimeError("process failed before SQLite commit")

    monkeypatch.setattr(archive, "_publish_file", interrupt)
    with pytest.raises(RuntimeError):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row()])),
            archive,
            (query,),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert (root / "batches" / f"{query.request_id}.json").exists()
    assert FinancialArchive(root).list_committed() == ()
    with pytest.raises(ValueError, match="target already exists"):
        _acquire(root, query, pd.DataFrame([_row()]))


def test_target_symlink_conflict_does_not_write_outside_archive(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    query = _query()
    outside = tmp_path / "outside.json"
    outside.write_text("unchanged")
    (root / "batches" / f"{query.request_id}.json").symlink_to(outside)
    with pytest.raises(ValueError, match="target already exists"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row()])),
            archive,
            (query,),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert outside.read_text() == "unchanged"
    assert archive.list_committed() == ()


def test_partial_file_write_does_not_publish_or_commit_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.financial_pit_acquisition as acquisition

    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    original_write = os.write
    calls = 0

    def short_then_fail(descriptor: int, payload: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return original_write(descriptor, payload[: len(payload) // 2])
        raise OSError("write interrupted")

    monkeypatch.setattr(acquisition.os, "write", short_then_fail)
    with pytest.raises(OSError, match="write interrupted"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row(), _row()])),
            archive,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert calls == 2
    assert list((root / "batches").iterdir()) == []
    assert archive.list_committed() == ()


def test_read_rejects_symlink_even_when_target_bytes_match_manifest(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    query = _query()
    _client, archive = _acquire(root, query, pd.DataFrame([_row()]))
    path = root / archive.receipt(query.request_id).relative_path
    outside = tmp_path / "outside.json"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(ValueError):
        archive.read_batch(query.request_id)


def test_manifest_symlink_is_rejected_before_sqlite_open(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    FinancialArchive(root)
    outside = tmp_path / "outside.sqlite3"
    outside.write_text("unchanged")
    (root / "manifest.sqlite3").unlink()
    (root / "manifest.sqlite3").symlink_to(outside)
    with pytest.raises(ValueError, match="manifest"):
        FinancialArchive(root)
    assert outside.read_text() == "unchanged"


def test_identical_adjacent_observations_keep_the_original_first_seen(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    times = [datetime(2026, 9, 28, hour, tzinfo=UTC) for hour in (1, 2, 3, 4)]
    for value, at in zip((100.0, 100.0, 120.0, 100.0), times, strict=True):
        _acquire(root, _query(), pd.DataFrame([_row(value=value)]), at=at)
    batches = FinancialArchive(root).list_committed()
    versions = observed_versions(batches)
    assert len(batches) == 4
    assert [version.first_observed_at for version in versions] == [times[0], times[2], times[3]]


def test_same_batch_conflicting_logical_key_is_evidence_but_not_pit_usable(
    tmp_path: Path,
) -> None:
    root = tmp_path / "archive"
    first_at = datetime(2026, 9, 28, 1, tzinfo=UTC)
    conflict_at = datetime(2026, 9, 28, 2, tzinfo=UTC)
    _acquire(root, _query(), pd.DataFrame([_row(value=90.0)]), at=first_at)
    query = _query()
    _client, archive = _acquire(
        root,
        query,
        pd.DataFrame([_row(value=100.0), _row(value=120.0)]),
        at=conflict_at,
    )
    batch = archive.read_batch(query.request_id)
    assert batch.row_count == 2
    assert all(row.conflicted and not row.pit_usable for row in batch.rows)
    versions = observed_versions(archive.list_committed())
    assert len(versions) == 3
    assert [version.first_observed_at for version in versions] == [
        first_at,
        conflict_at,
        conflict_at,
    ]
    start = date(2026, 4, 20)
    end = date(2026, 9, 29)
    calendar = SSECalendar(
        coverage_start=start,
        coverage_end=end,
        days=tuple(
            SSECalendarDay(day=start + timedelta(days=i), is_open=True)
            for i in range((end - start).days + 1)
        ),
    )
    facts = tuple(
        FinancialFact(
            source_api=version.query.api,
            field="value",
            ts_code="600000.SH",
            report_period=date(2025, 12, 31),
            report_type="1",
            ann_date=date(2026, 4, 20),
            f_ann_date=date(2026, 4, 21),
            first_observed_at=version.first_observed_at,
            value=Decimal(str(version.row.values["value"])),
        )
        for version in versions
    )
    decision = select_financial_fact(
        facts, as_of=conflict_at + timedelta(minutes=1), calendar=calendar
    )
    assert decision.status == "unknown"
    assert decision.reason == "conflicting_versions"


@pytest.mark.parametrize(("api", "count"), [("forecast", 3500), ("dividend", 2000)])
def test_other_documented_caps_are_marked_possible_truncation(
    tmp_path: Path, api: str, count: int
) -> None:
    query = _query(api)
    _client, archive = _acquire(tmp_path / "archive", query, pd.DataFrame([_row(api)] * count))
    assert archive.read_batch(query.request_id).status == "possibly_truncated"


def test_local_row_and_byte_limits_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _acquire(
            tmp_path / "rows",
            _query(),
            pd.DataFrame([_row(), _row()]),
            limits=AcquisitionLimits(max_rows_per_response=1),
        )
    assert FinancialArchive(tmp_path / "rows").list_committed() == ()
    with pytest.raises(ValueError, match="byte limit"):
        _acquire(
            tmp_path / "bytes",
            _query(),
            pd.DataFrame([_row()]),
            limits=AcquisitionLimits(max_batch_bytes=100),
        )
    assert FinancialArchive(tmp_path / "bytes").list_committed() == ()


def _process_acquire(root: str, request_id: str, gate: Any, output: Any) -> None:
    gate.wait(10)
    query = _query().model_copy(update={"request_id": UUID(request_id)})
    try:
        _acquire(Path(root), query, pd.DataFrame([_row()]))
        output.put("committed")
    except ValueError:
        output.put("rejected")


def test_two_processes_with_same_clock_serialize_through_sqlite(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    FinancialArchive(root)
    context = multiprocessing.get_context("spawn")
    gate = context.Event()
    output = context.Queue()
    children = [
        context.Process(target=_process_acquire, args=(os.fspath(root), str(uuid4()), gate, output))
        for _ in range(2)
    ]
    try:
        for child in children:
            child.start()
        gate.set()
        for child in children:
            child.join(15)
        assert all(child.exitcode == 0 for child in children)
        assert sorted((output.get(timeout=2), output.get(timeout=2))) == ["committed", "rejected"]
        assert len(FinancialArchive(root).list_committed()) == 1
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(5)


def test_two_processes_retrying_one_request_id_commit_only_one_batch(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    FinancialArchive(root)
    context = multiprocessing.get_context("spawn")
    gate = context.Event()
    output = context.Queue()
    request_id = str(uuid4())
    children = [
        context.Process(target=_process_acquire, args=(os.fspath(root), request_id, gate, output))
        for _ in range(2)
    ]
    try:
        for child in children:
            child.start()
        gate.set()
        for child in children:
            child.join(15)
        assert all(child.exitcode == 0 for child in children)
        assert [output.get(timeout=2) for _ in children] == ["committed", "committed"]
        assert len(FinancialArchive(root).list_committed()) == 1
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(5)


def test_missing_persistent_manifest_cannot_reset_the_observation_clock(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    (root / "manifest.sqlite3").unlink()
    with pytest.raises(ValueError, match="manifest"):
        FinancialArchive(root)


def test_missing_high_water_cannot_be_rebuilt_behind_committed_batches(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    with sqlite3.connect(root / "manifest.sqlite3") as connection:
        connection.execute("DELETE FROM clock_high_water")
    with pytest.raises(ValueError, match="high-water"):
        FinancialArchive(root)
    assert len(list((root / "batches").glob("*.json"))) == 1


def test_manifest_listing_blocks_concurrent_writer_until_snapshot_read_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    reader = FinancialArchive(root)
    real_load = reader._load_batch
    later_query = _query()
    started = threading.Event()
    finished = threading.Event()
    children: list[threading.Thread] = []

    def write_later() -> None:
        started.set()
        _acquire(
            root,
            later_query,
            pd.DataFrame([_row(value=120.0)]),
            at=datetime(2026, 9, 28, 13, tzinfo=UTC),
        )
        finished.set()

    def load_with_interleaving(*args: object, **kwargs: object) -> object:
        child = threading.Thread(target=write_later)
        children.append(child)
        child.start()
        assert started.wait(2)
        assert not finished.is_set()
        return real_load(*args, **kwargs)

    monkeypatch.setattr(reader, "_load_batch", load_with_interleaving)
    snapshot = reader.list_committed()
    for child in children:
        child.join(5)
        assert not child.is_alive()
    assert finished.is_set()
    assert len(snapshot) == 1
    assert len(FinancialArchive(root).list_committed()) == 2


def test_manifest_request_id_must_match_sealed_query_identity(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    original = _query()
    _acquire(root, original, pd.DataFrame([_row()]))
    wrong_id = uuid4()
    with sqlite3.connect(root / "manifest.sqlite3") as connection:
        connection.execute(
            "UPDATE batch_manifest SET request_id = ? WHERE request_id = ?",
            (str(wrong_id), str(original.request_id)),
        )
    with pytest.raises(ValueError, match="request identity|snapshot"):
        FinancialArchive(root).read_batch(wrong_id)


def test_replaced_empty_regular_manifest_cannot_reset_committed_history(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    manifest = root / "manifest.sqlite3"
    manifest.unlink()
    manifest.write_bytes(b"")

    with pytest.raises(ValueError, match="manifest|snapshot|identity"):
        FinancialArchive(root)


def test_replaced_valid_empty_manifest_from_other_archive_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    other = tmp_path / "other"
    FinancialArchive(other)
    (root / "manifest.sqlite3").write_bytes((other / "manifest.sqlite3").read_bytes())

    with pytest.raises(ValueError, match="identity|manifest|snapshot"):
        FinancialArchive(root)


def test_manifest_symlink_swap_only_during_sqlite_connect_cannot_write_outside(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.financial_pit_acquisition as acquisition

    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    manifest = root / "manifest.sqlite3"
    outside = tmp_path / "outside.sqlite3"
    outside.write_bytes(b"")
    real_connect = sqlite3.connect
    swaps: list[str] = []

    def swap_during_connect(
        database: object, *args: object, **kwargs: object
    ) -> sqlite3.Connection:
        saved = root / "saved-manifest.sqlite3"
        manifest.rename(saved)
        manifest.symlink_to(outside)
        swaps.append(str(database))
        try:
            return real_connect(database, *args, **kwargs)
        finally:
            manifest.unlink()
            saved.rename(manifest)

    monkeypatch.setattr(acquisition.sqlite3, "connect", swap_during_connect)
    archive._initialize_database()

    assert swaps
    assert outside.read_bytes() == b""


def test_hardlinked_sqlite_shm_sidecar_cannot_write_outside(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    outside = tmp_path / "outside-shm"
    outside.write_bytes(b"X" * 32768)
    os.link(outside, root / "manifest.sqlite3-shm")

    with pytest.raises(ValueError, match="sidecar|shm|unsafe"):
        FinancialArchive(root)
    assert outside.read_bytes() == b"X" * 32768


def test_wide_supplier_response_is_rejected_before_observation_clock(tmp_path: Path) -> None:
    row = _row()
    row.update({f"extra_{index}": "x" for index in range(300)})
    ticks: list[int] = []

    def clock() -> datetime:
        ticks.append(1)
        return datetime(2026, 9, 28, 12, tzinfo=UTC)

    archive = FinancialArchive(tmp_path / "archive")
    with pytest.raises(ValueError, match="column|field|response"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([row])),
            archive,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=clock,
        )
    assert ticks == []
    assert archive.list_committed() == ()


def test_serialized_response_over_byte_cap_is_rejected_before_observation_clock(
    tmp_path: Path,
) -> None:
    ticks: list[int] = []

    def clock() -> datetime:
        ticks.append(1)
        return datetime(2026, 9, 28, 12, tzinfo=UTC)

    archive = FinancialArchive(tmp_path / "archive", limits=AcquisitionLimits(max_batch_bytes=100))
    with pytest.raises(ValueError, match="byte limit"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row()])),
            archive,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=clock,
        )
    assert ticks == []
    assert archive.list_committed() == ()


def test_old_valid_snapshot_from_same_archive_cannot_roll_back_history(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    old_snapshot = (root / "manifest.sqlite3").read_bytes()
    _acquire(
        root,
        _query(),
        pd.DataFrame([_row(value=120.0)]),
        at=datetime(2026, 9, 28, 13, tzinfo=UTC),
    )
    (root / "manifest.sqlite3").write_bytes(old_snapshot)

    with pytest.raises(ValueError, match="snapshot|anchor|rollback"):
        FinancialArchive(root)


def test_directory_fsync_error_after_snapshot_replace_recovers_same_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.financial_pit_acquisition as acquisition

    root = tmp_path / "archive"
    FinancialArchive(root)
    query = _query()
    root_inode = root.stat().st_ino
    real_fsync = os.fsync
    failed = False

    def uncertain_fsync(descriptor: int) -> None:
        nonlocal failed
        info = os.fstat(descriptor)
        if not failed and stat.S_ISDIR(info.st_mode) and info.st_ino == root_inode:
            failed = True
            raise OSError("snapshot directory fsync failed")
        real_fsync(descriptor)

    monkeypatch.setattr(acquisition.os, "fsync", uncertain_fsync)
    with pytest.raises(OSError, match="directory fsync"):
        _acquire(root, query, pd.DataFrame([_row()]))
    assert failed
    monkeypatch.setattr(acquisition.os, "fsync", real_fsync)
    retry_client = FakeTushare(PermissionError("retry must not fetch"))
    recovered = FinancialArchive(root)
    receipt = acquire_financial_batches(
        retry_client,
        recovered,
        (query,),
        run_day=date(2026, 9, 28),
        clock=_clock(datetime(2026, 9, 28, 11, tzinfo=UTC)),
    )[0]
    assert receipt.observed_at == datetime(2026, 9, 28, 12, tzinfo=UTC)
    assert retry_client.calls == []
    assert len(recovered.list_committed()) == 1


def test_anchor_fsync_error_recovers_one_complete_snapshot_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.financial_pit_acquisition as acquisition

    root = tmp_path / "archive"
    FinancialArchive(root)
    anchor = root / "manifest.anchor.jsonl"
    anchor_inode = anchor.stat().st_ino
    initial_size = anchor.stat().st_size
    query = _query()
    real_fsync = os.fsync
    failed = False

    def uncertain_fsync(descriptor: int) -> None:
        nonlocal failed
        info = os.fstat(descriptor)
        if not failed and info.st_ino == anchor_inode and info.st_size > initial_size:
            failed = True
            raise OSError("anchor fsync failed")
        real_fsync(descriptor)

    monkeypatch.setattr(acquisition.os, "fsync", uncertain_fsync)
    with pytest.raises(OSError, match="anchor fsync"):
        _acquire(root, query, pd.DataFrame([_row()]))
    assert failed
    monkeypatch.setattr(acquisition.os, "fsync", real_fsync)
    retry_client = FakeTushare(PermissionError("retry must not fetch"))
    recovered = FinancialArchive(root)
    assert len(recovered.list_committed()) == 1
    receipt = acquire_financial_batches(
        retry_client,
        recovered,
        (query,),
        run_day=date(2026, 9, 28),
        clock=_clock(datetime(2026, 9, 28, 11, tzinfo=UTC)),
    )[0]
    assert receipt.observed_at == datetime(2026, 9, 28, 12, tzinfo=UTC)
    assert retry_client.calls == []


def test_partial_anchor_tail_is_trimmed_before_reading_committed_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    query = _query()
    _acquire(root, query, pd.DataFrame([_row()]))
    anchor = root / "manifest.anchor.jsonl"
    with anchor.open("ab") as stream:
        stream.write(b'{"generation":2')

    reopened = FinancialArchive(root)
    assert reopened.read_batch(query.request_id).row_count == 1
    assert anchor.read_bytes().endswith(b"\n")


@pytest.mark.parametrize("name", ["manifest.sqlite3", "manifest.anchor.jsonl"])
def test_replacing_pinned_state_file_before_snapshot_commit_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    query = _query()
    publish = archive._publish_file

    def replace_after_batch(root_fd: int, request_id: UUID, data: bytes) -> None:
        publish(root_fd, request_id, data)
        target = root / name
        saved = root / f"saved-{name}"
        target.rename(saved)
        target.write_bytes(saved.read_bytes())

    monkeypatch.setattr(archive, "_publish_file", replace_after_batch)
    with pytest.raises(ValueError, match="manifest|anchor"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row()])),
            archive,
            (query,),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert FinancialArchive(root).list_committed() == ()


def test_torn_anchor_append_after_snapshot_commit_is_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    query = _query()

    def torn_append(descriptor: int, record: object) -> None:
        os.write(descriptor, b'{"generation":')
        raise OSError("anchor append interrupted")

    monkeypatch.setattr(archive, "_append_anchor", torn_append)
    with pytest.raises(OSError, match="anchor append"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row()])),
            archive,
            (query,),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    assert (root / "manifest.anchor.jsonl").read_bytes().endswith(b'{"generation":')

    recovered = FinancialArchive(root)
    assert recovered.read_batch(query.request_id).row_count == 1
    assert (root / "manifest.anchor.jsonl").read_bytes().endswith(b"\n")


def test_snapshot_ahead_of_anchor_requires_intact_new_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    query = _query()

    def interrupted_anchor(descriptor: int, record: object) -> None:
        raise OSError("anchor not published")

    monkeypatch.setattr(archive, "_append_anchor", interrupted_anchor)
    with pytest.raises(OSError, match="anchor not published"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row()])),
            archive,
            (query,),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 12, tzinfo=UTC)),
        )
    (root / "batches" / f"{query.request_id}.json").unlink()

    with pytest.raises(ValueError, match="committed batch"):
        FinancialArchive(root)
