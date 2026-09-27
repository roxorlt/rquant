"""Offline acquisition and immutable observation behavior for seven Tushare APIs."""

from __future__ import annotations

import json
import multiprocessing
import os
import sqlite3
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

    def interrupt(request_id: UUID, data: bytes) -> None:
        publish(request_id, data)
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
    root.mkdir()
    (root / "batches").mkdir()
    outside = tmp_path / "outside.sqlite3"
    outside.write_text("unchanged")
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
    archive = FinancialArchive(root)
    with pytest.raises(ValueError, match="high-water"):
        acquire_financial_batches(
            FakeTushare(pd.DataFrame([_row(value=120.0)])),
            archive,
            (_query(),),
            run_day=date(2026, 9, 28),
            clock=_clock(datetime(2026, 9, 28, 11, tzinfo=UTC)),
        )
    assert len(list((root / "batches").glob("*.json"))) == 1


def test_manifest_listing_reads_rows_and_clock_from_one_sqlite_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    _acquire(root, _query(), pd.DataFrame([_row()]))
    reader = FinancialArchive(root)
    real_connect = reader._connect
    later_query = _query()
    invoked: list[bool] = []

    def connect_with_interleaving() -> sqlite3.Connection:
        connection = real_connect()

        def before_select(sql: str) -> None:
            if sql.startswith("SELECT observed_at FROM clock_high_water") and not invoked:
                invoked.append(True)
                _acquire(
                    root,
                    later_query,
                    pd.DataFrame([_row(value=120.0)]),
                    at=datetime(2026, 9, 28, 13, tzinfo=UTC),
                )

        connection.set_trace_callback(before_select)
        return connection

    monkeypatch.setattr(reader, "_connect", connect_with_interleaving)
    snapshot = reader.list_committed()
    assert invoked == [True]
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
    with pytest.raises(ValueError, match="request identity"):
        FinancialArchive(root).read_batch(wrong_id)
