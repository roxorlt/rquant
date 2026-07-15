"""Resumable historical minute backfill execution."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from rquant.backfill_manifest import MinuteBackfillTask, minute_session_spec
from rquant.backfill_state import (
    BackfillManifestInput,
    BackfillStateStore,
    BackfillTaskInput,
)
from rquant.storage.duckdb import DuckDBStore


def _task(
    task_id: str,
    ts_code: str,
    open_dates: tuple[date, ...],
) -> MinuteBackfillTask:
    rows = len(open_dates) * len(minute_session_spec().expected_times())
    return MinuteBackfillTask(
        task_id=task_id,
        ts_code=ts_code,
        source="tushare",
        freq="1min",
        start_date=open_dates[0],
        end_date=open_dates[-1],
        open_dates=open_dates,
        expected_rows=rows,
        response_row_limit=8_000,
        possible_truncation=rows == 8_000,
    )


def _persist_tasks(
    state: BackfillStateStore,
    tasks: tuple[MinuteBackfillTask, ...],
    *,
    manifest_id: str = "manifest-runner",
    max_attempts: int = 3,
) -> None:
    state.persist_manifest(
        BackfillManifestInput(
            manifest_id=manifest_id,
            payload={"strategy": "runner-test"},
            tasks=tuple(
                BackfillTaskInput(
                    task_id=task.task_id,
                    payload=task.model_dump(mode="json"),
                    max_attempts=max_attempts,
                )
                for task in tasks
            ),
            eligibility=(),
        )
    )


def _minute_frame(ts_code: str, dates: tuple[date, ...]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for trading_date in dates:
        for minute_time in minute_session_spec().expected_times():
            rows.append(
                {
                    "ts_code": ts_code,
                    "trade_time": datetime.combine(trading_date, minute_time),
                    "freq": "1min",
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.0,
                    "vol": 100.0,
                    "amount": 1_000.0,
                    "source": "tushare",
                }
            )
    return pd.DataFrame(rows)


class _FailIfCalledAdapter:
    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        raise AssertionError(f"unexpected request: {ts_code} {freq} {start} {end}")


def test_preexisting_complete_task_skips_api_after_interrupted_state_write(
    tmp_path: Path,
) -> None:
    from rquant.intraday_backfill import run_backfill_manifest

    task = _task("a" * 64, "300001.SZ", (date(2026, 6, 25),))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,))
    with DuckDBStore(tmp_path / "market.duckdb") as store:
        store.upsert_minute_bars(_minute_frame(task.ts_code, task.open_dates))
        summary = run_backfill_manifest(
            store,
            state,
            _FailIfCalledAdapter(),
            manifest_id="manifest-runner",
            worker_id="test-worker",
        )

    assert summary.claimed_tasks == 1
    assert summary.succeeded_tasks == 1
    assert summary.failed_tasks == 0
    assert summary.skipped_complete_tasks == 1
    assert summary.request_count == 0
    persisted = state.get_task("manifest-runner", task.task_id)
    assert persisted.status == "succeeded"
    assert persisted.metrics.covered_sessions == 1


def test_final_attempt_crash_recovers_completed_duckdb_rows_without_api(
    tmp_path: Path,
) -> None:
    from datetime import UTC, timedelta

    from rquant.intraday_backfill import run_backfill_manifest

    task = _task("3" * 64, "300001.SZ", (date(2026, 6, 25),))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,), max_attempts=1)
    claim = state.claim_task(
        "manifest-runner",
        worker_id="crashed-worker",
        lease_seconds=1,
        now=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert claim is not None
    with DuckDBStore(tmp_path / "market.duckdb") as store:
        store.upsert_minute_bars(_minute_frame(task.ts_code, task.open_dates))
        summary = run_backfill_manifest(
            store,
            state,
            _FailIfCalledAdapter(),
            manifest_id="manifest-runner",
            worker_id="recovery-worker",
        )

    assert summary.succeeded_tasks == 1
    assert summary.skipped_complete_tasks == 1
    assert summary.request_count == 0
    assert state.get_task("manifest-runner", task.task_id).status == "succeeded"


def test_final_attempt_crash_marks_incomplete_task_terminal_without_api(
    tmp_path: Path,
) -> None:
    from datetime import UTC, timedelta

    from rquant.intraday_backfill import run_backfill_manifest

    task = _task("4" * 64, "300001.SZ", (date(2026, 6, 25),))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,), max_attempts=1)
    claim = state.claim_task(
        "manifest-runner",
        worker_id="crashed-worker",
        lease_seconds=1,
        now=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert claim is not None
    with DuckDBStore(tmp_path / "market.duckdb") as store:
        summary = run_backfill_manifest(
            store,
            state,
            _FailIfCalledAdapter(),
            manifest_id="manifest-runner",
            worker_id="recovery-worker",
        )

    failed = state.get_task("manifest-runner", task.task_id)
    assert summary.failed_tasks == 1
    assert summary.request_count == 0
    assert failed.status == "failed"
    assert failed.failure is not None
    assert failed.failure.code == "lease_expired"
    assert failed.failure.retryable is False


def test_runtime_deadline_stops_before_claiming_another_task(tmp_path: Path) -> None:
    from datetime import UTC, timedelta

    from rquant.intraday_backfill import run_backfill_manifest

    task = _task("5" * 64, "300001.SZ", (date(2026, 6, 25),))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,))
    with DuckDBStore(tmp_path / "market.duckdb") as store:
        summary = run_backfill_manifest(
            store,
            state,
            _FailIfCalledAdapter(),
            manifest_id="manifest-runner",
            worker_id="test-worker",
            stop_before=datetime.now(UTC) - timedelta(seconds=1),
        )

    assert summary.claimed_tasks == 0
    assert state.get_task("manifest-runner", task.task_id).status == "pending"


def test_store_factory_releases_duckdb_during_source_request(tmp_path: Path) -> None:
    from rquant.intraday_backfill import run_backfill_manifest

    day = date(2026, 6, 25)
    task = _task("6" * 64, "300001.SZ", (day,))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,))
    active_stores = 0

    @contextmanager
    def store_factory():
        nonlocal active_stores
        with DuckDBStore(tmp_path / "market.duckdb") as current:
            active_stores += 1
            try:
                yield current
            finally:
                active_stores -= 1

    class Adapter:
        def stk_mins(self, ts_code, freq, start, end):
            del freq, start, end
            assert active_stores == 0
            return _minute_frame(ts_code, (day,))

    summary = run_backfill_manifest(
        None,
        state,
        Adapter(),
        manifest_id="manifest-runner",
        worker_id="test-worker",
        store_factory=store_factory,
    )

    assert summary.succeeded_tasks == 1
    assert active_stores == 0


def test_lost_lease_discards_late_source_rows_before_duckdb_write(
    tmp_path: Path,
) -> None:
    from datetime import UTC, timedelta

    from rquant.backfill_state import BackfillFailure
    from rquant.intraday_backfill import run_backfill_manifest

    day = date(2026, 6, 25)
    task = _task("7" * 64, "300001.SZ", (day,))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,), max_attempts=1)

    class Adapter:
        def stk_mins(self, ts_code, freq, start, end):
            del freq, start, end
            connection = state._connect()
            try:
                connection.execute(
                    "UPDATE backfill_task SET lease_expires_at = ? "
                    "WHERE manifest_id = ? AND task_id = ?",
                    (
                        (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                        "manifest-runner",
                        task.task_id,
                    ),
                )
            finally:
                connection.close()
            recovery = state.claim_task(
                "manifest-runner",
                worker_id="recovery-worker",
                lease_seconds=60,
            )
            assert recovery is not None and recovery.recovery_only
            state.mark_task_failed(
                recovery,
                failure=BackfillFailure(
                    code="lease_expired",
                    message="recovery found no complete rows",
                    retryable=False,
                ),
            )
            return _minute_frame(ts_code, (day,))

    with DuckDBStore(tmp_path / "market.duckdb") as store:
        summary = run_backfill_manifest(
            store,
            state,
            Adapter(),
            manifest_id="manifest-runner",
            worker_id="slow-worker",
        )
        written = store._conn.execute(
            "SELECT count(*) FROM minute_bar WHERE ts_code = ?",
            [task.ts_code],
        ).fetchone()[0]

    assert summary.lost_claim_tasks == 1
    assert summary.request_count == 1
    assert summary.returned_rows == 241
    assert written == 0
    assert state.get_task("manifest-runner", task.task_id).failure.code == (
        "lease_expired"
    )


def test_source_empty_fails_one_task_but_runner_continues(tmp_path: Path) -> None:
    from rquant.intraday_backfill import run_backfill_manifest

    day = date(2026, 6, 25)
    empty_task = _task("b" * 64, "300001.SZ", (day,))
    good_task = _task("c" * 64, "300002.SZ", (day,))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (empty_task, good_task))

    class Adapter:
        def stk_mins(self, ts_code, freq, start, end):
            del freq, start, end
            if ts_code == empty_task.ts_code:
                return pd.DataFrame()
            return _minute_frame(ts_code, (day,))

    with DuckDBStore(tmp_path / "market.duckdb") as store:
        summary = run_backfill_manifest(
            store,
            state,
            Adapter(),
            manifest_id="manifest-runner",
            worker_id="test-worker",
        )

    assert summary.claimed_tasks == 2
    assert summary.succeeded_tasks == 1
    assert summary.failed_tasks == 1
    assert state.get_task("manifest-runner", empty_task.task_id).failure.code == (
        "source_empty"
    )
    assert state.get_task("manifest-runner", good_task.task_id).status == "succeeded"


def test_unexpected_task_error_is_recorded_and_runner_continues(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant.intraday_backfill import run_backfill_manifest

    day = date(2026, 6, 25)
    broken_task = _task("1" * 64, "300001.SZ", (day,))
    good_task = _task("2" * 64, "300002.SZ", (day,))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (broken_task, good_task))

    class Adapter:
        def stk_mins(self, ts_code, freq, start, end):
            del freq, start, end
            return _minute_frame(ts_code, (day,))

    with DuckDBStore(tmp_path / "market.duckdb") as store:
        original_upsert = store.upsert_minute_bars

        def flaky_upsert(frame: pd.DataFrame) -> int:
            if frame.iloc[0]["ts_code"] == broken_task.ts_code:
                raise RuntimeError("simulated DuckDB write failure")
            return original_upsert(frame)

        monkeypatch.setattr(store, "upsert_minute_bars", flaky_upsert)
        summary = run_backfill_manifest(
            store,
            state,
            Adapter(),
            manifest_id="manifest-runner",
            worker_id="test-worker",
        )

    broken = state.get_task("manifest-runner", broken_task.task_id)
    assert summary.claimed_tasks == 2
    assert summary.failed_tasks == 1
    assert summary.succeeded_tasks == 1
    assert broken.failure is not None
    assert broken.failure.code == "task_execution_error"
    assert broken.failure.retryable is True
    assert state.get_task("manifest-runner", good_task.task_id).status == "succeeded"


def test_exact_row_limit_response_is_split_before_writing(tmp_path: Path) -> None:
    from rquant.intraday_backfill import run_backfill_manifest

    days = (date(2026, 6, 25), date(2026, 6, 26))
    task = _task("d" * 64, "300001.SZ", days)
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,))

    class Adapter:
        def __init__(self) -> None:
            self.calls: list[tuple[date, date]] = []

        def stk_mins(self, ts_code, freq, start, end):
            del freq
            self.calls.append((start.date(), end.date()))
            if start.date() != end.date():
                row = _minute_frame(ts_code, (start.date(),)).iloc[0].to_dict()
                return pd.DataFrame([row] * 8_000)
            return _minute_frame(ts_code, (start.date(),))

    adapter = Adapter()
    with DuckDBStore(tmp_path / "market.duckdb") as store:
        summary = run_backfill_manifest(
            store,
            state,
            adapter,
            manifest_id="manifest-runner",
            worker_id="test-worker",
        )
        count = store._conn.execute(
            "SELECT count(*) FROM minute_bar WHERE ts_code = ?",
            [task.ts_code],
        ).fetchone()[0]

    assert adapter.calls == [(days[0], days[1]), (days[0], days[0]), (days[1], days[1])]
    assert summary.succeeded_tasks == 1
    assert summary.request_count == 3
    assert summary.returned_rows == 8_000 + 482
    assert summary.written_rows == 482
    assert count == 482


def test_empty_prelisting_session_is_allowed_missing(tmp_path: Path) -> None:
    from rquant.intraday_backfill import run_backfill_manifest

    day = date(2026, 6, 25)
    task = _task("e" * 64, "300001.SZ", (day,))
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    _persist_tasks(state, (task,))

    class EmptyAdapter:
        def stk_mins(self, ts_code, freq, start, end):
            del ts_code, freq, start, end
            return pd.DataFrame()

    with DuckDBStore(tmp_path / "market.duckdb") as store:
        store.upsert_stock_basic(
            pd.DataFrame(
                [
                    {
                        "ts_code": task.ts_code,
                        "symbol": "300001",
                        "name": "未上市样本",
                        "area": "深圳",
                        "industry": "测试",
                        "list_date": "20260701",
                        "market": "创业板",
                    }
                ]
            )
        )
        summary = run_backfill_manifest(
            store,
            state,
            EmptyAdapter(),
            manifest_id="manifest-runner",
            worker_id="test-worker",
        )

    persisted = state.get_task("manifest-runner", task.task_id)
    assert summary.succeeded_tasks == 1
    assert persisted.status == "succeeded"
    assert persisted.metrics.allowed_missing_sessions == 1
