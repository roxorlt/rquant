from __future__ import annotations

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event, Lock

import pandas as pd
import pytest

import rquant.adapter.tushare as tushare_module
from rquant.daily_close_gateway import DailyCloseFetchResult, DailyCloseSourceRequest
from rquant.runtime_builder_daily import _tushare_daily_close_fetcher
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaConflictError,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)
from rquant.source_quota_transport import QuotaBoundTransportObserver

OBSERVED = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)
TRADE_DATE = date(2026, 7, 31)


class _MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class _FakeDailyPro:
    def __init__(self, *, fail_daily_once: bool = False) -> None:
        self.calls: list[str] = []
        self._fail_daily_once = fail_daily_once

    def daily(self, **_kwargs: str) -> pd.DataFrame:
        self.calls.append("daily")
        if self._fail_daily_once:
            self._fail_daily_once = False
            raise TimeoutError("retry daily")
        return pd.DataFrame(
            (
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260731",
                    "open": 10.0,
                    "high": 10.4,
                    "low": 9.9,
                    "close": 10.2,
                    "pre_close": 9.95,
                    "change": 0.25,
                    "pct_chg": 2.51,
                    "vol": 1_000.0,
                    "amount": 10_200.0,
                },
            )
        )

    def daily_basic(self, **_kwargs: str) -> pd.DataFrame:
        self.calls.append("daily_basic")
        return pd.DataFrame(
            (
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260731",
                    "turnover_rate": 0.5,
                    "volume_ratio": 1.2,
                    "total_mv": 200.0,
                    "circ_mv": 180.0,
                },
            )
        )

    def adj_factor(self, **_kwargs: str) -> pd.DataFrame:
        self.calls.append("adj_factor")
        return pd.DataFrame(
            ({"ts_code": "600000.SH", "trade_date": "20260731", "adj_factor": 1.01},)
        )

    def index_daily(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append(f"index_daily:{kwargs['ts_code']}")
        return pd.DataFrame(
            (
                {
                    "ts_code": kwargs["ts_code"],
                    "trade_date": "20260731",
                    "open": 3_200.0,
                    "high": 3_230.0,
                    "low": 3_190.0,
                    "close": 3_220.0,
                    "pre_close": 3_198.0,
                    "change": 22.0,
                    "pct_chg": 0.688,
                    "vol": 2_000.0,
                    "amount": 30_000.0,
                },
            )
        )

    def stock_basic(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append("stock_basic")
        assert "list_status" in kwargs["fields"].split(",")
        return pd.DataFrame(
            (
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "list_status": kwargs["list_status"],
                },
            )
        )

    def stock_st(self, **_kwargs: str) -> pd.DataFrame:
        self.calls.append("stock_st")
        return pd.DataFrame(columns=("ts_code", "name", "trade_date", "type", "type_name"))

    def suspend_d(self, **_kwargs: str) -> pd.DataFrame:
        self.calls.append("suspend_d")
        return pd.DataFrame(columns=("ts_code", "trade_date", "suspend_timing", "suspend_type"))


def _fetcher(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake: _FakeDailyPro,
) -> tuple[QuotaBoundTransportObserver, object]:
    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda _token: fake)
    monkeypatch.setattr(tushare_module.time, "sleep", lambda _seconds: None)
    observer = QuotaBoundTransportObserver(
        path=tmp_path / "quota.sqlite3",
        source="tushare.daily_close",
        quota_units_per_window=20,
        window_kind="day",
        clock=lambda: OBSERVED,
    )
    fetch = _tushare_daily_close_fetcher(
        {"TUSHARE_TOKEN_MAIN": "not-a-real-token"},
        transport_observer=observer,
    )
    return observer, fetch


def _request() -> DailyCloseSourceRequest:
    return DailyCloseSourceRequest(source="tushare.daily_close", trade_date=TRADE_DATE)


def test_daily_receipt_counts_eleven_real_transport_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeDailyPro()
    observer, fetch = _fetcher(tmp_path, monkeypatch, fake)

    with observer.scope(logical_request_id="capture-1", observed_at=OBSERVED):
        result = fetch(_request())

    assert isinstance(result, DailyCloseFetchResult)
    assert result.actual_call_count == 11
    assert len(result.call_receipts) == 11
    assert len(fake.calls) == 11
    assert observer.remaining(now=OBSERVED) == 9


def test_each_retry_gets_a_separate_failure_or_success_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeDailyPro(fail_daily_once=True)
    observer, fetch = _fetcher(tmp_path, monkeypatch, fake)

    with observer.scope(logical_request_id="capture-retry", observed_at=OBSERVED):
        result = fetch(_request())

    assert isinstance(result, DailyCloseFetchResult)
    assert result.actual_call_count == 12
    assert [receipt.outcome for receipt in result.call_receipts[:2]] == [
        SourceQuotaAttemptOutcome.FAILURE,
        SourceQuotaAttemptOutcome.SUCCESS,
    ]
    assert fake.calls[:2] == ["daily", "daily"]
    assert observer.remaining(now=OBSERVED) == 8


def test_two_captures_never_send_more_than_twenty_transport_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeDailyPro()
    observer, fetch = _fetcher(tmp_path, monkeypatch, fake)

    with observer.scope(logical_request_id="capture-1", observed_at=OBSERVED):
        first = fetch(_request())
    with (
        pytest.raises(SourceQuotaExhaustedError),
        observer.scope(logical_request_id="capture-2", observed_at=OBSERVED),
    ):
        fetch(_request())

    assert isinstance(first, DailyCloseFetchResult)
    assert first.actual_call_count == 11
    assert len(fake.calls) == 20
    assert observer.remaining(now=OBSERVED) == 0


def test_killed_transport_stays_pending_and_restart_cannot_dispatch_again(
    tmp_path: Path,
) -> None:
    path = tmp_path / "quota.sqlite3"
    first_store = SourceQuotaStore(path, boot_id="boot-a", monotonic_ns=lambda: 0)
    first = QuotaBoundTransportObserver(
        store=first_store,
        source="tushare.daily_close",
        quota_units_per_window=1,
        window_kind="day",
        clock=lambda: OBSERVED,
    )
    calls = 0

    def kill() -> None:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt()

    with (
        pytest.raises(KeyboardInterrupt),
        first.scope(logical_request_id="capture-kill", observed_at=OBSERVED),
    ):
        first.observe("daily", kill)

    restarted_store = SourceQuotaStore(
        path,
        boot_id="boot-b",
        monotonic_ns=lambda: 1,
    )
    restarted = QuotaBoundTransportObserver(
        store=restarted_store,
        source="tushare.daily_close",
        quota_units_per_window=1,
        window_kind="day",
        clock=lambda: OBSERVED,
    )
    with (
        pytest.raises(SourceQuotaConflictError, match="uncertain"),
        restarted.scope(logical_request_id="capture-kill", observed_at=OBSERVED),
    ):
        restarted.observe("daily", lambda: pytest.fail("must not call transport"))

    assert calls == 1
    (pending,) = restarted_store.list_attempts(source="tushare.daily_close")
    assert pending.outcome is SourceQuotaAttemptOutcome.PENDING
    recovered = restarted_store.recover_stale_attempts(
        source="tushare.daily_close",
        now=OBSERVED,
        min_age=timedelta(minutes=5),
    )
    assert recovered[0].outcome is SourceQuotaAttemptOutcome.UNKNOWN
    assert restarted.remaining(now=OBSERVED) == 0


@pytest.mark.parametrize("kill_ordinal", (2, 6, 11))
def test_logical_request_outcome_includes_every_transport_attempt(
    tmp_path: Path,
    kill_ordinal: int,
) -> None:
    path = tmp_path / "quota.sqlite3"
    first_store = SourceQuotaStore(path, boot_id="boot-a", monotonic_ns=lambda: 0)
    first = QuotaBoundTransportObserver(
        store=first_store,
        source="tushare.daily_close",
        quota_units_per_window=20,
        window_kind="day",
        clock=lambda: OBSERVED,
    )
    logical_request_id = f"capture-kill-{kill_ordinal}"

    def kill() -> None:
        raise KeyboardInterrupt()

    with (
        pytest.raises(KeyboardInterrupt),
        first.scope(logical_request_id=logical_request_id, observed_at=OBSERVED),
    ):
        for ordinal in range(1, kill_ordinal + 1):
            operation = kill if ordinal == kill_ordinal else lambda: None
            first.observe(f"call-{ordinal}", operation)

    assert first.request_outcome(logical_request_id) is SourceQuotaAttemptOutcome.PENDING
    assert len(first.request_attempts(logical_request_id)) == kill_ordinal

    restarted_store = SourceQuotaStore(
        path,
        boot_id="boot-b",
        monotonic_ns=lambda: 1,
    )
    restarted_store.recover_stale_attempts(
        source="tushare.daily_close",
        now=OBSERVED,
        min_age=timedelta(minutes=5),
    )
    restarted = QuotaBoundTransportObserver(
        store=restarted_store,
        source="tushare.daily_close",
        quota_units_per_window=20,
        window_kind="day",
        clock=lambda: OBSERVED,
    )
    external_calls = 0

    def forbidden() -> None:
        nonlocal external_calls
        external_calls += 1

    assert restarted.request_outcome(logical_request_id) is SourceQuotaAttemptOutcome.UNKNOWN
    with (
        pytest.raises(SourceQuotaConflictError),
        restarted.scope(logical_request_id=logical_request_id, observed_at=OBSERVED),
    ):
        restarted.observe("call-1", forbidden)
    assert external_calls == 0


def test_logical_request_outcome_aggregates_mixed_terminal_results(tmp_path: Path) -> None:
    observer = QuotaBoundTransportObserver(
        path=tmp_path / "quota.sqlite3",
        source="tushare.reference_slow",
        quota_units_per_window=20,
        window_kind="minute",
        clock=lambda: OBSERVED,
    )
    with observer.scope(logical_request_id="mixed", observed_at=OBSERVED):
        with pytest.raises(TimeoutError):
            observer.observe("stock_st", lambda: (_ for _ in ()).throw(TimeoutError()))
        observer.observe("stock_basic", lambda: None)
    with observer.scope(logical_request_id="all-success", observed_at=OBSERVED):
        observer.observe("stock_st", lambda: None)
        observer.observe("stock_basic", lambda: None)

    assert observer.request_outcome("mixed") is SourceQuotaAttemptOutcome.FAILURE
    assert observer.request_outcome("all-success") is SourceQuotaAttemptOutcome.SUCCESS


def test_retry_crossing_minute_uses_dispatch_time_window_and_caps_new_minute(
    tmp_path: Path,
) -> None:
    before_boundary = datetime(2026, 7, 31, 9, 0, 59, tzinfo=UTC)
    after_boundary = datetime(2026, 7, 31, 9, 1, tzinfo=UTC)
    clock = _MutableClock(before_boundary)
    observer = QuotaBoundTransportObserver(
        path=tmp_path / "quota.sqlite3",
        source="tushare.reference_slow",
        quota_units_per_window=12,
        window_kind="minute",
        clock=clock,
    )
    transport_calls = 0

    def transport(*, fail: bool = False) -> None:
        nonlocal transport_calls
        transport_calls += 1
        if fail:
            raise TimeoutError("retry across minute")

    with observer.scope(logical_request_id="capture-cross-minute", observed_at=before_boundary):
        with pytest.raises(TimeoutError):
            observer.observe("stock_st", lambda: transport(fail=True))
        clock.now = after_boundary
        for ordinal in range(12):
            observer.observe(f"stock_basic:{ordinal}", transport)
        with pytest.raises(SourceQuotaExhaustedError):
            observer.observe("suspend_d", transport)

    assert transport_calls == 13
    assert observer.remaining(now=before_boundary) == 11
    assert observer.remaining(now=after_boundary) == 0


def test_new_revision_in_same_minute_cannot_exceed_shared_transport_quota(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 31, 9, 1, tzinfo=UTC)
    clock = _MutableClock(now)
    observer = QuotaBoundTransportObserver(
        path=tmp_path / "quota.sqlite3",
        source="tushare.reference_slow",
        quota_units_per_window=12,
        window_kind="minute",
        clock=clock,
    )
    transport_calls = 0

    def transport() -> None:
        nonlocal transport_calls
        transport_calls += 1

    with observer.scope(logical_request_id="revision-0", observed_at=now):
        for ordinal in range(12):
            observer.observe(f"reference:{ordinal}", transport)
    with (
        pytest.raises(SourceQuotaExhaustedError),
        observer.scope(logical_request_id="revision-1", observed_at=now),
    ):
        observer.observe("stock_st", transport)

    assert transport_calls == 12
    assert observer.remaining(now=now) == 0


def test_clock_rollback_clamps_to_latest_dispatch_window_without_extra_quota(
    tmp_path: Path,
) -> None:
    trusted = datetime(2026, 7, 31, 9, 1, tzinfo=UTC)
    rolled_back = trusted - timedelta(seconds=1)
    path = tmp_path / "quota.sqlite3"
    clock = _MutableClock(trusted)
    observer = QuotaBoundTransportObserver(
        path=path,
        source="tushare.reference_slow",
        quota_units_per_window=2,
        window_kind="minute",
        clock=clock,
    )
    transport_calls = 0

    def transport() -> None:
        nonlocal transport_calls
        transport_calls += 1

    with observer.scope(logical_request_id="revision-0", observed_at=trusted):
        observer.observe("stock_st", transport)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM quota_source_clock")
    clock.now = rolled_back
    restarted = QuotaBoundTransportObserver(
        path=path,
        source="tushare.reference_slow",
        quota_units_per_window=2,
        window_kind="minute",
        clock=clock,
    )
    with restarted.scope(logical_request_id="revision-1", observed_at=trusted):
        restarted.observe("stock_st", transport)
    with (
        pytest.raises(SourceQuotaExhaustedError),
        restarted.scope(logical_request_id="revision-2", observed_at=trusted),
    ):
        restarted.observe("stock_st", transport)

    attempts = restarted.request_attempts("revision-1")
    assert transport_calls == 2
    assert attempts[0].prepared_at == trusted
    assert attempts[0].dispatched_at == trusted
    assert attempts[0].clock_rollback_count >= 1
    assert restarted.remaining(now=trusted) == 0
    with pytest.raises(SourceQuotaExhaustedError, match="no active window"):
        restarted.remaining(now=rolled_back)


def test_dispatch_clock_is_sampled_after_waiting_for_sqlite_write_lock(tmp_path: Path) -> None:
    before_boundary = datetime(2026, 7, 31, 9, 0, 59, tzinfo=UTC)
    after_boundary = datetime(2026, 7, 31, 9, 1, tzinfo=UTC)
    path = tmp_path / "quota.sqlite3"
    clock_called = Event()
    worker_started = Event()

    class _LockAwareClock(_MutableClock):
        def __call__(self) -> datetime:
            clock_called.set()
            return self.now

    clock = _LockAwareClock(before_boundary)
    observer = QuotaBoundTransportObserver(
        path=path,
        source="tushare.reference_slow",
        quota_units_per_window=12,
        window_kind="minute",
        clock=clock,
    )
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")

    def dispatch() -> None:
        worker_started.set()
        with observer.scope(logical_request_id="lock-wait", observed_at=before_boundary):
            observer.observe("stock_st", lambda: None)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(dispatch)
            assert worker_started.wait(timeout=1)
            clock_called.wait(timeout=0.1)
            clock.now = after_boundary
            blocker.rollback()
            future.result(timeout=2)
    finally:
        blocker.close()

    (attempt,) = observer.request_attempts("lock-wait")
    assert attempt.prepared_at == after_boundary
    assert attempt.dispatched_at == after_boundary
    assert observer.remaining(now=after_boundary) == 11
    with pytest.raises(SourceQuotaExhaustedError, match="no active window"):
        observer.remaining(now=before_boundary)


def test_thirteen_concurrent_dispatches_crossing_lock_boundary_send_only_twelve(
    tmp_path: Path,
) -> None:
    before_boundary = datetime(2026, 7, 31, 9, 0, 59, tzinfo=UTC)
    after_boundary = datetime(2026, 7, 31, 9, 1, tzinfo=UTC)
    path = tmp_path / "quota.sqlite3"
    clock = _MutableClock(before_boundary)
    observer = QuotaBoundTransportObserver(
        path=path,
        source="tushare.reference_slow",
        quota_units_per_window=12,
        window_kind="minute",
        clock=clock,
    )
    barrier = Barrier(14)
    counter_lock = Lock()
    transport_calls = 0
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")

    def dispatch(ordinal: int) -> None:
        nonlocal transport_calls

        def transport() -> None:
            nonlocal transport_calls
            with counter_lock:
                transport_calls += 1

        barrier.wait(timeout=2)
        try:
            with observer.scope(
                logical_request_id=f"concurrent-{ordinal}",
                observed_at=before_boundary,
            ):
                observer.observe("stock_st", transport)
        except SourceQuotaExhaustedError:
            pass

    try:
        with ThreadPoolExecutor(max_workers=13) as executor:
            futures = [executor.submit(dispatch, ordinal) for ordinal in range(13)]
            barrier.wait(timeout=2)
            time.sleep(0.1)
            clock.now = after_boundary
            blocker.rollback()
            for future in futures:
                future.result(timeout=3)
    finally:
        blocker.close()

    assert transport_calls == 12
    assert observer.remaining(now=after_boundary) == 0
    with pytest.raises(SourceQuotaExhaustedError, match="no active window"):
        observer.remaining(now=before_boundary)
