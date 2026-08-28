from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import (
    MarketMinuteGateway,
    MarketMinuteGatewayConfig,
    MarketMinuteValidationError,
)
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)
from rquant.source_quota_transport import QuotaBoundTransportObserver

RECEIVED = datetime(2026, 7, 31, 1, 31, 5, tzinfo=UTC)


def _frame(*, minute: str = "2026-07-31 09:31:00", close: float = 10.1) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": minute,
                "open": 10.0,
                "high": max(10.2, close),
                "low": 9.9,
                "close": close,
                "vol": 100_000,
                "amount": 1_010_000.0,
            }
        ]
    )


def _gateway(
    tmp_path: Path,
    fetcher: Callable[[], pd.DataFrame],
    *,
    quota_store: SourceQuotaStore | None = None,
    quota_units_per_window: int | None = None,
    quota_cost_per_request: int = 1,
    pending_recovery_min_age_seconds: int = 60,
    completion_clock: Callable[[], datetime] | None = None,
    source_clock: Callable[[], datetime] = lambda: RECEIVED,
) -> MarketMinuteGateway:
    transport_observer = (
        None
        if quota_units_per_window is None or quota_store is None
        else QuotaBoundTransportObserver(
            store=quota_store,
            source="tushare.rt_min",
            quota_units_per_window=quota_units_per_window,
            window_kind="minute",
            clock=source_clock,
        )
    )
    resolved_fetcher = fetcher
    if transport_observer is not None:

        def resolved_fetcher() -> pd.DataFrame:
            return transport_observer.observe("rt_min", fetcher)

    return MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=resolved_fetcher,
        completion_clock=completion_clock,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
            quota_units_per_window=quota_units_per_window,
            quota_cost_per_request=quota_cost_per_request,
            pending_recovery_min_age_seconds=pending_recovery_min_age_seconds,
        ),
        quota_store=quota_store,
        transport_observer=transport_observer,
    )


def test_gateway_publishes_only_after_fetch_and_encoding_complete(tmp_path: Path) -> None:
    completed_at = RECEIVED + timedelta(seconds=4)
    gateway = _gateway(
        tmp_path,
        _frame,
        completion_clock=lambda: completed_at,
    )

    capture = gateway.capture_once(received_at=RECEIVED)

    record = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    assert record.envelope.received_at == RECEIVED
    assert record.envelope.available_at == completed_at
    assert capture.pointer.published_at == completed_at


def test_gateway_rejects_event_window_after_source_completion(tmp_path: Path) -> None:
    completed_at = RECEIVED + timedelta(seconds=4)
    gateway = _gateway(
        tmp_path,
        lambda: _frame(minute="2026-07-31 09:32:00"),
        completion_clock=lambda: completed_at,
    )

    with pytest.raises(MarketMinuteValidationError, match="future event window"):
        gateway.capture_once(received_at=RECEIVED)

    assert gateway.spool.current(LiveChannel.MARKET_MINUTE) is None


def test_gateway_fetches_once_and_publishes_normalized_parquet(tmp_path: Path) -> None:
    calls = 0

    def fetch() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame()

    gateway = _gateway(tmp_path, fetch)
    capture = gateway.capture_once(received_at=RECEIVED)

    assert calls == 1
    assert capture.published is True
    assert capture.pointer.sequence == 0
    assert capture.pointer.quality_status is BatchQualityStatus.PUBLISHED
    record = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    restored = gateway.decode_payload(gateway.spool.read_payload(record))
    assert list(restored["ts_code"]) == ["600000.SH"]
    assert restored.loc[0, "trade_time"].tzinfo is not None


def test_gateway_schema_dual_write_validates_before_publish_and_commits_after(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    events: list[str] = []

    class _DualWriter:
        def prepare_payload(self, values: object, *, observed_at: datetime) -> object:
            assert isinstance(values, dict)
            assert values["batch_id"]
            assert observed_at == RECEIVED
            events.append("prepare")
            return values

        def commit_payload(self, prepared: object, *, operation_id: str) -> object:
            assert prepared is not None
            assert operation_id.startswith("market-minute:")
            assert spool.current(LiveChannel.MARKET_MINUTE) is not None
            events.append("commit")
            return object()

    gateway = MarketMinuteGateway(
        spool=spool,
        fetcher=_frame,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
        schema_dual_writer=_DualWriter(),
    )

    gateway.capture_once(received_at=RECEIVED)

    assert events == ["prepare", "commit"]


def test_gateway_schema_dual_write_mismatch_fails_before_spool_publish(
    tmp_path: Path,
) -> None:
    class _RejectingDualWriter:
        def prepare_payload(self, _values: object, *, observed_at: datetime) -> object:
            assert observed_at == RECEIVED
            raise ValueError("shared field close differs")

        def commit_payload(self, _prepared: object, *, operation_id: str) -> object:
            raise AssertionError(f"must not commit {operation_id}")

    gateway = MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=_frame,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
        schema_dual_writer=_RejectingDualWriter(),
    )

    with pytest.raises(ValueError, match="shared field close"):
        gateway.capture_once(received_at=RECEIVED)

    assert gateway.spool.current(LiveChannel.MARKET_MINUTE) is None


def test_gateway_suppresses_exact_duplicate_but_revises_changed_minute(
    tmp_path: Path,
) -> None:
    frames = [_frame(), _frame(), _frame(close=10.2)]
    gateway = _gateway(tmp_path, lambda: frames.pop(0))

    first = gateway.capture_once(received_at=RECEIVED)
    duplicate = gateway.capture_once(received_at=RECEIVED + timedelta(seconds=5))
    revision = gateway.capture_once(received_at=RECEIVED + timedelta(seconds=10))

    assert first.published is True
    assert duplicate.published is False
    assert duplicate.pointer == first.pointer
    assert revision.pointer.sequence == 1
    records = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    assert [item.envelope.revision for item in records] == [1, 2]
    assert records[1].envelope.revises_batch_id == records[0].envelope.batch_id


def test_gateway_resets_revision_for_next_market_minute(tmp_path: Path) -> None:
    frames = [_frame(), _frame(minute="2026-07-31 09:32:00")]
    gateway = _gateway(tmp_path, lambda: frames.pop(0))

    gateway.capture_once(received_at=RECEIVED)
    gateway.capture_once(received_at=RECEIVED + timedelta(minutes=1))

    records = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    assert [item.envelope.revision for item in records] == [1, 1]
    assert records[1].envelope.revises_batch_id is None


def test_gateway_links_late_correction_to_same_minute_across_newer_batches(
    tmp_path: Path,
) -> None:
    frames = [
        _frame(minute="2026-07-31 09:31:00", close=10.1),
        _frame(minute="2026-07-31 09:32:00", close=10.2),
        _frame(minute="2026-07-31 09:31:00", close=10.3),
        _frame(minute="2026-07-31 09:31:00", close=10.4),
    ]
    gateway = _gateway(tmp_path, lambda: frames.pop(0))

    for offset in (0, 60, 65, 70):
        gateway.capture_once(received_at=RECEIVED + timedelta(seconds=offset))

    records = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    assert [record.envelope.revision for record in records] == [1, 1, 2, 3]
    assert records[2].envelope.revises_batch_id == records[0].envelope.batch_id
    assert records[3].envelope.revises_batch_id == records[2].envelope.batch_id
    assert records[2].envelope.available_at > records[2].envelope.event_time_end


def test_gateway_suppresses_late_duplicate_without_regressing_current_pointer(
    tmp_path: Path,
) -> None:
    frames = [
        _frame(minute="2026-07-31 09:31:00", close=10.1),
        _frame(minute="2026-07-31 09:32:00", close=10.2),
        _frame(minute="2026-07-31 09:31:00", close=10.1),
    ]
    gateway = _gateway(tmp_path, lambda: frames.pop(0))

    gateway.capture_once(received_at=RECEIVED)
    newer = gateway.capture_once(received_at=RECEIVED + timedelta(seconds=60))
    duplicate = gateway.capture_once(received_at=RECEIVED + timedelta(seconds=65))

    assert duplicate.published is False
    assert duplicate.pointer == newer.pointer
    assert gateway.spool.current(LiveChannel.MARKET_MINUTE) == newer.pointer
    assert len(gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)) == 2


def test_gateway_rebuilds_revision_index_only_once_per_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frames = [
        _frame(minute=f"2026-07-31 09:{31 + offset:02d}:00", close=10.1 + offset / 10)
        for offset in range(4)
    ]
    gateway = _gateway(tmp_path, lambda: frames.pop(0))
    original = gateway.spool.list_after
    scans = 0

    def counted_list_after(channel: LiveChannel, *, sequence: int):
        nonlocal scans
        scans += 1
        return original(channel, sequence=sequence)

    monkeypatch.setattr(gateway.spool, "list_after", counted_list_after)

    for offset in range(4):
        gateway.capture_once(received_at=RECEIVED + timedelta(minutes=offset))

    assert scans == 1


def test_gateway_publishes_explicit_stale_batch_on_source_failure(tmp_path: Path) -> None:
    def fail() -> pd.DataFrame:
        raise TimeoutError("source unavailable")

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fail,
        quota_store=quota,
        quota_units_per_window=1,
    )
    capture = gateway.capture_once(received_at=RECEIVED)

    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    record = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    assert record.envelope.row_count == 0
    assert record.envelope.degraded_reasons == ("source_error:TimeoutError",)
    assert gateway.decode_payload(gateway.spool.read_payload(record)).empty
    (attempt,) = quota.list_attempts(source="tushare.rt_min")
    assert attempt.outcome is SourceQuotaAttemptOutcome.FAILURE
    assert quota.remaining("tushare.rt_min", now=RECEIVED) == 0


def test_gateway_rejects_structurally_invalid_source_frame_without_publishing(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, lambda: pd.DataFrame([{"ts_code": "600000.SH"}]))

    with pytest.raises(MarketMinuteValidationError, match="missing columns"):
        gateway.capture_once(received_at=RECEIVED)
    assert gateway.spool.current(LiveChannel.MARKET_MINUTE) is None


def test_gateway_accounts_for_each_source_call_and_fails_stale_when_quota_exhausts(
    tmp_path: Path,
) -> None:
    calls = 0

    def fetch() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame()

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=2,
    )

    gateway.capture_once(received_at=RECEIVED)
    gateway.capture_once(received_at=RECEIVED + timedelta(seconds=5))
    exhausted = gateway.capture_once(received_at=RECEIVED + timedelta(seconds=10))

    assert calls == 2
    assert quota.remaining("tushare.rt_min", now=RECEIVED + timedelta(seconds=11)) == 0
    assert exhausted.pointer.quality_status is BatchQualityStatus.STALE
    latest = gateway.spool.list_after(
        LiveChannel.MARKET_MINUTE,
        sequence=exhausted.pointer.sequence - 1,
    )[0]
    assert latest.envelope.degraded_reasons == ("source_error:SourceQuotaExhaustedError",)


def test_gateway_charges_actual_capture_cost_below_configured_call_budget(
    tmp_path: Path,
) -> None:
    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        _frame,
        quota_store=quota,
        quota_units_per_window=2,
        quota_cost_per_request=2,
    )

    gateway.capture_once(received_at=RECEIVED, quota_cost_units=1)

    assert quota.remaining("tushare.rt_min", now=RECEIVED) == 1


def test_gateway_kill_leaves_durable_attempt_and_restart_does_not_refetch(
    tmp_path: Path,
) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    calls = 0

    def kill() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt()

    first = _gateway(
        tmp_path,
        kill,
        quota_store=SourceQuotaStore(
            quota_path,
            boot_id="boot-a",
            monotonic_ns=lambda: 0,
        ),
        quota_units_per_window=1,
    )
    with pytest.raises(KeyboardInterrupt):
        first.capture_once(received_at=RECEIVED)

    restarted = _gateway(
        tmp_path,
        lambda: pytest.fail("durable unknown attempt must not refetch"),
        quota_store=SourceQuotaStore(
            quota_path,
            boot_id="boot-a",
            monotonic_ns=lambda: 61_000_000_000,
        ),
        quota_units_per_window=1,
    )
    capture = restarted.capture_once(received_at=RECEIVED)

    assert calls == 1
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    quota = SourceQuotaStore(quota_path)
    assert quota.remaining("tushare.rt_min", now=RECEIVED) == 0
    (attempt,) = quota.list_attempts(source="tushare.rt_min")
    assert attempt.dispatched_at is not None
    assert attempt.outcome is SourceQuotaAttemptOutcome.UNKNOWN


def test_gateway_binding_failure_rolls_back_before_rt_min_transport(tmp_path: Path) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    quota = SourceQuotaStore(quota_path)
    with sqlite3.connect(quota_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_minute_transport_binding
            BEFORE INSERT ON quota_transport_attempt
            BEGIN
                SELECT RAISE(ABORT, 'injected binding failure');
            END
            """
        )
    transport_calls = 0

    def fetch() -> pd.DataFrame:
        nonlocal transport_calls
        transport_calls += 1
        return _frame()

    gateway = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=1,
    )

    capture = gateway.capture_once(received_at=RECEIVED)

    assert transport_calls == 0
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    assert quota.list_attempts(source="tushare.rt_min") == ()
    with pytest.raises(SourceQuotaExhaustedError, match="no active window"):
        quota.remaining("tushare.rt_min", now=RECEIVED)
