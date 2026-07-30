from __future__ import annotations

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
from rquant.source_quota_store import SourceQuotaStore

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
    fetcher: object,
    *,
    quota_store: SourceQuotaStore | None = None,
    quota_units_per_window: int | None = None,
) -> MarketMinuteGateway:
    return MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=fetcher,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
            quota_units_per_window=quota_units_per_window,
        ),
        quota_store=quota_store,
    )


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
    assert duplicate.pointer.sequence == 0
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


def test_gateway_publishes_explicit_stale_batch_on_source_failure(tmp_path: Path) -> None:
    def fail() -> pd.DataFrame:
        raise TimeoutError("source unavailable")

    gateway = _gateway(tmp_path, fail)
    capture = gateway.capture_once(received_at=RECEIVED)

    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    record = gateway.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    assert record.envelope.row_count == 0
    assert record.envelope.degraded_reasons == ("source_error:TimeoutError",)
    assert gateway.decode_payload(gateway.spool.read_payload(record)).empty


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
    assert latest.envelope.degraded_reasons == (
        "source_error:SourceQuotaExhaustedError",
    )
