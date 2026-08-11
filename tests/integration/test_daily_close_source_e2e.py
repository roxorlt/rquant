from __future__ import annotations

import multiprocessing as mp
from datetime import UTC, date, datetime
from pathlib import Path
from queue import Empty
from typing import Any

import pytest

from rquant.daily_close_gateway import (
    DailyCloseGateway,
    DailyCloseGatewayConfig,
    DailyCloseSourceRequest,
)
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool

TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)


def _snapshot() -> dict[str, object]:
    return {
        "daily_bar": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.4,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 9.95,
                "change": 0.25,
                "pct_chg": 2.5126,
                "vol": 1_000.0,
                "amount": 10_200.0,
            },
        ),
        "daily_basic": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "turnover_rate": 0.5,
                "volume_ratio": 1.2,
                "total_mv": 200_000.0,
                "circ_mv": 180_000.0,
            },
        ),
        "adj_factor": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "adj_factor": 1.01,
            },
        ),
        "index_daily": (
            {
                "ts_code": "000001.SH",
                "trade_date": TRADE_DATE,
                "open": 3200.0,
                "high": 3230.0,
                "low": 3190.0,
                "close": 3220.0,
                "pre_close": 3198.0,
                "change": 22.0,
                "pct_chg": 0.688,
                "vol": 2_000.0,
                "amount": 30_000.0,
            },
        ),
        "security_status": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "name": "浦发银行",
                "is_st": False,
                "listing_status": "L",
            },
        ),
        "suspension_status": (),
        "partial_datasets": (),
    }


def _gateway(root: Path, fetcher: Any) -> DailyCloseGateway:
    return DailyCloseGateway(
        spool=LiveBatchSpool(root),
        fetcher=fetcher,
        config=DailyCloseGatewayConfig(
            producer_version="daily-close-e2e-v1",
            producer_commit="c" * 40,
        ),
    )


def _concurrent_capture_worker(
    root: str,
    barrier: Any,
    fetch_count: Any,
    results: Any,
) -> None:
    def fetch(_request: DailyCloseSourceRequest) -> object:
        with fetch_count.get_lock():
            fetch_count.value += 1
        return _snapshot()

    try:
        gateway = _gateway(Path(root), fetch)
        barrier.wait(timeout=5)
        capture = gateway.capture_once(
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
        )
        results.put(
            {
                "batch_id": capture.batch_id,
                "sequence": capture.sequence,
                "published": capture.published,
                "error": None,
            }
        )
    except BaseException as exc:
        results.put(
            {
                "batch_id": None,
                "sequence": None,
                "published": None,
                "error": f"{type(exc).__name__}:{exc}",
            }
        )


def test_one_raw_generation_is_shareable_by_two_readonly_consumers(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path / "live", lambda _request: _snapshot())
    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    first_consumer = LiveBatchSpool(tmp_path / "live", read_only=True)
    second_consumer = LiveBatchSpool(tmp_path / "live", read_only=True)
    first_records = first_consumer.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)
    second_records = second_consumer.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)

    assert len(first_records) == len(second_records) == 1
    assert first_records[0].envelope.batch_id == second_records[0].envelope.batch_id
    assert first_records[0].envelope.batch_id == capture.batch_id
    assert first_consumer.read_payload(first_records[0]) == second_consumer.read_payload(
        second_records[0]
    )
    assert not list(tmp_path.rglob("*.duckdb"))


def test_two_concurrent_source_starts_share_one_remote_request(tmp_path: Path) -> None:
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(2)
    fetch_count = ctx.Value("i", 0)
    results = ctx.Queue()
    processes = [
        ctx.Process(
            target=_concurrent_capture_worker,
            args=(str(tmp_path / "live"), barrier, fetch_count, results),
        )
        for _ in range(2)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
            pytest.fail("daily-close capture worker timed out")

    observed: list[dict[str, object]] = []
    try:
        for _ in processes:
            observed.append(results.get(timeout=2))
    except Empty:
        pytest.fail("daily-close capture worker did not report a result")

    assert [process.exitcode for process in processes] == [0, 0]
    assert [item["error"] for item in observed] == [None, None]
    assert fetch_count.value == 1
    assert sorted(item["published"] for item in observed) == [False, True]
    assert len({item["batch_id"] for item in observed}) == 1
    assert {item["sequence"] for item in observed} == {0}


def test_restart_finishes_staged_raw_capture_without_repeating_remote_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal calls
        calls += 1
        return _snapshot()

    root = tmp_path / "live"
    gateway = _gateway(root, fetch)
    original_atomic_write = LiveBatchSpool._atomic_write

    class SimulatedHardExit(BaseException):
        pass

    def exit_after_manifest(path: Path, payload: bytes) -> None:
        original_atomic_write(path, payload)
        if path.parent.name == LiveChannel.DAILY_CLOSE.value and path.suffix == ".json":
            raise SimulatedHardExit

    monkeypatch.setattr(
        LiveBatchSpool,
        "_atomic_write",
        staticmethod(exit_after_manifest),
    )
    with pytest.raises(SimulatedHardExit):
        gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    monkeypatch.setattr(
        LiveBatchSpool,
        "_atomic_write",
        staticmethod(original_atomic_write),
    )

    restarted = _gateway(root, fetch)
    recovered = restarted.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
    )

    assert calls == 1
    assert recovered.published is True
    assert recovered.sequence == 0
    records = restarted.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)
    assert len(records) == 1
    assert restarted.decode_payload(restarted.spool.read_payload(records[0])).revision == 1
    assert not list((root / "publication-intents").glob("*.json"))
    assert not list((root / "capture-staging").glob("*.pending"))
