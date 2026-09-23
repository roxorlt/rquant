from __future__ import annotations

import hashlib
import multiprocessing as mp
import re
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from queue import Empty
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.auction_match_gateway import (
    AUCTION_MATCH_COLUMNS,
    AuctionMatchGateway,
    AuctionMatchGatewayConfig,
    AuctionMatchValidationError,
)
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)

TRADE_DATE = date(2026, 7, 31)
RECEIVED = datetime(2026, 7, 31, 1, 26, 5, tzinfo=UTC)
EXPECTED = ("000001.SZ", "600000.SH")


def _row(
    ts_code: str,
    *,
    trade_date: date | str = TRADE_DATE,
    price: object = 10.2,
    pre_close: object = 10.0,
    vol: object = 100_000.0,
    amount: object = 1_020_000.0,
    turnover_rate: object = 0.1,
    volume_ratio: object = 1.5,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "price": price,
        "vol": vol,
        "amount": amount,
        "pre_close": pre_close,
        "turnover_rate": turnover_rate,
        "volume_ratio": volume_ratio,
    }


def _frame(*codes: str) -> pd.DataFrame:
    return pd.DataFrame([_row(code) for code in codes])


def _gateway(
    tmp_path: Path,
    fetcher: object,
    *,
    min_coverage_ratio: float = 0.95,
    quota_store: SourceQuotaStore | None = None,
    quota_units_per_window: int | None = None,
    dispatch_clock: Callable[[], datetime] = lambda: RECEIVED,
) -> AuctionMatchGateway:
    return AuctionMatchGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=fetcher,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-v1",
            producer_commit="a" * 40,
            min_coverage_ratio=min_coverage_ratio,
            quota_units_per_window=quota_units_per_window,
        ),
        quota_store=quota_store,
        dispatch_clock=(None if quota_units_per_window is None else dispatch_clock),
    )


def _records(gateway: AuctionMatchGateway):
    return gateway.spool.list_after(LiveChannel.AUCTION_MATCH, sequence=-1)


def _concurrent_capture_worker(
    spool_root: str,
    quota_path: str,
    barrier: Any,
    fetch_count: Any,
    results: Any,
) -> None:
    def fetch(_: date) -> pd.DataFrame:
        with fetch_count.get_lock():
            fetch_count.value += 1
        time.sleep(0.25)
        return _frame(*EXPECTED)

    try:
        gateway = AuctionMatchGateway(
            spool=LiveBatchSpool(Path(spool_root)),
            fetcher=fetch,
            config=AuctionMatchGatewayConfig(
                producer_version="auction-match-v1",
                producer_commit="a" * 40,
                quota_units_per_window=2,
            ),
            quota_store=SourceQuotaStore(Path(quota_path)),
            dispatch_clock=lambda: RECEIVED,
        )
        barrier.wait(timeout=5)
        capture = gateway.capture_once(
            trade_date=TRADE_DATE,
            received_at=RECEIVED,
            expected_codes=EXPECTED,
        )
        results.put(
            {
                "published": capture.published,
                "batch_id": capture.pointer.batch_id,
                "sequence": capture.pointer.sequence,
                "error": None,
            }
        )
    except BaseException as exc:
        results.put(
            {
                "published": None,
                "batch_id": None,
                "sequence": None,
                "error": f"{type(exc).__name__}:{exc}",
            }
        )


class _PausedBeforeLockGateway(AuctionMatchGateway):
    def __init__(
        self,
        *,
        entered_lock: Any,
        release_lock: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._entered_lock = entered_lock
        self._release_lock = release_lock

    @contextmanager
    def _capture_lock(self) -> Iterator[bool]:
        self._entered_lock.set()
        if not self._release_lock.wait(timeout=5):
            raise TimeoutError("capture lock release was not signalled")
        with super()._capture_lock() as waited:
            yield waited


def _delayed_lock_capture_worker(
    spool_root: str,
    quota_path: str,
    entered_lock: Any,
    release_lock: Any,
    fetch_count: Any,
    results: Any,
) -> None:
    def fetch(_: date) -> pd.DataFrame:
        with fetch_count.get_lock():
            fetch_count.value += 1
        return _frame(*EXPECTED)

    try:
        gateway = _PausedBeforeLockGateway(
            spool=LiveBatchSpool(Path(spool_root)),
            fetcher=fetch,
            config=AuctionMatchGatewayConfig(
                producer_version="auction-match-v1",
                producer_commit="a" * 40,
                quota_units_per_window=2,
            ),
            quota_store=SourceQuotaStore(Path(quota_path)),
            dispatch_clock=lambda: RECEIVED + timedelta(seconds=5),
            entered_lock=entered_lock,
            release_lock=release_lock,
        )
        capture = gateway.capture_once(
            trade_date=TRADE_DATE,
            received_at=RECEIVED + timedelta(seconds=5),
            expected_codes=EXPECTED,
        )
        results.put(
            {
                "published": capture.published,
                "batch_id": capture.pointer.batch_id,
                "sequence": capture.pointer.sequence,
                "error": None,
            }
        )
    except BaseException as exc:
        results.put(
            {
                "published": None,
                "batch_id": None,
                "sequence": None,
                "error": f"{type(exc).__name__}:{exc}",
            }
        )


@pytest.mark.parametrize("preinitialize_spool", [False, True], ids=["cold", "initialized"])
def test_concurrent_identical_capture_is_single_fetch_and_single_publish(
    tmp_path: Path,
    preinitialize_spool: bool,
) -> None:
    ctx = mp.get_context("spawn")
    spool_root = tmp_path / "live"
    quota_path = tmp_path / "quota.sqlite3"
    SourceQuotaStore(quota_path)
    if preinitialize_spool:
        LiveBatchSpool(spool_root).source_descriptor(LiveChannel.AUCTION_MATCH)

    barrier = ctx.Barrier(2)
    fetch_count = ctx.Value("i", 0)
    results = ctx.Queue()
    processes = [
        ctx.Process(
            target=_concurrent_capture_worker,
            args=(
                str(spool_root),
                str(quota_path),
                barrier,
                fetch_count,
                results,
            ),
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
            pytest.fail("concurrent capture worker timed out")

    observed: list[dict[str, object]] = []
    try:
        for _ in processes:
            observed.append(results.get(timeout=2))
    except Empty:
        pytest.fail("concurrent capture worker did not report a result")

    assert [process.exitcode for process in processes] == [0, 0]
    assert [item["error"] for item in observed] == [None, None]
    assert fetch_count.value == 1
    assert sorted(item["published"] for item in observed) == [False, True]
    assert len({item["batch_id"] for item in observed}) == 1
    assert {item["sequence"] for item in observed} == {0}
    assert (
        SourceQuotaStore(quota_path).remaining(
            "tushare.stk_auction",
            now=RECEIVED,
        )
        == 1
    )

    records = LiveBatchSpool(spool_root).list_after(
        LiveChannel.AUCTION_MATCH,
        sequence=-1,
    )
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.PUBLISHED


def test_call_started_before_publish_skips_fetch_even_if_lock_was_uncontended(
    tmp_path: Path,
) -> None:
    ctx = mp.get_context("spawn")
    spool_root = tmp_path / "live"
    quota_path = tmp_path / "quota.sqlite3"
    quota = SourceQuotaStore(quota_path)
    entered_lock = ctx.Event()
    release_lock = ctx.Event()
    fetch_count = ctx.Value("i", 0)
    results = ctx.Queue()
    delayed = ctx.Process(
        target=_delayed_lock_capture_worker,
        args=(
            str(spool_root),
            str(quota_path),
            entered_lock,
            release_lock,
            fetch_count,
            results,
        ),
    )
    delayed.start()
    assert entered_lock.wait(timeout=5), "delayed capture did not reach the lock boundary"

    def fetch(_: date) -> pd.DataFrame:
        with fetch_count.get_lock():
            fetch_count.value += 1
        return _frame(*EXPECTED)

    first = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=2,
    ).capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )
    release_lock.set()
    delayed.join(timeout=10)
    if delayed.is_alive():
        delayed.terminate()
        delayed.join(timeout=2)
        pytest.fail("delayed capture worker timed out")

    try:
        second = results.get(timeout=2)
    except Empty:
        pytest.fail("delayed capture worker did not report a result")

    assert delayed.exitcode == 0
    assert second["error"] is None
    assert first.published is True
    assert second["published"] is False
    assert second["batch_id"] == first.pointer.batch_id
    assert second["sequence"] == first.pointer.sequence == 0
    assert fetch_count.value == 1
    assert (
        quota.remaining(
            "tushare.stk_auction",
            now=RECEIVED + timedelta(seconds=5),
        )
        == 1
    )


def test_config_is_frozen_and_validates_coverage_and_quota_contract(
    tmp_path: Path,
) -> None:
    config = AuctionMatchGatewayConfig(
        producer_version="v1",
        producer_commit="a" * 40,
    )

    assert config.source == "tushare.stk_auction"
    assert config.dataset_id == "auction_match"
    assert config.min_coverage_ratio == 0.95
    with pytest.raises(ValidationError):
        config.min_coverage_ratio = 0.5
    for invalid in (0, -0.1, 1.01):
        with pytest.raises(ValidationError):
            AuctionMatchGatewayConfig(
                producer_version="v1",
                producer_commit="a" * 40,
                min_coverage_ratio=invalid,
            )
    with pytest.raises(ValueError, match="quota_store"):
        _gateway(
            tmp_path,
            lambda _: _frame(*EXPECTED),
            quota_units_per_window=1,
        )


def test_full_coverage_publishes_canonical_parquet_and_restores_pre_close(
    tmp_path: Path,
) -> None:
    calls: list[date] = []

    def fetch(trade_date: date) -> pd.DataFrame:
        calls.append(trade_date)
        return _frame("600000.SH", "000001.SZ")

    gateway = _gateway(tmp_path, fetch)
    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=("600000.SH", "000001.SZ", "600000.SH"),
        required_codes=("000001.SZ",),
    )

    assert calls == [TRADE_DATE]
    assert capture.published is True
    assert capture.expected_count == 2
    assert capture.observed_count == 2
    assert capture.coverage_ratio == 1.0
    assert capture.missing_required_codes == ()
    assert capture.pointer.sequence == 0
    assert capture.pointer.revision == 1
    assert capture.pointer.quality_status is BatchQualityStatus.PUBLISHED

    record = _records(gateway)[0]
    restored = gateway.decode_payload(gateway.spool.read_payload(record))
    assert tuple(restored.columns) == AUCTION_MATCH_COLUMNS
    assert restored["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert restored["pre_close"].tolist() == [10.0, 10.0]
    assert restored["trade_date"].tolist() == [TRADE_DATE, TRADE_DATE]
    assert record.envelope.row_count == 2
    assert (
        record.envelope.content_sha256
        == hashlib.sha256(gateway.spool.read_payload(record)).hexdigest()
    )
    assert record.envelope.event_time_start == datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
    assert record.envelope.event_time_end == datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
    assert record.envelope.source_time == datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
    assert record.envelope.received_at == RECEIVED
    assert record.envelope.available_at == RECEIVED
    assert record.envelope.producer_commit == "a" * 40


def test_filters_extra_stock_and_etf_outside_expected_universe(tmp_path: Path) -> None:
    gateway = _gateway(
        tmp_path,
        lambda _: _frame("600000.SH", "000001.SZ", "510300.SH", "300001.SZ"),
    )

    gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    record = _records(gateway)[0]
    restored = gateway.decode_payload(gateway.spool.read_payload(record))
    assert restored["ts_code"].tolist() == ["000001.SZ", "600000.SH"]


def test_rejects_capture_before_0926_without_calling_or_publishing(tmp_path: Path) -> None:
    calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame(*EXPECTED)

    gateway = _gateway(tmp_path, fetch)
    with pytest.raises(AuctionMatchValidationError, match="09:26"):
        gateway.capture_once(
            trade_date=TRADE_DATE,
            received_at=datetime(2026, 7, 31, 1, 25, 59, tzinfo=UTC),
            expected_codes=EXPECTED,
        )

    assert calls == 0
    assert gateway.spool.current(LiveChannel.AUCTION_MATCH) is None


@pytest.mark.parametrize("unsafe_kind", ["symlink", "broad_mode", "hardlink"])
def test_capture_lock_rejects_unsafe_files_before_fetch(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame(*EXPECTED)

    gateway = _gateway(tmp_path, fetch)
    lock_path = gateway.spool.root / ".auction_match.capture.lock"
    target = tmp_path / "unsafe-lock-target"
    target.write_text("unsafe", encoding="utf-8")
    if unsafe_kind == "symlink":
        lock_path.symlink_to(target)
    elif unsafe_kind == "broad_mode":
        lock_path.write_text("unsafe", encoding="utf-8")
        lock_path.chmod(0o644)
    else:
        lock_path.hardlink_to(target)

    with pytest.raises(AuctionMatchValidationError, match="capture lock"):
        gateway.capture_once(
            trade_date=TRADE_DATE,
            received_at=RECEIVED,
            expected_codes=EXPECTED,
        )

    assert calls == 0
    assert gateway.spool.current(LiveChannel.AUCTION_MATCH) is None


def test_capture_lock_revalidates_path_after_waiting_for_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import auction_match_gateway as gateway_module

    calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame(*EXPECTED)

    gateway = _gateway(tmp_path, fetch)
    lock_path = gateway.spool.root / ".auction_match.capture.lock"
    phase = 0

    def replace_while_waiting(_descriptor: int, operation: int) -> None:
        nonlocal phase
        if phase == 0 and operation == gateway_module.fcntl.LOCK_EX | gateway_module.fcntl.LOCK_NB:
            phase = 1
            raise BlockingIOError
        if phase == 1 and operation == gateway_module.fcntl.LOCK_EX:
            lock_path.unlink()
            lock_path.write_bytes(b"replacement")
            lock_path.chmod(0o600)
            phase = 2

    monkeypatch.setattr(gateway_module.fcntl, "flock", replace_while_waiting)

    with pytest.raises(AuctionMatchValidationError, match="capture lock"):
        gateway.capture_once(
            trade_date=TRADE_DATE,
            received_at=RECEIVED,
            expected_codes=EXPECTED,
        )

    assert phase == 2
    assert calls == 0


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (pd.DataFrame([{"ts_code": "600000.SH"}]), "missing columns"),
        (pd.DataFrame([_row("600000.SH", trade_date=date(2026, 7, 30))]), "trade_date"),
        (pd.DataFrame([_row("BAD")]), "ts_code"),
        (pd.DataFrame([_row("600000.SH"), _row("600000.SH")]), "duplicate"),
        (pd.DataFrame([_row("600000.SH", price=True)]), "bool"),
        #: 必填列里的 ±inf / NaN 不在这张表里：那是**丢行**，见
        #: `test_non_finite_required_rows_are_dropped_and_the_batch_still_publishes`。
        #: 可选列里的 ±inf 仍然整批拒——可选列允许缺，不允许是个无穷大。
        (pd.DataFrame([_row("600000.SH", volume_ratio=np.inf)]), "finite"),
        (pd.DataFrame([_row("600000.SH", vol=-1)]), "nonnegative"),
        (pd.DataFrame([_row("600000.SH", price=0)]), "positive"),
        (pd.DataFrame([_row("600000.SH", pre_close=-1)]), "positive"),
        (pd.DataFrame([_row("600000.SH", turnover_rate=-0.1)]), "nonnegative"),
    ],
)
def test_structural_errors_publish_an_empty_degraded_batch_that_names_them(
    tmp_path: Path,
    raw: pd.DataFrame,
    message: str,
) -> None:
    """读不懂的报文是一条可见的降级，不是一次抛出（#277）。

    改动前这一句在 `try` 之外：上游给了一份读不懂的东西，异常穿过 `capture_once`，既不发
    批次也不留回执，而构建器的早退分支随后把心跳洗成正常——2026-09-21 现场的形状。现在
    发一个 **零行** 的 DEGRADED 批次，`degraded_reasons` 里带 `validation_failed:` 加原话，
    所以「今天的报文不合法」这件事在心跳上看得见。一行不合法的数据都没有进 payload，
    这才是 fail closed 的本意；`normalize_frame` 自己照旧抛，见下一条用例。
    """

    gateway = _gateway(tmp_path, lambda _: raw)

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=("600000.SH",),
    )

    assert capture.published is True
    assert capture.observed_count == 0
    assert capture.pointer.quality_status is BatchQualityStatus.DEGRADED
    envelope = _records(gateway)[0].envelope
    assert envelope.row_count == 0
    assert len(envelope.degraded_reasons) == 1
    reason = envelope.degraded_reasons[0]
    assert reason.startswith("validation_failed:")
    assert re.search(message, reason), reason
    #: 空结果那一条降级不该同时挂上来：这次不是「返回空」，是「返回了读不懂的东西」
    assert "empty_source_result" not in envelope.degraded_reasons
    assert gateway.decode_payload(gateway.spool.read_payload(_records(gateway)[0])).empty


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (pd.DataFrame([{"ts_code": "600000.SH"}]), "missing columns"),
        (pd.DataFrame([_row("600000.SH", trade_date=date(2026, 7, 30))]), "trade_date"),
        (pd.DataFrame([_row("BAD")]), "ts_code"),
        (pd.DataFrame([_row("600000.SH"), _row("600000.SH")]), "duplicate"),
        (pd.DataFrame([_row("600000.SH", price=True)]), "bool"),
        (pd.DataFrame([_row("600000.SH", volume_ratio=np.inf)]), "finite"),
        (pd.DataFrame([_row("600000.SH", vol=-1)]), "nonnegative"),
        (pd.DataFrame([_row("600000.SH", price=0)]), "positive"),
    ],
)
def test_normalize_frame_itself_still_refuses_every_structural_error(
    raw: pd.DataFrame,
    message: str,
) -> None:
    """降级的是 `_capture_locked` 那一层，校验本身一条都没有放宽。"""

    with pytest.raises(AuctionMatchValidationError, match=message):
        AuctionMatchGateway.normalize_frame(
            raw,
            trade_date=TRADE_DATE,
            expected_codes=("600000.SH",),
        )


def test_a_column_less_empty_frame_is_empty_rather_than_missing_columns() -> None:
    """09:26 那三次「返回空」的形状：零行、零列，是空，不是缺八个字段。"""

    normalized = AuctionMatchGateway.normalize_frame(
        pd.DataFrame(),
        trade_date=TRADE_DATE,
        expected_codes=("600000.SH",),
    )

    assert normalized.empty
    assert tuple(normalized.columns) == AUCTION_MATCH_COLUMNS


def test_an_empty_frame_still_refuses_an_invalid_expected_universe() -> None:
    """空表走的是捷径，但 `expected_codes` 的校验排在捷径之前，没有被跳过。"""

    with pytest.raises(AuctionMatchValidationError, match="expected_codes"):
        AuctionMatchGateway.normalize_frame(
            pd.DataFrame(),
            trade_date=TRADE_DATE,
            expected_codes=(),
        )


def test_optional_metrics_allow_nan_and_round_trip(tmp_path: Path) -> None:
    raw = pd.DataFrame([_row("600000.SH", turnover_rate=np.nan, volume_ratio=None)])
    gateway = _gateway(tmp_path, lambda _: raw)

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=("600000.SH",),
    )

    assert capture.pointer.quality_status is BatchQualityStatus.PUBLISHED
    restored = gateway.decode_payload(gateway.spool.read_payload(_records(gateway)[0]))
    assert pd.isna(restored.loc[0, "turnover_rate"])
    assert pd.isna(restored.loc[0, "volume_ratio"])


def _no_match_row(ts_code: str) -> dict[str, object]:
    """2026-09-23 生产现场里「今天没有集合竞价成交」的那种一行。

    当天 `stk_auction(20260923)` 的 6,077 行里有 407 行长这样：`price` 是 NaN，同一行的
    `vol` 与 `amount` 都是 0，`pre_close` 反而是有数的（例：`600289.SH price=NaN vol=0
    amount=0 pre_close=4.42 turnover_rate=0.0 volume_ratio=0.0`）。整张表因此被判
    `required numeric values must be finite`，批次发成零行 DEGRADED。
    """

    return _row(
        ts_code,
        price=np.nan,
        vol=0.0,
        amount=0.0,
        pre_close=4.42,
        turnover_rate=0.0,
        volume_ratio=0.0,
    )


def test_non_finite_required_rows_are_dropped_and_the_batch_still_publishes(
    tmp_path: Path,
) -> None:
    """必填数值不是有限数的那些行丢掉，剩下的照常发布（2026-09-23 现场）。

    三行被丢：一行 `price` 是 NaN（当天真实形状），一行 `pre_close` 是 NaN，一行 `amount`
    是 +inf。三行的代码都不在竞价全集里——停牌到今天的票昨天没有日线，而全集正是
    「昨天有日线的代码」。于是覆盖率不受影响，批次是 PUBLISHED，下游
    `candidate.auction_gap` 读得到它。
    """

    raw = pd.DataFrame(
        [
            _row("000001.SZ"),
            _row("600000.SH"),
            _no_match_row("600289.SH"),
            _row("300001.SZ", pre_close=np.nan),
            _row("002001.SZ", amount=np.inf),
        ]
    )
    gateway = _gateway(tmp_path, lambda _: raw)

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert capture.rows_dropped_non_finite == 3
    assert capture.observed_count == 2
    assert capture.coverage_ratio == 1.0
    assert capture.pointer.quality_status is BatchQualityStatus.PUBLISHED
    envelope = _records(gateway)[0].envelope
    assert envelope.row_count == 2
    #: PUBLISHED 的信封不许带 degraded_reasons（`BatchEnvelope` 的不变量），所以丢行这件事
    #: 记在采集结果上、由心跳那一层说出来，见 test_auction_match_runtime_source.py
    assert envelope.degraded_reasons == ()
    restored = gateway.decode_payload(gateway.spool.read_payload(_records(gateway)[0]))
    assert tuple(restored["ts_code"].astype(str)) == EXPECTED


def test_a_dropped_row_inside_the_universe_costs_coverage(tmp_path: Path) -> None:
    """丢的那一行若在全集里，覆盖率照跌——覆盖率那条规则一个字都没有放宽。

    生产上这正是要盯的数：全集是「昨天有日线的代码」，今天停牌的票仍在全集里。全集里被丢
    掉的行数超过 `1 - min_coverage_ratio`（默认 0.95，即 5%）时，批次是 DEGRADED，
    `candidate.auction_gap` 照旧读不到 PUBLISHED 批次。
    """

    raw = pd.DataFrame([_row("000001.SZ"), _no_match_row("600000.SH")])
    gateway = _gateway(tmp_path, lambda _: raw)

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert capture.rows_dropped_non_finite == 1
    assert capture.observed_count == 1
    assert capture.coverage_ratio == 0.5
    assert capture.pointer.quality_status is BatchQualityStatus.DEGRADED
    assert _records(gateway)[0].envelope.degraded_reasons == ("coverage_below_minimum",)


def test_a_frame_where_every_row_is_non_finite_is_degraded_not_an_exception(
    tmp_path: Path,
) -> None:
    """全是 NaN 的一张表走覆盖率那条路降级，不抛异常、不静默。"""

    raw = pd.DataFrame([_no_match_row(code) for code in EXPECTED])
    gateway = _gateway(tmp_path, lambda _: raw)

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert capture.rows_dropped_non_finite == 2
    assert capture.observed_count == 0
    assert capture.pointer.quality_status is BatchQualityStatus.DEGRADED
    reasons = _records(gateway)[0].envelope.degraded_reasons
    assert "coverage_below_minimum" in reasons
    assert not any(reason.startswith("validation_failed:") for reason in reasons)
    assert _records(gateway)[0].envelope.row_count == 0


def test_normalize_capture_counts_the_rows_it_dropped() -> None:
    """规范化那一层自己就把「丢了几行」交出来，覆盖率之外不改任何判据。"""

    raw = pd.DataFrame(
        [
            _row("600000.SH"),
            _no_match_row("000001.SZ"),
            _row("300001.SZ", vol=np.inf),
        ]
    )

    normalized = AuctionMatchGateway.normalize_capture(
        raw,
        trade_date=TRADE_DATE,
        expected_codes=("600000.SH", "000001.SZ", "300001.SZ"),
    )

    assert normalized.rows_dropped_non_finite == 2
    assert tuple(normalized.frame["ts_code"].astype(str)) == ("600000.SH",)
    #: 只要表的老写法照旧
    assert AuctionMatchGateway.normalize_frame(
        raw,
        trade_date=TRADE_DATE,
        expected_codes=("600000.SH", "000001.SZ", "300001.SZ"),
    ).equals(normalized.frame)


def test_low_coverage_and_missing_required_are_degraded(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, lambda _: _frame("600000.SH"))

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
        required_codes=("000001.SZ",),
    )

    assert capture.observed_count == 1
    assert capture.coverage_ratio == 0.5
    assert capture.missing_required_codes == ("000001.SZ",)
    assert capture.pointer.quality_status is BatchQualityStatus.DEGRADED
    assert _records(gateway)[0].envelope.degraded_reasons == (
        "coverage_below_minimum",
        "required_codes_missing",
    )


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (pd.DataFrame(columns=AUCTION_MATCH_COLUMNS), "empty_source_result"),
        #: 09:26 现场那一张：零行**零列**。改动前它走的是「缺八个字段」那条抛出的路，
        #: 一个批次都留不下来（#277）
        (pd.DataFrame(), "empty_source_result"),
        (_frame("300001.SZ"), "expected_universe_no_match"),
    ],
)
def test_successful_empty_result_is_degraded_with_clear_reason(
    tmp_path: Path,
    raw: pd.DataFrame,
    reason: str,
) -> None:
    gateway = _gateway(tmp_path, lambda _: raw)

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert capture.observed_count == 0
    assert capture.coverage_ratio == 0.0
    assert capture.pointer.quality_status is BatchQualityStatus.DEGRADED
    assert _records(gateway)[0].envelope.degraded_reasons == (
        reason,
        "coverage_below_minimum",
    )


def test_source_error_publishes_stale_empty_batch(tmp_path: Path) -> None:
    def fail(_: date) -> pd.DataFrame:
        raise TimeoutError("unavailable")

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fail,
        quota_store=quota,
        quota_units_per_window=1,
    )
    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
        required_codes=("000001.SZ",),
    )

    assert capture.observed_count == 0
    assert capture.missing_required_codes == ("000001.SZ",)
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    record = _records(gateway)[0]
    assert record.envelope.degraded_reasons == ("source_error:TimeoutError",)
    assert gateway.decode_payload(gateway.spool.read_payload(record)).empty
    (attempt,) = quota.list_attempts(source="tushare.stk_auction")
    assert attempt.outcome is SourceQuotaAttemptOutcome.FAILURE
    assert quota.remaining("tushare.stk_auction", now=RECEIVED) == 0


def test_fetcher_validation_error_is_a_source_error_not_a_structural_error(
    tmp_path: Path,
) -> None:
    def fail(_: date) -> pd.DataFrame:
        raise AuctionMatchValidationError("upstream happened to use the same class")

    gateway = _gateway(tmp_path, fail)
    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    assert _records(gateway)[0].envelope.degraded_reasons == (
        "source_error:AuctionMatchValidationError",
    )


def test_duplicate_suppression_same_day_revision_and_next_day_reset(
    tmp_path: Path,
) -> None:
    next_date = date(2026, 8, 3)
    frames = [
        _frame(*EXPECTED),
        pd.DataFrame([_row("000001.SZ"), _row("600000.SH", price=10.3)]),
        pd.DataFrame(
            [
                _row("000001.SZ", trade_date=next_date),
                _row("600000.SH", trade_date=next_date),
            ]
        ),
    ]
    gateway = _gateway(tmp_path, lambda _: frames.pop(0))

    first = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )
    duplicate = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=5),
        expected_codes=EXPECTED,
    )
    revision = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=10),
        expected_codes=EXPECTED,
        retry_ordinal=1,
    )
    following = gateway.capture_once(
        trade_date=next_date,
        received_at=datetime(2026, 8, 3, 1, 26, 5, tzinfo=UTC),
        expected_codes=EXPECTED,
    )

    assert first.published is True
    assert duplicate.published is False
    assert duplicate.pointer.sequence == 0
    assert revision.pointer.sequence == 1
    assert following.pointer.sequence == 2
    records = _records(gateway)
    assert [record.envelope.revision for record in records] == [1, 2, 1]
    assert records[1].envelope.revises_batch_id == records[0].envelope.batch_id
    assert records[2].envelope.revises_batch_id is None


def test_request_universe_change_is_a_same_day_revision(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, lambda _: _frame(*EXPECTED))

    gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=("600000.SH",),
    )
    changed = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=5),
        expected_codes=EXPECTED,
    )

    records = _records(gateway)
    assert changed.published is True
    assert records[0].envelope.source_request_id != records[1].envelope.source_request_id
    assert records[1].envelope.revision == 2


def test_request_id_is_stable_across_received_times(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, lambda _: _frame(*EXPECTED))
    first = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )
    first_record = _records(gateway)[0]
    duplicate = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=5),
        expected_codes=EXPECTED,
    )

    assert duplicate.published is False
    assert first.pointer == duplicate.pointer
    assert (
        _records(gateway)[0].envelope.source_request_id == first_record.envelope.source_request_id
    )


def test_invalid_universe_rejected_before_fetch(tmp_path: Path) -> None:
    calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame(*EXPECTED)

    gateway = _gateway(tmp_path, fetch)
    invalid_cases = [
        ((), ()),
        (("600000",), ()),
        (("600000.SH",), ("000001.SZ",)),
    ]
    for expected, required in invalid_cases:
        with pytest.raises(AuctionMatchValidationError):
            gateway.capture_once(
                trade_date=TRADE_DATE,
                received_at=RECEIVED,
                expected_codes=expected,
                required_codes=required,
            )

    assert calls == 0


def test_quota_consumes_each_source_attempt_and_exhaustion_publishes_stale(
    tmp_path: Path,
) -> None:
    calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _frame(*EXPECTED)

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=2,
    )

    gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )
    gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=5),
        expected_codes=EXPECTED,
        retry_ordinal=1,
    )
    exhausted = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=10),
        expected_codes=EXPECTED,
        retry_ordinal=2,
    )

    assert calls == 2
    assert (
        quota.remaining(
            "tushare.stk_auction",
            now=RECEIVED + timedelta(seconds=11),
        )
        == 0
    )
    assert exhausted.pointer.quality_status is BatchQualityStatus.STALE
    assert _records(gateway)[-1].envelope.degraded_reasons == (
        "source_error:SourceQuotaExhaustedError",
    )


def test_source_exception_still_consumes_acquired_quota(tmp_path: Path) -> None:
    def fail(_: date) -> pd.DataFrame:
        raise ConnectionError("down")

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fail,
        quota_store=quota,
        quota_units_per_window=1,
    )

    gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert quota.remaining("tushare.stk_auction", now=RECEIVED) == 0


def test_transport_binding_failure_rolls_back_claim_before_provider_dispatch(
    tmp_path: Path,
) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    quota = SourceQuotaStore(quota_path)
    with sqlite3.connect(quota_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_auction_transport_binding
            BEFORE INSERT ON quota_transport_attempt
            BEGIN
                SELECT RAISE(ABORT, 'injected binding failure');
            END
            """
        )
    provider_calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal provider_calls
        provider_calls += 1
        return _frame(*EXPECTED)

    gateway = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=1,
    )

    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )

    assert provider_calls == 0
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    assert quota.list_attempts(source="tushare.stk_auction") == ()
    with pytest.raises(SourceQuotaExhaustedError, match="no active window"):
        quota.remaining("tushare.stk_auction", now=RECEIVED)


def test_killed_provider_attempt_is_durable_and_restart_does_not_refetch(tmp_path: Path) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    calls = 0

    def kill(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt()

    first = _gateway(
        tmp_path,
        kill,
        quota_store=SourceQuotaStore(quota_path),
        quota_units_per_window=1,
    )
    with pytest.raises(KeyboardInterrupt):
        first.capture_once(
            trade_date=TRADE_DATE,
            received_at=RECEIVED,
            expected_codes=EXPECTED,
        )

    restarted = _gateway(
        tmp_path,
        lambda _date: pytest.fail("durable unknown attempt must not refetch"),
        quota_store=SourceQuotaStore(quota_path),
        quota_units_per_window=1,
    )
    capture = restarted.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED + timedelta(seconds=5),
        expected_codes=EXPECTED,
    )

    assert calls == 1
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    quota = SourceQuotaStore(quota_path)
    assert quota.remaining("tushare.stk_auction", now=RECEIVED) == 0
    (attempt,) = quota.list_attempts(source="tushare.stk_auction")
    assert attempt.dispatched_at is not None
    assert attempt.outcome is SourceQuotaAttemptOutcome.UNKNOWN


@pytest.mark.parametrize("first_quality", ["stale", "degraded"])
def test_same_received_at_retries_nonpublished_quality_and_charges_each_fetch(
    tmp_path: Path,
    first_quality: str,
) -> None:
    calls = 0

    def fetch(_: date) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        if calls == 1:
            if first_quality == "stale":
                raise TimeoutError("retryable")
            return _frame("600000.SH")
        return _frame(*EXPECTED)

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=2,
    )

    first = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
    )
    retry = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=RECEIVED,
        expected_codes=EXPECTED,
        retry_ordinal=1,
    )

    assert first.pointer.quality_status in {
        BatchQualityStatus.STALE,
        BatchQualityStatus.DEGRADED,
    }
    assert retry.published is True
    assert retry.pointer.quality_status is BatchQualityStatus.PUBLISHED
    assert retry.pointer.revision == 2
    assert calls == 2
    assert quota.remaining("tushare.stk_auction", now=RECEIVED) == 0


def test_payload_encoding_is_deterministic_after_canonical_normalization(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, lambda _: _frame(*EXPECTED))
    first = gateway.normalize_frame(
        _frame("600000.SH", "000001.SZ"),
        trade_date=TRADE_DATE,
        expected_codes=EXPECTED,
    )
    second = gateway.normalize_frame(
        _frame("000001.SZ", "600000.SH"),
        trade_date=TRADE_DATE,
        expected_codes=EXPECTED,
    )

    assert gateway.encode_payload(first) == gateway.encode_payload(second)
