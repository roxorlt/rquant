"""80 股写盘容量改造：分组提交（cache）与 status 快照节流（feed）。

对应卡片 card-91c74a3b。默认路径（group_commit=0 / status_flush_interval=0）必须
与旧行为逐字节一致，因此这里同时覆盖「旧默认不变」和「新可选路径」。
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.l2.cache import CacheError, RawL2Cache
from rquant.l2.feed import FeedCollector
from rquant.l2.models import FeedStatus

from tests.unit.test_l2_cache import observation


def _append(cache: RawL2Cache, count: int) -> list:
    batches = []
    for _ in range(count):
        item = observation().model_copy(
            update={"epoch_id": "group_commit_test",
                    "received_at": datetime(2026, 11, 6, 1, 30, tzinfo=UTC)})
        batches.append(cache.append(item))
    return batches


def _batch_dirs(root: Path) -> list[str]:
    return sorted(p.name for p in (root / "batches").iterdir() if p.name.isdigit())


def _pending_dirs(root: Path) -> list[str]:
    return [p.name for p in (root / "batches").iterdir() if p.name.startswith(".pending-")]


def test_group_commit_publishes_contiguous_prefix_and_head(tmp_path: Path) -> None:
    cache = RawL2Cache(tmp_path, group_commit=4)
    with cache.writer():
        batches = _append(cache, 10)
        assert [batch.local_batch_sequence for batch in batches] == list(range(1, 11))
    reader = RawL2Cache(tmp_path)
    status = reader.read_status()
    assert status.last_sequence == 10
    page = reader.read_batches(after_sequence=0, limit=100)
    assert [batch.local_batch_sequence for batch in page.batches] == list(range(1, 11))
    assert (tmp_path / "batches" / ".head").exists()
    assert _batch_dirs(tmp_path) == [f"{index:020d}" for index in range(1, 11)]
    assert _pending_dirs(tmp_path) == []


def test_group_commit_flushes_before_status_snapshot(tmp_path: Path) -> None:
    cache = RawL2Cache(tmp_path, group_commit=64)
    with cache.writer():
        _append(cache, 3)
        # 3 < 64：还在内存组里；写 status 必须先落盘，否则 status 会声称存在未提交批次
        assert _batch_dirs(tmp_path) == []
        cache.write_status(FeedStatus(state="connected"))
        assert _batch_dirs(tmp_path) == [f"{index:020d}" for index in range(1, 4)]
        assert RawL2Cache(tmp_path).read_status().last_sequence == 3
    assert RawL2Cache(tmp_path).read_status().last_sequence == 3


def test_group_commit_discards_unflushed_batches_on_error(tmp_path: Path) -> None:
    cache = RawL2Cache(tmp_path, group_commit=8)
    with pytest.raises(RuntimeError):
        with cache.writer():
            _append(cache, 5)
            raise RuntimeError("simulated crash before group flush")
    reader = RawL2Cache(tmp_path)
    assert reader.read_status().last_sequence == 0
    assert _batch_dirs(tmp_path) == []
    assert _pending_dirs(tmp_path) == []


def test_group_commit_crash_leaves_only_committed_prefix(tmp_path: Path) -> None:
    cache = RawL2Cache(tmp_path, group_commit=4)
    with pytest.raises(RuntimeError):
        with cache.writer():
            _append(cache, 6)  # 1-4 committed by threshold, 5-6 still buffered
            raise RuntimeError("boom")
    directories = _batch_dirs(tmp_path)
    assert directories == [f"{index:020d}" for index in range(1, 5)]
    reader = RawL2Cache(tmp_path)
    assert reader.read_status().last_sequence == 4
    # writer recovery over the committed prefix must be clean
    with reader.writer():
        assert reader.read_status().last_sequence == 4


def test_legacy_group_commit_zero_keeps_immediate_publication(tmp_path: Path) -> None:
    cache = RawL2Cache(tmp_path)
    with cache.writer():
        _append(cache, 2)
        assert _batch_dirs(tmp_path) == [f"{index:020d}" for index in range(1, 3)]


def test_group_commit_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        RawL2Cache(tmp_path, group_commit=-1)
    with pytest.raises(ValueError):
        RawL2Cache(tmp_path, group_commit=1025)
    with pytest.raises(ValueError):
        RawL2Cache(tmp_path, group_commit="4")  # type: ignore[arg-type]


class _RecordingCache:
    def __init__(self) -> None:
        self.statuses: list[FeedStatus] = []

    def write_status(self, status: FeedStatus) -> None:
        self.statuses.append(status)


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _collector(cache, clock: _Clock, *, status_flush_interval: float) -> FeedCollector:
    return FeedCollector(
        cache, bootstrap_path=Path("bootstrap.bin"),
        order_template_path=Path("order.bin"), trade_template_path=Path("trade.bin"),
        clock=clock, sleep=clock.sleep, status_flush_interval=status_flush_interval)


def test_status_flush_interval_throttles_non_transition_updates() -> None:
    cache, clock = _RecordingCache(), _Clock()
    collector = _collector(cache, clock, status_flush_interval=1.0)
    collector._update(market_batch_count=1)   # 首次没有基线 -> 落盘
    collector._update(market_batch_count=2)   # 节流
    collector._update(last_market_received_at=datetime(2026, 11, 6, tzinfo=UTC))  # 节流
    assert len(cache.statuses) == 1
    clock.now += 1.0
    collector._update(market_batch_count=3)
    assert len(cache.statuses) == 2
    # 状态迁移必须立即落盘（不等间隔）
    collector._update(state="listening_candidate")
    assert len(cache.statuses) == 3
    assert cache.statuses[-1].state == "listening_candidate"


def test_status_flush_interval_zero_keeps_snapshot_per_update() -> None:
    cache, clock = _RecordingCache(), _Clock()
    collector = _collector(cache, clock, status_flush_interval=0.0)
    collector._update(market_batch_count=1)
    collector._update(market_batch_count=2)
    assert len(cache.statuses) == 2


def test_status_flush_interval_validation() -> None:
    cache, clock = _RecordingCache(), _Clock()
    for value in (-1.0, 61.0, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            _collector(cache, clock, status_flush_interval=value)


def test_cli_rejects_out_of_range_write_path_options(tmp_path: Path) -> None:
    from rquant.l2.__main__ import main

    base = [
        "--cache-root", str(tmp_path / "cache"),
        "--bootstrap-file", str(tmp_path / "bootstrap.bin"),
        "--order-template", str(tmp_path / "order.bin"),
        "--trade-template", str(tmp_path / "trade.bin"),
    ]
    with pytest.raises(SystemExit):
        main(base + ["--commit-group", "5000"])
    with pytest.raises(SystemExit):
        main(base + ["--status-flush-interval", "-1"])
