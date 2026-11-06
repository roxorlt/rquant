"""Unit tests for the offline L2 consumer throughput benchmark.

These tests are intentionally fast: the legacy two-second sleep is proven with a
fake clock, and the network consumers run against a 130-500 batch synthetic
cache on a random loopback port.
"""
from __future__ import annotations

import hashlib
import urllib.error
from pathlib import Path

import pytest

from rquant.l2 import consumer_bench as bench
from rquant.l2.cache import RawL2Cache


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def fake_batch(sequence: int) -> dict[str, object]:
    return {
        "local_batch_sequence": sequence,
        "response_sha256": "a" * 64,
        "table": {"row_data_sha256": "b" * 64, "source_sha256": "c" * 64},
    }


def fake_page(cursor: int, limit: int, total: int) -> bench.Page:
    count = min(limit, total - cursor)
    return bench.Page(
        next_sequence=cursor + count,
        batches=tuple(fake_batch(sequence)
                      for sequence in range(cursor + 1, cursor + count + 1)))


def test_legacy_two_second_sleep_ceiling_is_at_most_64_batches_per_second():
    assert bench.legacy_cycle_ceiling(0.0) == 64.0
    measured = bench.legacy_cycle_ceiling(0.1917)
    assert 58.0 < measured < 64.0
    assert measured < 131.2

    clock = FakeClock()
    total = 10_000
    request_seconds = 0.1917

    def fetch(cursor: int, limit: int) -> bench.Page:
        clock.advance(request_seconds)
        return fake_page(cursor, limit, total)

    result = bench.legacy_http_loop(fetch, deadline_seconds=20 * (2.0 + request_seconds) - 0.1,
                                    clock=clock, sleep=clock.sleep)
    assert result["gets"] == 20
    assert result["batches"] == 20 * bench.DEFAULT_LIMIT
    assert result["batches_per_second"] == pytest.approx(128 / (2.0 + request_seconds))
    assert result["batches_per_second"] < 64.0
    assert result["batches_per_second"] < 131.2


def test_bulk_and_writer_cache_builds_are_deterministic_and_share_chain(tmp_path: Path):
    bulk_a = bench.build_synthetic_cache(tmp_path / "bulk-a", 64, mode="bulk")
    bulk_b = bench.build_synthetic_cache(tmp_path / "bulk-b", 64, mode="bulk")
    writer = bench.build_synthetic_cache(tmp_path / "writer", 64, mode="writer")
    assert bulk_a.chain_sha256 == bulk_b.chain_sha256 == writer.chain_sha256

    cache = RawL2Cache(tmp_path / "bulk-a")
    assert cache.read_status().last_sequence == 64
    page = cache.read_batches(after_sequence=0, limit=128)
    assert len(page.batches) == 64
    assert page.batches[0].local_batch_sequence == 1
    assert page.batches[-1].local_batch_sequence == 64

    cache_chain = bench.read_cache_chain(tmp_path / "bulk-a")
    writer_chain = bench.read_cache_chain(tmp_path / "writer")
    assert cache_chain.batches == writer_chain.batches == 64
    assert cache_chain.chain_sha256 == writer_chain.chain_sha256 == bulk_a.chain_sha256


def test_calibrated_http_consumer_catches_up_and_matches_cache_chain(tmp_path: Path):
    batches = 500
    build = bench.build_synthetic_cache(tmp_path / "cache", batches, mode="bulk")
    cache_chain = bench.read_cache_chain(tmp_path / "cache")
    with bench.start_api_server(tmp_path / "cache") as server:
        head = bench.http_status_head(server.base_url)
        assert head == batches
        consumer = bench.HttpConsumer(bench.http_fetch(server.base_url),
                                      idle_sleep_seconds=0.02)
        phase = consumer.run(lambda: consumer.cursor >= head)
        idle = consumer.idle_hold(0.1)
        assert bench.http_status_head(server.base_url) == head

    assert consumer.cursor == consumer.batches == batches == head
    assert phase.batches == batches
    assert phase.seconds < 10
    assert phase.batches_per_second > 131.2
    assert consumer.retries == 0
    assert consumer.partial_pages == 1
    assert idle.batches == 0
    assert idle.gets >= 1
    assert consumer.chain_sha256 == cache_chain.chain_sha256 == build.chain_sha256

    summary = consumer.summary()
    assert summary["limit"] == bench.DEFAULT_LIMIT
    assert summary["gets"] == 4 + idle.gets
    assert summary["get_p50_ms"] > 0
    assert summary["get_p95_ms"] >= summary["get_p50_ms"]
    assert summary["catch_up"]["batches"] == batches
    assert summary["head_hold"]["batches"] == 0


def test_http_consumer_requests_limit_128_and_waits_at_head(tmp_path: Path):
    batches = 130
    bench.build_synthetic_cache(tmp_path / "cache", batches, mode="bulk")
    with bench.start_api_server(tmp_path / "cache") as server:
        head = bench.http_status_head(server.base_url)
        consumer = bench.HttpConsumer(bench.http_fetch(server.base_url), limit=128,
                                      idle_sleep_seconds=0.02)
        consumer.run(lambda: consumer.cursor >= head)
        idle = consumer.idle_hold(0.12)

    assert head == batches
    assert consumer.page_sizes == [128, 2]
    assert consumer.batches == batches
    assert sum(consumer.page_sizes) == batches
    assert consumer.partial_pages == 1
    assert idle.gets >= 2
    assert idle.batches == 0
    assert consumer.cursor == batches
    assert consumer.idle_latencies_ms and min(consumer.idle_latencies_ms) >= 0
    summary = consumer.summary()
    assert summary["idle_get_p50_ms"] > 0
    assert summary["full_pages"] == 1
    assert summary["partial_pages"] == 1


def test_http_consumer_retries_transient_errors_and_bounds_stalls():
    attempts: list[int] = []
    failures = {"remaining": 2}
    total = 4

    def flaky_fetch(cursor: int, limit: int) -> bench.Page:
        attempts.append(cursor)
        if failures["remaining"]:
            failures["remaining"] -= 1
            raise urllib.error.URLError("synthetic transient")
        return fake_page(cursor, limit, total)

    consumer = bench.HttpConsumer(flaky_fetch, retry_sleep_seconds=0.0)
    phase = consumer.run(lambda: consumer.cursor >= total)
    assert consumer.retries == 2
    assert attempts[:3] == [0, 0, 0]
    assert consumer.cursor == consumer.batches == phase.batches == total
    expected_chain = hashlib.sha256(b"".join(
        f"{sequence}:{'a' * 64}:{'b' * 64}:{'c' * 64}\n".encode()
        for sequence in range(1, total + 1))).hexdigest()
    assert consumer.chain_sha256 == expected_chain

    stalled_polls = {"count": 0}

    def stuck_fetch(cursor: int, limit: int) -> bench.Page:
        stalled_polls["count"] += 1
        return bench.Page(next_sequence=cursor, batches=())

    stuck = bench.HttpConsumer(stuck_fetch, max_stall_polls=4, idle_sleep_seconds=0.001,
                               timeout_seconds=5)
    with pytest.raises(bench.ConsumerStalled):
        stuck.run(lambda: False)
    assert stalled_polls["count"] == 4

    jump = bench.HttpConsumer(
        lambda cursor, limit: bench.Page(cursor + 2, (fake_batch(cursor + 1),)),
        idle_sleep_seconds=0.001)
    with pytest.raises(bench.ConsumerGap):
        jump.run(lambda: False)

    overflow = bench.HttpConsumer(
        lambda cursor, limit: bench.Page(cursor + 1, tuple(fake_batch(cursor + 1)
                                                           for _ in range(129))),
        idle_sleep_seconds=0.001)
    with pytest.raises(bench.ConsumerLimitViolation):
        overflow.run(lambda: False)


def test_websocket_reference_consumer_reaches_head_and_matches_chain(tmp_path: Path):
    batches = 200
    bench.build_synthetic_cache(tmp_path / "cache", batches, mode="bulk")
    cache_chain = bench.read_cache_chain(tmp_path / "cache")
    with bench.start_api_server(tmp_path / "cache") as server:
        head = bench.http_status_head(server.base_url)
        result = bench.run_ws_consumer(server.ws_url, head=head)

    assert head == result.batches == result.cursor_end == batches
    assert result.chain_sha256 == cache_chain.chain_sha256
    assert result.reconnects == 0
    assert result.messages == 2
    assert result.batches_per_second > 131.2


def test_run_benchmark_small_report_is_integrity_ok(tmp_path: Path):
    report = bench.run_benchmark(batches=256, hold_seconds=0.05, cache_root=tmp_path / "cache",
                                 ws_reference=True)
    assert report["schema"] == bench.SCHEMA
    assert report["integrity"]["ok"] is True
    assert report["integrity"]["http_chain_match"] is True
    assert report["integrity"]["ws_chain_match"] is True
    assert report["integrity"]["head_at_start"] == report["integrity"]["head_at_end"] == 256
    assert report["consumers"]["http"]["batches"] == 256
    assert report["consumers"]["http"]["chain_batches"] == 256
    assert report["consumers"]["ws_reference"]["batches"] == 256
    assert report["legacy_two_second_sleep_baseline"]["ceilings_batches_per_second"][
        "zero_latency_api"] == 64.0
    assert report["comparison_40_stock_2026_10_29"]["producer_batches_per_second_40_stock"] == 131.2
    assert report["metric_definitions"]["chain_sha256"]
    assert report["cache"]["cache_root"] == str(tmp_path / "cache")
