"""Bounded writer recovery and diagnosable cache failures (2026-11-06 incident)."""
from __future__ import annotations

import errno
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

import rquant.l2.cache as cache_module
from rquant.l2.cache import CacheError, RawL2Cache
from rquant.l2.models import MarketObservation
from tests.unit.test_l2_cache import observed_trade_push_data


def observation(offset: int = 0) -> MarketObservation:
    data = observed_trade_push_data()
    data["received_at"] = data["received_at"] + __import__("datetime").timedelta(seconds=offset)
    return MarketObservation.model_validate(data)


def fill(root: Path, count: int) -> None:
    cache = RawL2Cache(root)
    with cache.writer():
        for index in range(count):
            cache.append(observation(index))
        cache.write_status(cache.read_status())


def test_writer_recovery_with_head_decodes_only_the_tail(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cache"
    fill(root, 300)
    calls = {"count": 0}
    original = RawL2Cache._read_batch

    def counting(*args, **kwargs):  # noqa: ANN002, ANN003
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(RawL2Cache, "_read_batch", staticmethod(counting))
    monkeypatch.setattr(RawL2Cache, "_BOUNDED_RECOVERY_MIN_BATCHES", 100)
    cache = RawL2Cache(root)
    started = time.perf_counter()
    with cache.writer():
        elapsed = time.perf_counter() - started
    assert calls["count"] <= RawL2Cache._RECOVERY_TAIL_BATCHES + 2
    assert elapsed < 5
    assert RawL2Cache(root).read_batches(after_sequence=299, limit=1).batches[0].local_batch_sequence == 300


def test_writer_recovery_falls_back_to_full_audit_without_head(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    fill(root, 8)
    (root / "batches" / ".head").unlink()
    calls = {"count": 0}
    original = RawL2Cache._read_batch

    def counting(*args, **kwargs):  # noqa: ANN002, ANN003
        calls["count"] += 1
        return original(*args, **kwargs)

    cache = RawL2Cache(root)
    RawL2Cache._read_batch = staticmethod(counting)  # type: ignore[method-assign]
    try:
        with cache.writer():
            pass
    finally:
        RawL2Cache._read_batch = staticmethod(original)
        pass
    assert calls["count"] >= 8


def test_disk_guard_reports_disk_full(tmp_path: Path, monkeypatch) -> None:
    class Fake:
        f_bavail = 1
        f_frsize = 4096

    monkeypatch.setattr(cache_module.os, "statvfs", lambda path: Fake())
    cache = RawL2Cache(tmp_path / "cache")
    with pytest.raises(CacheError) as failure:
        with cache.writer():
            cache.append(observation())
    assert failure.value.code == "disk_full"
    assert failure.value.detail is not None and failure.value.detail.startswith("free=")


def test_write_errno_maps_to_diagnosable_code(tmp_path: Path, monkeypatch) -> None:
    def broken(*args, **kwargs):  # noqa: ANN002, ANN003
        raise OSError(errno.ENOSPC, "no space left on device")

    monkeypatch.setattr(cache_module, "_write_new", broken)
    cache = RawL2Cache(tmp_path / "cache")
    with pytest.raises(CacheError) as failure:
        with cache.writer():
            cache.append(observation())
    assert failure.value.code == "disk_full"
    assert failure.value.detail is not None and "errno=28" in failure.value.detail


def test_record_cache_error_side_file(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    cache = RawL2Cache(root)
    cache.record_cache_error(CacheError("disk_full", "free=0 needed=1"))
    payload = json.loads((root / ".last-cache-error.json").read_text())
    assert payload["schema"] == "rquant-l2-cache-error-v1"
    assert payload["code"] == "disk_full" and payload["detail"] == "free=0 needed=1"
    assert datetime.fromisoformat(payload["at"]).tzinfo is not None
