"""The web API follows Serving generations: switch on a new one, never on a broken one."""

from __future__ import annotations

import os
import stat
import threading
from pathlib import Path

from pytest import MonkeyPatch

from rquant.serving_publisher import ServingGenerationLease, ServingReader
from rquant.web import serving
from rquant.web.serving import GenerationTracker
from tests.support.web_serving_fixture import build_web_fixture


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def _corrupt_database(root: Path, generation_id: str) -> None:
    database = root / "generations" / generation_id / "serving.duckdb"
    os.chmod(database.parent, stat.S_IRWXU)
    os.chmod(database, stat.S_IRUSR | stat.S_IWUSR)
    with database.open("r+b") as handle:
        handle.seek(4096)
        handle.write(b"\xff" * 64)


def test_unavailable_until_a_generation_is_published_then_follows_it(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    tracker = GenerationTracker(root)
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is None
        assert tracker.failure is not None

        first = build_web_fixture(root, "baseline")
        tracker.refresh()
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert borrowed.manifest.generation_id == first.generation_id
            assert borrowed.cursor.execute("SELECT count(*) FROM runtime_services").fetchone() == (
                8,
            )
        assert tracker.failure is None
    finally:
        tracker.close()


def test_switches_to_the_new_generation_after_the_pointer_check_interval(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    clock = _Clock()
    tracker = GenerationTracker(root, pointer_check_seconds=2.0, monotonic=clock)
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert borrowed.manifest.generation_id == first.generation_id

        second = build_web_fixture(root, "baseline", sequence=1)
        clock.value = 1.0  # inside the interval: current.json is not re-read yet
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert borrowed.manifest.generation_id == first.generation_id
        clock.value = 2.5
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert borrowed.manifest.generation_id == second.generation_id
            assert borrowed.pointer is not None
            assert borrowed.pointer.previous_generation_id == first.generation_id
    finally:
        tracker.close()


def test_an_in_flight_request_finishes_on_its_generation_before_it_is_closed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    tracker = GenerationTracker(root)
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            old_lease = tracker._current.lease  # type: ignore[union-attr]
            build_web_fixture(root, "baseline", sequence=1)
            tracker.refresh()
            assert tracker.current_generation_id != first.generation_id
            # The switch happened, but this request keeps reading the first generation.
            assert not old_lease.closed
            assert borrowed.manifest.generation_id == first.generation_id
            assert borrowed.cursor.execute("SELECT count(*) FROM signals").fetchone() == (2,)
        assert old_lease.closed
    finally:
        tracker.close()


def test_a_generation_that_fails_verification_never_replaces_the_served_one(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    tracker = GenerationTracker(root)
    try:
        tracker.refresh()
        second = build_web_fixture(root, "baseline", sequence=1)
        _corrupt_database(root, second.generation_id)
        assert ServingReader(root).current_pointer().generation_id == second.generation_id

        tracker.refresh()
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert borrowed.manifest.generation_id == first.generation_id
            assert borrowed.fallback_detail is not None
            assert second.generation_id[:8] in borrowed.fallback_detail
            assert "校验失败" in borrowed.fallback_detail
    finally:
        tracker.close()


def test_close_releases_the_lease_and_the_background_thread(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    tracker = GenerationTracker(root)
    tracker.start(0.05)
    tracker.refresh()
    lease = tracker._current.lease  # type: ignore[union-attr]
    tracker.close()
    assert lease.closed
    assert tracker._thread is None
    with tracker.borrow() as borrowed:  # a closed tracker serves nothing
        assert borrowed is None


def test_a_sibling_borrow_waits_until_the_new_generation_is_verified_and_installed(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    clock = _Clock()
    verified_new = threading.Event()
    allow_install = threading.Event()
    sibling_checked_clock = threading.Event()
    sibling_done = threading.Event()
    views: dict[str, tuple[str, str | None]] = {}
    failures: list[Exception] = []
    threads: list[threading.Thread] = []

    def monotonic() -> float:
        if threading.current_thread().name == "sibling-borrow":
            sibling_checked_clock.set()
        return clock()

    tracker = GenerationTracker(root, pointer_check_seconds=0.001, monotonic=monotonic)
    try:
        tracker.refresh()
        assert tracker._current is not None
        old_lease = tracker._current.lease
        second = build_web_fixture(root, "baseline", sequence=1)
        clock.value = 1.0

        class PausedVerifiedReader(ServingReader):
            def acquire_generation(self) -> ServingGenerationLease:
                lease = super().acquire_generation()
                if lease.manifest.generation_id == second.generation_id:
                    verified_new.set()
                    if not allow_install.wait(timeout=5):
                        lease.close()
                        raise TimeoutError("verified-generation install barrier timed out")
                return lease

        monkeypatch.setattr(serving, "ServingReader", PausedVerifiedReader)

        def borrow(name: str) -> None:
            try:
                with tracker.borrow() as borrowed:
                    assert borrowed is not None
                    assert borrowed.cursor.execute("SELECT count(*) FROM signals").fetchone() == (
                        2,
                    )
                    views[name] = (borrowed.manifest.generation_id, borrowed.fallback_detail)
            except Exception as error:
                failures.append(error)
            finally:
                if name == "sibling":
                    sibling_done.set()

        refreshing = threading.Thread(target=borrow, args=("refreshing",))
        threads.append(refreshing)
        refreshing.start()
        assert verified_new.wait(timeout=5)
        assert tracker._refresh_lock.locked()
        assert tracker.current_generation_id == first.generation_id
        assert not old_lease.closed

        sibling = threading.Thread(target=borrow, args=("sibling",), name="sibling-borrow")
        threads.append(sibling)
        sibling.start()
        assert sibling_checked_clock.wait(timeout=5)
        returned_before_install = sibling_done.wait(timeout=0.25)
        allow_install.set()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()

        assert not failures
        assert not returned_before_install
        assert views == {
            "refreshing": (second.generation_id, None),
            "sibling": (second.generation_id, None),
        }
        assert old_lease.closed
    finally:
        allow_install.set()
        for thread in threads:
            thread.join(timeout=5)
        tracker.close()
        assert all(not thread.is_alive() for thread in threads)
