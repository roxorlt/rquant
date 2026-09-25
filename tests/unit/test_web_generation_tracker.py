"""The web API follows Serving generations: switch on a new one, never on a broken one."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from rquant.serving_publisher import ServingReader
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
