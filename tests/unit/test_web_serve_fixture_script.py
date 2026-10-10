"""``scripts/serve_web_fixture.py``: the browser tests' pinned, running clock."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from scripts import serve_web_fixture


def test_the_clock_starts_at_the_pinned_instant_and_runs() -> None:
    ticks = iter((100.0, 100.0, 175.5))
    start = datetime(2026, 9, 24, 7, 36, tzinfo=UTC)

    clock = serve_web_fixture.running_clock(start, monotonic=lambda: next(ticks))

    assert clock() == start
    assert clock() == start + timedelta(seconds=75.5)


def test_now_requires_a_time_zone() -> None:
    assert serve_web_fixture._instant("2026-09-24T07:36:00Z") == datetime(
        2026, 9, 24, 7, 36, tzinfo=UTC
    )
    with pytest.raises(Exception, match="time zone"):
        serve_web_fixture._instant("2026-09-24T07:36:00")
