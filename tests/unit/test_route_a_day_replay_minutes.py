"""`route_a_day_replay.ReplayMinuteAdapter`: never a bar the clock has not reached."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo

import pandas as pd

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "route_a_day_replay.py"
DAY = date(2026, 9, 24)
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("route_a_day_replay_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _bars(code: str, source: str, start: time, count: int) -> pd.DataFrame:
    rows = []
    for step in range(count):
        stamp = datetime.combine(DAY, start) + timedelta(minutes=step)
        rows.append(
            {
                "ts_code": code,
                "trade_time": stamp,
                "freq": "1min",
                "open": 10.0 + step,
                "high": 10.5 + step,
                "low": 9.5 + step,
                "close": 10.0 + step,
                "vol": 100.0,
                "amount": 1000.0,
                "source": source,
            }
        )
    return pd.DataFrame(rows)


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime.combine(DAY, time(hour, minute, second), tzinfo=SHANGHAI).astimezone(UTC)


def test_the_latest_bar_the_clock_has_reached_is_served_and_nothing_later() -> None:
    module = _script()
    clock = module.ReplayClock(_at(9, 31, 7))
    adapter = module.ReplayMinuteAdapter(
        _bars("600000.SH", "tushare_rt", time(9, 30), 5),
        clock=clock,
        lag_seconds=0,
        tushare_fetch=None,
        trade_date=DAY,
    )

    frame = adapter.rt_min(["600000.SH", "000001.SZ"])

    assert list(frame["trade_time"]) == [datetime.combine(DAY, time(9, 31))]
    assert frame["source"].tolist() == ["tushare_rt"]
    assert adapter.missing == {"000001.SZ"}
    #: before the first bar, nothing at all
    clock.now = _at(9, 29, 59)
    assert adapter.rt_min(["600000.SH"]).empty
    #: a lag holds the bar back until its minute has closed
    strict = module.ReplayMinuteAdapter(
        _bars("600000.SH", "tushare_rt", time(9, 30), 5),
        clock=module.ReplayClock(_at(9, 31, 7)),
        lag_seconds=60,
        tushare_fetch=None,
        trade_date=DAY,
    )
    assert list(strict.rt_min(["600000.SH"])["trade_time"]) == [datetime.combine(DAY, time(9, 30))]


def test_one_source_per_code_so_two_labellings_never_share_a_minute() -> None:
    module = _script()
    frame = pd.concat(
        [
            _bars("600000.SH", "tushare_rt", time(9, 30), 4),
            _bars("600000.SH", "tushare", time(9, 31), 2),
        ]
    )
    adapter = module.ReplayMinuteAdapter(
        frame,
        clock=module.ReplayClock(_at(9, 40)),
        lag_seconds=0,
        tushare_fetch=None,
        trade_date=DAY,
    )

    assert adapter.sources == {"600000.SH": "replica:tushare_rt"}
    assert list(adapter.rt_min(["600000.SH"])["trade_time"]) == [datetime.combine(DAY, time(9, 33))]


def test_codes_the_replica_lacks_are_fetched_once_and_only_for_the_trade_date() -> None:
    module = _script()
    calls: list[str] = []

    def fetch(code: str) -> pd.DataFrame:
        calls.append(code)
        if code == "000002.SZ":
            raise RuntimeError("quota")
        other_day = _bars(code, "tushare", time(9, 31), 3)
        other_day["trade_time"] = other_day["trade_time"] - timedelta(days=1)
        return pd.concat([_bars(code, "tushare", time(9, 31), 3), other_day])

    adapter = module.ReplayMinuteAdapter(
        None,
        clock=module.ReplayClock(_at(9, 45)),
        lag_seconds=0,
        tushare_fetch=fetch,
        trade_date=DAY,
    )

    first = adapter.rt_min(["000001.SZ", "000002.SZ"])
    adapter.rt_min(["000001.SZ", "000002.SZ"])

    assert list(first["trade_time"]) == [datetime.combine(DAY, time(9, 33))]
    assert calls == ["000001.SZ", "000002.SZ"]
    assert adapter.tushare_fetched == {"000001.SZ": 3}
    assert adapter.tushare_failed == {"000002.SZ": "RuntimeError: quota"}
    assert adapter.missing == {"000002.SZ"}
    assert adapter.fetched_frame()["replay_origin"].unique().tolist() == ["tushare:tushare"]
