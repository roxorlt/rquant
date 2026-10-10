"""Module 12: read-only task overview + logs from systemd (no run-now: that needs root).

Lists ``rquant*`` services/timers via ``systemctl`` and tails ``journalctl`` for one unit.
Both run as the API's own user; journal access may be denied, which is shown, not raised.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable

from pydantic import BaseModel

Runner = Callable[[list[str]], str]
_UNIT = re.compile(r"^rquant[\w@.:-]*\.(service|timer)$")


class TaskRow(BaseModel):
    unit: str
    kind: str
    active: str
    sub: str
    description: str
    next_run: str | None = None
    last_run: str | None = None


class TaskLog(BaseModel):
    unit: str
    lines: list[str]
    error: str | None = None


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True).stdout


def list_tasks(runner: Runner = run) -> list[TaskRow]:
    units = runner(["systemctl", "list-units", "--all", "--plain", "--no-legend", "--no-pager",
                    "rquant*"])
    timers = runner(["systemctl", "list-timers", "--all", "--plain", "--no-legend", "--no-pager",
                     "rquant*"])
    rows: dict[str, TaskRow] = {}
    for line in units.splitlines():
        parts = line.split(None, 4)
        if len(parts) < 4 or not _UNIT.match(parts[0]):
            continue
        unit, _load, active, sub = parts[:4]
        rows[unit] = TaskRow(unit=unit, kind=unit.rsplit(".", 1)[1], active=active, sub=sub,
                             description=parts[4] if len(parts) > 4 else "")
    for line in timers.splitlines():
        m = re.search(r"(rquant[\w@.:-]*\.timer)", line)
        if not m or m.group(1) not in rows:
            continue
        before = line[: m.start()].split()
        # NEXT(4 tokens: Day Date Time TZ) LEFT(…) LAST(4) PASSED(…) — keep it lenient.
        stamps = re.findall(r"\w{3} \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+", " ".join(before))
        rows[m.group(1)].next_run = stamps[0] if stamps else None
        rows[m.group(1)].last_run = stamps[1] if len(stamps) > 1 else None
    return sorted(rows.values(), key=lambda r: r.unit)


def tail(unit: str, lines: int = 200, runner: Runner = run) -> TaskLog:
    if not _UNIT.match(unit):
        return TaskLog(unit=unit, lines=[], error="只允许 rquant* 单元")
    try:
        out = runner(["journalctl", "-u", unit, "-n", str(max(1, min(lines, 1000))),
                      "--no-pager", "-o", "short-iso"])
    except (subprocess.SubprocessError, OSError) as exc:
        return TaskLog(unit=unit, lines=[], error=f"读取日志失败：{type(exc).__name__}")
    return TaskLog(unit=unit, lines=out.splitlines())


def demo_runner(cmd: list[str]) -> str:
    """Fixture-mode stand-in (e2e / local dev)."""
    if cmd[:2] == ["systemctl", "list-units"]:
        return ("rquant-web.service loaded active running rQuant web API\n"
                "rquant-daily.service loaded inactive dead rQuant daily ingest\n"
                "rquant-daily.timer loaded active waiting rQuant daily ingest timer\n")
    if cmd[:2] == ["systemctl", "list-timers"]:
        return ("Mon 2026-10-12 17:30:00 CST 1h left Fri 2026-10-09 17:30:00 CST 2 days ago "
                "rquant-daily.timer rquant-daily.service\n")
    return "2026-10-09T17:30:01+0800 host rquant-daily[1]: ingest ok rows=5412\n"
