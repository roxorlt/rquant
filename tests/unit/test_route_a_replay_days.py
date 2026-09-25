"""`scripts/route_a_replay_days.py`: several days, each its own process, one report.

The day script is replaced by a small fake (`--day-script`) that sleeps, records when it ran,
and leaves the files a real day leaves (a `summary.json` in a sandbox under its replay root,
or a refusal line), so the scheduling and the report are checked without a single world.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "route_a_replay_days.py"
DATES = ("2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24")

FAKE_DAY = """
import json, os, sys, time
from pathlib import Path

args = sys.argv[1:]
day = args[args.index("--trade-date") + 1]
root = Path(args[args.index("--replay-root") + 1])
log = Path(os.environ["FAKE_DAY_LOG"])
with log.open("a") as handle:
    event = {"event": "start", "day": day, "at": time.time(), "argv": args}
    handle.write(json.dumps(event) + "\\n")
time.sleep(float(os.environ.get("FAKE_DAY_SECONDS", "0.5")))
if "--dry-plan" in args:
    cannot = ["reference_slow: no batch"] if day == "2026-09-21" else []
    print("DRY-PLAN " + json.dumps({
        "trade_date": day, "cannot": cannot,
        "inputs": {"reference_slow": {"origin": "synthesized"}, "calendar": {"origin": "recorded"}},
    }))
    code = 2 if cannot else 0
elif day == "2026-09-21":
    print("REPLAY REFUSED: no reference-slow batch targets 2026-09-21")
    code = 2
else:
    sandbox = root / f"20260925T000000-{day}"
    sandbox.mkdir(parents=True)
    crashed = ["strategy.n_shape.v1"] if day == "2026-09-22" else []
    summary = {
        "trade_date": day,
        "mode": {"production_faithful": True},
        "inputs": {"provenance": {
            "reference_slow": {"origin": "synthesized", "source": "live capture"},
            "auction_match": {"origin": "recorded", "source": "spool"},
        }},
        "candidates_per_family": {"auction_gap": 3, "n_shape": 1},
        "chain": {
            "signals_per_strategy": {"auction_gap": 2, "n_shape": 0},
            "paper": {"paper_fill": 1},
            "serving": {"same_day": True, "signals_rows_today": 2},
        },
        "verdict": {
            "same_day_serving_generation": True,
            "notifier_shadow_only": True,
            "crashed_roles": crashed,
        },
        "roles": {
            "strategy.n_shape.v1": {
                "iterations": 351, "total_failures": 4 if crashed else 0,
                "errors": {"boom": {}} if crashed else {}, "crashes": [{}] if crashed else [],
                "first_output_at": "2026-09-22T09:31:08+08:00",
            },
        },
        "profile": {"roles": {"strategy.n_shape.v1": {"p50": 0.2, "p95": 0.9}}},
        "stage_seconds": {"total": 1.0},
    }
    (sandbox / "summary.json").write_text(json.dumps(summary))
    code = 1 if crashed else 0
with log.open("a") as handle:
    handle.write(json.dumps({"event": "end", "day": day, "at": time.time()}) + "\\n")
sys.exit(code)
"""


def _runner() -> ModuleType:
    name = "route_a_replay_days_under_test"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _host(tmp_path: Path) -> dict[str, Path]:
    data = tmp_path / "host" / "data"
    runtime = data / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    return {
        "runtime": runtime,
        "replica": data / "rquant_ro.duckdb",
        "inputs": data / "runtime-production-inputs.json",
    }


def _run(
    tmp_path: Path, *arguments: str, seconds: float = 0.5
) -> tuple[subprocess.CompletedProcess[str], list[dict[str, object]]]:
    host = _host(tmp_path)
    fake = tmp_path / "fake_day.py"
    fake.write_text(FAKE_DAY, encoding="utf-8")
    log = tmp_path / "fake-day.log"
    environment = dict(os.environ)
    environment["FAKE_DAY_LOG"] = str(log)
    environment["FAKE_DAY_SECONDS"] = str(seconds)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day-script",
            str(fake),
            "--runtime-root",
            str(host["runtime"]),
            "--replica",
            str(host["replica"]),
            "--production-inputs",
            str(host["inputs"]),
            "--poll-seconds",
            "0.05",
            *arguments,
        ],
        capture_output=True,
        text=True,
        env=environment,
        cwd=str(REPO_ROOT),
        timeout=300,
        check=False,
    )
    events = (
        [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        if log.exists()
        else []
    )
    return result, events


def _most_at_once(events: list[dict[str, object]]) -> int:
    running = most = 0
    for event in sorted(events, key=lambda item: (float(item["at"]), item["event"] == "start")):
        running += 1 if event["event"] == "start" else -1
        most = max(most, running)
    return most


def test_days_run_as_separate_processes_at_most_parallel_at_a_time(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    result, events = _run(
        tmp_path,
        "--trade-dates",
        ",".join(reversed(DATES)),
        "--replay-root",
        str(replay_root),
        "--parallel",
        "2",
        "--",
        "--tushare",
        "--until",
        "11:30",
    )

    assert result.returncode == 1, result.stdout + result.stderr
    starts = {event["day"]: event for event in events if event["event"] == "start"}
    #: every day once, started in date order, never more than two at a time, two at once
    started = [
        line.split()[1] for line in result.stdout.splitlines() if line.startswith("started ")
    ]
    assert started == list(DATES)
    assert sorted(starts) == list(DATES)
    assert len([event for event in events if event["event"] == "start"]) == len(DATES)
    #: the runner's own count when it started each day, and the days' own clocks
    running = [
        int(line.split("(", 1)[1].split()[0])
        for line in result.stdout.splitlines()
        if line.startswith("started ")
    ]
    assert max(running) == 2
    assert _most_at_once(events) <= 2
    argv = starts["2026-09-18"]["argv"]
    assert argv[argv.index("--tushare-cache") + 1] == str(replay_root / "tushare-cache")
    assert argv[-3:] == ["--tushare", "--until", "11:30"]
    (root,) = replay_root.glob("days-*")
    assert argv[argv.index("--replay-root") + 1] == str(root / "2026-09-18")

    report = json.loads((root / "report.json").read_text(encoding="utf-8"))
    by_day = {day["trade_date"]: day for day in report["days"]}
    assert [day["trade_date"] for day in report["days"]] == list(DATES)
    assert report["exit_code"] == 1
    assert report["production_untouched"] is True
    assert by_day["2026-09-18"]["verdict"] == "OK"
    assert by_day["2026-09-18"]["candidates_per_family"] == {"auction_gap": 3, "n_shape": 1}
    assert by_day["2026-09-18"]["signals_per_strategy"] == {"auction_gap": 2, "n_shape": 0}
    assert by_day["2026-09-18"]["paper_fills"] == 1
    assert by_day["2026-09-18"]["notifier_shadow_only"] is True
    assert by_day["2026-09-18"]["serving_same_day"] is True
    assert by_day["2026-09-18"]["inputs"]["reference_slow"]["origin"] == "synthesized"
    assert by_day["2026-09-18"]["roles"]["strategy.n_shape.v1"]["rounds"] == 351
    assert by_day["2026-09-21"]["exit_code"] == 2
    assert by_day["2026-09-21"]["refused"] == "no reference-slow batch targets 2026-09-21"
    assert by_day["2026-09-22"]["verdict"] == "FAILED"
    assert by_day["2026-09-22"]["roles"]["strategy.n_shape.v1"]["crashes"] == 1

    markdown = (root / "report.md").read_text(encoding="utf-8")
    assert "| 2026-09-18 | OK | 0 | reference_slow |" in markdown
    assert "| 2026-09-22 | FAILED | 1 |" in markdown
    assert "strategy.n_shape.v1" in markdown.split("| 2026-09-22 | FAILED | 1 |")[1].split("\n")[0]
    assert "- 2026-09-21: no reference-slow batch targets 2026-09-21" in markdown
    assert (root / "2026-09-21.log").read_text(encoding="utf-8").startswith("REPLAY REFUSED")


def test_one_at_a_time_never_overlaps(tmp_path: Path) -> None:
    result, events = _run(
        tmp_path,
        "--trade-dates",
        "2026-09-18,2026-09-23,2026-09-24",
        "--replay-root",
        str(tmp_path / "replay"),
        "--parallel",
        "1",
        seconds=0.2,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert _most_at_once(events) == 1
    assert all("(1 running" in line for line in result.stdout.splitlines() if "started" in line)


def test_the_dry_plans_of_several_days_are_one_report(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    result, events = _run(
        tmp_path,
        "--trade-dates",
        "2026-09-18,2026-09-21",
        "--replay-root",
        str(replay_root),
        "--dry-plan",
        seconds=0.05,
    )

    assert result.returncode == 2, result.stdout + result.stderr
    assert all("--dry-plan" in event["argv"] for event in events if event["event"] == "start")
    (root,) = replay_root.glob("days-*")
    report = json.loads((root / "report.json").read_text(encoding="utf-8"))
    by_day = {day["trade_date"]: day for day in report["days"]}
    assert by_day["2026-09-18"]["dry_plan"] == {
        "cannot": [],
        "inputs": {"reference_slow": "synthesized", "calendar": "recorded"},
    }
    assert by_day["2026-09-21"]["dry_plan"]["cannot"] == ["reference_slow: no batch"]
    markdown = (root / "report.md").read_text(encoding="utf-8")
    assert "| 2026-09-18 | PLAN OK | 0 | reference_slow |" in markdown
    assert "| 2026-09-21 | CANNOT | 2 |" in markdown


def test_a_replay_root_inside_production_is_refused_before_any_day_starts(
    tmp_path: Path,
) -> None:
    host = _host(tmp_path)
    result, events = _run(
        tmp_path,
        "--trade-dates",
        "2026-09-18",
        "--replay-root",
        str(host["runtime"] / "replay"),
    )

    assert result.returncode == 2
    assert "overlaps production path" in result.stdout
    assert events == []
    assert not (host["runtime"] / "replay").exists()


def test_dates_are_parsed_once_each_and_sessions_come_from_the_newest_calendar(
    tmp_path: Path,
) -> None:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.strict_json import canonical_json_bytes

    runner = _runner()
    assert runner.parse_trade_dates(" 2026-09-22,2026-09-18,,2026-09-22 ") == [
        date(2026, 9, 18),
        date(2026, 9, 22),
    ]
    with pytest.raises(runner.DaysRefusedError, match="not a YYYY-MM-DD"):
        runner.parse_trade_dates("2026-09-31")

    runtime = tmp_path / "runtime"
    directory = runtime / "authorities" / "market-calendar" / "generations"
    directory.mkdir(parents=True)
    opens = tuple(date.fromisoformat(day) for day in (*DATES, "2026-09-25", "2026-09-28"))
    for generated, open_dates in (
        (datetime(2026, 9, 1, tzinfo=UTC), opens[:3]),
        (datetime(2026, 9, 24, 10, tzinfo=UTC), opens),
    ):
        calendar = MarketCalendarAuthority.create(
            schema_version=1,
            exchange="SSE",
            producer_commit="c" * 40,
            coverage_start=date(2026, 9, 1),
            coverage_end=date(2026, 12, 31),
            open_dates=open_dates,
            generated_at=generated,
        )
        (directory / f"{calendar.content_sha256}.json").write_bytes(
            canonical_json_bytes(calendar.model_dump(mode="json"))
        )

    days = runner.last_sessions(runtime, 3, before=date(2026, 9, 25), read_bytes=Path.read_bytes)
    assert days == [date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)]
    with pytest.raises(runner.DaysRefusedError, match="holds only 5 open dates"):
        runner.last_sessions(runtime, 9, before=date(2026, 9, 25), read_bytes=Path.read_bytes)
