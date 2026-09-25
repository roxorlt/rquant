"""Replay several trading days through `route_a_day_replay.py`, each in its own process.

    PYTHONDONTWRITEBYTECODE=1 <checkout>/.venv/bin/python \\
        <checkout>/scripts/route_a_replay_days.py --last-n-sessions 5 \\
        --replay-root /home/lighthouse/replay --parallel 2 -- --tushare

Every day is a separate `route_a_day_replay.py` process (so a day uses its own core and its
own memory, and a crash or a refusal ends that day only), at most `--parallel` at a time
(default 2: the host has four cores and production runs beside it). Everything after `--` is
handed to every day unchanged; the Tushare cache defaults to `<replay-root>/tushare-cache`
for all of them, so a day asks Tushare nothing another day, or an earlier run, already asked.

It writes `<replay-root>/days-<stamp>/`: one directory and one log per day, `report.json`
(per day and per role: rounds, failures, first output, candidates, signals per strategy,
paper fills, the notifier's shadow-only check, the same-day serving generation, every
input's provenance, the verdict) and `report.md` (the same as two short tables).

`--dry-plan` runs every day's dry plan instead (read-only, no Tushare request) and reports
which days can be replayed and, for the others, exactly what is missing.

Production is read exactly as the day script reads it: `--last-n-sessions` reads the
market-calendar generations with plain `open(rb)`, under the day script's own audit hook.

Exit status: 0 when every day exited 0, 1 when any day failed, 2 when any day was refused
(and none failed) or on a usage error.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
for _entry in (REPO_ROOT, REPO_ROOT / "scripts", REPO_ROOT / "src"):
    if str(_entry) not in sys.path:
        sys.path.insert(0, str(_entry))

_SHANGHAI = ZoneInfo("Asia/Shanghai")
DAY_SCRIPT = REPO_ROOT / "scripts" / "route_a_day_replay.py"
DEFAULT_RUNTIME_ROOT = Path("/home/lighthouse/rquant/data/runtime")
DEFAULT_REPLICA = Path("/home/lighthouse/rquant/data/rquant_ro.duckdb")
DEFAULT_PRODUCTION_INPUTS = Path("/home/lighthouse/rquant/data/runtime-production-inputs.json")
DRY_PLAN_PREFIX = "DRY-PLAN "


class DaysRefusedError(RuntimeError):
    """A usage error or a refused setup; exit 2."""


# ---------------------------------------------------------------------------------------
# Which days
# ---------------------------------------------------------------------------------------


def parse_trade_dates(text: str) -> list[date]:
    days: set[date] = set()
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            days.add(date.fromisoformat(item))
        except ValueError as error:
            raise DaysRefusedError(f"--trade-dates: {item!r} is not a YYYY-MM-DD date") from error
    if not days:
        raise DaysRefusedError("--trade-dates names no date")
    return sorted(days)


def last_sessions(
    runtime_root: Path,
    count: int,
    *,
    before: date,
    read_bytes: Callable[[Path], bytes],
) -> list[date]:
    """The `count` open dates before `before`, from the newest market-calendar generation."""

    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.strict_json import strict_json_loads

    if count < 1:
        raise DaysRefusedError("--last-n-sessions must be at least 1")
    directory = runtime_root / "authorities" / "market-calendar" / "generations"
    calendars = []
    for path in sorted(directory.glob("*.json")) if directory.is_dir() else ():
        try:
            calendars.append(
                MarketCalendarAuthority.model_validate(strict_json_loads(read_bytes(path)))
            )
        except (OSError, ValueError):
            continue
    if not calendars:
        raise DaysRefusedError(f"no readable market-calendar generation under {directory}")
    newest = max(calendars, key=lambda calendar: calendar.generated_at)
    days = [day for day in newest.open_dates if day < before][-count:]
    if len(days) < count:
        raise DaysRefusedError(
            f"the newest calendar ({newest.content_sha256[:12]}) holds only {len(days)} open "
            f"dates before {before}"
        )
    return days


# ---------------------------------------------------------------------------------------
# Running them
# ---------------------------------------------------------------------------------------


@dataclass
class DayRun:
    trade_date: date
    replay_root: Path
    log_path: Path
    argv: list[str]
    process: Any = None
    started_at: float | None = None
    ended_at: float | None = None
    exit_code: int | None = None
    output: dict[str, Any] = field(default_factory=dict)

    @property
    def wall_seconds(self) -> float | None:
        if self.started_at is None or self.ended_at is None:
            return None
        return round(self.ended_at - self.started_at, 1)


def day_argv(
    *,
    python: str,
    script: Path,
    trade_date: date,
    replay_root: Path,
    runtime_root: Path,
    replica: Path,
    production_inputs: Path,
    cache_root: Path,
    dry_plan: bool,
    passthrough: Sequence[str],
) -> list[str]:
    argv = [
        python,
        str(script),
        "--trade-date",
        trade_date.isoformat(),
        "--replay-root",
        str(replay_root),
        "--runtime-root",
        str(runtime_root),
        "--replica",
        str(replica),
        "--production-inputs",
        str(production_inputs),
    ]
    if "--tushare-cache" not in passthrough:
        argv += ["--tushare-cache", str(cache_root)]
    if dry_plan and "--dry-plan" not in passthrough:
        argv.append("--dry-plan")
    return argv + list(passthrough)


def run_days(
    runs: Sequence[DayRun],
    *,
    parallel: int,
    poll_seconds: float = 2.0,
    log: Callable[[str], None] = print,
    launch: Callable[[DayRun], Any] | None = None,
) -> None:
    """Start the days in date order, at most `parallel` at a time, until all have ended."""

    if parallel < 1:
        raise DaysRefusedError("--parallel must be at least 1")

    def default_launch(run: DayRun) -> Any:
        run.replay_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        handle = open(run.log_path, "w", encoding="utf-8")  # noqa: SIM115 - closed on exit
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        process = subprocess.Popen(
            run.argv,
            stdout=handle,
            stderr=subprocess.STDOUT,
            env=environment,
            cwd=str(REPO_ROOT),
            start_new_session=True,
        )
        process._rquant_log = handle  # noqa: SLF001 - closed when the day ends
        return process

    start = launch or default_launch
    pending = list(runs)
    running: list[DayRun] = []
    try:
        while pending or running:
            while pending and len(running) < parallel:
                run = pending.pop(0)
                run.started_at = time.monotonic()
                run.process = start(run)
                running.append(run)
                log(f"started {run.trade_date} ({len(running)} running, {len(pending)} waiting)")
            for run in list(running):
                code = run.process.poll()
                if code is None:
                    continue
                run.ended_at = time.monotonic()
                run.exit_code = int(code)
                handle = getattr(run.process, "_rquant_log", None)
                if handle is not None:
                    handle.close()
                running.remove(run)
                log(f"ended   {run.trade_date}: exit {code} after {run.wall_seconds}s")
            if running:
                time.sleep(poll_seconds)
    except KeyboardInterrupt:
        for run in running:
            #: the day's own process group only (`start_new_session`); never a pattern kill
            with suppress(ProcessLookupError, AttributeError):
                os.killpg(run.process.pid, signal.SIGTERM)
        raise


# ---------------------------------------------------------------------------------------
# What they found
# ---------------------------------------------------------------------------------------


def _summary_of(run: DayRun) -> tuple[dict[str, Any] | None, Path | None]:
    if not run.replay_root.is_dir():
        return None, None
    found = sorted(run.replay_root.glob("*/summary.json"), key=lambda path: path.stat().st_mtime)
    if not found:
        return None, None
    return json.loads(found[-1].read_text(encoding="utf-8")), found[-1]


def _log_lines(run: DayRun) -> list[str]:
    try:
        return run.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []


def day_report(run: DayRun) -> dict[str, Any]:
    """One day's row of the report, from its summary (or its log when it wrote none)."""

    lines = _log_lines(run)
    refused = next(
        (
            line.split("REPLAY REFUSED:", 1)[1].strip()
            for line in lines
            if "REPLAY REFUSED:" in line
        ),
        None,
    )
    dry = next(
        (
            json.loads(line[len(DRY_PLAN_PREFIX) :])
            for line in reversed(lines)
            if line.startswith(DRY_PLAN_PREFIX)
        ),
        None,
    )
    summary, summary_path = _summary_of(run)
    report: dict[str, Any] = {
        "trade_date": run.trade_date.isoformat(),
        "exit_code": run.exit_code,
        "wall_seconds": run.wall_seconds,
        "log": str(run.log_path),
        "summary": None if summary_path is None else str(summary_path),
        "refused": refused or (summary or {}).get("refused"),
    }
    if dry is not None:
        report["dry_plan"] = {
            "cannot": dry.get("cannot", []),
            "inputs": {
                label: entry.get("origin") for label, entry in (dry.get("inputs") or {}).items()
            },
        }
        return report
    if summary is None:
        report["verdict"] = "REFUSED" if run.exit_code == 2 else "FAILED (no summary)"
        return report
    chain = summary.get("chain") or {}
    verdict = summary.get("verdict") or {}
    roles = summary.get("roles") or {}
    profile = (summary.get("profile") or {}).get("roles") or {}
    report.update(
        {
            "verdict": (
                "OK" if run.exit_code == 0 else ("REFUSED" if run.exit_code == 2 else "FAILED")
            ),
            "verdict_detail": verdict,
            "production_faithful": (summary.get("mode") or {}).get("production_faithful"),
            "inputs": {
                label: {"origin": entry.get("origin"), "source": entry.get("source")}
                for label, entry in ((summary.get("inputs") or {}).get("provenance") or {}).items()
            },
            "candidates_per_family": summary.get("candidates_per_family"),
            "signals_per_strategy": chain.get("signals_per_strategy"),
            "paper_fills": (chain.get("paper") or {}).get("paper_fill"),
            "notifier_shadow_only": verdict.get("notifier_shadow_only"),
            "serving_same_day": verdict.get("same_day_serving_generation"),
            "serving_signals_today": (chain.get("serving") or {}).get("signals_rows_today"),
            "stage_seconds": summary.get("stage_seconds"),
            "roles": {
                label: {
                    "rounds": role.get("iterations"),
                    "failures": role.get("total_failures"),
                    "errors": len(role.get("errors") or {}),
                    "crashes": len(role.get("crashes") or ()),
                    "first_output_at": role.get("first_output_at"),
                    "step_p50": (profile.get(label) or {}).get("p50"),
                    "step_p95": (profile.get(label) or {}).get("p95"),
                }
                for label, role in roles.items()
            },
        }
    )
    return report


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, dict):
        return " ".join(f"{key}={_cell(item)}" for key, item in sorted(value.items())) or "-"
    return str(value).replace("|", "/")


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Route A day replays ({report['created_at']})",
        "",
        f"days: {len(report['days'])}, parallel: {report['parallel']}, "
        f"exit: {report['exit_code']}; passed to each day: "
        f"`{' '.join(report['passthrough']) or '-'}`",
        "",
        "| date | verdict | exit | inputs synthesized | candidates | signals | paper fills "
        "| shadow only | serving same day | failing roles | wall s |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for day in report["days"]:
        synthesized = sorted(
            label
            for label, entry in (day.get("inputs") or {}).items()
            if entry.get("origin") == "synthesized"
        )
        if "dry_plan" in day:
            synthesized = sorted(
                label
                for label, origin in day["dry_plan"]["inputs"].items()
                if origin == "synthesized"
            )
        failing = sorted(
            label
            for label, role in (day.get("roles") or {}).items()
            if role.get("failures") or role.get("crashes")
        )
        verdict = day.get("verdict") or ("PLAN OK" if day.get("exit_code") == 0 else "CANNOT")
        lines.append(
            "| "
            + " | ".join(
                _cell(value)
                for value in (
                    day["trade_date"],
                    verdict,
                    day.get("exit_code"),
                    ", ".join(synthesized) or "none",
                    day.get("candidates_per_family"),
                    day.get("signals_per_strategy"),
                    day.get("paper_fills"),
                    day.get("notifier_shadow_only"),
                    day.get("serving_same_day"),
                    ", ".join(failing) or "none",
                    day.get("wall_seconds"),
                )
            )
            + " |"
        )
    refusals = [
        (day["trade_date"], reason)
        for day in report["days"]
        for reason in (
            (day.get("dry_plan") or {}).get("cannot")
            or ([day["refused"]] if day.get("refused") else [])
        )
    ]
    if refusals:
        lines += ["", "## Refused or cannot be produced", ""]
        lines += [f"- {day}: {reason}" for day, reason in refusals]
    role_rows = [
        (day["trade_date"], label, role)
        for day in report["days"]
        for label, role in sorted((day.get("roles") or {}).items())
    ]
    if role_rows:
        lines += [
            "",
            "## Per role",
            "",
            "| date | role | rounds | failures | errors | crashes | first output | step p50 s "
            "| step p95 s |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for day, label, role in role_rows:
            first = role.get("first_output_at")
            lines.append(
                "| "
                + " | ".join(
                    _cell(value)
                    for value in (
                        day,
                        label,
                        role.get("rounds"),
                        role.get("failures"),
                        role.get("errors"),
                        role.get("crashes"),
                        str(first)[11:19] if first else None,
                        role.get("step_p50"),
                        role.get("step_p95"),
                    )
                )
                + " |"
            )
    return "\n".join(lines) + "\n"


def combined_exit_code(runs: Sequence[DayRun]) -> int:
    codes = [run.exit_code for run in runs]
    if any(code not in (0, 2) for code in codes):
        return 1
    return 2 if any(code == 2 for code in codes) else 0


# ---------------------------------------------------------------------------------------
# The command
# ---------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        epilog="Everything after `--` is passed to every day's route_a_day_replay.py.",
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--trade-dates", help="comma-separated YYYY-MM-DD dates")
    which.add_argument(
        "--last-n-sessions",
        type=int,
        help="the N open sessions before --before, from the newest host calendar generation",
    )
    parser.add_argument(
        "--before",
        default=None,
        help="with --last-n-sessions: sessions strictly before this date (default: today)",
    )
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--parallel", type=int, default=2)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--replica", type=Path, default=DEFAULT_REPLICA)
    parser.add_argument("--production-inputs", type=Path, default=DEFAULT_PRODUCTION_INPUTS)
    parser.add_argument(
        "--tushare-cache",
        type=Path,
        default=None,
        help="shared by every day (default: <replay-root>/tushare-cache)",
    )
    parser.add_argument("--dry-plan", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--day-script", type=Path, default=DAY_SCRIPT, help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None, *, out: Callable[[str], None] = print) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if "--" in raw:
        split = raw.index("--")
        own, passthrough = raw[:split], raw[split + 1 :]
    else:
        own, passthrough = raw, []
    parser = build_parser()
    arguments = parser.parse_args(own)
    try:
        return run(arguments, passthrough=passthrough, out=out)
    except DaysRefusedError as error:
        out(f"DAYS REFUSED: {error}")
        return 2


def run(
    arguments: argparse.Namespace,
    *,
    passthrough: Sequence[str],
    out: Callable[[str], None] = print,
) -> int:
    import route_a_day_replay as day_script

    if arguments.parallel < 1:
        raise DaysRefusedError("--parallel must be at least 1")
    if arguments.parallel > max(1, (os.cpu_count() or 1) - 1):
        out(
            f"WARNING: --parallel {arguments.parallel} on {os.cpu_count()} cores leaves "
            "production less than one core"
        )
    for flag in ("--trade-date", "--replay-root"):
        if flag in passthrough:
            raise DaysRefusedError(f"{flag} is the runner's own; do not pass it after --")
    runtime_root = arguments.runtime_root.resolve()
    replica = arguments.replica.resolve()
    production_inputs = arguments.production_inputs.resolve()
    replay_root = arguments.replay_root.resolve()
    cache_root = (arguments.tushare_cache or replay_root / "tushare-cache").resolve()
    protected = day_script.production_roots(runtime_root, replica, production_inputs)
    try:
        for label, path in (("replay root", replay_root), ("Tushare cache", cache_root)):
            day_script.refuse_overlap(label, path, protected)
    except day_script.ReplayRefusedError as error:
        raise DaysRefusedError(str(error)) from error
    #: the day script's own hook: nothing this process does may write under production
    day_script._PROTECTED_ROOTS = tuple(protected)  # noqa: SLF001 - the same hook, shared
    sys.addaudithook(day_script._audit_hook)  # noqa: SLF001
    audit = day_script.ProductionAudit(roots=())

    if arguments.trade_dates:
        days = parse_trade_dates(arguments.trade_dates)
    else:
        before = (
            date.fromisoformat(arguments.before)
            if arguments.before
            else datetime.now(_SHANGHAI).date()
        )
        days = last_sessions(
            runtime_root, arguments.last_n_sessions, before=before, read_bytes=audit.read_bytes
        )
    stamp = datetime.now(_SHANGHAI).strftime("%Y%m%dT%H%M%S")
    root = replay_root / f"days-{stamp}"
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    runs = [
        DayRun(
            trade_date=day,
            replay_root=root / day.isoformat(),
            log_path=root / f"{day.isoformat()}.log",
            argv=day_argv(
                python=sys.executable,
                script=arguments.day_script.resolve(),
                trade_date=day,
                replay_root=root / day.isoformat(),
                runtime_root=runtime_root,
                replica=replica,
                production_inputs=production_inputs,
                cache_root=cache_root,
                dry_plan=arguments.dry_plan,
                passthrough=passthrough,
            ),
        )
        for day in days
    ]
    out(
        f"replaying {len(runs)} day(s) {[day.isoformat() for day in days]} under {root}, "
        f"{arguments.parallel} at a time"
    )
    run_days(runs, parallel=arguments.parallel, poll_seconds=arguments.poll_seconds, log=out)
    exit_code = combined_exit_code(runs)
    report = {
        "created_at": datetime.now(_SHANGHAI).isoformat(timespec="seconds"),
        "root": str(root),
        "parallel": arguments.parallel,
        "dry_plan": bool(arguments.dry_plan),
        "passthrough": list(passthrough),
        "tushare_cache": str(cache_root),
        "exit_code": exit_code,
        "days": [day_report(run) for run in runs],
    }
    audited = audit.finish()
    report["production_untouched"] = not (
        audited["changed_during_our_read"] or audited["changed_in_place_later"]
    )
    (root / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    markdown = render_markdown(report)
    (root / "report.md").write_text(markdown, encoding="utf-8")
    day_script._PROTECTED_ROOTS = ()  # noqa: SLF001
    out(markdown)
    out(f"report: {root / 'report.json'}")
    out(f"report: {root / 'report.md'}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
