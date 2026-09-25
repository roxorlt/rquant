"""Replay one recorded trading day through the real Route A roles, in a private sandbox (AH).

The trading-day e2e (`tests/integration/test_route_a_trading_day_full_chain_e2e.py`) runs the
chain's second half once, at one instant, over three codes of fixture data. This replays a
whole recorded session instead: the host's own sealed reference batch, auction-match batch,
market calendar, runtime inputs and read-only replica are copied (read only) into a fresh
private sandbox, the world the e2e builds (installed generation, staged and published
authority chain, credentials laid out the way systemd lays them out) is built around them,
and an injected clock walks the day from 09:15 to 15:05 while every role on the signal path
takes one real step per tick, in dependency order:

    reference_slow_publisher -> candidate_publisher x3 -> market_minute_source ->
    paper_constraint_publisher -> feature_live -> strategy_live x3 -> signal_router ->
    paper_broker -> notifier (shadow) -> runtime_health_publisher -> serving_publisher

Every role is the real role: built by the real builder from the real manifest through the
real `runtime_service_main.run`, entering the real service loop. What is replaced is named:

* **the clock** -- every role's registry clock and heartbeat clock read the replay clock;
* **the minute source's transport** -- `market_minute_source` asks its adapter's `rt_min`
  for the watchlist its own candidate-universe loader computed; the adapter answers with the
  latest *complete* recorded bar of that code (replica `minute_bar`, else Tushare
  `stk_mins` for the day with `--tushare`), never a bar the clock has not reached;
* **the two upstream sources** -- `reference_slow_source` and `auction_match_source` are not
  run: their recorded batches are re-sealed under the sandbox commit and throwaway keys; on a
  day the host holds no recording of (or with `--synthesize-sources`), the batches are
  *synthesized* by the live capture code from the replica as it stood before the session and
  from Tushare's historical endpoints (`route_a_replay_sources.py` says exactly how), and
  `summary.json` labels every input recorded or synthesized, with its source;
* **the service loop's wait** -- one iteration per tick, handed out by the driver, instead of
  the manifest interval;
* **the unit sandbox** -- roles run as threads of this process, one at a time, without
  systemd, namespaces, `ReadWritePaths` or the package-L Python sandbox (see the report).

Nothing outside the sandbox is written. Production files are opened read-only (plain
`open(..., "rb")`, DuckDB `read_only=True` through `ATTACH ... (READ_ONLY)`), an audit hook
refuses any Python-level write under a production root, and every production path read is
stat'ed before, right after and at the end of the run. Tushare answers are kept in a cache
under the replay root (`--tushare-cache`), never anywhere else.

    PYTHONDONTWRITEBYTECODE=1 <checkout>/.venv/bin/python \\
        <checkout>/scripts/route_a_day_replay.py --trade-date 2026-09-24 \\
        --replay-root /home/lighthouse/replay [--tushare] [--until 11:30]

Several days, each in its own process: `scripts/route_a_replay_days.py`.

Exit status: 0 when a same-day serving generation exists at the end (even with no signal);
1 when none does, or when any role crashed (its thread ended with an exception); 2 on a
usage error or a refused setup -- including a day for which some input cannot be produced,
which is named in the message; no day ever borrows another day's batch.
"""

from __future__ import annotations

import argparse
import cProfile
import hashlib
import io
import json
import os
import pstats
import secrets
import shutil
import sqlite3
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import closing, suppress
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
for _entry in (REPO_ROOT, REPO_ROOT / "scripts", REPO_ROOT / "src"):
    if str(_entry) not in sys.path:
        sys.path.insert(0, str(_entry))

import route_a_replay_sources as sources  # noqa: E402 - needs `scripts/` on the path

_SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_RUNTIME_ROOT = Path("/home/lighthouse/rquant/data/runtime")
DEFAULT_REPLICA = Path("/home/lighthouse/rquant/data/rquant_ro.duckdb")
DEFAULT_PRODUCTION_INPUTS = Path("/home/lighthouse/rquant/data/runtime-production-inputs.json")
_REFERENCE_SOURCE_KEY_ID = "reference-source-v1"
#: the v0.33.20 source promised `prepared_at + 5 s`; its batches say when they were prepared
_OLD_SOURCE_GUARD = timedelta(seconds=5)

#: The chain, in dependency order. Each entry is a role of `PRODUCTION_ROLE_POLICY`; a role
#: with several instances (candidate publishers, strategies) runs every instance in turn.
CHAIN: tuple[str, ...] = (
    "reference_slow_publisher",
    "candidate_publisher",
    "market_minute_source",
    "paper_constraint_publisher",
    "feature_live",
    "strategy_live",
    "signal_router",
    "paper_broker",
    "notifier",
    "runtime_health_publisher",
    "serving_publisher",
)
#: candidate publishers in the order their downstream wants them: auction_gap first
_CANDIDATE_ORDER = ("auction_gap", "n_shape", "growth_board_surge")


class ReplayRefusedError(RuntimeError):
    """Setup refused; the replay exits 2 with this message."""


def _local(day: date, value: str | clock_time) -> datetime:
    parsed = value if isinstance(value, clock_time) else clock_time.fromisoformat(value)
    return datetime.combine(day, parsed, tzinfo=_SHANGHAI).astimezone(UTC)


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset, tuple)):
        return sorted(value) if isinstance(value, (set, frozenset)) else list(value)
    return str(value)


# ---------------------------------------------------------------------------------------
# Reading production: read only, audited
# ---------------------------------------------------------------------------------------


def _stat_tuple(path: Path) -> tuple[int, int, int, int] | None:
    try:
        observed = path.stat()
    except OSError:
        return None
    return (observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns)


@dataclass
class ProductionAudit:
    """Every production file this run read, stat'ed around the read and at the end."""

    roots: tuple[Path, ...]
    reads: dict[str, dict[str, Any]] = field(default_factory=dict)

    def before(self, path: Path) -> None:
        entry = self.reads.setdefault(str(path), {"before": _stat_tuple(path)})
        entry.setdefault("before", _stat_tuple(path))

    def after(self, path: Path) -> None:
        self.reads[str(path)]["after_read"] = _stat_tuple(path)

    def read_bytes(self, path: Path) -> bytes:
        self.before(path)
        with open(path, "rb") as handle:
            payload = handle.read()
        self.after(path)
        return payload

    def finish(self) -> dict[str, Any]:
        """Classify each path: unchanged, replaced by its own producer, or changed in place.

        Our reads are `open(rb)` and DuckDB `READ_ONLY`, so the only change this run could
        make is an in-place one on a read path; a *different inode* at the end is the file's
        own producer replacing it (the replica is re-synced every five minutes) and is
        reported, not blamed.
        """

        in_place: list[str] = []
        replaced: list[str] = []
        for path, entry in self.reads.items():
            entry["end"] = _stat_tuple(Path(path))
            before = entry.get("before")
            during = entry.get("after_read")
            if before != during:
                in_place.append(path)
                continue
            end = entry["end"]
            if end == before:
                continue
            if before is None or end is None or end[:2] != before[:2]:
                replaced.append(path)
            else:
                in_place.append(path)
        return {
            "paths_read": len(self.reads),
            "changed_during_our_read": [
                p
                for p in in_place
                if self.reads[p].get("before") != self.reads[p].get("after_read")
            ],
            "changed_in_place_later": [
                p
                for p in in_place
                if self.reads[p].get("before") == self.reads[p].get("after_read")
            ],
            "replaced_by_producer_later": replaced,
            "reads": self.reads,
        }


_PROTECTED_ROOTS: tuple[str, ...] = ()
_WRITE_EVENTS = frozenset(
    {
        "os.mkdir",
        "os.remove",
        "os.rename",
        "os.rmdir",
        "os.symlink",
        "os.link",
        "os.chmod",
        "os.chown",
        "os.utime",
        "os.truncate",
        "shutil.rmtree",
        "shutil.move",
        "shutil.copyfile",
    }
)


def _is_protected(value: object) -> bool:
    if isinstance(value, bytes):
        value = os.fsdecode(value)
    if not isinstance(value, (str, os.PathLike)):
        return False
    try:
        candidate = os.path.abspath(os.fspath(value))
    except (TypeError, ValueError):
        return False
    return any(
        candidate == root or candidate.startswith(root + os.sep) for root in _PROTECTED_ROOTS
    )


def _audit_hook(event: str, arguments: tuple[Any, ...]) -> None:
    """Refuse every Python-level write under a production root, whoever asks for it.

    It cannot see a write made from C (SQLite, DuckDB, pyarrow), which is why no rquant API
    is ever pointed at a production path: production is read with plain `open(rb)` and one
    DuckDB `ATTACH ... (READ_ONLY)`, and everything else runs on the sandbox copies.
    """

    if not _PROTECTED_ROOTS:
        return
    if event == "open" and arguments:
        path, mode, flags = (list(arguments) + [None, None])[:3]
        writing = False
        if isinstance(mode, str) and any(letter in mode for letter in "wax+"):
            writing = True
        if isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC):
            writing = True
        if writing and _is_protected(path):
            raise PermissionError(f"route_a_day_replay refuses to write production path {path}")
    elif event in _WRITE_EVENTS and arguments:
        for value in arguments[:2]:
            if _is_protected(value):
                raise PermissionError(
                    f"route_a_day_replay refuses {event} on production path {value}"
                )
    elif event == "sqlite3.connect" and arguments:
        database = arguments[0]
        text = os.fsdecode(database) if isinstance(database, bytes) else str(database)
        if text.startswith("file:"):
            location = text[5:].split("?", 1)[0]
            if _is_protected(location) and "mode=ro" not in text:
                raise PermissionError(f"route_a_day_replay refuses a writable open of {text}")
        elif _is_protected(text):
            raise PermissionError(f"route_a_day_replay refuses a writable open of {text}")


# ---------------------------------------------------------------------------------------
# What the host recorded
# ---------------------------------------------------------------------------------------


@dataclass
class RecordedDay:
    """The day's inputs: what the host recorded, and which of them the replay synthesizes.

    A recorded batch is kept even when it is synthesized over (`--synthesize-sources`): the
    replay then reports how far the synthesized one is from it (`fidelity_vs_recorded`).
    """

    trade_date: date
    calendar: Any
    calendar_bytes: bytes
    calendar_path: Path | None
    universe: dict[str, Any] | None
    routing_policy: bytes
    trade_calendar: bytes
    history: bytes
    history_source: Path
    inputs_fingerprints: dict[str, dict[str, str | bool]]
    reference_envelope: Any = None
    reference_snapshot: Any = None
    reference_manifest_path: Path | None = None
    auction_records: list[tuple[Any, bytes, Path]] = field(default_factory=list)
    auction_target: Any = None
    synthesize_reference: bool = False
    synthesize_auction: bool = False
    provenance: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: why a synthesis needs Tushare that this run has not got (the dry plan reports them)
    tushare_needed: list[str] = field(default_factory=list)


def _envelopes(directory: Path, audit: ProductionAudit) -> list[tuple[Any, Path]]:
    from rquant.live_contracts import BatchEnvelope

    found: list[tuple[Any, Path]] = []
    if not directory.is_dir():
        return found
    for manifest in sorted(directory.glob("*.json")):
        if not manifest.name[:-5].isdigit():
            continue
        found.append((BatchEnvelope.model_validate_json(audit.read_bytes(manifest)), manifest))
    return found


REFERENCE_BATCHES = Path("live") / "reference-slow" / "batches" / "reference_slow"
AUCTION_BATCHES = Path("live") / "auction-match" / "batches" / "auction_match"


def _read_reference(
    runtime_root: Path,
    trade_date: date,
    sequence: int | None,
    audit: ProductionAudit,
) -> tuple[tuple[Any, Any, Path] | None, list[str]]:
    """The host batch that targets the day, or None; and what the spool holds."""

    from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
    from rquant.strict_json import strict_model_validate_canonical_json

    directory = runtime_root / REFERENCE_BATCHES
    candidates = _envelopes(directory, audit)
    if sequence is not None:
        candidates = [item for item in candidates if item[0].sequence == sequence]
    seen: list[str] = []
    for envelope, manifest in sorted(candidates, key=lambda item: -item[0].sequence):
        payload = audit.read_bytes(manifest.with_suffix(".payload"))
        if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
            raise ReplayRefusedError(f"{manifest}: payload does not match content_sha256")
        snapshot = strict_model_validate_canonical_json(ReferenceSlowSourceSnapshot, payload)
        if snapshot.content_sha256 != envelope.batch_id:
            raise ReplayRefusedError(f"{manifest}: payload does not match batch_id")
        seen.append(f"seq {envelope.sequence} targets {snapshot.target_trade_date}")
        if snapshot.target_trade_date == trade_date:
            return (envelope, snapshot, manifest), seen
    return None, seen


def _read_auction(
    runtime_root: Path,
    trade_date: date,
    sequence: int | None,
    audit: ProductionAudit,
) -> tuple[tuple[list[tuple[Any, bytes, Path]], Any] | None, list[str]]:
    """The host's PUBLISHED batch for the day and every batch before it, or None."""

    from rquant.live_contracts import BatchQualityStatus

    directory = runtime_root / AUCTION_BATCHES
    envelopes = _envelopes(directory, audit)
    seen = [
        f"seq {envelope.sequence} {envelope.event_time_end.astimezone(_SHANGHAI).date()} "
        f"{envelope.quality_status.value} rows={envelope.row_count}"
        for envelope, _ in sorted(envelopes, key=lambda item: item[0].sequence)
    ]
    today = [
        envelope
        for envelope, _ in envelopes
        if envelope.event_time_end.astimezone(_SHANGHAI).date() == trade_date
        and envelope.quality_status is BatchQualityStatus.PUBLISHED
        and (sequence is None or envelope.sequence == sequence)
    ]
    if not today:
        return None, seen
    target = max(today, key=lambda envelope: envelope.sequence)
    records: list[tuple[Any, bytes, Path]] = []
    for envelope, manifest in sorted(envelopes, key=lambda item: item[0].sequence):
        if envelope.sequence > target.sequence:
            break
        payload = audit.read_bytes(manifest.with_suffix(".payload"))
        if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
            raise ReplayRefusedError(f"{manifest}: payload does not match content_sha256")
        records.append((envelope, payload, manifest))
    return (records, target), seen


def _read_universe(
    runtime_root: Path, trade_date: date, audit: ProductionAudit
) -> dict[str, Any] | None:
    directory = runtime_root / "authorities" / "auction-universe" / "generations"
    if not directory.is_dir():
        return None
    best: dict[str, Any] | None = None
    for path in sorted(directory.glob("*.json")):
        try:
            document = json.loads(audit.read_bytes(path))
        except (OSError, ValueError):
            continue
        if not isinstance(document, dict) or document.get("effective_trade_date") != str(
            trade_date
        ):
            continue
        if best is None or str(document.get("available_at")) > str(best.get("available_at")):
            best = {**document, "_path": str(path)}
    return best


#: what `--tushare` / `--tushare-offline` unlock, said the same way in every refusal
_TUSHARE_HINT = (
    "synthesizing it needs Tushare: pass --tushare (with TUSHARE_TOKEN_MAIN in the "
    "environment) or --tushare-offline with a filled --tushare-cache"
)


def history_sessions_before(payload: bytes, trade_date: date) -> dict[str, Any]:
    """How many sessions of the sealed minute history precede the day (only
    `trade_time` is read)."""

    import pyarrow.compute as compute
    import pyarrow.parquet as parquet

    table = parquet.read_table(io.BytesIO(payload), columns=["trade_time"])
    if table.num_rows == 0:
        return {"sessions_before_trade_date": 0, "sessions_total": 0}
    days = {
        (value.date() if isinstance(value, datetime) else value)
        for value in compute.unique(table.column("trade_time")).to_pylist()
    }
    return {
        "sessions_before_trade_date": sum(1 for day in days if day < trade_date),
        "sessions_total": len(days),
        "sessions_on_or_after_trade_date_dropped": sum(1 for day in days if day >= trade_date),
    }


def read_recorded_day(
    *,
    runtime_root: Path,
    production_inputs: Path,
    trade_date: date,
    reference_sequence: int | None,
    auction_sequence: int | None,
    audit: ProductionAudit,
    synthesize: bool = False,
    tushare: bool = False,
    refuse_without_tushare: bool = True,
) -> RecordedDay:
    """What the host recorded for the day, and which source batch the replay synthesizes.

    A batch is synthesized when the host holds none for the day, or with `synthesize`
    (`--synthesize-sources`). Synthesizing needs Tushare; without it the day is refused here,
    with the exact spool contents, before anything is built (or, for the dry plan, the
    reasons are kept in `tushare_needed` and every other input is still resolved). The
    calendar is the one the recorded reference batch names, or -- for a synthesized one --
    the generation the host had installed before the day opened
    (`route_a_replay_sources.choose_calendar`).
    """

    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.strict_json import strict_json_loads

    reference, reference_seen = _read_reference(runtime_root, trade_date, reference_sequence, audit)
    auction, auction_seen = _read_auction(runtime_root, trade_date, auction_sequence, audit)
    synthesize_reference = synthesize or reference is None
    synthesize_auction = synthesize or auction is None
    missing: list[str] = []
    if synthesize_reference and not tushare:
        missing.append(
            "reference_slow: "
            + (
                f"no reference-slow batch in {runtime_root / REFERENCE_BATCHES} targets "
                f"{trade_date} (the spool holds: {reference_seen or 'nothing'})"
                if reference is None
                else "--synthesize-sources"
            )
            + f"; {_TUSHARE_HINT}"
        )
    if synthesize_auction and not tushare:
        missing.append(
            "auction_match: "
            + (
                f"no PUBLISHED auction-match batch for {trade_date} in "
                f"{runtime_root / AUCTION_BATCHES} (the spool holds: {auction_seen or 'nothing'})"
                if auction is None
                else "--synthesize-sources"
            )
            + f"; {_TUSHARE_HINT}"
        )
    if missing and refuse_without_tushare:
        raise ReplayRefusedError(" | ".join(missing))

    provenance: dict[str, dict[str, Any]] = {}
    #: A recorded day keeps the calendar its recorded batch names even when the batches are
    #: synthesized over it: `--synthesize-sources` measures the batches, and the calendar the
    #: host really used is known. The choice a day without a recording gets is still run, and
    #: reported as `heuristic_check`, so the recorded day also tests that choice.
    if reference is not None:
        envelope, snapshot, _manifest_path = reference
        calendar_sha = snapshot.source_snapshot_ids["calendar"]
        calendar_path: Path | None = (
            runtime_root
            / "authorities"
            / "market-calendar"
            / "generations"
            / f"{calendar_sha}.json"
        )
        calendar_bytes = audit.read_bytes(calendar_path)
        #: the loader's own decoding (`load_market_calendar_authority`), not a canonical-form one
        calendar = MarketCalendarAuthority.model_validate(strict_json_loads(calendar_bytes))
        if calendar.content_sha256 != calendar_sha:
            raise ReplayRefusedError(
                "the calendar generation does not match the batch's calendar id"
            )
        provenance["calendar"] = {
            "origin": "recorded",
            "source": str(calendar_path),
            "generated_at": calendar.generated_at,
            "why": "the generation the recorded reference-slow batch names",
        }
        if synthesize_reference:
            try:
                check = sources.choose_calendar(
                    runtime_root=runtime_root, trade_date=trade_date, audit=audit
                )
            except sources.InputUnavailableError as error:
                provenance["calendar"]["heuristic_check"] = {"refused": str(error)}
            else:
                provenance["calendar"]["heuristic_check"] = {
                    "would_pick": check.calendar.content_sha256,
                    "would_pick_origin": check.provenance["origin"],
                    "matches_recorded": check.calendar.content_sha256 == calendar.content_sha256,
                }
    else:
        try:
            choice = sources.choose_calendar(
                runtime_root=runtime_root, trade_date=trade_date, audit=audit
            )
        except sources.InputUnavailableError as error:
            raise ReplayRefusedError(f"calendar: {error}") from error
        calendar, calendar_bytes, calendar_path = choice.calendar, choice.payload, choice.path
        provenance["calendar"] = choice.provenance
    if trade_date not in calendar.open_dates:
        raise ReplayRefusedError(
            f"{trade_date} is not an open date of calendar {calendar.content_sha256}"
        )
    universe = _read_universe(runtime_root, trade_date, audit)

    if reference is not None:
        provenance["reference_slow"] = {
            "origin": "synthesized" if synthesize_reference else "recorded",
            "source": (
                "the live capture over the replica before the session + Tushare "
                "stock_basic / stock_st / adj_factor / suspend_d"
                if synthesize_reference
                else str(reference[2])
            ),
            "recorded_batch": str(reference[2]),
            "recorded_sequence": reference[0].sequence,
            "recorded_captured_at": reference[1].captured_at,
        }
    else:
        provenance["reference_slow"] = {
            "origin": "synthesized",
            "source": (
                "the live capture over the replica before the session + Tushare "
                "stock_basic / stock_st / adj_factor / suspend_d"
            ),
            "recorded_batch": None,
            "spool_holds": reference_seen,
        }
    provenance["auction_match"] = {
        "origin": "synthesized" if synthesize_auction else "recorded",
        "source": (
            "the live gateway capture over Tushare stk_auction"
            if synthesize_auction
            else str(auction[0][-1][2])
        ),
        "recorded_batch": None if auction is None else str(auction[0][-1][2]),
        "spool_holds": auction_seen,
    }
    provenance["auction_universe"] = (
        {
            "origin": "recorded",
            "source": universe.get("_path"),
            "codes": len(universe.get("codes", ())),
        }
        if universe is not None
        else {
            "origin": "synthesized" if synthesize_auction else "absent",
            "source": (
                "the live universe publisher over the replica's prior-session daily_bar"
                if synthesize_auction
                else "no host generation for the day (only reported, never read by the chain)"
            ),
        }
    )

    document = json.loads(audit.read_bytes(production_inputs))
    fingerprints: dict[str, dict[str, str | bool]] = {}
    loaded: dict[str, tuple[bytes, Path]] = {}
    for label, path_key, sha_key in (
        ("routing_policy", "routing_policy_path", "routing_policy_fingerprint"),
        ("trade_calendar", "trade_calendar_path", "trade_calendar_sha256"),
        ("history", "historical_minutes_snapshot_path", "historical_minutes_snapshot_id"),
    ):
        if not isinstance(document, dict) or not document.get(path_key):
            keys = sorted(document) if isinstance(document, dict) else type(document).__name__
            raise ReplayRefusedError(f"{production_inputs} names no {path_key} (keys: {keys})")
        source = Path(str(document[path_key]))
        payload = audit.read_bytes(source)
        observed = hashlib.sha256(payload).hexdigest()
        fingerprints[label] = {
            "path": str(source),
            "recorded": str(document.get(sha_key)),
            "observed": observed,
            "matches": observed == document.get(sha_key),
        }
        loaded[label] = (payload, source)
    history = history_sessions_before(loaded["history"][0], trade_date)
    if not history["sessions_before_trade_date"]:
        raise ReplayRefusedError(
            f"history: the sealed minute history {loaded['history'][1]} holds no session before "
            f"{trade_date} ({history['sessions_total']} sessions, all on or after it); "
            "feature_live would start without any prior minutes"
        )
    from rquant.intraday_feature_engine import IntradayFeatureConfig

    for label in ("routing_policy", "trade_calendar"):
        provenance[label] = {"origin": "recorded", "source": str(loaded[label][1])}
    provenance["history"] = {
        "origin": "recorded",
        "source": str(loaded["history"][1]),
        **history,
        #: the same-clock medians use up to this many prior sessions
        "lookback_sessions": IntradayFeatureConfig.model_fields["lookback_sessions"].default,
        "note": "rows on or after the trade date are dropped; fewer prior sessions than the "
        "lookback make the same-clock medians use the sessions there are",
    }
    return RecordedDay(
        trade_date=trade_date,
        reference_envelope=None if reference is None else reference[0],
        reference_snapshot=None if reference is None else reference[1],
        reference_manifest_path=None if reference is None else reference[2],
        calendar=calendar,
        calendar_bytes=calendar_bytes,
        calendar_path=calendar_path,
        auction_records=[] if auction is None else auction[0],
        auction_target=None if auction is None else auction[1],
        universe=universe,
        routing_policy=loaded["routing_policy"][0],
        trade_calendar=loaded["trade_calendar"][0],
        history=loaded["history"][0],
        history_source=loaded["history"][1],
        inputs_fingerprints=fingerprints,
        synthesize_reference=synthesize_reference,
        synthesize_auction=synthesize_auction,
        provenance=provenance,
        tushare_needed=missing,
    )


# ---------------------------------------------------------------------------------------
# The read-only replica: a pre-open extract, plus the day's minutes kept aside
# ---------------------------------------------------------------------------------------


_EMPTY_TABLES = {
    "daily_bar": "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)",
    "screen_result": (
        "CREATE TABLE screen_result (trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, "
        "name VARCHAR, close DOUBLE, pct_chg DOUBLE, extra JSON, created_at TIMESTAMP)"
    ),
    "minute_bar": (
        "CREATE TABLE minute_bar (ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, "
        "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE, "
        "source VARCHAR, created_at TIMESTAMP)"
    ),
}


def extract_replica(
    *,
    replica: Path,
    target: Path,
    minutes_target: Path,
    trade_date: date,
    calendar: Any,
    daily_sessions: int,
    minute_sessions: int,
    synced_at: datetime,
    audit: ProductionAudit,
    threads: int = 2,
    memory_limit: str = "2GB",
) -> dict[str, Any]:
    """The replica as it stood before the session opened, and the session's minutes aside.

    `daily_bar` and `screen_result` keep only sessions **before** the trade date: the host's
    replica today already carries the day's own close, and a candidate publisher reading it
    would be reading its own future. `minute_bar` in the extract keeps the prior
    `minute_sessions` sessions (the notifier's page projection requires the table); the
    trade date's own minutes go to a separate parquet the minute replay serves bar by bar.
    """

    import duckdb

    prior = tuple(day for day in calendar.open_dates if day < trade_date)
    daily_from = prior[-daily_sessions] if len(prior) >= daily_sessions else prior[0]
    minute_from = prior[-minute_sessions] if minute_sessions > 0 else trade_date
    next_day = trade_date + timedelta(days=1)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    audit.before(replica)
    counts: dict[str, int] = {}
    connection = duckdb.connect(str(target))
    try:
        #: gentle on a host whose own services keep running beside the replay
        connection.execute(f"SET threads = {int(threads)}")
        connection.execute(f"SET memory_limit = '{memory_limit}'")
        connection.execute(f"ATTACH '{replica}' AS source_replica (READ_ONLY)")
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'source_replica'"
            ).fetchall()
        }
        selections = {
            "daily_bar": (
                "SELECT * FROM source_replica.daily_bar WHERE trade_date >= ? AND trade_date < ?",
                [daily_from, trade_date],
            ),
            "screen_result": (
                "SELECT * FROM source_replica.screen_result "
                "WHERE trade_date >= ? AND trade_date < ?",
                [prior[-10] if len(prior) >= 10 else prior[0], trade_date],
            ),
            "minute_bar": (
                "SELECT * FROM source_replica.minute_bar WHERE trade_time >= ? AND trade_time < ?",
                [
                    datetime.combine(minute_from, clock_time(0)),
                    datetime.combine(trade_date, clock_time(0)),
                ],
            ),
        }
        for table, (query, parameters) in selections.items():
            if table in present:
                connection.execute(f"CREATE TABLE {table} AS {query}", parameters)
            else:
                connection.execute(_EMPTY_TABLES[table])
            counts[table] = int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        if "minute_bar" in present:
            day_minutes = connection.execute(
                "SELECT ts_code, trade_time, freq, open, high, low, close, vol, amount, source "
                "FROM source_replica.minute_bar WHERE trade_time >= ? AND trade_time < ? "
                "AND freq = '1min'",
                [
                    datetime.combine(trade_date, clock_time(0)),
                    datetime.combine(next_day, clock_time(0)),
                ],
            ).fetchdf()
        else:
            day_minutes = None
        latest_daily = (
            connection.execute("SELECT max(trade_date) FROM source_replica.daily_bar").fetchone()[0]
            if "daily_bar" in present
            else None
        )
        connection.execute("DETACH source_replica")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    audit.after(replica)
    wal = Path(f"{target}.wal")
    if wal.exists():
        raise ReplayRefusedError("the replica extract left a WAL behind")
    target.chmod(0o644)
    stamp = synced_at.timestamp()
    os.utime(target, (stamp, stamp))

    import pandas as pd

    frame = (
        day_minutes
        if day_minutes is not None
        else pd.DataFrame(
            columns=[
                "ts_code",
                "trade_time",
                "freq",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
                "source",
            ]
        )
    )
    frame.to_parquet(minutes_target, index=False)
    return {
        "path": str(target),
        "tables_on_host": sorted(present),
        "rows": counts,
        "daily_bar_from": daily_from,
        "daily_bar_latest_on_host": latest_daily,
        "minute_bar_from": minute_from,
        "synced_at": synced_at,
        "trade_date_minute_rows": len(frame),
        "trade_date_minute_codes": int(frame["ts_code"].nunique()) if len(frame) else 0,
        "trade_date_minute_sources": (
            {str(k): int(v) for k, v in frame["source"].value_counts().items()}
            if len(frame)
            else {}
        ),
    }


# ---------------------------------------------------------------------------------------
# The minute replay: the source's transport, answered from the record
# ---------------------------------------------------------------------------------------


_MINUTE_COLUMNS = (
    "ts_code",
    "trade_time",
    "freq",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "source",
)


class ReplayMinuteAdapter:
    """`rt_min` over the recorded day: the latest bar of each asked code the clock has reached.

    A bar is served once `trade_time <= now - lag_seconds`. The real `rt_min` answers with
    the newest bar, in progress, so the default lag of 0 keeps the host's freshness (the
    strategies refuse a market-minute feature more than 60 s old). The price of that is the
    one bar in progress: the host saw it partial, the replay serves its final values, so
    for bar-start labels (the host's `tushare_rt` rows run 09:30..14:59) the newest bar can
    carry up to one minute of the future. `--minute-lag-seconds 60` removes that and makes
    every bar ~60 s older than the host would have had it.
    """

    def __init__(
        self,
        frame: Any,
        *,
        clock: Callable[[], datetime],
        lag_seconds: float,
        tushare_fetch: Callable[[str], Any] | None,
        trade_date: date,
    ) -> None:
        import pandas as pd

        self._clock = clock
        self._lag = timedelta(seconds=lag_seconds)
        self._fetch = tushare_fetch
        self._trade_date = trade_date
        self._bars: dict[str, Any] = {}
        self.sources: dict[str, str] = {}
        self.requested: set[str] = set()
        self.missing: set[str] = set()
        self.tushare_fetched: dict[str, int] = {}
        self.tushare_failed: dict[str, str] = {}
        self.calls = 0
        self.rows_served = 0
        self.last_universe: tuple[str, ...] = ()
        if frame is not None and len(frame):
            frame = frame.copy()
            frame["trade_time"] = pd.to_datetime(frame["trade_time"])
            for code, rows in frame.groupby("ts_code"):
                self._install(str(code), rows, origin="replica")

    def _install(self, code: str, rows: Any, *, origin: str) -> None:
        #: one source per code: two sources label the same minute differently, and mixing
        #: them would put two bars into one minute
        counts = rows["source"].value_counts()
        preference = {"tushare_rt": 0, "tushare": 1}
        chosen = sorted(
            counts.index, key=lambda source: (-int(counts[source]), preference.get(source, 9))
        )[0]
        picked = rows[rows["source"] == chosen].drop_duplicates("trade_time", keep="last")
        self._bars[code] = picked.sort_values("trade_time").reset_index(drop=True)
        self.sources[code] = f"{origin}:{chosen}"

    def _bars_for(self, code: str) -> Any:
        if code in self._bars:
            return self._bars[code]
        if self._fetch is not None and code not in self.tushare_failed:
            try:
                rows = self._fetch(code)
            except Exception as error:  # noqa: BLE001 - recorded, the code stays missing
                self.tushare_failed[code] = f"{type(error).__name__}: {error}"
                return None
            if rows is not None and len(rows):
                import pandas as pd

                rows = rows.copy()
                rows["trade_time"] = pd.to_datetime(rows["trade_time"])
                rows = rows[rows["trade_time"].dt.date == self._trade_date]
                if len(rows):
                    self.tushare_fetched[code] = len(rows)
                    self._install(code, rows, origin="tushare")
                    return self._bars[code]
            self.tushare_failed[code] = "no rows for the trade date"
        return None

    def rt_min(self, codes: list[str], freq: str = "1min") -> Any:
        import pandas as pd

        if freq != "1min":
            raise ValueError(f"the replay serves 1min bars only, not {freq}")
        self.calls += 1
        self.last_universe = tuple(codes)
        cutoff = (self._clock() - self._lag).astimezone(_SHANGHAI).replace(tzinfo=None)
        rows: list[dict[str, Any]] = []
        for code in codes:
            self.requested.add(code)
            bars = self._bars_for(code)
            if bars is None:
                self.missing.add(code)
                continue
            index = int(bars["trade_time"].searchsorted(pd.Timestamp(cutoff), side="right"))
            if index == 0:
                continue
            bar = bars.iloc[index - 1]
            rows.append(
                {
                    "ts_code": code,
                    "trade_time": bar["trade_time"].to_pydatetime(),
                    "freq": "1min",
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "vol": float(bar["vol"]),
                    "amount": float(bar["amount"]),
                    "source": "tushare_rt",
                }
            )
        self.rows_served += len(rows)
        return pd.DataFrame(rows, columns=list(_MINUTE_COLUMNS))

    def fetched_frame(self) -> Any:
        import pandas as pd

        frames = [
            self._bars[code].assign(replay_origin=self.sources[code])
            for code in sorted(self._bars)
            if self.sources.get(code, "").startswith("tushare:")
        ]
        return pd.concat(frames, ignore_index=True) if frames else None


# ---------------------------------------------------------------------------------------
# The clock and the role runner
# ---------------------------------------------------------------------------------------


class ReplayClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


_THREAD = threading.local()


class TickBaton:
    """The service loop's stop event, turned into a one-iteration-per-tick hand-off.

    `run_service_loop` calls `wait()` once after every iteration (the replay makes
    `_wait_for_stop` a single wait). That call reports the iteration done and blocks until
    the driver hands out the next tick, or asks the role to stop.
    """

    def __init__(self, profiler: cProfile.Profile | None = None) -> None:
        self._grant = threading.Semaphore(0)
        self.done = threading.Semaphore(0)
        self._stopped = False
        self.iterations = 0
        #: `--cprofile`: on only while this role's own iteration runs, so the profile holds
        #: this role alone whether the interpreter profiles per thread (3.11) or not
        self.profiler = profiler

    def is_set(self) -> bool:
        return self._stopped

    def set(self) -> None:
        self._stopped = True
        self._grant.release()

    def grant(self) -> None:
        self._grant.release()

    def wait(self, timeout: float | None = None) -> bool:  # noqa: ARG002 - ticks, not time
        self.iterations += 1
        if self.profiler is not None:
            self.profiler.disable()
        self.done.release()
        self._grant.acquire()
        if self.profiler is not None and not self._stopped:
            self.profiler.enable()
        return self._stopped


class _NoSignalWatcher:
    active = True
    unarmed_signums: tuple[int, ...] = ()

    def __enter__(self) -> _NoSignalWatcher:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


@dataclass
class RoleState:
    role: str
    instance: str
    service_id: str
    label: str
    argv: list[str]
    environment: dict[str, str]
    control_root: Path
    manifest: Any
    baton: TickBaton | None = None
    thread: threading.Thread | None = None
    crash: str | None = None
    crashes: list[dict[str, Any]] = field(default_factory=list)
    iterations: int = 0
    wall_seconds: float = 0.0
    max_iteration_seconds: float = 0.0
    errors: dict[str, dict[str, Any]] = field(default_factory=dict)
    degraded: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: the cause a degraded round gave (`degraded_detail`), and the last informational
    #: counts (`observations`), both heartbeat file fields since package AI
    degraded_details: dict[str, dict[str, Any]] = field(default_factory=dict)
    observations: dict[str, int] = field(default_factory=dict)
    last_heartbeat: Any = None
    max_processed: int = 0
    first_output_at: datetime | None = None
    #: wall seconds of every step, and the session phase of the tick it ran at
    step_seconds: list[float] = field(default_factory=list)
    step_phases: list[str] = field(default_factory=list)
    #: ticks the driver did not hand this role (`--serving-every-ticks`)
    skipped_ticks: int = 0
    profile_path: Path | None = None


#: The session phases a tick falls in, by local wall time.
_PHASES = (
    ("pre_open", clock_time(9, 30)),
    ("morning", clock_time(11, 30, 59)),
    ("lunch", clock_time(13, 0)),
    ("afternoon", clock_time(15, 0, 59)),
)


def session_phase(moment: datetime) -> str:
    local = moment.astimezone(_SHANGHAI).time()
    for name, end in _PHASES:
        if local < end:
            return name
    return "post_close"


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def step_profile(
    states: Sequence[RoleState], tick_walls: Sequence[tuple[str, float]], drive_seconds: float
) -> dict[str, Any]:
    """Where the drive's wall time went: per role, per phase, and outside every role.

    `harness_seconds` is the drive minus every role's steps -- the driver's own bookkeeping
    (heartbeat reads, serving retention, the clock, the environment swap).
    """

    roles: dict[str, Any] = {}
    phase_totals: dict[str, float] = {}
    for state in states:
        by_phase: dict[str, float] = {}
        for seconds, phase in zip(state.step_seconds, state.step_phases, strict=True):
            by_phase[phase] = by_phase.get(phase, 0.0) + seconds
            phase_totals[phase] = phase_totals.get(phase, 0.0) + seconds
        roles[state.label] = {
            "steps": len(state.step_seconds),
            "total": round(sum(state.step_seconds), 3),
            "p50": round(_percentile(state.step_seconds, 0.5), 4),
            "p95": round(_percentile(state.step_seconds, 0.95), 4),
            "max": round(max(state.step_seconds, default=0.0), 4),
            "by_phase": {phase: round(value, 3) for phase, value in sorted(by_phase.items())},
            "skipped_ticks": state.skipped_ticks,
        }
    role_seconds = sum(sum(state.step_seconds) for state in states)
    tick_by_phase: dict[str, list[float]] = {}
    for phase, seconds in tick_walls:
        tick_by_phase.setdefault(phase, []).append(seconds)
    return {
        "drive_seconds": round(drive_seconds, 2),
        "role_seconds": round(role_seconds, 2),
        "harness_seconds": round(drive_seconds - role_seconds, 2),
        "harness_share": round((drive_seconds - role_seconds) / drive_seconds, 4)
        if drive_seconds
        else 0.0,
        "ticks": len(tick_walls),
        "tick_seconds": {
            phase: {
                "ticks": len(values),
                "total": round(sum(values), 2),
                "p50": round(_percentile(values, 0.5), 3),
                "p95": round(_percentile(values, 0.95), 3),
            }
            for phase, values in sorted(tick_by_phase.items())
        },
        "role_seconds_by_phase": {
            phase: round(value, 2) for phase, value in sorted(phase_totals.items())
        },
        "slowest_roles": [
            label for label, _ in sorted(roles.items(), key=lambda item: -item[1]["total"])[:5]
        ],
        "roles": roles,
    }


def install_runner_patches(
    monkeypatch: Any, clock: ReplayClock, adapter: ReplayMinuteAdapter
) -> None:
    """The five seams the thread runner needs, each a module global of the real code path."""

    import rquant.runtime_service_builtin as builtin_module
    import rquant.runtime_service_control as control_module
    import rquant.runtime_service_main as service_main

    real_registry = builtin_module.build_builtin_registry
    real_manifest_runner = service_main.run_runtime_service_manifest

    def registry(**kwargs: Any) -> Any:
        kwargs.setdefault("clock", clock)
        kwargs.setdefault("adapter_factory", lambda: adapter)
        return real_registry(**kwargs)

    def manifest_runner(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("clock", clock)
        return real_manifest_runner(*args, **kwargs)

    def single_wait(stop_event: Any, delay: float, *, monotonic_clock: Any) -> bool:  # noqa: ARG001
        return stop_event.wait(delay) or stop_event.is_set()

    monkeypatch.setattr(builtin_module, "build_builtin_registry", registry)
    monkeypatch.setattr(service_main, "run_runtime_service_manifest", manifest_runner)
    monkeypatch.setattr(service_main, "Event", lambda: _THREAD.baton)
    #: `signal.signal` is main-thread only; the roles here are threads, and nothing sends
    #: them a signal -- the driver stops them through the baton
    monkeypatch.setattr(
        service_main,
        "signal",
        SimpleNamespace(
            signal=lambda *_args: None,
            getsignal=lambda *_args: None,
            SIGINT=2,
            SIGTERM=15,
        ),
    )
    monkeypatch.setattr(service_main, "StopSignalWatcher", lambda **_kwargs: _NoSignalWatcher())
    monkeypatch.setattr(control_module, "_wait_for_stop", single_wait)


#: What `--assume-listing-classification` is now (package AI). The flag stays accepted so
#: an existing command line keeps working, and does nothing: `reference_slow_publisher`
#: writes `market` / `exchange` / `instrument_class` / `security_class` on every
#: LISTING_STATUS record itself, so nothing is left for a stub to derive.
LISTING_CLASSIFICATION_FLAG_WARNING = (
    "--assume-listing-classification is a no-op since package AI: the reference-slow "
    "publisher writes the four listing-classification fields itself; nothing is stubbed"
)


class RoleRunner:
    """Runs every role as a thread of this process, one role at a time, one step per tick."""

    def __init__(
        self,
        states: list[RoleState],
        *,
        clock: ReplayClock,
        role_timeout_seconds: float,
        max_restarts: int,
        log: Callable[[str], None],
        cprofile: frozenset[str] = frozenset(),
        profile_root: Path | None = None,
    ) -> None:
        self.states = states
        self.clock = clock
        self.role_timeout_seconds = role_timeout_seconds
        self.max_restarts = max_restarts
        self.log = log
        self.hung: str | None = None
        self.cprofile = cprofile
        self.profile_root = profile_root

    def _start(self, state: RoleState) -> None:
        import rquant.runtime_service_main as service_main

        profiler = cProfile.Profile() if state.label in self.cprofile else None
        baton = TickBaton(profiler)
        state.baton = baton
        state.crash = None
        arguments = service_main.build_parser().parse_args(state.argv)

        def target() -> None:
            _THREAD.baton = baton
            if profiler is not None:
                profiler.enable()
            try:
                service_main.run(arguments)
            except BaseException as error:  # noqa: BLE001 - recorded, reported, exit 1
                state.crash = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )
            finally:
                if profiler is not None:
                    profiler.disable()
                    self._dump_profile(state, profiler)
                baton.done.release()

        state.thread = threading.Thread(target=target, name=state.label, daemon=True)
        state.thread.start()

    def _dump_profile(self, state: RoleState, profiler: cProfile.Profile) -> None:
        """`<label>.<start>.pstats`, and the top of it as text beside it."""

        if self.profile_root is None:
            return
        self.profile_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        stem = f"{state.label}.{len(state.crashes)}"
        profiler.dump_stats(str(self.profile_root / f"{stem}.pstats"))
        text = io.StringIO()
        for order in ("cumulative", "tottime"):
            text.write(f"== {state.label}: top 40 by {order} ==\n")
            pstats.Stats(profiler, stream=text).sort_stats(order).print_stats(40)
        state.profile_path = self.profile_root / f"{stem}.txt"
        state.profile_path.write_text(text.getvalue(), encoding="utf-8")

    def _await(self, state: RoleState) -> bool:
        assert state.baton is not None and state.thread is not None
        deadline = time.monotonic() + self.role_timeout_seconds
        while True:
            if state.baton.done.acquire(timeout=0.5):
                return True
            if time.monotonic() > deadline:
                self.hung = (
                    f"{state.label} did not finish its iteration in {self.role_timeout_seconds}s"
                )
                return False

    def step(self, state: RoleState, now: datetime) -> bool:
        """One iteration of `state` at `now`. False means the run must stop (a hang)."""

        self.clock.now = now
        saved = dict(os.environ)
        os.environ.clear()
        os.environ.update(state.environment)
        if "HOME" in saved:
            os.environ["HOME"] = saved["HOME"]
        started = time.perf_counter()
        try:
            alive = state.thread is not None and state.thread.is_alive() and state.crash is None
            if not alive:
                if len(state.crashes) > self.max_restarts:
                    return True
                self._start(state)
            else:
                assert state.baton is not None
                state.baton.grant()
            if not self._await(state):
                return False
        finally:
            os.environ.clear()
            os.environ.update(saved)
        elapsed = time.perf_counter() - started
        state.wall_seconds += elapsed
        state.max_iteration_seconds = max(state.max_iteration_seconds, elapsed)
        state.step_seconds.append(elapsed)
        state.step_phases.append(session_phase(now))
        if state.crash is not None:
            state.crashes.append({"at": now, "traceback": state.crash})
            self.log(
                f"  CRASH {state.label} at {now.astimezone(_SHANGHAI):%H:%M:%S}: "
                f"{state.crash.strip().splitlines()[-1]}"
            )
            if state.thread is not None:
                state.thread.join(timeout=5)
            return True
        state.iterations += 1
        self._observe(state, now)
        return True

    def _observe(self, state: RoleState, now: datetime) -> None:
        from rquant.runtime_service_control import RuntimeServiceControl

        heartbeat = RuntimeServiceControl.read_heartbeat(
            state.control_root, state.manifest.service_spec
        )
        state.last_heartbeat = heartbeat
        if heartbeat is None:
            return
        state.max_processed = max(state.max_processed, int(heartbeat.processed_count))
        if state.first_output_at is None and heartbeat.output_sequence >= 0:
            state.first_output_at = now
        local = now.astimezone(_SHANGHAI).strftime("%H:%M:%S")
        if heartbeat.last_error and heartbeat.consecutive_failures > 0:
            entry = state.errors.setdefault(
                heartbeat.last_error[:600], {"first": local, "count": 0}
            )
            entry["count"] += 1
            entry["last"] = local
        for reason in heartbeat.degraded_reasons or ():
            entry = state.degraded.setdefault(reason[:300], {"first": local, "count": 0})
            entry["count"] += 1
            entry["last"] = local
        detail = getattr(heartbeat, "degraded_detail", None)
        if detail:
            entry = state.degraded_details.setdefault(detail[:600], {"first": local, "count": 0})
            entry["count"] += 1
            entry["last"] = local
        observations = getattr(heartbeat, "observations", None)
        if observations:
            state.observations = dict(observations)

    def stop_all(self) -> None:
        for state in reversed(self.states):
            if state.thread is None or not state.thread.is_alive() or state.baton is None:
                continue
            saved = dict(os.environ)
            os.environ.clear()
            os.environ.update(state.environment)
            if "HOME" in saved:
                os.environ["HOME"] = saved["HOME"]
            try:
                state.baton.set()
                state.thread.join(timeout=60)
            finally:
                os.environ.clear()
                os.environ.update(saved)


# ---------------------------------------------------------------------------------------
# The world
# ---------------------------------------------------------------------------------------


def _backdate(path: Path, when: datetime) -> None:
    stamp = when.timestamp()
    os.utime(path, (stamp, stamp))


def _history_before(payload: bytes, trade_date: date, target: Path) -> tuple[bytes, dict[str, Any]]:
    """The sealed minute history, with nothing from the trade date or later in it.

    Only `trade_time` and `ts_code` are read to check it; the whole frame is loaded (and
    rewritten through the production exporter's own writer) only when rows on or after
    the trade date have to go, which changes the id the profile records.
    """

    import io

    import pyarrow.compute as compute
    import pyarrow.parquet as parquet

    table = parquet.read_table(io.BytesIO(payload), columns=["trade_time", "ts_code"])
    facts: dict[str, Any] = {"rows": table.num_rows, "bytes": len(payload)}
    if table.num_rows == 0:
        return payload, facts
    stamps = table.column("trade_time")
    facts["first"] = compute.min(stamps).as_py()
    facts["last"] = compute.max(stamps).as_py()
    facts["codes"] = len(compute.unique(table.column("ts_code")))
    last = facts["last"]
    last_day = last.date() if isinstance(last, datetime) else last
    if last_day < trade_date:
        return payload, facts
    import pandas as pd
    from export_intraday_snapshot import write_snapshot

    frame = pd.read_parquet(io.BytesIO(payload))
    late = pd.to_datetime(frame["trade_time"]).dt.date >= trade_date
    kept = frame[~late].reset_index(drop=True)
    write_snapshot(kept, target)
    facts["dropped_same_day_or_later_rows"] = int(late.sum())
    return target.read_bytes(), facts


def _schema_rollout_facts(runtime_root: Path, *, receipt: Any) -> dict[str, Any]:
    """Every plan directory under the sandbox's `control/schema-rollouts`, with its phase.

    `plans` counts plan directories, not channels: two installs' plans on the same channel
    are two plans. `phases_by_channel` lists each channel's plans' phases.
    """

    from rquant.runtime_deployment_bundle import load_runtime_schema_rollout

    rollouts = runtime_root / "control" / "schema-rollouts"
    directories = sorted(rollouts.iterdir()) if rollouts.is_dir() else []
    by_channel: dict[str, list[str]] = {}
    for directory in directories:
        authority, store = load_runtime_schema_rollout(
            runtime_root, plan_id=directory.name, read_only=True
        )
        by_channel.setdefault(authority.plan.dataset_id, []).append(
            store.get_state(directory.name).phase.value
        )
    return {
        "plans": len(directories),
        "receipt_plan_ids": len(receipt.schema_rollout_plan_ids),
        "phases_by_channel": {channel: sorted(phases) for channel, phases in by_channel.items()},
    }


@dataclass
class SynthesisInputs:
    """What the two synthesized source batches are made of, prepared before the world,
    and -- once sealed -- what they came out as, for the candidate-input comparison."""

    source: Any
    evidence_database: Path
    evidence: dict[str, Any]
    universe_root: Path
    phase_seconds: float
    reference_snapshot: Any = None
    auction_payload: bytes | None = None


def build_world(
    *,
    sandbox: Path,
    recorded: RecordedDay,
    monkeypatch: Any,
    generations: int,
    replica_extract: Callable[[Path], dict[str, Any]],
    log: Callable[[str], None],
    synthesis: SynthesisInputs | None = None,
) -> tuple[Any, dict[str, Any], dict[str, Path], Any]:
    """The e2e's world (`build_trading_day_chain`), around the host's recorded inputs."""

    import tests.unit.test_runtime_production_profile as profile_fixtures
    from rquant.runtime_deployment_bundle import acknowledge_runtime_schema_rollout_preparation
    from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld, _production_bundle
    from tests.integration.test_route_a_live_chain_idle_e2e import PREVIOUS_COMMIT
    from tests.integration.test_route_a_trading_day_full_chain_e2e import (
        confirm_deliveries_without_the_network,
        deliver_credentials,
    )
    from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority
    from tests.unit.test_runtime_authority_publish import World

    trade_date = recorded.trade_date
    frozen_at = _local(trade_date - timedelta(days=1), "00:00:00")
    facts: dict[str, Any] = {}
    history, facts["history"] = _history_before(
        recorded.history, trade_date, sandbox / "inputs" / "history-before-trade-date.parquet"
    )
    authority = create_shadow_ed25519_test_authority(sandbox / "shadow-completion-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    real_inputs = profile_fixtures._inputs
    frozen = (
        ("routing_policy_path", recorded.routing_policy, 0o444),
        ("trade_calendar_path", recorded.trade_calendar, 0o600),
        ("historical_minutes_snapshot_path", history, 0o600),
    )

    def recorded_inputs(path: Path) -> Any:
        inputs = real_inputs(path)
        for attribute, payload, mode in frozen:
            target = Path(getattr(inputs, attribute))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
            target.chmod(mode)
        return inputs.model_copy(
            update={
                #: the one mode that can never deliver (#281)
                "notifier_delivery_mode": "shadow",
                "shadow_completion_active_key_id": authority.keyring.active_key_id,
                "shadow_completion_active_public_key_pem": public_key_pem,
                "routing_policy_fingerprint": hashlib.sha256(recorded.routing_policy).hexdigest(),
                "trade_calendar_sha256": hashlib.sha256(recorded.trade_calendar).hexdigest(),
                "historical_minutes_snapshot_id": hashlib.sha256(history).hexdigest(),
            }
        )

    monkeypatch.setattr(profile_fixtures, "_inputs", recorded_inputs)
    log("building the world: authority chain, generation(s), credentials")
    world = World(sandbox / "authority-root", monkeypatch).build()
    runtime_root = sandbox / "host" / "data" / "runtime"
    rollout_started_at = _local(trade_date, "09:14:00")
    if generations == 2:
        _production_bundle(
            sandbox / "bundle-previous",
            monkeypatch,
            producer_commit=PREVIOUS_COMMIT,
            runtime_root=runtime_root,
            schema_bootstrap_reason="route A day replay bootstrap",
            market_calendar_authority=recorded.calendar,
        )
    bundle_root = sandbox / "bundle-target"
    inputs, profile, receipt, sealed = _production_bundle(
        bundle_root,
        monkeypatch,
        producer_commit=world.commit,
        runtime_root=runtime_root,
        schema_bootstrap_reason=None if generations == 2 else "route A day replay bootstrap",
        definition_registry_root=runtime_root.parent / f"definitions-{world.commit[:7]}",
        market_calendar_authority=recorded.calendar,
        schema_rollout_started_at=rollout_started_at if generations == 2 else None,
    )
    for attribute, payload, mode in frozen:
        target = Path(getattr(inputs, attribute))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        target.chmod(mode)
        _backdate(target, frozen_at)
    route = RouteAWorld(world, inputs.runtime_root)
    route.profile = profile
    route.receipt = receipt
    route.sealed_credentials = sealed
    route.stage_and_publish()
    recorded.history = b""
    if generations == 2:
        acknowledge_runtime_schema_rollout_preparation(
            route.runtime_root, now=rollout_started_at + timedelta(seconds=37)
        )
    #: What the second install staged. Since package AJ (#228) a generation whose channels keep
    #: their shape gets no plan, so with `--generations 2` this is `plans: 0`; before it, the
    #: sixteen plans here are what wedged market_minute_source at the first capture (F5, #304).
    facts["schema_rollout"] = _schema_rollout_facts(route.runtime_root, receipt=receipt)
    facts["world_commit"] = world.commit
    facts["generations"] = sorted(
        path.name for path in (route.runtime_root / "generations").iterdir() if path.is_dir()
    )
    facts["calendar_written_identical"] = (
        Path(inputs.market_calendar_authority_path).read_bytes() == recorded.calendar_bytes
    )
    facts["replica"] = replica_extract(Path(inputs.readonly_replica_database_path))

    if (recorded.synthesize_reference or recorded.synthesize_auction) and synthesis is None:
        raise ReplayRefusedError("a synthesized source batch needs its prepared inputs")
    if recorded.synthesize_reference:
        assert synthesis is not None
        log("synthesizing the reference-slow batch through the live capture")
        facts["reference"] = synthesize_reference_batch(
            route, recorded, synthesis, key_root=bundle_root, commit=world.commit
        )
    else:
        facts["reference"] = reseal_reference_batch(
            route, recorded, key_root=bundle_root, commit=world.commit
        )
    if recorded.synthesize_auction:
        assert synthesis is not None
        log("synthesizing the auction-match batch through the live gateway")
        facts["auction"] = synthesize_auction_batches(
            route, recorded, synthesis, commit=world.commit
        )
    else:
        facts["auction"] = republish_auction_batches(route, recorded, commit=world.commit)
    if (
        synthesis is not None
        and recorded.reference_snapshot is not None
        and (recorded.auction_target is not None)
    ):
        facts["fidelity_candidate_inputs"] = candidate_input_fidelity(recorded, synthesis)
    credentials = deliver_credentials(route, sandbox / "credentials", monkeypatch)
    recorder = confirm_deliveries_without_the_network(monkeypatch)
    return route, facts, credentials, recorder


def candidate_input_fidelity(recorded: RecordedDay, synthesis: SynthesisInputs) -> dict[str, Any]:
    """Per auction code, what the candidate input reads under the recorded batches against
    what it reads under this run's (either side may be the recorded one)."""

    recorded_payload = next(
        payload
        for envelope, payload, _ in recorded.auction_records
        if envelope.sequence == recorded.auction_target.sequence
    )
    return sources.compare_candidate_inputs(
        recorded_snapshot=recorded.reference_snapshot,
        recorded_calendar=recorded.calendar,
        synthesized_snapshot=synthesis.reference_snapshot or recorded.reference_snapshot,
        synthesized_calendar=recorded.calendar,
        recorded_auction_payload=recorded_payload,
        synthesized_auction_payload=synthesis.auction_payload or recorded_payload,
    )


def _manifest(route: Any, service_id: str) -> Any:
    return next(item for item in route.profile.manifests if item.service_id == service_id)


def _manifest_of_kind(route: Any, kind: str) -> list[Any]:
    return [item for item in route.profile.manifests if item.service_kind.value == kind]


def reseal_reference_batch(
    route: Any, recorded: RecordedDay, *, key_root: Path, commit: str
) -> dict[str, Any]:
    """The host's sealed batch, re-sealed into the sandbox spool under the sandbox commit.

    Only the producer commit moves (the payload binds it, so the content id moves with it);
    every fact, the capture instant and the calendar id stay the host's. It is signed with
    the source key the sandbox bundle sealed into `reference_slow_publisher`'s credential,
    so the real publisher role verifies it the way it verifies the host's.
    """

    from rquant.live_contracts import LiveChannel
    from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
    from rquant.runtime_contracts import canonical_sha256

    original = recorded.reference_envelope
    snapshot = recorded.reference_snapshot
    identity = snapshot.model_dump(mode="python", exclude={"content_sha256"})
    identity["producer_commit"] = commit
    resealed = ReferenceSlowSourceSnapshot.model_validate(
        {**identity, "content_sha256": canonical_sha256(identity)}
    )
    spool = _reference_spool(route, key_root)
    prepared_at = original.available_at - _OLD_SOURCE_GUARD
    result = sources.seal_reference_snapshot(
        spool=spool,
        calendar=recorded.calendar,
        snapshot=resealed,
        producer_commit=commit,
        producer_version=original.producer_version,
        prepared_at=prepared_at,
    )
    (record,) = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    return {
        "origin": "recorded",
        "host_sequence": original.sequence,
        "host_available_at": original.available_at,
        "host_producer_commit": original.producer_commit,
        "captured_at": snapshot.captured_at,
        "securities": len(snapshot.security_facts),
        "daily_facts": len(snapshot.daily_facts),
        "resealed_available_at": record.envelope.available_at,
        "output_sequence": result.output_sequence,
    }


def _reference_spool(route: Any, key_root: Path) -> Any:
    """The sandbox reference-slow spool, signing with the source key the sandbox bundle
    sealed into `reference_slow_publisher`'s credential, so the real publisher verifies it."""

    from rquant.live_spool import (
        LiveBatchSpool,
        ReferenceSourceBatchSigner,
        ReferenceSourceBatchVerifier,
    )

    key = key_root / "reference-source-ed25519"
    signer = ReferenceSourceBatchSigner(
        key_id=_REFERENCE_SOURCE_KEY_ID, private_key=key.read_text("ascii")
    )
    verifier = ReferenceSourceBatchVerifier(
        key_id=_REFERENCE_SOURCE_KEY_ID,
        public_key=key.with_suffix(".pub").read_text("ascii").strip(),
    )
    publisher = _manifest(route, "reference-slow.publisher.v1")
    spool_root = Path(str(publisher.settings["spool_root"]))
    return LiveBatchSpool(spool_root, source_signer=signer, source_verifier=verifier)


def synthesize_reference_batch(
    route: Any,
    recorded: RecordedDay,
    synthesis: SynthesisInputs,
    *,
    key_root: Path,
    commit: str,
) -> dict[str, Any]:
    """The day's reference-slow batch, captured and sealed by the live code (never copied).

    `capture_reference_slow_source_snapshot` runs under the sandbox commit with the limits
    of the sandbox's own `reference-slow.source.v1` manifest, over the evidence extract and
    the day's Tushare answers; `capture_reference_slow_batch` seals it with the source key,
    prepared five seconds after the capture completed.
    """

    from rquant.auction_match_gateway import AuctionMatchGateway
    from rquant.live_contracts import LiveChannel
    from rquant.runtime_service_builtin import ReferenceSlowSourceSettings

    source_manifest = _manifest(route, "reference-slow.source.v1")
    settings = ReferenceSlowSourceSettings.model_validate(dict(source_manifest.settings))
    #: which codes matched in the opening auction: the recorded batch when the host has one,
    #: else Tushare's `stk_auction(D)` (already asked for, since the auction is synthesized)
    auction_frame = (
        AuctionMatchGateway.decode_payload(
            next(
                payload
                for envelope, payload, _ in recorded.auction_records
                if envelope.sequence == recorded.auction_target.sequence
            )
        )
        if recorded.auction_target is not None
        else synthesis.source.stk_auction(recorded.trade_date)
    )
    known = sources.KnownAtCapture(
        synthesis.source, traded_in_auction=sources.auction_traded_codes(auction_frame)
    )
    snapshot = sources.synthesize_reference_snapshot(
        evidence_database=synthesis.evidence_database,
        source=known,
        calendar=recorded.calendar,
        trade_date=recorded.trade_date,
        producer_commit=commit,
        limits=settings.limits.model_dump(mode="python"),
    )
    synthesis.reference_snapshot = snapshot
    spool = _reference_spool(route, key_root)
    prepared_at = snapshot.captured_at + timedelta(
        seconds=sources.SYNTHETIC_REFERENCE_PREPARE_SECONDS
    )
    try:
        result = sources.seal_reference_snapshot(
            spool=spool,
            calendar=recorded.calendar,
            snapshot=snapshot,
            producer_commit=commit,
            producer_version=settings.producer_version,
            prepared_at=prepared_at,
        )
    except Exception as error:  # noqa: BLE001 - a refusal, named
        raise ReplayRefusedError(
            f"reference_slow: the synthesized snapshot could not be sealed: "
            f"{type(error).__name__}: {error}"
        ) from error
    (record,) = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    facts: dict[str, Any] = {
        "origin": "synthesized",
        "observed_at": sources.local_instant(
            recorded.trade_date, sources.SYNTHETIC_REFERENCE_OBSERVED
        ),
        "captured_at": snapshot.captured_at,
        "securities": len(snapshot.security_facts),
        "daily_facts": len(snapshot.daily_facts),
        "suspended_codes": len(snapshot.suspended_codes),
        "projections": {
            projection.table_name: len(projection.rows) for projection in snapshot.projections
        },
        "resealed_available_at": record.envelope.available_at,
        "output_sequence": result.output_sequence,
        "evidence": synthesis.evidence,
        "anachronisms": sources.reference_anachronisms(snapshot, known),
    }
    if recorded.reference_snapshot is not None:
        facts["fidelity_vs_recorded"] = sources.compare_reference(
            recorded.reference_snapshot, snapshot
        )
    return facts


def synthesize_auction_batches(
    route: Any, recorded: RecordedDay, synthesis: SynthesisInputs, *, commit: str
) -> dict[str, Any]:
    """The day's auction-match batch, captured by the live gateway over `stk_auction(D)`.

    Expected codes are the host's universe generation for the day when it has one, else the
    live universe publisher's answer over the evidence extract. Received at the auction
    source's `capture_start` plus the tick phase -- its first attempt of the day.
    """

    from rquant.live_spool import LiveBatchSpool
    from rquant.runtime_service_builtin import AuctionMatchSourceSettings

    candidate = next(
        item
        for item in _manifest_of_kind(route, "candidate_publisher")
        if item.settings["strategy_id"] == "auction_gap"
    )
    source_manifest = _manifest(route, "auction-match.source.v1")
    settings = AuctionMatchSourceSettings.model_validate(dict(source_manifest.settings))
    if recorded.universe is not None:
        codes = tuple(str(code) for code in recorded.universe.get("codes", ()))
        universe_facts: dict[str, Any] = {
            "origin": "recorded",
            "source": recorded.universe.get("_path"),
            "codes": len(codes),
        }
    else:
        codes, universe_facts = sources.synthesize_auction_universe(
            evidence_database=synthesis.evidence_database,
            authority_root=synthesis.universe_root,
            calendar=recorded.calendar,
            trade_date=recorded.trade_date,
            producer_commit=commit,
        )
        recorded.provenance["auction_universe"] = {
            key: value for key, value in universe_facts.items() if key != "observed_at"
        }
    received_at = sources.local_instant(recorded.trade_date, settings.capture_start) + timedelta(
        seconds=synthesis.phase_seconds
    )
    facts = sources.synthesize_auction_batch(
        spool=LiveBatchSpool(Path(str(candidate.settings["auction_spool_root"]))),
        source=synthesis.source,
        trade_date=recorded.trade_date,
        expected_codes=codes,
        settings={
            "source": settings.source,
            "dataset_id": settings.dataset_id,
            "producer_version": settings.producer_version,
            "min_coverage_ratio": settings.min_coverage_ratio,
        },
        producer_commit=commit,
        received_at=received_at,
    )
    payload = facts.pop("payload")
    synthesis.auction_payload = payload
    facts["origin"] = "synthesized"
    facts["universe"] = universe_facts
    if recorded.auction_target is not None:
        recorded_payload = next(
            item_payload
            for envelope, item_payload, _ in recorded.auction_records
            if envelope.sequence == recorded.auction_target.sequence
        )
        facts["fidelity_vs_recorded"] = sources.compare_auction(recorded_payload, payload)
    if facts["quality_status"] != "published":
        facts["warning"] = (
            "the synthesized batch is not PUBLISHED, so auction_gap has no candidates today: "
            f"{facts['degraded_reasons']}"
        )
    return facts


def republish_auction_batches(route: Any, recorded: RecordedDay, *, commit: str) -> dict[str, Any]:
    """The host's auction-match batches up to the day's, re-published under the sandbox commit.

    `batch_id` binds the sequence, the event window and the content hash, not the commit, so
    the payload and every identity the candidate input reads stay the host's; only the
    envelope's `producer_commit` moves, because `auction_gap_candidate_input` refuses a batch
    produced by another commit than its own.
    """

    from rquant.auction_match_gateway import AuctionMatchGateway
    from rquant.live_contracts import BatchEnvelope
    from rquant.live_spool import LiveBatchSpool

    manifest = next(
        item
        for item in _manifest_of_kind(route, "candidate_publisher")
        if item.settings["strategy_id"] == "auction_gap"
    )
    spool = LiveBatchSpool(Path(str(manifest.settings["auction_spool_root"])))
    for envelope, payload, _ in recorded.auction_records:
        moved = BatchEnvelope.model_validate(
            {**envelope.model_dump(mode="python"), "producer_commit": commit}
        )
        spool.publish(moved, payload)
    target = recorded.auction_target
    frame = AuctionMatchGateway.decode_payload(
        next(
            payload
            for envelope, payload, _ in recorded.auction_records
            if envelope.sequence == target.sequence
        )
    )
    universe = recorded.universe
    codes = set(str(code) for code in frame["ts_code"])
    return {
        "origin": "recorded",
        "sequences": [envelope.sequence for envelope, _, _ in recorded.auction_records],
        "target_sequence": target.sequence,
        "rows": int(target.row_count),
        "available_at": target.available_at,
        "universe": None
        if universe is None
        else {
            "path": universe.get("_path"),
            "codes": len(universe.get("codes", ())),
            "available_at": universe.get("available_at"),
            "auction_codes_outside_universe": len(codes - set(universe.get("codes", ()))),
        },
    }


def role_states(route: Any, credentials: dict[str, Path]) -> list[RoleState]:
    import tests.integration.test_route_a_all_roles_sandbox_e2e as harness
    from tests.integration.test_route_a_live_chain_idle_e2e import _instance_name

    by_instance = {_instance_name(item.service_id): item for item in route.profile.manifests}
    states: list[RoleState] = []
    for role in CHAIN:
        instances = harness.instance_of(route, role)
        manifests = [by_instance[instance] for instance in instances]
        if role == "candidate_publisher":
            manifests.sort(key=lambda item: _CANDIDATE_ORDER.index(item.settings["strategy_id"]))
        for manifest in manifests:
            instance = _instance_name(manifest.service_id)
            resolved = harness.launch(route, role, instance, credentials.get(instance))
            argv = harness.relocated(route, list(resolved["module_argv"]))
            if resolved["module"] != "rquant.runtime_service_main":
                raise ReplayRefusedError(f"{role} is not a runtime_service_main role")
            states.append(
                RoleState(
                    role=role,
                    instance=instance,
                    service_id=manifest.service_id,
                    label=manifest.service_id,
                    argv=argv,
                    environment=dict(resolved["environment"]),
                    control_root=Path(argv[argv.index("--control-root") + 1]),
                    manifest=manifest,
                )
            )
    return states


# ---------------------------------------------------------------------------------------
# Reading what the chain left behind
# ---------------------------------------------------------------------------------------


def _sqlite_counts(
    path: Path, prefix: str | None = None, tables: Iterable[str] = ()
) -> dict[str, int]:
    if not path.is_file():
        return {}
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as connection:
        present = [
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        ]
        wanted = [
            name for name in present if (prefix and name.startswith(prefix)) or name in tables
        ]
        return {
            name: int(connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0])
            for name in sorted(wanted)
        }


def summarize_chain(route: Any, trade_date: date, adapter: ReplayMinuteAdapter) -> dict[str, Any]:
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool
    from tests.integration.test_route_a_trading_day_full_chain_e2e import (
        feature_batches,
        notification_attempt_receipts,
        notification_counts,
        runner_signal_count,
        serving_current,
        serving_generations,
        setting_of,
        signal_bus_counts,
    )

    summary: dict[str, Any] = {}
    minute_root = setting_of(route, "market-minute.source.v1", "spool_root")
    minute_batches: list[Any] = []
    if (minute_root / "batches").is_dir():
        minute_batches = [
            record.envelope
            for record in LiveBatchSpool(minute_root).list_after(
                LiveChannel.MARKET_MINUTE, sequence=-1
            )
        ]
    summary["market_minute"] = {
        "batches": len(minute_batches),
        "rows": sum(int(envelope.row_count) for envelope in minute_batches),
        "non_published": sum(
            1 for envelope in minute_batches if envelope.quality_status.value != "published"
        ),
        "watchlist_codes_last_call": len(adapter.last_universe),
        "watchlist_codes_ever": len(adapter.requested),
        "codes_without_minutes": sorted(adapter.missing),
        "codes_by_origin": _count_by(adapter.sources.values(), lambda value: value.split(":")[0]),
        "tushare_fetched_codes": len(adapter.tushare_fetched),
        "tushare_failed": adapter.tushare_failed,
        "adapter_calls": adapter.calls,
        "rows_served": adapter.rows_served,
        #: per watchlist code, where its minutes came from (what a signal diff needs)
        "watchlist_codes": sorted(adapter.requested),
        "origin_by_watchlist_code": {
            code: adapter.sources.get(code)
            or (
                f"tushare_failed: {adapter.tushare_failed[code]}"
                if code in adapter.tushare_failed
                else "missing"
            )
            for code in sorted(adapter.requested)
        },
    }
    summary["feature_batches_high_watermark"] = feature_batches(route)
    strategies = {
        str(item.settings["strategy_id"]): runner_signal_count(route, item.service_id)
        for item in _manifest_of_kind(route, "strategy_live")
    }
    summary["signals_per_strategy"] = strategies
    summary["signal_bus"] = signal_bus_counts(route)
    broker_path = setting_of(route, "paper-broker.shadow-main.v1", "broker_path")
    summary["paper"] = _sqlite_counts(
        broker_path,
        tables=(
            "paper_intent",
            "paper_order",
            "paper_fill",
            "paper_lot",
            "paper_execution_receipt",
        ),
    )
    notifications = notification_counts(route)
    receipts = notification_attempt_receipts(route)
    summary["notifier"] = {
        **notifications,
        "attempt_receipts": len(receipts),
        "all_receipts_shadow": all(receipt.startswith("shadow:") for receipt in receipts),
        "non_shadow_receipts": [
            receipt for receipt in receipts if not receipt.startswith("shadow:")
        ],
        "suppress_delivery": _manifest(route, "notifier.admin.shadow.v1").settings[
            "suppress_delivery"
        ],
    }
    generations = serving_generations(route)
    pointer = serving_current(route)
    serving_root = setting_of(route, "serving.publisher.v1", "serving_root")
    serving: dict[str, Any] = {
        "serving_root": str(serving_root),
        "generations": [path.name for path in generations],
        "current": pointer,
    }
    if pointer is not None:
        published = datetime.fromisoformat(str(pointer["published_at"]).replace("Z", "+00:00"))
        serving["current_published_local"] = published.astimezone(_SHANGHAI)
        serving["same_day"] = published.astimezone(_SHANGHAI).date() == trade_date
        serving.update(_serving_signal_rows(serving_root, trade_date))
    else:
        serving["same_day"] = False
    summary["serving"] = serving
    return summary


def _count_by(values: Iterable[str], key: Callable[[str], str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[key(value)] = counts.get(key(value), 0) + 1
    return counts


def _serving_signal_rows(serving_root: Path, trade_date: date) -> dict[str, Any]:
    from rquant.serving_publisher import ServingReader

    opened = datetime.combine(trade_date, clock_time(9, 15), tzinfo=_SHANGHAI)
    literal = opened.isoformat()
    listed: list[dict[str, Any]] = []
    with ServingReader(serving_root).open_current_readonly() as connection:
        total = int(connection.execute("SELECT count(*) FROM signals").fetchone()[0])
        today = (
            int(
                connection.execute(
                    "SELECT count(*) FROM signals WHERE TRY_CAST(event_time AS TIMESTAMPTZ) >= "
                    f"TIMESTAMPTZ '{literal}'"
                ).fetchone()[0]
            )
            if total
            else 0
        )
        if today:
            rows = connection.execute(
                "SELECT epoch_ms(TRY_CAST(event_time AS TIMESTAMPTZ)), strategy_id, "
                "candidate_id, action FROM signals WHERE TRY_CAST(event_time AS TIMESTAMPTZ) "
                f">= TIMESTAMPTZ '{literal}' ORDER BY 1, 2, 3, 4 LIMIT 5000"
            ).fetchall()
            listed = [
                {
                    "event_time_local": datetime.fromtimestamp(stamp / 1000, _SHANGHAI)
                    .replace(tzinfo=None)
                    .isoformat(),
                    "strategy_id": str(strategy),
                    "candidate_id": str(candidate),
                    "action": str(action),
                }
                for stamp, strategy, candidate, action in rows
            ]
    return {
        "signals_rows_total": total,
        "signals_rows_today": today,
        "signals_filter": f">= {literal}",
        "signals_today_list": listed,
    }


def signal_fidelity(
    summary: Mapping[str, Any], other_path: Path, *, until: str, trade_date: date
) -> dict[str, Any]:
    """This run's serving signals against another run's of the same day, up to the earlier
    run's last tick, with what each run knew about every code that differs.

    `other_path` is a `summary.json` or the sandbox holding one. A summary written before
    `signals_today_list` existed is read through its sandbox's serving generation instead.
    """

    other_file = other_path / "summary.json" if other_path.is_dir() else other_path
    other = json.loads(other_file.read_text(encoding="utf-8"))
    if str(other.get("trade_date")) != trade_date.isoformat():
        return {
            "other": str(other_file),
            "refused": f"the other run replayed {other.get('trade_date')}, not {trade_date}",
        }
    other_serving = (other.get("chain") or {}).get("serving") or {}
    other_list = other_serving.get("signals_today_list")
    if other_list is None:
        root = other_serving.get("serving_root")
        if not root:
            return {"other": str(other_file), "refused": "the other run has no serving root"}
        other_list = _serving_signal_rows(Path(root), trade_date)["signals_today_list"]
    other_until = str((other.get("mode") or {}).get("until") or "15:05:00")
    window = min(clock_time.fromisoformat(until), clock_time.fromisoformat(other_until))
    diff = sources.compare_signal_lists(
        (summary.get("chain") or {}).get("serving", {}).get("signals_today_list") or [],
        other_list,
        until_local=window.isoformat(),
    )
    mine = (summary.get("chain") or {}).get("market_minute") or {}
    theirs = (other.get("chain") or {}).get("market_minute") or {}
    candidates = (summary.get("world") or {}).get("fidelity_candidate_inputs") or {}

    def minutes_of(chain: Mapping[str, Any], code: str) -> str | None:
        origin = (chain.get("origin_by_watchlist_code") or {}).get(code)
        if origin is not None:
            return str(origin)
        if code in (chain.get("tushare_failed") or {}):
            return f"tushare_failed: {chain['tushare_failed'][code]}"
        if code in (chain.get("codes_without_minutes") or ()):
            return "missing"
        return None

    diff["by_code"] = {
        code: {
            "candidate_input_differs": code in set(candidates.get("codes_differ") or ()),
            "candidate_input": (candidates.get("by_code") or {}).get(code),
            "in_watchlist": {
                "this_run": code in set(mine.get("watchlist_codes") or ()),
                "other_run": (
                    code in set(theirs["watchlist_codes"]) if "watchlist_codes" in theirs else None
                ),
            },
            "minutes": {"this_run": minutes_of(mine, code), "other_run": minutes_of(theirs, code)},
        }
        for code in diff["codes_differ"]
    }
    diff["other"] = str(other_file)
    diff["other_mode"] = {
        key: (other.get("mode") or {}).get(key)
        for key in ("until", "synthesize_sources", "tushare", "serving_every_ticks")
    }
    diff["other_inputs"] = {
        label: entry.get("origin")
        for label, entry in ((other.get("inputs") or {}).get("provenance") or {}).items()
    }
    return diff


class ServingRetention:
    """Keep the sandbox's serving tree bounded; count what the publisher cut.

    Every tick moves `runtime_health` (heartbeats), so the real publisher cuts a generation
    on most ticks, each a whole `serving.duckdb`. The host has `artifact_retention` for
    that; the replay does not run it, so it keeps the generations `current.json` names
    (current and previous) plus the newest `keep` and removes the rest -- in the sandbox
    only -- and reports how many were cut and how large they were.
    """

    def __init__(self, serving_root: Path, keep: int) -> None:
        self.root = serving_root
        self.keep = keep
        self.seen: dict[str, int] = {}
        self.removed = 0

    def after_step(self) -> None:
        generations = self.root / "generations"
        if not generations.is_dir():
            return
        entries = [path for path in generations.iterdir() if path.is_dir()]
        for path in entries:
            if path.name not in self.seen:
                self.seen[path.name] = _tree_bytes(path)
        if self.keep <= 0:
            return
        pinned: set[str] = set()
        pointer = self.root / "current.json"
        if pointer.is_file():
            document = json.loads(pointer.read_text(encoding="utf-8"))
            pinned = {
                str(document.get("generation_id")),
                str(document.get("previous_generation_id")),
            }
        newest = sorted(entries, key=lambda path: path.stat().st_mtime_ns)[-self.keep :]
        pinned |= {path.name for path in newest}
        for path in entries:
            if path.name in pinned:
                continue
            for directory, _subdirectories, _files in os.walk(path):
                os.chmod(directory, 0o700)
            shutil.rmtree(path)
            self.removed += 1

    def facts(self) -> dict[str, Any]:
        sizes = sorted(self.seen.values())
        return {
            "generations_cut": len(self.seen),
            "removed_by_replay_retention": self.removed,
            "keep": self.keep,
            "bytes_per_generation_median": sizes[len(sizes) // 2] if sizes else 0,
            "bytes_per_generation_max": sizes[-1] if sizes else 0,
            "bytes_all_generations": sum(sizes),
        }


# ---------------------------------------------------------------------------------------
# The day
# ---------------------------------------------------------------------------------------


def schedule(
    trade_date: date,
    *,
    start: str,
    until: str,
    step_seconds: float,
    phase_seconds: float,
    reference_start: str,
    reference_interval_seconds: float,
) -> list[tuple[datetime, str]]:
    """Grid ticks (every role) and the pre-open reference rounds (the publisher alone)."""

    begin = _local(trade_date, start) + timedelta(seconds=phase_seconds)
    end = _local(trade_date, until)
    events: list[tuple[datetime, str]] = []
    now = begin
    while now <= end:
        events.append((now, "tick"))
        now += timedelta(seconds=step_seconds)
    rounds = _local(trade_date, reference_start)
    decision = _local(trade_date, "09:25:00")
    while rounds < decision and rounds <= end:
        events.append((rounds, "reference"))
        rounds += timedelta(seconds=reference_interval_seconds)
    return sorted(events, key=lambda item: item[0])


def _cprofile_labels(arguments: argparse.Namespace) -> frozenset[str]:
    return frozenset(
        label.strip()
        for value in (arguments.cprofile or ())
        for label in value.split(",")
        if label.strip()
    )


def not_production_faithful_because(arguments: argparse.Namespace) -> list[str]:
    """What this run's cadence changes against production, one reason per line.

    Inputs are labelled separately (`inputs.provenance`); this is about the harness.
    """

    reasons: list[str] = []
    if int(arguments.serving_every_ticks) > 1:
        reasons.append(
            f"serving.publisher.v1 runs every {int(arguments.serving_every_ticks)} ticks "
            f"({int(arguments.serving_every_ticks) * arguments.step_seconds:.0f} s) and on the "
            "last one; production runs it every 30 s"
        )
    return reasons


def synthesized_inputs(provenance: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return sorted(
        label for label, entry in provenance.items() if entry.get("origin") == "synthesized"
    )


def production_roots(runtime_root: Path, replica: Path, production_inputs: Path) -> list[str]:
    """Every directory the audit hook refuses writes under."""

    return sorted(
        {
            str(runtime_root),
            str(replica.parent),
            str(production_inputs.parent),
            str(runtime_root.parent),
        }
    )


def refuse_overlap(label: str, path: Path, protected: Iterable[str]) -> None:
    for root in protected:
        candidate = Path(root)
        if path.is_relative_to(candidate) or candidate.is_relative_to(path):
            raise ReplayRefusedError(f"the {label} {path} overlaps production path {root}")


def run_replay(arguments: argparse.Namespace, *, out: Callable[[str], None] = print) -> int:
    import tempfile

    import pytest

    global _PROTECTED_ROOTS

    trade_date = date.fromisoformat(arguments.trade_date)
    today = datetime.now(_SHANGHAI).date()
    if trade_date > today:
        raise ReplayRefusedError(f"{trade_date} has not happened yet (today is {today})")
    runtime_root = arguments.runtime_root.resolve()
    replica = arguments.replica.resolve()
    production_inputs = arguments.production_inputs.resolve()
    protected = production_roots(runtime_root, replica, production_inputs)
    replay_root = arguments.replay_root.resolve()
    stamp = datetime.now(_SHANGHAI).strftime("%Y%m%dT%H%M%S")
    sandbox = replay_root / f"{stamp}-{secrets.token_hex(3)}"
    cache_root = (arguments.tushare_cache or replay_root / "tushare-cache").resolve()
    for label, path in (("replay root", sandbox), ("Tushare cache", cache_root)):
        refuse_overlap(label, path, protected)

    token = os.environ.get("TUSHARE_TOKEN_MAIN") if arguments.tushare else None
    if arguments.tushare and not arguments.tushare_offline and not token:
        raise ReplayRefusedError("--tushare needs TUSHARE_TOKEN_MAIN in the process environment")
    tushare_mode = "offline" if arguments.tushare_offline else ("online" if token else None)

    _PROTECTED_ROOTS = tuple(protected)
    sys.addaudithook(_audit_hook)
    if arguments.dry_plan:
        try:
            return dry_plan(
                arguments,
                trade_date=trade_date,
                runtime_root=runtime_root,
                replica=replica,
                production_inputs=production_inputs,
                sandbox=sandbox,
                cache_root=cache_root,
                tushare_mode=tushare_mode,
                out=out,
            )
        finally:
            _PROTECTED_ROOTS = ()
    replay_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    sandbox.mkdir(mode=0o700)
    sandbox.chmod(0o700)
    for name in ("tmp", "inputs", "scratch"):
        (sandbox / name).mkdir(mode=0o700)
    out(f"replay sandbox: {sandbox}")
    wall_started = time.monotonic()
    saved_environment = dict(os.environ)
    saved_cwd = os.getcwd()
    saved_tempdir = tempfile.tempdir
    os.chdir(sandbox)
    tempfile.tempdir = str(sandbox / "tmp")
    os.environ["TMPDIR"] = str(sandbox / "tmp")
    #: nothing on the role path reads `Settings`; if anything does, it reads the sandbox,
    #: never a `.env`
    os.environ["RQUANT_DISABLE_DOTENV"] = "1"
    if not os.environ.get("TUSHARE_TOKEN_MAIN"):
        os.environ["TUSHARE_TOKEN_MAIN"] = "replay-placeholder-token-000000000000"
    #: `tests/conftest.py` does these two for the suite; this process imports the test
    #: helpers without it. No delivery key of the operator's shell ever reaches `Settings`,
    #: and legacy notification paths are off even if something reads them.
    for name in [key for key in os.environ if key.startswith(("PUSHDEER", "PUSHPLUS"))]:
        os.environ.pop(name)
    os.environ["NOTIFY_ENABLED"] = "false"
    #: anything that writes under `~` (git config lookups, a DuckDB extension autoload,
    #: tushare's token file) lands in the sandbox; the roles get the same HOME
    (sandbox / "home").mkdir(mode=0o700)
    os.environ["HOME"] = str(sandbox / "home")
    for name, value in (
        ("DATA_DIR", sandbox / "scratch"),
        ("DUCKDB_PATH", sandbox / "scratch" / "rquant.duckdb"),
        ("PARQUET_DIR", sandbox / "scratch" / "parquet"),
        ("LOG_DIR", sandbox / "scratch" / "logs"),
    ):
        os.environ[name] = str(value)
    monkeypatch = pytest.MonkeyPatch()
    #: The replay is not a systemd unit, whatever cgroup the operator's shell sits in: an SSH
    #: session scope reads as no unit, but a shell inside some `*.service` (a panel's web
    #: terminal, `systemd-run`) would make every credstore role refuse its credential as
    #: "belongs to another unit" -- the reason `tests/conftest.py` points this probe away
    #: for the suite (`_the_suite_is_not_a_runtime_unit`).
    import rquant.runtime_capabilities as capabilities_module

    host_facts = {
        "platform": sys.platform,
        "uid": os.getuid(),
        "gid": os.getgid(),
        "root_mode": oct(os.stat("/").st_mode & 0o7777),
        "cpu_count": os.cpu_count(),
        "shell_systemd_unit": capabilities_module._systemd_unit_name(),
    }
    monkeypatch.setattr(
        capabilities_module, "_SYSTEMD_CGROUP_PATH", sandbox / "not-a-systemd-unit" / "cgroup"
    )
    audit = ProductionAudit(roots=tuple(Path(root) for root in protected))
    unfaithful = not_production_faithful_because(arguments)
    summary: dict[str, Any] = {
        "trade_date": trade_date,
        "sandbox": str(sandbox),
        "mode": {
            "step_seconds": arguments.step_seconds,
            "phase_seconds": arguments.phase_seconds,
            "until": arguments.until,
            "tushare": tushare_mode,
            "tushare_cache": str(cache_root) if tushare_mode else None,
            "synthesize_sources": bool(arguments.synthesize_sources),
            "generations": arguments.generations,
            "minute_lag_seconds": arguments.minute_lag_seconds,
            "serving_every_ticks": arguments.serving_every_ticks,
            "cprofile": sorted(_cprofile_labels(arguments)),
            "assume_listing_classification": bool(arguments.assume_listing_classification),
            "runner": "in-process threads, one role at a time, no unit sandbox",
            "production_faithful": not unfaithful,
            "not_production_faithful_because": unfaithful,
        },
        "stage_seconds": {},
        "host": host_facts,
    }
    for reason in unfaithful:
        out(f"WARNING: not production-faithful: {reason}")
    cache = (
        sources.TushareCache(cache_root, token=token, offline=tushare_mode == "offline")
        if tushare_mode
        else None
    )
    exit_code = 1
    runner: RoleRunner | None = None
    try:
        started = time.monotonic()
        recorded = read_recorded_day(
            runtime_root=runtime_root,
            production_inputs=production_inputs,
            trade_date=trade_date,
            reference_sequence=arguments.reference_sequence,
            auction_sequence=arguments.auction_sequence,
            audit=audit,
            synthesize=arguments.synthesize_sources,
            tushare=cache is not None,
        )
        #: only what this run re-seals; a recorded batch kept for the comparison is in
        #: `provenance.*.recorded_batch`
        summary["inputs"] = {
            "reference_batch": None
            if recorded.reference_manifest_path is None or recorded.synthesize_reference
            else str(recorded.reference_manifest_path),
            "calendar": None if recorded.calendar_path is None else str(recorded.calendar_path),
            "auction_batches": []
            if recorded.synthesize_auction
            else [str(path) for _, _, path in recorded.auction_records],
            "production_inputs": recorded.inputs_fingerprints,
            "provenance": recorded.provenance,
        }
        probe = sources.probe_replica(
            replica=replica, trade_date=trade_date, calendar=recorded.calendar, audit=audit
        )
        summary["inputs"]["replica_probe"] = probe
        reasons = sources.replica_refusals(
            probe,
            trade_date=trade_date,
            synthesize_reference=recorded.synthesize_reference,
            tushare_minutes=cache is not None,
        )
        if reasons:
            raise ReplayRefusedError("replica: " + " | ".join(reasons))
        recorded.provenance["minute_bar"] = {
            "origin": "recorded",
            "source": f"replica minute_bar ({probe['trade_date_minute_rows']} rows, "
            f"{probe['trade_date_minute_codes']} codes)",
            "gaps": "Tushare stk_mins through the cache" if cache is not None else "left missing",
        }
        synthesis: SynthesisInputs | None = None
        if recorded.synthesize_reference or recorded.synthesize_auction:
            assert cache is not None
            day_source = sources.CachedDaySource(cache, trade_date=trade_date, today=today)
            out(f"asking Tushare (cache {cache_root}) for the day's source answers")
            summary["inputs"]["tushare_prefetch"] = sources.prefetch_day(
                day_source,
                reference=recorded.synthesize_reference,
                auction=recorded.synthesize_auction,
            )
            evidence = sources.extract_reference_evidence(
                replica=replica,
                target=sandbox / "inputs" / "reference-evidence.duckdb",
                trade_date=trade_date,
                calendar=recorded.calendar,
                audit=audit,
                require_adj_factor=recorded.synthesize_reference,
            )
            synthesis = SynthesisInputs(
                source=day_source,
                evidence_database=Path(evidence["path"]),
                evidence=evidence,
                universe_root=sandbox / "inputs" / "auction-universe",
                phase_seconds=arguments.phase_seconds,
            )
        minutes_path = sandbox / "inputs" / "minutes-replica.parquet"
        summary["stage_seconds"]["read_host"] = round(time.monotonic() - started, 2)

        def extract(target: Path) -> dict[str, Any]:
            begun = time.monotonic()
            facts = extract_replica(
                replica=replica,
                target=target,
                minutes_target=minutes_path,
                trade_date=trade_date,
                calendar=recorded.calendar,
                daily_sessions=arguments.replica_daily_sessions,
                minute_sessions=arguments.replica_minute_sessions,
                synced_at=_local(trade_date, arguments.replica_synced_at),
                audit=audit,
            )
            summary["stage_seconds"]["replica_extract"] = round(time.monotonic() - begun, 2)
            return facts

        started = time.monotonic()
        route, facts, credentials, recorder = build_world(
            sandbox=sandbox,
            recorded=recorded,
            monkeypatch=monkeypatch,
            generations=arguments.generations,
            replica_extract=extract,
            log=out,
            synthesis=synthesis,
        )
        summary["world"] = facts
        summary["stage_seconds"]["build_world"] = round(time.monotonic() - started, 2)

        import pandas as pd

        clock = ReplayClock(_local(trade_date, arguments.start))
        adapter = ReplayMinuteAdapter(
            pd.read_parquet(minutes_path),
            clock=clock,
            lag_seconds=arguments.minute_lag_seconds,
            tushare_fetch=None
            if cache is None
            else sources.cached_minute_fetcher(cache, trade_date),
            trade_date=trade_date,
        )
        install_runner_patches(monkeypatch, clock, adapter)
        if arguments.assume_listing_classification:
            out(f"WARNING: {LISTING_CLASSIFICATION_FLAG_WARNING}")
        summary["stubs"] = {
            "listing_classification": (
                "off (flag given, a no-op since package AI)"
                if arguments.assume_listing_classification
                else "off"
            ),
        }
        states = role_states(route, credentials)
        runner = RoleRunner(
            states,
            clock=clock,
            role_timeout_seconds=arguments.role_timeout_seconds,
            max_restarts=arguments.max_restarts,
            log=out,
            cprofile=_cprofile_labels(arguments),
            profile_root=sandbox / "profile",
        )
        unknown = _cprofile_labels(arguments) - {state.label for state in states}
        if unknown:
            raise ReplayRefusedError(
                f"--cprofile names no role of this chain: {sorted(unknown)} "
                f"(roles: {sorted(state.label for state in states)})"
            )
        events = schedule(
            trade_date,
            start=arguments.start,
            until=arguments.until,
            step_seconds=arguments.step_seconds,
            phase_seconds=arguments.phase_seconds,
            reference_start=arguments.reference_start,
            reference_interval_seconds=5.0,
        )
        spacing = min(1.0, arguments.step_seconds / (2 * max(1, len(states))))
        retention = ServingRetention(
            Path(str(_manifest(route, "serving.publisher.v1").settings["serving_root"])),
            arguments.keep_serving_generations,
        )
        out(f"driving {len(states)} role instances over {len(events)} events")
        started = time.monotonic()
        reference = next(state for state in states if state.role == "reference_slow_publisher")
        stopped = False
        tick_indexes = [index for index, (_, kind) in enumerate(events) if kind == "tick"]
        last_tick = tick_indexes[-1] if tick_indexes else -1
        serving_every = max(1, int(arguments.serving_every_ticks))
        tick_number = 0
        tick_walls: list[tuple[str, float]] = []
        for index, (moment, kind) in enumerate(events):
            if kind == "reference":
                if reference.first_output_at is not None:
                    continue
                if not runner.step(reference, moment):
                    stopped = True
                    break
                continue
            tick_started = time.perf_counter()
            for position, state in enumerate(states):
                if (
                    state.role == "serving_publisher"
                    and tick_number % serving_every
                    and index != last_tick
                ):
                    state.skipped_ticks += 1
                    continue
                if not runner.step(state, moment + timedelta(seconds=spacing * position)):
                    stopped = True
                    break
                if state.role == "serving_publisher":
                    retention.after_step()
            tick_walls.append((session_phase(moment), time.perf_counter() - tick_started))
            tick_number += 1
            if stopped:
                break
            if index % max(1, int(1800 / arguments.step_seconds)) == 0 or index == len(events) - 1:
                out(
                    f"  {moment.astimezone(_SHANGHAI):%H:%M:%S} "
                    f"elapsed {time.monotonic() - started:6.0f}s "
                    f"failing={_failing(states)} "
                    f"crashes={sum(len(state.crashes) for state in states)}"
                )
        drive_seconds = time.monotonic() - started
        summary["stage_seconds"]["drive_the_day"] = round(drive_seconds, 2)
        runner.stop_all()
        summary["profile"] = step_profile(states, tick_walls, drive_seconds)
        profiles = {state.label: str(state.profile_path) for state in states if state.profile_path}
        if profiles:
            summary["profile"]["cprofile"] = profiles
        summary["hung"] = runner.hung
        summary["roles"] = {
            state.label: {
                "role": state.role,
                "iterations": state.iterations,
                "wall_seconds": round(state.wall_seconds, 2),
                "max_iteration_seconds": round(state.max_iteration_seconds, 3),
                "first_output_at": state.first_output_at.astimezone(_SHANGHAI)
                if state.first_output_at
                else None,
                "max_processed_count": state.max_processed,
                "output_sequence": None
                if state.last_heartbeat is None
                else state.last_heartbeat.output_sequence,
                "total_successes": None
                if state.last_heartbeat is None
                else state.last_heartbeat.total_successes,
                "total_failures": None
                if state.last_heartbeat is None
                else state.last_heartbeat.total_failures,
                "errors": state.errors,
                "degraded_reasons": state.degraded,
                "degraded_details": state.degraded_details,
                "observations": state.observations,
                "crashes": state.crashes,
            }
            for state in states
        }
        summary["candidates_per_family"] = {
            str(state.manifest.settings["strategy_id"]): state.max_processed
            for state in states
            if state.role == "candidate_publisher"
        }
        summary["chain"] = summarize_chain(route, trade_date, adapter)
        summary["chain"]["serving"]["retention"] = retention.facts()
        summary["notifier_provider_deliveries"] = len(recorder.deliveries)
        fetched = adapter.fetched_frame()
        if fetched is not None:
            fetched.to_parquet(sandbox / "inputs" / "minutes-tushare.parquet", index=False)
        recorded.provenance["minute_bar"]["codes_by_origin"] = summary["chain"]["market_minute"][
            "codes_by_origin"
        ]
        if arguments.compare_signals_with is not None:
            summary["signal_fidelity"] = signal_fidelity(
                summary,
                arguments.compare_signals_with.resolve(),
                until=arguments.until,
                trade_date=trade_date,
            )
        crashed = [state.label for state in states if state.crashes]
        serving_ok = bool(summary["chain"]["serving"].get("same_day"))
        shadow_ok = (
            summary["chain"]["notifier"]["all_receipts_shadow"]
            and summary["notifier_provider_deliveries"] == 0
        )
        summary["verdict"] = {
            "same_day_serving_generation": serving_ok,
            "notifier_shadow_only": shadow_ok,
            "crashed_roles": crashed,
            "hung": runner.hung,
            "synthesized_inputs": synthesized_inputs(recorded.provenance),
            "production_faithful": not unfaithful,
        }
        exit_code = 0 if serving_ok and shadow_ok and not crashed and runner.hung is None else 1
    except (ReplayRefusedError, sources.InputUnavailableError) as error:
        summary["refused"] = str(error)
        out(f"summary: {sandbox / 'summary.json'}")
        if isinstance(error, ReplayRefusedError):
            raise
        raise ReplayRefusedError(str(error)) from error
    except Exception as error:  # noqa: BLE001 - reported in the summary, exit 1
        summary["fatal"] = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        out(f"FATAL: {type(error).__name__}: {error}")
        exit_code = 1
    finally:
        if runner is not None:
            runner.stop_all()
        if cache is not None:
            summary.setdefault("inputs", {})["tushare_cache"] = cache.summary()
        summary["production_audit"] = audit.finish()
        summary["peak_rss_bytes"] = _peak_rss_bytes()
        summary["stage_seconds"]["total"] = round(time.monotonic() - wall_started, 2)
        summary["sandbox_bytes"] = _tree_bytes(sandbox)
        audit_ok = (
            not summary["production_audit"]["changed_during_our_read"]
            and not summary["production_audit"]["changed_in_place_later"]
        )
        summary.setdefault("verdict", {})["production_untouched"] = audit_ok
        if not audit_ok:
            exit_code = 1
        (sandbox / "summary.json").write_text(
            json.dumps(
                summary, indent=2, sort_keys=True, default=_json_default, ensure_ascii=False
            ),
            encoding="utf-8",
        )
        monkeypatch.undo()
        os.chdir(saved_cwd)
        tempfile.tempdir = saved_tempdir
        os.environ.clear()
        os.environ.update(saved_environment)
        _PROTECTED_ROOTS = ()
    print_summary(summary, out=out)
    out(f"summary: {sandbox / 'summary.json'}")
    out(f"REPLAY {'OK' if exit_code == 0 else 'FAILED'}")
    return exit_code


def _peak_rss_bytes() -> int:
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    #: kilobytes on Linux, bytes on macOS
    return int(peak if sys.platform == "darwin" else peak * 1024)


def _tree_bytes(root: Path) -> int:
    total = 0
    for directory, _subdirectories, files in os.walk(root):
        for name in files:
            with suppress(OSError):
                total += (Path(directory) / name).lstat().st_size
    return total


def _brief(values: dict[str, Any], *, drop: tuple[str, ...]) -> str:
    kept = {key: value for key, value in values.items() if key not in drop}
    return json.dumps(kept, default=_json_default, ensure_ascii=False)


def _failing(states: list[RoleState]) -> list[str]:
    return sorted(
        state.label
        for state in states
        if state.last_heartbeat is not None and state.last_heartbeat.consecutive_failures
    )


def print_summary(summary: dict[str, Any], *, out: Callable[[str], None]) -> None:
    chain = summary.get("chain", {})
    minute = chain.get("market_minute", {})
    serving = chain.get("serving", {})
    lines = [
        f"trade date: {summary.get('trade_date')}",
        f"candidates per family: {summary.get('candidates_per_family')}",
        f"market minute: {_brief(minute, drop=('codes_without_minutes', 'tushare_failed'))}",
        f"codes without minutes: {len(minute.get('codes_without_minutes', []))}",
        f"feature batches (high watermark): {chain.get('feature_batches_high_watermark')}",
        f"signals per strategy: {chain.get('signals_per_strategy')}",
        f"signal bus: {chain.get('signal_bus')}",
        f"paper: {chain.get('paper')}",
        f"notifier: {_brief(chain.get('notifier', {}), drop=('non_shadow_receipts',))}",
        f"serving: {_brief(serving, drop=('current', 'generations'))}",
        f"serving generations kept: {len(serving.get('generations', []))}",
        f"stage seconds: {summary.get('stage_seconds')}",
        f"verdict: {summary.get('verdict')}",
    ]
    for label, entry in sorted(((summary.get("inputs") or {}).get("provenance") or {}).items()):
        lines.append(f"  input {label:<17} {entry.get('origin', '?'):<11} {entry.get('source')}")
    world = summary.get("world") or {}
    reference_fidelity = (world.get("reference") or {}).get("fidelity_vs_recorded")
    if reference_fidelity is not None:
        lines.append(
            f"fidelity reference vs recorded: differing={reference_fidelity['differing'] or 'none'}"
        )
        for table, entry in reference_fidelity["projection_rows"].items():
            if not entry["identical"]:
                lines.append(
                    f"  projection {table}: recorded {entry['recorded']} synthesized "
                    f"{entry['synthesized']}, only recorded {entry['only_recorded_count']} "
                    f"{entry['only_recorded'][:8]}, only synthesized "
                    f"{entry['only_synthesized_count']} {entry['only_synthesized'][:8]}, "
                    f"values differ {entry['values_differ_count']}"
                )
    auction_fidelity = (world.get("auction") or {}).get("fidelity_vs_recorded")
    if auction_fidelity is not None:
        lines.append(f"fidelity auction vs recorded: {_brief(auction_fidelity, drop=())}")
    candidates = world.get("fidelity_candidate_inputs")
    if candidates is not None:
        lines.append(
            f"fidelity candidate inputs: {candidates['codes_differ_count']} of "
            f"{candidates['codes_compared']} auction codes read different evidence "
            f"{candidates['codes_differ'][:10]}"
        )
    signals = summary.get("signal_fidelity")
    if signals is not None:
        lines.append(
            f"signal fidelity vs {signals.get('other')}: "
            + (
                signals["refused"]
                if "refused" in signals
                else f"identical={signals['identical']} common={signals['common']} "
                f"until {signals['until_local']}; only this run {signals['only_this_run']}; "
                f"only the other run {signals['only_other_run']}"
            )
        )
        for code, entry in (signals.get("by_code") or {}).items():
            lines.append(
                f"  {code}: {json.dumps(entry, default=_json_default, ensure_ascii=False)}"
            )
    profile = summary.get("profile") or {}
    if profile:
        lines.append(
            f"profile: drive {profile['drive_seconds']}s = roles {profile['role_seconds']}s + "
            f"harness {profile['harness_seconds']}s over {profile['ticks']} ticks; "
            f"slowest: {profile['slowest_roles']}"
        )
        for phase, entry in profile["tick_seconds"].items():
            lines.append(
                f"  ticks {phase:<10} n={entry['ticks']:<4} total={entry['total']:>9}s "
                f"p50={entry['p50']:>7}s p95={entry['p95']:>7}s"
            )
    for label, role in (summary.get("roles") or {}).items():
        lines.append(
            f"  {label:<40} it={role['iterations']:<4} wall={role['wall_seconds']:>8}s "
            f"max={role['max_iteration_seconds']:>7}s out={role['output_sequence']} "
            f"fail={role['total_failures']} crashes={len(role['crashes'])} "
            f"errors={len(role['errors'])} first_output={role['first_output_at']}"
        )
        for message, entry in list(role["errors"].items())[:5]:
            span = f"{entry['first']}..{entry.get('last')}"
            lines.append(f"      error x{entry['count']} {span}: {message[:240]}")
    if summary.get("fatal"):
        lines.append("fatal: " + summary["fatal"].strip().splitlines()[-1])
    for line in lines:
        out(line)


#: the line a dry plan ends with, for `route_a_replay_days.py` to read
DRY_PLAN_PREFIX = "DRY-PLAN "


def dry_plan(
    arguments: argparse.Namespace,
    *,
    trade_date: date,
    runtime_root: Path,
    replica: Path,
    production_inputs: Path,
    sandbox: Path,
    cache_root: Path,
    tushare_mode: str | None,
    out: Callable[[str], None],
) -> int:
    """For this one day: every input, recorded or synthesized and from what, and whether it
    can be produced. Writes nothing and asks Tushare nothing; 2 when some input cannot be.

    It resolves the day exactly as a run does (`read_recorded_day`, `probe_replica`,
    `replica_refusals`), so a day the dry plan passes is refused by a run only for what
    only a run can find out: a Tushare answer, or a live capture's own refusal.
    """

    def describe(path: Path) -> str:
        try:
            observed = path.stat()
        except OSError as error:
            return f"MISSING ({error.strerror})"
        modified = datetime.fromtimestamp(observed.st_mtime, _SHANGHAI)
        return f"{observed.st_size} bytes, mtime {modified:%Y-%m-%d %H:%M:%S}"

    audit = ProductionAudit(roots=())
    plan: dict[str, Any] = {"trade_date": trade_date, "cannot": [], "inputs": {}}
    out(f"dry plan for trade date {trade_date} (nothing is written, Tushare is not asked)")
    out(f"sandbox would be: {sandbox} (0700)")
    tushare = tushare_mode is not None
    out(f"Tushare: {tushare_mode or 'off'}" + (f" (cache {cache_root})" if tushare else ""))
    recorded: RecordedDay | None = None
    try:
        recorded = read_recorded_day(
            runtime_root=runtime_root,
            production_inputs=production_inputs,
            trade_date=trade_date,
            reference_sequence=arguments.reference_sequence,
            auction_sequence=arguments.auction_sequence,
            audit=audit,
            synthesize=arguments.synthesize_sources,
            tushare=tushare,
            refuse_without_tushare=False,
        )
    except ReplayRefusedError as error:
        plan["cannot"].append(str(error))
    if recorded is not None:
        plan["cannot"].extend(recorded.tushare_needed)
        plan["inputs"] = recorded.provenance
        out("recorded inputs, read with plain open(rb) and copied into the sandbox:")
        if recorded.reference_manifest_path is not None and not recorded.synthesize_reference:
            for path in (
                recorded.reference_manifest_path,
                recorded.reference_manifest_path.with_suffix(".payload"),
            ):
                out(f"  reference_slow {path}: {describe(path)}")
        if not recorded.synthesize_auction:
            for _, _, manifest in recorded.auction_records:
                for path in (manifest, manifest.with_suffix(".payload")):
                    out(f"  auction_match  {path}: {describe(path)}")
        if recorded.calendar_path is not None:
            out(f"  calendar       {recorded.calendar_path}: {describe(recorded.calendar_path)}")
        out(f"  inputs doc     {production_inputs}: {describe(production_inputs)}")
        for label, fingerprint in recorded.inputs_fingerprints.items():
            path = Path(str(fingerprint["path"]))
            out(f"  {label:<14} {path}: {describe(path)}")
        out("per input:")
        for label, entry in sorted(recorded.provenance.items()):
            details = {
                key: value
                for key, value in entry.items()
                if key not in {"origin", "source", "spool_holds", "note"}
                and value is not None
                and not (key == "recorded_batch" and value == entry.get("source"))
            }
            out(f"  {label:<17} {entry.get('origin', '?'):<11} {entry.get('source')}")
            if entry.get("spool_holds") is not None and entry.get("origin") == "synthesized":
                out(f"                    host spool holds: {entry['spool_holds'] or 'nothing'}")
            if details:
                rendered = json.dumps(details, default=_json_default, ensure_ascii=False)
                out(f"                    {rendered}")
        try:
            probe = sources.probe_replica(
                replica=replica, trade_date=trade_date, calendar=recorded.calendar, audit=audit
            )
        except Exception as error:  # noqa: BLE001 - reported as a refusal
            plan["cannot"].append(f"replica: cannot be read: {type(error).__name__}: {error}")
        else:
            plan["replica_probe"] = probe
            out(f"replica {replica} ({describe(replica)}), through ATTACH ... (READ_ONLY):")
            out(f"  prior five sessions' daily_bar rows: {probe['prior_five']}")
            out(
                f"  adj_factor rows on {probe['prior_trade_date']}: "
                f"{probe.get('prior_adj_factor_rows')}; {trade_date}'s 1min minute_bar: "
                f"{probe['trade_date_minute_rows']} rows, {probe['trade_date_minute_codes']} codes"
            )
            plan["cannot"].extend(
                f"replica: {reason}"
                for reason in sources.replica_refusals(
                    probe,
                    trade_date=trade_date,
                    synthesize_reference=recorded.synthesize_reference,
                    tushare_minutes=tushare,
                )
            )
        if tushare and (recorded.synthesize_reference or recorded.synthesize_auction):
            cache = sources.TushareCache(cache_root, offline=True)
            cached = sources.cached_answers(
                cache,
                trade_date,
                reference=recorded.synthesize_reference,
                auction=recorded.synthesize_auction,
            )
            plan["tushare_cache"] = cached
            out(f"Tushare answers already cached: {cached}")
            if tushare_mode == "offline":
                plan["cannot"].extend(
                    f"tushare: --tushare-offline and {name} for {trade_date} is not cached in "
                    f"{cache_root}"
                    for name, present in cached.items()
                    if name != "stk_mins_codes" and not present
                )
    out(
        f"clock: {arguments.start} + {arguments.phase_seconds}s phase, "
        f"every {arguments.step_seconds}s, until {arguments.until}; "
        f"reference publisher rounds from {arguments.reference_start}; serving every "
        f"{arguments.serving_every_ticks} tick(s)"
    )
    out(f"roles, in order: {', '.join(CHAIN)}")
    audited = audit.finish()
    plan["production_untouched"] = not (
        audited["changed_during_our_read"] or audited["changed_in_place_later"]
    )
    plan["paths_read"] = audited["paths_read"]
    if plan["cannot"]:
        out(f"CANNOT REPLAY {trade_date}:")
        for reason in plan["cannot"]:
            out(f"  - {reason}")
    else:
        out(f"every input of {trade_date} can be produced")
    out(
        DRY_PLAN_PREFIX
        + json.dumps(plan, default=_json_default, sort_keys=True, ensure_ascii=False)
    )
    return 2 if plan["cannot"] else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--replica", type=Path, default=DEFAULT_REPLICA)
    parser.add_argument("--production-inputs", type=Path, default=DEFAULT_PRODUCTION_INPUTS)
    parser.add_argument("--reference-sequence", type=int, default=None)
    parser.add_argument("--auction-sequence", type=int, default=None)
    parser.add_argument("--start", default="09:15:00", help="first tick, local (default 09:15)")
    parser.add_argument("--until", default="15:05:00", help="last tick, local (default 15:05)")
    parser.add_argument("--step-seconds", type=float, default=60.0)
    parser.add_argument(
        "--phase-seconds",
        type=float,
        default=7.0,
        help="offset of every tick from the round minute, so no instant sits on a boundary",
    )
    parser.add_argument("--reference-start", default="09:21:26")
    parser.add_argument(
        "--minute-lag-seconds",
        type=float,
        default=0.0,
        help="serve a bar once trade_time <= now - lag (default 0: the bar whose label the "
        "clock has reached, as rt_min does; 60 = only bars complete under bar-start labels)",
    )
    parser.add_argument(
        "--assume-listing-classification",
        action="store_true",
        help="accepted and ignored (a warning is printed): the reference-slow publisher "
        "writes market / exchange / instrument_class / security_class on LISTING_STATUS "
        "itself since package AI, so there is nothing left to stub",
    )
    parser.add_argument("--replica-synced-at", default="09:12:00")
    parser.add_argument("--replica-daily-sessions", type=int, default=60)
    parser.add_argument("--replica-minute-sessions", type=int, default=1)
    parser.add_argument("--generations", type=int, choices=(1, 2), default=1)
    parser.add_argument(
        "--keep-serving-generations",
        type=int,
        default=4,
        help="serving generations kept in the sandbox besides current/previous (0 = all)",
    )
    parser.add_argument("--role-timeout-seconds", type=float, default=900.0)
    parser.add_argument("--max-restarts", type=int, default=3)
    parser.add_argument(
        "--tushare",
        action="store_true",
        help="ask Tushare (TUSHARE_TOKEN_MAIN) for what the replica and the host spool lack: "
        "stk_mins for missing minute codes, and the source answers a synthesized batch needs",
    )
    parser.add_argument(
        "--tushare-offline",
        action="store_true",
        help="as --tushare, but answer only from --tushare-cache and never make a request; "
        "an answer the cache lacks refuses the day (a minute code stays missing)",
    )
    parser.add_argument(
        "--tushare-cache",
        type=Path,
        default=None,
        help="where Tushare answers are kept, one parquet per (endpoint, day[, code]) "
        "(default: <replay-root>/tushare-cache; shared by every run under that root)",
    )
    parser.add_argument(
        "--synthesize-sources",
        action="store_true",
        help="synthesize the reference-slow and auction-match batches even when the host "
        "recorded them, and report how far they are from the recorded ones",
    )
    parser.add_argument(
        "--serving-every-ticks",
        type=int,
        default=1,
        help="step serving.publisher.v1 only every N ticks (and on the last one); N > 1 "
        "marks the run not production-faithful (production publishes every 30 s)",
    )
    parser.add_argument(
        "--cprofile",
        action="append",
        default=None,
        metavar="SERVICE_ID[,SERVICE_ID]",
        help="cProfile these roles' iterations; <sandbox>/profile/<service>.<n>.pstats and "
        "a top-40 text beside it",
    )
    parser.add_argument(
        "--compare-signals-with",
        type=Path,
        default=None,
        metavar="SUMMARY_OR_SANDBOX",
        help="diff this run's serving signals against another run of the same day (its "
        "summary.json or sandbox), up to the earlier run's last tick: summary.signal_fidelity",
    )
    parser.add_argument("--dry-plan", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.step_seconds <= 0:
        parser.error("--step-seconds must be positive")
    if arguments.serving_every_ticks < 1:
        parser.error("--serving-every-ticks must be at least 1")
    import rquant

    print(f"rquant imported from {Path(rquant.__file__).resolve().parent}")
    try:
        return run_replay(arguments)
    except (ReplayRefusedError, sources.InputUnavailableError) as error:
        print(f"REPLAY REFUSED: {error}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
