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
  run: their recorded batches are re-sealed under the sandbox commit and throwaway keys;
* **the service loop's wait** -- one iteration per tick, handed out by the driver, instead of
  the manifest interval;
* **the unit sandbox** -- roles run as threads of this process, one at a time, without
  systemd, namespaces, `ReadWritePaths` or the package-L Python sandbox (see the report).

Nothing outside the sandbox is written. Production files are opened read-only (plain
`open(..., "rb")`, DuckDB `read_only=True` through `ATTACH ... (READ_ONLY)`), an audit hook
refuses any Python-level write under a production root, and every production path read is
stat'ed before, right after and at the end of the run.

    PYTHONDONTWRITEBYTECODE=1 <checkout>/.venv/bin/python \\
        <checkout>/scripts/route_a_day_replay.py --trade-date 2026-09-24 \\
        --replay-root /home/lighthouse/replay [--tushare] [--until 11:30]

Exit status: 0 when a same-day serving generation exists at the end (even with no signal);
1 when none does, or when any role crashed (its thread ended with an exception); 2 on a
usage error or a refused setup.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterable, Sequence
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
    trade_date: date
    reference_envelope: Any
    reference_snapshot: Any
    reference_manifest_path: Path
    calendar: Any
    calendar_bytes: bytes
    calendar_path: Path
    auction_records: list[tuple[Any, bytes, Path]]
    auction_target: Any
    universe: dict[str, Any] | None
    routing_policy: bytes
    trade_calendar: bytes
    history: bytes
    history_source: Path
    inputs_fingerprints: dict[str, dict[str, str | bool]]


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


def _read_reference(
    runtime_root: Path,
    trade_date: date,
    sequence: int | None,
    audit: ProductionAudit,
) -> tuple[Any, Any, Path]:
    from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
    from rquant.strict_json import strict_model_validate_canonical_json

    directory = runtime_root / "live" / "reference-slow" / "batches" / "reference_slow"
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
        seen.append(f"{envelope.sequence}:{snapshot.target_trade_date}")
        if snapshot.target_trade_date == trade_date:
            return envelope, snapshot, manifest
    raise ReplayRefusedError(
        f"no reference-slow batch in {directory} targets {trade_date} (seen: {seen})"
    )


def _read_auction(
    runtime_root: Path,
    trade_date: date,
    sequence: int | None,
    audit: ProductionAudit,
) -> tuple[list[tuple[Any, bytes, Path]], Any]:
    from rquant.live_contracts import BatchQualityStatus

    directory = runtime_root / "live" / "auction-match" / "batches" / "auction_match"
    envelopes = _envelopes(directory, audit)
    today = [
        envelope
        for envelope, _ in envelopes
        if envelope.event_time_end.astimezone(_SHANGHAI).date() == trade_date
        and envelope.quality_status is BatchQualityStatus.PUBLISHED
        and (sequence is None or envelope.sequence == sequence)
    ]
    if not today:
        raise ReplayRefusedError(
            f"no published auction-match batch for {trade_date} in {directory} "
            f"(sequences: {[envelope.sequence for envelope, _ in envelopes]})"
        )
    target = max(today, key=lambda envelope: envelope.sequence)
    records: list[tuple[Any, bytes, Path]] = []
    for envelope, manifest in sorted(envelopes, key=lambda item: item[0].sequence):
        if envelope.sequence > target.sequence:
            break
        payload = audit.read_bytes(manifest.with_suffix(".payload"))
        if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
            raise ReplayRefusedError(f"{manifest}: payload does not match content_sha256")
        records.append((envelope, payload, manifest))
    return records, target


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


def read_recorded_day(
    *,
    runtime_root: Path,
    production_inputs: Path,
    trade_date: date,
    reference_sequence: int | None,
    auction_sequence: int | None,
    audit: ProductionAudit,
) -> RecordedDay:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.strict_json import strict_json_loads

    envelope, snapshot, reference_manifest = _read_reference(
        runtime_root, trade_date, reference_sequence, audit
    )
    calendar_sha = snapshot.source_snapshot_ids["calendar"]
    calendar_path = (
        runtime_root / "authorities" / "market-calendar" / "generations" / f"{calendar_sha}.json"
    )
    calendar_bytes = audit.read_bytes(calendar_path)
    #: the loader's own decoding (`load_market_calendar_authority`), not a canonical-form one
    calendar = MarketCalendarAuthority.model_validate(strict_json_loads(calendar_bytes))
    if calendar.content_sha256 != calendar_sha:
        raise ReplayRefusedError("the calendar generation does not match the batch's calendar id")
    if trade_date not in calendar.open_dates:
        raise ReplayRefusedError(f"{trade_date} is not an open date of calendar {calendar_sha}")
    auction_records, auction_target = _read_auction(
        runtime_root, trade_date, auction_sequence, audit
    )
    universe = _read_universe(runtime_root, trade_date, audit)

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
    return RecordedDay(
        trade_date=trade_date,
        reference_envelope=envelope,
        reference_snapshot=snapshot,
        reference_manifest_path=reference_manifest,
        calendar=calendar,
        calendar_bytes=calendar_bytes,
        calendar_path=calendar_path,
        auction_records=auction_records,
        auction_target=auction_target,
        universe=universe,
        routing_policy=loaded["routing_policy"][0],
        trade_calendar=loaded["trade_calendar"][0],
        history=loaded["history"][0],
        history_source=loaded["history"][1],
        inputs_fingerprints=fingerprints,
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


def tushare_day_fetcher(token: str, trade_date: date) -> Callable[[str], Any]:
    """`stk_mins` for one code over the trade date, through `rquant.adapter.tushare`."""

    from rquant.adapter.tushare import TushareAdapter

    adapter = TushareAdapter(token=token, backup_token="")
    start = datetime.combine(trade_date, clock_time(9, 0))
    end = datetime.combine(trade_date, clock_time(15, 30))

    def fetch(code: str) -> Any:
        frame = adapter.stk_mins(code, "1min", start, end)
        if frame is None or not len(frame):
            return frame
        frame = frame.copy()
        if "source" not in frame.columns:
            frame["source"] = "tushare"
        return frame[[column for column in _MINUTE_COLUMNS if column in frame.columns]]

    return fetch


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

    def __init__(self) -> None:
        self._grant = threading.Semaphore(0)
        self.done = threading.Semaphore(0)
        self._stopped = False
        self.iterations = 0

    def is_set(self) -> bool:
        return self._stopped

    def set(self) -> None:
        self._stopped = True
        self._grant.release()

    def grant(self) -> None:
        self._grant.release()

    def wait(self, timeout: float | None = None) -> bool:  # noqa: ARG002 - ticks, not time
        self.iterations += 1
        self.done.release()
        self._grant.acquire()
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
    ) -> None:
        self.states = states
        self.clock = clock
        self.role_timeout_seconds = role_timeout_seconds
        self.max_restarts = max_restarts
        self.log = log
        self.hung: str | None = None

    def _start(self, state: RoleState) -> None:
        import rquant.runtime_service_main as service_main

        baton = TickBaton()
        state.baton = baton
        state.crash = None
        arguments = service_main.build_parser().parse_args(state.argv)

        def target() -> None:
            _THREAD.baton = baton
            try:
                service_main.run(arguments)
            except BaseException as error:  # noqa: BLE001 - recorded, reported, exit 1
                state.crash = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )
            finally:
                baton.done.release()

        state.thread = threading.Thread(target=target, name=state.label, daemon=True)
        state.thread.start()

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
    """Every plan under the sandbox's `control/schema-rollouts`, by channel and phase."""

    from rquant.runtime_deployment_bundle import load_runtime_schema_rollout

    rollouts = runtime_root / "control" / "schema-rollouts"
    plans: dict[str, str] = {}
    if rollouts.is_dir():
        for directory in sorted(rollouts.iterdir()):
            authority, store = load_runtime_schema_rollout(
                runtime_root, plan_id=directory.name, read_only=True
            )
            plans[authority.plan.dataset_id] = store.get_state(directory.name).phase.value
    return {
        "plans": len(plans),
        "receipt_plan_ids": len(receipt.schema_rollout_plan_ids),
        "phases": plans,
    }


def build_world(
    *,
    sandbox: Path,
    recorded: RecordedDay,
    monkeypatch: Any,
    generations: int,
    replica_extract: Callable[[Path], dict[str, Any]],
    log: Callable[[str], None],
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

    facts["reference"] = reseal_reference_batch(
        route, recorded, key_root=bundle_root, commit=world.commit
    )
    facts["auction"] = republish_auction_batches(route, recorded, commit=world.commit)
    credentials = deliver_credentials(route, sandbox / "credentials", monkeypatch)
    recorder = confirm_deliveries_without_the_network(monkeypatch)
    return route, facts, credentials, recorder


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

    from rquant.live_spool import (
        LiveBatchSpool,
        ReferenceSourceBatchSigner,
        ReferenceSourceBatchVerifier,
    )
    from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
    from rquant.reference_slow_runtime import capture_reference_slow_batch
    from rquant.runtime_contracts import canonical_sha256

    original = recorded.reference_envelope
    snapshot = recorded.reference_snapshot
    identity = snapshot.model_dump(mode="python", exclude={"content_sha256"})
    identity["producer_commit"] = commit
    resealed = ReferenceSlowSourceSnapshot.model_validate(
        {**identity, "content_sha256": canonical_sha256(identity)}
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
    spool = LiveBatchSpool(spool_root, source_signer=signer, source_verifier=verifier)
    prepared_at = original.available_at - _OLD_SOURCE_GUARD
    result = capture_reference_slow_batch(
        spool=spool,
        calendar=recorded.calendar,
        observed_at=snapshot.captured_at,
        producer_commit=commit,
        producer_version=original.producer_version,
        snapshot_loader=lambda: resealed,
        completion_clock=lambda: prepared_at,
    )
    from rquant.live_contracts import LiveChannel

    (record,) = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    return {
        "host_sequence": original.sequence,
        "host_available_at": original.available_at,
        "host_producer_commit": original.producer_commit,
        "captured_at": snapshot.captured_at,
        "securities": len(snapshot.security_facts),
        "daily_facts": len(snapshot.daily_facts),
        "resealed_available_at": record.envelope.available_at,
        "output_sequence": result.output_sequence,
    }


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
    return {
        "signals_rows_total": total,
        "signals_rows_today": today,
        "signals_filter": f">= {literal}",
    }


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


def run_replay(arguments: argparse.Namespace, *, out: Callable[[str], None] = print) -> int:
    import tempfile

    import pytest

    global _PROTECTED_ROOTS

    trade_date = date.fromisoformat(arguments.trade_date)
    runtime_root = arguments.runtime_root.resolve()
    replica = arguments.replica.resolve()
    production_inputs = arguments.production_inputs.resolve()
    protected = sorted(
        {
            str(runtime_root),
            str(replica.parent),
            str(production_inputs.parent),
            str(runtime_root.parent),
        }
    )
    replay_root = arguments.replay_root.resolve()
    stamp = datetime.now(_SHANGHAI).strftime("%Y%m%dT%H%M%S")
    sandbox = replay_root / f"{stamp}-{secrets.token_hex(3)}"
    for root in protected:
        candidate = Path(root)
        if sandbox.is_relative_to(candidate) or candidate.is_relative_to(sandbox):
            raise ReplayRefusedError(f"the replay root {sandbox} overlaps production path {root}")

    token = os.environ.get("TUSHARE_TOKEN_MAIN") if arguments.tushare else None
    if arguments.tushare and not token:
        raise ReplayRefusedError("--tushare needs TUSHARE_TOKEN_MAIN in the process environment")

    if arguments.dry_plan:
        return dry_plan(
            arguments,
            trade_date=trade_date,
            runtime_root=runtime_root,
            replica=replica,
            production_inputs=production_inputs,
            sandbox=sandbox,
            out=out,
        )

    _PROTECTED_ROOTS = tuple(protected)
    sys.addaudithook(_audit_hook)
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
    summary: dict[str, Any] = {
        "trade_date": trade_date,
        "sandbox": str(sandbox),
        "mode": {
            "step_seconds": arguments.step_seconds,
            "phase_seconds": arguments.phase_seconds,
            "until": arguments.until,
            "tushare": bool(arguments.tushare),
            "generations": arguments.generations,
            "minute_lag_seconds": arguments.minute_lag_seconds,
            "assume_listing_classification": bool(arguments.assume_listing_classification),
            "runner": "in-process threads, one role at a time, no unit sandbox",
        },
        "stage_seconds": {},
        "host": host_facts,
    }
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
        )
        summary["inputs"] = {
            "reference_batch": str(recorded.reference_manifest_path),
            "calendar": str(recorded.calendar_path),
            "auction_batches": [str(path) for _, _, path in recorded.auction_records],
            "production_inputs": recorded.inputs_fingerprints,
        }
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
        )
        summary["world"] = facts
        summary["stage_seconds"]["build_world"] = round(time.monotonic() - started, 2)

        import pandas as pd

        clock = ReplayClock(_local(trade_date, arguments.start))
        adapter = ReplayMinuteAdapter(
            pd.read_parquet(minutes_path),
            clock=clock,
            lag_seconds=arguments.minute_lag_seconds,
            tushare_fetch=tushare_day_fetcher(token, trade_date) if token else None,
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
        for index, (moment, kind) in enumerate(events):
            if kind == "reference":
                if reference.first_output_at is not None:
                    continue
                if not runner.step(reference, moment):
                    stopped = True
                    break
                continue
            for position, state in enumerate(states):
                if not runner.step(state, moment + timedelta(seconds=spacing * position)):
                    stopped = True
                    break
                if state.role == "serving_publisher":
                    retention.after_step()
            if stopped:
                break
            if index % max(1, int(1800 / arguments.step_seconds)) == 0 or index == len(events) - 1:
                out(
                    f"  {moment.astimezone(_SHANGHAI):%H:%M:%S} "
                    f"elapsed {time.monotonic() - started:6.0f}s "
                    f"failing={_failing(states)} "
                    f"crashes={sum(len(state.crashes) for state in states)}"
                )
        summary["stage_seconds"]["drive_the_day"] = round(time.monotonic() - started, 2)
        runner.stop_all()
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
        }
        exit_code = 0 if serving_ok and shadow_ok and not crashed and runner.hung is None else 1
    except ReplayRefusedError:
        raise
    except Exception as error:  # noqa: BLE001 - reported in the summary, exit 1
        summary["fatal"] = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        out(f"FATAL: {type(error).__name__}: {error}")
        exit_code = 1
    finally:
        if runner is not None:
            runner.stop_all()
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


def dry_plan(
    arguments: argparse.Namespace,
    *,
    trade_date: date,
    runtime_root: Path,
    replica: Path,
    production_inputs: Path,
    sandbox: Path,
    out: Callable[[str], None],
) -> int:
    """What would be read and copied, and where the sandbox would go. Writes nothing."""

    def describe(path: Path) -> str:
        try:
            observed = path.stat()
        except OSError as error:
            return f"MISSING ({error.strerror})"
        modified = datetime.fromtimestamp(observed.st_mtime, _SHANGHAI)
        return f"{observed.st_size} bytes, mtime {modified:%Y-%m-%d %H:%M:%S}"

    out(f"dry plan for trade date {trade_date} (nothing is written)")
    out(f"sandbox would be: {sandbox} (0700)")
    reference_dir = runtime_root / "live" / "reference-slow" / "batches" / "reference_slow"
    auction_dir = runtime_root / "live" / "auction-match" / "batches" / "auction_match"
    out("read (plain open rb), copied into the sandbox:")
    for directory in (reference_dir, auction_dir):
        if directory.is_dir():
            for path in sorted(directory.iterdir()):
                if path.suffix in {".json", ".payload"} and path.stem.isdigit():
                    out(f"  {path}: {describe(path)}")
        else:
            out(f"  {directory}: MISSING")
    for directory in (
        runtime_root / "authorities" / "market-calendar" / "generations",
        runtime_root / "authorities" / "auction-universe" / "generations",
    ):
        count = len(list(directory.glob("*.json"))) if directory.is_dir() else 0
        out(f"  {directory}/*.json: {count} generations (the one named / effective {trade_date})")
    out(f"  {production_inputs}: {describe(production_inputs)}")
    try:
        document = json.loads(production_inputs.read_text(encoding="utf-8"))
        for key in (
            "routing_policy_path",
            "trade_calendar_path",
            "historical_minutes_snapshot_path",
        ):
            path = Path(str(document.get(key)))
            out(f"  {key} -> {path}: {describe(path)}")
    except (OSError, ValueError) as error:
        out(f"  cannot read the inputs document: {error}")
    out("read through DuckDB ATTACH ... (READ_ONLY), extracted into the sandbox:")
    out(f"  {replica}: {describe(replica)}")
    out(f"    daily_bar: the {arguments.replica_daily_sessions} sessions before {trade_date}")
    out("    screen_result: the 10 sessions before the trade date")
    out(
        f"    minute_bar: the {arguments.replica_minute_sessions} session(s) before it, and "
        f"{trade_date}'s 1min rows kept aside for the minute replay"
    )
    out(f"tushare stk_mins for codes the replica lacks: {'yes' if arguments.tushare else 'no'}")
    out(
        f"clock: {arguments.start} + {arguments.phase_seconds}s phase, "
        f"every {arguments.step_seconds}s, until {arguments.until}; "
        f"reference publisher rounds from {arguments.reference_start}"
    )
    out(f"roles, in order: {', '.join(CHAIN)}")
    return 0


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
    parser.add_argument("--tushare", action="store_true")
    parser.add_argument("--dry-plan", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.step_seconds <= 0:
        parser.error("--step-seconds must be positive")
    import rquant

    print(f"rquant imported from {Path(rquant.__file__).resolve().parent}")
    try:
        return run_replay(arguments)
    except ReplayRefusedError as error:
        print(f"REPLAY REFUSED: {error}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
