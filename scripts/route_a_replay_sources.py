"""Source inputs for `route_a_day_replay.py` on a day the host holds no recording of.

The host keeps one batch per source and session, and keeps few of them: on 2026-09-25 the
reference-slow spool held a single batch (seq 0, captured 09-24 09:21) and the auction-match
spool two (seq 0, 09-23, zero rows and DEGRADED; seq 1, 09-24, PUBLISHED). So the replay had
a faithful input for 09-24 only, and refused every other day at its first read. This module
builds the two source batches of any day the read-only replica still covers, the way the live
sources build them, out of what can still be had for that day -- through the live code itself,
never a copy of it:

* **reference-slow** -- `reference_slow_source.capture_reference_slow_source_snapshot`, the live
  source's capture, over (a) a sandbox extract of the replica *as it stood before the session*
  (`daily_bar` joined to `adj_factor` for the prior open session, plus the projection tables the
  capture reads), and (b) the Tushare answers the live capture asks for: `stock_st(D)`,
  `adj_factor(D)` and `suspend_d(D)` are historical endpoints and exact; `stock_basic(L/D/P)`
  only answers today's listing, which is the one anachronism (names, and so the name-derived
  ST flag, are today's; `stock_st(D)` stays the authority). Sealed by the live
  `reference_slow_runtime.capture_reference_slow_batch` into the sandbox spool, observed at the
  first round of the 09:20-09:25 window.
* **auction universe** -- `auction_universe_source.publish_auction_universe_from_daily_snapshot`
  over the same extract (the prior session's `daily_bar` codes), at the universe publisher's
  last round before its 09:15 protection window, unless the host holds a generation for the day.
* **auction-match** -- `AuctionMatchGateway.capture_once`, the live source's capture, fed by
  Tushare's historical `stk_auction(D)` through the live adapter method (#277's `pre_close`
  column included), at the source's first capture instant.
* **market calendar** -- the host generation that was current before the day opened; when every
  host generation that opens the day was generated after it, the newest one re-dated to the
  evening before (labelled: every role refuses a calendar generated after its clock).

Tushare is only ever reached through `TushareCache`: one parquet per (endpoint, key) under the
replay root, so a second run of a day -- or the next day of a multi-day run -- asks nothing
again, and `--tushare-offline` replays from the cache alone. Every input a run uses is
labelled in `summary.json` (`inputs.provenance`) as recorded or synthesized, with its source.
An input that cannot be produced raises `InputUnavailableError`, which the replay turns into a
refusal (exit 2): no day ever borrows another day's batch.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_SHANGHAI = ZoneInfo("Asia/Shanghai")

#: The live reference source's first round of its 09:20-09:25 window, at the replay's
#: default tick phase; its capture's own duration; and the pause before sealing starts. The
#: sealed batch is visible `_SOURCE_VISIBILITY_GUARD` (30 s) later, 09:20:57, before the
#: publisher's first replayed round at `--reference-start` (09:21:26).
SYNTHETIC_REFERENCE_OBSERVED = "09:20:07"
SYNTHETIC_REFERENCE_CAPTURE_SECONDS = 15.0
SYNTHETIC_REFERENCE_PREPARE_SECONDS = 5.0
#: The auction universe publisher's last round before its 09:15-15:10 protection window.
SYNTHETIC_UNIVERSE_OBSERVED = "09:10:07"
#: The earliest instant any synthesized input is observed at: a calendar generated later
#: than this is future evidence for the whole replayed day.
CALENDAR_NOT_AFTER = SYNTHETIC_UNIVERSE_OBSERVED
#: The deepest per-code look-back of any query the reference-slow capture makes: its
#: `daily_bar` projection reads each liquid code's last 120 rows (`rn <= 120`), and
#: `market_liquidity` / the liquidity ranking read each code's last 5 -- rows counted *per
#: code*, not sessions of the calendar, so a code that stopped trading long ago still has
#: them (host 2026-09-24: 54 such codes in `market_liquidity`).
REFERENCE_EVIDENCE_DAILY_ROWS_PER_CODE = 120
#: The five sessions `auction_gap_candidate_input` reads volumes for.
PRIOR_SESSIONS_AUCTION_GAP = 5


class InputUnavailableError(RuntimeError):
    """One input of the day cannot be produced; the replay refuses the day (exit 2)."""


class TushareCacheMissError(LookupError):
    """`--tushare-offline` and the answer is not in the cache."""


def local_instant(day: date, value: str | clock_time) -> datetime:
    parsed = value if isinstance(value, clock_time) else clock_time.fromisoformat(value)
    return datetime.combine(day, parsed, tzinfo=_SHANGHAI).astimezone(UTC)


def _day_key(day: date) -> str:
    return day.strftime("%Y%m%d")


# ---------------------------------------------------------------------------------------
# Tushare, through a disk cache inside the replay root
# ---------------------------------------------------------------------------------------


def default_adapter_factory(token: str) -> Any:
    """The live adapter, primary token only (the backup token lacks the paid endpoints)."""

    from rquant.adapter.tushare import TushareAdapter

    return TushareAdapter(token=token, backup_token="")


@dataclass
class TushareCache:
    """Tushare answers on disk: `<root>/<endpoint>/<key>.parquet` plus a `.json` receipt.

    The receipt says when the answer was fetched and what it hashed to, so a replay that
    reads it can say so. Writes are atomic (a private temporary name, then `os.replace`), so
    two replays of different days sharing the cache never see half a file.
    """

    root: Path
    token: str | None = None
    offline: bool = False
    adapter_factory: Callable[[str], Any] = default_adapter_factory
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    ledger: list[dict[str, Any]] = field(default_factory=list)
    _adapter: Any = None

    def path(self, endpoint: str, key: str) -> Path:
        return self.root / endpoint / f"{key}.parquet"

    def load(self, endpoint: str, key: str) -> tuple[Any, dict[str, Any]] | None:
        import pandas as pd

        path = self.path(endpoint, key)
        if not path.is_file():
            return None
        frame = pd.read_parquet(path)
        receipt_path = path.with_suffix(".json")
        receipt: dict[str, Any] = {}
        if receipt_path.is_file():
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        return frame, receipt

    def store(
        self, endpoint: str, key: str, frame: Any, *, fetched_at: datetime | None = None
    ) -> Path:
        path = self.path(endpoint, key)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        token = f"{os.getpid()}.{secrets.token_hex(4)}"
        temporary = path.with_name(f".{path.name}.{token}.tmp")
        frame.to_parquet(temporary, index=False)
        payload = temporary.read_bytes()
        os.replace(temporary, path)
        receipt = {
            "endpoint": endpoint,
            "key": key,
            "rows": int(len(frame)),
            "columns": [str(column) for column in frame.columns],
            "fetched_at": (fetched_at or self.clock()).isoformat(),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        receipt_temporary = path.with_name(f".{path.stem}.json.{token}.tmp")
        receipt_temporary.write_text(json.dumps(receipt, sort_keys=True), encoding="utf-8")
        os.replace(receipt_temporary, path.with_suffix(".json"))
        return path

    def keys(self, endpoint: str, prefix: str = "") -> list[str]:
        directory = self.root / endpoint / prefix if prefix else self.root / endpoint
        if not directory.is_dir():
            return []
        return sorted(
            str(path.relative_to(self.root / endpoint).with_suffix(""))
            for path in directory.rglob("*.parquet")
            if not path.name.startswith(".")
        )

    def adapter(self) -> Any:
        if self.offline:
            raise TushareCacheMissError("--tushare-offline: no Tushare request is made")
        if not self.token:
            raise TushareCacheMissError("no Tushare token (TUSHARE_TOKEN_MAIN) in the environment")
        if self._adapter is None:
            self._adapter = self.adapter_factory(self.token)
        return self._adapter

    def get(
        self,
        endpoint: str,
        key: str,
        request: Callable[[Any], Any],
        *,
        cache_empty: bool,
    ) -> Any:
        """The cached answer, else one request (cached when it has rows or `cache_empty`)."""

        import pandas as pd

        cached = self.load(endpoint, key)
        if cached is not None:
            frame, receipt = cached
            self._record(endpoint, key, "cache", len(frame), receipt.get("fetched_at"))
            return frame
        if self.offline:
            raise TushareCacheMissError(
                f"{endpoint}/{key} is not in the Tushare cache {self.root} and "
                "--tushare-offline forbids a request"
            )
        frame = request(self.adapter())
        if frame is None:
            frame = pd.DataFrame()
        fetched_at = self.clock()
        if len(frame) or cache_empty:
            self.store(endpoint, key, frame, fetched_at=fetched_at)
        self._record(endpoint, key, "tushare", len(frame), fetched_at.isoformat())
        return frame

    def _record(
        self, endpoint: str, key: str, origin: str, rows: int, fetched_at: str | None
    ) -> None:
        self.ledger.append(
            {
                "endpoint": endpoint,
                "key": key,
                "origin": origin,
                "rows": int(rows),
                "fetched_at": fetched_at,
            }
        )

    def summary(self) -> dict[str, Any]:
        """Per endpoint: answers from the cache, from Tushare, and empty ones; `stk_mins`
        is counted, the few whole-day endpoints are listed one by one."""

        by_endpoint: dict[str, dict[str, int]] = {}
        listed: list[dict[str, Any]] = []
        for entry in self.ledger:
            counts = by_endpoint.setdefault(
                entry["endpoint"], {"cache": 0, "tushare": 0, "empty": 0}
            )
            counts[entry["origin"]] += 1
            if not entry["rows"]:
                counts["empty"] += 1
            if entry["endpoint"] != "stk_mins":
                listed.append(entry)
        return {
            "root": str(self.root),
            "offline": self.offline,
            "by_endpoint": by_endpoint,
            "whole_day_answers": listed,
        }


class CachedDaySource:
    """What the live reference source and auction-match source ask Tushare, for one day.

    The method names and signatures are the live adapter's (`ReferenceSlowAdapter` and
    `AuctionMatchAdapter.stk_auction`), so the live capture functions take this object as
    their adapter unchanged.
    """

    def __init__(self, cache: TushareCache, *, trade_date: date, today: date) -> None:
        self.cache = cache
        self.trade_date = trade_date
        self.today = today

    def _require_day(self, trade_date: date, endpoint: str) -> None:
        if trade_date != self.trade_date:
            raise InputUnavailableError(
                f"{endpoint} asked for {trade_date}, but this replay is of {self.trade_date}"
            )

    def stock_basic(self, list_status: str = "L") -> Any:
        """Today's listing. Any listing fetched on or after the day covers every code the
        prior session's `daily_bar` holds (a code delisted since is in the D list), so the
        newest cached one fetched no earlier than the day is reused."""

        fetched_days = [
            key.split("/", 1)[1]
            for key in self.cache.keys("stock_basic", list_status)
            if key.count("/") == 1
        ]
        usable = sorted(day for day in fetched_days if day >= _day_key(self.trade_date))
        key = f"{list_status}/{usable[-1] if usable else _day_key(self.today)}"
        return self.cache.get(
            "stock_basic",
            key,
            lambda adapter: adapter.stock_basic(list_status=list_status),
            cache_empty=True,
        )

    def stock_st_raw(self, trade_date: date) -> Any:
        self._require_day(trade_date, "stock_st")
        return self.cache.get(
            "stock_st",
            _day_key(trade_date),
            lambda adapter: adapter.stock_st_raw(trade_date),
            cache_empty=False,
        )

    def suspend_d_raw(self, trade_date: date) -> Any:
        self._require_day(trade_date, "suspend_d")
        return self.cache.get(
            "suspend_d",
            _day_key(trade_date),
            lambda adapter: adapter.suspend_d_raw(trade_date),
            cache_empty=True,
        )

    def adj_factor_by_date(self, trade_date: date) -> Any:
        self._require_day(trade_date, "adj_factor")
        return self.cache.get(
            "adj_factor",
            _day_key(trade_date),
            lambda adapter: adapter.adj_factor_by_date(trade_date),
            cache_empty=False,
        )

    def stk_auction(self, trade_date: date) -> Any:
        self._require_day(trade_date, "stk_auction")
        return self.cache.get(
            "stk_auction",
            _day_key(trade_date),
            lambda adapter: adapter.stk_auction(trade_date),
            cache_empty=False,
        )


def cached_minute_fetcher(cache: TushareCache, trade_date: date) -> Callable[[str], Any]:
    """`stk_mins` for one code over the trade date, one cached answer per (code, day)."""

    start = datetime.combine(trade_date, clock_time(9, 0))
    end = datetime.combine(trade_date, clock_time(15, 30))
    columns = (
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

    def fetch(code: str) -> Any:
        frame = cache.get(
            "stk_mins",
            f"1min/{_day_key(trade_date)}/{code}",
            lambda adapter: adapter.stk_mins(code, "1min", start, end),
            cache_empty=True,
        )
        if frame is None or not len(frame):
            return frame
        frame = frame.copy()
        if "source" not in frame.columns:
            frame["source"] = "tushare"
        return frame[[column for column in columns if column in frame.columns]]

    return fetch


def cached_answers(
    cache: TushareCache, trade_date: date, *, reference: bool, auction: bool
) -> dict[str, Any]:
    """Which whole-day answers the cache already holds for the day (nothing is asked)."""

    day = _day_key(trade_date)
    status: dict[str, Any] = {}
    if reference:
        for list_status in ("L", "D", "P"):
            status[f"stock_basic({list_status})"] = any(
                key.split("/", 1)[1] >= day
                for key in cache.keys("stock_basic", list_status)
                if key.count("/") == 1
            )
        for endpoint in ("stock_st", "adj_factor", "suspend_d"):
            status[endpoint] = cache.path(endpoint, day).is_file()
    if auction:
        status["stk_auction"] = cache.path("stk_auction", day).is_file()
    status["stk_mins_codes"] = len(cache.keys("stk_mins", f"1min/{day}"))
    return status


def prefetch_day(source: CachedDaySource, *, reference: bool, auction: bool) -> dict[str, Any]:
    """Ask for every whole-day answer a synthesis will need, before the world is built.

    A refusal here costs seconds; the same refusal after the world was built costs a
    minute. Historical answers that come back empty are refused rather than sealed: an
    empty `stk_auction` for a past session means Tushare has nothing for it, not that the
    market held no auction.
    """

    day = source.trade_date
    facts: dict[str, Any] = {}

    def ask(label: str, call: Callable[[], Any], *, required_rows: bool) -> Any:
        try:
            frame = call()
        except TushareCacheMissError as error:
            raise InputUnavailableError(f"Tushare {label} for {day}: {error}") from error
        except InputUnavailableError:
            raise
        except Exception as error:  # noqa: BLE001 - named in the refusal
            raise InputUnavailableError(
                f"Tushare {label} for {day} failed: {type(error).__name__}: {error}"
            ) from error
        rows = 0 if frame is None else len(frame)
        if required_rows and not rows:
            raise InputUnavailableError(
                f"Tushare {label} for {day} returned no rows; a past session with none "
                "cannot be synthesized"
            )
        facts[label] = rows
        return frame

    if reference:
        for status in ("L", "D", "P"):
            ask(
                f"stock_basic({status})",
                lambda status=status: source.stock_basic(status),
                required_rows=status == "L",
            )
        ask("stock_st", lambda: source.stock_st_raw(day), required_rows=True)
        ask("adj_factor", lambda: source.adj_factor_by_date(day), required_rows=True)
        ask("suspend_d", lambda: source.suspend_d_raw(day), required_rows=False)
    if auction:
        ask("stk_auction", lambda: source.stk_auction(day), required_rows=True)
    return facts


# ---------------------------------------------------------------------------------------
# The market calendar valid on the day
# ---------------------------------------------------------------------------------------


@dataclass
class CalendarChoice:
    calendar: Any
    payload: bytes
    path: Path | None
    provenance: dict[str, Any]


def _opens_with_neighbours(calendar: Any, trade_date: date) -> bool:
    if not calendar.coverage_start <= trade_date <= calendar.coverage_end:
        return False
    if trade_date not in calendar.open_dates:
        return False
    return any(day < trade_date for day in calendar.open_dates) and any(
        day > trade_date for day in calendar.open_dates
    )


def choose_calendar(
    *, runtime_root: Path, trade_date: date, audit: Any, redate: bool = True
) -> CalendarChoice:
    """The generation the host had installed before the day opened, else the newest one
    that opens the day, re-dated to the evening before (`redate`), else a refusal."""

    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.strict_json import canonical_json_bytes, strict_json_loads

    directory = runtime_root / "authorities" / "market-calendar" / "generations"
    not_after = local_instant(trade_date, CALENDAR_NOT_AFTER)
    seen: list[str] = []
    usable: list[tuple[Any, bytes, Path]] = []
    if directory.is_dir():
        for path in sorted(directory.glob("*.json")):
            try:
                payload = audit.read_bytes(path)
                calendar = MarketCalendarAuthority.model_validate(strict_json_loads(payload))
            except (OSError, ValueError) as error:
                seen.append(f"{path.name}: unreadable ({type(error).__name__})")
                continue
            seen.append(f"{path.name[:12]}: generated {calendar.generated_at.isoformat()}")
            if calendar.content_sha256 != path.stem:
                continue
            if _opens_with_neighbours(calendar, trade_date):
                usable.append((calendar, payload, path))
    valid = [item for item in usable if item[0].generated_at <= not_after]
    if valid:
        calendar, payload, path = max(valid, key=lambda item: item[0].generated_at)
        return CalendarChoice(
            calendar=calendar,
            payload=payload,
            path=path,
            provenance={
                "origin": "recorded",
                "source": str(path),
                "generated_at": calendar.generated_at,
                "generations_seen": len(seen),
            },
        )
    if usable and redate:
        newest, _payload, path = max(usable, key=lambda item: item[0].generated_at)
        generated_at = local_instant(trade_date - timedelta(days=1), "20:00:00")
        redated = MarketCalendarAuthority.create(
            schema_version=newest.schema_version,
            exchange=newest.exchange,
            producer_commit=newest.producer_commit,
            coverage_start=newest.coverage_start,
            coverage_end=newest.coverage_end,
            open_dates=newest.open_dates,
            generated_at=generated_at,
        )
        return CalendarChoice(
            calendar=redated,
            payload=canonical_json_bytes(redated.model_dump(mode="json")),
            path=None,
            provenance={
                "origin": "synthesized",
                "source": f"{path} re-dated",
                "host_generated_at": newest.generated_at,
                "generated_at": generated_at,
                "why": (
                    f"every host generation that opens {trade_date} was generated after "
                    f"{not_after.astimezone(_SHANGHAI):%Y-%m-%d %H:%M} local, and every role "
                    "refuses a calendar generated after its clock"
                ),
                "generations_seen": len(seen),
            },
        )
    raise InputUnavailableError(
        f"no market-calendar generation under {directory} opens {trade_date} with an open "
        f"session on both sides (seen: {seen or 'none'})"
    )


# ---------------------------------------------------------------------------------------
# What the replica holds for the day
# ---------------------------------------------------------------------------------------


def _attached(replica: Path, *, threads: int, memory_limit: str) -> Any:
    import duckdb

    connection = duckdb.connect()
    connection.execute(f"SET threads = {int(threads)}")
    connection.execute(f"SET memory_limit = '{memory_limit}'")
    connection.execute(f"ATTACH '{replica}' AS source_replica (READ_ONLY)")
    return connection


def _tables(connection: Any) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'source_replica'"
        ).fetchall()
    }


def probe_replica(
    *,
    replica: Path,
    trade_date: date,
    calendar: Any,
    audit: Any,
    threads: int = 2,
    memory_limit: str = "1GB",
) -> dict[str, Any]:
    """Counts, not rows: what the day needs from the replica, and whether it is there.

    Read through an in-memory DuckDB with the replica `ATTACH`ed `READ_ONLY`, exactly as the
    extract reads it; nothing is written anywhere.
    """

    prior = tuple(day for day in calendar.open_dates if day < trade_date)
    prior_five = prior[-PRIOR_SESSIONS_AUCTION_GAP:]
    facts: dict[str, Any] = {
        "prior_trade_date": prior[-1] if prior else None,
        "prior_five": {str(day): 0 for day in prior_five},
    }
    audit.before(replica)
    connection = _attached(replica, threads=threads, memory_limit=memory_limit)
    try:
        present = _tables(connection)
        facts["tables"] = sorted(present)
        if "daily_bar" in present and prior_five:
            for day, count in connection.execute(
                "SELECT trade_date, count(*) FROM source_replica.daily_bar "
                "WHERE trade_date >= ? AND trade_date <= ? GROUP BY trade_date",
                [prior_five[0], prior_five[-1]],
            ).fetchall():
                key = str(day if isinstance(day, date) else day.date())
                if key in facts["prior_five"]:
                    facts["prior_five"][key] = int(count)
            facts["trade_date_daily_rows"] = int(
                connection.execute(
                    "SELECT count(*) FROM source_replica.daily_bar WHERE trade_date = ?",
                    [trade_date],
                ).fetchone()[0]
            )
        facts["prior_adj_factor_rows"] = (
            int(
                connection.execute(
                    "SELECT count(*) FROM source_replica.adj_factor WHERE trade_date = ?",
                    [facts["prior_trade_date"]],
                ).fetchone()[0]
            )
            if "adj_factor" in present and facts["prior_trade_date"] is not None
            else None
        )
        if "minute_bar" in present:
            rows, codes = connection.execute(
                "SELECT count(*), count(DISTINCT ts_code) FROM source_replica.minute_bar "
                "WHERE trade_time >= ? AND trade_time < ? AND freq = '1min'",
                [
                    datetime.combine(trade_date, clock_time(0)),
                    datetime.combine(trade_date + timedelta(days=1), clock_time(0)),
                ],
            ).fetchone()
            facts["trade_date_minute_rows"] = int(rows)
            facts["trade_date_minute_codes"] = int(codes)
        else:
            facts["trade_date_minute_rows"] = 0
            facts["trade_date_minute_codes"] = 0
    finally:
        connection.close()
    audit.after(replica)
    return facts


def replica_refusals(
    probe: Mapping[str, Any],
    *,
    trade_date: date,
    synthesize_reference: bool,
    tushare_minutes: bool,
) -> list[str]:
    """Every reason the replica cannot carry the day, each one precise."""

    reasons: list[str] = []
    if probe.get("prior_trade_date") is None:
        reasons.append(f"the calendar has no open session before {trade_date}")
        return reasons
    if "daily_bar" not in probe.get("tables", ()):
        reasons.append("the replica has no daily_bar table")
    else:
        missing = [day for day, count in probe["prior_five"].items() if not count]
        if len(probe["prior_five"]) < PRIOR_SESSIONS_AUCTION_GAP:
            reasons.append(
                f"the calendar holds only {len(probe['prior_five'])} open sessions before "
                f"{trade_date}; auction_gap reads five"
            )
        if missing:
            reasons.append(
                f"replica daily_bar has no rows for {', '.join(missing)} -- auction_gap reads "
                f"the five sessions before {trade_date}"
            )
    if synthesize_reference and not probe.get("prior_adj_factor_rows"):
        reasons.append(
            f"replica adj_factor has no rows for {probe['prior_trade_date']}; the reference-slow "
            "capture joins daily_bar to adj_factor on the prior session"
        )
    if not probe.get("trade_date_minute_rows") and not tushare_minutes:
        reasons.append(
            f"replica minute_bar has no 1min rows for {trade_date}; pass --tushare (or "
            "--tushare-offline with a filled cache) to take the watchlist's minutes from stk_mins"
        )
    return reasons


# ---------------------------------------------------------------------------------------
# The reference-slow evidence as it stood before the session
# ---------------------------------------------------------------------------------------


#: undated tables: copied as they are now, and labelled as such
_UNDATED_EVIDENCE_TABLES = (
    "stock_basic",
    "risk_blacklist",
    "dc_board",
    "dc_board_member",
    "kpl_concept_member",
)


def extract_reference_evidence(
    *,
    replica: Path,
    target: Path,
    trade_date: date,
    calendar: Any,
    audit: Any,
    require_adj_factor: bool = True,
    threads: int = 2,
    memory_limit: str = "2GB",
) -> dict[str, Any]:
    """Every row `capture_reference_slow_source_snapshot` could read at 09:20 on the day.

    Equivalent, query for query, to the replica with every row dated on or after the trade
    date removed -- which is what the live capture read, since the replica does not carry
    the day's own close until that evening. The capture's windows are per code, not per
    calendar session: `market_liquidity` takes each code's *latest* `daily_basic` row and
    *last five* `daily_bar` rows wherever they fall, so a calendar window (package AK's
    first cut: ten sessions of `daily_basic`, 130 of `daily_bar`) silently dropped every code
    whose history ends before it -- 54 of 5,619 rows on 2026-09-24. So:

    * `daily_bar`: each code's last `REFERENCE_EVIDENCE_DAILY_ROWS_PER_CODE` rows before the
      day (the deepest window any of the capture's queries reads; the prior-session joins
      are each code's newest row);
    * `daily_basic`: each code's newest row before the day (`market_liquidity`'s `rn = 1`;
      the `nl_screen_universe` join on the prior session is that same row or none);
    * `adj_factor`, `daily_state`, `daily_indicator`: the prior session, the only one read;
    * undated tables (`stock_basic`, `risk_blacklist` -- whose query filters by
      `imported_at` itself --, the board membership snapshots): today's, as labelled.

    What cannot be undone is a row dated before the day but written after the capture (a
    backfill): it is indistinguishable here, and the provenance says so.
    """

    import duckdb

    prior = tuple(day for day in calendar.open_dates if day < trade_date)
    if not prior:
        raise InputUnavailableError(f"the calendar has no open session before {trade_date}")
    prior_trade_date = prior[-1]
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    audit.before(replica)
    counts: dict[str, int] = {}
    connection = duckdb.connect(str(target))
    try:
        connection.execute(f"SET threads = {int(threads)}")
        connection.execute(f"SET memory_limit = '{memory_limit}'")
        connection.execute(f"ATTACH '{replica}' AS source_replica (READ_ONLY)")
        present = _tables(connection)
        newest_first = "ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC)"
        dated = {
            "daily_bar": (
                f"trade_date < ? QUALIFY {newest_first} <= "
                f"{int(REFERENCE_EVIDENCE_DAILY_ROWS_PER_CODE)}",
                [trade_date],
            ),
            "daily_basic": (f"trade_date < ? QUALIFY {newest_first} = 1", [trade_date]),
            "adj_factor": ("trade_date = ?", [prior_trade_date]),
            "daily_state": ("trade_date = ?", [prior_trade_date]),
            "daily_indicator": ("trade_date = ?", [prior_trade_date]),
        }
        for table, (condition, parameters) in dated.items():
            if table in present:
                connection.execute(
                    f"CREATE TABLE {table} AS "
                    f"SELECT * FROM source_replica.{table} WHERE {condition}",
                    parameters,
                )
                counts[table] = int(
                    connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                )
        for table in _UNDATED_EVIDENCE_TABLES:
            if table in present:
                connection.execute(f"CREATE TABLE {table} AS SELECT * FROM source_replica.{table}")
                counts[table] = int(
                    connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                )
        stale = (
            int(
                connection.execute(
                    "SELECT count(*) FROM (SELECT ts_code, max(trade_date) AS last "
                    "FROM daily_bar GROUP BY ts_code) WHERE last < ?",
                    [prior_trade_date],
                ).fetchone()[0]
            )
            if "daily_bar" in counts
            else 0
        )
        connection.execute("DETACH source_replica")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    audit.after(replica)
    if Path(f"{target}.wal").exists():
        raise InputUnavailableError("the reference evidence extract left a WAL behind")
    target.chmod(0o600)
    if "daily_bar" not in counts:
        raise InputUnavailableError("the replica has no daily_bar table")
    if require_adj_factor and not counts.get("adj_factor"):
        raise InputUnavailableError(
            f"the replica has no adj_factor rows for {prior_trade_date}, which the "
            "reference-slow capture joins to daily_bar"
        )
    return {
        "path": str(target),
        "prior_trade_date": prior_trade_date,
        "daily_bar_rows_per_code": REFERENCE_EVIDENCE_DAILY_ROWS_PER_CODE,
        "codes_not_trading_on_prior_session": stale,
        "rows": counts,
        "undated_tables_as_of_today": sorted(set(_UNDATED_EVIDENCE_TABLES) & set(counts)),
        "note": "rows dated before the trade date but written after its 09:20 capture "
        "(a backfill) cannot be told apart and are included",
    }


# ---------------------------------------------------------------------------------------
# The two source batches, by the live code
# ---------------------------------------------------------------------------------------


def auction_traded_codes(frame: Any) -> frozenset[str]:
    """Codes that matched in the day's opening call auction: a positive volume at a finite
    price. None of them can have been suspended for the whole day at 09:20."""

    import numpy as np
    import pandas as pd

    if frame is None or not len(frame):
        return frozenset()
    price = pd.to_numeric(frame["price"], errors="coerce").to_numpy(dtype="float64")
    volume = pd.to_numeric(frame["vol"], errors="coerce").to_numpy(dtype="float64")
    traded = np.isfinite(price) & (price > 0) & np.isfinite(volume) & (volume > 0)
    return frozenset(str(code).strip() for code in frame.loc[traded, "ts_code"])


class KnownAtCapture:
    """The day's answers as the 09:20 capture could have had them.

    `suspend_d(D)` asked afterwards also lists what happened *during* the day. An intraday
    halt with a timing range is `partial` and changes no record, but one recorded without a
    timing reads as a full-day suspension (`suspension._session_scope`), and the reference
    publisher then marks the code suspended for the whole day -- and auction_gap drops it.
    A code that matched in the day's opening auction was trading at 09:25, so a full-day
    suspension of it cannot have been what the live capture saw: such rows are left out and
    listed (`dropped_suspensions`). Every other answer passes through unchanged.
    """

    def __init__(self, source: CachedDaySource, *, traded_in_auction: frozenset[str]) -> None:
        self._source = source
        self.trade_date = source.trade_date
        self.traded_in_auction = traded_in_auction
        self.dropped_suspensions: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._source, name)

    def suspend_d_raw(self, trade_date: date) -> Any:
        from rquant.suspension import _session_scope

        frame = self._source.suspend_d_raw(trade_date)
        if frame is None or not len(frame) or not self.traded_in_auction:
            return frame
        keep: list[bool] = []
        for row in frame.to_dict("records"):
            code = str(row.get("ts_code", "")).strip()
            kind = str(row.get("suspend_type", "")).strip().upper()
            raw_timing = row.get("suspend_timing")
            timing = (
                "" if raw_timing is None or raw_timing != raw_timing else str(raw_timing).strip()
            )
            full_day = _session_scope(kind, timing) == "full_day"
            drop = full_day and code in self.traded_in_auction
            keep.append(not drop)
            if drop:
                self.dropped_suspensions.append(f"{code} {kind} {timing or '(no timing)'}".strip())
        return frame.loc[keep].reset_index(drop=True)


def synthesize_reference_snapshot(
    *,
    evidence_database: Path,
    source: CachedDaySource | KnownAtCapture,
    calendar: Any,
    trade_date: date,
    producer_commit: str,
    limits: Mapping[str, Any] | None = None,
) -> Any:
    """The live capture over the evidence and the day's Tushare answers, observed 09:20:07."""

    from rquant.reference_slow_source import (
        ReferenceSlowSourceLimits,
        capture_reference_slow_source_snapshot,
    )

    observed = local_instant(trade_date, SYNTHETIC_REFERENCE_OBSERVED)
    completed = observed + timedelta(seconds=SYNTHETIC_REFERENCE_CAPTURE_SECONDS)
    try:
        return capture_reference_slow_source_snapshot(
            database_path=evidence_database,
            adapter=source,
            calendar=calendar,
            target_trade_date=trade_date,
            captured_at=observed,
            completion_clock=lambda: completed,
            producer_commit=producer_commit,
            limits=ReferenceSlowSourceLimits.model_validate(dict(limits or {})),
        )
    except Exception as error:  # noqa: BLE001 - every cause is a refusal, named
        raise InputUnavailableError(
            f"the reference-slow batch for {trade_date} cannot be synthesized: "
            f"{type(error).__name__}: {error}"
        ) from error


def seal_reference_snapshot(
    *,
    spool: Any,
    calendar: Any,
    snapshot: Any,
    producer_commit: str,
    producer_version: str,
    prepared_at: datetime,
) -> Any:
    """`capture_reference_slow_batch`, the live sealing, with the snapshot as its capture."""

    from rquant.reference_slow_runtime import capture_reference_slow_batch

    return capture_reference_slow_batch(
        spool=spool,
        calendar=calendar,
        observed_at=snapshot.captured_at,
        producer_commit=producer_commit,
        producer_version=producer_version,
        snapshot_loader=lambda: snapshot,
        completion_clock=lambda: prepared_at,
    )


def reference_anachronisms(
    snapshot: Any, source: CachedDaySource | KnownAtCapture
) -> dict[str, Any]:
    """What today's `stock_basic` may have put into a past day's facts, counted.

    `is_st` is `name says ST or stock_st(D) lists it`; the name is today's, so a code that
    became ST after the day reads ST on it. Those codes are listed, not corrected.
    """

    from rquant.security_status import normalize_name

    listed = set()
    frame = source.stock_st_raw(source.trade_date)
    if frame is not None and len(frame):
        listed = {str(code).strip().upper() for code in frame["ts_code"]}
    name_only = sorted(
        fact.ts_code
        for fact in snapshot.security_facts
        if fact.is_st and fact.ts_code not in listed and normalize_name(fact.name)[1]
    )
    #: suspend_d(D) asked afterwards also lists what happened *during* the day: an intraday
    #: halt (a partial timing) changes no record; a full-day S the 09:20 view may not have
    #: known would make the code suspended -- `fidelity_vs_recorded.suspended` says whether
    #: the host's own capture agreed
    full_day: list[str] = []
    partial: list[str] = []
    #: what the capture was given, after `KnownAtCapture` (if any) left rows out
    suspensions = source.suspend_d_raw(source.trade_date)
    if isinstance(source, KnownAtCapture):
        source.dropped_suspensions = sorted(set(source.dropped_suspensions))
    if suspensions is not None and len(suspensions):
        from rquant.suspension import normalize_suspend_d_snapshot

        events = normalize_suspend_d_snapshot(
            suspensions, trade_date=source.trade_date, queried_at=snapshot.captured_at
        ).events
        full_day = sorted(
            {
                event.ts_code
                for event in events
                if event.suspend_type == "S" and event.session_scope == "full_day"
            }
        )
        partial = sorted(
            {
                f"{event.ts_code} {event.suspend_type} {event.suspend_timing}".strip()
                for event in events
                if event.session_scope != "full_day"
            }
        )
    dropped = list(getattr(source, "dropped_suspensions", ()))
    return {
        "stock_basic_is_todays_listing": True,
        "st_by_todays_name_only": {"count": len(name_only), "codes": name_only[:20]},
        "suspend_d_asked_after_the_day": {
            "full_day_suspended": {"count": len(full_day), "codes": full_day[:40]},
            "partial_or_resumption_events": {"count": len(partial), "events": partial[:40]},
            "left_out_traded_in_the_opening_auction": {
                "count": len(dropped),
                "events": dropped[:40],
            },
        },
    }


def synthesize_auction_universe(
    *,
    evidence_database: Path,
    authority_root: Path,
    calendar: Any,
    trade_date: date,
    producer_commit: str,
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """The live universe publisher's answer for the day, from the prior session's codes."""

    from rquant.auction_universe_source import publish_auction_universe_from_daily_snapshot

    observed = local_instant(trade_date, SYNTHETIC_UNIVERSE_OBSERVED)
    try:
        receipt = publish_auction_universe_from_daily_snapshot(
            database_path=evidence_database,
            authority_root=authority_root,
            calendar=calendar,
            observed_at=observed,
            producer_commit=producer_commit,
        )
    except Exception as error:  # noqa: BLE001 - a refusal, named
        raise InputUnavailableError(
            f"the auction universe for {trade_date} cannot be synthesized: "
            f"{type(error).__name__}: {error}"
        ) from error
    if receipt.effective_trade_date != trade_date:
        raise InputUnavailableError(
            f"the universe publisher answered for {receipt.effective_trade_date}, not {trade_date}"
        )
    document = json.loads(receipt.generation_path.read_text(encoding="utf-8"))
    codes = tuple(str(code) for code in document["codes"])
    return codes, {
        "origin": "synthesized",
        "source": f"replica daily_bar codes of {receipt.reference_trade_date}",
        "path": str(receipt.generation_path),
        "codes": len(codes),
        "observed_at": observed,
    }


def synthesize_auction_batch(
    *,
    spool: Any,
    source: CachedDaySource,
    trade_date: date,
    expected_codes: Iterable[str],
    settings: Mapping[str, Any],
    producer_commit: str,
    received_at: datetime,
) -> dict[str, Any]:
    """`AuctionMatchGateway.capture_once` -- the live capture -- over `stk_auction(D)`."""

    from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
    from rquant.live_contracts import LiveChannel

    config = AuctionMatchGatewayConfig(
        source=str(settings.get("source", "tushare.stk_auction")),
        dataset_id=str(settings.get("dataset_id", "auction_match")),
        producer_version=str(settings["producer_version"]),
        producer_commit=producer_commit,
        min_coverage_ratio=float(settings.get("min_coverage_ratio", 0.95)),
    )
    gateway = AuctionMatchGateway(spool=spool, fetcher=source.stk_auction, config=config)
    capture = gateway.capture_once(
        trade_date=trade_date,
        received_at=received_at,
        expected_codes=tuple(expected_codes),
        retry_ordinal=0,
    )
    (record,) = spool.list_after(LiveChannel.AUCTION_MATCH, sequence=capture.pointer.sequence - 1)
    envelope = record.envelope
    if envelope.degraded_reasons and any(
        reason.startswith("source_error:") for reason in envelope.degraded_reasons
    ):
        raise InputUnavailableError(
            f"the auction-match capture for {trade_date} could not read stk_auction: "
            f"{list(envelope.degraded_reasons)}"
        )
    return {
        "sequence": envelope.sequence,
        "quality_status": envelope.quality_status.value,
        "degraded_reasons": list(envelope.degraded_reasons),
        "rows": int(envelope.row_count),
        "expected_codes": capture.expected_count,
        "coverage_ratio": round(capture.coverage_ratio, 6),
        "min_coverage_ratio": config.min_coverage_ratio,
        "rows_dropped_non_finite": capture.rows_dropped_non_finite,
        "available_at": envelope.available_at,
        "payload": spool.read_payload(record),
    }


# ---------------------------------------------------------------------------------------
# Synthesized against recorded, on a day that has both
# ---------------------------------------------------------------------------------------


def _row_key(row: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    return "|".join(str(row.get(key)) for key in keys)


def _row_value(row: Mapping[str, Any]) -> str:
    return json.dumps(dict(row), sort_keys=True, default=str, ensure_ascii=False)


def compare_projections(recorded: Any, synthesized: Any) -> dict[str, Any]:
    """Row by row, per projection: rows only one side has, by the contract's key, and rows
    both have with different values -- not just the counts."""

    from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS

    left = {projection.table_name: projection.rows for projection in recorded.projections}
    right = {projection.table_name: projection.rows for projection in synthesized.projections}
    result: dict[str, Any] = {}
    for table in sorted(set(left) | set(right)):
        contract = PAGE_PROJECTION_CONTRACTS.get(table)
        keys = tuple(contract.sort_keys) if contract is not None else ()
        left_rows = {
            (_row_key(row, keys) if keys else _row_value(row)): _row_value(row)
            for row in left.get(table, ())
        }
        right_rows = {
            (_row_key(row, keys) if keys else _row_value(row)): _row_value(row)
            for row in right.get(table, ())
        }
        only_recorded = sorted(set(left_rows) - set(right_rows))
        only_synthesized = sorted(set(right_rows) - set(left_rows))
        changed = sorted(
            key for key in set(left_rows) & set(right_rows) if left_rows[key] != right_rows[key]
        )
        result[table] = {
            "recorded": len(left.get(table, ())),
            "synthesized": len(right.get(table, ())),
            "key": list(keys),
            "only_recorded_count": len(only_recorded),
            "only_recorded": only_recorded[:60],
            "only_synthesized_count": len(only_synthesized),
            "only_synthesized": only_synthesized[:60],
            "values_differ_count": len(changed),
            "values_differ": changed[:20],
            "identical": not (only_recorded or only_synthesized or changed),
        }
    return result


def compare_reference(recorded: Any, synthesized: Any) -> dict[str, Any]:
    """How far a synthesized reference snapshot is from the host's own, fact by fact and
    projection row by projection row; `differing` names everything that is not identical."""

    recorded_codes = {fact.ts_code for fact in recorded.security_facts}
    synthesized_codes = {fact.ts_code for fact in synthesized.security_facts}
    both = recorded_codes & synthesized_codes
    recorded_daily = {fact.ts_code: fact for fact in recorded.daily_facts}
    synthesized_daily = {fact.ts_code: fact for fact in synthesized.daily_facts}
    recorded_security = {fact.ts_code: fact for fact in recorded.security_facts}
    synthesized_security = {fact.ts_code: fact for fact in synthesized.security_facts}

    def differing(pairs: Iterable[tuple[Any, Any]], attribute: str) -> list[str]:
        return sorted(
            left.ts_code
            for left, right in pairs
            if getattr(left, attribute) != getattr(right, attribute)
        )

    daily_pairs = [(recorded_daily[code], synthesized_daily[code]) for code in sorted(both)]
    security_pairs = [
        (recorded_security[code], synthesized_security[code]) for code in sorted(both)
    ]
    suspended_differ = sorted(set(recorded.suspended_codes) ^ set(synthesized.suspended_codes))
    projections = compare_projections(recorded, synthesized)
    result: dict[str, Any] = {
        "securities": {"recorded": len(recorded_codes), "synthesized": len(synthesized_codes)},
        "only_recorded": sorted(recorded_codes - synthesized_codes)[:20],
        "only_recorded_count": len(recorded_codes - synthesized_codes),
        "only_synthesized": sorted(synthesized_codes - recorded_codes)[:20],
        "only_synthesized_count": len(synthesized_codes - recorded_codes),
        "suspended": {
            "recorded": len(recorded.suspended_codes),
            "synthesized": len(synthesized.suspended_codes),
            "differ": suspended_differ[:20],
            "differ_count": len(suspended_differ),
        },
        "source_snapshot_ids_differ": sorted(
            key
            for key in set(recorded.source_snapshot_ids) | set(synthesized.source_snapshot_ids)
            if recorded.source_snapshot_ids.get(key) != synthesized.source_snapshot_ids.get(key)
        ),
        "projection_rows": projections,
        "projections": {
            projection.table_name: len(projection.rows) for projection in synthesized.projections
        },
        "recorded_projections": {
            projection.table_name: len(projection.rows) for projection in recorded.projections
        },
    }
    for attribute in ("close_raw", "prior_adj_factor", "adj_factor"):
        codes = differing(daily_pairs, attribute)
        result[f"daily_{attribute}_differ"] = {"count": len(codes), "codes": codes[:20]}
    for attribute in (
        "name",
        "is_st",
        "market",
        "list_date",
        "delist_date",
        "source_list_status",
    ):
        codes = differing(security_pairs, attribute)
        result[f"security_{attribute}_differ"] = {"count": len(codes), "codes": codes[:20]}
    differ = [
        label for label, entry in result.items() if isinstance(entry, dict) and entry.get("count")
    ]
    if result["only_recorded_count"] or result["only_synthesized_count"]:
        differ.append("securities")
    if suspended_differ:
        differ.append("suspended")
    differ += [
        f"projection:{table}" for table, entry in projections.items() if not entry["identical"]
    ]
    result["differing"] = sorted(differ)
    return result


def _record_view(snapshot: Any, calendar: Any) -> dict[str, dict[str, dict[str, Any]]]:
    """Each code's registry payloads, exactly as the reference publisher derives them."""

    from rquant.reference_slow_publisher import _record_payloads

    daily = {fact.ts_code: fact for fact in snapshot.daily_facts}
    suspended = set(snapshot.suspended_codes)
    view: dict[str, dict[str, dict[str, Any]]] = {}
    for security in snapshot.security_facts:
        try:
            payloads = _record_payloads(
                daily=daily[security.ts_code],
                security=security,
                suspended=security.ts_code in suspended,
                target_trade_date=snapshot.target_trade_date,
                open_dates=calendar.open_dates,
            )
        except Exception as error:  # noqa: BLE001 - the publisher would refuse it too
            view[security.ts_code] = {"_error": {"error": f"{type(error).__name__}: {error}"}}
            continue
        view[security.ts_code] = {
            str(getattr(dataset, "value", dataset)): dict(payload) for dataset, payload in payloads
        }
    return view


def compare_candidate_inputs(
    *,
    recorded_snapshot: Any,
    recorded_calendar: Any,
    synthesized_snapshot: Any,
    synthesized_calendar: Any,
    recorded_auction_payload: bytes,
    synthesized_auction_payload: bytes,
) -> dict[str, Any]:
    """Per auction code: does anything auction_gap's candidate input reads differ?

    The candidate input reads, per code, the auction row and four registry records (ST,
    suspension, listing, price limit; the reference publisher also writes board membership
    and the adjustment factor). The records are derived here by the publisher's own
    `_record_payloads` from each snapshot and its calendar, so a code listed here reads
    different evidence in the two runs, and a code not listed reads the same. The prior
    five sessions' volumes come from the same replica extract in both runs.
    """

    from rquant.auction_match_gateway import AuctionMatchGateway

    recorded_rows = AuctionMatchGateway.decode_payload(recorded_auction_payload).set_index(
        "ts_code"
    )
    synthesized_rows = AuctionMatchGateway.decode_payload(synthesized_auction_payload).set_index(
        "ts_code"
    )
    auction_columns = [column for column in recorded_rows.columns if column in synthesized_rows]
    recorded_view = _record_view(recorded_snapshot, recorded_calendar)
    synthesized_view = _record_view(synthesized_snapshot, synthesized_calendar)
    codes = sorted(
        {str(code) for code in recorded_rows.index} | {str(code) for code in synthesized_rows.index}
    )
    by_code: dict[str, dict[str, Any]] = {}
    for code in codes:
        entry: dict[str, Any] = {}
        in_recorded, in_synthesized = code in recorded_rows.index, code in synthesized_rows.index
        if in_recorded != in_synthesized:
            entry["auction_row_only_in"] = "recorded" if in_recorded else "synthesized"
        elif in_recorded:
            columns = []
            for column in auction_columns:
                left, right = recorded_rows.at[code, column], synthesized_rows.at[code, column]
                if not (left == right or (left != left and right != right)):
                    columns.append(column)
            if columns:
                entry["auction_columns"] = columns
        left_records = recorded_view.get(code)
        right_records = synthesized_view.get(code)
        if (left_records is None) != (right_records is None):
            entry["reference_only_in"] = "recorded" if left_records is not None else "synthesized"
        elif left_records is not None and right_records is not None:
            fields: dict[str, dict[str, list[Any]]] = {}
            for dataset in sorted(set(left_records) | set(right_records)):
                left_payload = left_records.get(dataset, {})
                right_payload = right_records.get(dataset, {})
                changed = {
                    field: [left_payload.get(field), right_payload.get(field)]
                    for field in sorted(set(left_payload) | set(right_payload))
                    if left_payload.get(field) != right_payload.get(field)
                }
                if changed:
                    fields[dataset] = changed
            if fields:
                entry["reference_records"] = fields
        if entry:
            by_code[code] = entry
    return {
        "codes_compared": len(codes),
        "codes_differ_count": len(by_code),
        "codes_differ": sorted(by_code)[:200],
        "by_code": {code: by_code[code] for code in sorted(by_code)[:60]},
        "calendar_differs": recorded_calendar.content_sha256 != synthesized_calendar.content_sha256,
        "not_compared": "prior-five daily volumes: the same replica extract feeds both runs",
    }


def compare_signal_lists(
    this: Sequence[Mapping[str, Any]],
    other: Sequence[Mapping[str, Any]],
    *,
    until_local: str,
) -> dict[str, Any]:
    """Serving signals of two runs of one day, up to the earlier run's last tick."""

    def keyed(rows: Sequence[Mapping[str, Any]]) -> set[tuple[str, str, str, str]]:
        return {
            (
                str(row["event_time_local"]),
                str(row["strategy_id"]),
                str(row["candidate_id"]),
                str(row["action"]),
            )
            for row in rows
            if str(row["event_time_local"])[11:19] <= until_local
        }

    mine, theirs = keyed(this), keyed(other)
    only_this = sorted(mine - theirs)
    only_other = sorted(theirs - mine)
    fields = ("event_time_local", "strategy_id", "candidate_id", "action")
    return {
        "until_local": until_local,
        "common": len(mine & theirs),
        "only_this_run": [dict(zip(fields, row, strict=True)) for row in only_this],
        "only_other_run": [dict(zip(fields, row, strict=True)) for row in only_other],
        "identical": not (only_this or only_other),
        "codes_differ": sorted({row[2] for row in only_this} | {row[2] for row in only_other}),
    }


def compare_auction(recorded_payload: bytes, synthesized_payload: bytes) -> dict[str, Any]:
    """How far a synthesized auction payload is from the host's own, column by column."""

    from rquant.auction_match_gateway import AuctionMatchGateway

    recorded = AuctionMatchGateway.decode_payload(recorded_payload).set_index("ts_code")
    synthesized = AuctionMatchGateway.decode_payload(synthesized_payload).set_index("ts_code")
    both = recorded.index.intersection(synthesized.index)
    result: dict[str, Any] = {
        "rows": {"recorded": int(len(recorded)), "synthesized": int(len(synthesized))},
        "only_recorded_count": int(len(recorded.index.difference(synthesized.index))),
        "only_recorded": sorted(str(code) for code in recorded.index.difference(synthesized.index))[
            :20
        ],
        "only_synthesized_count": int(len(synthesized.index.difference(recorded.index))),
        "only_synthesized": sorted(
            str(code) for code in synthesized.index.difference(recorded.index)
        )[:20],
    }
    for column in ("price", "vol", "amount", "pre_close", "turnover_rate", "volume_ratio"):
        left = recorded.loc[both, column].astype("float64")
        right = synthesized.loc[both, column].astype("float64")
        same = (left == right) | (left.isna() & right.isna())
        close = ((left - right).abs() <= 1e-9 * right.abs().clip(lower=1.0)) | same
        result[f"{column}_differ"] = int((~close).sum())
    return result


__all__ = [
    "CALENDAR_NOT_AFTER",
    "auction_traded_codes",
    "CachedDaySource",
    "CalendarChoice",
    "InputUnavailableError",
    "KnownAtCapture",
    "TushareCache",
    "TushareCacheMissError",
    "cached_answers",
    "cached_minute_fetcher",
    "choose_calendar",
    "compare_auction",
    "compare_candidate_inputs",
    "compare_projections",
    "compare_reference",
    "compare_signal_lists",
    "extract_reference_evidence",
    "local_instant",
    "prefetch_day",
    "probe_replica",
    "reference_anachronisms",
    "replica_refusals",
    "seal_reference_snapshot",
    "synthesize_auction_batch",
    "synthesize_auction_universe",
    "synthesize_reference_snapshot",
]
