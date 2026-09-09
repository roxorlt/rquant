"""Capture current slow-reference source facts into one sealed snapshot."""

from __future__ import annotations

import math
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import TYPE_CHECKING, Literal, Protocol
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, field_validator

from rquant.readside_replica_gate import (
    ReplicaReadGate,
    descriptor_reopen_path,
    is_write_lock_error,
)
from rquant.reference_slow_publisher import (
    ReferenceDailyFact,
    ReferenceSecurityFact,
    ReferenceSlowSourceSnapshot,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.security_status import normalize_name
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.strict_json import canonical_json_bytes
from rquant.suspension import normalize_suspend_d_snapshot

if TYPE_CHECKING:  # pragma: no cover - typing only
    import duckdb

_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class ReferenceSlowSourceError(RuntimeError):
    """Current source evidence cannot be sealed without ambiguity."""


class ReferenceSlowSourceLimits(RuntimeContractModel):
    snapshot_max_bytes: int = Field(default=8 * 1024**3, gt=0)
    snapshot_min_free_bytes: int = Field(default=1024**3, ge=0)
    snapshot_copy_timeout_seconds: float = Field(default=60.0, gt=0, le=300)
    query_chunk_rows: int = Field(default=512, gt=0, le=10_000)
    max_response_rows: int = Field(default=10_000, gt=0, le=100_000)
    max_response_bytes: int = Field(default=8 * 1024**2, gt=0)


_DEFAULT_LIMITS = ReferenceSlowSourceLimits()


class ReferenceSlowAdapter(Protocol):
    def stock_basic(self, list_status: str = "L") -> pd.DataFrame: ...

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame: ...

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame: ...

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame: ...


class _ReferenceSourceFact(RuntimeContractModel):
    ts_code: str

    @field_validator("ts_code")
    @classmethod
    def validate_ts_code(cls, value: str) -> str:
        if _TS_CODE_PATTERN.fullmatch(value) is None:
            raise ValueError("ts_code must be a canonical Tushare A-share code")
        return value


class ReferenceDailySourceFact(_ReferenceSourceFact):
    trade_date: date
    close_raw: float = Field(gt=0, allow_inf_nan=False)


class ReferenceAdjustmentSourceFact(_ReferenceSourceFact):
    trade_date: date
    adj_factor: float = Field(gt=0, allow_inf_nan=False)


class ReferenceSecuritySourceFact(_ReferenceSourceFact):
    name: str = Field(min_length=1)
    is_st: bool | None = None
    list_date: date
    delist_date: date | None = None
    source_list_status: Literal["L", "D", "P"] = "L"
    market: str = Field(min_length=1)


class ReferenceSuspensionSourceFact(_ReferenceSourceFact):
    trade_date: date
    suspend_type: Literal["S", "R"]
    session_scope: Literal["full_day", "partial", "unknown"]


def _ordered_unique_by_code(
    facts: tuple[_ReferenceSourceFact, ...],
    *,
    label: str,
) -> tuple[_ReferenceSourceFact, ...]:
    ordered = tuple(sorted(facts, key=lambda fact: fact.ts_code))
    codes = tuple(fact.ts_code for fact in ordered)
    if len(codes) != len(set(codes)):
        raise ReferenceSlowSourceError(f"{label} contains duplicate ts_code values")
    return ordered


def assemble_reference_slow_source_snapshot(
    *,
    calendar: MarketCalendarAuthority,
    observed_at: datetime,
    producer_commit: str,
    daily_facts: tuple[ReferenceDailySourceFact, ...],
    adjustment_facts: tuple[ReferenceAdjustmentSourceFact, ...],
    security_facts: tuple[ReferenceSecuritySourceFact, ...],
    security_source_facts: tuple[ReferenceSecuritySourceFact, ...] | None = None,
    suspension_facts: tuple[ReferenceSuspensionSourceFact, ...] = (),
) -> ReferenceSlowSourceSnapshot:
    """Seal normalized facts using the target session's point-in-time cutoff."""

    calendar = MarketCalendarAuthority.model_validate(calendar)
    observed = normalize_aware_utc(observed_at)
    local = observed.astimezone(_SHANGHAI)
    target_trade_date = local.date()
    if local.timetz().replace(tzinfo=None) > time(9, 25):
        raise ReferenceSlowSourceError("reference source must be captured by 09:25")
    if calendar.generated_at > observed:
        raise ReferenceSlowSourceError("calendar is future evidence")
    if target_trade_date not in calendar.open_dates:
        raise ReferenceSlowSourceError("target session is not an authoritative open date")
    prior_dates = tuple(item for item in calendar.open_dates if item < target_trade_date)
    next_dates = tuple(item for item in calendar.open_dates if item > target_trade_date)
    if not prior_dates or not next_dates:
        raise ReferenceSlowSourceError("calendar lacks adjacent open-session coverage")
    prior_trade_date = prior_dates[-1]

    ordered_daily = tuple(_ordered_unique_by_code(tuple(daily_facts), label="daily source"))
    ordered_security = tuple(
        _ordered_unique_by_code(tuple(security_facts), label="security source")
    )
    ordered_security_evidence = tuple(
        _ordered_unique_by_code(
            tuple(security_source_facts or security_facts),
            label="full security source",
        )
    )
    if not ordered_daily:
        raise ReferenceSlowSourceError("daily source cannot be empty")
    daily_codes = tuple(fact.ts_code for fact in ordered_daily)
    if daily_codes != tuple(fact.ts_code for fact in ordered_security):
        raise ReferenceSlowSourceError("daily and security source universes differ")
    if any(fact.trade_date != prior_trade_date for fact in ordered_daily):
        raise ReferenceSlowSourceError("daily source must use the exact prior open date")

    adjustments: dict[tuple[str, date], ReferenceAdjustmentSourceFact] = {}
    for fact in adjustment_facts:
        key = (fact.ts_code, fact.trade_date)
        if key in adjustments:
            raise ReferenceSlowSourceError("adjustment source contains duplicate keys")
        adjustments[key] = fact
    required_adjustments = {
        (code, trade_date)
        for code in daily_codes
        for trade_date in (prior_trade_date, target_trade_date)
    }
    if set(adjustments) != required_adjustments:
        raise ReferenceSlowSourceError(
            "adjustment source must exactly cover prior and target sessions"
        )

    selected_suspensions = tuple(
        sorted(
            suspension_facts,
            key=lambda fact: (fact.ts_code, fact.suspend_type, fact.session_scope),
        )
    )
    if any(
        fact.trade_date != target_trade_date or fact.ts_code not in daily_codes
        for fact in selected_suspensions
    ):
        raise ReferenceSlowSourceError("suspension source is outside target universe/session")
    if len(selected_suspensions) != len(set(selected_suspensions)):
        raise ReferenceSlowSourceError("suspension source contains duplicate facts")
    events_by_code: dict[str, list[ReferenceSuspensionSourceFact]] = {}
    for fact in selected_suspensions:
        events_by_code.setdefault(fact.ts_code, []).append(fact)
    suspended_codes: list[str] = []
    for code, events in events_by_code.items():
        full_day = tuple(
            event
            for event in events
            if event.suspend_type == "S" and event.session_scope == "full_day"
        )
        contradictory = tuple(
            event
            for event in events
            if event.suspend_type != "S" or event.session_scope != "full_day"
        )
        if full_day and contradictory:
            raise ReferenceSlowSourceError(f"{code} suspension evidence conflicts")
        if full_day:
            suspended_codes.append(code)

    publisher_daily = tuple(
        ReferenceDailyFact(
            ts_code=fact.ts_code,
            trade_date=fact.trade_date,
            close_raw=fact.close_raw,
            prior_adj_factor=adjustments[(fact.ts_code, prior_trade_date)].adj_factor,
            adj_factor=adjustments[(fact.ts_code, target_trade_date)].adj_factor,
        )
        for fact in ordered_daily
    )
    publisher_security = tuple(
        ReferenceSecurityFact(
            ts_code=fact.ts_code,
            name=fact.name,
            is_st=fact.is_st,
            list_date=fact.list_date,
            delist_date=fact.delist_date,
            source_list_status=fact.source_list_status,
            market=fact.market,
        )
        for fact in ordered_security
    )
    source_snapshot_ids = {
        "calendar": calendar.content_sha256,
        "daily": canonical_sha256(
            {
                "contract": "reference-slow-market-source/v1",
                "daily": ordered_daily,
                "adjustments": tuple(adjustments[key] for key in sorted(adjustments)),
            }
        ),
        "security": canonical_sha256(
            {
                "contract": "reference-slow-security-source/v1",
                "facts": ordered_security_evidence,
            }
        ),
        "suspension": canonical_sha256(
            {
                "contract": "reference-slow-suspension-source/v1",
                "facts": selected_suspensions,
            }
        ),
    }
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=target_trade_date,
        captured_at=observed,
        producer_commit=producer_commit,
        source_snapshot_ids=source_snapshot_ids,
        daily_facts=publisher_daily,
        security_facts=publisher_security,
        suspended_codes=tuple(sorted(suspended_codes)),
        trade_calendar_open_dates=calendar.open_dates,
    )


def _normalized_absolute_path(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("reference source database path must be absolute and normalized")
    for parent in (candidate.parent, *candidate.parents):
        if parent == Path(parent.anchor):
            break
        try:
            observed = parent.lstat()
        except OSError as exc:
            raise ReferenceSlowSourceError("reference source parent is unavailable") from exc
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise ReferenceSlowSourceError("reference source path contains an unsafe parent")
    return candidate


def _validate_database(value: os.stat_result) -> None:
    """The mode rule the file this source is given can actually satisfy (#249, #250).

    Since #250 this source reads the five-minute read-only replica `rquant_ro.duckdb`,
    which `scripts/sync-readonly-replica.sh` recreates at 0644 every five minutes, so a
    0600 requirement refused it on every iteration. What protects the read is that the
    file is owned by the runtime and neither group nor other can write it; 0600 and 0400
    still pass. This validates the *source* database only — the private copy this module
    makes of it is created 0600 and checked by its own path.
    """

    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise ReferenceSlowSourceError("reference source database is a symlink or unsafe file")
    if value.st_uid != os.geteuid():
        raise ReferenceSlowSourceError("reference source database owner does not match")
    if stat.S_IMODE(value.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
        raise ReferenceSlowSourceError(
            "reference source database must not be group or other writable"
        )
    if value.st_nlink != 1:
        raise ReferenceSlowSourceError("reference source database must have one hard link")


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written < 1:
            raise ReferenceSlowSourceError("reference source private copy write stalled")
        offset += written


@dataclass(frozen=True, slots=True)
class _OpenedDatabase:
    """The connection one verified read got, and which of the three openers gave it."""

    connection: duckdb.DuckDBPyConnection
    #: `"descriptor"` (the inode is pinned and nothing is copied -- what a Linux runtime
    #: host gets), `"copy"` (a private byte copy inside this role's own `PrivateTmp`, the
    #: fallback for an engine that refuses the descriptor path, capped by
    #: `snapshot_max_bytes`), or `"in_place"` (the name is re-opened; the identity checks
    #: around the read are what say the generation did not move).
    opened_through: str


@contextmanager
def _verified_database_read(
    database_path: Path,
    *,
    limits: ReferenceSlowSourceLimits = _DEFAULT_LIMITS,
    monotonic_deadline: float,
    monotonic_clock: Callable[[], float],
) -> Iterator[_OpenedDatabase]:
    """Read one generation of the read-only replica, without copying 10 GB to do it (#256).

    Until v0.33.6 this copied the whole database into `PrivateTmp` and queried the copy.
    That was never reached before #250 pointed this source at the replica, and once it was,
    it could not succeed: the production replica is about 10 GB against a `snapshot_max_bytes`
    of 8 GiB, so the source was DEGRADED every iteration with
    `exceeds maximum byte budget` (package P review MF-2) -- and the two iterations that a
    smaller database *did* let through moved 10 GB through the page cache while the 17:00
    daily pipeline was trying to scan the main database (#256).

    So the copy is now the *fallback*, not the path. The order is
    `serving_page_projection_source._StableReadonlyDuckDB`'s, for the reasons written out
    there:

    1. **the pinned descriptor** -- `duckdb.connect("/proc/self/fd/<n>")` opens the inode
       this function already validated, so a `rename()` over the name cannot swap the
       generation under the read, and not one byte is copied. The pinned duckdb 1.5.2
       accepts this on Linux, which is the production host.
    2. **a private copy in `PrivateTmp`** -- for an engine that refuses that path (macOS
       resolves `/dev/fd/<n>` to a name it then cannot open). Bounded exactly as before by
       `snapshot_max_bytes`, `snapshot_min_free_bytes` and the deadline; safe here because
       the replica is replaced by `mv` and this function refuses it outright if it has a
       WAL sidecar, so there is no live writer whose last checkpoint could be served.
    3. **the database in place** -- when the copy is refused because the generation is
       larger than the budget or the disk has no headroom. Nothing is pinned, and this
       does not pretend otherwise: what stands is the identity of the descriptor and of
       the name, compared before and after the read, exactly as `auction_gap_candidate_input`
       and the notifier's projection already do against this same file.

    Every check that was here is still here, with the same refusals: mode, owner, link
    count, the open-race identity, the WAL sidecar before and after, and the descriptor's
    and the name's identity after the read. Two things moved. The messages say "reading"
    rather than "snapshotting", because for the branch a runtime host takes there is no
    snapshot. And the **database directory's fingerprint now guards the two by-name
    branches only** (ruling 26): a read that holds the inode cannot be misled by another
    file in the same directory changing, and that directory is where the replica sync
    works four times a run, twice inside this source's capture window.
    """

    import duckdb

    limits = ReferenceSlowSourceLimits.model_validate(limits)
    descriptor = -1
    parent_descriptor = -1
    try:
        parent_descriptor = os.open(
            database_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        parent_before = os.fstat(parent_descriptor)
        before = database_path.lstat()
        _validate_database(before)
        descriptor = os.open(
            database_path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        _validate_database(opened)
        if _identity(before) != _identity(opened):
            raise ReferenceSlowSourceError("reference source database changed while opening")
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
        raise ReferenceSlowSourceError("reference source database is unavailable") from exc
    except ReferenceSlowSourceError:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
        raise

    wal_path = Path(f"{database_path}.wal")
    connection = None
    copy_directory = None
    try:
        if wal_path.exists() or wal_path.is_symlink():
            raise ReferenceSlowSourceError("reference source database has an unsealed WAL sidecar")
        if monotonic_clock() > monotonic_deadline:
            raise ReferenceSlowSourceError("reference source read deadline expired")
        opened_through = "in_place"
        reopen = descriptor_reopen_path(descriptor)
        if reopen is not None:
            try:
                connection = duckdb.connect(reopen, read_only=True)
            except Exception as exc:  # noqa: BLE001 - classified, then refused or passed
                if is_write_lock_error(exc):
                    raise ReferenceSlowSourceError(
                        "reference source database query failed"
                    ) from exc
            else:
                opened_through = "descriptor"
        if connection is None:
            target = database_path
            copied = _private_copy_of_generation(
                descriptor,
                opened,
                limits=limits,
                monotonic_deadline=monotonic_deadline,
                monotonic_clock=monotonic_clock,
            )
            if copied is not None:
                copy_directory, target = copied
                opened_through = "copy"
            try:
                connection = duckdb.connect(str(target), read_only=True)
            except duckdb.Error as exc:
                raise ReferenceSlowSourceError("reference source database query failed") from exc
        yield _OpenedDatabase(connection=connection, opened_through=opened_through)
        if monotonic_clock() > monotonic_deadline:
            raise ReferenceSlowSourceError("reference source read deadline expired")

        after_read = os.fstat(descriptor)
        #: identity before validity, so that a generation replaced under the read is named
        #: as that rather than as "must have one hard link" -- an unlinked inode fails the
        #: link count first, and the link count is not what an operator needs to be told.
        if _identity(opened) != _identity(after_read):
            raise ReferenceSlowSourceError("reference source database changed while reading")
        _validate_database(after_read)
        try:
            current = database_path.lstat()
        except OSError as exc:
            raise ReferenceSlowSourceError(
                "reference source database changed while reading"
            ) from exc
        if _identity(opened) != _identity(current):
            raise ReferenceSlowSourceError("reference source database changed while reading")
        _validate_database(current)
        if wal_path.exists() or wal_path.is_symlink():
            raise ReferenceSlowSourceError("reference source database has an unsealed WAL sidecar")
        if opened_through != "descriptor":
            #: **Ruling 26.** The directory fingerprint is a guard for a read that reaches
            #: the database *by name*: the private copy and the in-place branch both do,
            #: so anything that rewrites the directory under them could have changed what
            #: they are reading. The pinned-descriptor branch does not -- it holds the
            #: inode this function validated, a `rename()` cannot swap it, and the
            #: pre/post `fstat` plus the post-read name-identity check already say the
            #: generation did not move. What the fingerprint adds *there* is a refusal
            #: whenever anybody touches any other file in the same directory, and that is
            #: `scripts/sync-readonly-replica.sh` doing its normal job: it mutates
            #: `data/` four times per run (create `.tmp.$$`, `mv`, `rm -f *.wal`, `mv` the
            #: sidecar) and its timer fires at 09:20 and 09:25 -- both ends of this
            #: source's 09:20-09:25 capture window. Keeping it on the pinned path would
            #: have made the replica sync the most likely cause of a DEGRADED
            #: reference-slow (package Q review SF-2).
            parent_after = os.fstat(parent_descriptor)
            if (
                parent_after.st_dev,
                parent_after.st_ino,
                parent_after.st_mtime_ns,
                parent_after.st_ctime_ns,
            ) != (
                parent_before.st_dev,
                parent_before.st_ino,
                parent_before.st_mtime_ns,
                parent_before.st_ctime_ns,
            ):
                raise ReferenceSlowSourceError(
                    "reference source database directory changed while reading"
                )
    finally:
        if connection is not None:
            connection.close()
        if copy_directory is not None:
            copy_directory.cleanup()
        os.close(descriptor)
        os.close(parent_descriptor)


def _private_copy_of_generation(
    descriptor: int,
    opened: os.stat_result,
    *,
    limits: ReferenceSlowSourceLimits,
    monotonic_deadline: float,
    monotonic_clock: Callable[[], float],
) -> tuple[TemporaryDirectory[str], Path] | None:
    """A 0600 byte copy of the pinned inode, or None when it is not worth taking.

    Only reached when the engine refused the descriptor path. `None` -- the generation is
    larger than `snapshot_max_bytes`, or the temporary filesystem has no headroom -- means
    "read it where it lies" rather than "refuse", which is the whole of #256: the
    production replica is 10 GB and must be *read*, not copied and not refused.
    """

    if opened.st_size > limits.snapshot_max_bytes:
        return None
    temporary_root = Path(tempfile.gettempdir())
    if shutil.disk_usage(temporary_root).free < opened.st_size + limits.snapshot_min_free_bytes:
        return None
    if monotonic_clock() > monotonic_deadline:
        raise ReferenceSlowSourceError("reference source read deadline expired")

    directory = TemporaryDirectory(prefix="rquant-reference-source-")
    try:
        temporary_path = Path(directory.name)
        temporary_path.chmod(0o700)
        copy_path = temporary_path / "evidence.duckdb"
        copy_descriptor = os.open(
            copy_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        copied = 0
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            while True:
                if monotonic_clock() > monotonic_deadline:
                    raise ReferenceSlowSourceError("reference source read deadline expired")
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                _write_all(copy_descriptor, chunk)
                copied += len(chunk)
            os.fsync(copy_descriptor)
        finally:
            os.close(copy_descriptor)

        after_copy = os.fstat(descriptor)
        _validate_database(after_copy)
        if _identity(opened) != _identity(after_copy) or copied != opened.st_size:
            raise ReferenceSlowSourceError("reference source database changed while reading")
        copy = copy_path.stat()
        if (
            not stat.S_ISREG(copy.st_mode)
            or copy.st_uid != os.geteuid()
            or copy.st_nlink != 1
            or stat.S_IMODE(copy.st_mode) != 0o600
            or copy.st_size != copied
        ):
            raise ReferenceSlowSourceError("reference source private copy is unsafe")
    except BaseException:
        directory.cleanup()
        raise
    return directory, copy_path


def _projection_scalar(value: object) -> str | int | float | bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ReferenceSlowSourceError("reference projection contains a non-finite value")
        return value
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=ZoneInfo("UTC"))
        return normalized.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


_ReferenceEvidence = tuple[
    tuple[tuple[str, float, float], ...],
    dict[str, tuple[dict[str, str | int | float | bool | None], ...]],
]


def _load_database_reference_evidence(
    database_path: Path,
    *,
    prior_trade_date: date,
    projection_as_of_date: date | None = None,
    limits: ReferenceSlowSourceLimits = _DEFAULT_LIMITS,
    monotonic_deadline: float = float("inf"),
    monotonic_clock: Callable[[], float] = monotonic,
    read_gate: ReplicaReadGate[_ReferenceEvidence] | None = None,
) -> _ReferenceEvidence:
    """Everything this source takes out of the replica, read only when it changed (#256).

    **One iteration asks at most once** -- `capture_reference_slow_batch` calls either the
    snapshot loader or the revision loader, and the revision loader is given a single date.
    The target session and the five revision look-backs are six *iterations* carrying six
    different gate keys, and the gate cannot and does not merge them (review MF-2).

    What the gate is worth here is the retry: the capture window is 09:20-09:25, about ten
    iterations at this role's thirty-second interval, and roughly eight of them ask the
    *same* key. When a capture succeeds those eight take an early return and never read at
    all; when it keeps failing -- quota, credential, and until this package the copy budget
    -- every one of them used to re-read the replica from scratch. Now the first one reads
    and the rest recognise the generation.
    """

    if read_gate is None:
        return _query_database_reference_evidence(
            database_path,
            prior_trade_date=prior_trade_date,
            projection_as_of_date=projection_as_of_date,
            limits=limits,
            monotonic_deadline=monotonic_deadline,
            monotonic_clock=monotonic_clock,
        )
    return read_gate.read(
        lambda: _query_database_reference_evidence(
            database_path,
            prior_trade_date=prior_trade_date,
            projection_as_of_date=projection_as_of_date,
            limits=limits,
            monotonic_deadline=monotonic_deadline,
            monotonic_clock=monotonic_clock,
        ),
        key=("reference-slow-evidence", prior_trade_date, projection_as_of_date),
    ).value


def _query_database_reference_evidence(
    database_path: Path,
    *,
    prior_trade_date: date,
    projection_as_of_date: date | None = None,
    limits: ReferenceSlowSourceLimits = _DEFAULT_LIMITS,
    monotonic_deadline: float = float("inf"),
    monotonic_clock: Callable[[], float] = monotonic,
) -> _ReferenceEvidence:
    import duckdb

    limits = ReferenceSlowSourceLimits.model_validate(limits)
    projection_date = projection_as_of_date or prior_trade_date
    normalized: list[tuple[str, float, float]] = []
    response_bytes = 0
    projections: dict[str, tuple[dict[str, str | int | float | bool | None], ...]] = {}
    with _verified_database_read(
        database_path,
        limits=limits,
        monotonic_deadline=monotonic_deadline,
        monotonic_clock=monotonic_clock,
    ) as opened:
        connection = opened.connection
        try:
            cursor = connection.execute(
                """
                SELECT daily.ts_code, daily.close, adjustment.adj_factor
                FROM daily_bar AS daily
                JOIN adj_factor AS adjustment
                  ON adjustment.ts_code = daily.ts_code
                 AND adjustment.trade_date = daily.trade_date
                WHERE daily.trade_date = ?
                ORDER BY daily.ts_code
                """,
                [prior_trade_date],
            )
            while True:
                if monotonic_clock() > monotonic_deadline:
                    raise ReferenceSlowSourceError(
                        "reference source read deadline expired"
                    )
                rows = cursor.fetchmany(limits.query_chunk_rows)
                if not rows:
                    break
                for raw_code, raw_close, raw_factor in rows:
                    code = str(raw_code).strip().upper()
                    close = float(raw_close)
                    factor = float(raw_factor)
                    if (
                        _TS_CODE_PATTERN.fullmatch(code) is None
                        or not math.isfinite(close)
                        or close <= 0
                        or not math.isfinite(factor)
                        or factor <= 0
                    ):
                        raise ReferenceSlowSourceError(
                            "prior daily evidence contains an invalid row"
                        )
                    normalized.append((code, close, factor))
                    if len(normalized) > limits.max_response_rows:
                        raise ReferenceSlowSourceError("prior daily evidence exceeds row limit")
                    response_bytes += len(canonical_json_bytes([code, close, factor]))
                    if response_bytes > limits.max_response_bytes:
                        raise ReferenceSlowSourceError("prior daily evidence exceeds byte limit")

            table_rows = connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            tables = {str(row[0]) for row in table_rows}
            column_rows = connection.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'main'
                """
            ).fetchall()
            table_columns: dict[str, set[str]] = {}
            for table_name, column_name in column_rows:
                table_columns.setdefault(str(table_name), set()).add(str(column_name))

            def bounded_projection(
                table_name: str,
                sql: str,
                parameters: list[object] | None = None,
                *,
                source_table: str | None = None,
            ) -> None:
                nonlocal response_bytes
                if (source_table or table_name) not in tables:
                    return
                contract = PAGE_PROJECTION_CONTRACTS[table_name]
                result = connection.execute(sql, parameters or [])
                columns = tuple(str(item[0]) for item in result.description)
                fetched = result.fetchmany(min(contract.max_rows, limits.max_response_rows) + 1)
                if len(fetched) > min(contract.max_rows, limits.max_response_rows):
                    raise ReferenceSlowSourceError(
                        f"{table_name} reference projection exceeds row limit"
                    )
                rows = tuple(
                    {
                        column: _projection_scalar(value)
                        for column, value in zip(columns, row, strict=True)
                    }
                    for row in fetched
                )
                response_bytes += len(canonical_json_bytes(rows))
                if response_bytes > limits.max_response_bytes:
                    raise ReferenceSlowSourceError(
                        "reference projections exceed the source byte budget"
                    )
                projections[table_name] = rows

            if {"ts_code", "name", "industry"}.issubset(table_columns.get("stock_basic", set())):
                bounded_projection(
                    "stock_basic",
                    """
                    SELECT basic.ts_code, basic.name, COALESCE(basic.industry, '') AS industry
                    FROM stock_basic AS basic
                    JOIN daily_bar AS daily
                      ON daily.ts_code = basic.ts_code
                     AND daily.trade_date = ?
                    ORDER BY basic.ts_code
                    LIMIT 8000
                    """,
                    [prior_trade_date],
                )
            bounded_projection(
                "risk_blacklist",
                """
                SELECT ts_code, list_label, expires_at,
                       CAST(imported_at AS TIMESTAMP) AT TIME ZONE 'UTC' AS imported_at
                FROM risk_blacklist
                WHERE imported_at <= ? AND expires_at >= ?
                ORDER BY list_label, ts_code
                """,
                [projection_date, projection_date],
            )
            bounded_projection(
                "dc_board",
                """
                SELECT ts_code, name, idx_type
                FROM dc_board
                WHERE idx_type IN ('行业板块', '概念板块')
                ORDER BY ts_code
                """,
            )
            bounded_projection(
                "dc_board_member",
                """
                SELECT board_code, con_code
                FROM dc_board_member
                ORDER BY board_code, con_code
                LIMIT 10000
                """,
            )
            bounded_projection(
                "kpl_concept_member",
                """
                SELECT board_code, board_name, con_code
                FROM kpl_concept_member
                ORDER BY board_code, con_code
                LIMIT 10000
                """,
            )
            daily_columns = table_columns.get("daily_bar", set())
            if {
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            }.issubset(daily_columns):
                bounded_projection(
                    "daily_bar",
                    """
                    WITH recent_amount AS (
                        SELECT ts_code, amount,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ts_code ORDER BY trade_date DESC
                               ) AS rn
                        FROM daily_bar
                        WHERE trade_date <= ?
                    ), liquid_codes AS (
                        SELECT ts_code
                        FROM recent_amount
                        WHERE rn <= 5
                        GROUP BY ts_code
                        ORDER BY AVG(amount) DESC NULLS LAST, ts_code
                        LIMIT 80
                    ), ranked AS (
                        SELECT daily.ts_code, daily.trade_date, daily.open, daily.high,
                               daily.low, daily.close, daily.vol,
                               ROW_NUMBER() OVER (
                                   PARTITION BY daily.ts_code ORDER BY daily.trade_date DESC
                               ) AS rn
                        FROM daily_bar AS daily
                        JOIN liquid_codes USING (ts_code)
                        WHERE daily.trade_date <= ?
                    )
                    SELECT ts_code, trade_date, open, high, low, close, vol
                    FROM ranked
                    WHERE rn <= 120
                    ORDER BY ts_code, trade_date
                    LIMIT 10000
                    """,
                    [projection_date, projection_date],
                )
            if {"ts_code", "trade_date", "circ_mv"}.issubset(
                table_columns.get("daily_basic", set())
            ) and {"ts_code", "trade_date", "amount"}.issubset(daily_columns):
                bounded_projection(
                    "market_liquidity",
                    """
                    WITH basic AS (
                        SELECT ts_code, circ_mv,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ts_code ORDER BY trade_date DESC
                               ) AS rn
                        FROM daily_basic
                        WHERE trade_date <= ?
                    ), recent AS (
                        SELECT ts_code, amount,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ts_code ORDER BY trade_date DESC
                               ) AS rn
                        FROM daily_bar
                        WHERE trade_date <= ?
                    ), amount5 AS (
                        SELECT ts_code, AVG(amount * 1000.0) AS avg_amount_5d
                        FROM recent WHERE rn <= 5 GROUP BY ts_code
                    )
                    SELECT basic.ts_code, basic.circ_mv, amount5.avg_amount_5d
                    FROM basic JOIN amount5 USING (ts_code)
                    WHERE basic.rn = 1
                    ORDER BY basic.ts_code
                    LIMIT 8000
                    """,
                    [projection_date, projection_date],
                    source_table="daily_basic",
                )
            if (
                {"ts_code", "trade_date", "close", "pct_chg"}.issubset(daily_columns)
                and {"ts_code", "name"}.issubset(table_columns.get("stock_basic", set()))
                and {
                    "ts_code",
                    "trade_date",
                    "is_st",
                    "is_bj",
                    "board_type",
                }.issubset(table_columns.get("daily_state", set()))
            ):
                projection_columns = [
                    "daily.trade_date",
                    "daily.ts_code",
                    "basic.name",
                    "state.is_st",
                    "state.is_bj",
                    "state.board_type",
                    'daily.close AS "CLOSE[0]"',
                    'daily.pct_chg AS "PCT_CHG[0]"',
                ]
                daily_aliases = {
                    "open": "OPEN",
                    "high": "HIGH",
                    "low": "LOW",
                    "pre_close": "PRE_CLOSE",
                    "vol": "VOL",
                    "amount": "AMOUNT",
                }
                indicator_aliases = {
                    "ma5": "MA5",
                    "ma10": "MA10",
                    "ma20": "MA20",
                    "ma60": "MA60",
                    "rsi6": "RSI6",
                    "rsi14": "RSI14",
                    "macd": "MACD",
                    "macd_signal": "MACD_SIGNAL",
                    "macd_hist": "MACD_HIST",
                    "kdj_k": "KDJ_K",
                    "kdj_d": "KDJ_D",
                    "kdj_j": "KDJ_J",
                }
                state_aliases = {
                    "is_limit_up": "IS_LIMIT_UP",
                    "is_limit_down": "IS_LIMIT_DOWN",
                    "is_first_limit_up": "IS_FIRST_LIMIT_UP",
                    "is_yiziban": "IS_YIZIBAN",
                    "consecutive_limit_ups": "CONSECUTIVE_LIMIT_UPS",
                    "body_upper": "BODY_UPPER",
                    "body_lower": "BODY_LOWER",
                }
                basic_aliases = {
                    "circ_mv": "CIRC_MV",
                    "total_mv": "TOTAL_MV",
                    "turnover_rate": "TURNOVER_RATE",
                }
                projection_columns.extend(
                    f'daily.{column} AS "{alias}[0]"'
                    for column, alias in daily_aliases.items()
                    if column in daily_columns
                )
                projection_columns.extend(
                    f'state.{column} AS "{alias}[0]"'
                    for column, alias in state_aliases.items()
                    if column in table_columns["daily_state"]
                )
                joins = """
                    FROM daily_bar AS daily
                    JOIN stock_basic AS basic USING (ts_code)
                    JOIN daily_state AS state
                      ON state.ts_code = daily.ts_code
                     AND state.trade_date = daily.trade_date
                """
                if {"ts_code", "trade_date"}.union(indicator_aliases).issubset(
                    table_columns.get("daily_indicator", set())
                ):
                    joins += """
                    LEFT JOIN daily_indicator AS indicator
                      ON indicator.ts_code = daily.ts_code
                     AND indicator.trade_date = daily.trade_date
                    """
                    projection_columns.extend(
                        f'indicator.{column} AS "{alias}[0]"'
                        for column, alias in indicator_aliases.items()
                    )
                if {"ts_code", "trade_date"}.union(basic_aliases).issubset(
                    table_columns.get("daily_basic", set())
                ):
                    joins += """
                    LEFT JOIN daily_basic AS basic_daily
                      ON basic_daily.ts_code = daily.ts_code
                     AND basic_daily.trade_date = daily.trade_date
                    """
                    projection_columns.extend(
                        f'basic_daily.{column} AS "{alias}[0]"'
                        for column, alias in basic_aliases.items()
                    )
                bounded_projection(
                    "nl_screen_universe",
                    f"""
                    SELECT {", ".join(projection_columns)}
                    {joins}
                    WHERE daily.trade_date = ?
                    ORDER BY daily.trade_date, daily.ts_code
                    """,
                    [prior_trade_date],
                    source_table="daily_state",
                )
        except duckdb.Error as exc:
            raise ReferenceSlowSourceError("reference source database query failed") from exc

    result = tuple(normalized)
    if not result:
        raise ReferenceSlowSourceError("prior open session has no daily reference evidence")
    if tuple(code for code, _close, _factor in result) != tuple(
        sorted({code for code, _close, _factor in result})
    ):
        raise ReferenceSlowSourceError("prior daily evidence contains duplicate codes")
    return result, projections


def _load_prior_daily(
    database_path: Path,
    *,
    prior_trade_date: date,
    limits: ReferenceSlowSourceLimits = _DEFAULT_LIMITS,
    monotonic_deadline: float = float("inf"),
    monotonic_clock: Callable[[], float] = monotonic,
) -> tuple[tuple[str, float, float], ...]:
    rows, _projections = _load_database_reference_evidence(
        database_path,
        prior_trade_date=prior_trade_date,
        projection_as_of_date=prior_trade_date,
        limits=limits,
        monotonic_deadline=monotonic_deadline,
        monotonic_clock=monotonic_clock,
    )
    return rows


def _parse_date(value: object, *, field_name: str) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return (
            datetime.strptime(text, "%Y%m%d").date() if len(text) == 8 else date.fromisoformat(text)
        )
    except ValueError as exc:
        raise ReferenceSlowSourceError(f"{field_name} contains an invalid date") from exc


def _required_frame(
    frame: object,
    *,
    label: str,
    columns: set[str],
    limits: ReferenceSlowSourceLimits,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ReferenceSlowSourceError(f"{label} source must return a DataFrame")
    missing = columns.difference(frame.columns)
    if missing:
        raise ReferenceSlowSourceError(
            f"{label} source is missing columns: {', '.join(sorted(missing))}"
        )
    if len(frame) > limits.max_response_rows:
        raise ReferenceSlowSourceError(f"{label} source exceeds row limit")
    frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())
    if frame_bytes > limits.max_response_bytes:
        raise ReferenceSlowSourceError(f"{label} source exceeds byte limit")
    return frame


def _frame_rows(frame: pd.DataFrame, columns: tuple[str, ...]) -> Iterator[tuple[object, ...]]:
    indexes = tuple(int(frame.columns.get_loc(column)) for column in columns)
    for row in frame.itertuples(index=False, name=None):
        yield tuple(row[index] for index in indexes)


def _security_source_facts(
    frame: pd.DataFrame,
    *,
    list_status: str,
    st_codes: frozenset[str],
    limits: ReferenceSlowSourceLimits,
) -> tuple[ReferenceSecuritySourceFact, ...]:
    required = _required_frame(
        frame,
        label="stock_basic",
        columns={
            "ts_code",
            "name",
            "list_date",
            "delist_date",
            "market",
        },
        limits=limits,
    )
    rows: dict[str, ReferenceSecuritySourceFact] = {}
    has_delist_date = "delist_date" in required.columns
    columns = ("ts_code", "name", "list_date", "market") + (
        ("delist_date",) if has_delist_date else ()
    )
    for row in _frame_rows(required, columns):
        raw_code, raw_name, raw_list_date, raw_market, *optional_delist = row
        code = str(raw_code).strip().upper()
        if _TS_CODE_PATTERN.fullmatch(code) is None:
            raise ReferenceSlowSourceError("stock_basic contains an invalid ts_code")
        if code in rows:
            raise ReferenceSlowSourceError("stock_basic contains duplicate status rows")
        name, name_is_st = normalize_name(raw_name)
        if name is None or name_is_st is None:
            raise ReferenceSlowSourceError(f"{code} stock_basic name is invalid")
        if list_status not in {"L", "D", "P"}:
            raise ReferenceSlowSourceError("stock_basic contains an invalid list_status")
        raw_delist_date = optional_delist[0] if optional_delist else None
        delist_date = (
            None
            if raw_delist_date is None
            or pd.isna(raw_delist_date)
            or not str(raw_delist_date).strip()
            else _parse_date(raw_delist_date, field_name="stock_basic.delist_date")
        )
        if list_status == "D" and delist_date is None:
            raise ReferenceSlowSourceError("delisted stock_basic row lacks delist_date")
        rows[code] = ReferenceSecuritySourceFact(
            ts_code=code,
            name=name,
            is_st=name_is_st or code in st_codes,
            list_date=_parse_date(raw_list_date, field_name="stock_basic.list_date"),
            delist_date=delist_date,
            source_list_status=list_status,
            market=str(raw_market).strip(),
        )
    return tuple(rows[code] for code in sorted(rows))


def _is_tradable_on(
    fact: ReferenceSecuritySourceFact,
    *,
    target_trade_date: date,
) -> bool:
    if fact.list_date > target_trade_date or fact.source_list_status == "P":
        return False
    return fact.delist_date is None or fact.delist_date > target_trade_date


def _st_codes(
    frame: pd.DataFrame,
    *,
    target_trade_date: date,
    limits: ReferenceSlowSourceLimits,
) -> frozenset[str]:
    required = _required_frame(
        frame,
        label="stock_st",
        columns={"ts_code", "trade_date"},
        limits=limits,
    )
    codes: list[str] = []
    for raw_code, raw_trade_date in _frame_rows(required, ("ts_code", "trade_date")):
        if _parse_date(raw_trade_date, field_name="stock_st.trade_date") != target_trade_date:
            raise ReferenceSlowSourceError("stock_st contains a different trade_date")
        code = str(raw_code).strip().upper()
        if _TS_CODE_PATTERN.fullmatch(code) is None:
            raise ReferenceSlowSourceError("stock_st contains an invalid ts_code")
        codes.append(code)
    if len(codes) != len(set(codes)):
        raise ReferenceSlowSourceError("stock_st contains duplicate codes")
    return frozenset(codes)


def _target_factors(
    frame: pd.DataFrame,
    *,
    target_trade_date: date,
    codes: tuple[str, ...],
    limits: ReferenceSlowSourceLimits,
) -> dict[str, float]:
    required = _required_frame(
        frame,
        label="adj_factor",
        columns={"ts_code", "trade_date", "adj_factor"},
        limits=limits,
    )
    factors: dict[str, float] = {}
    for raw_code, raw_trade_date, raw_factor in _frame_rows(
        required,
        ("ts_code", "trade_date", "adj_factor"),
    ):
        code = str(raw_code).strip().upper()
        if code not in codes:
            continue
        if code in factors:
            raise ReferenceSlowSourceError("adj_factor contains duplicate selected codes")
        if _parse_date(raw_trade_date, field_name="adj_factor.trade_date") != target_trade_date:
            raise ReferenceSlowSourceError("adj_factor contains a different trade_date")
        factor = float(raw_factor)
        if not math.isfinite(factor) or factor <= 0:
            raise ReferenceSlowSourceError("adj_factor contains an invalid value")
        factors[code] = factor
    if set(factors) != set(codes):
        raise ReferenceSlowSourceError("target adj_factor does not cover prior daily universe")
    return factors


def capture_reference_slow_source_snapshot(
    *,
    database_path: Path,
    adapter: ReferenceSlowAdapter,
    calendar: MarketCalendarAuthority,
    target_trade_date: date,
    captured_at: datetime,
    completion_clock: Callable[[], datetime],
    producer_commit: str,
    limits: ReferenceSlowSourceLimits = _DEFAULT_LIMITS,
    monotonic_clock: Callable[[], float] = monotonic,
    read_gate: ReplicaReadGate[_ReferenceEvidence] | None = None,
) -> ReferenceSlowSourceSnapshot:
    """Capture all current source responses once and seal their relevant facts."""

    calendar = MarketCalendarAuthority.model_validate(calendar)
    started = normalize_aware_utc(captured_at)
    if calendar.generated_at > started:
        raise ReferenceSlowSourceError("calendar is future evidence")
    if target_trade_date not in calendar.open_dates:
        raise ReferenceSlowSourceError("target_trade_date is not open")
    local_observed = started.astimezone(_SHANGHAI)
    if target_trade_date > local_observed.date():
        raise ReferenceSlowSourceError("source capture cannot target a future trade date")
    if local_observed.timetz().replace(tzinfo=None) > time(9, 25):
        raise ReferenceSlowSourceError("source capture must complete by 09:25 Asia/Shanghai")
    prior_dates = tuple(item for item in calendar.open_dates if item < target_trade_date)
    if not prior_dates:
        raise ReferenceSlowSourceError("calendar has no prior open date")
    prior_trade_date = prior_dates[-1]

    limits = ReferenceSlowSourceLimits.model_validate(limits)
    decision_cutoff = datetime.combine(
        local_observed.date(),
        time(9, 25),
        tzinfo=_SHANGHAI,
    )
    read_seconds = min(
        limits.snapshot_copy_timeout_seconds,
        max(0.0, (decision_cutoff - started).total_seconds()),
    )
    read_deadline = monotonic_clock() + read_seconds
    prior_rows, database_projections = _load_database_reference_evidence(
        _normalized_absolute_path(database_path),
        prior_trade_date=prior_trade_date,
        projection_as_of_date=target_trade_date,
        limits=limits,
        monotonic_deadline=read_deadline,
        monotonic_clock=monotonic_clock,
        read_gate=read_gate,
    )
    codes = tuple(code for code, _close, _factor in prior_rows)
    stock_st_frame = adapter.stock_st_raw(target_trade_date)
    observed_st_codes = _st_codes(
        stock_st_frame,
        target_trade_date=target_trade_date,
        limits=limits,
    )
    security_by_code: dict[str, ReferenceSecuritySourceFact] = {}
    for list_status in ("L", "D", "P"):
        frame = adapter.stock_basic(list_status=list_status)
        facts = _security_source_facts(
            frame,
            list_status=list_status,
            st_codes=observed_st_codes,
            limits=limits,
        )
        for fact in facts:
            if fact.ts_code in security_by_code:
                raise ReferenceSlowSourceError("stock_basic contains duplicate cross-status rows")
            security_by_code[fact.ts_code] = fact
    all_security_facts = tuple(security_by_code[code] for code in sorted(security_by_code))
    missing_security = sorted(set(codes).difference(security_by_code))
    if missing_security:
        raise ReferenceSlowSourceError(
            f"stock_basic does not cover prior daily universe: {', '.join(missing_security[:10])}"
        )
    selected_codes = tuple(
        code
        for code in codes
        if _is_tradable_on(security_by_code[code], target_trade_date=target_trade_date)
    )
    if not selected_codes:
        raise ReferenceSlowSourceError("target session tradable universe is empty")
    selected_code_set = frozenset(selected_codes)
    prior_rows = tuple(row for row in prior_rows if row[0] in selected_code_set)
    codes = selected_codes
    securities = tuple(security_by_code[code] for code in codes)
    target_factor_frame = adapter.adj_factor_by_date(target_trade_date)
    target_factors = _target_factors(
        target_factor_frame,
        target_trade_date=target_trade_date,
        codes=codes,
        limits=limits,
    )
    suspension_frame = adapter.suspend_d_raw(target_trade_date)
    if not suspension_frame.empty:
        _required_frame(
            suspension_frame,
            label="suspend_d",
            columns={"ts_code", "trade_date", "suspend_timing", "suspend_type"},
            limits=limits,
        )
    completed = normalize_aware_utc(completion_clock())
    if completed < started:
        raise ReferenceSlowSourceError("source completion time precedes capture start")
    local_completed = completed.astimezone(_SHANGHAI)
    if local_completed.date() != local_observed.date():
        raise ReferenceSlowSourceError("source capture must complete on its discovery date")
    if local_completed.timetz().replace(tzinfo=None) > time(9, 25):
        raise ReferenceSlowSourceError("source capture must complete by 09:25 Asia/Shanghai")
    suspension = normalize_suspend_d_snapshot(
        suspension_frame,
        trade_date=target_trade_date,
        queried_at=completed,
    )
    daily_facts = tuple(
        ReferenceDailySourceFact(
            ts_code=code,
            trade_date=prior_trade_date,
            close_raw=close,
        )
        for code, close, _prior_factor in prior_rows
    )
    adjustment_facts = tuple(
        fact
        for code, _close, prior_factor in prior_rows
        for fact in (
            ReferenceAdjustmentSourceFact(
                ts_code=code,
                trade_date=prior_trade_date,
                adj_factor=prior_factor,
            ),
            ReferenceAdjustmentSourceFact(
                ts_code=code,
                trade_date=target_trade_date,
                adj_factor=target_factors[code],
            ),
        )
    )
    security_facts = tuple(
        ReferenceSecuritySourceFact(
            ts_code=fact.ts_code,
            name=fact.name,
            is_st=fact.is_st,
            list_date=fact.list_date,
            delist_date=fact.delist_date,
            source_list_status=fact.source_list_status,
            market=fact.market,
        )
        for fact in securities
    )
    suspension_facts = tuple(
        ReferenceSuspensionSourceFact(
            ts_code=event.ts_code,
            trade_date=target_trade_date,
            suspend_type=event.suspend_type,
            session_scope=event.session_scope,
        )
        for event in suspension.events
        if event.ts_code in codes
    )
    base_snapshot = assemble_reference_slow_source_snapshot(
        calendar=calendar,
        observed_at=completed,
        producer_commit=producer_commit,
        daily_facts=daily_facts,
        adjustment_facts=adjustment_facts,
        security_facts=security_facts,
        security_source_facts=all_security_facts,
        suspension_facts=suspension_facts,
    )
    by_table = {projection.table_name: projection for projection in base_snapshot.projections}
    for table_name, rows in database_projections.items():
        prepared_rows = rows
        if table_name == "risk_blacklist":
            combined = {
                (str(row["list_label"]), str(row["ts_code"])): dict(row)
                for row in by_table[table_name].rows
            }
            combined.update(
                {(str(row["list_label"]), str(row["ts_code"])): dict(row) for row in rows}
            )
            prepared_rows = tuple(combined[key] for key in sorted(combined))
        by_table[table_name] = ServingProjectionPayload(
            table_name=table_name,
            available_at=completed,
            rows=prepared_rows,
        )
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=base_snapshot.target_trade_date,
        captured_at=base_snapshot.captured_at,
        producer_commit=base_snapshot.producer_commit,
        source_snapshot_ids=base_snapshot.source_snapshot_ids,
        daily_facts=base_snapshot.daily_facts,
        security_facts=base_snapshot.security_facts,
        suspended_codes=base_snapshot.suspended_codes,
        projections=tuple(by_table[table_name] for table_name in sorted(by_table)),
    )


__all__ = [
    "ReferenceAdjustmentSourceFact",
    "ReferenceDailySourceFact",
    "ReferenceSlowAdapter",
    "ReferenceSlowSourceError",
    "ReferenceSecuritySourceFact",
    "ReferenceSuspensionSourceFact",
    "assemble_reference_slow_source_snapshot",
    "capture_reference_slow_source_snapshot",
]
