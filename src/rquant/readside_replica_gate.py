"""What every read-side role needs to read the five-minute replica cheaply (#256).

Two things live here, because both are about the same file and the same cost.

**Opening one generation without copying it.** `duckdb.connect("/proc/self/fd/<n>")`
opens the *inode* a descriptor holds rather than the name it was reached through, so a
`rename()` over the name during the open cannot swap the generation and nothing is
created anywhere. Linux publishes `/proc/self/fd` and the BSDs `/dev/fd`; the pinned
duckdb 1.5.2 accepts the Linux form and refuses the macOS one, so the fallback is not
theoretical and the caller is told which branch it got.

**Not opening it at all.** On 2026-09-08 and 2026-09-09 the 17:00 daily pipeline stalled
in its `daily_state` stage while the runtime roles were running, and finished within a
minute of their being stopped; memory was not the constraint (9 GB free on 09-09). The
production replica `data/rquant_ro.duckdb` is about 10 GB, the notifier scanned all of
`minute_bar` in it every two seconds, and with the 15-minute backup and the 5-minute
replica copy the page cache could not hold both databases, so the daily's own scans fell
to disk. The replica is *replaced* by `scripts/sync-readonly-replica.sh`, never written
in place, so `(st_dev, st_ino, st_size, st_mtime_ns)` names one immutable generation and
one `lstat` per iteration is enough to know whether this role has already read it.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Generic, TypeVar

from rquant.runtime_contracts import normalize_aware_utc
from rquant.runtime_market_session import MARKET_TIMEZONE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import duckdb

T = TypeVar("T")

#: Where a still-open descriptor can be re-opened by name, in preference order. Same two
#: directories `serving_page_projection_source` uses, for the same reason.
DESCRIPTOR_DIRECTORIES = ("/proc/self/fd", "/dev/fd")

#: What DuckDB says when another process holds the write lock, lowercased. Two markers,
#: because the wording differs across builds and only the first half is stable.
WRITE_LOCK_MARKERS = ("could not set lock", "conflicting lock")

#: The twenty minutes around the open in which a role that already has an answer must not
#: go and get a newer one. On 2026-09-14 the 09:25 replica `cp` of ten gigabytes, the
#: production monitor's own startup scan of the main database, the 09:30 backup `cp`+`gzip`
#: of ten more and four read-side roles opening the new generation landed inside the same
#: minute; load went 14 -> 21, several processes sat in D state, and **the production
#: monitor produced no poll for ten minutes after the open** (#268). Nothing these roles
#: publish is worth that, because nothing they publish changes in those twenty minutes in
#: a way a twenty-minute-old generation misreports.
DEFAULT_NO_READ_WINDOW = (time(9, 20), time(9, 40))


@dataclass(frozen=True, slots=True)
class ReplicaReadProfile:
    """How often this role may open a *new* generation, and when it may not at all.

    Package Q stopped these roles re-reading a generation they had already read. What it
    could not stop is the generation changing: `rquant-replica-sync.timer` replaces the
    replica every five minutes on a trading day, so "read only when it changed" is "read
    every five minutes" -- and for the notifier one of those reads is a multi-gigabyte
    aggregate over `minute_bar` (#268).

    `min_reread_interval` is the floor between two *opens*. A generation that arrives
    inside it is seen, and deliberately not read: the role keeps the answer it has and
    says so in its heartbeat, as `replica_skipped_by_floor`.

    `no_read_window` is a pair of **market-local** times, half-open, that suspends new
    generation reads outright. `None` means the role has a window of its own that already
    confines it -- which is the case for three of the four readers, and why this is not
    simply on everywhere:

    * `reference-slow.source.v1` captures inside 09:20-09:25, and
    * `candidate.auction_gap.v1` assembles inside 09:26-09:30,

    both of which lie *inside* 09:20-09:40. A blanket window would not slow those two
    down; it would stop them working. `auction-universe.publisher.v1` refuses to publish
    anywhere in 09:15-15:10 on its own account, so the window would never bind on it
    either. The notifier is the one role that reads all day, every two seconds, and whose
    read is the expensive one -- so the notifier is the role that carries the window.

    The floor never blocks a role that has **no** answer yet. A cold start inside the
    window must read, or the role has nothing to publish at all and goes DEGRADED for
    twenty minutes; suppressing a *re-*read is the point, not suppressing the role.
    """

    #: the shortest gap between two opens of this replica. Zero leaves the gate exactly as
    #: package Q left it: every new generation is read.
    min_reread_interval: timedelta = timedelta(0)
    #: market-local `[start, end)` in which a role that has an answer keeps it
    no_read_window: tuple[time, time] | None = None

    def __post_init__(self) -> None:
        if self.min_reread_interval < timedelta(0):
            raise ValueError("minimum re-read interval cannot be negative")
        window = self.no_read_window
        if window is not None:
            start, end = window
            if not isinstance(start, time) or not isinstance(end, time):
                raise TypeError("a no-read window is a pair of times")
            if start.tzinfo is not None or end.tzinfo is not None:
                raise ValueError("a no-read window is stated in market-local time")
            if start >= end:
                raise ValueError("a no-read window must start before it ends")

    def suspends_reads_at(self, observed_at: datetime) -> bool:
        """Whether `observed_at` falls inside this profile's no-read window.

        The market clock is `runtime_market_session.MARKET_TIMEZONE`, the same one
        `may_fetch_market_minute` is decided in, imported rather than restated.

        The calendar's *open dates* are deliberately not consulted. A window that also
        asked "is today a trading day" would need the signed calendar authority inside the
        gate, and the notifier -- the only role carrying a window -- is configured with no
        calendar path at all. The cost of being wrong on a Sunday is that a page projection
        is up to twenty minutes older than it could be between 09:20 and 09:40 on a day
        when nothing is trading, which is nothing; the cost of being wrong on a Monday is
        #268.
        """

        window = self.no_read_window
        if window is None:
            return False
        start, end = window
        local = observed_at.astimezone(MARKET_TIMEZONE).timetz().replace(tzinfo=None)
        return start <= local < end


#: What a gate built without a profile gets: every new generation is read, which is
#: exactly where package Q left these roles. The four production roles are given their own
#: by their builder.
UNLIMITED_READ_PROFILE = ReplicaReadProfile()

#: `notifier.admin.shadow.v1`. A two-second loop whose generation read is the whole of
#: what it takes from the replica -- 44,052,711 of 44,052,711 bytes on package Q's
#: measurement replica, all of it the `minute_bar` aggregate. Fifteen minutes is the floor
#: #268 asks for, and it is the role that carries the open window.
NOTIFIER_PAGE_PROJECTION_PROFILE = ReplicaReadProfile(
    min_reread_interval=timedelta(minutes=15),
    no_read_window=DEFAULT_NO_READ_WINDOW,
)

#: `reference-slow.source.v1`, floored at its own 09:20-09:25 capture window. Its loop runs
#: every thirty seconds, so a *retrying* capture -- a quota refusal, a credential -- asks
#: about ten times per window, and before package Q every one of those re-read the replica.
#: The gate stopped the repeats of one generation; this stops the repeats across the
#: generation the 09:25 replica sync drops in the middle of the window.
REFERENCE_SLOW_SOURCE_PROFILE = ReplicaReadProfile(min_reread_interval=timedelta(minutes=5))

#: `candidate.auction_gap.v1`, floored at its own 09:26-09:30 assembly window. What it
#: reads is prior sessions' `daily_bar` volumes, which do not change while the session
#: opens, so a generation arriving inside the window carries the same answer at the cost of
#: another scan. One read per session is the whole of what this role needs.
AUCTION_GAP_CANDIDATE_PROFILE = ReplicaReadProfile(min_reread_interval=timedelta(minutes=4))

#: `auction-universe.publisher.v1`. It has no narrow window -- it refuses 09:15-15:10 and
#: works either side -- so its floor is one replica generation. It publishes once per
#: session and then recognises its own `current.json` without asking the gate; the floor
#: bounds the loop before that, which package Q measured at about ten reads per generation.
AUCTION_UNIVERSE_PUBLISHER_PROFILE = ReplicaReadProfile(min_reread_interval=timedelta(minutes=5))


def _utc_now() -> datetime:
    return datetime.now(UTC)


def descriptor_reopen_path(descriptor: int) -> str | None:
    """The name that re-opens exactly this descriptor's inode, or None if the OS has none."""

    for directory in DESCRIPTOR_DIRECTORIES:
        if os.path.isdir(directory):
            return f"{directory}/{descriptor}"
    return None


def is_write_lock_error(error: BaseException) -> bool:
    """Whether this is "somebody else holds the write lock" rather than a path refusal.

    #255's whole cost was that the two were reported as one sentence: an engine that does
    not understand `/proc/self/fd/<n>` and a database `rquant-monitor` holds open produced
    the same message, and the window chased the wrong cause. A caller that falls back on a
    path refusal must not fall back on a lock -- the fallback opens the same locked file.
    """

    text = str(error).lower()
    return any(marker in text for marker in WRITE_LOCK_MARKERS)


def connect_pinned_readonly(
    path: Path,
    descriptor: int,
) -> tuple[duckdb.DuckDBPyConnection, str]:
    """Open `path` read-only through the inode `descriptor` holds, or by name if it cannot.

    Returns the connection and which branch took: `"descriptor"` (the inode is pinned;
    what a Linux runtime host gets) or `"in_place"` (the name is re-opened and the caller's
    identity check after the read is what says the generation did not move). A write lock
    is re-raised as itself rather than treated as a path refusal.
    """

    import duckdb

    reopen = descriptor_reopen_path(descriptor)
    if reopen is not None:
        try:
            return duckdb.connect(reopen, read_only=True), "descriptor"
        except Exception as error:  # noqa: BLE001 - classified, then re-raised or passed
            if is_write_lock_error(error):
                raise
    return duckdb.connect(str(path), read_only=True), "in_place"


@dataclass(frozen=True, slots=True)
class ReplicaGeneration:
    """Which file, and which version of it, one `lstat` away.

    `st_size` and `st_mtime_ns` are in here as well as the inode number because an inode
    is reused: the replica sync unlinks the previous generation, and the next `mv` can
    land on the same number. All four together are what a role compares against the
    generation it last read.
    """

    device: int
    inode: int
    size: int
    mtime_ns: int

    @classmethod
    def of(cls, value: os.stat_result) -> ReplicaGeneration:
        return cls(value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)

    @classmethod
    def observe(cls, path: Path) -> ReplicaGeneration | None:
        """This generation, or None when there is no regular file to read at that name."""

        try:
            observed = os.lstat(path)
        except OSError:
            return None
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
            return None
        return cls.of(observed)

    @property
    def modified_at(self) -> datetime:
        """When this generation was last written, which is the point in time it carries.

        `auction_gap_candidate_input` already takes the replica's `st_mtime_ns` as the
        `available_at` of everything inside it, so this is the codebase's own reading of
        the file rather than a new claim about it.
        """

        return datetime.fromtimestamp(self.mtime_ns / 1_000_000_000, tz=UTC)


@dataclass(frozen=True, slots=True)
class ReplicaRead(Generic[T]):
    """One iteration's answer, and what it cost.

    `value` is `None` on the one kind of read that has no answer: the loader raised. That
    object never leaves `read()` -- the exception does -- but it is what `last_read` and
    `iteration_summary()` report, because a read that failed part-way still opened the
    database and still cost what it cost (review SF-7).
    """

    value: T
    #: whether this iteration opened the database, or reused what the last open returned
    opened: bool
    generation: ReplicaGeneration | None
    #: bytes this process read from the filesystem while the loader ran, where the
    #: platform will say (Linux `/proc/self/io`); None where it will not
    read_bytes: int | None = None
    #: whether this iteration saw a *newer* generation and kept the previous answer
    #: because this role's profile would not let it open one yet (#268). Never true
    #: together with `opened`: the floor is a decision not to open.
    skipped_by_floor: bool = False


def _process_read_bytes() -> int | None:
    """Bytes this process has read through the read syscalls, from `/proc/self/io` `rchar`.

    `rchar` rather than `read_bytes` on purpose: `read_bytes` counts what actually went to
    the block device, so a query served from the page cache reports zero -- and the page
    cache is exactly what #256 is about, so a metric that goes quiet when the cache is
    warm measures the wrong thing. `rchar` counts what the process asked for either way.
    It is process-wide, so a role that reads other files while the loader runs attributes
    them here too; for these four roles the loader does nothing but query the replica.
    """

    try:
        with open("/proc/self/io", "rb") as handle:
            for line in handle:
                if line.startswith(b"rchar:"):
                    return int(line.split(b":", 1)[1].strip())
    except (OSError, ValueError):
        return None
    return None


class ReplicaReadGate(Generic[T]):
    """One role's memory of the replica generation it last read, and of what it read.

    The rule is the one ruling 25 states: `lstat` the replica each iteration, compare with
    the generation this role last read, and open the database only when they differ. What
    makes that sound rather than a guess is that the replica is *replaced*, never written
    in place -- `sync-readonly-replica.sh` writes `rquant_ro.duckdb.tmp.$$`, verifies it,
    and `mv`s it over the name -- so identical `(dev, ino, size, mtime_ns)` is identical
    content.

    `key` is whatever else the answer depends on: the trade date a query is bound to, the
    codes it asks for. A different key is a different question and always re-opens.

    `cutoff` is for the one reader whose question moves on its own: the notifier's page
    projection is `f(contents, now)`, and `now` advances every two seconds. Reuse there is
    allowed only when the cached answer was taken at a point in time at or after the
    generation's own `mtime` -- every row in the file was written before the file was, so
    a later cutoff admits exactly the same rows -- and never backwards.

    **Calendar granularity is the caller's, and it belongs in `key`.** This class compares
    instants and knows nothing about anybody's local zone; a reader whose predicates are
    written against a *date* (`trade_date <= ?`) has to put that date in `key`, in the zone
    its own code already uses. Until the package Q review this method also compared
    `cutoff.date()`, which is the **UTC** date, while three docstrings claimed it was the
    local one -- true-by-accident for the only caller (its `key` carried the local date)
    and misleading for the next one. The comparison is gone rather than corrected: two
    mechanisms for one rule is how the wrong one gets relied on.

    `begin_iteration()` / `iteration_summary()` are the *per-iteration* scope the heartbeat
    needs. `last_read` alone answers "what did the most recent `read()` do", which is not
    the same question: a role whose loop iteration returns before asking the gate at all --
    reference-slow outside its 09:20-09:25 capture window, the auction-gap publisher
    outside 09:26-09:30 -- would otherwise report the last *real* read forever, and
    reference-slow's heartbeat would read `replica_opened=true` all day (review MF-1).
    """

    def __init__(
        self,
        path: Path,
        *,
        observer: Callable[[Path], ReplicaGeneration | None] = ReplicaGeneration.observe,
        profile: ReplicaReadProfile = UNLIMITED_READ_PROFILE,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.path = Path(os.path.abspath(path))
        self._observer = observer
        #: how often this role may open a new generation, and when it may not (#268).
        #: The default profile is inert, so a gate built without one behaves exactly as
        #: package Q left it; the four production roles are given theirs by their builder,
        #: which is where a role's identity lives.
        self.profile = profile
        self._clock = clock
        self._generation: ReplicaGeneration | None = None
        self._key: Hashable = None
        self._cutoff: datetime | None = None
        self._value: T | None = None
        self._cached = False
        self._last: ReplicaRead[T] | None = None
        #: when this gate last actually opened the database, for the floor
        self._opened_at: datetime | None = None

    @property
    def last_read(self) -> ReplicaRead[T] | None:
        """What the most recent `read()` did -- since `begin_iteration()`, if it was called."""

        return self._last

    def begin_iteration(self) -> None:
        """Start a new loop iteration: forget what the last one did, keep what it read.

        The cache is deliberately untouched. What is cleared is only the *report*, so that
        an iteration which never reaches a `read()` says so instead of repeating the last
        one that did.
        """

        self._last = None

    def iteration_skipped_by_floor(self) -> bool:
        """Whether this iteration kept an older answer because of the profile (#268).

        False for an iteration that never asked and for one that read: those are `False, 0`
        and `True, bytes` in `iteration_summary()` respectively, and neither is a skip. It
        is a separate accessor rather than a third member of that tuple because
        `iteration_summary()` is what four builders and `run_service_loop`'s failure path
        already unpack, and widening it would say nothing they do not each have to ask for.
        """

        read = self._last
        return read is not None and read.skipped_by_floor

    def iteration_summary(self) -> tuple[bool, int | None]:
        """`(opened, read_bytes)` for this iteration, for the heartbeat.

        `(False, 0)` when this iteration never asked -- it opened nothing and read nothing,
        which is a fact rather than an absence. `(False, 0)` again when it asked and
        recognised the generation. `(True, bytes)` when it opened the database, with
        `bytes` `None` on a platform that will not say (see `_process_read_bytes`) --
        **including when the loader then raised**, because that read still happened.
        """

        read = self._last
        if read is None:
            return False, 0
        return read.opened, read.read_bytes

    def forget(self) -> None:
        self._cached = False
        self._value = None
        self._generation = None
        self._cutoff = None

    def _reusable(
        self,
        current: ReplicaGeneration,
        key: Hashable,
        cutoff: datetime | None,
    ) -> bool:
        if not self._cached or self._generation != current or self._key != key:
            return False
        if cutoff is None:
            return self._cutoff is None
        if self._cutoff is None:
            return False
        return self._cutoff >= current.modified_at and cutoff >= self._cutoff

    def _reusable_across_generations(self, key: Hashable, cutoff: datetime | None) -> bool:
        """Whether the cached answer may stand in for a generation it was not taken from.

        The same question -- `key` is what makes a question a different one, and a
        different question always opens. What is dropped compared with `_reusable` is the
        one clause that binds the answer to *this* generation, `self._cutoff >=
        current.modified_at`: keeping an answer taken before the file was written is
        exactly what the floor is for. `cutoff >= self._cutoff` stays, because a reader
        whose question moves forward in time may not be handed an answer from later than
        it is asking about.
        """

        if not self._cached or self._key != key:
            return False
        if cutoff is None:
            return self._cutoff is None
        if self._cutoff is None:
            return False
        return cutoff >= self._cutoff

    def _floor_blocks(self, observed_at: datetime) -> bool:
        """Whether this role's profile refuses to open a new generation right now.

        `observed_at` has already been through `normalize_aware_utc`, so the window's
        `astimezone` is answering in the market clock rather than in whatever the host
        thinks local time is -- the production host runs on CST and the difference would
        not show there, which is exactly how it would get out.

        A clock that moved backwards -- NTP on a host that has just come up -- does not
        block: an answer must never be held because the floor's arithmetic went negative.
        """

        if self.profile.suspends_reads_at(observed_at):
            return True
        interval = self.profile.min_reread_interval
        if interval <= timedelta(0) or self._opened_at is None:
            return False
        elapsed = observed_at - self._opened_at
        if elapsed < timedelta(0):
            return False
        return elapsed < interval

    def read(
        self,
        loader: Callable[[], T],
        *,
        key: Hashable = (),
        cutoff: datetime | None = None,
    ) -> ReplicaRead[T]:
        current = self._observer(self.path)
        if current is not None and self._reusable(current, key, cutoff):
            read = ReplicaRead(
                value=self._value,  # type: ignore[arg-type]
                opened=False,
                generation=current,
                read_bytes=0,
            )
            self._last = read
            return read

        #: The generation this role has is not the generation on disk, so the answer is
        #: out of date -- and on a trading day that is true every five minutes, all day,
        #: because the replica is *replaced* on a timer rather than because anything this
        #: role reads has changed (#268). The floor is where that is decided: inside it,
        #: the previous answer stands and the heartbeat says it was kept. A role with no
        #: answer at all is never held here -- `_reusable_across_generations` is False
        #: when nothing is cached -- so a cold start still reads.
        now = normalize_aware_utc(self._clock())
        #: `current is None` is not "a newer generation this role may skip", it is *no
        #: readable file at that name* -- deleted, replaced by a symlink, replaced by a
        #: directory. Holding the previous answer there would publish a stale page for a
        #: quarter of an hour and report it as `replica_skipped_by_floor=true`, which
        #: DEPLOY.md tells the owner to read as the fix working (review SF-1). A vanished
        #: replica must surface as the failure package Q designed: the loader is called,
        #: it says the replica is gone, and the heartbeat says the round failed.
        if (
            current is not None
            and self._floor_blocks(now)
            and self._reusable_across_generations(key, cutoff)
        ):
            read = ReplicaRead(
                value=self._value,  # type: ignore[arg-type]
                opened=False,
                generation=current,
                read_bytes=0,
                skipped_by_floor=True,
            )
            self._last = read
            return read

        before_bytes = _process_read_bytes()

        def measured() -> int | None:
            after_bytes = _process_read_bytes()
            if before_bytes is None or after_bytes is None:
                return None
            return max(0, after_bytes - before_bytes)

        self._opened_at = now
        try:
            value = loader()
        except BaseException:
            #: A loader that raises part-way still opened the database and still read
            #: bytes (review SF-7). The auction-gap publisher's degraded branch is reached
            #: exactly that way -- the replica is replaced under the read and the loader's
            #: own identity check refuses -- and an iteration summary saying `(False, 0)`
            #: there would understate the very cost this package exists to count. What is
            #: *not* kept is the answer: `forget()` below, so the next iteration reads.
            self._last = ReplicaRead(
                value=None,  # type: ignore[arg-type]
                opened=True,
                generation=current,
                read_bytes=measured(),
            )
            self.forget()
            raise
        read_bytes = measured()
        after = self._observer(self.path)
        #: cache only what a whole read saw one generation of. A generation that moved
        #: while the loader ran is not refused here -- every one of these loaders has its
        #: own identity check for that and a better sentence to say about it -- it is
        #: simply not remembered, so the next iteration reads again.
        if current is not None and after == current:
            self._generation = current
            self._key = key
            self._cutoff = cutoff
            self._value = value
            self._cached = True
        else:
            self.forget()
        read = ReplicaRead(
            value=value,
            opened=True,
            generation=after,
            read_bytes=read_bytes,
        )
        self._last = read
        return read
