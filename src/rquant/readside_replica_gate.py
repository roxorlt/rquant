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
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:  # pragma: no cover - typing only
    import duckdb

T = TypeVar("T")

#: Where a still-open descriptor can be re-opened by name, in preference order. Same two
#: directories `serving_page_projection_source` uses, for the same reason.
DESCRIPTOR_DIRECTORIES = ("/proc/self/fd", "/dev/fd")

#: What DuckDB says when another process holds the write lock, lowercased. Two markers,
#: because the wording differs across builds and only the first half is stable.
WRITE_LOCK_MARKERS = ("could not set lock", "conflicting lock")


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
    """One iteration's answer, and what it cost."""

    value: T
    #: whether this iteration opened the database, or reused what the last open returned
    opened: bool
    generation: ReplicaGeneration | None
    #: bytes this process read from the filesystem while the loader ran, where the
    #: platform will say (Linux `/proc/self/io`); None where it will not
    read_bytes: int | None = None


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
    a later cutoff admits exactly the same rows -- and inside the same local date, which
    is the granularity the projection's `trade_date <= ?` predicates use.
    """

    def __init__(
        self,
        path: Path,
        *,
        observer: Callable[[Path], ReplicaGeneration | None] = ReplicaGeneration.observe,
    ) -> None:
        self.path = Path(os.path.abspath(path))
        self._observer = observer
        self._generation: ReplicaGeneration | None = None
        self._key: Hashable = None
        self._cutoff: datetime | None = None
        self._value: T | None = None
        self._cached = False
        self._last: ReplicaRead[T] | None = None

    @property
    def last_read(self) -> ReplicaRead[T] | None:
        """What the most recent `read()` did, for the heartbeat to report."""

        return self._last

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
        return (
            self._cutoff >= current.modified_at
            and cutoff >= self._cutoff
            and cutoff.date() == self._cutoff.date()
        )

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

        before_bytes = _process_read_bytes()
        value = loader()
        after_bytes = _process_read_bytes()
        read_bytes = (
            None
            if before_bytes is None or after_bytes is None
            else max(0, after_bytes - before_bytes)
        )
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
