"""The gate every read-side role puts in front of the 10 GB replica (#256).

Ruling 25's first rule stated as cases: one `lstat` per iteration, and the database is
opened only when that `lstat` says this role has not already read this generation. What
makes it sound is that `scripts/sync-readonly-replica.sh` *replaces* the file -- writes
`rquant_ro.duckdb.tmp.$$`, verifies it, `mv`s it over the name -- so identical
`(dev, ino, size, mtime_ns)` is identical content, and a `rename()` is always a new
generation.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.readside_replica_gate import (
    ReplicaGeneration,
    ReplicaReadGate,
    connect_pinned_readonly,
    descriptor_reopen_path,
    is_write_lock_error,
)

SYNCED_AT = datetime(2026, 8, 11, 1, 23, tzinfo=UTC)


def _replica(path: Path, *, payload: bytes = b"generation-one", synced_at: datetime = SYNCED_AT):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o644)
    stamp = synced_at.timestamp()
    os.utime(path, (stamp, stamp))
    return path


def _replace(path: Path, *, payload: bytes, synced_at: datetime) -> None:
    """What the replica timer does: a new file, `mv`'d over the name."""

    staged = path.parent / f"{path.name}.tmp.1"
    staged.write_bytes(payload)
    staged.chmod(0o644)
    stamp = synced_at.timestamp()
    os.utime(staged, (stamp, stamp))
    os.replace(staged, path)


class _Loader:
    def __init__(self, value: object = "rows") -> None:
        self.calls = 0
        self.value = value

    def __call__(self) -> object:
        self.calls += 1
        return self.value


def test_an_unchanged_generation_is_read_once_and_never_opened_again(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    reads = [gate.read(loader) for _ in range(3)]

    assert loader.calls == 1
    assert [read.opened for read in reads] == [True, False, False]
    assert [read.value for read in reads] == ["rows"] * 3
    assert reads[1].read_bytes == 0


def test_a_replaced_generation_is_opened_exactly_once_more(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader)
    gate.read(loader)
    _replace(
        replica,
        payload=b"generation-two-is-longer",
        synced_at=SYNCED_AT + timedelta(minutes=5),
    )
    after = [gate.read(loader) for _ in range(3)]

    assert loader.calls == 2
    assert [read.opened for read in after] == [True, False, False]


def test_a_generation_reusing_the_same_inode_number_is_still_a_new_generation(
    tmp_path: Path,
) -> None:
    """The sync unlinks the previous file, so the next `mv` can land on the same number."""

    first = ReplicaGeneration(device=1, inode=7, size=10, mtime_ns=100)
    same_number = ReplicaGeneration(device=1, inode=7, size=11, mtime_ns=100)
    same_size = ReplicaGeneration(device=1, inode=7, size=10, mtime_ns=101)

    assert first != same_number
    assert first != same_size
    assert first == ReplicaGeneration(device=1, inode=7, size=10, mtime_ns=100)


def test_a_different_question_reopens_the_same_generation(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader, key=("2026-08-10",))
    reused = gate.read(loader, key=("2026-08-10",))
    other = gate.read(loader, key=("2026-08-11",))

    assert reused.opened is False
    assert other.opened is True
    assert loader.calls == 2


def test_a_generation_that_moves_under_the_read_is_not_remembered(tmp_path: Path) -> None:
    """The loaders refuse a torn read themselves; the gate simply does not cache one."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    calls = 0

    def loader() -> str:
        nonlocal calls
        calls += 1
        _replace(
            replica,
            payload=b"replaced-mid-read",
            synced_at=SYNCED_AT + timedelta(minutes=calls * 5),
        )
        return "rows"

    first = gate.read(loader)
    second = gate.read(loader)

    assert first.opened is True
    assert second.opened is True
    assert calls == 2


def test_a_generation_that_moved_under_the_read_is_not_remembered_under_the_old_one(
    tmp_path: Path,
) -> None:
    """The stricter half of the rule, with a scripted observer so it can actually be seen.

    `test_a_generation_that_moves_under_the_read_is_not_remembered` uses a real
    replacement, so the next iteration observes the *new* generation and re-reads whether
    or not the torn read was cached -- it cannot tell the two apart. This one scripts the
    observations: the read starts on G1, ends on G2, and the iteration after it is back on
    G1 (a sequence a sync that staged, moved, and rolled back would produce). Caching the
    torn read under G1 would serve G2's answer as G1's; the rule is that a read whose
    generation moved is simply not remembered.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    first = ReplicaGeneration(device=1, inode=7, size=10, mtime_ns=100)
    second = ReplicaGeneration(device=1, inode=8, size=11, mtime_ns=200)
    observations = iter((first, second, first, first))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica, observer=lambda _path: next(observations)
    )
    loader = _Loader()

    torn = gate.read(loader)
    again = gate.read(loader)

    assert torn.opened is True
    assert again.opened is True
    assert loader.calls == 2


def test_a_replica_that_is_not_there_is_never_remembered(tmp_path: Path) -> None:
    gate: ReplicaReadGate[object] = ReplicaReadGate(tmp_path / "absent.duckdb")
    loader = _Loader()

    first = gate.read(loader)
    second = gate.read(loader)

    assert first.generation is None
    assert (first.opened, second.opened) == (True, True)
    assert loader.calls == 2


def test_a_symlink_at_the_replica_name_is_never_a_generation(tmp_path: Path) -> None:
    target = _replica(tmp_path / "real.duckdb")
    link = tmp_path / "rquant_ro.duckdb"
    link.symlink_to(target)

    assert ReplicaGeneration.observe(link) is None


def test_a_point_in_time_read_is_reused_only_after_the_generation_was_written(
    tmp_path: Path,
) -> None:
    """The notifier's question moves on its own; ruling 25's rule for that is exact.

    Every row in the replica was written before the file was, so an answer taken at or
    after the file's own `mtime` already admitted every row a later cutoff would. Taken
    *before* it -- a clock behind the file, which is what a replica synced after this
    iteration started looks like -- it did not, and must not be reused.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb", synced_at=SYNCED_AT)
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    early = gate.read(loader, cutoff=SYNCED_AT - timedelta(seconds=1))
    after_early = gate.read(loader, cutoff=SYNCED_AT + timedelta(seconds=2))
    sealed = gate.read(loader, cutoff=SYNCED_AT + timedelta(seconds=4))

    assert (early.opened, after_early.opened, sealed.opened) == (True, True, False)
    assert loader.calls == 2


def test_calendar_granularity_is_the_caller_s_and_it_lives_in_the_key(
    tmp_path: Path,
) -> None:
    """The gate compares instants and knows nothing about anybody's local zone (SF-3).

    It used to compare `cutoff.date()` as well -- the **UTC** date -- while three
    docstrings called it the local one. That was true by accident for the only caller,
    whose `key` already carried the local date, and would have been a trap for the next.
    A caller whose predicates are written against a date puts the date in `key`, and the
    gate then refuses to answer one date's question with another date's answer.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb", synced_at=SYNCED_AT)
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()
    #: 23:59:50 and 00:01 Asia/Shanghai, which is one UTC day either side of local midnight
    before_midnight = datetime(2026, 8, 11, 15, 59, 50, tzinfo=UTC)
    after_midnight = datetime(2026, 8, 11, 16, 1, tzinfo=UTC)

    gate.read(loader, key=("page", date(2026, 8, 11)), cutoff=before_midnight)
    same_local_day = gate.read(
        loader, key=("page", date(2026, 8, 11)), cutoff=before_midnight
    )
    next_local_day = gate.read(loader, key=("page", date(2026, 8, 12)), cutoff=after_midnight)

    assert (same_local_day.opened, next_local_day.opened) == (False, True)
    assert loader.calls == 2
    #: and without the date in the key the gate happily reuses, because the instant rule
    #: alone is satisfied -- which is exactly why the caller must supply it
    bare: ReplicaReadGate[object] = ReplicaReadGate(replica)
    bare_loader = _Loader()
    bare.read(bare_loader, cutoff=before_midnight)
    assert bare.read(bare_loader, cutoff=after_midnight).opened is False


def test_an_iteration_that_never_asked_reports_that_rather_than_the_last_one(
    tmp_path: Path,
) -> None:
    """Review MF-1: `last_read` answers a different question from "what did this iteration do".

    Four of `capture_reference_slow_batch`'s paths return before the loader is reached, and
    the auction-gap publisher returns outside 09:26-09:30; without a per-iteration scope
    those iterations reported the last real read, and reference-slow's heartbeat would have
    said `replica_opened=true` from 09:25 until the next day.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    #: an iteration that has not asked anything at all
    assert gate.iteration_summary() == (False, 0)

    gate.begin_iteration()
    gate.read(loader)
    opened_summary = gate.iteration_summary()

    gate.begin_iteration()
    gate.read(loader)
    reused_summary = gate.iteration_summary()

    gate.begin_iteration()
    silent_summary = gate.iteration_summary()

    assert opened_summary[0] is True
    assert reused_summary == (False, 0)
    assert silent_summary == (False, 0)
    assert loader.calls == 1


def test_beginning_an_iteration_clears_the_report_and_not_the_cache(
    tmp_path: Path,
) -> None:
    """The cache has to survive the boundary or the gate would open once per iteration."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader)
    gate.begin_iteration()
    assert gate.last_read is None
    reused = gate.read(loader)

    assert reused.opened is False
    assert loader.calls == 1


def test_a_cutoff_that_goes_backwards_is_not_served_from_a_later_answer(
    tmp_path: Path,
) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb", synced_at=SYNCED_AT)
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader, cutoff=SYNCED_AT + timedelta(minutes=2))
    earlier = gate.read(loader, cutoff=SYNCED_AT + timedelta(minutes=1))

    assert earlier.opened is True
    assert loader.calls == 2


def test_a_plain_read_and_a_point_in_time_read_are_not_the_same_answer(
    tmp_path: Path,
) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb", synced_at=SYNCED_AT)
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader)
    with_cutoff = gate.read(loader, cutoff=SYNCED_AT + timedelta(minutes=1))

    assert with_cutoff.opened is True
    assert loader.calls == 2


def test_the_generation_carries_the_point_in_time_the_codebase_already_reads_off_it(
    tmp_path: Path,
) -> None:
    """`auction_gap_candidate_input` takes the replica's `st_mtime_ns` as its `available_at`."""

    replica = _replica(tmp_path / "rquant_ro.duckdb", synced_at=SYNCED_AT)
    generation = ReplicaGeneration.observe(replica)

    assert generation is not None
    assert generation.modified_at == SYNCED_AT


def test_read_bytes_is_either_a_measurement_or_an_honest_absence(tmp_path: Path) -> None:
    """`/proc/self/io` on Linux, nothing on macOS -- and never a fabricated number."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)

    def loader() -> bytes:
        return replica.read_bytes()

    opened = gate.read(loader)

    if Path("/proc/self/io").exists():
        assert opened.read_bytes is not None
        assert opened.read_bytes >= len(b"generation-one")
    else:
        assert opened.read_bytes is None


def test_a_write_lock_is_told_apart_from_a_path_the_engine_does_not_understand() -> None:
    """#255's whole cost was that these two were one sentence."""

    locked = duckdb.Error(
        'IO Error: Could not set lock on file "/x/rquant.duckdb": Conflicting lock is held'
    )
    unknown = duckdb.Error('IO Error: Cannot open database "/dev/fd/x.duckdb" in read-only mode')

    assert is_write_lock_error(locked) is True
    assert is_write_lock_error(unknown) is False


def test_the_pinned_open_says_which_branch_it_took(tmp_path: Path) -> None:
    """Linux pins the inode through the descriptor; macOS re-opens the name."""

    database = tmp_path / "daily.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR)")
    connection.execute("INSERT INTO daily_bar VALUES ('600000.SH')")
    connection.execute("CHECKPOINT")
    connection.close()

    descriptor = os.open(database, os.O_RDONLY)
    try:
        opened, branch = connect_pinned_readonly(database, descriptor)
        try:
            assert branch in {"descriptor", "in_place"}
            assert opened.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 1
            if descriptor_reopen_path(descriptor) is None:
                assert branch == "in_place"
        finally:
            opened.close()
    finally:
        os.close(descriptor)


def test_forgetting_a_generation_makes_the_next_iteration_read_again(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader)
    gate.forget()
    reopened = gate.read(loader)

    assert reopened.opened is True
    assert loader.calls == 2


def test_the_gate_reports_its_last_read_for_the_heartbeat(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    assert gate.last_read is None
    gate.read(loader)
    assert gate.last_read is not None and gate.last_read.opened is True
    gate.read(loader)
    assert gate.last_read is not None and gate.last_read.opened is False


def test_the_gate_refuses_a_relative_path() -> None:
    gate: ReplicaReadGate[object] = ReplicaReadGate(Path("data/rquant_ro.duckdb"))

    assert gate.path.is_absolute()


@pytest.mark.parametrize("directory", ["/proc/self/fd", "/dev/fd"])
def test_the_descriptor_directories_are_the_two_this_platform_family_publishes(
    directory: str,
) -> None:
    from rquant.readside_replica_gate import DESCRIPTOR_DIRECTORIES

    assert directory in DESCRIPTOR_DIRECTORIES
