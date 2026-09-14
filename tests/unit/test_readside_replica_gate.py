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
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.readside_replica_gate import (
    DEFAULT_NO_READ_WINDOW,
    NOTIFIER_PAGE_PROJECTION_PROFILE,
    UNLIMITED_READ_PROFILE,
    ReplicaGeneration,
    ReplicaReadGate,
    ReplicaReadProfile,
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


def test_a_read_that_raised_part_way_still_counts_as_an_open(tmp_path: Path) -> None:
    """Review SF-7: a loader that fails did open the database, and it did read bytes.

    This is the auction-gap publisher's degraded branch: the replica is replaced under the
    read, the loader's own identity check refuses, and the iteration reports
    `auction_gap_input_unavailable`. Saying `(False, 0)` there would understate the cost
    this package exists to count -- and would hide exactly the iteration an operator wants
    to see, the one that paid for a read and got nothing.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)

    def failing_loader() -> object:
        replica.read_bytes()
        raise RuntimeError("daily snapshot changed while reading")

    gate.begin_iteration()
    with pytest.raises(RuntimeError, match="changed while reading"):
        gate.read(failing_loader)

    opened, read_bytes = gate.iteration_summary()
    assert opened is True
    assert read_bytes is None or read_bytes >= len(b"generation-one")
    assert gate.last_read is not None and gate.last_read.value is None


def test_a_read_that_raised_is_not_remembered_for_the_next_iteration(
    tmp_path: Path,
) -> None:
    """Reporting the open is not the same as trusting what it returned."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()
    attempts = {"count": 0}

    def sometimes_failing() -> object:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("torn")
        return loader()

    with pytest.raises(RuntimeError, match="torn"):
        gate.read(sometimes_failing)
    recovered = gate.read(sometimes_failing)

    assert recovered.opened is True
    assert loader.calls == 1


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


# ---------------------------------------------------------------------------------------
# #268: how often a role may open a *new* generation
# ---------------------------------------------------------------------------------------


class _Clock:
    """A clock the test moves, so the floor is asserted rather than waited out."""

    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


def test_three_consecutive_replacements_inside_the_floor_cost_one_read(tmp_path: Path) -> None:
    """The shape of a trading day: a new generation every five minutes, all day.

    Package Q's gate opens the database once per generation, which on a trading day is
    once every five minutes per role -- and on 2026-09-14 four of those, the replica `cp`,
    the production monitor's startup scan and the 09:30 backup landed together and the
    monitor did not poll for ten minutes after the open (#268).

    Three replacements inside one floor cost one read. On the production cadence that is
    the notifier's fifteen-minute floor skipping two generations out of every three; the
    third, which falls on the floor's own edge, is the read that starts the next one
    (`test_a_generation_arriving_after_the_floor_is_read`).
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(minutes=15)),
        clock=clock,
    )
    loader = _Loader()

    first = gate.read(loader)
    reads = []
    for generation in range(3):
        clock.advance(timedelta(minutes=4))
        _replace(
            replica,
            payload=f"generation-{generation + 2}".encode(),
            synced_at=SYNCED_AT + timedelta(minutes=4 * (generation + 1)),
        )
        reads.append(gate.read(loader))

    assert loader.calls == 1
    assert first.opened is True
    assert first.skipped_by_floor is False
    assert [read.opened for read in reads] == [False, False, False]
    assert [read.skipped_by_floor for read in reads] == [True, True, True]
    #: the answer that stands is the one the first read took
    assert [read.value for read in reads] == ["rows"] * 3


def test_a_generation_arriving_after_the_floor_is_read(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(minutes=15)),
        clock=clock,
    )
    loader = _Loader()

    gate.read(loader)
    clock.advance(timedelta(minutes=16))
    _replace(replica, payload=b"generation-two", synced_at=SYNCED_AT + timedelta(minutes=15))
    second = gate.read(loader)

    assert loader.calls == 2
    assert second.opened is True
    assert second.skipped_by_floor is False


def test_a_role_with_no_answer_yet_is_never_held_by_the_floor(tmp_path: Path) -> None:
    """Suppressing a re-read is the point; suppressing the role is not.

    A cold start inside the no-read window has nothing to publish at all, and holding it
    would invent a second, longer outage on top of the one this is about.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    #: 01:30 UTC is 09:30 in the market clock, the middle of the window
    clock = _Clock(datetime(2026, 9, 14, 1, 30, tzinfo=UTC))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=NOTIFIER_PAGE_PROJECTION_PROFILE,
        clock=clock,
    )
    loader = _Loader()

    first = gate.read(loader)

    assert loader.calls == 1
    assert first.opened is True
    assert first.skipped_by_floor is False


def test_the_no_read_window_holds_a_newer_generation_and_lets_go_at_its_end(
    tmp_path: Path,
) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(datetime(2026, 9, 14, 1, 10, tzinfo=UTC))  # 09:10 market time
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(no_read_window=DEFAULT_NO_READ_WINDOW),
        clock=clock,
    )
    loader = _Loader()

    gate.read(loader)
    clock.now = datetime(2026, 9, 14, 1, 25, tzinfo=UTC)  # 09:25, inside
    _replace(replica, payload=b"generation-two", synced_at=SYNCED_AT + timedelta(minutes=5))
    inside = gate.read(loader)
    clock.now = datetime(2026, 9, 14, 1, 40, tzinfo=UTC)  # 09:40, the far edge
    after = gate.read(loader)

    assert inside.opened is False
    assert inside.skipped_by_floor is True
    assert after.opened is True
    assert after.skipped_by_floor is False
    assert loader.calls == 2


def test_a_different_question_is_never_answered_from_the_floor(tmp_path: Path) -> None:
    """`key` is what makes a question a different one, and a different one always opens."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(hours=1)),
        clock=clock,
    )
    loader = _Loader()

    gate.read(loader, key=("session", date(2026, 9, 14)))
    clock.advance(timedelta(minutes=1))
    _replace(replica, payload=b"generation-two", synced_at=SYNCED_AT + timedelta(minutes=5))
    other = gate.read(loader, key=("session", date(2026, 9, 15)))

    assert loader.calls == 2
    assert other.opened is True
    assert other.skipped_by_floor is False


def test_a_clock_that_went_backwards_does_not_hold_an_answer_for_ever(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(hours=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(minutes=15)),
        clock=clock,
    )
    loader = _Loader()

    gate.read(loader)
    #: NTP stepping a host that has just come up
    clock.now = SYNCED_AT
    _replace(replica, payload=b"generation-two", synced_at=SYNCED_AT + timedelta(minutes=5))
    second = gate.read(loader)

    assert loader.calls == 2
    assert second.opened is True


def test_a_failed_read_inside_the_floor_is_retried_rather_than_held(tmp_path: Path) -> None:
    """A loader that raised leaves no answer, so there is nothing for the floor to keep."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(hours=1)),
        clock=clock,
    )
    calls = {"count": 0}

    def failing() -> object:
        calls["count"] += 1
        raise RuntimeError("the loader could not finish")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            gate.read(failing)
        clock.advance(timedelta(seconds=2))

    assert calls["count"] == 3


def test_the_iteration_report_separates_a_skip_from_a_recognised_generation(
    tmp_path: Path,
) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(hours=1)),
        clock=clock,
    )
    loader = _Loader()

    gate.begin_iteration()
    assert gate.iteration_summary() == (False, 0)
    assert gate.iteration_skipped_by_floor() is False

    gate.begin_iteration()
    gate.read(loader)
    assert gate.iteration_summary()[0] is True
    assert gate.iteration_skipped_by_floor() is False

    gate.begin_iteration()
    gate.read(loader)
    assert gate.iteration_summary() == (False, 0)
    assert gate.iteration_skipped_by_floor() is False, "same generation is not a skip"

    _replace(replica, payload=b"generation-two", synced_at=SYNCED_AT + timedelta(minutes=5))
    gate.begin_iteration()
    gate.read(loader)
    assert gate.iteration_summary() == (False, 0)
    assert gate.iteration_skipped_by_floor() is True


def test_a_gate_built_without_a_profile_is_where_package_q_left_it(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader)
    _replace(replica, payload=b"generation-two", synced_at=SYNCED_AT + timedelta(minutes=5))
    second = gate.read(loader)

    assert gate.profile is UNLIMITED_READ_PROFILE
    assert loader.calls == 2
    assert second.opened is True
    assert second.skipped_by_floor is False


@pytest.mark.parametrize(
    "arguments",
    [
        {"min_reread_interval": timedelta(seconds=-1)},
        {"no_read_window": (time(9, 40), time(9, 20))},
        {"no_read_window": (time(9, 20), time(9, 20))},
        {"no_read_window": (time(9, 20, tzinfo=UTC), time(9, 40))},
    ],
)
def test_an_unusable_profile_is_refused_where_it_is_written(arguments: dict) -> None:
    with pytest.raises((ValueError, TypeError)):
        ReplicaReadProfile(**arguments)


def test_the_window_is_read_in_the_market_clock_not_the_hosts() -> None:
    """The same clock `may_fetch_market_minute` is decided in, imported rather than copied."""

    from rquant.runtime_market_session import MARKET_TIMEZONE

    profile = ReplicaReadProfile(no_read_window=DEFAULT_NO_READ_WINDOW)
    inside = datetime(2026, 9, 14, 9, 30, tzinfo=MARKET_TIMEZONE)
    assert profile.suspends_reads_at(inside)
    assert profile.suspends_reads_at(inside.astimezone(UTC))
    #: 09:30 UTC is 17:30 in the market clock, which is nowhere near the window
    assert not profile.suspends_reads_at(datetime(2026, 9, 14, 9, 30, tzinfo=UTC))


def test_a_vanished_replica_is_a_failure_and_never_a_floor_skip(tmp_path: Path) -> None:
    """Review SF-1: `current is None` is not "a newer generation this role may skip".

    `ReplicaGeneration.observe` answers `None` for a name with no regular file behind it --
    deleted, turned into a symlink, turned into a directory. Before this guard the floor
    branch fired there too, so a role with a cached answer would serve it for the whole
    interval and report `replica_skipped_by_floor=true` -- which DEPLOY.md tells the owner
    to read as the fix working. A replica that is gone has to surface as the failure
    package Q designed: the loader is called, it says so, and the round fails.
    """

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(minutes=15)),
        clock=clock,
    )
    calls = {"count": 0}

    def loader() -> object:
        calls["count"] += 1
        if not replica.exists():
            raise RuntimeError("the replica is gone")
        return "answer"

    gate.read(loader)
    replica.unlink()
    clock.advance(timedelta(seconds=2))

    with pytest.raises(RuntimeError, match="the replica is gone"):
        gate.read(loader)

    assert calls["count"] == 2, "the loader must be asked, not answered from the cache"
    assert gate.last_read is not None
    assert gate.last_read.skipped_by_floor is False
    assert gate.iteration_skipped_by_floor() is False


def test_a_replica_replaced_by_a_directory_is_refused_the_same_way(tmp_path: Path) -> None:
    """The other shape `observe()` answers `None` for, so the guard is not about `unlink`."""

    replica = _replica(tmp_path / "rquant_ro.duckdb")
    clock = _Clock(SYNCED_AT + timedelta(minutes=1))
    gate: ReplicaReadGate[object] = ReplicaReadGate(
        replica,
        profile=ReplicaReadProfile(min_reread_interval=timedelta(minutes=15)),
        clock=clock,
    )
    loader = _Loader()

    gate.read(loader)
    replica.unlink()
    replica.mkdir()
    clock.advance(timedelta(seconds=2))
    gate.read(loader)

    assert loader.calls == 2
    assert gate.last_read is not None
    assert gate.last_read.skipped_by_floor is False
