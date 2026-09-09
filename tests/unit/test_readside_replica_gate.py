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
from datetime import UTC, datetime, timedelta
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


def test_a_point_in_time_read_is_not_reused_across_a_local_date(tmp_path: Path) -> None:
    """`trade_date <= ?` is a date predicate, so a new date is a new question."""

    replica = _replica(tmp_path / "rquant_ro.duckdb", synced_at=SYNCED_AT)
    gate: ReplicaReadGate[object] = ReplicaReadGate(replica)
    loader = _Loader()

    gate.read(loader, cutoff=datetime(2026, 8, 11, 23, 59, 50, tzinfo=UTC))
    same_day = gate.read(loader, cutoff=datetime(2026, 8, 11, 23, 59, 59, tzinfo=UTC))
    next_day = gate.read(loader, cutoff=datetime(2026, 8, 12, 0, 0, 1, tzinfo=UTC))

    assert (same_day.opened, next_day.opened) == (False, True)
    assert loader.calls == 2


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
