from __future__ import annotations

import ctypes
import fcntl
import hashlib
import os
import sqlite3
import struct
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest

import rquant.artifact_retention as artifact_retention_module
import rquant.lab_artifact_catalog as catalog_module
from rquant.lab_artifact_catalog import (
    LabArtifactCatalogIntegrityError,
    LabArtifactCatalogRegistrar,
    LabArtifactCatalogRunResult,
    LabArtifactChildDirectory,
    LabArtifactDirectoryFrontier,
    LabArtifactDirectoryScanPage,
    _parse_directory_entry_block,
    _read_directory_entry_chunk,
)
from rquant.lab_artifact_catalog_runtime import (
    LabArtifactCatalogAlreadyRunningError,
    LabArtifactCatalogRuntime,
    LabArtifactDiscoveryQueue,
)

NOW = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)


def _linux_dirent(name: str, *, inode: int, offset: int) -> bytes:
    encoded = name.encode("ascii")
    record_length = (19 + len(encoded) + 1 + 7) // 8 * 8
    return (
        struct.pack("=QQHB", inode, offset, record_length, 4)
        + encoded
        + b"\0"
        + b"\0" * (record_length - 20 - len(encoded))
    )


def test_linux_directory_entry_block_parser_matches_production_abi() -> None:
    payload = b"".join(
        (
            _linux_dirent(".", inode=1, offset=1),
            _linux_dirent("..", inode=2, offset=2),
            _linux_dirent(
                "11111111-1111-4111-8111-111111111111",
                inode=3,
                offset=3,
            ),
        )
    )

    assert _parse_directory_entry_block(payload, platform="linux") == (
        "11111111-1111-4111-8111-111111111111",
    )


def test_linux_directory_chunk_calls_getdirentries64_and_resumes_bounded_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = [
        _linux_dirent("11111111-1111-4111-8111-111111111111", inode=3, offset=17),
        _linux_dirent("22222222-2222-4222-8222-222222222222", inode=4, offset=29),
        b"",
    ]
    cursor = 0
    requested_offsets: list[int] = []
    calls: list[tuple[int, int]] = []

    class FakeGetDirEntries64:
        argtypes: object = None
        restype: object = None

        def __call__(
            self,
            descriptor: int,
            buffer: object,
            chunk_bytes: int,
            _base_offset: object,
        ) -> int:
            nonlocal cursor
            payload = payloads[len(calls)]
            calls.append((descriptor, chunk_bytes))
            if payload:
                ctypes.memmove(buffer, payload, len(payload))
                cursor = 17 if len(calls) == 1 else 29
            return len(payload)

    function = FakeGetDirEntries64()

    class FakeLibc:
        getdirentries64 = function

        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"unexpected libc symbol: {name}")

    def fake_lseek(_descriptor: int, offset: int, whence: int) -> int:
        nonlocal cursor
        if whence == os.SEEK_SET:
            requested_offsets.append(offset)
            cursor = offset
        elif whence != os.SEEK_CUR:
            raise AssertionError(f"unexpected whence: {whence}")
        return cursor

    monkeypatch.setattr(catalog_module.sys, "platform", "linux")
    monkeypatch.setattr(catalog_module.ctypes, "CDLL", lambda *_args, **_kwargs: FakeLibc())
    monkeypatch.setattr(catalog_module.os, "lseek", fake_lseek)

    first = _read_directory_entry_chunk(91, 0, chunk_bytes=64)
    second = _read_directory_entry_chunk(91, first[1], chunk_bytes=64)
    exhausted = _read_directory_entry_chunk(91, second[1], chunk_bytes=64)

    assert first == (("11111111-1111-4111-8111-111111111111",), 17, False)
    assert second == (("22222222-2222-4222-8222-222222222222",), 29, False)
    assert exhausted == ((), 29, True)
    assert requested_offsets == [0, 17, 29]
    assert calls == [(91, 64), (91, 64), (91, 64)]
    assert function.argtypes is not None
    assert function.restype is ctypes.c_ssize_t


class _RegistrarStub:
    def __init__(self, entries: list[str]) -> None:
        self.entries = entries
        self.discovery_calls = 0
        self.discovery_limits: list[int] = []
        self.registered_pages: list[tuple[str, ...]] = []
        self.fail_registration = False

    def scan_directory_page(
        self,
        frontier: LabArtifactDirectoryFrontier,
        *,
        max_entries: int,
    ) -> LabArtifactDirectoryScanPage:
        self.discovery_calls += 1
        self.discovery_limits.append(max_entries)
        start = frontier.directory_offset
        selected = tuple(self.entries[start : start + max_entries])
        next_offset = start + len(selected)
        return LabArtifactDirectoryScanPage(
            frontier_sequence=frontier.frontier_sequence,
            frontier_revision=frontier.revision,
            directory_device=frontier.directory_device or 11,
            directory_inode=frontier.directory_inode or 22,
            directory_offset=next_offset,
            buffered_entry_names=(),
            exhausted=next_offset >= len(self.entries),
            scanned_entries=len(selected),
            child_directories=(),
            bundle_paths=selected,
        )

    def run_once(self, *, bundle_paths: tuple[str, ...]) -> LabArtifactCatalogRunResult:
        self.registered_pages.append(bundle_paths)
        if self.fail_registration:
            raise LabArtifactCatalogIntegrityError("injected registration failure")
        return LabArtifactCatalogRunResult(
            status="completed",
            completed_at=NOW,
            scanned_bundles=len(bundle_paths),
            registered_objects=len(bundle_paths),
            registered_copies=len(bundle_paths),
            registered_references=4 * len(bundle_paths),
            unchanged_bundles=0,
            total_bytes=10 * len(bundle_paths),
            content_hashes=tuple(
                hashlib.sha256(path.encode("ascii")).hexdigest() for path in bundle_paths
            ),
            has_more=False,
            next_cursor=None,
        )


class _WideRegistrarStub(_RegistrarStub):
    def __init__(self, child_count: int) -> None:
        super().__init__([])
        self.child_count = child_count

    def scan_directory_page(
        self,
        frontier: LabArtifactDirectoryFrontier,
        *,
        max_entries: int,
    ) -> LabArtifactDirectoryScanPage:
        self.discovery_calls += 1
        self.discovery_limits.append(max_entries)
        if frontier.relative_directory == "jobs":
            consumed = min(self.child_count, max_entries)
            return LabArtifactDirectoryScanPage(
                frontier_sequence=frontier.frontier_sequence,
                frontier_revision=frontier.revision,
                directory_device=11,
                directory_inode=22,
                directory_offset=consumed,
                buffered_entry_names=(),
                exhausted=consumed == self.child_count,
                scanned_entries=consumed,
                child_directories=tuple(
                    LabArtifactChildDirectory(
                        relative_directory=f"jobs/job-{index}",
                        directory_kind="shards",
                    )
                    for index in range(consumed)
                ),
                bundle_paths=(),
            )
        return LabArtifactDirectoryScanPage(
            frontier_sequence=frontier.frontier_sequence,
            frontier_revision=frontier.revision,
            directory_device=11,
            directory_inode=frontier.frontier_sequence + 100,
            directory_offset=1,
            buffered_entry_names=(),
            exhausted=True,
            scanned_entries=1,
            child_directories=(),
            bundle_paths=(f"{frontier.relative_directory}/bundle",),
        )


class _RoundRobinRegistrarStub(_RegistrarStub):
    def __init__(self, *, pages_per_directory: int, fail_first: bool = False) -> None:
        super().__init__([])
        self.pages_per_directory = pages_per_directory
        self.fail_first = fail_first
        self.frontier_calls: list[tuple[str, int]] = []

    def scan_directory_page(
        self,
        frontier: LabArtifactDirectoryFrontier,
        *,
        max_entries: int,
    ) -> LabArtifactDirectoryScanPage:
        assert max_entries == 1
        self.frontier_calls.append((frontier.relative_directory, frontier.directory_offset))
        if self.fail_first:
            self.fail_first = False
            raise LabArtifactCatalogIntegrityError("injected frontier scan crash")
        next_offset = frontier.directory_offset + 1
        return LabArtifactDirectoryScanPage(
            frontier_sequence=frontier.frontier_sequence,
            frontier_revision=frontier.revision,
            directory_device=11,
            directory_inode=frontier.frontier_sequence + 100,
            directory_offset=next_offset,
            buffered_entry_names=(),
            exhausted=next_offset >= self.pages_per_directory,
            scanned_entries=1,
            child_directories=(),
            bundle_paths=(),
        )


class _PartialRegistrationRegistrarStub(_RegistrarStub):
    def run_once(self, *, bundle_paths: tuple[str, ...]) -> LabArtifactCatalogRunResult:
        self.registered_pages.append(bundle_paths)
        selected = bundle_paths[:1]
        has_more = len(bundle_paths) > 1
        return LabArtifactCatalogRunResult(
            status="partial" if has_more else "completed",
            completed_at=NOW,
            scanned_bundles=len(selected),
            registered_objects=len(selected),
            registered_copies=len(selected),
            registered_references=4 * len(selected),
            unchanged_bundles=0,
            total_bytes=10 * len(selected),
            content_hashes=tuple(
                hashlib.sha256(path.encode("ascii")).hexdigest() for path in selected
            ),
            has_more=has_more,
            next_cursor=bundle_paths[1] if has_more else None,
        )


def _seed_active_frontiers(
    queue: LabArtifactDiscoveryQueue,
    relative_directories: tuple[str, ...],
) -> None:
    with queue._connect(read_only=False) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            UPDATE artifact_catalog_discovery_metadata
            SET scan_generation = 1
            WHERE singleton = 1
            """
        )
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery_frontier(
                scan_generation, relative_directory, directory_kind, status
            ) VALUES (1, ?, 'shards', 'active')
            """,
            ((relative_directory,) for relative_directory in relative_directories),
        )
        connection.commit()


def _append_active_frontier(
    queue: LabArtifactDiscoveryQueue,
    relative_directory: str,
) -> None:
    with queue._connect(read_only=False) as connection:
        connection.execute(
            """
            INSERT INTO artifact_catalog_discovery_frontier(
                scan_generation, relative_directory, directory_kind, status
            ) VALUES (1, ?, 'shards', 'active')
            """,
            (relative_directory,),
        )


def _runtime(
    tmp_path: Path,
    registrar: _RegistrarStub,
    *,
    max_bundles: int = 1,
    max_discovery_entries: int = 1,
    max_directories_per_step: int = 1,
    max_discovery_seconds: float = 1.0,
    monotonic: Callable[[], float] | None = None,
) -> LabArtifactCatalogRuntime:
    return LabArtifactCatalogRuntime(
        registrar=registrar,  # type: ignore[arg-type]
        discovery_queue=LabArtifactDiscoveryQueue(
            tmp_path / "discovery.sqlite3",
            managed_trust_root=tmp_path,
        ),
        max_bundles=max_bundles,
        max_discovery_entries=max_discovery_entries,
        max_directories_per_step=max_directories_per_step,
        max_discovery_seconds=max_discovery_seconds,
        lock_path=tmp_path / "catalog.lock",
        clock=lambda: NOW,
        monotonic=monotonic,
    )


@pytest.mark.parametrize(
    "extra_columns",
    (
        "",
        ", frontier_cursor INTEGER NOT NULL DEFAULT 0 CHECK(frontier_cursor >= 0)",
        (
            ", frontier_cursor INTEGER NOT NULL DEFAULT 0 CHECK(frontier_cursor >= 0)"
            ", frontier_round_ceiling INTEGER NOT NULL DEFAULT 0 "
            "CHECK(frontier_round_ceiling >= 0)"
        ),
    ),
    ids=("no-cursor", "cursor-only", "complete"),
)
def test_discovery_queue_migrates_all_legacy_metadata_generations_idempotently(
    tmp_path: Path,
    extra_columns: str,
) -> None:
    path = tmp_path / "discovery.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            f"""
            CREATE TABLE artifact_catalog_discovery_metadata (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL CHECK(revision >= 0),
                scan_generation INTEGER NOT NULL CHECK(scan_generation >= 0),
                last_scan_at TEXT
                {extra_columns}
            );
            INSERT INTO artifact_catalog_discovery_metadata(
                singleton, revision, scan_generation, last_scan_at
            ) VALUES (1, 9, 4, '2026-08-01T03:00:00+00:00');
            """
        )
    path.chmod(0o600)

    queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)

    with queue._connect(read_only=True) as connection:
        columns = tuple(
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(artifact_catalog_discovery_metadata)"
            ).fetchall()
        )
        round_ceiling = int(
            connection.execute(
                """
                SELECT frontier_round_ceiling
                FROM artifact_catalog_discovery_metadata
                WHERE singleton = 1
                """
            ).fetchone()[0]
        )
        metadata = tuple(
            connection.execute(
                """
                SELECT revision, scan_generation, last_scan_at
                FROM artifact_catalog_discovery_metadata WHERE singleton = 1
                """
            ).fetchone()
        )

    assert "frontier_cursor" in columns
    assert "frontier_round_ceiling" in columns
    assert round_ceiling == 0
    assert metadata == (9, 4, "2026-08-01T03:00:00+00:00")


def test_discovery_metadata_migration_rolls_back_fault_and_restarts_without_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "discovery.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE artifact_catalog_discovery_metadata (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL CHECK(revision >= 0),
                scan_generation INTEGER NOT NULL CHECK(scan_generation >= 0),
                last_scan_at TEXT
            );
            INSERT INTO artifact_catalog_discovery_metadata(
                singleton, revision, scan_generation, last_scan_at
            ) VALUES (1, 11, 5, NULL);
            """
        )
    path.chmod(0o600)
    original = LabArtifactDiscoveryQueue._after_discovery_metadata_base_initialized

    def fail_after_base(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("fault after base schema")

    monkeypatch.setattr(
        LabArtifactDiscoveryQueue,
        "_after_discovery_metadata_base_initialized",
        staticmethod(fail_after_base),
    )
    with pytest.raises(RuntimeError, match="fault after base"):
        LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    monkeypatch.setattr(
        LabArtifactDiscoveryQueue,
        "_after_discovery_metadata_base_initialized",
        staticmethod(original),
    )

    queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)

    assert queue.read_state().revision == 11
    with queue._connect(read_only=True) as connection:
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(artifact_catalog_discovery_metadata)"
            ).fetchall()
        }
    assert {"frontier_cursor", "frontier_round_ceiling"} <= columns


def test_run_step_scans_no_more_than_the_configured_directory_entry_budget(
    tmp_path: Path,
) -> None:
    registrar = _RegistrarStub(["jobs/a", "jobs/b", "jobs/c"])
    runtime = _runtime(tmp_path, registrar, max_discovery_entries=1)

    first = runtime.run_step()
    restarted = _runtime(tmp_path, registrar, max_discovery_entries=1).run_step()

    assert first.scanned_directory_entries == 1
    assert restarted.scanned_directory_entries == 1
    assert registrar.discovery_limits == [1, 1]
    assert first.processed_paths == ("jobs/a",)
    assert restarted.processed_paths == ("jobs/b",)


def test_run_step_advances_multiple_wide_frontiers_with_global_budgets(
    tmp_path: Path,
) -> None:
    registrar = _WideRegistrarStub(child_count=4)
    step = _runtime(
        tmp_path,
        registrar,
        max_bundles=10,
        max_discovery_entries=6,
        max_directories_per_step=3,
    ).run_step()

    assert step.scanned_directory_entries == 6
    assert step.scanned_directories == 3
    assert step.discovered_bundles == 2
    assert len(step.processed_paths) == 2
    assert registrar.discovery_limits == [6, 2, 1]


def test_run_step_stops_multi_frontier_scan_at_monotonic_deadline(
    tmp_path: Path,
) -> None:
    registrar = _WideRegistrarStub(child_count=2)
    observed = iter((10.0, 12.0))
    step = _runtime(
        tmp_path,
        registrar,
        max_bundles=10,
        max_discovery_entries=10,
        max_directories_per_step=10,
        max_discovery_seconds=1.0,
        monotonic=lambda: next(observed),
    ).run_step()

    assert step.scanned_directories == 1
    assert step.scanned_directory_entries == 2
    assert registrar.discovery_limits == [10]


def test_wide_frontiers_round_robin_across_steps_and_restarts(tmp_path: Path) -> None:
    path = tmp_path / "discovery.sqlite3"
    queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    directories = ("jobs/a/shards", "jobs/b/shards", "jobs/c/shards")
    _seed_active_frontiers(queue, directories)
    registrar = _RoundRobinRegistrarStub(pages_per_directory=3)

    for _step in range(6):
        queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
        result = queue.scan_step(
            registrar,  # type: ignore[arg-type]
            max_entries=1,
            max_directories=1,
            deadline=1.0,
            monotonic=lambda: 0.0,
            discovered_at=NOW,
        )
        assert result.scanned_directory_entries == 1
        assert result.scanned_directories == 1

    assert registrar.frontier_calls == [
        ("jobs/a/shards", 0),
        ("jobs/b/shards", 0),
        ("jobs/c/shards", 0),
        ("jobs/a/shards", 1),
        ("jobs/b/shards", 1),
        ("jobs/c/shards", 1),
    ]


def test_round_robin_freezes_each_round_while_frontiers_are_continuously_appended(
    tmp_path: Path,
) -> None:
    path = tmp_path / "discovery.sqlite3"
    queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    original = ("jobs/a/shards", "jobs/b/shards")
    _seed_active_frontiers(queue, original)
    registrar = _RoundRobinRegistrarStub(pages_per_directory=100)

    for index in range(6):
        queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
        if index:
            _append_active_frontier(queue, f"jobs/new-{index}/shards")
        queue.scan_step(
            registrar,  # type: ignore[arg-type]
            max_entries=1,
            max_directories=1,
            deadline=1.0,
            monotonic=lambda: 0.0,
            discovered_at=NOW,
        )

    assert registrar.frontier_calls[:4] == [
        ("jobs/a/shards", 0),
        ("jobs/b/shards", 0),
        ("jobs/a/shards", 1),
        ("jobs/b/shards", 1),
    ]


def test_round_robin_advances_a_frozen_round_when_its_remaining_frontier_is_deleted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "discovery.sqlite3"
    queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    _seed_active_frontiers(queue, ("jobs/a/shards", "jobs/b/shards"))
    registrar = _RoundRobinRegistrarStub(pages_per_directory=100)

    queue.scan_step(
        registrar,  # type: ignore[arg-type]
        max_entries=1,
        max_directories=1,
        deadline=1.0,
        monotonic=lambda: 0.0,
        discovered_at=NOW,
    )
    with queue._connect(read_only=False) as connection:
        connection.execute(
            """
            DELETE FROM artifact_catalog_discovery_frontier
            WHERE relative_directory = 'jobs/b/shards'
            """
        )
    _append_active_frontier(queue, "jobs/new/shards")

    restarted = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    restarted.scan_step(
        registrar,  # type: ignore[arg-type]
        max_entries=1,
        max_directories=1,
        deadline=1.0,
        monotonic=lambda: 0.0,
        discovered_at=NOW,
    )

    assert registrar.frontier_calls == [
        ("jobs/a/shards", 0),
        ("jobs/a/shards", 1),
    ]


def test_round_robin_cursor_survives_scan_crash_without_losing_frontier(
    tmp_path: Path,
) -> None:
    path = tmp_path / "discovery.sqlite3"
    queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
    directories = ("jobs/a/shards", "jobs/b/shards", "jobs/c/shards")
    _seed_active_frontiers(queue, directories)
    registrar = _RoundRobinRegistrarStub(pages_per_directory=2, fail_first=True)

    with pytest.raises(LabArtifactCatalogIntegrityError, match="injected frontier scan crash"):
        queue.scan_step(
            registrar,  # type: ignore[arg-type]
            max_entries=1,
            max_directories=1,
            deadline=1.0,
            monotonic=lambda: 0.0,
            discovered_at=NOW,
        )

    for _step in range(3):
        queue = LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
        queue.scan_step(
            registrar,  # type: ignore[arg-type]
            max_entries=1,
            max_directories=1,
            deadline=1.0,
            monotonic=lambda: 0.0,
            discovered_at=NOW,
        )

    assert registrar.frontier_calls == [
        ("jobs/a/shards", 0),
        ("jobs/b/shards", 0),
        ("jobs/c/shards", 0),
        ("jobs/a/shards", 0),
    ]


def test_deleted_frontier_is_audited_once_and_does_not_block_later_root_rescan(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    jobs = artifact_root / "jobs"
    jobs.mkdir(parents=True, mode=0o700)
    artifact_root.chmod(0o700)
    jobs.chmod(0o700)
    first = jobs / "00000000-0000-4000-8000-000000000001" / "shards"
    second = jobs / "00000000-0000-4000-8000-000000000002" / "shards"
    first.mkdir(parents=True, mode=0o700)
    second.mkdir(parents=True, mode=0o700)
    queue_path = tmp_path / "state" / "discovery.sqlite3"
    queue_path.parent.mkdir(mode=0o700)
    queue = LabArtifactDiscoveryQueue(queue_path, managed_trust_root=tmp_path)
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=artifact_root,
        reference_store=object(),  # type: ignore[arg-type]
        owner_resolver=lambda _manifest: None,  # type: ignore[arg-type]
        location_id="test",
        failure_domain="test",
    )

    queue.scan_step(
        registrar,
        max_entries=100,
        max_directories=1,
        deadline=1.0,
        monotonic=lambda: 0.0,
        discovered_at=NOW,
    )
    first.rmdir()
    first.parent.rmdir()
    newcomer = jobs / "00000000-0000-4000-8000-000000000003" / "shards"
    newcomer.mkdir(parents=True, mode=0o700)

    missing_seen = False
    for _attempt in range(2):
        try:
            queue.scan_step(
                registrar,
                max_entries=100,
                max_directories=1,
                deadline=1.0,
                monotonic=lambda: 0.0,
                discovered_at=NOW,
            )
        except LabArtifactCatalogIntegrityError as exc:
            assert "missing" in str(exc)
            missing_seen = True
            break
    assert missing_seen is True

    for _attempt in range(8):
        queue = LabArtifactDiscoveryQueue(queue_path, managed_trust_root=tmp_path)
        queue.scan_step(
            registrar,
            max_entries=100,
            max_directories=1,
            deadline=1.0,
            monotonic=lambda: 0.0,
            discovered_at=NOW,
        )
        with queue._connect(read_only=True) as connection:
            newcomer_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM artifact_catalog_discovery_frontier
                    WHERE relative_directory = ?
                    """,
                    (newcomer.relative_to(artifact_root).as_posix(),),
                ).fetchone()[0]
            )
        if newcomer_count:
            break

    assert newcomer_count == 1
    with queue._connect(read_only=True) as connection:
        failures = connection.execute(
            """
            SELECT relative_directory, failure_reason
            FROM artifact_catalog_discovery_frontier_failure
            ORDER BY failed_at, frontier_sequence
            """
        ).fetchall()
    assert [(row[0], row[1]) for row in failures] == [
        (first.relative_to(artifact_root).as_posix(), "artifact ancestor is missing")
    ]


def test_directory_frontier_and_pending_page_survive_restart(tmp_path: Path) -> None:
    registrar = _RegistrarStub(["jobs/a", "jobs/b", "jobs/c"])
    first = _runtime(
        tmp_path,
        registrar,
        max_bundles=1,
        max_discovery_entries=2,
    ).run_step()
    restarted = _runtime(
        tmp_path,
        registrar,
        max_bundles=1,
        max_discovery_entries=1,
    ).run_step()

    assert first.discovered_bundles == 2
    assert first.processed_paths == ("jobs/a",)
    assert first.pending_bundles == 1
    assert restarted.processed_paths == ("jobs/b",)
    assert restarted.discovered_bundles == 1
    assert restarted.pending_bundles == 1
    assert registrar.registered_pages[:2] == [("jobs/a",), ("jobs/b",)]


def test_completed_frontier_generations_are_pruned_to_current_and_recovery_generation(
    tmp_path: Path,
) -> None:
    registrar = _RegistrarStub([])
    runtime = _runtime(tmp_path, registrar)

    for _generation in range(8):
        runtime.run_step()

    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    with queue._connect(read_only=True) as connection:
        current_generation = int(
            connection.execute(
                """
                SELECT scan_generation
                FROM artifact_catalog_discovery_metadata
                WHERE singleton = 1
                """
            ).fetchone()[0]
        )
        generations = tuple(
            int(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT scan_generation
                FROM artifact_catalog_discovery_frontier
                ORDER BY scan_generation
                """
            ).fetchall()
        )
        frontier_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM artifact_catalog_discovery_frontier"
            ).fetchone()[0]
        )

    assert generations == (current_generation - 1, current_generation)
    assert frontier_count <= 2


@pytest.mark.parametrize(
    ("index_name", "sql", "parameters", "table"),
    [
        (
            "artifact_catalog_active_frontier_round_robin_idx",
            """
            SELECT *
            FROM artifact_catalog_discovery_frontier
            WHERE scan_generation = ? AND status = 'active'
              AND frontier_sequence > ?
              AND frontier_sequence <= ?
            ORDER BY frontier_sequence
            LIMIT 1
            """,
            (1, 2_500, 10_000),
            "artifact_catalog_discovery_frontier",
        ),
        (
            "artifact_catalog_active_frontier_round_robin_idx",
            """
            SELECT MAX(frontier_sequence)
            FROM artifact_catalog_discovery_frontier
            WHERE scan_generation = ? AND status = 'active'
            """,
            (1,),
            "artifact_catalog_discovery_frontier",
        ),
        (
            "artifact_catalog_pending_queue_idx",
            """
            SELECT relative_path
            FROM artifact_catalog_discovery
            WHERE status = 'pending'
            ORDER BY discovery_sequence
            LIMIT ?
            """,
            (10,),
            "artifact_catalog_discovery",
        ),
        (
            "artifact_catalog_pending_queue_idx",
            """
            SELECT COUNT(*)
            FROM artifact_catalog_discovery
            WHERE status = 'pending'
            """,
            (),
            "artifact_catalog_discovery",
        ),
        (
            "artifact_catalog_completed_frontier_generation_idx",
            """
            DELETE FROM artifact_catalog_discovery_frontier
            WHERE status = 'completed' AND scan_generation < ?
            """,
            (1,),
            "artifact_catalog_discovery_frontier",
        ),
    ],
)
def test_discovery_hot_queries_ignore_completed_history_without_temp_sort(
    tmp_path: Path,
    index_name: str,
    sql: str,
    parameters: tuple[object, ...],
    table: str,
) -> None:
    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    completed_count = 5_000
    with queue._connect(read_only=False) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery(
                relative_path, status, discovered_at, completed_at, content_sha256
            ) VALUES (?, 'completed', ?, ?, ?)
            """,
            (
                (
                    f"jobs/completed-{index}",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    f"{index:064x}",
                )
                for index in range(completed_count)
            ),
        )
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery(
                relative_path, status, discovered_at
            ) VALUES (?, 'pending', ?)
            """,
            ((f"jobs/pending-{index}", NOW.isoformat()) for index in range(3)),
        )
        connection.execute(
            """
            UPDATE artifact_catalog_discovery_metadata
            SET scan_generation = 1
            WHERE singleton = 1
            """
        )
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery_frontier(
                scan_generation, relative_directory, directory_kind, status
            ) VALUES (1, ?, 'shards', 'completed')
            """,
            ((f"jobs/completed-{index}/shards",) for index in range(completed_count)),
        )
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery_frontier(
                scan_generation, relative_directory, directory_kind, status
            ) VALUES (1, ?, 'shards', 'active')
            """,
            ((f"jobs/active-{index}/shards",) for index in range(3)),
        )
        connection.commit()

    with queue._connect(read_only=True) as connection:
        details = tuple(
            str(row[3])
            for row in connection.execute(
                f"EXPLAIN QUERY PLAN {sql}",
                parameters,
            ).fetchall()
        )

    assert any(index_name in detail for detail in details), details
    assert all("USE TEMP B-TREE" not in detail for detail in details)
    assert all(detail != f"SCAN {table}" for detail in details)
    if table == "artifact_catalog_discovery":
        assert queue.list_pending(limit=10) == (
            "jobs/pending-0",
            "jobs/pending-1",
            "jobs/pending-2",
        )
        assert queue.pending_count() == 3


def test_discovery_hot_path_select_count_is_constant_with_completed_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    with queue._connect(read_only=False) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery(
                relative_path, status, discovered_at, completed_at, content_sha256
            ) VALUES (?, 'completed', ?, ?, ?)
            """,
            (
                (
                    f"jobs/completed-{index}",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    f"{index:064x}",
                )
                for index in range(5_000)
            ),
        )
        connection.execute(
            """
            INSERT INTO artifact_catalog_discovery(
                relative_path, status, discovered_at
            ) VALUES ('jobs/pending', 'pending', ?)
            """,
            (NOW.isoformat(),),
        )
        connection.execute(
            """
            UPDATE artifact_catalog_discovery_metadata
            SET scan_generation = 1
            WHERE singleton = 1
            """
        )
        connection.executemany(
            """
            INSERT INTO artifact_catalog_discovery_frontier(
                scan_generation, relative_directory, directory_kind, status
            ) VALUES (1, ?, 'shards', 'completed')
            """,
            ((f"jobs/completed-{index}/shards",) for index in range(5_000)),
        )
        connection.execute(
            """
            INSERT INTO artifact_catalog_discovery_frontier(
                scan_generation, relative_directory, directory_kind, status
            ) VALUES (1, 'jobs/active/shards', 'shards', 'active')
            """
        )
        connection.commit()

    select_statements: list[str] = []
    original_connect = queue._connect

    class CountingConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection

        def execute(
            self,
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> object:
            if sql.lstrip().upper().startswith("SELECT"):
                select_statements.append(sql)
            return self._connection.execute(sql, parameters)  # type: ignore[attr-defined]

        def __getattr__(self, name: str) -> object:
            return getattr(self._connection, name)

    @contextmanager
    def counted_connect(*, read_only: bool) -> Iterator[object]:
        with original_connect(read_only=read_only) as connection:
            yield CountingConnection(connection)

    monkeypatch.setattr(queue, "_connect", counted_connect)

    queue._next_frontier(timestamp=NOW.isoformat())
    assert queue.list_pending(limit=10) == ("jobs/pending",)
    assert queue.pending_count() == 1
    assert len(select_statements) <= 5


def test_discovery_queue_closes_when_row_factory_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    setup_error = RuntimeError("discovery row factory failed")

    class FailingSetupConnection:
        def __init__(self) -> None:
            self.closed = False

        @property
        def row_factory(self) -> object | None:
            return None

        @row_factory.setter
        def row_factory(self, _value: object) -> None:
            raise setup_error

        def close(self) -> None:
            self.closed = True

    connection = FailingSetupConnection()
    monkeypatch.setattr(
        queue._path_authority,
        "open_verified_connection",
        lambda _opener: connection,
    )

    with pytest.raises(RuntimeError, match="row factory"), queue._connect(read_only=True):
        pass

    assert connection.closed


def test_discovery_queue_routes_business_error_into_verified_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    business_error = RuntimeError("discovery consumer failed")
    observed_primary: list[BaseException | None] = []

    def record_close(
        connection: object,
        _authority: object,
        *,
        primary_error: BaseException | None = None,
        known_identity_failure: bool = False,
    ) -> None:
        del known_identity_failure
        observed_primary.append(primary_error)
        connection.close()  # type: ignore[attr-defined]

    monkeypatch.setattr(
        artifact_retention_module,
        "close_verified_sqlite_connection",
        record_close,
    )

    with pytest.raises(RuntimeError, match="discovery consumer"), queue._connect(read_only=True):
        raise business_error

    assert observed_primary == [business_error]


def test_failed_page_remains_pending_and_is_retried_after_restart(tmp_path: Path) -> None:
    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    queue.record_discovery(("jobs/a",), discovered_at=NOW)
    failing = _RegistrarStub([])
    failing.fail_registration = True

    with pytest.raises(LabArtifactCatalogIntegrityError, match="injected"):
        _runtime(tmp_path, failing).run_step()

    assert queue.list_pending(limit=10) == ("jobs/a",)
    recovered = _RegistrarStub([])
    step = _runtime(tmp_path, recovered).run_step()
    assert step.processed_paths == ("jobs/a",)
    assert queue.list_pending(limit=10) == ()


def test_partial_registration_checkpoints_only_processed_pending_prefix(
    tmp_path: Path,
) -> None:
    queue = LabArtifactDiscoveryQueue(
        tmp_path / "discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    queue.record_discovery(("jobs/a", "jobs/b"), discovered_at=NOW)
    registrar = _PartialRegistrationRegistrarStub([])

    step = _runtime(tmp_path, registrar, max_bundles=2).run_step()

    assert step.processed_paths == ("jobs/a",)
    assert step.pending_bundles == 1
    assert queue.list_pending(limit=10) == ("jobs/b",)


def test_new_lexically_earlier_bundle_is_found_in_a_later_bounded_pass(
    tmp_path: Path,
) -> None:
    old = "jobs/ffffffff-ffff-4fff-8fff-ffffffffffff"
    new = "jobs/00000000-0000-4000-8000-000000000001"
    registrar = _RegistrarStub([old])
    runtime = _runtime(tmp_path, registrar)

    assert runtime.run_step().processed_paths == (old,)
    registrar.entries.insert(0, new)

    observed: list[str] = []
    for _attempt in range(4):
        observed.extend(runtime.run_step().processed_paths)
        if new in observed:
            break

    assert new in observed
    assert registrar.discovery_calls <= 5
    assert all(limit == 1 for limit in registrar.discovery_limits)


def test_runtime_fails_closed_when_another_single_writer_holds_lock(tmp_path: Path) -> None:
    registrar = _RegistrarStub(["jobs/a"])
    runtime = _runtime(tmp_path, registrar)
    descriptor = os.open(tmp_path / "catalog.lock", os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(LabArtifactCatalogAlreadyRunningError):
            runtime.run_step()
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    assert registrar.discovery_calls == 0


def test_discovery_queue_rejects_relative_sqlite_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exact absolute"):
        LabArtifactDiscoveryQueue(
            Path("relative/discovery.sqlite3"),
            managed_trust_root=tmp_path,
        )


def test_discovery_queue_requires_explicit_managed_trust_root(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="managed_trust_root"):
        LabArtifactDiscoveryQueue(tmp_path / "discovery.sqlite3")


@pytest.mark.parametrize("hazard", ["parent-symlink", "final-symlink", "hardlink", "mode"])
def test_discovery_queue_rejects_unsafe_sqlite_path(tmp_path: Path, hazard: str) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "discovery.sqlite3"
    if hazard == "parent-symlink":
        linked = tmp_path / "linked"
        linked.symlink_to(private, target_is_directory=True)
        path = linked / path.name
    else:
        seed = private / "seed.sqlite3"
        seed.touch(mode=0o600)
        if hazard == "final-symlink":
            path.symlink_to(seed)
        elif hazard == "hardlink":
            os.link(seed, path)
        else:
            path.touch(mode=0o600)
            path.chmod(0o640)

    with pytest.raises(ValueError, match="symlink|hard link|mode|unsafe"):
        LabArtifactDiscoveryQueue(path, managed_trust_root=tmp_path)
