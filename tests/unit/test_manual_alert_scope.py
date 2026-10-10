from __future__ import annotations

from datetime import datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any

from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    build_manual_watchlist_projections,
)
from rquant.serving_publisher import ServingReader
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

NOW = FIXTURE_BUILT_AT + timedelta(minutes=5)
MAX_AGE = timedelta(minutes=30)


def _row(
    owner: str,
    code: str,
    version: int,
    *,
    deleted: bool = False,
    expiry: datetime | None = None,
) -> ManualWatchlistProjectionRow:
    return ManualWatchlistProjectionRow(
        owner_id=owner,
        ts_code=code,
        version=version,
        deleted=deleted,
        source=None if deleted else "detail",
        price_levels_json="[]" if deleted else '["10.00"]',
        expires_at=expiry,
        updated_at=None if deleted else FIXTURE_BUILT_AT - timedelta(minutes=1),
    )


def _publish(
    root: Path,
    *,
    sequence: int = 0,
    rows: tuple[ManualWatchlistProjectionRow, ...] = (),
    activated: bool = True,
) -> None:
    built_at = FIXTURE_BUILT_AT + timedelta(minutes=sequence)
    snapshot = (
        ManualWatchlistAuthoritySnapshot.create(
            activated_at=FIXTURE_BUILT_AT - timedelta(days=1), rows=rows
        )
        if activated
        else None
    )
    build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        signal_projections=build_manual_watchlist_projections(snapshot, observed_at=built_at),
    )


def test_scope_preserves_owner_code_version_and_filters_at_one_instant(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rows=(
            _row("alice", "600001.SH", 2),
            _row("bob", "600001.SH", 7),
            _row("alice", "600002.SH", 3, expiry=NOW),
            _row("alice", "600003.SH", 4, deleted=True),
        ),
    )
    scope_module = import_module("rquant.serving_manual_watchlist_read")
    with ServingReader(root).acquire_generation() as lease:
        result = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert result.availability == "ready"
        assert result.generation_id == lease.manifest.generation_id
        assert result.evaluated_at == NOW
        assert result.source_generation_id == lease.manifest.source_generations["signals"]
        assert [(row.owner_id, row.ts_code, row.version) for row in result.members] == [
            ("alice", "600001.SH", 2),
            ("bob", "600001.SH", 7),
        ]


def test_trusted_empty_is_ready_but_unactivated_and_missing_are_unavailable(tmp_path: Path) -> None:
    scope_module = import_module("rquant.serving_manual_watchlist_read")
    absent = scope_module.read_manual_alert_scope(
        None, evaluated_at=NOW, max_generation_age=MAX_AGE
    )
    assert absent.availability == "unavailable"
    assert absent.generation_id is None
    assert not hasattr(absent, "members")

    root = tmp_path / "serving"
    _publish(root)
    with ServingReader(root).acquire_generation() as lease:
        ready = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert ready.availability == "ready"
        assert ready.members == ()
    _publish(root, sequence=1, activated=False)
    with ServingReader(root).acquire_generation() as lease:
        unavailable = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert unavailable.availability == "unavailable"
        assert unavailable.generation_id == lease.manifest.generation_id
        assert not hasattr(unavailable, "members")


def test_each_borrowed_generation_is_self_consistent_and_age_bound(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    scope_module = import_module("rquant.serving_manual_watchlist_read")
    with ServingReader(root).acquire_generation() as old_lease:
        _publish(root, sequence=1, rows=(_row("bob", "600002.SH", 2),))
        with ServingReader(root).acquire_generation() as new_lease:
            old = scope_module.read_manual_alert_scope(
                old_lease, evaluated_at=NOW, max_generation_age=MAX_AGE
            )
            current = scope_module.read_manual_alert_scope(
                new_lease, evaluated_at=NOW, max_generation_age=MAX_AGE
            )
            assert old.availability == current.availability == "ready"
            assert old.generation_id != current.generation_id
            assert [(row.owner_id, row.ts_code) for row in old.members] == [("alice", "600001.SH")]
            assert [(row.owner_id, row.ts_code) for row in current.members] == [
                ("bob", "600002.SH")
            ]
            stale = scope_module.read_manual_alert_scope(
                old_lease, evaluated_at=NOW, max_generation_age=timedelta(minutes=1)
            )
            assert stale.availability == "unavailable"
            assert not hasattr(stale, "members")


class _CorruptingCursor:
    def __init__(self, delegate: Any, *, fault: str) -> None:
        self._delegate = delegate
        self._fault = fault
        self._query = ""

    def execute(self, query: str, parameters: Any = ()) -> _CorruptingCursor:
        if self._fault == "missing_table" and "FROM manual_watchlist ORDER BY" in query:
            raise RuntimeError("manual watchlist table is missing")
        self._query = query
        self._delegate.execute(query, parameters)
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._delegate.fetchall()
        if self._fault == "missing_status" and "FROM projection_status" in self._query:
            return rows[:-1]
        if self._fault == "bad_row" and "FROM manual_watchlist ORDER BY" in self._query and rows:
            bad = list(rows[0])
            bad[2] = 0
            return [tuple(bad), *rows[1:]]
        return rows

    def close(self) -> None:
        self._delegate.close()


class _CorruptingConnection:
    def __init__(self, delegate: Any, *, fault: str) -> None:
        self._delegate = delegate
        self._fault = fault

    def cursor(self) -> _CorruptingCursor:
        return _CorruptingCursor(self._delegate.cursor(), fault=self._fault)

    def close(self) -> None:
        self._delegate.close()


def test_bad_row_count_pointer_or_row_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    scope_module = import_module("rquant.serving_manual_watchlist_read")
    with ServingReader(root).acquire_generation() as lease:
        valid_manifest = lease.manifest
        counts = dict(valid_manifest.row_counts)
        counts["manual_watchlist"] += 1
        lease.manifest = valid_manifest.model_copy(update={"row_counts": counts})
        mismatched = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert mismatched.availability == "unavailable"
        assert not hasattr(mismatched, "members")
        lease.manifest = valid_manifest

        valid_pointer = lease.pointer
        assert valid_pointer is not None
        lease.pointer = valid_pointer.model_copy(update={"generation_id": "f" * 64})
        wrong_pointer = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert wrong_pointer.availability == "unavailable"
        lease.pointer = valid_pointer

        lease.connection = _CorruptingConnection(lease.connection, fault="bad_row")
        bad_row = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert bad_row.availability == "unavailable"
        assert not hasattr(bad_row, "members")


def test_missing_table_or_status_cannot_authorize_scope(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    scope_module = import_module("rquant.serving_manual_watchlist_read")
    for fault in ("missing_table", "missing_status"):
        with ServingReader(root).acquire_generation() as lease:
            lease.connection = _CorruptingConnection(lease.connection, fault=fault)
            result = scope_module.read_manual_alert_scope(
                lease, evaluated_at=NOW, max_generation_age=MAX_AGE
            )
            assert result.availability == "unavailable"
            assert not hasattr(result, "members")


def test_over_capacity_owner_cannot_authorize_scope(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rows=tuple(_row("alice", f"{600000 + index:06d}.SH", 1) for index in range(501)),
    )
    scope_module = import_module("rquant.serving_manual_watchlist_read")
    with ServingReader(root).acquire_generation() as lease:
        result = scope_module.read_manual_alert_scope(
            lease, evaluated_at=NOW, max_generation_age=MAX_AGE
        )
        assert result.availability == "unavailable"
        assert not hasattr(result, "members")
