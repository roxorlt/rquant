"""Immutable bounded source snapshots for page-only Serving projections."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from tempfile import mkdtemp
from types import MappingProxyType
from typing import Annotated, Self
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
from pydantic import Field, StringConstraints, field_serializer, field_validator, model_validator

from rquant.canvas_publication_receipt import (
    CanvasPublicationCatalogRecord,
    CanvasPublicationKeyring,
    CanvasPublicationReceipt,
    CanvasPublicationReceiptStore,
    canvas_catalog_record_hash,
    canvas_command_hash,
    canvas_publication_effect_id,
    canvas_publication_generation_id,
    canvas_publication_receipt_id,
    canvas_source_identity_hash,
)
from rquant.notification_state import (
    NotificationProjectionAuthoritySnapshot,
    NotificationProjectionSourceReceipt,
    NotificationStateStore,
)
from rquant.page_control import (
    CanvasCurrentHead,
    PageControlOutbox,
    PageControlStatus,
    read_canvas_current_head,
)
from rquant.research_gate import (
    ResearchGateFailure,
    ResearchGateRequest,
    evaluate_store_research_gate,
    research_gate_metadata_ready,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.serving_read_models import ServingProjectionPayload
from rquant.storage.duckdb import DuckDBStore

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_SCREEN_PRESETS = 256
_MAX_MINUTE_SOURCES = 127
_MAX_CANVAS_HITS = 20_000
_MAX_CANVAS_DEFINITIONS = 512
_MAX_CANVAS_DEFINITION_BYTES = 64 * 1024
_MAX_CANVAS_CATALOG_BYTES = 2 * 1024 * 1024
_MAX_RESEARCH_GATES = 512
_MAX_PULSE_ROWS = 512
_MAX_PULSE_FILE_BYTES = 256 * 1024
_MAX_ALERT_FILE_BYTES = 512 * 1024
_MAX_RUNTIME_CONFIG_BYTES = 16 * 1024
_CANVAS_CATALOG_SCHEMA_VERSION = 1
_PAGE_CONTROL_PROTOCOL_MARKER = "safe-effect-journal-v2"
_PAGE_CONTROL_PROTOCOL_VERSION = 2
_COMPANION_SIGNAL_TABLES = frozenset(
    {
        "screen_result",
        "pool2_watch",
        "monitor_event",
        "surge_event",
        "market_snapshot",
        "market_overview",
        "intraday_kline",
    }
)
_EMPTY_PROJECTION_AVAILABLE_AT = datetime(1970, 1, 1, tzinfo=UTC)


class PageProjectionSourceIntegrityError(RuntimeError):
    """A mutable or malformed operational snapshot cannot become Serving evidence."""


@dataclass(frozen=True)
class _BoundReadonlyDirectory:
    path: Path
    descriptors: tuple[int, ...]
    component_names: tuple[str, ...]
    label: str

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]

    def verify(self) -> None:
        for parent, child, component in zip(
            self.descriptors[:-1],
            self.descriptors[1:],
            self.component_names,
            strict=True,
        ):
            try:
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError as exc:
                raise PageProjectionSourceIntegrityError(
                    f"{self.label} ancestor changed while bound"
                ) from exc
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                raise PageProjectionSourceIntegrityError(
                    f"{self.label} ancestors must be regular non-symlink directories"
                )
            if (entry.st_dev, entry.st_ino) != (
                os.fstat(child).st_dev,
                os.fstat(child).st_ino,
            ):
                raise PageProjectionSourceIntegrityError(
                    f"{self.label} ancestor rotated while bound"
                )

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            with suppress(OSError):
                os.close(descriptor)


def _bind_readonly_directory(path: Path, *, label: str) -> _BoundReadonlyDirectory:
    normalized = Path(os.path.abspath(path))
    flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    component_names: list[str] = []
    try:
        descriptors.append(os.open(normalized.anchor, flags))
        for component in normalized.parts[1:]:
            parent = descriptors[-1]
            entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                raise PageProjectionSourceIntegrityError(
                    f"{label} ancestors must be regular non-symlink directories"
                )
            descriptor = os.open(component, flags, dir_fd=parent)
            opened = os.fstat(descriptor)
            if (entry.st_dev, entry.st_ino) != (opened.st_dev, opened.st_ino):
                os.close(descriptor)
                raise PageProjectionSourceIntegrityError(f"{label} ancestor rotated while open")
            descriptors.append(descriptor)
            component_names.append(component)
        binding = _BoundReadonlyDirectory(
            path=normalized,
            descriptors=tuple(descriptors),
            component_names=tuple(component_names),
            label=label,
        )
        binding.verify()
        return binding
    except FileNotFoundError:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise
    except Exception:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _read_bound_optional_file(
    binding: _BoundReadonlyDirectory,
    name: str,
    *,
    max_bytes: int,
) -> tuple[bytes, os.stat_result] | None:
    try:
        item = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(item.st_mode) or not stat.S_ISREG(item.st_mode):
        raise PageProjectionSourceIntegrityError(
            f"surge live source {name} must be a regular non-symlink file"
        )
    if item.st_size > max_bytes:
        raise PageProjectionSourceIntegrityError(f"surge live source {name} exceeds size bound")
    descriptor = os.open(
        name,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=binding.descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(item):
            raise PageProjectionSourceIntegrityError(f"surge live source {name} rotated while open")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(max_bytes + 1)
        after = os.fstat(descriptor)
        if _file_identity(after) != _file_identity(opened):
            raise PageProjectionSourceIntegrityError(f"surge live source {name} changed while read")
    finally:
        os.close(descriptor)
    if len(raw) > max_bytes:
        raise PageProjectionSourceIntegrityError(f"surge live source {name} exceeds size bound")
    binding.verify()
    return raw, opened


def _local_naive(value: datetime) -> datetime:
    return normalize_aware_utc(value).astimezone(_SHANGHAI).replace(tzinfo=None)


def _database_timestamp(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise PageProjectionSourceIntegrityError("projection timestamp is not a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=_SHANGHAI)
    return value.astimezone(UTC)


class _StableReadonlyDuckDB:
    """Open one regular immutable-generation file and reject pointer rotation mid-read."""

    def __init__(self, path: Path) -> None:
        normalized = Path(os.path.abspath(path))
        if not normalized.is_absolute():
            raise ValueError("projection database path must be absolute")
        self.path = normalized
        self._before: os.stat_result | None = None
        self._bound_directory: Path | None = None
        self._bound_path: Path | None = None
        self._bound_identity: os.stat_result | None = None
        self.connection: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        before = os.lstat(self.path)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise PageProjectionSourceIntegrityError(
                "projection database must be a regular non-symlink file"
            )
        self._before = before
        bound_directory = Path(
            mkdtemp(
                prefix=f".{self.path.name}.{uuid4().hex}.",
                dir=self.path.parent,
            )
        )
        os.chmod(bound_directory, 0o700)
        bound_path = bound_directory / "generation.duckdb"
        try:
            os.link(self.path, bound_path, follow_symlinks=False)
            bound = os.lstat(bound_path)
            after_link = os.lstat(self.path)
            if _file_identity(bound) != _file_identity(before) or _file_identity(
                after_link
            ) != _file_identity(before):
                raise PageProjectionSourceIntegrityError(
                    "projection database rotated while binding its opened generation"
                )
            self._bound_directory = bound_directory
            self._bound_path = bound_path
            self._bound_identity = bound
            self.connection = duckdb.connect(str(bound_path), read_only=True)
        except Exception:
            with suppress(FileNotFoundError):
                os.unlink(bound_path)
            with suppress(FileNotFoundError):
                os.rmdir(bound_directory)
            raise
        return self.connection

    @property
    def bound_path(self) -> Path:
        if self._bound_path is None or self.connection is None:
            raise RuntimeError("projection database generation is not currently bound")
        return self._bound_path

    def __exit__(self, *_error: object) -> None:
        assert self._before is not None
        try:
            assert self._bound_path is not None
            assert self._bound_identity is not None
            bound_after = os.lstat(self._bound_path)
            if _file_identity(bound_after) != _file_identity(self._bound_identity):
                raise PageProjectionSourceIntegrityError(
                    "projection database opened generation rotated while read"
                )
            after = os.lstat(self.path)
            if _file_identity(after) != _file_identity(self._before):
                raise PageProjectionSourceIntegrityError(
                    "projection database rotated while the snapshot was being read"
                )
        finally:
            if self.connection is not None:
                self.connection.close()
            if self._bound_path is not None:
                with suppress(FileNotFoundError):
                    os.unlink(self._bound_path)
            if self._bound_directory is not None:
                with suppress(FileNotFoundError):
                    os.rmdir(self._bound_directory)


@dataclass(frozen=True)
class _ReadonlyPageControlAudit:
    command_id: str
    command_kind: str
    command_hash: str
    payload: Mapping[str, object]
    status: PageControlStatus


class _ReadonlyPageControlAuditReader:
    _REQUIRED_TABLES = {
        "page_control_command": {
            "command_id": ("TEXT", 0, None, 1),
            "command_kind": ("TEXT", 1, None, 0),
            "command_hash": ("TEXT", 1, None, 0),
            "payload_json": ("TEXT", 1, None, 0),
            "status": ("TEXT", 1, None, 0),
            "enqueued_at": ("TEXT", 1, None, 0),
            "completed_at": ("TEXT", 0, None, 0),
            "result_json": ("TEXT", 0, None, 0),
            "error": ("TEXT", 0, None, 0),
            "processing_owner": ("TEXT", 0, None, 0),
            "lease_expires_at": ("TEXT", 0, None, 0),
            "attempt_count": ("INTEGER", 1, "0", 0),
            "claim_token": ("TEXT", 0, None, 0),
        },
        "page_control_effect": {
            "command_id": ("TEXT", 0, None, 1),
            "command_hash": ("TEXT", 1, None, 0),
            "effect_kind": ("TEXT", 1, None, 0),
            "status": ("TEXT", 1, None, 0),
            "owner_id": ("TEXT", 1, None, 0),
            "claim_token": ("TEXT", 1, None, 0),
            "started_at": ("TEXT", 1, None, 0),
            "completed_at": ("TEXT", 0, None, 0),
            "result_json": ("TEXT", 0, None, 0),
            "error": ("TEXT", 0, None, 0),
        },
        "page_control_protocol_activation": {
            "marker_name": ("TEXT", 0, None, 1),
            "protocol_version": ("INTEGER", 1, None, 0),
            "activated_at": ("TEXT", 1, None, 0),
        },
    }

    def __init__(self, path: Path) -> None:
        self.path = Path(os.path.abspath(path))
        self._snapshot_connection: sqlite3.Connection | None = None
        validated = self._validate_schema()
        self._validated_node_identity = (validated.st_dev, validated.st_ino)

    def _connect(self, path: Path | None = None) -> sqlite3.Connection:
        database_path = self.path if path is None else path
        uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=0)
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                f"PageControl audit cannot be opened read-only: {exc}"
            ) from exc
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        if self._snapshot_connection is not None:
            yield self._snapshot_connection
            return
        with self._connect() as connection:
            yield connection

    @contextmanager
    def snapshot(self) -> Iterator[None]:
        if self._snapshot_connection is not None:
            raise RuntimeError("PageControl audit snapshot is already active")
        before = os.lstat(self.path)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or (before.st_dev, before.st_ino) != self._validated_node_identity
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database rotated or is not a regular non-symlink file"
            )
        bound_directory = Path(
            mkdtemp(
                prefix=f".{self.path.name}.{uuid4().hex}.",
                dir=self.path.parent,
            )
        )
        os.chmod(bound_directory, 0o700)
        bound_path = bound_directory / "generation.sqlite3"
        connection: sqlite3.Connection | None = None
        try:
            os.link(self.path, bound_path, follow_symlinks=False)
            bound = os.lstat(bound_path)
            after_link = os.lstat(self.path)
            if _file_identity(bound) != _file_identity(before) or _file_identity(
                after_link
            ) != _file_identity(before):
                raise PageProjectionSourceIntegrityError(
                    "PageControl audit rotated while binding its exact generation"
                )
            connection = self._connect(bound_path)
            self._validate_schema_connection(connection)
            connection.execute("BEGIN")
            before_data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
            after_data_version = before_data_version
            self._snapshot_connection = connection
            self.assert_quiescent()
            yield
            after_data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
        except BaseException:
            self._snapshot_connection = None
            if connection is not None:
                connection.rollback()
                connection.close()
            with suppress(FileNotFoundError):
                os.unlink(bound_path)
            with suppress(FileNotFoundError):
                os.rmdir(bound_directory)
            raise
        else:
            self._snapshot_connection = None
            assert connection is not None
            connection.rollback()
            connection.close()
        integrity_error: PageProjectionSourceIntegrityError | None = None
        try:
            after = os.lstat(self.path)
            bound_after = os.lstat(bound_path)
            with self._connect(bound_path) as current:
                inflight = current.execute(
                    """
                    SELECT 1 FROM page_control_command
                    WHERE status IN (?, ?)
                    LIMIT 1
                    """,
                    (PageControlStatus.PENDING.value, PageControlStatus.PROCESSING.value),
                ).fetchone()
            if (
                inflight is not None
                or _file_identity(after) != _file_identity(before)
                or _file_identity(bound_after) != _file_identity(bound)
                or after_data_version != before_data_version
            ):
                integrity_error = PageProjectionSourceIntegrityError(
                    "PageControl audit generation changed or entered in-flight state"
                )
        except (OSError, sqlite3.Error) as exc:
            integrity_error = PageProjectionSourceIntegrityError(
                f"PageControl audit generation cannot be revalidated: {exc}"
            )
        finally:
            with suppress(FileNotFoundError):
                os.unlink(bound_path)
            with suppress(FileNotFoundError):
                os.rmdir(bound_directory)
        if integrity_error is not None:
            raise integrity_error

    def _validate_schema(self) -> os.stat_result:
        try:
            before = os.lstat(self.path)
        except FileNotFoundError as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database does not exist"
            ) from exc
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database must be a regular non-symlink file"
            )
        try:
            with self._connect() as connection:
                self._validate_schema_connection(connection)
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                f"PageControl audit schema cannot be read: {exc}"
            ) from exc
        after = os.lstat(self.path)
        if _file_identity(after) != _file_identity(before):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database rotated while validating schema"
            )
        return after

    def _validate_schema_connection(self, connection: sqlite3.Connection) -> None:
        for table_name, expected in self._REQUIRED_TABLES.items():
            rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
            observed = {
                str(row[1]): (
                    str(row[2]).upper(),
                    int(row[3]),
                    None if row[4] is None else str(row[4]),
                    int(row[5]),
                )
                for row in rows
            }
            if observed != expected:
                raise PageProjectionSourceIntegrityError(
                    "PageControl audit schema is invalid or incomplete"
                )
        foreign_keys = connection.execute("PRAGMA foreign_key_list(page_control_effect)").fetchall()
        if not any(
            str(row[2]) == "page_control_command"
            and str(row[3]) == "command_id"
            and str(row[4]) == "command_id"
            for row in foreign_keys
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit schema lacks its effect authority constraint"
            )
        marker = connection.execute(
            """
            SELECT protocol_version FROM page_control_protocol_activation
            WHERE marker_name = ?
            """,
            (_PAGE_CONTROL_PROTOCOL_MARKER,),
        ).fetchone()
        if marker is None or int(marker[0]) != _PAGE_CONTROL_PROTOCOL_VERSION:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit schema lacks its activation authority"
            )

    def assert_quiescent(self) -> None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT command_id FROM page_control_command
                WHERE status IN (?, ?)
                LIMIT 1
                """,
                (PageControlStatus.PENDING.value, PageControlStatus.PROCESSING.value),
            ).fetchone()
        if row is not None:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit contains an in-flight mutating command"
            )

    def audit(self, command_id: str) -> _ReadonlyPageControlAudit | None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT c.command_id, c.command_kind, c.command_hash,
                       c.payload_json, c.status,
                       e.command_id AS effect_command_id,
                       e.command_hash AS effect_command_hash,
                       e.effect_kind, e.status AS effect_status
                FROM page_control_command AS c
                LEFT JOIN page_control_effect AS e USING (command_id)
                WHERE c.command_id = ?
                """,
                (command_id,),
            ).fetchone()
        if row is None:
            return None
        return self._audit_row(row)

    def canvas_mutations(self) -> Mapping[str, _ReadonlyPageControlAudit]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT c.command_id, c.command_kind, c.command_hash,
                       c.payload_json, c.status,
                       e.command_id AS effect_command_id,
                       e.command_hash AS effect_command_hash,
                       e.effect_kind, e.status AS effect_status
                FROM page_control_command AS c
                LEFT JOIN page_control_effect AS e USING (command_id)
                WHERE c.status = ?
                  AND c.command_kind IN (?, ?, ?, ?, ?)
                ORDER BY c.rowid
                """,
                (
                    PageControlStatus.SUCCEEDED.value,
                    "save_canvas",
                    "delete_canvas",
                    "set_canvas_pool_refs",
                    "save_user_pool",
                    "fork_builtin_pool",
                ),
            ).fetchall()
        latest: dict[str, _ReadonlyPageControlAudit] = {}
        for row in rows:
            audit = self._audit_row(row)
            canvas_name = self._canvas_name_for_audit(audit)
            if canvas_name is not None and canvas_name != "__default__":
                latest[canvas_name] = audit
        return MappingProxyType(latest)

    @staticmethod
    def _canvas_name_for_audit(audit: _ReadonlyPageControlAudit) -> str | None:
        field_name = (
            "name"
            if audit.command_kind in {"save_canvas", "delete_canvas", "set_canvas_pool_refs"}
            else "canvas_name"
        )
        value = audit.payload.get(field_name)
        if value is None:
            return None
        if not isinstance(value, str):
            raise PageProjectionSourceIntegrityError(
                "PageControl canvas mutation has an invalid canvas name"
            )
        return value

    @staticmethod
    def _audit_row(row: sqlite3.Row) -> _ReadonlyPageControlAudit:
        try:
            payload = json.loads(row["payload_json"])
            status = PageControlStatus(row["status"])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PageProjectionSourceIntegrityError("PageControl audit row is malformed") from exc
        if not isinstance(payload, dict):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit command payload is not an object"
            )
        command_id = str(row["command_id"])
        if payload.get("command_id") != command_id:
            raise PageProjectionSourceIntegrityError("PageControl audit command identity mismatch")
        command_hash = str(row["command_hash"])
        if canonical_sha256(payload) != command_hash:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit command payload hash mismatch"
            )
        if (
            row["effect_command_id"] != command_id
            or row["effect_command_hash"] != command_hash
            or row["effect_kind"] != row["command_kind"]
            or row["effect_status"] != "succeeded"
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit lacks its matching succeeded effect authority"
            )
        return _ReadonlyPageControlAudit(
            command_id=command_id,
            command_kind=str(row["command_kind"]),
            command_hash=command_hash,
            payload=MappingProxyType(payload),
            status=status,
        )


class DuckDBSignalPageProjectionSource:
    """Build bounded point-in-time page projections from an atomic read replica."""

    def __init__(
        self,
        database_path: Path,
        *,
        canvas_catalog_root: Path | None = None,
        canvas_receipt_root: Path | None = None,
        canvas_publication_keyring: CanvasPublicationKeyring | None = None,
        page_control_outbox: PageControlOutbox | Path | None = None,
        surge_live_root: Path | None = None,
    ) -> None:
        self.database_path = Path(os.path.abspath(database_path))
        self.canvas_catalog_root = (
            None if canvas_catalog_root is None else Path(os.path.abspath(canvas_catalog_root))
        )
        self.canvas_receipt_root = (
            None if canvas_receipt_root is None else Path(os.path.abspath(canvas_receipt_root))
        )
        self.canvas_publication_keyring = canvas_publication_keyring
        self.surge_live_root = (
            None if surge_live_root is None else Path(os.path.abspath(surge_live_root))
        )
        if page_control_outbox is None:
            self.page_control_outbox = None
        else:
            audit_path = (
                page_control_outbox.path
                if isinstance(page_control_outbox, PageControlOutbox)
                else Path(page_control_outbox)
            )
            self.page_control_outbox = _ReadonlyPageControlAuditReader(audit_path)
        if self.canvas_catalog_root is not None and self.page_control_outbox is None:
            raise PageProjectionSourceIntegrityError(
                "configured canvas catalog requires readonly PageControl audit authority"
            )
        if self.canvas_catalog_root is not None and (
            self.canvas_receipt_root is None or self.canvas_publication_keyring is None
        ):
            raise PageProjectionSourceIntegrityError(
                "configured canvas catalog requires receipt root and keyring authority"
            )

    def __call__(self, observed_at: datetime, /) -> SignalPageProjectionSnapshot:
        if self.page_control_outbox is None:
            return self._build_snapshot(observed_at)
        with self.page_control_outbox.snapshot():
            return self._build_snapshot(observed_at)

    def _build_snapshot(self, observed_at: datetime) -> SignalPageProjectionSnapshot:
        observed = normalize_aware_utc(observed_at)
        cutoff = _local_naive(observed)
        with _StableReadonlyDuckDB(self.database_path) as connection:
            self._require_tables(connection)
            screen_rows = connection.execute(
                """
                SELECT preset_name, MIN(trade_date), MAX(trade_date), COUNT(*)
                FROM screen_result
                WHERE trade_date <= ? AND created_at <= ?
                GROUP BY preset_name
                ORDER BY preset_name
                LIMIT ?
                """,
                (cutoff.date(), cutoff, _MAX_SCREEN_PRESETS + 1),
            ).fetchall()
            if len(screen_rows) > _MAX_SCREEN_PRESETS:
                raise PageProjectionSourceIntegrityError(
                    "screen bounds exceed the bounded projection limit"
                )
            screen_bounds = tuple(
                ScreenBoundsProjectionRow(
                    preset_name=str(preset),
                    min_date=minimum,
                    max_date=maximum,
                    candidate_count=int(count),
                )
                for preset, minimum, maximum, count in screen_rows
            )
            minute_coverage = self._minute_coverage(connection, cutoff=cutoff)
            latest_row = connection.execute(
                """
                SELECT MAX(trade_date)
                FROM screen_result
                WHERE trade_date <= ? AND created_at <= ?
                """,
                (cutoff.date(), cutoff),
            ).fetchone()
            latest_date = None if latest_row is None else latest_row[0]
            diagnostics: tuple[CanvasDiagnosticProjectionRow, ...] = ()
            hits: tuple[CanvasHitProjectionRow, ...] = ()
            if latest_date is not None:
                diagnostic_rows = connection.execute(
                    """
                    SELECT preset_name, COUNT(*)
                    FROM screen_result
                    WHERE trade_date = ? AND created_at <= ?
                    GROUP BY preset_name
                    ORDER BY preset_name
                    LIMIT ?
                    """,
                    (latest_date, cutoff, _MAX_SCREEN_PRESETS + 1),
                ).fetchall()
                if len(diagnostic_rows) > _MAX_SCREEN_PRESETS:
                    raise PageProjectionSourceIntegrityError(
                        "canvas diagnostics exceed the bounded projection limit"
                    )
                diagnostics = tuple(
                    CanvasDiagnosticProjectionRow(
                        trade_date=latest_date,
                        preset_name=str(preset),
                        step_index=0,
                        rule_label="final",
                        remaining_count=int(count),
                    )
                    for preset, count in diagnostic_rows
                )
                hit_rows = connection.execute(
                    """
                    SELECT preset_name, ts_code, name, close, pct_chg
                    FROM screen_result
                    WHERE trade_date = ? AND created_at <= ?
                    ORDER BY preset_name, ts_code
                    LIMIT ?
                    """,
                    (latest_date, cutoff, _MAX_CANVAS_HITS + 1),
                ).fetchall()
                if len(hit_rows) > _MAX_CANVAS_HITS:
                    raise PageProjectionSourceIntegrityError(
                        "canvas hits exceed the bounded projection limit"
                    )
                hits = tuple(
                    CanvasHitProjectionRow(
                        trade_date=latest_date,
                        preset_name=str(preset),
                        ts_code=str(ts_code),
                        row_json=json.dumps(
                            {
                                "close": close,
                                "name": name,
                                "pct_chg": pct_chg,
                                "ts_code": ts_code,
                            },
                            ensure_ascii=True,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    )
                    for preset, ts_code, name, close, pct_chg in hit_rows
                )
            available_row = connection.execute(
                """
                SELECT MAX(created_at) FROM (
                    SELECT MAX(created_at) AS created_at
                    FROM screen_result WHERE trade_date <= ? AND created_at <= ?
                    UNION ALL
                    SELECT MAX(created_at) AS created_at
                    FROM minute_bar WHERE trade_time <= ? AND created_at <= ?
                )
                """,
                (cutoff.date(), cutoff, cutoff, cutoff),
            ).fetchone()
        if available_row is None or available_row[0] is None:
            raise PageProjectionSourceIntegrityError("projection database has no PIT evidence")
        available = _database_timestamp(available_row[0])
        canvas_definitions = self._canvas_definitions(observed=observed)
        pulse_history, pulse_alerts, runtime_config = _read_surge_live_projection_sources(
            self.surge_live_root,
            observed=observed,
        )
        if canvas_definitions:
            available = max(
                available,
                max(item.updated_at for item in canvas_definitions),
            )
        return SignalPageProjectionSnapshot.create(
            available_at=available,
            screen_bounds=screen_bounds,
            minute_coverage=minute_coverage,
            canvas_diagnostics=diagnostics,
            canvas_latest_trade_date=(
                None
                if latest_date is None
                else CanvasLatestTradeDateProjectionRow(trade_date=latest_date)
            ),
            canvas_hits=hits,
            canvas_definitions=canvas_definitions,
            pulse_history=pulse_history,
            pulse_alerts=pulse_alerts,
            surge_runtime_config=runtime_config,
        )

    def _canvas_definitions(
        self,
        *,
        observed: datetime,
    ) -> tuple[CanvasDefinitionProjectionRow, ...]:
        root = self.canvas_catalog_root
        if root is None:
            return ()
        try:
            catalog_binding = _bind_readonly_directory(
                root,
                label="canvas catalog",
            )
        except FileNotFoundError:
            self._verify_canvas_current_heads((), observed=observed)
            return ()
        if self.canvas_receipt_root is None:
            catalog_binding.close()
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt authority is unavailable"
            )
        root_descriptor = catalog_binding.descriptor
        try:
            catalog_binding.verify()
            rows: list[CanvasDefinitionProjectionRow] = []
            total_bytes = 0
            for child_name in sorted(os.listdir(root_descriptor)):
                if not child_name.endswith(".json") or Path(child_name).name != child_name:
                    continue
                item = os.stat(child_name, dir_fd=root_descriptor, follow_symlinks=False)
                if stat.S_ISLNK(item.st_mode) or not stat.S_ISREG(item.st_mode):
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record must be a regular non-symlink file"
                    )
                if item.st_size > _MAX_CANVAS_DEFINITION_BYTES:
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record exceeds size bound"
                    )
                total_bytes += item.st_size
                if total_bytes > _MAX_CANVAS_CATALOG_BYTES:
                    raise PageProjectionSourceIntegrityError("canvas catalog exceeds size bound")
                descriptor = os.open(
                    child_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_descriptor,
                )
                try:
                    opened = os.fstat(descriptor)
                    if _file_identity(opened) != _file_identity(item):
                        raise PageProjectionSourceIntegrityError(
                            "canvas catalog record rotated while read"
                        )
                    with os.fdopen(descriptor, "rb", closefd=False) as handle:
                        raw_bytes = handle.read(_MAX_CANVAS_DEFINITION_BYTES + 1)
                finally:
                    os.close(descriptor)
                if len(raw_bytes) > _MAX_CANVAS_DEFINITION_BYTES:
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record exceeds size bound"
                    )
                try:
                    raw = json.loads(raw_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record is not valid JSON"
                    ) from exc
                row = CanvasDefinitionProjectionRow.from_catalog_record(
                    file_name=child_name,
                    raw=raw,
                    observed=observed,
                )
                self._verify_canvas_page_control_receipt(
                    row,
                    observed=observed,
                )
                rows.append(row)
                if len(rows) > _MAX_CANVAS_DEFINITIONS:
                    raise PageProjectionSourceIntegrityError("canvas catalog exceeds row bound")
            catalog_binding.verify()
        finally:
            catalog_binding.close()
        self._verify_canvas_current_heads(tuple(rows), observed=observed)
        return tuple(rows)

    def _verify_canvas_current_heads(
        self,
        rows: tuple[CanvasDefinitionProjectionRow, ...],
        *,
        observed: datetime,
    ) -> None:
        if self.canvas_catalog_root is None or self.canvas_publication_keyring is None:
            if rows:
                raise PageProjectionSourceIntegrityError(
                    "canvas current head authority is unavailable"
                )
            return
        head_root = self.canvas_catalog_root.parent / "canvas-publication-heads"
        heads = self._read_canvas_head_authority(
            head_root,
            label="canvas current head",
            observed=observed,
        )
        watermark_root = self.canvas_catalog_root.parent / "canvas-publication-watermarks"
        watermarks = self._read_canvas_head_authority(
            watermark_root,
            label="canvas immutable watermark",
            observed=observed,
        )
        if set(heads) != set(watermarks):
            raise PageProjectionSourceIntegrityError(
                "canvas current heads do not match immutable watermark authority"
            )
        for name, head in heads.items():
            watermark = watermarks[name]
            if (
                head.receipt.receipt_id != watermark.receipt.receipt_id
                or head.sequence != watermark.sequence
                or head.state != watermark.state
                or head.publication_receipt_id != watermark.publication_receipt_id
            ):
                raise PageProjectionSourceIntegrityError(
                    "canvas authority rollback detected by immutable watermark"
                )
        rows_by_name = {row.name: row for row in rows}
        active_names = {name for name, head in heads.items() if head.state == "active"}
        if active_names != set(rows_by_name):
            raise PageProjectionSourceIntegrityError(
                "canvas catalog does not match the complete current head authority"
            )
        if self.page_control_outbox is not None:
            audit_names = set(self.page_control_outbox.canvas_mutations())
            if audit_names != set(heads):
                raise PageProjectionSourceIntegrityError(
                    "canvas current heads do not match PageControl mutation authority"
                )
        for name, head in heads.items():
            if head.state == "deleted" and name in rows_by_name:
                raise PageProjectionSourceIntegrityError(
                    "canvas tombstone conflicts with a catalog definition"
                )
            self._verify_canvas_head_page_control_audit(head)

    def _read_canvas_head_authority(
        self,
        root: Path,
        *,
        label: str,
        observed: datetime,
    ) -> dict[str, CanvasCurrentHead]:
        try:
            binding = _bind_readonly_directory(root, label=label)
        except FileNotFoundError as exc:
            has_audit_authority = self.page_control_outbox is not None and bool(
                self.page_control_outbox.canvas_mutations()
            )
            if has_audit_authority:
                raise PageProjectionSourceIntegrityError(f"{label} authority is missing") from exc
            return {}
        descriptor = binding.descriptor
        try:
            binding.verify()
            names = sorted(os.listdir(descriptor))
            heads: dict[str, CanvasCurrentHead] = {}
            for name in names:
                item = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISLNK(item.st_mode) or not stat.S_ISDIR(item.st_mode):
                    raise PageProjectionSourceIntegrityError(
                        f"{label} entry must be a regular non-symlink directory"
                    )
                child_descriptor = os.open(
                    name,
                    os.O_RDONLY
                    | os.O_CLOEXEC
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=descriptor,
                )
                try:
                    opened = os.fstat(child_descriptor)
                    if (opened.st_dev, opened.st_ino) != (item.st_dev, item.st_ino):
                        raise PageProjectionSourceIntegrityError(
                            f"{label} entry rotated while open"
                        )
                    head = read_canvas_current_head(
                        root,
                        name,
                        self.canvas_publication_keyring,
                        observed_at=observed,
                        directory_descriptor=child_descriptor,
                    )
                except Exception as exc:
                    raise PageProjectionSourceIntegrityError(
                        f"{label} cannot be verified: {exc}"
                    ) from exc
                finally:
                    os.close(child_descriptor)
                if head is not None:
                    heads[name] = head
            binding.verify()
        finally:
            binding.close()
        return heads

    def _verify_canvas_head_page_control_audit(self, head: CanvasCurrentHead) -> None:
        if self.page_control_outbox is None:
            return
        audit = self.page_control_outbox.audit(head.receipt.claims.command.command_id)
        if audit is None or audit.status != PageControlStatus.SUCCEEDED:
            raise PageProjectionSourceIntegrityError(
                "PageControl receipt is missing or not succeeded for canvas current head"
            )
        if (
            audit.command_kind != head.authority_command_kind
            or audit.command_hash != head.authority_command_hash
            or audit.payload.get("kind") != audit.command_kind
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl command authority mismatch for canvas current head"
            )
        latest = self.page_control_outbox.canvas_mutations().get(head.receipt.claims.command.name)
        if latest is None or latest.command_id != audit.command_id:
            raise PageProjectionSourceIntegrityError(
                "canvas current head is not the latest PageControl mutation authority"
            )

    def _verify_canvas_page_control_receipt(
        self,
        row: CanvasDefinitionProjectionRow,
        *,
        observed: datetime,
    ) -> None:
        if self.canvas_receipt_root is None or self.canvas_publication_keyring is None:
            raise PageProjectionSourceIntegrityError(
                "CanvasPublicationReceipt keyring and root are required for canvas definitions"
            )
        try:
            receipt_binding = _bind_readonly_directory(
                self.canvas_receipt_root,
                label="canvas publication receipt root",
            )
            try:
                publication = CanvasPublicationReceiptStore(
                    self.canvas_receipt_root,
                    directory_descriptor=receipt_binding.descriptor,
                ).read(row.publication_receipt_id)
                receipt_binding.verify()
            finally:
                receipt_binding.close()
        except Exception as exc:
            raise PageProjectionSourceIntegrityError(
                f"canvas publication receipt cannot be read safely: {exc}"
            ) from exc
        if not self.canvas_publication_keyring.verify_publication_receipt(
            publication,
            require_active=True,
        ):
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt active signature verification failed"
            )
        self._verify_canvas_receipt_time(publication, observed=observed)
        self._verify_canvas_publication_receipt_semantics(row, publication)
        if self.canvas_catalog_root is None:
            raise PageProjectionSourceIntegrityError("canvas current head root cannot be derived")
        try:
            head = read_canvas_current_head(
                self.canvas_catalog_root.parent / "canvas-publication-heads",
                row.name,
                self.canvas_publication_keyring,
                observed_at=observed,
            )
        except Exception as exc:
            raise PageProjectionSourceIntegrityError(
                f"canvas current head cannot be verified: {exc}"
            ) from exc
        if (
            head is None
            or head.state != "active"
            or head.publication_receipt_id != publication.receipt_id
        ):
            raise PageProjectionSourceIntegrityError(
                "canvas catalog does not match its exact current head"
            )
        self._verify_canvas_head_page_control_audit(head)

    @staticmethod
    def _verify_canvas_receipt_time(
        publication: CanvasPublicationReceipt,
        *,
        observed: datetime,
    ) -> None:
        claims = publication.claims
        timestamps = (
            ("requested_at", claims.command.requested_at),
            ("created_at", claims.created_at),
            ("catalog created_at", claims.catalog_record.created_at),
            ("catalog updated_at", claims.catalog_record.updated_at),
        )
        for label, value in timestamps:
            if value > observed:
                raise PageProjectionSourceIntegrityError(
                    f"canvas publication receipt {label} contains future evidence"
                )

    @staticmethod
    def _verify_canvas_publication_receipt_semantics(
        row: CanvasDefinitionProjectionRow,
        publication: CanvasPublicationReceipt,
    ) -> None:
        claims = publication.claims
        command = claims.command
        if canvas_command_hash(command) != row.command_hash:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt command hash mismatch"
            )
        expected_source_identity_hash = canvas_source_identity_hash(
            command_id=command.command_id,
            command_hash=row.command_hash,
            source=command.source,
        )
        if expected_source_identity_hash != row.source_identity_hash:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt source identity mismatch"
            )
        expected_effect_id = canvas_publication_effect_id(
            command_hash=row.command_hash,
            source_identity_hash=row.source_identity_hash,
            consumer_service_id=claims.consumer_service_id,
            consumer_instance_id=claims.consumer_instance_id,
        )
        if claims.effect_id != expected_effect_id:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt effect identity mismatch"
            )
        expected_generation_id = canvas_publication_generation_id(
            command_hash=row.command_hash,
            source_identity_hash=row.source_identity_hash,
            effect_id=claims.effect_id,
        )
        if row.publication_generation_id != expected_generation_id:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt generation mismatch"
            )
        expected_receipt_id = canvas_publication_receipt_id(
            command_hash=row.command_hash,
            source_identity_hash=row.source_identity_hash,
            effect_id=claims.effect_id,
            generation_id=row.publication_generation_id,
        )
        if publication.receipt_id != expected_receipt_id:
            raise PageProjectionSourceIntegrityError("canvas publication receipt identity mismatch")
        catalog = claims.catalog_record
        if catalog.model_dump(mode="json") != {
            "schema_version": row.schema_version,
            "name": row.name,
            "description": row.description,
            "pool_refs": list(row.pool_refs),
            "created_at": row.created_at.isoformat().replace("+00:00", "Z"),
            "updated_at": row.updated_at.isoformat().replace("+00:00", "Z"),
            "source": row.source,
            "command_id": row.command_id,
            "command_hash": row.command_hash,
            "source_identity_hash": row.source_identity_hash,
            "publication_generation_id": row.publication_generation_id,
            "publication_receipt_id": row.publication_receipt_id,
            "record_hash": row.record_hash,
        }:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt catalog semantics do not match catalog"
            )
        command_fields = {
            "name": row.name,
            "description": row.description,
            "pool_refs": row.pool_refs,
            "source": row.source,
            "command_id": row.command_id,
        }
        for field_name, expected in command_fields.items():
            if getattr(command, field_name) != expected:
                raise PageProjectionSourceIntegrityError(
                    "canvas publication receipt command payload does not match catalog"
                )
        if claims.catalog_record_hash != row.record_hash:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt record hash does not match catalog"
            )

    @staticmethod
    def _require_tables(connection: duckdb.DuckDBPyConnection) -> None:
        rows = connection.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name IN ('screen_result', 'minute_bar')
            """
        ).fetchall()
        if {str(row[0]) for row in rows} != {"screen_result", "minute_bar"}:
            raise PageProjectionSourceIntegrityError(
                "projection database is missing screen_result or minute_bar"
            )

    @staticmethod
    def _minute_coverage(
        connection: duckdb.DuckDBPyConnection,
        *,
        cutoff: datetime,
    ) -> tuple[MinuteCoverageProjectionRow, ...]:
        rows = connection.execute(
            """
            SELECT COALESCE(source, 'unknown'), COUNT(*), COUNT(DISTINCT ts_code),
                   COUNT(DISTINCT CAST(trade_time AS DATE)), MIN(trade_time), MAX(trade_time)
            FROM minute_bar
            WHERE freq = '1min' AND trade_time <= ? AND created_at <= ?
            GROUP BY COALESCE(source, 'unknown')
            ORDER BY COALESCE(source, 'unknown')
            LIMIT ?
            """,
            (cutoff, cutoff, _MAX_MINUTE_SOURCES + 1),
        ).fetchall()
        if len(rows) > _MAX_MINUTE_SOURCES:
            raise PageProjectionSourceIntegrityError(
                "minute sources exceed the bounded projection limit"
            )
        total = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT ts_code),
                   COUNT(DISTINCT CAST(trade_time AS DATE)), MIN(trade_time), MAX(trade_time)
            FROM minute_bar
            WHERE freq = '1min' AND trade_time <= ? AND created_at <= ?
            """,
            (cutoff, cutoff),
        ).fetchone()
        values: list[MinuteCoverageProjectionRow] = []
        if total is not None and int(total[0]) > 0:
            values.append(
                MinuteCoverageProjectionRow(
                    is_total=True,
                    source="all",
                    rows_count=int(total[0]),
                    codes_count=int(total[1]),
                    trade_dates=int(total[2]),
                    min_time=_database_timestamp(total[3]),
                    max_time=_database_timestamp(total[4]),
                )
            )
        values.extend(
            MinuteCoverageProjectionRow(
                is_total=False,
                source=str(source),
                rows_count=int(count),
                codes_count=int(codes),
                trade_dates=int(trade_dates),
                min_time=_database_timestamp(minimum),
                max_time=_database_timestamp(maximum),
            )
            for source, count, codes, trade_dates, minimum, maximum in rows
        )
        return tuple(values)


class DuckDBLabPageProjectionSource:
    """Project formal research gate metadata from one stable research replica."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(os.path.abspath(database_path))

    def __call__(self, observed_at: datetime, /) -> LabPageProjectionSnapshot:
        observed = normalize_aware_utc(observed_at)
        stable = _StableReadonlyDuckDB(self.database_path)
        with stable as connection:
            self._require_tables(connection)
            candidates = connection.execute(
                """
                SELECT snapshot.snapshot_id, snapshot.strategy_name, snapshot.code_commit,
                       TRY_CAST(json_extract_string(
                           table_watermarks, '$.manifest_start_date') AS DATE),
                       TRY_CAST(json_extract_string(
                           table_watermarks, '$.manifest_end_date') AS DATE),
                       snapshot.as_of_time, snapshot.completed_at, binding.binding_hash,
                       binding.completed_at
                FROM dataset_snapshot AS snapshot
                JOIN dataset_snapshot_binding AS binding USING (snapshot_id)
                WHERE snapshot.status = 'ready' AND binding.status = 'ready'
                  AND snapshot.as_of_time <= ?
                  AND snapshot.completed_at <= ?
                  AND binding.completed_at <= ?
                ORDER BY snapshot.completed_at DESC, snapshot_id
                LIMIT ?
                """,
                (observed, observed, observed, _MAX_RESEARCH_GATES + 1),
            ).fetchall()
            if len(candidates) > _MAX_RESEARCH_GATES:
                raise PageProjectionSourceIntegrityError(
                    "research gates exceed the bounded projection limit"
                )
            rows: list[ResearchGateProjectionRow] = []
            with DuckDBStore(stable.bound_path, read_only=True) as store:
                for (
                    snapshot_id,
                    strategy_name,
                    code_commit,
                    range_start,
                    range_end,
                    as_of_time,
                    snapshot_completed_at,
                    binding_hash,
                    binding_completed_at,
                ) in candidates:
                    if range_start is None or range_end is None:
                        raise PageProjectionSourceIntegrityError(
                            "ready research snapshot lacks its manifest range"
                        )
                    audit_row = connection.execute(
                        """
                        SELECT audit_run_id, completed_at
                        FROM data_audit_run
                        WHERE status = 'completed' AND completed_at <= ?
                          AND as_of_date >= ? AND range_start <= ? AND range_end >= ?
                        ORDER BY as_of_date DESC, completed_at DESC
                        LIMIT 1
                        """,
                        (observed, range_end, range_start, range_end),
                    ).fetchone()
                    audit_run_id = None if audit_row is None else str(audit_row[0])
                    decision = evaluate_store_research_gate(
                        store,
                        ResearchGateRequest(
                            mode="formal",
                            strategy_name=str(strategy_name),
                            start_date=range_start,
                            end_date=range_end,
                            audit_run_id=audit_run_id,
                            dataset_snapshot_id=str(snapshot_id),
                            dataset_binding_hash=str(binding_hash),
                            code_commit=str(code_commit),
                        ),
                        binding_verified=False,
                    )
                    completion_candidates = [
                        _database_timestamp(snapshot_completed_at),
                        _database_timestamp(binding_completed_at),
                    ]
                    if audit_row is not None:
                        completion_candidates.append(_database_timestamp(audit_row[1]))
                    rows.append(
                        ResearchGateProjectionRow(
                            strategy_name=str(strategy_name),
                            range_start=range_start,
                            range_end=range_end,
                            as_of_time=_database_timestamp(as_of_time),
                            completed_at=max(completion_candidates),
                            code_commit=str(code_commit),
                            audit_run_id=decision.audit_run_id,
                            dataset_snapshot_id=decision.dataset_snapshot_id,
                            dataset_binding_hash=decision.dataset_binding_hash,
                            coverage_ratios=decision.coverage_ratios,
                            coverage_counts=decision.coverage_counts,
                            failures=decision.failures,
                            metadata_ready=research_gate_metadata_ready(decision),
                        )
                    )
        available_at = (
            _EMPTY_PROJECTION_AVAILABLE_AT if not rows else max(row.completed_at for row in rows)
        )
        return LabPageProjectionSnapshot.create(
            available_at=available_at,
            rows=tuple(rows),
        )

    @staticmethod
    def _require_tables(connection: duckdb.DuckDBPyConnection) -> None:
        required = {
            "data_audit_run",
            "data_quality_issue",
            "dataset_coverage",
            "dataset_snapshot",
            "dataset_snapshot_binding",
        }
        rows = connection.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ANY(?)
            """,
            (list(sorted(required)),),
        ).fetchall()
        if {str(row[0]) for row in rows} != required:
            raise PageProjectionSourceIntegrityError(
                "research metadata database is missing gate authority tables"
            )


class SignalPageProjectionProducer:
    """Publish the signal-owned page source into notification authority state."""

    def __init__(
        self,
        *,
        source: DuckDBSignalPageProjectionSource,
        store: NotificationStateStore,
        companion_projections: tuple[ServingProjectionPayload, ...] | None = None,
    ) -> None:
        self.source = source
        self.store = store
        if (
            companion_projections is not None
            and {item.table_name for item in companion_projections} != _COMPANION_SIGNAL_TABLES
        ):
            raise ValueError("signal companion projections are incomplete")
        self.companion_projections = companion_projections

    def publish(self, observed_at: datetime) -> NotificationProjectionAuthoritySnapshot:
        observed = normalize_aware_utc(observed_at)
        snapshot = self.source(observed)
        page_source = NotificationProjectionSourceReceipt.create(
            dataset_id="signal-page-projections",
            generation_id=snapshot.content_sha256,
            sequence=int(snapshot.available_at.timestamp() * 1_000_000),
            event_time=snapshot.available_at,
            published_at=observed,
            projections=snapshot.projections,
        )
        if self.companion_projections is None:
            previous = self.store.serving_snapshot(observed_at=observed, history_limit=1)
            previous_by_name = {
                projection.table_name: projection for projection in previous.payload.projections
            }
            companion_projections = tuple(
                previous_by_name.get(table_name)
                or ServingProjectionPayload(
                    table_name=table_name,
                    available_at=_EMPTY_PROJECTION_AVAILABLE_AT,
                    rows=(),
                )
                for table_name in sorted(_COMPANION_SIGNAL_TABLES)
            )
        else:
            companion_projections = self.companion_projections
        companion_identity = {
            "dataset_id": "signal-companion-projections",
            "projections": companion_projections,
        }
        companion_source = NotificationProjectionSourceReceipt.create(
            dataset_id="signal-companion-projections",
            generation_id=canonical_sha256(companion_identity),
            sequence=int(
                max(item.available_at for item in companion_projections).timestamp() * 1_000_000
            ),
            event_time=max(item.available_at for item in companion_projections),
            published_at=observed,
            projections=companion_projections,
        )
        authority = NotificationProjectionAuthoritySnapshot.create_from_sources(
            observed_at=observed,
            sources=(page_source, companion_source),
        )
        self.store.publish_projection_authority(authority)
        return authority


class ScreenBoundsProjectionRow(RuntimeContractModel):
    preset_name: str = Field(min_length=1)
    min_date: date
    max_date: date
    candidate_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.min_date > self.max_date:
            raise ValueError("screen bounds min_date exceeds max_date")
        return self


class MinuteCoverageProjectionRow(RuntimeContractModel):
    is_total: bool
    source: str = Field(min_length=1)
    rows_count: int = Field(ge=0)
    codes_count: int = Field(ge=0)
    trade_dates: int = Field(ge=0)
    min_time: AwareUtcDatetime | None = None
    max_time: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if (self.min_time is None) != (self.max_time is None):
            raise ValueError("minute coverage timestamps must be bound together")
        if (
            self.min_time is not None
            and self.max_time is not None
            and self.min_time > self.max_time
        ):
            raise ValueError("minute coverage min_time exceeds max_time")
        return self


class CanvasDiagnosticProjectionRow(RuntimeContractModel):
    trade_date: date
    preset_name: str = Field(min_length=1)
    step_index: int = Field(ge=0)
    rule_label: str = Field(min_length=1)
    remaining_count: int = Field(ge=0)


class CanvasLatestTradeDateProjectionRow(RuntimeContractModel):
    trade_date: date


class CanvasHitProjectionRow(RuntimeContractModel):
    trade_date: date
    preset_name: str = Field(min_length=1)
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    row_json: str = Field(min_length=2)

    @field_validator("row_json")
    @classmethod
    def validate_row_json(cls, value: str) -> str:
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("canvas hit row_json must contain a JSON object")
        return json.dumps(parsed, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


class CanvasDefinitionProjectionRow(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=128, pattern=r"^[\w\u4e00-\u9fff-]+$")
    description: str = Field(default="", max_length=8_192)
    pool_refs: tuple[str, ...] = Field(default_factory=tuple, max_length=256)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    source: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=128)
    command_hash: Sha256
    source_identity_hash: Sha256
    publication_generation_id: Sha256
    publication_receipt_id: Sha256
    record_hash: Sha256
    version_hash: Sha256

    @model_validator(mode="after")
    def validate_definition(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("canvas definition updated_at precedes created_at")
        expected_source_identity_hash = canvas_source_identity_hash(
            command_id=self.command_id,
            command_hash=self.command_hash,
            source=self.source,
        )
        if self.source_identity_hash != expected_source_identity_hash:
            raise ValueError("canvas definition source identity hash mismatch")
        expected = canonical_sha256(
            {
                "schema_version": self.schema_version,
                "name": self.name,
                "description": self.description,
                "pool_refs": self.pool_refs,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "source": self.source,
                "command_id": self.command_id,
                "command_hash": self.command_hash,
                "source_identity_hash": self.source_identity_hash,
                "publication_generation_id": self.publication_generation_id,
                "publication_receipt_id": self.publication_receipt_id,
                "record_hash": self.record_hash,
            }
        )
        if self.version_hash != expected:
            raise ValueError("canvas definition version hash mismatch")
        return self

    @classmethod
    def from_catalog_record(
        cls,
        *,
        file_name: str,
        raw: object,
        observed: datetime,
    ) -> Self:
        if not isinstance(raw, dict):
            raise PageProjectionSourceIntegrityError("canvas catalog record must be a JSON object")
        allowed = {
            "schema_version",
            "name",
            "description",
            "pool_refs",
            "created_at",
            "updated_at",
            "source",
            "command_id",
            "command_hash",
            "source_identity_hash",
            "publication_generation_id",
            "publication_receipt_id",
            "record_hash",
        }
        if set(raw) != allowed:
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record must bind command identity and record hash"
            )
        if raw.get("schema_version") != _CANVAS_CATALOG_SCHEMA_VERSION:
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record schema version is unsupported"
            )
        record_hash = raw.get("record_hash")
        if not isinstance(record_hash, str):
            raise PageProjectionSourceIntegrityError("canvas catalog record hash is invalid")
        name = raw.get("name")
        if not isinstance(name, str) or file_name != f"{name}.json":
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record name does not match its path"
            )
        pool_refs = raw.get("pool_refs", [])
        if not isinstance(pool_refs, list) or not all(isinstance(item, str) for item in pool_refs):
            raise PageProjectionSourceIntegrityError("canvas catalog record pool_refs are invalid")

        def parse_time(field: str) -> datetime:
            value = raw.get(field)
            if not isinstance(value, str):
                raise PageProjectionSourceIntegrityError(
                    f"canvas catalog record {field} is missing"
                )
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                parsed = normalize_aware_utc(parsed)
            except ValueError as exc:
                raise PageProjectionSourceIntegrityError(
                    f"canvas catalog record {field} is invalid"
                ) from exc
            if parsed > observed:
                raise PageProjectionSourceIntegrityError(
                    "canvas catalog record contains future evidence"
                )
            return parsed

        created_at = parse_time("created_at")
        updated_at = parse_time("updated_at")
        description = raw.get("description", "")
        source = raw.get("source", "page_control")
        command_id = raw.get("command_id")
        command_hash = raw.get("command_hash")
        source_identity_hash = raw.get("source_identity_hash")
        publication_generation_id = raw.get("publication_generation_id")
        publication_receipt_id = raw.get("publication_receipt_id")
        if not all(
            isinstance(value, str)
            for value in (
                description,
                source,
                command_id,
                command_hash,
                source_identity_hash,
                publication_generation_id,
                publication_receipt_id,
            )
        ):
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record has invalid text or command identity fields"
            )
        catalog_record = CanvasPublicationCatalogRecord(
            schema_version=_CANVAS_CATALOG_SCHEMA_VERSION,
            name=name,
            description=description,
            pool_refs=tuple(pool_refs),
            created_at=created_at,
            updated_at=updated_at,
            source=source,
            command_id=command_id,
            command_hash=command_hash,
            source_identity_hash=source_identity_hash,
            publication_generation_id=publication_generation_id,
            publication_receipt_id=publication_receipt_id,
            record_hash=record_hash,
        )
        identity = {
            "schema_version": _CANVAS_CATALOG_SCHEMA_VERSION,
            "name": name,
            "description": description,
            "pool_refs": tuple(pool_refs),
            "created_at": created_at,
            "updated_at": updated_at,
            "source": source,
            "command_id": command_id,
            "command_hash": command_hash,
            "source_identity_hash": source_identity_hash,
            "publication_generation_id": publication_generation_id,
            "publication_receipt_id": publication_receipt_id,
            "record_hash": record_hash,
        }
        if record_hash != canvas_catalog_record_hash(catalog_record):
            raise PageProjectionSourceIntegrityError("canvas catalog record hash mismatch")
        try:
            return cls(**identity, version_hash=canonical_sha256(identity))
        except ValueError as exc:
            raise PageProjectionSourceIntegrityError("canvas catalog record is invalid") from exc


class PulseHistoryProjectionRow(RuntimeContractModel):
    trade_date: date
    as_of: AwareUtcDatetime
    t: str = Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
    limit_up: int = Field(ge=0)
    limit_down: int = Field(ge=0)
    broken: int = Field(ge=0)
    up: int = Field(ge=0)
    down: int = Field(ge=0)
    up_ratio_pct: float | None = None
    total: int = Field(ge=0)


class PulseAlertProjectionRow(RuntimeContractModel):
    trade_date: date
    as_of: AwareUtcDatetime
    t: str = Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
    kind: str = Field(min_length=1, max_length=64)
    kind_label: str = Field(min_length=1, max_length=64)
    before: float
    after: float
    window_minutes: int = Field(ge=1, le=241)
    message: str = Field(min_length=1, max_length=2_048)


class SurgeRuntimeConfigProjectionRow(RuntimeContractModel):
    trade_date: date
    as_of: AwareUtcDatetime
    boards: tuple[str, ...] = Field(min_length=1, max_length=4)
    k_rough: float
    k_cum: float
    ratio_cap: float
    skip_first_minutes: int = Field(ge=0, le=240)
    tushare_rate_per_min: int = Field(ge=1, le=1_000)
    require_price_strength: bool
    max_room_to_limit_pct: float

    @field_validator("boards")
    @classmethod
    def validate_boards(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        allowed = {"main", "gem", "star", "bj"}
        if len(value) != len(set(value)) or not set(value).issubset(allowed):
            raise ValueError("surge runtime config boards are invalid")
        return value


class PulseHistoryProjectionSource(RuntimeContractModel):
    available_at: AwareUtcDatetime
    rows: tuple[PulseHistoryProjectionRow, ...] = Field(max_length=_MAX_PULSE_ROWS)


class PulseAlertProjectionSource(RuntimeContractModel):
    available_at: AwareUtcDatetime
    rows: tuple[PulseAlertProjectionRow, ...] = Field(max_length=_MAX_PULSE_ROWS)


class SurgeRuntimeConfigProjectionSource(RuntimeContractModel):
    available_at: AwareUtcDatetime
    row: SurgeRuntimeConfigProjectionRow


def _source_file_time(
    item: os.stat_result,
    *,
    observed: datetime,
    name: str,
) -> datetime:
    value = datetime.fromtimestamp(item.st_mtime_ns / 1_000_000_000, tz=UTC)
    if value > observed:
        raise PageProjectionSourceIntegrityError(
            f"surge live source {name} contains future file evidence"
        )
    return value


def _parse_jsonl_objects(raw: bytes, *, name: str) -> tuple[dict[str, object], ...]:
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PageProjectionSourceIntegrityError(
            f"surge live source {name} is not valid UTF-8"
        ) from exc
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PageProjectionSourceIntegrityError(
                f"surge live source {name} has invalid JSON at line {line_number}"
            ) from exc
        if not isinstance(value, dict):
            raise PageProjectionSourceIntegrityError(
                f"surge live source {name} line {line_number} must be an object"
            )
        rows.append(value)
        if len(rows) > _MAX_PULSE_ROWS:
            raise PageProjectionSourceIntegrityError(f"surge live source {name} exceeds row bound")
    return tuple(rows)


def _pulse_as_of(trade_date: date, minute: str) -> datetime:
    try:
        parsed_time = time.fromisoformat(minute)
    except ValueError as exc:
        raise PageProjectionSourceIntegrityError("pulse minute is invalid") from exc
    return datetime.combine(trade_date, parsed_time, tzinfo=_SHANGHAI).astimezone(UTC)


def _pulse_history_source_row(
    value: dict[str, object],
    *,
    trade_date: date,
) -> PulseHistoryProjectionRow:
    expected = {
        "t",
        "limit_up",
        "limit_down",
        "broken",
        "up",
        "down",
        "up_ratio_pct",
        "total",
    }
    if set(value) != expected:
        raise PageProjectionSourceIntegrityError("pulse history fields are invalid")
    try:
        return PulseHistoryProjectionRow(
            trade_date=trade_date,
            as_of=_pulse_as_of(trade_date, str(value["t"])),
            **value,
        )
    except ValueError as exc:
        raise PageProjectionSourceIntegrityError("pulse history row is invalid") from exc


def _pulse_alert_source_row(
    value: dict[str, object],
    *,
    trade_date: date,
) -> PulseAlertProjectionRow:
    expected = {
        "t",
        "kind",
        "kind_label",
        "before",
        "after",
        "window_minutes",
        "message",
    }
    if set(value) != expected:
        raise PageProjectionSourceIntegrityError("pulse alert fields are invalid")
    try:
        return PulseAlertProjectionRow(
            trade_date=trade_date,
            as_of=_pulse_as_of(trade_date, str(value["t"])),
            **value,
        )
    except ValueError as exc:
        raise PageProjectionSourceIntegrityError("pulse alert row is invalid") from exc


def _read_surge_live_projection_sources(
    root: Path | None,
    *,
    observed: datetime,
) -> tuple[
    PulseHistoryProjectionSource | None,
    PulseAlertProjectionSource | None,
    SurgeRuntimeConfigProjectionSource | None,
]:
    if root is None:
        return None, None, None
    try:
        binding = _bind_readonly_directory(root, label="surge live projection source")
    except FileNotFoundError:
        return None, None, None
    trade_date = observed.astimezone(_SHANGHAI).date()
    try:
        history_file = _read_bound_optional_file(
            binding,
            f"pulse-{trade_date.isoformat()}.jsonl",
            max_bytes=_MAX_PULSE_FILE_BYTES,
        )
        alert_file = _read_bound_optional_file(
            binding,
            f"pulse_alerts-{trade_date.isoformat()}.jsonl",
            max_bytes=_MAX_ALERT_FILE_BYTES,
        )
        config_file = _read_bound_optional_file(
            binding,
            "runtime_config.json",
            max_bytes=_MAX_RUNTIME_CONFIG_BYTES,
        )
    finally:
        binding.close()

    history_source = None
    if history_file is not None:
        raw, item = history_file
        rows = tuple(
            _pulse_history_source_row(value, trade_date=trade_date)
            for value in _parse_jsonl_objects(
                raw,
                name=f"pulse-{trade_date.isoformat()}.jsonl",
            )
        )
        _source_file_time(item, observed=observed, name="pulse history")
        if any(row.as_of > observed for row in rows):
            raise PageProjectionSourceIntegrityError("pulse history contains future evidence")
        history_source = PulseHistoryProjectionSource(
            available_at=observed,
            rows=rows,
        )

    alert_source = None
    if alert_file is not None:
        raw, item = alert_file
        rows = tuple(
            _pulse_alert_source_row(value, trade_date=trade_date)
            for value in _parse_jsonl_objects(
                raw,
                name=f"pulse_alerts-{trade_date.isoformat()}.jsonl",
            )
        )
        _source_file_time(item, observed=observed, name="pulse alerts")
        if any(row.as_of > observed for row in rows):
            raise PageProjectionSourceIntegrityError("pulse alerts contain future evidence")
        alert_source = PulseAlertProjectionSource(available_at=observed, rows=rows)

    config_source = None
    if config_file is not None:
        raw, item = config_file
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PageProjectionSourceIntegrityError(
                "surge runtime config is not valid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise PageProjectionSourceIntegrityError("surge runtime config must be an object")
        expected_fields = {
            "day",
            "boards",
            "k_rough",
            "k_cum",
            "ratio_cap",
            "skip_first_minutes",
            "tushare_rate_per_min",
            "require_price_strength",
            "max_room_to_limit_pct",
        }
        if set(value) != expected_fields:
            raise PageProjectionSourceIntegrityError(
                "surge runtime config fields do not match the writer contract"
            )
        file_time = _source_file_time(item, observed=observed, name="runtime config")
        try:
            config_day = date.fromisoformat(str(value.pop("day")))
            row = SurgeRuntimeConfigProjectionRow(
                trade_date=config_day,
                as_of=file_time,
                **value,
            )
        except ValueError as exc:
            raise PageProjectionSourceIntegrityError("surge runtime config is invalid") from exc
        if config_day > trade_date:
            raise PageProjectionSourceIntegrityError(
                "surge runtime config contains a future trade date"
            )
        config_source = SurgeRuntimeConfigProjectionSource(
            available_at=observed,
            row=row,
        )
    return history_source, alert_source, config_source


class ResearchGateProjectionRow(RuntimeContractModel):
    strategy_name: str = Field(min_length=1)
    range_start: date
    range_end: date
    as_of_time: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    code_commit: CommitSha
    audit_run_id: str | None = None
    dataset_snapshot_id: str | None = None
    dataset_binding_hash: str | None = None
    coverage_ratios: Mapping[str, float | None]
    coverage_counts: Mapping[str, tuple[int, int]]
    failures: tuple[ResearchGateFailure, ...] = ()
    metadata_ready: bool

    @field_validator("coverage_ratios", "coverage_counts", mode="after")
    @classmethod
    def freeze_mapping(cls, value: Mapping[str, object]) -> Mapping[str, object]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("coverage_ratios", "coverage_counts")
    def serialize_mapping(self, value: Mapping[str, object]) -> dict[str, object]:
        return dict(value)

    @model_validator(mode="after")
    def validate_evidence(self) -> Self:
        if self.range_start > self.range_end:
            raise ValueError("research gate range_start exceeds range_end")
        if self.completed_at < self.as_of_time:
            raise ValueError("research gate completion precedes as_of_time")
        if any(total < covered or covered < 0 for covered, total in self.coverage_counts.values()):
            raise ValueError("research gate coverage counts are invalid")
        return self


class SignalPageProjectionSnapshot(RuntimeContractModel):
    available_at: AwareUtcDatetime
    projections: tuple[ServingProjectionPayload, ...]
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        required_names = {
            "screen_bounds",
            "minute_coverage",
            "canvas_diagnostic",
            "canvas_latest_trade_date",
            "canvas_hit",
            "canvas_definition",
        }
        optional_names = {"pulse_history", "pulse_alert", "surge_runtime_config"}
        published_names = {item.table_name for item in self.projections}
        if not required_names.issubset(published_names) or not published_names.issubset(
            required_names | optional_names
        ):
            raise ValueError("signal page projection snapshot is incomplete")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("signal page projection snapshot hash mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        available_at: datetime,
        screen_bounds: tuple[ScreenBoundsProjectionRow, ...] = (),
        minute_coverage: tuple[MinuteCoverageProjectionRow, ...] = (),
        canvas_diagnostics: tuple[CanvasDiagnosticProjectionRow, ...] = (),
        canvas_latest_trade_date: CanvasLatestTradeDateProjectionRow | None = None,
        canvas_hits: tuple[CanvasHitProjectionRow, ...] = (),
        canvas_definitions: tuple[CanvasDefinitionProjectionRow, ...] = (),
        pulse_history: PulseHistoryProjectionSource | None = None,
        pulse_alerts: PulseAlertProjectionSource | None = None,
        surge_runtime_config: SurgeRuntimeConfigProjectionSource | None = None,
    ) -> SignalPageProjectionSnapshot:
        available = normalize_aware_utc(available_at)
        rows = {
            "screen_bounds": tuple(_screen_bounds_row(row) for row in screen_bounds),
            "minute_coverage": tuple(_minute_coverage_row(row) for row in minute_coverage),
            "canvas_diagnostic": tuple(_canvas_diagnostic_row(row) for row in canvas_diagnostics),
            "canvas_latest_trade_date": (
                ()
                if canvas_latest_trade_date is None
                else (
                    {
                        "snapshot_key": "current",
                        "trade_date": canvas_latest_trade_date.trade_date.isoformat(),
                    },
                )
            ),
            "canvas_hit": tuple(_canvas_hit_row(row) for row in canvas_hits),
            "canvas_definition": tuple(_canvas_definition_row(row) for row in canvas_definitions),
        }
        projections: tuple[ServingProjectionPayload, ...] = tuple(
            ServingProjectionPayload(
                table_name=table_name,
                available_at=available,
                rows=table_rows,
            )
            for table_name, table_rows in sorted(rows.items())
        )
        optional: list[ServingProjectionPayload] = []
        if pulse_history is not None:
            optional.append(
                ServingProjectionPayload(
                    table_name="pulse_history",
                    available_at=pulse_history.available_at,
                    rows=tuple(_pulse_history_row(row) for row in pulse_history.rows),
                )
            )
        if pulse_alerts is not None:
            optional.append(
                ServingProjectionPayload(
                    table_name="pulse_alert",
                    available_at=pulse_alerts.available_at,
                    rows=tuple(_pulse_alert_row(row) for row in pulse_alerts.rows),
                )
            )
        if surge_runtime_config is not None:
            optional.append(
                ServingProjectionPayload(
                    table_name="surge_runtime_config",
                    available_at=surge_runtime_config.available_at,
                    rows=(_surge_runtime_config_row(surge_runtime_config.row),),
                )
            )
        projections = tuple(sorted((*projections, *optional), key=lambda item: item.table_name))
        snapshot_available = max(item.available_at for item in projections)
        identity = {"available_at": snapshot_available, "projections": projections}
        return cls(**identity, content_sha256=canonical_sha256(identity))


class LabPageProjectionSnapshot(RuntimeContractModel):
    available_at: AwareUtcDatetime
    projections: tuple[ServingProjectionPayload, ...]
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        if tuple(item.table_name for item in self.projections) != ("research_gate_metadata",):
            raise ValueError("lab page projection snapshot is incomplete")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("lab page projection snapshot hash mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        available_at: datetime,
        rows: tuple[ResearchGateProjectionRow, ...] = (),
    ) -> LabPageProjectionSnapshot:
        available = normalize_aware_utc(available_at)
        projection = ServingProjectionPayload(
            table_name="research_gate_metadata",
            available_at=available,
            rows=tuple(_research_gate_row(row) for row in rows),
        )
        identity = {"available_at": available, "projections": (projection,)}
        return cls(**identity, content_sha256=canonical_sha256(identity))


def _screen_bounds_row(row: ScreenBoundsProjectionRow) -> dict[str, object]:
    return {
        "preset_name": row.preset_name,
        "min_date": row.min_date.isoformat(),
        "max_date": row.max_date.isoformat(),
        "candidate_count": row.candidate_count,
    }


def _minute_coverage_row(row: MinuteCoverageProjectionRow) -> dict[str, object]:
    return {
        "is_total": row.is_total,
        "source": row.source,
        "rows_count": row.rows_count,
        "codes_count": row.codes_count,
        "trade_dates": row.trade_dates,
        "min_time": None if row.min_time is None else row.min_time.isoformat(),
        "max_time": None if row.max_time is None else row.max_time.isoformat(),
    }


def _canvas_diagnostic_row(row: CanvasDiagnosticProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "preset_name": row.preset_name,
        "step_index": row.step_index,
        "rule_label": row.rule_label,
        "remaining_count": row.remaining_count,
    }


def _canvas_hit_row(row: CanvasHitProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "preset_name": row.preset_name,
        "ts_code": row.ts_code,
        "row_json": row.row_json,
    }


def _canvas_definition_row(row: CanvasDefinitionProjectionRow) -> dict[str, object]:
    return {
        "name": row.name,
        "description": row.description,
        "pool_refs_json": json.dumps(list(row.pool_refs), ensure_ascii=True, separators=(",", ":")),
        "created_at": row.created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": row.updated_at.isoformat().replace("+00:00", "Z"),
        "source": row.source,
        "command_id": row.command_id,
        "command_hash": row.command_hash,
        "source_identity_hash": row.source_identity_hash,
        "record_hash": row.record_hash,
        "version_hash": row.version_hash,
    }


def _pulse_history_row(row: PulseHistoryProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "as_of": row.as_of.isoformat(),
        "t": row.t,
        "limit_up": row.limit_up,
        "limit_down": row.limit_down,
        "broken": row.broken,
        "up": row.up,
        "down": row.down,
        "up_ratio_pct": row.up_ratio_pct,
        "total": row.total,
    }


def _pulse_alert_row(row: PulseAlertProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "as_of": row.as_of.isoformat(),
        "t": row.t,
        "kind": row.kind,
        "kind_label": row.kind_label,
        "before": row.before,
        "after": row.after,
        "window_minutes": row.window_minutes,
        "message": row.message,
    }


def _surge_runtime_config_row(
    row: SurgeRuntimeConfigProjectionRow,
) -> dict[str, object]:
    return {
        "snapshot_key": "current",
        "trade_date": row.trade_date.isoformat(),
        "as_of": row.as_of.isoformat(),
        "boards_json": json.dumps(list(row.boards), ensure_ascii=True, separators=(",", ":")),
        "k_rough": row.k_rough,
        "k_cum": row.k_cum,
        "ratio_cap": row.ratio_cap,
        "skip_first_minutes": row.skip_first_minutes,
        "tushare_rate_per_min": row.tushare_rate_per_min,
        "require_price_strength": row.require_price_strength,
        "max_room_to_limit_pct": row.max_room_to_limit_pct,
    }


def _research_gate_row(row: ResearchGateProjectionRow) -> dict[str, object]:
    return {
        "strategy_name": row.strategy_name,
        "range_start": row.range_start.isoformat(),
        "range_end": row.range_end.isoformat(),
        "as_of_time": row.as_of_time.isoformat(),
        "completed_at": row.completed_at.isoformat(),
        "code_commit": row.code_commit,
        "audit_run_id": row.audit_run_id,
        "dataset_snapshot_id": row.dataset_snapshot_id,
        "dataset_binding_hash": row.dataset_binding_hash,
        "coverage_ratios_json": json.dumps(
            dict(row.coverage_ratios), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ),
        "coverage_counts_json": json.dumps(
            dict(row.coverage_counts), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ),
        "failures_json": json.dumps(
            [item.model_dump(mode="json") for item in row.failures],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "metadata_ready": row.metadata_ready,
    }


__all__ = [
    "CanvasDiagnosticProjectionRow",
    "CanvasDefinitionProjectionRow",
    "CanvasHitProjectionRow",
    "CanvasLatestTradeDateProjectionRow",
    "DuckDBLabPageProjectionSource",
    "DuckDBSignalPageProjectionSource",
    "LabPageProjectionSnapshot",
    "MinuteCoverageProjectionRow",
    "PulseAlertProjectionRow",
    "PulseAlertProjectionSource",
    "PulseHistoryProjectionRow",
    "PulseHistoryProjectionSource",
    "ResearchGateProjectionRow",
    "ScreenBoundsProjectionRow",
    "SignalPageProjectionProducer",
    "SignalPageProjectionSnapshot",
    "SurgeRuntimeConfigProjectionRow",
    "SurgeRuntimeConfigProjectionSource",
]
