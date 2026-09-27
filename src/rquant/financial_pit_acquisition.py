"""Bounded Tushare financial observations sealed as immutable local batches."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

FinancialAPI = Literal[
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express", "dividend"
]
FinancialStatus = Literal["observed", "empty", "possibly_truncated"]
FinancialScalar = str | int | float | None

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DATE_PATTERN = re.compile(r"^[0-9]{8}$")
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_OFFICIAL_ROW_CAP = {"fina_indicator": 100, "forecast": 3500, "dividend": 2000}
_ANNOUNCEMENT_WINDOW_APIS = frozenset({"income", "balancesheet", "cashflow", "forecast", "express"})
_REQUIRED_COLUMNS = {
    "fina_indicator": frozenset({"ts_code", "end_date", "ann_date"}),
    "income": frozenset({"ts_code", "ann_date", "f_ann_date", "end_date", "report_type"}),
    "balancesheet": frozenset({"ts_code", "ann_date", "f_ann_date", "end_date", "report_type"}),
    "cashflow": frozenset({"ts_code", "ann_date", "f_ann_date", "end_date", "report_type"}),
    "forecast": frozenset({"ts_code", "ann_date", "end_date", "type"}),
    "express": frozenset({"ts_code", "ann_date", "end_date"}),
    "dividend": frozenset({"ts_code", "ann_date", "end_date", "div_proc"}),
}
_MANIFEST_NAME = "manifest.sqlite3"
_IDENTITY_NAME = "archive.identity.json"
_ANCHOR_NAME = "manifest.anchor.jsonl"
_MAX_SNAPSHOT_BYTES = 64_000_000
_MAX_ANCHOR_BYTES = 16_000_000
_MAX_ANCHOR_LINE_BYTES = 1024


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observation clock must return a timezone-aware datetime")
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError("observation clock is outside the supported range") from exc


def _date_text(value: date) -> str:
    return value.strftime("%Y%m%d")


def _parse_supplier_date(value: FinancialScalar) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or _DATE_PATTERN.fullmatch(value) is None:
        raise ValueError("supplier date must be an eight-digit YYYYMMDD string")
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid supplier date {value!r}") from exc


class _Model(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )


class FinancialQuery(_Model):
    request_id: UUID
    api: FinancialAPI
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    period: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    ann_date: date | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> FinancialQuery:
        if self.api == "fina_indicator":
            if self.period is None or any(
                value is not None for value in (self.start_date, self.end_date, self.ann_date)
            ):
                raise ValueError("fina_indicator requires only a single period")
        elif self.api == "dividend":
            if self.ann_date is None or any(
                value is not None for value in (self.period, self.start_date, self.end_date)
            ):
                raise ValueError("dividend requires only a single ann_date")
        elif (
            self.start_date is None
            or self.end_date is None
            or self.period is not None
            or self.ann_date is not None
        ):
            raise ValueError(f"{self.api} requires only an announcement date window")
        return self

    def supplier_parameters(self) -> dict[str, str]:
        if self.api == "fina_indicator":
            assert self.period is not None
            return {"ts_code": self.ts_code, "period": _date_text(self.period)}
        if self.api == "dividend":
            assert self.ann_date is not None
            return {"ts_code": self.ts_code, "ann_date": _date_text(self.ann_date)}
        assert self.start_date is not None and self.end_date is not None
        return {
            "ts_code": self.ts_code,
            "start_date": _date_text(self.start_date),
            "end_date": _date_text(self.end_date),
        }


class AcquisitionLimits(_Model):
    max_requests: int = Field(default=32, gt=0, le=1000)
    max_symbols: int = Field(default=16, gt=0, le=1000)
    max_rows_per_response: int = Field(default=5000, gt=0, le=10000)
    max_batch_bytes: int = Field(default=8_000_000, gt=0, le=32_000_000)
    max_field_chars: int = Field(default=8192, gt=0, le=65_536)
    max_columns: int = Field(default=256, gt=0, le=1024)


class FinancialRawRow(_Model):
    values: dict[str, FinancialScalar]
    pit_usable: bool
    conflicted: bool = False
    content_sha256: str = Field(pattern=_SHA256_PATTERN)


class FinancialBatch(_Model):
    schema_version: Literal[1] = 1
    query: FinancialQuery
    observed_at: datetime
    status: FinancialStatus
    row_count: int
    rows: tuple[FinancialRawRow, ...]

    @model_validator(mode="after")
    def validate_content(self) -> FinancialBatch:
        if self.observed_at.utcoffset() != timedelta(0):
            raise ValueError("batch observation must be UTC")
        if self.row_count != len(self.rows):
            raise ValueError("batch row count mismatch")
        if (self.row_count == 0) != (self.status == "empty"):
            raise ValueError("batch status does not match row count")
        return self


class FinancialReceipt(_Model):
    query: FinancialQuery
    observed_at: datetime
    status: FinancialStatus
    row_count: int
    relative_path: str
    file_sha256: str = Field(pattern=_SHA256_PATTERN)
    byte_count: int = Field(gt=0)


class FinancialObservedVersion(_Model):
    query: FinancialQuery
    row: FinancialRawRow
    first_observed_at: datetime
    first_response_status: FinancialStatus


class _AnchorRecord(_Model):
    archive_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=0)
    snapshot_sha256: str = Field(pattern=_SHA256_PATTERN)
    high_water: str | None
    batch_count: int = Field(ge=0)
    previous_record_sha256: str | None = Field(pattern=_SHA256_PATTERN)


@dataclass
class _ArchiveState:
    root_fd: int
    identity_fd: int
    snapshot_fd: int
    anchor_fd: int
    connection: sqlite3.Connection
    archive_id: str
    snapshot_sha256: str
    anchor: _AnchorRecord
    anchor_record_sha256: str


def _validate_query_for_day(query: FinancialQuery, run_day: date) -> None:
    if type(run_day) is not date:
        raise ValueError("run_day must be a date")
    if query.api in _ANNOUNCEMENT_WINDOW_APIS:
        assert query.start_date is not None and query.end_date is not None
        if query.start_date > query.end_date:
            raise ValueError("announcement window must have ordered bounds")
        if (query.end_date - query.start_date).days >= 31:
            raise ValueError("announcement window may span at most 31 civil days")
        if query.end_date > run_day:
            raise ValueError("announcement window cannot end after run_day")
    elif query.api == "dividend":
        assert query.ann_date is not None
        if query.ann_date > run_day:
            raise ValueError("dividend announcement cannot be in the future")
    else:
        assert query.period is not None
        if query.period > run_day:
            raise ValueError("report period cannot be in the future")


def _normalize_scalar(value: object, max_field_chars: int) -> FinancialScalar:
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) > max_field_chars:
            raise ValueError("supplier field exceeds character limit")
        return value
    if isinstance(value, bool):
        raise TypeError("supplier booleans are not financial scalar values")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("supplier numeric field must be finite")
        return value
    raise TypeError(f"unsupported supplier field type: {type(value).__name__}")


def _logical_key(api: FinancialAPI, values: Mapping[str, FinancialScalar]) -> tuple[str, ...]:
    report_type = (
        values.get("report_type") if api in {"income", "balancesheet", "cashflow"} else None
    )
    if api == "forecast":
        report_type = values.get("type")
    return (api, str(values.get("ts_code")), str(values.get("end_date")), str(report_type or ""))


def _normalized_rows(
    query: FinancialQuery, response: object, limits: AcquisitionLimits
) -> tuple[FinancialRawRow, ...]:
    if not isinstance(response, pd.DataFrame):
        raise TypeError("Tushare financial response must be a DataFrame")
    if len(response) > limits.max_rows_per_response:
        raise ValueError("supplier response exceeds local row limit")
    official_cap = _OFFICIAL_ROW_CAP.get(query.api)
    if official_cap is not None and len(response) > official_cap:
        raise ValueError("supplier response exceeds documented interface limit")
    columns = list(response.columns)
    if len(columns) > limits.max_columns:
        raise ValueError("supplier response exceeds local column limit")
    if any(not isinstance(column, str) for column in columns) or len(set(columns)) != len(columns):
        raise ValueError("supplier response columns must be unique strings")
    if any(len(column) > limits.max_field_chars for column in columns):
        raise ValueError("supplier column name exceeds character limit")
    if len(response) and not _REQUIRED_COLUMNS[query.api].issubset(columns):
        raise ValueError("supplier response lacks required columns")

    rows: list[FinancialRawRow] = []
    value_bytes_seen = 0
    for source_values in response.itertuples(index=False, name=None):
        values = {
            name: _normalize_scalar(value, limits.max_field_chars)
            for name, value in zip(columns, source_values, strict=True)
        }
        value_bytes = _json_bytes(values)
        value_bytes_seen += len(value_bytes)
        if value_bytes_seen > limits.max_batch_bytes:
            raise ValueError("supplier response exceeds local batch byte limit")
        if values["ts_code"] != query.ts_code:
            raise ValueError("supplier row belongs to a different security")
        parsed_dates = {
            name: _parse_supplier_date(value)
            for name, value in values.items()
            if name.endswith("_date")
        }
        ann_date = parsed_dates.get("ann_date")
        end_date = parsed_dates.get("end_date")
        if query.api == "fina_indicator":
            if end_date is not None and end_date != query.period:
                raise ValueError("supplier report period is outside the request")
        elif query.api == "dividend":
            if ann_date is not None and ann_date != query.ann_date:
                raise ValueError("supplier announcement is outside the request")
        elif ann_date is not None and not query.start_date <= ann_date <= query.end_date:
            raise ValueError("supplier announcement is outside the request")

        type_field = "report_type" if query.api in {"income", "balancesheet", "cashflow"} else None
        if query.api == "forecast":
            type_field = "type"
        if query.api == "dividend":
            type_field = "div_proc"
        pit_usable = ann_date is not None and end_date is not None
        if type_field is not None:
            type_value = values.get(type_field)
            if type_value is not None and not isinstance(type_value, str):
                raise TypeError(f"supplier {type_field} must be a string")
            if not type_value:
                pit_usable = False
        rows.append(
            FinancialRawRow(
                values=values,
                pit_usable=pit_usable,
                content_sha256=_sha256(value_bytes),
            )
        )

    hashes_by_key: dict[tuple[str, ...], set[str]] = {}
    for row in rows:
        key = _logical_key(query.api, row.values)
        hashes_by_key.setdefault(key, set()).add(row.content_sha256)
    return tuple(
        row.model_copy(update={"pit_usable": False, "conflicted": True})
        if len(hashes_by_key[_logical_key(query.api, row.values)]) > 1
        else row
        for row in rows
    )


def _open_directory(path: Path, *, create_last: bool = False) -> int:
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("archive directory must be an absolute path without traversal")
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open("/", flags)
    try:
        components = path.parts[1:]
        for index, component in enumerate(components):
            if create_last and index == len(components) - 1:
                with suppress(FileExistsError):
                    os.mkdir(component, 0o700, dir_fd=descriptor)
            next_descriptor = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except OSError as exc:
        os.close(descriptor)
        raise ValueError(f"archive path is not a safe directory: {path}") from exc


def _open_child_directory(parent_fd: int, name: str, *, create: bool = False) -> int:
    if create:
        with suppress(FileExistsError):
            os.mkdir(name, 0o700, dir_fd=parent_fd)
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        if opened.st_uid != os.getuid() or opened.st_mode & 0o022:
            os.close(descriptor)
            raise ValueError(f"archive child has unsafe permissions: {name}")
        return descriptor
    except OSError as exc:
        raise ValueError(f"archive child is not a safe directory: {name}") from exc


def _write_all(descriptor: int, data: bytes) -> None:
    written = 0
    while written < len(data):
        count = os.write(descriptor, data[written:])
        if count <= 0:
            raise OSError("archive write made no progress")
        written += count


def _safe_regular_stat(root_fd: int, name: str, descriptor: int) -> os.stat_result:
    opened = os.fstat(descriptor)
    named = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or opened.st_mode & 0o022
        or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        or not stat.S_ISREG(named.st_mode)
        or named.st_nlink != 1
    ):
        raise ValueError(f"archive {name} is not a safe single-link regular file")
    return opened


def _open_regular(
    root_fd: int, name: str, *, max_bytes: int, writable: bool = False
) -> tuple[int, bytes]:
    flags = (os.O_RDWR | os.O_APPEND if writable else os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=root_fd)
    except OSError as exc:
        raise ValueError(f"archive {name} cannot be safely opened") from exc
    try:
        opened = _safe_regular_stat(root_fd, name, descriptor)
        if opened.st_size > max_bytes:
            raise ValueError(f"archive {name} exceeds its byte limit")
        data = bytearray()
        while len(data) <= max_bytes:
            chunk = os.read(descriptor, min(1_048_576, max_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        if len(data) != opened.st_size or len(data) > max_bytes:
            raise ValueError(f"archive {name} changed during read")
        after = os.fstat(descriptor)
        if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError(f"archive {name} changed during read")
        return descriptor, bytes(data)
    except BaseException:
        os.close(descriptor)
        raise


def _create_regular(root_fd: int, name: str, data: bytes) -> None:
    descriptor = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=root_fd,
    )
    try:
        _write_all(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _anchor_line(record: _AnchorRecord) -> bytes:
    line = _json_bytes(record.model_dump(mode="json")) + b"\n"
    if len(line) > _MAX_ANCHOR_LINE_BYTES:
        raise ValueError("anchor record exceeds its byte limit")
    return line


def _parse_anchor(data: bytes) -> tuple[_AnchorRecord, str, int]:
    if not data or len(data) > _MAX_ANCHOR_BYTES:
        raise ValueError("archive anchor is missing or exceeds its byte limit")
    complete_size = data.rfind(b"\n") + 1
    if complete_size == 0 or len(data) - complete_size > _MAX_ANCHOR_LINE_BYTES:
        raise ValueError("archive anchor has no complete bounded record")
    previous: _AnchorRecord | None = None
    previous_digest: str | None = None
    for line in data[:complete_size].splitlines(keepends=True):
        if len(line) > _MAX_ANCHOR_LINE_BYTES:
            raise ValueError("anchor record exceeds its byte limit")
        try:
            record = _AnchorRecord.model_validate_json(line[:-1], strict=True)
        except Exception as exc:
            raise ValueError("archive anchor has an invalid record") from exc
        if _anchor_line(record) != line:
            raise ValueError("archive anchor record is not canonical")
        if previous is None:
            if (
                record.generation != 0
                or record.high_water is not None
                or record.batch_count != 0
                or record.previous_record_sha256 is not None
            ):
                raise ValueError("archive anchor has an invalid genesis")
        elif (
            record.archive_id != previous.archive_id
            or record.generation != previous.generation + 1
            or record.previous_record_sha256 != previous_digest
            or record.batch_count != previous.batch_count + 1
            or record.high_water is None
            or (previous.high_water is not None and record.high_water <= previous.high_water)
        ):
            raise ValueError("archive anchor chain or high-water is invalid")
        if record.high_water is not None:
            try:
                at = datetime.fromisoformat(record.high_water)
            except ValueError as exc:
                raise ValueError("archive anchor high-water is invalid") from exc
            if (
                at.tzinfo is None
                or at.utcoffset() != timedelta(0)
                or record.high_water != at.isoformat(timespec="microseconds")
            ):
                raise ValueError("archive anchor high-water is not canonical UTC")
        previous, previous_digest = record, _sha256(line[:-1])
    assert previous is not None and previous_digest is not None
    return previous, previous_digest, complete_size


class FinancialArchive:
    """Immutable batches and a locked, anchored SQLite state snapshot."""

    def __init__(self, root: Path, *, limits: AcquisitionLimits | None = None) -> None:
        self.root = Path(root)
        self.limits = limits or AcquisitionLimits()
        root_existed = self.root.exists() or self.root.is_symlink()
        root_fd = _open_directory(self.root, create_last=True)
        os.close(root_fd)
        self._initialize_database(create=not root_existed)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _locked_root(self) -> Iterator[int]:
        root_fd = _open_directory(self.root)
        try:
            opened = os.fstat(root_fd)
            if opened.st_uid != os.getuid() or opened.st_mode & 0o022:
                raise ValueError("archive root has unsafe owner or permissions")
            fcntl.flock(root_fd, fcntl.LOCK_EX)
            self._check_root(root_fd)
            yield root_fd
        finally:
            os.close(root_fd)

    def _check_root(self, root_fd: int) -> None:
        opened = os.fstat(root_fd)
        named = os.stat(self.root, follow_symlinks=False)
        if (
            not stat.S_ISDIR(named.st_mode)
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_uid != os.getuid()
            or opened.st_mode & 0o022
        ):
            raise ValueError("archive root path changed or has unsafe permissions")

    def _reject_sqlite_sidecars(self, root_fd: int) -> None:
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                os.stat(_MANIFEST_NAME + suffix, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise ValueError("unsafe SQLite sidecar exists in archive")

    def _initialize_database(self, *, create: bool = False) -> None:
        if not create:
            with self._state():
                return
        with self._locked_root() as root_fd:
            batches_fd = _open_child_directory(root_fd, "batches", create=True)
            os.close(batches_fd)
            archive_id = uuid4().hex
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """CREATE TABLE batch_manifest (
                        request_id TEXT PRIMARY KEY,
                        query_json TEXT NOT NULL,
                        observed_at TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL,
                        row_count INTEGER NOT NULL,
                        relative_path TEXT NOT NULL UNIQUE,
                        file_sha256 TEXT NOT NULL,
                        byte_count INTEGER NOT NULL
                    )"""
                )
                connection.execute(
                    """CREATE TABLE clock_high_water (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        observed_at TEXT NOT NULL
                    )"""
                )
                connection.execute(
                    """CREATE TABLE archive_meta (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        archive_id TEXT NOT NULL,
                        schema_version INTEGER NOT NULL,
                        generation INTEGER NOT NULL,
                        previous_snapshot_sha256 TEXT
                    )"""
                )
                connection.execute(
                    """INSERT INTO archive_meta
                       (singleton, archive_id, schema_version, generation, previous_snapshot_sha256)
                       VALUES (1, ?, 1, 0, NULL)""",
                    (archive_id,),
                )
                connection.commit()
                snapshot = connection.serialize()
            finally:
                connection.close()
            identity = _json_bytes({"archive_id": archive_id, "schema_version": 1}) + b"\n"
            anchor = _AnchorRecord(
                archive_id=archive_id,
                generation=0,
                snapshot_sha256=_sha256(snapshot),
                high_water=None,
                batch_count=0,
                previous_record_sha256=None,
            )
            _create_regular(root_fd, _IDENTITY_NAME, identity)
            _create_regular(root_fd, _MANIFEST_NAME, snapshot)
            _create_regular(root_fd, _ANCHOR_NAME, _anchor_line(anchor))
            os.fsync(root_fd)

    def _manifest_metadata(
        self, connection: sqlite3.Connection, archive_id: str
    ) -> tuple[int, str | None, int, str | None]:
        try:
            meta = connection.execute("SELECT * FROM archive_meta WHERE singleton = 1").fetchone()
            high = connection.execute(
                "SELECT observed_at FROM clock_high_water WHERE singleton = 1"
            ).fetchone()
            count, newest = connection.execute(
                "SELECT COUNT(*), MAX(observed_at) FROM batch_manifest"
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise ValueError("archive manifest schema is invalid") from exc
        if meta is None or meta["archive_id"] != archive_id or meta["schema_version"] != 1:
            raise ValueError("archive manifest identity or schema is invalid")
        high_water = None if high is None else high["observed_at"]
        if high_water != newest or (count == 0) != (high_water is None):
            raise ValueError("manifest and clock high-water disagree")
        if high_water is not None:
            try:
                parsed = datetime.fromisoformat(high_water)
            except (TypeError, ValueError) as exc:
                raise ValueError("manifest high-water is invalid") from exc
            if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
                raise ValueError("manifest high-water must be UTC")
        return meta["generation"], high_water, count, meta["previous_snapshot_sha256"]

    @contextmanager
    def _state(self) -> Iterator[_ArchiveState]:
        with self._locked_root() as root_fd:
            identity_fd = snapshot_fd = anchor_fd = -1
            connection: sqlite3.Connection | None = None
            try:
                self._reject_sqlite_sidecars(root_fd)
                batches_fd = _open_child_directory(root_fd, "batches")
                os.close(batches_fd)
                identity_fd, identity_bytes = _open_regular(root_fd, _IDENTITY_NAME, max_bytes=256)
                try:
                    identity = json.loads(identity_bytes)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError("archive identity is invalid") from exc
                if (
                    not isinstance(identity, dict)
                    or set(identity) != {"archive_id", "schema_version"}
                    or not isinstance(identity["archive_id"], str)
                    or re.fullmatch(r"[0-9a-f]{32}", identity["archive_id"]) is None
                    or identity["schema_version"] != 1
                    or identity_bytes != _json_bytes(identity) + b"\n"
                ):
                    raise ValueError("archive identity is invalid")
                snapshot_fd, snapshot = _open_regular(
                    root_fd, _MANIFEST_NAME, max_bytes=_MAX_SNAPSHOT_BYTES
                )
                anchor_fd, anchor_bytes = _open_regular(
                    root_fd, _ANCHOR_NAME, max_bytes=_MAX_ANCHOR_BYTES, writable=True
                )
                anchor, anchor_digest, complete_size = _parse_anchor(anchor_bytes)
                if complete_size < len(anchor_bytes):
                    os.ftruncate(anchor_fd, complete_size)
                    os.fsync(anchor_fd)
                if anchor.archive_id != identity["archive_id"]:
                    raise ValueError("archive anchor identity is invalid")
                connection = self._connect()
                try:
                    connection.deserialize(snapshot)
                except (sqlite3.DatabaseError, MemoryError, ValueError) as exc:
                    raise ValueError("archive snapshot is corrupt") from exc
                generation, high_water, count, previous_snapshot = self._manifest_metadata(
                    connection, identity["archive_id"]
                )
                snapshot_digest = _sha256(snapshot)
                if snapshot_digest == anchor.snapshot_sha256:
                    if (
                        generation != anchor.generation
                        or high_water != anchor.high_water
                        or count != anchor.batch_count
                    ):
                        raise ValueError("archive snapshot and anchor disagree")
                    os.fsync(anchor_fd)
                elif (
                    generation == anchor.generation + 1
                    and previous_snapshot == anchor.snapshot_sha256
                    and count == anchor.batch_count + 1
                    and high_water is not None
                    and (anchor.high_water is None or high_water > anchor.high_water)
                ):
                    newest = connection.execute(
                        "SELECT * FROM batch_manifest ORDER BY observed_at DESC LIMIT 1"
                    ).fetchone()
                    if newest is None:
                        raise ValueError("archive recovery lacks its new batch")
                    self._load_batch(self._receipt_from_row(newest), root_fd=root_fd)
                    recovered = _AnchorRecord(
                        archive_id=identity["archive_id"],
                        generation=generation,
                        snapshot_sha256=snapshot_digest,
                        high_water=high_water,
                        batch_count=count,
                        previous_record_sha256=anchor_digest,
                    )
                    self._check_root(root_fd)
                    _safe_regular_stat(root_fd, _IDENTITY_NAME, identity_fd)
                    _safe_regular_stat(root_fd, _MANIFEST_NAME, snapshot_fd)
                    _safe_regular_stat(root_fd, _ANCHOR_NAME, anchor_fd)
                    os.fsync(root_fd)
                    self._append_anchor(anchor_fd, recovered)
                    anchor, anchor_digest = recovered, _sha256(_anchor_line(recovered)[:-1])
                else:
                    raise ValueError("archive snapshot rollback or anchor mismatch")
                yield _ArchiveState(
                    root_fd=root_fd,
                    identity_fd=identity_fd,
                    snapshot_fd=snapshot_fd,
                    anchor_fd=anchor_fd,
                    connection=connection,
                    archive_id=identity["archive_id"],
                    snapshot_sha256=snapshot_digest,
                    anchor=anchor,
                    anchor_record_sha256=anchor_digest,
                )
            finally:
                if connection is not None:
                    connection.close()
                for descriptor in (identity_fd, snapshot_fd, anchor_fd):
                    if descriptor >= 0:
                        os.close(descriptor)

    def _append_anchor(self, anchor_fd: int, record: _AnchorRecord) -> None:
        line = _anchor_line(record)
        if os.fstat(anchor_fd).st_size + len(line) > _MAX_ANCHOR_BYTES:
            raise ValueError("archive anchor is full")
        _write_all(anchor_fd, line)
        os.fsync(anchor_fd)

    def _receipt_from_row(self, row: sqlite3.Row) -> FinancialReceipt:
        receipt = FinancialReceipt(
            query=FinancialQuery.model_validate_json(row["query_json"]),
            observed_at=datetime.fromisoformat(row["observed_at"]),
            status=row["status"],
            row_count=row["row_count"],
            relative_path=row["relative_path"],
            file_sha256=row["file_sha256"],
            byte_count=row["byte_count"],
        )
        if row["request_id"] != str(receipt.query.request_id):
            raise ValueError("manifest request identity disagrees with sealed query")
        expected = f"batches/{receipt.query.request_id}.json"
        if receipt.relative_path != expected:
            raise ValueError("manifest path does not match request identity")
        if receipt.byte_count > self.limits.max_batch_bytes:
            raise ValueError("manifest batch exceeds local byte limit")
        return receipt

    def _read_receipt(self, request_id: UUID) -> FinancialReceipt | None:
        with self._state() as state:
            row = state.connection.execute(
                "SELECT * FROM batch_manifest WHERE request_id = ?", (str(request_id),)
            ).fetchone()
            if row is not None:
                return self._receipt_from_row(row)
            batches_fd = _open_child_directory(state.root_fd, "batches")
            try:
                try:
                    os.stat(f"{request_id}.json", dir_fd=batches_fd, follow_symlinks=False)
                except FileNotFoundError:
                    return None
                except OSError as exc:
                    raise ValueError("uncommitted batch target cannot be inspected") from exc
            finally:
                os.close(batches_fd)
            raise ValueError("uncommitted batch target already exists; request is pending")

    def receipt(self, request_id: UUID) -> FinancialReceipt:
        with self._state() as state:
            row = state.connection.execute(
                "SELECT * FROM batch_manifest WHERE request_id = ?", (str(request_id),)
            ).fetchone()
            if row is None:
                raise KeyError(request_id)
            receipt = self._receipt_from_row(row)
            self._load_batch(receipt, root_fd=state.root_fd)
            return receipt

    def read_batch(self, request_id: UUID) -> FinancialBatch:
        with self._state() as state:
            row = state.connection.execute(
                "SELECT * FROM batch_manifest WHERE request_id = ?", (str(request_id),)
            ).fetchone()
            if row is None:
                raise KeyError(request_id)
            return self._load_batch(self._receipt_from_row(row), root_fd=state.root_fd)

    def list_committed(self) -> tuple[FinancialBatch, ...]:
        with self._state() as state:
            rows = state.connection.execute(
                "SELECT * FROM batch_manifest ORDER BY observed_at"
            ).fetchall()
            receipts = tuple(self._receipt_from_row(row) for row in rows)
            return tuple(self._load_batch(receipt, root_fd=state.root_fd) for receipt in receipts)

    def _load_batch(self, receipt: FinancialReceipt, *, root_fd: int) -> FinancialBatch:
        try:
            batches_fd = _open_child_directory(root_fd, "batches")
            try:
                descriptor = os.open(
                    f"{receipt.query.request_id}.json",
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=batches_fd,
                )
                try:
                    before = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(before.st_mode)
                        or before.st_nlink != 1
                        or before.st_size != receipt.byte_count
                    ):
                        raise ValueError("committed batch is not an intact regular file")
                    data = b""
                    while len(data) <= receipt.byte_count:
                        chunk = os.read(descriptor, receipt.byte_count + 1 - len(data))
                        if not chunk:
                            break
                        data += chunk
                    after = os.fstat(descriptor)
                    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
                        raise ValueError("committed batch changed during read")
                finally:
                    os.close(descriptor)
            finally:
                os.close(batches_fd)
        except OSError as exc:
            raise ValueError("committed batch cannot be safely opened") from exc
        if len(data) != receipt.byte_count or _sha256(data) != receipt.file_sha256:
            raise ValueError("committed batch length or digest mismatch")
        try:
            batch = FinancialBatch.model_validate_json(data, strict=True)
        except Exception as exc:
            raise ValueError("committed batch has invalid format") from exc
        if _json_bytes(batch.model_dump(mode="json")) != data:
            raise ValueError("committed batch is not canonical JSON")
        if (
            batch.query != receipt.query
            or batch.observed_at != receipt.observed_at
            or batch.status != receipt.status
            or batch.row_count != receipt.row_count
        ):
            raise ValueError("committed batch disagrees with manifest")
        for row in batch.rows:
            if _sha256(_json_bytes(row.values)) != row.content_sha256:
                raise ValueError("committed row digest mismatch")
        return batch

    def _publish_file(self, root_fd: int, request_id: UUID, data: bytes) -> None:
        batches_fd = _open_child_directory(root_fd, "batches")
        try:
            temporary = f".tmp-{uuid4().hex}"
            target = f"{request_id}.json"
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=batches_fd,
            )
            try:
                _write_all(descriptor, data)
                os.fsync(descriptor)
                try:
                    os.link(
                        temporary,
                        target,
                        src_dir_fd=batches_fd,
                        dst_dir_fd=batches_fd,
                        follow_symlinks=False,
                    )
                except FileExistsError as exc:
                    raise ValueError("batch target already exists; refusing overwrite") from exc
                os.fsync(batches_fd)
            finally:
                os.close(descriptor)
                os.unlink(temporary, dir_fd=batches_fd)
                os.fsync(batches_fd)
        finally:
            os.close(batches_fd)

    def _publish_snapshot(self, state: _ArchiveState, snapshot: bytes) -> None:
        if len(snapshot) > _MAX_SNAPSHOT_BYTES:
            raise ValueError("archive snapshot exceeds its byte limit")
        temporary = f".manifest-{uuid4().hex}.tmp"
        try:
            _create_regular(state.root_fd, temporary, snapshot)
            self._check_root(state.root_fd)
            _safe_regular_stat(state.root_fd, _IDENTITY_NAME, state.identity_fd)
            _safe_regular_stat(state.root_fd, _ANCHOR_NAME, state.anchor_fd)
            _safe_regular_stat(state.root_fd, _MANIFEST_NAME, state.snapshot_fd)
            self._reject_sqlite_sidecars(state.root_fd)
            os.replace(
                temporary,
                _MANIFEST_NAME,
                src_dir_fd=state.root_fd,
                dst_dir_fd=state.root_fd,
            )
            os.fsync(state.root_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=state.root_fd)

    def _append(
        self,
        query: FinancialQuery,
        rows: tuple[FinancialRawRow, ...],
        status: FinancialStatus,
        *,
        clock: Callable[[], datetime],
    ) -> FinancialReceipt:
        with self._state() as state:
            connection = state.connection
            try:
                connection.execute("BEGIN IMMEDIATE")
                previous = connection.execute(
                    "SELECT observed_at FROM clock_high_water WHERE singleton = 1"
                ).fetchone()
                newest = connection.execute(
                    "SELECT MAX(observed_at) AS observed_at FROM batch_manifest"
                ).fetchone()["observed_at"]
                if (previous is None and newest is not None) or (
                    previous is not None and previous["observed_at"] != newest
                ):
                    raise ValueError("manifest and clock high-water disagree")
                existing_row = connection.execute(
                    "SELECT * FROM batch_manifest WHERE request_id = ?", (str(query.request_id),)
                ).fetchone()
                if existing_row is not None:
                    existing = self._receipt_from_row(existing_row)
                    if existing.query != query:
                        raise ValueError("request ID was already committed for another query")
                    self._load_batch(existing, root_fd=state.root_fd)
                    connection.commit()
                    return existing
                observed_at = _utc(clock())
                if previous is not None and observed_at <= datetime.fromisoformat(
                    previous["observed_at"]
                ):
                    raise ValueError(
                        "observation clock did not advance beyond committed high-water"
                    )
                observation_day = observed_at.astimezone(_SHANGHAI).date()
                if query.api in _ANNOUNCEMENT_WINDOW_APIS and query.end_date > observation_day:
                    raise ValueError("announcement query ended after observation day")
                if query.api == "dividend" and query.ann_date > observation_day:
                    raise ValueError("dividend query is after observation day")
                batch = FinancialBatch(
                    query=query,
                    observed_at=observed_at,
                    status=status,
                    row_count=len(rows),
                    rows=rows,
                )
                data = _json_bytes(batch.model_dump(mode="json"))
                if len(data) > self.limits.max_batch_bytes:
                    raise ValueError("batch exceeds local byte limit")
                receipt = FinancialReceipt(
                    query=query,
                    observed_at=observed_at,
                    status=status,
                    row_count=len(rows),
                    relative_path=f"batches/{query.request_id}.json",
                    file_sha256=_sha256(data),
                    byte_count=len(data),
                )
                self._publish_file(state.root_fd, query.request_id, data)
                connection.execute(
                    """INSERT INTO batch_manifest
                       (request_id, query_json, observed_at, status, row_count,
                        relative_path, file_sha256, byte_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        str(query.request_id),
                        _json_bytes(query.model_dump(mode="json")).decode("utf-8"),
                        observed_at.isoformat(timespec="microseconds"),
                        status,
                        len(rows),
                        receipt.relative_path,
                        receipt.file_sha256,
                        receipt.byte_count,
                    ),
                )
                connection.execute(
                    """INSERT INTO clock_high_water(singleton, observed_at) VALUES (1, ?)
                       ON CONFLICT(singleton) DO UPDATE SET observed_at=excluded.observed_at""",
                    (observed_at.isoformat(timespec="microseconds"),),
                )
                connection.execute(
                    """UPDATE archive_meta
                       SET generation = generation + 1, previous_snapshot_sha256 = ?
                       WHERE singleton = 1""",
                    (state.snapshot_sha256,),
                )
                connection.commit()
                snapshot = connection.serialize()
                record = _AnchorRecord(
                    archive_id=state.archive_id,
                    generation=state.anchor.generation + 1,
                    snapshot_sha256=_sha256(snapshot),
                    high_water=observed_at.isoformat(timespec="microseconds"),
                    batch_count=state.anchor.batch_count + 1,
                    previous_record_sha256=state.anchor_record_sha256,
                )
                if (
                    os.fstat(state.anchor_fd).st_size + len(_anchor_line(record))
                    > _MAX_ANCHOR_BYTES
                ):
                    raise ValueError("archive anchor is full")
                self._publish_snapshot(state, snapshot)
                self._check_root(state.root_fd)
                _safe_regular_stat(state.root_fd, _IDENTITY_NAME, state.identity_fd)
                _safe_regular_stat(state.root_fd, _ANCHOR_NAME, state.anchor_fd)
                self._append_anchor(state.anchor_fd, record)
                return receipt
            except BaseException:
                connection.rollback()
                raise


def acquire_financial_batches(
    client: object,
    archive: FinancialArchive,
    requests: Sequence[FinancialQuery],
    *,
    run_day: date,
    clock: Callable[[], datetime],
) -> tuple[FinancialReceipt, ...]:
    """Fetch explicit requests; each supplier response is one atomic observation."""

    if type(run_day) is not date:
        raise ValueError("run_day must be a date")
    if not requests or len(requests) > archive.limits.max_requests:
        raise ValueError("acquisition requires a nonempty bounded request list")
    validated = tuple(FinancialQuery.model_validate(query.model_dump()) for query in requests)
    if len({query.request_id for query in validated}) != len(validated):
        raise ValueError("request IDs must be unique within an acquisition")
    if len({query.ts_code for query in validated}) > archive.limits.max_symbols:
        raise ValueError("acquisition exceeds the explicit symbol cap")
    for query in validated:
        _validate_query_for_day(query, run_day)

    receipts: list[FinancialReceipt] = []
    for query in validated:
        existing = archive._read_receipt(query.request_id)
        if existing is not None:
            if existing.query != query:
                raise ValueError("request ID was already committed for another query")
            archive.read_batch(query.request_id)
            receipts.append(existing)
            continue
        method = getattr(client, query.api)
        response = method(**query.supplier_parameters())
        rows = _normalized_rows(query, response, archive.limits)
        cap = _OFFICIAL_ROW_CAP.get(query.api)
        status: FinancialStatus = (
            "empty" if not rows else "possibly_truncated" if cap == len(rows) else "observed"
        )
        largest_clock_text = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
        preflight = FinancialBatch(
            query=query,
            observed_at=largest_clock_text,
            status=status,
            row_count=len(rows),
            rows=rows,
        )
        if len(_json_bytes(preflight.model_dump(mode="json"))) > archive.limits.max_batch_bytes:
            raise ValueError("supplier response exceeds local batch byte limit")
        receipts.append(archive._append(query, rows, status, clock=clock))
    return tuple(receipts)


def observed_versions(batches: Sequence[FinancialBatch]) -> tuple[FinancialObservedVersion, ...]:
    """Collapse only consecutive identical content for each PIT logical row key."""

    versions: list[FinancialObservedVersion] = []
    latest: dict[tuple[str, ...], str | None] = {}
    previous_at: datetime | None = None
    for batch in batches:
        if previous_at is not None and batch.observed_at <= previous_at:
            raise ValueError("financial batches must have strictly increasing observation times")
        previous_at = batch.observed_at
        for row in batch.rows:
            key = _logical_key(batch.query.api, row.values)
            version = FinancialObservedVersion(
                query=batch.query,
                row=row,
                first_observed_at=batch.observed_at,
                first_response_status=batch.status,
            )
            if not row.pit_usable:
                latest[key] = None
                versions.append(version)
                continue
            if latest.get(key) == row.content_sha256:
                continue
            latest[key] = row.content_sha256
            versions.append(version)
    return tuple(versions)
