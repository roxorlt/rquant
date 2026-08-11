"""Atomic immutable serving generations for read-only dashboard consumers."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, Self

import duckdb
import pandas as pd
from pydantic import Field, StringConstraints, field_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.serving_contracts import (
    ServingCurrentPointer,
    ServingDatasetWatermark,
    ServingGenerationManifest,
)

_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[[0-9]+\])?$")
_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_IMMUTABLE_DIRECTORY_MODE = 0o500
_PRIVATE_FILE_MODE = 0o600
_IMMUTABLE_FILE_MODE = 0o400
_MAX_METADATA_BYTES = 8 * 1024 * 1024
_DEFAULT_MAX_DATABASE_BYTES = 256 * 1024 * 1024
_DEFAULT_MAX_GENERATION_ROWS = 2_000_000

SortKey = Annotated[str, StringConstraints(min_length=1)]
DuckDBColumnType = Literal[
    "VARCHAR",
    "BIGINT",
    "DOUBLE",
    "BOOLEAN",
    "DATE",
    "TIMESTAMPTZ",
]
FailureHook = Callable[[str], None]


def validate_serving_table_identifier(identifier: str) -> str:
    """Return a flat table identifier or reject it before SQL construction."""

    if not isinstance(identifier, str) or _SAFE_TABLE_NAME.fullmatch(identifier) is None:
        raise ValueError("serving table identifier is not allowed")
    return identifier


def validate_serving_column_identifier(identifier: str) -> str:
    """Return a supported serving column identifier, including indicator offsets."""

    if not isinstance(identifier, str) or _SAFE_COLUMN_NAME.fullmatch(identifier) is None:
        raise ValueError("serving column identifier is not allowed")
    return identifier


def quote_serving_table_identifier(identifier: str) -> str:
    """Validate and quote a serving table identifier for DuckDB SQL."""

    return f'"{validate_serving_table_identifier(identifier)}"'


def quote_serving_column_identifier(identifier: str) -> str:
    """Validate and quote a serving column identifier for DuckDB SQL."""

    return f'"{validate_serving_column_identifier(identifier)}"'


class ServingIntegrityError(RuntimeError):
    """A serving pointer, manifest, or database failed integrity validation."""


class ServingTableSpec(RuntimeContractModel):
    """Deterministic physical row order for one serving table."""

    sort_keys: tuple[SortKey, ...] = Field(min_length=1)
    column_types: tuple[tuple[SortKey, DuckDBColumnType], ...] = ()

    @field_validator("sort_keys")
    @classmethod
    def validate_sort_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("sort_keys must be unique")
        for name in value:
            validate_serving_column_identifier(name)
        return value

    @field_validator("column_types")
    @classmethod
    def validate_column_types(
        cls,
        value: tuple[tuple[str, DuckDBColumnType], ...],
    ) -> tuple[tuple[str, DuckDBColumnType], ...]:
        names = tuple(name for name, _kind in value)
        if len(names) != len(set(names)):
            raise ValueError("column_types names must be unique")
        for name in names:
            validate_serving_column_identifier(name)
        return value


class _FileIdentity(RuntimeContractModel):
    device: int
    inode: int
    size: int
    modified_ns: int

    @classmethod
    def from_stat(cls, observed: os.stat_result) -> Self:
        return cls(
            device=observed.st_dev,
            inode=observed.st_ino,
            size=observed.st_size,
            modified_ns=observed.st_mtime_ns,
        )


class _ServingPublicationIntent(RuntimeContractModel):
    expected_pointer: ServingCurrentPointer
    previous_pointer: ServingCurrentPointer | None = None


class _ServingPublicationReceipt(RuntimeContractModel):
    pointer: ServingCurrentPointer
    previous_pointer: ServingCurrentPointer | None = None


class _ServingRecoveryRecord(RuntimeContractModel):
    status: str
    expected_generation_id: str
    observed_generation_id: str | None = None
    previous_generation_id: str | None = None
    recovered_at: datetime


class ServingPublisher:
    """Publish isolated DuckDB generations and atomically select the current one."""

    def __init__(
        self,
        root: str | Path,
        producer_commit: str,
        schema_version: int = 1,
        *,
        table_specs: Mapping[str, ServingTableSpec],
        max_database_bytes: int = _DEFAULT_MAX_DATABASE_BYTES,
        max_generation_rows: int = _DEFAULT_MAX_GENERATION_ROWS,
    ) -> None:
        if not _COMMIT_SHA.fullmatch(producer_commit):
            raise ValueError("producer_commit must be a lowercase 40-character commit SHA")
        if schema_version < 1:
            raise ValueError("schema_version must be at least 1")
        if not table_specs:
            raise ValueError("table_specs cannot be empty")
        if type(max_database_bytes) is not int or max_database_bytes < 1:
            raise ValueError("max_database_bytes must be a positive integer")
        if type(max_generation_rows) is not int or max_generation_rows < 1:
            raise ValueError("max_generation_rows must be a positive integer")

        normalized_specs: dict[str, ServingTableSpec] = {}
        for table_name, table_spec in table_specs.items():
            self._validate_table_name(table_name)
            if not isinstance(table_spec, ServingTableSpec):
                raise TypeError("table_specs values must be ServingTableSpec instances")
            normalized_specs[table_name] = table_spec

        self.root = Path(root)
        self.generations_root = self.root / "generations"
        self.current_path = self.root / "current.json"
        self.receipts_root = self.root / "receipts"
        self.recovery_root = self.root / "recovery"
        self.publication_intent_path = self.root / "publication-intent.json"
        self.publish_lock_path = self.root / ".publish.lock"
        self.producer_commit = producer_commit
        self.schema_version = schema_version
        self.max_database_bytes = max_database_bytes
        self.max_generation_rows = max_generation_rows
        self.table_specs = MappingProxyType(dict(sorted(normalized_specs.items())))
        self._prepare_private_directory(self.root)
        self._prepare_private_directory(self.generations_root)
        self._prepare_private_directory(self.receipts_root)
        self._prepare_private_directory(self.recovery_root)
        with self._publish_lock():
            self._recover_incomplete_publication()

    def publish(
        self,
        tables: Mapping[str, pd.DataFrame],
        watermarks: Sequence[ServingDatasetWatermark],
        source_generations: Mapping[str, str],
        built_at: datetime,
        failure_hook: FailureHook | None = None,
    ) -> ServingGenerationManifest:
        """Build and verify an immutable generation before switching ``current.json``."""

        with self._publish_lock():
            self._recover_incomplete_publication()
            return self._publish_locked(
                tables=tables,
                watermarks=watermarks,
                source_generations=source_generations,
                built_at=built_at,
                failure_hook=failure_hook,
            )

    def _publish_locked(
        self,
        *,
        tables: Mapping[str, pd.DataFrame],
        watermarks: Sequence[ServingDatasetWatermark],
        source_generations: Mapping[str, str],
        built_at: datetime,
        failure_hook: FailureHook | None,
    ) -> ServingGenerationManifest:

        if set(tables) != set(self.table_specs):
            raise ValueError("tables must exactly match table_specs")
        normalized_tables = {
            table_name: self._normalize_table(
                table_name,
                tables[table_name],
                self.table_specs[table_name],
            )
            for table_name in sorted(tables)
        }
        total_rows = sum(len(frame) for frame in normalized_tables.values())
        if total_rows > self.max_generation_rows:
            raise ServingIntegrityError("serving generation exceeds its row budget")
        candidate = self.generations_root / f".candidate-{uuid.uuid4().hex}"
        candidate.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        database_path = candidate / "serving.duckdb"
        finalized = False

        try:
            self._build_database(database_path, normalized_tables)
            database_size = os.stat(database_path, follow_symlinks=False).st_size
            if database_size > self.max_database_bytes:
                raise ServingIntegrityError("serving generation exceeds its database byte budget")
            self._call_failure_hook(failure_hook, "after_database_close")
            row_counts = self._verify_database(database_path, normalized_tables)
            self._call_failure_hook(failure_hook, "after_database_verify")
            content_sha256, _ = self._hash_regular_file(database_path, label="database")
            manifest = ServingGenerationManifest(
                schema_version=self.schema_version,
                source_generations=source_generations,
                watermarks=tuple(watermarks),
                content_sha256=content_sha256,
                row_counts=row_counts,
                built_at=built_at,
                producer_commit=self.producer_commit,
            )
            manifest_path = candidate / "manifest.json"
            self._write_private_file(manifest_path, self._model_json_bytes(manifest))
            parsed_manifest = self._read_manifest_file(manifest_path)
            if parsed_manifest != manifest:
                raise ServingIntegrityError("candidate manifest verification failed")
            self._fsync_directory(candidate)
            self._call_failure_hook(failure_hook, "after_manifest_write")

            generation_path = self.generations_root / manifest.generation_id
            if generation_path.exists():
                self._verify_existing_generation(generation_path, manifest)
                shutil.rmtree(candidate)
            else:
                os.chmod(database_path, _IMMUTABLE_FILE_MODE)
                os.chmod(manifest_path, _IMMUTABLE_FILE_MODE)
                os.replace(candidate, generation_path)
                os.chmod(generation_path, _IMMUTABLE_DIRECTORY_MODE)
                self._fsync_directory(self.generations_root)
            finalized = True

            existing_pointer = self._current_pointer_if_present()
            if (
                existing_pointer is not None
                and existing_pointer.generation_id == manifest.generation_id
            ):
                current_manifest = self._read_manifest_for_pointer(existing_pointer)
                self._verify_generation_database(current_manifest)
                self._ensure_receipt(existing_pointer, previous_pointer=None)
                return current_manifest

            pointer = ServingCurrentPointer(
                generation_id=manifest.generation_id,
                manifest_sha256=canonical_sha256(manifest),
                published_at=manifest.built_at,
                previous_generation_id=(
                    existing_pointer.generation_id if existing_pointer is not None else None
                ),
            )
            self._call_failure_hook(failure_hook, "before_pointer_switch")
            intent = _ServingPublicationIntent(
                expected_pointer=pointer,
                previous_pointer=existing_pointer,
            )
            self._write_intent(intent)
            switched = False
            try:
                self._atomic_write_current(pointer)
                switched = True
                self._call_failure_hook(failure_hook, "after_pointer_switch")
                self._ensure_receipt(pointer, previous_pointer=existing_pointer)
                self._clear_intent()
            except Exception:
                if switched:
                    self._compensate_failed_publication(intent)
                else:
                    self._clear_intent()
                raise
            return manifest
        finally:
            if not finalized and candidate.exists():
                shutil.rmtree(candidate)

    def current_pointer(self) -> ServingCurrentPointer:
        """Return the validated current selector."""

        if not self.current_path.exists():
            raise ServingIntegrityError("current pointer is missing")
        return self._read_pointer_file(self.current_path)

    def current_manifest(self) -> ServingGenerationManifest:
        """Return the manifest bound by the current selector."""

        return self._read_manifest_for_pointer(self.current_pointer())

    def open_current_readonly(self) -> duckdb.DuckDBPyConnection:
        """Verify and open the current generation as a read-only DuckDB connection."""

        return ServingReader(self.root).open_current_readonly()

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        try:
            validate_serving_table_identifier(table_name)
        except ValueError as exc:
            raise ValueError("table name must be a flat SQL identifier") from exc

    @staticmethod
    def _prepare_private_directory(path: Path) -> None:
        if path.is_symlink():
            raise ServingIntegrityError(f"serving directory cannot be a symlink: {path}")
        path.mkdir(parents=True, exist_ok=True, mode=_PRIVATE_DIRECTORY_MODE)
        if not path.is_dir():
            raise ServingIntegrityError(f"serving path is not a directory: {path}")
        os.chmod(path, _PRIVATE_DIRECTORY_MODE)

    @staticmethod
    def _normalize_table(
        table_name: str,
        frame: pd.DataFrame,
        spec: ServingTableSpec,
    ) -> pd.DataFrame:
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(f"table {table_name} must be a pandas DataFrame")
        if any(not isinstance(column, str) or not column for column in frame.columns):
            raise ValueError(f"table {table_name} columns must be non-empty strings")
        if len(frame.columns) != len(set(frame.columns)):
            raise ValueError(f"table {table_name} columns must be unique")
        for column in frame.columns:
            validate_serving_column_identifier(column)
        missing_sort_keys = [key for key in spec.sort_keys if key not in frame.columns]
        if missing_sort_keys:
            raise ValueError(f"table {table_name} is missing sort key {missing_sort_keys[0]}")
        if frame.duplicated(subset=list(spec.sort_keys), keep=False).any():
            raise ValueError(f"table {table_name} sort keys must be unique")
        missing_typed_columns = [
            column for column, _kind in spec.column_types if column not in frame.columns
        ]
        if missing_typed_columns:
            raise ValueError(
                f"table {table_name} is missing typed column {missing_typed_columns[0]}"
            )

        columns = sorted(frame.columns)
        return (
            frame.loc[:, columns]
            .sort_values(list(spec.sort_keys), kind="mergesort", na_position="last")
            .reset_index(drop=True)
            .copy(deep=True)
        )

    def _build_database(
        self,
        path: Path,
        tables: Mapping[str, pd.DataFrame],
    ) -> None:
        connection = duckdb.connect(str(path))
        try:
            for index, table_name in enumerate(sorted(tables)):
                registration = f"_serving_source_{index}"
                connection.register(registration, tables[table_name])
                try:
                    declared_types = dict(self.table_specs[table_name].column_types)
                    select_columns = ", ".join(
                        (
                            f"CAST({quote_serving_column_identifier(column)} AS "
                            f"{declared_types[column]}) AS "
                            f"{quote_serving_column_identifier(column)}"
                            if column in declared_types
                            else quote_serving_column_identifier(column)
                        )
                        for column in tables[table_name].columns
                    )
                    connection.execute(
                        f"CREATE TABLE {quote_serving_table_identifier(table_name)} AS "
                        f"SELECT {select_columns} FROM "
                        f"{quote_serving_table_identifier(registration)}"
                    )
                finally:
                    connection.unregister(registration)
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        os.chmod(path, _PRIVATE_FILE_MODE)
        self._fsync_file(path)

    def _verify_database(
        self,
        path: Path,
        tables: Mapping[str, pd.DataFrame],
    ) -> Mapping[str, int]:
        try:
            connection = duckdb.connect(str(path), read_only=True)
        except duckdb.Error as exc:
            raise ServingIntegrityError("candidate database is not queryable") from exc
        row_counts = {table_name: len(frame) for table_name, frame in tables.items()}
        try:
            self._verify_open_connection(connection, row_counts)
            for table_name, frame in tables.items():
                observed_columns = [
                    row[0]
                    for row in connection.execute(
                        f"DESCRIBE {quote_serving_table_identifier(table_name)}"
                    ).fetchall()
                ]
                if observed_columns != list(frame.columns):
                    raise ServingIntegrityError(
                        f"candidate table {table_name} column verification failed"
                    )
        finally:
            connection.close()
        return row_counts

    def _verify_open_connection(
        self,
        connection: duckdb.DuckDBPyConnection,
        row_counts: Mapping[str, int],
    ) -> None:
        observed_tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        }
        if observed_tables != set(row_counts):
            raise ServingIntegrityError("serving database table set does not match manifest")
        for table_name, expected_count in row_counts.items():
            observed_count = connection.execute(
                f"SELECT count(*) FROM {quote_serving_table_identifier(table_name)}"
            ).fetchone()
            if observed_count is None or observed_count[0] != expected_count:
                raise ServingIntegrityError(
                    f"serving table {table_name} row count does not match manifest"
                )

    @staticmethod
    def _call_failure_hook(failure_hook: FailureHook | None, stage: str) -> None:
        if failure_hook is not None:
            failure_hook(stage)

    def _verify_existing_generation(
        self,
        generation_path: Path,
        expected_manifest: ServingGenerationManifest,
    ) -> None:
        if generation_path.is_symlink() or not generation_path.is_dir():
            raise ServingIntegrityError("existing generation path is unsafe")
        observed_manifest = self._read_manifest_file(generation_path / "manifest.json")
        if observed_manifest != expected_manifest:
            raise ServingIntegrityError("existing generation manifest does not match candidate")
        self._verify_generation_database(observed_manifest)

    def _verify_generation_database(self, manifest: ServingGenerationManifest) -> None:
        database_path = self._database_path(manifest.generation_id)
        content_sha256, _ = self._hash_regular_file(database_path, label="database")
        if content_sha256 != manifest.content_sha256:
            raise ServingIntegrityError("database content hash does not match manifest")

    def _database_path(self, generation_id: str) -> Path:
        if not _SHA256.fullmatch(generation_id):
            raise ServingIntegrityError("generation id is not a SHA-256 digest")
        generation_path = self.generations_root / generation_id
        if generation_path.is_symlink() or not generation_path.is_dir():
            raise ServingIntegrityError("current generation directory is missing or unsafe")
        database_path = generation_path / "serving.duckdb"
        if not database_path.exists():
            raise ServingIntegrityError("current database is missing")
        return database_path

    def _current_pointer_if_present(self) -> ServingCurrentPointer | None:
        if not self.current_path.exists():
            return None
        return self._read_pointer_file(self.current_path)

    def _read_manifest_for_pointer(
        self,
        pointer: ServingCurrentPointer,
    ) -> ServingGenerationManifest:
        generation_path = self.generations_root / pointer.generation_id
        if generation_path.is_symlink() or not generation_path.is_dir():
            raise ServingIntegrityError("current generation directory is missing or unsafe")
        manifest = self._read_manifest_file(generation_path / "manifest.json")
        if manifest.generation_id != pointer.generation_id:
            raise ServingIntegrityError("manifest generation id does not match current pointer")
        if canonical_sha256(manifest) != pointer.manifest_sha256:
            raise ServingIntegrityError("manifest hash does not match current pointer")
        return manifest

    def _read_pointer_file(self, path: Path) -> ServingCurrentPointer:
        try:
            payload = self._read_regular_file(path, label="current pointer")
            return ServingCurrentPointer.model_validate_json(payload)
        except ServingIntegrityError:
            raise
        except Exception as exc:
            raise ServingIntegrityError("current pointer is invalid") from exc

    def _read_manifest_file(self, path: Path) -> ServingGenerationManifest:
        try:
            payload = self._read_regular_file(path, label="manifest")
            return ServingGenerationManifest.model_validate_json(payload)
        except ServingIntegrityError:
            raise
        except Exception as exc:
            raise ServingIntegrityError("manifest is invalid") from exc

    def _atomic_write_current(self, pointer: ServingCurrentPointer) -> None:
        if self.current_path.is_symlink():
            raise ServingIntegrityError("current pointer cannot be a symlink")
        temporary = self.root / f".current-{uuid.uuid4().hex}.tmp"
        try:
            self._write_private_file(temporary, self._model_json_bytes(pointer), exclusive=True)
            os.replace(temporary, self.current_path)
            os.chmod(self.current_path, _PRIVATE_FILE_MODE)
            self._fsync_directory(self.root)
        finally:
            if temporary.exists():
                temporary.unlink()

    @contextmanager
    def _publish_lock(self) -> Iterator[None]:
        if self.publish_lock_path.is_symlink():
            raise ServingIntegrityError("publish lock cannot be a symlink")
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.publish_lock_path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise ServingIntegrityError("publish lock cannot be opened safely") from exc
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISREG(observed.st_mode):
                raise ServingIntegrityError("publish lock is not a regular file")
            os.fchmod(descriptor, _PRIVATE_FILE_MODE)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _write_intent(self, intent: _ServingPublicationIntent) -> None:
        if self.publication_intent_path.exists():
            raise ServingIntegrityError("a serving publication intent is already active")
        self._write_private_file(
            self.publication_intent_path,
            self._model_json_bytes(intent),
            exclusive=True,
        )
        self._fsync_directory(self.root)

    def _read_intent(self) -> _ServingPublicationIntent:
        try:
            payload = self._read_regular_file(
                self.publication_intent_path,
                label="publication intent",
            )
            return _ServingPublicationIntent.model_validate_json(payload)
        except ServingIntegrityError:
            raise
        except Exception as exc:
            raise ServingIntegrityError("publication intent is invalid") from exc

    def _clear_intent(self) -> None:
        try:
            self.publication_intent_path.unlink()
        except FileNotFoundError:
            return
        self._fsync_directory(self.root)

    def _receipt_path(self, generation_id: str) -> Path:
        if not _SHA256.fullmatch(generation_id):
            raise ServingIntegrityError("receipt generation id is not a SHA-256 digest")
        return self.receipts_root / f"{generation_id}.json"

    def _ensure_receipt(
        self,
        pointer: ServingCurrentPointer,
        *,
        previous_pointer: ServingCurrentPointer | None,
    ) -> None:
        receipt = _ServingPublicationReceipt(
            pointer=pointer,
            previous_pointer=previous_pointer,
        )
        path = self._receipt_path(pointer.generation_id)
        if path.exists():
            try:
                observed = _ServingPublicationReceipt.model_validate_json(
                    self._read_regular_file(path, label="publication receipt")
                )
            except Exception as exc:
                raise ServingIntegrityError("publication receipt is invalid") from exc
            if observed.pointer != pointer:
                raise ServingIntegrityError("publication receipt conflicts with current pointer")
            return
        self._write_private_file(path, self._model_json_bytes(receipt), exclusive=True)
        self._fsync_directory(self.receipts_root)

    def _recover_incomplete_publication(self) -> None:
        if not self.publication_intent_path.exists():
            return
        intent = self._read_intent()
        receipt_path = self._receipt_path(intent.expected_pointer.generation_id)
        if receipt_path.exists():
            receipt = _ServingPublicationReceipt.model_validate_json(
                self._read_regular_file(receipt_path, label="publication receipt")
            )
            if receipt.pointer == intent.expected_pointer:
                self._clear_intent()
                return
        self._compensate_failed_publication(intent)

    def _compensate_failed_publication(self, intent: _ServingPublicationIntent) -> None:
        observed = self._current_pointer_if_present()
        if observed == intent.expected_pointer:
            restored = self._cas_restore_pointer(
                expected=intent.expected_pointer,
                replacement=intent.previous_pointer,
            )
            if not restored:
                observed = self._current_pointer_if_present()
                self._record_recovery_state(intent, observed, status="cas_conflict")
        elif observed != intent.previous_pointer:
            self._record_recovery_state(intent, observed, status="current_advanced")
        receipt_path = self._receipt_path(intent.expected_pointer.generation_id)
        if receipt_path.exists():
            receipt_path.unlink()
            self._fsync_directory(self.receipts_root)
        self._clear_intent()

    def _cas_restore_pointer(
        self,
        *,
        expected: ServingCurrentPointer,
        replacement: ServingCurrentPointer | None,
    ) -> bool:
        observed = self._current_pointer_if_present()
        if observed != expected:
            return False
        if replacement is None:
            try:
                self.current_path.unlink()
            except FileNotFoundError:
                return False
            self._fsync_directory(self.root)
            return True
        self._atomic_write_current(replacement)
        return True

    def _record_recovery_state(
        self,
        intent: _ServingPublicationIntent,
        observed: ServingCurrentPointer | None,
        *,
        status: str,
    ) -> None:
        record = _ServingRecoveryRecord(
            status=status,
            expected_generation_id=intent.expected_pointer.generation_id,
            observed_generation_id=(observed.generation_id if observed is not None else None),
            previous_generation_id=(
                intent.previous_pointer.generation_id
                if intent.previous_pointer is not None
                else None
            ),
            recovered_at=datetime.now(UTC),
        )
        path = self.recovery_root / f"{uuid.uuid4().hex}.json"
        self._write_private_file(path, self._model_json_bytes(record), exclusive=True)
        self._fsync_directory(self.recovery_root)

    @staticmethod
    def _model_json_bytes(model: RuntimeContractModel) -> bytes:
        payload = model.model_dump(mode="json")
        return (
            json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")

    @staticmethod
    def _write_private_file(path: Path, payload: bytes, *, exclusive: bool = False) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_CLOEXEC
        flags |= os.O_EXCL if exclusive else os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, _PRIVATE_FILE_MODE)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.chmod(path, _PRIVATE_FILE_MODE)

    @classmethod
    def _read_regular_file(cls, path: Path, *, label: str) -> bytes:
        payload, _ = cls._read_regular_file_with_identity(path, label=label)
        return payload

    @staticmethod
    def _read_regular_file_with_identity(
        path: Path,
        *,
        label: str,
    ) -> tuple[bytes, _FileIdentity]:
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise ServingIntegrityError(f"{label} is missing") from exc
        except OSError as exc:
            raise ServingIntegrityError(f"{label} cannot be opened safely") from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ServingIntegrityError(f"{label} is not a regular file")
            if before.st_size > _MAX_METADATA_BYTES:
                raise ServingIntegrityError(f"{label} exceeds its byte budget")
            chunks: list[bytes] = []
            total_bytes = 0
            while chunk := os.read(descriptor, 1024 * 1024):
                total_bytes += len(chunk)
                if total_bytes > _MAX_METADATA_BYTES:
                    raise ServingIntegrityError(f"{label} exceeds its byte budget")
                chunks.append(chunk)
            after = os.fstat(descriptor)
            before_identity = _FileIdentity.from_stat(before)
            after_identity = _FileIdentity.from_stat(after)
            if before_identity != after_identity:
                raise ServingIntegrityError(f"{label} changed while reading")
            return b"".join(chunks), after_identity
        finally:
            os.close(descriptor)

    @classmethod
    def _hash_regular_file(cls, path: Path, *, label: str) -> tuple[str, _FileIdentity]:
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise ServingIntegrityError(f"{label} is missing") from exc
        except OSError as exc:
            raise ServingIntegrityError(f"{label} cannot be opened safely") from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ServingIntegrityError(f"{label} is not a regular file")
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            after = os.fstat(descriptor)
            before_identity = _FileIdentity.from_stat(before)
            after_identity = _FileIdentity.from_stat(after)
            if before_identity != after_identity:
                raise ServingIntegrityError(f"{label} changed while hashing")
            return digest.hexdigest(), after_identity
        finally:
            os.close(descriptor)

    @staticmethod
    def _fsync_file(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class ServingGenerationLease:
    """One manifest and one read-only connection acquired from the same pointer read."""

    def __init__(
        self,
        *,
        pointer: ServingCurrentPointer | None,
        manifest: ServingGenerationManifest,
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        self.pointer = pointer
        self.manifest = manifest
        self.connection = connection
        self.closed = False

    def __enter__(self) -> ServingGenerationLease:
        if self.closed:
            raise ServingIntegrityError("serving generation lease is closed")
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.connection.close()
        self.closed = True

    def detach_connection(self) -> duckdb.DuckDBPyConnection:
        if self.closed:
            raise ServingIntegrityError("serving generation lease is closed")
        self.closed = True
        return self.connection


class ServingReader:
    """Read one verified serving generation without mutating its filesystem."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        if self.root.is_symlink():
            raise ServingIntegrityError("serving root cannot be a symlink")
        if not self.root.is_dir():
            raise ServingIntegrityError("serving root is missing or is not a directory")
        self.generations_root = self.root / "generations"
        self.current_path = self.root / "current.json"

    def current_pointer(self) -> ServingCurrentPointer:
        """Return the current selector after validating its file identity."""

        return self._read_pointer_file(self.current_path)

    def current_manifest(self) -> ServingGenerationManifest:
        """Return the manifest cryptographically bound to the current selector."""

        return self._read_manifest_for_pointer(self.current_pointer())

    def open_current_readonly(self) -> duckdb.DuckDBPyConnection:
        """Verify and open the current DuckDB generation in read-only mode."""

        return self.acquire_generation().detach_connection()

    def acquire_generation(self) -> ServingGenerationLease:
        """Acquire one pointer-bound manifest and connection as an indivisible lease."""

        pointer = self.current_pointer()
        manifest = self._read_manifest_for_pointer(pointer)
        return self._acquire_manifest(manifest, pointer=pointer)

    def acquire_historical_generation(self, generation_id: str) -> ServingGenerationLease:
        """Acquire one immutable retained generation without consulting current.json."""

        if not _SHA256.fullmatch(generation_id):
            raise ServingIntegrityError("generation id is not a SHA-256 digest")
        generation_path = self.generations_root / generation_id
        if generation_path.is_symlink() or not generation_path.is_dir():
            raise ServingIntegrityError("historical generation directory is missing or unsafe")
        manifest = self._read_manifest_file(generation_path / "manifest.json")
        if manifest.generation_id != generation_id:
            raise ServingIntegrityError("historical manifest generation id mismatch")
        return self._acquire_manifest(manifest, pointer=None)

    def _acquire_manifest(
        self,
        manifest: ServingGenerationManifest,
        *,
        pointer: ServingCurrentPointer | None,
    ) -> ServingGenerationLease:
        database_path = self._database_path(manifest.generation_id)
        content_sha256, identity = ServingPublisher._hash_regular_file(
            database_path,
            label="database",
        )
        if content_sha256 != manifest.content_sha256:
            raise ServingIntegrityError("database content hash does not match manifest")

        try:
            connection = duckdb.connect(str(database_path), read_only=True)
        except (duckdb.Error, OSError) as exc:
            raise ServingIntegrityError("current database is not queryable") from exc
        try:
            current_sha256, current_identity = ServingPublisher._hash_regular_file(
                database_path,
                label="database",
            )
            if current_identity != identity or current_sha256 != content_sha256:
                raise ServingIntegrityError("database identity changed while opening")
            ServingPublisher._verify_open_connection(self, connection, manifest.row_counts)
        except Exception:
            connection.close()
            raise
        return ServingGenerationLease(
            pointer=pointer,
            manifest=manifest,
            connection=connection,
        )

    def _database_path(self, generation_id: str) -> Path:
        if not _SHA256.fullmatch(generation_id):
            raise ServingIntegrityError("generation id is not a SHA-256 digest")
        if self.generations_root.is_symlink() or not self.generations_root.is_dir():
            raise ServingIntegrityError("generations root is missing or unsafe")
        generation_path = self.generations_root / generation_id
        if generation_path.is_symlink() or not generation_path.is_dir():
            raise ServingIntegrityError("current generation directory is missing or unsafe")
        database_path = generation_path / "serving.duckdb"
        if not database_path.exists():
            raise ServingIntegrityError("current database is missing")
        return database_path

    def _read_manifest_for_pointer(
        self,
        pointer: ServingCurrentPointer,
    ) -> ServingGenerationManifest:
        generation_path = self.generations_root / pointer.generation_id
        if generation_path.is_symlink() or not generation_path.is_dir():
            raise ServingIntegrityError("current generation directory is missing or unsafe")
        manifest = self._read_manifest_file(generation_path / "manifest.json")
        if manifest.generation_id != pointer.generation_id:
            raise ServingIntegrityError("manifest generation id does not match current pointer")
        if canonical_sha256(manifest) != pointer.manifest_sha256:
            raise ServingIntegrityError("manifest hash does not match current pointer")
        return manifest

    @staticmethod
    def _read_pointer_file(path: Path) -> ServingCurrentPointer:
        try:
            payload = ServingPublisher._read_regular_file(path, label="current pointer")
            return ServingCurrentPointer.model_validate_json(payload)
        except ServingIntegrityError:
            raise
        except Exception as exc:
            raise ServingIntegrityError("current pointer is invalid") from exc

    @staticmethod
    def _read_manifest_file(path: Path) -> ServingGenerationManifest:
        try:
            payload = ServingPublisher._read_regular_file(path, label="manifest")
            return ServingGenerationManifest.model_validate_json(payload)
        except ServingIntegrityError:
            raise
        except Exception as exc:
            raise ServingIntegrityError("manifest is invalid") from exc
