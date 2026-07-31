"""Atomic immutable serving generations for read-only dashboard consumers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Self

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
_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_IMMUTABLE_DIRECTORY_MODE = 0o500
_PRIVATE_FILE_MODE = 0o600
_IMMUTABLE_FILE_MODE = 0o400

SortKey = Annotated[str, StringConstraints(min_length=1)]
FailureHook = Callable[[str], None]


class ServingIntegrityError(RuntimeError):
    """A serving pointer, manifest, or database failed integrity validation."""


class ServingTableSpec(RuntimeContractModel):
    """Deterministic physical row order for one serving table."""

    sort_keys: tuple[SortKey, ...] = Field(min_length=1)

    @field_validator("sort_keys")
    @classmethod
    def validate_sort_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("sort_keys must be unique")
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


class ServingPublisher:
    """Publish isolated DuckDB generations and atomically select the current one."""

    def __init__(
        self,
        root: str | Path,
        producer_commit: str,
        schema_version: int = 1,
        *,
        table_specs: Mapping[str, ServingTableSpec],
    ) -> None:
        if not _COMMIT_SHA.fullmatch(producer_commit):
            raise ValueError("producer_commit must be a lowercase 40-character commit SHA")
        if schema_version < 1:
            raise ValueError("schema_version must be at least 1")
        if not table_specs:
            raise ValueError("table_specs cannot be empty")

        normalized_specs: dict[str, ServingTableSpec] = {}
        for table_name, table_spec in table_specs.items():
            self._validate_table_name(table_name)
            if not isinstance(table_spec, ServingTableSpec):
                raise TypeError("table_specs values must be ServingTableSpec instances")
            normalized_specs[table_name] = table_spec

        self.root = Path(root)
        self.generations_root = self.root / "generations"
        self.current_path = self.root / "current.json"
        self.producer_commit = producer_commit
        self.schema_version = schema_version
        self.table_specs = MappingProxyType(dict(sorted(normalized_specs.items())))
        self._prepare_private_directory(self.root)
        self._prepare_private_directory(self.generations_root)

    def publish(
        self,
        tables: Mapping[str, pd.DataFrame],
        watermarks: Sequence[ServingDatasetWatermark],
        source_generations: Mapping[str, str],
        built_at: datetime,
        failure_hook: FailureHook | None = None,
    ) -> ServingGenerationManifest:
        """Build and verify an immutable generation before switching ``current.json``."""

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
        candidate = self.generations_root / f".candidate-{uuid.uuid4().hex}"
        candidate.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        database_path = candidate / "serving.duckdb"
        finalized = False

        try:
            self._build_database(database_path, normalized_tables)
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
            self._atomic_write_current(pointer)
            self._call_failure_hook(failure_hook, "after_pointer_switch")
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

        pointer = self.current_pointer()
        manifest = self._read_manifest_for_pointer(pointer)
        database_path = self._database_path(manifest.generation_id)
        content_sha256, identity = self._hash_regular_file(database_path, label="database")
        if content_sha256 != manifest.content_sha256:
            raise ServingIntegrityError("database content hash does not match manifest")

        try:
            connection = duckdb.connect(str(database_path), read_only=True)
        except (duckdb.Error, OSError) as exc:
            raise ServingIntegrityError("current database is not queryable") from exc

        try:
            current_sha256, current_identity = self._hash_regular_file(
                database_path,
                label="database",
            )
            if current_identity != identity or current_sha256 != content_sha256:
                raise ServingIntegrityError("database identity changed while opening")
            self._verify_open_connection(connection, manifest.row_counts)
        except Exception:
            connection.close()
            raise
        return connection

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        if not isinstance(table_name, str) or not _SAFE_TABLE_NAME.fullmatch(table_name):
            raise ValueError("table name must be a flat SQL identifier")

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
        missing_sort_keys = [key for key in spec.sort_keys if key not in frame.columns]
        if missing_sort_keys:
            raise ValueError(f"table {table_name} is missing sort key {missing_sort_keys[0]}")
        if frame.duplicated(subset=list(spec.sort_keys), keep=False).any():
            raise ValueError(f"table {table_name} sort keys must be unique")

        columns = sorted(frame.columns)
        return (
            frame.loc[:, columns]
            .sort_values(list(spec.sort_keys), kind="mergesort", na_position="last")
            .reset_index(drop=True)
            .copy(deep=True)
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

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
                    connection.execute(
                        f"CREATE TABLE {self._quote_identifier(table_name)} AS "
                        f"SELECT * FROM {self._quote_identifier(registration)}"
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
                        f"DESCRIBE {self._quote_identifier(table_name)}"
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
                f"SELECT count(*) FROM {self._quote_identifier(table_name)}"
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
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
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

        manifest = self.current_manifest()
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
        return connection

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

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return ServingPublisher._quote_identifier(identifier)
