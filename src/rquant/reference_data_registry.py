"""Append-only point-in-time registry for slowly changing reference data."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Self

from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class ReferenceDataset(StrEnum):
    """Reference domains shared by live decisions and historical replay."""

    ST_STATUS = "security_st_status"
    SUSPENSION_STATUS = "security_suspension_status"
    LISTING_STATUS = "security_listing_status"
    BOARD_MEMBERSHIP = "security_board_membership"
    ADJUSTMENT_FACTOR = "security_adjustment_factor"
    PRICE_LIMIT_REGIME = "security_price_limit_regime"


class ReferenceDataConflictError(RuntimeError):
    """An append or pointer transition conflicts with immutable history."""


class ReferenceDataIntegrityError(RuntimeError):
    """Persisted reference state cannot be trusted."""


class ReferenceDataUnavailableError(RuntimeError):
    """No unambiguous point-in-time value exists for a decision."""


def _encode_time(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decode_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReferenceDataIntegrityError("stored reference timestamp is naive")
    return parsed.astimezone(UTC)


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


class ReferenceRecord(RuntimeContractModel):
    """One immutable observation in a business-key revision lineage."""

    dataset_id: str = Field(min_length=1)
    key: str = Field(min_length=1)
    effective_from: AwareUtcDatetime
    effective_to: AwareUtcDatetime | None = None
    revision: int = Field(ge=1)
    source: str = Field(min_length=1)
    first_available_at: AwareUtcDatetime
    replacement_reason: str | None = Field(default=None, min_length=1)
    payload: Mapping[str, JsonValue] = Field(min_length=1)
    payload_sha256: Sha256 | str = ""
    record_id: Sha256 | str = ""

    @field_validator("payload", mode="after")
    @classmethod
    def canonicalize_payload(
        cls,
        value: Mapping[str, JsonValue],
    ) -> Mapping[str, JsonValue]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("payload keys must be nonempty strings")
        copied = json.loads(_canonical_json(dict(value)))
        return MappingProxyType(dict(sorted(copied.items())))

    @field_serializer("payload")
    def serialize_payload(self, value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return dict(value)

    def identity_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-record/v1",
            "dataset_id": self.dataset_id,
            "key": self.key,
            "effective_from": self.effective_from,
            "effective_to": self.effective_to,
            "revision": self.revision,
            "source": self.source,
            "first_available_at": self.first_available_at,
            "replacement_reason": self.replacement_reason,
            "payload_sha256": self.payload_sha256,
        }

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be after effective_from")
        if self.revision == 1 and self.replacement_reason is not None:
            raise ValueError("revision 1 cannot have replacement_reason")
        if self.revision > 1 and self.replacement_reason is None:
            raise ValueError("replacement_reason is required after revision 1")

        expected_payload_hash = canonical_sha256(self.payload)
        if self.payload_sha256 and self.payload_sha256 != expected_payload_hash:
            raise ValueError("payload_sha256 does not match payload")
        object.__setattr__(self, "payload_sha256", expected_payload_hash)

        expected_record_id = canonical_sha256(self.identity_payload())
        if self.record_id and self.record_id != expected_record_id:
            raise ValueError("record_id does not match immutable record content")
        object.__setattr__(self, "record_id", expected_record_id)
        return self


class ReferenceAppendResult(RuntimeContractModel):
    record: ReferenceRecord
    inserted: bool


class ReferenceGenerationManifest(RuntimeContractModel):
    schema_version: int = Field(default=1, ge=1)
    generation_id: Sha256 | str = ""
    previous_generation_id: Sha256 | None = None
    published_at: AwareUtcDatetime
    row_count: int = Field(ge=0)
    dataset_counts: Mapping[str, int]
    record_ids: tuple[Sha256, ...]
    content_sha256: Sha256
    manifest_sha256: Sha256 | str = ""

    @field_validator("dataset_counts", mode="after")
    @classmethod
    def canonicalize_counts(cls, value: Mapping[str, int]) -> Mapping[str, int]:
        if any(not key or count < 0 for key, count in value.items()):
            raise ValueError("dataset_counts must contain nonnegative named counts")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("dataset_counts")
    def serialize_counts(self, value: Mapping[str, int]) -> dict[str, int]:
        return dict(value)

    def generation_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-generation/v1",
            "schema_version": self.schema_version,
            "previous_generation_id": self.previous_generation_id,
            "published_at": self.published_at,
            "row_count": self.row_count,
            "dataset_counts": self.dataset_counts,
            "record_ids": self.record_ids,
            "content_sha256": self.content_sha256,
        }

    def manifest_payload(self) -> dict[str, object]:
        return {**self.generation_payload(), "generation_id": self.generation_id}

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        if self.row_count != len(self.record_ids):
            raise ValueError("row_count does not match record_ids")
        if len(self.record_ids) != len(set(self.record_ids)):
            raise ValueError("record_ids must be unique")
        if tuple(sorted(self.record_ids)) != self.record_ids:
            raise ValueError("record_ids must be canonically sorted")
        if sum(self.dataset_counts.values()) != self.row_count:
            raise ValueError("dataset_counts do not sum to row_count")
        expected_content = canonical_sha256(self.record_ids)
        if self.content_sha256 != expected_content:
            raise ValueError("content_sha256 does not match record_ids")
        expected_generation = canonical_sha256(self.generation_payload())
        if self.generation_id and self.generation_id != expected_generation:
            raise ValueError("generation_id does not match manifest content")
        object.__setattr__(self, "generation_id", expected_generation)
        expected_manifest = canonical_sha256(self.manifest_payload())
        if self.manifest_sha256 and self.manifest_sha256 != expected_manifest:
            raise ValueError("manifest_sha256 does not match manifest content")
        object.__setattr__(self, "manifest_sha256", expected_manifest)
        return self


class ReferenceCurrentPointer(RuntimeContractModel):
    generation_id: Sha256
    manifest_sha256: Sha256
    switched_at: AwareUtcDatetime
    previous_generation_id: Sha256 | None = None

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        if self.previous_generation_id == self.generation_id:
            raise ValueError("previous_generation_id must differ from generation_id")
        return self


class ReferenceLookup(RuntimeContractModel):
    record: ReferenceRecord
    generation_id: Sha256
    event_time: AwareUtcDatetime
    decision_time: AwareUtcDatetime


class ReferenceRegistry:
    """SQLite authority for immutable slow-reference revisions and generations."""

    _SCHEMA_VERSION = 1

    def __init__(self, path: Path | str, *, busy_timeout_ms: int = 5_000) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self._validate_integrity()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise ReferenceDataIntegrityError("reference registry requires WAL mode")
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS reference_metadata(
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS reference_record(
                        record_id TEXT PRIMARY KEY,
                        dataset_id TEXT NOT NULL,
                        business_key TEXT NOT NULL,
                        effective_from TEXT NOT NULL,
                        effective_to TEXT,
                        revision INTEGER NOT NULL CHECK(revision >= 1),
                        source TEXT NOT NULL,
                        first_available_at TEXT NOT NULL,
                        replacement_reason TEXT,
                        payload_json TEXT NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        UNIQUE(dataset_id, business_key, effective_from, revision)
                    );
                    CREATE INDEX IF NOT EXISTS reference_record_lookup
                    ON reference_record(dataset_id, business_key, first_available_at);
                    CREATE TABLE IF NOT EXISTS reference_generation(
                        generation_id TEXT PRIMARY KEY,
                        previous_generation_id TEXT,
                        published_at TEXT NOT NULL,
                        row_count INTEGER NOT NULL,
                        dataset_counts_json TEXT NOT NULL,
                        content_sha256 TEXT NOT NULL,
                        manifest_json TEXT NOT NULL,
                        manifest_sha256 TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS reference_generation_member(
                        generation_id TEXT NOT NULL REFERENCES reference_generation(generation_id),
                        record_id TEXT NOT NULL REFERENCES reference_record(record_id),
                        PRIMARY KEY(generation_id, record_id)
                    );
                    CREATE TABLE IF NOT EXISTS reference_current(
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        generation_id TEXT NOT NULL REFERENCES reference_generation(generation_id),
                        manifest_sha256 TEXT NOT NULL,
                        switched_at TEXT NOT NULL,
                        previous_generation_id TEXT
                    );
                    """
                )
                existing = connection.execute(
                    "SELECT value FROM reference_metadata WHERE key = 'schema_version'"
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO reference_metadata(key, value) VALUES ('schema_version', ?)",
                        (str(self._SCHEMA_VERSION),),
                    )
                elif existing["value"] != str(self._SCHEMA_VERSION):
                    raise ReferenceDataIntegrityError("unsupported reference registry schema")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> ReferenceRecord:
        try:
            return ReferenceRecord(
                record_id=row["record_id"],
                dataset_id=row["dataset_id"],
                key=row["business_key"],
                effective_from=_decode_time(row["effective_from"]),
                effective_to=_decode_time(row["effective_to"]),
                revision=int(row["revision"]),
                source=row["source"],
                first_available_at=_decode_time(row["first_available_at"]),
                replacement_reason=row["replacement_reason"],
                payload=json.loads(row["payload_json"]),
                payload_sha256=row["payload_sha256"],
            )
        except Exception as exc:
            raise ReferenceDataIntegrityError("stored reference record is invalid") from exc

    @staticmethod
    def _manifest_from_row(row: sqlite3.Row) -> ReferenceGenerationManifest:
        try:
            manifest = ReferenceGenerationManifest.model_validate_json(row["manifest_json"])
        except Exception as exc:
            raise ReferenceDataIntegrityError("stored generation manifest is invalid") from exc
        columns = (
            row["generation_id"],
            row["previous_generation_id"],
            row["published_at"],
            int(row["row_count"]),
            row["dataset_counts_json"],
            row["content_sha256"],
            row["manifest_sha256"],
        )
        expected = (
            manifest.generation_id,
            manifest.previous_generation_id,
            _encode_time(manifest.published_at),
            manifest.row_count,
            _canonical_json(dict(manifest.dataset_counts)),
            manifest.content_sha256,
            manifest.manifest_sha256,
        )
        if columns != expected:
            raise ReferenceDataIntegrityError("generation manifest hash or columns mismatch")
        return manifest

    def _validate_integrity(self) -> None:
        with self._connect() as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise ReferenceDataIntegrityError("reference registry integrity_check failed")
            record_rows = connection.execute(
                "SELECT * FROM reference_record ORDER BY dataset_id, business_key, "
                "effective_from, revision"
            ).fetchall()
            records = tuple(self._record_from_row(row) for row in record_rows)
            self._validate_revision_history(records)
            rows = connection.execute(
                "SELECT * FROM reference_generation ORDER BY published_at, generation_id"
            ).fetchall()
            for row in rows:
                manifest = self._manifest_from_row(row)
                member_rows = connection.execute(
                    """
                    SELECT r.* FROM reference_record AS r
                    JOIN reference_generation_member AS m ON m.record_id = r.record_id
                    WHERE m.generation_id = ? ORDER BY r.record_id
                    """,
                    (manifest.generation_id,),
                ).fetchall()
                member_records = tuple(
                    self._record_from_row(member_row) for member_row in member_rows
                )
                member_ids = tuple(record.record_id for record in member_records)
                member_counts = dict(Counter(record.dataset_id for record in member_records))
                if member_ids != manifest.record_ids:
                    raise ReferenceDataIntegrityError("generation membership hash mismatch")
                if member_counts != dict(manifest.dataset_counts):
                    raise ReferenceDataIntegrityError("generation dataset counts mismatch")
            pointer_row = connection.execute(
                "SELECT * FROM reference_current WHERE singleton = 1"
            ).fetchone()
            if pointer_row is not None:
                pointer = self._pointer_from_row(pointer_row)
                manifest = self._generation_in_connection(connection, pointer.generation_id)
                if pointer.manifest_sha256 != manifest.manifest_sha256:
                    raise ReferenceDataIntegrityError("current pointer manifest hash mismatch")

    @classmethod
    def _validate_revision_history(cls, records: tuple[ReferenceRecord, ...]) -> None:
        grouped: dict[tuple[str, str], list[ReferenceRecord]] = {}
        for record in records:
            grouped.setdefault((record.dataset_id, record.key), []).append(record)
        for business_records in grouped.values():
            lineages: dict[datetime, list[ReferenceRecord]] = {}
            for record in business_records:
                lineages.setdefault(record.effective_from, []).append(record)
            latest: list[ReferenceRecord] = []
            for lineage in lineages.values():
                ordered = sorted(lineage, key=lambda record: record.revision)
                expected_revisions = list(range(1, len(ordered) + 1))
                if [record.revision for record in ordered] != expected_revisions:
                    raise ReferenceDataIntegrityError("reference revision history has a gap")
                if any(
                    later.first_available_at < earlier.first_available_at
                    for earlier, later in pairwise(ordered)
                ):
                    raise ReferenceDataIntegrityError(
                        "reference revision availability moves backwards"
                    )
                latest.append(ordered[-1])
            for index, first in enumerate(latest):
                for second in latest[index + 1 :]:
                    if cls._periods_overlap(first, second):
                        raise ReferenceDataIntegrityError("overlapping effective reference values")

    @staticmethod
    def _pointer_from_row(row: sqlite3.Row) -> ReferenceCurrentPointer:
        try:
            return ReferenceCurrentPointer(
                generation_id=row["generation_id"],
                manifest_sha256=row["manifest_sha256"],
                switched_at=_decode_time(row["switched_at"]),
                previous_generation_id=row["previous_generation_id"],
            )
        except Exception as exc:
            raise ReferenceDataIntegrityError("stored current pointer is invalid") from exc

    def append(self, record: ReferenceRecord) -> ReferenceAppendResult:
        validated = ReferenceRecord.model_validate(record)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                exact = connection.execute(
                    """
                    SELECT * FROM reference_record
                    WHERE dataset_id = ? AND business_key = ?
                      AND effective_from = ? AND revision = ?
                    """,
                    (
                        validated.dataset_id,
                        validated.key,
                        _encode_time(validated.effective_from),
                        validated.revision,
                    ),
                ).fetchone()
                if exact is not None:
                    existing = self._record_from_row(exact)
                    if existing != validated:
                        raise ReferenceDataConflictError(
                            f"revision {validated.revision} already has different content"
                        )
                    connection.rollback()
                    return ReferenceAppendResult(record=existing, inserted=False)

                lineage = connection.execute(
                    """
                    SELECT * FROM reference_record
                    WHERE dataset_id = ? AND business_key = ? AND effective_from = ?
                    ORDER BY revision
                    """,
                    (
                        validated.dataset_id,
                        validated.key,
                        _encode_time(validated.effective_from),
                    ),
                ).fetchall()
                if lineage:
                    previous = self._record_from_row(lineage[-1])
                    if validated.revision != previous.revision + 1:
                        raise ReferenceDataConflictError(
                            f"next revision must be {previous.revision + 1}"
                        )
                    if validated.first_available_at < previous.first_available_at:
                        raise ReferenceDataConflictError(
                            "first_available_at cannot regress across revisions"
                        )
                elif validated.revision != 1:
                    raise ReferenceDataConflictError("new lineage must start at revision 1")

                self._reject_overlap(connection, validated)
                connection.execute(
                    """
                    INSERT INTO reference_record(
                        record_id, dataset_id, business_key, effective_from, effective_to,
                        revision, source, first_available_at, replacement_reason,
                        payload_json, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        validated.record_id,
                        validated.dataset_id,
                        validated.key,
                        _encode_time(validated.effective_from),
                        _encode_time(validated.effective_to) if validated.effective_to else None,
                        validated.revision,
                        validated.source,
                        _encode_time(validated.first_available_at),
                        validated.replacement_reason,
                        _canonical_json(dict(validated.payload)),
                        validated.payload_sha256,
                    ),
                )
                connection.commit()
                return ReferenceAppendResult(record=validated, inserted=True)
            except BaseException:
                connection.rollback()
                raise

    def _reject_overlap(
        self,
        connection: sqlite3.Connection,
        candidate: ReferenceRecord,
    ) -> None:
        rows = connection.execute(
            """
            SELECT record_id, dataset_id, business_key, effective_from, effective_to,
                   revision, source, first_available_at, replacement_reason,
                   payload_json, payload_sha256
            FROM reference_record
            WHERE dataset_id = ? AND business_key = ? AND effective_from != ?
            ORDER BY effective_from, revision
            """,
            (candidate.dataset_id, candidate.key, _encode_time(candidate.effective_from)),
        ).fetchall()
        latest: dict[datetime, ReferenceRecord] = {}
        for row in rows:
            record = self._record_from_row(row)
            current = latest.get(record.effective_from)
            if current is None or record.revision > current.revision:
                latest[record.effective_from] = record
        for existing in latest.values():
            if self._periods_overlap(candidate, existing):
                raise ReferenceDataConflictError(
                    f"effective period overlap with lineage {existing.effective_from.isoformat()}"
                )

    @staticmethod
    def _periods_overlap(first: ReferenceRecord, second: ReferenceRecord) -> bool:
        first_before_second_end = (
            second.effective_to is None or first.effective_from < second.effective_to
        )
        second_before_first_end = (
            first.effective_to is None or second.effective_from < first.effective_to
        )
        return first_before_second_end and second_before_first_end

    def records(self, *, dataset_id: str, key: str) -> tuple[ReferenceRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM reference_record
                WHERE dataset_id = ? AND business_key = ?
                ORDER BY effective_from, revision
                """,
                (dataset_id, key),
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def publish(self, *, published_at: datetime) -> ReferenceGenerationManifest:
        observed = normalize_aware_utc(published_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current_row = connection.execute(
                    "SELECT * FROM reference_current WHERE singleton = 1"
                ).fetchone()
                current = self._pointer_from_row(current_row) if current_row is not None else None
                if current is not None and observed < current.switched_at:
                    raise ReferenceDataConflictError("publication time cannot move backwards")

                rows = connection.execute(
                    """
                    SELECT * FROM reference_record
                    WHERE first_available_at <= ? ORDER BY record_id
                    """,
                    (_encode_time(observed),),
                ).fetchall()
                records = tuple(self._record_from_row(row) for row in rows)
                record_ids = tuple(record.record_id for record in records)
                content_sha256 = canonical_sha256(record_ids)
                if current is not None:
                    current_manifest = self._generation_in_connection(
                        connection, current.generation_id
                    )
                    if current_manifest.content_sha256 == content_sha256:
                        connection.rollback()
                        return current_manifest
                counts = Counter(record.dataset_id for record in records)
                manifest = ReferenceGenerationManifest(
                    previous_generation_id=current.generation_id if current else None,
                    published_at=observed,
                    row_count=len(records),
                    dataset_counts=dict(counts),
                    record_ids=record_ids,
                    content_sha256=content_sha256,
                )
                manifest_json = manifest.model_dump_json()
                connection.execute(
                    """
                    INSERT INTO reference_generation(
                        generation_id, previous_generation_id, published_at, row_count,
                        dataset_counts_json, content_sha256, manifest_json, manifest_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manifest.generation_id,
                        manifest.previous_generation_id,
                        _encode_time(manifest.published_at),
                        manifest.row_count,
                        _canonical_json(dict(manifest.dataset_counts)),
                        manifest.content_sha256,
                        manifest_json,
                        manifest.manifest_sha256,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO reference_generation_member(generation_id, record_id)
                    VALUES (?, ?)
                    """,
                    ((manifest.generation_id, record_id) for record_id in record_ids),
                )
                connection.execute(
                    """
                    INSERT INTO reference_current(
                        singleton, generation_id, manifest_sha256, switched_at,
                        previous_generation_id
                    ) VALUES (1, ?, ?, ?, ?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        generation_id = excluded.generation_id,
                        manifest_sha256 = excluded.manifest_sha256,
                        switched_at = excluded.switched_at,
                        previous_generation_id = excluded.previous_generation_id
                    """,
                    (
                        manifest.generation_id,
                        manifest.manifest_sha256,
                        _encode_time(observed),
                        current.generation_id if current else None,
                    ),
                )
                connection.commit()
                return manifest
            except BaseException:
                connection.rollback()
                raise

    def _generation_in_connection(
        self,
        connection: sqlite3.Connection,
        generation_id: str,
    ) -> ReferenceGenerationManifest:
        row = connection.execute(
            "SELECT * FROM reference_generation WHERE generation_id = ?",
            (generation_id,),
        ).fetchone()
        if row is None:
            raise ReferenceDataUnavailableError(f"generation {generation_id} does not exist")
        return self._manifest_from_row(row)

    def generation(self, generation_id: str) -> ReferenceGenerationManifest:
        with self._connect() as connection:
            return self._generation_in_connection(connection, generation_id)

    def current_pointer(self) -> ReferenceCurrentPointer:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM reference_current WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise ReferenceDataUnavailableError("current reference generation is missing")
            pointer = self._pointer_from_row(row)
            manifest = self._generation_in_connection(connection, pointer.generation_id)
            if pointer.manifest_sha256 != manifest.manifest_sha256:
                raise ReferenceDataIntegrityError("current pointer manifest hash mismatch")
            return pointer

    def current_manifest(self) -> ReferenceGenerationManifest:
        pointer = self.current_pointer()
        return self.generation(pointer.generation_id)

    def rollback(
        self,
        generation_id: str,
        *,
        switched_at: datetime,
    ) -> ReferenceCurrentPointer:
        observed = normalize_aware_utc(switched_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current_row = connection.execute(
                    "SELECT * FROM reference_current WHERE singleton = 1"
                ).fetchone()
                if current_row is None:
                    raise ReferenceDataUnavailableError("current reference generation is missing")
                current = self._pointer_from_row(current_row)
                if observed < current.switched_at:
                    raise ReferenceDataConflictError("rollback time cannot move backwards")
                target = self._generation_in_connection(connection, generation_id)
                if target.generation_id == current.generation_id:
                    connection.rollback()
                    return current
                pointer = ReferenceCurrentPointer(
                    generation_id=target.generation_id,
                    manifest_sha256=target.manifest_sha256,
                    switched_at=observed,
                    previous_generation_id=current.generation_id,
                )
                connection.execute(
                    """
                    UPDATE reference_current
                    SET generation_id = ?, manifest_sha256 = ?, switched_at = ?,
                        previous_generation_id = ?
                    WHERE singleton = 1
                    """,
                    (
                        pointer.generation_id,
                        pointer.manifest_sha256,
                        _encode_time(pointer.switched_at),
                        pointer.previous_generation_id,
                    ),
                )
                connection.commit()
                return pointer
            except BaseException:
                connection.rollback()
                raise

    def as_of(
        self,
        *,
        dataset_id: str,
        key: str,
        event_time: datetime,
        decision_time: datetime,
        generation_id: str | None = None,
    ) -> ReferenceLookup:
        event = normalize_aware_utc(event_time)
        decision = normalize_aware_utc(decision_time)
        with self._connect() as connection:
            selected_generation = generation_id
            if selected_generation is None:
                pointer_row = connection.execute(
                    "SELECT * FROM reference_current WHERE singleton = 1"
                ).fetchone()
                if pointer_row is None:
                    raise ReferenceDataUnavailableError("current reference generation is missing")
                selected_generation = self._pointer_from_row(pointer_row).generation_id
            self._generation_in_connection(connection, selected_generation)
            rows = connection.execute(
                """
                SELECT r.* FROM reference_record AS r
                JOIN reference_generation_member AS m ON m.record_id = r.record_id
                WHERE m.generation_id = ? AND r.dataset_id = ? AND r.business_key = ?
                ORDER BY r.effective_from, r.revision
                """,
                (selected_generation, dataset_id, key),
            ).fetchall()
        if not rows:
            raise ReferenceDataUnavailableError("reference key is not present in generation")
        records = tuple(self._record_from_row(row) for row in rows)
        visible = tuple(record for record in records if record.first_available_at <= decision)
        if not visible:
            raise ReferenceDataUnavailableError("reference value is not available at decision_time")

        latest: dict[datetime, ReferenceRecord] = {}
        for record in visible:
            existing = latest.get(record.effective_from)
            if existing is None or record.revision > existing.revision:
                latest[record.effective_from] = record
        effective = tuple(
            record
            for record in latest.values()
            if record.effective_from <= event
            and (record.effective_to is None or event < record.effective_to)
        )
        if not effective:
            raise ReferenceDataUnavailableError("reference value is not effective at event_time")
        if len(effective) != 1:
            raise ReferenceDataIntegrityError("overlapping effective reference values")
        return ReferenceLookup(
            record=effective[0],
            generation_id=selected_generation,
            event_time=event,
            decision_time=decision,
        )
