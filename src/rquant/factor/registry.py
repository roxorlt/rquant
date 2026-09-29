"""SQLite authority for immutable versions of declarative factor definitions."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from rquant.factor.definition import FactorDefinition
from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json

_FACTOR_ID_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_COMMAND_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_TABLE_NAMES = frozenset(
    {"factor_versions", "factor_heads", "factor_commands", "factor_archive_events"}
)
_MAX_LIST_LIMIT = 1000


class FactorRegistryError(RuntimeError):
    """Base error for the factor definition authority."""


class FactorConflictError(FactorRegistryError):
    """A command identity or expected head conflicts with committed state."""


class FactorIntegrityError(FactorRegistryError):
    """Persisted factor history is incomplete or invalid."""


class FactorHeadRef(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    version: int = Field(ge=1, strict=True)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FactorHeadState(FactorHeadRef):
    archived: bool


class SaveFactorDefinitionRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    command_id: str
    definition: FactorDefinition
    expected_head: FactorHeadRef | None

    @field_validator("command_id")
    @classmethod
    def _check_command_id(cls, value: str) -> str:
        if _COMMAND_ID_PATTERN.fullmatch(value) is None:
            raise ValueError("command ID is invalid")
        return value


class ArchiveFactorRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    command_id: str
    factor_id: str
    expected_head: FactorHeadRef

    @field_validator("command_id")
    @classmethod
    def _check_command_id(cls, value: str) -> str:
        if _COMMAND_ID_PATTERN.fullmatch(value) is None:
            raise ValueError("command ID is invalid")
        return value

    @field_validator("factor_id")
    @classmethod
    def _check_factor_id(cls, value: str) -> str:
        if _FACTOR_ID_PATTERN.fullmatch(value) is None:
            raise ValueError("factor ID is invalid")
        return value


class FactorDefinitionReceipt(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    command_id: str
    action: Literal["save", "archive"]
    factor_id: str
    version: int = Field(ge=1, strict=True)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    archived: bool


class FactorDefinitionRecord(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    definition: FactorDefinition
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    head: FactorHeadState
    is_head: bool


def _canonical_model_json(model: BaseModel) -> str:
    return canonical_json_bytes(model.model_dump(mode="json", round_trip=True)).decode("utf-8")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _request_sha256(action: Literal["save", "archive"], request: BaseModel) -> str:
    payload = {"action": action, "request": request.model_dump(mode="json", round_trip=True)}
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _checked_factor_id(factor_id: str) -> str:
    if not isinstance(factor_id, str) or _FACTOR_ID_PATTERN.fullmatch(factor_id) is None:
        raise ValueError("factor ID is invalid")
    return factor_id


def _decode_definition(payload: str) -> FactorDefinition:
    try:
        return strict_model_validate_canonical_json(FactorDefinition, payload)
    except (TypeError, ValueError) as error:
        raise FactorIntegrityError("stored factor definition is invalid") from error


def _decode_receipt(payload: str) -> FactorDefinitionReceipt:
    try:
        return strict_model_validate_canonical_json(FactorDefinitionReceipt, payload)
    except (TypeError, ValueError) as error:
        raise FactorIntegrityError("stored factor command receipt is invalid") from error


class FactorDefinitionRegistry:
    """One independent SQLite file; reads never create or repair it."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path).absolute()

    @staticmethod
    def _require_schema(connection: sqlite3.Connection) -> None:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if version != 1 or not _TABLE_NAMES.issubset(tables):
            raise FactorIntegrityError("factor registry schema is incomplete")

    @staticmethod
    def _initialize_schema(connection: sqlite3.Connection) -> None:
        for statement in (
            """CREATE TABLE factor_versions (
                factor_id TEXT NOT NULL,
                version INTEGER NOT NULL CHECK (version >= 1),
                definition_json TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                PRIMARY KEY (factor_id, version)
            )""",
            """CREATE TABLE factor_heads (
                factor_id TEXT NOT NULL PRIMARY KEY,
                version INTEGER NOT NULL CHECK (version >= 1),
                content_sha256 TEXT NOT NULL,
                archived INTEGER NOT NULL CHECK (archived IN (0, 1)),
                FOREIGN KEY (factor_id, version) REFERENCES factor_versions(factor_id, version)
            )""",
            """CREATE TABLE factor_commands (
                command_id TEXT NOT NULL PRIMARY KEY,
                action TEXT NOT NULL CHECK (action IN ('save', 'archive')),
                request_sha256 TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                receipt_sha256 TEXT NOT NULL
            )""",
            """CREATE TABLE factor_archive_events (
                command_id TEXT NOT NULL PRIMARY KEY REFERENCES factor_commands(command_id)
                    DEFERRABLE INITIALLY DEFERRED,
                factor_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                content_sha256 TEXT NOT NULL,
                UNIQUE (factor_id, version),
                FOREIGN KEY (factor_id, version) REFERENCES factor_versions(factor_id, version)
            )""",
            "PRAGMA user_version = 1",
        ):
            connection.execute(statement)

    @contextmanager
    def _writer(self) -> Iterator[sqlite3.Connection]:
        existed = self.path.exists()
        connection = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            if not tables and not existed:
                self._initialize_schema(connection)
                connection.execute("COMMIT")
                connection.execute("BEGIN IMMEDIATE")
            self._require_schema(connection)
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection | None]:
        if not self.path.exists():
            yield None
            return
        if not self.path.is_file():
            raise FactorIntegrityError("factor registry path is not a file")
        try:
            connection = sqlite3.connect(
                f"{self.path.as_uri()}?mode=ro", uri=True, timeout=10, isolation_level=None
            )
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("BEGIN")
                self._require_schema(connection)
                yield connection
            finally:
                connection.close()
        except sqlite3.DatabaseError as error:
            raise FactorIntegrityError("factor registry cannot be read") from error

    @staticmethod
    def _replay(
        connection: sqlite3.Connection,
        *,
        command_id: str,
        action: Literal["save", "archive"],
        factor_id: str,
        request_sha256: str,
    ) -> FactorDefinitionReceipt | None:
        row = connection.execute(
            "SELECT action, request_sha256, receipt_json, receipt_sha256 "
            "FROM factor_commands WHERE command_id = ?",
            (command_id,),
        ).fetchone()
        if row is None:
            return None
        if row["request_sha256"] != request_sha256:
            raise FactorConflictError("command ID was used for another request")
        if row["action"] != action:
            raise FactorIntegrityError("stored command action differs from request")
        payload = row["receipt_json"]
        if not isinstance(payload, str) or _sha256(payload) != row["receipt_sha256"]:
            raise FactorIntegrityError("stored command receipt digest differs")
        receipt = _decode_receipt(payload)
        if (
            receipt.command_id != command_id
            or receipt.action != action
            or receipt.factor_id != factor_id
            or receipt.archived != (action == "archive")
        ):
            raise FactorIntegrityError("stored command receipt identity differs")
        _, records = FactorDefinitionRegistry._load_factor(connection, factor_id)
        if (
            receipt.version > len(records)
            or records[receipt.version - 1].content_sha256 != receipt.content_sha256
        ):
            raise FactorIntegrityError("stored command receipt differs from factor version")
        if action == "archive":
            event = connection.execute(
                "SELECT content_sha256 FROM factor_archive_events "
                "WHERE command_id = ? AND factor_id = ? AND version = ?",
                (command_id, factor_id, receipt.version),
            ).fetchone()
            if event is None or event["content_sha256"] != receipt.content_sha256:
                raise FactorIntegrityError("stored archive receipt differs from event")
        return receipt

    @staticmethod
    def _record_command(
        connection: sqlite3.Connection,
        *,
        receipt: FactorDefinitionReceipt,
        request_sha256: str,
    ) -> None:
        payload = _canonical_model_json(receipt)
        connection.execute(
            "INSERT INTO factor_commands "
            "(command_id, action, request_sha256, receipt_json, receipt_sha256) "
            "VALUES (?, ?, ?, ?, ?)",
            (receipt.command_id, receipt.action, request_sha256, payload, _sha256(payload)),
        )

    @staticmethod
    def _load_factor(
        connection: sqlite3.Connection, factor_id: str
    ) -> tuple[FactorHeadState | None, tuple[FactorDefinitionRecord, ...]]:
        head_row = connection.execute(
            "SELECT version, content_sha256, archived FROM factor_heads WHERE factor_id = ?",
            (factor_id,),
        ).fetchone()
        version_rows = connection.execute(
            "SELECT version, definition_json, content_sha256 FROM factor_versions "
            "WHERE factor_id = ? ORDER BY version",
            (factor_id,),
        ).fetchall()
        if head_row is None:
            if version_rows:
                raise FactorIntegrityError("factor versions have no head")
            return None, ()
        head_version = head_row["version"]
        head_digest = head_row["content_sha256"]
        archived = head_row["archived"]
        if (
            type(head_version) is not int
            or head_version < 1
            or not isinstance(head_digest, str)
            or _DIGEST_PATTERN.fullmatch(head_digest) is None
            or type(archived) is not int
            or archived not in (0, 1)
            or len(version_rows) != head_version
        ):
            raise FactorIntegrityError("factor head or version range is invalid")
        head = FactorHeadState(
            version=head_version, content_sha256=head_digest, archived=bool(archived)
        )
        records: list[FactorDefinitionRecord] = []
        for allocated_version, row in enumerate(version_rows, start=1):
            payload = row["definition_json"]
            digest = row["content_sha256"]
            if (
                row["version"] != allocated_version
                or not isinstance(payload, str)
                or not isinstance(digest, str)
                or _DIGEST_PATTERN.fullmatch(digest) is None
                or _sha256(payload) != digest
            ):
                raise FactorIntegrityError("factor version sequence or digest is invalid")
            definition = _decode_definition(payload)
            if definition.factor_id != factor_id or definition.version != allocated_version:
                raise FactorIntegrityError("factor definition identity differs from row key")
            records.append(
                FactorDefinitionRecord(
                    definition=definition,
                    content_sha256=digest,
                    head=head,
                    is_head=allocated_version == head_version,
                )
            )
        if records[-1].content_sha256 != head_digest:
            raise FactorIntegrityError("factor head digest differs from latest version")
        archive_events = connection.execute(
            "SELECT content_sha256 FROM factor_archive_events WHERE factor_id = ? AND version = ?",
            (factor_id, head_version),
        ).fetchall()
        if (
            len(archive_events) > 1
            or bool(archive_events) != bool(archived)
            or (archive_events and archive_events[0]["content_sha256"] != head_digest)
        ):
            raise FactorIntegrityError("factor archive state differs from archive event")
        return head, tuple(records)

    def save(self, request: SaveFactorDefinitionRequest) -> FactorDefinitionReceipt:
        request = SaveFactorDefinitionRequest.model_validate(request)
        definition = request.definition
        request_digest = _request_sha256("save", request)
        with self._writer() as connection:
            replay = self._replay(
                connection,
                command_id=request.command_id,
                action="save",
                factor_id=definition.factor_id,
                request_sha256=request_digest,
            )
            if replay is not None:
                return replay
            head, _ = self._load_factor(connection, definition.factor_id)
            if head is None:
                if request.expected_head is not None or definition.version != 1:
                    raise FactorConflictError("first factor version requires an empty head")
            elif (
                request.expected_head is None
                or request.expected_head.version != head.version
                or request.expected_head.content_sha256 != head.content_sha256
                or definition.version != head.version + 1
            ):
                raise FactorConflictError("factor head changed or next version is invalid")
            payload = _canonical_model_json(definition)
            digest = _sha256(payload)
            connection.execute(
                "INSERT INTO factor_versions "
                "(factor_id, version, definition_json, content_sha256) VALUES (?, ?, ?, ?)",
                (definition.factor_id, definition.version, payload, digest),
            )
            connection.execute(
                "INSERT INTO factor_heads (factor_id, version, content_sha256, archived) "
                "VALUES (?, ?, ?, 0) ON CONFLICT(factor_id) DO UPDATE SET "
                "version = excluded.version, content_sha256 = excluded.content_sha256, "
                "archived = 0",
                (definition.factor_id, definition.version, digest),
            )
            receipt = FactorDefinitionReceipt(
                command_id=request.command_id,
                action="save",
                factor_id=definition.factor_id,
                version=definition.version,
                content_sha256=digest,
                archived=False,
            )
            self._record_command(connection, receipt=receipt, request_sha256=request_digest)
            return receipt

    def archive(self, request: ArchiveFactorRequest) -> FactorDefinitionReceipt:
        request = ArchiveFactorRequest.model_validate(request)
        request_digest = _request_sha256("archive", request)
        with self._writer() as connection:
            replay = self._replay(
                connection,
                command_id=request.command_id,
                action="archive",
                factor_id=request.factor_id,
                request_sha256=request_digest,
            )
            if replay is not None:
                return replay
            head, _ = self._load_factor(connection, request.factor_id)
            if (
                head is None
                or head.archived
                or request.expected_head.version != head.version
                or request.expected_head.content_sha256 != head.content_sha256
            ):
                raise FactorConflictError("factor head changed or was already archived")
            receipt = FactorDefinitionReceipt(
                command_id=request.command_id,
                action="archive",
                factor_id=request.factor_id,
                version=head.version,
                content_sha256=head.content_sha256,
                archived=True,
            )
            connection.execute(
                "UPDATE factor_heads SET archived = 1 WHERE factor_id = ?",
                (request.factor_id,),
            )
            connection.execute(
                "INSERT INTO factor_archive_events "
                "(command_id, factor_id, version, content_sha256) VALUES (?, ?, ?, ?)",
                (request.command_id, request.factor_id, head.version, head.content_sha256),
            )
            self._record_command(connection, receipt=receipt, request_sha256=request_digest)
            return receipt

    def get_head(self, factor_id: str) -> FactorDefinitionRecord | None:
        factor_id = _checked_factor_id(factor_id)
        with self._reader() as connection:
            if connection is None:
                return None
            _, records = self._load_factor(connection, factor_id)
            return records[-1] if records else None

    def get_version(self, factor_id: str, version: int) -> FactorDefinitionRecord | None:
        factor_id = _checked_factor_id(factor_id)
        if type(version) is not int or version < 1:
            raise ValueError("factor version is invalid")
        with self._reader() as connection:
            if connection is None:
                return None
            _, records = self._load_factor(connection, factor_id)
            return records[version - 1] if version <= len(records) else None

    def list_current(
        self, *, include_archived: bool = False, limit: int = 100
    ) -> tuple[FactorDefinitionRecord, ...]:
        if type(limit) is not int or not 1 <= limit <= _MAX_LIST_LIMIT:
            raise ValueError("factor list limit is invalid")
        with self._reader() as connection:
            if connection is None:
                return ()
            factor_ids = connection.execute(
                "SELECT factor_id FROM factor_heads UNION "
                "SELECT factor_id FROM factor_versions ORDER BY factor_id"
            ).fetchall()
            current: list[FactorDefinitionRecord] = []
            for row in factor_ids:
                factor_id = row["factor_id"]
                if (
                    not isinstance(factor_id, str)
                    or _FACTOR_ID_PATTERN.fullmatch(factor_id) is None
                ):
                    raise FactorIntegrityError("stored factor ID is invalid")
                _, records = self._load_factor(connection, factor_id)
                if records and (include_archived or not records[-1].head.archived):
                    current.append(records[-1])
            return tuple(current[:limit])
