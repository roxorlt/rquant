"""Typed command outbox for page-owned control actions.

Streamlit pages submit immutable commands. Only the control consumer owns the
mutable Canvas, preset, query-log, and Lab export paths.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import sqlite3
import stat
import urllib.request
from collections.abc import Callable, Mapping
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Protocol
from uuid import UUID, uuid4

from pydantic import Field, JsonValue, TypeAdapter, field_validator

from rquant.canvas_publication_receipt import (
    CanvasPublicationCatalogRecord,
    CanvasPublicationCommand,
    CanvasPublicationKeyring,
    CanvasPublicationReceipt,
    CanvasPublicationReceiptStore,
    CanvasPublicationSigner,
    build_canvas_publication_claims,
)
from rquant.lab_job_protocol import LabCommand
from rquant.llm.schemas import RuleCall
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

_SAFE_NAME = re.compile(r"^[\w\u4e00-\u9fff-]+$")
_CANVAS_CATALOG_SCHEMA_VERSION = 1
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_MANAGED_JSON_BYTES = 1024 * 1024
_MAX_MANAGED_LOG_BYTES = 8 * 1024 * 1024
_DEFAULT_LEASE_SECONDS = 30
_MAX_REQUEST_FUTURE_SKEW = timedelta(minutes=5)
DEFAULT_PAGE_CONTROL_SERVICE_ID = "rquant-page-control"
_CONSUMER_MUTEX_SUFFIX = ".consumer.lock"
_SAFE_EFFECT_JOURNAL_MARKER = "safe-effect-journal-v2"
_SAFE_EFFECT_JOURNAL_VERSION = 2
_LOCAL_FILESYSTEM_FENCE_SCHEMA_VERSION = 1
_CANVAS_HEAD_CONTRACT = "canvas-current-head/v1"
_CANVAS_HEAD_SOURCE = "canvas_current_head"
_CANVAS_WATERMARK_DIRECTORY = "canvas-publication-watermarks"
_HELD_CONSUMER_MUTEXES: set[Path] = set()
_EXTERNAL_LAB_COMMAND_KINDS = frozenset(
    {
        "submit_lab_command",
        "export_lab_artifact_zip",
        "discard_lab_artifact_zip",
    }
)


class PageControlCommand(RuntimeContractModel):
    kind: str
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime


class SaveCanvas(PageControlCommand):
    kind: Literal["save_canvas"] = "save_canvas"
    name: str
    description: str = ""
    pool_refs: tuple[str, ...] = ()
    source: str = "page_control"

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class DeleteCanvas(PageControlCommand):
    kind: Literal["delete_canvas"] = "delete_canvas"
    name: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class SetCanvasPoolRefs(PageControlCommand):
    kind: Literal["set_canvas_pool_refs"] = "set_canvas_pool_refs"
    name: str
    pool_refs: tuple[str, ...]

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class SaveUserPool(PageControlCommand):
    kind: Literal["save_user_pool"] = "save_user_pool"
    base_name: str
    description: str = ""
    rule_calls: tuple[RuleCall, ...] = ()
    include_columns: tuple[str, ...] = ()
    source: str = "page_control"
    canvas_name: str | None = None

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="user pool name")

    @field_validator("canvas_name")
    @classmethod
    def validate_canvas_name(cls, value: str | None) -> str | None:
        return None if value is None else _validated_name(value, label="canvas name")


class DeleteUserPool(PageControlCommand):
    kind: Literal["delete_user_pool"] = "delete_user_pool"
    base_name: str

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="user pool name")


class ForkBuiltinPool(PageControlCommand):
    kind: Literal["fork_builtin_pool"] = "fork_builtin_pool"
    builtin_name: str
    target_base_name: str
    canvas_name: str | None = None

    @field_validator("builtin_name", "target_base_name")
    @classmethod
    def validate_pool_name(cls, value: str) -> str:
        return _validated_name(value, label="pool name")

    @field_validator("canvas_name")
    @classmethod
    def validate_canvas_name(cls, value: str | None) -> str | None:
        return None if value is None else _validated_name(value, label="canvas name")


class SaveNlPreset(PageControlCommand):
    kind: Literal["save_nl_preset"] = "save_nl_preset"
    name: str
    description: str = ""
    rule_calls: tuple[RuleCall, ...] = ()
    include_columns: tuple[str, ...] = ()
    overwrite: bool = False

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="preset name")


class AppendNlQueryLog(PageControlCommand):
    kind: Literal["append_nl_query_log"] = "append_nl_query_log"
    query: str = Field(min_length=1)
    plan: JsonValue | None = None
    outcome: Literal["success", "clarification", "error"]
    error: str | None = None


class InitializeLabExports(PageControlCommand):
    kind: Literal["initialize_lab_exports"] = "initialize_lab_exports"
    export_root: Path
    runtime_root: Path


class SubmitLabCommand(PageControlCommand):
    kind: Literal["submit_lab_command"] = "submit_lab_command"
    command: LabCommand
    interaction_key: str | None = Field(default=None, min_length=1, max_length=256)


class ExportLabArtifactZip(PageControlCommand):
    kind: Literal["export_lab_artifact_zip"] = "export_lab_artifact_zip"
    job_id: UUID


class DiscardLabArtifactZip(PageControlCommand):
    kind: Literal["discard_lab_artifact_zip"] = "discard_lab_artifact_zip"
    request_id: UUID
    job_id: UUID
    path: Path
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class LabArtifactZipResult(RuntimeContractModel):
    request_id: UUID
    job_id: UUID
    path: Path
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class LabPageControlBackend(Protocol):
    def submit_command(
        self,
        command: LabCommand,
        *,
        interaction_key: str | None,
    ) -> JsonValue: ...

    def export_zip(self, job_id: UUID) -> JsonValue: ...

    def discard_zip(self, command: DiscardLabArtifactZip) -> JsonValue: ...


PageControlCommandValue = Annotated[
    SaveCanvas
    | DeleteCanvas
    | SetCanvasPoolRefs
    | SaveUserPool
    | DeleteUserPool
    | ForkBuiltinPool
    | SaveNlPreset
    | AppendNlQueryLog
    | InitializeLabExports
    | SubmitLabCommand
    | ExportLabArtifactZip
    | DiscardLabArtifactZip,
    Field(discriminator="kind"),
]
_COMMAND_ADAPTER = TypeAdapter(PageControlCommandValue)


class PageControlStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class PageControlEffectStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


_PAGE_CONTROL_TERMINAL_STATUSES = frozenset(
    {
        PageControlStatus.SUCCEEDED,
        PageControlStatus.FAILED,
        PageControlStatus.AMBIGUOUS,
    }
)
_PAGE_CONTROL_EFFECT_TERMINAL_STATUSES = frozenset(
    {
        PageControlEffectStatus.SUCCEEDED,
        PageControlEffectStatus.FAILED,
        PageControlEffectStatus.AMBIGUOUS,
    }
)


class PageControlReceipt(RuntimeContractModel):
    command_id: str
    status: PageControlStatus
    enqueued_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None = None
    result: JsonValue | None = None
    error: str | None = None


class PageControlCommandAudit(RuntimeContractModel):
    command_id: str
    command_kind: str
    command_hash: str
    status: PageControlStatus
    result: JsonValue | None = None


class PageControlClaim(RuntimeContractModel):
    command: PageControlCommandValue
    owner_id: str
    claim_token: str


class PageControlEffectRecord(RuntimeContractModel):
    command_id: str
    command_hash: str
    effect_kind: str
    status: PageControlEffectStatus
    owner_id: str
    claim_token: str
    result: JsonValue | None = None
    error: str | None = None


@dataclass(frozen=True)
class _ExecutionOutcome:
    status: PageControlStatus
    result: JsonValue | None
    error: str | None = None


class _RetryableCommittedLocalEffectError(RuntimeError):
    """A journaled local mutation needs recovery before it can be terminalized."""


@dataclass(frozen=True)
class _LocalEffectFenceTarget:
    role: str
    path: Path
    create: bool = True


@dataclass(frozen=True)
class _BoundManagedDirectory:
    path: Path
    descriptors: tuple[int, ...]
    component_names: tuple[str, ...]

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
                raise ValueError(
                    f"managed directory ancestor changed while bound: {self.path}"
                ) from exc
            if stat.S_ISLNK(entry.st_mode):
                raise ValueError(f"managed directory ancestor cannot be a symlink: {self.path}")
            if not stat.S_ISDIR(entry.st_mode):
                raise ValueError(f"managed directory ancestor is not a directory: {self.path}")
            if _file_node_tuple(entry) != _file_node_tuple(os.fstat(child)):
                raise ValueError(f"managed directory ancestor changed while bound: {self.path}")

    def duplicate(self) -> int:
        self.verify()
        return os.dup(self.descriptor)

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            with suppress(OSError):
                os.close(descriptor)


_ACTIVE_EFFECT_DIRECTORY_BINDINGS: ContextVar[Mapping[Path, _BoundManagedDirectory] | None] = (
    ContextVar("page_control_effect_directory_bindings", default=None)
)


@dataclass(frozen=True)
class CanvasCurrentHead:
    receipt: CanvasPublicationReceipt
    state: Literal["active", "deleted"]
    sequence: int
    previous_head_receipt_id: str | None
    publication_receipt_id: str | None
    authority_command_kind: str
    authority_command_hash: str


def read_canvas_current_head(
    root: Path,
    canvas_name: str,
    keyring: CanvasPublicationKeyring,
    *,
    observed_at: datetime | None = None,
    directory_descriptor: int | None = None,
) -> CanvasCurrentHead | None:
    canvas_root = Path(os.path.abspath(root)) / _validated_name(
        canvas_name,
        label="canvas name",
    )
    if directory_descriptor is None:
        try:
            directory = _open_existing_managed_directory(canvas_root)
        except FileNotFoundError:
            return None
    else:
        directory = os.dup(directory_descriptor)
    try:
        _verify_open_directory_matches_path(canvas_root, directory)
        names = sorted(os.listdir(directory))
    finally:
        os.close(directory)
    if not names:
        return None
    nodes: list[CanvasCurrentHead] = []
    store = CanvasPublicationReceiptStore(
        canvas_root,
        directory_descriptor=directory_descriptor,
    )
    for name in names:
        if not name.endswith(".json") or Path(name).name != name:
            raise ValueError("canvas current head directory contains an invalid entry")
        receipt_id = name.removesuffix(".json")
        publication = store.read(receipt_id)
        if not keyring.verify_publication_receipt(publication):
            raise ValueError("canvas current head signature verification failed")
        if observed_at is not None:
            _assert_canvas_receipt_not_future(publication, observed_at=observed_at)
        command = publication.claims.command
        if (
            command.name != canvas_name
            or command.source != _CANVAS_HEAD_SOURCE
            or command.pool_refs
        ):
            raise ValueError("canvas current head signed semantics do not match")
        try:
            payload = json.loads(command.description)
        except json.JSONDecodeError as exc:
            raise ValueError("canvas current head payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("canvas current head payload is not an object")
        expected_keys = {
            "authority_command_hash",
            "authority_command_kind",
            "canvas_name",
            "contract",
            "previous_head_receipt_id",
            "publication_receipt_id",
            "sequence",
            "state",
        }
        if set(payload) != expected_keys or payload.get("contract") != _CANVAS_HEAD_CONTRACT:
            raise ValueError("canvas current head payload schema mismatch")
        if payload.get("canvas_name") != canvas_name:
            raise ValueError("canvas current head canvas identity mismatch")
        state = payload.get("state")
        sequence = payload.get("sequence")
        previous = payload.get("previous_head_receipt_id")
        active_receipt_id = payload.get("publication_receipt_id")
        command_kind = payload.get("authority_command_kind")
        command_hash = payload.get("authority_command_hash")
        if state not in {"active", "deleted"} or not isinstance(sequence, int):
            raise ValueError("canvas current head state is invalid")
        if sequence < 1:
            raise ValueError("canvas current head sequence is invalid")
        if previous is not None and (
            not isinstance(previous, str) or re.fullmatch(r"[0-9a-f]{64}", previous) is None
        ):
            raise ValueError("canvas current head predecessor is invalid")
        if state == "active":
            if (
                not isinstance(active_receipt_id, str)
                or re.fullmatch(r"[0-9a-f]{64}", active_receipt_id) is None
            ):
                raise ValueError("canvas current head publication identity is invalid")
        elif active_receipt_id is not None:
            raise ValueError("canvas tombstone cannot reference an active publication")
        if not isinstance(command_kind, str) or not command_kind:
            raise ValueError("canvas current head command kind is invalid")
        if not isinstance(command_hash, str) or re.fullmatch(r"[0-9a-f]{64}", command_hash) is None:
            raise ValueError("canvas current head command hash is invalid")
        canonical_payload = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if canonical_payload != command.description:
            raise ValueError("canvas current head payload is not canonical JSON")
        nodes.append(
            CanvasCurrentHead(
                receipt=publication,
                state=state,
                sequence=sequence,
                previous_head_receipt_id=previous,
                publication_receipt_id=active_receipt_id,
                authority_command_kind=command_kind,
                authority_command_hash=command_hash,
            )
        )
    nodes.sort(key=lambda item: item.sequence)
    for index, node in enumerate(nodes):
        expected_sequence = index + 1
        expected_previous = None if index == 0 else nodes[index - 1].receipt.receipt_id
        if node.sequence != expected_sequence or node.previous_head_receipt_id != expected_previous:
            raise ValueError("canvas current head chain is not linear and complete")
    current = nodes[-1]
    if not keyring.verify_publication_receipt(current.receipt, require_active=True):
        raise ValueError("canvas current head active signature verification failed")
    return current


class PageControlOutbox:
    """Durable, idempotent command authority separate from page state."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS page_control_command (
                    command_id TEXT PRIMARY KEY,
                    command_kind TEXT NOT NULL,
                    command_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    enqueued_at TEXT NOT NULL,
                    completed_at TEXT,
                    result_json TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS page_control_effect (
                    command_id TEXT PRIMARY KEY,
                    command_hash TEXT NOT NULL,
                    effect_kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    claim_token TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    result_json TEXT,
                    error TEXT,
                    FOREIGN KEY(command_id) REFERENCES page_control_command(command_id)
                );
                CREATE INDEX IF NOT EXISTS page_control_pending_idx
                    ON page_control_command(status, enqueued_at, command_id);
                CREATE TABLE IF NOT EXISTS page_control_protocol_activation (
                    marker_name TEXT PRIMARY KEY,
                    protocol_version INTEGER NOT NULL,
                    activated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column(connection, "processing_owner", "TEXT")
            self._ensure_column(connection, "lease_expires_at", "TEXT")
            self._ensure_column(connection, "attempt_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "claim_token", "TEXT")
            connection.commit()
            self._activate_safe_effect_journal_protocol(connection)

    @staticmethod
    def _ensure_column(connection: sqlite3.Connection, name: str, definition: str) -> None:
        try:
            connection.execute(f"ALTER TABLE page_control_command ADD COLUMN {name} {definition}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise

    @staticmethod
    def _activate_safe_effect_journal_protocol(connection: sqlite3.Connection) -> None:
        activated_at = datetime.now(UTC).isoformat(timespec="microseconds")
        connection.execute("BEGIN IMMEDIATE")
        try:
            marker = connection.execute(
                """
                SELECT 1 FROM page_control_protocol_activation
                WHERE marker_name = ?
                """,
                (_SAFE_EFFECT_JOURNAL_MARKER,),
            ).fetchone()
            if marker is None:
                PageControlOutbox._terminalize_unjournaled_external_processing(
                    connection,
                    observed_at=activated_at,
                )
                connection.execute(
                    """
                    INSERT INTO page_control_protocol_activation(
                        marker_name, protocol_version, activated_at
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        _SAFE_EFFECT_JOURNAL_MARKER,
                        _SAFE_EFFECT_JOURNAL_VERSION,
                        activated_at,
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def _terminalize_unjournaled_external_processing(
        connection: sqlite3.Connection,
        *,
        observed_at: str,
    ) -> None:
        rows = connection.execute(
            """
            SELECT
                c.command_id,
                c.command_kind,
                c.command_hash,
                c.payload_json
            FROM page_control_command AS c
            LEFT JOIN page_control_effect AS e
                ON e.command_id = c.command_id
            WHERE c.status = ?
              AND c.command_kind IN (?, ?, ?)
              AND (c.lease_expires_at IS NULL OR c.lease_expires_at <= ?)
              AND e.command_id IS NULL
            """,
            (
                PageControlStatus.PROCESSING.value,
                *sorted(_EXTERNAL_LAB_COMMAND_KINDS),
                observed_at,
            ),
        ).fetchall()
        for row in rows:
            result = _ambiguous_external_effect_result_from_row(row)
            connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = ?,
                    processing_owner = NULL, lease_expires_at = NULL,
                    claim_token = NULL
                WHERE command_id = ? AND status = ?
                """,
                (
                    PageControlStatus.AMBIGUOUS.value,
                    observed_at,
                    json.dumps(result, ensure_ascii=True),
                    "external Lab effect lacks a PageControl effect journal",
                    row["command_id"],
                    PageControlStatus.PROCESSING.value,
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        return connection

    def enqueue(self, command: PageControlCommandValue) -> PageControlReceipt:
        payload = command.model_dump_json()
        command_hash = _command_hash(command)
        enqueued_at = command.requested_at.isoformat(timespec="microseconds")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            if existing is not None:
                if existing["command_hash"] != command_hash:
                    raise ValueError("command_id already exists with different payload")
                return self._receipt(existing)
            connection.execute(
                """
                INSERT INTO page_control_command(
                    command_id, command_kind, command_hash, payload_json,
                    status, enqueued_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    command.command_id,
                    command.kind,
                    command_hash,
                    payload,
                    PageControlStatus.PENDING.value,
                    enqueued_at,
                ),
            )
        receipt = self.receipt(command.command_id)
        assert receipt is not None
        return receipt

    def claim(
        self,
        *,
        limit: int,
        owner_id: str = "page-control-consumer",
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> tuple[PageControlCommandValue, ...]:
        return tuple(
            record.command
            for record in self.claim_records(
                limit=limit,
                owner_id=owner_id,
                lease_seconds=lease_seconds,
                now=now,
            )
        )

    def claim_records(
        self,
        *,
        limit: int,
        owner_id: str = "page-control-consumer",
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> tuple[PageControlClaim, ...]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if not owner_id:
            raise ValueError("owner_id is required")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        observed = _normalize_utc(now or datetime.now(UTC))
        observed_at = observed.isoformat(timespec="microseconds")
        lease_expires_at = (observed + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT command_id, payload_json
                FROM page_control_command
                WHERE status = ?
                   OR (status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                ORDER BY CASE status WHEN ? THEN 0 ELSE 1 END, enqueued_at, rowid
                LIMIT ?
                """,
                (
                    PageControlStatus.PENDING.value,
                    PageControlStatus.PROCESSING.value,
                    observed_at,
                    PageControlStatus.PENDING.value,
                    limit,
                ),
            ).fetchall()
            claims: list[PageControlClaim] = []
            for row in rows:
                claim_token = uuid4().hex
                changed = connection.execute(
                    """
                    UPDATE page_control_command
                    SET status = ?, processing_owner = ?, lease_expires_at = ?,
                        attempt_count = COALESCE(attempt_count, 0) + 1,
                        claim_token = ?
                    WHERE command_id = ?
                      AND (
                        status = ?
                        OR (
                            status = ? AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?
                        )
                      )
                    """,
                    (
                        PageControlStatus.PROCESSING.value,
                        owner_id,
                        lease_expires_at,
                        claim_token,
                        row["command_id"],
                        PageControlStatus.PENDING.value,
                        PageControlStatus.PROCESSING.value,
                        observed_at,
                    ),
                ).rowcount
                if changed == 1:
                    claims.append(
                        PageControlClaim(
                            command=_COMMAND_ADAPTER.validate_json(row["payload_json"]),
                            owner_id=owner_id,
                            claim_token=claim_token,
                        )
                    )
            connection.commit()
        return tuple(claims)

    def complete(
        self,
        command_id: str,
        *,
        result: JsonValue | None = None,
        error: str | None = None,
        status: PageControlStatus | None = None,
        owner_id: str | None = None,
        claim_token: str | None = None,
    ) -> PageControlReceipt:
        status = status or (
            PageControlStatus.FAILED if error is not None else PageControlStatus.SUCCEEDED
        )
        if status not in _PAGE_CONTROL_TERMINAL_STATUSES:
            raise ValueError("command completion status must be terminal")
        completed_at = datetime.now(UTC).isoformat(timespec="microseconds")
        owner_predicate = ""
        owner_values: tuple[str, ...] = ()
        if owner_id is not None or claim_token is not None:
            if owner_id is None or claim_token is None:
                raise ValueError("owner_id and claim_token must be provided together")
            owner_predicate = " AND processing_owner = ? AND claim_token = ?"
            owner_values = (owner_id, claim_token)
        with self._connect() as connection:
            changed = connection.execute(
                f"""
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = ?,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ?{owner_predicate}
                """,
                (
                    status.value,
                    completed_at,
                    None if result is None else json.dumps(result, ensure_ascii=True),
                    error,
                    command_id,
                    PageControlStatus.PROCESSING.value,
                    *owner_values,
                ),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT * FROM page_control_command WHERE command_id = ?",
                    (command_id,),
                ).fetchone()
                if row is not None and PageControlStatus(row["status"]) in (
                    _PAGE_CONTROL_TERMINAL_STATUSES
                ):
                    return self._receipt(row)
                raise RuntimeError("stale page control claim cannot complete command")
        receipt = self.receipt(command_id)
        assert receipt is not None
        return receipt

    def release_claim_for_retry(
        self,
        command_id: str,
        *,
        owner_id: str,
        claim_token: str,
    ) -> PageControlReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = NULL, result_json = NULL, error = NULL,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ?
                  AND processing_owner = ? AND claim_token = ?
                  AND EXISTS (
                    SELECT 1 FROM page_control_effect AS effect
                    WHERE effect.command_id = page_control_command.command_id
                      AND effect.status = ?
                      AND effect.owner_id = ? AND effect.claim_token = ?
                  )
                """,
                (
                    PageControlStatus.PENDING.value,
                    command_id,
                    PageControlStatus.PROCESSING.value,
                    owner_id,
                    claim_token,
                    PageControlEffectStatus.STARTED.value,
                    owner_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                connection.rollback()
                raise RuntimeError("stale page control claim cannot be released for retry")
            connection.commit()
        receipt = self.receipt(command_id)
        assert receipt is not None
        return receipt

    def begin_effect(
        self,
        command: PageControlCommandValue,
        *,
        owner_id: str,
        claim_token: str,
        now: datetime | None = None,
    ) -> tuple[PageControlEffectRecord, bool]:
        command_hash = _command_hash(command)
        observed_at = _normalize_utc(now or datetime.now(UTC)).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT command_hash, status, processing_owner, claim_token
                FROM page_control_command WHERE command_id = ?
                """,
                (command.command_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("page control command disappeared")
            if row["command_hash"] != command_hash:
                raise ValueError("command payload hash changed")
            if (
                PageControlStatus(row["status"]) != PageControlStatus.PROCESSING
                or row["processing_owner"] != owner_id
                or row["claim_token"] != claim_token
            ):
                raise RuntimeError("stale page control claim cannot start effect")
            existing = connection.execute(
                "SELECT * FROM page_control_effect WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            created = False
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO page_control_effect(
                        command_id, command_hash, effect_kind, status,
                        owner_id, claim_token, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        command.command_id,
                        command_hash,
                        command.kind,
                        PageControlEffectStatus.STARTED.value,
                        owner_id,
                        claim_token,
                        observed_at,
                    ),
                )
                created = True
            else:
                if existing["command_hash"] != command_hash:
                    raise ValueError("effect command hash mismatch")
                if PageControlEffectStatus(existing["status"]) == PageControlEffectStatus.STARTED:
                    connection.execute(
                        """
                        UPDATE page_control_effect
                        SET owner_id = ?, claim_token = ?
                        WHERE command_id = ? AND status = ?
                        """,
                        (
                            owner_id,
                            claim_token,
                            command.command_id,
                            PageControlEffectStatus.STARTED.value,
                        ),
                    )
            connection.commit()
        effect = self.effect(command.command_id)
        assert effect is not None
        return effect, created

    def finish_effect(
        self,
        command_id: str,
        *,
        status: PageControlEffectStatus,
        result: JsonValue | None = None,
        error: str | None = None,
        owner_id: str,
        claim_token: str,
    ) -> PageControlEffectRecord:
        if status not in _PAGE_CONTROL_EFFECT_TERMINAL_STATUSES:
            raise ValueError("effect completion status must be terminal")
        completed_at = datetime.now(UTC).isoformat(timespec="microseconds")
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE page_control_effect
                SET status = ?, completed_at = ?, result_json = ?, error = ?
                WHERE command_id = ? AND status = ? AND owner_id = ? AND claim_token = ?
                """,
                (
                    status.value,
                    completed_at,
                    None if result is None else json.dumps(result, ensure_ascii=True),
                    error,
                    command_id,
                    PageControlEffectStatus.STARTED.value,
                    owner_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT * FROM page_control_effect WHERE command_id = ?",
                    (command_id,),
                ).fetchone()
                if row is not None and PageControlEffectStatus(row["status"]) in (
                    _PAGE_CONTROL_EFFECT_TERMINAL_STATUSES
                ):
                    return self._effect_record(row)
                raise RuntimeError("stale page control claim cannot finish effect")
        effect = self.effect(command_id)
        assert effect is not None
        return effect

    def record_started_effect_result(
        self,
        command_id: str,
        *,
        result: JsonValue,
        owner_id: str,
        claim_token: str,
    ) -> PageControlEffectRecord:
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE page_control_effect
                SET result_json = ?
                WHERE command_id = ? AND status = ? AND owner_id = ? AND claim_token = ?
                """,
                (
                    json.dumps(result, ensure_ascii=True),
                    command_id,
                    PageControlEffectStatus.STARTED.value,
                    owner_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("stale page control claim cannot update effect")
        effect = self.effect(command_id)
        assert effect is not None
        return effect

    def receipt(self, command_id: str) -> PageControlReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def effect(self, command_id: str) -> PageControlEffectRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_effect WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        return None if row is None else self._effect_record(row)

    def audit(self, command_id: str) -> PageControlCommandAudit | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        if row is None:
            return None
        return PageControlCommandAudit(
            command_id=row["command_id"],
            command_kind=row["command_kind"],
            command_hash=row["command_hash"],
            status=PageControlStatus(row["status"]),
            result=(None if row["result_json"] is None else json.loads(row["result_json"])),
        )

    def latest_succeeded_canvas_mutation(
        self,
        canvas_name: str,
    ) -> PageControlCommandValue | None:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT command_kind, command_hash, payload_json
                FROM page_control_command
                WHERE status = ?
                  AND command_kind IN (?, ?, ?, ?, ?)
                ORDER BY rowid DESC
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
        for row in rows:
            command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if command.kind != row["command_kind"] or _command_hash(command) != row["command_hash"]:
                raise ValueError("PageControl canvas mutation authority is malformed")
            affected_canvas = (
                command.name
                if isinstance(command, (SaveCanvas, DeleteCanvas, SetCanvasPoolRefs))
                else command.canvas_name
            )
            if affected_canvas == canvas_name:
                return command
        return None

    @staticmethod
    def _receipt(row: sqlite3.Row) -> PageControlReceipt:
        return PageControlReceipt(
            command_id=row["command_id"],
            status=PageControlStatus(row["status"]),
            enqueued_at=datetime.fromisoformat(row["enqueued_at"]),
            completed_at=(
                None if row["completed_at"] is None else datetime.fromisoformat(row["completed_at"])
            ),
            result=(None if row["result_json"] is None else json.loads(row["result_json"])),
            error=row["error"],
        )

    @staticmethod
    def _effect_record(row: sqlite3.Row) -> PageControlEffectRecord:
        return PageControlEffectRecord(
            command_id=row["command_id"],
            command_hash=row["command_hash"],
            effect_kind=row["effect_kind"],
            status=PageControlEffectStatus(row["status"]),
            owner_id=row["owner_id"],
            claim_token=row["claim_token"],
            result=(None if row["result_json"] is None else json.loads(row["result_json"])),
            error=row["error"],
        )


class PageControlConsumer:
    """The only component permitted to mutate page-managed local artifacts."""

    def __init__(
        self,
        *,
        outbox: PageControlOutbox,
        data_dir: Path,
        log_dir: Path,
        allowed_lab_export_roots: tuple[Path, ...] = (),
        lab_backend: LabPageControlBackend | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        consumer_id: str | None = None,
        consumer_service_id: str = DEFAULT_PAGE_CONTROL_SERVICE_ID,
        canvas_publication_signer: CanvasPublicationSigner | None = None,
        canvas_publication_keyring: CanvasPublicationKeyring | None = None,
    ) -> None:
        self.outbox = outbox
        self.data_dir = Path(os.path.abspath(data_dir))
        self.log_dir = Path(os.path.abspath(log_dir))
        self.allowed_lab_export_roots = tuple(
            Path(os.path.abspath(path)) for path in allowed_lab_export_roots
        )
        self.lab_backend = lab_backend
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_seconds = lease_seconds
        self.consumer_service_id = consumer_service_id
        self.consumer_id = consumer_id or _default_page_control_consumer_id(
            consumer_service_id=consumer_service_id,
            outbox_path=outbox.path,
        )
        self.canvas_publication_signer = canvas_publication_signer
        self.canvas_publication_keyring = canvas_publication_keyring

    def drain(self, *, limit: int) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            return self._drain_locked(limit=limit)

    def _drain_locked(self, *, limit: int) -> tuple[PageControlReceipt, ...]:
        receipts: list[PageControlReceipt] = []
        for claim in self.outbox.claim_records(
            limit=limit,
            owner_id=self.consumer_id,
            lease_seconds=self.lease_seconds,
            now=self.clock(),
        ):
            try:
                outcome = self._execute_claim(claim)
            except _RetryableCommittedLocalEffectError:
                receipts.append(
                    self.outbox.release_claim_for_retry(
                        claim.command.command_id,
                        owner_id=claim.owner_id,
                        claim_token=claim.claim_token,
                    )
                )
            except Exception as exc:
                receipts.append(
                    self.outbox.complete(
                        claim.command.command_id,
                        error=f"{type(exc).__name__}: {exc}",
                        owner_id=claim.owner_id,
                        claim_token=claim.claim_token,
                    )
                )
            else:
                receipts.append(
                    self.outbox.complete(
                        claim.command.command_id,
                        result=outcome.result,
                        error=outcome.error,
                        status=outcome.status,
                        owner_id=claim.owner_id,
                        claim_token=claim.claim_token,
                    )
                )
        return tuple(receipts)

    def _consumer_mutex_path(self) -> Path:
        return self.outbox.path.with_name(f"{self.outbox.path.name}{_CONSUMER_MUTEX_SUFFIX}")

    def _assert_command_time(self, command: PageControlCommandValue) -> None:
        observed_at = _normalize_utc(self.clock())
        if command.requested_at > observed_at + _MAX_REQUEST_FUTURE_SKEW:
            raise ValueError("page control command requested_at exceeds allowed future clock skew")

    def _execute_claim(self, claim: PageControlClaim) -> _ExecutionOutcome:
        command = claim.command
        self._assert_command_time(command)
        effect, created = self.outbox.begin_effect(
            command,
            owner_id=claim.owner_id,
            claim_token=claim.claim_token,
            now=self.clock(),
        )
        terminal = self._outcome_from_effect(effect)
        if terminal is not None:
            return terminal
        local_fence_targets = self._local_effect_fence_targets(command)
        if created and local_fence_targets:
            try:
                local_fence = self._local_effect_fence(local_fence_targets)
                effect = self.outbox.record_started_effect_result(
                    command.command_id,
                    result=local_fence,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
            except Exception as exc:
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        if local_fence_targets:
            mismatch_reason = self._local_effect_fence_mismatch_reason(
                effect,
                local_fence_targets,
            )
            if mismatch_reason is not None:
                result = _ambiguous_local_effect_result(command, reason=mismatch_reason)
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.AMBIGUOUS,
                    result=result,
                    error=mismatch_reason,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        try:
            bindings = self._bind_local_effect_directories(
                effect,
                local_fence_targets,
            )
        except Exception as exc:
            reason = f"local filesystem effect directory binding failed: {exc}"
            result = _ambiguous_local_effect_result(command, reason=reason)
            effect = self.outbox.finish_effect(
                command.command_id,
                status=PageControlEffectStatus.AMBIGUOUS,
                result=result,
                error=reason,
                owner_id=claim.owner_id,
                claim_token=claim.claim_token,
            )
            outcome = self._outcome_from_effect(effect)
            assert outcome is not None
            return outcome
        binding_token = _ACTIVE_EFFECT_DIRECTORY_BINDINGS.set(bindings)
        try:
            return self._execute_bound_claim(claim, effect=effect, created=created)
        finally:
            _ACTIVE_EFFECT_DIRECTORY_BINDINGS.reset(binding_token)
            for binding in bindings.values():
                binding.close()

    def _execute_bound_claim(
        self,
        claim: PageControlClaim,
        *,
        effect: PageControlEffectRecord,
        created: bool,
    ) -> _ExecutionOutcome:
        command = claim.command
        if not created:
            try:
                recovered = self._recover_started_effect(command)
            except Exception as exc:
                if self._has_committed_local_mutation(command):
                    raise _RetryableCommittedLocalEffectError(
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            if recovered is not None:
                self._verify_bound_effect_directories()
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.SUCCEEDED,
                    result=recovered,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            if _is_external_lab_effect(command):
                result = _ambiguous_lab_effect_result(command)
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.AMBIGUOUS,
                    result=result,
                    error="external Lab effect started without a durable result",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        try:
            result = self._execute(command)
        except Exception as exc:
            try:
                recovered = self._recover_started_effect(command)
            except Exception as recovery_exc:
                if self._has_committed_local_mutation(command):
                    raise _RetryableCommittedLocalEffectError(
                        f"{type(exc).__name__}: {exc}; recovery failed: "
                        f"{type(recovery_exc).__name__}: {recovery_exc}"
                    ) from recovery_exc
                recovered = None
            if recovered is not None:
                self._verify_bound_effect_directories()
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.SUCCEEDED,
                    result=recovered,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            effect = self.outbox.finish_effect(
                command.command_id,
                status=PageControlEffectStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
                owner_id=claim.owner_id,
                claim_token=claim.claim_token,
            )
            outcome = self._outcome_from_effect(effect)
            assert outcome is not None
            return outcome
        self._verify_bound_effect_directories()
        effect = self.outbox.finish_effect(
            command.command_id,
            status=PageControlEffectStatus.SUCCEEDED,
            result=result,
            owner_id=claim.owner_id,
            claim_token=claim.claim_token,
        )
        outcome = self._outcome_from_effect(effect)
        assert outcome is not None
        return outcome

    def _bind_local_effect_directories(
        self,
        effect: PageControlEffectRecord,
        targets: tuple[_LocalEffectFenceTarget, ...],
    ) -> dict[Path, _BoundManagedDirectory]:
        if not targets:
            return {}
        result = effect.result
        if not isinstance(result, dict) or not isinstance(result.get("targets"), list):
            raise ValueError("started directory fence is unavailable")
        raw_by_path = {
            Path(os.path.abspath(str(item["path"]))): item
            for item in result["targets"]
            if isinstance(item, Mapping) and isinstance(item.get("path"), str)
        }
        bindings: dict[Path, _BoundManagedDirectory] = {}
        try:
            for target in targets:
                path = Path(os.path.abspath(target.path))
                raw = raw_by_path[path]
                if raw.get("missing") is True:
                    continue
                binding = _bind_managed_directory(path, create=False)
                observed = os.fstat(binding.descriptor)
                if observed.st_dev != raw.get("st_dev") or observed.st_ino != raw.get("st_ino"):
                    binding.close()
                    raise ValueError(f"fenced directory generation changed: {path}")
                bindings[path] = binding
            return bindings
        except Exception:
            for binding in bindings.values():
                binding.close()
            raise

    @staticmethod
    def _verify_bound_effect_directories() -> None:
        for binding in (_ACTIVE_EFFECT_DIRECTORY_BINDINGS.get() or {}).values():
            binding.verify()

    @staticmethod
    def _open_effect_directory(path: Path, *, create: bool) -> int:
        normalized = Path(os.path.abspath(path))
        binding = (_ACTIVE_EFFECT_DIRECTORY_BINDINGS.get() or {}).get(normalized)
        if binding is not None:
            return binding.duplicate()
        return (
            _open_or_create_managed_directory(normalized)
            if create
            else _open_existing_managed_directory(normalized)
        )

    @staticmethod
    def _bound_effect_directory_descriptor(path: Path) -> int | None:
        binding = (_ACTIVE_EFFECT_DIRECTORY_BINDINGS.get() or {}).get(Path(os.path.abspath(path)))
        if binding is None:
            return None
        binding.verify()
        return binding.descriptor

    def _has_committed_local_mutation(self, command: PageControlCommandValue) -> bool:
        if not isinstance(command, DeleteCanvas):
            return False
        try:
            head = self._current_canvas_head(command.name)
        except Exception:
            return False
        return (
            head is not None
            and head.state == "deleted"
            and head.receipt.claims.command.command_id == command.command_id
            and head.authority_command_kind == command.kind
            and head.authority_command_hash == _command_hash(command)
        )

    @staticmethod
    def _outcome_from_effect(
        effect: PageControlEffectRecord,
    ) -> _ExecutionOutcome | None:
        if effect.status == PageControlEffectStatus.STARTED:
            return None
        if effect.status == PageControlEffectStatus.SUCCEEDED:
            return _ExecutionOutcome(PageControlStatus.SUCCEEDED, effect.result)
        if effect.status == PageControlEffectStatus.AMBIGUOUS:
            return _ExecutionOutcome(
                PageControlStatus.AMBIGUOUS,
                effect.result,
                effect.error,
            )
        return _ExecutionOutcome(PageControlStatus.FAILED, effect.result, effect.error)

    def _execute(self, command: PageControlCommandValue) -> JsonValue:
        if isinstance(command, SaveCanvas):
            return self._save_canvas(command)
        if isinstance(command, DeleteCanvas):
            return self._delete_canvas(command)
        if isinstance(command, SetCanvasPoolRefs):
            existing, _publication = self._read_verified_canvas_catalog(command.name)
            save = SaveCanvas(
                command_id=command.command_id,
                requested_at=command.requested_at,
                name=command.name,
                description=existing.description,
                pool_refs=command.pool_refs,
                source="canvas_edit",
            )
            return self._save_canvas(save, identity_command=command)
        if isinstance(command, SaveUserPool):
            result = self._save_user_pool(command)
            if command.canvas_name is not None and command.canvas_name != "__default__":
                canvas_result = self._add_pool_to_canvas(
                    command.canvas_name,
                    f"user/{command.base_name}",
                    identity_command=command,
                )
                if isinstance(result, dict):
                    result = dict(result)
                    result["canvas_result"] = canvas_result
            return result
        if isinstance(command, DeleteUserPool):
            return {"deleted": self._delete(self._user_pool_path(command.base_name))}
        if isinstance(command, ForkBuiltinPool):
            return self._fork_builtin(command)
        if isinstance(command, SaveNlPreset):
            if not command.overwrite and self._managed_json_exists(
                self._user_pool_path(command.name)
            ):
                raise FileExistsError(f"preset already exists: {command.name}")
            save = SaveUserPool(
                command_id=command.command_id,
                requested_at=command.requested_at,
                base_name=command.name,
                description=command.description,
                rule_calls=command.rule_calls,
                include_columns=command.include_columns,
                source="nl_input",
            )
            return self._save_user_pool(save, identity_command=command)
        if isinstance(command, AppendNlQueryLog):
            return self._append_nl_log(command)
        if isinstance(command, InitializeLabExports):
            return self._initialize_lab_exports(command)
        if isinstance(command, SubmitLabCommand):
            return self._lab_backend().submit_command(
                command.command,
                interaction_key=command.interaction_key,
            )
        if isinstance(command, ExportLabArtifactZip):
            return self._lab_backend().export_zip(command.job_id)
        if isinstance(command, DiscardLabArtifactZip):
            return self._lab_backend().discard_zip(command)
        raise TypeError(f"unsupported page control command: {type(command).__name__}")

    def _lab_backend(self) -> LabPageControlBackend:
        if self.lab_backend is None:
            raise RuntimeError("Lab page control backend is unavailable")
        return self.lab_backend

    def _local_effect_fence_targets(
        self,
        command: PageControlCommandValue,
    ) -> tuple[_LocalEffectFenceTarget, ...]:
        if isinstance(command, SaveCanvas):
            return self._canvas_publication_fence_targets(command.name)
        if isinstance(command, SetCanvasPoolRefs):
            return self._canvas_publication_fence_targets(command.name)
        if isinstance(command, SaveUserPool):
            targets = [
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.base_name).parent,
                )
            ]
            if command.canvas_name is not None and command.canvas_name != "__default__":
                targets.extend(self._canvas_publication_fence_targets(command.canvas_name))
            return tuple(targets)
        if isinstance(command, SaveNlPreset):
            return (
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.name).parent,
                ),
            )
        if isinstance(command, ForkBuiltinPool):
            targets = [
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.target_base_name).parent,
                )
            ]
            if command.canvas_name is not None and command.canvas_name != "__default__":
                targets.extend(self._canvas_publication_fence_targets(command.canvas_name))
            return tuple(targets)
        if isinstance(command, AppendNlQueryLog):
            return (
                _LocalEffectFenceTarget(
                    role="nl_query_log_directory",
                    path=self.log_dir,
                ),
            )
        if isinstance(command, DeleteCanvas):
            return self._canvas_publication_fence_targets(
                command.name,
                create_canvas=False,
            )
        if isinstance(command, DeleteUserPool):
            return (
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.base_name).parent,
                    create=False,
                ),
            )
        return ()

    def _canvas_publication_fence_targets(
        self,
        canvas_name: str,
        *,
        create_canvas: bool = True,
    ) -> tuple[_LocalEffectFenceTarget, ...]:
        return (
            _LocalEffectFenceTarget(
                role="canvas_directory",
                path=self._canvas_path(canvas_name).parent,
                create=create_canvas,
            ),
            _LocalEffectFenceTarget(
                role="canvas_receipt_directory",
                path=self.data_dir / "canvas-publication-receipts",
            ),
            _LocalEffectFenceTarget(
                role="canvas_head_directory",
                path=self._canvas_head_root(canvas_name),
            ),
            _LocalEffectFenceTarget(
                role="canvas_watermark_directory",
                path=self._canvas_watermark_root(canvas_name),
            ),
        )

    def _local_effect_fence(
        self,
        targets: tuple[_LocalEffectFenceTarget, ...],
    ) -> dict[str, object]:
        return {
            "schema_version": _LOCAL_FILESYSTEM_FENCE_SCHEMA_VERSION,
            "kind": "local_filesystem_fence",
            "targets": [self._local_effect_target_identity(target) for target in targets],
        }

    @staticmethod
    def _local_effect_target_identity(
        target: _LocalEffectFenceTarget,
    ) -> dict[str, object]:
        path = Path(os.path.abspath(target.path))
        descriptor: int | None = None
        try:
            try:
                descriptor = (
                    _open_or_create_managed_directory(path)
                    if target.create
                    else _open_existing_managed_directory(path)
                )
            except FileNotFoundError:
                if target.create:
                    raise
                return {
                    "role": target.role,
                    "path": str(path),
                    "missing": True,
                }
            _verify_open_directory_matches_path(path, descriptor)
            observed = os.fstat(descriptor)
            return {
                "role": target.role,
                "path": str(path),
                "st_dev": observed.st_dev,
                "st_ino": observed.st_ino,
            }
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _local_effect_fence_mismatch_reason(
        self,
        effect: PageControlEffectRecord,
        targets: tuple[_LocalEffectFenceTarget, ...],
    ) -> str | None:
        result = effect.result
        if not isinstance(result, dict):
            return "local filesystem effect lacks a started directory fence"
        if result.get("schema_version") != _LOCAL_FILESYSTEM_FENCE_SCHEMA_VERSION:
            return "local filesystem effect has an unsupported directory fence"
        if result.get("kind") != "local_filesystem_fence":
            return "local filesystem effect has an unsupported fence kind"
        raw_targets = result.get("targets")
        if not isinstance(raw_targets, list):
            return "local filesystem effect has an invalid directory fence"
        expected_paths = {str(Path(os.path.abspath(target.path))) for target in targets}
        observed_by_path: dict[str, Mapping[str, object]] = {}
        for raw_target in raw_targets:
            if not isinstance(raw_target, Mapping):
                return "local filesystem effect has an invalid directory fence target"
            path_value = raw_target.get("path")
            if not isinstance(path_value, str):
                return "local filesystem effect has an invalid directory fence path"
            observed_by_path[str(Path(os.path.abspath(path_value)))] = raw_target
        if set(observed_by_path) != expected_paths:
            return "local filesystem effect directory fence targets do not match command"
        for target in targets:
            reason = self._local_effect_target_mismatch_reason(
                target,
                observed_by_path[str(Path(os.path.abspath(target.path)))],
            )
            if reason is not None:
                return reason
        return None

    @staticmethod
    def _local_effect_target_mismatch_reason(
        target: _LocalEffectFenceTarget,
        observed_target: Mapping[str, object],
    ) -> str | None:
        path = Path(os.path.abspath(target.path))
        if observed_target.get("missing") is True:
            try:
                os.stat(path, follow_symlinks=False)
            except FileNotFoundError:
                return None
            return "local filesystem effect target directory appeared after start"
        st_dev = observed_target.get("st_dev")
        st_ino = observed_target.get("st_ino")
        if not isinstance(st_dev, int) or not isinstance(st_ino, int):
            return "local filesystem effect has an invalid directory identity"
        binding: _BoundManagedDirectory | None = None
        try:
            binding = _bind_managed_directory(path, create=False)
            binding.verify()
            current = os.fstat(binding.descriptor)
        except Exception:
            return "local filesystem effect target directory cannot be verified"
        finally:
            if binding is not None:
                binding.close()
        if current.st_dev != st_dev or current.st_ino != st_ino:
            return "local filesystem effect target directory changed after start"
        return None

    def _recover_started_effect(self, command: PageControlCommandValue) -> JsonValue | None:
        if isinstance(command, SaveCanvas):
            return self._recover_canvas_result(self._canvas_path(command.name), command)
        if isinstance(command, SetCanvasPoolRefs):
            return self._recover_canvas_result(self._canvas_path(command.name), command)
        if isinstance(command, SaveUserPool):
            result = self._recover_user_pool_result(command, identity_command=command)
            if result is None:
                return None
            if command.canvas_name is not None and command.canvas_name != "__default__":
                canvas_result = self._recover_canvas_pool_link(command)
                if canvas_result is None:
                    canvas_result = self._add_pool_to_canvas(
                        command.canvas_name,
                        f"user/{command.base_name}",
                        identity_command=command,
                    )
                if isinstance(result, dict):
                    result = dict(result)
                    result["canvas_result"] = canvas_result
            return result
        if isinstance(command, SaveNlPreset):
            save = SaveUserPool(
                command_id=command.command_id,
                requested_at=command.requested_at,
                base_name=command.name,
                description=command.description,
                rule_calls=command.rule_calls,
                include_columns=command.include_columns,
                source="nl_input",
            )
            return self._recover_user_pool_result(save, identity_command=command)
        if isinstance(command, ForkBuiltinPool):
            save = self._fork_builtin_save_command(command)
            result = self._recover_user_pool_result(save, identity_command=command)
            if result is None:
                return None
            if command.canvas_name is not None and command.canvas_name != "__default__":
                canvas_result = self._recover_canvas_pool_link(command)
                if canvas_result is None:
                    canvas_result = self._add_pool_to_canvas(
                        command.canvas_name,
                        f"user/{command.target_base_name}",
                        identity_command=command,
                    )
                if isinstance(result, dict):
                    result = dict(result)
                    result["canvas_result"] = canvas_result
            return result
        if isinstance(command, AppendNlQueryLog):
            if _managed_jsonl_contains_command_id(
                self.log_dir / "nl_queries.jsonl",
                command.command_id,
            ):
                return {"path": str(self.log_dir / "nl_queries.jsonl")}
            return None
        if isinstance(command, DeleteCanvas):
            return self._recover_delete_canvas(command)
        if isinstance(command, DeleteUserPool):
            return self._recover_delete_result(self._user_pool_path(command.base_name))
        if isinstance(command, InitializeLabExports):
            return self._recover_lab_exports(command)
        return None

    def _save_canvas(
        self,
        command: SaveCanvas,
        *,
        identity_command: PageControlCommandValue | None = None,
    ) -> JsonValue:
        if command.name == "__default__":
            raise ValueError("default canvas is virtual and cannot be persisted")
        identity = command if identity_command is None else identity_command
        path = self._canvas_path(command.name)
        existing: CanvasPublicationCatalogRecord | None = None
        catalog_exists = self._managed_json_exists(path)
        current_head = self._current_canvas_head(command.name)
        if catalog_exists:
            existing, _existing_publication = self._read_verified_canvas_catalog(
                command.name,
                expected_head=current_head,
            )
        else:
            self._assert_canvas_head_matches_latest_authority(
                command.name,
                current_head,
            )
            if current_head is not None and current_head.state == "active":
                raise ValueError("canvas current head is active but catalog is missing")
        if (
            current_head is not None
            and identity.requested_at < current_head.receipt.claims.command.requested_at
        ):
            raise ValueError("canvas current head is newer than the requested update")
        publication_command = self._canvas_publication_command(
            command,
            identity_command=identity,
        )
        requested_at = publication_command.requested_at
        created_at = existing.created_at if existing is not None else requested_at
        signer, keyring = self._require_canvas_publication_authority()
        if getattr(signer, "key_id", None) != keyring.active_key_id:
            raise ValueError("CanvasPublicationReceipt signer key must be active")
        publication = signer.issue_publication(
            build_canvas_publication_claims(
                command=publication_command,
                catalog_created_at=created_at,
                catalog_updated_at=requested_at,
                consumer_service_id=self.consumer_service_id,
                consumer_instance_id=self.consumer_id,
            )
        )
        if not keyring.verify_publication_receipt(publication, require_active=True):
            raise ValueError("CanvasPublicationReceipt active signature verification failed")
        self._canvas_publication_receipt_store().write_immutable(publication)
        self._atomic_json(
            path,
            publication.claims.catalog_record.model_dump(mode="json"),
            command_id=identity.command_id,
        )
        self._publish_canvas_head(
            identity_command=identity,
            canvas_name=command.name,
            state="active",
            publication_receipt_id=publication.receipt_id,
        )
        return self._canvas_publication_result(path, publication)

    def _save_user_pool(
        self,
        command: SaveUserPool,
        *,
        identity_command: PageControlCommandValue | None = None,
    ) -> JsonValue:
        identity = command if identity_command is None else identity_command
        path = self._user_pool_path(command.base_name)
        payload = {
            "name": command.base_name,
            "description": command.description,
            "rules": [rule.model_dump(mode="json") for rule in command.rule_calls],
            "include_columns": list(command.include_columns),
            "updated_at": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
            "source": command.source,
            "command_id": identity.command_id,
            "command_hash": _command_hash(identity),
        }
        self._atomic_json(path, payload, command_id=identity.command_id)
        return {"path": str(path)}

    def _fork_builtin(self, command: ForkBuiltinPool) -> JsonValue:
        save = self._fork_builtin_save_command(command)
        path = self._user_pool_path(command.target_base_name)
        if self._managed_json_exists(path):
            recovered = self._recover_user_pool_result(save, identity_command=command)
            if recovered is not None:
                if command.canvas_name is not None and command.canvas_name != "__default__":
                    canvas_result = self._recover_canvas_pool_link(command)
                    if canvas_result is None:
                        canvas_result = self._add_pool_to_canvas(
                            command.canvas_name,
                            f"user/{command.target_base_name}",
                            identity_command=command,
                        )
                    if isinstance(recovered, dict):
                        recovered = dict(recovered)
                        recovered["canvas_result"] = canvas_result
                return recovered
            raise FileExistsError(f"user/{command.target_base_name} already exists")
        result = self._save_user_pool(save, identity_command=command)
        if command.canvas_name is not None and command.canvas_name != "__default__":
            canvas_result = self._add_pool_to_canvas(
                command.canvas_name,
                f"user/{command.target_base_name}",
                identity_command=command,
            )
            if isinstance(result, dict):
                result = dict(result)
                result["canvas_result"] = canvas_result
        return result

    def _fork_builtin_save_command(self, command: ForkBuiltinPool) -> SaveUserPool:
        from rquant.presets import PRESET_SCREENS

        if command.builtin_name not in PRESET_SCREENS:
            raise KeyError(f"unknown preset: {command.builtin_name}")
        preset = PRESET_SCREENS[command.builtin_name]
        if not preset.rule_calls:
            raise ValueError("builtin preset has no immutable rule metadata")
        return SaveUserPool(
            command_id=command.command_id,
            requested_at=command.requested_at,
            base_name=command.target_base_name,
            description=f"Fork from builtin/{command.builtin_name}: {preset.description}",
            rule_calls=tuple(preset.rule_calls),
            include_columns=tuple(preset.include_columns),
            source="fork_from_builtin",
            canvas_name=command.canvas_name,
        )

    def _add_pool_to_canvas(
        self,
        canvas_name: str,
        pool_name: str,
        *,
        identity_command: PageControlCommandValue | None = None,
    ) -> JsonValue:
        current, _publication = self._read_verified_canvas_catalog(canvas_name)
        pool_refs = list(current.pool_refs)
        if pool_name not in pool_refs:
            pool_refs.append(pool_name)
        save = SaveCanvas(
            command_id=f"canvas-{canonical_sha256([canvas_name, pool_name])}",
            requested_at=datetime.now(UTC),
            name=canvas_name,
            description=current.description,
            pool_refs=tuple(str(value) for value in pool_refs),
            source="canvas_edit",
        )
        return self._save_canvas(save, identity_command=identity_command)

    def _recover_canvas_result(
        self,
        path: Path,
        identity_command: PageControlCommandValue,
    ) -> JsonValue | None:
        if not self._managed_json_exists(path):
            return None
        try:
            record, publication = self._read_verified_canvas_catalog(
                path.stem,
                expected_head=None,
                require_current_head=False,
            )
        except ValueError:
            raise
        if record.command_id != identity_command.command_id:
            return None
        if publication.claims.command.command_id != identity_command.command_id:
            return None
        self._publish_canvas_head(
            identity_command=identity_command,
            canvas_name=record.name,
            state="active",
            publication_receipt_id=publication.receipt_id,
        )
        return self._canvas_publication_result(path, publication)

    def _recover_user_pool_result(
        self,
        command: SaveUserPool,
        *,
        identity_command: PageControlCommandValue,
    ) -> JsonValue | None:
        path = self._user_pool_path(command.base_name)
        if not self._managed_json_exists(path):
            return None
        raw = self._read_json(path)
        if raw.get("command_id") != identity_command.command_id:
            return None
        if raw.get("command_hash") != _command_hash(identity_command):
            return None
        return {"path": str(path)}

    def _recover_canvas_pool_link(
        self,
        command: SaveUserPool | ForkBuiltinPool,
    ) -> JsonValue | None:
        canvas_name = command.canvas_name
        if canvas_name is None or canvas_name == "__default__":
            return None
        path = self._canvas_path(canvas_name)
        recovered = self._recover_canvas_result(path, command)
        if recovered is None:
            return None
        raw = self._read_json(path)
        base_name = (
            command.base_name if isinstance(command, SaveUserPool) else command.target_base_name
        )
        if f"user/{base_name}" not in raw.get("pool_refs", []):
            return None
        return recovered

    def _read_verified_canvas_catalog(
        self,
        canvas_name: str,
        *,
        expected_head: CanvasCurrentHead | None = None,
        require_current_head: bool = True,
    ) -> tuple[CanvasPublicationCatalogRecord, CanvasPublicationReceipt]:
        path = self._canvas_path(canvas_name)
        try:
            raw = self._read_json(path)
            record = CanvasPublicationCatalogRecord.model_validate(raw)
            publication = self._canvas_publication_receipt_store().read(
                record.publication_receipt_id
            )
            _signer, keyring = self._require_canvas_publication_authority()
            if not keyring.verify_publication_receipt(publication, require_active=True):
                raise ValueError("CanvasPublicationReceipt active signature verification failed")
            if publication.claims.catalog_record != record:
                raise ValueError("CanvasPublicationReceipt catalog semantics do not match")
            if publication.claims.command.name != canvas_name:
                raise ValueError("CanvasPublicationReceipt canvas name does not match")
            if require_current_head:
                head = expected_head or self._current_canvas_head(canvas_name)
                if (
                    head is None
                    or head.state != "active"
                    or head.publication_receipt_id != publication.receipt_id
                ):
                    raise ValueError(
                        "CanvasPublicationReceipt catalog semantics do not match current head"
                    )
                self._assert_canvas_head_matches_latest_authority(canvas_name, head)
            return record, publication
        except Exception as exc:
            if isinstance(exc, ValueError) and "catalog semantics" in str(exc):
                raise
            raise ValueError(
                f"CanvasPublicationReceipt catalog semantics cannot be verified: {exc}"
            ) from exc

    def _canvas_head_root(self, canvas_name: str) -> Path:
        return (
            self.data_dir
            / "canvas-publication-heads"
            / _validated_name(
                canvas_name,
                label="canvas name",
            )
        )

    def _canvas_watermark_root(self, canvas_name: str) -> Path:
        return (
            self.data_dir
            / _CANVAS_WATERMARK_DIRECTORY
            / _validated_name(
                canvas_name,
                label="canvas name",
            )
        )

    def _current_canvas_head(self, canvas_name: str) -> CanvasCurrentHead | None:
        _signer, keyring = self._require_canvas_publication_authority()
        canvas_root = self._canvas_head_root(canvas_name)
        return read_canvas_current_head(
            self.data_dir / "canvas-publication-heads",
            canvas_name,
            keyring,
            directory_descriptor=self._bound_effect_directory_descriptor(canvas_root),
        )

    def _current_canvas_watermark(self, canvas_name: str) -> CanvasCurrentHead | None:
        _signer, keyring = self._require_canvas_publication_authority()
        canvas_root = self._canvas_watermark_root(canvas_name)
        return read_canvas_current_head(
            self.data_dir / _CANVAS_WATERMARK_DIRECTORY,
            canvas_name,
            keyring,
            directory_descriptor=self._bound_effect_directory_descriptor(canvas_root),
        )

    @staticmethod
    def _assert_canvas_watermark_matches_head(
        head: CanvasCurrentHead | None,
        watermark: CanvasCurrentHead | None,
    ) -> None:
        if head is None and watermark is None:
            return
        if (
            head is None
            or watermark is None
            or head.receipt.receipt_id != watermark.receipt.receipt_id
            or head.sequence != watermark.sequence
            or head.state != watermark.state
            or head.publication_receipt_id != watermark.publication_receipt_id
        ):
            raise ValueError("canvas current head does not match immutable watermark authority")

    def _assert_canvas_head_matches_latest_authority(
        self,
        canvas_name: str,
        head: CanvasCurrentHead | None,
    ) -> None:
        latest = self.outbox.latest_succeeded_canvas_mutation(canvas_name)
        if latest is None:
            if head is None:
                return
            raise ValueError("canvas current head lacks authoritative PageControl command history")
        if (
            head is None
            or head.receipt.claims.command.command_id != latest.command_id
            or head.authority_command_kind != latest.kind
            or head.authority_command_hash != _command_hash(latest)
        ):
            raise ValueError(
                "canvas current head does not match latest PageControl command authority"
            )

    def _publish_canvas_head(
        self,
        *,
        identity_command: PageControlCommandValue,
        canvas_name: str,
        state: Literal["active", "deleted"],
        publication_receipt_id: str | None,
    ) -> CanvasCurrentHead:
        signer, keyring = self._require_canvas_publication_authority()
        if getattr(signer, "key_id", None) != keyring.active_key_id:
            raise ValueError("CanvasPublicationReceipt signer key must be active")
        current = self._current_canvas_head(canvas_name)
        watermark = self._current_canvas_watermark(canvas_name)
        self._assert_canvas_watermark_matches_head(current, watermark)
        authority_hash = _command_hash(identity_command)
        if current is not None and current.receipt.claims.command.command_id == (
            identity_command.command_id
        ):
            if (
                current.state != state
                or current.publication_receipt_id != publication_receipt_id
                or current.authority_command_kind != identity_command.kind
                or current.authority_command_hash != authority_hash
            ):
                raise ValueError("canvas current head command identity conflicts")
            self._publish_canvas_watermark(current)
            return current
        if (
            current is not None
            and identity_command.requested_at < current.receipt.claims.command.requested_at
        ):
            raise ValueError("canvas current head is newer than the requested update")
        payload = {
            "authority_command_hash": authority_hash,
            "authority_command_kind": identity_command.kind,
            "canvas_name": canvas_name,
            "contract": _CANVAS_HEAD_CONTRACT,
            "previous_head_receipt_id": (None if current is None else current.receipt.receipt_id),
            "publication_receipt_id": publication_receipt_id,
            "sequence": 1 if current is None else current.sequence + 1,
            "state": state,
        }
        description = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        head_command = CanvasPublicationCommand(
            command_id=identity_command.command_id,
            requested_at=identity_command.requested_at,
            name=canvas_name,
            description=description,
            pool_refs=(),
            source=_CANVAS_HEAD_SOURCE,
        )
        head_receipt = signer.issue_publication(
            build_canvas_publication_claims(
                command=head_command,
                catalog_created_at=identity_command.requested_at,
                catalog_updated_at=identity_command.requested_at,
                consumer_service_id=self.consumer_service_id,
                consumer_instance_id=self.consumer_id,
            )
        )
        if not keyring.verify_publication_receipt(head_receipt, require_active=True):
            raise ValueError("canvas current head active signature verification failed")
        head_root = self._canvas_head_root(canvas_name)
        CanvasPublicationReceiptStore(
            head_root,
            directory_descriptor=self._bound_effect_directory_descriptor(head_root),
        ).write_immutable(head_receipt)
        published = self._current_canvas_head(canvas_name)
        if published is None or published.receipt.receipt_id != head_receipt.receipt_id:
            raise ValueError("canvas current head publication did not become authoritative")
        self._publish_canvas_watermark(published)
        return published

    def _publish_canvas_watermark(self, head: CanvasCurrentHead) -> None:
        current = self._current_canvas_watermark(head.receipt.claims.command.name)
        if current is not None and current.receipt.receipt_id == head.receipt.receipt_id:
            return
        expected_sequence = 1 if current is None else current.sequence + 1
        expected_previous = None if current is None else current.receipt.receipt_id
        if head.sequence != expected_sequence or head.previous_head_receipt_id != expected_previous:
            raise ValueError("canvas immutable watermark would fork or roll back")
        watermark_root = self._canvas_watermark_root(head.receipt.claims.command.name)
        CanvasPublicationReceiptStore(
            watermark_root,
            directory_descriptor=self._bound_effect_directory_descriptor(watermark_root),
        ).write_immutable(head.receipt)
        published = self._current_canvas_watermark(head.receipt.claims.command.name)
        if published is None or published.receipt.receipt_id != head.receipt.receipt_id:
            raise ValueError("canvas immutable watermark publication failed")

    def _delete_canvas(self, command: DeleteCanvas) -> JsonValue:
        path = self._canvas_path(command.name)
        catalog_exists = self._managed_json_exists(path)
        current = self._current_canvas_head(command.name)
        if catalog_exists:
            self._read_verified_canvas_catalog(command.name, expected_head=current)
        else:
            self._assert_canvas_head_matches_latest_authority(command.name, current)
            if current is not None and current.state == "active":
                raise ValueError("canvas current head is active but catalog is missing")
        self._publish_canvas_head(
            identity_command=command,
            canvas_name=command.name,
            state="deleted",
            publication_receipt_id=None,
        )
        self._delete(path)
        return {"deleted": True}

    def _recover_delete_canvas(self, command: DeleteCanvas) -> JsonValue | None:
        current = self._current_canvas_head(command.name)
        if (
            current is None
            or current.receipt.claims.command.command_id != command.command_id
            or current.state != "deleted"
            or current.authority_command_kind != command.kind
            or current.authority_command_hash != _command_hash(command)
        ):
            return None
        self._publish_canvas_watermark(current)
        path = self._canvas_path(command.name)
        if self._managed_json_exists(path):
            self._delete(path)
        return {"deleted": True}

    def _require_canvas_publication_authority(
        self,
    ) -> tuple[CanvasPublicationSigner, CanvasPublicationKeyring]:
        if self.canvas_publication_signer is None or self.canvas_publication_keyring is None:
            raise RuntimeError("CanvasPublicationReceipt signer and public keyring are required")
        return self.canvas_publication_signer, self.canvas_publication_keyring

    def _canvas_publication_receipt_store(self) -> CanvasPublicationReceiptStore:
        root = self.data_dir / "canvas-publication-receipts"
        return CanvasPublicationReceiptStore(
            root,
            directory_descriptor=self._bound_effect_directory_descriptor(root),
        )

    @staticmethod
    def _canvas_publication_command(
        command: SaveCanvas,
        *,
        identity_command: PageControlCommandValue,
    ) -> CanvasPublicationCommand:
        return CanvasPublicationCommand(
            command_id=identity_command.command_id,
            requested_at=identity_command.requested_at,
            name=command.name,
            description=command.description,
            pool_refs=command.pool_refs,
            source=command.source,
        )

    @staticmethod
    def _canvas_publication_result(
        path: Path,
        publication: CanvasPublicationReceipt,
    ) -> dict[str, object]:
        claims = publication.claims
        return {
            "path": str(path),
            "command_hash": claims.command_hash,
            "source_identity_hash": claims.source_identity_hash,
            "record_hash": claims.catalog_record_hash,
            "publication_generation_id": claims.generation_id,
            "publication_receipt_id": publication.receipt_id,
            "publication_receipt_hash": publication.receipt_hash,
            "publication_effect_id": claims.effect_id,
            "publication_key_id": publication.key_id,
            "publication_receipt_path": str(
                path.parent.parent
                / "canvas-publication-receipts"
                / f"{publication.receipt_id}.json"
            ),
        }

    def _recover_delete_result(self, path: Path) -> JsonValue | None:
        if not self._managed_json_exists(path):
            return {"deleted": True}
        return None

    def _recover_lab_exports(self, command: InitializeLabExports) -> JsonValue | None:
        requested = (
            Path(os.path.abspath(command.export_root)),
            Path(os.path.abspath(command.runtime_root)),
        )
        if any(path not in self.allowed_lab_export_roots for path in requested):
            return None
        descriptors: list[int] = []
        try:
            for path in requested:
                if path.exists():
                    descriptor = _open_existing_managed_directory(path)
                else:
                    descriptor = _open_or_create_managed_directory(path)
                _verify_open_directory_matches_path(path, descriptor)
                descriptors.append(descriptor)
            for descriptor in descriptors:
                _fsync_descriptor(descriptor)
            for path, descriptor in zip(requested, descriptors, strict=True):
                _verify_open_directory_matches_path(path, descriptor)
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
        return {"paths": [str(path) for path in requested]}

    def _append_nl_log(self, command: AppendNlQueryLog) -> JsonValue:
        path = self.log_dir / "nl_queries.jsonl"
        record = {
            "command_id": command.command_id,
            "ts": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
            "query": command.query,
            "plan": command.plan,
            "outcome": command.outcome,
            "error": command.error,
        }
        _append_managed_jsonl(path, record, command_id=command.command_id)
        return {"path": str(path)}

    def _initialize_lab_exports(self, command: InitializeLabExports) -> JsonValue:
        requested = (
            Path(os.path.abspath(command.export_root)),
            Path(os.path.abspath(command.runtime_root)),
        )
        if any(path not in self.allowed_lab_export_roots for path in requested):
            raise ValueError("Lab export directory is not allowlisted")
        descriptors: list[int] = []
        try:
            for path in requested:
                path.mkdir(
                    parents=True,
                    mode=_PRIVATE_DIRECTORY_MODE,
                    exist_ok=True,
                )
                descriptor = _open_or_create_managed_directory(path)
                _verify_open_directory_matches_path(path, descriptor)
                descriptors.append(descriptor)
            for path, descriptor in zip(requested, descriptors, strict=True):
                _verify_open_directory_matches_path(path, descriptor)
                _fsync_descriptor(descriptor)
                _verify_open_directory_matches_path(path, descriptor)
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
        return {"paths": [str(path) for path in requested]}

    def _canvas_path(self, name: str) -> Path:
        return self.data_dir / "canvases" / f"{_validated_name(name, label='canvas name')}.json"

    def _user_pool_path(self, name: str) -> Path:
        return self.data_dir / "user_presets" / f"{_validated_name(name, label='pool name')}.json"

    @staticmethod
    def _read_json(path: Path) -> dict[str, object]:
        descriptor = PageControlConsumer._open_effect_directory(
            path.parent,
            create=False,
        )
        value = json.loads(
            _read_managed_file(path, directory_descriptor=descriptor).decode("utf-8")
        )
        if not isinstance(value, dict):
            raise ValueError(f"expected JSON object: {path}")
        return value

    @staticmethod
    def _delete(path: Path) -> bool:
        if not PageControlConsumer._managed_json_exists(path):
            return False
        descriptor = PageControlConsumer._open_effect_directory(
            path.parent,
            create=False,
        )
        try:
            _verify_open_directory_matches_path(path.parent, descriptor)
            os.unlink(path.name, dir_fd=descriptor)
            _verify_open_directory_matches_path(path.parent, descriptor)
            _fsync_descriptor(descriptor)
            _verify_open_directory_matches_path(path.parent, descriptor)
            return True
        finally:
            os.close(descriptor)

    @staticmethod
    def _atomic_json(path: Path, payload: object, *, command_id: str) -> None:
        descriptor = PageControlConsumer._open_effect_directory(
            path.parent,
            create=True,
        )
        temp_name = f".{path.name}.{canonical_sha256(command_id)[:12]}.{uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        file_descriptor: int | None = None
        try:
            _verify_open_directory_matches_path(path.parent, descriptor)
            file_descriptor = os.open(
                temp_name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=descriptor,
            )
            payload_bytes = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
                "utf-8"
            )
            with os.fdopen(file_descriptor, "wb", closefd=False) as handle:
                handle.write(payload_bytes)
                handle.flush()
            os.fsync(file_descriptor)
            os.fchmod(file_descriptor, _PRIVATE_FILE_MODE)
            _verify_open_directory_matches_path(path.parent, descriptor)
            os.replace(
                temp_name,
                path.name,
                src_dir_fd=descriptor,
                dst_dir_fd=descriptor,
            )
            _verify_open_directory_matches_path(path.parent, descriptor)
            _fsync_descriptor(descriptor)
            _verify_open_directory_matches_path(path.parent, descriptor)
        except Exception:
            with suppress(FileNotFoundError):
                os.unlink(temp_name, dir_fd=descriptor)
            raise
        finally:
            if file_descriptor is not None:
                os.close(file_descriptor)
            os.close(descriptor)

    @staticmethod
    def _managed_json_exists(path: Path) -> bool:
        try:
            descriptor = PageControlConsumer._open_effect_directory(
                path.parent,
                create=False,
            )
        except FileNotFoundError:
            return False
        try:
            _verify_open_directory_matches_path(path.parent, descriptor)
            try:
                observed = os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if stat.S_ISLNK(observed.st_mode):
                raise ValueError(f"managed JSON file cannot be a symlink: {path}")
            if not stat.S_ISREG(observed.st_mode):
                raise ValueError(f"managed JSON path is not a regular file: {path}")
            _verify_open_directory_matches_path(path.parent, descriptor)
            return True
        finally:
            os.close(descriptor)


class PageControlService:
    """Synchronous service boundary backed by the durable control outbox."""

    def __init__(
        self,
        *,
        outbox: PageControlOutbox,
        consumer: PageControlConsumer,
    ) -> None:
        self.outbox = outbox
        self.consumer = consumer

    def submit(self, command: PageControlCommandValue) -> PageControlReceipt:
        receipt = self.outbox.enqueue(command)
        for _ in range(100):
            if receipt.status in _PAGE_CONTROL_TERMINAL_STATUSES:
                return receipt
            drained = self.consumer.drain(limit=100)
            observed = self.outbox.receipt(command.command_id)
            if observed is None:
                raise RuntimeError("page control command disappeared")
            if observed.status is PageControlStatus.PENDING:
                return observed
            if not drained and observed.status is PageControlStatus.PROCESSING:
                return observed
            receipt = observed
        raise RuntimeError("page control command did not reach a terminal state")


PageControlTransport = Callable[[dict[str, object]], dict[str, object]]


class PageControlUnavailableError(RuntimeError):
    """The loopback control authority cannot accept a page command right now."""


class PageControlClient:
    """Page-side API client; it has no filesystem persistence capability."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        transport: PageControlTransport | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        self.endpoint = endpoint or os.environ.get(
            "RQUANT_PAGE_CONTROL_URL",
            "http://127.0.0.1:8767/v1/commands",
        )
        self.transport = transport or self._post
        self.timeout_seconds = timeout_seconds

    def submit(self, command: PageControlCommandValue) -> PageControlReceipt:
        try:
            response = self.transport(command.model_dump(mode="json"))
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise PageControlUnavailableError(
                f"page control service unavailable: {type(exc).__name__}: {exc}"
            ) from exc
        return PageControlReceipt.model_validate(response)

    def _post(self, payload: dict[str, object]) -> dict[str, object]:
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            body = json.loads(response.read().decode("utf-8"))
        if not isinstance(body, dict):
            raise RuntimeError("page control service returned a non-object response")
        return body


def _validated_name(value: str, *, label: str) -> str:
    candidate = value.strip()
    if candidate in {"", ".", ".."} or _SAFE_NAME.fullmatch(candidate) is None:
        raise ValueError(f"{label} contains unsafe characters")
    return candidate


def _command_hash(command: PageControlCommandValue) -> str:
    return canonical_sha256(command.model_dump(mode="json"))


def _default_page_control_consumer_id(
    *,
    consumer_service_id: str,
    outbox_path: Path,
) -> str:
    service_id = consumer_service_id.strip()
    if not service_id:
        raise ValueError("consumer_service_id is required")
    canonical_outbox_path = str(Path(os.path.abspath(outbox_path)).resolve(strict=False))
    instance_hash = canonical_sha256(
        {
            "contract": "page-control-consumer-instance/v1",
            "consumer_service_id": service_id,
            "outbox_path": canonical_outbox_path,
        }
    )
    return f"{service_id}:{instance_hash}"


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone aware")
    return value.astimezone(UTC)


def _parse_canvas_timestamp(value: str) -> datetime:
    return _normalize_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _assert_canvas_receipt_not_future(
    receipt: CanvasPublicationReceipt,
    *,
    observed_at: datetime,
) -> None:
    observed = _normalize_utc(observed_at)
    claims = receipt.claims
    timestamps = (
        ("requested_at", claims.command.requested_at),
        ("created_at", claims.created_at),
        ("catalog created_at", claims.catalog_record.created_at),
        ("catalog updated_at", claims.catalog_record.updated_at),
    )
    for label, value in timestamps:
        if value > observed:
            raise ValueError(f"canvas publication receipt {label} contains future evidence")


def _canvas_source_identity_hash(
    *,
    command_id: str,
    command_hash: str,
    source: str,
) -> str:
    return canonical_sha256(
        {
            "schema_version": _CANVAS_CATALOG_SCHEMA_VERSION,
            "command_id": command_id,
            "command_hash": command_hash,
            "source": source,
        }
    )


def _open_or_create_managed_directory(path: Path) -> int:
    binding = _bind_managed_directory(path, create=True)
    try:
        return os.dup(binding.descriptor)
    finally:
        binding.close()


def _open_existing_managed_directory(path: Path) -> int:
    binding = _bind_managed_directory(path, create=False)
    try:
        return os.dup(binding.descriptor)
    finally:
        binding.close()


def _bind_managed_directory(path: Path, *, create: bool) -> _BoundManagedDirectory:
    normalized = Path(os.path.abspath(path))
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptors: list[int] = []
    component_names: list[str] = []
    try:
        descriptors.append(os.open(normalized.anchor, flags))
        components = normalized.parts[1:]
        for index, component in enumerate(components):
            parent = descriptors[-1]
            try:
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, _PRIVATE_DIRECTORY_MODE, dir_fd=parent)
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(entry.st_mode):
                raise ValueError(f"managed directory ancestor cannot be a symlink: {normalized}")
            if not stat.S_ISDIR(entry.st_mode):
                raise ValueError(f"managed directory ancestor is not a directory: {normalized}")
            descriptor = os.open(component, flags, dir_fd=parent)
            opened = os.fstat(descriptor)
            if _file_node_tuple(entry) != _file_node_tuple(opened):
                os.close(descriptor)
                raise ValueError(f"managed directory ancestor changed while opening: {normalized}")
            descriptors.append(descriptor)
            component_names.append(component)
            if index == len(components) - 1:
                os.fchmod(descriptor, _PRIVATE_DIRECTORY_MODE)
        binding = _BoundManagedDirectory(
            path=normalized,
            descriptors=tuple(descriptors),
            component_names=tuple(component_names),
        )
        binding.verify()
        return binding
    except FileNotFoundError:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise
    except OSError as exc:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise ValueError(f"managed directory cannot be opened safely: {normalized}") from exc
    except Exception:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise


def _read_managed_file(
    path: Path,
    *,
    directory_descriptor: int | None = None,
) -> bytes:
    directory = (
        _open_existing_managed_directory(path.parent)
        if directory_descriptor is None
        else directory_descriptor
    )
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    file_descriptor: int | None = None
    try:
        _verify_open_directory_matches_path(path.parent, directory)
        try:
            file_descriptor = os.open(path.name, flags, dir_fd=directory)
        except OSError as exc:
            raise ValueError(f"managed JSON file cannot be opened safely: {path}") from exc
        observed = os.fstat(file_descriptor)
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"managed JSON file cannot be a symlink: {path}")
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError(f"managed JSON path is not a regular file: {path}")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(file_descriptor, 1024 * 1024):
            total += len(chunk)
            if total > _MAX_MANAGED_JSON_BYTES:
                raise ValueError("managed JSON file exceeds its byte budget")
            chunks.append(chunk)
        after = os.fstat(file_descriptor)
        if _file_identity_tuple(after) != _file_identity_tuple(observed):
            raise ValueError(f"managed JSON file changed while reading: {path}")
        _verify_open_directory_matches_path(path.parent, directory)
        return b"".join(chunks)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(directory)


def _append_managed_jsonl(
    path: Path,
    record: Mapping[str, object],
    *,
    command_id: str,
) -> None:
    directory = _open_or_create_managed_directory(path.parent)
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    file_descriptor: int | None = None
    try:
        _verify_open_directory_matches_path(path.parent, directory)
        try:
            file_descriptor = os.open(
                path.name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=directory,
            )
        except OSError as exc:
            raise ValueError(
                f"managed JSONL file cannot be opened safely without following symlinks: {path}"
            ) from exc
        opened = os.fstat(file_descriptor)
        if stat.S_ISLNK(opened.st_mode):
            raise ValueError(f"managed JSONL file cannot be a symlink: {path}")
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError(f"managed JSONL path is not a regular file: {path}")
        os.fchmod(file_descriptor, _PRIVATE_FILE_MODE)
        _verify_open_file_matches_entry(
            directory,
            path.name,
            opened,
            label=f"managed JSONL file: {path}",
        )
        payload = _read_descriptor_bytes(
            file_descriptor,
            byte_limit=_MAX_MANAGED_LOG_BYTES,
            label="managed JSONL file",
        )
        after_read = os.fstat(file_descriptor)
        if _file_identity_tuple(after_read) != _file_identity_tuple(opened):
            raise ValueError(f"managed JSONL file changed while reading: {path}")
        _verify_open_directory_matches_path(path.parent, directory)
        if _jsonl_contains_command_id(payload, command_id):
            _verify_open_directory_matches_path(path.parent, directory)
            return
        line = (
            json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")
        os.lseek(file_descriptor, 0, os.SEEK_END)
        written = os.write(file_descriptor, line)
        if written != len(line):
            raise OSError("short write while appending managed JSONL record")
        os.fsync(file_descriptor)
        _verify_open_directory_matches_path(path.parent, directory)
        _verify_open_file_matches_entry(
            directory,
            path.name,
            os.fstat(file_descriptor),
            label=f"managed JSONL file: {path}",
        )
        _verify_open_directory_matches_path(path.parent, directory)
        _fsync_descriptor(directory)
        _verify_open_directory_matches_path(path.parent, directory)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(directory)


def _read_descriptor_bytes(
    descriptor: int,
    *,
    byte_limit: int,
    label: str,
) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    total = 0
    while chunk := os.read(descriptor, 1024 * 1024):
        total += len(chunk)
        if total > byte_limit:
            raise ValueError(f"{label} exceeds its byte budget")
        chunks.append(chunk)
    return b"".join(chunks)


def _jsonl_contains_command_id(payload: bytes, command_id: str) -> bool:
    for line in payload.decode("utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise ValueError("managed JSONL record is not a JSON object")
        if raw.get("command_id") == command_id:
            return True
    return False


def _managed_jsonl_contains_command_id(
    path: Path,
    command_id: str,
) -> bool:
    try:
        directory = _open_existing_managed_directory(path.parent)
    except FileNotFoundError:
        return False
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    file_descriptor: int | None = None
    try:
        _verify_open_directory_matches_path(path.parent, directory)
        try:
            file_descriptor = os.open(path.name, flags, dir_fd=directory)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ValueError(
                f"managed JSONL file cannot be opened safely without following symlinks: {path}"
            ) from exc
        observed = os.fstat(file_descriptor)
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"managed JSONL file cannot be a symlink: {path}")
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError(f"managed JSONL path is not a regular file: {path}")
        _verify_open_file_matches_entry(
            directory,
            path.name,
            observed,
            label=f"managed JSONL file: {path}",
        )
        payload = _read_descriptor_bytes(
            file_descriptor,
            byte_limit=_MAX_MANAGED_LOG_BYTES,
            label="managed JSONL file",
        )
        after_read = os.fstat(file_descriptor)
        if _file_identity_tuple(after_read) != _file_identity_tuple(observed):
            raise ValueError(f"managed JSONL file changed while reading: {path}")
        _verify_open_directory_matches_path(path.parent, directory)
        return _jsonl_contains_command_id(payload, command_id)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(directory)


def _is_external_lab_effect(command: PageControlCommandValue) -> bool:
    return isinstance(
        command,
        (SubmitLabCommand, ExportLabArtifactZip, DiscardLabArtifactZip),
    )


def _ambiguous_lab_effect_result(command: PageControlCommandValue) -> dict[str, object]:
    return {
        "outcome": "ambiguous_completed_at_most_once",
        "command_id": command.command_id,
        "command_kind": command.kind,
        "command_hash": _command_hash(command),
        "reason": (
            "external Lab effect was durably started, but no result was recorded before "
            "the owner was reclaimed"
        ),
    }


def _ambiguous_local_effect_result(
    command: PageControlCommandValue,
    *,
    reason: str,
) -> dict[str, object]:
    return {
        "outcome": "ambiguous_completed_at_most_once",
        "command_id": command.command_id,
        "command_kind": command.kind,
        "command_hash": _command_hash(command),
        "reason": reason,
    }


def _ambiguous_external_effect_result_from_row(row: sqlite3.Row) -> dict[str, object]:
    try:
        command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
    except ValueError:
        return {
            "outcome": "ambiguous_completed_at_most_once",
            "command_id": row["command_id"],
            "command_kind": row["command_kind"],
            "command_hash": row["command_hash"],
            "reason": (
                "legacy external Lab command was processing without a PageControl effect journal"
            ),
        }
    return {
        **_ambiguous_lab_effect_result(command),
        "reason": (
            "legacy external Lab command was processing without a PageControl effect journal"
        ),
    }


class _PageControlExecutionMutex:
    def __init__(self, path: Path) -> None:
        self.path = Path(os.path.abspath(path))
        self.directory_descriptor: int | None = None
        self.file_descriptor: int | None = None
        self.acquired = False

    def __enter__(self) -> bool:
        self.directory_descriptor = _open_or_create_managed_directory(self.path.parent)
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            self.file_descriptor = os.open(
                self.path.name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=self.directory_descriptor,
            )
        except OSError as exc:
            self._close()
            raise ValueError(f"consumer mutex cannot be opened safely: {self.path}") from exc
        opened = os.fstat(self.file_descriptor)
        if not stat.S_ISREG(opened.st_mode):
            self._close()
            raise ValueError(f"consumer mutex path is not a regular file: {self.path}")
        try:
            _verify_open_file_matches_entry(
                self.directory_descriptor,
                self.path.name,
                opened,
                label=f"consumer mutex: {self.path}",
            )
        except Exception:
            self._close()
            raise
        if self.path in _HELD_CONSUMER_MUTEXES:
            self._close()
            return False
        try:
            fcntl.flock(self.file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._close()
            return False
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                self._close()
                return False
            self._close()
            raise
        try:
            _verify_open_file_matches_entry(
                self.directory_descriptor,
                self.path.name,
                opened,
                label=f"consumer mutex: {self.path}",
            )
        except ValueError:
            self._unlock_and_close()
            return False
        os.fchmod(self.file_descriptor, _PRIVATE_FILE_MODE)
        secured = os.fstat(self.file_descriptor)
        try:
            _verify_open_file_matches_entry(
                self.directory_descriptor,
                self.path.name,
                secured,
                label=f"consumer mutex: {self.path}",
                expected_mode=_PRIVATE_FILE_MODE,
            )
        except ValueError:
            self._unlock_and_close()
            return False
        _HELD_CONSUMER_MUTEXES.add(self.path)
        self.acquired = True
        return True

    def __exit__(self, *_error: object) -> None:
        if self.acquired and self.file_descriptor is not None:
            with suppress(OSError):
                fcntl.flock(self.file_descriptor, fcntl.LOCK_UN)
            _HELD_CONSUMER_MUTEXES.discard(self.path)
            self.acquired = False
        self._close()

    def _unlock_and_close(self) -> None:
        if self.file_descriptor is not None:
            with suppress(OSError):
                fcntl.flock(self.file_descriptor, fcntl.LOCK_UN)
        self._close()

    def _close(self) -> None:
        if self.file_descriptor is not None:
            os.close(self.file_descriptor)
            self.file_descriptor = None
        if self.directory_descriptor is not None:
            os.close(self.directory_descriptor)
            self.directory_descriptor = None


def _verify_open_file_matches_entry(
    directory_descriptor: int,
    file_name: str,
    opened: os.stat_result,
    *,
    label: str,
    expected_mode: int | None = None,
) -> None:
    try:
        entry = os.stat(
            file_name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"{label} changed while open") from exc
    if stat.S_ISLNK(entry.st_mode):
        raise ValueError(f"{label} cannot be a symlink")
    if not stat.S_ISREG(entry.st_mode):
        raise ValueError(f"{label} is not a regular file")
    if _file_node_tuple(entry) != _file_node_tuple(opened):
        raise ValueError(f"{label} changed while open")
    if expected_mode is not None and stat.S_IMODE(entry.st_mode) != expected_mode:
        raise ValueError(f"{label} has unsafe permissions")


def _verify_open_directory_matches_path(path: Path, descriptor: int) -> None:
    try:
        entry = os.stat(path, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError(f"managed directory changed while opening: {path}") from exc
    if stat.S_ISLNK(entry.st_mode):
        raise ValueError(f"managed directory cannot be a symlink: {path}")
    if not stat.S_ISDIR(entry.st_mode):
        raise ValueError(f"managed path is not a directory: {path}")
    opened = os.fstat(descriptor)
    if _file_node_tuple(entry) != _file_node_tuple(opened):
        raise ValueError(f"managed directory changed while opening: {path}")


def _file_node_tuple(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _file_identity_tuple(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _fsync_descriptor(descriptor: int) -> None:
    os.fsync(descriptor)


def parse_page_control_command(payload: object) -> PageControlCommandValue:
    return _COMMAND_ADAPTER.validate_python(payload)


__all__ = [
    "AppendNlQueryLog",
    "DEFAULT_PAGE_CONTROL_SERVICE_ID",
    "DeleteCanvas",
    "DeleteUserPool",
    "DiscardLabArtifactZip",
    "ExportLabArtifactZip",
    "ForkBuiltinPool",
    "InitializeLabExports",
    "LabArtifactZipResult",
    "LabPageControlBackend",
    "PageControlCommandValue",
    "PageControlClient",
    "PageControlConsumer",
    "PageControlOutbox",
    "PageControlReceipt",
    "PageControlService",
    "PageControlStatus",
    "PageControlUnavailableError",
    "parse_page_control_command",
    "SaveCanvas",
    "SaveNlPreset",
    "SaveUserPool",
    "SetCanvasPoolRefs",
    "SubmitLabCommand",
]
