"""Bounded SELECT projection of the original PageControl command/effect journal.

The caller supplies its already-bound readonly SQLite snapshot, verified current
roles and source generation, and its original authority's actor resolver. A
connection, model, hash or resolver assertion alone does not prove this origin.
The shared owner must still enforce identity, enabled/allowlist/CSRF/resource
conditions and current role fences. This module creates no store or connection.
Unknown actors remain unknown; payload users and worker owners are never inferred.
"""

from __future__ import annotations

import base64
import binascii
import json
import sqlite3
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from hmac import compare_digest
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from rquant.collaboration_roles import RoleName, RoleState, Sha256, UserId, parse_role_state
from rquant.runtime_contracts import canonical_sha256

MAX_AUDIT_SCAN_ROWS = 512
MAX_AUDIT_ROW_BYTES = 1024 * 1024
MAX_AUDIT_READ_BYTES = 4 * 1024 * 1024
MAX_AUDIT_CURSOR_BYTES = 4096
CommandId = Annotated[str, Field(min_length=1, max_length=128)]
CommandKind = Annotated[str, Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_]*$")]
CommandStatus = Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
EffectStatus = Literal["started", "succeeded", "failed", "ambiguous"]
AuditOutcome = Literal["pending", "processing", "accepted", "processed", "failed", "ambiguous"]
ActorResolver = Callable[[str, str], str | None]


class _AuditModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    @field_validator("schema_version", mode="before", check_fields=False)
    @classmethod
    def exact_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("unknown audit version")
        return value


class CommandAuditQuery(_AuditModel):
    limit: int = Field(default=50, ge=1, le=100)
    actor_id: UserId | None = None
    command_kind: CommandKind | None = None
    time_from: AwareDatetime | None = None
    time_until: AwareDatetime | None = None
    cursor: str | None = Field(default=None, min_length=1, max_length=MAX_AUDIT_CURSOR_BYTES)

    @field_validator("time_from", "time_until")
    @classmethod
    def utc_filter(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.utcoffset() != timedelta(0):
            raise ValueError("audit filters must use UTC")
        return value

    @model_validator(mode="after")
    def time_range(self) -> Self:
        if (
            self.time_from is not None
            and self.time_until is not None
            and self.time_from >= self.time_until
        ):
            raise ValueError("audit time range is empty or reversed")
        return self


class CommandAuditItem(_AuditModel):
    schema_version: Literal[1] = 1
    command_id: CommandId
    command_kind: CommandKind
    command_hash: Sha256
    actor_id: UserId | None
    actor_label: str = Field(max_length=64)
    command_status: CommandStatus
    effect_status: EffectStatus | None
    enqueued_at: AwareDatetime
    completed_at: AwareDatetime | None
    effect_started_at: AwareDatetime | None
    effect_completed_at: AwareDatetime | None
    outcome: AuditOutcome
    summary: str = Field(max_length=32)


class CommandAuditPage(_AuditModel):
    schema_version: Literal[1] = 1
    source_generation: Sha256
    role_revision: int = Field(ge=1, le=2**63 - 1)
    items: tuple[CommandAuditItem, ...] = Field(max_length=100)
    next_cursor: str | None = Field(max_length=MAX_AUDIT_CURSOR_BYTES)
    # Internal measurement is absent after the original private JSON round trip.
    scanned_count: int | None = Field(default=None, ge=0, le=MAX_AUDIT_SCAN_ROWS, exclude=True)


class CommandAuditSourceWindow(_AuditModel):
    items: tuple[CommandAuditItem, ...] = Field(max_length=MAX_AUDIT_SCAN_ROWS)
    has_more: bool


def read_command_audit_source(
    connection: sqlite3.Connection, *, actor_resolver: ActorResolver,
) -> CommandAuditSourceWindow:
    """Carry the owner's bounded global view, without inventing a user for the worker.

    This is a projection helper, not a permission proof or a user-facing read.
    Its caller must pin the original private journal and current roles. The Web
    read still applies current actor filtering through read_command_audit.
    """
    if not connection.in_transaction:
        raise ValueError("original audit read snapshot is required")
    rows = connection.execute("""
        SELECT c.rowid AS original_rowid,c.command_id,c.command_kind,c.command_hash,
               c.status AS command_status,c.enqueued_at,c.completed_at AS command_completed_at,
               length(CAST(c.payload_json AS BLOB)) AS payload_bytes,
               length(CAST(c.result_json AS BLOB)) AS command_result_bytes,
               e.command_hash AS effect_command_hash,e.effect_kind,e.status AS effect_status,
               e.started_at AS effect_started_at,e.completed_at AS effect_completed_at,
               length(CAST(e.result_json AS BLOB)) AS effect_result_bytes
        FROM page_control_command c LEFT JOIN page_control_effect e ON e.command_id=c.command_id
        ORDER BY c.enqueued_at DESC,c.command_id DESC LIMIT ?
    """, (MAX_AUDIT_SCAN_ROWS + 1,))
    names = tuple(item[0] for item in rows.description)
    items: list[CommandAuditItem] = []
    read_bytes = 0
    has_more = False
    for raw in rows:
        if len(items) == MAX_AUDIT_SCAN_ROWS:
            has_more = True
            break
        row = dict(zip(names, raw, strict=True))
        sizes = tuple(int(row[name] or 0) for name in
                      ("payload_bytes", "command_result_bytes", "effect_result_bytes"))
        read_bytes += sum(sizes)
        if any(size > MAX_AUDIT_ROW_BYTES for size in sizes) or read_bytes > MAX_AUDIT_READ_BYTES:
            raise ValueError("original audit source exceeds read capacity")
        body = connection.execute("""
            SELECT c.payload_json,c.result_json,e.result_json FROM page_control_command c
            LEFT JOIN page_control_effect e ON e.command_id=c.command_id WHERE c.rowid=?
        """, (row["original_rowid"],)).fetchone()
        if body is None:
            raise ValueError("original audit body disappeared from its snapshot")
        row.update(zip(("payload_json", "command_result_json", "effect_result_json"), body, strict=True))
        _payload, command_result, effect_result = _original(row)
        actor = actor_resolver(str(row["command_id"]), str(row["command_hash"]))
        if actor is not None:
            actor = TypeAdapter(UserId).validate_python(actor, strict=True)
        outcome, summary = _outcome(row["command_status"], row["effect_status"],
                                    (command_result, effect_result))
        items.append(CommandAuditItem(
            command_id=row["command_id"], command_kind=row["command_kind"], command_hash=row["command_hash"],
            actor_id=actor, actor_label=actor or "未记录操作人", command_status=row["command_status"],
            effect_status=row["effect_status"], enqueued_at=_timestamp(row["enqueued_at"]),
            completed_at=_optional_timestamp(row["command_completed_at"]),
            effect_started_at=_optional_timestamp(row["effect_started_at"]),
            effect_completed_at=_optional_timestamp(row["effect_completed_at"]), outcome=outcome, summary=summary))
    return CommandAuditSourceWindow(items=tuple(items), has_more=has_more)


class _AuditCursor(_AuditModel):
    schema_version: Literal[1]
    scope_sha256: Sha256
    # Do not place a hidden user's UUID, time, actor or payload in a public cursor.
    after_rowid: int = Field(ge=1, le=2**63 - 1)
    content_sha256: Sha256

    @model_validator(mode="after")
    def digest(self) -> Self:
        if not compare_digest(
            self.content_sha256,
            canonical_sha256(self.model_dump(mode="json", exclude={"content_sha256"})),
        ):
            raise ValueError("audit cursor digest differs")
        return self


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate audit JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ValueError("non-finite audit JSON")


def _json(data: bytes) -> object:
    return json.loads(data, object_pairs_hook=_unique_object, parse_constant=_reject_constant)


def _cursor(value: str, scope: str) -> _AuditCursor:
    try:
        raw = base64.b64decode(value.encode("ascii"), altchars=b"-_", validate=True)
        if not raw or len(raw) > MAX_AUDIT_CURSOR_BYTES:
            raise ValueError("audit cursor exceeds capacity")
        _json(raw)
        parsed = _AuditCursor.model_validate_json(raw)
        if not compare_digest(parsed.scope_sha256, scope):
            raise ValueError("audit cursor scope changed")
        return parsed
    except (ValueError, TypeError, binascii.Error, RecursionError) as exc:
        raise ValueError("audit cursor is invalid or stale") from exc


def _next_cursor(rowid: int, scope: str) -> str:
    body = {"schema_version": 1, "scope_sha256": scope, "after_rowid": rowid}
    cursor = _AuditCursor(**body, content_sha256=canonical_sha256(body))
    return base64.urlsafe_b64encode(cursor.model_dump_json().encode()).decode("ascii")


def _role(state: RoleState | None, viewer_id: str) -> tuple[RoleState, RoleName]:
    try:
        if type(state) is not RoleState:
            raise ValueError("current role state is absent")
        current = parse_role_state(state.model_dump_json().encode())
        viewer = TypeAdapter(UserId).validate_python(viewer_id, strict=True)
        for entry in current.users:
            if entry.username == viewer:
                return current, entry.role
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("current audit role authority is unavailable") from exc
    raise PermissionError("current audit user has no registered role")


def _timestamp(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError("original audit time is missing")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("original audit time must be aware UTC")
    return result


def _optional_timestamp(value: object) -> datetime | None:
    return None if value is None else _timestamp(value)


def _original(row: Mapping[str, object]) -> tuple[object, object, object]:
    if row["payload_json"] is None:
        raise ValueError("original audit body is absent")
    payload = _json(str(row["payload_json"]).encode())
    if not isinstance(payload, dict) or (
        payload.get("command_id") != row["command_id"]
        or payload.get("kind") != row["command_kind"]
        or canonical_sha256(payload) != row["command_hash"]
    ):
        raise ValueError("original audit command body/hash binding differs")
    if row["effect_status"] is not None and (
        row["effect_command_hash"] != row["command_hash"]
        or row["effect_kind"] != row["command_kind"]
    ):
        raise ValueError("original audit effect binding differs")
    command_result = (
        None
        if row["command_result_json"] is None
        else _json(str(row["command_result_json"]).encode())
    )
    effect_result = (
        None
        if row["effect_result_json"] is None
        else _json(str(row["effect_result_json"]).encode())
    )
    return payload, command_result, effect_result


def _outcome(
    command_status: str, effect_status: object, results: tuple[object, object]
) -> tuple[AuditOutcome, str]:
    if command_status == "failed":
        return "failed", "命令失败"
    if command_status == "ambiguous":
        return "ambiguous", "结果待核对"
    if effect_status == "failed":
        return "failed", "效果失败"
    if effect_status == "ambiguous":
        return "ambiguous", "效果待核对"
    if (command_status == "succeeded" or effect_status == "succeeded") and any(
        isinstance(value, dict) and value.get("status") in ("queued", "submitted", "accepted")
        for value in results
    ):
        return "accepted", "已接受"
    if command_status == "succeeded":
        return "processed", "命令已处理"
    if command_status == "processing":
        return "processing", "处理中"
    return "pending", "待处理"


def read_command_audit(
    connection: sqlite3.Connection,
    *,
    state: RoleState | None,
    viewer_id: str,
    source_generation: str,
    query: CommandAuditQuery,
    actor_resolver: ActorResolver | None = None,
) -> CommandAuditPage:
    """Read one bounded page. Resolver must be the actual original private owner.

    Cursor hashes detect stale/corrupt material, not identity. Current role and
    proven-actor filtering run on every page, even for a forged cursor position.
    The source generation must bind the caller's journal/actor snapshot.
    """
    current, role = _role(state, viewer_id)
    generation = TypeAdapter(Sha256).validate_python(source_generation, strict=True)
    query = CommandAuditQuery.model_validate(query.model_dump(mode="python"))
    if role != "admin" and query.actor_id not in (None, viewer_id):
        raise PermissionError("audit filter names another user")
    if not connection.in_transaction:
        raise ValueError("original audit read snapshot is required")
    scope = canonical_sha256(
        {
            "source_generation": generation,
            "viewer_id": viewer_id,
            "role": role,
            "role_revision": current.revision,
            "role_sha256": current.content_sha256,
            "query": query.model_dump(mode="json", exclude={"cursor"}),
        }
    )
    predicates: list[str] = []
    values: list[object] = []
    if query.command_kind is not None:
        predicates.append("c.command_kind = ?")
        values.append(query.command_kind)
    for name, operator in (("time_from", ">="), ("time_until", "<")):
        bound = getattr(query, name)
        if bound is not None:
            predicates.append(f"c.enqueued_at {operator} ?")
            values.append(bound.isoformat(timespec="microseconds"))
    if query.cursor is not None:
        parsed = _cursor(query.cursor, scope)
        boundary = connection.execute(
            "SELECT enqueued_at, command_id FROM page_control_command WHERE rowid = ?",
            (parsed.after_rowid,),
        ).fetchone()
        if boundary is None:
            raise ValueError("audit cursor original row is unavailable")
        predicates.append("(c.enqueued_at < ? OR (c.enqueued_at = ? AND c.command_id < ?))")
        values.extend((boundary[0], boundary[0], boundary[1]))
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    sql = f"""
        SELECT c.rowid AS original_rowid, c.command_id, c.command_kind,
               c.command_hash, c.status AS command_status, c.enqueued_at,
               c.completed_at AS command_completed_at,
               length(CAST(c.payload_json AS BLOB)) AS payload_bytes,
               length(CAST(c.result_json AS BLOB)) AS command_result_bytes,
               e.command_hash AS effect_command_hash, e.effect_kind,
               e.status AS effect_status, e.started_at AS effect_started_at,
               e.completed_at AS effect_completed_at,
               length(CAST(e.result_json AS BLOB)) AS effect_result_bytes
        FROM page_control_command c LEFT JOIN page_control_effect e ON e.command_id = c.command_id
        {where}
        ORDER BY c.enqueued_at DESC, c.command_id DESC LIMIT ?
    """
    rows = connection.execute(sql, tuple(values) + (MAX_AUDIT_SCAN_ROWS + 1,))
    names = tuple(item[0] for item in rows.description)
    items: list[CommandAuditItem] = []
    scanned = 0
    read_bytes = 0
    after = 0
    has_more = False
    for raw in rows:
        if scanned >= MAX_AUDIT_SCAN_ROWS or len(items) >= query.limit:
            has_more = True
            break
        row = dict(zip(names, raw, strict=True))
        sizes = tuple(
            int(row[name] or 0)
            for name in ("payload_bytes", "command_result_bytes", "effect_result_bytes")
        )
        if any(size > MAX_AUDIT_ROW_BYTES for size in sizes):
            raise ValueError("original audit row exceeds capacity")
        read_bytes += sum(sizes)
        if read_bytes > MAX_AUDIT_READ_BYTES:
            raise ValueError("original audit page exceeds read capacity")
        # Keep the lookahead metadata-only; enforce byte bounds before transfer.
        body = connection.execute(
            """
            SELECT c.payload_json, c.result_json, e.result_json
            FROM page_control_command c
            LEFT JOIN page_control_effect e ON e.command_id = c.command_id
            WHERE c.rowid = ?
            """,
            (row["original_rowid"],),
        ).fetchone()
        if body is None:
            raise ValueError("original audit body disappeared from its snapshot")
        row.update(
            zip(("payload_json", "command_result_json", "effect_result_json"), body, strict=True)
        )
        _payload, command_result, effect_result = _original(row)
        command_id = TypeAdapter(CommandId).validate_python(row["command_id"], strict=True)
        command_hash = TypeAdapter(Sha256).validate_python(row["command_hash"], strict=True)
        command_kind = TypeAdapter(CommandKind).validate_python(row["command_kind"], strict=True)
        status = TypeAdapter(CommandStatus).validate_python(row["command_status"], strict=True)
        effect_status = TypeAdapter(EffectStatus | None).validate_python(
            row["effect_status"], strict=True
        )
        enqueued_at = _timestamp(row["enqueued_at"])
        completed_at = _optional_timestamp(row["command_completed_at"])
        effect_started_at = _optional_timestamp(row["effect_started_at"])
        effect_completed_at = _optional_timestamp(row["effect_completed_at"])
        actor = None
        if actor_resolver is not None:
            try:
                resolved = actor_resolver(command_id, command_hash)
                actor = (
                    None
                    if resolved is None
                    else TypeAdapter(UserId).validate_python(resolved, strict=True)
                )
            except (ValueError, TypeError, PermissionError) as exc:
                raise PermissionError("original audit actor authority is unavailable") from exc
        scanned += 1
        after = int(row["original_rowid"])
        if role != "admin" and actor != viewer_id:
            continue
        if query.actor_id is not None and actor != query.actor_id:
            continue
        outcome, summary = _outcome(status, effect_status, (command_result, effect_result))
        items.append(
            CommandAuditItem(
                command_id=command_id,
                command_kind=command_kind,
                command_hash=command_hash,
                actor_id=actor,
                actor_label=actor or "未记录操作人",
                command_status=status,
                effect_status=effect_status,
                enqueued_at=enqueued_at,
                completed_at=completed_at,
                effect_started_at=effect_started_at,
                effect_completed_at=effect_completed_at,
                outcome=outcome,
                summary=summary,
            )
        )
    return CommandAuditPage(
        source_generation=generation,
        role_revision=current.revision,
        items=tuple(items),
        next_cursor=_next_cursor(after, scope) if has_more else None,
        scanned_count=scanned,
    )
