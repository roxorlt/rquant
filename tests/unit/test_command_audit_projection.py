from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.collaboration_commands import (
    PreparedUserRoleChange,
    SetUserRoleCommand,
    SetUserRoleRequest,
    confirm_set_user_role,
    parse_set_user_role_command,
    prepare_set_user_role,
    require_original_role_command,
    role_command_sha256,
)
from rquant.collaboration_roles import RoleEntry, RoleName, RoleState
from rquant.command_audit_projection import (
    MAX_AUDIT_READ_BYTES,
    MAX_AUDIT_ROW_BYTES,
    MAX_AUDIT_SCAN_ROWS,
    CommandAuditPage,
    CommandAuditQuery,
    read_command_audit,
)
from rquant.page_control import (
    PageControlEffectStatus,
    PageControlOutbox,
    PageControlStatus,
    SubmitBackfillPlan,
)
from rquant.runtime_contracts import canonical_sha256

NOW = datetime(2026, 10, 6, 4, 0, tzinfo=UTC)
COMMAND_ID = "38ecc733-29b4-4400-9d5a-901b84af078c"
GENERATION = "a" * 64


def roles(*, alice_role: RoleName = "admin", revision: int = 1) -> RoleState:
    return RoleState.create(
        revision=revision,
        users=(
            RoleEntry(username="alice", role=alice_role),
            RoleEntry(username="bob", role="admin"),
            RoleEntry(username="reader", role="viewer"),
            RoleEntry(username="research", role="researcher"),
        ),
    )


def role_request(state: RoleState | None = None) -> SetUserRoleRequest:
    state = state or roles()
    return SetUserRoleRequest(
        schema_version=1,
        kind="set_user_role",
        command_id=COMMAND_ID,
        requested_at=NOW,
        actor_id="alice",
        target_id="reader",
        new_role="researcher",
        expected_revision=state.revision,
        expected_state_sha256=state.content_sha256,
    )


def role_command(
    request: SetUserRoleRequest | None = None,
) -> tuple[SetUserRoleCommand, PreparedUserRoleChange]:
    request = request or role_request()
    prepared = prepare_set_user_role(roles(), request, now=NOW)
    command = SetUserRoleCommand(
        **request.model_dump(mode="python"),
        preparation=prepared,
        entered_target="reader",
    )
    return command, prepared


def test_role_command_round_trip_binds_original_complete_body() -> None:
    command, prepared = role_command()
    parsed = parse_set_user_role_command(command.model_dump_json().encode())
    assert parsed == command
    require_original_role_command(
        parsed,
        command_id=COMMAND_ID,
        command_hash=role_command_sha256(command),
        payload_json=command.model_dump_json().encode(),
    )
    result = confirm_set_user_role(roles(), parsed, original_preparation=prepared, now=NOW)
    assert result.revision == 2
    assert next(entry.role for entry in result.users if entry.username == "reader") == "researcher"
    assert prepared.request_sha256 == canonical_sha256(prepared.request.model_dump(mode="json"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("requested_at", NOW + timedelta(seconds=1)),
        ("command_id", "6b0a9018-3d72-4215-8f70-f95db593b033"),
        ("actor_id", "bob"),
        ("target_id", "research"),
        ("new_role", "viewer"),
    ],
)
def test_confirmation_rejects_changed_original_intent(field: str, value: object) -> None:
    command, prepared = role_command()
    changed = command.model_copy(update={field: value})
    with pytest.raises((ValueError, PermissionError)):
        confirm_set_user_role(roles(), changed, original_preparation=prepared, now=NOW)


@pytest.mark.parametrize("state", [roles(revision=2), roles(alice_role="viewer")])
def test_confirmation_rechecks_current_role_and_cas(state: RoleState) -> None:
    command, prepared = role_command()
    with pytest.raises(PermissionError):
        confirm_set_user_role(state, command, original_preparation=prepared, now=NOW)


@pytest.mark.parametrize("delta", [timedelta(seconds=-1), timedelta(minutes=2)])
def test_confirmation_time_is_checked_at_effect(delta: timedelta) -> None:
    command, prepared = role_command()
    with pytest.raises(PermissionError):
        confirm_set_user_role(roles(), command, original_preparation=prepared, now=NOW + delta)


def test_original_server_preparation_is_required_and_not_recreated() -> None:
    command, prepared = role_command()
    replacement = prepare_set_user_role(roles(), prepared.request, now=NOW + timedelta(seconds=1))
    with pytest.raises(PermissionError):
        confirm_set_user_role(
            roles(), command, original_preparation=replacement, now=NOW + timedelta(seconds=2)
        )
    with pytest.raises(PermissionError):
        confirm_set_user_role(roles(), command, original_preparation=None, now=NOW)


@pytest.mark.parametrize("change", ["uuid", "hash", "body"])
def test_original_uuid_lookup_requires_full_body_and_hash(change: str) -> None:
    command, _prepared = role_command()
    values = {
        "command_id": COMMAND_ID,
        "command_hash": role_command_sha256(command),
        "payload_json": command.model_dump_json().encode(),
    }
    if change == "uuid":
        values["command_id"] = "6b0a9018-3d72-4215-8f70-f95db593b033"
    elif change == "hash":
        values["command_hash"] = "b" * 64
    else:
        data = command.model_dump(mode="json")
        data["entered_target"] = "research"
        values["payload_json"] = json.dumps(data).encode()
        values["command_hash"] = canonical_sha256(data)
    with pytest.raises((ValueError, PermissionError)):
        require_original_role_command(command, **values)


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), None),
        (("schema_version",), True),
        (("schema_version",), 2),
        (("kind",), "save_canvas"),
        (("command_id",), "historic-local-id"),
        (("command_id",), COMMAND_ID.upper()),
        (("requested_at",), "2026-10-06T04:00:00"),
        (("requested_at",), "2026-10-06T12:00:00+08:00"),
        (("preparation", "schema_version"), None),
        (("preparation", "confirmation", "schema_version"), None),
    ],
)
def test_new_command_parser_has_explicit_version_without_legacy_fallback(
    path: tuple[str, ...],
    value: object,
) -> None:
    command, _prepared = role_command()
    data = command.model_dump(mode="json")
    target = data
    for name in path[:-1]:
        target = target[name]
    if value is None:
        target.pop(path[-1])
    else:
        target[path[-1]] = value
    with pytest.raises(ValueError):
        parse_set_user_role_command(json.dumps(data).encode())


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":1,"schema_version":1}',
        b'{"schema_version":1,"x":NaN}',
        b"{}",
        b"{",
        b" " * (32 * 1024 + 1),
    ],
    ids=["duplicate-keys", "nonfinite", "missing-fields", "invalid-json", "oversized"],
)
def test_command_parser_rejects_duplicate_nonfinite_missing_or_oversized_bytes(
    payload: bytes,
) -> None:
    with pytest.raises(ValueError):
        parse_set_user_role_command(payload)


@pytest.fixture
def outbox(tmp_path: Path) -> PageControlOutbox:
    return PageControlOutbox(tmp_path / "actual-page-control.sqlite3")


def enqueue(
    outbox: PageControlOutbox, number: int, *, at: datetime | None = None
) -> SubmitBackfillPlan:
    command = SubmitBackfillPlan(
        command_id=f"legacy-{number:04d}",
        requested_at=at or NOW + timedelta(seconds=number),
        actor_id="reader",
        audit_start=date(2026, 10, 1),
        completed_through=date(2026, 10, 2),
    )
    outbox.enqueue(command)
    return command


@contextmanager
def snapshot(outbox: PageControlOutbox) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(outbox.path.as_uri() + "?mode=ro", uri=True)
    connection.execute("BEGIN")
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def read(
    outbox: PageControlOutbox,
    *,
    viewer: str = "alice",
    query: CommandAuditQuery | None = None,
    state: RoleState | None = None,
    actors: dict[tuple[str, str], str] | None = None,
    generation: str = GENERATION,
) -> CommandAuditPage:
    with snapshot(outbox) as connection:
        return read_command_audit(
            connection,
            state=state or roles(),
            viewer_id=viewer,
            source_generation=generation,
            query=query or CommandAuditQuery(),
            actor_resolver=None
            if actors is None
            else lambda command_id, command_hash: actors.get((command_id, command_hash)),
        )


def attest(command: SubmitBackfillPlan, actor: str) -> dict[tuple[str, str], str]:
    # Synthetic resolver evidence exercises the boundary; it is not real ingress proof.
    return {(command.command_id, canonical_sha256(command.model_dump(mode="json"))): actor}


def test_actual_original_journal_unknown_actor_is_admin_only(outbox: PageControlOutbox) -> None:
    command = enqueue(outbox, 1)
    outbox.claim_records(limit=1, owner_id="reader", now=NOW + timedelta(minutes=1))
    admin = read(outbox)
    assert admin.items[0].command_id == command.command_id
    assert admin.items[0].actor_id is None
    assert admin.items[0].actor_label == "未记录操作人"
    assert read(outbox, viewer="reader").items == ()
    assert "processing_owner" not in admin.model_dump_json()
    assert "audit_start" not in admin.model_dump_json()


@pytest.mark.parametrize("viewer", ["reader", "research"])
def test_proven_actor_is_visible_only_to_current_user_or_admin(
    outbox: PageControlOutbox, viewer: str
) -> None:
    own, other = enqueue(outbox, 1), enqueue(outbox, 2)
    actors = attest(own, viewer) | attest(other, "bob")
    page = read(outbox, viewer=viewer, actors=actors)
    assert [item.command_id for item in page.items] == [own.command_id]
    filtered = read(outbox, actors=actors, query=CommandAuditQuery(actor_id="bob"))
    assert [item.command_id for item in filtered.items] == [other.command_id]
    with pytest.raises(PermissionError):
        read(outbox, viewer=viewer, actors=actors, query=CommandAuditQuery(actor_id="bob"))


def test_resolver_is_bound_to_actual_original_command_hash(outbox: PageControlOutbox) -> None:
    command = enqueue(outbox, 1)
    wrong = {(command.command_id, "0" * 64): "reader"}
    assert read(outbox, viewer="reader", actors=wrong).items == ()


@pytest.mark.parametrize("status", ["pending", "processing", "succeeded", "failed", "ambiguous"])
def test_original_command_states_remain_exact_without_task_completion_claim(
    outbox: PageControlOutbox, status: str
) -> None:
    command = enqueue(outbox, 1)
    if status != "pending":
        claim = outbox.claim_records(limit=1, owner_id="worker", now=NOW + timedelta(minutes=1))[0]
        if status != "processing":
            outbox.begin_effect(claim.command, owner_id="worker", claim_token=claim.claim_token)
            effect_status = PageControlEffectStatus(status)
            result = {"status": "queued"} if status == "succeeded" else None
            outbox.finish_effect(
                command.command_id,
                status=effect_status,
                result=result,
                owner_id="worker",
                claim_token=claim.claim_token,
            )
            outbox.complete(
                command.command_id,
                status=PageControlStatus(status),
                result=result,
                owner_id="worker",
                claim_token=claim.claim_token,
            )
    item = read(outbox).items[0]
    assert item.command_status == status
    if status == "succeeded":
        assert item.outcome == "accepted"
        assert item.summary == "已接受"
    assert "已完成" not in item.summary


def test_terminal_effect_before_receipt_recovery_is_not_hidden(outbox: PageControlOutbox) -> None:
    command = enqueue(outbox, 1)
    claim = outbox.claim_records(limit=1, owner_id="worker", now=NOW + timedelta(minutes=1))[0]
    outbox.begin_effect(claim.command, owner_id="worker", claim_token=claim.claim_token)
    outbox.finish_effect(
        command.command_id,
        status=PageControlEffectStatus.SUCCEEDED,
        result={"status": "submitted"},
        owner_id="worker",
        claim_token=claim.claim_token,
    )
    item = read(outbox).items[0]
    assert item.command_status == "processing"
    assert item.effect_status == "succeeded"
    assert item.outcome == "accepted"


def test_person_time_action_filters_use_parameterized_original_rows(
    outbox: PageControlOutbox,
) -> None:
    commands = [enqueue(outbox, number) for number in (1, 2, 3)]
    actors = (
        attest(commands[0], "reader") | attest(commands[1], "reader") | attest(commands[2], "bob")
    )
    page = read(
        outbox,
        actors=actors,
        query=CommandAuditQuery(
            actor_id="reader",
            command_kind="submit_backfill_plan",
            time_from=NOW + timedelta(seconds=2),
            time_until=NOW + timedelta(seconds=3),
        ),
    )
    assert [item.command_id for item in page.items] == [commands[1].command_id]


def test_descending_cursor_keeps_equal_time_rows_without_duplicates(
    outbox: PageControlOutbox,
) -> None:
    for number in range(1, 6):
        enqueue(outbox, number, at=NOW)
    seen: list[str] = []
    cursor = None
    while True:
        page = read(outbox, query=CommandAuditQuery(limit=2, cursor=cursor))
        seen.extend(item.command_id for item in page.items)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    assert seen == [f"legacy-{number:04d}" for number in range(5, 0, -1)]


@pytest.mark.parametrize("change", ["source", "viewer", "role", "filter", "limit"])
def test_cursor_is_bound_to_current_role_scope_and_original_source(
    outbox: PageControlOutbox, change: str
) -> None:
    for number in range(1, 4):
        enqueue(outbox, number)
    cursor = read(outbox, query=CommandAuditQuery(limit=1)).next_cursor
    kwargs = {"query": CommandAuditQuery(limit=1, cursor=cursor)}
    if change == "source":
        kwargs["generation"] = "b" * 64
    elif change == "viewer":
        kwargs["viewer"] = "bob"
    elif change == "role":
        kwargs["state"] = roles(alice_role="viewer")
    elif change == "filter":
        kwargs["query"] = CommandAuditQuery(
            limit=1, cursor=cursor, command_kind="submit_backfill_plan"
        )
    else:
        kwargs["query"] = CommandAuditQuery(limit=2, cursor=cursor)
    with pytest.raises((ValueError, PermissionError)):
        read(outbox, **kwargs)


def test_private_filtered_scan_has_bounded_continuation(outbox: PageControlOutbox) -> None:
    for number in range(1, MAX_AUDIT_SCAN_ROWS + 3):
        enqueue(outbox, number)
    page = read(outbox, viewer="reader", query=CommandAuditQuery(limit=1))
    assert page.items == ()
    assert page.scanned_count == MAX_AUDIT_SCAN_ROWS
    assert page.next_cursor is not None
    following = read(
        outbox, viewer="reader", query=CommandAuditQuery(limit=1, cursor=page.next_cursor)
    )
    assert following.items == ()
    assert following.next_cursor is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("command_hash", "f" * 64),
        ("command_kind", "save_canvas"),
        ("payload_json", '{"kind":"submit_backfill_plan","kind":"submit_backfill_plan"}'),
        ("status", "completed"),
        ("enqueued_at", "2026-10-06T04:00:00"),
        ("result_json", '{"x":NaN}'),
        ("payload_json", " " * (MAX_AUDIT_ROW_BYTES + 1)),
    ],
    ids=[
        "wrong-command-hash",
        "wrong-command-kind",
        "duplicate-payload-keys",
        "wrong-status",
        "missing-timezone",
        "nonfinite-result",
        "oversized-payload",
    ],
)
def test_corrupt_or_over_capacity_original_row_is_refused(
    outbox: PageControlOutbox, field: str, value: str
) -> None:
    command = enqueue(outbox, 1)
    with outbox._connect() as connection:
        connection.execute(
            f"UPDATE page_control_command SET {field} = ? WHERE command_id = ?",
            (value, command.command_id),
        )
    with pytest.raises(ValueError):
        read(outbox)


@pytest.mark.parametrize(
    "field,value", [("command_hash", "0" * 64), ("effect_kind", "save_canvas")]
)
def test_effect_must_bind_original_hash_and_kind(
    outbox: PageControlOutbox, field: str, value: str
) -> None:
    command = enqueue(outbox, 1)
    claim = outbox.claim_records(limit=1, owner_id="worker", now=NOW + timedelta(minutes=1))[0]
    outbox.begin_effect(claim.command, owner_id="worker", claim_token=claim.claim_token)
    with outbox._connect() as connection:
        connection.execute(
            f"UPDATE page_control_effect SET {field} = ? WHERE command_id = ?",
            (value, command.command_id),
        )
    with pytest.raises(ValueError):
        read(outbox)


def test_projection_does_not_write_original_journal(outbox: PageControlOutbox) -> None:
    enqueue(outbox, 1)
    before = outbox.path.read_bytes()
    with snapshot(outbox) as connection:
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        read_command_audit(
            connection,
            state=roles(),
            viewer_id="alice",
            source_generation=GENERATION,
            query=CommandAuditQuery(),
        )
        assert connection.total_changes == 0
        assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)
    assert outbox.path.read_bytes() == before


@pytest.mark.parametrize(
    "query",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"command_kind": "x' OR 1=1 --"},
        {"actor_id": "reader'"},
        {"time_from": NOW + timedelta(seconds=1), "time_until": NOW},
        {"time_from": datetime(2026, 10, 6)},
        {"cursor": "x" * 4097},
    ],
)
def test_filter_and_cursor_bounds(query: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        CommandAuditQuery(**query)


def test_current_role_source_and_read_snapshot_are_required(outbox: PageControlOutbox) -> None:
    enqueue(outbox, 1)
    with snapshot(outbox) as connection:
        for state, viewer in ((None, "alice"), (roles(), "unknown")):
            with pytest.raises(PermissionError):
                read_command_audit(
                    connection,
                    state=state,
                    viewer_id=viewer,
                    source_generation=GENERATION,
                    query=CommandAuditQuery(),
                )
    with (
        sqlite3.connect(outbox.path.as_uri() + "?mode=ro", uri=True) as connection,
        pytest.raises(ValueError),
    ):
        read_command_audit(
            connection,
            state=roles(),
            viewer_id="alice",
            source_generation=GENERATION,
            query=CommandAuditQuery(),
        )


class CountedCursor(sqlite3.Cursor):
    def _count(self, row: tuple[object, ...] | None) -> None:
        if row is not None:
            self.connection.fetched_bytes += sum(
                len(value.encode()) for value in row if isinstance(value, str) and len(value) > 1024
            )

    def __next__(self) -> tuple[object, ...]:
        row = super().__next__()
        self._count(row)
        return row

    def fetchone(self) -> tuple[object, ...] | None:
        row = super().fetchone()
        self._count(row)
        return row


class CountedConnection(sqlite3.Connection):
    fetched_bytes: int = 0

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> CountedCursor:
        return self.cursor(factory=CountedCursor).execute(sql, parameters)


def test_read_capacity_includes_actual_payload_transfer_and_lookahead(
    outbox: PageControlOutbox,
) -> None:
    for number in range(1, 4):
        enqueue(outbox, number)
    result = {"status": "queued", "original_display": "x" * (900 * 1024)}
    for claim in outbox.claim_records(limit=3, owner_id="worker", now=NOW + timedelta(minutes=1)):
        outbox.begin_effect(claim.command, owner_id="worker", claim_token=claim.claim_token)
        outbox.finish_effect(
            claim.command.command_id,
            status=PageControlEffectStatus.SUCCEEDED,
            result=result,
            owner_id="worker",
            claim_token=claim.claim_token,
        )
        outbox.complete(
            claim.command.command_id,
            result=result,
            owner_id="worker",
            claim_token=claim.claim_token,
        )
    connection = sqlite3.connect(
        outbox.path.as_uri() + "?mode=ro", uri=True, factory=CountedConnection
    )
    try:
        connection.execute("BEGIN")
        page = read_command_audit(
            connection,
            state=roles(),
            viewer_id="alice",
            source_generation=GENERATION,
            query=CommandAuditQuery(limit=2),
        )
        assert len(page.items) == 2
        assert page.next_cursor is not None
        assert connection.fetched_bytes <= MAX_AUDIT_READ_BYTES
        assert "original_display" not in page.model_dump_json()
        connection.fetched_bytes = 0
        with pytest.raises(ValueError, match="read capacity"):
            read_command_audit(
                connection,
                state=roles(),
                viewer_id="alice",
                source_generation=GENERATION,
                query=CommandAuditQuery(limit=3),
            )
        assert connection.fetched_bytes <= MAX_AUDIT_READ_BYTES
        connection.fetched_bytes = 0
        following = read_command_audit(
            connection,
            state=roles(),
            viewer_id="alice",
            source_generation=GENERATION,
            query=CommandAuditQuery(limit=2, cursor=page.next_cursor),
        )
        assert len(following.items) == 1
        assert following.next_cursor is None
        assert connection.fetched_bytes <= MAX_AUDIT_READ_BYTES
    finally:
        connection.rollback()
        connection.close()


def test_private_cursor_does_not_publish_hidden_user_uuid_time_or_actor(
    outbox: PageControlOutbox,
) -> None:
    import base64

    for number in range(1, MAX_AUDIT_SCAN_ROWS + 2):
        enqueue(outbox, number)
    page = read(outbox, viewer="reader", query=CommandAuditQuery(limit=1))
    data = base64.urlsafe_b64decode(page.next_cursor).decode()
    assert "legacy-" not in data
    assert "2026-10-06" not in data
    assert "actor_id" not in data
    assert "scanned_count" not in page.model_dump_json()


def test_confirmation_exact_typed_target_is_required() -> None:
    command, prepared = role_command()
    changed = command.model_copy(update={"entered_target": "Reader"})
    with pytest.raises(PermissionError):
        confirm_set_user_role(roles(), changed, original_preparation=prepared, now=NOW)
