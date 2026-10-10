"""Pure role rules: trusted file/ingress/fences are a later shared contract."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

if TYPE_CHECKING:
    from rquant.collaboration_roles import (
        OriginalAccessConditions,
        PermissionPolicy,
        RoleChangeRequest,
        RoleState,
    )

NOW = datetime(2026, 10, 6, 4, 0, tzinfo=UTC)
REQUEST = str(UUID(int=1))


def core() -> ModuleType:
    try:
        return import_module("rquant.collaboration_roles")
    except ModuleNotFoundError:
        pytest.fail(
            "required collaboration role core is not implemented", pytrace=False
        )


def state(c: ModuleType) -> RoleState:
    return c.RoleState.create(
        revision=3,
        users=(
            c.RoleEntry(username="admin", role="admin"),
            c.RoleEntry(username="alice", role="researcher"),
            c.RoleEntry(username="bob", role="viewer"),
        ),
    )


def policy(c: ModuleType) -> PermissionPolicy:
    return c.PermissionPolicy(
        entries=(
            c.PermissionRule(
                method="POST",
                path="/api/v1/data/executions/commands",
                command_kind="execute_backfill_plan",
                roles=("admin",),
            ),
            c.PermissionRule(
                method="POST",
                path="/api/v1/tasks/jobs/commands",
                command_kind="submit_lab_command",
                roles=("admin", "researcher"),
            ),
            c.PermissionRule(
                method="GET",
                path="/api/v1/factors/results",
                roles=("admin", "researcher", "viewer"),
            ),
        )
    )


def gates(c: ModuleType, **changes: object) -> OriginalAccessConditions:
    return c.OriginalAccessConditions(
        **(
            dict(
                identity_verified=True,
                installed=True,
                enabled=True,
                allowlisted=True,
                owner_allowed=True,
                csrf_valid=True,
                resource_allowed=True,
            )
            | changes
        )
    )


def request(c: ModuleType, s: RoleState, **changes: object) -> RoleChangeRequest:
    return c.RoleChangeRequest(
        **(
            dict(
                command_id=REQUEST,
                actor_id="admin",
                target_id="alice",
                new_role="viewer",
                expected_revision=s.revision,
                expected_state_sha256=s.content_sha256,
            )
            | changes
        )
    )


@pytest.mark.parametrize(
    "missing",
    [
        "identity_verified",
        "installed",
        "enabled",
        "allowlisted",
        "owner_allowed",
        "csrf_valid",
        "resource_allowed",
    ],
)
def test_admin_cannot_bypass_any_original_gate(missing: str) -> None:
    c = core()
    with pytest.raises(PermissionError):
        c.require_permission(
            state(c),
            actor_id="admin",
            policy=policy(c),
            method="GET",
            path="/api/v1/factors/results",
            conditions=gates(c, **{missing: False}),
        )


def test_maintenance_research_and_read_are_separate() -> None:
    c = core()
    s = state(c)
    c.require_permission(
        s,
        actor_id="alice",
        policy=policy(c),
        method="POST",
        path="/api/v1/tasks/jobs/commands",
        command_kind="submit_lab_command",
        conditions=gates(c),
    )
    c.require_permission(
        s,
        actor_id="bob",
        policy=policy(c),
        method="GET",
        path="/api/v1/factors/results",
        conditions=gates(c),
    )
    with pytest.raises(PermissionError):
        c.require_permission(
            s,
            actor_id="alice",
            policy=policy(c),
            method="POST",
            path="/api/v1/data/executions/commands",
            command_kind="execute_backfill_plan",
            conditions=gates(c),
        )


@pytest.mark.parametrize(
    "path,kind",
    [
        ("/api/v1/future", None),
        ("/api/v1/data/executions/commands", "future_kind"),
        ("/api/v1/data/executions/commands", None),
    ],
)
def test_unknown_operation_or_kind_denies(path: str, kind: str | None) -> None:
    c = core()
    with pytest.raises(PermissionError):
        c.require_permission(
            state(c),
            actor_id="admin",
            policy=policy(c),
            method="POST",
            path=path,
            command_kind=kind,
            conditions=gates(c),
        )


def test_missing_unknown_tampered_and_legacy_state_deny() -> None:
    c = core()
    s = state(c)
    for authority, actor, mode in [
        (None, "admin", "enforced"),
        (s, "unknown", "enforced"),
        (s, "admin", "legacy"),
        (s.model_copy(update={"revision": 4}), "admin", "enforced"),
    ]:
        with pytest.raises(PermissionError):
            c.require_permission(
                authority,
                actor_id=actor,
                policy=policy(c),
                method="GET",
                path="/api/v1/factors/results",
                conditions=gates(c),
                mode=mode,
            )


def test_role_file_parser_is_versioned_bounded_and_hash_checked() -> None:
    c = core()
    s = state(c)
    assert c.parse_role_state(s.model_dump_json().encode()) == s
    for data in [
        None,
        b"{}",
        s.model_dump_json()
        .replace('"schema_version":1', '"schema_version":2')
        .encode(),
        b" " * (c.MAX_ROLE_STATE_BYTES + 1),
    ]:
        with pytest.raises(PermissionError):
            c.parse_role_state(data)
    with pytest.raises(ValueError):
        c.RoleState.create(
            revision=1, users=(c.RoleEntry(username="alice", role="viewer"),)
        )
    with pytest.raises(ValueError):
        c.RoleState.create(
            revision=1, users=(c.RoleEntry(username="admin", role="admin"),) * 2
        )


def test_prepare_does_not_change_roles_confirm_cas_changes_once() -> None:
    c = core()
    s = state(c)
    r = request(c, s)
    challenge = c.prepare_role_change(s, r, now=NOW)
    assert s.users[1].role == "researcher"
    assert challenge.expires_at == NOW + timedelta(minutes=2)
    result = c.confirm_role_change(
        s, r, challenge, entered_target="alice", now=NOW + timedelta(seconds=1)
    )
    assert result.revision == 4 and result.users[1].role == "viewer"
    with pytest.raises(PermissionError):
        c.confirm_role_change(
            result, r, challenge, entered_target="alice", now=NOW + timedelta(seconds=2)
        )


@pytest.mark.parametrize(
    "change",
    ["expired", "future", "actor", "target", "role", "uuid", "body", "username"],
)
def test_confirmation_is_bound_to_exact_request_and_time(change: str) -> None:
    c = core()
    s = state(c)
    r = request(c, s)
    challenge = c.prepare_role_change(s, r, now=NOW)
    mutated = {
        "actor": {"actor_id": "alice"},
        "target": {"target_id": "bob"},
        "role": {"new_role": "admin"},
        "uuid": {"command_id": str(UUID(int=2))},
        "body": {"expected_state_sha256": "f" * 64},
    }.get(change, {})
    r = r.model_copy(update=mutated)
    when = (
        NOW + timedelta(minutes=2)
        if change == "expired"
        else NOW - timedelta(seconds=1)
        if change == "future"
        else NOW
    )
    with pytest.raises(PermissionError):
        c.confirm_role_change(
            s,
            r,
            challenge,
            entered_target="bob" if change == "username" else "alice",
            now=when,
        )


def test_current_revocation_last_admin_and_unknown_target_deny() -> None:
    c = core()
    s = state(c)
    for target in ("admin", "unknown"):
        with pytest.raises(PermissionError):
            c.prepare_role_change(s, request(c, s, target_id=target), now=NOW)
    challenge = c.prepare_role_change(s, request(c, s), now=NOW)
    revoked = c.RoleState.create(
        revision=4,
        users=(
            c.RoleEntry(username="admin", role="viewer"),
            c.RoleEntry(username="alice", role="admin"),
            c.RoleEntry(username="bob", role="viewer"),
        ),
    )
    with pytest.raises(PermissionError):
        c.confirm_role_change(
            revoked, request(c, s), challenge, entered_target="alice", now=NOW
        )


def test_invalid_users_roles_policy_and_naive_time_reject() -> None:
    c = core()
    for user in ("", " alice", "a\x00b", "../alice"):
        with pytest.raises(ValueError):
            c.RoleEntry(username=user, role="viewer")
    with pytest.raises(ValueError):
        c.RoleEntry(username="alice", role="owner")
    with pytest.raises(ValueError):
        c.PermissionPolicy(entries=policy(c).entries * 2)
    with pytest.raises(PermissionError):
        c.prepare_role_change(
            state(c), request(c, state(c)), now=NOW.replace(tzinfo=None)
        )


def test_version_bool_duplicate_json_and_undeclared_actor_flags_reject() -> None:
    c = core()
    s = state(c)
    for version in (True, "1", 2):
        data = json.loads(s.model_dump_json()) | {"schema_version": version}
        with pytest.raises(PermissionError):
            c.parse_role_state(json.dumps(data).encode())
    duplicate = (
        s.model_dump_json()
        .replace('"schema_version":1', '"schema_version":2,"schema_version":1')
        .encode()
    )
    with pytest.raises(PermissionError):
        c.parse_role_state(duplicate)
    with pytest.raises(ValueError):
        gates(c, identity_verified=1)
