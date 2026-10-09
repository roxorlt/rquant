"""Pure C15 rules; a model or digest is not a trusted identity or role authority.

The PageControl adapter must read its one current roles file, verify the original
ingress, intersect original access conditions, and persist the original command
and atomic effect. These rules do not install users, sign confirmations, perform
UUID journal lookup, or provide a second authentication system. Legacy callers
retain their original behavior outside this module; C15 is unavailable in legacy.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from hmac import compare_digest
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import canonical_sha256

RoleName = Literal["admin", "researcher", "viewer"]
# Same username alphabet and length as the original web.security.current_user.
UserId = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._@-]+$")
]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Revision = Annotated[int, Field(ge=1, le=2**63 - 1)]
MAX_ROLE_STATE_BYTES = 64 * 1024
MAX_ROLE_USERS = 256
MAX_PERMISSION_RULES = 2048
ROLE_CONFIRMATION_TTL = timedelta(minutes=2)


class _RuleModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    @field_validator("schema_version", mode="before", check_fields=False)
    @classmethod
    def exact_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("unknown role rule version")
        return value


class RoleEntry(_RuleModel):
    username: UserId
    role: RoleName


class RoleState(_RuleModel):
    schema_version: Literal[1] = 1
    revision: Revision
    users: tuple[RoleEntry, ...] = Field(min_length=1, max_length=MAX_ROLE_USERS)
    content_sha256: Sha256

    @model_validator(mode="after")
    def verify_state(self) -> Self:
        names = tuple(entry.username for entry in self.users)
        if len(set(names)) != len(names) or not any(
            entry.role == "admin" for entry in self.users
        ):
            raise ValueError(
                "role state needs unique registered users and an explicit admin"
            )
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"content_sha256"})
        )
        if not compare_digest(self.content_sha256, expected):
            raise ValueError("role state digest differs")
        if len(self.model_dump_json().encode("utf-8")) > MAX_ROLE_STATE_BYTES:
            raise ValueError("role state exceeds capacity")
        return self

    @classmethod
    def create(cls, *, revision: int, users: tuple[RoleEntry, ...]) -> Self:
        """Build an explicit state, not a bootstrap or first-visitor assignment."""
        body = {"schema_version": 1, "revision": revision, "users": users}
        return cls(**body, content_sha256=canonical_sha256(body))


class PermissionRule(_RuleModel):
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
    path: str = Field(
        min_length=9, max_length=256, pattern=r"^/api/v1/[A-Za-z0-9_{}./-]+$"
    )
    command_kind: str | None = Field(
        default=None, min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_]*$"
    )
    roles: tuple[RoleName, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def unique_roles(self) -> Self:
        if (
            len(set(self.roles)) != len(self.roles)
            or "/../" in self.path
            or "//" in self.path
        ):
            raise ValueError("permission rule is ambiguous")
        return self


class PermissionPolicy(_RuleModel):
    schema_version: Literal[1] = 1
    entries: tuple[PermissionRule, ...] = Field(max_length=MAX_PERMISSION_RULES)

    @model_validator(mode="after")
    def unique_operations(self) -> Self:
        keys = tuple(
            (entry.method, entry.path, entry.command_kind) for entry in self.entries
        )
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate permission operation")
        return self


class OriginalAccessConditions(_RuleModel):
    """Caller assertions requiring original trusted verification at each gate."""

    identity_verified: bool
    installed: bool
    enabled: bool
    allowlisted: bool
    owner_allowed: bool
    csrf_valid: bool
    resource_allowed: bool


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate role JSON key")
        value[key] = item
    return value


def _reject_constant(value: str) -> object:
    raise ValueError("non-finite role JSON")


def parse_role_state(data: bytes | None) -> RoleState:
    """Parse bytes already read by the original private authority; never read files."""
    try:
        if not isinstance(data, bytes) or not data or len(data) > MAX_ROLE_STATE_BYTES:
            raise ValueError("role bytes absent or over capacity")
        json.loads(
            data, object_pairs_hook=_unique_json_object, parse_constant=_reject_constant
        )
        return RoleState.model_validate_json(data)
    except (ValueError, TypeError, RecursionError) as exc:
        raise PermissionError("current role authority is unavailable") from exc


def _current_state(state: RoleState | None) -> RoleState:
    try:
        if type(state) is not RoleState:
            raise ValueError("current role state absent")
        return RoleState.model_validate(state.model_dump(mode="python"))
    except (ValueError, TypeError) as exc:
        raise PermissionError("current role authority is unavailable") from exc


def _role(state: RoleState, actor_id: str) -> RoleName:
    for entry in state.users:
        if entry.username == actor_id:
            return entry.role
    raise PermissionError("current user has no registered role")


def require_permission(
    state: RoleState | None,
    *,
    actor_id: str,
    policy: PermissionPolicy,
    method: str,
    path: str,
    conditions: OriginalAccessConditions,
    command_kind: str | None = None,
    mode: Literal["enforced", "legacy"] = "enforced",
) -> RoleName:
    """Match the original router's exact descriptor, never a future-route fallback."""
    if mode != "enforced":
        raise PermissionError("C15 needs enforced current authority")
    authority = _current_state(state)
    try:
        policy = PermissionPolicy.model_validate(policy.model_dump(mode="python"))
        conditions = OriginalAccessConditions.model_validate(
            conditions.model_dump(mode="python")
        )
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("original access facts or policy are invalid") from exc
    if not all(conditions.model_dump(mode="python").values()):
        raise PermissionError("an original access condition denies this operation")
    role = _role(authority, actor_id)
    for entry in policy.entries:
        if (entry.method, entry.path, entry.command_kind) == (
            method,
            path,
            command_kind,
        ):
            if role in entry.roles:
                return role
            break
    raise PermissionError("current role cannot perform this operation")


class RoleChangeRequest(_RuleModel):
    schema_version: Literal[1] = 1
    kind: Literal["set_user_role"] = "set_user_role"
    command_id: str = Field(min_length=36, max_length=36)
    actor_id: UserId
    target_id: UserId
    new_role: RoleName
    expected_revision: Revision
    expected_state_sha256: Sha256

    @field_validator("command_id")
    @classmethod
    def canonical_uuid(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("role command needs the original canonical UUID")
        return value


class RoleChangeConfirmation(_RuleModel):
    """Pure binding content; the original service must protect issuance and lookup."""

    schema_version: Literal[1] = 1
    command_id: str
    request_sha256: Sha256
    actor_id: UserId
    target_id: UserId
    old_role: RoleName
    new_role: RoleName
    expected_revision: Revision
    expected_state_sha256: Sha256
    issued_at: AwareDatetime
    expires_at: AwareDatetime
    content_sha256: Sha256

    @model_validator(mode="after")
    def verify_binding(self) -> Self:
        if (
            str(UUID(self.command_id)) != self.command_id
            or self.expires_at - self.issued_at != ROLE_CONFIRMATION_TTL
        ):
            raise ValueError("invalid role confirmation interval or command")
        if self.content_sha256 != canonical_sha256(
            self.model_dump(mode="python", exclude={"content_sha256"})
        ):
            raise ValueError("role confirmation content differs")
        return self


def _role_change(
    state: RoleState | None, request: RoleChangeRequest
) -> tuple[RoleState, RoleChangeRequest, RoleName]:
    authority = _current_state(state)
    try:
        request = RoleChangeRequest.model_validate(request.model_dump(mode="python"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("role command body is invalid") from exc
    if _role(authority, request.actor_id) != "admin":
        raise PermissionError("current admin permission is required")
    if (request.expected_revision, request.expected_state_sha256) != (
        authority.revision,
        authority.content_sha256,
    ):
        raise PermissionError("role state changed; prepare again")
    old = _role(authority, request.target_id)
    if (
        old == "admin"
        and request.new_role != "admin"
        and sum(entry.role == "admin" for entry in authority.users) == 1
    ):
        raise PermissionError("the last admin cannot be removed")
    if authority.revision == 2**63 - 1:
        raise PermissionError("role revision capacity exceeded")
    return authority, request, old


def _aware_now(now: datetime) -> None:
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise PermissionError("role confirmation needs an aware service time")


def prepare_role_change(
    state: RoleState | None, request: RoleChangeRequest, *, now: datetime
) -> RoleChangeConfirmation:
    _aware_now(now)
    authority, request, old = _role_change(state, request)
    body = {
        "schema_version": 1,
        "command_id": request.command_id,
        "request_sha256": canonical_sha256(request.model_dump(mode="python")),
        "actor_id": request.actor_id,
        "target_id": request.target_id,
        "old_role": old,
        "new_role": request.new_role,
        "expected_revision": authority.revision,
        "expected_state_sha256": authority.content_sha256,
        "issued_at": now,
        "expires_at": now + ROLE_CONFIRMATION_TTL,
    }
    return RoleChangeConfirmation(**body, content_sha256=canonical_sha256(body))


def confirm_role_change(
    state: RoleState | None,
    request: RoleChangeRequest,
    confirmation: RoleChangeConfirmation,
    *,
    entered_target: str,
    now: datetime,
) -> RoleState:
    _aware_now(now)
    authority, request, old = _role_change(state, request)
    try:
        confirmation = RoleChangeConfirmation.model_validate(
            confirmation.model_dump(mode="python")
        )
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("role confirmation is invalid") from exc
    if not confirmation.issued_at <= now < confirmation.expires_at:
        raise PermissionError("role confirmation expired or not yet valid")
    if entered_target != request.target_id:
        raise PermissionError("enter the exact target username")
    expected = (
        request.command_id,
        canonical_sha256(request.model_dump(mode="python")),
        request.actor_id,
        request.target_id,
        old,
        request.new_role,
        authority.revision,
        authority.content_sha256,
    )
    actual = (
        confirmation.command_id,
        confirmation.request_sha256,
        confirmation.actor_id,
        confirmation.target_id,
        confirmation.old_role,
        confirmation.new_role,
        confirmation.expected_revision,
        confirmation.expected_state_sha256,
    )
    if actual != expected:
        raise PermissionError("role confirmation belongs to different command content")
    users = tuple(
        RoleEntry(
            username=entry.username,
            role=request.new_role
            if entry.username == request.target_id
            else entry.role,
        )
        for entry in authority.users
    )
    return RoleState.create(revision=authority.revision + 1, users=users)
