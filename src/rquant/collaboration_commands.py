"""Additive role command bodies for the original PageControl owner.

These models and hashes do not issue trusted confirmations or prove a caller's
identity. The original owner must verify ingress/current access, store its issued
preparation, look up the original UUID/body, and apply the Core state through its
existing atomic command/effect path. No old payload, writer or parser is changed.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import secrets
import stat
import tempfile
import threading
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from datetime import datetime, timedelta
from hmac import compare_digest
from pathlib import Path
from typing import Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    TypeAdapter,
    ValidationInfo,
    field_validator,
    model_validator,
)

from rquant.collaboration_roles import (
    PermissionPolicy,
    PermissionRule,
    RoleChangeConfirmation,
    RoleChangeRequest,
    RoleName,
    RoleState,
    Sha256,
    UserId,
    confirm_role_change,
    parse_role_state,
    prepare_role_change,
)
from rquant.runtime_contracts import canonical_sha256

MAX_ROLE_COMMAND_BYTES = 32 * 1024


class SetUserRoleRequest(RoleChangeRequest):
    schema_version: Literal[1]
    kind: Literal["set_user_role"]
    requested_at: AwareDatetime

    @field_validator("requested_at", mode="before")
    @classmethod
    def http_utc_request(cls, value: object) -> object:
        if isinstance(value, str):
            return TypeAdapter(AwareDatetime).validate_json(json.dumps(value))
        return value

    @field_validator("requested_at")
    @classmethod
    def utc_request(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("role command time must be UTC")
        return value

    def core_request(self) -> RoleChangeRequest:
        return RoleChangeRequest.model_validate(
            self.model_dump(mode="python", exclude={"requested_at"})
        )


class PreparedUserRoleChange(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )
    schema_version: Literal[1]
    request: SetUserRoleRequest
    request_sha256: Sha256
    confirmation: RoleChangeConfirmation

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("unknown role preparation version")
        return value

    @field_validator("confirmation", mode="before")
    @classmethod
    def explicit_confirmation_version(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, dict) and (
            type(value.get("schema_version")) is not int or value.get("schema_version") != 1
        ):
            raise ValueError("role confirmation needs its explicit version")
        if isinstance(value, dict) and (info.mode == "json" or isinstance(value.get("issued_at"), str)):
            # Keep strict Core datetime validation in JSON mode after this boundary.
            return RoleChangeConfirmation.model_validate_json(json.dumps(value))
        return value

    @model_validator(mode="after")
    def bound_request(self) -> Self:
        request = self.request.core_request()
        if not compare_digest(
            self.request_sha256, canonical_sha256(self.request.model_dump(mode="json"))
        ) or (
            self.confirmation.request_sha256 != canonical_sha256(request.model_dump(mode="python"))
        ):
            raise ValueError("role preparation differs from its full original request")
        for name in (
            "command_id",
            "actor_id",
            "target_id",
            "new_role",
            "expected_revision",
            "expected_state_sha256",
        ):
            if getattr(self.confirmation, name) != getattr(request, name):
                raise ValueError("role preparation binding differs")
        return self


class SetUserRoleCommand(SetUserRoleRequest):
    preparation: PreparedUserRoleChange
    entered_target: UserId

    def original_request(self) -> SetUserRoleRequest:
        return SetUserRoleRequest.model_validate(
            self.model_dump(mode="python", exclude={"preparation", "entered_target"})
        )

    @model_validator(mode="after")
    def same_preparation(self) -> Self:
        if self.original_request() != self.preparation.request:
            raise ValueError("role command differs from its original preparation")
        return self


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate role command JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ValueError("non-finite role command JSON")


def parse_set_user_role_command(payload: bytes) -> SetUserRoleCommand:
    """Parse only the explicit new contract; do not fall back to legacy commands."""
    try:
        if type(payload) is not bytes or not payload or len(payload) > MAX_ROLE_COMMAND_BYTES:
            raise ValueError("role command is absent or exceeds capacity")
        json.loads(payload, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        return SetUserRoleCommand.model_validate_json(payload)
    except (ValueError, TypeError, RecursionError) as exc:
        raise ValueError("invalid role command body") from exc


def _command(value: SetUserRoleCommand) -> SetUserRoleCommand:
    try:
        if type(value) is not SetUserRoleCommand:
            raise ValueError("role command type differs")
        return parse_set_user_role_command(value.model_dump_json().encode())
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("role command is not valid") from exc


def role_command_sha256(command: SetUserRoleCommand) -> str:
    """Use the original PageControl command hash convention, including every field."""
    return canonical_sha256(_command(command).model_dump(mode="json"))


def prepare_set_user_role(
    state: RoleState | None,
    request: SetUserRoleRequest,
    *,
    now: datetime,
) -> PreparedUserRoleChange:
    try:
        if type(request) is not SetUserRoleRequest:
            raise ValueError("role request type differs")
        request = SetUserRoleRequest.model_validate(request.model_dump(mode="python"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("role request is not valid") from exc
    return PreparedUserRoleChange(
        schema_version=1,
        request=request,
        request_sha256=canonical_sha256(request.model_dump(mode="json")),
        confirmation=prepare_role_change(state, request.core_request(), now=now),
    )


def confirm_set_user_role(
    state: RoleState | None,
    command: SetUserRoleCommand,
    *,
    original_preparation: PreparedUserRoleChange | None,
    now: datetime,
) -> RoleState:
    """Require the owner's actual issued preparation; never reconstruct it on retry."""
    command = _command(command)
    try:
        if type(original_preparation) is not PreparedUserRoleChange:
            raise ValueError("original preparation is absent")
        original = PreparedUserRoleChange.model_validate(
            original_preparation.model_dump(mode="python")
        )
        if command.preparation != original or command.original_request() != original.request:
            raise ValueError("original preparation differs")
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("original role preparation is unavailable or differs") from exc
    return confirm_role_change(
        state,
        original.request.core_request(),
        original.confirmation,
        entered_target=command.entered_target,
        now=now,
    )


def require_original_role_command(
    command: SetUserRoleCommand,
    *,
    command_id: str,
    command_hash: str,
    payload_json: bytes,
) -> None:
    """Compare a row already obtained by original UUID lookup; no IO or authority proof."""
    candidate = _command(command)
    try:
        stored = parse_set_user_role_command(payload_json)
        if (
            command_id != candidate.command_id
            or stored.command_id != command_id
            or not compare_digest(command_hash, role_command_sha256(stored))
            or stored != candidate
        ):
            raise ValueError("original command UUID, full body or hash differs")
    except (ValueError, TypeError) as exc:
        raise PermissionError("original role command lookup differs") from exc


class CommandAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[1]
    origin: Literal["page-control-private-peer/v1"]
    actor_id: UserId
    command_id: str
    command_kind: str
    command_sha256: Sha256
    role_revision: int
    role_state_sha256: Sha256
    issued_at: AwareDatetime
    expires_at: AwareDatetime
    signature: Sha256
    journal_identity: Sha256 | None = None

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_private_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("unknown private command authorization version")
        return value


class IssuedRolePreparation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    preparation: PreparedUserRoleChange
    issuance_proof: Sha256


# These kinds are the original PageControl union. New kinds are denied until their
# original endpoint and owner have been classified; role never grants a backend.
_RESEARCH_KINDS = frozenset({
    "submit_minute_replay", "export_minute_replay_zip", "submit_minute_parameter_study",
    "request_promotion_review", "run_strategy_walk_forward",
    "ack_alert", "execute_screen_query", "save_screen_query_preset", "save_research_query",
    "add_watchlist_item", "remove_watchlist_item", "save_price_alert_rule",
    "set_price_alert_rule_enabled", "delete_price_alert_rule", "save_alert_rule",
    "set_alert_rule_enabled", "delete_alert_rule", "save_factor_definition", "archive_factor",
    "submit_factor_run", "set_factor_tracked", "save_strategy_template", "archive_strategy_template",
    "run_strategy_template", "set_paper_account_paused", "save_paper_portfolio_configuration",
    "run_paper_portfolio_research", "save_canvas", "create_canvas", "delete_canvas",
    "set_canvas_pool_refs", "add_pool_to_canvas", "save_user_pool", "save_user_pool_v2",
    "save_user_pool_v3", "save_formula_pool_v1", "delete_user_pool", "fork_builtin_pool",
    "save_nl_preset", "append_nl_query_log", "initialize_lab_exports", "submit_lab_command",
    "submit_backfill_plan", "submit_data_audit_report", "submit_formula_market_run",
    "export_lab_artifact_zip", "submit_portfolio_backtest", "export_portfolio_backtest_zip",
    "register_experiment_family", "cancel_experiment_family", "set_experiment_note",
    "unseal_experiment_outer_test", "discard_lab_artifact_zip",
    "set_monitor_builtin_enabled",
})
_ADMIN_KINDS = frozenset({
    "prepare_promotion_approval", "approve_promotion",
    "set_user_role", "prepare_unit_run", "request_unit_run", "set_lab_scheduling_paused",
    "prepare_notifier_delivery_mode", "set_notifier_delivery_mode",
    "prepare_backfill_execution", "execute_backfill_plan", "pause_data_center_execution",
    "resume_data_center_execution", "prepare_financial_collection", "execute_financial_collection",
    "set_experiment_holdout_policy",
})


class PageControlRoleAuthority:
    """The original PageControl owner's adapter to its one installed roles file.

    Private peer admission proves ingress; the short-lived MAC only carries that
    existing proof over original loopback. It does not authenticate body actors.
    Restart invalidates unsubmitted frames. Durable provenance stays in the same
    protected command row, whose original payload/hash must be checked on reads.
    """

    def __init__(self, *, mode: Literal["legacy", "enforced"] = "legacy",
                 roles_path: Path | None = None, clock: Callable[[], datetime] | None = None) -> None:
        if mode not in {"legacy", "enforced"}:
            raise ValueError("unknown collaboration mode")
        from datetime import UTC
        self.mode = mode
        self.roles_path = None if roles_path is None else Path(os.path.abspath(roles_path))
        self._locked_directory: ContextVar[tuple[int, int, bool] | None] = ContextVar("page_control_role_directory", default=None)
        self.clock = clock or (lambda: datetime.now(UTC))
        self._frame_key = secrets.token_bytes(32)
        self._journal_path: Path | None = None
        self._journal_identity: str | None = None

    def bind_outbox(self, path: Path) -> None:
        if self.mode != "enforced":
            return
        path = Path(os.path.abspath(path))
        if self.roles_path is None or path.parent != self.roles_path.parent:
            raise PermissionError("roles must be installed beside the original private outbox")
        info = path.stat(follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise PermissionError("original outbox is not owner-private")
        identity = canonical_sha256({"path": str(path), "device": info.st_dev, "inode": info.st_ino})
        if self._journal_path is not None and (path != self._journal_path or identity != self._journal_identity):
            raise PermissionError("role authority is already bound to another original outbox")
        self._journal_path = path
        self._journal_identity = identity

    def require_outbox_path(self, path: Path) -> str:
        if self.mode != "enforced" or Path(os.path.abspath(path)) != self._journal_path:
            raise PermissionError("readonly source differs from original bound outbox")
        identity = self.require_outbox_identity()
        if identity is None:
            raise PermissionError("original bound outbox is unavailable")
        return identity

    def require_outbox_identity(self) -> str | None:
        if self._journal_path is None:
            return None
        info = self._journal_path.stat(follow_symlinks=False)
        actual = canonical_sha256({"path": str(self._journal_path), "device": info.st_dev, "inode": info.st_ino})
        if (actual != self._journal_identity or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid() or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise PermissionError("original outbox identity changed")
        return actual

    @contextlib.contextmanager
    def locked(self, *, read_only: bool = False) -> Iterator[int]:
        path = self.roles_path
        if self.mode != "enforced" or path is None or path.name != "roles.json":
            raise PermissionError("current role authority is unavailable")
        active = self._locked_directory
        held = active.get()
        if held is not None and held[0] == threading.get_ident():
            if not read_only and not held[2]:
                raise PermissionError("read-only role lock cannot be upgraded")
            yield held[1]
            return
        try:
            if path.parent.resolve(strict=True) != path.parent:
                raise ValueError("role parent is linked")
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                info = os.fstat(fd)
                if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
                    raise ValueError("role parent is not owner-private")
                fcntl.flock(fd, fcntl.LOCK_SH if read_only else fcntl.LOCK_EX)
                token = active.set((threading.get_ident(), fd, not read_only))
                try:
                    yield fd
                finally:
                    active.reset(token)
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
        except (OSError, ValueError) as exc:
            raise PermissionError("current role authority is unavailable") from exc

    def _read_locked(self, directory: int) -> RoleState:
        self.require_outbox_identity()
        fd = os.open("roles.json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        try:
            before = os.fstat(fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                    or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                    or not 1 <= before.st_size <= 64 * 1024):
                raise PermissionError("role file is not owner-private")
            data = os.read(fd, 64 * 1024 + 1)
            after = os.fstat(fd)
            named = os.stat("roles.json", dir_fd=directory, follow_symlinks=False)
            if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    or (after.st_dev, after.st_ino) != (named.st_dev, named.st_ino)):
                raise PermissionError("role file changed during read")
            return parse_role_state(data)
        finally:
            os.close(fd)

    def read_state(self) -> RoleState:
        try:
            with self.locked(read_only=True) as directory:
                return self._read_locked(directory)
        except (OSError, ValueError) as exc:
            raise PermissionError("current role authority is unavailable") from exc

    def current_role(self, actor_id: str) -> RoleName:
        state = self.read_state()
        for entry in state.users:
            if entry.username == actor_id:
                return entry.role
        raise PermissionError("current user has no registered role")

    def require_operation(self, actor_id: str, method: str, path: str) -> RoleName:
        role = self.current_role(actor_id)
        for rule in COLLABORATION_POLICY.entries:
            if (rule.method, rule.path, rule.command_kind) == (method, path, None):
                if role in rule.roles:
                    return role
                break
        raise PermissionError("current role cannot perform this operation")

    def require_command(self, actor_id: str, kind: str) -> RoleName:
        role = self.current_role(actor_id)
        permitted = {"admin"} if kind in _ADMIN_KINDS else (
            {"admin", "researcher"} if kind in _RESEARCH_KINDS else set()
        )
        if role not in permitted:
            raise PermissionError("current role cannot perform this command")
        return role

    def _mac(self, purpose: str, body: Mapping[str, object]) -> str:
        digest = canonical_sha256({"purpose": purpose, "body": body})
        return hmac.new(self._frame_key, digest.encode("ascii"), hashlib.sha256).hexdigest()

    def issue_authorization(self, actor_id: str, body: Mapping[str, object]) -> CommandAuthorization:
        self.require_command(actor_id, str(body.get("kind", "")))
        if any(body.get(name) not in (None, actor_id) for name in ("actor_id", "owner_id")):
            raise PermissionError("original authenticated actor differs")
        state = self.read_state()
        now = self.clock()
        unsigned = {"schema_version": 1, "origin": "page-control-private-peer/v1",
            "actor_id": actor_id, "command_id": str(body.get("command_id", "")),
            "command_kind": str(body.get("kind", "")), "command_sha256": canonical_sha256(body),
            "role_revision": state.revision, "role_state_sha256": state.content_sha256,
            "journal_identity": self.require_outbox_identity(),
            "issued_at": now, "expires_at": now + timedelta(minutes=2)}
        return CommandAuthorization(**unsigned, signature=self._mac("command", unsigned))

    def verify_authorization(self, proof: CommandAuthorization, body: Mapping[str, object]) -> CommandAuthorization:
        try:
            proof = CommandAuthorization.model_validate_json(proof.model_dump_json())
            unsigned = proof.model_dump(mode="python", exclude={"signature"})
            if (not compare_digest(proof.signature, self._mac("command", unsigned))
                    or proof.command_sha256 != canonical_sha256(body)
                    or proof.command_id != body.get("command_id") or proof.command_kind != body.get("kind")
                    or proof.journal_identity != self.require_outbox_identity()
                    or not proof.issued_at <= self.clock() < proof.expires_at
                    or proof.expires_at - proof.issued_at != timedelta(minutes=2)):
                raise ValueError("original private authorization differs or expired")
            self.require_command(proof.actor_id, proof.command_kind)
            return proof
        except (ValueError, TypeError, AttributeError) as exc:
            raise PermissionError("original private authorization is unavailable") from exc

    def prepare_role(self, request: SetUserRoleRequest, *, authenticated_actor_id: str) -> IssuedRolePreparation:
        if request.actor_id != authenticated_actor_id:
            raise PermissionError("original authenticated actor differs")
        preparation = prepare_set_user_role(self.read_state(), request, now=self.clock())
        return IssuedRolePreparation(preparation=preparation,
            issuance_proof=self._mac("role-preparation", preparation.model_dump(mode="python")))

    def validate_preparation(self, command: SetUserRoleCommand, *, authenticated_actor_id: str,
                             issuance_proof: str) -> RoleState:
        if (command.actor_id != authenticated_actor_id or not compare_digest(issuance_proof,
                self._mac("role-preparation", command.preparation.model_dump(mode="python")))):
            raise PermissionError("original role preparation is unavailable")
        return confirm_set_user_role(self.read_state(), command,
            original_preparation=command.preparation, now=self.clock())

    def _write_locked(self, directory: int, state: RoleState) -> None:
        held = self._locked_directory.get()
        if held is None or held != (threading.get_ident(), directory, True):
            raise PermissionError("role write requires the original exclusive lock")
        assert self.roles_path is not None
        descriptor, name = tempfile.mkstemp(prefix=".roles-", dir=self.roles_path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            raw = state.model_dump_json().encode("utf-8")
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(Path(name).name, "roles.json", src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            if os.path.lexists(name):
                os.unlink(name)

    def confirm_role(self, command: SetUserRoleCommand, *, authenticated_actor_id: str,
                     issuance_proof: str) -> RoleState:
        with self.locked() as directory:
            # No nested lock: validate binding and current CAS under this owner lock.
            state = self._read_locked(directory)
            if command.actor_id != authenticated_actor_id or not compare_digest(issuance_proof,
                    self._mac("role-preparation", command.preparation.model_dump(mode="python"))):
                raise PermissionError("original role preparation is unavailable")
            after = confirm_set_user_role(state, command,
                original_preparation=command.preparation, now=self.clock())
            self._write_locked(directory, after)
            return after


# Original accepted 146 operations plus the actual AI9; no wildcard/future allow.
COLLABORATION_POLICY = PermissionPolicy(entries=(
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/sources', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/parameter-sources', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/studies/capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/studies', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/backtests/minute-runtime/studies', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/studies/{command_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/studies/{command_id}/heatmap', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/runs', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/backtests/minute-runtime/runs', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/runs/{job_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/runs/{job_id}/nav', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/runs/{job_id}/rows', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/runs/{job_id}/report.html', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/minute-runtime/runs/{job_id}/exports/{request_id}.zip', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/backtests/minute-runtime/exports', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/strategy-promotions/{strategy_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/strategy-promotions/commands', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/strategy-promotions/commands/lookup', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/strategy-promotions/commands/resume', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/backtests/portfolio/exports', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/runs', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/backtests/portfolio/runs', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/runs/{job_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/runs/{job_id}/exports/{request_id}.zip', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/runs/{job_id}/nav', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/runs/{job_id}/report.html', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/portfolio/runs/{job_id}/rows', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/backtests/{run_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/audit-report/calendar', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/data/audit-report/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/data/backfill-plans', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/data/backfill-plans/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/data/backfill-plans/{plan_hash}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/catalog', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/catalog/{dataset}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/collection', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/executions', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/data/executions/commands', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/data/financial-sources', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/fundamentals/summary', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/health', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/issues', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/data/report', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/experiments/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/experiments/compare', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/families/{family_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/families/{family_id}/heatmap', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/mine', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/results/{experiment_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/results/{experiment_id}/statistics', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/factors/capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/factors/definitions', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/factors/definitions/save', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/definitions/save/resume', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/definitions/save/retry', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/definitions/{factor_id}/archive', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/definitions/{factor_id}/archive/resume', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/factors/results', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/factors/results/{job_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/factors/run-availability', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/factors/runs', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/runs/resume', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/runs/retry', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/tracking/commands', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/tracking/commands/resume', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/factors/tracking/commands/retry', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/factors/{factor_id}/tracking', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/health', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/meta', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/monitor/ack', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/monitor/channels', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/monitor/condition-rules', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/monitor/condition-rules/commands', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/monitor/condition-rules/commands/resume', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/monitor/condition-rules/head', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/monitor/price-rules', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/monitor/price-rules/commands', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/monitor/price-rules/commands/resume', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/monitor/price-rules/events', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/monitor/price-rules/head', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/monitor/price-rules/runtime', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/monitor/runtime', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/monitor/timeline', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/overview', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/boards', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/boards/{board_code}/members', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/pulse', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/stocks/{ts_code}/daily', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/stocks/{ts_code}/intraday', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/surge', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/panorama/surge/search', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/paper-portfolios', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/paper-portfolios/{account_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/paper-portfolios/{account_id}/band', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/paper-portfolios/{account_id}/configuration', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/paper-portfolios/{account_id}/history', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/paper-portfolios/{account_id}/pause/confirm', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/paper-portfolios/{account_id}/pause/prepare', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/paper-portfolios/{account_id}/reconcile', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/paper-portfolios/{account_id}/recover', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/paper-portfolios/{account_id}/research/{job_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/paper-portfolios/{account_id}/research/{job_id}/download', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/paper/accounts', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/pools', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/pools/editor', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/pools/editor/commands', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/pools/editor/nl-preview', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/pools/formula', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/pools/formula/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/pools/formula/{base_name}/members', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/research/catalog', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/research/queries', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/research/queries/resume', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/research/queries/save', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/research/query', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/screen/blocks', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/nl-preview', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/screen/query/alert-draft', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/screen/query/alert-drafts/{draft_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/query/execute', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/screen/query/executions/{execution_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/screen/query/executions/{execution_id}/results', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/screen/query/history', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/query/lookup', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/screen/query/presets', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/query/presets/save', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/screen/query/resume', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/screen/run', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/tdx/market/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/screen/tdx/market/jobs', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/screen/tdx/market/jobs/{task_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/screen/tdx/market/jobs/{task_id}/matches', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/tdx/parse', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/screen/tdx/preview', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/screen/tdx/preview/source', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/stocks/search', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/stocks/{ts_code}/summary', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/strategies', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/strategy-templates', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/strategy-templates/commands', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/strategy-templates/commands/resume', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/strategy-templates/sources', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/strategy-templates/{strategy_id}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/strategy-templates/{strategy_id}/runs', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/strategy-templates/{strategy_id}/runs/resume', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/strategy-templates/{strategy_id}/versions', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/tasks/control-capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/tasks/controls/lookup', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/tasks/controls/resume', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/tasks/jobs', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/tasks/jobs/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/tasks/jobs/control-capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/tasks/jobs/{job_id}/events', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/tasks/overview', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/tasks/notifications/mode', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/tasks/notifications/mode/prepare', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/tasks/monitor/builtins/commands', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/tasks/scheduling/commands', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/tasks/services/log-capabilities', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/tasks/services/{unit}/logs', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/tasks/units/{unit}/run', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/tasks/units/{unit}/run/prepare', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/watchlist', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/watchlist/commands', roles=('admin', 'researcher')),
    PermissionRule(method='GET', path='/api/v1/watchlist/{ts_code}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/ai/capabilities', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/ai/news/{stock_code}', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/ai/usage', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/ai/backtests/confirm', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/ai/backtests/prepare', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/ai/backtests/prepare/lookup', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/ai/interpretations/read', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/ai/requests', roles=('admin', 'researcher')),
    PermissionRule(method='POST', path='/api/v1/ai/requests/lookup', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/collaboration/me', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/collaboration/users', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/collaboration/audit', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='POST', path='/api/v1/collaboration/roles/prepare', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/collaboration/roles/commands', roles=('admin',)),
    PermissionRule(method='POST', path='/api/v1/collaboration/roles/lookup', roles=('admin',)),
    PermissionRule(method='GET', path='/api/v1/factors/results/{job_id}/report', roles=('admin', 'researcher', 'viewer')),
    PermissionRule(method='GET', path='/api/v1/experiments/template-results/{job_id}/report.html', roles=('admin', 'researcher', 'viewer')),
))
