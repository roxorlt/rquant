"""Offline C15 cases at the original PageControl/verified private entry boundary."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rquant.page_control import PageControlService

from rquant.collaboration_commands import (
    PageControlRoleAuthority,
    SetUserRoleCommand,
    SetUserRoleRequest,
)
from rquant.collaboration_roles import RoleEntry, RoleState

NOW = datetime(2026, 10, 6, 8, tzinfo=UTC)


def installed(tmp_path: Path) -> tuple[PageControlRoleAuthority, Path]:
    root = tmp_path / "private-owner"
    root.mkdir(mode=0o700)
    roles = root / "roles.json"
    state = RoleState.create(revision=1, users=(
        RoleEntry(username="admin", role="admin"),
        RoleEntry(username="alice", role="researcher"),
        RoleEntry(username="bob", role="viewer"),
    ))
    roles.write_text(state.model_dump_json())
    roles.chmod(0o600)
    return PageControlRoleAuthority(mode="enforced", roles_path=roles, clock=lambda: NOW), roles


def test_missing_authority_never_creates_first_admin(tmp_path: Path) -> None:
    authority = PageControlRoleAuthority(mode="enforced", roles_path=tmp_path / "roles.json")
    with pytest.raises(PermissionError):
        authority.current_role("visitor")
    assert not (tmp_path / "roles.json").exists()


@pytest.mark.parametrize("unsafe", ["symlink", "public", "hardlink"])
def test_private_roles_file_is_required(tmp_path: Path, unsafe: str) -> None:
    authority, roles = installed(tmp_path)
    if unsafe == "symlink":
        target = roles.with_name("original.json")
        roles.rename(target)
        roles.symlink_to(target)
    elif unsafe == "public":
        roles.chmod(0o644)
    else:
        os.link(roles, roles.with_name("second-link.json"))
    with pytest.raises(PermissionError):
        authority.current_role("admin")


def test_exact_policy_and_revoke_use_current_file(tmp_path: Path) -> None:
    authority, roles = installed(tmp_path)
    authority.require_operation("alice", "POST", "/api/v1/ai/requests")
    with pytest.raises(PermissionError):
        authority.require_operation("alice", "POST", "/api/v1/tasks/scheduling/commands")
    with pytest.raises(PermissionError):
        authority.require_operation("admin", "POST", "/api/v1/future/health-reset")
    state = authority.read_state()
    roles.write_text(RoleState.create(revision=2, users=tuple(
        RoleEntry(username=e.username, role="viewer" if e.username == "alice" else e.role)
        for e in state.users
    )).model_dump_json())
    with pytest.raises(PermissionError):
        authority.require_operation("alice", "POST", "/api/v1/ai/requests")


def test_token_cannot_be_typed_forged_or_used_after_revoke(tmp_path: Path) -> None:
    authority, roles = installed(tmp_path)
    body = {"kind": "submit_portfolio_backtest", "command_id": str(uuid4()), "requested_at": NOW.isoformat(), "spec": {"sealed": "original"}}
    proof = authority.issue_authorization("alice", body)
    assert authority.verify_authorization(proof, body).actor_id == "alice"
    altered = dict(body, command_id=str(uuid4()))
    with pytest.raises(PermissionError):
        authority.verify_authorization(proof, altered)
    forged = proof.model_copy(update={"actor_id": "admin"})
    with pytest.raises(PermissionError):
        authority.verify_authorization(forged, body)
    state = authority.read_state()
    roles.write_text(RoleState.create(revision=2, users=tuple(
        RoleEntry(username=e.username, role="viewer" if e.username == "alice" else e.role)
        for e in state.users
    )).model_dump_json())
    with pytest.raises(PermissionError):
        authority.verify_authorization(proof, body)


def test_two_step_role_change_cas_exact_target_and_last_admin(tmp_path: Path) -> None:
    authority, _ = installed(tmp_path)
    state = authority.read_state()
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()), requested_at=NOW,
        actor_id="admin", target_id="alice", new_role="viewer", expected_revision=state.revision,
        expected_state_sha256=state.content_sha256)
    issued = authority.prepare_role(request, authenticated_actor_id="admin")
    command = SetUserRoleCommand(**request.model_dump(), preparation=issued.preparation, entered_target="alice")
    with pytest.raises(PermissionError):
        authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof="0" * 64)
    after = authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof=issued.issuance_proof)
    assert after.revision == 2 and authority.current_role("alice") == "viewer"
    with pytest.raises(PermissionError):
        authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof=issued.issuance_proof)
    last = request.model_copy(update={"command_id": str(uuid4()), "target_id": "admin", "expected_revision": after.revision,
        "expected_state_sha256": after.content_sha256})
    with pytest.raises(PermissionError):
        authority.prepare_role(last, authenticated_actor_id="admin")


def test_expired_proof_and_legacy_c15_deny(tmp_path: Path) -> None:
    authority, roles = installed(tmp_path)
    body = {"kind": "submit_portfolio_backtest", "command_id": str(uuid4())}
    proof = authority.issue_authorization("alice", body)
    authority.clock = lambda: NOW + timedelta(minutes=2)
    with pytest.raises(PermissionError):
        authority.verify_authorization(proof, body)
    legacy = PageControlRoleAuthority(mode="legacy", roles_path=roles)
    with pytest.raises(PermissionError):
        legacy.current_role("admin")


def test_corrupt_role_json_is_fail_closed(tmp_path: Path) -> None:
    authority, roles = installed(tmp_path)
    body = json.loads(roles.read_text())
    roles.write_text(json.dumps(dict(body, revision=3)))
    with pytest.raises(PermissionError):
        authority.read_state()


def original_control(tmp_path: Path):
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    authority, roles = installed(tmp_path)
    outbox = PageControlOutbox(roles.parent / "page-control.sqlite3")
    outbox.path.chmod(0o600)
    consumer = PageControlConsumer(outbox=outbox, data_dir=tmp_path / "data", log_dir=tmp_path / "logs", clock=lambda: NOW)
    return PageControlService(outbox=outbox, consumer=consumer, collaboration=authority), authority


def reader_control(tmp_path: Path) -> tuple[PageControlService, PageControlRoleAuthority, RoleState]:
    control, authority = original_control(tmp_path)
    before = authority.read_state()
    initial = RoleState.create(revision=before.revision,
        users=before.users + (RoleEntry(username="reader", role="admin"),))
    assert authority.roles_path is not None
    authority.roles_path.write_text(initial.model_dump_json())
    return control, authority, initial


@pytest.mark.parametrize("operation,gap", [("me", 1), ("users", 2)])
def test_current_role_read_is_one_state_during_original_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str, gap: int,
) -> None:
    import threading

    from rquant.page_control import PageControlReceipt, PageControlStatus
    from rquant.web.models.collaboration import CollaborationMe, CollaborationPrivateRequest

    control, authority, initial = reader_control(tmp_path)
    command, proof = original_demotion(authority, "reader")
    trigger, started, finished = threading.Event(), threading.Event(), threading.Event()
    receipts: list[PageControlReceipt] = []
    errors: list[BaseException] = []
    actual_read = authority.read_state
    reads = 0

    def withdraw() -> None:
        try:
            assert trigger.wait(5), "read barrier was not reached"
            started.set()
            receipts.append(control.submit_role(command, authenticated_actor_id="admin", issuance_proof=proof))
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    writer = threading.Thread(target=withdraw, name="c15-original-role-writer", daemon=False)

    def pause_after_read() -> RoleState:
        nonlocal reads
        snapshot = actual_read()
        if threading.current_thread() is not writer:
            reads += 1
            if reads == gap:
                trigger.set()
                assert started.wait(5), "independent writer did not start"
                # The old unlocked gap lets the actual CAS finish; a held read
                # fence delays it. Both orders must return one authorized state.
                finished.wait(0.25)
        return snapshot

    monkeypatch.setattr(authority, "read_state", pause_after_read)
    writer.start()
    try:
        returned = control.collaboration_request(CollaborationPrivateRequest(
            schema_version=1, operation=operation, authenticated_actor_id="reader"))
    finally:
        trigger.set()
        writer.join(5)
        assert not writer.is_alive(), "original role writer remained active"
        assert not errors
        monkeypatch.setattr(authority, "read_state", actual_read)
    current = actual_read()
    assert receipts[0].status is PageControlStatus.SUCCEEDED
    assert control.outbox.effect(command.command_id).status.value == "succeeded"
    assert current.revision == initial.revision + 1
    assert authority.current_role("reader") == "viewer"
    assert control.lookup_role(command, authenticated_actor_id="admin") == receipts[0]
    if operation == "me":
        assert isinstance(returned, CollaborationMe)
        matching = {initial.content_sha256: initial, current.content_sha256: current}[returned.state_sha256]
        role = next(entry.role for entry in matching.users if entry.username == "reader")
        assert returned.revision == matching.revision
        assert returned.role == role
        assert returned.can_manage_users == (role == "admin")
        assert returned.can_research == (role in {"admin", "researcher"})
    else:
        assert isinstance(returned, RoleState)
        assert returned in (initial, current)
        assert next(entry.role for entry in returned.users if entry.username == "reader") == "admin"


@pytest.mark.parametrize("operation", ["me", "users"])
def test_completed_original_revocation_precedes_new_current_role_read(
    tmp_path: Path, operation: str,
) -> None:
    import threading

    from rquant.page_control import PageControlReceipt, PageControlStatus
    from rquant.web.models.collaboration import CollaborationMe, CollaborationPrivateRequest

    control, authority, initial = reader_control(tmp_path)
    command, proof = original_demotion(authority, "reader")
    receipts: list[PageControlReceipt] = []
    errors: list[BaseException] = []

    def withdraw() -> None:
        try:
            receipts.append(control.submit_role(command, authenticated_actor_id="admin", issuance_proof=proof))
        except BaseException as exc:
            errors.append(exc)

    writer = threading.Thread(target=withdraw, name="c15-revocation-first", daemon=False)
    writer.start()
    writer.join(5)
    assert not writer.is_alive() and not errors
    assert receipts[0].status is PageControlStatus.SUCCEEDED
    assert control.lookup_role(command, authenticated_actor_id="admin") == receipts[0]
    current = authority.read_state()
    assert current.revision == initial.revision + 1
    message = CollaborationPrivateRequest(schema_version=1, operation=operation, authenticated_actor_id="reader")
    if operation == "users":
        with pytest.raises(PermissionError):
            control.collaboration_request(message)
    else:
        returned = control.collaboration_request(message)
        assert isinstance(returned, CollaborationMe)
        assert returned.role == "viewer" and not returned.can_manage_users and not returned.can_research
        assert (returned.revision, returned.state_sha256) == (current.revision, current.content_sha256)


def test_actual_original_role_journal_and_full_uuid_lookup(tmp_path: Path) -> None:
    from rquant.page_control import PageControlStatus, parse_page_control_command
    control, authority = original_control(tmp_path)
    state = authority.read_state()
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()), requested_at=NOW,
        actor_id="admin", target_id="alice", new_role="viewer", expected_revision=state.revision,
        expected_state_sha256=state.content_sha256)
    prepared = authority.prepare_role(request, authenticated_actor_id="admin")
    command = SetUserRoleCommand(**request.model_dump(), preparation=prepared.preparation, entered_target="alice")
    with pytest.raises(ValueError):
        parse_page_control_command(command.model_dump(mode="json"))
    with pytest.raises(PermissionError):
        control.submit(command)
    receipt = control.submit_role(command, authenticated_actor_id="admin", issuance_proof=prepared.issuance_proof)
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert control.outbox.effect(command.command_id).status.value == "succeeded"
    assert authority.current_role("alice") == "viewer"
    assert control.lookup_role(command, authenticated_actor_id="admin") == receipt
    changed = command.model_copy(update={"entered_target": "bob"})
    with pytest.raises(PermissionError):
        control.lookup_role(changed, authenticated_actor_id="admin")


def test_actual_peer_request_audit_uses_journal_actor_not_worker(tmp_path: Path) -> None:
    from rquant.command_audit_projection import CommandAuditQuery
    from rquant.web.models.collaboration import CollaborationPrivateRequest
    control, authority = original_control(tmp_path)
    state = authority.read_state()
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()), requested_at=NOW,
        actor_id="admin", target_id="alice", new_role="viewer", expected_revision=state.revision,
        expected_state_sha256=state.content_sha256)
    issued = authority.prepare_role(request, authenticated_actor_id="admin")
    command = SetUserRoleCommand(**request.model_dump(), preparation=issued.preparation, entered_target="alice")
    control.submit_role(command, authenticated_actor_id="admin", issuance_proof=issued.issuance_proof)
    page = control.collaboration_request(CollaborationPrivateRequest(schema_version=1, operation="audit",
        authenticated_actor_id="admin", audit_query=CommandAuditQuery()))
    assert len(page.items) == 1 and page.items[0].actor_id == "admin"
    own = control.collaboration_request(CollaborationPrivateRequest(schema_version=1, operation="audit",
        authenticated_actor_id="bob", audit_query=CommandAuditQuery()))
    assert own.items == ()


def test_actual_app_constructs_new_routes_and_exact_policy_offline(tmp_path: Path) -> None:
    from rquant.collaboration_commands import COLLABORATION_POLICY
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    app = create_app(WebSettings(serving_root=tmp_path / "serving"), background=False)
    paths = app.openapi()["paths"]
    actual = {(method.upper(), path) for path, operations in paths.items() for method in operations}
    policy = {(entry.method, entry.path) for entry in COLLABORATION_POLICY.entries}
    assert actual <= policy
    assert ("POST", "/api/v1/collaboration/roles/commands") in actual
    assert ("GET", "/api/v1/factors/results/{job_id}/report") in actual
    assert ("GET", "/api/v1/experiments/template-results/{job_id}/report.html") in actual
    assert paths["/api/v1/collaboration/me"]["get"]["responses"]["200"]
    assert app.state.web.settings.collaboration_mode == "legacy"
    assert not (tmp_path / "serving").exists()


def test_applied_self_demotion_recovers_only_original_receipt_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.page_control import PageControlStatus
    control, authority = original_control(tmp_path)
    initial = authority.read_state()
    state = RoleState.create(revision=initial.revision,
        users=initial.users + (RoleEntry(username="second-admin", role="admin"),))
    authority.roles_path.write_text(state.model_dump_json())
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()),
        requested_at=NOW, actor_id="admin", target_id="admin", new_role="viewer",
        expected_revision=state.revision, expected_state_sha256=state.content_sha256)
    issued = authority.prepare_role(request, authenticated_actor_id="admin")
    command = SetUserRoleCommand(**request.model_dump(), preparation=issued.preparation, entered_target="admin")
    write = authority._write_locked
    writes = 0

    def crash_after_write(directory: int, after: RoleState) -> None:
        nonlocal writes
        writes += 1
        write(directory, after)
        raise KeyboardInterrupt("synthetic crash after role CAS, before effect receipt")

    monkeypatch.setattr(authority, "_write_locked", crash_after_write)
    with pytest.raises(KeyboardInterrupt):
        control.submit_role(command, authenticated_actor_id="admin", issuance_proof=issued.issuance_proof)
    raw = authority.roles_path.read_bytes()
    assert authority.current_role("admin") == "viewer"
    assert control.outbox.effect(command.command_id).status.value == "started"
    control.consumer.clock = lambda: NOW + timedelta(seconds=31)
    recovered = control.consumer.drain(limit=1)
    assert len(recovered) == 1 and recovered[0].status is PageControlStatus.SUCCEEDED
    assert recovered[0].result["role"] == "viewer"
    assert writes == 1 and authority.roles_path.read_bytes() == raw
    with pytest.raises(PermissionError):
        control.lookup_role(command, authenticated_actor_id="admin")


def test_another_private_journal_metadata_cannot_attest_current_actor(tmp_path: Path) -> None:
    import sqlite3
    left, right = tmp_path / "left", tmp_path / "right"
    left.mkdir(mode=0o700)
    right.mkdir(mode=0o700)
    control, authority = original_control(left)
    target, _ = original_control(right)
    state = authority.read_state()
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()),
        requested_at=NOW, actor_id="admin", target_id="alice", new_role="viewer",
        expected_revision=state.revision, expected_state_sha256=state.content_sha256)
    issued = authority.prepare_role(request, authenticated_actor_id="admin")
    command = SetUserRoleCommand(**request.model_dump(), preparation=issued.preparation, entered_target="alice")
    control.submit_role(command, authenticated_actor_id="admin", issuance_proof=issued.issuance_proof)
    with sqlite3.connect(control.outbox.path) as source:
        source.row_factory = sqlite3.Row
        row = source.execute("SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)).fetchone()
    with sqlite3.connect(target.outbox.path) as destination:
        destination.execute("INSERT INTO page_control_command (" + ",".join(row.keys()) + ") VALUES (" +
            ",".join("?" for _ in row.keys()) + ")", tuple(row))
    with pytest.raises(PermissionError):
        target.outbox.trusted_command_actor(command.command_id, row["command_hash"])


def test_real_asgi_identity_current_roles_exact_routes_and_original_csrf(tmp_path: Path) -> None:
    import asyncio

    import httpx

    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.portfolio_backtest_artifact import PortfolioResultReader
    from rquant.web.app import create_app
    from rquant.web.collaboration_gateway import CollaborationGateway
    from rquant.web.models.backtests import PortfolioSourceOption
    from rquant.web.portfolio_backtest_service import PortfolioWebService
    from rquant.web.settings import WebSettings
    from tests.unit.test_backtest_platform import config

    control, _ = original_control(tmp_path)
    proof = tmp_path / "synthetic-proxy-proof"
    proof.write_text("a" * 64)
    proof.chmod(0o400)
    private_errors: list[str] = []

    def private(message: object) -> bytes:
        try:
            return control.collaboration_request(message).model_dump_json().encode()
        except Exception as exc:
            private_errors.append(str(exc))
            raise

    gateway = CollaborationGateway(Path("/private/tmp") / ("c15-unused-" + uuid4().hex + ".sock"),
        expected_service_uid=os.geteuid() + 1, shared_gid=os.getegid(),
        transport=private)
    settings = WebSettings(serving_root=tmp_path / "unused-serving", collaboration_mode="enforced",
        ingress_socket_path=Path("/private/tmp") / ("c15-web-unused-" + uuid4().hex + ".sock"), proxy_proof_file=proof,
        lab_control_users=("admin", "alice", "bob"))
    jobs = LabJobStore(tmp_path / "portfolio.sqlite")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    original_config = config()
    portfolio = PortfolioWebService(reader=reader, results=PortfolioResultReader(reader=reader, artifact_root=tmp_path / "artifacts"),
        sources=(PortfolioSourceOption(key=original_config.source_key, version=original_config.source_version,
            label="原完整行情", start_date=original_config.start_date, end_date=original_config.end_date, updated_at=NOW,
            ranking_available=False, industry_available=False, opening_verified=True),),
        default_config=original_config, preparation_available=True)
    app = create_app(settings, background=False, collaboration_gateway=gateway, portfolio_backtests=portfolio)

    @app.get("/api/v1/future/owner-change")
    def future() -> dict[str, bool]:
        return {"called": True}

    def headers(actor: str, *, csrf: bool = True) -> dict[str, str]:
        value = {"x-rquant-user": actor, "x-rquant-proxy-proof": "a" * 64,
            "Content-Type": "application/json"}
        if csrf:
            value["x-rquant-csrf"] = "1"
        return value

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://offline.test") as client:
            unproved = await client.get("/api/v1/collaboration/me", headers={"x-rquant-user": "admin"})
            assert unproved.status_code == 401
            current = await client.get("/api/v1/collaboration/me", headers=headers("bob"))
            assert current.status_code == 200, current.json()
            assert current.json()["data"]["role"] == "viewer"
            assert (await client.get("/api/v1/backtests/portfolio/capabilities", headers=headers("alice"))).json()["data"]["can_run"] is True
            assert (await client.get("/api/v1/backtests/portfolio/capabilities", headers=headers("bob"))).json()["data"]["can_run"] is False
            assert (await client.get("/api/v1/collaboration/users", headers=headers("bob"))).status_code == 403
            assert (await client.get("/api/v1/future/owner-change", headers=headers("admin"))).status_code == 403
            assert (await client.post("/api/v1/tasks/scheduling/commands", json={}, headers=headers("alice"))).status_code == 403
            assert (await client.post("/api/v1/ai/requests", json={}, headers=headers("bob"))).status_code == 403
            assert (await client.post("/api/v1/ai/requests/lookup", json={}, headers=headers("alice", csrf=False))).status_code == 403
            users = await client.get("/api/v1/collaboration/users", headers=headers("admin"))
            assert users.status_code == 200
            state = RoleState.model_validate_json(json.dumps(users.json()["data"]))
            request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()),
                requested_at=NOW, actor_id="admin", target_id="alice", new_role="viewer",
                expected_revision=state.revision, expected_state_sha256=state.content_sha256)
            prepared = await client.post("/api/v1/collaboration/roles/prepare",
                json=request.model_dump(mode="json"), headers=headers("admin"))
            assert prepared.status_code == 200, prepared.json()
            from rquant.collaboration_commands import IssuedRolePreparation
            issued = IssuedRolePreparation.model_validate_json(json.dumps(prepared.json()["data"]))
            command = SetUserRoleCommand(**request.model_dump(), preparation=issued.preparation, entered_target="alice")
            from rquant.web.models.collaboration import CollaborationRoleSubmit
            submitted = await client.post("/api/v1/collaboration/roles/commands", headers=headers("admin"),
                json=CollaborationRoleSubmit(command=command, issuance_proof=issued.issuance_proof).model_dump(mode="json"))
            assert submitted.status_code == 200, {"response": submitted.json(), "private_errors": private_errors}
            assert submitted.json()["data"]["status"] == "succeeded"
            withdrawn = await client.get("/api/v1/collaboration/me", headers=headers("alice"))
            assert withdrawn.status_code == 200 and withdrawn.json()["data"]["role"] == "viewer"
            assert (await client.post("/api/v1/ai/requests", json={}, headers=headers("alice"))).status_code == 403
            admin_audit = await client.get("/api/v1/collaboration/audit", headers=headers("admin"))
            assert admin_audit.status_code == 200, {"response": admin_audit.json(), "private_errors": private_errors}
            assert admin_audit.json()["data"]["items"][0]["actor_id"] == "admin"
            own_audit = await client.get("/api/v1/collaboration/audit", headers=headers("bob"))
            assert own_audit.status_code == 200 and own_audit.json()["data"]["items"] == []
    asyncio.run(scenario())


@pytest.mark.parametrize("additions", [False, True])
def test_original_readonly_source_accepts_exact_legacy_or_typed_additive_journal(
    tmp_path: Path, additions: bool,
) -> None:
    import sqlite3

    from rquant.serving_page_projection_source import (
        PageProjectionSourceIntegrityError,
        _ReadonlyPageControlAuditReader,
    )
    control, _ = original_control(tmp_path)
    if not additions:
        with sqlite3.connect(control.outbox.path) as connection:
            connection.execute("ALTER TABLE page_control_command DROP COLUMN authorization_json")
            connection.execute("ALTER TABLE page_control_effect DROP COLUMN original_admission_json")
    readonly = _ReadonlyPageControlAuditReader(control.outbox.path)
    assert readonly.path == control.outbox.path
    with sqlite3.connect(control.outbox.path) as connection:
        connection.execute("ALTER TABLE page_control_command ADD COLUMN unknown_authority TEXT")
    with pytest.raises(PageProjectionSourceIntegrityError):
        _ReadonlyPageControlAuditReader(control.outbox.path)


def test_original_readonly_journal_publishes_bound_roles_and_audit(tmp_path: Path) -> None:
    from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
    control, authority = original_control(tmp_path)
    state = authority.read_state()
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()), requested_at=NOW,
        actor_id="admin", target_id="alice", new_role="viewer", expected_revision=state.revision,
        expected_state_sha256=state.content_sha256)
    issued = authority.prepare_role(request, authenticated_actor_id="admin")
    command = SetUserRoleCommand(**request.model_dump(), preparation=issued.preparation, entered_target="alice")
    control.submit_role(command, authenticated_actor_id="admin", issuance_proof=issued.issuance_proof)
    readonly = _ReadonlyPageControlAuditReader(control.outbox.path)
    observed = max(NOW, datetime.now(UTC)) + timedelta(seconds=1)
    projections = readonly.collaboration_projections(authority, observed_at=observed)
    tables = {item.table_name: item for item in projections}
    assert set(tables) == {"collaboration_role", "command_audit", "command_audit_window"}
    assert {row["username"]: row["role"] for row in tables["collaboration_role"].rows}["alice"] == "viewer"
    assert tables["command_audit"].rows[0]["actor_id"] == "admin"
    assert tables["command_audit"].rows[0]["outcome"] == "processed"
    assert tables["command_audit_window"].rows[0]["role_state_sha256"] == authority.read_state().content_sha256
    other_root = tmp_path / "other"
    other_root.mkdir()
    other, unrelated = original_control(other_root)
    with pytest.raises(PermissionError):
        readonly.collaboration_projections(unrelated, observed_at=observed)
    assert other.outbox.path != control.outbox.path
    from rquant.serving_read_models import ServingProjectionInput, ServingReadModelInput
    inputs = tuple(ServingProjectionInput.bind(item, owner_dataset_id="lab_jobs", owner_generation_id="b" * 64)
        for item in projections)
    ServingReadModelInput(observed_at=observed, projections=inputs)
    with pytest.raises(ValueError, match="partial"):
        ServingReadModelInput(observed_at=observed, projections=inputs[:-1])
    window = inputs[1]
    altered = window.model_copy(update={"rows": ({**window.rows[0], "role_state_sha256": "0" * 64},)})
    with pytest.raises(ValueError, match="role state differ"):
        ServingReadModelInput(observed_at=observed, projections=(inputs[0], altered, inputs[2]))


def test_original_lab_publisher_uses_same_private_journal_for_role_graph(tmp_path: Path) -> None:
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
    from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
    control, authority = original_control(tmp_path)
    jobs = LabJobStore(tmp_path / "lab.sqlite")
    jobs.initialize()
    reader = _ReadonlyPageControlAuditReader(control.outbox.path)
    source = LabJobsServingSourceReader(reader=LabJobReader(jobs.path),
        collaboration_audit_reader=reader, collaboration=authority)
    before = control.outbox.path.read_bytes()
    observed = max(NOW, datetime.now(UTC)) + timedelta(seconds=1)
    first = source(observed)
    assert first == source(observed)
    tables = {item.table_name: item for item in first.payload.projections}
    assert tables["sealed_result_owner"].rows == ()
    assert tables["collaboration_role"].rows[1]["username"] == "alice"
    assert first.payload.lab_jobs == ()
    assert control.outbox.path.read_bytes() == before
    with pytest.raises(PermissionError):
        LabJobsServingSourceReader(reader=LabJobReader(jobs.path), collaboration=authority)


def test_original_private_listener_empty_editors_require_explicit_enforced_roles(tmp_path: Path) -> None:
    from rquant.factor_definition_admission import (
        FactorDefinitionAdmission,
        build_factor_definition_admission_server,
    )
    from rquant.page_control import PageControlService
    control, _authority = original_control(tmp_path)
    admission = FactorDefinitionAdmission(control, editor_users=frozenset())
    # Only exercise the original pre-bind UID rejection here; Root runs actual UDS.
    with pytest.raises(ValueError, match="distinct trusted Web UID"):
        build_factor_definition_admission_server(admission,
            socket_path=Path("/private/tmp") / ("c15-roles-" + uuid4().hex + ".sock"),
            trusted_web_uid=os.geteuid(), shared_gid=os.getegid())
    legacy = PageControlService(outbox=control.outbox, consumer=control.consumer)
    assert build_factor_definition_admission_server(
        FactorDefinitionAdmission(legacy, editor_users=frozenset()),
        socket_path=None, trusted_web_uid=None, shared_gid=None) is None


def test_original_private_versions_reject_boolean_alias(tmp_path: Path) -> None:
    from rquant.collaboration_commands import CommandAuthorization
    from rquant.web.models.collaboration import CollaborationPrivateRequest

    control, authority = original_control(tmp_path)
    from rquant.page_control import SaveUserPool
    command = SaveUserPool(command_id=str(uuid4()), requested_at=NOW,
        base_name="my_pool")
    proof = authority.issue_authorization("alice", command.model_dump(mode="json"))
    with pytest.raises(ValueError):
        CommandAuthorization.model_validate(proof.model_dump() | {"schema_version": True})
    with pytest.raises(ValueError):
        CollaborationPrivateRequest(schema_version=True, operation="me", authenticated_actor_id="alice")
    assert control.outbox.audit(command.command_id) is None


def test_original_lab_publisher_settings_require_paired_private_sources(tmp_path: Path) -> None:
    from rquant.runtime_builder_authority import LabJobsPublisherSettings

    old = LabJobsPublisherSettings(lab_jobs_path=tmp_path / "jobs.sqlite", authority_root=tmp_path / "authority")
    assert "collaboration_outbox_path" not in old.model_dump(mode="json")
    paired = LabJobsPublisherSettings(lab_jobs_path=old.lab_jobs_path, authority_root=old.authority_root,
        collaboration_outbox_path=tmp_path / "private" / "control.sqlite",
        collaboration_roles_path=tmp_path / "private" / "roles.json")
    assert paired.collaboration_roles_path.name == "roles.json"
    with pytest.raises(ValueError):
        LabJobsPublisherSettings(lab_jobs_path=old.lab_jobs_path, authority_root=old.authority_root,
            collaboration_outbox_path=paired.collaboration_outbox_path)
    with pytest.raises(ValueError):
        LabJobsPublisherSettings(lab_jobs_path=old.lab_jobs_path, authority_root=old.authority_root,
            collaboration_outbox_path=paired.collaboration_outbox_path,
            collaboration_roles_path=tmp_path / "other" / "roles.json")


@pytest.mark.parametrize("prior", ["unknown", "other", "same"])
def test_original_factor_dedup_cannot_adopt_prior_owner(tmp_path: Path, prior: str) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        save_factor_run_configuration,
    )
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from rquant.screen.query_history import prepare_private_screen_outbox
    from rquant.web.models.collaboration import ResultOwnerQuery
    from tests.unit.test_factor_run_configuration import _configured

    factor_root = tmp_path / "factor"
    factor_root.mkdir(mode=0o700)
    root, reference, request = _configured(factor_root)
    with open_factor_run_configuration(root, reference) as loaded:
        config = loaded.configuration
    reference = save_factor_run_configuration(root, config.model_copy(update={"factor_run_users": ("alice", "bob")}))
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: NOW)
    authority, roles = installed(tmp_path)
    state = authority.read_state()
    roles.write_text(RoleState.create(revision=2, users=tuple(
        RoleEntry(username=item.username, role="researcher" if item.username == "bob" else item.role)
        for item in state.users)).model_dump_json())
    path = roles.parent / "control.sqlite"
    prepare_private_screen_outbox(path)
    outbox = PageControlOutbox(path)
    control = PageControlService(outbox=outbox, consumer=PageControlConsumer(outbox=outbox,
        data_dir=tmp_path / "data", log_dir=tmp_path / "log", clock=lambda: NOW, factor_run_backend=backend),
        collaboration=authority)
    request = request.model_copy(update={"requested_at": NOW})
    instance = config.registry_identity.instance_id
    if prior == "unknown":
        plan = backend.compile(request, verified_registry_instance_id=instance)
        with open_factor_run_configuration(root, reference) as loaded:
            first = loaded.open_ledger(clock=lambda: NOW).submit(str(uuid4()), plan.spec)
        first_job_id, first_spec = first.job_id, first.spec_sha256
    else:
        receipt = control._submit_trusted_factor_run(request,
            authenticated_actor_id="alice" if prior == "same" else "bob", verified_registry_instance_id=instance)
        assert receipt.status.value == "succeeded", receipt
        first_job_id, first_spec = receipt.result["job_id"], receipt.result["spec_sha256"]
    duplicate = request.model_copy(update={"command_id": str(uuid4())})
    from rquant.factor.result_serving import (
        FactorResultProjectionReader,
        validate_factor_result_projections,
    )
    from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
    reader = FactorResultProjectionReader(config.ledger_identity, config.artifact_root,
        collaboration_audit_reader=_ReadonlyPageControlAuditReader(outbox.path), collaboration=authority)
    projected = reader(max(NOW, datetime.now(UTC)) + timedelta(seconds=1), other_projections=())
    snapshot = validate_factor_result_projections({item.table_name: item for item in projected})
    assert len(snapshot.index) == (0 if prior == "unknown" else 1)
    if prior == "same":
        receipt = control._submit_trusted_factor_run(duplicate, authenticated_actor_id="alice",
            verified_registry_instance_id=instance)
        assert receipt.status.value == "succeeded" and receipt.result["job_id"] == first_job_id
        proof = control._trusted_result_owner(ResultOwnerQuery(domain="factor", job_id=first_job_id,
            spec_hash=first_spec), authenticated_actor_id="alice")
        assert proof.command_id == request.command_id
    else:
        with pytest.raises(PermissionError):
            control._submit_trusted_factor_run(duplicate, authenticated_actor_id="alice",
                verified_registry_instance_id=instance)
        with open_factor_run_configuration(root, reference) as loaded:
            assert not loaded.open_ledger(clock=lambda: NOW).command_exists(duplicate.command_id)
        with pytest.raises(LookupError):
            control._trusted_result_owner(ResultOwnerQuery(domain="factor", job_id=first_job_id,
                spec_hash=first_spec), authenticated_actor_id="alice")


def test_original_unknown_lab_target_is_rejected_before_new_effect(tmp_path: Path) -> None:
    from rquant.page_control import ExportLabArtifactZip

    control, authority = original_control(tmp_path)
    command = ExportLabArtifactZip(command_id=str(uuid4()), requested_at=NOW, job_id=uuid4())
    proof = authority.issue_authorization("alice", command.model_dump(mode="json"))
    receipt = control.submit_authorized(command, proof)
    assert receipt.status.value == "failed"
    assert control.outbox.effect(command.command_id) is None


def test_original_body_lookup_rejects_capacity_before_transfer(tmp_path: Path) -> None:
    import tracemalloc

    from rquant.page_control import SaveUserPool
    control, authority = original_control(tmp_path)
    command = SaveUserPool(command_id=str(uuid4()), requested_at=NOW, base_name="bounded_pool")
    control.submit_authorized(command, authority.issue_authorization("alice", command.model_dump(mode="json")))
    with control.outbox._connect() as connection:
        connection.execute("UPDATE page_control_command SET payload_json=? WHERE command_id=?",
            ('"' + 'a' * (2 * 1024 * 1024) + '"', command.command_id))
    tracemalloc.start()
    try:
        with pytest.raises(ValueError, match="capacity"):
            control.outbox.original_command_bytes(command.command_id)
        assert tracemalloc.get_traced_memory()[1] < 1024 * 1024
    finally:
        tracemalloc.stop()


def install_original_ai_roles(outbox: object, *, clock: Callable[[], datetime]) -> PageControlRoleAuthority:
    from rquant.page_control import PageControlOutbox
    assert type(outbox) is PageControlOutbox
    outbox.path.chmod(0o600)
    outbox.path.parent.chmod(0o700)
    roles = outbox.path.parent / "roles.json"
    roles.write_text(RoleState.create(revision=1, users=(
        RoleEntry(username="admin", role="admin"), RoleEntry(username="researcher", role="researcher"),
        RoleEntry(username="alice", role="researcher"), RoleEntry(username="other", role="researcher"),
    )).model_dump_json())
    roles.chmod(0o600)
    authority = PageControlRoleAuthority(mode="enforced", roles_path=roles, clock=clock)
    authority.bind_outbox(outbox.path)
    outbox.collaboration = authority
    return authority


def original_demotion(authority: PageControlRoleAuthority, actor: str) -> tuple[SetUserRoleCommand, str]:
    state = authority.read_state()
    request = SetUserRoleRequest(schema_version=1, kind="set_user_role", command_id=str(uuid4()),
        requested_at=authority.clock(), actor_id="admin", target_id=actor, new_role="viewer",
        expected_revision=state.revision, expected_state_sha256=state.content_sha256)
    prepared = authority.prepare_role(request, authenticated_actor_id="admin")
    return SetUserRoleCommand(**request.model_dump(), preparation=prepared.preparation, entered_target=actor), prepared.issuance_proof


def test_actual_ai_reserve_linearizes_with_original_admin_revocation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import threading

    import httpx

    from tests.unit.test_ai_assistance import _owner, _response
    calls = []
    owner, request, adapter = _owner(tmp_path, lambda body: (calls.append(body), httpx.Response(200, json=_response()))[1])
    authority = install_original_ai_roles(owner.outbox, clock=owner.clock)
    command, proof = original_demotion(authority, "researcher")
    completed = threading.Event()
    started = threading.Event()
    errors: list[BaseException] = []
    def withdraw() -> None:
        started.set()
        try:
            authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof=proof)
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()
    thread = threading.Thread(target=withdraw)
    reserve = owner.outbox.ai_usage_reserve
    def concurrent_reserve(*args: object, **kwargs: object) -> object:
        thread.start()
        assert started.wait(5)
        assert not completed.wait(0.25), "current role changed during original budget commit"
        return reserve(*args, **kwargs)
    monkeypatch.setattr(owner.outbox, "ai_usage_reserve", concurrent_reserve)
    def gate() -> None:
        if thread.ident is not None:
            thread.join(5)
            assert completed.is_set() and not errors
    try:
        with pytest.raises(PermissionError):
            owner.generate("researcher", request, submission_gate=gate)
        assert calls == []
        assert owner.outbox.ai_usage_lookup("researcher", request.request_id).state == "reserved"
    finally:
        if thread.ident is not None:
            thread.join(5)
        adapter.close()
    assert not thread.is_alive()


def test_actual_ai_revocation_during_paid_call_keeps_usage_without_new_result(tmp_path: Path) -> None:
    import httpx

    from tests.unit.test_ai_assistance import _owner, _response
    holder = []
    def reply(body: object) -> object:
        authority, command, proof = holder
        authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof=proof)
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 8, "completion_tokens": 3},
            arguments=json.dumps({"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]})))
    owner, request, adapter = _owner(tmp_path, reply)
    authority = install_original_ai_roles(owner.outbox, clock=owner.clock)
    holder.extend((authority, *original_demotion(authority, "researcher")))
    try:
        view = owner.generate("researcher", request)
        record = owner.outbox.ai_usage_lookup("researcher", request.request_id)
        assert view.state == "completed" and view.result is None
        assert record.usage.total_tokens == 11 and record.error_code == "permission_changed"
        assert authority.current_role("researcher") == "viewer"
        assert owner.generate("researcher", request) == view
    finally:
        adapter.close()


def test_original_consumer_fence_carries_journal_actor_and_cleans_context(tmp_path: Path) -> None:
    from rquant.page_control import _TRUSTED_COLLABORATION_ACTOR, SaveUserPool
    control, authority = original_control(tmp_path)
    command = SaveUserPool(command_id=str(uuid4()), requested_at=NOW, base_name="bounded_pool")
    control.submit_authorized(command, authority.issue_authorization("alice", command.model_dump(mode="json")))
    assert _TRUSTED_COLLABORATION_ACTOR.get() is None
    with control.outbox.command_fence(command):
        assert _TRUSTED_COLLABORATION_ACTOR.get() == "alice"
    assert _TRUSTED_COLLABORATION_ACTOR.get() is None


@pytest.mark.parametrize("operation", ["facts", "digest"])
def test_original_news_persistence_linearizes_with_current_role(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str) -> None:
    import threading

    from rquant.stock_news_digest import validate_stock_news_digest
    from rquant.stock_news_sources import StockNewsArtifactStore
    from tests.unit.test_stock_news_digest import collection, draft
    control, authority = original_control(tmp_path)
    store = StockNewsArtifactStore(tmp_path / "news", outbox=control.outbox)
    facts = collection()
    if operation == "digest":
        store.put_facts(facts)
    command, proof = original_demotion(authority, "alice")
    started = threading.Event()
    completed = threading.Event()
    errors: list[BaseException] = []
    def withdraw() -> None:
        started.set()
        try:
            authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof=proof)
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()
    thread = threading.Thread(target=withdraw)
    put = store._put
    def concurrent_put(*args: object, **kwargs: object) -> object:
        thread.start()
        assert started.wait(5)
        assert not completed.wait(0.25), "current role changed during original news file/DB commit"
        return put(*args, **kwargs)
    monkeypatch.setattr(store, "_put", concurrent_put)
    try:
        if operation == "facts":
            store.put_facts(facts)
        else:
            store.put_digest(facts, validate_stock_news_digest(facts, draft=draft(facts)), model_id="gpt-test", template_version="v1")
    finally:
        if thread.ident is not None:
            thread.join(5)
    assert completed.is_set() and not thread.is_alive() and not errors
    assert authority.current_role("alice") == "viewer"
    assert store.read_facts("alice", facts.stock_code) == facts


def test_original_prepared_source_requires_current_original_submit_actor(tmp_path: Path) -> None:
    from rquant.ai_screen_backtest_source import (
        AIHistoricalScreenSource,
        AIScreenBacktestArtifacts,
        AIScreenBacktestPipeline,
        build_original_portfolio_writer,
    )
    from rquant.page_control import PageControlService
    from rquant.portfolio_backtest_commands import SubmitPortfolioBacktest
    from rquant.web.models.ai_assistance import AIBacktestConfirmRequest, AIBacktestPrepareRequest
    from tests.support.ai_assistance_fixture import build_portfolio_foundation
    from tests.unit.test_ai_screen_backtest_source import prepared_history_fixture
    screen, base, config, previous, history, original, now = prepared_history_fixture(tmp_path)
    authority = install_original_ai_roles(history.outbox, clock=lambda: now)
    control = PageControlService(outbox=history.outbox, consumer=previous.consumer, collaboration=authority)
    foundation = build_portfolio_foundation(tmp_path / "lab", base, clock=lambda: now)
    pipeline = AIScreenBacktestPipeline(history=history,
        source=AIHistoricalScreenSource(screen=screen, base=base, default_config=config),
        artifacts=AIScreenBacktestArtifacts(tmp_path / "private" / "prepared"), clock=lambda: now)
    control.consumer.portfolio_backend = build_original_portfolio_writer(pipeline=pipeline,
        commands=foundation.commands, metadata_path=foundation.metadata_path, catalog_path=foundation.catalog_path,
        lake_root=foundation.lake_root, input_root=foundation.input_root, protocol=foundation.protocol,
        code_commit=foundation.code_commit, clock=lambda: now)
    view = pipeline.prepare("researcher", AIBacktestPrepareRequest(request_id=uuid4(), execution_id=original.command_id,
        start_date=config.start_date, end_date=config.end_date))
    with pytest.raises(PermissionError):
        pipeline.provider(view.config.source_key, view.config.source_version)
    other = SubmitPortfolioBacktest(command_id=str(uuid4()), requested_at=now, actor_id="other", config=view.config)
    receipt = control.submit_authorized(other, authority.issue_authorization("other", other.model_dump(mode="json")))
    assert receipt.status.value == "failed" and foundation.commands.spool.pending() == ()
    request = AIBacktestConfirmRequest(command_id=uuid4(), requested_at=now, prepared_request_id=view.request_id,
        config_sha256=view.config_sha256, proof_sha256=view.proof_sha256)
    result = pipeline.confirm("researcher", request, control=control)
    assert result.receipt.status.value == "succeeded", result
    assert len(foundation.commands.spool.pending()) == 1
    assert control.outbox.authorize_ai_portfolio_result("researcher", result.job_id).config == view.config
    with control.outbox._connect() as connection:
        connection.execute("UPDATE page_control_command SET authorization_json=NULL WHERE command_id=?", (str(request.command_id),))
    with pytest.raises(PermissionError):
        control.outbox.authorize_ai_portfolio_result("researcher", result.job_id)


def test_original_audit_generation_rejects_large_actor_metadata_before_transfer(tmp_path: Path) -> None:
    import tracemalloc

    from rquant.command_audit_projection import CommandAuditQuery
    from rquant.page_control import SaveUserPool
    from rquant.web.models.collaboration import CollaborationPrivateRequest
    control, authority = original_control(tmp_path)
    command = SaveUserPool(command_id=str(uuid4()), requested_at=NOW, base_name="bounded_pool")
    control.submit_authorized(command, authority.issue_authorization("alice", command.model_dump(mode="json")))
    with control.outbox._connect() as connection:
        connection.execute("UPDATE page_control_command SET authorization_json=? WHERE command_id=?",
            ('"' + 'a' * (2 * 1024 * 1024) + '"', command.command_id))
    message = CollaborationPrivateRequest(schema_version=1, operation="audit", authenticated_actor_id="admin",
        audit_query=CommandAuditQuery())
    tracemalloc.start()
    try:
        with pytest.raises(ValueError, match="capacity"):
            control.collaboration_request(message)
        assert tracemalloc.get_traced_memory()[1] < 1024 * 1024
    finally:
        tracemalloc.stop()


def test_original_nightly_stops_new_source_calls_after_current_role_revocation(tmp_path: Path) -> None:
    from datetime import date

    import httpx

    from rquant.stock_news_sources import (
        AINightlyNewsCommand,
        AINightlyNewsRunner,
        EastmoneyStockNewsCollector,
        StockNewsArtifactStore,
        StockNewsCompany,
        StockNewsHttpTransport,
        news_research_scope,
    )
    from tests.unit.test_ai_assistance import _owner
    from tests.unit.test_stock_news_sources import observer, scheduling_gate
    owner, _, adapter = _owner(tmp_path, lambda body: (_ for _ in ()).throw(AssertionError("no model call")))
    owner.clock = lambda: NOW
    authority = install_original_ai_roles(owner.outbox, clock=owner.clock)
    command, proof = original_demotion(authority, "researcher")
    owner.contexts.news = StockNewsArtifactStore(tmp_path / "private-news", outbox=owner.outbox)
    calls = []
    def reply(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            authority.confirm_role(command, authenticated_actor_id="admin", issuance_proof=proof)
        if request.url.path == "/api/security/ann":
            return httpx.Response(200, json={"success": 1, "data": {"list": [], "total_hits": 0}})
        if request.url.path == "/report/list":
            return httpx.Response(200, json={"data": [], "TotalPage": 0})
        return httpx.Response(200, content=(request.url.params["cb"] + '({"result":{"cmsArticleWebOld":[]},"hitsTotal":0});').encode())
    quota = observer(tmp_path)
    transport = StockNewsHttpTransport(observer=quota, transport=httpx.MockTransport(reply))
    gate, _, _, _ = scheduling_gate(tmp_path)
    collector = EastmoneyStockNewsCollector(transport, page_limit=1, clock=owner.clock)
    runner = AINightlyNewsRunner(owner, collector, gate, users=frozenset({"researcher"}), enabled=True)
    scope = news_research_scope("researcher", pool_members=("600001.SH",), watchlist_members=(), pool_version="a" * 64, watchlist_version="b" * 64)
    original = AINightlyNewsCommand(request_id=uuid4(), scope=scope,
        companies=(StockNewsCompany(stock_code="600001.SH", company_name="样本公司", source_sha256="c" * 64),),
        start_date=date(2026, 10, 1), end_date=NOW.date())
    runner.journal.reserve(original)
    try:
        progress = runner.run_original("researcher", original.request_id)
        assert len(calls) == 1
        assert progress.pending_codes == ("600001.SH",)
        assert runner.journal.phase("researcher", original.request_id, "600001.SH") == "unknown"
        assert owner.outbox.ai_usage_account_calls("shared", NOW.date()) == 0
    finally:
        runner.close()
        adapter.close()
