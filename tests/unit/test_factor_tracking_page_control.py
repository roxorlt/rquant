"""Tracking toggles retain the original PageControl effect and permissions."""

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest


def _control(tmp_path: Path) -> tuple[object, ...]:
    from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from tests.unit.test_factor_run_configuration import _configured

    root, reference, run = _configured(tmp_path)
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    identity = store.initialize()
    backend = FactorTrackingPageControlBackend(
        root, reference, identity, enabled=True, tracking_users=frozenset({"alice"})
    )
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path,
        log_dir=tmp_path,
        factor_tracking_backend=backend,
        clock=lambda: run.requested_at,
    )
    service = PageControlService(outbox=outbox, consumer=consumer)
    body = FactorTrackingRequest(
        command_id=str(uuid4()),
        requested_at=run.requested_at,
        serving_generation_id=run.serving_generation_id,
        factor_id=run.parameters.factor_id,
        tracked=True,
        expected_head=run.parameters.expected_head,
    )
    return service, backend, outbox, store, identity, body


def test_tracking_owned_setter_and_original_lookup_do_not_recompile(tmp_path: Path) -> None:
    service, backend, _, store, identity, body = _control(tmp_path)
    receipt = service._submit_trusted_factor_tracking(
        body,
        authenticated_actor_id="alice",
        verified_registry_instance_id=backend.registry_identity.instance_id,
    )
    assert receipt.status.value == "succeeded" and receipt.result["tracked"]
    backend.reference = backend.reference.model_copy(update={"sha256": "0" * 64})
    restored = service._resume_trusted_factor_tracking(body, authenticated_actor_id="alice")
    assert restored.result == receipt.result
    assert (
        store.get(body.factor_id, expected_identity=identity).generation
        == receipt.result["tracking_generation"]
    )
    with pytest.raises(PermissionError):
        service._resume_trusted_factor_tracking(body, authenticated_actor_id="bob")
    with pytest.raises(ValueError):
        service._resume_trusted_factor_tracking(
            body.model_copy(update={"tracked": False}), authenticated_actor_id="alice"
        )


def test_tracking_effect_lost_after_state_commit_recovers_original_and_preserves_other_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.page_control import SaveCanvas

    service, backend, outbox, store, identity, body = _control(tmp_path)
    outbox.enqueue(SaveCanvas(command_id="unrelated", requested_at=body.requested_at, name="待办"))
    finish = outbox.finish_effect
    monkeypatch.setattr(
        outbox,
        "finish_effect",
        lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt("after tracking commit")),
    )
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_tracking(
            body,
            authenticated_actor_id="alice",
            verified_registry_instance_id=backend.registry_identity.instance_id,
        )
    original = store.lookup(body, actor_id="alice", expected_identity=identity)
    assert original is not None and outbox.effect(body.command_id).status.value == "started"
    monkeypatch.setattr(outbox, "finish_effect", finish)
    service.consumer.clock = lambda: body.requested_at + timedelta(minutes=10)
    backend.reference = backend.reference.model_copy(update={"sha256": "0" * 64})
    receipt = service._resume_trusted_factor_tracking(body, authenticated_actor_id="alice")
    assert receipt.result == original.model_dump(mode="json")
    assert outbox.receipt("unrelated").status.value == "pending"


def test_tracking_generic_submit_and_forged_owned_identity_are_refused(tmp_path: Path) -> None:
    from rquant.page_control import SetFactorTracked, parse_page_control_command

    service, backend, outbox, _, _, body = _control(tmp_path)
    command = SetFactorTracked(
        command_id=body.command_id, requested_at=body.requested_at, request=body
    )
    for submit in (outbox.enqueue, service.submit, parse_page_control_command):
        with pytest.raises(ValueError):
            submit(command)
    with pytest.raises(ValueError):
        parse_page_control_command(command.model_dump())
    with pytest.raises(ValueError):
        service._submit_trusted_factor_tracking(
            body, authenticated_actor_id="alice", verified_registry_instance_id="0" * 32
        )
    assert outbox.receipt(body.command_id) is None


def test_tracking_private_socket_permissions_original_receipt_and_cleanup(tmp_path: Path) -> None:
    import os
    from tempfile import TemporaryDirectory
    from threading import Thread

    from rquant.factor_tracking_admission import (
        FactorTrackingAdmission,
        FactorTrackingAdmissionClient,
        FactorTrackingAdmissionRejectedError,
        FactorTrackingAdmissionUnavailableError,
        build_factor_tracking_admission_server,
    )

    service, backend, outbox, _, _, body = _control(tmp_path)
    web_uid = os.geteuid() + 1
    with TemporaryDirectory(prefix="ft-", dir=Path("/tmp").resolve()) as directory:
        private = Path(directory)
        os.chown(private, os.geteuid(), os.getegid())
        private.chmod(0o710)
        socket = private / "tracking.sock"
        server = build_factor_tracking_admission_server(
            FactorTrackingAdmission(service, enabled=True, tracking_users=frozenset({"alice"})),
            socket_path=socket,
            trusted_web_uid=web_uid,
            shared_gid=os.getegid(),
            peer_uid=lambda _: web_uid,
        )
        assert server is not None
        thread = Thread(
            target=server.serve_forever, daemon=True, name="synthetic-tracking-listener"
        )
        thread.start()
        try:
            client = FactorTrackingAdmissionClient(
                socket,
                expected_service_uid=os.geteuid(),
                shared_gid=os.getegid(),
                client_uid=lambda: web_uid,
            )
            with pytest.raises(FactorTrackingAdmissionRejectedError):
                client.lookup(body, authenticated_actor_id="editor-only")
            assert outbox.receipt(body.command_id) is None
            applied = client.submit(
                body,
                authenticated_actor_id="alice",
                verified_registry_instance_id=backend.registry_identity.instance_id,
            )
            assert (
                applied.status == "applied"
                and client.resume(body, authenticated_actor_id="alice") == applied
            )
            server.peer_uid = lambda _: -1
            with pytest.raises(FactorTrackingAdmissionUnavailableError):
                client.lookup(body, authenticated_actor_id="alice")
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
            assert not thread.is_alive() and not socket.exists()
