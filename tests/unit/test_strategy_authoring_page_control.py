"""The original PageControl journal owns template effects and exact retries."""

from __future__ import annotations

from datetime import timedelta

import pytest

from rquant.page_control import (
    PageControlCommandConflictError,
    PageControlStatus,
)
from rquant.page_control_service import build_page_control_service
from rquant.strategy_authoring import (
    StrategyAuthoringPageControlBackend,
)
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate
from tests.unit.test_strategy_authoring import NOW, catalog, draft, store


def service_for(tmp_path, target, *, enabled: bool = True):
    backend = StrategyAuthoringPageControlBackend(target, editor_users=("alice",), enabled=enabled)
    return build_page_control_service(
        outbox_path=tmp_path / "page-control.sqlite",
        data_dir=tmp_path / "private-data",
        log_dir=tmp_path / "private-logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        strategy_authoring_backend=backend,
        clock=lambda: NOW + timedelta(minutes=1),
    )


def submit(service, target, request, *, owner: str = "alice", sources=None):
    return service._submit_trusted_strategy_authoring(
        request,
        authenticated_actor_id=owner,
        verified_metadata_identity=target.identity(),
        catalog=catalog() if sources is None else sources,
    )


def test_public_request_and_forged_owned_request_have_no_journal_side_effect(tmp_path) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    with pytest.raises(ValueError, match="trusted"):
        service.submit(request)
    assert service.outbox.receipt(request.command_id) is None
    owned = service.consumer.strategy_authoring_backend.compile(
        request,
        authenticated_actor_id="alice",
        catalog=catalog(),
        expected_identity=target.identity(),
    )
    with pytest.raises(ValueError, match="trusted"):
        service.submit(owned)
    with pytest.raises(ValueError, match="trusted"):
        service.outbox.enqueue(owned)
    assert service.outbox.receipt(request.command_id) is None
    assert target.list_current(owner_id="alice") == ()


def test_original_journal_receipt_binds_exact_original_registry_and_metadata(tmp_path) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    receipt = submit(service, target, request)
    assert receipt.status is PageControlStatus.SUCCEEDED
    saved = target.lookup_command(request, owner_id="alice")
    assert receipt.result == saved.model_dump(mode="json")
    effect = service.outbox.effect(request.command_id)
    assert effect.status.value == "succeeded" and effect.result == receipt.result
    version = target.get_version(saved.strategy_id, 1, owner_id="alice")
    original = target.definition_registry(saved.strategy_id).read_strategy_spec(
        version.head.registration_fingerprint
    )
    assert original.record_hash == saved.head.record_hash
    assert service.outbox.audit(request.command_id).command_kind == "save_strategy_template"


def test_original_lookup_does_not_consult_new_source_head_or_metadata_identity(
    tmp_path, monkeypatch
) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    first = submit(service, target, request)
    identity = target.identity()

    def unexpected_read(*args, **kwargs):
        raise AssertionError("exact journal retry must not read a new catalog or head")

    monkeypatch.setattr(target, "accept", unexpected_read)
    monkeypatch.setattr(target, "get_current", unexpected_read)
    backend = service.consumer.strategy_authoring_backend
    monkeypatch.setattr(backend, "identity", unexpected_read)
    retried = service._submit_trusted_strategy_authoring(
        request,
        authenticated_actor_id="alice",
        verified_metadata_identity=identity.model_copy(update={"instance_id": "f" * 32}),
        catalog=catalog(generation="new-generation"),
    )
    assert retried == first
    for changed in (
        request.model_copy(update={"name": "换正文"}),
        request.model_copy(update={"generation_id": "new-generation"}),
    ):
        with pytest.raises(PageControlCommandConflictError):
            service._lookup_trusted_strategy_authoring(changed, authenticated_actor_id="alice")
    with pytest.raises((PermissionError, PageControlCommandConflictError)):
        service._lookup_trusted_strategy_authoring(request, authenticated_actor_id="bob")


def test_registry_to_metadata_crash_recovers_original_command_without_a_new_version(
    tmp_path, monkeypatch
) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    complete = target._commit_saved

    def interrupted(*args, **kwargs):
        raise RuntimeError("interrupt after registry before metadata")

    monkeypatch.setattr(target, "_commit_saved", interrupted)
    receipt = submit(service, target, request)
    assert receipt.status is PageControlStatus.PENDING
    assert service.outbox.effect(request.command_id).status.value == "started"
    assert target.list_current(owner_id="alice") == ()
    frozen = target.accepted_command(request, owner_id="alice")
    original = target.definition_registry(frozen.strategy_id).latest_strategy_spec(
        frozen.strategy_id, as_of=NOW
    )
    assert original.spec.version == 1
    monkeypatch.setattr(target, "_commit_saved", complete)
    recovered = submit(service, target, request, sources=catalog(generation="changed"))
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result["strategy_id"] == frozen.strategy_id
    assert len(target.versions(frozen.strategy_id, owner_id="alice")) == 1


def test_admission_before_enqueue_crash_preserves_server_id_and_original_body(
    tmp_path, monkeypatch
) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    enqueue = service.outbox.enqueue_trusted_strategy_authoring

    def interrupted(*args, **kwargs):
        raise RuntimeError("interrupt before original journal enqueue")

    monkeypatch.setattr(service.outbox, "enqueue_trusted_strategy_authoring", interrupted)
    with pytest.raises(RuntimeError, match="before original journal"):
        submit(service, target, request)
    assert service.outbox.receipt(request.command_id) is None
    accepted = target.accepted_command(request, owner_id="alice")
    assert accepted is not None
    monkeypatch.setattr(service.outbox, "enqueue_trusted_strategy_authoring", enqueue)
    recovered = submit(service, target, request, sources=catalog(generation="changed"))
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result["strategy_id"] == accepted.strategy_id
    assert len(target.list_current(owner_id="alice")) == 1


def test_pending_original_effect_cannot_write_a_replaced_metadata_store(
    tmp_path, monkeypatch
) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    backend = service.consumer.strategy_authoring_backend
    original_submit = backend.submit

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt("process ended after original effect started")

    monkeypatch.setattr(backend, "submit", interrupted)
    with pytest.raises(KeyboardInterrupt):
        submit(service, target, request)
    target.path.rename(tmp_path / "original-authoring.sqlite")
    replacement = store(tmp_path)
    monkeypatch.setattr(backend, "submit", original_submit)
    service.consumer.clock = lambda: NOW + timedelta(minutes=2)
    recovered = submit(service, replacement, request)
    assert recovered.status is PageControlStatus.PENDING
    assert replacement.list_current(owner_id="alice") == ()
    assert service.outbox.effect(request.command_id).status.value == "started"


def test_success_receipt_cannot_be_rebound_to_a_replaced_metadata_store(tmp_path) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    first = submit(service, target, request)
    assert first.status is PageControlStatus.SUCCEEDED
    target.path.rename(tmp_path / "original-authoring.sqlite")
    replacement = store(tmp_path)
    with pytest.raises(RuntimeError, match="identity"):
        submit(service, replacement, request)
    assert replacement.list_current(owner_id="alice") == ()
    assert service.outbox.receipt(request.command_id) == first


@pytest.mark.parametrize("enabled,owner", [(False, "alice"), (True, "bob")])
def test_default_off_and_non_editor_have_no_admission_effect(tmp_path, enabled, owner) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target, enabled=enabled)
    request = draft()
    with pytest.raises(PermissionError):
        submit(service, target, request, owner=owner)
    assert service.outbox.receipt(request.command_id) is None
    assert target.accepted_command(request, owner_id=owner) is None


def test_targeted_settle_leaves_unrelated_original_commands_pending(tmp_path) -> None:
    from rquant.page_control import SaveCanvas

    target = store(tmp_path)
    service = service_for(tmp_path, target)
    unrelated = SaveCanvas(command_id="unrelated", requested_at=NOW, name="untouched")
    service.outbox.enqueue(unrelated)
    receipt = submit(service, target, draft())
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert service.outbox.receipt(unrelated.command_id).status is PageControlStatus.PENDING
    assert not (tmp_path / "private-data" / "canvases").exists()


def test_archive_has_original_journal_effect_and_blocks_new_save(tmp_path) -> None:
    from uuid import uuid4

    target = store(tmp_path)
    service = service_for(tmp_path, target)
    saved = submit(service, target, draft()).result
    current = target.get_current(saved["strategy_id"], owner_id="alice")
    archive = ArchiveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="generation-a",
        strategy_id=current.strategy_id,
        expected_head=current.head,
    )
    receipt = submit(service, target, archive)
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert target.get_current(current.strategy_id, owner_id="alice").archived
    assert submit(service, target, archive, sources=catalog(generation="changed")) == receipt
    with pytest.raises(ValueError, match="archived"):
        submit(service, target, draft(strategy_id=current.strategy_id, expected_head=current.head))


def test_archive_admission_before_enqueue_blocks_new_save_and_run(tmp_path, monkeypatch) -> None:
    from uuid import uuid4

    target = store(tmp_path)
    service = service_for(tmp_path, target)
    saved = submit(service, target, draft()).result
    current = target.get_current(saved["strategy_id"], owner_id="alice")
    archive = ArchiveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="generation-a",
        strategy_id=current.strategy_id,
        expected_head=current.head,
    )
    enqueue = service.outbox.enqueue_trusted_strategy_authoring

    def interrupted(*args, **kwargs):
        raise RuntimeError("archive accepted before journal enqueue")

    monkeypatch.setattr(service.outbox, "enqueue_trusted_strategy_authoring", interrupted)
    with pytest.raises(RuntimeError, match="archive accepted"):
        submit(service, target, archive)
    with pytest.raises(ValueError, match="awaiting recovery"):
        target.accept(
            draft(strategy_id=current.strategy_id, expected_head=current.head),
            owner_id="alice",
            catalog=catalog(),
        )
    with pytest.raises(ValueError, match="awaiting recovery"):
        target.admit_run(
            current.strategy_id,
            current.head,
            owner_id="alice",
            command_id=str(uuid4()),
            request_hash="1" * 64,
        )
    monkeypatch.setattr(service.outbox, "enqueue_trusted_strategy_authoring", enqueue)
    receipt = submit(service, target, archive, sources=catalog(generation="changed"))
    assert receipt.status is PageControlStatus.SUCCEEDED
