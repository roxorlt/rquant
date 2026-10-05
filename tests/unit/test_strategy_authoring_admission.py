"""Private template admission preserves the original journal and transport bounds."""

from __future__ import annotations

import pytest

from rquant.strategy_authoring_admission import (
    StrategyAuthoringAdmission,
    decode_strategy_authoring_request,
    build_strategy_authoring_admission_server,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_strategy_authoring import catalog, draft, store
from tests.unit.test_strategy_authoring_page_control import service_for


def test_private_submit_and_retry_use_the_original_journal_before_source_provider(tmp_path) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    calls = []

    def source_provider(owner, generation):
        calls.append((owner, generation))
        return catalog(owner=owner, generation=generation)

    admission = StrategyAuthoringAdmission(service, source_catalog_provider=source_provider)
    request = draft()
    first = admission.submit(request, authenticated_actor_id="alice", verified_metadata_identity=target.identity())
    second = admission.submit(request, authenticated_actor_id="alice", verified_metadata_identity=target.identity())
    assert first == second
    assert first.original_request == request and first.owner_id == "alice"
    assert first.metadata_identity == target.identity()
    assert calls == [("alice", "generation-a")]
    assert admission.lookup(request, authenticated_actor_id="alice") == first
    assert admission.resume(request, authenticated_actor_id="alice") == first


def test_private_pre_enqueue_recovery_does_not_read_a_changed_source(tmp_path, monkeypatch) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    target.accept(request, owner_id="alice", catalog=catalog())

    def forbidden(*args):
        raise AssertionError("accepted original must not consult new source")

    admission = StrategyAuthoringAdmission(service, source_catalog_provider=forbidden)
    result = admission.submit(request, authenticated_actor_id="alice", verified_metadata_identity=target.identity())
    assert result.receipt.status.value == "succeeded"
    assert len(target.list_current(owner_id="alice")) == 1


def test_resume_uses_metadata_original_before_journal_enqueue_and_its_original_inode(tmp_path) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    request = draft()
    accepted = target.accept(request, owner_id="alice", catalog=catalog())

    def forbidden(*args):
        raise AssertionError("resume must not read current source")

    admission = StrategyAuthoringAdmission(service, source_catalog_provider=forbidden)
    assert accepted.metadata_identity == target.identity()
    result = admission.resume(request, authenticated_actor_id="alice")
    assert result.receipt.status.value == "succeeded"
    assert result.metadata_identity == accepted.metadata_identity

    pending = draft()
    target.accept(pending, owner_id="alice", catalog=catalog())
    old = target.path.read_bytes()
    target.path.rename(tmp_path / "old-private-meta.sqlite")
    target.path.write_bytes(old)
    target.path.chmod(0o600)
    with pytest.raises(RuntimeError, match="identity"):
        admission.resume(pending, authenticated_actor_id="alice")
    assert service.outbox.receipt(pending.command_id) is None


def test_private_decoder_accepts_only_the_typed_ownerless_body(tmp_path) -> None:
    target = store(tmp_path)
    request = draft()
    envelope = {"authenticated_actor_id": "alice", "request": request.model_dump(mode="json"), "verified_metadata_identity": target.identity().model_dump(mode="json")}
    decoded = decode_strategy_authoring_request(canonical_json_bytes(envelope), mode="submit")
    assert decoded.request == request and decoded.authenticated_actor_id == "alice"
    for changed in ({**envelope, "owner_id": "bob"}, {**envelope, "request": {**envelope["request"], "owner_id": "alice"}}, {**envelope, "verified_metadata_identity": None}):
        with pytest.raises(ValueError):
            decode_strategy_authoring_request(canonical_json_bytes(changed), mode="submit")
    with pytest.raises(ValueError):
        decode_strategy_authoring_request(b'{"authenticated_actor_id":"alice","authenticated_actor_id":"bob"}', mode="lookup")


def test_private_listener_is_default_off_without_creating_directories(tmp_path) -> None:
    target = store(tmp_path)
    service = service_for(tmp_path, target, enabled=False)
    admission = StrategyAuthoringAdmission(service, source_catalog_provider=lambda owner, generation: catalog())
    path = tmp_path / "must-not-exist" / "authoring.sock"
    assert build_strategy_authoring_admission_server(admission, socket_path=path) is None
    assert not path.parent.exists()
    with pytest.raises(PermissionError):
        admission.lookup(draft(), authenticated_actor_id="alice")
