"""Authoring tests cover owner, CAS, retry and interrupted original publication."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from rquant.strategy_authoring import StrategyAuthoringConflict, StrategyAuthoringStore
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate, SaveStrategyTemplate
from rquant.strategy_authoring_source import StrategySourceCatalog, TemplatePoolReference
from rquant.strategy_template import StrategyTemplate

NOW = datetime(2026, 10, 5, tzinfo=UTC)
COMMIT = "0" * 40


def draft(*, strategy_id: str | None = None, expected_head: object = None, name: str = "我的策略", command_id: str | None = None, generation_id: str = "generation-a") -> SaveStrategyTemplate:
    return SaveStrategyTemplate(command_id=command_id or str(uuid4()), requested_at=NOW, generation_id=generation_id, strategy_id=strategy_id, expected_head=expected_head, name=name, change_note="初次保存" if strategy_id is None else "修改规则", rules=StrategyTemplate.model_validate({"entry": {"kind": "conditions", "conditions": [{"key": "not_st"}]}, "weight_rule": {"max_positions": 10}, "rebalance_rule": {"kind": "daily"}}))


def store(tmp_path) -> StrategyAuthoringStore:
    target = StrategyAuthoringStore(tmp_path / "authoring.sqlite", definition_root=tmp_path / "definitions", producer_commit=COMMIT, clock=lambda: NOW)
    target.initialize()
    return target


def catalog(*, owner: str = "alice", generation: str = "generation-a") -> StrategySourceCatalog:
    return StrategySourceCatalog(owner_id=owner, generation_id=generation, pools=(), signals=())


def test_new_save_publishes_original_definition_before_confirming_head(tmp_path) -> None:
    target = store(tmp_path)
    request = draft()
    receipt = target.save(request, owner_id="alice", catalog=catalog())
    assert receipt.action == "save" and receipt.head.version == 1
    assert receipt.strategy_id.startswith("template_")
    version = target.get_version(receipt.strategy_id, 1, owner_id="alice")
    assert version.name == "我的策略" and version.rules == request.rules
    registration = target.definition_registry(receipt.strategy_id).read_strategy_spec(receipt.head.registration_fingerprint)
    assert registration.record_hash == receipt.head.record_hash
    assert registration.spec.spec_fingerprint == receipt.head.spec_fingerprint
    assert target.list_current(owner_id="alice")[0].head == receipt.head
    assert not (tmp_path / "paper.sqlite").exists()


def test_retry_lookup_precedes_changed_catalog_and_rejects_changed_original_body(tmp_path) -> None:
    target = store(tmp_path)
    request = draft()
    saved = target.save(request, owner_id="alice", catalog=catalog())
    assert target.save(request, owner_id="alice", catalog=catalog(generation="generation-b")) == saved
    with pytest.raises(StrategyAuthoringConflict, match="original"):
        target.save(request.model_copy(update={"name": "不同正文"}), owner_id="alice", catalog=catalog())
    assert target.get_version(saved.strategy_id, 1, owner_id="alice").name == "我的策略"


def test_foreign_owner_cannot_read_modify_or_recover(tmp_path) -> None:
    target = store(tmp_path)
    request = draft()
    saved = target.save(request, owner_id="alice", catalog=catalog())
    assert target.list_current(owner_id="bob") == ()
    with pytest.raises(PermissionError):
        target.get_version(saved.strategy_id, 1, owner_id="bob")
    with pytest.raises(PermissionError):
        target.save(draft(strategy_id=saved.strategy_id, expected_head=saved.head), owner_id="bob", catalog=catalog(owner="bob"))
    with pytest.raises(PermissionError):
        target.lookup_command(request, owner_id="bob")


def test_same_head_two_saves_allow_exactly_one_and_preserve_previous_version(tmp_path) -> None:
    target = store(tmp_path)
    first = target.save(draft(), owner_id="alice", catalog=catalog())
    updates = [draft(strategy_id=first.strategy_id, expected_head=first.head, name=name) for name in ("版本甲", "版本乙")]
    def run(request: SaveStrategyTemplate) -> object:
        try:
            return target.save(request, owner_id="alice", catalog=catalog())
        except StrategyAuthoringConflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(run, updates))
    assert sum(result == "conflict" for result in results) == 1
    assert len(target.versions(first.strategy_id, owner_id="alice")) == 2
    assert target.get_version(first.strategy_id, 1, owner_id="alice").head == first.head


def test_interrupted_registry_publication_recovers_same_id_and_body(tmp_path, monkeypatch) -> None:
    target = store(tmp_path)
    request = draft()
    original = target._commit_saved
    def interrupted(*args: object, **kwargs: object) -> None:
        raise RuntimeError("interrupt after original registry publication")
    monkeypatch.setattr(target, "_commit_saved", interrupted)
    with pytest.raises(RuntimeError, match="interrupt"):
        target.save(request, owner_id="alice", catalog=catalog())
    assert target.list_current(owner_id="alice") == ()
    frozen = target.accepted_command(request, owner_id="alice")
    assert target.definition_registry(frozen.strategy_id).latest_strategy_spec(frozen.strategy_id, as_of=NOW) is not None
    monkeypatch.setattr(target, "_commit_saved", original)
    saved = target.save(request, owner_id="alice", catalog=catalog(generation="changed"))
    assert saved.strategy_id == frozen.strategy_id
    assert len(target.versions(saved.strategy_id, owner_id="alice")) == 1


def test_archive_uses_head_and_retains_original_versions_receipts(tmp_path) -> None:
    target = store(tmp_path)
    request = draft()
    saved = target.save(request, owner_id="alice", catalog=catalog())
    archive = ArchiveStrategyTemplate(command_id=str(uuid4()), requested_at=NOW, generation_id="generation-a", strategy_id=saved.strategy_id, expected_head=saved.head)
    receipt = target.archive(archive, owner_id="alice")
    assert receipt.action == "archive" and target.get_current(saved.strategy_id, owner_id="alice").archived
    assert target.lookup_command(request, owner_id="alice") == saved
    assert target.archive(archive, owner_id="alice") == receipt
    assert target.get_version(saved.strategy_id, 1, owner_id="alice").head == saved.head
    with pytest.raises(StrategyAuthoringConflict, match="archived"):
        target.save(draft(strategy_id=saved.strategy_id, expected_head=saved.head), owner_id="alice", catalog=catalog())
    with pytest.raises(StrategyAuthoringConflict, match="archived"):
        target.admit_run(saved.strategy_id, saved.head, owner_id="alice", command_id=str(uuid4()), request_hash="1" * 64)


def test_pool_source_owner_and_exact_version_are_required(tmp_path) -> None:
    target = store(tmp_path)
    request = draft().model_copy(update={"rules": StrategyTemplate.model_validate({"entry": {"kind": "pool", "pool_key": "user/pool", "version": 2, "body_hash": "1" * 64}, "weight_rule": {"max_positions": 10}, "rebalance_rule": {"kind": "daily"}})})
    foreign = StrategySourceCatalog(owner_id="alice", generation_id="generation-a", pools=(TemplatePoolReference(pool_key="user/pool", version=2, body_hash="1" * 64, owner_id="bob", name="池子"),), signals=())
    with pytest.raises(PermissionError):
        target.save(request, owner_id="alice", catalog=foreign)
    assert target.list_current(owner_id="alice") == ()


def test_client_request_time_cannot_backdate_original_registration_or_saved_facts(tmp_path) -> None:
    from datetime import timedelta

    admitted_at = NOW + timedelta(hours=2)
    completed_at = admitted_at + timedelta(seconds=20)
    server_now = [admitted_at]
    target = StrategyAuthoringStore(
        tmp_path / "authoring.sqlite", definition_root=tmp_path / "definitions",
        producer_commit=COMMIT, clock=lambda: server_now[0],
    )
    target.initialize()
    request = draft().model_copy(update={"requested_at": NOW - timedelta(days=30)})
    accepted = target.accept(request, owner_id="alice", catalog=catalog())
    assert accepted.accepted_at == admitted_at
    server_now[0] = completed_at
    receipt = target.complete_save(accepted)
    metadata = target.get_current(receipt.strategy_id, owner_id="alice")
    registry = target.definition_registry(receipt.strategy_id)
    original = registry.read_strategy_spec(receipt.head.registration_fingerprint)
    assert original.registered_at == original.available_at == completed_at
    assert metadata.saved_at == completed_at
    assert receipt.completed_at == completed_at
    assert registry.latest_strategy_spec(receipt.strategy_id, as_of=NOW) is None
    server_now[0] += timedelta(days=1)
    assert target.save(request, owner_id="alice", catalog=catalog(generation="changed")) == receipt
    assert target.accepted_command(request, owner_id="alice").accepted_at == admitted_at
