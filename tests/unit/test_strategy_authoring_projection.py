"""Only confirmed owner/version facts may enter the new Serving projection."""

from __future__ import annotations

from datetime import timedelta

import pytest

from rquant.strategy_authoring_projection import (
    StrategyAuthoringProjectionTables,
    build_strategy_authoring_snapshot,
    project_strategy_authoring,
)
from tests.unit.test_strategy_authoring import NOW, catalog, draft, store


def test_projection_keeps_confirmed_versions_and_exact_head_without_registry_orphans(tmp_path, monkeypatch) -> None:
    target = store(tmp_path)
    first = target.save(draft(), owner_id="alice", catalog=catalog())
    second = target.save(draft(strategy_id=first.strategy_id, expected_head=first.head, name="第二版"), owner_id="alice", catalog=catalog())
    write = target._commit_saved

    def interrupted(*args, **kwargs):
        raise RuntimeError("unconfirmed original registration")

    monkeypatch.setattr(target, "_commit_saved", interrupted)
    with pytest.raises(RuntimeError):
        target.save(draft(), owner_id="alice", catalog=catalog())
    monkeypatch.setattr(target, "_commit_saved", write)
    snapshot = build_strategy_authoring_snapshot(target, available_at=NOW + timedelta(seconds=1), source_catalogs=(catalog(),))
    assert len(snapshot.versions) == 2
    assert [row.metadata.head.version for row in snapshot.versions] == [1, 2]
    assert [row.is_head for row in snapshot.versions] == [False, True]
    assert snapshot.versions[1].metadata.head == second.head
    projections = project_strategy_authoring(snapshot)
    assert projections.restore() == snapshot


def test_projection_rejects_body_tampering_owner_mix_and_future_saved_facts(tmp_path) -> None:
    target = store(tmp_path)
    target.save(draft(), owner_id="alice", catalog=catalog())
    snapshot = build_strategy_authoring_snapshot(target, available_at=NOW, source_catalogs=(catalog(),))
    projections = project_strategy_authoring(snapshot)
    changed = projections.definitions[0].model_copy(update={"owner_id": "bob"})
    with pytest.raises(ValueError, match="owner|snapshot"):
        StrategyAuthoringProjectionTables(state=projections.state, definitions=(changed,), sources=projections.sources).restore()
    with pytest.raises(ValueError, match="future"):
        build_strategy_authoring_snapshot(target, available_at=NOW - timedelta(seconds=1), source_catalogs=(catalog(),))


def test_owner_catalog_and_version_reads_share_one_exact_projection(tmp_path) -> None:
    target = store(tmp_path)
    alice = target.save(draft(), owner_id="alice", catalog=catalog())
    bob = target.save(draft(name="其他用户"), owner_id="bob", catalog=catalog(owner="bob"))
    snapshot = build_strategy_authoring_snapshot(target, available_at=NOW, source_catalogs=(catalog(), catalog(owner="bob")))
    assert tuple(row.metadata.strategy_id for row in snapshot.for_owner("alice")) == (alice.strategy_id,)
    assert tuple(row.metadata.strategy_id for row in snapshot.for_owner("bob")) == (bob.strategy_id,)
    assert snapshot.sources_for("alice", generation_id="new-serving").generation_id == "new-serving"
    assert snapshot.sources_for("alice", generation_id="new-serving").owner_id == "alice"
    assert snapshot.for_owner("nobody") == ()
