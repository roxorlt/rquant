"""Accepted saves reserve the existing bounded Serving publication capacity."""

import pytest

from rquant import strategy_authoring as authoring
from rquant.strategy_authoring import StrategyAuthoringConflict
from tests.unit.test_strategy_authoring import catalog, draft, store


def no_definition_effect(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("capacity refusal must precede original definition publication")


@pytest.mark.parametrize("boundary", ("MAX_TEMPLATE_COUNT", "MAX_TEMPLATE_VERSIONS"))
def test_pending_saves_reserve_global_count_before_registry_effect(
    tmp_path, monkeypatch, boundary
) -> None:
    monkeypatch.setattr(authoring, boundary, 2, raising=False)
    target = store(tmp_path)
    originals = [draft(name=f"已受理 {index}") for index in range(2)]
    accepted = [
        target.accept(request, owner_id="alice", catalog=catalog()) for request in originals
    ]
    monkeypatch.setattr(target, "definition_registry", no_definition_effect)
    with pytest.raises(StrategyAuthoringConflict, match="budget"):
        target.save(draft(), owner_id="alice", catalog=catalog())
    assert (
        target.accept(originals[0], owner_id="alice", catalog=catalog(generation="changed"))
        == accepted[0]
    )


def test_committed_and_pending_versions_reserve_projection_bytes(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        authoring, "MAX_TEMPLATE_PROJECTION_BYTES", 1024 * 1024 + 12000, raising=False
    )
    target = store(tmp_path)
    saved = target.save(draft(), owner_id="alice", catalog=catalog())
    original = draft(strategy_id=saved.strategy_id, expected_head=saved.head).model_copy(
        update={"change_note": "改" * 1024}
    )
    monkeypatch.setattr(target, "definition_registry", no_definition_effect)
    with pytest.raises(StrategyAuthoringConflict, match="budget"):
        target.save(original, owner_id="alice", catalog=catalog())
    assert target.accepted_command(original, owner_id="alice") is None
