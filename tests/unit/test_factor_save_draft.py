"""Server-owned factor definitions from bounded browser drafts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from rquant.factor.draft import FactorSaveDraft, build_draft_definition


def _draft(**changes: object) -> FactorSaveDraft:
    fields = {
        "generation_id": "a" * 64,
        "command_id": "factor-create-1",
        "requested_at": datetime(2026, 9, 29, tzinfo=UTC),
        "mode": "create",
        "factor_id": None,
        "expected_head": None,
        "name_zh": "价量强度",
        "category": "technical",
        "direction": "higher_is_better",
        "expression": "ts_mean(close, 5) / ref(vol, 2)",
    }
    fields.update(changes)
    return FactorSaveDraft.model_validate(fields)


def test_create_draft_server_owns_definition_and_stable_id() -> None:
    draft = _draft()
    first = build_draft_definition(draft, authenticated_actor_id="researcher")
    second = build_draft_definition(draft, authenticated_actor_id="researcher")
    assert first == second
    assert first.factor_id.startswith("f_")
    assert first.version == 1
    assert first.earliest_available_date is None
    assert first.dependency_columns == ("close", "vol")
    assert first.max_history_window == 5
    assert set(first.feature_catalog.columns) == {"open", "high", "low", "close", "vol", "amount"}
    assert (
        build_draft_definition(draft, authenticated_actor_id="other").factor_id != first.factor_id
    )


def test_edit_draft_gets_next_version_with_unknown_earliest_date() -> None:
    draft = _draft(
        mode="edit",
        factor_id="factor_one",
        expected_head={"version": 4, "content_sha256": "b" * 64},
    )
    definition = build_draft_definition(draft, authenticated_actor_id="researcher")
    assert definition.factor_id == "factor_one"
    assert definition.version == 5
    assert definition.earliest_available_date is None


@pytest.mark.parametrize(
    "change",
    [
        {"earliest_available_date": "2024-01-01"},
        {"feature_catalog": {"columns": ["close"]}},
        {"version": 7},
        {"dependency_columns": ["close"]},
        {"mode": "create", "factor_id": "factor_one"},
        {"mode": "edit", "factor_id": "factor_one"},
    ],
)
def test_draft_rejects_client_owned_or_inconsistent_fields(change: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _draft(**change)


@pytest.mark.parametrize("expression", ["ts_mean(vwap, 5)", "industry_neutralize(close)"])
def test_draft_rejects_unavailable_feature_or_operator(expression: str) -> None:
    with pytest.raises((ValidationError, ValueError)):
        build_draft_definition(_draft(expression=expression), authenticated_actor_id="researcher")
