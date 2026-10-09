from __future__ import annotations

import importlib
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest

from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.runtime_contracts import canonical_sha256


def commands() -> object:
    return importlib.import_module("rquant.minute_backtest_parameter_study_commands")


def request() -> object:
    return commands().MinuteParameterStudyExecutionRequest(
        request_id=UUID("891bfe91-7dcd-461b-b1a3-ecfbef66036e"),
        owner_id="synthetic-researcher",
        source_key="synthetic.parameter-facts",
        source_version=1,
        full_input_hash="a" * 64,
        parameters=MinuteParameterSet(parameters=MinuteNShapeParameters()),
        formal_protocol={
            "train_range": {"start_date": date(2026, 7, 31), "end_date": date(2026, 7, 31)},
            "validation_range": {"start_date": date(2026, 8, 3), "end_date": date(2026, 8, 3)},
            "frozen_outer_test_range": {
                "start_date": date(2026, 8, 4),
                "end_date": date(2026, 8, 4),
            },
        },
        settings=({"score_profile": "v1", "top_n": 1, "min_trades": 1},),
        random_seed=17,
        requested_at=datetime(2026, 10, 7, 10, tzinfo=UTC),
        deadline=datetime(2026, 10, 7, 11, tzinfo=UTC),
        mode="single",
    )


def test_pure_command_import_keeps_execution_out_of_the_public_type_dependency() -> None:
    import sys

    before = sys.modules.get("rquant.minute_backtest_parameter_study_execution")
    api = commands()
    assert api.__name__ == "rquant.minute_backtest_parameter_study_commands"
    assert sys.modules.get("rquant.minute_backtest_parameter_study_execution") is before


def test_execution_reexports_the_same_request_and_window_class_identity() -> None:
    execution = importlib.import_module("rquant.minute_backtest_parameter_study_execution")
    assert (
        execution.MinuteParameterStudyExecutionRequest
        is commands().MinuteParameterStudyExecutionRequest
    )
    assert (
        execution.MinuteParameterStudyWindowSettings
        is commands().MinuteParameterStudyWindowSettings
    )


def test_pure_request_preserves_complete_json_hash_and_round_trip() -> None:
    original = request()
    json_value = original.model_dump(mode="json")
    restored = commands().MinuteParameterStudyExecutionRequest.model_validate_json(
        original.model_dump_json()
    )
    assert restored == original
    assert restored.model_dump(mode="json") == json_value
    assert canonical_sha256(restored.model_dump(mode="json")) == canonical_sha256(json_value)
    assert restored.parameters == original.parameters
    assert restored.formal_protocol == original.formal_protocol
    assert restored.search is None and restored.walk_forward is None


def test_original_parent_command_binds_uuid_actor_time_and_exact_request() -> None:
    original = request()
    command = commands().SubmitMinuteParameterStudy(
        command_id=str(original.request_id),
        requested_at=original.requested_at,
        actor_id=original.owner_id,
        request=original,
    )
    restored = commands().SubmitMinuteParameterStudy.model_validate_json(command.model_dump_json())
    assert restored == command and restored.request is not None
    assert restored.request.model_dump_json() == original.model_dump_json()
    assert canonical_sha256(restored.request.model_dump(mode="json")) == canonical_sha256(
        original.model_dump(mode="json")
    )
    assert restored.command_id == str(original.request_id)


@pytest.mark.parametrize("field", ["command_id", "actor_id", "requested_at"])
def test_parent_command_rejects_conflicting_uuid_actor_or_request_time(field: str) -> None:
    original = request()
    body = dict(
        command_id=str(original.request_id),
        requested_at=original.requested_at,
        actor_id=original.owner_id,
        request=original,
    )
    body[field] = {
        "command_id": "990371b3-b27b-45ca-a5bc-fd10af0338ae",
        "actor_id": "another-user",
        "requested_at": original.requested_at + timedelta(microseconds=1),
    }[field]
    with pytest.raises(ValueError, match="original request"):
        commands().SubmitMinuteParameterStudy.model_validate(body)


def test_pure_request_rejects_unbound_search_and_hidden_window_defaults() -> None:
    original = request()
    body = original.model_dump(mode="python")
    body["mode"] = "random"
    with pytest.raises(ValueError, match="complete original search"):
        commands().MinuteParameterStudyExecutionRequest.model_validate(body)
    with pytest.raises(ValueError):
        commands().MinuteParameterStudyWindowSettings(fold_count=6, min_training_dates=2)
