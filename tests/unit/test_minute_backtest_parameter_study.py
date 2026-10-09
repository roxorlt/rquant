from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module, util
from types import ModuleType

import pytest
from pydantic import ValidationError
from tests.unit.test_minute_backtest_parameter_optimizer import study_candidate
from tests.unit.test_minute_backtest_study_protocols import study_protocol

from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_study_protocols import MinuteStudyCandidate
from rquant.runtime_contracts import canonical_sha256


def study_binding_api() -> ModuleType:
    assert util.find_spec("rquant.minute_backtest_parameter_study") is not None, (
        "three-part minute study binding is not implemented"
    )
    return import_module("rquant.minute_backtest_parameter_study")


def binding() -> object:
    protocol = study_protocol(
        split={
            "train_start": date(2026, 7, 1),
            "train_end": date(2026, 7, 29),
            "test_start": date(2026, 8, 1),
            "test_end": date(2026, 8, 3),
        }
    )
    formal = MinuteExperimentProtocol.model_validate(
        {
            "train_range": {"start_date": date(2026, 7, 1), "end_date": date(2026, 7, 29)},
            "validation_range": {
                "start_date": date(2026, 7, 30),
                "end_date": date(2026, 7, 31),
            },
            "frozen_outer_test_range": {
                "start_date": date(2026, 8, 1),
                "end_date": date(2026, 8, 3),
            },
        }
    )
    return study_binding_api().MinuteParameterStudyBinding.from_formal_protocol(
        protocol=protocol, formal_protocol=formal, request_hash="a" * 64
    )


def candidate_at(bound: object, day: date) -> MinuteStudyCandidate:
    data = study_candidate(bound.protocol).model_dump(mode="python")
    prior_clock = data["event_time"]
    clock = datetime.combine(day, prior_clock.timetz())
    offset = clock - prior_clock
    data["trade_date"] = day
    data["event_time"] = clock
    data["available_at"] += offset
    for value in data["features"]:
        value["available_at"] += offset
    return MinuteStudyCandidate.model_validate(data)


def test_binding_keeps_primary_study_id_and_binds_every_formal_range_and_request() -> None:
    bound = binding()
    assert bound.protocol.study_id == bound.study_id
    assert bound.protocol.source.full_input_hash == "1" * 64
    assert bound.binding_hash == canonical_sha256(bound.model_dump(mode="json"))
    assert type(bound).model_validate_json(bound.model_dump_json()) == bound
    body = bound.model_dump(mode="python")
    body["request_hash"] = "b" * 64
    assert type(bound).model_validate(body).binding_hash != bound.binding_hash
    body = bound.model_dump(mode="python")
    body["validation_range"]["start_date"] = date(2026, 7, 31)
    assert type(bound).model_validate(body).binding_hash != bound.binding_hash


@pytest.mark.parametrize("field", ["train_range", "frozen_outer_test_range"])
def test_primary_study_cannot_substitute_validation_for_training_or_outer(field: str) -> None:
    bound = binding()
    body = bound.model_dump(mode="python")
    body[field] = body["validation_range"]
    with pytest.raises(ValidationError):
        type(bound).model_validate(body)


@pytest.mark.parametrize(
    "day,partition",
    [
        (date(2026, 7, 29), "training"),
        (date(2026, 7, 31), "validation"),
        (date(2026, 8, 1), "out_of_sample"),
    ],
)
def test_three_part_entry_selection_uses_original_numeric_owner(
    day: date, partition: str
) -> None:
    from rquant.minute_backtest_parameter_optimizer import select_minute_study_candidates

    bound = binding()
    row = candidate_at(bound, day)
    api = import_module("rquant.minute_backtest_parameter_optimizer")
    selected = api.select_minute_three_part_study_candidates(
        bound, (row,), decision_cutoff=row.available_at
    )
    assert len(selected) == 1
    assert selected[0].partition == partition
    assert selected[0].execution_binding_hash == bound.binding_hash
    assert selected[0].study_id == bound.study_id
    assert selected[0].source_hash == bound.protocol.source.full_input_hash
    if partition != "validation":
        original = select_minute_study_candidates(
            bound.protocol, (row,), decision_cutoff=row.available_at
        )
        assert selected[0].feature_score == original[0].feature_score
        assert selected[0].feature_rank == original[0].feature_rank
    else:
        with pytest.raises(ValueError, match="outside"):
            select_minute_study_candidates(
                bound.protocol, (row,), decision_cutoff=row.available_at
            )


def test_three_part_selection_rejects_future_values_and_undeclared_dates() -> None:
    api = import_module("rquant.minute_backtest_parameter_optimizer")
    bound = binding()
    row = candidate_at(bound, date(2026, 7, 31))
    data = row.model_dump(mode="python")
    data["features"][0]["available_at"] += timedelta(seconds=30)
    future = MinuteStudyCandidate.model_validate(data)
    with pytest.raises(ValueError, match="not available"):
        api.select_minute_three_part_study_candidates(
            bound, (future,), decision_cutoff=row.available_at
        )
    with pytest.raises(ValueError, match="outside"):
        api.select_minute_three_part_study_candidates(
            bound, (), decision_cutoff=datetime(2026, 8, 4, 1, 32, tzinfo=UTC)
        )


def test_public_study_controls_bind_to_the_full_request_without_head_or_path_claims() -> None:
    from rquant.minute_backtest_commands import MinuteParameterRunConfig
    from rquant.minute_backtest_parameter_study import (
        MinuteParameterStudySettings, verify_minute_parameter_study_request,
    )

    bound = binding()
    config = MinuteParameterRunConfig(source_key=bound.protocol.source.source_key,
        source_version=bound.protocol.source.source_version,
        full_input_hash=bound.protocol.source.full_input_hash,
        parameters=bound.protocol.parameters,
        protocol=MinuteExperimentProtocol(train_range=bound.train_range,
            validation_range=bound.validation_range,
            frozen_outer_test_range=bound.frozen_outer_test_range),
        random_seed=bound.protocol.random_seed, deadline=bound.protocol.requested_at+timedelta(hours=1),
        study=bound.settings)
    bound = type(bound).model_validate(bound.model_dump(mode="python") | {
        "request_hash": canonical_sha256(config.model_dump(mode="json"))})
    verify_minute_parameter_study_request(bound, config)
    for field, value in (("top_n", 1), ("min_trades", 1), ("score_profile", "no_market")):
        altered = config.model_copy(update={"study": config.study.model_copy(update={field: value})})
        with pytest.raises(PermissionError):
            verify_minute_parameter_study_request(bound, altered)
    with pytest.raises(PermissionError):
        verify_minute_parameter_study_request(None, config)
    for name in ("head", "path", "trusted"):
        with pytest.raises(ValidationError):
            MinuteParameterStudySettings.model_validate(config.study.model_dump() | {name: "claim"})
    original = config.model_copy(update={"study": None})
    assert "study" not in original.model_dump(mode="json")
    verify_minute_parameter_study_request(None, original)
