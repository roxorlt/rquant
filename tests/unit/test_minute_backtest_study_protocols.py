from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module, util
from types import ModuleType
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet

if TYPE_CHECKING:
    from rquant.minute_backtest_study_protocols import MinuteStudyProtocol


def study_api() -> ModuleType:
    assert util.find_spec("rquant.minute_backtest_study_protocols") is not None, (
        "typed study protocol is not implemented"
    )
    return import_module("rquant.minute_backtest_study_protocols")


def study_protocol(**updates: object) -> MinuteStudyProtocol:
    api = study_api()
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(entry_mode="factor_confirm"))
    values: dict[str, object] = {
        "source": {
            "source_key": "synthetic.parameter-study",
            "source_version": 1,
            "owner_id": "researcher",
            "full_input_hash": "1" * 64,
            "dataset_snapshot_id": "2" * 64,
            "frequency": "1min",
            "start_date": date(2026, 7, 1),
            "end_date": date(2026, 8, 10),
            "published_at": datetime(2026, 10, 6, tzinfo=UTC),
        },
        "head": {
            "definition_id": parameters.definition_id,
            "definition_version": 1,
            "evaluator_semantic_version": "2.0.0",
            "parameter_fingerprint": parameters.fingerprint,
            "registration_fingerprint": "3" * 64,
            "spec_fingerprint": "4" * 64,
            "executable_fingerprint": "5" * 64,
            "producer_commit": "6" * 40,
        },
        "parameters": parameters,
        "split": {
            "train_start": date(2026, 7, 1),
            "train_end": date(2026, 7, 30),
            "test_start": date(2026, 7, 31),
            "test_end": date(2026, 8, 3),
        },
        "score_profile": "v1",
        "top_n": 2,
        "min_trades": 5,
        "random_seed": 17,
        "requested_at": datetime(2026, 10, 7, tzinfo=UTC),
    }
    values.update(updates)
    return api.MinuteStudyProtocol.model_validate(values)


def test_protocol_round_trip_binds_the_complete_recipe_and_original_profile() -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.topn_selection import resolve_score_profiles

    protocol = study_protocol()
    assert (
        study_api().MinuteStudyProtocol.model_validate_json(protocol.model_dump_json()) == protocol
    )
    assert protocol.profile_fingerprint == canonical_sha256(resolve_score_profiles(["v1"])[0])
    assert protocol.study_id == canonical_sha256(
        {
            "protocol": protocol.model_dump(mode="json"),
            "score_profile": resolve_score_profiles(["v1"])[0].model_dump(mode="json"),
        }
    )
    assert protocol.worker_seed == 17
    assert protocol.split.partition(date(2026, 7, 30)) == "train"
    assert protocol.split.partition(date(2026, 7, 31)) == "test"


@pytest.mark.parametrize(
    "field,value",
    [
        ("score_profile", "v2_low_position"),
        ("top_n", 3),
        ("min_trades", 6),
        ("random_seed", 18),
        ("requested_at", datetime(2026, 10, 7, 0, 1, tzinfo=UTC)),
    ],
)
def test_each_execution_selection_field_changes_the_study_identity(
    field: str, value: object
) -> None:
    first = study_protocol()
    changed = study_protocol(**{field: value})
    assert changed.study_id != first.study_id


@pytest.mark.parametrize(
    "section,field,value",
    [
        ("source", "source_key", "another-source"),
        ("source", "source_version", 2),
        ("source", "owner_id", "another-owner"),
        ("source", "full_input_hash", "7" * 64),
        ("source", "dataset_snapshot_id", "8" * 64),
        ("head", "registration_fingerprint", "9" * 64),
        ("head", "spec_fingerprint", "a" * 64),
        ("head", "executable_fingerprint", "b" * 64),
        ("head", "producer_commit", "c" * 40),
        ("split", "train_end", date(2026, 7, 29)),
        ("split", "test_end", date(2026, 8, 4)),
    ],
)
def test_source_head_and_split_are_in_the_full_identity(
    section: str, field: str, value: object
) -> None:
    original = study_protocol()
    body = original.model_dump(mode="python")
    body[section][field] = value
    assert study_api().MinuteStudyProtocol.model_validate(body).study_id != original.study_id


def test_advanced_paper_terms_and_new_recipe_head_remain_bound() -> None:
    original = study_protocol()
    body = original.model_dump(mode="python")
    body["parameters"]["parameters"]["paper"]["entry_slippage_pct"] = 0.012
    recipe = MinuteParameterSet.model_validate(body["parameters"])
    body["head"]["parameter_fingerprint"] = recipe.fingerprint
    body["head"]["definition_id"] = recipe.definition_id
    changed = study_api().MinuteStudyProtocol.model_validate(body)
    assert changed.study_id != original.study_id
    assert changed.parameters.parameters.paper.entry_slippage_pct == 0.012


@pytest.mark.parametrize(
    "section,field,value",
    [
        ("split", "test_start", date(2026, 7, 30)),
        ("split", "train_end", date(2026, 6, 30)),
        ("source", "end_date", date(2026, 8, 1)),
        ("source", "frequency", "5min"),
        ("source", "published_at", datetime(2026, 10, 8, tzinfo=UTC)),
        ("head", "parameter_fingerprint", "9" * 64),
        ("head", "definition_id", "foreign.parameters"),
        ("head", "definition_version", 2),
    ],
)
def test_detached_frequency_head_future_source_or_overlapping_window_is_rejected(
    section: str, field: str, value: object
) -> None:
    body = study_protocol().model_dump(mode="python")
    body[section][field] = value
    with pytest.raises(ValidationError):
        study_api().MinuteStudyProtocol.model_validate(body)


@pytest.mark.parametrize(
    "values",
    [
        {"score_profile": "future_winner"},
        {"top_n": 0},
        {"min_trades": 0},
        {"random_seed": True},
        {"random_seed": -1},
        {"random_seed": 2**63},
        {"lookahead_returns": [1.0]},
        {"requested_at": datetime(2026, 10, 7)},
    ],
)
def test_unknown_request_fields_profiles_and_invalid_seed_or_clock_are_rejected(
    values: dict[str, object],
) -> None:
    with pytest.raises((ValidationError, ValueError)):
        study_protocol(**values)


def test_only_declared_train_or_test_dates_are_selectable() -> None:
    protocol = study_protocol()
    with pytest.raises(ValueError):
        protocol.split.partition(protocol.split.test_end + timedelta(days=1))


def test_source_and_split_keep_the_original_inclusive_date_budget() -> None:
    from rquant.minute_backtest_contracts import MAX_DATE_SPAN

    api = study_api()
    protocol = study_protocol()
    body = protocol.source.model_dump(mode="python")
    body["end_date"] = body["start_date"] + timedelta(days=MAX_DATE_SPAN - 1)
    assert api.MinuteStudySource.model_validate(body).end_date == body["end_date"]
    body["end_date"] += timedelta(days=1)
    with pytest.raises(ValidationError):
        api.MinuteStudySource.model_validate(body)
    split = protocol.split.model_dump(mode="python")
    split["test_end"] = split["train_start"] + timedelta(days=MAX_DATE_SPAN)
    with pytest.raises(ValidationError):
        api.MinuteStudySplit.model_validate(split)


def auction_study() -> MinuteStudyProtocol:
    from rquant.minute_backtest_parameters import MinuteAuctionGapParameters

    body = study_protocol().model_dump(mode="python")
    recipe = MinuteParameterSet(
        parameters=MinuteAuctionGapParameters(
            start_date="2026-07-01",
            end_date="2026-08-03",
        )
    )
    body["parameters"] = recipe
    body["head"]["definition_id"] = recipe.definition_id
    body["head"]["parameter_fingerprint"] = recipe.fingerprint
    return study_api().MinuteStudyProtocol.model_validate(body)


def test_auction_recipe_covers_a_fold_without_changing_its_actual_registered_head() -> None:
    original = auction_study()
    body = original.model_dump(mode="python")
    body["split"] = {
        "train_start": date(2026, 7, 1),
        "train_end": date(2026, 7, 5),
        "test_start": date(2026, 7, 6),
        "test_end": date(2026, 7, 7),
    }
    fold = study_api().MinuteStudyProtocol.model_validate(body)
    assert fold.parameters == original.parameters
    assert fold.parameters.model_dump(mode="json") == original.parameters.model_dump(mode="json")
    assert fold.head == original.head
    assert fold.source == original.source
    assert fold.study_id != original.study_id


@pytest.mark.parametrize("boundary", ["start", "end"])
def test_auction_recipe_rejects_fold_dates_beyond_the_actual_parameter_window(
    boundary: str,
) -> None:
    body = auction_study().model_dump(mode="python")
    if boundary == "start":
        body["source"]["start_date"] = date(2026, 6, 1)
        body["split"]["train_start"] = date(2026, 6, 30)
    else:
        body["split"]["test_end"] = date(2026, 8, 4)
    with pytest.raises(ValidationError):
        study_api().MinuteStudyProtocol.model_validate(body)
