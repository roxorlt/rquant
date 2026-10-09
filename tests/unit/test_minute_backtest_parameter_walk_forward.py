from __future__ import annotations

from datetime import date, timedelta
from importlib import import_module, util
from types import ModuleType

import pytest
from pydantic import ValidationError
from tests.unit.test_minute_backtest_study_protocols import auction_study, study_api, study_protocol

from rquant import topn_walk_forward
from rquant.minute_backtest_parameter_optimizer import (
    MinuteStudyTrainingObservation,
    rank_minute_study_training,
)
from rquant.minute_backtest_parameters import MinuteGrowthParameters, MinuteParameterSet


def wf_api() -> ModuleType:
    assert util.find_spec("rquant.minute_backtest_parameter_walk_forward") is not None, (
        "typed minute walk-forward plan is not implemented"
    )
    return import_module("rquant.minute_backtest_parameter_walk_forward")


def request(**updates: object) -> object:
    protocol = study_protocol()
    body = {
        "templates": (protocol,),
        "calendar_source": protocol.source,
        "calendar_dates": tuple(date(2026, 7, 1) + timedelta(days=i) for i in range(20)),
        "calendar_complete": True,
        "fold_count": 6,
        "min_train_dates": None,
    }
    body.update(updates)
    return wf_api().MinuteParameterWalkForwardRequest.model_validate(body)


def test_all_six_windows_call_the_original_date_owner_once_and_bind_each_complete_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = wf_api()
    payload = request()
    original_owner = topn_walk_forward.build_expanding_folds
    expected = original_owner(
        list(payload.calendar_dates),
        fold_count=6,
        min_train_dates=max(2, len(payload.calendar_dates) // 3),
    )
    calls: list[object] = []

    def same_owner(dates: list[date], *, fold_count: int, min_train_dates: int = 1) -> object:
        calls.append((dates, fold_count, min_train_dates))
        return original_owner(dates, fold_count=fold_count, min_train_dates=min_train_dates)

    monkeypatch.setattr(topn_walk_forward, "build_expanding_folds", same_owner)

    def no_trade_proxy(*args: object, **kwargs: object) -> None:
        pytest.fail("trade-based research cannot substitute the minute runner")

    monkeypatch.setattr(topn_walk_forward, "run_topn_walk_forward", no_trade_proxy)
    plan = api.build_minute_parameter_walk_forward(payload)
    assert calls == [(list(payload.calendar_dates), 6, 6)]
    assert plan.state == "ready" and plan.results_state == "pending"
    assert len(plan.folds) == 6
    for fold, old in zip(plan.folds, expected, strict=True):
        assert fold.fold == old.fold
        assert fold.train_dates == tuple(old.train_dates) and fold.test_dates == tuple(
            old.test_dates
        )
        assert max(fold.train_dates) < min(fold.test_dates)
        protocol = fold.protocols[0]
        assert protocol.source == payload.templates[0].source
        assert protocol.head == payload.templates[0].head
        assert protocol.parameters == payload.templates[0].parameters
        assert protocol.worker_seed == 17 and protocol.score_profile == "v1"
        assert protocol.top_n == 2 and protocol.min_trades == 5
        assert protocol.split.train_start == old.train_dates[0]
        assert protocol.split.train_end == old.train_dates[-1]
        assert protocol.split.test_start == old.test_dates[0]
        assert protocol.split.test_end == old.test_dates[-1]
    assert len({fold.protocols[0].study_id for fold in plan.folds}) == 6
    assert api.MinuteParameterWalkForwardPlan.model_validate_json(plan.model_dump_json()) == plan


def test_missing_calendar_or_insufficient_real_dates_do_not_pad_six_folds() -> None:
    api = wf_api()
    short = request(calendar_dates=tuple(date(2026, 7, 1) + timedelta(days=i) for i in range(4)))
    plan = api.build_minute_parameter_walk_forward(short)
    assert plan.state == "unavailable" and len(plan.folds) == 2
    assert "insufficient_fold_dates" in plan.unavailable_reasons
    incomplete = api.build_minute_parameter_walk_forward(request(calendar_complete=False))
    assert incomplete.state == "unavailable" and incomplete.folds == ()
    assert "incomplete_calendar" in incomplete.unavailable_reasons
    empty = api.build_minute_parameter_walk_forward(request(calendar_dates=()))
    assert empty.state == "unavailable" and empty.folds == ()


@pytest.mark.parametrize(
    "change",
    [
        "source",
        "owner",
        "duplicate_date",
        "outside_window",
        "duplicate_recipe",
        "unknown_field",
        "bad_fold_count",
    ],
)
def test_unknown_detached_or_invalid_plans_are_rejected(change: str) -> None:
    api = wf_api()
    body = request().model_dump(mode="python")
    if change == "source":
        body["calendar_source"]["full_input_hash"] = "f" * 64
    elif change == "owner":
        body["calendar_source"]["owner_id"] = "foreign"
    elif change == "duplicate_date":
        body["calendar_dates"] += body["calendar_dates"][:1]
    elif change == "outside_window":
        body["calendar_dates"] += (date(2026, 8, 4),)
    elif change == "duplicate_recipe":
        body["templates"] += body["templates"]
    elif change == "unknown_field":
        body["test_future_return_pct"] = 999.0
    else:
        body["fold_count"] = True
    with pytest.raises(ValueError):
        api.MinuteParameterWalkForwardRequest.model_validate(body)


def test_auction_and_growth_fold_dates_keep_the_complete_registered_recipe() -> None:
    growth_body = study_protocol().model_dump(mode="python")
    growth = MinuteParameterSet(
        parameters=MinuteGrowthParameters(require_fresh_surge=True, require_board_favor=True)
    )
    growth_body["parameters"] = growth
    growth_body["head"]["definition_id"] = growth.definition_id
    growth_body["head"]["parameter_fingerprint"] = growth.fingerprint
    templates = (auction_study(), study_api().MinuteStudyProtocol.model_validate(growth_body))
    plan = wf_api().build_minute_parameter_walk_forward(request(templates=templates))
    assert plan.state == "ready"
    for fold in plan.folds:
        for protocol, original in zip(fold.protocols, templates, strict=True):
            assert protocol.parameters == original.parameters and protocol.head == original.head
            assert protocol.source.full_input_hash == original.source.full_input_hash
    assert templates[0].parameters.parameters.start_date == "2026-07-01"
    assert templates[0].parameters.parameters.end_date == "2026-08-03"


def test_request_fields_and_calendar_order_bind_the_plan_identity() -> None:
    api = wf_api()
    first = api.build_minute_parameter_walk_forward(request())
    altered = api.build_minute_parameter_walk_forward(request(min_train_dates=7))
    assert first.plan_id != altered.plan_id
    seed = study_protocol(random_seed=18)
    assert (
        first.plan_id != api.build_minute_parameter_walk_forward(request(templates=(seed,))).plan_id
    )
    fewer = api.build_minute_parameter_walk_forward(request(fold_count=5))
    assert first.plan_id != fewer.plan_id and len(fewer.folds) == 5
    unknown = api.build_minute_parameter_walk_forward(request(calendar_complete=False))
    assert first.plan_id != unknown.plan_id


def test_per_fold_training_selection_delegates_to_the_frozen_owner_and_refuses_foreign_fold() -> (
    None
):
    api = wf_api()
    plan = api.build_minute_parameter_walk_forward(request())
    first = plan.folds[0].protocols[0]
    observation = MinuteStudyTrainingObservation(
        study_id=first.study_id,
        source=first.source,
        head=first.head,
        parameter_fingerprint=first.parameters.fingerprint,
        train_start=first.split.train_start,
        train_end=first.split.train_end,
        result_hash="8" * 64,
        available_at=first.requested_at,
        summary={
            "trades": 8,
            "mean_ret_pct": 1.0,
            "win_rate_pct": 60.0,
            "worst_ret_pct": -2.0,
            "gap_stop_rate_pct": 0.0,
        },
    )
    expected = rank_minute_study_training(
        (first,), (observation,), selection_cutoff=first.requested_at
    )
    assert (
        api.select_minute_walk_forward_training(
            plan, fold=1, observations=(observation,), selection_cutoff=first.requested_at
        )
        == expected
    )
    with pytest.raises(ValueError):
        api.select_minute_walk_forward_training(
            plan, fold=2, observations=(observation,), selection_cutoff=first.requested_at
        )
    with pytest.raises(ValueError):
        api.select_minute_walk_forward_training(
            plan,
            fold=1,
            observations=(observation,),
            selection_cutoff=first.requested_at - timedelta(seconds=1),
        )
    invalid = api.build_minute_parameter_walk_forward(request(calendar_complete=False))
    with pytest.raises(ValueError):
        api.select_minute_walk_forward_training(
            invalid, fold=1, observations=(), selection_cutoff=first.requested_at
        )


def test_a_plan_cannot_claim_actual_worker_results() -> None:
    api = wf_api()
    plan = api.build_minute_parameter_walk_forward(request())
    body = plan.model_dump(mode="python")
    body["results_state"] = "sealed"
    with pytest.raises(ValidationError):
        api.MinuteParameterWalkForwardPlan.model_validate(body)


def test_training_cannot_consume_a_relabelled_original_fold() -> None:
    api = wf_api()
    plan = api.build_minute_parameter_walk_forward(request())
    body = plan.model_dump(mode="python")
    body["folds"][0]["train_dates"] = body["folds"][0]["train_dates"][:-1]
    body["folds"][0]["protocols"][0]["split"]["train_end"] = body["folds"][0]["train_dates"][-1]
    detached = api.MinuteParameterWalkForwardPlan.model_validate(body)
    with pytest.raises(ValueError, match="original expanding-window owner"):
        api.select_minute_walk_forward_training(
            detached,
            fold=1,
            observations=(),
            selection_cutoff=plan.request.templates[0].requested_at,
        )
