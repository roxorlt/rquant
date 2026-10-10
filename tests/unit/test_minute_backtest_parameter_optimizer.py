from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module, util
from types import ModuleType
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pydantic import ValidationError
from tests.unit.test_minute_backtest_study_protocols import study_api, study_protocol

from rquant import topn_selection

if TYPE_CHECKING:
    from rquant.minute_backtest_study_protocols import MinuteStudyCandidate, MinuteStudyProtocol


def optimizer_api() -> ModuleType:
    assert util.find_spec("rquant.minute_backtest_parameter_optimizer") is not None, (
        "PIT parameter selection is not implemented"
    )
    return import_module("rquant.minute_backtest_parameter_optimizer")


def study_candidate(
    protocol: MinuteStudyProtocol, code: str = "600000.SH", strength: float = 2.0
) -> MinuteStudyCandidate:
    clock = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    values = {
        "signal_rel_amount_same_minute_20d": strength,
        "signal_rel_cum_amount_asof_20d": strength,
        "signal_amount_accel_5m": strength,
        "signal_amount_accel_10m": strength,
        "accum_obv_change_20d_pct": 20.0,
        "accum_ad_flow_20d_pct": 10.0,
        "accum_up_down_amount_ratio_20d": 1.5,
        "accum_heavy_no_drop_days_20d": 2.0,
        "price_position_90d_pct": 60.0,
        "distance_to_high_90d_pct": 10.0,
        "market_up_ratio_pct": 50.0,
        "index_csi1000_pct_chg": 1.0,
        "ma_alignment": 1.0,
        "price_percentile_250d": 0.1,
        "market_above_ma20_ratio_pct": 35.0,
    }
    return study_api().MinuteStudyCandidate(
        source=protocol.source,
        head=protocol.head,
        parameter_fingerprint=protocol.parameters.fingerprint,
        candidate_id="candidate-" + code,
        ts_code=code,
        trade_date=date(2026, 7, 31),
        event_time=clock,
        available_at=clock + timedelta(seconds=5),
        features=tuple(
            study_api().MinuteStudyFeature(name=name, value=value, available_at=clock)
            for name, value in values.items()
        ),
    )


@pytest.mark.parametrize("profile", topn_selection.available_score_profile_names())
def test_all_eleven_profiles_call_the_original_owner_and_match_every_selected_value(
    profile: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    protocol = study_protocol(score_profile=profile)
    candidates = (
        study_candidate(protocol, "600000.SH", 0.8),
        study_candidate(protocol, "000001.SZ", 4.0),
        study_candidate(protocol, "300001.SZ", 2.0),
    )
    frame = pd.DataFrame(
        [
            {
                **{item.name: item.value for item in row.features},
                "ts_code": row.ts_code,
                "buy_date": row.trade_date,
                "entry_time": row.event_time,
            }
            for row in candidates
        ]
    )
    original_profile = topn_selection.resolve_score_profiles([profile])[0]
    expected = topn_selection.select_topn_by_feature_score(
        frame, top_n=2, score_profile=original_profile
    )
    owner = topn_selection.feature_score
    observed: list[object] = []

    def same_owner(row: pd.Series, score_profile: object = None) -> float:
        observed.append(score_profile)
        return owner(row, score_profile)

    monkeypatch.setattr(topn_selection, "feature_score", same_owner)
    selected = optimizer_api().select_minute_study_candidates(
        protocol, candidates, decision_cutoff=datetime(2026, 7, 31, 1, 32, tzinfo=UTC)
    )
    assert len(observed) == 3
    assert all(value == original_profile for value in observed)
    assert [(row.ts_code, row.feature_score, row.feature_rank) for row in selected] == [
        (row.ts_code, row.feature_score, row.feature_rank) for row in expected.itertuples()
    ]
    assert all(
        row.study_id == protocol.study_id and row.source_hash == protocol.source.full_input_hash
        for row in selected
    )


@pytest.mark.parametrize(
    "change",
    [
        "future_candidate",
        "future_feature",
        "foreign_source",
        "foreign_owner",
        "foreign_head",
        "foreign_parameter",
        "unknown_feature",
        "wrong_day",
        "unknown_required_feature",
    ],
)
def test_future_foreign_or_unknown_facts_do_not_produce_a_partial_success(change: str) -> None:
    protocol = study_protocol()
    row = study_candidate(protocol).model_dump(mode="python")
    if change == "future_candidate":
        row["available_at"] = datetime(2026, 7, 31, 2, tzinfo=UTC)
    elif change == "future_feature":
        row["features"][0]["available_at"] = datetime(2026, 7, 31, 2, tzinfo=UTC)
    elif change == "foreign_source":
        row["source"]["full_input_hash"] = "f" * 64
    elif change == "foreign_owner":
        row["source"]["owner_id"] = "foreign"
    elif change == "foreign_head":
        row["head"]["registration_fingerprint"] = "f" * 64
    elif change == "foreign_parameter":
        row["parameter_fingerprint"] = "f" * 64
    elif change == "unknown_feature":
        row["features"][0]["name"] = "future_return_pct"
    elif change == "wrong_day":
        row["trade_date"] = date(2026, 8, 1)
    else:
        row["features"] = row["features"][1:]
    with pytest.raises((ValueError, ValidationError)):
        invalid = study_api().MinuteStudyCandidate.model_validate(row)
        optimizer_api().select_minute_study_candidates(
            protocol,
            (study_candidate(protocol, "000001.SZ"), invalid),
            decision_cutoff=datetime(2026, 7, 31, 1, 32, tzinfo=UTC),
        )


def test_duplicate_candidate_and_naive_decision_clock_are_rejected() -> None:
    protocol = study_protocol()
    row = study_candidate(protocol)
    with pytest.raises(ValueError):
        optimizer_api().select_minute_study_candidates(
            protocol, (row, row), decision_cutoff=row.available_at
        )
    with pytest.raises(ValueError):
        optimizer_api().select_minute_study_candidates(
            protocol, (row,), decision_cutoff=datetime(2026, 7, 31, 1, 32)
        )


def test_known_missing_optional_values_keep_the_original_math_and_temperature_gate() -> None:
    protocol = study_protocol(score_profile="v2_env_gate")
    data = study_candidate(protocol).model_dump(mode="python")
    for item in data["features"]:
        if item["name"] == "market_above_ma20_ratio_pct":
            item["value"] = 10.0
    cold = study_api().MinuteStudyCandidate.model_validate(data)
    assert (
        optimizer_api().select_minute_study_candidates(
            protocol, (cold,), decision_cutoff=cold.available_at
        )
        == ()
    )
    for item in data["features"]:
        if item["name"] == "market_above_ma20_ratio_pct":
            item["value"] = None
    missing = study_api().MinuteStudyCandidate.model_validate(data)
    selected = optimizer_api().select_minute_study_candidates(
        protocol, (missing,), decision_cutoff=missing.available_at
    )
    expected = topn_selection.feature_score(
        pd.Series({item.name: item.value for item in missing.features}),
        topn_selection.resolve_score_profiles(["v2_env_gate"])[0],
    )
    assert selected[0].feature_score == expected


def test_training_selection_reuses_original_penalty_and_cannot_read_test_winners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import strategy_optimizer

    original_owner = strategy_optimizer._score_row
    calls: list[int] = []

    def same_owner(row: pd.Series, *, min_trades: int) -> float:
        calls.append(min_trades)
        return original_owner(row, min_trades=min_trades)

    monkeypatch.setattr(strategy_optimizer, "_score_row", same_owner)

    first = study_protocol()
    second = study_protocol(score_profile="no_market")
    api = optimizer_api()

    def observation(protocol: MinuteStudyProtocol, mean: float) -> object:
        return api.MinuteStudyTrainingObservation(
            study_id=protocol.study_id,
            source=protocol.source,
            parameter_fingerprint=protocol.parameters.fingerprint,
            head=protocol.head,
            train_start=protocol.split.train_start,
            train_end=protocol.split.train_end,
            result_hash="8" * 64,
            available_at=protocol.requested_at,
            summary={
                "trades": 8,
                "mean_ret_pct": mean,
                "win_rate_pct": 60.0,
                "worst_ret_pct": -2.0,
                "gap_stop_rate_pct": 0.0,
            },
        )

    observations = (observation(first, 1.0), observation(second, 0.5))
    ranked = api.rank_minute_study_training(
        (first, second), observations, selection_cutoff=first.requested_at
    )
    assert calls == [first.min_trades, second.min_trades]
    assert ranked[0].study_id == first.study_id
    expected = original_owner(
        pd.Series(observations[0].summary.model_dump()), min_trades=first.min_trades
    )
    assert ranked[0].training_score == expected
    body = observations[1].model_dump(mode="python")
    body["test_mean_ret_pct"] = 999.0
    with pytest.raises(ValidationError):
        api.MinuteStudyTrainingObservation.model_validate(body)
    body = observations[0].model_dump(mode="python")
    body["train_end"] = first.split.test_end
    with pytest.raises(ValueError):
        api.rank_minute_study_training(
            (first,),
            (api.MinuteStudyTrainingObservation.model_validate(body),),
            selection_cutoff=first.requested_at,
        )


def test_low_sample_and_unavailable_training_facts_cannot_be_a_winner() -> None:
    protocol = study_protocol(min_trades=10)
    api = optimizer_api()
    values = dict(
        study_id=protocol.study_id,
        source=protocol.source,
        parameter_fingerprint=protocol.parameters.fingerprint,
        head=protocol.head,
        train_start=protocol.split.train_start,
        train_end=protocol.split.train_end,
        result_hash="8" * 64,
        available_at=protocol.requested_at,
    )
    few = api.MinuteStudyTrainingObservation(
        **values,
        summary={
            "trades": 1,
            "mean_ret_pct": 50.0,
            "win_rate_pct": 100.0,
            "worst_ret_pct": 50.0,
            "gap_stop_rate_pct": 0.0,
        },
    )
    assert (
        api.rank_minute_study_training((protocol,), (few,), selection_cutoff=protocol.requested_at)
        == ()
    )
    with pytest.raises(ValidationError):
        api.MinuteStudyTrainingObservation(
            **values,
            summary={
                "trades": 20,
                "mean_ret_pct": None,
                "win_rate_pct": 50.0,
                "worst_ret_pct": -2.0,
                "gap_stop_rate_pct": 0.0,
            },
        )


@pytest.mark.parametrize("change", ["future", "source", "owner", "head", "recipe", "unknown_study"])
def test_training_rank_refuses_detached_or_not_yet_available_evidence(change: str) -> None:
    protocol = study_protocol()
    api = optimizer_api()
    body = {
        "study_id": protocol.study_id,
        "source": protocol.source.model_dump(mode="python"),
        "head": protocol.head.model_dump(mode="python"),
        "parameter_fingerprint": protocol.parameters.fingerprint,
        "train_start": protocol.split.train_start,
        "train_end": protocol.split.train_end,
        "result_hash": "8" * 64,
        "available_at": protocol.requested_at,
        "summary": {
            "trades": 8,
            "mean_ret_pct": 1.0,
            "win_rate_pct": 60.0,
            "worst_ret_pct": -2.0,
            "gap_stop_rate_pct": 0.0,
        },
    }
    if change == "future":
        body["available_at"] = protocol.requested_at + timedelta(seconds=1)
    elif change == "source":
        body["source"]["full_input_hash"] = "f" * 64
    elif change == "owner":
        body["source"]["owner_id"] = "foreign"
    elif change == "head":
        body["head"]["registration_fingerprint"] = "f" * 64
    elif change == "recipe":
        body["parameter_fingerprint"] = "f" * 64
    else:
        body["study_id"] = "f" * 64
    with pytest.raises(ValueError):
        api.rank_minute_study_training(
            (protocol,),
            (api.MinuteStudyTrainingObservation.model_validate(body),),
            selection_cutoff=protocol.requested_at,
        )


def test_a_missing_training_result_cannot_leave_a_partial_winner() -> None:
    protocol = study_protocol()
    with pytest.raises(ValueError):
        optimizer_api().rank_minute_study_training(
            (protocol,),
            (),
            selection_cutoff=protocol.requested_at,
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True])
def test_non_numeric_score_observations_are_unknown_not_zero(value: object) -> None:
    with pytest.raises(ValidationError):
        study_api().MinuteStudyFeature(
            name="signal_amount_accel_5m",
            value=value,
            available_at=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        )


def test_empty_window_and_actual_zero_are_distinct() -> None:
    protocol = study_protocol()
    api = optimizer_api()
    assert (
        api.select_minute_study_candidates(
            protocol,
            (),
            decision_cutoff=datetime(2026, 7, 31, 1, 32, tzinfo=UTC),
        )
        == ()
    )
    zero = study_candidate(protocol).model_dump(mode="python")
    for item in zero["features"]:
        item["value"] = 0.0
    candidate = study_api().MinuteStudyCandidate.model_validate(zero)
    result = api.select_minute_study_candidates(
        protocol,
        (candidate,),
        decision_cutoff=candidate.available_at,
    )
    assert len(result) == 1
    assert result[0].feature_score == topn_selection.feature_score(
        pd.Series({item.name: 0.0 for item in candidate.features}),
        topn_selection.resolve_score_profiles([protocol.score_profile])[0],
    )
