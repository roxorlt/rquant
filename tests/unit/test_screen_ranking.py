from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from rquant.screen.ranking import RankingCondition, rank_screen_results


def test_ranking_uses_weighted_valid_percentiles_and_returns_top_n() -> None:
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
            "momentum": [30.0, 20.0, 10.0],
            "volatility": [3.0, 2.0, 1.0],
        }
    )
    before = frame.copy(deep=True)

    result = rank_screen_results(
        frame,
        [
            RankingCondition("momentum", ascending=False, weight=2),
            RankingCondition("volatility", ascending=True, weight=1),
        ],
        top_n=2,
    )

    assert result["ts_code"].tolist() == ["000001.SZ", "000002.SZ"]
    assert result["ranking_score"].tolist() == pytest.approx([700 / 9, 200 / 3])
    pd.testing.assert_frame_equal(frame, before)


def test_ties_receive_average_percentile_and_stock_code_breaks_final_tie() -> None:
    frame = pd.DataFrame(
        {"ts_code": ["000002.SZ", "000003.SZ", "000001.SZ"], "metric": [10, 0, 10]}
    )
    condition = RankingCondition("metric", ascending=False, weight=1)

    forward = rank_screen_results(frame, [condition], top_n=3)
    reversed_result = rank_screen_results(frame.iloc[::-1], [condition], top_n=3)

    assert forward["ts_code"].tolist() == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert forward["ranking_score"].tolist() == pytest.approx([250 / 3, 250 / 3, 100 / 3])
    pd.testing.assert_frame_equal(forward, reversed_result)


def test_nonfinite_values_score_zero_and_rank_after_finite_values() -> None:
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
            "metric": [np.nan, np.inf, -np.inf, 10.0],
        }
    )

    result = rank_screen_results(
        frame, [RankingCondition("metric", ascending=False, weight=1)], top_n=4
    )

    assert result["ts_code"].tolist() == ["000004.SZ", "000001.SZ", "000002.SZ", "000003.SZ"]
    assert result["ranking_score"].tolist() == pytest.approx([100, 0, 0, 0])


def test_fewer_missing_positive_weight_metrics_break_equal_score_ties() -> None:
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
            "first": [np.nan, 10.0, 20.0],
            "second": [20.0, 10.0, np.nan],
        }
    )

    result = rank_screen_results(
        frame,
        [
            RankingCondition("first", ascending=False, weight=1),
            RankingCondition("second", ascending=False, weight=1),
        ],
        top_n=3,
    )

    assert result["ranking_score"].tolist() == pytest.approx([50, 50, 50])
    assert result["ts_code"].tolist() == ["000002.SZ", "000001.SZ", "000003.SZ"]


def test_complete_stock_precedes_higher_scoring_stock_with_missing_metric() -> None:
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "momentum": [100.0, 10.0],
            "valuation": [np.nan, 5.0],
        }
    )

    result = rank_screen_results(
        frame,
        [
            RankingCondition("momentum", ascending=False, weight=9),
            RankingCondition("valuation", ascending=False, weight=1),
        ],
        top_n=1,
    )

    assert result["ts_code"].tolist() == ["000002.SZ"]
    assert result["ranking_score"].tolist() == pytest.approx([55])


def test_weights_are_proportional_and_zero_weight_does_not_affect_ties() -> None:
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "metric": [10.0, 10.0],
            "ignored": [np.nan, 5.0],
        }
    )

    result = rank_screen_results(
        frame,
        [
            RankingCondition("metric", ascending=False, weight=10),
            RankingCondition("ignored", ascending=False, weight=0),
        ],
        top_n=5,
    )

    assert result["ts_code"].tolist() == ["000001.SZ", "000002.SZ"]
    assert result["ranking_score"].tolist() == pytest.approx([75, 75])


@pytest.mark.parametrize(
    ("conditions", "message"),
    [
        ([], "condition"),
        ([RankingCondition("metric", ascending=False, weight=0)], "positive"),
        ([RankingCondition("metric", ascending=False, weight=-1)], "weight"),
        ([RankingCondition("metric", ascending=False, weight=np.nan)], "weight"),
        ([RankingCondition("metric", ascending=False, weight=np.inf)], "weight"),
        (
            [RankingCondition("metric", False, 1), RankingCondition("metric", True, 1)],
            "duplicate",
        ),
        ([RankingCondition("unknown", ascending=False, weight=1)], "unknown"),
    ],
)
def test_invalid_conditions_have_actionable_errors(
    conditions: list[RankingCondition], message: str
) -> None:
    frame = pd.DataFrame({"ts_code": ["000001.SZ"], "metric": [1.0]})

    with pytest.raises(ValueError, match=message):
        rank_screen_results(frame, conditions, top_n=1)


@pytest.mark.parametrize("top_n", [0, -1, 1.5, True])
def test_top_n_must_be_a_positive_integer(top_n: object) -> None:
    frame = pd.DataFrame({"ts_code": ["000001.SZ"], "metric": [1.0]})

    with pytest.raises(ValueError, match="top_n"):
        rank_screen_results(frame, [RankingCondition("metric", False, 1)], top_n=top_n)


def test_duplicate_codes_are_rejected() -> None:
    frame = pd.DataFrame({"ts_code": ["000001.SZ", "000001.SZ"], "metric": [1, 2]})

    with pytest.raises(ValueError, match="duplicate.*ts_code"):
        rank_screen_results(frame, [RankingCondition("metric", False, 1)], top_n=1)


def test_duplicate_frame_columns_are_rejected() -> None:
    frame = pd.DataFrame([["000001.SZ", 1, 2]], columns=["ts_code", "metric", "metric"])

    with pytest.raises(ValueError, match="duplicate.*column"):
        rank_screen_results(frame, [RankingCondition("metric", False, 1)], top_n=1)


@pytest.mark.parametrize("metric", [["high", "low"], [True, False]])
def test_nonnumeric_metrics_are_rejected(metric: list[object]) -> None:
    frame = pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "metric": metric})

    with pytest.raises(ValueError, match="numeric.*metric"):
        rank_screen_results(frame, [RankingCondition("metric", False, 1)], top_n=1)


def test_empty_screen_result_preserves_columns_and_adds_score() -> None:
    frame = pd.DataFrame(columns=["ts_code", "metric"])

    result = rank_screen_results(frame, [RankingCondition("metric", False, 1)], top_n=3)

    assert result.empty
    assert result.columns.tolist() == ["ts_code", "metric", "ranking_score"]
