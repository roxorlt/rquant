from __future__ import annotations

import pandas as pd
import pytest


@pytest.mark.parametrize(
    ("table_name", "metrics"),
    [
        (
            "rankings",
            {"robust_score": 1.0, "test_trades": 2, "train_trades": 2},
        ),
        (
            "topn_rankings",
            {"robust_score": 1.0, "test_trades": 2, "train_trades": 2},
        ),
        (
            "walk_forward_rankings",
            {"robust_score": 1.0, "folds": 2, "test_trades": 2},
        ),
    ],
)
def test_rankings_use_candidate_id_as_stable_final_tie_break(
    table_name: str,
    metrics: dict[str, object],
) -> None:
    from rquant.strategy_ranking import rank_strategy_table

    frame = pd.DataFrame(
        [
            {"rank": 1, "candidate_id": "z-candidate", **metrics},
            {"rank": 2, "candidate_id": "a-candidate", **metrics},
        ]
    )

    forward = rank_strategy_table(frame, table_name=table_name)
    reversed_result = rank_strategy_table(
        frame.iloc[::-1].reset_index(drop=True),
        table_name=table_name,
    )

    pd.testing.assert_frame_equal(forward, reversed_result)
    assert forward["candidate_id"].tolist() == ["a-candidate", "z-candidate"]
    assert forward["rank"].tolist() == [1, 2]
