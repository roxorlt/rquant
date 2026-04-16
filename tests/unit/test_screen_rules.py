"""筛选积木单测。"""

import pandas as pd

from tests.fixtures.wide_frames import make_wide_frame


class TestFixture:
    def test_default_frame_has_expected_columns(self) -> None:
        df = make_wide_frame(lookback=3)
        assert "CLOSE[0]" in df.columns
        assert "CLOSE[3]" in df.columns
        assert "IS_FIRST_LIMIT_UP[1]" in df.columns
        assert "is_st" in df.columns
        assert "board_type" in df.columns
        assert len(df) == 5

    def test_overrides_apply(self) -> None:
        df = make_wide_frame(
            lookback=1,
            overrides={("300001.SZ", "CLOSE[0]"): 42.0},
        )
        assert df.loc[df["ts_code"] == "300001.SZ", "CLOSE[0]"].iloc[0] == 42.0
        assert df.loc[df["ts_code"] == "000001.SZ", "CLOSE[0]"].iloc[0] == 0.0

    def test_board_type_detection(self) -> None:
        df = make_wide_frame()
        board = dict(zip(df["ts_code"], df["board_type"]))
        assert board["000001.SZ"] == "main"
        assert board["300001.SZ"] == "gem"
        assert board["688001.SH"] == "star"
        assert board["833001.BJ"] == "bj"
