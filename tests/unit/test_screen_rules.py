"""筛选积木单测。"""

import pytest

from rquant.screen.rules import (
    above_ma,
    between,
    board_in,
    consecutive_ups_gte,
    cross_above,
    cross_below,
    first_limit_up,
    gt,
    gte,
    limit_down,
    limit_up,
    lt,
    lte,
    not_bj,
    not_limit_up,
    not_st,
    rsi_overbought,
    rsi_oversold,
    volume_ratio_gte,
    yiziban,
)
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
        board = dict(zip(df["ts_code"], df["board_type"], strict=True))
        assert board["000001.SZ"] == "main"
        assert board["300001.SZ"] == "gem"
        assert board["688001.SH"] == "star"
        assert board["833001.BJ"] == "bj"


class TestAttributeRules:
    def test_not_st_excludes_st_stocks(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "is_st"): True})
        mask = not_st()(df)
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_not_bj_excludes_bj_stocks(self) -> None:
        df = make_wide_frame()
        mask = not_bj()(df)
        assert not mask.loc[df["ts_code"] == "833001.BJ"].iloc[0]
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    @pytest.mark.parametrize(
        "whitelist,expected_allowed,expected_blocked",
        [
            (["main"], "000001.SZ", "300001.SZ"),
            (["main", "gem"], "300001.SZ", "688001.SH"),
            (["bj"], "833001.BJ", "000001.SZ"),
        ],
    )
    def test_board_in_whitelist(
        self, whitelist: list[str], expected_allowed: str, expected_blocked: str
    ) -> None:
        df = make_wide_frame()
        mask = board_in(whitelist)(df)
        assert mask.loc[df["ts_code"] == expected_allowed].iloc[0]
        assert not mask.loc[df["ts_code"] == expected_blocked].iloc[0]

    def test_attribute_rules_min_lookback_is_zero(self) -> None:
        assert not_st().min_lookback == 0
        assert not_bj().min_lookback == 0
        assert board_in(["main"]).min_lookback == 0


class TestLimitRules:
    def test_limit_up_today(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "IS_LIMIT_UP[0]"): True})
        mask = limit_up(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_limit_up_yesterday(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={("300001.SZ", "IS_LIMIT_UP[1]"): True},
        )
        mask = limit_up(offset=1)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert limit_up(offset=1).min_lookback == 1

    def test_not_limit_up(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "IS_LIMIT_UP[0]"): True})
        mask = not_limit_up(offset=0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_first_limit_up(self) -> None:
        df = make_wide_frame(
            overrides={("300001.SZ", "IS_FIRST_LIMIT_UP[0]"): True},
        )
        mask = first_limit_up(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_yiziban(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "IS_YIZIBAN[0]"): True})
        mask = yiziban(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_consecutive_ups_gte(self) -> None:
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "CONSECUTIVE_LIMIT_UPS[0]"): 2,
                ("000001.SZ", "CONSECUTIVE_LIMIT_UPS[0]"): 1,
            }
        )
        mask = consecutive_ups_gte(2)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_limit_down(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "IS_LIMIT_DOWN[0]"): True})
        mask = limit_down(offset=0)(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]


class TestCompareRules:
    def test_gt_field_vs_field_cross_day(self) -> None:
        # 300001.SZ 今高 > 昨收
        df = make_wide_frame(
            lookback=2,
            overrides={
                ("300001.SZ", "HIGH[0]"): 12.0,
                ("300001.SZ", "CLOSE[1]"): 10.0,
                ("000001.SZ", "HIGH[0]"): 10.0,
                ("000001.SZ", "CLOSE[1]"): 12.0,
            },
        )
        rule = gt("HIGH[0]", "CLOSE[1]")
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_gt_field_vs_constant(self) -> None:
        df = make_wide_frame(
            overrides={("300001.SZ", "CLOSE[0]"): 15.0, ("000001.SZ", "CLOSE[0]"): 5.0},
        )
        rule = gt("CLOSE[0]", 10.0)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 0

    def test_lt(self) -> None:
        df = make_wide_frame(
            overrides={("000001.SZ", "CLOSE[0]"): 5.0, ("300001.SZ", "CLOSE[0]"): 15.0},
        )
        mask = lt("CLOSE[0]", 10.0)(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_gte_boundary(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "CLOSE[0]"): 10.0})
        assert gte("CLOSE[0]", 10.0)(df).loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_lte_boundary(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "CLOSE[0]"): 10.0})
        assert lte("CLOSE[0]", 10.0)(df).loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_between(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "CLOSE[0]"): 8.0,
                ("300001.SZ", "CLOSE[0]"): 15.0,
                ("688001.SH", "CLOSE[0]"): 25.0,
            },
        )
        mask = between("CLOSE[0]", 10.0, 20.0)(df)
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "688001.SH"].iloc[0]


class TestIndicatorRules:
    def test_cross_above_today(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={
                # 300001 今日上穿：今 MA5 > MA20，昨 MA5 <= MA20
                ("300001.SZ", "MA5[0]"): 12.0,
                ("300001.SZ", "MA20[0]"): 10.0,
                ("300001.SZ", "MA5[1]"): 9.0,
                ("300001.SZ", "MA20[1]"): 10.0,
                # 000001 未上穿：昨天已经在上方
                ("000001.SZ", "MA5[0]"): 12.0,
                ("000001.SZ", "MA20[0]"): 10.0,
                ("000001.SZ", "MA5[1]"): 11.0,
                ("000001.SZ", "MA20[1]"): 10.0,
            },
        )
        rule = cross_above("MA5", "MA20")
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_cross_below_today(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={
                ("000001.SZ", "MA5[0]"): 9.0,
                ("000001.SZ", "MA20[0]"): 10.0,
                ("000001.SZ", "MA5[1]"): 11.0,
                ("000001.SZ", "MA20[1]"): 10.0,
            },
        )
        mask = cross_below("MA5", "MA20")(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_above_ma(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "CLOSE[0]"): 15.0,
                ("000001.SZ", "MA20[0]"): 10.0,
                ("300001.SZ", "CLOSE[0]"): 8.0,
                ("300001.SZ", "MA20[0]"): 10.0,
            },
        )
        mask = above_ma(period=20)(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_rsi_oversold(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "RSI14[0]"): 25.0,
                ("300001.SZ", "RSI14[0]"): 50.0,
            }
        )
        mask = rsi_oversold()(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_rsi_overbought(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "RSI14[0]"): 75.0})
        mask = rsi_overbought()(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]


class TestVolumeRules:
    def test_volume_ratio_gte(self) -> None:
        df = make_wide_frame(
            lookback=5,
            overrides={
                # 今量 100 / 前 5 日均量 10 = 10 倍
                ("300001.SZ", "VOL[0]"): 100.0,
                ("300001.SZ", "VOL[1]"): 10.0,
                ("300001.SZ", "VOL[2]"): 10.0,
                ("300001.SZ", "VOL[3]"): 10.0,
                ("300001.SZ", "VOL[4]"): 10.0,
                ("300001.SZ", "VOL[5]"): 10.0,
                # 000001 今量 = 前 5 日均量，ratio = 1
                ("000001.SZ", "VOL[0]"): 10.0,
                ("000001.SZ", "VOL[1]"): 10.0,
                ("000001.SZ", "VOL[2]"): 10.0,
                ("000001.SZ", "VOL[3]"): 10.0,
                ("000001.SZ", "VOL[4]"): 10.0,
                ("000001.SZ", "VOL[5]"): 10.0,
            },
        )
        rule = volume_ratio_gte(2.0)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 5
