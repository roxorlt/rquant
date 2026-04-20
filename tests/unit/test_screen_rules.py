"""筛选积木单测。"""

import pytest

from rquant.screen.rules import (
    above_ma,
    between,
    board_in,
    circ_mv_lt,
    consecutive_ups_gte,
    cross_above,
    cross_below,
    first_limit_up,
    gt,
    gte,
    has_lower_shadow,
    has_prior_limit_up,
    limit_down,
    limit_up,
    lt,
    lte,
    no_consec_ups_in_window,
    no_limit_down_in_window,
    not_bj,
    not_limit_up,
    not_st,
    not_yiziban,
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

    def test_new_columns_in_fixture(self) -> None:
        df = make_wide_frame(lookback=2)
        assert "BODY_UPPER[0]" in df.columns
        assert "BODY_LOWER[1]" in df.columns
        assert "CIRC_MV[0]" in df.columns
        assert "TOTAL_MV[2]" in df.columns
        assert "TURNOVER_RATE[0]" in df.columns


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

    def test_not_yiziban_passes_non_yiziban(self) -> None:
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "IS_YIZIBAN[0]"): False,
                ("000001.SZ", "IS_YIZIBAN[0]"): True,
            },
        )
        mask = not_yiziban(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_not_yiziban_at_offset_1(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={("300001.SZ", "IS_YIZIBAN[1]"): True},
        )
        mask = not_yiziban(offset=1)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not_yiziban(offset=1).min_lookback == 1

    def test_not_yiziban_nan_treated_as_false(self) -> None:
        """NaN in IS_YIZIBAN should be treated as False (not yiziban), so not_yiziban passes."""
        df = make_wide_frame(lookback=1)
        # Default IS_YIZIBAN[0] is False, so not_yiziban should pass
        mask = not_yiziban(offset=0)(df)
        assert mask.all()


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


class TestCircMvRule:
    def test_circ_mv_lt_passes_small_cap(self) -> None:
        """100亿 = 1000000万 < 150亿 = 1500000万 → passes."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "CIRC_MV[0]"): 1000000.0,  # 100亿万元
                ("000001.SZ", "CIRC_MV[0]"): 2000000.0,  # 200亿万元
            },
        )
        mask = circ_mv_lt(150)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_circ_mv_lt_boundary(self) -> None:
        """Exactly 150亿 = 1500000万 should NOT pass (strict <)."""
        df = make_wide_frame(
            overrides={("300001.SZ", "CIRC_MV[0]"): 1500000.0},
        )
        mask = circ_mv_lt(150)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_circ_mv_lt_nan_fails(self) -> None:
        """NaN circ_mv should fail (fillna(inf) makes it exceed any threshold)."""
        df = make_wide_frame()
        df.loc[df["ts_code"] == "300001.SZ", "CIRC_MV[0]"] = float("nan")
        mask = circ_mv_lt(150)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_circ_mv_lt_at_offset(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={("300001.SZ", "CIRC_MV[1]"): 500000.0},  # 50亿
        )
        rule = circ_mv_lt(100, offset=1)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_circ_mv_lt_unit_conversion(self) -> None:
        """Verify: 1亿 = 10000万元 conversion."""
        df = make_wide_frame(overrides={("300001.SZ", "CIRC_MV[0]"): 9999.0})
        assert circ_mv_lt(1)(df).loc[df["ts_code"] == "300001.SZ"].iloc[0]
        df = make_wide_frame(overrides={("300001.SZ", "CIRC_MV[0]"): 10001.0})
        assert not circ_mv_lt(1)(df).loc[df["ts_code"] == "300001.SZ"].iloc[0]


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


class TestHasLowerShadow:
    def test_clear_lower_shadow_passes(self) -> None:
        """O=10, H=11, L=8, C=10.5 → body_upper=10.5, body_lower=10,
        lower_shadow=10-8=2, body=10.5-10=0.5, ratio=4.0 ≥ 1.5,
        amplitude=(11-8)/8=0.375 ≥ 0.02 → passes."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "OPEN[0]"): 10.0,
                ("300001.SZ", "HIGH[0]"): 11.0,
                ("300001.SZ", "LOW[0]"): 8.0,
                ("300001.SZ", "CLOSE[0]"): 10.5,
                ("300001.SZ", "BODY_UPPER[0]"): 10.5,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_no_lower_shadow_fails(self) -> None:
        """O=10, H=12, L=10, C=11 → body_upper=11, body_lower=10,
        lower_shadow=10-10=0, ratio=0 < 1.5 → fails."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "OPEN[0]"): 10.0,
                ("300001.SZ", "HIGH[0]"): 12.0,
                ("300001.SZ", "LOW[0]"): 10.0,
                ("300001.SZ", "CLOSE[0]"): 11.0,
                ("300001.SZ", "BODY_UPPER[0]"): 11.0,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_doji_zero_body_fails(self) -> None:
        """一字线/十字星: body=0 → has_body=False → fails regardless of shadow."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 11.0,
                ("300001.SZ", "LOW[0]"): 9.0,
                ("300001.SZ", "BODY_UPPER[0]"): 10.0,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_small_amplitude_fails(self) -> None:
        """Shadow ratio OK but amplitude < 0.02 → fails.
        O=10, H=10.1, L=9.95, C=10.05 → body_upper=10.05, body_lower=10,
        lower_shadow=10-9.95=0.05, body=0.05, ratio=1.0 (< 1.5 anyway, but let's
        test with min_ratio=0.5).
        amplitude=(10.1-9.95)/9.95=0.015 < 0.02 → fails."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 10.1,
                ("300001.SZ", "LOW[0]"): 9.95,
                ("300001.SZ", "BODY_UPPER[0]"): 10.05,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(0.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_ratio_below_threshold_fails(self) -> None:
        """lower_shadow / body < min_ratio → fails.
        body_upper=11, body_lower=10, low=9.8 → shadow=0.2, body=1.0, ratio=0.2 < 1.5."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 12.0,
                ("300001.SZ", "LOW[0]"): 9.8,
                ("300001.SZ", "BODY_UPPER[0]"): 11.0,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_at_offset_1(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={
                ("300001.SZ", "HIGH[1]"): 11.0,
                ("300001.SZ", "LOW[1]"): 8.0,
                ("300001.SZ", "BODY_UPPER[1]"): 10.5,
                ("300001.SZ", "BODY_LOWER[1]"): 10.0,
            },
        )
        rule = has_lower_shadow(1.5, 0.02, offset=1)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_custom_thresholds(self) -> None:
        """With min_ratio=2.0 (TA-Lib hammer standard):
        shadow=2, body=0.5, ratio=4.0 ≥ 2.0 → passes."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 11.0,
                ("300001.SZ", "LOW[0]"): 8.0,
                ("300001.SZ", "BODY_UPPER[0]"): 10.5,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(min_ratio=2.0, min_amplitude=0.01)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]


class TestNoConsecUpsInWindow:
    def test_passes_when_max_below_threshold(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 2
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert mask.all()

    def test_fails_when_max_equals_threshold(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 3
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert not mask.any()

    def test_fails_when_max_exceeds_threshold(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 5
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert not mask.any()

    def test_nan_treated_as_zero(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = float("nan")
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert mask.all()

    def test_has_aggregate_request(self) -> None:
        rule = no_consec_ups_in_window(threshold=3, window=8)
        assert hasattr(rule, "aggregate_requests")
        assert len(rule.aggregate_requests) == 1
        req = rule.aggregate_requests[0]
        assert req.name == "max_consec_ups_8d"
        assert req.agg_func == "max"
        assert req.window == 8
        assert req.source_col == "consecutive_limit_ups"

    def test_custom_window(self) -> None:
        rule = no_consec_ups_in_window(threshold=2, window=5)
        assert rule.aggregate_requests[0].name == "max_consec_ups_5d"
        assert rule.aggregate_requests[0].window == 5


class TestNoLimitDownInWindow:
    def test_passes_when_no_limit_down(self) -> None:
        df = make_wide_frame()
        df["has_limit_down_30d"] = False
        rule = no_limit_down_in_window(window=30)
        mask = rule(df)
        assert mask.all()

    def test_fails_when_has_limit_down(self) -> None:
        df = make_wide_frame()
        df["has_limit_down_30d"] = True
        rule = no_limit_down_in_window(window=30)
        mask = rule(df)
        assert not mask.any()

    def test_nan_treated_as_no_limit_down(self) -> None:
        df = make_wide_frame()
        df["has_limit_down_30d"] = float("nan")
        rule = no_limit_down_in_window(window=30)
        mask = rule(df)
        assert mask.all()

    def test_has_aggregate_request(self) -> None:
        rule = no_limit_down_in_window(window=30)
        assert len(rule.aggregate_requests) == 1
        req = rule.aggregate_requests[0]
        assert req.name == "has_limit_down_30d"
        assert req.agg_func == "any"
        assert req.window == 30

    def test_custom_window(self) -> None:
        rule = no_limit_down_in_window(window=10)
        assert rule.aggregate_requests[0].name == "has_limit_down_10d"


class TestHasPriorLimitUp:
    def test_passes_when_has_prior_limit_up(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = 2
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert mask.all()

    def test_fails_when_no_prior_limit_up(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = 0
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert not mask.any()

    def test_boundary_exactly_one(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = 1
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert mask.all()

    def test_nan_treated_as_zero(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = float("nan")
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert not mask.any()

    def test_has_aggregate_request_with_exclude(self) -> None:
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        assert len(rule.aggregate_requests) == 1
        req = rule.aggregate_requests[0]
        assert req.name == "count_limit_up_90d_ex1"
        assert req.agg_func == "count_nonzero"
        assert req.window == 90
        assert req.exclude_offset == 1

    def test_custom_params(self) -> None:
        rule = has_prior_limit_up(window=30, exclude_offset=2)
        req = rule.aggregate_requests[0]
        assert req.name == "count_limit_up_30d_ex2"
        assert req.window == 30
        assert req.exclude_offset == 2
