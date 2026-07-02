"""派生状态字段单测：涨停分档/首板/一字板/连板/实体上下沿/ST 判断。"""

from datetime import date

import pandas as pd
import pytest

from rquant.state.derive import (
    _classify_board,
    _detect_st,
    _limit_pct,
    derive_state,
)


class TestBoardClassify:
    @pytest.mark.parametrize(
        "ts_code,expected",
        [
            ("000001.SZ", "main"),
            ("600519.SH", "main"),
            ("601988.SH", "main"),
            ("002594.SZ", "main"),
            ("300750.SZ", "gem"),
            ("301089.SZ", "gem"),
            ("688981.SH", "star"),
            ("689009.SH", "star"),
            ("833533.BJ", "bj"),
            ("920002.BJ", "bj"),
        ],
    )
    def test_classify(self, ts_code: str, expected: str) -> None:
        assert _classify_board(ts_code) == expected


class TestDetectST:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("贵州茅台", False),
            ("平安银行", False),
            ("ST 康美", True),
            ("ST康美", True),
            ("*ST 金洲", True),
            ("*ST金洲", True),
            ("SST 某某", True),
            ("", False),
            (None, False),
            # 退市票 join 不到 stock_basic，名字是 NaN（float）——7/2 长区间回补真实崩过
            (float("nan"), False),
        ],
    )
    def test_detect(self, name: str | None, expected: bool) -> None:
        assert _detect_st(name) is expected


class TestLimitPct:
    def test_st_main(self) -> None:
        assert _limit_pct(is_st=True, board_type="main") == 0.05

    def test_main_normal(self) -> None:
        assert _limit_pct(is_st=False, board_type="main") == 0.10

    def test_gem(self) -> None:
        assert _limit_pct(is_st=False, board_type="gem") == 0.20

    def test_star(self) -> None:
        assert _limit_pct(is_st=False, board_type="star") == 0.20

    def test_bj(self) -> None:
        assert _limit_pct(is_st=False, board_type="bj") == 0.30

    def test_st_beats_board(self) -> None:
        # ST 无论板块都是 5%（MVP 简化）
        assert _limit_pct(is_st=True, board_type="gem") == 0.05


class TestDeriveState:
    def _mkbar(
        self,
        d: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        pre_close: float,
    ) -> dict:
        return {
            "trade_date": date.fromisoformat(d),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "pre_close": pre_close,
        }

    def test_empty_returns_empty(self) -> None:
        assert derive_state(pd.DataFrame(), "000001.SZ").empty

    def test_main_board_limit_up(self) -> None:
        df = pd.DataFrame(
            [
                # pre_close 10.0 → limit_up_price = 11.00
                self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 11.0, 10.0),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        assert out.iloc[0]["limit_pct"] == 0.10
        assert out.iloc[0]["limit_up_price"] == 11.00
        assert out.iloc[0]["limit_down_price"] == 9.00
        assert bool(out.iloc[0]["is_limit_up"])
        assert out.iloc[0]["board_type"] == "main"

    def test_gem_20pct_limit(self) -> None:
        df = pd.DataFrame(
            [
                # pre_close 100 → limit_up_price = 120
                self._mkbar("2024-01-02", 110.0, 120.0, 108.0, 120.0, 100.0),
            ]
        )
        out = derive_state(df, "300750.SZ", "宁德时代")
        assert out.iloc[0]["limit_pct"] == 0.20
        assert out.iloc[0]["limit_up_price"] == 120.0
        assert bool(out.iloc[0]["is_limit_up"])

    def test_bj_30pct_limit(self) -> None:
        df = pd.DataFrame(
            [
                # pre_close 10 → limit_up_price = 13.00
                self._mkbar("2024-01-02", 11.0, 13.0, 10.5, 13.0, 10.0),
            ]
        )
        out = derive_state(df, "833533.BJ", "某股")
        assert out.iloc[0]["limit_pct"] == 0.30
        assert out.iloc[0]["limit_up_price"] == 13.00
        assert bool(out.iloc[0]["is_limit_up"])

    def test_st_5pct_limit(self) -> None:
        df = pd.DataFrame(
            [
                # pre_close 10 → limit_up_price = 10.50（ST 5%）
                self._mkbar("2024-01-02", 10.2, 10.5, 10.0, 10.5, 10.0),
            ]
        )
        out = derive_state(df, "000408.SZ", "*ST藏格")
        assert out.iloc[0]["limit_pct"] == 0.05
        assert out.iloc[0]["limit_up_price"] == 10.50
        assert bool(out.iloc[0]["is_st"])
        assert bool(out.iloc[0]["is_limit_up"])

    def test_limit_price_round_half_up(self) -> None:
        # ROUND_HALF_UP 语义：pre=3.30 × 1.05 = 3.465 必须进位到 3.47
        # pandas.round(2) 会用银行家舍入给出 3.46，这里验证我们用的是 HALF_UP
        df = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 3.40, 3.47, 3.35, 3.47, 3.30),
            ]
        )
        out = derive_state(df, "000004.SZ", "*ST国华")
        assert out.iloc[0]["limit_up_price"] == 3.47  # 不是 3.46
        assert out.iloc[0]["limit_down_price"] == 3.14  # 3.30 × 0.95 = 3.135 → 3.14

    def test_limit_up_with_tolerance(self) -> None:
        # close 比 limit_up_price 低 0.005（小于容差 0.01）应识别为涨停
        df = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 10.995, 10.0),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        assert bool(out.iloc[0]["is_limit_up"])

    def test_limit_up_beyond_tolerance(self) -> None:
        # close 比 limit_up_price 低 0.02（大于容差 0.01）不算涨停
        df = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 10.98, 10.0),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        assert not bool(out.iloc[0]["is_limit_up"])

    def test_first_limit_up_flag(self) -> None:
        df = pd.DataFrame(
            [
                # day1: 未涨停
                self._mkbar("2024-01-02", 10.0, 10.5, 9.9, 10.3, 10.0),
                # day2: 涨停（首板，昨日未涨）
                self._mkbar("2024-01-03", 10.4, 11.33, 10.3, 11.33, 10.3),
                # day3: 涨停（连板 2）
                self._mkbar("2024-01-04", 11.5, 12.46, 11.4, 12.46, 11.33),
                # day4: 未涨停
                self._mkbar("2024-01-05", 12.5, 12.6, 12.0, 12.2, 12.46),
                # day5: 涨停（首板，因 day4 未涨）
                self._mkbar("2024-01-06", 12.3, 13.42, 12.2, 13.42, 12.2),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        first = out["is_first_limit_up"].tolist()
        assert first == [False, True, False, False, True]

    def test_yiziban_detect(self) -> None:
        df = pd.DataFrame(
            [
                # 一字板：4 价完全相等 且 涨停
                self._mkbar("2024-01-02", 11.0, 11.0, 11.0, 11.0, 10.0),
                # 非一字板：虽然涨停但开盘不等于最高
                self._mkbar("2024-01-03", 11.5, 12.10, 11.5, 12.10, 11.0),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        assert bool(out.iloc[0]["is_yiziban"])
        assert not bool(out.iloc[1]["is_yiziban"])

    def test_consecutive_limit_ups(self) -> None:
        df = pd.DataFrame(
            [
                # 连 3 板
                self._mkbar("2024-01-02", 10.4, 11.0, 10.3, 11.0, 10.0),  # 1 板
                self._mkbar("2024-01-03", 11.5, 12.10, 11.4, 12.10, 11.0),  # 2 板
                self._mkbar("2024-01-04", 12.2, 13.31, 12.1, 13.31, 12.10),  # 3 板
                # 断开
                self._mkbar("2024-01-05", 13.0, 13.5, 12.5, 12.8, 13.31),  # 0
                # 新起 2 板
                self._mkbar("2024-01-08", 13.0, 14.08, 12.9, 14.08, 12.8),  # 1 板
                self._mkbar("2024-01-09", 14.2, 15.49, 14.1, 15.49, 14.08),  # 2 板
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        assert out["consecutive_limit_ups"].tolist() == [1, 2, 3, 0, 1, 2]

    def test_limit_down(self) -> None:
        df = pd.DataFrame(
            [
                # pre_close 10 → limit_down_price = 9.00
                self._mkbar("2024-01-02", 9.5, 9.6, 9.0, 9.0, 10.0),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        assert bool(out.iloc[0]["is_limit_down"])
        assert out.iloc[0]["limit_down_price"] == 9.00

    def test_body_upper_lower(self) -> None:
        df = pd.DataFrame(
            [
                # 阳线：open 低 close 高
                self._mkbar("2024-01-02", 10.0, 10.8, 9.9, 10.5, 10.0),
                # 阴线：open 高 close 低
                self._mkbar("2024-01-03", 10.6, 10.7, 10.0, 10.1, 10.5),
            ]
        )
        out = derive_state(df, "600519.SH", "贵州茅台")
        # 阳线：上沿=close=10.5, 下沿=open=10.0
        assert out.iloc[0]["body_upper"] == 10.5
        assert out.iloc[0]["body_lower"] == 10.0
        # 阴线：上沿=open=10.6, 下沿=close=10.1
        assert out.iloc[1]["body_upper"] == 10.6
        assert out.iloc[1]["body_lower"] == 10.1
