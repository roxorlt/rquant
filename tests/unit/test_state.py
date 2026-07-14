"""派生状态字段单测：逐日 PIT ST 状态、涨跌停和 nullable 连板链。"""

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.state.derive import (
    DailyStateSeed,
    _classify_board,
    _limit_pct,
    derive_state,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


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

    @pytest.mark.parametrize(
        ("board_type", "expected"),
        [("gem", 0.20), ("star", 0.20), ("bj", 0.30)],
    )
    def test_live_growth_boards_do_not_use_main_board_st_limit(
        self,
        board_type: str,
        expected: float,
    ) -> None:
        assert _limit_pct(is_st=True, board_type=board_type) == expected


class TestDeriveState:
    def _mkbar(
        self,
        d: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        pre_close: float,
    ) -> dict[str, object]:
        return {
            "trade_date": date.fromisoformat(d),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "pre_close": pre_close,
        }

    def _status(
        self,
        ts_code: str,
        dates: list[date],
        *,
        is_st: bool = False,
        name: str | None = None,
    ) -> pd.DataFrame:
        resolved_name = name or ("*ST测试" if is_st else "普通测试")
        return pd.DataFrame(
            {
                "ts_code": ts_code,
                "trade_date": dates,
                "name": resolved_name,
                "is_st": pd.Series([is_st] * len(dates), dtype="boolean"),
                "available_at": [
                    datetime.combine(d, time(9, 25), tzinfo=SHANGHAI) for d in dates
                ],
                "conflict_reason": [None] * len(dates),
            }
        )

    def _derive(
        self,
        bars: pd.DataFrame,
        ts_code: str,
        *,
        is_st: bool = False,
        name: str | None = None,
    ) -> pd.DataFrame:
        bars_with_listing = self._with_listing_window(bars)
        dates = bars_with_listing["trade_date"].tolist()
        return derive_state(
            bars_with_listing,
            ts_code,
            self._status(ts_code, dates, is_st=is_st, name=name),
        )

    def _with_listing_window(
        self,
        bars: pd.DataFrame,
        *,
        list_date: date = date(1990, 1, 2),
        fifth_trading_date: date | None = date(1990, 1, 8),
    ) -> pd.DataFrame:
        result = bars.copy()
        result["list_date"] = list_date
        result["fifth_listing_trade_date"] = fifth_trading_date
        return result

    def _with_pretrade_dates(
        self,
        bars: pd.DataFrame,
        expected: list[date | None],
    ) -> pd.DataFrame:
        result = bars.copy()
        result["expected_pretrade_date"] = expected
        return result

    def test_empty_returns_empty(self) -> None:
        assert derive_state(pd.DataFrame(), "000001.SZ", pd.DataFrame()).empty

    def test_main_board_limit_up(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 11.0, 10.0)]
        )
        out = self._derive(df, "600519.SH")
        assert out.iloc[0]["limit_pct"] == 0.10
        assert out.iloc[0]["limit_up_price"] == 11.00
        assert out.iloc[0]["limit_down_price"] == 9.00
        assert bool(out.iloc[0]["is_limit_up"])
        assert out.iloc[0]["board_type"] == "main"

    def test_nonempty_derivation_requires_authoritative_listing_columns(self) -> None:
        bars = pd.DataFrame([self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 11.0, 10.0)])

        with pytest.raises(ValueError, match="listing eligibility"):
            derive_state(
                bars,
                "600519.SH",
                self._status("600519.SH", [date(2024, 1, 2)]),
            )

    @pytest.mark.parametrize(
        "ts_code",
        [
            "600519.SH",
            "300750.SZ",
            "688981.SH",
        ],
    )
    def test_first_five_listing_days_are_explicitly_unsupported(
        self,
        ts_code: str,
    ) -> None:
        dates = [date(2024, 1, day) for day in range(2, 8)]
        sixth_close = 12.0 if ts_code.startswith(("300", "688")) else 11.0
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [
                    self._mkbar(
                        day.isoformat(),
                        10.0,
                        sixth_close if index == 5 else 11.0,
                        10.0,
                        sixth_close if index == 5 else 11.0,
                        10.0,
                    )
                    for index, day in enumerate(dates)
                ]
            ),
            [date(2023, 12, 29), *dates[:-1]],
        )
        bars = self._with_listing_window(
            bars,
            list_date=dates[0],
            fifth_trading_date=dates[4],
        )

        out = derive_state(bars, ts_code, self._status(ts_code, dates))

        assert out.iloc[:5]["limit_pct"].isna().all()
        assert out.iloc[:5]["is_limit_up"].isna().all()
        assert out.iloc[5]["limit_pct"] in {0.10, 0.20}
        assert out.iloc[5]["is_limit_up"] == True  # noqa: E712
        assert pd.isna(out.iloc[5]["consecutive_limit_ups"])

    @pytest.mark.parametrize(
        ("ts_code", "list_date", "next_date", "expected"),
        [
            ("600519.SH", date(2020, 1, 2), date(2020, 1, 3), 0.10),
            ("300750.SZ", date(2020, 8, 20), date(2020, 8, 21), 0.10),
            ("833533.BJ", date(2024, 1, 2), date(2024, 1, 3), 0.30),
        ],
    )
    def test_legacy_and_bj_listing_day_is_unsupported_then_limit_applies(
        self,
        ts_code: str,
        list_date: date,
        next_date: date,
        expected: float,
    ) -> None:
        bars = self._with_listing_window(
            pd.DataFrame(
                [
                    self._mkbar(list_date.isoformat(), 10.0, 11.0, 10.0, 11.0, 10.0),
                    self._mkbar(next_date.isoformat(), 10.0, 11.0, 10.0, 11.0, 10.0),
                ]
            ),
            list_date=list_date,
            fifth_trading_date=None,
        )
        status_dates = bars["trade_date"].tolist()

        out = derive_state(bars, ts_code, self._status(ts_code, status_dates))

        assert pd.isna(out.iloc[0]["limit_pct"])
        assert out.iloc[1]["limit_pct"] == expected

    @pytest.mark.parametrize(
        ("ts_code", "list_date", "fifth_date", "sixth_date", "expected"),
        [
            (
                "300750.SZ",
                date(2020, 8, 24),
                date(2020, 8, 28),
                date(2020, 8, 31),
                0.20,
            ),
            (
                "600519.SH",
                date(2023, 4, 10),
                date(2023, 4, 14),
                date(2023, 4, 17),
                0.10,
            ),
        ],
    )
    def test_five_day_no_limit_rules_apply_at_exact_reform_boundaries(
        self,
        ts_code: str,
        list_date: date,
        fifth_date: date,
        sixth_date: date,
        expected: float,
    ) -> None:
        bars = self._with_listing_window(
            pd.DataFrame(
                [
                    self._mkbar(list_date.isoformat(), 10.0, 11.0, 10.0, 11.0, 10.0),
                    self._mkbar(fifth_date.isoformat(), 10.0, 11.0, 10.0, 11.0, 10.0),
                    self._mkbar(sixth_date.isoformat(), 10.0, 12.0, 10.0, 12.0, 10.0),
                ]
            ),
            list_date=list_date,
            fifth_trading_date=fifth_date,
        )

        out = derive_state(
            bars,
            ts_code,
            self._status(ts_code, [list_date, fifth_date, sixth_date]),
        )

        assert out.iloc[:2]["limit_pct"].isna().all()
        assert out.iloc[2]["limit_pct"] == expected

    def test_gem_20pct_limit(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 110.0, 120.0, 108.0, 120.0, 100.0)]
        )
        out = self._derive(df, "300750.SZ")
        assert out.iloc[0]["limit_pct"] == 0.20
        assert out.iloc[0]["limit_up_price"] == 120.0
        assert bool(out.iloc[0]["is_limit_up"])

    def test_bj_30pct_limit(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 11.0, 13.0, 10.5, 13.0, 10.0)]
        )
        out = self._derive(df, "833533.BJ")
        assert out.iloc[0]["limit_pct"] == 0.30
        assert out.iloc[0]["limit_up_price"] == 13.00
        assert bool(out.iloc[0]["is_limit_up"])

    def test_st_5pct_limit(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 10.2, 10.5, 10.0, 10.5, 10.0)]
        )
        out = self._derive(df, "000408.SZ", is_st=True, name="*ST藏格")
        assert out.iloc[0]["limit_pct"] == 0.05
        assert out.iloc[0]["limit_up_price"] == 10.50
        assert bool(out.iloc[0]["is_st"])
        assert bool(out.iloc[0]["is_limit_up"])

    def test_status_changes_before_during_and_after_st(self) -> None:
        dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        bars = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 10.0, 10.5, 9.9, 10.5, 10.0),
                self._mkbar("2024-01-03", 10.0, 10.5, 9.9, 10.5, 10.0),
                self._mkbar("2024-01-04", 10.0, 10.5, 9.9, 10.5, 10.0),
            ]
        )
        status = pd.DataFrame(
            {
                "ts_code": ["600000.SH"] * 3,
                "trade_date": dates,
                "name": ["浦发银行", "ST浦发", "浦发银行"],
                "is_st": pd.Series([False, True, False], dtype="boolean"),
                "available_at": [
                    datetime.combine(d, time(9, 25), tzinfo=SHANGHAI) for d in dates
                ],
                "conflict_reason": [None, None, None],
            }
        )

        out = derive_state(self._with_listing_window(bars), "600000.SH", status)

        assert out["is_st"].tolist() == [False, True, False]
        assert out["limit_pct"].tolist() == [0.10, 0.05, 0.10]
        assert out["is_limit_up"].tolist() == [False, True, False]

    @pytest.mark.parametrize(
        ("trade_date", "ts_code", "is_st", "expected"),
        [
            (date(2020, 8, 21), "300001.SZ", False, 0.10),
            (date(2020, 8, 21), "300001.SZ", True, 0.05),
            (date(2020, 8, 24), "300001.SZ", False, 0.20),
            (date(2020, 8, 24), "300001.SZ", True, 0.20),
            (date(2020, 8, 21), "688001.SH", True, 0.20),
            (date(2020, 8, 21), "830001.BJ", True, 0.30),
        ],
    )
    def test_historical_exchange_limit_regimes(
        self,
        trade_date: date,
        ts_code: str,
        is_st: bool,
        expected: float,
    ) -> None:
        bars = pd.DataFrame(
            [
                self._mkbar(
                    trade_date.isoformat(),
                    10.0,
                    10.5,
                    9.5,
                    10.0,
                    10.0,
                )
            ]
        )

        out = self._derive(bars, ts_code, is_st=is_st)

        assert out.iloc[0]["limit_pct"] == expected

    def test_limit_price_round_half_up(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 3.40, 3.47, 3.35, 3.47, 3.30)]
        )
        out = self._derive(df, "000004.SZ", is_st=True, name="*ST国华")
        assert out.iloc[0]["limit_up_price"] == 3.47
        assert out.iloc[0]["limit_down_price"] == 3.14

    def test_limit_up_with_tolerance(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 10.995, 10.0)]
        )
        out = self._derive(df, "600519.SH")
        assert bool(out.iloc[0]["is_limit_up"])

    def test_limit_up_beyond_tolerance(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 10.2, 11.0, 10.0, 10.98, 10.0)]
        )
        out = self._derive(df, "600519.SH")
        assert not bool(out.iloc[0]["is_limit_up"])

    def test_first_limit_up_flag(self) -> None:
        df = self._with_pretrade_dates(
            pd.DataFrame(
                [
                    self._mkbar("2024-01-02", 10.0, 10.5, 9.9, 10.3, 10.0),
                    self._mkbar("2024-01-03", 10.4, 11.33, 10.3, 11.33, 10.3),
                    self._mkbar("2024-01-04", 11.5, 12.46, 11.4, 12.46, 11.33),
                    self._mkbar("2024-01-05", 12.5, 12.6, 12.0, 12.2, 12.46),
                    self._mkbar("2024-01-08", 12.3, 13.42, 12.2, 13.42, 12.2),
                ]
            ),
            [
                date(2023, 12, 29),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ],
        )
        out = self._derive(df, "600519.SH")
        assert out["is_first_limit_up"].tolist() == [False, True, False, False, True]

    def test_yiziban_detect(self) -> None:
        df = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 11.0, 11.0, 11.0, 11.0, 10.0),
                self._mkbar("2024-01-03", 11.5, 12.10, 11.5, 12.10, 11.0),
            ]
        )
        out = self._derive(df, "600519.SH")
        assert bool(out.iloc[0]["is_yiziban"])
        assert not bool(out.iloc[1]["is_yiziban"])

    def test_yiziban_requires_equal_ohlc_at_cent_precision(self) -> None:
        row = self._mkbar("2024-01-02", 11.0, 11.01, 11.0, 11.0, 10.0)

        out = self._derive(pd.DataFrame([row]), "600519.SH")

        assert bool(out.iloc[0]["is_limit_up"])
        assert not bool(out.iloc[0]["is_yiziban"])

    @pytest.mark.parametrize("missing_column", ["open", "high", "low", "close"])
    def test_yiziban_is_unknown_when_any_ohlc_value_is_missing(
        self,
        missing_column: str,
    ) -> None:
        row = self._mkbar("2024-01-02", 11.0, 11.0, 11.0, 11.0, 10.0)
        row[missing_column] = None

        out = self._derive(pd.DataFrame([row]), "600519.SH")

        assert pd.isna(out.iloc[0]["is_yiziban"])

    def test_target_limit_up_continues_from_typed_predecessor_seed(self) -> None:
        trade_date = date(2024, 1, 3)
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [self._mkbar("2024-01-03", 11.5, 12.1, 11.4, 12.1, 11.0)]
            ),
            [date(2024, 1, 2)],
        )
        seed = DailyStateSeed(
            trade_date=date(2024, 1, 2),
            is_limit_up=True,
            consecutive_limit_ups=2,
        )

        out = derive_state(
            self._with_listing_window(bars),
            "600519.SH",
            self._status("600519.SH", [trade_date]),
            seed=seed,
        ).iloc[0]

        assert out["is_first_limit_up"] == False  # noqa: E712
        assert out["consecutive_limit_ups"] == 3

    def test_target_limit_up_starts_after_typed_non_limit_seed(self) -> None:
        trade_date = date(2024, 1, 3)
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [self._mkbar("2024-01-03", 10.5, 11.0, 10.4, 11.0, 10.0)]
            ),
            [date(2024, 1, 2)],
        )
        seed = DailyStateSeed(
            trade_date=date(2024, 1, 2),
            is_limit_up=False,
            consecutive_limit_ups=0,
        )

        out = derive_state(
            self._with_listing_window(bars),
            "600519.SH",
            self._status("600519.SH", [trade_date]),
            seed=seed,
        ).iloc[0]

        assert out["is_first_limit_up"] == True  # noqa: E712
        assert out["consecutive_limit_ups"] == 1

    def test_seed_calendar_mismatch_keeps_target_chain_unknown(self) -> None:
        trade_date = date(2024, 1, 4)
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [self._mkbar("2024-01-04", 10.5, 11.0, 10.4, 11.0, 10.0)]
            ),
            [date(2024, 1, 3)],
        )
        seed = DailyStateSeed(
            trade_date=date(2024, 1, 2),
            is_limit_up=False,
            consecutive_limit_ups=0,
        )

        out = derive_state(
            self._with_listing_window(bars),
            "600519.SH",
            self._status("600519.SH", [trade_date]),
            seed=seed,
        ).iloc[0]

        assert pd.isna(out["is_first_limit_up"])
        assert pd.isna(out["consecutive_limit_ups"])

    def test_consecutive_limit_ups(self) -> None:
        df = self._with_pretrade_dates(
            pd.DataFrame(
                [
                    self._mkbar("2024-01-02", 10.0, 10.2, 9.8, 10.0, 10.0),
                    self._mkbar("2024-01-03", 10.4, 11.0, 10.3, 11.0, 10.0),
                    self._mkbar("2024-01-04", 11.5, 12.10, 11.4, 12.10, 11.0),
                    self._mkbar("2024-01-05", 12.2, 13.31, 12.1, 13.31, 12.10),
                    self._mkbar("2024-01-08", 13.0, 13.5, 12.5, 12.8, 13.31),
                    self._mkbar("2024-01-09", 13.0, 14.08, 12.9, 14.08, 12.8),
                    self._mkbar("2024-01-10", 14.2, 15.49, 14.1, 15.49, 14.08),
                ]
            ),
            [
                date(2023, 12, 29),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
                date(2024, 1, 8),
                date(2024, 1, 9),
            ],
        )
        out = self._derive(df, "600519.SH")
        assert out["consecutive_limit_ups"].tolist() == [0, 1, 2, 3, 0, 1, 2]

    def test_unknown_status_fields_are_nullable_but_board_and_body_remain(self) -> None:
        bars = pd.DataFrame(
            [self._mkbar("2024-01-02", 10.0, 11.0, 9.9, 11.0, 10.0)]
        )

        out = derive_state(
            self._with_listing_window(bars),
            "600519.SH",
            pd.DataFrame(),
        ).iloc[0]

        nullable = [
            "is_st",
            "limit_pct",
            "limit_up_price",
            "limit_down_price",
            "is_limit_up",
            "is_limit_down",
            "is_first_limit_up",
            "is_yiziban",
            "consecutive_limit_ups",
        ]
        assert all(pd.isna(out[column]) for column in nullable)
        assert out["board_type"] == "main"
        assert not bool(out["is_bj"])
        assert out["body_upper"] == 11.0
        assert out["body_lower"] == 10.0

    def test_conflict_status_is_unknown(self) -> None:
        d = date(2024, 1, 2)
        bars = pd.DataFrame(
            [self._mkbar("2024-01-02", 10.0, 10.5, 9.9, 10.5, 10.0)]
        )
        status = pd.DataFrame(
            {
                "ts_code": ["600000.SH"],
                "trade_date": [d],
                "name": [None],
                "is_st": pd.Series([pd.NA], dtype="boolean"),
                "available_at": [pd.NaT],
                "conflict_reason": ["stock_st_name_conflict"],
            }
        )

        out = derive_state(
            self._with_listing_window(bars),
            "600000.SH",
            status,
        ).iloc[0]

        assert pd.isna(out["is_st"])
        assert pd.isna(out["limit_pct"])
        assert pd.isna(out["is_limit_up"])

    def test_future_visible_status_is_unknown_until_a_later_trade_date(self) -> None:
        bars = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 10.0, 10.5, 9.9, 10.5, 10.0),
                self._mkbar("2024-01-03", 10.0, 10.5, 9.9, 10.5, 10.0),
            ]
        )
        status = pd.DataFrame(
            {
                "ts_code": ["600000.SH", "600000.SH"],
                "trade_date": [date(2024, 1, 2), date(2024, 1, 3)],
                "name": ["ST迟报", "ST迟报"],
                "is_st": pd.Series([True, True], dtype="boolean"),
                "available_at": [
                    datetime(2024, 1, 3, 9, 25, tzinfo=SHANGHAI),
                    datetime(2024, 1, 3, 9, 25, tzinfo=SHANGHAI),
                ],
                "conflict_reason": [None, None],
            }
        )

        out = derive_state(self._with_listing_window(bars), "600000.SH", status)

        assert pd.isna(out.iloc[0]["is_st"])
        assert out.iloc[1]["is_st"] == True  # noqa: E712
        assert out.iloc[1]["limit_pct"] == 0.05

    def test_unknown_breaks_chain_until_explicit_non_limit_up_resets_it(self) -> None:
        dates = [date(2024, 1, day) for day in range(2, 7)]
        bars = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 10.5, 11.0, 10.4, 11.0, 10.0),
                self._mkbar("2024-01-03", 11.5, 12.1, 11.4, 12.1, 11.0),
                self._mkbar("2024-01-04", 12.5, 13.31, 12.4, 13.31, 12.1),
                self._mkbar("2024-01-05", 13.0, 13.2, 12.5, 12.8, 13.31),
                self._mkbar("2024-01-06", 13.0, 14.08, 12.9, 14.08, 12.8),
            ]
        )
        status = self._status("600000.SH", [dates[0], *dates[2:]])

        out = derive_state(self._with_listing_window(bars), "600000.SH", status)

        limit_up = out["is_limit_up"].tolist()
        first = out["is_first_limit_up"].tolist()
        consecutive = out["consecutive_limit_ups"].tolist()
        assert limit_up[0] == True  # noqa: E712
        assert pd.isna(limit_up[1])
        assert limit_up[2:] == [True, False, True]
        assert pd.isna(first[0])
        assert pd.isna(first[1]) and pd.isna(first[2])
        assert first[3] == False  # noqa: E712
        assert pd.isna(first[4])
        assert pd.isna(consecutive[0])
        assert pd.isna(consecutive[1]) and pd.isna(consecutive[2])
        assert consecutive[3] == 0
        assert pd.isna(consecutive[4])

    def test_missing_price_makes_limit_flags_unknown_and_breaks_chain(self) -> None:
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [
                    self._mkbar("2024-01-02", 10.5, 11.0, 10.4, 11.0, 10.0),
                    self._mkbar("2024-01-03", 11.0, 11.2, 10.8, 11.0, float("nan")),
                    self._mkbar("2024-01-04", 11.5, 12.1, 11.4, 12.1, 11.0),
                ]
            ),
            [date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 3)],
        )

        out = self._derive(bars, "600000.SH")

        assert out.iloc[0]["is_limit_up"] == True  # noqa: E712
        assert pd.isna(out.iloc[1]["is_limit_up"])
        assert pd.isna(out.iloc[1]["is_limit_down"])
        assert out.iloc[2]["is_limit_up"] == True  # noqa: E712
        assert out["consecutive_limit_ups"].isna().all()

    def test_first_observed_limit_up_has_unknown_chain(self) -> None:
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [self._mkbar("2024-01-03", 10.5, 11.0, 10.4, 11.0, 10.0)]
            ),
            [date(2024, 1, 2)],
        )

        out = self._derive(bars, "600000.SH").iloc[0]

        assert out["is_limit_up"] == True  # noqa: E712
        assert pd.isna(out["is_first_limit_up"])
        assert pd.isna(out["consecutive_limit_ups"])

    def test_missing_expected_trading_day_breaks_chain(self) -> None:
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [
                    self._mkbar("2024-01-02", 10.0, 10.2, 9.8, 10.0, 10.0),
                    self._mkbar("2024-01-04", 10.5, 11.0, 10.4, 11.0, 10.0),
                ]
            ),
            [date(2023, 12, 29), date(2024, 1, 3)],
        )

        out = self._derive(bars, "600000.SH")

        assert out.iloc[0]["consecutive_limit_ups"] == 0
        assert pd.isna(out.iloc[1]["is_first_limit_up"])
        assert pd.isna(out.iloc[1]["consecutive_limit_ups"])

    def test_weekend_does_not_break_chain_when_calendar_links_friday(self) -> None:
        bars = self._with_pretrade_dates(
            pd.DataFrame(
                [
                    self._mkbar("2024-01-05", 10.0, 10.2, 9.8, 10.0, 10.0),
                    self._mkbar("2024-01-08", 10.5, 11.0, 10.4, 11.0, 10.0),
                ]
            ),
            [date(2024, 1, 4), date(2024, 1, 5)],
        )

        out = self._derive(bars, "600000.SH")

        assert out.iloc[1]["is_first_limit_up"] == True  # noqa: E712
        assert out.iloc[1]["consecutive_limit_ups"] == 1

    def test_limit_down(self) -> None:
        df = pd.DataFrame(
            [self._mkbar("2024-01-02", 9.5, 9.6, 9.0, 9.0, 10.0)]
        )
        out = self._derive(df, "600519.SH")
        assert bool(out.iloc[0]["is_limit_down"])
        assert out.iloc[0]["limit_down_price"] == 9.00

    def test_body_upper_lower(self) -> None:
        df = pd.DataFrame(
            [
                self._mkbar("2024-01-02", 10.0, 10.8, 9.9, 10.5, 10.0),
                self._mkbar("2024-01-03", 10.6, 10.7, 10.0, 10.1, 10.5),
            ]
        )
        out = self._derive(df, "600519.SH")
        assert out.iloc[0]["body_upper"] == 10.5
        assert out.iloc[0]["body_lower"] == 10.0
        assert out.iloc[1]["body_upper"] == 10.6
        assert out.iloc[1]["body_lower"] == 10.1
