"""screen() 主流程单测 —— 不依赖 DuckDB，注入假的 loader。"""

from unittest.mock import patch

import pandas as pd
import pytest

from rquant.screen import screen
from rquant.screen.rules import (
    AggregateRequest,
    _tag_aggregates,
    _tag_lookback,
    gt,
    not_bj,
    not_st,
)
from tests.fixtures.wide_frames import make_wide_frame


class TestScreenCore:
    def test_and_combine(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "is_st"): True,
                ("300001.SZ", "CLOSE[0]"): 15.0,
                ("300001.SZ", "PCT_CHG[0]"): 2.0,
                ("688001.SH", "CLOSE[0]"): 8.0,
                ("688001.SH", "PCT_CHG[0]"): 1.0,
            }
        )
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st(), not_bj(), gt("CLOSE[0]", 10.0)],
            )
        assert list(result["ts_code"]) == ["300001.SZ"]
        assert set(result.columns) >= {"ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"}

    def test_empty_result_returns_empty_df(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[gt("CLOSE[0]", 9999.0)],
            )
        assert len(result) == 0
        assert list(result.columns)[:4] == ["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]

    def test_include_columns(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "MA20[0]"): 11.0})
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                include_columns=["MA20[0]"],
            )
        assert "MA20[0]" in result.columns

    def test_lookback_auto_inferred_from_rules(self) -> None:
        # fixture lookback=3 is frame depth; inferred lookback passed to
        # load_universe() is max(min_lookback) = max(0, 2) = 2
        df = make_wide_frame(lookback=3)
        from rquant.screen.rules import first_limit_up

        rules = [not_st(), first_limit_up(offset=2)]
        with patch("rquant.screen.core.load_universe") as mock_loader:
            mock_loader.return_value = df
            screen(trade_date="2026-04-15", rules=rules)
            assert mock_loader.call_args.kwargs["lookback"] == 2

    def test_explicit_lookback_overrides_inference(self) -> None:
        df = make_wide_frame(lookback=10)
        with patch("rquant.screen.core.load_universe") as mock_loader:
            mock_loader.return_value = df
            screen(trade_date="2026-04-15", rules=[not_st()], lookback=10)
            assert mock_loader.call_args.kwargs["lookback"] == 10


class TestAggregateCollection:
    def test_collect_aggregates_from_rules(self) -> None:
        from rquant.screen.core import _collect_aggregates

        def dummy_rule(df):
            return df["ts_code"].notna()

        dummy_rule = _tag_lookback(dummy_rule, 0)
        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )
        dummy_rule = _tag_aggregates(dummy_rule, [req])

        aggregates = _collect_aggregates([not_st(), dummy_rule])
        assert len(aggregates) == 1
        assert aggregates[0].name == "max_consec_ups_8d"

    def test_collect_aggregates_deduplicates(self) -> None:
        from rquant.screen.core import _collect_aggregates

        req = AggregateRequest(
            name="same_name",
            source_table="daily_state",
            source_col="x",
            agg_func="max",
            window=5,
        )

        def r1(df):
            return df["ts_code"].notna()

        r1 = _tag_lookback(r1, 0)
        r1 = _tag_aggregates(r1, [req])

        def r2(df):
            return df["ts_code"].notna()

        r2 = _tag_lookback(r2, 0)
        r2 = _tag_aggregates(r2, [req])

        aggregates = _collect_aggregates([r1, r2])
        assert len(aggregates) == 1

    def test_collect_aggregates_empty_when_no_requests(self) -> None:
        from rquant.screen.core import _collect_aggregates

        aggregates = _collect_aggregates([not_st(), not_bj()])
        assert aggregates == []

    def test_screen_passes_aggregates_to_loader(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 0

        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )

        def rule_with_agg(df):
            return df["max_consec_ups_8d"] < 3

        rule_with_agg = _tag_lookback(rule_with_agg, 0)
        rule_with_agg = _tag_aggregates(rule_with_agg, [req])

        with patch("rquant.screen.core.load_universe") as mock_loader:
            mock_loader.return_value = df
            screen(trade_date="2026-04-15", rules=[rule_with_agg])
            call_kwargs = mock_loader.call_args.kwargs
            assert "aggregate_requests" in call_kwargs
            assert len(call_kwargs["aggregate_requests"]) == 1
            assert call_kwargs["aggregate_requests"][0].name == "max_consec_ups_8d"


class TestWhitelist:
    def test_whitelist_handles_loader_empty_frame_without_columns(self) -> None:
        with patch(
            "rquant.screen.core.load_universe",
            return_value=pd.DataFrame(),
        ):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                include_columns=["MA20[0]"],
                ts_code_whitelist=["300001.SZ"],
            )

        assert result.empty
        assert list(result.columns) == [
            "ts_code",
            "name",
            "CLOSE[0]",
            "PCT_CHG[0]",
            "MA20[0]",
        ]

    def test_whitelist_rejects_nonempty_frame_without_ts_code(self) -> None:
        with (
            patch(
                "rquant.screen.core.load_universe",
                return_value=pd.DataFrame({"name": ["invalid"]}),
            ),
            pytest.raises(KeyError, match="ts_code"),
        ):
            screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=["300001.SZ"],
            )

    def test_whitelist_filters_to_subset(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=["300001.SZ"],
            )
        assert list(result["ts_code"]) == ["300001.SZ"]

    def test_whitelist_none_returns_all(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=None,
            )
        # Default fixture: 000001.SZ has is_st=False by default, plus 300001.SZ and 688001.SH
        assert len(result) >= 2

    def test_whitelist_empty_returns_empty(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=[],
            )
        assert len(result) == 0
        assert "ts_code" in result.columns

    def test_whitelist_with_nonexistent_code(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=["999999.SZ"],
            )
        assert len(result) == 0
