"""screen() 主流程单测 —— 不依赖 DuckDB，注入假的 loader。"""

from unittest.mock import patch

from rquant.screen import screen
from rquant.screen.rules import gt, not_bj, not_st
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
