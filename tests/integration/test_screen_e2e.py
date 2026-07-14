"""screen() 端到端测试：复刻用户原始场景。"""

from datetime import UTC, date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.presets import ScreenPreset
from rquant.screen import screen
from rquant.screen.rules import first_limit_up, gt, not_bj, not_limit_up, not_st
from rquant.security_status import SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay

SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.mark.integration
class TestUserScenario:
    """非 ST + 非北交所 + 昨首板 + 今未涨停 + 今高>昨收。"""

    @pytest.fixture
    def store(self, tmp_path) -> DuckDBStore:
        s = DuckDBStore(path=tmp_path / "e2e.duckdb")

        # 三只股票，T-1 = 2026-04-14（周二）、T = 2026-04-15（周三）
        #   300001.SZ：昨首板、今未涨停、今高>昨收 → 命中
        #   000001.SZ：昨首板、今未涨停、今高<昨收 → 不命中
        #   833001.BJ：和 300001 同形态，但北交所 → 不命中
        daily_rows = []
        state_rows = []
        basic_rows = []
        for (
            code,
            nm,
            is_bj,
            board,
            p_prev,
            o_today,
            h_today,
            l_today,
            c_today,
            limit_up_yesterday,
        ) in [
            (
                "300001.SZ",
                "当前特锐德",
                False,
                "gem",
                10.0,
                11.0,
                13.0,
                11.0,
                12.0,
                True,
            ),
            (
                "000001.SZ",
                "平安银行",
                False,
                "main",
                20.0,
                20.0,
                20.5,
                19.5,
                19.8,
                True,
            ),
            ("833001.BJ", "北交所", True, "bj", 5.0, 5.5, 7.0, 5.2, 6.3, True),
        ]:
            daily_rows.extend(
                [
                    {
                        "ts_code": code,
                        "trade_date": date(2026, 4, 14),
                        "open": p_prev,
                        "high": p_prev * 1.1,
                        "low": p_prev,
                        "close": p_prev * 1.1,
                        "pre_close": p_prev,
                        "change": p_prev * 0.1,
                        "pct_chg": 10.0,
                        "vol": 1000.0,
                        "amount": 10000.0,
                    },
                    {
                        "ts_code": code,
                        "trade_date": date(2026, 4, 15),
                        "open": o_today,
                        "high": h_today,
                        "low": l_today,
                        "close": c_today,
                        "pre_close": p_prev * 1.1,
                        "change": c_today - p_prev * 1.1,
                        "pct_chg": (c_today - p_prev * 1.1) / (p_prev * 1.1) * 100,
                        "vol": 1200.0,
                        "amount": 12000.0,
                    },
                ]
            )
            basic_rows.append(
                {
                    "ts_code": code,
                    "symbol": code.split(".")[0],
                    "name": nm,
                    "area": "X",
                    "industry": "Y",
                    "list_date": date(2020, 1, 1),
                    "market": board,
                }
            )
            state_rows.extend(
                [
                    {
                        "ts_code": code,
                        "trade_date": date(2026, 4, 14),
                        "is_st": False,
                        "is_bj": is_bj,
                        "board_type": board,
                        "limit_pct": 0.10,
                        "limit_up_price": p_prev * 1.1,
                        "limit_down_price": p_prev * 0.9,
                        "is_limit_up": True,
                        "is_limit_down": False,
                        "is_first_limit_up": limit_up_yesterday,
                        "is_yiziban": False,
                        "consecutive_limit_ups": 1,
                        "body_upper": p_prev * 1.1,
                        "body_lower": p_prev,
                    },
                    {
                        "ts_code": code,
                        "trade_date": date(2026, 4, 15),
                        "is_st": False,
                        "is_bj": is_bj,
                        "board_type": board,
                        "limit_pct": 0.10,
                        "limit_up_price": p_prev * 1.21,
                        "limit_down_price": p_prev * 0.99,
                        "is_limit_up": False,
                        "is_limit_down": False,
                        "is_first_limit_up": False,
                        "is_yiziban": False,
                        "consecutive_limit_ups": 0,
                        "body_upper": max(o_today, c_today),
                        "body_lower": min(o_today, c_today),
                    },
                ]
            )
        s.upsert_daily(pd.DataFrame(daily_rows))
        s.upsert_stock_basic(pd.DataFrame(basic_rows))
        s.upsert_state(pd.DataFrame(state_rows))
        s.upsert_trade_calendar([
            TradeCalendarDay(
                exchange="SSE",
                cal_date=trade_day,
                is_open=True,
                source="tushare",
                updated_at=datetime(2026, 4, 16, tzinfo=UTC),
            )
            for trade_day in (date(2026, 4, 14), date(2026, 4, 15))
        ])
        historical_names = {
            "300001.SZ": "历史特锐德",
            "000001.SZ": "历史平安银行",
            "833001.BJ": "历史北交所",
        }
        s.upsert_stock_status([
            SecurityStatusDaily(
                ts_code=code,
                trade_date=trade_day,
                name=historical_names[code],
                is_st=False,
                name_source="namechange",
                st_source="namechange+stock_st",
                available_at=datetime(
                    trade_day.year,
                    trade_day.month,
                    trade_day.day,
                    9,
                    25,
                    tzinfo=SHANGHAI,
                ),
                ingested_at=datetime(2026, 4, 16, tzinfo=UTC),
            )
            for code in historical_names
            for trade_day in (date(2026, 4, 14), date(2026, 4, 15))
        ])
        yield s
        s.close()

    def test_user_scenario(self, store: DuckDBStore) -> None:
        result = screen(
            trade_date="2026-04-15",
            rules=[
                not_st(),
                not_bj(),
                first_limit_up(offset=1),
                not_limit_up(offset=0),
                gt("HIGH[0]", "CLOSE[1]"),
            ],
            store=store,
        )
        assert list(result["ts_code"]) == ["300001.SZ"]
        assert result.loc[0, "name"] == "历史特锐德"

    def test_daily_pipeline_persists_historical_status_name(
        self, store: DuckDBStore
    ) -> None:
        from rquant.pipeline import run_daily_pipeline

        preset = ScreenPreset(
            name="pit-name",
            description="test",
            rules=[
                not_st(),
                not_bj(),
                first_limit_up(offset=1),
                not_limit_up(offset=0),
                gt("HIGH[0]", "CLOSE[1]"),
            ],
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"pit-name": preset}),
            patch("rquant.pipeline._sync_pool2_watch"),
            patch("rquant.pipeline._push_daily_summary"),
            patch("rquant.monitor.check_exits"),
        ):
            summary = run_daily_pipeline(
                "2026-04-15",
                store=store,
                minute_backfill=False,
            )

        assert summary == {"pit-name": 1}
        stored = store.query_screen_result("2026-04-15", "pit-name")
        assert stored.loc[0, "name"] == "历史特锐德"
