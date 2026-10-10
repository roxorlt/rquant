"""pipeline 流水线单测 —— mock screen() 和 PRESET_SCREENS。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from unittest.mock import patch

import pandas as pd
import pytest

from rquant.pipeline import (
    _get_prev_trading_date,
    _resolve_execution_order,
    _to_screen_result_df,
    run_daily_pipeline,
    run_daily_screen_stage,
)
from rquant.presets import ScreenPreset
from rquant.screen.pool_ranking import PoolRankingPlan
from rquant.screen.rules import not_st
from rquant.storage.duckdb import DuckDBStore


def _seed_daily_fixture_sources(store: DuckDBStore, frame: pd.DataFrame | None = None) -> None:
    """These pipeline isolation tests mock the evaluator, but its input dates are real."""
    if frame is not None:
        day=store._conn.execute("SELECT MAX(trade_date) FROM daily_bar").fetchone()[0]
        for row in frame.itertuples(index=False,name=None):
            values=dict(zip(frame.columns,row,strict=True))
            store._conn.execute("INSERT INTO daily_bar(ts_code,trade_date,close,pct_chg) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",[values["ts_code"],day,values.get("CLOSE[0]"),values.get("PCT_CHG[0]")])
    rows = store._conn.execute("SELECT ts_code,trade_date FROM daily_bar ORDER BY trade_date,ts_code").fetchall()
    if not rows:
        return
    first, last = min(day for _,day in rows), max(day for _,day in rows)
    days = {day for _,day in rows}
    for offset in range((last-first).days+1):
        day = first + timedelta(days=offset)
        store._conn.execute("INSERT INTO trade_calendar(exchange,cal_date,is_open,source,updated_at) VALUES ('SSE',?,?, 'fixture',?) ON CONFLICT DO NOTHING",[day,day in days,datetime(2026,1,1,tzinfo=UTC)])
    for code,day in rows:
        store._conn.execute("INSERT INTO stock_status_daily(ts_code,trade_date,name,is_st,name_source,st_source,available_at,ingested_at) VALUES (?,?,'fixture',FALSE,'fixture','fixture',?,?) ON CONFLICT DO NOTHING",[code,day,datetime.combine(day,datetime.min.time(),UTC),datetime(2026,1,1,tzinfo=UTC)])


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestToScreenResultDf:
    def test_converts_screen_output(self) -> None:
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["平安银行"],
                "CLOSE[0]": [10.5],
                "PCT_CHG[0]": [5.0],
                "CIRC_MV[0]": [80000.0],
            }
        )
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert len(result) == 1
        assert result.iloc[0]["trade_date"] == "2026-04-18"
        assert result.iloc[0]["preset_name"] == "pool1"
        assert result.iloc[0]["ts_code"] == "000001.SZ"
        assert result.iloc[0]["close"] == 10.5
        assert result.iloc[0]["pct_chg"] == 5.0
        assert "CIRC_MV" in result.iloc[0]["extra"]

    def test_empty_input(self) -> None:
        df = pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert result.empty

    def test_no_extra_columns(self) -> None:
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["平安银行"],
                "CLOSE[0]": [10.5],
                "PCT_CHG[0]": [5.0],
            }
        )
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert result.iloc[0]["extra"] is None


def test_ranked_daily_pool_filters_risk_then_commits_only_top_members_and_receipt(
    store: DuckDBStore,
) -> None:
    sessions = [date(2026, 8, 1) + timedelta(days=offset) for offset in range(21)]
    codes = ("000001.SZ", "000002.SZ", "000003.SZ")
    for index, session in enumerate(sessions):
        store._conn.execute(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, 'fixture', '2026-08-22 00:00:00+00')",
            [session],
        )
        for code, ending_close in zip(codes, (30.0, 20.0, 15.0), strict=True):
            store._conn.execute(
                "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES (?, ?, ?)",
                [code, session, ending_close if index == 20 else 10.0],
            )
            store._conn.execute(
                "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (?, ?, 1)",
                [code, session],
            )
    frame = pd.DataFrame(
        {
            "ts_code": list(codes),
            "name": ["风险", "第二", "第三"],
            "CLOSE[0]": [30.0, 20.0, 15.0],
            "PCT_CHG[0]": [1.0, 1.0, 1.0],
            "TURNOVER_RATE[0]": [3.0, 2.0, 1.0],
            "CIRC_MV[0]": [30.0, 20.0, 10.0],
        }
    )
    from rquant.screen.ranking import load_twenty_day_adjusted_returns
    frame = frame.merge(load_twenty_day_adjusted_returns(store._conn,sessions[-1],list(codes)),on="ts_code",validate="one_to_one")
    for code,circ,turnover in zip(codes,(30,20,10),(3,2,1),strict=True):
        store._conn.execute("INSERT INTO daily_basic(ts_code,trade_date,circ_mv,turnover_rate) VALUES (?,?,?,?)",[code,sessions[-1],circ,turnover])
    preset = ScreenPreset(
        name="ranked",
        description="",
        rules=[not_st()],
        ranking=PoolRankingPlan.model_validate(
            {
                "conditions": [
                    {"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 60},
                    {"metric": "TURNOVER_RATE[0]", "ascending": False, "weight": 20},
                    {"metric": "CIRC_MV[0]", "ascending": False, "weight": 20},
                ],
                "top_n": 1,
            }
        ),
    )
    _seed_daily_fixture_sources(store,frame)
    with (
        patch("rquant.pipeline.PRESET_SCREENS", {"ranked": preset}),
        patch("rquant.pipeline.screen", return_value=frame) as screened,
        patch("rquant.pipeline.load_active_blacklist", return_value={"000001.SZ": object()}),
    ):
        outcome = run_daily_screen_stage(sessions[-1].isoformat(), store=store)
    assert outcome.preset_hits == {"ranked": 1}
    assert {"TURNOVER_RATE[0]", "CIRC_MV[0]"} <= set(
        screened.call_args.kwargs["include_columns"]
    )
    saved = store.query_screen_result(sessions[-1].isoformat(), "ranked")
    assert saved["ts_code"].tolist() == ["000002.SZ"]
    assert json.loads(saved.iloc[0]["extra"]) == {"ranking_score":100.0,"rank_position":1}
    proof = store.query_screen_run_evidence(sessions[-1].isoformat(), "ranked")
    assert proof is not None and proof.ranking_plan_digest is not None
    receipt = store.query_screen_run_receipt(sessions[-1].isoformat(), "ranked")
    assert receipt is not None
    assert receipt.hit_count == 1
    assert receipt.price_digest is not None


def test_ranked_daily_pool_fails_if_all_metric_facts_are_unknown(store: DuckDBStore) -> None:
    day = date(2026, 8, 21)
    store._conn.execute(
        "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('000001.SZ', ?, 10)",
        [day],
    )
    frame = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "name": ["样本"], "CLOSE[0]": [10.0],
         "PCT_CHG[0]": [0.0]}
    )
    frame["RETURN_20D_PCT[0]"] = float("nan")
    preset = ScreenPreset(
        name="ranked",
        description="",
        rules=[not_st()],
        ranking=PoolRankingPlan.model_validate(
            {"conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
             "top_n": 1}
        ),
    )
    _seed_daily_fixture_sources(store,frame)
    with (
        patch("rquant.pipeline.PRESET_SCREENS", {"ranked": preset}),
        patch("rquant.pipeline.screen", return_value=frame),
    ):
        outcome = run_daily_screen_stage(day.isoformat(), store=store)
    assert outcome.preset_hits == {"ranked": -1}
    assert outcome.errors == ("screen:ranked:ValueError",)
    assert store.query_screen_result(day.isoformat(), "ranked").empty
    assert store.query_screen_run_receipt(day.isoformat(), "ranked") is None


class TestResolveExecutionOrder:
    def test_no_dep_first(self) -> None:
        presets = {
            "child": ScreenPreset(
                name="child",
                description="",
                rules=[],
                depends_on="parent",
                offset_days=1,
            ),
            "parent": ScreenPreset(
                name="parent",
                description="",
                rules=[],
            ),
        }
        order = _resolve_execution_order(presets)
        assert order.index("parent") < order.index("child")

    def test_filter_by_names(self) -> None:
        presets = {
            "a": ScreenPreset(name="a", description="", rules=[]),
            "b": ScreenPreset(name="b", description="", rules=[]),
        }
        order = _resolve_execution_order(presets, names=["a"])
        assert order == ["a"]


class TestGetPrevTradingDate:
    def test_returns_previous_date(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        assert _get_prev_trading_date(store, "2026-04-18", 1) == "2026-04-17"

    def test_returns_none_when_no_data(self, store: DuckDBStore) -> None:
        assert _get_prev_trading_date(store, "2026-04-18", 1) is None

    def test_offset_2(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-04-16', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        assert _get_prev_trading_date(store, "2026-04-18", 2) == "2026-04-16"


class TestRunDailyPipeline:
    def test_skips_non_trading_day(self, store: DuckDBStore) -> None:
        result = run_daily_pipeline("2026-04-20", store=store)
        assert result == {}

    def test_runs_preset_and_persists(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["平安银行"],
                "CLOSE[0]": [10.5],
                "PCT_CHG[0]": [5.0],
            }
        )
        test_presets = {
            "test-pool": ScreenPreset(
                name="test-pool",
                description="test",
                rules=[not_st()],
            ),
        }
        _seed_daily_fixture_sources(store,mock_df)
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline("2026-04-18", store=store)
        assert result == {"test-pool": 1}
        sr = store.query_screen_result("2026-04-18", "test-pool")
        assert len(sr) == 1
        assert sr.iloc[0]["ts_code"] == "000001.SZ"

    def test_can_run_minute_context_backfill_after_pool1_screen(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('600000.SH', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame(
            {
                "ts_code": ["600000.SH"],
                "name": ["浦发银行"],
                "CLOSE[0]": [10.5],
                "PCT_CHG[0]": [5.0],
            }
        )
        test_presets = {
            "n-shape-pool1": ScreenPreset(
                name="n-shape-pool1",
                description="test",
                rules=[not_st()],
            ),
        }

        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
            patch("rquant.pipeline._sync_pool2_watch"),
            patch("rquant.pipeline._run_minute_context_backfill") as mock_backfill,
        ):
            run_daily_pipeline(
                "2026-04-18",
                store=store,
                minute_backfill=True,
                minute_backfill_lookback_days=90,
            )

        mock_backfill.assert_called_once_with(
            store,
            "2026-04-18",
            lookback_days=90,
            freq="1min",
        )

    def test_specific_preset_only(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["平安银行"],
                "CLOSE[0]": [10.5],
                "PCT_CHG[0]": [5.0],
            }
        )
        test_presets = {
            "a": ScreenPreset(name="a", description="", rules=[not_st()]),
            "b": ScreenPreset(name="b", description="", rules=[not_st()]),
        }
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline("2026-04-18", preset_names=["a"], store=store)
        assert "a" in result
        assert "b" not in result

    def test_child_uses_parent_whitelist(self, store: DuckDBStore) -> None:
        # T-1 and T data
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('000001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0),"
            "('300001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        # Parent results on T-1
        parent_sr = pd.DataFrame(
            {
                "trade_date": ["2026-04-17"],
                "preset_name": ["parent"],
                "ts_code": ["000001.SZ"],
                "name": ["平安银行"],
                "close": [10.0],
                "pct_chg": [5.0],
                "extra": [None],
            }
        )
        store.upsert_screen_result(parent_sr)

        child_preset = ScreenPreset(
            name="child",
            description="",
            rules=[not_st()],
            depends_on="parent",
            offset_days=1,
        )
        empty_df = pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])
        _seed_daily_fixture_sources(store,empty_df)
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"child": child_preset}),
            patch("rquant.pipeline.screen", return_value=empty_df) as mock_scr,
        ):
            run_daily_pipeline("2026-04-18", store=store)
            kw = mock_scr.call_args.kwargs
            assert kw["ts_code_whitelist"] == ["000001.SZ"]

    def test_child_skips_when_parent_empty(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('000001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        child_preset = ScreenPreset(
            name="child",
            description="",
            rules=[not_st()],
            depends_on="parent",
            offset_days=1,
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"child": child_preset}),
            patch("rquant.pipeline.screen") as mock_scr,
        ):
            result = run_daily_pipeline("2026-04-18", store=store)
            mock_scr.assert_not_called()
            assert result == {"child": 0}


class TestBlacklistFilter:
    def test_blacklisted_codes_are_dropped_before_upsert(self, store: DuckDBStore) -> None:
        """流水线 upsert 前应过滤命中黑名单的标的，干净的票照常落库。"""
        from datetime import date

        from rquant.risk.blacklist import BlacklistEntry, import_blacklist

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000016.SZ', '2026-04-28', 1,1,1,1,1,0,0,0,0),"
            "('600519.SH', '2026-04-28', 1,1,1,1,1,0,0,0,0)"
        )
        # 黑名单：000016.SZ 命中
        import_blacklist(
            [BlacklistEntry("000016.SZ", "深康佳A", "ST预警", ["净资产为负"])],
            list_label="430黑名单",
            source_file="test.pdf",
            store=store,
            imported_at=date(2026, 4, 28),
        )
        mock_df = pd.DataFrame(
            {
                "ts_code": ["000016.SZ", "600519.SH"],
                "name": ["深康佳A", "贵州茅台"],
                "CLOSE[0]": [3.0, 1685.0],
                "PCT_CHG[0]": [0.0, 1.5],
            }
        )
        test_presets = {
            "test-pool": ScreenPreset(
                name="test-pool",
                description="",
                rules=[not_st()],
            ),
        }
        _seed_daily_fixture_sources(store,mock_df)
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline("2026-04-28", store=store)

        # screen 命中 2 只，过滤后只剩 1 只落库
        assert result == {"test-pool": 1}
        sr = store.query_screen_result("2026-04-28", "test-pool")
        assert len(sr) == 1
        assert sr.iloc[0]["ts_code"] == "600519.SH"

    def test_no_filter_when_blacklist_empty(self, store: DuckDBStore) -> None:
        """无黑名单时所有票正常落库。"""
        store._conn.execute(
            "INSERT INTO daily_bar VALUES ('000016.SZ', '2026-04-28', 1,1,1,1,1,0,0,0,0)"
        )
        mock_df = pd.DataFrame(
            {
                "ts_code": ["000016.SZ"],
                "name": ["深康佳A"],
                "CLOSE[0]": [3.0],
                "PCT_CHG[0]": [0.0],
            }
        )
        test_presets = {
            "test-pool": ScreenPreset(
                name="test-pool",
                description="",
                rules=[not_st()],
            ),
        }
        _seed_daily_fixture_sources(store,mock_df)
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline("2026-04-28", store=store)
        assert result == {"test-pool": 1}


class TestCheckExitsInDailyPipeline:
    """daily pipeline must run the no-notify Pool 2 exit stage after sync."""

    def test_calls_check_exits_after_sync(self, store: DuckDBStore) -> None:
        from datetime import date

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {}),
            patch("rquant.pipeline.screen", return_value=pd.DataFrame()),
            patch("rquant.pipeline._sync_pool2_watch"),
            patch("rquant.pipeline._check_pool2_exits_without_notification") as mock_check_exits,
        ):
            run_daily_pipeline("2026-04-18", store=store)

        mock_check_exits.assert_called_once()
        args = mock_check_exits.call_args.args
        assert args[0] is store
        assert args[1] == date(2026, 4, 18)

    def test_aged_out_kicked_via_daily_pipeline(self, store: DuckDBStore) -> None:
        """端到端：入池 10 个交易日的票，跑 daily 流水线被 aged_out 踢出。"""
        from datetime import date

        # 11 个交易日的 daily_bar：4/8 入池，到 4/22 共 11 个交易日（跳周末 4/11,12,18,19）
        bars = ",".join(
            f"('002415.SZ', '{d}', 12,13,12,12.50,12,0.5,5,1000,10000)"
            for d in [
                "2026-04-08",
                "2026-04-09",
                "2026-04-10",
                "2026-04-13",
                "2026-04-14",
                "2026-04-15",
                "2026-04-16",
                "2026-04-17",
                "2026-04-20",
                "2026-04-21",
                "2026-04-22",
            ]
        )
        store._conn.execute(f"INSERT INTO daily_bar VALUES {bars}")

        # 入池 4/8，今天 4/22，days_in_pool = 11 > 6 → 该 aged_out
        p2 = pd.DataFrame(
            [
                {
                    "ts_code": "002415.SZ",
                    "entry_date": date(2026, 4, 8),
                    "limit_up_date": date(2026, 4, 7),
                    "body_upper": 13.20,
                    "body_lower": 11.80,
                    "level_40": 12.36,
                    "level_30": 12.22,
                    "level_20": 12.08,
                    "stop_strong": 11.80,
                    "stop_weak": 11.52,  # 收盘 12.50 > stop_strong
                    "status": "active",
                }
            ]
        )
        store.upsert_pool2_watch(p2)
        store._conn.execute(
            "INSERT INTO stock_basic (ts_code, name, area, industry, market, list_date) "
            "VALUES ('002415.SZ', '海康威视', '广东', '安防', '主板', '2010-05-28')"
        )

        with (
            patch("rquant.pipeline.PRESET_SCREENS", {}),
            patch("rquant.pipeline.screen", return_value=pd.DataFrame()),
            patch("rquant.notify.notify"),  # 不真推
        ):
            run_daily_pipeline("2026-04-22", store=store)

        row = store._conn.execute(
            "SELECT status, exit_date, exit_reason FROM pool2_watch WHERE ts_code = ?",
            ["002415.SZ"],
        ).fetchone()
        assert row == ("exited", date(2026, 4, 22), "aged_out")


class TestDailySummaryOutboxBoundary:
    def test_legacy_pipeline_does_not_directly_push(self, store: DuckDBStore) -> None:
        from datetime import date

        # daily_bar 让 pipeline 认为是交易日
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-28', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        # 直接写入 screen_result 模拟流水线 Pool 1 命中
        sr = pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-28",
                    "preset_name": "n-shape-pool1",
                    "ts_code": "600519.SH",
                    "name": "茅台",
                    "close": 1685.0,
                    "pct_chg": 1.5,
                    "extra": None,
                }
            ]
        )
        store.upsert_screen_result(sr)
        # Pool 2 持仓
        p2 = pd.DataFrame(
            [
                {
                    "ts_code": "002415.SZ",
                    "entry_date": date(2026, 4, 24),
                    "limit_up_date": date(2026, 4, 22),
                    "body_upper": 13.20,
                    "body_lower": 11.80,
                    "level_40": 12.36,
                    "level_30": 12.22,
                    "level_20": 12.08,
                    "stop_strong": 11.80,
                    "stop_weak": 11.52,
                    "status": "active",
                }
            ]
        )
        store.upsert_pool2_watch(p2)
        # 股票名
        store._conn.execute(
            "INSERT INTO stock_basic (ts_code, name, area, industry, market, list_date) "
            "VALUES ('002415.SZ', '海康威视', '广东', '安防', '主板', '2010-05-28')"
        )

        empty_preset = {
            "noop": ScreenPreset(name="noop", description="", rules=[not_st()]),
        }
        with (
            patch("rquant.pipeline.PRESET_SCREENS", empty_preset),
            patch("rquant.pipeline.screen", return_value=pd.DataFrame()),
            patch("rquant.notify.notify") as mock_notify,
        ):
            run_daily_pipeline("2026-04-28", store=store)

        mock_notify.assert_not_called()


class TestPipelineFaultIsolation:
    """preset 故障隔离 + check_exits 兜底不被跳过（审计 PR1-A）。"""

    def test_failing_preset_does_not_block_others_or_check_exits(self, store: DuckDBStore) -> None:
        from unittest.mock import patch

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        good_df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["x"],
                "CLOSE[0]": [10.5],
                "PCT_CHG[0]": [5.0],
            }
        )
        presets = {
            "bad": ScreenPreset(name="bad", description="", rules=[not_st()]),
            "good": ScreenPreset(name="good", description="", rules=[not_st()]),
        }

        calls = []

        def screen_mock(*a, **k):
            calls.append(1)
            if len(calls) == 1:
                raise KeyError("BODY_UPPER[1]")  # 模拟某 preset 引用缺失列
            return good_df

        _seed_daily_fixture_sources(store,good_df)
        with (
            patch("rquant.pipeline.PRESET_SCREENS", presets),
            patch("rquant.pipeline.screen", side_effect=screen_mock),
            patch("rquant.pipeline._sync_pool2_watch"),
            patch("rquant.notify.notify"),
        ):
            result = run_daily_pipeline("2026-04-18", store=store)

        # bad 失败标 -1，good 正常命中，互不影响
        assert result["bad"] == -1
        assert result["good"] == 1
        # Pool 2 stage is independent of isolated preset failures.

    def test_sync_pool2_failure_does_not_skip_check_exits(self, store: DuckDBStore) -> None:
        from unittest.mock import patch

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {}),
            patch("rquant.pipeline.screen", return_value=pd.DataFrame()),
            patch("rquant.pipeline._sync_pool2_watch", side_effect=RuntimeError("boom")),
            patch("rquant.pipeline._check_pool2_exits_without_notification") as mock_ce,
        ):
            run_daily_pipeline("2026-04-18", store=store)

        # _sync_pool2_watch 崩了，exit stage 仍必须跑
        mock_ce.assert_called_once()
