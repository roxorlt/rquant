"""panorama_data 单测：快照归一化、涨停价对拍 derive、脉搏计数、板块聚合、
资金流归一化、只读库读取（tmp_path 库）与外部源失败空态。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant import panorama_data
from rquant.panorama_data import (
    add_limit_prices,
    aggregate_board_amount,
    board_constituents,
    compute_market_pulse,
    fetch_market_snapshot,
    fetch_sector_fund_flow,
    industry_fallback_members,
    load_board_members,
    load_pool_flags,
)
from rquant.state.derive import derive_state
from rquant.storage.duckdb import DuckDBStore


def _sina_spot_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "代码": ["sh600519", "sz300750", "sh688981", "bj920008", "sh600222", "xx123"],
            "名称": ["贵州茅台", "宁德时代", "中芯国际", "某北交所", "*ST海润", "坏行"],
            "最新价": [1500.0, 250.0, 90.0, 13.0, "3.47", 1.0],
            "今开": [1480.0, 245.0, 88.0, 12.5, 3.40, 1.0],
            "最高": [1510.0, 252.0, 91.0, 13.2, 3.47, 1.0],
            "最低": [1475.0, 244.0, 87.5, 12.4, 3.38, 1.0],
            "昨收": [1490.0, 240.0, 89.0, 12.0, 3.30, 1.0],
            "涨跌幅": [0.67, 4.17, 1.12, 8.33, None, 0.0],
            "成交量": [1e6, 2e7, 3e7, 5e5, 1e6, 0],
            "成交额": [1.5e9, 5e9, 2.7e9, 6.5e6, 3.4e6, 0],
        }
    )


class TestFetchMarketSnapshot:
    def test_normalizes_columns_and_codes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(panorama_data, "_fetch_spot", _sina_spot_df)
        df = fetch_market_snapshot()
        # 'xx123' 后 6 位 'xx123' 非法代码被丢弃（实际 5 位）
        assert set(df["ts_code"]) == {
            "600519.SH", "300750.SZ", "688981.SH", "920008.BJ", "600222.SH",
        }
        row = df[df["ts_code"] == "600519.SH"].iloc[0]
        assert row["price"] == 1500.0
        assert row["pre_close"] == 1490.0
        # 字符串数值被 coerce
        st_row = df[df["ts_code"] == "600222.SH"].iloc[0]
        assert st_row["price"] == pytest.approx(3.47)
        # pct_chg 缺失时用 price/pre_close 兜底
        assert st_row["pct_chg"] == pytest.approx((3.47 / 3.30 - 1) * 100)

    def test_fetch_failure_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def boom() -> pd.DataFrame:
            raise ConnectionError("sina down")

        monkeypatch.setattr(panorama_data, "_fetch_spot", boom)
        assert fetch_market_snapshot().empty

    def test_missing_required_column_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            panorama_data, "_fetch_spot", lambda: _sina_spot_df().drop(columns=["昨收"])
        )
        assert fetch_market_snapshot().empty


class TestLimitPrices:
    def test_matches_derive_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """对拍：add_limit_prices 的涨停/跌停价必须与 derive_state 逐票一致。

        覆盖主板 10% / 创业板 20% / 科创板 20% / 北交所 30% / ST 5%，
        且含 half-up 舍入敏感样本（3.30×1.05=3.465 → 3.47，银行家舍入会给 3.46）。
        """
        monkeypatch.setattr(panorama_data, "_fetch_spot", _sina_spot_df)
        snap = add_limit_prices(fetch_market_snapshot())
        for _, row in snap.iterrows():
            daily = pd.DataFrame(
                {
                    "trade_date": [date(2026, 7, 3)],
                    "open": [row["open"]],
                    "high": [row["high"]],
                    "low": [row["low"]],
                    "close": [row["price"]],
                    "pre_close": [row["pre_close"]],
                }
            )
            expected = derive_state(daily, row["ts_code"], name=row["name"]).iloc[0]
            assert row["limit_pct"] == expected["limit_pct"], row["ts_code"]
            assert row["limit_up_price"] == expected["limit_up_price"], row["ts_code"]
            assert row["limit_down_price"] == expected["limit_down_price"], row["ts_code"]

    def test_st_half_up_rounding(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(panorama_data, "_fetch_spot", _sina_spot_df)
        snap = add_limit_prices(fetch_market_snapshot())
        st_row = snap[snap["ts_code"] == "600222.SH"].iloc[0]
        assert st_row["limit_pct"] == 0.05
        assert st_row["limit_up_price"] == pytest.approx(3.47)  # half-up，非 3.46

    def test_empty_passthrough(self) -> None:
        assert add_limit_prices(pd.DataFrame()).empty


class TestMarketPulse:
    def _snapshot(self) -> pd.DataFrame:
        # 全部主板 10%：pre_close 10 → 涨停 11.00 / 跌停 9.00
        rows = [
            # (code, price, high, pre_close)
            ("600001.SH", 11.00, 11.00, 10.0),  # 涨停
            ("600002.SH", 10.50, 11.00, 10.0),  # 炸板（触 11 回落）
            ("600003.SH", 9.00, 9.80, 10.0),    # 跌停 + 下跌
            ("600004.SH", 10.20, 10.30, 10.0),  # 上涨
            ("600005.SH", 9.90, 10.10, 10.0),   # 下跌
            ("600006.SH", 10.00, 10.05, 10.0),  # 平盘
            ("600007.SH", 0.0, 0.0, 10.0),      # 停牌，剔除
        ]
        df = pd.DataFrame(rows, columns=["ts_code", "price", "high", "pre_close"])
        df["name"] = "普通票"
        return add_limit_prices(df)

    def test_counts(self) -> None:
        pulse = compute_market_pulse(self._snapshot())
        assert pulse.total_count == 6
        assert pulse.limit_up_count == 1
        assert pulse.broken_count == 1
        assert pulse.limit_down_count == 1
        assert pulse.up_count == 3  # 涨停 + 炸板回落(10.5) + 10.2
        assert pulse.down_count == 2
        assert pulse.flat_count == 1
        assert pulse.up_ratio_pct == pytest.approx(50.0)

    def test_empty_returns_zero_pulse(self) -> None:
        pulse = compute_market_pulse(pd.DataFrame())
        assert pulse.total_count == 0
        assert pulse.up_ratio_pct is None

    def test_missing_limit_columns_returns_zero_pulse(self) -> None:
        df = pd.DataFrame({"price": [10.0], "pre_close": [9.0]})
        assert compute_market_pulse(df).total_count == 0


def _em_flow_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "序号": [1, 2, 3],
            "名称": ["半导体", "白酒", "券商"],
            "今日涨跌幅": [3.2, -0.5, 1.1],
            "今日主力净流入-净额": [5.2e9, -1.1e9, 2.3e9],
            "今日主力净流入-净占比": [8.5, -2.1, 3.3],
            "今日主力净流入最大股": ["中芯国际", "贵州茅台", "东方财富"],
        }
    )


class TestSectorFundFlow:
    def test_normalize_with_indicator_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(panorama_data, "_fetch_sector_fund_flow_raw", lambda s: _em_flow_df())
        df = fetch_sector_fund_flow()
        assert list(df.columns) == [
            "board_name", "pct_chg", "main_net_amount", "main_net_rate", "leading_stock",
        ]
        # 按净流入额降序
        assert df["board_name"].tolist() == ["半导体", "券商", "白酒"]
        assert df["main_net_amount"].iloc[0] == pytest.approx(5.2e9)
        assert df["main_net_rate"].iloc[0] == pytest.approx(8.5)

    def test_five_day_prefix_still_parses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        renamed = _em_flow_df().rename(columns=lambda c: str(c).replace("今日", "5日"))
        monkeypatch.setattr(panorama_data, "_fetch_sector_fund_flow_raw", lambda s: renamed)
        df = fetch_sector_fund_flow()
        assert not df.empty
        assert df["board_name"].iloc[0] == "半导体"

    def test_fetch_failure_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def boom(sector_type: str) -> pd.DataFrame:
            raise ConnectionError("em down")

        monkeypatch.setattr(panorama_data, "_fetch_sector_fund_flow_raw", boom)
        assert fetch_sector_fund_flow().empty

    def test_missing_required_column_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        broken = _em_flow_df().drop(columns=["今日主力净流入-净额"])
        monkeypatch.setattr(panorama_data, "_fetch_sector_fund_flow_raw", lambda s: broken)
        assert fetch_sector_fund_flow().empty


class TestBoardAggregation:
    def _members(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "board_code": ["BK1", "BK1", "BK2", "BK2", "BK9"],
                "board_name": ["半导体", "半导体", "白酒", "白酒", "空概念"],
                "idx_type": ["行业板块"] * 4 + ["概念板块"],
                "con_code": ["600001.SH", "600002.SH", "600003.SH", "999999.SH", "600001.SH"],
            }
        )

    def _snapshot(self) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "ts_code": ["600001.SH", "600002.SH", "600003.SH"],
                "name": ["甲", "乙", "丙"],
                "price": [11.00, 10.50, 9.50],
                "high": [11.00, 11.00, 9.80],
                "pre_close": [10.0, 10.0, 10.0],
                "pct_chg": [10.0, 5.0, -5.0],
                "amount": [2e8, 1e8, 4e8],
            }
        )
        return add_limit_prices(df)

    def test_aggregate_amount_and_limit_count(self) -> None:
        agg = aggregate_board_amount(self._snapshot(), self._members(), idx_type="行业板块")
        assert agg["board_code"].tolist() == ["BK2", "BK1"]  # 4e8 > 2e8+1e8
        bk1 = agg[agg["board_code"] == "BK1"].iloc[0]
        assert bk1["amount"] == pytest.approx(3e8)
        assert bk1["limit_up_count"] == 1  # 600001 涨停
        assert bk1["stock_count"] == 2
        assert bk1["pct_chg_median"] == pytest.approx(7.5)
        bk2 = agg[agg["board_code"] == "BK2"].iloc[0]
        assert bk2["stock_count"] == 1  # 999999.SH 快照无行情，不计

    def test_aggregate_empty_inputs(self) -> None:
        assert aggregate_board_amount(pd.DataFrame(), self._members()).empty
        assert aggregate_board_amount(self._snapshot(), pd.DataFrame()).empty
        assert aggregate_board_amount(self._snapshot(), self._members(), idx_type="地域板块").empty

    def test_constituents_with_pool_flags(self) -> None:
        flags = {"600001.SH": "pool1:demo + pool2", "600003.SH": "pool2"}
        cons = board_constituents("BK1", self._members(), self._snapshot(), flags)
        assert cons["ts_code"].tolist() == ["600001.SH", "600002.SH"]  # amount 降序
        assert cons.iloc[0]["pools"] == "pool1:demo + pool2"
        assert cons.iloc[1]["pools"] == ""
        assert bool(cons.iloc[0]["is_limit_up"]) is True
        assert bool(cons.iloc[1]["is_limit_up"]) is False

    def test_constituents_unknown_board_empty(self) -> None:
        assert board_constituents("BK404", self._members(), self._snapshot()).empty


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestDuckDBReaders:
    def _seed_boards(self, store: DuckDBStore) -> None:
        store._conn.execute(
            """
            INSERT INTO dc_board (ts_code, name, idx_type)
            VALUES ('BK0001.DC', '半导体', '行业板块'),
                   ('BK0002.DC', 'AI眼镜', '概念板块'),
                   ('BK0003.DC', '广东板块', '地域板块')
            """
        )
        store._conn.execute(
            """
            INSERT INTO dc_board_member (board_code, con_code, con_name)
            VALUES ('BK0001.DC', '688981.SH', '中芯国际'),
                   ('BK0002.DC', '002241.SZ', '歌尔股份'),
                   ('BK0003.DC', '000001.SZ', '平安银行')
            """
        )

    def test_load_board_members_excludes_region(self, store: DuckDBStore) -> None:
        self._seed_boards(store)
        df = load_board_members(store=store)
        assert set(df["idx_type"]) == {"行业板块", "概念板块"}  # 地域板块被排除
        assert set(df["con_code"]) == {"688981.SH", "002241.SZ"}
        row = df[df["board_code"] == "BK0001.DC"].iloc[0]
        assert row["board_name"] == "半导体"

    def test_load_board_members_empty_table(self, store: DuckDBStore) -> None:
        assert load_board_members(store=store).empty

    def test_industry_fallback_members(self, store: DuckDBStore) -> None:
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, symbol, name, industry)
            VALUES ('600519.SH', '600519', '贵州茅台', '白酒'),
                   ('600000.SH', '600000', '浦发银行', NULL)
            """
        )
        df = industry_fallback_members(store=store)
        assert df["con_code"].tolist() == ["600519.SH"]  # industry 为空的不进兜底
        assert df.iloc[0]["board_name"] == "白酒"
        assert df.iloc[0]["idx_type"] == "行业板块"

    def test_load_pool_flags_latest_date_and_active_only(self, store: DuckDBStore) -> None:
        store._conn.execute(
            """
            INSERT INTO screen_result (trade_date, preset_name, ts_code, name)
            VALUES ('2026-07-01', 'old_preset', '600000.SH', '旧票'),
                   ('2026-07-02', 'demo', '600519.SH', '贵州茅台'),
                   ('2026-07-02', 'demo2', '600519.SH', '贵州茅台')
            """
        )
        store._conn.execute(
            """
            INSERT INTO pool2_watch (ts_code, entry_date, limit_up_date, body_upper,
                                     body_lower, level_40, level_30, level_20,
                                     stop_strong, stop_weak, status)
            VALUES ('600519.SH', '2026-07-01', '2026-06-30', 10, 9, 9.6, 9.4, 9.2, 9, 8.8,
                    'active'),
                   ('000001.SZ', '2026-06-01', '2026-05-30', 10, 9, 9.6, 9.4, 9.2, 9, 8.8,
                    'exited')
            """
        )
        flags = load_pool_flags(store=store)
        # 只取最新 trade_date 的 pool1；已退出的 pool2 不标
        assert "600000.SH" not in flags
        assert "000001.SZ" not in flags
        assert flags["600519.SH"] == "pool1:demo + pool1:demo2 + pool2"

    def test_load_pool_flags_empty(self, store: DuckDBStore) -> None:
        assert load_pool_flags(store=store) == {}
