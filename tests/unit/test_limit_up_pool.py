"""涨停池每日采集测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import rquant.limit_up_pool as limit_up_pool
from rquant.limit_up_pool import capture_zt_pool, normalize_zt_pool, to_ts_code
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _raw_zt_pool() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "序号": 1, "代码": "002273", "名称": "水晶光电", "涨跌幅": 10.02,
            "最新价": 20.5, "成交额": 1.5e9, "流通市值": 2.0e10,
            "总市值": 2.5e10, "换手率": 7.5, "封板资金": 3.2e8,
            "首次封板时间": "092500", "最后封板时间": "142000",
            "炸板次数": 1, "涨停统计": "3/2", "连板数": 2,
            "所属行业": "光学光电子",
        },
        {
            "序号": 2, "代码": "600519", "名称": "贵州茅台", "涨跌幅": 10.0,
            "最新价": 1800.0, "成交额": 9.9e9, "流通市值": 2.2e12,
            "总市值": 2.2e12, "换手率": 0.5, "封板资金": 8.8e8,
            # akshare 偶发返回 int 时间：92500 丢首位 0
            "首次封板时间": 92500, "最后封板时间": 145900,
            "炸板次数": 0, "涨停统计": "1/1", "连板数": 1,
            "所属行业": "白酒",
        },
        {
            "序号": 3, "代码": "430047", "名称": "诺思兰德", "涨跌幅": 30.0,
            "最新价": 12.0, "成交额": 1.0e8, "流通市值": 1.0e9,
            "总市值": 1.5e9, "换手率": 15.0, "封板资金": 5.0e7,
            "首次封板时间": "100000", "最后封板时间": "100000",
            "炸板次数": 0, "涨停统计": "1/1", "连板数": 1,
            "所属行业": "生物制品",
        },
    ])


class TestToTsCode:
    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [
            ("600000", "600000.SH"),
            ("688981", "688981.SH"),
            ("002273", "002273.SZ"),
            ("000001", "000001.SZ"),
            ("300750", "300750.SZ"),
            ("430047", "430047.BJ"),
            ("839167", "839167.BJ"),
            ("920108", "920108.BJ"),
        ],
    )
    def test_exchange_mapping(self, symbol: str, expected: str) -> None:
        assert to_ts_code(symbol) == expected

    @pytest.mark.parametrize("symbol", ["123456", "12345", "abcdef", "", "60000A"])
    def test_unmappable_returns_none(self, symbol: str) -> None:
        assert to_ts_code(symbol) is None


class TestNormalizeZtPool:
    def test_maps_all_fields(self) -> None:
        trading_date = date(2026, 7, 2)

        df = normalize_zt_pool(_raw_zt_pool(), trading_date)

        assert len(df) == 3
        assert set(df["ts_code"]) == {"002273.SZ", "600519.SH", "430047.BJ"}
        row = df[df["ts_code"] == "002273.SZ"].iloc[0]
        assert row["trade_date"] == trading_date
        assert row["name"] == "水晶光电"
        assert row["pct_chg"] == 10.02
        assert row["close"] == 20.5
        assert row["amount"] == 1.5e9
        assert row["circ_mv"] == 2.0e10
        assert row["total_mv"] == 2.5e10
        assert row["turnover_rate"] == 7.5
        assert row["seal_amount"] == 3.2e8
        assert row["first_seal_time"] == "092500"
        assert row["last_seal_time"] == "142000"
        assert int(row["break_count"]) == 1
        assert row["limit_up_stat"] == "3/2"
        assert int(row["consecutive_boards"]) == 2
        assert row["industry"] == "光学光电子"
        assert row["source"] == "eastmoney"

    def test_int_seal_time_zero_padded(self) -> None:
        df = normalize_zt_pool(_raw_zt_pool(), date(2026, 7, 2))

        row = df[df["ts_code"] == "600519.SH"].iloc[0]
        assert row["first_seal_time"] == "092500"
        assert row["last_seal_time"] == "145900"

    def test_drops_unmappable_symbol(self) -> None:
        raw = _raw_zt_pool()
        raw.loc[0, "代码"] = "123456"

        df = normalize_zt_pool(raw, date(2026, 7, 2))

        assert len(df) == 2
        assert "123456" not in set(df["ts_code"])

    def test_missing_code_column_raises(self) -> None:
        with pytest.raises(ValueError, match="代码"):
            normalize_zt_pool(pd.DataFrame([{"名称": "x"}]), date(2026, 7, 2))

    def test_empty_raw_returns_empty(self) -> None:
        assert normalize_zt_pool(pd.DataFrame(), date(2026, 7, 2)).empty


class TestCaptureZtPool:
    def test_capture_writes_to_store(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetched: list[str] = []

        def fake_fetch(ds: str) -> pd.DataFrame:
            fetched.append(ds)
            return _raw_zt_pool()

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", fake_fetch)

        rows = capture_zt_pool(date(2026, 7, 2), store)

        assert rows == 3
        assert fetched == ["20260702"]
        out = store.query_limit_up_pool(date(2026, 7, 2))
        assert len(out) == 3
        # 连板数倒序，2 连板在最前
        assert out.iloc[0]["ts_code"] == "002273.SZ"
        assert out.iloc[0]["consecutive_boards"] == 2

    def test_capture_is_idempotent(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            limit_up_pool, "_fetch_zt_pool", lambda ds: _raw_zt_pool()
        )

        capture_zt_pool(date(2026, 7, 2), store)
        capture_zt_pool(date(2026, 7, 2), store)

        assert len(store.query_limit_up_pool(date(2026, 7, 2))) == 3

    def test_capture_defaults_to_today(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetched: list[str] = []

        def fake_fetch(ds: str) -> pd.DataFrame:
            fetched.append(ds)
            return _raw_zt_pool()

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", fake_fetch)

        capture_zt_pool(store=store)

        assert fetched == [date.today().strftime("%Y%m%d")]

    def test_fetch_failure_returns_zero(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(ds: str) -> pd.DataFrame:
            raise RuntimeError("eastmoney blocked")

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", boom)

        assert capture_zt_pool(date(2026, 7, 2), store) == 0
        assert store.query_limit_up_pool(date(2026, 7, 2)).empty

    def test_empty_result_returns_zero(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            limit_up_pool, "_fetch_zt_pool", lambda ds: pd.DataFrame()
        )

        assert capture_zt_pool(date(2026, 7, 2), store) == 0

    def test_column_change_returns_zero_not_raise(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """东财列名变更（缺 '代码'）不炸：warning + 0，日终链路容忍缺采。"""
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: pd.DataFrame([{"名称": "x"}]),
        )

        assert capture_zt_pool(date(2026, 7, 2), store) == 0
