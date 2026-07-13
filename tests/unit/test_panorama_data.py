"""panorama_data 单测：快照归一化、涨停价对拍 derive、脉搏计数、板块聚合、
资金流归一化、只读库读取（tmp_path 库）与外部源失败空态。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant import panorama_data
from rquant.panorama_data import (
    add_limit_prices,
    aggregate_board_amount,
    aggregate_board_limit_ups,
    board_constituents,
    compute_market_pulse,
    fetch_market_snapshot,
    fetch_sector_fund_flow,
    industry_fallback_members,
    load_board_members,
    load_kpl_concept_members,
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


def _kill_em_spot_tiers(monkeypatch: pytest.MonkeyPatch) -> None:
    """打死东财两级（Session.get 抛错 + 置空 SOCKS 禁用该级），强制快照走 sina 兜底。"""
    import requests

    def boom(self: object, url: str, **kwargs: object) -> None:
        raise ConnectionError("em down")

    monkeypatch.setattr(requests.Session, "get", boom)
    monkeypatch.setenv("RQUANT_PANORAMA_SOCKS", "")


class TestFetchMarketSnapshot:
    def test_normalizes_columns_and_codes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _kill_em_spot_tiers(monkeypatch)
        monkeypatch.setattr(panorama_data, "_fetch_spot", _sina_spot_df)
        df = fetch_market_snapshot()
        assert df.attrs["route"] == "sina"
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

        _kill_em_spot_tiers(monkeypatch)
        monkeypatch.setattr(panorama_data, "_fetch_spot", boom)
        df = fetch_market_snapshot()
        assert df.empty
        assert df.attrs["route"] == "none"

    def test_missing_required_column_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _kill_em_spot_tiers(monkeypatch)
        monkeypatch.setattr(
            panorama_data, "_fetch_spot", lambda: _sina_spot_df().drop(columns=["昨收"])
        )
        assert fetch_market_snapshot().empty


# 东财 push2 clist 股票 diff 行（f5 成交量单位手；停牌票数值字段给 "-"）
_EM_SPOT_ROWS: list[dict] = [
    {"f12": "600519", "f14": "贵州茅台", "f2": 1500.0, "f17": 1480.0, "f15": 1510.0,
     "f16": 1475.0, "f18": 1490.0, "f3": 0.67, "f5": 10000, "f6": 1.5e9},
    {"f12": "300750", "f14": "宁德时代", "f2": 250.0, "f17": 245.0, "f15": 252.0,
     "f16": 244.0, "f18": 240.0, "f3": 4.17, "f5": 200000, "f6": 5e9},
    {"f12": "920008", "f14": "某北交所", "f2": "-", "f17": "-", "f15": "-",
     "f16": "-", "f18": 12.0, "f3": "-", "f5": "-", "f6": "-"},
]

_SNAPSHOT_COLUMNS = [
    "ts_code", "name", "price", "open", "high", "low", "pre_close",
    "pct_chg", "volume", "amount",
]


class TestSnapshotRouting:
    """全市场快照三级路由：东财 clist 直连 → 同实现走 SOCKS → 新浪兜底 → 全败空表。"""

    def test_em_direct_normalizes_volume_to_shares(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[dict] = []
        sessions: list[object] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            sessions.append(self)
            seen.append(kwargs)
            return _FakeResp({"data": {"diff": _EM_SPOT_ROWS, "total": len(_EM_SPOT_ROWS)}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_market_snapshot()
        assert df.attrs["route"] == "em_direct"
        assert seen[0]["proxies"] is None  # 直连级不带代理
        assert seen[0]["params"]["fs"] == panorama_data._EM_SPOT_FS
        assert sessions[0].trust_env is False  # 加固 Session（不吸系统代理环境）
        assert list(df.columns) == _SNAPSHOT_COLUMNS
        mt = df[df["ts_code"] == "600519.SH"].iloc[0]
        assert mt["volume"] == pytest.approx(1e6)  # f5 手 ×100 → 股
        assert mt["amount"] == pytest.approx(1.5e9)  # 成交额已是元，不换算
        assert mt["pct_chg"] == pytest.approx(0.67)
        # 停牌票 "-" 占位 coerce 成 NaN、行保留；涨跌幅缺失时 price/pre_close 兜底
        bj = df[df["ts_code"] == "920008.BJ"].iloc[0]
        assert pd.isna(bj["price"]) and pd.isna(bj["volume"])

    def test_em_rows_missing_key_fields_fall_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """em 返回 200 但缺 f12/f2 关键字段 → 该级视为空，落到 sina 兜底。"""
        rows = [{"f14": "贵州茅台", "f3": 0.67}]

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            return _FakeResp({"data": {"diff": rows, "total": 1}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setenv("RQUANT_PANORAMA_SOCKS", "")
        monkeypatch.setattr(panorama_data, "_fetch_spot", _sina_spot_df)
        df = fetch_market_snapshot()
        assert df.attrs["route"] == "sina"

    def test_direct_fails_socks_takes_over(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[dict] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            seen.append(kwargs)
            if kwargs["proxies"] is None:
                raise ConnectionError("blacklisted")
            return _FakeResp({"data": {"diff": _EM_SPOT_ROWS, "total": len(_EM_SPOT_ROWS)}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setenv("RQUANT_PANORAMA_SOCKS", "socks5h://127.0.0.1:9999")
        df = fetch_market_snapshot()
        assert df.attrs["route"] == "em_socks"
        assert seen[0]["proxies"] is None  # 先试直连
        assert seen[1]["proxies"] == {
            "http": "socks5h://127.0.0.1:9999",
            "https": "socks5h://127.0.0.1:9999",
        }
        assert list(df.columns) == _SNAPSHOT_COLUMNS
        mt = df[df["ts_code"] == "600519.SH"].iloc[0]
        assert mt["price"] == pytest.approx(1500.0)
        assert mt["volume"] == pytest.approx(1e6)  # f5 手 ×100 → 股

    def test_direct_paginates_until_total(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """服务端钳制单页行数时按 total 累积翻页取全量（直连/SOCKS 共用实现）。"""
        pages: list[int] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            params = kwargs["params"]
            pages.append(params["pn"])
            n = 100 if params["pn"] == 1 else 50
            start = (params["pn"] - 1) * 100
            diff = [
                {"f12": f"{600000 + start + i:06d}", "f14": f"股{start + i}", "f2": 10.0,
                 "f17": 9.9, "f15": 10.2, "f16": 9.8, "f18": 9.9, "f3": 1.0,
                 "f5": 1000, "f6": 1e6}
                for i in range(n)
            ]
            return _FakeResp({"data": {"diff": diff, "total": 150}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_market_snapshot()
        assert pages == [1, 2]
        assert len(df) == 150
        assert df.attrs["route"] == "em_direct"

    def test_both_em_fail_sina_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setattr(panorama_data, "_fetch_spot", _sina_spot_df)
        df = fetch_market_snapshot()
        assert df.attrs["route"] == "sina"
        # sina 成交量已是股，不做 ×100 换算
        mt = df[df["ts_code"] == "600519.SH"].iloc[0]
        assert mt["volume"] == pytest.approx(1e6)

    def test_allow_sina_false_skips_sina_tier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """allow_sina=False（poller 对 sina 熔断中）→ 不碰 sina，直接空表。"""

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        sina_calls: list[int] = []

        def sina_spy() -> pd.DataFrame:
            sina_calls.append(1)
            return _sina_spot_df()

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setattr(panorama_data, "_fetch_spot", sina_spy)
        df = fetch_market_snapshot(allow_sina=False)
        assert df.empty
        assert df.attrs["route"] == "none"
        assert sina_calls == []  # sina 级完全未被调用

    def test_all_routes_fail_empty_route_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        def sina_boom() -> pd.DataFrame:
            raise ConnectionError("sina down")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setattr(panorama_data, "_fetch_spot", sina_boom)
        df = fetch_market_snapshot()
        assert df.empty
        assert df.attrs["route"] == "none"

    def test_direct_tier_no_akshare_em_spot(self) -> None:
        """直连级已改为自实现 clist（可注入加固 headers），不再调用 ak.stock_zh_a_spot_em。"""
        import inspect

        src = inspect.getsource(panorama_data)
        assert "stock_zh_a_spot_em(" not in src  # 无调用点（注释里的名字提及不算）
        assert not hasattr(panorama_data, "_fetch_em_spot_direct")


class TestLimitPrices:
    def test_matches_derive_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """对拍：add_limit_prices 的涨停/跌停价必须与 derive_state 逐票一致。

        覆盖主板 10% / 创业板 20% / 科创板 20% / 北交所 30% / ST 5%，
        且含 half-up 舍入敏感样本（3.30×1.05=3.465 → 3.47，银行家舍入会给 3.46）。
        """
        _kill_em_spot_tiers(monkeypatch)
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
            compact_name = str(row["name"]).upper().replace(" ", "")
            status = pd.DataFrame(
                {
                    "ts_code": [row["ts_code"]],
                    "trade_date": [date(2026, 7, 3)],
                    "name": [row["name"]],
                    "is_st": pd.Series(
                        [compact_name.startswith(("ST", "*ST", "SST"))],
                        dtype="boolean",
                    ),
                    "available_at": [
                        datetime(2026, 7, 3, 9, 25, tzinfo=ZoneInfo("Asia/Shanghai"))
                    ],
                    "conflict_reason": [None],
                }
            )
            expected = derive_state(daily, row["ts_code"], status).iloc[0]
            assert row["limit_pct"] == expected["limit_pct"], row["ts_code"]
            assert row["limit_up_price"] == expected["limit_up_price"], row["ts_code"]
            assert row["limit_down_price"] == expected["limit_down_price"], row["ts_code"]

    def test_st_half_up_rounding(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _kill_em_spot_tiers(monkeypatch)
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


# 东财 push2 diff 行（字段结构对齐 2026-07-03 实测 JSON）
_EM_ROWS: list[dict] = [
    {"f12": "BK1211", "f14": "汽车", "f2": 4117.15, "f3": 3.89,
     "f62": 7549719296.0, "f184": 7.28, "f204": "比亚迪"},
    {"f12": "BK0481", "f14": "汽车零部件", "f2": 46786.0, "f3": 4.29,
     "f62": 6368454912.0, "f184": 7.52, "f204": "拓普集团"},
    {"f12": "BK0475", "f14": "银行", "f2": 3300.0, "f3": "-",
     "f62": -1.1e9, "f184": "-", "f204": "-"},
]

_FLOW_COLUMNS = [
    "board_code", "board_name", "pct_chg", "main_net_amount", "main_net_rate", "leading_stock",
]


def _ths_flow_df() -> pd.DataFrame:
    # 同花顺即时口径实测列（概念接口同样用「行业」列名，资金单位亿元）
    return pd.DataFrame(
        {
            "序号": [1, 2],
            "行业": ["贵金属", "电机"],
            "行业指数": [4861.87, 3781.74],
            "行业-涨跌幅": [6.04, 5.90],
            "流入资金": [144.56, 43.38],
            "流出资金": [130.69, 33.14],
            "净额": [13.87, 10.25],
            "公司家数": [14, 26],
            "领涨股": ["西部黄金", "江苏雷利"],
            "领涨股-涨跌幅": [10.02, 13.57],
            "当前价": [25.91, 33.06],
        }
    )


class _FakeResp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


class TestSectorFundFlowRouting:
    def test_direct_success_maps_em_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[dict] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            calls.append(kwargs)
            return _FakeResp({"data": {"diff": _EM_ROWS, "total": len(_EM_ROWS)}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_sector_fund_flow()
        assert df.attrs["route"] == "em_direct"
        assert calls[0]["proxies"] is None
        assert list(df.columns) == _FLOW_COLUMNS
        # 按 f62 主力净流入额降序，单位元
        assert df["board_name"].tolist() == ["汽车", "汽车零部件", "银行"]
        assert df["main_net_amount"].iloc[0] == pytest.approx(7549719296.0)
        assert df["pct_chg"].iloc[0] == pytest.approx(3.89)
        assert df["main_net_rate"].iloc[0] == pytest.approx(7.28)
        assert df["leading_stock"].iloc[0] == "比亚迪"
        # "-" 占位被 coerce 成 NaN
        bank = df[df["board_name"] == "银行"].iloc[0]
        assert pd.isna(bank["pct_chg"]) and pd.isna(bank["main_net_rate"])

    def test_em_requests_use_hardened_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """东财请求必须走加固 Session：trust_env=False + UA/Referer/Connection:close。"""
        captured: dict = {}

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            captured["trust_env"] = self.trust_env
            captured["headers"] = dict(self.headers)
            return _FakeResp({"data": {"diff": _EM_ROWS, "total": len(_EM_ROWS)}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_sector_fund_flow()
        assert df.attrs["route"] == "em_direct"
        assert captured["trust_env"] is False
        assert "Chrome" in captured["headers"]["User-Agent"]
        assert captured["headers"]["Referer"] == "https://quote.eastmoney.com/"
        assert captured["headers"]["Connection"] == "close"

    def test_direct_paginates_until_total(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """实测单页被限 100 行（total≈496），须翻页取全量。"""
        pages: list[int] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            params = kwargs["params"]
            pages.append(params["pn"])
            row = {"f12": f"BK{params['pn']:04d}", "f14": f"板块{params['pn']}",
                   "f3": 1.0, "f62": 1e9 - params["pn"], "f184": 1.0, "f204": "某股"}
            diff = [row] * (100 if params["pn"] <= 2 else 50)
            return _FakeResp({"data": {"diff": diff, "total": 250}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_sector_fund_flow()
        assert pages == [1, 2, 3]
        assert len(df) == 250

    def test_direct_fails_socks_takes_over(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen_proxies: list[dict | None] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            seen_proxies.append(kwargs["proxies"])
            if kwargs["proxies"] is None:
                raise ConnectionError("RST by firewall")
            return _FakeResp({"data": {"diff": _EM_ROWS, "total": len(_EM_ROWS)}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setenv("RQUANT_PANORAMA_SOCKS", "socks5h://127.0.0.1:9999")
        df = fetch_sector_fund_flow()
        assert df.attrs["route"] == "em_socks"
        assert seen_proxies[0] is None
        assert seen_proxies[1] == {
            "http": "socks5h://127.0.0.1:9999",
            "https": "socks5h://127.0.0.1:9999",
        }
        assert df["board_name"].iloc[0] == "汽车"

    def test_socks_default_proxy_when_env_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen_proxies: list[dict | None] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            seen_proxies.append(kwargs["proxies"])
            if kwargs["proxies"] is None:
                raise ConnectionError("RST")
            return _FakeResp({"data": {"diff": _EM_ROWS, "total": len(_EM_ROWS)}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.delenv("RQUANT_PANORAMA_SOCKS", raising=False)
        df = fetch_sector_fund_flow()
        assert df.attrs["route"] == "em_socks"
        assert seen_proxies[1]["https"] == "socks5h://127.0.0.1:1086"

    def test_both_em_fail_ths_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        seen_types: list[str] = []

        def fake_ths(sector_type: str) -> pd.DataFrame:
            seen_types.append(sector_type)
            return _ths_flow_df()

        monkeypatch.setattr(panorama_data, "_fetch_ths_flow_raw", fake_ths)
        df = fetch_sector_fund_flow("概念资金流")
        assert df.attrs["route"] == "ths"
        assert seen_types == ["概念资金流"]
        assert list(df.columns) == _FLOW_COLUMNS
        # 净额单位亿元 → 元；无主力净占比 → NaN；领涨股为涨幅口径
        assert df["board_name"].tolist() == ["贵金属", "电机"]
        assert df["main_net_amount"].iloc[0] == pytest.approx(13.87e8)
        assert df["pct_chg"].iloc[0] == pytest.approx(6.04)
        assert df["main_net_rate"].isna().all()
        assert df["leading_stock"].tolist() == ["西部黄金", "江苏雷利"]

    def test_ths_missing_leading_stock_column(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        broken = _ths_flow_df().drop(columns=["领涨股", "领涨股-涨跌幅"])
        monkeypatch.setattr(panorama_data, "_fetch_ths_flow_raw", lambda s: broken)
        df = fetch_sector_fund_flow()
        assert df.attrs["route"] == "ths"
        assert df["leading_stock"].isna().all()

    def test_all_routes_fail_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        def ths_boom(sector_type: str) -> pd.DataFrame:
            raise ConnectionError("ths 403")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setattr(panorama_data, "_fetch_ths_flow_raw", ths_boom)
        df = fetch_sector_fund_flow()
        assert df.empty
        assert df.attrs["route"] == "none"

    def test_em_missing_amount_field_falls_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """东财返回 200 但缺 f62 → 该级视为空，落到 THS 兜底。"""
        rows = [{"f12": "BK1", "f14": "汽车", "f3": 1.0}]

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            return _FakeResp({"data": {"diff": rows, "total": 1}})

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setattr(panorama_data, "_fetch_ths_flow_raw", lambda s: _ths_flow_df())
        df = fetch_sector_fund_flow()
        assert df.attrs["route"] == "ths"


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


class TestBoardLimitUpAggregation:
    """板块涨停排行聚合：口径对齐 compute_market_pulse（同容差同判定）。"""

    def _snapshot(self) -> pd.DataFrame:
        # 全部主板 10%：pre_close 10 → 涨停价 11.00
        rows = [
            # (code, price, high)
            ("600001.SH", 11.00, 11.00),  # 涨停
            ("600002.SH", 10.50, 11.00),  # 炸板（触 11 回落）
            ("600003.SH", 11.00, 11.00),  # 涨停
            ("600004.SH", 10.20, 10.30),  # 普通上涨
            ("600005.SH", 0.0, 0.0),      # 停牌：不计入涨停/炸板
        ]
        df = pd.DataFrame(rows, columns=["ts_code", "price", "high"])
        df["pre_close"] = 10.0
        df["name"] = "普通票"
        return add_limit_prices(df)

    def _kpl_members(self) -> pd.DataFrame:
        # 开盘啦口径：无 idx_type 列；题材成分互相重叠
        return pd.DataFrame(
            {
                "board_code": ["KP1", "KP1", "KP1", "KP1", "KP2", "KP2", "KP3"],
                "board_name": ["人形机器人"] * 4 + ["长鑫存储", "长鑫存储", "冷题材"],
                "con_code": [
                    "600001.SH", "600002.SH", "600004.SH", "600005.SH",
                    "600001.SH", "600003.SH", "600004.SH",
                ],
            }
        )

    def test_counts_sort_and_zero_filter(self) -> None:
        agg = aggregate_board_limit_ups(self._snapshot(), self._kpl_members())
        # 冷题材 0 涨停不显示；KP2 涨停占比 100% > KP1 25% 同涨停数时靠前…
        # 此处 KP2 2 涨停 > KP1 1 涨停，直接按涨停数降序
        assert agg["board_code"].tolist() == ["KP2", "KP1"]
        kp1 = agg[agg["board_code"] == "KP1"].iloc[0]
        assert kp1["limit_up_count"] == 1
        assert kp1["broken_count"] == 1
        assert kp1["stock_count"] == 4  # 停牌票计成分数，不计涨停/炸板
        assert kp1["limit_up_ratio_pct"] == pytest.approx(25.0)
        kp2 = agg[agg["board_code"] == "KP2"].iloc[0]
        assert kp2["limit_up_count"] == 2
        assert kp2["broken_count"] == 0
        assert kp2["limit_up_ratio_pct"] == pytest.approx(100.0)
        assert "冷题材" not in set(agg["board_name"])

    def test_tie_break_by_ratio(self) -> None:
        members = pd.DataFrame(
            {
                "board_code": ["A", "A", "B"],
                "board_name": ["大板块", "大板块", "小板块"],
                "con_code": ["600001.SH", "600004.SH", "600003.SH"],
            }
        )
        agg = aggregate_board_limit_ups(self._snapshot(), members)
        # 同为 1 个涨停：小板块 100% 占比排在大板块 50% 前
        assert agg["board_code"].tolist() == ["B", "A"]

    def test_dc_members_idx_type_filter(self) -> None:
        members = pd.DataFrame(
            {
                "board_code": ["BK1", "BK2"],
                "board_name": ["半导体", "AI眼镜"],
                "idx_type": ["行业板块", "概念板块"],
                "con_code": ["600001.SH", "600003.SH"],
            }
        )
        agg = aggregate_board_limit_ups(self._snapshot(), members, idx_type="概念板块")
        assert agg["board_name"].tolist() == ["AI眼镜"]
        # 开盘啦成分无 idx_type 列：idx_type 参数不生效，全量聚合
        kpl = aggregate_board_limit_ups(self._snapshot(), self._kpl_members(), idx_type="概念板块")
        assert not kpl.empty

    def test_snapshot_without_limit_price_degrades_empty(self) -> None:
        raw = self._snapshot().drop(columns=["limit_up_price"])
        assert aggregate_board_limit_ups(raw, self._kpl_members()).empty

    def test_empty_inputs(self) -> None:
        assert aggregate_board_limit_ups(pd.DataFrame(), self._kpl_members()).empty
        assert aggregate_board_limit_ups(self._snapshot(), pd.DataFrame()).empty

    def test_no_limit_up_anywhere_returns_empty(self) -> None:
        df = pd.DataFrame(
            {"ts_code": ["600004.SH"], "price": [10.2], "high": [10.3], "pre_close": [10.0]}
        )
        df["name"] = "普通票"
        snap = add_limit_prices(df)
        assert aggregate_board_limit_ups(snap, self._kpl_members()).empty


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

    def test_load_kpl_concept_members(self, store: DuckDBStore) -> None:
        store._conn.execute(
            """
            INSERT INTO kpl_concept_member
                (board_code, board_name, con_code, con_name, trade_date, description, hot_num)
            VALUES ('000084.KP', '人形机器人', '002472.SZ', '双环传动', '2026-07-02', '理由', 10),
                   ('000129.KP', '长鑫存储概念', '601133.SH', '柏诚股份', '2026-07-02', '理由', 20)
            """
        )
        df = load_kpl_concept_members(store=store)
        assert list(df.columns) == ["board_code", "board_name", "con_code"]
        assert set(df["board_name"]) == {"人形机器人", "长鑫存储概念"}

    def test_load_kpl_concept_members_empty_table(self, store: DuckDBStore) -> None:
        assert load_kpl_concept_members(store=store).empty

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


class TestStrengthScore:
    def test_strength_neutralizes_size(self) -> None:
        """小票放量上攻的强度分应高于巨量但平庸的大票。"""
        import pandas as pd

        from rquant.panorama_data import add_strength_score

        df = pd.DataFrame({
            "ts_code": ["600000.SH", "300001.SZ"],
            "name": ["大票", "小票"],
            "price": [10.5, 11.8],
            "pre_close": [10.0, 10.0],
            "pct_chg": [5.0, 18.0],
            "amount": [5e9, 8e8],
            "limit_up_price": [11.0, 12.0],
        })
        liquidity = pd.DataFrame({
            "ts_code": ["600000.SH", "300001.SZ"],
            "circ_mv": [8_000_000.0, 300_000.0],  # 万元：800亿 vs 30亿
            "avg_amount_5d": [4.8e9, 2.0e8],
        })
        out = add_strength_score(df, liquidity)
        big = out.loc[out.ts_code == "600000.SH", "strength"].iloc[0]
        small = out.loc[out.ts_code == "300001.SZ", "strength"].iloc[0]
        assert small > big
        # 换手强度：小票 8亿/30亿=26.7% >> 大票 50亿/800亿=6.3%
        assert out.loc[out.ts_code == "300001.SZ", "turnover_pct"].iloc[0] > 20

    def test_strength_without_liquidity_degrades(self) -> None:
        """无流动性基准时强度分退化为可得分量均值，不报错。"""
        import pandas as pd

        from rquant.panorama_data import add_strength_score

        df = pd.DataFrame({
            "ts_code": ["600000.SH", "000001.SZ"],
            "price": [10.5, 9.0],
            "pre_close": [10.0, 10.0],
            "pct_chg": [5.0, -10.0],
            "amount": [1e9, 2e9],
            "limit_up_price": [11.0, 11.0],
        })
        out = add_strength_score(df, None)
        assert out["strength"].notna().all()
        assert (
            out.loc[out.ts_code == "600000.SH", "strength"].iloc[0]
            > out.loc[out.ts_code == "000001.SZ", "strength"].iloc[0]
        )
