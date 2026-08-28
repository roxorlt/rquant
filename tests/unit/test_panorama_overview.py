"""panorama v2 数据层单测（U 组 U1-U7）：合并总表 build_board_overview、
个股图表 fetch_intraday_trend / load_daily_kline、fake 模式确定性 fixture。

trends2 网络 mock 替换 requests.get / _fetch_sina_minute_raw 调用点；日K 用
tmp_path 独立店（绝不碰主库）；fake 模式用 monkeypatch.setenv 隔离。
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest

from rquant import panorama_data
from rquant.panorama_data import (
    add_limit_prices,
    aggregate_board_limit_ups,
    build_board_overview,
    fetch_intraday_trend,
    load_daily_kline,
)
from rquant.storage.duckdb import DuckDBStore

_OVERVIEW_COLUMNS = [
    "board_code",
    "board_name",
    "amount",
    "main_net_amount",
    "main_net_rate",
    "pct_chg_median",
    "limit_up_count",
    "broken_count",
    "stock_count",
    "limit_up_ratio_pct",
    "leading_stock",
]


@pytest.fixture(autouse=True)
def _isolate_fake_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A legacy environment flag must never select the Panorama fixture backend."""
    monkeypatch.delenv("RQUANT_PANORAMA_FAKE", raising=False)


@pytest.fixture
def _test_fixture_backend() -> Iterator[None]:
    with panorama_data._panorama_test_fixtures():
        yield


# ── 合并总表 fixtures ──────────────────────────────────────────────────────────


def _snapshot() -> pd.DataFrame:
    # 全部主板 10%：pre_close 10 → 涨停价 11.00
    rows = [
        # (code, price, high, amount)
        ("600001.SH", 11.00, 11.00, 2e8),  # 涨停
        ("600002.SH", 10.50, 11.00, 1e8),  # 炸板（触 11 回落）
        ("600003.SH", 11.00, 11.00, 3e8),  # 涨停
        ("600004.SH", 10.20, 10.30, 4e8),  # 普通上涨
        ("600005.SH", None, None, 0.0),  # 停牌：不计涨停/炸板，pct_chg NaN
    ]
    df = pd.DataFrame(rows, columns=["ts_code", "price", "high", "amount"])
    df["pre_close"] = 10.0
    df["name"] = "普通票"
    df["pct_chg"] = (df["price"] / df["pre_close"] - 1) * 100
    return add_limit_prices(df)


def _dc_members() -> pd.DataFrame:
    # BK0001 半导体（行业）: 600001/600002/600003；BK0002 白酒（行业）: 600004/600005；
    # BK0003 AI（概念）: 600001 —— 东财行业体系应被 idx_type 过滤掉
    return pd.DataFrame(
        {
            "board_code": ["BK0001.DC"] * 3 + ["BK0002.DC"] * 2 + ["BK0003.DC"],
            "board_name": ["半导体"] * 3 + ["白酒"] * 2 + ["AI"],
            "idx_type": ["行业板块"] * 5 + ["概念板块"],
            "con_code": [
                "600001.SH",
                "600002.SH",
                "600003.SH",
                "600004.SH",
                "600005.SH",
                "600001.SH",
            ],
        }
    )


def _em_flow() -> pd.DataFrame:
    # 东财路由：board_code 为 BK 码，f"{code}.DC" 精确 join
    return pd.DataFrame(
        {
            "board_code": ["BK0001", "BK0002"],
            "board_name": ["半导体", "白酒"],
            "pct_chg": [3.0, 1.0],
            "main_net_amount": [5e8, 2e8],
            "main_net_rate": [4.0, 1.5],
            "leading_stock": ["甲", "丁"],
        }
    )


def _kpl_members() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "board_code": ["KP1", "KP1", "KP1", "KP2"],
            "board_name": ["人形机器人", "人形机器人", "人形机器人", "冷题材"],
            "con_code": ["600001.SH", "600002.SH", "600004.SH", "600005.SH"],
        }
    )


class TestBuildBoardOverviewU1:
    def test_columns_amount_desc_and_counts(self) -> None:
        ov = build_board_overview(
            _snapshot(), _dc_members(), pd.DataFrame(), _em_flow(), "东财行业"
        )
        assert list(ov.columns) == _OVERVIEW_COLUMNS
        # AI（概念板块）被 idx_type 过滤，只剩两个行业板块，amount 降序
        assert ov["board_name"].tolist() == ["半导体", "白酒"]
        bk1 = ov[ov["board_code"] == "BK0001.DC"].iloc[0]
        assert bk1["amount"] == pytest.approx(6e8)
        assert bk1["stock_count"] == 3
        assert bk1["limit_up_count"] == 2  # 600001 / 600003
        assert bk1["broken_count"] == 1  # 600002
        assert bk1["limit_up_ratio_pct"] == pytest.approx(66.7)
        # 净流入来自资金流精确 join（BK0001 + '.DC'）
        assert bk1["main_net_amount"] == pytest.approx(5e8)
        assert bk1["main_net_rate"] == pytest.approx(4.0)
        assert bk1["leading_stock"] == "甲"

    def test_limit_counts_match_aggregate_board_limit_ups(self) -> None:
        """涨停/炸板计数与 aggregate_board_limit_ups 同口径逐板一致。"""
        snap, members = _snapshot(), _dc_members()
        ov = build_board_overview(snap, members, pd.DataFrame(), _em_flow(), "东财行业")
        ref = aggregate_board_limit_ups(snap, members, idx_type="行业板块")
        ref_map = ref.set_index("board_code")
        for _, row in ov.iterrows():
            code = row["board_code"]
            if code in ref_map.index:
                assert row["limit_up_count"] == ref_map.loc[code, "limit_up_count"]
                assert row["broken_count"] == ref_map.loc[code, "broken_count"]
            else:
                # aggregate 过滤 0 涨停板块，总表保留 → 计数必为 0
                assert row["limit_up_count"] == 0

    def test_zero_limit_up_board_retained(self) -> None:
        ov = build_board_overview(
            _snapshot(), _dc_members(), pd.DataFrame(), _em_flow(), "东财行业"
        )
        assert "白酒" in set(ov["board_name"])  # 0 涨停仍保留（成交额有意义）


class TestFundFlowJoinU2:
    def test_em_route_precise_join(self) -> None:
        ov = build_board_overview(
            _snapshot(), _dc_members(), pd.DataFrame(), _em_flow(), "东财行业"
        )
        bk1 = ov[ov["board_code"] == "BK0001.DC"].iloc[0]
        assert bk1["main_net_amount"] == pytest.approx(5e8)

    def test_ths_route_name_join(self) -> None:
        # board_code 全缺（同花顺路由）→ 降级按 board_name join
        flow = pd.DataFrame(
            {
                "board_code": [None, None],
                "board_name": ["半导体", "白酒"],
                "pct_chg": [3.0, 1.0],
                "main_net_amount": [9e8, 2e8],
                "main_net_rate": [float("nan"), float("nan")],
                "leading_stock": ["甲", "丁"],
            }
        )
        ov = build_board_overview(_snapshot(), _dc_members(), pd.DataFrame(), flow, "东财行业")
        bk1 = ov[ov["board_name"] == "半导体"].iloc[0]
        assert bk1["main_net_amount"] == pytest.approx(9e8)

    def test_unmatched_board_flow_nan_row_kept(self) -> None:
        # 资金流只覆盖 BK0001，白酒无对应行 → flow 列 NaN 但行保留
        flow = _em_flow().iloc[[0]]
        ov = build_board_overview(_snapshot(), _dc_members(), pd.DataFrame(), flow, "东财行业")
        bai = ov[ov["board_name"] == "白酒"].iloc[0]
        assert pd.isna(bai["main_net_amount"])
        assert bai["leading_stock"] is None or pd.isna(bai["leading_stock"])
        assert bai["amount"] == pytest.approx(4e8)  # 成交额仍在


class TestKplSystemU3:
    def test_kpl_aggregation_flow_all_nan(self) -> None:
        ov = build_board_overview(
            _snapshot(), _dc_members(), _kpl_members(), pd.DataFrame(), "开盘啦题材"
        )
        assert list(ov.columns) == _OVERVIEW_COLUMNS
        # 冷题材 0 涨停但成交额仍在 → 保留；资金流三列全 NaN
        assert ov["main_net_amount"].isna().all()
        assert ov["main_net_rate"].isna().all()
        assert ov["leading_stock"].isna().all()
        kp1 = ov[ov["board_code"] == "KP1"].iloc[0]
        # KP1 成分 600001(涨停) / 600002(炸板) / 600004(普通)
        assert kp1["limit_up_count"] == 1
        assert kp1["broken_count"] == 1
        assert kp1["stock_count"] == 3

    def test_kpl_ignores_flow_even_if_passed(self) -> None:
        # 即使误传东财资金流，kpl 体系也不 join（资金流列保持 NaN）
        ov = build_board_overview(
            _snapshot(), _dc_members(), _kpl_members(), _em_flow(), "开盘啦题材"
        )
        assert ov["main_net_amount"].isna().all()


class TestOverviewEmptyU4:
    def test_empty_snapshot_returns_empty(self) -> None:
        assert build_board_overview(
            pd.DataFrame(), _dc_members(), pd.DataFrame(), _em_flow(), "东财行业"
        ).empty

    def test_empty_members_returns_empty(self) -> None:
        assert build_board_overview(
            _snapshot(), pd.DataFrame(), pd.DataFrame(), _em_flow(), "东财行业"
        ).empty

    def test_empty_flow_keeps_amount_and_limit_columns(self) -> None:
        ov = build_board_overview(
            _snapshot(), _dc_members(), pd.DataFrame(), pd.DataFrame(), "东财行业"
        )
        assert not ov.empty
        assert ov["amount"].notna().all()
        assert ov["main_net_amount"].isna().all()  # 无资金流 → NaN
        bk1 = ov[ov["board_code"] == "BK0001.DC"].iloc[0]
        assert bk1["limit_up_count"] == 2

    def test_unknown_system_raises(self) -> None:
        with pytest.raises(ValueError, match="未知板块体系"):
            build_board_overview(_snapshot(), _dc_members(), pd.DataFrame(), _em_flow(), "外星")


# ── U5 trends2 分时/5日 ────────────────────────────────────────────────────────


class _FakeResp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


def _trends_payload(count: int, start: str = "2026-07-04 09:30") -> dict:
    base = pd.Timestamp(start)
    trends = [
        f"{(base + pd.Timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M')},"
        f"{10.0 + i * 0.01:.2f},{1000 + i},{10.0 + i * 0.005:.2f}"
        for i in range(count)
    ]
    return {"data": {"trends": trends}}


class TestFetchIntradayTrendU5:
    def test_em_direct_parse_types(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[dict] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            seen.append(kwargs["params"])
            return _FakeResp(_trends_payload(240))

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_intraday_trend("600519.SH", ndays=1)
        assert df.attrs["route"] == "em_direct"
        assert list(df.columns) == ["dt", "price", "avg_price", "volume"]
        assert pd.api.types.is_datetime64_any_dtype(df["dt"])
        assert pd.api.types.is_numeric_dtype(df["price"])
        assert pd.api.types.is_numeric_dtype(df["avg_price"])
        assert pd.api.types.is_numeric_dtype(df["volume"])
        assert len(df) == 240
        # 沪市 secid = 1.600519
        assert seen[0]["secid"] == "1.600519"
        assert seen[0]["ndays"] == 1

    def test_ndays5_secid_and_param(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[dict] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            seen.append(kwargs["params"])
            return _FakeResp(_trends_payload(1200))

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        df = fetch_intraday_trend("300750.SZ", ndays=5)
        assert len(df) == 1200
        assert seen[0]["ndays"] == 5
        assert seen[0]["secid"] == "0.300750"  # 深市 0.

    def test_direct_fails_socks_takes_over(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen_proxies: list[dict | None] = []

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            seen_proxies.append(kwargs["proxies"])
            if kwargs["proxies"] is None:
                raise ConnectionError("RST")
            return _FakeResp(_trends_payload(240))

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setenv("RQUANT_PANORAMA_SOCKS", "socks5h://127.0.0.1:9999")
        df = fetch_intraday_trend("600519.SH")
        assert df.attrs["route"] == "em_socks"
        assert seen_proxies[0] is None
        assert seen_proxies[1] == {
            "http": "socks5h://127.0.0.1:9999",
            "https": "socks5h://127.0.0.1:9999",
        }

    def test_em_all_fail_sina_fallback_avg_nan(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        sina_raw = pd.DataFrame(
            {
                "day": ["2026-07-04 09:31:00", "2026-07-04 09:32:00"],
                "open": [10.0, 10.1],
                "high": [10.2, 10.3],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
                "volume": [1000, 2000],
            }
        )
        monkeypatch.setattr(panorama_data, "_fetch_sina_minute_raw", lambda ts: sina_raw)
        df = fetch_intraday_trend("600519.SH")
        assert df.attrs["route"] == "sina"
        assert list(df.columns) == ["dt", "price", "avg_price", "volume"]
        assert df["avg_price"].isna().all()  # 新浪无均价线
        assert df["price"].tolist() == [10.1, 10.2]

    def test_sina_fallback_trims_to_ndays(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """新浪固定返回约 5 天分钟——分时(ndays=1)必须裁到最近一天，否则渲染成 5 日图。"""

        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        # 三个交易日各两根分钟
        rows = []
        for day in ("2026-07-02", "2026-07-03", "2026-07-06"):
            rows += [
                {"day": f"{day} 09:31:00", "close": 10.0, "volume": 1000},
                {"day": f"{day} 09:32:00", "close": 10.1, "volume": 1100},
            ]
        sina_raw = pd.DataFrame(rows)
        monkeypatch.setattr(panorama_data, "_fetch_sina_minute_raw", lambda ts: sina_raw)

        one = fetch_intraday_trend("600519.SH", ndays=1)
        assert one.attrs["route"] == "sina"
        assert pd.to_datetime(one["dt"]).dt.normalize().nunique() == 1  # 只剩最近一天
        assert pd.to_datetime(one["dt"]).dt.date.max().isoformat() == "2026-07-06"

        five = fetch_intraday_trend("600519.SH", ndays=5)
        assert pd.to_datetime(five["dt"]).dt.normalize().nunique() == 3  # 不足 5 天，全保留

    def test_all_routes_fail_empty_route_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get(self: object, url: str, **kwargs: object) -> _FakeResp:
            raise ConnectionError("em unreachable")

        def sina_boom(ts_code: str) -> pd.DataFrame:
            raise ConnectionError("sina down")

        import requests

        monkeypatch.setattr(requests.Session, "get", fake_get)
        monkeypatch.setattr(panorama_data, "_fetch_sina_minute_raw", sina_boom)
        df = fetch_intraday_trend("600519.SH")
        assert df.empty
        assert df.attrs["route"] == "none"


# ── U6 日K ─────────────────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestLoadDailyKlineU6:
    def _seed(self, store: DuckDBStore, n: int = 150) -> pd.DataFrame:
        dates = pd.bdate_range("2025-01-06", periods=n)
        close = pd.Series([10.0 + i * 0.1 for i in range(n)])
        df = pd.DataFrame(
            {
                "ts_code": "600001.SH",
                "trade_date": dates,
                "open": close - 0.1,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "pre_close": close - 0.05,
                "change": 0.0,
                "pct_chg": 0.0,
                "vol": [1e5 + i for i in range(n)],
                "amount": [1e7 + i for i in range(n)],
            }
        )
        store.upsert_daily(df)
        return df

    def test_returns_last_120_with_ma(self, store: DuckDBStore) -> None:
        self._seed(store, 150)
        kl = load_daily_kline("600001.SH", store=store)
        assert len(kl) == 120
        assert list(kl.columns) == [
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ma5",
            "ma10",
            "ma20",
        ]
        # MA 就地滚动：首 4 根 ma5 为 NaN，第 5 根起有值
        assert kl["ma5"].iloc[:4].isna().all()
        assert kl["ma5"].iloc[4] == pytest.approx(kl["close"].iloc[:5].mean())
        assert kl["ma20"].iloc[:19].isna().all()
        assert kl["ma20"].iloc[19] == pytest.approx(kl["close"].iloc[:20].mean())
        # 只取最近 120 根：起点为原始第 30 根
        assert kl["close"].iloc[0] == pytest.approx(10.0 + 30 * 0.1)

    def test_unknown_code_empty(self, store: DuckDBStore) -> None:
        self._seed(store, 150)
        assert load_daily_kline("999999.SZ", store=store).empty

    def test_rejects_unbounded_history_window(self, store: DuckDBStore) -> None:
        self._seed(store, 150)

        result = load_daily_kline("600001.SH", n=241, store=store)

        assert result.empty
        assert result.attrs["serving_state"] == "unavailable"
        assert "between 1 and 240" in result.attrs["serving_detail"]

    def test_store_open_failure_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def boom() -> DuckDBStore:
            raise RuntimeError("serving unavailable")

        monkeypatch.setattr(panorama_data, "_open_serving_store", boom)
        assert load_daily_kline("600001.SH").empty


# ── U7 fake 模式 ───────────────────────────────────────────────────────────────


class TestFakeModeU7:
    def test_env_gate_isolation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert panorama_data._fake_enabled() is False
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        assert panorama_data._fake_enabled() is False
        with panorama_data._panorama_test_fixtures():
            assert panorama_data._fake_enabled() is True
        assert panorama_data._fake_enabled() is False

    def test_all_eight_fetchers_shape(self, _test_fixture_backend: None) -> None:
        snap = panorama_data.fetch_market_snapshot()
        assert set(
            [
                "ts_code",
                "name",
                "price",
                "open",
                "high",
                "low",
                "pre_close",
                "pct_chg",
                "volume",
                "amount",
            ]
        ).issubset(snap.columns)
        assert len(snap) >= 30

        flow = panorama_data.fetch_sector_fund_flow("行业资金流")
        assert not flow.empty
        assert set(
            [
                "board_code",
                "board_name",
                "pct_chg",
                "main_net_amount",
                "main_net_rate",
                "leading_stock",
            ]
        ).issubset(flow.columns)
        assert flow.attrs["route"] == "em_direct"

        members = panorama_data.load_board_members()
        assert set(members["idx_type"]) == {"行业板块", "概念板块"}

        kpl = panorama_data.load_kpl_concept_members()
        assert list(kpl.columns) == ["board_code", "board_name", "con_code"]
        assert "人形机器人" in set(kpl["board_name"])
        assert (kpl["board_name"] == "人形机器人").sum() >= 5

        flags = panorama_data.load_pool_flags()
        assert isinstance(flags, dict) and flags

        liq = panorama_data.load_liquidity_baseline()
        assert set(["ts_code", "circ_mv", "avg_amount_5d"]).issubset(liq.columns)
        assert not liq.empty

        trend = panorama_data.fetch_intraday_trend("600001.SH", ndays=1)
        assert list(trend.columns) == ["dt", "price", "avg_price", "volume"]
        assert len(trend) == 240
        trend5 = panorama_data.fetch_intraday_trend("600001.SH", ndays=5)
        assert len(trend5) == 1200

        kline = panorama_data.load_daily_kline("600001.SH")
        assert set(
            ["trade_date", "open", "high", "low", "close", "volume", "ma5", "ma10", "ma20"]
        ).issubset(kline.columns)
        assert len(kline) == 120

    def test_fake_kline_mixed_candles(self, _test_fixture_backend: None) -> None:
        """fake 日K 必须阳/阴混合，红/绿两条蜡烛渲染路径都有视觉覆盖。"""
        kl = panorama_data.load_daily_kline("600001.SH")
        assert (kl["close"] > kl["open"]).any()
        assert (kl["close"] < kl["open"]).any()

    def test_fake_intraday_real_session_stamps_with_lunch_gap(
        self, _test_fixture_backend: None
    ) -> None:
        """fake 分时 dt 覆盖 09:30–11:29 与 13:00–15:00 两段、不含 11:30–12:59 午休。

        午休断裂是分时图空档修复（idx 序号轴）的可视验证前提，fake 必须复现。
        """
        trend = panorama_data.fetch_intraday_trend("600001.SH", ndays=1)
        assert len(trend) == 240
        assert trend["dt"].dt.date.nunique() == 1
        hm = trend["dt"].dt.strftime("%H:%M")
        assert ((hm >= "09:30") & (hm <= "11:29")).sum() == 120
        assert ((hm >= "13:00") & (hm <= "15:00")).sum() == 120
        assert ((hm >= "11:30") & (hm <= "12:59")).sum() == 0
        assert trend["dt"].iloc[0].strftime("%H:%M") == "09:30"
        assert trend["dt"].iloc[-1].strftime("%H:%M") == "14:59"

    def test_fake_5day_has_five_distinct_trading_days(self, _test_fixture_backend: None) -> None:
        """5 日 fake 含 5 个交易日各 240 根真实时段时间戳（隔夜断裂，可视验证空档）。"""
        trend5 = panorama_data.fetch_intraday_trend("600001.SH", ndays=5)
        assert len(trend5) == 1200
        per_day = trend5.groupby(trend5["dt"].dt.date).size()
        assert len(per_day) == 5
        assert (per_day == 240).all()
        hm = trend5["dt"].dt.strftime("%H:%M")
        assert ((hm >= "11:30") & (hm <= "12:59")).sum() == 0

    def test_fake_snapshot_has_two_limit_ups(self, _test_fixture_backend: None) -> None:
        snap = add_limit_prices(panorama_data.fetch_market_snapshot())
        pulse = panorama_data.compute_market_pulse(snap)
        assert pulse.limit_up_count >= 2
        assert pulse.broken_count >= 1

    def test_fake_overview_board_code_join(self, _test_fixture_backend: None) -> None:
        # fake 资金流 board_code 可精确 join 成分板块（覆盖精确 join 路径）
        snap = add_limit_prices(panorama_data.fetch_market_snapshot())
        members = panorama_data.load_board_members()
        flow = panorama_data.fetch_sector_fund_flow("行业资金流")
        ov = build_board_overview(snap, members, pd.DataFrame(), flow, "东财行业")
        bk1 = ov[ov["board_code"] == "BK0001.DC"].iloc[0]
        assert bk1["main_net_amount"] == pytest.approx(1.23e9)
