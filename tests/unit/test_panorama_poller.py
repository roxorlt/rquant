"""SourcePoller 单测：slot 语义、每源独立熔断、sina 级特殊熔断、refresh_now、status。

全部注入 FakeClock + mock fetcher，不碰网络；refresh_now / start 幂等用真实
daemon 线程 + threading.Event 同步（wait 带超时，无长 sleep）。
"""

from __future__ import annotations

import threading

import pandas as pd
import pytest

from rquant.panorama_poller import FLOW_TYPES, SNAPSHOT_KEY, SourcePoller


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _snap_df(route: str = "em_direct", n: int = 3) -> pd.DataFrame:
    df = pd.DataFrame(
        {"ts_code": [f"60000{i}.SH" for i in range(1, n + 1)], "price": [10.0] * n}
    )
    df.attrs["route"] = route
    return df


def _flow_df(route: str = "em_direct") -> pd.DataFrame:
    df = pd.DataFrame({"board_name": ["半导体"], "main_net_amount": [1e8]})
    df.attrs["route"] = route
    return df


class TestSlotSemantics:
    def test_never_succeeded_returns_empty_defaults(self) -> None:
        poller = SourcePoller(
            now=FakeClock(),
            snapshot_fetcher=lambda allow: _snap_df(),
            flow_fetcher=lambda s: _flow_df(),
        )
        df, as_of, route = poller.snapshot()
        assert df.empty and as_of == "" and route == "none"
        fdf, froute = poller.flow("行业资金流")
        assert fdf.empty and froute == "none"

    def test_success_updates_slots(self) -> None:
        poller = SourcePoller(
            now=FakeClock(),
            snapshot_fetcher=lambda allow: _snap_df(),
            flow_fetcher=lambda s: _flow_df("em_socks"),
        )
        poller.poll_once()
        df, as_of, route = poller.snapshot()
        assert len(df) == 3 and route == "em_direct"
        assert as_of != ""  # HH:MM:SS 墙钟
        for sector in FLOW_TYPES:
            fdf, froute = poller.flow(sector)
            assert not fdf.empty and froute == "em_socks"

    def test_failure_keeps_last_known_good(self) -> None:
        clock = FakeClock()
        state = {"fail": False}

        def snap_fetch(allow: bool) -> pd.DataFrame:
            if state["fail"]:
                raise ConnectionError("RST by risk-control")
            return _snap_df()

        poller = SourcePoller(
            now=clock, snapshot_fetcher=snap_fetch, flow_fetcher=lambda s: _flow_df()
        )
        poller.poll_once()
        df1, as_of1, _ = poller.snapshot()
        state["fail"] = True
        clock.advance(60)
        poller.poll_once()
        df2, as_of2, route2 = poller.snapshot()
        assert as_of2 == as_of1  # 失败保留上一次好数据
        assert df2.equals(df1)
        assert route2 == "em_direct"
        snap_status = poller.status()[SNAPSHOT_KEY]
        assert "ConnectionError" in snap_status["last_error"]
        assert snap_status["consec_fail"] == 1

    def test_empty_result_counts_as_failure(self) -> None:
        poller = SourcePoller(
            now=FakeClock(),
            snapshot_fetcher=lambda allow: pd.DataFrame(),
            flow_fetcher=lambda s: _flow_df(),
        )
        poller.poll_once()
        df, as_of, _ = poller.snapshot()
        assert df.empty and as_of == ""
        assert poller.status()[SNAPSHOT_KEY]["last_error"] != ""


class TestCircuitBreaker:
    def test_three_fails_cooldown_180_and_recovery(self) -> None:
        clock = FakeClock()
        calls = {"行业资金流": 0, "概念资金流": 0}

        def flow_fetch(sector: str) -> pd.DataFrame:
            calls[sector] += 1
            if sector == "行业资金流":
                raise ConnectionError("dead")
            return _flow_df()

        poller = SourcePoller(
            now=clock, snapshot_fetcher=lambda a: _snap_df(), flow_fetcher=flow_fetch
        )
        for _ in range(3):
            poller.poll_once()
            clock.advance(30)
        assert calls["行业资金流"] == 3
        status = poller.status()["行业资金流"]
        assert status["consec_fail"] == 3
        assert status["cooling_until"] > clock()
        # 冷却期内该源不被调用；概念流独立不受影响
        poller.poll_once()
        assert calls["行业资金流"] == 3
        assert calls["概念资金流"] == 4
        # 冷却过后恢复尝试
        clock.advance(181)
        poller.poll_once()
        assert calls["行业资金流"] == 4

    def test_sina_tier_two_fails_cooldown_300(self) -> None:
        clock = FakeClock()  # t=1000
        allow_seq: list[bool] = []

        def snap_fetch(allow: bool) -> pd.DataFrame:
            allow_seq.append(allow)
            return pd.DataFrame()  # 三级路由全败（sina 若被允许则也被尝试并失败）

        poller = SourcePoller(
            now=clock, snapshot_fetcher=snap_fetch, flow_fetcher=lambda s: _flow_df()
        )
        poller.poll_once()  # t=1000 sina 连败 1
        clock.advance(61)
        poller.poll_once()  # t=1061 sina 连败 2 → 冷却至 1361
        clock.advance(61)
        poller.poll_once()  # t=1122 sina 冷却中 → allow=False；快照连败 3 → 冷却至 1302
        assert allow_seq == [True, True, False]
        clock.advance(61)  # t=1183 快照源整体冷却中 → fetcher 不被调用
        poller.poll_once()
        assert len(allow_seq) == 3
        clock.t = 1310  # 过快照冷却（1302），sina 仍冷却（<1361）
        poller.poll_once()
        assert allow_seq == [True, True, False, False]
        clock.t = 1500  # 双双过冷却 → sina 恢复允许
        poller.poll_once()
        assert allow_seq[-1] is True
        assert poller.status()["sina"]["consec_fail"] >= 2

    def test_sina_reset_on_sina_success(self) -> None:
        clock = FakeClock()
        state = {"mode": "fail"}

        def snap_fetch(allow: bool) -> pd.DataFrame:
            if state["mode"] == "fail":
                return pd.DataFrame()
            return _snap_df(route="sina")

        poller = SourcePoller(
            now=clock, snapshot_fetcher=snap_fetch, flow_fetcher=lambda s: _flow_df()
        )
        poller.poll_once()
        state["mode"] = "ok"
        clock.advance(61)
        poller.poll_once()
        assert poller.status()["sina"]["consec_fail"] == 0
        assert poller.snapshot()[2] == "sina"


class TestThreadLifecycle:
    def test_refresh_now_triggers_immediate_round(self) -> None:
        fetched = threading.Event()
        counts = {"n": 0}

        def snap_fetch(allow: bool) -> pd.DataFrame:
            counts["n"] += 1
            fetched.set()
            return _snap_df()

        poller = SourcePoller(
            interval=300.0, snapshot_fetcher=snap_fetch, flow_fetcher=lambda s: _flow_df()
        )
        poller.start()
        try:
            assert fetched.wait(5.0)  # 启动即第一轮
            first = counts["n"]
            fetched.clear()
            poller.refresh_now()  # 不等 300s interval，立即再开一轮
            assert fetched.wait(5.0)
            assert counts["n"] > first
        finally:
            poller.stop()

    def test_start_idempotent(self) -> None:
        poller = SourcePoller(
            interval=300.0,
            snapshot_fetcher=lambda a: _snap_df(),
            flow_fetcher=lambda s: _flow_df(),
        )
        poller.start()
        try:
            t1 = poller._thread
            poller.start()
            assert poller._thread is t1
        finally:
            poller.stop()


class TestStatus:
    def test_status_fields_complete(self) -> None:
        clock = FakeClock()
        poller = SourcePoller(
            now=clock,
            snapshot_fetcher=lambda a: _snap_df(),
            flow_fetcher=lambda s: _flow_df(),
        )
        poller.poll_once()
        clock.advance(42)
        status = poller.status()
        assert status["fetching"] is False
        for key in (SNAPSHOT_KEY, *FLOW_TYPES, "sina"):
            entry = status[key]
            assert set(entry) == {
                "last_success_ts", "age_seconds", "route",
                "consec_fail", "cooling_until", "last_error",
            }
        snap = status[SNAPSHOT_KEY]
        assert snap["age_seconds"] == pytest.approx(42.0)
        assert snap["route"] == "em_direct"
        assert snap["last_success_ts"] != ""
