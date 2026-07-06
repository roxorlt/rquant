"""surge-watch 单测（U1-U10，全离线，注入时钟/mock 源，不真 sleep 不碰网络）。

口径 v1: rough1.5×20d·curve / confirm2.0×3d同刻 / VWAP门。
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import numpy as np
import pandas as pd

from rquant.surge_watch import (
    CURVE_POINTS,
    SurgeBaseline,
    SurgeConfig,
    SurgeConfirmed,
    SurgeWatcher,
    _is_lunch,
    _minute_delta,
    _rough_candidates,
    append_events,
    atomic_write_parquet,
    build_surge_messages,
    build_three_day_baseline,
    grid_index,
    linear_progress_curve,
    load_progress_curve,
    load_theme_map,
    run_simulate,
    run_surge_watch,
    series_to_frame,
)

CST = timezone(timedelta(hours=8))
_SNAP_COLS = ["ts_code", "name", "price", "pre_close", "pct_chg", "volume", "amount",
              "limit_up_price"]


def mk_snap(rows: list[dict]) -> pd.DataFrame:
    """构造检测域快照（默认补全 limit_up_price = pre_close×1.2 创业/科创档）。"""
    filled = []
    for r in rows:
        row = {c: r.get(c) for c in _SNAP_COLS}
        if row.get("name") is None:
            row["name"] = row["ts_code"]
        if row.get("limit_up_price") is None and row.get("pre_close") is not None:
            row["limit_up_price"] = round(row["pre_close"] * 1.2, 2)
        filled.append(row)
    return pd.DataFrame(filled, columns=_SNAP_COLS)


def flat_curve() -> np.ndarray:
    """恒定曲线（除末点归一无关），rough 阈值可精确算。"""
    return np.linspace(1.0 / CURVE_POINTS, 1.0, CURVE_POINTS)


def mk_baseline(
    avg20: dict[str, float], *, curve: np.ndarray | None = None, theme: dict[str, str] | None = None
) -> SurgeBaseline:
    return SurgeBaseline(
        avg_amount_20d=avg20,
        theme=theme or {},
        curve=curve if curve is not None else flat_curve(),
    )


def mk_minute_bars(
    day_amounts: dict[date, list[float]], start: dt_time = dt_time(9, 30)
) -> pd.DataFrame:
    """构造 stk_mins 风格 bars：{日期: [逐分钟成交额]}，从 start 起每分钟一根。"""
    rows: list[dict] = []
    for d, amts in day_amounts.items():
        for i, a in enumerate(amts):
            t = (datetime.combine(d, start) + timedelta(minutes=i))
            rows.append({"ts_code": "X", "trade_time": t, "amount": float(a)})
    return pd.DataFrame(rows, columns=["ts_code", "trade_time", "amount"])


# ── U1 曲线标定/加载 ────────────────────────────────────────────────────────────


class TestU1Curve:
    def test_load_valid_curve(self, tmp_path: Path) -> None:
        pts = list(np.linspace(0.004, 1.0, CURVE_POINTS))
        p = tmp_path / "c.json"
        p.write_text(json.dumps({"points": pts}), encoding="utf-8")
        c = load_progress_curve(p)
        assert len(c) == CURVE_POINTS
        assert (np.diff(c) >= -1e-9).all()      # 单调不减
        assert c[0] < 0.2 and c[0] >= 0          # 首≈0
        assert abs(c[-1] - 1.0) < 1e-9           # 尾=1

    def test_missing_file_falls_back_linear_with_warning(self, tmp_path: Path, caplog) -> None:
        c = load_progress_curve(tmp_path / "nope.json")
        assert np.allclose(c, linear_progress_curve())
        assert abs(c[-1] - 1.0) < 1e-9

    def test_packaged_curve_loads_and_is_monotone(self) -> None:
        c = load_progress_curve()  # 包内标定产物
        assert len(c) == CURVE_POINTS
        assert (np.diff(c) >= -1e-9).all()
        assert abs(c[-1] - 1.0) < 1e-9

    def test_wrong_point_count_falls_back(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.json"
        p.write_text(json.dumps({"points": [0.1, 0.5, 1.0]}), encoding="utf-8")
        assert np.allclose(load_progress_curve(p), linear_progress_curve())

    def test_grid_index_boundaries(self) -> None:
        assert grid_index(dt_time(9, 30)) == 0
        assert grid_index(dt_time(9, 0)) == 0        # 盘前钳制
        assert grid_index(dt_time(11, 30)) == 120
        assert grid_index(dt_time(12, 0)) == 120     # 午休并入上午末点
        assert grid_index(dt_time(13, 1)) == 121
        assert grid_index(dt_time(15, 0)) == 240
        assert grid_index(dt_time(15, 30)) == 240    # 收盘后钳制


# ── U2 粗筛 ─────────────────────────────────────────────────────────────────────


class TestU2Rough:
    def _base(self) -> tuple[SurgeBaseline, SurgeConfig, int]:
        curve = flat_curve()
        gi = grid_index(dt_time(10, 5))
        base = mk_baseline({"300001.SZ": 1e8}, curve=curve)
        return base, SurgeConfig(), gi

    def test_threshold_exact_boundary_passes(self) -> None:
        base, cfg, gi = self._base()
        thr = cfg.k_rough * 1e8 * float(base.curve[gi])
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 11, "pre_close": 10,
                         "pct_chg": 5, "volume": 1e6, "amount": thr}])
        assert _rough_candidates(snap, base, cfg, gi) == ["300001.SZ"]
        snap2 = mk_snap([{"ts_code": "300001.SZ", "price": 11, "pre_close": 10,
                          "pct_chg": 5, "volume": 1e6, "amount": thr - 1}])
        assert _rough_candidates(snap2, base, cfg, gi) == []

    def test_st_excluded(self) -> None:
        base, cfg, gi = self._base()
        thr = cfg.k_rough * 1e8 * float(base.curve[gi])
        snap = mk_snap([{"ts_code": "300001.SZ", "name": "ST科", "price": 11, "pre_close": 10,
                         "pct_chg": 5, "volume": 1e6, "amount": thr * 3}])
        assert _rough_candidates(snap, base, cfg, gi) == []

    def test_pct_chg_non_positive_excluded(self) -> None:
        base, cfg, gi = self._base()
        thr = cfg.k_rough * 1e8 * float(base.curve[gi])
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 9, "pre_close": 10,
                         "pct_chg": -1, "volume": 1e6, "amount": thr * 3}])
        assert _rough_candidates(snap, base, cfg, gi) == []

    def test_missing_baseline_skipped(self) -> None:
        base, cfg, gi = self._base()  # avg20 只有 300001
        snap = mk_snap([{"ts_code": "301999.SZ", "price": 11, "pre_close": 10,
                         "pct_chg": 5, "volume": 1e6, "amount": 9e9}])
        assert _rough_candidates(snap, base, cfg, gi) == []

    def test_yuan_vs_thousand_yuan_trap(self) -> None:
        """千元→元陷阱：avg20 若误按千元（少 1000 倍）阈值会 1000× 偏小，人人过关。

        用元口径基线时，1×avg20 的当日额在盘中（curve<1）不必然过 1.5× 阈值；
        故意灌 1000 倍错误基线验证会误放行 → 反证元口径的必要。
        """
        cfg = SurgeConfig()
        gi = grid_index(dt_time(11, 0))
        amount = 5e7  # 元
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 11, "pre_close": 10,
                         "pct_chg": 5, "volume": 1e6, "amount": amount}])
        base_yuan = mk_baseline({"300001.SZ": 1e8})       # 正确：1 亿元 → 高阈值
        base_thousand = mk_baseline({"300001.SZ": 1e5})   # 错误：当千元用 → 阈值 1000× 偏小
        assert _rough_candidates(snap, base_yuan, cfg, gi) == []
        assert _rough_candidates(snap, base_thousand, cfg, gi) == ["300001.SZ"]


# ── U3 分钟序列 ─────────────────────────────────────────────────────────────────


class TestU3MinuteSeries:
    def test_cum_series_and_delta(self) -> None:
        base = mk_baseline({})
        w = SurgeWatcher(base, minute_fetcher=lambda c, d: pd.DataFrame())
        w._update_cum_series(mk_snap([{"ts_code": "300001.SZ", "amount": 100}]), 0)
        w._update_cum_series(mk_snap([{"ts_code": "300001.SZ", "amount": 260}]), 1)
        arr = w.cum_series["300001.SZ"]
        assert arr[0] == 100 and arr[1] == 260
        assert _minute_delta(arr, 1) == 160          # 本分钟增量
        assert _minute_delta(arr, 0) == 100          # 首格无前值 → 本身

    def test_snapshot_miss_leaves_nan_then_recovers(self) -> None:
        base = mk_baseline({})
        w = SurgeWatcher(base, minute_fetcher=lambda c, d: pd.DataFrame())
        w._update_cum_series(mk_snap([{"ts_code": "300001.SZ", "amount": 100}]), 0)
        # gi=1 该票缺席（快照 miss）→ NaN，不崩
        w._update_cum_series(mk_snap([{"ts_code": "300099.SZ", "amount": 5}]), 1)
        w._update_cum_series(mk_snap([{"ts_code": "300001.SZ", "amount": 300}]), 2)
        arr = w.cum_series["300001.SZ"]
        assert np.isnan(arr[1])
        assert _minute_delta(arr, 2) == 200          # 跳过 NaN 取到 idx0=100


# ── U4 确认层 ───────────────────────────────────────────────────────────────────


class TestU4Confirm:
    def _today(self) -> date:
        return date(2026, 7, 6)

    def _three_day_bars(self) -> pd.DataFrame:
        d = self._today()
        return mk_minute_bars({
            d - timedelta(days=1): [300, 300, 300],
            d - timedelta(days=2): [200, 200, 200],
            d - timedelta(days=3): [100, 100, 100],
        })

    def test_three_day_median_construction(self) -> None:
        base = build_three_day_baseline(self._three_day_bars(), self._today(), 3)
        assert base.days_used == 3
        # cum 各日 [300..][200..][100..]，逐格中位：idx0=200, idx1=400, idx2=600
        assert base.cum_median[0] == 200
        assert base.cum_median[1] == 400
        assert base.cum_median[2] == 600
        assert base.minute_median[0] == 200          # 同分钟增量中位

    def test_confirm_boundary_and_vwap_gate(self) -> None:
        curve = flat_curve()
        base = mk_baseline({"300001.SZ": 1e6}, curve=curve)
        bars = self._three_day_bars()
        w = SurgeWatcher(base, config=SurgeConfig(), minute_fetcher=lambda c, d: bars)
        gi = 1  # 09:31，base_cum[1]=400
        now = datetime(2026, 7, 6, 9, 31, tzinfo=CST)
        # rel = amount/400；恰好 2.0× → amount 800；VWAP: price ≥ amount/volume
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 800, "amount": 800}])
        w.cum_series["300001.SZ"] = np.full(CURVE_POINTS, np.nan)
        w.cum_series["300001.SZ"][gi] = 800
        base_3d = build_three_day_baseline(self._three_day_bars(), now.date())
        w.confirm_cache["300001.SZ"] = base_3d
        assert base_3d.cum_median[gi] == 400
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today
        assert w._pending_push[0].rel_cum_3d == 2.0

    def test_vwap_reject(self) -> None:
        base = mk_baseline({"300001.SZ": 1e6})
        w = SurgeWatcher(base, minute_fetcher=lambda c, d: self._three_day_bars())
        gi = 1
        now = datetime(2026, 7, 6, 9, 31, tzinfo=CST)
        # price < vwap(=amount/volume=2.0) → 拒
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 500, "amount": 1000}])
        w.confirm_cache["300001.SZ"] = build_three_day_baseline(self._three_day_bars(), now.date())
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" not in w.pushed_today

    def test_daily_cache_hit_no_refetch(self) -> None:
        """当日缓存命中不重拉（tushare spy 调用数）。"""
        calls: list[str] = []
        # 大基线：过 rough 但 rel<K_confirm（cum 250k / 3日中位 300k = 0.83×）→ 不确认但已缓存
        big_bars = mk_minute_bars({
            self._today() - timedelta(days=i): [100000, 100000, 100000] for i in (1, 2, 3)
        })

        def spy(code: str, d: date) -> pd.DataFrame:
            calls.append(code)
            return big_bars

        base = mk_baseline({"300001.SZ": 1e6}, curve=flat_curve())
        w = SurgeWatcher(base, config=SurgeConfig(), minute_fetcher=spy)
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 100.0, "pre_close": 90,
                         "pct_chg": 5, "volume": 1e6, "amount": 250000}])
        w.tick(snap, datetime(2026, 7, 6, 10, 0, tzinfo=CST))
        assert calls == ["300001.SZ"]
        assert "300001.SZ" not in w.pushed_today      # rel<2.0 未确认
        # 下一分钟仍未确认，命中缓存不再拉
        w.tick(snap, datetime(2026, 7, 6, 10, 1, tzinfo=CST))
        assert calls == ["300001.SZ"]                # 无第二次取数
        assert "300001.SZ" in w.confirm_cache


# ── U5 去重/静默/折叠 ───────────────────────────────────────────────────────────


class TestU5DedupSilentFold:
    def _confirming_watcher(self) -> SurgeWatcher:
        d = date(2026, 7, 6)
        bars = mk_minute_bars({d - timedelta(days=i): [10, 10, 10] for i in (1, 2, 3)})
        base = mk_baseline({"300001.SZ": 1e6}, curve=flat_curve())
        return SurgeWatcher(base, config=SurgeConfig(), minute_fetcher=lambda c, dd: bars)

    def _snap(self) -> pd.DataFrame:
        # amount 1e7 过 rough；rel=1e7/base_cum(30)≫2；VWAP: price100 ≥ vwap(1e7/1e6=10)
        return mk_snap([{"ts_code": "300001.SZ", "price": 100.0, "pre_close": 90,
                         "pct_chg": 5, "volume": 1e6, "amount": 1e7}])

    def test_once_per_day(self) -> None:
        w = self._confirming_watcher()
        snap = self._snap()
        r1 = w.tick(snap, datetime(2026, 7, 6, 10, 0, tzinfo=CST))
        assert r1.confirmed and r1.confirmed[0].ts_code == "300001.SZ"
        r2 = w.tick(snap, datetime(2026, 7, 6, 10, 1, tzinfo=CST))
        assert r2.confirmed == []                    # 每票每日仅推一次

    def test_silent_window_collects_then_flushes(self) -> None:
        w = self._confirming_watcher()
        snap = self._snap()
        r_silent = w.tick(snap, datetime(2026, 7, 6, 9, 31, tzinfo=CST))  # 9:33 前
        assert r_silent.pushes == [] and r_silent.confirmed == []
        assert len(w._pending_push) == 1             # 收集不丢
        r_flush = w.tick(mk_snap([]), datetime(2026, 7, 6, 9, 33, tzinfo=CST))
        assert r_flush.pushes and r_flush.confirmed[0].ts_code == "300001.SZ"

    def test_fold_over_max(self) -> None:
        confirmed = [
            SurgeConfirmed(ts_code=f"3000{i:02d}.SZ", name=f"n{i}", pct_chg=5.0, rel_cum_3d=3.0)
            for i in range(9)
        ]
        cfg = SurgeConfig()
        msgs = build_surge_messages(confirmed, datetime(2026, 7, 6, 10, 5, tzinfo=CST), cfg)
        assert len(msgs) == 1
        title, body = msgs[0]
        assert "9 只" in title
        assert body.count("\n- ") == cfg.max_per_push + 1  # 8 只 + 折叠行
        assert "另有 1 只" in body


# ── U6 tushare 限频队列 ─────────────────────────────────────────────────────────


class TestU6RateLimit:
    def _bars(self) -> pd.DataFrame:
        d = date(2026, 7, 6)
        return mk_minute_bars({d - timedelta(days=i): [1, 1, 1] for i in (1, 2, 3)})

    def test_two_per_minute_fifo(self) -> None:
        calls: list[str] = []
        bars = self._bars()

        def spy(code: str, d: date) -> pd.DataFrame:
            calls.append(code)
            return bars

        codes = [f"3000{i:02d}.SZ" for i in range(10)]
        avg20 = {c: 1e6 for c in codes}
        w = SurgeWatcher(
            mk_baseline(avg20, curve=flat_curve()), config=SurgeConfig(), minute_fetcher=spy
        )
        # 全部 rel 高（amount 100 / base_cum 3 → 巨大），过 rough（amount≫阈值）
        snap = mk_snap([{"ts_code": c, "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 100, "amount": 1e7} for c in codes])
        for k in range(6):
            w.tick(snap, datetime(2026, 7, 6, 10, k, tzinfo=CST))
            assert len(calls) == min(2 * (k + 1), 10)   # 每 tick 至多 2 次
        assert calls == codes                            # FIFO 顺序

    def test_failed_candidate_retries_without_blocking(self) -> None:
        state = {"fail_once": True}
        bars = self._bars()

        def spy(code: str, d: date) -> pd.DataFrame:
            if code == "300000.SZ" and state["fail_once"]:
                state["fail_once"] = False
                raise ConnectionError("boom")
            return bars

        codes = ["300000.SZ", "300001.SZ"]
        w = SurgeWatcher(mk_baseline({c: 1e6 for c in codes}, curve=flat_curve()),
                         config=SurgeConfig(), minute_fetcher=spy)
        # volume 大使 VWAP 门通过（vwap=1e7/1e8=0.1 ≤ price1.0）→ 取数成功即确认
        snap = mk_snap([{"ts_code": c, "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e8, "amount": 1e7} for c in codes])
        w.tick(snap, datetime(2026, 7, 6, 10, 0, tzinfo=CST))  # 300000 失败, 300001 成功
        assert "300001.SZ" in w.pushed_today
        assert "300000.SZ" not in w.pushed_today
        w.tick(snap, datetime(2026, 7, 6, 10, 1, tzinfo=CST))  # 300000 重试成功
        assert "300000.SZ" in w.pushed_today


# ── U7 守卫 ─────────────────────────────────────────────────────────────────────


class TestU7Guards:
    def test_non_trading_day_exits(self) -> None:
        called = {"snap": 0}

        def snap_fetch() -> pd.DataFrame:
            called["snap"] += 1
            return mk_snap([])

        rc = run_surge_watch(
            baseline=mk_baseline({}),
            is_trading_day_fn=lambda d: False,
            snapshot_fetcher=snap_fetch,
            full_snapshot_fetcher=lambda: pd.DataFrame(),
            minute_fetcher=lambda c, d: pd.DataFrame(),
            notify_fn=lambda *a, **k: None,
            now_fn=lambda: datetime(2026, 7, 4, 10, 0, tzinfo=CST),
        )
        assert rc == 0 and called["snap"] == 0

    def test_lunch_boundaries(self) -> None:
        assert not _is_lunch(dt_time(11, 30))
        assert _is_lunch(dt_time(11, 31))
        assert _is_lunch(dt_time(12, 59))
        assert not _is_lunch(dt_time(13, 0))
        assert not _is_lunch(dt_time(14, 57))

    def test_five_miss_degrade_alert_once_and_backoff(self, tmp_path: Path) -> None:
        sleeps: list[float] = []
        notifies: list[tuple] = []
        clock = {"t": datetime(2026, 7, 6, 10, 0, tzinfo=CST)}

        def now_fn() -> datetime:
            return clock["t"]

        def sleep_fn(sec: float) -> None:
            sleeps.append(sec)
            clock["t"] = clock["t"] + timedelta(seconds=sec)

        run_surge_watch(
            baseline=mk_baseline({}),
            base_dir=tmp_path,
            is_trading_day_fn=lambda d: True,
            snapshot_fetcher=lambda: mk_snap([]),   # 恒 miss
            full_snapshot_fetcher=lambda: pd.DataFrame(),
            minute_fetcher=lambda c, d: pd.DataFrame(),
            notify_fn=lambda scene, **k: notifies.append((scene, k)),
            now_fn=now_fn,
            sleep_fn=sleep_fn,
            max_ticks=8,
        )
        err = [n for n in notifies if n[0] == "error"]
        assert len(err) == 1                         # 降级告警恰一条
        # streak 5/6/7 的退避 = 60/120/300（前 4 次 miss 每次 60）
        assert sleeps[:4] == [60, 60, 60, 60]
        assert sleeps[4:7] == [60, 120, 300]


# ── U8 落盘 ─────────────────────────────────────────────────────────────────────


class TestU8Persist:
    def test_events_jsonl_structure(self, tmp_path: Path) -> None:
        p = tmp_path / "events.jsonl"
        append_events(p, [SurgeConfirmed(ts_code="300001.SZ", name="n", theme="人形机器人",
                                         confirmed_at="10:05", rel_cum_3d=2.5, cum_amount=9e7)])
        append_events(p, [SurgeConfirmed(ts_code="688001.SH", name="m", confirmed_at="10:06")])
        lines = p.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        rec = json.loads(lines[0])
        assert rec["ts_code"] == "300001.SZ" and rec["theme"] == "人形机器人"
        assert rec["rel_cum_3d"] == 2.5

    def test_series_parquet_roundtrip(self, tmp_path: Path) -> None:
        cum = {"300001.SZ": np.full(CURVE_POINTS, np.nan)}
        cum["300001.SZ"][0] = 100.0
        cum["300001.SZ"][2] = 300.0
        df = series_to_frame(cum)
        assert set(df.columns) == {"ts_code", "minute_idx", "cum_amount"}
        assert len(df) == 2                          # 只落非 NaN 格
        p = tmp_path / "s.parquet"
        atomic_write_parquet(df, p)
        back = pd.read_parquet(p)
        assert len(back) == 2

    def test_atomic_write_no_tmp_left(self, tmp_path: Path) -> None:
        p = tmp_path / "snapshot.parquet"
        atomic_write_parquet(mk_snap([{"ts_code": "300001.SZ", "amount": 1}]), p)
        assert p.exists()
        assert not (tmp_path / "snapshot.parquet.tmp").exists()


# ── U9 poller 云端 feed 第 0 路由 ───────────────────────────────────────────────


class TestU9CloudFeed:
    def _cloud_df(self) -> pd.DataFrame:
        df = mk_snap([{"ts_code": "300001.SZ", "price": 11, "pre_close": 10, "pct_chg": 5,
                       "volume": 1e6, "amount": 9e7}])
        return df

    def test_fresh_cloud_feed_skips_local_fetch(self, tmp_path: Path) -> None:
        from rquant.panorama_poller import SourcePoller

        spy = {"n": 0}

        def snap_fetch(allow: bool) -> pd.DataFrame:
            spy["n"] += 1
            df = self._cloud_df()
            df.attrs["route"] = "em_direct"
            return df

        poller = SourcePoller(
            now=lambda: 1000.0,
            snapshot_fetcher=snap_fetch,
            flow_fetcher=lambda s: pd.DataFrame({"board_name": ["x"], "main_net_amount": [1]},
                                                ).assign(),
            cloud_feed_fetcher=lambda: (self._cloud_df(), "cloud_feed"),
            drop_dir=tmp_path,
        )
        poller._poll_snapshot()
        df, as_of, route = poller.snapshot()
        assert route == "cloud_feed" and not df.empty
        assert spy["n"] == 0                          # 本机自拉零调用

    def test_stale_or_failed_cloud_falls_back(self, tmp_path: Path) -> None:
        from rquant.panorama_poller import SourcePoller

        spy = {"n": 0}

        def snap_fetch(allow: bool) -> pd.DataFrame:
            spy["n"] += 1
            df = self._cloud_df()
            df.attrs["route"] = "em_direct"
            return df

        poller = SourcePoller(
            now=lambda: 1000.0,
            snapshot_fetcher=snap_fetch,
            flow_fetcher=lambda s: pd.DataFrame(),
            cloud_feed_fetcher=lambda: None,          # 陈旧/失败 → None
            drop_dir=tmp_path,
        )
        poller._poll_snapshot()
        _, _, route = poller.snapshot()
        assert route == "em_direct" and spy["n"] == 1  # 回落自拉

    def test_env_unset_default_no_network(self, monkeypatch) -> None:
        """env 未配 → _default_cloud_feed 立即返回 None，不发 HTTP（回归护栏）。"""
        from rquant.panorama_poller import _default_cloud_feed

        monkeypatch.delenv("RQUANT_CLOUD_FEED_URL", raising=False)
        assert _default_cloud_feed() is None


# ── U10 推送 ────────────────────────────────────────────────────────────────────


class TestU10Push:
    def test_message_structure_fields(self) -> None:
        c = SurgeConfirmed(ts_code="300001.SZ", name="机器人A", theme="人形机器人",
                           pct_chg=8.3, rel_cum_3d=2.7, cum_amount=1.2e8,
                           minute_delta=3e6, minute_delta_median_3d=1e6, room_to_limit_pct=4.5)
        now = datetime(2026, 7, 6, 10, 5, tzinfo=CST)
        title, body = build_surge_messages([c], now, SurgeConfig())[0]
        assert "10:05" in title
        assert "人形机器人" in body                   # 题材
        assert "量比3日2.7×" in body                  # 量比
        assert "距涨停4.5%" in body                   # 距涨停空间
        assert "口径 v1" in body                      # 尾注口径版本

    def test_dry_run_no_push(self, tmp_path: Path, capsys) -> None:
        d = date(2026, 7, 6)
        bars = mk_minute_bars({d - timedelta(days=i): [10, 10, 10] for i in (1, 2, 3)})
        notifies: list = []
        clock = {"t": datetime(2026, 7, 6, 10, 0, tzinfo=CST)}
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e8, "amount": 1e7}])

        def now_fn() -> datetime:
            return clock["t"]

        def sleep_fn(sec: float) -> None:
            clock["t"] = clock["t"] + timedelta(seconds=sec)

        run_surge_watch(
            dry_run=True,
            baseline=mk_baseline({"300001.SZ": 1e6}, curve=flat_curve()),
            base_dir=tmp_path,
            is_trading_day_fn=lambda dd: True,
            snapshot_fetcher=lambda: snap,
            full_snapshot_fetcher=lambda: pd.DataFrame(),
            minute_fetcher=lambda c, dd: bars,
            notify_fn=lambda scene, **k: notifies.append((scene, k)),
            now_fn=now_fn,
            sleep_fn=sleep_fn,
            max_ticks=2,
        )
        assert notifies == []                         # dry-run 零推送
        out = capsys.readouterr().out
        assert "DRY-RUN" in out and "300001.SZ" in out


# ── 通知 scene 路由（surge_watch 只 admin） ─────────────────────────────────────


class TestNotifyScene:
    def test_surge_watch_pushdeer_only(self, monkeypatch) -> None:
        from rquant.notify import api as notify_api

        pd_calls: list = []
        pp_calls: list = []
        # 只能改真实字段（pushdeer_key_list / pushplus_token_list 是只读 property）
        monkeypatch.setattr(notify_api.settings, "notify_enabled", True, raising=False)
        monkeypatch.setattr(notify_api.settings, "notify_surge_watch", True, raising=False)
        monkeypatch.setattr(notify_api.settings, "pushdeer_keys", "k1", raising=False)
        monkeypatch.setattr(notify_api.settings, "pushplus_tokens", "t1", raising=False)

        class _PD:
            def __init__(self, keys, endpoint): ...
            def push(self, title, body):
                pd_calls.append((title, body))
                return [(True, None)]

        class _PP:
            def __init__(self, tokens, endpoint): ...
            def push(self, title, body):
                pp_calls.append((title, body))
                return [(True, None)]

        monkeypatch.setattr(notify_api, "PushDeerClient", _PD)
        monkeypatch.setattr(notify_api, "PushPlusClient", _PP)
        notify_api.notify("surge_watch", title="爆量", body="x")
        assert len(pd_calls) == 1                     # 推 PushDeer（admin）
        assert pp_calls == []                         # 不推 PushPlus（美丞）


# ── E1 一天快照序列回放（自包含离线 fixture） ───────────────────────────────────


def _write_sim_fixture(sim_dir: Path) -> None:
    """构造一天快照序列 fixture：A 10:05 爆量、B 09:31 爆量(9:33 才推)、C 粗筛过确认不过。"""
    day = date(2026, 7, 6)
    sim_dir.mkdir(parents=True, exist_ok=True)
    codes = {"300111.SZ": "人形机器人", "300222.SZ": "存储芯片", "688333.SH": ""}
    # baseline：avg20（元）+ 题材
    (sim_dir / "baseline.json").write_text(json.dumps({
        "avg20": {c: 5e7 for c in codes},
        "theme": {"300111.SZ": "人形机器人", "300222.SZ": "存储芯片"},
    }), encoding="utf-8")
    # confirm_bars：3 prior days × 241 根恒定额 → 同刻累计中位可控
    #   A/B 每分钟 5e5（cum 小 → rel 大）；C 每分钟 3e6（cum 大 → rel<2）
    per_min = {"300111.SZ": 5e5, "300222.SZ": 5e5, "688333.SH": 3e6}
    rows: list[dict] = []
    for c in codes:
        for dd in (1, 2, 3):
            d0 = day - timedelta(days=dd)
            for i in range(241):
                t = datetime.combine(d0, dt_time(9, 30)) + timedelta(minutes=i)
                rows.append({"ts_code": c, "trade_time": t, "amount": per_min[c]})
    pd.DataFrame(rows).to_parquet(sim_dir / "confirm_bars.parquet", index=False)
    # 逐分钟快照 09:30..10:06
    for i in range(37):
        t = (datetime(2026, 7, 6, 9, 30) + timedelta(minutes=i)).time()
        hhmm = f"{t.hour:02d}{t.minute:02d}"
        # 爆量前累计额压在开盘 rough 阈值（≈465k）之下，确保只在爆量分钟才成候选
        a_amt = 2e8 if t >= dt_time(10, 5) else 3e5          # A 10:05 爆量
        b_amt = 1e8 if t >= dt_time(9, 31) else 1e5          # B 09:31 爆量
        c_amt = 1e8 if t >= dt_time(10, 5) else 3e5          # C 10:05 起过 rough
        snap = mk_snap([
            {"ts_code": "300111.SZ", "name": "机器人A", "price": 25, "pre_close": 22,
             "pct_chg": 9, "volume": 1e7, "amount": a_amt},
            {"ts_code": "300222.SZ", "name": "存储B", "price": 15, "pre_close": 13.5,
             "pct_chg": 8, "volume": 1e7, "amount": b_amt},
            {"ts_code": "688333.SH", "name": "科创C", "price": 30, "pre_close": 27,
             "pct_chg": 7, "volume": 1e7, "amount": c_amt},
        ])
        snap.to_parquet(sim_dir / f"2026-07-06T{hhmm}.parquet", index=False)


class TestE1Simulate:
    def test_replay_three_cases(self, tmp_path: Path, capsys) -> None:
        sim_dir = tmp_path / "sim"
        _write_sim_fixture(sim_dir)
        pushes: list[tuple] = []
        rc = run_simulate(
            sim_dir,
            dry_run=False,
            base_dir=tmp_path / "live",
            notify_fn=lambda scene, **k: pushes.append((scene, k)),
        )
        assert rc == 0
        events = (tmp_path / "live" / "events-2026-07-06.jsonl").read_text(encoding="utf-8")
        recs = [json.loads(x) for x in events.strip().split("\n")]
        by_code = {r["ts_code"]: r for r in recs}
        # A、B 确认；C 粗筛过但确认不过 → 无 event
        assert set(by_code) == {"300111.SZ", "300222.SZ"}
        assert "688333.SH" not in by_code
        # B 09:31 爆量但 09:33 才推（静默收集）
        assert by_code["300222.SZ"]["confirmed_at"] == "09:31"
        b_push = [p for p in pushes if "存储B" in p[1]["body"]][0]
        assert "09:33" in b_push[1]["title"]         # 推送发生在 09:33 flush
        # A 10:05 确认并推送，带题材
        assert by_code["300111.SZ"]["confirmed_at"] == "10:05"
        assert by_code["300111.SZ"]["theme"] == "人形机器人"
        assert all(p[0] == "surge_watch" for p in pushes)


# ── U11 题材映射三级兜底链 ──────────────────────────────────────────────────────


class _RawStore:
    """轻量 store 桩：只暴露 ``_conn``（load_theme_map 传入路径 owns=False 不关闭）。"""

    def __init__(self, conn) -> None:
        self._conn = conn


@contextmanager
def _capture_loguru(level: str = "INFO"):
    """捕获 loguru 输出到 list（pytest caplog 抓不到 loguru，自建 sink）。"""
    from loguru import logger

    msgs: list[str] = []
    sink_id = logger.add(lambda m: msgs.append(str(m)), level=level)
    try:
        yield msgs
    finally:
        logger.remove(sink_id)


def _theme_store(tmp_path: Path, *, tables: dict[str, str]) -> _RawStore:
    """建一个只含指定表的 tmp DuckDB（不走 DuckDBStore，避免 ALL_DDL 建满全表）。"""
    import duckdb

    conn = duckdb.connect(str(tmp_path / "theme.duckdb"))
    for ddl in tables.values():
        conn.execute(ddl)
    return _RawStore(conn)


_KPL_MEMBER_DDL = (
    "CREATE TABLE kpl_concept_member "
    "(board_code VARCHAR, board_name VARCHAR, con_code VARCHAR)"
)
_KPL_DAILY_DDL = (
    "CREATE TABLE kpl_concept_member_daily "
    "(trade_date DATE, board_code VARCHAR, board_name VARCHAR, con_code VARCHAR)"
)
_DC_BOARD_DDL = (
    "CREATE TABLE dc_board (ts_code VARCHAR, name VARCHAR, idx_type VARCHAR)"
)
_DC_MEMBER_DDL = (
    "CREATE TABLE dc_board_member (board_code VARCHAR, con_code VARCHAR)"
)


class TestU11ThemeMapFallback:
    def test_level1_kpl_snapshot_hit(self, tmp_path: Path) -> None:
        store = _theme_store(tmp_path, tables={"kpl": _KPL_MEMBER_DDL})
        store._conn.executemany(
            "INSERT INTO kpl_concept_member VALUES (?, ?, ?)",
            [("000129.KP", "人形机器人", "300111.SZ"),
             ("000130.KP", "存储", "300222.SZ")],
        )
        with _capture_loguru() as logs:
            m = load_theme_map(store)
        assert m == {"300111.SZ": "人形机器人", "300222.SZ": "存储"}
        assert any("命中 kpl_concept_member" in x for x in logs)

    def test_level1_keeps_first_theme_for_multi_board(self, tmp_path: Path) -> None:
        store = _theme_store(tmp_path, tables={"kpl": _KPL_MEMBER_DDL})
        store._conn.executemany(
            "INSERT INTO kpl_concept_member VALUES (?, ?, ?)",
            [("000129.KP", "人形机器人", "300111.SZ"),
             ("000131.KP", "减速器", "300111.SZ")],  # 同票第二题材，应被忽略
        )
        m = load_theme_map(store)
        assert m == {"300111.SZ": "人形机器人"}

    def test_level2_daily_latest_date_when_snapshot_missing(self, tmp_path: Path) -> None:
        store = _theme_store(tmp_path, tables={"daily": _KPL_DAILY_DDL})
        store._conn.executemany(
            "INSERT INTO kpl_concept_member_daily VALUES (?, ?, ?, ?)",
            [(date(2026, 7, 2), "000130.KP", "旧题材", "300222.SZ"),
             (date(2026, 7, 3), "000129.KP", "最新题材", "300111.SZ")],
        )
        with _capture_loguru() as logs:
            m = load_theme_map(store)
        # kpl_concept_member 缺表 → 降级到 daily，只取最新 trade_date 打点
        assert m == {"300111.SZ": "最新题材"}
        assert any("命中 kpl_concept_member_daily" in x for x in logs)

    def test_level3_dc_concept_when_kpl_missing(self, tmp_path: Path) -> None:
        store = _theme_store(
            tmp_path, tables={"b": _DC_BOARD_DDL, "m": _DC_MEMBER_DDL}
        )
        store._conn.executemany(
            "INSERT INTO dc_board VALUES (?, ?, ?)",
            [("BK0001", "工程建设", "概念板块"),
             ("BK0002", "钢铁行业", "行业板块")],  # 非概念，应被 WHERE 排除
        )
        store._conn.executemany(
            "INSERT INTO dc_board_member VALUES (?, ?)",
            [("BK0001", "601390.SH"), ("BK0002", "600019.SH")],
        )
        with _capture_loguru() as logs:
            m = load_theme_map(store)
        assert m == {"601390.SH": "工程建设"}  # 行业板块成分不入题材映射
        assert any("命中 dc_board_member" in x for x in logs)

    def test_level1_short_circuits_before_level3(self, tmp_path: Path) -> None:
        store = _theme_store(
            tmp_path,
            tables={"kpl": _KPL_MEMBER_DDL, "b": _DC_BOARD_DDL, "m": _DC_MEMBER_DDL},
        )
        store._conn.execute(
            "INSERT INTO kpl_concept_member VALUES ('000129.KP', 'kpl题材', '300111.SZ')"
        )
        store._conn.execute("INSERT INTO dc_board VALUES ('BK0001', 'dc题材', '概念板块')")
        store._conn.execute("INSERT INTO dc_board_member VALUES ('BK0001', '300111.SZ')")
        m = load_theme_map(store)
        assert m == {"300111.SZ": "kpl题材"}  # 命中即止，不落到东财

    def test_empty_level_falls_through(self, tmp_path: Path) -> None:
        # kpl 表存在但空 → 降级到 daily（也空）→ 降级到东财概念（有数据）
        store = _theme_store(
            tmp_path,
            tables={
                "kpl": _KPL_MEMBER_DDL,
                "daily": _KPL_DAILY_DDL,
                "b": _DC_BOARD_DDL,
                "m": _DC_MEMBER_DDL,
            },
        )
        store._conn.execute("INSERT INTO dc_board VALUES ('BK0001', '东财题材', '概念板块')")
        store._conn.execute("INSERT INTO dc_board_member VALUES ('BK0001', '601390.SH')")
        m = load_theme_map(store)
        assert m == {"601390.SH": "东财题材"}

    def test_all_missing_returns_empty_no_raise(self, tmp_path: Path) -> None:
        # 三级表全缺（云端只读副本无 kpl_* 也无 dc_board）→ 每级 CatalogException
        # 被吞，返回空 dict，不炸
        store = _theme_store(tmp_path, tables={})
        m = load_theme_map(store)
        assert m == {}
