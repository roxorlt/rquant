"""surge-watch 单测（全离线，注入时钟/mock 源，不真 sleep 不碰网络）。

口径 v3（纯累计，2026-07-06 全天真实分钟回测标定）：rough1.2×20d·curve /
确认层纯累计比值 rel_cum = today_cum / N(=4)日同刻累计中位 ∈ [k_cum2.5, ratio_cap8] /
skip 开盘前 1 分（9:32 起确认）/ 可买性守卫（距涨停≤1%或已封板不推）。
VWAP 门 + 单分钟增量门为 v2 遗留，v3 默认关（require_vwap=False、k_delta_confirm=0）。
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from rquant.surge_watch import (
    CURVE_POINTS,
    SurgeBaseline,
    SurgeConfig,
    SurgeConfirmed,
    SurgeWatcher,
    _detection_domain,
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


def rel_amount(
    bars: pd.DataFrame, gi: int, rel: float, *, today: date = date(2026, 7, 6), n: int = 4
) -> float:
    """反算使 rel_cum == rel 的当日累计额（snapshot amount）。

    v3 纯累计比值有上下门 [k_cum, ratio_cap]，测试须把 rel 精确落在带内——直接用
    N 日同刻累计中位 × rel 得到当日累计额，避免手算 (gi+1)×m。
    """
    base = build_three_day_baseline(bars, today, n)
    return rel * float(base.cum_median[gi])


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
        base_yuan = mk_baseline({"300001.SZ": 1e8})       # 正确：1 亿元 → 高阈值
        base_thousand = mk_baseline({"300001.SZ": 1e5})   # 错误：当千元用 → 阈值 1000× 偏小
        # 当日额压在元口径阈值之下（对 k_rough 放松鲁棒）：thousand 口径阈值小 1000×，误放行
        amount = cfg.k_rough * 1e8 * float(base_yuan.curve[gi]) * 0.9
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 11, "pre_close": 10,
                         "pct_chg": 5, "volume": 1e6, "amount": amount}])
        assert _rough_candidates(snap, base_yuan, cfg, gi) == []
        assert _rough_candidates(snap, base_thousand, cfg, gi) == ["300001.SZ"]

    def test_relaxed_rough_admits_earlier_candidate(self) -> None:
        """粗筛放松 1.5→1.2：卡在 [1.2×,1.5×) 阈值带的当日额，v3 放行早入确认池，旧值挡下。"""
        base, _cfg, gi = self._base()
        amount = 1.35 * 1e8 * float(base.curve[gi])   # 300001 avg20=1e8，量在两阈值之间
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 11, "pre_close": 10,
                         "pct_chg": 5, "volume": 1e6, "amount": amount}])
        assert _rough_candidates(snap, base, SurgeConfig(), gi) == ["300001.SZ"]   # v3 1.2 放行
        assert _rough_candidates(snap, base, SurgeConfig(k_rough=1.5), gi) == []   # 旧 1.5 挡下


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

    def _confirm_watcher(self, **cfg_kw) -> tuple[SurgeWatcher, pd.DataFrame, datetime, int]:
        """gi=2（09:32，skip_first_minutes=1 后首个可确认格），cum_median[2]=600。"""
        base = mk_baseline({"300001.SZ": 1e6}, curve=flat_curve())
        bars = self._three_day_bars()
        w = SurgeWatcher(base, config=SurgeConfig(**cfg_kw), minute_fetcher=lambda c, d: bars)
        gi = 2
        now = datetime(2026, 7, 6, 9, 32, tzinfo=CST)
        cache = build_three_day_baseline(bars, now.date(), 4)
        w.confirm_cache["300001.SZ"] = cache
        assert cache.cum_median[gi] == 600
        return w, bars, now, gi

    def test_pure_cum_lower_boundary(self) -> None:
        """纯累计下门 k_cum=2.5：rel 恰好 2.5（amount=1500）→ 确认；1499→2.498<2.5→拒。"""
        w, _bars, now, gi = self._confirm_watcher()
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e6, "amount": 1500}])
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today
        assert w._pending_push[0].rel_cum == 2.5

    def test_pure_cum_below_lower_rejected(self) -> None:
        w, _bars, now, gi = self._confirm_watcher()
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e6, "amount": 1499}])   # rel 2.498 < 2.5
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" not in w.pushed_today

    def test_ratio_cap_upper_boundary(self) -> None:
        """毒尾封顶 ratio_cap=8.0：rel 恰好 8.0（amount=4800）→ 确认（含上界）。"""
        w, _bars, now, gi = self._confirm_watcher()
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e6, "amount": 4800}])
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today
        assert w._pending_push[0].rel_cum == 8.0

    def test_ratio_cap_toxic_tail_rejected(self) -> None:
        """rel > ratio_cap 视为极端出货毒尾（放巨量往往出货）→ 不推。"""
        w, _bars, now, gi = self._confirm_watcher()
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e6, "amount": 4801}])   # rel 8.002 > 8.0
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" not in w.pushed_today

    def test_skip_first_minutes_blocks_before_eligible(self) -> None:
        """skip_first_minutes=1：9:30(gi0)/9:31(gi1) 恒不确认，即便 rel 已过下门。"""
        w, _bars, _now, _gi = self._confirm_watcher()
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e6, "amount": 4000}])   # rel≫2.5 于任意格
        # 9:31（gi=1）被 skip 挡下，不确认
        w._evaluate("300001.SZ", snap, datetime(2026, 7, 6, 9, 31, tzinfo=CST), 1)
        assert "300001.SZ" not in w.pushed_today
        # 9:32（gi=2）越过 skip → 确认
        w._evaluate("300001.SZ", snap, datetime(2026, 7, 6, 9, 32, tzinfo=CST), 2)
        assert "300001.SZ" in w.pushed_today

    def test_vwap_gate_only_when_enabled(self) -> None:
        """require_vwap=True 时才叠加 VWAP 门（v3 默认关）：price<vwap 则拒。"""
        # rel=4800/600=8.0 落带内，但 price 1.0 < vwap(amount/volume=4800/1000=4.8) → 拒
        w_on, _b, now, gi = self._confirm_watcher(require_vwap=True)
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1000, "amount": 4800}])
        w_on._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" not in w_on.pushed_today
        # 默认关：同一 snap 不看 VWAP → 确认
        w_off, _b2, now2, gi2 = self._confirm_watcher()
        w_off._evaluate("300001.SZ", snap, now2, gi2)
        assert "300001.SZ" in w_off.pushed_today

    def test_daily_cache_hit_no_refetch(self) -> None:
        """当日缓存命中不重拉（tushare spy 调用数）。"""
        calls: list[str] = []
        # 大基线：过 rough 但 rel<k_cum（cum 250k / N 日中位 300k = 0.83×）→ 不确认但已缓存
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
        assert "300001.SZ" not in w.pushed_today      # rel<k_cum(2.5) 未确认
        # 下一分钟仍未确认，命中缓存不再拉
        w.tick(snap, datetime(2026, 7, 6, 10, 1, tzinfo=CST))
        assert calls == ["300001.SZ"]                # 无第二次取数
        assert "300001.SZ" in w.confirm_cache


# ── U5 去重/静默/折叠 ───────────────────────────────────────────────────────────


class TestU5DedupSilentFold:
    def _bars(self) -> pd.DataFrame:
        d = date(2026, 7, 6)
        # 三日每分钟恒定 1e4（全 241 网格）→ cum_median[gi]=(gi+1)×1e4，rel 精确可控
        return mk_minute_bars({d - timedelta(days=i): [1e4] * 241 for i in (1, 2, 3)})

    def _watcher(self) -> tuple[SurgeWatcher, pd.DataFrame]:
        # avg20 微小 → rough 恒过；确认由纯累计比值决定，rel 由 snap amount 精确落带内
        bars = self._bars()
        base = mk_baseline({"300001.SZ": 1.0}, curve=flat_curve())
        return SurgeWatcher(base, config=SurgeConfig(), minute_fetcher=lambda c, dd: bars), bars

    def _snap_at(self, gi: int, bars: pd.DataFrame, rel: float = 4.0) -> pd.DataFrame:
        # amount = rel × N日同刻累计中位 → rel_cum 精确 = rel（落 [k_cum,ratio_cap]）
        amount = rel_amount(bars, gi, rel)
        return mk_snap([{"ts_code": "300001.SZ", "price": 100.0, "pre_close": 90,
                         "pct_chg": 5, "volume": 1e6, "amount": amount}])

    def test_once_per_day(self) -> None:
        w, bars = self._watcher()
        snap = self._snap_at(30, bars)               # gi30=10:00，rel=4 落带内
        r1 = w.tick(snap, datetime(2026, 7, 6, 10, 0, tzinfo=CST))
        assert r1.confirmed and r1.confirmed[0].ts_code == "300001.SZ"
        r2 = w.tick(snap, datetime(2026, 7, 6, 10, 1, tzinfo=CST))
        assert r2.confirmed == []                    # 每票每日仅推一次

    def test_silent_window_collects_then_flushes(self) -> None:
        w, bars = self._watcher()
        snap = self._snap_at(2, bars)                # gi2=9:32（skip 后首个可确认格）
        r_silent = w.tick(snap, datetime(2026, 7, 6, 9, 32, tzinfo=CST))  # 9:33 前
        assert r_silent.pushes == [] and r_silent.confirmed == []
        assert len(w._pending_push) == 1             # 收集不丢
        r_flush = w.tick(mk_snap([]), datetime(2026, 7, 6, 9, 33, tzinfo=CST))
        assert r_flush.pushes and r_flush.confirmed[0].ts_code == "300001.SZ"

    def test_fold_over_max(self) -> None:
        confirmed = [
            SurgeConfirmed(ts_code=f"3000{i:02d}.SZ", name=f"n{i}", pct_chg=5.0, rel_cum=3.0)
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
        # 三日每分钟恒定 1e5（全 241 网格）→ cum_median[gi]=(gi+1)×1e5，rel 精确可控
        return mk_minute_bars({d - timedelta(days=i): [1e5] * 241 for i in (1, 2, 3)})

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
        # amount 1e7 ≫ rough 阈值 → 全部入队（本例只验限频/FIFO，不看确认结果）
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
        # amount 使 rel_cum=4 落带内（gi30，cum_median[30]=3.1e6）→ 取数成功即确认
        amt0 = rel_amount(bars, 30, 4.0)
        snap = mk_snap([{"ts_code": c, "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e8, "amount": amt0} for c in codes])
        w.tick(snap, datetime(2026, 7, 6, 10, 0, tzinfo=CST))  # 300000 失败, 300001 成功
        assert "300001.SZ" in w.pushed_today
        assert "300000.SZ" not in w.pushed_today
        # 下一分钟（gi31）amount 相应 rel=4 → 300000 重试成功即确认
        amt1 = rel_amount(bars, 31, 4.0)
        snap2 = mk_snap([{"ts_code": c, "price": 1.0, "pre_close": 0.9,
                          "pct_chg": 5, "volume": 1e8, "amount": amt1} for c in codes])
        w.tick(snap2, datetime(2026, 7, 6, 10, 1, tzinfo=CST))  # 300000 重试成功
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
                                         confirmed_at="10:05", rel_cum=2.5, cum_amount=9e7)])
        append_events(p, [SurgeConfirmed(ts_code="688001.SH", name="m", confirmed_at="10:06")])
        lines = p.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        rec = json.loads(lines[0])
        assert rec["ts_code"] == "300001.SZ" and rec["theme"] == "人形机器人"
        assert rec["rel_cum"] == 2.5

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
                           pct_chg=8.3, rel_cum=2.7, cum_amount=1.2e8,
                           minute_delta=3e6, minute_delta_median=1e6, room_to_limit_pct=4.5)
        now = datetime(2026, 7, 6, 10, 5, tzinfo=CST)
        title, body = build_surge_messages([c], now, SurgeConfig())[0]
        assert "10:05" in title
        assert "人形机器人" in body                   # 题材
        assert "累计比4日2.7×" in body                # 纯累计比值（N=4）
        assert "距涨停4.5%" in body                   # 距涨停空间
        assert "口径 v3(纯累计)" in body              # 尾注口径版本
        assert "累计比值∈[2.5,8]" in body            # 上下门口径
        assert "skip前1分(9:31)" in body             # skip_first_minutes 口径
        assert "增量门" not in body                   # v2 增量门 v3 默认关，报文不出现
        assert "观察提示，非买入信号" in body          # 定位尾注

    def test_dry_run_no_push(self, tmp_path: Path, capsys) -> None:
        d = date(2026, 7, 6)
        # 三日每分钟恒定 1e4 → cum_median[gi]=(gi+1)×1e4，amount 使 rel=4 落带内
        bars = mk_minute_bars({d - timedelta(days=i): [1e4] * 241 for i in (1, 2, 3)})
        notifies: list = []
        clock = {"t": datetime(2026, 7, 6, 10, 0, tzinfo=CST)}
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 1.0, "pre_close": 0.9,
                         "pct_chg": 5, "volume": 1e8, "amount": rel_amount(bars, 30, 4.0)}])

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
    """一天快照序列 fixture（口径 v3 纯累计三戏路）：
    - G 300111.SZ：9:31 累计已越 2.5×（rel 恒 3.0），但 skip_first_minutes 挡下 9:31、
      9:32 才确认，静默窗持到 9:33 才推；
    - H 300222.SZ：10:05 放巨量 rel=10× > ratio_cap(8) 毒尾 → 不推、无 event；
    - K 688333.SH：累计不足 rel 恒 1.5× < k_cum(2.5) → 不推、无 event。
    confirm_bars 三日恒定 m/min → cum_median[gi]=(gi+1)×m，snap amount 精确控 rel。"""
    day = date(2026, 7, 6)
    sim_dir.mkdir(parents=True, exist_ok=True)
    per_min = {"300111.SZ": 1e5, "300222.SZ": 1e5, "688333.SH": 1e6}  # 确认基线 m/min
    (sim_dir / "baseline.json").write_text(json.dumps({
        "avg20": {c: 1e4 for c in per_min},         # avg20 微小 → 粗筛恒过，候选早入确认池
        "theme": {"300111.SZ": "人形机器人", "300222.SZ": "存储芯片"},
    }), encoding="utf-8")
    rows: list[dict] = []
    for c in per_min:
        for dd in (1, 2, 3):
            d0 = day - timedelta(days=dd)
            for i in range(241):
                t = datetime.combine(d0, dt_time(9, 30)) + timedelta(minutes=i)
                rows.append({"ts_code": c, "trade_time": t, "amount": per_min[c]})
    pd.DataFrame(rows).to_parquet(sim_dir / "confirm_bars.parquet", index=False)
    # 逐分钟快照 09:30..10:06（morning gi == i），amount = rel × base_cum[i]=(i+1)×m
    for i in range(37):
        t = (datetime(2026, 7, 6, 9, 30) + timedelta(minutes=i)).time()
        hhmm = f"{t.hour:02d}{t.minute:02d}"
        g_amt = 3.0 * (i + 1) * 1e5                          # G：rel 恒 3.0（9:31 即越 2.5）
        h_amt = 1e4 if i < 35 else 10.0 * (i + 1) * 1e5      # H：10:05(i=35) rel=10 毒尾
        k_amt = 1.5 * (i + 1) * 1e6                          # K：rel 恒 1.5 累计不足
        snap = mk_snap([
            {"ts_code": "300111.SZ", "name": "机器人G", "price": 25, "pre_close": 22,
             "pct_chg": 9, "volume": 1e7, "amount": g_amt},
            {"ts_code": "300222.SZ", "name": "存储H", "price": 15, "pre_close": 13.5,
             "pct_chg": 8, "volume": 1e7, "amount": h_amt},
            {"ts_code": "688333.SH", "name": "科创K", "price": 30, "pre_close": 27,
             "pct_chg": 7, "volume": 1e7, "amount": k_amt},
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
        # 仅 G 确认；H 毒尾（rel>8）、K 累计不足（rel<2.5）粗筛过但纯累计门不过 → 无 event
        assert set(by_code) == {"300111.SZ"}
        assert "300222.SZ" not in by_code and "688333.SH" not in by_code
        # G：9:31 rel 已 3.0>2.5，但 skip 挡到 9:32 才确认，静默窗持到 9:33 才推
        assert by_code["300111.SZ"]["confirmed_at"] == "09:32"
        assert by_code["300111.SZ"]["rel_cum"] == 3.0
        assert by_code["300111.SZ"]["theme"] == "人形机器人"
        g_push = [p for p in pushes if "机器人G" in p[1]["body"]][0]
        assert "09:33" in g_push[1]["title"]         # 推送发生在 09:33 flush
        body = g_push[1]["body"]
        assert "口径 v3(纯累计)" in body and "观察提示，非买入信号" in body
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


# ── U12 v3 默认值（2026-07-06 全天真实分钟回测标定） ─────────────────────────────


class TestU12ConfigDefaults:
    def test_v3_defaults(self) -> None:
        cfg = SurgeConfig()
        assert cfg.k_cum == 2.5                  # 纯累计下门（替代 v2 k_confirm=3.0）
        assert cfg.ratio_cap == 8.0              # 毒尾封顶：比值 >8 视为极端出货不推
        assert cfg.skip_first_minutes == 1       # 跳开盘前 1 分，9:32 起确认
        assert cfg.cum_lookback_days == 4        # N 日同刻累计中位（v2 是 3）
        assert cfg.k_delta_confirm == 0.0        # v2 增量门 v3 默认关
        assert cfg.require_vwap is False         # v2 VWAP 门 v3 默认关
        assert cfg.k_rough == 1.2                # 粗筛放松 1.5→1.2，候选早入确认池
        assert cfg.max_room_to_limit_pct == 1.0  # 可买性守卫保留，距涨停≤1% 不推


# ── U13 同分钟增量门 ────────────────────────────────────────────────────────────


class TestU13DeltaGate:
    """v2 遗留单分钟增量门（v3 默认关，须显式 k_delta>0 开启）：确认时点要求
    「当分钟增量 ≥ k_delta × N日同分钟中位」。rel 统一压在带内以隔离增量门单因子。"""

    def _today(self) -> date:
        return date(2026, 7, 6)

    def _bars_const(self) -> pd.DataFrame:
        # 三日每分钟恒定 100（全 241 网格）→ minute_median=100、cum_median[gi]=(gi+1)*100
        d = self._today()
        return mk_minute_bars({d - timedelta(days=i): [100] * 241 for i in (1, 2, 3)})

    def _bars_no_gi30(self) -> pd.DataFrame:
        # 三日仅前 3 分钟有量 → gi30 处 minute_median=0（中位≤0 场景，cum 由 cumsum 续 300）
        d = self._today()
        return mk_minute_bars({d - timedelta(days=i): [100, 100, 100] for i in (1, 2, 3)})

    def _watcher(self, bars: pd.DataFrame, *, minute_delta: float, k_delta: float) -> tuple:
        cfg = SurgeConfig(k_delta_confirm=k_delta)   # v3 默认关，显式开增量门测门
        base = mk_baseline({"300001.SZ": 1e6}, curve=flat_curve())
        w = SurgeWatcher(base, config=cfg, minute_fetcher=lambda c, d: bars)
        gi = 30
        now = datetime(2026, 7, 6, 10, 0, tzinfo=CST)
        w.confirm_cache["300001.SZ"] = build_three_day_baseline(bars, now.date(), 4)
        # amount 使 rel_cum=4 稳落 [k_cum, ratio_cap]，把确认成败隔离到增量门单因子
        amount = rel_amount(bars, gi, 4.0)
        arr = np.full(CURVE_POINTS, np.nan)
        arr[gi - 1] = amount - minute_delta
        arr[gi] = amount                  # 本分钟增量 = minute_delta
        w.cum_series["300001.SZ"] = arr
        snap = mk_snap([{"ts_code": "300001.SZ", "price": 100.0, "pre_close": 90,
                         "pct_chg": 5, "volume": 1e6, "amount": amount}])
        return w, snap, now, gi

    def test_delta_gate_passes_at_boundary(self) -> None:
        # minute_median[30]=100，k_delta=3.0 → 门槛 300；增量 300 恰好 ≥ → 通过
        w, snap, now, gi = self._watcher(self._bars_const(), minute_delta=300, k_delta=3.0)
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today
        assert w._pending_push and w._pending_push[0].minute_delta == 300

    def test_delta_gate_blocks_below_threshold(self) -> None:
        # 增量 299 < 300 → 拦截，不确认
        w, snap, now, gi = self._watcher(self._bars_const(), minute_delta=299, k_delta=3.0)
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" not in w.pushed_today
        assert w._pending_push == []

    def test_delta_gate_median_non_positive_fails(self) -> None:
        # gi30 同分钟中位=0（None-fail 语义）：即便增量巨大也不过（rel 仍落带内）
        w, snap, now, gi = self._watcher(self._bars_no_gi30(), minute_delta=1e6, k_delta=3.0)
        base = build_three_day_baseline(self._bars_no_gi30(), self._today(), 4)
        assert base.minute_median[gi] == 0
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" not in w.pushed_today

    def test_delta_gate_disabled_when_zero(self) -> None:
        # k_delta=0 关门（v3 默认）：中位=0 且增量极小也照常确认（其余门通过）
        w, snap, now, gi = self._watcher(self._bars_no_gi30(), minute_delta=1, k_delta=0.0)
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today


# ── U14 可买性守卫 ──────────────────────────────────────────────────────────────


class TestU14Buyability:
    """确认后现价距涨停 ≤ max_room（或已封板）→ 标 unbuyable，占名额、落 events、不推送。"""

    def _confirmable(
        self, *, price: float, pre_close: float, max_room: float | None = None
    ) -> tuple:
        d = date(2026, 7, 6)
        bars = mk_minute_bars({d - timedelta(days=i): [100] * 241 for i in (1, 2, 3)})
        cfg = SurgeConfig() if max_room is None else SurgeConfig(max_room_to_limit_pct=max_room)
        base = mk_baseline({"300001.SZ": 1e6}, curve=flat_curve())
        w = SurgeWatcher(base, config=cfg, minute_fetcher=lambda c, dd: bars)
        gi = 30
        now = datetime(2026, 7, 6, 10, 0, tzinfo=CST)
        w.confirm_cache["300001.SZ"] = build_three_day_baseline(bars, now.date(), 4)
        # amount 使 rel_cum=4 落带内 → 纯累计确认通过，唯一变量是可买性守卫
        amount = rel_amount(bars, gi, 4.0)
        arr = np.full(CURVE_POINTS, np.nan)
        arr[gi] = amount
        w.cum_series["300001.SZ"] = arr
        # gem 20cm：limit_up = pre_close×1.2（mk_snap 默认档）
        snap = mk_snap([{"ts_code": "300001.SZ", "price": price, "pre_close": pre_close,
                         "pct_chg": 8, "volume": 1e6, "amount": amount}])
        return w, snap, now, gi

    def test_within_room_blocks_push_but_logs_event(self) -> None:
        # pre_close 100 → limit_up 120；price 119.5 → room 0.42% ≤1% → unbuyable
        w, snap, now, gi = self._confirmable(price=119.5, pre_close=100)
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today          # 仍占「每票每日一次」名额
        assert w._pending_push == []                  # 不进报文
        assert len(w._pending_events) == 1
        assert w._pending_events[0].status == "unbuyable"

    def test_already_sealed_blocks(self) -> None:
        # price = limit_up（封板）→ room 0 ≤1% → unbuyable
        w, snap, now, gi = self._confirmable(price=120.0, pre_close=100)
        w._evaluate("300001.SZ", snap, now, gi)
        assert w._pending_push == []
        assert w._pending_events[0].status == "unbuyable"

    def test_unbuyable_flushed_to_events_not_pushed(self) -> None:
        # 过静默窗 flush：TickResult.confirmed 含 unbuyable（落 events），pushes 空
        w, snap, now, gi = self._confirmable(price=119.5, pre_close=100)
        w._evaluate("300001.SZ", snap, now, gi)
        res = w._flush(now)
        assert res.pushes == []
        assert len(res.confirmed) == 1 and res.confirmed[0].status == "unbuyable"

    def test_buyable_stock_unaffected(self) -> None:
        # pre_close 100 → limit_up 120；price 110 → room 9.1% >1% → 正常推送
        w, snap, now, gi = self._confirmable(price=110.0, pre_close=100)
        w._evaluate("300001.SZ", snap, now, gi)
        assert "300001.SZ" in w.pushed_today
        assert len(w._pending_push) == 1
        assert w._pending_push[0].status == "confirmed"
        assert w._pending_events == []


# ── U15 CLI 门槛参数解析 ────────────────────────────────────────────────────────


class TestU15CliParse:
    def test_gate_args_parsed(self) -> None:
        from rquant.cli import build_parser

        args = build_parser().parse_args(
            ["surge-watch", "--k-cum", "3.0", "--ratio-cap", "10", "--skip-first-minutes", "2",
             "--k-delta", "1.5", "--require-vwap", "--max-room", "0.5"]
        )
        assert args.k_cum == 3.0
        assert args.ratio_cap == 10.0
        assert args.skip_first_minutes == 2
        assert args.k_delta == 1.5
        assert args.require_vwap is True
        assert args.max_room == 0.5

    def test_gate_args_default_to_v3(self) -> None:
        from rquant.cli import build_parser

        args = build_parser().parse_args(["surge-watch"])
        assert args.k_cum == 2.5
        assert args.ratio_cap == 8.0
        assert args.skip_first_minutes == 1
        assert args.k_delta == 0.0
        assert args.require_vwap is False
        assert args.max_room == 1.0


# ── E2 仿真路径 v3 门（毒尾封顶拦一只、可买性守卫拦一只、正常票照推） ─────────────


def _write_sim_v3_fixture(sim_dir: Path) -> None:
    """一天快照序列：D 毒尾（rel=10>8）无 event、E 可买性守卫拦（unbuyable 不推）、
    F 正常确认并推送。confirm_bars 三日恒定 1e5/min → cum_median[gi]=(gi+1)×1e5，
    10:05(gi35) base_cum=3.6e6，snap amount = rel × base_cum 精确控 rel。"""
    day = date(2026, 7, 6)
    sim_dir.mkdir(parents=True, exist_ok=True)
    names = {"300901.SZ": "毒D", "300902.SZ": "拦E", "300903.SZ": "爆F"}
    (sim_dir / "baseline.json").write_text(json.dumps({
        "avg20": {c: 1e6 for c in names},          # 候选在爆量分钟入池（pre-boom 压 1e4 落选）
        "theme": {"300903.SZ": "存储芯片"},
    }), encoding="utf-8")

    rows: list[dict] = []
    for dd in (1, 2, 3):
        d0 = day - timedelta(days=dd)
        for c in names:                             # 三票同基线 1e5/min（全 241 网格）
            for i in range(241):
                t = datetime.combine(d0, dt_time(9, 30)) + timedelta(minutes=i)
                rows.append({"ts_code": c, "trade_time": t, "amount": 1e5})
    pd.DataFrame(rows).to_parquet(sim_dir / "confirm_bars.parquet", index=False)

    for i in range(37):                             # 09:30..10:06 逐分钟（morning gi==i）
        t = (datetime(2026, 7, 6, 9, 30) + timedelta(minutes=i)).time()
        hhmm = f"{t.hour:02d}{t.minute:02d}"
        boom = i >= 35                              # 10:05 起爆量
        base_cum = (i + 1) * 1e5
        d_amt = 10.0 * base_cum if boom else 1e4    # D：rel=10 > ratio_cap(8) 毒尾
        ef_amt = 4.0 * base_cum if boom else 1e4    # E/F：rel=4 落带内
        snap = mk_snap([
            # D：rel=10 毒尾 → 不确认、无 event（price/room 不影响，先被 cap 拦）
            {"ts_code": "300901.SZ", "name": "毒D", "price": 11.0, "pre_close": 10,
             "pct_chg": 10, "volume": 1e7, "amount": d_amt},
            # E：limit_up=12.0，price 11.9 → room 0.84% ≤1% → 确认但 unbuyable
            {"ts_code": "300902.SZ", "name": "拦E", "price": 11.9, "pre_close": 10,
             "pct_chg": 19, "volume": 1e9, "amount": ef_amt},
            # F：price 11 → room 9.1% >1% → 正常推送
            {"ts_code": "300903.SZ", "name": "爆F", "price": 11.0, "pre_close": 10,
             "pct_chg": 10, "volume": 1e9, "amount": ef_amt},
        ])
        snap.to_parquet(sim_dir / f"2026-07-06T{hhmm}.parquet", index=False)


class TestE2SimulateV3Gates:
    def test_cap_and_room_gates_in_simulate(self, tmp_path: Path) -> None:
        sim_dir = tmp_path / "sim_v3"
        _write_sim_v3_fixture(sim_dir)
        pushes: list[tuple] = []
        rc = run_simulate(
            sim_dir,
            dry_run=False,
            base_dir=tmp_path / "live",
            notify_fn=lambda scene, **k: pushes.append((scene, k)),
        )
        assert rc == 0
        events = (tmp_path / "live" / "events-2026-07-06.jsonl").read_text(encoding="utf-8")
        by_code = {json.loads(x)["ts_code"]: json.loads(x) for x in events.strip().split("\n")}
        # D 被毒尾封顶拦（rel>8）→ 根本不产生 event
        assert "300901.SZ" not in by_code
        # E 确认但可买性守卫拦 → 落 event 标 unbuyable
        assert by_code["300902.SZ"]["status"] == "unbuyable"
        assert by_code["300902.SZ"]["rel_cum"] == 4.0
        # F 正常确认 → status confirmed
        assert by_code["300903.SZ"]["status"] == "confirmed"
        # 推送只含 F（买得进），不含 E（买不进）
        assert len(pushes) == 1
        body = pushes[0][1]["body"]
        assert "爆F" in body and "拦E" not in body
        assert "口径 v3(纯累计)" in body and "观察提示，非买入信号" in body


# ── U16 全市场重构（D1）：全市场取数 → 检测层过滤 config.boards ─────────────────


class TestU16FullMarketRefactor:
    """一次全市场快照兼作检测输入（过滤 boards + 排 ST）与共享 feed（snapshot_full 全落）。"""

    def _full_market(self) -> pd.DataFrame:
        """主板/创业/科创/北交所 + 一只 ST 创业股，amount 足够大（避免 rough 落选干扰）。"""
        rows = [
            {"ts_code": "600519.SH", "name": "主板甲", "price": 11, "pre_close": 10,
             "pct_chg": 10, "volume": 1e7, "amount": 5e8},   # main
            {"ts_code": "300111.SZ", "name": "创业乙", "price": 11, "pre_close": 10,
             "pct_chg": 10, "volume": 1e7, "amount": 5e8},   # gem
            {"ts_code": "688333.SH", "name": "科创丙", "price": 11, "pre_close": 10,
             "pct_chg": 10, "volume": 1e7, "amount": 5e8},   # star
            {"ts_code": "830001.BJ", "name": "北交丁", "price": 11, "pre_close": 10,
             "pct_chg": 10, "volume": 1e7, "amount": 5e8},   # bj
            {"ts_code": "300777.SZ", "name": "ST妖戊", "price": 11, "pre_close": 10,
             "pct_chg": 10, "volume": 1e7, "amount": 5e8},   # gem 但 ST
        ]
        return mk_snap(rows)

    def test_detection_domain_narrows_to_boards(self) -> None:
        det = _detection_domain(self._full_market(), ("gem", "star"))
        codes = set(det["ts_code"])
        # 只留创业+科创（含 ST 创业股，ST 由 rough 排除，不在此过滤）；主板/北交所被过滤
        assert codes == {"300111.SZ", "688333.SH", "300777.SZ"}
        assert "600519.SH" not in codes and "830001.BJ" not in codes

    def test_detection_domain_empty_passthrough(self) -> None:
        empty = mk_snap([])
        assert _detection_domain(empty, ("gem", "star")).empty

    def test_rough_candidates_exclude_st_and_offboard(self) -> None:
        full = self._full_market()
        det = _detection_domain(full, ("gem", "star"))
        base = mk_baseline({c: 1e6 for c in full["ts_code"]}, curve=flat_curve())
        rough = _rough_candidates(det, base, SurgeConfig(), gi=30)
        # 主板/北交所被检测层过滤、ST 创业被 rough 排 → 候选只剩非 ST 创业+科创
        assert set(rough) == {"300111.SZ", "688333.SH"}

    def test_snapshot_full_persists_main_rows_detection_narrowed(self, tmp_path: Path) -> None:
        clock = {"t": datetime(2026, 7, 6, 10, 0, tzinfo=CST)}
        run_surge_watch(
            dry_run=True,
            baseline=mk_baseline({}, curve=flat_curve()),
            base_dir=tmp_path,
            is_trading_day_fn=lambda d: True,
            snapshot_fetcher=self._full_market,
            minute_fetcher=lambda c, d: pd.DataFrame(),
            notify_fn=lambda *a, **k: None,
            now_fn=lambda: clock["t"],
            sleep_fn=lambda s: clock.__setitem__("t", clock["t"] + timedelta(seconds=s)),
            max_ticks=1,
        )
        full_back = pd.read_parquet(tmp_path / "snapshot_full.parquet")
        assert "600519.SH" in set(full_back["ts_code"])   # 主板行进共享 feed
        assert "830001.BJ" in set(full_back["ts_code"])
        det_back = pd.read_parquet(tmp_path / "snapshot.parquet")
        assert set(det_back["ts_code"]) == {"300111.SZ", "688333.SH", "300777.SZ"}

    def test_full_market_fs_covers_all_segments(self) -> None:
        """U4：surge 复用 panorama 全市场 fs（沪深主板+创业+科创+北交所，不再是两段）。"""
        from rquant.panorama_data import _EM_SPOT_FS

        for seg in ("m:0+t:6", "m:0+t:80", "m:1+t:2", "m:1+t:23", "m:0+t:81+s:2048"):
            assert seg in _EM_SPOT_FS

    def test_snapshot_sina_fallback_when_em_blocked(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """东财两路全掐（RemoteDisconnected）→ 降级新浪，拿到全市场而非零快照饿死。"""
        import rquant.surge_watch as sw

        monkeypatch.delenv("RQUANT_PANORAMA_SOCKS", raising=False)

        def em_blocked(*a: object, **k: object) -> pd.DataFrame:
            raise ConnectionError("Remote end closed connection without response")

        sina_raw = pd.DataFrame({
            "代码": ["sh600519", "sz300111"],
            "名称": ["贵州茅台", "测试创业"],
            "最新价": [1700.0, 12.0],
            "今开": [1690.0, 11.5],
            "最高": [1710.0, 12.5],
            "最低": [1680.0, 11.0],
            "昨收": [1695.0, 11.8],
            "涨跌幅": [0.3, 1.7],
            "成交量": [100, 200],
            "成交额": [1e8, 2e6],
        })
        monkeypatch.setattr(sw, "_fetch_em_clist", em_blocked)
        monkeypatch.setattr("rquant.panorama_data._fetch_spot", lambda: sina_raw)

        out = sw.fetch_full_market_snapshot()
        assert out.attrs["route"] == "sina"
        assert not out.empty
        assert "600519.SH" in set(out["ts_code"])
        assert "limit_up_price" in out.columns  # add_limit_prices 已应用

    def test_snapshot_all_routes_fail_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import rquant.surge_watch as sw

        monkeypatch.delenv("RQUANT_PANORAMA_SOCKS", raising=False)
        monkeypatch.setattr(
            sw, "_fetch_em_clist", lambda *a, **k: (_ for _ in ()).throw(ConnectionError("x"))
        )
        monkeypatch.setattr(
            "rquant.panorama_data._fetch_spot",
            lambda: (_ for _ in ()).throw(ConnectionError("sina down")),
        )
        out = sw.fetch_full_market_snapshot()
        assert out.attrs["route"] == "none"
        assert out.empty
