"""pulse_watch 单测：滑窗四规则 / 冷却 / 预热静默 / seed 续算 / jsonl IO。全离线。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import pandas as pd
import pytest

from rquant.pulse_watch import (
    PulseAlert,
    PulseAnomalyWatcher,
    PulseConfig,
    PulsePoint,
    PulseSession,
    alerts_path,
    append_jsonl,
    pulse_path,
    read_pulse_points,
)

CST = timezone(timedelta(hours=8))


def _pt(t: str, *, limit_up: int = 20, limit_down: int = 2, broken: int = 1,
        up_ratio: float | None = 50.0) -> PulsePoint:
    return PulsePoint(
        t=t, limit_up=limit_up, limit_down=limit_down, broken=broken,
        up=2600, down=2400, up_ratio_pct=up_ratio, total=5400,
    )


def _now(t: str) -> datetime:
    hh, mm = t.split(":")
    return datetime(2026, 7, 29, int(hh), int(mm), tzinfo=CST)


def _feed_flat(w: PulseAnomalyWatcher, start_minute: int, n: int, **kw) -> None:
    """连续喂 n 个平稳分钟（09:30 起算 start_minute 偏移）。"""
    for i in range(n):
        m = 9 * 60 + 30 + start_minute + i
        t = f"{m // 60:02d}:{m % 60:02d}"
        w.observe(_pt(t, **kw), _now(t))


class TestWatcherRules:
    def test_warmup_no_alert_before_window(self) -> None:
        w = PulseAnomalyWatcher()
        # 前 10 分钟内即使涨停暴增也不告警（无 10 分钟前参照点）
        for i, n in enumerate([20, 25, 30, 35, 40]):
            t = f"09:{30 + i:02d}"
            assert w.observe(_pt(t, limit_up=n), _now(t)) == []

    def test_limit_up_surge_triggers_and_cools_down(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)  # 09:30..09:40 平稳，建立参照
        alerts = w.observe(_pt("09:41", limit_up=26), _now("09:41"))  # 20→26 净增6≥5
        assert len(alerts) == 1
        a = alerts[0]
        assert a.kind == "limit_up_surge" and a.kind_label == "涨停潮"
        assert a.before == 20 and a.after == 26
        assert "20 → 26" in a.message and "+6" in a.message
        # 冷却 30 分钟内同类不再触发
        assert w.observe(_pt("09:45", limit_up=40), _now("09:45")) == []

    def test_broken_and_limit_down_surge(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)
        alerts = w.observe(
            _pt("09:41", broken=4, limit_down=5), _now("09:41")
        )  # 炸板 1→4 (+3) 且跌停 2→5 (+3)
        kinds = {a.kind for a in alerts}
        assert kinds == {"broken_surge", "limit_down_surge"}

    def test_ratio_jump_needs_both_values(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11, up_ratio=None)  # 参照点无占比 → 不触发
        assert w.observe(_pt("09:41", up_ratio=70.0), _now("09:41")) == []
        w2 = PulseAnomalyWatcher()
        _feed_flat(w2, 0, 11, up_ratio=48.0)
        alerts = w2.observe(_pt("09:41", up_ratio=64.0), _now("09:41"))  # +16pct≥15
        assert [a.kind for a in alerts] == ["ratio_jump"]
        assert "48% → 64%" in alerts[0].message

    def test_ratio_jump_downward_triggers(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11, up_ratio=48.0)
        alerts = w.observe(_pt("09:41", up_ratio=30.0), _now("09:41"))  # -18pct≥15
        assert [a.kind for a in alerts] == ["ratio_jump"]
        assert "48% → 30%" in alerts[0].message

    def test_same_minute_duplicate_ignored(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)
        w.observe(_pt("09:41", limit_up=26), _now("09:41"))
        # 同分钟重复喂（rerun/重试场景）不重复记录也不再告警
        assert w.observe(_pt("09:41", limit_up=30), _now("09:41")) == []

    def test_lunch_gap_uses_trading_minutes(self) -> None:
        w = PulseAnomalyWatcher()
        # 11:20..11:30 平稳（11 点），13:01 相对 11:30 只隔 1 个交易分钟 →
        # 参照点应是 11:21（10 交易分钟前），而非墙钟 12:51
        for i in range(11):
            t = f"11:{20 + i:02d}"
            w.observe(_pt(t), _now(t))
        alerts = w.observe(_pt("13:01", limit_up=26), _now("13:01"))
        assert len(alerts) == 1  # 11:21 的 20 → 13:01 的 26

    def test_seed_restores_history_and_cooldown(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)
        assert len(w.observe(_pt("09:41", limit_up=26), _now("09:41"))) == 1
        # 重启：用同样的点序列 seed 新实例（含已触发过告警的分钟）
        points = [_pt(f"09:{30 + i:02d}") for i in range(11)] + [_pt("09:41", limit_up=26)]
        w2 = PulseAnomalyWatcher()
        assert w2.seed(points) == 12
        # seed 静默回放已登记冷却：紧接着的分钟不重复告警
        assert w2.observe(_pt("09:42", limit_up=27), _now("09:42")) == []


class TestJsonlIO:
    def test_paths(self, tmp_path: Path) -> None:
        d = date(2026, 7, 29)
        assert pulse_path(tmp_path, d).name == "pulse-2026-07-29.jsonl"
        assert alerts_path(tmp_path, d).name == "pulse_alerts-2026-07-29.jsonl"

    def test_append_and_read_roundtrip(self, tmp_path: Path) -> None:
        p = pulse_path(tmp_path / "sub", date(2026, 7, 29))  # 自动建目录
        append_jsonl(p, _pt("09:31").model_dump())
        append_jsonl(p, _pt("09:32", limit_up=21).model_dump())
        p.write_text(p.read_text(encoding="utf-8") + "not json\n", encoding="utf-8")
        pts = read_pulse_points(p)  # 坏行跳过
        assert [x.t for x in pts] == ["09:31", "09:32"]
        assert pts[1].limit_up == 21

    def test_read_missing_file_empty(self, tmp_path: Path) -> None:
        assert read_pulse_points(tmp_path / "nope.jsonl") == []

    def test_read_corrupt_bytes_returns_empty_without_raising(self, tmp_path: Path) -> None:
        p = pulse_path(tmp_path, date(2026, 7, 29))
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"\xff\xfe garbage")  # 非 UTF-8 → decode 会抛，read_pulse_points 须吞掉
        assert read_pulse_points(p) == []

    def test_session_construction_survives_corrupt_pulse_file(self, tmp_path: Path) -> None:
        day = date(2026, 7, 29)
        p = pulse_path(tmp_path, day)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"\xff\xfe garbage")
        s = PulseSession(tmp_path, day, notify_fn=lambda *a, **k: None)
        assert s.watcher._points == []  # seed 降级为空滑窗，未抛异常


def _pulse_snap(limit_up: int, *, broken: int = 0, total: int = 100) -> pd.DataFrame:
    """构造能让 compute_market_pulse 得出指定涨停/炸板数的最小快照。"""
    rows = []
    for i in range(total):
        pre = 10.0
        cap = 11.0
        if i < limit_up:
            price, high = 11.0, 11.0          # 涨停
        elif i < limit_up + broken:
            price, high = 10.5, 11.0          # 触板回落 = 炸板
        else:
            price, high = 10.2, 10.3          # 普通上涨
        rows.append({
            "ts_code": f"30{i:04d}.SZ", "price": price, "high": high,
            "pre_close": pre, "limit_up_price": cap, "limit_down_price": 9.0,
        })
    return pd.DataFrame(rows)


class TestPulseSession:
    def test_records_points_and_pushes_alert(self, tmp_path: Path) -> None:
        calls: list[tuple] = []
        day = date(2026, 7, 29)
        s = PulseSession(tmp_path, day, notify_fn=lambda scene, **kw: calls.append((scene, kw)))
        for i in range(11):  # 09:30..09:40 平稳
            s.on_snapshot(_pulse_snap(20), _now(f"09:{30 + i:02d}"))
        alerts = s.on_snapshot(_pulse_snap(26), _now("09:41"))  # 涨停 20→26
        assert len(alerts) == 1
        assert pulse_path(tmp_path, day).read_text(encoding="utf-8").count("\n") == 12
        assert alerts_path(tmp_path, day).exists()
        assert calls and calls[0][0] == "pulse_alert"
        assert "脉搏异动 09:41 涨停潮" == calls[0][1]["title"]
        assert "当前 涨停 26" in calls[0][1]["body"]

    def test_dry_run_prints_instead_of_notify(self, tmp_path: Path, capsys) -> None:
        calls: list[tuple] = []
        s = PulseSession(tmp_path, date(2026, 7, 29), dry_run=True,
                         notify_fn=lambda scene, **kw: calls.append((scene, kw)))
        for i in range(11):
            s.on_snapshot(_pulse_snap(20), _now(f"09:{30 + i:02d}"))
        s.on_snapshot(_pulse_snap(26), _now("09:41"))
        assert calls == []
        assert "DRY-RUN" in capsys.readouterr().out

    def test_seed_from_existing_file(self, tmp_path: Path) -> None:
        day = date(2026, 7, 29)
        s1 = PulseSession(tmp_path, day, notify_fn=lambda *a, **k: None)
        for i in range(11):
            s1.on_snapshot(_pulse_snap(20), _now(f"09:{30 + i:02d}"))
        s1.on_snapshot(_pulse_snap(26), _now("09:41"))
        # 重启：新 session 从文件 seed，冷却生效不重复推
        calls: list[tuple] = []
        s2 = PulseSession(tmp_path, day, notify_fn=lambda scene, **kw: calls.append(scene))
        assert s2.on_snapshot(_pulse_snap(27), _now("09:42")) == []
        assert calls == []

    def test_empty_snapshot_skipped(self, tmp_path: Path) -> None:
        s = PulseSession(tmp_path, date(2026, 7, 29), notify_fn=lambda *a, **k: None)
        assert s.on_snapshot(pd.DataFrame(), _now("09:31")) == []
        assert not pulse_path(tmp_path, date(2026, 7, 29)).exists()
