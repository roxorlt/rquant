"""每日健康摘要单测：systemd 状态解析 + watchdog log 解析 + 报文构造。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from rquant.health import (
    ServiceSnapshot,
    _parse_systemd_ts,
    build_daily_report,
    get_service_snapshot,
    read_watchdog_log,
)


class TestParseSystemdTs:
    def test_normal(self):
        dt = _parse_systemd_ts("Thu 2026-04-30 11:31:03 CST")
        assert dt is not None
        assert dt.year == 2026
        assert dt.hour == 11

    def test_empty_returns_none(self):
        assert _parse_systemd_ts("") is None

    def test_zero_returns_none(self):
        assert _parse_systemd_ts("0") is None

    def test_na_returns_none(self):
        assert _parse_systemd_ts("n/a") is None

    def test_garbage_returns_none(self):
        assert _parse_systemd_ts("not a timestamp") is None


class TestGetServiceSnapshot:
    @patch("rquant.health.subprocess.run")
    def test_parses_active_running(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=(
                "ActiveState=active\n"
                "SubState=running\n"
                "ExecMainStartTimestamp=Thu 2026-04-30 09:25:00 CST\n"
                "ExecMainExitTimestamp=\n"
                "ExecMainStatus=0\n"
            ),
            returncode=0,
        )
        snap = get_service_snapshot("rquant-monitor.service", today=date(2026, 4, 30))
        assert snap.active_state == "active"
        assert snap.start_today is not None
        assert snap.start_today.hour == 9
        assert snap.exit_today is None
        assert snap.duration_sec is None  # exit 没值就没 duration

    @patch("rquant.health.subprocess.run")
    def test_parses_completed_today(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=(
                "ActiveState=inactive\n"
                "SubState=dead\n"
                "ExecMainStartTimestamp=Thu 2026-04-30 09:25:00 CST\n"
                "ExecMainExitTimestamp=Thu 2026-04-30 15:00:30 CST\n"
                "ExecMainStatus=0\n"
            ),
            returncode=0,
        )
        snap = get_service_snapshot("rquant-monitor.service", today=date(2026, 4, 30))
        assert snap.active_state == "inactive"
        assert snap.exit_status == 0
        assert snap.duration_sec is not None
        assert snap.duration_sec >= 5 * 3600  # ≥ 5h

    @patch("rquant.health.subprocess.run")
    def test_yesterday_timestamps_are_none(self, mock_run):
        # last run was yesterday
        mock_run.return_value = MagicMock(
            stdout=(
                "ActiveState=inactive\n"
                "SubState=dead\n"
                "ExecMainStartTimestamp=Wed 2026-04-29 09:25:00 CST\n"
                "ExecMainExitTimestamp=Wed 2026-04-29 15:00:00 CST\n"
                "ExecMainStatus=0\n"
            ),
            returncode=0,
        )
        snap = get_service_snapshot("rquant-monitor.service", today=date(2026, 4, 30))
        assert snap.start_today is None
        assert snap.exit_today is None
        # 但 active_state 还是会读到（it's the latest known state）
        assert snap.active_state == "inactive"

    @patch("rquant.health.subprocess.run", side_effect=Exception("connection lost"))
    def test_systemctl_failure_returns_unknown(self, _mock):
        snap = get_service_snapshot("rquant-monitor.service")
        assert snap.active_state == "unknown"
        assert snap.start_today is None


class TestReadWatchdogLog:
    def test_counts_tags(self, tmp_path: Path):
        d = tmp_path / "logs"
        d.mkdir()
        (d / "watchdog-2026-04-30.log").write_text(
            "2026-04-30T09:30:01+08:00 active\n"
            "2026-04-30T09:32:01+08:00 active\n"
            "2026-04-30T09:34:01+08:00 skip-clean-exit\n"
            "2026-04-30T09:36:01+08:00 alert-restart\n"
            "2026-04-30T09:38:01+08:00 alert-restart\n"
        )
        counts = read_watchdog_log(d, today=date(2026, 4, 30))
        assert counts == {
            "active": 2,
            "skip-clean-exit": 1,
            "alert-restart": 2,
        }

    def test_missing_file_returns_empty(self, tmp_path: Path):
        d = tmp_path / "logs"
        d.mkdir()
        counts = read_watchdog_log(d, today=date(2026, 4, 30))
        assert counts == {}

    def test_only_today_log_used(self, tmp_path: Path):
        d = tmp_path / "logs"
        d.mkdir()
        (d / "watchdog-2026-04-29.log").write_text(
            "2026-04-29T09:30:01+08:00 alert-restart\n"
        )
        (d / "watchdog-2026-04-30.log").write_text(
            "2026-04-30T09:30:01+08:00 active\n"
        )
        counts = read_watchdog_log(d, today=date(2026, 4, 30))
        assert counts == {"active": 1}


class TestBuildDailyReport:
    def _empty_business(self) -> dict[str, int]:
        return {"price_level_events": 0}

    def test_holiday_clean_run(self):
        """节假日：monitor 几秒退、watchdog 全 skip、daily 未触发——全绿。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service",
            active_state="inactive", sub_state="dead",
            start_today=datetime(2026, 5, 1, 9, 25, 0),
            exit_today=datetime(2026, 5, 1, 9, 25, 5),
            exit_status=0,
            duration_sec=5,
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        watchdog = {"skip-clean-exit": 60, "out-of-window": 14}
        subject, body = build_daily_report(
            date(2026, 5, 1), is_trading_day_flag=False,
            monitor=monitor, daily=daily,
            watchdog_counts=watchdog, business=self._empty_business(),
        )
        assert "非交易日" in subject
        assert "✅ monitor" in body
        assert "✅ watchdog" in body
        assert "skip=60" in body
        # 不应把 out-of-window 算进 in-window 总数
        assert "交易时段 60" in body

    def test_holiday_alert_storm(self):
        """节假日告警轰炸 bug 复现：watchdog alert > 0 就该红色警告。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service",
            active_state="inactive", sub_state="dead",
            start_today=datetime(2026, 5, 1, 9, 25, 0),
            exit_today=datetime(2026, 5, 1, 9, 25, 5),
            exit_status=0, duration_sec=5,
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        watchdog = {"alert-restart": 60}
        _, body = build_daily_report(
            date(2026, 5, 1), is_trading_day_flag=False,
            monitor=monitor, daily=daily,
            watchdog_counts=watchdog, business=self._empty_business(),
        )
        assert "⚠️ watchdog" in body
        assert "alert 60" in body or "**alert 60" in body
        # 交易时段总数 = active + skip + alert
        assert "交易时段 60" in body

    def test_trading_day_full_run(self):
        """交易日：monitor 跑足 5h 跨午休、watchdog 60 次 active、daily 17:00 未到——日报正常。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service",
            active_state="inactive", sub_state="dead",
            start_today=datetime(2026, 5, 6, 9, 25, 0),
            exit_today=datetime(2026, 5, 6, 15, 0, 30),
            exit_status=0,
            duration_sec=5 * 3600 + 35 * 60 + 30,  # 5h35m30s
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        watchdog = {"active": 60}
        _, body = build_daily_report(
            date(2026, 5, 6), is_trading_day_flag=True,
            monitor=monitor, daily=daily,
            watchdog_counts=watchdog,
            business={"price_level_events": 12, "screen_n-shape-pool1": 0},
        )
        assert "跑足" in body
        assert "✅ watchdog" in body
        assert "⏰ daily" in body  # 17:00 还未触发
        assert "12 条" in body

    def test_trading_day_lunch_break_bug(self):
        """跨午休 bug 复发：monitor 11:31 就退了，时长 < 5h。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service",
            active_state="inactive", sub_state="dead",
            start_today=datetime(2026, 5, 6, 9, 25, 0),
            exit_today=datetime(2026, 5, 6, 11, 31, 0),
            exit_status=0,
            duration_sec=2 * 3600 + 6 * 60,  # 2h6m
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        _, body = build_daily_report(
            date(2026, 5, 6), is_trading_day_flag=True,
            monitor=monitor, daily=daily,
            watchdog_counts={"active": 60, "out-of-window": 14},
            business=self._empty_business(),
        )
        assert "⚠️ monitor" in body
        assert "跨午休 bug 复发" in body

    def test_trading_day_watchdog_zero_triggers(self):
        """交易日 watchdog 一次都没触发——timer 出问题了。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service",
            active_state="inactive", sub_state="dead",
            start_today=datetime(2026, 5, 6, 9, 25, 0),
            exit_today=datetime(2026, 5, 6, 15, 0, 30),
            exit_status=0,
            duration_sec=5 * 3600 + 35 * 60,
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        _, body = build_daily_report(
            date(2026, 5, 6), is_trading_day_flag=True,
            monitor=monitor, daily=daily,
            watchdog_counts={},
            business=self._empty_business(),
        )
        assert "❌ watchdog" in body
        assert "交易时段 0" in body

    def test_daily_failed(self):
        """daily 失败：明确显示 status code。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="failed", sub_state="failed",
            start_today=datetime(2026, 4, 30, 17, 0, 0),
            exit_today=datetime(2026, 4, 30, 17, 0, 30),
            exit_status=1, duration_sec=30,
        )
        _, body = build_daily_report(
            date(2026, 4, 30), is_trading_day_flag=True,
            monitor=monitor, daily=daily,
            watchdog_counts={},
            business=self._empty_business(),
        )
        assert "❌ daily: status=1" in body

    def test_holiday_only_out_of_window(self):
        """节假日 watchdog timer 触发但全在 09:30 前自检退（极端 edge case）。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service", active_state="inactive", sub_state="dead",
            start_today=datetime(2026, 5, 1, 9, 25, 0),
            exit_today=datetime(2026, 5, 1, 9, 25, 5),
            exit_status=0, duration_sec=5,
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        # 只有 out-of-window，没有 in-window 触发（不该出现，但测兜底）
        _, body = build_daily_report(
            date(2026, 5, 1), is_trading_day_flag=False,
            monitor=monitor, daily=daily,
            watchdog_counts={"out-of-window": 14},
            business=self._empty_business(),
        )
        assert "ℹ️ watchdog" in body
        assert "交易时段 0" in body
        assert "盘外 14 次" in body

    def test_monitor_never_triggered(self):
        """monitor 今天根本没触发——timer 出问题。"""
        monitor = ServiceSnapshot(
            unit="rquant-monitor.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        daily = ServiceSnapshot(
            unit="rquant-daily.service", active_state="inactive", sub_state="dead",
            start_today=None, exit_today=None, exit_status=None, duration_sec=None,
        )
        _, body = build_daily_report(
            date(2026, 5, 6), is_trading_day_flag=True,
            monitor=monitor, daily=daily,
            watchdog_counts={},
            business=self._empty_business(),
        )
        assert "❌ monitor" in body
        assert "未触发" in body
