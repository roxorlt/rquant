"""notify.messages 5 类场景的消息构造单测。"""

from __future__ import annotations

from datetime import date

import pytest


class TestPriceLevel:
    def test_basic_format(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "price_level",
            ts_code="002415.SZ",
            name="海康威视",
            level="40",
            trigger_price=12.30,
            body_upper=13.20,
            body_lower=11.80,
            level_40=12.36,
            level_30=12.22,
            level_20=12.08,
            stop_strong=11.80,
            stop_weak=11.52,
            pool="pool2",
            entry_date=date(2026, 4, 18),
            days_in_pool=5,
        )

        assert title == "002415.SZ 海康威视 40%档 ¥12.30"
        assert "# 002415.SZ | 40%档触发 | 现价 ¥12.30" in body
        assert "bodyTop: ¥13.20" in body
        assert "40档:    ¥12.36" in body
        assert "30档:    ¥12.22" in body
        assert "20档:    ¥12.08" in body
        assert "bodyBtm: ¥11.80" in body
        assert "强止：¥11.80 | 弱止：¥11.52" in body
        assert "pool2，入池 2026-04-18 第 5 日" in body

    def test_stop_strong_label(self) -> None:
        from rquant.notify.messages import build_message

        title, _ = build_message(
            "price_level",
            ts_code="600519.SH",
            name="贵州茅台",
            level="strong",
            trigger_price=1620.0,
            body_upper=1700.0, body_lower=1650.0,
            level_40=1680.0, level_30=1670.0, level_20=1660.0,
            stop_strong=1650.0, stop_weak=1620.0,
            pool="pool2",
            entry_date="2026-04-18",
            days_in_pool=3,
        )
        assert "强止档" in title


class TestPool2Exit:
    def test_kicked_and_held(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "pool2_exit",
            trade_date=date(2026, 4, 28),
            auto_kicked=[
                {
                    "ts_code": "002415.SZ", "name": "海康",
                    "close": 11.65, "threshold": 11.80,
                    "reason_label": "弱止",
                },
            ],
            expired_held=[
                {
                    "ts_code": "002846.SZ", "name": "英联",
                    "entry_date": date(2026, 4, 24),
                    "days_in_pool": 3,
                },
            ],
        )
        assert "踢出 1 / 待决策 1" in title
        assert "## 自动踢出（跌破止损）" in body
        assert "002415.SZ 海康 收盘 ¥11.65 < 弱止 ¥11.80" in body
        assert "## 待决策（超期已保留）" in body
        assert "002846.SZ" in body
        assert "rquant pool2 remove 002846.SZ" in body

    def test_all_empty(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "pool2_exit",
            trade_date=date(2026, 4, 28),
            auto_kicked=[],
            expired_held=[],
        )
        assert "踢出 0 / 待决策 0" in title
        assert "无退出事件" in body


class TestDailySummary:
    def test_basic(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "daily_summary",
            trade_date=date(2026, 4, 28),
            pool1_hits=[
                {"ts_code": "600519.SH", "name": "茅台", "close": 1685.0},
            ],
            pool2_active=[
                {
                    "ts_code": "002415.SZ", "name": "海康",
                    "entry_date": date(2026, 4, 18),
                    "days_in_pool": 5,
                },
            ],
            duration_seconds=28.4,
        )
        assert "P1 命中 1, P2 持仓 1" in title
        assert "Pool 1 候选" in body
        assert "600519.SH 茅台 收 ¥1685.00" in body
        assert "Pool 2 持仓" in body
        assert "耗时 28s" in body

    def test_empty_lists(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "daily_summary",
            trade_date=date(2026, 4, 28),
            pool1_hits=[],
            pool2_active=[],
            duration_seconds=10.0,
        )
        assert "P1 命中 0, P2 持仓 0" in title
        assert "（无）" in body


class TestError:
    def test_includes_component_and_traceback(self) -> None:
        from rquant.notify.messages import build_message

        try:
            raise ConnectionError("Tushare timeout")
        except ConnectionError as e:
            title, body = build_message("error", component="ingest_daily", exc=e)

        assert "ingest_daily" in title
        assert "失败" in title
        assert "组件：ingest_daily" in body
        assert "ConnectionError: Tushare timeout" in body
        assert "```" in body  # markdown code fence


class TestHeartbeat:
    def test_start_event(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "heartbeat",
            event="start",
            watchlist_count=26,
            pool1_count=13,
            pool2_count=13,
        )
        assert "▶ Monitor 启动 26 只" == title
        assert "pool2=13, pool1=13" in body

    def test_stop_event(self) -> None:
        from rquant.notify.messages import build_message

        title, body = build_message(
            "heartbeat",
            event="stop",
            triggers_summary={"40": 5, "30": 2, "strong": 1},
            auto_kicked_count=2,
        )
        assert "⏹ Monitor 结束: 触发 8 次" == title
        assert "40% 5" in body
        assert "30% 2" in body
        assert "强止 1" in body
        assert "Pool 2 自动踢出 2 只" in body


class TestRouterErrors:
    def test_unknown_scene_raises(self) -> None:
        from rquant.notify.messages import build_message

        with pytest.raises(ValueError, match="未知 scene"):
            build_message("nonexistent")
