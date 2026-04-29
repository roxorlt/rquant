"""notify.api notify(scene, **kwargs) 入口路由 + 开关单测。"""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture()
def mock_settings():
    """提供可调整的 settings mock。"""
    with patch("rquant.notify.api.settings") as m:
        m.notify_enabled = True
        m.notify_price_level = True
        m.notify_pool2_exit = True
        m.notify_daily_summary = True
        m.notify_error = True
        m.notify_heartbeat = True
        m.pushdeer_key_list = ["k1"]
        m.pushdeer_endpoint = "https://api2.pushdeer.com/message/push"
        m.pushplus_token_list = ["pp1"]
        m.pushplus_endpoint = "http://www.pushplus.plus/send"
        yield m


class TestNotifyDispatch:
    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_calls_both_channels(
        self, MockPushDeer, MockPushPlus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        notify(
            "heartbeat",
            event="start",
            watchlist_count=10,
            pool1_count=5,
            pool2_count=5,
        )

        MockPushDeer.assert_called_once_with(
            keys=["k1"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        MockPushPlus.assert_called_once_with(
            tokens=["pp1"],
            endpoint="http://www.pushplus.plus/send",
        )
        # 双通道都推
        MockPushDeer.return_value.push.assert_called_once()
        MockPushPlus.return_value.push.assert_called_once()

        args, _ = MockPushDeer.return_value.push.call_args
        assert args[0] == "▶ Monitor 启动 10 只"

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_global_disabled_skips(
        self, MockPushDeer, MockPushPlus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        mock_settings.notify_enabled = False
        notify("heartbeat", event="start")
        MockPushDeer.assert_not_called()
        MockPushPlus.assert_not_called()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_per_scene_disabled_skips(
        self, MockPushDeer, MockPushPlus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        mock_settings.notify_heartbeat = False
        notify("heartbeat", event="start")
        MockPushDeer.assert_not_called()
        MockPushPlus.assert_not_called()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_message_build_failure_logged_not_raised(
        self, MockPushDeer, MockPushPlus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        notify("heartbeat")  # missing required arg
        MockPushDeer.assert_not_called()
        MockPushPlus.assert_not_called()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_push_exception_swallowed_independent(
        self, MockPushDeer, MockPushPlus, mock_settings
    ) -> None:
        """单通道失败不影响另一通道。"""
        from rquant.notify.api import notify

        MockPushDeer.return_value.push.side_effect = RuntimeError("pd boom")
        # Should not raise; PushPlus 仍被调用
        notify(
            "heartbeat",
            event="start",
            watchlist_count=1,
            pool1_count=1,
            pool2_count=0,
        )
        MockPushPlus.return_value.push.assert_called_once()
