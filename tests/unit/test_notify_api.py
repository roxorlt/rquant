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
        yield m


class TestNotifyDispatch:
    @patch("rquant.notify.api.PushDeerClient")
    def test_calls_client_push(self, MockClient, mock_settings) -> None:
        from rquant.notify.api import notify

        notify(
            "heartbeat",
            event="start",
            watchlist_count=10,
            pool1_count=5,
            pool2_count=5,
        )

        MockClient.assert_called_once_with(
            keys=["k1"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        instance = MockClient.return_value
        instance.push.assert_called_once()

        args, _ = instance.push.call_args
        assert args[0] == "▶ Monitor 启动 10 只"

    @patch("rquant.notify.api.PushDeerClient")
    def test_global_disabled_skips(self, MockClient, mock_settings) -> None:
        from rquant.notify.api import notify

        mock_settings.notify_enabled = False
        notify("heartbeat", event="start")
        MockClient.assert_not_called()

    @patch("rquant.notify.api.PushDeerClient")
    def test_per_scene_disabled_skips(self, MockClient, mock_settings) -> None:
        from rquant.notify.api import notify

        mock_settings.notify_heartbeat = False
        notify("heartbeat", event="start")
        MockClient.assert_not_called()

    @patch("rquant.notify.api.PushDeerClient")
    def test_message_build_failure_logged_not_raised(
        self, MockClient, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        # heartbeat without event= raises in builder
        notify("heartbeat")  # missing required arg
        MockClient.assert_not_called()

    @patch("rquant.notify.api.PushDeerClient")
    def test_push_exception_swallowed(self, MockClient, mock_settings) -> None:
        from rquant.notify.api import notify

        MockClient.return_value.push.side_effect = RuntimeError("boom")
        # Should not raise
        notify(
            "heartbeat",
            event="start",
            watchlist_count=1,
            pool1_count=1,
            pool2_count=0,
        )
