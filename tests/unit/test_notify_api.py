"""notify.api notify(scene, **kwargs) 入口路由 + 开关单测。"""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture()
def mock_settings(tmp_path):
    """提供可调整的 settings mock；同时 stub 日志写入避免污染真实 JSONL。"""
    with patch("rquant.notify.api.settings") as m, patch("rquant.notify.api._log_notification"):
        m.notify_enabled = True
        m.notify_price_level = True
        m.notify_pool2_exit = True
        m.notify_daily_summary = True
        m.notify_error = True
        m.notify_heartbeat = True
        m.pushdeer_key_list = ["k1"]
        m.pushdeer_recipient_id_list = ["admin"]
        m.pushdeer_endpoint = "https://api2.pushdeer.com/message/push"
        m.pushplus_token_list = ["pp1"]
        m.pushplus_recipient_id_list = ["collaborator"]
        m.pushplus_endpoint = "https://www.pushplus.plus/send"
        m.notification_state_path_resolved = tmp_path / "notification-state.sqlite3"
        m.notification_state_busy_timeout_ms = 5_000
        m.notify_error_cooldown_seconds = 1_800
        yield m


class TestNotifyDispatch:
    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_calls_both_channels(self, mock_pushdeer, mock_pushplus, mock_settings) -> None:
        from rquant.notify.api import notify

        notify(
            "heartbeat",
            event="start",
            watchlist_count=10,
            pool1_count=5,
            pool2_count=5,
        )

        mock_pushdeer.assert_called_once_with(
            keys=["k1"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        mock_pushplus.assert_called_once_with(
            tokens=["pp1"],
            endpoint="https://www.pushplus.plus/send",
        )
        # 双通道都推
        mock_pushdeer.return_value.push.assert_called_once()
        mock_pushplus.return_value.push.assert_called_once()

        args, _ = mock_pushdeer.return_value.push.call_args
        assert args[0] == "▶ Monitor 启动 10 只"

    @patch("rquant.notify.api._log_notification")
    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_logs_stable_recipient_ids_and_normalized_errors_only(
        self,
        mock_pushdeer,
        mock_pushplus,
        mock_log,
        mock_settings,
    ) -> None:
        from rquant.notify.api import notify

        mock_settings.pushdeer_key_list = ["PDU_private_iphone", "PDU_private_mac"]
        mock_settings.pushdeer_recipient_id_list = ["admin", "admin"]
        mock_pushdeer.return_value.push.return_value = [
            (False, "provider said private detail"),
            (False, "transport_error"),
        ]
        mock_pushplus.return_value.push.return_value = []

        notify(
            "heartbeat",
            event="start",
            watchlist_count=1,
            pool1_count=1,
            pool2_count=0,
        )

        pushdeer_logs = [
            call.args for call in mock_log.call_args_list if call.args[1] == "pushdeer"
        ]
        assert [args[2] for args in pushdeer_logs] == ["admin", "admin"]
        assert [args[4] for args in pushdeer_logs] == [
            "delivery_failed",
            "transport_error",
        ]
        rendered = repr(mock_log.call_args_list)
        assert "PDU_private" not in rendered
        assert "provider said private detail" not in rendered

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_global_disabled_skips(self, mock_pushdeer, mock_pushplus, mock_settings) -> None:
        from rquant.notify.api import notify

        mock_settings.notify_enabled = False
        notify("heartbeat", event="start")
        mock_pushdeer.assert_not_called()
        mock_pushplus.assert_not_called()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_per_scene_disabled_skips(self, mock_pushdeer, mock_pushplus, mock_settings) -> None:
        from rquant.notify.api import notify

        mock_settings.notify_heartbeat = False
        notify("heartbeat", event="start")
        mock_pushdeer.assert_not_called()
        mock_pushplus.assert_not_called()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_message_build_failure_logged_not_raised(
        self, mock_pushdeer, mock_pushplus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        notify("heartbeat")  # missing required arg
        mock_pushdeer.assert_not_called()
        mock_pushplus.assert_not_called()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_push_exception_swallowed_independent(
        self, mock_pushdeer, mock_pushplus, mock_settings
    ) -> None:
        """单通道失败不影响另一通道。"""
        from rquant.notify.api import notify

        mock_pushdeer.return_value.push.side_effect = RuntimeError("pd boom")
        # Should not raise; PushPlus 仍被调用
        notify(
            "heartbeat",
            event="start",
            watchlist_count=1,
            pool1_count=1,
            pool2_count=0,
        )
        mock_pushplus.return_value.push.assert_called_once()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_repeated_error_is_suppressed_across_calls(
        self, mock_pushdeer, mock_pushplus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        mock_pushdeer.return_value.push.return_value = [(True, None)]
        mock_pushplus.return_value.push.return_value = [(True, None)]

        notify("error", component="cli:monitor", exc=RuntimeError("schema mismatch"))
        notify("error", component="cli:monitor", exc=RuntimeError("schema mismatch"))

        mock_pushdeer.return_value.push.assert_called_once()
        mock_pushplus.return_value.push.assert_called_once()

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_failed_error_delivery_releases_suppression_gate(
        self, mock_pushdeer, mock_pushplus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        mock_pushdeer.return_value.push.return_value = [(False, "down")]
        mock_pushplus.return_value.push.return_value = [(False, "down")]

        notify("error", component="cli:monitor", exc=RuntimeError("schema mismatch"))
        notify("error", component="cli:monitor", exc=RuntimeError("schema mismatch"))

        assert mock_pushdeer.return_value.push.call_count == 2
        assert mock_pushplus.return_value.push.call_count == 2

    @patch("rquant.notify.api.PushPlusClient")
    @patch("rquant.notify.api.PushDeerClient")
    def test_gate_failure_suppresses_error_instead_of_failing_open(
        self, mock_pushdeer, mock_pushplus, mock_settings
    ) -> None:
        from rquant.notify.api import notify

        with patch(
            "rquant.notify.api.NotificationGate.claim",
            side_effect=OSError("all gates unavailable"),
        ):
            notify("error", component="cli:monitor", exc=RuntimeError("schema mismatch"))

        mock_pushdeer.return_value.push.assert_not_called()
        mock_pushplus.return_value.push.assert_not_called()
