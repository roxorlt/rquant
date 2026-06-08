"""CLI 入口单测 —— 仅验证 argparse 解析，不启动调度器。"""

from __future__ import annotations

import subprocess
import sys

from rquant.cli import build_parser


class TestBuildParser:
    def test_serve_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve"])
        assert args.command == "serve"
        assert args.hour == 17

    def test_serve_custom_hour(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve", "--hour", "16"])
        assert args.hour == 16

    def test_run_daily_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run-daily"])
        assert args.command == "run-daily"
        assert args.date is None
        assert args.preset is None

    def test_run_daily_with_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "run-daily", "--date", "2026-04-18", "--preset", "n-shape-pool1"
        ])
        assert args.date == "2026-04-18"
        assert args.preset == "n-shape-pool1"

    def test_no_command_returns_none(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None


class TestCLISmoke:
    def test_help_exits_0(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "rquant" in result.stdout

    def test_run_daily_help(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "run-daily", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "--date" in result.stdout


class TestMonitorParser:
    def test_default_interval(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor"])
        assert args.command == "monitor"
        assert args.interval == 5

    def test_custom_interval(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor", "--interval", "10"])
        assert args.interval == 10


class TestPool2Parser:
    def test_list(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["pool2", "list"])
        assert args.command == "pool2"
        assert args.pool2_action == "list"

    def test_remove(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["pool2", "remove", "002415.SZ"])
        assert args.pool2_action == "remove"
        assert args.ts_code == "002415.SZ"


class TestNotifyTestParser:
    def test_notify_test_registered(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["notify-test"])
        assert args.command == "notify-test"


class TestCmdNotifyTest:
    def test_no_keys_returns_1(self, monkeypatch) -> None:
        from rquant.cli import cmd_notify_test
        from unittest.mock import MagicMock

        # Empty key_list -> should fail fast
        import rquant.config as cfg_mod
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "")

        rc = cmd_notify_test(MagicMock())
        assert rc == 1

    def test_pushes_and_returns_0_on_success(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch
        from rquant.cli import cmd_notify_test

        import rquant.config as cfg_mod
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1,k2")

        with patch("rquant.notify.client.requests.post") as mock_post:
            mock_post.return_value = MagicMock(json=lambda: {"code": 0})
            rc = cmd_notify_test(MagicMock())

        assert rc == 0
        assert mock_post.call_count == 2  # 两个 key 都推

    def test_partial_failure_returns_0_when_any_success(
        self, monkeypatch
    ) -> None:
        from unittest.mock import MagicMock, patch
        from rquant.cli import cmd_notify_test

        import rquant.config as cfg_mod
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1,k2")

        responses = [
            MagicMock(json=lambda: {"code": 0}),
            MagicMock(json=lambda: {"code": 1, "error": "bad key"}),
        ]
        with patch("rquant.notify.client.requests.post", side_effect=responses):
            rc = cmd_notify_test(MagicMock())
        assert rc == 0

    def test_all_fail_returns_1(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch
        from rquant.cli import cmd_notify_test

        import rquant.config as cfg_mod
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1")

        with patch("rquant.notify.client.requests.post") as mock_post:
            mock_post.return_value = MagicMock(json=lambda: {"code": 1, "error": "x"})
            rc = cmd_notify_test(MagicMock())
        assert rc == 1


class TestMainErrorReporting:
    def test_main_catches_run_daily_error_and_notifies(self, monkeypatch) -> None:
        from unittest.mock import patch
        from rquant.cli import main

        # Force run-daily to raise
        def boom(_args):
            raise ValueError("test boom")

        monkeypatch.setattr("rquant.cli.cmd_run_daily", boom)

        with patch("sys.argv", ["rquant", "run-daily", "--no-ingest"]):
            with patch("rquant.notify.notify") as mock_notify:
                rc = main()

        assert rc == 1
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args.kwargs
        assert mock_notify.call_args.args[0] == "error"
        assert call_kwargs["component"] == "cli:run-daily"
        assert isinstance(call_kwargs["exc"], ValueError)

    def test_main_does_not_wrap_serve(self, monkeypatch) -> None:
        """serve 内部已自处理异常，main 不再加 try/except。"""
        from unittest.mock import patch
        from rquant.cli import main

        def boom(_args):
            raise ValueError("inner")

        monkeypatch.setattr("rquant.cli.cmd_serve", boom)
        with patch("sys.argv", ["rquant", "serve"]):
            with patch("rquant.notify.notify") as mock_notify:
                # main 让 serve 异常直接冒出来
                import pytest
                with pytest.raises(ValueError):
                    main()
                mock_notify.assert_not_called()


class TestIngestWithRetry:
    """_ingest_with_retry 的网络异常重试（6/4 真实事故：tushare ReadTimeout）。"""

    def test_network_error_retries_then_succeeds(self, monkeypatch) -> None:
        from unittest.mock import patch

        import requests

        from rquant import cli

        # 前两次抛 ReadTimeout，第三次成功返回 5000 行
        calls = []

        def flaky(_date):
            calls.append(1)
            if len(calls) < 3:
                raise requests.exceptions.ReadTimeout("boom")
            return 5000

        monkeypatch.setattr("rquant.ingest.ingest_daily", flaky)
        with patch("rquant.cli.time.sleep"):  # 跳过真实 sleep
            result = cli._ingest_with_retry("2026-06-04")

        assert result == 5000
        assert len(calls) == 3

    def test_network_error_exhausted_reraises(self, monkeypatch) -> None:
        from unittest.mock import patch

        import pytest
        import requests

        from rquant import cli

        def always_timeout(_date):
            raise requests.exceptions.ReadTimeout("persistent")

        monkeypatch.setattr("rquant.ingest.ingest_daily", always_timeout)
        with patch("rquant.cli.time.sleep"):
            with pytest.raises(requests.exceptions.ReadTimeout):
                cli._ingest_with_retry("2026-06-04")

    def test_data_not_ready_retries_then_zero(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant import cli

        # 始终返回 0（数据未就绪），重试用尽后返回 0（不抛）
        monkeypatch.setattr("rquant.ingest.ingest_daily", lambda _d: 0)
        with patch("rquant.cli.time.sleep"):
            result = cli._ingest_with_retry("2026-06-04")

        assert result == 0


class TestIngestRetryBusinessError:
    """_ingest_with_retry 也重试 tushare 服务端业务错误（裸 Exception，非 RequestException）。"""

    def test_business_exception_retries_then_succeeds(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant import cli

        calls = []

        def flaky(_date):
            calls.append(1)
            if len(calls) < 2:
                # tushare 客户端业务错误抛裸 Exception（如限频/接口临时故障）
                raise Exception("抱歉，您每分钟最多访问该接口600次")
            return 5000

        monkeypatch.setattr("rquant.ingest.ingest_daily", flaky)
        with patch("rquant.cli.time.sleep"):
            result = cli._ingest_with_retry("2026-06-04")

        assert result == 5000
        assert len(calls) == 2

    def test_business_exception_exhausted_reraises(self, monkeypatch) -> None:
        from unittest.mock import patch

        import pytest

        from rquant import cli

        def always_fail(_date):
            raise Exception("接口下线")

        monkeypatch.setattr("rquant.ingest.ingest_daily", always_fail)
        with patch("rquant.cli.time.sleep"):
            with pytest.raises(Exception, match="接口下线"):
                cli._ingest_with_retry("2026-06-04")
