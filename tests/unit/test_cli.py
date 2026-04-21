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
