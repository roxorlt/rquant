"""CLI 入口单测 —— 仅验证 argparse 解析，不启动调度器。"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from rquant.cli import build_parser
from rquant.lab_daemon import ensure_private_directory as _real_ensure_private_directory
from rquant.lab_daemon import verify_lab_runtime_prepared as _real_verify_lab_runtime_prepared
from tests.highwater_ed25519_support import export_public_keyring, write_private_manifest

_LAB_EXPECTED_ROOT = "/tmp/rquant-expected"
_LAB_TRUSTED_GIT = "/usr/bin/git"
_LAB_GENERATION = "1" * 40
_LAB_DEPLOYMENT_LOCK = "/tmp/.rquant-deploy/rquant-expected.lock"
_LAB_STARTUP_DEADLINE = 9_999_999_999.0
_LAB_GENERATION_ARGUMENTS = [
    "--deployment-generation",
    _LAB_GENERATION,
    "--deployment-lock-path",
    _LAB_DEPLOYMENT_LOCK,
    "--deployment-generation-fd",
    "9",
    "--deployment-operation-id",
    "a" * 32,
    "--deployment-environment-generation",
    "b" * 64,
]
_LAB_DAEMON_GENERATION_ARGUMENTS = [
    *_LAB_GENERATION_ARGUMENTS,
    "--startup-deadline-monotonic",
    str(_LAB_STARTUP_DEADLINE),
]
_LAB_RUNTIME_BOOTSTRAP_ARGUMENTS = [
    "--runtime-code-config",
    "/etc/rquant/runtime-code-bootstrap.json",
    "--runtime-code-trusted-base",
    "/etc/rquant",
    "--runtime-code-authority-uid",
    "0",
    "--runtime-code-authority-gid",
    "0",
]
_LAB_RUNTIME_BOOTSTRAP_VALUES = {
    "runtime_code_config": Path("/etc/rquant/runtime-code-bootstrap.json"),
    "runtime_code_trusted_base": Path("/etc/rquant"),
    "runtime_code_authority_uid": 0,
    "runtime_code_authority_gid": 0,
}


def test_legacy_shadow_recovery_cli_is_recovery_only() -> None:
    args = build_parser().parse_args(
        [
            "legacy-shadow-recover",
            "--source",
            "surge",
            "--date",
            "2026-08-03",
        ]
    )

    assert args.command == "legacy-shadow-recover"
    assert args.source == "surge"
    assert args.date == "2026-08-03"
    assert not hasattr(args, "events_path")
    assert not hasattr(args, "exported_at")


def test_lab_startup_deadline_binding_rejects_missing_or_expired_value() -> None:
    from rquant.cli import _lab_startup_deadline_binding

    with pytest.raises(RuntimeError, match="startup deadline"):
        _lab_startup_deadline_binding(argparse.Namespace())
    with pytest.raises(RuntimeError, match="startup deadline"):
        _lab_startup_deadline_binding(
            argparse.Namespace(startup_deadline_monotonic=time.monotonic() - 1)
        )


class _FakeLabSqliteAuthority:
    def __init__(self, path: Path, *, created: bool = False) -> None:
        self.path = path
        self.created = created

    def close(self) -> None:
        pass


class _FixedCliResourceClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class _FixedCliSystemResourceProbe:
    def available_memory_bytes(self) -> int:
        return 16 * 1024**3

    def available_disk_bytes(self, _path: Path) -> int:
        return 100 * 1024**3

    def cpu_load_pct(self) -> float:
        return 10.0

    def io_pressure_pct(self) -> float:
        return 5.0


class TestBuildParser:
    @pytest.fixture(autouse=True)
    def _fixed_absent_production_runtime(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from rquant import runtime_deployment_profile as deployment_module

        parent = tmp_path / "fixed-production-parent"
        parent.mkdir(mode=0o700)
        monkeypatch.setattr(
            deployment_module,
            "LINUX_PRODUCTION_RUNTIME_ROOT",
            parent / "runtime",
        )

    def test_production_daily_dag_has_no_free_profile_root(self) -> None:
        with pytest.raises(SystemExit) as error:
            build_parser().parse_args(
                [
                    "daily-dag",
                    "--profile-root",
                    "/tmp/attacker-controlled",
                    "--trade-date",
                    "2026-08-03",
                    "--source-generation-id",
                    "a" * 64,
                    "--source-content-hash",
                    "b" * 64,
                    "--command-manifest-hash",
                    "e" * 64,
                    "--code-commit",
                    "c" * 40,
                    "--profile-hash",
                    "d" * 64,
                ]
            )

        assert error.value.code == 2

    def test_daily_dag_dev_is_an_explicit_separate_entry(self, tmp_path: Path) -> None:
        args = build_parser().parse_args(
            [
                "daily-dag-dev",
                "--profile-root",
                str(tmp_path.resolve()),
                "--trade-date",
                "2026-08-03",
                "--source-generation-id",
                "a" * 64,
                "--source-content-hash",
                "b" * 64,
                "--command-manifest-hash",
                "e" * 64,
                "--code-commit",
                "c" * 40,
                "--profile-hash",
                "d" * 64,
            ]
        )

        assert args.command == "daily-dag-dev"
        assert args.profile_root == tmp_path.resolve()

    def test_daily_dag_dev_defaults_to_preview_shadow(self) -> None:
        args = build_parser().parse_args(
            [
                "daily-dag-dev",
                "--profile-root",
                "/tmp/daily-dag",
                "--trade-date",
                "2026-08-03",
                "--source-generation-id",
                "a" * 64,
                "--source-content-hash",
                "b" * 64,
                "--command-manifest-hash",
                "e" * 64,
                "--code-commit",
                "c" * 40,
                "--profile-hash",
                "d" * 64,
            ]
        )

        assert args.command == "daily-dag-dev"
        assert args.action == "preview"
        assert not hasattr(args, "report_authority_command")

    @pytest.mark.parametrize(
        ("option", "value"),
        [
            ("--report-authority-command", "/tmp/runner-owned-helper"),
            ("--report-authority-argument", "--keys-file=/tmp/runner-keys"),
            ("--report-authority-state-root", "/tmp/runner-owned-state"),
            ("--development-test-report-authority", "/tmp/test-capability"),
        ],
    )
    def test_daily_dag_rejects_runner_owned_authority_injection(
        self,
        option: str,
        value: str,
    ) -> None:
        with pytest.raises(SystemExit) as error:
            build_parser().parse_args(
                [
                    "daily-dag-dev",
                    "--profile-root",
                    "/tmp/daily-dag",
                    "--trade-date",
                    "2026-08-03",
                    "--source-generation-id",
                    "a" * 64,
                    "--source-content-hash",
                    "b" * 64,
                    "--command-manifest-hash",
                    "e" * 64,
                    "--code-commit",
                    "c" * 40,
                    "--profile-hash",
                    "d" * 64,
                    option,
                    value,
                ]
            )

        assert error.value.code == 2

    def test_daily_dag_preview_is_readonly_and_emits_bound_plan(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from rquant.cli import cmd_daily_dag
        from rquant.daily_pipeline_ledger import DailyPipelineMode, DailyPipelineStorageProfile

        storage_profile = DailyPipelineStorageProfile.create(
            root=tmp_path.resolve(),
            mode=DailyPipelineMode.SHADOW,
            profile_hash="d" * 64,
        )
        args = build_parser().parse_args(
            [
                "daily-dag-dev",
                "--profile-root",
                str(storage_profile.root),
                "--trade-date",
                "2026-08-03",
                "--source-generation-id",
                "a" * 64,
                "--source-content-hash",
                "b" * 64,
                "--command-manifest-hash",
                "e" * 64,
                "--code-commit",
                "c" * 40,
                "--profile-hash",
                "d" * 64,
            ]
        )

        assert cmd_daily_dag(args) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["action"] == "preview"
        assert result["mode"] == "shadow"
        assert len(result["plan_hash"]) == 64
        assert result["run_id"].startswith("daily-")
        assert storage_profile.state_path.exists() is False

    def test_daily_dag_dev_is_disabled_by_production_profile(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from rquant import cli as cli_module
        from rquant import config as config_module

        monkeypatch.setattr(config_module.settings, "app_env", "prod")
        development_root = tmp_path / "development"
        args = build_parser().parse_args(
            [
                "daily-dag-dev",
                "--profile-root",
                str(development_root.resolve()),
                "--trade-date",
                "2026-08-03",
                "--source-generation-id",
                "a" * 64,
                "--source-content-hash",
                "b" * 64,
                "--command-manifest-hash",
                "e" * 64,
                "--code-commit",
                "c" * 40,
                "--profile-hash",
                "d" * 64,
            ]
        )

        assert cli_module.cmd_daily_dag(args) == 2
        assert development_root.exists() is False

    def test_daily_dag_production_rejects_development_test_authority(
        self,
        tmp_path: Path,
    ) -> None:
        from rquant.cli import cmd_daily_dag
        from rquant.daily_pipeline_report_authority import (
            DailyPipelineDevelopmentTestReportAuthority,
        )

        class _DevelopmentAuthority:
            def compare_and_advance(self, _report: object) -> int:
                return 1

        args = build_parser().parse_args(
            [
                "daily-dag",
                "--trade-date",
                "2026-08-03",
                "--source-generation-id",
                "a" * 64,
                "--source-content-hash",
                "b" * 64,
                "--command-manifest-hash",
                "e" * 64,
                "--code-commit",
                "c" * 40,
                "--profile-hash",
                "d" * 64,
            ]
        )

        assert (
            cmd_daily_dag(
                args,
                development_test_report_authority=DailyPipelineDevelopmentTestReportAuthority(
                    capability=_DevelopmentAuthority()
                ),
            )
            == 2
        )

    def test_daily_dag_shadow_defaults_to_readonly_status(self) -> None:
        args = build_parser().parse_args(
            [
                "daily-dag-shadow",
                "--report-root",
                "/tmp/daily-shadow-reports",
                "--expected-trade-date",
                "2026-08-03",
            ]
        )

        assert args.command == "daily-dag-shadow"
        assert args.action == "status"
        assert args.minimum_real_trading_days == 10
        assert args.expected_trade_date == [date(2026, 8, 3)]

    def test_daily_dag_shadow_status_does_not_create_a_report_directory(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from rquant.cli import cmd_daily_dag_shadow

        report_root = tmp_path / "not-created-by-status"
        monkeypatch.setenv("RQUANT_DAILY_SHADOW_SIGNING_KEY", "x" * 32)
        args = build_parser().parse_args(
            [
                "daily-dag-shadow",
                "--report-root",
                str(report_root),
                "--expected-trade-date",
                "2026-07-20",
                "--expected-trade-date",
                "2026-07-21",
                "--expected-trade-date",
                "2026-07-22",
                "--expected-trade-date",
                "2026-07-23",
                "--expected-trade-date",
                "2026-07-24",
                "--expected-trade-date",
                "2026-07-27",
                "--expected-trade-date",
                "2026-07-28",
                "--expected-trade-date",
                "2026-07-29",
                "--expected-trade-date",
                "2026-07-30",
                "--expected-trade-date",
                "2026-07-31",
            ]
        )

        assert cmd_daily_dag_shadow(args) == 0
        assert report_root.exists() is False
        assert json.loads(capsys.readouterr().out)["mode"] == "shadow_readonly"

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
        assert not args.skip_minute_backfill
        assert args.minute_lookback_days == 90

    def test_run_daily_with_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "run-daily",
                "--date",
                "2026-04-18",
                "--preset",
                "n-shape-pool1",
                "--skip-minute-backfill",
                "--minute-lookback-days",
                "60",
            ]
        )
        assert args.date == "2026-04-18"
        assert args.preset == "n-shape-pool1"
        assert args.skip_minute_backfill
        assert args.minute_lookback_days == 60

    def test_no_command_returns_none(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

    def test_daily_indicator_backfill_defaults_to_preview_and_requires_apply(
        self,
    ) -> None:
        preview = build_parser().parse_args(
            [
                "daily-indicator-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-14",
            ]
        )
        apply = build_parser().parse_args(
            [
                "daily-indicator-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-14",
                "--apply",
            ]
        )

        assert preview.command == "daily-indicator-backfill"
        assert preview.start_date == date(2026, 4, 1)
        assert preview.end_date == date(2026, 7, 14)
        assert preview.apply is False
        assert apply.apply is True

    def test_research_export_requires_typed_dataset_and_dates(self) -> None:
        args = build_parser().parse_args(
            [
                "research-export",
                "--dataset",
                "minute_bar",
                "--start-date",
                "2026-07-14",
                "--end-date",
                "2026-07-15",
                "--dry-run",
            ]
        )

        assert args.command == "research-export"
        assert args.dataset == "minute_bar"
        assert args.start_date == date(2026, 7, 14)
        assert args.end_date == date(2026, 7, 15)
        assert args.dry_run is True

    def test_research_export_rejects_unsupported_dataset(self) -> None:
        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(
                [
                    "research-export",
                    "--dataset",
                    "daily_bar",
                    "--start-date",
                    "2026-07-14",
                    "--end-date",
                    "2026-07-15",
                ]
            )

        assert exc.value.code == 2

    def test_research_migration_snapshot_uses_explicit_identity_and_paths(self) -> None:
        args = build_parser().parse_args(
            [
                "research-migration",
                "snapshot",
                "--source-database",
                "/data/rquant.duckdb",
                "--recovery-dir",
                "/data/recovery",
                "--artifact-dir",
                "/data/strategy_lab_runs",
                "--snapshot-id",
                "research-20260716T160000Z-a1b2c3d4",
                "--code-commit",
                "a" * 40,
            ]
        )

        assert args.command == "research-migration"
        assert args.migration_command == "snapshot"
        assert args.source_database == Path("/data/rquant.duckdb")
        assert args.recovery_dir == Path("/data/recovery")
        assert args.artifact_dir == Path("/data/strategy_lab_runs")
        assert args.code_commit == "a" * 40

    def test_research_migration_publish_requires_explicit_apply(self) -> None:
        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(
                [
                    "research-migration",
                    "publish",
                    "--bundle-path",
                    "/staging/research-20260716T160000Z-a1b2c3d4",
                    "--target-data-dir",
                    "/srv/rquant/data",
                ]
            )

        assert exc.value.code == 2


class TestCLISmoke:
    def test_help_exits_0(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "rquant" in result.stdout

    def test_run_daily_help(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "run-daily", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--date" in result.stdout


class TestDailyIndicatorBackfill:
    def test_preview_uses_readonly_store_and_prints_json(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        import rquant.cli as cli
        from rquant.storage.duckdb import DuckDBStore

        store = DuckDBStore(tmp_path / "indicator-preview.duckdb")
        context = MagicMock()
        context.__enter__.return_value = store
        context.__exit__.side_effect = lambda *_: store.close()
        writer = MagicMock(side_effect=AssertionError("writer opened"))
        monkeypatch.setattr(cli, "open_readonly_store", lambda: context)
        monkeypatch.setattr(cli, "DuckDBStore", writer)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)
        args = build_parser().parse_args(
            [
                "daily-indicator-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-14",
            ]
        )

        assert cli.cmd_daily_indicator_backfill(args) == 0
        assert json.loads(capsys.readouterr().out) == {
            "code_count": 0,
            "estimated_rows": 0,
            "actual_rows": 0,
            "start_date": "2026-04-01",
            "end_date": "2026-07-14",
            "dry_run": True,
            "consistency_mode": "detached_snapshot_plus_source_fingerprint",
            "toctou_status": "not_applicable",
        }
        writer.assert_not_called()

    def test_explicit_apply_uses_writer(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        import rquant.cli as cli
        import rquant.indicator_backfill as backfill_module
        from rquant.storage.duckdb import DuckDBStore

        db_path = tmp_path / "indicator-apply.duckdb"
        with DuckDBStore(db_path):
            pass
        events: list[str] = []

        class _Context:
            def __init__(self, store: DuckDBStore, role: str) -> None:
                self.store = store
                self.role = role

            def __enter__(self) -> DuckDBStore:
                events.append(f"{self.role}_enter")
                return self.store

            def __exit__(self, *args: object) -> None:
                del args
                events.append(f"{self.role}_exit")
                self.store.close()

        def readonly() -> _Context:
            events.append("readonly_construct")
            return _Context(DuckDBStore(db_path, read_only=True), "readonly")

        def writable() -> _Context:
            events.append("writer_construct")
            return _Context(DuckDBStore(db_path), "writer")

        writer = MagicMock(side_effect=writable)
        replica = MagicMock(side_effect=AssertionError("replica opened"))
        monkeypatch.setattr(cli, "DuckDBStore", writer)
        monkeypatch.setattr(cli, "open_readonly_store", replica)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)
        monkeypatch.setattr(
            backfill_module,
            "open_detached_daily_indicator_store",
            readonly,
        )
        monkeypatch.setattr(
            backfill_module,
            "_now",
            lambda: datetime(2026, 7, 18, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        args = build_parser().parse_args(
            [
                "daily-indicator-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-14",
                "--apply",
            ]
        )

        assert cli.cmd_daily_indicator_backfill(args) == 0
        assert json.loads(capsys.readouterr().out)["dry_run"] is False
        writer.assert_called_once_with()
        replica.assert_not_called()
        assert events == [
            "readonly_construct",
            "readonly_enter",
            "readonly_exit",
            "writer_construct",
            "writer_enter",
            "writer_exit",
        ]

    def test_apply_returns_two_inside_protected_window(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        import rquant.cli as cli
        import rquant.indicator_backfill as backfill_module

        writer = MagicMock(side_effect=AssertionError("writer opened"))
        monkeypatch.setattr(cli, "DuckDBStore", writer)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)
        monkeypatch.setattr(
            backfill_module,
            "_now",
            lambda: datetime(2026, 7, 17, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        args = build_parser().parse_args(
            [
                "daily-indicator-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-14",
                "--apply",
            ]
        )

        assert cli.cmd_daily_indicator_backfill(args) == 2
        writer.assert_not_called()

    def test_apply_returns_two_when_indicator_coverage_is_incomplete(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import rquant.cli as cli
        import rquant.indicator_backfill as backfill_module

        def reject(**_: object) -> None:
            raise backfill_module.DailyIndicatorBackfillCoverageError(
                "daily_indicator coverage 1.00% is below 99.00% (1/100 rows)"
            )

        monkeypatch.setattr(
            backfill_module,
            "run_daily_indicator_backfill",
            reject,
        )
        monkeypatch.setattr(cli, "setup_logging", lambda: None)
        args = build_parser().parse_args(
            [
                "daily-indicator-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-14",
                "--apply",
            ]
        )

        assert cli.cmd_daily_indicator_backfill(args) == 2


class TestResearchExport:
    def test_command_requires_replica_and_passes_typed_plan(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_catalog as catalog_module
        from rquant import research_lake as lake_module
        from rquant import research_manifest as manifest_module
        from rquant.storage import duckdb as duckdb_module

        connection = MagicMock()
        summary = MagicMock()
        summary.model_dump_json.return_value = '{"status":"planned"}'
        observed: dict[str, object] = {}
        publish_lock = MagicMock()
        publish_lock_factory = MagicMock(return_value=publish_lock)

        def open_replica(*, require_replica: bool = False) -> MagicMock:
            observed["require_replica"] = require_replica
            return connection

        def export_dataset(source: object, **kwargs: object) -> MagicMock:
            observed["source"] = source
            observed.update(kwargs)
            return summary

        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(config_module.settings, "research_db_path", None)
        monkeypatch.setattr(config_module.settings, "research_lake_dir", None)
        monkeypatch.setattr(duckdb_module, "open_readonly_connection", open_replica)
        monkeypatch.setattr(catalog_module, "exclusive_file_lock", publish_lock_factory)
        monkeypatch.setattr(lake_module, "export_research_dataset", export_dataset)
        monkeypatch.setattr(manifest_module, "detect_code_commit", lambda: "a" * 40)
        args = build_parser().parse_args(
            [
                "research-export",
                "--dataset",
                "auction_bar",
                "--start-date",
                "2026-07-14",
                "--end-date",
                "2026-07-15",
            ]
        )

        assert cli.cmd_research_export(args) == 0
        assert observed["require_replica"] is True
        assert observed["source"] is connection
        assert observed["dataset"] == "auction_bar"
        assert observed["start_date"] == date(2026, 7, 14)
        assert observed["end_date"] == date(2026, 7, 15)
        assert observed["dry_run"] is False
        assert observed["code_commit"] == "a" * 40
        publish_lock_factory.assert_called_once_with(tmp_path / "research-publish.lock")
        connection.close.assert_called_once_with()
        assert capsys.readouterr().out.strip() == '{"status":"planned"}'

    def test_command_refuses_formal_publish_after_authority_exists(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_catalog as catalog_module
        from rquant import research_lake as lake_module
        from rquant.storage import duckdb as duckdb_module

        connection = MagicMock()
        export_dataset = MagicMock()
        publish_lock = MagicMock()
        (tmp_path / "research-authority-candidate.json").write_text("{}\n", encoding="utf-8")
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(config_module.settings, "research_db_path", None)
        monkeypatch.setattr(config_module.settings, "research_lake_dir", None)
        monkeypatch.setattr(
            duckdb_module,
            "open_readonly_connection",
            MagicMock(return_value=connection),
        )
        monkeypatch.setattr(
            catalog_module,
            "exclusive_file_lock",
            MagicMock(return_value=publish_lock),
        )
        monkeypatch.setattr(lake_module, "export_research_dataset", export_dataset)
        args = build_parser().parse_args(
            [
                "research-export",
                "--dataset",
                "auction_bar",
                "--start-date",
                "2026-07-14",
                "--end-date",
                "2026-07-15",
            ]
        )

        assert cli.cmd_research_export(args) == 3
        export_dataset.assert_not_called()
        connection.close.assert_called_once_with()


class TestResearchIngest:
    def test_default_date_skips_authoritative_closed_day_before_adapter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_ingest as ingest_module

        source = tmp_path / "rquant_ro.duckdb"
        is_open = MagicMock(return_value=False)
        adapter_factory = MagicMock()
        monkeypatch.setattr(config_module.settings, "duckdb_readonly_path", source)
        monkeypatch.setattr(config_module.settings, "research_cloud_ingest_enabled", True)
        monkeypatch.setattr(ingest_module, "research_trade_date_is_open", is_open)
        monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", adapter_factory)
        args = build_parser().parse_args(["research-ingest"])
        expected_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()

        assert args.date is None
        assert cli.cmd_research_ingest(args) == 0
        is_open.assert_called_once_with(source, expected_date)
        adapter_factory.assert_not_called()
        assert json.loads(capsys.readouterr().out) == {
            "status": "skipped",
            "trade_date": expected_date.isoformat(),
            "reason": "closed_trade_date",
        }

    def test_scheduled_explicit_closed_day_skips_before_adapter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_ingest as ingest_module

        source = tmp_path / "rquant_ro.duckdb"
        is_open = MagicMock(return_value=False)
        adapter_factory = MagicMock()
        monkeypatch.setattr(config_module.settings, "duckdb_readonly_path", source)
        monkeypatch.setattr(config_module.settings, "research_cloud_ingest_enabled", True)
        monkeypatch.setattr(ingest_module, "research_trade_date_is_open", is_open)
        monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", adapter_factory)
        args = build_parser().parse_args(
            [
                "research-ingest",
                "--date",
                "2026-07-17",
                "--scheduled",
            ]
        )

        assert cli.cmd_research_ingest(args) == 0
        is_open.assert_called_once_with(source, date(2026, 7, 17))
        adapter_factory.assert_not_called()

    def test_command_is_disabled_by_default_before_adapter_or_writes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module

        adapter = MagicMock()
        monkeypatch.setattr(config_module.settings, "research_cloud_ingest_enabled", False)
        monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", adapter)
        args = build_parser().parse_args(["research-ingest", "--date", "2026-07-17"])

        assert cli.cmd_research_ingest(args) == 3
        adapter.assert_not_called()

    def test_command_delegates_paths_and_returns_degraded_exit_two(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_ingest as ingest_module
        from rquant import research_manifest as manifest_module

        adapter = MagicMock()
        adapter_factory = MagicMock(return_value=adapter)
        result = MagicMock(status="degraded")
        result.model_dump_json.return_value = '{"status":"degraded"}'
        run_ingest = MagicMock(return_value=result)
        source = tmp_path / "rquant_ro.duckdb"
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(config_module.settings, "duckdb_readonly_path", source)
        monkeypatch.setattr(config_module.settings, "research_cloud_ingest_enabled", True)
        monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", adapter_factory)
        monkeypatch.setattr(ingest_module, "run_daily_research_ingest", run_ingest)
        monkeypatch.setattr(manifest_module, "detect_code_commit", lambda: "a" * 40)
        args = build_parser().parse_args(["research-ingest", "--date", "2026-07-17"])

        assert cli.cmd_research_ingest(args) == 2
        run_ingest.assert_called_once_with(
            source_database=source,
            paths=ingest_module.ResearchIngestPaths.from_data_dir(tmp_path),
            trade_date=date(2026, 7, 17),
            adapter=adapter,
            code_commit="a" * 40,
            dry_run=False,
            recovery=False,
        )
        assert capsys.readouterr().out.strip() == '{"status":"degraded"}'

    def test_dry_run_is_allowed_while_production_switch_is_off(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_ingest as ingest_module

        result = MagicMock(status="planned")
        result.model_dump_json.return_value = '{"status":"planned"}'
        run_ingest = MagicMock(return_value=result)
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(config_module.settings, "research_cloud_ingest_enabled", False)
        monkeypatch.setattr(ingest_module, "run_daily_research_ingest", run_ingest)
        args = build_parser().parse_args(
            [
                "research-ingest",
                "--date",
                "2026-07-17",
                "--dry-run",
            ]
        )

        assert cli.cmd_research_ingest(args) == 0
        assert run_ingest.call_args.kwargs["dry_run"] is True

    def test_recover_uses_explicit_historical_mode(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_ingest as ingest_module
        from rquant import research_manifest as manifest_module

        result = MagicMock(status="candidate")
        result.model_dump_json.return_value = '{"status":"candidate"}'
        run_ingest = MagicMock(return_value=result)
        source = tmp_path / "rquant_ro.duckdb"
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(config_module.settings, "duckdb_readonly_path", source)
        monkeypatch.setattr(config_module.settings, "research_cloud_ingest_enabled", True)
        monkeypatch.setattr(ingest_module, "run_daily_research_ingest", run_ingest)
        monkeypatch.setattr(manifest_module, "detect_code_commit", lambda: "a" * 40)
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter", MagicMock(return_value=MagicMock())
        )
        args = build_parser().parse_args(["research-ingest", "--date", "2026-07-16", "--recover"])

        assert cli.cmd_research_ingest(args) == 0
        assert run_ingest.call_args.kwargs["trade_date"] == date(2026, 7, 16)
        assert run_ingest.call_args.kwargs["recovery"] is True

    def test_recover_requires_explicit_date(self) -> None:
        args = build_parser().parse_args(["research-ingest", "--recover"])

        with pytest.raises(ValueError, match="requires --date"):
            from rquant.cli import cmd_research_ingest

            cmd_research_ingest(args)

    def test_readiness_command_returns_one_for_stale_replica(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_ingest as ingest_module

        result = MagicMock(status="not_ready")
        result.model_dump_json.return_value = '{"status":"not_ready"}'
        assess = MagicMock(return_value=result)
        source = tmp_path / "rquant_ro.duckdb"
        monkeypatch.setattr(config_module.settings, "duckdb_readonly_path", source)
        monkeypatch.setattr(ingest_module, "assess_research_ingest_readiness", assess)
        args = build_parser().parse_args(["research-ingest-readiness", "--date", "2026-07-17"])

        assert cli.cmd_research_ingest_readiness(args) == 1
        assess.assert_called_once_with(source, date(2026, 7, 17))
        assert capsys.readouterr().out.strip() == '{"status":"not_ready"}'

    def test_authority_status_is_read_only_and_machine_readable(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import research_ingest as ingest_module

        status = MagicMock()
        status.model_dump_json.return_value = '{"status":"candidate"}'
        inspect = MagicMock(return_value=status)
        monkeypatch.setattr(ingest_module, "inspect_research_authority", inspect)
        args = build_parser().parse_args(["research-authority-status", "--data-dir", str(tmp_path)])

        assert cli.cmd_research_authority_status(args) == 0
        inspect.assert_called_once_with(ingest_module.ResearchIngestPaths.from_data_dir(tmp_path))
        assert capsys.readouterr().out.strip() == '{"status":"candidate"}'


class TestResearchAuctionRepair:
    def test_parser_requires_apply_and_plan_id_together(self) -> None:
        parser = build_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "research-repair-auction",
                    "--date",
                    "2026-07-14",
                    "--apply",
                ]
            )
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "research-repair-auction",
                    "--date",
                    "2026-07-14",
                    "--plan-id",
                    "a" * 64,
                ]
            )

    def test_apply_is_disabled_before_adapter_when_switch_is_off(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module

        adapter_factory = MagicMock()
        monkeypatch.setattr(
            config_module.settings,
            "research_cloud_ingest_enabled",
            False,
        )
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            adapter_factory,
        )
        args = build_parser().parse_args(
            [
                "research-repair-auction",
                "--date",
                "2026-07-14",
                "--apply",
                "--plan-id",
                "a" * 64,
            ]
        )

        assert cli.cmd_research_repair_auction(args) == 3
        adapter_factory.assert_not_called()

    def test_preview_delegates_all_dates_and_prints_plan_id(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_manifest as manifest_module
        from rquant import research_repair as repair_module

        source = tmp_path / "rquant_ro.duckdb"
        adapter = MagicMock()
        adapter_factory = MagicMock(return_value=adapter)
        result = MagicMock(status="planned", plan_id="f" * 64)
        result.model_dump.return_value = {
            "status": "planned",
            "plan": {"trade_dates": ["2026-07-14", "2026-07-15"]},
            "observation": None,
        }
        run_repair = MagicMock(return_value=result)
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(config_module.settings, "duckdb_readonly_path", source)
        monkeypatch.setattr(
            config_module.settings,
            "research_cloud_ingest_enabled",
            False,
        )
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            adapter_factory,
        )
        monkeypatch.setattr(
            repair_module,
            "run_research_auction_repair",
            run_repair,
        )
        monkeypatch.setattr(
            manifest_module,
            "detect_code_commit",
            lambda: "a" * 40,
        )
        args = build_parser().parse_args(
            [
                "research-repair-auction",
                "--date",
                "2026-07-15",
                "--date",
                "2026-07-14",
            ]
        )

        assert cli.cmd_research_repair_auction(args) == 0
        run_repair.assert_called_once_with(
            source_database=source,
            paths=repair_module.ResearchIngestPaths.from_data_dir(tmp_path),
            trade_dates=(date(2026, 7, 15), date(2026, 7, 14)),
            adapter=adapter,
            code_commit="a" * 40,
            apply=False,
            plan_id=None,
        )
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "planned"
        assert output["plan_id"] == "f" * 64


class TestResearchMinuteRepair:
    def test_parser_requires_apply_and_plan_id_together(self) -> None:
        parser = build_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "research-repair-minute",
                    "--manifest-id",
                    "b" * 64,
                    "--apply",
                ]
            )
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "research-repair-minute",
                    "--manifest-id",
                    "b" * 64,
                    "--plan-id",
                    "a" * 64,
                ]
            )

    def test_parser_rejects_non_sha_manifest_id(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args(
                [
                    "research-repair-minute",
                    "--manifest-id",
                    "not-a-sha",
                ]
            )

    def test_apply_is_disabled_before_opening_backfill_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_minute_repair as repair_module

        state_factory = MagicMock()
        snapshot_factory = MagicMock()
        run_repair = MagicMock()
        monkeypatch.setattr(
            config_module.settings,
            "research_cloud_ingest_enabled",
            False,
        )
        monkeypatch.setattr(cli, "BackfillStateStore", state_factory)
        monkeypatch.setattr(
            cli,
            "open_backfill_state_snapshot",
            snapshot_factory,
        )
        monkeypatch.setattr(
            repair_module,
            "run_research_minute_repair",
            run_repair,
        )
        args = build_parser().parse_args(
            [
                "research-repair-minute",
                "--manifest-id",
                "b" * 64,
                "--apply",
                "--plan-id",
                "a" * 64,
            ]
        )

        assert cli.cmd_research_repair_minute(args) == 3
        state_factory.assert_not_called()
        snapshot_factory.assert_not_called()
        run_repair.assert_not_called()

    def test_preview_is_read_only_and_prints_content_bound_plan_id(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_manifest as manifest_module
        from rquant import research_minute_repair as repair_module

        source = tmp_path / "rquant_ro.duckdb"
        state_path = tmp_path / "backfill.sqlite3"
        state = MagicMock()
        state_factory = MagicMock(side_effect=AssertionError("live state opened"))
        snapshot_context = MagicMock()
        snapshot_context.__enter__.return_value = state
        snapshot_context.__exit__.return_value = False
        snapshot_factory = MagicMock(return_value=snapshot_context)
        result = MagicMock(status="planned", plan_id="f" * 64)
        result.model_dump.return_value = {
            "status": "planned",
            "plan": {"missing_session_count": 3},
            "observation": None,
        }
        run_repair = MagicMock(return_value=result)
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(
            config_module.settings,
            "duckdb_readonly_path",
            source,
        )
        monkeypatch.setattr(
            config_module.settings,
            "backfill_state_path",
            state_path,
        )
        monkeypatch.setattr(
            config_module.settings,
            "research_cloud_ingest_enabled",
            False,
        )
        monkeypatch.setattr(cli, "BackfillStateStore", state_factory)
        monkeypatch.setattr(
            cli,
            "open_backfill_state_snapshot",
            snapshot_factory,
        )
        monkeypatch.setattr(
            repair_module,
            "run_research_minute_repair",
            run_repair,
        )
        monkeypatch.setattr(
            manifest_module,
            "detect_code_commit",
            lambda: "a" * 40,
        )
        args = build_parser().parse_args(
            [
                "research-repair-minute",
                "--manifest-id",
                "b" * 64,
            ]
        )

        assert cli.cmd_research_repair_minute(args) == 0
        snapshot_factory.assert_called_once_with(
            state_path,
            busy_timeout_ms=config_module.settings.backfill_state_busy_timeout_ms,
        )
        state_factory.assert_not_called()
        run_repair.assert_called_once_with(
            source_database=source,
            primary_database=config_module.settings.duckdb_path,
            paths=repair_module.ResearchIngestPaths.from_data_dir(tmp_path),
            state=state,
            manifest_id="b" * 64,
            code_commit="a" * 40,
            apply=False,
            plan_id=None,
        )
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "planned"
        assert output["plan_id"] == "f" * 64

    def test_apply_delegates_confirmed_plan_and_prints_candidate(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import config as config_module
        from rquant import research_manifest as manifest_module
        from rquant import research_minute_repair as repair_module

        state = MagicMock()
        snapshot_context = MagicMock()
        snapshot_context.__enter__.return_value = state
        snapshot_context.__exit__.return_value = False
        snapshot_factory = MagicMock(return_value=snapshot_context)
        result = MagicMock(status="candidate", plan_id="f" * 64)
        result.model_dump.return_value = {
            "status": "candidate",
            "plan": {"missing_session_count": 3},
            "observation": {"observation_kind": "minute_repair"},
        }
        run_repair = MagicMock(return_value=result)
        monkeypatch.setattr(config_module.settings, "data_dir", tmp_path)
        monkeypatch.setattr(
            config_module.settings,
            "research_cloud_ingest_enabled",
            True,
        )
        monkeypatch.setattr(
            cli,
            "BackfillStateStore",
            MagicMock(side_effect=AssertionError("live state opened")),
        )
        monkeypatch.setattr(
            cli,
            "open_backfill_state_snapshot",
            snapshot_factory,
        )
        monkeypatch.setattr(
            repair_module,
            "run_research_minute_repair",
            run_repair,
        )
        monkeypatch.setattr(
            manifest_module,
            "detect_code_commit",
            lambda: "a" * 40,
        )
        args = build_parser().parse_args(
            [
                "research-repair-minute",
                "--manifest-id",
                "b" * 64,
                "--apply",
                "--plan-id",
                "f" * 64,
            ]
        )

        assert cli.cmd_research_repair_minute(args) == 0
        snapshot_factory.assert_called_once_with(
            config_module.settings.backfill_state_path_resolved,
            busy_timeout_ms=config_module.settings.backfill_state_busy_timeout_ms,
        )
        assert run_repair.call_args.kwargs["apply"] is True
        assert run_repair.call_args.kwargs["plan_id"] == "f" * 64
        assert run_repair.call_args.kwargs["primary_database"] == config_module.settings.duckdb_path
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "candidate"
        assert output["plan_id"] == "f" * 64


class TestFormalSmokeReplay:
    def test_parser_requires_exact_formal_evidence(self) -> None:
        runtime_arguments = [
            "--runtime-code-config",
            "/tmp/runtime-code-bootstrap.json",
            "--runtime-code-trusted-base",
            "/tmp",
            "--runtime-code-authority-uid",
            "1",
            "--runtime-code-authority-gid",
            "1",
        ]
        args = build_parser().parse_args(
            [
                "formal-smoke-replay",
                "--strategy",
                "n_shape",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-02",
                "--audit-run-id",
                "a" * 64,
                "--snapshot-id",
                "b" * 64,
                "--binding-hash",
                "c" * 64,
                "--output-dir",
                "/tmp/formal-smoke",
                "--execution-timeout-seconds",
                "0.25",
                *runtime_arguments,
            ]
        )

        assert args.command == "formal-smoke-replay"
        assert args.strategy == "n_shape"
        assert args.start_date == date(2026, 4, 1)
        assert args.end_date == date(2026, 7, 2)
        assert args.audit_run_id == "a" * 64
        assert args.snapshot_id == "b" * 64
        assert args.binding_hash == "c" * 64
        assert args.output_dir == Path("/tmp/formal-smoke")
        assert args.execution_timeout_seconds == 0.25

        with pytest.raises(SystemExit):
            build_parser().parse_args(
                [
                    "formal-smoke-replay",
                    "--strategy",
                    "n_shape",
                    "--start-date",
                    "2026-04-01",
                    "--end-date",
                    "2026-07-02",
                    "--audit-run-id",
                    "not-a-hash",
                    "--snapshot-id",
                    "b" * 64,
                    "--binding-hash",
                    "c" * 64,
                    *runtime_arguments,
                ]
            )
        with pytest.raises(SystemExit):
            build_parser().parse_args(
                [
                    "formal-smoke-replay",
                    "--strategy",
                    "n_shape",
                    "--start-date",
                    "2026-04-01",
                    "--end-date",
                    "2026-07-02",
                    "--audit-run-id",
                    "a" * 64,
                    "--snapshot-id",
                    "b" * 64,
                    "--binding-hash",
                    "c" * 64,
                    "--execution-timeout-seconds",
                    "0",
                    *runtime_arguments,
                ]
            )
        with pytest.raises(SystemExit):
            build_parser().parse_args(
                [
                    "formal-smoke-replay",
                    "--strategy",
                    "n_shape",
                    "--start-date",
                    "2026-04-01",
                    "--end-date",
                    "2026-07-02",
                    "--audit-run-id",
                    "a" * 64,
                    "--snapshot-id",
                    "b" * 64,
                    "--binding-hash",
                    "c" * 64,
                    "--mode",
                    "exploratory",
                    *runtime_arguments,
                ]
            )

    def test_command_detects_commit_runs_formal_replay_and_prints_json(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import formal_runtime_composition as composition_module
        from rquant import formal_smoke_execution as execution_module
        from rquant.runtime_code_attestation import CodeTrustEvidence

        result = MagicMock()
        result.model_dump.return_value = {
            "status": "comparable",
            "strategy": "n_shape",
            "run_id": "formal-run-1",
            "strategy_spec_hash": "e" * 64,
            "result_hash": "f" * 64,
        }
        run = MagicMock(return_value=result)
        capability = MagicMock()
        capability.evidence = CodeTrustEvidence(
            generation_id="a" * 64,
            attestation_sha256="b" * 64,
            content_root_sha256="d" * 64,
            promotion_sequence=1,
            provenance_commit="d" * 40,
        )

        monkeypatch.setattr(
            composition_module,
            "open_formal_runtime_capability",
            lambda **_kwargs: capability,
        )
        monkeypatch.setattr(
            execution_module,
            "run_attested_formal_smoke",
            run,
        )
        args = build_parser().parse_args(
            [
                "formal-smoke-replay",
                "--strategy",
                "n_shape",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-02",
                "--audit-run-id",
                "a" * 64,
                "--snapshot-id",
                "b" * 64,
                "--binding-hash",
                "c" * 64,
                "--output-dir",
                str(tmp_path),
                "--runtime-code-config",
                "/tmp/runtime-code-bootstrap.json",
                "--runtime-code-trusted-base",
                "/tmp",
                "--runtime-code-authority-uid",
                "1",
                "--runtime-code-authority-gid",
                "1",
            ]
        )

        assert cli.cmd_formal_smoke_replay(args) == 0

        assert run.call_args.args == (capability,)
        assert run.call_args.kwargs["strategy"] == "n_shape"
        assert run.call_args.kwargs["start_date"] == date(2026, 4, 1)
        assert run.call_args.kwargs["end_date"] == date(2026, 7, 2)
        assert run.call_args.kwargs["audit_run_id"] == "a" * 64
        assert run.call_args.kwargs["dataset_snapshot_id"] == "b" * 64
        assert run.call_args.kwargs["dataset_binding_hash"] == "c" * 64
        assert run.call_args.kwargs["output_dir"] == tmp_path
        reference = run.call_args.kwargs["bootstrap_reference"]
        assert reference.configuration_path == Path("/tmp/runtime-code-bootstrap.json")
        assert reference.trusted_base == Path("/tmp")
        assert reference.expected_authority_uid == 1
        assert reference.expected_authority_gid == 1
        assert run.call_args.kwargs["environment_source"] is os.environ
        assert run.call_args.kwargs["execution_deadline_monotonic"] > time.monotonic()
        assert json.loads(capsys.readouterr().out) == result.model_dump.return_value

    def test_command_rejects_missing_runtime_capability_before_compute(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import formal_runtime_composition as composition_module
        from rquant import formal_smoke_execution as execution_module

        run = MagicMock()
        monkeypatch.setattr(
            composition_module,
            "open_formal_runtime_capability",
            lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("invalid")),
        )
        monkeypatch.setattr(
            execution_module,
            "run_attested_formal_smoke",
            run,
        )
        args = build_parser().parse_args(
            [
                "formal-smoke-replay",
                "--strategy",
                "n_shape",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-02",
                "--audit-run-id",
                "a" * 64,
                "--snapshot-id",
                "b" * 64,
                "--binding-hash",
                "c" * 64,
                "--runtime-code-config",
                "/tmp/runtime-code-bootstrap.json",
                "--runtime-code-trusted-base",
                "/tmp",
                "--runtime-code-authority-uid",
                "1",
                "--runtime-code-authority-gid",
                "1",
            ]
        )

        assert cli.cmd_formal_smoke_replay(args) == 2
        run.assert_not_called()


class TestResearchMigration:
    def test_verify_command_prints_machine_readable_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import research_migration as migration_module

        result = MagicMock()
        result.model_dump_json.return_value = '{"status":"verified"}'
        verify = MagicMock(return_value=result)
        monkeypatch.setattr(migration_module, "verify_research_migration_bundle", verify)
        args = build_parser().parse_args(
            [
                "research-migration",
                "verify",
                "--bundle-path",
                "/staging/research-20260716T160000Z-a1b2c3d4",
            ]
        )

        assert cli.cmd_research_migration(args) == 0
        verify.assert_called_once_with(Path("/staging/research-20260716T160000Z-a1b2c3d4"))
        assert capsys.readouterr().out.strip() == '{"status":"verified"}'

    def test_publish_command_delegates_only_with_apply(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from unittest.mock import MagicMock

        from rquant import cli
        from rquant import research_migration as migration_module

        result = MagicMock()
        result.model_dump_json.return_value = '{"status":"published"}'
        publish = MagicMock(return_value=result)
        monkeypatch.setattr(migration_module, "publish_research_migration_bundle", publish)
        args = build_parser().parse_args(
            [
                "research-migration",
                "publish",
                "--bundle-path",
                "/staging/research-20260716T160000Z-a1b2c3d4",
                "--target-data-dir",
                "/srv/rquant/data",
                "--apply",
            ]
        )

        assert cli.cmd_research_migration(args) == 0
        publish.assert_called_once_with(
            Path("/staging/research-20260716T160000Z-a1b2c3d4"),
            target_data_dir=Path("/srv/rquant/data"),
        )
        assert capsys.readouterr().out.strip() == '{"status":"published"}'


class TestTradeCalendarBootstrap:
    def test_parser_defaults_to_2020_through_current_year(self) -> None:
        args = build_parser().parse_args(["trade-calendar-bootstrap"])

        assert args.command == "trade-calendar-bootstrap"
        assert args.start_date == date(2020, 1, 1)
        assert args.end_date == date(date.today().year, 12, 31)

    def test_parser_accepts_custom_iso_dates(self) -> None:
        args = build_parser().parse_args(
            [
                "trade-calendar-bootstrap",
                "--start-date",
                "2024-02-01",
                "--end-date",
                "2024-02-29",
            ]
        )

        assert args.start_date == date(2024, 2, 1)
        assert args.end_date == date(2024, 2, 29)

    def test_parser_rejects_non_iso_date_with_argparse_exit_2(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(["trade-calendar-bootstrap", "--start-date", "20240201"])

        assert exc.value.code == 2
        assert "--start-date" in capsys.readouterr().err

    def test_start_after_end_fails_without_fetch_or_writer(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        import rquant.cli as cli

        adapter_factory = MagicMock()
        store_factory = MagicMock()
        monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", adapter_factory)
        monkeypatch.setattr(cli, "DuckDBStore", store_factory)

        result = cli.cmd_trade_calendar_bootstrap(
            SimpleNamespace(
                start_date=date(2026, 1, 2),
                end_date=date(2026, 1, 1),
            )
        )

        assert result != 0
        adapter_factory.assert_not_called()
        store_factory.assert_not_called()

    def test_fetch_and_validation_finish_before_writer_enters(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        import rquant.cli as cli

        writer_active = False
        events: list[str] = []
        authoritative_rows = [object()]

        class _Store:
            def __enter__(self) -> _Store:
                nonlocal writer_active
                assert writer_active is False
                writer_active = True
                events.append("writer_enter")
                return self

            def __exit__(self, *_: object) -> None:
                nonlocal writer_active
                writer_active = False
                events.append("writer_exit")

        def fake_fetch(
            adapter: object,
            *,
            exchange: str,
            start: date,
            end: date,
        ) -> list[object]:
            assert writer_active is False
            assert adapter is adapter_instance
            assert (exchange, start, end) == (
                "SSE",
                date(2026, 1, 1),
                date(2026, 1, 5),
            )
            events.append("fetch")
            return authoritative_rows

        def fake_persist(
            store: _Store,
            rows: list[object],
            *,
            exchange: str,
            start: date,
            end: date,
        ) -> SimpleNamespace:
            assert writer_active is True
            assert isinstance(store, _Store)
            assert rows is authoritative_rows
            assert (exchange, start, end) == (
                "SSE",
                date(2026, 1, 1),
                date(2026, 1, 5),
            )
            events.append("persist")
            return SimpleNamespace(
                requested_days=5,
                fetched_days=5,
                upserted_days=5,
            )

        adapter_instance = object()
        info = MagicMock()
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter_instance),
        )
        monkeypatch.setattr("rquant.trade_calendar.fetch_trade_calendar_rows", fake_fetch)
        monkeypatch.setattr("rquant.trade_calendar.persist_verified_trade_calendar", fake_persist)
        monkeypatch.setattr(cli, "DuckDBStore", MagicMock(side_effect=_Store))
        monkeypatch.setattr(cli, "setup_logging", MagicMock())
        monkeypatch.setattr(cli, "logger", SimpleNamespace(info=info))

        result = cli.cmd_trade_calendar_bootstrap(
            SimpleNamespace(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 5),
            )
        )

        assert result == 0
        assert events == ["fetch", "writer_enter", "persist", "writer_exit"]
        message = str(info.call_args.args[0])
        assert "processed=5" in message
        assert "changed" not in message

    def test_invalid_pretrade_chain_fails_before_writer_opens(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        import rquant.cli as cli

        class _Adapter:
            def trade_cal_raw(
                self,
                start: date,
                end: date,
                exchange: str = "SSE",
            ) -> pd.DataFrame:
                return pd.DataFrame(
                    [
                        {
                            "exchange": exchange,
                            "cal_date": start,
                            "is_open": True,
                            "pretrade_date": date(2025, 12, 31),
                        },
                        {
                            "exchange": exchange,
                            "cal_date": end,
                            "is_open": False,
                            "pretrade_date": date(2025, 12, 31),
                        },
                    ]
                )

        store_factory = MagicMock(side_effect=AssertionError("writer opened"))
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=_Adapter()),
        )
        monkeypatch.setattr(cli, "DuckDBStore", store_factory)
        monkeypatch.setattr(cli, "setup_logging", MagicMock())

        with pytest.raises(ValueError, match="pretrade_date chain"):
            cli.cmd_trade_calendar_bootstrap(
                SimpleNamespace(
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 1, 2),
                )
            )

        store_factory.assert_not_called()


class TestSuspensionBackfillParser:
    def test_parser_accepts_range_and_full_refresh(self) -> None:
        args = build_parser().parse_args(
            [
                "suspension-backfill",
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-15",
                "--full-refresh",
                "--dry-run",
            ]
        )

        assert args.command == "suspension-backfill"
        assert args.start_date == date(2026, 7, 1)
        assert args.end_date == date(2026, 7, 15)
        assert args.full_refresh is True
        assert args.dry_run is True

    def test_dry_run_does_not_construct_tushare_adapter(
        self,
        monkeypatch,
        capsys,
    ) -> None:
        import rquant.cli as cli
        import rquant.suspension as suspension_module

        class Plan:
            def model_dump(self, *, mode: str) -> dict[str, object]:
                assert mode == "json"
                return {"status": "ready"}

        calls: list[tuple[date, date, bool]] = []

        def plan(
            *,
            store_factory,
            start: date,
            end: date,
            missing_only: bool,
        ) -> Plan:
            assert store_factory is cli.open_readonly_store
            calls.append((start, end, missing_only))
            return Plan()

        monkeypatch.setattr(
            suspension_module,
            "plan_suspension_backfill",
            plan,
            raising=False,
        )
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            lambda: pytest.fail("dry-run constructed TushareAdapter"),
        )
        monkeypatch.setattr(cli, "setup_logging", lambda: None)

        result = cli.cmd_suspension_backfill(
            SimpleNamespace(
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 15),
                full_refresh=True,
                dry_run=True,
            )
        )

        assert result == 0
        assert calls == [(date(2026, 7, 1), date(2026, 7, 15), False)]
        assert json.loads(capsys.readouterr().out) == {"status": "ready"}


class TestSecurityStatusBackfillParser:
    def test_parser_accepts_dry_run_and_full_refresh(self) -> None:
        args = build_parser().parse_args(
            [
                "security-status-backfill",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-07-15",
                "--dry-run",
                "--full-refresh",
            ]
        )

        assert args.command == "security-status-backfill"
        assert args.start_date == date(2026, 4, 1)
        assert args.end_date == date(2026, 7, 15)
        assert args.dry_run is True
        assert args.full_refresh is True


class TestPreflightParser:
    def test_research_profile_is_explicitly_selectable(self) -> None:
        args = build_parser().parse_args(["preflight", "--profile", "research"])

        assert args.command == "preflight"
        assert args.profile == "research"

    def test_authority_daemon_commands_require_explicit_config_paths(self) -> None:
        root = build_parser().parse_args(
            [
                "external-monotonic-root-serve",
                "--config",
                "/etc/rquant/external-monotonic-root.json",
            ]
        )
        resource = build_parser().parse_args(
            [
                "resource-authority-serve",
                "--config",
                "/etc/rquant/resource-authority.json",
                "--code-sha",
                "1" * 40,
            ]
        )

        assert root.config == Path("/etc/rquant/external-monotonic-root.json")
        assert resource.config == Path("/etc/rquant/resource-authority.json")
        assert resource.code_sha == "1" * 40

    def test_authority_daemon_cli_rejects_unselected_production_manifests(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import rquant.resource_authority_service as authority_service
        from rquant.cli import (
            cmd_external_monotonic_root_serve,
            cmd_resource_authority_serve,
        )

        def selected_environment(path: Path, **_: object) -> dict[str, str]:
            if path == authority_service.EXTERNAL_ROOT_ENVIRONMENT_PATH:
                return {
                    "APP_ENV": "prod",
                    "RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH": (
                        "/etc/rquant/selected-external-root.json"
                    ),
                }
            return {
                "APP_ENV": "prod",
                "RQUANT_CODE_COMMIT": "1" * 40,
                "RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT": "/var/lib/rquant-serving/runtime_health",
                "RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON": "{}",
                "RQUANT_LAB_RESOURCE_POLICY_VERSION": "lab-resource-v1",
                "RQUANT_LAB_TRADE_CALENDAR_PATH": "/var/lib/rquant-serving/calendar.json",
                "RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH": (
                    "/etc/rquant/selected-resource-authority.json"
                ),
                "RQUANT_RESOURCE_AUTHORITY_STATE_DIR": "/var/lib/rquant-resource-authority",
            }

        monkeypatch.setattr(
            authority_service,
            "load_closed_authority_environment",
            selected_environment,
        )
        with pytest.raises(RuntimeError, match="configured production manifest"):
            cmd_external_monotonic_root_serve(
                argparse.Namespace(config=Path("/etc/rquant/external-monotonic-root.json"))
            )
        with pytest.raises(RuntimeError, match="configured production identity"):
            cmd_resource_authority_serve(
                argparse.Namespace(
                    config=Path("/etc/rquant/resource-authority.json"),
                    code_sha="1" * 40,
                )
            )

    def test_resource_authority_daemon_cli_uses_only_the_closed_environment(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import rquant.cli as cli
        import rquant.lab_resource_authority_adapter as adapter_module
        import rquant.resource_authority_service as authority_service
        import rquant.runtime_resource_admission as admission_module

        adapter = object()
        configuration = SimpleNamespace(
            service_configuration=SimpleNamespace(adapter_configuration=adapter)
        )
        environment = {
            "APP_ENV": "prod",
            "RQUANT_CODE_COMMIT": "1" * 40,
            "RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT": ("/var/lib/rquant-serving/runtime_health"),
            "RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON": "{}",
            "RQUANT_LAB_RESOURCE_POLICY_VERSION": "lab-resource-v1",
            "RQUANT_LAB_TRADE_CALENDAR_PATH": ("/var/lib/rquant-serving/market-calendar.json"),
            "RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH": (
                "/etc/rquant/resource-authority.json"
            ),
            "RQUANT_RESOURCE_AUTHORITY_STATE_DIR": ("/var/lib/rquant-resource-authority"),
        }
        snapshot_provider = object()
        policy = object()
        service = object()
        captured: dict[str, object] = {}

        monkeypatch.setattr(
            authority_service,
            "load_closed_authority_environment",
            lambda *_args, **_kwargs: environment,
        )
        monkeypatch.setattr(
            authority_service,
            "load_resource_authority_daemon_configuration",
            lambda *_args, **_kwargs: configuration,
        )
        monkeypatch.setattr(
            adapter_module,
            "parse_resource_authority_adapter_config",
            lambda _payload: adapter,
        )
        monkeypatch.setattr(
            cli,
            "_build_lab_worker_resource_admission",
            lambda **_kwargs: SimpleNamespace(
                require_resource_admission=True,
                resource_snapshot_provider=snapshot_provider,
            ),
        )
        monkeypatch.setattr(
            admission_module,
            "admission_policy_for_version",
            lambda version: captured.setdefault("policy_version", version) or policy,
        )
        monkeypatch.setattr(
            authority_service,
            "compose_resource_authority_daemon",
            lambda **kwargs: captured.setdefault("composition", kwargs) or service,
        )
        monkeypatch.setattr(
            cli,
            "_serve_closed_unix_authority",
            lambda selected, *, label: captured.update(service=selected, label=label) or 0,
        )

        assert (
            cli.cmd_resource_authority_serve(
                argparse.Namespace(
                    config=Path("/etc/rquant/resource-authority.json"),
                    code_sha="1" * 40,
                )
            )
            == 0
        )
        assert captured["policy_version"] == "lab-resource-v1"
        composition = captured["composition"]
        assert isinstance(composition, dict)
        assert composition["configuration"] is configuration
        assert composition["snapshot_provider"] is snapshot_provider


class TestRuntimeRecoveryParser:
    def test_recovery_numeric_limits_do_not_materialize_argparse_choices(self) -> None:
        parser = build_parser()
        root_subparsers = next(
            action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
        )
        recovery_parser = root_subparsers.choices["runtime-recovery"]
        recovery_subparsers = next(
            action
            for action in recovery_parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        execute_parser = recovery_subparsers.choices["execute"]
        numeric_actions = {
            action.dest: action
            for action in execute_parser._actions
            if action.dest
            in {
                "deadline_seconds",
                "lease_seconds",
                "schedule_cycle_seconds",
                "max_attempts",
                "retry_delay_seconds",
            }
        }

        assert set(numeric_actions) == {
            "deadline_seconds",
            "lease_seconds",
            "schedule_cycle_seconds",
            "max_attempts",
            "retry_delay_seconds",
        }
        assert all(action.choices is None for action in numeric_actions.values())

    def test_backup_execute_requires_exact_plan_and_private_credential(self) -> None:
        plan_id = "a" * 64
        args = build_parser().parse_args(
            [
                "runtime-recovery-backup",
                "execute",
                "--config",
                "/tmp/recovery-backup.json",
                "--credential-file",
                "/tmp/recovery.key.json",
                "--plan-id",
                plan_id,
            ]
        )

        assert args.command == "runtime-recovery-backup"
        assert args.recovery_action == "execute"
        assert args.plan_id == plan_id

    def test_rehearsal_execute_has_bounded_deadline(self) -> None:
        args = build_parser().parse_args(
            [
                "runtime-recovery",
                "execute",
                "--publication-root",
                "/tmp/recovery-backups",
                "--state-path",
                "/tmp/recovery-state/state.sqlite3",
                "--receipt-root",
                "/tmp/recovery-state/receipts",
                "--restore-root",
                "/tmp/recovery-restore",
                "--credential-file",
                "/tmp/recovery.key.json",
                "--plan-id",
                "b" * 64,
                "--deadline-seconds",
                "900",
            ]
        )

        assert args.recovery_action == "execute"
        assert args.deadline_seconds == 900
        assert args.schedule_cycle_seconds is None

    def test_rehearsal_request_id_is_stable_within_one_systemd_cycle(self) -> None:
        from rquant.cli import _runtime_recovery_request_id

        manifest_id = "c" * 64
        first = datetime(2026, 8, 2, 3, 40, tzinfo=UTC)

        assert _runtime_recovery_request_id(
            manifest_id=manifest_id,
            now=first,
            schedule_cycle_seconds=604800,
        ) == _runtime_recovery_request_id(
            manifest_id=manifest_id,
            now=first + timedelta(hours=1),
            schedule_cycle_seconds=604800,
        )
        assert _runtime_recovery_request_id(
            manifest_id=manifest_id,
            now=first,
            schedule_cycle_seconds=604800,
        ) != _runtime_recovery_request_id(
            manifest_id=manifest_id,
            now=first + timedelta(days=8),
            schedule_cycle_seconds=604800,
        )

    def test_backup_dry_run_and_execute_emit_structured_contracts(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from rquant.cli import cmd_runtime_recovery_backup
        from rquant.runtime_recovery_backup import RecoveryBackupConfig
        from rquant.strict_json import canonical_json_bytes
        from tests.unit.test_runtime_recovery_backup import _config

        config_payload = _config(tmp_path).model_dump(
            mode="python",
            exclude={"config_id"},
        )
        config_payload["signer_key_id"] = "production-recovery-v1"
        config = RecoveryBackupConfig.model_validate(config_payload)
        config_path = tmp_path / "backup-config.json"
        config_path.write_bytes(canonical_json_bytes(config.model_dump(mode="json")))
        credential = tmp_path / "credential.json"
        credential.write_bytes(
            canonical_json_bytes({"key_id": "production-recovery-v1", "secret_hex": "ab" * 32})
        )
        credential.chmod(0o600)

        assert (
            cmd_runtime_recovery_backup(
                argparse.Namespace(
                    recovery_action="dry-run",
                    config=config_path,
                    credential_file=credential,
                )
            )
            == 0
        )
        preview = json.loads(capsys.readouterr().out)
        assert preview["artifact_count"] == len(config.artifacts)
        assert (
            cmd_runtime_recovery_backup(
                argparse.Namespace(
                    recovery_action="execute",
                    config=config_path,
                    credential_file=credential,
                    plan_id=preview["plan_id"],
                )
            )
            == 0
        )
        receipt = json.loads(capsys.readouterr().out)
        assert receipt["status"] == "succeeded"
        assert receipt["artifact_count"] == len(config.artifacts)

        from rquant.cli import cmd_runtime_recovery

        recovery_args = argparse.Namespace(
            recovery_action="dry-run",
            publication_root=Path(config.publication_root),
            state_path=tmp_path / "recovery-service" / "state.sqlite3",
            receipt_root=tmp_path / "recovery-service" / "receipts",
            restore_root=tmp_path / "rehearsal-restore",
            credential_file=credential,
            deadline_seconds=60,
            schedule_cycle_seconds=None,
            worker_id="cli-recovery-test",
            max_attempts=1,
            retry_delay_seconds=1,
        )
        assert cmd_runtime_recovery(recovery_args) == 0
        recovery_preview = json.loads(capsys.readouterr().out)
        recovery_args.recovery_action = "execute"
        recovery_args.plan_id = recovery_preview["plan_id"]

        assert cmd_runtime_recovery(recovery_args) == 0
        recovery_result = json.loads(capsys.readouterr().out)
        assert recovery_result["status"] == "succeeded"

    def test_recovery_cli_uses_artifact_key_from_trusted_rotation_set(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from rquant.cli import cmd_runtime_recovery, cmd_runtime_recovery_backup
        from rquant.runtime_recovery_backup import (
            RecoveryBackupAuthenticator,
            RecoveryBackupIntegrityError,
            RecoveryBackupProducer,
        )
        from rquant.strict_json import canonical_json_bytes
        from tests.unit.test_runtime_recovery_backup import _config

        config = _config(tmp_path)
        config_path = tmp_path / "backup-config.json"
        config_path.write_bytes(canonical_json_bytes(config.model_dump(mode="json")))
        old_credential = tmp_path / "old-credential.json"
        old_credential.write_bytes(
            canonical_json_bytes({"key_id": config.signer_key_id, "secret_hex": "ab" * 32})
        )
        old_credential.chmod(0o600)
        active_credential = tmp_path / "active-credential.json"
        active_credential.write_bytes(
            canonical_json_bytes({"key_id": "production-recovery-v2", "secret_hex": "cd" * 32})
        )
        active_credential.chmod(0o600)
        old_signer = RecoveryBackupAuthenticator.from_file(old_credential)
        producer = RecoveryBackupProducer(config=config, signer=old_signer)
        producer.execute(expected_plan_id=producer.preview().plan_id)
        monkeypatch.setenv("RQUANT_RECOVERY_TRUSTED_CREDENTIAL_FILES", str(old_credential))

        assert (
            cmd_runtime_recovery_backup(
                argparse.Namespace(
                    recovery_action="status",
                    config=config_path,
                    credential_file=active_credential,
                )
            )
            == 0
        )
        assert json.loads(capsys.readouterr().out)["status"] == "ready"
        recovery_args = argparse.Namespace(
            recovery_action="dry-run",
            publication_root=Path(config.publication_root),
            state_path=tmp_path / "recovery-service" / "state.sqlite3",
            receipt_root=tmp_path / "recovery-service" / "receipts",
            restore_root=tmp_path / "rehearsal-restore",
            credential_file=active_credential,
            deadline_seconds=60,
            schedule_cycle_seconds=None,
            worker_id="cli-rotation-test",
            max_attempts=1,
            retry_delay_seconds=1,
        )
        assert cmd_runtime_recovery(recovery_args) == 0
        preview = json.loads(capsys.readouterr().out)
        assert preview["status"] == "ready"
        recovery_args.recovery_action = "execute"
        recovery_args.plan_id = preview["plan_id"]
        assert cmd_runtime_recovery(recovery_args) == 0
        assert json.loads(capsys.readouterr().out)["status"] == "succeeded"

        monkeypatch.delenv("RQUANT_RECOVERY_TRUSTED_CREDENTIAL_FILES")
        with pytest.raises(RecoveryBackupIntegrityError, match="trusted|signature|key"):
            cmd_runtime_recovery(recovery_args)


class TestLimitUpPoolCommands:
    def test_repair_defaults_to_dry_run(self) -> None:
        args = build_parser().parse_args(["zt-pool-repair"])

        assert args.command == "zt-pool-repair"
        assert args.apply is False
        assert args.plan_id is None

    def test_repair_apply_requires_both_explicit_flags(self) -> None:
        plan_id = "a" * 64

        accepted = build_parser().parse_args(["zt-pool-repair", "--apply", "--plan-id", plan_id])
        assert accepted.apply is True
        assert accepted.plan_id == plan_id

        for incomplete in (
            ["zt-pool-repair", "--apply"],
            ["zt-pool-repair", "--plan-id", plan_id],
        ):
            with pytest.raises(SystemExit) as caught:
                build_parser().parse_args(incomplete)
            assert caught.value.code == 2

    def test_repair_rejects_non_sha256_plan_id(self) -> None:
        with pytest.raises(SystemExit) as caught:
            build_parser().parse_args(["zt-pool-repair", "--apply", "--plan-id", "not-a-plan"])

        assert caught.value.code == 2

    def test_repair_dry_run_then_explicit_apply(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
    ) -> None:
        import rquant.cli as cli
        from rquant.data_quality import (
            build_limit_up_pool_closed_day_repair_plan,
        )
        from rquant.storage.duckdb import DuckDBStore

        store = DuckDBStore(tmp_path / "repair-cli.duckdb")
        request.addfinalizer(store.close)
        closed_date = date(2026, 7, 12)
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO trade_calendar
            (exchange, cal_date, is_open, source, updated_at)
            VALUES ('SSE', ?, FALSE, 'test', now())
            """,
            [closed_date],
        )
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO limit_up_pool_daily (ts_code, trade_date, source)
            VALUES ('600001.SH', ?, 'eastmoney')
            """,
            [closed_date],
        )

        class _StoreContext:
            def __enter__(self) -> DuckDBStore:
                return store

            def __exit__(self, *_: object) -> None:
                return None

        monkeypatch.setattr(cli, "DuckDBStore", _StoreContext)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)

        assert cli.cmd_zt_pool_repair(SimpleNamespace(apply=False, plan_id=None)) == 0
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone() == (1,)
        plan = build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None

        assert cli.cmd_zt_pool_repair(SimpleNamespace(apply=True, plan_id=plan.plan_id)) == 0
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone() == (0,)
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone() == (1,)

    def test_repair_dry_run_returns_nonzero_when_calendar_is_unknown(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
    ) -> None:
        import rquant.cli as cli
        from rquant.storage.duckdb import DuckDBStore

        store = DuckDBStore(tmp_path / "repair-cli-unknown.duckdb")
        request.addfinalizer(store.close)
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO limit_up_pool_daily (ts_code, trade_date, source)
            VALUES ('600001.SH', DATE '2026-07-12', 'eastmoney')
            """
        )

        class _StoreContext:
            def __enter__(self) -> DuckDBStore:
                return store

            def __exit__(self, *_: object) -> None:
                return None

        monkeypatch.setattr(cli, "DuckDBStore", _StoreContext)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)

        assert cli.cmd_zt_pool_repair(SimpleNamespace(apply=False, plan_id=None)) == 1
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone() == (1,)
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone() == (0,)

    def test_repair_apply_returns_nonzero_for_empty_plan(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
    ) -> None:
        import rquant.cli as cli
        from rquant.data_quality import (
            build_limit_up_pool_closed_day_repair_plan,
        )
        from rquant.storage.duckdb import DuckDBStore

        store = DuckDBStore(tmp_path / "repair-cli-empty.duckdb")
        request.addfinalizer(store.close)
        plan = build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None and plan.before_count == 0

        class _StoreContext:
            def __enter__(self) -> DuckDBStore:
                return store

            def __exit__(self, *_: object) -> None:
                return None

        monkeypatch.setattr(cli, "DuckDBStore", _StoreContext)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)

        assert cli.cmd_zt_pool_repair(SimpleNamespace(apply=True, plan_id=plan.plan_id)) == 1
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone() == (0,)

    def test_capture_calendar_guard_returns_nonzero(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import rquant.cli as cli
        import rquant.limit_up_pool as limit_up_pool

        def blocked_capture(trade_date: date | None) -> int:
            assert trade_date == date(2026, 7, 3)
            raise limit_up_pool.LimitUpPoolCalendarGuardError(
                date(2026, 7, 3),
                stage="pre_fetch",
                detail="calendar unknown",
            )

        monkeypatch.setattr(limit_up_pool, "capture_zt_pool", blocked_capture)

        result = cli.cmd_zt_pool_capture(SimpleNamespace(date="2026-07-03"))

        assert result == 1

    def test_capture_business_conflict_uses_neutral_blocked_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import rquant.cli as cli
        import rquant.limit_up_pool as limit_up_pool

        def blocked_capture(trade_date: date | None) -> int:
            assert trade_date == date(2026, 7, 3)
            raise limit_up_pool.LimitUpPoolWriteConflictError(date(2026, 7, 3))

        errors: list[str] = []
        monkeypatch.setattr(limit_up_pool, "capture_zt_pool", blocked_capture)
        monkeypatch.setattr(cli, "setup_logging", lambda: None)
        monkeypatch.setattr(
            cli,
            "logger",
            SimpleNamespace(error=errors.append),
        )

        result = cli.cmd_zt_pool_capture(SimpleNamespace(date="2026-07-03"))

        assert result == 1
        assert len(errors) == 1
        assert "zt-pool-capture 被阻断" in errors[0]
        assert "交易日历" not in errors[0]


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _prepare_sync_script(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    project = tmp_path / "project"
    scripts_dir = project / "scripts"
    fake_bin = project / "fake-bin"
    scripts_dir.mkdir(parents=True)
    fake_bin.mkdir()
    (project / ".venv" / "bin").mkdir(parents=True)

    source = Path(__file__).resolve().parents[2] / "scripts" / "sync-from-cloud.sh"
    script = scripts_dir / "sync-from-cloud.sh"
    script.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    script.chmod(0o755)
    (project / ".env").write_text(
        "RQUANT_BACKUP_USER=test\n"
        "RQUANT_BACKUP_TOKEN=test-token\n"
        "RQUANT_BACKUP_URL=https://backup.invalid\n"
        "PUSHDEER_KEYS=test-key\n",
        encoding="utf-8",
    )

    calls = project / "calls.log"
    curl_calls = project / "curl-calls.log"
    _write_executable(
        project / ".venv" / "bin" / "rquant",
        """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "${SYNC_TEST_CALLS}"
if [[ -f "${SYNC_TEST_LOCK_PID_FILE}" ]]; then
    printf 'lock-pid:%s\\n' "$(cat "${SYNC_TEST_LOCK_PID_FILE}")" >> "${SYNC_TEST_CALLS}"
fi
if [[ -f "${SYNC_TEST_COMPLETION_FILE}" ]]; then
    printf 'marker-before:%s\\n' "$*" >> "${SYNC_TEST_CALLS}"
fi
case "${1:-}" in
    research-sync) exit "${RESEARCH_SYNC_EXIT:-0}" ;;
    zt-pool-capture) exit "${ZT_POOL_EXIT:-0}" ;;
    limit-list-backfill) exit "${LIMIT_LIST_EXIT:-0}" ;;
    data-backfill) exit "${DATA_BACKFILL_EXIT:-0}" ;;
    *) exit 0 ;;
esac
""",
    )
    _write_executable(
        fake_bin / "curl",
        """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "${SYNC_TEST_CURL_CALLS}"
args="$*"
output=""
while (( $# > 0 )); do
    if [[ "$1" == "-o" ]]; then
        output="$2"
        shift 2
    else
        shift
    fi
done
if [[ -n "${output}" ]]; then
    if [[ "${args}" == *"latest.json"* ]]; then
        printf '{"snapshot_at":"2026-07-14T09:00:00Z"}\\n' > "${output}"
    else
        printf 'compressed-placeholder\\n' > "${output}"
    fi
fi
if [[ "${args}" == *"-w %{http_code}"* ]]; then
    printf '200'
fi
""",
    )
    _write_executable(
        fake_bin / "gzip",
        """#!/usr/bin/env bash
printf 'duckdb-placeholder\\n'
""",
    )
    _write_executable(
        fake_bin / "mkdir",
        """#!/usr/bin/env bash
if [[ "${SYNC_TEST_MKDIR_FAIL_DATA:-0}" == "1" && "${1:-}" == "-p" ]]; then
    exit 1
fi
if [[ "${SYNC_TEST_MKDIR_FAIL_LOCK:-0}" == "1" && "${1:-}" == *".sync-from-cloud.lock" ]]; then
    exit 1
fi
exec /bin/mkdir "$@"
""",
    )
    _write_executable(
        fake_bin / "sleep",
        """#!/usr/bin/env bash
if [[ -n "${SYNC_TEST_PUBLISH_LOCK_PID_ON_SLEEP:-}" ]]; then
    publish_tmp="${SYNC_TEST_LOCK_PID_FILE}.fake-publisher"
    printf '%s\n' "${SYNC_TEST_PUBLISH_LOCK_PID_ON_SLEEP}" > "${publish_tmp}"
    /bin/mv "${publish_tmp}" "${SYNC_TEST_LOCK_PID_FILE}"
fi
""",
    )
    _write_executable(
        fake_bin / "mv",
        """#!/usr/bin/env bash
if [[ "${2:-}" == "${SYNC_TEST_LOCK_PID_FILE}" ]]; then
    destination_before="missing"
    if [[ -e "${SYNC_TEST_LOCK_PID_FILE}" ]]; then
        destination_before=$(cat "${SYNC_TEST_LOCK_PID_FILE}")
    fi
    printf 'destination-before:%s\n' "${destination_before}" \
        >> "${SYNC_TEST_LOCK_PUBLISH_CALLS}"
    printf 'source:%s\n' "$(cat "${1}")" \
        >> "${SYNC_TEST_LOCK_PUBLISH_CALLS}"
fi
exec /bin/mv "$@"
""",
    )
    _write_executable(
        fake_bin / "date",
        """#!/usr/bin/env bash
if [[ "${SYNC_TEST_DAILY_WINDOW:-0}" != "1" ]]; then
    exec /bin/date "$@"
fi
case "${1:-}" in
    +%H) printf '17\\n' ;;
    +%M) printf '20\\n' ;;
    +%u) printf '2\\n' ;;
    '+%Y-%m-%d') printf '2026-07-14\\n' ;;
    +%s) printf '1784011200\\n' ;;
    '+%Y-%m-%d %H:%M:%S') printf '2026-07-14 17:20:00\\n' ;;
    *) exec /bin/date "$@" ;;
esac
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["SYNC_TEST_CALLS"] = str(calls)
    env["SYNC_TEST_CURL_CALLS"] = str(curl_calls)
    env["SYNC_TEST_COMPLETION_FILE"] = str(project / "data" / ".last-research-sync-date")
    env["SYNC_TEST_LOCK_PID_FILE"] = str(project / "data" / ".sync-from-cloud.lock" / "pid")
    env["SYNC_TEST_LOCK_PUBLISH_CALLS"] = str(project / "lock-publish.log")
    return script, env, calls


def _rquant_command_calls(path: Path) -> list[str]:
    return [
        call
        for call in path.read_text(encoding="utf-8").splitlines()
        if not call.startswith(("lock-pid:", "marker-before:"))
    ]


class TestSyncFromCloudFlags:
    @pytest.mark.parametrize(
        "args",
        [
            ["--force", "--skip-post-sync-captures"],
            ["--skip-post-sync-captures", "--force"],
        ],
    )
    def test_skip_post_sync_captures_accepts_either_order(
        self,
        tmp_path: Path,
        args: list[str],
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        env["SYNC_TEST_DAILY_WINDOW"] = "1"

        result = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stdout + result.stderr
        calls = _rquant_command_calls(calls_path)
        assert calls == [
            f"research-sync --backup {script.parents[1] / 'data' / 'cloud_backup.duckdb'}"
        ]
        assert "baseline snapshot_at=" in result.stdout
        assert Path(env["SYNC_TEST_COMPLETION_FILE"]).read_text().strip() == "2026-07-14"

    def test_force_without_skip_runs_all_post_sync_captures(self, tmp_path: Path) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        env["SYNC_TEST_DAILY_WINDOW"] = "1"

        result = subprocess.run(
            ["bash", str(script), "--force"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stdout + result.stderr
        raw_calls = calls_path.read_text(encoding="utf-8").splitlines()
        assert _rquant_command_calls(calls_path) == [
            f"research-sync --backup {script.parents[1] / 'data' / 'cloud_backup.duckdb'}",
            "zt-pool-capture",
            "limit-list-backfill --today",
            "data-backfill --dataset kpl_concept --today",
        ]
        assert not any(call.startswith("marker-before:") for call in raw_calls)
        assert Path(env["SYNC_TEST_COMPLETION_FILE"]).read_text().strip() == "2026-07-14"

    def test_zt_pool_failure_alerts_stays_nonzero_and_does_not_mark_complete(
        self,
        tmp_path: Path,
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        env["SYNC_TEST_DAILY_WINDOW"] = "1"
        env["ZT_POOL_EXIT"] = "1"

        result = subprocess.run(
            ["bash", str(script), "--force"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode != 0
        assert _rquant_command_calls(calls_path) == [
            f"research-sync --backup {script.parents[1] / 'data' / 'cloud_backup.duckdb'}",
            "zt-pool-capture",
            "limit-list-backfill --today",
            "data-backfill --dataset kpl_concept --today",
        ]
        assert not Path(env["SYNC_TEST_COMPLETION_FILE"]).exists()
        curl_calls = Path(env["SYNC_TEST_CURL_CALLS"]).read_text(encoding="utf-8")
        assert "-X POST" in curl_calls

    def test_unknown_argument_exits_2(self, tmp_path: Path) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)

        result = subprocess.run(
            ["bash", str(script), "--unknown"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 2
        assert not calls_path.exists()

    @pytest.mark.parametrize(
        ("args", "expected_code"),
        [([], 0), (["--force"], 75)],
    )
    def test_active_lock_uses_normal_or_force_contention_code(
        self,
        tmp_path: Path,
        args: list[str],
        expected_code: int,
    ) -> None:
        script, env, _ = _prepare_sync_script(tmp_path)
        lock_dir = script.parents[1] / "data" / ".sync-from-cloud.lock"
        lock_dir.mkdir(parents=True)
        pid_file = lock_dir / "pid"
        pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")

        result = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == expected_code
        assert pid_file.read_text(encoding="utf-8").strip() == str(os.getpid())

    @pytest.mark.parametrize(
        ("args", "expected_code"),
        [([], 0), (["--force"], 75)],
    )
    def test_pending_pid_publication_becomes_active_lock_without_removal(
        self,
        tmp_path: Path,
        args: list[str],
        expected_code: int,
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        lock_dir = script.parents[1] / "data" / ".sync-from-cloud.lock"
        lock_dir.mkdir(parents=True)
        pid_file = lock_dir / "pid"
        env["SYNC_TEST_DAILY_WINDOW"] = "1"
        env["SYNC_TEST_PUBLISH_LOCK_PID_ON_SLEEP"] = str(os.getpid())

        result = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == expected_code, result.stdout + result.stderr
        assert pid_file.read_text(encoding="utf-8").strip() == str(os.getpid())
        assert not calls_path.exists()

    @pytest.mark.parametrize(
        ("args", "expected_code"),
        [([], 0), (["--force"], 75)],
    )
    def test_empty_pid_is_waited_for_then_atomically_replaced_by_active_owner(
        self,
        tmp_path: Path,
        args: list[str],
        expected_code: int,
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        lock_dir = script.parents[1] / "data" / ".sync-from-cloud.lock"
        lock_dir.mkdir(parents=True)
        pid_file = lock_dir / "pid"
        pid_file.write_text("", encoding="utf-8")
        env["SYNC_TEST_DAILY_WINDOW"] = "1"
        env["SYNC_TEST_PUBLISH_LOCK_PID_ON_SLEEP"] = str(os.getpid())

        result = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == expected_code, result.stdout + result.stderr
        assert pid_file.read_text(encoding="utf-8").strip() == str(os.getpid())
        assert not calls_path.exists()

    @pytest.mark.parametrize(
        ("args", "expected_code"),
        [([], 0), (["--force"], 75)],
    )
    def test_live_interrupted_pid_publication_is_preserved_as_active_lock(
        self,
        tmp_path: Path,
        args: list[str],
        expected_code: int,
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        lock_dir = script.parents[1] / "data" / ".sync-from-cloud.lock"
        lock_dir.mkdir(parents=True)
        interrupted_pid = lock_dir / "pid.tmp.interrupted"
        interrupted_pid.write_text(f"{os.getpid()}\n", encoding="utf-8")
        env["SYNC_TEST_DAILY_WINDOW"] = "1"

        result = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == expected_code, result.stdout + result.stderr
        assert interrupted_pid.read_text(encoding="utf-8").strip() == str(os.getpid())
        assert not calls_path.exists()

    def test_dead_interrupted_pid_publication_is_recovered_once(
        self,
        tmp_path: Path,
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        lock_dir = script.parents[1] / "data" / ".sync-from-cloud.lock"
        lock_dir.mkdir(parents=True)
        (lock_dir / "pid.tmp.interrupted").write_text(
            "99999999\n",
            encoding="utf-8",
        )

        result = subprocess.run(
            ["bash", str(script), "--force", "--skip-post-sync-captures"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stdout + result.stderr
        assert _rquant_command_calls(calls_path) == [
            f"research-sync --backup {script.parents[1] / 'data' / 'cloud_backup.duckdb'}"
        ]
        assert not lock_dir.exists()

    @pytest.mark.parametrize("stale_pid", ["invalid", "99999999"])
    def test_stale_or_invalid_pid_lock_is_recovered(
        self,
        tmp_path: Path,
        stale_pid: str,
    ) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)
        lock_dir = script.parents[1] / "data" / ".sync-from-cloud.lock"
        lock_dir.mkdir(parents=True)
        (lock_dir / "pid").write_text(f"{stale_pid}\n", encoding="utf-8")

        result = subprocess.run(
            ["bash", str(script), "--force", "--skip-post-sync-captures"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stdout + result.stderr
        assert "stale lock" in result.stdout
        assert (
            calls_path.read_text(encoding="utf-8")
            .splitlines()[0]
            .startswith("research-sync --backup")
        )
        assert not lock_dir.exists()

    def test_data_directory_creation_failure_exits_1(self, tmp_path: Path) -> None:
        script, env, _ = _prepare_sync_script(tmp_path)
        env["SYNC_TEST_MKDIR_FAIL_DATA"] = "1"

        result = subprocess.run(
            ["bash", str(script), "--force"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "failed to create data/log directories" in result.stderr

    def test_lock_mkdir_permission_error_exits_1(self, tmp_path: Path) -> None:
        script, env, _ = _prepare_sync_script(tmp_path)
        env["SYNC_TEST_MKDIR_FAIL_LOCK"] = "1"

        result = subprocess.run(
            ["bash", str(script), "--force"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "failed to create sync lock" in result.stdout

    def test_owned_lock_writes_pid_and_is_removed_on_exit(self, tmp_path: Path) -> None:
        script, env, calls_path = _prepare_sync_script(tmp_path)

        result = subprocess.run(
            ["bash", str(script), "--force", "--skip-post-sync-captures"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stdout + result.stderr
        lock_observations = [
            call
            for call in calls_path.read_text(encoding="utf-8").splitlines()
            if call.startswith("lock-pid:")
        ]
        assert len(lock_observations) == 1
        assert lock_observations[0].removeprefix("lock-pid:").isdigit()
        publish_observations = (
            Path(env["SYNC_TEST_LOCK_PUBLISH_CALLS"]).read_text(encoding="utf-8").splitlines()
        )
        assert publish_observations == [
            "destination-before:missing",
            f"source:{lock_observations[0].removeprefix('lock-pid:')}",
        ]
        assert not Path(env["SYNC_TEST_LOCK_PID_FILE"]).parent.exists()

    def test_research_sync_failure_remains_nonzero_after_stale_check(
        self,
        tmp_path: Path,
    ) -> None:
        script, env, _ = _prepare_sync_script(tmp_path)
        env["RESEARCH_SYNC_EXIT"] = "1"

        result = subprocess.run(
            ["bash", str(script), "--force", "--skip-post-sync-captures"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode != 0
        assert "merge FAILED" in result.stdout


class TestCmdMarketDailyBackfill:
    def test_remote_backfill_finishes_before_recompute_writer_opens(
        self,
        monkeypatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant.cli import cmd_market_daily_backfill

        writer_active = False
        events: list[str] = []

        class _Store:
            def __enter__(self):
                nonlocal writer_active
                assert writer_active is False
                writer_active = True
                events.append("writer_enter")
                return self

            def __exit__(self, *_: object) -> None:
                nonlocal writer_active
                writer_active = False
                events.append("writer_exit")

        store_factory = MagicMock(side_effect=_Store)
        adapter = object()

        def fake_backfill(*args, **kwargs):
            assert writer_active is False
            assert args == ("2020-01-01", "2020-01-02", adapter)
            assert kwargs["store_factory"] is store_factory
            events.append("remote_backfill")
            return {
                "failed_dates": [],
                "affected_codes": ["600000.SH"],
                "state_tail_start_date": "2020-01-01",
            }

        def fake_recompute(store, *, codes, start_date, status_mode):
            assert writer_active is True
            assert isinstance(store, _Store)
            assert codes == ["600000.SH"]
            assert start_date == date(2020, 1, 1)
            assert status_mode == "verified_no_fetch"
            events.append("recompute")
            return 1

        monkeypatch.setattr("rquant.cli.DuckDBStore", store_factory)
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr(
            "rquant.market_backfill.backfill_market_daily",
            fake_backfill,
        )
        monkeypatch.setattr(
            "rquant.market_backfill.recompute_daily_state",
            fake_recompute,
        )
        args = MagicMock(
            start_date="2020-01-01",
            end_date="2020-01-02",
            dry_run=False,
            skip_state_recompute=False,
        )

        result = cmd_market_daily_backfill(args)

        assert result == 0
        assert events == [
            "remote_backfill",
            "writer_enter",
            "recompute",
            "writer_exit",
        ]

    def test_skip_state_recompute_leaves_invalidated_state_without_rebuild(
        self,
        monkeypatch,
    ) -> None:
        from unittest.mock import MagicMock

        from rquant.cli import cmd_market_daily_backfill

        store_factory = MagicMock()
        recompute = MagicMock()
        monkeypatch.setattr("rquant.cli.DuckDBStore", store_factory)
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=object()),
        )
        monkeypatch.setattr(
            "rquant.market_backfill.backfill_market_daily",
            MagicMock(
                return_value={
                    "failed_dates": [],
                    "affected_codes": ["600000.SH"],
                }
            ),
        )
        monkeypatch.setattr(
            "rquant.market_backfill.recompute_daily_state",
            recompute,
        )

        result = cmd_market_daily_backfill(
            MagicMock(
                start_date="2020-01-01",
                end_date="2020-01-02",
                dry_run=False,
                skip_state_recompute=True,
            )
        )

        assert result == 0
        store_factory.assert_not_called()
        recompute.assert_not_called()

    def test_parser_names_state_recompute_and_keeps_hidden_legacy_alias(
        self,
    ) -> None:
        parser = build_parser()
        required = [
            "market-daily-backfill",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2020-01-02",
        ]

        renamed = parser.parse_args([*required, "--skip-state-recompute"])
        legacy = parser.parse_args([*required, "--skip-state"])

        assert renamed.skip_state_recompute is True
        assert legacy.skip_state_recompute is True

    def test_market_backfill_help_explains_tail_invalidation(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "rquant.cli",
                "market-daily-backfill",
                "--help",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "--skip-state-recompute" in result.stdout
        assert "原子尾段重算" in result.stdout


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


class TestMiddayBriefingParser:
    def test_morning_pulse_defaults(self) -> None:
        args = build_parser().parse_args(["morning-pulse"])
        assert args.command == "morning-pulse"
        assert args.slot is None
        assert not args.force
        assert not args.dry_run

    def test_morning_pulse_args(self) -> None:
        args = build_parser().parse_args(
            ["morning-pulse", "--slot", "10:30", "--force", "--dry-run"]
        )
        assert args.slot == "10:30"
        assert args.force
        assert args.dry_run

    def test_midday_report_args(self) -> None:
        args = build_parser().parse_args(["midday-report", "--date", "2026-07-06", "--dry-run"])
        assert args.command == "midday-report"
        assert args.date == "2026-07-06"
        assert args.dry_run


class TestRtMinuteFetchParser:
    def test_rt_minute_fetch_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "rt-minute-fetch",
                "--ts-code",
                "605366.SH,301051.SZ",
            ]
        )
        assert args.command == "rt-minute-fetch"
        assert args.ts_code == ["605366.SH,301051.SZ"]
        assert args.freq == "1min"

    def test_rt_minute_fetch_accepts_repeated_codes(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "rt-minute-fetch",
                "--ts-code",
                "605366.SH",
                "--ts-code",
                "301051.SZ",
                "--freq",
                "5min",
            ]
        )
        assert args.ts_code == ["605366.SH", "301051.SZ"]
        assert args.freq == "5min"


class TestRtMinuteDailyFetchParser:
    def test_rt_minute_daily_fetch_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "rt-minute-daily-fetch",
                "--ts-code",
                "605366.SH,301051.SZ",
            ]
        )
        assert args.command == "rt-minute-daily-fetch"
        assert args.ts_code == ["605366.SH,301051.SZ"]
        assert args.freq == "1min"

    def test_rt_minute_daily_fetch_accepts_repeated_codes(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "rt-minute-daily-fetch",
                "--ts-code",
                "605366.SH",
                "--ts-code",
                "301051.SZ",
                "--freq",
                "5min",
            ]
        )
        assert args.ts_code == ["605366.SH", "301051.SZ"]
        assert args.freq == "5min"


class TestGrowthBoardSurgeReplayParser:
    def test_growth_board_surge_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "growth-board-surge-replay",
                "--start-date",
                "2026-06-25",
                "--end-date",
                "2026-06-26",
            ]
        )
        assert args.command == "growth-board-surge-replay"
        assert args.freq == "1min"
        assert args.min_signal_time == "09:30"
        assert args.lookback_days == 20
        assert args.min_hist_days == 10
        assert args.min_cum_amount_ratio == 1.4
        assert args.min_same_minute_amount_ratio == 2.0
        assert args.max_hold_days == 3
        assert args.require_inner_outer is False
        assert args.max_inner_outer_ratio == 1.0
        assert args.require_large_net_vol is False
        assert args.min_large_net_vol == 0.0
        assert args.factor_confirm is False
        assert args.factor_score_threshold == 45.0
        assert args.output is None

    def test_growth_board_surge_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "growth-board-surge-replay",
                "--start-date",
                "2026-06-25",
                "--end-date",
                "2026-06-26",
                "--freq",
                "5min",
                "--min-signal-time",
                "09:35",
                "--lookback-days",
                "30",
                "--min-hist-days",
                "15",
                "--min-cum-amount-ratio",
                "1.8",
                "--min-same-minute-amount-ratio",
                "3.0",
                "--max-hold-days",
                "2",
                "--require-inner-outer",
                "--min-inner-outer-ratio",
                "1.2",
                "--require-large-net-vol",
                "--min-large-net-vol",
                "100",
                "--factor-confirm",
                "--factor-score-threshold",
                "50",
                "--output",
                "/tmp/growth.csv",
            ]
        )
        assert args.freq == "5min"
        assert args.min_signal_time == "09:35"
        assert args.lookback_days == 30
        assert args.min_hist_days == 15
        assert args.min_cum_amount_ratio == 1.8
        assert args.min_same_minute_amount_ratio == 3.0
        assert args.max_hold_days == 2
        assert args.require_inner_outer is True
        assert args.max_inner_outer_ratio == 1.2
        assert args.require_large_net_vol is True
        assert args.min_large_net_vol == 100.0
        assert args.factor_confirm is True
        assert args.factor_score_threshold == 50.0
        assert args.output == "/tmp/growth.csv"


class TestMoneyflowBackfillParser:
    def test_moneyflow_backfill_requires_date(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["moneyflow-backfill", "--date", "2026-06-26"])
        assert args.command == "moneyflow-backfill"
        assert args.date == "2026-06-26"


class TestMinuteBackfillParser:
    def test_minute_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["minute-backfill", "--date", "2026-06-24"])
        assert args.command == "minute-backfill"
        assert args.date == "2026-06-24"
        assert args.lookback_days == 90
        assert args.freq == "1min"
        assert args.preset == "n-shape-pool1"
        assert args.ts_code is None
        assert not args.dry_run

    def test_minute_backfill_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "minute-backfill",
                "--date",
                "2026-06-24",
                "--lookback-days",
                "90",
                "--freq",
                "5min",
                "--preset",
                "n-shape-pool2",
                "--ts-code",
                "600000.SH",
                "--dry-run",
            ]
        )
        assert args.lookback_days == 90
        assert args.freq == "5min"
        assert args.preset == "n-shape-pool2"
        assert args.ts_code == "600000.SH"
        assert args.dry_run


class TestMinuteReplayParser:
    def test_minute_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "minute-replay",
                "--start-date",
                "2026-06-01",
                "--end-date",
                "2026-06-24",
            ]
        )
        assert args.command == "minute-replay"
        assert args.start_date == "2026-06-01"
        assert args.end_date == "2026-06-24"
        assert args.preset == "n-shape-pool1"
        assert args.freq == "1min"
        assert args.entry_mode == "first_break"
        assert args.factor_score_threshold == 35.0
        assert args.max_hold_days == 5
        assert not args.volume_profile
        assert args.volume_profile_lookbacks == [90]
        assert args.output is None

    def test_minute_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "minute-replay",
                "--start-date",
                "2026-06-01",
                "--end-date",
                "2026-06-24",
                "--preset",
                "n-shape-pool2",
                "--freq",
                "5min",
                "--entry-mode",
                "amount_surge",
                "--max-hold-days",
                "3",
                "--volume-profile",
                "--volume-profile-lookbacks",
                "90",
                "--output",
                "/private/tmp/replay.csv",
            ]
        )
        assert args.preset == "n-shape-pool2"
        assert args.freq == "5min"
        assert args.entry_mode == "amount_surge"
        assert args.max_hold_days == 3
        assert args.volume_profile
        assert args.volume_profile_lookbacks == [90]
        assert args.output == "/private/tmp/replay.csv"

    def test_minute_replay_factor_confirm_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "minute-replay",
                "--start-date",
                "2026-06-01",
                "--end-date",
                "2026-06-24",
                "--entry-mode",
                "factor_confirm",
                "--factor-score-threshold",
                "65",
            ]
        )
        assert args.entry_mode == "factor_confirm"
        assert args.factor_score_threshold == 65.0


class TestMinuteReplayBackfillParser:
    def test_minute_replay_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "minute-replay-backfill",
                "--start-date",
                "2026-06-01",
                "--end-date",
                "2026-06-24",
            ]
        )
        assert args.command == "minute-replay-backfill"
        assert args.start_date == "2026-06-01"
        assert args.end_date == "2026-06-24"
        assert args.preset == "n-shape-pool1"
        assert args.freq == "1min"
        assert args.max_hold_days == 5
        assert args.ts_code is None
        assert not args.dry_run

    def test_minute_replay_backfill_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "minute-replay-backfill",
                "--start-date",
                "2026-06-01",
                "--end-date",
                "2026-06-24",
                "--preset",
                "n-shape-pool2",
                "--freq",
                "5min",
                "--max-hold-days",
                "3",
                "--ts-code",
                "600000.SH",
                "--dry-run",
            ]
        )
        assert args.preset == "n-shape-pool2"
        assert args.freq == "5min"
        assert args.max_hold_days == 3
        assert args.ts_code == "600000.SH"
        assert args.dry_run


class TestAuctionBackfillParser:
    def test_auction_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-backfill",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-02-18",
            ]
        )
        assert args.command == "auction-backfill"
        assert args.start_date == "2025-01-01"
        assert args.end_date == "2025-02-18"
        assert not args.dry_run

    def test_auction_backfill_dry_run(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-backfill",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-02-18",
                "--dry-run",
            ]
        )
        assert args.dry_run


class TestAuctionMinuteFallbackParser:
    def test_auction_minute_fallback_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-minute-fallback",
                "--date",
                "2026-06-26",
            ]
        )
        assert args.command == "auction-minute-fallback"
        assert args.date == "2026-06-26"
        assert not args.dry_run


class TestAuctionGapReplayParser:
    def test_auction_gap_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-gap-replay",
                "--start-date",
                "2025-01-16",
                "--end-date",
                "2026-06-24",
            ]
        )
        assert args.command == "auction-gap-replay"
        assert args.start_date == "2025-01-16"
        assert args.end_date == "2026-06-24"
        assert args.gap_mode == "close"
        assert args.st_filter == "case_insensitive"
        assert args.min_ratio == 0.15
        assert args.max_ratio == 5.0
        assert args.output is None

    def test_auction_gap_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-gap-replay",
                "--start-date",
                "2025-01-16",
                "--end-date",
                "2026-06-24",
                "--gap-mode",
                "strict_high",
                "--st-filter",
                "literal_lower",
                "--min-ratio",
                "0.2",
                "--max-ratio",
                "2",
                "--output",
                "/private/tmp/auction-gap.csv",
            ]
        )
        assert args.gap_mode == "strict_high"
        assert args.st_filter == "literal_lower"
        assert args.min_ratio == 0.2
        assert args.max_ratio == 2.0
        assert args.output == "/private/tmp/auction-gap.csv"


class TestAuctionGapMinuteReplayParser:
    def test_auction_gap_minute_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-gap-minute-replay",
                "--start-date",
                "2025-01-16",
                "--end-date",
                "2026-06-24",
            ]
        )
        assert args.command == "auction-gap-minute-replay"
        assert args.start_date == "2025-01-16"
        assert args.end_date == "2026-06-24"
        assert args.gap_mode == "close"
        assert args.st_filter == "case_insensitive"
        assert args.min_ratio == 0.15
        assert args.max_ratio == 5.0
        assert args.max_hold_days == 1
        assert args.seal_hold_days is None
        assert args.seal_hold_max_open_times == 0
        assert args.factor_score_threshold is None
        assert args.output is None

    def test_auction_gap_minute_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-gap-minute-replay",
                "--start-date",
                "2025-01-16",
                "--end-date",
                "2026-06-24",
                "--gap-mode",
                "strict_high",
                "--st-filter",
                "literal_lower",
                "--min-ratio",
                "0.2",
                "--max-ratio",
                "2",
                "--max-hold-days",
                "2",
                "--seal-hold-days",
                "3",
                "--seal-hold-max-open-times",
                "1",
                "--factor-score-threshold",
                "45",
                "--output",
                "/private/tmp/auction-gap-minute.csv",
            ]
        )
        assert args.gap_mode == "strict_high"
        assert args.st_filter == "literal_lower"
        assert args.min_ratio == 0.2
        assert args.max_ratio == 2.0
        assert args.max_hold_days == 2
        assert args.seal_hold_days == 3
        assert args.seal_hold_max_open_times == 1
        assert args.factor_score_threshold == 45.0
        assert args.output == "/private/tmp/auction-gap-minute.csv"


class TestAuctionGapMinuteBackfillParser:
    def test_auction_gap_minute_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "auction-gap-minute-backfill",
                "--start-date",
                "2025-01-16",
                "--end-date",
                "2026-06-24",
            ]
        )
        assert args.command == "auction-gap-minute-backfill"
        assert args.start_date == "2025-01-16"
        assert args.end_date == "2026-06-24"
        assert args.gap_mode == "close"
        assert args.st_filter == "case_insensitive"
        assert args.max_hold_days == 1
        assert not args.dry_run


class TestCmdAuctionGapReplay:
    def test_uses_readonly_store(self, monkeypatch, tmp_path) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_auction_gap_replay

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        calls = []
        monkeypatch.setattr(
            "rquant.cli.open_readonly_store",
            lambda **kwargs: calls.append(kwargs) or store,
        )
        replay_mock = MagicMock(return_value=pd.DataFrame())
        monkeypatch.setattr(
            "rquant.auction_gap_strategy.run_auction_gap_replay",
            replay_mock,
        )

        args = MagicMock(
            start_date="2025-01-16",
            end_date="2026-06-24",
            persist_positions=False,
            run_id=None,
            gap_mode="close",
            min_ratio=0.15,
            max_ratio=5.0,
            st_filter="case_insensitive",
            output=None,
        )

        rc = cmd_auction_gap_replay(args)

        assert rc == 0
        assert calls == [
            {
                "required_tables": [
                    "auction_bar",
                    "daily_bar",
                    "daily_state",
                    "stock_status_daily",
                ]
            }
        ]
        replay_mock.assert_called_once()
        assert replay_mock.call_args.args[0] is store


class TestCmdAuctionGapMinuteReplay:
    def test_uses_readonly_store_with_minute_table(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_auction_gap_minute_replay

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        calls = []
        monkeypatch.setattr(
            "rquant.cli.open_readonly_store",
            lambda **kwargs: calls.append(kwargs) or store,
        )
        candidate_mock = MagicMock(return_value=pd.DataFrame({"ts_code": ["600000.SH"]}))
        replay_mock = MagicMock(return_value=pd.DataFrame())
        monkeypatch.setattr(
            "rquant.auction_gap_strategy.run_auction_gap_replay",
            candidate_mock,
        )
        monkeypatch.setattr(
            "rquant.auction_gap_strategy.run_auction_gap_minute_replay",
            replay_mock,
        )

        args = MagicMock(
            start_date="2025-01-16",
            end_date="2026-06-24",
            persist_positions=False,
            run_id=None,
            gap_mode="close",
            min_ratio=0.15,
            max_ratio=5.0,
            st_filter="case_insensitive",
            max_hold_days=1,
            seal_hold_days=None,
            seal_hold_max_open_times=0,
            output=None,
        )

        rc = cmd_auction_gap_minute_replay(args)

        assert rc == 0
        assert calls == [
            {
                "required_tables": [
                    "auction_bar",
                    "daily_bar",
                    "daily_state",
                    "minute_bar",
                    "stock_status_daily",
                ]
            }
        ]
        candidate_mock.assert_called_once()
        replay_mock.assert_called_once()


class TestCmdAuctionGapMinuteBackfill:
    def test_writes_main_store(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        from rquant.cli import cmd_auction_gap_minute_backfill

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        adapter = MagicMock()
        summary = MagicMock(failed_requests=0)
        summary.model_dump.return_value = {"planned_requests": 1}
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        backfill_mock = MagicMock(return_value=summary)
        monkeypatch.setattr(
            "rquant.intraday_backfill.backfill_auction_gap_minute_replay_window",
            backfill_mock,
        )

        args = MagicMock(
            start_date="2025-01-16",
            end_date="2026-06-24",
            persist_positions=False,
            run_id=None,
            gap_mode="close",
            min_ratio=0.15,
            max_ratio=5.0,
            st_filter="case_insensitive",
            max_hold_days=1,
            freq="1min",
            ts_code=None,
            dry_run=False,
        )

        rc = cmd_auction_gap_minute_backfill(args)

        assert rc == 0
        backfill_mock.assert_called_once()


class TestCmdAuctionMinuteFallback:
    def test_writes_fallback_rows(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        from rquant.cli import cmd_auction_minute_fallback

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        summary = MagicMock()
        summary.model_dump.return_value = {"rows_written": 1}

        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))
        fallback_mock = MagicMock(return_value=summary)
        monkeypatch.setattr(
            "rquant.auction_backfill.synthesize_open_auction_from_minute",
            fallback_mock,
        )

        args = MagicMock(date="2026-06-26", dry_run=False)
        rc = cmd_auction_minute_fallback(args)

        assert rc == 0
        fallback_mock.assert_called_once_with(
            store,
            "2026-06-26",
            dry_run=False,
        )


class TestCmdRtMinuteFetch:
    def test_fetches_and_writes_minute_bar(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_rt_minute_fetch

        adapter = MagicMock()
        adapter.rt_min.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "605366.SH",
                    "trade_time": pd.Timestamp("2026-07-01 15:00:00"),
                    "freq": "1min",
                    "open": 12.85,
                    "high": 12.85,
                    "low": 12.85,
                    "close": 12.85,
                    "vol": 1110200.0,
                    "amount": 14266070.0,
                    "source": "tushare_rt",
                }
            ]
        )
        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        store.upsert_minute_bars.return_value = 1

        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))

        args = MagicMock(ts_code=["605366.SH,301051.SZ"], freq="1min")
        rc = cmd_rt_minute_fetch(args)

        assert rc == 0
        adapter.rt_min.assert_called_once_with(
            ["605366.SH", "301051.SZ"],
            freq="1min",
        )
        store.upsert_minute_bars.assert_called_once()


class TestCmdRtMinuteDailyFetch:
    def test_fetches_and_writes_open_to_now_minute_bar(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_rt_minute_daily_fetch

        adapter = MagicMock()
        adapter.rt_min_daily.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "605366.SH",
                    "trade_time": pd.Timestamp("2026-07-01 09:30:00"),
                    "freq": "1min",
                    "open": 12.80,
                    "high": 12.80,
                    "low": 12.80,
                    "close": 12.80,
                    "vol": 777300.0,
                    "amount": 9949440.0,
                    "source": "tushare_rt_daily",
                }
            ]
        )
        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        store.upsert_minute_bars.return_value = 1

        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))

        args = MagicMock(ts_code=["605366.SH,301051.SZ"], freq="1min")
        rc = cmd_rt_minute_daily_fetch(args)

        assert rc == 0
        adapter.rt_min_daily.assert_called_once_with(
            ["605366.SH", "301051.SZ"],
            freq="1min",
        )
        store.upsert_minute_bars.assert_called_once()


class TestCmdMoneyflowBackfill:
    def test_fetches_and_writes_moneyflow_daily(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_moneyflow_backfill

        adapter = MagicMock()
        adapter.moneyflow.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "trade_date": pd.Timestamp("2026-06-26").date(),
                    "buy_lg_vol": 1200.0,
                    "sell_lg_vol": 700.0,
                    "buy_elg_vol": 500.0,
                    "sell_elg_vol": 100.0,
                    "large_net_vol": 900.0,
                    "large_net_amount": 1234.56,
                    "source": "tushare",
                }
            ]
        )
        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        store.upsert_moneyflow_daily.return_value = 1

        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))

        args = MagicMock(date="2026-06-26")
        rc = cmd_moneyflow_backfill(args)

        assert rc == 0
        adapter.moneyflow.assert_called_once()
        store.upsert_moneyflow_daily.assert_called_once()


class TestCmdGrowthBoardSurgeReplay:
    def test_runs_replay_with_readonly_store(self, monkeypatch) -> None:
        from datetime import time
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_growth_board_surge_replay

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        trades = pd.DataFrame(
            [
                {
                    "signal_date": "2026-06-25",
                    "ts_code": "300001.SZ",
                    "name": "创业样本",
                    "entry_time": "2026-06-25 09:34:00",
                    "entry_price": 10.8,
                    "exit_time": "2026-06-26 15:00:00",
                    "exit_price": 11.6,
                    "exit_reason": "time_1d",
                    "ret_pct": 7.4074,
                }
            ]
        )

        open_store = MagicMock(return_value=store)
        replay = MagicMock(return_value=trades)
        monkeypatch.setattr("rquant.cli.open_readonly_store", open_store)
        monkeypatch.setattr(
            "rquant.growth_board_surge_strategy.run_growth_board_surge_replay",
            replay,
        )

        args = MagicMock(
            start_date="2026-06-25",
            end_date="2026-06-26",
            freq="1min",
            min_signal_time="09:33",
            lookback_days=20,
            min_hist_days=10,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
            require_inner_outer=False,
            max_inner_outer_ratio=1.0,
            require_large_net_vol=False,
            min_large_net_vol=0.0,
            require_board_favor=False,
            min_board_gap_up_ratio=0.5,
            min_board_auction_amount_ratio=1.0,
            factor_confirm=False,
            factor_score_threshold=45.0,
            max_hold_days=1,
            output=None,
        )
        rc = cmd_growth_board_surge_replay(args)

        assert rc == 0
        open_store.assert_called_once_with(
            required_tables=[
                "daily_bar",
                "daily_indicator",
                "daily_state",
                "minute_bar",
                "stock_status_daily",
            ]
        )
        replay.assert_called_once()
        config = replay.call_args.kwargs["config"]
        assert config.min_signal_time == time(9, 33)
        assert config.min_cum_amount_ratio == 1.4


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
        from unittest.mock import MagicMock

        # Empty key_list -> should fail fast
        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test

        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "")

        rc = cmd_notify_test(MagicMock())
        assert rc == 1

    def test_pushes_and_returns_0_on_success(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test

        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1,k2")

        with patch("rquant.notify.client.requests.post") as mock_post:
            mock_post.return_value = MagicMock(json=lambda: {"code": 0})
            rc = cmd_notify_test(MagicMock())

        assert rc == 0
        assert mock_post.call_count == 2  # 两个 key 都推

    def test_partial_failure_returns_0_when_any_success(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test

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

        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test

        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1")

        with patch("rquant.notify.client.requests.post") as mock_post:
            mock_post.return_value = MagicMock(json=lambda: {"code": 1, "error": "x"})
            rc = cmd_notify_test(MagicMock())
        assert rc == 1


class TestMainErrorReporting:
    def test_main_catches_run_daily_error_as_typed_outbox(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant.cli import main

        # Force run-daily to raise
        def boom(_args):
            raise ValueError("test boom")

        monkeypatch.setattr("rquant.cli.cmd_run_daily", boom)

        with (
            patch("sys.argv", ["rquant", "run-daily", "--no-ingest"]),
            patch("rquant.cli._record_daily_error_outbox", create=True) as mock_outbox,
            patch("rquant.notify.notify") as mock_notify,
        ):
            rc = main()

        assert rc == 1
        mock_outbox.assert_called_once()
        call_kwargs = mock_outbox.call_args.kwargs
        assert call_kwargs["component"] == "cli:run-daily"
        assert isinstance(call_kwargs["exc"], ValueError)
        assert call_kwargs["trade_date"] == date.today()
        mock_notify.assert_not_called()

    def test_serve_daily_job_error_uses_typed_outbox(self, monkeypatch) -> None:
        from types import ModuleType
        from unittest.mock import patch

        from rquant import cli

        class FakeScheduler:
            job = None

            def scheduled_job(self, *_args: object, **_kwargs: object):
                def register(function):
                    self.job = function
                    return function

                return register

            def shutdown(self, *, wait: bool) -> None:
                del wait

            def start(self) -> None:
                assert self.job is not None
                self.job()

        apscheduler = ModuleType("apscheduler")
        schedulers = ModuleType("apscheduler.schedulers")
        blocking = ModuleType("apscheduler.schedulers.blocking")
        blocking.BlockingScheduler = FakeScheduler
        monkeypatch.setitem(sys.modules, "apscheduler", apscheduler)
        monkeypatch.setitem(sys.modules, "apscheduler.schedulers", schedulers)
        monkeypatch.setitem(sys.modules, "apscheduler.schedulers.blocking", blocking)

        def failed_ingest(_date: str) -> int:
            raise ValueError("ingest failed")

        monkeypatch.setattr("rquant.cli._ingest_with_retry", failed_ingest)
        monkeypatch.setattr("rquant.cli.signal.signal", lambda *_args: None)

        with (
            patch("rquant.cli._record_daily_error_outbox") as mock_outbox,
            patch("rquant.notify.notify") as mock_notify,
        ):
            assert cli.cmd_serve(SimpleNamespace(hour=17)) == 0

        mock_outbox.assert_called_once()
        assert mock_outbox.call_args.kwargs["component"] == "daily_job"
        assert isinstance(mock_outbox.call_args.kwargs["exc"], ValueError)
        mock_notify.assert_not_called()

    def test_daily_error_outbox_persists_typed_signal_without_direct_notification(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from types import SimpleNamespace
        from unittest.mock import patch

        from rquant import cli
        from rquant.delivery_contracts import DeliveryChannel
        from rquant.signal_bus import SignalBusStore

        settings = SimpleNamespace(
            data_dir=tmp_path,
            notify_enabled=True,
            notify_error=True,
            pushdeer_recipient_id_list=["admin"],
            pushplus_recipient_id_list=[],
        )
        monkeypatch.setattr("rquant.config.settings", settings)
        monkeypatch.setenv("RQUANT_CODE_COMMIT", "a" * 40)

        with patch("rquant.notify.notify") as mock_notify:
            cli._record_daily_error_outbox(
                component="daily_job",
                exc=ValueError("upstream failed"),
                trade_date=date(2026, 8, 3),
            )

        mock_notify.assert_not_called()
        bus = SignalBusStore(tmp_path / "daily-close-signal-bus.sqlite3")
        records = bus.outbox_records()
        assert len(records) == 1
        assert records[0].target.channel is DeliveryChannel.PUSHDEER
        signal = bus.signal(records[0].signal_id)
        assert signal.strategy_id == "daily-close-error"
        assert signal.evidence["component"] == "daily_job"

    def test_daily_notification_failure_only_records_health_without_recursing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from types import SimpleNamespace
        from unittest.mock import patch

        from rquant import cli

        class FailingStore:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

        class FailingProducer:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

            def emit(self, *_args: object, **_kwargs: object) -> object:
                raise RuntimeError("outbox unavailable")

        settings = SimpleNamespace(
            data_dir=tmp_path,
            notify_enabled=True,
            notify_error=True,
            pushdeer_recipient_id_list=["admin"],
            pushplus_recipient_id_list=[],
        )
        monkeypatch.setattr("rquant.config.settings", settings)
        monkeypatch.setattr("rquant.signal_bus.SignalBusStore", FailingStore)
        monkeypatch.setattr(
            "rquant.daily_notification_producer.DailyNotificationProducer",
            FailingProducer,
        )

        with (
            patch("rquant.cli.logger.error") as mock_health,
            patch("rquant.notify.notify") as mock_notify,
        ):
            cli._record_daily_error_outbox(
                component="daily_job",
                exc=ValueError("upstream failed"),
                trade_date=date(2026, 8, 3),
            )

        mock_notify.assert_not_called()
        assert any(
            "daily_notification_health=degraded" in str(call.args[0])
            for call in mock_health.call_args_list
        )

    def test_main_does_not_wrap_serve(self, monkeypatch) -> None:
        """serve 内部已自处理异常，main 不再加 try/except。"""
        from unittest.mock import patch

        from rquant.cli import main

        def boom(_args):
            raise ValueError("inner")

        monkeypatch.setattr("rquant.cli.cmd_serve", boom)
        with (
            patch("sys.argv", ["rquant", "serve"]),
            patch("rquant.notify.notify") as mock_notify,
        ):
            # main 让 serve 异常直接冒出来
            import pytest

            with pytest.raises(ValueError):
                main()
            mock_notify.assert_not_called()

    def test_monitor_failure_relies_on_systemd_alert_only(self, monkeypatch) -> None:
        """Long-running monitor must not duplicate its systemd OnFailure alert."""
        from unittest.mock import patch

        from rquant.cli import main

        def boom(_args):
            raise RuntimeError("schema mismatch")

        monkeypatch.setattr("rquant.cli.cmd_monitor", boom)
        with (
            patch("sys.argv", ["rquant", "monitor"]),
            patch("rquant.notify.notify") as mock_notify,
        ):
            assert main() == 1

        mock_notify.assert_not_called()

    def test_minute_repair_failure_is_caught_and_notified(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import patch

        from rquant.cli import main

        def boom(_args):
            raise RuntimeError("repair failed")

        monkeypatch.setattr("rquant.cli.cmd_research_repair_minute", boom)
        with (
            patch(
                "sys.argv",
                [
                    "rquant",
                    "research-repair-minute",
                    "--manifest-id",
                    "b" * 64,
                ],
            ),
            patch("rquant.notify.notify") as mock_notify,
        ):
            assert main() == 1

        mock_notify.assert_called_once()
        assert mock_notify.call_args.kwargs["component"] == "cli:research-repair-minute"


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
        with patch("rquant.cli.time.sleep"), pytest.raises(requests.exceptions.ReadTimeout):
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
        with patch("rquant.cli.time.sleep"), pytest.raises(Exception, match="接口下线"):
            cli._ingest_with_retry("2026-06-04")


class TestLabSchedulerCli:
    @pytest.fixture(autouse=True)
    def configured_daemon_runtime(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from rquant import (
            cli,
            job_center_authority,
            lab_artifact_protocol,
            lab_artifacts,
            lab_daemon,
        )
        from rquant.config import settings

        class FakeKeyring:
            def verification_key(self, _key_id: str) -> None:
                return None

        class FakeLock:
            def __init__(self, *_args: object, mutation_guard: object) -> None:
                assert callable(mutation_guard)

            def __enter__(self) -> FakeLock:
                return self

            def __exit__(self, *_args: object) -> None:
                pass

        class FakeArtifactStore:
            def __init__(
                self,
                _path: Path,
                *,
                mutation_guard: object,
            ) -> None:
                assert callable(mutation_guard)

            def close(self) -> None:
                pass

        class FakeArtifactCommitSpool:
            def __init__(
                self,
                _path: Path,
                *,
                mutation_guard: object,
            ) -> None:
                assert callable(mutation_guard)

        monkeypatch.setattr(lab_daemon, "LabDaemonLock", FakeLock)
        monkeypatch.setattr(
            cli,
            "_establish_lab_runtime_identity",
            lambda _args: ("1" * 40, object(), object(), lambda: "1" * 40),
        )
        monkeypatch.setattr(
            lab_daemon,
            "require_lab_runtime_binding",
            lambda _root, _git, **_kwargs: "1" * 40,
        )
        monkeypatch.setattr(lab_daemon, "verify_lab_runtime_prepared", lambda *_a, **_k: {})
        monkeypatch.setattr(
            lab_daemon,
            "load_lab_job_center_authority_manifest",
            lambda *_a, **_k: SimpleNamespace(),
        )
        monkeypatch.setattr(
            lab_daemon,
            "ensure_private_directory",
            lambda path, *, label, mutation_guard: path,
        )
        monkeypatch.setattr(
            lab_daemon,
            "prepare_lab_runtime_sqlite_authority",
            lambda _root, *, label, path, mutation_guard: _FakeLabSqliteAuthority(path),
        )
        monkeypatch.setattr(
            lab_daemon.LabAuthorityKeyring,
            "load",
            classmethod(lambda cls, **kwargs: FakeKeyring()),
        )
        monkeypatch.setattr(lab_artifacts, "LabJobArtifactStore", FakeArtifactStore)
        monkeypatch.setattr(
            lab_artifact_protocol,
            "LabArtifactCommitSpool",
            FakeArtifactCommitSpool,
        )
        monkeypatch.setattr(settings, "lab_finalizer_authority_key_id", "active")
        monkeypatch.setattr(settings, "lab_finalizer_authority_key_path", Path("/tmp/key"))
        monkeypatch.setattr(
            settings,
            "lab_finalizer_authority_keyring_path",
            Path("/tmp/keyring"),
        )
        monkeypatch.setattr(settings, "lab_trusted_git_path", Path(_LAB_TRUSTED_GIT))
        monkeypatch.setattr(
            job_center_authority,
            "resolve_current_job_center_authority_binding",
            lambda *_args, **_kwargs: SimpleNamespace(
                runtime_deployment_root=Path("/tmp/rquant-production-runtime"),
                runtime_root=settings.lab_runtime_dir_resolved,
                lab_jobs_path=settings.lab_jobs_path_resolved,
                command_spool_path=settings.lab_job_command_dir_resolved,
                final_artifact_root=settings.lab_final_artifact_dir_resolved,
                deployment_profile_id="2" * 64,
                deployment_generation_hash="3" * 64,
                runtime_mode="local-test",
                lab_highwater=None,
            ),
        )

    def test_parser_accepts_once_and_preserves_lab_run(self) -> None:
        scheduler = build_parser().parse_args(
            [
                "lab-scheduler",
                *_LAB_RUNTIME_BOOTSTRAP_ARGUMENTS,
                "--runtime-deployment-root",
                "/tmp/rquant-production-runtime",
                *_LAB_DAEMON_GENERATION_ARGUMENTS,
                "--once",
            ]
        )
        legacy = build_parser().parse_args(["lab-run", "--spec", "/tmp/spec.json"])

        assert scheduler.command == "lab-scheduler"
        assert scheduler.once is True
        assert legacy.command == "lab-run"
        assert legacy.spec == "/tmp/spec.json"

    def test_parser_defaults_to_forever(self) -> None:
        args = build_parser().parse_args(
            [
                "lab-scheduler",
                *_LAB_RUNTIME_BOOTSTRAP_ARGUMENTS,
                "--runtime-deployment-root",
                "/tmp/rquant-production-runtime",
                *_LAB_DAEMON_GENERATION_ARGUMENTS,
            ]
        )

        assert args.once is False

    def test_scheduler_requires_prepared_runtime_before_creating_managed_paths(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import argparse

        from rquant import lab_daemon
        from rquant.cli import cmd_lab_scheduler

        monkeypatch.setattr(
            lab_daemon,
            "load_lab_job_center_authority_manifest",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                lab_daemon.LabDaemonConfigurationError("prepared sentinel missing")
            ),
        )
        monkeypatch.setattr(
            lab_daemon,
            "ensure_private_directory",
            lambda *_args, **_kwargs: pytest.fail(
                "scheduler created a directory before prepared sentinel validation"
            ),
        )

        with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="prepared sentinel"):
            cmd_lab_scheduler(
                argparse.Namespace(
                    once=True,
                    **_LAB_RUNTIME_BOOTSTRAP_VALUES,
                    runtime_deployment_root="/tmp/rquant-production-runtime",
                    startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
                )
            )

    def test_scheduler_requires_installed_current_authority_before_sqlite_prepare(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import argparse

        from rquant import lab_daemon
        from rquant.cli import cmd_lab_scheduler

        monkeypatch.setattr(
            lab_daemon,
            "load_lab_job_center_authority_manifest",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                lab_daemon.LabDaemonConfigurationError("authority manifest missing")
            ),
        )
        monkeypatch.setattr(
            lab_daemon,
            "prepare_lab_runtime_sqlite_authority",
            lambda *_args, **_kwargs: pytest.fail(
                "scheduler touched SQLite before authority validation"
            ),
        )

        with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="authority manifest"):
            cmd_lab_scheduler(
                argparse.Namespace(
                    once=True,
                    **_LAB_RUNTIME_BOOTSTRAP_VALUES,
                    runtime_deployment_root="/tmp/rquant-production-runtime",
                    startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
                )
            )

    def test_cmd_lab_scheduler_once_initializes_ticks_and_releases(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        import argparse

        from rquant import (
            job_center_authority,
            lab_daemon,
            lab_job_protocol,
            lab_jobs,
            lab_scheduler,
            lab_shard_protocol,
        )
        from rquant.cli import cmd_lab_scheduler

        calls: list[str] = []

        class FakeStore:
            def __init__(
                self,
                path: Path,
                *,
                busy_timeout_ms: int,
                identity_authority: object,
                mutation_guard: object,
            ) -> None:
                calls.append(f"store:{path.name}:{busy_timeout_ms}")
                assert isinstance(identity_authority, _FakeLabSqliteAuthority)
                assert callable(mutation_guard)

            def initialize(self) -> None:
                calls.append("initialize")

        class FakeSpool:
            def __init__(self, path: Path, *, mutation_guard: object) -> None:
                calls.append(f"spool:{path.name}")
                assert callable(mutation_guard)

        class FakeClaimSpool:
            def __init__(
                self,
                path: Path,
                *,
                claim_advance_hook: object,
                mutation_guard: object,
            ) -> None:
                calls.append(f"claim_spool:{path.name}")
                assert callable(claim_advance_hook)
                assert callable(mutation_guard)

        class FakeReportSpool:
            def __init__(self, path: Path, *, mutation_guard: object) -> None:
                calls.append(f"report_spool:{path.name}")
                assert callable(mutation_guard)

        class FakeScheduler:
            def __init__(self, **kwargs: object) -> None:
                calls.append(f"scheduler:{kwargs['owner_id']}")
                assert kwargs["max_commands_per_tick"] == 64
                assert kwargs["max_reports_per_tick"] == 64
                assert kwargs["max_plans_per_tick"] == 64
                assert kwargs["max_claims_per_tick"] == 16
                assert kwargs["max_claim_authority_per_tick"] == 128
                assert kwargs["max_artifact_commits_per_tick"] == 64
                reader = kwargs["integrity_auditor"]
                assert reader.highwater_observer is not None
                assert "--machine-receipt" in kwargs["full_integrity_command"]
                assert callable(kwargs["full_integrity_remediation_authorizer"])

            def run_once(self) -> SimpleNamespace:
                calls.append("run_once")
                return SimpleNamespace(model_dump_json=lambda: "{}")

            def release(self) -> None:
                calls.append("release")

        monkeypatch.setattr(lab_jobs, "LabJobStore", FakeStore)
        monkeypatch.setattr(lab_job_protocol, "LabCommandSpool", FakeSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabClaimSpool", FakeClaimSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabReportSpool", FakeReportSpool)
        monkeypatch.setattr(lab_scheduler, "LabScheduler", FakeScheduler)
        monkeypatch.setattr(
            lab_daemon,
            "prepare_lab_runtime_sqlite_authority",
            lambda _root, *, label, path, mutation_guard: (
                calls.append(f"sqlite:{path.name}:{label}:True")
                or _FakeLabSqliteAuthority(path, created=True)
            ),
        )
        monkeypatch.setattr(lab_daemon, "require_unique_runtime_paths", lambda _paths: None)
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)
        from rquant.config import settings

        private_manifest, _public_key = write_private_manifest(
            tmp_path / "lab-highwater-private-keys.json",
            active_key_id="hw-v1",
        )
        credential = export_public_keyring(
            private_manifest,
            tmp_path / "lab-highwater-public-keys.json",
        )
        monkeypatch.setattr(settings, "lab_runtime_dir", tmp_path / "lab-runtime")
        monkeypatch.setattr(settings, "lab_finalizer_state_dir", tmp_path / "lab-state")
        monkeypatch.setattr(settings, "lab_highwater_authority_command_json", "")
        monkeypatch.setattr(settings, "lab_highwater_stable_identity", "")
        monkeypatch.setattr(settings, "lab_highwater_trusted_keyring_path", None)
        profile = SimpleNamespace(
            authority_command=(
                "/usr/bin/sudo",
                "-n",
                "/usr/local/libexec/rquant-lab-highwater-authority",
            ),
            stable_identity="lab-test-production",
            trusted_keyring_path=credential,
            timeout_seconds=3.0,
            allow_identity_rotation=False,
            production_mode=True,
        )
        monkeypatch.setattr(
            job_center_authority,
            "resolve_current_job_center_authority_binding",
            lambda *_args, **_kwargs: SimpleNamespace(
                runtime_deployment_root=Path("/tmp/rquant-production-runtime"),
                runtime_root=settings.lab_runtime_dir_resolved,
                lab_jobs_path=settings.lab_jobs_path_resolved,
                command_spool_path=settings.lab_job_command_dir_resolved,
                final_artifact_root=settings.lab_final_artifact_dir_resolved,
                deployment_profile_id="2" * 64,
                deployment_generation_hash="3" * 64,
                runtime_mode="linux-production",
                lab_highwater=profile,
            ),
        )

        result = cmd_lab_scheduler(
            argparse.Namespace(
                once=True,
                **_LAB_RUNTIME_BOOTSTRAP_VALUES,
                runtime_deployment_root="/tmp/rquant-production-runtime",
                startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
            )
        )

        assert result == 0
        assert "sqlite:lab_jobs.sqlite3:lab jobs SQLite:True" in calls
        assert "initialize" in calls
        assert "claim_spool:claims" in calls
        assert "report_spool:reports" in calls
        assert calls[-2:] == ["run_once", "release"]

    def test_cmd_lab_scheduler_forever_installs_cooperative_signal_handlers(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import argparse
        import signal

        from rquant import (
            lab_daemon,
            lab_job_protocol,
            lab_scheduler,
            lab_shard_protocol,
            lab_worker,
        )
        from rquant.cli import cmd_lab_scheduler

        handlers: dict[int, object] = {}
        calls: list[str] = []

        class FakeScheduler:
            def __init__(self, **_kwargs: object) -> None:
                pass

            def request_stop(self) -> None:
                calls.append("request_stop")

            def run_forever(self) -> None:
                calls.append("run_forever")
                handler = handlers[signal.SIGTERM]
                assert callable(handler)
                handler(signal.SIGTERM, None)

        class FakeSpool:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

        class FakeReclaimer:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

            def reclaim(self, *_args: object) -> None:
                pass

        def fake_signal(signum: int, handler: object) -> object:
            previous = handlers.get(signum, signal.SIG_DFL)
            handlers[signum] = handler
            return previous

        monkeypatch.setattr(lab_scheduler, "LabScheduler", FakeScheduler)
        monkeypatch.setattr(lab_job_protocol, "LabCommandSpool", FakeSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabClaimSpool", FakeSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabReportSpool", FakeSpool)
        monkeypatch.setattr(lab_worker, "LabArtifactReclaimer", FakeReclaimer)
        monkeypatch.setattr(
            "rquant.lab_jobs.LabJobStore.initialize",
            lambda _self: None,
        )
        monkeypatch.setattr(lab_daemon, "require_unique_runtime_paths", lambda _paths: None)
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)
        monkeypatch.setattr(signal, "signal", fake_signal)

        result = cmd_lab_scheduler(
            argparse.Namespace(
                once=False,
                **_LAB_RUNTIME_BOOTSTRAP_VALUES,
                runtime_deployment_root="/tmp/rquant-production-runtime",
                startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
            )
        )

        assert result == 0
        assert calls == ["run_forever", "request_stop"]


class TestLabWorkerCli:
    @pytest.fixture(autouse=True)
    def configured_daemon_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from rquant import lab_daemon
        from rquant.config import settings
        from rquant.runtime_market_session import MarketCalendarAuthority

        class FakeLock:
            def __init__(self, *_args: object, mutation_guard: object) -> None:
                assert callable(mutation_guard)

            def __enter__(self) -> FakeLock:
                return self

            def __exit__(self, *_args: object) -> None:
                pass

        monkeypatch.setattr(lab_daemon, "LabDaemonLock", FakeLock)
        monkeypatch.setattr(
            lab_daemon,
            "require_lab_runtime_binding",
            lambda _root, _git, **_kwargs: "1" * 40,
        )
        monkeypatch.setattr(lab_daemon, "verify_lab_runtime_prepared", lambda *_a, **_k: {})
        monkeypatch.setattr(
            lab_daemon,
            "ensure_private_directory",
            lambda path, *, label, mutation_guard: path,
        )
        monkeypatch.setattr(lab_daemon, "require_unique_runtime_paths", lambda _paths: None)
        monkeypatch.setattr(settings, "lab_worker_id", "worker-a")
        monkeypatch.setattr(settings, "lab_scheduler_worker_ids", "worker-a")
        monkeypatch.setattr(settings, "lab_trusted_git_path", Path(_LAB_TRUSTED_GIT))
        calendar_path = tmp_path / "market-calendar.json"
        calendar = MarketCalendarAuthority.create(
            schema_version=1,
            exchange="SSE",
            producer_commit="1" * 40,
            coverage_start=date(2026, 1, 1),
            coverage_end=date(2027, 1, 1),
            open_dates=(),
            generated_at=datetime(2025, 12, 31, tzinfo=UTC),
        )
        calendar_path.write_text(
            json.dumps(calendar.model_dump(mode="json"), separators=(",", ":")),
            encoding="utf-8",
        )
        calendar_path.chmod(0o600)
        monkeypatch.setattr(
            settings,
            "rquant_lab_resource_policy_version",
            "lab-resource-v1",
        )
        monkeypatch.setattr(
            settings,
            "rquant_lab_live_slo_authority_root",
            tmp_path / "runtime-health-authority",
        )
        monkeypatch.setattr(settings, "rquant_lab_trade_calendar_path", calendar_path)
        monkeypatch.delenv("RQUANT_LAB_RESOURCE_POLICY_VERSION", raising=False)
        monkeypatch.delenv("RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT", raising=False)
        monkeypatch.delenv("RQUANT_LAB_TRADE_CALENDAR_PATH", raising=False)

    def test_parser_accepts_worker_identity_and_once(self) -> None:
        args = build_parser().parse_args(
            [
                "lab-worker",
                "--expected-checkout-root",
                _LAB_EXPECTED_ROOT,
                "--trusted-git-path",
                _LAB_TRUSTED_GIT,
                *_LAB_DAEMON_GENERATION_ARGUMENTS,
                "--worker-id",
                "worker-a",
                "--once",
            ]
        )

        assert args.command == "lab-worker"
        assert args.worker_id == "worker-a"
        assert args.once is True
        assert args.legacy_no_resource_admission is False

    def test_parser_accepts_explicit_legacy_resource_admission_opt_out(self) -> None:
        args = build_parser().parse_args(
            [
                "lab-worker",
                "--expected-checkout-root",
                _LAB_EXPECTED_ROOT,
                "--trusted-git-path",
                _LAB_TRUSTED_GIT,
                *_LAB_DAEMON_GENERATION_ARGUMENTS,
                "--legacy-no-resource-admission",
            ]
        )

        assert args.legacy_no_resource_admission is True

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("rquant_lab_resource_policy_version", "", "policy version"),
            ("rquant_lab_live_slo_authority_root", None, "live SLO"),
            ("rquant_lab_trade_calendar_path", None, "trade calendar"),
        ],
    )
    def test_real_worker_cli_fails_closed_when_resource_authority_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        field: str,
        value: object,
        message: str,
    ) -> None:
        from rquant.cli import cmd_lab_worker
        from rquant.config import settings

        monkeypatch.setattr(settings, field, value)

        with pytest.raises(RuntimeError, match=message):
            cmd_lab_worker(
                argparse.Namespace(
                    worker_id="worker-a",
                    once=True,
                    expected_checkout_root=_LAB_EXPECTED_ROOT,
                    trusted_git_path=_LAB_TRUSTED_GIT,
                    startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
                )
            )

    def test_parser_accepts_one_shot_lab_runtime_prepare(self) -> None:
        args = build_parser().parse_args(
            [
                "lab-runtime-prepare",
                "--runtime-code-config",
                "/etc/rquant/runtime-code-bootstrap.json",
                "--runtime-code-trusted-base",
                "/etc/rquant",
                "--runtime-code-authority-uid",
                "0",
                "--runtime-code-authority-gid",
                "0",
                "--runtime-deployment-root",
                "/private/tmp/rquant-production-runtime",
                *_LAB_DAEMON_GENERATION_ARGUMENTS,
            ]
        )

        assert args.command == "lab-runtime-prepare"
        assert args.runtime_deployment_root == Path("/private/tmp/rquant-production-runtime")
        assert args.runtime_code_config == Path("/etc/rquant/runtime-code-bootstrap.json")
        assert not hasattr(args, "expected_checkout_root")
        assert not hasattr(args, "trusted_git_path")
        assert not hasattr(args, "definition_registry_root")

    def test_runtime_prepare_installs_current_job_center_authority(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from rquant import (
            artifact_retention_catalog_authority,
            cli,
            job_center_authority,
            lab_daemon,
            lab_jobs,
            runtime_deployment_profile,
        )
        from rquant.cli import cmd_lab_runtime_prepare
        from rquant.config import settings
        from rquant.runtime_service_entrypoint import RuntimeServiceKind

        runtime_root = tmp_path / "runtime"
        release_root = tmp_path / "generation" / "release"
        calls: list[str] = []

        class FakeSqliteAuthority:
            path = runtime_root / "lab_jobs.sqlite3"

            def close(self) -> None:
                calls.append("close")

        class FakeStore:
            def __init__(self, path: Path, **kwargs: object) -> None:
                assert path == FakeSqliteAuthority.path
                assert kwargs["identity_authority"].path == path

            def initialize(self) -> None:
                calls.append("initialize")

        monkeypatch.setattr(settings, "lab_runtime_dir", runtime_root)
        monkeypatch.setattr(settings, "lab_jobs_path", FakeSqliteAuthority.path)
        monkeypatch.setattr(settings, "lab_job_command_dir", runtime_root / "commands")
        monkeypatch.setattr(
            settings,
            "lab_final_artifact_dir",
            runtime_root / "final-artifacts",
        )
        monkeypatch.setattr(
            cli,
            "_establish_lab_runtime_identity",
            lambda _args: (
                "1" * 40,
                SimpleNamespace(
                    capability=SimpleNamespace(release_root=release_root),
                ),
                object(),
                lambda: "1" * 40,
            ),
        )

        def prepare_layout(*_args: object, **kwargs: object) -> None:
            assert kwargs["checkout_root"] == release_root
            calls.append("layout")

        monkeypatch.setattr(
            lab_daemon,
            "prepare_lab_runtime_layout",
            prepare_layout,
        )
        monkeypatch.setattr(
            lab_daemon,
            "prepare_lab_runtime_sqlite_authority",
            lambda *_args, **_kwargs: calls.append("sqlite") or FakeSqliteAuthority(),
        )
        monkeypatch.setattr(lab_jobs, "LabJobStore", FakeStore)
        deployment_root = tmp_path / "production-runtime"
        retention_state_root = deployment_root / "control" / "artifact-retention"
        retention_reference_store = deployment_root / "research" / "artifact-catalog"
        monkeypatch.setattr(
            runtime_deployment_profile,
            "load_current_runtime_deployment_profile",
            lambda _root: SimpleNamespace(
                producer_commit="1" * 40,
                manifests=(
                    SimpleNamespace(
                        service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
                        producer_commit="1" * 40,
                        settings={
                            "state_root": str(retention_state_root),
                            "reference_store_path": str(retention_reference_store),
                        },
                    ),
                ),
            ),
        )
        monkeypatch.setattr(
            artifact_retention_catalog_authority,
            "initialize_retention_catalog_authority",
            lambda **_kwargs: calls.append("retention"),
        )
        binding = SimpleNamespace(
            runtime_deployment_root=deployment_root,
            runtime_root=runtime_root,
            lab_jobs_path=FakeSqliteAuthority.path,
            command_spool_path=runtime_root / "commands",
            final_artifact_root=runtime_root / "final-artifacts",
            definition_registry_root=tmp_path / "definitions",
            experiment_registry_path=runtime_root / "experiment_registry.sqlite3",
            dataset_authority_path=runtime_root / "research_ro.duckdb",
            catalog_authority_root=runtime_root / "artifact-catalog",
            catalog_authority_receipt_path=(runtime_root / "artifact-catalog" / "current.json"),
            deployment_profile_id="2" * 64,
            deployment_generation_hash="3" * 64,
        )
        monkeypatch.setattr(
            job_center_authority,
            "resolve_current_job_center_authority_binding",
            lambda *_args, **_kwargs: binding,
        )

        def publish(**kwargs: object) -> object:
            calls.append("authority")
            assert kwargs["code_sha"] == "1" * 40
            assert kwargs["runtime_root"] == runtime_root
            assert kwargs["lab_jobs_path"] == FakeSqliteAuthority.path
            assert kwargs["deployment_profile_id"] == "2" * 64
            assert kwargs["deployment_generation_hash"] == "3" * 64
            assert callable(kwargs["current_code_sha"])
            return object()

        monkeypatch.setattr(
            job_center_authority,
            "publish_install_current_job_center_authority",
            publish,
        )

        result = cmd_lab_runtime_prepare(
            argparse.Namespace(
                runtime_code_config=Path("/etc/rquant/runtime-code-bootstrap.json"),
                runtime_code_trusted_base=Path("/etc/rquant"),
                runtime_code_authority_uid=0,
                runtime_code_authority_gid=0,
                runtime_deployment_root=deployment_root,
                expected_code_sha=None,
                startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
            )
        )

        assert result == 0
        assert calls == [
            "retention",
            "layout",
            "sqlite",
            "initialize",
            "close",
            "authority",
        ]

    def test_worker_requires_prepared_runtime_before_creating_spools(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import argparse

        from rquant import lab_daemon
        from rquant.cli import cmd_lab_worker

        monkeypatch.setattr(
            lab_daemon,
            "verify_lab_runtime_prepared",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                lab_daemon.LabDaemonConfigurationError("prepared sentinel missing")
            ),
        )
        monkeypatch.setattr(
            lab_daemon,
            "ensure_private_directory",
            lambda *_args, **_kwargs: pytest.fail(
                "worker created a spool before prepared sentinel validation"
            ),
        )

        with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="prepared sentinel"):
            cmd_lab_worker(
                argparse.Namespace(
                    worker_id="worker-a",
                    once=True,
                    expected_checkout_root=_LAB_EXPECTED_ROOT,
                    trusted_git_path=_LAB_TRUSTED_GIT,
                    startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
                )
            )

    def test_production_resource_manifest_requires_explicit_v2_configuration(self) -> None:
        from rquant.cli import _build_lab_worker_resource_authority_manifest
        from rquant.lab_daemon import LabDaemonConfigurationError
        from rquant.lab_resource_authority_adapter import ResourceAuthorityAdapterConfig
        from rquant.strict_json import canonical_model_json_bytes

        settings = SimpleNamespace(
            app_env="prod",
            rquant_lab_resource_authority_config_json="",
        )
        admission = SimpleNamespace(require_resource_admission=True)

        with pytest.raises(LabDaemonConfigurationError, match="explicit V2"):
            _build_lab_worker_resource_authority_manifest(
                settings=settings,
                resource_admission=admission,
            )

        settings.rquant_lab_resource_authority_config_json = canonical_model_json_bytes(
            ResourceAuthorityAdapterConfig(
                mode="test-standalone",
                endpoint=Path("/tmp/rqa.sock"),
                expected_uid=1000,
                expected_gid=1000,
                authority_id="test-resource-authority",
                trusted_role_inventory_hash="a" * 64,
            )
        ).decode("utf-8")
        with pytest.raises(LabDaemonConfigurationError, match="production resource authority"):
            _build_lab_worker_resource_authority_manifest(
                settings=settings,
                resource_admission=admission,
            )

    def test_production_resource_manifest_builds_only_closed_v2(self) -> None:
        from rquant.cli import _build_lab_worker_resource_authority_manifest
        from rquant.lab_resource_authority_adapter import (
            LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
            ExternalResourceJournalRootConfig,
            ResourceAuthorityAdapterConfig,
        )
        from rquant.strict_json import canonical_json_bytes

        config = ResourceAuthorityAdapterConfig(
            mode="production",
            endpoint=Path("/run/rquant/resource-authority.sock"),
            expected_uid=1000,
            expected_gid=1000,
            authority_id="resource-authority",
            high_water_authority_id="resource-high-water-authority",
            external_root_config=ExternalResourceJournalRootConfig(
                transport="unix-socket-v1",
                transport_manifest_hash="9" * 64,
                root_authority_id="external-root-authority",
                root_store_id="external-root-store",
                root_issuer="resource-root-issuer",
                root_key_id="resource-root-key",
                root_public_key_fingerprint="e" * 64,
                witness_rollback_domain_id="external-root-domain",
                local_rollback_domain_id="resource-authority-domain",
            ),
            trusted_role_inventory_hash="a" * 64,
        )
        settings = SimpleNamespace(
            app_env="prod",
            rquant_lab_resource_authority_config_json=canonical_json_bytes(
                config.model_dump(mode="json", round_trip=True)
            ).decode("utf-8"),
        )

        manifest = _build_lab_worker_resource_authority_manifest(
            settings=settings,
            resource_admission=SimpleNamespace(require_resource_admission=True),
        )

        assert manifest is not None
        assert manifest.registry.registry_id == LAB_RESOURCE_AUTHORITY_REGISTRY_ID

    def test_cli_resource_bindings_run_real_bounded_spawn_probe(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from rquant.cli import _build_lab_worker_resource_admission
        from rquant.config import settings
        from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
        from rquant.lab_worker import (
            LabWorker,
            build_builtin_resource_authority_manifest,
        )
        from rquant.runtime_contracts import canonical_sha256
        from rquant.runtime_market_session import MarketCalendarAuthority
        from rquant.runtime_service_control import (
            RuntimeServiceHealth,
            RuntimeServiceHeartbeat,
            RuntimeServicePlane,
            RuntimeServiceStatus,
        )
        from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
        from rquant.runtime_serving_snapshot import RuntimeHealthPayload, SourceReadResult
        from rquant.serving_contracts import FreshnessStatus

        commit = "1" * 40
        observed_at = datetime.now(UTC)
        heartbeat = RuntimeServiceHeartbeat(
            service_id="feature-live",
            spec_fingerprint="b" * 64,
            run_id="c" * 64,
            generation=1,
            status=RuntimeServiceStatus.RUNNING,
            started_at=observed_at - timedelta(minutes=2),
            heartbeat_at=observed_at - timedelta(seconds=1),
            last_success_at=observed_at - timedelta(seconds=2),
            recent_step_durations_seconds=(0.2,),
            last_step_duration_seconds=0.2,
            p95_step_duration_seconds=0.2,
        )
        payload = RuntimeHealthPayload(
            runtime_services=(
                RuntimeServiceHealth(
                    service_id="feature-live",
                    plane=RuntimeServicePlane.LIVE,
                    status=RuntimeServiceStatus.RUNNING,
                    stale=False,
                    observed_at=observed_at - timedelta(seconds=1),
                    heartbeat=heartbeat,
                ),
            )
        )
        source_values: dict[str, object] = {
            "dataset_id": "runtime_health",
            "sequence": 1,
            "event_time": observed_at - timedelta(seconds=1),
            "published_at": observed_at - timedelta(seconds=1),
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": payload,
        }
        source_values["generation_id"] = canonical_sha256(source_values)
        authority_root = tmp_path / "runtime-health-authority"
        ServingSourceAuthorityPublisher(
            root=authority_root,
            producer_commit=commit,
            dataset_id="runtime_health",
            payload_kind="runtime_health",
            clock=lambda: observed_at - timedelta(seconds=1),
        ).publish(SourceReadResult.model_validate(source_values))
        calendar = MarketCalendarAuthority.create(
            schema_version=1,
            exchange="SSE",
            producer_commit=commit,
            coverage_start=observed_at.date(),
            coverage_end=observed_at.date(),
            open_dates=(observed_at.date(),),
            generated_at=observed_at - timedelta(days=1),
        )
        calendar_path = tmp_path / "market-calendar.json"
        calendar_path.write_text(
            json.dumps(calendar.model_dump(mode="json"), separators=(",", ":")),
            encoding="utf-8",
        )
        calendar_path.chmod(0o600)
        artifact_root = tmp_path / "artifacts"
        artifact_root.mkdir()
        monkeypatch.setattr(settings, "rquant_lab_live_slo_authority_root", authority_root)
        monkeypatch.setattr(settings, "rquant_lab_trade_calendar_path", calendar_path)
        monkeypatch.setattr(settings, "lab_worker_artifact_dir", artifact_root)

        bindings = _build_lab_worker_resource_admission(
            settings=settings,
            code_sha=commit,
            legacy_opt_out=False,
        )
        assert bindings.resource_snapshot_provider is not None
        assert bindings.admission_policy_provider is not None
        authority_manifest = build_builtin_resource_authority_manifest(
            bindings.resource_snapshot_provider,
            bindings.admission_policy_provider,
        )
        worker = LabWorker(
            worker_id="worker-a",
            claim_spool=LabClaimSpool(tmp_path / "claims"),
            report_spool=LabReportSpool(tmp_path / "reports"),
            artifact_root=artifact_root,
            resource_authority_manifest=authority_manifest,
            require_resource_admission=bindings.require_resource_admission,
            resource_probe_timeout_seconds=3,
            verified_code_sha_provider=lambda: commit,
        )

        snapshot = worker._bounded_resource_snapshot(timeout_seconds=3)

        assert abs((snapshot.observed_at - observed_at).total_seconds()) < 3
        assert snapshot.live_healthy is True
        watermark = worker.snapshot_authority_watermark
        if snapshot.live_slo_applicable:
            assert snapshot.live_backlog_age_seconds == 2
            assert watermark is not None
            assert watermark.sequence == 1
        else:
            assert watermark is None

    @pytest.mark.parametrize(
        ("status", "expected_exit"),
        [
            ("idle", 0),
            ("succeeded", 0),
            ("failed", 1),
            ("stopped", 1),
            ("reported", 2),
            ("awaiting_receipt", 2),
            ("unknown", 2),
        ],
    )
    def test_cmd_lab_worker_builds_spools_and_runs_one_tick(
        self,
        monkeypatch: pytest.MonkeyPatch,
        status: str,
        expected_exit: int,
    ) -> None:
        import argparse

        from rquant import lab_shard_protocol, lab_worker
        from rquant.cli import cmd_lab_worker

        calls: list[str] = []

        class FakeSpool:
            def __init__(self, path: Path, *, mutation_guard: object) -> None:
                calls.append(f"spool:{path.name}")
                assert callable(mutation_guard)

        class FakeWorker:
            def __init__(self, **kwargs: object) -> None:
                calls.append(f"worker:{kwargs['worker_id']}")
                assert kwargs["verified_code_sha_provider"] is not None
                assert kwargs["require_resource_admission"] is True
                assert kwargs["resource_authority_manifest"] is not None
                assert kwargs["shard_runtime_manifest"] is not None
                assert kwargs["claim_publication_verifier"] is None
                assert kwargs["v2_claim_publication_enabled"] is False
                forbidden = {
                    "adapter_registry",
                    "exploratory_store_factory",
                    "metadata_store_factory",
                    "resource_snapshot_provider",
                    "admission_policy_provider",
                }
                assert forbidden.isdisjoint(kwargs)

            def run_once(self) -> SimpleNamespace:
                calls.append("run_once")
                return SimpleNamespace(status=status, model_dump_json=lambda: "{}")

        monkeypatch.setattr(lab_shard_protocol, "LabClaimSpool", FakeSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabReportSpool", FakeSpool)
        monkeypatch.setattr(lab_worker, "LabWorker", FakeWorker)
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)

        result = cmd_lab_worker(
            argparse.Namespace(
                worker_id="worker-a",
                once=True,
                expected_checkout_root=_LAB_EXPECTED_ROOT,
                trusted_git_path=_LAB_TRUSTED_GIT,
                startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
            )
        )

        assert result == expected_exit
        assert "spool:claims" in calls
        assert "spool:reports" in calls
        assert calls[-2:] == ["worker:worker-a", "run_once"]

    @pytest.mark.parametrize("material", (None, b"{}"))
    def test_cmd_lab_worker_v2_requires_public_verifier_before_worker_construction(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        material: bytes | None,
    ) -> None:
        from rquant import lab_shard_protocol, lab_worker
        from rquant.cli import cmd_lab_worker
        from rquant.config import settings
        from rquant.lab_daemon import LabDaemonConfigurationError

        material_path = None if material is None else tmp_path / "invalid-verifier.json"
        if material_path is not None:
            material_path.write_bytes(material)
        monkeypatch.setattr(settings, "lab_v2_claim_publication_enabled", True)
        monkeypatch.setattr(settings, "lab_claim_publication_worker_verifier_path", material_path)
        monkeypatch.setattr(lab_worker, "LabWorker", lambda **_kwargs: pytest.fail("worker built"))
        monkeypatch.setattr(
            lab_shard_protocol,
            "LabClaimSpool",
            lambda *_args, **_kwargs: object(),
        )
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)

        with pytest.raises(LabDaemonConfigurationError, match="public verifier material"):
            cmd_lab_worker(
                argparse.Namespace(
                    worker_id="worker-a",
                    once=True,
                    expected_checkout_root=_LAB_EXPECTED_ROOT,
                    trusted_git_path=_LAB_TRUSTED_GIT,
                    startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
                )
            )

    def test_cmd_lab_worker_v2_builds_verify_only_publication_gate(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from rquant import lab_worker
        from rquant.cli import cmd_lab_worker
        from rquant.config import settings
        from rquant.lab_claim_finalizer_trust import (
            LabClaimFinalizerTrustCertificate,
            LabClaimPublicationWorkerVerificationConfig,
            sign_lab_claim_finalizer_trust_certificate,
        )
        from rquant.lab_claim_publication import LabClaimSpoolReceiptAuthorityV2
        from rquant.lab_jobs import LabJobStore
        from rquant.source_broker_v2_job_protocol import SourceBrokerV2AuthorityRef
        from rquant.strict_json import canonical_model_json_bytes
        from tests.unit.test_adapter_manifest import create_test_authorities

        runtime = tmp_path / "runtime"
        runtime.mkdir(mode=0o700)
        for field, name in (
            ("lab_runtime_dir", "runtime"),
            ("lab_jobs_path", "lab_jobs.sqlite3"),
            ("lab_job_claim_dir", "claims"),
            ("lab_job_report_dir", "reports"),
            ("lab_worker_artifact_dir", "worker-artifacts"),
            ("lab_daemon_lock_dir", "locks"),
        ):
            monkeypatch.setattr(settings, field, runtime if name == "runtime" else runtime / name)
        store = LabJobStore(settings.lab_jobs_path_resolved)
        store.initialize()
        authorities = create_test_authorities(tmp_path / "keys")
        with store._connect() as connection:  # noqa: SLF001 - fixture binds the cert to this inode
            binding = store._finalizer_authority_binding(connection, path=store.path)  # noqa: SLF001
        certificate = sign_lab_claim_finalizer_trust_certificate(
            root_signer=authorities.finalizer_trust_root,
            certificate=LabClaimFinalizerTrustCertificate(
                root_issuer=authorities.finalizer_trust_root.issuer,
                root_key_id=authorities.finalizer_trust_root.key_id,
                finalizer_issuer=authorities.finalizer_runtime.issuer,
                finalizer_key_id=authorities.finalizer_runtime.key_id,
                finalizer_public_key_fingerprint=(
                    authorities.finalizer_runtime.public_key_fingerprint
                ),
                store_id=str(binding["store_id"]),
                database_device=binding["database_generation"][0],
                database_inode=binding["database_generation"][1],
                schema_version_bound=int(binding["schema_version"]),
                not_before=datetime(2020, 1, 1, tzinfo=UTC),
                expires_at=datetime(2030, 1, 1, tzinfo=UTC),
                signature="unsigned",
            ),
        )
        records = authorities.records
        root_records = tuple(
            record for record in records if record.key_purpose == "lab_claim_finalizer_root"
        )
        finalizer_records = tuple(
            record for record in records if record.key_purpose == "lab_claim_finalizer"
        )
        plan_records = tuple(
            record for record in records if record.key_purpose == "source_use_plan_v2"
        )
        spool_authority = LabClaimSpoolReceiptAuthorityV2(
            root_id="a" * 32,
            publisher_authority=SourceBrokerV2AuthorityRef(
                authority_id="source-stage",
                key_id="publisher-v1",
                purpose="publish-receipt",
                schema_version=1,
                generation=1,
                fence_hash="b" * 64,
            ),
        )
        material = LabClaimPublicationWorkerVerificationConfig(
            audience="lab-worker",
            trust_certificate=certificate,
            root_public_keys=root_records,
            finalizer_public_keys=finalizer_records,
            source_plan_public_keys=plan_records,
            spool_receipt_authority=spool_authority.model_dump(mode="json"),
            current_claim_socket_path=str(runtime / "current-claim.sock"),
            current_claim_socket_owner_uid=os.getuid(),
            current_claim_socket_group_gid=os.getgid(),
            current_claim_socket_mode=0o600,
            current_claim_server_uid=os.getuid(),
            current_claim_server_gid=os.getgid(),
            current_claim_timeout_ms=1_000,
        )
        material_path = runtime / "claim-publication-verifier.json"
        material_path.write_bytes(canonical_model_json_bytes(material))
        material_path.chmod(0o600)
        monkeypatch.setattr(settings, "lab_v2_claim_publication_enabled", True)
        monkeypatch.setattr(settings, "lab_claim_publication_worker_verifier_path", material_path)
        captured: dict[str, object] = {}

        class FakeWorker:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            def run_once(self) -> SimpleNamespace:
                return SimpleNamespace(status="idle", model_dump_json=lambda: "{}")

        monkeypatch.setattr(lab_worker, "LabWorker", FakeWorker)
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)

        assert (
            cmd_lab_worker(
                argparse.Namespace(
                    worker_id="worker-a",
                    once=True,
                    expected_checkout_root=_LAB_EXPECTED_ROOT,
                    trusted_git_path=_LAB_TRUSTED_GIT,
                    startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
                )
            )
            == 0
        )
        assert captured["v2_claim_publication_enabled"] is True
        assert type(captured["claim_publication_verifier"]).__name__ == (
            "LabClaimPublicationWorkerVerifier"
        )

    def test_real_cli_worker_starts_across_a_to_b_with_one_runtime_authority(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        import argparse

        from rquant import lab_daemon, lab_shard_protocol, lab_worker
        from rquant.cli import _lab_runtime_layout, cmd_lab_worker
        from rquant.config import settings
        from rquant.runtime_market_session import MarketCalendarAuthority

        current = {"sha": "a" * 40}
        data = tmp_path / "data"
        runtime = data / "lab-runtime"
        managed = {
            "lab_runtime_dir": runtime,
            "lab_jobs_path": runtime / "lab_jobs.sqlite3",
            "lab_job_command_dir": runtime / "commands",
            "lab_job_claim_dir": runtime / "claims",
            "lab_job_report_dir": runtime / "reports",
            "lab_worker_artifact_dir": runtime / "worker-artifacts",
            "lab_final_artifact_dir": runtime / "final-artifacts",
            "lab_artifact_commit_dir": runtime / "artifact-commits",
            "lab_daemon_lock_dir": runtime / "locks",
            "lab_finalizer_state_dir": runtime / "finalizer-state",
            "lab_readiness_dir": runtime / "readiness",
        }
        monkeypatch.setattr(settings, "data_dir", data)
        for field, path in managed.items():
            monkeypatch.setattr(settings, field, path)
        monkeypatch.setattr(
            lab_daemon,
            "ensure_private_directory",
            _real_ensure_private_directory,
        )
        directories, files, legacy = _lab_runtime_layout()
        runtime.mkdir(parents=True, mode=0o700)
        database = files["lab jobs SQLite"]
        database.write_bytes(b"")
        database.chmod(0o600)
        lab_daemon.prepare_lab_runtime_layout(
            runtime,
            checkout_root=Path(_LAB_EXPECTED_ROOT),
            managed_directories=directories,
            managed_files=files,
            legacy_paths=legacy,
            mutation_guard=lambda: "a" * 40,
        )
        authority_id = json.loads(
            lab_daemon.lab_runtime_prepared_path(runtime).read_text(encoding="utf-8")
        )["runtime_authority_id"]

        class MinimalSpool:
            def __init__(self, _path: Path, *, mutation_guard: object) -> None:
                assert callable(mutation_guard)

        class MinimalWorker:
            def __init__(self, **kwargs: object) -> None:
                self.verify = kwargs["verified_code_sha_provider"]

            def run_once(self) -> SimpleNamespace:
                assert callable(self.verify)
                assert self.verify() == current["sha"]
                return SimpleNamespace(status="idle", model_dump_json=lambda: "{}")

        monkeypatch.setattr(
            lab_daemon,
            "require_lab_runtime_binding",
            lambda _root, _git, **_kwargs: current["sha"],
        )
        monkeypatch.setattr(
            lab_daemon,
            "verify_lab_runtime_prepared",
            _real_verify_lab_runtime_prepared,
        )
        monkeypatch.setattr(lab_shard_protocol, "LabClaimSpool", MinimalSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabReportSpool", MinimalSpool)
        monkeypatch.setattr(lab_worker, "LabWorker", MinimalWorker)
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)
        args = argparse.Namespace(
            worker_id="worker-a",
            once=True,
            expected_checkout_root=_LAB_EXPECTED_ROOT,
            trusted_git_path=_LAB_TRUSTED_GIT,
            startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
        )

        def publish_calendar(commit: str) -> None:
            calendar = MarketCalendarAuthority.create(
                schema_version=1,
                exchange="SSE",
                producer_commit=commit,
                coverage_start=date(2026, 1, 1),
                coverage_end=date(2027, 1, 1),
                open_dates=(),
                generated_at=datetime(2025, 12, 31, tzinfo=UTC),
            )
            assert settings.rquant_lab_trade_calendar_path is not None
            settings.rquant_lab_trade_calendar_path.write_text(
                json.dumps(calendar.model_dump(mode="json"), separators=(",", ":")),
                encoding="utf-8",
            )
            settings.rquant_lab_trade_calendar_path.chmod(0o600)

        publish_calendar(current["sha"])
        assert cmd_lab_worker(args) == 0
        current["sha"] = "b" * 40
        publish_calendar(current["sha"])
        assert cmd_lab_worker(args) == 0

        assert (
            json.loads(lab_daemon.lab_runtime_prepared_path(runtime).read_text(encoding="utf-8"))[
                "runtime_authority_id"
            ]
            == authority_id
        )

    def test_cmd_lab_worker_forever_installs_both_stop_signals(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import argparse
        import signal

        from rquant import lab_shard_protocol, lab_worker
        from rquant.cli import cmd_lab_worker

        handlers: dict[int, object] = {}
        calls: list[str] = []

        class FakeSpool:
            def __init__(self, _path: Path, *, mutation_guard: object) -> None:
                assert callable(mutation_guard)

        class FakeWorker:
            def __init__(self, **_kwargs: object) -> None:
                pass

            def request_stop(self) -> None:
                calls.append("request_stop")

            def run_forever(self, *, install_signal_handlers: bool) -> None:
                assert install_signal_handlers is False
                calls.append("run_forever")
                for signum in (signal.SIGINT, signal.SIGTERM):
                    handler = handlers[signum]
                    assert callable(handler)
                    handler(signum, None)

        def fake_signal(signum: int, handler: object) -> object:
            previous = handlers.get(signum, signal.SIG_DFL)
            handlers[signum] = handler
            return previous

        monkeypatch.setattr(lab_shard_protocol, "LabClaimSpool", FakeSpool)
        monkeypatch.setattr(lab_shard_protocol, "LabReportSpool", FakeSpool)
        monkeypatch.setattr(lab_worker, "LabWorker", FakeWorker)
        monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)
        monkeypatch.setattr(signal, "signal", fake_signal)

        result = cmd_lab_worker(
            argparse.Namespace(
                worker_id="worker-a",
                once=False,
                expected_checkout_root=_LAB_EXPECTED_ROOT,
                trusted_git_path=_LAB_TRUSTED_GIT,
                startup_deadline_monotonic=_LAB_STARTUP_DEADLINE,
            )
        )

        assert result == 0
        assert calls == ["run_forever", "request_stop", "request_stop"]


class TestDailyDagDevAbsenceGuardRegression:
    """daily-dag-dev 必须在生产 root 出现或路径被 symlink 污染时拒绝且零写入。"""

    @staticmethod
    def _dev_argv(profile_root: Path, *extra: str) -> list[str]:
        return [
            "daily-dag-dev",
            "--profile-root",
            str(profile_root),
            "--trade-date",
            "2026-08-03",
            "--source-generation-id",
            "a" * 64,
            "--source-content-hash",
            "b" * 64,
            "--command-manifest-hash",
            "e" * 64,
            "--code-commit",
            "c" * 40,
            "--profile-hash",
            "d" * 64,
            *extra,
        ]

    @staticmethod
    def _patch_runtime_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
        from rquant import runtime_deployment_profile as deployment_module

        monkeypatch.setattr(deployment_module, "LINUX_PRODUCTION_RUNTIME_ROOT", root)

    def test_symlinked_ancestor_with_missing_target_rejects_and_leaves_dev_root_empty(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from rquant.cli import cmd_daily_dag

        base = tmp_path.resolve()
        missing_target = base / "real-parent"
        parent = base / "fixed-production-parent"
        parent.symlink_to(missing_target, target_is_directory=True)
        self._patch_runtime_root(monkeypatch, parent / "runtime")
        dev_root = base / "dev-root"
        args = build_parser().parse_args(self._dev_argv(dev_root))

        assert cmd_daily_dag(args) == 2
        assert capsys.readouterr().out == ""
        assert not dev_root.exists()
        assert not missing_target.exists()

    def test_absent_to_present_race_rejects_preview_and_leaves_dev_root_empty(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from rquant import cli as cli_module

        base = tmp_path.resolve()
        parent = base / "fixed-production-parent"
        parent.mkdir(mode=0o700)
        runtime_root = parent / "runtime"
        self._patch_runtime_root(monkeypatch, runtime_root)
        dev_root = base / "dev-root"

        original_plan = cli_module._daily_dag_control_plan

        def racing_plan(namespace: argparse.Namespace):
            plan = original_plan(namespace)
            runtime_root.mkdir(mode=0o700)
            return plan

        monkeypatch.setattr(cli_module, "_daily_dag_control_plan", racing_plan)
        args = build_parser().parse_args(self._dev_argv(dev_root))

        assert cli_module.cmd_daily_dag(args) == 2
        assert capsys.readouterr().out == ""
        assert not dev_root.exists()


def test_lab_claim_finalizer_command_is_registered_and_requires_private_material(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import config
    from rquant.cli import build_parser, cmd_lab_claim_finalizer
    from rquant.lab_daemon import LabDaemonConfigurationError

    parser = build_parser()
    parsed = parser.parse_args(
        [
            "lab-claim-finalizer",
            "--expected-checkout-root",
            "/tmp/checkout",
            "--trusted-git-path",
            "/usr/bin/git",
            "--deployment-generation",
            "generation-a",
            "--deployment-lock-path",
            "/tmp/deployment.lock",
            "--deployment-generation-fd",
            "9",
            "--startup-deadline-monotonic",
            "1",
            "--once",
        ]
    )
    assert parsed.command == "lab-claim-finalizer"
    assert parsed.once is True

    monkeypatch.setattr(config.settings, "lab_claim_finalizer_enabled", True, raising=False)
    monkeypatch.setattr(
        config.settings,
        "lab_claim_finalizer_runtime_material_path",
        None,
        raising=False,
    )
    with pytest.raises(LabDaemonConfigurationError, match="private.*material|material.*missing"):
        cmd_lab_claim_finalizer(parsed)


def test_lab_claim_finalizer_command_runs_real_authority_finalizer_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    import argparse
    from contextlib import nullcontext
    from tempfile import mkdtemp
    from threading import Event, Thread

    from rquant import cli as cli_module
    from rquant import config, lab_daemon, source_broker_v2_authority
    from rquant.current_claim_authority import ExternalCurrentClaimRootConfig
    from rquant.external_monotonic_root import UnixSocketExternalMonotonicRootManifest
    from rquant.external_monotonic_root_service import (
        EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
        ExternalMonotonicRootUnixService,
        ExternalRootServiceConfiguration,
        OpenSslExternalMonotonicRootSigner,
        PersistentExternalMonotonicRootBackend,
    )
    from rquant.lab_claim_finalizer_composition import (
        LabClaimFinalizerRuntimeMaterial,
        compose_production_lab_claim_finalizer_daemon,
    )
    from rquant.lab_claim_finalizer_daemon import LabClaimFinalizerDaemon
    from rquant.lab_claim_publication import ClaimPublicationStatus
    from rquant.lab_shard_protocol import LabShardClaimV2
    from rquant.source_broker_v2_job_protocol import SourceBrokerV2AuthorityRef
    from rquant.strict_json import canonical_model_json_bytes
    from tests.unit.test_adapter_manifest import create_test_authorities
    from tests.unit.test_lab_claim_publication import (
        _finalizer_issuer,
        _prepared_authority_finalizer,
    )

    runtime_root = (tmp_path / "lab-runtime").resolve()
    runtime_root.mkdir(mode=0o700)
    private_root = (tmp_path / "claim-finalizer-private").resolve()
    private_root.mkdir(mode=0o700)
    authorities = create_test_authorities(private_root / "keys")
    prepared_at = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=10)
    _seed, store, held = _prepared_authority_finalizer(
        runtime_root,
        authority_set=authorities,
        now=prepared_at,
    )

    current_root_keys = private_root / "current-root-keys"
    current_root_keys.mkdir(mode=0o700)
    current_root_private = current_root_keys / "root.private.pem"
    current_root_public = current_root_keys / "root.public.pem"
    subprocess.run(
        (
            "openssl",
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(current_root_private),
        ),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (
            "openssl",
            "pkey",
            "-in",
            str(current_root_private),
            "-pubout",
            "-out",
            str(current_root_public),
        ),
        check=True,
        capture_output=True,
    )
    current_root_private.chmod(0o600)
    current_root_public.chmod(0o600)
    current_root_signer = OpenSslExternalMonotonicRootSigner(
        private_key_path=current_root_private,
        public_key_path=current_root_public,
        issuer="claim-current-root",
        key_id="claim-current-root-v1",
        key_purpose="current-claim-monotonic-root",
        allowed_namespaces=frozenset(
            {
                EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
                "rquant-current-claim-anti-rollback-root/v1",
            }
        ),
    )
    socket_root = Path(mkdtemp(prefix="rqcf-")).resolve()
    socket_root.chmod(0o700)
    socket_path = socket_root / "root.sock"
    manifest = UnixSocketExternalMonotonicRootManifest(
        role="current_claim_monotonic_root",
        authority_id="claim-current-root",
        store_id="claim-current-root-store",
        rollback_domain_id="claim-current-root-domain",
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        socket_mode=0o600,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=2_000,
        max_response_bytes=1024 * 1024,
    )
    root_config = ExternalCurrentClaimRootConfig(
        transport=manifest.transport,
        transport_manifest_hash=manifest.manifest_hash,
        root_authority_id=manifest.authority_id,
        root_store_id=manifest.store_id,
        root_issuer=current_root_signer.issuer,
        root_key_id=current_root_signer.key_id,
        root_public_key_fingerprint=current_root_signer.public_key_fingerprint,
        witness_rollback_domain_id=manifest.rollback_domain_id,
        local_rollback_domain_id="claim-finalizer-local-domain",
    )
    service = ExternalMonotonicRootUnixService(
        configuration=ExternalRootServiceConfiguration(
            socket_path=socket_path,
            socket_uid=os.getuid(),
            socket_gid=os.getgid(),
            service_uid=os.getuid(),
            service_gid=os.getgid(),
            allowed_peer_uid=os.getuid(),
            allowed_peer_gid=os.getgid(),
            socket_mode=0o600,
            socket_directory_mode=0o700,
            role=manifest.role,
            authority_id=manifest.authority_id,
            store_id=manifest.store_id,
            rollback_domain_id=manifest.rollback_domain_id,
            transport_manifest_hash=manifest.manifest_hash,
        ),
        backend=PersistentExternalMonotonicRootBackend(
            private_root / "current-root.sqlite3",
            role=manifest.role,
            authority_id=manifest.authority_id,
            store_id=manifest.store_id,
        ),
        handler=source_broker_v2_authority._CurrentClaimRootRoleHandler(  # noqa: SLF001
            current_root_signer
        ),
        probe_signer=current_root_signer,
    )
    stop = Event()
    service_errors: list[BaseException] = []

    def serve_current_root() -> None:
        try:
            service.serve_forever(stop=stop)
        except BaseException as exc:
            service_errors.append(exc)

    service_thread = Thread(target=serve_current_root, name="test-claim-current-root")

    def close_current_root() -> None:
        stop.set()
        service.wake()
        if service_thread.ident is not None:
            service_thread.join(timeout=5)
        socket_path.unlink(missing_ok=True)
        if socket_root.exists():
            socket_root.rmdir()

    request.addfinalizer(close_current_root)

    issuer = _finalizer_issuer(store, authority_set=authorities)
    root_secret_path = private_root / "finalizer-root.secret"
    root_secret_path.write_bytes(b"test-lab-claim-finalizer-root-key-0001")
    root_secret_path.chmod(0o600)
    records = authorities.records
    material = LabClaimFinalizerRuntimeMaterial(
        audience="lab-claim-publication",
        trust_certificate=issuer._trust_certificate,  # noqa: SLF001
        root_public_keys=tuple(
            record for record in records if record.key_purpose == "lab_claim_finalizer_root"
        ),
        finalizer_public_keys=tuple(
            record for record in records if record.key_purpose == "lab_claim_finalizer"
        ),
        adapter_manifest_public_keys=tuple(
            record for record in records if record.key_purpose == "adapter_manifest"
        ),
        scheduler_intent_public_keys=tuple(
            record for record in records if record.key_purpose == "scheduler_intent_authorization"
        ),
        source_plan_public_keys=tuple(
            record for record in records if record.key_purpose == "source_use_plan_v2"
        ),
        finalizer_runtime_private_key_path=(
            private_root / "keys" / "finalizer-runtime-v1.private.pem"
        ),
        finalizer_root_secret_path=root_secret_path,
        source_stage_path=runtime_root / "source-stage.sqlite3",
        source_queue_path=runtime_root / "source-runner.sqlite3",
        spool_receipt_publisher=SourceBrokerV2AuthorityRef(
            authority_id="finalizer-authority",
            key_id="finalizer-key-v2",
            purpose="rquant-finalizer-receipt",
            schema_version=2,
            generation=7,
            fence_hash="7" * 64,
        ),
        current_claim_state_path=private_root / "production-current-claim.sqlite3",
        current_claim_authority_id="production-current-claim",
        current_claim_plan_private_key_path=private_root / "keys" / "plan-v2.private.pem",
        current_claim_external_root_manifest=manifest,
        current_claim_external_root_config=root_config,
        current_claim_external_root_public_key_path=current_root_public,
    )
    material_path = private_root / "runtime-material.json"
    material_path.write_bytes(canonical_model_json_bytes(material))
    material_path.chmod(0o600)

    setting_values = config.settings.model_dump()
    setting_values.update(
        {
            "data_dir": tmp_path / "data",
            "duckdb_path": tmp_path / "duckdb" / "rquant.duckdb",
            "duckdb_readonly_path": None,
            "backfill_state_path": None,
            "parquet_dir": tmp_path / "parquet",
            "log_dir": tmp_path / "logs",
            "lab_runtime_dir": runtime_root,
            "lab_jobs_path": store.path,
            "lab_job_command_dir": runtime_root / "commands",
            "lab_job_claim_dir": runtime_root / "finalizer-claims",
            "lab_job_report_dir": runtime_root / "reports",
            "lab_worker_artifact_dir": runtime_root / "worker-artifacts",
            "lab_final_artifact_dir": runtime_root / "final-artifacts",
            "lab_artifact_commit_dir": runtime_root / "artifact-commits",
            "lab_daemon_lock_dir": runtime_root / "locks",
            "lab_finalizer_state_dir": runtime_root / "finalizer-state",
            "lab_readiness_dir": runtime_root / "readiness",
            "lab_finalizer_authority_key_path": None,
            "lab_finalizer_authority_keyring_path": None,
            "lab_claim_publication_worker_verifier_path": None,
            "lab_highwater_trusted_keyring_path": None,
            "lab_claim_finalizer_enabled": True,
            "lab_claim_finalizer_runtime_material_path": material_path,
            "lab_claim_finalizer_owner_id": "finalizer-replay",
            "lab_claim_finalizer_lease_seconds": 60,
            "lab_claim_finalizer_poll_interval_ms": 10,
            "lab_claim_finalizer_max_publications_per_tick": 1,
            "lab_claim_finalizer_failure_backoff_seconds": 1,
            "lab_claim_finalizer_failure_backoff_max_seconds": 2,
            "lab_jobs_busy_timeout_ms": 5_000,
            "lab_trusted_git_path": Path("/usr/bin/git"),
        }
    )
    test_settings = config.Settings.model_validate(setting_values)

    class FakeLock:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> FakeLock:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    monkeypatch.setattr(config, "settings", test_settings)
    monkeypatch.setattr(lab_daemon, "LabDaemonLock", FakeLock)
    monkeypatch.setattr(
        lab_daemon,
        "ensure_private_directory",
        lambda path, **_kwargs: path,
    )
    monkeypatch.setattr(
        cli_module,
        "_establish_lab_runtime_identity",
        lambda _args: ("1" * 40, object(), object(), lambda: nullcontext()),
    )
    monkeypatch.setattr(cli_module, "_verify_prepared_lab_runtime", lambda *_args: None)
    monkeypatch.setattr(
        cli_module,
        "_lab_daemon_readiness_context",
        lambda *_a, **_k: nullcontext(),
    )
    monkeypatch.setattr(cli_module, "setup_logging", lambda: None)

    direct_daemon: LabClaimFinalizerDaemon | None = None
    try:
        service_thread.start()
        assert service.ready.wait(timeout=5), repr(service_errors)
        direct_daemon = compose_production_lab_claim_finalizer_daemon(
            settings=test_settings,
            mutation_guard=lambda: nullcontext(),
        )
        forbidden_slots = {"_provider", "_adapter", "_worker", "_runtime_client"}
        assert forbidden_slots.isdisjoint(LabClaimFinalizerDaemon.__slots__)
        daemon_capabilities = tuple(
            getattr(direct_daemon, slot) for slot in LabClaimFinalizerDaemon.__slots__
        )
        assert all(capability is not current_root_signer for capability in daemon_capabilities)
        assert all(
            capability is not authorities.finalizer_trust_root for capability in daemon_capabilities
        )
        assert not isinstance(
            direct_daemon._authority_issuer._runtime_signer,  # noqa: SLF001
            OpenSslExternalMonotonicRootSigner,
        )
        assert (
            direct_daemon._authority_issuer._runtime_signer.key_purpose  # noqa: SLF001
            == "lab_claim_finalizer"
        )
        assert (
            direct_daemon._authority_issuer._runtime_signer  # noqa: SLF001
            is not authorities.finalizer_trust_root
        )
        preimage = LabShardClaimV2.model_validate_json(held.claim_preimage_bytes, strict=True)
        direct_daemon._current_claim_authority.replace_current(preimage)  # noqa: SLF001
        direct_daemon.close()
        direct_daemon = None

        result = cli_module.cmd_lab_claim_finalizer(
            argparse.Namespace(
                expected_checkout_root=tmp_path,
                trusted_git_path="/usr/bin/git",
                once=True,
            )
        )
    finally:
        if direct_daemon is not None:
            direct_daemon.close()
        close_current_root()

    assert result == 0, repr(
        store.list_claim_publication_finalizer_observations(held.identity.attempt_id)
    )
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
    assert record.spool_receipt_bytes
    store.validate_finalizer_published_attestation(
        held.identity,
        trust_verifier=issuer._trust_verifier,  # noqa: SLF001
        now=datetime.now(UTC),
    )
    assert not service_thread.is_alive()
    assert service_errors == []
    assert not socket_path.exists()


def test_claim_finalizer_production_composition_rejects_missing_private_and_certificate(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_composition import (
        compose_production_lab_claim_finalizer_daemon,
    )
    from rquant.lab_daemon import LabDaemonConfigurationError

    material = tmp_path / "claim-finalizer-private.json"
    material.write_bytes(b"{}")
    material.chmod(0o600)
    settings = SimpleNamespace(lab_claim_finalizer_runtime_material_path=material)

    with pytest.raises(LabDaemonConfigurationError, match="runtime material is invalid"):
        compose_production_lab_claim_finalizer_daemon(settings=settings)

    @pytest.mark.parametrize("action", ["apply", "recover", "retry"])
    def test_absent_to_present_race_rejects_side_effect_actions(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        action: str,
    ) -> None:
        from rquant import cli as cli_module

        base = tmp_path.resolve()
        parent = base / "fixed-production-parent"
        parent.mkdir(mode=0o700)
        runtime_root = parent / "runtime"
        self._patch_runtime_root(monkeypatch, runtime_root)
        dev_root = base / "dev-root"
        spool_root = base / "spool-root"
        spool_root.mkdir(mode=0o700)

        preview_args = build_parser().parse_args(self._dev_argv(dev_root))
        assert cli_module.cmd_daily_dag(preview_args) == 0
        preview = json.loads(capsys.readouterr().out)
        assert not dev_root.exists()

        original_plan = cli_module._daily_dag_control_plan

        def racing_plan(namespace: argparse.Namespace):
            plan = original_plan(namespace)
            if not runtime_root.exists():
                runtime_root.mkdir(mode=0o700)
            return plan

        monkeypatch.setattr(cli_module, "_daily_dag_control_plan", racing_plan)
        args = build_parser().parse_args(
            self._dev_argv(
                dev_root,
                "--action",
                action,
                "--apply",
                "--run-id",
                preview["run_id"],
                "--plan-hash",
                preview["plan_hash"],
                "--source-spool-root",
                str(spool_root),
            )
        )

        assert cli_module.cmd_daily_dag(args) == 2
        assert capsys.readouterr().out == ""
        assert not dev_root.exists()
