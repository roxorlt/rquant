"""CLI 入口单测 —— 仅验证 argparse 解析，不启动调度器。"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

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
        assert not args.skip_minute_backfill
        assert args.minute_lookback_days == 90

    def test_run_daily_with_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "run-daily",
            "--date", "2026-04-18",
            "--preset", "n-shape-pool1",
            "--skip-minute-backfill",
            "--minute-lookback-days", "60",
        ])
        assert args.date == "2026-04-18"
        assert args.preset == "n-shape-pool1"
        assert args.skip_minute_backfill
        assert args.minute_lookback_days == 60

    def test_no_command_returns_none(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

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
        from rquant import research_lake as lake_module
        from rquant import research_manifest as manifest_module
        from rquant.storage import duckdb as duckdb_module

        connection = MagicMock()
        summary = MagicMock()
        summary.model_dump_json.return_value = '{"status":"planned"}'
        observed: dict[str, object] = {}

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
                "--dry-run",
            ]
        )

        assert cli.cmd_research_export(args) == 0
        assert observed["require_replica"] is True
        assert observed["source"] is connection
        assert observed["dataset"] == "auction_bar"
        assert observed["start_date"] == date(2026, 7, 14)
        assert observed["end_date"] == date(2026, 7, 15)
        assert observed["dry_run"] is True
        assert observed["code_commit"] == "a" * 40
        connection.close.assert_called_once_with()
        assert capsys.readouterr().out.strip() == '{"status":"planned"}'


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
            build_parser().parse_args(
                ["trade-calendar-bootstrap", "--start-date", "20240201"]
            )

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
        monkeypatch.setattr(cli.logger, "info", info)

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
            ]
        )

        assert args.command == "suspension-backfill"
        assert args.start_date == date(2026, 7, 1)
        assert args.end_date == date(2026, 7, 15)
        assert args.full_refresh is True


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


class TestLimitUpPoolCommands:
    def test_repair_defaults_to_dry_run(self) -> None:
        args = build_parser().parse_args(["zt-pool-repair"])

        assert args.command == "zt-pool-repair"
        assert args.apply is False
        assert args.plan_id is None

    def test_repair_apply_requires_both_explicit_flags(self) -> None:
        plan_id = "a" * 64

        accepted = build_parser().parse_args(
            ["zt-pool-repair", "--apply", "--plan-id", plan_id]
        )
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
            build_parser().parse_args(
                ["zt-pool-repair", "--apply", "--plan-id", "not-a-plan"]
            )

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

        assert cli.cmd_zt_pool_repair(
            SimpleNamespace(apply=False, plan_id=None)
        ) == 0
        assert store._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone() == (1,)
        plan = build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None

        assert cli.cmd_zt_pool_repair(
            SimpleNamespace(apply=True, plan_id=plan.plan_id)
        ) == 0
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

        assert cli.cmd_zt_pool_repair(
            SimpleNamespace(apply=False, plan_id=None)
        ) == 1
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

        assert cli.cmd_zt_pool_repair(
            SimpleNamespace(apply=True, plan_id=plan.plan_id)
        ) == 1
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
    env["SYNC_TEST_COMPLETION_FILE"] = str(
        project / "data" / ".last-research-sync-date"
    )
    env["SYNC_TEST_LOCK_PID_FILE"] = str(
        project / "data" / ".sync-from-cloud.lock" / "pid"
    )
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
        assert calls_path.read_text(encoding="utf-8").splitlines()[0].startswith(
            "research-sync --backup"
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
        publish_observations = Path(
            env["SYNC_TEST_LOCK_PUBLISH_CALLS"]
        ).read_text(encoding="utf-8").splitlines()
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
            return {"failed_dates": [], "affected_codes": ["600000.SH"]}

        def fake_recompute(store, *, codes, status_mode):
            assert writer_active is True
            assert isinstance(store, _Store)
            assert codes == ["600000.SH"]
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

    def test_skip_state_recompute_keeps_invalidated_tails_without_rebuild(
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
        assert "陈旧状态尾部" in result.stdout


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
        args = build_parser().parse_args(
            ["midday-report", "--date", "2026-07-06", "--dry-run"]
        )
        assert args.command == "midday-report"
        assert args.date == "2026-07-06"
        assert args.dry_run


class TestRtMinuteFetchParser:
    def test_rt_minute_fetch_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-fetch",
            "--ts-code", "605366.SH,301051.SZ",
        ])
        assert args.command == "rt-minute-fetch"
        assert args.ts_code == ["605366.SH,301051.SZ"]
        assert args.freq == "1min"

    def test_rt_minute_fetch_accepts_repeated_codes(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-fetch",
            "--ts-code", "605366.SH",
            "--ts-code", "301051.SZ",
            "--freq", "5min",
        ])
        assert args.ts_code == ["605366.SH", "301051.SZ"]
        assert args.freq == "5min"


class TestRtMinuteDailyFetchParser:
    def test_rt_minute_daily_fetch_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-daily-fetch",
            "--ts-code", "605366.SH,301051.SZ",
        ])
        assert args.command == "rt-minute-daily-fetch"
        assert args.ts_code == ["605366.SH,301051.SZ"]
        assert args.freq == "1min"

    def test_rt_minute_daily_fetch_accepts_repeated_codes(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-daily-fetch",
            "--ts-code", "605366.SH",
            "--ts-code", "301051.SZ",
            "--freq", "5min",
        ])
        assert args.ts_code == ["605366.SH", "301051.SZ"]
        assert args.freq == "5min"


class TestGrowthBoardSurgeReplayParser:
    def test_growth_board_surge_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "growth-board-surge-replay",
            "--start-date", "2026-06-25",
            "--end-date", "2026-06-26",
        ])
        assert args.command == "growth-board-surge-replay"
        assert args.freq == "1min"
        assert args.min_signal_time == "09:30"
        assert args.lookback_days == 20
        assert args.min_hist_days == 10
        assert args.min_cum_amount_ratio == 1.4
        assert args.min_same_minute_amount_ratio == 2.0
        assert args.max_hold_days == 3
        assert args.require_inner_outer is False
        assert args.min_inner_outer_ratio == 1.0
        assert args.require_large_net_vol is False
        assert args.min_large_net_vol == 0.0
        assert args.factor_confirm is False
        assert args.factor_score_threshold == 45.0
        assert args.output is None

    def test_growth_board_surge_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "growth-board-surge-replay",
            "--start-date", "2026-06-25",
            "--end-date", "2026-06-26",
            "--freq", "5min",
            "--min-signal-time", "09:35",
            "--lookback-days", "30",
            "--min-hist-days", "15",
            "--min-cum-amount-ratio", "1.8",
            "--min-same-minute-amount-ratio", "3.0",
            "--max-hold-days", "2",
            "--require-inner-outer",
            "--min-inner-outer-ratio", "1.2",
            "--require-large-net-vol",
            "--min-large-net-vol", "100",
            "--factor-confirm",
            "--factor-score-threshold", "50",
            "--output", "/tmp/growth.csv",
        ])
        assert args.freq == "5min"
        assert args.min_signal_time == "09:35"
        assert args.lookback_days == 30
        assert args.min_hist_days == 15
        assert args.min_cum_amount_ratio == 1.8
        assert args.min_same_minute_amount_ratio == 3.0
        assert args.max_hold_days == 2
        assert args.require_inner_outer is True
        assert args.min_inner_outer_ratio == 1.2
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
        args = parser.parse_args([
            "minute-backfill",
            "--date", "2026-06-24",
            "--lookback-days", "90",
            "--freq", "5min",
            "--preset", "n-shape-pool2",
            "--ts-code", "600000.SH",
            "--dry-run",
        ])
        assert args.lookback_days == 90
        assert args.freq == "5min"
        assert args.preset == "n-shape-pool2"
        assert args.ts_code == "600000.SH"
        assert args.dry_run


class TestMinuteReplayParser:
    def test_minute_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
        ])
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
        args = parser.parse_args([
            "minute-replay",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
            "--preset", "n-shape-pool2",
            "--freq", "5min",
            "--entry-mode", "amount_surge",
            "--max-hold-days", "3",
            "--volume-profile",
            "--volume-profile-lookbacks", "90",
            "--output", "/private/tmp/replay.csv",
        ])
        assert args.preset == "n-shape-pool2"
        assert args.freq == "5min"
        assert args.entry_mode == "amount_surge"
        assert args.max_hold_days == 3
        assert args.volume_profile
        assert args.volume_profile_lookbacks == [90]
        assert args.output == "/private/tmp/replay.csv"

    def test_minute_replay_factor_confirm_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
            "--entry-mode", "factor_confirm",
            "--factor-score-threshold", "65",
        ])
        assert args.entry_mode == "factor_confirm"
        assert args.factor_score_threshold == 65.0


class TestMinuteReplayBackfillParser:
    def test_minute_replay_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay-backfill",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
        ])
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
        args = parser.parse_args([
            "minute-replay-backfill",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
            "--preset", "n-shape-pool2",
            "--freq", "5min",
            "--max-hold-days", "3",
            "--ts-code", "600000.SH",
            "--dry-run",
        ])
        assert args.preset == "n-shape-pool2"
        assert args.freq == "5min"
        assert args.max_hold_days == 3
        assert args.ts_code == "600000.SH"
        assert args.dry_run


class TestAuctionBackfillParser:
    def test_auction_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-backfill",
            "--start-date", "2025-01-01",
            "--end-date", "2025-02-18",
        ])
        assert args.command == "auction-backfill"
        assert args.start_date == "2025-01-01"
        assert args.end_date == "2025-02-18"
        assert not args.dry_run

    def test_auction_backfill_dry_run(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-backfill",
            "--start-date", "2025-01-01",
            "--end-date", "2025-02-18",
            "--dry-run",
        ])
        assert args.dry_run


class TestAuctionMinuteFallbackParser:
    def test_auction_minute_fallback_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-minute-fallback",
            "--date", "2026-06-26",
        ])
        assert args.command == "auction-minute-fallback"
        assert args.date == "2026-06-26"
        assert not args.dry_run


class TestAuctionGapReplayParser:
    def test_auction_gap_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
        ])
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
        args = parser.parse_args([
            "auction-gap-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
            "--gap-mode", "strict_high",
            "--st-filter", "literal_lower",
            "--min-ratio", "0.2",
            "--max-ratio", "2",
            "--output", "/private/tmp/auction-gap.csv",
        ])
        assert args.gap_mode == "strict_high"
        assert args.st_filter == "literal_lower"
        assert args.min_ratio == 0.2
        assert args.max_ratio == 2.0
        assert args.output == "/private/tmp/auction-gap.csv"


class TestAuctionGapMinuteReplayParser:
    def test_auction_gap_minute_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-minute-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
        ])
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
        args = parser.parse_args([
            "auction-gap-minute-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
            "--gap-mode", "strict_high",
            "--st-filter", "literal_lower",
            "--min-ratio", "0.2",
            "--max-ratio", "2",
            "--max-hold-days", "2",
            "--seal-hold-days", "3",
            "--seal-hold-max-open-times", "1",
            "--factor-score-threshold", "45",
            "--output", "/private/tmp/auction-gap-minute.csv",
        ])
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
        args = parser.parse_args([
            "auction-gap-minute-backfill",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
        ])
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
        assert calls == [{
            "required_tables": [
                "auction_bar",
                "daily_bar",
                "daily_state",
                "stock_status_daily",
            ]
        }]
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
        assert calls == [{
            "required_tables": [
                "auction_bar",
                "daily_bar",
                "daily_state",
                "minute_bar",
                "stock_status_daily",
            ]
        }]
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
        adapter.rt_min.return_value = pd.DataFrame([
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
        ])
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
        adapter.rt_min_daily.return_value = pd.DataFrame([
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
        ])
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
        adapter.moneyflow.return_value = pd.DataFrame([
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
        ])
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
        trades = pd.DataFrame([{
            "signal_date": "2026-06-25",
            "ts_code": "300001.SZ",
            "name": "创业样本",
            "entry_time": "2026-06-25 09:34:00",
            "entry_price": 10.8,
            "exit_time": "2026-06-26 15:00:00",
            "exit_price": 11.6,
            "exit_reason": "time_1d",
            "ret_pct": 7.4074,
        }])

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
            min_inner_outer_ratio=1.0,
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

    def test_partial_failure_returns_0_when_any_success(
        self, monkeypatch
    ) -> None:
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
    def test_main_catches_run_daily_error_and_notifies(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant.cli import main

        # Force run-daily to raise
        def boom(_args):
            raise ValueError("test boom")

        monkeypatch.setattr("rquant.cli.cmd_run_daily", boom)

        with (
            patch("sys.argv", ["rquant", "run-daily", "--no-ingest"]),
            patch("rquant.notify.notify") as mock_notify,
        ):
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
