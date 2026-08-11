"""Config 层单测：确保 .env 能正确加载、字段校验生效。"""

import stat
import unicodedata
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.config import Settings, _default_settings_env_file, settings


def _settings_values(tmp_path: Path) -> dict[str, object]:
    return {
        "_env_file": None,
        "tushare_token_main": "x" * 32,
        "data_dir": tmp_path / "data",
        "duckdb_path": tmp_path / "data" / "rquant.duckdb",
        "parquet_dir": tmp_path / "data" / "parquet",
        "log_dir": tmp_path / "logs",
    }


def test_runtime_can_disable_implicit_project_dotenv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RQUANT_DISABLE_DOTENV", raising=False)
    assert _default_settings_env_file() == ".env"

    monkeypatch.setenv("RQUANT_DISABLE_DOTENV", "1")
    assert _default_settings_env_file() is None

    monkeypatch.setenv("RQUANT_DISABLE_DOTENV", "true")
    assert _default_settings_env_file() is None


class TestSettings:
    def test_tushare_token_loaded(self) -> None:
        assert len(settings.tushare_token_main) >= 32

    def test_data_dir_exists(self) -> None:
        assert settings.data_dir.exists()
        assert settings.data_dir.is_dir()

    def test_duckdb_parent_exists(self) -> None:
        assert settings.duckdb_path.parent.exists()

    def test_app_env_valid(self) -> None:
        assert settings.app_env in ("dev", "prod")

    def test_lab_resource_admission_settings_load_from_rquant_environment(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        live_root = tmp_path / "runtime-health"
        calendar_path = tmp_path / "market-calendar.json"
        monkeypatch.setenv("RQUANT_LAB_RESOURCE_POLICY_VERSION", "lab-resource-v1")
        monkeypatch.setenv("RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON", '{"schema_version":1}')
        root_service_config = tmp_path / "external-root.json"
        resource_service_config = tmp_path / "resource-authority.json"
        monkeypatch.setenv(
            "RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH",
            str(root_service_config),
        )
        monkeypatch.setenv(
            "RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH",
            str(resource_service_config),
        )
        monkeypatch.setenv("RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT", str(live_root))
        monkeypatch.setenv("RQUANT_LAB_TRADE_CALENDAR_PATH", str(calendar_path))

        configured = Settings(**_settings_values(tmp_path))

        assert configured.rquant_lab_resource_policy_version == "lab-resource-v1"
        assert configured.rquant_lab_resource_authority_config_json == '{"schema_version":1}'
        assert configured.rquant_external_monotonic_root_service_config_path == (
            root_service_config
        )
        assert configured.rquant_resource_authority_service_config_path == (resource_service_config)
        assert configured.rquant_lab_live_slo_authority_root == live_root
        assert configured.rquant_lab_trade_calendar_path == calendar_path

    @pytest.mark.parametrize(
        "field",
        [
            "rquant_external_monotonic_root_service_config_path",
            "rquant_resource_authority_service_config_path",
        ],
    )
    def test_authority_service_config_paths_must_be_absolute(
        self,
        tmp_path: Path,
        field: str,
    ) -> None:
        with pytest.raises(ValidationError, match="absolute"):
            Settings(**_settings_values(tmp_path), **{field: Path("relative.json")})

    def test_backfill_state_uses_configurable_separate_sqlite_path(
        self,
        tmp_path: Path,
    ) -> None:
        state_path = tmp_path / "state" / "backfill.sqlite3"
        configured = Settings(
            **_settings_values(tmp_path),
            backfill_state_path=state_path,
            backfill_state_busy_timeout_ms=1_234,
        )

        assert configured.backfill_state_path_resolved == state_path
        assert configured.backfill_state_path_resolved != configured.duckdb_path
        assert state_path.parent.is_dir()
        assert configured.backfill_state_busy_timeout_ms == 1_234

    def test_backfill_state_path_defaults_under_data_dir(self, tmp_path: Path) -> None:
        configured = Settings(
            **_settings_values(tmp_path),
            backfill_state_path="",
        )

        assert configured.backfill_state_path_resolved == (
            tmp_path / "data" / "backfill_state.sqlite3"
        )

    def test_backfill_planner_resource_limits_are_configurable(
        self,
        tmp_path: Path,
    ) -> None:
        configured = Settings(
            **_settings_values(tmp_path),
            backfill_planner_memory_limit_mb=1_024,
            backfill_planner_threads=3,
        )

        assert configured.backfill_planner_memory_limit_mb == 1_024
        assert configured.backfill_planner_threads == 3

    def test_lab_job_paths_default_under_data_dir_without_runtime_side_effects(
        self,
        tmp_path: Path,
    ) -> None:
        configured = Settings(
            **_settings_values(tmp_path),
            lab_jobs_path="",
            lab_job_command_dir="",
            lab_job_claim_dir="",
            lab_job_report_dir="",
            lab_worker_artifact_dir="",
        )

        runtime = tmp_path / "data" / "lab-runtime"
        assert configured.lab_runtime_dir_resolved == runtime
        assert configured.lab_jobs_path_resolved == runtime / "lab_jobs.sqlite3"
        assert configured.lab_job_command_dir_resolved == runtime / "commands"
        assert configured.lab_job_claim_dir_resolved == runtime / "claims"
        assert configured.lab_job_report_dir_resolved == runtime / "reports"
        assert configured.lab_worker_artifact_dir_resolved == (runtime / "worker-artifacts")
        assert not runtime.exists()
        assert not configured.lab_job_command_dir_resolved.exists()
        assert not configured.lab_job_claim_dir_resolved.exists()
        assert not configured.lab_job_report_dir_resolved.exists()
        assert not configured.lab_worker_artifact_dir_resolved.exists()

    def test_lab_daemon_paths_default_to_distinct_absolute_pure_roots(
        self,
        tmp_path: Path,
    ) -> None:
        configured = Settings(**_settings_values(tmp_path))

        roots = {
            configured.lab_job_command_dir_resolved,
            configured.lab_job_claim_dir_resolved,
            configured.lab_job_report_dir_resolved,
            configured.lab_worker_artifact_dir_resolved,
            configured.lab_final_artifact_dir_resolved,
            configured.lab_artifact_commit_dir_resolved,
            configured.lab_daemon_lock_dir_resolved,
            configured.lab_finalizer_state_dir_resolved,
        }
        assert len(roots) == 8
        assert all(path.is_absolute() for path in roots)
        assert all(not path.exists() for path in roots)

    def test_default_lab_sqlite_parent_is_private_under_public_umask(
        self,
        tmp_path: Path,
    ) -> None:
        configured = Settings(**_settings_values(tmp_path))

        assert configured.lab_jobs_path_resolved.parent == configured.lab_runtime_dir_resolved
        assert configured.lab_runtime_dir_resolved == configured.data_dir / "lab-runtime"

    def test_lab_runtime_defaults_do_not_chmod_or_write_shared_data_parent(
        self,
        tmp_path: Path,
    ) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir(mode=0o755)
        data_dir.chmod(0o755)

        configured = Settings(**_settings_values(tmp_path))

        assert stat.S_IMODE(data_dir.stat().st_mode) == 0o755
        assert not configured.lab_runtime_dir_resolved.exists()
        assert configured.lab_jobs_path_resolved == (
            configured.lab_runtime_dir_resolved / "lab_jobs.sqlite3"
        )
        assert configured.lab_job_command_dir_resolved == (
            configured.lab_runtime_dir_resolved / "commands"
        )
        assert configured.lab_finalizer_state_dir_resolved == (
            configured.lab_runtime_dir_resolved / "finalizer-state"
        )

    def test_lab_managed_path_must_be_inside_private_runtime_root(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError, match="direct children"):
            Settings(
                **_settings_values(tmp_path),
                lab_job_command_dir=tmp_path / "outside-commands",
            )

    @pytest.mark.parametrize(
        "field",
        [
            "lab_jobs_path",
            "lab_job_command_dir",
            "lab_job_claim_dir",
            "lab_job_report_dir",
            "lab_worker_artifact_dir",
            "lab_final_artifact_dir",
            "lab_artifact_commit_dir",
            "lab_daemon_lock_dir",
            "lab_finalizer_state_dir",
        ],
    )
    def test_lab_daemon_rejects_relative_managed_paths(
        self,
        tmp_path: Path,
        field: str,
    ) -> None:
        with pytest.raises(ValidationError, match="absolute"):
            Settings(**_settings_values(tmp_path), **{field: Path("relative") / field})

    def test_lab_daemon_rejects_nested_managed_roots(self, tmp_path: Path) -> None:
        root = tmp_path / "data" / "lab-root"
        with pytest.raises(ValidationError, match="nested"):
            Settings(
                **_settings_values(tmp_path),
                lab_job_command_dir=root,
                lab_job_claim_dir=root / "claims",
            )

    @pytest.mark.parametrize("alias_kind", ["case", "unicode"])
    def test_lab_daemon_rejects_macos_normalized_path_aliases(
        self,
        tmp_path: Path,
        alias_kind: str,
    ) -> None:
        root = tmp_path / "not-created"
        if alias_kind == "case":
            command = root / "LabCommands"
            claim = root / "labcommands"
        else:
            command = root / unicodedata.normalize("NFC", "cafe\u0301")
            claim = root / unicodedata.normalize("NFD", "cafe\u0301")

        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **_settings_values(tmp_path),
                lab_job_command_dir=command,
                lab_job_claim_dir=claim,
            )

        assert not root.exists()

    def test_lab_daemon_rejects_reverse_nested_managed_root_under_database(
        self,
        tmp_path: Path,
    ) -> None:
        root = tmp_path / "not-created"
        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **_settings_values(tmp_path),
                lab_jobs_path=root / "lab.sqlite3",
                lab_job_command_dir=root / "lab.sqlite3" / "commands",
            )
        assert not root.exists()

    def test_lab_daemon_rejects_reverse_nested_managed_root_under_key_file(
        self,
        tmp_path: Path,
    ) -> None:
        root = tmp_path / "not-created"
        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **_settings_values(tmp_path),
                lab_finalizer_authority_key_path=root / "authority.key",
                lab_job_command_dir=root / "authority.key" / "commands",
            )
        assert not root.exists()

    def test_lab_daemon_rejects_nested_database_paths(self, tmp_path: Path) -> None:
        values = _settings_values(tmp_path)
        values["duckdb_path"] = tmp_path / "data" / "operational.duckdb"
        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **values,
                lab_jobs_path=values["duckdb_path"] / "lab.sqlite3",
            )

    @pytest.mark.parametrize(
        ("existing_field", "lab_inside_existing"),
        [
            ("research_lake_dir", True),
            ("research_lake_dir", False),
            ("research_staging_dir", True),
            ("research_staging_dir", False),
        ],
    )
    def test_lab_daemon_rejects_nesting_with_existing_managed_roots(
        self,
        tmp_path: Path,
        existing_field: str,
        lab_inside_existing: bool,
    ) -> None:
        root = tmp_path / "not-created"
        existing = root / "existing"
        lab = existing / "lab" if lab_inside_existing else root
        existing = existing if lab_inside_existing else lab / "existing"

        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **_settings_values(tmp_path),
                **{
                    existing_field: existing,
                    "lab_job_command_dir": lab,
                },
            )

        assert not root.exists()

    def test_lab_database_conflict_does_not_create_database_parent(
        self,
        tmp_path: Path,
    ) -> None:
        root = tmp_path / "not-created"
        values = _settings_values(tmp_path)
        values["duckdb_path"] = root / "operational.duckdb"

        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **values,
                lab_job_command_dir=root / "operational.duckdb" / "commands",
            )

        assert not root.exists()

    def test_lab_daemon_rejects_absolute_noncanonical_path(self, tmp_path: Path) -> None:
        noncanonical = tmp_path / "state" / ".." / "commands"
        with pytest.raises(ValidationError, match="canonical"):
            Settings(
                **_settings_values(tmp_path),
                lab_job_command_dir=noncanonical,
            )

    def test_lab_daemon_rejects_relative_data_dir_before_creating_defaults(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        values = _settings_values(tmp_path)
        values["data_dir"] = Path("relative-data")

        with pytest.raises(ValidationError, match="DATA_DIR.*absolute canonical"):
            Settings(**values)

        assert not (tmp_path / "relative-data").exists()

    def test_lab_conflict_validation_does_not_create_managed_paths(
        self,
        tmp_path: Path,
    ) -> None:
        root = tmp_path / "not-created"
        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(
                **_settings_values(tmp_path),
                lab_job_command_dir=root / "spool",
                lab_job_claim_dir=root / "spool" / "claims",
            )

        assert not root.exists()

    def test_invalid_lab_config_creates_no_base_or_managed_directories(
        self,
        tmp_path: Path,
    ) -> None:
        data_dir = tmp_path / "fresh-data"
        parquet_dir = tmp_path / "fresh-parquet"
        log_dir = tmp_path / "fresh-logs"
        managed = tmp_path / "fresh-managed"
        values = _settings_values(tmp_path)
        values.update(
            data_dir=data_dir,
            duckdb_path=data_dir / "rquant.duckdb",
            parquet_dir=parquet_dir,
            log_dir=log_dir,
            lab_job_command_dir=managed,
            lab_job_claim_dir=managed / "claims",
        )

        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(**values)

        assert not data_dir.exists()
        assert not parquet_dir.exists()
        assert not log_dir.exists()
        assert not managed.exists()

    @pytest.mark.parametrize(
        "existing_field",
        ["parquet_dir", "log_dir", "panorama_users_path"],
    )
    @pytest.mark.parametrize("relation", ["equal", "lab_inside", "existing_inside"])
    def test_lab_paths_are_bidirectionally_isolated_from_other_write_paths(
        self,
        tmp_path: Path,
        existing_field: str,
        relation: str,
    ) -> None:
        root = tmp_path / "not-created"
        existing = root / "existing"
        if relation == "equal":
            lab = existing
        elif relation == "lab_inside":
            lab = existing / "lab"
        else:
            lab = root
            existing = lab / "existing"
        values = _settings_values(tmp_path)
        configured_field = (
            "RQUANT_PANORAMA_USERS_PATH"
            if existing_field == "panorama_users_path"
            else existing_field
        )
        values[configured_field] = existing
        values["lab_job_command_dir"] = lab

        with pytest.raises(ValidationError, match="alias or nest"):
            Settings(**values)

        assert not root.exists()

    def test_lab_daemon_rejects_database_inside_managed_root(self, tmp_path: Path) -> None:
        root = tmp_path / "data" / "commands"
        with pytest.raises(ValidationError, match="database"):
            Settings(
                **_settings_values(tmp_path),
                lab_job_command_dir=root,
                lab_jobs_path=root / "lab.sqlite3",
            )

    def test_lab_scheduler_runtime_settings_are_configurable(
        self,
        tmp_path: Path,
    ) -> None:
        configured = Settings(
            **_settings_values(tmp_path),
            lab_jobs_busy_timeout_ms=1_234,
            lab_scheduler_poll_interval_ms=250,
            lab_scheduler_lease_seconds=90,
            lab_scheduler_heartbeat_seconds=30,
            lab_scheduler_shard_lease_seconds=120,
            lab_scheduler_max_commands_per_tick=12,
            lab_scheduler_max_reports_per_tick=11,
            lab_scheduler_max_plans_per_tick=13,
            lab_scheduler_max_claims_per_tick=3,
            lab_scheduler_max_claim_authority_per_tick=14,
            lab_scheduler_worker_ids="worker-a, worker-b",
            lab_worker_poll_interval_ms=125,
            lab_worker_heartbeat_seconds=15,
            lab_worker_lease_extension_seconds=90,
            lab_worker_receipt_timeout_seconds=20,
            lab_worker_max_shards_per_tick=1,
            lab_worker_id="worker-a",
            lab_finalizer_poll_interval_ms=500,
            lab_finalizer_max_jobs_per_tick=7,
            lab_finalizer_failure_cooldown_seconds=45,
            lab_finalizer_failure_cooldown_max_seconds=300,
            lab_scheduler_max_artifact_commits_per_tick=9,
        )

        assert configured.lab_jobs_busy_timeout_ms == 1_234
        assert configured.lab_scheduler_poll_interval_ms == 250
        assert configured.lab_scheduler_lease_seconds == 90
        assert configured.lab_scheduler_heartbeat_seconds == 30
        assert configured.lab_scheduler_shard_lease_seconds == 120
        assert configured.lab_scheduler_max_commands_per_tick == 12
        assert configured.lab_scheduler_max_reports_per_tick == 11
        assert configured.lab_scheduler_max_plans_per_tick == 13
        assert configured.lab_scheduler_max_claims_per_tick == 3
        assert configured.lab_scheduler_max_claim_authority_per_tick == 14
        assert configured.lab_scheduler_worker_id_list == ("worker-a", "worker-b")
        assert configured.lab_worker_poll_interval_ms == 125
        assert configured.lab_worker_heartbeat_seconds == 15
        assert configured.lab_worker_lease_extension_seconds == 90
        assert configured.lab_worker_receipt_timeout_seconds == 20
        assert configured.lab_worker_max_shards_per_tick == 1
        assert configured.lab_worker_id == "worker-a"
        assert configured.lab_finalizer_poll_interval_ms == 500
        assert configured.lab_finalizer_max_jobs_per_tick == 7
        assert configured.lab_finalizer_failure_cooldown_seconds == 45
        assert configured.lab_finalizer_failure_cooldown_max_seconds == 300
        assert configured.lab_scheduler_max_artifact_commits_per_tick == 9

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("lab_jobs_busy_timeout_ms", 0),
            ("lab_scheduler_poll_interval_ms", 0),
            ("lab_scheduler_lease_seconds", 0),
            ("lab_scheduler_heartbeat_seconds", 0),
            ("lab_scheduler_shard_lease_seconds", 0),
            ("lab_scheduler_max_commands_per_tick", 0),
            ("lab_scheduler_max_commands_per_tick", 257),
            ("lab_scheduler_max_reports_per_tick", 0),
            ("lab_scheduler_max_reports_per_tick", 257),
            ("lab_scheduler_max_plans_per_tick", 0),
            ("lab_scheduler_max_plans_per_tick", 257),
            ("lab_scheduler_max_claims_per_tick", 0),
            ("lab_scheduler_max_claims_per_tick", 129),
            ("lab_scheduler_max_claim_authority_per_tick", 0),
            ("lab_scheduler_max_claim_authority_per_tick", 513),
            ("lab_worker_poll_interval_ms", 0),
            ("lab_worker_heartbeat_seconds", 0),
            ("lab_worker_lease_extension_seconds", 0),
            ("lab_worker_receipt_timeout_seconds", 0),
            ("lab_worker_max_shards_per_tick", 0),
            ("lab_worker_max_shards_per_tick", 2),
            ("lab_finalizer_poll_interval_ms", 0),
            ("lab_finalizer_max_jobs_per_tick", 0),
            ("lab_finalizer_max_jobs_per_tick", 129),
            ("lab_finalizer_failure_cooldown_seconds", 0),
            ("lab_finalizer_failure_cooldown_max_seconds", 86_401),
            ("lab_scheduler_max_artifact_commits_per_tick", 0),
            ("lab_scheduler_max_artifact_commits_per_tick", 257),
        ],
    )
    def test_lab_scheduler_rejects_non_positive_runtime_settings(
        self,
        tmp_path: Path,
        field: str,
        value: int,
    ) -> None:
        with pytest.raises(ValidationError):
            Settings(**_settings_values(tmp_path), **{field: value})

    def test_lab_finalizer_cooldown_maximum_must_cover_base(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError, match="cooldown maximum"):
            Settings(
                **_settings_values(tmp_path),
                lab_finalizer_failure_cooldown_seconds=60,
                lab_finalizer_failure_cooldown_max_seconds=30,
            )

    def test_lab_scheduler_lease_must_cover_three_heartbeats(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(ValidationError, match="3.*heartbeat"):
            Settings(
                **_settings_values(tmp_path),
                lab_scheduler_lease_seconds=29,
                lab_scheduler_heartbeat_seconds=10,
            )

    def test_lab_worker_heartbeat_must_precede_scheduler_shard_lease(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(ValidationError, match="worker heartbeat must precede"):
            Settings(
                **_settings_values(tmp_path),
                lab_scheduler_shard_lease_seconds=30,
                lab_worker_heartbeat_seconds=30,
            )

    def test_lab_worker_heartbeat_must_precede_lease_extension(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(ValidationError, match="worker heartbeat must precede lease extension"):
            Settings(
                **_settings_values(tmp_path),
                lab_scheduler_shard_lease_seconds=31,
                lab_worker_heartbeat_seconds=30,
                lab_worker_lease_extension_seconds=29,
                lab_worker_receipt_timeout_seconds=1,
            )

    @pytest.mark.parametrize(
        "existing_path",
        [
            "duckdb_path",
            "duckdb_readonly_path",
            "backfill_state_path",
            "research_db_path",
            "research_readonly_db_path",
            "notification_state_path",
        ],
    )
    def test_lab_jobs_database_must_not_alias_existing_database_paths(
        self,
        tmp_path: Path,
        existing_path: str,
    ) -> None:
        values = _settings_values(tmp_path)
        default_paths = {
            "duckdb_path": tmp_path / "data" / "rquant.duckdb",
            "duckdb_readonly_path": tmp_path / "data" / "rquant_ro.duckdb",
            "backfill_state_path": tmp_path / "data" / "backfill.sqlite3",
            "research_db_path": tmp_path / "data" / "research.duckdb",
            "research_readonly_db_path": tmp_path / "data" / "research_ro.duckdb",
            "notification_state_path": tmp_path / "data" / "notification.sqlite3",
        }
        values.update(default_paths)
        alias = default_paths[existing_path]

        with pytest.raises(ValidationError, match="lab jobs path must differ"):
            Settings(**values, lab_jobs_path=alias)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("backfill_planner_memory_limit_mb", 255),
            ("backfill_planner_threads", 0),
            ("backfill_planner_threads", 5),
        ],
    )
    def test_backfill_planner_rejects_unsafe_resource_limits(
        self,
        tmp_path: Path,
        field: str,
        value: int,
    ) -> None:
        with pytest.raises(ValidationError):
            Settings(
                **_settings_values(tmp_path),
                **{field: value},
            )

    def test_backfill_state_rejects_duckdb_path(self, tmp_path: Path) -> None:
        duckdb_path = tmp_path / "data" / "rquant.duckdb"

        with pytest.raises(ValidationError, match="backfill state path must differ"):
            Settings(
                **_settings_values(tmp_path),
                backfill_state_path=duckdb_path,
            )

    def test_backfill_state_rejects_readonly_duckdb_path(self, tmp_path: Path) -> None:
        readonly_path = tmp_path / "data" / "rquant_ro.duckdb"

        with pytest.raises(ValidationError, match="backfill state path must differ"):
            Settings(
                **_settings_values(tmp_path),
                duckdb_readonly_path=readonly_path,
                backfill_state_path=readonly_path,
            )

    def test_research_paths_default_under_data_dir(self, tmp_path: Path) -> None:
        configured = Settings(
            **_settings_values(tmp_path),
            research_db_path="",
            research_lake_dir="",
            research_readonly_db_path="",
            research_staging_dir="",
        )

        assert configured.research_db_path_resolved == tmp_path / "data" / "research.duckdb"
        assert configured.research_readonly_db_path_resolved == (
            tmp_path / "data" / "research_ro.duckdb"
        )
        assert configured.research_lake_dir_resolved == tmp_path / "data" / "lake"
        assert configured.research_staging_dir_resolved == (tmp_path / "data" / "research_staging")
        assert configured.research_lake_dir_resolved.is_dir()
        assert configured.research_staging_dir_resolved.is_dir()
        assert configured.research_cloud_ingest_enabled is False

    @pytest.mark.parametrize(
        "field",
        [
            "research_db_path",
            "research_readonly_db_path",
            "research_lake_dir",
            "research_staging_dir",
        ],
    )
    def test_research_paths_must_not_alias_operational_duckdb(
        self,
        tmp_path: Path,
        field: str,
    ) -> None:
        duckdb_path = tmp_path / "data" / "rquant.duckdb"

        with pytest.raises(ValidationError, match="research paths must differ"):
            Settings(
                **_settings_values(tmp_path),
                **{field: duckdb_path},
            )

    def test_research_readonly_catalog_must_not_alias_writable_catalog(
        self, tmp_path: Path
    ) -> None:
        catalog = tmp_path / "data" / "research.duckdb"

        with pytest.raises(ValidationError, match="research paths must differ"):
            Settings(
                **_settings_values(tmp_path),
                research_db_path=catalog,
                research_readonly_db_path=catalog,
            )

    def test_readonly_duckdb_must_not_alias_main_by_symlink(self, tmp_path: Path) -> None:
        main = tmp_path / "data" / "rquant.duckdb"
        main.parent.mkdir(parents=True)
        main.touch()
        replica = tmp_path / "data" / "rquant_ro.duckdb"
        replica.symlink_to(main)

        with pytest.raises(ValidationError, match="readonly DuckDB path must differ"):
            Settings(
                **_settings_values(tmp_path),
                duckdb_readonly_path=replica,
            )
