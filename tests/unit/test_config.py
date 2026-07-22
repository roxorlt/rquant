"""Config 层单测：确保 .env 能正确加载、字段校验生效。"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.config import Settings, settings


def _settings_values(tmp_path: Path) -> dict[str, object]:
    return {
        "_env_file": None,
        "tushare_token_main": "x" * 32,
        "data_dir": tmp_path / "data",
        "duckdb_path": tmp_path / "data" / "rquant.duckdb",
        "parquet_dir": tmp_path / "data" / "parquet",
        "log_dir": tmp_path / "logs",
    }


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
        assert configured.research_staging_dir_resolved == (
            tmp_path / "data" / "research_staging"
        )
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
