"""Config 层单测：确保 .env 能正确加载、字段校验生效。"""

from rquant.config import settings


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
