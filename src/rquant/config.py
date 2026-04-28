"""全局配置：从 .env 读取，通过 Pydantic Settings 校验。

使用：
    from rquant.config import settings
    settings.tushare_token_main
"""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    tushare_token_main: str = Field(..., min_length=32)
    tushare_token_backup: str | None = Field(default=None)

    data_dir: Path
    duckdb_path: Path
    parquet_dir: Path

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_dir: Path

    app_env: Literal["dev", "prod"] = "dev"

    pushdeer_keys: str = Field(default="")
    pushdeer_endpoint: str = Field(default="https://api2.pushdeer.com/message/push")
    notify_enabled: bool = True
    notify_price_level: bool = True
    notify_pool2_exit: bool = True
    notify_daily_summary: bool = True
    notify_error: bool = True
    notify_heartbeat: bool = True

    @property
    def pushdeer_key_list(self) -> list[str]:
        return [k.strip() for k in self.pushdeer_keys.split(",") if k.strip()]

    @field_validator("data_dir", "parquet_dir", "log_dir", mode="after")
    @classmethod
    def ensure_dir_exists(cls, v: Path) -> Path:
        v.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("duckdb_path", mode="after")
    @classmethod
    def ensure_duckdb_parent_exists(cls, v: Path) -> Path:
        v.parent.mkdir(parents=True, exist_ok=True)
        return v


settings = Settings()  # type: ignore[call-arg]
