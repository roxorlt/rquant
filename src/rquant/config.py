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
    pushplus_tokens: str = Field(default="")
    pushplus_endpoint: str = Field(default="http://www.pushplus.plus/send")
    notify_enabled: bool = True
    notify_price_level: bool = True
    notify_pool2_exit: bool = True
    notify_daily_summary: bool = True
    notify_error: bool = True
    notify_heartbeat: bool = True

    pool2_max_age_days: int = 6

    # ===== LLM (Week 7) =====
    deepseek_api_key: str = Field(default="")
    deepseek_base_url: str = Field(default="https://api.deepseek.com")
    deepseek_model: str = Field(default="deepseek-v4-flash")

    @property
    def deepseek_enabled(self) -> bool:
        return bool(self.deepseek_api_key)

    @property
    def pushdeer_key_list(self) -> list[str]:
        return [k.strip() for k in self.pushdeer_keys.split(",") if k.strip()]

    @property
    def pushplus_token_list(self) -> list[str]:
        return [t.strip() for t in self.pushplus_tokens.split(",") if t.strip()]

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
