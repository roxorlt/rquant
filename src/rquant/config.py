"""全局配置：从 .env 读取，通过 Pydantic Settings 校验。

使用：
    from rquant.config import settings
    settings.tushare_token_main
"""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
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
    duckdb_readonly_path: Path | None = None
    backfill_state_path: Path | None = None
    backfill_state_busy_timeout_ms: int = Field(default=5_000, ge=1)
    parquet_dir: Path

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_dir: Path

    app_env: Literal["dev", "prod"] = "dev"

    pushdeer_keys: str = Field(default="")
    pushdeer_endpoint: str = Field(default="https://api2.pushdeer.com/message/push")
    pushplus_tokens: str = Field(default="")
    pushplus_endpoint: str = Field(default="http://www.pushplus.plus/send")
    notify_enabled: bool = True
    notification_state_path: Path | None = None
    notification_state_busy_timeout_ms: int = Field(default=5_000, ge=1)
    notify_error_cooldown_seconds: int = Field(default=1_800, ge=0)
    notify_ops_cooldown_seconds: int = Field(default=1_800, ge=0)
    notify_price_level: bool = True
    notify_pool2_exit: bool = True
    notify_daily_summary: bool = True
    notify_error: bool = True
    notify_heartbeat: bool = True
    notify_morning_pulse: bool = True
    notify_midday_report: bool = True
    notify_surge_watch: bool = True

    pool2_max_age_days: int = 6

    # 盘中分钟行情主源。tushare = 付费 rt_min 主源 + akshare 兜底；
    # akshare = 紧急回退开关（rt_min 权限到期 / 故障 / 止损时不改代码切回纯 akshare）
    intraday_quote_source: str = "tushare"

    # 盘中 rt_min 轮询节流（秒）。monitor 主循环 interval=5s，但 rt_min 是分钟级
    # 数据，5s 打一次 API 纯烧配额；距上次成功拉取不足该间隔时用内存缓存合成行情
    rt_min_poll_seconds: int = 15

    # ===== LLM (Week 7) =====
    deepseek_api_key: str = Field(default="")
    deepseek_base_url: str = Field(default="https://api.deepseek.com")
    deepseek_model: str = Field(default="deepseek-v4-flash")

    # ===== 全景页登录网关（微信友好 cookie 登录，替代 basic auth）=====
    # 签名令牌密钥：部署时 `openssl rand -hex 32` 生成一次写 .env；为空则登录服务拒绝启动。
    # 用 validation_alias 对齐既有 RQUANT_* env 命名（poller/backup 同风格），
    # systemd EnvironmentFile=.env 注入进程环境后由 Settings 读取。
    panorama_cookie_secret: str = Field(
        default="", validation_alias="RQUANT_PANORAMA_COOKIE_SECRET"
    )
    panorama_users_path: Path | None = Field(
        default=None, validation_alias="RQUANT_PANORAMA_USERS_PATH"
    )
    # map 网关令牌：云端 nginx 无 http_auth_request_module 时改用 map 静态比对 cookie，
    # nginx 只认这一个字面值（不验 hmac 签名），所有已登录用户共用同一 cookie 值。
    # 显式配置优先；为空时从 cookie_secret 确定性派生（见 panorama_gate_token_resolved），
    # 免得再单独配一份、且重启后稳定不变。
    panorama_gate_token: str = Field(
        default="", validation_alias="RQUANT_PANORAMA_GATE_TOKEN"
    )

    @property
    def deepseek_enabled(self) -> bool:
        return bool(self.deepseek_api_key)

    @property
    def pushdeer_key_list(self) -> list[str]:
        return [k.strip() for k in self.pushdeer_keys.split(",") if k.strip()]

    @property
    def pushplus_token_list(self) -> list[str]:
        return [t.strip() for t in self.pushplus_tokens.split(",") if t.strip()]

    @field_validator("intraday_quote_source", mode="after")
    @classmethod
    def validate_intraday_quote_source(cls, v: str) -> str:
        # env 里 INTRADAY_QUOTE_SOURCE= 留空读到 ""（不走字段默认），回落 tushare
        normalized = v.strip().lower()
        if not normalized:
            return "tushare"
        if normalized not in ("tushare", "akshare"):
            raise ValueError(
                f"intraday_quote_source 只允许 'tushare' 或 'akshare'，收到 {v!r}"
            )
        return normalized

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

    @field_validator("backfill_state_path", mode="before")
    @classmethod
    def normalize_empty_backfill_state_path(cls, v: object) -> object:
        return None if isinstance(v, str) and not v.strip() else v

    @field_validator("backfill_state_path", mode="after")
    @classmethod
    def ensure_backfill_state_parent_exists(cls, v: Path | None) -> Path | None:
        if v is not None:
            v.parent.mkdir(parents=True, exist_ok=True)
        return v

    @model_validator(mode="after")
    def validate_backfill_state_is_separate(self) -> "Settings":
        state_path = self.backfill_state_path_resolved.resolve()
        if state_path in {
            self.duckdb_path.resolve(),
            self.duckdb_readonly_path_resolved.resolve(),
        }:
            raise ValueError(
                "backfill state path must differ from DuckDB main and readonly paths"
            )
        return self

    @property
    def duckdb_readonly_path_resolved(self) -> Path:
        """副本路径未显式配置时，从主库路径派生（同目录、_ro 后缀）。"""
        if self.duckdb_readonly_path is not None:
            return self.duckdb_readonly_path
        return self.duckdb_path.with_name(self.duckdb_path.stem + "_ro.duckdb")

    @property
    def backfill_state_path_resolved(self) -> Path:
        """回补状态库独立于 DuckDB；未配置时放在 data_dir。"""
        path = self.backfill_state_path or self.data_dir / "backfill_state.sqlite3"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def notification_state_path_resolved(self) -> Path:
        path = self.notification_state_path or self.data_dir / "notification_state.sqlite3"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def panorama_users_path_resolved(self) -> Path:
        """用户库路径未显式配置时，落 data_dir 下 panorama-users.txt。"""
        if self.panorama_users_path is not None:
            return self.panorama_users_path
        return self.data_dir / "panorama-users.txt"

    @property
    def panorama_gate_token_resolved(self) -> str:
        """map 网关令牌：显式 RQUANT_PANORAMA_GATE_TOKEN 优先，否则从 cookie_secret 派生。

        两者都为空则返回空串——由登录服务启动时 raise SystemExit 拦截，绝不静默降级。
        """
        # 延迟导入，避免 config 模块加载时把 http.server 一并拉进来。
        from rquant.panorama_auth import derive_gate_token

        if self.panorama_gate_token:
            return self.panorama_gate_token
        return derive_gate_token(self.panorama_cookie_secret)


settings = Settings()  # type: ignore[call-arg]
