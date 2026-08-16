"""全局配置：从 .env 读取，通过 Pydantic Settings 校验。

使用：
    from rquant.config import settings
    settings.tushare_token_main
"""

import os
import unicodedata
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _canonical_absolute_path(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ValueError(f"{label} must be an absolute canonical path")
    if candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"{label} must be canonical (no '.' or '..')")
    if candidate.resolve(strict=False) != candidate:
        raise ValueError(f"{label} must be canonical (no symlink aliases)")
    return candidate


def _paths_alias_or_nest(left: Path, right: Path) -> bool:
    left_key = tuple(unicodedata.normalize("NFC", part).casefold() for part in left.parts)
    right_key = tuple(unicodedata.normalize("NFC", part).casefold() for part in right.parts)
    return (
        left_key == right_key
        or left_key[: len(right_key)] == right_key
        or right_key[: len(left_key)] == left_key
    )


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
    backfill_planner_memory_limit_mb: int = Field(default=2_048, ge=256)
    backfill_planner_threads: int = Field(default=2, ge=1, le=4)
    lab_runtime_dir: Path | None = None
    lab_jobs_path: Path | None = None
    lab_job_command_dir: Path | None = None
    lab_job_claim_dir: Path | None = None
    lab_job_report_dir: Path | None = None
    lab_worker_artifact_dir: Path | None = None
    lab_final_artifact_dir: Path | None = None
    lab_artifact_commit_dir: Path | None = None
    lab_daemon_lock_dir: Path | None = None
    lab_finalizer_state_dir: Path | None = None
    lab_readiness_dir: Path | None = None
    lab_trusted_git_path: Path = Path("/usr/bin/git")
    lab_finalizer_authority_key_id: str = ""
    lab_finalizer_authority_key_path: Path | None = None
    lab_finalizer_authority_keyring_path: Path | None = None
    # V2 workers receive one canonical public-only verification bundle.  It
    # contains no finalizer or offline-root private signing material.
    lab_v2_claim_publication_enabled: bool = False
    lab_claim_publication_worker_verifier_path: Path | None = None
    # Dedicated claim-finalizer process only.  The canonical material document
    # references private runtime signing keys and must never be logged.
    lab_claim_finalizer_enabled: bool = False
    # Production daemons select only ``current`` below this controlled root.
    # The legacy single-file field remains available for isolated development fixtures.
    lab_claim_finalizer_runtime_material_root: Path | None = None
    lab_claim_finalizer_runtime_trusted_base: Path = Path("/etc/rquant")
    lab_claim_finalizer_runtime_material_path: Path | None = None
    lab_claim_finalizer_owner_id: str = "rquant-claim-finalizer"
    lab_claim_finalizer_lease_seconds: int = Field(default=60, ge=3, le=3_600)
    lab_claim_finalizer_poll_interval_ms: int = Field(default=500, ge=1, le=60_000)
    lab_claim_finalizer_max_publications_per_tick: int = Field(default=16, ge=1, le=100)
    lab_claim_finalizer_failure_backoff_seconds: int = Field(default=1, ge=1, le=3_600)
    lab_claim_finalizer_failure_backoff_max_seconds: int = Field(default=30, ge=1, le=3_600)
    # The Lab process only receives verification keys through the service
    # credential directory.  The compare-and-advance authority and its state
    # remain outside the Lab runtime root.
    lab_highwater_authority_command_json: str = ""
    lab_highwater_stable_identity: str = ""
    lab_highwater_trusted_keyring_path: Path | None = None
    lab_highwater_timeout_seconds: float = Field(default=10.0, ge=0.1, le=300)
    lab_highwater_allow_identity_rotation: bool = False
    lab_jobs_busy_timeout_ms: int = Field(default=5_000, ge=1)
    lab_scheduler_poll_interval_ms: int = Field(default=250, ge=1)
    lab_scheduler_lease_seconds: int = Field(default=60, ge=1)
    lab_scheduler_heartbeat_seconds: int = Field(default=10, ge=1)
    lab_scheduler_shard_lease_seconds: int = Field(default=300, ge=1)
    lab_scheduler_max_commands_per_tick: int = Field(default=64, ge=1, le=256)
    lab_scheduler_max_reports_per_tick: int = Field(default=64, ge=1, le=256)
    lab_scheduler_max_plans_per_tick: int = Field(default=64, ge=1, le=256)
    lab_scheduler_max_claims_per_tick: int = Field(default=16, ge=1, le=128)
    lab_scheduler_max_claim_authority_per_tick: int = Field(default=128, ge=1, le=512)
    lab_scheduler_max_artifact_commits_per_tick: int = Field(default=64, ge=1, le=256)
    lab_scheduler_full_integrity_interval_seconds: int = Field(default=3_600, ge=60)
    lab_scheduler_full_integrity_budget_seconds: float = Field(default=30.0, ge=0.01, le=300)
    lab_scheduler_worker_ids: str = ""
    lab_worker_id: str = "rquant-mac-primary"
    lab_worker_poll_interval_ms: int = Field(default=250, ge=1)
    lab_worker_heartbeat_seconds: int = Field(default=30, ge=1)
    lab_worker_lease_extension_seconds: int = Field(default=120, ge=1, le=3_600)
    lab_worker_receipt_timeout_seconds: int = Field(default=30, ge=1)
    lab_worker_max_shards_per_tick: int = Field(default=1, ge=1, le=1)
    rquant_lab_resource_policy_version: str = ""
    rquant_lab_resource_authority_config_json: str = ""
    rquant_external_monotonic_root_service_config_path: Path | None = None
    rquant_resource_authority_service_config_path: Path | None = None
    rquant_lab_live_slo_authority_root: Path | None = None
    rquant_lab_trade_calendar_path: Path | None = None
    lab_finalizer_poll_interval_ms: int = Field(default=1_000, ge=1)
    lab_finalizer_max_jobs_per_tick: int = Field(default=8, ge=1, le=128)
    lab_finalizer_failure_cooldown_seconds: int = Field(default=30, ge=1, le=3_600)
    lab_finalizer_failure_cooldown_max_seconds: int = Field(default=3_600, ge=1, le=86_400)
    parquet_dir: Path
    research_db_path: Path | None = None
    research_readonly_db_path: Path | None = None
    research_lake_dir: Path | None = None
    research_staging_dir: Path | None = None
    research_cloud_ingest_enabled: bool = False

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_dir: Path

    app_env: Literal["dev", "prod"] = "dev"

    pushdeer_keys: str = Field(default="")
    pushdeer_recipient_ids: str = Field(default="admin")
    pushdeer_endpoint: str = Field(default="https://api2.pushdeer.com/message/push")
    pushplus_tokens: str = Field(default="")
    pushplus_recipient_ids: str = Field(default="admin")
    pushplus_endpoint: str = Field(default="https://www.pushplus.plus/send")
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
    notify_pulse_alert: bool = True

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
    panorama_gate_token: str = Field(default="", validation_alias="RQUANT_PANORAMA_GATE_TOKEN")

    @property
    def deepseek_enabled(self) -> bool:
        return bool(self.deepseek_api_key)

    @property
    def pushdeer_key_list(self) -> list[str]:
        return [k.strip() for k in self.pushdeer_keys.split(",") if k.strip()]

    @property
    def pushplus_token_list(self) -> list[str]:
        return [t.strip() for t in self.pushplus_tokens.split(",") if t.strip()]

    @staticmethod
    def _recipient_ids_for_credentials(
        raw_recipient_ids: str,
        credentials: list[str],
        *,
        default_recipient_id: str,
    ) -> list[str]:
        if not credentials:
            return []
        recipient_ids = [item.strip() for item in raw_recipient_ids.split(",") if item.strip()] or [
            default_recipient_id
        ]
        if len(recipient_ids) == 1:
            return recipient_ids * len(credentials)
        if len(recipient_ids) != len(credentials):
            raise ValueError(
                "notification recipient ids must contain one shared id or one id per credential"
            )
        return recipient_ids

    @property
    def pushdeer_recipient_id_list(self) -> list[str]:
        return self._recipient_ids_for_credentials(
            self.pushdeer_recipient_ids,
            self.pushdeer_key_list,
            default_recipient_id="admin",
        )

    @property
    def pushplus_recipient_id_list(self) -> list[str]:
        return self._recipient_ids_for_credentials(
            self.pushplus_recipient_ids,
            self.pushplus_token_list,
            default_recipient_id="admin",
        )

    @field_validator("pushdeer_endpoint", "pushplus_endpoint", mode="before")
    @classmethod
    def require_https_notification_endpoint(cls, v: object) -> object:
        if not isinstance(v, str):
            return v
        normalized = v.strip()
        if normalized == "http://www.pushplus.plus/send":
            normalized = "https://www.pushplus.plus/send"
        parsed = urlsplit(normalized)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            raise ValueError("notification endpoint must use HTTPS")
        return normalized

    @model_validator(mode="after")
    def validate_notification_recipient_mappings(self) -> "Settings":
        _ = self.pushdeer_recipient_id_list
        _ = self.pushplus_recipient_id_list
        return self

    @field_validator("intraday_quote_source", mode="after")
    @classmethod
    def validate_intraday_quote_source(cls, v: str) -> str:
        # env 里 INTRADAY_QUOTE_SOURCE= 留空读到 ""（不走字段默认），回落 tushare
        normalized = v.strip().lower()
        if not normalized:
            return "tushare"
        if normalized not in ("tushare", "akshare"):
            raise ValueError(f"intraday_quote_source 只允许 'tushare' 或 'akshare'，收到 {v!r}")
        return normalized

    @field_validator("data_dir", "parquet_dir", "log_dir", mode="before")
    @classmethod
    def require_canonical_base_dir(cls, v: object, info: ValidationInfo) -> object:
        if isinstance(v, (str, Path)):
            _canonical_absolute_path(Path(v), label=info.field_name.upper())
        return v

    @field_validator("data_dir", "parquet_dir", "log_dir", mode="after")
    @classmethod
    def retain_pure_base_dir(cls, v: Path) -> Path:
        return v

    @field_validator("duckdb_path", mode="after")
    @classmethod
    def validate_canonical_duckdb_path(cls, v: Path) -> Path:
        _canonical_absolute_path(v, label="DUCKDB_PATH")
        return v

    @field_validator("backfill_state_path", mode="before")
    @classmethod
    def normalize_empty_backfill_state_path(cls, v: object) -> object:
        return None if isinstance(v, str) and not v.strip() else v

    @field_validator(
        "lab_jobs_path",
        "lab_runtime_dir",
        "lab_job_command_dir",
        "lab_job_claim_dir",
        "lab_job_report_dir",
        "lab_worker_artifact_dir",
        "lab_final_artifact_dir",
        "lab_artifact_commit_dir",
        "lab_daemon_lock_dir",
        "lab_finalizer_state_dir",
        "lab_readiness_dir",
        "lab_finalizer_authority_key_path",
        "lab_finalizer_authority_keyring_path",
        "lab_claim_publication_worker_verifier_path",
        "lab_claim_finalizer_runtime_material_root",
        "lab_claim_finalizer_runtime_material_path",
        "lab_highwater_trusted_keyring_path",
        "lab_trusted_git_path",
        "rquant_lab_live_slo_authority_root",
        "rquant_lab_trade_calendar_path",
        "rquant_external_monotonic_root_service_config_path",
        "rquant_resource_authority_service_config_path",
        mode="before",
    )
    @classmethod
    def normalize_empty_lab_job_path(cls, v: object) -> object:
        return None if isinstance(v, str) and not v.strip() else v

    @field_validator(
        "lab_jobs_path",
        "lab_runtime_dir",
        "lab_job_command_dir",
        "lab_job_claim_dir",
        "lab_job_report_dir",
        "lab_worker_artifact_dir",
        "lab_final_artifact_dir",
        "lab_artifact_commit_dir",
        "lab_daemon_lock_dir",
        "lab_finalizer_state_dir",
        "lab_readiness_dir",
        "lab_finalizer_authority_key_path",
        "lab_finalizer_authority_keyring_path",
        "lab_claim_publication_worker_verifier_path",
        "lab_claim_finalizer_runtime_material_root",
        "lab_claim_finalizer_runtime_material_path",
        "lab_highwater_trusted_keyring_path",
        "lab_trusted_git_path",
        "rquant_lab_live_slo_authority_root",
        "rquant_lab_trade_calendar_path",
        "rquant_external_monotonic_root_service_config_path",
        "rquant_resource_authority_service_config_path",
        mode="after",
    )
    @classmethod
    def require_absolute_lab_job_path(cls, v: Path | None) -> Path | None:
        if v is not None:
            _canonical_absolute_path(v, label="lab managed path")
        return v

    @field_validator(
        "duckdb_readonly_path",
        "research_db_path",
        "research_readonly_db_path",
        "research_lake_dir",
        "research_staging_dir",
        "notification_state_path",
        "panorama_users_path",
        mode="before",
    )
    @classmethod
    def normalize_empty_research_path(cls, v: object) -> object:
        return None if isinstance(v, str) and not v.strip() else v

    @field_validator(
        "duckdb_readonly_path",
        "research_db_path",
        "research_readonly_db_path",
        "research_lake_dir",
        "research_staging_dir",
        "notification_state_path",
        "panorama_users_path",
        mode="after",
    )
    @classmethod
    def require_canonical_optional_path(
        cls,
        v: Path | None,
        info: ValidationInfo,
    ) -> Path | None:
        if v is not None:
            label = (
                "readonly DuckDB path must differ from main DuckDB path and configured storage path"
                if info.field_name == "duckdb_readonly_path"
                else "configured storage path"
            )
            _canonical_absolute_path(v, label=label)
        return v

    @field_validator("backfill_state_path", mode="after")
    @classmethod
    def ensure_backfill_state_parent_exists(cls, v: Path | None) -> Path | None:
        if v is not None:
            _canonical_absolute_path(v, label="BACKFILL_STATE_PATH")
        return v

    @model_validator(mode="after")
    def validate_backfill_state_is_separate(self) -> "Settings":
        state_path = self.backfill_state_path_resolved.resolve()
        main_path = self.duckdb_path.resolve()
        readonly_path = self.duckdb_readonly_path_resolved.resolve()
        if main_path == readonly_path:
            raise ValueError("readonly DuckDB path must differ from main DuckDB path")
        operational_paths = {
            main_path,
            readonly_path,
        }
        if state_path in operational_paths:
            raise ValueError("backfill state path must differ from DuckDB main and readonly paths")
        research_paths = {
            (self.research_db_path or self.data_dir / "research.duckdb").resolve(),
            (self.research_readonly_db_path or self.data_dir / "research_ro.duckdb").resolve(),
            (self.research_lake_dir or self.data_dir / "lake").resolve(),
            (self.research_staging_dir or self.data_dir / "research_staging").resolve(),
        }
        if research_paths & operational_paths:
            raise ValueError("research paths must differ from DuckDB main and readonly paths")
        if len(research_paths) != 4:
            raise ValueError("research paths must differ from each other")
        return self

    @model_validator(mode="after")
    def validate_lab_scheduler_storage(self) -> "Settings":
        if self.lab_scheduler_lease_seconds < 3 * self.lab_scheduler_heartbeat_seconds:
            raise ValueError("lab scheduler lease must be >= 3 * heartbeat interval")
        if self.lab_worker_heartbeat_seconds >= self.lab_scheduler_shard_lease_seconds:
            raise ValueError("lab worker heartbeat must precede scheduler shard lease expiry")
        if self.lab_worker_heartbeat_seconds >= self.lab_worker_lease_extension_seconds:
            raise ValueError("lab worker heartbeat must precede lease extension")
        if (
            self.lab_finalizer_failure_cooldown_max_seconds
            < self.lab_finalizer_failure_cooldown_seconds
        ):
            raise ValueError("lab finalizer cooldown maximum must not be below its base")
        if (
            self.lab_claim_finalizer_failure_backoff_max_seconds
            < self.lab_claim_finalizer_failure_backoff_seconds
        ):
            raise ValueError("claim finalizer backoff maximum must not be below its base")
        if not self.lab_claim_finalizer_owner_id.strip():
            raise ValueError("claim finalizer owner id must not be empty")
        if (
            self.lab_claim_finalizer_enabled or self.lab_v2_claim_publication_enabled
        ) and self.lab_claim_finalizer_runtime_material_root is None:
            raise ValueError(
                "enabled V2 claim finalizer/worker requires "
                "LAB_CLAIM_FINALIZER_RUNTIME_MATERIAL_ROOT"
            )
        if (
            self.lab_claim_finalizer_enabled or self.lab_v2_claim_publication_enabled
        ) and self.lab_claim_finalizer_runtime_material_path is not None:
            raise ValueError(
                "legacy LAB_CLAIM_FINALIZER_RUNTIME_MATERIAL_PATH is forbidden when V2 is enabled"
            )
        lab_runtime_root = _canonical_absolute_path(
            self.lab_runtime_dir or self.data_dir / "lab-runtime",
            label="lab runtime root",
        )
        lab_path = _canonical_absolute_path(
            self.lab_jobs_path or lab_runtime_root / "lab_jobs.sqlite3",
            label="lab jobs path",
        )
        existing_database_paths = (
            _canonical_absolute_path(self.duckdb_path, label="DuckDB path"),
            _canonical_absolute_path(
                self.duckdb_readonly_path
                or self.duckdb_path.with_name(self.duckdb_path.stem + "_ro.duckdb"),
                label="readonly DuckDB path",
            ),
            _canonical_absolute_path(
                self.backfill_state_path or self.data_dir / "backfill_state.sqlite3",
                label="backfill state path",
            ),
            _canonical_absolute_path(
                self.research_db_path or self.data_dir / "research.duckdb",
                label="research database path",
            ),
            _canonical_absolute_path(
                self.research_readonly_db_path or self.data_dir / "research_ro.duckdb",
                label="readonly research database path",
            ),
            _canonical_absolute_path(
                self.notification_state_path or self.data_dir / "notification_state.sqlite3",
                label="notification state path",
            ),
            _canonical_absolute_path(
                self.panorama_users_path or self.data_dir / "panorama-users.txt",
                label="panorama users path",
            ),
        )
        if lab_path in existing_database_paths:
            raise ValueError("lab jobs path must differ from all existing database paths")
        managed_dirs = (
            _canonical_absolute_path(
                self.lab_job_command_dir or lab_runtime_root / "commands",
                label="lab command spool",
            ),
            _canonical_absolute_path(
                self.lab_job_claim_dir or lab_runtime_root / "claims",
                label="lab claim spool",
            ),
            _canonical_absolute_path(
                self.lab_job_report_dir or lab_runtime_root / "reports",
                label="lab report spool",
            ),
            _canonical_absolute_path(
                self.lab_worker_artifact_dir or lab_runtime_root / "worker-artifacts",
                label="lab worker artifact root",
            ),
            _canonical_absolute_path(
                self.lab_final_artifact_dir or lab_runtime_root / "final-artifacts",
                label="lab final artifact root",
            ),
            _canonical_absolute_path(
                self.lab_artifact_commit_dir or lab_runtime_root / "artifact-commits",
                label="lab artifact commit spool",
            ),
            _canonical_absolute_path(
                self.lab_daemon_lock_dir or lab_runtime_root / "locks",
                label="lab daemon lock root",
            ),
            _canonical_absolute_path(
                self.lab_finalizer_state_dir or lab_runtime_root / "finalizer-state",
                label="lab finalizer state root",
            ),
            _canonical_absolute_path(
                self.lab_readiness_dir or lab_runtime_root / "readiness",
                label="lab readiness root",
            ),
        )
        existing_managed_dirs = (
            _canonical_absolute_path(self.parquet_dir, label="Parquet root"),
            _canonical_absolute_path(self.log_dir, label="log root"),
            _canonical_absolute_path(
                self.research_lake_dir or self.data_dir / "lake",
                label="research lake path",
            ),
            _canonical_absolute_path(
                self.research_staging_dir or self.data_dir / "research_staging",
                label="research staging path",
            ),
        )
        key_paths = tuple(
            _canonical_absolute_path(path, label="lab authority key path")
            for path in (
                self.lab_finalizer_authority_key_path,
                self.lab_finalizer_authority_keyring_path,
                self.lab_claim_publication_worker_verifier_path,
                self.lab_claim_finalizer_runtime_material_root,
                self.lab_claim_finalizer_runtime_material_path,
                self.lab_highwater_trusted_keyring_path,
            )
            if path is not None
        )
        isolated_paths = (
            existing_database_paths + existing_managed_dirs + (lab_path,) + managed_dirs + key_paths
        )
        for index, left in enumerate(isolated_paths):
            for right in isolated_paths[index + 1 :]:
                if _paths_alias_or_nest(left, right):
                    raise ValueError(
                        "lab database/managed/key paths must not alias or nest "
                        f"(nested paths): {left} <> {right}"
                    )
        for existing in existing_database_paths + existing_managed_dirs + key_paths:
            if _paths_alias_or_nest(lab_runtime_root, existing):
                raise ValueError(
                    "lab runtime root must not alias or nest another storage path "
                    f"(nested paths): {lab_runtime_root} <> {existing}"
                )
        if lab_path.parent != lab_runtime_root or any(
            path.parent != lab_runtime_root for path in managed_dirs
        ):
            raise ValueError(
                "lab database and managed directories must be direct children of lab runtime root"
            )
        workers = self.lab_scheduler_worker_id_list
        if len(set(workers)) != len(workers):
            raise ValueError("lab scheduler worker ids must be unique")
        if not self.lab_worker_id.strip():
            raise ValueError("lab worker id must not be empty")
        if workers and self.lab_worker_id not in workers:
            raise ValueError("lab worker id must be present in scheduler worker ids")
        return self

    @model_validator(mode="after")
    def materialize_base_directories(self) -> "Settings":
        data_dir_existed = self.data_dir.exists()
        self.data_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        if not data_dir_existed:
            self.data_dir.chmod(0o700)
        for path in (self.parquet_dir, self.log_dir):
            path.mkdir(parents=True, exist_ok=True)
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
    def lab_jobs_path_resolved(self) -> Path:
        return self.lab_jobs_path or self.lab_runtime_dir_resolved / "lab_jobs.sqlite3"

    @property
    def lab_runtime_dir_resolved(self) -> Path:
        return self.lab_runtime_dir or self.data_dir / "lab-runtime"

    @property
    def lab_job_command_dir_resolved(self) -> Path:
        return self.lab_job_command_dir or self.lab_runtime_dir_resolved / "commands"

    @property
    def lab_job_claim_dir_resolved(self) -> Path:
        return self.lab_job_claim_dir or self.lab_runtime_dir_resolved / "claims"

    @property
    def lab_job_report_dir_resolved(self) -> Path:
        return self.lab_job_report_dir or self.lab_runtime_dir_resolved / "reports"

    @property
    def lab_worker_artifact_dir_resolved(self) -> Path:
        return self.lab_worker_artifact_dir or self.lab_runtime_dir_resolved / "worker-artifacts"

    @property
    def lab_final_artifact_dir_resolved(self) -> Path:
        return self.lab_final_artifact_dir or self.lab_runtime_dir_resolved / "final-artifacts"

    @property
    def lab_artifact_commit_dir_resolved(self) -> Path:
        return self.lab_artifact_commit_dir or self.lab_runtime_dir_resolved / "artifact-commits"

    @property
    def lab_daemon_lock_dir_resolved(self) -> Path:
        return self.lab_daemon_lock_dir or self.lab_runtime_dir_resolved / "locks"

    @property
    def lab_finalizer_state_dir_resolved(self) -> Path:
        return self.lab_finalizer_state_dir or self.lab_runtime_dir_resolved / "finalizer-state"

    @property
    def lab_readiness_dir_resolved(self) -> Path:
        return self.lab_readiness_dir or self.lab_runtime_dir_resolved / "readiness"

    @property
    def lab_scheduler_worker_id_list(self) -> tuple[str, ...]:
        return tuple(
            worker.strip() for worker in self.lab_scheduler_worker_ids.split(",") if worker.strip()
        )

    @property
    def research_db_path_resolved(self) -> Path:
        """Research metadata catalog; never aliases the operational DuckDB."""
        path = self.research_db_path or self.data_dir / "research.duckdb"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def research_readonly_db_path_resolved(self) -> Path:
        """Read-only research catalog atomically refreshed after daily sealing."""
        path = self.research_readonly_db_path or self.data_dir / "research_ro.duckdb"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def research_lake_dir_resolved(self) -> Path:
        """Partitioned Parquet root for immutable-at-publish research data."""
        path = self.research_lake_dir or self.data_dir / "lake"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def research_staging_dir_resolved(self) -> Path:
        """Daily monitor evidence used to validate the expected minute universe."""
        path = self.research_staging_dir or self.data_dir / "research_staging"
        path.mkdir(parents=True, exist_ok=True)
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


def _default_settings_env_file() -> str | None:
    disabled = os.getenv("RQUANT_DISABLE_DOTENV", "").strip().lower()
    return None if disabled in {"1", "true", "yes", "on"} else ".env"


settings = Settings(_env_file=_default_settings_env_file())  # type: ignore[call-arg]
