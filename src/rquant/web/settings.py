"""Web API settings, read from the process environment only.

``rquant.config`` is deliberately not used: constructing it reads ``.env``, and the web
process must not see the secrets file at all (its systemd unit hides it). The default
listener is loopback TCP; authenticated identity requires a private Web UDS and a
separate read-only proxy-proof file.
"""

from __future__ import annotations

import ipaddress
import os
import re
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StrictInt,
    field_validator,
    model_validator,
)

from rquant.serving_paths import serving_root_from_env

BIND_ENV_VAR = "RQUANT_WEB_BIND"
PAGE_CONTROL_URL_ENV_VAR = "RQUANT_PAGE_CONTROL_URL"
STALE_AFTER_ENV_VAR = "RQUANT_WEB_STALE_AFTER_SECONDS"
SCREEN_PRIMARY_ENV_VAR = "RQUANT_WEB_SCREEN_PRIMARY_PATH"
SCREEN_REPLICA_ENV_VAR = "RQUANT_WEB_SCREEN_REPLICA_PATH"
SCREEN_HISTORY_ENV_VAR = "RQUANT_WEB_SCREEN_HISTORY_ROOT"
SCREEN_RSI_ENV_VAR = "RQUANT_WEB_SCREEN_RSI_ROOT"
FORMULA_MARKET_RESULT_ENV_VAR = "RQUANT_WEB_FORMULA_MARKET_RESULT_ROOT"
FORMULA_POOL_DAILY_RESULT_ENV_VAR = "RQUANT_WEB_FORMULA_POOL_DAILY_RESULT_ROOT"
CATALOG_SAMPLES_ENV_VAR = "RQUANT_WEB_CATALOG_SAMPLES_FILE"
NL_OPENAI_API_KEY_ENV_VAR = "RQUANT_WEB_NL_OPENAI_API_KEY"
NL_OPENAI_MODEL_ENV_VAR = "RQUANT_WEB_NL_OPENAI_MODEL"
ACK_ADMISSION_SOCKET_ENV_VAR = "RQUANT_WEB_ACK_ADMISSION_SOCKET"
WATCHLIST_ADMISSION_SOCKET_ENV_VAR = "RQUANT_WEB_WATCHLIST_ADMISSION_SOCKET"
FACTOR_ADMISSION_SOCKET_ENV_VAR = "RQUANT_WEB_FACTOR_ADMISSION_SOCKET"
FACTOR_ADMISSION_SERVICE_UID_ENV_VAR = "RQUANT_WEB_FACTOR_ADMISSION_SERVICE_UID"
FACTOR_ADMISSION_SHARED_GID_ENV_VAR = "RQUANT_WEB_FACTOR_ADMISSION_SHARED_GID"
FACTOR_EDITOR_USERS_ENV_VAR = "RQUANT_WEB_FACTOR_EDITOR_USERS"
INGRESS_SOCKET_ENV_VAR = "RQUANT_WEB_INGRESS_SOCKET"
PROXY_PROOF_FILE_ENV_VAR = "RQUANT_WEB_PROXY_PROOF_FILE"
LOG_ADMIN_USERS_ENV_VAR = "RQUANT_WEB_LOG_ADMIN_USERS"
LAB_CONTROL_USERS_ENV_VAR = "RQUANT_WEB_LAB_CONTROL_USERS"
UNIT_LOG_SOCKET_ENV_VAR = "RQUANT_WEB_UNIT_LOG_SOCKET"
UNIT_LOG_SERVICE_UID_ENV_VAR = "RQUANT_WEB_UNIT_LOG_SERVICE_UID"
UNIT_LOG_WEB_GROUP_GID_ENV_VAR = "RQUANT_WEB_UNIT_LOG_WEB_GROUP_GID"
UNIT_LOG_MANIFEST_ENV_VAR = "RQUANT_WEB_UNIT_LOG_MANIFEST"
UNIT_LOG_PUBLIC_KEY_ENV_VAR = "RQUANT_WEB_UNIT_LOG_PUBLIC_KEY"
UNIT_LOG_EXPECTED_HOST_ENV_VAR = "RQUANT_WEB_UNIT_LOG_EXPECTED_HOST"
UNIT_LOG_VERIFIED_UNITS_ENV_VAR = "RQUANT_WEB_UNIT_LOG_VERIFIED_UNITS"
UNIT_LOG_AUDIT_DIR_ENV_VAR = "RQUANT_WEB_UNIT_LOG_AUDIT_DIR"
_ADMIN_USER_PATTERN = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")
_HOST_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$")
_INITIAL_STRUCTURED_LOG_UNITS = frozenset({"rquant-daily.service", "rquant-backup.service"})

DEFAULT_BIND = "127.0.0.1:8768"
DEFAULT_PAGE_CONTROL_URL = "http://127.0.0.1:8767/v1/commands"
#: Same budget the Streamlit pages use (``query_acquired_serving_frame``'s default).
DEFAULT_STALE_AFTER_SECONDS = 600.0


def parse_bind(value: str) -> tuple[str, int]:
    """Split ``host:port`` and refuse anything but a loopback address.

    The default TCP listener serves public reads; private identity is only available
    through the explicitly configured Unix ingress and proxy proof.
    """

    host, separator, port_text = value.strip().rpartition(":")
    if not separator or not host or not port_text.isdigit():
        raise ValueError(f"bind address must be host:port, got {value!r}")
    host = host.strip("[]")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError(f"bind port out of range: {port}")
    if host != "localhost":
        try:
            address = ipaddress.ip_address(host)
        except ValueError as exc:
            raise ValueError(f"bind host must be a loopback address, got {host!r}") from exc
        if not address.is_loopback:
            raise ValueError(f"bind host must be a loopback address, got {host!r}")
    return host, port


class WebSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    serving_root: Path
    bind: str = DEFAULT_BIND
    page_control_url: str = DEFAULT_PAGE_CONTROL_URL
    stale_after_seconds: float = Field(default=DEFAULT_STALE_AFTER_SECONDS, gt=0)
    #: A request re-reads ``current.json`` at most this often.
    pointer_check_seconds: float = Field(default=2.0, gt=0)
    #: The background task checks for a new generation this often.
    background_check_seconds: float = Field(default=5.0, gt=0)
    #: Request handler threads (DuckDB cursors in flight at once).
    worker_threads: int = Field(default=4, ge=1, le=32)
    screen_primary_path: Path | None = None
    screen_replica_path: Path | None = None
    screen_history_root: Path | None = None
    screen_rsi_root: Path | None = None
    formula_market_result_root: Path | None = None
    formula_pool_daily_result_root: Path | None = None
    catalog_samples_file: Path | None = None
    nl_openai_api_key: SecretStr | None = None
    nl_openai_model: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
    ack_admission_socket_path: Path | None = None
    watchlist_admission_socket_path: Path | None = None
    factor_admission_socket_path: Path | None = None
    factor_admission_service_uid: StrictInt | None = None
    factor_admission_shared_gid: StrictInt | None = None
    factor_editor_users: frozenset[str] = frozenset()
    ingress_socket_path: Path | None = None
    proxy_proof_file: Path | None = None
    log_admin_users: frozenset[str] = frozenset()
    lab_control_users: frozenset[str] = frozenset()
    unit_log_socket_path: Path | None = None
    unit_log_service_uid: StrictInt | None = None
    unit_log_web_group_gid: StrictInt | None = None
    unit_log_manifest_path: Path | None = None
    unit_log_public_key_path: Path | None = None
    unit_log_expected_host: str | None = None
    unit_log_verified_units: frozenset[str] = frozenset()
    unit_log_audit_dir: Path | None = None

    @model_validator(mode="after")
    def validate_sources_and_ingress(self) -> Self:
        if (self.nl_openai_api_key is None) != (self.nl_openai_model is None):
            raise ValueError("natural-language API key and model must be configured together")
        if self.nl_openai_api_key is not None and self.ingress_socket_path is None:
            raise ValueError("natural-language model requires private Web ingress")
        if (self.screen_primary_path is None) != (self.screen_replica_path is None):
            raise ValueError("screen primary and replica paths must be configured together")
        if self.ingress_socket_path is not None and self.bind != DEFAULT_BIND:
            raise ValueError("private Web ingress cannot also configure a TCP bind")
        if self.proxy_proof_file is not None and self.ingress_socket_path is None:
            raise ValueError("proxy proof requires private Web ingress")
        if self.lab_control_users and self.ingress_socket_path is None:
            raise ValueError("Lab control requires private Web ingress")
        if self.ack_admission_socket_path is not None:
            if self.ingress_socket_path is None:
                raise ValueError("ack admission requires private Web ingress")
            if self.ingress_socket_path.parent == self.ack_admission_socket_path.parent:
                raise ValueError("private Web ingress and ack admission need separate directories")
        if self.watchlist_admission_socket_path is not None:
            if self.ingress_socket_path is None:
                raise ValueError("watchlist admission requires private Web ingress")
            if not self.watchlist_admission_socket_path.is_absolute():
                raise ValueError("watchlist admission socket path must be absolute")
            reserved = {self.ingress_socket_path.parent}
            if self.ack_admission_socket_path is not None:
                reserved.add(self.ack_admission_socket_path.parent)
            if self.watchlist_admission_socket_path.parent in reserved:
                raise ValueError("watchlist admission socket needs a separate directory")
        factor_fields = (
            self.factor_admission_socket_path,
            self.factor_admission_service_uid,
            self.factor_admission_shared_gid,
        )
        if any(field is not None for field in factor_fields) or self.factor_editor_users:
            if (
                not all(field is not None for field in factor_fields)
                or not self.factor_editor_users
                or self.ingress_socket_path is None
            ):
                raise ValueError("factor archive requires private ingress, socket, IDs and editors")
            if self.factor_admission_service_uid == os.geteuid():
                raise ValueError("factor archive service UID must differ from Web UID")
            reserved = {self.ingress_socket_path.parent}
            if self.ack_admission_socket_path is not None:
                reserved.add(self.ack_admission_socket_path.parent)
            if self.watchlist_admission_socket_path is not None:
                reserved.add(self.watchlist_admission_socket_path.parent)
            if self.factor_admission_socket_path.parent in reserved:
                raise ValueError("factor archive socket needs a separate private directory")
        log_fields = (
            self.unit_log_socket_path,
            self.unit_log_service_uid,
            self.unit_log_web_group_gid,
            self.unit_log_manifest_path,
            self.unit_log_public_key_path,
            self.unit_log_expected_host,
        )
        if any(value is not None for value in log_fields) and not all(
            value is not None for value in log_fields
        ):
            raise ValueError("service log settings must be configured together")
        if all(value is None for value in log_fields):
            if self.unit_log_verified_units or self.unit_log_audit_dir is not None:
                raise ValueError("verified service logs require the full private configuration")
            return self
        if self.ingress_socket_path is None or not self.log_admin_users:
            raise ValueError("service logs require private Web ingress and exact admins")
        if self.unit_log_service_uid == os.geteuid():
            raise ValueError("service log owner must differ from Web process owner")
        if self.unit_log_socket_path is not None and self.unit_log_socket_path.parent in {
            self.ingress_socket_path.parent,
            self.ack_admission_socket_path.parent if self.ack_admission_socket_path else None,
        }:
            raise ValueError("service logs need a separate private socket directory")
        if self.unit_log_audit_dir is not None and self.unit_log_audit_dir in {
            self.serving_root,
            self.ingress_socket_path.parent,
            self.ack_admission_socket_path.parent if self.ack_admission_socket_path else None,
            self.unit_log_socket_path.parent if self.unit_log_socket_path else None,
            self.unit_log_manifest_path.parent if self.unit_log_manifest_path else None,
            self.unit_log_public_key_path.parent if self.unit_log_public_key_path else None,
        }:
            raise ValueError("service log audit needs a dedicated directory")
        return self

    @field_validator("bind")
    @classmethod
    def validate_bind(cls, value: str) -> str:
        parse_bind(value)
        return value

    @field_validator("page_control_url")
    @classmethod
    def validate_page_control_url(cls, value: str) -> str:
        if value != DEFAULT_PAGE_CONTROL_URL:
            raise ValueError("page control URL must be the fixed IPv4 loopback endpoint")
        return value

    @field_validator("ack_admission_socket_path")
    @classmethod
    def validate_ack_admission_socket_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("ack admission socket path must be absolute")
        return value

    @field_validator("ingress_socket_path")
    @classmethod
    def validate_ingress_socket_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("private Web ingress socket path must be absolute")
        return value

    @field_validator("proxy_proof_file")
    @classmethod
    def validate_proxy_proof_file(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or ".." in value.parts):
            raise ValueError("proxy proof file path must be absolute and canonical")
        return value

    @field_validator("log_admin_users", "lab_control_users", "factor_editor_users")
    @classmethod
    def validate_operator_users(cls, value: frozenset[str]) -> frozenset[str]:
        if len(value) > 16 or any(_ADMIN_USER_PATTERN.fullmatch(user) is None for user in value):
            raise ValueError("operator users must be a bounded list of exact user names")
        return value

    @field_validator("formula_market_result_root", "formula_pool_daily_result_root")
    @classmethod
    def validate_readonly_result_root(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or ".." in value.parts):
            raise ValueError("read-only result root must be absolute and canonical")
        return value

    @field_validator("factor_admission_socket_path")
    @classmethod
    def validate_factor_admission_path(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or ".." in value.parts):
            raise ValueError("factor archive socket path must be absolute and canonical")
        return value

    @field_validator("factor_admission_service_uid", "factor_admission_shared_gid")
    @classmethod
    def validate_factor_admission_identity(cls, value: int | None) -> int | None:
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("factor archive IDs must be nonnegative integers")
        return value

    @field_validator("unit_log_socket_path", "unit_log_manifest_path", "unit_log_public_key_path")
    @classmethod
    def validate_unit_log_path(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or ".." in value.parts):
            raise ValueError("service log paths must be absolute and canonical")
        return value

    @field_validator("unit_log_audit_dir")
    @classmethod
    def validate_unit_log_audit_dir(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or ".." in value.parts):
            raise ValueError("service log audit directory must be absolute and canonical")
        return value

    @field_validator("unit_log_service_uid", "unit_log_web_group_gid")
    @classmethod
    def validate_unit_log_identity(cls, value: int | None) -> int | None:
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("service log owner and group IDs must be nonnegative integers")
        return value

    @field_validator("unit_log_expected_host")
    @classmethod
    def validate_unit_log_host(cls, value: str | None) -> str | None:
        if value is not None and _HOST_PATTERN.fullmatch(value) is None:
            raise ValueError("service log host must be an exact hostname")
        return value

    @field_validator("unit_log_verified_units")
    @classmethod
    def validate_unit_log_verified_units(cls, value: frozenset[str]) -> frozenset[str]:
        if not value.issubset(_INITIAL_STRUCTURED_LOG_UNITS):
            raise ValueError("service logs must name registered structured services exactly")
        return value

    @property
    def bind_host(self) -> str:
        return parse_bind(self.bind)[0]

    @property
    def bind_port(self) -> int:
        return parse_bind(self.bind)[1]

    @property
    def stale_after(self) -> timedelta:
        return timedelta(seconds=self.stale_after_seconds)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, *, bind: str | None = None) -> Self:
        source = os.environ if environ is None else environ
        values: dict[str, object] = {"serving_root": Path(serving_root_from_env(source))}
        chosen_bind = bind or source.get(BIND_ENV_VAR, "").strip()
        if chosen_bind:
            values["bind"] = chosen_bind
        page_control_url = source.get(PAGE_CONTROL_URL_ENV_VAR, "").strip()
        if page_control_url:
            values["page_control_url"] = page_control_url
        stale_after = source.get(STALE_AFTER_ENV_VAR, "").strip()
        if stale_after:
            values["stale_after_seconds"] = float(stale_after)
        primary = source.get(SCREEN_PRIMARY_ENV_VAR, "").strip()
        replica = source.get(SCREEN_REPLICA_ENV_VAR, "").strip()
        if primary:
            values["screen_primary_path"] = Path(primary)
        if replica:
            values["screen_replica_path"] = Path(replica)
        history = source.get(SCREEN_HISTORY_ENV_VAR, "").strip()
        if history:
            values["screen_history_root"] = Path(history)
        rsi = source.get(SCREEN_RSI_ENV_VAR, "").strip()
        if rsi:
            values["screen_rsi_root"] = Path(rsi)
        formula_market_result = source.get(FORMULA_MARKET_RESULT_ENV_VAR, "").strip()
        if formula_market_result:
            values["formula_market_result_root"] = Path(formula_market_result)
        formula_pool_daily_result = source.get(FORMULA_POOL_DAILY_RESULT_ENV_VAR, "").strip()
        if formula_pool_daily_result:
            values["formula_pool_daily_result_root"] = Path(formula_pool_daily_result)
        samples = source.get(CATALOG_SAMPLES_ENV_VAR, "").strip()
        if samples:
            values["catalog_samples_file"] = Path(samples)
        nl_key = source.get(NL_OPENAI_API_KEY_ENV_VAR, "").strip()
        nl_model = source.get(NL_OPENAI_MODEL_ENV_VAR, "").strip()
        if nl_key:
            values["nl_openai_api_key"] = SecretStr(nl_key)
        if nl_model:
            values["nl_openai_model"] = nl_model
        admission_socket = source.get(ACK_ADMISSION_SOCKET_ENV_VAR, "").strip()
        if admission_socket:
            values["ack_admission_socket_path"] = Path(admission_socket)
        watchlist_socket = source.get(WATCHLIST_ADMISSION_SOCKET_ENV_VAR, "").strip()
        if watchlist_socket:
            values["watchlist_admission_socket_path"] = Path(watchlist_socket)
        factor_socket = source.get(FACTOR_ADMISSION_SOCKET_ENV_VAR, "").strip()
        if factor_socket:
            values["factor_admission_socket_path"] = Path(factor_socket)
        for env_name, field_name in (
            (FACTOR_ADMISSION_SERVICE_UID_ENV_VAR, "factor_admission_service_uid"),
            (FACTOR_ADMISSION_SHARED_GID_ENV_VAR, "factor_admission_shared_gid"),
        ):
            raw = source.get(env_name, "").strip()
            if raw:
                values[field_name] = int(raw)
        editors = source.get(FACTOR_EDITOR_USERS_ENV_VAR, "").strip()
        if editors:
            names = tuple(user.strip() for user in editors.split(","))
            if any(not name for name in names) or len(set(names)) != len(names):
                raise ValueError("factor editors must be distinct nonempty names")
            values["factor_editor_users"] = frozenset(names)
        ingress_socket = source.get(INGRESS_SOCKET_ENV_VAR, "").strip()
        if ingress_socket:
            if bind is not None or source.get(BIND_ENV_VAR, "").strip():
                raise ValueError("private Web ingress cannot also configure a TCP bind")
            values["ingress_socket_path"] = Path(ingress_socket)
        proof_file = source.get(PROXY_PROOF_FILE_ENV_VAR, "").strip()
        if proof_file:
            values["proxy_proof_file"] = Path(proof_file)
        admins = source.get(LOG_ADMIN_USERS_ENV_VAR, "").strip()
        if admins:
            names = tuple(user.strip() for user in admins.split(","))
            if any(not name for name in names) or len(set(names)) != len(names):
                raise ValueError("log admins must be distinct nonempty user names")
            values["log_admin_users"] = frozenset(names)
        lab_users = source.get(LAB_CONTROL_USERS_ENV_VAR, "").strip()
        if lab_users:
            names = tuple(user.strip() for user in lab_users.split(","))
            if any(not name for name in names) or len(set(names)) != len(names):
                raise ValueError("Lab control users must be distinct nonempty names")
            values["lab_control_users"] = frozenset(names)
        log_paths = (
            (UNIT_LOG_SOCKET_ENV_VAR, "unit_log_socket_path"),
            (UNIT_LOG_MANIFEST_ENV_VAR, "unit_log_manifest_path"),
            (UNIT_LOG_PUBLIC_KEY_ENV_VAR, "unit_log_public_key_path"),
        )
        for source_name, field_name in log_paths:
            value = source.get(source_name, "").strip()
            if value:
                values[field_name] = Path(value)
        for source_name, field_name in (
            (UNIT_LOG_SERVICE_UID_ENV_VAR, "unit_log_service_uid"),
            (UNIT_LOG_WEB_GROUP_GID_ENV_VAR, "unit_log_web_group_gid"),
        ):
            value = source.get(source_name, "").strip()
            if value:
                values[field_name] = int(value)
        expected_host = source.get(UNIT_LOG_EXPECTED_HOST_ENV_VAR, "").strip()
        if expected_host:
            values["unit_log_expected_host"] = expected_host
        verified = source.get(UNIT_LOG_VERIFIED_UNITS_ENV_VAR, "").strip()
        if verified:
            names = tuple(unit.strip() for unit in verified.split(","))
            if any(not unit for unit in names) or len(set(names)) != len(names):
                raise ValueError("verified service logs must be distinct exact units")
            values["unit_log_verified_units"] = frozenset(names)
        audit_dir = source.get(UNIT_LOG_AUDIT_DIR_ENV_VAR, "").strip()
        if audit_dir:
            values["unit_log_audit_dir"] = Path(audit_dir)
        return cls.model_validate(values)
