"""Web API settings, read from the process environment only.

``rquant.config`` is deliberately not used: constructing it reads ``.env``, and the web
process must not see the secrets file at all (its systemd unit hides it). The default
listener is loopback TCP; acknowledgment admission requires a separate private Web UDS.
"""

from __future__ import annotations

import ipaddress
import os
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.serving_paths import serving_root_from_env

BIND_ENV_VAR = "RQUANT_WEB_BIND"
PAGE_CONTROL_URL_ENV_VAR = "RQUANT_PAGE_CONTROL_URL"
STALE_AFTER_ENV_VAR = "RQUANT_WEB_STALE_AFTER_SECONDS"
SCREEN_PRIMARY_ENV_VAR = "RQUANT_WEB_SCREEN_PRIMARY_PATH"
SCREEN_REPLICA_ENV_VAR = "RQUANT_WEB_SCREEN_REPLICA_PATH"
SCREEN_HISTORY_ENV_VAR = "RQUANT_WEB_SCREEN_HISTORY_ROOT"
CATALOG_SAMPLES_ENV_VAR = "RQUANT_WEB_CATALOG_SAMPLES_FILE"
ACK_ADMISSION_SOCKET_ENV_VAR = "RQUANT_WEB_ACK_ADMISSION_SOCKET"
INGRESS_SOCKET_ENV_VAR = "RQUANT_WEB_INGRESS_SOCKET"

DEFAULT_BIND = "127.0.0.1:8768"
DEFAULT_PAGE_CONTROL_URL = "http://127.0.0.1:8767/v1/commands"
#: Same budget the Streamlit pages use (``query_acquired_serving_frame``'s default).
DEFAULT_STALE_AFTER_SECONDS = 600.0


def parse_bind(value: str) -> tuple[str, int]:
    """Split ``host:port`` and refuse anything but a loopback address.

    The API has no authentication of its own; nginx basic auth in front of it is the
    only gate, so it must never listen on an address nginx does not front.
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
    catalog_samples_file: Path | None = None
    ack_admission_socket_path: Path | None = None
    ingress_socket_path: Path | None = None

    @model_validator(mode="after")
    def validate_sources_and_ingress(self) -> Self:
        if (self.screen_primary_path is None) != (self.screen_replica_path is None):
            raise ValueError("screen primary and replica paths must be configured together")
        if self.ingress_socket_path is not None and self.bind != DEFAULT_BIND:
            raise ValueError("private Web ingress cannot also configure a TCP bind")
        if self.ack_admission_socket_path is not None:
            if self.ingress_socket_path is None:
                raise ValueError("ack admission requires private Web ingress")
            if self.ingress_socket_path.parent == self.ack_admission_socket_path.parent:
                raise ValueError("private Web ingress and ack admission need separate directories")
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
        samples = source.get(CATALOG_SAMPLES_ENV_VAR, "").strip()
        if samples:
            values["catalog_samples_file"] = Path(samples)
        admission_socket = source.get(ACK_ADMISSION_SOCKET_ENV_VAR, "").strip()
        if admission_socket:
            values["ack_admission_socket_path"] = Path(admission_socket)
        ingress_socket = source.get(INGRESS_SOCKET_ENV_VAR, "").strip()
        if ingress_socket:
            if bind is not None or source.get(BIND_ENV_VAR, "").strip():
                raise ValueError("private Web ingress cannot also configure a TCP bind")
            values["ingress_socket_path"] = Path(ingress_socket)
        return cls.model_validate(values)
