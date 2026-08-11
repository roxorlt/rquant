"""Systemd credential loading for capability-scoped runtime services."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal

from pydantic import StringConstraints, field_serializer, field_validator

from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

CAPABILITY_KEYS: Mapping[RuntimeServiceKind, frozenset[str]] = MappingProxyType(
    {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: frozenset(
            {
                "TUSHARE_TOKEN_MAIN",
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
                "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
                "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
            }
        ),
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: frozenset(
            {
                "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID",
                "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
                "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
            }
        ),
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: frozenset(
            {"TUSHARE_TOKEN_MAIN", "TUSHARE_TOKEN_BACKUP"}
        ),
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
        RuntimeServiceKind.NOTIFIER: frozenset(
            {
                "PUSHDEER_KEYS",
                "PUSHPLUS_TOKENS",
                "PUSHDEER_ENDPOINT",
                "PUSHPLUS_ENDPOINT",
                "PUSHDEER_RECIPIENT_IDS",
                "PUSHPLUS_RECIPIENT_IDS",
            }
        ),
        RuntimeServiceKind.ARTIFACT_RETENTION: frozenset(
            {"RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"}
        ),
    }
)
SECRET_CAPABILITY_KEYS = frozenset(
    {
        "TUSHARE_TOKEN_MAIN",
        "TUSHARE_TOKEN_BACKUP",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
        "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL",
    }
)
_MAX_CAPABILITY_BYTES = 1024 * 1024
GenerationHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
InstanceName = Annotated[str, StringConstraints(pattern=r"^svc-[0-9a-f]{64}$")]


class RuntimeCapabilityCredential(RuntimeContractModel):
    schema_version: Literal[2] = 2
    service_id: str
    service_kind: RuntimeServiceKind
    instance_name: InstanceName
    bundle_generation: GenerationHash
    capabilities: Mapping[str, str]

    @field_validator("capabilities")
    @classmethod
    def freeze_capabilities(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("capabilities")
    def serialize_capabilities(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)


class LoadedRuntimeCapabilities(Mapping[str, str]):
    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = MappingProxyType(dict(values))

    def __getitem__(self, key: str) -> str:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"LoadedRuntimeCapabilities(keys={tuple(self._values)!r}, values=<redacted>)"


def _normalize_runtime_capabilities(values: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(values, Mapping):
        raise TypeError("runtime capabilities must be a mapping")
    normalized: dict[str, str] = {}
    for name, value in sorted(values.items()):
        if not isinstance(name, str) or not name:
            raise ValueError("runtime capability names must be nonempty strings")
        if not isinstance(value, str) or not value:
            raise ValueError(f"runtime capability {name} must be a nonempty string")
        if any(character in value for character in ("\x00", "\n", "\r")):
            raise ValueError(f"runtime capability {name} has an unsafe value")
        normalized[name] = value
    return normalized


def serialize_runtime_capabilities(values: Mapping[str, str]) -> bytes:
    return canonical_json_bytes(_normalize_runtime_capabilities(values))


def serialize_runtime_credential(
    *,
    service_id: str,
    service_kind: RuntimeServiceKind,
    instance_name: str,
    bundle_generation: str,
    values: Mapping[str, str],
) -> bytes:
    credential = RuntimeCapabilityCredential(
        service_id=service_id,
        service_kind=service_kind,
        instance_name=instance_name,
        bundle_generation=bundle_generation,
        capabilities=_normalize_runtime_capabilities(values),
    )
    return canonical_json_bytes(credential.model_dump(mode="json"))


def _read_private_credential(path: Path) -> bytes:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError("systemd credential path must be absolute and normalized")
    descriptor = -1
    try:
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("systemd credential must be a regular file")
        if observed.st_uid != os.geteuid():
            raise ValueError("systemd credential must be owned by the runtime uid")
        if observed.st_nlink != 1:
            raise ValueError("systemd credential hardlink count must be one")
        if observed.st_mode & 0o077:
            raise ValueError("systemd credential must not be group or world accessible")
        if observed.st_size <= 0 or observed.st_size > _MAX_CAPABILITY_BYTES:
            raise ValueError("systemd credential size is unsafe")
        payload = os.read(descriptor, _MAX_CAPABILITY_BYTES + 1)
        after = os.fstat(descriptor)
        if (
            len(payload) != observed.st_size
            or len(payload) > _MAX_CAPABILITY_BYTES
            or (observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("systemd credential changed while being read")
        return payload
    except OSError as exc:
        raise ValueError("systemd credential is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def load_systemd_runtime_capabilities(
    service_kind: RuntimeServiceKind,
    *,
    expected_service_id: str,
    expected_instance: str,
    expected_generation: str,
    environ: Mapping[str, str] | None = None,
) -> Mapping[str, str]:
    if not expected_service_id.strip():
        raise ValueError("expected runtime service id must be nonempty")
    if re.fullmatch(r"svc-[0-9a-f]{64}", expected_instance) is None:
        raise ValueError("expected runtime instance is invalid")
    if re.fullmatch(r"[0-9a-f]{64}", expected_generation) is None:
        raise ValueError("expected runtime generation must be a lowercase SHA-256")
    target = environ if environ is not None else os.environ
    credential_directory = target.get("CREDENTIALS_DIRECTORY", "").strip()
    if not credential_directory:
        return LoadedRuntimeCapabilities({})
    payload = _read_private_credential(Path(credential_directory) / "capabilities.json")
    credential = strict_model_validate_json(RuntimeCapabilityCredential, payload)
    if credential.service_id != expected_service_id:
        raise ValueError("systemd capability credential service does not match runtime")
    if credential.service_kind is not service_kind:
        raise ValueError("systemd capability credential kind does not match runtime")
    if credential.instance_name != expected_instance:
        raise ValueError("systemd capability credential instance does not match runtime")
    if credential.bundle_generation != expected_generation:
        raise ValueError("systemd capability credential generation does not match runtime")
    decoded = credential.capabilities
    allowed = CAPABILITY_KEYS.get(service_kind, frozenset())
    unknown = set(decoded) - allowed
    if unknown:
        raise ValueError(
            "systemd capability credential contains keys outside the service allowlist"
        )
    loaded: dict[str, str] = {}
    for name, value in sorted(decoded.items()):
        if not isinstance(value, str) or not value:
            raise ValueError("systemd capability values must be nonempty strings")
        existing = target.get(name)
        if existing is not None:
            if existing != value:
                raise ValueError("systemd capability conflicts with the process environment")
            raise ValueError("systemd capability is already present in the process environment")
        loaded[name] = value
    return LoadedRuntimeCapabilities(loaded)


__all__ = [
    "CAPABILITY_KEYS",
    "LoadedRuntimeCapabilities",
    "RuntimeCapabilityCredential",
    "SECRET_CAPABILITY_KEYS",
    "load_systemd_runtime_capabilities",
    "serialize_runtime_capabilities",
    "serialize_runtime_credential",
]
