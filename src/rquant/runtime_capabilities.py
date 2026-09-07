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
#: The credential id every runtime unit declares on its `LoadCredentialEncrypted=` line and
#: the `--name=` the root sealer encrypts under. systemd puts the decrypted plaintext at
#: `$CREDENTIALS_DIRECTORY/<this name>`, so the three places have to agree letter for letter.
RUNTIME_CAPABILITY_CREDENTIAL_NAME = "capabilities.json"
#: Where a Linux host says which unit this process belongs to, and where systemd puts the
#: unit's decrypted credentials. Only ever read to explain a failure, never to find the
#: credential itself — that address comes from `CREDENTIALS_DIRECTORY` and nowhere else.
_SYSTEMD_CGROUP_PATH = Path("/proc/self/cgroup")
_SYSTEMD_CREDENTIALS_ROOT = Path("/run/credentials")
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


def _systemd_unit_name() -> str | None:
    """The systemd unit this process belongs to, out of its own cgroup, or `None`."""

    try:
        text = _SYSTEMD_CGROUP_PATH.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        leaf = line.rpartition(":")[2].rpartition("/")[2]
        if leaf.endswith(".service"):
            return leaf
    return None


def _undelivered_credential_reason() -> str | None:
    """Why no credential directory reached this systemd unit's role child, or `None`.

    The two causes need different repairs and used to be indistinguishable, which is what
    made #215 read as "the capability is missing" when the capability had in fact been
    sealed, delivered and decrypted. systemd exports `CREDENTIALS_DIRECTORY` to the unit's
    ExecStart, which is the runtime-exec wrapper; the wrapper then builds the role child's
    environment from an empty dictionary and copies only the names the root-owned profile
    allowlists for that role, so an unlisted name is dropped without a word. The decrypted
    file is still on disk under `/run/credentials/<unit>` either way, and that is the
    evidence that tells the two apart.

    `None` means this process is not running under a systemd unit at all — a bare
    diagnostic run, or the suite. There is no delivery mechanism to accuse there, so the
    caller keeps the behaviour it has always had and lets the role's own builder refuse for
    the capability it actually wanted.
    """

    unit = _systemd_unit_name()
    if unit is None:
        return None
    delivered = _SYSTEMD_CREDENTIALS_ROOT / unit / RUNTIME_CAPABILITY_CREDENTIAL_NAME
    try:
        present = delivered.exists()
    except OSError:  # pragma: no cover - an unreadable /run/credentials is not the diagnosis
        present = False
    if present:
        return (
            f"systemd did load it for unit {unit}, so CREDENTIALS_DIRECTORY was dropped "
            "between the unit and this process: the runtime profile's environment allowlist "
            "for this role does not carry CREDENTIALS_DIRECTORY"
        )
    return (
        f"systemd loaded no {RUNTIME_CAPABILITY_CREDENTIAL_NAME} for unit {unit}: check the "
        "unit's LoadCredentialEncrypted= line and the sealed credstore entry for this instance"
    )


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
    expected_generation: str | None,
    environ: Mapping[str, str] | None = None,
) -> Mapping[str, str]:
    """The capability values systemd decrypted for this service instance, or refuse.

    `expected_generation` is the **deployment bundle** generation, the only namespace a
    sealed credential is ever bound to: `runtime_deployment_bundle` stamps its own
    `generation_hash` into every plaintext it hands the sealer. It is deliberately not the
    authority chain's generation id, which is what the wrapper forwards as
    `--expected-generation` and which never equals the bundle hash by construction — passing
    that one here would refuse every correctly sealed credential (the same two-namespace
    mistake as #207, and the next wall the credstore roles would have hit after #215).

    `None` means the caller has no deployment bundle at all (Route B publishes none). There
    is then nothing to bind a credential to, so a kind that needs one refuses, and a kind
    that does not may still not quietly accept one.
    """

    if not expected_service_id.strip():
        raise ValueError("expected runtime service id must be nonempty")
    if re.fullmatch(r"svc-[0-9a-f]{64}", expected_instance) is None:
        raise ValueError("expected runtime instance is invalid")
    target = environ if environ is not None else os.environ
    credential_directory = target.get("CREDENTIALS_DIRECTORY", "").strip()
    required = bool(CAPABILITY_KEYS.get(service_kind, frozenset()))
    if not credential_directory and required:
        # The delivery diagnosis comes first and applies on both routes. Which of the two
        # links broke does not depend on whether a deployment bundle exists, and a role
        # started under a systemd unit that was supposed to carry a credential has a broken
        # link either way — putting the Route B branch ahead of this made both messages
        # unreachable there and let the same silent degradation back in.
        reason = _undelivered_credential_reason()
        if reason is not None:
            # Not "the capability is missing": the capability may well have been sealed and
            # decrypted. Say which of the two links is broken so the repair is the right one.
            raise ValueError(
                f"runtime capability credential was not delivered to "
                f"{service_kind.value}: {reason}"
            )
    if expected_generation is None:
        # Route B publishes no deployment bundle, and the bundle generation is the only
        # namespace a credential is ever sealed in, so on this route no credential for this
        # instance can exist and none could be bound if it did. A kind that needs one
        # therefore cannot run here at all — that is a structural fact rather than a
        # diagnosis, which is why it refuses even outside a systemd unit, unlike the branch
        # above. A kind that needs none may still not quietly accept one.
        if required or credential_directory:
            raise ValueError(
                "runtime capability credential cannot be bound without a deployment generation"
            )
        return LoadedRuntimeCapabilities({})
    if re.fullmatch(r"[0-9a-f]{64}", expected_generation) is None:
        raise ValueError("expected runtime generation must be a lowercase SHA-256")
    if not credential_directory:
        # Route A, nothing delivered, and nothing to accuse: a bare diagnostic run. The
        # role's own builder refuses for the capability it wanted, exactly as it always did.
        return LoadedRuntimeCapabilities({})
    credential_path = Path(credential_directory) / RUNTIME_CAPABILITY_CREDENTIAL_NAME
    try:
        payload = _read_private_credential(credential_path)
    except ValueError as exc:
        if not credential_path.exists():
            raise ValueError(
                f"the systemd credential directory carries no "
                f"{RUNTIME_CAPABILITY_CREDENTIAL_NAME}: systemd loaded credentials for this "
                f"unit but not this one, so the unit's LoadCredentialEncrypted= name does "
                f"not match what the sealer encrypted under"
            ) from exc
        raise
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
    unknown = set(decoded) - CAPABILITY_KEYS.get(service_kind, frozenset())
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
    "RUNTIME_CAPABILITY_CREDENTIAL_NAME",
    "LoadedRuntimeCapabilities",
    "RuntimeCapabilityCredential",
    "SECRET_CAPABILITY_KEYS",
    "load_systemd_runtime_capabilities",
    "serialize_runtime_capabilities",
    "serialize_runtime_credential",
]
