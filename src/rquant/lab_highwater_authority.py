"""Least-privilege client for the independent Lab high-water authority.

The authority owns both the durable monotonic chain and its Ed25519 private
keys.  A Lab runner holds public verification keys only.  In production it can
invoke exactly one root-owned helper through a no-argument sudo rule; neither a
helper path, a state root nor key material is configurable by the runner.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_CODE_IDENTITY = re.compile(r"^[0-9a-f]{40,128}$")
_PROFILE_IDENTITY = re.compile(r"^[0-9a-f]{64,128}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_ED25519_SIGNATURE = re.compile(r"^[A-Za-z0-9+/]{86}==$")
_MAX_RESPONSE_BYTES = 64 * 1024
_REJECTION_EXIT_CODE = 3
_KEYRING_GENESIS_HASH = "0" * 64

PRODUCTION_LAB_HIGHWATER_COMMAND: Final[tuple[str, str, str]] = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-lab-highwater-authority",
)
_PRODUCTION_HELPER = Path(PRODUCTION_LAB_HIGHWATER_COMMAND[-1])

LabHighWaterReceiptKind = Literal["incremental", "full"]


class LabHighWaterAuthorityError(RuntimeError):
    """Base failure for the external high-water authority; always fail closed."""


class LabHighWaterDegradedError(LabHighWaterAuthorityError):
    """The authority is unavailable, timed out, or returned an invalid receipt."""


class LabHighWaterRollbackError(LabHighWaterAuthorityError):
    """The authority rejected the observation as a monotonicity conflict."""


class _HighWaterModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class LabHighWaterMark(_HighWaterModel):
    sequence: int = Field(ge=0)
    stable_identity: str = Field(min_length=1, max_length=512)
    database_generation: tuple[int, int]
    schema_generation: int = Field(ge=1)
    mutation_epoch: int = Field(ge=0)
    chain_generation: int = Field(ge=0)
    chain_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_kind: LabHighWaterReceiptKind
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_identity: str = Field(pattern=r"^[0-9a-f]{40,128}$")
    profile_identity: str = Field(pattern=r"^[0-9a-f]{64,128}$")
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class LabHighWaterReceipt(_HighWaterModel):
    schema_version: Literal[1] = 1
    operation: Literal["observe", "status", "degrade", "remediate"]
    outcome: Literal["advanced", "unchanged", "current", "degraded", "authorized"]
    nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    high_water: LabHighWaterMark | None
    key_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^[A-Za-z0-9+/]{86}==$")

    def verify(self, trusted_key_provider: Callable[[str], bytes | None]) -> None:
        payload = self.model_dump(mode="json", exclude={"receipt_hash", "signature"})
        expected_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        if self.receipt_hash != expected_hash:
            raise LabHighWaterDegradedError("high-water receipt hash is invalid")
        public_key = trusted_key_provider(self.key_id)
        if public_key is None:
            raise LabHighWaterDegradedError("high-water receipt signing key is not trusted")
        if not _verify_ed25519(public_key, expected_hash.encode("ascii"), self.signature):
            raise LabHighWaterDegradedError("high-water receipt signature is invalid")


@dataclass(frozen=True)
class LabHighWaterTrustedKeyring(Mapping[str, bytes]):
    """Self-bound public keys with one live signer and historical verification keys."""

    generation: int
    previous_manifest_hash: str
    active_key_id: str
    active_public_key: bytes
    previous_public_keys: Mapping[str, bytes]
    manifest_hash: str
    signature: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "previous_public_keys",
            MappingProxyType(dict(self.previous_public_keys)),
        )

    @property
    def previous_key_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.previous_public_keys))

    def __getitem__(self, key_id: str) -> bytes:
        if key_id == self.active_key_id:
            return self.active_public_key
        return self.previous_public_keys[key_id]

    def __iter__(self) -> Iterator[str]:
        yield self.active_key_id
        yield from sorted(self.previous_public_keys)

    def __len__(self) -> int:
        return 1 + len(self.previous_public_keys)


def _openssl_binary() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise ValueError("openssl is required to verify high-water Ed25519 receipts")


def _run_openssl(
    arguments: list[str], *, input_data: bytes, timeout_seconds: float = 5.0
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            [_openssl_binary(), *arguments],
            input=input_data,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("openssl verification is unavailable") from exc


def _validate_ed25519_public_key(public_key: bytes) -> None:
    result = _run_openssl(
        ["pkey", "-pubin", "-pubcheck", "-text_pub", "-noout"], input_data=public_key
    )
    if result.returncode != 0 or b"ED25519" not in result.stdout.upper():
        raise ValueError("high-water trusted keyring contains a non-Ed25519 public key")


def _verify_ed25519(public_key: bytes, payload: bytes, signature: str) -> bool:
    try:
        import base64

        decoded_signature = base64.b64decode(signature, validate=True)
        if len(decoded_signature) != 64:
            return False
        with tempfile.TemporaryDirectory(prefix="rquant-highwater-verify-") as temporary_dir:
            root = Path(temporary_dir)
            public_path = root / "public.pem"
            signature_path = root / "signature.bin"
            message_path = root / "message.bin"
            public_path.write_bytes(public_key)
            signature_path.write_bytes(decoded_signature)
            message_path.write_bytes(payload)
            os.chmod(public_path, 0o600)
            os.chmod(signature_path, 0o600)
            os.chmod(message_path, 0o600)
            result = _run_openssl(
                [
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(public_path),
                    "-sigfile",
                    str(signature_path),
                    "-rawin",
                    "-in",
                    str(message_path),
                ],
                input_data=b"",
            )
    except (OSError, ValueError):
        return False
    return result.returncode == 0


def _verify_production_helper_identity(command: tuple[str, ...]) -> None:
    """Reject mutable helper paths before asking sudo to execute the authority."""

    if command != PRODUCTION_LAB_HIGHWATER_COMMAND:
        raise ValueError("production high-water authority command must be the fixed sudo helper")
    for directory in (Path("/usr"), Path("/usr/local"), Path("/usr/local/libexec")):
        observed = os.stat(directory, follow_symlinks=False)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != 0
            or observed.st_mode & 0o022
        ):
            raise ValueError("production high-water helper parent is not root-owned and immutable")
    descriptor = -1
    try:
        descriptor = os.open(
            _PRODUCTION_HELPER,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        named = os.stat(_PRODUCTION_HELPER, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_uid != 0
            or stat.S_IMODE(opened.st_mode) != 0o755
        ):
            raise ValueError("production high-water helper must be root:root mode 0755")
    except OSError as exc:
        raise ValueError("production high-water helper is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@dataclass(frozen=True)
class LabHighWaterAuthorityConfig:
    """Explicit dependency injection for the external high-water authority."""

    command: tuple[str, ...]
    stable_identity: str
    code_identity: str
    profile_identity: str
    trusted_key_provider: Callable[[str], bytes | None]
    active_key_id: str | None = None
    timeout_seconds: float = 10.0
    allow_identity_rotation: bool = False
    production_mode: bool = False
    helper_identity_validator: Callable[[tuple[str, ...]], None] = (
        _verify_production_helper_identity
    )

    def __post_init__(self) -> None:
        if not self.command or any(
            not isinstance(part, str) or not part for part in self.command
        ):
            raise ValueError("high-water authority command is invalid")
        if not self.stable_identity or len(self.stable_identity) > 512:
            raise ValueError("high-water authority stable_identity is invalid")
        if _CODE_IDENTITY.fullmatch(self.code_identity) is None:
            raise ValueError("high-water authority code_identity must be a SHA")
        if _PROFILE_IDENTITY.fullmatch(self.profile_identity) is None:
            raise ValueError("high-water authority profile_identity must be a SHA")
        if not 0.1 <= self.timeout_seconds <= 300:
            raise ValueError("high-water authority timeout_seconds is outside the safe range")
        if self.production_mode and self.command != PRODUCTION_LAB_HIGHWATER_COMMAND:
            raise ValueError(
                "production high-water authority command must be the fixed sudo helper"
            )

    def resolved_active_key_id(self) -> str | None:
        if self.active_key_id is not None:
            return self.active_key_id
        provider_owner = getattr(self.trusted_key_provider, "__self__", None)
        if isinstance(provider_owner, LabHighWaterTrustedKeyring):
            return provider_owner.active_key_id
        return None


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("high-water trusted keyring contains duplicate JSON keys")
        result[key] = value
    return result


def load_highwater_trusted_keys(path: Path) -> LabHighWaterTrustedKeyring:
    """Load and authenticate a public-only Ed25519 active/previous keyring."""

    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        observed = os.fstat(descriptor)
        mode = stat.S_IMODE(observed.st_mode)
        if not stat.S_ISREG(observed.st_mode) or mode not in {0o400, 0o444, 0o600}:
            raise ValueError("high-water trusted keyring file is unsafe")
        if observed.st_size <= 0 or observed.st_size > _MAX_RESPONSE_BYTES:
            raise ValueError("high-water trusted keyring size is unsafe")
        payload = os.read(descriptor, _MAX_RESPONSE_BYTES + 1)
        after = os.fstat(descriptor)
        if len(payload) != observed.st_size or (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("high-water trusted keyring changed while reading")
    except OSError as exc:
        raise ValueError("high-water trusted keyring is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        document = json.loads(payload, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise ValueError("high-water trusted keyring is not valid JSON") from exc
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != 3
        or set(document)
        != {
            "schema_version",
            "generation",
            "previous_manifest_hash",
            "active_key_id",
            "active_public_key",
            "previous_public_keys",
            "manifest_hash",
            "signature",
        }
    ):
        raise ValueError("high-water trusted keyring shape is invalid")
    generation = document["generation"]
    previous_manifest_hash = document["previous_manifest_hash"]
    active_key_id = document["active_key_id"]
    active_public_key_raw = document["active_public_key"]
    previous_public_keys_raw = document["previous_public_keys"]
    manifest_hash = document["manifest_hash"]
    signature = document["signature"]
    if type(generation) is not int or generation < 1:
        raise ValueError("high-water trusted keyring generation is invalid")
    if (
        not isinstance(previous_manifest_hash, str)
        or _HEX64.fullmatch(previous_manifest_hash) is None
    ):
        raise ValueError("high-water trusted keyring previous manifest hash is invalid")
    if not isinstance(active_key_id, str) or _KEY_ID.fullmatch(active_key_id) is None:
        raise ValueError("high-water trusted keyring active key id is invalid")
    if not isinstance(active_public_key_raw, str) or len(active_public_key_raw) > 8_192:
        raise ValueError("high-water trusted keyring active public key is invalid")
    if not isinstance(previous_public_keys_raw, dict):
        raise ValueError("high-water trusted keyring previous public keys are invalid")
    if generation == 1 and (
        previous_manifest_hash != _KEYRING_GENESIS_HASH or previous_public_keys_raw
    ):
        raise ValueError("high-water trusted keyring genesis binding is invalid")
    if generation > 1 and (
        previous_manifest_hash == _KEYRING_GENESIS_HASH or not previous_public_keys_raw
    ):
        raise ValueError("high-water trusted keyring rotation binding is invalid")
    active_public_key = active_public_key_raw.encode("utf-8")
    _validate_ed25519_public_key(active_public_key)
    previous_public_keys: dict[str, bytes] = {}
    for key_id, raw_public_key in previous_public_keys_raw.items():
        if not isinstance(key_id, str) or _KEY_ID.fullmatch(key_id) is None:
            raise ValueError("high-water trusted keyring contains an invalid key id")
        if key_id == active_key_id:
            raise ValueError("high-water active key cannot also be a previous key")
        if not isinstance(raw_public_key, str) or len(raw_public_key) > 8_192:
            raise ValueError("high-water trusted keyring contains an invalid public key")
        public_key = raw_public_key.encode("utf-8")
        _validate_ed25519_public_key(public_key)
        previous_public_keys[key_id] = public_key
    body = {
        "schema_version": 3,
        "generation": generation,
        "previous_manifest_hash": previous_manifest_hash,
        "active_key_id": active_key_id,
        "active_public_key": active_public_key_raw,
        "previous_public_keys": previous_public_keys_raw,
    }
    expected_manifest_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    if not isinstance(manifest_hash, str) or manifest_hash != expected_manifest_hash:
        raise ValueError("high-water trusted keyring manifest hash is invalid")
    if not isinstance(signature, str) or _ED25519_SIGNATURE.fullmatch(signature) is None:
        raise ValueError("high-water trusted keyring manifest signature is invalid")
    if not _verify_ed25519(active_public_key, manifest_hash.encode("ascii"), signature):
        raise ValueError("high-water trusted keyring manifest signature is invalid")
    return LabHighWaterTrustedKeyring(
        generation=generation,
        previous_manifest_hash=previous_manifest_hash,
        active_key_id=active_key_id,
        active_public_key=active_public_key,
        previous_public_keys=previous_public_keys,
        manifest_hash=manifest_hash,
        signature=signature,
    )


class LabHighWaterAuthorityClient:
    """Submit compare-and-advance observations and verify signed receipts."""

    def __init__(self, config: LabHighWaterAuthorityConfig) -> None:
        self.config = config

    def _invoke(self, request: dict[str, object]) -> LabHighWaterReceipt:
        nonce = str(request["nonce"])
        if self.config.production_mode:
            try:
                self.config.helper_identity_validator(self.config.command)
            except ValueError as exc:
                raise LabHighWaterDegradedError(str(exc)) from exc
        try:
            result = subprocess.run(
                list(self.config.command),
                input=canonical_json_bytes(request),
                capture_output=True,
                check=False,
                timeout=self.config.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise LabHighWaterDegradedError(
                "high-water authority exceeded its hard timeout"
            ) from exc
        except OSError as exc:
            raise LabHighWaterDegradedError("high-water authority is unavailable") from exc
        if result.returncode == _REJECTION_EXIT_CODE:
            detail = " ".join(result.stderr.decode("utf-8", "replace").split())[:400]
            raise LabHighWaterRollbackError(
                f"high-water authority rejected the observation: {detail}"
            )
        if result.returncode != 0 or not result.stdout:
            detail = " ".join(result.stderr.decode("utf-8", "replace").split())[:400]
            raise LabHighWaterDegradedError(f"high-water authority failed: {detail}")
        if len(result.stdout) > _MAX_RESPONSE_BYTES:
            raise LabHighWaterDegradedError("high-water authority response size is unsafe")
        try:
            receipt = strict_model_validate_json(LabHighWaterReceipt, result.stdout)
        except Exception as exc:
            raise LabHighWaterDegradedError(
                "high-water authority returned an invalid receipt"
            ) from exc
        active_key_id = self.config.resolved_active_key_id()
        if active_key_id is None:
            raise LabHighWaterDegradedError(
                "high-water active signing key is unavailable"
            )
        if receipt.key_id != active_key_id:
            raise LabHighWaterDegradedError(
                "high-water live receipt was not signed by the active signing key"
            )
        receipt.verify(
            lambda key_id: (
                self.config.trusted_key_provider(key_id)
                if key_id == active_key_id
                else None
            )
        )
        if receipt.nonce != nonce:
            raise LabHighWaterDegradedError("high-water authority receipt was replayed")
        if receipt.operation != request["operation"]:
            raise LabHighWaterDegradedError("high-water authority receipt operation mismatch")
        return receipt

    def observe(
        self,
        *,
        database_generation: tuple[int, int],
        schema_generation: int,
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        receipt_kind: LabHighWaterReceiptKind,
        receipt_hash: str,
    ) -> LabHighWaterReceipt:
        if _HEX64.fullmatch(chain_head_hash) is None:
            raise ValueError("high-water observation chain_head_hash is invalid")
        if _HEX64.fullmatch(receipt_hash) is None:
            raise ValueError("high-water observation receipt_hash is invalid")
        nonce = secrets.token_hex(32)
        request: dict[str, object] = {
            "schema_version": 1,
            "operation": "observe",
            "stable_identity": self.config.stable_identity,
            "database_generation": list(database_generation),
            "schema_generation": schema_generation,
            "mutation_epoch": mutation_epoch,
            "chain_generation": chain_generation,
            "chain_head_hash": chain_head_hash,
            "receipt_kind": receipt_kind,
            "receipt_hash": receipt_hash,
            "code_identity": self.config.code_identity,
            "profile_identity": self.config.profile_identity,
            "allow_identity_rotation": self.config.allow_identity_rotation,
            "nonce": nonce,
        }
        receipt = self._invoke(request)
        if receipt.outcome not in {"advanced", "unchanged"}:
            raise LabHighWaterDegradedError("high-water authority receipt outcome is invalid")
        mark = receipt.high_water
        if mark is None:
            raise LabHighWaterDegradedError("high-water authority receipt omits the watermark")
        expected = (
            self.config.stable_identity,
            tuple(database_generation),
            schema_generation,
            mutation_epoch,
            chain_generation,
            chain_head_hash,
            receipt_kind,
            receipt_hash,
            self.config.code_identity,
            self.config.profile_identity,
        )
        observed = (
            mark.stable_identity,
            mark.database_generation,
            mark.schema_generation,
            mark.mutation_epoch,
            mark.chain_generation,
            mark.chain_head_hash,
            mark.receipt_kind,
            mark.receipt_hash,
            mark.code_identity,
            mark.profile_identity,
        )
        if receipt.outcome == "advanced" and observed != expected:
            raise LabHighWaterDegradedError(
                "high-water authority receipt does not bind the observation"
            )
        if receipt.outcome == "unchanged" and (
            observed[:6] != expected[:6] or observed[8:] != expected[8:]
        ):
            raise LabHighWaterDegradedError(
                "high-water authority receipt does not match the observed watermark"
            )
        return receipt

    def status(self) -> LabHighWaterReceipt:
        nonce = secrets.token_hex(32)
        request: dict[str, object] = {
            "schema_version": 1,
            "operation": "status",
            "stable_identity": self.config.stable_identity,
            "nonce": nonce,
        }
        receipt = self._invoke(request)
        if receipt.outcome != "current":
            raise LabHighWaterDegradedError("high-water authority status outcome is invalid")
        return receipt

    def mark_degraded(self, reason: str) -> LabHighWaterReceipt:
        """Persist an authority-owned full-audit degradation fence."""

        normalized_reason = " ".join(reason.split())
        if not normalized_reason or len(normalized_reason) > 1_024:
            raise ValueError("high-water degradation reason is invalid")
        nonce = secrets.token_hex(32)
        receipt = self._invoke(
            {
                "schema_version": 1,
                "operation": "degrade",
                "stable_identity": self.config.stable_identity,
                "code_identity": self.config.code_identity,
                "profile_identity": self.config.profile_identity,
                "reason_hash": hashlib.sha256(normalized_reason.encode("utf-8")).hexdigest(),
                "nonce": nonce,
            }
        )
        if receipt.outcome != "degraded":
            raise LabHighWaterDegradedError("high-water degradation receipt is invalid")
        return receipt

    def authorize_remediation(self) -> LabHighWaterReceipt:
        """Consume one administrator-issued remediation authorization."""

        nonce = secrets.token_hex(32)
        receipt = self._invoke(
            {
                "schema_version": 1,
                "operation": "remediate",
                "stable_identity": self.config.stable_identity,
                "code_identity": self.config.code_identity,
                "profile_identity": self.config.profile_identity,
                "nonce": nonce,
            }
        )
        if receipt.outcome != "authorized":
            raise LabHighWaterDegradedError("high-water remediation authorization is invalid")
        return receipt


__all__ = [
    "LabHighWaterAuthorityClient",
    "LabHighWaterAuthorityConfig",
    "LabHighWaterAuthorityError",
    "LabHighWaterDegradedError",
    "LabHighWaterMark",
    "LabHighWaterReceipt",
    "LabHighWaterReceiptKind",
    "LabHighWaterRollbackError",
    "LabHighWaterTrustedKeyring",
    "PRODUCTION_LAB_HIGHWATER_COMMAND",
    "load_highwater_trusted_keys",
]
