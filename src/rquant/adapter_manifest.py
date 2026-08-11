"""Ed25519-authorized contracts for external research data adapters."""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import subprocess
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Protocol, Self

from pydantic import BaseModel, Field, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

ADAPTER_MANIFEST_NAMESPACE = "rquant-adapter-manifest/v1"
SOURCE_USE_PLAN_NAMESPACE = "rquant-source-use-plan/v1"
SOURCE_USE_PLAN_V2_NAMESPACE = "rquant-source-use-plan/v2"
SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE = "rquant-scheduler-intent-authorization/v1"
BROKER_RECEIPT_NAMESPACE = "rquant-source-broker-receipt/v1"
BROKER_STATEMENT_NAMESPACE = "rquant-source-broker-statement/v1"
QUOTA_EFFECT_NAMESPACE = "rquant-source-quota-effect/v1"
REPLAY_CLAIM_NAMESPACE = "rquant-source-replay-claim/v1"
BROKER_OUTBOX_NAMESPACE = "rquant-source-broker-outbox/v1"
LAB_CLAIM_FINALIZER_ROOT_NAMESPACE = "rquant-lab-claim-finalizer-root/v1"
LAB_CLAIM_FINALIZER_NAMESPACE = "rquant-lab-claim-finalizer/v1"
RUNTIME_CODE_ROOT_NAMESPACE = "rquant-runtime-code-root/v1"
RUNTIME_CODE_ATTESTATION_NAMESPACE = "rquant-runtime-code-attestation/v1"
RUNTIME_CODE_PROMOTION_NAMESPACE = "rquant-runtime-code-promotion/v1"
_ED25519_SIGNATURE_BYTES = 64
_VERIFY_ONLY_KEYRING_CACHE_SIZE = 512

KeyPurpose = Literal[
    "adapter_manifest",
    "source_use_plan",
    "source_use_plan_v2",
    "scheduler_intent_authorization",
    "broker_receipt",
    "quota_effect",
    "replay_claim",
    "broker_outbox",
    "lab_claim_finalizer_root",
    "lab_claim_finalizer",
    "rquant_runtime_code_root",
    "rquant_runtime_code_signer",
    "rquant_runtime_code_promotion_root",
]
RotationState = Literal["active", "previous"]
NetworkMode = Literal["none", "provider"]

_PURPOSE_NAMESPACES: Mapping[KeyPurpose, frozenset[str]] = {
    "adapter_manifest": frozenset({ADAPTER_MANIFEST_NAMESPACE}),
    "source_use_plan": frozenset({SOURCE_USE_PLAN_NAMESPACE}),
    "source_use_plan_v2": frozenset({SOURCE_USE_PLAN_V2_NAMESPACE}),
    "scheduler_intent_authorization": frozenset({SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE}),
    "broker_receipt": frozenset({BROKER_RECEIPT_NAMESPACE, BROKER_STATEMENT_NAMESPACE}),
    "quota_effect": frozenset({QUOTA_EFFECT_NAMESPACE}),
    "replay_claim": frozenset({REPLAY_CLAIM_NAMESPACE}),
    "broker_outbox": frozenset({BROKER_OUTBOX_NAMESPACE}),
    "lab_claim_finalizer_root": frozenset({LAB_CLAIM_FINALIZER_ROOT_NAMESPACE}),
    "lab_claim_finalizer": frozenset({LAB_CLAIM_FINALIZER_NAMESPACE}),
    "rquant_runtime_code_root": frozenset({RUNTIME_CODE_ROOT_NAMESPACE}),
    "rquant_runtime_code_signer": frozenset({RUNTIME_CODE_ATTESTATION_NAMESPACE}),
    "rquant_runtime_code_promotion_root": frozenset({RUNTIME_CODE_PROMOTION_NAMESPACE}),
}


class ManifestSignatureError(ValueError):
    """Raised when a signed adapter contract cannot be trusted."""


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _domain_payload(*, key_purpose: KeyPurpose, namespace: str, payload: bytes) -> bytes:
    return _canonical_json_bytes(
        {
            "contract": "rquant-ed25519-domain-separation/v1",
            "key_purpose": key_purpose,
            "namespace": namespace,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )


def _openssl_binary() -> str:
    candidates = ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl"))
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise ValueError("openssl is required for Ed25519 contract verification")


def _valid_signature(signature: str) -> bool:
    try:
        return len(base64.b64decode(signature, validate=True)) == _ED25519_SIGNATURE_BYTES
    except (TypeError, ValueError):
        return False


def _valid_fingerprint(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


@lru_cache(maxsize=256)
def _validate_public_key(public_key: bytes) -> None:
    try:
        completed = subprocess.run(
            (
                _openssl_binary(),
                "pkey",
                "-pubin",
                "-pubcheck",
                "-text_pub",
                "-noout",
            ),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise ValueError("public key is not a usable Ed25519 key") from exc
    if completed.returncode != 0 or b"ED25519" not in completed.stdout.upper():
        raise ValueError("public key is not Ed25519")


@lru_cache(maxsize=256)
def ed25519_public_key_fingerprint(public_key: bytes) -> str:
    _validate_public_key(public_key)
    try:
        completed = subprocess.run(
            (_openssl_binary(), "pkey", "-pubin", "-outform", "DER"),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise ValueError("public key fingerprint cannot be derived") from exc
    if completed.returncode != 0 or not completed.stdout:
        raise ValueError("public key fingerprint cannot be derived")
    return hashlib.sha256(completed.stdout).hexdigest()


def _verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
    if not _valid_signature(signature):
        return False
    decoded = base64.b64decode(signature, validate=True)
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-source-auth-") as directory_name:
            root = Path(directory_name)
            root.chmod(0o700)
            public_path = root / "public.pem"
            payload_path = root / "payload.bin"
            signature_path = root / "signature.bin"
            public_path.write_bytes(public_key)
            payload_path.write_bytes(payload)
            signature_path.write_bytes(decoded)
            for path in (public_path, payload_path, signature_path):
                path.chmod(0o600)
            completed = subprocess.run(
                (
                    _openssl_binary(),
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(public_path),
                    "-sigfile",
                    str(signature_path),
                    "-rawin",
                    "-in",
                    str(payload_path),
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return False
    return completed.returncode == 0


class Ed25519SigningClient(Protocol):
    @property
    def key_purpose(self) -> KeyPurpose: ...

    @property
    def allowed_namespaces(self) -> frozenset[str]: ...

    @property
    def public_key_fingerprint(self) -> str: ...

    def sign(
        self,
        *,
        key_purpose: KeyPurpose,
        namespace: str,
        payload: bytes,
    ) -> str: ...


class Ed25519ContractSigner:
    """Opaque signing client binding; private key material never enters contract models."""

    def __init__(
        self,
        *,
        key_id: str,
        issuer: str,
        key_purpose: KeyPurpose,
        client: Ed25519SigningClient,
    ) -> None:
        self.key_id = key_id.strip()
        self.issuer = issuer.strip()
        self.key_purpose = key_purpose
        self._client = client
        if not self.key_id or not self.issuer:
            raise ValueError("Ed25519 signer identity must be nonempty")
        expected_namespaces = _PURPOSE_NAMESPACES[key_purpose]
        if client.key_purpose != key_purpose:
            raise ValueError("Ed25519 signing client purpose does not match signer")
        if client.allowed_namespaces != expected_namespaces:
            raise ValueError("Ed25519 signing client namespace binding is invalid")
        if not _valid_fingerprint(client.public_key_fingerprint):
            raise ValueError("Ed25519 signing client public key fingerprint is invalid")
        self.public_key_fingerprint = client.public_key_fingerprint

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if namespace not in self._client.allowed_namespaces:
            raise ValueError("namespace is not allowed for the signing client purpose")
        signature = self._client.sign(
            key_purpose=self.key_purpose,
            namespace=namespace,
            payload=_domain_payload(
                key_purpose=self.key_purpose,
                namespace=namespace,
                payload=payload,
            ),
        )
        if not _valid_signature(signature):
            raise ValueError("Ed25519 signing client returned an invalid signature")
        return signature


class Ed25519PublicKeyRecord(RuntimeContractModel):
    key_id: str = Field(min_length=1, max_length=200)
    issuer: str = Field(min_length=1, max_length=200)
    key_purpose: KeyPurpose
    rotation: RotationState
    public_key_pem: bytes = Field(min_length=1)

    @property
    def public_key_fingerprint(self) -> str:
        return ed25519_public_key_fingerprint(self.public_key_pem)


@dataclass(frozen=True)
class _VerifiedSignatureCacheKey:
    public_key_pem: bytes
    domain_payload: bytes
    signature_bytes: bytes
    issuer: str
    key_id: str
    key_purpose: KeyPurpose
    namespace: str
    rotation: RotationState
    allowed_issuers: frozenset[str]
    allowed_key_ids: frozenset[str]


@dataclass
class _VerificationFlight:
    ready: threading.Event = field(default_factory=threading.Event)
    result: bool | None = None
    error: BaseException | None = None


class VerifyOnlyEd25519Keyring:
    """Public-only verifier with explicit issuer, purpose, and rotation allowlists."""

    def __init__(
        self,
        *,
        records: tuple[Ed25519PublicKeyRecord, ...],
        issuer_allowlist: Mapping[KeyPurpose, frozenset[str]],
        rotation_allowlist: Mapping[tuple[str, KeyPurpose], frozenset[str]],
    ) -> None:
        if not records:
            raise ValueError("Ed25519 keyring requires public key records")
        if len({record.key_id for record in records}) != len(records):
            raise ValueError("Ed25519 key ids must be globally unique")
        fingerprint_purposes: dict[str, KeyPurpose] = {}
        for record in records:
            _validate_public_key(record.public_key_pem)
            fingerprint = record.public_key_fingerprint
            previous_purpose = fingerprint_purposes.setdefault(fingerprint, record.key_purpose)
            if previous_purpose != record.key_purpose:
                raise ValueError("Ed25519 public key fingerprint cannot overlap signing roles")
        self._records = MappingProxyType(
            {record.key_id: record.model_copy(deep=True) for record in records}
        )
        self._issuer_allowlist = MappingProxyType(
            {purpose: frozenset(issuers) for purpose, issuers in issuer_allowlist.items()}
        )
        self._rotation_allowlist = MappingProxyType(
            {
                (issuer, purpose): frozenset(key_ids)
                for (issuer, purpose), key_ids in rotation_allowlist.items()
            }
        )
        self._verified_signatures: OrderedDict[_VerifiedSignatureCacheKey, None] = OrderedDict()
        self._inflight_verifications: dict[_VerifiedSignatureCacheKey, _VerificationFlight] = {}
        self._verification_lock = threading.Lock()

    def fingerprints_for_purpose(self, key_purpose: KeyPurpose) -> frozenset[str]:
        issuers = self._issuer_allowlist.get(key_purpose, frozenset())
        return frozenset(
            record.public_key_fingerprint
            for record in self._records.values()
            if record.key_purpose == key_purpose
            and record.issuer in issuers
            and record.key_id
            in self._rotation_allowlist.get(
                (record.issuer, key_purpose),
                frozenset(),
            )
        )

    def allows_signer(self, signer: Ed25519ContractSigner) -> bool:
        record = self._records.get(signer.key_id)
        return bool(
            record is not None
            and record.issuer == signer.issuer
            and record.key_purpose == signer.key_purpose
            and record.public_key_fingerprint == signer.public_key_fingerprint
            and signer.public_key_fingerprint in self.fingerprints_for_purpose(signer.key_purpose)
        )

    def verify(
        self,
        *,
        issuer: str,
        key_id: str,
        key_purpose: KeyPurpose,
        namespace: str,
        payload: bytes,
        signature: str,
    ) -> bool:
        verification = self._verification_state(
            issuer=issuer,
            key_id=key_id,
            key_purpose=key_purpose,
            namespace=namespace,
            payload=payload,
            signature=signature,
        )
        if verification is None:
            return False

        cache_key, public_key, domain_payload = verification
        with self._verification_lock:
            if cache_key in self._verified_signatures:
                self._verified_signatures.move_to_end(cache_key)
                return True
            flight = self._inflight_verifications.get(cache_key)
            if flight is None:
                flight = _VerificationFlight()
                self._inflight_verifications[cache_key] = flight
                leader = True
            else:
                leader = False

        if not leader:
            flight.ready.wait()
            if flight.error is not None:
                raise flight.error
            return bool(flight.result)

        try:
            verified = _verify_signature(
                public_key=public_key,
                payload=domain_payload,
                signature=signature,
            )
        except BaseException as exc:
            with self._verification_lock:
                self._inflight_verifications.pop(cache_key, None)
                flight.error = exc
                flight.ready.set()
            raise

        with self._verification_lock:
            self._inflight_verifications.pop(cache_key, None)
            flight.result = verified
            if verified:
                self._verified_signatures[cache_key] = None
                self._verified_signatures.move_to_end(cache_key)
                while len(self._verified_signatures) > _VERIFY_ONLY_KEYRING_CACHE_SIZE:
                    self._verified_signatures.popitem(last=False)
            flight.ready.set()
        return verified

    def _verification_state(
        self,
        *,
        issuer: str,
        key_id: str,
        key_purpose: KeyPurpose,
        namespace: str,
        payload: bytes,
        signature: str,
    ) -> tuple[_VerifiedSignatureCacheKey, bytes, bytes] | None:
        record = self._records.get(key_id)
        if record is None:
            return None
        if record.issuer != issuer or record.key_purpose != key_purpose:
            return None
        allowed_issuers = self._issuer_allowlist.get(key_purpose, frozenset())
        allowed_keys = self._rotation_allowlist.get((issuer, key_purpose), frozenset())
        if issuer not in allowed_issuers or key_id not in allowed_keys:
            return None
        signature_bytes = _decode_signature_bytes(signature)
        if signature_bytes is None:
            return None
        domain_payload = _domain_payload(
            key_purpose=key_purpose,
            namespace=namespace,
            payload=payload,
        )
        return (
            _VerifiedSignatureCacheKey(
                public_key_pem=record.public_key_pem,
                domain_payload=domain_payload,
                signature_bytes=signature_bytes,
                issuer=issuer,
                key_id=key_id,
                key_purpose=key_purpose,
                namespace=namespace,
                rotation=record.rotation,
                allowed_issuers=allowed_issuers,
                allowed_key_ids=allowed_keys,
            ),
            record.public_key_pem,
            domain_payload,
        )


def _decode_signature_bytes(signature: str) -> bytes | None:
    try:
        decoded = base64.b64decode(signature, validate=True)
    except (TypeError, ValueError):
        return None
    if len(decoded) != _ED25519_SIGNATURE_BYTES:
        return None
    return decoded


class PydanticModelSchema(RuntimeContractModel):
    model_name: str = Field(min_length=1, max_length=500)
    schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def from_model(cls, model: type[BaseModel]) -> PydanticModelSchema:
        return cls(
            model_name=f"{model.__module__}.{model.__qualname__}",
            schema_hash=canonical_sha256(model.model_json_schema()),
        )


class _Ed25519SignedContract(RuntimeContractModel):
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"signature"})

    def signing_bytes(self) -> bytes:
        return _canonical_json_bytes(self.signing_payload())

    def _verify(
        self,
        keyring: VerifyOnlyEd25519Keyring,
        *,
        purpose: KeyPurpose,
        namespace: str,
    ) -> bool:
        return _valid_signature(self.signature) and keyring.verify(
            issuer=self.issuer,
            key_id=self.key_id,
            key_purpose=purpose,
            namespace=namespace,
            payload=self.signing_bytes(),
            signature=self.signature,
        )


class AdapterManifest(_Ed25519SignedContract):
    key_purpose: Literal["adapter_manifest"] = "adapter_manifest"
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    adapter_code_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    network: NetworkMode
    source: str | None = Field(default=None, min_length=1, max_length=200)
    operation: str | None = Field(default=None, min_length=1, max_length=200)
    cost_per_call: int = Field(strict=True, ge=0)
    max_calls: int = Field(strict=True, ge=0)
    request_schema: PydanticModelSchema
    response_schema: PydanticModelSchema
    temporal_policy: AdapterManifestTemporalPolicyV2 | None = None

    @model_validator(mode="after")
    def validate_network_contract(self) -> Self:
        if self.network == "provider":
            if self.source is None or self.operation is None:
                raise ValueError("provider manifest requires source and operation")
            if self.cost_per_call < 1 or self.max_calls < 1:
                raise ValueError("provider manifest requires positive cost and max_calls")
        elif (
            any(value is not None for value in (self.source, self.operation))
            or self.cost_per_call != 0
            or self.max_calls != 0
        ):
            raise ValueError("network none manifest cannot declare provider use")
        return self

    @property
    def manifest_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"signature"}))

    def verify(self, keyring: VerifyOnlyEd25519Keyring) -> bool:
        return self._verify(
            keyring,
            purpose="adapter_manifest",
            namespace=ADAPTER_MANIFEST_NAMESPACE,
        )

    def require_verified(self, keyring: VerifyOnlyEd25519Keyring) -> None:
        if not self.verify(keyring):
            raise ManifestSignatureError("adapter manifest signature is invalid")


class AdapterManifestTemporalPolicyV2(RuntimeContractModel):
    """Signed validity and point-in-time availability bound for a provider manifest."""

    schema_version: Literal[2] = 2
    valid_from: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    availability_lag_seconds: int = Field(strict=True, ge=0, le=31_536_000)

    @model_validator(mode="after")
    def validate_temporal_policy(self) -> Self:
        if self.expires_at <= self.valid_from:
            raise ValueError("manifest temporal policy expires_at must follow valid_from")
        return self

    def latest_available_at(self, now: datetime) -> datetime:
        return now.astimezone(UTC) - timedelta(seconds=self.availability_lag_seconds)


class SourceUsePlan(_Ed25519SignedContract):
    key_purpose: Literal["source_use_plan"] = "source_use_plan"
    audience: str = Field(min_length=1, max_length=200)
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    nonce: str = Field(min_length=1, max_length=500)
    single_use_authority_id: str = Field(min_length=1, max_length=200)
    claim_token: str = Field(min_length=1, max_length=500)
    manifest: AdapterManifest
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    adapter_code_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    network: NetworkMode
    source: str | None = Field(default=None, min_length=1, max_length=200)
    operation: str | None = Field(default=None, min_length=1, max_length=200)
    cost_per_call: int = Field(strict=True, ge=0)
    max_calls: int = Field(strict=True, ge=0)
    request_schema: PydanticModelSchema
    response_schema: PydanticModelSchema

    @classmethod
    def from_manifest(
        cls,
        manifest: AdapterManifest,
        *,
        issuer: str,
        key_id: str,
        claim_token: str,
        audience: str,
        not_before: datetime,
        expires_at: datetime,
        nonce: str,
        single_use_authority_id: str,
    ) -> SourceUsePlan:
        return cls(
            issuer=issuer,
            key_id=key_id,
            signature="",
            audience=audience,
            not_before=not_before,
            expires_at=expires_at,
            nonce=nonce,
            single_use_authority_id=single_use_authority_id,
            claim_token=claim_token,
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            adapter_id=manifest.adapter_id,
            adapter_version=manifest.adapter_version,
            adapter_code_hash=manifest.adapter_code_hash,
            network=manifest.network,
            source=manifest.source,
            operation=manifest.operation,
            cost_per_call=manifest.cost_per_call,
            max_calls=manifest.max_calls,
            request_schema=manifest.request_schema,
            response_schema=manifest.response_schema,
        )

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        if self.expires_at <= self.not_before:
            raise ValueError("expires_at must follow not_before")
        manifest = self.manifest
        if self.manifest_hash != manifest.manifest_hash:
            raise ValueError("source plan manifest hash does not match manifest content")
        bound_fields = (
            "adapter_id",
            "adapter_version",
            "adapter_code_hash",
            "network",
            "source",
            "operation",
            "cost_per_call",
            "request_schema",
            "response_schema",
        )
        if any(getattr(self, name) != getattr(manifest, name) for name in bound_fields):
            raise ValueError("source plan conflicts with manifest")
        if self.max_calls > manifest.max_calls:
            raise ValueError("source plan max_calls exceeds manifest")
        return self

    @property
    def plan_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"signature"}))

    def verify(self, keyring: VerifyOnlyEd25519Keyring) -> bool:
        return self._verify(
            keyring,
            purpose="source_use_plan",
            namespace=SOURCE_USE_PLAN_NAMESPACE,
        ) and self.manifest.verify(keyring)

    def require_verified(self, keyring: VerifyOnlyEd25519Keyring) -> None:
        if not self.verify(keyring):
            raise ManifestSignatureError("source use plan signature is invalid")
