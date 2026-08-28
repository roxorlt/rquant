"""Production composition for a formal immutable runtime capability."""

from __future__ import annotations

import math
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, ValidationError, field_validator, model_validator

from rquant.adapter_manifest import Ed25519PublicKeyRecord, VerifyOnlyEd25519Keyring
from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    open_secure_regular_file_lease,
)
from rquant.external_monotonic_root import (
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.runtime_code_attestation import (
    RUNTIME_CODE_PROMOTION_KEY_PURPOSE,
    RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
    RUNTIME_CODE_PROMOTION_ROLE,
    RuntimeCodePromotionTrustBoundary,
)
from rquant.runtime_code_generation import (
    RuntimeCodeGenerationCapability,
    RuntimeCodeGenerationError,
    open_attested_runtime_generation,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import StrictJsonError, strict_model_validate_canonical_json

_MAX_CONFIGURATION_BYTES = 1024 * 1024


class FormalRuntimeCompositionError(RuntimeError):
    """The immutable runtime bootstrap configuration cannot be trusted."""


class FormalRuntimeBootstrapConfiguration(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-runtime-bootstrap/v1"] = "rquant-formal-runtime-bootstrap/v1"
    runtime_root: Path
    trusted_base: Path
    expected_material_uid: int = Field(strict=True, ge=0)
    expected_material_gid: int = Field(strict=True, ge=0)
    expected_audience: str = Field(min_length=1, max_length=200)
    expected_installation_id: str = Field(min_length=1, max_length=200)
    expected_target_platform: str = Field(min_length=1, max_length=200)
    expected_python_abi: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    root_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1, max_length=4)
    runtime_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1, max_length=4)
    promotion_key: Ed25519PublicKeyRecord
    promotion_config: ExternalMonotonicRootConfig
    promotion_transport: UnixSocketExternalMonotonicRootManifest
    promotion_subject_authority_id: str = Field(min_length=1, max_length=200)

    @field_validator("runtime_root", "trusted_base", mode="after")
    @classmethod
    def validate_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or Path(value.absolute()) != value:
            raise ValueError("formal runtime bootstrap paths must be canonical absolute")
        return value

    @model_validator(mode="after")
    def validate_trust_roles(self) -> Self:
        try:
            self.runtime_root.relative_to(self.trusted_base)
        except ValueError as exc:
            raise ValueError("formal runtime root escapes trusted base") from exc
        if any(record.key_purpose != "rquant_runtime_code_root" for record in self.root_keys):
            raise ValueError("formal runtime root key role is invalid")
        if any(record.key_purpose != "rquant_runtime_code_signer" for record in self.runtime_keys):
            raise ValueError("formal runtime signer key role is invalid")
        if self.promotion_key.key_purpose != RUNTIME_CODE_PROMOTION_KEY_PURPOSE:
            raise ValueError("formal runtime promotion key role is invalid")
        promotion_identity = (
            self.promotion_config.transport,
            self.promotion_config.transport_manifest_hash,
            self.promotion_config.role,
            self.promotion_config.root_authority_id,
            self.promotion_config.root_store_id,
            self.promotion_config.root_issuer,
            self.promotion_config.root_key_id,
            self.promotion_config.root_key_purpose,
            self.promotion_config.root_receipt_namespace,
            self.promotion_config.root_public_key_fingerprint,
            self.promotion_config.witness_rollback_domain_id,
        )
        expected_identity = (
            "unix-socket-v1",
            self.promotion_transport.manifest_hash,
            RUNTIME_CODE_PROMOTION_ROLE,
            self.promotion_transport.authority_id,
            self.promotion_transport.store_id,
            self.promotion_key.issuer,
            self.promotion_key.key_id,
            RUNTIME_CODE_PROMOTION_KEY_PURPOSE,
            RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
            self.promotion_key.public_key_fingerprint,
            self.promotion_transport.rollback_domain_id,
        )
        if promotion_identity != expected_identity:
            raise ValueError("formal runtime promotion trust binding is inconsistent")
        return self


def _keyring(records: tuple[Ed25519PublicKeyRecord, ...]) -> VerifyOnlyEd25519Keyring:
    purposes = {record.key_purpose for record in records}
    issuers = {
        purpose: frozenset(record.issuer for record in records if record.key_purpose == purpose)
        for purpose in purposes
    }
    rotation = {
        (issuer, purpose): frozenset(
            record.key_id
            for record in records
            if record.key_purpose == purpose and record.issuer == issuer
        )
        for purpose in purposes
        for issuer in issuers[purpose]
    }
    return VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist=issuers,
        rotation_allowlist=rotation,
    )


class _ExternalRootKeyringVerifier:
    signature_algorithm: Literal["ed25519"] = "ed25519"

    def __init__(self, record: Ed25519PublicKeyRecord) -> None:
        self.issuer = record.issuer
        self.key_id = record.key_id
        self.key_purpose = record.key_purpose
        self.public_key_fingerprint = record.public_key_fingerprint
        self._keyring = _keyring((record,))

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        return self._keyring.verify(
            issuer=self.issuer,
            key_id=self.key_id,
            key_purpose=self.key_purpose,
            namespace=namespace,
            payload=payload,
            signature=signature,
        )


class _PromotionCurrentReader:
    def __init__(
        self,
        *,
        trust: ExternalMonotonicRootTrustBoundary,
        configuration: FormalRuntimeBootstrapConfiguration,
    ) -> None:
        self._trust = trust
        self._configuration = configuration

    def __call__(self, installation_id: str, target_platform: str) -> bytes | None:
        configuration = self._configuration
        if (
            installation_id != configuration.expected_installation_id
            or target_platform != configuration.expected_target_platform
        ):
            raise FormalRuntimeCompositionError(
                "formal runtime promotion subject does not match bootstrap configuration"
            )
        config = configuration.promotion_config
        response = self._trust.invoke(
            ExternalMonotonicRootRequest.close(
                kind="current",
                role=config.role,
                root_authority_id=config.root_authority_id,
                root_store_id=config.root_store_id,
                subject_authority_id=configuration.promotion_subject_authority_id,
                challenge_nonce=secrets.token_hex(32),
            )
        )
        return None if response is None else response.encode("utf-8")


def open_formal_runtime_capability(
    *,
    configuration_path: Path,
    trusted_base: Path,
    expected_authority_uid: int,
    expected_authority_gid: int,
    startup_deadline_monotonic: float,
) -> RuntimeCodeGenerationCapability:
    """Load root-protected public trust material and open one formal capability."""

    try:
        if (
            not math.isfinite(startup_deadline_monotonic)
            or time.monotonic() >= startup_deadline_monotonic
        ):
            raise FormalRuntimeCompositionError("formal runtime startup deadline expired")
        with open_secure_regular_file_lease(
            configuration_path,
            trusted_root=trusted_base,
            expected_uid=expected_authority_uid,
            expected_gid=expected_authority_gid,
            allowed_modes=frozenset({0o400, 0o440, 0o444}),
            max_bytes=_MAX_CONFIGURATION_BYTES,
        ) as configuration_lease:
            configuration = strict_model_validate_canonical_json(
                FormalRuntimeBootstrapConfiguration,
                configuration_lease.read_all(max_bytes=_MAX_CONFIGURATION_BYTES),
            )
        if configuration.trusted_base != trusted_base:
            raise FormalRuntimeCompositionError(
                "formal runtime trusted base does not match bootstrap configuration"
            )
        root_keyring = _keyring(configuration.root_keys)
        runtime_keyring = _keyring(configuration.runtime_keys)
        client = UnixSocketExternalMonotonicRootClient(configuration.promotion_transport)
        external_trust = ExternalMonotonicRootTrustBoundary(
            config=configuration.promotion_config,
            client=client,
            root_verifiers=(_ExternalRootKeyringVerifier(configuration.promotion_key),),
        )
        promotion_trust = RuntimeCodePromotionTrustBoundary(
            trust=external_trust,
            current_reader=_PromotionCurrentReader(
                trust=external_trust,
                configuration=configuration,
            ),
        )
        capability = open_attested_runtime_generation(
            runtime_root=configuration.runtime_root,
            trusted_base=configuration.trusted_base,
            root_keyring=root_keyring,
            runtime_keyring=runtime_keyring,
            promotion_trust=promotion_trust,
            expected_uid=configuration.expected_material_uid,
            expected_gid=configuration.expected_material_gid,
            expected_audience=configuration.expected_audience,
            expected_installation_id=configuration.expected_installation_id,
            expected_target_platform=configuration.expected_target_platform,
            now=datetime.now(UTC),
        )
        if (
            capability.loaded.attestation.execution_spec.python_abi
            != configuration.expected_python_abi
        ):
            capability.close()
            raise FormalRuntimeCompositionError("formal runtime Python ABI binding is invalid")
        if time.monotonic() >= startup_deadline_monotonic:
            capability.close()
            raise FormalRuntimeCompositionError("formal runtime startup deadline expired")
        return capability
    except FormalRuntimeCompositionError:
        raise
    except (
        AuthorityPathSecurityError,
        ExternalMonotonicRootSecurityError,
        OSError,
        RuntimeCodeGenerationError,
        StrictJsonError,
        ValidationError,
        ValueError,
    ) as exc:
        raise FormalRuntimeCompositionError(
            "formal runtime bootstrap configuration or generation is invalid"
        ) from exc


__all__ = [
    "FormalRuntimeBootstrapConfiguration",
    "FormalRuntimeCompositionError",
    "open_formal_runtime_capability",
]
