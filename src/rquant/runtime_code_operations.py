"""Offline packaging and privileged operations for formal runtime generations."""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
import shutil
import subprocess
import tempfile
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal, Self

from pydantic import Field, ValidationError, field_validator, model_validator

from rquant.adapter_manifest import (
    RUNTIME_CODE_ATTESTATION_NAMESPACE,
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    KeyPurpose,
    VerifyOnlyEd25519Keyring,
    ed25519_public_key_fingerprint,
)
from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    open_secure_regular_file_lease,
)
from rquant.external_monotonic_root import (
    ExternalMonotonicRootClient,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.formal_runtime_composition import FormalRuntimeBootstrapConfiguration
from rquant.runtime_code_attestation import (
    RUNTIME_CODE_PROMOTION_KEY_PURPOSE,
    RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
    RUNTIME_CODE_PROMOTION_ROLE,
    RuntimeCodeAttestation,
    RuntimeCodeExecutionSpec,
    RuntimeCodePromotionReceipt,
    RuntimeCodePromotionTrust,
    RuntimeCodePromotionTrustBoundary,
    RuntimeCodeTrustError,
    VerifiedRuntimeCodeAttestation,
    compute_runtime_code_generation_id,
    require_runtime_code_attestation,
    require_runtime_code_promotion_receipt,
    sign_runtime_code_attestation,
)
from rquant.runtime_code_generation import (
    LoadedRuntimeCodeGeneration,
    RuntimeCodeCollectFile,
    RuntimeCodeGenerationError,
    RuntimeCodeGenerationInstaller,
    RuntimeCodeInstallReceipt,
    RuntimeCodeInstallRequest,
    collect_runtime_code_bundle,
    require_attested_runtime_generation,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

RUNTIME_CODE_EXIT_OK: Final = 0
RUNTIME_CODE_EXIT_INVALID: Final = 2
RUNTIME_CODE_EXIT_CONFLICT: Final = 3
RUNTIME_CODE_EXIT_UNAVAILABLE: Final = 4
_ZERO_HASH: Final = "0" * 64
_MAX_AUTHORITY_BYTES: Final = 16 * 1024 * 1024
_PACKAGE_NAMES: Final = (
    "runtime-code.bundle",
    "runtime-code-attestation.json",
    "runtime-code-certificate.json",
    "runtime-code-promotion-receipt.json",
)
_PACKAGE_MODES: Final = frozenset({0o400, 0o440, 0o444, 0o600, 0o640, 0o644})
_FORMAL_COMMANDS: Final = frozenset(
    {
        "lab-claim-finalizer",
        "lab-finalizer",
        "lab-runtime-prepare",
        "lab-scheduler",
        "lab-worker",
    }
)
_LEGACY_ARGUMENTS: Final = frozenset(
    {
        "--checkout-root",
        "--expected-checkout-root",
        "--expected-code-root",
        "--release-managed-checkout",
        "--trusted-git-path",
    }
)


class RuntimeCodeOperationError(RuntimeError):
    """Safe operator-facing failure with a stable process exit code."""

    def __init__(self, message: str, *, exit_code: int = RUNTIME_CODE_EXIT_INVALID) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def _absolute_path(value: Path) -> Path:
    if not value.is_absolute() or Path(os.path.abspath(value)) != value:
        raise ValueError("runtime code operation paths must be canonical absolute")
    return value


class RuntimeCodePackageRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-package-request/v1"] = (
        "rquant-runtime-code-package-request/v1"
    )
    checkout_root: Path
    output_root: Path
    certificate_path: Path
    files: tuple[RuntimeCodeCollectFile, ...] = Field(min_length=1)
    execution_spec: RuntimeCodeExecutionSpec
    audience: str = Field(min_length=1, max_length=200)
    installation_id: str = Field(min_length=1, max_length=200)
    target_platform: str = Field(min_length=1, max_length=200)
    provenance_commit: str = Field(pattern=r"^[0-9a-f]{40}([0-9a-f]{24})?$")
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    promotion_sequence: int = Field(strict=True, ge=1)
    previous_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    now: AwareUtcDatetime
    expected_source_uid: int = Field(strict=True, ge=0)
    expected_source_gid: int = Field(strict=True, ge=0)

    @field_validator("checkout_root", "output_root", "certificate_path", mode="after")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _absolute_path(value)

    @model_validator(mode="after")
    def validate_lifetime(self) -> Self:
        if self.not_before >= self.expires_at:
            raise ValueError("runtime code package lifetime is empty")
        return self


class RuntimeCodePackageCeremonyRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-package-ceremony/v1"] = (
        "rquant-runtime-code-package-ceremony/v1"
    )
    package: RuntimeCodePackageRequest
    runtime_key_id: str = Field(min_length=1, max_length=200)
    runtime_private_key_path: Path
    promotion_private_key_path: Path

    @field_validator("runtime_private_key_path", "promotion_private_key_path", mode="after")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _absolute_path(value)


class RuntimeCodeRotateRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-rotate-request/v1"] = (
        "rquant-runtime-code-rotate-request/v1"
    )
    retained_package_root: Path
    output_root: Path
    promotion_sequence: int = Field(strict=True, ge=1)
    expected_audience: str = Field(min_length=1, max_length=200)
    expected_installation_id: str = Field(min_length=1, max_length=200)
    expected_target_platform: str = Field(min_length=1, max_length=200)
    now: AwareUtcDatetime

    @field_validator("retained_package_root", "output_root", mode="after")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _absolute_path(value)


class RuntimeCodeRotateCeremonyRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-rotate-ceremony/v1"] = (
        "rquant-runtime-code-rotate-ceremony/v1"
    )
    rotation: RuntimeCodeRotateRequest
    promotion_private_key_path: Path

    @field_validator("promotion_private_key_path", mode="after")
    @classmethod
    def validate_path(cls, value: Path) -> Path:
        return _absolute_path(value)


class RuntimeCodeFormalService(RuntimeContractModel):
    command: Literal[
        "lab-claim-finalizer",
        "lab-finalizer",
        "lab-runtime-prepare",
        "lab-scheduler",
        "lab-worker",
    ]
    #: Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-02. This carried a
    #: `wrapper_path` naming `scripts/run-lab-daemon.py` so the migration preflight could
    #: check the checkout wrapper's call graph. The unit no longer executes that script, so
    #: there is no second artifact to inspect and no field to name it with — the service is
    #: its unit, and the argv behind it comes from the root-owned role policy.
    unit_path: Path

    @field_validator("unit_path", mode="after")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _absolute_path(value)


class RuntimeCodeMigrationRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-migration-request/v1"] = (
        "rquant-runtime-code-migration-request/v1"
    )
    install: RuntimeCodeInstallRequest
    formal_services: tuple[RuntimeCodeFormalService, ...] = Field(min_length=1)
    expected_configuration_path: Path
    expected_trusted_base: Path
    expected_authority_uid: int = Field(strict=True, ge=0)
    expected_authority_gid: int = Field(strict=True, ge=0)
    legacy_paths: tuple[Path, ...] = ()

    @field_validator(
        "expected_configuration_path",
        "expected_trusted_base",
        "legacy_paths",
        mode="after",
    )
    @classmethod
    def validate_paths(cls, value: Path | tuple[Path, ...]) -> Path | tuple[Path, ...]:
        if isinstance(value, tuple):
            return tuple(_absolute_path(path) for path in value)
        return _absolute_path(value)


class RuntimeCodePackageResult(RuntimeContractModel):
    status: Literal["packaged", "rotated"]
    output_root: Path
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    attestation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bundle_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_root_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    promotion_sequence: int = Field(strict=True, ge=1)
    previous_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    external_promotion_required: Literal[True] = True


class RuntimeCodeInstallPlan(RuntimeContractModel):
    status: Literal["ready"] = "ready"
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_generation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    promotion_sequence: int = Field(strict=True, ge=1)
    write_performed: Literal[False] = False
    checks: tuple[str, ...]


class RuntimeCodeInspectResult(RuntimeContractModel):
    status: Literal["verified"] = "verified"
    runtime_root: Path
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    promotion_sequence: int = Field(strict=True, ge=1)
    provenance_commit: str = Field(pattern=r"^[0-9a-f]{40}([0-9a-f]{24})?$")
    attestation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_root_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class RuntimeCodeInstallResult(RuntimeContractModel):
    status: Literal["installed"] = "installed"
    receipt: RuntimeCodeInstallReceipt
    checks: tuple[str, ...]


class _ExternalRootVerifier:
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
        if (
            installation_id != self._configuration.expected_installation_id
            or target_platform != self._configuration.expected_target_platform
        ):
            raise RuntimeCodeOperationError("runtime code promotion subject is invalid")
        config = self._configuration.promotion_config
        response = self._trust.invoke(
            ExternalMonotonicRootRequest.close(
                kind="current",
                role=config.role,
                root_authority_id=config.root_authority_id,
                root_store_id=config.root_store_id,
                subject_authority_id=self._configuration.promotion_subject_authority_id,
                challenge_nonce=secrets.token_hex(32),
            )
        )
        return None if response is None else response.encode("utf-8")


class _OfflinePromotionClient:
    """Identity-only client for ceremonies that must not contact the live root."""

    def __init__(self, manifest: UnixSocketExternalMonotonicRootManifest) -> None:
        self.role = manifest.role
        self.authority_id = manifest.authority_id
        self.store_id = manifest.store_id
        self.transport = manifest.transport
        self.manifest_hash = manifest.manifest_hash
        self.rollback_domain_id = manifest.rollback_domain_id

    def invoke(self, *, request_json: str) -> str | None:
        del request_json
        raise RuntimeCodeOperationError(
            "offline runtime code ceremony cannot query or mutate the promotion root",
            exit_code=RUNTIME_CODE_EXIT_UNAVAILABLE,
        )


def _keyring(records: tuple[Ed25519PublicKeyRecord, ...]) -> VerifyOnlyEd25519Keyring:
    purposes = {record.key_purpose for record in records}
    issuers = {
        purpose: frozenset(record.issuer for record in records if record.key_purpose == purpose)
        for purpose in purposes
    }
    rotations = {
        (issuer, purpose): frozenset(
            record.key_id
            for record in records
            if record.issuer == issuer and record.key_purpose == purpose
        )
        for purpose, purpose_issuers in issuers.items()
        for issuer in purpose_issuers
    }
    return VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist=issuers,
        rotation_allowlist=rotations,
    )


def load_runtime_code_bootstrap_configuration(
    path: Path,
    *,
    trusted_base: Path,
    expected_uid: int,
    expected_gid: int,
) -> FormalRuntimeBootstrapConfiguration:
    try:
        with open_secure_regular_file_lease(
            path,
            trusted_root=trusted_base,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            allowed_modes=frozenset({0o400, 0o440, 0o444}),
            max_bytes=1024 * 1024,
        ) as lease:
            configuration = strict_model_validate_canonical_json(
                FormalRuntimeBootstrapConfiguration,
                lease.read_all(max_bytes=1024 * 1024),
            )
        if configuration.trusted_base != trusted_base:
            raise RuntimeCodeOperationError(
                "runtime code trusted base does not match configuration"
            )
        return configuration
    except RuntimeCodeOperationError:
        raise
    except (
        AuthorityPathSecurityError,
        OSError,
        StrictJsonError,
        ValidationError,
        ValueError,
    ) as exc:
        raise RuntimeCodeOperationError("runtime code bootstrap configuration is invalid") from exc


def compose_runtime_code_generation_operator(
    configuration: FormalRuntimeBootstrapConfiguration,
    *,
    promotion_client: ExternalMonotonicRootClient | None = None,
    offline: bool = False,
) -> RuntimeCodeGenerationOperator:
    if promotion_client is not None and offline:
        raise RuntimeCodeOperationError("runtime code operator mode is ambiguous")
    client = promotion_client
    if client is None:
        client = (
            _OfflinePromotionClient(configuration.promotion_transport)
            if offline
            else UnixSocketExternalMonotonicRootClient(configuration.promotion_transport)
        )
    external_trust = ExternalMonotonicRootTrustBoundary(
        config=configuration.promotion_config,
        client=client,
        root_verifiers=(_ExternalRootVerifier(configuration.promotion_key),),
    )
    promotion_trust = RuntimeCodePromotionTrustBoundary(
        trust=external_trust,
        current_reader=_PromotionCurrentReader(
            trust=external_trust,
            configuration=configuration,
        ),
    )
    return RuntimeCodeGenerationOperator(
        runtime_root=configuration.runtime_root,
        trusted_base=configuration.trusted_base,
        root_keyring=_keyring(configuration.root_keys),
        runtime_keyring=_keyring(configuration.runtime_keys),
        promotion_trust=promotion_trust,
        expected_uid=configuration.expected_material_uid,
        expected_gid=configuration.expected_material_gid,
        expected_audience=configuration.expected_audience,
        expected_installation_id=configuration.expected_installation_id,
        expected_target_platform=configuration.expected_target_platform,
    )


class RuntimeCodeGenerationOperator:
    """Explicit operator boundary around the frozen generation trust core."""

    def __init__(
        self,
        *,
        runtime_root: Path,
        trusted_base: Path,
        root_keyring: VerifyOnlyEd25519Keyring,
        runtime_keyring: VerifyOnlyEd25519Keyring,
        promotion_trust: RuntimeCodePromotionTrust,
        expected_uid: int,
        expected_gid: int,
        expected_audience: str,
        expected_installation_id: str,
        expected_target_platform: str,
    ) -> None:
        self.runtime_root = _absolute_path(runtime_root)
        self.trusted_base = _absolute_path(trusted_base)
        self._root_keyring = root_keyring
        self._runtime_keyring = runtime_keyring
        self._promotion_trust = promotion_trust
        self._expected_uid = expected_uid
        self._expected_gid = expected_gid
        self._expected_audience = expected_audience
        self._expected_installation_id = expected_installation_id
        self._expected_target_platform = expected_target_platform

    def package(
        self,
        request: RuntimeCodePackageRequest,
        *,
        runtime_signer: Ed25519ContractSigner,
        promotion_signer: Ed25519ContractSigner,
    ) -> RuntimeCodePackageResult:
        try:
            self._require_subject(
                audience=request.audience,
                installation_id=request.installation_id,
                target_platform=request.target_platform,
            )
            current = self._current(now=request.now, required=False)
            minimum_sequence, expected_previous = _next_promotion(current)
            if (
                request.promotion_sequence < minimum_sequence
                or request.previous_receipt_sha256 != expected_previous
            ):
                raise RuntimeCodeOperationError(
                    "runtime code package promotion predecessor is stale",
                    exit_code=RUNTIME_CODE_EXIT_CONFLICT,
                )
            bundle = collect_runtime_code_bundle(
                request.checkout_root,
                request.files,
                expected_uid=request.expected_source_uid,
                expected_gid=request.expected_source_gid,
            )
            certificate_bytes = _read_secure_file(
                request.certificate_path,
                trusted_root=request.certificate_path.parent,
                expected_uid=request.expected_source_uid,
                expected_gid=request.expected_source_gid,
                max_bytes=_MAX_AUTHORITY_BYTES,
            )
            attestation = sign_runtime_code_attestation(
                signer=runtime_signer,
                bundle=bundle,
                execution_spec=request.execution_spec,
                audience=request.audience,
                installation_id=request.installation_id,
                target_platform=request.target_platform,
                provenance_commit=request.provenance_commit,
                not_before=request.not_before,
                expires_at=request.expires_at,
            )
            attestation_bytes = canonical_model_json_bytes(attestation)
            verified = require_runtime_code_attestation(
                attestation_bytes=attestation_bytes,
                certificate_bytes=certificate_bytes,
                bundle_bytes=bundle.bundle_bytes,
                root_keyring=self._root_keyring,
                runtime_keyring=self._runtime_keyring,
                expected_audience=request.audience,
                expected_installation_id=request.installation_id,
                expected_target_platform=request.target_platform,
                now=request.now,
            )
            receipt = _sign_promotion_receipt(
                promotion_signer=promotion_signer,
                promotion_trust=self._promotion_trust,
                attestation_bytes=attestation_bytes,
                bundle_sha256=verified.bundle.bundle_sha256,
                content_root_sha256=verified.bundle.content_root_sha256,
                installation_id=request.installation_id,
                target_platform=request.target_platform,
                promotion_sequence=request.promotion_sequence,
                previous_receipt_sha256=request.previous_receipt_sha256,
            )
            payloads = {
                "runtime-code.bundle": bundle.bundle_bytes,
                "runtime-code-attestation.json": attestation_bytes,
                "runtime-code-certificate.json": certificate_bytes,
                "runtime-code-promotion-receipt.json": canonical_model_json_bytes(receipt),
            }
            _publish_package(request.output_root, payloads)
            return _package_result(
                status="packaged",
                output_root=request.output_root,
                attestation=attestation,
                attestation_bytes=attestation_bytes,
                receipt=receipt,
            )
        except RuntimeCodeOperationError:
            raise
        except (
            AuthorityPathSecurityError,
            OSError,
            RuntimeCodeGenerationError,
            RuntimeCodeTrustError,
            StrictJsonError,
            ValidationError,
            ValueError,
        ) as exc:
            raise RuntimeCodeOperationError("runtime code package is invalid") from exc

    def rotate(
        self,
        request: RuntimeCodeRotateRequest,
        *,
        promotion_signer: Ed25519ContractSigner,
    ) -> RuntimeCodePackageResult:
        try:
            self._require_subject(
                audience=request.expected_audience,
                installation_id=request.expected_installation_id,
                target_platform=request.expected_target_platform,
            )
            current = self._current(now=request.now, required=True)
            assert current is not None
            if request.promotion_sequence <= current.promotion_receipt.promotion_sequence:
                raise RuntimeCodeOperationError(
                    "runtime code rollback requires a higher promotion sequence",
                    exit_code=RUNTIME_CODE_EXIT_CONFLICT,
                )
            payloads = _read_package(
                request.retained_package_root,
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
            )
            verified = require_runtime_code_attestation(
                attestation_bytes=payloads["runtime-code-attestation.json"],
                certificate_bytes=payloads["runtime-code-certificate.json"],
                bundle_bytes=payloads["runtime-code.bundle"],
                root_keyring=self._root_keyring,
                runtime_keyring=self._runtime_keyring,
                expected_audience=request.expected_audience,
                expected_installation_id=request.expected_installation_id,
                expected_target_platform=request.expected_target_platform,
                now=request.now,
            )
            attestation_bytes = payloads["runtime-code-attestation.json"]
            receipt = _sign_promotion_receipt(
                promotion_signer=promotion_signer,
                promotion_trust=self._promotion_trust,
                attestation_bytes=attestation_bytes,
                bundle_sha256=verified.bundle.bundle_sha256,
                content_root_sha256=verified.bundle.content_root_sha256,
                installation_id=request.expected_installation_id,
                target_platform=request.expected_target_platform,
                promotion_sequence=request.promotion_sequence,
                previous_receipt_sha256=current.promotion_receipt.receipt_hash,
            )
            payloads["runtime-code-promotion-receipt.json"] = canonical_model_json_bytes(receipt)
            _publish_package(request.output_root, payloads)
            return _package_result(
                status="rotated",
                output_root=request.output_root,
                attestation=verified.attestation,
                attestation_bytes=attestation_bytes,
                receipt=receipt,
            )
        except RuntimeCodeOperationError:
            raise
        except (
            AuthorityPathSecurityError,
            OSError,
            RuntimeCodeGenerationError,
            RuntimeCodeTrustError,
            StrictJsonError,
            ValidationError,
            ValueError,
        ) as exc:
            raise RuntimeCodeOperationError("runtime code rotation is invalid") from exc

    def dry_run(self, request: RuntimeCodeMigrationRequest) -> RuntimeCodeInstallPlan:
        try:
            verified, receipt, current = self._verify_install_candidate(request.install)
            checks = self._preflight_migration(
                request,
                deployment_generation=verified.attestation.provenance_commit,
            )
            return RuntimeCodeInstallPlan(
                generation_id=receipt.generation_id,
                previous_generation_id=(
                    None if current is None else current.evidence.generation_id
                ),
                promotion_sequence=receipt.promotion_sequence,
                checks=checks
                + (
                    "candidate-signatures-and-content-verified",
                    "promotion-current-and-anti-rollback-verified",
                    "install-plan-is-read-only",
                ),
            )
        except RuntimeCodeOperationError:
            raise
        except (
            AuthorityPathSecurityError,
            ExternalMonotonicRootSecurityError,
            OSError,
            RuntimeCodeGenerationError,
            RuntimeCodeTrustError,
            StrictJsonError,
            ValidationError,
            ValueError,
        ) as exc:
            raise RuntimeCodeOperationError(
                "runtime code migration preflight or promotion verification failed"
            ) from exc

    def install(self, request: RuntimeCodeMigrationRequest) -> RuntimeCodeInstallResult:
        plan = self.dry_run(request)
        try:
            receipt = RuntimeCodeGenerationInstaller(
                runtime_root=self.runtime_root,
                trusted_base=self.trusted_base,
                root_keyring=self._root_keyring,
                runtime_keyring=self._runtime_keyring,
                promotion_trust=self._promotion_trust,  # type: ignore[arg-type]
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
            ).install(request.install)
            return RuntimeCodeInstallResult(status="installed", receipt=receipt, checks=plan.checks)
        except RuntimeCodeGenerationError as exc:
            raise RuntimeCodeOperationError(
                "runtime code installation failed; previous generation remains selected",
                exit_code=RUNTIME_CODE_EXIT_CONFLICT,
            ) from exc

    def inspect(self, *, now: datetime | None = None) -> RuntimeCodeInspectResult:
        try:
            current = self._current(now=now or datetime.now(UTC), required=True)
            assert current is not None
            evidence = current.evidence
            return RuntimeCodeInspectResult(
                runtime_root=self.runtime_root,
                generation_id=evidence.generation_id,
                promotion_sequence=evidence.promotion_sequence,
                provenance_commit=evidence.provenance_commit,
                attestation_sha256=evidence.attestation_sha256,
                content_root_sha256=evidence.content_root_sha256,
            )
        except RuntimeCodeOperationError:
            raise
        except (
            AuthorityPathSecurityError,
            ExternalMonotonicRootSecurityError,
            OSError,
            RuntimeCodeGenerationError,
            RuntimeCodeTrustError,
            StrictJsonError,
            ValidationError,
            ValueError,
        ) as exc:
            raise RuntimeCodeOperationError("runtime code current generation is invalid") from exc

    def _preflight_migration(
        self,
        request: RuntimeCodeMigrationRequest,
        *,
        deployment_generation: str,
    ) -> tuple[str, ...]:
        from rquant.formal_runtime_command import (
            FormalRuntimeCommandError,
            compose_formal_daemon_argv,
            inspect_formal_systemd_service,
        )

        self._require_subject(
            audience=request.install.expected_audience,
            installation_id=request.install.expected_installation_id,
            target_platform=request.install.expected_target_platform,
        )
        for path in request.legacy_paths:
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            raise RuntimeCodeOperationError(
                "legacy runtime Git or checkout residue blocks formal migration",
                exit_code=RUNTIME_CODE_EXIT_CONFLICT,
            )
        for service in request.formal_services:
            if service.command not in _FORMAL_COMMANDS:
                raise RuntimeCodeOperationError("formal runtime service command is invalid")
            try:
                inspected = inspect_formal_systemd_service(unit_path=service.unit_path)
                binding = inspected.wrapper.bootstrap
                if (
                    inspected.wrapper.command != service.command
                    or binding.configuration_path != request.expected_configuration_path
                    or binding.trusted_base != request.expected_trusted_base
                    or binding.authority_uid != request.expected_authority_uid
                    or binding.authority_gid != request.expected_authority_gid
                ):
                    raise FormalRuntimeCommandError(
                        "formal runtime service binding does not match migration request"
                    )
                compose_formal_daemon_argv(
                    inspected.wrapper,
                    deployment_generation=deployment_generation,
                    deployment_generation_fd=3,
                    startup_deadline_monotonic=1.0,
                )
            except (FormalRuntimeCommandError, OSError) as exc:
                raise RuntimeCodeOperationError(
                    "formal runtime service artifact validation failed",
                    exit_code=RUNTIME_CODE_EXIT_CONFLICT,
                ) from exc
        return (
            "public-trust-material-verified",
            "formal-service-artifacts-and-argv-verified",
            "legacy-runtime-residue-absent",
        )

    def _require_subject(
        self,
        *,
        audience: str,
        installation_id: str,
        target_platform: str,
    ) -> None:
        if (audience, installation_id, target_platform) != (
            self._expected_audience,
            self._expected_installation_id,
            self._expected_target_platform,
        ):
            raise RuntimeCodeOperationError(
                "runtime code operation subject does not match immutable configuration"
            )

    def _verify_install_candidate(
        self,
        request: RuntimeCodeInstallRequest,
    ) -> tuple[
        VerifiedRuntimeCodeAttestation,
        RuntimeCodePromotionReceipt,
        LoadedRuntimeCodeGeneration | None,
    ]:
        payloads = _read_package(
            request.source_root,
            expected_uid=self._expected_uid,
            expected_gid=self._expected_gid,
            paths={
                "runtime-code.bundle": request.bundle_path,
                "runtime-code-attestation.json": request.attestation_path,
                "runtime-code-certificate.json": request.certificate_path,
                "runtime-code-promotion-receipt.json": request.receipt_path,
            },
        )
        verified = require_runtime_code_attestation(
            attestation_bytes=payloads["runtime-code-attestation.json"],
            certificate_bytes=payloads["runtime-code-certificate.json"],
            bundle_bytes=payloads["runtime-code.bundle"],
            root_keyring=self._root_keyring,
            runtime_keyring=self._runtime_keyring,
            expected_audience=request.expected_audience,
            expected_installation_id=request.expected_installation_id,
            expected_target_platform=request.expected_target_platform,
            now=request.now,
        )
        current = self._current(now=request.now, required=False)
        minimum_sequence, expected_previous = _next_promotion(current)
        receipt = require_runtime_code_promotion_receipt(
            receipt_bytes=payloads["runtime-code-promotion-receipt.json"],
            trust=self._promotion_trust,  # type: ignore[arg-type]
            attestation_sha256=hashlib.sha256(
                payloads["runtime-code-attestation.json"]
            ).hexdigest(),
            bundle_sha256=verified.bundle.bundle_sha256,
            content_root_sha256=verified.bundle.content_root_sha256,
            installation_id=request.expected_installation_id,
            target_platform=request.expected_target_platform,
            minimum_promotion_sequence=minimum_sequence,
            expected_previous_receipt_sha256=expected_previous,
        )
        self._promotion_trust.require_current_receipt(receipt=receipt)  # type: ignore[attr-defined]
        return verified, receipt, current

    def _current(
        self,
        *,
        now: datetime,
        required: bool,
    ) -> LoadedRuntimeCodeGeneration | None:
        current_path = self.runtime_root / "current"
        try:
            current_path.lstat()
        except FileNotFoundError:
            if required:
                raise RuntimeCodeOperationError(
                    "runtime code current generation is missing",
                    exit_code=RUNTIME_CODE_EXIT_CONFLICT,
                ) from None
            return None
        return require_attested_runtime_generation(
            runtime_root=self.runtime_root,
            trusted_base=self.trusted_base,
            root_keyring=self._root_keyring,
            runtime_keyring=self._runtime_keyring,
            promotion_trust=self._promotion_trust,  # type: ignore[arg-type]
            expected_uid=self._expected_uid,
            expected_gid=self._expected_gid,
            expected_audience=self._expected_audience,
            expected_installation_id=self._expected_installation_id,
            expected_target_platform=self._expected_target_platform,
            now=now,
            require_current_promotion=False,
        )


def _next_promotion(
    current: LoadedRuntimeCodeGeneration | None,
) -> tuple[int, str]:
    if current is None:
        return 1, _ZERO_HASH
    return (
        current.promotion_receipt.promotion_sequence + 1,
        current.promotion_receipt.receipt_hash,
    )


def _read_secure_file(
    path: Path,
    *,
    trusted_root: Path,
    expected_uid: int,
    expected_gid: int,
    max_bytes: int,
    allowed_modes: frozenset[int] = _PACKAGE_MODES,
) -> bytes:
    with open_secure_regular_file_lease(
        path,
        trusted_root=trusted_root,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        allowed_modes=allowed_modes,
        max_bytes=max_bytes,
    ) as lease:
        return lease.read_all(max_bytes=max_bytes)


def _read_package(
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    paths: Mapping[str, Path] | None = None,
) -> dict[str, bytes]:
    try:
        observed = {path.name for path in root.iterdir()}
    except OSError as exc:
        raise RuntimeCodeOperationError("runtime code package root is unavailable") from exc
    if observed != set(_PACKAGE_NAMES):
        raise RuntimeCodeOperationError("runtime code package artifact set is not canonical")
    selected = paths or {name: root / name for name in _PACKAGE_NAMES}
    if set(selected) != set(_PACKAGE_NAMES):
        raise RuntimeCodeOperationError("runtime code package artifact set is incomplete")
    return {
        name: _read_secure_file(
            path,
            trusted_root=root,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            max_bytes=(
                1024 * 1024 * 1024 if name == "runtime-code.bundle" else _MAX_AUTHORITY_BYTES
            ),
        )
        for name, path in selected.items()
    }


def _sign_promotion_receipt(
    *,
    promotion_signer: Ed25519ContractSigner,
    promotion_trust: RuntimeCodePromotionTrust,
    attestation_bytes: bytes,
    bundle_sha256: str,
    content_root_sha256: str,
    installation_id: str,
    target_platform: str,
    promotion_sequence: int,
    previous_receipt_sha256: str,
) -> RuntimeCodePromotionReceipt:
    if promotion_signer.key_purpose != RUNTIME_CODE_PROMOTION_KEY_PURPOSE:
        raise RuntimeCodeOperationError("runtime code promotion signer role is invalid")
    attestation_sha256 = hashlib.sha256(attestation_bytes).hexdigest()
    generation_id = compute_runtime_code_generation_id(
        attestation_sha256=attestation_sha256,
        bundle_sha256=bundle_sha256,
        content_root_sha256=content_root_sha256,
        installation_id=installation_id,
        target_platform=target_platform,
        promotion_sequence=promotion_sequence,
    )
    config = promotion_trust.config
    unsigned = RuntimeCodePromotionReceipt(
        role=RUNTIME_CODE_PROMOTION_ROLE,
        root_authority_id=config.root_authority_id,
        root_store_id=config.root_store_id,
        issuer=promotion_signer.issuer,
        key_id=promotion_signer.key_id,
        key_purpose=RUNTIME_CODE_PROMOTION_KEY_PURPOSE,
        namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        public_key_fingerprint=promotion_signer.public_key_fingerprint,
        rollback_domain_id=config.witness_rollback_domain_id,
        attestation_sha256=attestation_sha256,
        bundle_sha256=bundle_sha256,
        content_root_sha256=content_root_sha256,
        installation_id=installation_id,
        target_platform=target_platform,
        generation_id=generation_id,
        promotion_sequence=promotion_sequence,
        previous_receipt_sha256=previous_receipt_sha256,
        signature="unsigned",
    )
    receipt = unsigned.model_copy(
        update={
            "signature": promotion_signer.sign(
                namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )
    require_runtime_code_promotion_receipt(
        receipt_bytes=canonical_model_json_bytes(receipt),
        trust=promotion_trust,  # type: ignore[arg-type]
        attestation_sha256=attestation_sha256,
        bundle_sha256=bundle_sha256,
        content_root_sha256=content_root_sha256,
        installation_id=installation_id,
        target_platform=target_platform,
        minimum_promotion_sequence=promotion_sequence,
        expected_previous_receipt_sha256=previous_receipt_sha256,
    )
    return receipt


def _package_result(
    *,
    status: Literal["packaged", "rotated"],
    output_root: Path,
    attestation: RuntimeCodeAttestation,
    attestation_bytes: bytes,
    receipt: RuntimeCodePromotionReceipt,
) -> RuntimeCodePackageResult:
    return RuntimeCodePackageResult(
        status=status,
        output_root=output_root,
        generation_id=receipt.generation_id,
        attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
        bundle_sha256=attestation.bundle_sha256,
        content_root_sha256=attestation.content_root_sha256,
        promotion_sequence=receipt.promotion_sequence,
        previous_receipt_sha256=receipt.previous_receipt_sha256,
        external_promotion_required=True,
    )


def _publish_package(output_root: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != set(_PACKAGE_NAMES):
        raise RuntimeCodeOperationError("runtime code package artifact set is invalid")
    try:
        output_root.lstat()
    except FileNotFoundError:
        pass
    else:
        raise RuntimeCodeOperationError(
            "runtime code package output already exists",
            exit_code=RUNTIME_CODE_EXIT_CONFLICT,
        )
    if not output_root.parent.is_dir():
        raise RuntimeCodeOperationError("runtime code package output parent is missing")
    staging = output_root.with_name(f".{output_root.name}.{uuid.uuid4().hex}.staging")
    staging.mkdir(mode=0o700)
    try:
        for name in _PACKAGE_NAMES:
            path = staging / name
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as handle:
                    handle.write(payloads[name])
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                os.close(descriptor)
            os.chmod(path, 0o444, follow_symlinks=False)
        _fsync_directory(staging)
        os.replace(staging, output_root)
        _fsync_directory(output_root.parent)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _OfflineSigningClient:
    """OpenSSL is intentionally reachable only from explicit offline ceremonies."""

    def __init__(
        self,
        *,
        private_key_path: Path,
        public_record: Ed25519PublicKeyRecord,
        allowed_namespaces: frozenset[str],
        expected_uid: int,
        expected_gid: int,
    ) -> None:
        self._private_key_path = private_key_path
        self.key_purpose = public_record.key_purpose
        self.allowed_namespaces = allowed_namespaces
        self.public_key_fingerprint = public_record.public_key_fingerprint
        self._lock = threading.Lock()
        _read_secure_file(
            private_key_path,
            trusted_root=private_key_path.parent,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            max_bytes=64 * 1024,
            allowed_modes=frozenset({0o400, 0o600}),
        )
        completed = _run_openssl(("pkey", "-in", str(private_key_path), "-pubout"))
        if (
            completed.returncode != 0
            or ed25519_public_key_fingerprint(completed.stdout)
            != public_record.public_key_fingerprint
        ):
            raise RuntimeCodeOperationError("runtime code private key does not match public trust")

    def sign(self, *, key_purpose: KeyPurpose, namespace: str, payload: bytes) -> str:
        if key_purpose != self.key_purpose or namespace not in self.allowed_namespaces:
            raise RuntimeCodeOperationError("runtime code offline signer boundary changed")
        with self._lock, tempfile.TemporaryDirectory(prefix="rquant-runtime-code-sign-") as name:
            root = Path(name)
            message = root / "message.bin"
            signature = root / "signature.bin"
            message.write_bytes(payload)
            message.chmod(0o600)
            completed = _run_openssl(
                (
                    "pkeyutl",
                    "-sign",
                    "-inkey",
                    str(self._private_key_path),
                    "-rawin",
                    "-in",
                    str(message),
                    "-out",
                    str(signature),
                )
            )
            if completed.returncode != 0:
                raise RuntimeCodeOperationError("runtime code offline signing failed")
            raw = signature.read_bytes()
        if len(raw) != 64:
            raise RuntimeCodeOperationError("runtime code offline signature is invalid")
        return base64.b64encode(raw).decode("ascii")


def offline_contract_signer(
    *,
    private_key_path: Path,
    public_record: Ed25519PublicKeyRecord,
    expected_uid: int,
    expected_gid: int,
) -> Ed25519ContractSigner:
    namespaces = {
        "rquant_runtime_code_signer": frozenset({RUNTIME_CODE_ATTESTATION_NAMESPACE}),
        "rquant_runtime_code_promotion_root": frozenset({RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE}),
    }
    try:
        allowed_namespaces = namespaces[public_record.key_purpose]
    except KeyError as exc:
        raise RuntimeCodeOperationError("runtime code offline signer role is invalid") from exc
    return Ed25519ContractSigner(
        key_id=public_record.key_id,
        issuer=public_record.issuer,
        key_purpose=public_record.key_purpose,
        client=_OfflineSigningClient(
            private_key_path=private_key_path,
            public_record=public_record,
            allowed_namespaces=allowed_namespaces,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        ),
    )


def _run_openssl(arguments: tuple[str, ...]) -> subprocess.CompletedProcess[bytes]:
    executable = next(
        (
            candidate
            for candidate in (Path("/opt/homebrew/bin/openssl"), Path("/usr/bin/openssl"))
            if candidate.is_file()
        ),
        None,
    )
    if executable is None:
        raise RuntimeCodeOperationError(
            "OpenSSL is unavailable for the explicit offline ceremony",
            exit_code=RUNTIME_CODE_EXIT_UNAVAILABLE,
        )
    try:
        return subprocess.run(
            (str(executable), *arguments),
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=10,
            env={"PATH": "/usr/bin:/bin", "OPENSSL_CONF": "/dev/null"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeCodeOperationError(
            "OpenSSL failed during the explicit offline ceremony",
            exit_code=RUNTIME_CODE_EXIT_UNAVAILABLE,
        ) from exc


def load_runtime_code_operation_request(
    path: Path,
    model: type[RuntimeContractModel],
    *,
    trusted_base: Path,
    expected_uid: int,
    expected_gid: int,
) -> RuntimeContractModel:
    try:
        payload = _read_secure_file(
            path,
            trusted_root=trusted_base,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            max_bytes=_MAX_AUTHORITY_BYTES,
        )
        return strict_model_validate_canonical_json(model, payload)
    except (
        AuthorityPathSecurityError,
        OSError,
        StrictJsonError,
        ValidationError,
        ValueError,
    ) as exc:
        raise RuntimeCodeOperationError("runtime code operation request is invalid") from exc


def stable_runtime_code_error_payload(
    action: str,
    error: RuntimeCodeOperationError,
) -> dict[str, object]:
    return {
        "action": action,
        "exit_code": error.exit_code,
        "message": str(error),
        "status": "error",
    }


__all__ = [
    "RUNTIME_CODE_EXIT_CONFLICT",
    "RUNTIME_CODE_EXIT_INVALID",
    "RUNTIME_CODE_EXIT_OK",
    "RUNTIME_CODE_EXIT_UNAVAILABLE",
    "RuntimeCodeFormalService",
    "RuntimeCodeGenerationOperator",
    "RuntimeCodeInspectResult",
    "RuntimeCodeInstallPlan",
    "RuntimeCodeInstallResult",
    "RuntimeCodeMigrationRequest",
    "RuntimeCodeOperationError",
    "RuntimeCodePackageCeremonyRequest",
    "RuntimeCodePackageRequest",
    "RuntimeCodePackageResult",
    "RuntimeCodeRotateCeremonyRequest",
    "RuntimeCodeRotateRequest",
    "compose_runtime_code_generation_operator",
    "load_runtime_code_bootstrap_configuration",
    "load_runtime_code_operation_request",
    "offline_contract_signer",
    "stable_runtime_code_error_payload",
]
