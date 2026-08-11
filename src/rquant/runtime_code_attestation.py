"""Signed content authority for immutable formal runtime generations."""

from __future__ import annotations

import hashlib
import io
import tarfile
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Final, Literal, Protocol, Self

from pydantic import Field, ValidationError, field_validator, model_validator

from rquant.adapter_manifest import (
    RUNTIME_CODE_ATTESTATION_NAMESPACE,
    RUNTIME_CODE_ROOT_NAMESPACE,
    Ed25519ContractSigner,
    VerifyOnlyEd25519Keyring,
)
from rquant.external_monotonic_root import ExternalMonotonicRootReceiptIdentity
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE: Final = "rquant-runtime-code-promotion/v1"
RUNTIME_CODE_PROMOTION_ROLE: Final = "rquant_runtime_code_promotion_root"
RUNTIME_CODE_PROMOTION_KEY_PURPOSE: Final = "rquant_runtime_code_promotion_root"
RUNTIME_CODE_TRUST_PURPOSE: Final = "rquant-formal-runtime-code/v1"
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_PROVENANCE_PATTERN = r"^[0-9a-f]{40}([0-9a-f]{24})?$"
_MAX_FILE_BYTES = 256 * 1024 * 1024
_MAX_BUNDLE_BYTES = 1024 * 1024 * 1024
_CODE_SUFFIXES = frozenset({".py", ".pyc", ".pyo", ".so", ".dylib", ".pyd", ".pth"})
_SPECIAL_CODE_NAMES = frozenset({"sitecustomize.py", "usercustomize.py"})


class RuntimeCodeTrustError(ValueError):
    """A runtime content authority record or byte stream is not trusted."""


def _canonical_relative_path(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("runtime code path must be nonempty and canonical")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("runtime code path must be ASCII") from exc
    if "\\" in value or "//" in value or value.startswith("/"):
        raise ValueError("runtime code path must be a canonical relative POSIX path")
    path = PurePosixPath(value)
    if str(path) != value or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("runtime code path must be a canonical relative POSIX path")
    if any(part.casefold() == ".git" for part in path.parts):
        raise ValueError("runtime code paths cannot contain Git metadata")
    return value


def _canonical_absolute_path(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("runtime interpreter path must be canonical")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("runtime interpreter path must be ASCII") from exc
    if "\\" in value or "//" in value or not value.startswith("/"):
        raise ValueError("runtime interpreter path must be absolute POSIX")
    path = PurePosixPath(value)
    if str(path) != value or any(part in {".", ".."} for part in path.parts):
        raise ValueError("runtime interpreter path must be canonical")
    return value


def _ordered_unique_paths(paths: tuple[str, ...], *, label: str) -> None:
    if paths != tuple(sorted(paths)):
        raise ValueError(f"{label} must be ordered by canonical path")
    if len(set(paths)) != len(paths):
        raise ValueError(f"{label} contains duplicate paths")
    if len({path.casefold() for path in paths}) != len(paths):
        raise ValueError(f"{label} contains a case collision")


class RuntimeCodeFile(RuntimeContractModel):
    path: str
    type: Literal["regular"] = "regular"
    mode: Literal[292, 365]
    size: int = Field(strict=True, ge=0, le=_MAX_FILE_BYTES)
    sha256: str = Field(pattern=_HASH_PATTERN)

    @field_validator("path", mode="before")
    @classmethod
    def validate_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("runtime code path must be text")
        return _canonical_relative_path(value)

    @property
    def code_capable(self) -> bool:
        path = PurePosixPath(self.path)
        return (
            path.suffix.casefold() in _CODE_SUFFIXES or path.name.casefold() in _SPECIAL_CODE_NAMES
        )


class RuntimeCodeBundleEntry(RuntimeContractModel):
    path: str
    mode: Literal[292, 365]
    content: bytes = Field(max_length=_MAX_FILE_BYTES)

    @field_validator("path", mode="before")
    @classmethod
    def validate_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("runtime code path must be text")
        return _canonical_relative_path(value)

    @property
    def descriptor(self) -> RuntimeCodeFile:
        return RuntimeCodeFile(
            path=self.path,
            mode=self.mode,
            size=len(self.content),
            sha256=hashlib.sha256(self.content).hexdigest(),
        )


class RuntimeCodeBundle(RuntimeContractModel):
    bundle_bytes: bytes
    bundle_sha256: str = Field(pattern=_HASH_PATTERN)
    content_root_sha256: str = Field(pattern=_HASH_PATTERN)
    files: tuple[RuntimeCodeFile, ...] = Field(min_length=1)
    entries: tuple[RuntimeCodeBundleEntry, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_bundle_contract(self) -> Self:
        paths = tuple(file.path for file in self.files)
        _ordered_unique_paths(paths, label="runtime code file table")
        if tuple(entry.descriptor for entry in self.entries) != self.files:
            raise ValueError("runtime bundle entries do not match file table")
        if hashlib.sha256(self.bundle_bytes).hexdigest() != self.bundle_sha256:
            raise ValueError("runtime bundle hash mismatch")
        if _content_root_sha256(self.files) != self.content_root_sha256:
            raise ValueError("runtime bundle content root mismatch")
        return self


class RuntimeCodeExecutionSpec(RuntimeContractModel):
    launcher_path: str
    working_directory: str
    import_roots: tuple[str, ...] = Field(min_length=1)
    interpreter_path: str
    interpreter_sha256: str = Field(pattern=_HASH_PATTERN)
    python_abi: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    environment_allowlist: tuple[str, ...] = ()

    @field_validator("launcher_path", "working_directory", mode="before")
    @classmethod
    def validate_relative_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("execution path must be text")
        return _canonical_relative_path(value)

    @field_validator("import_roots", mode="before")
    @classmethod
    def validate_import_roots(cls, value: object) -> object:
        if not isinstance(value, (tuple, list)):
            raise ValueError("import roots must be a sequence")
        return tuple(_canonical_relative_path(item) for item in value)

    @field_validator("interpreter_path", mode="before")
    @classmethod
    def validate_interpreter_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("interpreter path must be text")
        return _canonical_absolute_path(value)

    @model_validator(mode="after")
    def validate_execution_environment(self) -> Self:
        _ordered_unique_paths(self.import_roots, label="runtime import roots")
        if self.environment_allowlist != tuple(sorted(set(self.environment_allowlist))):
            raise ValueError("runtime environment allowlist must be ordered and unique")
        for name in self.environment_allowlist:
            if (
                not name.isascii()
                or not name.replace("_", "A").isalnum()
                or name != name.upper()
                or name.startswith(("PYTHON", "GIT_", "DYLD_", "LD_"))
            ):
                raise ValueError("runtime environment allowlist contains a routing variable")
        return self


class RuntimeCodeTrustCertificate(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-trust-certificate/v1"] = (
        "rquant-runtime-code-trust-certificate/v1"
    )
    purpose: Literal["rquant-formal-runtime-code/v1"] = RUNTIME_CODE_TRUST_PURPOSE
    root_issuer: str = Field(min_length=1, max_length=200)
    root_key_id: str = Field(min_length=1, max_length=200)
    runtime_issuer: str = Field(min_length=1, max_length=200)
    runtime_key_id: str = Field(min_length=1, max_length=200)
    runtime_public_key_fingerprint: str = Field(pattern=_HASH_PATTERN)
    audience: str = Field(min_length=1, max_length=200)
    installation_id: str = Field(min_length=1, max_length=200)
    target_platform: str = Field(min_length=1, max_length=200)
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_interval(self) -> Self:
        if self.not_before >= self.expires_at:
            raise ValueError("runtime code certificate interval is empty")
        return self

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class RuntimeCodeAttestation(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-attestation/v1"] = "rquant-runtime-code-attestation/v1"
    purpose: Literal["rquant-formal-runtime-code/v1"] = RUNTIME_CODE_TRUST_PURPOSE
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    audience: str = Field(min_length=1, max_length=200)
    installation_id: str = Field(min_length=1, max_length=200)
    target_platform: str = Field(min_length=1, max_length=200)
    provenance_commit: str = Field(pattern=_PROVENANCE_PATTERN)
    content_root_sha256: str = Field(pattern=_HASH_PATTERN)
    bundle_sha256: str = Field(pattern=_HASH_PATTERN)
    files: tuple[RuntimeCodeFile, ...] = Field(min_length=1)
    execution_spec: RuntimeCodeExecutionSpec
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_attested_content(self) -> Self:
        _ordered_unique_paths(
            tuple(file.path for file in self.files),
            label="runtime code file table",
        )
        if self.not_before >= self.expires_at:
            raise ValueError("runtime code attestation interval is empty")
        by_path = {file.path: file for file in self.files}
        launcher = by_path.get(self.execution_spec.launcher_path)
        if launcher is None or launcher.mode != 0o555:
            raise ValueError("runtime launcher must be an attested executable")
        if not any(
            file.path == root or file.path.startswith(root + "/")
            for root in self.execution_spec.import_roots
            for file in self.files
        ):
            raise ValueError("runtime import roots contain no attested files")
        return self

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class RuntimeCodePromotionReceipt(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-promotion-receipt/v1"] = (
        "rquant-runtime-code-promotion-receipt/v1"
    )
    role: Literal["rquant_runtime_code_promotion_root"] = RUNTIME_CODE_PROMOTION_ROLE
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["rquant_runtime_code_promotion_root"] = RUNTIME_CODE_PROMOTION_KEY_PURPOSE
    namespace: Literal["rquant-runtime-code-promotion/v1"] = (
        RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE
    )
    signature_algorithm: Literal["ed25519"] = "ed25519"
    public_key_fingerprint: str = Field(pattern=_HASH_PATTERN)
    rollback_domain_id: str = Field(min_length=1, max_length=200)
    attestation_sha256: str = Field(pattern=_HASH_PATTERN)
    bundle_sha256: str = Field(pattern=_HASH_PATTERN)
    content_root_sha256: str = Field(pattern=_HASH_PATTERN)
    installation_id: str = Field(min_length=1, max_length=200)
    target_platform: str = Field(min_length=1, max_length=200)
    generation_id: str = Field(pattern=_HASH_PATTERN)
    promotion_sequence: int = Field(strict=True, ge=1)
    previous_receipt_sha256: str = Field(pattern=_HASH_PATTERN)
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def receipt_hash(self) -> str:
        return hashlib.sha256(canonical_model_json_bytes(self)).hexdigest()

    @property
    def external_identity(self) -> ExternalMonotonicRootReceiptIdentity:
        return ExternalMonotonicRootReceiptIdentity(
            role=self.role,
            root_authority_id=self.root_authority_id,
            root_store_id=self.root_store_id,
            closed=True,
            issuer=self.issuer,
            key_id=self.key_id,
            key_purpose=self.key_purpose,
            namespace=self.namespace,
            signature_algorithm=self.signature_algorithm,
            public_key_fingerprint=self.public_key_fingerprint,
        )


class RuntimeCodeGenerationArtifact(RuntimeContractModel):
    path: str
    mode: Literal[292, 365]
    size: int = Field(strict=True, ge=0, le=_MAX_BUNDLE_BYTES)
    sha256: str = Field(pattern=_HASH_PATTERN)

    @field_validator("path", mode="before")
    @classmethod
    def validate_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("generation artifact path must be text")
        return _canonical_relative_path(value)


class RuntimeCodeGenerationManifest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-runtime-code-generation-manifest/v1"] = (
        "rquant-runtime-code-generation-manifest/v1"
    )
    generation_id: str = Field(pattern=_HASH_PATTERN)
    attestation_sha256: str = Field(pattern=_HASH_PATTERN)
    receipt_sha256: str = Field(pattern=_HASH_PATTERN)
    bundle_sha256: str = Field(pattern=_HASH_PATTERN)
    materialized_tree_root_sha256: str = Field(pattern=_HASH_PATTERN)
    artifacts: tuple[RuntimeCodeGenerationArtifact, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_artifact_table(self) -> Self:
        _ordered_unique_paths(
            tuple(artifact.path for artifact in self.artifacts),
            label="runtime generation artifact table",
        )
        return self


class CodeTrustEvidence(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-code-trust-evidence/v1"] = "rquant-code-trust-evidence/v1"
    generation_id: str = Field(pattern=_HASH_PATTERN)
    attestation_sha256: str = Field(pattern=_HASH_PATTERN)
    content_root_sha256: str = Field(pattern=_HASH_PATTERN)
    promotion_sequence: int = Field(strict=True, ge=1)
    provenance_commit: str = Field(pattern=_PROVENANCE_PATTERN)


class VerifiedRuntimeCodeAttestation(RuntimeContractModel):
    certificate: RuntimeCodeTrustCertificate
    attestation: RuntimeCodeAttestation
    bundle: RuntimeCodeBundle


class RuntimeCodePromotionConfig(Protocol):
    role: str
    root_authority_id: str
    root_store_id: str
    root_issuer: str
    root_key_id: str
    root_key_purpose: str
    root_receipt_namespace: str
    root_signature_algorithm: str
    root_public_key_fingerprint: str
    witness_rollback_domain_id: str


class RuntimeCodePromotionTrust(Protocol):
    @property
    def config(self) -> RuntimeCodePromotionConfig: ...

    def verify_receipt(
        self,
        *,
        identity: ExternalMonotonicRootReceiptIdentity,
        signing_bytes: bytes,
        signature: str,
    ) -> None: ...


def _content_root_sha256(files: tuple[RuntimeCodeFile, ...]) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-runtime-code-content-root/v1",
                "files": [file.model_dump(mode="json") for file in files],
            }
        )
    ).hexdigest()


def build_runtime_code_bundle(entries: tuple[RuntimeCodeBundleEntry, ...]) -> RuntimeCodeBundle:
    if not entries:
        raise ValueError("runtime code bundle cannot be empty")
    ordered = tuple(sorted(entries, key=lambda entry: entry.path))
    _ordered_unique_paths(tuple(entry.path for entry in ordered), label="runtime bundle")
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for entry in ordered:
            info = tarfile.TarInfo(entry.path)
            info.type = tarfile.REGTYPE
            info.mode = entry.mode
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            info.size = len(entry.content)
            archive.addfile(info, io.BytesIO(entry.content))
    bundle_bytes = stream.getvalue()
    if len(bundle_bytes) > _MAX_BUNDLE_BYTES:
        raise ValueError("runtime code bundle is oversized")
    files = tuple(entry.descriptor for entry in ordered)
    return RuntimeCodeBundle(
        bundle_bytes=bundle_bytes,
        bundle_sha256=hashlib.sha256(bundle_bytes).hexdigest(),
        content_root_sha256=_content_root_sha256(files),
        files=files,
        entries=ordered,
    )


def require_canonical_runtime_code_bundle(
    bundle_bytes: bytes,
    *,
    expected_files: tuple[RuntimeCodeFile, ...],
    expected_content_root_sha256: str,
) -> RuntimeCodeBundle:
    if not 0 < len(bundle_bytes) <= _MAX_BUNDLE_BYTES:
        raise RuntimeCodeTrustError("runtime code bundle size is invalid")
    try:
        _ordered_unique_paths(
            tuple(file.path for file in expected_files),
            label="runtime code file table",
        )
        entries: list[RuntimeCodeBundleEntry] = []
        with tarfile.open(fileobj=io.BytesIO(bundle_bytes), mode="r:") as archive:
            members = archive.getmembers()
            if len(members) != len(expected_files):
                raise RuntimeCodeTrustError("runtime bundle table differs from attestation")
            for member, expected in zip(members, expected_files, strict=True):
                if (
                    member.name != expected.path
                    or member.type != tarfile.REGTYPE
                    or member.mode != expected.mode
                    or member.size != expected.size
                    or member.uid != 0
                    or member.gid != 0
                    or member.uname != ""
                    or member.gname != ""
                    or member.mtime != 0
                    or member.pax_headers
                ):
                    raise RuntimeCodeTrustError("runtime bundle member is not canonical")
                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeCodeTrustError("runtime bundle regular file is unreadable")
                content = source.read(expected.size + 1)
                if (
                    len(content) != expected.size
                    or hashlib.sha256(content).hexdigest() != expected.sha256
                ):
                    raise RuntimeCodeTrustError("runtime bundle file hash differs from attestation")
                entries.append(
                    RuntimeCodeBundleEntry(
                        path=expected.path,
                        mode=expected.mode,
                        content=content,
                    )
                )
        verified = build_runtime_code_bundle(tuple(entries))
        if (
            verified.bundle_bytes != bundle_bytes
            or verified.files != expected_files
            or verified.content_root_sha256 != expected_content_root_sha256
        ):
            raise RuntimeCodeTrustError("runtime code bundle is not canonical")
        return verified
    except RuntimeCodeTrustError:
        raise
    except (OSError, tarfile.TarError, ValidationError, ValueError) as exc:
        raise RuntimeCodeTrustError("runtime code bundle is invalid") from exc


def sign_runtime_code_trust_certificate(
    *,
    root_signer: Ed25519ContractSigner,
    runtime_signer: Ed25519ContractSigner,
    audience: str,
    installation_id: str,
    target_platform: str,
    not_before: datetime,
    expires_at: datetime,
) -> RuntimeCodeTrustCertificate:
    if root_signer.key_purpose != "rquant_runtime_code_root":
        raise RuntimeCodeTrustError("runtime code root signer role is invalid")
    if runtime_signer.key_purpose != "rquant_runtime_code_signer":
        raise RuntimeCodeTrustError("runtime code signer role is invalid")
    unsigned = RuntimeCodeTrustCertificate(
        root_issuer=root_signer.issuer,
        root_key_id=root_signer.key_id,
        runtime_issuer=runtime_signer.issuer,
        runtime_key_id=runtime_signer.key_id,
        runtime_public_key_fingerprint=runtime_signer.public_key_fingerprint,
        audience=audience,
        installation_id=installation_id,
        target_platform=target_platform,
        not_before=not_before,
        expires_at=expires_at,
        signature="unsigned",
    )
    return unsigned.model_copy(
        update={
            "signature": root_signer.sign(
                namespace=RUNTIME_CODE_ROOT_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )


def sign_runtime_code_attestation(
    *,
    signer: Ed25519ContractSigner,
    bundle: RuntimeCodeBundle,
    execution_spec: RuntimeCodeExecutionSpec,
    audience: str,
    installation_id: str,
    target_platform: str,
    provenance_commit: str,
    not_before: datetime,
    expires_at: datetime,
) -> RuntimeCodeAttestation:
    if signer.key_purpose != "rquant_runtime_code_signer":
        raise RuntimeCodeTrustError("runtime code signer role is invalid")
    unsigned = RuntimeCodeAttestation(
        issuer=signer.issuer,
        key_id=signer.key_id,
        audience=audience,
        installation_id=installation_id,
        target_platform=target_platform,
        provenance_commit=provenance_commit,
        content_root_sha256=bundle.content_root_sha256,
        bundle_sha256=bundle.bundle_sha256,
        files=bundle.files,
        execution_spec=execution_spec,
        not_before=not_before,
        expires_at=expires_at,
        signature="unsigned",
    )
    return unsigned.model_copy(
        update={
            "signature": signer.sign(
                namespace=RUNTIME_CODE_ATTESTATION_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )


def require_runtime_code_attestation(
    *,
    attestation_bytes: bytes,
    certificate_bytes: bytes,
    bundle_bytes: bytes,
    root_keyring: VerifyOnlyEd25519Keyring,
    runtime_keyring: VerifyOnlyEd25519Keyring,
    expected_audience: str,
    expected_installation_id: str,
    expected_target_platform: str,
    now: datetime,
) -> VerifiedRuntimeCodeAttestation:
    try:
        certificate = strict_model_validate_canonical_json(
            RuntimeCodeTrustCertificate,
            certificate_bytes,
        )
        attestation = strict_model_validate_canonical_json(
            RuntimeCodeAttestation,
            attestation_bytes,
        )
        current = now.astimezone(UTC)
        expected_binding = (
            expected_audience,
            expected_installation_id,
            expected_target_platform,
        )
        if (
            (certificate.audience, certificate.installation_id, certificate.target_platform)
            != expected_binding
            or (attestation.audience, attestation.installation_id, attestation.target_platform)
            != expected_binding
            or not (certificate.not_before <= current < certificate.expires_at)
            or not (attestation.not_before <= current < attestation.expires_at)
            or attestation.not_before < certificate.not_before
            or attestation.expires_at > certificate.expires_at
            or certificate.runtime_issuer != attestation.issuer
            or certificate.runtime_key_id != attestation.key_id
            or certificate.runtime_public_key_fingerprint
            not in runtime_keyring.fingerprints_for_purpose("rquant_runtime_code_signer")
            or not root_keyring.verify(
                issuer=certificate.root_issuer,
                key_id=certificate.root_key_id,
                key_purpose="rquant_runtime_code_root",
                namespace=RUNTIME_CODE_ROOT_NAMESPACE,
                payload=certificate.signing_bytes(),
                signature=certificate.signature,
            )
            or not runtime_keyring.verify(
                issuer=attestation.issuer,
                key_id=attestation.key_id,
                key_purpose="rquant_runtime_code_signer",
                namespace=RUNTIME_CODE_ATTESTATION_NAMESPACE,
                payload=attestation.signing_bytes(),
                signature=attestation.signature,
            )
            or hashlib.sha256(bundle_bytes).hexdigest() != attestation.bundle_sha256
        ):
            raise RuntimeCodeTrustError("runtime code attestation trust chain is invalid")
        bundle = require_canonical_runtime_code_bundle(
            bundle_bytes,
            expected_files=attestation.files,
            expected_content_root_sha256=attestation.content_root_sha256,
        )
        return VerifiedRuntimeCodeAttestation(
            certificate=certificate,
            attestation=attestation,
            bundle=bundle,
        )
    except RuntimeCodeTrustError:
        raise
    except (StrictJsonError, ValidationError, ValueError) as exc:
        raise RuntimeCodeTrustError("runtime code attestation is invalid") from exc


def compute_runtime_code_generation_id(
    *,
    attestation_sha256: str,
    bundle_sha256: str,
    content_root_sha256: str,
    installation_id: str,
    target_platform: str,
    promotion_sequence: int,
) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-runtime-code-generation-basis/v1",
            "attestation_sha256": attestation_sha256,
            "bundle_sha256": bundle_sha256,
            "content_root_sha256": content_root_sha256,
            "installation_id": installation_id,
            "target_platform": target_platform,
            "promotion_sequence": promotion_sequence,
        }
    )


def require_runtime_code_promotion_receipt(
    *,
    receipt_bytes: bytes,
    trust: RuntimeCodePromotionTrust,
    attestation_sha256: str,
    bundle_sha256: str,
    content_root_sha256: str,
    installation_id: str,
    target_platform: str,
    minimum_promotion_sequence: int,
    expected_previous_receipt_sha256: str,
) -> RuntimeCodePromotionReceipt:
    try:
        receipt = strict_model_validate_canonical_json(RuntimeCodePromotionReceipt, receipt_bytes)
        config = trust.config
        expected_identity = (
            config.role,
            config.root_authority_id,
            config.root_store_id,
            config.root_issuer,
            config.root_key_id,
            config.root_key_purpose,
            config.root_receipt_namespace,
            config.root_signature_algorithm,
            config.root_public_key_fingerprint,
            config.witness_rollback_domain_id,
        )
        observed_identity = (
            receipt.role,
            receipt.root_authority_id,
            receipt.root_store_id,
            receipt.issuer,
            receipt.key_id,
            receipt.key_purpose,
            receipt.namespace,
            receipt.signature_algorithm,
            receipt.public_key_fingerprint,
            receipt.rollback_domain_id,
        )
        if observed_identity != expected_identity:
            raise RuntimeCodeTrustError("runtime code promotion receipt role is untrusted")
        expected_binding = (
            attestation_sha256,
            bundle_sha256,
            content_root_sha256,
            installation_id,
            target_platform,
        )
        if (
            (
                receipt.attestation_sha256,
                receipt.bundle_sha256,
                receipt.content_root_sha256,
                receipt.installation_id,
                receipt.target_platform,
            )
            != expected_binding
            or receipt.previous_receipt_sha256 != expected_previous_receipt_sha256
        ):
            raise RuntimeCodeTrustError("runtime code promotion receipt binding is invalid")
        if receipt.promotion_sequence < minimum_promotion_sequence:
            raise RuntimeCodeTrustError("runtime code promotion rollback is forbidden")
        generation_id = compute_runtime_code_generation_id(
            attestation_sha256=attestation_sha256,
            bundle_sha256=bundle_sha256,
            content_root_sha256=content_root_sha256,
            installation_id=installation_id,
            target_platform=target_platform,
            promotion_sequence=receipt.promotion_sequence,
        )
        if receipt.generation_id != generation_id:
            raise RuntimeCodeTrustError("runtime code promotion generation binding is invalid")
        trust.verify_receipt(
            identity=receipt.external_identity,
            signing_bytes=receipt.signing_bytes(),
            signature=receipt.signature,
        )
        return receipt
    except RuntimeCodeTrustError:
        raise
    except (AttributeError, StrictJsonError, ValidationError, ValueError) as exc:
        raise RuntimeCodeTrustError("runtime code promotion receipt is invalid") from exc


__all__ = [
    "CodeTrustEvidence",
    "RUNTIME_CODE_ATTESTATION_NAMESPACE",
    "RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE",
    "RUNTIME_CODE_ROOT_NAMESPACE",
    "RuntimeCodeAttestation",
    "RuntimeCodeBundle",
    "RuntimeCodeBundleEntry",
    "RuntimeCodeExecutionSpec",
    "RuntimeCodeFile",
    "RuntimeCodeGenerationArtifact",
    "RuntimeCodeGenerationManifest",
    "RuntimeCodePromotionReceipt",
    "RuntimeCodeTrustCertificate",
    "RuntimeCodeTrustError",
    "VerifiedRuntimeCodeAttestation",
    "build_runtime_code_bundle",
    "compute_runtime_code_generation_id",
    "require_canonical_runtime_code_bundle",
    "require_runtime_code_attestation",
    "require_runtime_code_promotion_receipt",
    "sign_runtime_code_attestation",
    "sign_runtime_code_trust_certificate",
]
