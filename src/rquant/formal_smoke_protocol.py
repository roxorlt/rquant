"""Canonical cross-generation protocol for formal smoke execution."""

from __future__ import annotations

import hashlib
import os
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Self

from pydantic import ConfigDict, Field, field_validator, model_validator

from rquant.authority_path_security import AuthorityPathSecurityError
from rquant.runtime_code_attestation import CodeTrustEvidence, RuntimeCodeFile
from rquant.runtime_code_generation import (
    RuntimeCodeGenerationCapability,
    RuntimeCodeGenerationError,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, canonical_model_json_bytes

FormalSmokeStrategy = Literal["n_shape", "growth_board_surge", "auction_gap"]
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_COMMIT_PATTERN = r"^[0-9a-f]{40}$"


def _canonical_absolute_path(value: Path, *, label: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"{label} must be a canonical absolute path")
    return candidate


def _canonical_relative_path(value: str, *, label: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or not path.parts
        or path.is_absolute()
        or str(path) != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError(f"{label} must be a canonical relative path")
    return value


class FormalSmokeExecutionIdentity(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-smoke-execution-identity/v1"] = (
        "rquant-formal-smoke-execution-identity/v1"
    )
    generation_id: str = Field(pattern=_HASH_PATTERN)
    generation_root: Path
    material_uid: int = Field(strict=True, ge=0)
    material_gid: int = Field(strict=True, ge=0)
    launcher: RuntimeCodeFile
    interpreter: RuntimeCodeFile
    working_directory: str
    import_roots: tuple[str, ...] = Field(min_length=1)
    python_abi: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    bootstrap_sha256: str = Field(pattern=_HASH_PATTERN)
    code_files: tuple[RuntimeCodeFile, ...] = Field(min_length=1)

    @field_validator("generation_root", mode="after")
    @classmethod
    def validate_generation_root(cls, value: Path) -> Path:
        return _canonical_absolute_path(value, label="formal smoke generation root")

    @field_validator("working_directory", mode="before")
    @classmethod
    def validate_working_directory(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("formal smoke working directory must be text")
        return _canonical_relative_path(value, label="formal smoke working directory")

    @field_validator("import_roots", mode="before")
    @classmethod
    def validate_import_roots(cls, value: object) -> object:
        if not isinstance(value, (tuple, list)):
            raise ValueError("formal smoke import roots must be a sequence")
        roots = tuple(
            _canonical_relative_path(item, label="formal smoke import root") for item in value
        )
        if roots != tuple(sorted(set(roots))):
            raise ValueError("formal smoke import roots must be ordered and unique")
        return roots

    @model_validator(mode="after")
    def validate_file_identity(self) -> Self:
        paths = tuple(file.path for file in self.code_files)
        if paths != tuple(sorted(set(paths))):
            raise ValueError("formal smoke code files must be ordered and unique")
        by_path = {file.path: file for file in self.code_files}
        if by_path.get(self.launcher.path) != self.launcher:
            raise ValueError("formal smoke launcher is absent from attested code files")
        if by_path.get(self.interpreter.path) != self.interpreter:
            raise ValueError("formal smoke interpreter is absent from attested code files")
        if self.launcher.mode != 0o555 or self.interpreter.mode != 0o555:
            raise ValueError("formal smoke launcher and interpreter must be executable")
        for path in (self.working_directory, *self.import_roots):
            if PurePosixPath(path).parts[0] != "release":
                raise ValueError("formal smoke execution path escapes release")
        for root in self.import_roots:
            if not any(path == root or path.startswith(root + "/") for path in paths):
                raise ValueError("formal smoke import root contains no attested code")
        return self


class FormalSmokeBootstrapReference(RuntimeContractModel):
    configuration_path: Path
    trusted_base: Path
    expected_authority_uid: int = Field(strict=True, ge=0)
    expected_authority_gid: int = Field(strict=True, ge=0)

    @field_validator("configuration_path", "trusted_base", mode="after")
    @classmethod
    def validate_bootstrap_path(cls, value: Path) -> Path:
        return _canonical_absolute_path(value, label="formal smoke bootstrap path")

    @model_validator(mode="after")
    def validate_configuration_boundary(self) -> Self:
        try:
            self.configuration_path.relative_to(self.trusted_base)
        except ValueError as exc:
            raise ValueError("formal smoke bootstrap configuration escapes trusted base") from exc
        return self


class FormalSmokeExecutionRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-smoke-execution-request/v1"] = (
        "rquant-formal-smoke-execution-request/v1"
    )
    strategy: FormalSmokeStrategy
    start_date: date
    end_date: date
    audit_run_id: str = Field(pattern=_HASH_PATTERN)
    dataset_snapshot_id: str = Field(pattern=_HASH_PATTERN)
    dataset_binding_hash: str = Field(pattern=_HASH_PATTERN)
    code_commit: str = Field(pattern=_COMMIT_PATTERN)
    code_trust_evidence: CodeTrustEvidence
    execution_identity: FormalSmokeExecutionIdentity
    bootstrap_reference: FormalSmokeBootstrapReference
    artifact_root: Path
    staging_root: Path

    @field_validator("artifact_root", "staging_root", mode="after")
    @classmethod
    def validate_artifact_path(cls, value: Path) -> Path:
        return _canonical_absolute_path(value, label="formal smoke artifact path")

    @model_validator(mode="after")
    def validate_request_binding(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("formal smoke start_date cannot be after end_date")
        if self.code_commit != self.code_trust_evidence.provenance_commit:
            raise ValueError("formal smoke commit does not match code evidence")
        if self.execution_identity.generation_id != self.code_trust_evidence.generation_id:
            raise ValueError("formal smoke generation does not match code evidence")
        if self.staging_root.parent != self.artifact_root or not self.staging_root.name.startswith(
            ".formal-smoke-"
        ):
            raise ValueError("formal smoke staging root is outside the artifact root")
        return self


class FormalSmokeReplayRequest(RuntimeContractModel):
    """Generation-local business request derived from a verified execution request."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )

    strategy: FormalSmokeStrategy
    start_date: date
    end_date: date
    audit_run_id: str = Field(pattern=_HASH_PATTERN)
    dataset_snapshot_id: str = Field(pattern=_HASH_PATTERN)
    dataset_binding_hash: str = Field(pattern=_HASH_PATTERN)
    code_commit: str = Field(pattern=_COMMIT_PATTERN)
    runtime_capability: RuntimeCodeGenerationCapability

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("formal smoke start_date cannot be after end_date")
        try:
            self.runtime_capability.require_live()
        except (AuthorityPathSecurityError, RuntimeCodeGenerationError) as exc:
            raise ValueError("formal smoke runtime capability is invalid") from exc
        if self.code_commit != self.runtime_capability.evidence.provenance_commit:
            raise ValueError("formal smoke code_commit does not match runtime capability")
        return self


class FormalSmokeReplayPayload(RuntimeContractModel):
    status: Literal["comparable"] = "comparable"
    strategy: FormalSmokeStrategy
    fixed_spec_version: Literal["stage1-smoke-v1"]
    run_id: str = Field(min_length=1, max_length=300)
    audit_run_id: str = Field(pattern=_HASH_PATTERN)
    dataset_snapshot_id: str = Field(pattern=_HASH_PATTERN)
    dataset_binding_hash: str = Field(pattern=_HASH_PATTERN)
    code_commit: str = Field(pattern=_COMMIT_PATTERN)
    strategy_spec_hash: str = Field(pattern=_HASH_PATTERN)
    result_hash: str = Field(pattern=_HASH_PATTERN)
    sample_count: int = Field(strict=True, ge=0)
    metrics: dict[str, Any]
    missing_evidence: tuple[str, ...]


class FormalSmokeReplayResult(FormalSmokeReplayPayload):
    json_path: Path
    markdown_path: Path


class FormalSmokeArtifactReceipt(RuntimeContractModel):
    kind: Literal["json", "markdown"]
    relative_path: str
    size: int = Field(strict=True, ge=0, le=64 * 1024 * 1024)
    sha256: str = Field(pattern=_HASH_PATTERN)

    @field_validator("relative_path", mode="before")
    @classmethod
    def validate_relative_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("formal smoke artifact path must be text")
        return _canonical_relative_path(value, label="formal smoke artifact path")


def formal_smoke_request_digest(request: FormalSmokeExecutionRequest) -> str:
    return hashlib.sha256(canonical_model_json_bytes(request)).hexdigest()


def formal_smoke_result_digest(
    result: FormalSmokeReplayPayload,
    artifacts: tuple[FormalSmokeArtifactReceipt, ...],
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
                "result": result.model_dump(mode="json"),
            }
        )
    ).hexdigest()


class FormalSmokeExecutionReceipt(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-smoke-execution-receipt/v1"] = (
        "rquant-formal-smoke-execution-receipt/v1"
    )
    code_trust_evidence: CodeTrustEvidence
    request_digest: str = Field(pattern=_HASH_PATTERN)
    execution_identity: FormalSmokeExecutionIdentity
    result: FormalSmokeReplayPayload
    artifacts: tuple[FormalSmokeArtifactReceipt, ...] = Field(min_length=2, max_length=2)
    result_digest: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_receipt_binding(self) -> Self:
        if self.execution_identity.generation_id != self.code_trust_evidence.generation_id:
            raise ValueError("formal smoke receipt generation does not match evidence")
        if self.result.code_commit != self.code_trust_evidence.provenance_commit:
            raise ValueError("formal smoke receipt result does not match evidence")
        if tuple(artifact.kind for artifact in self.artifacts) != ("json", "markdown"):
            raise ValueError("formal smoke receipt artifact set is invalid")
        expected_paths = (
            f"strategy_lab_runs/{self.result.run_id}.json",
            f"strategy_lab_runs/{self.result.run_id}.md",
        )
        if tuple(artifact.relative_path for artifact in self.artifacts) != expected_paths:
            raise ValueError("formal smoke receipt artifact paths do not match result")
        if self.result_digest != formal_smoke_result_digest(self.result, self.artifacts):
            raise ValueError("formal smoke receipt result digest is invalid")
        return self


def formal_smoke_receipt_digest(receipt: FormalSmokeExecutionReceipt) -> str:
    return hashlib.sha256(canonical_model_json_bytes(receipt)).hexdigest()


class FormalSmokeAttestedReplayResult(FormalSmokeReplayResult):
    execution_receipt: FormalSmokeExecutionReceipt
    execution_receipt_digest: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_accepted_execution(self) -> Self:
        payload = FormalSmokeReplayPayload.model_validate(
            {name: getattr(self, name) for name in FormalSmokeReplayPayload.model_fields}
        )
        if payload != self.execution_receipt.result:
            raise ValueError("formal smoke accepted result does not match execution receipt")
        if self.execution_receipt_digest != formal_smoke_receipt_digest(self.execution_receipt):
            raise ValueError("formal smoke accepted receipt digest is invalid")
        return self


__all__ = [
    "FormalSmokeArtifactReceipt",
    "FormalSmokeAttestedReplayResult",
    "FormalSmokeBootstrapReference",
    "FormalSmokeExecutionIdentity",
    "FormalSmokeExecutionReceipt",
    "FormalSmokeExecutionRequest",
    "FormalSmokeReplayPayload",
    "FormalSmokeReplayRequest",
    "FormalSmokeReplayResult",
    "FormalSmokeStrategy",
    "formal_smoke_request_digest",
    "formal_smoke_receipt_digest",
    "formal_smoke_result_digest",
]
