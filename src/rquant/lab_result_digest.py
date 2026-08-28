"""Typed provenance for Strategy Lab shard content digests."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

CURRENT_RESULT_MANIFEST_SCHEMA_VERSION = 2
LEGACY_RESULT_MANIFEST_SCHEMA_VERSION = 1
CURRENT_CONTENT_DIGEST_ALGORITHM = "rquant-pandas-table-json-sha256-v2"
LEGACY_CONTENT_DIGEST_ALGORITHM = "pandas-orient-table-json-sha256-v1"


class LabResultDigestModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


class LabLegacyContentDigestProvenance(LabResultDigestModel):
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    manifest_schema_version: Literal[1] = LEGACY_RESULT_MANIFEST_SCHEMA_VERSION
    content_digest_algorithm: Literal["pandas-orient-table-json-sha256-v1"] = (
        LEGACY_CONTENT_DIGEST_ALGORITHM
    )


class LabResolvedContentDigestProvenance(LabResultDigestModel):
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    manifest_schema_version: Literal[1, 2]
    content_digest_algorithm: Literal[
        "pandas-orient-table-json-sha256-v1",
        "rquant-pandas-table-json-sha256-v2",
    ]
    legacy: bool


class LabResultDigestProvenanceError(ValueError):
    """A shard result is not bound to an authorized digest contract."""


class LabResultDigestPolicy(LabResultDigestModel):
    legacy_allowlist: tuple[LabLegacyContentDigestProvenance, ...] = ()

    @model_validator(mode="after")
    def validate_unique_legacy_provenance(self) -> LabResultDigestPolicy:
        identities = tuple(
            (
                item.code_sha,
                item.manifest_schema_version,
                item.content_digest_algorithm,
            )
            for item in self.legacy_allowlist
        )
        if len(identities) != len(set(identities)):
            raise ValueError("legacy digest provenance entries must be unique")
        return self

    def allows_legacy(
        self,
        *,
        code_sha: str,
        manifest_schema_version: int,
        content_digest_algorithm: str,
    ) -> bool:
        identity = (
            code_sha,
            manifest_schema_version,
            content_digest_algorithm,
        )
        return any(
            identity
            == (
                item.code_sha,
                item.manifest_schema_version,
                item.content_digest_algorithm,
            )
            for item in self.legacy_allowlist
        )


def resolve_success_digest_provenance(
    *,
    expected_job_code_sha: str,
    result_manifest_schema_version: int | None,
    content_digest_algorithm: str | None,
    worker_code_sha: str | None,
    policy: LabResultDigestPolicy,
) -> LabResolvedContentDigestProvenance:
    supplied = (
        result_manifest_schema_version,
        content_digest_algorithm,
        worker_code_sha,
    )
    if supplied == (None, None, None):
        if not policy.allows_legacy(
            code_sha=expected_job_code_sha,
            manifest_schema_version=LEGACY_RESULT_MANIFEST_SCHEMA_VERSION,
            content_digest_algorithm=LEGACY_CONTENT_DIGEST_ALGORITHM,
        ):
            raise LabResultDigestProvenanceError(
                "unversioned shard success is not eligible for legacy digest recovery"
            )
        return LabResolvedContentDigestProvenance(
            code_sha=expected_job_code_sha,
            manifest_schema_version=LEGACY_RESULT_MANIFEST_SCHEMA_VERSION,
            content_digest_algorithm=LEGACY_CONTENT_DIGEST_ALGORITHM,
            legacy=True,
        )
    if supplied != (
        CURRENT_RESULT_MANIFEST_SCHEMA_VERSION,
        CURRENT_CONTENT_DIGEST_ALGORITHM,
        expected_job_code_sha,
    ):
        raise LabResultDigestProvenanceError(
            "current shard success digest provenance conflicts with job code identity"
        )
    return LabResolvedContentDigestProvenance(
        code_sha=expected_job_code_sha,
        manifest_schema_version=CURRENT_RESULT_MANIFEST_SCHEMA_VERSION,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
        legacy=False,
    )


def require_matching_manifest_digest_provenance(
    provenance: LabResolvedContentDigestProvenance,
    *,
    manifest_schema_version: int,
    content_digest_algorithm: str | None,
    worker_code_sha: str | None,
) -> None:
    expected = (
        provenance.manifest_schema_version,
        None if provenance.legacy else provenance.content_digest_algorithm,
        None if provenance.legacy else provenance.code_sha,
    )
    actual = (
        manifest_schema_version,
        content_digest_algorithm,
        worker_code_sha,
    )
    if actual != expected:
        raise LabResultDigestProvenanceError(
            "result manifest digest provenance conflicts with accepted success report"
        )
