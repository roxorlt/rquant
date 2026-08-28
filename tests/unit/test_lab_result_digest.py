from __future__ import annotations

import pytest
from pydantic import ValidationError

from rquant.lab_result_digest import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    LEGACY_CONTENT_DIGEST_ALGORITHM,
    LabLegacyContentDigestProvenance,
    LabResultDigestPolicy,
    LabResultDigestProvenanceError,
    require_matching_manifest_digest_provenance,
    resolve_success_digest_provenance,
)

LEGACY_CODE_SHA = "53dc0afe74d5af44f1d4a4bcda149d6a5b52c854"


def test_legacy_digest_policy_is_empty_and_fail_closed_by_default() -> None:
    policy = LabResultDigestPolicy()

    assert not policy.allows_legacy(
        code_sha=LEGACY_CODE_SHA,
        manifest_schema_version=1,
        content_digest_algorithm=LEGACY_CONTENT_DIGEST_ALGORITHM,
    )


def test_legacy_digest_policy_requires_exact_typed_provenance() -> None:
    policy = LabResultDigestPolicy(
        legacy_allowlist=(LabLegacyContentDigestProvenance(code_sha=LEGACY_CODE_SHA),)
    )

    assert policy.allows_legacy(
        code_sha=LEGACY_CODE_SHA,
        manifest_schema_version=1,
        content_digest_algorithm=LEGACY_CONTENT_DIGEST_ALGORITHM,
    )
    assert not policy.allows_legacy(
        code_sha="0" * 40,
        manifest_schema_version=1,
        content_digest_algorithm=LEGACY_CONTENT_DIGEST_ALGORITHM,
    )
    assert not policy.allows_legacy(
        code_sha=LEGACY_CODE_SHA,
        manifest_schema_version=2,
        content_digest_algorithm=LEGACY_CONTENT_DIGEST_ALGORITHM,
    )
    assert not policy.allows_legacy(
        code_sha=LEGACY_CODE_SHA,
        manifest_schema_version=1,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
    )


def test_legacy_digest_policy_rejects_duplicate_or_untyped_entries() -> None:
    provenance = LabLegacyContentDigestProvenance(code_sha=LEGACY_CODE_SHA)

    with pytest.raises(ValidationError, match="unique"):
        LabResultDigestPolicy(legacy_allowlist=(provenance, provenance))
    with pytest.raises(ValidationError):
        LabLegacyContentDigestProvenance(
            code_sha=LEGACY_CODE_SHA,
            manifest_schema_version=2,
        )


def test_current_digest_provenance_binds_exact_job_code_and_manifest_contract() -> None:
    provenance = resolve_success_digest_provenance(
        expected_job_code_sha="1" * 40,
        result_manifest_schema_version=2,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
        worker_code_sha="1" * 40,
        policy=LabResultDigestPolicy(),
    )

    require_matching_manifest_digest_provenance(
        provenance,
        manifest_schema_version=2,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
        worker_code_sha="1" * 40,
    )
    with pytest.raises(LabResultDigestProvenanceError, match="job code"):
        resolve_success_digest_provenance(
            expected_job_code_sha="1" * 40,
            result_manifest_schema_version=2,
            content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
            worker_code_sha="2" * 40,
            policy=LabResultDigestPolicy(),
        )


def test_legacy_digest_provenance_requires_exact_v1_manifest_shape() -> None:
    policy = LabResultDigestPolicy(
        legacy_allowlist=(LabLegacyContentDigestProvenance(code_sha=LEGACY_CODE_SHA),)
    )
    provenance = resolve_success_digest_provenance(
        expected_job_code_sha=LEGACY_CODE_SHA,
        result_manifest_schema_version=None,
        content_digest_algorithm=None,
        worker_code_sha=None,
        policy=policy,
    )

    require_matching_manifest_digest_provenance(
        provenance,
        manifest_schema_version=1,
        content_digest_algorithm=None,
        worker_code_sha=None,
    )
    with pytest.raises(LabResultDigestProvenanceError, match="manifest"):
        require_matching_manifest_digest_provenance(
            provenance,
            manifest_schema_version=2,
            content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
            worker_code_sha=LEGACY_CODE_SHA,
        )
