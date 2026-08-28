from __future__ import annotations

from datetime import datetime

from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2ClaimRef,
    SourceBrokerV2FenceRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2LineageRef,
    SourceBrokerV2QuotaRef,
    canonical_job_sha256,
)


def legacy_intent_envelope(
    *,
    source_id: str,
    source_authority: SourceBrokerV2AuthorityRef,
    request: bytes,
    deadline: datetime,
    claim: SourceBrokerV2ClaimRef,
    quota: SourceBrokerV2QuotaRef,
    fence: SourceBrokerV2FenceRef,
    lineage: SourceBrokerV2LineageRef,
) -> SourceBrokerV2JobIntentEnvelope:
    normalized_deadline = deadline.astimezone()
    request_hash = canonical_job_sha256(request)
    binding = {
        "claim": claim.model_dump(mode="python"),
        "deadline": normalized_deadline,
        "fence": fence.model_dump(mode="python"),
        "lineage": lineage.model_dump(mode="python"),
        "quota": quota.model_dump(mode="python"),
        "request_hash": request_hash,
        "source_authority": source_authority.model_dump(mode="python"),
        "source_id": source_id,
    }
    operation_id = canonical_job_sha256(
        {"binding": binding, "contract": "rquant-source-broker-v2-job-operation-id/v2"}
    )
    operation_hash = canonical_job_sha256(
        {
            "binding": binding,
            "contract": "rquant-source-broker-v2-job-operation-hash/v2",
            "operation_id": operation_id,
        }
    )
    return SourceBrokerV2JobIntentEnvelope.model_validate(
        {
            "source_id": source_id,
            "source_authority": source_authority,
            "operation_id": operation_id,
            "operation_hash": operation_hash,
            "request": request,
            "request_hash": request_hash,
            "deadline": normalized_deadline,
            "claim": claim,
            "quota": quota,
            "fence": fence,
            "lineage": lineage,
        }
    )
