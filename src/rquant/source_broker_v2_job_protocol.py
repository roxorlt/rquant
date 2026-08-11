"""Canonical, non-executable contracts for the SourceBroker v2 job runner."""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol, Self

from pydantic import ConfigDict, Field, ValidationError, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.scheduler_intent_authorization import SchedulerIntentAuthorizationV1
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

SOURCE_BROKER_V2_JOB_MAX_REQUEST_BYTES = 256 * 1024
SOURCE_BROKER_V2_JOB_MAX_EVIDENCE_BYTES = 512 * 1024
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_SOURCE_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,62}[A-Za-z0-9])?$"
_SAFE_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,198}[A-Za-z0-9])?$"
_FORBIDDEN_TOKENS = frozenset(
    {
        "auth",
        "authorization",
        "callback",
        "class",
        "credential",
        "credentials",
        "exec",
        "executable",
        "factory",
        "function",
        "import",
        "lambda",
        "module",
        "password",
        "pickle",
        "provider",
        "secret",
        "token",
    }
)
_FORBIDDEN_COMPACT_IDENTIFIERS = frozenset(
    {
        "accesskey",
        "accesstoken",
        "apikey",
        "callbackimport",
        "clientsecret",
        "dbpassword",
        "picklepayload",
        "privatekey",
        "runtimecredential",
        "cookie",
        "cookies",
        "sessioncookie",
        "sourcetoken",
    }
)


class SourceBrokerV2JobProtocolError(RuntimeError):
    """A durable SourceBroker v2 job record is malformed or unsafe."""


class _StrictJobModel(RuntimeContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
        str_strip_whitespace=False,
    )


class SourceBrokerV2JobOutcomeStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class SourceBrokerV2AuthorityRef(_StrictJobModel):
    authority_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    key_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    purpose: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    schema_version: int = Field(strict=True, ge=1)
    generation: int = Field(strict=True, ge=1)
    fence_hash: str = Field(pattern=_HASH_PATTERN)


class SourceBrokerV2ClaimRef(_StrictJobModel):
    saga_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    claim_binding_hash: str = Field(pattern=_HASH_PATTERN)
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    claim_plan_hash: str = Field(pattern=_HASH_PATTERN)
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    claim_payload_hash: str = Field(pattern=_HASH_PATTERN)
    authority: SourceBrokerV2AuthorityRef


class SourceBrokerV2QuotaRef(_StrictJobModel):
    parent_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    quota_cost: int = Field(strict=True, ge=1)
    authority: SourceBrokerV2AuthorityRef


class SourceBrokerV2FenceRef(_StrictJobModel):
    owner_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    owner_token_hash: str = Field(pattern=_HASH_PATTERN)
    generation: int = Field(strict=True, ge=1)
    external_root_hash: str = Field(pattern=_HASH_PATTERN)
    claim_token_hash: str = Field(pattern=_HASH_PATTERN)


class SourceBrokerV2LineageRef(_StrictJobModel):
    lineage_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    authority: SourceBrokerV2AuthorityRef


class SourceBrokerV2JobIntentEnvelope(_StrictJobModel):
    """Opaque source intent bound to external authority roots, never executable code."""

    source_id: str = Field(pattern=_SOURCE_ID_PATTERN, min_length=1, max_length=64)
    source_authority: SourceBrokerV2AuthorityRef
    operation_id: str = Field(pattern=_HASH_PATTERN)
    operation_hash: str = Field(pattern=_HASH_PATTERN)
    request: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_JOB_MAX_REQUEST_BYTES)
    request_hash: str = Field(pattern=_HASH_PATTERN)
    deadline: AwareUtcDatetime
    claim: SourceBrokerV2ClaimRef
    quota: SourceBrokerV2QuotaRef
    fence: SourceBrokerV2FenceRef
    lineage: SourceBrokerV2LineageRef
    authorization: SchedulerIntentAuthorizationV1 | None = None
    authorization_payload: bytes | None = Field(default=None, min_length=1)
    authorization_payload_commitment: str | None = Field(default=None, pattern=_HASH_PATTERN)
    authorization_template_commitment: str | None = Field(default=None, pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_binding(self) -> Self:
        request_hash = _canonical_payload_hash(
            self.request,
            label="request",
            reject_forbidden=True,
        )
        if self.request_hash != request_hash:
            raise ValueError("request_hash conflicts with request")
        binding = _intent_binding(
            source_id=self.source_id,
            source_authority=self.source_authority,
            request_hash=self.request_hash,
            deadline=self.deadline,
            claim=self.claim,
            quota=self.quota,
            fence=self.fence,
            lineage=self.lineage,
        )
        operation_id, operation_hash = _operation_identity(binding)
        if self.operation_id != operation_id:
            raise ValueError("operation_id conflicts with canonical intent")
        if self.operation_hash != operation_hash:
            raise ValueError("operation_hash conflicts with canonical intent")
        authorization_fields = (
            self.authorization,
            self.authorization_payload,
            self.authorization_payload_commitment,
            self.authorization_template_commitment,
        )
        if any(value is not None for value in authorization_fields):
            if any(value is None for value in authorization_fields):
                raise ValueError("authorized job intent fields must be complete")
            assert self.authorization is not None
            assert self.authorization_payload is not None
            assert self.authorization_payload_commitment is not None
            assert self.authorization_template_commitment is not None
            payload_commitment = hashlib.sha256(self.authorization_payload).hexdigest()
            if (
                self.authorization_payload_commitment != payload_commitment
                or self.authorization.payload_commitment != payload_commitment
                or self.authorization.template_commitment != self.authorization_template_commitment
            ):
                raise ValueError("authorized job intent commitments conflict")
        return self

    @property
    def intent_hash(self) -> str:
        return canonical_job_model_sha256(self)


class SourceBrokerV2NativeEvidence(_StrictJobModel):
    """Canonical request/receipt pair; authenticity is checked by an injected verifier."""

    kind: Literal["source", "claim", "quota", "lineage"]
    request: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_JOB_MAX_EVIDENCE_BYTES)
    request_hash: str = Field(pattern=_HASH_PATTERN)
    receipt: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_JOB_MAX_EVIDENCE_BYTES)
    receipt_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def create(
        cls,
        *,
        kind: Literal["source", "claim", "quota", "lineage"],
        request: bytes,
        receipt: bytes,
    ) -> SourceBrokerV2NativeEvidence:
        return cls(
            kind=kind,
            request=request,
            request_hash=_canonical_payload_hash(request, label=f"{kind} evidence request"),
            receipt=receipt,
            receipt_hash=_canonical_payload_hash(receipt, label=f"{kind} evidence receipt"),
        )

    @model_validator(mode="after")
    def validate_evidence(self) -> Self:
        if self.request_hash != _canonical_payload_hash(
            self.request,
            label=f"{self.kind} evidence request",
        ):
            raise ValueError(f"{self.kind} evidence request_hash conflicts")
        if self.receipt_hash != _canonical_payload_hash(
            self.receipt,
            label=f"{self.kind} evidence receipt",
        ):
            raise ValueError(f"{self.kind} evidence receipt_hash conflicts")
        return self

    @property
    def request_json(self) -> dict[str, Any]:
        return _canonical_json_object(self.request, label=f"{self.kind} evidence request")

    @property
    def receipt_json(self) -> dict[str, Any]:
        return _canonical_json_object(self.receipt, label=f"{self.kind} evidence receipt")

    @property
    def evidence_hash(self) -> str:
        return canonical_job_model_sha256(self)


class SourceBrokerV2JobOutcomeEnvelope(_StrictJobModel):
    """Structurally bound terminal evidence produced only after external verification."""

    source_id: str = Field(pattern=_SOURCE_ID_PATTERN, min_length=1, max_length=64)
    operation_id: str = Field(pattern=_HASH_PATTERN)
    operation_hash: str = Field(pattern=_HASH_PATTERN)
    outcome_id: str = Field(pattern=_HASH_PATTERN)
    outcome_hash: str = Field(pattern=_HASH_PATTERN)
    evidence_chain_hash: str = Field(pattern=_HASH_PATTERN)
    status: SourceBrokerV2JobOutcomeStatus
    response: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_JOB_MAX_REQUEST_BYTES)
    response_hash: str = Field(pattern=_HASH_PATTERN)
    source_evidence: SourceBrokerV2NativeEvidence
    claim_evidence: SourceBrokerV2NativeEvidence
    quota_evidence: SourceBrokerV2NativeEvidence
    lineage_evidence: SourceBrokerV2NativeEvidence
    source_authority: SourceBrokerV2AuthorityRef
    claim: SourceBrokerV2ClaimRef
    quota: SourceBrokerV2QuotaRef
    fence: SourceBrokerV2FenceRef
    lineage: SourceBrokerV2LineageRef

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        evidence = (
            ("source", self.source_evidence),
            ("claim", self.claim_evidence),
            ("quota", self.quota_evidence),
            ("lineage", self.lineage_evidence),
        )
        for expected_kind, item in evidence:
            if item.kind != expected_kind:
                raise ValueError(f"{expected_kind} evidence kind conflicts")
        response_hash = _canonical_payload_hash(
            self.response,
            label="outcome response",
            reject_forbidden=True,
        )
        if self.response_hash != response_hash:
            raise ValueError("response_hash conflicts with outcome response")
        evidence_chain_hash = _evidence_chain_hash(
            operation_hash=self.operation_hash,
            response_hash=self.response_hash,
            source_evidence=self.source_evidence,
            claim_evidence=self.claim_evidence,
            quota_evidence=self.quota_evidence,
            lineage_evidence=self.lineage_evidence,
        )
        if self.evidence_chain_hash != evidence_chain_hash:
            raise ValueError("evidence_chain_hash conflicts with four-receipt chain")
        outcome_id = _outcome_id(
            operation_hash=self.operation_hash,
            status=self.status,
            evidence_chain_hash=evidence_chain_hash,
        )
        if self.outcome_id != outcome_id:
            raise ValueError("outcome_id conflicts with outcome evidence")
        outcome_hash = _outcome_hash(
            source_id=self.source_id,
            operation_id=self.operation_id,
            operation_hash=self.operation_hash,
            status=self.status,
            response_hash=self.response_hash,
            evidence_chain_hash=evidence_chain_hash,
            source_authority=self.source_authority,
            claim=self.claim,
            quota=self.quota,
            fence=self.fence,
            lineage=self.lineage,
        )
        if self.outcome_hash != outcome_hash:
            raise ValueError("outcome_hash conflicts with authority references")
        return self


class SourceBrokerV2JobOutcomeVerifier(Protocol):
    def verify_source(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        response: bytes,
        status: SourceBrokerV2JobOutcomeStatus,
        deadline: float,
    ) -> None: ...

    def verify_claim(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None: ...

    def verify_quota(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None: ...

    def verify_lineage(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        source_receipt_hash: str,
        claim_receipt_hash: str,
        quota_receipt_hash: str,
        deadline: float,
    ) -> None: ...


def build_verified_job_outcome(
    *,
    intent: SourceBrokerV2JobIntentEnvelope,
    status: SourceBrokerV2JobOutcomeStatus,
    response: bytes,
    source_evidence: SourceBrokerV2NativeEvidence,
    claim_evidence: SourceBrokerV2NativeEvidence,
    quota_evidence: SourceBrokerV2NativeEvidence,
    lineage_evidence: SourceBrokerV2NativeEvidence,
    verifier: SourceBrokerV2JobOutcomeVerifier,
    deadline: float,
) -> SourceBrokerV2JobOutcomeEnvelope:
    """Verify all native authorities, then build one inseparable evidence chain."""

    if verifier is None:
        raise TypeError("a native evidence verifier is required")
    if type(deadline) not in {float, int} or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError("verification deadline must be a positive finite value")
    verifier.verify_source(
        intent=intent,
        evidence=source_evidence,
        response=response,
        status=status,
        deadline=float(deadline),
    )
    verifier.verify_claim(intent=intent, evidence=claim_evidence, deadline=float(deadline))
    verifier.verify_quota(intent=intent, evidence=quota_evidence, deadline=float(deadline))
    verifier.verify_lineage(
        intent=intent,
        evidence=lineage_evidence,
        source_receipt_hash=source_evidence.receipt_hash,
        claim_receipt_hash=claim_evidence.receipt_hash,
        quota_receipt_hash=quota_evidence.receipt_hash,
        deadline=float(deadline),
    )
    response_hash = _canonical_payload_hash(
        response,
        label="outcome response",
        reject_forbidden=True,
    )
    evidence_chain_hash = _evidence_chain_hash(
        operation_hash=intent.operation_hash,
        response_hash=response_hash,
        source_evidence=source_evidence,
        claim_evidence=claim_evidence,
        quota_evidence=quota_evidence,
        lineage_evidence=lineage_evidence,
    )
    outcome_id = _outcome_id(
        operation_hash=intent.operation_hash,
        status=status,
        evidence_chain_hash=evidence_chain_hash,
    )
    outcome_hash = _outcome_hash(
        source_id=intent.source_id,
        operation_id=intent.operation_id,
        operation_hash=intent.operation_hash,
        status=status,
        response_hash=response_hash,
        evidence_chain_hash=evidence_chain_hash,
        source_authority=intent.source_authority,
        claim=intent.claim,
        quota=intent.quota,
        fence=intent.fence,
        lineage=intent.lineage,
    )
    return SourceBrokerV2JobOutcomeEnvelope(
        source_id=intent.source_id,
        operation_id=intent.operation_id,
        operation_hash=intent.operation_hash,
        outcome_id=outcome_id,
        outcome_hash=outcome_hash,
        evidence_chain_hash=evidence_chain_hash,
        status=status,
        response=response,
        response_hash=response_hash,
        source_evidence=source_evidence,
        claim_evidence=claim_evidence,
        quota_evidence=quota_evidence,
        lineage_evidence=lineage_evidence,
        source_authority=intent.source_authority,
        claim=intent.claim,
        quota=intent.quota,
        fence=intent.fence,
        lineage=intent.lineage,
    )


def canonical_request_bytes(value: Any) -> bytes:
    """Encode an opaque value as duplicate-free canonical JSON bytes."""

    payload = canonical_json_bytes(value)
    strict_canonical_json_loads(payload)
    return payload


def require_safe_canonical_request_bytes(value: bytes, *, label: str = "request") -> bytes:
    """Reject non-canonical or capability-bearing request structures without echoing data."""

    _canonical_payload_hash(value, label=label, reject_forbidden=True)
    return value


def canonical_job_sha256(value: object) -> str:
    if isinstance(value, bytes | bytearray):
        return hashlib.sha256(bytes(value)).hexdigest()
    return canonical_sha256(value)


def canonical_job_model_bytes(value: _StrictJobModel) -> bytes:
    return canonical_model_json_bytes(value)


def canonical_job_model_sha256(value: _StrictJobModel) -> str:
    return canonical_job_sha256(canonical_job_model_bytes(value))


def parse_job_intent(payload: bytes) -> SourceBrokerV2JobIntentEnvelope:
    return _parse_model(SourceBrokerV2JobIntentEnvelope, payload, "job intent")


def parse_job_outcome(payload: bytes) -> SourceBrokerV2JobOutcomeEnvelope:
    return _parse_model(SourceBrokerV2JobOutcomeEnvelope, payload, "job outcome")


def _intent_binding(
    *,
    source_id: str,
    source_authority: SourceBrokerV2AuthorityRef,
    request_hash: str,
    deadline: datetime,
    claim: SourceBrokerV2ClaimRef,
    quota: SourceBrokerV2QuotaRef,
    fence: SourceBrokerV2FenceRef,
    lineage: SourceBrokerV2LineageRef,
) -> dict[str, object]:
    return {
        "claim": claim.model_dump(mode="python"),
        "deadline": deadline,
        "fence": fence.model_dump(mode="python"),
        "lineage": lineage.model_dump(mode="python"),
        "quota": quota.model_dump(mode="python"),
        "request_hash": request_hash,
        "source_authority": source_authority.model_dump(mode="python"),
        "source_id": source_id,
    }


def _operation_identity(binding: dict[str, object]) -> tuple[str, str]:
    operation_id = canonical_job_sha256(
        {
            "binding": binding,
            "contract": "rquant-source-broker-v2-job-operation-id/v2",
        }
    )
    operation_hash = canonical_job_sha256(
        {
            "binding": binding,
            "contract": "rquant-source-broker-v2-job-operation-hash/v2",
            "operation_id": operation_id,
        }
    )
    return operation_id, operation_hash


def _evidence_chain_hash(
    *,
    operation_hash: str,
    response_hash: str,
    source_evidence: SourceBrokerV2NativeEvidence,
    claim_evidence: SourceBrokerV2NativeEvidence,
    quota_evidence: SourceBrokerV2NativeEvidence,
    lineage_evidence: SourceBrokerV2NativeEvidence,
) -> str:
    return canonical_job_sha256(
        {
            "claim_evidence_hash": claim_evidence.evidence_hash,
            "contract": "rquant-source-broker-v2-four-authority-chain/v2",
            "lineage_evidence_hash": lineage_evidence.evidence_hash,
            "operation_hash": operation_hash,
            "quota_evidence_hash": quota_evidence.evidence_hash,
            "response_hash": response_hash,
            "source_evidence_hash": source_evidence.evidence_hash,
        }
    )


def _outcome_id(
    *, operation_hash: str, status: SourceBrokerV2JobOutcomeStatus, evidence_chain_hash: str
) -> str:
    return canonical_job_sha256(
        {
            "contract": "rquant-source-broker-v2-job-outcome-id/v2",
            "evidence_chain_hash": evidence_chain_hash,
            "operation_hash": operation_hash,
            "status": status.value,
        }
    )


def _outcome_hash(
    *,
    source_id: str,
    operation_id: str,
    operation_hash: str,
    status: SourceBrokerV2JobOutcomeStatus,
    response_hash: str,
    evidence_chain_hash: str,
    source_authority: SourceBrokerV2AuthorityRef,
    claim: SourceBrokerV2ClaimRef,
    quota: SourceBrokerV2QuotaRef,
    fence: SourceBrokerV2FenceRef,
    lineage: SourceBrokerV2LineageRef,
) -> str:
    return canonical_job_sha256(
        {
            "claim": claim.model_dump(mode="python"),
            "contract": "rquant-source-broker-v2-job-outcome-hash/v2",
            "evidence_chain_hash": evidence_chain_hash,
            "fence": fence.model_dump(mode="python"),
            "lineage": lineage.model_dump(mode="python"),
            "operation_hash": operation_hash,
            "operation_id": operation_id,
            "quota": quota.model_dump(mode="python"),
            "response_hash": response_hash,
            "source_authority": source_authority.model_dump(mode="python"),
            "source_id": source_id,
            "status": status.value,
        }
    )


def _canonical_payload_hash(
    payload: bytes,
    *,
    label: str,
    reject_forbidden: bool = False,
) -> str:
    value = _canonical_json_value(payload, label=label)
    if reject_forbidden:
        _reject_forbidden_fields(value)
    return canonical_job_sha256(payload)


def _canonical_json_value(payload: bytes, *, label: str) -> object:
    if type(payload) is not bytes:
        raise ValueError(f"{label} must be bytes")
    try:
        return strict_canonical_json_loads(payload)
    except (StrictJsonError, UnicodeError, ValueError, TypeError) as exc:
        raise ValueError(f"{label} must be canonical JSON bytes") from exc


def _canonical_json_object(payload: bytes, *, label: str) -> dict[str, Any]:
    value = _canonical_json_value(payload, label=label)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a canonical object")
    return value


def _parse_model(model: type[_StrictJobModel], payload: bytes, label: str) -> Any:
    try:
        return strict_model_validate_canonical_json(model, payload)
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceBrokerV2JobProtocolError(f"source broker v2 {label} is malformed") from exc


def _require_aware_deadline(deadline: datetime) -> datetime:
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise ValueError("deadline must be timezone-aware")
    return deadline.astimezone(UTC)


def _canonical_identifier_parts(key: str) -> tuple[tuple[str, ...], str]:
    normalized = unicodedata.normalize("NFKC", key)
    normalized = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", normalized)
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized)
    parts = tuple(re.findall(r"[a-z0-9]+", normalized.casefold()))
    return parts, "".join(parts)


def _reject_forbidden_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("canonical JSON object keys must be strings")
            parts, compact = _canonical_identifier_parts(key)
            if (
                key.startswith("__")
                or _FORBIDDEN_TOKENS.intersection(parts)
                or (compact in _FORBIDDEN_COMPACT_IDENTIFIERS)
            ):
                raise ValueError(f"forbidden executable or credential field: {key}")
            _reject_forbidden_fields(item)
    elif isinstance(value, list):
        for item in value:
            _reject_forbidden_fields(item)
