"""Versioned, signed source-operation contracts independent of worker and broker runtime."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self
from uuid import UUID

from pydantic import Field, StringConstraints, ValidationError, model_validator

from rquant.adapter_manifest import (
    SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE,
    SOURCE_USE_PLAN_V2_NAMESPACE,
    AdapterManifest,
    Ed25519ContractSigner,
    VerifyOnlyEd25519Keyring,
    _Ed25519SignedContract,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.scheduler_intent_authorization import SchedulerIntentAuthorizationV1
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2ClaimRef,
    SourceBrokerV2FenceRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2LineageRef,
    SourceBrokerV2QuotaRef,
    canonical_job_sha256,
    canonical_request_bytes,
    require_safe_canonical_request_bytes,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_json_loads,
)

if TYPE_CHECKING:
    from rquant.lab_shard_protocol import LabShardClaimV2, StrategyShardPayloadV2

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_OPERATION_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,198}[A-Za-z0-9])?$"
_PUBLIC_TS_CODE = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$"),
]
_PUBLIC_REQUEST_FIELD = Literal[
    "ts_code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]


class SourceOperationContractError(ValueError):
    """Raised when a v2 source operation contract cannot be used safely."""


class SourceAttemptBindingV2(RuntimeContractModel):
    """Identity of one scheduler-owned external-source attempt."""

    schema_version: Literal[2] = 2
    job_id: UUID
    run_id: UUID | None = None
    spec_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    shard_id: UUID
    attempt_id: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    worker_id: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_run_identity(self) -> Self:
        if (self.run_id is None) == (self.spec_hash is None):
            raise ValueError("source attempt requires exactly one of run_id or spec_hash")
        return self

    @property
    def attempt_identity_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SourceResourceRequestV2(RuntimeContractModel):
    """Closed resource request deliberately bound to an external manifest."""

    schema_version: Literal[2] = 2
    source: str = Field(min_length=1, max_length=200)
    operation: str = Field(min_length=1, max_length=200)
    cost_per_call: int = Field(strict=True, ge=1)
    requested_calls: int = Field(strict=True, ge=1)

    @classmethod
    def from_manifest(
        cls,
        manifest: AdapterManifest,
        *,
        requested_calls: int,
    ) -> SourceResourceRequestV2:
        validated_manifest = AdapterManifest.model_validate(manifest)
        if validated_manifest.network != "provider":
            raise ValueError("source resource request requires a provider manifest")
        return cls(
            source=validated_manifest.source or "",
            operation=validated_manifest.operation or "",
            cost_per_call=validated_manifest.cost_per_call,
            requested_calls=requested_calls,
        )

    @property
    def resource_request_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SourceIntentV2(RuntimeContractModel):
    """Planner-time external-source intent with a signed manifest and no open fields."""

    schema_version: Literal[2] = 2
    network: Literal["provider"] = "provider"
    manifest: AdapterManifest
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    resource_request: SourceResourceRequestV2
    resource_request_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_manifest(
        cls,
        manifest: AdapterManifest,
        *,
        resource_request: SourceResourceRequestV2,
    ) -> SourceIntentV2:
        return cls(
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            resource_request=resource_request,
            resource_request_hash=resource_request.resource_request_hash,
        )

    @model_validator(mode="after")
    def validate_source_intent(self) -> Self:
        manifest = self.manifest
        request = self.resource_request
        if manifest.network != "provider":
            raise ValueError("source intent requires a provider manifest")
        if self.manifest_hash != manifest.manifest_hash:
            raise ValueError("source intent manifest_hash does not match manifest")
        if self.resource_request_hash != request.resource_request_hash:
            raise ValueError("source intent resource_request_hash does not match request")
        if (
            request.source != manifest.source
            or request.operation != manifest.operation
            or request.cost_per_call != manifest.cost_per_call
        ):
            raise ValueError("source intent resource request conflicts with manifest")
        if request.requested_calls > manifest.max_calls:
            raise ValueError("source intent requested_calls exceeds manifest max_calls")
        return self

    @property
    def source_contract_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))

    def verify(self, keyring: VerifyOnlyEd25519Keyring) -> bool:
        return self.manifest.verify(keyring)

    def require_verified(self, keyring: VerifyOnlyEd25519Keyring) -> None:
        try:
            validated = SourceIntentV2.model_validate(self)
        except ValidationError as exc:
            raise SourceOperationContractError(f"source intent is invalid: {exc}") from exc
        if not validated.verify(keyring):
            raise SourceOperationContractError("source intent manifest signature is invalid")


class SourceBrokerV2SchedulerIntentTemplate(RuntimeContractModel):
    """Public immutable input for deriving one manifest-bound broker job intent."""

    schema_version: Literal[2] = 2
    source_intent: SourceIntentV2
    source_contract_hash: str = Field(pattern=_HASH_PATTERN)
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    resource_request_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    adapter_code_hash: str = Field(pattern=_HASH_PATTERN)
    source_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=_OPERATION_ID_PATTERN,
    )
    request: SourceBrokerV2PublicRequest
    request_hash: str = Field(pattern=_HASH_PATTERN)
    deadline_offset_seconds: int = Field(strict=True, ge=1, le=86_400)
    saga_id: str = Field(min_length=1, max_length=200, pattern=_OPERATION_ID_PATTERN)
    source_authority: SourceBrokerV2AuthorityRef
    claim_authority: SourceBrokerV2AuthorityRef
    quota_parent_id: str = Field(
        min_length=1,
        max_length=200,
        pattern=_OPERATION_ID_PATTERN,
    )
    quota_authority: SourceBrokerV2AuthorityRef
    lineage_id: str = Field(
        min_length=1,
        max_length=200,
        pattern=_OPERATION_ID_PATTERN,
    )
    lineage_authority: SourceBrokerV2AuthorityRef
    fence_external_root_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_source_intent(
        cls,
        *,
        source_intent: SourceIntentV2,
        source_id: str,
        request: SourceBrokerV2PublicRequest,
        deadline_offset_seconds: int,
        saga_id: str,
        source_authority: SourceBrokerV2AuthorityRef,
        claim_authority: SourceBrokerV2AuthorityRef,
        quota_parent_id: str,
        quota_authority: SourceBrokerV2AuthorityRef,
        lineage_id: str,
        lineage_authority: SourceBrokerV2AuthorityRef,
        fence_external_root_hash: str,
    ) -> SourceBrokerV2SchedulerIntentTemplate:
        validated_intent = SourceIntentV2.model_validate(source_intent, strict=True)
        return cls(
            source_intent=validated_intent,
            source_contract_hash=validated_intent.source_contract_hash,
            manifest_hash=validated_intent.manifest_hash,
            resource_request_hash=validated_intent.resource_request_hash,
            adapter_id=validated_intent.manifest.adapter_id,
            adapter_version=validated_intent.manifest.adapter_version,
            adapter_code_hash=validated_intent.manifest.adapter_code_hash,
            source_id=source_id,
            request=request,
            request_hash=canonical_job_sha256(request.canonical_bytes),
            deadline_offset_seconds=deadline_offset_seconds,
            saga_id=saga_id,
            source_authority=source_authority,
            claim_authority=claim_authority,
            quota_parent_id=quota_parent_id,
            quota_authority=quota_authority,
            lineage_id=lineage_id,
            lineage_authority=lineage_authority,
            fence_external_root_hash=fence_external_root_hash,
        )

    @model_validator(mode="after")
    def validate_template(self) -> Self:
        manifest = self.source_intent.manifest
        if (
            self.source_contract_hash != self.source_intent.source_contract_hash
            or self.manifest_hash != self.source_intent.manifest_hash
            or self.resource_request_hash != self.source_intent.resource_request_hash
        ):
            raise ValueError("scheduler intent template source contract hashes conflict")
        if (
            self.adapter_id,
            self.adapter_version,
            self.adapter_code_hash,
        ) != (
            manifest.adapter_id,
            manifest.adapter_version,
            manifest.adapter_code_hash,
        ):
            raise ValueError("scheduler intent template adapter identity conflicts with manifest")
        if self.source_id != manifest.source:
            raise ValueError("scheduler intent template source_id conflicts with manifest")
        if self.request.dataset != manifest.operation:
            raise ValueError("scheduler intent template request dataset conflicts with manifest")
        if self.request_hash != canonical_job_sha256(self.request.canonical_bytes):
            raise ValueError("scheduler intent template request_hash conflicts with request")
        try:
            require_safe_canonical_request_bytes(self.request.canonical_bytes)
            require_safe_canonical_request_bytes(
                canonical_json_bytes(self.model_dump(mode="json")),
                label="scheduler intent template",
            )
        except ValueError as exc:
            raise ValueError("scheduler intent template contains a forbidden field") from exc
        if (
            self.source_intent.resource_request.requested_calls
            * self.source_intent.resource_request.cost_per_call
            < 1
        ):
            raise ValueError("scheduler intent template quota cost is invalid")
        return self


class SourceBrokerV2PublicRequest(RuntimeContractModel):
    """The entire public, typed request surface available to scheduler templates."""

    schema_version: Literal[2] = 2
    dataset: Literal["daily_bars"] = "daily_bars"
    symbols: tuple[_PUBLIC_TS_CODE, ...] = Field(min_length=1, max_length=500)
    frequency: Literal["1d", "1min", "5min", "15min", "30min", "60min"] = "1d"
    requested_start: date
    requested_end: date
    as_of: date
    fields: tuple[_PUBLIC_REQUEST_FIELD, ...] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        if self.requested_start > self.requested_end:
            raise ValueError("public request requested_start must not exceed requested_end")
        if self.as_of < self.requested_end:
            raise ValueError("public request as_of must not precede requested_end")
        if len(set(self.symbols)) != len(self.symbols) or len(set(self.fields)) != len(self.fields):
            raise ValueError("public request symbols and fields must be unique")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_request_bytes(self.model_dump(mode="json"))


def issue_scheduler_intent_authorization_v1(
    payload: StrategyShardPayloadV2,
    *,
    signer: Ed25519ContractSigner,
    valid_from: datetime,
    expires_at: datetime,
) -> SchedulerIntentAuthorizationV1:
    """Issue a detached, signed authority for one exact public source payload."""

    from rquant.lab_shard_protocol import StrategyShardPayloadV2

    if type(payload) is not StrategyShardPayloadV2:
        raise TypeError("scheduler intent authorization requires an exact v2 payload")
    if signer.key_purpose != "scheduler_intent_authorization":
        raise ValueError("scheduler intent authorization signer purpose is invalid")
    commitments = _scheduler_intent_authorization_commitments(payload)
    unsigned = SchedulerIntentAuthorizationV1(
        authority_id=signer.issuer,
        issuer=signer.issuer,
        key_id=signer.key_id,
        **commitments,
        valid_from=normalize_aware_utc(valid_from),
        expires_at=normalize_aware_utc(expires_at),
        signature="",
    )
    return unsigned.model_copy(
        update={
            "signature": signer.sign(
                namespace=SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE,
                payload=unsigned.signing_preimage(),
            )
        }
    )


def require_scheduler_intent_authorization_v1(
    payload: StrategyShardPayloadV2,
    *,
    keyring: VerifyOnlyEd25519Keyring,
    now: datetime,
) -> SchedulerIntentAuthorizationV1:
    """Require a currently valid signature bound to every payload-side commitment."""

    from rquant.lab_shard_protocol import StrategyShardPayloadV2

    if type(payload) is not StrategyShardPayloadV2:
        raise TypeError("scheduler intent authorization requires an exact v2 payload")
    authorization = payload.scheduler_intent_authorization
    if authorization is None:
        raise SourceOperationContractError("scheduler intent authorization is required")
    try:
        authorization.require_verified(keyring, now=now)
    except ValueError as exc:
        raise SourceOperationContractError(str(exc)) from exc
    for field, commitment in _scheduler_intent_authorization_commitments(payload).items():
        if getattr(authorization, field) != commitment:
            raise SourceOperationContractError(
                f"scheduler intent authorization {field} conflicts with payload"
            )
    return authorization


def _scheduler_intent_authorization_commitments(
    payload: StrategyShardPayloadV2,
) -> dict[str, str]:
    template = payload.scheduler_intent_template
    if template is None:
        raise SourceOperationContractError(
            "scheduler intent authorization requires a scheduler template"
        )
    quota_cost = (
        template.source_intent.resource_request.cost_per_call
        * template.source_intent.resource_request.requested_calls
    )
    return {
        "payload_commitment": payload.authorization_payload_commitment,
        "template_commitment": canonical_sha256(template.model_dump(mode="python")),
        "public_request_commitment": template.request_hash,
        "manifest_commitment": template.manifest_hash,
        "source_contract_commitment": template.source_contract_hash,
        "resource_quota_commitment": canonical_sha256(
            {
                "authority": template.quota_authority.model_dump(mode="python"),
                "cost": quota_cost,
                "parent_id": template.quota_parent_id,
            }
        ),
        "lineage_authority_commitment": canonical_sha256(
            template.lineage_authority.model_dump(mode="python")
        ),
    }


def require_authorized_source_broker_v2_job_intent(
    intent: SourceBrokerV2JobIntentEnvelope,
    *,
    manifest_keyring: VerifyOnlyEd25519Keyring,
    authorization_keyring: VerifyOnlyEd25519Keyring,
    now: datetime,
) -> SourceBrokerV2JobIntentEnvelope:
    """Verify an executable envelope without any scheduler or issuer dependency."""

    from rquant.lab_shard_protocol import StrategyShardPayloadV2

    try:
        validated_intent = SourceBrokerV2JobIntentEnvelope.model_validate(intent, strict=True)
    except (TypeError, ValueError, ValidationError) as exc:
        raise SourceOperationContractError("source broker job intent is invalid") from exc
    if (
        validated_intent.authorization is None
        or validated_intent.authorization_payload is None
        or validated_intent.authorization_payload_commitment is None
        or validated_intent.authorization_template_commitment is None
    ):
        raise SourceOperationContractError("source broker job intent authorization is required")
    try:
        payload = StrategyShardPayloadV2.model_validate_json(validated_intent.authorization_payload)
    except (TypeError, ValueError, ValidationError) as exc:
        raise SourceOperationContractError(
            "source broker job authorization payload is invalid"
        ) from exc
    if payload.scheduler_intent_authorization is not None:
        raise SourceOperationContractError(
            "source broker job authorization payload must exclude authorization"
        )
    if payload.authorization_payload_bytes != validated_intent.authorization_payload:
        raise SourceOperationContractError(
            "source broker job authorization payload is not canonical"
        )
    authorized_payload = payload.with_scheduler_intent_authorization(validated_intent.authorization)
    try:
        authorized_payload.source_intent.require_verified(manifest_keyring)
    except Exception as exc:
        raise SourceOperationContractError(
            "source broker job manifest signature is invalid"
        ) from exc
    authorization = require_scheduler_intent_authorization_v1(
        authorized_payload,
        keyring=authorization_keyring,
        now=now,
    )
    template = authorized_payload.scheduler_intent_template
    assert template is not None
    quota_cost = (
        template.source_intent.resource_request.cost_per_call
        * template.source_intent.resource_request.requested_calls
    )
    if (
        validated_intent.source_id != template.source_id
        or validated_intent.source_authority != template.source_authority
        or validated_intent.request != template.request.canonical_bytes
        or validated_intent.request_hash != authorization.public_request_commitment
        or validated_intent.claim.manifest_hash != authorization.manifest_commitment
        or validated_intent.quota.parent_id != template.quota_parent_id
        or validated_intent.quota.quota_cost != quota_cost
        or validated_intent.quota.authority != template.quota_authority
        or validated_intent.lineage.authority != template.lineage_authority
        or validated_intent.lineage.lineage_id != template.lineage_id
        or validated_intent.claim.authority != template.claim_authority
        or validated_intent.fence.external_root_hash != template.fence_external_root_hash
        or validated_intent.deadline > authorization.expires_at
        or validated_intent.deadline <= normalize_aware_utc(now)
    ):
        raise SourceOperationContractError(
            "source broker job intent conflicts with scheduler authorization"
        )
    return validated_intent


def build_source_broker_v2_scheduler_intent(
    payload: StrategyShardPayloadV2,
    *,
    claim: LabShardClaimV2,
    manifest_keyring: VerifyOnlyEd25519Keyring,
    authorization_keyring: VerifyOnlyEd25519Keyring,
    deadline: datetime,
    now: datetime,
) -> SourceBrokerV2JobIntentEnvelope:
    """Build one scheduler intent from a claim-bound canonical payload and public keyring."""

    from rquant.lab_shard_protocol import LabShardClaimV2, StrategyShardPayloadV2

    try:
        if type(payload) is not StrategyShardPayloadV2:
            raise TypeError("scheduler intent requires a v2 strategy shard payload")
        validated_payload = StrategyShardPayloadV2.model_validate(payload, strict=True)
        payload_bytes = canonical_model_json_bytes(validated_payload)
        validated_claim = LabShardClaimV2.model_validate(claim, strict=True)
    except (TypeError, ValueError) as exc:
        raise SourceOperationContractError("scheduler intent payload or claim is invalid") from exc
    if validated_claim.source_use_plan is not None:
        raise SourceOperationContractError("scheduler intent requires an unbound claim")
    if (
        payload_bytes.decode("utf-8") != validated_claim.definition.payload_json
        or canonical_job_sha256(payload_bytes) != validated_claim.definition.payload_hash
    ):
        raise SourceOperationContractError("scheduler intent payload conflicts with claim")
    if validated_claim.strategy_payload != validated_payload:
        raise SourceOperationContractError("scheduler intent payload conflicts with claim")
    template = validated_payload.scheduler_intent_template
    if template is None:
        raise SourceOperationContractError("scheduler broker intent template is required")
    try:
        validated_payload.source_intent.require_verified(manifest_keyring)
    except Exception as exc:
        raise SourceOperationContractError(
            "scheduler intent manifest signature is invalid"
        ) from exc
    temporal_policy = validated_payload.source_intent.manifest.temporal_policy
    current = normalize_aware_utc(now)
    if temporal_policy is None:
        raise SourceOperationContractError(
            "scheduler intent requires a signed manifest temporal policy"
        )
    if not temporal_policy.valid_from <= current < temporal_policy.expires_at:
        raise SourceOperationContractError(
            "scheduler intent manifest temporal policy is not current"
        )
    if normalize_aware_utc(deadline) > temporal_policy.expires_at:
        raise SourceOperationContractError("scheduler intent deadline exceeds manifest expiry")
    latest_available = temporal_policy.latest_available_at(current).date()
    request = template.request
    if request.requested_end > latest_available or request.as_of > latest_available:
        raise SourceOperationContractError(
            "scheduler intent request exceeds signed data availability"
        )
    validated_template = SourceBrokerV2SchedulerIntentTemplate.model_validate(template, strict=True)
    authorization = require_scheduler_intent_authorization_v1(
        validated_payload,
        keyring=authorization_keyring,
        now=current,
    )
    validated_intent = SourceIntentV2.model_validate(validated_payload.source_intent, strict=True)
    validated_attempt = SourceAttemptBindingV2.model_validate(
        validated_claim.attempt_binding,
        strict=True,
    )
    if validated_template.source_intent != validated_intent:
        raise SourceOperationContractError("scheduler intent source intent conflicts with template")
    if validated_claim.strategy_payload.source_intent != validated_intent:
        raise SourceOperationContractError("scheduler intent source intent conflicts with claim")
    if validated_claim.attempt_binding != validated_attempt:
        raise SourceOperationContractError("scheduler intent attempt conflicts with claim")
    expected_deadline = validated_claim.claimed_at + timedelta(
        seconds=validated_template.deadline_offset_seconds
    )
    normalized_deadline = normalize_aware_utc(deadline)
    if (
        normalized_deadline != expected_deadline
        or normalized_deadline > validated_claim.lease_expires_at
        or normalized_deadline > authorization.expires_at
    ):
        raise SourceOperationContractError("scheduler intent deadline conflicts with frozen claim")
    if (
        validated_template.manifest_hash != validated_claim.manifest_hash
        or validated_template.adapter_code_hash != validated_claim.adapter_code_hash
        or validated_template.source_contract_hash != validated_claim.payload_source_contract_hash
        or validated_template.adapter_id != validated_claim.definition.adapter_id
        or validated_template.adapter_version != validated_claim.definition.adapter_version
    ):
        raise SourceOperationContractError(
            "scheduler intent template conflicts with claim manifest"
        )
    claim_binding_hash = canonical_sha256(
        {
            "attempt_binding": validated_attempt,
            "claim_token": validated_claim.claim_token,
            "contract": "rquant-lab-source-stage-binding/v1",
            "plan_hash": validated_claim.definition.plan_hash,
        }
    )
    claim_token_hash = canonical_sha256({"claim_token": validated_claim.claim_token})
    try:
        claim_ref = SourceBrokerV2ClaimRef(
            saga_id=validated_template.saga_id,
            claim_binding_hash=claim_binding_hash,
            claim_generation=validated_attempt.claim_generation,
            scheduler_fencing_token=validated_attempt.scheduler_fencing_token,
            attempt_identity_hash=validated_attempt.attempt_identity_hash,
            claim_plan_hash=validated_claim.definition.plan_hash,
            manifest_hash=validated_claim.manifest_hash,
            claim_payload_hash=validated_claim.definition.payload_hash,
            authority=validated_template.claim_authority,
        )
        quota_ref = SourceBrokerV2QuotaRef(
            parent_id=validated_template.quota_parent_id,
            quota_cost=(
                validated_intent.resource_request.cost_per_call
                * validated_intent.resource_request.requested_calls
            ),
            authority=validated_template.quota_authority,
        )
        fence_ref = SourceBrokerV2FenceRef(
            owner_id=validated_attempt.worker_id,
            owner_token_hash=claim_token_hash,
            generation=validated_attempt.claim_generation,
            external_root_hash=validated_template.fence_external_root_hash,
            claim_token_hash=claim_token_hash,
        )
        lineage_ref = SourceBrokerV2LineageRef(
            lineage_id=validated_template.lineage_id,
            authority=validated_template.lineage_authority,
        )
        request_bytes = validated_template.request.canonical_bytes
        request_hash = canonical_job_sha256(request_bytes)
        binding = {
            "claim": claim_ref.model_dump(mode="python"),
            "deadline": normalized_deadline,
            "fence": fence_ref.model_dump(mode="python"),
            "lineage": lineage_ref.model_dump(mode="python"),
            "quota": quota_ref.model_dump(mode="python"),
            "request_hash": request_hash,
            "source_authority": validated_template.source_authority.model_dump(mode="python"),
            "source_id": validated_template.source_id,
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
        return SourceBrokerV2JobIntentEnvelope(
            source_id=validated_template.source_id,
            source_authority=validated_template.source_authority,
            operation_id=operation_id,
            operation_hash=operation_hash,
            request=request_bytes,
            request_hash=request_hash,
            deadline=normalized_deadline,
            claim=claim_ref,
            quota=quota_ref,
            fence=fence_ref,
            lineage=lineage_ref,
            authorization=authorization,
            authorization_payload=validated_payload.authorization_payload_bytes,
            authorization_payload_commitment=validated_payload.authorization_payload_commitment,
            authorization_template_commitment=authorization.template_commitment,
        )
    except ValueError as exc:
        raise SourceOperationContractError(
            f"scheduler intent broker envelope is invalid: {exc}"
        ) from exc


class SourceUsePlanV2(_Ed25519SignedContract):
    """Attempt-bound v2 authorization for one source-broker operation."""

    schema_version: Literal[2] = 2
    key_purpose: Literal["source_use_plan_v2"] = "source_use_plan_v2"
    audience: str = Field(min_length=1, max_length=200)
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    lease_expires_at: AwareUtcDatetime
    nonce: str = Field(min_length=1, max_length=500)
    single_use_authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(
        min_length=1,
        max_length=200,
        pattern=_OPERATION_ID_PATTERN,
    )
    attempt_binding: SourceAttemptBindingV2
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    adapter_code_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    payload_source_contract_hash: str = Field(pattern=_HASH_PATTERN)
    source_intent: SourceIntentV2
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    resource_request_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_source_intent(
        cls,
        source_intent: SourceIntentV2,
        *,
        issuer: str,
        key_id: str,
        attempt_binding: SourceAttemptBindingV2,
        adapter_id: str,
        adapter_version: str,
        adapter_code_hash: str,
        payload_hash: str,
        payload_source_contract_hash: str,
        operation_id: str,
        audience: str,
        not_before: datetime,
        expires_at: datetime,
        lease_expires_at: datetime,
        nonce: str,
        single_use_authority_id: str,
    ) -> SourceUsePlanV2:
        return cls(
            issuer=issuer,
            key_id=key_id,
            signature="",
            audience=audience,
            not_before=not_before,
            expires_at=expires_at,
            lease_expires_at=lease_expires_at,
            nonce=nonce,
            single_use_authority_id=single_use_authority_id,
            operation_id=operation_id,
            attempt_binding=attempt_binding,
            attempt_identity_hash=attempt_binding.attempt_identity_hash,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            adapter_code_hash=adapter_code_hash,
            payload_hash=payload_hash,
            payload_source_contract_hash=payload_source_contract_hash,
            source_intent=source_intent,
            manifest_hash=source_intent.manifest_hash,
            resource_request_hash=source_intent.resource_request_hash,
        )

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        if self.expires_at <= self.not_before:
            raise ValueError("expires_at must follow not_before")
        if self.lease_expires_at <= self.not_before:
            raise ValueError("lease_expires_at must follow not_before")
        if self.expires_at > self.lease_expires_at:
            raise ValueError("source plan cannot outlive its claim lease")
        if self.attempt_identity_hash != self.attempt_binding.attempt_identity_hash:
            raise ValueError("source plan attempt_identity_hash does not match binding")
        manifest = self.source_intent.manifest
        if (
            self.adapter_id,
            self.adapter_version,
            self.adapter_code_hash,
        ) != (
            manifest.adapter_id,
            manifest.adapter_version,
            manifest.adapter_code_hash,
        ):
            raise ValueError("source plan adapter identity conflicts with manifest")
        if self.payload_source_contract_hash != self.source_intent.source_contract_hash:
            raise ValueError("source plan payload source contract hash does not match intent")
        if self.manifest_hash != self.source_intent.manifest_hash:
            raise ValueError("source plan manifest_hash does not match source intent")
        if self.resource_request_hash != self.source_intent.resource_request_hash:
            raise ValueError("source plan resource_request_hash does not match source intent")
        return self

    @property
    def plan_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"signature"}))

    def verify(self, keyring: VerifyOnlyEd25519Keyring) -> bool:
        return self._verify(
            keyring,
            purpose="source_use_plan_v2",
            namespace=SOURCE_USE_PLAN_V2_NAMESPACE,
        ) and self.source_intent.verify(keyring)


class CurrentClaimConsumptionBindingV2(RuntimeContractModel):
    """Exact idempotency binding for one current-claim plan-signing effect."""

    schema_version: Literal[2] = 2
    authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(
        min_length=1,
        max_length=200,
        pattern=_OPERATION_ID_PATTERN,
    )
    attempt_binding: SourceAttemptBindingV2
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    lease_expires_at: AwareUtcDatetime
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    resource_request_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    adapter_code_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    payload_source_contract_hash: str = Field(pattern=_HASH_PATTERN)
    nonce: str = Field(min_length=1, max_length=500)

    @classmethod
    def from_plan(cls, plan: SourceUsePlanV2) -> CurrentClaimConsumptionBindingV2:
        validated = SourceUsePlanV2.model_validate(plan, strict=True)
        return cls(
            authority_id=validated.single_use_authority_id,
            operation_id=validated.operation_id,
            attempt_binding=validated.attempt_binding,
            attempt_identity_hash=validated.attempt_identity_hash,
            not_before=validated.not_before,
            expires_at=validated.expires_at,
            lease_expires_at=validated.lease_expires_at,
            plan_hash=validated.plan_hash,
            manifest_hash=validated.manifest_hash,
            resource_request_hash=validated.resource_request_hash,
            adapter_id=validated.adapter_id,
            adapter_version=validated.adapter_version,
            adapter_code_hash=validated.adapter_code_hash,
            payload_hash=validated.payload_hash,
            payload_source_contract_hash=validated.payload_source_contract_hash,
            nonce=validated.nonce,
        )

    @model_validator(mode="after")
    def validate_attempt_hash(self) -> Self:
        if self.attempt_identity_hash != self.attempt_binding.attempt_identity_hash:
            raise ValueError("consumption attempt_identity_hash does not match binding")
        return self

    @property
    def binding_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))

    def matches_plan(self, plan: SourceUsePlanV2) -> bool:
        try:
            return self == CurrentClaimConsumptionBindingV2.from_plan(plan)
        except ValidationError:
            return False


class CurrentClaimPlanSignerIdentityV2(RuntimeContractModel):
    """Public identity of the authority-owned v2 plan signer."""

    schema_version: Literal[2] = 2
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["source_use_plan_v2"] = "source_use_plan_v2"
    namespace: Literal["rquant-source-use-plan/v2"] = SOURCE_USE_PLAN_V2_NAMESPACE


class CurrentClaimPlanIssueV2(RuntimeContractModel):
    """Closed canonical input to one authority-serialized plan issue operation."""

    schema_version: Literal[2] = 2
    binding: CurrentClaimConsumptionBindingV2
    binding_hash: str = Field(pattern=_HASH_PATTERN)
    unsigned_plan: SourceUsePlanV2

    @classmethod
    def from_unsigned_plan(cls, unsigned_plan: SourceUsePlanV2) -> CurrentClaimPlanIssueV2:
        validated = SourceUsePlanV2.model_validate(unsigned_plan, strict=True)
        binding = CurrentClaimConsumptionBindingV2.from_plan(validated)
        return cls(
            binding=binding,
            binding_hash=binding.binding_hash,
            unsigned_plan=validated,
        )

    @model_validator(mode="after")
    def validate_issue(self) -> Self:
        if self.unsigned_plan.signature:
            raise ValueError("current claim issue requires an unsigned source use plan")
        if self.binding_hash != self.binding.binding_hash:
            raise ValueError("current claim issue binding_hash does not match binding")
        if not self.binding.matches_plan(self.unsigned_plan):
            raise ValueError("current claim issue binding does not match unsigned plan")
        return self

    @property
    def issue_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class CurrentClaimConsumptionV2(RuntimeContractModel):
    """Immutable authority receipt containing the atomically committed signed plan."""

    schema_version: Literal[2] = 2
    binding: CurrentClaimConsumptionBindingV2
    binding_hash: str = Field(pattern=_HASH_PATTERN)
    signed_plan: SourceUsePlanV2
    committed_at: AwareUtcDatetime

    @classmethod
    def from_signed_plan(
        cls,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        signed_plan: SourceUsePlanV2,
        committed_at: datetime,
    ) -> CurrentClaimConsumptionV2:
        return cls(
            binding=binding,
            binding_hash=binding.binding_hash,
            signed_plan=signed_plan,
            committed_at=committed_at,
        )

    @model_validator(mode="after")
    def validate_receipt(self) -> Self:
        if self.binding_hash != self.binding.binding_hash:
            raise ValueError("consumption binding_hash does not match binding")
        if not self.binding.matches_plan(self.signed_plan):
            raise ValueError("consumption signed plan does not match exact binding")
        if not self.signed_plan.signature:
            raise ValueError("consumption receipt requires a signed source use plan")
        if self.committed_at >= min(
            self.binding.expires_at,
            self.binding.lease_expires_at,
        ):
            raise ValueError("consumption cannot commit after plan or claim lease expiry")
        return self

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class CurrentClaimAuthorityProtocol(Protocol):
    """High-water authority atomically committing idempotent signed plan receipts."""

    @property
    def authority_id(self) -> str: ...

    @property
    def plan_signer_identity(self) -> CurrentClaimPlanSignerIdentityV2: ...

    def replace_current(self, claim: LabShardClaimV2) -> LabShardClaimV2: ...

    def issue_plan_once(
        self,
        *,
        issue: CurrentClaimPlanIssueV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2: ...

    def verify_current(
        self,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2: ...


SourceUsePlanV2Input = SourceUsePlanV2 | Mapping[str, object] | str | bytes | bytearray


def _normalize_verification_time(now: datetime) -> datetime:
    try:
        return normalize_aware_utc(now)
    except ValueError as exc:
        raise SourceOperationContractError(
            "source plan verification time must be timezone-aware"
        ) from exc


def _strict_source_use_plan_v2(plan: SourceUsePlanV2Input) -> SourceUsePlanV2:
    try:
        if isinstance(plan, (SourceUsePlanV2, Mapping)):
            validated = SourceUsePlanV2.model_validate(plan, strict=True)
            raw = canonical_model_json_bytes(validated)
        elif isinstance(plan, str):
            raw = plan.encode("utf-8")
            validated = None
        elif isinstance(plan, (bytes, bytearray)):
            raw = bytes(plan)
            validated = None
        else:
            raise TypeError("unsupported source use plan input")
        decoded = strict_json_loads(raw)
        if not isinstance(decoded, dict):
            raise ValueError("source use plan must encode a JSON object")
        if raw != canonical_json_bytes(decoded):
            raise ValueError("source use plan JSON is not canonical")
        roundtripped = SourceUsePlanV2.model_validate_json(raw, strict=True)
        if validated is not None and roundtripped != validated:
            raise ValueError("source use plan canonical roundtrip changed the model")
        validated = roundtripped
        if canonical_model_json_bytes(roundtripped) != raw:
            raise ValueError("source use plan canonical roundtrip changed the contract")
    except (TypeError, ValueError, ValidationError) as exc:
        raise SourceOperationContractError(f"source use plan contract is invalid: {exc}") from exc
    return validated


def _strict_current_claim_plan_issue_v2(
    issue: CurrentClaimPlanIssueV2,
) -> CurrentClaimPlanIssueV2:
    try:
        validated = CurrentClaimPlanIssueV2.model_validate(issue, strict=True)
        raw = canonical_model_json_bytes(validated)
        roundtripped = CurrentClaimPlanIssueV2.model_validate_json(raw, strict=True)
        if roundtripped != validated or canonical_model_json_bytes(roundtripped) != raw:
            raise ValueError("current claim issue canonical roundtrip changed the contract")
    except (TypeError, ValueError, ValidationError) as exc:
        raise SourceOperationContractError(
            f"current claim issue contract is invalid: {exc}"
        ) from exc
    return validated


def require_current_claim_plan_issue_v2(
    issue: CurrentClaimPlanIssueV2,
    *,
    keyring: VerifyOnlyEd25519Keyring,
    authority_id: str,
    signer_identity: CurrentClaimPlanSignerIdentityV2,
) -> CurrentClaimPlanIssueV2:
    """Strictly validate a new issue before an authority signs or mutates state."""

    validated = _strict_current_claim_plan_issue_v2(issue)
    try:
        identity = CurrentClaimPlanSignerIdentityV2.model_validate(
            signer_identity,
            strict=True,
        )
    except ValidationError as exc:
        raise SourceOperationContractError(
            f"current claim signer identity is invalid: {exc}"
        ) from exc
    plan = validated.unsigned_plan
    if validated.binding.authority_id != authority_id or (
        plan.single_use_authority_id != authority_id
    ):
        raise SourceOperationContractError("current claim issue authority identity changed")
    if (plan.issuer, plan.key_id) != (identity.issuer, identity.key_id):
        raise SourceOperationContractError("current claim issue signer identity changed")
    plan.source_intent.require_verified(keyring)
    return validated


def _strict_consumption_receipt(
    receipt: CurrentClaimConsumptionV2,
) -> CurrentClaimConsumptionV2:
    try:
        return CurrentClaimConsumptionV2.model_validate(receipt, strict=True)
    except ValidationError as exc:
        raise SourceOperationContractError(
            f"current claim consumption contract is invalid: {exc}"
        ) from exc


def _require_consumption_matches(
    receipt: CurrentClaimConsumptionV2,
    *,
    binding: CurrentClaimConsumptionBindingV2,
    expected_plan: SourceUsePlanV2,
    keyring: VerifyOnlyEd25519Keyring,
    now: datetime,
) -> CurrentClaimConsumptionV2:
    validated = _strict_consumption_receipt(receipt)
    if validated.binding != binding or validated.binding_hash != binding.binding_hash:
        raise SourceOperationContractError(
            "current claim consumption does not match the exact operation binding"
        )
    signed_plan = _strict_source_use_plan_v2(validated.signed_plan)
    if signed_plan.signing_payload() != expected_plan.signing_payload():
        raise SourceOperationContractError(
            "current claim consumption returned a rebound signed plan"
        )
    if not signed_plan.verify(keyring):
        raise SourceOperationContractError(
            "current claim consumption signed plan signature is invalid"
        )
    if validated.committed_at > now:
        raise SourceOperationContractError("current claim consumption is from the future")
    return validated


def require_current_claim_consumption_v2(
    *,
    current_claim_authority: CurrentClaimAuthorityProtocol,
    plan: SourceUsePlanV2,
    keyring: VerifyOnlyEd25519Keyring,
    now: datetime,
) -> CurrentClaimConsumptionV2:
    """Verify that a committed plan still belongs to the current claim high-water."""

    current = _normalize_verification_time(now)
    validated_plan = _strict_source_use_plan_v2(plan)
    binding = CurrentClaimConsumptionBindingV2.from_plan(validated_plan)
    try:
        receipt = current_claim_authority.verify_current(binding=binding, now=current)
    except SourceOperationContractError:
        raise
    except Exception as exc:
        raise SourceOperationContractError(
            f"current claim authority verification failed: {exc}"
        ) from exc
    return _require_consumption_matches(
        receipt,
        binding=binding,
        expected_plan=validated_plan,
        keyring=keyring,
        now=current,
    )


def sign_source_use_plan_v2(
    *,
    claim: LabShardClaimV2,
    current_claim_authority: CurrentClaimAuthorityProtocol,
    keyring: VerifyOnlyEd25519Keyring,
    operation_id: str,
    audience: str,
    now: datetime,
    expires_at: datetime,
    nonce: str,
) -> SourceUsePlanV2:
    """Atomically consume a current claim and recover its signed plan by operation id."""

    from rquant.lab_shard_protocol import LabShardClaimV2, StrategyShardPayloadV2

    try:
        validated_claim = LabShardClaimV2.model_validate(claim, strict=True)
        signer_identity = CurrentClaimPlanSignerIdentityV2.model_validate(
            current_claim_authority.plan_signer_identity,
            strict=True,
        )
    except ValidationError as exc:
        raise SourceOperationContractError(
            f"source claim or authority signer contract is invalid: {exc}"
        ) from exc
    if validated_claim.source_use_plan is not None:
        raise SourceOperationContractError("source plan issuance requires an unbound current claim")
    payload = validated_claim.strategy_payload
    if not isinstance(payload, StrategyShardPayloadV2):
        raise SourceOperationContractError("source plan issuance requires a v2 external payload")
    current = _normalize_verification_time(now)
    unsigned = SourceUsePlanV2.from_source_intent(
        payload.source_intent,
        issuer=signer_identity.issuer,
        key_id=signer_identity.key_id,
        attempt_binding=validated_claim.attempt_binding,
        adapter_id=validated_claim.definition.adapter_id,
        adapter_version=validated_claim.definition.adapter_version,
        adapter_code_hash=validated_claim.adapter_code_hash,
        payload_hash=validated_claim.definition.payload_hash,
        payload_source_contract_hash=validated_claim.payload_source_contract_hash,
        operation_id=operation_id,
        audience=audience,
        not_before=validated_claim.claimed_at,
        expires_at=expires_at,
        lease_expires_at=validated_claim.lease_expires_at,
        nonce=nonce,
        single_use_authority_id=current_claim_authority.authority_id,
    )
    issue = CurrentClaimPlanIssueV2.from_unsigned_plan(unsigned)
    receipt = current_claim_authority.issue_plan_once(
        issue=issue,
        now=current,
    )
    validated_receipt = _require_consumption_matches(
        receipt,
        binding=issue.binding,
        expected_plan=unsigned,
        keyring=keyring,
        now=current,
    )
    return validated_receipt.signed_plan


def require_source_use_plan_v2(
    plan: SourceUsePlanV2Input,
    *,
    keyring: VerifyOnlyEd25519Keyring,
    audience: str,
    now: datetime,
) -> SourceUsePlanV2:
    validated = _strict_source_use_plan_v2(plan)
    if not validated.verify(keyring):
        raise SourceOperationContractError("source use plan signature is invalid")
    current = _normalize_verification_time(now)
    if validated.audience != audience:
        raise SourceOperationContractError("source use plan audience does not match")
    if current < validated.not_before:
        raise SourceOperationContractError("source use plan is not active")
    if current >= validated.expires_at:
        raise SourceOperationContractError("source use plan is expired")
    if current >= validated.lease_expires_at:
        raise SourceOperationContractError("source use plan lease is expired")
    return validated
