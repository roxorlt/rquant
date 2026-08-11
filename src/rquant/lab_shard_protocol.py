"""Typed durable shard claim and worker report protocol for Strategy Lab."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from bisect import bisect_right
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Final, Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabCommandSpool,
    LabQuarantinedCommand,
    LabSpoolFileIdentity,
    RequestContentConflictError,
)
from rquant.lab_result_digest import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    CURRENT_RESULT_MANIFEST_SCHEMA_VERSION,
)
from rquant.scheduler_intent_authorization import SchedulerIntentAuthorizationV1
from rquant.source_broker_v2_job_protocol import SourceBrokerV2AuthorityRef
from rquant.source_operation_contracts import (
    CurrentClaimAuthorityProtocol,
    SourceAttemptBindingV2,
    SourceBrokerV2SchedulerIntentTemplate,
    SourceIntentV2,
    SourceOperationContractError,
    SourceUsePlanV2,
    require_current_claim_consumption_v2,
    require_source_use_plan_v2,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_json_loads,
    strict_model_validate_canonical_json,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
MAX_STRATEGY_SHARD_PAYLOAD_BYTES: Final[int] = 1_048_576
_SPOOL_NAME = re.compile(
    r"(?:(?P<sequence>[0-9]{20})-)?"
    r"(?P<message_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
)
_ACK_NAME = re.compile(
    r"(?P<message_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
)
_CURRENT_CLAIM_NAME = re.compile(
    r"(?P<job_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\."
    r"(?P<shard_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
)
_ADMISSION_TEMP_NAME = re.compile(
    r"execution-admission-v1-"
    r"(?P<claim_token>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})-"
    r"[0-9a-f]{32}\.tmp"
)
_PUBLISH_RECEIPT_AUTHORITY_NAME = "receipt-authority-v2.json"
_PUBLISH_RECEIPT_ENTRY_NAME = re.compile(r"^[0-9a-f]{64}\.json$")
MAX_SHARD_HEARTBEAT_EXTENSION_SECONDS = 3_600
SQLITE_SIGNED_INTEGER_MAX: Final[int] = (1 << 63) - 1
LAB_SHARD_DURATION_MS_MIN: Final[float] = 1e-6
LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE: Final[float] = 1e15
LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE: Final[float] = 1e18


class LabShardProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


class LabShardWorkPlan(LabShardProtocolModel):
    phase: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    work_unit_name: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    work_units: int = Field(strict=True, ge=1, le=SQLITE_SIGNED_INTEGER_MAX)
    static_duration_ms: int = Field(strict=True, ge=1, le=SQLITE_SIGNED_INTEGER_MAX)


class LabShardTelemetry(LabShardWorkPlan):
    duration_ms: float = Field(
        strict=True,
        ge=LAB_SHARD_DURATION_MS_MIN,
        lt=LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
        allow_inf_nan=False,
    )
    throughput_units_per_second: float = Field(
        strict=True,
        gt=0,
        lt=LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE,
        allow_inf_nan=False,
    )

    @classmethod
    def from_work_plan(
        cls,
        work_plan: LabShardWorkPlan,
        *,
        monotonic_started: float,
        monotonic_finished: float,
    ) -> LabShardTelemetry:
        elapsed_seconds = monotonic_finished - monotonic_started
        if not math.isfinite(elapsed_seconds) or elapsed_seconds <= 0:
            raise ValueError("monotonic shard duration must be finite and positive")
        duration_ms = elapsed_seconds * 1_000
        if (
            not math.isfinite(duration_ms)
            or duration_ms < LAB_SHARD_DURATION_MS_MIN
            or duration_ms >= LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE
        ):
            raise ValueError("monotonic shard duration is outside the persisted telemetry domain")
        throughput = work_plan.work_units / elapsed_seconds
        if (
            not math.isfinite(throughput)
            or throughput <= 0
            or throughput >= LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE
        ):
            raise ValueError("shard throughput is outside the persisted telemetry domain")
        return cls(
            **work_plan.model_dump(),
            duration_ms=duration_ms,
            throughput_units_per_second=throughput,
        )

    @model_validator(mode="after")
    def validate_throughput(self) -> LabShardTelemetry:
        observed_work_units = self.throughput_units_per_second * (self.duration_ms * 0.001)
        if not math.isclose(
            observed_work_units,
            float(self.work_units),
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError("throughput_units_per_second does not match duration and work_units")
        return self


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _reject_float(_: str) -> float:
    raise ValueError("floating-point JSON values are not allowed")


def _reject_constant(_: str) -> object:
    raise ValueError("payload must contain finite JSON values")


def validate_strategy_shard_payload_utf8(raw: str | bytes, *, field: str) -> bytes:
    """Accept exact text primitives and reject oversized input before parsing."""

    if type(raw) is str:
        try:
            raw_bytes = raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(f"{field} rejected: reason=payload_utf8_invalid") from exc
    elif type(raw) is bytes:
        raw_bytes = raw
        if len(raw_bytes) > MAX_STRATEGY_SHARD_PAYLOAD_BYTES:
            raise ValueError(
                f"{field} rejected: size_bytes={len(raw_bytes)} "
                f"sha256={hashlib.sha256(raw_bytes).hexdigest()} reason=payload_too_large"
            )
        try:
            raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{field} rejected: reason=payload_utf8_invalid") from exc
    else:
        raise ValueError(f"{field} rejected: reason=payload_type_invalid")
    if len(raw_bytes) > MAX_STRATEGY_SHARD_PAYLOAD_BYTES:
        raise ValueError(
            f"{field} rejected: size_bytes={len(raw_bytes)} "
            f"sha256={hashlib.sha256(raw_bytes).hexdigest()} reason=payload_too_large"
        )
    return raw_bytes


def _canonical_json_object(raw: str, *, field: str) -> str:
    validate_strategy_shard_payload_utf8(raw, field=field)
    try:
        value = strict_json_loads(
            raw,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid {field}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{field} must encode a JSON object")
    return canonical_json_bytes(value).decode("utf-8")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_hash(payload: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class LabShardDefinition(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    shard_id: UUID = UUID(int=0)
    shard_index: int = Field(strict=True, ge=0)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    payload_json: str = Field(min_length=2)
    payload_hash: str = ""
    work_plan: LabShardWorkPlan | None = None

    @classmethod
    def from_payload(
        cls,
        *,
        shard_index: int,
        adapter_id: str,
        adapter_version: str,
        plan_hash: str,
        payload_json: str,
        work_plan: LabShardWorkPlan | None = None,
    ) -> LabShardDefinition:
        return cls(
            shard_index=shard_index,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            plan_hash=plan_hash,
            payload_json=payload_json,
            work_plan=work_plan,
        )

    @model_validator(mode="after")
    def validate_identity(self) -> LabShardDefinition:
        canonical_payload = _canonical_json_object(self.payload_json, field="payload_json")
        payload_hash = _sha256_text(canonical_payload)
        if self.payload_hash and self.payload_hash != payload_hash:
            raise ValueError("payload_hash does not match canonical payload_json")
        shard_identity: dict[str, object] = {
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "payload_hash": payload_hash,
            "plan_hash": self.plan_hash,
            "shard_index": self.shard_index,
        }
        if self.work_plan is not None:
            shard_identity["work_plan"] = self.work_plan.model_dump(mode="json")
        shard_name = canonical_json_bytes(shard_identity).decode("utf-8")
        shard_id = uuid5(NAMESPACE_URL, f"rquant:lab-shard:{shard_name}")
        if self.shard_id.int and self.shard_id != shard_id:
            raise ValueError("shard_id does not match deterministic shard definition")
        object.__setattr__(self, "payload_json", canonical_payload)
        object.__setattr__(self, "payload_hash", payload_hash)
        object.__setattr__(self, "shard_id", shard_id)
        return self


class StrategyShardPayloadV1(LabShardProtocolModel):
    """Legacy local-only payload; it can never authorize external source use."""

    schema_version: Literal[1] = 1
    network: Literal["none"] = "none"
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    payload_json: str = Field(min_length=2)

    @model_validator(mode="after")
    def validate_payload(self) -> StrategyShardPayloadV1:
        object.__setattr__(
            self,
            "payload_json",
            _canonical_json_object(self.payload_json, field="payload_json"),
        )
        return self


class StrategyShardPayloadV2(LabShardProtocolModel):
    """External-source payload; its source intent is closed and manifest-bound."""

    schema_version: Literal[2] = 2
    network: Literal["provider"] = "provider"
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    payload_json: str = Field(min_length=2)
    source_intent: SourceIntentV2
    source_contract_hash: str = Field(pattern=_HASH_PATTERN)
    scheduler_intent_template: SourceBrokerV2SchedulerIntentTemplate | None = None
    scheduler_intent_authorization: SchedulerIntentAuthorizationV1 | None = None

    @classmethod
    def from_source_intent(
        cls,
        *,
        adapter_id: str,
        adapter_version: str,
        payload_json: str,
        source_intent: SourceIntentV2,
        scheduler_intent_template: SourceBrokerV2SchedulerIntentTemplate | None = None,
        scheduler_intent_authorization: SchedulerIntentAuthorizationV1 | None = None,
    ) -> StrategyShardPayloadV2:
        return cls(
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            payload_json=payload_json,
            source_intent=source_intent,
            source_contract_hash=source_intent.source_contract_hash,
            scheduler_intent_template=scheduler_intent_template,
            scheduler_intent_authorization=scheduler_intent_authorization,
        )

    @model_validator(mode="after")
    def validate_payload(self) -> StrategyShardPayloadV2:
        source_manifest = self.source_intent.manifest
        if (
            self.adapter_id,
            self.adapter_version,
        ) != (
            source_manifest.adapter_id,
            source_manifest.adapter_version,
        ):
            raise ValueError("external payload adapter identity conflicts with source intent")
        if self.source_contract_hash != self.source_intent.source_contract_hash:
            raise ValueError("external payload source_contract_hash does not match source intent")
        if (
            self.scheduler_intent_template is not None
            and self.scheduler_intent_template.source_intent != self.source_intent
        ):
            raise ValueError(
                "external payload scheduler intent template conflicts with source intent"
            )
        if (
            self.scheduler_intent_authorization is not None
            and self.scheduler_intent_authorization.payload_commitment
            != self.authorization_payload_commitment
        ):
            raise ValueError(
                "external payload scheduler intent authorization conflicts with payload"
            )
        object.__setattr__(
            self,
            "payload_json",
            _canonical_json_object(self.payload_json, field="payload_json"),
        )
        return self

    @property
    def authorization_payload_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", exclude={"scheduler_intent_authorization"})
        )

    @property
    def authorization_payload_commitment(self) -> str:
        return hashlib.sha256(self.authorization_payload_bytes).hexdigest()

    def with_scheduler_intent_authorization(
        self,
        authorization: SchedulerIntentAuthorizationV1,
    ) -> StrategyShardPayloadV2:
        return StrategyShardPayloadV2.model_validate(
            self.model_dump(mode="python") | {"scheduler_intent_authorization": authorization}
        )


StrategyShardPayload = StrategyShardPayloadV1 | StrategyShardPayloadV2


def parse_strategy_shard_payload(payload_json: str | bytes) -> StrategyShardPayload:
    try:
        payload = strict_json_loads(
            validate_strategy_shard_payload_utf8(payload_json, field="payload_json")
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid strategy shard payload: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("strategy shard payload must encode a JSON object")
    schema_version = payload.get("schema_version")
    if type(schema_version) is not int:
        raise ValueError("strategy shard payload schema_version must be an integer")
    if schema_version == 1:
        return StrategyShardPayloadV1.model_validate(payload)
    if schema_version == 2:
        return StrategyShardPayloadV2.model_validate_json(canonical_json_bytes(payload))
    raise ValueError("unsupported strategy shard payload schema_version")


def require_external_strategy_shard_payload(
    payload: StrategyShardPayload,
    *,
    keyring: object,
) -> StrategyShardPayloadV2:
    if isinstance(payload, StrategyShardPayloadV1):
        raise SourceOperationContractError("strategy shard payload v1 is permanently offline-only")
    if not isinstance(payload, StrategyShardPayloadV2):
        raise SourceOperationContractError(
            "strategy shard payload must be a recognized v2 contract"
        )
    from rquant.adapter_manifest import VerifyOnlyEd25519Keyring

    if not isinstance(keyring, VerifyOnlyEd25519Keyring):
        raise SourceOperationContractError("strategy shard payload requires a verification keyring")
    try:
        validated = StrategyShardPayloadV2.model_validate(payload, strict=True)
    except (TypeError, ValueError) as exc:
        raise SourceOperationContractError(
            f"strategy shard payload v2 contract is invalid: {exc}"
        ) from exc
    validated.source_intent.require_verified(keyring)
    return validated


class LabShardClaim(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    definition: LabShardDefinition
    worker_id: str = Field(min_length=1)
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    claimed_at: datetime
    lease_expires_at: datetime

    @model_validator(mode="after")
    def validate_lease(self) -> LabShardClaim:
        claimed_at = _utc(self.claimed_at, field="claimed_at")
        expires_at = _utc(self.lease_expires_at, field="lease_expires_at")
        if expires_at <= claimed_at:
            raise ValueError("lease_expires_at must be after claimed_at")
        object.__setattr__(self, "claimed_at", claimed_at)
        object.__setattr__(self, "lease_expires_at", expires_at)
        return self

    @property
    def shard_id(self) -> UUID:
        return self.definition.shard_id

    @property
    def shard_index(self) -> int:
        return self.definition.shard_index

    @property
    def payload_hash(self) -> str:
        return self.definition.payload_hash

    @property
    def plan_hash(self) -> str:
        return self.definition.plan_hash


class LabShardClaimV2(LabShardProtocolModel):
    """Current v2 claim, optionally carrying its authority-issued source plan."""

    schema_version: Literal[2] = 2
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    definition: LabShardDefinition
    worker_id: str = Field(min_length=1)
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    claimed_at: datetime
    lease_expires_at: datetime
    source_use_plan: SourceUsePlanV2 | None = None
    source_plan_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_code_hash: str = Field(pattern=_HASH_PATTERN)
    payload_source_contract_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_current_attempt(
        cls,
        *,
        definition: LabShardDefinition,
        attempt_binding: SourceAttemptBindingV2,
        claimed_at: datetime,
        lease_expires_at: datetime,
    ) -> LabShardClaimV2:
        if attempt_binding.spec_hash is None:
            raise ValueError("lab source claim requires a spec_hash attempt binding")
        validated_definition = LabShardDefinition.model_validate(definition, strict=True)
        if attempt_binding.shard_id != validated_definition.shard_id:
            raise ValueError("source attempt shard_id does not match shard definition")
        payload = parse_strategy_shard_payload(validated_definition.payload_json)
        if not isinstance(payload, StrategyShardPayloadV2):
            raise ValueError("lab source claim v2 requires a v2 external payload")
        return cls(
            job_id=attempt_binding.job_id,
            spec_hash=attempt_binding.spec_hash,
            definition=validated_definition,
            worker_id=attempt_binding.worker_id,
            claim_token=attempt_binding.attempt_id,
            claim_generation=attempt_binding.claim_generation,
            scheduler_fencing_token=attempt_binding.scheduler_fencing_token,
            claimed_at=claimed_at,
            lease_expires_at=lease_expires_at,
            manifest_hash=payload.source_intent.manifest_hash,
            adapter_code_hash=payload.source_intent.manifest.adapter_code_hash,
            payload_source_contract_hash=payload.source_contract_hash,
        )

    @model_validator(mode="after")
    def validate_source_claim(self) -> LabShardClaimV2:
        claimed_at = _utc(self.claimed_at, field="claimed_at")
        lease_expires_at = _utc(self.lease_expires_at, field="lease_expires_at")
        if lease_expires_at <= claimed_at:
            raise ValueError("lease_expires_at must be after claimed_at")
        definition = LabShardDefinition.model_validate(self.definition, strict=True)
        payload = parse_strategy_shard_payload(definition.payload_json)
        if not isinstance(payload, StrategyShardPayloadV2):
            raise ValueError("lab source claim v2 requires a v2 external payload")
        manifest = payload.source_intent.manifest
        if (
            definition.adapter_id,
            definition.adapter_version,
        ) != (
            payload.adapter_id,
            payload.adapter_version,
        ) or (
            payload.adapter_id,
            payload.adapter_version,
        ) != (
            manifest.adapter_id,
            manifest.adapter_version,
        ):
            raise ValueError("claim adapter identity conflicts with payload or signed manifest")
        if self.adapter_code_hash != manifest.adapter_code_hash:
            raise ValueError("claim adapter_code_hash does not match signed manifest")
        if self.manifest_hash != payload.source_intent.manifest_hash:
            raise ValueError("claim manifest_hash does not match signed source intent")
        if self.payload_source_contract_hash != payload.source_contract_hash:
            raise ValueError("claim payload source_contract_hash does not match payload")
        expected_binding = SourceAttemptBindingV2(
            job_id=self.job_id,
            spec_hash=self.spec_hash,
            shard_id=definition.shard_id,
            attempt_id=self.claim_token,
            claim_generation=self.claim_generation,
            scheduler_fencing_token=self.scheduler_fencing_token,
            worker_id=self.worker_id,
        )
        if self.source_use_plan is None:
            if self.source_plan_hash is not None:
                raise ValueError("unbound source claim cannot declare source_plan_hash")
        else:
            plan = SourceUsePlanV2.model_validate(self.source_use_plan, strict=True)
            if self.source_plan_hash != plan.plan_hash:
                raise ValueError("source_plan_hash does not match source use plan")
            if plan.attempt_binding != expected_binding:
                raise ValueError("source use plan attempt binding does not match claim")
            if plan.lease_expires_at != lease_expires_at:
                raise ValueError("source use plan lease_expires_at does not match claim")
            expected_plan_identity = (
                definition.adapter_id,
                definition.adapter_version,
                self.adapter_code_hash,
                definition.payload_hash,
                self.payload_source_contract_hash,
                self.manifest_hash,
                payload.source_intent.resource_request_hash,
            )
            observed_plan_identity = (
                plan.adapter_id,
                plan.adapter_version,
                plan.adapter_code_hash,
                plan.payload_hash,
                plan.payload_source_contract_hash,
                plan.manifest_hash,
                plan.resource_request_hash,
            )
            if observed_plan_identity != expected_plan_identity:
                raise ValueError(
                    "source use plan identity conflicts with claim definition or payload"
                )
        object.__setattr__(self, "claimed_at", claimed_at)
        object.__setattr__(self, "lease_expires_at", lease_expires_at)
        object.__setattr__(self, "definition", definition)
        return self

    @property
    def shard_id(self) -> UUID:
        return self.definition.shard_id

    @property
    def shard_index(self) -> int:
        return self.definition.shard_index

    @property
    def payload_hash(self) -> str:
        return self.definition.payload_hash

    @property
    def plan_hash(self) -> str:
        return self.definition.plan_hash

    @property
    def attempt_binding(self) -> SourceAttemptBindingV2:
        return SourceAttemptBindingV2(
            job_id=self.job_id,
            spec_hash=self.spec_hash,
            shard_id=self.definition.shard_id,
            attempt_id=self.claim_token,
            claim_generation=self.claim_generation,
            scheduler_fencing_token=self.scheduler_fencing_token,
            worker_id=self.worker_id,
        )

    @property
    def strategy_payload(self) -> StrategyShardPayloadV2:
        payload = parse_strategy_shard_payload(self.definition.payload_json)
        if not isinstance(payload, StrategyShardPayloadV2):
            raise ValueError("lab source claim v2 requires a v2 external payload")
        return payload

    def bind_source_use_plan(self, plan: SourceUsePlanV2) -> LabShardClaimV2:
        if self.source_use_plan is not None:
            raise ValueError("source claim is already bound to a source use plan")
        validated_plan = SourceUsePlanV2.model_validate(plan, strict=True)
        return LabShardClaimV2.model_validate(
            {
                **self.model_dump(mode="python"),
                "source_use_plan": validated_plan,
                "source_plan_hash": validated_plan.plan_hash,
            },
            strict=True,
        )


LabSpoolClaim = LabShardClaim | LabShardClaimV2


def _validate_spool_claim(value: LabSpoolClaim) -> LabSpoolClaim:
    if isinstance(value, LabShardClaimV2):
        return LabShardClaimV2.model_validate(value, strict=True)
    return LabShardClaim.model_validate(value, strict=True)


def _parse_spool_claim(payload: bytes) -> LabSpoolClaim:
    decoded = strict_json_loads(payload)
    if not isinstance(decoded, dict):
        raise ValueError("spool claim must encode a JSON object")
    schema_version = decoded.get("schema_version")
    if schema_version == 1:
        return strict_model_validate_canonical_json(LabShardClaim, payload)
    if schema_version == 2:
        return strict_model_validate_canonical_json(LabShardClaimV2, payload)
    raise ValueError("unsupported spool claim schema_version")


def require_source_bound_claim_v2(
    claim: LabShardClaimV2,
    *,
    keyring: object,
    current_claim_authority: CurrentClaimAuthorityProtocol,
    audience: str,
    now: datetime,
) -> LabShardClaimV2:
    from rquant.adapter_manifest import VerifyOnlyEd25519Keyring

    if not isinstance(keyring, VerifyOnlyEd25519Keyring):
        raise SourceOperationContractError("source claim requires a verification keyring")
    try:
        validated_claim = LabShardClaimV2.model_validate(claim, strict=True)
    except (TypeError, ValueError) as exc:
        raise SourceOperationContractError(f"source claim contract is invalid: {exc}") from exc
    if validated_claim.source_use_plan is None:
        raise SourceOperationContractError("source claim is not bound to a source use plan")
    plan = require_source_use_plan_v2(
        validated_claim.source_use_plan,
        keyring=keyring,
        audience=audience,
        now=now,
    )
    if plan.single_use_authority_id != current_claim_authority.authority_id:
        raise SourceOperationContractError("source plan current-claim authority does not match")
    require_current_claim_consumption_v2(
        current_claim_authority=current_claim_authority,
        plan=plan,
        keyring=keyring,
        now=now,
    )
    return validated_claim


class LabClaimHighWater(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    claim: LabSpoolClaim
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabClaimHighWater:
        expected = _canonical_hash(self.claim.model_dump(mode="json", exclude_none=True))
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match current claim")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabRetiredClaimAuthority(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    claim: LabSpoolClaim
    outcome: Literal["accepted", "revoked"]
    reason: str = Field(min_length=1)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabRetiredClaimAuthority:
        expected = _canonical_hash(
            {
                "claim": self.claim.model_dump(mode="json", exclude_none=True),
                "outcome": self.outcome,
                "reason": self.reason,
                "schema_version": self.schema_version,
            }
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match retired claim authority")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabClaimSupersededError(RuntimeError):
    """A claim is older than, or conflicts with, the durable shard high-water."""


class LabClaimAlreadyConsumedError(RuntimeError):
    """A durable receipt proves that this exact claim was already delivered."""


class LabClaimRevokedError(RuntimeError):
    """A durable receipt proves that this exact claim must not be delivered."""


class LabClaimNotConsumedError(RuntimeError):
    """Execution admission requires immutable delivery history first."""


class LabClaimDeliveryReceipt(LabShardProtocolModel):
    """Immutable delivery history; ``revoked`` is accepted for legacy ledgers only."""

    schema_version: Literal[1] = 1
    status: Literal["consumed", "revoked"] = "consumed"
    claim: LabSpoolClaim
    reason: str | None = Field(default=None, min_length=1)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabClaimDeliveryReceipt:
        expected = _canonical_hash(self.claim.model_dump(mode="json", exclude_none=True))
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match claim delivery receipt")
        if self.status == "revoked" and self.reason is None:
            raise ValueError("revoked claim receipt requires reason")
        if self.status == "consumed" and self.reason is not None:
            raise ValueError("consumed claim receipt cannot include reason")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabClaimRevocation(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    claim: LabSpoolClaim
    reason: str = Field(min_length=1)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabClaimRevocation:
        expected = _canonical_hash(
            {
                "claim": self.claim.model_dump(mode="json", exclude_none=True),
                "reason": self.reason,
                "schema_version": self.schema_version,
            }
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match claim revocation")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabExecutionAdmission(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    claim: LabSpoolClaim
    delivery_content_hash: str = Field(pattern=_HASH_PATTERN)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabExecutionAdmission:
        expected = _canonical_hash(
            {
                "claim": self.claim.model_dump(mode="json", exclude_none=True),
                "delivery_content_hash": self.delivery_content_hash,
                "schema_version": self.schema_version,
            }
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match execution admission")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabClaimReconcileResult(LabShardProtocolModel):
    claim_token: UUID
    status: Literal["reconciled", "failed", "not_configured"]
    error: str | None = None

    @model_validator(mode="after")
    def validate_error(self) -> LabClaimReconcileResult:
        if self.status == "failed" and not self.error:
            raise ValueError("failed reconciliation requires error")
        if self.status != "failed" and self.error is not None:
            raise ValueError("successful reconciliation cannot include error")
        return self


class LabShardHeartbeat(LabShardProtocolModel):
    report_type: Literal["heartbeat"] = "heartbeat"
    lease_extension_seconds: int = Field(
        strict=True,
        ge=1,
        le=MAX_SHARD_HEARTBEAT_EXTENSION_SECONDS,
    )


class LabShardSucceeded(LabShardProtocolModel):
    report_type: Literal["shard_succeeded"] = "shard_succeeded"
    result_manifest_hash: str = Field(pattern=_HASH_PATTERN)
    result_manifest_schema_version: Literal[2] | None = None
    content_digest_algorithm: Literal["rquant-pandas-table-json-sha256-v2"] | None = None
    worker_code_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    telemetry: LabShardTelemetry | None = None

    @classmethod
    def current(
        cls,
        *,
        result_manifest_hash: str,
        worker_code_sha: str,
        telemetry: LabShardTelemetry | None = None,
    ) -> LabShardSucceeded:
        return cls(
            result_manifest_hash=result_manifest_hash,
            result_manifest_schema_version=CURRENT_RESULT_MANIFEST_SCHEMA_VERSION,
            content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
            worker_code_sha=worker_code_sha,
            telemetry=telemetry,
        )

    @model_validator(mode="after")
    def validate_digest_provenance(self) -> LabShardSucceeded:
        provenance_fields = {
            "result_manifest_schema_version",
            "content_digest_algorithm",
            "worker_code_sha",
        }
        supplied_fields = provenance_fields.intersection(self.model_fields_set)
        provenance = (
            self.result_manifest_schema_version,
            self.content_digest_algorithm,
            self.worker_code_sha,
        )
        if not supplied_fields and provenance == (None, None, None):
            return self
        if supplied_fields != provenance_fields:
            raise ValueError("shard success digest provenance is incomplete or unsupported")
        if (
            self.result_manifest_schema_version == CURRENT_RESULT_MANIFEST_SCHEMA_VERSION
            and self.content_digest_algorithm == CURRENT_CONTENT_DIGEST_ALGORITHM
            and self.worker_code_sha is not None
        ):
            return self
        raise ValueError("shard success digest provenance is incomplete or unsupported")

    @model_serializer(mode="wrap")
    def serialize_digest_provenance(
        self,
        handler: SerializerFunctionWrapHandler,
    ) -> object:
        payload = handler(self)
        provenance_fields = {
            "result_manifest_schema_version",
            "content_digest_algorithm",
            "worker_code_sha",
        }
        if isinstance(payload, dict) and not provenance_fields.intersection(self.model_fields_set):
            for field in provenance_fields:
                payload.pop(field, None)
        return payload


class LabShardFailed(LabShardProtocolModel):
    report_type: Literal["shard_failed"] = "shard_failed"
    failure_json: str = Field(min_length=2)
    failure_hash: str = ""

    @model_validator(mode="after")
    def validate_failure(self) -> LabShardFailed:
        canonical = _canonical_json_object(self.failure_json, field="failure_json")
        failure_hash = _sha256_text(canonical)
        if self.failure_hash and self.failure_hash != failure_hash:
            raise ValueError("failure_hash does not match canonical failure_json")
        object.__setattr__(self, "failure_json", canonical)
        object.__setattr__(self, "failure_hash", failure_hash)
        return self


class LabWorkerStopped(LabShardProtocolModel):
    report_type: Literal["worker_stopped"] = "worker_stopped"
    reason: str = Field(min_length=1)


LabWorkerReportBody = Annotated[
    LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    Field(discriminator="report_type"),
]


class LabWorkerReport(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    report_id: UUID
    job_id: UUID
    shard_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    worker_id: str = Field(min_length=1)
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    reported_at: datetime
    body: LabWorkerReportBody
    content_hash: str = ""

    @classmethod
    def from_claim(
        cls,
        claim: LabShardClaim,
        *,
        report_id: UUID,
        reported_at: datetime,
        body: LabWorkerReportBody,
    ) -> LabWorkerReport:
        return cls(
            report_id=report_id,
            job_id=claim.job_id,
            shard_id=claim.shard_id,
            spec_hash=claim.spec_hash,
            payload_hash=claim.payload_hash,
            worker_id=claim.worker_id,
            claim_token=claim.claim_token,
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            reported_at=reported_at,
            body=body,
        )

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabWorkerReport:
        reported_at = _utc(self.reported_at, field="reported_at")
        body = self.body.model_dump(mode="json", exclude_none=True)
        expected = _canonical_hash(
            {
                "body": body,
                "claim_generation": self.claim_generation,
                "claim_token": str(self.claim_token),
                "job_id": str(self.job_id),
                "payload_hash": self.payload_hash,
                "report_id": str(self.report_id),
                "reported_at": reported_at.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "scheduler_fencing_token": self.scheduler_fencing_token,
                "schema_version": self.schema_version,
                "shard_id": str(self.shard_id),
                "spec_hash": self.spec_hash,
                "worker_id": self.worker_id,
            }
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match canonical worker report")
        object.__setattr__(self, "reported_at", reported_at)
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return self.model_dump_json()


class LabReportReceipt(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    report_id: UUID
    content_hash: str = Field(pattern=_HASH_PATTERN)
    job_id: UUID
    shard_id: UUID
    worker_id: str | None = Field(default=None, min_length=1)
    claim_token: UUID | None = None
    claim_generation: int | None = Field(default=None, strict=True, ge=1)
    scheduler_fencing_token: int | None = Field(default=None, strict=True, ge=1)
    report_type: (
        Literal[
            "heartbeat",
            "shard_succeeded",
            "shard_failed",
            "worker_stopped",
        ]
        | None
    ) = None
    result_manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    status: Literal["accepted", "rejected"]
    reason: str = Field(min_length=1)
    accepted_at: datetime

    @classmethod
    def from_report(
        cls,
        report: LabWorkerReport,
        *,
        status: Literal["accepted", "rejected"],
        reason: str,
        accepted_at: datetime,
    ) -> LabReportReceipt:
        return cls(
            report_id=report.report_id,
            content_hash=report.content_hash,
            job_id=report.job_id,
            shard_id=report.shard_id,
            worker_id=report.worker_id,
            claim_token=report.claim_token,
            claim_generation=report.claim_generation,
            scheduler_fencing_token=report.scheduler_fencing_token,
            report_type=report.body.report_type,
            result_manifest_hash=(
                report.body.result_manifest_hash
                if isinstance(report.body, LabShardSucceeded)
                else None
            ),
            status=status,
            reason=reason,
            accepted_at=accepted_at,
        )

    @model_validator(mode="after")
    def validate_time(self) -> LabReportReceipt:
        identity = (
            self.worker_id,
            self.claim_token,
            self.claim_generation,
            self.scheduler_fencing_token,
            self.report_type,
        )
        if any(value is not None for value in identity) and not all(
            value is not None for value in identity
        ):
            raise ValueError("receipt attempt identity must be complete when present")
        if self.report_type == "shard_succeeded":
            if self.result_manifest_hash is None:
                raise ValueError("success receipt requires result_manifest_hash")
        elif self.result_manifest_hash is not None:
            raise ValueError("only success receipt may contain result_manifest_hash")
        object.__setattr__(self, "accepted_at", _utc(self.accepted_at, field="accepted_at"))
        return self


class LabClaimSpoolEntry(LabShardProtocolModel):
    path: Path
    claim: LabSpoolClaim
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabConsumedClaim(LabShardProtocolModel):
    path: Path
    receipt: LabClaimDeliveryReceipt


class LabRevokedClaim(LabShardProtocolModel):
    path: Path
    revocation: LabClaimRevocation


class LabAdmittedExecution(LabShardProtocolModel):
    path: Path
    admission: LabExecutionAdmission


LabHotClaimNamespace = Literal["pending", "current", "revoked"]


class LabPendingClaimCursor(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    after_sequence: int | None = Field(default=None, ge=0)
    cycle_ceiling_sequence: int | None = Field(default=None, ge=0)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabPendingClaimCursor:
        if (self.after_sequence is None) != (self.cycle_ceiling_sequence is None):
            raise ValueError("pending claim cursor bounds must be both present or absent")
        if (
            self.after_sequence is not None
            and self.cycle_ceiling_sequence is not None
            and self.after_sequence > self.cycle_ceiling_sequence
        ):
            raise ValueError("pending claim cursor exceeds its cycle ceiling")
        expected = _canonical_hash(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match pending claim cursor")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabHotClaimBatch(LabShardProtocolModel):
    claims: tuple[LabSpoolClaim, ...]
    next_cursor: LabPendingClaimCursor
    scanned_namespaces: tuple[LabHotClaimNamespace, ...] = ()
    inspected: int = Field(ge=0)


class LabReportSpoolEntry(LabShardProtocolModel):
    path: Path
    report: LabWorkerReport
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabAcknowledgedReport(LabShardProtocolModel):
    path: Path
    receipt: LabReportReceipt


class _TypedSpoolBase(LabCommandSpool):
    @staticmethod
    def _message_name_parts(name: str) -> tuple[int | None, UUID]:
        match = _SPOOL_NAME.fullmatch(name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid pending message basename: {name}")
        sequence = match.group("sequence")
        return (int(sequence) if sequence is not None else None, UUID(match.group("message_id")))

    @staticmethod
    def _ack_message_id(name: str) -> UUID:
        match = _ACK_NAME.fullmatch(name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid ack basename: {name}")
        return UUID(match.group("message_id"))

    def _pending_for_message_locked(self, message_id: UUID) -> Path | None:
        matches: list[Path] = []
        for candidate in self._managed_paths(self.pending_dir, "*.json"):
            try:
                _sequence, candidate_id = self._message_name_parts(candidate.name)
            except InvalidCommandEnvelopeError:
                continue
            if candidate_id == message_id:
                matches.append(candidate)
        if len(matches) > 1:
            raise InvalidCommandEnvelopeError(f"multiple pending messages for {message_id}")
        return matches[0] if matches else None

    @staticmethod
    def _delivery_key(path: Path) -> tuple[int, int, str]:
        try:
            sequence, _message_id = _TypedSpoolBase._message_name_parts(path.name)
        except InvalidCommandEnvelopeError:
            return (0, 0, path.name)
        return (1, sequence or 0, path.name)

    def pending_paths(self, *, limit: int | None = None) -> tuple[Path, ...]:
        with self._exclusive_lock():
            paths = tuple(
                sorted(
                    self._managed_paths(self.pending_dir, "*.json"),
                    key=self._delivery_key,
                )
            )
            return paths if limit is None else paths[:limit]

    def quarantine(
        self,
        entry_or_path: LabClaimSpoolEntry | LabReportSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> LabQuarantinedCommand:
        if isinstance(entry_or_path, LabClaimSpoolEntry | LabReportSpoolEntry):
            identity = LabSpoolFileIdentity(
                path=entry_or_path.path,
                device=entry_or_path.device,
                inode=entry_or_path.inode,
            )
            return super().quarantine(identity, reason=reason)
        return super().quarantine(entry_or_path, reason=reason)


class LabClaimSpool(_TypedSpoolBase):
    """Scheduler-to-worker durable claim channel."""

    def __init__(
        self,
        root: Path,
        *,
        claim_advance_hook: Callable[[LabSpoolClaim], None] | None = None,
        mutation_guard: Callable[[], object] | None = None,
        publish_receipt_publisher: SourceBrokerV2AuthorityRef | None = None,
    ) -> None:
        super().__init__(root, mutation_guard=mutation_guard)
        self.current_dir = self.root / "current"
        self.retired_dir = self.root / "archive" / "retired"
        self.revoked_dir = self.root / "revoked"
        self.archived_revoked_dir = self.root / "archive" / "revoked"
        self.pending_cursor_path = self.root / ".hot-pending-cursor-v1.json"
        self.admitted_dir = self.root / "admitted"
        self.admission_tmp_dir = self.admitted_dir / ".tmp"
        self.publish_receipt_dir = self.root / "publish-receipts-v2"
        self.publish_receipt_authority_path = (
            self.publish_receipt_dir / _PUBLISH_RECEIPT_AUTHORITY_NAME
        )
        self._publish_receipt_publisher = publish_receipt_publisher
        with self._exclusive_lock():
            self._ensure_directory(self.current_dir)
            self._ensure_directory(self.retired_dir)
            self._ensure_directory(self.revoked_dir)
            self._ensure_directory(self.archived_revoked_dir)
            self._ensure_directory(self.admitted_dir)
            self._ensure_directory(self.admission_tmp_dir, mode=0o700)
            self._ensure_directory(self.publish_receipt_dir)
        self._claim_advance_hook = claim_advance_hook

    def set_claim_advance_hook(
        self,
        hook: Callable[[LabSpoolClaim], None],
    ) -> None:
        self._claim_advance_hook = hook

    @staticmethod
    def _claim_order(claim: LabSpoolClaim) -> tuple[int, int, datetime, int]:
        return (
            claim.claim_generation,
            claim.scheduler_fencing_token,
            claim.claimed_at,
            claim.claim_token.int,
        )

    def _current_path(self, job_id: UUID, shard_id: UUID) -> Path:
        return self.current_dir / f"{job_id}.{shard_id}.json"

    def _consumed_path(self, claim_token: UUID) -> Path:
        return self.ack_dir / f"{claim_token}.json"

    def _retired_path(self, job_id: UUID, shard_id: UUID) -> Path:
        return self.retired_dir / f"{job_id}.{shard_id}.json"

    def _revoked_path(self, claim_token: UUID) -> Path:
        return self.revoked_dir / f"{claim_token}.json"

    def _archived_revoked_path(self, claim_token: UUID) -> Path:
        return self.archived_revoked_dir / f"{claim_token}.json"

    def _admission_path(self, claim_token: UUID) -> Path:
        return self.admitted_dir / f"{claim_token}.json"

    def _load_pending_cursor_locked(self) -> LabPendingClaimCursor:
        if not self._managed_entry_exists(self.pending_cursor_path, self.root):
            return LabPendingClaimCursor()
        _candidate, payload, _file_stat = self._read_regular_child(
            self.pending_cursor_path,
            self.root,
        )
        try:
            return strict_model_validate_canonical_json(LabPendingClaimCursor, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid durable pending claim cursor: {exc}"
            ) from exc

    def _publish_pending_cursor_locked(self, cursor: LabPendingClaimCursor) -> None:
        validated = LabPendingClaimCursor.model_validate(cursor)
        self._replace_managed_payload(
            self.pending_cursor_path,
            canonical_model_json_bytes(validated),
        )
        if self._load_pending_cursor_locked() != validated:
            raise InvalidCommandEnvelopeError("durable pending claim cursor readback mismatch")

    def _hot_namespace_paths(self, directory: Path) -> tuple[Path, ...]:
        paths: list[Path] = []
        descriptor = self._open_managed_directory(directory)
        try:
            with os.scandir(descriptor) as entries:
                for entry in entries:
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise InvalidCommandEnvelopeError(
                            f"unsafe hot claim entry {entry.name}: {exc}"
                        ) from exc
                    if not entry.name.endswith(".json"):
                        raise InvalidCommandEnvelopeError(
                            f"unexpected hot claim entry {entry.name}"
                        )
                    if not stat.S_ISREG(entry_stat.st_mode) or entry_stat.st_nlink != 1:
                        raise InvalidCommandEnvelopeError(f"unsafe hot claim entry {entry.name}")
                    paths.append(directory / entry.name)
        except OSError as exc:
            raise InvalidCommandEnvelopeError(
                f"cannot enumerate hot claim namespace {directory.name}: {exc}"
            ) from exc
        finally:
            os.close(descriptor)
        return tuple(sorted(paths, key=lambda path: path.name))

    @staticmethod
    def _pending_slice(
        paths: tuple[Path, ...],
        *,
        cursor: LabPendingClaimCursor,
        limit: int,
    ) -> tuple[tuple[Path, ...], LabPendingClaimCursor]:
        if not paths:
            return (), LabPendingClaimCursor()
        sequenced: list[tuple[int, Path]] = []
        for path in paths:
            sequence, _message_id = _TypedSpoolBase._message_name_parts(path.name)
            if sequence is None:
                raise InvalidCommandEnvelopeError(
                    f"hot pending claim lacks durable delivery sequence: {path.name}"
                )
            sequenced.append((sequence, path))
        sequenced.sort(key=lambda item: item[0])
        sequences = [item[0] for item in sequenced]
        after_sequence = cursor.after_sequence
        cycle_ceiling = cursor.cycle_ceiling_sequence
        if after_sequence is None or cycle_ceiling is None:
            after_sequence = 0
            cycle_ceiling = sequences[-1]
        start = bisect_right(sequences, after_sequence)
        stop = bisect_right(sequences, cycle_ceiling)
        if start >= stop:
            after_sequence = 0
            cycle_ceiling = sequences[-1]
            start = 0
            stop = len(sequenced)
        selected_items = sequenced[start : min(stop, start + limit)]
        selected = tuple(path for _sequence, path in selected_items)
        if not selected:
            return (), LabPendingClaimCursor(
                after_sequence=after_sequence,
                cycle_ceiling_sequence=cycle_ceiling,
            )
        return selected, LabPendingClaimCursor(
            after_sequence=selected_items[-1][0],
            cycle_ceiling_sequence=cycle_ceiling,
        )

    def _load_consumed_locked(self, claim_token: UUID) -> LabConsumedClaim:
        path = self._consumed_path(claim_token)
        candidate, payload, _file_stat = self._read_regular_child(path, self.ack_dir)
        if self._ack_message_id(candidate.name) != claim_token:
            raise InvalidCommandEnvelopeError(
                f"consumed claim token does not match basename {candidate.name}"
            )
        try:
            receipt = strict_model_validate_canonical_json(LabClaimDeliveryReceipt, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid consumed claim receipt {candidate.name}: {exc}"
            ) from exc
        if receipt.claim.claim_token != claim_token:
            raise InvalidCommandEnvelopeError(
                f"consumed claim identity does not match basename {candidate.name}"
            )
        return LabConsumedClaim(path=candidate, receipt=receipt)

    def _load_revocation_file_locked(
        self,
        claim_token: UUID,
        *,
        path: Path,
        parent: Path,
    ) -> LabRevokedClaim:
        candidate, payload, _file_stat = self._read_regular_child(path, parent)
        if self._ack_message_id(candidate.name) != claim_token:
            raise InvalidCommandEnvelopeError(
                f"revoked claim token does not match basename {candidate.name}"
            )
        try:
            revocation = strict_model_validate_canonical_json(LabClaimRevocation, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid claim revocation {candidate.name}: {exc}"
            ) from exc
        if revocation.claim.claim_token != claim_token:
            raise InvalidCommandEnvelopeError(
                f"revoked claim identity does not match basename {candidate.name}"
            )
        return LabRevokedClaim(path=candidate, revocation=revocation)

    def _load_revocation_locked(self, claim_token: UUID) -> LabRevokedClaim:
        return self._load_revocation_file_locked(
            claim_token,
            path=self._revoked_path(claim_token),
            parent=self.revoked_dir,
        )

    def _load_archived_revocation_locked(self, claim_token: UUID) -> LabRevokedClaim:
        return self._load_revocation_file_locked(
            claim_token,
            path=self._archived_revoked_path(claim_token),
            parent=self.archived_revoked_dir,
        )

    def _load_admission_locked(self, claim_token: UUID) -> LabAdmittedExecution:
        path = self._admission_path(claim_token)
        candidate, payload, _file_stat = self._read_regular_child(path, self.admitted_dir)
        if self._ack_message_id(candidate.name) != claim_token:
            raise InvalidCommandEnvelopeError(
                f"execution admission token does not match basename {candidate.name}"
            )
        try:
            admission = strict_model_validate_canonical_json(LabExecutionAdmission, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid execution admission {candidate.name}: {exc}"
            ) from exc
        if admission.claim.claim_token != claim_token:
            raise InvalidCommandEnvelopeError(
                f"execution admission identity does not match basename {candidate.name}"
            )
        return LabAdmittedExecution(path=candidate, admission=admission)

    def _cleanup_admission_temporaries_locked(self) -> None:
        directory_fd = self._open_managed_directory(self.admission_tmp_dir)
        try:
            for temporary in sorted(self._managed_paths(self.admission_tmp_dir, "*")):
                match = _ADMISSION_TEMP_NAME.fullmatch(temporary.name)
                if match is None:
                    raise InvalidCommandEnvelopeError(
                        f"unknown execution admission temporary: {temporary.name}"
                    )
                observed = os.stat(
                    temporary.name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                if not stat.S_ISREG(observed.st_mode) or observed.st_nlink not in {1, 2}:
                    raise InvalidCommandEnvelopeError(
                        f"unsafe execution admission temporary: {temporary.name}"
                    )
                if observed.st_nlink == 2:
                    token = UUID(match.group("claim_token"))
                    target = self._admission_path(token)
                    if not self._managed_entry_exists(target, target.parent):
                        raise InvalidCommandEnvelopeError(
                            "linked execution admission temporary has no marker"
                        )
                    target_stat = self._managed_entry_stat(target, target.parent)
                    if (target_stat.st_dev, target_stat.st_ino) != (
                        observed.st_dev,
                        observed.st_ino,
                    ):
                        raise InvalidCommandEnvelopeError(
                            "execution admission temporary conflicts with marker"
                        )
                self._guard_mutation()
                os.unlink(temporary.name, dir_fd=directory_fd)
                os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _publish_admission_locked(
        self,
        admission: LabExecutionAdmission,
    ) -> LabAdmittedExecution:
        target = self._admission_path(admission.claim.claim_token)
        temporary = self.admission_tmp_dir / (
            f"execution-admission-v1-{admission.claim.claim_token}-{uuid4().hex}.tmp"
        )
        temporary_name = temporary.name
        target_name = target.name
        temporary_directory_fd = self._open_managed_directory(self.admission_tmp_dir)
        target_directory_fd = self._open_managed_directory(self.admitted_dir)
        temporary_fd = -1
        try:
            temporary_fd = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=temporary_directory_fd,
            )
            payload = canonical_model_json_bytes(admission)
            offset = 0
            while offset < len(payload):
                offset += os.write(temporary_fd, payload[offset:])
            os.fsync(temporary_fd)
            try:
                self._guard_mutation()
                os.link(
                    temporary_name,
                    target_name,
                    src_dir_fd=temporary_directory_fd,
                    dst_dir_fd=target_directory_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                existing = self._load_admission_locked(admission.claim.claim_token)
                if existing.admission != admission:
                    raise RequestContentConflictError(
                        f"claim_token {admission.claim.claim_token} has conflicting admission"
                    ) from None
            os.fsync(target_directory_fd)
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
            with suppress(FileNotFoundError):
                self._before_admission_temporary_unlink(temporary)
                os.unlink(temporary_name, dir_fd=temporary_directory_fd)
                os.fsync(temporary_directory_fd)
            os.close(target_directory_fd)
            os.close(temporary_directory_fd)
        published = self._load_admission_locked(admission.claim.claim_token)
        if published.admission != admission:
            raise RequestContentConflictError(
                f"claim_token {admission.claim.claim_token} has conflicting admission"
            )
        return published

    @staticmethod
    def _before_admission_temporary_unlink(_temporary: Path) -> None:
        """Fault-injection boundary before dir-fd-bound admission cleanup."""

    def _revocation_locked(self, claim: LabSpoolClaim) -> LabRevokedClaim | None:
        path = self._revoked_path(claim.claim_token)
        if self._managed_entry_exists(path, path.parent):
            revoked = self._load_revocation_locked(claim.claim_token)
            if revoked.revocation.claim != claim:
                raise RequestContentConflictError(
                    f"claim_token {claim.claim_token} has conflicting revocation"
                )
            return revoked
        archived_path = self._archived_revoked_path(claim.claim_token)
        if self._managed_entry_exists(archived_path, archived_path.parent):
            revoked = self._load_archived_revocation_locked(claim.claim_token)
            if revoked.revocation.claim != claim:
                raise RequestContentConflictError(
                    f"claim_token {claim.claim_token} has conflicting archived revocation"
                )
            return revoked
        consumed_path = self._consumed_path(claim.claim_token)
        if not self._managed_entry_exists(consumed_path, consumed_path.parent):
            return None
        legacy = self._load_consumed_locked(claim.claim_token)
        if legacy.receipt.claim != claim:
            raise RequestContentConflictError(
                f"claim_token {claim.claim_token} has conflicting receipt"
            )
        if legacy.receipt.status != "revoked":
            return None
        return LabRevokedClaim(
            path=legacy.path,
            revocation=LabClaimRevocation(
                claim=claim,
                reason=legacy.receipt.reason or "legacy revocation",
            ),
        )

    def _unlink_current_locked(self, claim: LabSpoolClaim) -> None:
        path = self._current_path(claim.job_id, claim.shard_id)
        if not self._managed_entry_exists(path, path.parent):
            return
        marker = self._load_current_locked(claim.job_id, claim.shard_id)
        if marker.claim != claim:
            return
        name = self._direct_child_name(path, self.current_dir)
        directory_fd = self._open_managed_directory(self.current_dir)
        try:
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if not stat.S_ISREG(current.st_mode) or current.st_nlink != 1:
                raise InvalidCommandEnvelopeError(
                    f"current claim marker {name} is not a single-link regular file"
                )
            self._guard_mutation()
            os.unlink(name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _load_current_locked(self, job_id: UUID, shard_id: UUID) -> LabClaimHighWater:
        path = self._current_path(job_id, shard_id)
        candidate, payload, _file_stat = self._read_regular_child(path, self.current_dir)
        match = _CURRENT_CLAIM_NAME.fullmatch(candidate.name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid current claim basename: {candidate.name}")
        try:
            marker = strict_model_validate_canonical_json(LabClaimHighWater, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid current claim marker {candidate.name}: {exc}"
            ) from exc
        if (
            marker.claim.job_id != UUID(match.group("job_id"))
            or marker.claim.shard_id != UUID(match.group("shard_id"))
            or marker.claim.job_id != job_id
            or marker.claim.shard_id != shard_id
        ):
            raise InvalidCommandEnvelopeError(
                f"current claim marker identity does not match basename {candidate.name}"
            )
        return marker

    def _load_retired_locked(
        self,
        job_id: UUID,
        shard_id: UUID,
    ) -> LabRetiredClaimAuthority:
        path = self._retired_path(job_id, shard_id)
        candidate, payload, _file_stat = self._read_regular_child(path, self.retired_dir)
        match = _CURRENT_CLAIM_NAME.fullmatch(candidate.name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid retired claim basename: {candidate.name}")
        try:
            marker = strict_model_validate_canonical_json(LabRetiredClaimAuthority, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid retired claim marker {candidate.name}: {exc}"
            ) from exc
        if (
            marker.claim.job_id != UUID(match.group("job_id"))
            or marker.claim.shard_id != UUID(match.group("shard_id"))
            or marker.claim.job_id != job_id
            or marker.claim.shard_id != shard_id
        ):
            raise InvalidCommandEnvelopeError(
                f"retired claim marker identity does not match basename {candidate.name}"
            )
        return marker

    def _publish_retired_locked(
        self,
        marker: LabRetiredClaimAuthority,
    ) -> LabRetiredClaimAuthority:
        target = self._retired_path(marker.claim.job_id, marker.claim.shard_id)
        if self._managed_entry_exists(target, target.parent):
            existing = self._load_retired_locked(marker.claim.job_id, marker.claim.shard_id)
            if existing == marker:
                return existing
            if (
                marker.claim.claim_generation <= existing.claim.claim_generation
                or marker.claim.scheduler_fencing_token < existing.claim.scheduler_fencing_token
                or self._claim_order(marker.claim) <= self._claim_order(existing.claim)
            ):
                raise LabClaimSupersededError(
                    "retired claim does not advance durable cold high-water"
                )
        self._replace_managed_payload(
            target,
            canonical_model_json_bytes(marker),
        )
        published = self._load_retired_locked(marker.claim.job_id, marker.claim.shard_id)
        if published != marker:
            raise RequestContentConflictError("retired claim authority changed during publish")
        return published

    def _retired_blocks_locked(self, claim: LabSpoolClaim) -> bool:
        path = self._retired_path(claim.job_id, claim.shard_id)
        if not self._managed_entry_exists(path, path.parent):
            return False
        retired = self._load_retired_locked(claim.job_id, claim.shard_id)
        if retired.claim == claim:
            return True
        return (
            claim.claim_generation <= retired.claim.claim_generation
            or claim.scheduler_fencing_token < retired.claim.scheduler_fencing_token
            or self._claim_order(claim) <= self._claim_order(retired.claim)
        )

    def current(self, job_id: UUID, shard_id: UUID) -> LabClaimHighWater:
        with self._exclusive_lock():
            return self._load_current_locked(job_id, shard_id)

    def retired_high_water(
        self,
        job_id: UUID,
        shard_id: UUID,
    ) -> LabRetiredClaimAuthority:
        with self._exclusive_lock():
            return self._load_retired_locked(job_id, shard_id)

    def current_claims(self) -> tuple[LabSpoolClaim, ...]:
        with self._exclusive_lock():
            claims: list[LabSpoolClaim] = []
            for path in sorted(self._managed_paths(self.current_dir, "*.json")):
                match = _CURRENT_CLAIM_NAME.fullmatch(path.name)
                if match is None:
                    raise InvalidCommandEnvelopeError(
                        f"invalid current claim basename: {path.name}"
                    )
                claim = self._load_current_locked(
                    UUID(match.group("job_id")),
                    UUID(match.group("shard_id")),
                ).claim
                if self._revocation_locked(claim) is None:
                    claims.append(claim)
            return tuple(claims)

    def _publish_current_locked(self, marker: LabClaimHighWater) -> None:
        target = self._current_path(marker.claim.job_id, marker.claim.shard_id)
        self._replace_managed_payload(
            target,
            canonical_model_json_bytes(marker),
        )

    def is_current(self, claim: LabSpoolClaim) -> bool:
        validated = _validate_spool_claim(claim)
        with self._exclusive_lock():
            if self._revocation_locked(validated) is not None:
                return False
            if self._retired_blocks_locked(validated):
                return False
            if not self._managed_entry_exists(
                self._current_path(validated.job_id, validated.shard_id),
                self.current_dir,
            ):
                return False
            marker = self._load_current_locked(validated.job_id, validated.shard_id)
            return marker.claim == validated

    def is_revoked(self, claim: LabSpoolClaim) -> bool:
        validated = _validate_spool_claim(claim)
        with self._exclusive_lock():
            return self._revocation_locked(validated) is not None

    def revocation(self, claim_token: UUID) -> LabRevokedClaim:
        with self._exclusive_lock():
            path = self._revoked_path(claim_token)
            if self._managed_entry_exists(path, path.parent):
                return self._load_revocation_locked(claim_token)
            archived_path = self._archived_revoked_path(claim_token)
            if self._managed_entry_exists(archived_path, archived_path.parent):
                return self._load_archived_revocation_locked(claim_token)
            legacy = self._load_consumed_locked(claim_token)
            if legacy.receipt.status != "revoked":
                raise InvalidCommandEnvelopeError(f"claim {claim_token} has no revocation evidence")
            return LabRevokedClaim(
                path=legacy.path,
                revocation=LabClaimRevocation(
                    claim=legacy.receipt.claim,
                    reason=legacy.receipt.reason or "legacy revocation",
                ),
            )

    def execution_admission(self, claim_token: UUID) -> LabAdmittedExecution:
        with self._exclusive_lock():
            self._cleanup_admission_temporaries_locked()
            return self._load_admission_locked(claim_token)

    def admit_execution(self, claim: LabSpoolClaim) -> LabAdmittedExecution:
        """Persist the single execution point-of-admission under the claim lock."""
        validated = _validate_spool_claim(claim)
        with self._exclusive_lock():
            self._cleanup_admission_temporaries_locked()
            if self._revocation_locked(validated) is not None:
                raise LabClaimRevokedError(
                    f"claim {validated.claim_token} was revoked before execution admission"
                )
            if self._retired_blocks_locked(validated):
                raise LabClaimSupersededError(
                    "claim was terminally retired before execution admission"
                )
            current_path = self._current_path(validated.job_id, validated.shard_id)
            if not self._managed_entry_exists(current_path, current_path.parent):
                raise LabClaimSupersededError(
                    "claim has no durable high-water at execution admission"
                )
            marker = self._load_current_locked(validated.job_id, validated.shard_id)
            if marker.claim != validated:
                raise LabClaimSupersededError(
                    "claim is not the durable high-water at execution admission"
                )
            consumed_path = self._consumed_path(validated.claim_token)
            if not self._managed_entry_exists(consumed_path, consumed_path.parent):
                raise LabClaimNotConsumedError(
                    f"claim {validated.claim_token} has no consumed delivery receipt"
                )
            consumed = self._load_consumed_locked(validated.claim_token)
            if consumed.receipt.claim != validated:
                raise RequestContentConflictError(
                    f"claim_token {validated.claim_token} has conflicting receipt"
                )
            if consumed.receipt.status != "consumed":
                raise LabClaimRevokedError(
                    f"claim {validated.claim_token} has legacy revocation evidence"
                )
            admission = LabExecutionAdmission(
                claim=validated,
                delivery_content_hash=consumed.receipt.content_hash,
            )
            admission_path = self._admission_path(validated.claim_token)
            if self._managed_entry_exists(admission_path, admission_path.parent):
                existing = self._load_admission_locked(validated.claim_token)
                if existing.admission != admission:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} has conflicting admission"
                    )
                return existing
            return self._publish_admission_locked(admission)

    def is_admitted(self, claim: LabSpoolClaim) -> bool:
        """Return whether immutable execution-admission history exists for the claim."""
        validated = _validate_spool_claim(claim)
        with self._exclusive_lock():
            self._cleanup_admission_temporaries_locked()
            admission_path = self._admission_path(validated.claim_token)
            if not self._managed_entry_exists(admission_path, admission_path.parent):
                return False
            admission = self._load_admission_locked(validated.claim_token)
            if admission.admission.claim != validated:
                raise RequestContentConflictError(
                    f"claim_token {validated.claim_token} has conflicting admission"
                )
            return True

    def publish(
        self,
        claim: LabSpoolClaim,
    ) -> LabClaimSpoolEntry | LabConsumedClaim | LabRevokedClaim:
        validated = _validate_spool_claim(claim)
        payload = canonical_model_json_bytes(validated)
        with self._exclusive_lock():
            revoked = self._revocation_locked(validated)
            if revoked is not None:
                return revoked
            consumed_path = self._consumed_path(validated.claim_token)
            if self._managed_entry_exists(consumed_path, consumed_path.parent):
                consumed = self._load_consumed_locked(validated.claim_token)
                if consumed.receipt.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} was consumed with different content"
                    )
                return consumed
            if self._retired_blocks_locked(validated):
                raise LabClaimSupersededError(
                    "claim does not advance the durable retired shard high-water"
                )
            if self._managed_entry_exists(
                self._current_path(validated.job_id, validated.shard_id),
                self.current_dir,
            ):
                current = self._load_current_locked(validated.job_id, validated.shard_id)
            else:
                current = None
            if (
                current is not None
                and current.claim != validated
                and (
                    validated.claim_generation <= current.claim.claim_generation
                    or validated.scheduler_fencing_token < current.claim.scheduler_fencing_token
                    or self._claim_order(validated) <= self._claim_order(current.claim)
                )
            ):
                raise LabClaimSupersededError("claim does not advance the durable shard high-water")
            pending = self._pending_for_message_locked(validated.claim_token)
            if pending is not None:
                existing = self.load(pending)
                if existing.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} already has different content"
                    )
            if pending is not None:
                entry = existing
            else:
                sequence = self._next_sequence_locked()
                target = self.pending_dir / f"{sequence:020d}-{validated.claim_token}.json"
                if not self._publish_no_clobber(target, payload):
                    raise RequestContentConflictError(
                        f"delivery sequence {sequence} already exists"
                    )
                entry = self.load(target)
            if current is None or current.claim != validated:
                self._publish_current_locked(LabClaimHighWater(claim=validated))
            return entry

    def _v2_published_entry_identity(
        self,
        entry: LabClaimSpoolEntry,
        final_claim: LabShardClaimV2,
    ) -> tuple[str, str, str]:
        """Return the stable locator, entry id, and digest for a current final claim."""

        validated_claim = LabShardClaimV2.model_validate(final_claim, strict=True)
        if validated_claim.source_use_plan is None:
            raise ValueError("v2 spool receipt requires a final bound claim")
        published = self.load(entry.path)
        if published != entry or published.claim != validated_claim:
            raise ValueError("spool entry conflicts with final claim")
        current = self._load_current_locked(validated_claim.job_id, validated_claim.shard_id)
        if current.claim != validated_claim:
            raise ValueError("spool entry does not hold the current final claim")
        try:
            locator = published.path.relative_to(self.root).as_posix()
        except ValueError as exc:
            raise ValueError("spool entry is outside the spool root") from exc
        if not locator.startswith("pending/") or "/../" in f"/{locator}":
            raise ValueError("spool entry locator is not a pending relative path")
        content_digest = hashlib.sha256(canonical_model_json_bytes(validated_claim)).hexdigest()
        entry_id = hashlib.sha256(
            canonical_json_bytes(
                {
                    "content_digest": content_digest,
                    "contract": "rquant-lab-claim-spool-entry-id/v2",
                    "locator": locator,
                }
            )
        ).hexdigest()
        return locator, entry_id, content_digest

    def v2_published_entry_identity(
        self,
        *,
        entry: LabClaimSpoolEntry,
        final_claim: LabShardClaimV2,
    ) -> tuple[str, str, str]:
        """Resolve the stable identity used by the v2 receipt factory."""

        with self._exclusive_lock():
            return self._v2_published_entry_identity(entry, final_claim)

    def _persist_v2_publish_receipt_bytes(
        self,
        *,
        entry: LabClaimSpoolEntry,
        final_claim: LabShardClaimV2,
        candidate_bytes: bytes,
    ) -> bytes:
        """Atomically retain the first receipt for an immutable final-claim entry."""

        with self._exclusive_lock():
            _locator, entry_id, _content_digest = self._v2_published_entry_identity(
                entry,
                final_claim,
            )
            target = self.publish_receipt_dir / f"{entry_id}.json"
            if self._managed_entry_exists(target, self.publish_receipt_dir):
                _candidate, stored, _stat = self._read_regular_child(
                    target,
                    self.publish_receipt_dir,
                )
                return stored
            if not self._publish_no_clobber(target, candidate_bytes):
                _candidate, stored, _stat = self._read_regular_child(
                    target,
                    self.publish_receipt_dir,
                )
                return stored
            _candidate, stored, _stat = self._read_regular_child(target, self.publish_receipt_dir)
            if stored != candidate_bytes:
                raise InvalidCommandEnvelopeError("v2 publish receipt sidecar readback mismatch")
            return stored

    def v2_publish_receipt_authority_bytes(self) -> bytes:
        """Return the stable, non-path authority descriptor for this spool root."""

        with self._exclusive_lock():
            target = self.publish_receipt_authority_path
            if not self._managed_entry_exists(target, self.publish_receipt_dir):
                if self._publish_receipt_publisher is None:
                    raise InvalidCommandEnvelopeError(
                        "v2 publish receipt authority publisher is not configured"
                    )
                candidate = canonical_json_bytes(
                    {
                        "contract": "rquant-lab-claim-spool-receipt-authority/v2",
                        "publisher_authority": self._publish_receipt_publisher.model_dump(
                            mode="json"
                        ),
                        "root_id": uuid4().hex,
                        "schema_version": 2,
                        "sidecar_protocol": "rquant-lab-claim-spool-publish-receipt/v2",
                    }
                )
                if not self._publish_no_clobber(target, candidate):
                    pass
            _candidate, stored, _stat = self._read_regular_child(
                target,
                self.publish_receipt_dir,
            )
            try:
                parsed = strict_json_loads(stored)
            except Exception as exc:
                raise InvalidCommandEnvelopeError(
                    "v2 publish receipt authority descriptor is invalid"
                ) from exc
            if (
                not isinstance(parsed, dict)
                or set(parsed)
                != {
                    "contract",
                    "publisher_authority",
                    "root_id",
                    "schema_version",
                    "sidecar_protocol",
                }
                or parsed.get("contract") != "rquant-lab-claim-spool-receipt-authority/v2"
                or parsed.get("schema_version") != 2
                or parsed.get("sidecar_protocol") != "rquant-lab-claim-spool-publish-receipt/v2"
                or not isinstance(parsed.get("root_id"), str)
                or re.fullmatch(r"[0-9a-f]{32}", parsed["root_id"]) is None
                or parsed.get("publisher_authority")
                != (
                    None
                    if self._publish_receipt_publisher is None
                    else self._publish_receipt_publisher.model_dump(mode="json")
                )
                or canonical_json_bytes(parsed) != stored
            ):
                raise InvalidCommandEnvelopeError(
                    "v2 publish receipt authority descriptor is invalid"
                )
            return stored

    def load_v2_publish_receipt_sidecar(self, entry_id: str) -> bytes:
        """Read one immutable v2 receipt sidecar through the managed no-follow path."""

        if _PUBLISH_RECEIPT_ENTRY_NAME.fullmatch(f"{entry_id}.json") is None:
            raise ValueError("v2 publish receipt entry id is invalid")
        with self._exclusive_lock():
            target = self.publish_receipt_dir / f"{entry_id}.json"
            _candidate, stored, _stat = self._read_regular_child(
                target,
                self.publish_receipt_dir,
            )
            return stored

    def revoke(self, claim: LabSpoolClaim, *, reason: str) -> LabRevokedClaim:
        """Durably fence an exact delivery before removing its spool visibility."""
        validated = _validate_spool_claim(claim)
        normalized_reason = " ".join(reason.split())
        if not normalized_reason:
            raise ValueError("revoke reason must not be empty")
        revocation = LabClaimRevocation(
            claim=validated,
            reason=normalized_reason,
        )
        with self._exclusive_lock():
            self._cleanup_admission_temporaries_locked()
            receipt_path = self._consumed_path(validated.claim_token)
            if self._managed_entry_exists(receipt_path, receipt_path.parent):
                existing = self._load_consumed_locked(validated.claim_token)
                if existing.receipt.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} has conflicting receipt"
                    )
            revoked_path = self._revoked_path(validated.claim_token)
            if self._managed_entry_exists(revoked_path, revoked_path.parent):
                revoked = self._load_revocation_locked(validated.claim_token)
                if revoked.revocation != revocation:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} has conflicting revocation"
                    )
            else:
                created = self._publish_no_clobber(
                    revoked_path,
                    canonical_model_json_bytes(revocation),
                )
                if not created:
                    revoked = self._load_revocation_locked(validated.claim_token)
                    if revoked.revocation != revocation:
                        raise RequestContentConflictError(
                            f"claim_token {validated.claim_token} has conflicting revocation"
                        )
                revoked = self._load_revocation_locked(validated.claim_token)

            self._unlink_current_locked(validated)
            pending = self._pending_for_message_locked(validated.claim_token)
            if pending is not None:
                entry = self.load(pending)
                if entry.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} has conflicting pending delivery"
                    )
                self._unlink_pending(
                    entry.path,
                    device=entry.device,
                    inode=entry.inode,
                )
            return revoked

    def _archive_revocation_locked(self, claim: LabSpoolClaim) -> None:
        source = self._revoked_path(claim.claim_token)
        if not self._managed_entry_exists(source, source.parent):
            archived = self._load_archived_revocation_locked(claim.claim_token)
            if archived.revocation.claim != claim:
                raise RequestContentConflictError(
                    f"claim_token {claim.claim_token} has conflicting archived revocation"
                )
            return
        revoked = self._load_revocation_locked(claim.claim_token)
        if revoked.revocation.claim != claim:
            raise RequestContentConflictError(
                f"claim_token {claim.claim_token} has conflicting revocation"
            )
        target = self._archived_revoked_path(claim.claim_token)
        created = self._publish_no_clobber(
            target,
            canonical_model_json_bytes(revoked.revocation),
        )
        archived = self._load_archived_revocation_locked(claim.claim_token)
        if archived.revocation != revoked.revocation:
            raise RequestContentConflictError(
                f"claim_token {claim.claim_token} has conflicting archived revocation"
            )
        if created or archived.revocation == revoked.revocation:
            source_stat = self._managed_entry_stat(source, self.revoked_dir)
            self._unlink_managed_entry(
                source,
                self.revoked_dir,
                expected=source_stat,
            )

    def retire(
        self,
        claim: LabSpoolClaim,
        *,
        outcome: Literal["accepted", "revoked"],
        reason: str,
    ) -> LabRetiredClaimAuthority:
        """Move exact terminal delivery authority out of the scheduler hot set."""
        validated = _validate_spool_claim(claim)
        normalized_reason = " ".join(reason.split())
        if not normalized_reason:
            raise ValueError("retire reason must not be empty")
        marker = LabRetiredClaimAuthority(
            claim=validated,
            outcome=outcome,
            reason=normalized_reason,
        )
        with self._exclusive_lock():
            if outcome == "accepted":
                consumed_path = self._consumed_path(validated.claim_token)
                if not self._managed_entry_exists(consumed_path, consumed_path.parent):
                    raise LabClaimNotConsumedError(
                        "accepted retirement requires immutable consumed delivery history"
                    )
                consumed = self._load_consumed_locked(validated.claim_token)
                if consumed.receipt.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} has conflicting receipt"
                    )
                if consumed.receipt.status != "consumed":
                    raise LabClaimRevokedError(
                        "accepted retirement conflicts with legacy revocation history"
                    )
                if self._revocation_locked(validated) is not None:
                    raise LabClaimRevokedError(
                        "accepted retirement conflicts with durable revocation history"
                    )
            elif self._revocation_locked(validated) is None:
                raise LabClaimRevokedError("revoked retirement requires durable revocation history")
            retired = self._publish_retired_locked(marker)
            self._unlink_current_locked(validated)
            pending = self._pending_for_message_locked(validated.claim_token)
            if pending is not None:
                entry = self.load(pending)
                if entry.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} has conflicting pending delivery"
                    )
                self._unlink_pending(
                    entry.path,
                    device=entry.device,
                    inode=entry.inode,
                )
            if outcome == "revoked":
                self._archive_revocation_locked(validated)
            return retired

    def hot_delivery_batch(
        self,
        *,
        limit: int,
        cursor: LabPendingClaimCursor | None = None,
    ) -> LabHotClaimBatch:
        """Read bounded pending plus every active/unreconciled hot authority entry."""
        if limit < 1:
            raise ValueError("hot delivery batch limit must be positive")
        with self._exclusive_lock():
            durable_cursor = self._load_pending_cursor_locked()
            if cursor is not None:
                requested_cursor = LabPendingClaimCursor.model_validate(cursor)
                if requested_cursor != durable_cursor:
                    raise RequestContentConflictError(
                        "pending claim cursor conflicts with durable authority cursor"
                    )
            namespace_order: tuple[LabHotClaimNamespace, ...] = (
                "pending",
                "current",
                "revoked",
            )
            directories = {
                "pending": self.pending_dir,
                "current": self.current_dir,
                "revoked": self.revoked_dir,
            }
            paths_by_namespace = {
                namespace: self._hot_namespace_paths(directories[namespace])
                for namespace in namespace_order
            }
            claims: dict[UUID, LabSpoolClaim] = {}
            pending_paths, next_cursor = self._pending_slice(
                paths_by_namespace["pending"],
                cursor=durable_cursor,
                limit=limit,
            )
            selected_by_namespace = {
                "pending": pending_paths,
                "current": paths_by_namespace["current"],
                "revoked": paths_by_namespace["revoked"],
            }
            scanned_namespaces: list[LabHotClaimNamespace] = []
            inspected = sum(len(paths) for paths in selected_by_namespace.values())
            for namespace in namespace_order:
                selected = selected_by_namespace[namespace]
                if selected:
                    scanned_namespaces.append(namespace)
                for path in selected:
                    if namespace == "pending":
                        claim = self.load(path).claim
                    elif namespace == "current":
                        match = _CURRENT_CLAIM_NAME.fullmatch(path.name)
                        if match is None:
                            raise InvalidCommandEnvelopeError(
                                f"invalid current claim basename: {path.name}"
                            )
                        claim = self._load_current_locked(
                            UUID(match.group("job_id")),
                            UUID(match.group("shard_id")),
                        ).claim
                    else:
                        token = self._ack_message_id(path.name)
                        claim = self._load_revocation_locked(token).revocation.claim
                    existing = claims.get(claim.claim_token)
                    if existing is not None and existing != claim:
                        raise RequestContentConflictError(
                            f"claim_token {claim.claim_token} has conflicting hot evidence"
                        )
                    claims[claim.claim_token] = claim
            if next_cursor != durable_cursor:
                self._publish_pending_cursor_locked(next_cursor)
            return LabHotClaimBatch(
                claims=tuple(claims.values()),
                next_cursor=next_cursor,
                scanned_namespaces=tuple(scanned_namespaces),
                inspected=inspected,
            )

    def delivery_claims(self) -> tuple[LabSpoolClaim, ...]:
        """Return every non-revoked claim requiring SQLite reconciliation."""
        with self._exclusive_lock():
            claims: dict[UUID, LabSpoolClaim] = {}

            def remember(claim: LabSpoolClaim) -> None:
                existing = claims.get(claim.claim_token)
                if existing is not None and existing != claim:
                    raise RequestContentConflictError(
                        f"claim_token {claim.claim_token} has conflicting delivery evidence"
                    )
                claims[claim.claim_token] = claim

            for path in sorted(self._managed_paths(self.pending_dir, "*.json")):
                remember(self.load(path).claim)
            for path in sorted(self._managed_paths(self.current_dir, "*.json")):
                match = _CURRENT_CLAIM_NAME.fullmatch(path.name)
                if match is None:
                    raise InvalidCommandEnvelopeError(
                        f"invalid current claim basename: {path.name}"
                    )
                remember(
                    self._load_current_locked(
                        UUID(match.group("job_id")),
                        UUID(match.group("shard_id")),
                    ).claim
                )
            for path in sorted(self._managed_paths(self.ack_dir, "*.json")):
                token = self._ack_message_id(path.name)
                receipt = self._load_consumed_locked(token).receipt
                if receipt.status == "consumed" and self._revocation_locked(receipt.claim) is None:
                    remember(receipt.claim)
            return tuple(
                sorted(
                    claims.values(),
                    key=lambda item: (
                        str(item.job_id),
                        str(item.shard_id),
                        item.claim_generation,
                        str(item.claim_token),
                    ),
                )
            )

    def reconcile_current(self) -> tuple[LabClaimReconcileResult, ...]:
        # Lock order invariant: claim snapshots are complete before callbacks may take
        # the report/artifact lock. No callback runs while the claim lock is held.
        return self.reconcile_claims(self.current_claims())

    def reconcile_claims(
        self,
        claims: tuple[LabSpoolClaim, ...],
    ) -> tuple[LabClaimReconcileResult, ...]:
        """Run claim hooks for an authority-selected snapshot without directory scans."""
        results: list[LabClaimReconcileResult] = []
        for claim in claims:
            if self._claim_advance_hook is None:
                results.append(
                    LabClaimReconcileResult(
                        claim_token=claim.claim_token,
                        status="not_configured",
                    )
                )
                continue
            try:
                self._claim_advance_hook(claim)
            except Exception as exc:
                message = " ".join((str(exc) or type(exc).__name__).split())[:400]
                results.append(
                    LabClaimReconcileResult(
                        claim_token=claim.claim_token,
                        status="failed",
                        error=f"{type(exc).__name__}: {message}",
                    )
                )
            else:
                results.append(
                    LabClaimReconcileResult(
                        claim_token=claim.claim_token,
                        status="reconciled",
                    )
                )
        return tuple(results)

    def load(self, path: Path) -> LabClaimSpoolEntry:
        candidate, payload, file_stat = self._read_regular_child(Path(path), self.pending_dir)
        identity = LabSpoolFileIdentity(
            path=candidate,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )
        try:
            _sequence, filename_token = self._message_name_parts(candidate.name)
            claim = _parse_spool_claim(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid shard claim {candidate.name}: {exc}",
                file_identity=identity,
            ) from exc
        if claim.claim_token != filename_token:
            raise InvalidCommandEnvelopeError(
                f"claim_token does not match basename {candidate.name}",
                file_identity=identity,
            )
        return LabClaimSpoolEntry(
            path=candidate,
            claim=claim,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )

    def pending(self, *, limit: int | None = None) -> tuple[LabClaimSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def consume(self, entry: LabClaimSpoolEntry) -> LabSpoolClaim:
        with self._exclusive_lock():
            current = self.load(entry.path)
            if (current.device, current.inode) != (entry.device, entry.inode):
                raise InvalidCommandEnvelopeError("pending claim was replaced before consume")
            if current.claim != entry.claim:
                raise InvalidCommandEnvelopeError("pending claim changed before consume")
            if self._revocation_locked(entry.claim) is not None:
                self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
                raise LabClaimRevokedError(f"claim {entry.claim.claim_token} was revoked")
            if self._retired_blocks_locked(entry.claim):
                raise LabClaimSupersededError("pending claim was terminally retired before consume")
            consumed_path = self._consumed_path(entry.claim.claim_token)
            if self._managed_entry_exists(consumed_path, consumed_path.parent):
                consumed = self._load_consumed_locked(entry.claim.claim_token)
                if consumed.receipt.claim != entry.claim:
                    raise RequestContentConflictError(
                        f"claim_token {entry.claim.claim_token} has conflicting receipt"
                    )
                self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
                if consumed.receipt.status == "revoked":
                    raise LabClaimRevokedError(f"claim {entry.claim.claim_token} was revoked")
                raise LabClaimAlreadyConsumedError(
                    f"claim {entry.claim.claim_token} was already consumed"
                )
            marker = self._load_current_locked(entry.claim.job_id, entry.claim.shard_id)
            if marker.claim != entry.claim:
                raise LabClaimSupersededError("pending claim is not the durable shard high-water")
            receipt = LabClaimDeliveryReceipt(claim=entry.claim)
            created = self._publish_no_clobber(
                consumed_path,
                canonical_model_json_bytes(receipt),
            )
            if not created:
                consumed = self._load_consumed_locked(entry.claim.claim_token)
                if consumed.receipt != receipt:
                    raise RequestContentConflictError(
                        f"claim_token {entry.claim.claim_token} has conflicting receipt"
                    )
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
        return entry.claim


class LabReportSpool(_TypedSpoolBase):
    """Worker-to-scheduler durable report channel with exactly-once receipts."""

    @contextmanager
    def evidence_lock(self) -> Iterator[None]:
        """Serialize report evidence mutation with artifact isolation."""
        with self._exclusive_lock():
            yield

    def pending_locked(self) -> tuple[LabReportSpoolEntry, ...]:
        paths = tuple(
            sorted(
                self._managed_paths(self.pending_dir, "*.json"),
                key=self._delivery_key,
            )
        )
        return tuple(self.load(path) for path in paths)

    def receipt_paths_locked(self) -> tuple[Path, ...]:
        return tuple(sorted(self._managed_paths(self.ack_dir, "*.json")))

    def publish(self, report: LabWorkerReport) -> LabReportSpoolEntry | LabAcknowledgedReport:
        validated = LabWorkerReport.model_validate(report)
        payload = canonical_model_json_bytes(validated)
        with self._exclusive_lock():
            ack_path = self.ack_dir / f"{validated.report_id}.json"
            pending = self._pending_for_message_locked(validated.report_id)
            if self._managed_entry_exists(ack_path, self.ack_dir):
                receipt = self.load_receipt(ack_path)
                if receipt.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"report_id {validated.report_id} already has different content"
                    )
                return LabAcknowledgedReport(path=ack_path, receipt=receipt)
            if pending is not None:
                existing = self.load(pending)
                if existing.report.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"report_id {validated.report_id} already has different content"
                    )
                return existing
            sequence = self._next_sequence_locked()
            target = self.pending_dir / f"{sequence:020d}-{validated.report_id}.json"
            if not self._publish_no_clobber(target, payload):
                raise RequestContentConflictError(f"delivery sequence {sequence} already exists")
            return self.load(target)

    def load(self, path: Path) -> LabReportSpoolEntry:
        candidate, payload, file_stat = self._read_regular_child(Path(path), self.pending_dir)
        identity = LabSpoolFileIdentity(
            path=candidate,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )
        try:
            _sequence, filename_id = self._message_name_parts(candidate.name)
            report = strict_model_validate_canonical_json(LabWorkerReport, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid worker report {candidate.name}: {exc}",
                file_identity=identity,
            ) from exc
        if report.report_id != filename_id:
            raise InvalidCommandEnvelopeError(
                f"report_id does not match basename {candidate.name}",
                file_identity=identity,
            )
        return LabReportSpoolEntry(
            path=candidate,
            report=report,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )

    def pending(self, *, limit: int | None = None) -> tuple[LabReportSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def ack(
        self,
        entry: LabReportSpoolEntry,
        receipt: LabReportReceipt,
    ) -> LabAcknowledgedReport:
        if (
            receipt.report_id != entry.report.report_id
            or receipt.content_hash != entry.report.content_hash
            or receipt.job_id != entry.report.job_id
            or receipt.shard_id != entry.report.shard_id
        ):
            raise ValueError("receipt does not match worker report")
        if receipt.claim_token is not None and (
            receipt.worker_id,
            receipt.claim_token,
            receipt.claim_generation,
            receipt.scheduler_fencing_token,
            receipt.report_type,
            receipt.result_manifest_hash,
        ) != (
            entry.report.worker_id,
            entry.report.claim_token,
            entry.report.claim_generation,
            entry.report.scheduler_fencing_token,
            entry.report.body.report_type,
            (
                entry.report.body.result_manifest_hash
                if isinstance(entry.report.body, LabShardSucceeded)
                else None
            ),
        ):
            raise ValueError("receipt attempt identity does not match worker report")
        with self._exclusive_lock():
            current = self.load(entry.path)
            if (current.device, current.inode) != (entry.device, entry.inode):
                raise InvalidCommandEnvelopeError("pending report was replaced before ack")
            if current.report != entry.report:
                raise InvalidCommandEnvelopeError("pending report changed before ack")
            target = self.ack_dir / f"{receipt.report_id}.json"
            created = self._publish_no_clobber(
                target,
                canonical_model_json_bytes(receipt),
            )
            if not created and self.load_receipt(target) != receipt:
                raise RequestContentConflictError(
                    f"report_id {receipt.report_id} already has a different receipt"
                )
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
            return LabAcknowledgedReport(path=target, receipt=receipt)

    def load_receipt(self, path: Path) -> LabReportReceipt:
        candidate, payload, _file_stat = self._read_regular_child(Path(path), self.ack_dir)
        filename_id = self._ack_message_id(candidate.name)
        try:
            receipt = strict_model_validate_canonical_json(LabReportReceipt, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid report receipt {candidate.name}: {exc}"
            ) from exc
        if receipt.report_id != filename_id:
            raise InvalidCommandEnvelopeError(
                f"report receipt id does not match basename {candidate.name}"
            )
        return receipt
