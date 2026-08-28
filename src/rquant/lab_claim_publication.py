"""Canonical, append-only publication records for externally sourced Lab claims."""

from __future__ import annotations

import hashlib
import hmac
import re
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
)
from rquant.lab_shard_protocol import LabClaimSpool, LabClaimSpoolEntry, LabShardClaimV2
from rquant.lab_source_stage import (
    LabSourceStageBinding,
    LabSourceStageRecord,
    LabSourceStageState,
    LabSourceStageStoreAuthority,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
)
from rquant.source_operation_contracts import SourceUsePlanV2
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_SAFE_WORKER_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,198}[A-Za-z0-9])?$"
V2_UNASSIGNED_WORKER_ID = "v2-unassigned"


class ClaimPublicationStatus(StrEnum):
    HELD_SOURCE = "HELD_SOURCE"
    SOURCE_QUEUED = "SOURCE_QUEUED"
    READY_TO_PUBLISH = "READY_TO_PUBLISH"
    PUBLISHED = "PUBLISHED"
    ABORTED = "ABORTED"


class ClaimPublicationAuditAction(StrEnum):
    CREATED = "created"
    TRANSITIONED = "transitioned"
    REPLAYED = "replayed"
    CONFLICT = "conflict"


class _FrozenPublicationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class LabClaimPublicationFinalizerRootKey:
    """Narrow root-MAC capability injected only into finalizer composition."""

    __slots__ = ("_descriptor", "_secret", "_key_digest")

    def __init__(self, *, secret: bytes) -> None:
        if type(secret) is not bytes or len(secret) < 32:
            raise ValueError("finalizer root key must contain at least 32 bytes")
        self._secret = secret
        self._key_digest = hashlib.sha256(
            b"rquant-lab-claim-publication-finalizer-root/v1\0" + secret
        ).hexdigest()
        self._descriptor = "root-" + self._key_digest[:32]

    @property
    def descriptor(self) -> str:
        return self._descriptor

    @property
    def key_digest(self) -> str:
        return self._key_digest

    def sign(self, payload: bytes) -> str:
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, mac: str) -> bool:
        return hmac.compare_digest(self.sign(payload), mac)


class LabClaimPublicationFinalizerAuthority:
    """Opaque, durable, fenced capability for the V2 C/D publication mutations.

    The capability token is intentionally not serializable and is never persisted
    directly.  Its durable SHA-256 commitment is checked with the current store
    identity and fence by each C/D transaction.
    """

    __slots__ = (
        "canonical_job_store_path",
        "database_generation",
        "store_id",
        "schema_version",
        "implementation_digest",
        "owner_id",
        "lease_id",
        "fencing_token",
        "root_descriptor",
        "root_key_digest",
        "expires_at",
        "lease_commitment",
        "authority_mac",
        "_root_key",
        "_trust_certificate",
        "_trust_verifier",
        "_runtime_signer",
    )

    scope = "rquant-lab-claim-publication-finalizer/v2"

    def __init__(
        self,
        *,
        canonical_job_store_path: str,
        database_generation: tuple[int, int],
        store_id: str,
        schema_version: int,
        implementation_digest: str,
        owner_id: str,
        lease_id: int,
        fencing_token: int,
        root_key: LabClaimPublicationFinalizerRootKey,
        expires_at: datetime,
        lease_commitment: str,
        authority_mac: str,
        trust_certificate: LabClaimFinalizerTrustCertificate | None = None,
        trust_verifier: LabClaimFinalizerTrustVerifier | None = None,
        runtime_signer: object | None = None,
    ) -> None:
        if not Path(canonical_job_store_path).is_absolute():
            raise ValueError("finalizer authority store path must be absolute")
        if not owner_id or lease_id < 1 or fencing_token < 1:
            raise ValueError("finalizer authority identity is invalid")
        if type(root_key) is not LabClaimPublicationFinalizerRootKey:
            raise TypeError("finalizer authority requires an exact root key")
        expiry = _aware_utc(expires_at)
        if not re.fullmatch(_HASH_PATTERN, lease_commitment) or not re.fullmatch(
            _HASH_PATTERN, authority_mac
        ):
            raise ValueError("finalizer authority MAC is invalid")
        self.canonical_job_store_path = canonical_job_store_path
        self.database_generation = database_generation
        self.store_id = store_id
        self.schema_version = schema_version
        self.implementation_digest = implementation_digest
        self.owner_id = owner_id
        self.lease_id = lease_id
        self.fencing_token = fencing_token
        self.root_descriptor = root_key.descriptor
        self.root_key_digest = root_key.key_digest
        self.expires_at = expiry
        self.lease_commitment = lease_commitment
        self.authority_mac = authority_mac
        self._root_key = root_key
        self._trust_certificate = trust_certificate
        self._trust_verifier = trust_verifier
        self._runtime_signer = runtime_signer

    def root_mac_matches(self, payload: bytes, *, root_descriptor: str, key_digest: str) -> bool:
        return (
            self.root_descriptor == root_descriptor
            and self.root_key_digest == key_digest
            and self._root_key.verify(payload, self.authority_mac)
        )


class LabClaimPublicationIdentity(_FrozenPublicationModel):
    attempt_id: UUID
    job_id: UUID
    shard_id: UUID
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    worker_id: str = Field(pattern=_SAFE_WORKER_ID_PATTERN, min_length=1, max_length=200)
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_claim(cls, claim: LabShardClaimV2) -> LabClaimPublicationIdentity:
        validated = LabShardClaimV2.model_validate(claim, strict=True)
        return cls(
            attempt_id=validated.claim_token,
            job_id=validated.job_id,
            shard_id=validated.shard_id,
            claim_token=validated.claim_token,
            claim_generation=validated.claim_generation,
            scheduler_fencing_token=validated.scheduler_fencing_token,
            worker_id=validated.worker_id,
            spec_hash=validated.spec_hash,
            plan_hash=validated.definition.plan_hash,
            payload_hash=validated.definition.payload_hash,
        )

    @model_validator(mode="after")
    def validate_attempt_alias(self) -> LabClaimPublicationIdentity:
        if self.attempt_id != self.claim_token:
            raise ValueError("attempt_id and claim_token must identify the same v2 claim")
        return self


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("claim publication timestamps must be timezone-aware")
    try:
        offset = value.utcoffset()
    except (OverflowError, ValueError) as exc:
        raise ValueError("claim publication timestamp is outside the UTC datetime domain") from exc
    if offset is None:
        raise ValueError("claim publication timestamps must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError("claim publication timestamp is outside the UTC datetime domain") from exc


def _validate_deadline_order(
    source_wait_deadline: datetime, publication_deadline: datetime
) -> None:
    if source_wait_deadline > publication_deadline:
        raise ValueError("source_wait_deadline must not exceed publication_deadline")


def _canonical_json_object(value: bytes, *, field: str) -> bytes:
    try:
        decoded = strict_canonical_json_loads(value)
    except Exception as exc:
        raise ValueError(f"{field} must contain canonical JSON bytes") from exc
    if not isinstance(decoded, dict):
        raise ValueError(f"{field} must contain a canonical JSON object")
    return value


def _canonical_model(value: bytes, model: type[BaseModel], *, field: str) -> BaseModel:
    try:
        parsed = strict_model_validate_canonical_json(model, value.decode("utf-8"))
    except (UnicodeDecodeError, ValidationError, ValueError, TypeError) as exc:
        raise ValueError(f"{field} must contain canonical {model.__name__} bytes") from exc
    if canonical_model_json_bytes(parsed) != value:
        raise ValueError(f"{field} must contain canonical {model.__name__} bytes")
    return parsed


def _require_sha256(value: bytes, observed: str, *, field: str) -> None:
    if hashlib.sha256(value).hexdigest() != observed:
        raise ValueError(f"{field} conflicts with canonical bytes")


def _claim_from_preimage(value: bytes, *, field: str) -> LabShardClaimV2:
    parsed = _canonical_model(value, LabShardClaimV2, field=field)
    assert isinstance(parsed, LabShardClaimV2)
    if parsed.source_use_plan is not None or parsed.source_plan_hash is not None:
        raise ValueError("claim preimage must be an unbound LabShardClaimV2")
    return parsed


def _require_claim_identity(claim: LabShardClaimV2, identity: LabClaimPublicationIdentity) -> None:
    if LabClaimPublicationIdentity.from_claim(claim) != identity:
        raise ValueError("claim bytes do not match the publication attempt identity")


class HeldDraft(_FrozenPublicationModel):
    """A-only input: no source-stage, outcome, plan, or spool facts are known yet."""

    identity: LabClaimPublicationIdentity
    claim_preimage_bytes: bytes
    claim_preimage_hash: str = Field(pattern=_HASH_PATTERN)
    claim_protocol: Literal["rquant-lab-shard-claim"] = "rquant-lab-shard-claim"
    claim_protocol_version: Literal["v2"] = "v2"
    source_wait_deadline: datetime
    publication_deadline: datetime

    @field_validator("source_wait_deadline", "publication_deadline")
    @classmethod
    def validate_deadline(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_held_draft(self) -> HeldDraft:
        _validate_deadline_order(self.source_wait_deadline, self.publication_deadline)
        _require_sha256(
            self.claim_preimage_bytes,
            self.claim_preimage_hash,
            field="claim_preimage_hash",
        )
        _require_claim_identity(
            _claim_from_preimage(self.claim_preimage_bytes, field="claim_preimage_bytes"),
            self.identity,
        )
        return self


def source_stage_store_authority_from_canonical_bytes(
    value: bytes,
) -> LabSourceStageStoreAuthority:
    """Decode the source-stage module's canonical authority descriptor."""

    parsed = _canonical_model(
        value,
        LabSourceStageStoreAuthority,
        field="source_stage_authority_bytes",
    )
    assert isinstance(parsed, LabSourceStageStoreAuthority)
    return parsed


class QueueBinding(_FrozenPublicationModel):
    """B-only source dispatch facts, written once after an A record exists."""

    source_stage_binding_bytes: bytes
    source_stage_binding_hash: str = Field(pattern=_HASH_PATTERN)
    source_intent_bytes: bytes
    source_intent_hash: str = Field(pattern=_HASH_PATTERN)
    source_operation_id: str = Field(pattern=_HASH_PATTERN)
    source_operation_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_queue_binding(self) -> QueueBinding:
        _require_sha256(
            self.source_stage_binding_bytes,
            self.source_stage_binding_hash,
            field="source_stage_binding_hash",
        )
        _require_sha256(
            self.source_intent_bytes,
            self.source_intent_hash,
            field="source_intent_hash",
        )
        binding = _canonical_model(
            self.source_stage_binding_bytes,
            LabSourceStageBinding,
            field="source_stage_binding_bytes",
        )
        intent = _canonical_model(
            self.source_intent_bytes,
            SourceBrokerV2JobIntentEnvelope,
            field="source_intent_bytes",
        )
        assert isinstance(binding, LabSourceStageBinding)
        assert isinstance(intent, SourceBrokerV2JobIntentEnvelope)
        if (intent.operation_id, intent.operation_hash) != (
            self.source_operation_id,
            self.source_operation_hash,
        ):
            raise ValueError("source operation identity conflicts with source intent")
        # The source-stage model owns the detailed binding rules; materialize its
        # public QUEUED record instead of duplicating its source-broker checks here.
        LabSourceStageRecord(
            binding=binding,
            state=LabSourceStageState.QUEUED,
            intent=intent,
            intent_bytes=self.source_intent_bytes,
            intent_hash=intent.intent_hash,
            operation_id=intent.operation_id,
            operation_hash=intent.operation_hash,
            attempt_identity_hash=binding.attempt_identity_hash,
            created_at=datetime(1970, 1, 1, tzinfo=UTC),
            updated_at=datetime(1970, 1, 1, tzinfo=UTC),
            record_hash="0" * 64,
        )
        return self


class ReadyBinding(_FrozenPublicationModel):
    """C-only verified source result, signed plan, and final bound claim."""

    ready_source_stage_record_bytes: bytes
    ready_source_stage_record_hash: str = Field(pattern=_HASH_PATTERN)
    verified_source_outcome_hash: str = Field(pattern=_HASH_PATTERN)
    verified_evidence_chain_hash: str = Field(pattern=_HASH_PATTERN)
    source_use_plan_bytes: bytes
    source_use_plan_hash: str = Field(pattern=_HASH_PATTERN)
    final_claim_bytes: bytes
    final_claim_hash: str = Field(pattern=_HASH_PATTERN)
    current_claim_receipt_bytes: bytes
    current_claim_receipt_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_ready_binding(self) -> ReadyBinding:
        for bytes_value, hash_value, name in (
            (
                self.ready_source_stage_record_bytes,
                self.ready_source_stage_record_hash,
                "ready_source_stage_record_hash",
            ),
            (self.source_use_plan_bytes, self.source_use_plan_hash, "source_use_plan_hash"),
            (self.final_claim_bytes, self.final_claim_hash, "final_claim_hash"),
            (
                self.current_claim_receipt_bytes,
                self.current_claim_receipt_hash,
                "current_claim_receipt_hash",
            ),
        ):
            _require_sha256(bytes_value, hash_value, field=name)
        _canonical_model(
            self.ready_source_stage_record_bytes,
            LabSourceStageRecord,
            field="ready_source_stage_record_bytes",
        )
        _canonical_model(
            self.source_use_plan_bytes,
            SourceUsePlanV2,
            field="source_use_plan_bytes",
        )
        _canonical_model(self.final_claim_bytes, LabShardClaimV2, field="final_claim_bytes")
        from rquant.source_operation_contracts import CurrentClaimConsumptionV2

        _canonical_model(
            self.current_claim_receipt_bytes,
            CurrentClaimConsumptionV2,
            field="current_claim_receipt_bytes",
        )
        return self


class PublishReceipt(_FrozenPublicationModel):
    """D-only canonical spool receipt. Its timestamp is the publication mutation time."""

    spool_receipt_bytes: bytes
    spool_receipt_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> PublishReceipt:
        _canonical_json_object(self.spool_receipt_bytes, field="spool_receipt_bytes")
        _require_sha256(
            self.spool_receipt_bytes, self.spool_receipt_hash, field="spool_receipt_hash"
        )
        return self


class LabClaimSpoolPublishReceiptV2(_FrozenPublicationModel):
    """Canonical D receipt for one exact final claim made durable in the claim spool."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-lab-claim-spool-publish-receipt/v2"] = (
        "rquant-lab-claim-spool-publish-receipt/v2"
    )
    final_claim_bytes: bytes
    final_claim_hash: str = Field(pattern=_HASH_PATTERN)
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    spool_entry_relative_locator: str = Field(min_length=1, max_length=500)
    spool_entry_id: str = Field(pattern=_HASH_PATTERN)
    spool_content_digest: str = Field(pattern=_HASH_PATTERN)
    spool_entry_identity_hash: str = Field(pattern=_HASH_PATTERN)
    spool_root_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    publisher_authority: SourceBrokerV2AuthorityRef
    committed_at: datetime

    @classmethod
    def from_published_entry(
        cls,
        *,
        spool: LabClaimSpool,
        entry: LabClaimSpoolEntry,
        final_claim: LabShardClaimV2,
        committed_at: datetime,
    ) -> LabClaimSpoolPublishReceiptV2:
        validated_claim = LabShardClaimV2.model_validate(final_claim, strict=True)
        if validated_claim.source_use_plan is None:
            raise ValueError("spool publish receipt requires a final bound claim")
        if not isinstance(entry, LabClaimSpoolEntry):
            raise ValueError("spool publish receipt requires a real spool entry")
        authority = _canonical_model(
            spool.v2_publish_receipt_authority_bytes(),
            LabClaimSpoolReceiptAuthorityV2,
            field="spool receipt authority descriptor",
        )
        assert isinstance(authority, LabClaimSpoolReceiptAuthorityV2)
        locator, entry_id, content_digest = spool.v2_published_entry_identity(
            entry=entry,
            final_claim=validated_claim,
        )
        final_claim_bytes = canonical_model_json_bytes(validated_claim)
        identity_hash = _spool_entry_identity_hash(
            locator=locator,
            entry_id=entry_id,
            content_digest=content_digest,
            final_claim_hash=content_digest,
        )
        candidate = cls(
            final_claim_bytes=final_claim_bytes,
            final_claim_hash=content_digest,
            attempt_identity_hash=validated_claim.attempt_binding.attempt_identity_hash,
            spool_entry_relative_locator=locator,
            spool_entry_id=entry_id,
            spool_content_digest=content_digest,
            spool_entry_identity_hash=identity_hash,
            spool_root_id=authority.root_id,
            publisher_authority=authority.publisher_authority,
            committed_at=committed_at,
        )
        stored = spool._persist_v2_publish_receipt_bytes(
            entry=entry,
            final_claim=validated_claim,
            candidate_bytes=canonical_model_json_bytes(candidate),
        )
        parsed = _canonical_model(
            stored,
            cls,
            field="spool publish receipt sidecar",
        )
        assert isinstance(parsed, cls)
        parsed.require_current_published_entry(spool=spool, final_claim=validated_claim)
        return parsed

    @model_validator(mode="after")
    def validate_receipt(self) -> LabClaimSpoolPublishReceiptV2:
        _require_sha256(self.final_claim_bytes, self.final_claim_hash, field="final_claim_hash")
        final_claim = _canonical_model(
            self.final_claim_bytes,
            LabShardClaimV2,
            field="final_claim_bytes",
        )
        assert isinstance(final_claim, LabShardClaimV2)
        if final_claim.source_use_plan is None:
            raise ValueError("spool receipt final claim must be source-plan bound")
        if self.attempt_identity_hash != final_claim.attempt_binding.attempt_identity_hash:
            raise ValueError("spool receipt attempt identity conflicts with final claim")
        if (
            not self.spool_entry_relative_locator.startswith("pending/")
            or self.spool_entry_relative_locator.startswith("/")
            or "/../" in f"/{self.spool_entry_relative_locator}"
            or "/./" in f"/{self.spool_entry_relative_locator}"
        ):
            raise ValueError("spool receipt locator must be a pending relative path")
        if self.spool_content_digest != self.final_claim_hash:
            raise ValueError("spool receipt content digest conflicts with final claim")
        expected_identity_hash = _spool_entry_identity_hash(
            locator=self.spool_entry_relative_locator,
            entry_id=self.spool_entry_id,
            content_digest=self.spool_content_digest,
            final_claim_hash=self.final_claim_hash,
        )
        if self.spool_entry_identity_hash != expected_identity_hash:
            raise ValueError("spool receipt entry identity conflicts with locator")
        object.__setattr__(self, "committed_at", _aware_utc(self.committed_at))
        return self

    def to_publish_receipt(self) -> PublishReceipt:
        payload = canonical_model_json_bytes(self)
        return PublishReceipt(
            spool_receipt_bytes=payload,
            spool_receipt_hash=hashlib.sha256(payload).hexdigest(),
        )

    def require_current_published_entry(
        self,
        *,
        spool: LabClaimSpool,
        final_claim: LabShardClaimV2,
    ) -> LabClaimSpoolPublishReceiptV2:
        validated_claim = LabShardClaimV2.model_validate(final_claim, strict=True)
        if canonical_model_json_bytes(validated_claim) != self.final_claim_bytes:
            raise ValueError("spool receipt final claim conflicts with supplied claim")
        if not spool.is_current(validated_claim):
            raise ValueError("spool receipt final claim is no longer current")
        candidate = spool.root / self.spool_entry_relative_locator
        try:
            entry = spool.load(candidate)
            locator, entry_id, content_digest = spool.v2_published_entry_identity(
                entry=entry,
                final_claim=validated_claim,
            )
        except Exception as exc:
            raise ValueError("spool receipt pending entry is unavailable") from exc
        if (
            entry.claim != validated_claim
            or locator != self.spool_entry_relative_locator
            or entry_id != self.spool_entry_id
            or content_digest != self.spool_content_digest
        ):
            raise ValueError("spool receipt pending entry conflicts with current claim")
        return self


class LabClaimSpoolReceiptAuthorityV2(_FrozenPublicationModel):
    """Public logical identity for one configured, private spool receipt root."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-lab-claim-spool-receipt-authority/v2"] = (
        "rquant-lab-claim-spool-receipt-authority/v2"
    )
    root_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    publisher_authority: SourceBrokerV2AuthorityRef
    sidecar_protocol: Literal["rquant-lab-claim-spool-publish-receipt/v2"] = (
        "rquant-lab-claim-spool-publish-receipt/v2"
    )


class LabClaimSpoolReceiptVerifier:
    """Read-only verifier for one explicitly configured LabClaimSpool root.

    The persisted descriptor is the logical root identity.  Filesystem identity
    checks remain inside LabClaimSpool's private no-follow reader and are never
    included in receipt or authority commitments.
    """

    def __init__(
        self,
        *,
        spool: LabClaimSpool,
        authority: LabClaimSpoolReceiptAuthorityV2,
    ) -> None:
        self._spool = spool
        self.authority = LabClaimSpoolReceiptAuthorityV2.model_validate(authority, strict=True)

    @classmethod
    def from_spool(cls, spool: LabClaimSpool) -> LabClaimSpoolReceiptVerifier:
        try:
            authority = _canonical_model(
                spool.v2_publish_receipt_authority_bytes(),
                LabClaimSpoolReceiptAuthorityV2,
                field="spool receipt authority descriptor",
            )
        except Exception as exc:
            raise ValueError("spool receipt authority descriptor is invalid") from exc
        assert isinstance(authority, LabClaimSpoolReceiptAuthorityV2)
        return cls(spool=spool, authority=authority)

    def verify(
        self,
        receipt: LabClaimSpoolPublishReceiptV2,
        *,
        final_claim: LabShardClaimV2,
    ) -> LabClaimSpoolPublishReceiptV2:
        validated = LabClaimSpoolPublishReceiptV2.model_validate(receipt, strict=True)
        try:
            current_authority = _canonical_model(
                self._spool.v2_publish_receipt_authority_bytes(),
                LabClaimSpoolReceiptAuthorityV2,
                field="spool receipt authority descriptor",
            )
            sidecar = self._spool.load_v2_publish_receipt_sidecar(
                validated.spool_entry_id,
            )
        except Exception as exc:
            raise ValueError("spool receipt provenance is unavailable") from exc
        assert isinstance(current_authority, LabClaimSpoolReceiptAuthorityV2)
        if current_authority != self.authority:
            raise ValueError("spool receipt authority root conflicts with verifier")
        if (
            validated.spool_root_id != self.authority.root_id
            or validated.publisher_authority != self.authority.publisher_authority
        ):
            raise ValueError("spool receipt authority conflicts with verifier")
        if sidecar != canonical_model_json_bytes(validated):
            raise ValueError("spool receipt sidecar conflicts with receipt")
        validated.require_current_published_entry(
            spool=self._spool,
            final_claim=final_claim,
        )
        return validated


def require_v2_publish_receipt_for_final_claim(
    receipt: PublishReceipt,
    *,
    final_claim: LabShardClaimV2,
) -> LabClaimSpoolPublishReceiptV2:
    """Reject a generic D receipt whenever the publication has a v2 final claim."""

    validated_receipt = PublishReceipt.model_validate(receipt, strict=True)
    validated_claim = LabShardClaimV2.model_validate(final_claim, strict=True)
    try:
        parsed = _canonical_model(
            validated_receipt.spool_receipt_bytes,
            LabClaimSpoolPublishReceiptV2,
            field="spool_receipt_bytes",
        )
    except ValueError as exc:
        raise ValueError("v2 final claim requires a canonical typed spool receipt") from exc
    assert isinstance(parsed, LabClaimSpoolPublishReceiptV2)
    final_claim_bytes = canonical_model_json_bytes(validated_claim)
    if (
        parsed.final_claim_bytes != final_claim_bytes
        or parsed.final_claim_hash != hashlib.sha256(final_claim_bytes).hexdigest()
        or parsed.attempt_identity_hash != validated_claim.attempt_binding.attempt_identity_hash
    ):
        raise ValueError("typed spool receipt conflicts with the exact final claim")
    return parsed


def require_v2_spool_receipt_provenance(
    receipt: PublishReceipt,
    *,
    final_claim: LabShardClaimV2,
    verifier: LabClaimSpoolReceiptVerifier | None,
) -> LabClaimSpoolPublishReceiptV2:
    """Require both typed v2 receipt consistency and an actual configured spool sidecar."""

    typed = require_v2_publish_receipt_for_final_claim(receipt, final_claim=final_claim)
    if verifier is None:
        raise ValueError("v2 final claim requires a configured spool receipt verifier")
    return verifier.verify(typed, final_claim=final_claim)


def _spool_entry_identity_hash(
    *,
    locator: str,
    entry_id: str,
    content_digest: str,
    final_claim_hash: str,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "content_digest": content_digest,
                "contract": "rquant-lab-claim-spool-entry-identity/v2",
                "entry_id": entry_id,
                "final_claim_hash": final_claim_hash,
                "locator": locator,
            }
        )
    ).hexdigest()


class LabClaimPublicationRecord(_FrozenPublicationModel):
    identity: LabClaimPublicationIdentity
    claim_preimage_bytes: bytes
    claim_preimage_hash: str = Field(pattern=_HASH_PATTERN)
    claim_protocol: Literal["rquant-lab-shard-claim"] = "rquant-lab-shard-claim"
    claim_protocol_version: Literal["v2"] = "v2"
    source_wait_deadline: datetime
    publication_deadline: datetime
    source_stage_authority_bytes: bytes
    source_stage_authority_hash: str = Field(pattern=_HASH_PATTERN)
    source_stage_binding_bytes: bytes | None = None
    source_stage_binding_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    source_intent_bytes: bytes | None = None
    source_intent_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    source_operation_id: str | None = Field(default=None, pattern=_HASH_PATTERN)
    source_operation_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    queued_source_stage_record_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    ready_source_stage_record_bytes: bytes | None = None
    ready_source_stage_record_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    verified_source_outcome_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    verified_evidence_chain_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    source_use_plan_bytes: bytes | None = None
    source_use_plan_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    final_claim_bytes: bytes | None = None
    final_claim_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    current_claim_receipt_bytes: bytes | None = None
    current_claim_receipt_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    spool_receipt_bytes: bytes | None = None
    spool_receipt_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    status: ClaimPublicationStatus
    version: int = Field(strict=True, ge=0)
    created_at: datetime
    updated_at: datetime
    queued_at: datetime | None = None
    ready_at: datetime | None = None
    published_at: datetime | None = None
    aborted_at: datetime | None = None
    terminal_reason: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,63}$")
    record_commitment: str = Field(pattern=_HASH_PATTERN)

    @field_validator(
        "source_wait_deadline",
        "publication_deadline",
        "created_at",
        "updated_at",
        "queued_at",
        "ready_at",
        "published_at",
        "aborted_at",
    )
    @classmethod
    def validate_timestamp(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _aware_utc(value)

    @model_validator(mode="after")
    def validate_record(self) -> LabClaimPublicationRecord:
        _validate_deadline_order(self.source_wait_deadline, self.publication_deadline)
        held = HeldDraft(
            identity=self.identity,
            claim_preimage_bytes=self.claim_preimage_bytes,
            claim_preimage_hash=self.claim_preimage_hash,
            claim_protocol=self.claim_protocol,
            claim_protocol_version=self.claim_protocol_version,
            source_wait_deadline=self.source_wait_deadline,
            publication_deadline=self.publication_deadline,
        )
        _require_sha256(
            self.source_stage_authority_bytes,
            self.source_stage_authority_hash,
            field="source_stage_authority_hash",
        )
        source_stage_store_authority_from_canonical_bytes(self.source_stage_authority_bytes)
        queue_values = (
            self.source_stage_binding_bytes,
            self.source_stage_binding_hash,
            self.source_intent_bytes,
            self.source_intent_hash,
            self.source_operation_id,
            self.source_operation_hash,
            self.queued_source_stage_record_hash,
        )
        ready_values = (
            self.ready_source_stage_record_bytes,
            self.ready_source_stage_record_hash,
            self.verified_source_outcome_hash,
            self.verified_evidence_chain_hash,
            self.source_use_plan_bytes,
            self.source_use_plan_hash,
            self.final_claim_bytes,
            self.final_claim_hash,
            self.current_claim_receipt_bytes,
            self.current_claim_receipt_hash,
        )
        receipt_values = (self.spool_receipt_bytes, self.spool_receipt_hash)
        queue_complete = all(value is not None for value in queue_values)
        ready_complete = all(value is not None for value in ready_values)
        receipt_complete = all(value is not None for value in receipt_values)
        if any(value is not None for value in queue_values) and not queue_complete:
            raise ValueError("source queue fields must be present together")
        if any(value is not None for value in ready_values) and not ready_complete:
            raise ValueError("ready publication fields must be present together")
        if any(value is not None for value in receipt_values) and not receipt_complete:
            raise ValueError("spool receipt fields must be present together")
        if queue_complete:
            QueueBinding(
                source_stage_binding_bytes=self.source_stage_binding_bytes or b"",
                source_stage_binding_hash=self.source_stage_binding_hash or "",
                source_intent_bytes=self.source_intent_bytes or b"",
                source_intent_hash=self.source_intent_hash or "",
                source_operation_id=self.source_operation_id or "",
                source_operation_hash=self.source_operation_hash or "",
            )
        if ready_complete:
            ReadyBinding(
                ready_source_stage_record_bytes=self.ready_source_stage_record_bytes or b"",
                ready_source_stage_record_hash=self.ready_source_stage_record_hash or "",
                verified_source_outcome_hash=self.verified_source_outcome_hash or "",
                verified_evidence_chain_hash=self.verified_evidence_chain_hash or "",
                source_use_plan_bytes=self.source_use_plan_bytes or b"",
                source_use_plan_hash=self.source_use_plan_hash or "",
                final_claim_bytes=self.final_claim_bytes or b"",
                final_claim_hash=self.final_claim_hash or "",
                current_claim_receipt_bytes=self.current_claim_receipt_bytes or b"",
                current_claim_receipt_hash=self.current_claim_receipt_hash or "",
            )
        if receipt_complete:
            PublishReceipt(
                spool_receipt_bytes=self.spool_receipt_bytes or b"",
                spool_receipt_hash=self.spool_receipt_hash or "",
            )
        if self.updated_at < self.created_at:
            raise ValueError("updated_at precedes created_at")
        if self.queued_at is not None and self.queued_at < self.created_at:
            raise ValueError("queued_at precedes created_at")
        if self.ready_at is not None and (self.queued_at is None or self.ready_at < self.queued_at):
            raise ValueError("ready_at requires a prior source queue binding")
        if self.published_at is not None and (
            self.ready_at is None or self.published_at < self.ready_at
        ):
            raise ValueError("published_at requires a prior ready binding")
        matrix = {
            ClaimPublicationStatus.HELD_SOURCE: (
                self.version == 0
                and not queue_complete
                and not ready_complete
                and not receipt_complete
                and self.queued_at is None
                and self.ready_at is None
                and self.published_at is None
                and self.aborted_at is None
                and self.terminal_reason is None
            ),
            ClaimPublicationStatus.SOURCE_QUEUED: (
                self.version == 1
                and queue_complete
                and not ready_complete
                and not receipt_complete
                and self.queued_at is not None
                and self.ready_at is None
                and self.published_at is None
                and self.aborted_at is None
                and self.terminal_reason is None
            ),
            ClaimPublicationStatus.READY_TO_PUBLISH: (
                self.version == 2
                and queue_complete
                and ready_complete
                and not receipt_complete
                and self.queued_at is not None
                and self.ready_at is not None
                and self.published_at is None
                and self.aborted_at is None
                and self.terminal_reason is None
            ),
            ClaimPublicationStatus.PUBLISHED: (
                self.version == 3
                and queue_complete
                and ready_complete
                and receipt_complete
                and self.queued_at is not None
                and self.ready_at is not None
                and self.published_at is not None
                and self.aborted_at is None
                and self.terminal_reason is None
            ),
            ClaimPublicationStatus.ABORTED: (
                self.version in {1, 2, 3}
                and self.aborted_at is not None
                and self.published_at is None
                and not receipt_complete
                and self.terminal_reason is not None
                and (
                    (
                        self.version == 1
                        and not queue_complete
                        and not ready_complete
                        and self.queued_at is None
                        and self.ready_at is None
                    )
                    or (
                        self.version == 2
                        and queue_complete
                        and not ready_complete
                        and self.queued_at is not None
                        and self.ready_at is None
                    )
                    or (
                        self.version == 3
                        and queue_complete
                        and ready_complete
                        and self.queued_at is not None
                        and self.ready_at is not None
                    )
                )
            ),
        }
        if not matrix[self.status]:
            raise ValueError("claim publication status fields are inconsistent")
        if self.recomputed_commitment() != self.record_commitment:
            raise ValueError("claim publication record commitment mismatch")
        # Keep the local binding alive: it documents the A fields participating
        # in every commitment and prevents accidental widening of the record.
        assert held.identity == self.identity
        return self

    def recomputed_commitment(self) -> str:
        return hashlib.sha256(canonical_claim_publication_record_bytes(self)).hexdigest()


def canonical_claim_publication_record_bytes(record: LabClaimPublicationRecord) -> bytes:
    return canonical_json_bytes(record.model_dump(mode="json", exclude={"record_commitment"}))


class LabClaimPublicationAuditRecord(_FrozenPublicationModel):
    audit_ref: UUID
    attempt_id: UUID
    action: ClaimPublicationAuditAction
    prior_status: ClaimPublicationStatus | None = None
    new_status: ClaimPublicationStatus
    reason_code: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    record_commitment: str = Field(pattern=_HASH_PATTERN)
    occurred_at: datetime
    audit_hash: str = Field(pattern=_HASH_PATTERN)

    @field_validator("occurred_at")
    @classmethod
    def validate_occurred_at(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_audit_hash(self) -> LabClaimPublicationAuditRecord:
        if self.recomputed_hash() != self.audit_hash:
            raise ValueError("claim publication audit hash mismatch")
        return self

    def recomputed_hash(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(self.model_dump(mode="json", exclude={"audit_hash"}))
        ).hexdigest()


class LabClaimPublicationMutation(_FrozenPublicationModel):
    record: LabClaimPublicationRecord
    audit_ref: UUID | None
    replayed: bool


class LabClaimPublicationObservationDegradation(_FrozenPublicationModel):
    """Redacted durable retry metadata for a failed primary observation write."""

    degradation_ref: UUID
    attempt_id: UUID
    publication_identity_hash: str = Field(pattern=_HASH_PATTERN)
    authority_fencing_token: int = Field(strict=True, ge=1)
    event_type: Literal["ready", "published", "replayed", "blocked"]
    reason_code: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    reason_code_hash: str = Field(pattern=_HASH_PATTERN)
    error_class: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
    next_retry_at: datetime
    created_at: datetime

    @field_validator("next_retry_at", "created_at")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_commitments(self) -> LabClaimPublicationObservationDegradation:
        if hashlib.sha256(self.reason_code.encode("ascii")).hexdigest() != self.reason_code_hash:
            raise ValueError("finalizer observation degradation reason hash mismatch")
        return self


class LabClaimPublicationRolloutEvidence(_FrozenPublicationModel):
    """Canonical rollout evidence reconstructed from one signed PUBLISHED record."""

    attempt_id: UUID
    evidence_hash: str = Field(pattern=_HASH_PATTERN)
    publication_identity: str = Field(min_length=2, max_length=8_192)

    @classmethod
    def from_record(cls, record: LabClaimPublicationRecord) -> LabClaimPublicationRolloutEvidence:
        if record.status is not ClaimPublicationStatus.PUBLISHED:
            raise ValueError("rollout evidence requires a published record")
        identity_bytes = canonical_model_json_bytes(record.identity)
        return cls(
            attempt_id=record.identity.attempt_id,
            evidence_hash=hashlib.sha256(identity_bytes).hexdigest(),
            publication_identity=identity_bytes.decode("utf-8"),
        )

    @model_validator(mode="after")
    def validate_identity_binding(self) -> LabClaimPublicationRolloutEvidence:
        try:
            identity = strict_model_validate_canonical_json(
                LabClaimPublicationIdentity, self.publication_identity
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("rollout publication identity must be canonical") from exc
        identity_bytes = canonical_model_json_bytes(identity)
        if identity.attempt_id != self.attempt_id:
            raise ValueError("rollout publication attempt differs")
        if hashlib.sha256(identity_bytes).hexdigest() != self.evidence_hash:
            raise ValueError("rollout publication evidence hash differs")
        return self


class LabClaimPublicationRolloutEvidenceOutboxItem(_FrozenPublicationModel):
    """One durable v16 degradation row bound to its current PUBLISHED record."""

    degradation_ref: UUID
    evidence: LabClaimPublicationRolloutEvidence
    record: LabClaimPublicationRecord
    authority_fencing_token: int = Field(strict=True, ge=1)
    next_retry_at: datetime
    created_at: datetime

    @field_validator("next_retry_at", "created_at")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_record_binding(self) -> LabClaimPublicationRolloutEvidenceOutboxItem:
        if self.evidence != LabClaimPublicationRolloutEvidence.from_record(self.record):
            raise ValueError("rollout outbox record binding differs")
        return self
