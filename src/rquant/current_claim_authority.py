"""Persistent, root-pinned current-claim authority for source-use plan issuance.

The scheduler publishes an unbound current claim here before a worker can obtain a
source-use plan.  This module deliberately owns both the claim high-water and the
one-shot signing receipt: splitting those writes would allow a reclaimed attempt to
race a signature.  Production persistence is pinned to an independent monotonic
root; the explicit ``test-standalone`` mode is intentionally non-production.
"""

from __future__ import annotations

import secrets
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol, Self

from pydantic import Field, ValidationError, model_validator

from rquant.adapter_manifest import SOURCE_USE_PLAN_V2_NAMESPACE, VerifyOnlyEd25519Keyring
from rquant.external_monotonic_root import (
    ExternalMonotonicRootClient,
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootReceiptIdentity,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootSignatureVerifier,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
)
from rquant.lab_shard_protocol import LabShardClaimV2
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_operation_contracts import (
    CurrentClaimConsumptionBindingV2,
    CurrentClaimConsumptionV2,
    CurrentClaimPlanIssueV2,
    CurrentClaimPlanSignerIdentityV2,
    SourceOperationContractError,
    require_current_claim_plan_issue_v2,
    require_source_use_plan_v2,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_SCHEMA_VERSION = 1
_APPLICATION_ID = 0x52514343
_ZERO_HASH = "0" * 64
_META_TABLE = "current_claim_authority_meta"
_CURRENT_TABLE = "current_claim_current"
_ISSUE_TABLE = "current_claim_issue"
_JOURNAL_TABLE = "current_claim_journal"
_PENDING_TABLE = "current_claim_pending"
_ROOT_SCHEMA_VERSION = 1
_ROOT_APPLICATION_ID = 0x52514352
_ROOT_ROLE = "current_claim_monotonic_root"
_ROOT_KEY_PURPOSE = "current-claim-monotonic-root"
_ROOT_RECEIPT_NAMESPACE = "rquant-current-claim-anti-rollback-root/v1"
_ROOT_SIGNATURE_ALGORITHM = "ed25519"
_ROOT_META_TABLE = "current_claim_root_meta"
_ROOT_STATE_TABLE = "current_claim_root_state"
_ROOT_OPERATION_TABLE = "current_claim_root_operation"
_TABLE_SQL = {
    _META_TABLE: """
        CREATE TABLE current_claim_authority_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL CHECK (schema_version = 1),
            authority_id TEXT NOT NULL,
            mode TEXT NOT NULL CHECK (
                mode IN ('production', 'test-external', 'test-standalone')
            ),
            root_config_hash TEXT NOT NULL,
            checkpoint_json TEXT NOT NULL,
            claim_accumulator TEXT NOT NULL,
            issue_accumulator TEXT NOT NULL,
            claim_count INTEGER NOT NULL CHECK (claim_count >= 0),
            issue_count INTEGER NOT NULL CHECK (issue_count >= 0)
        ) STRICT
    """,
    _CURRENT_TABLE: """
        CREATE TABLE current_claim_current (
            claim_slot TEXT PRIMARY KEY,
            claim_json TEXT NOT NULL,
            claim_hash TEXT NOT NULL,
            claim_generation INTEGER NOT NULL CHECK (claim_generation >= 1),
            scheduler_fencing_token INTEGER NOT NULL CHECK (scheduler_fencing_token >= 1)
        ) STRICT
    """,
    _ISSUE_TABLE: """
        CREATE TABLE current_claim_issue (
            operation_id TEXT PRIMARY KEY,
            attempt_identity_hash TEXT NOT NULL UNIQUE,
            issue_hash TEXT NOT NULL,
            receipt_json TEXT NOT NULL
        ) STRICT
    """,
    _JOURNAL_TABLE: """
        CREATE TABLE current_claim_journal (
            journal_index INTEGER PRIMARY KEY CHECK (journal_index > 0),
            mutation_id TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL CHECK (kind IN ('publish', 'issue')),
            payload_hash TEXT NOT NULL,
            previous_journal_hash TEXT NOT NULL,
            journal_hash TEXT NOT NULL UNIQUE
        ) STRICT
    """,
    _PENDING_TABLE: """
        CREATE TABLE current_claim_pending (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            pending_json TEXT NOT NULL
        ) STRICT
    """,
}


class CurrentClaimAuthoritySecurityError(RuntimeError):
    """The authority configuration, root, or durable state is untrusted."""


class CurrentClaimAuthorityRepairRequiredError(CurrentClaimAuthoritySecurityError):
    """The external root is ahead of local state without an exact pending proof."""

    def __init__(self, state: CurrentClaimAuthorityRepairState) -> None:
        self.state = state
        super().__init__(f"current claim authority repair required: {state.reason}")


class SourceUsePlanSigner(Protocol):
    """Closed signer owned by the authority, never supplied per issue call."""

    issuer: str
    key_id: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


class CurrentClaimCheckpoint(RuntimeContractModel):
    """Immutable digest of all authority-owned materialized state."""

    schema_version: Literal[1]
    contract: Literal["rquant-current-claim-checkpoint/v1"]
    authority_id: str = Field(min_length=1, max_length=200)
    operation_count: int = Field(strict=True, ge=0)
    journal_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_root: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def checkpoint_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class CurrentClaimAntiRollbackRoot(RuntimeContractModel):
    """Root response that must exactly bind one compare-and-advance request."""

    schema_version: Literal[1]
    contract: Literal["rquant-current-claim-anti-rollback-root/v1"]
    role: Literal["current_claim_monotonic_root"]
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    current_claim_authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: CurrentClaimCheckpoint
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["current-claim-monotonic-root"] = _ROOT_KEY_PURPOSE
    namespace: Literal["rquant-current-claim-anti-rollback-root/v1"] = _ROOT_RECEIPT_NAMESPACE
    signature_algorithm: Literal["ed25519"] = _ROOT_SIGNATURE_ALGORITHM
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class CurrentClaimExternalRootReceipt(RuntimeContractModel):
    """Fresh signed response binding one external request to the current root state."""

    schema_version: Literal[1]
    contract: Literal["rquant-current-claim-external-root-receipt/v1"]
    role: Literal["current_claim_monotonic_root"]
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    current_claim_authority_id: str = Field(min_length=1, max_length=200)
    request_kind: Literal["current", "pin", "advance"]
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: CurrentClaimCheckpoint
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["current-claim-monotonic-root"] = _ROOT_KEY_PURPOSE
    namespace: Literal["rquant-current-claim-anti-rollback-root/v1"] = _ROOT_RECEIPT_NAMESPACE
    signature_algorithm: Literal["ed25519"] = _ROOT_SIGNATURE_ALGORITHM
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class CurrentClaimMonotonicRoot(Protocol):
    """Independent monotonic store required in production mode."""

    @property
    def authority_id(self) -> str: ...

    @property
    def role(self) -> Literal["current_claim_monotonic_root"]: ...

    @property
    def store_id(self) -> str: ...

    @property
    def storage_path(self) -> Path | None: ...

    def current(
        self, *, current_claim_authority_id: str
    ) -> CurrentClaimAntiRollbackRoot | None: ...

    def pin(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot: ...

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot: ...


class CurrentClaimRootSigner(Protocol):
    """Pinned signer role for the independent monotonic root journal."""

    issuer: str
    key_id: str
    key_purpose: Literal["current-claim-monotonic-root"]
    signature_algorithm: Literal["ed25519"]
    public_key_fingerprint: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool: ...


class ExternalCurrentClaimRootConfig(ExternalMonotonicRootConfig):
    """Closed production binding for a witness outside the local rollback domain."""

    role: Literal["current_claim_monotonic_root"] = _ROOT_ROLE
    root_key_purpose: Literal["current-claim-monotonic-root"] = _ROOT_KEY_PURPOSE
    root_receipt_namespace: Literal["rquant-current-claim-anti-rollback-root/v1"] = (
        _ROOT_RECEIPT_NAMESPACE
    )


ExternalCurrentClaimRootClient = ExternalMonotonicRootClient
CurrentClaimRootSignatureVerifier = ExternalMonotonicRootSignatureVerifier


class ExternalCurrentClaimMonotonicRootAdapter:
    """Validated production adapter for an independently operated CAS witness."""

    def __init__(
        self,
        *,
        config: ExternalCurrentClaimRootConfig,
        client: UnixSocketExternalMonotonicRootClient,
        root_verifiers: tuple[CurrentClaimRootSignatureVerifier, ...],
    ) -> None:
        if type(client) is not UnixSocketExternalMonotonicRootClient:
            raise CurrentClaimAuthoritySecurityError(
                "production external root requires the closed Unix peer client"
            )
        self._initialize(
            config=config,
            client=client,
            root_verifiers=root_verifiers,
            production_ready=True,
        )

    @classmethod
    def for_nonproduction_test(
        cls,
        *,
        config: ExternalCurrentClaimRootConfig,
        client: ExternalCurrentClaimRootClient,
        root_verifiers: tuple[CurrentClaimRootSignatureVerifier, ...],
    ) -> ExternalCurrentClaimMonotonicRootAdapter:
        instance = cls.__new__(cls)
        instance._initialize(
            config=config,
            client=client,
            root_verifiers=root_verifiers,
            production_ready=False,
        )
        return instance

    def _initialize(
        self,
        *,
        config: ExternalCurrentClaimRootConfig,
        client: ExternalCurrentClaimRootClient,
        root_verifiers: tuple[CurrentClaimRootSignatureVerifier, ...],
        production_ready: bool,
    ) -> None:
        try:
            validated = ExternalCurrentClaimRootConfig.model_validate(config, strict=True)
            trust = ExternalMonotonicRootTrustBoundary(
                config=validated,
                client=client,
                root_verifiers=root_verifiers,
            )
        except (ValidationError, ExternalMonotonicRootSecurityError) as exc:
            raise CurrentClaimAuthoritySecurityError(
                "external root adapter config is invalid"
            ) from exc
        self._config = validated
        self._trust = trust
        self._production_ready = production_ready

    @property
    def authority_id(self) -> str:
        return self._config.root_authority_id

    @property
    def role(self) -> Literal["current_claim_monotonic_root"]:
        return _ROOT_ROLE

    @property
    def store_id(self) -> str:
        return self._config.root_store_id

    @property
    def storage_path(self) -> None:
        return None

    @property
    def config(self) -> ExternalCurrentClaimRootConfig:
        return self._config

    @property
    def production_ready(self) -> bool:
        return self._production_ready

    def current(self, *, current_claim_authority_id: str) -> CurrentClaimAntiRollbackRoot | None:
        request = ExternalMonotonicRootRequest.close(
            kind="current",
            role=self._config.role,
            root_authority_id=self._config.root_authority_id,
            root_store_id=self._config.root_store_id,
            subject_authority_id=current_claim_authority_id,
            challenge_nonce=secrets.token_hex(32),
        )
        response = self._invoke(request)
        return None if response is None else self._require_verified(response, request=request)

    def pin(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot:
        request, response = self._invoke_mutation(
            kind="pin",
            operation_id=operation_id,
            current_claim_authority_id=current_claim_authority_id,
            previous_checkpoint_hash=_ZERO_HASH,
            checkpoint=checkpoint,
        )
        if response is None:
            raise CurrentClaimAuthoritySecurityError("external witness pin returned no receipt")
        return self._require_verified(response, request=request)

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot:
        request, response = self._invoke_mutation(
            kind="advance",
            operation_id=operation_id,
            current_claim_authority_id=current_claim_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )
        if response is None:
            raise CurrentClaimAuthoritySecurityError("external witness advance returned no receipt")
        return self._require_verified(response, request=request)

    def _invoke_mutation(
        self,
        *,
        kind: Literal["pin", "advance"],
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> tuple[ExternalMonotonicRootRequest, str | None]:
        request = ExternalMonotonicRootRequest.close(
            kind=kind,
            role=self._config.role,
            root_authority_id=self._config.root_authority_id,
            root_store_id=self._config.root_store_id,
            subject_authority_id=current_claim_authority_id,
            challenge_nonce=secrets.token_hex(32),
            operation_id=operation_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint_contract=checkpoint.contract,
            checkpoint_hash=checkpoint.checkpoint_hash,
            checkpoint_json=canonical_model_json_bytes(checkpoint).decode("utf-8"),
        )
        return request, self._invoke(request)

    def _invoke(self, request: ExternalMonotonicRootRequest) -> str | None:
        try:
            return self._trust.invoke(request)
        except ExternalMonotonicRootSecurityError as exc:
            raise CurrentClaimAuthoritySecurityError(str(exc)) from exc

    def _require_verified(
        self,
        receipt_json: str,
        *,
        request: ExternalMonotonicRootRequest,
    ) -> CurrentClaimAntiRollbackRoot:
        try:
            validated = strict_model_validate_canonical_json(
                CurrentClaimExternalRootReceipt,
                receipt_json,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError(
                "external witness receipt is malformed"
            ) from exc
        try:
            self._trust.verify_receipt(
                identity=ExternalMonotonicRootReceiptIdentity(
                    role=validated.role,
                    root_authority_id=validated.root_authority_id,
                    root_store_id=validated.root_store_id,
                    closed=validated.closed,
                    issuer=validated.issuer,
                    key_id=validated.key_id,
                    key_purpose=validated.key_purpose,
                    namespace=validated.namespace,
                    signature_algorithm=validated.signature_algorithm,
                    public_key_fingerprint=validated.public_key_fingerprint,
                ),
                signing_bytes=validated.signing_bytes(),
                signature=validated.signature,
            )
        except ExternalMonotonicRootSecurityError as exc:
            raise CurrentClaimAuthoritySecurityError(str(exc)) from exc
        if (
            validated.request_kind != request.kind
            or validated.request_hash != request.request_hash
            or validated.challenge_nonce != request.challenge_nonce
            or validated.current_claim_authority_id != request.subject_authority_id
        ):
            raise CurrentClaimAuthoritySecurityError(
                "external witness receipt does not bind the fresh request challenge"
            )
        if request.kind != "current" and (
            validated.operation_id != request.operation_id
            or validated.previous_checkpoint_hash != request.previous_checkpoint_hash
            or validated.checkpoint.checkpoint_hash != request.checkpoint_hash
        ):
            raise CurrentClaimAuthoritySecurityError(
                "external witness mutation receipt conflicts with the exact request"
            )
        return CurrentClaimAntiRollbackRoot(
            schema_version=1,
            contract="rquant-current-claim-anti-rollback-root/v1",
            role=validated.role,
            root_authority_id=validated.root_authority_id,
            root_store_id=validated.root_store_id,
            current_claim_authority_id=validated.current_claim_authority_id,
            operation_id=validated.operation_id,
            previous_checkpoint_hash=validated.previous_checkpoint_hash,
            checkpoint=validated.checkpoint,
            closed=validated.closed,
            issuer=validated.issuer,
            key_id=validated.key_id,
            key_purpose=validated.key_purpose,
            namespace=validated.namespace,
            signature_algorithm=validated.signature_algorithm,
            public_key_fingerprint=validated.public_key_fingerprint,
            signature=validated.signature,
        )


class CurrentClaimRootOperationRequest(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-current-claim-root-operation/v1"]
    kind: Literal["pin", "advance"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    current_claim_authority_id: str = Field(min_length=1, max_length=200)
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: CurrentClaimCheckpoint

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class CurrentClaimRootPreflight(RuntimeContractModel):
    mode: Literal["test-only-materializer"] = "test-only-materializer"
    non_production: Literal[True] = True
    role: Literal["current_claim_monotonic_root"]
    authority_id: str = Field(min_length=1, max_length=200)
    store_id: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_db_path: Path


class CurrentClaimRootAuditSummary(RuntimeContractModel):
    role: Literal["current_claim_monotonic_root"]
    authority_id: str = Field(min_length=1, max_length=200)
    store_id: str = Field(min_length=1, max_length=200)
    operation_count: int = Field(strict=True, ge=0)
    state_count: int = Field(strict=True, ge=0)
    journal_root: str = Field(pattern=r"^[0-9a-f]{64}$")


class CurrentClaimRootAnchor(RuntimeContractModel):
    operation_count: int = Field(strict=True, ge=0)
    journal_root: str = Field(pattern=r"^[0-9a-f]{64}$")


_ROOT_TABLE_SQL = {
    _ROOT_META_TABLE: """
        CREATE TABLE current_claim_root_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL CHECK (schema_version = 1),
            role TEXT NOT NULL CHECK (role = 'current_claim_monotonic_root'),
            authority_id TEXT NOT NULL,
            store_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            public_key_fingerprint TEXT NOT NULL,
            operation_count INTEGER NOT NULL CHECK (operation_count >= 0),
            journal_root TEXT NOT NULL
        ) STRICT
    """,
    _ROOT_STATE_TABLE: """
        CREATE TABLE current_claim_root_state (
            current_claim_authority_id TEXT PRIMARY KEY,
            root_json TEXT NOT NULL
        ) STRICT
    """,
    _ROOT_OPERATION_TABLE: """
        CREATE TABLE current_claim_root_operation (
            operation_id TEXT PRIMARY KEY,
            operation_index INTEGER NOT NULL UNIQUE CHECK (operation_index > 0),
            request_json TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            root_json TEXT NOT NULL,
            previous_operation_hash TEXT NOT NULL,
            operation_hash TEXT NOT NULL UNIQUE
        ) STRICT
    """,
}


class CurrentClaimAuthorityPreflight(RuntimeContractModel):
    mode: Literal["production", "test-external", "test-standalone"]
    non_production: bool
    root_required: bool
    root_configured: bool
    root_authority_id: str | None = None
    authority_db_path: Path
    root_store_path: Path | None = None


class CurrentClaimAuthorityAuditSummary(RuntimeContractModel):
    authority_id: str = Field(min_length=1, max_length=200)
    operation_count: int = Field(strict=True, ge=0)
    claim_count: int = Field(strict=True, ge=0)
    issue_count: int = Field(strict=True, ge=0)
    journal_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mode: Literal["production", "test-external", "test-standalone"]
    non_production: bool


class CurrentClaimMaterializedAnchor(RuntimeContractModel):
    checkpoint: CurrentClaimCheckpoint
    claim_accumulator: str = Field(pattern=r"^[0-9a-f]{64}$")
    issue_accumulator: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_count: int = Field(strict=True, ge=0)
    issue_count: int = Field(strict=True, ge=0)


class CurrentClaimAuthorityRepairState(RuntimeContractModel):
    status: Literal["repair_required"]
    reason: Literal[
        "external_root_ahead_without_pending_proof",
        "external_root_diverges_at_same_high_water",
        "local_state_ahead_of_external_root",
    ]
    authority_id: str = Field(min_length=1, max_length=200)
    root_authority_id: str = Field(min_length=1, max_length=200)
    local_operation_count: int = Field(strict=True, ge=0)
    root_operation_count: int = Field(strict=True, ge=0)
    local_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    non_production: bool


class CurrentClaimPendingMutation(RuntimeContractModel):
    """Closed local proof for completing exactly one externally rooted mutation."""

    schema_version: Literal[1]
    contract: Literal["rquant-current-claim-pending-mutation/v1"]
    kind: Literal["publish", "issue"]
    mutation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: CurrentClaimCheckpoint
    journal_index: int = Field(strict=True, ge=1)
    journal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    journal_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_slot: str | None = Field(default=None, min_length=1, max_length=500)
    claim: LabShardClaimV2 | None = None
    claim_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    issue: CurrentClaimPlanIssueV2 | None = None
    receipt: CurrentClaimConsumptionV2 | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> Self:
        if self.kind == "publish":
            if (
                self.claim_slot is None
                or self.claim is None
                or self.claim_hash is None
                or self.issue is not None
                or self.receipt is not None
            ):
                raise ValueError("publish pending mutation shape is invalid")
            if self.claim_hash != _claim_hash(self.claim):
                raise ValueError("publish pending claim hash is invalid")
        elif self.claim_slot is not None or self.claim is not None or self.claim_hash is not None:
            raise ValueError("issue pending mutation cannot carry a current claim replacement")
        if self.kind == "issue":
            if self.issue is None or self.receipt is None:
                raise ValueError("issue pending mutation is incomplete")
            _require_receipt_matches_issue(self.receipt, self.issue)
        elif self.issue is not None or self.receipt is not None:
            raise ValueError("publish pending mutation cannot carry an issue receipt")
        return self


def _canonical_json(value: RuntimeContractModel) -> str:
    return canonical_model_json_bytes(value).decode("utf-8")


def _absolute_path(path: Path) -> Path:
    return path.expanduser().resolve()


def _normalize_now(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SourceOperationContractError("current claim authority time must be timezone-aware")
    return value.astimezone(UTC)


def _claim_slot(claim: LabShardClaimV2) -> str:
    return f"{claim.job_id}:{claim.shard_id}"


def _claim_hash(claim: LabShardClaimV2) -> str:
    return canonical_sha256(claim.model_dump(mode="python"))


def _claim_leaf_hash(*, claim_slot: str, claim_hash: str) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-current-claim-materialized-claim/v1",
            "claim_slot": claim_slot,
            "claim_hash": claim_hash,
        }
    )


def _issue_leaf_hash(
    *,
    operation_id: str,
    issue_hash: str,
    receipt_hash: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-current-claim-materialized-issue/v1",
            "operation_id": operation_id,
            "issue_hash": issue_hash,
            "receipt_hash": receipt_hash,
        }
    )


def _xor_hashes(*values: str) -> str:
    accumulator = 0
    for value in values:
        accumulator ^= int(value, 16)
    return f"{accumulator:064x}"


def _materialized_state_root(
    *,
    claim_accumulator: str,
    issue_accumulator: str,
    claim_count: int,
    issue_count: int,
) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-current-claim-materialized-state/v2",
            "claim_accumulator": claim_accumulator,
            "claim_count": claim_count,
            "issue_accumulator": issue_accumulator,
            "issue_count": issue_count,
        }
    )


def _journal_hash(
    *,
    journal_index: int,
    mutation_id: str,
    kind: Literal["publish", "issue"],
    payload_hash: str,
    previous_journal_hash: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-current-claim-journal-entry/v1",
            "journal_index": journal_index,
            "kind": kind,
            "mutation_id": mutation_id,
            "payload_hash": payload_hash,
            "previous_journal_hash": previous_journal_hash,
        }
    )


def _require_receipt_matches_issue(
    receipt: CurrentClaimConsumptionV2,
    issue: CurrentClaimPlanIssueV2,
) -> None:
    if receipt.binding != issue.binding or receipt.binding_hash != issue.binding_hash:
        raise CurrentClaimAuthoritySecurityError("receipt binding differs from persisted issue")
    unsigned = receipt.signed_plan.model_copy(update={"signature": ""})
    reconstructed = CurrentClaimPlanIssueV2.from_unsigned_plan(unsigned)
    if reconstructed != issue:
        raise CurrentClaimAuthoritySecurityError("receipt signed plan differs from persisted issue")


class SQLiteCurrentClaimMonotonicRoot:
    """NONPRODUCTION local materializer for current-claim checkpoint tests.

    Its immutable journal is useful for tests and local recovery exercises, but the
    database remains in the same rollback domain as other local files. Production
    composition therefore rejects this class and requires an external CAS adapter.
    """

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        store_id: str,
        signer: CurrentClaimRootSigner,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self._path = _absolute_path(path)
        self._authority_id = authority_id.strip()
        self._store_id = store_id.strip()
        self._signer = signer
        self._busy_timeout_ms = busy_timeout_ms
        if not self._authority_id or not self._store_id:
            raise CurrentClaimAuthoritySecurityError(
                "root authority and store identity must be nonempty"
            )
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise CurrentClaimAuthoritySecurityError("root busy_timeout_ms must be a positive int")
        if (
            signer.key_purpose != _ROOT_KEY_PURPOSE
            or signer.signature_algorithm != _ROOT_SIGNATURE_ALGORITHM
        ):
            raise CurrentClaimAuthoritySecurityError("root signer purpose or algorithm is invalid")
        if not signer.issuer.strip() or not signer.key_id.strip():
            raise CurrentClaimAuthoritySecurityError("root signer key identity is empty")
        fingerprint = signer.public_key_fingerprint
        if len(fingerprint) != 64 or any(value not in "0123456789abcdef" for value in fingerprint):
            raise CurrentClaimAuthoritySecurityError(
                "root signer public key fingerprint is invalid"
            )
        challenge = canonical_model_json_bytes(
            CurrentClaimRootPreflight(
                role=_ROOT_ROLE,
                authority_id=self._authority_id,
                store_id=self._store_id,
                key_id=signer.key_id,
                public_key_fingerprint=fingerprint,
                root_db_path=self._path,
            )
        )
        try:
            signature = signer.sign(namespace=_ROOT_RECEIPT_NAMESPACE, payload=challenge)
            trusted = signer.verify(
                namespace=_ROOT_RECEIPT_NAMESPACE,
                payload=challenge,
                signature=signature,
            )
        except Exception as exc:
            raise CurrentClaimAuthoritySecurityError(
                "root signer self-verification failed"
            ) from exc
        if not signature or not trusted:
            raise CurrentClaimAuthoritySecurityError("root signer self-verification failed")
        self._initialize_or_open()

    @property
    def authority_id(self) -> str:
        return self._authority_id

    @property
    def role(self) -> Literal["current_claim_monotonic_root"]:
        return _ROOT_ROLE

    @property
    def store_id(self) -> str:
        return self._store_id

    @property
    def storage_path(self) -> Path:
        return self._path

    def preflight(self) -> CurrentClaimRootPreflight:
        self.audit_summary()
        return CurrentClaimRootPreflight(
            role=_ROOT_ROLE,
            authority_id=self._authority_id,
            store_id=self._store_id,
            key_id=self._signer.key_id,
            public_key_fingerprint=self._signer.public_key_fingerprint,
            root_db_path=self._path,
        )

    def pin(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot:
        return self._write(
            kind="pin",
            operation_id=operation_id,
            current_claim_authority_id=current_claim_authority_id,
            previous_checkpoint_hash=_ZERO_HASH,
            checkpoint=checkpoint,
        )

    def current(self, *, current_claim_authority_id: str) -> CurrentClaimAntiRollbackRoot | None:
        identifier = current_claim_authority_id.strip()
        if not identifier:
            raise CurrentClaimAuthoritySecurityError(
                "current claim authority identity must be nonempty"
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._read_root_anchor(connection)
                result = self._read_current_root(connection, identifier)
                connection.commit()
                return result
            except BaseException:
                connection.rollback()
                raise

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot:
        return self._write(
            kind="advance",
            operation_id=operation_id,
            current_claim_authority_id=current_claim_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )

    def audit_summary(self) -> CurrentClaimRootAuditSummary:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._audit(connection)
                connection.commit()
                return summary
            except BaseException:
                connection.rollback()
                raise

    def _write(
        self,
        *,
        kind: Literal["pin", "advance"],
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAntiRollbackRoot:
        try:
            request = CurrentClaimRootOperationRequest(
                schema_version=1,
                contract="rquant-current-claim-root-operation/v1",
                kind=kind,
                operation_id=operation_id,
                current_claim_authority_id=current_claim_authority_id.strip(),
                previous_checkpoint_hash=previous_checkpoint_hash,
                checkpoint=checkpoint,
            )
        except ValidationError as exc:
            raise CurrentClaimAuthoritySecurityError("root operation request is invalid") from exc
        if request.checkpoint.authority_id != request.current_claim_authority_id:
            raise CurrentClaimAuthoritySecurityError(
                "root checkpoint authority identity is invalid"
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                anchor = self._read_root_anchor(connection)
                existing = connection.execute(
                    "SELECT * FROM current_claim_root_operation WHERE operation_id = ?",
                    (request.operation_id,),
                ).fetchone()
                if existing is not None:
                    stored_request, root = self._verify_root_operation_row(existing)
                    if stored_request != request:
                        raise CurrentClaimAuthoritySecurityError(
                            "root operation_id was rebound to a different request"
                        )
                    connection.commit()
                    return root
                current = self._read_current_root(
                    connection,
                    request.current_claim_authority_id,
                )
                if kind == "pin":
                    if current is not None:
                        raise CurrentClaimAuthoritySecurityError("root authority is already pinned")
                    if (
                        request.previous_checkpoint_hash != _ZERO_HASH
                        or request.checkpoint.operation_count != 0
                    ):
                        raise CurrentClaimAuthoritySecurityError(
                            "root pin requires the exact genesis checkpoint"
                        )
                else:
                    if current is None:
                        raise CurrentClaimAuthoritySecurityError("root authority is not pinned")
                    if (
                        request.previous_checkpoint_hash != current.checkpoint.checkpoint_hash
                        or request.checkpoint.operation_count
                        != current.checkpoint.operation_count + 1
                    ):
                        raise CurrentClaimAuthoritySecurityError(
                            "root compare-and-advance high-water mismatch"
                        )
                root = self._sign_root(request)
                operation_index = anchor.operation_count + 1
                operation_hash = canonical_sha256(
                    {
                        "contract": "rquant-current-claim-root-journal-entry/v1",
                        "operation_index": operation_index,
                        "previous_operation_hash": anchor.journal_root,
                        "request_hash": request.request_hash,
                        "root_hash": canonical_sha256(root.model_dump(mode="python")),
                    }
                )
                inserted = connection.execute(
                    "INSERT INTO current_claim_root_operation("
                    "operation_id, operation_index, request_json, request_hash, root_json, "
                    "previous_operation_hash, operation_hash"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        request.operation_id,
                        operation_index,
                        _canonical_json(request),
                        request.request_hash,
                        _canonical_json(root),
                        anchor.journal_root,
                        operation_hash,
                    ),
                ).rowcount
                if inserted != 1:
                    raise CurrentClaimAuthoritySecurityError("root operation append failed closed")
                if current is None:
                    connection.execute(
                        "INSERT INTO current_claim_root_state("
                        "current_claim_authority_id, root_json) VALUES (?, ?)",
                        (request.current_claim_authority_id, _canonical_json(root)),
                    )
                else:
                    updated = connection.execute(
                        "UPDATE current_claim_root_state SET root_json = ? "
                        "WHERE current_claim_authority_id = ? AND root_json = ?",
                        (
                            _canonical_json(root),
                            request.current_claim_authority_id,
                            _canonical_json(current),
                        ),
                    ).rowcount
                    if updated != 1:
                        raise CurrentClaimAuthoritySecurityError(
                            "root state compare-and-swap failed closed"
                        )
                updated = connection.execute(
                    "UPDATE current_claim_root_meta SET operation_count = ?, "
                    "journal_root = ? WHERE singleton = 1",
                    (operation_index, operation_hash),
                ).rowcount
                if updated != 1:
                    raise CurrentClaimAuthoritySecurityError(
                        "root metadata high-water update failed closed"
                    )
                if self._read_root_anchor(connection) != CurrentClaimRootAnchor(
                    operation_count=operation_index,
                    journal_root=operation_hash,
                ):
                    raise CurrentClaimAuthoritySecurityError("root metadata diverges after append")
                if (
                    self._read_current_root(
                        connection,
                        request.current_claim_authority_id,
                    )
                    != root
                ):
                    raise CurrentClaimAuthoritySecurityError(
                        "root materialized state diverges after append"
                    )
                connection.commit()
                return root
            except BaseException:
                connection.rollback()
                raise

    def _sign_root(self, request: CurrentClaimRootOperationRequest) -> CurrentClaimAntiRollbackRoot:
        unsigned = CurrentClaimAntiRollbackRoot(
            schema_version=1,
            contract="rquant-current-claim-anti-rollback-root/v1",
            role=_ROOT_ROLE,
            root_authority_id=self._authority_id,
            root_store_id=self._store_id,
            current_claim_authority_id=request.current_claim_authority_id,
            operation_id=request.operation_id,
            previous_checkpoint_hash=request.previous_checkpoint_hash,
            checkpoint=request.checkpoint,
            issuer=self._signer.issuer,
            key_id=self._signer.key_id,
            key_purpose=_ROOT_KEY_PURPOSE,
            namespace=_ROOT_RECEIPT_NAMESPACE,
            signature_algorithm=_ROOT_SIGNATURE_ALGORITHM,
            public_key_fingerprint=self._signer.public_key_fingerprint,
            signature="pending",
        )
        signature = self._signer.sign(
            namespace=_ROOT_RECEIPT_NAMESPACE,
            payload=unsigned.signing_bytes(),
        )
        signed = unsigned.model_copy(update={"signature": signature})
        self._verify_root(signed)
        return signed

    def _verify_root(self, root: CurrentClaimAntiRollbackRoot) -> None:
        if (
            root.role != _ROOT_ROLE
            or root.root_authority_id != self._authority_id
            or root.root_store_id != self._store_id
            or root.issuer != self._signer.issuer
            or root.key_id != self._signer.key_id
            or root.key_purpose != _ROOT_KEY_PURPOSE
            or root.namespace != _ROOT_RECEIPT_NAMESPACE
            or root.signature_algorithm != _ROOT_SIGNATURE_ALGORITHM
            or root.public_key_fingerprint != self._signer.public_key_fingerprint
            or not root.signature
        ):
            raise CurrentClaimAuthoritySecurityError(
                "root receipt role, authority, store, or signer identity is invalid"
            )
        try:
            verified = self._signer.verify(
                namespace=_ROOT_RECEIPT_NAMESPACE,
                payload=root.signing_bytes(),
                signature=root.signature,
            )
        except Exception as exc:
            raise CurrentClaimAuthoritySecurityError(
                "root receipt signature verification failed"
            ) from exc
        if not verified:
            raise CurrentClaimAuthoritySecurityError("root receipt signature verification failed")

    def _parse_root(self, value: str) -> CurrentClaimAntiRollbackRoot:
        try:
            root = strict_model_validate_canonical_json(CurrentClaimAntiRollbackRoot, value)
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError(
                "root receipt persistent JSON is malformed or non-canonical"
            ) from exc
        self._verify_root(root)
        return root

    @staticmethod
    def _parse_request(value: str) -> CurrentClaimRootOperationRequest:
        try:
            return strict_model_validate_canonical_json(
                CurrentClaimRootOperationRequest,
                value,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError(
                "root request persistent JSON is malformed or non-canonical"
            ) from exc

    def _read_root_anchor(self, connection: sqlite3.Connection) -> CurrentClaimRootAnchor:
        rows = connection.execute("SELECT * FROM current_claim_root_meta").fetchall()
        if len(rows) != 1:
            raise CurrentClaimAuthoritySecurityError("root metadata is missing or duplicated")
        row = rows[0]
        if (
            int(row["singleton"]) != 1
            or int(row["schema_version"]) != _ROOT_SCHEMA_VERSION
            or str(row["role"]) != _ROOT_ROLE
            or str(row["authority_id"]) != self._authority_id
            or str(row["store_id"]) != self._store_id
            or str(row["key_id"]) != self._signer.key_id
            or str(row["public_key_fingerprint"]) != self._signer.public_key_fingerprint
        ):
            if str(row["store_id"]) != self._store_id:
                raise CurrentClaimAuthoritySecurityError(
                    "root store identity does not match configured authority"
                )
            raise CurrentClaimAuthoritySecurityError(
                "root role, authority, or signer metadata was tampered"
            )
        try:
            anchor = CurrentClaimRootAnchor(
                operation_count=int(row["operation_count"]),
                journal_root=str(row["journal_root"]),
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError(
                "root metadata high-water is malformed"
            ) from exc
        tail = connection.execute(
            "SELECT operation_index, operation_hash FROM current_claim_root_operation "
            "ORDER BY operation_index DESC LIMIT 1"
        ).fetchone()
        if anchor.operation_count == 0:
            if tail is not None or anchor.journal_root != _ZERO_HASH:
                raise CurrentClaimAuthoritySecurityError("root metadata high-water was tampered")
        elif (
            tail is None
            or int(tail["operation_index"]) != anchor.operation_count
            or str(tail["operation_hash"]) != anchor.journal_root
        ):
            raise CurrentClaimAuthoritySecurityError("root metadata high-water was tampered")
        return anchor

    def _read_current_root(
        self,
        connection: sqlite3.Connection,
        current_claim_authority_id: str,
    ) -> CurrentClaimAntiRollbackRoot | None:
        row = connection.execute(
            "SELECT root_json FROM current_claim_root_state WHERE current_claim_authority_id = ?",
            (current_claim_authority_id,),
        ).fetchone()
        if row is None:
            return None
        root = self._parse_root(str(row["root_json"]))
        if root.current_claim_authority_id != current_claim_authority_id:
            raise CurrentClaimAuthoritySecurityError(
                "root materialized state identity was tampered"
            )
        operation = connection.execute(
            "SELECT * FROM current_claim_root_operation WHERE operation_id = ?",
            (root.operation_id,),
        ).fetchone()
        if operation is None:
            raise CurrentClaimAuthoritySecurityError(
                "root materialized state has no immutable operation"
            )
        _request, persisted = self._verify_root_operation_row(operation)
        if persisted != root:
            raise CurrentClaimAuthoritySecurityError(
                "root materialized state diverges from immutable operation"
            )
        return root

    def _verify_root_operation_row(
        self,
        row: sqlite3.Row,
    ) -> tuple[CurrentClaimRootOperationRequest, CurrentClaimAntiRollbackRoot]:
        request = self._parse_request(str(row["request_json"]))
        root = self._parse_root(str(row["root_json"]))
        try:
            operation_index = int(row["operation_index"])
        except (TypeError, ValueError) as exc:
            raise CurrentClaimAuthoritySecurityError(
                "root operation row index is malformed"
            ) from exc
        operation_hash = canonical_sha256(
            {
                "contract": "rquant-current-claim-root-journal-entry/v1",
                "operation_index": operation_index,
                "previous_operation_hash": str(row["previous_operation_hash"]),
                "request_hash": request.request_hash,
                "root_hash": canonical_sha256(root.model_dump(mode="python")),
            }
        )
        if (
            operation_index < 1
            or str(row["operation_id"]) != request.operation_id
            or str(row["request_hash"]) != request.request_hash
            or str(row["operation_hash"]) != operation_hash
            or root.operation_id != request.operation_id
            or root.current_claim_authority_id != request.current_claim_authority_id
            or root.previous_checkpoint_hash != request.previous_checkpoint_hash
            or root.checkpoint != request.checkpoint
        ):
            raise CurrentClaimAuthoritySecurityError("root operation row was tampered or rebound")
        return request, root

    def _audit(self, connection: sqlite3.Connection) -> CurrentClaimRootAuditSummary:
        self._validate_schema(connection)
        anchor = self._read_root_anchor(connection)
        expected_states: dict[str, CurrentClaimAntiRollbackRoot] = {}
        previous_hash = _ZERO_HASH
        operations = connection.execute(
            "SELECT * FROM current_claim_root_operation ORDER BY operation_index"
        ).fetchall()
        for expected_index, row in enumerate(operations, start=1):
            request = self._parse_request(str(row["request_json"]))
            root = self._parse_root(str(row["root_json"]))
            if (
                str(row["operation_id"]) != request.operation_id
                or int(row["operation_index"]) != expected_index
                or str(row["request_hash"]) != request.request_hash
                or str(row["previous_operation_hash"]) != previous_hash
                or root.operation_id != request.operation_id
                or root.current_claim_authority_id != request.current_claim_authority_id
                or root.previous_checkpoint_hash != request.previous_checkpoint_hash
                or root.checkpoint != request.checkpoint
            ):
                raise CurrentClaimAuthoritySecurityError(
                    "root operation row was tampered or rebound"
                )
            current = expected_states.get(request.current_claim_authority_id)
            if request.kind == "pin":
                if (
                    current is not None
                    or request.previous_checkpoint_hash != _ZERO_HASH
                    or request.checkpoint.operation_count != 0
                ):
                    raise CurrentClaimAuthoritySecurityError(
                        "root journal contains an invalid pin transition"
                    )
            elif (
                current is None
                or request.previous_checkpoint_hash != current.checkpoint.checkpoint_hash
                or request.checkpoint.operation_count != current.checkpoint.operation_count + 1
            ):
                raise CurrentClaimAuthoritySecurityError("root journal contains a fork or rollback")
            operation_hash = canonical_sha256(
                {
                    "contract": "rquant-current-claim-root-journal-entry/v1",
                    "operation_index": expected_index,
                    "previous_operation_hash": previous_hash,
                    "request_hash": request.request_hash,
                    "root_hash": canonical_sha256(root.model_dump(mode="python")),
                }
            )
            if str(row["operation_hash"]) != operation_hash:
                raise CurrentClaimAuthoritySecurityError("root operation journal hash was tampered")
            expected_states[request.current_claim_authority_id] = root
            previous_hash = operation_hash
        states: dict[str, CurrentClaimAntiRollbackRoot] = {}
        for row in connection.execute(
            "SELECT * FROM current_claim_root_state ORDER BY current_claim_authority_id"
        ):
            identifier = str(row["current_claim_authority_id"])
            root = self._parse_root(str(row["root_json"]))
            if root.current_claim_authority_id != identifier:
                raise CurrentClaimAuthoritySecurityError(
                    "root materialized state identity was tampered"
                )
            states[identifier] = root
        if states != expected_states:
            raise CurrentClaimAuthoritySecurityError(
                "root materialized state diverges from immutable journal"
            )
        if anchor.operation_count != len(operations) or anchor.journal_root != previous_hash:
            raise CurrentClaimAuthoritySecurityError("root metadata high-water was tampered")
        return CurrentClaimRootAuditSummary(
            role=_ROOT_ROLE,
            authority_id=self._authority_id,
            store_id=self._store_id,
            operation_count=len(operations),
            state_count=len(states),
            journal_root=previous_hash,
        )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self._path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA trusted_schema = OFF")
            yield connection
        finally:
            connection.close()

    def _initialize_or_open(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                objects = connection.execute(
                    "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                ).fetchall()
                if not objects:
                    self._create_schema(connection)
                self._audit(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        for sql in _ROOT_TABLE_SQL.values():
            connection.execute(sql)
        connection.execute(f"PRAGMA application_id = {_ROOT_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version = {_ROOT_SCHEMA_VERSION}")
        inserted = connection.execute(
            "INSERT INTO current_claim_root_meta("
            "singleton, schema_version, role, authority_id, store_id, key_id, "
            "public_key_fingerprint, operation_count, journal_root"
            ") VALUES (1, 1, ?, ?, ?, ?, ?, 0, ?)",
            (
                _ROOT_ROLE,
                self._authority_id,
                self._store_id,
                self._signer.key_id,
                self._signer.public_key_fingerprint,
                _ZERO_HASH,
            ),
        ).rowcount
        if inserted != 1:
            raise CurrentClaimAuthoritySecurityError("root metadata initialization failed closed")

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA application_id").fetchone()[0] != _ROOT_APPLICATION_ID:
            raise CurrentClaimAuthoritySecurityError("root schema application id is invalid")
        if connection.execute("PRAGMA user_version").fetchone()[0] != _ROOT_SCHEMA_VERSION:
            raise CurrentClaimAuthoritySecurityError("root schema version is invalid")
        objects = {
            (str(row["type"]), str(row["name"]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {("table", name) for name in _ROOT_TABLE_SQL}
        if objects != expected:
            raise CurrentClaimAuthoritySecurityError("root schema has unknown or missing objects")


class PersistentCurrentClaimAuthority:
    """SQLite implementation of :class:`CurrentClaimAuthorityProtocol`.

    ``replace_current`` and ``issue_plan_once`` share one SQLite writer boundary.
    In production each mutation first becomes a durable, closed pending proof, then
    compares its candidate checkpoint with an external monotonic root.  A response
    loss can therefore be recovered without signing twice.  The authority never
    accepts a signer or callback per issue operation.
    """

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        signer: SourceUsePlanSigner,
        keyring: VerifyOnlyEd25519Keyring,
        monotonic_root: CurrentClaimMonotonicRoot | None = None,
        mode: Literal["production", "test-external", "test-standalone"] = "production",
        busy_timeout_ms: int = 5_000,
    ) -> None:
        normalized_id = authority_id.strip()
        if not normalized_id:
            raise CurrentClaimAuthoritySecurityError("authority_id must be nonempty")
        if mode not in {"production", "test-external", "test-standalone"}:
            raise CurrentClaimAuthoritySecurityError("authority mode is invalid")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise CurrentClaimAuthoritySecurityError("busy_timeout_ms must be a positive int")
        if mode in {"production", "test-external"} and monotonic_root is None:
            raise CurrentClaimAuthoritySecurityError(
                "production current claim authority requires an independent monotonic root"
            )
        if mode == "production" and (
            type(monotonic_root) is not ExternalCurrentClaimMonotonicRootAdapter
            or not isinstance(monotonic_root, ExternalCurrentClaimMonotonicRootAdapter)
            or not monotonic_root.production_ready
        ):
            raise CurrentClaimAuthoritySecurityError(
                "production current claim authority requires a validated external witness adapter; "
                "the local SQLite materializer is non-production"
            )
        if (
            mode == "test-external"
            and type(monotonic_root) is not ExternalCurrentClaimMonotonicRootAdapter
        ):
            raise CurrentClaimAuthoritySecurityError(
                "test-external current claim authority requires the explicit test adapter"
            )
        if mode == "test-standalone" and monotonic_root is not None:
            raise CurrentClaimAuthoritySecurityError(
                "test-standalone current claim authority cannot compose a production root"
            )
        self.path = _absolute_path(path)
        self._authority_id = normalized_id
        self._signer = signer
        self._keyring = keyring
        self._mode = mode
        self._root = monotonic_root
        self._root_config_hash = (
            monotonic_root.config.config_hash
            if isinstance(monotonic_root, ExternalCurrentClaimMonotonicRootAdapter)
            else _ZERO_HASH
        )
        self._busy_timeout_ms = busy_timeout_ms
        self._root_store_path: Path | None = None
        if monotonic_root is not None and monotonic_root.storage_path is not None:
            root_path = _absolute_path(monotonic_root.storage_path)
            if root_path == self.path:
                raise CurrentClaimAuthoritySecurityError(
                    "authority database and monotonic root storage must be independent"
                )
            self._root_store_path = root_path
        identity = self.plan_signer_identity
        if identity.key_purpose != "source_use_plan_v2":
            raise CurrentClaimAuthoritySecurityError("current claim signer purpose is invalid")
        self._initialize_or_open()
        if self._mode in {"production", "test-external"}:
            self._synchronize_production_root()

    @property
    def authority_id(self) -> str:
        return self._authority_id

    @property
    def monotonic_root(self) -> CurrentClaimMonotonicRoot | None:
        return self._root

    @property
    def plan_signer_identity(self) -> CurrentClaimPlanSignerIdentityV2:
        return CurrentClaimPlanSignerIdentityV2(
            issuer=self._signer.issuer,
            key_id=self._signer.key_id,
        )

    def preflight(self) -> CurrentClaimAuthorityPreflight:
        if self._mode in {"production", "test-external"}:
            self._synchronize_production_root()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._audit_state(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        root = self._root
        return CurrentClaimAuthorityPreflight(
            mode=self._mode,
            non_production=self._mode != "production",
            root_required=self._mode in {"production", "test-external"},
            root_configured=root is not None,
            root_authority_id=None if root is None else root.authority_id,
            authority_db_path=self.path,
            root_store_path=self._root_store_path,
        )

    def replace_current(self, claim: LabShardClaimV2) -> LabShardClaimV2:
        """Publish a strictly newer unbound claim for its scheduler shard slot."""

        validated = self._strict_claim(claim)
        if validated.source_use_plan is not None:
            raise SourceOperationContractError(
                "current claim publication requires an unbound claim"
            )
        try:
            validated.strategy_payload.source_intent.require_verified(self._keyring)
        except SourceOperationContractError:
            raise
        except Exception as exc:
            raise SourceOperationContractError("current claim source intent is invalid") from exc
        slot = _claim_slot(validated)
        claim_hash = _claim_hash(validated)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_in_transaction(connection)
                existing = self._read_current(connection, slot)
                if existing is not None:
                    if existing == validated:
                        connection.commit()
                        return existing
                    if (
                        validated.claim_generation <= existing.claim_generation
                        or validated.scheduler_fencing_token <= existing.scheduler_fencing_token
                    ):
                        raise SourceOperationContractError(
                            "current claim replacement is stale relative to authority high-water"
                        )
                pending = self._build_publish_pending(
                    connection,
                    slot=slot,
                    claim=validated,
                    claim_hash=claim_hash,
                    previous_claim=existing,
                )
                self._write_pending(connection, pending)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self._root_and_complete(pending).claim  # type: ignore[return-value]

    publish_current = replace_current

    def issue_plan_once(
        self,
        *,
        issue: CurrentClaimPlanIssueV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        """Sign at most once for one current attempt, recovering durable responses."""

        current_now = _normalize_now(now)
        try:
            structurally_valid = CurrentClaimPlanIssueV2.model_validate(issue, strict=True)
        except ValidationError as exc:
            raise SourceOperationContractError(
                f"current claim issue contract is invalid: {exc}"
            ) from exc
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_in_transaction(connection)
                existing = self._read_issue(connection, structurally_valid.binding.operation_id)
                if existing is not None:
                    stored_issue_hash, receipt = existing
                    if stored_issue_hash != structurally_valid.issue_hash:
                        raise SourceOperationContractError(
                            "operation_id consumption binding changed"
                        )
                    self._verify_receipt(receipt, structurally_valid)
                    connection.commit()
                    return receipt
                validated = require_current_claim_plan_issue_v2(
                    structurally_valid,
                    keyring=self._keyring,
                    authority_id=self._authority_id,
                    signer_identity=self.plan_signer_identity,
                )
                self._require_current(connection, binding=validated.binding, now=current_now)
                prior = connection.execute(
                    "SELECT operation_id FROM current_claim_issue WHERE attempt_identity_hash = ?",
                    (validated.binding.attempt_identity_hash,),
                ).fetchone()
                if prior is not None:
                    raise SourceOperationContractError(
                        "source attempt was already consumed by a different operation_id"
                    )
                unsigned = validated.unsigned_plan
                signature = self._signer.sign(
                    namespace=SOURCE_USE_PLAN_V2_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
                signed = unsigned.model_copy(update={"signature": signature})
                signed = require_source_use_plan_v2(
                    signed,
                    keyring=self._keyring,
                    audience=unsigned.audience,
                    now=current_now,
                )
                if signed.signing_payload() != unsigned.signing_payload():
                    raise SourceOperationContractError("authority signer rebound unsigned plan")
                self._require_current(connection, binding=validated.binding, now=current_now)
                receipt = CurrentClaimConsumptionV2.from_signed_plan(
                    binding=validated.binding,
                    signed_plan=signed,
                    committed_at=current_now,
                )
                self._verify_receipt(receipt, validated)
                pending = self._build_issue_pending(
                    connection,
                    issue=validated,
                    receipt=receipt,
                )
                self._write_pending(connection, pending)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        completed = self._root_and_complete(pending)
        if completed.receipt is None:
            raise CurrentClaimAuthoritySecurityError("completed issue mutation has no receipt")
        return completed.receipt

    def verify_current(
        self,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        current_now = _normalize_now(now)
        try:
            validated_binding = CurrentClaimConsumptionBindingV2.model_validate(
                binding,
                strict=True,
            )
        except ValidationError as exc:
            raise SourceOperationContractError("current claim binding is invalid") from exc
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_in_transaction(connection)
                self._require_current(connection, binding=validated_binding, now=current_now)
                stored = self._read_issue(connection, validated_binding.operation_id)
                if stored is None:
                    raise SourceOperationContractError(
                        "current claim has no exact committed operation"
                    )
                _issue_hash, receipt = stored
                if receipt.binding != validated_binding:
                    raise SourceOperationContractError(
                        "current claim has no exact committed operation"
                    )
                unsigned = receipt.signed_plan.model_copy(update={"signature": ""})
                issue = CurrentClaimPlanIssueV2.from_unsigned_plan(unsigned)
                self._verify_receipt(receipt, issue)
                connection.commit()
                return receipt
            except BaseException:
                connection.rollback()
                raise

    @contextmanager
    def hold_current(
        self,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> Iterator[CurrentClaimConsumptionV2]:
        """Hold the exact current claim across one externally ordered ledger CAS."""

        current_now = _normalize_now(now)
        validated_binding = CurrentClaimConsumptionBindingV2.model_validate(binding, strict=True)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_in_transaction(connection)
                self._require_current(connection, binding=validated_binding, now=current_now)
                stored = self._read_issue(connection, validated_binding.operation_id)
                if stored is None:
                    raise SourceOperationContractError(
                        "current claim has no exact committed operation"
                    )
                _issue_hash, receipt = stored
                if receipt.binding != validated_binding:
                    raise SourceOperationContractError(
                        "current claim has no exact committed operation"
                    )
                self._verify_receipt(
                    receipt,
                    CurrentClaimPlanIssueV2.from_unsigned_plan(
                        receipt.signed_plan.model_copy(update={"signature": ""})
                    ),
                )
                yield receipt
                self._require_current(connection, binding=validated_binding, now=current_now)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def audit_summary(self) -> CurrentClaimAuthorityAuditSummary:
        if self._mode in {"production", "test-external"}:
            self._synchronize_production_root()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._audit_state(connection)
                connection.commit()
                return summary
            except BaseException:
                connection.rollback()
                raise

    def _root_and_complete(
        self, pending: CurrentClaimPendingMutation
    ) -> CurrentClaimPendingMutation:
        if self._mode == "test-standalone":
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._apply_pending(connection, pending)
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            return pending
        root = self._require_root()
        rooted = self._coerce_root(
            root.compare_and_advance(
                operation_id=pending.mutation_id,
                current_claim_authority_id=self._authority_id,
                previous_checkpoint_hash=pending.previous_checkpoint_hash,
                checkpoint=pending.checkpoint,
            )
        )
        if rooted is None or not self._root_matches_pending(rooted, pending):
            raise CurrentClaimAuthoritySecurityError(
                "monotonic root compare-and-advance returned a divergent response"
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_in_transaction(connection)
                completed = self._read_pending(connection)
                if completed is not None:
                    raise CurrentClaimAuthoritySecurityError(
                        "rooted pending mutation was not completed"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return pending

    def _synchronize_production_root(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_in_transaction(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _synchronize_in_transaction(self, connection: sqlite3.Connection) -> None:
        summary = self._summary_from_anchor(self._read_materialized_anchor(connection))
        if self._mode == "test-standalone":
            pending = self._read_pending(connection)
            if pending is not None:
                self._apply_pending(connection, pending)
            return
        root_store = self._require_root()
        pending = self._read_pending(connection)
        rooted = self._coerce_root(
            root_store.current(current_claim_authority_id=self._authority_id)
        )
        if rooted is None:
            if summary.operation_count != 0 or pending is not None:
                raise CurrentClaimAuthorityRepairRequiredError(
                    self._repair_state(
                        reason="local_state_ahead_of_external_root",
                        summary=summary,
                        root_checkpoint=CurrentClaimCheckpoint(
                            schema_version=1,
                            contract="rquant-current-claim-checkpoint/v1",
                            authority_id=self._authority_id,
                            operation_count=0,
                            journal_root=_ZERO_HASH,
                            state_root=_materialized_state_root(
                                claim_accumulator=_ZERO_HASH,
                                issue_accumulator=_ZERO_HASH,
                                claim_count=0,
                                issue_count=0,
                            ),
                        ),
                    )
                )
            checkpoint = self._checkpoint_from_summary(summary)
            operation_id = canonical_sha256(
                {
                    "contract": "rquant-current-claim-root-pin/v1",
                    "authority_id": self._authority_id,
                    "checkpoint_hash": checkpoint.checkpoint_hash,
                }
            )
            pinned = self._coerce_root(
                root_store.pin(
                    operation_id=operation_id,
                    current_claim_authority_id=self._authority_id,
                    checkpoint=checkpoint,
                )
            )
            if pinned is None or not self._root_matches(
                pinned,
                operation_id=operation_id,
                previous_checkpoint_hash=_ZERO_HASH,
                checkpoint=checkpoint,
            ):
                raise CurrentClaimAuthoritySecurityError(
                    "monotonic root pin returned a divergent response"
                )
            return
        local = self._checkpoint_from_summary(summary)
        if rooted.checkpoint == local:
            if pending is None:
                return
            if self._root_matches_pending(rooted, pending):
                self._apply_pending(connection, pending)
                return
            advanced = self._coerce_root(
                root_store.compare_and_advance(
                    operation_id=pending.mutation_id,
                    current_claim_authority_id=self._authority_id,
                    previous_checkpoint_hash=pending.previous_checkpoint_hash,
                    checkpoint=pending.checkpoint,
                )
            )
            if advanced is None or not self._root_matches_pending(advanced, pending):
                raise CurrentClaimAuthoritySecurityError(
                    "pending monotonic root advance returned a divergent response"
                )
            self._apply_pending(connection, pending)
            return
        if pending is not None and self._root_matches_pending(rooted, pending):
            self._apply_pending(connection, pending)
            return
        if rooted.checkpoint.operation_count > local.operation_count:
            reason: Literal[
                "external_root_ahead_without_pending_proof",
                "external_root_diverges_at_same_high_water",
                "local_state_ahead_of_external_root",
            ] = "external_root_ahead_without_pending_proof"
        elif rooted.checkpoint.operation_count == local.operation_count:
            reason = "external_root_diverges_at_same_high_water"
        else:
            reason = "local_state_ahead_of_external_root"
        raise CurrentClaimAuthorityRepairRequiredError(
            self._repair_state(reason=reason, summary=summary, root_checkpoint=rooted.checkpoint)
        )

    def _repair_state(
        self,
        *,
        reason: Literal[
            "external_root_ahead_without_pending_proof",
            "external_root_diverges_at_same_high_water",
            "local_state_ahead_of_external_root",
        ],
        summary: CurrentClaimAuthorityAuditSummary,
        root_checkpoint: CurrentClaimCheckpoint,
    ) -> CurrentClaimAuthorityRepairState:
        root = self._require_root()
        return CurrentClaimAuthorityRepairState(
            status="repair_required",
            reason=reason,
            authority_id=self._authority_id,
            root_authority_id=root.authority_id,
            local_operation_count=summary.operation_count,
            root_operation_count=root_checkpoint.operation_count,
            local_checkpoint_hash=summary.checkpoint_hash,
            root_checkpoint_hash=root_checkpoint.checkpoint_hash,
            non_production=self._mode != "production",
        )

    def _root_matches_pending(
        self,
        root: CurrentClaimAntiRollbackRoot,
        pending: CurrentClaimPendingMutation,
    ) -> bool:
        return self._root_matches(
            root,
            operation_id=pending.mutation_id,
            previous_checkpoint_hash=pending.previous_checkpoint_hash,
            checkpoint=pending.checkpoint,
        )

    def _root_matches(
        self,
        receipt: CurrentClaimAntiRollbackRoot,
        *,
        operation_id: str,
        previous_checkpoint_hash: str,
        checkpoint: CurrentClaimCheckpoint,
    ) -> bool:
        authority_root = self._require_root()
        return (
            receipt.role == _ROOT_ROLE
            and receipt.root_authority_id == authority_root.authority_id
            and receipt.root_store_id == authority_root.store_id
            and receipt.current_claim_authority_id == self._authority_id
            and receipt.operation_id == operation_id
            and receipt.previous_checkpoint_hash == previous_checkpoint_hash
            and receipt.checkpoint == checkpoint
            and bool(receipt.signature)
        )

    def _build_publish_pending(
        self,
        connection: sqlite3.Connection,
        *,
        slot: str,
        claim: LabShardClaimV2,
        claim_hash: str,
        previous_claim: LabShardClaimV2 | None,
    ) -> CurrentClaimPendingMutation:
        anchor = self._read_materialized_anchor(connection)
        summary = self._summary_from_anchor(anchor)
        payload_hash = canonical_sha256(
            {
                "claim_hash": claim_hash,
                "claim_slot": slot,
                "contract": "rquant-current-claim-publish/v1",
            }
        )
        mutation_id = canonical_sha256(
            {
                "authority_id": self._authority_id,
                "contract": "rquant-current-claim-root-mutation/v1",
                "kind": "publish",
                "payload_hash": payload_hash,
                "previous_checkpoint_hash": summary.checkpoint_hash,
            }
        )
        journal_index = summary.operation_count + 1
        journal_hash = _journal_hash(
            journal_index=journal_index,
            mutation_id=mutation_id,
            kind="publish",
            payload_hash=payload_hash,
            previous_journal_hash=summary.journal_root,
        )
        claim_accumulator = _xor_hashes(
            anchor.claim_accumulator,
            _claim_leaf_hash(claim_slot=slot, claim_hash=claim_hash),
            *(
                ()
                if previous_claim is None
                else (
                    _claim_leaf_hash(
                        claim_slot=slot,
                        claim_hash=_claim_hash(previous_claim),
                    ),
                )
            ),
        )
        checkpoint = self._checkpoint_from_materialized(
            operation_count=journal_index,
            journal_root=journal_hash,
            claim_accumulator=claim_accumulator,
            issue_accumulator=anchor.issue_accumulator,
            claim_count=anchor.claim_count + (previous_claim is None),
            issue_count=anchor.issue_count,
        )
        return CurrentClaimPendingMutation(
            schema_version=1,
            contract="rquant-current-claim-pending-mutation/v1",
            kind="publish",
            mutation_id=mutation_id,
            previous_checkpoint_hash=summary.checkpoint_hash,
            checkpoint=checkpoint,
            journal_index=journal_index,
            journal_hash=journal_hash,
            journal_payload_hash=payload_hash,
            claim_slot=slot,
            claim=claim,
            claim_hash=claim_hash,
        )

    def _build_issue_pending(
        self,
        connection: sqlite3.Connection,
        *,
        issue: CurrentClaimPlanIssueV2,
        receipt: CurrentClaimConsumptionV2,
    ) -> CurrentClaimPendingMutation:
        anchor = self._read_materialized_anchor(connection)
        summary = self._summary_from_anchor(anchor)
        payload_hash = canonical_sha256(
            {
                "contract": "rquant-current-claim-issue/v1",
                "issue_hash": issue.issue_hash,
                "receipt_hash": receipt.receipt_hash,
            }
        )
        mutation_id = canonical_sha256(
            {
                "authority_id": self._authority_id,
                "contract": "rquant-current-claim-root-mutation/v1",
                "kind": "issue",
                "operation_id": issue.binding.operation_id,
                "payload_hash": payload_hash,
                "previous_checkpoint_hash": summary.checkpoint_hash,
            }
        )
        journal_index = summary.operation_count + 1
        journal_hash = _journal_hash(
            journal_index=journal_index,
            mutation_id=mutation_id,
            kind="issue",
            payload_hash=payload_hash,
            previous_journal_hash=summary.journal_root,
        )
        checkpoint = self._checkpoint_from_materialized(
            operation_count=journal_index,
            journal_root=journal_hash,
            claim_accumulator=anchor.claim_accumulator,
            issue_accumulator=_xor_hashes(
                anchor.issue_accumulator,
                _issue_leaf_hash(
                    operation_id=issue.binding.operation_id,
                    issue_hash=issue.issue_hash,
                    receipt_hash=receipt.receipt_hash,
                ),
            ),
            claim_count=anchor.claim_count,
            issue_count=anchor.issue_count + 1,
        )
        return CurrentClaimPendingMutation(
            schema_version=1,
            contract="rquant-current-claim-pending-mutation/v1",
            kind="issue",
            mutation_id=mutation_id,
            previous_checkpoint_hash=summary.checkpoint_hash,
            checkpoint=checkpoint,
            journal_index=journal_index,
            journal_hash=journal_hash,
            journal_payload_hash=payload_hash,
            issue=issue,
            receipt=receipt,
        )

    def _write_pending(
        self, connection: sqlite3.Connection, pending: CurrentClaimPendingMutation
    ) -> None:
        existing = self._read_pending(connection)
        if existing is not None:
            if existing != pending:
                raise CurrentClaimAuthoritySecurityError(
                    "different pending current-claim mutation blocks progress"
                )
            return
        inserted = connection.execute(
            "INSERT INTO current_claim_pending(singleton, pending_json) VALUES (1, ?)",
            (_canonical_json(pending),),
        ).rowcount
        if inserted != 1:
            raise CurrentClaimAuthoritySecurityError("pending mutation append failed closed")

    def _anchor_after_pending(
        self,
        connection: sqlite3.Connection,
        *,
        anchor: CurrentClaimMaterializedAnchor,
        pending: CurrentClaimPendingMutation,
    ) -> CurrentClaimMaterializedAnchor:
        if pending.kind == "publish":
            assert pending.claim_slot is not None
            assert pending.claim_hash is not None
            previous_claim = self._read_current(connection, pending.claim_slot)
            claim_accumulator = _xor_hashes(
                anchor.claim_accumulator,
                _claim_leaf_hash(
                    claim_slot=pending.claim_slot,
                    claim_hash=pending.claim_hash,
                ),
                *(
                    ()
                    if previous_claim is None
                    else (
                        _claim_leaf_hash(
                            claim_slot=pending.claim_slot,
                            claim_hash=_claim_hash(previous_claim),
                        ),
                    )
                ),
            )
            issue_accumulator = anchor.issue_accumulator
            claim_count = anchor.claim_count + (1 if previous_claim is None else 0)
            issue_count = anchor.issue_count
        else:
            assert pending.issue is not None
            assert pending.receipt is not None
            if self._read_issue(connection, pending.issue.binding.operation_id) is not None:
                raise CurrentClaimAuthoritySecurityError(
                    "pending issue operation already exists before apply"
                )
            claim_accumulator = anchor.claim_accumulator
            issue_accumulator = _xor_hashes(
                anchor.issue_accumulator,
                _issue_leaf_hash(
                    operation_id=pending.issue.binding.operation_id,
                    issue_hash=pending.issue.issue_hash,
                    receipt_hash=pending.receipt.receipt_hash,
                ),
            )
            claim_count = anchor.claim_count
            issue_count = anchor.issue_count + 1
        checkpoint = self._checkpoint_from_materialized(
            operation_count=pending.journal_index,
            journal_root=pending.journal_hash,
            claim_accumulator=claim_accumulator,
            issue_accumulator=issue_accumulator,
            claim_count=claim_count,
            issue_count=issue_count,
        )
        result = CurrentClaimMaterializedAnchor(
            checkpoint=checkpoint,
            claim_accumulator=claim_accumulator,
            issue_accumulator=issue_accumulator,
            claim_count=claim_count,
            issue_count=issue_count,
        )
        if result.checkpoint != pending.checkpoint:
            raise CurrentClaimAuthoritySecurityError(
                "pending mutation materialized root is divergent"
            )
        return result

    def _apply_pending(
        self, connection: sqlite3.Connection, pending: CurrentClaimPendingMutation
    ) -> None:
        anchor = self._read_materialized_anchor(connection)
        if anchor.checkpoint.checkpoint_hash != pending.previous_checkpoint_hash:
            raise CurrentClaimAuthoritySecurityError(
                "pending mutation predecessor is not the local high-water"
            )
        stored = self._read_pending(connection)
        if stored != pending:
            raise CurrentClaimAuthoritySecurityError("pending mutation was changed before apply")
        next_anchor = self._anchor_after_pending(
            connection,
            anchor=anchor,
            pending=pending,
        )
        if pending.kind == "publish":
            assert pending.claim_slot is not None
            assert pending.claim is not None
            assert pending.claim_hash is not None
            connection.execute(
                "INSERT INTO current_claim_current("
                "claim_slot, claim_json, claim_hash, claim_generation, scheduler_fencing_token"
                ") VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(claim_slot) DO UPDATE SET "
                "claim_json=excluded.claim_json, claim_hash=excluded.claim_hash, "
                "claim_generation=excluded.claim_generation, "
                "scheduler_fencing_token=excluded.scheduler_fencing_token",
                (
                    pending.claim_slot,
                    _canonical_json(pending.claim),
                    pending.claim_hash,
                    pending.claim.claim_generation,
                    pending.claim.scheduler_fencing_token,
                ),
            )
        else:
            assert pending.issue is not None
            assert pending.receipt is not None
            inserted = connection.execute(
                "INSERT INTO current_claim_issue("
                "operation_id, attempt_identity_hash, issue_hash, receipt_json"
                ") VALUES (?, ?, ?, ?)",
                (
                    pending.issue.binding.operation_id,
                    pending.issue.binding.attempt_identity_hash,
                    pending.issue.issue_hash,
                    _canonical_json(pending.receipt),
                ),
            ).rowcount
            if inserted != 1:
                raise CurrentClaimAuthoritySecurityError("issue receipt append failed closed")
        inserted = connection.execute(
            "INSERT INTO current_claim_journal("
            "journal_index, mutation_id, kind, payload_hash, previous_journal_hash, journal_hash"
            ") VALUES (?, ?, ?, ?, ?, ?)",
            (
                pending.journal_index,
                pending.mutation_id,
                pending.kind,
                pending.journal_payload_hash,
                anchor.checkpoint.journal_root,
                pending.journal_hash,
            ),
        ).rowcount
        if inserted != 1:
            raise CurrentClaimAuthoritySecurityError("authority journal append failed closed")
        updated = connection.execute(
            "UPDATE current_claim_authority_meta SET checkpoint_json = ?, "
            "claim_accumulator = ?, issue_accumulator = ?, claim_count = ?, issue_count = ? "
            "WHERE singleton = 1 AND checkpoint_json = ?",
            (
                _canonical_json(next_anchor.checkpoint),
                next_anchor.claim_accumulator,
                next_anchor.issue_accumulator,
                next_anchor.claim_count,
                next_anchor.issue_count,
                _canonical_json(anchor.checkpoint),
            ),
        ).rowcount
        if updated != 1:
            raise CurrentClaimAuthoritySecurityError("authority checkpoint update failed closed")
        deleted = connection.execute(
            "DELETE FROM current_claim_pending WHERE singleton = 1"
        ).rowcount
        if deleted != 1:
            raise CurrentClaimAuthoritySecurityError("pending mutation delete failed closed")
        if self._read_materialized_anchor(connection) != next_anchor:
            raise CurrentClaimAuthoritySecurityError("pending mutation produced a divergent state")
        if pending.kind == "publish":
            assert pending.claim_slot is not None
            assert pending.claim is not None
            if self._read_current(connection, pending.claim_slot) != pending.claim:
                raise CurrentClaimAuthoritySecurityError("pending claim row diverges after apply")
        else:
            assert pending.issue is not None
            assert pending.receipt is not None
            if self._read_issue(connection, pending.issue.binding.operation_id) != (
                pending.issue.issue_hash,
                pending.receipt,
            ):
                raise CurrentClaimAuthoritySecurityError("pending issue row diverges after apply")

    def _require_current(
        self,
        connection: sqlite3.Connection,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> LabShardClaimV2:
        if binding.authority_id != self._authority_id:
            raise SourceOperationContractError("claim is not the current authority high-water")
        slot = f"{binding.attempt_binding.job_id}:{binding.attempt_binding.shard_id}"
        current = self._read_current(connection, slot)
        if current is None:
            raise SourceOperationContractError("claim is not the current authority high-water")
        payload = current.strategy_payload
        manifest = payload.source_intent.manifest
        expected_source_identity = (
            current.definition.adapter_id,
            current.definition.adapter_version,
            current.adapter_code_hash,
            current.definition.payload_hash,
            current.payload_source_contract_hash,
            current.manifest_hash,
            payload.source_intent.resource_request_hash,
        )
        observed_source_identity = (
            binding.adapter_id,
            binding.adapter_version,
            binding.adapter_code_hash,
            binding.payload_hash,
            binding.payload_source_contract_hash,
            binding.manifest_hash,
            binding.resource_request_hash,
        )
        if (
            binding.attempt_binding != current.attempt_binding
            or binding.lease_expires_at != current.lease_expires_at
            or observed_source_identity != expected_source_identity
            or (binding.adapter_id, binding.adapter_version)
            != (manifest.adapter_id, manifest.adapter_version)
        ):
            raise SourceOperationContractError("claim is not the current authority high-water")
        if now < binding.not_before:
            raise SourceOperationContractError("current source plan is not active")
        if now >= min(binding.expires_at, binding.lease_expires_at):
            raise SourceOperationContractError("current source plan or claim lease is expired")
        return current

    def _strict_claim(self, claim: LabShardClaimV2) -> LabShardClaimV2:
        try:
            raw = canonical_model_json_bytes(claim)
            return strict_model_validate_canonical_json(LabShardClaimV2, raw)
        except (TypeError, ValueError, ValidationError) as exc:
            raise SourceOperationContractError("current claim contract is invalid") from exc

    def _verify_receipt(
        self,
        receipt: CurrentClaimConsumptionV2,
        issue: CurrentClaimPlanIssueV2,
    ) -> None:
        try:
            raw = canonical_model_json_bytes(receipt)
            validated = strict_model_validate_canonical_json(CurrentClaimConsumptionV2, raw)
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError("stored receipt is malformed") from exc
        _require_receipt_matches_issue(validated, issue)
        plan = validated.signed_plan
        identity = self.plan_signer_identity
        if (
            plan.single_use_authority_id != self._authority_id
            or (plan.issuer, plan.key_id) != (identity.issuer, identity.key_id)
            or not plan.verify(self._keyring)
        ):
            raise CurrentClaimAuthoritySecurityError("stored receipt signature is invalid")

    def _read_current(self, connection: sqlite3.Connection, slot: str) -> LabShardClaimV2 | None:
        row = connection.execute(
            "SELECT * FROM current_claim_current WHERE claim_slot = ?", (slot,)
        ).fetchone()
        if row is None:
            return None
        try:
            claim = strict_model_validate_canonical_json(LabShardClaimV2, str(row["claim_json"]))
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError("current claim row is malformed") from exc
        if (
            str(row["claim_hash"]) != _claim_hash(claim)
            or int(row["claim_generation"]) != claim.claim_generation
            or int(row["scheduler_fencing_token"]) != claim.scheduler_fencing_token
            or str(row["claim_slot"]) != _claim_slot(claim)
        ):
            raise CurrentClaimAuthoritySecurityError("current claim row was tampered")
        return claim

    def _all_current_claims(self, connection: sqlite3.Connection) -> dict[str, LabShardClaimV2]:
        claims: dict[str, LabShardClaimV2] = {}
        rows = connection.execute(
            "SELECT claim_slot FROM current_claim_current ORDER BY claim_slot"
        )
        for row in rows:
            slot = str(row["claim_slot"])
            claim = self._read_current(connection, slot)
            if claim is None:
                raise CurrentClaimAuthoritySecurityError("current claim vanished during audit")
            claims[slot] = claim
        return claims

    def _read_issue(
        self, connection: sqlite3.Connection, operation_id: str
    ) -> tuple[str, CurrentClaimConsumptionV2] | None:
        row = connection.execute(
            "SELECT * FROM current_claim_issue WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            return None
        try:
            receipt = strict_model_validate_canonical_json(
                CurrentClaimConsumptionV2, str(row["receipt_json"])
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError("issue receipt row is malformed") from exc
        unsigned = receipt.signed_plan.model_copy(update={"signature": ""})
        issue = CurrentClaimPlanIssueV2.from_unsigned_plan(unsigned)
        if (
            str(row["operation_id"]) != receipt.binding.operation_id
            or str(row["attempt_identity_hash"]) != receipt.binding.attempt_identity_hash
            or str(row["issue_hash"]) != issue.issue_hash
        ):
            raise CurrentClaimAuthoritySecurityError("issue receipt row was tampered")
        self._verify_receipt(receipt, issue)
        return issue.issue_hash, receipt

    def _all_issues(
        self, connection: sqlite3.Connection
    ) -> dict[str, tuple[str, CurrentClaimConsumptionV2]]:
        issues: dict[str, tuple[str, CurrentClaimConsumptionV2]] = {}
        rows = connection.execute(
            "SELECT operation_id FROM current_claim_issue ORDER BY operation_id"
        )
        for row in rows:
            operation_id = str(row["operation_id"])
            stored = self._read_issue(connection, operation_id)
            if stored is None:
                raise CurrentClaimAuthoritySecurityError("issue receipt vanished during audit")
            issues[operation_id] = stored
        return issues

    def _read_pending(self, connection: sqlite3.Connection) -> CurrentClaimPendingMutation | None:
        rows = connection.execute("SELECT * FROM current_claim_pending").fetchall()
        if not rows:
            return None
        if len(rows) != 1 or int(rows[0]["singleton"]) != 1:
            raise CurrentClaimAuthoritySecurityError("pending mutation table is malformed")
        try:
            return strict_model_validate_canonical_json(
                CurrentClaimPendingMutation, str(rows[0]["pending_json"])
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError("pending mutation is malformed") from exc

    def _candidate_checkpoint(
        self,
        *,
        claims: Mapping[str, LabShardClaimV2],
        issues: Mapping[str, tuple[str, CurrentClaimConsumptionV2]],
        operation_count: int,
        journal_root: str,
    ) -> CurrentClaimCheckpoint:
        claim_accumulator = _xor_hashes(
            *(
                _claim_leaf_hash(claim_slot=slot, claim_hash=_claim_hash(claim))
                for slot, claim in claims.items()
            )
        )
        issue_accumulator = _xor_hashes(
            *(
                _issue_leaf_hash(
                    operation_id=operation_id,
                    issue_hash=issue_hash,
                    receipt_hash=receipt.receipt_hash,
                )
                for operation_id, (issue_hash, receipt) in issues.items()
            )
        )
        return self._checkpoint_from_materialized(
            operation_count=operation_count,
            journal_root=journal_root,
            claim_accumulator=claim_accumulator,
            issue_accumulator=issue_accumulator,
            claim_count=len(claims),
            issue_count=len(issues),
        )

    def _checkpoint_from_materialized(
        self,
        *,
        operation_count: int,
        journal_root: str,
        claim_accumulator: str,
        issue_accumulator: str,
        claim_count: int,
        issue_count: int,
    ) -> CurrentClaimCheckpoint:
        return CurrentClaimCheckpoint(
            schema_version=1,
            contract="rquant-current-claim-checkpoint/v1",
            authority_id=self._authority_id,
            operation_count=operation_count,
            journal_root=journal_root,
            state_root=_materialized_state_root(
                claim_accumulator=claim_accumulator,
                issue_accumulator=issue_accumulator,
                claim_count=claim_count,
                issue_count=issue_count,
            ),
        )

    def _checkpoint_from_summary(
        self, summary: CurrentClaimAuthorityAuditSummary
    ) -> CurrentClaimCheckpoint:
        return CurrentClaimCheckpoint(
            schema_version=1,
            contract="rquant-current-claim-checkpoint/v1",
            authority_id=self._authority_id,
            operation_count=summary.operation_count,
            journal_root=summary.journal_root,
            state_root=summary.state_root,
        )

    def _read_materialized_anchor(
        self,
        connection: sqlite3.Connection,
    ) -> CurrentClaimMaterializedAnchor:
        rows = connection.execute("SELECT * FROM current_claim_authority_meta").fetchall()
        if len(rows) != 1:
            raise CurrentClaimAuthoritySecurityError("authority metadata is missing or duplicated")
        row = rows[0]
        if (
            int(row["singleton"]) != 1
            or int(row["schema_version"]) != _SCHEMA_VERSION
            or str(row["authority_id"]) != self._authority_id
            or str(row["mode"]) != self._mode
            or str(row["root_config_hash"]) != self._root_config_hash
        ):
            raise CurrentClaimAuthoritySecurityError("authority metadata was tampered")
        try:
            checkpoint = strict_model_validate_canonical_json(
                CurrentClaimCheckpoint,
                str(row["checkpoint_json"]),
            )
            anchor = CurrentClaimMaterializedAnchor(
                checkpoint=checkpoint,
                claim_accumulator=str(row["claim_accumulator"]),
                issue_accumulator=str(row["issue_accumulator"]),
                claim_count=int(row["claim_count"]),
                issue_count=int(row["issue_count"]),
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise CurrentClaimAuthoritySecurityError("authority checkpoint is malformed") from exc
        if (
            anchor.checkpoint.authority_id != self._authority_id
            or anchor.checkpoint.state_root
            != _materialized_state_root(
                claim_accumulator=anchor.claim_accumulator,
                issue_accumulator=anchor.issue_accumulator,
                claim_count=anchor.claim_count,
                issue_count=anchor.issue_count,
            )
        ):
            raise CurrentClaimAuthoritySecurityError("authority checkpoint was tampered")
        self._verify_journal_tail(connection, anchor.checkpoint)
        return anchor

    @staticmethod
    def _verify_journal_tail(
        connection: sqlite3.Connection,
        checkpoint: CurrentClaimCheckpoint,
    ) -> None:
        tail = connection.execute(
            "SELECT journal_index, journal_hash FROM current_claim_journal "
            "ORDER BY journal_index DESC LIMIT 1"
        ).fetchone()
        if checkpoint.operation_count == 0:
            if tail is not None or checkpoint.journal_root != _ZERO_HASH:
                raise CurrentClaimAuthoritySecurityError(
                    "authority journal tail diverges from checkpoint"
                )
            return
        if (
            tail is None
            or int(tail["journal_index"]) != checkpoint.operation_count
            or str(tail["journal_hash"]) != checkpoint.journal_root
        ):
            raise CurrentClaimAuthoritySecurityError(
                "authority journal tail diverges from checkpoint"
            )

    def _summary_from_anchor(
        self,
        anchor: CurrentClaimMaterializedAnchor,
    ) -> CurrentClaimAuthorityAuditSummary:
        return CurrentClaimAuthorityAuditSummary(
            authority_id=self._authority_id,
            operation_count=anchor.checkpoint.operation_count,
            claim_count=anchor.claim_count,
            issue_count=anchor.issue_count,
            journal_root=anchor.checkpoint.journal_root,
            state_root=anchor.checkpoint.state_root,
            checkpoint_hash=anchor.checkpoint.checkpoint_hash,
            mode=self._mode,
            non_production=self._mode != "production",
        )

    def _audit_state(self, connection: sqlite3.Connection) -> CurrentClaimAuthorityAuditSummary:
        self._validate_schema(connection)
        anchor = self._read_materialized_anchor(connection)
        claims = self._all_current_claims(connection)
        issues = self._all_issues(connection)
        previous_hash = _ZERO_HASH
        operations = connection.execute(
            "SELECT * FROM current_claim_journal ORDER BY journal_index"
        ).fetchall()
        for expected_index, row in enumerate(operations, start=1):
            kind = str(row["kind"])
            if kind not in {"publish", "issue"}:
                raise CurrentClaimAuthoritySecurityError("journal kind is invalid")
            calculated = _journal_hash(
                journal_index=expected_index,
                mutation_id=str(row["mutation_id"]),
                kind=kind,  # type: ignore[arg-type]
                payload_hash=str(row["payload_hash"]),
                previous_journal_hash=previous_hash,
            )
            if (
                int(row["journal_index"]) != expected_index
                or str(row["previous_journal_hash"]) != previous_hash
                or str(row["journal_hash"]) != calculated
            ):
                raise CurrentClaimAuthoritySecurityError("authority journal was tampered")
            previous_hash = calculated
        checkpoint = self._candidate_checkpoint(
            claims=claims,
            issues=issues,
            operation_count=len(operations),
            journal_root=previous_hash,
        )
        claim_accumulator = _xor_hashes(
            *(
                _claim_leaf_hash(claim_slot=slot, claim_hash=_claim_hash(claim))
                for slot, claim in claims.items()
            )
        )
        issue_accumulator = _xor_hashes(
            *(
                _issue_leaf_hash(
                    operation_id=operation_id,
                    issue_hash=issue_hash,
                    receipt_hash=receipt.receipt_hash,
                )
                for operation_id, (issue_hash, receipt) in issues.items()
            )
        )
        if (
            anchor.checkpoint != checkpoint
            or anchor.claim_accumulator != claim_accumulator
            or anchor.issue_accumulator != issue_accumulator
            or anchor.claim_count != len(claims)
            or anchor.issue_count != len(issues)
        ):
            raise CurrentClaimAuthoritySecurityError("authority checkpoint was tampered")
        return self._summary_from_anchor(anchor)

    def _coerce_root(
        self, value: CurrentClaimAntiRollbackRoot | None
    ) -> CurrentClaimAntiRollbackRoot | None:
        if value is None:
            return None
        try:
            root = CurrentClaimAntiRollbackRoot.model_validate(value, strict=True)
        except ValidationError as exc:
            raise CurrentClaimAuthoritySecurityError(
                "monotonic root response is malformed"
            ) from exc
        if (
            root.role != _ROOT_ROLE
            or root.root_authority_id != self._require_root().authority_id
            or root.root_store_id != self._require_root().store_id
            or root.current_claim_authority_id != self._authority_id
        ):
            raise CurrentClaimAuthoritySecurityError("monotonic root identity is invalid")
        return root

    def _require_root(self) -> CurrentClaimMonotonicRoot:
        if self._mode == "test-standalone" or self._root is None:
            raise CurrentClaimAuthoritySecurityError("production monotonic root is unavailable")
        return self._root

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA trusted_schema = OFF")
            yield connection
        finally:
            connection.close()

    def _initialize_or_open(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                objects = connection.execute(
                    "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                ).fetchall()
                if not objects:
                    self._create_schema(connection)
                self._audit_state(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        for sql in _TABLE_SQL.values():
            connection.execute(sql)
        connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
        checkpoint = self._candidate_checkpoint(
            claims={}, issues={}, operation_count=0, journal_root=_ZERO_HASH
        )
        inserted = connection.execute(
            "INSERT INTO current_claim_authority_meta("
            "singleton, schema_version, authority_id, mode, root_config_hash, checkpoint_json, "
            "claim_accumulator, issue_accumulator, claim_count, issue_count"
            ") VALUES (1, ?, ?, ?, ?, ?, ?, ?, 0, 0)",
            (
                _SCHEMA_VERSION,
                self._authority_id,
                self._mode,
                self._root_config_hash,
                _canonical_json(checkpoint),
                _ZERO_HASH,
                _ZERO_HASH,
            ),
        ).rowcount
        if inserted != 1:
            raise CurrentClaimAuthoritySecurityError("authority metadata initialization failed")

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID:
            raise CurrentClaimAuthoritySecurityError("authority application id is invalid")
        if connection.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION:
            raise CurrentClaimAuthoritySecurityError("authority schema version is invalid")
        objects = {
            (str(row["type"]), str(row["name"]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {("table", name) for name in _TABLE_SQL}
        if objects != expected:
            raise CurrentClaimAuthoritySecurityError(
                "authority schema has unknown or missing objects"
            )


def compose_production_current_claim_authority(
    path: Path,
    *,
    authority_id: str,
    signer: SourceUsePlanSigner,
    keyring: VerifyOnlyEd25519Keyring,
    external_root: ExternalCurrentClaimMonotonicRootAdapter,
    busy_timeout_ms: int = 5_000,
) -> PersistentCurrentClaimAuthority:
    """Build production authority only from the validated external witness adapter."""

    authority_path = _absolute_path(path)
    if (
        type(external_root) is not ExternalCurrentClaimMonotonicRootAdapter
        or not external_root.production_ready
    ):
        raise CurrentClaimAuthoritySecurityError(
            "production composition requires a validated external witness adapter"
        )
    return PersistentCurrentClaimAuthority(
        authority_path,
        authority_id=authority_id,
        signer=signer,
        keyring=keyring,
        monotonic_root=external_root,
        mode="production",
        busy_timeout_ms=busy_timeout_ms,
    )
