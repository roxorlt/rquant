"""Closed external monotonic-root binding for the source quota authority."""

from __future__ import annotations

import secrets
from typing import Final, Literal, Protocol, Self

from pydantic import Field, ValidationError, model_validator

from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
    ExternalMonotonicRootClient,
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootReceiptIdentity,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootSignatureVerifier,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
)
from rquant.external_monotonic_root_service import (
    ClosedExternalMonotonicRootVerifier,
    ExternalRootStoredState,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

SOURCE_QUOTA_ROOT_ROLE = "source_quota_monotonic_root"
SOURCE_QUOTA_ROOT_KEY_PURPOSE = "source-quota-monotonic-root"
SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE = "rquant-source-quota-anti-rollback-root/v1"
SOURCE_QUOTA_ROOT_CHECKPOINT_CONTRACT = "rquant-source-quota-external-checkpoint/v1"
_MAX_RECEIPT_BYTES: Final = 1024 * 1024


class SourceQuotaExternalRootSecurityError(RuntimeError):
    """The source-quota external root binding or response is untrusted."""


class SourceQuotaExternalCheckpoint(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-quota-external-checkpoint/v1"] = (
        SOURCE_QUOTA_ROOT_CHECKPOINT_CONTRACT
    )
    source_quota_authority_id: str = Field(min_length=1, max_length=200)
    binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    journal_count: int = Field(strict=True, ge=0)
    mutation_counter: int = Field(strict=True, ge=0)
    global_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    clock_high_water: str | None = None
    local_checkpoint_signature_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.mutation_counter != self.journal_count:
            raise ValueError("source quota checkpoint counters diverge")
        if self.journal_count == 0 and self.global_head_hash != EXTERNAL_MONOTONIC_ROOT_ZERO_HASH:
            raise ValueError("empty source quota checkpoint must use the zero root")
        if self.journal_count > 0 and self.global_head_hash == EXTERNAL_MONOTONIC_ROOT_ZERO_HASH:
            raise ValueError("nonempty source quota checkpoint cannot use the zero root")
        return self

    @property
    def checkpoint_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SourceQuotaExternalRootReceipt(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-quota-external-root-receipt/v1"] = (
        "rquant-source-quota-external-root-receipt/v1"
    )
    role: Literal["source_quota_monotonic_root"] = SOURCE_QUOTA_ROOT_ROLE
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    source_quota_authority_id: str = Field(min_length=1, max_length=200)
    request_kind: Literal["current", "pin", "advance"]
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: SourceQuotaExternalCheckpoint
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["source-quota-monotonic-root"] = SOURCE_QUOTA_ROOT_KEY_PURPOSE
    namespace: Literal["rquant-source-quota-anti-rollback-root/v1"] = (
        SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE
    )
    signature_algorithm: Literal["ed25519"] = "ed25519"
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1, max_length=16_384)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class SourceQuotaExternalRootConfig(ExternalMonotonicRootConfig):
    role: Literal["source_quota_monotonic_root"] = SOURCE_QUOTA_ROOT_ROLE
    root_key_purpose: Literal["source-quota-monotonic-root"] = SOURCE_QUOTA_ROOT_KEY_PURPOSE
    root_receipt_namespace: Literal["rquant-source-quota-anti-rollback-root/v1"] = (
        SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE
    )


class SourceQuotaExternalRootSigner(Protocol):
    issuer: str
    key_id: str
    key_purpose: Literal["source-quota-monotonic-root"]
    signature_algorithm: Literal["ed25519"]
    public_key_fingerprint: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


class SourceQuotaExternalRootRoleHandler:
    """Role response materializer used by the generic external-root daemon."""

    def __init__(self, signer: SourceQuotaExternalRootSigner) -> None:
        if (
            signer.key_purpose != SOURCE_QUOTA_ROOT_KEY_PURPOSE
            or signer.signature_algorithm != "ed25519"
        ):
            raise SourceQuotaExternalRootSecurityError("source quota root signer role is invalid")
        self._signer = signer

    def response_json(
        self,
        request: ExternalMonotonicRootRequest,
        state: ExternalRootStoredState | None,
    ) -> str | None:
        if state is None:
            return None
        if (
            request.role != SOURCE_QUOTA_ROOT_ROLE
            or state.role != request.role
            or state.root_authority_id != request.root_authority_id
            or state.root_store_id != request.root_store_id
            or state.subject_authority_id != request.subject_authority_id
        ):
            raise SourceQuotaExternalRootSecurityError("source quota root state identity diverges")
        try:
            checkpoint = strict_model_validate_canonical_json(
                SourceQuotaExternalCheckpoint,
                state.checkpoint_json,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external checkpoint is malformed"
            ) from exc
        if (
            checkpoint.checkpoint_hash != state.checkpoint_hash
            or checkpoint.source_quota_authority_id != request.subject_authority_id
        ):
            raise SourceQuotaExternalRootSecurityError(
                "source quota external checkpoint identity diverges"
            )
        unsigned = SourceQuotaExternalRootReceipt(
            root_authority_id=request.root_authority_id,
            root_store_id=request.root_store_id,
            source_quota_authority_id=request.subject_authority_id,
            request_kind=request.kind,
            request_hash=request.request_hash,
            challenge_nonce=request.challenge_nonce,
            operation_id=state.operation_id,
            previous_checkpoint_hash=state.previous_checkpoint_hash,
            checkpoint=checkpoint,
            issuer=self._signer.issuer,
            key_id=self._signer.key_id,
            public_key_fingerprint=self._signer.public_key_fingerprint,
            signature="pending",
        )
        signature = self._signer.sign(
            namespace=SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE,
            payload=unsigned.signing_bytes(),
        )
        return canonical_model_json_bytes(
            unsigned.model_copy(update={"signature": signature})
        ).decode("utf-8")


class SourceQuotaExternalMonotonicRootAdapter:
    """Closed source-quota client over the frozen external CAS protocol."""

    def __init__(
        self,
        *,
        config: SourceQuotaExternalRootConfig,
        client: UnixSocketExternalMonotonicRootClient,
        root_verifiers: tuple[ClosedExternalMonotonicRootVerifier, ...],
    ) -> None:
        if type(config) is not SourceQuotaExternalRootConfig:
            raise SourceQuotaExternalRootSecurityError(
                "production source quota root requires the closed identity binding"
            )
        if type(client) is not UnixSocketExternalMonotonicRootClient:
            raise SourceQuotaExternalRootSecurityError(
                "production source quota root requires the closed Unix peer client"
            )
        if (
            type(root_verifiers) is not tuple
            or len(root_verifiers) != 1
            or type(root_verifiers[0]) is not ClosedExternalMonotonicRootVerifier
        ):
            raise SourceQuotaExternalRootSecurityError(
                "production source quota root requires the closed verifier"
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
        config: SourceQuotaExternalRootConfig,
        client: ExternalMonotonicRootClient,
        root_verifiers: tuple[ExternalMonotonicRootSignatureVerifier, ...],
    ) -> SourceQuotaExternalMonotonicRootAdapter:
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
        config: SourceQuotaExternalRootConfig,
        client: ExternalMonotonicRootClient,
        root_verifiers: tuple[ExternalMonotonicRootSignatureVerifier, ...],
        production_ready: bool,
    ) -> None:
        try:
            validated = SourceQuotaExternalRootConfig.model_validate(config, strict=True)
            trust = ExternalMonotonicRootTrustBoundary(
                config=validated,
                client=client,
                root_verifiers=root_verifiers,
            )
        except (ValidationError, ExternalMonotonicRootSecurityError, AttributeError) as exc:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root configuration is invalid"
            ) from exc
        self._config = validated
        self._trust = trust
        self._production_ready = production_ready

    @property
    def config(self) -> SourceQuotaExternalRootConfig:
        return self._config

    @property
    def production_ready(self) -> bool:
        return self._production_ready

    def current(
        self,
        *,
        source_quota_authority_id: str,
        challenge_nonce: str | None = None,
    ) -> SourceQuotaExternalRootReceipt | None:
        request = ExternalMonotonicRootRequest.close(
            kind="current",
            role=self._config.role,
            root_authority_id=self._config.root_authority_id,
            root_store_id=self._config.root_store_id,
            subject_authority_id=source_quota_authority_id,
            challenge_nonce=challenge_nonce or secrets.token_hex(32),
        )
        response = self._invoke(request)
        return None if response is None else self._require_verified(response, request=request)

    def build_mutation_request(
        self,
        *,
        kind: Literal["pin", "advance"],
        operation_id: str,
        source_quota_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: SourceQuotaExternalCheckpoint,
        challenge_nonce: str | None = None,
    ) -> ExternalMonotonicRootRequest:
        return ExternalMonotonicRootRequest.close(
            kind=kind,
            role=self._config.role,
            root_authority_id=self._config.root_authority_id,
            root_store_id=self._config.root_store_id,
            subject_authority_id=source_quota_authority_id,
            challenge_nonce=challenge_nonce or secrets.token_hex(32),
            operation_id=operation_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint_contract=checkpoint.contract,
            checkpoint_hash=checkpoint.checkpoint_hash,
            checkpoint_json=canonical_model_json_bytes(checkpoint).decode("utf-8"),
        )

    def invoke_mutation(
        self,
        request: ExternalMonotonicRootRequest,
    ) -> SourceQuotaExternalRootReceipt:
        if request.kind not in {"pin", "advance"}:
            raise SourceQuotaExternalRootSecurityError(
                "source quota root mutation request kind is invalid"
            )
        response = self._invoke(request)
        if response is None:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root mutation returned no receipt"
            )
        return self._require_verified(response, request=request)

    def verify_stored_receipt(
        self,
        receipt_json: str,
        *,
        source_quota_authority_id: str,
    ) -> SourceQuotaExternalRootReceipt:
        receipt = self._decode_and_verify_receipt(receipt_json)
        if receipt.source_quota_authority_id != source_quota_authority_id:
            raise SourceQuotaExternalRootSecurityError(
                "stored source quota external receipt authority diverges"
            )
        return receipt

    def _invoke(self, request: ExternalMonotonicRootRequest) -> str | None:
        try:
            response = self._trust.invoke(request)
        except Exception as exc:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root invocation failed closed"
            ) from exc
        if response is not None and (
            type(response) is not str or len(response.encode("utf-8")) > _MAX_RECEIPT_BYTES
        ):
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root response is invalid or oversized"
            )
        return response

    def _require_verified(
        self,
        receipt_json: str,
        *,
        request: ExternalMonotonicRootRequest,
    ) -> SourceQuotaExternalRootReceipt:
        receipt = self._decode_and_verify_receipt(receipt_json)
        if (
            receipt.request_kind != request.kind
            or receipt.request_hash != request.request_hash
            or receipt.challenge_nonce != request.challenge_nonce
            or receipt.source_quota_authority_id != request.subject_authority_id
        ):
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root receipt does not bind the request"
            )
        if request.kind != "current" and (
            receipt.operation_id != request.operation_id
            or receipt.previous_checkpoint_hash != request.previous_checkpoint_hash
            or receipt.checkpoint.checkpoint_hash != request.checkpoint_hash
        ):
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root mutation receipt diverges"
            )
        if receipt.checkpoint.source_quota_authority_id != request.subject_authority_id:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external checkpoint authority diverges"
            )
        return receipt

    def _decode_and_verify_receipt(
        self,
        receipt_json: str,
    ) -> SourceQuotaExternalRootReceipt:
        try:
            receipt = strict_model_validate_canonical_json(
                SourceQuotaExternalRootReceipt,
                receipt_json,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root receipt is malformed"
            ) from exc
        try:
            self._trust.verify_receipt(
                identity=ExternalMonotonicRootReceiptIdentity(
                    role=receipt.role,
                    root_authority_id=receipt.root_authority_id,
                    root_store_id=receipt.root_store_id,
                    closed=receipt.closed,
                    issuer=receipt.issuer,
                    key_id=receipt.key_id,
                    key_purpose=receipt.key_purpose,
                    namespace=receipt.namespace,
                    signature_algorithm=receipt.signature_algorithm,
                    public_key_fingerprint=receipt.public_key_fingerprint,
                ),
                signing_bytes=receipt.signing_bytes(),
                signature=receipt.signature,
            )
        except ExternalMonotonicRootSecurityError as exc:
            raise SourceQuotaExternalRootSecurityError(
                "source quota external root receipt verification failed"
            ) from exc
        return receipt


__all__ = [
    "SOURCE_QUOTA_ROOT_CHECKPOINT_CONTRACT",
    "SOURCE_QUOTA_ROOT_KEY_PURPOSE",
    "SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE",
    "SOURCE_QUOTA_ROOT_ROLE",
    "SourceQuotaExternalCheckpoint",
    "SourceQuotaExternalMonotonicRootAdapter",
    "SourceQuotaExternalRootConfig",
    "SourceQuotaExternalRootReceipt",
    "SourceQuotaExternalRootRoleHandler",
    "SourceQuotaExternalRootSecurityError",
]
