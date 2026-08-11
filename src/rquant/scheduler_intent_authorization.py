"""Standalone signed authorization contract for scheduler-owned source intents."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import ConfigDict, Field, model_validator

from rquant.adapter_manifest import (
    SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE,
    VerifyOnlyEd25519Keyring,
    _Ed25519SignedContract,
)
from rquant.runtime_contracts import AwareUtcDatetime, normalize_aware_utc

_HASH_PATTERN = r"^[0-9a-f]{64}$"


class SchedulerIntentAuthorizationError(ValueError):
    """A scheduler intent authorization is invalid, expired, or untrusted."""


class SchedulerIntentAuthorizationV1(_Ed25519SignedContract):
    """Signed authority for one exact scheduler-to-broker source intent payload."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
        str_strip_whitespace=False,
    )

    schema_version: Literal[1] = 1
    key_purpose: Literal["scheduler_intent_authorization"] = "scheduler_intent_authorization"
    authority_id: str = Field(min_length=1, max_length=200)
    payload_commitment: str = Field(pattern=_HASH_PATTERN)
    template_commitment: str = Field(pattern=_HASH_PATTERN)
    public_request_commitment: str = Field(pattern=_HASH_PATTERN)
    manifest_commitment: str = Field(pattern=_HASH_PATTERN)
    source_contract_commitment: str = Field(pattern=_HASH_PATTERN)
    resource_quota_commitment: str = Field(pattern=_HASH_PATTERN)
    lineage_authority_commitment: str = Field(pattern=_HASH_PATTERN)
    valid_from: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_authorization(self) -> Self:
        if self.issuer != self.authority_id:
            raise ValueError("scheduler intent authorization issuer conflicts with authority_id")
        if self.expires_at <= self.valid_from:
            raise ValueError("scheduler intent authorization expires_at must follow valid_from")
        return self

    def signing_preimage(self) -> bytes:
        return self.signing_bytes()

    def verify(self, keyring: VerifyOnlyEd25519Keyring) -> bool:
        return self._verify(
            keyring,
            purpose="scheduler_intent_authorization",
            namespace=SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE,
        )

    def require_verified(
        self,
        keyring: VerifyOnlyEd25519Keyring,
        *,
        now: datetime,
    ) -> None:
        current = normalize_aware_utc(now)
        if not self.valid_from <= current < self.expires_at:
            raise SchedulerIntentAuthorizationError(
                "scheduler intent authorization is not currently valid"
            )
        if not self.verify(keyring):
            raise SchedulerIntentAuthorizationError(
                "scheduler intent authorization signature is invalid"
            )
