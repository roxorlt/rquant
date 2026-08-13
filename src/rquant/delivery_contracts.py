"""Immutable routing and notification outbox contracts."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class DeliveryChannel(StrEnum):
    PUSHDEER = "pushdeer"
    PUSHPLUS = "pushplus"


class RouterDisposition(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    QUARANTINED = "quarantined"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    RETRY = "retry"
    SUCCEEDED = "succeeded"
    EXPIRED = "expired"
    DEAD_LETTER = "dead_letter"


_TERMINAL_STATUSES = frozenset(
    {OutboxStatus.SUCCEEDED, OutboxStatus.EXPIRED, OutboxStatus.DEAD_LETTER}
)


class DeliveryTarget(RuntimeContractModel):
    recipient_id: str = Field(min_length=1)
    channel: DeliveryChannel

    def delivery_key(self, signal_id: str) -> str:
        if _SHA256_PATTERN.fullmatch(signal_id) is None:
            raise ValueError("signal_id must be a lowercase SHA-256 digest")
        return canonical_sha256(
            {
                "contract": "delivery-target/v1",
                "signal_id": signal_id,
                "recipient_id": self.recipient_id,
                "channel": self.channel,
            }
        )


class RouterReceipt(RuntimeContractModel):
    signal_id: Sha256
    disposition: RouterDisposition
    global_sequence: int | None = Field(default=None, ge=1)
    reason: str | None = Field(default=None, min_length=1)
    received_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_disposition(self) -> Self:
        if self.disposition in {
            RouterDisposition.ACCEPTED,
            RouterDisposition.DUPLICATE,
        }:
            if self.global_sequence is None:
                raise ValueError("accepted and duplicate receipts require global_sequence")
        else:
            if self.global_sequence is not None:
                raise ValueError("quarantined receipts cannot have global_sequence")
            if self.reason is None:
                raise ValueError("quarantined receipts require reason")
        return self


class OutboxRecord(RuntimeContractModel):
    outbox_id: Sha256 | None = None
    signal_id: Sha256
    target: DeliveryTarget
    status: OutboxStatus
    expires_at: AwareUtcDatetime
    attempt_count: int = Field(ge=0)
    next_attempt_at: AwareUtcDatetime | None = None
    lease_owner: str | None = Field(default=None, min_length=1)
    lease_until: AwareUtcDatetime | None = None
    last_error: str | None = Field(default=None, min_length=1)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        if self.created_at >= self.expires_at:
            raise ValueError("expires_at must be after created_at")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be at or after created_at")
        if self.status not in _TERMINAL_STATUSES and self.updated_at >= self.expires_at:
            raise ValueError("active outbox updated_at must be before expires_at")
        if self.next_attempt_at is not None:
            if self.next_attempt_at < self.updated_at:
                raise ValueError("next_attempt_at must be at or after updated_at")
            if self.next_attempt_at >= self.expires_at:
                raise ValueError("next_attempt_at must be before expires_at")

        has_lease_owner = self.lease_owner is not None
        has_lease_until = self.lease_until is not None
        if has_lease_owner != has_lease_until:
            raise ValueError("lease_owner and lease_until must be provided together")
        if self.status is OutboxStatus.LEASED:
            if not has_lease_owner:
                raise ValueError("leased records require lease_owner and lease_until")
            if self.lease_until is not None:
                if self.lease_until <= self.updated_at:
                    raise ValueError("lease_until must be after updated_at")
                if self.lease_until > self.expires_at:
                    raise ValueError("lease_until cannot be after expires_at")
        elif has_lease_owner:
            raise ValueError("lease fields are only valid while status is leased")

        if self.status in _TERMINAL_STATUSES and (
            self.next_attempt_at is not None or has_lease_owner
        ):
            raise ValueError("terminal records cannot have a next attempt or lease")
        if (
            self.status in {OutboxStatus.EXPIRED, OutboxStatus.DEAD_LETTER}
            and self.last_error is None
        ):
            raise ValueError("expired and dead-letter records require last_error")
        if self.status is OutboxStatus.RETRY:
            if self.next_attempt_at is None:
                raise ValueError("retry records require next_attempt_at")
            if self.last_error is None:
                raise ValueError("retry records require last_error")

        expected_outbox_id = self.target.delivery_key(self.signal_id)
        if self.outbox_id is None:
            object.__setattr__(self, "outbox_id", expected_outbox_id)
        elif self.outbox_id != expected_outbox_id:
            raise ValueError("outbox_id does not match signal and delivery target")
        return self


class OutboxAttempt(RuntimeContractModel):
    outbox_id: Sha256
    attempt_no: int = Field(ge=1)
    started_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    success: bool
    provider_receipt: str | None = Field(default=None, min_length=1)
    error: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_attempt(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must be at or after started_at")
        if self.success:
            if self.provider_receipt is None or self.error is not None:
                raise ValueError("successful attempts require provider_receipt and forbid error")
        elif self.error is None or self.provider_receipt is not None:
            raise ValueError("failed attempts require error and forbid provider_receipt")
        return self
