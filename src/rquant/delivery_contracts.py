"""Immutable routing and notification outbox contracts."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import Field, StrictInt, StringConstraints, model_validator

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


class PhysicalPostMember(RuntimeContractModel):
    outbox_id: Sha256
    attempt_no: StrictInt = Field(ge=1, le=5)


class PhysicalPostBinding(RuntimeContractModel):
    group_id: Sha256
    owner_id: str = Field(min_length=1, max_length=128)
    target: DeliveryTarget
    members: tuple[PhysicalPostMember, ...] = Field(min_length=1, max_length=100)
    request_sha256: Sha256
    request_utf8_bytes: StrictInt = Field(ge=1, le=64 * 1024)
    issued_at: AwareUtcDatetime

    @model_validator(mode="after")
    def unique_members(self) -> Self:
        if len(self.target.recipient_id) > 128:
            raise ValueError("physical recipient exceeds the bounded identity")
        if len({item.outbox_id for item in self.members}) != len(self.members):
            raise ValueError("physical request repeats a logical member")
        return self

    def physical_id(self, key_slot: int = 0) -> str:
        return canonical_sha256({"contract": "notification-physical-post/v1",
                                 "binding": self, "key_slot": key_slot})


class PhysicalPostObservation(RuntimeContractModel):
    binding: PhysicalPostBinding
    key_slot: StrictInt = Field(ge=0, le=99)
    called_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    disposition: Literal["accepted", "rejected", "unknown"]
    reason: Literal["channel_accepted", "channel_rejected", "post_exception", "invalid_reply"]

    @model_validator(mode="after")
    def call_order(self) -> Self:
        if self.called_at < self.binding.issued_at or self.completed_at < self.called_at:
            raise ValueError("physical call precedes its original intent or completion")
        expected = {"accepted": "channel_accepted", "rejected": "channel_rejected"}
        if self.disposition != "unknown" and self.reason != expected[self.disposition]:
            raise ValueError("physical disposition does not match the actual reply")
        if self.disposition == "unknown" and self.reason not in {"post_exception", "invalid_reply"}:
            raise ValueError("unknown physical call requires a missing definitive reply")
        return self


class NotificationMergeBinding(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    source_id: str = Field(min_length=1, max_length=128)
    installation_sha256: Sha256
    role_revision: Sha256
    generation_id: str = Field(min_length=1, max_length=128)
    mode: Literal["shadow", "live"]


class NotificationMergeGroup(RuntimeContractModel):
    group_id: Sha256
    binding: NotificationMergeBinding
    cohort_sha256: Sha256
    target: DeliveryTarget
    family: Literal["signal", "price", "condition", "builtin"]
    opened_at: AwareUtcDatetime
    due_at: AwareUtcDatetime
    status: Literal["waiting", "intent", "succeeded", "failed", "unknown", "shadow"]
    members: tuple[Sha256, ...] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def fixed_window(self) -> Self:
        from datetime import timedelta

        if self.due_at - self.opened_at != timedelta(seconds=30):
            raise ValueError("notification merge window must be exactly thirty seconds")
        if len(set(self.members)) != len(self.members):
            raise ValueError("notification group repeats a logical member")
        if len(self.target.recipient_id) > 128:
            raise ValueError("merge recipient exceeds the bounded identity")
        return self


class NotificationChannelStatistics(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    target: DeliveryTarget
    mode: Literal["shadow", "live"]
    logical_count: StrictInt = Field(ge=0)
    member_attempts: StrictInt = Field(ge=0)
    member_retries: StrictInt = Field(ge=0)
    physical_requests: StrictInt = Field(ge=0)
    accepted_count: StrictInt = Field(ge=0)
    rejected_count: StrictInt = Field(ge=0)
    unknown_count: StrictInt = Field(ge=0)
    possible_requests: StrictInt = Field(ge=0)
    last_accepted_at: AwareUtcDatetime | None = None
    binding_sha256: Sha256 | None = None

    @model_validator(mode="after")
    def observed_counts(self) -> Self:
        if self.physical_requests != self.accepted_count + self.rejected_count + self.unknown_count:
            raise ValueError("physical request counts must equal actual POST observations")
        if self.mode == "shadow" and (self.physical_requests or self.possible_requests):
            raise ValueError("shadow notification has no physical request")
        if (self.last_accepted_at is not None) != (self.accepted_count > 0):
            raise ValueError("last accepted time requires an actual channel accepted reply")
        return self


class NotificationRuntimeWindow(RuntimeContractModel):
    protocol: Literal["rquant.notification-runtime-window/v1"] = "rquant.notification-runtime-window/v1"
    state: Literal["ready", "unavailable"]
    reason: str = Field(min_length=1, max_length=80)
    observed_at: AwareUtcDatetime
    binding: NotificationMergeBinding | None
    source_receipt_sha256: Sha256 | None
    covered_from: AwareUtcDatetime | None
    covered_through: AwareUtcDatetime
    complete: bool
    history_count: StrictInt = Field(ge=0, le=1024)
    returned_history_count: StrictInt = Field(ge=0, le=512)
    truncated: bool
    applied_revision: StrictInt | None = Field(default=None, ge=0)
    applied_command_id: str | None = Field(default=None, max_length=128)
    monitor_installation_sha256: Sha256 | None = None
    capability_observed_at: AwareUtcDatetime | None = None
    available_targets: tuple[DeliveryTarget, ...] | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def actual_coverage(self) -> Self:
        if self.covered_through != self.observed_at or self.covered_from is not None and self.covered_from > self.observed_at:
            raise ValueError("notification statistics have a future or different owner cutoff")
        if self.complete != (self.state == "ready") or self.complete and (self.binding is None or self.source_receipt_sha256 is None or self.covered_from is None):
            raise ValueError("complete notification statistics need their original observation window")
        if self.returned_history_count > self.history_count or self.truncated != (self.returned_history_count < self.history_count):
            raise ValueError("notification history coverage differs from the retained original groups")
        if self.capability_observed_at is not None and self.capability_observed_at > self.observed_at:
            raise ValueError("notification capability was not yet observed by its original owner")
        if (self.available_targets is None) != (self.capability_observed_at is None):
            raise ValueError("notification capability needs the actual original loader observation")
        return self


class NotificationRuntimeChannelState(RuntimeContractModel):
    protocol: Literal["rquant.notification-runtime-channel/v1"] = "rquant.notification-runtime-channel/v1"
    owner_id: str = Field(min_length=1, max_length=128)
    channel: DeliveryChannel
    recipient_scope_ref: Sha256
    source_receipt_sha256: Sha256
    mode: Literal["shadow", "live"]
    observed_at: AwareUtcDatetime
    covered_from: AwareUtcDatetime
    covered_through: AwareUtcDatetime
    complete: Literal[True] = True
    targets: tuple[DeliveryTarget, ...] = Field(min_length=1, max_length=64)
    statistics: tuple[NotificationChannelStatistics, ...] = Field(min_length=1, max_length=1024)
    logical_count: StrictInt = Field(ge=0)
    member_attempts: StrictInt = Field(ge=0)
    member_retries: StrictInt = Field(ge=0)
    physical_requests: StrictInt = Field(ge=0)
    accepted_count: StrictInt = Field(ge=0)
    rejected_count: StrictInt = Field(ge=0)
    physical_unknown_count: StrictInt = Field(ge=0)
    possible_requests: StrictInt = Field(ge=0)
    last_accepted_at: AwareUtcDatetime | None
    applied_revision: StrictInt | None = Field(default=None, ge=0)
    accepted_pct: float | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def original_totals(self) -> Self:
        if self.covered_from > self.covered_through or self.covered_through != self.observed_at:
            raise ValueError("notification channel statistics changed their original window")
        if any(row.owner_id != self.owner_id or row.target.channel != self.channel for row in self.statistics):
            raise ValueError("notification channel statistics mix owners or channels")
        targets = tuple(sorted({row.target for row in self.statistics}, key=lambda row: row.recipient_id))
        if targets != self.targets or canonical_sha256(targets) != self.recipient_scope_ref:
            raise ValueError("notification channel scope omits a real receiver")
        for field in ("logical_count", "member_attempts", "member_retries", "physical_requests", "accepted_count", "rejected_count", "possible_requests"):
            if getattr(self, field) != sum(getattr(row, field) for row in self.statistics):
                raise ValueError("notification totals differ from the exact owner records")
        if self.physical_unknown_count != sum(row.unknown_count for row in self.statistics) or self.last_accepted_at != max((row.last_accepted_at for row in self.statistics if row.last_accepted_at is not None), default=None):
            raise ValueError("notification actual POST results differ from the original observations")
        if self.accepted_pct is not None and (not self.physical_requests or self.physical_unknown_count
                or self.possible_requests or self.accepted_pct != round(100 * self.accepted_count / self.physical_requests, 1)):
            raise ValueError("notification acceptance rate needs definitive original POST results")
        return self


class NotificationRuntimeAttemptView(RuntimeContractModel):
    intent: PhysicalPostBinding
    observation: PhysicalPostObservation | None

    @model_validator(mode="after")
    def exact_attempt(self) -> Self:
        if self.observation is not None and self.observation.binding != self.intent:
            raise ValueError("notification timeline changed its original physical attempt")
        return self


class NotificationRuntimeGroupView(RuntimeContractModel):
    protocol: Literal["rquant.notification-runtime-group/v1"] = "rquant.notification-runtime-group/v1"
    group: NotificationMergeGroup
    inspected_at: AwareUtcDatetime
    source_receipt_sha256: Sha256
    attempts: tuple[NotificationRuntimeAttemptView, ...] = Field(max_length=5)

    @model_validator(mode="after")
    def actual_group(self) -> Self:
        if self.group.opened_at > self.inspected_at:
            raise ValueError("notification history contains a future original group")
        if any(row.intent.group_id != self.group.group_id or row.intent.owner_id != self.group.binding.owner_id
                or row.intent.target != self.group.target or row.intent.issued_at > self.inspected_at
                or row.observation is not None and row.observation.completed_at > self.inspected_at
                or not {member.outbox_id for member in row.intent.members} <= set(self.group.members)
                for row in self.attempts):
            raise ValueError("notification timeline omits or mixes original member attempts")
        return self
