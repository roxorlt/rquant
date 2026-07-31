"""Atomic bridge from immutable strategy signal spools to the signal bus."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.delivery_contracts import DeliveryTarget
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.signal_bus import (
    RouteDecisionKind,
    RouteReceiptDisposition,
    RouteSourceDescriptor,
    SignalBusStore,
    SignalRouteConflictError,
    SignalRouteCursor,
    SignalRouteSequenceError,
    canonical_delivery_targets,
    routing_decision_fingerprint,
)
from rquant.signal_contracts import SignalEnvelope
from rquant.strategy_runner import RunnerSignalRecord, StrategyRunnerStore

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class RoutingConfigurationUnavailableError(RuntimeError):
    """A transient resolver dependency failed, so the cursor must not advance."""


class RoutingDecisionAction(StrEnum):
    ROUTE = "route"
    NO_TARGET = "no_target"


class RoutingDecision(RuntimeContractModel):
    routing_policy_fingerprint: Sha256
    action: RoutingDecisionAction
    targets: tuple[DeliveryTarget, ...] = ()
    reason_code: str | None = Field(default=None, min_length=1)

    @field_validator("targets")
    @classmethod
    def canonicalize_targets(
        cls,
        targets: tuple[DeliveryTarget, ...],
    ) -> tuple[DeliveryTarget, ...]:
        return canonical_delivery_targets(targets)

    @model_validator(mode="after")
    def validate_action(self) -> Self:
        if self.action is RoutingDecisionAction.ROUTE:
            if not self.targets or self.reason_code is not None:
                raise ValueError("ROUTE requires targets and forbids reason_code")
        elif self.targets or self.reason_code is None:
            raise ValueError("NO_TARGET requires reason_code and forbids targets")
        return self

    @property
    def fingerprint(self) -> str:
        return routing_decision_fingerprint(
            routing_policy_fingerprint=self.routing_policy_fingerprint,
            decision_kind=RouteDecisionKind(self.action.value),
            targets=self.targets,
            reason_code=self.reason_code,
        )

    @classmethod
    def route(
        cls,
        *,
        routing_policy_fingerprint: str,
        targets: tuple[DeliveryTarget, ...],
    ) -> RoutingDecision:
        return cls(
            routing_policy_fingerprint=routing_policy_fingerprint,
            action=RoutingDecisionAction.ROUTE,
            targets=targets,
        )

    @classmethod
    def no_target(
        cls,
        *,
        routing_policy_fingerprint: str,
        reason_code: str,
    ) -> RoutingDecision:
        return cls(
            routing_policy_fingerprint=routing_policy_fingerprint,
            action=RoutingDecisionAction.NO_TARGET,
            reason_code=reason_code,
        )


class RunnerSignalSource(Protocol):
    def descriptor(self) -> RouteSourceDescriptor: ...

    def signals_after(self, *, sequence: int) -> tuple[RunnerSignalRecord, ...]: ...


class StrategyRunnerSignalSource:
    """Expose one durable strategy runner spool through the routing source contract."""

    def __init__(self, *, source_id: str, store: StrategyRunnerStore) -> None:
        normalized = source_id.strip()
        if not normalized:
            raise ValueError("source_id must not be empty")
        self.source_id = normalized
        self.store = store

    def descriptor(self) -> RouteSourceDescriptor:
        return RouteSourceDescriptor(
            source_id=self.source_id,
            generation_id=self.store.source_generation_id,
            strategy_spec_fingerprint=self.store.spec.spec_fingerprint,
            first_sequence=1,
            high_watermark=self.store.signal_high_watermark(),
        )

    def signals_after(self, *, sequence: int) -> tuple[RunnerSignalRecord, ...]:
        return self.store.signals_after(sequence=sequence)


TargetResolver = Callable[[SignalEnvelope], RoutingDecision]


class SignalRouteSummary(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    started_after_sequence: int = Field(ge=0)
    last_sequence: int = Field(ge=0)
    routed_count: int = Field(ge=0)
    target_count: int = Field(ge=0)
    duplicate_count: int = Field(ge=0)
    no_target_count: int = Field(ge=0)
    expired_count: int = Field(ge=0)
    deferred_count: int = Field(ge=0)
    routed_at: AwareUtcDatetime


class _CursorStoreConfig(RuntimeContractModel):
    routing_policy_fingerprint: Sha256
    busy_timeout_ms: int = Field(default=5_000, strict=True, ge=1)


class _RouteRunRequest(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    routed_at: AwareUtcDatetime
    limit: int = Field(strict=True, ge=1)


class SignalRouteCursorStore:
    """Compatibility facade; route progress is authoritative in SignalBusStore."""

    def __init__(
        self,
        path: Path,
        *,
        routing_policy_fingerprint: str,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        config = _CursorStoreConfig(
            routing_policy_fingerprint=routing_policy_fingerprint,
            busy_timeout_ms=busy_timeout_ms,
        )
        self.path = Path(path)
        self.routing_policy_fingerprint = config.routing_policy_fingerprint
        self.busy_timeout_ms = config.busy_timeout_ms
        self._bus: SignalBusStore | None = None

    def bind(self, bus: SignalBusStore) -> None:
        if self._bus is not None and self._bus.path.resolve() != bus.path.resolve():
            raise SignalRouteConflictError(
                "cursor facade cannot be rebound to another signal bus generation"
            )
        self._bus = bus

    def cursor(self, source_id: str) -> SignalRouteCursor:
        if self._bus is None:
            normalized = source_id.strip()
            if not normalized:
                raise ValueError("source_id must not be empty")
            return SignalRouteCursor(source_id=normalized, last_sequence=0)
        return self._bus.route_cursor(source_id)


def route_runner_signals(
    *,
    source_id: str,
    source: RunnerSignalSource,
    bus: SignalBusStore,
    cursors: SignalRouteCursorStore,
    routed_at: datetime,
    target_resolver: TargetResolver,
    limit: int,
) -> SignalRouteSummary:
    """Route a bounded prefix with source receipt and outbox in one transaction."""

    request = _RouteRunRequest(
        source_id=source_id,
        routed_at=routed_at,
        limit=limit,
    )
    descriptor = RouteSourceDescriptor.model_validate(source.descriptor())
    if descriptor.source_id != request.source_id:
        raise SignalRouteConflictError(
            "requested source_id does not match the frozen source descriptor"
        )
    cursors.bind(bus)
    cursor = bus.bind_route_source(
        descriptor,
        routing_policy_fingerprint=cursors.routing_policy_fingerprint,
        observed_at=request.routed_at,
    )
    started_after = cursor.last_sequence
    raw_records = source.signals_after(sequence=started_after)
    records = tuple(
        record
        if isinstance(record, RunnerSignalRecord)
        else RunnerSignalRecord.model_validate(record)
        for record in raw_records
    )
    expected = started_after + 1
    for record in records:
        if record.sequence != expected:
            raise SignalRouteSequenceError(
                f"expected runner sequence {expected}, got {record.sequence}"
            )
        expected += 1
    if records and records[-1].sequence > descriptor.high_watermark:
        raise SignalRouteSequenceError("source returned records above its declared high watermark")
    if descriptor.high_watermark > started_after and (
        not records or records[-1].sequence < descriptor.high_watermark
    ):
        raise SignalRouteSequenceError("source tail is missing below the declared high watermark")

    routed_count = 0
    target_count = 0
    duplicate_count = 0
    no_target_count = 0
    expired_count = 0
    deferred_count = 0

    for record in records[: request.limit]:
        signal = record.signal
        if signal.available_at > request.routed_at:
            deferred_count = 1
            break
        decision = RoutingDecision.model_validate(target_resolver(signal))
        if decision.routing_policy_fingerprint != cursors.routing_policy_fingerprint:
            raise SignalRouteConflictError(
                "routing decision does not belong to the frozen routing policy"
            )
        committed = bus.commit_source_route(
            descriptor=descriptor,
            routing_policy_fingerprint=cursors.routing_policy_fingerprint,
            source_sequence=record.sequence,
            signal=signal,
            decision_kind=RouteDecisionKind(decision.action.value),
            decision_fingerprint=decision.fingerprint,
            reason_code=decision.reason_code,
            targets=decision.targets,
            routed_at=request.routed_at,
        )
        cursor = bus.route_cursor(request.source_id)
        if committed.duplicate:
            duplicate_count += 1
            continue
        target_count += committed.receipt.target_count
        if committed.receipt.disposition is RouteReceiptDisposition.ROUTED:
            routed_count += 1
        elif committed.receipt.disposition is RouteReceiptDisposition.NO_TARGET:
            no_target_count += 1
        else:
            expired_count += 1

    return SignalRouteSummary(
        source_id=request.source_id,
        started_after_sequence=started_after,
        last_sequence=cursor.last_sequence,
        routed_count=routed_count,
        target_count=target_count,
        duplicate_count=duplicate_count,
        no_target_count=no_target_count,
        expired_count=expired_count,
        deferred_count=deferred_count,
        routed_at=request.routed_at,
    )


__all__ = [
    "RouteSourceDescriptor",
    "RoutingConfigurationUnavailableError",
    "RoutingDecision",
    "RoutingDecisionAction",
    "RunnerSignalSource",
    "SignalRouteConflictError",
    "SignalRouteCursor",
    "SignalRouteCursorStore",
    "SignalRouteSequenceError",
    "SignalRouteSummary",
    "StrategyRunnerSignalSource",
    "TargetResolver",
    "route_runner_signals",
]
