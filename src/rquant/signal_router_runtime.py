"""Atomic bridge from immutable strategy signal spools to the signal bus."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol, Self
from urllib.parse import quote

from pydantic import Field, StrictInt, StringConstraints, field_validator, model_validator

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


class SourceSnapshot(RuntimeContractModel):
    descriptor: RouteSourceDescriptor


class RunnerSignalBatch(RuntimeContractModel):
    snapshot: SourceSnapshot
    after_sequence: StrictInt = Field(ge=0)
    limit: StrictInt = Field(ge=0)
    records: tuple[RunnerSignalRecord, ...]

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if len(self.records) > self.limit:
            raise ValueError("runner signal batch exceeds its requested limit")
        return self


class RunnerSignalSource(Protocol):
    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch: ...


class StrategyRunnerSignalSource:
    """Expose one durable strategy runner spool through the routing source contract."""

    def __init__(self, *, source_id: str, store: StrategyRunnerStore) -> None:
        normalized = source_id.strip()
        if not normalized:
            raise ValueError("source_id must not be empty")
        self.source_id = normalized
        self.store = store

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        _validate_batch_request(after_sequence=after_sequence, limit=limit)
        connect = getattr(self.store, "_connect", None)
        if not callable(connect):
            raise TypeError("strategy runner store does not expose a transactional connection")
        try:
            with connect() as connection:
                connection.execute("BEGIN")
                identity = _query_source_snapshot(connection)
                records = _query_signal_records(
                    connection,
                    after_sequence=after_sequence,
                    high_watermark=identity.high_watermark,
                    limit=limit,
                )
        except sqlite3.Error as exc:
            raise ValueError("runner signals are unavailable") from exc
        descriptor = RouteSourceDescriptor(
            source_id=self.source_id,
            generation_id=identity.generation_id,
            strategy_spec_fingerprint=identity.strategy_spec_fingerprint,
            first_sequence=1,
            high_watermark=identity.high_watermark,
        )
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(descriptor=descriptor),
            after_sequence=after_sequence,
            limit=limit,
            records=records,
        )


class ReadonlyStrategyRunnerSignalSource:
    """Read one live runner spool through SQLite's read-only URI contract."""

    def __init__(
        self,
        *,
        source_id: str,
        path: Path,
        expected_strategy_spec_fingerprint: str,
        expected_evaluator_contract_fingerprint: str,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        normalized_source_id = source_id.strip()
        if not normalized_source_id:
            raise ValueError("source_id must not be empty")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        for label, value in (
            ("strategy spec", expected_strategy_spec_fingerprint),
            ("evaluator contract", expected_evaluator_contract_fingerprint),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"expected {label} fingerprint must be SHA-256")
        self.source_id = normalized_source_id
        self.path = self._require_safe_path(path)
        self.expected_strategy_spec_fingerprint = expected_strategy_spec_fingerprint
        self.expected_evaluator_contract_fingerprint = expected_evaluator_contract_fingerprint
        self.busy_timeout_ms = busy_timeout_ms
        observed = self.path.stat(follow_symlinks=False)
        self._file_identity = (observed.st_dev, observed.st_ino)
        self._read_identity()

    @staticmethod
    def _require_safe_path(path: Path) -> Path:
        candidate = Path(path)
        if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
            raise ValueError("runner source path must be absolute and normalized")
        current = Path(candidate.anchor)
        for component in candidate.parts[1:]:
            current /= component
            try:
                observed = current.lstat()
            except FileNotFoundError as exc:
                raise ValueError(f"runner source is unavailable: {candidate}") from exc
            if stat.S_ISLNK(observed.st_mode):
                raise ValueError(f"runner source path contains a symlink: {current}")
        observed = candidate.lstat()
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("runner source must be a regular file")
        return candidate

    def _connect(self) -> sqlite3.Connection:
        observed = self.path.stat(follow_symlinks=False)
        if (observed.st_dev, observed.st_ino) != self._file_identity:
            raise ValueError("runner source identity changed")
        uri = f"file:{quote(str(self.path), safe='/')}?mode=ro"
        try:
            connection = sqlite3.connect(
                uri,
                uri=True,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
            )
        except sqlite3.Error as exc:
            raise ValueError("runner source is unavailable in read-only mode") from exc
        try:
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA query_only = ON")
            connected = connection.execute("PRAGMA database_list").fetchone()
            if connected is None or Path(str(connected[2])).resolve() != self.path.resolve():
                raise ValueError("runner source connection resolved to another file")
            return connection
        except BaseException:
            connection.close()
            raise

    def _read_identity(self) -> tuple[str, str, str]:
        try:
            with self._connect() as connection:
                metadata = connection.execute(
                    """
                    SELECT strategy_spec_fingerprint, evaluator_contract_fingerprint
                    FROM runner_metadata WHERE singleton = 1
                    """
                ).fetchone()
                source = connection.execute(
                    """
                    SELECT source_generation_id
                    FROM runner_source_identity WHERE singleton = 1
                    """
                ).fetchone()
        except sqlite3.Error as exc:
            raise ValueError("runner source schema is unavailable") from exc
        if metadata is None or source is None:
            raise ValueError("runner source identity is unavailable")
        spec_fingerprint = str(metadata["strategy_spec_fingerprint"])
        evaluator_fingerprint = str(metadata["evaluator_contract_fingerprint"])
        generation_id = str(source["source_generation_id"])
        if spec_fingerprint != self.expected_strategy_spec_fingerprint:
            raise ValueError("runner source strategy spec identity does not match")
        if evaluator_fingerprint != self.expected_evaluator_contract_fingerprint:
            raise ValueError("runner source evaluator contract identity does not match")
        for label, value in (
            ("source generation", generation_id),
            ("strategy spec", spec_fingerprint),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"runner {label} is not a SHA-256 digest")
        return generation_id, spec_fingerprint, evaluator_fingerprint

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        _validate_batch_request(after_sequence=after_sequence, limit=limit)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN")
                identity = _query_source_snapshot(connection)
                self._validate_identity(identity)
                records = _query_signal_records(
                    connection,
                    after_sequence=after_sequence,
                    high_watermark=identity.high_watermark,
                    limit=limit,
                )
        except sqlite3.Error as exc:
            raise ValueError("runner signals are unavailable") from exc
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(
                descriptor=RouteSourceDescriptor(
                    source_id=self.source_id,
                    generation_id=identity.generation_id,
                    strategy_spec_fingerprint=identity.strategy_spec_fingerprint,
                    first_sequence=1,
                    high_watermark=identity.high_watermark,
                )
            ),
            after_sequence=after_sequence,
            limit=limit,
            records=records,
        )

    def _validate_identity(self, identity: _SourceIdentitySnapshot) -> None:
        if identity.strategy_spec_fingerprint != self.expected_strategy_spec_fingerprint:
            raise ValueError("runner source strategy spec identity does not match")
        if identity.evaluator_contract_fingerprint != self.expected_evaluator_contract_fingerprint:
            raise ValueError("runner source evaluator contract identity does not match")


class _SourceIdentitySnapshot(RuntimeContractModel):
    generation_id: Sha256
    strategy_spec_fingerprint: Sha256
    evaluator_contract_fingerprint: Sha256
    high_watermark: StrictInt = Field(ge=0)


def _validate_batch_request(*, after_sequence: int, limit: int) -> None:
    if isinstance(after_sequence, bool) or not isinstance(after_sequence, int):
        raise ValueError("signal sequence must be an integer")
    if after_sequence < 0:
        raise ValueError("signal sequence must be nonnegative")
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("signal batch limit must be an integer")
    if limit < 0:
        raise ValueError("signal batch limit must be nonnegative")


def _query_source_snapshot(connection: sqlite3.Connection) -> _SourceIdentitySnapshot:
    row = connection.execute(
        """
        SELECT metadata.strategy_spec_fingerprint,
               metadata.evaluator_contract_fingerprint,
               source.source_generation_id,
               (SELECT max(sequence) FROM runner_signal) AS high_watermark
        FROM runner_metadata AS metadata
        CROSS JOIN runner_source_identity AS source
        WHERE metadata.singleton = 1 AND source.singleton = 1
        """
    ).fetchone()
    if row is None:
        raise ValueError("runner source identity is unavailable")
    return _SourceIdentitySnapshot(
        generation_id=str(row["source_generation_id"]),
        strategy_spec_fingerprint=str(row["strategy_spec_fingerprint"]),
        evaluator_contract_fingerprint=str(row["evaluator_contract_fingerprint"]),
        high_watermark=(0 if row["high_watermark"] is None else int(row["high_watermark"])),
    )


def _query_signal_records(
    connection: sqlite3.Connection,
    *,
    after_sequence: int,
    high_watermark: int,
    limit: int,
) -> tuple[RunnerSignalRecord, ...]:
    rows = connection.execute(
        """
        SELECT sequence, payload_json FROM runner_signal
        WHERE sequence > ? AND sequence <= ?
        ORDER BY sequence LIMIT ?
        """,
        (after_sequence, high_watermark, limit),
    ).fetchall()
    return tuple(
        RunnerSignalRecord(
            sequence=int(row["sequence"]),
            signal=json.loads(str(row["payload_json"])),
        )
        for row in rows
    )


TargetResolver = Callable[[SignalEnvelope], RoutingDecision]


class SignalRouteSummary(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    source_generation_id: Sha256
    source_high_watermark: int = Field(ge=0)
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
    cursors.bind(bus)
    observed_cursor = bus.route_cursor(request.source_id)
    batch = RunnerSignalBatch.model_validate(
        source.read_batch(
            after_sequence=observed_cursor.last_sequence,
            limit=request.limit,
        )
    )
    if batch.after_sequence != observed_cursor.last_sequence or batch.limit != request.limit:
        raise SignalRouteConflictError(
            "source batch request does not match the router cursor and limit"
        )
    descriptor = batch.snapshot.descriptor
    if descriptor.source_id != request.source_id:
        raise SignalRouteConflictError(
            "requested source_id does not match the frozen source descriptor"
        )
    cursor = bus.bind_route_source(
        descriptor,
        routing_policy_fingerprint=cursors.routing_policy_fingerprint,
        observed_at=request.routed_at,
    )
    started_after = observed_cursor.last_sequence
    records = batch.records
    expected = started_after + 1
    for record in records:
        if record.sequence != expected:
            raise SignalRouteSequenceError(
                f"expected runner sequence {expected}, got {record.sequence}"
            )
        expected += 1
    if records and records[-1].sequence > descriptor.high_watermark:
        raise SignalRouteSequenceError("source returned records above its declared high watermark")
    if descriptor.high_watermark > started_after and not records:
        raise SignalRouteSequenceError("source tail is missing below the declared high watermark")
    if (
        records
        and len(records) < request.limit
        and records[-1].sequence < descriptor.high_watermark
    ):
        raise SignalRouteSequenceError("source tail is missing below the declared high watermark")

    routed_count = 0
    target_count = 0
    duplicate_count = 0
    no_target_count = 0
    expired_count = 0
    deferred_count = 0

    for record in records:
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
        source_generation_id=descriptor.generation_id,
        source_high_watermark=descriptor.high_watermark,
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
    "RunnerSignalBatch",
    "RunnerSignalSource",
    "ReadonlyStrategyRunnerSignalSource",
    "SignalRouteConflictError",
    "SignalRouteCursor",
    "SignalRouteCursorStore",
    "SignalRouteSequenceError",
    "SignalRouteSummary",
    "SourceSnapshot",
    "StrategyRunnerSignalSource",
    "TargetResolver",
    "route_runner_signals",
]
