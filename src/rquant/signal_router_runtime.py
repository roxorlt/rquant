"""Atomic bridge from immutable strategy signal spools to the signal bus."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import time as monotonic_time
from collections.abc import Callable, Mapping
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol, Self
from urllib.parse import quote

from pydantic import Field, StrictInt, StringConstraints, field_validator, model_validator

from rquant.delivery_contracts import DeliveryTarget
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_shadow_validation import ShadowSourceCompletionReceipt
from rquant.signal_bus import (
    RouteDecisionKind,
    RouteReceiptDisposition,
    RouteSourceDescriptor,
    SignalBusStore,
    SignalRouteConflictError,
    SignalRouteCursor,
    SignalRouteSequenceError,
    canonical_delivery_targets,
    require_legacy_signal_write,
    routing_decision_fingerprint,
)
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)
from rquant.strategy_runner import (
    RunnerSignalRecord,
    RunnerSignalRouteDrainEvidence,
    StrategyRunnerStore,
)
from rquant.strategy_spec import StrategySpec

_DEFAULT_MAX_RECORDS = 100_000
_DEFAULT_MAX_RAW_BYTES = 128 * 1024 * 1024
_DEFAULT_MAX_RECORD_BYTES = 4 * 1024 * 1024
_DEFAULT_FETCH_SIZE = 512
_MAX_COMPLETION_RECEIPT_BYTES = 64 * 1024
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 100_000

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


def _preflight_json_text(raw: bytes, *, label: str, maximum_bytes: int) -> str:
    if len(raw) > maximum_bytes:
        raise ValueError(f"{label} exceeds the byte budget")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} is not valid UTF-8") from exc
    depth = 0
    nodes = 0
    in_string = False
    escaped = False
    in_atom = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if in_atom:
            if character not in " \t\r\n,]}":
                continue
            in_atom = False
        if character == '"':
            in_string = True
            nodes += 1
        elif character in "[{":
            depth += 1
            nodes += 1
            if depth > _MAX_JSON_DEPTH:
                raise ValueError(f"{label} exceeds the JSON depth budget")
        elif character in "]}":
            depth -= 1
        elif character not in " \t\r\n,:":
            in_atom = True
            nodes += 1
        if nodes > _MAX_JSON_NODES:
            raise ValueError(f"{label} exceeds the JSON node budget")
    return text


def _reject_duplicate_signal_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    decoded: dict[str, object] = {}
    for key, value in pairs:
        if key in decoded:
            raise ValueError(f"runner signal payload contains duplicate JSON key: {key}")
        decoded[key] = value
    return decoded


def _bounded_json_loads(raw: bytes, *, label: str, maximum_bytes: int) -> object:
    text = _preflight_json_text(raw, label=label, maximum_bytes=maximum_bytes)
    try:
        decoded = json.loads(text, object_pairs_hook=_reject_duplicate_signal_json_keys)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    stack = [decoded]
    nodes = 0
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > _MAX_JSON_NODES:
            raise ValueError(f"{label} exceeds the JSON node budget")
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return decoded


class RoutingConfigurationUnavailableError(RuntimeError):
    """A transient resolver dependency failed, so the cursor must not advance."""


class SignalRouteBacklogError(ValueError):
    """The persisted router cursor has not drained the requested runner prefix yet."""


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


class CurrentRunnerSignalRecord(RuntimeContractModel):
    """Reader-only runner record for a current-family stored envelope."""

    sequence: int = Field(ge=1)
    signal: CurrentSignalEnvelope


RouterSignalRecord = RunnerSignalRecord | CurrentRunnerSignalRecord


class RunnerSignalBatch(RuntimeContractModel):
    snapshot: SourceSnapshot
    after_sequence: StrictInt = Field(ge=0)
    limit: StrictInt = Field(ge=0)
    records: tuple[RouterSignalRecord, ...]

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if len(self.records) > self.limit:
            raise ValueError("runner signal batch exceeds its requested limit")
        return self


class _RunnerReadBudget(RuntimeContractModel):
    max_records: StrictInt = Field(ge=1)
    max_raw_bytes: StrictInt = Field(ge=1)
    max_record_bytes: StrictInt = Field(ge=1)
    fetch_size: StrictInt = Field(ge=1, le=10_000)

    @model_validator(mode="after")
    def validate_byte_bounds(self) -> _RunnerReadBudget:
        if self.max_record_bytes > self.max_raw_bytes:
            raise ValueError("runner record byte budget cannot exceed total byte budget")
        return self


def _validate_read_budget(
    *,
    max_records: int,
    max_raw_bytes: int,
    max_record_bytes: int,
    fetch_size: int,
) -> _RunnerReadBudget:
    return _RunnerReadBudget(
        max_records=max_records,
        max_raw_bytes=max_raw_bytes,
        max_record_bytes=max_record_bytes,
        fetch_size=fetch_size,
    )


class RunnerSignalSource(Protocol):
    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch: ...


class StrategyRunnerSignalSource:
    """Expose one durable strategy runner spool through the routing source contract."""

    def __init__(
        self,
        *,
        source_id: str,
        store: StrategyRunnerStore,
        max_records: int = _DEFAULT_MAX_RECORDS,
        max_raw_bytes: int = _DEFAULT_MAX_RAW_BYTES,
        max_record_bytes: int = _DEFAULT_MAX_RECORD_BYTES,
        fetch_size: int = _DEFAULT_FETCH_SIZE,
    ) -> None:
        normalized = source_id.strip()
        if not normalized:
            raise ValueError("source_id must not be empty")
        self.source_id = normalized
        self.store = store
        self._read_budget = _validate_read_budget(
            max_records=max_records,
            max_raw_bytes=max_raw_bytes,
            max_record_bytes=max_record_bytes,
            fetch_size=fetch_size,
        )

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
                    budget=self._read_budget,
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

    def strategy_identity(self) -> tuple[str, int, str]:
        return (
            self.store.spec.strategy_id,
            self.store.spec.version,
            self.store.spec.spec_fingerprint,
        )

    def read_completion_receipt(
        self,
        *,
        trade_date: date,
    ) -> ShadowSourceCompletionReceipt:
        connect = getattr(self.store, "_connect", None)
        if not callable(connect):
            raise TypeError("strategy runner store does not expose a transactional connection")
        try:
            with connect() as connection:
                connection.execute("BEGIN")
                identity = _query_source_snapshot(connection)
                return _query_completion_receipt(
                    connection,
                    trade_date=trade_date,
                    source_id=self.source_id,
                    identity=identity,
                )
        except sqlite3.Error as exc:
            raise ValueError("runner completion receipt is unavailable") from exc

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch:
        _validate_batch_request(after_sequence=after_sequence, limit=limit)
        connect = getattr(self.store, "_connect", None)
        if not callable(connect):
            raise TypeError("strategy runner store does not expose a transactional connection")
        try:
            with connect() as connection:
                connection.execute("BEGIN")
                identity = _query_source_snapshot(connection)
                receipt = _query_completion_receipt(
                    connection,
                    trade_date=trade_date,
                    source_id=self.source_id,
                    identity=identity,
                )
                records = _query_signal_records(
                    connection,
                    after_sequence=max(
                        after_sequence,
                        0
                        if receipt.segment_start_sequence is None
                        else receipt.segment_start_sequence,
                    ),
                    high_watermark=_completion_high_watermark(receipt),
                    limit=limit,
                    budget=self._read_budget,
                )
        except sqlite3.Error as exc:
            raise ValueError("completed runner signals are unavailable") from exc
        return _completed_runner_batch(
            source_id=self.source_id,
            identity=identity,
            receipt=receipt,
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
        max_records: int = _DEFAULT_MAX_RECORDS,
        max_raw_bytes: int = _DEFAULT_MAX_RAW_BYTES,
        max_record_bytes: int = _DEFAULT_MAX_RECORD_BYTES,
        fetch_size: int = _DEFAULT_FETCH_SIZE,
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
        self._read_budget = _validate_read_budget(
            max_records=max_records,
            max_raw_bytes=max_raw_bytes,
            max_record_bytes=max_record_bytes,
            fetch_size=fetch_size,
        )
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

    def strategy_identity(self) -> tuple[str, int, str]:
        try:
            with self._connect() as connection:
                connection.execute("BEGIN")
                size_row = connection.execute(
                    """
                    SELECT length(CAST(strategy_spec_json AS BLOB)) AS payload_bytes
                    FROM runner_metadata WHERE singleton = 1
                    """
                ).fetchone()
                if size_row is None:
                    raise ValueError("runner strategy identity is unavailable")
                if int(size_row["payload_bytes"]) > _MAX_COMPLETION_RECEIPT_BYTES:
                    raise ValueError("runner strategy identity exceeds the byte budget")
                row = connection.execute(
                    """
                    SELECT CAST(strategy_spec_json AS BLOB) AS payload_bytes,
                           strategy_spec_fingerprint
                    FROM runner_metadata WHERE singleton = 1
                    """
                ).fetchone()
        except sqlite3.Error as exc:
            raise ValueError("runner strategy identity is unavailable") from exc
        if row is None:
            raise ValueError("runner strategy identity is unavailable")
        raw = bytes(row["payload_bytes"])
        if len(raw) > _MAX_COMPLETION_RECEIPT_BYTES:
            raise ValueError("runner strategy identity exceeds the byte budget")
        try:
            spec = StrategySpec.model_validate(
                _bounded_json_loads(
                    raw,
                    label="runner strategy identity payload",
                    maximum_bytes=_MAX_COMPLETION_RECEIPT_BYTES,
                )
            )
        except (TypeError, ValueError, RecursionError) as exc:
            raise ValueError("runner strategy identity payload is invalid") from exc
        if (
            spec.spec_fingerprint != str(row["strategy_spec_fingerprint"])
            or spec.spec_fingerprint != self.expected_strategy_spec_fingerprint
        ):
            raise ValueError("runner strategy identity does not match its fingerprint")
        return spec.strategy_id, spec.version, spec.spec_fingerprint

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
                    budget=self._read_budget,
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

    def read_completion_receipt(
        self,
        *,
        trade_date: date,
    ) -> ShadowSourceCompletionReceipt:
        try:
            with self._connect() as connection:
                connection.execute("BEGIN")
                identity = _query_source_snapshot(connection)
                self._validate_identity(identity)
                return _query_completion_receipt(
                    connection,
                    trade_date=trade_date,
                    source_id=self.source_id,
                    identity=identity,
                )
        except sqlite3.Error as exc:
            raise ValueError("runner completion receipt is unavailable") from exc

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch:
        _validate_batch_request(after_sequence=after_sequence, limit=limit)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN")
                identity = _query_source_snapshot(connection)
                self._validate_identity(identity)
                receipt = _query_completion_receipt(
                    connection,
                    trade_date=trade_date,
                    source_id=self.source_id,
                    identity=identity,
                )
                records = _query_signal_records(
                    connection,
                    after_sequence=max(
                        after_sequence,
                        0
                        if receipt.segment_start_sequence is None
                        else receipt.segment_start_sequence,
                    ),
                    high_watermark=_completion_high_watermark(receipt),
                    limit=limit,
                    budget=self._read_budget,
                )
        except sqlite3.Error as exc:
            raise ValueError("completed runner signals are unavailable") from exc
        return _completed_runner_batch(
            source_id=self.source_id,
            identity=identity,
            receipt=receipt,
            after_sequence=after_sequence,
            limit=limit,
            records=records,
        )

    def _validate_identity(self, identity: _SourceIdentitySnapshot) -> None:
        if identity.strategy_spec_fingerprint != self.expected_strategy_spec_fingerprint:
            raise ValueError("runner source strategy spec identity does not match")
        if identity.evaluator_contract_fingerprint != self.expected_evaluator_contract_fingerprint:
            raise ValueError("runner source evaluator contract identity does not match")


def _completion_high_watermark(receipt: ShadowSourceCompletionReceipt) -> int:
    if receipt.high_watermark is None:
        raise ValueError("runner completion receipt has no signal high watermark")
    return receipt.high_watermark


def _query_completion_receipt(
    connection: sqlite3.Connection,
    *,
    trade_date: date,
    source_id: str,
    identity: _SourceIdentitySnapshot,
) -> ShadowSourceCompletionReceipt:
    if not connection.in_transaction:
        raise ValueError("runner completion receipt requires one read snapshot")
    size_row = connection.execute(
        """
        SELECT length(CAST(payload_json AS BLOB)) AS payload_bytes
        FROM runner_session_close_receipt
        WHERE trade_date = ?
        """,
        (trade_date.isoformat(),),
    ).fetchone()
    if size_row is None:
        raise ValueError("runner completion receipt is missing")
    if int(size_row["payload_bytes"]) > _MAX_COMPLETION_RECEIPT_BYTES:
        raise ValueError("runner completion receipt exceeds the byte budget")
    row = connection.execute(
        """
        SELECT trade_date, receipt_id, source_id, signal_high_watermark,
               CAST(payload_json AS BLOB) AS payload_bytes
        FROM runner_session_close_receipt
        WHERE trade_date = ?
        """,
        (trade_date.isoformat(),),
    ).fetchone()
    if row is None:
        raise ValueError("runner completion receipt changed after byte preflight")
    raw = bytes(row["payload_bytes"])
    if len(raw) > _MAX_COMPLETION_RECEIPT_BYTES:
        raise ValueError("runner completion receipt exceeds the byte budget")
    try:
        receipt = ShadowSourceCompletionReceipt.model_validate(
            _bounded_json_loads(
                raw,
                label="runner completion receipt payload",
                maximum_bytes=_MAX_COMPLETION_RECEIPT_BYTES,
            )
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError(f"runner completion receipt payload is invalid: {exc}") from exc
    canonical = json.dumps(
        receipt.model_dump(mode="json"),
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if raw != canonical:
        raise ValueError("runner completion receipt payload is not canonical")
    if (
        str(row["trade_date"]) != receipt.trade_date.isoformat()
        or str(row["receipt_id"]) != receipt.receipt_id
        or str(row["source_id"]) != receipt.source_id
        or int(row["signal_high_watermark"]) != receipt.high_watermark
    ):
        raise ValueError("runner completion receipt identity does not match payload")
    if receipt.evidence_origin != "production" or receipt.source != "isolated":
        raise ValueError("runner completion receipt is not production isolated evidence")
    if receipt.source_id != source_id:
        raise ValueError("runner completion receipt source does not match")
    if receipt.runner_generation_id != identity.generation_id:
        raise ValueError("runner completion receipt generation does not match")
    if receipt.high_watermark is None or receipt.high_watermark > identity.high_watermark:
        raise ValueError("runner completion receipt exceeds the durable source watermark")
    return receipt


def _completed_runner_batch(
    *,
    source_id: str,
    identity: _SourceIdentitySnapshot,
    receipt: ShadowSourceCompletionReceipt,
    after_sequence: int,
    limit: int,
    records: tuple[RunnerSignalRecord, ...],
) -> RunnerSignalBatch:
    return RunnerSignalBatch(
        snapshot=SourceSnapshot(
            descriptor=RouteSourceDescriptor(
                source_id=source_id,
                generation_id=identity.generation_id,
                strategy_spec_fingerprint=identity.strategy_spec_fingerprint,
                first_sequence=(
                    1
                    if receipt.segment_start_sequence is None
                    else receipt.segment_start_sequence + 1
                ),
                high_watermark=_completion_high_watermark(receipt),
            )
        ),
        after_sequence=after_sequence,
        limit=limit,
        records=records,
    )


class ReadonlySignalRouteAuthority:
    """Read persisted route receipts without opening the signal bus for writes."""

    def __init__(
        self,
        *,
        path: Path,
        expected_routing_policy_fingerprint: str,
        busy_timeout_ms: int = 5_000,
        fetch_size: int = _DEFAULT_FETCH_SIZE,
        max_session_records: int = _DEFAULT_MAX_RECORDS,
        max_session_raw_bytes: int = _DEFAULT_MAX_RAW_BYTES,
        max_receipt_bytes: int = _DEFAULT_MAX_RECORD_BYTES,
        deadline_seconds: float = 5.0,
        monotonic_clock: Callable[[], float] = monotonic_time.monotonic,
    ) -> None:
        if len(expected_routing_policy_fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in expected_routing_policy_fingerprint
        ):
            raise ValueError("expected routing policy fingerprint must be SHA-256")
        if busy_timeout_ms < 1 or fetch_size < 1:
            raise ValueError("route authority read bounds must be positive")
        if (
            isinstance(max_session_records, bool)
            or isinstance(max_session_raw_bytes, bool)
            or isinstance(max_receipt_bytes, bool)
            or not isinstance(max_session_records, int)
            or not isinstance(max_session_raw_bytes, int)
            or not isinstance(max_receipt_bytes, int)
            or max_session_records < 1
            or max_session_raw_bytes < 1
            or max_receipt_bytes < 1
            or max_receipt_bytes > max_session_raw_bytes
            or isinstance(deadline_seconds, bool)
            or not isinstance(deadline_seconds, (int, float))
            or deadline_seconds <= 0
            or not callable(monotonic_clock)
        ):
            raise ValueError("route authority session budget is invalid")
        self.path = ReadonlyStrategyRunnerSignalSource._require_safe_path(path)
        self.expected_routing_policy_fingerprint = expected_routing_policy_fingerprint
        self.busy_timeout_ms = busy_timeout_ms
        self.fetch_size = min(fetch_size, 10_000)
        self.max_session_records = max_session_records
        self.max_session_raw_bytes = max_session_raw_bytes
        self.max_receipt_bytes = max_receipt_bytes
        self.deadline_seconds = float(deadline_seconds)
        self.monotonic_clock = monotonic_clock
        observed = self.path.stat(follow_symlinks=False)
        self._file_identity = (observed.st_dev, observed.st_ino)

    def _connect(self) -> sqlite3.Connection:
        observed = self.path.stat(follow_symlinks=False)
        if (observed.st_dev, observed.st_ino) != self._file_identity:
            raise ValueError("signal route authority identity changed")
        uri = f"file:{quote(str(self.path), safe='/')}?mode=ro"
        try:
            connection = sqlite3.connect(
                uri,
                uri=True,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
            )
        except sqlite3.Error as exc:
            raise ValueError("signal route authority is unavailable") from exc
        try:
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA query_only = ON")
            connected = connection.execute("PRAGMA database_list").fetchone()
            if connected is None or Path(str(connected[2])).resolve() != self.path.resolve():
                raise ValueError("signal route authority connection resolved to another file")
            return connection
        except BaseException:
            connection.close()
            raise

    def read_drain_evidence(
        self,
        *,
        source_id: str,
        runner_generation_id: str,
        strategy_spec_fingerprint: str,
        trade_date: date,
        segment_start_sequence: int,
        routed_through_sequence: int,
        observed_at: datetime,
    ) -> RunnerSignalRouteDrainEvidence:
        observed = normalize_aware_utc(observed_at)
        if not isinstance(trade_date, date):
            raise ValueError("route segment trade_date is required")
        if (
            isinstance(segment_start_sequence, bool)
            or not isinstance(segment_start_sequence, int)
            or segment_start_sequence < 0
            or routed_through_sequence < segment_start_sequence
        ):
            raise ValueError("route segment sequence range is invalid")
        segment_record_count = routed_through_sequence - segment_start_sequence
        if segment_record_count > self.max_session_records:
            raise ValueError("signal route session exceeds the record budget")
        deadline = self.monotonic_clock() + self.deadline_seconds

        def deadline_guard() -> int:
            return int(self.monotonic_clock() > deadline)

        try:
            with self._connect() as connection:
                connection.execute("BEGIN")
                connection.set_progress_handler(deadline_guard, 1_000)
                generation = connection.execute(
                    """
                    SELECT metadata_value FROM signal_bus_metadata
                    WHERE metadata_key = 'source_generation_id'
                    """
                ).fetchone()
                source = connection.execute(
                    "SELECT * FROM signal_route_source WHERE source_id = ?",
                    (source_id,),
                ).fetchone()
                if generation is None:
                    raise ValueError("signal route authority generation is unavailable")
                if source is None:
                    raise SignalRouteBacklogError(
                        "signal route authority has not observed the runner source"
                    )
                if str(source["generation_id"]) != runner_generation_id:
                    raise ValueError("signal route authority runner generation changed")
                if str(source["strategy_spec_fingerprint"]) != strategy_spec_fingerprint:
                    raise ValueError("signal route authority strategy identity changed")
                if (
                    str(source["routing_policy_fingerprint"])
                    != self.expected_routing_policy_fingerprint
                ):
                    raise ValueError("signal route authority routing policy changed")
                observed_high_watermark = int(source["observed_high_watermark"])
                last_sequence = int(source["last_sequence"])
                if (
                    observed_high_watermark < routed_through_sequence
                    or last_sequence < routed_through_sequence
                ):
                    raise SignalRouteBacklogError(
                        "signal route backlog is not routed through the prefix"
                    )
                updated_at = normalize_aware_utc(
                    datetime.fromisoformat(str(source["updated_at"]).replace("Z", "+00:00"))
                )
                if updated_at > observed:
                    raise ValueError("signal route authority was updated after observation")
                chain_hash = canonical_sha256(
                    {
                        "contract": "runner-signal-route-session-chain/v1",
                        "source_id": source_id,
                        "trade_date": trade_date,
                        "signal_authority_generation_id": str(generation["metadata_value"]),
                        "routing_policy_fingerprint": (self.expected_routing_policy_fingerprint),
                        "segment_start_sequence": segment_start_sequence,
                    }
                )
                cursor = connection.execute(
                    """
                    SELECT * FROM signal_route_receipt
                    WHERE source_id = ?
                      AND source_sequence > ? AND source_sequence <= ?
                    ORDER BY source_sequence
                    """,
                    (source_id, segment_start_sequence, routed_through_sequence),
                )
                expected_sequence = segment_start_sequence + 1
                count = 0
                raw_bytes = 0
                while True:
                    if self.monotonic_clock() > deadline:
                        raise ValueError("signal route session read exceeded its deadline")
                    rows = cursor.fetchmany(self.fetch_size)
                    if not rows:
                        break
                    for row in rows:
                        if self.monotonic_clock() > deadline:
                            raise ValueError("signal route session read exceeded its deadline")
                        if int(row["source_sequence"]) != expected_sequence:
                            raise ValueError("signal route session has a sequence gap")
                        payload = json.dumps(
                            dict(row),
                            ensure_ascii=True,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                        if len(payload) > self.max_receipt_bytes:
                            raise ValueError("signal route receipt exceeds the byte budget")
                        count += 1
                        raw_bytes += len(payload)
                        if count > self.max_session_records:
                            raise ValueError("signal route session exceeds the record budget")
                        if raw_bytes > self.max_session_raw_bytes:
                            raise ValueError("signal route session exceeds the raw byte budget")
                        chain_hash = canonical_sha256(
                            {
                                "previous": chain_hash,
                                "source_sequence": int(row["source_sequence"]),
                                "receipt_sha256": hashlib.sha256(payload).hexdigest(),
                                "receipt_bytes": len(payload),
                            }
                        )
                        expected_sequence += 1
                connection.set_progress_handler(None, 0)
                if count != segment_record_count:
                    raise ValueError("signal route session segment is incomplete")
        except sqlite3.Error as exc:
            if "interrupted" in str(exc).lower():
                raise ValueError("signal route session read exceeded its deadline") from exc
            raise ValueError("signal route authority schema is unavailable") from exc
        route_receipts_sha256 = canonical_sha256(
            {
                "contract": "runner-signal-route-session/v1",
                "source_id": source_id,
                "routing_policy_fingerprint": self.expected_routing_policy_fingerprint,
                "trade_date": trade_date,
                "segment_start_sequence": segment_start_sequence,
                "routed_through_sequence": routed_through_sequence,
                "receipt_count": count,
                "receipt_raw_bytes": raw_bytes,
                "segment_chain_hash": chain_hash,
            }
        )
        return RunnerSignalRouteDrainEvidence(
            source_id=source_id,
            runner_generation_id=runner_generation_id,
            strategy_spec_fingerprint=strategy_spec_fingerprint,
            signal_authority_generation_id=str(generation["metadata_value"]),
            routing_policy_fingerprint=self.expected_routing_policy_fingerprint,
            trade_date=trade_date,
            segment_start_sequence=segment_start_sequence,
            segment_record_count=count,
            segment_raw_bytes=raw_bytes,
            segment_chain_hash=chain_hash,
            observed_high_watermark=observed_high_watermark,
            routed_through_sequence=routed_through_sequence,
            last_sequence=last_sequence,
            route_receipts_sha256=route_receipts_sha256,
            observed_at=observed,
        )


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
    budget: _RunnerReadBudget,
) -> tuple[RouterSignalRecord, ...]:
    preflight = connection.execute(
        """
        WITH bounded AS (
            SELECT payload_json FROM runner_signal
            WHERE sequence > ? AND sequence <= ?
            ORDER BY sequence LIMIT ?
        )
        SELECT count(*) AS record_count,
               COALESCE(sum(length(CAST(payload_json AS BLOB))), 0) AS raw_bytes,
               COALESCE(max(length(CAST(payload_json AS BLOB))), 0) AS max_record_bytes
        FROM bounded
        """,
        (after_sequence, high_watermark, limit),
    ).fetchone()
    if preflight is None:
        raise ValueError("runner signal batch preflight is unavailable")
    record_count = int(preflight["record_count"])
    raw_bytes = int(preflight["raw_bytes"])
    max_record_bytes = int(preflight["max_record_bytes"])
    if record_count > budget.max_records:
        raise ValueError("runner signal batch exceeds the record budget")
    if max_record_bytes > budget.max_record_bytes:
        raise ValueError("runner signal record exceeds the byte budget")
    if raw_bytes > budget.max_raw_bytes:
        raise ValueError("runner signal batch exceeds the raw byte budget")

    cursor = connection.execute(
        """
        SELECT sequence, signal_id, CAST(payload_json AS BLOB) AS payload_bytes
        FROM runner_signal
        WHERE sequence > ? AND sequence <= ?
        ORDER BY sequence LIMIT ?
        """,
        (after_sequence, high_watermark, limit),
    )
    records: list[RouterSignalRecord] = []
    consumed = 0
    while True:
        rows = cursor.fetchmany(budget.fetch_size)
        if not rows:
            break
        for row in rows:
            raw = row["payload_bytes"]
            if not isinstance(raw, bytes):
                raise ValueError("runner signal payload is not a byte string")
            if len(raw) > budget.max_record_bytes:
                raise ValueError("runner signal record exceeds the byte budget")
            consumed += len(raw)
            if consumed > budget.max_raw_bytes:
                raise ValueError("runner signal batch exceeds the raw byte budget")
            if len(records) + 1 > budget.max_records:
                raise ValueError("runner signal batch exceeds the record budget")
            decoded = _bounded_json_loads(
                raw,
                label="runner signal payload",
                maximum_bytes=budget.max_record_bytes,
            )
            try:
                if not isinstance(decoded, Mapping):
                    raise TypeError("runner signal payload must be a JSON object")
                signal = parse_signal_envelope(decoded)
                if str(row["signal_id"]) != signal.signal_id:
                    raise ValueError("runner stored signal_id does not match signal payload")
                record = (
                    CurrentRunnerSignalRecord(sequence=int(row["sequence"]), signal=signal)
                    if isinstance(signal, CurrentSignalEnvelope)
                    else RunnerSignalRecord(sequence=int(row["sequence"]), signal=signal)
                )
            except (TypeError, ValueError, RecursionError) as exc:
                raise ValueError("runner signal payload is invalid") from exc
            records.append(record)
    if len(records) != record_count or consumed != raw_bytes:
        raise ValueError("runner signal batch changed after its budget preflight")
    return tuple(records)


TargetResolver = Callable[[SignalEnvelopeFamily], RoutingDecision]


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
    for record in batch.records:
        require_legacy_signal_write(
            record.signal,
            operation="route_runner_signals",
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
    "ReadonlySignalRouteAuthority",
    "RouteSourceDescriptor",
    "RoutingConfigurationUnavailableError",
    "RoutingDecision",
    "RoutingDecisionAction",
    "RunnerSignalBatch",
    "RunnerSignalSource",
    "ReadonlyStrategyRunnerSignalSource",
    "SignalRouteConflictError",
    "SignalRouteBacklogError",
    "SignalRouteCursor",
    "SignalRouteCursorStore",
    "SignalRouteSequenceError",
    "SignalRouteSummary",
    "SourceSnapshot",
    "StrategyRunnerSignalSource",
    "TargetResolver",
    "route_runner_signals",
]
