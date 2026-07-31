"""Deterministic single-strategy runner with an immutable signal spool."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Annotated

import pandas as pd
from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
    FeatureRequirement,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.strategy_spec import StrategyLifecycleState, StrategySpec

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class StrategyBatchConflictError(RuntimeError):
    """A runner input sequence was missing or reused with different evidence."""


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in sorted(value.items())}
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_thaw_json(item) for item in value]
    return value


class StrategyDecision(RuntimeContractModel):
    """One pure evaluator result for the candidate's current lifecycle state."""

    event: str = Field(min_length=1)
    expected_from_state: StrategyLifecycleState
    expected_to_state: StrategyLifecycleState
    expected_action: SignalAction | None
    action: SignalAction | None = None
    reason_codes: tuple[str, ...] = ()
    evidence: Mapping[str, JsonValue] = Field(default_factory=dict)
    expires_after: timedelta | None = None

    @field_validator("reason_codes")
    @classmethod
    def validate_reason_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("reason_codes cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("reason_codes must be unique")
        return tuple(sorted(values))

    @field_validator("evidence")
    @classmethod
    def freeze_evidence(cls, value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        frozen = _freeze_json(value)
        if not isinstance(frozen, Mapping):
            raise TypeError("evidence must be a mapping")
        canonical_sha256(frozen)
        return frozen  # type: ignore[return-value]

    @field_serializer("evidence")
    def serialize_evidence(self, value: Mapping[str, JsonValue]) -> dict[str, object]:
        thawed = _thaw_json(value)
        if not isinstance(thawed, dict):
            raise TypeError("evidence must serialize as a mapping")
        return thawed

    @model_validator(mode="after")
    def validate_signal_fields(self) -> StrategyDecision:
        if self.expected_action is not self.action:
            raise ValueError("expected_action must equal action")
        if self.action is SignalAction.B_INTENT and self.expected_to_state in {
            StrategyLifecycleState.IDLE,
            StrategyLifecycleState.TERMINAL,
        }:
            raise ValueError(
                f"{self.action.value} cannot transition to {self.expected_to_state.value}"
            )
        if self.action is None:
            if self.reason_codes or self.expires_after is not None:
                raise ValueError("transition-only decisions cannot contain signal fields")
        else:
            if not self.reason_codes:
                raise ValueError("signal decisions require reason_codes")
            if self.expires_after is None or self.expires_after <= timedelta(0):
                raise ValueError("signal decisions require a positive expires_after")
        return self


class StrategyCandidateState(RuntimeContractModel):
    strategy_spec_fingerprint: Sha256
    candidate_id: str = Field(min_length=1)
    state: StrategyLifecycleState
    last_feature_sequence: int = Field(ge=-1)
    last_feature_batch_id: str | None = None
    updated_at: AwareUtcDatetime


class RunnerSignalRecord(RuntimeContractModel):
    sequence: int = Field(ge=1)
    signal: SignalEnvelope


class StrategyBatchResult(RuntimeContractModel):
    feature_batch_id: str = Field(min_length=1)
    feature_sequence: int = Field(ge=0)
    processed_candidates: int = Field(ge=0)
    transitioned_candidates: int = Field(ge=0)
    skipped_candidates: int = Field(ge=0)
    signals: tuple[RunnerSignalRecord, ...]

    @model_validator(mode="after")
    def validate_counts(self) -> StrategyBatchResult:
        if self.transitioned_candidates + self.skipped_candidates > self.processed_candidates:
            raise ValueError("transitioned and skipped counts exceed processed candidates")
        return self


StrategyEvaluator = Callable[
    [StrategySpec, StrategyCandidateState, Mapping[str, object]],
    StrategyDecision | None,
]


def _json_payload(model: RuntimeContractModel) -> str:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _normalize_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")
    if "ts_code" not in frame.columns:
        raise ValueError("feature frame requires ts_code")
    if len(frame.columns) != len(set(frame.columns)):
        raise ValueError("feature frame columns must be unique")
    normalized = frame.copy(deep=True)
    normalized["ts_code"] = normalized["ts_code"].astype("string").str.strip()
    if normalized["ts_code"].isna().any() or (normalized["ts_code"] == "").any():
        raise ValueError("feature frame ts_code cannot be empty")
    if normalized["ts_code"].duplicated().any():
        raise ValueError("feature frame ts_code must be unique")
    return normalized.sort_values("ts_code", kind="stable").reset_index(drop=True)


def _canonical_feature_value(value: object) -> JsonValue:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("feature timestamps must be timezone-aware")
        return value.isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("feature datetimes must be timezone-aware")
        return value.isoformat()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return _canonical_feature_value(value.item())
        except ValueError:
            raise
        except (AttributeError, TypeError):
            pass
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if not math.isfinite(value):
            raise ValueError("feature payload forbids infinite values")
        return value
    if isinstance(value, (str, bool, int)):
        return value
    raise TypeError(f"feature payload values must be JSON scalars, got {type(value).__name__}")


def canonical_feature_payload(frame: pd.DataFrame, *, schema_version: int) -> bytes:
    """Encode the exact feature payload contract shared with intraday producers."""

    if schema_version < 1:
        raise ValueError("schema_version must be positive")
    normalized = _normalize_feature_frame(frame)
    rows = [
        {key: _canonical_feature_value(value) for key, value in row.items()}
        for row in normalized.to_dict(orient="records")
    ]
    return json.dumps(
        {"schema_version": schema_version, "rows": rows},
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_sha256(value: str, *, label: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")
    return value


class StrategyRunnerStore:
    """Own one exact strategy spec, candidate states, and its signal sequence."""

    def __init__(
        self,
        path: Path,
        *,
        spec: StrategySpec,
        evaluator_contract_fingerprint: Sha256,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.spec = spec
        self.evaluator_contract_fingerprint = _validate_sha256(
            evaluator_contract_fingerprint,
            label="evaluator_contract_fingerprint",
        )
        self.busy_timeout_ms = busy_timeout_ms
        self._transitions = {
            (transition.from_state, transition.event): transition.to_state
            for transition in spec.transitions
        }
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runner_metadata (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    strategy_spec_fingerprint TEXT NOT NULL,
                    strategy_spec_json TEXT NOT NULL,
                    evaluator_contract_fingerprint TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS candidate_state (
                    candidate_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    last_feature_sequence INTEGER NOT NULL,
                    last_feature_batch_id TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runner_signal (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL UNIQUE,
                    feature_sequence INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS processed_batch (
                    feature_sequence INTEGER PRIMARY KEY,
                    feature_batch_id TEXT NOT NULL UNIQUE,
                    envelope_fingerprint TEXT NOT NULL,
                    feature_payload_hash TEXT NOT NULL,
                    dataset_snapshot_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    result_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runner_source_identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    source_generation_id TEXT NOT NULL
                );
                """
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                source_row = connection.execute(
                    """
                    SELECT source_generation_id
                    FROM runner_source_identity WHERE singleton = 1
                    """
                ).fetchone()
                source_generation_id = (
                    secrets.token_hex(32)
                    if source_row is None
                    else str(source_row["source_generation_id"])
                )
                if source_row is None:
                    connection.execute(
                        """
                        INSERT INTO runner_source_identity(singleton, source_generation_id)
                        VALUES (1, ?)
                        """,
                        (source_generation_id,),
                    )
                _validate_sha256(source_generation_id, label="source_generation_id")
                existing = connection.execute(
                    """
                    SELECT strategy_spec_fingerprint, evaluator_contract_fingerprint
                    FROM runner_metadata WHERE singleton = 1
                    """
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO runner_metadata(
                            singleton, strategy_spec_fingerprint, strategy_spec_json,
                            evaluator_contract_fingerprint
                        ) VALUES (1, ?, ?, ?)
                        """,
                        (
                            self.spec.spec_fingerprint,
                            _json_payload(self.spec),
                            self.evaluator_contract_fingerprint,
                        ),
                    )
                elif existing["strategy_spec_fingerprint"] != self.spec.spec_fingerprint:
                    raise ValueError("strategy spec does not match persisted runner identity")
                elif (
                    existing["evaluator_contract_fingerprint"]
                    != self.evaluator_contract_fingerprint
                ):
                    raise ValueError("evaluator contract does not match persisted runner identity")
                connection.commit()
                self.source_generation_id = source_generation_id
            except BaseException:
                connection.rollback()
                raise

    def process_batch(
        self,
        envelope: FeatureBatchEnvelope,
        frame: pd.DataFrame,
        *,
        feature_payload: bytes | str | None = None,
        dataset_snapshot_id: Sha256,
        observed_at: datetime,
        evaluator: StrategyEvaluator,
    ) -> StrategyBatchResult:
        observed_at = normalize_aware_utc(observed_at)
        dataset_snapshot_id = _validate_sha256(
            dataset_snapshot_id,
            label="dataset_snapshot_id",
        )
        self._validate_batch(envelope, frame, observed_at=observed_at)
        envelope_fingerprint = canonical_sha256(envelope)
        normalized = self._normalize_frame(frame)
        self._validate_feature_structure(envelope, normalized)
        canonical_payload = canonical_feature_payload(
            normalized,
            schema_version=envelope.schema_version,
        )
        if feature_payload is not None:
            supplied_payload = (
                feature_payload.encode("utf-8")
                if isinstance(feature_payload, str)
                else feature_payload
            )
            if supplied_payload != canonical_payload:
                raise StrategyBatchConflictError(
                    "supplied feature payload does not match the canonical DataFrame payload"
                )
        feature_payload_hash = hashlib.sha256(canonical_payload).hexdigest()
        if feature_payload_hash != envelope.content_hash:
            raise StrategyBatchConflictError(
                "feature payload hash does not match envelope content_hash"
            )

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT * FROM processed_batch WHERE feature_sequence = ?",
                    (envelope.sequence,),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["feature_batch_id"] != envelope.batch_id
                        or existing["envelope_fingerprint"] != envelope_fingerprint
                        or existing["feature_payload_hash"] != feature_payload_hash
                        or existing["dataset_snapshot_id"] != dataset_snapshot_id
                        or observed_at < datetime.fromisoformat(existing["observed_at"])
                    ):
                        raise StrategyBatchConflictError(
                            "immutable batch sequence contains conflicting evidence"
                        )
                    connection.rollback()
                    return StrategyBatchResult.model_validate_json(existing["result_json"])

                previous = connection.execute(
                    "SELECT * FROM processed_batch ORDER BY feature_sequence DESC LIMIT 1"
                ).fetchone()
                last_sequence = -1 if previous is None else int(previous["feature_sequence"])
                expected = last_sequence + 1
                if envelope.sequence != expected:
                    raise StrategyBatchConflictError(
                        f"next feature sequence must be {expected}, got {envelope.sequence}"
                    )
                if previous is not None:
                    previous_times = {
                        "event_time": datetime.fromisoformat(previous["event_time"]),
                        "available_at": datetime.fromisoformat(previous["available_at"]),
                        "observed_at": datetime.fromisoformat(previous["observed_at"]),
                    }
                    current_times = {
                        "event_time": envelope.event_time,
                        "available_at": envelope.available_at,
                        "observed_at": observed_at,
                    }
                    for label, current_time in current_times.items():
                        if current_time < previous_times[label]:
                            raise StrategyBatchConflictError(
                                f"{label} cannot move backwards across feature sequences"
                            )

                records: list[RunnerSignalRecord] = []
                transitioned = 0
                skipped = 0
                for row in normalized.to_dict(orient="records"):
                    candidate_id = str(row["ts_code"])
                    state = self._candidate_state(connection, candidate_id, observed_at)
                    features = self._candidate_features(envelope, row)
                    if features is None:
                        skipped += 1
                        self._write_state(
                            connection,
                            state.model_copy(
                                update={
                                    "last_feature_sequence": envelope.sequence,
                                    "last_feature_batch_id": envelope.batch_id,
                                    "updated_at": observed_at,
                                }
                            ),
                        )
                        continue

                    decision = evaluator(self.spec, state, features)
                    if decision is None:
                        next_state = state.state
                    else:
                        transition_key = (state.state, decision.event)
                        if transition_key not in self._transitions:
                            raise ValueError(
                                f"event {decision.event!r} is invalid from state "
                                f"{state.state.value}"
                            )
                        next_state = self._transitions[transition_key]
                        if decision.expected_from_state is not state.state:
                            raise ValueError(
                                "decision expected_from_state does not match candidate state"
                            )
                        if decision.expected_to_state is not next_state:
                            raise ValueError(
                                "decision expected_to_state does not match strategy transition"
                            )
                        transitioned += 1
                        if decision.action is not None:
                            if decision.action.value not in self.spec.allowed_actions:
                                raise ValueError(
                                    f"action {decision.action.value!r} is not allowed by "
                                    "strategy spec"
                                )
                            evidence = _thaw_json(decision.evidence)
                            if not isinstance(evidence, dict):
                                raise TypeError("decision evidence must be a mapping")
                            if "runner_transition" in evidence:
                                raise ValueError(
                                    "decision evidence cannot override runner_transition"
                                )
                            evidence["runner_transition"] = {
                                "event": decision.event,
                                "from_state": state.state.value,
                                "to_state": next_state.value,
                                "feature_batch_id": envelope.batch_id,
                                "feature_sequence": envelope.sequence,
                                "evaluator_contract_fingerprint": (
                                    self.evaluator_contract_fingerprint
                                ),
                            }
                            signal = SignalEnvelope(
                                schema_version=1,
                                strategy_id=self.spec.strategy_id,
                                strategy_version=str(self.spec.version),
                                parameter_fingerprint=self.spec.parameter_fingerprint,
                                dataset_snapshot_id=dataset_snapshot_id,
                                feature_snapshot_id=envelope.content_hash,
                                event_time=envelope.event_time,
                                available_at=observed_at,
                                candidate_id=candidate_id,
                                action=decision.action,
                                reason_codes=decision.reason_codes,
                                evidence=evidence,
                                expires_at=observed_at + decision.expires_after,  # type: ignore[operator]
                                producer_commit=self.spec.producer_commit,
                            )
                            cursor = connection.execute(
                                """
                                INSERT INTO runner_signal(
                                    signal_id, feature_sequence, payload_json
                                ) VALUES (?, ?, ?)
                                """,
                                (signal.signal_id, envelope.sequence, _json_payload(signal)),
                            )
                            records.append(
                                RunnerSignalRecord(
                                    sequence=int(cursor.lastrowid),
                                    signal=signal.model_dump(mode="json"),
                                )
                            )

                    self._write_state(
                        connection,
                        state.model_copy(
                            update={
                                "state": next_state,
                                "last_feature_sequence": envelope.sequence,
                                "last_feature_batch_id": envelope.batch_id,
                                "updated_at": observed_at,
                            }
                        ),
                    )

                result = StrategyBatchResult(
                    feature_batch_id=envelope.batch_id,
                    feature_sequence=envelope.sequence,
                    processed_candidates=len(normalized),
                    transitioned_candidates=transitioned,
                    skipped_candidates=skipped,
                    signals=tuple(record.model_dump(mode="json") for record in records),
                )
                connection.execute(
                    """
                    INSERT INTO processed_batch(
                        feature_sequence, feature_batch_id, envelope_fingerprint,
                        feature_payload_hash, dataset_snapshot_id, event_time,
                        available_at, observed_at, result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        envelope.sequence,
                        envelope.batch_id,
                        envelope_fingerprint,
                        feature_payload_hash,
                        dataset_snapshot_id,
                        envelope.event_time.isoformat(),
                        envelope.available_at.isoformat(),
                        observed_at.isoformat(),
                        _json_payload(result),
                    ),
                )
                connection.commit()
                return result
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def _validate_batch(
        self,
        envelope: FeatureBatchEnvelope,
        frame: pd.DataFrame,
        *,
        observed_at: datetime,
    ) -> None:
        if envelope.contract_id != self.spec.feature_contract_id:
            raise ValueError("feature contract id does not match strategy spec")
        if envelope.contract_version < self.spec.min_feature_contract_version:
            raise ValueError("feature contract version is below strategy minimum")
        if observed_at < envelope.available_at:
            raise ValueError("batch cannot be processed before feature available_at")
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("frame must be a pandas DataFrame")
        if len(frame) != envelope.row_count:
            raise ValueError("feature frame row count does not match envelope")

    @staticmethod
    def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
        return _normalize_feature_frame(frame)

    @staticmethod
    def _has_scalar_value(value: object) -> bool:
        return value is not None and pd.api.types.is_scalar(value) and not bool(pd.isna(value))

    @staticmethod
    def _status_is_usable(
        status: FeatureFieldStatus,
        *,
        allow_degraded: bool,
    ) -> bool:
        if status.status in {
            FeatureAvailability.UNAVAILABLE,
            FeatureAvailability.STALE,
        }:
            return False
        return status.status is not FeatureAvailability.DEGRADED or allow_degraded

    def _eligible_optional_requirements(
        self,
        envelope: FeatureBatchEnvelope,
    ) -> tuple[FeatureRequirement, ...]:
        return tuple(
            requirement
            for requirement in self.spec.optional_features
            if envelope.contract_version >= requirement.min_contract_version
        )

    def _validate_feature_structure(
        self,
        envelope: FeatureBatchEnvelope,
        frame: pd.DataFrame,
    ) -> None:
        incompatible_required = sorted(
            requirement.name
            for requirement in self.spec.required_features
            if envelope.contract_version < requirement.min_contract_version
        )
        if incompatible_required:
            raise ValueError(
                "feature contract version is below required features: "
                + ", ".join(incompatible_required)
            )
        requirements = self.spec.required_features + self._eligible_optional_requirements(envelope)
        missing_columns = sorted(
            requirement.name
            for requirement in requirements
            if requirement.name not in frame.columns
        )
        if missing_columns:
            raise ValueError("missing feature columns: " + ", ".join(missing_columns))
        missing_statuses = sorted(
            requirement.name
            for requirement in requirements
            if envelope.field_status(requirement.name) is None
        )
        if missing_statuses:
            raise ValueError("missing field status for: " + ", ".join(missing_statuses))

    def _candidate_features(
        self,
        envelope: FeatureBatchEnvelope,
        row: Mapping[str, object],
    ) -> Mapping[str, object] | None:
        features: dict[str, object] = {}
        for requirement in self.spec.required_features:
            status = envelope.field_status(requirement.name)
            if status is None:
                raise RuntimeError("feature structure was not validated")
            if not self._status_is_usable(
                status,
                allow_degraded=requirement.allow_degraded,
            ):
                return None
            value = row[requirement.name]
            if not self._has_scalar_value(value):
                return None
            features[requirement.name] = value
        for requirement in self._eligible_optional_requirements(envelope):
            status = envelope.field_status(requirement.name)
            if status is None:
                raise RuntimeError("feature structure was not validated")
            value = row[requirement.name]
            if self._status_is_usable(
                status,
                allow_degraded=requirement.allow_degraded,
            ) and self._has_scalar_value(value):
                features[requirement.name] = value
        return MappingProxyType(features)

    def _candidate_state(
        self,
        connection: sqlite3.Connection,
        candidate_id: str,
        observed_at: datetime,
    ) -> StrategyCandidateState:
        row = connection.execute(
            "SELECT * FROM candidate_state WHERE candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return StrategyCandidateState(
                strategy_spec_fingerprint=self.spec.spec_fingerprint,
                candidate_id=candidate_id,
                state=self.spec.initial_state,
                last_feature_sequence=-1,
                updated_at=observed_at,
            )
        return self._state_from_row(row)

    def _write_state(
        self,
        connection: sqlite3.Connection,
        state: StrategyCandidateState,
    ) -> None:
        connection.execute(
            """
            INSERT INTO candidate_state(
                candidate_id, state, last_feature_sequence,
                last_feature_batch_id, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(candidate_id) DO UPDATE SET
                state = excluded.state,
                last_feature_sequence = excluded.last_feature_sequence,
                last_feature_batch_id = excluded.last_feature_batch_id,
                updated_at = excluded.updated_at
            """,
            (
                state.candidate_id,
                state.state.value,
                state.last_feature_sequence,
                state.last_feature_batch_id,
                state.updated_at.isoformat(),
            ),
        )

    def candidate_state(self, candidate_id: str) -> StrategyCandidateState | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM candidate_state WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchone()
        return None if row is None else self._state_from_row(row)

    def signals_after(self, *, sequence: int) -> tuple[RunnerSignalRecord, ...]:
        if sequence < 0:
            raise ValueError("signal sequence must be nonnegative")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT sequence, payload_json FROM runner_signal
                WHERE sequence > ? ORDER BY sequence
                """,
                (sequence,),
            ).fetchall()
        return tuple(
            RunnerSignalRecord(
                sequence=row["sequence"],
                signal=json.loads(row["payload_json"]),
            )
            for row in rows
        )

    def last_batch_sequence(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT max(feature_sequence) AS value FROM processed_batch"
            ).fetchone()
        return -1 if row is None or row["value"] is None else int(row["value"])

    def signal_high_watermark(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT max(sequence) AS value FROM runner_signal").fetchone()
        return 0 if row is None or row["value"] is None else int(row["value"])

    def _state_from_row(self, row: sqlite3.Row) -> StrategyCandidateState:
        return StrategyCandidateState(
            strategy_spec_fingerprint=self.spec.spec_fingerprint,
            candidate_id=row["candidate_id"],
            state=StrategyLifecycleState(row["state"]),
            last_feature_sequence=row["last_feature_sequence"],
            last_feature_batch_id=row["last_feature_batch_id"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )


__all__ = [
    "RunnerSignalRecord",
    "StrategyBatchConflictError",
    "StrategyBatchResult",
    "StrategyCandidateState",
    "StrategyDecision",
    "StrategyEvaluator",
    "StrategyRunnerStore",
    "canonical_feature_payload",
]
