"""Deterministic single-strategy runner with an immutable signal spool."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, Protocol
from zoneinfo import ZoneInfo

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
    FeatureContract,
    FeatureFieldStatus,
    FeatureInstanceEnvelope,
    FeatureRequirement,
    LateFeaturePolicy,
    MissingFeaturePolicy,
)
from rquant.feature_spool import FeatureSessionCloseMarker
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    CompletionAttestationSigner,
    ShadowSourceCompletionReceipt,
    shadow_completion_receipt_body_sha256,
)
from rquant.signal_contracts import (
    SignalAction,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)
from rquant.strategy_candidate_snapshot import candidate_occurrence_id
from rquant.strategy_spec import StrategyLifecycleState, StrategySpec

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_SESSION_CLOSE_RECEIPT_BYTES = 64 * 1024
_MAX_PROTOCOL_JSON_DEPTH = 64
_MAX_PROTOCOL_JSON_NODES = 100_000
_MAX_RUNNER_SESSION_RECORDS = 100_000
_MAX_RUNNER_SESSION_RAW_BYTES = 128 * 1024 * 1024
_CANDIDATE_METADATA_COLUMNS = (
    "candidate_occurrence_id",
    "candidate_effective_trade_date",
    "candidate_variant",
    "candidate_generation_sha256",
    "candidate_snapshot_schema_version",
)
_CANDIDATE_STATE_PRE_HIGH_WATERMARK_SCHEMA = (
    (0, "occurrence_id", "TEXT", 1, 1),
    (1, "candidate_id", "TEXT", 1, 0),
    (2, "candidate_effective_trade_date", "TEXT", 0, 0),
    (3, "candidate_variant", "TEXT", 0, 0),
    (4, "candidate_generation_sha256", "TEXT", 0, 0),
    (5, "candidate_snapshot_schema_version", "INTEGER", 0, 0),
    (6, "state", "TEXT", 1, 0),
    (7, "last_feature_sequence", "INTEGER", 1, 0),
    (8, "last_feature_batch_id", "TEXT", 0, 0),
    (9, "updated_at", "TEXT", 1, 0),
)
_CANDIDATE_STATE_SCHEMA = _CANDIDATE_STATE_PRE_HIGH_WATERMARK_SCHEMA + (
    (10, "eligible_high_price_raw", "REAL", 0, 0),
    (11, "eligible_high_source_event_time", "TEXT", 0, 0),
    (12, "eligible_high_available_at", "TEXT", 0, 0),
)
_LEGACY_CANDIDATE_STATE_SCHEMA = (
    (0, "candidate_id", "TEXT", 0, 1),
    (1, "state", "TEXT", 1, 0),
    (2, "last_feature_sequence", "INTEGER", 1, 0),
    (3, "last_feature_batch_id", "TEXT", 0, 0),
    (4, "updated_at", "TEXT", 1, 0),
)
_PROCESSED_BATCH_BASE_SCHEMA = {
    "feature_sequence": ("INTEGER", 0, 1),
    "feature_batch_id": ("TEXT", 1, 0),
    "envelope_fingerprint": ("TEXT", 1, 0),
    "feature_payload_hash": ("TEXT", 1, 0),
    "dataset_snapshot_id": ("TEXT", 1, 0),
    "event_time": ("TEXT", 1, 0),
    "available_at": ("TEXT", 1, 0),
    "observed_at": ("TEXT", 1, 0),
    "result_json": ("TEXT", 1, 0),
}
_PROCESSED_BATCH_RECEIPT_SCHEMA = {
    "source_generation_id": ("TEXT", 0, 0),
    "source_sequence": ("INTEGER", 0, 0),
    "source_batch_id": ("TEXT", 0, 0),
    "source_content_hash": ("TEXT", 0, 0),
}
_RUNNER_METADATA_LEGACY_SCHEMA = (
    (0, "singleton", "INTEGER", 0, 1),
    (1, "strategy_spec_fingerprint", "TEXT", 1, 0),
    (2, "strategy_spec_json", "TEXT", 1, 0),
    (3, "evaluator_contract_fingerprint", "TEXT", 1, 0),
)
_RUNNER_METADATA_SCHEMA = (
    *_RUNNER_METADATA_LEGACY_SCHEMA,
    (4, "candidate_input_mode", "TEXT", 0, 0),
)
_RUNNER_SOURCE_IDENTITY_SCHEMA = (
    (0, "singleton", "INTEGER", 0, 1),
    (1, "source_generation_id", "TEXT", 1, 0),
)
_RUNNER_SIGNAL_LEGACY_SCHEMA = (
    (0, "sequence", "INTEGER", 0, 1),
    (1, "signal_id", "TEXT", 1, 0),
    (2, "feature_sequence", "INTEGER", 1, 0),
    (3, "payload_json", "TEXT", 1, 0),
)
_RUNNER_SIGNAL_SCHEMA = (
    (0, "sequence", "INTEGER", 0, 1),
    (1, "signal_id", "TEXT", 1, 0),
    (2, "feature_sequence", "INTEGER", 1, 0),
    (3, "candidate_id", "TEXT", 1, 0),
    (4, "action", "TEXT", 1, 0),
    (5, "entry_signal_id", "TEXT", 0, 0),
    (6, "candidate_occurrence_id", "TEXT", 0, 0),
    (7, "event_time", "TEXT", 1, 0),
    (8, "available_at", "TEXT", 1, 0),
    (9, "expires_at", "TEXT", 1, 0),
    (10, "payload_json", "TEXT", 1, 0),
)
_RUNNER_SIGNAL_LEGACY_TABLE_SQL = (
    "createtablerunner_signal("
    "sequenceintegerprimarykeyautoincrement,"
    "signal_idtextnotnullunique,"
    "feature_sequenceintegernotnull,"
    "payload_jsontextnotnull)"
)
_RUNNER_SIGNAL_TABLE_SQL = (
    "createtablerunner_signal("
    "sequenceintegerprimarykeyautoincrement,"
    "signal_idtextnotnullunique,"
    "feature_sequenceintegernotnull,"
    "candidate_idtextnotnull,"
    "actiontextnotnull,"
    "entry_signal_idtext,"
    "candidate_occurrence_idtext,"
    "event_timetextnotnull,"
    "available_attextnotnull,"
    "expires_attextnotnull,"
    "payload_jsontextnotnull)"
)
_RUNNER_SIGNAL_ENTRY_INDEX_NAME = "runner_signal_entry_lookup_idx"
_RUNNER_SIGNAL_ENTRY_INDEX_SQL = (
    "createindexrunner_signal_entry_lookup_idxonrunner_signal("
    "candidate_id,candidate_occurrence_id,action,sequencedesc)"
)
_RUNNER_SIGNAL_EXIT_INDEX_NAME = "runner_signal_exit_lookup_idx"
_RUNNER_SIGNAL_EXIT_INDEX_SQL = (
    "createindexrunner_signal_exit_lookup_idxonrunner_signal("
    "candidate_id,candidate_occurrence_id,entry_signal_id,action,available_at,sequence)"
)
_RUNNER_SESSION_CLOSE_RECEIPT_SCHEMA = (
    (0, "trade_date", "TEXT", 0, 1),
    (1, "receipt_id", "TEXT", 1, 0),
    (2, "source_id", "TEXT", 1, 0),
    (3, "signal_high_watermark", "INTEGER", 1, 0),
    (4, "payload_json", "TEXT", 1, 0),
)
_RUNNER_SESSION_CLOSE_RECEIPT_TABLE_SQL = (
    "createtablerunner_session_close_receipt("
    "trade_datetextprimarykey,"
    "receipt_idtextnotnullunique,"
    "source_idtextnotnull,"
    "signal_high_watermarkintegernotnull,"
    "payload_jsontextnotnull)"
)
_RUNNER_SESSION_SEGMENT_SCHEMA = (
    (0, "trade_date", "TEXT", 0, 1),
    (1, "runner_generation_id", "TEXT", 1, 0),
    (2, "start_after_sequence", "INTEGER", 1, 0),
    (3, "final_sequence", "INTEGER", 1, 0),
    (4, "record_count", "INTEGER", 1, 0),
    (5, "raw_bytes", "INTEGER", 1, 0),
    (6, "chain_hash", "TEXT", 1, 0),
    (7, "final_feature_sequence", "INTEGER", 1, 0),
    (8, "final_feature_batch_id", "TEXT", 1, 0),
)
_RUNNER_SESSION_SEGMENT_TABLE_SQL = (
    "createtablerunner_session_segment("
    "trade_datetextprimarykey,"
    "runner_generation_idtextnotnull,"
    "start_after_sequenceintegernotnull,"
    "final_sequenceintegernotnull,"
    "record_countintegernotnull,"
    "raw_bytesintegernotnull,"
    "chain_hashtextnotnull,"
    "final_feature_sequenceintegernotnull,"
    "final_feature_batch_idtextnotnull)"
)
_SINGLETON_CHECK_SQL = "check(singleton=1)"
_CANDIDATE_INPUT_MODE_CHECK_SQL = "check(candidate_input_modein('flat','occurrence'))"
_SOURCE_SEQUENCE_INDEX_NAME = "processed_batch_source_sequence_uq"
_SOURCE_SEQUENCE_INDEX_SQL = (
    "createuniqueindexprocessed_batch_source_sequence_uq"
    "onprocessed_batch(source_sequence)wheresource_sequenceisnotnull"
)
_EXECUTION_LIFECYCLE_FEATURES = frozenset(
    {
        "entry_fill_status",
        "exit_execution_status",
        "position_closed",
        "holding_trading_sessions",
        "position_sellable",
        "entry_price_raw",
        "structure_stop_price_raw",
        "eligible_high_price_raw",
        "remaining_position_fraction",
    }
)


class StrategyBatchConflictError(RuntimeError):
    """A runner input sequence was missing or reused with different evidence."""


def _decode_session_close_receipt(raw: bytes) -> ShadowSourceCompletionReceipt:
    if len(raw) > _MAX_SESSION_CLOSE_RECEIPT_BYTES:
        raise ValueError("runner session close receipt exceeds the byte budget")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("runner session close receipt is not valid UTF-8") from exc
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
            if depth > _MAX_PROTOCOL_JSON_DEPTH:
                raise ValueError("runner session close receipt exceeds the JSON depth budget")
        elif character in "]}":
            depth -= 1
        elif character not in " \t\r\n,:":
            in_atom = True
            nodes += 1
        if nodes > _MAX_PROTOCOL_JSON_NODES:
            raise ValueError("runner session close receipt exceeds the JSON node budget")
    try:
        decoded = json.loads(text)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("runner session close receipt payload is invalid") from exc
    stack = [decoded]
    nodes = 0
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > _MAX_PROTOCOL_JSON_NODES:
            raise ValueError("runner session close receipt exceeds the JSON node budget")
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    try:
        receipt = ShadowSourceCompletionReceipt.model_validate(decoded)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("runner session close receipt payload is invalid") from exc
    if raw != _json_payload(receipt).encode("utf-8"):
        raise ValueError("runner session close receipt payload is not canonical")
    return receipt


class StrategySourceBatchReceipt(RuntimeContractModel):
    """Exact durable evidence for one common feature-spool source batch."""

    source_generation_id: Sha256
    source_sequence: int = Field(ge=0)
    source_batch_id: str = Field(min_length=1)
    source_content_hash: Sha256


class RunnerSignalRouteDrainEvidence(RuntimeContractModel):
    """Durable signal-router state proving one runner prefix has no backlog."""

    evidence_id: Sha256 | None = None
    source_id: str = Field(min_length=1)
    runner_generation_id: Sha256
    strategy_spec_fingerprint: Sha256
    signal_authority_generation_id: Sha256
    routing_policy_fingerprint: Sha256
    trade_date: date
    segment_start_sequence: int = Field(ge=0)
    segment_record_count: int = Field(ge=0)
    segment_raw_bytes: int = Field(ge=0)
    segment_chain_hash: Sha256
    observed_high_watermark: int = Field(ge=0)
    routed_through_sequence: int = Field(ge=0)
    last_sequence: int = Field(ge=0)
    route_receipts_sha256: Sha256
    observed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_drain(self) -> RunnerSignalRouteDrainEvidence:
        if self.routed_through_sequence < self.segment_start_sequence:
            raise ValueError("route segment range is invalid")
        if self.segment_record_count != (
            self.routed_through_sequence - self.segment_start_sequence
        ):
            raise ValueError("route segment record count does not match its range")
        if self.observed_high_watermark < self.routed_through_sequence:
            raise ValueError("route authority did not observe the requested runner prefix")
        if self.last_sequence < self.routed_through_sequence:
            raise ValueError("signal route authority still has a runner backlog")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"evidence_id"}))
        if self.evidence_id is None:
            object.__setattr__(self, "evidence_id", expected)
        elif self.evidence_id != expected:
            raise ValueError("route drain evidence id does not match content")
        return self


class StrategyLifecycleFeatureSource(Protocol):
    def resolve(
        self,
        *,
        candidate_id: str,
        entry_signal: SignalEnvelopeFamily,
        exit_signals: tuple[SignalEnvelopeFamily, ...],
        decision_cutoff: datetime,
        market_features: Mapping[str, object],
        market_feature_statuses: Mapping[str, FeatureFieldStatus],
        previous_eligible_high_price_raw: float | None,
        previous_high_source_event_time: datetime | None,
        previous_high_available_at: datetime | None,
    ) -> FeatureInstanceEnvelope: ...


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
    candidate_occurrence_id: Sha256 | None = None
    candidate_effective_trade_date: date | None = None
    candidate_variant: str | None = Field(default=None, min_length=1)
    candidate_generation_sha256: Sha256 | None = None
    candidate_snapshot_schema_version: Literal[1, 2, 3] | None = None
    state: StrategyLifecycleState
    last_feature_sequence: int = Field(ge=-1)
    last_feature_batch_id: str | None = None
    updated_at: AwareUtcDatetime
    eligible_high_price_raw: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    eligible_high_source_event_time: AwareUtcDatetime | None = None
    eligible_high_available_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_candidate_metadata(self) -> StrategyCandidateState:
        values = (
            self.candidate_occurrence_id,
            self.candidate_effective_trade_date,
            self.candidate_variant,
            self.candidate_generation_sha256,
            self.candidate_snapshot_schema_version,
        )
        if any(value is None for value in values) and any(value is not None for value in values):
            raise ValueError("candidate occurrence metadata must be all present or all absent")
        high_watermark = (
            self.eligible_high_price_raw,
            self.eligible_high_source_event_time,
            self.eligible_high_available_at,
        )
        if any(value is None for value in high_watermark) and any(
            value is not None for value in high_watermark
        ):
            raise ValueError("eligible high watermark evidence must be all present or all absent")
        if (
            self.eligible_high_source_event_time is not None
            and self.eligible_high_available_at is not None
            and (
                self.eligible_high_source_event_time > self.eligible_high_available_at
                or self.eligible_high_available_at > self.updated_at
            )
        ):
            raise ValueError("eligible high watermark is not point-in-time visible")
        return self

    @property
    def state_key(self) -> str:
        return self.candidate_occurrence_id or self.candidate_id

    @property
    def runner_transition_metadata(self) -> dict[str, JsonValue]:
        if self.candidate_occurrence_id is None:
            return {}
        if (
            self.candidate_effective_trade_date is None
            or self.candidate_variant is None
            or self.candidate_generation_sha256 is None
            or self.candidate_snapshot_schema_version is None
        ):
            raise RuntimeError("validated candidate occurrence metadata is incomplete")
        return {
            "candidate_occurrence_id": self.candidate_occurrence_id,
            "candidate_effective_trade_date": self.candidate_effective_trade_date.isoformat(),
            "candidate_variant": self.candidate_variant,
            "candidate_generation_sha256": self.candidate_generation_sha256,
            "candidate_snapshot_schema_version": self.candidate_snapshot_schema_version,
        }


class RunnerSignalRecord(RuntimeContractModel):
    sequence: int = Field(ge=1)
    signal: SignalEnvelopeFamily


class _RunnerSessionSegment(RuntimeContractModel):
    trade_date: date
    runner_generation_id: Sha256
    start_after_sequence: int = Field(ge=0)
    final_sequence: int = Field(ge=0)
    record_count: int = Field(ge=0)
    raw_bytes: int = Field(ge=0)
    chain_hash: Sha256
    final_feature_sequence: int = Field(ge=0)
    final_feature_batch_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_range(self) -> _RunnerSessionSegment:
        if self.final_sequence < self.start_after_sequence:
            raise ValueError("runner session segment sequence range is invalid")
        if self.record_count != self.final_sequence - self.start_after_sequence:
            raise ValueError("runner session segment record count does not match its range")
        return self


def _runner_segment_seed(
    *,
    trade_date: date,
    runner_generation_id: str,
    strategy_spec_fingerprint: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "runner-session-segment-chain/v1",
            "trade_date": trade_date,
            "runner_generation_id": runner_generation_id,
            "strategy_spec_fingerprint": strategy_spec_fingerprint,
        }
    )


def _advance_runner_segment_chain(previous: str, record: RunnerSignalRecord) -> str:
    payload = _canonical_json_bytes(record.model_dump(mode="json"))
    return canonical_sha256(
        {
            "previous": previous,
            "record_sha256": hashlib.sha256(payload).hexdigest(),
            "record_bytes": len(payload),
            "sequence": record.sequence,
        }
    )


def _runner_session_raw_input_id(
    *,
    source_id: str,
    runner_generation_id: str,
    strategy_spec_fingerprint: str,
    segment: _RunnerSessionSegment,
) -> str:
    return canonical_sha256(
        {
            "contract": "shadow-runner-session-raw-input/v3",
            "descriptor": {
                "source_id": source_id,
                "generation_id": runner_generation_id,
                "strategy_spec_fingerprint": strategy_spec_fingerprint,
                "first_sequence": segment.start_after_sequence + 1,
                "high_watermark": segment.final_sequence,
                "trade_date": segment.trade_date,
            },
            "records_chain_hash": segment.chain_hash,
            "record_count": segment.record_count,
            "raw_bytes": segment.raw_bytes,
        }
    )


def runner_signal_raw_input_id(
    *,
    source_id: str,
    runner_generation_id: str,
    strategy_spec_fingerprint: str,
    high_watermark: int,
    records: Sequence[RunnerSignalRecord],
) -> str:
    """Hash the frozen source descriptor and every raw runner record in its prefix."""

    if not source_id.strip():
        raise ValueError("runner source_id cannot be empty")
    _validate_sha256(runner_generation_id, label="runner_generation_id")
    _validate_sha256(strategy_spec_fingerprint, label="strategy_spec_fingerprint")
    if isinstance(high_watermark, bool) or not isinstance(high_watermark, int):
        raise ValueError("runner high watermark must be an integer")
    if high_watermark < 0:
        raise ValueError("runner high watermark must be nonnegative")
    if len(records) != high_watermark:
        raise ValueError("runner raw record prefix does not match its high watermark")
    digest = hashlib.sha256()
    consumed = 0
    for expected, record in enumerate(records, start=1):
        verified = RunnerSignalRecord.model_validate(record)
        if verified.sequence != expected:
            raise ValueError("runner raw record prefix has a sequence gap")
        payload = _canonical_json_bytes(verified.model_dump(mode="json"))
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        consumed += len(payload)
    return canonical_sha256(
        {
            "contract": "shadow-runner-raw-input/v2",
            "descriptor": {
                "source_id": source_id,
                "generation_id": runner_generation_id,
                "strategy_spec_fingerprint": strategy_spec_fingerprint,
                "first_sequence": 1,
                "high_watermark": high_watermark,
            },
            "records_sha256": digest.hexdigest(),
            "record_count": len(records),
            "raw_bytes": consumed,
        }
    )


class StrategyBatchResult(RuntimeContractModel):
    feature_batch_id: str = Field(min_length=1)
    feature_sequence: int = Field(ge=0)
    processed_candidates: int = Field(ge=0)
    transitioned_candidates: int = Field(ge=0)
    skipped_candidates: int = Field(ge=0)
    signals: tuple[RunnerSignalRecord, ...]
    lifecycle_feature_fingerprints: Mapping[str, Sha256] = Field(default_factory=dict)

    @field_validator("lifecycle_feature_fingerprints")
    @classmethod
    def freeze_lifecycle_fingerprints(
        cls,
        values: Mapping[str, Sha256],
    ) -> Mapping[str, Sha256]:
        if any(not key for key in values):
            raise ValueError("lifecycle feature fingerprint keys cannot be empty")
        return MappingProxyType(dict(sorted(values.items())))

    @field_serializer("lifecycle_feature_fingerprints")
    def serialize_lifecycle_fingerprints(
        self,
        values: Mapping[str, Sha256],
    ) -> dict[str, Sha256]:
        return dict(values)

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


def _utc_iso(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat().replace("+00:00", "Z")


def _signal_index_values(
    signal: SignalEnvelopeFamily,
) -> tuple[str, str, str | None, str | None, str, str, str]:
    transition = signal.evidence.get("runner_transition")
    occurrence_id: str | None = None
    if transition is not None:
        if not isinstance(transition, Mapping):
            raise ValueError("runner_signal transition evidence must be a mapping")
        raw_occurrence_id = transition.get("candidate_occurrence_id")
        if raw_occurrence_id is not None:
            occurrence_id = str(raw_occurrence_id)
            _validate_sha256(occurrence_id, label="runner_signal candidate_occurrence_id")
    raw_entry_signal_id = signal.evidence.get("entry_signal_id")
    entry_signal_id = None if raw_entry_signal_id is None else str(raw_entry_signal_id)
    if signal.action in {SignalAction.REDUCE, SignalAction.S_INTENT}:
        if entry_signal_id is None:
            raise ValueError("runner_signal exit requires entry_signal_id")
        _validate_sha256(entry_signal_id, label="runner_signal entry_signal_id")
    elif entry_signal_id is not None:
        raise ValueError("runner_signal non-exit cannot carry entry_signal_id")
    return (
        signal.candidate_id,
        signal.action.value,
        entry_signal_id,
        occurrence_id,
        _utc_iso(signal.event_time),
        _utc_iso(signal.available_at),
        _utc_iso(signal.expires_at),
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


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_supplied_feature_payload(
    feature_payload: bytes | str,
    *,
    envelope: FeatureBatchEnvelope,
    canonical_frame_payload: bytes,
) -> tuple[bytes, str]:
    if isinstance(feature_payload, str):
        try:
            supplied = feature_payload.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise StrategyBatchConflictError("supplied feature payload is not valid UTF-8") from exc
    elif isinstance(feature_payload, bytes):
        supplied = feature_payload
    else:
        raise StrategyBatchConflictError("supplied feature payload must be bytes or str")
    try:
        text = supplied.decode("utf-8")
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except UnicodeDecodeError as exc:
        raise StrategyBatchConflictError("supplied feature payload is not valid UTF-8") from exc
    except (json.JSONDecodeError, ValueError) as exc:
        raise StrategyBatchConflictError(f"invalid supplied feature payload: {exc}") from exc
    if not isinstance(decoded, dict):
        raise StrategyBatchConflictError("supplied feature payload must be a JSON object")
    try:
        canonical_supplied = _canonical_json_bytes(decoded)
    except (TypeError, ValueError) as exc:
        raise StrategyBatchConflictError(
            f"supplied feature payload contains an invalid JSON value: {exc}"
        ) from exc
    if supplied != canonical_supplied:
        raise StrategyBatchConflictError("supplied feature payload must use canonical JSON bytes")
    schema_version = decoded.get("schema_version")
    if type(schema_version) is not int or schema_version != envelope.schema_version:
        raise StrategyBatchConflictError(
            "supplied feature payload schema_version does not match envelope"
        )
    supplied_rows = decoded.get("rows")
    if not isinstance(supplied_rows, list):
        raise StrategyBatchConflictError("supplied feature payload requires a rows list")
    expected_rows = json.loads(canonical_frame_payload)["rows"]
    if _canonical_json_bytes(supplied_rows) != _canonical_json_bytes(expected_rows):
        raise StrategyBatchConflictError(
            "supplied feature payload rows do not exactly match the DataFrame"
        )
    payload_hash = hashlib.sha256(supplied).hexdigest()
    if payload_hash != envelope.content_hash:
        raise StrategyBatchConflictError(
            "supplied feature payload hash does not match envelope content_hash"
        )
    return supplied, payload_hash


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
        feature_contract: FeatureContract | None = None,
        lifecycle_feature_source: StrategyLifecycleFeatureSource | None = None,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.spec = spec
        if feature_contract is not None:
            if not isinstance(feature_contract, FeatureContract):
                raise TypeError("feature_contract must be a FeatureContract")
            if feature_contract.contract_id != spec.feature_contract_id:
                raise ValueError("feature contract id does not match strategy spec")
            if feature_contract.version < spec.min_feature_contract_version:
                raise ValueError("feature contract version is below strategy minimum")
            if feature_contract.producer_commit != spec.producer_commit:
                raise ValueError("feature contract producer commit does not match strategy spec")
        self.feature_contract = feature_contract
        self.lifecycle_feature_source = lifecycle_feature_source
        self._feature_definitions = (
            {}
            if feature_contract is None
            else {feature.name: feature for feature in feature_contract.features}
        )
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
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._audit_runner_metadata_schema(connection)
                self._audit_runner_source_identity_schema(connection)
                runner_signal_schema_state = self._audit_runner_signal_schema(connection)
                self._audit_runner_session_close_receipt_schema(connection)
                self._audit_runner_session_segment_schema(connection)
                existing = self._read_persisted_runner_identity(connection)
                if existing is not None:
                    if existing["strategy_spec_fingerprint"] != self.spec.spec_fingerprint:
                        raise ValueError("strategy spec does not match persisted runner identity")
                    if (
                        existing["evaluator_contract_fingerprint"]
                        != self.evaluator_contract_fingerprint
                    ):
                        raise ValueError(
                            "evaluator contract does not match persisted runner identity"
                        )
                source_generation_id = self._read_persisted_source_identity(connection)
                if source_generation_id is None:
                    source_generation_id = secrets.token_hex(32)
                _validate_sha256(source_generation_id, label="source_generation_id")

                self._ensure_runner_metadata_schema(connection)
                self._ensure_candidate_state_schema(connection)
                self._ensure_processed_batch_schema(connection)
                self._ensure_runner_signal_schema(
                    connection,
                    state=runner_signal_schema_state,
                )
                connection.execute(
                    """
                CREATE TABLE IF NOT EXISTS runner_metadata (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    strategy_spec_fingerprint TEXT NOT NULL,
                    strategy_spec_json TEXT NOT NULL,
                    evaluator_contract_fingerprint TEXT NOT NULL,
                    candidate_input_mode TEXT
                        CHECK(candidate_input_mode IN ('flat', 'occurrence'))
                )
                """
                )
                connection.execute(
                    """
                CREATE TABLE IF NOT EXISTS candidate_state (
                    occurrence_id TEXT NOT NULL PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    candidate_effective_trade_date TEXT,
                    candidate_variant TEXT,
                    candidate_generation_sha256 TEXT,
                    candidate_snapshot_schema_version INTEGER,
                    state TEXT NOT NULL,
                    last_feature_sequence INTEGER NOT NULL,
                    last_feature_batch_id TEXT,
                    updated_at TEXT NOT NULL,
                    eligible_high_price_raw REAL,
                    eligible_high_source_event_time TEXT,
                    eligible_high_available_at TEXT
                )
                """
                )
                connection.execute(
                    """
                CREATE TABLE IF NOT EXISTS processed_batch (
                    feature_sequence INTEGER PRIMARY KEY,
                    feature_batch_id TEXT NOT NULL UNIQUE,
                    envelope_fingerprint TEXT NOT NULL,
                    feature_payload_hash TEXT NOT NULL,
                    dataset_snapshot_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    source_generation_id TEXT,
                    source_sequence INTEGER,
                    source_batch_id TEXT,
                    source_content_hash TEXT,
                    result_json TEXT NOT NULL
                )
                """
                )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS processed_batch_source_sequence_uq
                    ON processed_batch(source_sequence)
                    WHERE source_sequence IS NOT NULL
                    """
                )
                connection.execute(
                    """
                CREATE TABLE IF NOT EXISTS runner_source_identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    source_generation_id TEXT NOT NULL
                )
                """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runner_session_close_receipt (
                        trade_date TEXT PRIMARY KEY,
                        receipt_id TEXT NOT NULL UNIQUE,
                        source_id TEXT NOT NULL,
                        signal_high_watermark INTEGER NOT NULL,
                        payload_json TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runner_session_segment (
                        trade_date TEXT PRIMARY KEY,
                        runner_generation_id TEXT NOT NULL,
                        start_after_sequence INTEGER NOT NULL,
                        final_sequence INTEGER NOT NULL,
                        record_count INTEGER NOT NULL,
                        raw_bytes INTEGER NOT NULL,
                        chain_hash TEXT NOT NULL,
                        final_feature_sequence INTEGER NOT NULL,
                        final_feature_batch_id TEXT NOT NULL
                    )
                    """
                )
                if self._audit_runner_metadata_schema(connection) != "current":
                    raise ValueError("runner_metadata schema did not upgrade to current")
                self._audit_runner_source_identity_schema(connection)
                if self._audit_runner_signal_schema(connection) != "current":
                    raise ValueError("runner_signal schema did not upgrade to current")
                self._audit_runner_session_close_receipt_schema(connection)
                self._audit_runner_session_segment_schema(connection)
                if self._processed_batch_schema_state(connection) != "current":
                    raise ValueError("processed_batch source receipt schema is incomplete")
                self._audit_processed_batch_constraints(
                    connection,
                    require_source_index=True,
                )
                if self._read_persisted_source_identity(connection) is None:
                    connection.execute(
                        """
                        INSERT INTO runner_source_identity(singleton, source_generation_id)
                        VALUES (1, ?)
                        """,
                        (source_generation_id,),
                    )
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
                connection.commit()
                self.source_generation_id = source_generation_id
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (name,),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _table_schema(
        connection: sqlite3.Connection,
        name: str,
    ) -> tuple[tuple[int, str, str, int, int], ...]:
        return tuple(
            (
                int(row["cid"]),
                str(row["name"]),
                str(row["type"]).upper(),
                int(row["notnull"]),
                int(row["pk"]),
            )
            for row in connection.execute(f"PRAGMA table_info({name})").fetchall()
        )

    @staticmethod
    def _canonical_schema_sql(sql: str) -> str:
        canonical = re.sub(r"\s+", "", sql).lower()
        for token in ('"', "`", "[", "]"):
            canonical = canonical.replace(token, "")
        return canonical.replace("ifnotexists", "").removesuffix(";")

    @classmethod
    def _schema_sql(
        cls,
        connection: sqlite3.Connection,
        *,
        object_type: str,
        name: str,
    ) -> str:
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?",
            (object_type, name),
        ).fetchone()
        if row is None or row["sql"] is None:
            raise ValueError(f"{name} schema SQL is unavailable")
        return cls._canonical_schema_sql(str(row["sql"]))

    @classmethod
    def _audit_singleton_rows(
        cls,
        connection: sqlite3.Connection,
        table: str,
    ) -> None:
        rows = connection.execute(
            f"SELECT singleton, count(*) AS n FROM {table} GROUP BY singleton"
        ).fetchall()
        if len(rows) > 1 or any(row["singleton"] != 1 or int(row["n"]) != 1 for row in rows):
            raise ValueError(f"{table} contains invalid or duplicate singleton rows")

    @classmethod
    def _audit_runner_metadata_schema(
        cls,
        connection: sqlite3.Connection,
    ) -> Literal["legacy", "current"] | None:
        if not cls._table_exists(connection, "runner_metadata"):
            return None
        schema = cls._table_schema(connection, "runner_metadata")
        if schema == _RUNNER_METADATA_LEGACY_SCHEMA:
            state: Literal["legacy", "current"] = "legacy"
            expected_checks = (_SINGLETON_CHECK_SQL,)
        elif schema == _RUNNER_METADATA_SCHEMA:
            state = "current"
            expected_checks = (
                _SINGLETON_CHECK_SQL,
                _CANDIDATE_INPUT_MODE_CHECK_SQL,
            )
        else:
            raise ValueError("runner_metadata schema is unsupported")
        sql = cls._schema_sql(
            connection,
            object_type="table",
            name="runner_metadata",
        )
        if sql.count("check(") != len(expected_checks) or any(
            check not in sql for check in expected_checks
        ):
            raise ValueError("runner_metadata schema CHECK constraints are unsupported")
        cls._audit_singleton_rows(connection, "runner_metadata")
        return state

    @classmethod
    def _audit_runner_source_identity_schema(
        cls,
        connection: sqlite3.Connection,
    ) -> None:
        if not cls._table_exists(connection, "runner_source_identity"):
            return
        if cls._table_schema(connection, "runner_source_identity") != (
            _RUNNER_SOURCE_IDENTITY_SCHEMA
        ):
            raise ValueError("runner_source_identity schema is unsupported")
        sql = cls._schema_sql(
            connection,
            object_type="table",
            name="runner_source_identity",
        )
        if sql.count("check(") != 1 or _SINGLETON_CHECK_SQL not in sql:
            raise ValueError("runner_source_identity schema CHECK is unsupported")
        cls._audit_singleton_rows(connection, "runner_source_identity")

    @classmethod
    def _unique_constraint_columns(
        cls,
        connection: sqlite3.Connection,
        table: str,
    ) -> set[tuple[str, ...]]:
        constraints: set[tuple[str, ...]] = set()
        for row in connection.execute(f"PRAGMA index_list({table})").fetchall():
            if int(row["unique"]) != 1 or str(row["origin"]) != "u" or int(row["partial"]):
                continue
            index_name = str(row["name"]).replace("'", "''")
            columns = tuple(
                str(item["name"])
                for item in connection.execute(f"PRAGMA index_info('{index_name}')").fetchall()
            )
            constraints.add(columns)
        return constraints

    @classmethod
    def _audit_runner_session_close_receipt_schema(
        cls,
        connection: sqlite3.Connection,
    ) -> None:
        if not cls._table_exists(connection, "runner_session_close_receipt"):
            return
        if cls._table_schema(connection, "runner_session_close_receipt") != (
            _RUNNER_SESSION_CLOSE_RECEIPT_SCHEMA
        ):
            raise ValueError("runner session close receipt schema is unsupported")
        if (
            cls._schema_sql(
                connection,
                object_type="table",
                name="runner_session_close_receipt",
            )
            != _RUNNER_SESSION_CLOSE_RECEIPT_TABLE_SQL
        ):
            raise ValueError("runner session close receipt canonical DDL is unsupported")
        if ("receipt_id",) not in cls._unique_constraint_columns(
            connection,
            "runner_session_close_receipt",
        ):
            raise ValueError("runner session close receipt id must be unique")
        preflight = connection.execute(
            "SELECT COALESCE(max(length(CAST(payload_json AS BLOB))), 0) "
            "FROM runner_session_close_receipt"
        ).fetchone()
        if preflight is None or int(preflight[0]) > _MAX_SESSION_CLOSE_RECEIPT_BYTES:
            raise ValueError("runner session close receipt exceeds the byte budget")
        for row in connection.execute(
            "SELECT trade_date, receipt_id, source_id, signal_high_watermark, "
            "CAST(payload_json AS BLOB) AS payload_bytes "
            "FROM runner_session_close_receipt ORDER BY trade_date"
        ):
            cls._session_close_receipt_from_row(row)

    @classmethod
    def _audit_runner_session_segment_schema(cls, connection: sqlite3.Connection) -> None:
        if not cls._table_exists(connection, "runner_session_segment"):
            return
        if cls._table_schema(connection, "runner_session_segment") != (
            _RUNNER_SESSION_SEGMENT_SCHEMA
        ):
            raise ValueError("runner session segment schema is unsupported")
        if (
            cls._schema_sql(
                connection,
                object_type="table",
                name="runner_session_segment",
            )
            != _RUNNER_SESSION_SEGMENT_TABLE_SQL
        ):
            raise ValueError("runner session segment canonical DDL is unsupported")
        for row in connection.execute("SELECT * FROM runner_session_segment ORDER BY trade_date"):
            cls._runner_session_segment_from_row(row)

    @staticmethod
    def _runner_session_segment_from_row(row: sqlite3.Row) -> _RunnerSessionSegment:
        return _RunnerSessionSegment(
            trade_date=date.fromisoformat(str(row["trade_date"])),
            runner_generation_id=str(row["runner_generation_id"]),
            start_after_sequence=int(row["start_after_sequence"]),
            final_sequence=int(row["final_sequence"]),
            record_count=int(row["record_count"]),
            raw_bytes=int(row["raw_bytes"]),
            chain_hash=str(row["chain_hash"]),
            final_feature_sequence=int(row["final_feature_sequence"]),
            final_feature_batch_id=str(row["final_feature_batch_id"]),
        )

    @staticmethod
    def _session_close_receipt_from_row(
        row: sqlite3.Row,
    ) -> ShadowSourceCompletionReceipt:
        raw_value = row["payload_bytes"]
        if not isinstance(raw_value, bytes):
            raise ValueError("runner session close receipt payload is not bytes")
        receipt = _decode_session_close_receipt(raw_value)
        if (
            row["trade_date"] != receipt.trade_date.isoformat()
            or row["receipt_id"] != receipt.receipt_id
            or row["source_id"] != receipt.source_id
            or int(row["signal_high_watermark"]) != receipt.high_watermark
        ):
            raise ValueError("runner session close receipt identity does not match payload")
        return receipt

    @classmethod
    def _audit_runner_signal_schema(
        cls,
        connection: sqlite3.Connection,
    ) -> Literal["legacy", "current"] | None:
        if not cls._table_exists(connection, "runner_signal"):
            return None
        schema = cls._table_schema(connection, "runner_signal")
        if schema not in {_RUNNER_SIGNAL_LEGACY_SCHEMA, _RUNNER_SIGNAL_SCHEMA}:
            raise ValueError("runner_signal schema is unsupported")
        if ("signal_id",) not in cls._unique_constraint_columns(connection, "runner_signal"):
            raise ValueError("runner_signal requires a signal_id UNIQUE constraint")
        sql = cls._schema_sql(
            connection,
            object_type="table",
            name="runner_signal",
        )
        if schema == _RUNNER_SIGNAL_LEGACY_SCHEMA:
            if sql != _RUNNER_SIGNAL_LEGACY_TABLE_SQL:
                raise ValueError("runner_signal canonical DDL is unsupported")
            return "legacy"
        if sql != _RUNNER_SIGNAL_TABLE_SQL:
            raise ValueError("runner_signal canonical DDL is unsupported")
        expected_indexes = {
            _RUNNER_SIGNAL_ENTRY_INDEX_NAME: _RUNNER_SIGNAL_ENTRY_INDEX_SQL,
            _RUNNER_SIGNAL_EXIT_INDEX_NAME: _RUNNER_SIGNAL_EXIT_INDEX_SQL,
        }
        for name, expected_sql in expected_indexes.items():
            if (
                cls._schema_sql(
                    connection,
                    object_type="index",
                    name=name,
                )
                != expected_sql
            ):
                raise ValueError(f"runner_signal index {name} is unsupported")
        for row in connection.execute("SELECT * FROM runner_signal ORDER BY sequence"):
            cls._runner_signal_from_row(row)
        return "current"

    @staticmethod
    def _runner_signal_from_row(row: sqlite3.Row) -> SignalEnvelopeFamily:
        try:
            signal = parse_signal_envelope(row["payload_json"])
        except (TypeError, ValueError) as exc:
            raise ValueError("runner_signal payload is invalid") from exc
        indexed = _signal_index_values(signal)
        actual = (
            str(row["candidate_id"]),
            str(row["action"]),
            None if row["entry_signal_id"] is None else str(row["entry_signal_id"]),
            (
                None
                if row["candidate_occurrence_id"] is None
                else str(row["candidate_occurrence_id"])
            ),
            str(row["event_time"]),
            str(row["available_at"]),
            str(row["expires_at"]),
        )
        if (
            row["signal_id"] != signal.signal_id
            or actual != indexed
            or row["payload_json"] != _json_payload(signal)
        ):
            raise ValueError("runner_signal indexed identity does not match canonical payload")
        return signal

    @classmethod
    def _ensure_runner_signal_schema(
        cls,
        connection: sqlite3.Connection,
        *,
        state: Literal["legacy", "current"] | None,
    ) -> None:
        if state == "legacy":
            legacy_rows = connection.execute(
                "SELECT * FROM runner_signal ORDER BY sequence"
            ).fetchall()
            migrated: list[tuple[object, ...]] = []
            for row in legacy_rows:
                try:
                    signal = parse_signal_envelope(row["payload_json"])
                except (TypeError, ValueError) as exc:
                    raise ValueError("runner_signal legacy payload is invalid") from exc
                if row["signal_id"] != signal.signal_id or row["payload_json"] != _json_payload(
                    signal
                ):
                    raise ValueError(
                        "runner_signal legacy identity does not match canonical payload"
                    )
                migrated.append(
                    (
                        row["sequence"],
                        row["signal_id"],
                        row["feature_sequence"],
                        *_signal_index_values(signal),
                        row["payload_json"],
                    )
                )
            connection.execute("ALTER TABLE runner_signal RENAME TO runner_signal_legacy")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runner_signal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL UNIQUE,
                feature_sequence INTEGER NOT NULL,
                candidate_id TEXT NOT NULL,
                action TEXT NOT NULL,
                entry_signal_id TEXT,
                candidate_occurrence_id TEXT,
                event_time TEXT NOT NULL,
                available_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        if state == "legacy":
            connection.executemany(
                """
                INSERT INTO runner_signal(
                    sequence, signal_id, feature_sequence, candidate_id, action,
                    entry_signal_id, candidate_occurrence_id,
                    event_time, available_at, expires_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                migrated,
            )
            connection.execute("DROP TABLE runner_signal_legacy")
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS runner_signal_entry_lookup_idx
            ON runner_signal(candidate_id, candidate_occurrence_id, action, sequence DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS runner_signal_exit_lookup_idx
            ON runner_signal(
                candidate_id, candidate_occurrence_id, entry_signal_id,
                action, available_at, sequence
            )
            """
        )

    @classmethod
    def _read_persisted_runner_identity(
        cls,
        connection: sqlite3.Connection,
    ) -> sqlite3.Row | None:
        if not cls._table_exists(connection, "runner_metadata"):
            return None
        return connection.execute(
            """
            SELECT strategy_spec_fingerprint, evaluator_contract_fingerprint
            FROM runner_metadata WHERE singleton = 1
            """
        ).fetchone()

    @classmethod
    def _read_persisted_source_identity(
        cls,
        connection: sqlite3.Connection,
    ) -> str | None:
        if not cls._table_exists(connection, "runner_source_identity"):
            return None
        row = connection.execute(
            """
            SELECT source_generation_id
            FROM runner_source_identity WHERE singleton = 1
            """
        ).fetchone()
        return None if row is None else str(row["source_generation_id"])

    @staticmethod
    def _ensure_candidate_state_schema(connection: sqlite3.Connection) -> None:
        if not StrategyRunnerStore._table_exists(connection, "candidate_state"):
            return
        schema = tuple(
            (
                int(row["cid"]),
                str(row["name"]),
                str(row["type"]).upper(),
                int(row["notnull"]),
                int(row["pk"]),
            )
            for row in connection.execute("PRAGMA table_info(candidate_state)").fetchall()
        )
        if schema == _CANDIDATE_STATE_SCHEMA:
            return
        if schema == _CANDIDATE_STATE_PRE_HIGH_WATERMARK_SCHEMA:
            connection.execute(
                "ALTER TABLE candidate_state ADD COLUMN eligible_high_price_raw REAL"
            )
            connection.execute(
                "ALTER TABLE candidate_state ADD COLUMN eligible_high_source_event_time TEXT"
            )
            connection.execute(
                "ALTER TABLE candidate_state ADD COLUMN eligible_high_available_at TEXT"
            )
            return
        if schema != _LEGACY_CANDIDATE_STATE_SCHEMA:
            raise ValueError("candidate_state schema is unsupported")
        row_count = int(connection.execute("SELECT count(*) FROM candidate_state").fetchone()[0])
        if row_count:
            raise ValueError("non-empty legacy candidate_state cannot be mapped to occurrences")
        connection.execute("ALTER TABLE candidate_state RENAME TO candidate_state_legacy")
        connection.execute(
            """
            CREATE TABLE candidate_state (
                occurrence_id TEXT NOT NULL PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                candidate_effective_trade_date TEXT,
                candidate_variant TEXT,
                candidate_generation_sha256 TEXT,
                candidate_snapshot_schema_version INTEGER,
                state TEXT NOT NULL,
                last_feature_sequence INTEGER NOT NULL,
                last_feature_batch_id TEXT,
                updated_at TEXT NOT NULL,
                eligible_high_price_raw REAL,
                eligible_high_source_event_time TEXT,
                eligible_high_available_at TEXT
            )
            """
        )
        connection.execute("DROP TABLE candidate_state_legacy")

    @classmethod
    def _ensure_runner_metadata_schema(cls, connection: sqlite3.Connection) -> None:
        state = cls._audit_runner_metadata_schema(connection)
        if state is None or state == "current":
            return
        connection.execute(
            """
            ALTER TABLE runner_metadata ADD COLUMN candidate_input_mode TEXT
            CHECK(candidate_input_mode IN ('flat', 'occurrence'))
            """
        )
        if cls._audit_runner_metadata_schema(connection) != "current":
            raise ValueError("runner_metadata schema migration is incomplete")

    @classmethod
    def _processed_batch_schema_state(
        cls,
        connection: sqlite3.Connection,
    ) -> Literal["legacy", "current"] | None:
        if not cls._table_exists(connection, "processed_batch"):
            return None
        schema = {
            str(row["name"]): (
                str(row["type"]).upper(),
                int(row["notnull"]),
                int(row["pk"]),
            )
            for row in connection.execute("PRAGMA table_info(processed_batch)").fetchall()
        }
        if not all(
            schema.get(name) == expected for name, expected in _PROCESSED_BATCH_BASE_SCHEMA.items()
        ):
            raise ValueError("processed_batch base schema is unsupported")
        receipt_columns = {
            "source_generation_id": "TEXT",
            "source_sequence": "INTEGER",
            "source_batch_id": "TEXT",
            "source_content_hash": "TEXT",
        }
        allowed_columns = set(_PROCESSED_BATCH_BASE_SCHEMA) | set(receipt_columns)
        if set(schema) - allowed_columns:
            raise ValueError("processed_batch schema contains unsupported columns")
        existing_receipt = set(schema) & set(receipt_columns)
        if existing_receipt and existing_receipt != set(receipt_columns):
            raise ValueError("processed_batch source receipt schema is incomplete")
        if existing_receipt:
            if any(
                schema[name] != expected
                for name, expected in _PROCESSED_BATCH_RECEIPT_SCHEMA.items()
            ):
                raise ValueError("processed_batch source receipt schema is unsupported")
            return "current"
        return "legacy"

    @classmethod
    def _audit_processed_batch_constraints(
        cls,
        connection: sqlite3.Connection,
        *,
        require_source_index: bool,
    ) -> None:
        if ("feature_batch_id",) not in cls._unique_constraint_columns(
            connection,
            "processed_batch",
        ):
            raise ValueError("processed_batch requires a feature_batch_id UNIQUE constraint")
        indexes = {
            str(row["name"]): row
            for row in connection.execute("PRAGMA index_list(processed_batch)").fetchall()
        }
        source_index = indexes.get(_SOURCE_SEQUENCE_INDEX_NAME)
        if source_index is None:
            if require_source_index:
                raise ValueError("processed_batch source sequence index is missing")
            return
        index_name = _SOURCE_SEQUENCE_INDEX_NAME.replace("'", "''")
        columns = tuple(
            str(row["name"])
            for row in connection.execute(f"PRAGMA index_info('{index_name}')").fetchall()
        )
        sql = cls._schema_sql(
            connection,
            object_type="index",
            name=_SOURCE_SEQUENCE_INDEX_NAME,
        )
        if (
            int(source_index["unique"]) != 1
            or int(source_index["partial"]) != 1
            or columns != ("source_sequence",)
            or sql != _SOURCE_SEQUENCE_INDEX_SQL
        ):
            raise ValueError("processed_batch source sequence index is unsupported")

    @classmethod
    def _ensure_processed_batch_schema(cls, connection: sqlite3.Connection) -> None:
        state = cls._processed_batch_schema_state(connection)
        if state is None:
            return
        cls._audit_processed_batch_constraints(
            connection,
            require_source_index=False,
        )
        if state == "current":
            return
        receipt_columns = {
            "source_generation_id": "TEXT",
            "source_sequence": "INTEGER",
            "source_batch_id": "TEXT",
            "source_content_hash": "TEXT",
        }
        for name, column_type in receipt_columns.items():
            connection.execute(f"ALTER TABLE processed_batch ADD COLUMN {name} {column_type}")
        if cls._processed_batch_schema_state(connection) != "current":
            raise ValueError("processed_batch source receipt schema migration is incomplete")

    def _update_runner_session_segment(
        self,
        connection: sqlite3.Connection,
        *,
        envelope: FeatureBatchEnvelope,
        records: Sequence[RunnerSignalRecord],
    ) -> _RunnerSessionSegment:
        trade_date = envelope.event_time.astimezone(_SHANGHAI).date()
        row = connection.execute(
            "SELECT * FROM runner_session_segment WHERE trade_date = ?",
            (trade_date.isoformat(),),
        ).fetchone()
        if row is None:
            if records:
                start_after_sequence = records[0].sequence - 1
            else:
                watermark = connection.execute(
                    "SELECT max(sequence) AS value FROM runner_signal"
                ).fetchone()
                start_after_sequence = (
                    0
                    if watermark is None or watermark["value"] is None
                    else int(watermark["value"])
                )
            previous = _runner_segment_seed(
                trade_date=trade_date,
                runner_generation_id=self.source_generation_id,
                strategy_spec_fingerprint=self.spec.spec_fingerprint,
            )
            final_sequence = start_after_sequence
            record_count = 0
            raw_bytes = 0
        else:
            persisted = self._runner_session_segment_from_row(row)
            if persisted.runner_generation_id != self.source_generation_id:
                raise StrategyBatchConflictError("runner session segment generation changed")
            if envelope.sequence != persisted.final_feature_sequence + 1:
                raise StrategyBatchConflictError(
                    "runner session feature sequence must advance contiguously"
                )
            start_after_sequence = persisted.start_after_sequence
            previous = persisted.chain_hash
            final_sequence = persisted.final_sequence
            record_count = persisted.record_count
            raw_bytes = persisted.raw_bytes
        for record in records:
            verified = RunnerSignalRecord.model_validate(record)
            if verified.sequence != final_sequence + 1:
                raise StrategyBatchConflictError("runner session signal sequence has a gap")
            payload = _canonical_json_bytes(verified.model_dump(mode="json"))
            previous = _advance_runner_segment_chain(previous, verified)
            final_sequence = verified.sequence
            record_count += 1
            raw_bytes += len(payload)
            if record_count > _MAX_RUNNER_SESSION_RECORDS:
                raise StrategyBatchConflictError("runner session segment exceeds the record budget")
            if raw_bytes > _MAX_RUNNER_SESSION_RAW_BYTES:
                raise StrategyBatchConflictError(
                    "runner session segment exceeds the raw byte budget"
                )
        segment = _RunnerSessionSegment(
            trade_date=trade_date,
            runner_generation_id=self.source_generation_id,
            start_after_sequence=start_after_sequence,
            final_sequence=final_sequence,
            record_count=record_count,
            raw_bytes=raw_bytes,
            chain_hash=previous,
            final_feature_sequence=envelope.sequence,
            final_feature_batch_id=envelope.batch_id,
        )
        connection.execute(
            """
            INSERT INTO runner_session_segment(
                trade_date, runner_generation_id, start_after_sequence,
                final_sequence, record_count, raw_bytes, chain_hash,
                final_feature_sequence, final_feature_batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date) DO UPDATE SET
                runner_generation_id = excluded.runner_generation_id,
                start_after_sequence = excluded.start_after_sequence,
                final_sequence = excluded.final_sequence,
                record_count = excluded.record_count,
                raw_bytes = excluded.raw_bytes,
                chain_hash = excluded.chain_hash,
                final_feature_sequence = excluded.final_feature_sequence,
                final_feature_batch_id = excluded.final_feature_batch_id
            """,
            (
                segment.trade_date.isoformat(),
                segment.runner_generation_id,
                segment.start_after_sequence,
                segment.final_sequence,
                segment.record_count,
                segment.raw_bytes,
                segment.chain_hash,
                segment.final_feature_sequence,
                segment.final_feature_batch_id,
            ),
        )
        return segment

    def process_batch(
        self,
        envelope: FeatureBatchEnvelope,
        frame: pd.DataFrame,
        *,
        feature_payload: bytes | str | None = None,
        source_receipt: StrategySourceBatchReceipt | None = None,
        dataset_snapshot_id: Sha256,
        observed_at: datetime,
        evaluator: StrategyEvaluator,
    ) -> StrategyBatchResult:
        observed_at = normalize_aware_utc(observed_at)
        if source_receipt is not None:
            if not isinstance(source_receipt, StrategySourceBatchReceipt):
                raise TypeError("source_receipt must be a StrategySourceBatchReceipt")
            if source_receipt.source_sequence != envelope.sequence:
                raise ValueError("source receipt sequence must match feature envelope sequence")
        dataset_snapshot_id = _validate_sha256(
            dataset_snapshot_id,
            label="dataset_snapshot_id",
        )
        self._validate_batch(envelope, frame, observed_at=observed_at)
        envelope_fingerprint = canonical_sha256(envelope)
        normalized = self._normalize_frame(frame)
        self._validate_candidate_metadata_columns(normalized)
        candidate_input_mode = (
            "occurrence" if "candidate_occurrence_id" in normalized.columns else "flat"
        )
        self._validate_feature_structure(envelope, normalized)
        self._validate_feature_availability(
            envelope,
            normalized,
            observed_at=observed_at,
        )
        canonical_payload = canonical_feature_payload(
            normalized,
            schema_version=envelope.schema_version,
        )
        if feature_payload is None:
            feature_payload_hash = hashlib.sha256(canonical_payload).hexdigest()
            if feature_payload_hash != envelope.content_hash:
                raise StrategyBatchConflictError(
                    "feature payload hash does not match envelope content_hash"
                )
        else:
            _, feature_payload_hash = _validate_supplied_feature_payload(
                feature_payload,
                envelope=envelope,
                canonical_frame_payload=canonical_payload,
            )

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._lock_candidate_input_mode(connection, candidate_input_mode)
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
                        or not self._source_receipt_matches(existing, source_receipt)
                        or observed_at < datetime.fromisoformat(existing["observed_at"])
                    ):
                        raise StrategyBatchConflictError(
                            "immutable batch sequence contains conflicting evidence"
                        )
                    connection.rollback()
                    return StrategyBatchResult.model_validate_json(existing["result_json"])

                batch_trade_date = envelope.event_time.astimezone(_SHANGHAI).date()
                closed = connection.execute(
                    """
                    SELECT receipt_id FROM runner_session_close_receipt
                    WHERE trade_date = ?
                    """,
                    (batch_trade_date.isoformat(),),
                ).fetchone()
                if closed is not None:
                    raise StrategyBatchConflictError(
                        "late feature batch cannot mutate an already closed session"
                    )

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
                lifecycle_feature_fingerprints: dict[str, str] = {}
                transitioned = 0
                skipped = 0
                for row in normalized.to_dict(orient="records"):
                    candidate_id = str(row["ts_code"])
                    state = self._candidate_state(
                        connection,
                        candidate_id,
                        row,
                        observed_at,
                    )
                    features = self._candidate_features(
                        envelope,
                        row,
                        candidate_id=candidate_id,
                    )
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

                    features, lifecycle_fingerprint, state = self._merge_lifecycle_features(
                        connection,
                        state=state,
                        features=features,
                        envelope=envelope,
                        row=row,
                        candidate_id=candidate_id,
                        observed_at=observed_at,
                    )
                    if lifecycle_fingerprint is not None:
                        lifecycle_feature_fingerprints[state.state_key] = lifecycle_fingerprint

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
                            if decision.action in {
                                SignalAction.REDUCE,
                                SignalAction.S_INTENT,
                            }:
                                if "entry_signal_id" in evidence:
                                    raise ValueError(
                                        "decision evidence cannot override entry_signal_id"
                                    )
                                entry_signal = self._latest_entry_signal(
                                    connection,
                                    state=state,
                                )
                                if entry_signal is None:
                                    raise ValueError(
                                        "sell action requires a persisted entry signal"
                                    )
                                evidence["entry_signal_id"] = entry_signal.signal_id
                            evidence["runner_transition"] = {
                                **state.runner_transition_metadata,
                                "event": decision.event,
                                "from_state": state.state.value,
                                "to_state": next_state.value,
                                "feature_batch_id": envelope.batch_id,
                                "feature_sequence": envelope.sequence,
                                "evaluator_contract_fingerprint": (
                                    self.evaluator_contract_fingerprint
                                ),
                                **(
                                    {}
                                    if lifecycle_fingerprint is None
                                    else {"lifecycle_feature_fingerprint": lifecycle_fingerprint}
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
                            signal_payload = _json_payload(signal)
                            signal_index_values = _signal_index_values(signal)
                            cursor = connection.execute(
                                """
                                INSERT INTO runner_signal(
                                    signal_id, feature_sequence, candidate_id, action,
                                    entry_signal_id, candidate_occurrence_id,
                                    event_time, available_at, expires_at, payload_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    signal.signal_id,
                                    envelope.sequence,
                                    *signal_index_values,
                                    signal_payload,
                                ),
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

                self._update_runner_session_segment(
                    connection,
                    envelope=envelope,
                    records=records,
                )
                result = StrategyBatchResult(
                    feature_batch_id=envelope.batch_id,
                    feature_sequence=envelope.sequence,
                    processed_candidates=len(normalized),
                    transitioned_candidates=transitioned,
                    skipped_candidates=skipped,
                    signals=tuple(record.model_dump(mode="json") for record in records),
                    lifecycle_feature_fingerprints=lifecycle_feature_fingerprints,
                )
                connection.execute(
                    """
                    INSERT INTO processed_batch(
                        feature_sequence, feature_batch_id, envelope_fingerprint,
                        feature_payload_hash, dataset_snapshot_id, event_time,
                        available_at, observed_at, source_generation_id,
                        source_sequence, source_batch_id, source_content_hash,
                        result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        None if source_receipt is None else source_receipt.source_generation_id,
                        None if source_receipt is None else source_receipt.source_sequence,
                        None if source_receipt is None else source_receipt.source_batch_id,
                        None if source_receipt is None else source_receipt.source_content_hash,
                        _json_payload(result),
                    ),
                )
                connection.commit()
                return result
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    @staticmethod
    def _lock_candidate_input_mode(
        connection: sqlite3.Connection,
        candidate_input_mode: str,
    ) -> None:
        row = connection.execute(
            "SELECT candidate_input_mode FROM runner_metadata WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("runner identity is missing")
        persisted = row["candidate_input_mode"]
        if persisted is None:
            connection.execute(
                "UPDATE runner_metadata SET candidate_input_mode = ? WHERE singleton = 1",
                (candidate_input_mode,),
            )
            return
        if persisted != candidate_input_mode:
            raise StrategyBatchConflictError(
                f"candidate input mode is locked to {persisted}, got {candidate_input_mode}"
            )

    @staticmethod
    def _source_receipt_matches(
        row: sqlite3.Row,
        receipt: StrategySourceBatchReceipt | None,
    ) -> bool:
        persisted = (
            row["source_generation_id"],
            row["source_sequence"],
            row["source_batch_id"],
            row["source_content_hash"],
        )
        expected = (
            (None, None, None, None)
            if receipt is None
            else (
                receipt.source_generation_id,
                receipt.source_sequence,
                receipt.source_batch_id,
                receipt.source_content_hash,
            )
        )
        return persisted == expected

    def replay_source_batch(
        self,
        receipt: StrategySourceBatchReceipt,
        *,
        observed_at: datetime,
    ) -> StrategyBatchResult | None:
        if not isinstance(receipt, StrategySourceBatchReceipt):
            raise TypeError("receipt must be a StrategySourceBatchReceipt")
        observed = normalize_aware_utc(observed_at)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM processed_batch WHERE feature_sequence = ?",
                (receipt.source_sequence,),
            ).fetchone()
        if row is None:
            return None
        if not self._source_receipt_matches(row, receipt):
            raise StrategyBatchConflictError(
                "source batch receipt conflicts with persisted source evidence"
            )
        if observed < datetime.fromisoformat(row["observed_at"]):
            raise StrategyBatchConflictError("source batch replay observed_at moved backwards")
        return StrategyBatchResult.model_validate_json(row["result_json"])

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
    def _validate_candidate_metadata_columns(frame: pd.DataFrame) -> None:
        present = tuple(column in frame.columns for column in _CANDIDATE_METADATA_COLUMNS)
        if any(present) and not all(present):
            raise ValueError(
                "candidate occurrence metadata columns must be all present or all absent"
            )

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
        if frame.empty and envelope.row_count == 0:
            return
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
        requirements = tuple(
            requirement
            for requirement in requirements
            if not (
                self.lifecycle_feature_source is not None
                and requirement.name in _EXECUTION_LIFECYCLE_FEATURES
            )
        )
        missing_columns = sorted(
            requirement.name
            for requirement in requirements
            if requirement.name not in frame.columns
        )
        if missing_columns:
            raise ValueError("missing feature columns: " + ", ".join(missing_columns))
        missing_statuses = sorted(
            f"{candidate_id}:{requirement.name}"
            for candidate_id in frame["ts_code"].astype(str)
            for requirement in requirements
            if envelope.field_status(
                requirement.name,
                candidate_id=candidate_id,
            )
            is None
        )
        if missing_statuses:
            raise ValueError("missing field status for: " + ", ".join(missing_statuses))

    def _validate_feature_availability(
        self,
        envelope: FeatureBatchEnvelope,
        frame: pd.DataFrame,
        *,
        observed_at: datetime,
    ) -> None:
        if self.feature_contract is None:
            return
        if envelope.contract_version != self.feature_contract.version:
            raise ValueError("feature envelope version does not match published contract")
        requirements = self.spec.required_features + self._eligible_optional_requirements(envelope)
        for candidate_id in frame["ts_code"].astype(str):
            for requirement in requirements:
                if (
                    self.lifecycle_feature_source is not None
                    and requirement.name in _EXECUTION_LIFECYCLE_FEATURES
                ):
                    continue
                definition = self._feature_definitions.get(requirement.name)
                if definition is None:
                    raise ValueError(
                        f"published feature contract does not define {requirement.name}"
                    )
                status = envelope.field_status(
                    requirement.name,
                    candidate_id=candidate_id,
                )
                if status is None:
                    raise RuntimeError("feature structure was not validated")
                if status.decision_cutoff > observed_at:
                    raise ValueError(
                        f"feature {candidate_id}:{requirement.name} decision_cutoff "
                        "is in the future"
                    )
                availability = definition.availability_contract
                if (
                    status.status is FeatureAvailability.UNAVAILABLE
                    and availability.missing_policy is MissingFeaturePolicy.FAIL_CLOSED
                ):
                    raise ValueError(
                        f"feature {candidate_id}:{requirement.name} is missing under "
                        "fail_closed policy"
                    )
                if status.actual_delay_seconds <= availability.max_delay_seconds:
                    continue
                if availability.late_policy is LateFeaturePolicy.FAIL_CLOSED:
                    raise ValueError(
                        f"feature {candidate_id}:{requirement.name} exceeds max_delay_seconds"
                    )
                expected_status = (
                    FeatureAvailability.STALE
                    if availability.late_policy is LateFeaturePolicy.MARK_STALE
                    else FeatureAvailability.DEGRADED
                )
                if status.status is not expected_status:
                    raise ValueError(
                        f"feature {candidate_id}:{requirement.name} exceeds "
                        f"max_delay_seconds without {expected_status.value} status"
                    )

    def _candidate_features(
        self,
        envelope: FeatureBatchEnvelope,
        row: Mapping[str, object],
        *,
        candidate_id: str,
    ) -> Mapping[str, object] | None:
        features: dict[str, object] = {}
        for requirement in self.spec.required_features:
            if (
                self.lifecycle_feature_source is not None
                and requirement.name in _EXECUTION_LIFECYCLE_FEATURES
            ):
                continue
            status = envelope.field_status(
                requirement.name,
                candidate_id=candidate_id,
            )
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
            if (
                self.lifecycle_feature_source is not None
                and requirement.name in _EXECUTION_LIFECYCLE_FEATURES
            ):
                continue
            status = envelope.field_status(
                requirement.name,
                candidate_id=candidate_id,
            )
            if status is None:
                raise RuntimeError("feature structure was not validated")
            value = row[requirement.name]
            if self._status_is_usable(
                status,
                allow_degraded=requirement.allow_degraded,
            ) and self._has_scalar_value(value):
                features[requirement.name] = value
        return MappingProxyType(features)

    def _merge_lifecycle_features(
        self,
        connection: sqlite3.Connection,
        *,
        state: StrategyCandidateState,
        features: Mapping[str, object],
        envelope: FeatureBatchEnvelope,
        row: Mapping[str, object],
        candidate_id: str,
        observed_at: datetime,
    ) -> tuple[Mapping[str, object], str | None, StrategyCandidateState]:
        if self.lifecycle_feature_source is None or state.state not in {
            StrategyLifecycleState.ARMED,
            StrategyLifecycleState.HOLDING,
        }:
            return features, None, state
        entry_signal = self._latest_entry_signal(connection, state=state)
        if entry_signal is None:
            raise ValueError("armed or holding candidate is missing its entry signal")
        session_high = row.get("session_high")
        if not self._has_scalar_value(session_high):
            raise ValueError("paper lifecycle requires candidate session_high")
        session_high_status = envelope.field_status(
            "session_high",
            candidate_id=candidate_id,
        )
        if session_high_status is None:
            raise ValueError("paper lifecycle requires candidate session_high status")
        self._validate_instance_availability(
            "session_high",
            session_high_status,
            observed_at=observed_at,
            require_exact_cutoff=False,
        )
        lifecycle_market_features = dict(features)
        lifecycle_market_features["session_high"] = session_high
        overlay = self.lifecycle_feature_source.resolve(
            candidate_id=state.candidate_id,
            entry_signal=entry_signal,
            exit_signals=self._exit_signals_for_entry(
                connection,
                state=state,
                entry_signal=entry_signal,
                decision_cutoff=observed_at,
            ),
            decision_cutoff=observed_at,
            market_features=MappingProxyType(lifecycle_market_features),
            market_feature_statuses=MappingProxyType({"session_high": session_high_status}),
            previous_eligible_high_price_raw=state.eligible_high_price_raw,
            previous_high_source_event_time=state.eligible_high_source_event_time,
            previous_high_available_at=state.eligible_high_available_at,
        )
        if not isinstance(overlay, FeatureInstanceEnvelope):
            raise TypeError("lifecycle source must return a FeatureInstanceEnvelope")
        expected_names = (
            {"entry_fill_status"}
            if state.state is StrategyLifecycleState.ARMED
            else _EXECUTION_LIFECYCLE_FEATURES - {"entry_fill_status"}
        )
        if not expected_names.issubset(overlay.values):
            raise ValueError("paper lifecycle source is missing required state fields")
        merged = dict(features)
        for name, value in overlay.values.items():
            if name not in _EXECUTION_LIFECYCLE_FEATURES:
                raise ValueError("paper lifecycle source returned an undeclared field")
            if name in merged:
                raise ValueError("paper lifecycle source collided with market features")
            status = overlay.field_status(name)
            if status is None:
                raise RuntimeError("feature instance envelope is internally inconsistent")
            self._validate_instance_availability(name, status, observed_at=observed_at)
            merged[name] = value
        high_status = overlay.field_status("eligible_high_price_raw")
        if high_status is not None:
            if high_status.candidate_id not in {None, candidate_id}:
                raise ValueError("paper lifecycle high watermark candidate does not match")
            state = state.model_copy(
                update={
                    "eligible_high_price_raw": float(overlay.values["eligible_high_price_raw"]),
                    "eligible_high_source_event_time": high_status.source_event_time,
                    "eligible_high_available_at": high_status.available_at,
                }
            )
        return MappingProxyType(merged), overlay.instance_fingerprint, state

    def _validate_instance_availability(
        self,
        name: str,
        status: FeatureFieldStatus,
        *,
        observed_at: datetime,
        require_exact_cutoff: bool = True,
    ) -> None:
        if self.feature_contract is None:
            raise ValueError("lifecycle features require a published feature contract")
        definition = self._feature_definitions.get(name)
        if definition is None:
            raise ValueError(f"published feature contract does not define {name}")
        if status.decision_cutoff > observed_at or (
            require_exact_cutoff and status.decision_cutoff != observed_at
        ):
            raise ValueError(f"feature {name} decision_cutoff does not match runner cutoff")
        availability = definition.availability_contract
        if status.actual_delay_seconds > availability.max_delay_seconds:
            if availability.late_policy is LateFeaturePolicy.FAIL_CLOSED:
                raise ValueError(f"feature {name} exceeds max_delay_seconds")
            expected_status = (
                FeatureAvailability.STALE
                if availability.late_policy is LateFeaturePolicy.MARK_STALE
                else FeatureAvailability.DEGRADED
            )
            if status.status is not expected_status:
                raise ValueError(
                    f"feature {name} exceeds max_delay_seconds without "
                    f"{expected_status.value} status"
                )

    @staticmethod
    def _latest_entry_signal(
        connection: sqlite3.Connection,
        *,
        state: StrategyCandidateState,
    ) -> SignalEnvelopeFamily | None:
        row = connection.execute(
            """
            SELECT * FROM runner_signal
            WHERE candidate_id = ? AND candidate_occurrence_id IS ?
              AND action = 'b_intent'
            ORDER BY sequence DESC LIMIT 1
            """,
            (state.candidate_id, state.candidate_occurrence_id),
        ).fetchone()
        return None if row is None else StrategyRunnerStore._runner_signal_from_row(row)

    @staticmethod
    def _exit_signals_for_entry(
        connection: sqlite3.Connection,
        *,
        state: StrategyCandidateState,
        entry_signal: SignalEnvelopeFamily,
        decision_cutoff: datetime,
    ) -> tuple[SignalEnvelopeFamily, ...]:
        rows = connection.execute(
            """
            SELECT * FROM runner_signal
            WHERE candidate_id = ? AND candidate_occurrence_id IS ?
              AND entry_signal_id = ?
              AND action IN ('reduce', 's_intent')
              AND available_at <= ?
            ORDER BY sequence
            """,
            (
                state.candidate_id,
                state.candidate_occurrence_id,
                entry_signal.signal_id,
                _utc_iso(decision_cutoff),
            ),
        ).fetchall()
        return tuple(StrategyRunnerStore._runner_signal_from_row(row) for row in rows)

    def _candidate_state(
        self,
        connection: sqlite3.Connection,
        candidate_id: str,
        candidate_row: Mapping[str, object],
        observed_at: datetime,
    ) -> StrategyCandidateState:
        metadata = self._candidate_metadata(candidate_id, candidate_row)
        occurrence_id = str(metadata.get("candidate_occurrence_id") or candidate_id)
        row = connection.execute(
            "SELECT * FROM candidate_state WHERE occurrence_id = ?",
            (occurrence_id,),
        ).fetchone()
        if row is None:
            if self.lifecycle_feature_source is not None:
                active = connection.execute(
                    """
                    SELECT * FROM candidate_state
                    WHERE candidate_id = ? AND state IN ('armed', 'holding')
                    ORDER BY updated_at DESC
                    """,
                    (candidate_id,),
                ).fetchall()
                if len(active) > 1:
                    raise StrategyBatchConflictError(
                        "candidate has multiple active execution lifecycles"
                    )
                if active:
                    return self._state_from_row(active[0])
            return StrategyCandidateState(
                strategy_spec_fingerprint=self.spec.spec_fingerprint,
                candidate_id=candidate_id,
                **metadata,
                state=self.spec.initial_state,
                last_feature_sequence=-1,
                updated_at=observed_at,
            )
        state = self._state_from_row(row)
        expected = {
            "candidate_id": candidate_id,
            **{column: metadata.get(column) for column in _CANDIDATE_METADATA_COLUMNS},
        }
        actual = {
            "candidate_id": state.candidate_id,
            **{column: getattr(state, column) for column in _CANDIDATE_METADATA_COLUMNS},
        }
        if actual != expected:
            raise StrategyBatchConflictError(
                "candidate occurrence metadata drift conflicts with persisted state"
            )
        return state

    def _candidate_metadata(
        self,
        candidate_id: str,
        row: Mapping[str, object],
    ) -> dict[str, object]:
        if "candidate_occurrence_id" not in row:
            return {}
        values = {column: row[column] for column in _CANDIDATE_METADATA_COLUMNS}
        if any(value is None or value is pd.NA for value in values.values()):
            raise ValueError("candidate occurrence metadata values cannot be null")
        occurrence_id = values["candidate_occurrence_id"]
        generation = values["candidate_generation_sha256"]
        if not isinstance(occurrence_id, str) or SHA256_PATTERN.fullmatch(occurrence_id) is None:
            raise ValueError("candidate_occurrence_id must be a lowercase SHA-256 digest")
        if not isinstance(generation, str) or SHA256_PATTERN.fullmatch(generation) is None:
            raise ValueError("candidate_generation_sha256 must be a lowercase SHA-256 digest")
        effective_raw = values["candidate_effective_trade_date"]
        if not isinstance(effective_raw, str):
            raise ValueError("candidate_effective_trade_date must be an ISO date string")
        try:
            effective_trade_date = date.fromisoformat(effective_raw)
        except ValueError as exc:
            raise ValueError("candidate_effective_trade_date must be an ISO date string") from exc
        if effective_trade_date.isoformat() != effective_raw:
            raise ValueError("candidate_effective_trade_date must be a canonical ISO date")
        variant = values["candidate_variant"]
        if not isinstance(variant, str) or not variant.strip():
            raise ValueError("candidate_variant must be a non-empty string")
        schema_version = values["candidate_snapshot_schema_version"]
        if type(schema_version) is not int or schema_version not in {1, 2, 3}:
            raise ValueError("candidate_snapshot_schema_version must be 1, 2 or 3")
        expected_occurrence = candidate_occurrence_id(
            strategy_id=self.spec.strategy_id,
            strategy_version=str(self.spec.version),
            candidate_id=candidate_id,
            variant=variant,
            effective_trade_date=effective_trade_date,
        )
        if occurrence_id != expected_occurrence:
            raise ValueError("candidate_occurrence_id does not bind candidate metadata")
        return {
            "candidate_occurrence_id": occurrence_id,
            "candidate_effective_trade_date": effective_trade_date,
            "candidate_variant": variant,
            "candidate_generation_sha256": generation,
            "candidate_snapshot_schema_version": schema_version,
        }

    def _write_state(
        self,
        connection: sqlite3.Connection,
        state: StrategyCandidateState,
    ) -> None:
        connection.execute(
            """
            INSERT INTO candidate_state(
                occurrence_id, candidate_id, candidate_effective_trade_date,
                candidate_variant, candidate_generation_sha256,
                candidate_snapshot_schema_version, state, last_feature_sequence,
                last_feature_batch_id, updated_at, eligible_high_price_raw,
                eligible_high_source_event_time, eligible_high_available_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(occurrence_id) DO UPDATE SET
                candidate_id = excluded.candidate_id,
                candidate_effective_trade_date = excluded.candidate_effective_trade_date,
                candidate_variant = excluded.candidate_variant,
                candidate_generation_sha256 = excluded.candidate_generation_sha256,
                candidate_snapshot_schema_version = excluded.candidate_snapshot_schema_version,
                state = excluded.state,
                last_feature_sequence = excluded.last_feature_sequence,
                last_feature_batch_id = excluded.last_feature_batch_id,
                updated_at = excluded.updated_at,
                eligible_high_price_raw = excluded.eligible_high_price_raw,
                eligible_high_source_event_time = excluded.eligible_high_source_event_time,
                eligible_high_available_at = excluded.eligible_high_available_at
            """,
            (
                state.state_key,
                state.candidate_id,
                (
                    None
                    if state.candidate_effective_trade_date is None
                    else state.candidate_effective_trade_date.isoformat()
                ),
                state.candidate_variant,
                state.candidate_generation_sha256,
                state.candidate_snapshot_schema_version,
                state.state.value,
                state.last_feature_sequence,
                state.last_feature_batch_id,
                state.updated_at.isoformat(),
                state.eligible_high_price_raw,
                (
                    None
                    if state.eligible_high_source_event_time is None
                    else state.eligible_high_source_event_time.isoformat()
                ),
                (
                    None
                    if state.eligible_high_available_at is None
                    else state.eligible_high_available_at.isoformat()
                ),
            ),
        )

    def candidate_state(self, candidate_id: str) -> StrategyCandidateState | None:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM candidate_state WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchall()
        if len(rows) > 1:
            raise ValueError(f"candidate_id {candidate_id!r} is ambiguous across occurrences")
        return None if not rows else self._state_from_row(rows[0])

    def candidate_occurrence_state(
        self,
        occurrence_id: str,
    ) -> StrategyCandidateState | None:
        if not isinstance(occurrence_id, str) or not occurrence_id:
            raise ValueError("occurrence_id cannot be empty")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM candidate_state WHERE occurrence_id = ?",
                (occurrence_id,),
            ).fetchone()
        return None if row is None else self._state_from_row(row)

    def signals_after(self, *, sequence: int) -> tuple[RunnerSignalRecord, ...]:
        if sequence < 0:
            raise ValueError("signal sequence must be nonnegative")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM runner_signal
                WHERE sequence > ? ORDER BY sequence
                """,
                (sequence,),
            ).fetchall()
        return tuple(
            RunnerSignalRecord(
                sequence=row["sequence"],
                signal=self._runner_signal_from_row(row),
            )
            for row in rows
        )

    def _runner_records_through(
        self,
        connection: sqlite3.Connection,
        *,
        high_watermark: int,
    ) -> tuple[RunnerSignalRecord, ...]:
        records: list[RunnerSignalRecord] = []
        cursor = connection.execute(
            """
            SELECT * FROM runner_signal
            WHERE sequence <= ? ORDER BY sequence
            """,
            (high_watermark,),
        )
        while True:
            rows = cursor.fetchmany(1_000)
            if not rows:
                break
            for row in rows:
                records.append(
                    RunnerSignalRecord(
                        sequence=int(row["sequence"]),
                        signal=self._runner_signal_from_row(row),
                    )
                )
        return tuple(records)

    def runner_raw_input_id(
        self,
        *,
        source_id: str,
        high_watermark: int,
    ) -> str:
        with self._connect() as connection:
            records = self._runner_records_through(
                connection,
                high_watermark=high_watermark,
            )
        return runner_signal_raw_input_id(
            source_id=source_id,
            runner_generation_id=self.source_generation_id,
            strategy_spec_fingerprint=self.spec.spec_fingerprint,
            high_watermark=high_watermark,
            records=records,
        )

    def runner_session_raw_input_id(
        self,
        *,
        source_id: str,
        trade_date: date,
    ) -> str:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runner_session_segment WHERE trade_date = ?",
                (trade_date.isoformat(),),
            ).fetchone()
        if row is None:
            raise ValueError("runner session segment is missing")
        return _runner_session_raw_input_id(
            source_id=source_id,
            runner_generation_id=self.source_generation_id,
            strategy_spec_fingerprint=self.spec.spec_fingerprint,
            segment=self._runner_session_segment_from_row(row),
        )

    def runner_session_route_bounds(self, trade_date: date) -> tuple[int, int]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runner_session_segment WHERE trade_date = ?",
                (trade_date.isoformat(),),
            ).fetchone()
        if row is None:
            raise ValueError("runner session segment is missing")
        segment = self._runner_session_segment_from_row(row)
        return segment.start_after_sequence, segment.final_sequence

    def session_close_receipt(
        self,
        trade_date: date,
    ) -> ShadowSourceCompletionReceipt | None:
        with self._connect() as connection:
            connection.execute("BEGIN")
            size = connection.execute(
                "SELECT length(CAST(payload_json AS BLOB)) "
                "FROM runner_session_close_receipt WHERE trade_date = ?",
                (trade_date.isoformat(),),
            ).fetchone()
            if size is None:
                return None
            if int(size[0]) > _MAX_SESSION_CLOSE_RECEIPT_BYTES:
                raise ValueError("runner session close receipt exceeds the byte budget")
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
            raise ValueError("runner session close receipt changed after byte preflight")
        return self._session_close_receipt_from_row(row)

    def publish_session_close_receipt(
        self,
        *,
        trade_date: date,
        session_close_at: datetime,
        source_id: str,
        calendar_generation_id: str,
        producer_service_id: str,
        producer_instance_id: str,
        producer_version: str,
        produced_at: datetime,
        feature_close_marker: FeatureSessionCloseMarker,
        attestation_signer: CompletionAttestationSigner,
        strategy_registration_fingerprint: str,
        executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        feature_registration_fingerprint: str,
        feature_contract_fingerprint: str,
        producer_manifest_fingerprint: str,
        route_evidence: RunnerSignalRouteDrainEvidence,
        fault_hook: Callable[[str], None] | None = None,
    ) -> ShadowSourceCompletionReceipt:
        """Atomically seal one fully processed and fully routed SSE session."""

        produced = normalize_aware_utc(produced_at)
        close = normalize_aware_utc(session_close_at)
        route = RunnerSignalRouteDrainEvidence.model_validate(route_evidence)
        if not isinstance(feature_close_marker, FeatureSessionCloseMarker):
            raise TypeError("feature close marker is required")
        if not callable(getattr(attestation_signer, "issue", None)):
            raise TypeError("completion attestation signer is required")
        marker = FeatureSessionCloseMarker.model_validate(feature_close_marker)
        if close.astimezone(_SHANGHAI).date() != trade_date:
            raise ValueError("session close does not match trade_date")
        if produced < close:
            raise StrategyBatchConflictError(
                "session close receipt cannot be produced before close"
            )
        if route.observed_at > produced:
            raise StrategyBatchConflictError("route drain evidence is not visible at receipt time")
        if route.source_id != source_id:
            raise StrategyBatchConflictError("route drain evidence source does not match runner")
        if route.runner_generation_id != self.source_generation_id:
            raise StrategyBatchConflictError("route drain runner generation does not match")
        if route.strategy_spec_fingerprint != self.spec.spec_fingerprint:
            raise StrategyBatchConflictError("route drain strategy identity does not match")
        _validate_sha256(calendar_generation_id, label="calendar_generation_id")
        for label, value in (
            ("strategy_registration_fingerprint", strategy_registration_fingerprint),
            ("executable_fingerprint", executable_fingerprint),
            ("candidate_schema_fingerprint", candidate_schema_fingerprint),
            ("feature_registration_fingerprint", feature_registration_fingerprint),
            ("feature_contract_fingerprint", feature_contract_fingerprint),
            ("producer_manifest_fingerprint", producer_manifest_fingerprint),
        ):
            _validate_sha256(value, label=label)
        if marker.trade_date != trade_date or marker.session_close_at != close:
            raise StrategyBatchConflictError("feature close marker does not match session")
        if marker.calendar_generation_id != calendar_generation_id:
            raise StrategyBatchConflictError("feature close marker calendar generation changed")
        if marker.produced_at > produced:
            raise StrategyBatchConflictError("feature close marker was unavailable at receipt time")
        if not producer_service_id.strip() or not producer_instance_id.strip():
            raise ValueError("producer service and instance identities cannot be empty")
        if not producer_version.strip():
            raise ValueError("producer_version cannot be empty")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing_size = connection.execute(
                    "SELECT length(CAST(payload_json AS BLOB)) "
                    "FROM runner_session_close_receipt WHERE trade_date = ?",
                    (trade_date.isoformat(),),
                ).fetchone()
                if (
                    existing_size is not None
                    and int(existing_size[0]) > _MAX_SESSION_CLOSE_RECEIPT_BYTES
                ):
                    raise ValueError("runner session close receipt exceeds the byte budget")
                existing = connection.execute(
                    """
                    SELECT trade_date, receipt_id, source_id, signal_high_watermark,
                           CAST(payload_json AS BLOB) AS payload_bytes
                    FROM runner_session_close_receipt
                    WHERE trade_date = ?
                    """,
                    (trade_date.isoformat(),),
                ).fetchone()
                if existing is not None:
                    persisted = self._session_close_receipt_from_row(existing)
                    if (
                        persisted.source_id != source_id
                        or persisted.calendar_generation_id != calendar_generation_id
                        or persisted.producer_service_id != producer_service_id
                        or persisted.producer_instance_id != producer_instance_id
                        or persisted.producer_version != producer_version
                        or persisted.runner_generation_id != self.source_generation_id
                        or persisted.signal_authority_generation_id
                        != route.signal_authority_generation_id
                        or persisted.route_receipts_id != route.route_receipts_sha256
                        or persisted.feature_close_marker_id != marker.marker_id
                    ):
                        raise StrategyBatchConflictError(
                            "conflicting immutable session close receipt"
                        )
                    persisted_attestation = persisted.completion_attestation
                    if persisted_attestation is None:
                        raise StrategyBatchConflictError(
                            "durable session close receipt has no completion attestation"
                        )
                    persisted_claims = persisted_attestation.claims
                    if (
                        persisted_claims.strategy_registration_fingerprint
                        != strategy_registration_fingerprint
                        or persisted_claims.executable_fingerprint != executable_fingerprint
                        or persisted_claims.candidate_schema_fingerprint
                        != candidate_schema_fingerprint
                        or persisted_claims.feature_registration_fingerprint
                        != feature_registration_fingerprint
                        or persisted_claims.feature_contract_fingerprint
                        != feature_contract_fingerprint
                        or persisted_claims.producer_manifest_fingerprint
                        != producer_manifest_fingerprint
                        or persisted_claims.routing_policy_fingerprint
                        != route.routing_policy_fingerprint
                    ):
                        raise StrategyBatchConflictError(
                            "conflicting completion attestation business identity"
                        )
                    connection.rollback()
                    return persisted
                processed = connection.execute(
                    """
                    SELECT feature_sequence, feature_batch_id, event_time,
                           source_generation_id, source_sequence,
                           source_batch_id, source_content_hash
                    FROM processed_batch
                    WHERE source_generation_id = ? AND source_sequence = ?
                    """,
                    (marker.source_generation_id, marker.final_sequence),
                ).fetchone()
                if (
                    processed is None
                    or int(processed["source_sequence"]) != marker.final_sequence
                    or str(processed["source_batch_id"]) != marker.final_batch_id
                    or str(processed["source_content_hash"]) != marker.final_content_hash
                ):
                    raise StrategyBatchConflictError(
                        "runner has not consumed the exact feature session close marker"
                    )
                processed_event_time = normalize_aware_utc(
                    datetime.fromisoformat(str(processed["event_time"]))
                )
                if processed_event_time != close:
                    raise StrategyBatchConflictError(
                        "runner final feature event must equal the exact 15:00 close"
                    )
                last_feature_sequence = int(processed["feature_sequence"])
                segment_row = connection.execute(
                    "SELECT * FROM runner_session_segment WHERE trade_date = ?",
                    (trade_date.isoformat(),),
                ).fetchone()
                if segment_row is None:
                    raise StrategyBatchConflictError("runner session segment is missing")
                segment = self._runner_session_segment_from_row(segment_row)
                if (
                    route.trade_date != trade_date
                    or route.segment_start_sequence != segment.start_after_sequence
                    or route.segment_record_count != segment.record_count
                ):
                    raise StrategyBatchConflictError(
                        "signal route backlog or segment mismatch with the runner session"
                    )
                if (
                    segment.final_feature_sequence != last_feature_sequence
                    or segment.final_feature_batch_id != str(processed["feature_batch_id"])
                ):
                    raise StrategyBatchConflictError(
                        "runner session segment does not reach the feature close marker"
                    )
                signal_high_watermark = segment.final_sequence
                if route.routed_through_sequence != signal_high_watermark:
                    raise StrategyBatchConflictError(
                        "signal route backlog is not drained through the runner close watermark"
                    )
                raw_input_id = _runner_session_raw_input_id(
                    source_id=source_id,
                    runner_generation_id=self.source_generation_id,
                    strategy_spec_fingerprint=self.spec.spec_fingerprint,
                    segment=segment,
                )
                unsigned_receipt = ShadowSourceCompletionReceipt(
                    evidence_origin="production",
                    source="isolated",
                    source_id=source_id,
                    trade_date=trade_date,
                    session_close_at=close,
                    complete_through=close,
                    input_identity=raw_input_id,
                    produced_at=produced,
                    producer_commit=self.spec.producer_commit,
                    producer_version=producer_version,
                    producer_service_id=producer_service_id,
                    producer_instance_id=producer_instance_id,
                    runner_generation_id=self.source_generation_id,
                    signal_authority_generation_id=route.signal_authority_generation_id,
                    calendar_generation_id=calendar_generation_id,
                    last_sequence=last_feature_sequence,
                    high_watermark=signal_high_watermark,
                    route_receipts_id=route.route_receipts_sha256,
                    feature_source_generation_id=marker.source_generation_id,
                    feature_close_marker_id=marker.marker_id,
                    feature_segment_chain_hash=marker.segment_chain_hash,
                    segment_start_sequence=segment.start_after_sequence,
                    segment_record_count=segment.record_count,
                    segment_chain_hash=segment.chain_hash,
                )
                claims = CompletionAttestationClaims(
                    completion_receipt_body_sha256=shadow_completion_receipt_body_sha256(
                        unsigned_receipt
                    ),
                    trade_date=trade_date,
                    session_close_at=close,
                    source_id=source_id,
                    input_identity=raw_input_id,
                    strategy_id=self.spec.strategy_id,
                    strategy_version=self.spec.version,
                    strategy_registration_fingerprint=strategy_registration_fingerprint,
                    strategy_spec_fingerprint=self.spec.spec_fingerprint,
                    executable_fingerprint=executable_fingerprint,
                    candidate_schema_fingerprint=candidate_schema_fingerprint,
                    feature_registration_fingerprint=feature_registration_fingerprint,
                    feature_contract_fingerprint=feature_contract_fingerprint,
                    routing_policy_fingerprint=route.routing_policy_fingerprint,
                    producer_manifest_fingerprint=producer_manifest_fingerprint,
                    producer_commit=self.spec.producer_commit,
                    producer_version=producer_version,
                    producer_service_id=producer_service_id,
                    producer_instance_id=producer_instance_id,
                    calendar_generation_id=calendar_generation_id,
                    feature_source_generation_id=marker.source_generation_id,
                    feature_close_marker_id=str(marker.marker_id),
                    feature_segment_chain_hash=marker.segment_chain_hash,
                    runner_generation_id=self.source_generation_id,
                    runner_segment_start_sequence=segment.start_after_sequence,
                    runner_segment_final_sequence=segment.final_sequence,
                    runner_segment_record_count=segment.record_count,
                    runner_segment_chain_hash=segment.chain_hash,
                    signal_authority_generation_id=route.signal_authority_generation_id,
                    route_receipts_id=route.route_receipts_sha256,
                )
                attestation = attestation_signer.issue(claims)
                receipt = ShadowSourceCompletionReceipt.model_validate(
                    {
                        **unsigned_receipt.model_dump(
                            mode="python",
                            exclude={"receipt_id"},
                        ),
                        "completion_attestation": attestation,
                    }
                )
                connection.execute(
                    """
                    INSERT INTO runner_session_close_receipt(
                        trade_date, receipt_id, source_id,
                        signal_high_watermark, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        trade_date.isoformat(),
                        receipt.receipt_id,
                        source_id,
                        signal_high_watermark,
                        _json_payload(receipt),
                    ),
                )
                if fault_hook is not None:
                    fault_hook("after_session_close_receipt_insert")
                connection.commit()
                if fault_hook is not None:
                    fault_hook("after_session_close_receipt_commit")
                return receipt
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

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
            candidate_occurrence_id=(
                row["occurrence_id"] if row["candidate_effective_trade_date"] is not None else None
            ),
            candidate_effective_trade_date=(
                None
                if row["candidate_effective_trade_date"] is None
                else date.fromisoformat(row["candidate_effective_trade_date"])
            ),
            candidate_variant=row["candidate_variant"],
            candidate_generation_sha256=row["candidate_generation_sha256"],
            candidate_snapshot_schema_version=row["candidate_snapshot_schema_version"],
            state=StrategyLifecycleState(row["state"]),
            last_feature_sequence=row["last_feature_sequence"],
            last_feature_batch_id=row["last_feature_batch_id"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
            eligible_high_price_raw=row["eligible_high_price_raw"],
            eligible_high_source_event_time=(
                None
                if row["eligible_high_source_event_time"] is None
                else datetime.fromisoformat(row["eligible_high_source_event_time"])
            ),
            eligible_high_available_at=(
                None
                if row["eligible_high_available_at"] is None
                else datetime.fromisoformat(row["eligible_high_available_at"])
            ),
        )


__all__ = [
    "RunnerSignalRecord",
    "RunnerSignalRouteDrainEvidence",
    "StrategyBatchConflictError",
    "StrategyBatchResult",
    "StrategyCandidateState",
    "StrategyDecision",
    "StrategyEvaluator",
    "StrategyLifecycleFeatureSource",
    "StrategyRunnerStore",
    "StrategySourceBatchReceipt",
    "canonical_feature_payload",
    "runner_signal_raw_input_id",
]
