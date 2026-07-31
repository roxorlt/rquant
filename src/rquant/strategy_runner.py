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
from typing import Annotated, Literal

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
from rquant.strategy_candidate_snapshot import candidate_occurrence_id
from rquant.strategy_spec import StrategyLifecycleState, StrategySpec

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_METADATA_COLUMNS = (
    "candidate_occurrence_id",
    "candidate_effective_trade_date",
    "candidate_variant",
    "candidate_generation_sha256",
    "candidate_snapshot_schema_version",
)
_CANDIDATE_STATE_SCHEMA = (
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
_RUNNER_SIGNAL_SCHEMA = (
    (0, "sequence", "INTEGER", 0, 1),
    (1, "signal_id", "TEXT", 1, 0),
    (2, "feature_sequence", "INTEGER", 1, 0),
    (3, "payload_json", "TEXT", 1, 0),
)
_RUNNER_SIGNAL_TABLE_SQL = (
    "createtablerunner_signal("
    "sequenceintegerprimarykeyautoincrement,"
    "signal_idtextnotnullunique,"
    "feature_sequenceintegernotnull,"
    "payload_jsontextnotnull)"
)
_SINGLETON_CHECK_SQL = "check(singleton=1)"
_CANDIDATE_INPUT_MODE_CHECK_SQL = "check(candidate_input_modein('flat','occurrence'))"
_SOURCE_SEQUENCE_INDEX_NAME = "processed_batch_source_sequence_uq"
_SOURCE_SEQUENCE_INDEX_SQL = (
    "createuniqueindexprocessed_batch_source_sequence_uq"
    "onprocessed_batch(source_sequence)wheresource_sequenceisnotnull"
)


class StrategyBatchConflictError(RuntimeError):
    """A runner input sequence was missing or reused with different evidence."""


class StrategySourceBatchReceipt(RuntimeContractModel):
    """Exact durable evidence for one common feature-spool source batch."""

    source_generation_id: Sha256
    source_sequence: int = Field(ge=0)
    source_batch_id: str = Field(min_length=1)
    source_content_hash: Sha256


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
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._audit_runner_metadata_schema(connection)
                self._audit_runner_source_identity_schema(connection)
                self._audit_runner_signal_schema(connection)
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
                    updated_at TEXT NOT NULL
                )
                """
                )
                connection.execute(
                    """
                CREATE TABLE IF NOT EXISTS runner_signal (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL UNIQUE,
                    feature_sequence INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
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
                if self._audit_runner_metadata_schema(connection) != "current":
                    raise ValueError("runner_metadata schema did not upgrade to current")
                self._audit_runner_source_identity_schema(connection)
                self._audit_runner_signal_schema(connection)
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
    def _audit_runner_signal_schema(cls, connection: sqlite3.Connection) -> None:
        if not cls._table_exists(connection, "runner_signal"):
            return
        if cls._table_schema(connection, "runner_signal") != _RUNNER_SIGNAL_SCHEMA:
            raise ValueError("runner_signal schema is unsupported")
        if ("signal_id",) not in cls._unique_constraint_columns(connection, "runner_signal"):
            raise ValueError("runner_signal requires a signal_id UNIQUE constraint")
        sql = cls._schema_sql(
            connection,
            object_type="table",
            name="runner_signal",
        )
        if sql != _RUNNER_SIGNAL_TABLE_SQL:
            raise ValueError("runner_signal canonical DDL is unsupported")

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
                updated_at TEXT NOT NULL
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
                    state = self._candidate_state(
                        connection,
                        candidate_id,
                        row,
                        observed_at,
                    )
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
                                **state.runner_transition_metadata,
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
                last_feature_batch_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(occurrence_id) DO UPDATE SET
                candidate_id = excluded.candidate_id,
                candidate_effective_trade_date = excluded.candidate_effective_trade_date,
                candidate_variant = excluded.candidate_variant,
                candidate_generation_sha256 = excluded.candidate_generation_sha256,
                candidate_snapshot_schema_version = excluded.candidate_snapshot_schema_version,
                state = excluded.state,
                last_feature_sequence = excluded.last_feature_sequence,
                last_feature_batch_id = excluded.last_feature_batch_id,
                updated_at = excluded.updated_at
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
        )


__all__ = [
    "RunnerSignalRecord",
    "StrategyBatchConflictError",
    "StrategyBatchResult",
    "StrategyCandidateState",
    "StrategyDecision",
    "StrategyEvaluator",
    "StrategyRunnerStore",
    "StrategySourceBatchReceipt",
    "canonical_feature_payload",
]
