from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Event, Lock

import pandas as pd
import pytest

import rquant.strategy_runner as strategy_runner
from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.signal_contracts import SignalAction
from rquant.strategy_candidate_feature_join import (
    StrategyCandidateFeatureBatch,
    join_strategy_candidate_features,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
)
from rquant.strategy_runner import (
    StrategyBatchConflictError,
    StrategyDecision,
    StrategyRunnerStore,
    StrategySourceBatchReceipt,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)

NOW = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
EVALUATOR_FINGERPRINT = "e" * 64


def _spec(
    *,
    producer_commit: str = "c" * 40,
    optional_features: tuple[FeatureRequirement, ...] = (),
    required_features: tuple[FeatureRequirement, ...] | None = None,
) -> StrategySpec:
    return StrategySpec(
        strategy_id="growth-board-surge-v1",
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=1,
        required_features=required_features
        or (
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=1,
            ),
        ),
        optional_features=optional_features,
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.ARMED,
                event="reset",
                to_state=StrategyLifecycleState.IDLE,
            ),
        ),
        parameters={"min_ratio": 1.4},
        allowed_actions=(SignalAction.B_INTENT.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=producer_commit,
    )


def _envelope(
    *,
    sequence: int = 0,
    batch_id: str | None = None,
    available_at: datetime = NOW,
    event_time: datetime | None = None,
    content_hash: str | None = None,
    contract_id: str = "intraday-pit",
    contract_version: int = 1,
    status: FeatureAvailability = FeatureAvailability.AVAILABLE,
    field_statuses: tuple[FeatureFieldStatus, ...] | None = None,
    row_count: int = 1,
) -> FeatureBatchEnvelope:
    batch_available_at = available_at + timedelta(minutes=sequence)
    return FeatureBatchEnvelope(
        schema_version=1,
        batch_id=batch_id or f"feature-{sequence}",
        contract_id=contract_id,
        contract_version=contract_version,
        input_batch_ids=(f"raw-{sequence}",),
        sequence=sequence,
        event_time=event_time or NOW + timedelta(minutes=sequence),
        available_at=batch_available_at,
        row_count=row_count,
        content_hash=content_hash or _payload_hash(_frame()),
        field_statuses=field_statuses
        if field_statuses is not None
        else (_status("rel_same_minute", status=status, available_at=batch_available_at),),
        producer_commit="b" * 40,
    )


def _frame(value: float = 2.0) -> pd.DataFrame:
    return pd.DataFrame({"ts_code": ["300001.SZ"], "rel_same_minute": [value]})


def _payload_hash(frame: pd.DataFrame, *, schema_version: int = 1) -> str:
    return hashlib.sha256(
        strategy_runner.canonical_feature_payload(frame, schema_version=schema_version)
    ).hexdigest()


def _status(
    name: str,
    *,
    status: FeatureAvailability = FeatureAvailability.AVAILABLE,
    available_at: datetime = NOW,
) -> FeatureFieldStatus:
    return FeatureFieldStatus(
        name=name,
        status=status,
        available_at=available_at,
        reason=None if status is FeatureAvailability.AVAILABLE else f"{name} is {status.value}",
    )


def _entry_decision(*_args: object) -> StrategyDecision:
    return StrategyDecision(
        event="entry_ready",
        expected_from_state=StrategyLifecycleState.IDLE,
        expected_to_state=StrategyLifecycleState.ARMED,
        expected_action=SignalAction.B_INTENT,
        action=SignalAction.B_INTENT,
        reason_codes=("relative_volume_confirmed",),
        evidence={"rel_same_minute": 2.0},
        expires_after=timedelta(minutes=5),
    )


def _store(
    path: Path,
    *,
    spec: StrategySpec | None = None,
    evaluator_contract_fingerprint: str = EVALUATOR_FINGERPRINT,
) -> StrategyRunnerStore:
    return StrategyRunnerStore(
        path,
        spec=spec or _spec(),
        evaluator_contract_fingerprint=evaluator_contract_fingerprint,
    )


def _joined_feature_batch(
    tmp_path: Path,
    *,
    empty_requested_authority: bool = False,
    trade_date: date = date(2026, 7, 31),
    sequence: int = 0,
    available_at: datetime = NOW,
    candidate_id: str = "300001.SZ",
    variant: str = "default",
) -> StrategyCandidateFeatureBatch:
    decision_at = available_at - timedelta(days=1)
    requested_rows = (
        ()
        if empty_requested_authority
        else (
            StrategyCandidateRecord(
                strategy_id="growth-board-surge-v1",
                strategy_version="1",
                candidate_id=candidate_id,
                variant=variant,
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=trade_date,
                reference_trade_date=date(2026, 7, 30),
                price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                static_features={"candidate_score": 0.91},
                reference_snapshot_ids={"daily": "1" * 64},
            ),
        )
    )
    requested_root = (
        tmp_path / f"requested-candidates-{trade_date.isoformat()}-{sequence}"
    ).resolve()
    StrategyCandidateSnapshotSpool(requested_root).publish_strategy_records(
        strategy_id="growth-board-surge-v1",
        strategy_version="1",
        source_snapshot_ids={
            "candidate_input": hashlib.sha256(str(requested_root).encode()).hexdigest()
        },
        trade_date=trade_date,
        captured_at=available_at,
        producer_commit="b" * 40,
        rows=requested_rows,
    )
    authorities = [
        CandidateUniverseAuthority(
            strategy_id="growth-board-surge-v1",
            strategy_version="1",
            snapshot_root=requested_root,
            required=True,
            max_age_seconds=60,
        )
    ]
    if empty_requested_authority:
        other_root = (tmp_path / "other-candidates").resolve()
        other_row = StrategyCandidateRecord(
            strategy_id="other-strategy",
            strategy_version="1",
            candidate_id=candidate_id,
            variant="default",
            decision_at=decision_at,
            available_at=decision_at + timedelta(minutes=1),
            effective_trade_date=trade_date,
            reference_trade_date=date(2026, 7, 30),
            price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
            static_features={"other_score": 0.8},
            reference_snapshot_ids={"daily": "2" * 64},
        )
        StrategyCandidateSnapshotSpool(other_root).publish_strategy_records(
            strategy_id="other-strategy",
            strategy_version="1",
            source_snapshot_ids={
                "candidate_input": hashlib.sha256(str(other_root).encode()).hexdigest()
            },
            trade_date=trade_date,
            captured_at=available_at,
            producer_commit="b" * 40,
            rows=(other_row,),
        )
        authorities.append(
            CandidateUniverseAuthority(
                strategy_id="other-strategy",
                strategy_version="1",
                snapshot_root=other_root,
                required=True,
                max_age_seconds=60,
            )
        )
    universe = RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit="b" * 40,
            authorities=tuple(authorities),
        )
    ).load(as_of=available_at, required_trade_date=trade_date)
    common_frame = pd.DataFrame({"ts_code": [candidate_id], "rel_same_minute": [2.0]})
    common_envelope = _envelope(
        sequence=sequence,
        available_at=available_at - timedelta(minutes=sequence),
        event_time=available_at,
        content_hash=_payload_hash(common_frame),
        row_count=1,
    )
    return join_strategy_candidate_features(
        common_envelope,
        common_frame,
        universe,
        "growth-board-surge-v1",
        "1",
    )


def test_feature_payload_serialization_contract_is_canonical() -> None:
    frame = pd.DataFrame(
        {
            "rel_same_minute": [None, 2.0],
            "feature_time": [
                pd.Timestamp("2026-07-31T01:31:00Z"),
                pd.Timestamp("2026-07-31T01:30:00Z"),
            ],
            "ts_code": ["600001.SH", "300001.SZ"],
        }
    )
    expected = json.dumps(
        {
            "rows": [
                {
                    "feature_time": "2026-07-31T01:30:00+00:00",
                    "rel_same_minute": 2.0,
                    "ts_code": "300001.SZ",
                },
                {
                    "feature_time": "2026-07-31T01:31:00+00:00",
                    "rel_same_minute": None,
                    "ts_code": "600001.SH",
                },
            ],
            "schema_version": 2,
        },
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    assert strategy_runner.canonical_feature_payload(frame, schema_version=2) == expected


def test_process_batch_persists_state_and_signal_atomically(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    result = store.process_batch(
        _envelope(),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    assert result.processed_candidates == 1
    assert result.transitioned_candidates == 1
    assert len(result.signals) == 1
    record = result.signals[0]
    signal = record.signal
    assert record.sequence == 1
    assert signal.candidate_id == "300001.SZ"
    assert signal.available_at == NOW
    assert signal.feature_snapshot_id == _payload_hash(_frame())
    assert signal.evidence["runner_transition"] == {
        "evaluator_contract_fingerprint": EVALUATOR_FINGERPRINT,
        "event": "entry_ready",
        "feature_batch_id": "feature-0",
        "feature_sequence": 0,
        "from_state": "idle",
        "to_state": "armed",
    }
    assert store.candidate_state("300001.SZ").state is StrategyLifecycleState.ARMED
    assert store.signals_after(sequence=0) == result.signals


def test_exact_batch_retry_is_idempotent_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    first = _store(path)
    expected = first.process_batch(
        _envelope(),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    reopened = _store(path)
    retried = reopened.process_batch(
        _envelope(),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: pytest.fail("idempotent retry must not evaluate again"),
    )

    assert retried == expected
    assert reopened.signals_after(sequence=0) == expected.signals


def test_same_envelope_with_different_frame_is_a_conflict(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")
    envelope = _envelope()
    store.process_batch(
        envelope,
        _frame(2.0),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    with pytest.raises(StrategyBatchConflictError, match="payload"):
        store.process_batch(
            envelope,
            _frame(99.0),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=_entry_decision,
        )


def test_process_batch_accepts_exact_intraday_payload_bytes(tmp_path: Path) -> None:
    frame = _frame()
    payload = strategy_runner.canonical_feature_payload(frame, schema_version=1)
    store = _store(tmp_path / "runner.sqlite3")

    result = store.process_batch(
        _envelope(content_hash=hashlib.sha256(payload).hexdigest()),
        frame,
        feature_payload=payload,
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    assert result.processed_candidates == 1


def test_process_batch_accepts_real_joined_extended_payload(tmp_path: Path) -> None:
    joined = _joined_feature_batch(tmp_path)
    store = _store(tmp_path / "runner.sqlite3")

    result = store.process_batch(
        joined.envelope,
        joined.frame,
        feature_payload=joined.payload_bytes,
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    assert result.processed_candidates == 1
    assert store.last_batch_sequence() == 0

    changed = json.loads(joined.payload_json)
    changed["retry_metadata"] = "changed"
    changed_payload = json.dumps(
        changed,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    changed_envelope = joined.envelope.model_copy(
        update={"content_hash": hashlib.sha256(changed_payload).hexdigest()}
    )
    with pytest.raises(StrategyBatchConflictError, match="immutable batch"):
        store.process_batch(
            changed_envelope,
            joined.frame,
            feature_payload=changed_payload,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: pytest.fail("conflicting retry must not evaluate"),
        )


def test_joined_signal_binds_candidate_occurrence_evidence(tmp_path: Path) -> None:
    joined = _joined_feature_batch(tmp_path)
    store = _store(tmp_path / "runner.sqlite3")

    result = store.process_batch(
        joined.envelope,
        joined.frame,
        feature_payload=joined.payload_bytes,
        dataset_snapshot_id=joined.envelope.input_fingerprint,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    row = joined.frame.iloc[0]
    assert result.signals[0].signal.candidate_id == "300001.SZ"
    assert result.signals[0].signal.evidence["runner_transition"] == {
        "candidate_effective_trade_date": "2026-07-31",
        "candidate_generation_sha256": row["candidate_generation_sha256"],
        "candidate_occurrence_id": row["candidate_occurrence_id"],
        "candidate_snapshot_schema_version": 3,
        "candidate_variant": "default",
        "evaluator_contract_fingerprint": EVALUATOR_FINGERPRINT,
        "event": "entry_ready",
        "feature_batch_id": joined.envelope.batch_id,
        "feature_sequence": 0,
        "from_state": "idle",
        "to_state": "armed",
    }


def test_same_stock_new_trade_date_starts_from_initial_occurrence_state(
    tmp_path: Path,
) -> None:
    first = _joined_feature_batch(tmp_path, sequence=0)
    second_at = NOW + timedelta(days=1)
    second = _joined_feature_batch(
        tmp_path,
        trade_date=date(2026, 8, 1),
        sequence=1,
        available_at=second_at,
    )
    store = _store(tmp_path / "runner.sqlite3")
    seen_states: list[StrategyLifecycleState] = []

    for batch, observed_at in ((first, NOW), (second, second_at)):
        store.process_batch(
            batch.envelope,
            batch.frame,
            feature_payload=batch.payload_bytes,
            dataset_snapshot_id=batch.envelope.input_fingerprint,
            observed_at=observed_at,
            evaluator=lambda spec, state, features: (
                seen_states.append(state.state) or _entry_decision(spec, state, features)
            ),
        )

    first_occurrence = str(first.frame.iloc[0]["candidate_occurrence_id"])
    second_occurrence = str(second.frame.iloc[0]["candidate_occurrence_id"])
    assert seen_states == [StrategyLifecycleState.IDLE, StrategyLifecycleState.IDLE]
    assert first_occurrence != second_occurrence
    assert store.candidate_occurrence_state(first_occurrence).state is StrategyLifecycleState.ARMED
    assert store.candidate_occurrence_state(second_occurrence).state is StrategyLifecycleState.ARMED
    with pytest.raises(ValueError, match="ambiguous"):
        store.candidate_state("300001.SZ")


def test_same_occurrence_metadata_drift_fails_without_committing_batch(
    tmp_path: Path,
) -> None:
    first = _joined_feature_batch(tmp_path, sequence=0)
    second_at = NOW + timedelta(minutes=1)
    changed_generation = _joined_feature_batch(
        tmp_path,
        sequence=1,
        available_at=second_at,
    )
    store = _store(tmp_path / "runner.sqlite3")
    store.process_batch(
        first.envelope,
        first.frame,
        feature_payload=first.payload_bytes,
        dataset_snapshot_id=first.envelope.input_fingerprint,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    with pytest.raises(StrategyBatchConflictError, match="metadata drift"):
        store.process_batch(
            changed_generation.envelope,
            changed_generation.frame,
            feature_payload=changed_generation.payload_bytes,
            dataset_snapshot_id=changed_generation.envelope.input_fingerprint,
            observed_at=second_at,
            evaluator=lambda *_args: pytest.fail("metadata drift must fail before evaluation"),
        )

    assert store.last_batch_sequence() == 0


def test_candidate_metadata_columns_are_all_present_or_all_absent(tmp_path: Path) -> None:
    joined = _joined_feature_batch(tmp_path)
    partial = joined.frame.drop(columns=["candidate_variant"])
    payload = strategy_runner.canonical_feature_payload(partial, schema_version=1)
    envelope = _envelope(content_hash=hashlib.sha256(payload).hexdigest())

    with pytest.raises(ValueError, match="all present or all absent"):
        _store(tmp_path / "runner.sqlite3").process_batch(
            envelope,
            partial,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )


@pytest.mark.parametrize(
    ("column", "value", "message"),
    (
        ("candidate_occurrence_id", "0" * 64, "does not bind"),
        ("candidate_effective_trade_date", "2026-7-31", "ISO date"),
        ("candidate_variant", "", "non-empty"),
        ("candidate_generation_sha256", "not-a-sha", "SHA-256"),
        ("candidate_snapshot_schema_version", 4, "must be 1, 2 or 3"),
    ),
)
def test_joined_candidate_metadata_values_are_strictly_validated(
    tmp_path: Path,
    column: str,
    value: object,
    message: str,
) -> None:
    joined = _joined_feature_batch(tmp_path)
    invalid = joined.frame.copy(deep=True)
    invalid.loc[0, column] = value
    payload = strategy_runner.canonical_feature_payload(invalid, schema_version=1)

    with pytest.raises(ValueError, match=message):
        _store(tmp_path / "runner.sqlite3").process_batch(
            _envelope(content_hash=hashlib.sha256(payload).hexdigest()),
            invalid,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )


def _create_legacy_candidate_state_table(path: Path, *, populated: bool) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE candidate_state (
                candidate_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                last_feature_sequence INTEGER NOT NULL,
                last_feature_batch_id TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        if populated:
            connection.execute(
                """
                INSERT INTO candidate_state VALUES (?, ?, ?, ?, ?)
                """,
                ("300001.SZ", "armed", 7, "legacy-7", NOW.isoformat()),
            )


def _candidate_state_columns(path: Path) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(path) as connection:
        return tuple(connection.execute("PRAGMA table_info(candidate_state)").fetchall())


def _create_persisted_runner_identity(
    path: Path,
    *,
    spec_fingerprint: str,
    evaluator_fingerprint: str,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE runner_metadata (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                strategy_spec_fingerprint TEXT NOT NULL,
                strategy_spec_json TEXT NOT NULL,
                evaluator_contract_fingerprint TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO runner_metadata VALUES (1, ?, ?, ?)",
            (spec_fingerprint, _spec().model_dump_json(), evaluator_fingerprint),
        )


def _create_custom_runner_metadata(
    path: Path,
    *,
    singleton_column: str = "singleton INTEGER PRIMARY KEY CHECK(singleton = 1)",
    spec_json_column: str = "strategy_spec_json TEXT NOT NULL",
    input_mode_column: str = (
        "candidate_input_mode TEXT CHECK(candidate_input_mode IN ('flat', 'occurrence'))"
    ),
    row_count: int = 1,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE runner_metadata (
                {singleton_column},
                strategy_spec_fingerprint TEXT NOT NULL,
                {spec_json_column},
                evaluator_contract_fingerprint TEXT NOT NULL,
                {input_mode_column}
            )
            """
        )
        connection.executemany(
            "INSERT INTO runner_metadata VALUES (1, ?, ?, ?, NULL)",
            [
                (
                    _spec().spec_fingerprint,
                    _spec().model_dump_json(),
                    EVALUATOR_FINGERPRINT,
                )
                for _ in range(row_count)
            ],
        )


def _create_processed_batch_table(
    path: Path,
    *,
    receipt_sequence_type: str | None = None,
    feature_batch_unique: bool = True,
    source_index_sql: str | None = None,
) -> None:
    receipt_columns = ""
    if receipt_sequence_type is not None:
        receipt_columns = f"""
            , source_generation_id TEXT
            , source_sequence {receipt_sequence_type}
            , source_batch_id TEXT
            , source_content_hash TEXT
        """
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE processed_batch (
                feature_sequence INTEGER PRIMARY KEY,
                feature_batch_id TEXT NOT NULL {"UNIQUE" if feature_batch_unique else ""},
                envelope_fingerprint TEXT NOT NULL,
                feature_payload_hash TEXT NOT NULL,
                dataset_snapshot_id TEXT NOT NULL,
                event_time TEXT NOT NULL,
                available_at TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                result_json TEXT NOT NULL
                {receipt_columns}
            )
            """
        )
        if source_index_sql is not None:
            connection.execute(source_index_sql)


@pytest.mark.parametrize("mismatch", ["spec", "evaluator"])
def test_persisted_identity_mismatch_does_not_upgrade_legacy_candidate_schema(
    tmp_path: Path,
    mismatch: str,
) -> None:
    path = tmp_path / f"runner-{mismatch}.sqlite3"
    _create_legacy_candidate_state_table(path, populated=False)
    _create_persisted_runner_identity(
        path,
        spec_fingerprint=("f" * 64 if mismatch == "spec" else _spec().spec_fingerprint),
        evaluator_fingerprint=("f" * 64 if mismatch == "evaluator" else EVALUATOR_FINGERPRINT),
    )
    before = _candidate_state_columns(path)

    with pytest.raises(ValueError, match="strategy spec|evaluator contract"):
        _store(path)

    assert _candidate_state_columns(path) == before


@pytest.mark.parametrize(
    ("case", "kwargs"),
    (
        (
            "missing_pk",
            {"singleton_column": "singleton INTEGER CHECK(singleton = 1)"},
        ),
        (
            "missing_singleton_check",
            {"singleton_column": "singleton INTEGER PRIMARY KEY"},
        ),
        (
            "missing_not_null",
            {"spec_json_column": "strategy_spec_json TEXT"},
        ),
        (
            "wrong_mode_type",
            {
                "input_mode_column": (
                    "candidate_input_mode INTEGER "
                    "CHECK(candidate_input_mode IN ('flat', 'occurrence'))"
                )
            },
        ),
        (
            "permissive_mode_check",
            {
                "input_mode_column": (
                    "candidate_input_mode TEXT "
                    "CHECK(candidate_input_mode IN ('flat', 'occurrence', 'unsafe'))"
                )
            },
        ),
    ),
)
def test_runner_metadata_rejects_malformed_schema(
    tmp_path: Path,
    case: str,
    kwargs: dict[str, object],
) -> None:
    path = tmp_path / f"runner-{case}.sqlite3"
    _create_custom_runner_metadata(path, **kwargs)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="runner_metadata schema"):
        _store(path)


def test_runner_metadata_rejects_duplicate_singleton_rows(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_custom_runner_metadata(
        path,
        singleton_column="singleton INTEGER CHECK(singleton = 1)",
        row_count=2,
    )

    with pytest.raises(ValueError, match="runner_metadata schema|singleton"):
        _store(path)


def test_initialization_failure_rolls_back_candidate_schema_upgrade(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_legacy_candidate_state_table(path, populated=False)
    _create_persisted_runner_identity(
        path,
        spec_fingerprint=_spec().spec_fingerprint,
        evaluator_fingerprint=EVALUATOR_FINGERPRINT,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE runner_source_identity (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                source_generation_id TEXT NOT NULL
            )
            """
        )
        connection.execute("INSERT INTO runner_source_identity VALUES (1, 'invalid')")
    before = _candidate_state_columns(path)

    with pytest.raises(ValueError, match="source_generation_id"):
        _store(path)

    assert _candidate_state_columns(path) == before


def test_failure_after_candidate_schema_migration_rolls_back_upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_legacy_candidate_state_table(path, populated=False)
    before = _candidate_state_columns(path)

    def fail_after_candidate_migration(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("failure after candidate migration")

    monkeypatch.setattr(
        StrategyRunnerStore,
        "_ensure_processed_batch_schema",
        staticmethod(fail_after_candidate_migration),
    )

    with pytest.raises(RuntimeError, match="failure after candidate migration"):
        _store(path)

    assert _candidate_state_columns(path) == before


@pytest.mark.parametrize(
    ("occurrence_column", "candidate_column"),
    (
        ("occurrence_id INTEGER NOT NULL PRIMARY KEY", "candidate_id TEXT NOT NULL"),
        ("occurrence_id TEXT NOT NULL PRIMARY KEY", "candidate_id TEXT"),
        ("occurrence_id TEXT NOT NULL", "candidate_id TEXT NOT NULL"),
    ),
)
def test_candidate_state_schema_rejects_matching_names_with_broken_constraints(
    tmp_path: Path,
    occurrence_column: str,
    candidate_column: str,
) -> None:
    path = tmp_path / "runner.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE candidate_state (
                {occurrence_column},
                {candidate_column},
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

    with pytest.raises(ValueError, match="candidate_state schema"):
        _store(path)


def test_empty_legacy_candidate_state_table_upgrades_atomically(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_legacy_candidate_state_table(path, populated=False)

    store = _store(path)

    with sqlite3.connect(path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(candidate_state)")}
    assert {
        "occurrence_id",
        "candidate_id",
        "candidate_effective_trade_date",
        "candidate_variant",
        "candidate_generation_sha256",
        "candidate_snapshot_schema_version",
    } <= columns
    assert store.candidate_state("300001.SZ") is None


def test_nonempty_legacy_candidate_state_table_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_legacy_candidate_state_table(path, populated=True)

    with pytest.raises(ValueError, match="non-empty legacy candidate_state"):
        _store(path)


def test_legacy_processed_batch_schema_upgrades_with_nullable_source_receipt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_processed_batch_table(path)

    _store(path)

    with sqlite3.connect(path) as connection:
        schema = {
            row[1]: (row[2], row[3], row[5])
            for row in connection.execute("PRAGMA table_info(processed_batch)")
        }
    assert schema["source_generation_id"] == ("TEXT", 0, 0)
    assert schema["source_sequence"] == ("INTEGER", 0, 0)
    assert schema["source_batch_id"] == ("TEXT", 0, 0)
    assert schema["source_content_hash"] == ("TEXT", 0, 0)
    with sqlite3.connect(path) as connection:
        indexes = {
            row[1]: (row[2], row[4])
            for row in connection.execute("PRAGMA index_list(processed_batch)")
        }
        source_index_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("processed_batch_source_sequence_uq",),
        ).fetchone()[0]
    assert indexes["processed_batch_source_sequence_uq"] == (1, 1)
    assert source_index_sql is not None
    assert "WHERE source_sequence IS NOT NULL" in source_index_sql


def test_processed_batch_rejects_source_receipt_with_wrong_column_type(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_processed_batch_table(path, receipt_sequence_type="TEXT")

    with pytest.raises(ValueError, match="processed_batch source receipt schema"):
        _store(path)


def test_processed_batch_rejects_missing_feature_batch_unique_constraint(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_processed_batch_table(path, feature_batch_unique=False)

    with pytest.raises(ValueError, match="feature_batch_id.*UNIQUE"):
        _store(path)


@pytest.mark.parametrize(
    "source_index_sql",
    (
        "CREATE INDEX processed_batch_source_sequence_uq ON processed_batch(source_sequence)",
        "CREATE UNIQUE INDEX processed_batch_source_sequence_uq "
        "ON processed_batch(source_sequence, source_batch_id) "
        "WHERE source_sequence IS NOT NULL",
        "CREATE UNIQUE INDEX processed_batch_source_sequence_uq "
        "ON processed_batch(source_sequence) WHERE source_sequence >= 0",
    ),
)
def test_processed_batch_rejects_wrong_named_source_receipt_index(
    tmp_path: Path,
    source_index_sql: str,
) -> None:
    path = tmp_path / "runner.sqlite3"
    _create_processed_batch_table(
        path,
        receipt_sequence_type="INTEGER",
        source_index_sql=source_index_sql,
    )

    with pytest.raises(ValueError, match="processed_batch source sequence index"):
        _store(path)


def test_feature_batch_id_cannot_be_reused_across_sequences(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")
    store.process_batch(
        _envelope(batch_id="shared-feature-batch"),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: None,
    )

    with pytest.raises(sqlite3.IntegrityError, match="feature_batch_id"):
        store.process_batch(
            _envelope(sequence=1, batch_id="shared-feature-batch"),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW + timedelta(minutes=1),
            evaluator=lambda *_args: None,
        )

    assert store.last_batch_sequence() == 0


def test_runner_source_identity_rejects_duplicate_singleton_rows(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE runner_source_identity (
                singleton INTEGER,
                source_generation_id TEXT NOT NULL
            )
            """
        )
        connection.executemany(
            "INSERT INTO runner_source_identity VALUES (1, ?)",
            [("a" * 64,), ("b" * 64,)],
        )

    with pytest.raises(ValueError, match="runner_source_identity schema|singleton"):
        _store(path)


def test_runner_signal_rejects_missing_signal_id_unique_constraint(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE runner_signal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL,
                feature_sequence INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )

    with pytest.raises(ValueError, match="runner_signal.*UNIQUE"):
        _store(path)


@pytest.mark.parametrize(
    ("case", "sequence_column", "feature_sequence_column"),
    (
        (
            "missing_autoincrement",
            "sequence INTEGER PRIMARY KEY",
            "feature_sequence INTEGER NOT NULL",
        ),
        (
            "extra_check",
            "sequence INTEGER PRIMARY KEY AUTOINCREMENT",
            "feature_sequence INTEGER NOT NULL CHECK(feature_sequence < 10)",
        ),
    ),
)
def test_runner_signal_rejects_noncanonical_table_ddl(
    tmp_path: Path,
    case: str,
    sequence_column: str,
    feature_sequence_column: str,
) -> None:
    path = tmp_path / f"runner-{case}.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE runner_signal (
                {sequence_column},
                signal_id TEXT NOT NULL UNIQUE,
                {feature_sequence_column},
                payload_json TEXT NOT NULL
            )
            """
        )

    with pytest.raises(ValueError, match="runner_signal canonical DDL"):
        _store(path)


def test_runner_signal_accepts_canonical_legacy_table_and_preserves_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE runner_signal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL UNIQUE,
                feature_sequence INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO runner_signal VALUES (?, ?, ?, ?)",
            (7, "legacy-signal", 3, '{"legacy":true}'),
        )

    _store(path)

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT sequence, signal_id, feature_sequence, payload_json FROM runner_signal"
        ).fetchone()
    assert row == (7, "legacy-signal", 3, '{"legacy":true}')


def test_process_batch_rejects_extended_metadata_and_row_tampering(tmp_path: Path) -> None:
    joined = _joined_feature_batch(tmp_path)
    store = _store(tmp_path / "runner.sqlite3")
    metadata = json.loads(joined.payload_json)
    metadata["forged_metadata"] = True
    metadata_payload = json.dumps(
        metadata,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    with pytest.raises(StrategyBatchConflictError, match="hash"):
        store.process_batch(
            joined.envelope,
            joined.frame,
            feature_payload=metadata_payload,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=_entry_decision,
        )

    rows = json.loads(joined.payload_json)
    rows["rows"][0]["rel_same_minute"] = 999.0
    row_payload = json.dumps(
        rows,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    forged_envelope = joined.envelope.model_copy(
        update={"content_hash": hashlib.sha256(row_payload).hexdigest()}
    )
    with pytest.raises(StrategyBatchConflictError, match="rows"):
        store.process_batch(
            forged_envelope,
            joined.frame,
            feature_payload=row_payload,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=_entry_decision,
        )

    assert store.last_batch_sequence() == -1


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b'{"schema_version":1,"rows":[],"rows":[]}', "duplicate"),
        (b'{"schema_version":1,"rows":[NaN]}', "constant"),
        (b"[]", "object"),
        (b'{ "rows": [], "schema_version": 1 }', "canonical"),
    ],
)
def test_process_batch_rejects_unsafe_supplied_json_payloads(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    store = _store(tmp_path / f"runner-{message}.sqlite3")
    empty = pd.DataFrame(columns=("ts_code", "rel_same_minute"))
    envelope = _envelope(
        row_count=0,
        content_hash=hashlib.sha256(payload).hexdigest(),
    )

    with pytest.raises(StrategyBatchConflictError, match=message):
        store.process_batch(
            envelope,
            empty,
            feature_payload=payload,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )


def test_empty_joined_batch_advances_cursor_without_static_contract_fields(
    tmp_path: Path,
) -> None:
    joined = _joined_feature_batch(tmp_path, empty_requested_authority=True)
    required = (
        FeatureRequirement(
            name="rel_same_minute",
            level=RequirementLevel.REQUIRED,
            min_contract_version=1,
        ),
        FeatureRequirement(
            name="candidate_score",
            level=RequirementLevel.REQUIRED,
            min_contract_version=1,
        ),
    )
    store = _store(tmp_path / "runner.sqlite3", spec=_spec(required_features=required))

    result = store.process_batch(
        joined.envelope,
        joined.frame,
        feature_payload=joined.payload_bytes,
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: pytest.fail("empty batch must not evaluate candidates"),
    )

    assert joined.static_feature_names == ()
    assert joined.envelope.row_count == len(joined.frame) == 0
    assert result.processed_candidates == 0
    assert result.skipped_candidates == 0
    assert result.signals == ()
    assert store.last_batch_sequence() == 0


@pytest.mark.parametrize("first_mode", ["flat", "occurrence"])
def test_candidate_input_mode_cannot_switch_after_reopen(
    tmp_path: Path,
    first_mode: str,
) -> None:
    path = tmp_path / "runner.sqlite3"
    first = _store(path)
    if first_mode == "flat":
        first.process_batch(
            _envelope(),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )
        second_envelope = _joined_feature_batch(
            tmp_path,
            sequence=1,
            available_at=NOW + timedelta(minutes=1),
        )
        second_frame = second_envelope.frame
        second_payload = second_envelope.payload_bytes
        envelope = second_envelope.envelope
    else:
        joined = _joined_feature_batch(tmp_path)
        first.process_batch(
            joined.envelope,
            joined.frame,
            feature_payload=joined.payload_bytes,
            dataset_snapshot_id=joined.envelope.input_fingerprint,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )
        second_frame = _frame()
        envelope = _envelope(sequence=1)
        second_payload = None

    with pytest.raises(StrategyBatchConflictError, match="candidate input mode"):
        _store(path).process_batch(
            envelope,
            second_frame,
            feature_payload=second_payload,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW + timedelta(minutes=1),
            evaluator=lambda *_args: pytest.fail("mode switch must fail before evaluation"),
        )

    assert _store(path).last_batch_sequence() == 0


def test_zero_row_joined_batch_locks_occurrence_input_mode(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    joined = _joined_feature_batch(tmp_path, empty_requested_authority=True)
    _store(path).process_batch(
        joined.envelope,
        joined.frame,
        feature_payload=joined.payload_bytes,
        dataset_snapshot_id=joined.envelope.input_fingerprint,
        observed_at=NOW,
        evaluator=lambda *_args: pytest.fail("empty joined batch must not evaluate"),
    )

    with pytest.raises(StrategyBatchConflictError, match="candidate input mode"):
        _store(path).process_batch(
            _envelope(sequence=1),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW + timedelta(minutes=1),
            evaluator=lambda *_args: None,
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("source_generation_id", "f" * 64),
        ("source_batch_id", "replacement-source-batch"),
        ("source_content_hash", "f" * 64),
    ),
)
def test_source_batch_replay_requires_all_exact_source_evidence(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    store = _store(tmp_path / "runner.sqlite3")
    envelope = _envelope()
    receipt = StrategySourceBatchReceipt(
        source_generation_id="a" * 64,
        source_sequence=envelope.sequence,
        source_batch_id="common-feature-0",
        source_content_hash=envelope.content_hash,
    )
    expected = store.process_batch(
        envelope,
        _frame(),
        source_receipt=receipt,
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: None,
    )

    assert store.replay_source_batch(receipt, observed_at=NOW) == expected
    changed = receipt.model_copy(update={field: replacement})
    with pytest.raises(StrategyBatchConflictError, match="source batch receipt"):
        store.replay_source_batch(changed, observed_at=NOW)


def test_batch_sequence_gap_and_conflicting_replay_fail_closed(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    with pytest.raises(StrategyBatchConflictError, match="sequence"):
        store.process_batch(
            _envelope(sequence=1),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW + timedelta(minutes=1),
            evaluator=_entry_decision,
        )

    store.process_batch(
        _envelope(),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )
    with pytest.raises(StrategyBatchConflictError, match="immutable batch"):
        replacement = _frame(3.0)
        store.process_batch(
            _envelope(content_hash=_payload_hash(replacement)),
            replacement,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=_entry_decision,
        )


@pytest.mark.parametrize(
    ("label", "first_envelope", "first_observed_at", "next_envelope", "next_observed_at"),
    (
        (
            "event_time",
            _envelope(event_time=NOW, available_at=NOW + timedelta(minutes=10)),
            NOW + timedelta(minutes=10),
            _envelope(
                sequence=1,
                event_time=NOW - timedelta(seconds=1),
                available_at=NOW + timedelta(minutes=10),
            ),
            NOW + timedelta(minutes=11),
        ),
        (
            "available_at",
            _envelope(event_time=NOW, available_at=NOW + timedelta(minutes=10)),
            NOW + timedelta(minutes=10),
            _envelope(
                sequence=1,
                event_time=NOW + timedelta(minutes=1),
                available_at=NOW + timedelta(minutes=4),
            ),
            NOW + timedelta(minutes=11),
        ),
        (
            "observed_at",
            _envelope(event_time=NOW, available_at=NOW + timedelta(minutes=1)),
            NOW + timedelta(minutes=10),
            _envelope(
                sequence=1,
                event_time=NOW + timedelta(minutes=1),
                available_at=NOW + timedelta(minutes=1),
            ),
            NOW + timedelta(minutes=5),
        ),
    ),
)
def test_batch_pit_times_cannot_move_backwards(
    tmp_path: Path,
    label: str,
    first_envelope: FeatureBatchEnvelope,
    first_observed_at: datetime,
    next_envelope: FeatureBatchEnvelope,
    next_observed_at: datetime,
) -> None:
    store = _store(tmp_path / f"{label}.sqlite3")
    store.process_batch(
        first_envelope,
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=first_observed_at,
        evaluator=lambda *_args: None,
    )

    with pytest.raises(StrategyBatchConflictError, match=label):
        store.process_batch(
            next_envelope,
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=next_observed_at,
            evaluator=lambda *_args: None,
        )

    assert store.last_batch_sequence() == 0


def test_runner_rejects_future_or_incompatible_feature_batches(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    with pytest.raises(ValueError, match="available_at"):
        store.process_batch(
            _envelope(available_at=NOW + timedelta(seconds=1)),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=_entry_decision,
        )
    with pytest.raises(ValueError, match="feature contract"):
        store.process_batch(
            _envelope(contract_id="other"),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=_entry_decision,
        )


def test_dataset_snapshot_id_is_validated_even_without_signal(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    with pytest.raises(ValueError, match="dataset_snapshot_id"):
        store.process_batch(
            _envelope(),
            _frame(),
            dataset_snapshot_id="not-a-sha",
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )

    assert store.last_batch_sequence() == -1


def test_required_feature_unavailable_skips_candidate_without_calling_evaluator(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    result = store.process_batch(
        _envelope(status=FeatureAvailability.UNAVAILABLE),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: pytest.fail("unavailable feature must not be evaluated"),
    )

    assert result.processed_candidates == 1
    assert result.transitioned_candidates == 0
    assert result.skipped_candidates == 1
    assert result.signals == ()
    assert store.candidate_state("300001.SZ").state is StrategyLifecycleState.IDLE


def test_evaluator_only_sees_declared_currently_usable_features(tmp_path: Path) -> None:
    optional = (
        FeatureRequirement(
            name="optional_valid",
            level=RequirementLevel.OPTIONAL,
            min_contract_version=1,
        ),
        FeatureRequirement(
            name="optional_stale",
            level=RequirementLevel.OPTIONAL,
            min_contract_version=1,
        ),
        FeatureRequirement(
            name="optional_future_contract",
            level=RequirementLevel.OPTIONAL,
            min_contract_version=2,
        ),
        FeatureRequirement(
            name="optional_degraded_allowed",
            level=RequirementLevel.OPTIONAL,
            min_contract_version=1,
            allow_degraded=True,
        ),
        FeatureRequirement(
            name="optional_degraded_blocked",
            level=RequirementLevel.OPTIONAL,
            min_contract_version=1,
        ),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["300001.SZ"],
            "rel_same_minute": [2.0],
            "optional_valid": [7.0],
            "optional_stale": [8.0],
            "optional_future_contract": [9.0],
            "optional_degraded_allowed": [10.0],
            "optional_degraded_blocked": [11.0],
            "future_return": [99.0],
        }
    )
    statuses = (
        _status("rel_same_minute"),
        _status("optional_valid"),
        _status("optional_stale", status=FeatureAvailability.STALE),
        _status(
            "optional_degraded_allowed",
            status=FeatureAvailability.DEGRADED,
        ),
        _status(
            "optional_degraded_blocked",
            status=FeatureAvailability.DEGRADED,
        ),
    )
    seen: dict[str, object] = {}
    store = _store(tmp_path / "runner.sqlite3", spec=_spec(optional_features=optional))

    result = store.process_batch(
        _envelope(
            content_hash=_payload_hash(frame),
            field_statuses=statuses,
        ),
        frame,
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda _spec, _state, features: seen.update(features) or None,
    )

    assert result.skipped_candidates == 0
    assert seen == {
        "rel_same_minute": 2.0,
        "optional_valid": 7.0,
        "optional_degraded_allowed": 10.0,
    }


def test_structural_feature_contract_errors_do_not_commit_batch(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    with pytest.raises(ValueError, match="field status.*rel_same_minute"):
        store.process_batch(
            _envelope(field_statuses=()),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )

    missing_column = pd.DataFrame({"ts_code": ["300001.SZ"]})
    with pytest.raises(ValueError, match="missing feature columns.*rel_same_minute"):
        store.process_batch(
            _envelope(content_hash=_payload_hash(missing_column)),
            missing_column,
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: None,
        )

    assert store.last_batch_sequence() == -1
    assert store.candidate_state("300001.SZ") is None


def test_candidate_missing_required_scalar_is_skipped_and_committed(tmp_path: Path) -> None:
    frame = _frame(float("nan"))
    store = _store(tmp_path / "runner.sqlite3")

    result = store.process_batch(
        _envelope(content_hash=_payload_hash(frame)),
        frame,
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: pytest.fail("missing scalar must not be evaluated"),
    )

    assert result.skipped_candidates == 1
    assert store.last_batch_sequence() == 0


def test_transition_state_and_action_contracts_fail_closed(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")
    store.process_batch(
        _envelope(),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    def reset_with_buy(*_args: object) -> StrategyDecision:
        return StrategyDecision(
            event="reset",
            expected_from_state=StrategyLifecycleState.ARMED,
            expected_to_state=StrategyLifecycleState.IDLE,
            expected_action=SignalAction.B_INTENT,
            action=SignalAction.B_INTENT,
            reason_codes=("invalid_reset_buy",),
            evidence={},
            expires_after=timedelta(minutes=5),
        )

    with pytest.raises(ValueError, match="b_intent.*idle"):
        store.process_batch(
            _envelope(sequence=1),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW + timedelta(minutes=1),
            evaluator=reset_with_buy,
        )

    assert store.last_batch_sequence() == 0
    assert store.candidate_state("300001.SZ").state is StrategyLifecycleState.ARMED


def test_transition_evidence_makes_repeated_signal_identity_auditable(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    def envelope(sequence: int) -> FeatureBatchEnvelope:
        return _envelope(
            sequence=sequence,
            event_time=NOW,
            available_at=NOW - timedelta(minutes=sequence),
        )

    first = store.process_batch(
        envelope(0),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )
    store.process_batch(
        envelope(1),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=lambda *_args: StrategyDecision(
            event="reset",
            expected_from_state=StrategyLifecycleState.ARMED,
            expected_to_state=StrategyLifecycleState.IDLE,
            expected_action=None,
        ),
    )
    second = store.process_batch(
        envelope(2),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )

    assert first.signals[0].signal.signal_id != second.signals[0].signal.signal_id
    assert first.signals[0].signal.evidence["runner_transition"]["feature_sequence"] == 0
    assert second.signals[0].signal.evidence["runner_transition"]["feature_sequence"] == 2


def test_concurrent_exact_batch_is_evaluated_once(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    first_store = _store(path)
    second_store = _store(path)
    entered = Event()
    release = Event()
    counter_lock = Lock()
    evaluations = 0

    def evaluator(*args: object) -> StrategyDecision:
        nonlocal evaluations
        with counter_lock:
            evaluations += 1
        entered.set()
        assert release.wait(timeout=5)
        return _entry_decision(*args)

    def run(store: StrategyRunnerStore) -> strategy_runner.StrategyBatchResult:
        return store.process_batch(
            _envelope(),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=evaluator,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(run, first_store)
        assert entered.wait(timeout=5)
        second_future = executor.submit(run, second_store)
        release.set()
        first_result = first_future.result(timeout=10)
        second_result = second_future.result(timeout=10)

    assert first_result == second_result
    assert evaluations == 1
    assert len(first_store.signals_after(sequence=0)) == 1


def test_process_kill_rolls_back_and_batch_can_be_replayed(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _store(path)

    def crash_inside_evaluator() -> None:
        store = _store(path)
        store.process_batch(
            _envelope(),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: os._exit(91),
        )

    process = multiprocessing.get_context("fork").Process(target=crash_inside_evaluator)
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 91

    recovered = _store(path)
    assert recovered.last_batch_sequence() == -1
    assert recovered.candidate_state("300001.SZ") is None
    assert recovered.signals_after(sequence=0) == ()

    result = recovered.process_batch(
        _envelope(),
        _frame(),
        dataset_snapshot_id="d" * 64,
        observed_at=NOW,
        evaluator=_entry_decision,
    )
    assert len(result.signals) == 1


def test_evaluator_failure_rolls_back_whole_batch(tmp_path: Path) -> None:
    store = _store(tmp_path / "runner.sqlite3")

    with pytest.raises(RuntimeError, match="boom"):
        store.process_batch(
            _envelope(),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=NOW,
            evaluator=lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")),
        )

    assert store.last_batch_sequence() == -1
    assert store.signals_after(sequence=0) == ()


def test_runner_database_is_bound_to_one_exact_strategy_spec(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _store(path)

    with pytest.raises(ValueError, match="strategy spec"):
        _store(path, spec=_spec(producer_commit="f" * 40))


def test_runner_database_is_bound_to_evaluator_contract(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _store(path)

    with pytest.raises(ValueError, match="evaluator contract"):
        _store(path, evaluator_contract_fingerprint="f" * 64)


def test_runner_source_generation_survives_reopen_but_changes_on_rebuild(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    first = _store(path)
    generation = first.source_generation_id

    assert _store(path).source_generation_id == generation
    assert first.signal_high_watermark() == 0

    path.unlink()
    rebuilt = _store(path)

    assert rebuilt.source_generation_id != generation
