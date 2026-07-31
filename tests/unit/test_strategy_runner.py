from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
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
from rquant.signal_contracts import SignalAction
from rquant.strategy_runner import (
    StrategyBatchConflictError,
    StrategyDecision,
    StrategyRunnerStore,
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
) -> StrategySpec:
    return StrategySpec(
        strategy_id="growth-board-surge-v1",
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=1,
        required_features=(
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
