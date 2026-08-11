from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd
import pytest

from rquant.dashboard.runtime_console_data import (
    ConsoleLoadState,
    load_runtime_console,
)
from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxStatus,
)
from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import (
    IntradayFeatureConfig,
    live_compute,
    replay_compute,
)
from rquant.lab_job_protocol import LabCommandEnvelope, ResumeJobCommand
from rquant.lab_jobs import JobStatus, LabJobReader
from rquant.notification_worker import NotificationDelivery, run_notification_batch
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import (
    ServingPublisher,
    ServingReader,
    ServingTableSpec,
)
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
    strategy_candidate_schema_fingerprint,
)
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_runner import (
    StrategyCandidateState,
    StrategyDecision,
    StrategyRunnerStore,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)
from tests.unit.test_lab_shard_control_plane import (
    NOW as LAB_NOW,
)
from tests.unit.test_lab_shard_control_plane import (
    _claim,
    _pause,
    _report,
    _setup,
    _success,
)

SHANGHAI = timezone(timedelta(hours=8))
DECISION_LOCAL = datetime(2026, 7, 31, 9, 40, 2, tzinfo=SHANGHAI)
DECISION_UTC = DECISION_LOCAL.astimezone(UTC)
PRODUCER_COMMIT = "a" * 40
EVALUATOR_FINGERPRINT = "b" * 64
DEFINITION_FINGERPRINT = "c" * 64
STATIC_FEATURE_SCHEMA = {
    "candidate_score": {"dtype": "number", "semantic": "candidate ranking score"}
}
CANDIDATE_SCHEMA_FINGERPRINT = strategy_candidate_schema_fingerprint(
    strategy_id="fault-matrix",
    strategy_version="1",
    static_feature_schema=STATIC_FEATURE_SCHEMA,
)


def _file_signatures(root: Path) -> dict[str, tuple[int, int, int, int]]:
    return {
        path.relative_to(root).as_posix(): (
            observed.st_mode,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
            observed.st_size,
        )
        for path in sorted((root, *root.rglob("*")))
        if not path.is_symlink()
        for observed in (os.stat(path, follow_symlinks=False),)
    }


def _minute_row(
    ts_code: str,
    trade_time: datetime,
    *,
    amount: float,
    close: float = 10.0,
) -> dict[str, object]:
    localized = trade_time.replace(tzinfo=trade_time.tzinfo or SHANGHAI)
    return {
        "ts_code": ts_code,
        "trade_time": trade_time,
        "available_at": localized + timedelta(seconds=2),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "vol": amount / close,
        "amount": amount,
    }


def _point_in_time_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    current = pd.DataFrame(
        [
            _minute_row(
                "600000.SH",
                datetime(2026, 7, 31, 9, 40),
                amount=10_000.0,
                close=10.1,
            )
        ]
    )
    historical = pd.DataFrame(
        [
            _minute_row(
                "600000.SH",
                datetime(2026, 7, day, 9, 40),
                amount=amount,
            )
            for day, amount in ((29, 4_000.0), (30, 6_000.0))
        ]
    )
    return current, historical


def _feature_config() -> IntradayFeatureConfig:
    return IntradayFeatureConfig(
        lookback_sessions=2,
        opening_acceleration_block_minutes=3,
        producer_commit=PRODUCER_COMMIT,
    )


def _strategy_spec() -> StrategySpec:
    return StrategySpec(
        strategy_id="fault-matrix",
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=3,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=3,
            ),
        ),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={"min_ratio": 1.4},
        allowed_actions=(SignalAction.B_INTENT.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=PRODUCER_COMMIT,
    )


def _evaluate(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: dict[str, object],
) -> StrategyDecision | None:
    ratio = float(features["rel_same_minute"])
    if ratio < float(spec.parameters["min_ratio"]):
        return None
    return StrategyDecision(
        event="entry_ready",
        expected_from_state=state.state,
        expected_to_state=StrategyLifecycleState.ARMED,
        expected_action=SignalAction.B_INTENT,
        action=SignalAction.B_INTENT,
        reason_codes=("same_minute_volume",),
        evidence={"rel_same_minute": ratio},
        expires_after=timedelta(minutes=5),
    )


def _publish_feature(root: Path, *, replay: bool = False) -> FeatureBatchSpool:
    current, historical = _point_in_time_frames()
    compute = replay_compute if replay else live_compute
    result = compute(
        current,
        historical,
        decision_time=DECISION_LOCAL,
        input_available_at=DECISION_UTC,
        input_batch_ids=("current-20260731-0940", "history-through-20260730"),
        sequence=0,
        config=_feature_config(),
    )
    spool = FeatureBatchSpool(root / "features")
    spool.publish(result.envelope, result.payload_bytes)
    return spool


def _runner(path: Path) -> StrategyRunnerStore:
    return StrategyRunnerStore(
        path,
        spec=_strategy_spec(),
        evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
    )


def _candidate_loader(root: Path) -> RuntimeCandidateUniverseLoader:
    snapshot_root = (root / "candidate-snapshots").resolve()
    decision_at = DECISION_UTC - timedelta(days=1)
    spec = _strategy_spec()
    StrategyCandidateSnapshotSpool(snapshot_root).publish_strategy_records(
        strategy_id=spec.strategy_id,
        strategy_version=str(spec.version),
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EVALUATOR_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate-input": "8" * 64},
        trade_date=DECISION_LOCAL.date(),
        captured_at=DECISION_UTC,
        producer_commit=PRODUCER_COMMIT,
        rows=(
            StrategyCandidateRecord(
                strategy_id=spec.strategy_id,
                strategy_version=str(spec.version),
                candidate_id="600000.SH",
                variant="fault-matrix",
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=DECISION_LOCAL.date(),
                reference_trade_date=(DECISION_LOCAL - timedelta(days=1)).date(),
                price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                static_features={"candidate_score": 0.9},
                reference_snapshot_ids={"daily": "9" * 64},
            ),
        ),
    )
    return RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit=PRODUCER_COMMIT,
            authorities=(
                CandidateUniverseAuthority(
                    strategy_id=_strategy_spec().strategy_id,
                    strategy_version=str(_strategy_spec().version),
                    snapshot_root=snapshot_root,
                    required=True,
                    max_age_seconds=60,
                    definition_fingerprint=DEFINITION_FINGERPRINT,
                    executable_fingerprint=EVALUATOR_FINGERPRINT,
                    candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
                    static_feature_names=("candidate_score",),
                    static_feature_schema=STATIC_FEATURE_SCHEMA,
                ),
            ),
        )
    )


def _run_one_strategy_signal(root: Path, *, replay: bool = False) -> StrategyRunnerStore:
    spool = _publish_feature(root, replay=replay)
    runner = _runner(root / "runner.sqlite3")
    summary = run_strategy_live_batch(
        feature_spool=spool,
        candidate_universe_loader=_candidate_loader(root),
        runner=runner,
        evaluator=_evaluate,
        observed_at=DECISION_UTC,
        limit=10,
    )
    assert summary.processed_count == 1
    assert summary.signal_count == 1
    return runner


def _publish_reference_generation(root: Path) -> tuple[ServingReader, Path]:
    publisher = ServingPublisher(
        root,
        producer_commit="d" * 40,
        table_specs={"signals": ServingTableSpec(sort_keys=("sequence",))},
    )
    manifest = publisher.publish(
        {"signals": pd.DataFrame({"sequence": [1], "candidate": ["600000.SH"]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="research-result",
                generation_id="e" * 64,
                event_time=DECISION_UTC,
                published_at=DECISION_UTC,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"research-result": "e" * 64},
        built_at=DECISION_UTC,
    )
    database = root / "generations" / manifest.generation_id / "serving.duckdb"
    return ServingReader(root), database


def test_worker_loss_does_not_block_live_or_page_checkpoint_resume(
    tmp_path: Path,
) -> None:
    reader, published_database = _publish_reference_generation(tmp_path / "published")
    published_manifest = reader.current_manifest()
    published_before = _file_signatures(tmp_path / "published")

    store, lease, job_id = _setup(
        tmp_path / "lab",
        count=2,
        with_work_plan=True,
    )
    abandoned = _claim(store, lease, worker="worker-a", duration=5)
    page = LabJobReader(store.path)
    assert page.get_job(job_id).status is JobStatus.RUNNING  # type: ignore[union-attr]

    live_runner = _run_one_strategy_signal(tmp_path / "live-plane")
    assert live_runner.signal_high_watermark() == 1
    del page

    reclaimed = _claim(store, lease, worker="worker-b", now_offset=8, duration=30)
    assert reclaimed.shard_id == abandoned.shard_id
    assert reclaimed.claim_generation == abandoned.claim_generation + 1
    assert reclaimed.claim_token != abandoned.claim_token

    _pause(store, lease, job_id, offset=9)
    success = store.apply_worker_report(
        _report(reclaimed, _success(reclaimed, duration_ms=250), offset=10),
        lease=lease,
        now=LAB_NOW + timedelta(seconds=10),
    )
    assert success.status == "accepted"

    reopened_page = LabJobReader(store.path)
    checkpointed = reopened_page.get_job(job_id)
    assert checkpointed is not None
    assert checkpointed.status is JobStatus.CHECKPOINTED
    resume = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=checkpointed.version,
                reason="resume after isolated worker recovery",
            ),
        ),
        lease=lease,
        now=LAB_NOW + timedelta(seconds=11),
    )
    assert resume.status == "applied"
    assert _claim(store, lease, worker="worker-c", now_offset=12).shard_index == 1

    assert reader.current_manifest() == published_manifest
    assert published_database.is_file()
    assert _file_signatures(tmp_path / "published") == published_before


class _RecordingProvider:
    def __init__(self) -> None:
        self.calls: list[NotificationDelivery] = []

    def deliver(self, delivery: NotificationDelivery) -> str:
        self.calls.append(delivery)
        return f"pushdeer:{delivery.signal.signal_id}"


def _signal(seed: str, *, expires_at: datetime) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="fault-matrix",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="1" * 64,
        feature_snapshot_id="2" * 64,
        event_time=DECISION_UTC - timedelta(seconds=2),
        available_at=DECISION_UTC,
        candidate_id=f"60000{seed}.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("durable_outbox",),
        evidence={"seed": seed},
        expires_at=expires_at,
        producer_commit="f" * 40,
    )


def test_notification_pause_keeps_signals_and_skips_expired_on_resume(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    target = DeliveryTarget(
        recipient_id="admin",
        channel=DeliveryChannel.PUSHDEER,
    )
    live = _signal("1", expires_at=DECISION_UTC + timedelta(minutes=1))
    expiring = _signal("2", expires_at=DECISION_UTC + timedelta(seconds=5))
    outbox_ids: dict[str, str] = {}
    for signal in (live, expiring):
        bus.ingest(signal, received_at=DECISION_UTC)
        outbox_ids[signal.signal_id] = bus.route(
            signal.signal_id,
            (target,),
            now=DECISION_UTC,
        )[0].outbox_id

    assert (
        len(
            bus.signals_after_global_sequence(
                after_sequence=0,
                through_sequence=2,
                observed_at=DECISION_UTC,
                limit=10,
            )
        )
        == 2
    )
    assert {record.status for record in bus.outbox_records()} == {OutboxStatus.PENDING}

    resumed_at = DECISION_UTC + timedelta(seconds=10)
    provider = _RecordingProvider()
    summary = run_notification_batch(
        bus,
        {DeliveryChannel.PUSHDEER: provider},
        worker_id="notifier-resumed",
        now=resumed_at,
        lease_for=timedelta(seconds=20),
        limit=10,
        clock=lambda: resumed_at + timedelta(seconds=1),
    )

    assert summary.succeeded_count == 1
    assert [delivery.signal.signal_id for delivery in provider.calls] == [live.signal_id]
    assert bus.outbox_record(outbox_ids[live.signal_id]).status is OutboxStatus.SUCCEEDED  # type: ignore[union-attr]
    assert bus.outbox_record(outbox_ids[expiring.signal_id]).status is OutboxStatus.EXPIRED  # type: ignore[union-attr]
    assert bus.signal(live.signal_id) == live
    assert bus.signal(expiring.signal_id) == expiring


def test_missing_minute_is_degraded_and_never_substituted_with_zero() -> None:
    current_rows: list[dict[str, object]] = []
    history_rows: list[dict[str, object]] = []
    for code in ("600000.SH", "600001.SH"):
        for offset in range(7):
            if not (code == "600001.SH" and offset == 3):
                current_rows.append(
                    _minute_row(
                        code,
                        datetime(2026, 7, 31, 9, 30 + offset),
                        amount=float((offset + 1) * 1_000),
                    )
                )
        for day, scale in ((29, 500.0), (30, 1_000.0)):
            for offset in range(7):
                history_rows.append(
                    _minute_row(
                        code,
                        datetime(2026, 7, day, 9, 30 + offset),
                        amount=scale * (offset + 1),
                    )
                )

    result = live_compute(
        pd.DataFrame(current_rows),
        pd.DataFrame(history_rows),
        decision_time=datetime(2026, 7, 31, 9, 36, 2, tzinfo=SHANGHAI),
        input_available_at=datetime(2026, 7, 31, 1, 36, 2, tzinfo=UTC),
        input_batch_ids=("current-with-gap", "history-complete"),
        sequence=0,
        config=_feature_config(),
    )

    status = result.envelope.field_status(
        "amount_accel_5m",
        candidate_id="600001.SH",
    )
    rows = result.frame.set_index("ts_code")
    assert status is not None
    assert status.status is FeatureAvailability.UNAVAILABLE
    assert status.reason == "non_contiguous_minutes"
    assert pd.notna(rows.loc["600000.SH", "amount_accel_5m"])
    assert pd.isna(rows.loc["600001.SH", "amount_accel_5m"])


def test_runner_restart_replays_committed_batch_without_duplicate_signal(
    tmp_path: Path,
) -> None:
    spool = _publish_feature(tmp_path)
    runner_path = tmp_path / "runner.sqlite3"
    runner = _runner(runner_path)

    def crash_after_commit(stage: str) -> None:
        if stage == "after_runner_commit":
            raise RuntimeError("simulated runner termination")

    with pytest.raises(RuntimeError, match="runner termination"):
        run_strategy_live_batch(
            feature_spool=spool,
            candidate_universe_loader=_candidate_loader(tmp_path),
            runner=runner,
            evaluator=_evaluate,
            observed_at=DECISION_UTC,
            limit=10,
            fault_hook=crash_after_commit,
        )

    committed = runner.signals_after(sequence=0)
    assert len(committed) == 1
    recovered = run_strategy_live_batch(
        feature_spool=spool,
        candidate_universe_loader=_candidate_loader(tmp_path),
        runner=_runner(runner_path),
        evaluator=lambda *_args: pytest.fail("committed replay must not evaluate again"),
        observed_at=DECISION_UTC + timedelta(seconds=1),
        limit=10,
    )

    assert recovered.replayed_count == 1
    assert recovered.signal_count == 0
    assert _runner(runner_path).signals_after(sequence=0) == committed


def _replay_event_sequence(root: Path) -> tuple[str, ...]:
    runner = _run_one_strategy_signal(root, replay=True)
    runner_record = runner.signals_after(sequence=0)[0]
    signal = runner_record.signal
    bus = SignalBusStore(root / "signal-bus.sqlite3")
    accepted = bus.ingest(signal, received_at=DECISION_UTC)
    target = DeliveryTarget(
        recipient_id="admin",
        channel=DeliveryChannel.PUSHDEER,
    )
    outbox = bus.route(signal.signal_id, (target,), now=DECISION_UTC)[0]
    bus_record = bus.signals_after_global_sequence(
        after_sequence=0,
        through_sequence=accepted.global_sequence or 0,
        observed_at=DECISION_UTC,
        limit=10,
    )[0]
    feature = (
        FeatureBatchSpool(root / "features")
        .list_after(
            sequence=-1,
            through_sequence=0,
            limit=1,
        )[0]
        .envelope
    )
    events = (
        {"kind": "feature", "payload": feature.model_dump(mode="json")},
        {"kind": "runner", "payload": runner_record.model_dump(mode="json")},
        {"kind": "signal_bus", "payload": bus_record.model_dump(mode="json")},
        {"kind": "outbox", "payload": outbox.model_dump(mode="json")},
    )
    return tuple(
        json.dumps(event, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        for event in events
    )


def test_same_snapshot_replay_has_identical_results_and_event_order(
    tmp_path: Path,
) -> None:
    first = _replay_event_sequence(tmp_path / "first")
    second = _replay_event_sequence(tmp_path / "second")

    assert second == first


def test_serving_reader_and_console_never_write_production_or_serving_files(
    tmp_path: Path,
) -> None:
    production_root = tmp_path / "production"
    production_root.mkdir()
    production_database = production_root / "rquant.duckdb"
    with duckdb.connect(str(production_database)) as connection:
        connection.execute("CREATE TABLE sentinel(value INTEGER)")
        connection.execute("INSERT INTO sentinel VALUES (7)")
    production_database.chmod(0o400)

    serving_root = tmp_path / "serving"
    source_generation = "9" * 64
    publisher = ServingPublisher(
        serving_root,
        producer_commit="8" * 40,
        table_specs=SERVING_TABLE_SPECS,
    )
    publisher.publish(
        build_serving_read_models(ServingReadModelInput(observed_at=DECISION_UTC)),
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="runtime-console",
                generation_id=source_generation,
                event_time=DECISION_UTC,
                published_at=DECISION_UTC,
                sequence=0,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"runtime-console": source_generation},
        built_at=DECISION_UTC,
    )
    production_before = _file_signatures(production_root)
    serving_before = _file_signatures(serving_root)

    snapshot = load_runtime_console(
        serving_root,
        now=DECISION_UTC + timedelta(minutes=1),
    )
    with (
        ServingReader(serving_root).open_current_readonly() as connection,
        pytest.raises(duckdb.InvalidInputException, match="read-only"),
    ):
        connection.execute("CREATE TABLE forbidden(value INTEGER)")

    assert snapshot.state is ConsoleLoadState.READY
    assert snapshot.detail == "serving generation verified"
    assert _file_signatures(production_root) == production_before
    assert _file_signatures(serving_root) == serving_before
    assert not tuple(production_root.glob("*.wal"))
