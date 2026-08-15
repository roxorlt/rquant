"""Real A-to-D fixture wired through the isolated daily DAG orchestrator."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.daily_canonical_publisher import DailyCanonicalPublisher
from rquant.daily_close_candidate import DailyCloseCandidateStore
from rquant.daily_close_validation import DailyCloseValidator
from rquant.daily_ledger_fence import DailyLedgerFenceGuard
from rquant.daily_pipeline_ledger import LeaseLost, StageResult
from rquant.daily_pool_stage import DailyDownstreamArtifactStore, DailyPoolStage, DailyScreenStage
from rquant.daily_summary_stage import DailySummaryStage
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.presets import ScreenPreset
from rquant.screen.rules import not_st
from rquant.signal_bus import SignalBusStore
from rquant.storage.duckdb import DuckDBStore
from tests.integration.test_daily_downstream_e2e import (
    COMMITTED_AT,
    TRADE_DATE,
    _calendar,
    _policy,
    _published,
    _seed_database,
    _signer,
)


class _Adapter:
    def __init__(
        self,
        stage_id: str,
        runner: Callable,
        *,
        crash_after_run_once: bool = False,
    ) -> None:
        self.stage_id = stage_id
        self._runner = runner
        self._crash_after_run_once = crash_after_run_once
        self.calls = 0
        self._completed_attempts: dict[tuple[str, int], StageResult] = {}

    def health(self, _context):
        from rquant.daily_pipeline_orchestrator import DailyStageHealth

        return DailyStageHealth(ready=True, detail="fixture_ready")

    def run(self, context):
        self.calls += 1
        key = (context.run.run_id, context.attempt.attempt_number)
        recovered = self._completed_attempts.get(key)
        if recovered is not None:
            return recovered
        result = self._runner(context)
        self._completed_attempts[key] = result
        if self._crash_after_run_once:
            self._crash_after_run_once = False
            raise SystemExit(f"simulated crash after {self.stage_id} durable side effects")
        return result


class _StaticSourceResolver:
    """The fixture raw spool identity is immutable for the life of one run."""

    def resolve(self, run):
        from rquant.daily_pipeline_orchestrator import DailySourceIdentity

        return DailySourceIdentity(
            source_generation_id=run.spec.source_generation_id,
            source_content_hash=run.spec.source_content_hash,
        )


@pytest.mark.parametrize(
    ("failure_mode", "failure_stage"),
    (
        ("clean", None),
        ("crash", "raw_capture"),
        ("crash", "validate_candidate"),
        ("crash", "canonical_publish"),
        ("crash", "screen"),
        ("crash", "pool"),
        ("crash", "summary"),
        ("lease_expiry", "raw_capture"),
        ("lease_expiry", "validate_candidate"),
        ("lease_expiry", "canonical_publish"),
        ("lease_expiry", "screen"),
        ("lease_expiry", "pool"),
        ("lease_expiry", "summary"),
    ),
    ids=(
        "clean",
        "crash-raw",
        "crash-candidate",
        "crash-canonical",
        "crash-screen",
        "crash-pool",
        "crash-summary",
        "lease-raw",
        "lease-candidate",
        "lease-canonical",
        "lease-screen",
        "lease-pool",
        "lease-summary",
    ),
)
def test_real_daily_fixture_reaches_outbox_through_orchestrator_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
    failure_stage: str | None,
) -> None:
    from rquant.daily_pipeline_ledger import (
        DailyPipelineLedger,
        DailyPipelineMode,
        DailyPipelineStorageProfile,
    )
    from rquant.daily_pipeline_orchestrator import (
        DEFAULT_DAILY_CLOSE_PIPELINE,
        DailyPipelineDefinition,
        DailyPipelineOrchestrator,
    )

    gateway, record = _published(tmp_path / "raw")
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    candidates = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    storage_profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="b" * 64,
    )
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-shadow",
    )
    database = tmp_path / "canonical.duckdb"
    _seed_database(database)
    artifacts = DailyDownstreamArtifactStore(tmp_path / "artifacts")
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    state: dict[str, object] = {}
    targets = (DeliveryTarget(recipient_id="e2e", channel=DeliveryChannel.PUSHDEER),)

    presets = {
        "n-shape-pool1": ScreenPreset(name="n-shape-pool1", description="e2e", rules=[not_st()]),
        "n-shape-pool2": ScreenPreset(
            name="n-shape-pool2",
            description="e2e",
            rules=[not_st()],
            depends_on="n-shape-pool1",
            offset_days=1,
        ),
    }
    frame = pd.DataFrame(
        {"ts_code": ["600000.SH"], "name": ["test"], "CLOSE[0]": [10.0], "PCT_CHG[0]": [1.0]}
    )
    monkeypatch.setattr("rquant.pipeline.PRESET_SCREENS", presets)
    monkeypatch.setattr("rquant.pipeline.screen", lambda **_kwargs: frame)

    def raw_capture(context) -> StageResult:
        payload = gateway.decode_payload(gateway.spool.read_payload(record))
        assert context.run.spec.source_generation_id == verified.source_generation_id
        assert context.run.spec.source_content_hash == payload.content_sha256
        return StageResult(
            content_hash=payload.content_sha256,
            evidence_hash=verified.source_generation_id,
        )

    def validate_candidate(context) -> StageResult:
        current = DailyCloseValidator(
            spool=gateway.spool,
            policy=_policy(),
            calendar=_calendar(),
        ).validate(record)
        candidate = candidates.publish(
            current,
            spool=gateway.spool,
            attempt=context.attempt,
            published_at=context.observed_at,
            fence_guard=DailyLedgerFenceGuard(ledger=ledger, lease=context.lease),
        )
        state["candidate"] = candidate
        return StageResult(
            content_hash=candidate.generation_id,
            evidence_hash=current.validation_sha256,
        )

    def canonical_publish(context) -> StageResult:
        candidate = state["candidate"]
        publisher = DailyCanonicalPublisher(
            candidate_store=candidates,
            raw_spool=gateway.spool,
            indicator_reader_factory=lambda: DuckDBStore(database, read_only=True),
            writer_factory=lambda: DuckDBStore(database),
            ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=context.lease),
            clock=lambda: context.observed_at,
        )
        receipt = publisher.publish(
            candidate.generation_id,
            attempt=context.attempt,
            ledger_input_identity=context.run.input_identity,
            committed_at=context.observed_at,
        )
        state["canonical"] = receipt
        return receipt.stage_result

    def screen(context) -> StageResult:
        stage = DailyScreenStage.from_ledger(
            writer_factory=lambda: DuckDBStore(database),
            artifact_store=artifacts,
            ledger=ledger,
            lease=context.lease,
            clock=lambda: context.observed_at,
        )
        artifact = stage.run(
            state["canonical"],
            attempt=context.attempt,
            ledger_input_identity=context.run.input_identity,
        )
        state["screen"] = artifact
        return artifact.stage_result

    def pool(context) -> StageResult:
        stage = DailyPoolStage.from_ledger(
            writer_factory=lambda: DuckDBStore(database),
            artifact_store=artifacts,
            ledger=ledger,
            lease=context.lease,
            clock=lambda: context.observed_at,
        )
        artifact = stage.run(
            state["canonical"],
            screen_result=state["screen"],
            attempt=context.attempt,
            ledger_input_identity=context.run.input_identity,
        )
        state["pool"] = artifact
        return artifact.stage_result

    def summary(context) -> StageResult:
        stage = DailySummaryStage.from_ledger(
            signal_bus=bus,
            strategy_version="daily-close-dag/v1",
            producer_commit="a" * 40,
            clock=lambda: context.observed_at,
            artifact_store=artifacts,
            canonical_reader_factory=lambda: DuckDBStore(database, read_only=True),
            ledger=ledger,
            lease=context.lease,
            notification_targets=targets,
        )
        try:
            artifact = stage.run(
                state["canonical"],
                screen_result=state["screen"],
                pool_result=state["pool"],
                attempt=context.attempt,
                ledger_input_identity=context.run.input_identity,
            )
        except Exception as exc:
            pytest.fail(f"summary stage error: {type(exc).__name__}: {exc}")
        state["summary"] = artifact
        return artifact.stage_result

    adapters = (
        _Adapter(
            "raw_capture",
            raw_capture,
            crash_after_run_once=(failure_mode == "crash" and failure_stage == "raw_capture"),
        ),
        _Adapter(
            "validate_candidate",
            validate_candidate,
            crash_after_run_once=(
                failure_mode == "crash" and failure_stage == "validate_candidate"
            ),
        ),
        _Adapter(
            "canonical_publish",
            canonical_publish,
            crash_after_run_once=(failure_mode == "crash" and failure_stage == "canonical_publish"),
        ),
        _Adapter(
            "screen",
            screen,
            crash_after_run_once=(failure_mode == "crash" and failure_stage == "screen"),
        ),
        _Adapter(
            "pool",
            pool,
            crash_after_run_once=(failure_mode == "crash" and failure_stage == "pool"),
        ),
        _Adapter(
            "summary",
            summary,
            crash_after_run_once=(failure_mode == "crash" and failure_stage == "summary"),
        ),
    )
    fixture_definition = DailyPipelineDefinition(stages=DEFAULT_DAILY_CLOSE_PIPELINE.stages[:6])
    orchestrator = DailyPipelineOrchestrator(
        ledger=ledger,
        service_owner="daily-shadow",
        definition=fixture_definition,
        adapters=adapters,
        source_resolver=_StaticSourceResolver(),
        clock=lambda: COMMITTED_AT,
        lease_for=timedelta(minutes=15),
        execution_mode="test_fixture",
    )
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=TRADE_DATE,
        source_generation_id=verified.source_generation_id,
        source_content_hash=verified.raw_content_sha256,
        command_manifest_hash="e" * 64,
        code_commit="a" * 40,
        profile_hash="b" * 64,
        now=COMMITTED_AT,
    )

    outcomes = []
    if failure_mode == "lease_expiry":
        assert failure_stage is not None
        before_target = orchestrator.definition.stage_ids.index(failure_stage)
        for expected_stage in orchestrator.definition.stage_ids[:before_target]:
            outcome = orchestrator.advance(run.run_id, now=COMMITTED_AT)
            assert outcome is not None
            assert outcome.stage_id == expected_stage
            outcomes.append(outcome)
        expiring = DailyPipelineOrchestrator(
            ledger=ledger,
            service_owner="daily-shadow",
            definition=fixture_definition,
            adapters=adapters,
            source_resolver=_StaticSourceResolver(),
            clock=lambda: COMMITTED_AT + timedelta(minutes=16),
            lease_for=timedelta(minutes=15),
            execution_mode="test_fixture",
        )
        with pytest.raises(LeaseLost, match="writer lease is stale"):
            expiring.advance(run.run_id, now=COMMITTED_AT)
        orchestrator = DailyPipelineOrchestrator(
            ledger=ledger,
            service_owner="daily-shadow",
            definition=fixture_definition,
            adapters=adapters,
            source_resolver=_StaticSourceResolver(),
            clock=lambda: COMMITTED_AT,
            lease_for=timedelta(minutes=15),
            execution_mode="test_fixture",
        )
    try:
        while (outcome := orchestrator.advance(run.run_id, now=COMMITTED_AT)) is not None:
            outcomes.append(outcome)
    except SystemExit:
        assert failure_mode == "crash"
        assert failure_stage is not None
        orchestrator = DailyPipelineOrchestrator(
            ledger=ledger,
            service_owner="daily-shadow",
            definition=fixture_definition,
            adapters=adapters,
            source_resolver=_StaticSourceResolver(),
            clock=lambda: COMMITTED_AT,
            lease_for=timedelta(minutes=15),
            execution_mode="test_fixture",
        )
        while (outcome := orchestrator.advance(run.run_id, now=COMMITTED_AT)) is not None:
            outcomes.append(outcome)
    duplicate = orchestrator.advance(run.run_id, now=COMMITTED_AT)

    assert [outcome.stage_id for outcome in outcomes] == list(orchestrator.definition.stage_ids)
    assert [outcome.disposition for outcome in outcomes] == ["succeeded"] * len(outcomes), outcomes
    assert duplicate is None
    assert bus.source_descriptor().high_watermark == 1
    assert len(state["summary"].summary_outbox_ids) == 1
    assert bus.outbox_record(state["summary"].summary_outbox_ids[0]) is not None
    assert {adapter.stage_id: adapter.calls for adapter in adapters} == {
        stage_id: 2 if stage_id == failure_stage and failure_mode != "clean" else 1
        for stage_id in orchestrator.definition.stage_ids
    }
    with DuckDBStore(database, read_only=True) as store:
        assert len(store.query_screen_result(TRADE_DATE.isoformat(), "n-shape-pool1")) == 1
