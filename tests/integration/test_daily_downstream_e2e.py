from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.daily_canonical_publisher import DailyCanonicalPublisher, DailyCanonicalPublishReceipt
from rquant.daily_close_candidate import DailyCandidateHmacSigner, DailyCloseCandidateStore
from rquant.daily_close_gateway import DailyCloseGateway, DailyCloseGatewayConfig
from rquant.daily_close_validation import (
    DailyCloseValidationPolicy,
    DailyCloseValidator,
    VerifiedDailyCloseBatch,
)
from rquant.daily_ledger_fence import DailyLedgerFenceGuard
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunRecord,
    DailyRunSpec,
    DailyStageAttempt,
    DailyStageSpec,
    DailyWriterLease,
    StageResult,
)
from rquant.daily_pool_stage import DailyDownstreamArtifactStore, DailyPoolStage, DailyScreenStage
from rquant.daily_summary_stage import DailySummaryStage
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.presets import ScreenPreset
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.screen.rules import not_st
from rquant.signal_bus import SignalBusStore
from rquant.storage.duckdb import DuckDBStore

TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)
AVAILABLE_AT = OBSERVED_AT + timedelta(seconds=2)
COMMITTED_AT = datetime(2026, 7, 31, 9, 10, tzinfo=UTC)


def _storage_profile(root: Path) -> DailyPipelineStorageProfile:
    root.mkdir(parents=True, exist_ok=True)
    return DailyPipelineStorageProfile.create(
        root=root.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="b" * 64,
    )


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=TRADE_DATE - timedelta(days=7),
        coverage_end=TRADE_DATE + timedelta(days=7),
        open_dates=(TRADE_DATE,),
        generated_at=OBSERVED_AT - timedelta(seconds=1),
    )


def _published(tmp_path: Path) -> tuple[DailyCloseGateway, object]:
    snapshot = {
        "daily_bar": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.4,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 9.95,
                "change": 0.25,
                "pct_chg": 2.512562814070352,
                "vol": 1_000.0,
                "amount": 10_200.0,
            },
        ),
        "daily_basic": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "turnover_rate": 0.5,
                "volume_ratio": 1.2,
                "total_mv": 200_000.0,
                "circ_mv": 180_000.0,
            },
        ),
        "adj_factor": ({"ts_code": "600000.SH", "trade_date": TRADE_DATE, "adj_factor": 1.01},),
        "index_daily": (
            {
                "ts_code": "000001.SH",
                "trade_date": TRADE_DATE,
                "open": 3200.0,
                "high": 3230.0,
                "low": 3190.0,
                "close": 3220.0,
                "pre_close": 3198.0,
                "change": 22.0,
                "pct_chg": 0.688,
                "vol": 2_000.0,
                "amount": 30_000.0,
            },
        ),
        "security_status": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "name": "浦发银行",
                "is_st": False,
                "listing_status": "L",
            },
        ),
        "suspension_status": (),
        "partial_datasets": (),
    }
    gateway = DailyCloseGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=lambda _request: snapshot,
        config=DailyCloseGatewayConfig(
            producer_version="integration-e2e-v1",
            producer_commit="a" * 40,
        ),
        completion_clock=lambda: AVAILABLE_AT,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    record = gateway.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)[-1]
    return gateway, record


def _policy() -> DailyCloseValidationPolicy:
    return DailyCloseValidationPolicy(
        expected_schema_version=1,
        min_daily_rows=1,
        max_daily_rows=10,
        required_index_codes=("000001.SH",),
    )


def _signer() -> DailyCandidateHmacSigner:
    return DailyCandidateHmacSigner(
        key_id="daily-integration-e2e",
        secret=b"daily-integration-e2e-secret-32-b",
    )


def _publish_candidate(
    store: DailyCloseCandidateStore,
    verified: VerifiedDailyCloseBatch,
    *,
    spool: LiveBatchSpool,
    ledger: DailyPipelineLedger,
    lease: DailyWriterLease,
    attempt: DailyStageAttempt,
):
    return store.publish(
        verified,
        spool=spool,
        attempt=attempt,
        published_at=attempt.claimed_at,
        fence_guard=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
    )


def _seed_database(path: Path) -> None:
    with DuckDBStore(path) as store:
        store._conn.execute(
            """
            INSERT INTO stock_basic
            (ts_code, symbol, name, area, industry, list_date, market)
            VALUES ('600000.SH', '600000', '浦发银行', '上海', '银行',
                    DATE '1999-11-10', '主板')
            """
        )
        store._conn.execute(
            """
            INSERT INTO trade_calendar
            (exchange, cal_date, is_open, pretrade_date, source, updated_at)
            VALUES ('SSE', ?, TRUE, NULL, 'test', ?)
            """,
            [TRADE_DATE, COMMITTED_AT],
        )


_NOTIFICATION_TARGETS = (
    DeliveryTarget(recipient_id="daily-close-test", channel=DeliveryChannel.PUSHDEER),
)


def _bootstrap_downstream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    DailyCanonicalPublishReceipt,
    DailyPipelineLedger,
    DailyWriterLease,
    DailyRunRecord,
    Path,
    DailyDownstreamArtifactStore,
]:
    gateway, record = _published(tmp_path / "raw")
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    candidates = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    ledger = DailyPipelineLedger(
        storage_profile=_storage_profile(tmp_path / "daily-profile"),
        service_owner="daily-close",
    )
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(minutes=15),
    )
    run = ledger.create_run(
        lease,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="a" * 40,
            profile_hash="b" * 64,
            stages=(
                DailyStageSpec(stage_id="validate"),
                DailyStageSpec(stage_id="canonical_publish", depends_on=("validate",)),
                DailyStageSpec(stage_id="screen", depends_on=("canonical_publish",)),
                DailyStageSpec(stage_id="pool", depends_on=("screen",)),
                DailyStageSpec(stage_id="summary", depends_on=("pool",)),
            ),
        ),
        now=COMMITTED_AT,
    )
    validate_attempt = ledger.claim_next(lease, now=COMMITTED_AT)
    assert validate_attempt is not None
    candidate = _publish_candidate(
        candidates,
        verified,
        spool=gateway.spool,
        ledger=ledger,
        lease=lease,
        attempt=validate_attempt,
    )
    ledger.succeed(
        lease,
        validate_attempt,
        StageResult(
            content_hash=verified.validation_sha256,
            evidence_hash=verified.raw_content_sha256,
        ),
        now=COMMITTED_AT + timedelta(seconds=1),
    )
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = DailyCanonicalPublisher(
        candidate_store=candidates,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: DuckDBStore(db_path),
        ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
        clock=lambda: COMMITTED_AT + timedelta(seconds=2),
    )
    canonical_attempt = ledger.claim_next(lease, now=COMMITTED_AT + timedelta(seconds=2))
    assert canonical_attempt is not None
    canonical = publisher.publish(
        candidate.generation_id,
        attempt=canonical_attempt,
        ledger_input_identity=run.input_identity,
        committed_at=COMMITTED_AT + timedelta(seconds=2),
    )
    ledger.succeed(
        lease,
        canonical_attempt,
        canonical.stage_result,
        now=COMMITTED_AT + timedelta(seconds=3),
    )
    presets = {
        "n-shape-pool1": ScreenPreset(
            name="n-shape-pool1",
            description="test",
            rules=[not_st()],
        ),
        "n-shape-pool2": ScreenPreset(
            name="n-shape-pool2",
            description="test",
            rules=[not_st()],
            depends_on="n-shape-pool1",
            offset_days=1,
        ),
    }
    frame = pd.DataFrame(
        {
            "ts_code": ["600000.SH"],
            "name": ["test"],
            "CLOSE[0]": [10.0],
            "PCT_CHG[0]": [1.0],
        }
    )
    monkeypatch.setattr("rquant.pipeline.PRESET_SCREENS", presets)
    monkeypatch.setattr("rquant.pipeline.screen", lambda **_kwargs: frame)
    return (
        canonical,
        ledger,
        lease,
        run,
        db_path,
        DailyDownstreamArtifactStore(tmp_path / "downstream"),
    )


def _screen_stage(
    *,
    db_path: Path,
    artifacts: DailyDownstreamArtifactStore,
    ledger: DailyPipelineLedger,
    lease: DailyWriterLease,
    now: datetime,
) -> DailyScreenStage:
    return DailyScreenStage.from_ledger(
        writer_factory=lambda: DuckDBStore(db_path),
        artifact_store=artifacts,
        ledger=ledger,
        lease=lease,
        clock=lambda: now,
    )


def _pool_stage(
    *,
    db_path: Path,
    artifacts: DailyDownstreamArtifactStore,
    ledger: DailyPipelineLedger,
    lease: DailyWriterLease,
    now: datetime,
) -> DailyPoolStage:
    return DailyPoolStage.from_ledger(
        writer_factory=lambda: DuckDBStore(db_path),
        artifact_store=artifacts,
        ledger=ledger,
        lease=lease,
        clock=lambda: now,
    )


def test_canonical_receipt_flows_to_screen_pool_summary_and_deduped_outbox(
    tmp_path: Path,
    monkeypatch,
) -> None:
    gateway, record = _published(tmp_path / "raw")
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    candidates = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    ledger = DailyPipelineLedger(
        storage_profile=_storage_profile(tmp_path / "daily-profile"),
        service_owner="daily-close",
    )
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(minutes=15),
    )
    run = ledger.create_run(
        lease,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="a" * 40,
            profile_hash="b" * 64,
            stages=(
                DailyStageSpec(stage_id="validate"),
                DailyStageSpec(stage_id="canonical_publish", depends_on=("validate",)),
                DailyStageSpec(stage_id="screen", depends_on=("canonical_publish",)),
                DailyStageSpec(stage_id="pool", depends_on=("screen",)),
                DailyStageSpec(stage_id="summary", depends_on=("pool",)),
            ),
        ),
        now=COMMITTED_AT,
    )
    validate_attempt = ledger.claim_next(lease, now=COMMITTED_AT)
    assert validate_attempt is not None
    candidate = _publish_candidate(
        candidates,
        verified,
        spool=gateway.spool,
        ledger=ledger,
        lease=lease,
        attempt=validate_attempt,
    )
    ledger.succeed(
        lease,
        validate_attempt,
        StageResult(
            content_hash=verified.validation_sha256, evidence_hash=verified.raw_content_sha256
        ),
        now=COMMITTED_AT + timedelta(seconds=1),
    )

    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = DailyCanonicalPublisher(
        candidate_store=candidates,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: DuckDBStore(db_path),
        ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
        clock=lambda: COMMITTED_AT + timedelta(seconds=2),
    )
    canonical_attempt = ledger.claim_next(lease, now=COMMITTED_AT + timedelta(seconds=2))
    assert canonical_attempt is not None
    canonical = publisher.publish(
        candidate.generation_id,
        attempt=canonical_attempt,
        ledger_input_identity=run.input_identity,
        committed_at=COMMITTED_AT + timedelta(seconds=2),
    )
    ledger.succeed(
        lease, canonical_attempt, canonical.stage_result, now=COMMITTED_AT + timedelta(seconds=3)
    )

    presets = {
        "n-shape-pool1": ScreenPreset(name="n-shape-pool1", description="test", rules=[not_st()]),
        "n-shape-pool2": ScreenPreset(
            name="n-shape-pool2",
            description="test",
            rules=[not_st()],
            depends_on="n-shape-pool1",
            offset_days=1,
        ),
        "broken": ScreenPreset(name="broken", description="test", rules=[not_st()]),
    }
    frame = pd.DataFrame(
        {
            "ts_code": ["600000.SH"],
            "name": ["test"],
            "CLOSE[0]": [10.0],
            "PCT_CHG[0]": [1.0],
        }
    )
    calls = 0

    def screen_or_fail(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated preset failure")
        return frame

    monkeypatch.setattr("rquant.pipeline.PRESET_SCREENS", presets)
    monkeypatch.setattr("rquant.pipeline.screen", screen_or_fail)

    artifacts = DailyDownstreamArtifactStore(tmp_path / "downstream")
    screen_attempt = ledger.claim_next(lease, now=COMMITTED_AT + timedelta(seconds=4))
    assert screen_attempt is not None
    screen_stage = DailyScreenStage.from_ledger(
        writer_factory=lambda: DuckDBStore(db_path),
        artifact_store=artifacts,
        ledger=ledger,
        lease=lease,
        clock=lambda: COMMITTED_AT + timedelta(seconds=4),
    )
    screen_result = screen_stage.run(
        canonical, attempt=screen_attempt, ledger_input_identity=run.input_identity
    )
    ledger.succeed(
        lease, screen_attempt, screen_result.stage_result, now=COMMITTED_AT + timedelta(seconds=5)
    )

    pool_attempt = ledger.claim_next(lease, now=COMMITTED_AT + timedelta(seconds=6))
    assert pool_attempt is not None
    pool_stage = DailyPoolStage.from_ledger(
        writer_factory=lambda: DuckDBStore(db_path),
        artifact_store=artifacts,
        ledger=ledger,
        lease=lease,
        clock=lambda: COMMITTED_AT + timedelta(seconds=6),
    )
    pool_result = pool_stage.run(
        canonical,
        screen_result=screen_result,
        attempt=pool_attempt,
        ledger_input_identity=run.input_identity,
    )
    ledger.succeed(
        lease, pool_attempt, pool_result.stage_result, now=COMMITTED_AT + timedelta(seconds=7)
    )

    summary_attempt = ledger.claim_next(lease, now=COMMITTED_AT + timedelta(seconds=8))
    assert summary_attempt is not None
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    summary_stage = DailySummaryStage.from_ledger(
        signal_bus=bus,
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: COMMITTED_AT + timedelta(seconds=8),
        artifact_store=artifacts,
        canonical_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        ledger=ledger,
        lease=lease,
        notification_targets=_NOTIFICATION_TARGETS,
    )
    summary = summary_stage.run(
        canonical,
        screen_result=screen_result,
        pool_result=pool_result,
        attempt=summary_attempt,
        ledger_input_identity=run.input_identity,
    )
    replay = summary_stage.run(
        canonical,
        screen_result=screen_result,
        pool_result=pool_result,
        attempt=summary_attempt,
        ledger_input_identity=run.input_identity,
    )
    ledger.succeed(
        lease, summary_attempt, summary.stage_result, now=COMMITTED_AT + timedelta(seconds=9)
    )
    assert replay.stage_result == summary.stage_result
    assert bus.source_descriptor().high_watermark == 2
    assert len(summary.summary_outbox_ids) == 1
    assert bus.outbox_record(summary.summary_outbox_ids[0]) is not None
    assert len(summary.error_signal_ids) == len(summary.error_outbox_ids) == 1
    assert bus.outbox_record(summary.error_outbox_ids[0]) is not None
    with DuckDBStore(db_path, read_only=True) as store:
        assert len(store.query_screen_result(TRADE_DATE.isoformat(), "n-shape-pool1")) == 1


def test_screen_crash_after_business_commit_restarts_without_duplicate_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical, ledger, lease, run, db_path, artifacts = _bootstrap_downstream(tmp_path, monkeypatch)
    screen_time = COMMITTED_AT + timedelta(seconds=4)
    attempt = ledger.claim_next(lease, now=screen_time)
    assert attempt is not None
    stage = _screen_stage(
        db_path=db_path,
        artifacts=artifacts,
        ledger=ledger,
        lease=lease,
        now=screen_time,
    )
    original_persist = artifacts.persist_screen

    class SimulatedCrash(BaseException):
        pass

    def crash_after_commit(*_args: object, **_kwargs: object) -> object:
        raise SimulatedCrash()

    monkeypatch.setattr(artifacts, "persist_screen", crash_after_commit)
    with pytest.raises(SimulatedCrash):
        stage.run(canonical, attempt=attempt, ledger_input_identity=run.input_identity)
    with DuckDBStore(db_path, read_only=True) as store:
        assert len(store.query_screen_result(TRADE_DATE.isoformat(), "n-shape-pool1")) == 1

    recovered_at = COMMITTED_AT + timedelta(minutes=16)
    recovered_lease = ledger.acquire_writer(
        owner="daily-close",
        now=recovered_at,
        lease_for=timedelta(minutes=15),
    )
    assert ledger.recover(recovered_lease, now=recovered_at).retried_stage_ids == ("screen",)
    retry = ledger.claim_next(recovered_lease, now=recovered_at)
    assert retry is not None and retry.stage_id == "screen" and retry.attempt_number == 2
    monkeypatch.setattr(artifacts, "persist_screen", original_persist)
    result = _screen_stage(
        db_path=db_path,
        artifacts=artifacts,
        ledger=ledger,
        lease=recovered_lease,
        now=recovered_at,
    ).run(canonical, attempt=retry, ledger_input_identity=run.input_identity)
    ledger.succeed(recovered_lease, retry, result.stage_result, now=recovered_at)
    with DuckDBStore(db_path, read_only=True) as store:
        assert len(store.query_screen_result(TRADE_DATE.isoformat(), "n-shape-pool1")) == 1


def test_prepared_screen_receipt_recovers_without_replaying_business_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical, ledger, lease, run, db_path, artifacts = _bootstrap_downstream(tmp_path, monkeypatch)
    screen_time = COMMITTED_AT + timedelta(seconds=4)
    attempt = ledger.claim_next(lease, now=screen_time)
    assert attempt is not None
    result = _screen_stage(
        db_path=db_path,
        artifacts=artifacts,
        ledger=ledger,
        lease=lease,
        now=screen_time,
    ).run(canonical, attempt=attempt, ledger_input_identity=run.input_identity)
    monkeypatch.setattr(
        "rquant.daily_pool_stage.run_daily_screen_stage",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("business stage replayed")),
    )
    assert (
        _screen_stage(
            db_path=db_path,
            artifacts=artifacts,
            ledger=ledger,
            lease=lease,
            now=screen_time,
        ).run(canonical, attempt=attempt, ledger_input_identity=run.input_identity)
        == result
    )
    prepared = ledger.prepare_success(lease, attempt, result.stage_result, now=screen_time)
    recovered_at = COMMITTED_AT + timedelta(minutes=16)
    recovered_lease = ledger.acquire_writer(
        owner="daily-close",
        now=recovered_at,
        lease_for=timedelta(minutes=15),
    )
    recovery = ledger.recover(recovered_lease, now=recovered_at)

    assert recovery.finalized_receipt_ids == (prepared.receipt_id,)
    assert ledger.stage(run.run_id, "screen").attempts == 1
    with DuckDBStore(db_path, read_only=True) as store:
        assert len(store.query_screen_result(TRADE_DATE.isoformat(), "n-shape-pool1")) == 1


def test_summary_outbox_crash_replays_to_a_single_signal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical, ledger, lease, run, db_path, artifacts = _bootstrap_downstream(tmp_path, monkeypatch)
    screen_time = COMMITTED_AT + timedelta(seconds=4)
    screen_attempt = ledger.claim_next(lease, now=screen_time)
    assert screen_attempt is not None
    screen_result = _screen_stage(
        db_path=db_path,
        artifacts=artifacts,
        ledger=ledger,
        lease=lease,
        now=screen_time,
    ).run(canonical, attempt=screen_attempt, ledger_input_identity=run.input_identity)
    ledger.succeed(lease, screen_attempt, screen_result.stage_result, now=screen_time)
    pool_time = COMMITTED_AT + timedelta(seconds=6)
    pool_attempt = ledger.claim_next(lease, now=pool_time)
    assert pool_attempt is not None
    pool_result = _pool_stage(
        db_path=db_path,
        artifacts=artifacts,
        ledger=ledger,
        lease=lease,
        now=pool_time,
    ).run(
        canonical,
        screen_result=screen_result,
        attempt=pool_attempt,
        ledger_input_identity=run.input_identity,
    )
    ledger.succeed(lease, pool_attempt, pool_result.stage_result, now=pool_time)
    summary_time = COMMITTED_AT + timedelta(seconds=8)
    summary_attempt = ledger.claim_next(lease, now=summary_time)
    assert summary_attempt is not None
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    stage = DailySummaryStage.from_ledger(
        signal_bus=bus,
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: summary_time,
        artifact_store=artifacts,
        canonical_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        ledger=ledger,
        lease=lease,
        notification_targets=_NOTIFICATION_TARGETS,
    )
    # A process can die after the business side effect commits but before the
    # orchestrator records the terminal stage receipt.  Leave that receipt
    # intentionally uncommitted, then exercise normal ledger recovery.
    first_result = stage.run(
        canonical,
        screen_result=screen_result,
        pool_result=pool_result,
        attempt=summary_attempt,
        ledger_input_identity=run.input_identity,
    )
    assert bus.source_descriptor().high_watermark == 1

    recovered_at = COMMITTED_AT + timedelta(minutes=16)
    recovered_lease = ledger.acquire_writer(
        owner="daily-close",
        now=recovered_at,
        lease_for=timedelta(minutes=15),
    )
    assert ledger.recover(recovered_lease, now=recovered_at).retried_stage_ids == ("summary",)
    retry = ledger.claim_next(recovered_lease, now=recovered_at)
    assert retry is not None and retry.stage_id == "summary" and retry.attempt_number == 2
    replay = DailySummaryStage.from_ledger(
        signal_bus=bus,
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: recovered_at,
        artifact_store=artifacts,
        canonical_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        ledger=ledger,
        lease=recovered_lease,
        notification_targets=_NOTIFICATION_TARGETS,
    ).run(
        canonical,
        screen_result=screen_result,
        pool_result=pool_result,
        attempt=retry,
        ledger_input_identity=run.input_identity,
    )
    ledger.succeed(recovered_lease, retry, replay.stage_result, now=recovered_at)
    assert bus.source_descriptor().high_watermark == 1
    assert replay.stage_result == first_result.stage_result
    assert len(replay.summary_outbox_ids) == 1
    assert bus.outbox_record(replay.summary_outbox_ids[0]) is not None
