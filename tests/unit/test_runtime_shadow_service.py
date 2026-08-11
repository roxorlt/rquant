from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import rquant.runtime_shadow_service as shadow_service
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_service import ShadowDailyServiceResult, run_shadow_daily_service
from rquant.runtime_shadow_sources import (
    LegacyMonitorEvent,
    LegacySurgeEvent,
    legacy_records_raw_input_id,
    runner_source_raw_input_id,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    CompletionAttestationSigner,
    CompletionAttestationVerifier,
    HmacCompletionAttestationAuthority,
    ShadowInputSnapshotIdentity,
    ShadowObservation,
    ShadowRetirementPolicy,
    ShadowSessionEvidence,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    attach_shadow_report_receipt,
    build_shadow_session_report,
    publish_shadow_session_report,
    shadow_completion_receipt_body_sha256,
    shadow_observation_set_id,
    shadow_session_boundaries,
    shadow_upstream_snapshot_id,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RunnerSignalBatch,
    SourceSnapshot,
)
from rquant.strategy_runner import RunnerSignalRecord
from tests.shadow_ed25519_support import (
    ShadowEd25519TestAuthority,
    create_shadow_ed25519_test_authority,
)

COMMIT = "a" * 40
OPEN_DATES = (
    date(2026, 8, 3),
    date(2026, 8, 4),
    date(2026, 8, 5),
    date(2026, 8, 6),
    date(2026, 8, 7),
    date(2026, 8, 10),
    date(2026, 8, 11),
    date(2026, 8, 12),
    date(2026, 8, 13),
    date(2026, 8, 14),
)
ATTESTATION_AUTHORITY = HmacCompletionAttestationAuthority(
    key_id="shadow-service-test",
    secret=b"shadow-service-test-attestation-key",
)


class _RunnerSource:
    def __init__(
        self,
        signals: tuple[SignalEnvelope, ...],
        source_id: str,
        *,
        attestation_signer: CompletionAttestationSigner = ATTESTATION_AUTHORITY,
    ) -> None:
        self.records = tuple(
            RunnerSignalRecord(sequence=index, signal=signal)
            for index, signal in enumerate(signals, start=1)
        )
        self.source_id = source_id
        self._attestation_signer = attestation_signer

    def _descriptor(self) -> RouteSourceDescriptor:
        return RouteSourceDescriptor(
            source_id=self.source_id,
            generation_id="e" * 64,
            strategy_spec_fingerprint="d" * 64,
            first_sequence=1,
            high_watermark=len(self.records),
        )

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch:
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(descriptor=self._descriptor()),
            after_sequence=after_sequence,
            limit=limit,
            records=tuple(item for item in self.records if item.sequence > after_sequence)[:limit],
        )

    def read_completion_receipt(self, *, trade_date: date) -> ShadowSourceCompletionReceipt:
        strategy_id = "growth_board_surge" if self.source_id == "growth-v1" else "n_shape"
        return _isolated_receipt(
            binding=_binding(strategy_id),
            source_id=self.source_id,
            trade_date=trade_date,
            input_identity=runner_source_raw_input_id(
                self._descriptor(),
                self.records,
                trade_date=trade_date,
            ),
            high_watermark=len(self.records),
            strategy_spec_fingerprint=self._descriptor().strategy_spec_fingerprint,
            attestation_signer=self._attestation_signer,
        )


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 8, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=OPEN_DATES,
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
    )


def _policy() -> ShadowRetirementPolicy:
    return ShadowRetirementPolicy(
        required_consecutive_sessions=10,
        strategy_bindings=(
            _binding("n_shape"),
            _binding("growth_board_surge"),
        ),
    )


def _binding(strategy_id: str) -> ShadowStrategyBinding:
    return ShadowStrategyBinding(
        strategy_id=strategy_id,
        strategy_version=1,
        definition_fingerprint="1" * 64,
        executable_fingerprint="2" * 64,
    )


def _isolated_receipt(
    *,
    binding: ShadowStrategyBinding,
    source_id: str,
    trade_date: date,
    input_identity: str,
    high_watermark: int,
    strategy_spec_fingerprint: str = "d" * 64,
    attestation_signer: CompletionAttestationSigner = ATTESTATION_AUTHORITY,
) -> ShadowSourceCompletionReceipt:
    _session_open, session_close = shadow_session_boundaries(trade_date)
    receipt = ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source="isolated",
        source_id=source_id,
        trade_date=trade_date,
        session_close_at=session_close,
        complete_through=session_close,
        input_identity=input_identity,
        produced_at=session_close + timedelta(minutes=10),
        producer_commit=COMMIT,
        producer_version="test-production-1",
        producer_service_id="strategy-live",
        producer_instance_id="test-primary",
        runner_generation_id="e" * 64,
        signal_authority_generation_id="a" * 64,
        calendar_generation_id=str(_calendar().content_sha256),
        last_sequence=0,
        high_watermark=high_watermark,
        route_receipts_id="c" * 64,
        feature_source_generation_id="3" * 64,
        feature_close_marker_id="4" * 64,
        feature_segment_chain_hash="5" * 64,
        segment_start_sequence=0,
        segment_record_count=high_watermark,
        segment_chain_hash="6" * 64,
    )
    claims = CompletionAttestationClaims(
        completion_receipt_body_sha256=shadow_completion_receipt_body_sha256(receipt),
        trade_date=trade_date,
        session_close_at=session_close,
        source_id=source_id,
        input_identity=input_identity,
        strategy_id=binding.strategy_id,
        strategy_version=binding.strategy_version,
        strategy_registration_fingerprint=binding.definition_fingerprint,
        strategy_spec_fingerprint=strategy_spec_fingerprint,
        executable_fingerprint=binding.executable_fingerprint,
        candidate_schema_fingerprint="7" * 64,
        feature_registration_fingerprint="8" * 64,
        feature_contract_fingerprint="9" * 64,
        routing_policy_fingerprint="b" * 64,
        producer_manifest_fingerprint="d" * 64,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        producer_service_id="strategy-live",
        producer_instance_id="test-primary",
        calendar_generation_id=str(_calendar().content_sha256),
        feature_source_generation_id="3" * 64,
        feature_close_marker_id="4" * 64,
        feature_segment_chain_hash="5" * 64,
        runner_generation_id="e" * 64,
        runner_segment_start_sequence=0,
        runner_segment_final_sequence=high_watermark,
        runner_segment_record_count=high_watermark,
        runner_segment_chain_hash="6" * 64,
        signal_authority_generation_id="a" * 64,
        route_receipts_id="c" * 64,
    )
    return ShadowSourceCompletionReceipt.model_validate(
        {
            **receipt.model_dump(mode="python", exclude={"receipt_id"}),
            "completion_attestation": attestation_signer.issue(claims),
        }
    )


@pytest.fixture
def ed25519_authority(tmp_path: Path) -> ShadowEd25519TestAuthority:
    return create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")


def _signal(
    *,
    trade_date: date,
    strategy_id: str,
    action: SignalAction,
    code: str,
    minute: int,
) -> SignalEnvelope:
    event_time = datetime.combine(
        trade_date,
        datetime(2026, 8, 14, 1, minute, tzinfo=UTC).timetz(),
    )
    return SignalEnvelope(
        schema_version=1,
        strategy_id=strategy_id,
        strategy_version="1",
        parameter_fingerprint="b" * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=3),
        candidate_id=code,
        action=action,
        reason_codes=("entry",),
        evidence={"visible_minute": f"09:{minute:02d}"},
        expires_at=event_time + timedelta(minutes=5),
        producer_commit=COMMIT,
    )


def _monitor_events(trade_date: date) -> tuple[LegacyMonitorEvent, ...]:
    return (
        LegacyMonitorEvent(
            trade_date=trade_date,
            ts_code="600001.SH",
            level="attack_strong_carry",
            trigger_time=datetime.combine(trade_date, datetime(2026, 8, 14, 9, 31).time()),
        ),
        LegacyMonitorEvent(
            trade_date=trade_date,
            ts_code="600001.SH",
            level="attack_break_high",
            trigger_time=datetime.combine(trade_date, datetime(2026, 8, 14, 9, 33).time()),
        ),
    )


def _surge_events() -> tuple[LegacySurgeEvent, ...]:
    return (
        LegacySurgeEvent(
            ts_code="300001.SZ",
            confirmed_at="09:35",
            status="confirmed",
        ),
    )


def _legacy_receipt(
    records: tuple[LegacyMonitorEvent | LegacySurgeEvent, ...],
    *,
    source_id: str,
    trade_date: date,
    producer_commit: str = COMMIT,
) -> ShadowSourceCompletionReceipt:
    _session_open, session_close = shadow_session_boundaries(trade_date)
    return ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source="legacy",
        source_id=source_id,
        trade_date=trade_date,
        session_close_at=session_close,
        complete_through=session_close,
        input_identity=legacy_records_raw_input_id(
            records,
            source_id=source_id,
            trade_date=trade_date,
        ),
        produced_at=session_close + timedelta(minutes=10),
        producer_commit=producer_commit,
        producer_version="test-production-1",
    )


def _observation(
    *,
    source: str,
    trade_date: date,
    binding: ShadowStrategyBinding,
    code: str,
    minute: int,
) -> ShadowObservation:
    event_time = datetime.combine(
        trade_date,
        datetime(2026, 8, 14, 1, minute, tzinfo=UTC).timetz(),
    )
    upstream = canonical_sha256(
        {"source": source, "strategy": binding.strategy_id, "date": trade_date, "code": code}
    )
    return ShadowObservation(
        source=source,
        strategy_id=binding.strategy_id,
        strategy_version=binding.strategy_version,
        definition_fingerprint=binding.definition_fingerprint,
        executable_fingerprint=binding.executable_fingerprint,
        trade_date=trade_date,
        ts_code=code,
        action="b_intent",
        event_time=event_time,
        available_at=event_time + timedelta(seconds=3),
        availability_basis="observed_completion",
        producer_commit=COMMIT,
        upstream_event_id=upstream,
        evidence_id=upstream,
    )


def _historical_report_path(
    root: Path,
    trade_date: date,
    *,
    authority: ShadowEd25519TestAuthority,
) -> Path:
    n_shape = _binding("n_shape")
    growth = _binding("growth_board_surge")
    legacy = (
        _observation(
            source="legacy",
            trade_date=trade_date,
            binding=n_shape,
            code="600001.SH",
            minute=33,
        ),
        _observation(
            source="legacy",
            trade_date=trade_date,
            binding=growth,
            code="300001.SZ",
            minute=35,
        ),
    )
    isolated = (
        _observation(
            source="isolated",
            trade_date=trade_date,
            binding=n_shape,
            code="600001.SH",
            minute=33,
        ),
        _observation(
            source="isolated",
            trade_date=trade_date,
            binding=growth,
            code="300001.SZ",
            minute=35,
        ),
    )
    session_open, session_close = shadow_session_boundaries(trade_date)
    captured_at = session_close + timedelta(minutes=10)
    snapshots = []
    for binding in (n_shape, growth):
        for source, observations in (("legacy", legacy), ("isolated", isolated)):
            scoped = tuple(item for item in observations if item.strategy_id == binding.strategy_id)
            source_id = f"{source}:{binding.strategy_id}"
            raw_input_id = canonical_sha256(
                {"source": source, "binding": binding, "trade_date": trade_date}
            )
            receipt = (
                _isolated_receipt(
                    binding=binding,
                    source_id=source_id,
                    trade_date=trade_date,
                    input_identity=raw_input_id,
                    high_watermark=0,
                    attestation_signer=authority.signer,
                )
                if source == "isolated"
                else ShadowSourceCompletionReceipt(
                    evidence_origin="production",
                    source=source,
                    source_id=source_id,
                    trade_date=trade_date,
                    session_close_at=session_close,
                    complete_through=session_close,
                    input_identity=raw_input_id,
                    produced_at=captured_at,
                    producer_commit=COMMIT,
                    producer_version="test-production-1",
                )
            )
            snapshots.append(
                ShadowInputSnapshotIdentity(
                    source=source,
                    source_id=source_id,
                    binding=binding,
                    raw_input_id=raw_input_id,
                    completion_receipt=receipt,
                    upstream_snapshot_id=shadow_upstream_snapshot_id(raw_input_id, receipt),
                    observation_set_id=shadow_observation_set_id(scoped),
                    captured_at=captured_at,
                    complete_through=session_close,
                    producer_commit=COMMIT,
                    producer_version="test-production-1",
                )
            )
    evidence = ShadowSessionEvidence(
        evidence_origin="production",
        calendar_authority_id=str(_calendar().content_sha256),
        evaluation_cutoff=captured_at,
        session_open_at=session_open,
        session_close_at=session_close,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        input_snapshots=tuple(snapshots),
    )
    report = build_shadow_session_report(
        trade_date=trade_date,
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=60_000_000,
        evidence=evidence,
        attestation_verifier=authority.keyring,
    )
    report = attach_shadow_report_receipt(
        report,
        signer=authority.signer,
        verifier=authority.keyring,
        producer_service_id="shadow-daily",
        producer_instance_id="service-test",
    )
    return publish_shadow_session_report(root, report)


def _run_latest_daily_service(
    tmp_path: Path,
    *,
    evaluated_at: datetime,
    authority: ShadowEd25519TestAuthority,
    attestation_verifier: CompletionAttestationVerifier | None = None,
    monitor_events: tuple[LegacyMonitorEvent, ...] | None = None,
) -> ShadowDailyServiceResult:
    trade_date = OPEN_DATES[-1]
    prepared_monitor_events = monitor_events or _monitor_events(trade_date)
    surge_events = _surge_events()
    return run_shadow_daily_service(
        calendar=_calendar(),
        evaluated_at=evaluated_at,
        policy=_policy(),
        report_root=tmp_path / "shadow-retry",
        legacy_exported_at=datetime(2026, 8, 14, 7, 10, tzinfo=UTC),
        legacy_monitor_commit=COMMIT,
        legacy_surge_commit=COMMIT,
        report_producer_commit=COMMIT,
        report_producer_version="test-production-1",
        legacy_monitor_events=prepared_monitor_events,
        legacy_monitor_completion_receipt=_legacy_receipt(
            prepared_monitor_events,
            source_id="legacy-monitor-events",
            trade_date=trade_date,
        ),
        legacy_surge_events=surge_events,
        legacy_surge_completion_receipt=_legacy_receipt(
            surge_events,
            source_id="legacy-surge-events",
            trade_date=trade_date,
        ),
        isolated_sources=(
            (
                _binding("n_shape"),
                _RunnerSource(
                    (
                        _signal(
                            trade_date=trade_date,
                            strategy_id="n_shape",
                            action=SignalAction.WATCH,
                            code="600001.SH",
                            minute=31,
                        ),
                        _signal(
                            trade_date=trade_date,
                            strategy_id="n_shape",
                            action=SignalAction.B_INTENT,
                            code="600001.SH",
                            minute=33,
                        ),
                    ),
                    "n-shape-v1",
                    attestation_signer=authority.signer,
                ),
            ),
            (
                _binding("growth_board_surge"),
                _RunnerSource(
                    (
                        _signal(
                            trade_date=trade_date,
                            strategy_id="growth_board_surge",
                            action=SignalAction.B_INTENT,
                            code="300001.SZ",
                            minute=35,
                        ),
                    ),
                    "growth-v1",
                    attestation_signer=authority.signer,
                ),
            ),
        ),
        attestation_verifier=attestation_verifier or authority.keyring,
        report_receipt_signer=authority.signer,
        report_receipt_verifier=authority.keyring,
        report_producer_service_id="shadow-daily",
        report_producer_instance_id="service-test",
    )


def test_daily_service_rejects_hmac_and_missing_report_signature_chain(
    tmp_path: Path,
    ed25519_authority: ShadowEd25519TestAuthority,
) -> None:
    with pytest.raises(ValueError, match="Ed25519"):
        _run_latest_daily_service(
            tmp_path,
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
            authority=ed25519_authority,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def test_daily_service_retry_ignores_wall_clock_and_rejects_content_conflict(
    tmp_path: Path,
    ed25519_authority: ShadowEd25519TestAuthority,
) -> None:
    first_evaluated_at = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    first = _run_latest_daily_service(
        tmp_path,
        evaluated_at=first_evaluated_at,
        authority=ed25519_authority,
    )
    retried = _run_latest_daily_service(
        tmp_path,
        evaluated_at=first_evaluated_at + timedelta(minutes=5),
        authority=ed25519_authority,
    )

    assert retried.report == first.report
    assert retried.report_path == first.report_path
    assert retried.report.evidence is not None
    assert retried.report.evidence.evaluation_cutoff == first_evaluated_at

    original = _monitor_events(OPEN_DATES[-1])
    changed = (original[0].model_copy(update={"trigger_price": 99.0}), *original[1:])
    with pytest.raises(ValueError, match="session.*conflict|conflict.*session"):
        _run_latest_daily_service(
            tmp_path,
            evaluated_at=first_evaluated_at + timedelta(minutes=10),
            authority=ed25519_authority,
            monitor_events=changed,
        )


def test_daily_service_retry_returns_durable_report_after_first_return_is_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ed25519_authority: ShadowEd25519TestAuthority,
) -> None:
    first_evaluated_at = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    real_publish = shadow_service.publish_shadow_report_retry
    lose_first_return = True

    def publish_then_lose_return(context: object, report: object) -> object:
        nonlocal lose_first_return
        result = real_publish(context, report)  # type: ignore[arg-type]
        if lose_first_return:
            lose_first_return = False
            raise RuntimeError("simulated lost return after durable publication")
        return result

    monkeypatch.setattr(
        shadow_service,
        "publish_shadow_report_retry",
        publish_then_lose_return,
    )

    with pytest.raises(RuntimeError, match="lost return"):
        _run_latest_daily_service(
            tmp_path,
            evaluated_at=first_evaluated_at,
            authority=ed25519_authority,
        )
    retried = _run_latest_daily_service(
        tmp_path,
        evaluated_at=first_evaluated_at + timedelta(minutes=5),
        authority=ed25519_authority,
    )

    assert retried.report.evidence is not None
    assert retried.report.evidence.evaluation_cutoff == first_evaluated_at
    assert tuple(
        item for item in retried.report_path.parent.glob("*.json") if not item.name.startswith(".")
    ) == (retried.report_path,)


def test_daily_service_uses_authority_adapters_and_accumulates_retirement_evidence(
    tmp_path: Path,
    ed25519_authority: ShadowEd25519TestAuthority,
) -> None:
    root = tmp_path / "shadow"
    historical = tuple(
        _historical_report_path(root, trade_date, authority=ed25519_authority)
        for trade_date in OPEN_DATES[:-1]
    )
    trade_date = OPEN_DATES[-1]
    n_shape_signals = (
        _signal(
            trade_date=trade_date,
            strategy_id="n_shape",
            action=SignalAction.WATCH,
            code="600001.SH",
            minute=31,
        ),
        _signal(
            trade_date=trade_date,
            strategy_id="n_shape",
            action=SignalAction.B_INTENT,
            code="600001.SH",
            minute=33,
        ),
    )
    growth_signals = (
        _signal(
            trade_date=trade_date,
            strategy_id="growth_board_surge",
            action=SignalAction.B_INTENT,
            code="300001.SZ",
            minute=35,
        ),
    )
    monitor_events = _monitor_events(trade_date)
    surge_events = _surge_events()

    result = run_shadow_daily_service(
        calendar=_calendar(),
        evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        policy=_policy(),
        report_root=root,
        legacy_exported_at=datetime(2026, 8, 14, 7, 10, tzinfo=UTC),
        legacy_monitor_commit=COMMIT,
        legacy_surge_commit=COMMIT,
        report_producer_commit=COMMIT,
        report_producer_version="test-production-1",
        legacy_monitor_events=monitor_events,
        legacy_monitor_completion_receipt=_legacy_receipt(
            monitor_events,
            source_id="legacy-monitor-events",
            trade_date=trade_date,
        ),
        legacy_surge_events=surge_events,
        legacy_surge_completion_receipt=_legacy_receipt(
            surge_events,
            source_id="legacy-surge-events",
            trade_date=trade_date,
        ),
        isolated_sources=(
            item
            for item in (
                (
                    _binding("n_shape"),
                    _RunnerSource(
                        n_shape_signals,
                        "n-shape-v1",
                        attestation_signer=ed25519_authority.signer,
                    ),
                ),
                (
                    _binding("growth_board_surge"),
                    _RunnerSource(
                        growth_signals,
                        "growth-v1",
                        attestation_signer=ed25519_authority.signer,
                    ),
                ),
            )
        ),
        historical_report_paths=historical,
        attestation_verifier=ed25519_authority.keyring,
        report_receipt_signer=ed25519_authority.signer,
        report_receipt_verifier=ed25519_authority.keyring,
        report_producer_service_id="shadow-daily",
        report_producer_instance_id="service-test",
    )

    assert result.report.trade_date == trade_date
    assert result.report_path.exists()
    assert result.report.legacy_count == 3
    assert result.report.isolated_count == 3
    assert result.report.evidence_origin == "production"
    assert result.report.evidence is not None
    assert result.report.evidence.calendar_authority_id == str(_calendar().content_sha256)
    assert len(result.report.evidence.input_snapshots) == 4
    assert result.evaluation.passed is True
    assert result.evaluation.accepted_session_count == 10


def test_daily_service_rejects_legacy_export_from_after_evaluation(
    tmp_path: Path,
    ed25519_authority: ShadowEd25519TestAuthority,
) -> None:
    evaluated_at = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="legacy export.*evaluation"):
        run_shadow_daily_service(
            calendar=_calendar(),
            evaluated_at=evaluated_at,
            policy=_policy(),
            report_root=tmp_path / "shadow",
            legacy_exported_at=evaluated_at + timedelta(microseconds=1),
            legacy_monitor_commit=COMMIT,
            legacy_surge_commit=COMMIT,
            report_producer_commit=COMMIT,
            report_producer_version="test-production-1",
            legacy_monitor_events=(),
            legacy_monitor_completion_receipt=_legacy_receipt(
                (),
                source_id="legacy-monitor-events",
                trade_date=OPEN_DATES[-1],
            ),
            legacy_surge_events=(),
            legacy_surge_completion_receipt=_legacy_receipt(
                (),
                source_id="legacy-surge-events",
                trade_date=OPEN_DATES[-1],
            ),
            isolated_sources=(
                (
                    _binding("n_shape"),
                    _RunnerSource(
                        (),
                        "n-shape-v1",
                        attestation_signer=ed25519_authority.signer,
                    ),
                ),
                (
                    _binding("growth_board_surge"),
                    _RunnerSource(
                        (),
                        "growth-v1",
                        attestation_signer=ed25519_authority.signer,
                    ),
                ),
            ),
            attestation_verifier=ed25519_authority.keyring,
            report_receipt_signer=ed25519_authority.signer,
            report_receipt_verifier=ed25519_authority.keyring,
            report_producer_service_id="shadow-daily",
            report_producer_instance_id="service-test",
        )
