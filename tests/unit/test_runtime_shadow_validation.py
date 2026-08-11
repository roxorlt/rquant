from __future__ import annotations

import multiprocessing
import os
import tracemalloc
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.runtime_shadow_validation as shadow_validation
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    CompletionAttestationSigner,
    HmacCompletionAttestationAuthority,
    ShadowCalendarSelection,
    ShadowInputSnapshotIdentity,
    ShadowObservation,
    ShadowRetirementPolicy,
    ShadowSessionEvidence,
    ShadowSessionReport,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    attach_shadow_report_receipt,
    build_shadow_session_report,
    evaluate_shadow_retirement_gate,
    load_shadow_session_report,
    publish_shadow_session_report,
    shadow_completion_receipt_body_sha256,
    shadow_upstream_snapshot_id,
)
from tests.shadow_ed25519_support import (
    ShadowEd25519TestAuthority,
    create_shadow_ed25519_test_authority,
)

COMMIT = "a" * 40
MATCH_TOLERANCE_MICROSECONDS = 60_000_000
REVIEWER_CALENDAR_AUTHORITY_ID = "5590" * 16
REVIEWER_WRONG_RECEIPT_CALENDAR_ID = "b" * 64
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
    key_id="shadow-test-key-v1",
    secret=b"shadow-test-completion-attestation-key-v1",
)
_ED25519_AUTHORITY: ShadowEd25519TestAuthority | None = None


@pytest.fixture(scope="module", autouse=True)
def _configure_ed25519_authority(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    global _ED25519_AUTHORITY
    _ED25519_AUTHORITY = create_shadow_ed25519_test_authority(
        tmp_path_factory.mktemp("shadow-validation-ed25519")
    )


def _ed25519_authority() -> ShadowEd25519TestAuthority:
    if _ED25519_AUTHORITY is None:
        raise AssertionError("Ed25519 test authority is not configured")
    return _ED25519_AUTHORITY


def _calendar(
    *,
    open_dates: tuple[date, ...] = OPEN_DATES,
    producer_commit: str = COMMIT,
) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=producer_commit,
        coverage_start=date(2026, 8, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=open_dates,
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
    )


def _selection(
    *,
    open_dates: tuple[date, ...] = OPEN_DATES,
    evaluated_at: datetime = datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    authority: MarketCalendarAuthority | None = None,
) -> ShadowCalendarSelection:
    return ShadowCalendarSelection.create(
        authority=authority or _calendar(open_dates=open_dates),
        evaluated_at=evaluated_at,
        maximum_sessions=20,
    )


def _binding(
    strategy_id: str = "growth_board_surge",
    strategy_version: int = 1,
    definition_fingerprint: str = "1" * 64,
    executable_fingerprint: str = "2" * 64,
) -> ShadowStrategyBinding:
    return ShadowStrategyBinding(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
    )


def _policy(
    *,
    bindings: tuple[ShadowStrategyBinding, ...] | None = None,
    minimum_legacy_recall_bps: int = 9500,
    minimum_isolated_precision_bps: int = 9500,
    maximum_isolated_p95_latency_microseconds: int = 10_000_000,
) -> ShadowRetirementPolicy:
    return ShadowRetirementPolicy(
        required_consecutive_sessions=10,
        strategy_bindings=bindings or (_binding(),),
        minimum_legacy_recall_bps=minimum_legacy_recall_bps,
        minimum_isolated_precision_bps=minimum_isolated_precision_bps,
        maximum_isolated_p95_latency_microseconds=(maximum_isolated_p95_latency_microseconds),
    )


def _observation(
    *,
    source: str,
    ts_code: str,
    minute: int = 31,
    action: str = "b_intent",
    trade_date: date = date(2026, 8, 3),
    strategy_id: str = "growth_board_surge",
    strategy_version: int = 1,
    definition_fingerprint: str = "1" * 64,
    executable_fingerprint: str = "2" * 64,
    availability_basis: str = "observed_completion",
    availability_delay_microseconds: int = 3_000_000,
    event_offset_microseconds: int = 0,
    evidence_salt: str = "default",
    upstream_salt: str | None = None,
) -> ShadowObservation:
    event_time = datetime.combine(trade_date, time(1, minute), tzinfo=UTC) + timedelta(
        microseconds=event_offset_microseconds
    )
    business_identity = {
        "source": source,
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "ts_code": ts_code,
        "action": action,
        "event_time": event_time,
        "salt": upstream_salt or evidence_salt,
    }
    return ShadowObservation(
        source=source,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        trade_date=event_time.date(),
        ts_code=ts_code,
        action=action,
        event_time=event_time,
        available_at=event_time + timedelta(microseconds=availability_delay_microseconds),
        availability_basis=availability_basis,
        producer_commit=COMMIT,
        upstream_event_id=canonical_sha256(business_identity),
        evidence_id=canonical_sha256({**business_identity, "evidence": evidence_salt}),
    )


def _report_for_date(
    trade_date: date,
    *,
    bindings: tuple[ShadowStrategyBinding, ...] = (_binding(),),
    isolated_delay_microseconds: int = 3_000_000,
) -> ShadowSessionReport:
    legacy: list[ShadowObservation] = []
    isolated: list[ShadowObservation] = []
    for index, binding in enumerate(bindings, start=1):
        code = f"{300000 + index:06d}.SZ"
        legacy.append(
            _observation(
                source="legacy",
                ts_code=code,
                trade_date=trade_date,
                strategy_id=binding.strategy_id,
                strategy_version=binding.strategy_version,
                availability_basis="export_observed_proxy",
                availability_delay_microseconds=60_000_000,
                upstream_salt=f"legacy-{index}",
            )
        )
        isolated.append(
            _observation(
                source="isolated",
                ts_code=code,
                trade_date=trade_date,
                strategy_id=binding.strategy_id,
                strategy_version=binding.strategy_version,
                availability_delay_microseconds=isolated_delay_microseconds,
                upstream_salt=f"isolated-{index}",
            )
        )
    return build_shadow_session_report(
        trade_date=trade_date,
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
    )


def _observation_set_id(observations: tuple[ShadowObservation, ...]) -> str:
    return canonical_sha256(
        {
            "contract": "runtime-shadow-observation-set/v2",
            "observation_ids": tuple(sorted(str(item.observation_id) for item in observations)),
        }
    )


def _production_evidence(
    trade_date: date,
    *,
    bindings: tuple[ShadowStrategyBinding, ...],
    legacy: tuple[ShadowObservation, ...],
    isolated: tuple[ShadowObservation, ...],
    calendar_authority_id: str | None = None,
    receipt_calendar_generation_id: str | None = None,
    signed: bool = True,
    attestation_signer: CompletionAttestationSigner | None = None,
) -> ShadowSessionEvidence:
    session_close = datetime.combine(trade_date, time(7, 0), tzinfo=UTC)
    captured_at = session_close + timedelta(minutes=10)
    selected_calendar_authority_id = calendar_authority_id or str(_calendar().content_sha256)
    selected_receipt_calendar_generation_id = (
        receipt_calendar_generation_id or selected_calendar_authority_id
    )
    snapshots = []
    for binding in bindings:
        for source, observations in (("legacy", legacy), ("isolated", isolated)):
            source_id = f"{source}:{binding.strategy_id}:v{binding.strategy_version}"
            raw_input_id = canonical_sha256(
                {"source": source, "trade_date": trade_date, "binding": binding}
            )
            authority = (
                {
                    "producer_service_id": "strategy-live",
                    "producer_instance_id": "test-primary",
                    "runner_generation_id": "e" * 64,
                    "signal_authority_generation_id": "a" * 64,
                    "calendar_generation_id": selected_receipt_calendar_generation_id,
                    "last_sequence": 0,
                    "high_watermark": 0,
                    "route_receipts_id": "c" * 64,
                    "feature_source_generation_id": "f" * 64,
                    "feature_close_marker_id": "1" * 64,
                    "feature_segment_chain_hash": "2" * 64,
                    "segment_start_sequence": 0,
                    "segment_record_count": 0,
                    "segment_chain_hash": "3" * 64,
                }
                if source == "isolated"
                else {}
            )
            receipt = ShadowSourceCompletionReceipt(
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
                **authority,
            )
            if source == "isolated" and signed:
                claims = CompletionAttestationClaims(
                    completion_receipt_body_sha256=shadow_completion_receipt_body_sha256(receipt),
                    trade_date=trade_date,
                    session_close_at=session_close,
                    source_id=source_id,
                    input_identity=raw_input_id,
                    strategy_id=binding.strategy_id,
                    strategy_version=binding.strategy_version,
                    strategy_registration_fingerprint=binding.definition_fingerprint,
                    strategy_spec_fingerprint="4" * 64,
                    executable_fingerprint=binding.executable_fingerprint,
                    candidate_schema_fingerprint="5" * 64,
                    feature_registration_fingerprint="6" * 64,
                    feature_contract_fingerprint="7" * 64,
                    routing_policy_fingerprint="8" * 64,
                    producer_manifest_fingerprint="9" * 64,
                    producer_commit=COMMIT,
                    producer_version="test-production-1",
                    producer_service_id="strategy-live",
                    producer_instance_id="test-primary",
                    calendar_generation_id=selected_receipt_calendar_generation_id,
                    feature_source_generation_id="f" * 64,
                    feature_close_marker_id="1" * 64,
                    feature_segment_chain_hash="2" * 64,
                    runner_generation_id="e" * 64,
                    runner_segment_start_sequence=0,
                    runner_segment_final_sequence=0,
                    runner_segment_record_count=0,
                    runner_segment_chain_hash="3" * 64,
                    signal_authority_generation_id="a" * 64,
                    route_receipts_id="c" * 64,
                )
                payload = receipt.model_dump(mode="python", exclude={"receipt_id"})
                signer = attestation_signer or _ed25519_authority().signer
                payload["completion_attestation"] = signer.issue(claims)
                receipt = ShadowSourceCompletionReceipt.model_validate(payload)
            snapshots.append(
                ShadowInputSnapshotIdentity(
                    source=source,
                    source_id=source_id,
                    binding=binding,
                    raw_input_id=raw_input_id,
                    completion_receipt=receipt,
                    upstream_snapshot_id=shadow_upstream_snapshot_id(raw_input_id, receipt),
                    observation_set_id=_observation_set_id(
                        tuple(
                            item
                            for item in observations
                            if (
                                item.strategy_id,
                                item.strategy_version,
                                item.definition_fingerprint,
                                item.executable_fingerprint,
                            )
                            == (
                                binding.strategy_id,
                                binding.strategy_version,
                                binding.definition_fingerprint,
                                binding.executable_fingerprint,
                            )
                        )
                    ),
                    captured_at=captured_at,
                    complete_through=session_close,
                    producer_commit=COMMIT,
                    producer_version="test-production-1",
                )
            )
    return ShadowSessionEvidence(
        evidence_origin="production",
        calendar_authority_id=selected_calendar_authority_id,
        evaluation_cutoff=captured_at,
        session_open_at=datetime.combine(trade_date, time(1, 25), tzinfo=UTC),
        session_close_at=session_close,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        input_snapshots=tuple(snapshots),
    )


def _production_report_for_date(
    trade_date: date,
    *,
    bindings: tuple[ShadowStrategyBinding, ...] | None = None,
    calendar_authority_id: str | None = None,
    signed: bool = True,
    legacy_hmac: bool = False,
) -> ShadowSessionReport:
    selected = bindings or (_binding(),)
    legacy = tuple(
        _observation(
            source="legacy",
            ts_code=f"{300001 + index:06d}.SZ",
            trade_date=trade_date,
            strategy_id=binding.strategy_id,
            strategy_version=binding.strategy_version,
            definition_fingerprint=binding.definition_fingerprint,
            executable_fingerprint=binding.executable_fingerprint,
            availability_basis="export_observed_proxy",
            upstream_salt=f"legacy-{trade_date}-{index}",
        )
        for index, binding in enumerate(selected)
    )
    isolated = tuple(
        _observation(
            source="isolated",
            ts_code=f"{300001 + index:06d}.SZ",
            trade_date=trade_date,
            strategy_id=binding.strategy_id,
            strategy_version=binding.strategy_version,
            definition_fingerprint=binding.definition_fingerprint,
            executable_fingerprint=binding.executable_fingerprint,
            upstream_salt=f"isolated-{trade_date}-{index}",
        )
        for index, binding in enumerate(selected)
    )
    evidence = _production_evidence(
        trade_date,
        bindings=selected,
        legacy=legacy,
        isolated=isolated,
        calendar_authority_id=calendar_authority_id,
        signed=signed,
        attestation_signer=(ATTESTATION_AUTHORITY if legacy_hmac else None),
    )
    if not signed or legacy_hmac:
        fixture = build_shadow_session_report(
            trade_date=trade_date,
            legacy=legacy,
            isolated=isolated,
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        )
        payload = fixture.model_dump(
            mode="python",
            exclude={"report_id", "evidence_origin", "evidence"},
        )
        return ShadowSessionReport(
            evidence_origin="production",
            evidence=evidence,
            **payload,
        )
    report = build_shadow_session_report(
        trade_date=trade_date,
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        evidence=evidence,
        attestation_verifier=_ed25519_authority().keyring,
    )
    return attach_shadow_report_receipt(
        report,
        signer=_ed25519_authority().signer,
        verifier=_ed25519_authority().keyring,
        producer_service_id="shadow-daily",
        producer_instance_id="validation-test",
    )


def _replace_isolated_receipt_calendar_without_revalidating_report(
    report: ShadowSessionReport,
    *,
    calendar_generation_id: str,
) -> ShadowSessionReport:
    assert report.evidence is not None
    snapshots = list(report.evidence.input_snapshots)
    isolated_index = next(
        index for index, snapshot in enumerate(snapshots) if snapshot.source == "isolated"
    )
    snapshot = snapshots[isolated_index]
    receipt = snapshot.completion_receipt.model_copy(
        update={"calendar_generation_id": calendar_generation_id}
    )
    snapshots[isolated_index] = snapshot.model_copy(update={"completion_receipt": receipt})
    historical_evidence = report.evidence.model_copy(update={"input_snapshots": tuple(snapshots)})
    return report.model_copy(update={"evidence": historical_evidence})


def _strip_attestation_without_revalidating_report(
    report: ShadowSessionReport,
) -> ShadowSessionReport:
    assert report.evidence is not None
    snapshots = tuple(
        snapshot.model_copy(
            update={
                "completion_receipt": snapshot.completion_receipt.model_copy(
                    update={"completion_attestation": None}
                )
            }
        )
        if snapshot.source == "isolated"
        else snapshot
        for snapshot in report.evidence.input_snapshots
    )
    return report.model_copy(
        update={"evidence": report.evidence.model_copy(update={"input_snapshots": snapshots})}
    )


def _crash_after_link(root: str, report_payload: dict[str, object]) -> None:
    import rquant.runtime_shadow_validation as shadow

    report = shadow.ShadowSessionReport.model_validate(report_payload)
    original_link = shadow.os.link

    def crashing_link(*args: object, **kwargs: object) -> None:
        original_link(*args, **kwargs)
        os._exit(91)

    shadow.os.link = crashing_link
    shadow.publish_shadow_session_report(Path(root), report)


def test_shadow_report_matches_same_semantics_and_uses_integer_microseconds() -> None:
    legacy = (
        _observation(source="legacy", ts_code="300001.SZ"),
        _observation(source="legacy", ts_code="300002.SZ", minute=32),
        _observation(source="legacy", ts_code="300003.SZ", minute=33),
    )
    isolated = (
        _observation(source="isolated", ts_code="300001.SZ"),
        _observation(source="isolated", ts_code="300002.SZ", minute=35),
        _observation(source="isolated", ts_code="300003.SZ", minute=33, action="watch"),
    )

    report = build_shadow_session_report(
        trade_date=date(2026, 8, 3),
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
    )

    assert report.matched_count == 1
    assert report.legacy_only_count == 2
    assert report.isolated_only_count == 2
    assert report.legacy_recall_bps == 3333
    assert report.isolated_precision_bps == 3333
    assert report.matches[0].event_delta_microseconds == 0
    assert report.matches[0].availability_delta_microseconds == 0
    assert {item.reason for item in report.discrepancies} == {
        "action_mismatch",
        "outside_time_tolerance",
    }


def test_shadow_report_maximizes_cardinality_before_minimizing_delta() -> None:
    legacy = (
        _observation(source="legacy", ts_code="300001.SZ", minute=3, evidence_salt="l1"),
        _observation(source="legacy", ts_code="300001.SZ", minute=8, evidence_salt="l2"),
    )
    isolated = (
        _observation(source="isolated", ts_code="300001.SZ", minute=0, evidence_salt="i1"),
        _observation(source="isolated", ts_code="300001.SZ", minute=5, evidence_salt="i2"),
    )
    report = build_shadow_session_report(
        trade_date=date(2026, 8, 3),
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=180_000_000,
    )
    assert report.matched_count == 2


def test_time_matcher_uses_linear_memory_for_dense_groups() -> None:
    legacy = tuple(
        _observation(
            source="legacy",
            ts_code="300001.SZ",
            event_offset_microseconds=index,
            evidence_salt=f"legacy-dense-{index}",
        )
        for index in range(400)
    )
    isolated = tuple(
        _observation(
            source="isolated",
            ts_code="300001.SZ",
            event_offset_microseconds=index,
            evidence_salt=f"isolated-dense-{index}",
        )
        for index in range(400)
    )

    tracemalloc.start()
    try:
        pairs = shadow_validation._maximum_time_matches(
            legacy,
            isolated,
            tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert len(pairs) == 400
    assert peak < 4 * 1024 * 1024


def test_report_builder_fails_closed_at_row_and_byte_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = tuple(
        _observation(
            source="legacy",
            ts_code=f"{300001 + index:06d}.SZ",
            evidence_salt=f"budget-{index}",
        )
        for index in range(3)
    )
    monkeypatch.setattr(shadow_validation, "_MAX_REPORT_OBSERVATIONS", 2)
    with pytest.raises(ValueError, match="observation.*budget"):
        build_shadow_session_report(
            trade_date=date(2026, 8, 3),
            legacy=legacy,
            isolated=(),
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        )

    monkeypatch.setattr(shadow_validation, "_MAX_REPORT_OBSERVATIONS", 100)
    monkeypatch.setattr(shadow_validation, "_MAX_REPORT_INPUT_BYTES", 64)
    with pytest.raises(ValueError, match="byte.*budget"):
        build_shadow_session_report(
            trade_date=date(2026, 8, 3),
            legacy=(legacy[0],),
            isolated=(),
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        )


def test_time_matcher_scales_to_one_hundred_thousand_rows() -> None:
    class _Point:
        __slots__ = ("event_time",)

        def __init__(self, event_time: datetime) -> None:
            self.event_time = event_time

    start = datetime(2026, 8, 3, 1, 25, tzinfo=UTC)
    legacy = tuple(_Point(start + timedelta(microseconds=index)) for index in range(100_000))
    isolated = tuple(_Point(start + timedelta(microseconds=index)) for index in range(100_000))

    pairs = shadow_validation._maximum_time_matches(
        legacy,
        isolated,
        tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
    )

    assert len(pairs) == 100_000


def test_legacy_export_proxy_is_not_counted_as_comparable_latency() -> None:
    report = build_shadow_session_report(
        trade_date=date(2026, 8, 3),
        legacy=(
            _observation(
                source="legacy",
                ts_code="300001.SZ",
                availability_basis="export_observed_proxy",
                availability_delay_microseconds=60_000_000,
            ),
        ),
        isolated=(
            _observation(
                source="isolated",
                ts_code="300001.SZ",
                availability_delay_microseconds=4_000_001,
            ),
        ),
        match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
    )
    assert report.matches[0].availability_delta_microseconds is None
    assert report.p95_latency_delta_microseconds is None
    assert report.isolated_latency_coverage_bps == 10_000
    assert report.isolated_p95_latency_microseconds == 4_000_001


def test_report_recomputes_every_derived_value_from_raw_observations() -> None:
    report = _report_for_date(date(2026, 8, 3))
    forged = report.model_dump(mode="python")
    forged["report_id"] = None
    forged["matched_count"] = 0
    forged["legacy_recall_bps"] = 10_000
    forged["matches"][0]["event_delta_microseconds"] = 9_999_000_000

    with pytest.raises(ValidationError, match="derived|match"):
        ShadowSessionReport.model_validate(forged)


def test_bps_is_floored_for_18999_of_20000() -> None:
    trade_date = date(2026, 8, 3)
    legacy = tuple(
        _observation(
            source="legacy",
            ts_code=f"{index:06d}.SZ",
            evidence_salt=f"legacy-{index}",
        )
        for index in range(20_000)
    )
    isolated = tuple(
        _observation(
            source="isolated",
            ts_code=f"{index:06d}.SZ",
            evidence_salt=f"isolated-{index}",
        )
        for index in range(18_999)
    )
    report = build_shadow_session_report(
        trade_date=trade_date,
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
    )
    assert report.legacy_recall_bps == 9499


def test_duplicate_business_event_is_rejected_even_with_new_evidence() -> None:
    first = _observation(source="legacy", ts_code="300001.SZ", evidence_salt="first")
    retry = _observation(
        source="legacy",
        ts_code="300001.SZ",
        evidence_salt="retry",
        upstream_salt="first",
    )
    with pytest.raises(ValueError, match="duplicate.*business event|upstream"):
        build_shadow_session_report(
            trade_date=date(2026, 8, 3),
            legacy=(first, retry),
            isolated=(),
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        )


def test_duplicate_semantic_event_is_rejected_even_with_new_upstream_id() -> None:
    first = _observation(
        source="legacy",
        ts_code="300001.SZ",
        evidence_salt="first",
        upstream_salt="upstream-first",
    )
    retry = _observation(
        source="legacy",
        ts_code="300001.SZ",
        evidence_salt="retry",
        upstream_salt="upstream-retry",
    )

    with pytest.raises(ValueError, match="duplicate.*business event|semantic"):
        build_shadow_session_report(
            trade_date=date(2026, 8, 3),
            legacy=(first, retry),
            isolated=(),
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
        )


def test_policy_fixes_tolerance_requires_bindings_and_ten_to_twenty_sessions() -> None:
    with pytest.raises(ValidationError):
        ShadowRetirementPolicy(
            required_consecutive_sessions=9,
            strategy_bindings=(_binding(),),
        )
    with pytest.raises(ValidationError):
        ShadowRetirementPolicy(
            required_consecutive_sessions=10,
            strategy_bindings=(),
        )
    with pytest.raises(ValidationError):
        ShadowRetirementPolicy(
            required_consecutive_sessions=10,
            strategy_bindings=(_binding(),),
            match_tolerance_microseconds=6 * 60 * 60 * 1_000_000,
        )
    with pytest.raises(ValidationError):
        ShadowRetirementPolicy(
            required_consecutive_sessions=10,
            strategy_bindings=(_binding(),),
            match_tolerance_microseconds=30 * 60 * 1_000_000,
        )
    with pytest.raises(ValidationError):
        ShadowRetirementPolicy(
            required_consecutive_sessions=10,
            strategy_bindings=(_binding(),),
            match_tolerance_microseconds=60_000_000.0,
        )


def test_strategy_bindings_keep_parallel_executable_identities_distinct() -> None:
    first = ShadowStrategyBinding(
        strategy_id="growth_board_surge",
        strategy_version=1,
        definition_fingerprint="1" * 64,
        executable_fingerprint="2" * 64,
    )
    second = ShadowStrategyBinding(
        strategy_id="growth_board_surge",
        strategy_version=1,
        definition_fingerprint="1" * 64,
        executable_fingerprint="3" * 64,
    )

    policy = _policy(bindings=(first, second))

    assert policy.strategy_bindings == (first, second)


def test_report_keeps_parallel_strategy_implementations_distinct() -> None:
    legacy_base = _observation(source="legacy", ts_code="300001.SZ")
    isolated_base = _observation(source="isolated", ts_code="300001.SZ")

    def rebound(
        item: ShadowObservation,
        *,
        source: str,
        definition: str,
        executable: str,
        salt: str,
    ) -> ShadowObservation:
        payload = item.model_dump(mode="python")
        payload.update(
            observation_id=None,
            source=source,
            definition_fingerprint=definition,
            executable_fingerprint=executable,
            upstream_event_id=canonical_sha256({"upstream": salt}),
            evidence_id=canonical_sha256({"evidence": salt}),
        )
        return ShadowObservation.model_validate(payload)

    legacy = (
        rebound(
            legacy_base,
            source="legacy",
            definition="1" * 64,
            executable="2" * 64,
            salt="legacy-one",
        ),
        rebound(
            legacy_base,
            source="legacy",
            definition="1" * 64,
            executable="3" * 64,
            salt="legacy-two",
        ),
    )
    isolated = (
        rebound(
            isolated_base,
            source="isolated",
            definition="1" * 64,
            executable="2" * 64,
            salt="isolated-one",
        ),
        rebound(
            isolated_base,
            source="isolated",
            definition="1" * 64,
            executable="3" * 64,
            salt="isolated-two",
        ),
    )

    report = build_shadow_session_report(
        trade_date=date(2026, 8, 3),
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
    )

    assert report.matched_count == 2
    assert {item.executable_fingerprint for item in report.legacy_observations} == {
        "2" * 64,
        "3" * 64,
    }


def test_six_hour_report_tolerance_cannot_pass_the_fixed_policy() -> None:
    trade_date = OPEN_DATES[-1]
    legacy = _observation(
        source="legacy",
        ts_code="300001.SZ",
        trade_date=trade_date,
        event_offset_microseconds=0,
    )
    isolated = _observation(
        source="isolated",
        ts_code="300001.SZ",
        trade_date=trade_date,
        event_offset_microseconds=6 * 60 * 60 * 1_000_000,
    )

    with pytest.raises(ValidationError, match="less than or equal"):
        build_shadow_session_report(
            trade_date=trade_date,
            legacy=(legacy,),
            isolated=(isolated,),
            match_tolerance_microseconds=6 * 60 * 60 * 1_000_000,
        )


def test_calendar_selection_rejects_weekend_authority_and_binds_latest_closed_session() -> None:
    with pytest.raises(ValueError, match="weekend"):
        _selection(open_dates=(date(2026, 8, 1), date(2026, 8, 3)))

    selection = _selection()
    assert selection.latest_closed_session == date(2026, 8, 14)
    assert selection.selected_open_dates == OPEN_DATES
    assert selection.selection_id is not None

    forged = selection.model_dump(mode="python")
    forged["selection_id"] = None
    forged["selected_open_dates"] = (*OPEN_DATES[1:], date(2026, 8, 17))
    forged["latest_closed_session"] = date(2026, 8, 17)
    with pytest.raises(ValidationError, match="authority|selected open dates"):
        ShadowCalendarSelection.model_validate(forged)


def test_retirement_gate_requires_latest_authoritative_sessions_per_strategy() -> None:
    bindings = (_binding("n_shape"), _binding("growth_board_surge"))
    policy = _policy(bindings=bindings)
    reports = tuple(_production_report_for_date(item, bindings=bindings) for item in OPEN_DATES)

    passed = evaluate_shadow_retirement_gate(
        reports,
        calendar_selection=_selection(),
        policy=policy,
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )
    assert passed.passed is True
    assert passed.accepted_session_count == 10
    assert {item.binding.strategy_id for item in passed.strategy_results} == {
        "n_shape",
        "growth_board_surge",
    }

    growth_missing = tuple(
        _production_report_for_date(item, bindings=(_binding("n_shape"),)) for item in OPEN_DATES
    )
    rejected = evaluate_shadow_retirement_gate(
        growth_missing,
        calendar_selection=_selection(),
        policy=policy,
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )
    assert rejected.passed is False
    growth = next(
        item
        for item in rejected.strategy_results
        if item.binding.strategy_id == "growth_board_surge"
    )
    assert growth.accepted_session_count == 0


def test_production_retirement_rejects_hmac_and_missing_report_verifier() -> None:
    reports = tuple(_production_report_for_date(item, legacy_hmac=True) for item in OPEN_DATES)

    evaluation = evaluate_shadow_retirement_gate(
        reports,
        calendar_selection=_selection(),
        policy=_policy(),
        attestation_verifier=ATTESTATION_AUTHORITY,
        report_receipt_verifier=None,
    )

    assert evaluation.passed is False
    assert evaluation.accepted_session_count == 0
    assert all(
        "production_ed25519_verifier_required" in item.reason_codes
        for item in evaluation.strategy_results
    )


def test_fixture_reports_cannot_unlock_the_production_retirement_gate() -> None:
    reports = tuple(_report_for_date(item) for item in OPEN_DATES)

    evaluation = evaluate_shadow_retirement_gate(
        reports,
        calendar_selection=_selection(),
        policy=_policy(),
        attestation_verifier=ATTESTATION_AUTHORITY,
    )

    assert evaluation.passed is False
    assert evaluation.accepted_session_count == 0
    assert "non_production_evidence" in evaluation.reason_codes


def test_unsigned_synthetic_production_strings_cannot_unlock_retirement() -> None:
    reports = tuple(_production_report_for_date(item, signed=False) for item in OPEN_DATES)

    evaluation = evaluate_shadow_retirement_gate(
        reports,
        calendar_selection=_selection(),
        policy=_policy(),
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )

    assert evaluation.passed is False
    assert evaluation.accepted_session_count == 0
    assert "report_receipt_unverified" in evaluation.reason_codes


def test_production_evidence_binds_calendar_cutoff_snapshots_and_session_coverage() -> None:
    binding = _binding()
    reports = []
    for trade_date in OPEN_DATES:
        legacy = (
            _observation(
                source="legacy",
                ts_code="300001.SZ",
                trade_date=trade_date,
                availability_basis="export_observed_proxy",
                upstream_salt=f"legacy-{trade_date}",
            ),
        )
        isolated = (
            _observation(
                source="isolated",
                ts_code="300001.SZ",
                trade_date=trade_date,
                upstream_salt=f"isolated-{trade_date}",
            ),
        )
        report = build_shadow_session_report(
            trade_date=trade_date,
            legacy=legacy,
            isolated=isolated,
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
            evidence=_production_evidence(
                trade_date,
                bindings=(binding,),
                legacy=legacy,
                isolated=isolated,
            ),
            attestation_verifier=_ed25519_authority().keyring,
        )
        reports.append(
            attach_shadow_report_receipt(
                report,
                signer=_ed25519_authority().signer,
                verifier=_ed25519_authority().keyring,
                producer_service_id="shadow-daily",
                producer_instance_id="validation-test",
            )
        )

    evaluation = evaluate_shadow_retirement_gate(
        reports,
        calendar_selection=_selection(),
        policy=_policy(),
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )

    assert evaluation.passed is True
    assert evaluation.accepted_session_count == 10


def test_production_evidence_rejects_reviewer_calendar_receipt_counterexample() -> None:
    trade_date = OPEN_DATES[-1]
    binding = _binding()
    legacy = (_observation(source="legacy", ts_code="300001.SZ", trade_date=trade_date),)
    isolated = (_observation(source="isolated", ts_code="300001.SZ", trade_date=trade_date),)

    with pytest.raises(ValueError, match="calendar.*receipt|receipt.*calendar"):
        _production_evidence(
            trade_date,
            bindings=(binding,),
            legacy=legacy,
            isolated=isolated,
            calendar_authority_id=REVIEWER_CALENDAR_AUTHORITY_ID,
            receipt_calendar_generation_id=REVIEWER_WRONG_RECEIPT_CALENDAR_ID,
        )


def test_production_evidence_rejects_mixed_calendar_receipts_across_versions() -> None:
    trade_date = OPEN_DATES[-1]
    bindings = (_binding(strategy_version=1), _binding(strategy_version=2))
    legacy = tuple(
        _observation(
            source="legacy",
            ts_code=f"30000{version}.SZ",
            trade_date=trade_date,
            strategy_version=version,
        )
        for version in (1, 2)
    )
    isolated = tuple(
        _observation(
            source="isolated",
            ts_code=f"30000{version}.SZ",
            trade_date=trade_date,
            strategy_version=version,
        )
        for version in (1, 2)
    )
    evidence = _production_evidence(
        trade_date,
        bindings=bindings,
        legacy=legacy,
        isolated=isolated,
        calendar_authority_id=REVIEWER_CALENDAR_AUTHORITY_ID,
        receipt_calendar_generation_id=REVIEWER_CALENDAR_AUTHORITY_ID,
    )
    payload = evidence.model_dump(mode="python")
    payload["evidence_id"] = None
    second_isolated = next(
        snapshot
        for snapshot in payload["input_snapshots"]
        if snapshot["source"] == "isolated" and snapshot["binding"]["strategy_version"] == 2
    )
    second_isolated["snapshot_id"] = None
    receipt_payload = second_isolated["completion_receipt"]
    receipt_payload["receipt_id"] = None
    receipt_payload["calendar_generation_id"] = REVIEWER_WRONG_RECEIPT_CALENDAR_ID
    with pytest.raises(ValidationError, match="attestation.*calendar|calendar.*receipt"):
        ShadowSessionEvidence.model_validate(payload)


def test_report_builder_revalidates_completion_receipt_calendar_lineage() -> None:
    valid = _production_report_for_date(OPEN_DATES[-1])
    historical = _replace_isolated_receipt_calendar_without_revalidating_report(
        valid,
        calendar_generation_id=REVIEWER_WRONG_RECEIPT_CALENDAR_ID,
    )
    assert historical.evidence is not None

    with pytest.raises(ValueError, match="calendar.*receipt|receipt.*calendar"):
        build_shadow_session_report(
            trade_date=historical.trade_date,
            legacy=historical.legacy_observations,
            isolated=historical.isolated_observations,
            match_tolerance_microseconds=historical.match_tolerance_microseconds,
            evidence=historical.evidence,
            attestation_verifier=_ed25519_authority().keyring,
        )


def test_production_evidence_rejects_incomplete_session_tail() -> None:
    trade_date = OPEN_DATES[-1]
    binding = _binding()
    legacy = (_observation(source="legacy", ts_code="300001.SZ", trade_date=trade_date),)
    isolated = (_observation(source="isolated", ts_code="300001.SZ", trade_date=trade_date),)
    evidence = _production_evidence(
        trade_date,
        bindings=(binding,),
        legacy=legacy,
        isolated=isolated,
    )
    payload = evidence.model_dump(mode="python")
    payload["evidence_id"] = None
    snapshot = payload["input_snapshots"][1]
    snapshot["snapshot_id"] = None
    snapshot["complete_through"] -= timedelta(microseconds=1)
    receipt_payload = snapshot["completion_receipt"]
    receipt_payload["receipt_id"] = None
    receipt_payload["complete_through"] = snapshot["complete_through"]
    with pytest.raises(ValidationError, match="full receipt body"):
        ShadowSourceCompletionReceipt.model_validate(receipt_payload)


def test_production_report_rejects_events_outside_market_session() -> None:
    trade_date = OPEN_DATES[-1]
    binding = _binding()
    legacy = (
        _observation(
            source="legacy",
            ts_code="300001.SZ",
            trade_date=trade_date,
            minute=1,
        ),
    )
    isolated = (
        _observation(
            source="isolated",
            ts_code="300001.SZ",
            trade_date=trade_date,
            minute=1,
        ),
    )

    with pytest.raises(ValidationError, match="market session"):
        build_shadow_session_report(
            trade_date=trade_date,
            legacy=legacy,
            isolated=isolated,
            match_tolerance_microseconds=MATCH_TOLERANCE_MICROSECONDS,
            evidence=_production_evidence(
                trade_date,
                bindings=(binding,),
                legacy=legacy,
                isolated=isolated,
            ),
            attestation_verifier=_ed25519_authority().keyring,
        )


def test_production_report_binds_observations_to_snapshot_producer() -> None:
    trade_date = OPEN_DATES[-1]
    binding = _binding()
    legacy = (_observation(source="legacy", ts_code="300001.SZ", trade_date=trade_date),)
    isolated = (_observation(source="isolated", ts_code="300001.SZ", trade_date=trade_date),)
    evidence = _production_evidence(
        trade_date,
        bindings=(binding,),
        legacy=legacy,
        isolated=isolated,
    )
    payload = evidence.model_dump(mode="python")
    payload["evidence_id"] = None
    snapshot = payload["input_snapshots"][1]
    snapshot["snapshot_id"] = None
    snapshot["producer_commit"] = "b" * 40
    receipt_payload = snapshot["completion_receipt"]
    receipt_payload["receipt_id"] = None
    receipt_payload["producer_commit"] = "b" * 40
    with pytest.raises(ValidationError, match="attestation.*producer_commit|producer"):
        ShadowSessionEvidence.model_validate(payload)


def test_retirement_gate_deduplicates_identical_retry_reports() -> None:
    reports = tuple(_production_report_for_date(item) for item in OPEN_DATES)

    evaluation = evaluate_shadow_retirement_gate(
        tuple(item for report in reports for item in (report, report)),
        calendar_selection=_selection(),
        policy=_policy(),
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )

    assert evaluation.passed is True
    assert evaluation.accepted_session_count == 10
    assert len(evaluation.evaluated_report_ids) == 10


def test_retirement_gate_rejects_historical_receipt_from_another_calendar_lineage() -> None:
    reports = list(_production_report_for_date(item) for item in OPEN_DATES)
    reports[-1] = _replace_isolated_receipt_calendar_without_revalidating_report(
        reports[-1],
        calendar_generation_id=REVIEWER_WRONG_RECEIPT_CALENDAR_ID,
    )

    with pytest.raises(ValidationError, match="calendar.*receipt|receipt.*calendar"):
        evaluate_shadow_retirement_gate(
            reports,
            calendar_selection=_selection(),
            policy=_policy(),
            attestation_verifier=_ed25519_authority().keyring,
            report_receipt_verifier=_ed25519_authority().keyring,
        )


def test_calendar_authority_rotation_requires_a_new_shadow_lineage() -> None:
    old_authority = _calendar()
    new_authority = _calendar(producer_commit="f" * 40)
    old_reports = tuple(
        _production_report_for_date(
            item,
            calendar_authority_id=str(old_authority.content_sha256),
        )
        for item in OPEN_DATES
    )
    new_reports = tuple(
        _production_report_for_date(
            item,
            calendar_authority_id=str(new_authority.content_sha256),
        )
        for item in OPEN_DATES
    )

    old_evaluation = evaluate_shadow_retirement_gate(
        old_reports,
        calendar_selection=_selection(authority=old_authority),
        policy=_policy(),
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )
    old_reports_against_new_authority = evaluate_shadow_retirement_gate(
        old_reports,
        calendar_selection=_selection(authority=new_authority),
        policy=_policy(),
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )
    new_evaluation = evaluate_shadow_retirement_gate(
        new_reports,
        calendar_selection=_selection(authority=new_authority),
        policy=_policy(),
        attestation_verifier=_ed25519_authority().keyring,
        report_receipt_verifier=_ed25519_authority().keyring,
    )

    assert old_evaluation.passed is True
    assert old_reports_against_new_authority.passed is False
    assert "calendar_identity_mismatch" in old_reports_against_new_authority.reason_codes
    assert new_evaluation.passed is True
    assert old_reports[0].report_id != new_reports[0].report_id
    assert old_reports[0].evidence is not None
    assert new_reports[0].evidence is not None
    assert old_reports[0].evidence.evidence_id != new_reports[0].evidence.evidence_id


def test_retirement_gate_uses_absolute_isolated_latency_with_legacy_proxy() -> None:
    reports = tuple(
        _report_for_date(item, isolated_delay_microseconds=10_000_001) for item in OPEN_DATES
    )
    evaluation = evaluate_shadow_retirement_gate(
        reports,
        calendar_selection=_selection(),
        policy=_policy(maximum_isolated_p95_latency_microseconds=10_000_000),
    )
    assert evaluation.passed is False
    assert evaluation.accepted_session_count == 0


def test_publication_is_immutable_loadable_and_idempotent(tmp_path: Path) -> None:
    report = _report_for_date(date(2026, 8, 3))
    first = publish_shadow_session_report(tmp_path / "shadow", report)
    second = publish_shadow_session_report(tmp_path / "shadow", report)
    loaded = load_shadow_session_report(first, expected_report_id=str(report.report_id))

    assert second == first
    assert loaded == report
    assert first.stat().st_nlink == 1
    assert first.stat().st_mode & 0o777 == 0o600

    first.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="shadow report"):
        load_shadow_session_report(first, expected_report_id=str(report.report_id))


def test_publication_rejects_conflicting_payload_for_same_session(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    first = _report_for_date(date(2026, 8, 3))
    conflicting = _report_for_date(
        date(2026, 8, 3),
        isolated_delay_microseconds=4_000_000,
    )
    publish_shadow_session_report(root, first)

    with pytest.raises(ValueError, match="session.*conflict|conflict.*session"):
        publish_shadow_session_report(root, conflicting)

    session_reports = tuple(
        item for item in (root / "2026-08-03").glob("*.json") if not item.name.startswith(".")
    )
    assert len(session_reports) == 1


def test_publication_rejects_report_above_serialized_byte_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shadow_validation, "_MAX_REPORT_BYTES", 64)

    with pytest.raises(ValueError, match="report.*byte budget"):
        publish_shadow_session_report(
            tmp_path / "shadow",
            _report_for_date(date(2026, 8, 3)),
        )

    assert not (tmp_path / "shadow").exists()


def test_loader_rejects_noncanonical_json_and_ancestor_symlink(tmp_path: Path) -> None:
    import json

    report = _report_for_date(date(2026, 8, 3))
    published = publish_shadow_session_report(tmp_path / "shadow", report)
    payload = json.loads(published.read_bytes())
    published.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical|content"):
        load_shadow_session_report(
            published,
            expected_report_id=str(report.report_id),
        )

    outside = tmp_path / "outside"
    outside.mkdir()
    escaped = outside / published.name
    escaped.write_bytes(report.canonical_bytes())
    escaped.chmod(0o600)
    alias = tmp_path / "loader-alias"
    alias.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink|unsafe"):
        load_shadow_session_report(
            alias / published.name,
            expected_report_id=str(report.report_id),
        )


def test_loader_rejects_wide_json_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    report_id = "0" * 64
    path = tmp_path / f"{report_id}.json"
    path.write_text(
        json.dumps(
            {"wide": list(range(32))},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    monkeypatch.setattr(shadow_validation, "_MAX_REPORT_JSON_NODES", 8, raising=False)
    monkeypatch.setattr(
        "rquant.strict_json.json.loads",
        lambda _value: pytest.fail("wide report must fail before json.loads"),
    )

    with pytest.raises(ValueError, match="node|width"):
        load_shadow_session_report(path, expected_report_id=report_id)


def test_publication_rejects_ancestor_symlink_escape(tmp_path: Path) -> None:
    report = _report_for_date(date(2026, 8, 3))
    outside = tmp_path / "outside"
    outside.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink|unsafe"):
        publish_shadow_session_report(alias / "shadow", report)
    assert not tuple(outside.rglob("*.json"))


def test_publication_recovers_after_hard_exit_between_link_and_cleanup(tmp_path: Path) -> None:
    report = _report_for_date(date(2026, 8, 3))
    root = tmp_path / "shadow"
    process = multiprocessing.get_context("spawn").Process(
        target=_crash_after_link,
        args=(str(root), report.model_dump(mode="json")),
    )
    process.start()
    process.join(10)
    assert process.exitcode == 91

    session = root / report.trade_date.isoformat()
    unrelated_intent = session / f".publish-{'f' * 64}.intent.json"
    unrelated_temporary = session / f".shadow-{'f' * 64}.tmp"
    unrelated_intent.write_bytes(b"other-report-intent")
    unrelated_temporary.write_bytes(b"other-report-temporary")

    published = publish_shadow_session_report(root, report)
    assert load_shadow_session_report(published, expected_report_id=str(report.report_id)) == report
    assert published.stat().st_nlink == 1
    assert unrelated_intent.read_bytes() == b"other-report-intent"
    assert unrelated_temporary.read_bytes() == b"other-report-temporary"
    assert not (session / f".publish-{report.report_id}.intent.json").exists()
    assert not (session / f".shadow-{report.report_id}.tmp").exists()
