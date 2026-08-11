"""Authority-driven daily shadow report and legacy retirement evaluation."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from pydantic import Field

from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_job import (
    prepare_shadow_report_retry,
    publish_shadow_report_retry,
)
from rquant.runtime_shadow_sources import (
    LegacyMonitorEvent,
    LegacySurgeEvent,
    ShadowRunnerSignalSource,
    read_isolated_runner_shadow_snapshot,
    read_legacy_monitor_shadow_snapshots,
    read_legacy_surge_events_shadow_snapshots,
)
from rquant.runtime_shadow_validation import (
    Ed25519CompletionAttestationKeyring,
    Ed25519ShadowReceiptKeyring,
    Ed25519ShadowReceiptSigner,
    ShadowCalendarSelection,
    ShadowInputSnapshotIdentity,
    ShadowRetirementEvaluation,
    ShadowRetirementPolicy,
    ShadowSessionEvidence,
    ShadowSessionReport,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    attach_shadow_report_receipt,
    build_shadow_session_report,
    evaluate_shadow_retirement_gate,
    load_shadow_session_report,
    shadow_observation_set_id,
    shadow_session_boundaries,
    verify_shadow_report_receipt,
)

_REPORT_NAME = re.compile(r"^(?P<report_id>[0-9a-f]{64})\.json$")
_SUPPORTED_SHADOW_STRATEGIES = frozenset({"n_shape", "growth_board_surge"})


class ShadowDailyServiceResult(RuntimeContractModel):
    trade_date: date
    calendar_selection: ShadowCalendarSelection
    report: ShadowSessionReport
    report_path: Path
    evaluation: ShadowRetirementEvaluation
    historical_report_count: int = Field(ge=0)


def _report_id_from_path(path: Path) -> str:
    matched = _REPORT_NAME.fullmatch(Path(path).name)
    if matched is None:
        raise ValueError("historical shadow report path has an invalid name")
    return matched.group("report_id")


def run_shadow_daily_service(
    *,
    calendar: MarketCalendarAuthority,
    evaluated_at: datetime,
    policy: ShadowRetirementPolicy,
    report_root: Path,
    legacy_exported_at: datetime,
    legacy_monitor_commit: str,
    legacy_surge_commit: str,
    report_producer_commit: str,
    report_producer_version: str,
    legacy_monitor_events: Iterable[LegacyMonitorEvent],
    legacy_monitor_completion_receipt: ShadowSourceCompletionReceipt,
    legacy_surge_events: Iterable[LegacySurgeEvent],
    legacy_surge_completion_receipt: ShadowSourceCompletionReceipt,
    isolated_sources: Iterable[tuple[ShadowStrategyBinding, ShadowRunnerSignalSource]],
    attestation_verifier: Ed25519CompletionAttestationKeyring,
    report_receipt_signer: Ed25519ShadowReceiptSigner,
    report_receipt_verifier: Ed25519ShadowReceiptKeyring,
    report_producer_service_id: str,
    report_producer_instance_id: str,
    historical_report_paths: Iterable[Path] = (),
    source_batch_size: int = 1_000,
    source_max_records: int = 100_000,
    source_max_raw_bytes: int = 128 * 1024 * 1024,
    source_max_record_bytes: int = 4 * 1024 * 1024,
) -> ShadowDailyServiceResult:
    """Publish the latest closed session and evaluate its real calendar suffix."""

    if not isinstance(attestation_verifier, Ed25519CompletionAttestationKeyring):
        raise ValueError("production daily completion verifier must be an Ed25519 keyring")
    if not isinstance(report_receipt_signer, Ed25519ShadowReceiptSigner):
        raise ValueError("production daily report signer must be an Ed25519 signer client")
    if not isinstance(report_receipt_verifier, Ed25519ShadowReceiptKeyring):
        raise ValueError("production daily report verifier must be an Ed25519 keyring")
    if report_receipt_signer.key_id != report_receipt_verifier.active_key_id:
        raise ValueError("production report signer must use the active key id")
    if not report_producer_service_id.strip() or not report_producer_instance_id.strip():
        raise ValueError("production daily report producer identity must be complete")
    verified_policy = ShadowRetirementPolicy.model_validate(policy.model_dump(mode="python"))
    normalized_evaluated_at = normalize_aware_utc(evaluated_at)
    if normalize_aware_utc(legacy_exported_at) > normalized_evaluated_at:
        raise ValueError("legacy export cannot be later than shadow evaluation")
    selection = ShadowCalendarSelection.create(
        authority=calendar,
        evaluated_at=normalized_evaluated_at,
        maximum_sessions=20,
    )
    trade_date = selection.latest_closed_session
    retry = prepare_shadow_report_retry(
        report_root=report_root,
        trade_date=trade_date,
        requested_at=normalized_evaluated_at,
        producer_commit=report_producer_commit,
        producer_version=report_producer_version,
    )
    report_cutoff = retry.effective_cutoff
    bindings = verified_policy.strategy_bindings
    unsupported = {item.strategy_id for item in bindings} - _SUPPORTED_SHADOW_STRATEGIES
    if unsupported:
        raise ValueError("shadow daily service has an unsupported strategy binding")
    prepared_isolated_sources = tuple(isolated_sources)
    source_bindings = tuple(item[0] for item in prepared_isolated_sources)
    if len(source_bindings) != len(set(source_bindings)):
        raise ValueError("isolated shadow source bindings must be unique")
    if set(source_bindings) != set(bindings):
        raise ValueError("isolated shadow sources must exactly match policy bindings")
    monitor_bindings = tuple(item for item in bindings if item.strategy_id == "n_shape")
    surge_bindings = tuple(item for item in bindings if item.strategy_id == "growth_board_surge")
    legacy_snapshots = dict(
        (
            *read_legacy_monitor_shadow_snapshots(
                legacy_monitor_events,
                trade_date=trade_date,
                exported_at=legacy_exported_at,
                producer_commit=legacy_monitor_commit,
                bindings=monitor_bindings,
                completion_receipt=legacy_monitor_completion_receipt,
                max_records=source_max_records,
                max_raw_bytes=source_max_raw_bytes,
                max_record_bytes=source_max_record_bytes,
            ),
            *read_legacy_surge_events_shadow_snapshots(
                legacy_surge_events,
                trade_date=trade_date,
                exported_at=legacy_exported_at,
                producer_commit=legacy_surge_commit,
                bindings=surge_bindings,
                completion_receipt=legacy_surge_completion_receipt,
                max_records=source_max_records,
                max_raw_bytes=source_max_raw_bytes,
                max_record_bytes=source_max_record_bytes,
            ),
        )
    )
    legacy = [
        observation
        for binding in bindings
        for observation in legacy_snapshots[binding].observations
    ]

    isolated = []
    isolated_snapshots = {}
    for binding, source in sorted(
        prepared_isolated_sources,
        key=lambda item: (
            item[0].strategy_id,
            item[0].strategy_version,
            item[0].definition_fingerprint,
            item[0].executable_fingerprint,
        ),
    ):
        frozen = read_isolated_runner_shadow_snapshot(
            source,
            trade_date=trade_date,
            observed_at=report_cutoff,
            binding=binding,
            expected_calendar_authority_id=str(selection.authority.content_sha256),
            batch_size=source_batch_size,
            max_records=source_max_records,
            max_raw_bytes=source_max_raw_bytes,
            max_record_bytes=source_max_record_bytes,
            attestation_verifier=attestation_verifier,
        )
        isolated_snapshots[binding] = frozen
        isolated.extend(frozen.observations)

    session_open, session_close = shadow_session_boundaries(trade_date)
    normalized_legacy_exported_at = normalize_aware_utc(legacy_exported_at)
    if normalized_legacy_exported_at < session_close:
        raise ValueError("legacy export does not cover the complete session")
    input_snapshots = []
    for binding in bindings:
        legacy_snapshot = legacy_snapshots[binding]
        legacy_items = legacy_snapshot.observations
        input_snapshots.append(
            ShadowInputSnapshotIdentity(
                source="legacy",
                source_id=legacy_snapshot.source_id,
                binding=binding,
                raw_input_id=legacy_snapshot.raw_input_id,
                completion_receipt=legacy_snapshot.completion_receipt,
                upstream_snapshot_id=legacy_snapshot.upstream_snapshot_id,
                observation_set_id=shadow_observation_set_id(legacy_items),
                captured_at=legacy_snapshot.captured_at,
                complete_through=legacy_snapshot.complete_through,
                producer_commit=legacy_snapshot.completion_receipt.producer_commit,
                producer_version=legacy_snapshot.completion_receipt.producer_version,
            )
        )
        frozen = isolated_snapshots[binding]
        input_snapshots.append(
            ShadowInputSnapshotIdentity(
                source="isolated",
                source_id=frozen.source_id,
                binding=binding,
                raw_input_id=frozen.raw_input_id,
                completion_receipt=frozen.completion_receipt,
                upstream_snapshot_id=frozen.upstream_snapshot_id,
                observation_set_id=shadow_observation_set_id(frozen.observations),
                captured_at=frozen.captured_at,
                complete_through=frozen.complete_through,
                producer_commit=frozen.completion_receipt.producer_commit,
                producer_version=frozen.completion_receipt.producer_version,
            )
        )
    evidence = ShadowSessionEvidence(
        evidence_origin="production",
        calendar_authority_id=str(calendar.content_sha256),
        evaluation_cutoff=report_cutoff,
        session_open_at=session_open,
        session_close_at=session_close,
        producer_commit=report_producer_commit,
        producer_version=report_producer_version,
        input_snapshots=tuple(input_snapshots),
    )

    report = build_shadow_session_report(
        trade_date=trade_date,
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=verified_policy.match_tolerance_microseconds,
        evidence=evidence,
        attestation_verifier=attestation_verifier,
    )
    report = attach_shadow_report_receipt(
        report,
        signer=report_receipt_signer,
        verifier=report_receipt_verifier,
        producer_service_id=report_producer_service_id,
        producer_instance_id=report_producer_instance_id,
    )
    if not verify_shadow_report_receipt(report, report_receipt_verifier):
        raise ValueError("new shadow report receipt is untrusted")
    report, report_path = publish_shadow_report_retry(retry, report)

    historical = []
    for path in historical_report_paths:
        historical.append(
            load_shadow_session_report(
                Path(path),
                expected_report_id=_report_id_from_path(Path(path)),
            )
        )
    all_reports = (*historical, report)
    evaluation = evaluate_shadow_retirement_gate(
        all_reports,
        calendar_selection=selection,
        policy=verified_policy,
        attestation_verifier=attestation_verifier,
        report_receipt_verifier=report_receipt_verifier,
    )
    return ShadowDailyServiceResult(
        trade_date=trade_date,
        calendar_selection=selection,
        report=report,
        report_path=report_path,
        evaluation=evaluation,
        historical_report_count=len(historical),
    )


__all__ = ["ShadowDailyServiceResult", "run_shadow_daily_service"]
