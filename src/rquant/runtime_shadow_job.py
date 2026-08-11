"""One-session shadow evidence collection and immutable publication."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from rquant.runtime_contracts import normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_sources import (
    ShadowRunnerSignalSource,
    read_isolated_runner_shadow_snapshot,
    read_legacy_monitor_shadow_snapshots,
    read_legacy_surge_shadow_snapshot,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationVerifier,
    Ed25519CompletionAttestationKeyring,
    Ed25519ShadowReceiptKeyring,
    Ed25519ShadowReceiptSigner,
    ShadowInputSnapshotIdentity,
    ShadowReportReceiptSigner,
    ShadowReportReceiptVerifier,
    ShadowSessionEvidence,
    ShadowSessionReport,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    attach_shadow_report_receipt,
    build_shadow_session_report,
    load_shadow_session_report,
    publish_shadow_session_report,
    recover_shadow_session_publication,
    shadow_observation_set_id,
    shadow_session_boundaries,
    verify_completion_attestation,
    verify_shadow_report_receipt,
)

_REQUIRED_LEGACY_BINDINGS = frozenset({"n_shape", "growth_board_surge"})
ShadowSessionFaultHook = Callable[[str], None]


class ShadowInputUnavailableError(ValueError):
    """A captured external input failed binding or validation before comparison."""


@dataclass(frozen=True)
class ShadowReportRetryContext:
    report_root: Path
    trade_date: date
    requested_at: datetime
    effective_cutoff: datetime
    producer_commit: str
    producer_version: str
    existing_report: ShadowSessionReport | None


def _normalized_absolute(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.abspath(candidate))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError(f"{label} must be absolute and normalized")
    return candidate


def prepare_shadow_report_retry(
    *,
    report_root: Path,
    trade_date: date,
    requested_at: datetime,
    producer_commit: str,
    producer_version: str,
) -> ShadowReportRetryContext:
    """Bind retries to the first durable business cutoff for one session."""

    root = _normalized_absolute(report_root, label="shadow report root")
    requested = normalize_aware_utc(requested_at)
    existing_report: ShadowSessionReport | None = None
    session = root / trade_date.isoformat()
    if session.exists():
        public_reports = tuple(
            path
            for path in session.iterdir()
            if path.name.endswith(".json") and not path.name.startswith(".")
        )
        if public_reports:
            recover_shadow_session_publication(root, trade_date=trade_date)
            existing_report = load_published_shadow_reports(
                root,
                trade_dates=(trade_date,),
            )[0]
            evidence = existing_report.evidence
            if evidence is None:
                raise ValueError("existing production shadow report has no evidence")
            if (
                evidence.producer_commit != producer_commit
                or evidence.producer_version != producer_version
            ):
                raise ValueError("existing shadow report producer identity conflicts")
            if evidence.evaluation_cutoff > requested:
                raise ValueError("existing shadow report was unavailable at requested_at")
            effective_cutoff = evidence.evaluation_cutoff
        else:
            effective_cutoff = requested
    else:
        effective_cutoff = requested
    return ShadowReportRetryContext(
        report_root=root,
        trade_date=trade_date,
        requested_at=requested,
        effective_cutoff=effective_cutoff,
        producer_commit=producer_commit,
        producer_version=producer_version,
        existing_report=existing_report,
    )


def publish_shadow_report_retry(
    context: ShadowReportRetryContext,
    report: ShadowSessionReport,
    *,
    fault_hook: ShadowSessionFaultHook | None = None,
) -> tuple[ShadowSessionReport, Path]:
    """Publish once, or return the exact durable report for an immutable retry."""

    validated = ShadowSessionReport.model_validate(report)
    if validated.trade_date != context.trade_date:
        raise ValueError("shadow retry report trade date changed")
    if context.existing_report is not None:
        if validated != context.existing_report:
            raise ValueError("shadow session conflicts with an existing immutable report")
        path = context.report_root / context.trade_date.isoformat() / f"{validated.report_id}.json"
        loaded = load_shadow_session_report(path, expected_report_id=str(validated.report_id))
        if loaded != validated:
            raise ValueError("durable shadow report changed during retry")
        return loaded, path
    path = publish_shadow_session_report(
        context.report_root,
        validated,
        fault_hook=fault_hook,
    )
    return validated, path


def _assert_existing_report_matches_execution(
    report: ShadowSessionReport,
    *,
    calendar: MarketCalendarAuthority,
    bindings: tuple[ShadowStrategyBinding, ...],
    match_tolerance_microseconds: int,
    attestation_verifier: CompletionAttestationVerifier,
    receipt_verifier: ShadowReportReceiptVerifier | None,
    expected_evidence_origin: Literal["production", "test_fixture"],
) -> None:
    evidence = report.evidence
    if report.evidence_origin != expected_evidence_origin or evidence is None:
        raise ValueError("existing shadow report evidence origin conflicts")
    if evidence.calendar_authority_id != calendar.content_sha256:
        raise ValueError("existing shadow report calendar lineage conflicts")
    if report.match_tolerance_microseconds != match_tolerance_microseconds:
        raise ValueError("existing shadow report tolerance conflicts")
    observed_bindings = {snapshot.binding for snapshot in evidence.input_snapshots}
    if observed_bindings != set(bindings):
        raise ValueError("existing shadow report strategy bindings conflict")
    if expected_evidence_origin == "production":
        if not isinstance(attestation_verifier, Ed25519CompletionAttestationKeyring):
            raise ValueError("production completion verifier must be Ed25519")
        if any(
            snapshot.source == "isolated"
            and not verify_completion_attestation(
                snapshot.completion_receipt,
                attestation_verifier,
            )
            for snapshot in evidence.input_snapshots
        ):
            raise ValueError("existing shadow completion attestation is untrusted")
        if not isinstance(receipt_verifier, Ed25519ShadowReceiptKeyring):
            raise ValueError("production shadow report receipt verifier must be Ed25519")
        if not verify_shadow_report_receipt(report, receipt_verifier):
            raise ValueError("existing shadow report receipt is untrusted")


def _run_shadow_session(
    *,
    trade_date: date,
    observed_at: datetime,
    producer_commit: str,
    producer_version: str,
    calendar: MarketCalendarAuthority,
    monitor_rows: Iterable[Mapping[str, object]],
    monitor_completion_receipt: ShadowSourceCompletionReceipt,
    surge_events_path: Path,
    surge_completion_receipt: ShadowSourceCompletionReceipt,
    runner_sources: Iterable[tuple[ShadowStrategyBinding, ShadowRunnerSignalSource]],
    report_root: Path,
    match_tolerance_microseconds: int,
    attestation_verifier: CompletionAttestationVerifier,
    evidence_origin: Literal["production", "test_fixture"],
    report_receipt_signer: ShadowReportReceiptSigner | None = None,
    report_receipt_verifier: ShadowReportReceiptVerifier | None = None,
    report_producer_service_id: str | None = None,
    report_producer_instance_id: str | None = None,
    fault_hook: ShadowSessionFaultHook | None = None,
    batch_size: int = 1_000,
    max_records_per_source: int = 100_000,
    max_raw_bytes_per_source: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
) -> ShadowSessionReport:
    """Freeze all comparable sources before publishing one session report."""

    retry = prepare_shadow_report_retry(
        report_root=report_root,
        trade_date=trade_date,
        requested_at=observed_at,
        producer_commit=producer_commit,
        producer_version=producer_version,
    )
    observed = retry.effective_cutoff
    prepared_runner_sources = tuple(runner_sources)
    bindings = tuple(binding for binding, _source in prepared_runner_sources)
    if len(bindings) != len(set(bindings)):
        raise ValueError("shadow runner bindings must be unique")
    if {item.strategy_id for item in bindings} != _REQUIRED_LEGACY_BINDINGS:
        raise ValueError("shadow session requires both legacy-comparable strategy bindings")
    verified_calendar = MarketCalendarAuthority.model_validate(calendar)
    if verified_calendar.generated_at > observed:
        raise ValueError("shadow calendar was generated after observation")
    if trade_date not in verified_calendar.open_dates:
        raise ValueError("shadow trade date is not an authoritative open session")
    session_open, session_close = shadow_session_boundaries(trade_date)
    if observed < session_close:
        raise ValueError("shadow session cannot run before authoritative close")
    if retry.existing_report is not None:
        _assert_existing_report_matches_execution(
            retry.existing_report,
            calendar=verified_calendar,
            bindings=bindings,
            match_tolerance_microseconds=match_tolerance_microseconds,
            attestation_verifier=attestation_verifier,
            receipt_verifier=report_receipt_verifier,
            expected_evidence_origin=evidence_origin,
        )
        evidence = retry.existing_report.evidence
        assert evidence is not None
        persisted_legacy_receipts = {
            snapshot.completion_receipt.receipt_id
            for snapshot in evidence.input_snapshots
            if snapshot.source == "legacy"
        }
        if str(monitor_completion_receipt.receipt_id) not in persisted_legacy_receipts:
            raise ValueError("shadow session conflicts with an existing legacy monitor receipt")
        if str(surge_completion_receipt.receipt_id) not in persisted_legacy_receipts:
            raise ValueError("shadow session conflicts with an existing legacy surge receipt")
        recovered, _path = publish_shadow_report_retry(
            retry,
            retry.existing_report,
            fault_hook=fault_hook,
        )
        return recovered
    receipt_configured = (
        report_receipt_signer,
        report_producer_service_id,
        report_producer_instance_id,
    )
    if any(item is not None for item in receipt_configured) and not all(
        item is not None for item in receipt_configured
    ):
        raise ValueError("shadow report receipt configuration must be complete")
    if evidence_origin == "test_fixture" and any(item is not None for item in receipt_configured):
        raise ValueError("non-production shadow sessions cannot issue report receipts")

    monitor_bindings = tuple(item for item in bindings if item.strategy_id == "n_shape")
    try:
        legacy_by_binding = dict(
            read_legacy_monitor_shadow_snapshots(
                monitor_rows,
                trade_date=trade_date,
                exported_at=observed,
                producer_commit=producer_commit,
                bindings=monitor_bindings,
                completion_receipt=monitor_completion_receipt,
                max_records=max_records_per_source,
                max_raw_bytes=max_raw_bytes_per_source,
                max_record_bytes=max_record_bytes,
            )
        )
    except (OSError, ValueError) as exc:
        raise ShadowInputUnavailableError(
            f"legacy monitor source is unavailable or invalid: {exc}"
        ) from exc
    legacy = []
    for binding in bindings:
        if binding.strategy_id != "n_shape":
            try:
                legacy_by_binding[binding] = read_legacy_surge_shadow_snapshot(
                    _normalized_absolute(surge_events_path, label="legacy surge source"),
                    trade_date=trade_date,
                    exported_at=observed,
                    producer_commit=producer_commit,
                    binding=binding,
                    completion_receipt=surge_completion_receipt,
                    max_file_bytes=max_raw_bytes_per_source,
                    max_line_bytes=max_record_bytes,
                    max_records=max_records_per_source,
                )
            except (OSError, ValueError) as exc:
                raise ShadowInputUnavailableError(
                    f"legacy surge source is unavailable or invalid: {exc}"
                ) from exc
        legacy.extend(legacy_by_binding[binding].observations)
    isolated = []
    isolated_snapshots = {}
    for binding, source in prepared_runner_sources:
        try:
            frozen = read_isolated_runner_shadow_snapshot(
                source,
                trade_date=trade_date,
                observed_at=observed,
                binding=binding,
                expected_calendar_authority_id=str(verified_calendar.content_sha256),
                batch_size=batch_size,
                max_records=max_records_per_source,
                max_raw_bytes=max_raw_bytes_per_source,
                max_record_bytes=max_record_bytes,
                attestation_verifier=attestation_verifier,
            )
        except (OSError, ValueError) as exc:
            raise ShadowInputUnavailableError(
                f"isolated runner source is unavailable or invalid: {exc}"
            ) from exc
        isolated_snapshots[binding] = frozen
        isolated.extend(frozen.observations)
    input_snapshots = []
    for binding in bindings:
        legacy_snapshot = legacy_by_binding[binding]
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
        runner = isolated_snapshots[binding]
        input_snapshots.append(
            ShadowInputSnapshotIdentity(
                source="isolated",
                source_id=runner.source_id,
                binding=binding,
                raw_input_id=runner.raw_input_id,
                completion_receipt=runner.completion_receipt,
                upstream_snapshot_id=runner.upstream_snapshot_id,
                observation_set_id=shadow_observation_set_id(runner.observations),
                captured_at=runner.captured_at,
                complete_through=runner.complete_through,
                producer_commit=runner.completion_receipt.producer_commit,
                producer_version=runner.completion_receipt.producer_version,
            )
        )
    evidence = ShadowSessionEvidence(
        evidence_origin=evidence_origin,
        calendar_authority_id=str(verified_calendar.content_sha256),
        evaluation_cutoff=observed,
        session_open_at=session_open,
        session_close_at=session_close,
        producer_commit=producer_commit,
        producer_version=producer_version,
        input_snapshots=tuple(input_snapshots),
    )
    report = build_shadow_session_report(
        trade_date=trade_date,
        legacy=legacy,
        isolated=isolated,
        match_tolerance_microseconds=match_tolerance_microseconds,
        evidence=evidence,
        attestation_verifier=attestation_verifier,
    )
    if report_receipt_signer is not None:
        assert report_producer_service_id is not None
        assert report_producer_instance_id is not None
        report = attach_shadow_report_receipt(
            report,
            signer=report_receipt_signer,
            verifier=report_receipt_verifier,
            producer_service_id=report_producer_service_id,
            producer_instance_id=report_producer_instance_id,
        )
        if report_receipt_verifier is not None and not verify_shadow_report_receipt(
            report,
            report_receipt_verifier,
        ):
            raise ValueError("new shadow report receipt is untrusted")
    published, _path = publish_shadow_report_retry(
        retry,
        report,
        fault_hook=fault_hook,
    )
    if fault_hook is not None:
        fault_hook("after_publish")
    return published


def run_shadow_session(
    *,
    trade_date: date,
    observed_at: datetime,
    producer_commit: str,
    producer_version: str,
    calendar: MarketCalendarAuthority,
    monitor_rows: Iterable[Mapping[str, object]],
    monitor_completion_receipt: ShadowSourceCompletionReceipt,
    surge_events_path: Path,
    surge_completion_receipt: ShadowSourceCompletionReceipt,
    runner_sources: Iterable[tuple[ShadowStrategyBinding, ShadowRunnerSignalSource]],
    report_root: Path,
    match_tolerance_microseconds: int,
    attestation_verifier: CompletionAttestationVerifier,
    fault_hook: ShadowSessionFaultHook | None = None,
    batch_size: int = 1_000,
    max_records_per_source: int = 100_000,
    max_raw_bytes_per_source: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
) -> ShadowSessionReport:
    """Compatibility runner that can publish only non-production fixture evidence."""

    return _run_shadow_session(
        trade_date=trade_date,
        observed_at=observed_at,
        producer_commit=producer_commit,
        producer_version=producer_version,
        calendar=calendar,
        monitor_rows=monitor_rows,
        monitor_completion_receipt=monitor_completion_receipt,
        surge_events_path=surge_events_path,
        surge_completion_receipt=surge_completion_receipt,
        runner_sources=runner_sources,
        report_root=report_root,
        match_tolerance_microseconds=match_tolerance_microseconds,
        attestation_verifier=attestation_verifier,
        fault_hook=fault_hook,
        evidence_origin="test_fixture",
        batch_size=batch_size,
        max_records_per_source=max_records_per_source,
        max_raw_bytes_per_source=max_raw_bytes_per_source,
        max_record_bytes=max_record_bytes,
    )


def run_shadow_production_session(
    *,
    report_receipt_signer: ShadowReportReceiptSigner,
    report_receipt_verifier: ShadowReportReceiptVerifier,
    report_producer_service_id: str,
    report_producer_instance_id: str,
    **kwargs: object,
) -> ShadowSessionReport:
    """Production-only facade: a signed receipt is mandatory and verified before return.

    The later profile/service fan-in only needs to provide the opaque signer client,
    public keyring and producer identity; it never needs private-key or report-path access.
    """

    attestation_verifier = kwargs.get("attestation_verifier")
    if not isinstance(attestation_verifier, Ed25519CompletionAttestationKeyring):
        raise ValueError("production shadow completion verifier must be an Ed25519 public keyring")
    if not isinstance(report_receipt_verifier, Ed25519ShadowReceiptKeyring):
        raise ValueError("production shadow report verifier must be an Ed25519 public keyring")
    if not isinstance(report_receipt_signer, Ed25519ShadowReceiptSigner):
        raise ValueError("production shadow report signer must be an Ed25519 signer client")
    if report_receipt_signer.key_id != report_receipt_verifier.active_key_id:
        raise ValueError("production report signer must use the active key id")
    if not report_producer_service_id.strip() or not report_producer_instance_id.strip():
        raise ValueError("production shadow report producer identity must be complete")

    arguments = dict(kwargs)
    arguments.update(
        report_receipt_signer=report_receipt_signer,
        report_receipt_verifier=report_receipt_verifier,
        report_producer_service_id=report_producer_service_id,
        report_producer_instance_id=report_producer_instance_id,
        evidence_origin="production",
    )
    return _run_shadow_session(**arguments)  # type: ignore[arg-type]


def load_published_shadow_reports(
    root: Path,
    *,
    trade_dates: Sequence[date],
) -> tuple[ShadowSessionReport, ...]:
    """Load exactly one immutable report for every requested authoritative session."""

    report_root = _normalized_absolute(root, label="shadow report root")
    observed_root = report_root.lstat()
    if stat.S_ISLNK(observed_root.st_mode) or not stat.S_ISDIR(observed_root.st_mode):
        raise ValueError("shadow report root is unsafe")
    if tuple(sorted(set(trade_dates))) != tuple(trade_dates):
        raise ValueError("shadow report trade dates must be ordered and unique")
    reports: list[ShadowSessionReport] = []
    for trade_date in trade_dates:
        session = report_root / trade_date.isoformat()
        try:
            observed_session = session.lstat()
        except FileNotFoundError as exc:
            raise ValueError("shadow session must contain exactly one report") from exc
        if stat.S_ISLNK(observed_session.st_mode) or not stat.S_ISDIR(observed_session.st_mode):
            raise ValueError("shadow report session is unsafe")
        candidates = tuple(
            path
            for path in session.iterdir()
            if path.name.endswith(".json") and not path.name.startswith(".")
        )
        if len(candidates) != 1:
            raise ValueError("shadow session must contain exactly one report")
        candidate = candidates[0]
        expected_report_id = candidate.name.removesuffix(".json")
        reports.append(
            load_shadow_session_report(
                candidate,
                expected_report_id=expected_report_id,
            )
        )
    return tuple(reports)


__all__ = [
    "ShadowInputUnavailableError",
    "ShadowReportRetryContext",
    "load_published_shadow_reports",
    "prepare_shadow_report_retry",
    "publish_shadow_report_retry",
    "run_shadow_production_session",
    "run_shadow_session",
]
