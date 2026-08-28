from __future__ import annotations

import base64
import json
import multiprocessing
import os
import shutil
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_job import (
    load_published_shadow_reports,
    run_shadow_production_session,
    run_shadow_session,
)
from rquant.runtime_shadow_sources import (
    legacy_records_raw_input_id,
    legacy_surge_file_raw_input_id,
    runner_source_raw_input_id,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    CompletionAttestationSigner,
    Ed25519ShadowReceiptKeyring,
    Ed25519ShadowReceiptSigner,
    HmacCompletionAttestationAuthority,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    shadow_completion_receipt_body_sha256,
    shadow_session_boundaries,
    verify_shadow_report_receipt,
)
from rquant.signal_bus import RouteSourceDescriptor
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import RunnerSignalBatch, SourceSnapshot
from rquant.strategy_runner import RunnerSignalRecord
from tests.shadow_ed25519_support import create_rotating_shadow_ed25519_test_authority

TRADE_DATE = date(2026, 7, 31)
COMMIT = "a" * 40
EXPORTED_AT = datetime(2026, 7, 31, 7, 10, tzinfo=UTC)
ATTESTATION_AUTHORITY = HmacCompletionAttestationAuthority(
    key_id="shadow-job-test",
    secret=b"shadow-job-test-attestation-secret",
)


class _OpenSslSigningClient:
    """Test fixture for an external signing client; Shadow receives no key path."""

    def __init__(self, private_key_path: Path) -> None:
        self._private_key_path = private_key_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        payload_path = self._private_key_path.parent / f"{namespace}.payload"
        signature_path = self._private_key_path.parent / f"{namespace}.signature"
        payload_path.write_bytes(payload)
        completed = subprocess.run(
            (
                _openssl(),
                "pkeyutl",
                "-sign",
                "-inkey",
                str(self._private_key_path),
                "-rawin",
                "-in",
                str(payload_path),
                "-out",
                str(signature_path),
            ),
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
        return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _openssl() -> str:
    binary = shutil.which("openssl")
    if binary is None:
        pytest.skip("openssl is required for Shadow Ed25519 process tests")
    return binary


def _receipt_signer_and_keyring(
    root: Path,
) -> tuple[Ed25519ShadowReceiptSigner, Ed25519ShadowReceiptKeyring, Path, bytes]:
    private_key = root / "shadow-report.private.pem"
    public_key = root / "shadow-report.public.pem"
    generated = subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    if generated.returncode != 0:
        raise RuntimeError(generated.stderr.decode("utf-8", errors="replace"))
    exported = subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    if exported.returncode != 0:
        raise RuntimeError(exported.stderr.decode("utf-8", errors="replace"))
    private_key.chmod(0o600)
    public = public_key.read_bytes()
    signer = Ed25519ShadowReceiptSigner(
        key_id="shadow-report-v1",
        client=_OpenSslSigningClient(private_key),
    )
    keyring = Ed25519ShadowReceiptKeyring(
        active_key_id="shadow-report-v1",
        active_public_key=public,
    )
    return signer, keyring, private_key, public


class _PoisonRows:
    def __iter__(self):
        raise AssertionError("a durable shadow receipt must prevent source regeneration on restart")


def _calendar(*, open_dates: tuple[date, ...] = (TRADE_DATE,)) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=open_dates,
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )


def _binding(strategy_id: str) -> ShadowStrategyBinding:
    return ShadowStrategyBinding(
        strategy_id=strategy_id,
        strategy_version=1,
        definition_fingerprint="1" * 64,
        executable_fingerprint="2" * 64,
    )


def _signal(
    *,
    strategy_id: str,
    code: str,
    action: SignalAction,
    minute: int,
) -> SignalEnvelope:
    event_time = datetime(2026, 7, 31, 1, minute, tzinfo=UTC)
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
        reason_codes=("shadow",),
        evidence={},
        expires_at=event_time + timedelta(minutes=5),
        producer_commit=COMMIT,
    )


class _Source:
    def __init__(
        self,
        source_id: str,
        signals: tuple[SignalEnvelope, ...],
        *,
        attestation_signer: CompletionAttestationSigner = ATTESTATION_AUTHORITY,
        producer_version: str = "test-production-1",
    ) -> None:
        self.source_id = source_id
        self._attestation_signer = attestation_signer
        self._producer_version = producer_version
        self.records = tuple(
            RunnerSignalRecord(sequence=index, signal=signal)
            for index, signal in enumerate(signals, start=1)
        )

    def _descriptor(self) -> RouteSourceDescriptor:
        return RouteSourceDescriptor(
            source_id=self.source_id,
            generation_id="e" * 64,
            strategy_spec_fingerprint="f" * 64,
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
        assert trade_date == TRADE_DATE
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(descriptor=self._descriptor()),
            after_sequence=after_sequence,
            limit=limit,
            records=tuple(record for record in self.records if record.sequence > after_sequence)[
                :limit
            ],
        )

    def read_completion_receipt(self, *, trade_date: date) -> ShadowSourceCompletionReceipt:
        return _completion_receipt(
            source="isolated",
            source_id=self.source_id,
            trade_date=trade_date,
            input_identity=runner_source_raw_input_id(self._descriptor(), self.records),
            high_watermark=len(self.records),
            attestation_signer=self._attestation_signer,
            producer_version=self._producer_version,
        )


def _monitor_rows() -> tuple[dict[str, object], ...]:
    return (
        {
            "trade_date": TRADE_DATE,
            "ts_code": "600001.SH",
            "level": "attack_strong_carry",
            "trigger_time": datetime(2026, 7, 31, 9, 31),
            "trigger_price": 10.2,
            "level_price": 10.0,
            "trigger_type": "strong_carry",
            "pool": "pool1",
        },
        {
            "trade_date": TRADE_DATE,
            "ts_code": "600001.SH",
            "level": "attack_break_high",
            "trigger_time": datetime(2026, 7, 31, 9, 33),
            "trigger_price": 10.5,
            "level_price": 10.4,
            "trigger_type": "break_t_high",
            "pool": "pool1",
        },
    )


def _surge_file(tmp_path: Path) -> Path:
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(
            {
                "ts_code": "300001.SZ",
                "name": "growth",
                "confirmed_at": "09:35",
                "status": "confirmed",
                "rel_cum": 2.8,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _completion_receipt(
    *,
    source: str,
    source_id: str,
    trade_date: date,
    input_identity: str,
    complete_through: datetime | None = None,
    producer_version: str = "test-production-1",
    high_watermark: int = 0,
    attestation_signer: CompletionAttestationSigner = ATTESTATION_AUTHORITY,
) -> ShadowSourceCompletionReceipt:
    _session_open, session_close = shadow_session_boundaries(trade_date)
    authority = (
        {
            "producer_service_id": "strategy-live",
            "producer_instance_id": "test-primary",
            "runner_generation_id": "e" * 64,
            "signal_authority_generation_id": "a" * 64,
            "calendar_generation_id": str(_calendar().content_sha256),
            "last_sequence": 0,
            "high_watermark": high_watermark,
            "route_receipts_id": "c" * 64,
            "feature_source_generation_id": "6" * 64,
            "feature_close_marker_id": "7" * 64,
            "feature_segment_chain_hash": "8" * 64,
            "segment_start_sequence": 0,
            "segment_record_count": high_watermark,
            "segment_chain_hash": "9" * 64,
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
        complete_through=complete_through or session_close,
        input_identity=input_identity,
        produced_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version=producer_version,
        **authority,
    )
    if source != "isolated":
        return receipt
    strategy_id = "growth_board_surge" if source_id == "growth-v1" else "n_shape"
    binding = _binding(strategy_id)
    claims = CompletionAttestationClaims(
        completion_receipt_body_sha256=shadow_completion_receipt_body_sha256(receipt),
        trade_date=trade_date,
        session_close_at=session_close,
        source_id=source_id,
        input_identity=input_identity,
        strategy_id=binding.strategy_id,
        strategy_version=binding.strategy_version,
        strategy_registration_fingerprint=binding.definition_fingerprint,
        strategy_spec_fingerprint="f" * 64,
        executable_fingerprint=binding.executable_fingerprint,
        candidate_schema_fingerprint="3" * 64,
        feature_registration_fingerprint="4" * 64,
        feature_contract_fingerprint="5" * 64,
        routing_policy_fingerprint="6" * 64,
        producer_manifest_fingerprint="7" * 64,
        producer_commit=COMMIT,
        producer_version=producer_version,
        producer_service_id="strategy-live",
        producer_instance_id="test-primary",
        calendar_generation_id=str(_calendar().content_sha256),
        feature_source_generation_id="6" * 64,
        feature_close_marker_id="7" * 64,
        feature_segment_chain_hash="8" * 64,
        runner_generation_id="e" * 64,
        runner_segment_start_sequence=0,
        runner_segment_final_sequence=high_watermark,
        runner_segment_record_count=high_watermark,
        runner_segment_chain_hash="9" * 64,
        signal_authority_generation_id="a" * 64,
        route_receipts_id="c" * 64,
    )
    return ShadowSourceCompletionReceipt.model_validate(
        {
            **receipt.model_dump(mode="python", exclude={"receipt_id"}),
            "completion_attestation": attestation_signer.issue(claims),
        }
    )


def _monitor_receipt() -> ShadowSourceCompletionReceipt:
    return _completion_receipt(
        source="legacy",
        source_id="legacy-monitor-events",
        trade_date=TRADE_DATE,
        input_identity=legacy_records_raw_input_id(
            _monitor_rows(),
            source_id="legacy-monitor-events",
            trade_date=TRADE_DATE,
        ),
    )


def _surge_receipt(path: Path) -> ShadowSourceCompletionReceipt:
    return _completion_receipt(
        source="legacy",
        source_id="legacy-surge-jsonl",
        trade_date=TRADE_DATE,
        input_identity=legacy_surge_file_raw_input_id(path, trade_date=TRADE_DATE),
    )


def _sources(
    *,
    attestation_signer: CompletionAttestationSigner = ATTESTATION_AUTHORITY,
    producer_version: str = "test-production-1",
) -> tuple[tuple[ShadowStrategyBinding, _Source], ...]:
    return (
        (
            _binding("n_shape"),
            _Source(
                "n-shape-v1",
                (
                    _signal(
                        strategy_id="n_shape",
                        code="600001.SH",
                        action=SignalAction.WATCH,
                        minute=31,
                    ),
                    _signal(
                        strategy_id="n_shape",
                        code="600001.SH",
                        action=SignalAction.B_INTENT,
                        minute=33,
                    ),
                ),
                attestation_signer=attestation_signer,
                producer_version=producer_version,
            ),
        ),
        (
            _binding("growth_board_surge"),
            _Source(
                "growth-v1",
                (
                    _signal(
                        strategy_id="growth_board_surge",
                        code="300001.SZ",
                        action=SignalAction.B_INTENT,
                        minute=35,
                    ),
                ),
                attestation_signer=attestation_signer,
                producer_version=producer_version,
            ),
        ),
    )


def test_shadow_session_accepts_attested_runner_version_independent_of_report_service(
    tmp_path: Path,
) -> None:
    surge_path = _surge_file(tmp_path)
    report = run_shadow_session(
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version="shadow-report-service-v9",
        calendar=_calendar(),
        monitor_rows=_monitor_rows(),
        monitor_completion_receipt=_monitor_receipt(),
        surge_events_path=surge_path,
        surge_completion_receipt=_surge_receipt(surge_path),
        runner_sources=_sources(producer_version="strategy-runner-v27"),
        report_root=tmp_path / "reports",
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )

    runner_snapshots = tuple(
        item for item in report.evidence.input_snapshots if item.source == "isolated"
    )
    assert {item.producer_version for item in runner_snapshots} == {"strategy-runner-v27"}
    assert report.evidence.producer_version == "shadow-report-service-v9"


def test_shadow_session_freezes_all_sources_and_publishes_one_loadable_report(
    tmp_path: Path,
) -> None:
    report_root = tmp_path / "reports"

    first = run_shadow_session(
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        calendar=_calendar(),
        monitor_rows=_monitor_rows(),
        monitor_completion_receipt=_monitor_receipt(),
        surge_events_path=_surge_file(tmp_path),
        surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
        runner_sources=_sources(),
        report_root=report_root,
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )
    second = run_shadow_session(
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT + timedelta(minutes=5),
        producer_commit=COMMIT,
        producer_version="test-production-1",
        calendar=_calendar(),
        monitor_rows=_monitor_rows(),
        monitor_completion_receipt=_monitor_receipt(),
        surge_events_path=_surge_file(tmp_path),
        surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
        runner_sources=_sources(),
        report_root=report_root,
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )

    assert first == second
    assert first.evidence_origin == "test_fixture"
    assert first.evidence is not None
    assert second.evidence is not None
    assert second.evidence.evaluation_cutoff == first.evidence.evaluation_cutoff
    assert first.legacy_count == 3
    assert first.isolated_count == 3
    assert first.matched_count == 3
    loaded = load_published_shadow_reports(report_root, trade_dates=(TRADE_DATE,))
    assert len(loaded) == 1
    assert loaded[0].report_id == first.report_id


def test_shadow_session_freezes_one_shot_runner_sources(tmp_path: Path) -> None:
    result = run_shadow_session(
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        calendar=_calendar(),
        monitor_rows=_monitor_rows(),
        monitor_completion_receipt=_monitor_receipt(),
        surge_events_path=_surge_file(tmp_path),
        surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
        runner_sources=(item for item in _sources()),
        report_root=tmp_path / "reports",
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )

    assert result.isolated_count == 3


def test_shadow_session_rejects_legacy_receipt_with_only_early_session_coverage(
    tmp_path: Path,
) -> None:
    raw_input_id = legacy_records_raw_input_id(
        _monitor_rows(),
        source_id="legacy-monitor-events",
        trade_date=TRADE_DATE,
    )

    with pytest.raises(ValueError, match="complete session"):
        run_shadow_session(
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            producer_commit=COMMIT,
            producer_version="test-production-1",
            calendar=_calendar(),
            monitor_rows=_monitor_rows(),
            monitor_completion_receipt=_completion_receipt(
                source="legacy",
                source_id="legacy-monitor-events",
                trade_date=TRADE_DATE,
                input_identity=raw_input_id,
                complete_through=datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
            ),
            surge_events_path=_surge_file(tmp_path),
            surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
            runner_sources=_sources(),
            report_root=tmp_path / "reports",
            match_tolerance_microseconds=60_000_000,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def test_shadow_session_rejects_changed_completion_receipt_as_same_day_conflict(
    tmp_path: Path,
) -> None:
    report_root = tmp_path / "reports"
    surge_path = _surge_file(tmp_path)
    common = {
        "trade_date": TRADE_DATE,
        "observed_at": EXPORTED_AT,
        "producer_commit": COMMIT,
        "producer_version": "test-production-1",
        "calendar": _calendar(),
        "monitor_rows": _monitor_rows(),
        "surge_events_path": surge_path,
        "surge_completion_receipt": _surge_receipt(surge_path),
        "runner_sources": _sources(),
        "report_root": report_root,
        "match_tolerance_microseconds": 60_000_000,
        "attestation_verifier": ATTESTATION_AUTHORITY,
    }
    run_shadow_session(
        **common,
        monitor_completion_receipt=_monitor_receipt(),
    )
    changed = _completion_receipt(
        source="legacy",
        source_id="legacy-monitor-events",
        trade_date=TRADE_DATE,
        input_identity=legacy_records_raw_input_id(
            _monitor_rows(),
            source_id="legacy-monitor-events",
            trade_date=TRADE_DATE,
        ),
        producer_version="test-production-2",
    )

    with pytest.raises(ValueError, match="session.*conflict|conflict.*session"):
        run_shadow_session(
            **common,
            monitor_completion_receipt=changed,
        )


def test_shadow_session_fails_closed_when_a_required_legacy_source_is_missing(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="surge source"):
        run_shadow_session(
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            producer_commit=COMMIT,
            producer_version="test-production-1",
            calendar=_calendar(),
            monitor_rows=_monitor_rows(),
            monitor_completion_receipt=_monitor_receipt(),
            surge_events_path=tmp_path / "missing.jsonl",
            surge_completion_receipt=_completion_receipt(
                source="legacy",
                source_id="legacy-surge-jsonl",
                trade_date=TRADE_DATE,
                input_identity="9" * 64,
            ),
            runner_sources=_sources(),
            report_root=tmp_path / "reports",
            match_tolerance_microseconds=60_000_000,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )
    assert not (tmp_path / "reports").exists()


def test_shadow_report_loader_rejects_ambiguous_reports_for_one_session(
    tmp_path: Path,
) -> None:
    root = tmp_path / "reports"
    result = run_shadow_session(
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        calendar=_calendar(),
        monitor_rows=_monitor_rows(),
        monitor_completion_receipt=_monitor_receipt(),
        surge_events_path=_surge_file(tmp_path),
        surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
        runner_sources=_sources(),
        report_root=root,
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )
    session = root / TRADE_DATE.isoformat()
    (session / f"{'0' * 64}.json").write_bytes((session / f"{result.report_id}.json").read_bytes())

    with pytest.raises(ValueError, match="exactly one"):
        load_published_shadow_reports(root, trade_dates=(TRADE_DATE,))


def test_shadow_session_rejects_intraday_and_incomplete_tail_evidence(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="before authoritative close"):
        run_shadow_session(
            trade_date=TRADE_DATE,
            observed_at=datetime(2026, 7, 31, 6, 59, 59, tzinfo=UTC),
            producer_commit=COMMIT,
            producer_version="test-production-1",
            calendar=_calendar(),
            monitor_rows=_monitor_rows(),
            monitor_completion_receipt=_monitor_receipt(),
            surge_events_path=_surge_file(tmp_path),
            surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
            runner_sources=_sources(),
            report_root=tmp_path / "reports",
            match_tolerance_microseconds=60_000_000,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def test_shadow_session_rejects_closed_calendar_day(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="authoritative open session"):
        run_shadow_session(
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            producer_commit=COMMIT,
            producer_version="test-production-1",
            calendar=_calendar(open_dates=(date(2026, 7, 30),)),
            monitor_rows=_monitor_rows(),
            monitor_completion_receipt=_monitor_receipt(),
            surge_events_path=_surge_file(tmp_path),
            surge_completion_receipt=_surge_receipt(_surge_file(tmp_path)),
            runner_sources=_sources(),
            report_root=tmp_path / "reports",
            match_tolerance_microseconds=60_000_000,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def _run_production_shadow_process(
    root: str,
    private_key: str,
    public_key: bytes,
    *,
    crash_stage: str | None,
    poison_inputs: bool,
) -> None:
    work_root = Path(root)
    signer = Ed25519ShadowReceiptSigner(
        key_id="shadow-report-v1",
        client=_OpenSslSigningClient(Path(private_key)),
    )
    keyring = Ed25519ShadowReceiptKeyring(
        active_key_id="shadow-report-v1",
        active_public_key=public_key,
    )
    surge_path = work_root / "events.jsonl"

    def fault_hook(stage: str) -> None:
        if crash_stage == stage:
            (work_root / "crash.marker").write_text(stage, encoding="ascii")
            os._exit(73)

    report = run_shadow_production_session(
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version="test-production-1",
        calendar=_calendar(),
        monitor_rows=(_PoisonRows() if poison_inputs else _monitor_rows()),
        monitor_completion_receipt=_monitor_receipt(),
        surge_events_path=surge_path,
        surge_completion_receipt=_surge_receipt(surge_path),
        runner_sources=_sources(attestation_signer=signer),
        report_root=(work_root / "reports").resolve(),
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=keyring,
        report_receipt_signer=signer,
        report_receipt_verifier=keyring,
        report_producer_service_id="shadow-daily",
        report_producer_instance_id="shadow-process-test",
        fault_hook=fault_hook,
    )
    (work_root / "result.txt").write_text(str(report.report_id), encoding="ascii")


@pytest.mark.parametrize(
    ("crash_stage", "poison_after_restart"),
    (
        ("after_intent_write", False),
        ("after_temporary_write", False),
        ("after_link", True),
        ("after_receipt_write", True),
    ),
)
def test_production_shadow_process_recovers_each_publication_stage_exactly_once(
    tmp_path: Path,
    crash_stage: str,
    poison_after_restart: bool,
) -> None:
    _signer, keyring, private_key, public_key = _receipt_signer_and_keyring(tmp_path)
    _surge_file(tmp_path)
    context = multiprocessing.get_context("spawn")
    first = context.Process(
        target=_run_production_shadow_process,
        args=(str(tmp_path), str(private_key), public_key),
        kwargs={"crash_stage": crash_stage, "poison_inputs": False},
    )
    first.start()
    first.join(timeout=20)
    assert first.exitcode == 73
    assert (tmp_path / "crash.marker").read_text(encoding="ascii") == crash_stage

    restarted = context.Process(
        target=_run_production_shadow_process,
        args=(str(tmp_path), str(private_key), public_key),
        kwargs={"crash_stage": None, "poison_inputs": poison_after_restart},
    )
    restarted.start()
    restarted.join(timeout=20)
    assert restarted.exitcode == 0
    report_id = (tmp_path / "result.txt").read_text(encoding="ascii")
    report_path = tmp_path / "reports" / TRADE_DATE.isoformat() / f"{report_id}.json"
    loaded = load_published_shadow_reports(
        (tmp_path / "reports").resolve(),
        trade_dates=(TRADE_DATE,),
    )[0]
    assert report_path.is_file()
    session_files = tuple(report_path.parent.iterdir())
    assert {path for path in session_files if path.name.endswith(".json")} == {
        report_path.parent / ".session-report-claim.json",
        report_path,
    }
    assert not tuple(path for path in session_files if path.name.endswith(".tmp"))
    assert not tuple(path for path in session_files if ".intent." in path.name)
    assert loaded.report_receipt is not None
    assert verify_shadow_report_receipt(loaded, keyring)
    assert loaded.report_receipt.claims.trade_date == TRADE_DATE
    assert loaded.report_receipt.claims.producer_service_id == "shadow-daily"
    assert loaded.report_receipt.claims.producer_instance_id == "shadow-process-test"
    assert loaded.report_receipt.claims.code_commit == COMMIT
    assert loaded.report_receipt.claims.producer_commit == COMMIT
    assert loaded.report_receipt.claims.input_hash == loaded.evidence.evidence_id
    assert len(loaded.report_receipt.claims.source_generation_id) == 64
    tampered = loaded.model_copy(
        update={
            "report_receipt": loaded.report_receipt.model_copy(
                update={"signature": base64.b64encode(b"x" * 64).decode("ascii")}
            )
        }
    )
    assert not verify_shadow_report_receipt(tampered, keyring)


def test_production_shadow_facade_rejects_test_hmac_completion_authority(
    tmp_path: Path,
) -> None:
    signer, keyring, _private_key, _public_key = _receipt_signer_and_keyring(tmp_path)
    with pytest.raises(ValueError, match="completion verifier must be an Ed25519"):
        run_shadow_production_session(
            report_receipt_signer=signer,
            report_receipt_verifier=keyring,
            report_producer_service_id="shadow-daily",
            report_producer_instance_id="test",
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def test_production_shadow_facade_rejects_incomplete_producer_identity(
    tmp_path: Path,
) -> None:
    signer, keyring, _private_key, _public_key = _receipt_signer_and_keyring(tmp_path)

    with pytest.raises(ValueError, match="identity must be complete"):
        run_shadow_production_session(
            report_receipt_signer=signer,
            report_receipt_verifier=keyring,
            report_producer_service_id="shadow-daily",
            report_producer_instance_id="   ",
            attestation_verifier=keyring,
        )


def test_new_production_session_rejects_previous_report_signing_key(
    tmp_path: Path,
) -> None:
    authority = create_rotating_shadow_ed25519_test_authority(tmp_path / "rotating-report")
    surge_path = _surge_file(tmp_path)

    with pytest.raises(ValueError, match="report signer.*active"):
        run_shadow_production_session(
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            producer_commit=COMMIT,
            producer_version="test-production-1",
            calendar=_calendar(),
            monitor_rows=_monitor_rows(),
            monitor_completion_receipt=_monitor_receipt(),
            surge_events_path=surge_path,
            surge_completion_receipt=_surge_receipt(surge_path),
            runner_sources=_sources(attestation_signer=authority.active_signer),
            report_root=(tmp_path / "previous-report-key").resolve(),
            match_tolerance_microseconds=60_000_000,
            attestation_verifier=authority.keyring,
            report_receipt_signer=authority.previous_signer,
            report_receipt_verifier=authority.keyring,
            report_producer_service_id="shadow-daily",
            report_producer_instance_id="rotation-test",
        )


def test_new_production_session_rejects_previous_completion_signing_key(
    tmp_path: Path,
) -> None:
    authority = create_rotating_shadow_ed25519_test_authority(tmp_path / "rotating-completion")
    surge_path = _surge_file(tmp_path)

    with pytest.raises(ValueError, match="completion.*active"):
        run_shadow_production_session(
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            producer_commit=COMMIT,
            producer_version="test-production-1",
            calendar=_calendar(),
            monitor_rows=_monitor_rows(),
            monitor_completion_receipt=_monitor_receipt(),
            surge_events_path=surge_path,
            surge_completion_receipt=_surge_receipt(surge_path),
            runner_sources=_sources(attestation_signer=authority.previous_signer),
            report_root=(tmp_path / "previous-completion-key").resolve(),
            match_tolerance_microseconds=60_000_000,
            attestation_verifier=authority.keyring,
            report_receipt_signer=authority.active_signer,
            report_receipt_verifier=authority.keyring,
            report_producer_service_id="shadow-daily",
            report_producer_instance_id="rotation-test",
        )


def test_restart_accepts_previous_keys_only_for_existing_immutable_report(
    tmp_path: Path,
) -> None:
    authority = create_rotating_shadow_ed25519_test_authority(tmp_path / "rotating-history")
    surge_path = _surge_file(tmp_path)
    report_root = (tmp_path / "historical-report").resolve()
    common = {
        "trade_date": TRADE_DATE,
        "observed_at": EXPORTED_AT,
        "producer_commit": COMMIT,
        "producer_version": "test-production-1",
        "calendar": _calendar(),
        "monitor_completion_receipt": _monitor_receipt(),
        "surge_events_path": surge_path,
        "surge_completion_receipt": _surge_receipt(surge_path),
        "report_root": report_root,
        "match_tolerance_microseconds": 60_000_000,
        "report_producer_service_id": "shadow-daily",
        "report_producer_instance_id": "rotation-test",
    }
    historical = run_shadow_production_session(
        **common,
        monitor_rows=_monitor_rows(),
        runner_sources=_sources(attestation_signer=authority.previous_signer),
        attestation_verifier=authority.previous_keyring,
        report_receipt_signer=authority.previous_signer,
        report_receipt_verifier=authority.previous_keyring,
    )

    restarted = run_shadow_production_session(
        **common,
        monitor_rows=_PoisonRows(),
        runner_sources=_sources(attestation_signer=authority.previous_signer),
        attestation_verifier=authority.keyring,
        report_receipt_signer=authority.active_signer,
        report_receipt_verifier=authority.keyring,
    )

    assert restarted == historical
    assert restarted.report_receipt is not None
    assert restarted.report_receipt.key_id == authority.previous_signer.key_id
    assert restarted.evidence is not None
    assert {
        snapshot.completion_receipt.completion_attestation.key_id
        for snapshot in restarted.evidence.input_snapshots
        if snapshot.source == "isolated"
        and snapshot.completion_receipt.completion_attestation is not None
    } == {authority.previous_signer.key_id}
