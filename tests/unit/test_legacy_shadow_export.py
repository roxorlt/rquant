from __future__ import annotations

import dataclasses
import hashlib
import hmac
import inspect
import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import rquant.legacy_shadow_export as legacy_shadow_export_module
from rquant.legacy_shadow_export import (
    LEGACY_SHADOW_FILESYSTEM_CONTRACT,
    HmacLegacyShadowRecoveryAuthority,
    LegacyShadowExportConflictError,
    LegacyShadowExportError,
    LegacyShadowExportManifest,
    LegacyShadowExportUnavailableError,
    LegacyShadowFilesystemPolicy,
    LegacyShadowFinalizationClaims,
    LegacyShadowFinalizationReceipt,
    LegacyShadowRecoveryMarkerClaims,
    LegacyShadowRunnerManifestBinding,
    LegacyShadowTestDependencies,
    LegacySurgeCollectionProof,
    fan_in_production_isolated_runner_exports,
    legacy_shadow_test_filesystem_policy,
    prepare_legacy_monitor_spool,
    publish_isolated_runner_export,
    publish_isolated_runner_production_exports,
    publish_legacy_monitor_export,
    publish_legacy_monitor_production_export,
    publish_legacy_surge_export,
    publish_legacy_surge_production_export,
    recover_legacy_shadow_export,
    recover_production_legacy_shadow_exports,
    validate_legacy_shadow_filesystem_contract,
)
from rquant.legacy_shadow_export import (
    load_accepted_legacy_shadow_export as _load_accepted_legacy_shadow_export,
)
from rquant.runtime_builder_shadow import (
    FilesystemShadowSessionInputLoader,
    ShadowSessionSettings,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_shadow_sources import runner_source_raw_input_id
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    HmacCompletionAttestationAuthority,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    shadow_completion_receipt_body_sha256,
    shadow_session_boundaries,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import RouteSourceDescriptor, RunnerSignalBatch, SourceSnapshot
from rquant.strategy_runner import RunnerSignalRecord
from rquant.strict_json import canonical_model_json_bytes

TRADE_DATE = date(2026, 8, 3)
COMMIT = "a" * 40
_RECOVERY_SECRET = b"legacy-shadow-recovery-test-secret"
_ATTESTATION_AUTHORITY = HmacCompletionAttestationAuthority(
    key_id="legacy-shadow-export-test",
    secret=b"legacy-shadow-export-test-secret",
)


def _dependencies(observed_at: datetime) -> LegacyShadowTestDependencies:
    monotonic_value = int(
        (observed_at - datetime(2026, 8, 3, tzinfo=UTC)).total_seconds() * 1_000_000_000
    )

    def wall_clock() -> datetime:
        return observed_at

    def monotonic_ns() -> int:
        return monotonic_value

    def boot_id() -> str:
        return "00000000-0000-4000-8000-000000000001"

    authority = HmacLegacyShadowRecoveryAuthority(
        key_id="legacy-shadow-recovery-test",
        secret=_RECOVERY_SECRET,
        wall_clock=wall_clock,
        monotonic_ns=monotonic_ns,
        boot_id=boot_id,
    )
    return LegacyShadowTestDependencies(
        wall_clock=wall_clock,
        monotonic_ns=monotonic_ns,
        boot_id=boot_id,
        recovery_signer=authority,
        recovery_verifier=authority,
        filesystem_policy=legacy_shadow_test_filesystem_policy(),
    )


def _sequenced_dependencies(
    *observations: tuple[datetime, int],
) -> LegacyShadowTestDependencies:
    wallclock_values = iter(item[0] for item in observations)
    monotonic_values = iter(item[1] for item in observations)
    last_wallclock = observations[-1][0]
    last_monotonic = observations[-1][1]

    def authority_wallclock() -> datetime:
        return next(wallclock_values, last_wallclock)

    def authority_monotonic() -> int:
        return next(monotonic_values, last_monotonic)

    def boot_id() -> str:
        return "00000000-0000-4000-8000-000000000001"

    authority = HmacLegacyShadowRecoveryAuthority(
        key_id="legacy-shadow-recovery-test",
        secret=_RECOVERY_SECRET,
        wall_clock=authority_wallclock,
        monotonic_ns=authority_monotonic,
        boot_id=boot_id,
    )
    return LegacyShadowTestDependencies(
        wall_clock=lambda: observations[0][0],
        monotonic_ns=lambda: observations[0][1],
        boot_id=boot_id,
        recovery_signer=authority,
        recovery_verifier=authority,
        filesystem_policy=legacy_shadow_test_filesystem_policy(),
    )


def load_accepted_legacy_shadow_export(
    *,
    root: Path,
    trade_date: date,
    expected_source_id: str | None,
    expected_commit: str,
):
    dependencies = _dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC))
    return _load_accepted_legacy_shadow_export(
        root=root,
        trade_date=trade_date,
        expected_source_id=expected_source_id,
        expected_commit=expected_commit,
        recovery_verifier=dependencies.recovery_verifier,
        filesystem_policy=dependencies.filesystem_policy,
    )


def test_production_publishers_do_not_accept_caller_export_time() -> None:
    for publisher in (
        publish_legacy_monitor_production_export,
        publish_legacy_surge_production_export,
        publish_isolated_runner_production_exports,
        fan_in_production_isolated_runner_exports,
    ):
        assert "exported_at" not in inspect.signature(publisher).parameters
    recovery_parameters = inspect.signature(recover_production_legacy_shadow_exports).parameters
    assert "rows" not in recovery_parameters
    assert "events_path" not in recovery_parameters
    assert "exported_at" not in recovery_parameters


def test_recovery_only_rejects_clock_rollback_without_source_input(tmp_path: Path) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    created_at = datetime(2026, 8, 3, 7, 3, tzinfo=UTC)
    dependencies = _dependencies(created_at)
    with pytest.raises(RuntimeError, match="before publish"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=_monitor_rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=dependencies,
            fault_hook=lambda point: (
                (_ for _ in ()).throw(RuntimeError("before publish"))
                if point == "before_publish"
                else None
            ),
        )

    with pytest.raises(LegacyShadowExportError, match="rollback"):
        recover_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
            dependencies=LegacyShadowTestDependencies(
                wall_clock=lambda: created_at - timedelta(seconds=1),
                monotonic_ns=lambda: 501_000_000_000,
                boot_id=dependencies.boot_id,
                recovery_signer=dependencies.recovery_signer,
                recovery_verifier=dependencies.recovery_verifier,
                filesystem_policy=dependencies.filesystem_policy,
            ),
        )


def test_recovery_only_rejects_staging_from_another_boot(tmp_path: Path) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    created_at = datetime(2026, 8, 3, 7, 3, tzinfo=UTC)
    dependencies = _dependencies(created_at)
    with pytest.raises(RuntimeError, match="before publish"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=_monitor_rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=dependencies,
            fault_hook=lambda point: (
                (_ for _ in ()).throw(RuntimeError("before publish"))
                if point == "before_publish"
                else None
            ),
        )

    with pytest.raises(LegacyShadowExportError, match="boot"):
        recover_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
            dependencies=LegacyShadowTestDependencies(
                wall_clock=lambda: created_at + timedelta(minutes=10),
                monotonic_ns=lambda: 900_000_000_000,
                boot_id=lambda: "00000000-0000-4000-8000-000000000002",
                recovery_signer=dependencies.recovery_signer,
                recovery_verifier=dependencies.recovery_verifier,
                filesystem_policy=dependencies.filesystem_policy,
            ),
        )


def _monitor_rows(*, trade_date: date = TRADE_DATE) -> tuple[dict[str, object], ...]:
    return (
        {
            "trade_date": trade_date,
            "ts_code": "600001.SH",
            "level": "attack_strong_carry",
            "trigger_time": datetime(2026, 8, 3, 14, 58),
            "trigger_price": 10.1,
        },
    )


def _surge_proof() -> LegacySurgeCollectionProof:
    return LegacySurgeCollectionProof.create(
        trade_date=TRADE_DATE,
        started_at=datetime(2026, 8, 3, 1, 25, tzinfo=UTC),
        first_success_at=datetime(2026, 8, 3, 1, 30, tzinfo=UTC),
        last_success_at=datetime(2026, 8, 3, 7, 0, tzinfo=UTC),
        successful_snapshots=240,
        nonempty_successful_snapshots=240,
        empty_successful_snapshots=0,
        failed_snapshots=0,
        maximum_active_gap_seconds=60,
        maximum_consecutive_misses=0,
        ending_consecutive_misses=0,
        source_routes=("tushare_rt",),
        market_universe_id="9" * 64,
        market_universe_expected_count=5_000,
        minimum_market_coverage_count=4_900,
        minimum_market_coverage_bps=9_800,
        source_health="healthy",
    )


def test_monitor_export_is_atomic_idempotent_and_replayable(tmp_path: Path) -> None:
    rows = _monitor_rows()
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()

    with pytest.raises(RuntimeError, match="before publish"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=rows,
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
            fault_hook=lambda point: (
                (_ for _ in ()).throw(RuntimeError("before publish"))
                if point == "before_publish"
                else None
            ),
        )
    assert not (root / TRADE_DATE.isoformat()).exists()

    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=rows,
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    replayed = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=rows,
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )

    assert replayed == published
    accepted = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
    )
    assert accepted.records[0]["trade_date"] == TRADE_DATE.isoformat()
    assert accepted.records[0]["trigger_time"] == "2026-08-03T14:58:00"
    assert accepted.completion_receipt.producer_commit == COMMIT
    assert accepted.completion_receipt.producer_version == "legacy-monitor-v1"
    assert accepted.manifest.contract == "legacy-shadow-export/v2"
    assert accepted.manifest.captured_at == datetime(2026, 8, 3, 7, 3, tzinfo=UTC)
    assert accepted.manifest.as_of == datetime(2026, 8, 3, 7, 0, tzinfo=UTC)
    assert accepted.completion_receipt.produced_at == datetime(2026, 8, 3, 7, 3, tzinfo=UTC)


def test_monitor_export_records_distinct_trusted_capture_and_completion_times(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_sequenced_dependencies(
            (datetime(2026, 8, 3, 7, 1, tzinfo=UTC), 60_000_000_000),
            (datetime(2026, 8, 3, 7, 2, tzinfo=UTC), 120_000_000_000),
        ),
    )

    accepted = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
    )
    marker = json.loads((published / "recovery-marker.json").read_text(encoding="utf-8"))
    assert accepted.manifest.captured_at == datetime(2026, 8, 3, 7, 1, tzinfo=UTC)
    assert accepted.completion_receipt.produced_at == datetime(2026, 8, 3, 7, 1, tzinfo=UTC)
    assert marker["claims"]["captured_at"] == "2026-08-03T07:01:00Z"
    assert marker["claims"]["produced_at"] == "2026-08-03T07:02:00Z"


def test_monitor_export_starting_in_window_but_finishing_late_cannot_sign(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    with pytest.raises(LegacyShadowExportError, match="window|signing"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=_monitor_rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_sequenced_dependencies(
                (datetime(2026, 8, 3, 7, 4, 59, tzinfo=UTC), 60_000_000_000),
                (datetime(2026, 8, 3, 7, 15, tzinfo=UTC), 661_000_000_000),
            ),
        )

    assert not (root / TRADE_DATE.isoformat()).exists()
    assert tuple(root.glob(".staging-*")) == ()
    assert tuple(root.glob(".building-*/recovery-marker.json")) == ()


def test_test_authority_rejects_receipt_that_becomes_durable_after_1505(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    with pytest.raises(LegacyShadowExportError, match="window|signing"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=_monitor_rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_sequenced_dependencies(
                (datetime(2026, 8, 3, 7, 4, 59, 700_000, tzinfo=UTC), 60_000_000_000),
                (datetime(2026, 8, 3, 7, 4, 59, 800_000, tzinfo=UTC), 60_100_000_000),
                (datetime(2026, 8, 3, 7, 4, 59, 900_000, tzinfo=UTC), 60_200_000_000),
                (datetime(2026, 8, 3, 7, 5, 0, 100_000, tzinfo=UTC), 60_400_000_000),
            ),
        )

    assert not (root / TRADE_DATE.isoformat()).exists()
    assert tuple(root.glob(".staging-*/finalization-receipt.json")) == ()


def test_monitor_export_recovers_an_exact_publish_crash_on_writer_restart(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    rows = _monitor_rows()

    with pytest.raises(RuntimeError, match="after publish"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=rows,
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
            fault_hook=lambda point: (
                (_ for _ in ()).throw(RuntimeError("after publish"))
                if point == "after_publish"
                else None
            ),
        )

    completed_before_restart = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
    )

    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=rows,
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 15, tzinfo=UTC)),
    )
    assert published == completed_before_restart.session_path

    assert (
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        ).session_path
        == published
    )
    accepted = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
    )
    assert accepted.manifest.captured_at == datetime(2026, 8, 3, 7, 3, tzinfo=UTC)
    assert accepted.completion_receipt.produced_at == datetime(2026, 8, 3, 7, 3, tzinfo=UTC)


def test_monitor_export_recovers_only_complete_window_staging_after_publish_window(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()

    with pytest.raises(RuntimeError, match="before publish"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=_monitor_rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
            fault_hook=lambda point: (
                (_ for _ in ()).throw(RuntimeError("before publish"))
                if point == "before_publish"
                else None
            ),
        )
    staging = tuple(root.glob(f".staging-{TRADE_DATE.isoformat()}-*"))
    assert len(staging) == 1

    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 15, tzinfo=UTC)),
    )

    assert not staging[0].exists()
    accepted = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
    )
    assert accepted.session_path == published
    assert accepted.manifest.captured_at == datetime(2026, 8, 3, 7, 3, tzinfo=UTC)


@pytest.mark.parametrize(
    "exported_at",
    (
        datetime(2026, 8, 3, 6, 59, tzinfo=UTC),
        datetime(2026, 8, 3, 7, 15, tzinfo=UTC),
    ),
    ids=("before-close", "after-window"),
)
def test_monitor_export_rejects_fresh_outside_window_before_consuming_rows(
    tmp_path: Path,
    exported_at: datetime,
) -> None:
    consumed = 0

    def rows():
        nonlocal consumed
        consumed += 1
        yield from _monitor_rows()

    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    with pytest.raises(LegacyShadowExportError, match="recovery root|unavailable"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(exported_at),
        )

    assert consumed == 0
    assert not (root / TRADE_DATE.isoformat()).exists()


def test_monitor_export_rejects_conflicts_cross_day_stale_commit_and_tampering(
    tmp_path: Path,
) -> None:
    rows = _monitor_rows()
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=rows,
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )

    changed = ({**rows[0], "trigger_price": 10.2},)
    with pytest.raises(LegacyShadowExportConflictError):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=changed,
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )
    with pytest.raises(LegacyShadowExportUnavailableError, match="producer commit"):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit="b" * 40,
        )
    with pytest.raises(ValueError, match="trade_date"):
        publish_legacy_monitor_export(
            root=(tmp_path / "cross-day").resolve(),
            trade_date=TRADE_DATE,
            rows=_monitor_rows(trade_date=date(2026, 8, 4)),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )

    events = published / "events.json"
    events.chmod(0o600)
    payload = json.loads(events.read_text(encoding="utf-8"))
    payload[0]["trigger_price"] = 999.0
    events.write_text(json.dumps(payload), encoding="utf-8")
    events.chmod(0o444)
    with pytest.raises(LegacyShadowExportUnavailableError, match="digest"):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


def test_reader_rejects_partial_unaccepted_monitor_batch(tmp_path: Path) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    session = root / TRADE_DATE.isoformat()
    session.mkdir(parents=True)
    (session / "events.json").write_text("[]", encoding="utf-8")

    with pytest.raises(LegacyShadowExportUnavailableError):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


def test_consumer_rejects_a_signed_batch_without_finalization_receipt(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    receipt = published / "finalization-receipt.json"
    assert receipt.is_file()
    receipt.chmod(0o600)
    receipt.unlink()

    with pytest.raises(LegacyShadowExportUnavailableError, match="finalization"):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


@pytest.mark.parametrize(
    "binding",
    ("marker_id", "directory_inode", "artifact_digest"),
)
def test_consumer_rejects_validly_signed_finalization_with_wrong_batch_binding(
    tmp_path: Path,
    binding: str,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    receipt_path = published / "finalization-receipt.json"
    document = json.loads(receipt_path.read_bytes())
    claims_values = dict(document["claims"])
    if binding == "marker_id":
        claims_values["marker_id"] = "f" * 64
    elif binding == "directory_inode":
        claims_values["directory_inode"] += 1
    else:
        claims_values["artifact_digests"] = {
            **claims_values["artifact_digests"],
            "events.json": "f" * 64,
        }
    claims = LegacyShadowFinalizationClaims.model_validate(claims_values)
    signature = hmac.new(
        _RECOVERY_SECRET,
        legacy_shadow_export_module._finalization_receipt_payload(claims),
        hashlib.sha256,
    ).hexdigest()
    values = {
        "contract": "legacy-shadow-finalization-receipt/v1",
        "key_id": "legacy-shadow-recovery-test",
        "signature_algorithm": "test-hmac-sha256",
        "claims": claims,
        "signature": signature,
    }
    forged = LegacyShadowFinalizationReceipt(
        receipt_id=canonical_sha256(values),
        **values,
    )
    receipt_path.chmod(0o600)
    receipt_path.write_bytes(canonical_model_json_bytes(forged))
    receipt_path.chmod(0o444)

    with pytest.raises(
        LegacyShadowExportUnavailableError,
        match="finalization receipt binding",
    ):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


def test_consumer_rejects_finalization_with_invalid_signature(tmp_path: Path) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    receipt_path = published / "finalization-receipt.json"
    document = json.loads(receipt_path.read_bytes())
    claims = LegacyShadowFinalizationClaims.model_validate(document["claims"])
    values = {
        "contract": document["contract"],
        "key_id": document["key_id"],
        "signature_algorithm": document["signature_algorithm"],
        "claims": claims,
        "signature": "0" * 64,
    }
    forged = LegacyShadowFinalizationReceipt(
        receipt_id=canonical_sha256(values),
        **values,
    )
    receipt_path.chmod(0o600)
    receipt_path.write_bytes(canonical_model_json_bytes(forged))
    receipt_path.chmod(0o444)

    with pytest.raises(
        LegacyShadowExportUnavailableError,
        match="finalization receipt signature",
    ):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


def test_consumer_maps_finalization_verifier_failure_to_unavailable(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    dependencies = _dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC))
    publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=dependencies,
    )

    class FailingFinalizationVerifier:
        def verify(self, marker: object) -> bool:
            return dependencies.recovery_verifier.verify(marker)

        def verify_finalization(self, receipt: object) -> bool:
            raise RuntimeError("external verifier unavailable")

    with pytest.raises(
        LegacyShadowExportUnavailableError,
        match="finalization receipt verification is unavailable",
    ):
        _load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
            recovery_verifier=FailingFinalizationVerifier(),
            filesystem_policy=dependencies.filesystem_policy,
        )


def test_reader_rejects_recomputed_marker_identity_with_forged_signature(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    marker_path = published / "recovery-marker.json"
    document = json.loads(marker_path.read_text(encoding="utf-8"))
    document["signature"] = "0" * 64
    marker_identity = {key: value for key, value in document.items() if key != "marker_id"}
    marker_identity["claims"] = LegacyShadowRecoveryMarkerClaims.model_validate(
        marker_identity["claims"]
    )
    document["marker_id"] = canonical_sha256(marker_identity)
    marker_path.chmod(0o600)
    marker_path.write_text(
        json.dumps(
            document,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    marker_path.chmod(0o444)

    with pytest.raises(LegacyShadowExportUnavailableError, match="signature"):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


def test_reader_maps_receipt_raw_input_binding_failure_to_unavailable(tmp_path: Path) -> None:
    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    published = publish_legacy_monitor_export(
        root=root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    accepted = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
    )
    receipt = ShadowSourceCompletionReceipt.model_validate(
        {
            **accepted.completion_receipt.model_dump(
                mode="python",
                exclude={"receipt_id"},
            ),
            "input_identity": "f" * 64,
        }
    )
    completion_payload = canonical_model_json_bytes(receipt)
    manifest_values = {
        **accepted.manifest.model_dump(mode="python", exclude={"batch_id"}),
        "completion_sha256": hashlib.sha256(completion_payload).hexdigest(),
        "completion_receipt_id": str(receipt.receipt_id),
        "input_identity": "f" * 64,
    }
    manifest = LegacyShadowExportManifest(
        batch_id=canonical_sha256(manifest_values),
        **manifest_values,
    )
    completion_path = published / "completion.json"
    manifest_path = published / "manifest.json"
    for path, payload in (
        (completion_path, completion_payload),
        (manifest_path, canonical_model_json_bytes(manifest)),
    ):
        path.chmod(0o600)
        path.write_bytes(payload)
        path.chmod(0o444)

    with pytest.raises(
        LegacyShadowExportUnavailableError,
        match="completion binding|marker (?:batch|artifact) digest",
    ):
        load_accepted_legacy_shadow_export(
            root=root,
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        )


def test_surge_export_copies_closed_jsonl_into_the_accepted_export_contract(
    tmp_path: Path,
) -> None:
    source = tmp_path / "surge-live" / "events-2026-08-03.jsonl"
    source.parent.mkdir()
    source.write_text(
        json.dumps(
            {
                "ts_code": "300001.SZ",
                "confirmed_at": "14:59",
                "status": "confirmed",
                "rel_cum": 2.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    root = (tmp_path / "legacy-shadow" / "surge").resolve()

    published = publish_legacy_surge_export(
        root=root,
        trade_date=TRADE_DATE,
        events_path=source,
        producer_commit=COMMIT,
        producer_version="legacy-surge-watch-v1",
        collection_proof=_surge_proof(),
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 4, tzinfo=UTC)),
    )

    accepted = load_accepted_legacy_shadow_export(
        root=root,
        trade_date=TRADE_DATE,
        expected_source_id="legacy-surge-jsonl",
        expected_commit=COMMIT,
    )
    assert published == accepted.session_path
    assert accepted.records[0]["ts_code"] == "300001.SZ"
    assert accepted.records[0]["confirmed_at"] == "14:59"
    assert accepted.records[0]["rel_cum"] == 2.5
    assert (published / "events.jsonl").read_text(encoding="utf-8").endswith("\n")


def test_surge_source_open_is_parent_bound_and_nofollow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    source = tmp_path / "surge-live" / "events-2026-08-03.jsonl"
    source.parent.mkdir()
    source.write_text(
        '{"confirmed_at":"14:59","status":"confirmed","ts_code":"300001.SZ"}\n',
        encoding="utf-8",
    )
    real_open = os.open
    observed: list[tuple[int, int | None]] = []

    def audited_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if path == source.name:
            observed.append((flags, dir_fd))
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(export_module.os, "open", audited_open)
    publish_legacy_surge_export(
        root=(tmp_path / "legacy-shadow" / "surge").resolve(),
        trade_date=TRADE_DATE,
        events_path=source,
        producer_commit=COMMIT,
        producer_version="legacy-surge-watch-v1",
        collection_proof=_surge_proof(),
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )

    assert len(observed) == 1
    flags, dir_fd = observed[0]
    assert dir_fd is not None
    assert flags & os.O_NOFOLLOW


def test_surge_source_rejects_symlinked_parent_and_fifo(tmp_path: Path) -> None:
    real_parent = tmp_path / "real-live"
    real_parent.mkdir()
    source = real_parent / "events-2026-08-03.jsonl"
    source.write_text("", encoding="utf-8")
    linked_parent = tmp_path / "linked-live"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    linked_file = tmp_path / "linked-events.jsonl"
    linked_file.symlink_to(source)
    fifo = tmp_path / "events.fifo"
    os.mkfifo(fifo)

    for index, unsafe in enumerate((linked_parent / source.name, linked_file, fifo)):
        with pytest.raises(LegacyShadowExportError, match="unsafe|unavailable"):
            publish_legacy_surge_export(
                root=(tmp_path / f"legacy-shadow-{index}").resolve(),
                trade_date=TRADE_DATE,
                events_path=unsafe,
                producer_commit=COMMIT,
                producer_version="legacy-surge-watch-v1",
                collection_proof=_surge_proof(),
                dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
            )


def test_surge_source_rejects_oversize_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    source = tmp_path / "events.jsonl"
    source.write_bytes(b"x" * 33)
    monkeypatch.setattr(export_module, "_MAX_EXPORT_BYTES", 32)
    real_read = os.read
    source_reads = 0

    def audited_read(descriptor: int, maximum: int) -> bytes:
        nonlocal source_reads
        if os.fstat(descriptor).st_ino == source.stat().st_ino:
            source_reads += 1
        return real_read(descriptor, maximum)

    monkeypatch.setattr(export_module.os, "read", audited_read)
    with pytest.raises(LegacyShadowExportError, match="budget|unsafe"):
        publish_legacy_surge_export(
            root=(tmp_path / "legacy-shadow" / "surge").resolve(),
            trade_date=TRADE_DATE,
            events_path=source,
            producer_commit=COMMIT,
            producer_version="legacy-surge-watch-v1",
            collection_proof=_surge_proof(),
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )

    assert source_reads == 0


def test_surge_source_rejects_path_replacement_while_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    source = tmp_path / "events.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    source.write_text(
        '{"confirmed_at":"14:59","status":"confirmed","ts_code":"300001.SZ"}\n',
        encoding="utf-8",
    )
    replacement.write_text("", encoding="utf-8")
    original_inode = source.stat().st_ino
    real_read = os.read
    replaced = False

    def replacing_read(descriptor: int, maximum: int) -> bytes:
        nonlocal replaced
        payload = real_read(descriptor, maximum)
        if not replaced and os.fstat(descriptor).st_ino == original_inode:
            os.replace(replacement, source)
            replaced = True
        return payload

    monkeypatch.setattr(export_module.os, "read", replacing_read)
    with pytest.raises(LegacyShadowExportError, match="changed"):
        publish_legacy_surge_export(
            root=(tmp_path / "legacy-shadow" / "surge").resolve(),
            trade_date=TRADE_DATE,
            events_path=source,
            producer_commit=COMMIT,
            producer_version="legacy-surge-watch-v1",
            collection_proof=_surge_proof(),
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )


def test_legacy_shadow_filesystem_preflight_declares_local_posix_contract(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "legacy-shadow").resolve()

    root.mkdir(parents=True)
    validated = validate_legacy_shadow_filesystem_contract(
        root,
        policy=legacy_shadow_test_filesystem_policy(),
    )

    assert validated == LEGACY_SHADOW_FILESYSTEM_CONTRACT
    assert validated.filesystem == "local-posix"
    assert validated.atomic_rename_same_filesystem is True
    assert validated.parent_dir_fd_nofollow is True


@pytest.mark.parametrize("filesystem", ("nfs4", "fuse.sshfs", "overlay", "unknownfs"))
def test_production_filesystem_preflight_rejects_remote_or_unknown_mounts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem: str,
) -> None:
    import rquant.legacy_shadow_export as export_module

    root = (tmp_path / "legacy-shadow").resolve()
    root.mkdir()
    monkeypatch.setattr(export_module.sys, "platform", "linux")
    monkeypatch.setattr(export_module, "_linux_mount_filesystem", lambda _path: filesystem)

    with pytest.raises(LegacyShadowExportError, match="approved local mount"):
        validate_legacy_shadow_filesystem_contract(
            root,
            policy=export_module.LegacyShadowFilesystemPolicy(mode="linux-production"),
        )


def test_export_rename_transaction_is_parent_dirfd_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    real_rename = os.rename
    observed: list[tuple[int | None, int | None]] = []

    def audited_rename(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        destination: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        if str(source).startswith((".building-", ".staging-")):
            observed.append((src_dir_fd, dst_dir_fd))
        real_rename(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )

    monkeypatch.setattr(export_module.os, "rename", audited_rename)
    publish_legacy_monitor_export(
        root=(tmp_path / "legacy-shadow" / "monitor").resolve(),
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )

    assert len(observed) == 2
    assert all(source is not None and source == destination for source, destination in observed)


def test_monitor_record_budget_rejects_before_consuming_the_full_iterable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    consumed = 0

    def rows():
        nonlocal consumed
        for _ in range(10):
            consumed += 1
            yield _monitor_rows()[0]

    monkeypatch.setattr(export_module, "_MAX_RECORDS", 2)
    with pytest.raises(LegacyShadowExportError, match="record count"):
        publish_legacy_monitor_export(
            root=(tmp_path / "legacy-shadow" / "monitor").resolve(),
            trade_date=TRADE_DATE,
            rows=rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )

    assert consumed == 3


def test_monitor_spool_rejects_record_budget_during_cursor_iteration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    consumed = 0

    def rows():
        nonlocal consumed
        while True:
            consumed += 1
            yield _monitor_rows()[0]

    monkeypatch.setattr(export_module, "_MAX_RECORDS", 2)
    with pytest.raises(LegacyShadowExportError, match="record count"):
        prepare_legacy_monitor_spool(
            root=(tmp_path / "legacy-shadow" / "monitor").resolve(),
            trade_date=TRADE_DATE,
            rows=rows(),
            filesystem_policy=legacy_shadow_test_filesystem_policy(),
        )

    assert consumed == 3


def test_monitor_record_and_total_byte_budgets_fail_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    monkeypatch.setattr(export_module, "_MAX_RECORD_BYTES", 32)
    with pytest.raises(LegacyShadowExportError, match="record.*byte budget"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=_monitor_rows(),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )
    assert not (root / TRADE_DATE.isoformat()).exists()

    monkeypatch.setattr(export_module, "_MAX_RECORD_BYTES", 1024)
    monkeypatch.setattr(export_module, "_MAX_EXPORT_BYTES", 300)
    with pytest.raises(LegacyShadowExportError, match="total byte budget"):
        publish_legacy_monitor_export(
            root=root,
            trade_date=TRADE_DATE,
            rows=(_monitor_rows()[0], _monitor_rows()[0], _monitor_rows()[0]),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-v1",
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )
    assert not (root / TRADE_DATE.isoformat()).exists()


def test_monitor_raw_and_envelopes_are_streamed_to_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    original = export_module._write_new_file_at

    def metadata_only(directory_descriptor: int, filename: str, payload: bytes) -> None:
        assert filename not in {"events.json", "records.jsonl"}
        original(directory_descriptor, filename, payload)

    monkeypatch.setattr(export_module, "_write_new_file_at", metadata_only)
    publish_legacy_monitor_export(
        root=(tmp_path / "legacy-shadow" / "monitor").resolve(),
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )


def test_surge_record_budget_stops_parsing_at_limit_plus_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    source = tmp_path / "events.jsonl"
    line = b'{"confirmed_at":"14:59","status":"confirmed","ts_code":"300001.SZ"}\n'
    source.write_bytes(line * 10)
    parsed = 0
    original = export_module.strict_json_loads

    def audited(payload: bytes):
        nonlocal parsed
        parsed += 1
        return original(payload)

    monkeypatch.setattr(export_module, "strict_json_loads", audited)
    monkeypatch.setattr(export_module, "_MAX_RECORDS", 2)
    with pytest.raises(LegacyShadowExportError, match="record count"):
        publish_legacy_surge_export(
            root=(tmp_path / "legacy-shadow" / "surge").resolve(),
            trade_date=TRADE_DATE,
            events_path=source,
            producer_commit=COMMIT,
            producer_version="legacy-surge-watch-v1",
            collection_proof=_surge_proof(),
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
        )

    assert parsed == 2


def test_production_export_wrappers_bind_roots_and_require_a_trusted_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = (tmp_path / "data").resolve()
    exported_at = datetime(2026, 8, 3, 7, 4, tzinfo=UTC)
    environment = {"RQUANT_CODE_COMMIT": COMMIT}
    surge_source = tmp_path / "surge-live" / "events-2026-08-03.jsonl"
    surge_source.parent.mkdir()
    surge_source.write_text(
        '{"confirmed_at":"14:59","status":"confirmed","ts_code":"300001.SZ"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "rquant.legacy_shadow_export._production_export_dependencies",
        lambda _data_dir, *, environment: _dependencies(exported_at),
    )
    monitor_spool = prepare_legacy_monitor_spool(
        root=data_dir / "legacy-shadow" / "monitor",
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        filesystem_policy=legacy_shadow_test_filesystem_policy(),
    )

    publish_legacy_monitor_production_export(
        data_dir=data_dir,
        trade_date=TRADE_DATE,
        spool=monitor_spool,
        environment=environment,
    )
    publish_legacy_surge_production_export(
        data_dir=data_dir,
        trade_date=TRADE_DATE,
        events_path=surge_source,
        collection_proof=_surge_proof(),
        environment=environment,
    )

    assert (
        load_accepted_legacy_shadow_export(
            root=data_dir / "legacy-shadow" / "monitor",
            trade_date=TRADE_DATE,
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
        ).manifest.producer_version
        == "legacy-monitor-shadow-export/v1"
    )
    assert (
        load_accepted_legacy_shadow_export(
            root=data_dir / "legacy-shadow" / "surge",
            trade_date=TRADE_DATE,
            expected_source_id="legacy-surge-jsonl",
            expected_commit=COMMIT,
        ).manifest.producer_version
        == "legacy-surge-shadow-export/v1"
    )

    with pytest.raises(LegacyShadowExportError, match="RQUANT_CODE_COMMIT"):
        publish_legacy_monitor_production_export(
            data_dir=data_dir,
            trade_date=TRADE_DATE,
            spool=monitor_spool,
            environment={"RQUANT_CODE_COMMIT": "not-a-commit"},
        )


class _CompletedRunner:
    def __init__(
        self,
        strategy_id: str = "n_shape",
        *,
        producer_version: str = "strategy-live-v1",
        producer_manifest_fingerprint: str = "8" * 64,
        producer_instance_id: str | None = None,
        registration_fingerprint: str = "2" * 64,
        strategy_spec_fingerprint: str = "f" * 64,
        executable_fingerprint: str = "3" * 64,
    ) -> None:
        self.strategy_id = strategy_id
        self.source_id = f"strategy.{strategy_id}.v1"
        producer_instance_id = producer_instance_id or f"strategy-{strategy_id}-primary"
        signal = SignalEnvelope(
            schema_version=1,
            strategy_id=strategy_id,
            strategy_version="1",
            parameter_fingerprint="b" * 64,
            dataset_snapshot_id="c" * 64,
            feature_snapshot_id="d" * 64,
            event_time=datetime(2026, 8, 3, 1, 31, tzinfo=UTC),
            available_at=datetime(2026, 8, 3, 1, 31, 3, tzinfo=UTC),
            candidate_id="600001.SH",
            action=SignalAction.WATCH,
            reason_codes=("shadow",),
            evidence={"source": "test"},
            expires_at=datetime(2026, 8, 3, 1, 36, tzinfo=UTC),
            producer_commit=COMMIT,
        )
        self.records = (RunnerSignalRecord(sequence=1, signal=signal),)
        self.descriptor = RouteSourceDescriptor(
            source_id=self.source_id,
            generation_id="e" * 64,
            strategy_spec_fingerprint=strategy_spec_fingerprint,
            first_sequence=1,
            high_watermark=1,
        )
        _session_open, session_close = shadow_session_boundaries(TRADE_DATE)
        raw_input_id = runner_source_raw_input_id(
            self.descriptor,
            self.records,
            trade_date=TRADE_DATE,
        )
        unsigned = ShadowSourceCompletionReceipt(
            evidence_origin="production",
            source="isolated",
            source_id=self.source_id,
            trade_date=TRADE_DATE,
            session_close_at=session_close,
            complete_through=session_close,
            input_identity=raw_input_id,
            produced_at=session_close + timedelta(minutes=1),
            producer_commit=COMMIT,
            producer_version=producer_version,
            producer_service_id=self.source_id,
            producer_instance_id=producer_instance_id,
            runner_generation_id="e" * 64,
            signal_authority_generation_id="a" * 64,
            calendar_generation_id="b" * 64,
            last_sequence=0,
            high_watermark=1,
            route_receipts_id="c" * 64,
            feature_source_generation_id="d" * 64,
            feature_close_marker_id="e" * 64,
            feature_segment_chain_hash="f" * 64,
            segment_start_sequence=0,
            segment_record_count=1,
            segment_chain_hash="1" * 64,
        )
        claims = CompletionAttestationClaims(
            completion_receipt_body_sha256=shadow_completion_receipt_body_sha256(unsigned),
            trade_date=TRADE_DATE,
            session_close_at=session_close,
            source_id=self.source_id,
            input_identity=raw_input_id,
            strategy_id=strategy_id,
            strategy_version=1,
            strategy_registration_fingerprint=registration_fingerprint,
            strategy_spec_fingerprint=strategy_spec_fingerprint,
            executable_fingerprint=executable_fingerprint,
            candidate_schema_fingerprint="4" * 64,
            feature_registration_fingerprint="5" * 64,
            feature_contract_fingerprint="6" * 64,
            routing_policy_fingerprint="7" * 64,
            producer_manifest_fingerprint=producer_manifest_fingerprint,
            producer_commit=COMMIT,
            producer_version=producer_version,
            producer_service_id=self.source_id,
            producer_instance_id=producer_instance_id,
            calendar_generation_id="b" * 64,
            feature_source_generation_id="d" * 64,
            feature_close_marker_id="e" * 64,
            feature_segment_chain_hash="f" * 64,
            runner_generation_id="e" * 64,
            runner_segment_start_sequence=0,
            runner_segment_final_sequence=1,
            runner_segment_record_count=1,
            runner_segment_chain_hash="1" * 64,
            signal_authority_generation_id="a" * 64,
            route_receipts_id="c" * 64,
        )
        self.receipt = ShadowSourceCompletionReceipt.model_validate(
            {
                **unsigned.model_dump(mode="python", exclude={"receipt_id"}),
                "completion_attestation": _ATTESTATION_AUTHORITY.issue(claims),
            }
        )

    def read_completion_receipt(self, *, trade_date: date) -> ShadowSourceCompletionReceipt:
        assert trade_date == TRADE_DATE
        return self.receipt

    def strategy_identity(self) -> tuple[str, int, str]:
        return self.strategy_id, 1, self.descriptor.strategy_spec_fingerprint

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch:
        assert trade_date == TRADE_DATE
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(descriptor=self.descriptor),
            after_sequence=after_sequence,
            limit=limit,
            records=tuple(item for item in self.records if item.sequence > after_sequence)[:limit],
        )


def test_isolated_runner_export_copies_only_the_completed_receipt_watermark(
    tmp_path: Path,
) -> None:
    source = _CompletedRunner()
    root = (tmp_path / "legacy-shadow" / "isolated-runners").resolve()

    published = publish_isolated_runner_export(
        root=root,
        strategy_id="n_shape",
        trade_date=TRADE_DATE,
        source=source,
        expected_commit=COMMIT,
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
    )

    accepted = load_accepted_legacy_shadow_export(
        root=root / "n_shape",
        trade_date=TRADE_DATE,
        expected_source_id=source.source_id,
        expected_commit=COMMIT,
    )
    assert accepted.session_path == published
    assert accepted.completion_receipt == source.receipt
    assert accepted.manifest.records_filename == "completed-batch.json"

    with pytest.raises(LegacyShadowExportError, match="recovery root|unavailable"):
        publish_isolated_runner_export(
            root=(tmp_path / "late-isolated").resolve(),
            strategy_id="n_shape",
            trade_date=TRADE_DATE,
            source=_CompletedRunner(),
            expected_commit=COMMIT,
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 15, tzinfo=UTC)),
        )


def test_isolated_runner_late_restart_only_promotes_signed_staging(
    tmp_path: Path,
) -> None:
    source = _CompletedRunner()
    root = (tmp_path / "legacy-shadow" / "isolated-runners").resolve()
    with pytest.raises(RuntimeError, match="before publish"):
        publish_isolated_runner_export(
            root=root,
            strategy_id="n_shape",
            trade_date=TRADE_DATE,
            source=source,
            expected_commit=COMMIT,
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
            fault_hook=lambda point: (
                (_ for _ in ()).throw(RuntimeError("before publish"))
                if point == "before_publish"
                else None
            ),
        )

    published = publish_isolated_runner_export(
        root=root,
        strategy_id="n_shape",
        trade_date=TRADE_DATE,
        source=source,
        expected_commit=COMMIT,
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 15, tzinfo=UTC)),
    )

    assert published == root / "n_shape" / TRADE_DATE.isoformat()
    assert (
        load_accepted_legacy_shadow_export(
            root=root / "n_shape",
            trade_date=TRADE_DATE,
            expected_source_id=source.source_id,
            expected_commit=COMMIT,
        ).session_path
        == published
    )


def test_isolated_runner_budgets_apply_before_materializing_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.legacy_shadow_export as export_module

    source = _CompletedRunner()
    monkeypatch.setattr(export_module, "_MAX_RECORDS", 0)
    with pytest.raises(LegacyShadowExportError, match="record count"):
        publish_isolated_runner_export(
            root=(tmp_path / "count").resolve(),
            strategy_id="n_shape",
            trade_date=TRADE_DATE,
            source=source,
            expected_commit=COMMIT,
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
        )

    monkeypatch.setattr(export_module, "_MAX_RECORDS", 100_000)
    monkeypatch.setattr(export_module, "_MAX_RECORD_BYTES", 32)
    with pytest.raises(LegacyShadowExportError, match="record.*byte budget"):
        publish_isolated_runner_export(
            root=(tmp_path / "record-bytes").resolve(),
            strategy_id="n_shape",
            trade_date=TRADE_DATE,
            source=source,
            expected_commit=COMMIT,
            dependencies=_dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
        )


def test_isolated_runner_production_fan_in_requires_both_shadow_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = (tmp_path / "data").resolve()
    sources = {
        "n_shape": _CompletedRunner("n_shape"),
        "growth_board_surge": _CompletedRunner("growth_board_surge"),
    }
    monkeypatch.setattr(
        "rquant.legacy_shadow_export._production_export_dependencies",
        lambda _data_dir, *, environment: _dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
    )
    authorities = {}
    for strategy_id, source in sources.items():
        attestation = source.receipt.completion_attestation
        assert attestation is not None
        claims = attestation.claims
        authorities[strategy_id] = SimpleNamespace(
            binding=LegacyShadowRunnerManifestBinding.create(
                strategy_id=claims.strategy_id,
                strategy_version=claims.strategy_version,
                producer_manifest_fingerprint=claims.producer_manifest_fingerprint,
                producer_commit=claims.producer_commit,
                producer_service_id=claims.producer_service_id,
                producer_instance_id=claims.producer_instance_id,
                producer_version=claims.producer_version,
                strategy_registration_fingerprint=(claims.strategy_registration_fingerprint),
                strategy_spec_fingerprint=claims.strategy_spec_fingerprint,
                evaluator_contract_fingerprint=claims.executable_fingerprint,
                executable_fingerprint=claims.executable_fingerprint,
            ),
            runner_state_path=(data_dir / "runtime" / f"{strategy_id}.sqlite3"),
        )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda _root: object(),
    )
    monkeypatch.setattr(
        "rquant.legacy_shadow_export._production_runner_authorities",
        lambda _profile, *, expected_commit: (
            authorities if expected_commit == COMMIT else pytest.fail("unexpected producer commit")
        ),
    )

    published = publish_isolated_runner_production_exports(
        data_dir=data_dir,
        trade_date=TRADE_DATE,
        sources=sources,
        environment={"RQUANT_CODE_COMMIT": COMMIT},
    )

    assert set(published) == {"n_shape", "growth_board_surge"}
    for strategy_id, source in sources.items():
        accepted = load_accepted_legacy_shadow_export(
            root=data_dir / "legacy-shadow" / "isolated-runners" / strategy_id,
            trade_date=TRADE_DATE,
            expected_source_id=source.source_id,
            expected_commit=COMMIT,
        )
        assert accepted.completion_receipt == source.receipt

    with pytest.raises(LegacyShadowExportError, match="exactly"):
        publish_isolated_runner_production_exports(
            data_dir=data_dir,
            trade_date=TRADE_DATE,
            sources={"n_shape": sources["n_shape"]},
            environment={"RQUANT_CODE_COMMIT": COMMIT},
        )


def test_profile_fan_in_uses_strategy_live_manifests_not_router_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = (tmp_path / "data").resolve()
    runner_manifests = []
    for strategy_id, producer_version in (
        ("n_shape", "strategy-live-n-shape-v7"),
        ("growth_board_surge", "strategy-live-growth-v11"),
    ):
        service_id = f"strategy.{strategy_id}.v1"
        runner_manifests.append(
            RuntimeServiceManifest(
                service_id=service_id,
                service_kind=RuntimeServiceKind.STRATEGY_LIVE,
                plane=RuntimeServicePlane.LIVE,
                interval_seconds=2,
                stale_after_seconds=30,
                producer_commit=COMMIT,
                settings={
                    "runner_state_path": str(data_dir / "runtime" / f"{service_id}.sqlite3"),
                    "producer_instance_id": f"instance-{strategy_id}",
                    "producer_version": producer_version,
                    "strategy_id": strategy_id,
                    "strategy_version": 1,
                    "strategy_registration_fingerprint": "2" * 64,
                    "strategy_spec_fingerprint": "f" * 64,
                    "evaluator_contract_fingerprint": "3" * 64,
                    "strategy_executable_fingerprint": "3" * 64,
                },
            )
        )
    sources = {
        manifest.service_id: _CompletedRunner(
            str(manifest.settings["strategy_id"]),
            producer_version=str(manifest.settings["producer_version"]),
            producer_manifest_fingerprint=manifest.manifest_fingerprint,
            producer_instance_id=str(manifest.settings["producer_instance_id"]),
        )
        for manifest in runner_manifests
    }
    router = RuntimeServiceManifest(
        service_id="signal-router.all-strategies.v1",
        service_kind=RuntimeServiceKind.SIGNAL_ROUTER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=2,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "signal_bus_path": str(data_dir / "runtime" / "signal_bus.sqlite3"),
            "signal_spool_root": str(data_dir / "runtime" / "signal-spool"),
            "routing_policy_fingerprint": "1" * 64,
            "routing_policy_path": str(data_dir / "runtime" / "routing-policy.json"),
            "batch_limit": 256,
            "sources": [
                {
                    "source_id": source_id,
                    "runner_state_path": str(data_dir / "runtime" / f"forged-{source_id}.sqlite3"),
                    "expected_strategy_registration_fingerprint": "2" * 64,
                    "expected_strategy_spec_fingerprint": "a" * 64,
                    "expected_evaluator_contract_fingerprint": "b" * 64,
                }
                for source_id in sources
            ],
        },
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda _root: SimpleNamespace(
            producer_commit=COMMIT,
            manifests=(router, *runner_manifests),
        ),
    )
    reader_arguments: list[dict[str, object]] = []
    monkeypatch.setattr(
        "rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource",
        lambda **kwargs: (
            reader_arguments.append(kwargs),
            sources[kwargs["source_id"]],
        )[1],
    )
    monkeypatch.setattr(
        "rquant.legacy_shadow_export._production_export_dependencies",
        lambda _data_dir, *, environment: _dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
    )

    published = fan_in_production_isolated_runner_exports(
        data_dir=data_dir,
        trade_date=TRADE_DATE,
        environment={"RQUANT_CODE_COMMIT": COMMIT},
    )

    assert set(published) == {"n_shape", "growth_board_surge"}
    expected_by_source = {manifest.service_id: manifest for manifest in runner_manifests}
    for arguments in reader_arguments:
        manifest = expected_by_source[str(arguments["source_id"])]
        assert arguments["path"] == Path(str(manifest.settings["runner_state_path"]))
        assert arguments["expected_strategy_spec_fingerprint"] == "f" * 64
        assert arguments["expected_evaluator_contract_fingerprint"] == "3" * 64


def test_shadow_filesystem_loader_accepts_only_validated_export_batches(tmp_path: Path) -> None:
    legacy_root = (tmp_path / "legacy-shadow").resolve()
    monitor_rows = _monitor_rows()
    publish_legacy_monitor_export(
        root=legacy_root / "monitor",
        trade_date=TRADE_DATE,
        rows=monitor_rows,
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    surge_source = tmp_path / "events.jsonl"
    surge_source.write_text(
        '{"confirmed_at":"14:59","status":"confirmed","ts_code":"300001.SZ"}\n',
        encoding="utf-8",
    )
    publish_legacy_surge_export(
        root=legacy_root / "surge",
        trade_date=TRADE_DATE,
        events_path=surge_source,
        producer_commit=COMMIT,
        producer_version="legacy-surge-watch-v1",
        collection_proof=_surge_proof(),
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 4, tzinfo=UTC)),
    )
    source = _CompletedRunner()
    growth_source = _CompletedRunner(
        "growth_board_surge",
        producer_version="strategy-live-growth-board-surge-v1",
        producer_manifest_fingerprint="9" * 64,
        registration_fingerprint="4" * 64,
        executable_fingerprint="5" * 64,
    )
    publish_isolated_runner_export(
        root=legacy_root / "isolated-runners",
        strategy_id="n_shape",
        trade_date=TRADE_DATE,
        source=source,
        expected_commit=COMMIT,
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
    )
    publish_isolated_runner_export(
        root=legacy_root / "isolated-runners",
        strategy_id="growth_board_surge",
        trade_date=TRADE_DATE,
        source=growth_source,
        expected_commit=COMMIT,
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 2, tzinfo=UTC)),
    )
    settings = ShadowSessionSettings(
        report_root=(tmp_path / "reports").resolve(),
        legacy_monitor_root=legacy_root / "monitor",
        legacy_surge_root=legacy_root / "surge",
        isolated_runner_root=legacy_root / "isolated-runners",
        calendar_path=(tmp_path / "calendar.json").resolve(),
        calendar_expected_commit=COMMIT,
        calendar_content_sha256="1" * 64,
        completion_active_key_id="completion-v1",
        completion_active_public_key_pem="test-public-key",
        report_active_key_id="report-v1",
        report_active_public_key_pem="test-public-key",
        signer_command=(
            "/usr/bin/sudo",
            "-n",
            "/usr/local/libexec/rquant-shadow-report-signer",
        ),
        report_producer_service_id="shadow-session",
        report_producer_instance_id="shadow-primary",
        signer_timeout_seconds=1.0,
        producer_version="shadow-v1",
        match_tolerance_microseconds=60_000_000,
        strategy_bindings=(
            ShadowStrategyBinding(
                strategy_id="n_shape",
                strategy_version=1,
                definition_fingerprint="2" * 64,
                executable_fingerprint="3" * 64,
            ),
            ShadowStrategyBinding(
                strategy_id="growth_board_surge",
                strategy_version=1,
                definition_fingerprint="4" * 64,
                executable_fingerprint="5" * 64,
            ),
        ),
        runner_manifest_bindings=tuple(
            load_accepted_legacy_shadow_export(
                root=legacy_root / "isolated-runners" / strategy_id,
                trade_date=TRADE_DATE,
                expected_source_id=None,
                expected_commit=COMMIT,
            ).manifest.runner_manifest_binding
            for strategy_id in ("n_shape", "growth_board_surge")
        ),
    )

    dependencies = _dependencies(datetime(2026, 8, 3, 7, 5, tzinfo=UTC))
    loader = FilesystemShadowSessionInputLoader(
        recovery_verifier=dependencies.recovery_verifier,
        filesystem_policy=dependencies.filesystem_policy,
    )
    loaded = loader.load(
        settings=settings,
        trade_date=TRADE_DATE,
        expected_export_commit=COMMIT,
    )
    assert loaded.monitor_rows[0]["ts_code"] == "600001.SH"
    for _binding, runner_source in loaded.runner_sources:
        runner_source.read_completion_receipt(trade_date=TRADE_DATE)

    n_shape_binding = next(
        binding for binding in settings.runner_manifest_bindings if binding.strategy_id == "n_shape"
    )
    for field_name, forged_value in (
        ("producer_manifest_fingerprint", "0" * 64),
        ("producer_commit", "b" * 40),
        ("producer_instance_id", "forged-instance"),
        ("producer_version", "forged-runner-version"),
        ("strategy_registration_fingerprint", "a" * 64),
        ("strategy_spec_fingerprint", "b" * 64),
        ("evaluator_contract_fingerprint", "c" * 64),
        ("executable_fingerprint", "d" * 64),
    ):
        values = n_shape_binding.model_dump(
            mode="python",
            exclude={"contract", "binding_id"},
        )
        values[field_name] = forged_value
        forged_binding = LegacyShadowRunnerManifestBinding.create(**values)
        strategy_bindings = list(settings.strategy_bindings)
        if field_name in {
            "strategy_registration_fingerprint",
            "executable_fingerprint",
        }:
            strategy_bindings[0] = strategy_bindings[0].model_copy(
                update={
                    (
                        "definition_fingerprint"
                        if field_name == "strategy_registration_fingerprint"
                        else "executable_fingerprint"
                    ): forged_value
                }
            )
        forged_settings = ShadowSessionSettings.model_validate(
            {
                **settings.model_dump(mode="python"),
                "strategy_bindings": strategy_bindings,
                "runner_manifest_bindings": (
                    forged_binding,
                    settings.runner_manifest_bindings[1],
                ),
            }
        )

        def consume_forged_binding(
            forged_settings: ShadowSessionSettings = forged_settings,
        ) -> None:
            forged_inputs = loader.load(
                settings=forged_settings,
                trade_date=TRADE_DATE,
                expected_export_commit=COMMIT,
            )
            next(
                runner
                for binding, runner in forged_inputs.runner_sources
                if binding.strategy_id == "n_shape"
            ).read_completion_receipt(trade_date=TRADE_DATE)

        with pytest.raises(LegacyShadowExportUnavailableError):
            consume_forged_binding()

    with pytest.raises(ValueError, match="service does not bind"):
        LegacyShadowRunnerManifestBinding.create(
            **{
                **n_shape_binding.model_dump(
                    mode="python",
                    exclude={"contract", "binding_id", "producer_service_id"},
                ),
                "producer_service_id": "strategy.growth_board_surge.v1",
            }
        )

    manifest = legacy_root / "monitor" / TRADE_DATE.isoformat() / "manifest.json"
    manifest.chmod(0o600)
    manifest.write_text("{}", encoding="utf-8")
    manifest.chmod(0o444)
    with pytest.raises(Exception, match="manifest|legacy shadow"):
        loader.load(
            settings=settings,
            trade_date=TRADE_DATE,
            expected_export_commit=COMMIT,
        )


# ---------------------------------------------------------------------------------------
# TP5 (PR-C): `_open_child_directory_at` derives its owner set from `allowed_modes`.
#
# An unprivileged test process cannot chown, so the negative branches are only reachable by
# faking the `st_uid` the predicate observes. `legacy_shadow_export` calls `os.fstat` in
# fourteen places -- the file predicate, the identity rebinds and the fsync fences all use
# it -- so the seam MUST dispatch on `stat.S_ISDIR` and leave every file observation alone;
# an indiscriminate patch would move the file predicate too and fake the result either way.
# ---------------------------------------------------------------------------------------

_ROOT_MODE = legacy_shadow_export_module._ROOT_MODE
_SESSION_MODE = legacy_shadow_export_module._SESSION_MODE
_GROUP_OTHER_WRITABLE_SESSION_MODE = 0o557
_GROUP_WRITABLE_SESSION_MODE = 0o575
_FOREIGN_UID = 999


def _stat_result_with_uid(observed: os.stat_result, uid: int) -> os.stat_result:
    fields = list(observed)
    fields[4] = uid
    return os.stat_result(
        fields,
        {
            "st_atime_ns": observed.st_atime_ns,
            "st_mtime_ns": observed.st_mtime_ns,
            "st_ctime_ns": observed.st_ctime_ns,
        },
    )


@contextmanager
def _faked_directory_owner(uid: int) -> Iterator[None]:
    """Fake `st_uid` on directory observations only; file observations stay untouched."""

    real_fstat = os.fstat
    real_stat = os.stat

    def patched_fstat(fd: int) -> os.stat_result:
        observed = real_fstat(fd)
        if stat.S_ISDIR(observed.st_mode):
            return _stat_result_with_uid(observed, uid)
        return observed

    def patched_stat(
        path: int | str | bytes | os.PathLike[str],
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        observed = real_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        if stat.S_ISDIR(observed.st_mode):
            return _stat_result_with_uid(observed, uid)
        return observed

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fstat", patched_fstat)
        patch.setattr(os, "stat", patched_stat)
        yield


def _production_filesystem_policy() -> LegacyShadowFilesystemPolicy:
    return LegacyShadowFilesystemPolicy(mode="linux-production")


def _named_allowed_modes(label: str) -> frozenset[int]:
    signed_session_modes = legacy_shadow_export_module._signed_session_modes
    return {
        "root-mode": frozenset({_ROOT_MODE}),
        "session-mode": frozenset({_SESSION_MODE}),
        "root-and-session-mode": frozenset({_ROOT_MODE, _SESSION_MODE}),
        "signed-session-modes-production": signed_session_modes(_production_filesystem_policy()),
        "signed-session-modes-offline": signed_session_modes(
            legacy_shadow_test_filesystem_policy()
        ),
    }[label]


def _named_owner_uid(label: str) -> int:
    return {"self": os.geteuid(), "root": 0, "foreign": _FOREIGN_UID}[label]


def _child_directory(parent: Path, *, name: str, mode: int) -> Path:
    child = parent / name
    child.mkdir(mode=_ROOT_MODE)
    os.chmod(child, mode)
    return child


def _directory_predicate_accepts(
    parent: Path,
    name: str,
    *,
    allowed_modes: frozenset[int],
    owner_uid: int,
) -> bool:
    parent_descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptor = -1
    try:
        with _faked_directory_owner(owner_uid):
            descriptor = legacy_shadow_export_module._open_child_directory_at(
                parent_descriptor,
                name,
                label="legacy shadow session",
                allowed_modes=allowed_modes,
            )
    except LegacyShadowExportUnavailableError:
        return False
    else:
        return True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


def test_build_phase_modes_keep_the_owner_set_pinned_to_the_effective_uid(
    tmp_path: Path,
) -> None:
    """C1 / I-TP5-1: `{0o700}` carries no session mode, so root ownership stays refused."""

    root = tmp_path / "legacy-shadow"
    root.mkdir(mode=_ROOT_MODE)
    _child_directory(root, name="building", mode=_ROOT_MODE)

    assert _directory_predicate_accepts(
        root,
        "building",
        allowed_modes=frozenset({_ROOT_MODE}),
        owner_uid=os.geteuid(),
    )
    assert not _directory_predicate_accepts(
        root,
        "building",
        allowed_modes=frozenset({_ROOT_MODE}),
        owner_uid=0,
    )


def test_session_mode_admits_the_root_signer_that_sealed_the_directory(tmp_path: Path) -> None:
    """C2: `0o555` is only ever produced by the root signer, so uid 0 must be admitted."""

    root = tmp_path / "legacy-shadow"
    root.mkdir(mode=_ROOT_MODE)
    _child_directory(root, name=TRADE_DATE.isoformat(), mode=_SESSION_MODE)

    assert _directory_predicate_accepts(
        root,
        TRADE_DATE.isoformat(),
        allowed_modes=frozenset({_SESSION_MODE}),
        owner_uid=0,
    )
    assert _directory_predicate_accepts(
        root,
        TRADE_DATE.isoformat(),
        allowed_modes=frozenset({_SESSION_MODE}),
        owner_uid=os.geteuid(),
    )


def test_session_mode_still_refuses_every_other_foreign_owner(tmp_path: Path) -> None:
    """C3: the widening admits uid 0 only; any third owner remains unsafe."""

    root = tmp_path / "legacy-shadow"
    root.mkdir(mode=_ROOT_MODE)
    _child_directory(root, name=TRADE_DATE.isoformat(), mode=_SESSION_MODE)

    assert not _directory_predicate_accepts(
        root,
        TRADE_DATE.isoformat(),
        allowed_modes=frozenset({_SESSION_MODE}),
        owner_uid=_FOREIGN_UID,
    )


def test_group_or_other_writable_directories_are_refused_even_when_allowed(
    tmp_path: Path,
) -> None:
    """C4 / I-TP5-7: the explicit `0o022` test must survive a widened `allowed_modes`."""

    root = tmp_path / "legacy-shadow"
    root.mkdir(mode=_ROOT_MODE)
    _child_directory(
        root,
        name=TRADE_DATE.isoformat(),
        mode=_GROUP_OTHER_WRITABLE_SESSION_MODE,
    )
    _child_directory(root, name="group-writable", mode=_GROUP_WRITABLE_SESSION_MODE)

    assert not _directory_predicate_accepts(
        root,
        TRADE_DATE.isoformat(),
        allowed_modes=frozenset({_SESSION_MODE}),
        owner_uid=0,
    )
    assert not _directory_predicate_accepts(
        root,
        TRADE_DATE.isoformat(),
        allowed_modes=frozenset({_SESSION_MODE, _GROUP_OTHER_WRITABLE_SESSION_MODE}),
        owner_uid=0,
    )
    assert not _directory_predicate_accepts(
        root,
        TRADE_DATE.isoformat(),
        allowed_modes=frozenset({_SESSION_MODE, _GROUP_OTHER_WRITABLE_SESSION_MODE}),
        owner_uid=os.geteuid(),
    )

    # The group half of the guard needs its own hostage. `0o557` sets only the other-write
    # bit, so narrowing `S_IWGRP | S_IWOTH` to `S_IWOTH` leaves every case above green and
    # the group half becomes dead code in the regression sense.
    assert not _directory_predicate_accepts(
        root,
        "group-writable",
        allowed_modes=frozenset({_SESSION_MODE, _GROUP_WRITABLE_SESSION_MODE}),
        owner_uid=0,
    )
    assert not _directory_predicate_accepts(
        root,
        "group-writable",
        allowed_modes=frozenset({_SESSION_MODE, _GROUP_WRITABLE_SESSION_MODE}),
        owner_uid=os.geteuid(),
    )


def test_signed_session_modes_pin_the_inputs_of_the_owner_derivation() -> None:
    """S13: the truth table's last two rows are only meaningful while these stay frozen."""

    signed_session_modes = legacy_shadow_export_module._signed_session_modes
    assert signed_session_modes(_production_filesystem_policy()) == frozenset({0o555})
    assert signed_session_modes(legacy_shadow_test_filesystem_policy()) == frozenset({0o700})


@pytest.mark.parametrize(
    ("allowed_modes_label", "directory_mode", "owner_label", "accepted"),
    (
        ("root-mode", _ROOT_MODE, "self", True),
        ("root-mode", _ROOT_MODE, "root", False),
        ("root-mode", _ROOT_MODE, "foreign", False),
        ("root-mode", _SESSION_MODE, "self", False),
        ("root-mode", _SESSION_MODE, "root", False),
        ("root-mode", _SESSION_MODE, "foreign", False),
        ("session-mode", _SESSION_MODE, "self", True),
        ("session-mode", _SESSION_MODE, "root", True),
        ("session-mode", _SESSION_MODE, "foreign", False),
        ("session-mode", _ROOT_MODE, "self", False),
        ("session-mode", _ROOT_MODE, "root", False),
        ("session-mode", _ROOT_MODE, "foreign", False),
        ("root-and-session-mode", _ROOT_MODE, "self", True),
        ("root-and-session-mode", _ROOT_MODE, "root", True),
        ("root-and-session-mode", _ROOT_MODE, "foreign", False),
        ("root-and-session-mode", _SESSION_MODE, "self", True),
        ("root-and-session-mode", _SESSION_MODE, "root", True),
        ("root-and-session-mode", _SESSION_MODE, "foreign", False),
        ("signed-session-modes-production", _SESSION_MODE, "self", True),
        ("signed-session-modes-production", _SESSION_MODE, "root", True),
        ("signed-session-modes-production", _SESSION_MODE, "foreign", False),
        ("signed-session-modes-offline", _ROOT_MODE, "self", True),
        ("signed-session-modes-offline", _ROOT_MODE, "root", False),
        ("signed-session-modes-offline", _ROOT_MODE, "foreign", False),
    ),
)
def test_directory_owner_predicate_truth_table(
    tmp_path: Path,
    allowed_modes_label: str,
    directory_mode: int,
    owner_label: str,
    accepted: bool,
) -> None:
    """C6 / I-TP5-2: the owner set follows `allowed_modes` alone, never the call site."""

    root = tmp_path / "legacy-shadow"
    root.mkdir(mode=_ROOT_MODE)
    _child_directory(root, name=TRADE_DATE.isoformat(), mode=directory_mode)

    assert (
        _directory_predicate_accepts(
            root,
            TRADE_DATE.isoformat(),
            allowed_modes=_named_allowed_modes(allowed_modes_label),
            owner_uid=_named_owner_uid(owner_label),
        )
        is accepted
    )


def test_root_sealed_session_loads_the_same_batch_as_a_publisher_owned_session(
    tmp_path: Path,
) -> None:
    """C5: the production seal (`root:root 0555`) yields a field-for-field identical batch."""

    legacy_root = (tmp_path / "legacy-shadow" / "monitor").resolve()
    publish_legacy_monitor_export(
        root=legacy_root,
        trade_date=TRADE_DATE,
        rows=_monitor_rows(),
        producer_commit=COMMIT,
        producer_version="legacy-monitor-v1",
        dependencies=_dependencies(datetime(2026, 8, 3, 7, 3, tzinfo=UTC)),
    )
    baseline = load_accepted_legacy_shadow_export(
        root=legacy_root,
        trade_date=TRADE_DATE,
        expected_source_id=None,
        expected_commit=COMMIT,
    )

    # I-TP5-1 end to end: the offline policy never carries the session mode, so a
    # root-owned session stays refused no matter how the reader is invoked.
    with _faked_directory_owner(0), pytest.raises(LegacyShadowExportUnavailableError):
        load_accepted_legacy_shadow_export(
            root=legacy_root,
            trade_date=TRADE_DATE,
            expected_source_id=None,
            expected_commit=COMMIT,
        )

    def production_signed_session_modes(policy: LegacyShadowFilesystemPolicy) -> frozenset[int]:
        return frozenset({_SESSION_MODE})

    session = legacy_root / TRADE_DATE.isoformat()
    os.chmod(session, _SESSION_MODE)
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                legacy_shadow_export_module,
                "_signed_session_modes",
                production_signed_session_modes,
            )
            with _faked_directory_owner(0):
                sealed = load_accepted_legacy_shadow_export(
                    root=legacy_root,
                    trade_date=TRADE_DATE,
                    expected_source_id=None,
                    expected_commit=COMMIT,
                )
    finally:
        os.chmod(session, _ROOT_MODE)

    compared = {field.name for field in dataclasses.fields(baseline)}
    assert compared == {
        "root",
        "session_path",
        "manifest",
        "records",
        "records_path",
        "completion_receipt",
        "completed_batch",
    }
    for name in sorted(compared):
        assert getattr(sealed, name) == getattr(baseline, name), name
