from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import textwrap
import threading
import time
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.lab_artifacts as lab_artifacts_module
from rquant.lab_artifacts import (
    LabArtifactAuthorizationError,
    LabArtifactConflictError,
    LabArtifactIndexEvidence,
    LabArtifactIntegrityError,
    LabArtifactPathError,
    LabArtifactPlatformError,
    LabArtifactRecoveryAuthority,
    LabJobArtifactCandidate,
    LabJobArtifactFile,
    LabJobArtifactManifest,
    LabJobArtifactStore,
    LabLegacyArtifactConflictError,
    LegacyArtifactIndex,
    canonical_json_bytes,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)


def _spec() -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=ResearchJobType.STRATEGY_REPLAY,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 24),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="2" * 64,
            binding_hash="3" * 64,
            audit_run_id="4" * 64,
        ),
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="5" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
        ),
        random_seed=20260725,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 7, 26, tzinfo=UTC),
        research_status="comparable",
    )


def _tables() -> dict[str, pd.DataFrame]:
    return {
        "empty": pd.DataFrame(
            {
                "trade_date": pd.Series([], dtype="datetime64[ns]"),
                "ts_code": pd.Series([], dtype="string"),
                "score": pd.Series([], dtype="float64"),
            }
        ),
        "trades": pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-07-23", "2026-07-24"]),
                "ts_code": pd.Series(["000001.SZ", "600000.SH"], dtype="string"),
                "shares": pd.Series([100, 200], dtype="int64"),
                "return": pd.Series([0.12345678901234566, -0.0], dtype="float64"),
            }
        ),
    }


def _prepare(
    store: LabJobArtifactStore,
    *,
    job_id: UUID | None = None,
) -> LabJobArtifactCandidate:
    return store.prepare_candidate(
        job_id=job_id or UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        spec=_spec(),
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={
            "decimal": Decimal("0.12345678901234567890123456789"),
            "when": datetime(2026, 7, 25, 8, 30, 1, 123456, tzinfo=UTC),
            "day": date(2026, 7, 25),
            "run_id": UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            "relative_path": Path("reports/full.md"),
            "float": 0.12345678901234566,
        },
        report_markdown="# Full report\n\nNo rounded metrics.\n",
        tables=_tables(),
    )


def _evidence(sealed):
    return LabArtifactIndexEvidence(
        job_id=sealed.manifest.job_id,
        sealed_path=sealed.path,
        manifest_hash=sealed.manifest_hash,
        complete_result_hash=sealed.manifest.complete_result_hash,
        bundle_device=sealed.device,
        bundle_inode=sealed.inode,
        file_identities=sealed.file_identities,
        indexed_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
    )


def _recovery_authority(candidate: LabJobArtifactCandidate) -> LabArtifactRecoveryAuthority:
    return LabArtifactRecoveryAuthority(
        job_id=candidate.job_id,
        spec_hash=candidate.manifest.spec_hash,
        plan_hash=candidate.manifest.plan_hash,
        adapter_id=candidate.manifest.adapter_id,
        adapter_version=candidate.manifest.adapter_version,
        result_contract_version=candidate.manifest.result_contract_version,
        code_sha=candidate.manifest.code_sha,
        dataset_snapshot=candidate.manifest.dataset_snapshot,
        expected_manifest_hash=candidate.manifest_hash,
    )


def _allow_writes(path: Path) -> None:
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    for child in path.rglob("*"):
        if child.is_dir():
            os.chmod(child, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        elif not child.is_symlink():
            os.chmod(child, stat.S_IRUSR | stat.S_IWUSR)


def _persist_forged_manifest(
    candidate: LabJobArtifactCandidate,
    *,
    files: tuple[LabJobArtifactFile, ...] | None = None,
    spec_hash: str | None = None,
    code_sha: str | None = None,
    plan_hash: str | None = None,
) -> None:
    selected_files = files or candidate.manifest.files
    selected_spec_hash = spec_hash or candidate.manifest.spec_hash
    selected_code_sha = code_sha or candidate.manifest.code_sha
    selected_plan_hash = plan_hash or candidate.manifest.plan_hash
    identity = {
        "job_id": candidate.manifest.job_id,
        "spec_hash": selected_spec_hash,
        "plan_hash": selected_plan_hash,
        "adapter_id": candidate.manifest.adapter_id,
        "adapter_version": candidate.manifest.adapter_version,
        "result_contract_version": candidate.manifest.result_contract_version,
        "code_sha": selected_code_sha,
        "dataset_snapshot": candidate.manifest.dataset_snapshot,
        "files": selected_files,
    }
    raw = json.loads(candidate.manifest.canonical_json_bytes())
    raw["spec_hash"] = selected_spec_hash
    raw["code_sha"] = selected_code_sha
    raw["plan_hash"] = selected_plan_hash
    raw["files"] = [item.model_dump(mode="json") for item in selected_files]
    raw["complete_result_hash"] = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
    manifest_bytes = json.dumps(
        raw,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    (candidate.path / "manifest.json").write_bytes(manifest_bytes)
    sums = {entry.relative_path: entry.sha256 for entry in selected_files}
    sums["manifest.json"] = hashlib.sha256(manifest_bytes).hexdigest()
    (candidate.path / "SHA256SUMS").write_text(
        "".join(f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())),
        encoding="ascii",
    )


def _resign_candidate_file(
    candidate: LabJobArtifactCandidate,
    *,
    relative_path: str,
    payload: bytes,
) -> None:
    (candidate.path / relative_path).write_bytes(payload)
    changed_files = tuple(
        item.model_copy(
            update={
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
        if item.relative_path == relative_path
        else item
        for item in candidate.manifest.files
    )
    _persist_forged_manifest(candidate, files=changed_files)


def test_prepare_verify_seal_and_idempotently_reuse_complete_bundle(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    verified = store.verify_candidate(candidate)
    sealed = store.seal_candidate(candidate)
    reused_candidate = _prepare(store)
    reused = store.seal_candidate(reused_candidate)

    assert verified.complete_result_hash == sealed.manifest.complete_result_hash
    assert reused.path == sealed.path
    assert reused.manifest_hash == sealed.manifest_hash
    assert reused.reused_existing is True
    assert not reused_candidate.path.exists()
    assert any(item.status == "quarantined" for item in store.list_candidate_recovery())
    assert (sealed.path / "spec.json").read_text() == _spec().canonical_json()
    metrics = json.loads((sealed.path / "metrics.json").read_text())
    assert metrics["decimal"] == {"$decimal": "0.12345678901234567890123456789"}
    assert metrics["float"] == {"$float": (0.12345678901234566).hex()}
    assert tuple(item.relative_path for item in sealed.manifest.files) == tuple(
        sorted(item.relative_path for item in sealed.manifest.files)
    )
    assert all(
        not (child.stat().st_mode & 0o222) for child in sealed.path.rglob("*") if child.is_file()
    )
    assert store.verify_sealed(sealed.path).manifest == sealed.manifest


def test_candidate_creation_path_swap_never_writes_external_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    external = tmp_path / "external"
    external.mkdir()
    displaced = store.candidates_root / "displaced-construction"
    swapped = False

    def swap_after_directory_bound(candidate_name: str, _descriptor: int) -> None:
        nonlocal swapped
        candidate_path = store.candidates_root / candidate_name
        os.rename(candidate_path, displaced)
        candidate_path.symlink_to(external, target_is_directory=True)
        swapped = True

    monkeypatch.setattr(
        store,
        "_after_candidate_directory_bound",
        swap_after_directory_bound,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="candidate.*identity"):
        _prepare(store)

    assert swapped is True
    assert list(external.iterdir()) == []
    assert list(displaced.iterdir()) == []


def test_candidate_creation_parent_swap_stops_before_writing_displaced_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    external = tmp_path / "external-parent"
    external.mkdir()
    displaced_root = tmp_path / "displaced-candidates"

    def swap_parent_after_directory_bound(_candidate_name: str, _descriptor: int) -> None:
        os.rename(store.candidates_root, displaced_root)
        store.candidates_root.symlink_to(external, target_is_directory=True)

    monkeypatch.setattr(
        store,
        "_after_candidate_directory_bound",
        swap_parent_after_directory_bound,
    )

    with pytest.raises(LabArtifactIntegrityError, match="candidate.*identity"):
        _prepare(store)

    displaced_candidate = next(displaced_root.iterdir())
    assert list(external.iterdir()) == []
    assert list(displaced_candidate.iterdir()) == []


def test_artifact_root_swap_fails_closed_without_writing_external_tree(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = LabJobArtifactStore(root)
    displaced = tmp_path / "displaced-artifacts"
    external = tmp_path / "external-artifacts"
    external.mkdir(mode=0o700)
    for name in ("candidates", "sealed", "quarantine", "seal-intents"):
        (external / name).mkdir(mode=0o700)
    os.rename(root, displaced)
    root.symlink_to(external, target_is_directory=True)

    with pytest.raises(LabArtifactIntegrityError, match="managed.*identity"):
        _prepare(store)

    assert all(list((external / name).iterdir()) == [] for name in os.listdir(external))


def test_artifact_root_rejects_ancestor_symlink_without_external_writes(tmp_path: Path) -> None:
    external_container = tmp_path / "external" / "container"
    external_container.mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(external_container.parent, target_is_directory=True)

    with pytest.raises((LabArtifactPathError, LabArtifactIntegrityError)):
        LabJobArtifactStore(alias / "container" / "artifacts")

    assert list(external_container.iterdir()) == []


def test_same_job_with_different_result_conflicts_without_clobber(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    first = store.seal_candidate(_prepare(store))
    changed = store.prepare_candidate(
        job_id=first.manifest.job_id,
        spec=_spec(),
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={"mean_return": Decimal("9.99")},
        report_markdown="# changed\n",
        tables=_tables(),
    )

    with pytest.raises(LabArtifactConflictError, match="different sealed result"):
        store.seal_candidate(changed)

    assert store.verify_sealed(first.path).manifest_hash == first.manifest_hash
    assert changed.path.exists()


def test_existing_sealed_inode_swap_does_not_return_stale_or_quarantine_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    candidate = _prepare(store)
    replacement = tmp_path / "replacement-sealed"
    displaced = tmp_path / "displaced-sealed"
    shutil.copytree(sealed.path, replacement)
    swapped = False

    def swap_after_bound(_bound: object, _sealed: object) -> None:
        nonlocal swapped
        os.chmod(sealed.path, 0o700)
        os.chmod(replacement, 0o700)
        os.rename(sealed.path, displaced)
        os.rename(replacement, sealed.path)
        swapped = True

    monkeypatch.setattr(store, "_after_existing_sealed_bound", swap_after_bound, raising=False)

    with pytest.raises(LabArtifactIntegrityError, match="sealed.*identity|bound.*identity"):
        store.seal_candidate(candidate)

    assert swapped is True
    assert candidate.path.exists()
    assert not any(
        item.status == "quarantined" and item.job_id == candidate.job_id
        for item in store.list_candidate_recovery()
    )


def test_atomic_publish_never_replaces_racing_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    target = store.sealed_root / candidate.job_id.hex
    original = lab_artifacts_module._rename_noreplace
    reserved = False

    def reserve_then_publish(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        nonlocal reserved
        os.mkdir(destination_name, mode=0o700, dir_fd=destination_parent)
        reserved = True
        original(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(
        store,
        "_atomic_publish_noreplace",
        reserve_then_publish,
        raising=False,
    )

    with pytest.raises(LabArtifactConflictError, match="atomically sealed"):
        store.seal_candidate(candidate)

    assert reserved is True
    assert target.is_dir()
    assert list(target.iterdir()) == []
    assert candidate.path.is_dir()


def test_atomic_publish_fails_closed_on_unsupported_platform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_parent = tmp_path / "source"
    destination_parent = tmp_path / "destination"
    source_parent.mkdir()
    destination_parent.mkdir()
    (source_parent / "bundle").mkdir()
    source_descriptor = os.open(source_parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    destination_descriptor = os.open(
        destination_parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    monkeypatch.setattr(lab_artifacts_module.sys, "platform", "unsupported-test-os")
    try:
        with pytest.raises(LabArtifactPlatformError, match="unsupported"):
            lab_artifacts_module._rename_noreplace(
                source_descriptor,
                "bundle",
                destination_descriptor,
                "sealed",
            )
    finally:
        os.close(source_descriptor)
        os.close(destination_descriptor)

    assert (source_parent / "bundle").is_dir()
    assert not (destination_parent / "sealed").exists()


def test_atomic_publish_race_reuses_identical_completed_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    target = store.sealed_root / candidate.job_id.hex
    original = lab_artifacts_module._rename_noreplace

    def publish_identical_then_race(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        shutil.copytree(candidate.path, target)
        for path in target.rglob("*"):
            os.chmod(path, 0o500 if path.is_dir() else 0o400)
        os.chmod(target, 0o500)
        original(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(store, "_atomic_publish_noreplace", publish_identical_then_race)

    reused = store.seal_candidate(candidate)

    assert reused.reused_existing is True
    assert reused.manifest_hash == candidate.manifest_hash
    assert not candidate.path.exists()
    assert store.verify_sealed(target).manifest_hash == candidate.manifest_hash


def test_seal_rejects_same_job_candidate_path_swap_after_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate_a = _prepare(store)
    candidate_b = store.prepare_candidate(
        job_id=candidate_a.job_id,
        spec=_spec(),
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={"result": "candidate-b"},
        report_markdown="# candidate B\n",
        tables=_tables(),
    )
    displaced_a = tmp_path / "displaced-a"
    original_verify = store.verify_candidate

    def verify_then_swap(
        candidate: LabJobArtifactCandidate,
        *,
        allow_interrupted_seal: bool = False,
    ) -> LabJobArtifactManifest:
        manifest = original_verify(
            candidate,
            allow_interrupted_seal=allow_interrupted_seal,
        )
        os.rename(candidate_a.path, displaced_a)
        os.rename(candidate_b.path, candidate_a.path)
        return manifest

    monkeypatch.setattr(store, "verify_candidate", verify_then_swap)

    with pytest.raises(LabArtifactIntegrityError, match="identity changed"):
        store.seal_candidate(candidate_a)

    assert not (store.sealed_root / candidate_a.job_id.hex).exists()
    assert (candidate_a.path / "report.md").read_text() == "# candidate B\n"
    assert (displaced_a / "report.md").read_text() != "# candidate B\n"


def test_fd_bound_fchmod_race_never_changes_external_symlink_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    report = candidate.path / "report.md"
    displaced = tmp_path / "original-report.md"
    external = tmp_path / "external.md"
    external.write_text("external", encoding="utf-8")
    os.chmod(external, 0o640)
    external_mode = stat.S_IMODE(external.stat().st_mode)
    original_fchmod = lab_artifacts_module.os.fchmod
    swapped = False

    def swap_path_before_fchmod(descriptor: int, mode: int) -> None:
        nonlocal swapped
        if mode == 0o400 and not swapped:
            swapped = True
            os.rename(report, displaced)
            report.symlink_to(external)
        original_fchmod(descriptor, mode)

    monkeypatch.setattr(lab_artifacts_module.os, "fchmod", swap_path_before_fchmod)

    with pytest.raises(LabArtifactIntegrityError):
        store.seal_candidate(candidate)

    assert swapped is True
    assert stat.S_IMODE(external.stat().st_mode) == external_mode


def test_recover_candidate_rejects_recovery_record_inode_replacement(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate_a = _prepare(store)
    record = store.list_candidate_recovery()[0]
    candidate_b = _prepare(store)
    displaced_a = tmp_path / "displaced-recovery-a"
    os.rename(candidate_a.path, displaced_a)
    os.rename(candidate_b.path, candidate_a.path)

    with pytest.raises(LabArtifactIntegrityError, match="recovery.*identity"):
        store.recover_candidate(record)


@pytest.mark.parametrize("target", ["manifest.json", "SHA256SUMS", "tables/trades.parquet"])
def test_verify_rejects_tampered_manifest_sums_or_parquet(
    tmp_path: Path,
    target: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    _allow_writes(sealed.path)
    path = sealed.path / target
    with path.open("ab") as stream:
        stream.write(b"tampered")

    with pytest.raises(LabArtifactIntegrityError):
        store.verify_sealed(sealed.path)


def test_verify_sealed_rejects_parquet_replaced_after_descriptor_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    target = sealed.path / "tables" / "trades.parquet"
    displaced = tmp_path / "original-trades.parquet"
    swapped = False

    def replace_after_read(relative_path: str, _bound: object) -> None:
        nonlocal swapped
        if relative_path == "tables/trades.parquet" and not swapped:
            os.chmod(target.parent, 0o700)
            os.rename(target, displaced)
            target.write_bytes(b"corrupt replacement")
            os.chmod(target, 0o400)
            swapped = True

    monkeypatch.setattr(store, "_after_bound_file_read", replace_after_read, raising=False)

    with pytest.raises(LabArtifactIntegrityError, match="identity|changed|permissions"):
        store.verify_sealed(sealed.path)

    assert swapped is True


def test_verify_candidate_rejects_report_symlink_replaced_after_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    report = candidate.path / "report.md"
    displaced = tmp_path / "original-report.md"
    swapped = False

    def symlink_after_read(relative_path: str, _bound: object) -> None:
        nonlocal swapped
        if relative_path == "report.md" and not swapped:
            os.rename(report, displaced)
            report.symlink_to(displaced)
            swapped = True

    monkeypatch.setattr(store, "_after_bound_file_read", symlink_after_read, raising=False)

    with pytest.raises(LabArtifactIntegrityError, match="identity|changed|unsafe"):
        store.verify_candidate(candidate)

    assert swapped is True


@pytest.mark.parametrize("case", ["missing", "extra", "symlink", "hardlink"])
def test_verify_rejects_incomplete_or_unsafe_inventory(tmp_path: Path, case: str) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    path = candidate.path / "report.md"
    if case == "missing":
        path.unlink()
    elif case == "extra":
        (candidate.path / "extra.txt").write_text("unexpected")
    elif case == "symlink":
        path.unlink()
        path.symlink_to(candidate.path / "spec.json")
    else:
        external = tmp_path / "external.md"
        os.link(path, external)

    with pytest.raises(LabArtifactIntegrityError):
        store.verify_candidate(candidate)


def test_candidate_recovery_and_logical_quarantine_never_delete_bytes(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    _prepare(store)
    restarted = LabJobArtifactStore(tmp_path / "artifacts")

    records = restarted.list_candidate_recovery()
    candidate = restarted._candidate_from_path(records[0].path)
    recovered = restarted.recover_candidate(
        records[0],
        authority=_recovery_authority(candidate),
    )

    assert records[0].status == "needs_authority"
    assert recovered.path.exists()
    second = _prepare(restarted, job_id=uuid4())
    quarantined = restarted.quarantine_candidate(second, reason="operator cleanup")
    assert quarantined.status == "quarantined"
    assert quarantined.path.exists()
    assert (quarantined.path / "report.md").read_bytes() == (
        b"# Full report\n\nNo rounded metrics.\n"
    )


def test_candidate_recovery_without_intent_requires_external_authority(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)

    assert record.status == "needs_authority"
    with pytest.raises(LabArtifactAuthorizationError, match="authority"):
        store.recover_candidate(record)

    sealed = store.recover_candidate(record, authority=_recovery_authority(candidate))
    assert sealed.manifest_hash == candidate.manifest_hash


def test_forged_recoverable_status_cannot_bypass_external_authority(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)
    forged = record.model_copy(update={"status": "recoverable"})

    with pytest.raises(LabArtifactAuthorizationError, match="authority"):
        store.recover_candidate(forged)


def test_resigned_candidate_cannot_recover_without_matching_external_authority(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    original_authority = _recovery_authority(candidate)
    _allow_writes(candidate.path)
    changed_report = b"# Re-signed report\n\nDifferent but internally consistent.\n"
    (candidate.path / "report.md").write_bytes(changed_report)
    changed_files = tuple(
        item.model_copy(
            update={
                "size": len(changed_report),
                "sha256": hashlib.sha256(changed_report).hexdigest(),
            }
        )
        if item.relative_path == "report.md"
        else item
        for item in candidate.manifest.files
    )
    _persist_forged_manifest(candidate, files=changed_files, plan_hash="9" * 64)
    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)

    with pytest.raises(LabArtifactAuthorizationError, match="authority"):
        store.recover_candidate(record)
    with pytest.raises(LabArtifactAuthorizationError, match="authority"):
        store.recover_candidate(record, authority=original_authority)


def test_invalid_crash_candidate_can_be_identity_bound_and_quarantined(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    extra = candidate.path / "partial.tmp"
    extra.write_bytes(b"incomplete but retained")

    record = store.list_candidate_recovery()[0]
    quarantined = store.quarantine_recovery_record(
        record,
        reason="failed candidate construction",
    )

    assert record.status == "invalid"
    assert quarantined.status == "quarantined"
    assert (quarantined.path / "partial.tmp").read_bytes() == b"incomplete but retained"
    assert not candidate.path.exists()


@pytest.mark.parametrize(
    ("relative_path", "mode"),
    [(".", 0o755), ("tables", 0o755), ("report.md", 0o644)],
)
def test_candidate_verification_requires_private_permissions(
    tmp_path: Path,
    relative_path: str,
    mode: int,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    target = candidate.path if relative_path == "." else candidate.path / relative_path
    os.chmod(target, mode)

    records = store.list_candidate_recovery()

    assert records[0].status == "invalid"
    assert "permissions" in (records[0].reason or "")


def test_interrupted_seal_after_atomic_rename_can_be_explicitly_recovered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    def crash_after_rename(_bound: object) -> None:
        os.chmod(store.sealed_root / candidate.job_id.hex / "report.md", 0o600)
        raise OSError("simulated crash after rename")

    monkeypatch.setattr(store, "_finalize_bound_directories", crash_after_rename)
    with pytest.raises(OSError, match="simulated crash"):
        store.seal_candidate(candidate)

    published = store.sealed_root / candidate.job_id.hex
    assert published.exists()
    assert published.stat().st_mode & stat.S_IWUSR
    restarted = LabJobArtifactStore(tmp_path / "artifacts")

    recovered = restarted.recover_interrupted_seal(published)

    assert recovered.path == published
    assert restarted.verify_sealed(published).manifest_hash == candidate.manifest_hash


def test_interrupted_seal_rejects_same_bytes_with_replaced_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    def crash_after_rename(_bound: object) -> None:
        raise OSError("simulated directory metadata crash")

    monkeypatch.setattr(store, "_finalize_bound_directories", crash_after_rename)
    with pytest.raises(OSError):
        store.seal_candidate(candidate)
    published = store.sealed_root / candidate.job_id.hex
    report = published / "report.md"
    original_bytes = report.read_bytes()
    displaced = tmp_path / "sealed-original-report.md"
    os.rename(report, displaced)
    report.write_bytes(original_bytes)
    os.chmod(report, 0o400)

    with pytest.raises(LabArtifactIntegrityError, match="seal intent.*identity"):
        LabJobArtifactStore(tmp_path / "artifacts").recover_interrupted_seal(published)


def test_seal_intent_replacement_after_binding_never_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    displaced = tmp_path / "original-seal-intent.json"
    swapped = False

    def replace_bound_intent(_bound: object) -> None:
        nonlocal swapped
        path = store.seal_intents_root / f"{candidate.job_id.hex}.json"
        os.rename(path, displaced)
        path.write_text("{}", encoding="utf-8")
        os.chmod(path, 0o600)
        swapped = True

    monkeypatch.setattr(store, "_after_seal_intent_bound", replace_bound_intent, raising=False)

    with pytest.raises(LabArtifactIntegrityError, match="seal intent.*identity|bound.*identity"):
        store.seal_candidate(candidate)

    assert swapped is True
    assert not (store.sealed_root / candidate.job_id.hex).exists()
    assert candidate.path.exists()


def test_seal_intent_replacement_during_freeze_never_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    original = store._seal_bound_files
    displaced = tmp_path / "freeze-seal-intent.json"

    def freeze_then_replace(bound: object) -> None:
        original(bound)  # type: ignore[arg-type]
        path = store.seal_intents_root / f"{candidate.job_id.hex}.json"
        os.rename(path, displaced)
        path.write_text("{}", encoding="utf-8")
        os.chmod(path, 0o600)

    monkeypatch.setattr(store, "_seal_bound_files", freeze_then_replace)

    with pytest.raises(LabArtifactIntegrityError, match="seal intent.*identity"):
        store.seal_candidate(candidate)

    assert not (store.sealed_root / candidate.job_id.hex).exists()
    assert candidate.path.exists()


@pytest.mark.parametrize("payload", [b"", b"{", b'{"partial":true}'])
def test_orphaned_seal_intent_temp_is_logically_isolated_before_retry(
    tmp_path: Path,
    payload: bytes,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    temporary = store.seal_intents_root / (f".{candidate.job_id.hex}.{uuid4().hex}.intent.tmp")
    temporary.write_bytes(payload)
    os.chmod(temporary, 0o600)

    sealed = store.seal_candidate(candidate)

    assert sealed.manifest_hash == candidate.manifest_hash
    assert not temporary.exists()
    intent_quarantine = store.root / "seal-intents-quarantine"
    assert any(item.read_bytes() == payload for item in intent_quarantine.iterdir())


def test_orphaned_seal_intent_temp_requires_exact_private_permissions(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    temporary = store.seal_intents_root / (f".{candidate.job_id.hex}.{uuid4().hex}.intent.tmp")
    temporary.write_bytes(b"{")
    os.chmod(temporary, 0o644)

    with pytest.raises(LabArtifactIntegrityError, match="permissions|unsafe"):
        store.seal_candidate(candidate)

    assert candidate.path.exists()
    assert temporary.exists()


def test_crash_before_seal_intent_publish_leaves_recoverable_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    def crash_before_publish(_descriptor: int, _name: str) -> None:
        raise OSError("crash before intent publish")

    monkeypatch.setattr(store, "_after_seal_intent_temp_fsync", crash_before_publish)

    with pytest.raises(OSError, match="before intent publish"):
        store.seal_candidate(candidate)

    assert not (store.seal_intents_root / f"{candidate.job_id.hex}.json").exists()
    assert candidate.path.exists()
    assert any(
        item.name.startswith(f".{candidate.job_id.hex}.")
        for item in store.seal_intents_root.iterdir()
    )

    restarted = LabJobArtifactStore(store.root)
    recovered = restarted.seal_candidate(restarted._candidate_from_path(candidate.path))
    assert recovered.manifest_hash == candidate.manifest_hash


def test_crash_after_seal_intent_publish_reuses_complete_final_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    def crash_after_publish(_bound: object) -> None:
        raise OSError("crash after intent publish")

    monkeypatch.setattr(store, "_after_seal_intent_publish", crash_after_publish)

    with pytest.raises(OSError, match="after intent publish"):
        store.seal_candidate(candidate)

    final_intent = store.seal_intents_root / f"{candidate.job_id.hex}.json"
    assert final_intent.is_file()
    assert candidate.path.exists()

    restarted = LabJobArtifactStore(store.root)
    recovered = restarted.seal_candidate(
        restarted._candidate_from_path(candidate.path, allow_interrupted_seal=True)
    )
    assert recovered.manifest_hash == candidate.manifest_hash


@pytest.mark.parametrize("payload", [b"", b"{", b'{"schema_version":1'])
def test_torn_final_intent_requires_authority_then_is_quarantined_and_rebuilt(
    tmp_path: Path,
    payload: bytes,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    intent = store.seal_intents_root / f"{candidate.job_id.hex}.json"
    intent.write_bytes(payload)
    os.chmod(intent, 0o600)
    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)

    assert record.status == "needs_authority"
    with pytest.raises(LabArtifactAuthorizationError, match="authority"):
        store.recover_candidate(record)

    sealed = store.recover_candidate(
        record,
        authority=_recovery_authority(candidate),
    )

    assert sealed.manifest_hash == candidate.manifest_hash
    assert any(
        item.read_bytes() == payload for item in (store.root / "seal-intents-quarantine").iterdir()
    )


def test_torn_intent_with_partially_frozen_candidate_recovers_with_authority(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    frozen = candidate.path / "report.md"
    os.chmod(frozen, 0o400)
    intent = store.seal_intents_root / f"{candidate.job_id.hex}.json"
    intent.write_bytes(b"{")
    os.chmod(intent, 0o600)

    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)

    assert record.status == "needs_authority"
    assert "recoverable_torn" in (record.reason or "")
    with pytest.raises(LabArtifactAuthorizationError, match="authority"):
        store.recover_candidate(record)

    sealed = store.recover_candidate(record, authority=_recovery_authority(candidate))

    assert sealed.manifest_hash == candidate.manifest_hash
    assert stat.S_IMODE((sealed.path / "report.md").stat().st_mode) == 0o400


@pytest.mark.parametrize(
    "boundary",
    ["before_directory_chmod", "after_tables_fsync", "before_bundle_fsync"],
)
def test_interrupted_seal_recovers_distinct_directory_metadata_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    def crash_at_boundary(bound: object) -> None:
        if boundary in {"after_tables_fsync", "before_bundle_fsync"}:
            os.fchmod(bound.tables_descriptor, 0o500)  # type: ignore[attr-defined]
            os.fsync(bound.tables_descriptor)  # type: ignore[attr-defined]
        if boundary == "before_bundle_fsync":
            os.fchmod(bound.bundle_descriptor, 0o500)  # type: ignore[attr-defined]
        raise OSError(boundary)

    monkeypatch.setattr(store, "_finalize_bound_directories", crash_at_boundary)
    with pytest.raises(OSError, match=boundary):
        store.seal_candidate(candidate)

    published = store.sealed_root / candidate.job_id.hex
    recovered = LabJobArtifactStore(tmp_path / "artifacts").recover_interrupted_seal(published)

    assert stat.S_IMODE(recovered.path.stat().st_mode) == 0o500
    assert stat.S_IMODE((recovered.path / "tables").stat().st_mode) == 0o500
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o400
        for path in recovered.path.rglob("*")
        if path.is_file()
    )


def test_seal_fsyncs_each_fd_after_fchmod_0400(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    events: list[tuple[str, int, int | None]] = []
    original_fchmod = lab_artifacts_module.os.fchmod
    original_fsync = lab_artifacts_module.os.fsync

    def record_fchmod(descriptor: int, mode: int) -> None:
        events.append(("fchmod", descriptor, mode))
        original_fchmod(descriptor, mode)

    def record_fsync(descriptor: int) -> None:
        events.append(("fsync", descriptor, None))
        original_fsync(descriptor)

    monkeypatch.setattr(lab_artifacts_module.os, "fchmod", record_fchmod)
    monkeypatch.setattr(lab_artifacts_module.os, "fsync", record_fsync)

    store.seal_candidate(candidate)

    file_chmods = [
        (index, descriptor)
        for index, (operation, descriptor, mode) in enumerate(events)
        if operation == "fchmod" and mode == 0o400
    ]
    assert len(file_chmods) == len(candidate.file_identities)
    for chmod_index, descriptor in file_chmods:
        assert any(
            operation == "fsync" and later_descriptor == descriptor
            for operation, later_descriptor, _mode in events[chmod_index + 1 :]
        )


@pytest.mark.parametrize("freeze_count", [1, 7])
def test_candidate_with_seal_intent_recovers_after_partial_file_freeze(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    freeze_count: int,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    expected_count = len(candidate.file_identities)
    count = min(freeze_count, expected_count)
    original = store._seal_bound_files

    def freeze_then_crash(bound: object) -> None:
        files = bound.files  # type: ignore[attr-defined]
        for relative_path in sorted(files)[:count]:
            descriptor = files[relative_path].descriptor
            os.fchmod(descriptor, 0o400)
            os.fsync(descriptor)
        raise OSError(f"crash after {count} file freezes")

    monkeypatch.setattr(store, "_seal_bound_files", freeze_then_crash)
    with pytest.raises(OSError, match="file freezes"):
        store.seal_candidate(candidate)
    monkeypatch.setattr(store, "_seal_bound_files", original)

    restarted = LabJobArtifactStore(tmp_path / "artifacts")
    records = restarted.list_candidate_recovery()
    record = next(item for item in records if item.path == candidate.path)

    assert record.status == "recoverable"
    sealed = restarted.recover_candidate(record)
    assert restarted.verify_sealed(sealed.path).manifest_hash == candidate.manifest_hash


def test_candidate_mixed_permissions_without_seal_intent_remains_invalid(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    os.chmod(candidate.path / "report.md", 0o400)

    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)

    assert record.status == "invalid"
    assert "permissions" in (record.reason or "")


def test_quarantine_race_never_reports_a_different_source_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    replacement = _prepare(store, job_id=uuid4())
    displaced = tmp_path / "quarantine-original"
    original = lab_artifacts_module._rename_noreplace

    def swap_then_quarantine(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        os.rename(candidate.path, displaced)
        os.rename(replacement.path, candidate.path)
        original(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(
        store,
        "_atomic_quarantine_noreplace",
        swap_then_quarantine,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="quarantine.*identity"):
        store.quarantine_candidate(candidate, reason="race test")


def test_quarantine_success_preserves_original_inode(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)

    record = store.quarantine_candidate(candidate, reason="operator isolation")

    assert (record.device, record.inode) == (candidate.device, candidate.inode)
    assert (record.path.stat().st_dev, record.path.stat().st_ino) == (
        candidate.device,
        candidate.inode,
    )


def test_process_crash_after_first_file_freeze_is_recoverable(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LabArtifactRecoveryAuthority, LabJobArtifactStore

        store = LabJobArtifactStore(Path(sys.argv[1]))
        record = next(
            item
            for item in store.list_candidate_recovery()
            if item.status == "needs_authority"
        )
        authority = LabArtifactRecoveryAuthority.model_validate_json(sys.argv[2])

        def freeze_one_then_exit(bound):
            item = bound.files[sorted(bound.files)[0]]
            os.fchmod(item.descriptor, 0o400)
            os.fsync(item.descriptor)
            os._exit(86)

        store._seal_bound_files = freeze_one_then_exit
        store.recover_candidate(record, authority=authority)
        """
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(store.root),
            _recovery_authority(candidate).model_dump_json(),
        ],
        check=False,
        cwd=Path(__file__).parents[2],
        env=os.environ.copy(),
    )

    assert completed.returncode == 86
    restarted = LabJobArtifactStore(store.root)
    record = next(
        item for item in restarted.list_candidate_recovery() if item.path == candidate.path
    )
    assert record.status == "recoverable"
    assert restarted.recover_candidate(record).manifest_hash == candidate.manifest_hash


@pytest.mark.parametrize(
    ("relative_path", "mode"),
    [("report.md", 0o444), ("tables", 0o555), (".", 0o555)],
)
def test_sealed_bundle_requires_exact_file_and_directory_permissions(
    tmp_path: Path,
    relative_path: str,
    mode: int,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    target = sealed.path if relative_path == "." else sealed.path / relative_path
    os.chmod(target, mode)

    with pytest.raises(LabArtifactIntegrityError, match="permissions"):
        store.verify_sealed(sealed.path)


def test_zip_export_is_byte_identical_and_requires_matching_index_evidence(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    evidence = _evidence(sealed)

    first = store.export_deterministic_zip(sealed.path, evidence, tmp_path / "one.zip")
    second = store.export_deterministic_zip(sealed.path, evidence, tmp_path / "two.zip")

    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        assert archive.namelist() == sorted(archive.namelist())
        assert all(item.date_time == (1980, 1, 1, 0, 0, 0) for item in archive.infolist())
    wrong = evidence.model_copy(update={"manifest_hash": "f" * 64})
    with pytest.raises(LabArtifactAuthorizationError):
        store.export_deterministic_zip(sealed.path, wrong, tmp_path / "denied.zip")
    with pytest.raises(LabArtifactAuthorizationError):
        store.export_deterministic_zip(
            _prepare(store, job_id=uuid4()).path,
            evidence,
            tmp_path / "candidate.zip",
        )


def test_zip_export_rechecks_bytes_after_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    evidence = _evidence(sealed)
    original = store._authorize_export

    def tamper_after_authorization(verified, supplied) -> None:
        original(verified, supplied)
        _allow_writes(verified.path)
        (verified.path / "report.md").write_text("changed", encoding="utf-8")

    monkeypatch.setattr(store, "_authorize_export", tamper_after_authorization)

    with pytest.raises(LabArtifactIntegrityError, match="export bytes conflict"):
        store.export_deterministic_zip(sealed.path, evidence, tmp_path / "tampered.zip")


def test_zip_export_rejects_bundle_inode_swap_after_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    evidence = _evidence(sealed)
    displaced = store.sealed_root / "original-sealed"
    replacement = store.sealed_root / "replacement-sealed"
    shutil.copytree(sealed.path, replacement)
    original = store._authorize_export

    def swap_after_authorization(verified, supplied) -> None:
        original(verified, supplied)
        os.chmod(sealed.path, 0o700)
        os.chmod(replacement, 0o700)
        os.rename(sealed.path, displaced)
        os.rename(replacement, sealed.path)

    monkeypatch.setattr(store, "_authorize_export", swap_after_authorization)

    with pytest.raises((LabArtifactAuthorizationError, LabArtifactIntegrityError)):
        store.export_deterministic_zip(sealed.path, evidence, tmp_path / "swapped.zip")


def test_zip_destination_reservation_is_never_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    destination = tmp_path / "reserved.zip"
    original = lab_artifacts_module._rename_noreplace

    def reserve_then_publish(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        descriptor = os.open(
            destination_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=destination_parent,
        )
        os.write(descriptor, b"reservation")
        os.close(descriptor)
        original(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(
        store,
        "_atomic_zip_publish_noreplace",
        reserve_then_publish,
        raising=False,
    )

    with pytest.raises(LabArtifactConflictError):
        store.export_deterministic_zip(sealed.path, _evidence(sealed), destination)

    assert destination.read_bytes() == b"reservation"


def test_public_verified_sealed_binding_keeps_transaction_evidence_bound(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    indexed_at = datetime(2026, 7, 25, 10, tzinfo=UTC)

    with store.bind_verified_sealed(sealed.path, indexed_at=indexed_at) as binding:
        assert isinstance(binding, lab_artifacts_module.LabVerifiedSealedBinding)
        assert binding.sealed.manifest_hash == sealed.manifest_hash
        assert binding.evidence == _evidence(sealed).model_copy(update={"indexed_at": indexed_at})
        assert all("descriptor" not in name for name in type(binding).model_fields)


def test_public_verified_sealed_binding_rechecks_every_inode_on_exit(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    report = sealed.path / "report.md"
    displaced = tmp_path / "bound-report.md"

    with (
        pytest.raises(LabArtifactIntegrityError, match="identity|changed"),
        store.bind_verified_sealed(
            sealed.path,
            indexed_at=datetime(2026, 7, 25, 10, tzinfo=UTC),
        ),
    ):
        os.chmod(sealed.path, 0o700)
        os.rename(report, displaced)
        report.write_bytes(displaced.read_bytes())
        os.chmod(report, 0o400)


def test_public_verified_binding_preserves_caller_and_integrity_errors(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    report = sealed.path / "report.md"
    displaced = tmp_path / "caller-error-report.md"

    with (
        pytest.raises(ExceptionGroup) as captured,
        store.bind_verified_sealed(
            sealed.path,
            indexed_at=datetime(2026, 7, 25, 10, tzinfo=UTC),
        ),
    ):
        os.chmod(sealed.path, 0o700)
        os.rename(report, displaced)
        report.write_bytes(displaced.read_bytes())
        os.chmod(report, 0o400)
        raise RuntimeError("caller transaction failed")

    flattened = list(captured.value.exceptions)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in flattened)


def test_zip_destination_rejects_ancestor_symlink_without_external_writes(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    external_container = tmp_path / "zip-external" / "container"
    external_container.mkdir(parents=True)
    alias = tmp_path / "zip-alias"
    alias.symlink_to(external_container.parent, target_is_directory=True)

    with pytest.raises((LabArtifactPathError, LabArtifactIntegrityError, OSError)):
        store.export_deterministic_zip(
            sealed.path,
            _evidence(sealed),
            alias / "container" / "exports" / "result.zip",
        )

    assert list(external_container.iterdir()) == []


def test_export_does_not_chmod_existing_caller_directory(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    output = tmp_path / "caller-owned"
    output.mkdir(mode=0o755)
    before_mode = stat.S_IMODE(output.stat().st_mode)

    store.export_deterministic_zip(
        sealed.path,
        _evidence(sealed),
        output / "result.zip",
    )

    assert stat.S_IMODE(output.stat().st_mode) == before_mode


def test_managed_artifact_directories_require_exact_private_permissions(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    os.chmod(store.sealed_root, 0o777)

    with pytest.raises(LabArtifactIntegrityError, match="permissions"):
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="n-shape",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"result": pd.DataFrame({"x": [1]})},
        )


def test_secure_directory_creation_fsyncs_parent_then_new_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "durable" / "nested"
    events: list[tuple[str, tuple[int, int]]] = []
    real_mkdir = lab_artifacts_module.os.mkdir
    real_fsync = lab_artifacts_module.os.fsync

    def record_mkdir(
        name: str,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        assert dir_fd is not None
        parent = os.fstat(dir_fd)
        real_mkdir(name, mode=mode, dir_fd=dir_fd)
        child = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        events.append(("mkdir", (parent.st_dev, parent.st_ino)))
        events.append(("child", (child.st_dev, child.st_ino)))

    def record_fsync(descriptor: int) -> None:
        observed = os.fstat(descriptor)
        events.append(("fsync", (observed.st_dev, observed.st_ino)))
        real_fsync(descriptor)

    monkeypatch.setattr(lab_artifacts_module.os, "mkdir", record_mkdir)
    monkeypatch.setattr(lab_artifacts_module.os, "fsync", record_fsync)

    descriptor = lab_artifacts_module._secure_open_directory(target, create=True)
    os.close(descriptor)

    mkdir_indexes = [index for index, event in enumerate(events) if event[0] == "mkdir"]
    assert mkdir_indexes
    for index in mkdir_indexes:
        parent_identity = events[index][1]
        child_identity = events[index + 1][1]
        subsequent_fsyncs = [event[1] for event in events[index + 2 :] if event[0] == "fsync"]
        assert subsequent_fsyncs[:2] == [parent_identity, child_identity]


def test_canonical_json_is_exact_for_supported_values_and_rejects_invalid_values() -> None:
    payload = {
        "date": date(2026, 7, 25),
        "datetime": datetime(2026, 7, 25, 8, 1, 2, 3, tzinfo=UTC),
        "decimal": Decimal("-0.00000000000000000001"),
        "path": Path("a/b"),
        "uuid": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    }
    first = canonical_json_bytes(payload)
    second = canonical_json_bytes(dict(reversed(tuple(payload.items()))))

    assert first == second
    assert b'"$decimal":"-0.00000000000000000001"' in first
    for invalid in (float("nan"), float("inf"), float("-inf"), object()):
        with pytest.raises((TypeError, ValueError)):
            canonical_json_bytes({"value": invalid})


def test_canonical_datetime_handles_utc_limits_and_normalizes_offset_overflow() -> None:
    assert canonical_json_bytes(datetime.min.replace(tzinfo=UTC))
    assert canonical_json_bytes(datetime.max.replace(tzinfo=UTC))
    underflow = datetime.min.replace(tzinfo=timezone(timedelta(hours=14)))
    overflow = datetime.max.replace(tzinfo=timezone(-timedelta(hours=14)))

    for value in (underflow, overflow):
        with pytest.raises(ValueError, match="outside the UTC datetime range"):
            canonical_json_bytes(value)


def test_prepare_rejects_unsafe_paths_nan_and_infinite_metrics(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    for table_name in ("../escape", "bad/name", ".hidden"):
        with pytest.raises(LabArtifactPathError):
            store.prepare_candidate(
                job_id=uuid4(),
                spec=_spec(),
                plan_hash="6" * 64,
                adapter_id="n-shape",
                adapter_version="1",
                result_contract_version="v1",
                metrics={},
                report_markdown="ok",
                tables={table_name: pd.DataFrame({"x": [1]})},
            )
    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="finite"):
            store.prepare_candidate(
                job_id=uuid4(),
                spec=_spec(),
                plan_hash="6" * 64,
                adapter_id="n-shape",
                adapter_version="1",
                result_contract_version="v1",
                metrics={"bad": value},
                report_markdown="ok",
                tables={"result": pd.DataFrame({"x": [1]})},
            )


@pytest.mark.parametrize(
    "forged_value",
    [
        {"$datetime": "not-a-datetime"},
        {"$float": "nan"},
        {"$decimal": "NaN"},
        {"$uuid": "not-a-uuid"},
        {"$path": 7},
        {"$date": "2026-99-99"},
        {"$date": "2026-07-25", "extra": True},
    ],
)
def test_resigned_metrics_with_invalid_or_ambiguous_reserved_tag_is_rejected(
    tmp_path: Path,
    forged_value: object,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    metrics_bytes = canonical_json_bytes({"forged": forged_value})
    _resign_candidate_file(
        candidate,
        relative_path="metrics.json",
        payload=metrics_bytes,
    )

    records = LabJobArtifactStore(store.root).list_candidate_recovery()

    assert records[0].status == "invalid"
    assert "metrics.json" in (records[0].reason or "")


def test_inventory_model_is_strict_frozen_and_forbids_extra_fields() -> None:
    item = LabJobArtifactFile(
        relative_path="report.md",
        media_type="text/markdown; charset=utf-8",
        size=1,
        sha256="a" * 64,
    )
    with pytest.raises(ValidationError):
        item.model_copy(update={"size": "1"})
    with pytest.raises(ValidationError):
        LabJobArtifactFile.model_validate(
            {
                "relative_path": "report.md",
                "media_type": "text/markdown; charset=utf-8",
                "size": 1,
                "sha256": "a" * 64,
                "unexpected": True,
            }
        )
    with pytest.raises(ValidationError):
        item.size = 2  # type: ignore[misc]


def test_verify_cross_checks_manifest_snapshot_and_code_sha_against_spec_json(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    manifest_data = candidate.manifest.model_dump(mode="python")
    manifest_data["code_sha"] = "7" * 40
    identity = {
        "job_id": candidate.manifest.job_id,
        "spec_hash": candidate.manifest.spec_hash,
        "plan_hash": candidate.manifest.plan_hash,
        "adapter_id": candidate.manifest.adapter_id,
        "adapter_version": candidate.manifest.adapter_version,
        "result_contract_version": candidate.manifest.result_contract_version,
        "code_sha": "7" * 40,
        "dataset_snapshot": candidate.manifest.dataset_snapshot,
        "files": candidate.manifest.files,
    }
    manifest_data["complete_result_hash"] = hashlib.sha256(
        canonical_json_bytes(identity)
    ).hexdigest()
    forged = LabJobArtifactManifest.model_validate(manifest_data)
    (candidate.path / "manifest.json").write_bytes(forged.canonical_json_bytes())
    sums = {entry.relative_path: entry.sha256 for entry in forged.files}
    sums["manifest.json"] = forged.manifest_hash
    (candidate.path / "SHA256SUMS").write_text(
        "".join(f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())),
        encoding="ascii",
    )

    restarted = LabJobArtifactStore(tmp_path / "artifacts")
    records = restarted.list_candidate_recovery()

    assert records[0].status == "invalid"
    assert "spec identity" in (records[0].reason or "")


def test_invalid_research_run_spec_cannot_be_rehashed_and_sealed(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    spec_payload = json.loads((candidate.path / "spec.json").read_bytes())
    spec_payload["resource_class"] = "not-a-resource-class"
    spec_bytes = json.dumps(
        spec_payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    (candidate.path / "spec.json").write_bytes(spec_bytes)
    files = tuple(
        item.model_copy(
            update={
                "size": len(spec_bytes),
                "sha256": hashlib.sha256(spec_bytes).hexdigest(),
            }
        )
        if item.relative_path == "spec.json"
        else item
        for item in candidate.manifest.files
    )
    _persist_forged_manifest(
        candidate,
        files=files,
        spec_hash=hashlib.sha256(spec_bytes).hexdigest(),
    )

    record = LabJobArtifactStore(tmp_path / "artifacts").list_candidate_recovery()[0]

    assert record.status == "invalid"
    assert "ResearchRunSpec" in (record.reason or "")


def test_v2_exploratory_snapshot_with_none_audit_id_is_valid(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    spec = _spec().model_copy(
        update={
            "research_status": "exploratory",
            "dataset_snapshot": DatasetSnapshotIdentity(
                snapshot_id="2" * 64,
                binding_hash="3" * 64,
                audit_run_id=None,
            ),
        }
    )
    candidate = store.prepare_candidate(
        job_id=uuid4(),
        spec=spec,
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={},
        report_markdown="# valid exploratory\n",
        tables={"result": pd.DataFrame({"value": [1]})},
    )

    sealed = store.seal_candidate(candidate)

    assert sealed.manifest.dataset_snapshot == spec.dataset_snapshot


@pytest.mark.parametrize(
    ("case", "relative_path", "media_type"),
    [
        ("extra", "extra.txt", "text/plain"),
        ("media", "spec.json", "text/plain"),
    ],
)
def test_rehashed_manifest_cannot_expand_or_retype_exact_bundle(
    tmp_path: Path,
    case: str,
    relative_path: str,
    media_type: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    if case == "extra":
        payload = b"not part of the exact result contract"
        (candidate.path / relative_path).write_bytes(payload)
        os.chmod(candidate.path / relative_path, 0o600)
        changed = (
            *candidate.manifest.files,
            LabJobArtifactFile(
                relative_path=relative_path,
                media_type=media_type,
                size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
            ),
        )
    else:
        changed = tuple(
            item.model_copy(update={"media_type": media_type})
            if item.relative_path == relative_path
            else item
            for item in candidate.manifest.files
        )
    _persist_forged_manifest(
        candidate,
        files=tuple(
            sorted(
                changed,
                key=lambda item: item.relative_path,
            )
        ),
    )

    record = LabJobArtifactStore(tmp_path / "artifacts").list_candidate_recovery()[0]

    assert record.status == "invalid"
    assert "exact" in (record.reason or "") or "media" in (record.reason or "")


def test_empty_table_and_dtypes_round_trip_exactly(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    by_path = {entry.relative_path: entry for entry in sealed.manifest.files}
    empty = by_path["tables/empty.parquet"]
    trades = by_path["tables/trades.parquet"]

    assert empty.parquet is not None
    assert empty.parquet.row_count == 0
    assert empty.parquet.dtypes == ("datetime64[ns]", "string", "float64")
    assert trades.parquet is not None
    assert trades.parquet.dtypes == tuple(str(dtype) for dtype in _tables()["trades"].dtypes)
    assert len(empty.parquet.content_sha256) == 64


def test_parquet_rejects_arrow_object_dictionary_semantic_changes(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    frame = pd.DataFrame(
        {
            "payload": pd.Series(
                [{"left": 1}, {"right": 2}],
                dtype="object",
            )
        }
    )

    with pytest.raises(LabArtifactIntegrityError, match="content|semantic"):
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="n-shape",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"object_values": frame},
        )


def test_legacy_import_is_read_only_idempotent_and_records_fd_identity(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_bytes(b'{"run":"old"}\n')
    before = source.stat()
    index = LegacyArtifactIndex(tmp_path / "legacy-index.sqlite3")

    first = index.import_file(logical_run_id="old-run", source_path=source)
    second = index.import_file(logical_run_id="old-run", source_path=source)
    after = source.stat()

    assert first.status == "imported"
    assert second.status == "reused"
    assert second.record == first.record
    assert first.record.source_path == source.absolute()
    assert first.record.device == before.st_dev
    assert first.record.inode == before.st_ino
    assert first.record.size == before.st_size
    assert first.record.mtime_ns == before.st_mtime_ns
    assert first.record.sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
    assert first.record.media_type == "application/json"
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )


def test_legacy_same_logical_run_different_hash_conflicts(tmp_path: Path) -> None:
    source = tmp_path / "legacy.md"
    source.write_text("first", encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "legacy-index.sqlite3")
    index.import_file(logical_run_id="old-run", source_path=source)
    source.write_text("second", encoding="utf-8")

    with pytest.raises(LabLegacyArtifactConflictError):
        index.import_file(logical_run_id="old-run", source_path=source)


@pytest.mark.parametrize("case", ["symlink", "hardlink", "directory"])
def test_legacy_rejects_non_private_regular_sources(tmp_path: Path, case: str) -> None:
    source = tmp_path / "legacy.json"
    if case == "directory":
        source.mkdir()
    else:
        target = tmp_path / "target.json"
        target.write_text("{}", encoding="utf-8")
        if case == "symlink":
            source.symlink_to(target)
        else:
            os.link(target, source)
    index = LegacyArtifactIndex(tmp_path / "legacy-index.sqlite3")

    with pytest.raises(LabArtifactIntegrityError):
        index.import_file(logical_run_id="old-run", source_path=source)


def test_legacy_detects_toctou_before_index_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text("{}", encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "legacy-index.sqlite3")
    original = index._before_commit_source_check

    def replace_then_check(path: Path, expected) -> None:
        replacement = path.with_suffix(".replacement")
        replacement.write_text('{"changed":true}', encoding="utf-8")
        os.replace(replacement, path)
        original(path, expected)

    monkeypatch.setattr(index, "_before_commit_source_check", replace_then_check)

    with pytest.raises(LabArtifactIntegrityError, match="changed"):
        index.import_file(logical_run_id="old-run", source_path=source)
    assert index.get("old-run") is None


def test_legacy_path_swap_after_precommit_check_never_publishes_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"original":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "legacy-index.sqlite3")
    original = index._before_commit_source_check
    swapped = False

    def check_then_replace(path: Path, expected: object) -> None:
        nonlocal swapped
        original(path, expected)  # type: ignore[arg-type]
        replacement = path.with_suffix(".replacement")
        replacement.write_text('{"changed":true}', encoding="utf-8")
        os.replace(replacement, path)
        swapped = True

    monkeypatch.setattr(index, "_before_commit_source_check", check_then_replace)

    with pytest.raises(LabArtifactIntegrityError, match="changed"):
        index.import_file(logical_run_id="old-run", source_path=source)

    assert swapped is True
    assert index.get("old-run") is None


def test_legacy_process_crash_after_stage_commit_remains_invisible_and_resumable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    index_path = tmp_path / "legacy-index.sqlite3"
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LegacyArtifactIndex

        index = LegacyArtifactIndex(Path(sys.argv[1]))
        index._after_stage_commit = lambda _record: os._exit(87)
        index.import_file(logical_run_id="old-run", source_path=Path(sys.argv[2]))
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script, str(index_path), str(source)],
        check=False,
        cwd=Path(__file__).parents[2],
        env=os.environ.copy(),
    )

    assert completed.returncode == 87
    restarted = LegacyArtifactIndex(index_path)
    assert restarted.get("old-run") is None
    imported = restarted.import_file(logical_run_id="old-run", source_path=source)
    assert imported.status == "imported"
    assert restarted.get("old-run") == imported.record


def test_legacy_partial_tail_is_truncated_without_losing_published_generation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "legacy-index.sqlite3"
    index = LegacyArtifactIndex(path)
    imported = index.import_file(logical_run_id="published-run", source_path=source)
    index.close()
    authority = path.with_name(f"{path.name}.authority.jsonl")
    complete = authority.read_bytes()
    with authority.open("ab") as stream:
        stream.write(b'{"event_type":"staged"')
        stream.flush()
        os.fsync(stream.fileno())

    restarted = LegacyArtifactIndex(path)

    assert restarted.get("published-run") == imported.record
    assert authority.read_bytes() == complete


@pytest.mark.parametrize("truncate_to", ["empty", "first_event"])
def test_legacy_head_detects_ledger_rollback_to_valid_prefix(
    tmp_path: Path,
    truncate_to: str,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    index.import_file(logical_run_id="published-run", source_path=source)
    index.close()
    authority = path.with_name(f"{path.name}.authority.jsonl")
    heads = path.with_name(f"{path.name}.authority.heads")

    assert len(tuple(heads.glob("*.json"))) == 3
    payload = authority.read_bytes()
    first_newline = payload.index(b"\n") + 1
    authority.write_bytes(b"" if truncate_to == "empty" else payload[:first_newline])

    with pytest.raises(LabArtifactIntegrityError, match="rollback|head|cursor"):
        LegacyArtifactIndex(path)


def test_legacy_recovers_head_after_crash_between_ledger_and_head_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    crashed = False

    def crash_before_head_publish() -> None:
        nonlocal crashed
        crashed = True
        raise OSError("crash before authority head publish")

    monkeypatch.setattr(
        index,
        "_after_ledger_fsync_before_head_publish",
        crash_before_head_publish,
        raising=False,
    )

    with pytest.raises((OSError, LabArtifactIntegrityError)):
        index.import_file(logical_run_id="published-run", source_path=source)

    assert crashed is True
    index.close()
    restarted = LegacyArtifactIndex(path)
    imported = restarted.import_file(logical_run_id="published-run", source_path=source)
    assert imported.status == "imported"
    assert restarted.get("published-run") == imported.record


@pytest.mark.parametrize("damage", ["deleted", "random", "schema", "replacement"])
def test_legacy_cache_is_rebuilt_from_authority_after_damage(
    tmp_path: Path,
    damage: str,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    imported = index.import_file(logical_run_id="published-run", source_path=source)
    index.close()
    journal = path.with_name(f"{path.name}-journal")

    if damage == "deleted":
        path.unlink()
        journal.unlink()
    elif damage == "random":
        path.write_bytes(os.urandom(257))
    elif damage == "schema":
        connection = sqlite3.connect(path)
        try:
            connection.execute("DROP TABLE legacy_artifact")
            connection.commit()
        finally:
            connection.close()
    else:
        replacement_path = tmp_path / "replacement" / "legacy.sqlite3"
        replacement_source = tmp_path / "replacement.json"
        replacement_source.write_text('{"replacement":true}', encoding="utf-8")
        replacement = LegacyArtifactIndex(replacement_path)
        replacement.import_file(
            logical_run_id="replacement-run",
            source_path=replacement_source,
        )
        replacement.close()
        shutil.copy2(replacement_path, path)

    restarted = LegacyArtifactIndex(path)
    assert restarted.get("published-run") == imported.record
    connection = sqlite3.connect(path)
    try:
        cached = connection.execute(
            "SELECT publication_state, operation_id, generation "
            "FROM legacy_artifact WHERE logical_run_id = ?",
            ("published-run",),
        ).fetchone()
    finally:
        connection.close()
    assert cached is not None
    assert cached[0] == "cached"
    if damage != "deleted":
        quarantine = path.parent / ".legacy-cache-quarantine"
        assert any(item.name.startswith(path.name) for item in quarantine.iterdir())


def test_legacy_cache_rebuild_temp_symlink_never_writes_external_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    index.import_file(logical_run_id="published-run", source_path=source)
    index.close()
    path.write_bytes(os.urandom(257))
    external = tmp_path / "external.sqlite3"
    external.write_bytes(b"external sentinel")
    external_before = external.read_bytes()
    swapped = False

    def replace_temp_with_external_symlink(name: str, _descriptor: int) -> None:
        nonlocal swapped
        temporary = path.parent / name
        temporary.unlink()
        temporary.symlink_to(external)
        swapped = True

    monkeypatch.setattr(
        LegacyArtifactIndex,
        "_after_cache_temp_bound",
        staticmethod(replace_temp_with_external_symlink),
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="cache.*identity|candidate"):
        LegacyArtifactIndex(path)

    assert swapped is True
    assert external.read_bytes() == external_before
    assert external.stat().st_size == len(external_before)


def test_legacy_cache_validation_is_readonly_before_damaged_cache_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    index.close()
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TABLE legacy_artifact")
        connection.commit()
    finally:
        connection.close()
    real_connect = sqlite3.connect
    calls: list[tuple[object, dict[str, object]]] = []

    def recording_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        calls.append((args[0], dict(kwargs)))
        return real_connect(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", recording_connect)

    restarted = LegacyArtifactIndex(path)
    restarted.close()

    assert calls
    database, options = calls[0]
    assert isinstance(database, str)
    assert database.endswith("?mode=ro")
    assert options["uri"] is True


def test_legacy_sqlite_connections_are_explicitly_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_connect = sqlite3.connect
    opened: list[sqlite3.Connection] = []
    explicitly_closed: set[int] = set()

    class TrackingConnection(sqlite3.Connection):
        def close(self) -> None:
            explicitly_closed.add(id(self))
            super().close()

    def tracking_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = TrackingConnection
        connection = real_connect(*args, **kwargs)  # type: ignore[arg-type]
        opened.append(connection)
        return connection

    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", tracking_connect)
    indexes = [
        LegacyArtifactIndex(tmp_path / f"index-{number}" / "legacy.sqlite3") for number in range(30)
    ]
    for index in indexes:
        index.close()

    assert opened
    assert explicitly_closed == {id(connection) for connection in opened}


def test_closing_thirty_legacy_indexes_has_no_file_descriptor_growth(tmp_path: Path) -> None:
    descriptor_root = Path("/proc/self/fd")
    if not descriptor_root.exists():
        descriptor_root = Path("/dev/fd")
    before = len(os.listdir(descriptor_root))

    for number in range(30):
        index = LegacyArtifactIndex(tmp_path / f"fd-index-{number}" / "legacy.sqlite3")
        index.close()

    after = len(os.listdir(descriptor_root))
    assert after <= before + 1


def test_legacy_process_lock_registry_releases_last_closed_instance(tmp_path: Path) -> None:
    path = tmp_path / "index" / "legacy.sqlite3"
    key = os.fspath(path.absolute())
    first = LegacyArtifactIndex(path)
    second = LegacyArtifactIndex(path)

    entry = lab_artifacts_module._LEGACY_PROCESS_LOCKS[key]
    assert entry.references == 2
    first.close()
    assert lab_artifacts_module._LEGACY_PROCESS_LOCKS[key].references == 1
    second.close()
    assert key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS


@pytest.mark.parametrize(
    "target",
    ["parent", "lock", "ledger", "heads", "head", "cache", "journal", "quarantine"],
)
def test_legacy_managed_paths_require_exact_private_permissions(
    tmp_path: Path,
    target: str,
) -> None:
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    index.close()
    heads = path.with_name(f"{path.name}.authority.heads")
    latest_head = sorted(heads.glob("*.json"))[-1]
    targets = {
        "parent": path.parent,
        "lock": path.with_name(f"{path.name}.lock"),
        "ledger": path.with_name(f"{path.name}.authority.jsonl"),
        "heads": heads,
        "head": latest_head,
        "cache": path,
        "journal": path.with_name(f"{path.name}-journal"),
        "quarantine": path.parent / ".legacy-cache-quarantine",
    }
    selected = targets[target]
    assert selected.exists(), f"managed legacy path is missing: {target}"
    os.chmod(selected, 0o777)

    with pytest.raises(LabArtifactIntegrityError, match="permissions|private"):
        LegacyArtifactIndex(path)


def test_legacy_parent_rejects_ancestor_symlink_without_external_writes(tmp_path: Path) -> None:
    external_container = tmp_path / "legacy-external" / "container"
    external_container.mkdir(parents=True)
    alias = tmp_path / "legacy-alias"
    alias.symlink_to(external_container.parent, target_is_directory=True)

    with pytest.raises((LabArtifactPathError, LabArtifactIntegrityError, OSError)):
        LegacyArtifactIndex(alias / "container" / "index" / "legacy.sqlite3")

    assert list(external_container.iterdir()) == []


def test_legacy_connect_inode_swap_never_publishes_to_original_or_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    source.write_text('{"source":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    replacement = LegacyArtifactIndex(tmp_path / "replacement" / "legacy.sqlite3")
    original_db = index.path.with_name("original.sqlite3")
    swapped = False

    def swap_before_connect() -> None:
        nonlocal swapped
        os.rename(index.path, original_db)
        shutil.copy2(replacement.path, index.path)
        swapped = True

    monkeypatch.setattr(index, "_before_sqlite_connect", swap_before_connect, raising=False)

    with pytest.raises(LabArtifactIntegrityError, match="index.*identity"):
        index.import_file(logical_run_id="swapped-run", source_path=source)

    assert swapped is True
    for database in (original_db, index.path):
        connection = sqlite3.connect(database)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM legacy_artifact "
                "WHERE logical_run_id = ? AND publication_state = 'published'",
                ("swapped-run",),
            ).fetchone()[0]
        finally:
            connection.close()
        assert count == 0


def test_legacy_inode_swap_after_sqlite_connect_never_publishes_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    source.write_text('{"source":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    replacement = LegacyArtifactIndex(tmp_path / "replacement" / "legacy.sqlite3")
    original_db = index.path.with_name("opened-original.sqlite3")

    def swap_after_connect(_connection: sqlite3.Connection) -> None:
        os.rename(index.path, original_db)
        shutil.copy2(replacement.path, index.path)

    monkeypatch.setattr(index, "_after_sqlite_connect", swap_after_connect)

    with pytest.raises(LabArtifactIntegrityError, match="index.*identity"):
        index.import_file(logical_run_id="post-connect-swap", source_path=source)

    authority = index.path.with_name(f"{index.path.name}.authority.jsonl")
    assert b'"event_type":"published"' not in authority.read_bytes()
    for database in (original_db, index.path):
        connection = sqlite3.connect(database)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM legacy_artifact "
                "WHERE logical_run_id = ? AND publication_state = 'published'",
                ("post-connect-swap",),
            ).fetchone()[0]
        finally:
            connection.close()
        assert count == 0


def test_legacy_multi_instance_stage_lock_prevents_takeover_and_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    first = LegacyArtifactIndex(path)
    second = LegacyArtifactIndex(path)
    staged = threading.Event()
    release = threading.Event()
    second_finished = threading.Event()
    results: list[str] = []
    errors: list[BaseException] = []

    def pause_after_stage(_record: object) -> None:
        staged.set()
        assert release.wait(timeout=5)

    monkeypatch.setattr(first, "_after_stage_commit", pause_after_stage)

    def run(index: LegacyArtifactIndex, finished: threading.Event | None = None) -> None:
        try:
            results.append(
                index.import_file(logical_run_id="shared-run", source_path=source).status
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            if finished is not None:
                finished.set()

    first_thread = threading.Thread(target=run, args=(first,))
    first_thread.start()
    assert staged.wait(timeout=5)
    second_thread = threading.Thread(target=run, args=(second, second_finished))
    second_thread.start()
    time.sleep(0.1)
    assert second_finished.is_set() is False
    release.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert errors == []
    assert sorted(results) == ["imported", "reused"]
    assert second.get("shared-run") is not None


def test_legacy_process_lock_serializes_stage_and_publish(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    staged = tmp_path / "staged.marker"
    release = tmp_path / "release.marker"
    first_script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path
        from rquant.lab_artifacts import LegacyArtifactIndex

        index = LegacyArtifactIndex(Path(sys.argv[1]))
        def pause(_record):
            Path(sys.argv[3]).write_text("staged", encoding="utf-8")
            while not Path(sys.argv[4]).exists():
                time.sleep(0.01)
        index._after_stage_commit = pause
        result = index.import_file(logical_run_id="process-run", source_path=Path(sys.argv[2]))
        print(result.status, flush=True)
        """
    )
    second_script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LegacyArtifactIndex

        index = LegacyArtifactIndex(Path(sys.argv[1]))
        result = index.import_file(logical_run_id="process-run", source_path=Path(sys.argv[2]))
        print(result.status, flush=True)
        """
    )
    environment = os.environ.copy()
    first = subprocess.Popen(
        [
            sys.executable,
            "-c",
            first_script,
            str(path),
            str(source),
            str(staged),
            str(release),
        ],
        cwd=Path(__file__).parents[2],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    second: subprocess.Popen[str] | None = None
    try:
        deadline = time.monotonic() + 5
        while not staged.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert staged.exists()
        second = subprocess.Popen(
            [sys.executable, "-c", second_script, str(path), str(source)],
            cwd=Path(__file__).parents[2],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        time.sleep(0.2)
        assert second.poll() is None
        release.write_text("release", encoding="utf-8")
        first_stdout, first_stderr = first.communicate(timeout=5)
        second_stdout, second_stderr = second.communicate(timeout=5)
        assert first.returncode == 0, first_stderr
        assert second.returncode == 0, second_stderr
        assert [first_stdout.strip(), second_stdout.strip()] == ["imported", "reused"]
    finally:
        for process in (first, second):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_legacy_clock_utc_overflow_is_normalized_to_value_error(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_text("{}", encoding="utf-8")
    overflowing = datetime.min.replace(tzinfo=timezone(timedelta(hours=14)))
    index = LegacyArtifactIndex(
        tmp_path / "legacy.sqlite3",
        clock=lambda: overflowing,
    )

    with pytest.raises(ValueError, match="outside the UTC datetime range"):
        index.import_file(logical_run_id="overflow", source_path=source)


def test_legacy_database_path_swap_fails_closed_without_touching_replacement(
    tmp_path: Path,
) -> None:
    index_dir = tmp_path / "index"
    index_dir.mkdir(mode=0o700)
    source = tmp_path / "legacy.json"
    source.write_text('{"source":true}', encoding="utf-8")
    index = LegacyArtifactIndex(index_dir / "legacy.sqlite3")
    index.import_file(logical_run_id="existing", source_path=source)
    displaced = index_dir / "original.sqlite3"

    replacement_dir = tmp_path / "replacement"
    replacement_dir.mkdir(mode=0o700)
    replacement = LegacyArtifactIndex(replacement_dir / "replacement.sqlite3")
    replacement_source = tmp_path / "replacement.json"
    replacement_source.write_text('{"replacement":true}', encoding="utf-8")
    replacement.import_file(logical_run_id="replacement", source_path=replacement_source)
    replacement_bytes = replacement.path.read_bytes()
    replacement_stat = replacement.path.stat()

    os.rename(index.path, displaced)
    index.path.symlink_to(replacement.path)

    with pytest.raises(LabArtifactIntegrityError, match="index.*identity"):
        index.import_file(logical_run_id="new", source_path=source)
    with pytest.raises(LabArtifactIntegrityError, match="index.*identity"):
        index.get("existing")

    after = replacement.path.stat()
    assert replacement.path.read_bytes() == replacement_bytes
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (
        replacement_stat.st_dev,
        replacement_stat.st_ino,
        replacement_stat.st_size,
        replacement_stat.st_mtime_ns,
    )


def test_legacy_journal_inode_swap_fails_closed_without_touching_replacement(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"source":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    journal = index.path.with_name(f"{index.path.name}-journal")
    displaced = journal.with_name(f"{journal.name}.original")

    assert journal.is_file()
    os.rename(journal, displaced)
    journal.write_bytes(b"replacement journal bytes")
    before = journal.stat()
    before_bytes = journal.read_bytes()

    with pytest.raises(LabArtifactIntegrityError, match="index.*journal.*identity"):
        index.import_file(logical_run_id="new", source_path=source)

    after = journal.stat()
    assert journal.read_bytes() == before_bytes
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )


def test_legacy_source_swap_after_published_fsync_is_invalidated_and_reimportable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"generation":1}', encoding="utf-8")
    index_path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(index_path)
    displaced = tmp_path / "legacy-generation-1.json"

    def replace_after_publish(_record: object) -> None:
        os.rename(source, displaced)
        source.write_text('{"generation":2}', encoding="utf-8")

    monkeypatch.setattr(
        index,
        "_after_published_authority_commit",
        replace_after_publish,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="changed"):
        index.import_file(logical_run_id="source-swap", source_path=source)

    authority = index_path.with_name(f"{index_path.name}.authority.jsonl")
    assert b'"event_type":"invalidated"' in authority.read_bytes()
    assert index.get("source-swap") is None

    monkeypatch.setattr(index, "_after_published_authority_commit", lambda _record: None)
    imported = index.import_file(logical_run_id="source-swap", source_path=source)
    assert imported.status == "imported"
    assert index.get("source-swap") == imported.record


def test_legacy_restart_reconciles_crash_after_publish_before_source_recheck(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"generation":1}', encoding="utf-8")
    index_path = tmp_path / "index" / "legacy.sqlite3"
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LegacyArtifactIndex

        index = LegacyArtifactIndex(Path(sys.argv[1]))
        source = Path(sys.argv[2])
        def replace_and_crash(_record):
            displaced = source.with_name("legacy-before-crash.json")
            os.rename(source, displaced)
            source.write_text('{"generation":2}', encoding="utf-8")
            os._exit(89)
        index._after_published_authority_commit = replace_and_crash
        index.import_file(logical_run_id="crashed-source-swap", source_path=source)
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script, str(index_path), str(source)],
        check=False,
        cwd=Path(__file__).parents[2],
        env=os.environ.copy(),
    )

    assert completed.returncode == 89
    restarted = LegacyArtifactIndex(index_path)
    authority = index_path.with_name(f"{index_path.name}.authority.jsonl")
    assert b'"event_type":"invalidated"' in authority.read_bytes()
    assert restarted.get("crashed-source-swap") is None
    imported = restarted.import_file(
        logical_run_id="crashed-source-swap",
        source_path=source,
    )
    assert imported.status == "imported"
    assert restarted.get("crashed-source-swap") == imported.record


def test_bound_legacy_source_preserves_caller_and_identity_failures(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_text("{}", encoding="utf-8")

    with (
        pytest.raises(ExceptionGroup) as captured,
        lab_artifacts_module._open_bound_readonly_file(
            source,
            label="legacy reviewer source",
        ),
    ):
        replacement = tmp_path / "replacement.json"
        replacement.write_text('{"changed":true}', encoding="utf-8")
        os.replace(replacement, source)
        raise RuntimeError("caller failed")

    assert any(isinstance(item, RuntimeError) for item in captured.value.exceptions)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)


def test_interrupted_seal_rejects_valid_same_job_bundle_not_bound_to_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts-a")
    candidate_a = _prepare(store)

    def crash_after_rename(_bound: object) -> None:
        raise OSError("intent A remains durable")

    monkeypatch.setattr(store, "_finalize_bound_directories", crash_after_rename)
    with pytest.raises(OSError, match="intent A"):
        store.seal_candidate(candidate_a)

    other = LabJobArtifactStore(tmp_path / "artifacts-b")
    candidate_b = other.prepare_candidate(
        job_id=candidate_a.job_id,
        spec=_spec(),
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={"bundle": "B"},
        report_markdown="# Bundle B\n",
        tables=_tables(),
    )
    sealed_b = other.seal_candidate(candidate_b)
    published = store.sealed_root / candidate_a.job_id.hex
    os.rename(published, tmp_path / "displaced-bundle-a")
    shutil.copytree(sealed_b.path, published, copy_function=shutil.copy2)

    with pytest.raises(LabArtifactIntegrityError, match="seal intent"):
        LabJobArtifactStore(tmp_path / "artifacts-a").recover_interrupted_seal(published)


def test_legacy_import_never_opens_disk_sqlite_for_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text("{}", encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    real_connect = sqlite3.connect
    calls: list[tuple[object, dict[str, object]]] = []

    def recording_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        calls.append((args[0], dict(kwargs)))
        return real_connect(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", recording_connect)

    imported = index.import_file(logical_run_id="memory-cache-only", source_path=source)

    assert imported.status == "imported"
    assert calls
    assert all(
        database == ":memory:"
        or (
            isinstance(database, str)
            and database.endswith("?mode=ro")
            and options.get("uri") is True
        )
        for database, options in calls
    )


def test_parquet_empty_categorical_dtype_round_trips_with_full_identity(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    frame = pd.DataFrame({"bucket": pd.Series(pd.Categorical([], categories=[], ordered=False))})
    candidate = store.prepare_candidate(
        job_id=uuid4(),
        spec=_spec(),
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={},
        report_markdown="ok",
        tables={"categories": frame},
    )
    sealed = store.seal_candidate(candidate)
    parquet = sealed.manifest.files[-1].parquet

    assert parquet is not None
    dtype = parquet.dtype_identities[0]
    assert dtype.family == "categorical"
    assert dtype.categories == ()
    assert dtype.ordered is False
    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def test_parquet_ordered_unused_categories_are_part_of_dtype_identity(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    frame = pd.DataFrame(
        {
            "bucket": pd.Series(
                pd.Categorical(
                    ["high"],
                    categories=["low", "mid", "high"],
                    ordered=True,
                )
            )
        }
    )
    sealed = store.seal_candidate(
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="n-shape",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"categories": frame},
        )
    )
    parquet = sealed.manifest.files[-1].parquet

    assert parquet is not None
    dtype = parquet.dtype_identities[0]
    assert dtype.family == "categorical"
    assert dtype.categories == ('"low"', '"mid"', '"high"')
    assert dtype.ordered is True
    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def test_parquet_empty_nullable_and_timezone_dtypes_have_stable_identity(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    frame = pd.DataFrame(
        {
            "count": pd.Series([], dtype="Int64"),
            "enabled": pd.Series([], dtype="boolean"),
            "label": pd.Series([], dtype="string"),
            "observed_at": pd.Series([], dtype="datetime64[ns, Asia/Shanghai]"),
        }
    )
    sealed = store.seal_candidate(
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="n-shape",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"nullable": frame},
        )
    )
    parquet = sealed.manifest.files[-1].parquet

    assert parquet is not None
    identities = {
        column: dtype
        for column, dtype in zip(
            parquet.columns,
            parquet.dtype_identities,
            strict=True,
        )
    }
    assert identities["count"].family == "extension"
    assert identities["enabled"].family == "extension"
    assert identities["label"].family == "extension"
    assert identities["observed_at"].family == "datetime_tz"
    assert identities["observed_at"].timezone == "Asia/Shanghai"
    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def _prepare_other_sealed_bundle(
    root: Path,
    *,
    job_id: UUID,
) -> lab_artifacts_module.LabSealedJobArtifact:
    store = LabJobArtifactStore(root)
    candidate = store.prepare_candidate(
        job_id=job_id,
        spec=_spec(),
        plan_hash="6" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={"replacement": True},
        report_markdown="# Replacement bundle\n",
        tables=_tables(),
    )
    return store.seal_candidate(candidate)


@pytest.mark.parametrize(
    "operation",
    ["seal_candidate", "idempotent_seal_candidate", "recover_candidate"],
)
def test_public_new_sealed_return_fails_if_path_is_replaced_after_final_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    if operation == "idempotent_seal_candidate":
        store.seal_candidate(candidate)
        candidate = _prepare(store)
    replacement = _prepare_other_sealed_bundle(
        tmp_path / "replacement-artifacts",
        job_id=candidate.job_id,
    )
    displaced = tmp_path / f"displaced-{operation}"
    swapped = False

    def replace_after_final_validation(sealed: object) -> None:
        nonlocal swapped
        path = sealed.path  # type: ignore[attr-defined]
        parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
        os.chmod(path.parent, 0o700)
        try:
            os.chmod(path, 0o700)
            os.rename(path, displaced)
            os.chmod(displaced, 0o500)
            shutil.copytree(replacement.path, path, copy_function=shutil.copy2)
        finally:
            os.chmod(path.parent, parent_mode)
        swapped = True

    monkeypatch.setattr(
        store,
        "_after_public_sealed_finalized",
        replace_after_final_validation,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="sealed|bound|identity"):
        if operation in {"seal_candidate", "idempotent_seal_candidate"}:
            store.seal_candidate(candidate)
        else:
            record = next(
                item for item in store.list_candidate_recovery() if item.path == candidate.path
            )
            store.recover_candidate(record, authority=_recovery_authority(candidate))

    assert swapped is True


@pytest.mark.parametrize("branch", ["interrupted", "already_sealed"])
def test_public_interrupted_recovery_return_keeps_final_bundle_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    branch: str,
) -> None:
    root = tmp_path / "artifacts"
    store = LabJobArtifactStore(root)
    candidate = _prepare(store)

    def crash_after_rename(_bound: object) -> None:
        raise OSError("leave recoverable sealed bundle")

    monkeypatch.setattr(store, "_finalize_bound_directories", crash_after_rename)
    with pytest.raises(OSError, match="recoverable"):
        store.seal_candidate(candidate)

    restarted = LabJobArtifactStore(root)
    published = restarted.sealed_root / candidate.job_id.hex
    if branch == "already_sealed":
        restarted.recover_interrupted_seal(published)
    replacement = _prepare_other_sealed_bundle(
        tmp_path / f"replacement-{branch}",
        job_id=candidate.job_id,
    )
    displaced = tmp_path / f"displaced-{branch}"
    swapped = False

    def replace_after_final_validation(sealed: object) -> None:
        nonlocal swapped
        path = sealed.path  # type: ignore[attr-defined]
        parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
        os.chmod(path.parent, 0o700)
        try:
            os.chmod(path, 0o700)
            os.rename(path, displaced)
            os.chmod(displaced, 0o500)
            shutil.copytree(replacement.path, path, copy_function=shutil.copy2)
        finally:
            os.chmod(path.parent, parent_mode)
        swapped = True

    monkeypatch.setattr(
        restarted,
        "_after_public_sealed_finalized",
        replace_after_final_validation,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="sealed|bound|identity"):
        restarted.recover_interrupted_seal(published)

    assert swapped is True


def test_public_sealed_finalizer_rejects_same_manifest_swap_before_rebinding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    replacement_store = LabJobArtifactStore(tmp_path / "replacement-artifacts")
    replacement = replacement_store.seal_candidate(
        _prepare(replacement_store, job_id=candidate.job_id)
    )
    displaced = tmp_path / "displaced-before-final-bind"
    swapped = False

    def replace_before_final_bind(path: Path) -> None:
        nonlocal swapped
        parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
        os.chmod(path.parent, 0o700)
        try:
            os.chmod(path, 0o700)
            os.rename(path, displaced)
            os.chmod(displaced, 0o500)
            shutil.copytree(replacement.path, path, copy_function=shutil.copy2)
        finally:
            os.chmod(path.parent, parent_mode)
        swapped = True

    monkeypatch.setattr(
        store,
        "_before_public_sealed_bind",
        replace_before_final_bind,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="expected.*identity|identity.*expected"):
        store.seal_candidate(candidate)

    assert swapped is True


def test_parquet_restores_python_string_storage_for_empty_and_nonempty_columns(
    tmp_path: Path,
) -> None:
    frame = pd.DataFrame(
        {
            "empty": pd.Series([], dtype=pd.StringDtype(storage="python")),
            "value": pd.Series(["alpha", pd.NA], dtype=pd.StringDtype(storage="python")),
        }
    )
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="dtype-review",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"strings": frame},
        )
    )
    parquet = sealed.manifest.files[-1].parquet

    assert parquet is not None
    assert [item.storage for item in parquet.dtype_identities] == ["python", "python"]
    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def test_parquet_restores_pyarrow_string_and_nullable_extension_dtypes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        {
            "label": pd.Series(["alpha", pd.NA], dtype=pd.StringDtype(storage="pyarrow")),
            "count": pd.Series([1, pd.NA], dtype="Int64"),
            "ratio": pd.Series([1.5, pd.NA], dtype="Float64"),
            "enabled": pd.Series([True, pd.NA], dtype="boolean"),
        }
    )
    original_read = lab_artifacts_module.pd.read_parquet

    def lossy_read(*args: object, **kwargs: object) -> pd.DataFrame:
        restored = original_read(*args, **kwargs)  # type: ignore[arg-type]
        for column in restored.columns:
            restored[column] = restored[column].astype(object)
        return restored

    monkeypatch.setattr(lab_artifacts_module.pd, "read_parquet", lossy_read)
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="dtype-review",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"nullable": frame},
        )
    )

    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def test_parquet_restores_categorical_category_extension_dtypes(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "string_bucket": pd.Series(
                pd.Categorical(
                    ["high"],
                    categories=pd.Index(
                        ["low", "high"],
                        dtype=pd.StringDtype(storage="python"),
                    ),
                    ordered=True,
                )
            ),
            "integer_bucket": pd.Series(
                pd.Categorical(
                    [1],
                    categories=pd.Index([1, 2], dtype="Int64"),
                    ordered=False,
                )
            ),
        }
    )
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="dtype-review",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"categories": frame},
        )
    )
    parquet = sealed.manifest.files[-1].parquet

    assert parquet is not None
    category_dtypes = [item.categories_dtype_identity for item in parquet.dtype_identities]
    assert category_dtypes[0] is not None and category_dtypes[0].storage == "python"
    assert category_dtypes[1] is not None
    assert category_dtypes[1].pandas_dtype == "Int64"
    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def test_parquet_restores_timezone_period_and_interval_dtypes(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "observed_at": pd.Series(
                pd.to_datetime(["2026-01-01 09:30", None]).tz_localize("Asia/Shanghai")
            ),
            "period": pd.Series([pd.Period("2026-01", freq="M"), pd.NaT]),
            "interval": pd.Series(
                pd.arrays.IntervalArray.from_tuples([(0, 1), None], closed="right")
            ),
        }
    )
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="dtype-review",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"temporal": frame},
        )
    )
    parquet = sealed.manifest.files[-1].parquet

    assert parquet is not None
    identities = dict(zip(parquet.columns, parquet.dtype_identities, strict=True))
    assert identities["period"].period_frequency == "M"
    assert identities["interval"].interval_closed == "right"
    assert identities["interval"].interval_subtype_identity is not None
    assert store.verify_sealed(sealed.path).manifest_hash == sealed.manifest_hash


def test_legacy_instance_rebinds_valid_cache_rebuilt_without_head_change(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    first = LegacyArtifactIndex(path)
    second = LegacyArtifactIndex(path)
    imported = first.import_file(logical_run_id="stable-run", source_path=source)
    assert second.get("stable-run") == imported.record
    original_inode = os.fstat(first._database_descriptor).st_ino
    heads = path.with_name(f"{path.name}.authority.heads")
    heads_before = {item.name: item.read_bytes() for item in sorted(heads.glob("*.json"))}

    path.write_bytes(os.urandom(257))

    assert second.get("stable-run") == imported.record
    rebuilt_inode = os.fstat(second._database_descriptor).st_ino
    assert rebuilt_inode != original_inode
    assert {item.name: item.read_bytes() for item in sorted(heads.glob("*.json"))} == heads_before

    assert first.get("stable-run") == imported.record
    assert os.fstat(first._database_descriptor).st_ino == rebuilt_inode


def test_legacy_generation_head_reservation_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    reservation: dict[str, str] = {}

    def reserve_head(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        descriptor = os.open(
            destination_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=destination_parent,
        )
        os.write(descriptor, b"reservation")
        os.close(descriptor)
        reservation["name"] = destination_name
        lab_artifacts_module._rename_noreplace(
            source_parent,
            source_name,
            destination_parent,
            destination_name,
        )

    monkeypatch.setattr(
        index,
        "_atomic_authority_head_publish_noreplace",
        reserve_head,
        raising=False,
    )

    with pytest.raises(LabArtifactConflictError, match="head.*exists|reservation|conflict"):
        index.import_file(logical_run_id="reserved-head", source_path=source)

    reserved = index.path.parent / index._authority_heads_name / reservation["name"]
    assert reserved.read_bytes() == b"reservation"


def test_legacy_selected_generation_head_inode_swap_is_a_conflict(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    index.import_file(logical_run_id="head-swap", source_path=source)
    selected = index._authority_heads_path / index._head_name
    displaced = tmp_path / "original-generation-head.json"
    payload = selected.read_bytes()

    os.rename(selected, displaced)
    selected.write_bytes(payload)
    os.chmod(selected, 0o600)

    with pytest.raises(
        (LabArtifactConflictError, LabArtifactIntegrityError),
        match="head.*changed|generation.*conflict|identity",
    ):
        index.get("head-swap")


def test_legacy_missing_multiple_generation_heads_is_not_crash_recovery(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    index = LegacyArtifactIndex(path)
    index.import_file(logical_run_id="head-loss", source_path=source)
    index.close()
    heads = sorted(path.with_name(f"{path.name}.authority.heads").glob("*.json"))
    assert len(heads) == 3

    heads[-1].unlink()
    heads[-2].unlink()

    with pytest.raises(LabArtifactIntegrityError, match="head.*missing|audit|recovery"):
        LegacyArtifactIndex(path)


def test_legacy_single_head_migrates_to_immutable_generations_from_ledger(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    original = LegacyArtifactIndex(path)
    imported = original.import_file(logical_run_id="migrated-head", source_path=source)
    original.close()
    heads = path.with_name(f"{path.name}.authority.heads")
    generation_files = sorted(heads.glob("*.json"))
    latest_payload = generation_files[-1].read_bytes()
    for item in generation_files:
        item.unlink()
    heads.rmdir()
    legacy_head = path.with_name(f"{path.name}.authority.head.json")
    legacy_head.write_bytes(latest_payload)
    os.chmod(legacy_head, 0o600)

    migrated = LegacyArtifactIndex(path)

    assert migrated.get("migrated-head") == imported.record
    assert len(tuple(heads.glob("*.json"))) == 3
    assert not legacy_head.exists()
    assert any(
        item.name.startswith(f"{legacy_head.name}.")
        for item in (path.parent / ".legacy-authority-quarantine").iterdir()
    )


def test_object_null_kinds_have_distinct_hashes_and_cannot_be_folded_by_parquet(
    tmp_path: Path,
) -> None:
    values = (None, float("nan"), pd.NA, pd.NaT)
    hashes = {
        lab_artifacts_module._table_content_hash(
            pd.DataFrame({"value": pd.Series([value], dtype=object)})
        )
        for value in values
    }

    assert len(hashes) == len(values)
    assert lab_artifacts_module._canonical_table_value(np.datetime64("NaT")) == {
        "$datetime_nat": "datetime64"
    }

    store = LabJobArtifactStore(tmp_path / "artifacts")
    frame = pd.DataFrame({"value": pd.Series(list(values), dtype=object)})
    with pytest.raises(
        LabArtifactIntegrityError,
        match="round-trip changed canonical content semantics|unsupported semantic",
    ):
        store.prepare_candidate(
            job_id=uuid4(),
            spec=_spec(),
            plan_hash="6" * 64,
            adapter_id="object-null-review",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={},
            report_markdown="ok",
            tables={"object_nulls": frame},
        )


def test_legacy_import_keeps_source_bound_through_cache_sync_and_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    displaced = tmp_path / "legacy.original.json"
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    swapped = False

    def replace_after_cache_sync(_record: object) -> None:
        nonlocal swapped
        os.rename(source, displaced)
        source.write_text('{"replacement":true}', encoding="utf-8")
        swapped = True

    monkeypatch.setattr(
        index,
        "_after_import_cache_sync",
        replace_after_cache_sync,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="source.*changed|publication"):
        index.import_file(logical_run_id="cache-race", source_path=source)

    assert swapped is True
    assert index.get("cache-race") is None

    source.unlink()
    os.rename(displaced, source)
    monkeypatch.setattr(index, "_after_import_cache_sync", lambda _record: None, raising=False)
    retried = index.import_file(logical_run_id="cache-race", source_path=source)
    assert retried.status == "imported"


def test_zip_destination_inode_swap_before_return_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    destination = tmp_path / "exports" / "bundle.zip"
    displaced = tmp_path / "published-original.zip"
    swapped = False

    def replace_zip(path: Path) -> None:
        nonlocal swapped
        os.rename(path, displaced)
        path.write_bytes(displaced.read_bytes())
        os.chmod(path, 0o600)
        swapped = True

    monkeypatch.setattr(
        store,
        "_after_zip_final_checks",
        replace_zip,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="ZIP.*identity|destination.*changed"):
        store.export_deterministic_zip(sealed.path, _evidence(sealed), destination)

    assert swapped is True


@pytest.mark.parametrize("operation", ["candidate", "recovery"])
def test_quarantine_target_inode_swap_before_return_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    displaced = tmp_path / f"quarantine-original-{operation}"
    swapped = False

    def replace_quarantine(record: object) -> None:
        nonlocal swapped
        path = record.path  # type: ignore[attr-defined]
        os.chmod(path, 0o700)
        os.rename(path, displaced)
        shutil.copytree(displaced, path, copy_function=shutil.copy2)
        swapped = True

    monkeypatch.setattr(
        store,
        "_after_quarantine_record_finalized",
        replace_quarantine,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="quarantine.*identity|target.*changed"):
        if operation == "candidate":
            store.quarantine_candidate(candidate, reason="review race")
        else:
            recovery = next(
                item for item in store.list_candidate_recovery() if item.path == candidate.path
            )
            store.quarantine_recovery_record(recovery, reason="review race")

    assert swapped is True
