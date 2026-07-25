from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4
from zipfile import ZipFile

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
        indexed_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
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
) -> None:
    selected_files = files or candidate.manifest.files
    selected_spec_hash = spec_hash or candidate.manifest.spec_hash
    selected_code_sha = code_sha or candidate.manifest.code_sha
    identity = {
        "job_id": candidate.manifest.job_id,
        "spec_hash": selected_spec_hash,
        "plan_hash": candidate.manifest.plan_hash,
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

    def verify_then_swap(candidate: LabJobArtifactCandidate) -> LabJobArtifactManifest:
        manifest = original_verify(candidate)
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
    recovered = restarted.recover_candidate(records[0])

    assert records[0].status == "recoverable"
    assert recovered.path.exists()
    second = _prepare(restarted, job_id=uuid4())
    quarantined = restarted.quarantine_candidate(second, reason="operator cleanup")
    assert quarantined.status == "quarantined"
    assert quarantined.path.exists()
    assert (quarantined.path / "report.md").read_bytes() == (
        b"# Full report\n\nNo rounded metrics.\n"
    )


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


def test_export_and_legacy_index_do_not_chmod_existing_caller_directories(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    output = tmp_path / "caller-owned"
    output.mkdir(mode=0o755)
    before_mode = stat.S_IMODE(output.stat().st_mode)

    LegacyArtifactIndex(output / "legacy.sqlite3")
    store.export_deterministic_zip(
        sealed.path,
        _evidence(sealed),
        output / "result.zip",
    )

    assert stat.S_IMODE(output.stat().st_mode) == before_mode


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
