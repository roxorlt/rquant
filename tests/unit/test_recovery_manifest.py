from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.recovery_manifest import (
    RecoveryArtifactRole,
    RecoveryArtifactSource,
    RecoveryCurrentPointer,
    RecoveryFaultPoint,
    RecoveryInventoryPlan,
    RecoveryInventoryRequirement,
    RecoveryManifestError,
    RecoveryRehearsalError,
    RecoveryRehearsalStatus,
    RecoveryRpoAssessment,
    RecoveryVerificationResult,
    RecoveryWatermarkSummary,
    append_recovery_manifest,
    build_recovery_manifest,
    load_recovery_manifest,
    read_recovery_current,
    rehearse_restore,
)

CAPTURED_AT = datetime(2026, 7, 31, 7, 30, tzinfo=UTC)
COMPLETED_AT = CAPTURED_AT + timedelta(minutes=4)


def _required_artifacts() -> tuple[tuple[str, RecoveryArtifactRole, str], ...]:
    return (
        ("production.duckdb", RecoveryArtifactRole.PRODUCTION_DUCKDB, "db/rquant.duckdb"),
        ("state.jobs", RecoveryArtifactRole.SQLITE_STATE, "state/jobs.sqlite3"),
        ("state.signals", RecoveryArtifactRole.SQLITE_STATE, "state/signals.sqlite3"),
        ("research.catalog", RecoveryArtifactRole.RESEARCH_CATALOG, "research/catalog.db"),
        ("research.lake", RecoveryArtifactRole.LAKE_MANIFEST, "research/lake.json"),
        ("artifact.metadata", RecoveryArtifactRole.ARTIFACT_METADATA, "meta/artifacts.db"),
        ("serving.current", RecoveryArtifactRole.SERVING_CURRENT, "serving/CURRENT.json"),
        ("serving.manifest", RecoveryArtifactRole.SERVING_MANIFEST, "serving/manifest.json"),
    )


def _plan() -> RecoveryInventoryPlan:
    return RecoveryInventoryPlan(
        plan_version=1,
        requirements=tuple(
            RecoveryInventoryRequirement(
                logical_role=logical_role,
                artifact_role=artifact_role,
                restore_path=restore_path,
            )
            for logical_role, artifact_role, restore_path in _required_artifacts()
        ),
    )


def _sources(root: Path) -> tuple[RecoveryArtifactSource, ...]:
    result: list[RecoveryArtifactSource] = []
    for index, (logical_role, artifact_role, restore_path) in enumerate(_required_artifacts()):
        source = root / "source" / restore_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(f"{logical_role}:generation-17\n".encode())
        result.append(
            RecoveryArtifactSource(
                logical_role=logical_role,
                artifact_role=artifact_role,
                absolute_path=str(source.resolve()),
                generation_id="generation-17",
                schema_version="v10",
                watermark=RecoveryWatermarkSummary(
                    high_watermark=f"batch-{index:02d}",
                    max_date=date(2026, 7, 30),
                    row_count=100 + index,
                ),
            )
        )
    return tuple(result)


def _manifest(root: Path):
    return build_recovery_manifest(
        plan=_plan(),
        sources=_sources(root),
        captured_at=CAPTURED_AT,
    )


def _accepted_verification(
    _candidate: Path,
    _manifest: object,
) -> RecoveryVerificationResult:
    return RecoveryVerificationResult(
        passed=True,
        checks=("duckdb-open", "sqlite-integrity", "serving-pointer"),
        rpo=RecoveryRpoAssessment(
            realtime_batch_lag=1,
            research_rebuildable=True,
            research_rebuild_basis=("research.catalog", "research.lake"),
        ),
    )


def _write_old_current(root: Path) -> RecoveryCurrentPointer:
    generation = root / "generations" / "old-generation"
    generation.mkdir(parents=True)
    pointer = RecoveryCurrentPointer(
        generation_id="old-generation",
        manifest_id="a" * 64,
        published_at=CAPTURED_AT - timedelta(days=1),
    )
    root.mkdir(parents=True, exist_ok=True)
    (root / "CURRENT.json").write_text(pointer.model_dump_json(), encoding="utf-8")
    return pointer


def test_build_manifest_captures_complete_typed_content_addressed_inventory(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)

    assert manifest.manifest_id == manifest.calculate_manifest_id()
    assert manifest.captured_at == CAPTURED_AT
    assert len(manifest.entries) == 8
    assert [entry.logical_role for entry in manifest.entries] == sorted(
        role for role, _artifact_role, _path in _required_artifacts()
    )
    assert (
        sum(entry.artifact_role is RecoveryArtifactRole.SQLITE_STATE for entry in manifest.entries)
        == 2
    )
    for entry in manifest.entries:
        source = Path(entry.absolute_path)
        assert source.is_absolute()
        assert entry.size_bytes == source.stat().st_size
        assert entry.sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
        assert entry.generation_id == "generation-17"
        assert entry.schema_version == "v10"
        assert entry.watermark.max_date == date(2026, 7, 30)
        assert entry.watermark.row_count is not None


def test_inventory_plan_makes_every_declared_sqlite_state_mandatory(tmp_path: Path) -> None:
    sources = tuple(
        source for source in _sources(tmp_path) if source.logical_role != "state.signals"
    )

    with pytest.raises(RecoveryManifestError, match="missing inventory roles: state.signals"):
        build_recovery_manifest(plan=_plan(), sources=sources, captured_at=CAPTURED_AT)


def test_inventory_rejects_duplicate_logical_role_and_role_mismatch(tmp_path: Path) -> None:
    requirements = _plan().requirements
    with pytest.raises(ValidationError, match="logical roles must be unique"):
        RecoveryInventoryPlan(plan_version=1, requirements=requirements + (requirements[0],))

    sources = list(_sources(tmp_path))
    sources[0] = sources[0].model_copy(
        update={"artifact_role": RecoveryArtifactRole.RESEARCH_CATALOG}
    )
    with pytest.raises(RecoveryManifestError, match="role mismatch for production.duckdb"):
        build_recovery_manifest(plan=_plan(), sources=tuple(sources), captured_at=CAPTURED_AT)


@pytest.mark.parametrize("restore_path", ("../escape.db", "/absolute/escape.db", "a\\b.db"))
def test_inventory_rejects_restore_path_traversal(restore_path: str) -> None:
    with pytest.raises(ValidationError, match="safe relative POSIX path"):
        RecoveryInventoryRequirement(
            logical_role="state.bad",
            artifact_role=RecoveryArtifactRole.SQLITE_STATE,
            restore_path=restore_path,
        )


def test_inventory_rejects_relative_source_and_symlink(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="absolute_path must be absolute"):
        RecoveryArtifactSource(
            logical_role="state.bad",
            artifact_role=RecoveryArtifactRole.SQLITE_STATE,
            absolute_path="relative.db",
            generation_id="g1",
            schema_version="v1",
            watermark=RecoveryWatermarkSummary(row_count=0),
        )

    sources = list(_sources(tmp_path))
    original = Path(sources[0].absolute_path)
    link = original.parent / "source-link"
    link.symlink_to(original)
    sources[0] = sources[0].model_copy(update={"absolute_path": str(link)})
    with pytest.raises(RecoveryManifestError, match="symlink"):
        build_recovery_manifest(plan=_plan(), sources=tuple(sources), captured_at=CAPTURED_AT)


def test_inventory_rejects_source_beneath_symlinked_parent(tmp_path: Path) -> None:
    sources = list(_sources(tmp_path))
    original = Path(sources[0].absolute_path)
    linked_parent = tmp_path / "linked-source"
    linked_parent.symlink_to(original.parent, target_is_directory=True)
    sources[0] = sources[0].model_copy(update={"absolute_path": str(linked_parent / original.name)})

    with pytest.raises(RecoveryManifestError, match="symlink component"):
        build_recovery_manifest(plan=_plan(), sources=tuple(sources), captured_at=CAPTURED_AT)


def test_manifest_store_rejects_symlinked_parent(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    real_parent = tmp_path / "real-manifest-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-manifest-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(RecoveryManifestError, match="symlink component"):
        append_recovery_manifest(linked_parent / "manifests", manifest)


def test_manifest_store_is_content_addressed_and_append_only(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    store = tmp_path / "manifest-store"

    first_path = append_recovery_manifest(store, manifest)
    assert first_path.name == f"{manifest.manifest_id}.json"
    assert load_recovery_manifest(first_path) == manifest
    assert append_recovery_manifest(store, manifest) == first_path

    changed_source = Path(manifest.entries[0].absolute_path)
    changed_source.write_bytes(b"new immutable source generation")
    second = build_recovery_manifest(
        plan=_plan(),
        sources=tuple(
            source.model_copy(update={"generation_id": "generation-18"})
            if source.logical_role == "production.duckdb"
            else source
            for source in _sources(tmp_path / "second")
        ),
        captured_at=CAPTURED_AT + timedelta(minutes=1),
    )
    second_path = append_recovery_manifest(store, second)

    assert second_path != first_path
    assert first_path.exists()
    assert len(tuple(store.glob("*.json"))) == 2


def test_append_refuses_corrupt_existing_manifest_object(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    store = tmp_path / "manifest-store"
    store.mkdir()
    target = store / f"{manifest.manifest_id}.json"
    target.write_text("{}", encoding="utf-8")

    with pytest.raises(RecoveryManifestError, match="existing manifest object differs"):
        append_recovery_manifest(store, manifest)
    assert target.read_text(encoding="utf-8") == "{}"


def test_restore_rehearsal_copies_verifies_and_atomically_publishes(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    restore_root = tmp_path / "restore"
    previous = _write_old_current(restore_root)

    report = rehearse_restore(
        manifest=manifest,
        target_root=restore_root,
        started_at=CAPTURED_AT + timedelta(minutes=2),
        completed_at=COMPLETED_AT,
        verifier=_accepted_verification,
    )

    assert report.status is RecoveryRehearsalStatus.PASSED
    assert report.previous_generation_id == previous.generation_id
    assert report.published_generation_id == manifest.manifest_id
    assert report.verification is not None
    assert report.verification.rpo.realtime_within_one_batch is True
    current = read_recovery_current(restore_root)
    assert current is not None
    assert current.generation_id == manifest.manifest_id
    generation = restore_root / "generations" / manifest.manifest_id
    for entry in manifest.entries:
        restored = generation / entry.restore_path
        assert restored.read_bytes() == Path(entry.absolute_path).read_bytes()
        assert hashlib.sha256(restored.read_bytes()).hexdigest() == entry.sha256
    report_path = restore_root / "reports" / f"{report.report_id}.json"
    assert report_path.exists()
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == "passed"
    assert not tuple(restore_root.glob(".candidate-*"))


@pytest.mark.parametrize(
    "fault_point",
    (
        RecoveryFaultPoint.AFTER_COPY,
        RecoveryFaultPoint.AFTER_HASH_VERIFY,
        RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH,
        RecoveryFaultPoint.AFTER_GENERATION_STAGE,
        RecoveryFaultPoint.AFTER_CURRENT_SWITCH,
    ),
)
def test_fault_injection_preserves_old_current_and_cleans_candidate(
    tmp_path: Path,
    fault_point: RecoveryFaultPoint,
) -> None:
    manifest = _manifest(tmp_path / fault_point.value)
    restore_root = tmp_path / f"restore-{fault_point.value}"
    previous = _write_old_current(restore_root)

    def inject(point: RecoveryFaultPoint, _candidate: Path) -> None:
        if point is fault_point:
            raise RuntimeError(f"injected {point.value}")

    with pytest.raises(RecoveryRehearsalError, match=fault_point.value) as caught:
        rehearse_restore(
            manifest=manifest,
            target_root=restore_root,
            started_at=CAPTURED_AT + timedelta(minutes=2),
            completed_at=COMPLETED_AT,
            verifier=_accepted_verification,
            fault_injector=inject,
        )

    assert caught.value.report.status is RecoveryRehearsalStatus.FAILED
    assert read_recovery_current(restore_root) == previous
    assert not tuple(restore_root.glob(".candidate-*"))
    assert not (restore_root / "generations" / manifest.manifest_id).exists()
    assert (restore_root / "reports" / f"{caught.value.report.report_id}.json").exists()


@pytest.mark.parametrize("source_failure", ("missing", "hash"))
def test_restore_rejects_missing_or_hash_drifted_source_before_publish(
    tmp_path: Path,
    source_failure: str,
) -> None:
    manifest = _manifest(tmp_path)
    restore_root = tmp_path / "restore"
    previous = _write_old_current(restore_root)
    source = Path(manifest.entries[0].absolute_path)
    if source_failure == "missing":
        source.unlink()
    else:
        source.write_bytes(b"source drift after capture")

    with pytest.raises(RecoveryRehearsalError, match=source_failure):
        rehearse_restore(
            manifest=manifest,
            target_root=restore_root,
            started_at=CAPTURED_AT + timedelta(minutes=2),
            completed_at=COMPLETED_AT,
            verifier=_accepted_verification,
        )

    assert read_recovery_current(restore_root) == previous
    assert not tuple(restore_root.glob(".candidate-*"))


@pytest.mark.parametrize(
    ("lag", "research_rebuildable", "message"),
    ((2, True, "real-time RPO exceeds one batch"), (1, False, "research is not rebuildable")),
)
def test_restore_requires_explicit_rpo_acceptance(
    tmp_path: Path,
    lag: int,
    research_rebuildable: bool,
    message: str,
) -> None:
    manifest = _manifest(tmp_path)
    restore_root = tmp_path / "restore"
    previous = _write_old_current(restore_root)

    def verify(_candidate: Path, _manifest: object) -> RecoveryVerificationResult:
        return RecoveryVerificationResult(
            passed=True,
            checks=("storage-integrity",),
            rpo=RecoveryRpoAssessment(
                realtime_batch_lag=lag,
                research_rebuildable=research_rebuildable,
                research_rebuild_basis=("research.catalog",) if research_rebuildable else (),
            ),
        )

    with pytest.raises(RecoveryRehearsalError, match=message) as caught:
        rehearse_restore(
            manifest=manifest,
            target_root=restore_root,
            started_at=CAPTURED_AT + timedelta(minutes=2),
            completed_at=COMPLETED_AT,
            verifier=verify,
        )

    assert caught.value.report.verification is not None
    assert read_recovery_current(restore_root) == previous
    assert not tuple(restore_root.glob(".candidate-*"))


def test_restore_rejects_symlink_introduced_after_capture(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    source = Path(manifest.entries[0].absolute_path)
    replacement = source.with_suffix(".replacement")
    replacement.write_bytes(source.read_bytes())
    source.unlink()
    source.symlink_to(replacement)

    with pytest.raises(RecoveryRehearsalError, match="symlink"):
        rehearse_restore(
            manifest=manifest,
            target_root=tmp_path / "restore",
            started_at=CAPTURED_AT + timedelta(minutes=2),
            completed_at=COMPLETED_AT,
            verifier=_accepted_verification,
        )


def test_restore_rejects_target_beneath_symlinked_parent(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path / "inventory")
    real_parent = tmp_path / "real-restore-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-restore-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(RecoveryManifestError, match="target path contains symlink component"):
        rehearse_restore(
            manifest=manifest,
            target_root=linked_parent / "restore",
            started_at=CAPTURED_AT + timedelta(minutes=2),
            completed_at=COMPLETED_AT,
            verifier=_accepted_verification,
        )
