from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest

import rquant.lab_artifact_catalog as catalog_module
from rquant.artifact_retention import ArtifactReferenceStore
from rquant.lab_artifact_catalog import (
    LabArtifactCatalogIntegrityError,
    LabArtifactCatalogRegistrar,
    LabArtifactDirectoryFrontier,
    LabArtifactDurableOwners,
)
from rquant.lab_worker import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    LabShardArtifactManifest,
    LabShardResultManifest,
    canonical_shard_frame_digest,
)

NOW = datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
JOB_ID = UUID("11111111-1111-4111-8111-111111111111")
SHARD_ID = UUID("22222222-2222-4222-8222-222222222222")
CLAIM_TOKEN = UUID("33333333-3333-4333-8333-333333333333")
SPEC_HASH = "a" * 64
PAYLOAD_HASH = "b" * 64
PLAN_HASH = "c" * 64
SNAPSHOT_ID = "d" * 64
EXPERIMENT_ID = "e" * 64
AUDIT_RUN_ID = "9" * 64
CODE_SHA = "f" * 40


class _CtimeShiftedStat:
    def __init__(self, observed: os.stat_result) -> None:
        self._observed = observed
        self.st_ctime_ns = observed.st_ctime_ns + 1

    def __getattr__(self, name: str) -> object:
        return getattr(self._observed, name)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sealed_bundle(
    root: Path,
    *,
    job_id: UUID = JOB_ID,
    shard_id: UUID = SHARD_ID,
    claim_token: UUID = CLAIM_TOKEN,
    fence: int = 7,
    generation: int = 3,
    payload_bytes: bytes | None = None,
) -> tuple[Path, LabShardResultManifest]:
    bundle = (
        root
        / "jobs"
        / str(job_id)
        / "shards"
        / str(shard_id)
        / "attempts"
        / f"{fence:020d}-{generation:020d}-{claim_token}"
    )
    bundle.mkdir(parents=True)
    frame = pd.DataFrame({"ts_code": ["600000.SH"], "score": [1.5]})
    artifact_path = bundle / "000-result.parquet"
    frame.to_parquet(artifact_path, index=False)
    if payload_bytes is not None:
        artifact_path.write_bytes(payload_bytes)
    payload = artifact_path.read_bytes()
    artifact = LabShardArtifactManifest(
        name="result",
        file_name=artifact_path.name,
        row_count=len(frame),
        columns=tuple(frame.columns),
        file_size=len(payload),
        file_sha256=_sha256(payload),
        content_sha256=canonical_shard_frame_digest(frame),
    )
    manifest = LabShardResultManifest(
        worker_code_sha=CODE_SHA,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
        job_id=job_id,
        shard_id=shard_id,
        claim_token=claim_token,
        claim_generation=generation,
        scheduler_fencing_token=fence,
        spec_hash=SPEC_HASH,
        payload_hash=PAYLOAD_HASH,
        plan_hash=PLAN_HASH,
        adapter_id="n-shape",
        adapter_version="1",
        artifacts=(artifact,),
    )
    (bundle / "manifest.json").write_text(manifest.canonical_json(), encoding="utf-8")
    return bundle, manifest


def _owners(manifest: LabShardResultManifest) -> LabArtifactDurableOwners:
    return LabArtifactDurableOwners(
        job_id=manifest.job_id,
        spec_hash=manifest.spec_hash,
        plan_hash=manifest.plan_hash,
        snapshot_id=SNAPSHOT_ID,
        experiment_id=EXPERIMENT_ID,
        audit_run_id=AUDIT_RUN_ID,
    )


def _registrar(
    root: Path,
    ledger: Path,
    *,
    now: datetime = NOW,
) -> tuple[LabArtifactCatalogRegistrar, ArtifactReferenceStore]:
    store = ArtifactReferenceStore(
        ledger,
        managed_trust_root=ledger.parent,
        clock=lambda: NOW + timedelta(days=365),
    )
    return (
        LabArtifactCatalogRegistrar(
            artifact_root=root,
            reference_store=store,
            owner_resolver=_owners,
            location_id="lab-artifacts-local",
            failure_domain="macbook-primary-disk",
            clock=lambda: now,
        ),
        store,
    )


def _discover_all(registrar: LabArtifactCatalogRegistrar) -> tuple[str, ...]:
    next_sequence = 2
    frontiers = [
        LabArtifactDirectoryFrontier(
            frontier_sequence=1,
            revision=0,
            scan_generation=1,
            relative_directory="jobs",
            directory_kind="jobs",
            directory_offset=0,
        )
    ]
    bundles: list[str] = []
    while frontiers:
        frontier = frontiers.pop(0)
        page = registrar.scan_directory_page(frontier, max_entries=8)
        bundles.extend(page.bundle_paths)
        for child in page.child_directories:
            frontiers.append(
                LabArtifactDirectoryFrontier(
                    frontier_sequence=next_sequence,
                    revision=0,
                    scan_generation=1,
                    relative_directory=child.relative_directory,
                    directory_kind=child.directory_kind,
                    directory_offset=0,
                )
            )
            next_sequence += 1
        if not page.exhausted:
            frontiers.insert(
                0,
                frontier.model_copy(
                    update={
                        "revision": frontier.revision + 1,
                        "directory_device": page.directory_device,
                        "directory_inode": page.directory_inode,
                        "directory_offset": page.directory_offset,
                        "buffered_entry_names": page.buffered_entry_names,
                    }
                ),
            )
    return tuple(bundles)


def _run_all(registrar: LabArtifactCatalogRegistrar):
    return registrar.run_once(bundle_paths=_discover_all(registrar))


def test_registers_verified_bundle_and_is_restart_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, manifest = _sealed_bundle(root)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    first = _run_all(registrar)
    later, _ = _registrar(
        root,
        tmp_path / "references.sqlite3",
        now=NOW + timedelta(days=1),
    )
    second = _run_all(later)

    identity = store.get_object(manifest.manifest_hash)
    copies = store.list_active_copies(manifest.manifest_hash)
    references = store.list_active_references(manifest.manifest_hash)
    assert first.status == "completed"
    assert (first.scanned_bundles, first.registered_objects) == (1, 1)
    assert (first.registered_copies, first.registered_references) == (1, 4)
    assert first.has_more is False and first.next_cursor is None
    assert second.registered_objects == 0
    assert second.registered_copies == 0
    assert second.registered_references == 0
    assert second.unchanged_bundles == 1
    assert identity.object_kind == "strategy_lab_shard_result_bundle"
    assert identity.size_bytes == sum(path.stat().st_size for path in bundle.iterdir())
    assert copies[0].storage_uri == bundle.as_uri()
    assert {(item.owner_type, item.owner_id) for item in references} == {
        ("job", str(JOB_ID)),
        ("snapshot", SNAPSHOT_ID),
        ("experiment", EXPERIMENT_ID),
        ("audit", AUDIT_RUN_ID),
    }
    assert len(store.list_audit_events()) == 6


def test_mtime_is_not_used_as_artifact_authority(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, manifest = _sealed_bundle(root)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")
    _run_all(registrar)
    original = store.get_object(manifest.manifest_hash)

    for path in (bundle, *bundle.iterdir()):
        stat_result = path.stat()
        os.utime(path, ns=(stat_result.st_atime_ns, stat_result.st_mtime_ns + 1_000_000))
    later, _ = _registrar(
        root,
        tmp_path / "references.sqlite3",
        now=NOW + timedelta(days=1),
    )

    result = _run_all(later)

    assert result.unchanged_bundles == 1
    assert store.get_object(manifest.manifest_hash) == original


def test_rejects_symlinked_jobs_root_without_touching_external_files(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    (root / "jobs").symlink_to(external, target_is_directory=True)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    with pytest.raises(LabArtifactCatalogIntegrityError, match="symlink|unsafe"):
        _run_all(registrar)

    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert store.list_audit_events() == ()


def test_rejects_hidden_symlink_inside_attempt_namespace(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root)
    external = tmp_path / "external-attempt"
    external.mkdir()
    (bundle.parent / ".reclaim-v1-unsafe").symlink_to(external, target_is_directory=True)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    with pytest.raises(LabArtifactCatalogIntegrityError, match="unsafe"):
        _run_all(registrar)

    assert external.exists()
    assert store.list_audit_events() == ()


def test_rejects_configured_root_reached_through_parent_symlink(tmp_path: Path) -> None:
    physical_parent = tmp_path / "physical"
    root = physical_parent / "artifacts"
    root.mkdir(mode=0o700, parents=True)
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(physical_parent, target_is_directory=True)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )

    with pytest.raises(ValueError, match="exact absolute path|symlink"):
        LabArtifactCatalogRegistrar(
            artifact_root=linked_parent / "artifacts",
            reference_store=store,
            owner_resolver=_owners,
            location_id="lab-artifacts-local",
            failure_domain="macbook-primary-disk",
            clock=lambda: NOW,
        )


@pytest.mark.parametrize("hazard", ["relative", "non-normalized"])
def test_rejects_noncanonical_artifact_root_path(tmp_path: Path, hazard: str) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    unsafe = (
        Path("relative/artifacts")
        if hazard == "relative"
        else Path(f"{tmp_path}/unused/../artifacts")
    )

    with pytest.raises(ValueError, match="exact absolute"):
        LabArtifactCatalogRegistrar(
            artifact_root=unsafe,
            reference_store=store,
            owner_resolver=_owners,
            location_id="lab-artifacts-local",
            failure_domain="macbook-primary-disk",
        )


def test_rejects_artifact_root_without_private_exact_mode(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o755)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )

    with pytest.raises(ValueError, match="mode|private|0700"):
        LabArtifactCatalogRegistrar(
            artifact_root=root,
            reference_store=store,
            owner_resolver=_owners,
            location_id="lab-artifacts-local",
            failure_domain="macbook-primary-disk",
        )


def test_rejects_artifact_root_not_owned_by_current_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    monkeypatch.setattr(catalog_module.os, "geteuid", lambda: root.stat().st_uid + 1)

    with pytest.raises(ValueError, match="owner"):
        LabArtifactCatalogRegistrar(
            artifact_root=root,
            reference_store=store,
            owner_resolver=_owners,
            location_id="lab-artifacts-local",
            failure_domain="macbook-primary-disk",
        )


def test_artifact_root_reparenting_fails_closed_even_when_root_inode_is_preserved(
    tmp_path: Path,
) -> None:
    container = tmp_path / "container"
    container.mkdir(mode=0o700)
    root = container / "artifacts"
    root.mkdir(mode=0o700)
    _sealed_bundle(root)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")
    retired = tmp_path / "retired"

    container.rename(retired)
    container.mkdir(mode=0o700)
    (retired / "artifacts").rename(root)

    with pytest.raises(LabArtifactCatalogIntegrityError, match="root|ancestor|identity"):
        _run_all(registrar)

    assert store.list_audit_events() == ()


@pytest.mark.parametrize("target_kind", ["artifact-root", "ancestor"])
def test_artifact_authority_binds_ctime_for_root_and_bundle_ancestors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
) -> None:
    container = tmp_path / "container"
    container.mkdir(mode=0o700)
    root = container / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root)
    registrar, _store = _registrar(root, tmp_path / "references.sqlite3")
    expected = registrar._ancestor_identities(bundle)
    target = root if target_kind == "artifact-root" else bundle.parent
    real_lstat = catalog_module.os.lstat

    def shifted_lstat(candidate: object) -> os.stat_result:
        observed = real_lstat(candidate)
        if Path(candidate) == target:
            return _CtimeShiftedStat(observed)  # type: ignore[return-value]
        return observed

    monkeypatch.setattr(catalog_module.os, "lstat", shifted_lstat)

    if target_kind == "artifact-root":
        with pytest.raises(LabArtifactCatalogIntegrityError, match="root|ancestor|identity"):
            registrar._root_authority.assert_current()
    else:
        assert registrar._ancestor_identities(bundle) != expected


def test_rejects_hardlinked_payload_before_registering_anything(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root)
    payload = bundle / "000-result.parquet"
    external_link = tmp_path / "external.parquet"
    os.link(payload, external_link)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    with pytest.raises(LabArtifactCatalogIntegrityError, match="hard link|unsafe"):
        _run_all(registrar)

    assert external_link.exists()
    assert store.list_audit_events() == ()


def test_rejects_hash_mismatch_before_registering_anything(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root)
    (bundle / "000-result.parquet").write_bytes(b"changed")
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    with pytest.raises(LabArtifactCatalogIntegrityError, match="hash|bytes"):
        _run_all(registrar)

    assert store.list_audit_events() == ()


def test_rejects_payload_replacement_between_verification_and_registration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root)
    payload = bundle / "000-result.parquet"
    replacement = tmp_path / "replacement.parquet"
    replacement.write_bytes(payload.read_bytes())
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )

    def replace_after_verification(manifest: LabShardResultManifest) -> LabArtifactDurableOwners:
        os.replace(replacement, payload)
        return _owners(manifest)

    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=replace_after_verification,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
    )
    relative = bundle.relative_to(root).as_posix()

    with pytest.raises(LabArtifactCatalogIntegrityError, match="changed|replacement"):
        registrar.run_once(bundle_paths=(relative,))

    assert store.list_audit_events() == ()


def test_rejects_payload_replacement_inside_registration_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root)
    payload = bundle / "000-result.parquet"
    replacement = tmp_path / "replacement.parquet"
    replacement.write_bytes(payload.read_bytes())
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")
    original_check = store._assert_no_deletion_claim

    def replace_after_transaction_guard(*args: object, **kwargs: object) -> None:
        os.replace(replacement, payload)
        original_check(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(store, "_assert_no_deletion_claim", replace_after_transaction_guard)

    with pytest.raises(LabArtifactCatalogIntegrityError, match="replacement|changed"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert store.list_audit_events() == ()


def test_rejects_manifest_identity_that_disagrees_with_sealed_path(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, manifest = _sealed_bundle(root)
    wrong_job = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    changed = manifest.model_copy(update={"job_id": wrong_job})
    (bundle / "manifest.json").write_text(changed.canonical_json(), encoding="utf-8")
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    with pytest.raises(LabArtifactCatalogIntegrityError, match="path identity"):
        _run_all(registrar)

    assert store.list_audit_events() == ()


def test_discovery_and_registration_are_separate_bounded_operations(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    _sealed_bundle(root)
    second_shard = UUID("44444444-4444-4444-8444-444444444444")
    _sealed_bundle(root, shard_id=second_shard)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    paths = _discover_all(registrar)
    first = registrar.run_once(bundle_paths=paths[:1])
    second = registrar.run_once(bundle_paths=paths[1:])

    assert len(paths) == 2
    assert first.has_more is False and first.next_cursor is None
    assert second.status == "completed"
    assert second.scanned_bundles == 1
    assert second.has_more is False and second.next_cursor is None
    assert len(store.list_active_references(first.content_hashes[0])) == 4
    assert len(store.list_active_references(second.content_hashes[0])) == 4


def test_registration_batch_enters_owner_resolver_batch_context_once(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    _sealed_bundle(root)
    _sealed_bundle(root, shard_id=UUID("44444444-4444-4444-8444-444444444444"))
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: NOW + timedelta(days=365),
    )

    class BatchResolver:
        def __init__(self) -> None:
            self.batch_calls = 0
            self.resolve_calls = 0
            self.active = False

        @contextmanager
        def batch(self):
            self.batch_calls += 1
            self.active = True
            try:
                yield
            finally:
                self.active = False

        def __call__(self, manifest: LabShardResultManifest) -> LabArtifactDurableOwners:
            assert self.active
            self.resolve_calls += 1
            return _owners(manifest)

    resolver = BatchResolver()
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=resolver,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
    )

    result = registrar.run_once(bundle_paths=_discover_all(registrar))

    assert result.scanned_bundles == 2
    assert resolver.batch_calls == 1
    assert resolver.resolve_calls == 2


def test_real_directory_frontier_is_bounded_and_next_generation_finds_earlier_uuid(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    late = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
    middle = UUID("77777777-7777-4777-8777-777777777777")
    _sealed_bundle(root, job_id=late)
    _sealed_bundle(root, job_id=middle)
    registrar, _store = _registrar(root, tmp_path / "references.sqlite3")

    def scan_jobs(generation: int) -> tuple[tuple[str, ...], tuple[int, ...]]:
        frontier = LabArtifactDirectoryFrontier(
            frontier_sequence=generation,
            revision=0,
            scan_generation=generation,
            relative_directory="jobs",
            directory_kind="jobs",
            directory_offset=0,
        )
        children: list[str] = []
        page_sizes: list[int] = []
        while True:
            page = registrar.scan_directory_page(frontier, max_entries=1)
            page_sizes.append(page.scanned_entries)
            children.extend(item.relative_directory for item in page.child_directories)
            if page.exhausted:
                return tuple(children), tuple(page_sizes)
            frontier = frontier.model_copy(
                update={
                    "revision": frontier.revision + 1,
                    "directory_device": page.directory_device,
                    "directory_inode": page.directory_inode,
                    "directory_offset": page.directory_offset,
                    "buffered_entry_names": page.buffered_entry_names,
                }
            )

    first_generation, first_page_sizes = scan_jobs(1)
    earlier = UUID("00000000-0000-4000-8000-000000000001")
    _sealed_bundle(root, job_id=earlier)
    next_generation, next_page_sizes = scan_jobs(2)

    assert len(first_generation) == 2
    assert all(size <= 1 for size in first_page_sizes)
    assert f"jobs/{earlier}/shards" in next_generation
    assert all(size <= 1 for size in next_page_sizes)


def test_accepts_every_canonical_uuid_identity_emitted_by_worker_contract(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    unrestricted_token = UUID("00000000-0000-0000-0000-000000000001")
    _sealed_bundle(root, claim_token=unrestricted_token)
    registrar, _store = _registrar(root, tmp_path / "references.sqlite3")

    result = _run_all(registrar)

    assert result.scanned_bundles == 1


def test_owner_binding_must_match_typed_manifest_identity(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    _sealed_bundle(root)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=lambda manifest: _owners(manifest).model_copy(
            update={"spec_hash": "9" * 64}
        ),
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="owner binding"):
        _run_all(registrar)

    assert store.list_audit_events() == ()


def test_registration_counts_do_not_scan_the_full_audit_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    _sealed_bundle(root)
    registrar, store = _registrar(root, tmp_path / "references.sqlite3")

    def reject_full_audit_scan() -> None:
        raise AssertionError("catalog batches must not scan the complete audit log")

    def reject_nonatomic_registration(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("catalog must use atomic bundle registration")

    monkeypatch.setattr(store, "list_audit_events", reject_full_audit_scan)
    monkeypatch.setattr(store, "register_object", reject_nonatomic_registration)
    monkeypatch.setattr(store, "register_copy", reject_nonatomic_registration)
    monkeypatch.setattr(store, "register_reference", reject_nonatomic_registration)

    result = _run_all(registrar)

    assert result.registered_objects == 1
    assert result.registered_copies == 1
    assert result.registered_references == 4


def test_terminal_owner_releaser_runs_only_after_atomic_registration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    _bundle, expected_manifest = _sealed_bundle(root)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    released: list[tuple[str, LabArtifactDurableOwners, datetime]] = []

    def release_terminal_owner(
        manifest: LabShardResultManifest,
        owners: LabArtifactDurableOwners,
        observed_at: datetime,
    ) -> None:
        assert {
            reference.owner_type
            for reference in store.list_active_references(manifest.manifest_hash)
        } == {"audit", "experiment", "job", "snapshot"}
        released.append((manifest.manifest_hash, owners, observed_at))

    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        terminal_owner_releaser=release_terminal_owner,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
    )

    result = _run_all(registrar)

    assert result.registered_references == 4
    assert released == [(result.content_hashes[0], _owners(expected_manifest), NOW)]


def test_registrar_auto_composes_terminal_releaser_from_trusted_owner_resolver(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    _bundle, expected_manifest = _sealed_bundle(root)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    released: list[str] = []

    class TrustedResolver:
        def __call__(self, manifest: LabShardResultManifest) -> LabArtifactDurableOwners:
            return _owners(manifest)

        def build_terminal_owner_releaser(self, reference_store: object):  # type: ignore[no-untyped-def]
            assert reference_store is store

            def release(
                manifest: LabShardResultManifest,
                _owners_value: LabArtifactDurableOwners,
                _observed_at: datetime,
            ) -> None:
                released.append(manifest.manifest_hash)

            return release

    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=TrustedResolver(),
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
    )

    result = _run_all(registrar)

    assert released == [expected_manifest.manifest_hash]
    assert result.registered_references == 4


def test_large_legal_artifact_is_hashed_in_bounded_streaming_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    payload = b"x" * (2 * 1024 * 1024 + 17)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=payload)
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    requested_reads: list[int] = []
    original_read = catalog_module.os.read

    def tracked_read(descriptor: int, size: int) -> bytes:
        requested_reads.append(size)
        return original_read(descriptor, size)

    monkeypatch.setattr(catalog_module.os, "read", tracked_read)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=len(payload),
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )

    result = registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert result.scanned_bundles == 1
    assert result.total_bytes == total_bytes
    payload_reads = [size for size in requested_reads if size == 1024 * 1024]
    assert len(payload_reads) == 2
    assert 18 in requested_reads
    assert max(requested_reads) <= 1024 * 1024


def test_single_artifact_file_budget_fails_closed_before_hashing(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    payload = b"x" * (1024 * 1024 + 1)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=payload)
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=len(payload) - 1,
        max_bundle_bytes=total_bytes + 1,
        max_step_bytes=total_bytes + 1,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="file.*budget|single-file"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert store.list_audit_events() == ()


def test_single_file_budget_also_applies_to_manifest(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=b"payload")
    manifest_bytes = (bundle / "manifest.json").stat().st_size
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=manifest_bytes - 1,
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="manifest|file.*budget"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert store.list_audit_events() == ()


def test_small_artifact_hash_read_request_is_bounded_by_remaining_file_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    payload = b"x" * 17
    bundle, _manifest = _sealed_bundle(root, payload_bytes=payload)
    manifest_bytes = (bundle / "manifest.json").stat().st_size
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    requested_reads: list[int] = []
    original_read = catalog_module.os.read

    def tracked_read(descriptor: int, size: int) -> bytes:
        requested_reads.append(size)
        return original_read(descriptor, size)

    monkeypatch.setattr(catalog_module.os, "read", tracked_read)
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=ArtifactReferenceStore(
            tmp_path / "references.sqlite3",
            managed_trust_root=tmp_path,
        ),
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=manifest_bytes,
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )

    registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert 1024 * 1024 not in requested_reads
    assert 18 in requested_reads


def test_bundle_byte_budget_is_inclusive_and_rejects_one_byte_over(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    payload = b"payload"
    bundle, _manifest = _sealed_bundle(root, payload_bytes=payload)
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    max_file_bytes = max(path.stat().st_size for path in bundle.iterdir())
    exact = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=ArtifactReferenceStore(
            tmp_path / "exact.sqlite3",
            managed_trust_root=tmp_path,
        ),
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=max_file_bytes,
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )

    assert (
        exact.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),)).total_bytes
        == total_bytes
    )

    store = ArtifactReferenceStore(
        tmp_path / "limited.sqlite3",
        managed_trust_root=tmp_path,
    )
    limited = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=max_file_bytes,
        max_bundle_bytes=total_bytes - 1,
        max_step_bytes=total_bytes,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="bundle.*budget"):
        limited.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))
    assert store.list_audit_events() == ()


def test_step_byte_budget_returns_partial_before_verifying_all_pending(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    first, _manifest = _sealed_bundle(root)
    second, _manifest = _sealed_bundle(
        root,
        shard_id=UUID("44444444-4444-4444-8444-444444444444"),
    )
    first_bytes = sum(path.stat().st_size for path in first.iterdir())
    second_bytes = sum(path.stat().st_size for path in second.iterdir())
    assert first_bytes == second_bytes
    owner_calls: list[UUID] = []

    def owners(manifest: LabShardResultManifest) -> LabArtifactDurableOwners:
        owner_calls.append(manifest.shard_id)
        return _owners(manifest)

    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=first_bytes,
        max_bundle_bytes=first_bytes,
        max_step_bytes=first_bytes,
        max_verification_seconds=5.0,
        monotonic=lambda: 0.0,
    )
    paths = tuple(
        path.relative_to(root).as_posix()
        for path in sorted((first, second), key=lambda item: item.as_posix())
    )

    result = registrar.run_once(bundle_paths=paths)

    assert result.status == "partial"
    assert result.scanned_bundles == 1
    assert result.has_more is True
    assert result.next_cursor == paths[1]
    assert owner_calls == [SHARD_ID]
    assert len(store.list_audit_events()) == 6


def test_slow_hash_reader_stops_mid_file_without_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    payload = b"x" * (3 * 1024 * 1024)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=payload)
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    elapsed = 0.0
    payload_reads = 0
    original_read = catalog_module.os.read

    def monotonic() -> float:
        return elapsed

    def slow_read(descriptor: int, size: int) -> bytes:
        nonlocal elapsed, payload_reads
        chunk = original_read(descriptor, size)
        if size == 1024 * 1024 and chunk:
            payload_reads += 1
            elapsed += 0.3
        return chunk

    monkeypatch.setattr(catalog_module.os, "read", slow_read)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        clock=lambda: NOW,
        max_artifact_file_bytes=len(payload),
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_verification_seconds=0.5,
        monotonic=monotonic,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="time budget"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert payload_reads == 2
    assert store.list_audit_events() == ()


def test_file_time_budget_stops_one_slow_file_before_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    payload = b"x" * (3 * 1024 * 1024)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=payload)
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    elapsed = 0.0
    original_read = catalog_module.os.read

    def slow_read(descriptor: int, size: int) -> bytes:
        nonlocal elapsed
        chunk = original_read(descriptor, size)
        if chunk:
            elapsed += 0.3
        return chunk

    monkeypatch.setattr(catalog_module.os, "read", slow_read)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        max_artifact_file_bytes=len(payload),
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_artifact_file_verification_seconds=0.5,
        max_bundle_verification_seconds=5.0,
        max_verification_seconds=5.0,
        monotonic=lambda: elapsed,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="file time budget"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert store.list_audit_events() == ()


def test_bundle_time_budget_spans_manifest_and_payload_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=b"payload")
    total_bytes = sum(path.stat().st_size for path in bundle.iterdir())
    elapsed = 0.0
    original_read = catalog_module.os.read

    def slow_read(descriptor: int, size: int) -> bytes:
        nonlocal elapsed
        chunk = original_read(descriptor, size)
        if chunk:
            elapsed += 0.3
        return chunk

    monkeypatch.setattr(catalog_module.os, "read", slow_read)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        max_artifact_file_bytes=total_bytes,
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_artifact_file_verification_seconds=0.5,
        max_bundle_verification_seconds=0.5,
        max_verification_seconds=5.0,
        monotonic=lambda: elapsed,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="bundle time budget"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert store.list_audit_events() == ()


def test_base_exception_mid_stream_closes_descriptor_and_registers_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    bundle, _manifest = _sealed_bundle(root, payload_bytes=b"x" * (2 * 1024 * 1024))
    interrupted_descriptor: int | None = None
    original_read = catalog_module.os.read

    def interrupt_payload(descriptor: int, size: int) -> bytes:
        nonlocal interrupted_descriptor
        if size == 1024 * 1024:
            interrupted_descriptor = descriptor
            raise KeyboardInterrupt("injected streaming interruption")
        return original_read(descriptor, size)

    monkeypatch.setattr(catalog_module.os, "read", interrupt_payload)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
    )

    with pytest.raises(KeyboardInterrupt, match="streaming interruption"):
        registrar.run_once(bundle_paths=(bundle.relative_to(root).as_posix(),))

    assert interrupted_descriptor is not None
    with pytest.raises(OSError):
        os.fstat(interrupted_descriptor)
    assert store.list_audit_events() == ()


def test_step_time_budget_returns_partial_after_completed_bundle_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    first, _manifest = _sealed_bundle(root, payload_bytes=b"first")
    second, _manifest = _sealed_bundle(
        root,
        shard_id=UUID("44444444-4444-4444-8444-444444444444"),
        payload_bytes=b"second",
    )
    elapsed = 0.0
    original_read = catalog_module.os.read

    def slow_read(descriptor: int, size: int) -> bytes:
        nonlocal elapsed
        chunk = original_read(descriptor, size)
        if chunk:
            elapsed += 0.2
        return chunk

    monkeypatch.setattr(catalog_module.os, "read", slow_read)
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )
    total_bytes = sum(
        path.stat().st_size for candidate in (first, second) for path in candidate.iterdir()
    )
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=root,
        reference_store=store,
        owner_resolver=_owners,
        location_id="lab-artifacts-local",
        failure_domain="macbook-primary-disk",
        max_artifact_file_bytes=total_bytes,
        max_bundle_bytes=total_bytes,
        max_step_bytes=total_bytes,
        max_artifact_file_verification_seconds=0.6,
        max_bundle_verification_seconds=0.6,
        max_verification_seconds=0.6,
        monotonic=lambda: elapsed,
    )
    paths = tuple(
        path.relative_to(root).as_posix()
        for path in sorted((first, second), key=lambda item: item.as_posix())
    )

    result = registrar.run_once(bundle_paths=paths)

    assert result.status == "partial"
    assert result.scanned_bundles == 1
    assert result.next_cursor == paths[1]
    assert len(store.list_audit_events()) == 6
