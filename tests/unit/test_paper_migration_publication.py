from __future__ import annotations

import hashlib
import inspect
import json
import os
import sqlite3
import stat
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.paper_ledger_migration as migration_module
import rquant.paper_migration_publication as publication_module
from rquant.paper_ledger_migration import migrate_v4_ledger_copy
from rquant.paper_migration_publication import (
    PaperMigrationMaterializationError,
    PaperMigrationPostCommitIndeterminateError,
    PaperMigrationPreCommitError,
    PublicationRootPolicy,
    canonical_manifest_bytes,
    local_audit_publication_root_policy,
    materialize_paper_migration_for_audit,
    observe_publication_root,
    parse_canonical_manifest,
    recover_paper_migration_publication,
)
from rquant.private_fs import rename_noreplace_at
from tests.fixtures.paper_ledger_v4_fixture import create_parent_v4_fixture


def _private_directory(path: Path, mode: int = 0o700) -> Path:
    path.mkdir()
    path.chmod(mode)
    return path


def _publish(
    tmp_path: Path,
    *,
    failure_after_phase: str | None = None,
) -> tuple[object, PublicationRootPolicy, Path, Path, Path]:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    staging_root = _private_directory(tmp_path / "staging")
    policy = local_audit_publication_root_policy(publication_root)
    result = migrate_v4_ledger_copy(
        source.path,
        publication_root,
        root_policy=policy,
        migration_code_identity="test-migration-code",
        failure_after_phase=failure_after_phase,
    )
    return result, policy, staging_root, source.path, publication_root


def _generation_path(result: object, publication_root: Path) -> Path:
    receipt = result.publication
    return publication_root / "generations" / receipt.manifest.generation_name


def _object_path(result: object, publication_root: Path) -> Path:
    receipt = result.publication
    return _generation_path(result, publication_root) / receipt.manifest.object_name


def test_rename_success_before_parent_fsync_is_post_commit_indeterminate(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_bytes = source.path.read_bytes()
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)

    with pytest.raises(PaperMigrationPostCommitIndeterminateError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
            failure_after_phase="after_generation_rename_before_parent_fsync",
        )

    state = caught.value.state
    assert state.publication_state == "GENERATION_RENAMED_UNCONFIRMED"
    assert (publication_root / "generations" / state.generation_name).is_dir()
    assert source.path.read_bytes() == source_bytes
    receipt = recover_paper_migration_publication(state, root_policy=policy)
    assert receipt.publication_state == "GENERATION_DURABLE_VERIFIED"


@pytest.mark.parametrize(
    (
        "case",
        "fault_point",
        "fsync_failure_index",
        "final_failure",
        "expected_reason",
    ),
    (
        (
            "source-parent-fsync",
            None,
            0,
            None,
            "SOURCE_PARENT_FSYNC_FAILED",
        ),
        (
            "generations-fsync",
            None,
            1,
            None,
            "GENERATIONS_FSYNC_FAILED",
        ),
        (
            "after-parent-fsync",
            "after_parent_fsync_before_final_verify",
            None,
            None,
            "FAULT_INJECTED",
        ),
        (
            "final-verify",
            "during_final_generation_verify",
            None,
            None,
            "FAULT_INJECTED",
        ),
        ("final-root", None, None, "root", "FINAL_ROOT_POLICY_FAILED"),
        ("final-inventory", None, None, "inventory", "FINAL_INVENTORY_FAILED"),
        ("final-manifest", None, None, "manifest", "FINAL_MANIFEST_FAILED"),
        ("final-object", None, None, "object", "FINAL_OBJECT_FAILED"),
        ("result-assembly", "before_result_assembly", None, None, "FAULT_INJECTED"),
    ),
)
def test_parent_fsync_or_final_verify_failure_never_rolls_back_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    fault_point: str | None,
    fsync_failure_index: int | None,
    final_failure: str | None,
    expected_reason: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    if fsync_failure_index is not None:
        original_checkpoint = publication_module._checkpoint
        original_fsync = os.fsync
        after_rename = False
        observed_parent_fsyncs = 0

        def checkpoint(fault: str | None, phase: str) -> None:
            nonlocal after_rename
            original_checkpoint(fault, phase)
            if phase == "after_generation_rename_before_parent_fsync":
                after_rename = True

        def fail_selected_parent_fsync(descriptor: int) -> None:
            nonlocal observed_parent_fsyncs
            if after_rename:
                selected = observed_parent_fsyncs == fsync_failure_index
                observed_parent_fsyncs += 1
                if selected:
                    raise OSError(f"injected {case} failure")
            original_fsync(descriptor)

        monkeypatch.setattr(publication_module, "_checkpoint", checkpoint)
        monkeypatch.setattr(publication_module.os, "fsync", fail_selected_parent_fsync)
    elif final_failure == "root":
        original_observe = publication_module.observe_publication_root
        observations = 0

        def fail_final_root(*args: object, **kwargs: object) -> object:
            nonlocal observations
            observations += 1
            if observations == 2:
                raise ValueError("final publication root policy failed")
            return original_observe(*args, **kwargs)

        monkeypatch.setattr(publication_module, "observe_publication_root", fail_final_root)
    elif final_failure is not None:

        def fail_final_verification(*_args: object, **_kwargs: object) -> object:
            raise ValueError(f"final publication {final_failure} failed")

        monkeypatch.setattr(
            publication_module,
            "_validate_receipt_from_generation",
            fail_final_verification,
        )

    with pytest.raises(PaperMigrationPostCommitIndeterminateError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
            failure_after_phase=fault_point,
        )

    assert (publication_root / "generations" / caught.value.state.generation_name).is_dir()
    assert caught.value.state.reason == expected_reason


def test_workspace_creation_failure_reports_visible_building_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    nonce = "a" * 64
    building_name = f".building-{nonce}"
    original_open = os.open
    original_observe = publication_module.observe_publication_root

    def fail_building_open(path: object, *args: object, **kwargs: object) -> int:
        if os.fspath(path) == building_name:
            raise OSError("injected building descriptor failure")
        return original_open(path, *args, **kwargs)

    def observe_then_fail_building_open(*args: object, **kwargs: object) -> object:
        handle = original_observe(*args, **kwargs)
        monkeypatch.setattr(publication_module.os, "open", fail_building_open)
        return handle

    monkeypatch.setattr(
        publication_module,
        "observe_publication_root",
        observe_then_fail_building_open,
    )
    with pytest.raises(PaperMigrationPreCommitError) as caught:
        publication_module.begin_paper_migration_publication(
            publication_root,
            root_policy=policy,
            publication_nonce=nonce,
        )

    assert caught.value.orphan is not None
    assert caught.value.orphan.building_name == building_name
    assert caught.value.orphan.failed_phase == "workspace_creation"
    assert (publication_root / "generations" / building_name).is_dir()


def test_migration_result_assembly_failure_is_post_commit_indeterminate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_sha256 = hashlib.sha256(source.path.read_bytes()).hexdigest()
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)

    def fail_result_assembly(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("injected migration result assembly failure")

    monkeypatch.setattr(migration_module, "PaperOfflineMigrationResult", fail_result_assembly)
    with pytest.raises(PaperMigrationPostCommitIndeterminateError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )

    state = caught.value.state
    assert state.reason == "RESULT_ASSEMBLY_FAILED"
    assert (publication_root / "generations" / state.generation_name).is_dir()
    assert hashlib.sha256(source.path.read_bytes()).hexdigest() == source_sha256
    receipt = recover_paper_migration_publication(state, root_policy=policy)
    assert receipt.publication_state == "GENERATION_DURABLE_VERIFIED"


def test_manifest_raw_bytes_are_canonical_and_hash_graph_is_acyclic(tmp_path: Path) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    receipt = result.publication
    manifest_path = _generation_path(result, root) / "publication-manifest.json"
    raw = manifest_path.read_bytes()

    assert raw == canonical_manifest_bytes(receipt.manifest)
    assert hashlib.sha256(raw).hexdigest() == receipt.manifest_sha256
    assert "manifest_sha256" not in type(receipt.manifest).model_fields
    assert "receipt_sha256" not in type(receipt.manifest).model_fields
    assert receipt.receipt_sha256 == publication_module.canonical_sha256(
        receipt.model_dump(mode="python", exclude={"receipt_sha256"})
    )
    for forbidden_field in ("unknown", "manifest_sha256", "receipt_sha256", "receipt"):
        payload = json.loads(raw)
        payload[forbidden_field] = True
        with pytest.raises(ValueError, match="manifest is invalid"):
            parse_canonical_manifest(json.dumps(payload, sort_keys=True).encode())
    duplicate = raw.rstrip()[:-1] + b',"contract":"duplicate"}\n'
    with pytest.raises(ValueError, match="duplicate"):
        parse_canonical_manifest(duplicate)
    with pytest.raises(ValueError, match="not canonical"):
        parse_canonical_manifest(raw.rstrip())
    receipt_payload = receipt.model_dump(mode="json")
    receipt_payload["receipt_sha256"] = "0" * 64
    with pytest.raises(ValidationError):
        type(receipt).model_validate(receipt_payload)
    wrong_digest_receipt = receipt.model_copy(update={"manifest_sha256": "0" * 64})
    with pytest.raises(PaperMigrationMaterializationError):
        materialize_paper_migration_for_audit(
            wrong_digest_receipt,
            root_policy=policy,
            staging_root=staging,
        )


def test_receipt_is_unsigned_self_consistency_only(tmp_path: Path) -> None:
    result, _policy, _staging, _source, _root = _publish(tmp_path)
    combined_fields = {
        *type(result).model_fields,
        *type(result.publication).model_fields,
        *type(result.publication.manifest).model_fields,
    }
    forbidden = {"signature", "key_id", "key_loader", "authorization", "display_path"}
    assert combined_fields.isdisjoint(forbidden)
    assert result.anchor_state == "CURRENT_HEAD_UNANCHORED"
    assert result.promotion_allowed is False


@pytest.mark.parametrize(
    "fault_point",
    (
        "source_preflight",
        "after_object_noreplace_rename",
        "after_manifest_fsync",
        "before_local_failure_disposition",
    ),
)
def test_local_audit_failure_disposition_never_unlinks_any_named_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_point: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    calls: list[str] = []

    def forbidden(*_args: object, **_kwargs: object) -> None:
        calls.append("destructive")
        raise AssertionError("publication attempted destructive cleanup")

    original_begin = migration_module.begin_paper_migration_publication

    def begin_then_trap_cleanup(*args: object, **kwargs: object) -> object:
        context = original_begin(*args, **kwargs)
        monkeypatch.setattr(os, "unlink", forbidden)
        monkeypatch.setattr(os, "rmdir", forbidden)
        return context

    monkeypatch.setattr(
        migration_module,
        "begin_paper_migration_publication",
        begin_then_trap_cleanup,
    )
    with pytest.raises(PaperMigrationPreCommitError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
            failure_after_phase=fault_point,
        )

    assert caught.value.orphan is not None
    assert not calls
    assert (publication_root / "generations" / caught.value.orphan.building_name).is_dir()


def test_local_audit_root_policy_exact_modes_creation_and_revalidation(tmp_path: Path) -> None:
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    handle = observe_publication_root(policy, create_generations=True)
    observation = handle.observation
    handle.close()

    assert observation.root.mode == 0o700
    assert observation.generations.mode == 0o700
    assert observation.capabilities.acl_state == "UNOBSERVED_LOCAL_AUDIT"
    handle = observe_publication_root(policy, create_generations=False)
    handle.close()
    with pytest.raises(ValueError, match="effective UID"):
        observe_publication_root(
            policy.model_copy(update={"owner_uid": os.geteuid() + 1}),
            create_generations=False,
        )
    with pytest.raises(ValueError, match="effective GID"):
        observe_publication_root(
            policy.model_copy(update={"group_gid": os.getegid() + 1}),
            create_generations=False,
        )
    publication_root.chmod(0o750)
    with pytest.raises(ValueError, match="root identity or mode"):
        observe_publication_root(policy, create_generations=False)
    physical_root = _private_directory(tmp_path / "physical-publication")
    symlink_root = tmp_path / "symlink-publication"
    symlink_root.symlink_to(physical_root, target_is_directory=True)
    with pytest.raises(OSError):
        observe_publication_root(
            local_audit_publication_root_policy(symlink_root),
            create_generations=True,
        )


@pytest.mark.parametrize("entry", ("object", "manifest"))
def test_consumer_rejects_receipt_synchronized_file_mode_policy_violation(
    tmp_path: Path,
    entry: str,
) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    receipt = result.publication
    generation = _generation_path(result, root)
    object_path = _object_path(result, root)
    manifest_path = generation / "publication-manifest.json"
    generation.chmod(0o700)
    manifest_path.chmod(0o600)
    manifest = receipt.manifest
    if entry == "object":
        object_path.chmod(0o600)
        manifest = manifest.model_copy(
            update={"object_identity": publication_module._file_identity(object_path.stat())}
        )
    raw_manifest = canonical_manifest_bytes(manifest)
    manifest_path.write_bytes(raw_manifest)
    if entry == "object":
        manifest_path.chmod(0o400)
    generation.chmod(0o500)
    forged_receipt = type(receipt)(
        manifest=manifest,
        manifest_sha256=hashlib.sha256(raw_manifest).hexdigest(),
        manifest_identity=publication_module._file_identity(manifest_path.stat()),
    )

    with pytest.raises(PaperMigrationMaterializationError):
        materialize_paper_migration_for_audit(
            forged_receipt,
            root_policy=policy,
            staging_root=staging,
        )


def test_separated_identity_policy_requires_preprovisioning_and_acl_observer(
    tmp_path: Path,
) -> None:
    trusted = _private_directory(tmp_path / "trusted", 0o750)
    root = _private_directory(trusted / "publication", 0o750)
    _private_directory(root / "generations", 0o750)
    policy = PublicationRootPolicy(
        profile="SEPARATED_IDENTITY",
        publication_root=root,
        trusted_base=trusted,
        owner_uid=os.geteuid(),
        group_gid=os.getegid(),
        reader_gid=os.getegid(),
        trusted_base_owner_uid=os.geteuid(),
        trusted_base_group_gid=os.getegid(),
        trusted_base_mode=0o750,
        allow_create_generations=False,
        root_mode=0o750,
        generations_mode=0o750,
        building_mode=0o700,
        committed_generation_mode=0o550,
        object_mode=0o440,
        manifest_mode=0o440,
        acl_requirement="REQUIRE_NO_EXTENDED_ACL",
    )
    with pytest.raises(ValueError, match="ACL observer"):
        observe_publication_root(policy, create_generations=False)
    handle = observe_publication_root(
        policy,
        create_generations=False,
        acl_observer=lambda _path: ("NO_EXTENDED_ACL", None),
    )
    handle.close()
    with pytest.raises(ValueError, match="unacceptable extended ACL"):
        observe_publication_root(
            policy,
            create_generations=False,
            acl_observer=lambda _path: ("APPROVED_ACL_DIGEST", "a" * 64),
        )
    approved_policy = policy.model_copy(
        update={
            "acl_requirement": "REQUIRE_APPROVED_ACL_DIGEST",
            "approved_acl_digest": "a" * 64,
        }
    )
    with pytest.raises(ValueError, match="ACL digest is not approved"):
        observe_publication_root(
            approved_policy,
            create_generations=False,
            acl_observer=lambda _path: ("APPROVED_ACL_DIGEST", "b" * 64),
        )
    missing_generations_root = _private_directory(trusted / "without-generations", 0o750)
    missing_generations_policy = policy.model_copy(
        update={"publication_root": missing_generations_root}
    )
    with pytest.raises(ValueError, match="pre-provisioned"):
        observe_publication_root(
            missing_generations_policy,
            create_generations=True,
            acl_observer=lambda _path: ("NO_EXTENDED_ACL", None),
        )
    root.chmod(0o700)
    with pytest.raises(ValueError, match="root identity or mode"):
        observe_publication_root(
            policy,
            create_generations=False,
            acl_observer=lambda _path: ("NO_EXTENDED_ACL", None),
        )


def test_actual_platform_publication_capabilities(tmp_path: Path) -> None:
    source_dir = _private_directory(tmp_path / "source")
    destination_dir = _private_directory(tmp_path / "destination")
    source_fd = os.open(source_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    destination_fd = os.open(destination_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    marked_success = False

    def mark_success() -> None:
        nonlocal marked_success
        marked_success = True

    try:
        object_fd = os.open(
            "object-ready",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=source_fd,
        )
        try:
            os.write(object_fd, b"object-one")
            os.fsync(object_fd)
        finally:
            os.close(object_fd)
        rename_noreplace_at(
            source_fd,
            "object-ready",
            destination_fd,
            "ledger-object",
            on_success=mark_success,
        )
        os.fsync(destination_fd)
        os.fsync(source_fd)
        assert marked_success
        assert (destination_dir / "ledger-object").read_bytes() == b"object-one"

        colliding_object_fd = os.open(
            "object-collision",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=source_fd,
        )
        try:
            os.write(colliding_object_fd, b"object-two")
            os.fsync(colliding_object_fd)
        finally:
            os.close(colliding_object_fd)
        marked_success = False
        with pytest.raises(FileExistsError):
            rename_noreplace_at(
                source_fd,
                "object-collision",
                destination_fd,
                "ledger-object",
                on_success=mark_success,
            )
        os.fsync(destination_fd)
        os.fsync(source_fd)
        assert not marked_success
        assert (destination_dir / "ledger-object").read_bytes() == b"object-one"
        assert (source_dir / "object-collision").read_bytes() == b"object-two"

        os.mkdir("ready-generation", 0o700, dir_fd=source_fd)
        ready_fd = os.open(
            "ready-generation",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=source_fd,
        )
        try:
            os.fsync(ready_fd)
        finally:
            os.close(ready_fd)
        marked_success = False
        rename_noreplace_at(
            source_fd,
            "ready-generation",
            destination_fd,
            "generation",
            on_success=mark_success,
        )
        os.fsync(destination_fd)
        os.fsync(source_fd)
        assert marked_success
        assert (destination_dir / "generation").is_dir()

        os.mkdir("ready-collision", 0o700, dir_fd=source_fd)
        marked_success = False
        with pytest.raises(FileExistsError):
            rename_noreplace_at(
                source_fd,
                "ready-collision",
                destination_fd,
                "generation",
                on_success=mark_success,
            )
        os.fsync(destination_fd)
        os.fsync(source_fd)
        assert not marked_success
        assert (destination_dir / "generation").is_dir()
        assert (source_dir / "ready-collision").is_dir()
    finally:
        os.close(destination_fd)
        os.close(source_fd)


def test_materialization_mutation_after_first_hash_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    object_path = _object_path(result, root)
    original_checkpoint = publication_module._checkpoint

    def mutate(fault: str | None, phase: str) -> None:
        if phase == "after_materialization_first_object_hash":
            object_path.chmod(0o600)
            with object_path.open("ab") as stream:
                stream.write(b"mutation")
        original_checkpoint(fault, phase)

    monkeypatch.setattr(publication_module, "_checkpoint", mutate)
    with pytest.raises(PaperMigrationMaterializationError):
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )


def test_materialization_mutation_during_copy_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    object_path = _object_path(result, root)
    original_checkpoint = publication_module._checkpoint

    def mutate(fault: str | None, phase: str) -> None:
        if phase == "during_materialization_copy":
            object_path.chmod(0o600)
            with object_path.open("ab") as stream:
                stream.write(b"mutation")
        original_checkpoint(fault, phase)

    monkeypatch.setattr(publication_module, "_checkpoint", mutate)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )
    assert caught.value.orphan is not None
    orphan = staging / caught.value.orphan.private_name
    assert orphan.is_file()
    assert stat.S_IMODE(orphan.stat().st_mode) == 0o600


def test_materialization_destination_collision_does_not_claim_foreign_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    suffix = "b" * 64
    private_name = (
        f"paper-migration-audit-{result.publication.manifest.publication_nonce}-{suffix}.sqlite3"
    )
    existing = staging / private_name
    existing.write_bytes(b"foreign private file")
    monkeypatch.setattr(publication_module.secrets, "token_hex", lambda _size: suffix)

    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )

    assert caught.value.orphan is None
    assert existing.read_bytes() == b"foreign private file"


def test_materialization_rejects_private_destination_mode_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    original_fchmod = os.fchmod

    def force_wrong_private_mode(descriptor: int, mode: int) -> None:
        original_fchmod(descriptor, 0o640 if mode == 0o600 else mode)

    monkeypatch.setattr(publication_module.os, "fchmod", force_wrong_private_mode)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )

    assert caught.value.orphan is not None
    orphan = staging / caught.value.orphan.private_name
    assert stat.S_IMODE(orphan.stat().st_mode) == 0o640


def test_only_verified_private_materialization_reaches_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    sqlite_opens: list[str] = []
    original_connect = sqlite3.connect

    def tracked_connect(database: object, *args: object, **kwargs: object) -> sqlite3.Connection:
        sqlite_opens.append(os.fspath(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(publication_module.sqlite3, "connect", tracked_connect)
    materialized = materialize_paper_migration_for_audit(
        result.publication,
        root_policy=policy,
        staging_root=staging,
    )
    assert materialized.private_path.parent == staging
    assert materialized.private_path != _object_path(result, root)
    assert materialized.materialized_sha256 == result.publication.manifest.candidate_sha256
    assert tuple(sorted(path.name for path in _generation_path(result, root).iterdir())) == (
        result.publication.manifest.object_name,
        "publication-manifest.json",
    )
    assert sqlite_opens == [f"file:{materialized.private_path}?mode=ro"]
    assert str(_generation_path(result, root)) not in sqlite_opens[0]
    with original_connect(materialized.private_path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_legacy_candidate_api_and_result_fields_are_absent(tmp_path: Path) -> None:
    signature = inspect.signature(migrate_v4_ledger_copy)
    assert "candidate_path" not in signature.parameters
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    with pytest.raises(TypeError):
        migrate_v4_ledger_copy(source.path, publication_root)
    with pytest.raises(TypeError):
        migrate_v4_ledger_copy(
            source.path,
            candidate_path=publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )
    result = migrate_v4_ledger_copy(
        source.path,
        publication_root,
        root_policy=policy,
        migration_code_identity="test-migration-code",
    )
    for field in ("candidate_path", "candidate_sha256", "display_path", "source_path"):
        with pytest.raises(AttributeError):
            getattr(result, field)


@pytest.mark.parametrize("extra_name", ("transformed.sqlite3-wal", "extra", "link"))
def test_publication_rejects_wal_sidecars_and_unexpected_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extra_name: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_sha = hashlib.sha256(source.path.read_bytes()).hexdigest()
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_checkpoint = migration_module._checkpoint

    def inject(fault: str | None, phase: str) -> None:
        if phase == "after_sqlite_connections_closed":
            building = next((publication_root / "generations").glob(".building-*"))
            target = building / "ready" / extra_name
            if extra_name == "link":
                target.symlink_to("transformed.sqlite3")
            else:
                target.write_bytes(b"unexpected")
        original_checkpoint(fault, phase)

    monkeypatch.setattr(migration_module, "_checkpoint", inject)
    with pytest.raises(PaperMigrationPreCommitError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )
    assert caught.value.orphan is not None
    assert hashlib.sha256(source.path.read_bytes()).hexdigest() == source_sha
    assert not tuple((publication_root / "generations").glob("generation-*"))


@pytest.mark.parametrize("collision", ("object", "generation"))
def test_object_and_generation_noreplace_collisions_preserve_existing_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collision: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_rename = publication_module.rename_noreplace_at
    preserved = b"pre-existing collision"

    def collide(
        source_fd: int,
        source_name: str,
        destination_fd: int,
        destination_name: str,
        *,
        on_success: object | None = None,
    ) -> None:
        should_collide = (collision == "object" and source_name == "transformed.sqlite3") or (
            collision == "generation" and source_name == "ready"
        )
        if should_collide:
            if collision == "object":
                descriptor = os.open(
                    destination_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=destination_fd,
                )
                os.write(descriptor, preserved)
                os.close(descriptor)
            else:
                os.mkdir(destination_name, 0o700, dir_fd=destination_fd)
        original_rename(
            source_fd,
            source_name,
            destination_fd,
            destination_name,
            on_success=on_success,
        )

    monkeypatch.setattr(publication_module, "rename_noreplace_at", collide)
    with pytest.raises(PaperMigrationPreCommitError):
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )
    if collision == "object":
        colliding = next((publication_root / "generations").glob(".building-*/ready/ledger-*"))
        assert colliding.read_bytes() == preserved
    else:
        colliding = next((publication_root / "generations").glob("generation-*"))
        assert colliding.is_dir()


def test_publication_contract_contains_no_current_pointer() -> None:
    source = inspect.getsource(publication_module)
    assert "current.json" not in source
    assert "current_pointer" not in source
    assert not any("current" in name.lower() for name in publication_module.__all__)


@pytest.mark.parametrize(
    "replacement_kind",
    ("regular", "symlink", "hardlink", "directory", "generation", "generations", "root"),
)
def test_consumer_rejects_regular_symlink_hardlink_and_directory_swap(
    tmp_path: Path,
    replacement_kind: str,
) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    object_path = _object_path(result, root)
    generation = object_path.parent
    replacement_path: Path
    if replacement_kind in {"regular", "symlink", "hardlink", "directory"}:
        generation.chmod(0o700)
        parked = generation / "parked-object"
        object_path.rename(parked)
        replacement_path = object_path
        if replacement_kind == "regular":
            object_path.write_bytes(b"replacement")
        elif replacement_kind == "symlink":
            object_path.symlink_to(parked.name)
        elif replacement_kind == "hardlink":
            os.link(parked, object_path)
        else:
            object_path.mkdir()
        generation.chmod(0o500)
    elif replacement_kind == "generation":
        parked = generation.with_name("parked-generation")
        generation.chmod(0o700)
        generation.rename(parked)
        parked.chmod(0o500)
        generation.mkdir(mode=0o500)
        replacement_path = generation
    elif replacement_kind == "generations":
        generations = generation.parent
        parked = generations.with_name("parked-generations")
        generations.rename(parked)
        generations.mkdir(mode=0o700)
        replacement_path = generations
    else:
        parked = root.with_name("parked-publication")
        root.rename(parked)
        root.mkdir(mode=0o700)
        (root / "generations").mkdir(mode=0o700)
        replacement_path = root

    with pytest.raises(PaperMigrationMaterializationError):
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )
    assert os.path.lexists(replacement_path)
