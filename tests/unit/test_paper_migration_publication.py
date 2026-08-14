from __future__ import annotations

import ast
import base64
import errno
import hashlib
import inspect
import json
import os
import shutil
import sqlite3
import stat
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant._paper_sqlite_image as sqlite_image_module
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


def test_rqs8_arch_p1_001_recovery_rejects_replaced_building_before_either_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
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
    assert state.contract == "rquant-paper-migration-post-commit/v2"
    generations = publication_root / "generations"
    building = generations / state.building_name
    parked = generations / "parked-original-building"
    building.rename(parked)
    building.mkdir(mode=policy.building_mode)
    building.chmod(policy.building_mode)
    fsyncs: list[int] = []
    original_fsync = os.fsync

    def tracked_fsync(descriptor: int) -> None:
        fsyncs.append(os.fstat(descriptor).st_ino)
        original_fsync(descriptor)

    monkeypatch.setattr(publication_module.os, "fsync", tracked_fsync)
    with pytest.raises(PaperMigrationPostCommitIndeterminateError):
        recover_paper_migration_publication(state, root_policy=policy)

    assert fsyncs == []
    assert parked.is_dir()
    assert building.is_dir()


def test_recovery_fsyncs_verified_building_before_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
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
    generations = publication_root / "generations"
    building = generations / state.building_name
    fsyncs: list[int] = []
    original_fsync = os.fsync

    def tracked_fsync(descriptor: int) -> None:
        fsyncs.append(os.fstat(descriptor).st_ino)
        original_fsync(descriptor)

    monkeypatch.setattr(publication_module.os, "fsync", tracked_fsync)
    receipt = recover_paper_migration_publication(state, root_policy=policy)

    assert receipt.publication_state == "GENERATION_DURABLE_VERIFIED"
    assert fsyncs == [building.stat().st_ino, generations.stat().st_ino]


def test_post_commit_state_is_strict_v2_and_rejects_v1_payload_before_recovery(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
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

    payload = caught.value.state.model_dump(mode="python")
    payload["contract"] = "rquant-paper-migration-post-commit/v1"
    with pytest.raises(ValidationError):
        publication_module.PaperMigrationPostCommitState.model_validate(payload)


def test_recovery_rejects_unparsed_v1_state_before_opening_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
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

    legacy_payload = caught.value.state.model_dump(mode="python")
    legacy_payload["contract"] = "rquant-paper-migration-post-commit/v1"
    opened_root = False

    def reject_root_open(*args: object, **kwargs: object) -> object:
        nonlocal opened_root
        opened_root = True
        raise AssertionError("recovery must parse the state before opening the publication root")

    monkeypatch.setattr(publication_module, "observe_publication_root", reject_root_open)
    with pytest.raises(ValidationError):
        recover_paper_migration_publication(legacy_payload, root_policy=policy)  # type: ignore[arg-type]
    assert opened_root is False


def test_post_rename_seam_is_immediate_and_observes_declared_generation_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_begin = migration_module._begin_paper_migration_publication
    original_rename = publication_module.rename_noreplace_at
    original_open = os.open
    original_read = os.read
    original_close = os.close
    original_fchmod = os.fchmod
    original_fchown = os.fchown
    original_fstat = os.fstat
    original_fsync = os.fsync
    original_write = os.write
    original_mkdir = os.mkdir
    original_unlink = os.unlink
    original_rmdir = os.rmdir
    original_os_rename = os.rename
    original_checkpoint = publication_module._checkpoint
    generation_rename_returned = False
    seam_entered = False
    post_rename_operations: list[str] = []
    callback_observed: list[bool] = []
    rename_identities: list[publication_module.PublicationStableDirectoryIdentity] = []
    manifest_identities: list[publication_module.PublicationStableDirectoryIdentity] = []
    seam_identities: list[publication_module.PublicationStableDirectoryIdentity] = []

    def forbidden_identity_projection(
        _self: object,
        *args: object,
        **kwargs: object,
    ) -> object:
        raise AssertionError("generation identity must come from post-mode fstat")

    def tracked_rename(
        source_fd: int,
        source_name: str,
        destination_fd: int,
        destination_name: str,
        *,
        on_success: object | None = None,
    ) -> None:
        nonlocal generation_rename_returned
        if generation_rename_returned and not seam_entered:
            post_rename_operations.append("rename_noreplace_at")
        is_generation = source_name == "ready" and destination_name.startswith("generation-")
        callback_called = False

        if is_generation:
            ready_fd = original_open(
                source_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=source_fd,
            )
            manifest_fd = original_open(
                "publication-manifest.json",
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=ready_fd,
            )
            try:
                identity = publication_module._directory_identity(original_fstat(ready_fd))
                raw_manifest_parts: list[bytes] = []
                while chunk := original_read(manifest_fd, 65536):
                    raw_manifest_parts.append(chunk)
                manifest = publication_module.parse_canonical_manifest(b"".join(raw_manifest_parts))
                rename_identities.append(identity)
                manifest_identities.append(manifest.generation_identity)
                assert identity.mode == policy.committed_generation_mode
            finally:
                original_close(manifest_fd)
                original_close(ready_fd)

        def tracked_success() -> None:
            nonlocal callback_called
            assert on_success is not None
            on_success()
            callback_called = True

        original_rename(
            source_fd,
            source_name,
            destination_fd,
            destination_name,
            on_success=tracked_success if on_success is not None else None,
        )
        if is_generation:
            callback_observed.append(callback_called)
            generation_rename_returned = True

    def record_post_rename(operation: str) -> None:
        if generation_rename_returned and not seam_entered:
            post_rename_operations.append(operation)

    def tracked_fchmod(descriptor: int, mode: int) -> None:
        record_post_rename("fchmod")
        original_fchmod(descriptor, mode)

    def tracked_fchown(descriptor: int, uid: int, gid: int) -> None:
        record_post_rename("fchown")
        original_fchown(descriptor, uid, gid)

    def tracked_fstat(descriptor: int) -> os.stat_result:
        record_post_rename("fstat")
        return original_fstat(descriptor)

    def tracked_fsync(descriptor: int) -> None:
        record_post_rename("fsync")
        original_fsync(descriptor)

    def tracked_open(*args: object, **kwargs: object) -> int:
        record_post_rename("open")
        return original_open(*args, **kwargs)

    def tracked_write(descriptor: int, data: bytes) -> int:
        record_post_rename("write")
        return original_write(descriptor, data)

    def tracked_mkdir(*args: object, **kwargs: object) -> None:
        record_post_rename("mkdir")
        original_mkdir(*args, **kwargs)

    def tracked_unlink(*args: object, **kwargs: object) -> None:
        record_post_rename("unlink")
        original_unlink(*args, **kwargs)

    def tracked_rmdir(*args: object, **kwargs: object) -> None:
        record_post_rename("rmdir")
        original_rmdir(*args, **kwargs)

    def tracked_os_rename(*args: object, **kwargs: object) -> None:
        record_post_rename("rename")
        original_os_rename(*args, **kwargs)

    def begin_then_enable_operation_tracking(*args: object, **kwargs: object) -> object:
        context = original_begin(*args, **kwargs)
        monkeypatch.setattr(publication_module.os, "open", tracked_open)
        monkeypatch.setattr(publication_module.os, "fchmod", tracked_fchmod)
        monkeypatch.setattr(publication_module.os, "fchown", tracked_fchown)
        monkeypatch.setattr(publication_module.os, "fstat", tracked_fstat)
        monkeypatch.setattr(publication_module.os, "fsync", tracked_fsync)
        monkeypatch.setattr(publication_module.os, "write", tracked_write)
        monkeypatch.setattr(publication_module.os, "mkdir", tracked_mkdir)
        monkeypatch.setattr(publication_module.os, "unlink", tracked_unlink)
        monkeypatch.setattr(publication_module.os, "rmdir", tracked_rmdir)
        monkeypatch.setattr(publication_module.os, "rename", tracked_os_rename)
        monkeypatch.setattr(
            publication_module.os,
            "supports_dir_fd",
            os.supports_dir_fd | {tracked_open, tracked_unlink, tracked_os_rename},
        )
        return context

    def inspect_immediate_seam(fault: str | None, phase: str) -> None:
        nonlocal seam_entered
        if phase == "after_generation_rename_before_parent_fsync":
            assert generation_rename_returned
            seam_entered = True
            generation = next((publication_root / "generations").glob("generation-*"))
            seam_identities.append(publication_module._directory_identity(generation.stat()))
        original_checkpoint(fault, phase)

    monkeypatch.setattr(
        publication_module.PublicationStableDirectoryIdentity,
        "model_copy",
        forbidden_identity_projection,
    )
    monkeypatch.setattr(
        migration_module,
        "_begin_paper_migration_publication",
        begin_then_enable_operation_tracking,
    )
    monkeypatch.setattr(publication_module, "rename_noreplace_at", tracked_rename)
    monkeypatch.setattr(publication_module, "_checkpoint", inspect_immediate_seam)

    with pytest.raises(PaperMigrationPostCommitIndeterminateError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
            failure_after_phase="after_generation_rename_before_parent_fsync",
        )

    assert post_rename_operations == []
    assert callback_observed == [True]
    assert rename_identities == manifest_identities == seam_identities
    receipt = recover_paper_migration_publication(caught.value.state, root_policy=policy)
    assert receipt.publication_state == "GENERATION_DURABLE_VERIFIED"
    assert receipt.manifest.generation_identity == seam_identities[0]
    assert receipt.manifest.generation_identity.mode == policy.committed_generation_mode


def test_ready_final_mode_failure_is_precommit_before_manifest_and_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_sha256 = hashlib.sha256(source.path.read_bytes()).hexdigest()
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_begin = migration_module._begin_paper_migration_publication
    original_fchmod = os.fchmod
    destructive_calls: list[str] = []
    ready_fd: int | None = None

    def begin_then_reject_final_mode(*args: object, **kwargs: object) -> object:
        nonlocal ready_fd
        context = original_begin(*args, **kwargs)
        ready_fd = context.ready_fd
        monkeypatch.setattr(publication_module.os, "fchmod", fail_ready_final_mode)
        monkeypatch.setattr(publication_module.os, "unlink", forbidden_destructive)
        monkeypatch.setattr(publication_module.os, "rmdir", forbidden_destructive)
        return context

    def fail_ready_final_mode(descriptor: int, mode: int) -> None:
        if descriptor == ready_fd and mode == policy.committed_generation_mode:
            raise PermissionError(errno.EACCES, "injected ready final-mode failure")
        original_fchmod(descriptor, mode)

    def forbidden_destructive(*_args: object, **_kwargs: object) -> None:
        destructive_calls.append("destructive")
        raise AssertionError("publication attempted destructive cleanup")

    monkeypatch.setattr(
        migration_module,
        "_begin_paper_migration_publication",
        begin_then_reject_final_mode,
    )

    with pytest.raises(PaperMigrationPreCommitError) as caught:
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )

    assert caught.value.orphan is not None
    orphan = publication_root / "generations" / caught.value.orphan.building_name
    assert orphan.is_dir()
    assert not (orphan / "ready" / "publication-manifest.json").exists()
    assert not list((publication_root / "generations").glob("generation-*"))
    assert hashlib.sha256(source.path.read_bytes()).hexdigest() == source_sha256
    assert destructive_calls == []


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
        publication_module._begin_paper_migration_publication(
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


def test_v1_manifest_and_receipt_fixed_fixture_bytes_are_unchanged() -> None:
    raw_manifest = (
        base64.b64decode(
            "eyJjYW5kaWRhdGVfc2hhMjU2IjoiY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2M"
            "iLCJjb250cmFjdCI6InJxdWFudC1wYXBlci1taWdyYXRpb24tcHVibGljYXRpb24tbWFuaWZlc3QvdjEiLCJnZW5lcmF0aW9uX2lkZW50aXR5Ijp7ImRldmljZSI6MSwiZmlsZV90eXBlIjoiZGlyZWN0b3J5IiwiZ2lkIjo4LCJpbm9kZSI6MTMsIm1vZGUiOjQ0OCwidWlkIjo3fSwiZ2VuZXJhdGlvbl9uYW1lIjoiZ2VuZXJhdGlvbi1iYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmIiLCJpbnZlbnRvcnkiOlsibGVkZ2VyLWNjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2Muc3FsaXRlMyIsInB1YmxpY2F0aW9uLW1hbmlmZXN0Lmpzb24iXSwibWlncmF0aW9uX2FsZ29yaXRobV9pZCI6InBhcGVyLWxlZGdlci12NC10by12NS1hcmNoaXZlLXYyIiwibWlncmF0aW9uX2F0dGVzdGF0aW9uX2RpZ2VzdCI6ImZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmYiLCJtaWdyYXRpb25fY29kZV9pZGVudGl0eSI6ImZpeGVkLXYxLWZpeHR1cmUiLCJvYmplY3RfaWRlbnRpdHkiOnsiY3RpbWVfbnMiOjEwMiwiZGV2aWNlIjoxLCJmaWxlX3R5cGUiOiJyZWd1bGFyIiwiZ2lkIjo4LCJpbm9kZSI6MTQsIm1vZGUiOjI1NiwibXRpbWVfbnMiOjEwMSwibmxpbmsiOjEsInNpemUiOjEyMzQsInVpZCI6N30sIm9iamVjdF9uYW1lIjoibGVkZ2VyLWNjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2Muc3FsaXRlMyIsInBvbGljeV9wcm9maWxlIjoiTE9DQUxfQVVESVQiLCJwdWJsaWNhdGlvbl9ub25jZSI6ImJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmIiLCJyb290X29ic2VydmF0aW9uIjp7ImNhcGFiaWxpdGllcyI6eyJhY2xfZGlnZXN0IjpudWxsLCJhY2xfc3RhdGUiOiJVTk9CU0VSVkVEX0xPQ0FMX0FVRElUIiwiZGlyX2ZkX29wZW4iOnRydWUsImRpcl9mZF9yZW5hbWUiOnRydWUsImRpcl9mZF9zdGF0Ijp0cnVlLCJkaXJfZmRfdW5saW5rIjp0cnVlLCJkaXJlY3RvcnlfZnN5bmMiOnRydWUsImZpbGVfZnN5bmMiOnRydWUsIm5vX3JlcGxhY2VfcHJpbWl0aXZlIjoicmVuYW1lYXR4X25wL1JFTkFNRV9FWENMIiwib19kaXJlY3RvcnkiOnRydWUsIm9fbm9mb2xsb3ciOnRydWUsInBsYXRmb3JtIjoiZGFyd2luIn0sImVmZmVjdGl2ZV9naWQiOjgsImVmZmVjdGl2ZV91aWQiOjcsImdlbmVyYXRpb25zIjp7ImRldmljZSI6MSwiZmlsZV90eXBlIjoiZGlyZWN0b3J5IiwiZ2lkIjo4LCJpbm9kZSI6MTIsIm1vZGUiOjQ0OCwidWlkIjo3fSwicG9saWN5X2lkIjoiYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYSIsInJvb3QiOnsiZGV2aWNlIjoxLCJmaWxlX3R5cGUiOiJkaXJlY3RvcnkiLCJnaWQiOjgsImlub2RlIjoxMSwibW9kZSI6NDQ4LCJ1aWQiOjd9fSwic291cmNlX3NoYTI1NiI6ImRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGRkZGQiLCJ0YXJnZXRfaW50ZXJuYWxfbWlncmF0aW9uX3ZlcnNpb24iOjQsInRhcmdldF9zY2hlbWFfaWRlbnRpdHkiOiJmaXhlZC10YXJnZXQtc2NoZW1hIiwidGFyZ2V0X3NjaGVtYV92ZXJzaW9uIjo1LCJ2NF9yZWNvbmNpbGlhdGlvbl9yZXBvcnRfZGlnZXN0IjoiZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZSJ9Cg=="
        )
        .replace(b"b" * 66, b"b" * 64)
        .replace(b"c" * 71, b"c" * 64)
        .replace(b"c" * 67, b"c" * 64)
        .replace(b"f" * 67, b"f" * 64)
    )
    manifest = parse_canonical_manifest(raw_manifest)
    assert canonical_manifest_bytes(manifest) == raw_manifest
    assert hashlib.sha256(raw_manifest).hexdigest() == (
        "3b9ac1787d77cb239dd3c8c1ecb4440cb0c9b8b4155c6e3970696c1c3dbe56a0"
    )
    receipt = publication_module.PaperMigrationPublicationReceipt(
        manifest=manifest,
        manifest_sha256=hashlib.sha256(raw_manifest).hexdigest(),
        manifest_identity=publication_module.PublicationFileIdentity(
            device=1,
            inode=15,
            uid=7,
            gid=8,
            mode=0o400,
            nlink=1,
            size=len(raw_manifest),
            mtime_ns=103,
            ctime_ns=104,
        ),
    )
    assert (
        receipt.receipt_sha256 == "39caed9c770f7cb186eb013b91edc2d6ab55dff66e6ac256790c6342e3557fc8"
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

    original_begin = migration_module._begin_paper_migration_publication

    def begin_then_trap_cleanup(*args: object, **kwargs: object) -> object:
        context = original_begin(*args, **kwargs)
        monkeypatch.setattr(os, "unlink", forbidden)
        monkeypatch.setattr(os, "rmdir", forbidden)
        return context

    monkeypatch.setattr(
        migration_module,
        "_begin_paper_migration_publication",
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
    assert policy.committed_generation_mode == 0o700
    legacy_policy = policy.model_dump(mode="python", exclude={"policy_id"})
    legacy_policy["committed_generation_mode"] = 0o500
    with pytest.raises(ValidationError, match="LOCAL_AUDIT publication policy is not exact"):
        PublicationRootPolicy.model_validate(legacy_policy)
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
    generation.chmod(policy.committed_generation_mode)
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
        committed_generation_mode=0o750,
        object_mode=0o440,
        manifest_mode=0o440,
        acl_requirement="REQUIRE_NO_EXTENDED_ACL",
    )
    assert policy.committed_generation_mode == 0o750
    legacy_policy = policy.model_dump(mode="python", exclude={"policy_id"})
    legacy_policy["committed_generation_mode"] = 0o550
    with pytest.raises(
        ValidationError,
        match="SEPARATED_IDENTITY publication policy is not exact",
    ):
        PublicationRootPolicy.model_validate(legacy_policy)
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


def test_actual_platform_publication_capabilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = _private_directory(tmp_path / "source")
    destination_dir = _private_directory(tmp_path / "destination")
    source_fd = os.open(source_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    destination_fd = os.open(destination_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    original_fsync = os.fsync
    fsync_calls = {"file": 0, "source_parent": 0, "destination_parent": 0}
    marked_success = False

    def tracked_fsync(descriptor: int) -> None:
        if descriptor == source_fd:
            fsync_calls["source_parent"] += 1
        elif descriptor == destination_fd:
            fsync_calls["destination_parent"] += 1
        elif stat.S_ISREG(os.fstat(descriptor).st_mode):
            fsync_calls["file"] += 1
        original_fsync(descriptor)

    def mark_success() -> None:
        nonlocal marked_success
        marked_success = True

    monkeypatch.setattr(os, "fsync", tracked_fsync)
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

        if sys.platform == "darwin":
            for mode in (0o500, 0o550):
                source_name = f"ready-denied-{mode:o}"
                destination_name = f"generation-denied-{mode:o}"
                os.mkdir(source_name, mode, dir_fd=source_fd)
                (source_dir / source_name).chmod(mode)
                marked_success = False
                with pytest.raises(OSError) as caught:
                    rename_noreplace_at(
                        source_fd,
                        source_name,
                        destination_fd,
                        destination_name,
                        on_success=mark_success,
                    )
                assert caught.value.errno == errno.EACCES
                assert not marked_success
                assert (source_dir / source_name).is_dir()
                assert not os.path.lexists(destination_dir / destination_name)
        else:
            assert sys.platform.startswith("linux")

        for mode in (0o700, 0o750):
            source_name = f"ready-generation-{mode:o}"
            destination_name = f"generation-{mode:o}"
            os.mkdir(source_name, mode, dir_fd=source_fd)
            (source_dir / source_name).chmod(mode)
            ready_fd = os.open(
                source_name,
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
                source_name,
                destination_fd,
                destination_name,
                on_success=mark_success,
            )
            os.fsync(source_fd)
            os.fsync(destination_fd)
            assert marked_success
            generation = destination_dir / destination_name
            assert generation.is_dir()
            assert stat.S_IMODE(generation.stat().st_mode) == mode

        os.mkdir("ready-collision", 0o700, dir_fd=source_fd)
        os.mkdir("generation-collision", 0o700, dir_fd=destination_fd)
        marked_success = False
        with pytest.raises(FileExistsError):
            rename_noreplace_at(
                source_fd,
                "ready-collision",
                destination_fd,
                "generation-collision",
                on_success=mark_success,
            )
        os.fsync(destination_fd)
        os.fsync(source_fd)
        assert not marked_success
        assert (destination_dir / "generation-collision").is_dir()
        assert (source_dir / "ready-collision").is_dir()
        assert fsync_calls["file"] >= 2
        assert fsync_calls["source_parent"] >= 1
        assert fsync_calls["destination_parent"] >= 1
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
    original_connect = sqlite3.connect
    sqlite_opens: list[str] = []

    def mutate(fault: str | None, phase: str) -> None:
        if phase == "after_materialization_first_object_hash":
            object_path.chmod(0o600)
            with object_path.open("ab") as stream:
                stream.write(b"mutation")
        original_checkpoint(fault, phase)

    def tracked_connect(database: object, *args: object, **kwargs: object) -> sqlite3.Connection:
        sqlite_opens.append(os.fspath(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(publication_module, "_checkpoint", mutate)
    monkeypatch.setattr(sqlite_image_module.sqlite3, "connect", tracked_connect)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )
    assert sqlite_opens == []
    assert caught.value.orphan is not None
    orphan = staging / caught.value.orphan.private_name
    assert orphan.is_file()
    assert stat.S_IMODE(orphan.stat().st_mode) == 0o600


def test_materialization_mutation_during_copy_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, root = _publish(tmp_path)
    object_path = _object_path(result, root)
    original_checkpoint = publication_module._checkpoint
    original_connect = sqlite3.connect
    sqlite_opens: list[str] = []

    def mutate(fault: str | None, phase: str) -> None:
        if phase == "during_materialization_copy":
            object_path.chmod(0o600)
            with object_path.open("ab") as stream:
                stream.write(b"mutation")
        original_checkpoint(fault, phase)

    def tracked_connect(database: object, *args: object, **kwargs: object) -> sqlite3.Connection:
        sqlite_opens.append(os.fspath(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(publication_module, "_checkpoint", mutate)
    monkeypatch.setattr(sqlite_image_module.sqlite3, "connect", tracked_connect)
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
    assert sqlite_opens == []


def test_materialization_final_destination_rehash_mismatch_prevents_sqlite_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    original_hash_fd = publication_module._hash_fd
    original_connect = sqlite3.connect
    hash_calls = 0
    sqlite_opens: list[str] = []

    def mismatch_final_destination(descriptor: int) -> tuple[str, int]:
        nonlocal hash_calls
        hash_calls += 1
        digest, size = original_hash_fd(descriptor)
        if hash_calls == 3:
            return "0" * 64, size
        return digest, size

    def tracked_connect(database: object, *args: object, **kwargs: object) -> sqlite3.Connection:
        sqlite_opens.append(os.fspath(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(publication_module, "_hash_fd", mismatch_final_destination)
    monkeypatch.setattr(sqlite_image_module.sqlite3, "connect", tracked_connect)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )

    assert hash_calls == 3
    assert sqlite_opens == []
    assert caught.value.orphan is not None
    orphan = staging / caught.value.orphan.private_name
    assert orphan.is_file()
    assert stat.S_IMODE(orphan.stat().st_mode) == 0o600


def test_rqs8_arch_p1_005_private_swap_after_memory_verification_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    original_checkpoint = publication_module._checkpoint
    original_open_memory = publication_module._open_memory_sqlite_image
    parked: Path | None = None
    substitute: Path | None = None
    memory_images: list[bytes] = []

    def track_memory_image(image: object) -> sqlite3.Connection:
        memory_images.append(image.data)
        return original_open_memory(image)

    def replace_private_after_memory_verification(fault: str | None, phase: str) -> None:
        nonlocal parked, substitute
        if phase == "after_private_memory_verification_before_final_rebind":
            private = next(staging.glob("paper-migration-audit-*.sqlite3"))
            parked = private.with_name("parked-private.sqlite3")
            substitute = private.with_name("substitute-private.sqlite3")
            private.rename(parked)
            shutil.copyfile(parked, substitute)
            with sqlite3.connect(substitute) as connection:
                connection.execute("CREATE TABLE substitute_marker(value TEXT NOT NULL)")
                connection.execute("INSERT INTO substitute_marker VALUES ('substituted')")
            substitute.replace(private)
        original_checkpoint(fault, phase)

    monkeypatch.setattr(
        publication_module,
        "_checkpoint",
        replace_private_after_memory_verification,
    )
    monkeypatch.setattr(publication_module, "_open_memory_sqlite_image", track_memory_image)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )

    assert parked is not None and parked.is_file()
    assert substitute is not None and substitute.exists() is False
    assert len(memory_images) == 1
    assert b"substitute_marker" not in memory_images[0]
    assert caught.value.orphan is not None
    substituted_private = staging / caught.value.orphan.private_name
    assert substituted_private.is_file()
    assert b"substitute_marker" in substituted_private.read_bytes()


def test_materialization_closes_final_private_rebind_before_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    original_open_regular_at = publication_module._open_regular_at
    rebound_descriptors: list[int] = []

    def tracked_open_regular_at(
        directory_descriptor: int,
        name: str,
        *,
        writable: bool = False,
    ) -> int:
        descriptor = original_open_regular_at(
            directory_descriptor,
            name,
            writable=writable,
        )
        if name.startswith("paper-migration-audit-"):
            rebound_descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(publication_module, "_open_regular_at", tracked_open_regular_at)
    materialize_paper_migration_for_audit(
        result.publication,
        root_policy=policy,
        staging_root=staging,
    )

    assert len(rebound_descriptors) == 1
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(rebound_descriptors[0])


@pytest.mark.parametrize("interleave", ("substitute", "mutate", "metadata"))
def test_v4_object_transition_rejects_interleaves_before_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interleave: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_sha256 = hashlib.sha256(source.path.read_bytes()).hexdigest()
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_checkpoint = publication_module._checkpoint
    original_fchmod = os.fchmod
    object_metadata_phase = False
    parked: Path | None = None

    def interleave_object(fault: str | None, phase: str) -> None:
        nonlocal object_metadata_phase, parked
        if phase == "after_object_rebind_before_metadata":
            object_metadata_phase = True
            building = next((publication_root / "generations").glob(".building-*"))
            object_path = next((building / "ready").glob("ledger-*.sqlite3"))
            if interleave == "substitute":
                parked = object_path.with_name("parked-object.sqlite3")
                object_path.rename(parked)
                shutil.copyfile(parked, object_path)
            elif interleave == "mutate":
                with object_path.open("r+b") as stream:
                    stream.seek(0)
                    stream.write(b"not-a-verified-sqlite-image")
        original_checkpoint(fault, phase)

    def fail_object_metadata(descriptor: int, mode: int) -> None:
        if interleave == "metadata" and object_metadata_phase and mode == policy.object_mode:
            raise PermissionError(errno.EACCES, "injected object metadata failure")
        original_fchmod(descriptor, mode)

    monkeypatch.setattr(publication_module, "_checkpoint", interleave_object)
    monkeypatch.setattr(publication_module.os, "fchmod", fail_object_metadata)
    with pytest.raises(PaperMigrationPreCommitError):
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )

    assert hashlib.sha256(source.path.read_bytes()).hexdigest() == source_sha256
    assert not tuple((publication_root / "generations").glob("generation-*"))
    if parked is not None:
        assert parked.is_file()


def test_rqs8_p1_009_financial_mutation_after_sqlite_close_is_precommit_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_sha256 = hashlib.sha256(source.path.read_bytes()).hexdigest()
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_checkpoint = migration_module._checkpoint

    def mutate_verified_transformed(fault: str | None, phase: str) -> None:
        if phase == "after_sqlite_connections_closed":
            building = next((publication_root / "generations").glob(".building-*"))
            transformed = building / "ready" / "transformed.sqlite3"
            with sqlite3.connect(transformed) as connection:
                connection.execute("UPDATE broker_account SET cash = '175233.7700'")
        original_checkpoint(fault, phase)

    monkeypatch.setattr(migration_module, "_checkpoint", mutate_verified_transformed)
    with pytest.raises(PaperMigrationPreCommitError):
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )

    assert hashlib.sha256(source.path.read_bytes()).hexdigest() == source_sha256
    assert not tuple((publication_root / "generations").glob("generation-*"))


def test_source_snapshot_profile_rejects_wrong_mode_before_image_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_checkpoint = migration_module._checkpoint
    captures: list[int] = []
    memory_opens: list[object] = []

    def drift_snapshot_mode(fault: str | None, phase: str) -> None:
        if phase == "source_preflight":
            snapshot = next(
                (publication_root / "generations").glob(".building-*/source-snapshot.sqlite3")
            )
            snapshot.chmod(0o640)
        original_checkpoint(fault, phase)

    def track_capture(descriptor: int) -> object:
        captures.append(descriptor)
        raise AssertionError("wrong source mode must fail before image capture")

    def track_memory_open(image: object) -> object:
        memory_opens.append(image)
        raise AssertionError("wrong source mode must fail before memory adapter open")

    monkeypatch.setattr(migration_module, "_checkpoint", drift_snapshot_mode)
    monkeypatch.setattr(migration_module, "_capture_stable_sqlite_image", track_capture)
    monkeypatch.setattr(migration_module, "_open_memory_sqlite_image", track_memory_open)
    with pytest.raises(PaperMigrationPreCommitError):
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )

    assert captures == []
    assert memory_opens == []


def test_transformed_profile_rejects_wrong_mode_before_second_image_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    publication_root = _private_directory(tmp_path / "publication")
    policy = local_audit_publication_root_policy(publication_root)
    original_open_regular_at = migration_module._open_regular_at
    original_capture = migration_module._capture_stable_sqlite_image
    captures: list[int] = []

    def drift_transformed_mode(
        directory_descriptor: int,
        name: str,
        *,
        writable: bool = False,
    ) -> int:
        descriptor = original_open_regular_at(
            directory_descriptor,
            name,
            writable=writable,
        )
        if name == "transformed.sqlite3":
            os.fchmod(descriptor, 0o640)
        return descriptor

    def track_capture(descriptor: int) -> object:
        captures.append(descriptor)
        return original_capture(descriptor)

    monkeypatch.setattr(migration_module, "_open_regular_at", drift_transformed_mode)
    monkeypatch.setattr(migration_module, "_capture_stable_sqlite_image", track_capture)
    with pytest.raises(PaperMigrationPreCommitError):
        migrate_v4_ledger_copy(
            source.path,
            publication_root,
            root_policy=policy,
            migration_code_identity="test-migration-code",
        )

    assert len(captures) == 1


@pytest.mark.parametrize("label", ("source snapshot", "transformed image"))
def test_migration_image_profiles_reject_descriptor_scoped_wrong_owner_before_adapter_open(
    monkeypatch: pytest.MonkeyPatch,
    label: str,
) -> None:
    metadata = os.stat_result(
        (
            stat.S_IFREG | 0o600,
            17,
            3,
            1,
            os.geteuid() + 1,
            os.getegid(),
            64,
            0,
            0,
            0,
        )
    )
    adapter_opens: list[object] = []
    original_open_memory = sqlite_image_module._DefaultSQLiteMemoryAdapter.open_memory

    def tracked_open_memory(adapter: object) -> sqlite3.Connection:
        adapter_opens.append(adapter)
        return original_open_memory(adapter)

    monkeypatch.setattr(migration_module.os, "fstat", lambda _descriptor: metadata)
    monkeypatch.setattr(
        sqlite_image_module._DefaultSQLiteMemoryAdapter,
        "open_memory",
        tracked_open_memory,
    )
    with pytest.raises(ValueError, match=label):
        migration_module._validate_stable_image_profile(
            17,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o600,
            label=label,
        )
    assert adapter_opens == []


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
    original_connect = sqlite3.connect
    sqlite_opens: list[str] = []

    def force_wrong_private_mode(descriptor: int, mode: int) -> None:
        original_fchmod(descriptor, 0o640 if mode == 0o600 else mode)

    def tracked_connect(database: object, *args: object, **kwargs: object) -> sqlite3.Connection:
        sqlite_opens.append(os.fspath(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(publication_module.os, "fchmod", force_wrong_private_mode)
    monkeypatch.setattr(sqlite_image_module.sqlite3, "connect", tracked_connect)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )

    assert caught.value.orphan is not None
    orphan = staging / caught.value.orphan.private_name
    assert stat.S_IMODE(orphan.stat().st_mode) == 0o640
    assert sqlite_opens == []


def test_materialization_rejects_private_destination_owner_drift_before_adapter_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    original_file_identity = publication_module._file_identity
    original_connect = sqlite3.connect
    sqlite_opens: list[str] = []

    def wrong_private_owner(metadata: os.stat_result) -> publication_module.PublicationFileIdentity:
        identity = original_file_identity(metadata)
        if identity.mode == 0o600:
            return identity.model_copy(update={"uid": identity.uid + 1})
        return identity

    def tracked_connect(database: object, *args: object, **kwargs: object) -> sqlite3.Connection:
        sqlite_opens.append(os.fspath(database))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(publication_module, "_file_identity", wrong_private_owner)
    monkeypatch.setattr(sqlite_image_module.sqlite3, "connect", tracked_connect)
    with pytest.raises(PaperMigrationMaterializationError) as caught:
        materialize_paper_migration_for_audit(
            result.publication,
            root_policy=policy,
            staging_root=staging,
        )

    assert caught.value.orphan is not None
    assert sqlite_opens == []


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

    monkeypatch.setattr(sqlite_image_module.sqlite3, "connect", tracked_connect)
    materialized = materialize_paper_migration_for_audit(
        result.publication,
        root_policy=policy,
        staging_root=staging,
    )
    assert materialized.private_path.parent == staging
    assert materialized.materialized_sha256 == result.publication.manifest.candidate_sha256
    assert tuple(sorted(path.name for path in _generation_path(result, root).iterdir())) == (
        result.publication.manifest.object_name,
        "publication-manifest.json",
    )
    assert sqlite_opens == [":memory:"]
    assert materialized.verification.sqlite_integrity == "ok"


def test_legacy_candidate_api_and_result_fields_are_absent(tmp_path: Path) -> None:
    public_operations = {
        name
        for module in (migration_module, publication_module)
        for name in module.__all__
        if inspect.isfunction(getattr(module, name))
        and name.startswith(
            ("begin_", "materialize_", "migrate_", "observe_", "publish_", "recover_")
        )
    }
    assert public_operations == {
        "materialize_paper_migration_for_audit",
        "migrate_paper_ledger_v4_offline_copy",
        "migrate_v4_ledger_copy",
        "recover_paper_migration_publication",
    }
    for private_detail in (
        "PaperMigrationPublicationContext",
        "PublicationRootHandle",
        "begin_paper_migration_publication",
        "publish_paper_migration_generation",
    ):
        assert not hasattr(publication_module, private_detail)
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


@pytest.mark.parametrize(
    ("extra_name", "entry_kind"),
    (
        ("transformed.sqlite3-wal", "file"),
        ("transformed.sqlite3-shm", "file"),
        ("transformed.sqlite3-journal", "file"),
        ("transformed.sqlite3.tmp", "file"),
        ("link", "symlink"),
        ("extra", "file"),
    ),
)
def test_publication_rejects_wal_sidecars_and_unexpected_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extra_name: str,
    entry_kind: str,
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
            if entry_kind == "symlink":
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


def test_paper_migration_documentation_is_audit_only() -> None:
    document = (
        Path(__file__).parents[2] / "docs" / "architecture" / "paper-research-cost-alignment.md"
    ).read_text(encoding="utf-8")
    for stale_claim in (
        "discard the candidate",
        "permits live promotion",
        "unanchored audit candidate",
    ):
        assert stale_claim not in document
    for required_claim in (
        "The migration result is audit-only and never authorizes live promotion.",
        "The library does not delete publication residue.",
        "require separate designs and explicit authorization",
    ):
        assert required_claim in document
    assert "private_path is a locator only" in document
    assert "may be opened by SQLite or" not in document


def test_v4_public_surface_and_locator_ast_contract() -> None:
    repository = Path(__file__).parents[2]
    operation_modules = {
        "src/rquant/paper_ledger_migration.py": {
            "migrate_v4_ledger_copy",
            "migrate_paper_ledger_v4_offline_copy",
        },
        "src/rquant/paper_migration_publication.py": {
            "recover_paper_migration_publication",
            "materialize_paper_migration_for_audit",
        },
    }
    operations = set().union(*operation_modules.values())
    for relative, expected in operation_modules.items():
        tree = ast.parse((repository / relative).read_text(encoding="utf-8"))
        definitions = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in operations
        }
        assert set(definitions) == expected
        for definition in definitions.values():
            parameters = {
                argument.arg
                for argument in (
                    *definition.args.posonlyargs,
                    *definition.args.args,
                    *definition.args.kwonlyargs,
                )
            }
            assert not parameters & {"callback", "consumer", "consumer_callback"}

    package_root = ast.parse((repository / "src/rquant/__init__.py").read_text(encoding="utf-8"))
    root_bindings = {
        alias.asname or alias.name.rsplit(".", maxsplit=1)[-1]
        for node in ast.walk(package_root)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    } | {
        node.name
        for node in ast.walk(package_root)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    assert not operations & root_bindings
    assert not any(name.startswith("_") for name in publication_module.__all__)
    assert not any(name.startswith("_") for name in migration_module.__all__)
    assert {name for name in publication_module.__all__ if name in operations} == {
        "recover_paper_migration_publication",
        "materialize_paper_migration_for_audit",
    }
    assert {name for name in migration_module.__all__ if name in operations} == operations

    class LocatorVisitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.classes: list[str] = []
            self.illegal_attributes: list[int] = []
            self.illegal_aliases: list[int] = []

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.classes.append(node.name)
            self.generic_visit(node)
            self.classes.pop()

        def visit_Attribute(self, node: ast.Attribute) -> None:
            if node.attr == "private_path" and self.classes != [
                "PaperMigrationAuditMaterialization"
            ]:
                self.illegal_attributes.append(node.lineno)
            self.generic_visit(node)

        def visit_Assign(self, node: ast.Assign) -> None:
            if isinstance(node.value, ast.Attribute) and node.value.attr == "private_path":
                self.illegal_aliases.append(node.lineno)
            self.generic_visit(node)

    locator_scan_paths = tuple((repository / "src" / "rquant").rglob("*.py")) + (
        repository / "tests/unit/test_paper_ledger_v4_migration.py",
        repository / "tests/unit/test_paper_broker.py",
    )
    for path in locator_scan_paths:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        visitor = LocatorVisitor()
        visitor.visit(tree)
        assert visitor.illegal_attributes == []
        assert visitor.illegal_aliases == []
        assert "file:{materialization.private_path}" not in source
    document = (repository / "docs/architecture/paper-research-cost-alignment.md").read_text(
        encoding="utf-8"
    )
    assert "may be opened" not in document
    assert "openable" not in document


def test_audit_materialization_v2_model_rejects_inconsistent_locator_and_evidence(
    tmp_path: Path,
) -> None:
    result, policy, staging, _source, _root = _publish(tmp_path)
    materialized = materialize_paper_migration_for_audit(
        result.publication,
        root_policy=policy,
        staging_root=staging,
    )
    assert materialized.contract == "rquant-paper-migration-audit-materialization/v2"
    payload = json.loads(materialized.model_dump_json())
    mutations = (
        lambda value: value.update(contract="rquant-paper-migration-audit-materialization/v1"),
        lambda value: value.update(unexpected=True),
        lambda value: value.update(private_name="not-a-private-name"),
        lambda value: value.update(private_path="relative.sqlite3"),
        lambda value: value.update(materialized_size=value["materialized_size"] + 1),
        lambda value: value["staging_root_identity"].update(mode=0o755),
        lambda value: value["verification"].update(source_sha256="0" * 64),
    )
    for mutate in mutations:
        invalid = json.loads(json.dumps(payload))
        mutate(invalid)
        with pytest.raises(ValidationError):
            publication_module.PaperMigrationAuditMaterialization.model_validate(invalid)


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
        generation.chmod(policy.committed_generation_mode)
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
        generation.chmod(policy.committed_generation_mode)
    elif replacement_kind == "generation":
        parked = generation.with_name("parked-generation")
        generation.chmod(policy.committed_generation_mode)
        generation.rename(parked)
        parked.chmod(policy.committed_generation_mode)
        generation.mkdir(mode=policy.committed_generation_mode)
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
