from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

import rquant.authority_path_security as authority_path_security
from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
)
from rquant.lab_claim_publication import LabClaimPublicationIdentity
from rquant.strict_json import canonical_model_json_bytes

from .test_adapter_manifest import _key_pair


def _keyring(record: object, *, purpose: str) -> VerifyOnlyEd25519Keyring:
    return VerifyOnlyEd25519Keyring(
        records=(record,),  # type: ignore[arg-type]
        issuer_allowlist={purpose: frozenset({record.issuer})},  # type: ignore[attr-defined]
        rotation_allowlist={(record.issuer, purpose): frozenset({record.key_id})},  # type: ignore[attr-defined]
    )


def _certificate(
    tmp_path: Path,
    *,
    database_generation: tuple[int, int] = (1, 2),
) -> tuple[object, object, object, object, LabClaimFinalizerTrustCertificate]:
    from rquant.lab_claim_finalizer_runtime import issue_offline_finalizer_certificate

    root, root_record = _key_pair(
        tmp_path / "root",
        key_id="offline-root",
        issuer="offline-root",
        key_purpose="lab_claim_finalizer_root",
        rotation="active",
    )
    runtime, runtime_record = _key_pair(
        tmp_path / "runtime",
        key_id="runtime-v1",
        issuer="finalizer",
        key_purpose="lab_claim_finalizer",
        rotation="active",
    )
    now = datetime.now(UTC)
    return (
        root,
        root_record,
        runtime,
        runtime_record,
        issue_offline_finalizer_certificate(
            root_signer=root,
            finalizer_signer=runtime,
            store_id="a" * 64,
            database_generation=database_generation,
            schema_version=16,
            not_before=now - timedelta(minutes=1),
            expires_at=now + timedelta(hours=1),
        ),
    )


def _published_triplet() -> tuple[str, str, str]:
    attempt_id = UUID("11111111-1111-4111-8111-111111111111")
    identity = LabClaimPublicationIdentity(
        attempt_id=attempt_id,
        job_id=UUID("22222222-2222-4222-8222-222222222222"),
        shard_id=UUID("33333333-3333-4333-8333-333333333333"),
        claim_token=attempt_id,
        claim_generation=1,
        scheduler_fencing_token=1,
        worker_id="worker-a",
        spec_hash="a" * 64,
        plan_hash="b" * 64,
        payload_hash="c" * 64,
    )
    return str(attempt_id), "d" * 64, canonical_model_json_bytes(identity).decode("utf-8")


def test_offline_issuer_signs_canonical_certificate_and_runtime_has_no_signer(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        inspect_offline_finalizer_certificate,
        issue_offline_finalizer_certificate,
    )

    _root, root_record, runtime, _runtime_record, certificate = _certificate(tmp_path)
    assert certificate.signature != "unsigned"
    assert issue_offline_finalizer_certificate.__name__ == "issue_offline_finalizer_certificate"
    inspected = inspect_offline_finalizer_certificate(certificate)
    assert inspected["schema_version_bound"] == 16
    assert "root_private" not in inspected
    verifier = LabClaimFinalizerTrustVerifier(
        root_keyring=_keyring(root_record, purpose="lab_claim_finalizer_root"),
        finalizer_keyring=_keyring(_runtime_record, purpose="lab_claim_finalizer"),
    )
    assert (
        verifier.require_certificate(
            certificate,
            store_id="a" * 64,
            database_generation=(1, 2),
            schema_version=16,
            now=datetime.now(UTC),
        )
        == certificate
    )
    assert runtime.key_purpose == "lab_claim_finalizer"  # type: ignore[attr-defined]


def test_installer_is_atomic_idempotent_and_never_persists_offline_root_private(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeInstallRequest,
        LabClaimFinalizerGenerationInstaller,
    )

    database = tmp_path / "lab.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version = 16")
    database.chmod(0o600)
    stat = database.stat()
    _root, root_record, _runtime, runtime_record, certificate = _certificate(
        tmp_path,
        database_generation=(stat.st_dev, stat.st_ino),
    )
    _plan, _plan_record = _key_pair(
        tmp_path / "plan",
        key_id="plan-v2",
        issuer="plan",
        key_purpose="source_use_plan_v2",
        rotation="active",
    )
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    root_capability = source / "root.capability"
    worker_bundle = source / "worker-verifier.json"
    root_capability.write_bytes(b"capability\n")
    worker_bundle.write_bytes(b'{"verify":true}\n')
    root_capability.chmod(0o600)
    worker_bundle.chmod(0o640)
    runtime_private = tmp_path / "runtime" / "runtime-v1.private.pem"
    runtime_public = tmp_path / "runtime" / "runtime-v1.public.pem"
    plan_private = tmp_path / "plan" / "plan-v2.private.pem"
    plan_public = tmp_path / "plan" / "plan-v2.public.pem"
    runtime_root = tmp_path / "finalizer-runtime"
    runtime_root.mkdir(mode=0o700)
    request = FinalizerRuntimeInstallRequest(
        certificate=certificate,
        database_path=database,
        store_id="a" * 64,
        schema_version=16,
        runtime_private_key_path=runtime_private,
        runtime_public_key_path=runtime_public,
        root_capability_secret_path=root_capability,
        current_plan_private_key_path=plan_private,
        current_plan_public_key_path=plan_public,
        worker_verify_bundle_path=worker_bundle,
        finalizer_public_key=runtime_record,
    )
    installer = LabClaimFinalizerGenerationInstaller(
        runtime_root=runtime_root,
        root_keyring=_keyring(root_record, purpose="lab_claim_finalizer_root"),
        finalizer_keyring=_keyring(runtime_record, purpose="lab_claim_finalizer"),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    preview = installer.install(request, dry_run=True)
    assert not preview.write_performed
    assert not (runtime_root / "current").exists()
    receipt = installer.install(request)
    assert receipt.write_performed
    assert (runtime_root / "current").read_text(encoding="ascii") == receipt.generation_id + "\n"
    assert installer.install(request).generation_id == receipt.generation_id
    assert b"offline-root.private" not in b"".join(
        path.read_bytes()
        for path in (runtime_root / "generations" / receipt.generation_id).iterdir()
    )


def test_installer_rejects_replaced_database_and_preserves_current_pointer(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeError,
        FinalizerRuntimeInstallRequest,
        LabClaimFinalizerGenerationInstaller,
    )

    database = tmp_path / "lab.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version = 16")
    database.chmod(0o600)
    first_stat = database.stat()
    _root, root_record, _runtime, runtime_record, certificate = _certificate(
        tmp_path,
        database_generation=(first_stat.st_dev, first_stat.st_ino),
    )
    _plan, _plan_record = _key_pair(
        tmp_path / "plan",
        key_id="plan-v2",
        issuer="plan",
        key_purpose="source_use_plan_v2",
        rotation="active",
    )
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    root_capability = source / "root.capability"
    worker_bundle = source / "worker-verifier.json"
    root_capability.write_bytes(b"capability\n")
    worker_bundle.write_bytes(b'{"verify":true}\n')
    root_capability.chmod(0o600)
    worker_bundle.chmod(0o640)
    request = FinalizerRuntimeInstallRequest(
        certificate=certificate,
        database_path=database,
        store_id="a" * 64,
        runtime_private_key_path=tmp_path / "runtime" / "runtime-v1.private.pem",
        runtime_public_key_path=tmp_path / "runtime" / "runtime-v1.public.pem",
        root_capability_secret_path=root_capability,
        current_plan_private_key_path=tmp_path / "plan" / "plan-v2.private.pem",
        current_plan_public_key_path=tmp_path / "plan" / "plan-v2.public.pem",
        worker_verify_bundle_path=worker_bundle,
        finalizer_public_key=runtime_record,
    )
    runtime_root = tmp_path / "finalizer-runtime"
    runtime_root.mkdir(mode=0o700)
    installer = LabClaimFinalizerGenerationInstaller(
        runtime_root=runtime_root,
        root_keyring=_keyring(root_record, purpose="lab_claim_finalizer_root"),
        finalizer_keyring=_keyring(runtime_record, purpose="lab_claim_finalizer"),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    first = installer.install(request)
    replacement = tmp_path / "replacement.sqlite3"
    with sqlite3.connect(replacement) as connection:
        connection.execute("PRAGMA user_version = 16")
    replacement.chmod(0o600)
    os.replace(replacement, database)
    with pytest.raises(FinalizerRuntimeError, match="certificate"):
        installer.install(request)
    assert (runtime_root / "current").read_text(encoding="ascii") == first.generation_id + "\n"


def test_rollout_requires_preflight_and_drains_without_deleting_published_records(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRolloutError,
        FinalizerRolloutPhase,
        FinalizerRolloutStore,
    )

    store = FinalizerRolloutStore(tmp_path / "rollout.sqlite3")
    with pytest.raises(FinalizerRolloutError, match="illegal"):
        store.transition(FinalizerRolloutPhase.FINALIZER_READY, evidence="nope")
    store.transition(FinalizerRolloutPhase.MATERIAL_INSTALLED, evidence="install:a")
    store.transition(FinalizerRolloutPhase.PREFLIGHT_OK, evidence="preflight:a")
    store.transition(FinalizerRolloutPhase.FINALIZER_READY, evidence="ready:a")
    store.require_v2_worker_enable()
    with pytest.raises(FinalizerRolloutError, match="scheduler"):
        store.require_scheduler_v2_emit()
    store.transition(FinalizerRolloutPhase.V2_WORKERS_READY, evidence="workers:a")
    store.require_v2_worker_enable()
    store.transition(FinalizerRolloutPhase.SCHEDULER_EMITS_V2, evidence="scheduler:a")
    store.require_scheduler_v2_emit()
    attempt_id, evidence_hash, publication_identity = _published_triplet()
    store.record_published(
        attempt_id=attempt_id,
        evidence_hash=evidence_hash,
        publication_identity=publication_identity,
    )
    store.begin_rollback(evidence="stop-new-emits")
    assert store.snapshot().phase is FinalizerRolloutPhase.DRAINING
    with pytest.raises(TypeError):
        store.complete_drain(evidence="outbox-empty")  # type: ignore[call-arg]
    from rquant.lab_jobs import LabJobStore

    job_store = LabJobStore(tmp_path / "empty-lab-jobs.sqlite3")
    job_store.initialize()
    with pytest.raises(FinalizerRolloutError, match="published evidence set differs"):
        store.complete_drain(evidence="outbox-empty", job_store=job_store)
    assert store.snapshot().phase is FinalizerRolloutPhase.DRAINING
    assert store.published_count() == 1


def _advance_rollout_to_scheduler_emit(store: object) -> None:
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutPhase

    for phase, evidence in (
        (FinalizerRolloutPhase.MATERIAL_INSTALLED, "install:a"),
        (FinalizerRolloutPhase.PREFLIGHT_OK, "preflight:a"),
        (FinalizerRolloutPhase.FINALIZER_READY, "ready:a"),
        (FinalizerRolloutPhase.V2_WORKERS_READY, "workers:a"),
        (FinalizerRolloutPhase.SCHEDULER_EMITS_V2, "scheduler:a"),
    ):
        store.transition(phase, evidence=evidence)  # type: ignore[union-attr]


def test_rollout_emit_permit_fences_drain_and_releases_on_exception(tmp_path: Path) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRolloutError,
        FinalizerRolloutPhase,
        FinalizerRolloutStore,
    )

    store = FinalizerRolloutStore(tmp_path / "rollout.sqlite3")
    _advance_rollout_to_scheduler_emit(store)
    acquired = threading.Event()
    release = threading.Event()
    permit_details: list[object] = []

    def hold_emit() -> None:
        with store.emit_permit(holder="scheduler-a:attempt:claim:shard") as permit:
            permit_details.append(permit)
            acquired.set()
            assert release.wait(timeout=5)

    emitter = threading.Thread(target=hold_emit)
    emitter.start()
    assert acquired.wait(timeout=2)
    drain_done = threading.Event()

    def begin_drain() -> None:
        store.begin_rollback(evidence="drain:a")
        drain_done.set()

    drainer = threading.Thread(target=begin_drain)
    drainer.start()
    time.sleep(0.15)
    assert not drain_done.is_set()
    release.set()
    emitter.join(timeout=5)
    drainer.join(timeout=5)
    assert not emitter.is_alive() and not drainer.is_alive()
    assert drain_done.is_set()
    permit = permit_details[0]
    assert permit.revision == 5  # type: ignore[union-attr]
    assert permit.store_identity == store.identity  # type: ignore[union-attr]
    assert permit.holder == "scheduler-a:attempt:claim:shard"  # type: ignore[union-attr]
    assert store.snapshot().phase is FinalizerRolloutPhase.DRAINING
    with (
        pytest.raises(FinalizerRolloutError, match="permit"),
        store.emit_permit(holder="scheduler-a:attempt:claim:shard"),
    ):
        pass

    released = FinalizerRolloutStore(tmp_path / "released.sqlite3")
    _advance_rollout_to_scheduler_emit(released)
    with (
        pytest.raises(RuntimeError, match="boom"),
        released.emit_permit(holder="scheduler-a:attempt:claim:shard"),
    ):
        raise RuntimeError("boom")
    assert (
        released.begin_rollback(evidence="drain:after-exception").phase
        is FinalizerRolloutPhase.DRAINING
    )


def test_rollout_published_replay_requires_exact_evidence_and_identity(tmp_path: Path) -> None:
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutError, FinalizerRolloutStore

    store = FinalizerRolloutStore(tmp_path / "rollout.sqlite3")
    attempt_id, evidence_hash, identity = _published_triplet()
    store.record_published(
        attempt_id=attempt_id,
        evidence_hash=evidence_hash,
        publication_identity=identity,
    )
    store.record_published(
        attempt_id=attempt_id,
        evidence_hash=evidence_hash,
        publication_identity=identity,
    )
    with pytest.raises(FinalizerRolloutError, match="evidence differs"):
        store.record_published(
            attempt_id=attempt_id,
            evidence_hash=evidence_hash,
            publication_identity=identity.replace("worker-a", "worker-b"),
        )
    with pytest.raises(FinalizerRolloutError, match="evidence differs"):
        store.record_published(
            attempt_id=attempt_id,
            evidence_hash="e" * 64,
            publication_identity=identity,
        )
    assert store.published_count() == 1


def test_rollout_published_evidence_requires_a_complete_canonical_triplet(tmp_path: Path) -> None:
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutError, FinalizerRolloutStore

    store = FinalizerRolloutStore(tmp_path / "rollout.sqlite3")
    attempt_id, evidence_hash, identity = _published_triplet()
    with pytest.raises(TypeError):
        store.record_published()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        store.record_published(attempt_id)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        store.record_published(  # type: ignore[call-arg]
            attempt_id=attempt_id, evidence_hash=evidence_hash
        )
    for invalid in (
        {"attempt_id": "", "evidence_hash": evidence_hash, "publication_identity": identity},
        {
            "attempt_id": "not-a-canonical-uuid",
            "evidence_hash": evidence_hash,
            "publication_identity": identity,
        },
        {"attempt_id": attempt_id, "evidence_hash": "", "publication_identity": identity},
        {
            "attempt_id": attempt_id,
            "evidence_hash": "d" * 63,
            "publication_identity": identity,
        },
        {"attempt_id": attempt_id, "evidence_hash": evidence_hash, "publication_identity": ""},
        {
            "attempt_id": attempt_id,
            "evidence_hash": evidence_hash,
            "publication_identity": identity + " ",
        },
        {
            "attempt_id": attempt_id,
            "evidence_hash": evidence_hash,
            "publication_identity": identity.replace('"attempt_id"', '"worker_id"', 1),
        },
    ):
        with pytest.raises(FinalizerRolloutError, match="published evidence"):
            store.record_published(**invalid)


@pytest.mark.parametrize("pointer_kind", ("symlink", "relative", "dangling"))
def test_current_pointer_fails_closed_before_generation_lookup(
    tmp_path: Path,
    pointer_kind: str,
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeError,
        load_current_lab_claim_finalizer_generation,
    )

    root = tmp_path / "runtime"
    root.mkdir(mode=0o700)
    pointer = root / "current"
    if pointer_kind == "symlink":
        pointer.symlink_to("generation")
    elif pointer_kind == "relative":
        pointer.write_text("../escape\n", encoding="ascii")
        pointer.chmod(0o640)
    else:
        pointer.write_text("a" * 64 + "\n", encoding="ascii")
        pointer.chmod(0o640)
    with pytest.raises(FinalizerRuntimeError):
        load_current_lab_claim_finalizer_generation(
            root,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            trusted_base=tmp_path,
            trusted_base_owner_uids=frozenset({os.getuid()}),
        )


@pytest.mark.parametrize("unsafe_kind", ("writable", "symlink"))
def test_trusted_base_rejects_unsafe_runtime_ancestor(tmp_path: Path, unsafe_kind: str) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeError,
        load_current_lab_claim_finalizer_generation,
    )

    base = tmp_path / "trusted"
    base.mkdir(mode=0o700)
    runtime = base / "runtime"
    runtime.mkdir(mode=0o700)
    if unsafe_kind == "writable":
        runtime.chmod(0o777)
    else:
        target = tmp_path / "target"
        target.mkdir(mode=0o700)
        runtime.rmdir()
        runtime.symlink_to(target, target_is_directory=True)
    with pytest.raises(FinalizerRuntimeError, match="trusted base"):
        load_current_lab_claim_finalizer_generation(
            runtime,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            trusted_base=base,
            trusted_base_owner_uids=frozenset({os.getuid()}),
        )


def test_current_pointer_replacement_during_descriptor_walk_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeError,
        load_current_lab_claim_finalizer_generation,
    )

    base = tmp_path / "trusted"
    base.mkdir(mode=0o700)
    runtime = base / "runtime"
    runtime.mkdir(mode=0o700)
    pointer = runtime / "current"
    pointer.write_text("a" * 64 + "\n", encoding="ascii")
    pointer.chmod(0o640)
    replacement = runtime / "replacement"
    replacement.write_text("b" * 64 + "\n", encoding="ascii")
    replacement.chmod(0o640)
    original_stat = authority_path_security.os.stat
    replaced = False

    def replace_after_named_stat(path: object, *args: object, **kwargs: object) -> os.stat_result:
        nonlocal replaced
        observed = original_stat(path, *args, **kwargs)
        if path == "current" and kwargs.get("dir_fd") is not None and not replaced:
            replaced = True
            os.replace(replacement, pointer)
        return observed

    monkeypatch.setattr(authority_path_security.os, "stat", replace_after_named_stat)
    with pytest.raises(FinalizerRuntimeError, match="pointer is unsafe"):
        load_current_lab_claim_finalizer_generation(
            runtime,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            trusted_base=base,
            trusted_base_owner_uids=frozenset({os.getuid()}),
        )
    assert replaced
