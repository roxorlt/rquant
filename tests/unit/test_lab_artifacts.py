from __future__ import annotations

import errno
import fcntl
import gc
import hashlib
import json
import os
import random
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import textwrap
import threading
import time
import tracemalloc
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4
from zipfile import ZipFile, ZipInfo

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.lab_artifacts as lab_artifacts_module
from rquant.lab_artifacts import (
    LabArtifactAuthorizationError,
    LabArtifactConflictError,
    LabArtifactError,
    LabArtifactFinalizationLockError,
    LabArtifactFinalizationLockTimeoutError,
    LabArtifactIndexEvidence,
    LabArtifactIntegrityError,
    LabArtifactPathError,
    LabArtifactPayloadBudget,
    LabArtifactPlatformError,
    LabArtifactRecoveryAuthority,
    LabArtifactRecoveryRecord,
    LabJobArtifactCandidate,
    LabJobArtifactFile,
    LabJobArtifactManifest,
    LabJobArtifactPlan,
    LabJobArtifactStore,
    LabLegacyArtifactConflictError,
    LabSealedJobArtifact,
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


def test_artifact_store_internal_mutation_fence_prevents_seal_publish(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    calls = 0

    def mutation_guard() -> str:
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise RuntimeError("runtime drifted before artifact publish")
        return "1" * 40

    store.mutation_guard = mutation_guard
    mutation_guard()

    with pytest.raises(RuntimeError, match="before artifact publish"):
        store.seal_candidate(candidate)

    assert candidate.path.is_dir()
    assert not (store.sealed_root / candidate.job_id.hex).exists()


def test_artifact_store_checks_guard_inside_initial_root_creation(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"

    def mutation_guard() -> str:
        raise RuntimeError("runtime drifted before artifact namespace creation")

    with pytest.raises(RuntimeError, match="artifact namespace creation"):
        LabJobArtifactStore(root, mutation_guard=mutation_guard)

    assert not root.exists()


def _prepare_arguments() -> dict[str, object]:
    return {
        "job_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "spec": _spec(),
        "plan_hash": "6" * 64,
        "adapter_id": "n-shape",
        "adapter_version": "1",
        "result_contract_version": "p14b1-v1",
        "metrics": {},
        "report_markdown": "# report\n",
        "tables": {"result": pd.DataFrame({"value": [1]})},
    }


def _evidence(sealed: LabSealedJobArtifact) -> LabArtifactIndexEvidence:
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


def _flatten_exception_group(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [nested for child in error.exceptions for nested in _flatten_exception_group(child)]
    return [error]


def _descriptor_is_closed(
    descriptor: int,
    fstat: Callable[[int], os.stat_result] = os.fstat,
) -> bool:
    try:
        fstat(descriptor)
    except OSError as exc:
        if exc.errno == errno.EBADF:
            return True
        raise
    return False


def _descriptor_identity_counts(
    identities: set[tuple[int, int]],
) -> dict[tuple[int, int], int]:
    descriptor_root = Path("/proc/self/fd")
    if not descriptor_root.exists():
        descriptor_root = Path("/dev/fd")
    counts = {identity: 0 for identity in identities}
    for entry in os.listdir(descriptor_root):
        if not entry.isdigit():
            continue
        try:
            observed = os.fstat(int(entry))
        except OSError:
            continue
        identity = (observed.st_dev, observed.st_ino)
        if identity in counts:
            counts[identity] += 1
    return counts


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


def _tree_identity(path: Path) -> tuple[tuple[object, ...], ...]:
    entries: list[tuple[object, ...]] = []
    for item in (path, *sorted(path.rglob("*"))):
        observed = item.lstat()
        relative_path = "." if item == path else item.relative_to(path).as_posix()
        payload = item.read_bytes() if item.is_file() and not item.is_symlink() else None
        entries.append(
            (
                relative_path,
                stat.S_IFMT(observed.st_mode),
                stat.S_IMODE(observed.st_mode),
                observed.st_dev,
                observed.st_ino,
                observed.st_size,
                observed.st_mtime_ns,
                observed.st_ctime_ns,
                payload,
            )
        )
    return tuple(entries)


def _artifact_namespace_identity(store: LabJobArtifactStore) -> dict[str, tuple[str, ...]]:
    roots = (
        store.candidates_root,
        store.sealed_root,
        store.quarantine_root,
        store.seal_intents_root,
        store.seal_intents_quarantine_root,
    )
    return {root.name: tuple(sorted(item.name for item in root.iterdir())) for root in roots}


def test_artifact_lifecycle_error_is_a_typed_integrity_failure() -> None:
    assert issubclass(
        lab_artifacts_module.LabArtifactLifecycleError,
        LabArtifactIntegrityError,
    )


def test_finalization_identity_lock_is_per_result_and_times_out_typed(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    first_job = uuid4()
    second_job = uuid4()
    unrelated_entered = threading.Event()
    same_identity_error: list[BaseException] = []

    def enter_unrelated() -> None:
        with store.finalization_identity_lock(
            job_id=second_job,
            manifest_hash="2" * 64,
            timeout_seconds=1,
        ):
            unrelated_entered.set()

    def contend_same_identity() -> None:
        try:
            with store.finalization_identity_lock(
                job_id=first_job,
                manifest_hash="1" * 64,
                timeout_seconds=0.05,
            ):
                raise AssertionError("contended identity lock was entered")
        except BaseException as exc:
            same_identity_error.append(exc)

    with store.finalization_identity_lock(
        job_id=first_job,
        manifest_hash="1" * 64,
        timeout_seconds=1,
    ):
        unrelated = threading.Thread(target=enter_unrelated)
        contended = threading.Thread(target=contend_same_identity)
        unrelated.start()
        contended.start()
        assert unrelated_entered.wait(timeout=0.5), "unrelated job was globally serialized"
        unrelated.join(timeout=2)
        contended.join(timeout=2)

    assert not unrelated.is_alive()
    assert not contended.is_alive()
    assert len(same_identity_error) == 1
    assert isinstance(same_identity_error[0], LabArtifactFinalizationLockTimeoutError)


def test_finalization_identity_lock_recovers_after_exit_and_rejects_clobber_or_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    marker = tmp_path / "locked"
    job_id = uuid4()
    manifest_hash = "3" * 64
    script = textwrap.dedent(
        """
        import os
        from pathlib import Path
        from uuid import UUID

        from rquant.lab_artifacts import LabJobArtifactStore

        store = LabJobArtifactStore(Path({root!r}))
        with store.finalization_identity_lock(
            job_id=UUID({job_id!r}),
            manifest_hash={manifest_hash!r},
            timeout_seconds=5,
        ):
            Path({marker!r}).write_text("locked", encoding="utf-8")
            os._exit(0)
        """
    ).format(
        root=str(root),
        job_id=str(job_id),
        manifest_hash=manifest_hash,
        marker=str(marker),
    )
    process = subprocess.Popen(
        [str(Path(sys.executable)), "-c", script],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, (stdout, stderr)

    store = LabJobArtifactStore(root)
    with store.finalization_identity_lock(
        job_id=job_id,
        manifest_hash=manifest_hash,
        timeout_seconds=1,
    ):
        pass

    lock_files = tuple(store.finalization_locks_root.iterdir())
    assert len(lock_files) == 1
    lock_file = lock_files[0]
    lock_file.unlink()
    lock_file.write_bytes(b"retained conflict evidence")
    lock_file.chmod(0o600)

    with (
        pytest.raises(LabArtifactFinalizationLockError, match="secure|regular|identity"),
        store.finalization_identity_lock(
            job_id=job_id,
            manifest_hash=manifest_hash,
            timeout_seconds=1,
        ),
    ):
        pass
    assert lock_file.read_bytes() == b"retained conflict evidence"

    external = tmp_path / "external-lock"
    external.write_text("external", encoding="utf-8")
    lock_file.unlink()
    lock_file.symlink_to(external)

    with (
        pytest.raises(LabArtifactFinalizationLockError, match="secure|regular|identity"),
        store.finalization_identity_lock(
            job_id=job_id,
            manifest_hash=manifest_hash,
            timeout_seconds=1,
        ),
    ):
        pass


def test_same_thread_close_inside_nested_finalization_activity_fails_fast_in_subprocess(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    script = textwrap.dedent(
        """
        import threading
        from pathlib import Path
        from uuid import UUID

        from rquant.lab_artifacts import (
            LabArtifactLifecycleError,
            LabJobArtifactStore,
        )

        store = LabJobArtifactStore(Path({root!r}))
        job_id = UUID({job_id!r})

        def reject_close(expected_depth):
            try:
                store.close()
            except LabArtifactLifecycleError:
                pass
            else:
                raise AssertionError("same-thread close unexpectedly succeeded")
            owner = threading.get_ident()
            assert store._preview_activity_owners == {{owner: expected_depth}}
            assert store._preview_activity_count == expected_depth
            assert store._closing is False
            assert store._closed is False
            store.list_candidate_recovery()

        with store.finalization_identity_lock(
            job_id=job_id,
            manifest_hash="4" * 64,
            timeout_seconds=1,
        ):
            with store._preview_activity():
                reject_close(2)
            reject_close(1)

        assert store._preview_activity_owners == {{}}
        assert store._preview_activity_count == 0
        store.list_candidate_recovery()
        store.close()
        assert store._closed is True
        """
    ).format(root=str(root), job_id=str(uuid4()))
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=3)
        pytest.fail(f"same-thread close deadlocked\nstdout={stdout}\nstderr={stderr}")

    assert process.returncode == 0, (stdout, stderr)


def test_other_thread_close_waits_for_finalization_activity_then_succeeds(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    close_started = threading.Event()
    close_finished = threading.Event()
    errors: list[BaseException] = []

    def close_store() -> None:
        close_started.set()
        try:
            store.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            close_finished.set()

    with store.finalization_identity_lock(
        job_id=uuid4(),
        manifest_hash="5" * 64,
        timeout_seconds=1,
    ):
        close_thread = threading.Thread(target=close_store)
        close_thread.start()
        assert close_started.wait(timeout=1)
        deadline = time.monotonic() + 1
        while not store._closing and time.monotonic() < deadline:
            time.sleep(0.01)
        assert store._closing is True
        assert close_finished.is_set() is False
        with pytest.raises(
            lab_artifacts_module.LabArtifactLifecycleError,
            match="owns preview activity",
        ):
            store.close()
        assert close_finished.is_set() is False

    close_thread.join(timeout=3)

    assert not close_thread.is_alive()
    assert close_finished.is_set() is True
    assert errors == []
    assert store._closed is True


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


def test_preview_candidate_is_readonly_and_prepare_materializes_the_exact_plan(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    namespaces_before = _artifact_namespace_identity(store)

    first = store.preview_candidate(**arguments)  # type: ignore[arg-type]
    second = store.preview_candidate(**arguments)  # type: ignore[arg-type]
    budgeted = store.preview_candidate(
        **arguments,  # type: ignore[arg-type]
        payload_budget=LabArtifactPayloadBudget(
            max_single_payload_bytes=16 * 1024 * 1024,
            max_total_payload_bytes=32 * 1024 * 1024,
            max_table_count=8,
        ),
    )

    assert isinstance(first, LabJobArtifactPlan)
    assert budgeted == second == first
    assert _artifact_namespace_identity(store) == namespaces_before
    candidate = store.prepare_candidate(**arguments)  # type: ignore[arg-type]
    assert candidate.manifest == first.manifest
    assert candidate.manifest_hash == first.manifest_hash
    for planned in first.payloads:
        assert (candidate.path / planned.relative_path).read_bytes() == planned.payload


def test_preview_candidate_stops_parquet_serialization_at_payload_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    arguments["tables"] = {"trades": pd.DataFrame([{"value": 1}])}
    getvalue_called = False

    def amplified_to_parquet(
        _frame: pd.DataFrame,
        output: object,
        *,
        index: bool,
    ) -> None:
        assert index is False
        output.write(b"x" * 2048)  # type: ignore[attr-defined]

    original_getvalue = lab_artifacts_module._BoundedBytesIO.getvalue

    def count_getvalue(buffer: object) -> bytes:
        nonlocal getvalue_called
        getvalue_called = True
        return original_getvalue(buffer)  # type: ignore[arg-type]

    monkeypatch.setattr(pd.DataFrame, "to_parquet", amplified_to_parquet)
    monkeypatch.setattr(lab_artifacts_module._BoundedBytesIO, "getvalue", count_getvalue)

    with pytest.raises(LabArtifactIntegrityError, match="payload byte budget"):
        store.preview_candidate(
            **arguments,  # type: ignore[arg-type]
            payload_budget=LabArtifactPayloadBudget(
                max_single_payload_bytes=1024,
                max_total_payload_bytes=64 * 1024,
                max_table_count=1,
            ),
        )

    assert getvalue_called is False
    assert _artifact_namespace_identity(store)["candidates"] == ()


def test_blocked_preview_serialization_does_not_hold_artifact_lifecycle_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    entered = threading.Event()
    release = threading.Event()
    listed = threading.Event()
    errors: list[BaseException] = []
    original = store._serialize_parquet

    def blocked(
        table_name: str,
        frame: pd.DataFrame,
        *,
        max_payload_bytes: int | None = None,
    ) -> object:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("preview serializer was not released")
        return original(
            table_name,
            frame,
            max_payload_bytes=max_payload_bytes,
        )

    def preview() -> None:
        try:
            store.preview_candidate(**_prepare_arguments())  # type: ignore[arg-type]
        except BaseException as exc:
            errors.append(exc)

    def list_recovery() -> None:
        try:
            store.list_candidate_recovery()
            listed.set()
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(store, "_serialize_parquet", blocked)
    preview_thread = threading.Thread(target=preview)
    list_thread = threading.Thread(target=list_recovery)
    preview_thread.start()
    assert entered.wait(timeout=2)
    list_thread.start()
    try:
        assert listed.wait(timeout=1), "preview held the artifact lifecycle lock"
    finally:
        release.set()
        preview_thread.join(timeout=5)
        list_thread.join(timeout=5)

    assert not preview_thread.is_alive() and not list_thread.is_alive()
    assert errors == []
    assert _artifact_namespace_identity(store) == {
        "candidates": (),
        "sealed": (),
        "quarantine": (),
        "seal-intents": (),
        "seal-intents-quarantine": (),
    }


def test_store_close_waits_for_preview_without_blocking_recovery_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    entered = threading.Event()
    release = threading.Event()
    close_started = threading.Event()
    close_finished = threading.Event()
    listed = threading.Event()
    errors: list[BaseException] = []
    original = store._serialize_parquet

    def blocked(
        table_name: str,
        frame: pd.DataFrame,
        *,
        max_payload_bytes: int | None = None,
    ) -> object:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("preview serializer was not released")
        return original(
            table_name,
            frame,
            max_payload_bytes=max_payload_bytes,
        )

    def preview() -> None:
        try:
            store.preview_candidate(**_prepare_arguments())  # type: ignore[arg-type]
        except BaseException as exc:
            errors.append(exc)

    def close() -> None:
        close_started.set()
        try:
            store.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            close_finished.set()

    def list_recovery() -> None:
        try:
            store.list_candidate_recovery()
            listed.set()
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(store, "_serialize_parquet", blocked)
    preview_thread = threading.Thread(target=preview)
    close_thread = threading.Thread(target=close)
    list_thread = threading.Thread(target=list_recovery)
    preview_thread.start()
    assert entered.wait(timeout=2)
    list_thread.start()
    assert listed.wait(timeout=1), "preview activity blocked readonly recovery inspection"
    close_thread.start()
    assert close_started.wait(timeout=1)
    deadline = time.monotonic() + 2
    while not getattr(store, "_closing", False) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert store._closing is True
    assert close_finished.is_set() is False
    with pytest.raises(LabArtifactIntegrityError, match="closing|closed"):
        store.preview_candidate(**_prepare_arguments())  # type: ignore[arg-type]

    release.set()
    preview_thread.join(timeout=5)
    close_thread.join(timeout=5)
    list_thread.join(timeout=5)

    assert not preview_thread.is_alive() and not close_thread.is_alive()
    assert errors == []
    assert close_finished.is_set() is True
    with pytest.raises(LabArtifactIntegrityError, match="closed"):
        store.preview_candidate(**_prepare_arguments())  # type: ignore[arg-type]


def test_prepare_rejects_hex_traversal_job_id_before_any_filesystem_write(
    tmp_path: Path,
) -> None:
    class ForgedJobId:
        hex = "../../escaped"

    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    arguments["job_id"] = ForgedJobId()
    namespaces_before = _artifact_namespace_identity(store)
    root_entries_before = tuple(sorted(item.name for item in tmp_path.iterdir()))

    with pytest.raises((TypeError, ValueError, LabArtifactIntegrityError)):
        store.prepare_candidate(**arguments)  # type: ignore[arg-type]

    assert _artifact_namespace_identity(store) == namespaces_before
    assert tuple(sorted(item.name for item in tmp_path.iterdir())) == root_entries_before


@pytest.mark.parametrize(
    "case",
    [
        "empty_adapter_id",
        "empty_adapter_version",
        "empty_result_contract",
        "invalid_spec",
        "invalid_metrics_tag",
        "metrics_not_mapping",
        "report_not_text",
        "unsafe_table_name",
        "unsafe_table_dtype",
    ],
)
def test_prepare_preflights_all_deterministic_input_errors_before_candidate_mkdir(
    tmp_path: Path,
    case: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    if case == "empty_adapter_id":
        arguments["adapter_id"] = " "
    elif case == "empty_adapter_version":
        arguments["adapter_version"] = ""
    elif case == "empty_result_contract":
        arguments["result_contract_version"] = "\t"
    elif case == "invalid_spec":
        spec_payload = _spec().model_dump(mode="python", round_trip=True)
        spec_payload["resource_class"] = "not-a-resource-class"
        arguments["spec"] = ResearchRunSpec.model_construct(**spec_payload)
    elif case == "invalid_metrics_tag":
        arguments["metrics"] = {"bad": {"$date": "not-a-date"}}
    elif case == "metrics_not_mapping":
        arguments["metrics"] = []
    elif case == "report_not_text":
        arguments["report_markdown"] = 7
    elif case == "unsafe_table_name":
        arguments["tables"] = {"../escape": pd.DataFrame({"value": [1]})}
    else:
        arguments["tables"] = {"result": pd.DataFrame({"value": [object()]})}
    namespaces_before = _artifact_namespace_identity(store)

    with pytest.raises((TypeError, ValueError, LabArtifactError)):
        store.prepare_candidate(**arguments)  # type: ignore[arg-type]

    assert _artifact_namespace_identity(store) == namespaces_before


def test_prepare_valid_input_still_seals_after_rejected_preflight(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    invalid = _prepare_arguments()
    invalid["adapter_id"] = ""

    with pytest.raises((TypeError, ValueError, LabArtifactIntegrityError)):
        store.prepare_candidate(**invalid)  # type: ignore[arg-type]

    candidate = store.prepare_candidate(**_prepare_arguments())  # type: ignore[arg-type]
    sealed = store.seal_candidate(candidate)
    assert sealed.manifest.job_id == candidate.job_id


@pytest.mark.parametrize(
    "mutation",
    [
        "job_id",
        "manifest_hash",
        "inventory_order",
        "inventory_missing",
        "inventory_size",
        "inventory_device",
        "inventory_inode",
    ],
)
def test_candidate_model_rejects_cross_field_identity_conflicts(
    tmp_path: Path,
    mutation: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    payload = candidate.model_dump(mode="python", round_trip=True)
    if mutation == "job_id":
        payload["job_id"] = uuid4()
    elif mutation == "manifest_hash":
        payload["manifest_hash"] = "f" * 64
    elif mutation == "inventory_order":
        payload["file_identities"] = tuple(reversed(candidate.file_identities))
    elif mutation == "inventory_missing":
        payload["file_identities"] = candidate.file_identities[:-1]
    elif mutation == "inventory_size":
        first, *remaining = candidate.file_identities
        payload["file_identities"] = (
            first.model_copy(update={"size": first.size + 1}),
            *remaining,
        )
    elif mutation == "inventory_device":
        first, *remaining = candidate.file_identities
        payload["file_identities"] = (
            first.model_copy(update={"device": first.device + 1}),
            *remaining,
        )
    else:
        first, second, *remaining = candidate.file_identities
        payload["file_identities"] = (
            first,
            second.model_copy(update={"inode": first.inode}),
            *remaining,
        )

    with pytest.raises(ValidationError):
        LabJobArtifactCandidate.model_validate(payload)

    if mutation == "job_id":
        with pytest.raises(ValidationError):
            candidate.model_copy(update={"job_id": payload["job_id"]})


@pytest.mark.parametrize("operation", ["seal", "quarantine"])
def test_forged_candidate_job_identity_has_zero_side_effects(
    tmp_path: Path,
    operation: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    forged_job_id = uuid4()
    forged_payload = candidate.model_dump(mode="python", round_trip=True)
    forged_payload["job_id"] = forged_job_id
    forged = LabJobArtifactCandidate.model_construct(**forged_payload)
    candidate_before = _tree_identity(candidate.path)
    namespaces_before = _artifact_namespace_identity(store)

    with pytest.raises(LabArtifactIntegrityError, match="candidate.*identity"):
        if operation == "seal":
            store.seal_candidate(forged)
        else:
            store.quarantine_candidate(forged, reason="reviewer forged job")

    assert candidate.path.exists()
    assert _tree_identity(candidate.path) == candidate_before
    assert _artifact_namespace_identity(store) == namespaces_before
    assert not (store.sealed_root / forged_job_id.hex).exists()
    assert not (store.seal_intents_root / f"{forged_job_id.hex}.json").exists()

    sealed = store.seal_candidate(candidate)
    assert sealed.path == store.sealed_root / candidate.job_id.hex


def test_forged_recovery_record_is_revalidated_before_any_recovery_side_effect(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)
    forged_job_id = uuid4()
    with pytest.raises(ValidationError):
        LabArtifactRecoveryRecord(
            path=record.path,
            status="invalid",
            job_id=forged_job_id,
            manifest_hash="f" * 64,
            device=record.device,
            inode=record.inode,
            reason="forged logical identity",
        )
    forged = LabArtifactRecoveryRecord.model_construct(
        **{
            **record.model_dump(mode="python", round_trip=True),
            "job_id": forged_job_id,
        }
    )
    candidate_before = _tree_identity(candidate.path)
    namespaces_before = _artifact_namespace_identity(store)

    with pytest.raises(LabArtifactIntegrityError, match="recovery evidence"):
        store.recover_candidate(forged, authority=_recovery_authority(candidate))

    assert _tree_identity(candidate.path) == candidate_before
    assert _artifact_namespace_identity(store) == namespaces_before
    sealed = store.recover_candidate(record, authority=_recovery_authority(candidate))
    assert sealed.manifest_hash == candidate.manifest_hash


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


def test_invalid_recovery_record_cannot_claim_job_b_or_move_candidate_a(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    (candidate.path / "partial.tmp").write_bytes(b"invalid candidate bytes")
    record = next(item for item in store.list_candidate_recovery() if item.path == candidate.path)
    forged_job_id = uuid4()
    forged = LabArtifactRecoveryRecord.model_construct(
        **{
            **record.model_dump(mode="python", round_trip=True),
            "job_id": forged_job_id,
            "manifest_hash": "f" * 64,
        }
    )
    candidate_before = _tree_identity(candidate.path)
    namespaces_before = _artifact_namespace_identity(store)

    with pytest.raises(LabArtifactIntegrityError, match="recovery evidence"):
        store.quarantine_recovery_record(forged, reason="forged B moves A")

    assert _tree_identity(candidate.path) == candidate_before
    assert _artifact_namespace_identity(store) == namespaces_before
    quarantined = store.quarantine_recovery_record(record, reason="actual invalid candidate")
    assert quarantined.job_id is None
    assert quarantined.manifest_hash is None
    assert (quarantined.path / "partial.tmp").read_bytes() == b"invalid candidate bytes"


def test_invalid_recovery_record_rederives_parseable_candidate_identity_before_quarantine(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    record = LabArtifactRecoveryRecord(
        path=candidate.path,
        status="invalid",
        device=candidate.device,
        inode=candidate.inode,
        file_type="directory",
        reason="stale caller classification",
    )

    quarantined = store.quarantine_recovery_record(record, reason="operator isolation")

    assert quarantined.job_id == candidate.job_id
    assert quarantined.manifest_hash == candidate.manifest_hash
    assert quarantined.device == candidate.device
    assert quarantined.inode == candidate.inode


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


def test_recover_interrupted_seal_normalizes_missing_intent_oserror(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    intent = store.seal_intents_root / f"{sealed.manifest.job_id.hex}.json"
    os.rename(intent, tmp_path / "missing-seal-intent.json")
    before_descriptors = len(os.listdir("/dev/fd"))

    with pytest.raises(
        LabArtifactIntegrityError,
        match="seal intent cannot be bound",
    ) as captured:
        store.recover_interrupted_seal(sealed.path)

    assert isinstance(captured.value.__cause__, FileNotFoundError)
    assert len(os.listdir("/dev/fd")) == before_descriptors


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

    with pytest.raises(ExceptionGroup) as captured:
        store.seal_candidate(candidate)

    assert len(captured.value.exceptions) == 2
    assert all(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)
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


def test_runtime_drift_before_seal_intent_rename_never_publishes_final_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drifted = False

    def mutation_guard() -> None:
        if drifted:
            raise RuntimeError("runtime drifted before seal intent rename")

    store = LabJobArtifactStore(tmp_path / "artifacts", mutation_guard=mutation_guard)
    candidate = _prepare(store)

    def drift_after_temp_fsync(_descriptor: int, _name: str) -> None:
        nonlocal drifted
        drifted = True

    monkeypatch.setattr(store, "_after_seal_intent_temp_fsync", drift_after_temp_fsync)

    with pytest.raises(RuntimeError, match="seal intent rename"):
        store.seal_candidate(candidate)

    assert not (store.seal_intents_root / f"{candidate.job_id.hex}.json").exists()
    assert candidate.path.exists()
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600
        for path in candidate.path.rglob("*")
        if path.is_file()
    )


def test_runtime_drift_before_first_freeze_leaves_candidate_unmodified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drifted = False

    def mutation_guard() -> None:
        if drifted:
            raise RuntimeError("runtime drifted before artifact freeze")

    store = LabJobArtifactStore(tmp_path / "artifacts", mutation_guard=mutation_guard)
    candidate = _prepare(store)

    def drift_after_intent_publish(_bound: object) -> None:
        nonlocal drifted
        drifted = True

    monkeypatch.setattr(store, "_after_seal_intent_publish", drift_after_intent_publish)

    with pytest.raises(RuntimeError, match="artifact freeze"):
        store.seal_candidate(candidate)

    assert candidate.path.exists()
    assert stat.S_IMODE(candidate.path.stat().st_mode) == 0o700
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600
        for path in candidate.path.rglob("*")
        if path.is_file()
    )


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

    assert record.status == "recoverable_torn"
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

    assert record.status == "recoverable_torn"
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


def test_bound_zip_destination_matches_existing_path_export(tmp_path: Path) -> None:
    from rquant.lab_artifacts import LabBoundZipDestination

    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    evidence = _evidence(sealed)
    path_export = store.export_deterministic_zip(
        sealed.path,
        evidence,
        tmp_path / "path-export.zip",
    )
    bound_parent = tmp_path / "bound-export"
    bound_parent.mkdir(mode=0o700)
    descriptor = os.open(
        bound_parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        observed = os.fstat(descriptor)
        bound_export = store.export_deterministic_zip_bound(
            sealed.path,
            evidence,
            LabBoundZipDestination(
                directory_path=bound_parent,
                directory_descriptor=descriptor,
                directory_device=observed.st_dev,
                directory_inode=observed.st_ino,
                file_name="result.zip",
            ),
        )
    finally:
        os.close(descriptor)

    assert bound_export == bound_parent / "result.zip"
    assert bound_export.read_bytes() == path_export.read_bytes()
    assert stat.S_IMODE(bound_export.stat().st_mode) == 0o600


def test_job_zip_export_accepts_only_job_id_and_returns_request_scoped_hash_receipts(
    tmp_path: Path,
) -> None:
    from rquant.lab_artifact_export import LabJobZipExportFacade
    from rquant.lab_jobs import LabJobReader
    from tests.unit.test_lab_finalizer import _ready_scenario

    scenario_root = tmp_path / "scenario"
    scenario_root.mkdir(mode=0o700)
    scenario = _ready_scenario(scenario_root, hold_days=(1,))
    assert scenario.finalizer().finalize(scenario.job_id).status == "published"
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    export_root = tmp_path / "private-exports"
    facade = LabJobZipExportFacade(
        reader=LabJobReader(scenario.store.path),
        artifact_store=scenario.artifact_store,
        export_root=export_root,
    )

    first = facade.export(scenario.job_id)
    repeated = facade.export(scenario.job_id)

    assert first.job_id == repeated.job_id == scenario.job_id
    assert first.request_id != repeated.request_id
    assert first.path != repeated.path
    assert first.path.relative_to(export_root) == Path(
        scenario.job_id.hex,
        first.request_id.hex,
        "result.zip",
    )
    assert repeated.path.relative_to(export_root) == Path(
        scenario.job_id.hex,
        repeated.request_id.hex,
        "result.zip",
    )
    assert first.byte_size == first.path.stat().st_size
    assert repeated.byte_size == repeated.path.stat().st_size
    assert first.sha256 == repeated.sha256
    assert first.sha256 == hashlib.sha256(first.path.read_bytes()).hexdigest()
    assert first.path.read_bytes() == repeated.path.read_bytes()
    forged = first.model_copy(update={"sha256": "0" * 64})
    with pytest.raises(LabArtifactIntegrityError, match="receipt"):
        facade.discard(forged)
    assert first.path.exists()

    facade.discard(first)

    assert not first.path.exists()
    assert first.path.parent.is_dir()
    first_tombstones = tuple(first.path.parent.glob("*.discarded"))
    assert len(first_tombstones) == 1
    assert first_tombstones[0].stat().st_size == 0
    assert repeated.path.exists()
    facade.discard(repeated)
    job_root = export_root / scenario.job_id.hex
    assert job_root.is_dir()
    retired_files = tuple(path for path in job_root.rglob("*") if path.is_file())
    assert len(retired_files) == 2
    assert sum(path.stat().st_size for path in retired_files) == 0
    with pytest.raises(TypeError, match="destination"):
        facade.export(  # type: ignore[call-arg]
            scenario.job_id,
            destination=tmp_path / "caller-selected.zip",
        )
    with pytest.raises(TypeError, match="receipt"):
        facade.discard(first.path)  # type: ignore[arg-type]


def test_job_zip_export_requires_authoritative_succeeded_sealed_result(
    tmp_path: Path,
) -> None:
    from rquant.lab_artifact_export import (
        LabJobZipExportFacade,
        LabJobZipExportUnavailableError,
    )
    from rquant.lab_jobs import LabJobReader
    from tests.unit.test_lab_finalizer import _ready_scenario

    scenario_root = tmp_path / "scenario"
    scenario_root.mkdir(mode=0o700)
    scenario = _ready_scenario(scenario_root, hold_days=(1,))
    export_root = tmp_path / "private-exports"
    facade = LabJobZipExportFacade(
        reader=LabJobReader(scenario.store.path),
        artifact_store=scenario.artifact_store,
        export_root=export_root,
    )

    with pytest.raises(LabJobZipExportUnavailableError, match="succeeded.*sealed"):
        facade.export(scenario.job_id)
    with pytest.raises(LabJobZipExportUnavailableError, match="succeeded.*sealed"):
        facade.export(uuid4())

    assert tuple(export_root.rglob("*.zip")) == ()


def test_job_zip_discard_reclaims_only_the_verified_inode_after_path_replacement(
    tmp_path: Path,
) -> None:
    from rquant.lab_artifact_export import LabJobZipExportFacade
    from rquant.lab_jobs import LabJobReader
    from tests.unit.test_lab_finalizer import _ready_scenario

    scenario_root = tmp_path / "scenario"
    scenario_root.mkdir(mode=0o700)
    scenario = _ready_scenario(scenario_root, hold_days=(1,))
    assert scenario.finalizer().finalize(scenario.job_id).status == "published"
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    facade = LabJobZipExportFacade(
        reader=LabJobReader(scenario.store.path),
        artifact_store=scenario.artifact_store,
        export_root=tmp_path / "private-exports",
    )
    receipt = facade.export(scenario.job_id)
    replacement_names: list[str] = []

    displaced_names: list[str] = []

    def replace_before_truncate(directory_descriptor: int, name: str) -> None:
        displaced = f"{name}.original"
        os.rename(
            name,
            displaced,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=directory_descriptor,
        )
        try:
            os.write(descriptor, b"replacement")
        finally:
            os.close(descriptor)
        replacement_names.append(name)
        displaced_names.append(displaced)

    facade._before_discard_truncate = replace_before_truncate  # type: ignore[method-assign]

    facade.discard(receipt)

    request_root = receipt.path.parent
    assert replacement_names
    assert (request_root / replacement_names[0]).read_bytes() == b"replacement"
    assert displaced_names
    assert (request_root / displaced_names[0]).stat().st_size == 0
    assert not receipt.path.exists()


def test_job_zip_export_record_budget_bounds_online_tombstones(tmp_path: Path) -> None:
    from rquant.lab_artifact_export import (
        LabJobZipExportCapacityError,
        LabJobZipExportFacade,
    )
    from rquant.lab_jobs import LabJobReader
    from tests.unit.test_lab_finalizer import _ready_scenario

    scenario_root = tmp_path / "scenario"
    scenario_root.mkdir(mode=0o700)
    scenario = _ready_scenario(scenario_root, hold_days=(1,))
    assert scenario.finalizer().finalize(scenario.job_id).status == "published"
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    facade = LabJobZipExportFacade(
        reader=LabJobReader(scenario.store.path),
        artifact_store=scenario.artifact_store,
        export_root=tmp_path / "private-exports",
        max_export_records=1,
    )
    receipt = facade.export(scenario.job_id)
    facade.discard(receipt)

    with pytest.raises(LabJobZipExportCapacityError, match="budget is exhausted"):
        facade.export(scenario.job_id)

    assert receipt.path.parent.is_dir()
    assert sum(path.stat().st_size for path in receipt.path.parent.iterdir()) == 0


def test_job_zip_export_rejects_symlink_or_replaced_export_root(tmp_path: Path) -> None:
    from rquant.lab_artifact_export import LabJobZipExportFacade
    from rquant.lab_artifacts import LabArtifactIntegrityError, LabArtifactPathError
    from rquant.lab_jobs import LabJobReader
    from tests.unit.test_lab_finalizer import _ready_scenario

    scenario_root = tmp_path / "scenario"
    scenario_root.mkdir(mode=0o700)
    scenario = _ready_scenario(scenario_root, hold_days=(1,))
    assert scenario.finalizer().finalize(scenario.job_id).status == "published"
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    external = tmp_path / "external"
    external.mkdir(mode=0o700)
    symlink_root = tmp_path / "symlink-exports"
    symlink_root.symlink_to(external, target_is_directory=True)

    with pytest.raises(LabArtifactPathError, match="unsafe"):
        LabJobZipExportFacade(
            reader=LabJobReader(scenario.store.path),
            artifact_store=scenario.artifact_store,
            export_root=symlink_root,
        )

    export_root = tmp_path / "private-exports"
    facade = LabJobZipExportFacade(
        reader=LabJobReader(scenario.store.path),
        artifact_store=scenario.artifact_store,
        export_root=export_root,
    )
    displaced = tmp_path / "displaced-exports"
    export_root.rename(displaced)
    export_root.mkdir(mode=0o700)

    with pytest.raises(LabArtifactIntegrityError, match="export root identity changed"):
        facade.export(scenario.job_id)

    assert tuple(export_root.iterdir()) == ()


def test_job_zip_export_root_swap_after_bound_open_never_writes_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_artifact_export import LabJobZipExportFacade
    from rquant.lab_artifacts import LabArtifactIntegrityError
    from rquant.lab_jobs import LabJobReader
    from tests.unit.test_lab_finalizer import _ready_scenario

    scenario_root = tmp_path / "scenario"
    scenario_root.mkdir(mode=0o700)
    scenario = _ready_scenario(scenario_root, hold_days=(1,))
    assert scenario.finalizer().finalize(scenario.job_id).status == "published"
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    export_root = tmp_path / "private-exports"
    displaced_root = tmp_path / "bound-private-exports"
    facade = LabJobZipExportFacade(
        reader=LabJobReader(scenario.store.path),
        artifact_store=scenario.artifact_store,
        export_root=export_root,
    )
    original_open = facade._open_bound_export_root
    swapped = False

    def swap_after_bound_open() -> int:
        nonlocal swapped
        descriptor = original_open()
        if not swapped:
            export_root.rename(displaced_root)
            export_root.mkdir(mode=0o700)
            swapped = True
        return descriptor

    monkeypatch.setattr(facade, "_open_bound_export_root", swap_after_bound_open)

    with pytest.raises(LabArtifactIntegrityError, match="identity changed"):
        facade.export(scenario.job_id)

    assert swapped is True
    assert tuple(export_root.iterdir()) == ()
    for controlled_path in displaced_root.rglob("*"):
        assert not controlled_path.is_symlink()
        assert controlled_path.relative_to(displaced_root).parts[0] == scenario.job_id.hex


def test_zip_export_interleaves_large_file_reads_and_archive_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    arguments["tables"] = {
        "alpha": pd.DataFrame(
            {
                "sequence": np.arange(250_000, dtype=np.int64),
                "value": np.linspace(-1.0, 1.0, 250_000, dtype=np.float64),
            }
        ),
        "beta": pd.DataFrame(
            {
                "sequence": np.arange(250_000, 500_000, dtype=np.int64),
                "value": np.linspace(1.0, 3.0, 250_000, dtype=np.float64),
            }
        ),
    }
    sealed = store.seal_candidate(store.prepare_candidate(**arguments))
    source_identities = {
        (path.stat().st_dev, path.stat().st_ino)
        for path in (sealed.path / "tables").glob("*.parquet")
    }
    original_read = lab_artifacts_module._read_descriptor
    original_os_read = lab_artifacts_module.os.read
    source_read_sizes: list[int] = []
    archive_write_sizes: list[int] = []
    full_read_identities: list[tuple[int, int]] = []
    stream_events: list[tuple[str, int]] = []
    archive_streaming = False

    def reject_full_source_read(descriptor: int) -> bytes:
        observed = os.fstat(descriptor)
        identity = (observed.st_dev, observed.st_ino)
        full_read_identities.append(identity)
        if identity in source_identities:
            raise AssertionError("Parquet source reached full descriptor read")
        return original_read(descriptor)

    def record_chunk_read(descriptor: int, size: int) -> bytes:
        observed = os.fstat(descriptor)
        chunk = original_os_read(descriptor, size)
        if archive_streaming and (observed.st_dev, observed.st_ino) in source_identities:
            source_read_sizes.append(size)
            if chunk:
                stream_events.append(("read", len(chunk)))
        return chunk

    class RecordingArchiveWriter:
        def __init__(self, raw: object) -> None:
            self._raw = raw

        def __enter__(self) -> RecordingArchiveWriter:
            self._raw.__enter__()  # type: ignore[attr-defined]
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: object | None,
        ) -> bool | None:
            return self._raw.__exit__(exc_type, exc_value, traceback)  # type: ignore[attr-defined]

        def write(self, data: bytes) -> int:
            archive_write_sizes.append(len(data))
            stream_events.append(("write", len(data)))
            return int(self._raw.write(data))  # type: ignore[attr-defined]

    class RecordingZipFile(ZipFile):
        def __enter__(self) -> RecordingZipFile:
            nonlocal archive_streaming
            super().__enter__()
            archive_streaming = True
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: object | None,
        ) -> None:
            nonlocal archive_streaming
            try:
                super().__exit__(exc_type, exc_value, traceback)
            finally:
                archive_streaming = False

        def open(
            self,
            name: str | ZipInfo,
            mode: str = "r",
            pwd: bytes | None = None,
            *,
            force_zip64: bool = False,
        ) -> object:
            opened = super().open(
                name,
                mode=mode,
                pwd=pwd,
                force_zip64=force_zip64,
            )
            if mode == "w":
                return RecordingArchiveWriter(opened)
            return opened

    monkeypatch.setattr(lab_artifacts_module, "_read_descriptor", reject_full_source_read)
    monkeypatch.setattr(lab_artifacts_module.os, "read", record_chunk_read)
    monkeypatch.setattr(lab_artifacts_module, "ZipFile", RecordingZipFile)

    destination = store.export_deterministic_zip(
        sealed.path,
        _evidence(sealed),
        tmp_path / "streamed.zip",
    )

    assert destination.is_file()
    assert not source_identities.intersection(full_read_identities)
    assert len(source_read_sizes) > 4
    assert max(source_read_sizes) <= 1024 * 1024
    assert len(archive_write_sizes) > 4
    assert max(archive_write_sizes) <= 1024 * 1024
    first_source_read = next(
        index for index, event in enumerate(stream_events) if event[0] == "read"
    )
    source_stream = stream_events[first_source_read:]
    assert [kind for kind, _size in source_stream] == [
        kind for _ in range(len(source_read_sizes) - 2) for kind in ("read", "write")
    ]
    assert all(
        source_stream[index][1] == source_stream[index + 1][1]
        for index in range(0, len(source_stream), 2)
    )


def test_zip_export_hashes_published_output_without_full_descriptor_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    arguments["tables"] = {
        "large": pd.DataFrame(
            {
                "sequence": np.arange(100_000, dtype=np.int64),
                "value": np.linspace(-5.0, 5.0, 100_000, dtype=np.float64),
            }
        )
    }
    sealed = store.seal_candidate(store.prepare_candidate(**arguments))
    original_read = lab_artifacts_module._read_descriptor
    original_stream_hash = lab_artifacts_module._sha256_descriptor
    full_read_identities: list[tuple[int, int]] = []
    stream_hash_identities: list[tuple[int, int]] = []

    def record_full_read(descriptor: int) -> bytes:
        observed = os.fstat(descriptor)
        full_read_identities.append((observed.st_dev, observed.st_ino))
        return original_read(descriptor)

    def record_stream_hash(descriptor: int) -> str:
        observed = os.fstat(descriptor)
        stream_hash_identities.append((observed.st_dev, observed.st_ino))
        return original_stream_hash(descriptor)

    monkeypatch.setattr(lab_artifacts_module, "_read_descriptor", record_full_read)
    monkeypatch.setattr(lab_artifacts_module, "_sha256_descriptor", record_stream_hash)

    destination = store.export_deterministic_zip(
        sealed.path,
        _evidence(sealed),
        tmp_path / "large-output.zip",
    )

    output_identity = (destination.stat().st_dev, destination.stat().st_ino)
    assert output_identity not in full_read_identities
    assert stream_hash_identities.count(output_identity) >= 3


def test_zip_export_rechecks_bytes_after_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    evidence = _evidence(sealed)
    original = store._authorize_export

    def tamper_after_authorization(
        verified: LabSealedJobArtifact,
        supplied: LabArtifactIndexEvidence,
    ) -> None:
        original(verified, supplied)
        _allow_writes(verified.path)
        (verified.path / "report.md").write_text("changed", encoding="utf-8")

    monkeypatch.setattr(store, "_authorize_export", tamper_after_authorization)

    with pytest.raises(
        LabArtifactIntegrityError,
        match="export bytes conflict|bound artifact bundle identity changed",
    ):
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

    def swap_after_authorization(
        verified: LabSealedJobArtifact,
        supplied: LabArtifactIndexEvidence,
    ) -> None:
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
    assert list(tmp_path.glob(f".{destination.name}.*.tmp")) == []
    assert list(tmp_path.glob(f".{destination.name}.*.discarded")) == []


def test_repeated_zip_conflicts_leave_no_temporary_files_or_descriptors(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    before_descriptors = len(os.listdir("/dev/fd"))

    for sequence in range(32):
        destination = tmp_path / f"reserved-{sequence}.zip"
        destination.write_bytes(b"reservation")
        with pytest.raises(LabArtifactConflictError, match="already exists"):
            store.export_deterministic_zip(sealed.path, _evidence(sealed), destination)

    assert list(tmp_path.glob(".*.tmp")) == []
    assert list(tmp_path.glob(".*.discarded")) == []
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_zip_temp_cleanup_never_removes_replacement_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    destination = tmp_path / "raced.zip"
    replacement_payload = b"attacker replacement"
    replaced_temp: Path | None = None

    def replace_temp_then_conflict(
        source_parent: int,
        source_name: str,
        _destination_parent: int,
        _destination_name: str,
    ) -> None:
        nonlocal replaced_temp
        os.unlink(source_name, dir_fd=source_parent)
        descriptor = os.open(
            source_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=source_parent,
        )
        try:
            os.write(descriptor, replacement_payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        replaced_temp = tmp_path / source_name
        raise OSError(errno.EEXIST, "injected destination conflict")

    monkeypatch.setattr(
        store,
        "_atomic_zip_publish_noreplace",
        replace_temp_then_conflict,
    )

    with pytest.raises(ExceptionGroup) as captured:
        store.export_deterministic_zip(sealed.path, _evidence(sealed), destination)

    assert any(isinstance(item, LabArtifactConflictError) for item in captured.value.exceptions)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)
    assert replaced_temp is not None
    assert replaced_temp.read_bytes() == replacement_payload


def test_zip_discard_cleanup_never_unlinks_replacement_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    destination = tmp_path / "reserved.zip"
    destination.write_bytes(b"reservation")
    replacement_payload = b"replacement isolation evidence"
    replacement_path: Path | None = None

    def replace_discard_before_unlink(parent_descriptor: int, name: str) -> None:
        nonlocal replacement_path
        os.unlink(name, dir_fd=parent_descriptor)
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=parent_descriptor,
        )
        try:
            os.write(descriptor, replacement_payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        replacement_path = tmp_path / name

    monkeypatch.setattr(
        store,
        "_before_zip_temporary_unlink",
        replace_discard_before_unlink,
        raising=False,
    )

    with pytest.raises(ExceptionGroup) as captured:
        store.export_deterministic_zip(sealed.path, _evidence(sealed), destination)

    assert any(isinstance(item, LabArtifactConflictError) for item in captured.value.exceptions)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)
    assert destination.read_bytes() == b"reservation"
    assert replacement_path is not None
    assert replacement_path.read_bytes() == replacement_payload


def test_zip_publication_and_cleanup_failures_are_both_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    destination = tmp_path / "reserved.zip"
    destination.write_bytes(b"reservation")
    before_descriptors = len(os.listdir("/dev/fd"))

    def fail_cleanup(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected ZIP cleanup failure")

    monkeypatch.setattr(store, "_quarantine_failed_zip_temporary", fail_cleanup)

    with pytest.raises(BaseExceptionGroup) as captured:
        store.export_deterministic_zip(sealed.path, _evidence(sealed), destination)

    assert any(isinstance(item, LabArtifactConflictError) for item in captured.value.exceptions)
    assert any(isinstance(item, OSError) for item in captured.value.exceptions)
    assert len(os.listdir("/dev/fd")) == before_descriptors


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


def test_seal_intent_preserves_caller_and_final_identity_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    candidate = _prepare(store)
    before_descriptors = len(os.listdir("/dev/fd"))

    def fail_final_identity(_bound: object) -> None:
        raise LabArtifactIntegrityError("injected final intent failure")

    with (
        pytest.raises(BaseExceptionGroup) as captured,
        store._bind_seal_intent(
            candidate.job_id,
            candidate=candidate,
            create=True,
        ),
    ):
        monkeypatch.setattr(
            store,
            "_assert_bound_seal_intent",
            fail_final_identity,
        )
        raise RuntimeError("injected caller failure")

    assert any(isinstance(item, RuntimeError) for item in captured.value.exceptions)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_public_verified_sealed_binding_reads_small_payloads_seven_times(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    read_descriptor = lab_artifacts_module._read_descriptor
    read_sizes: list[int] = []

    def count_read(descriptor: int) -> bytes:
        payload = read_descriptor(descriptor)
        read_sizes.append(len(payload))
        return payload

    monkeypatch.setattr(lab_artifacts_module, "_read_descriptor", count_read)

    with store.bind_verified_sealed(
        sealed.path,
        indexed_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
    ):
        pass

    assert len(read_sizes) == 7


def test_public_verified_sealed_binding_streams_large_parquet_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    arguments = _prepare_arguments()
    arguments["tables"] = {
        "large": pd.DataFrame(
            {
                "sequence": np.arange(250_000, dtype=np.int64),
                "value": np.linspace(-1.0, 1.0, 250_000, dtype=np.float64),
            }
        )
    }
    sealed = store.seal_candidate(store.prepare_candidate(**arguments))
    parquet_size = (sealed.path / "tables" / "large.parquet").stat().st_size
    read_descriptor = lab_artifacts_module._read_descriptor
    read_sizes: list[int] = []

    def record_small_read(descriptor: int) -> bytes:
        payload = read_descriptor(descriptor)
        read_sizes.append(len(payload))
        return payload

    monkeypatch.setattr(lab_artifacts_module, "_read_descriptor", record_small_read)

    with store.bind_verified_sealed(
        sealed.path,
        indexed_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
    ):
        pass

    assert len(read_sizes) == 7
    assert parquet_size > max(read_sizes)


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


def test_legacy_missing_source_failures_do_not_leak_parent_descriptors(tmp_path: Path) -> None:
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    missing = tmp_path / "sources" / "missing.json"
    missing.parent.mkdir(mode=0o700)
    before_descriptors = len(os.listdir("/dev/fd"))

    for sequence in range(20):
        with pytest.raises(LabArtifactIntegrityError, match="cannot be opened safely"):
            index.import_file(
                logical_run_id=f"missing-{sequence}",
                source_path=missing,
            )

    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_legacy_invalidated_source_can_publish_a_new_generation(tmp_path: Path) -> None:
    source = tmp_path / "legacy.md"
    source.write_text("first", encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "legacy-index.sqlite3")
    original = index.import_file(logical_run_id="old-run", source_path=source)
    source.write_text("second", encoding="utf-8")

    replacement = index.import_file(logical_run_id="old-run", source_path=source)

    assert replacement.status == "imported"
    assert replacement.record.sha256 != original.record.sha256
    assert index.get("old-run") == replacement.record


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

    def replace_then_check(
        path: Path,
        expected: lab_artifacts_module._FileObservation,
    ) -> None:
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


def test_legacy_cache_rebuild_parent_fsync_failure_closes_published_cache_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    registry_before = dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS)
    path = tmp_path / "index" / "legacy.sqlite3"
    failed = LegacyArtifactIndex.__new__(LegacyArtifactIndex)
    original_rename = lab_artifacts_module._rename_noreplace
    original_fsync = lab_artifacts_module.os.fsync
    cache_published = False

    def publish_then_mark(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        nonlocal cache_published
        original_rename(
            source_parent,
            source_name,
            destination_parent,
            destination_name,
        )
        if source_name.endswith(".cache.tmp") and destination_name == path.name:
            cache_published = True

    def fail_parent_fsync_after_cache_publish(descriptor: int) -> None:
        nonlocal cache_published
        if cache_published and descriptor == failed._parent_descriptor:
            cache_published = False
            raise OSError("injected parent fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(lab_artifacts_module, "_rename_noreplace", publish_then_mark)
    monkeypatch.setattr(lab_artifacts_module.os, "fsync", fail_parent_fsync_after_cache_publish)

    with pytest.raises(OSError, match="parent fsync"):
        failed.__init__(path)

    assert dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS) == registry_before
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors
    monkeypatch.setattr(lab_artifacts_module, "_rename_noreplace", original_rename)
    monkeypatch.setattr(lab_artifacts_module.os, "fsync", original_fsync)
    recovered = LegacyArtifactIndex(path)
    recovered.close()


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_private_file_open_closes_descriptor_on_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
) -> None:
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    parent_descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    before_descriptors = len(os.listdir("/dev/fd"))
    original_fsync = lab_artifacts_module.os.fsync

    def interrupt_file_fsync(descriptor: int) -> None:
        if descriptor != parent_descriptor:
            raise failure_type("injected base exception")
        original_fsync(descriptor)

    monkeypatch.setattr(lab_artifacts_module.os, "fsync", interrupt_file_fsync)

    with pytest.raises(failure_type, match="base exception"):
        lab_artifacts_module._open_or_create_private_regular_at(
            parent_descriptor,
            "legacy.sqlite3.lock",
            access_flags=os.O_RDWR,
            require_private_existing=True,
        )

    assert len(os.listdir("/dev/fd")) == before_descriptors
    os.close(parent_descriptor)


def test_private_file_open_preserves_primary_and_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    parent_descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    before_descriptors = len(os.listdir("/dev/fd"))
    original_fsync = lab_artifacts_module.os.fsync
    original_close = lab_artifacts_module.os.close
    interrupted_descriptor = -1

    def interrupt_file_fsync(descriptor: int) -> None:
        nonlocal interrupted_descriptor
        if descriptor != parent_descriptor:
            interrupted_descriptor = descriptor
            raise KeyboardInterrupt("primary open failure")
        original_fsync(descriptor)

    def close_then_fail(descriptor: int) -> None:
        original_close(descriptor)
        if descriptor == interrupted_descriptor:
            raise OSError("descriptor close failure")

    monkeypatch.setattr(lab_artifacts_module.os, "fsync", interrupt_file_fsync)
    monkeypatch.setattr(lab_artifacts_module.os, "close", close_then_fail)

    with pytest.raises(BaseExceptionGroup) as captured:
        lab_artifacts_module._open_or_create_private_regular_at(
            parent_descriptor,
            "legacy.sqlite3.lock",
            access_flags=os.O_RDWR,
            require_private_existing=True,
        )

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, KeyboardInterrupt) for item in flattened)
    assert any(isinstance(item, OSError) and "close" in str(item) for item in flattened)
    monkeypatch.setattr(lab_artifacts_module.os, "close", original_close)
    assert len(os.listdir("/dev/fd")) == before_descriptors
    os.close(parent_descriptor)


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_legacy_sqlite_connect_closes_connection_on_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
) -> None:
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    process_lock_key = index._process_lock_key
    before_descriptors = len(os.listdir("/dev/fd"))

    def interrupt_after_connect(_connection: sqlite3.Connection) -> None:
        raise failure_type("injected post-connect base exception")

    monkeypatch.setattr(index, "_after_sqlite_connect", interrupt_after_connect)

    with pytest.raises(failure_type, match="post-connect"):
        index.get("missing-run")

    assert len(os.listdir("/dev/fd")) == before_descriptors
    assert index._process_lock_entry is not None
    assert index._process_lock_entry.owner_thread_id is None
    index.close()
    assert process_lock_key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS


def test_legacy_sqlite_connect_preserves_primary_and_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    before_descriptors = len(os.listdir("/dev/fd"))
    real_connect = sqlite3.connect

    class FailingCloseConnection(sqlite3.Connection):
        def close(self) -> None:
            super().close()
            raise OSError("SQLite close failure")

    def failing_close_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = FailingCloseConnection
        return real_connect(*args, **kwargs)  # type: ignore[arg-type]

    def fail_after_connect(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("post-connect primary failure")

    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", failing_close_connect)
    monkeypatch.setattr(index, "_after_sqlite_connect", fail_after_connect)

    with pytest.raises(BaseExceptionGroup) as captured:
        index.get("missing-run")

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(isinstance(item, OSError) and "SQLite close" in str(item) for item in flattened)
    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", real_connect)
    assert len(os.listdir("/dev/fd")) == before_descriptors
    index.close()


def test_legacy_cache_context_preserves_caller_and_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    before_descriptors = len(os.listdir("/dev/fd"))
    real_connect = sqlite3.connect

    class FailingCloseConnection(sqlite3.Connection):
        def close(self) -> None:
            super().close()
            raise OSError("runtime SQLite close failure")

    def failing_close_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = FailingCloseConnection
        return real_connect(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", failing_close_connect)

    with pytest.raises(BaseExceptionGroup) as captured, index._cache_connection():
        raise RuntimeError("runtime SQLite caller failure")

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(
        isinstance(item, OSError) and "runtime SQLite close" in str(item) for item in flattened
    )
    monkeypatch.setattr(lab_artifacts_module.sqlite3, "connect", real_connect)
    assert len(os.listdir("/dev/fd")) == before_descriptors
    index.close()


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


def test_legacy_constructor_private_parent_failure_rolls_back_registry_and_fds(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "index"
    parent.mkdir(mode=0o755)
    os.chmod(parent, 0o755)
    path = parent / "legacy.sqlite3"
    registry_before = dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS)
    before_descriptors = len(os.listdir("/dev/fd"))
    failed = LegacyArtifactIndex.__new__(LegacyArtifactIndex)

    with pytest.raises(LabArtifactIntegrityError, match="permissions.*0700"):
        failed.__init__(path)

    assert dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS) == registry_before
    assert len(os.listdir("/dev/fd")) == before_descriptors
    os.chmod(parent, 0o700)
    recovered = LegacyArtifactIndex(path)
    process_lock_key = recovered._process_lock_key
    recovered.close()
    assert process_lock_key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS


@pytest.mark.parametrize("failure_point", ["lock", "authority", "head"])
def test_legacy_constructor_injected_failure_rolls_back_registry_and_fds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    path = tmp_path / "index" / "legacy.sqlite3"
    registry_before = dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS)
    before_descriptors = len(os.listdir("/dev/fd"))
    original_open = lab_artifacts_module._open_or_create_private_regular_at
    original_bind_head = LegacyArtifactIndex._bind_or_create_authority_head
    failed = LegacyArtifactIndex.__new__(LegacyArtifactIndex)

    def fail_selected_open(
        parent_descriptor: int,
        name: str,
        *,
        access_flags: int,
        require_private_existing: bool = False,
    ) -> tuple[int, bool]:
        selected = {
            "lock": f"{path.name}.lock",
            "authority": f"{path.name}.authority.jsonl",
        }.get(failure_point)
        if name == selected:
            raise OSError(f"injected {failure_point} open failure")
        return original_open(
            parent_descriptor,
            name,
            access_flags=access_flags,
            require_private_existing=require_private_existing,
        )

    def fail_head_initialization(_index: LegacyArtifactIndex) -> None:
        raise OSError("injected head initialization failure")

    monkeypatch.setattr(
        lab_artifacts_module,
        "_open_or_create_private_regular_at",
        fail_selected_open,
    )
    if failure_point == "head":
        monkeypatch.setattr(
            LegacyArtifactIndex,
            "_bind_or_create_authority_head",
            fail_head_initialization,
        )

    with pytest.raises(OSError, match=failure_point):
        failed.__init__(path)

    assert dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS) == registry_before
    assert len(os.listdir("/dev/fd")) == before_descriptors
    monkeypatch.setattr(
        lab_artifacts_module,
        "_open_or_create_private_regular_at",
        original_open,
    )
    monkeypatch.setattr(
        LegacyArtifactIndex,
        "_bind_or_create_authority_head",
        original_bind_head,
    )
    recovered = LegacyArtifactIndex(path)
    process_lock_key = recovered._process_lock_key
    recovered.close()
    assert process_lock_key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS


def test_legacy_constructor_preserves_primary_and_unlock_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    registry_before = dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS)
    path = tmp_path / "index" / "legacy.sqlite3"
    failed = LegacyArtifactIndex.__new__(LegacyArtifactIndex)
    original_flock = lab_artifacts_module.fcntl.flock
    unlock_failed = False

    def fail_head(_index: LegacyArtifactIndex) -> None:
        raise RuntimeError("primary head failure")

    def fail_first_unlock(descriptor: int, operation: int) -> None:
        nonlocal unlock_failed
        if operation == fcntl.LOCK_UN and not unlock_failed:
            unlock_failed = True
            raise OSError("unlock cleanup failure")
        original_flock(descriptor, operation)

    monkeypatch.setattr(LegacyArtifactIndex, "_bind_or_create_authority_head", fail_head)
    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", fail_first_unlock)

    with pytest.raises(BaseExceptionGroup) as captured:
        failed.__init__(path)

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(isinstance(item, OSError) and "unlock" in str(item) for item in flattened)
    assert unlock_failed is True
    assert failed._closed is True
    assert dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS) == registry_before
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_legacy_constructor_preserves_primary_and_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    registry_before = dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS)
    path = tmp_path / "index" / "legacy.sqlite3"
    failed = LegacyArtifactIndex.__new__(LegacyArtifactIndex)
    original_close = lab_artifacts_module.os.close
    close_failed = False

    def fail_head(_index: LegacyArtifactIndex) -> None:
        raise RuntimeError("primary head failure")

    def close_then_fail(descriptor: int) -> None:
        nonlocal close_failed
        should_fail = descriptor == failed._lock_descriptor and not close_failed
        original_close(descriptor)
        if should_fail:
            close_failed = True
            raise OSError("close cleanup failure")

    monkeypatch.setattr(LegacyArtifactIndex, "_bind_or_create_authority_head", fail_head)
    monkeypatch.setattr(lab_artifacts_module.os, "close", close_then_fail)

    with pytest.raises(BaseExceptionGroup) as captured:
        failed.__init__(path)

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(isinstance(item, OSError) and "close" in str(item) for item in flattened)
    assert close_failed is True
    assert failed._closed is True
    assert dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS) == registry_before
    monkeypatch.setattr(lab_artifacts_module.os, "close", original_close)
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_legacy_process_lock_registry_releases_last_closed_instance(tmp_path: Path) -> None:
    path = tmp_path / "index" / "legacy.sqlite3"
    first = LegacyArtifactIndex(path)
    second = LegacyArtifactIndex(path)
    key = first._process_lock_key
    assert key is not None

    entry = lab_artifacts_module._LEGACY_PROCESS_LOCKS[key]
    assert entry.references == 2
    first.close()
    assert lab_artifacts_module._LEGACY_PROCESS_LOCKS[key].references == 1
    second.close()
    assert key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS


@pytest.mark.parametrize("nested_instance", ["same", "second"])
def test_legacy_same_thread_reentry_is_rejected_without_unlocking_outer_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    nested_instance: str,
) -> None:
    path = tmp_path / "index" / "legacy.sqlite3"
    source = tmp_path / "legacy.json"
    source.write_text('{"source":true}', encoding="utf-8")
    first = LegacyArtifactIndex(path)
    second = LegacyArtifactIndex(path)
    target = first if nested_instance == "same" else second
    key = first._process_lock_key
    assert key is not None
    probe_descriptor = os.open(path.with_name(f"{path.name}.lock"), os.O_RDWR)
    descriptor_attributes = (
        "_parent_descriptor",
        "_lock_descriptor",
        "_authority_descriptor",
        "_heads_descriptor",
        "_head_descriptor",
        "_database_descriptor",
        "_journal_descriptor",
        "_cache_quarantine_descriptor",
        "_authority_quarantine_descriptor",
    )
    original_flock = lab_artifacts_module.fcntl.flock
    callback_active = False
    callback_count = 0

    def avoid_pre_fix_second_instance_deadlock(descriptor: int, operation: int) -> None:
        if (
            callback_active
            and nested_instance == "second"
            and descriptor == second._lock_descriptor
        ):
            return
        original_flock(descriptor, operation)

    def reenter_during_stage(_record: object) -> None:
        nonlocal callback_active, callback_count
        callback_active = True
        callback_count += 1
        started = time.monotonic()
        try:
            with pytest.raises(LabArtifactIntegrityError, match="re-enter|reentrant"):
                target.get("nested-run")
            assert time.monotonic() - started < 0.5
            with pytest.raises(OSError) as blocked:
                original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert blocked.value.errno in {errno.EACCES, errno.EAGAIN}
        finally:
            callback_active = False

    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", avoid_pre_fix_second_instance_deadlock)
    monkeypatch.setattr(first, "_after_stage_commit", reenter_during_stage)

    imported = first.import_file(logical_run_id="outer-run", source_path=source)

    assert imported.status == "imported"
    assert callback_count == 1
    assert lab_artifacts_module._LEGACY_PROCESS_LOCKS[key].references == 2
    original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    original_flock(probe_descriptor, fcntl.LOCK_UN)
    owned_descriptors = {
        descriptor
        for index in (first, second)
        for attribute in descriptor_attributes
        if (descriptor := getattr(index, attribute)) >= 0
    }
    owned_identities = {
        descriptor: (observed.st_dev, observed.st_ino)
        for descriptor in owned_descriptors
        if (observed := os.fstat(descriptor))
    }
    assert len(owned_identities) == len(owned_descriptors)
    probe_observation = os.fstat(probe_descriptor)
    probe_identity = (probe_observation.st_dev, probe_observation.st_ino)
    relevant_identities = set(owned_identities.values()) | {probe_identity}
    expected_identity_counts = {identity: 0 for identity in relevant_identities}
    for identity in (*owned_identities.values(), probe_identity):
        expected_identity_counts[identity] += 1
    assert _descriptor_identity_counts(relevant_identities) == expected_identity_counts
    os.close(probe_descriptor)
    first.close()
    second.close()
    assert key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS
    assert all(
        getattr(index, attribute) == -1
        for index in (first, second)
        for attribute in descriptor_attributes
    )
    assert _descriptor_identity_counts(relevant_identities) == {
        identity: 0 for identity in relevant_identities
    }


@pytest.mark.parametrize("path_variant", ["same", "case_alias"])
def test_legacy_constructor_reentry_uses_lock_inode_before_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path_variant: str,
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    canonical_path = tmp_path / "Index" / "legacy.sqlite3"
    source = tmp_path / "legacy.json"
    source.write_text('{"source":true}', encoding="utf-8")
    first = LegacyArtifactIndex(canonical_path)
    nested_path = canonical_path
    if path_variant == "case_alias":
        alias_parent = tmp_path / "index"
        if (
            not alias_parent.exists()
            or alias_parent.stat().st_ino != canonical_path.parent.stat().st_ino
        ):
            first.close()
            pytest.skip("filesystem is case-sensitive")
        nested_path = alias_parent / canonical_path.name
    lock_path = canonical_path.with_name(f"{canonical_path.name}.lock")
    lock_identity = (lock_path.stat().st_dev, lock_path.stat().st_ino)
    probe_descriptor = os.open(lock_path, os.O_RDWR)
    operation_descriptors = len(os.listdir("/dev/fd"))
    original_flock = lab_artifacts_module.fcntl.flock
    callback_active = False
    callback_count = 0
    inner_flock_calls: list[int] = []

    def reject_pre_fix_inner_flock(descriptor: int, operation: int) -> None:
        observed = os.fstat(descriptor)
        if (
            callback_active
            and descriptor != first._lock_descriptor
            and operation & fcntl.LOCK_EX
            and (observed.st_dev, observed.st_ino) == lock_identity
        ):
            inner_flock_calls.append(descriptor)
            raise AssertionError("nested constructor reached flock")
        original_flock(descriptor, operation)

    def construct_from_clock() -> datetime:
        nonlocal callback_active, callback_count
        if callback_count == 0:
            callback_count += 1
            callback_active = True
            started = time.monotonic()
            try:
                with pytest.raises(LabArtifactIntegrityError, match="reentrant"):
                    LegacyArtifactIndex(nested_path)
                assert time.monotonic() - started < 0.5
                with pytest.raises(OSError) as blocked:
                    original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                assert blocked.value.errno in {errno.EACCES, errno.EAGAIN}
            finally:
                callback_active = False
        return datetime(2026, 7, 26, 9, tzinfo=UTC)

    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", reject_pre_fix_inner_flock)
    first.clock = construct_from_clock

    imported = first.import_file(logical_run_id="constructor-reentry", source_path=source)

    assert imported.status == "imported"
    assert callback_count == 1
    assert inner_flock_calls == []
    assert first._process_lock_entry is not None
    assert first._process_lock_entry.references == 1
    original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    original_flock(probe_descriptor, fcntl.LOCK_UN)
    assert len(os.listdir("/dev/fd")) == operation_descriptors
    os.close(probe_descriptor)
    process_lock_key = first._process_lock_key
    first.close()
    assert process_lock_key not in lab_artifacts_module._LEGACY_PROCESS_LOCKS
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_legacy_runtime_lock_preserves_caller_and_unlock_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    entry = index._process_lock_entry
    assert entry is not None
    lock_path = index.path.with_name(f"{index.path.name}.lock")
    probe_descriptor = os.open(lock_path, os.O_RDWR)
    before_descriptors = len(os.listdir("/dev/fd"))
    original_flock = lab_artifacts_module.fcntl.flock
    unlock_failed = False

    def unlock_then_fail(descriptor: int, operation: int) -> None:
        nonlocal unlock_failed
        original_flock(descriptor, operation)
        if operation == fcntl.LOCK_UN and not unlock_failed:
            unlock_failed = True
            raise OSError("runtime legacy unlock failure")

    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", unlock_then_fail)

    with pytest.raises(BaseExceptionGroup) as captured, index._exclusive_index_lock():
        raise RuntimeError("runtime legacy caller failure")

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(
        isinstance(item, OSError) and "runtime legacy unlock" in str(item) for item in flattened
    )
    assert unlock_failed is True
    assert index._authority_lock_depth == 0
    assert entry.owner_thread_id is None
    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", original_flock)
    original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    original_flock(probe_descriptor, fcntl.LOCK_UN)
    assert len(os.listdir("/dev/fd")) == before_descriptors
    os.close(probe_descriptor)
    index.close()


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


def test_legacy_close_waits_for_same_instance_import_and_rejects_new_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    staged = threading.Event()
    release = threading.Event()
    close_started = threading.Event()
    close_finished = threading.Event()
    results: list[str] = []
    errors: list[BaseException] = []

    def pause_after_stage(_record: object) -> None:
        staged.set()
        if not release.wait(timeout=10):
            raise TimeoutError("legacy import was not released")

    def run_import() -> None:
        try:
            results.append(index.import_file(logical_run_id="close-run", source_path=source).status)
        except BaseException as exc:
            errors.append(exc)

    def close_index() -> None:
        close_started.set()
        try:
            index.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            close_finished.set()

    monkeypatch.setattr(index, "_after_stage_commit", pause_after_stage)
    import_thread = threading.Thread(target=run_import)
    close_thread = threading.Thread(target=close_index)
    import_thread.start()
    assert staged.wait(timeout=10)
    close_thread.start()
    assert close_started.wait(timeout=10)
    time.sleep(0.25)

    assert close_finished.is_set() is False
    with pytest.raises(LabArtifactIntegrityError, match="closing|closed"):
        index.get("close-run")

    release.set()
    import_thread.join(timeout=10)
    close_thread.join(timeout=10)

    assert not import_thread.is_alive() and not close_thread.is_alive()
    assert results == ["imported"]
    assert errors == []
    assert close_finished.is_set() is True
    index.close()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_legacy_close_waits_for_second_instance_staged_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"old":true}', encoding="utf-8")
    path = tmp_path / "index" / "legacy.sqlite3"
    closing = LegacyArtifactIndex(path)
    importing = LegacyArtifactIndex(path)
    staged = threading.Event()
    release = threading.Event()
    close_finished = threading.Event()
    results: list[str] = []
    errors: list[BaseException] = []

    def pause_after_stage(_record: object) -> None:
        staged.set()
        if not release.wait(timeout=10):
            raise TimeoutError("legacy import was not released")

    def run_import() -> None:
        try:
            results.append(
                importing.import_file(
                    logical_run_id="second-instance-close",
                    source_path=source,
                ).status
            )
        except BaseException as exc:
            errors.append(exc)

    def close_first() -> None:
        try:
            closing.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            close_finished.set()

    monkeypatch.setattr(importing, "_after_stage_commit", pause_after_stage)
    import_thread = threading.Thread(target=run_import)
    close_thread = threading.Thread(target=close_first)
    import_thread.start()
    assert staged.wait(timeout=10)
    close_thread.start()
    time.sleep(0.25)

    assert close_finished.is_set() is False
    release.set()
    import_thread.join(timeout=10)
    close_thread.join(timeout=10)

    assert not import_thread.is_alive() and not close_thread.is_alive()
    assert results == ["imported"]
    assert errors == []
    assert importing.get("second-instance-close") is not None
    closing.close()
    importing.close()


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


def test_legacy_lock_preserves_caller_and_exit_identity_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"source":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    displaced = tmp_path / "displaced-legacy-cache.sqlite3"
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))

    def replace_cache_and_fail(_record: object) -> None:
        os.rename(index.path, displaced)
        index.path.write_bytes(b"replacement cache")
        os.chmod(index.path, 0o600)
        raise RuntimeError("caller failed after publication")

    monkeypatch.setattr(index, "_after_import_cache_sync", replace_cache_and_fail)

    with pytest.raises(ExceptionGroup) as captured:
        index.import_file(logical_run_id="caller-and-identity", source_path=source)

    assert any(isinstance(item, RuntimeError) for item in captured.value.exceptions)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_legacy_import_preserves_main_and_cleanup_probe_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"source":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    before_descriptors = len(os.listdir("/dev/fd"))

    def fail_after_publish(_record: object) -> None:
        raise RuntimeError("main import failure")

    def fail_cleanup_probe(_event: object) -> bool:
        raise OSError("cleanup probe failure")

    monkeypatch.setattr(index, "_after_published_authority_commit", fail_after_publish)
    monkeypatch.setattr(index, "_published_source_matches", fail_cleanup_probe)

    with pytest.raises(ExceptionGroup) as captured:
        index.import_file(logical_run_id="main-and-probe", source_path=source)

    assert any(isinstance(item, RuntimeError) for item in captured.value.exceptions)
    assert any(isinstance(item, OSError) for item in captured.value.exceptions)
    assert len(os.listdir("/dev/fd")) == before_descriptors


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

    with pytest.raises(ExceptionGroup) as captured:
        index.import_file(logical_run_id="reserved-head", source_path=source)

    assert any(isinstance(item, LabArtifactConflictError) for item in captured.value.exceptions)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in captured.value.exceptions)
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


def test_table_content_hash_streams_legacy_canonical_bytes_with_bounded_memory() -> None:
    small = pd.DataFrame(
        {
            "code": ["000001.SZ", "600000.SH"],
            "ret_pct": [1.25, float("nan")],
        }
    )
    legacy_payload = {
        "columns": list(small.columns),
        "dtypes": [str(dtype) for dtype in small.dtypes],
        "dtype_identities": lab_artifacts_module._frame_dtype_identities(small),
        "rows": [
            [lab_artifacts_module._canonical_table_value(value) for value in row]
            for row in small.itertuples(index=False, name=None)
        ],
    }
    expected = hashlib.sha256(lab_artifacts_module.canonical_json_bytes(legacy_payload)).hexdigest()
    assert lab_artifacts_module._table_content_hash(small) == expected

    frame = pd.DataFrame(
        {
            "code": [f"{index:06d}.SZ" for index in range(100_000)],
            "value": range(100_000),
        }
    )
    frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())
    tracemalloc.start()
    lab_artifacts_module._table_content_hash(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak <= max(16 * 1024 * 1024, frame_bytes * 3)


def test_table_content_hash_bounds_single_large_cjk_cell_scratch() -> None:
    value = "\u4e2d" * (32 * 1024 * 1024)
    frame = pd.DataFrame({"value": [value]})

    tracemalloc.start()
    digest = lab_artifacts_module._table_content_hash(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest) == 64
    assert peak <= 8 * 1024 * 1024


def test_table_content_hash_streams_bytes_with_legacy_base64_semantics() -> None:
    rng = random.Random(20260728)
    for size in (0, 1, 2, 3, 4, 5, 3071, 3072, 3073, 6143, 6144, 6145):
        value = rng.randbytes(size)
        frame = pd.DataFrame({"value": pd.Series([value], dtype=object)})
        legacy_payload = {
            "columns": list(frame.columns),
            "dtypes": [str(dtype) for dtype in frame.dtypes],
            "dtype_identities": lab_artifacts_module._frame_dtype_identities(frame),
            "rows": [[lab_artifacts_module._canonical_table_value(value)]],
        }
        expected = hashlib.sha256(
            lab_artifacts_module.canonical_json_bytes(legacy_payload)
        ).hexdigest()

        assert lab_artifacts_module._table_content_hash(frame) == expected


def test_table_content_hash_bounds_single_large_bytes_cell_scratch() -> None:
    size = 64 * 1024 * 1024
    value = (b"\x00\xffabc" * ((size + 4) // 5))[:size]
    frame = pd.DataFrame({"value": pd.Series([value], dtype=object)})

    tracemalloc.start()
    digest = lab_artifacts_module._table_content_hash(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest) == 64
    assert peak <= 2 * 1024 * 1024


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


def test_legacy_different_source_conflict_preserves_valid_published_authority(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original.json"
    replacement = tmp_path / "replacement.json"
    original.write_text('{"source":"original"}', encoding="utf-8")
    replacement.write_text('{"source":"replacement"}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
    imported = index.import_file(logical_run_id="stable-run", source_path=original)
    ledger = index.path.with_name(f"{index.path.name}.authority.jsonl")
    heads = index._authority_heads_path
    ledger_before = ledger.read_bytes()
    heads_before = {
        item.name: (item.read_bytes(), item.stat().st_ino) for item in sorted(heads.glob("*.json"))
    }

    for _ in range(2):
        with pytest.raises(
            LabLegacyArtifactConflictError,
            match="different source|already references",
        ):
            index.import_file(logical_run_id="stable-run", source_path=replacement)

        assert ledger.read_bytes() == ledger_before
        assert {
            item.name: (item.read_bytes(), item.stat().st_ino)
            for item in sorted(heads.glob("*.json"))
        } == heads_before
        assert index.get("stable-run") == imported.record


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin namespace guard review")
def test_candidate_payload_never_follows_directory_moved_after_binding_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    displaced = tmp_path / "displaced-candidate"
    original_assert = store._assert_candidate_creation_binding
    calls = 0
    move_blocked = False

    def try_move_after_binding_check(**kwargs: object) -> None:
        nonlocal calls, move_blocked
        original_assert(**kwargs)  # type: ignore[arg-type]
        calls += 1
        if calls != 2:
            return
        candidate_name = str(kwargs["candidate_name"])
        try:
            os.rename(store.candidates_root / candidate_name, displaced)
        except PermissionError:
            move_blocked = True

    monkeypatch.setattr(
        store,
        "_assert_candidate_creation_binding",
        try_move_after_binding_check,
    )

    candidate = store.prepare_candidate(**_prepare_arguments())

    assert calls >= 2
    assert move_blocked is True
    assert not displaced.exists()
    assert candidate.path.is_dir()


@pytest.mark.skipif(sys.platform != "darwin", reason="Linux fallback simulated on Darwin")
def test_linux_candidate_namespace_fallback_blocks_rename_and_restores_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    original_assert = store._assert_candidate_creation_binding
    move_blocked = False

    def try_move_while_guarded(**kwargs: object) -> None:
        nonlocal move_blocked
        original_assert(**kwargs)  # type: ignore[arg-type]
        if kwargs.get("candidates_permissions") != 0o500:
            return
        candidate_name = str(kwargs["candidate_name"])
        try:
            os.rename(store.candidates_root / candidate_name, tmp_path / "linux-displaced")
        except PermissionError:
            move_blocked = True

    monkeypatch.setattr(
        store,
        "_assert_candidate_creation_binding",
        try_move_while_guarded,
    )
    monkeypatch.setattr(store, "_namespace_guard_platform", lambda: "linux")

    candidate = store.prepare_candidate(**_prepare_arguments())

    assert move_blocked is True
    assert stat.S_IMODE(store.candidates_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(candidate.path.stat().st_mode) == 0o700
    assert stat.S_IMODE((candidate.path / "tables").stat().st_mode) == 0o700


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"schema_version": 999}, "evidence"),
        ({"indexed_at": datetime(2026, 7, 25, 9)}, "evidence"),
    ],
)
def test_zip_revalidates_forged_evidence_before_creating_destination_parent(
    tmp_path: Path,
    update: dict[str, object],
    message: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    valid = _evidence(sealed)
    forged = LabArtifactIndexEvidence.model_construct(**(valid.model_dump(mode="python") | update))
    destination = tmp_path / "must-not-exist" / "nested" / "bundle.zip"

    with pytest.raises(LabArtifactAuthorizationError, match=message):
        store.export_deterministic_zip(sealed.path, forged, destination)

    assert not destination.parent.exists()


def test_zip_authorizes_valid_but_wrong_evidence_before_destination_side_effects(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    wrong = _evidence(sealed).model_copy(update={"job_id": uuid4()})
    destination = tmp_path / "must-not-exist-valid-evidence" / "nested" / "bundle.zip"

    with pytest.raises(LabArtifactAuthorizationError, match="does not authorize"):
        store.export_deterministic_zip(sealed.path, wrong, destination)

    assert not destination.parent.exists()


@pytest.mark.parametrize("operation", ["verify", "prepare"])
def test_candidate_public_return_rechecks_last_moment_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    prepared = _prepare(store) if operation == "verify" else None
    displaced = tmp_path / f"candidate-original-{operation}"
    invoked = False

    def swap_candidate(candidate: LabJobArtifactCandidate) -> None:
        nonlocal invoked
        invoked = True
        replacement = tmp_path / f"candidate-copy-{operation}"
        shutil.copytree(candidate.path, replacement, copy_function=shutil.copy2)
        os.rename(candidate.path, displaced)
        os.rename(replacement, candidate.path)

    monkeypatch.setattr(
        store,
        "_after_public_candidate_finalized",
        swap_candidate,
        raising=False,
    )

    with pytest.raises(LabArtifactIntegrityError, match="candidate.*identity|bundle.*changed"):
        if operation == "verify":
            assert prepared is not None
            store.verify_candidate(prepared)
        else:
            store.prepare_candidate(**_prepare_arguments())

    assert invoked is True


@pytest.mark.parametrize("entry_kind", ["regular", "symlink"])
def test_invalid_candidate_entry_can_be_bound_and_quarantined(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    entry = store.candidates_root / f"invalid-{entry_kind}"
    external = tmp_path / "external-source.txt"
    external.write_text("external remains unchanged", encoding="utf-8")
    if entry_kind == "regular":
        entry.write_text("invalid candidate", encoding="utf-8")
        os.chmod(entry, 0o600)
    else:
        entry.symlink_to(external)
    observed = entry.lstat()

    record = next(item for item in store.list_candidate_recovery() if item.path == entry)

    assert record.status == "invalid"
    assert record.device == observed.st_dev
    assert record.inode == observed.st_ino
    assert record.file_type == entry_kind
    quarantined = store.quarantine_recovery_record(record, reason="invalid namespace entry")
    assert quarantined.status == "quarantined"
    assert quarantined.device == observed.st_dev
    assert quarantined.inode == observed.st_ino
    assert quarantined.file_type == entry_kind
    assert not entry.exists() and not entry.is_symlink()
    assert external.read_text(encoding="utf-8") == "external remains unchanged"
    if entry_kind == "symlink":
        assert quarantined.path.is_symlink()
        assert quarantined.path.readlink() == external
    else:
        assert quarantined.path.read_text(encoding="utf-8") == "invalid candidate"


def test_legacy_logical_run_normalization_is_shared_by_import_and_get(tmp_path: Path) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"stable":true}', encoding="utf-8")
    index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")

    imported = index.import_file(logical_run_id="  run   one  ", source_path=source)

    assert imported.record.logical_run_id == "run one"
    assert index.get("  run   one  ") == imported.record
    assert index.get("run one") == imported.record


def test_namespace_guard_intent_is_canonical_and_archived_after_prepare(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")

    candidate = _prepare(store)

    assert list(store.namespace_guard_active_root.iterdir()) == []
    history = list(store.namespace_guard_history_root.glob("*.json"))
    assert len(history) == 1
    payload = history[0].read_bytes()
    intent = lab_artifacts_module.LabCandidateNamespaceGuardIntent.model_validate_json(payload)
    assert payload == intent.canonical_json_bytes()
    assert intent.candidate_name == candidate.path.name
    assert intent.phase == "armed"
    assert stat.S_IMODE(store.candidates_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(candidate.path.stat().st_mode) == 0o700
    assert stat.S_IMODE((candidate.path / "tables").stat().st_mode) == 0o700
    if sys.platform == "darwin":
        assert candidate.path.stat().st_flags & stat.UF_IMMUTABLE == 0
        assert (candidate.path / "tables").stat().st_flags & stat.UF_IMMUTABLE == 0


@pytest.mark.parametrize(
    "drift_stage",
    ["namespace_intent", "payload_completion", "intent_archive", "final_return"],
)
def test_candidate_runtime_drift_never_publishes_recoverable_complete_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift_stage: str,
) -> None:
    drifted = False

    def mutation_guard() -> str:
        if drifted:
            raise RuntimeError(f"runtime drifted at {drift_stage}")
        return "1" * 40

    root = tmp_path / "artifacts"
    store = LabJobArtifactStore(root, mutation_guard=mutation_guard)
    if drift_stage == "namespace_intent":
        original = store._publish_namespace_guard_intent

        def drift_before_intent(intent: object) -> None:
            nonlocal drifted
            drifted = True
            original(intent)  # type: ignore[arg-type]

        monkeypatch.setattr(store, "_publish_namespace_guard_intent", drift_before_intent)
    elif drift_stage == "payload_completion":

        def drift_before_payload(_intent: object) -> None:
            nonlocal drifted
            drifted = True

        monkeypatch.setattr(store, "_after_candidate_namespace_guarded", drift_before_payload)
    elif drift_stage == "intent_archive":
        original = store._archive_namespace_guard_intent

        def drift_before_archive(intent: object, **kwargs: object) -> None:
            nonlocal drifted
            drifted = True
            original(intent, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(store, "_archive_namespace_guard_intent", drift_before_archive)
    else:

        def drift_before_return(_candidate: object) -> None:
            nonlocal drifted
            drifted = True

        monkeypatch.setattr(
            store,
            "_before_complete_candidate_return",
            drift_before_return,
            raising=False,
        )

    with pytest.raises(BaseException) as raised:
        _prepare(store)
    assert "runtime drifted" in repr(raised.value)

    candidate_paths = tuple(store.candidates_root.iterdir())
    assert len(candidate_paths) == 1
    drifted = False
    monkeypatch.undo()
    store.close()
    restarted = LabJobArtifactStore(root, mutation_guard=mutation_guard)
    recovery = next(
        record
        for record in restarted.list_candidate_recovery()
        if record.path == candidate_paths[0]
    )
    assert recovery.status == "invalid"
    restarted.close()


def test_namespace_guard_intent_is_durable_before_namespace_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    activate = store._activate_candidate_namespace_guard

    def inspect_then_activate(
        intent: lab_artifacts_module.LabCandidateNamespaceGuardIntent,
        **descriptors: int,
    ) -> None:
        active = store.namespace_guard_active_root / f"{intent.candidate_name}.json"
        payload = active.read_bytes()
        assert payload == intent.canonical_json_bytes()
        assert active.stat().st_size == len(payload)
        activate(intent, **descriptors)

    monkeypatch.setattr(
        store,
        "_activate_candidate_namespace_guard",
        inspect_then_activate,
    )

    assert _prepare(store).path.is_dir()


def test_namespace_guard_publish_failure_quarantines_complete_temp_on_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    store = LabJobArtifactStore(root)

    def fail_publish(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected guard publish failure")

    monkeypatch.setattr(lab_artifacts_module, "_rename_noreplace", fail_publish)

    with pytest.raises(OSError, match="injected guard publish failure"):
        _prepare(store)

    temporary = list(store.namespace_guard_active_root.glob(".*.tmp"))
    assert len(temporary) == 1
    intent = lab_artifacts_module.LabCandidateNamespaceGuardIntent.model_validate_json(
        temporary[0].read_bytes()
    )
    assert temporary[0].read_bytes() == intent.canonical_json_bytes()

    monkeypatch.undo()
    store.close()
    restarted = LabJobArtifactStore(root)
    assert list(restarted.namespace_guard_active_root.iterdir()) == []
    assert len(list(restarted.namespace_guard_quarantine_root.iterdir())) == 1
    restarted.close()


@pytest.mark.parametrize("guard_platform", ["darwin", "linux"])
def test_process_crash_namespace_guard_is_recovered_before_exact_mode_validation(
    tmp_path: Path,
    guard_platform: str,
) -> None:
    if guard_platform == "darwin" and sys.platform != "darwin":
        pytest.skip("Darwin immutable inode flags require a Darwin filesystem")
    root = tmp_path / f"artifacts-{guard_platform}"
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LabJobArtifactStore
        from tests.unit.test_lab_artifacts import _prepare_arguments

        store = LabJobArtifactStore(Path(sys.argv[1]))
        store._namespace_guard_platform = lambda: sys.argv[2]
        store._after_candidate_namespace_guarded = lambda _intent: os._exit(91)
        store.prepare_candidate(**_prepare_arguments())
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script, str(root), guard_platform],
        check=False,
        cwd=Path(__file__).parents[2],
        env=os.environ.copy(),
    )

    assert completed.returncode == 91
    active = list((root / "namespace-guard-active").glob("*.json"))
    assert len(active) == 1
    candidate_path = next((root / "candidates").iterdir())
    if guard_platform == "darwin":
        assert candidate_path.stat().st_flags & stat.UF_IMMUTABLE
        assert (candidate_path / "tables").stat().st_flags & stat.UF_IMMUTABLE
    else:
        assert stat.S_IMODE((root / "candidates").stat().st_mode) == 0o500
        assert stat.S_IMODE(candidate_path.stat().st_mode) == 0o500
        assert stat.S_IMODE((candidate_path / "tables").stat().st_mode) == 0o500

    restarted = LabJobArtifactStore(root)

    assert stat.S_IMODE((root / "candidates").stat().st_mode) == 0o700
    assert stat.S_IMODE(candidate_path.stat().st_mode) == 0o700
    assert stat.S_IMODE((candidate_path / "tables").stat().st_mode) == 0o700
    if guard_platform == "darwin":
        assert candidate_path.stat().st_flags & stat.UF_IMMUTABLE == 0
        assert (candidate_path / "tables").stat().st_flags & stat.UF_IMMUTABLE == 0
    assert list(restarted.namespace_guard_active_root.iterdir()) == []
    assert list(restarted.namespace_guard_history_root.glob("*.json"))
    recovery = next(
        item for item in restarted.list_candidate_recovery() if item.path == candidate_path
    )
    assert recovery.status == "invalid"
    quarantined = restarted.quarantine_recovery_record(
        recovery,
        reason="crashed candidate",
    )
    assert quarantined.status == "quarantined"
    assert _prepare(restarted, job_id=uuid4()).path.is_dir()


def test_namespace_guard_cleanup_failure_poison_store_and_preserves_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")

    def fail_restore(*_args: object, **_kwargs: object) -> None:
        raise OSError("restore failed")

    monkeypatch.setattr(
        store,
        "_restore_candidate_namespace_guard",
        fail_restore,
        raising=False,
    )

    with pytest.raises(BaseException, match="restore failed|namespace guard"):
        _prepare(store)

    assert store.poisoned is True
    assert list(store.namespace_guard_active_root.glob("*.json"))
    with pytest.raises(LabArtifactIntegrityError, match="poisoned"):
        _prepare(store, job_id=uuid4())

    monkeypatch.undo()
    store.close()
    recovered = LabJobArtifactStore(store.root)
    assert list(recovered.namespace_guard_active_root.iterdir()) == []
    recovered.close()


def test_poisoned_store_rejects_all_public_artifact_operations_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    verify_candidate = _prepare(store, job_id=uuid4())
    seal_candidate = _prepare(store, job_id=uuid4())
    quarantine_candidate = _prepare(store, job_id=uuid4())
    recovery_candidate = _prepare(store, job_id=uuid4())
    recovery_record = next(
        record
        for record in store.list_candidate_recovery()
        if record.path == recovery_candidate.path
    )
    recovery_authority = _recovery_authority(recovery_candidate)
    sealed = store.seal_candidate(_prepare(store, job_id=uuid4()))
    evidence = _evidence(sealed)
    destination = tmp_path / "poisoned-export" / "bundle.zip"

    def fail_restore(*_args: object, **_kwargs: object) -> None:
        raise OSError("restore failed")

    monkeypatch.setattr(store, "_restore_candidate_namespace_guard", fail_restore)
    with pytest.raises(BaseException, match="restore failed|namespace guard"):
        _prepare(store, job_id=uuid4())
    assert store.poisoned is True
    active = next(store.namespace_guard_active_root.glob("*.json"))
    active_bytes = active.read_bytes()

    def bind_for_index() -> None:
        with store.bind_verified_sealed(
            sealed.path,
            indexed_at=datetime(2026, 7, 26, 9, tzinfo=UTC),
        ):
            pass

    operations: tuple[tuple[str, Callable[[], object]], ...] = (
        ("verify candidate", lambda: store.verify_candidate(verify_candidate)),
        ("verify sealed", lambda: store.verify_sealed(sealed.path)),
        ("bind for index", bind_for_index),
        ("list recovery", store.list_candidate_recovery),
        ("seal", lambda: store.seal_candidate(seal_candidate)),
        (
            "quarantine candidate",
            lambda: store.quarantine_candidate(quarantine_candidate, reason="poisoned"),
        ),
        (
            "quarantine recovery",
            lambda: store.quarantine_recovery_record(recovery_record, reason="poisoned"),
        ),
        (
            "recover candidate",
            lambda: store.recover_candidate(
                recovery_record,
                authority=recovery_authority,
            ),
        ),
        (
            "recover interrupted",
            lambda: store.recover_interrupted_seal(sealed.path),
        ),
        (
            "export",
            lambda: store.export_deterministic_zip(sealed.path, evidence, destination),
        ),
    )
    before = tuple(
        sorted(
            (
                path.relative_to(store.root).as_posix(),
                path.lstat().st_dev,
                path.lstat().st_ino,
                path.lstat().st_mode,
                path.lstat().st_size,
                path.lstat().st_mtime_ns,
                path.lstat().st_ctime_ns,
                path.read_bytes() if path.is_file() else b"",
            )
            for path in store.root.rglob("*")
        )
    )
    try:
        for _label, operation in operations:
            with pytest.raises(LabArtifactIntegrityError, match="poisoned"):
                operation()

        after = tuple(
            sorted(
                (
                    path.relative_to(store.root).as_posix(),
                    path.lstat().st_dev,
                    path.lstat().st_ino,
                    path.lstat().st_mode,
                    path.lstat().st_size,
                    path.lstat().st_mtime_ns,
                    path.lstat().st_ctime_ns,
                    path.read_bytes() if path.is_file() else b"",
                )
                for path in store.root.rglob("*")
            )
        )
        assert after == before
        assert active.read_bytes() == active_bytes
        assert not destination.parent.exists()
    finally:
        monkeypatch.undo()
        store.close()
        recovered = LabJobArtifactStore(store.root)
        recovered.close()


def test_inflight_export_completes_before_concurrent_poison_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    sealed = store.seal_candidate(_prepare(store))
    evidence = _evidence(sealed)
    destination = tmp_path / "concurrent-export" / "bundle.zip"
    authorized = threading.Event()
    release_export = threading.Event()
    poison_started = threading.Event()
    poison_finished = threading.Event()
    export_errors: list[BaseException] = []
    poison_errors: list[BaseException] = []
    authorize = store._authorize_export

    def pause_after_authorization(
        verified: LabSealedJobArtifact,
        supplied: LabArtifactIndexEvidence,
    ) -> None:
        authorize(verified, supplied)
        authorized.set()
        if not release_export.wait(timeout=10):
            raise TimeoutError("export was not released")

    def fail_restore(*_args: object, **_kwargs: object) -> None:
        raise OSError("restore failed")

    def run_export() -> None:
        try:
            store.export_deterministic_zip(sealed.path, evidence, destination)
        except BaseException as exc:
            export_errors.append(exc)

    def poison_store() -> None:
        poison_started.set()
        try:
            _prepare(store, job_id=uuid4())
        except BaseException as exc:
            poison_errors.append(exc)
        finally:
            poison_finished.set()

    monkeypatch.setattr(store, "_authorize_export", pause_after_authorization)
    monkeypatch.setattr(store, "_restore_candidate_namespace_guard", fail_restore)
    export_thread = threading.Thread(target=run_export)
    poison_thread = threading.Thread(target=poison_store)
    export_thread.start()
    assert authorized.wait(timeout=10)
    poison_thread.start()
    assert poison_started.wait(timeout=10)
    try:
        time.sleep(0.25)
        assert poison_finished.is_set() is False
        assert store.poisoned is False
    finally:
        release_export.set()
        export_thread.join(timeout=10)
        poison_thread.join(timeout=10)

    assert not export_thread.is_alive() and not poison_thread.is_alive()
    assert export_errors == []
    assert len(poison_errors) == 1
    assert destination.is_file()
    assert store.poisoned is True
    assert len(list(store.namespace_guard_active_root.glob("*.json"))) == 1

    monkeypatch.undo()
    store.close()
    recovered = LabJobArtifactStore(store.root)
    recovered.close()


def test_poison_is_shared_until_a_new_store_recovers_the_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    poisoned_store = LabJobArtifactStore(root)
    peer_store = LabJobArtifactStore(root)

    def fail_restore(*_args: object, **_kwargs: object) -> None:
        raise OSError("restore failed")

    monkeypatch.setattr(
        poisoned_store,
        "_restore_candidate_namespace_guard",
        fail_restore,
    )
    with pytest.raises(BaseException, match="restore failed|namespace guard"):
        _prepare(poisoned_store)

    assert poisoned_store.poisoned is True
    assert peer_store.poisoned is True
    with pytest.raises(LabArtifactIntegrityError, match="poisoned"):
        peer_store.list_candidate_recovery()

    monkeypatch.undo()
    recovered_store = LabJobArtifactStore(root)

    assert poisoned_store.poisoned is True
    assert peer_store.poisoned is False
    assert recovered_store.poisoned is False
    assert peer_store.list_candidate_recovery()

    poisoned_store.close()
    peer_store.close()
    recovered_store.close()


@pytest.mark.parametrize("use_second_store", [False, True])
def test_prepare_reentry_during_serialization_is_rejected_before_inner_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    use_second_store: bool,
) -> None:
    root = tmp_path / "artifacts"
    outer_store = LabJobArtifactStore(root)
    inner_store = LabJobArtifactStore(root) if use_second_store else outer_store
    serialize = outer_store._serialize_parquet
    attempted = False

    def attempt_reentry_then_serialize(
        table_name: str,
        frame: pd.DataFrame,
    ) -> tuple[bytes, LabJobArtifactFile]:
        nonlocal attempted
        if not attempted:
            attempted = True
            with pytest.raises(LabArtifactIntegrityError, match="reentrant"):
                _prepare(inner_store, job_id=uuid4())
        return serialize(table_name, frame)

    monkeypatch.setattr(outer_store, "_serialize_parquet", attempt_reentry_then_serialize)

    candidate = _prepare(outer_store)

    assert attempted is True
    assert outer_store.poisoned is False
    assert [path.name for path in outer_store.candidates_root.iterdir()] == [candidate.path.name]


def test_same_thread_reentrant_prepare_fails_without_recovering_outer_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    reentry_checked = False

    def attempt_reentry(
        intent: lab_artifacts_module.LabCandidateNamespaceGuardIntent,
    ) -> None:
        nonlocal reentry_checked
        active = store.namespace_guard_active_root / f"{intent.candidate_name}.json"
        active_bytes = active.read_bytes()
        with pytest.raises(LabArtifactIntegrityError, match="reentrant"):
            _prepare(store, job_id=uuid4())
        assert active.read_bytes() == active_bytes
        assert list(store.namespace_guard_history_root.iterdir()) == []
        reentry_checked = True

    monkeypatch.setattr(store, "_after_candidate_namespace_guarded", attempt_reentry)

    candidate = _prepare(store)

    assert reentry_checked is True
    assert store.poisoned is False
    assert list(store.namespace_guard_active_root.iterdir()) == []
    assert len(list(store.namespace_guard_history_root.glob("*.json"))) == 1
    assert [path.name for path in store.candidates_root.iterdir()] == [candidate.path.name]


def test_namespace_guard_lock_path_swap_fails_before_candidate_side_effect(
    tmp_path: Path,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    lock_path = store.root / "namespace-guard.lock"
    displaced = tmp_path / "original-namespace-guard.lock"
    os.rename(lock_path, displaced)
    lock_path.write_bytes(b"")
    os.chmod(lock_path, 0o600)
    before = tuple(store.candidates_root.iterdir())

    with pytest.raises(LabArtifactIntegrityError, match="guard lock.*identity|lock.*changed"):
        _prepare(store)

    assert tuple(store.candidates_root.iterdir()) == before


def test_namespace_guard_recovery_identity_mismatch_preserves_active_evidence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LabJobArtifactStore
        from tests.unit.test_lab_artifacts import _prepare_arguments

        store = LabJobArtifactStore(Path(sys.argv[1]))
        store._namespace_guard_platform = lambda: "linux"
        store._after_candidate_namespace_guarded = lambda _intent: os._exit(92)
        store.prepare_candidate(**_prepare_arguments())
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(root)],
        check=False,
        cwd=Path(__file__).parents[2],
        env=os.environ.copy(),
    )
    assert completed.returncode == 92
    active = next((root / "namespace-guard-active").glob("*.json"))
    active_before = active.read_bytes()
    candidate = next((root / "candidates").iterdir())
    displaced = tmp_path / "guarded-original-candidate"
    os.chmod(root / "candidates", 0o700)
    os.chmod(candidate, 0o700)
    os.chmod(candidate / "tables", 0o700)
    os.rename(candidate, displaced)
    shutil.copytree(displaced, candidate, copy_function=shutil.copy2)

    with pytest.raises(LabArtifactIntegrityError, match="namespace guard.*identity"):
        LabJobArtifactStore(root)

    assert active.read_bytes() == active_before
    assert active.exists()


def test_namespace_guard_serializes_prepare_across_store_instances(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    first = LabJobArtifactStore(root)
    second = LabJobArtifactStore(root)
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()
    errors: list[BaseException] = []

    def hold_first(_intent: object) -> None:
        first_entered.set()
        if not release_first.wait(timeout=10):
            raise TimeoutError("first prepare was not released")

    monkeypatch.setattr(first, "_after_candidate_namespace_guarded", hold_first, raising=False)
    monkeypatch.setattr(
        second,
        "_after_candidate_namespace_guarded",
        lambda _intent: second_entered.set(),
        raising=False,
    )

    def run(store: LabJobArtifactStore, job_id: UUID) -> None:
        try:
            _prepare(store, job_id=job_id)
        except BaseException as exc:
            errors.append(exc)

    thread_a = threading.Thread(target=run, args=(first, uuid4()))
    thread_b = threading.Thread(target=run, args=(second, uuid4()))
    thread_a.start()
    assert first_entered.wait(timeout=10)
    thread_b.start()
    time.sleep(0.25)
    assert second_entered.is_set() is False
    release_first.set()
    thread_a.join(timeout=10)
    thread_b.join(timeout=10)

    assert not thread_a.is_alive() and not thread_b.is_alive()
    assert errors == []
    assert second_entered.is_set() is True


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_artifact_store_constructor_base_exception_rolls_back_all_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    registry_before = dict(lab_artifacts_module._ARTIFACT_PROCESS_LOCKS)
    failed = LabJobArtifactStore.__new__(LabJobArtifactStore)

    def interrupt_recovery(_store: LabJobArtifactStore) -> None:
        raise failure_type("constructor recovery interrupted")

    monkeypatch.setattr(
        LabJobArtifactStore,
        "_recover_active_namespace_guards",
        interrupt_recovery,
    )

    with pytest.raises(failure_type, match="recovery interrupted"):
        failed.__init__(tmp_path / "artifacts")

    assert failed._closed is True
    assert failed._process_lock is None
    assert failed._process_lock_entry is None
    assert dict(lab_artifacts_module._ARTIFACT_PROCESS_LOCKS) == registry_before
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_artifact_store_constructor_preserves_primary_and_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gc.collect()
    before_descriptors = len(os.listdir("/dev/fd"))
    registry_before = dict(lab_artifacts_module._ARTIFACT_PROCESS_LOCKS)
    failed = LabJobArtifactStore.__new__(LabJobArtifactStore)
    original_close = lab_artifacts_module.os.close
    close_failed = False

    def fail_recovery(_store: LabJobArtifactStore) -> None:
        raise RuntimeError("constructor recovery failure")

    def close_then_fail(descriptor: int) -> None:
        nonlocal close_failed
        should_fail = descriptor == failed._guard_lock_descriptor and not close_failed
        original_close(descriptor)
        if should_fail:
            close_failed = True
            raise OSError("constructor close failure")

    monkeypatch.setattr(
        LabJobArtifactStore,
        "_recover_active_namespace_guards",
        fail_recovery,
    )
    monkeypatch.setattr(lab_artifacts_module.os, "close", close_then_fail)

    with pytest.raises(BaseExceptionGroup) as captured:
        failed.__init__(tmp_path / "artifacts")

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(isinstance(item, OSError) and "constructor close" in str(item) for item in flattened)
    assert close_failed is True
    assert failed._closed is True
    assert failed._process_lock is None
    assert failed._process_lock_entry is None
    assert dict(lab_artifacts_module._ARTIFACT_PROCESS_LOCKS) == registry_before
    monkeypatch.setattr(lab_artifacts_module.os, "close", original_close)
    gc.collect()
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_store_close_waits_for_inflight_namespace_guard_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    guarded = threading.Event()
    release = threading.Event()
    close_finished = threading.Event()
    errors: list[BaseException] = []

    def hold_prepare(_intent: object) -> None:
        guarded.set()
        if not release.wait(timeout=10):
            raise TimeoutError("prepare was not released")

    monkeypatch.setattr(store, "_after_candidate_namespace_guarded", hold_prepare)

    def prepare() -> None:
        try:
            _prepare(store)
        except BaseException as exc:
            errors.append(exc)

    def close() -> None:
        try:
            store.close()
            close_finished.set()
        except BaseException as exc:
            errors.append(exc)

    prepare_thread = threading.Thread(target=prepare)
    close_thread = threading.Thread(target=close)
    prepare_thread.start()
    assert guarded.wait(timeout=10)
    close_thread.start()
    time.sleep(0.25)
    assert close_finished.is_set() is False
    release.set()
    prepare_thread.join(timeout=10)
    close_thread.join(timeout=10)

    assert not prepare_thread.is_alive() and not close_thread.is_alive()
    assert close_finished.is_set() is True
    assert errors == []
    assert store._closed is True
    assert store._process_lock is None
    assert store._process_lock_entry is None


def test_closed_store_rejects_use_without_polluting_shared_lifecycle(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    closed_store = LabJobArtifactStore(root)
    peer_store = LabJobArtifactStore(root)
    shared_entry = closed_store._process_lock_entry
    assert shared_entry is not None
    assert peer_store._process_lock_entry is shared_entry

    closed_store.close()

    assert closed_store._closed is True
    assert closed_store._process_lock is None
    assert closed_store._process_lock_entry is None
    assert shared_entry.lifecycle_owner_thread_id is None
    assert shared_entry.prepare_owner_thread_id is None
    assert shared_entry.lifecycle_depth == 0
    before = tuple(
        sorted(
            (
                path.relative_to(root).as_posix(),
                path.lstat().st_dev,
                path.lstat().st_ino,
                path.lstat().st_mode,
                path.lstat().st_size,
                path.lstat().st_mtime_ns,
            )
            for path in root.rglob("*")
        )
    )

    with pytest.raises(LabArtifactIntegrityError, match="closed"):
        _prepare(closed_store, job_id=uuid4())

    after = tuple(
        sorted(
            (
                path.relative_to(root).as_posix(),
                path.lstat().st_dev,
                path.lstat().st_ino,
                path.lstat().st_mode,
                path.lstat().st_size,
                path.lstat().st_mtime_ns,
            )
            for path in root.rglob("*")
        )
    )
    assert after == before
    candidate = _prepare(peer_store, job_id=uuid4())
    assert candidate.path.is_dir()
    assert shared_entry.lifecycle_owner_thread_id is None
    assert shared_entry.prepare_owner_thread_id is None
    assert shared_entry.lifecycle_depth == 0
    closed_store.close()
    peer_store.close()


@pytest.mark.parametrize("failure_point", ["identity", "flock", "post_flock_identity"])
def test_artifact_lifecycle_pre_yield_failure_rolls_back_shared_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    entry = store._process_lock_entry
    assert entry is not None
    original_identity = store._assert_namespace_guard_lock_identity
    original_flock = lab_artifacts_module.fcntl.flock
    identity_calls = 0

    def fail_selected_identity() -> None:
        nonlocal identity_calls
        identity_calls += 1
        if failure_point == "identity" and identity_calls == 1:
            raise OSError("pre-yield identity failure")
        if failure_point == "post_flock_identity" and identity_calls == 2:
            raise OSError("post-flock identity failure")
        original_identity()

    def fail_selected_flock(descriptor: int, operation: int) -> None:
        if failure_point == "flock" and operation & fcntl.LOCK_EX:
            raise OSError("pre-yield flock failure")
        original_flock(descriptor, operation)

    monkeypatch.setattr(store, "_assert_namespace_guard_lock_identity", fail_selected_identity)
    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", fail_selected_flock)

    with pytest.raises(OSError, match="pre-yield|post-flock"):
        store.list_candidate_recovery()

    assert store._operation_depth == 0
    assert entry.lifecycle_owner_thread_id is None
    assert entry.prepare_owner_thread_id is None
    assert entry.lifecycle_depth == 0
    monkeypatch.undo()
    assert store.list_candidate_recovery() == ()
    store.close()


def test_artifact_lifecycle_preserves_caller_and_unlock_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    entry = store._process_lock_entry
    assert entry is not None
    lock_path = store.root / "namespace-guard.lock"
    probe_descriptor = os.open(lock_path, os.O_RDWR)
    before_descriptors = len(os.listdir("/dev/fd"))
    original_flock = lab_artifacts_module.fcntl.flock
    unlock_failed = False

    def unlock_then_fail(descriptor: int, operation: int) -> None:
        nonlocal unlock_failed
        original_flock(descriptor, operation)
        if operation == fcntl.LOCK_UN and not unlock_failed:
            unlock_failed = True
            raise OSError("runtime artifact unlock failure")

    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", unlock_then_fail)

    with (
        pytest.raises(BaseExceptionGroup) as captured,
        store._artifact_operation_lifecycle(prepare=False),
    ):
        raise RuntimeError("runtime artifact caller failure")

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(
        isinstance(item, OSError) and "runtime artifact unlock" in str(item) for item in flattened
    )
    assert unlock_failed is True
    assert store._operation_depth == 0
    assert entry.lifecycle_owner_thread_id is None
    assert entry.prepare_owner_thread_id is None
    assert entry.lifecycle_depth == 0
    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", original_flock)
    original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    original_flock(probe_descriptor, fcntl.LOCK_UN)
    assert len(os.listdir("/dev/fd")) == before_descriptors
    os.close(probe_descriptor)
    store.close()


def _child_exit_detail(process: subprocess.Popen[bytes], label: str, log: Path) -> str:
    """Say what a competitor's exit code was and show what it printed.

    Both children inherit the parent's stdout and stderr by default, so a child that
    dies of a signal contributes one orphan line to pytest's capture of the *parent*
    with nothing naming it. A `-6` on its own is not a diagnosis.
    """

    code = process.returncode
    detail = f"{label} exited with {code}"
    if code is not None and code < 0:
        with suppress(ValueError):
            detail += f" ({signal.Signals(-code).name})"
    captured = ""
    with suppress(OSError):
        captured = log.read_text(encoding="utf-8", errors="replace").strip()
    return detail + (f"\n{label} output:\n{captured}" if captured else f"\n{label} printed nothing")


def test_namespace_guard_serializes_prepare_across_processes(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    first_marker = tmp_path / "first-entered"
    second_marker = tmp_path / "second-entered"
    release = tmp_path / "release-first"
    script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path
        from uuid import UUID
        from rquant.lab_artifacts import LabJobArtifactStore
        from tests.unit.test_lab_artifacts import _prepare_arguments

        root, marker, release, job_id, should_wait = map(Path, sys.argv[1:6])
        # Written before the store exists. It is what separates "has not finished
        # importing yet" from "has booted and the guard is holding it", which is the
        # claim the assertion after the second spawn wants to make.
        marker.with_suffix(".booted").write_text("booted", encoding="utf-8")
        store = LabJobArtifactStore(root)
        def guarded(_intent):
            marker.write_text("entered", encoding="utf-8")
            if should_wait.name == "yes":
                deadline = time.monotonic() + 15
                while not release.exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("release marker missing")
                    time.sleep(0.02)
        store._after_candidate_namespace_guarded = guarded
        arguments = _prepare_arguments()
        arguments["job_id"] = UUID(job_id.name)
        store.prepare_candidate(**arguments)
        """
    )
    env = os.environ.copy()
    # A fatal signal in a child prints a Python traceback rather than dying mute.
    env["PYTHONFAULTHANDLER"] = "1"
    first_log = tmp_path / "first.log"
    second_log = tmp_path / "second.log"

    def spawn(marker: Path, log: Path, should_wait: str) -> subprocess.Popen[bytes]:
        # Each child's output goes to its own file instead of being inherited, so a
        # child that dies is reported with what it printed rather than leaving one
        # unattributed line in the parent's capture.
        handle = log.open("wb")
        try:
            return subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    script,
                    str(root),
                    str(marker),
                    str(release),
                    str(uuid4()),
                    should_wait,
                ],
                cwd=Path(__file__).parents[2],
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
        finally:
            handle.close()

    process_a = spawn(first_marker, first_log, "yes")
    deadline = time.monotonic() + 10
    while not first_marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert first_marker.exists(), _child_exit_detail(process_a, "first competitor", first_log)

    process_b = spawn(second_marker, second_log, "no")
    # Anchor the "it did not get in" claim to the second competitor having booted.
    # Its spawn and imports take 0.3-0.6s on a developer machine and longer on a
    # loaded runner, so a bare sleep of 0.3s is thin: it discriminates here, but on
    # a slow enough host it would be observing an interpreter that has not started
    # rather than a guard that is holding one.
    second_booted = second_marker.with_suffix(".booted")
    deadline = time.monotonic() + 30
    while not second_booted.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert second_booted.exists(), _child_exit_detail(process_b, "second competitor", second_log)
    time.sleep(0.3)
    assert second_marker.exists() is False
    release.write_text("go", encoding="utf-8")

    assert process_a.wait(timeout=15) == 0, _child_exit_detail(
        process_a, "first competitor", first_log
    )
    assert process_b.wait(timeout=15) == 0, _child_exit_detail(
        process_b, "second competitor", second_log
    )
    assert second_marker.exists() is True


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="real 0500 namespace fallback is Linux-specific",
)
def test_linux_namespace_guard_blocks_rename_and_recovers_after_process_crash(
    tmp_path: Path,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root bypasses directory write permissions")
    root = tmp_path / "artifacts"
    marker = tmp_path / "linux-guard.json"
    escaped = tmp_path / "escaped-candidate"
    script = textwrap.dedent(
        """
        import json
        import os
        import stat
        import sys
        from pathlib import Path
        from rquant.lab_artifacts import LabJobArtifactStore
        from tests.unit.test_lab_artifacts import _prepare_arguments

        root, marker, escaped = map(Path, sys.argv[1:4])
        store = LabJobArtifactStore(root)
        def crash_while_guarded(intent):
            candidate = store.candidates_root / intent.candidate_name
            tables = candidate / "tables"
            modes = [
                stat.S_IMODE(path.stat().st_mode)
                for path in (store.candidates_root, candidate, tables)
            ]
            blocked = False
            try:
                os.rename(candidate, escaped)
            except PermissionError:
                blocked = True
            marker.write_text(
                json.dumps(
                    {
                        "blocked": blocked,
                        "candidate_name": intent.candidate_name,
                        "modes": modes,
                    }
                ),
                encoding="utf-8",
            )
            os._exit(87)
        store._after_candidate_namespace_guarded = crash_while_guarded
        store.prepare_candidate(**_prepare_arguments())
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script, str(root), str(marker), str(escaped)],
        check=False,
        cwd=Path(__file__).parents[2],
        env=os.environ.copy(),
    )

    assert completed.returncode == 87
    observed = json.loads(marker.read_text(encoding="utf-8"))
    assert observed["blocked"] is True
    assert observed["modes"] == [0o500, 0o500, 0o500]
    candidate = root / "candidates" / observed["candidate_name"]
    assert escaped.exists() is False
    assert stat.S_IMODE((root / "candidates").stat().st_mode) == 0o500

    recovered = LabJobArtifactStore(root)

    assert stat.S_IMODE(recovered.candidates_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(candidate.stat().st_mode) == 0o700
    assert stat.S_IMODE((candidate / "tables").stat().st_mode) == 0o700
    assert list(recovered.namespace_guard_active_root.iterdir()) == []
    assert list(recovered.namespace_guard_history_root.glob("*.json"))


def test_durable_poison_blocks_export_from_an_already_open_process(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = LabJobArtifactStore(root)
    sealed = store.seal_candidate(_prepare(store))
    store.close()
    ready = tmp_path / "export-ready"
    release = tmp_path / "export-release"
    poison_complete = tmp_path / "poison-complete"
    export_result = tmp_path / "export-result"
    destination = tmp_path / "blocked-export" / "bundle.zip"
    exporter_script = textwrap.dedent(
        """
        import sys
        import time
        from datetime import UTC, datetime
        from pathlib import Path
        from rquant.lab_artifacts import LabArtifactIndexEvidence, LabJobArtifactStore

        root, sealed_path, ready, release, result, destination = map(Path, sys.argv[1:7])
        store = LabJobArtifactStore(root)
        sealed = store.verify_sealed(sealed_path)
        evidence = LabArtifactIndexEvidence(
            job_id=sealed.manifest.job_id,
            sealed_path=sealed.path,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            bundle_device=sealed.device,
            bundle_inode=sealed.inode,
            file_identities=sealed.file_identities,
            indexed_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
        )
        ready.write_text("ready", encoding="utf-8")
        deadline = time.monotonic() + 15
        while not release.exists():
            if time.monotonic() > deadline:
                raise TimeoutError("export release missing")
            time.sleep(0.02)
        try:
            store.export_deterministic_zip(sealed.path, evidence, destination)
        except BaseException as exc:
            result.write_text(f"blocked:{type(exc).__name__}:{exc}", encoding="utf-8")
        else:
            result.write_text("published", encoding="utf-8")
        """
    )
    poison_script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from uuid import uuid4
        from rquant.lab_artifacts import LabJobArtifactStore
        from tests.unit.test_lab_artifacts import _prepare_arguments

        root, marker = map(Path, sys.argv[1:3])
        store = LabJobArtifactStore(root)
        def fail_restore(*_args, **_kwargs):
            raise OSError("injected guard restore failure")
        store._restore_candidate_namespace_guard = fail_restore
        arguments = _prepare_arguments()
        arguments["job_id"] = uuid4()
        try:
            store.prepare_candidate(**arguments)
        except BaseException as exc:
            marker.write_text(f"{type(exc).__name__}:{exc}", encoding="utf-8")
        else:
            marker.write_text("unexpected-success", encoding="utf-8")
        """
    )
    environment = os.environ.copy()
    exporter = subprocess.Popen(
        [
            sys.executable,
            "-c",
            exporter_script,
            str(root),
            str(sealed.path),
            str(ready),
            str(release),
            str(export_result),
            str(destination),
        ],
        cwd=Path(__file__).parents[2],
        env=environment,
    )
    poisoner: subprocess.Popen[bytes] | None = None
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
        poisoner = subprocess.Popen(
            [sys.executable, "-c", poison_script, str(root), str(poison_complete)],
            cwd=Path(__file__).parents[2],
            env=environment,
        )
        assert poisoner.wait(timeout=15) == 0
        assert poison_complete.read_text(encoding="utf-8") != "unexpected-success"
        release.write_text("go", encoding="utf-8")
        assert exporter.wait(timeout=15) == 0
    finally:
        if exporter.poll() is None:
            exporter.kill()
            exporter.wait(timeout=5)
        if poisoner is not None and poisoner.poll() is None:
            poisoner.kill()
            poisoner.wait(timeout=5)

    assert export_result.read_text(encoding="utf-8").startswith("blocked:")
    assert destination.parent.exists() is False
    recovered = LabJobArtifactStore(root)
    recovered.close()


def test_cross_process_export_finishes_before_poison_transition(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = LabJobArtifactStore(root)
    sealed = store.seal_candidate(_prepare(store))
    store.close()
    export_entered = tmp_path / "export-entered"
    export_release = tmp_path / "export-release"
    export_result = tmp_path / "export-result"
    poison_started = tmp_path / "poison-started"
    poison_complete = tmp_path / "poison-complete"
    destination = tmp_path / "ordered-export" / "bundle.zip"
    exporter_script = textwrap.dedent(
        """
        import sys
        import time
        from datetime import UTC, datetime
        from pathlib import Path
        from rquant.lab_artifacts import LabArtifactIndexEvidence, LabJobArtifactStore

        root, sealed_path, entered, release, result, destination = map(Path, sys.argv[1:7])
        store = LabJobArtifactStore(root)
        sealed = store.verify_sealed(sealed_path)
        evidence = LabArtifactIndexEvidence(
            job_id=sealed.manifest.job_id,
            sealed_path=sealed.path,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            bundle_device=sealed.device,
            bundle_inode=sealed.inode,
            file_identities=sealed.file_identities,
            indexed_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
        )
        authorize = store._authorize_export
        def pause(verified, supplied):
            authorize(verified, supplied)
            entered.write_text("entered", encoding="utf-8")
            deadline = time.monotonic() + 15
            while not release.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("export release missing")
                time.sleep(0.02)
        store._authorize_export = pause
        store.export_deterministic_zip(sealed.path, evidence, destination)
        result.write_text("published", encoding="utf-8")
        """
    )
    poison_script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from uuid import uuid4
        from rquant.lab_artifacts import LabJobArtifactStore
        from tests.unit.test_lab_artifacts import _prepare_arguments

        root, started, complete = map(Path, sys.argv[1:4])
        started.write_text("started", encoding="utf-8")
        store = LabJobArtifactStore(root)
        def fail_restore(*_args, **_kwargs):
            raise OSError("injected guard restore failure")
        store._restore_candidate_namespace_guard = fail_restore
        arguments = _prepare_arguments()
        arguments["job_id"] = uuid4()
        try:
            store.prepare_candidate(**arguments)
        except BaseException as exc:
            complete.write_text(f"{type(exc).__name__}:{exc}", encoding="utf-8")
        else:
            complete.write_text("unexpected-success", encoding="utf-8")
        """
    )
    environment = os.environ.copy()
    exporter = subprocess.Popen(
        [
            sys.executable,
            "-c",
            exporter_script,
            str(root),
            str(sealed.path),
            str(export_entered),
            str(export_release),
            str(export_result),
            str(destination),
        ],
        cwd=Path(__file__).parents[2],
        env=environment,
    )
    poisoner: subprocess.Popen[bytes] | None = None
    try:
        deadline = time.monotonic() + 10
        while not export_entered.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert export_entered.exists()
        poisoner = subprocess.Popen(
            [
                sys.executable,
                "-c",
                poison_script,
                str(root),
                str(poison_started),
                str(poison_complete),
            ],
            cwd=Path(__file__).parents[2],
            env=environment,
        )
        deadline = time.monotonic() + 5
        while not poison_started.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert poison_started.exists()
        time.sleep(0.3)
        assert poison_complete.exists() is False
        export_release.write_text("go", encoding="utf-8")
        assert exporter.wait(timeout=15) == 0
        assert poisoner.wait(timeout=15) == 0
    finally:
        if exporter.poll() is None:
            exporter.kill()
            exporter.wait(timeout=5)
        if poisoner is not None and poisoner.poll() is None:
            poisoner.kill()
            poisoner.wait(timeout=5)

    assert export_result.read_text(encoding="utf-8") == "published"
    assert destination.is_file()
    assert poison_complete.read_text(encoding="utf-8") != "unexpected-success"
    recovered = LabJobArtifactStore(root)
    recovered.close()


def test_case_alias_same_inode_rejects_prepare_reentry_before_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_root = tmp_path / "Artifacts"
    outer_store = LabJobArtifactStore(canonical_root)
    alias_root = tmp_path / "artifacts"
    if not alias_root.exists() or alias_root.stat().st_ino != canonical_root.stat().st_ino:
        outer_store.close()
        pytest.skip("filesystem is case-sensitive")
    inner_store = LabJobArtifactStore(alias_root)
    serialize = outer_store._serialize_parquet
    attempted = False

    def attempt_reentry_then_serialize(
        table_name: str,
        frame: pd.DataFrame,
    ) -> tuple[bytes, LabJobArtifactFile]:
        nonlocal attempted
        if not attempted:
            attempted = True
            with pytest.raises(LabArtifactIntegrityError, match="reentrant"):
                _prepare(inner_store, job_id=uuid4())
        return serialize(table_name, frame)

    monkeypatch.setattr(outer_store, "_serialize_parquet", attempt_reentry_then_serialize)
    candidate = _prepare(outer_store)

    assert attempted is True
    assert outer_store._process_lock_entry is inner_store._process_lock_entry
    assert [path.name for path in outer_store.candidates_root.iterdir()] == [candidate.path.name]


def test_fifo_candidate_entry_is_quarantined_without_open_or_io(tmp_path: Path) -> None:
    store = LabJobArtifactStore(tmp_path / "artifacts")
    fifo = store.candidates_root / "invalid-fifo"
    os.mkfifo(fifo, 0o600)
    observed = fifo.lstat()

    record = next(item for item in store.list_candidate_recovery() if item.path == fifo)
    assert record.file_type == "other"
    quarantined = store.quarantine_recovery_record(record, reason="invalid fifo")

    assert quarantined.file_type == "other"
    assert (quarantined.device, quarantined.inode) == (observed.st_dev, observed.st_ino)
    assert stat.S_ISFIFO(quarantined.path.lstat().st_mode)
    assert not fifo.exists()


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("lock_kind", ["lifecycle", "namespace", "legacy"])
def test_flock_acquire_interrupt_rolls_back_unknown_os_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
    lock_kind: str,
) -> None:
    store: LabJobArtifactStore | None = None
    index: LegacyArtifactIndex | None = None
    if lock_kind == "legacy":
        index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
        lock_descriptor = index._lock_descriptor
        lock_path = index.path.with_name(f"{index.path.name}.lock")
    else:
        store = LabJobArtifactStore(tmp_path / "artifacts")
        lock_descriptor = store._guard_lock_descriptor
        lock_path = store.root / "namespace-guard.lock"
    probe_descriptor = os.open(lock_path, os.O_RDWR)
    original_flock = lab_artifacts_module.fcntl.flock
    interrupted = False

    def acquire_then_interrupt(descriptor: int, operation: int) -> None:
        nonlocal interrupted
        original_flock(descriptor, operation)
        if descriptor == lock_descriptor and operation == fcntl.LOCK_EX and not interrupted:
            interrupted = True
            raise failure_type("flock returned through an interrupting wrapper")

    monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", acquire_then_interrupt)
    try:
        with pytest.raises(failure_type, match="interrupting wrapper"):
            if lock_kind == "lifecycle":
                assert store is not None
                store.list_candidate_recovery()
            elif lock_kind == "namespace":
                assert store is not None
                with store._exclusive_namespace_guard():
                    pass
            else:
                assert index is not None
                with index._exclusive_index_lock():
                    pass
        monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", original_flock)
        original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_flock(probe_descriptor, fcntl.LOCK_UN)
        if store is not None:
            entry = store._process_lock_entry
            assert entry is not None
            assert entry.lifecycle_owner_thread_id is None
            assert entry.owner_thread_id is None
            assert entry.lifecycle_depth == 0
            assert store._operation_depth == 0
            assert store._guard_lock_depth == 0
        if index is not None:
            assert index._authority_lock_depth == 0
    finally:
        monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", original_flock)
        os.close(probe_descriptor)
        if index is not None:
            index.close()
        if store is not None:
            store.close()


@pytest.mark.parametrize("lock_kind", ["lifecycle", "namespace", "legacy"])
def test_flock_acquire_interrupt_and_rollback_failure_are_grouped_and_unlocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lock_kind: str,
) -> None:
    store: LabJobArtifactStore | None = None
    index: LegacyArtifactIndex | None = None
    if lock_kind == "legacy":
        index = LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
        lock_descriptor = index._lock_descriptor
        lock_path = index.path.with_name(f"{index.path.name}.lock")
    else:
        store = LabJobArtifactStore(tmp_path / "artifacts")
        lock_descriptor = store._guard_lock_descriptor
        lock_path = store.root / "namespace-guard.lock"
    probe_descriptor = os.open(lock_path, os.O_RDWR)
    original_flock = lab_artifacts_module.fcntl.flock
    interrupted = False
    rollback_failed = False

    def interrupt_and_fail_rollback(descriptor: int, operation: int) -> None:
        nonlocal interrupted, rollback_failed
        if descriptor != lock_descriptor:
            original_flock(descriptor, operation)
            return
        if operation == fcntl.LOCK_EX and not interrupted:
            original_flock(descriptor, operation)
            interrupted = True
            raise KeyboardInterrupt("flock acquire interrupt")
        if operation == fcntl.LOCK_UN and interrupted and not rollback_failed:
            rollback_failed = True
            raise OSError("flock rollback failure")
        original_flock(descriptor, operation)

    monkeypatch.setattr(
        lab_artifacts_module.fcntl,
        "flock",
        interrupt_and_fail_rollback,
    )
    try:
        captured: BaseException | None = None
        try:
            if lock_kind == "lifecycle":
                assert store is not None
                store.list_candidate_recovery()
            elif lock_kind == "namespace":
                assert store is not None
                with store._exclusive_namespace_guard():
                    pass
            else:
                assert index is not None
                with index._exclusive_index_lock():
                    pass
        except BaseException as error:
            captured = error
        assert isinstance(captured, BaseExceptionGroup)
        flattened = _flatten_exception_group(captured)
        assert any(isinstance(item, KeyboardInterrupt) for item in flattened)
        assert any(isinstance(item, OSError) and "rollback" in str(item) for item in flattened)
        monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", original_flock)
        original_flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_flock(probe_descriptor, fcntl.LOCK_UN)
    finally:
        monkeypatch.setattr(lab_artifacts_module.fcntl, "flock", original_flock)
        os.close(probe_descriptor)
        if index is not None:
            index.close()
        if store is not None:
            store.close()


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_secure_directory_open_closes_all_fds_on_early_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
) -> None:
    target = tmp_path / "private"
    target.mkdir(mode=0o700)
    before_descriptors = len(os.listdir("/dev/fd"))
    original_fstat = lab_artifacts_module.os.fstat
    original_open = lab_artifacts_module.os.open
    interrupted = False
    opened_descriptors: list[int] = []

    def record_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        descriptor = original_open(path, flags, *args, **kwargs)
        opened_descriptors.append(descriptor)
        return descriptor

    def interrupt_first_observation(descriptor: int) -> os.stat_result:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise failure_type("directory observation interrupted")
        return original_fstat(descriptor)

    monkeypatch.setattr(lab_artifacts_module.os, "open", record_open)
    monkeypatch.setattr(lab_artifacts_module.os, "fstat", interrupt_first_observation)
    try:
        with pytest.raises(failure_type, match="directory observation interrupted"):
            lab_artifacts_module._secure_open_directory(target, create=False)
        assert all(
            _descriptor_is_closed(descriptor, original_fstat) for descriptor in opened_descriptors
        )
    finally:
        monkeypatch.setattr(lab_artifacts_module.os, "open", original_open)
        monkeypatch.setattr(lab_artifacts_module.os, "fstat", original_fstat)
        for descriptor in opened_descriptors:
            with suppress(OSError):
                os.close(descriptor)
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_secure_directory_open_preserves_interrupt_and_close_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "private"
    target.mkdir(mode=0o700)
    before_descriptors = len(os.listdir("/dev/fd"))
    original_fstat = lab_artifacts_module.os.fstat
    original_open = lab_artifacts_module.os.open
    original_close = lab_artifacts_module.os.close
    interrupted = False
    close_failed = False
    opened_descriptors: list[int] = []

    def record_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        descriptor = original_open(path, flags, *args, **kwargs)
        opened_descriptors.append(descriptor)
        return descriptor

    def interrupt_first_observation(descriptor: int) -> os.stat_result:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt("directory observation interrupted")
        return original_fstat(descriptor)

    def close_then_fail(descriptor: int) -> None:
        nonlocal close_failed
        original_close(descriptor)
        if not close_failed:
            close_failed = True
            raise OSError("directory descriptor close failure")

    monkeypatch.setattr(lab_artifacts_module.os, "open", record_open)
    monkeypatch.setattr(lab_artifacts_module.os, "fstat", interrupt_first_observation)
    monkeypatch.setattr(lab_artifacts_module.os, "close", close_then_fail)
    try:
        captured: BaseException | None = None
        try:
            lab_artifacts_module._secure_open_directory(target, create=False)
        except BaseException as error:
            captured = error
        assert isinstance(captured, BaseExceptionGroup)
        flattened = _flatten_exception_group(captured)
        assert any(isinstance(item, KeyboardInterrupt) for item in flattened)
        assert any(isinstance(item, OSError) and "close" in str(item) for item in flattened)
    finally:
        monkeypatch.setattr(lab_artifacts_module.os, "open", original_open)
        monkeypatch.setattr(lab_artifacts_module.os, "fstat", original_fstat)
        monkeypatch.setattr(lab_artifacts_module.os, "close", original_close)
        for descriptor in opened_descriptors:
            with suppress(OSError):
                os.close(descriptor)
    assert len(os.listdir("/dev/fd")) == before_descriptors


def test_ensure_private_directory_preserves_interrupt_and_close_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "private"
    target.mkdir(mode=0o700)
    original_fchmod = lab_artifacts_module.os.fchmod
    original_close = lab_artifacts_module.os.close
    target_descriptor = -1
    close_failed = False

    def interrupt_fchmod(descriptor: int, mode: int) -> None:
        nonlocal target_descriptor
        target_descriptor = descriptor
        raise SystemExit(f"directory chmod interrupted at {mode:o}")

    def close_then_fail(descriptor: int) -> None:
        nonlocal close_failed
        original_close(descriptor)
        if descriptor == target_descriptor and not close_failed:
            close_failed = True
            raise OSError("private directory close failure")

    monkeypatch.setattr(lab_artifacts_module.os, "fchmod", interrupt_fchmod)
    monkeypatch.setattr(lab_artifacts_module.os, "close", close_then_fail)
    captured: BaseException | None = None
    try:
        lab_artifacts_module._ensure_private_directory(target, manage_existing=True)
    except BaseException as error:
        captured = error
    finally:
        monkeypatch.setattr(lab_artifacts_module.os, "fchmod", original_fchmod)
        monkeypatch.setattr(lab_artifacts_module.os, "close", original_close)

    assert isinstance(captured, BaseExceptionGroup)
    flattened = _flatten_exception_group(captured)
    assert any(isinstance(item, SystemExit) for item in flattened)
    assert any(isinstance(item, OSError) and "close" in str(item) for item in flattened)
    assert _descriptor_is_closed(target_descriptor)


@pytest.mark.parametrize("constructor", ["artifact", "legacy"])
def test_public_constructor_early_directory_interrupt_rolls_back_fds_and_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    constructor: str,
) -> None:
    artifact_registry_before = dict(lab_artifacts_module._ARTIFACT_PROCESS_LOCKS)
    legacy_registry_before = dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS)
    before_descriptors = len(os.listdir("/dev/fd"))
    original_fstat = lab_artifacts_module.os.fstat
    original_open = lab_artifacts_module.os.open
    interrupted = False
    opened_descriptors: list[int] = []

    def record_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        descriptor = original_open(path, flags, *args, **kwargs)
        opened_descriptors.append(descriptor)
        return descriptor

    def interrupt_first_observation(descriptor: int) -> os.stat_result:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt("constructor directory interrupt")
        return original_fstat(descriptor)

    monkeypatch.setattr(lab_artifacts_module.os, "open", record_open)
    monkeypatch.setattr(lab_artifacts_module.os, "fstat", interrupt_first_observation)
    try:
        with pytest.raises(KeyboardInterrupt, match="constructor directory interrupt"):
            if constructor == "artifact":
                LabJobArtifactStore(tmp_path / "artifacts")
            else:
                LegacyArtifactIndex(tmp_path / "index" / "legacy.sqlite3")
        assert all(
            _descriptor_is_closed(descriptor, original_fstat) for descriptor in opened_descriptors
        )
    finally:
        monkeypatch.setattr(lab_artifacts_module.os, "open", original_open)
        monkeypatch.setattr(lab_artifacts_module.os, "fstat", original_fstat)
        for descriptor in opened_descriptors:
            with suppress(OSError):
                os.close(descriptor)
    assert len(os.listdir("/dev/fd")) == before_descriptors
    assert dict(lab_artifacts_module._ARTIFACT_PROCESS_LOCKS) == artifact_registry_before
    assert dict(lab_artifacts_module._LEGACY_PROCESS_LOCKS) == legacy_registry_before


def test_bound_readonly_file_preserves_caller_identity_and_parent_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy.json"
    source.write_text('{"generation":1}', encoding="utf-8")
    replacement = tmp_path / "replacement.json"
    replacement.write_text('{"generation":2}', encoding="utf-8")
    before_descriptors = len(os.listdir("/dev/fd"))
    original_secure_open = lab_artifacts_module._secure_open_directory
    original_close = lab_artifacts_module.os.close
    final_check_started = False
    final_parent_descriptors: set[int] = set()

    def record_final_parent(
        path: Path,
        *,
        create: bool,
        create_mode: int = 0o700,
    ) -> int:
        descriptor = original_secure_open(
            path,
            create=create,
            create_mode=create_mode,
        )
        if final_check_started:
            final_parent_descriptors.add(descriptor)
        return descriptor

    def close_final_parent_then_fail(descriptor: int) -> None:
        original_close(descriptor)
        if descriptor in final_parent_descriptors:
            final_parent_descriptors.remove(descriptor)
            raise OSError("readonly parent close failure")

    monkeypatch.setattr(lab_artifacts_module, "_secure_open_directory", record_final_parent)
    monkeypatch.setattr(lab_artifacts_module.os, "close", close_final_parent_then_fail)

    with (
        pytest.raises(BaseExceptionGroup) as captured,
        lab_artifacts_module._open_bound_readonly_file(source, label="legacy source"),
    ):
        final_check_started = True
        os.replace(replacement, source)
        raise RuntimeError("readonly caller failure")

    flattened = _flatten_exception_group(captured.value)
    assert any(isinstance(item, RuntimeError) for item in flattened)
    assert any(isinstance(item, LabArtifactIntegrityError) for item in flattened)
    assert any(
        isinstance(item, OSError) and "readonly parent close" in str(item) for item in flattened
    )
    assert len(os.listdir("/dev/fd")) == before_descriptors
