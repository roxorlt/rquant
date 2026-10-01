"""Prepared statistics never replace the original lease and file authority."""

import copy
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest

from rquant.factor.job_ledger import (
    FactorEvaluationJobLedger,
    FactorLedgerCompletionError,
    FactorLedgerLeaseError,
)
from tests.unit.test_factor_job_ledger import _Clock
from tests.unit.test_factor_member_stream import _archive
from tests.unit.test_factor_stream_adapter import _pools, _prepared
from tests.unit.test_factor_stream_job_spec import _spec


def _sealed(tmp_path: Path) -> object:
    from contextlib import contextmanager

    from rquant.factor.stream_job_runner import run_factor_stream_job

    @contextmanager
    def fixture() -> object:
        with _prepared(tmp_path) as (metadata, lake, request):
            members, reference, request = _archive(tmp_path, request, _pools(request))
            spec = _spec(tmp_path, request, members, reference)
            root = tmp_path / "artifacts"
            root.mkdir(mode=0o700)
            clock = _Clock(request.formula.as_of)
            ledger_dir = tmp_path / "authority"
            ledger_dir.mkdir(mode=0o700)
            ledger = FactorEvaluationJobLedger(ledger_dir / "jobs.sqlite3", clock=clock)
            ledger.initialize()
            ledger.submit("test-stream", spec)
            claim = ledger.claim(lease_seconds=30)
            completion = run_factor_stream_job(
                spec,
                metadata_store=metadata,
                lake_root=lake,
                member_root=members,
                artifact_root=root,
                now=clock,
            )
            yield ledger, clock, claim, completion, root, members, metadata, lake

    return fixture()


def test_prepared_handle_allows_heartbeat_version_but_not_copies_or_another_instance(
    tmp_path: Path,
) -> None:
    with _sealed(tmp_path) as (ledger, clock, claim, completion, root, members, *_):
        handle = ledger.prepare_stream_completion(
            claim.job.job_id, claim.lease_token, completion, root, members
        )
        clone = FactorEvaluationJobLedger.open_existing(ledger._expected, clock=clock)
        for owner, candidate in ((ledger, object()), (ledger, copy.copy(handle)), (clone, handle)):
            with pytest.raises(FactorLedgerCompletionError):
                owner.complete_prepared_stream(
                    claim.job.job_id, claim.lease_token, claim.version, candidate
                )
        clock.instant += timedelta(seconds=1)
        renewed = ledger.heartbeat(claim.job.job_id, claim.lease_token, claim.version, 30)
        record = ledger.complete_prepared_stream(
            claim.job.job_id, claim.lease_token, renewed.version, handle
        )
        assert record.status == "succeeded" and record.spec.schema_version == 2
        assert not ledger._prepared
        assert ledger.submit("test-stream", record.spec).job_id == record.job_id


def test_actual_sqlite_writer_wait_rechecks_same_bytes_new_inode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, *_):
        handle = ledger.prepare_stream_completion(
            claim.job.job_id, claim.lease_token, completion, root, members
        )
        blocker = sqlite3.connect(ledger.path, isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        waiting = threading.Event()
        original_connect = sqlite3.connect

        def connected(*args: object, **kwargs: object) -> sqlite3.Connection:
            connection = original_connect(*args, **kwargs)
            if "mode=rw" in str(args[0]):
                connection.set_trace_callback(
                    lambda sql: waiting.set() if sql == "BEGIN IMMEDIATE" else None
                )
            return connection

        monkeypatch.setattr(sqlite3, "connect", connected)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    ledger.complete_prepared_stream,
                    claim.job.job_id,
                    claim.lease_token,
                    claim.version,
                    handle,
                )
                assert waiting.wait(2)
                path = root / completion.artifact_filename
                inode = path.stat().st_ino
                replacement = root / "replacement.json"
                replacement.write_bytes(path.read_bytes())
                replacement.chmod(0o600)
                os.replace(replacement, path)
                assert path.stat().st_ino != inode
                blocker.execute("ROLLBACK")
                with pytest.raises(FactorLedgerCompletionError, match="changed"):
                    future.result(timeout=5)
        finally:
            blocker.close()
        assert ledger.get(claim.job.job_id).status == "running"
        assert not ledger._prepared
        print(
            "SJ_WRITER_WAIT: real_write_lock=true inode_changed=true "
            "succeeded=false threads_joined=true"
        )


def test_expired_preparation_cannot_complete_or_survive_reclaim(tmp_path: Path) -> None:
    with _sealed(tmp_path) as (ledger, clock, claim, completion, root, members, *_):
        handle = ledger.prepare_stream_completion(
            claim.job.job_id, claim.lease_token, completion, root, members
        )
        clock.instant += timedelta(seconds=31)
        with pytest.raises(FactorLedgerLeaseError):
            ledger.complete_prepared_stream(
                claim.job.job_id, claim.lease_token, claim.version, handle
            )
        fresh = ledger.claim(lease_seconds=30)
        assert fresh.lease_token != claim.lease_token and not ledger._prepared
        with pytest.raises(FactorLedgerCompletionError):
            ledger.complete_prepared_stream(
                fresh.job.job_id, fresh.lease_token, fresh.version, handle
            )


@pytest.mark.parametrize(
    "swap",
    ["ledger", "artifact_root", "member_root", "member_file", "journal_file", "display_file"],
)
def test_prepared_file_or_root_generation_change_cannot_succeed(tmp_path: Path, swap: str) -> None:
    from rquant.factor.job_ledger import FactorLedgerIdentityError
    from rquant.factor.member_archive import load_factor_member_archive
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts

    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, *_):
        full = verify_factor_stream_artifacts(claim.job.spec, completion, root, members).full
        handle = ledger.prepare_stream_completion(
            claim.job.job_id, claim.lease_token, completion, root, members
        )
        if swap.endswith("root"):
            target = root if swap == "artifact_root" else members
            target.rename(target.with_name(target.name + "-old"))
            target.mkdir(mode=0o700)
        else:
            target = (
                ledger.path
                if swap == "ledger"
                else (
                    members
                    / load_factor_member_archive(members, claim.job.spec.member_archive)
                    .days[0]
                    .filename
                    if swap == "member_file"
                    else root
                    / (
                        full.journal.days[-1].artifact.filename
                        if swap == "journal_file"
                        else completion.display_artifact_filename
                    )
                )
            )
            replacement = target.with_name("same-bytes-replacement")
            replacement.write_bytes(target.read_bytes())
            replacement.chmod(0o600)
            os.replace(replacement, target)
        with pytest.raises((FactorLedgerCompletionError, FactorLedgerIdentityError)):
            ledger.complete_prepared_stream(
                claim.job.job_id, claim.lease_token, claim.version, handle
            )
        assert not ledger._prepared
        if swap != "ledger":
            assert ledger.get(claim.job.job_id).status == "running"


def test_another_claim_invalidates_handle_without_pinning_heartbeat_version(tmp_path: Path) -> None:
    with _sealed(tmp_path) as (ledger, clock, claim, completion, root, members, *_):
        handle = ledger.prepare_stream_completion(
            claim.job.job_id, claim.lease_token, completion, root, members
        )
        clone = FactorEvaluationJobLedger.open_existing(ledger._expected, clock=clock)
        clock.instant += timedelta(seconds=31)
        newer = clone.claim(lease_seconds=30)
        with pytest.raises(FactorLedgerLeaseError):
            ledger.complete_prepared_stream(
                claim.job.job_id, claim.lease_token, newer.version, handle
            )
        assert not ledger._prepared and clone.get(claim.job.job_id).status == "running"


def test_completion_for_another_exact_spec_does_not_prepare(tmp_path: Path) -> None:
    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, *_):
        different = claim.job.spec.model_copy(update={"code_revision": "b" * 40})
        job = ledger.submit("different-spec", different)
        next_claim = ledger.claim(lease_seconds=30)
        assert next_claim.job.job_id == job.job_id
        with pytest.raises(FactorLedgerCompletionError):
            ledger.prepare_stream_completion(
                job.job_id, next_claim.lease_token, completion, root, members
            )
        assert not ledger._prepared and ledger.get(job.job_id).status == "running"


def test_worker_prepares_with_running_heartbeat_and_short_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger, job_worker

    with _prepared(tmp_path) as (metadata, lake, request):
        members, reference, request = _archive(tmp_path, request, _pools(request))
        spec = _spec(tmp_path, request, members, reference)
        root, authority = tmp_path / "artifacts", tmp_path / "authority"
        root.mkdir(mode=0o700)
        authority.mkdir(mode=0o700)
        clock = _Clock(request.formula.as_of)
        ledger = FactorEvaluationJobLedger(authority / "jobs.sqlite3", clock=clock)
        ledger.initialize()
        job = ledger.submit("worker", spec)
        original = job_ledger.verify_factor_stream_artifacts
        receipt = {}

        def verify(*args: object) -> object:
            receipt["started"] = time.perf_counter()
            # Advance only this test's trusted clock while real heartbeats renew it.
            for _ in range(3):
                clock.instant += timedelta(seconds=1)
                time.sleep(0.06)
            core_started = time.perf_counter()
            checked = original(*args)
            receipt["core_seconds"] = time.perf_counter() - core_started
            receipt["seconds"] = time.perf_counter() - receipt["started"]
            receipt["version"] = ledger.get(job.job_id).version
            return checked

        monkeypatch.setattr(job_ledger, "verify_factor_stream_artifacts", verify)
        original_complete = ledger.complete_prepared_stream

        def complete(*args: object) -> object:
            assert not any(
                t.name == "factor-job-heartbeat" and t.is_alive() for t in threading.enumerate()
            )
            monkeypatch.setattr(
                job_ledger,
                "verify_factor_stream_artifacts",
                lambda *a: pytest.fail("statistics replay inside short CAS"),
            )
            return original_complete(*args)

        monkeypatch.setattr(ledger, "complete_prepared_stream", complete)
        result = job_worker.run_one_factor_job(
            ledger,
            metadata_store=metadata,
            lake_root=lake,
            artifact_root=root,
            member_root=members,
            runner_now=clock,
            lease_seconds=2,
            heartbeat_interval_seconds=0.02,
        )
        assert result.status == "succeeded" and receipt["version"] > 1
        assert not ledger._prepared and not list((lake / ".execution_sessions").iterdir())
        print(
            f"SJ_PREPARE: heartbeat_version={receipt['version']} "
            f"duration_s={receipt['seconds']:.6f} "
            f"actual_replay_s={receipt['core_seconds']:.6f} "
            "initial_lease_s=2 elapsed_clock_s=3 stopped_before_cas=true replay_after_stop=false"
        )


def test_cancellation_after_prepare_before_cas_discards_private_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.job_worker import run_one_factor_job

    with _sealed(tmp_path) as (ledger, clock, claim, _, root, members, metadata, lake):
        clock.instant += timedelta(seconds=31)
        original = threading.Thread.join

        def joined(thread: threading.Thread, timeout: float | None = None) -> None:
            original(thread, timeout)
            if thread.name == "factor-job-heartbeat":
                assert ledger._prepared
                raise KeyboardInterrupt("synthetic cancellation after preparation")

        monkeypatch.setattr(threading.Thread, "join", joined)
        with pytest.raises(KeyboardInterrupt):
            run_one_factor_job(
                ledger,
                metadata_store=metadata,
                lake_root=lake,
                artifact_root=root,
                member_root=members,
                runner_now=clock,
            )
        assert not ledger._prepared and ledger.get(claim.job.job_id).status == "running"
        assert not any(
            t.name == "factor-job-heartbeat" and t.is_alive() for t in threading.enumerate()
        )
