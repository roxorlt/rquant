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


def test_cancellation_after_heartbeat_join_before_terminal_branch_cleans_prepared(
    tmp_path: Path,
) -> None:
    import inspect
    import sys

    from rquant.factor.job_worker import run_one_factor_job

    with _sealed(tmp_path) as (ledger, clock, claim, _, root, members, metadata, lake):
        clock.instant += timedelta(seconds=31)
        lines, first_line = inspect.getsourcelines(run_one_factor_job)
        terminal_line = first_line + next(
            offset
            for offset, line in enumerate(lines)
            if "if thread.is_alive() or lost.is_set():" in line
        )
        reached = False
        previous_trace = sys.gettrace()

        def interrupt(frame: object, event: str, argument: object) -> object:
            nonlocal reached
            if (
                frame.f_code is run_one_factor_job.__code__
                and event == "line"
                and frame.f_lineno == terminal_line
            ):
                assert ledger._prepared
                assert not any(
                    t.name == "factor-job-heartbeat" and t.is_alive() for t in threading.enumerate()
                )
                assert not list((lake / ".execution_sessions").iterdir())
                reached = True
                sys.settrace(previous_trace)
                raise KeyboardInterrupt("synthetic cancellation after join before terminal branch")
            return interrupt

        try:
            sys.settrace(interrupt)
            with pytest.raises(KeyboardInterrupt, match="after join"):
                run_one_factor_job(
                    ledger,
                    metadata_store=metadata,
                    lake_root=lake,
                    artifact_root=root,
                    member_root=members,
                    runner_now=clock,
                )
        finally:
            sys.settrace(previous_trace)
        try:
            record = ledger.get(claim.job.job_id)
            print(
                f"SJ_FINAL_01: after_join={reached} status={record.status} "
                f"prepared_after_get={len(ledger._prepared)} heartbeat_threads=0 "
                "execution_copies=0"
            )
            assert reached and record.status == "running"
            assert not ledger._prepared
        finally:
            ledger._discard_job_prepared(claim.job.job_id)
            assert not ledger._prepared


def test_old_worker_cleanup_and_stale_prepare_preserve_new_claim_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker

    with _sealed(tmp_path) as (ledger, clock, claim, completion, root, members, metadata, lake):
        clock.instant += timedelta(seconds=31)
        original_complete = ledger.complete_prepared_stream
        newer = newer_handle = worker_token = None

        def reclaims_before_old_cas(
            job_id: str, lease_token: str, version: int, handle: object
        ) -> object:
            nonlocal newer, newer_handle, worker_token
            worker_token = lease_token
            clock.instant += timedelta(seconds=31)
            newer = ledger.claim(lease_seconds=30)
            assert newer.job.job_id == job_id and newer.lease_token != lease_token
            newer_handle = ledger.prepare_stream_completion(
                job_id, newer.lease_token, completion, root, members
            )
            assert len(ledger._prepared) == 1
            return original_complete(job_id, lease_token, version, handle)

        monkeypatch.setattr(job_worker, "run_factor_stream_job", lambda *a, **k: completion)
        monkeypatch.setattr(ledger, "complete_prepared_stream", reclaims_before_old_cas)
        try:
            old = job_worker.run_one_factor_job(
                ledger,
                metadata_store=metadata,
                lake_root=lake,
                artifact_root=root,
                member_root=members,
                runner_now=clock,
            )
            after_exit = len(ledger._prepared)
            assert newer is not None and newer_handle is not None and worker_token is not None
            renewed = ledger.heartbeat(newer.job.job_id, newer.lease_token, newer.version, 30)
            with pytest.raises(FactorLedgerLeaseError):
                ledger.prepare_stream_completion(
                    newer.job.job_id, worker_token, completion, root, members
                )
            after_stale_prepare = len(ledger._prepared)
            print(
                f"SJ_FINAL_02: old_status={old.status} prepared_after_exit={after_exit} "
                f"prepared_after_stale_prepare={after_stale_prepare} "
                f"new_heartbeat_version={renewed.version}"
            )
            finished = original_complete(
                newer.job.job_id, newer.lease_token, renewed.version, newer_handle
            )
            assert old.status == "lease_lost" and after_exit == after_stale_prepare == 1
            assert finished.status == "succeeded" and not ledger._prepared
            threads = sum(
                t.name == "factor-job-heartbeat" and t.is_alive() for t in threading.enumerate()
            )
            copies = len(list((lake / ".execution_sessions").iterdir()))
            assert threads == copies == 0
            print(
                f"SJ_FINAL_02_COMPLETE: new_status={finished.status} prepared=0 "
                f"heartbeat_threads={threads} execution_copies={copies}"
            )
        finally:
            ledger._discard_job_prepared(claim.job.job_id)


def test_cancellation_before_prepare_handle_assignment_cleans_own_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.job_worker import run_one_factor_job

    with _sealed(tmp_path) as (ledger, clock, claim, _, root, members, metadata, lake):
        clock.instant += timedelta(seconds=31)
        original = ledger.prepare_stream_completion

        def prepared_then_cancel(*args: object, **kwargs: object) -> object:
            handle = original(*args, **kwargs)
            assert handle in ledger._prepared
            raise KeyboardInterrupt("synthetic cancellation before handle assignment")

        monkeypatch.setattr(ledger, "prepare_stream_completion", prepared_then_cancel)
        with pytest.raises(KeyboardInterrupt, match="before handle assignment"):
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
        assert not list((lake / ".execution_sessions").iterdir())


def test_invalid_prepare_token_refuses_before_private_evidence_cleanup(tmp_path: Path) -> None:
    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, _, lake):
        handle = ledger.prepare_stream_completion(
            claim.job.job_id, claim.lease_token, completion, root, members
        )
        assert len(ledger._prepared) == 1
        try:
            with pytest.raises(FactorLedgerLeaseError):
                ledger.prepare_stream_completion(claim.job.job_id, None, completion, root, members)
            after_invalid = len(ledger._prepared)
            renewed = ledger.heartbeat(claim.job.job_id, claim.lease_token, claim.version, 30)
            print(
                f"SJ_FINAL_02_NULL: rejected=true prepared_after_invalid={after_invalid} "
                f"valid_heartbeat_version={renewed.version}"
            )
            finished = ledger.complete_prepared_stream(
                claim.job.job_id, claim.lease_token, renewed.version, handle
            )
            assert after_invalid == 1 and finished.status == "succeeded"
            assert not ledger._prepared
            threads = sum(
                t.name == "factor-job-heartbeat" and t.is_alive() for t in threading.enumerate()
            )
            copies = len(list((lake / ".execution_sessions").iterdir()))
            assert threads == copies == 0
            print(
                f"SJ_FINAL_02_NULL_COMPLETE: status={finished.status} prepared=0 "
                f"heartbeat_threads={threads} execution_copies={copies}"
            )
        finally:
            ledger._discard_job_prepared(claim.job.job_id)
