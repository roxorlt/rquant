from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest

from rquant.experiment_platform_commands import RegisterExperimentFamily, bind_experiment_platform
from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, LabResultState
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from tests.unit.test_backtest_chain import _remove_owned_tree
from tests.unit.test_experiment_platform import NOW, search
from tests.unit.test_experiment_platform_flow import preparation as preparation


def test_exp01_exp08d_exp14_original_host_native_worker_and_private_binding(
    preparation, tmp_path: Path
) -> None:
    if os.environ.get("RQUANT_M8_HOST_NATIVE_PROOF") != "1":
        pytest.skip("host native/socket proof is executed by the root task")
    platform, prepare, data, profile, reads, _ = preparation
    platform.install_policy(months=0, now=NOW)
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    commands = LabCommandSpool(tmp_path / "commands")
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commits = LabArtifactCommitSpool(tmp_path / "commits")
    artifacts = LabJobArtifactStore(tmp_path / "artifacts")
    results = PortfolioResultReader(reader=reader, artifact_root=artifacts.root)
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=commands,
        experiment_registry=platform.registry,
        definition_registry=prepare.definitions,
        clock=lambda: NOW,
    )
    binding = bind_experiment_platform(
        store=platform,
        commands=facade,
        prepare=prepare,
        results=results,
        default_config=search().base_config,
        owners=frozenset({"alice"}),
        administrators=frozenset({"alice"}),
        enabled=True,
    )
    control = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=control,
        consumer=PageControlConsumer(
            outbox=control,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            experiment_backend=binding.command_backend,
            clock=lambda: NOW,
        ),
    )
    command = RegisterExperimentFamily(
        command_id=str(UUID(int=950)), requested_at=NOW, actor_id="alice", request=search()
    )
    receipt = service.submit(command)
    assert receipt.status.value == "succeeded" and receipt.result["planned_count"] == 4, receipt
    assert service.submit(command) == receipt and len(reads) == 1
    job_ids = tuple(UUID(value) for value in receipt.result["job_ids"])
    hashes = {
        str(p.relative_to(tmp_path)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (*prepare.input_root.rglob("*.duckdb"), *prepare.lake_root.rglob("*.parquet"))
    }
    key = LabFinalizerAuthorityKey(key_id="m8-synthetic-host", secret=secrets.token_bytes(32))

    # Deterministic EXP08-D pause: original jobs/finalizer commit real successes,
    # while only the experiment lifecycle notification is held until cancellation.
    class PausedLifecycle:
        def validate_submission(
            self, envelope: LabCommandEnvelope, *, observed_at: datetime
        ) -> None:
            binding.lifecycle.validate_submission(envelope, observed_at=observed_at)

        def recover(self, *, observed_at: datetime) -> None:
            return None

        def synchronize(self, job_id: UUID, *, observed_at: datetime) -> None:
            return None

    scheduler = LabScheduler(
        store=jobs,
        spool=commands,
        owner_id="m8-synthetic-scheduler",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("m8-synthetic-worker",),
        shard_lease_seconds=120,
        artifact_commit_spool=commits,
        artifact_store=artifacts,
        adapter_registry=default_strategy_job_adapter_registry(),
        finalizer_authority_key_provider=lambda identifier: (
            key if identifier == key.key_id else None
        ),
        lifecycle_synchronizer=PausedLifecycle(),
        clock=lambda: NOW,
    )
    socket_root = Path(tempfile.mkdtemp(prefix="m8-native-", dir="/private/tmp"))
    previous_tmp = os.environ.get("TMPDIR")
    previous_tempdir = tempfile.tempdir
    os.environ["TMPDIR"] = str(socket_root)
    tempfile.tempdir = str(socket_root)
    thread = None
    outcomes = []
    failures = []
    try:
        worker = LabWorker(
            worker_id="m8-synthetic-worker",
            claim_spool=claims,
            report_spool=reports,
            artifact_root=tmp_path / "shards",
            verified_code_sha_provider=lambda: profile.producer_commit,
            shard_runtime_manifest=build_builtin_shard_runtime_manifest(
                catalog_path=tmp_path / "metadata.duckdb",
                forbidden_paths=(),
                snapshot_root=tmp_path / "worker-copies",
                research_lake_root=prepare.lake_root,
            ),
            heartbeat_interval_seconds=1,
            receipt_timeout_seconds=10,
            clock=lambda: NOW,
        )

        def run() -> None:
            try:
                outcomes.append(worker.run_once())
            except BaseException as error:
                failures.append(error)

        finalizer = LabFinalizer(
            reader=reader,
            shard_artifact_root=tmp_path / "shards",
            artifact_store=artifacts,
            commit_spool=commits,
            verified_code_sha_provider=lambda: profile.producer_commit,
            finalizer_authority_key_provider=lambda: key,
        )
        finalizations = []
        scheduler.run_once()
        finished_jobs = set()
        for _ in job_ids:
            scheduler.run_once()
            thread = threading.Thread(target=run, name="m8-native-proof")
            thread.start()
            deadline = time.monotonic() + 90
            while thread.is_alive() and time.monotonic() < deadline:
                scheduler.run_once()
                thread.join(0.02)
            assert not thread.is_alive(), "original native worker did not finish"
            if failures:
                raise failures[0]
            assert outcomes[-1].status == "succeeded", outcomes[-1]
            scheduler.run_once()
            ready = tuple(
                job_id
                for job_id in job_ids
                if job_id not in finished_jobs
                and reader.get_finalization_snapshot(job_id) is not None
            )
            assert len(ready) == 1
            job_id = ready[0]
            finalized = finalizer.finalize(job_id)
            assert finalized.status == "published", finalized
            finalizations.append(finalized)
            finished_jobs.add(job_id)
            scheduler.run_once()
            job = reader.get_job(job_id)
            assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED
            assert (
                platform.registry.get_attempt(job.spec.experiment.experiment_id).status.value
                == "registered"
            )
        family_id = receipt.result["family_id"]
        pending = platform.cancel_family(
            owner="alice", family_id=family_id, request_id=UUID(int=951), now=NOW
        )
        assert all(c.cancel_state == "pending" for c in pending)
        binding.lifecycle.recover(observed_at=NOW)
        assert all(platform.child(job_id).cancel_state == "already_completed" for job_id in job_ids)
        snapshot = binding.private_projection.snapshot(NOW)
        family = snapshot.families[0]
        assert len(snapshot.attempts) == 4 and all(
            f.attempt.status.value == "executed" and f.result_hash for f in snapshot.attempts
        )
        for fact in snapshot.attempts:
            with pytest.raises(PermissionError):
                results.read(fact.child.job_id)
            with pytest.raises(PermissionError):
                results.read(
                    fact.child.job_id,
                    private_owner="bob",
                    private_authority=binding.private_projection.authority,
                )
            read = results.read(
                fact.child.job_id,
                private_owner="alice",
                private_authority=binding.private_projection.authority,
            )
            assert read.bundle.result.status == "complete" and len(read.bundle.result.days) == 4
        evidence = binding.private_projection.authority.evidence(
            "alice", family_id, family.evidence_id
        )
        assert evidence.search_count == 4 and all(
            s.psr is None and s.dsr is None and s.reasons for s in evidence.statistics
        )
        assert not list((tmp_path / "worker-copies").iterdir())
        assert not list((prepare.lake_root / ".execution_sessions").iterdir())
        assert hashes == {
            str(p.relative_to(tmp_path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (*prepare.input_root.rglob("*.duckdb"), *prepare.lake_root.rglob("*.parquet"))
        }
        assert len(reads) == 1 and service.submit(command) == receipt
        print(
            json.dumps(
                {
                    "kind": "m8-synthetic-host-native/v1",
                    "actual_original_workers": len(outcomes),
                    "actual_finalizations": len(finalizations),
                    "original_jobs": len(reader.list_jobs().items),
                    "registered_attempts": 4,
                    "private_owned_full_results": 4,
                    "cancel_after_actual_success": "already_completed",
                    "source_unchanged": True,
                    "worker_sessions_empty": True,
                    "ordinary_private_denied": True,
                    "missing_independence_unavailable": True,
                    "actual_market_source": False,
                },
                ensure_ascii=False,
            )
        )
    finally:
        if thread is not None and thread.is_alive():
            thread.join(15)
        scheduler.release()
        artifacts.close()
        if previous_tmp is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = previous_tmp
        tempfile.tempdir = previous_tempdir
        if thread is None or not thread.is_alive():
            _remove_owned_tree(socket_root)
        else:
            raise RuntimeError(
                "native proof thread remains active; "
                "preserve its private source and socket directory"
            )
