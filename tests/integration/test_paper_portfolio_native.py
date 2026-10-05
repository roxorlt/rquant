"""Root-only proof of the original isolated paper research and download chain."""

from __future__ import annotations

import hashlib
import gc
import io
import multiprocessing
import threading
import time
import zipfile

import pytest

from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.lab_jobs import JobStatus, LabResultState
from rquant.paper_portfolio_admission import PaperPortfolioAdmission
from tests.unit.test_paper_research_runtime_chain import accept_and_change_head, host
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def test_original_isolated_worker_seals_readonly_reconcile_and_downloads_same_old_version(tmp_path):
    page, backend, runtime, request, scheduler, worker, finalizer, reader, source = host(tmp_path)
    ledger = runtime.ledger_source_for(source.broker)
    paths = (ledger.path, ledger.path.with_name(ledger.path.name+"-wal"), ledger.anchor_path)

    def financial_bytes():
        return tuple(hashlib.sha256(path.read_bytes()).hexdigest() if path is not None and path.exists() else None for path in paths)

    idle_writer = source.broker._connect()
    gc.collect()
    before = financial_bytes()
    before_children = {child.pid for child in multiprocessing.active_children()}
    outcomes, errors = [], []

    def tick() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=tick, name="paper-native-one-tick")
    try:
        job_id, owned = accept_and_change_head(page, backend, runtime, request, scheduler)
        thread.start()
        expires = time.monotonic()+55
        while thread.is_alive() and time.monotonic() < expires:
            scheduler.run_once()
            thread.join(timeout=0.02)
        assert not thread.is_alive(), "paper isolated worker exceeded its bounded fixture window"
        assert not errors, errors
        assert outcomes and outcomes[0].status == "succeeded", outcomes
        assert reader.reader.get_job(job_id).result_state is LabResultState.READY
        assert reader.read(account_id=request.account_id, job_id=job_id, owner_id="alice", as_of=EXECUTION_TIME) is None
        assert finalizer.finalize(job_id).status == "published"
        assert scheduler.run_once().artifact_commits_accepted == 1
        job = reader.reader.get_job(job_id)
        assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED and job.spec == owned.spec
        value = reader.read(account_id=request.account_id, job_id=job_id, owner_id="alice", as_of=EXECUTION_TIME)
        assert value.configuration_version == 1 and value.reconcile.status == "consistent"
        assert value.reconcile.account.cash == 195 and value.reconcile.account.holdings[0].quantity == 800
        export = LabJobZipExportFacade(reader=reader.reader, artifact_store=finalizer.artifact_store, export_root=tmp_path/"exports")
        admission = PaperPortfolioAdmission(page, backend=backend, result_reader=reader, zip_export=export)
        download = admission.download(account_id=request.account_id, job_id=job_id, authenticated_actor_id="alice")
        with zipfile.ZipFile(io.BytesIO(download.content)) as archive:
            assert archive.testzip() is None and {"spec.json", "manifest.json"} <= set(archive.namelist())
        with pytest.raises(PermissionError):
            admission.download(account_id=request.account_id, job_id=job_id, authenticated_actor_id="bob")
        assert reader.summary(account_id=request.account_id, job_id=job_id, owner_id="alice", as_of=EXECUTION_TIME).sealed == value
        assert financial_bytes() == before, "readonly research or trusted export changed original DB/WAL/anchor bytes"
        print("NATIVE_PAPER_PROOF original-journal/immutable-v1/scheduler/isolated-worker/readonly-reconcile/finalizer/sealed-reader/trusted-zip; actual cash195 quantity800; DB/WAL/anchor unchanged; synthetic prices+a*40 exploratory only")
    finally:
        worker.request_stop()
        if thread.ident is not None:
            thread.join(timeout=15)
        worker.close()
        scheduler.release()
        idle_writer.close()
        assert not thread.is_alive()
        assert {child.pid for child in multiprocessing.active_children()} <= before_children
        assert not worker._managed_authority_children
        print("NATIVE_PAPER_CLEANUP own-thread-joined/worker-closed/scheduler-released/no-new-active-child")
