"""Original accepted research survives journal and queue interruption."""

from datetime import timedelta
from uuid import uuid4, UUID

import pytest

from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.paper_portfolio_commands import PaperPortfolioPageControlBackend
from rquant.paper_portfolio_runtime import PaperPortfolioRuntimeCatalog
from rquant.paper_research_commands import RunPaperPortfolioResearch
from rquant.paper_research_submission import PaperResearchRunBackend, PaperResearchRunPreparer
from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
from rquant.research_catalog import ResearchCatalog
from rquant.storage.duckdb import DuckDBStore
from rquant.page_control_service import build_page_control_service
from tests.unit.test_paper_portfolio_ledger_views import filled
from tests.unit.test_paper_portfolio_view_source import market
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, _policy
from rquant.paper_signal_worker import PaperSignalQueueStore


def fixture(tmp_path):
    broker, _, _, runtime = filled(tmp_path)
    source = PaperPortfolioViewSource(runtime, broker=broker, queue=PaperSignalQueueStore(tmp_path/"queue.sqlite", policy=_policy()))
    jobs = LabJobStore(tmp_path/"jobs.sqlite")
    jobs.initialize()
    facade = LabCommandSubmissionFacade(reader=LabJobReader(jobs.path), spool=LabCommandSpool(tmp_path/"lab-commands"), clock=lambda: EXECUTION_TIME)
    root = tmp_path/"inputs"
    root.mkdir(mode=0o700)
    preparer = PaperResearchRunPreparer(sources=(source,), metadata_store_factory=lambda: DuckDBStore(tmp_path/"research.duckdb"),
                                       research_catalog=ResearchCatalog(tmp_path/"catalog.duckdb"), input_root=root, lake_root=tmp_path/"lake",
                                       code_sha="a"*40, clock=lambda: EXECUTION_TIME)
    runs = PaperResearchRunBackend(preparer=preparer, facade=facade)
    backend = PaperPortfolioPageControlBackend(PaperPortfolioRuntimeCatalog((runtime,)), clock=lambda: EXECUTION_TIME,
                                               editor_users=("alice",), enabled=True, research_backend=runs)
    service = build_page_control_service(outbox_path=tmp_path/"journal.sqlite", data_dir=tmp_path/"data", log_dir=tmp_path/"log",
                                        allowed_lab_export_roots=(), load_default_lab_backend=False, paper_portfolio_backend=backend,
                                        clock=lambda: EXECUTION_TIME)
    request = RunPaperPortfolioResearch(command_id=str(uuid4()), requested_at=EXECUTION_TIME, generation_id="a"*64,
                                        account_id=runtime.state.configuration.binding.account_id,
                                        configuration_fingerprint=runtime.state.configuration.fingerprint, task_name="paper_reconcile")
    return service, backend, runtime, request, jobs


def test_original_metadata_acceptance_before_enqueue_and_queue_publish_retry_use_exact_plan(tmp_path, monkeypatch):
    service, backend, runtime, request, _ = fixture(tmp_path)
    owned = backend.compile(request, authenticated_actor_id="alice", expected_identity=runtime.state.identity())
    runtime.state.start_configuration(runtime.state.configuration.model_copy(update={"version": 2, "configured_at": EXECUTION_TIME+timedelta(minutes=1)}))
    monkeypatch.setattr(backend.research_backend.preparer, "prepare", lambda *_args, **_kwargs: pytest.fail("original accepted run must not recompile"))
    assert backend.compile(request, authenticated_actor_id="alice", expected_identity=runtime.state.identity()) == owned
    receipt = backend.submit(owned)
    assert receipt["spec_hash"] == owned.spec.spec_hash and len(backend.research_backend.facade.spool.pending()) == 1
    assert backend.submit(owned) == receipt
    assert backend.has_effect(owned)
    with pytest.raises(ValueError):
        backend.compile(request.model_copy(update={"generation_id": "b"*64}), authenticated_actor_id="alice", expected_identity=runtime.state.identity())


def test_original_page_control_research_runs_into_original_lab_spool_and_recovers(tmp_path, monkeypatch):
    from rquant.page_control import PageControlStatus
    service, backend, runtime, request, _ = fixture(tmp_path)
    receipt = service._submit_trusted_paper_portfolio(request, authenticated_actor_id="alice", verified_metadata_identity=runtime.state.identity())
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result["job_id"] == request.command_id
    monkeypatch.setattr(backend, "compile", lambda *_args, **_kwargs: pytest.fail("retry must lookup original journal first"))
    assert service._resume_trusted_paper_portfolio(request, authenticated_actor_id="alice") == receipt
    assert len(backend.research_backend.facade.spool.pending()) == 1


def test_stored_research_receipt_cannot_be_replaced_with_another_valid_job(tmp_path):
    service, backend, runtime, request, _ = fixture(tmp_path)
    owned = backend.compile(request, authenticated_actor_id="alice", expected_identity=runtime.state.identity())
    receipt = backend.submit(owned)
    from rquant.paper_research_commands import PaperResearchSubmissionReceipt
    changed_id = str(uuid4())
    changed = PaperResearchSubmissionReceipt.model_validate({**receipt, "command_id": changed_id, "job_id": changed_id})
    with runtime.state._connection(write=True) as connection:
        connection.execute("UPDATE paper_research_admissions SET receipt_body=? WHERE command_id=?", (changed.model_dump_json(), request.command_id))
    with pytest.raises(ValueError):
        backend.submit(owned)


def test_queue_publish_lost_receipt_recovers_original_run_after_configuration_change(tmp_path, monkeypatch):
    from rquant.page_control import PageControlStatus
    service, backend, runtime, request, _ = fixture(tmp_path)
    facade = backend.research_backend.facade
    original = facade.submit_create
    original_recover = backend.recover
    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("original Lab publish completed before receipt loss")
    monkeypatch.setattr(facade, "submit_create", interrupted)
    def unavailable_recovery(_command):
        raise RuntimeError("original private receipt temporarily unavailable")
    monkeypatch.setattr(backend, "recover", unavailable_recovery)
    first = service._submit_trusted_paper_portfolio(request, authenticated_actor_id="alice", verified_metadata_identity=runtime.state.identity())
    assert first.status is PageControlStatus.PENDING and len(facade.spool.pending()) == 1
    runtime.state.start_configuration(runtime.state.configuration.model_copy(update={"version": 2, "configured_at": EXECUTION_TIME+timedelta(minutes=1)}))
    monkeypatch.setattr(facade, "submit_create", original)
    monkeypatch.setattr(backend, "recover", original_recover)
    monkeypatch.setattr(backend.research_backend.preparer, "prepare", lambda *_args, **_kwargs: pytest.fail("lost receipt must restore original accepted plan"))
    recovered = service._resume_trusted_paper_portfolio(request, authenticated_actor_id="alice")
    assert recovered.status is PageControlStatus.SUCCEEDED and len(facade.spool.pending()) == 1
    assert recovered.result["configuration_fingerprint"] == request.configuration_fingerprint


def test_full_admission_budget_rejects_before_source_work_and_original_retry_still_succeeds(tmp_path, monkeypatch):
    service, backend, runtime, request, _ = fixture(tmp_path)
    runs = backend.research_backend
    original = runs.compile(request, owner_id="alice", expected_identity=runtime.state.identity())
    with runtime.state._connection(write=True) as connection:
        connection.executemany("INSERT INTO paper_research_admissions VALUES(?,?,?,?,NULL)",
                               ((str(UUID(int=index+1)), "alice", request.model_dump_json(), original.model_dump_json()) for index in range(4095)))
    monkeypatch.setattr(runs.preparer, "prepare", lambda *_args, **_kwargs: pytest.fail("full capacity cannot reach source allocation"))
    assert runs.compile(request, owner_id="alice", expected_identity=runtime.state.identity()) == original
    with pytest.raises(ValueError, match="budget"):
        runs.compile(request.model_copy(update={"command_id": str(uuid4())}), owner_id="alice", expected_identity=runtime.state.identity())


def test_research_public_payload_and_default_disabled_backend_refuse(tmp_path):
    from rquant.page_control import parse_page_control_command
    service, backend, runtime, request, _ = fixture(tmp_path)
    owned = backend.compile(request, authenticated_actor_id="alice", expected_identity=runtime.state.identity())
    for value in (request, owned, owned.model_dump(mode="json")):
        with pytest.raises(ValueError):
            parse_page_control_command(value)
    with pytest.raises(ValueError):
        service.outbox.enqueue(owned)
    backend.research_backend = None
    with pytest.raises(PermissionError):
        service._submit_trusted_paper_portfolio(request, authenticated_actor_id="alice", verified_metadata_identity=runtime.state.identity())
    assert service.outbox.receipt(request.command_id) is None
