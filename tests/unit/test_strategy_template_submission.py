"""Run recovery uses the original PageControl and Lab publication journals."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.page_control import PageControlStatus
from rquant.page_control_service import build_page_control_service
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol, PortfolioSourceData
from rquant.research_catalog import ResearchCatalog
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring import (
    StrategyAuthoringConflict,
    StrategyAuthoringPageControlBackend,
    StrategyAuthoringStore,
)
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate
from rquant.strategy_template_run_commands import RunStrategyTemplate
from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
from rquant.strategy_template_source import StrategyTemplateSourceData, TemplateRawDay
from tests.unit.test_portfolio_backtest import _CODES, _request
from tests.unit.test_strategy_authoring import NOW, draft
from tests.unit.test_strategy_authoring import catalog as source_catalog
from tests.unit.test_strategy_authoring_page_control import submit
from tests.unit.test_strategy_template_adapter import adapter_fixture


def run_service(tmp_path):
    from rquant.strategy_template_submission import (
        StrategyTemplateRunBackend,
        StrategyTemplateRunPreparer,
    )

    target = StrategyAuthoringStore(
        tmp_path / "metadata.sqlite",
        definition_root=tmp_path / "definitions",
        producer_commit=_request((_CODES[0],)).producer_commit,
        clock=lambda: NOW,
    )
    target.initialize()
    target, value, catalog, _ = adapter_fixture(tmp_path, existing_store=target)
    now = value.definition.available_at + timedelta(seconds=1)
    target.clock = lambda: now
    directory = StrategyTemplateRuntimeDirectory(target, expected_identity=target.identity())
    jobs = LabJobStore(tmp_path / "jobs.sqlite")
    jobs.initialize()
    experiments = ExperimentRegistry(tmp_path / "experiments.sqlite", managed_trust_root=tmp_path)
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(jobs.path),
        spool=LabCommandSpool(tmp_path / "commands"),
        experiment_registry=experiments,
        template_directory=directory,
        clock=lambda: now,
    )
    source = StrategyTemplateSourceData(
        owner_id="alice",
        catalog=source_catalog(),
        portfolio=PortfolioSourceData(
            source_key="verified-screen",
            source_version=1,
            template=value.request,
            sources=value.sources,
            benchmarks={},
        ),
        days=tuple(
            TemplateRawDay(
                trade_date=day.trade_date,
                entry=day.entry.evidence,
                index_closes=day.index_closes,
                minutes=day.minutes,
            )
            for day in value.days
        ),
    )
    inputs = tmp_path / "inputs"
    inputs.mkdir(mode=0o700)
    protocol = PortfolioExperimentProtocol(
        train_range=DateRange(start_date="2025-01-01", end_date="2025-01-31"),
        validation_range=DateRange(start_date="2025-02-01", end_date="2025-02-28"),
        frozen_outer_test_range=DateRange(start_date="2025-03-01", end_date="2025-03-31"),
    )
    preparer = StrategyTemplateRunPreparer(
        source_provider=lambda owner, generation, version: (
            StrategyTemplateSourceData.model_validate(
                {
                    **source.model_dump(mode="python"),
                    "catalog": source.catalog.model_copy(update={"generation_id": generation}),
                    "material_hash": None,
                }
            )
        ),
        metadata_store_factory=lambda: DuckDBStore(tmp_path / "research.duckdb"),
        catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
        lake_root=tmp_path / "lake",
        input_root=inputs,
        experiments=experiments,
        protocol=protocol,
        code_commit=target.producer_commit,
        clock=lambda: now,
    )
    run_backend = StrategyTemplateRunBackend(
        target, facade=facade, preparer=preparer, expected_identity=target.identity()
    )
    backend = StrategyAuthoringPageControlBackend(
        target, editor_users=("alice",), enabled=True, run_backend=run_backend
    )
    service = build_page_control_service(
        outbox_path=tmp_path / "page-control.sqlite",
        data_dir=tmp_path / "private-data",
        log_dir=tmp_path / "private-logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        strategy_authoring_backend=backend,
        clock=lambda: now,
    )
    head = catalog.versions[0].head
    request = RunStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=now - timedelta(days=20),
        generation_id="generation-a",
        strategy_id=value.definition.logical_id,
        head=head,
        expected_head=head,
        start_date=value.request.days[0].trade_date,
        end_date=value.request.days[-1].trade_date,
        initial_cash=value.request.initial_cash,
    )
    return target, service, run_backend, request, jobs


def test_run_uses_original_journal_facade_experiment_and_one_spool(tmp_path) -> None:
    target, service, backend, request, _ = run_service(tmp_path)
    receipt = submit(service, target, request)
    assert receipt.status is PageControlStatus.SUCCEEDED
    accepted = target.accepted_run(request, owner_id="alice", expected_identity=target.identity())
    entry = backend.facade.spool.pending()[0]
    assert entry.envelope.command.job_id == UUID(request.command_id)
    assert entry.envelope.command.spec.spec_hash == accepted.spec.spec_hash
    assert receipt.result["spec_hash"] == accepted.spec.spec_hash
    assert (
        backend.facade.experiment_registry.get_submission_intent_for_job(UUID(request.command_id))
        is not None
    )
    assert receipt.result["completed_at"] != request.requested_at.isoformat()


def test_accepted_before_enqueue_restores_without_new_source_or_new_head(
    tmp_path, monkeypatch
) -> None:
    target, service, backend, request, _ = run_service(tmp_path)
    enqueue = service.outbox.enqueue_trusted_strategy_authoring
    monkeypatch.setattr(
        service.outbox,
        "enqueue_trusted_strategy_authoring",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("before enqueue")),
    )
    with pytest.raises(RuntimeError, match="before enqueue"):
        submit(service, target, request)
    accepted = target.accepted_run(request, owner_id="alice", expected_identity=target.identity())
    next_version = target.save(
        draft(strategy_id=request.strategy_id, expected_head=request.head),
        owner_id="alice",
        catalog=source_catalog(),
    )
    assert next_version.head.version == 2
    monkeypatch.setattr(
        backend.preparer,
        "prepare",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("retry read new source")),
    )
    monkeypatch.setattr(service.outbox, "enqueue_trusted_strategy_authoring", enqueue)
    result = submit(service, target, request, sources=source_catalog(generation="changed"))
    assert (
        result.status is PageControlStatus.SUCCEEDED
        and result.result["spec_hash"] == accepted.spec.spec_hash
    )
    assert len(backend.facade.spool.pending()) == 1


def test_run_public_changed_content_and_foreign_owner_are_rejected(tmp_path) -> None:
    target, service, backend, request, _ = run_service(tmp_path)
    with pytest.raises(ValueError, match="trusted"):
        service.submit(request)
    first = submit(service, target, request)
    with pytest.raises(ValueError):
        submit(
            service, target, request.model_copy(update={"initial_cash": request.initial_cash + 1})
        )
    with pytest.raises(PermissionError):
        submit(service, target, request, owner="bob")
    assert submit(service, target, request) == first
    assert len(backend.facade.spool.pending()) == 1


def test_pending_archive_refuses_run_before_trusted_source_preparation(
    tmp_path, monkeypatch
) -> None:
    target, _, backend, request, _ = run_service(tmp_path)
    archive = ArchiveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=request.requested_at,
        generation_id=request.generation_id,
        strategy_id=request.strategy_id,
        expected_head=request.expected_head,
    )
    target.accept_archive(
        archive, owner_id="alice", catalog=source_catalog(), expected_identity=target.identity()
    )

    def unexpected_source(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("pending archive must reject before source publication")

    monkeypatch.setattr(backend.preparer, "prepare", unexpected_source)
    with pytest.raises(StrategyAuthoringConflict, match="awaiting recovery"):
        backend.compile(request, owner_id="alice", expected_identity=target.identity())
