from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_platform import ExperimentPlatformStore, ExperimentSourceProfile
from rquant.experiment_platform_commands import ExperimentFamilyPreparer
from rquant.portfolio_backtest_definition import bootstrap_portfolio_definition
from rquant.portfolio_backtest_source import PortfolioRequestPreparer, PortfolioSourceData
from rquant.research_catalog import ResearchCatalog
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from tests.unit.test_backtest_platform import frozen
from tests.unit.test_experiment_platform import NOW, protocol, registry, search
from tests.unit.test_portfolio_backtest import _CODES, _request


@pytest.fixture
def preparation(tmp_path: Path):
    raw = _request((_CODES[0], _CODES[1], _CODES[0], _CODES[1], _CODES[0], _CODES[1]))
    data = PortfolioSourceData(
        source_key="verified-screen",
        source_version=1,
        template=raw,
        sources=frozen().sources,
        benchmarks={"000300.SH": tuple((d, 100.0 + i) for i, d in enumerate(raw.calendar.dates))},
    )
    definitions_root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=raw.producer_commit)
    bootstrap_builtin_definitions(
        definitions_root,
        producer_commit=raw.producer_commit,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=plan.plan_id,
    )
    bootstrap_portfolio_definition(definitions_root, producer_commit=raw.producer_commit, now=NOW)
    definitions = ImmutableDefinitionRegistry(
        definitions_root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=raw.producer_commit
        ).trusted_executable_registry(),
    )
    store = ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    profile = ExperimentSourceProfile(
        source_key=data.source_key,
        source_version=1,
        label="合成历史研究",
        producer_commit=raw.producer_commit,
        sources=data.sources,
        calendar=raw.calendar,
        coverage=protocol().train_range.model_copy(
            update={"end_date": protocol().frozen_outer_test_range.end_date}
        ),
        latest_complete=raw.days[-1].trade_date,
        phase_slice_available=True,
    )
    reads = []

    def provider(request):
        reads.append(request)
        dates = data.template.calendar.dates
        baseline = dates[dates.index(request.window.start_date) - 1]
        template = data.template.model_copy(
            update={
                "days": tuple(
                    d
                    for d in data.template.days
                    if request.window.start_date <= d.trade_date <= request.window.end_date
                )
            }
        )
        return PortfolioSourceData(
            source_key=data.source_key,
            source_version=1,
            template=template,
            sources=data.sources,
            benchmarks={
                key: tuple((d, v) for d, v in rows if baseline <= d <= request.window.end_date)
                for key, rows in data.benchmarks.items()
            },
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir(mode=0o700)
    kwargs = dict(
        metadata_store_factory=lambda: DuckDBStore(tmp_path / "metadata.duckdb"),
        catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
        lake_root=tmp_path / "lake",
        input_root=inputs,
        definitions=definitions,
        clock=lambda: NOW,
    )
    producer = ExperimentFamilyPreparer(
        store=store, profiles=(profile,), phase_provider=provider, **kwargs
    )
    return store, producer, data, profile, reads, kwargs


def test_exp11_actual_phase_materialization_and_exact_identity_replay(preparation) -> None:
    store, producer, data, profile, reads, _ = preparation
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=210),
        body_hash="a" * 64,
        request=search(),
        registered_at=NOW,
    )
    ready = producer(record)
    assert ready.state == "ready" and len(reads) == 1
    assert (
        reads[0].phase == "search"
        and reads[0].window.end_date == protocol().validation_range.end_date
    )
    first = store.preparation("alice", record.family_id, 0)
    assert first.source_identity == profile.source_identity
    assert first.prepared.frozen.request.days[-1].trade_date == protocol().validation_range.end_date
    assert first.prepared.frozen.request.input_generation_id != data.template.input_generation_id
    assert producer(ready) == ready and len(reads) == 1
    assert store.preparation("alice", record.family_id, 0) == first
    other = store.begin_request(
        owner="alice",
        request_id=UUID(int=211),
        body_hash="b" * 64,
        request=search(),
        registered_at=NOW,
    )
    producer(other)
    second = store.preparation("alice", other.family_id, 0)
    assert (
        second.prepared.frozen.request.input_generation_id
        != first.prepared.frozen.request.input_generation_id
    )
    assert second.source_identity == first.source_identity


def test_exp10_actual_ordinary_guard_calls_no_protected_provider(preparation) -> None:
    store, _, data, profile, reads, kwargs = preparation
    ordinary = PortfolioRequestPreparer(
        source_provider=lambda *args: reads.append(args) or data,
        experiments=store.registry,
        protocol=protocol(),
        code_commit=profile.producer_commit,
        protected_sources=frozenset({(profile.source_key, profile.source_version)}),
        **kwargs,
    )
    with pytest.raises(PermissionError, match="protected"):
        ordinary(search().base_config)
    assert reads == []


def test_exp11_provider_whole_or_wrong_source_rejected_before_publication(preparation) -> None:
    store, producer, data, _, reads, _ = preparation
    producer.phase_provider = lambda request: reads.append(request) or data
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=212),
        body_hash="c" * 64,
        request=search(),
        registered_at=NOW,
    )
    with pytest.raises(PermissionError, match="unbounded"):
        producer(record)
    assert len(reads) == 1 and store.registry.list_family_attempts(record.family_id) == ()
    assert store.preparation("alice", record.family_id, 0) is None
    assert tuple(producer.input_root.iterdir()) == ()


def test_exp09_changed_policy_stops_unadmitted_slice_before_any_read(preparation) -> None:
    store, producer, _, _, reads, _ = preparation
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=213),
        body_hash="c" * 64,
        request=search(),
        registered_at=NOW,
    )
    store.install_policy(
        months=1, now=NOW + timedelta(seconds=1), expected_version=record.policy.version
    )
    with pytest.raises(ValueError, match="policy"):
        producer(record)
    assert not reads


def test_exp06_interruption_after_publication_recovers_exact_owned_input(
    preparation, monkeypatch
) -> None:
    store, producer, _, _, reads, _ = preparation
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=214),
        body_hash="d" * 64,
        request=search(),
        registered_at=NOW,
    )
    save = store.save_preparation

    def crash(receipt):
        raise RuntimeError("publication complete before preparation receipt")

    monkeypatch.setattr(store, "save_preparation", crash)
    with pytest.raises(RuntimeError, match="publication complete"):
        producer(record)
    reserved = store.preparation_reservation("alice", record.family_id, 0)
    assert reserved is not None and Path(reserved.source_path).exists()
    assert store.registry.list_family_attempts(record.family_id) == ()
    identity = Path(reserved.source_path).stat().st_ino
    monkeypatch.setattr(store, "save_preparation", save)
    ready = producer(record)
    assert ready.state == "ready" and len(reads) == 2
    assert Path(reserved.source_path).stat().st_ino == identity
    assert store.preparation("alice", record.family_id, 0).source_path == reserved.source_path
    assert len(tuple(producer.input_root.iterdir())) == 4


def test_exp01_original_pagecontrol_spool_lab_and_private_source_composition(
    preparation, tmp_path: Path
) -> None:
    from rquant.experiment_platform_commands import (
        RegisterExperimentFamily,
        bind_experiment_platform,
    )
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.page_control import PageControlConsumer, PageControlOutbox
    from rquant.portfolio_backtest_artifact import PortfolioResultReader

    store, producer, _, _, reads, _ = preparation
    store.install_policy(months=0, now=NOW)
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    spool = LabCommandSpool(tmp_path / "commands")
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=spool,
        experiment_registry=store.registry,
        definition_registry=producer.definitions,
        clock=lambda: NOW,
    )
    results = PortfolioResultReader(reader=reader, artifact_root=tmp_path / "artifacts")
    options = dict(
        store=store,
        commands=facade,
        prepare=producer,
        results=results,
        default_config=search().base_config,
        owners=frozenset({"alice"}),
        administrators=frozenset({"alice"}),
    )
    disabled = bind_experiment_platform(**options)
    command = RegisterExperimentFamily(
        command_id=str(UUID(int=220)), requested_at=NOW, actor_id="alice", request=search()
    )
    with pytest.raises(PermissionError, match="disabled"):
        disabled.command_backend.freeze(command)
    assert not reads
    binding = bind_experiment_platform(**options, enabled=True)
    outbox = PageControlOutbox(tmp_path / "control" / "requests.sqlite3")
    outbox.enqueue(command)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        experiment_backend=binding.command_backend,
        clock=lambda: NOW,
    )
    receipt = consumer.drain(limit=10)[0]
    assert receipt.status.value == "succeeded", receipt
    assert receipt.result["planned_count"] == 4 and len(receipt.result["job_ids"]) == 4
    assert len(spool.pending()) == 4 and len(reads) == 1
    lease = jobs.acquire_scheduler_lease(owner_id="synthetic-scheduler", lease_seconds=60, now=NOW)
    for entry in spool.pending():
        jobs.apply_command(
            entry.envelope,
            lease=lease,
            now=NOW,
            submission_authority=lambda value, at: binding.lifecycle.validate_submission(
                value, observed_at=at
            ),
        )
    assert all(
        reader.get_job(entry.envelope.command.job_id).status.value == "queued"
        for entry in spool.pending()
    )
    assert outbox.enqueue(command) == receipt
    assert consumer.drain(limit=10) == ()
    source = binding.promotion_reader(NOW)
    tables = {p.table_name: p for p in source.payload.projections}
    assert tables["experiment_attempt"].rows == ()
    assert len(tables["experiment_private_attempt"].rows) == 4
    assert tables["experiment_private_window"].rows[0]["owner"] == "alice"
    assert binding.promotion_reader(NOW) == source
