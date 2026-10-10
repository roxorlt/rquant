from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_platform import (
    ExperimentFamilyRequest,
    ExperimentPlatformStore,
    ExperimentSourceProfile,
)
from rquant.experiment_platform_commands import (
    ExperimentCommandWriter,
    ExperimentFamilyPreparer,
    RegisterExperimentFamily,
    SetExperimentNote,
    UnsealExperimentOuterTest,
)
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.page_control import PageControlConsumer, PageControlOutbox
from rquant.portfolio_backtest_source import PortfolioSourceData
from rquant.research_catalog import ResearchCatalog
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from tests.unit.test_experiment_platform import NOW, registry, search
from tests.unit.test_experiment_platform_results import complete_family as complete_family
from tests.unit.test_portfolio_backtest import _CODES, _request


@pytest.mark.parametrize("marker_committed", [False, True])
def test_exp13_original_note_recovers_lost_effect_marker_without_another_note_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, marker_committed: bool
) -> None:
    store = ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    family = store.begin_request(
        owner="alice",
        request_id=UUID(int=1118),
        body_hash="a" * 64,
        request=search(),
        registered_at=NOW,
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(jobs.path),
        spool=LabCommandSpool(tmp_path / "spool"),
        experiment_registry=store.registry,
        clock=lambda: NOW,
    )
    # Note mutation uses only the installed clock; it performs no phase preparation.
    prepare = cast(ExperimentFamilyPreparer, SimpleNamespace(clock=lambda: NOW))
    writer = ExperimentCommandWriter(
        store=store,
        commands=facade,
        prepare=prepare,
        enabled=True,
        owners=frozenset({"alice"}),
    )
    outbox = PageControlOutbox(tmp_path / "control" / "requests.sqlite3")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        experiment_backend=writer,
        clock=lambda: NOW,
    )
    command = SetExperimentNote(
        command_id=str(UUID(int=1119)),
        requested_at=NOW,
        actor_id="alice",
        family_id=family.family_id,
        expected_version=0,
        text="原备注只保存一版",
    )
    record = outbox.record_started_effect_result
    calls = []

    def lost(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            if marker_committed:
                record(*args, **kwargs)
            raise OSError("synthetic interrupted effect marker receipt")
        return record(*args, **kwargs)

    monkeypatch.setattr(outbox, "record_started_effect_result", lost)
    outbox.enqueue(command)
    first = consumer.drain(limit=1)[0]
    assert first.status.value == "pending", first
    with store.registry._connect() as connection:
        assert connection.execute("SELECT version FROM experiment_note").fetchall()[0][0] == 1
    recovered = consumer.drain(limit=1)[0]
    assert recovered.status.value == "succeeded" and recovered.result["version"] == 1
    assert outbox.enqueue(command) == recovered and consumer.drain(limit=1) == ()
    with store.registry._connect() as connection:
        assert connection.execute("SELECT version FROM experiment_note").fetchall()[0][0] == 1
    assert facade.spool.pending() == ()


@pytest.mark.parametrize("failure", [PermissionError, ValueError])
def test_exp13_definite_preparation_rejection_stays_failed(
    tmp_path: Path, failure: type[Exception]
) -> None:
    store = ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(jobs.path),
        spool=LabCommandSpool(tmp_path / "spool"),
        experiment_registry=store.registry,
        clock=lambda: NOW,
    )
    calls = []

    class RejectedPreparation:
        clock = staticmethod(lambda: NOW)

        def template_baseline(
            self, owner: str, request: ExperimentFamilyRequest
        ) -> None:
            return None

        def __call__(self, record, *, grant=None):
            calls.append(record)
            raise failure("synthetic definite invalid source")

    writer = ExperimentCommandWriter(
        store=store,
        commands=facade,
        prepare=cast(ExperimentFamilyPreparer, RejectedPreparation()),
        enabled=True,
        owners=frozenset({"alice"}),
    )
    outbox = PageControlOutbox(tmp_path / "control" / "requests.sqlite3")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        experiment_backend=writer,
        clock=lambda: NOW,
    )
    command = RegisterExperimentFamily(
        command_id=str(UUID(int=1117)),
        requested_at=NOW,
        actor_id="alice",
        request=search(),
    )
    outbox.enqueue(command)
    rejected = consumer.drain(limit=1)[0]
    assert rejected.status.value == "failed"
    assert outbox.enqueue(command) == rejected and consumer.drain(limit=1) == ()
    assert len(calls) == 1 and facade.spool.pending() == ()


def test_exp06_exp13_exp23_original_control_recovers_outer_read_failure_and_note_receipt(
    complete_family, tmp_path: Path
) -> None:
    store, projection, results, _ = complete_family
    observed = NOW + timedelta(seconds=4)
    family = projection.snapshot(observed).families[0]
    chosen = projection.snapshot(observed).attempts[0]
    original = store.preparation("alice", family.family_id, 0).prepared
    raw = _request((_CODES[0], _CODES[1], _CODES[0], _CODES[1], _CODES[0], _CODES[1]))
    data = PortfolioSourceData(
        source_key=original.frozen.config.source_key,
        source_version=1,
        template=raw,
        sources=original.frozen.sources,
        benchmarks={
            "000300.SH": tuple((day, 100.0 + i) for i, day in enumerate(raw.calendar.dates))
        },
    )
    profile = ExperimentSourceProfile(
        source_key=data.source_key,
        source_version=1,
        label="合成完整历史",
        producer_commit=raw.producer_commit,
        sources=data.sources,
        calendar=raw.calendar,
        coverage=family.request.protocol.train_range.model_copy(
            update={
                "end_date": family.request.protocol.frozen_outer_test_range.end_date,
            }
        ),
        latest_complete=raw.days[-1].trade_date,
        phase_slice_available=True,
    )
    reads = []

    def provider(request):
        grants = store.list_outer_grants("alice")
        assert len(grants) == 1 and request.outer_grant_id == grants[0].grant_id
        assert request.window == grants[0].outer_range
        reads.append(request)
        if len(reads) == 1:
            raise RuntimeError("synthetic transient read failure after committed grant")
        dates = raw.calendar.dates
        baseline = dates[dates.index(request.window.start_date) - 1]
        return data.model_copy(
            update={
                "material_hash": None,
                "template": raw.model_copy(
                    update={
                        "days": tuple(
                            day
                            for day in raw.days
                            if request.window.start_date
                            <= day.trade_date
                            <= request.window.end_date
                        )
                    }
                ),
                "benchmarks": {
                    key: tuple(
                        (day, value)
                        for day, value in rows
                        if baseline <= day <= request.window.end_date
                    )
                    for key, rows in data.benchmarks.items()
                },
            }
        )

    definitions = ImmutableDefinitionRegistry(
        tmp_path / "definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=raw.producer_commit
        ).trusted_executable_registry(),
    )
    inputs = tmp_path / "outer-inputs"
    inputs.mkdir(mode=0o700)
    producer = ExperimentFamilyPreparer(
        store=store,
        definitions=definitions,
        profiles=(profile,),
        phase_provider=provider,
        metadata_store_factory=lambda: DuckDBStore(tmp_path / "outer-metadata.duckdb"),
        catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
        lake_root=tmp_path / "lake",
        input_root=inputs,
        clock=lambda: observed,
    )
    facade = LabCommandSubmissionFacade(
        reader=results.reader,
        spool=LabCommandSpool(tmp_path / "outer-commands"),
        experiment_registry=store.registry,
        definition_registry=definitions,
        clock=lambda: observed,
    )
    writer = ExperimentCommandWriter(
        store=store,
        commands=facade,
        prepare=producer,
        results=results,
        private_authority=projection.authority,
        enabled=True,
        owners=frozenset({"alice"}),
        administrators=frozenset({"alice"}),
    )
    outbox = PageControlOutbox(tmp_path / "outer-control" / "requests.sqlite3")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        experiment_backend=writer,
        clock=lambda: observed,
    )
    command = UnsealExperimentOuterTest(
        command_id=str(UUID(int=1120)),
        requested_at=observed,
        actor_id="alice",
        family_id=family.family_id,
        experiment_id=chosen.attempt.spec.experiment_id,
        result_hash=chosen.result_hash,
        confirmed=True,
    )
    outbox.enqueue(command)
    first = consumer.drain(limit=1)[0]
    print(
        "OUTER_PREPARATION_FACT",
        {
            "status": first.status.value,
            "grants": len(store.list_outer_grants("alice")),
            "reads": len(reads),
            "lab_commands": len(facade.spool.pending()),
        },
    )
    assert first.status.value == "pending", first
    grant = store.list_outer_grants("alice")[0]
    assert len(reads) == 1 and facade.spool.pending() == ()
    restored = consumer.drain(limit=1)[0]
    print("OUTER_RECOVERED_RECEIPT", restored.model_dump_json())
    assert (
        restored.status.value == "succeeded" and restored.result["status"] == "outer_admitted"
    ), restored
    assert restored.result["planned_count"] == len(facade.spool.pending()) == 1
    assert store.list_outer_grants("alice") == (grant,) and len(reads) == 2
    assert outbox.enqueue(command) == restored and consumer.drain(limit=1) == ()
    assert len(store.registry.list_family_attempts(family.family_id)) == 4
    outer = store.get_request("alice", UUID(command.command_id))
    assert outer.phase == "outer" and len(outer.actual_configurations) == 1
    prepared = store.preparation("alice", outer.family_id, 0).prepared
    assert tuple(day.trade_date for day in prepared.frozen.request.days) == tuple(
        day
        for day in raw.calendar.dates
        if grant.outer_range.start_date <= day <= grant.outer_range.end_date
    )
    assert prepared.frozen.request.initial_cash == original.frozen.request.initial_cash
    note = SetExperimentNote(
        command_id=str(UUID(int=1121)),
        requested_at=observed,
        actor_id="alice",
        family_id=family.family_id,
        expected_version=0,
        text="合成实际持久备注",
    )
    outbox.enqueue(note)
    receipt = consumer.drain(limit=1)[0]
    assert receipt.status.value == "succeeded" and receipt.result["version"] == 1
    assert outbox.enqueue(note) == receipt and consumer.drain(limit=1) == ()
    saved = next(
        f for f in projection.snapshot(observed).families if f.family_id == family.family_id
    ).note
    assert saved.version == 1 and saved.text == note.text
