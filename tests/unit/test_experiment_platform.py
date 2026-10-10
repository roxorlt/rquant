from __future__ import annotations

import importlib
import importlib.util
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentRegistryReadonlyReader,
)
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol
from tests.unit.test_backtest_platform import config

NOW = datetime(2026, 10, 5, 8, tzinfo=UTC)


def platform():
    name = "rquant.experiment_platform"
    assert importlib.util.find_spec(name), "formal experiment platform is missing"
    return importlib.import_module(name)


def protocol() -> PortfolioExperimentProtocol:
    return PortfolioExperimentProtocol(
        train_range=DateRange(start_date=date(2026, 8, 10), end_date=date(2026, 8, 11)),
        validation_range=DateRange(start_date=date(2026, 8, 12), end_date=date(2026, 8, 13)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 8, 14), end_date=date(2026, 8, 17)),
    )


def search(**changed):
    p = platform()
    return p.ExperimentSearchRequest(
        name="仓位实验",
        base_config=config(),
        protocol=protocol(),
        dimensions=(
            p.SearchDimension(parameter="weight_rule.max_positions", values=(1, 2)),
            p.SearchDimension(parameter="weight_rule.cash_reserve", values=("0.25", "0.50")),
        ),
        **changed,
    )


def registry(tmp_path: Path) -> ExperimentRegistry:
    root = tmp_path / "registry"
    root.mkdir(mode=0o700)
    return ExperimentRegistry(root / "experiments.sqlite3", managed_trust_root=root)


def test_exp02_actual_array_is_ordered_complete_and_random_without_replacement() -> None:
    p = platform()
    request = search()
    grid = p.enumerate_search(request)
    assert len(grid) == 4
    assert [item.weight_rule.max_positions for item in grid] == [1, 2, 1, 2]
    assert [item.weight_rule.cash_reserve for item in grid] == list(
        map(Decimal, (".25", ".25", ".50", ".50"))
    )
    random = p.enumerate_search(search(method="random", random_count=3, seed=42))
    assert random == p.enumerate_search(search(method="random", random_count=3, seed=42))
    assert len({value.config_hash for value in random}) == 3
    assert all(value in grid for value in random)


@pytest.mark.parametrize(
    "parameter,values",
    [
        ("weight_rule.max_positions", (True, 2)),
        ("weight_rule.max_positions", (1, 1)),
        ("weight_rule.max_positions", (501,)),
        ("weight_rule.cash_reserve", ("NaN",)),
        ("weight_rule.cash_reserve", ("Infinity",)),
        ("weight_rule.min_target_amount", ("0.001",)),
        ("source_version", (1,)),
        ("rebalance_rule.every_n_days", (0,)),
    ],
)
def test_exp02_illegal_dimensions_reject_before_publication(parameter, values) -> None:
    with pytest.raises((ValidationError, ValueError)):
        platform().SearchDimension(parameter=parameter, values=values)


def test_exp02_grid_capacity_is_not_silently_trimmed() -> None:
    p = platform()
    raw = search().model_dump(mode="python")
    raw["dimensions"] = (
        p.SearchDimension(parameter="weight_rule.max_positions", values=tuple(range(1, 17))),
        p.SearchDimension(
            parameter="weight_rule.cash_reserve", values=tuple(Decimal(i) / 100 for i in range(16))
        ),
    )
    with pytest.raises(ValueError, match="64"):
        p.enumerate_search(p.ExperimentSearchRequest.model_validate(raw))


def test_exp03_ranges_are_strict_and_cover_actual_calendar() -> None:
    p = platform()
    request = search()
    dates = tuple(date(2026, 8, day) for day in (7, 10, 11, 12, 13, 14, 17, 18))
    p.validate_experiment_dates(request, calendar=dates, latest_complete=date(2026, 10, 2))
    with pytest.raises(ValueError, match="calendar"):
        p.validate_experiment_dates(
            request, calendar=dates[:-3] + dates[-2:], latest_complete=date(2026, 10, 2)
        )
    raw = request.model_dump(mode="python")
    raw["protocol"]["validation_range"]["start_date"] = date(2026, 8, 11)
    with pytest.raises(ValueError, match="overlap|order"):
        p.ExperimentSearchRequest.model_validate(raw)


def test_exp09_server_policy_month_end_leap_and_last_complete_calendar() -> None:
    p = platform()
    assert p.month_cutoff(date(2024, 3, 31), 1) == date(2024, 2, 29)
    assert p.month_cutoff(date(2026, 3, 31), 1) == date(2026, 2, 28)
    assert p.holdout_cutoff(NOW, 0, calendar=(date(2026, 10, 2), date(2026, 10, 5))) == date(
        2026, 10, 5
    )
    assert p.holdout_cutoff(
        NOW.replace(hour=1), 0, calendar=(date(2026, 10, 2), date(2026, 10, 5))
    ) == date(2026, 10, 2)
    with pytest.raises(ValueError):
        p.HoldoutPolicy(version=1, months=37, updated_at=NOW)


def test_exp07_original_request_lookup_precedes_changed_configuration(tmp_path: Path) -> None:
    p = platform()
    store = p.ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    first = store.begin_request(
        owner="alice",
        request_id=UUID(int=1),
        body_hash="a" * 64,
        request=search(),
        registered_at=NOW,
    )
    again = store.begin_request(
        owner="alice",
        request_id=UUID(int=1),
        body_hash="a" * 64,
        request=search(method="random", random_count=1),
        registered_at=NOW + timedelta(days=1),
    )
    assert first == again
    with pytest.raises(ValueError, match="content"):
        store.begin_request(
            owner="alice",
            request_id=UUID(int=1),
            body_hash="b" * 64,
            request=search(),
            registered_at=NOW,
        )
    with pytest.raises(PermissionError):
        store.get_request("bob", UUID(int=1))


def test_exp12_grant_commits_before_read_and_overlap_does_not_reset(tmp_path: Path) -> None:
    p = platform()
    store = p.ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    request = store.begin_request(
        owner="alice",
        request_id=UUID(int=2),
        body_hash="c" * 64,
        request=search(),
        registered_at=NOW,
    )
    # Only terminal searches with an exact actual candidate may consume outer input.
    with pytest.raises(ValueError, match="terminal|ready"):
        store.admit_outer(
            owner="alice",
            family_id=request.family_id,
            request_id=UUID(int=3),
            experiment_id="a" * 64,
            now=NOW,
        )
    assert store.list_outer_grants("alice") == ()


def test_exp23_note_cas_original_request_and_owner(tmp_path: Path) -> None:
    p = platform()
    store = p.ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=4),
        body_hash="d" * 64,
        request=search(),
        registered_at=NOW,
    )
    note = store.set_note(
        owner="alice",
        family_id=record.family_id,
        request_id=UUID(int=5),
        expected_version=0,
        text="只调整仓位",
        now=NOW,
    )
    assert note.version == 1
    assert (
        store.set_note(
            owner="alice",
            family_id=record.family_id,
            request_id=UUID(int=5),
            expected_version=0,
            text="只调整仓位",
            now=NOW,
        )
        == note
    )
    with pytest.raises(ValueError, match="version"):
        store.set_note(
            owner="alice",
            family_id=record.family_id,
            request_id=UUID(int=6),
            expected_version=0,
            text="保留草稿",
            now=NOW,
        )
    with pytest.raises(PermissionError):
        store.set_note(
            owner="bob",
            family_id=record.family_id,
            request_id=UUID(int=7),
            expected_version=1,
            text="不应写入",
            now=NOW,
        )


def test_exp22_reader_has_explicit_legacy_collection_before_limit(tmp_path: Path) -> None:
    r = registry(tmp_path)
    reader = ExperimentRegistryReadonlyReader(r.path, managed_trust_root=r.path.parent)
    read = getattr(reader, "read_legacy_shared_serving_snapshot", None)
    assert callable(read), "old shared projection needs an explicit filtered collection"
    result = read(observed_at=NOW)
    assert result.attempts == () and not result.truncated


_DEFAULT_FAMILY_REQUEST = UUID(int=100)


def prepared_family(
    tmp_path: Path, *, owner: str = "alice", request_id: UUID = _DEFAULT_FAMILY_REQUEST
):
    from rquant import portfolio_backtest_source as sources
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandEnvelope
    from rquant.portfolio_backtest_definition import bootstrap_portfolio_definition
    from rquant.portfolio_backtest_source import (
        PortfolioSourceData,
        freeze_portfolio_config,
        publish_portfolio_input,
    )
    from rquant.research_catalog import ResearchCatalog
    from rquant.runtime_definition_bootstrap import (
        bootstrap_builtin_definitions,
        plan_builtin_definitions,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
    from tests.unit.test_portfolio_backtest import _CODES, _request

    build = getattr(sources, "build_portfolio_plan", None)
    assert callable(build), "complete family needs a pure original C6 plan builder"
    p = platform()
    r = registry(tmp_path)
    store = p.ExperimentPlatformStore(r, activate_private_schema=True)
    record = store.begin_request(
        owner=owner, request_id=request_id, body_hash="a" * 64, request=search(), registered_at=NOW
    )
    raw = _request((_CODES[0], _CODES[1], _CODES[0], _CODES[1], _CODES[0], _CODES[1]))
    data = PortfolioSourceData(
        source_key="verified-screen",
        source_version=1,
        template=raw,
        sources=__import__("tests.unit.test_backtest_platform", fromlist=["frozen"])
        .frozen()
        .sources,
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
    children = []
    catalog = ResearchCatalog(tmp_path / "catalog.duckdb")
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        for index, cfg in enumerate(record.actual_configurations):
            frozen = freeze_portfolio_config(data, cfg)
            published = publish_portfolio_input(
                frozen,
                metadata_store=metadata,
                source_path=tmp_path / f"input-{index}.duckdb",
                catalog=catalog,
                lake_root=tmp_path / "lake",
                now=NOW,
            )
            prepared = build(
                frozen,
                published,
                definitions=definitions,
                protocol=record.request.protocol,
                now=NOW,
                deadline=NOW + timedelta(hours=1),
                random_seed=record.request.seed,
                family_id=record.family_id,
                hypothesis_variant=f"configuration-{index}",
            )
            from rquant.experiment_platform_commands import _input_digest

            identity, digest = _input_digest(tmp_path / f"input-{index}.duckdb")
            store.save_preparation(
                p.ExperimentPreparationReceipt(
                    owner=owner,
                    family_id=record.family_id,
                    index=index,
                    source_identity="1" * 64,
                    source_path=str(tmp_path / f"input-{index}.duckdb"),
                    file_identity=identity,
                    file_sha256=digest,
                    prepared=prepared,
                )
            )
            envelope = LabCommandEnvelope(
                request_id=LabCommandSubmissionFacade._request_id(
                    p.stable_experiment_interaction(owner, request_id, index)
                ),
                command=prepared.submission(
                    job_id=p.stable_experiment_job(owner, request_id, index)
                ).command,
            )
            children.append(
                p.ExperimentChildRegistration(
                    config=cfg,
                    plan=prepared.formal_plan,
                    published=published,
                    intent=LabCommandSubmissionFacade._experiment_submission_intent(envelope),
                )
            )
    return store, record, tuple(children), definitions


def test_exp05_whole_original_family_rolls_back_all_four_tables(tmp_path: Path) -> None:
    store, record, children, _ = prepared_family(tmp_path)

    def interrupt(_):
        raise RuntimeError("after first actual child insert")

    with pytest.raises(RuntimeError, match="actual child"):
        store.register_family_submission(
            owner=record.owner, request_id=record.request_id, children=children, fault=interrupt
        )
    with store.registry._connect() as connection:
        for table in (
            "hypothesis_family_manifest",
            "formal_experiment_plan",
            "experiment_attempt",
            "experiment_submission_outbox",
            "experiment_private_family",
            "experiment_child_admission",
        ):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    assert store.get_request(record.owner, record.request_id).state == "preparing"
    ready = store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    assert ready.state == "ready"
    assert (
        store.register_family_submission(
            owner=record.owner, request_id=record.request_id, children=children
        )
        == ready
    )
    assert len(store.registry.list_family_attempts(record.family_id)) == 4
    assert len(store.registry.list_pending_submissions()) == 4


def test_exp08_cancel_before_admission_never_publishes_and_preserves_original_intents(
    tmp_path: Path,
) -> None:
    store, record, children, _ = prepared_family(tmp_path)
    store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    cancelled = store.cancel_family(
        owner=record.owner, family_id=record.family_id, request_id=UUID(int=101), now=NOW
    )
    assert all(
        c.cancel_state == "before_publication" and c.publish_grant_seq is None for c in cancelled
    )
    with pytest.raises(PermissionError):
        store.admit_publication(children[0].intent, now=NOW)
    assert all(
        a.status.value == "cancelled" for a in store.registry.list_family_attempts(record.family_id)
    )
    assert (
        store.registry.get_submission_intent_for_job(children[0].intent.job_id)
        == children[0].intent
    )


def test_exp08_publish_grant_is_persistent_and_cancel_stays_pending_when_job_not_found(
    tmp_path: Path,
) -> None:
    store, record, children, _ = prepared_family(tmp_path)
    store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    granted = store.admit_publication(children[0].intent, now=NOW)
    cancelled = store.cancel_family(
        owner=record.owner, family_id=record.family_id, request_id=UUID(int=102), now=NOW
    )
    pending = next(c for c in cancelled if c.job_id == granted.job_id)
    assert pending.cancel_state == "pending"
    assert pending.publish_grant_seq < pending.cancel_seq
    assert store.registry.get_attempt(granted.experiment_id).status.value == "registered"
    assert (
        store.admit_publication(children[0].intent, now=NOW).publish_grant_seq
        == granted.publish_grant_seq
    )
    store.validate_publication(children[0].intent)


def test_exp08_original_lab_callback_rejects_no_grant_and_accepts_same_pending_child(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore

    store, record, children, definitions = prepared_family(tmp_path)
    store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(jobs.path),
        spool=LabCommandSpool(tmp_path / "commands"),
        experiment_registry=store.registry,
        definition_registry=definitions,
        clock=lambda: NOW,
    )
    envelope = LabCommandEnvelope.model_validate_json(children[0].intent.envelope_json)
    with pytest.raises(PermissionError, match="grant"):
        facade.validate_prepared_experiment_submission(envelope, observed_at=NOW)
    lease = jobs.acquire_scheduler_lease(owner_id="synthetic-scheduler", lease_seconds=60, now=NOW)
    with pytest.raises(PermissionError, match="grant"):
        jobs.apply_command(
            envelope,
            lease=lease,
            now=NOW,
            submission_authority=lambda value, at: facade.validate_prepared_experiment_submission(
                value, observed_at=at
            ),
        )
    assert facade.reader.get_job(envelope.command.job_id) is None
    store.admit_publication(children[0].intent, now=NOW)
    store.cancel_family(
        owner=record.owner, family_id=record.family_id, request_id=UUID(int=103), now=NOW
    )
    facade.validate_prepared_experiment_submission(envelope, observed_at=NOW)
    receipt = facade.recover_pending_experiment_submissions()
    assert len(receipt) == 1
    assert len(facade.spool.pending()) == 1
    applied = jobs.apply_command(
        envelope,
        lease=lease,
        now=NOW,
        submission_authority=lambda value, at: facade.validate_prepared_experiment_submission(
            value, observed_at=at
        ),
    )
    assert (
        jobs.apply_command(
            envelope,
            lease=lease,
            now=NOW,
            submission_authority=lambda value, at: facade.validate_prepared_experiment_submission(
                value, observed_at=at
            ),
        )
        == applied
    )
    assert facade.reader.get_job(envelope.command.job_id).status.value == "queued"
    recover = getattr(facade, "recover_private_experiment_cancellations", None)
    assert callable(recover), "pending cancellation must use the original command chain"
    recover(observed_at=NOW)
    current = store.child(envelope.command.job_id)
    assert current.cancel_state == "pending" and len(current.cancel_request_chain) == 1
    cancel_entry = next(
        e
        for e in facade.spool.pending()
        if e.envelope.request_id == current.cancel_request_chain[0]
    )
    jobs.apply_command(cancel_entry.envelope, lease=lease, now=NOW)
    assert facade.reader.get_job(envelope.command.job_id).status.value == "cancelled"
    recover(observed_at=NOW)
    assert store.child(envelope.command.job_id).cancel_state == "confirmed"
    assert store.registry.get_attempt(current.experiment_id).status.value == "cancelled"


def test_exp10_protected_c6_source_rejects_ordinary_consumer_before_provider_call(
    tmp_path: Path,
) -> None:
    from inspect import signature

    from rquant.portfolio_backtest_source import PortfolioRequestPreparer

    assert "protected_sources" in signature(PortfolioRequestPreparer).parameters, (
        "ordinary C6 needs an exact protected source boundary without changing its default"
    )


def test_exp11_formal_preparer_requires_phase_slice_not_a_whole_source() -> None:
    import importlib.util

    assert importlib.util.find_spec("rquant.experiment_platform_commands"), (
        "formal phase preparation and exact outer admission are missing"
    )
    module = importlib.import_module("rquant.experiment_platform_commands")
    assert callable(getattr(module, "ExperimentFamilyPreparer", None))


def test_exp22_private_web_contract_is_strict_and_exportable(tmp_path: Path) -> None:
    from rquant.web.app import create_app
    from rquant.web.experiment_platform_models import (
        ExperimentEditableRequest,
        ExperimentSearchWrite,
    )
    from rquant.web.settings import WebSettings

    value = ExperimentEditableRequest.model_validate(search().model_dump(mode="python"))
    assert value.base_config.to_domain() == search().base_config
    with pytest.raises(ValidationError):
        ExperimentSearchWrite.model_validate(
            {
                "command_id": str(UUID(int=910)),
                "requested_at": NOW,
                "request": value,
                "actor_id": "bob",
            }
        )
    schema = create_app(WebSettings(serving_root=tmp_path / "serving"), background=False).openapi()
    for path in (
        "/experiments/mine",
        "/experiments/capabilities",
        "/experiments/compare",
        "/experiments/families/{family_id}",
        "/experiments/results/{experiment_id}",
        "/experiments/families/{family_id}/heatmap",
        "/experiments/results/{experiment_id}/statistics",
        "/experiments/commands",
    ):
        assert "/api/v1" + path in schema["paths"]


@pytest.mark.parametrize("view", (False, True))
def test_exp23_legacy_artifact_denies_private_family_before_payload(view: bool) -> None:
    from types import SimpleNamespace

    from rquant.portfolio_backtest_artifact import PortfolioResultReader

    raw_calls = []

    class Preview:
        def preview(self, *args, **kwargs):
            raw_calls.append(args)
            raise AssertionError("private raw preview called")

    job = SimpleNamespace(
        spec=SimpleNamespace(
            parameters=SimpleNamespace(strategy_name="portfolio_backtest"),
            experiment=SimpleNamespace(
                spec=SimpleNamespace(hypothesis_family="experiment-search:" + "a" * 64)
            ),
        )
    )
    reader = PortfolioResultReader.__new__(PortfolioResultReader)
    reader.reader = SimpleNamespace(
        get_artifact_preview_authority=lambda _: SimpleNamespace(job=job)
    )
    reader.previews = reader.views = Preview()
    with pytest.raises(PermissionError):
        if view:
            reader.read_view(
                UUID(int=1),
                table_name="portfolio_nav",
                expected_result_hash="a" * 64,
                offset=0,
                limit=1,
            )
        else:
            reader.read(UUID(int=1))
    assert raw_calls == []
