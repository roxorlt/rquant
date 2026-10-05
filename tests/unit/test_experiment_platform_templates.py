"""M8 uses original immutable template definitions and complete planned slots."""

from __future__ import annotations

import importlib.util
from uuid import UUID

import pytest

from rquant.experiment_platform import ExperimentSearchRequest
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_source import StrategySourceCatalog
from tests.unit.test_experiment_platform import NOW, search
from tests.unit.test_experiment_platform_flow import preparation as preparation
from tests.unit.test_strategy_authoring import draft


def original_stores(tmp_path, *, producer_commit="0" * 40):
    base = StrategyAuthoringStore(
        tmp_path / "base.sqlite",
        definition_root=tmp_path / "base-definitions",
        producer_commit=producer_commit,
        clock=lambda: NOW,
    )
    private = StrategyAuthoringStore(
        tmp_path / "private.sqlite",
        definition_root=tmp_path / "private-definitions",
        producer_commit=producer_commit,
        clock=lambda: NOW,
    )
    base.initialize()
    private.initialize()
    request = search()
    rules = draft().rules.model_copy(
        update={
            "weight_rule": request.base_config.weight_rule,
            "rebalance_rule": request.base_config.rebalance_rule,
        }
    )
    catalog = StrategySourceCatalog(
        owner_id="alice", generation_id="generation-a", pools=(), signals=()
    )
    saved = base.save(
        draft().model_copy(update={"rules": rules}), owner_id="alice", catalog=catalog
    )
    request = ExperimentSearchRequest.model_validate(
        request.model_dump(mode="python")
        | {"template": {"strategy_id": saved.strategy_id, "head": saved.head}}
    )
    return base, private, catalog, saved, request


def binding_for(tmp_path, *, producer_commit="0" * 40):
    name = "rquant.experiment_platform_templates"
    assert importlib.util.find_spec(name), "original template binding is missing"
    from rquant.experiment_platform_templates import ExperimentTemplateBinding

    base, private, catalog, saved, request = original_stores(
        tmp_path, producer_commit=producer_commit
    )
    binding = ExperimentTemplateBinding(
        original=base,
        expected_original_identity=base.identity(),
        private=private,
        expected_private_identity=private.identity(),
        catalogs=(catalog,),
    )
    return binding, base, private, saved, request


def admitted(tmp_path, binding, request):
    from rquant.experiment_platform import ExperimentPlatformStore
    from tests.unit.test_experiment_platform import registry

    store = ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    baseline = binding.baseline(owner="alice", request=request)
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=880),
        body_hash="b" * 64,
        request=request,
        registered_at=NOW,
        template_baseline=baseline,
    )
    return store, record


def test_c5t01_original_baseline_and_complete_slots_precede_any_save(tmp_path):
    binding, base, private, saved, request = binding_for(tmp_path)
    store, record = admitted(tmp_path, binding, request)
    slots = store.template_slots("alice", record.family_id)
    assert len(slots) == len(record.actual_configurations) == 4
    assert len({slot.request.command_id for slot in slots}) == 4
    assert all(slot.state == "pending" for slot in slots)
    assert private.list_current(owner_id="alice") == ()
    assert store.registry.list_family_attempts(record.family_id) == ()
    binding.prepare_definitions(store, record)
    assert len(private.list_current(owner_id="alice")) == 4
    assert base.get_current(saved.strategy_id, owner_id="alice").head == saved.head
    assert store.registry.list_family_attempts(record.family_id) == ()
    for slot, cfg in zip(
        store.template_slots("alice", record.family_id), record.actual_configurations, strict=True
    ):
        assert slot.receipt.head.version == 1 and slot.receipt.strategy_id != saved.strategy_id
        metadata = private.get_version(slot.receipt.strategy_id, 1, owner_id="alice")
        assert metadata.rules.weight_rule == cfg.weight_rule
        assert metadata.rules.rebalance_rule == cfg.rebalance_rule
        assert metadata.rules.entry == record.template_baseline.version.rules.entry
        assert metadata.rules.exit == record.template_baseline.version.rules.exit
        assert metadata.rules.index_filter == record.template_baseline.version.rules.index_filter
    with pytest.raises(PermissionError):
        binding.baseline(owner="bob", request=request)
    forged = request.model_copy(
        update={
            "template": request.template.model_copy(
                update={"head": saved.head.model_copy(update={"record_hash": "f" * 64})}
            )
        }
    )
    with pytest.raises(ValueError, match="head"):
        binding.baseline(owner="alice", request=forged)


@pytest.mark.parametrize("after", ["accept", "complete_save"])
def test_c5t02_lost_original_receipt_recovers_same_ids_and_complete_n(tmp_path, monkeypatch, after):
    binding, base, private, saved, request = binding_for(tmp_path)
    store, record = admitted(tmp_path, binding, request)
    original = getattr(private, after)
    seen = []

    def interrupted(*args, **kwargs):
        result = original(*args, **kwargs)
        seen.append(result.strategy_id)
        raise RuntimeError("original result persisted, reply lost")

    monkeypatch.setattr(private, after, interrupted)
    with pytest.raises(RuntimeError, match="reply lost"):
        binding.prepare_definitions(store, record)
    assert len(store.template_slots("alice", record.family_id)) == 4
    assert store.registry.list_family_attempts(record.family_id) == ()
    monkeypatch.setattr(private, after, original)
    first = binding.prepare_definitions(store, record)
    assert len(first) == 4 and first[0].strategy_id == seen[0]
    assert binding.prepare_definitions(store, record) == first
    assert len(private.list_current(owner_id="alice")) == 4
    assert all(len(private.versions(v.strategy_id, owner_id="alice")) == 1 for v in first)
    assert len(base.list_current(owner_id="alice")) == 1


def configure_template(preparation, tmp_path):
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_template_execution import TemplateEntryEvidence
    from rquant.strategy_template_source import StrategyTemplateSourceData, TemplateRawDay

    store, producer, data, profile, reads, _ = preparation
    sources = data.sources.model_copy(
        update={"ranking_hash": canonical_sha256(tuple(d.ranking for d in data.template.days))}
    )
    profile = type(profile).model_validate(
        profile.model_dump(mode="python") | {"sources": sources, "source_identity": None}
    )
    producer.profiles = (profile,)
    binding, base, private, saved, request = binding_for(
        tmp_path, producer_commit=profile.producer_commit
    )

    def provider(read, version):
        source = producer.phase_provider(read)
        source = type(source).model_validate(
            source.model_dump(mode="python") | {"sources": sources, "material_hash": None}
        )
        return StrategyTemplateSourceData(
            owner_id="alice",
            catalog=binding._catalog("alice"),
            portfolio=source,
            days=tuple(
                TemplateRawDay(
                    trade_date=d.trade_date,
                    entry=TemplateEntryEvidence(
                        observed_at=d.ranking.observed_at,
                        source_hash=d.ranking.source_identity,
                        rows=tuple({"ts_code": i.ts_code, "is_st": False} for i in d.instruments),
                    ),
                )
                for d in source.template.days
            ),
        )

    binding.phase_provider = provider
    producer.template_binding = binding
    return store, producer, profile, binding, request


def ready_template(preparation, tmp_path):
    store, producer, profile, binding, request = configure_template(preparation, tmp_path)
    reads = preparation[4]
    baseline = binding.baseline(owner="alice", request=request)
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=900),
        body_hash="c" * 64,
        request=request,
        registered_at=NOW,
        template_baseline=baseline,
    )
    ready = producer(record)
    assert ready.state == "ready" and len(reads) == 1
    attempts = store.registry.list_family_attempts(record.family_id)
    assert len(attempts) == 4 and all(
        a.spec.hypothesis_family == record.family_id for a in attempts
    )
    assert not any(a.spec.hypothesis_family.startswith("template:") for a in attempts)
    for index, cfg in enumerate(record.actual_configurations):
        receipt = store.preparation("alice", record.family_id, index)
        prepared = receipt.prepared
        assert prepared.frozen.request.weight_rule == cfg.weight_rule
        assert prepared.frozen.request.rebalance_rule == cfg.rebalance_rule
        assert tuple(d.trade_date for d in prepared.frozen.days) == tuple(
            d for d in profile.calendar.dates if cfg.start_date <= d <= cfg.end_date
        )
        assert prepared.spec.parameters.strategy_name == prepared.registration.logical_id
        assert prepared.registration.logical_id != "portfolio_backtest"
        assert prepared.spec.parameters.end_date == request.protocol.validation_range.end_date
    assert producer(ready) == ready and len(reads) == 1
    return store, producer, profile, binding, request, ready


def test_c5t05_c5t06_original_producer_prepares_complete_family_and_exact_phase(
    preparation, tmp_path
):
    store, producer, profile, binding, request, record = ready_template(preparation, tmp_path)
    import rquant.experiment_platform_templates as templates

    assert hasattr(templates, "ExperimentTemplateRuntimeBinding"), (
        "exact private original runtime selector is missing"
    )
    runtime = templates.ExperimentTemplateRuntimeBinding(store=store, binding=binding)
    receipt = store.preparation("alice", record.family_id, 0)
    from rquant.experiment_platform import stable_experiment_job

    job_id = stable_experiment_job("alice", record.request_id, 0)
    assert runtime.directory_for_job(job_id, receipt.prepared.spec) is binding.directory
    for forged_job in (UUID(int=1), stable_experiment_job("bob", record.request_id, 0)):
        with pytest.raises(PermissionError):
            runtime.directory_for_job(forged_job, receipt.prepared.spec)
    forged = store.preparation("alice", record.family_id, 1).prepared.spec
    with pytest.raises(PermissionError):
        runtime.directory_for_job(job_id, forged)


def test_c5t07_full_original_result_rejects_changed_definition_source_calendar_cost(
    preparation, tmp_path
):
    import rquant.strategy_template_artifact as artifact
    from rquant.strategy_template_run import StrategyTemplateResult, execute_strategy_template_input

    store, producer, profile, binding, request, record = ready_template(preparation, tmp_path)
    prepared = store.preparation("alice", record.family_id, 0).prepared
    (tmp_path / "result-ledger").mkdir(mode=0o700)
    result = execute_strategy_template_input(
        prepared.frozen, research_root=tmp_path / "result-ledger"
    )
    assert result.status == "complete" and len(result.days) == 4
    assert hasattr(artifact, "bind_complete_template_result"), (
        "full original template payload binding is missing"
    )
    artifact.bind_complete_template_result(prepared, result)
    from rquant.experiment_platform import stable_experiment_job
    from rquant.experiment_platform_projection import (
        ExperimentAttemptFact,
        ExperimentFamilyFact,
        ExperimentSearchContext,
    )
    from rquant.experiment_platform_template_evidence import result_from_template
    from rquant.strategy_template_artifact import TemplateReadResult

    job_id = stable_experiment_job("alice", record.request_id, 0)
    attempt = store.registry.get_attempt(prepared.formal_plan.spec.experiment_id)
    fact = ExperimentAttemptFact(
        owner="alice",
        family_id=record.family_id,
        index=0,
        configuration=record.actual_configurations[0],
        attempt=attempt,
        child=store.child(job_id),
        input_hash=prepared.frozen.input_hash,
        source_identity=profile.source_identity,
        spec_hash=prepared.spec.spec_hash,
        manifest_hash="c" * 64,
        result_hash="b" * 64,
    )
    family = ExperimentFamilyFact(
        owner="alice",
        family_id=record.family_id,
        request_id=record.request_id,
        name=request.name,
        request=ExperimentSearchContext.from_request(request),
        registered_at=record.registered_at,
        policy=record.policy,
        phase="search",
        parent_family_id=None,
        planned_count=4,
        potential_count=4,
        search_count=4,
    )
    # Only these wrapper artifact identities are synthetic. Native sealed evidence is root-run.
    projected = result_from_template(
        fact,
        family,
        TemplateReadResult(
            job_id=job_id,
            spec_hash=fact.spec_hash,
            manifest_hash=fact.manifest_hash,
            result_hash=fact.result_hash,
            result=result,
        ),
        prepared,
    )
    assert len(projected.curves) == 4 and tuple(len(p.curves) for p in projected.phases) == (2, 2)
    assert tuple(p.nav for p in projected.curves) == tuple(
        float(day.normalized_nav) for day in result.days
    )
    assert tuple(p for phase in projected.phases for p in phase.curves) == projected.curves
    assert projected.template.strategy_id != record.template_baseline.version.strategy_id
    assert projected.template.rules == prepared.frozen.rules
    assert projected.configuration.to_domain() == fact.configuration
    for field, value in (
        ("owner_id", "bob"),
        ("strategy_id", "template_" + "f" * 32),
        ("version", 2),
        ("definition_fingerprint", "f" * 64),
        ("definition_record_hash", "f" * 64),
        ("input_hash", "f" * 64),
        ("calendar_source_identity", "f" * 64),
        ("cost_spec_id", "f" * 64),
    ):
        if getattr(result, field) == value:
            value = "e" * 64
        forged = StrategyTemplateResult.model_validate(
            result.model_dump(mode="python") | {field: value, "content_hash": None}
        )
        with pytest.raises(ValueError):
            artifact.bind_complete_template_result(prepared, forged)
    with pytest.raises(ValueError):
        artifact.bind_complete_template_result(
            prepared,
            StrategyTemplateResult.model_validate(
                result.model_dump(mode="python") | {"days": result.days[1:], "content_hash": None}
            ),
        )
    with pytest.raises(ValueError):
        StrategyTemplateResult.model_validate(
            result.model_dump(mode="python") | {"content_hash": "f" * 64}
        )


def test_c5t01_template_selection_is_typed_and_cannot_accept_owner_or_path(tmp_path) -> None:
    request = search()
    target = StrategyAuthoringStore(
        tmp_path / "authoring.sqlite",
        definition_root=tmp_path / "definitions",
        producer_commit="0" * 40,
        clock=lambda: NOW,
    )
    target.initialize()
    catalog = StrategySourceCatalog(
        owner_id="alice", generation_id="generation-a", pools=(), signals=()
    )
    saved = target.save(
        draft().model_copy(
            update={
                "rules": draft().rules.model_copy(
                    update={
                        "weight_rule": request.base_config.weight_rule,
                        "rebalance_rule": request.base_config.rebalance_rule,
                    }
                )
            }
        ),
        owner_id="alice",
        catalog=catalog,
    )
    selection = {"strategy_id": saved.strategy_id, "head": saved.head}
    checked = ExperimentSearchRequest.model_validate(
        request.model_dump(mode="python") | {"template": selection}
    )
    assert checked.template.strategy_id == saved.strategy_id
    assert checked.template.head == saved.head
    for extra in ({"owner_id": "bob"}, {"path": str(target.path)}, {"adapter_id": "fake"}):
        with pytest.raises(ValueError):
            ExperimentSearchRequest.model_validate(
                request.model_dump(mode="python") | {"template": selection | extra}
            )
