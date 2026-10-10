"""Trusted stored fields enter the same previous-session formula and journal path."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest

from rquant.factor.capability import HISTORICAL_DAILY_V1, historical_daily_capabilities
from rquant.factor.definition import build_factor_definition
from rquant.factor.draft import FactorSaveDraft, build_draft_definition
from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
from rquant.factor.run_configuration import PreparedFactorMetadata
from rquant.factor.stream_adapter import FactorStreamAdapterRequest
from rquant.factor.stream_runner import run_factor_stream_research_with_decay
from rquant.factor.time_series import DecisionTime
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_factor_daily_feature_source import _AS_OF, _FIRST, _source
from tests.unit.test_factor_stream_adapter import _at, _pools


def _request(
    source: object, prepared: object, *, expression: str = "ma5 + turnover_rate + close"
) -> object:
    capability = historical_daily_capabilities(daily_features_available=True)
    definition = build_factor_definition(
        factor_id="stored_daily",
        name_zh="库存日线",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression=expression,
        feature_catalog=capability.feature_catalog(),
    )
    fields = tuple(
        c
        for c in definition.dependency_columns
        if c not in HISTORICAL_DAILY_V1.feature_catalog().columns
    )
    if not fields:
        definition = build_factor_definition(
            **{
                **definition.model_dump(exclude={"max_history_window"}),
                "feature_catalog": HISTORICAL_DAILY_V1.feature_catalog(),
            }
        )
    days = tuple(_FIRST + timedelta(days=i) for i in (1, 2, 3, 4))
    formula = FactorFormulaStreamRequest(
        definition=definition,
        computation_stock_codes=prepared.receipt.request.scope.stock_codes,
        trading_days=days,
        decision_times=tuple(DecisionTime(trade_date=d, decision_at=_at(d, 9, 25)) for d in days),
        as_of=_AS_OF,
        selection="all",
        sources=FactorFormulaStreamSources(
            source_mode="historical_retrospective",
            feature_source_id=prepared.snapshot.snapshot_id,
            feature_source_sha256=prepared.binding.binding_hash,
            security_source_id="synthetic-security-archive",
            security_source_sha256="b" * 64,
            daily_features=source.select(fields) if fields else None,
        ),
    )
    return FactorStreamAdapterRequest(
        source=prepared.admission_request,
        scope_content_hash=prepared.scope_content_hash,
        formula=formula,
        evaluation_days=days[1:3],
        holding_sessions=1,
        daily_feature_source=source if fields else None,
    )


def test_trusted_catalog_admits_stored_fields_but_keeps_old_draft_bytes() -> None:
    draft = FactorSaveDraft(
        generation_id="b" * 64,
        command_id="stored-test",
        requested_at=_AS_OF,
        mode="create",
        factor_id=None,
        expected_head=None,
        name_zh="库存指标",
        category="technical",
        direction="higher_is_better",
        expression="ts_mean(close, 2)",
    )
    old = build_draft_definition(draft, authenticated_actor_id="alice")
    caps = historical_daily_capabilities(daily_features_available=True)
    assert caps.version == "daily_stored_v1" and len(caps.fields) == 22
    stored_fields = caps.fields[len(HISTORICAL_DAILY_V1.fields) :]
    assert all(field.tracking_supported is True for field in stored_fields)
    assert all(field.tracking_unavailable_reason_zh is None for field in stored_fields)
    assert caps.model_dump()["fields"][:6] == HISTORICAL_DAILY_V1.model_dump()["fields"]
    assert len(HISTORICAL_DAILY_V1.fields) == 6 and HISTORICAL_DAILY_V1.version == "daily_v1"
    new = build_draft_definition(draft, authenticated_actor_id="alice", capabilities=caps)
    assert canonical_json_bytes(old.model_dump(mode="json")) == canonical_json_bytes(
        new.model_dump(mode="json")
    )
    saved = build_draft_definition(
        draft.model_copy(update={"expression": "ref(ma5, 1) + turnover_rate"}),
        authenticated_actor_id="alice",
        capabilities=caps,
    )
    assert saved.dependency_columns == ("ma5", "turnover_rate")
    assert saved.max_history_window == 2
    with pytest.raises(ValueError):
        build_draft_definition(
            draft.model_copy(update={"expression": "ma5"}), authenticated_actor_id="alice"
        )


def test_previous_sse_panel_mixed_raw_and_stored_values_match_manual_golden(tmp_path: Path) -> None:
    source, prepared, root = _source(tmp_path, count=12, days=8)
    request = _request(source, prepared)
    batches = []
    result = run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=root,
        universe_requests=_pools(request),
        batch_observer=batches.append,
    )
    completed = result.research.adapter_completion
    assert completed.daily_features == request.formula.sources.daily_features
    assert completed.daily_feature_read_query_count == 4
    for batch in batches:
        offset = (batch.universe.trade_date - _FIRST).days - 1
        assert batch.daily_features.panel_date == _FIRST + timedelta(days=offset)
        assert batch.daily_features.trade_date == batch.universe.trade_date
        values = {v.stock_code: v.value for v in batch.factor_values}
        for number in range(1, 13):
            code = f"{number:06d}.SZ"
            stored_ma = float(number + offset * 10)
            turnover = 0.5387 if number == 1 else stored_ma
            # Synthetic original SQL daily_bar: 10 + date_offset + code_number / 100.
            assert values[code] == pytest.approx(stored_ma + turnover + 10 + offset + number / 100)
        assert batch.daily_features.sha256 == canonical_sha256(
            batch.daily_features.model_dump(exclude={"sha256"})
        )
    assert result.research.formula_completion.processed_days == 4
    assert not list(root.glob(".daily-feature-*"))


def test_new_definition_requires_exact_selected_source_and_same_raw_binding(tmp_path: Path) -> None:
    source, prepared, _ = _source(tmp_path, count=12, days=8)
    request = _request(source, prepared)
    missing = {
        **request.formula.model_dump(),
        "sources": {**request.formula.sources.model_dump(), "daily_features": None},
    }
    with pytest.raises(ValueError):
        FactorFormulaStreamRequest.model_validate(missing)
    wrong = {
        **request.model_dump(),
        "formula": {
            **request.formula.model_dump(),
            "sources": {
                **request.formula.sources.model_dump(),
                "daily_features": source.select(("ma5",)),
            },
        },
    }
    with pytest.raises(ValueError):
        FactorStreamAdapterRequest.model_validate(wrong)
    source_fields = source.model_dump(exclude={"sha256"})
    source_fields["prepared_source_sha256"] = "d" * 64
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource

    wrong_source = FactorDailyFeatureSource(**source_fields, sha256=canonical_sha256(source_fields))
    with pytest.raises(ValueError):
        wrong_source.require_prepared(prepared)


def _configured(
    tmp_path: Path,
    *,
    expression: str = "ref(ma5, 1) + turnover_rate + close",
    count: int = 12,
    mutate: Callable | None = None,
) -> tuple:
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.registry import (
        FactorDefinitionRegistry,
        FactorHeadRef,
        SaveFactorDefinitionRequest,
    )
    from rquant.factor.run_configuration import (
        FactorRunConfiguration,
        FactorRunMemberBinding,
        save_factor_daily_feature_source,
        save_factor_prepared_source,
        save_factor_run_configuration,
    )
    from rquant.factor.run_request import FactorRunParameters, FactorRunRequest
    from tests.unit.test_factor_member_archive import _private
    from tests.unit.test_factor_member_stream import _archive

    source, prepared, lake = _source(tmp_path, count=count, days=8, mutate=mutate)
    request = _request(source, prepared, expression=expression)
    members, archive, request = _archive(tmp_path, request, _pools(request))
    registry = FactorDefinitionRegistry(tmp_path / "registry.sqlite")
    registry_identity = registry.initialize()
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="save-stored", definition=request.formula.definition, expected_head=None
        ),
        expected_identity=registry_identity,
    )
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    identity = ledger.initialize()
    root = _private(tmp_path / "configuration")
    config = FactorRunConfiguration(
        enabled=True,
        factor_run_users=("alice",),
        registry_identity=registry_identity,
        ledger_identity=identity,
        prepared_source=save_factor_prepared_source(root, prepared),
        lake_root=lake,
        member_root=members,
        artifact_root=_private(tmp_path / "artifacts"),
        members=(FactorRunMemberBinding(selection="all", archive=archive),),
        code_revision="b" * 40,
        daily_feature_source=save_factor_daily_feature_source(root, source),
    )
    reference = save_factor_run_configuration(root, config)
    browser = FactorRunRequest(
        command_id="00000000-0000-4000-8000-000000000016",
        requested_at=_AS_OF,
        serving_generation_id="c" * 64,
        parameters=FactorRunParameters(
            factor_id=request.formula.definition.factor_id,
            expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
            selection="all",
            start_date=_FIRST + timedelta(days=2),
            end_date=_FIRST + timedelta(days=3),
            holding_sessions=1,
            extended_statistics=True,
        ),
    )
    return root, reference, browser, prepared, config, source


def test_real_configured_worker_journal_replay_serving_and_web_use_exact_stored_source(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        run_configured_factor_worker,
    )
    from rquant.factor.run_plan import compile_factor_run_plan
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts

    root, reference, browser, prepared, config, source = _configured(tmp_path)
    assert len(FactorRunPageControlBackend(root, reference).capabilities().fields) == 22
    with open_factor_run_configuration(root, reference) as loaded:
        assert loaded.daily_features == source
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    plan = compile_factor_run_plan(
        root,
        reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    assert plan.spec.daily_feature_lake_root == config.lake_root
    from tests.unit.test_factor_neutralization_jobs import _service

    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    service, outbox = _service(tmp_path, backend, browser)
    receipt = service._submit_trusted_factor_run(
        browser,
        authenticated_actor_id="alice",
        verified_registry_instance_id=config.registry_identity.instance_id,
    )
    assert receipt.result["status"] == "queued"
    job = outbox.lookup_factor_run_command(browser, authenticated_actor_id="alice")[0]
    assert job.spec == plan.spec
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "succeeded", result
    job = result.record
    verified = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    )
    for day in verified.full.journal.days:
        batch = FactorDailyStreamBatch.model_validate_json(
            (config.artifact_root / day.artifact.filename).read_bytes()
        )
        assert (
            batch.daily_features.sources == plan.spec.adapter_request.formula.sources.daily_features
        )
        assert tuple(f.column for f in batch.daily_features.sources.fields) == (
            "ma5",
            "turnover_rate",
        )
        assert batch.daily_features.panel_date == day.trade_date - timedelta(days=1)
    assert (
        verified.display.daily_features == plan.spec.adapter_request.formula.sources.daily_features
    )
    assert verified.display.daily_features.value_semantics == "stored_not_recomputed"
    projections = project_factor_result_projections(
        config.ledger_identity, config.artifact_root, available_at=_AS_OF
    )
    projection = validate_factor_result_projections({p.table_name: p for p in projections})
    assert projection.displays[0].daily_features == verified.display.daily_features
    from tempfile import TemporaryDirectory

    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import TEST_PROXY_PROOF, ResearcherTestClient
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

    # Serving has its own observation time; the underlying job remains the synthetic frozen run.
    projections = project_factor_result_projections(
        config.ledger_identity, config.artifact_root, available_at=FIXTURE_BUILT_AT
    )
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline", factor_result_projections=projections)
    with TemporaryDirectory(
        prefix=".daily-features-web-", dir=Path(__file__).resolve().parents[2]
    ) as private:
        proof = Path(private) / "proof"
        proof.write_text(TEST_PROXY_PROOF)
        proof.chmod(0o400)
        app = create_app(
            WebSettings(
                serving_root=serving,
                stale_after_seconds=1e9,
                ingress_socket_path=Path(private) / "web.sock",
                proxy_proof_file=proof,
            ),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
            background=False,
        )
        with ResearcherTestClient(app) as client:
            index = client.get("/api/v1/factors/results")
            assert index.status_code == 200, index.text
            detail = client.get(
                "/api/v1/factors/results/" + job.job_id,
                params={"generation_id": index.json()["serving"]["generation_id"]},
            )
            assert detail.status_code == 200, detail.text
            data = detail.json()["data"]["research"]
            assert data["daily_features"] == verified.display.daily_features.model_dump(mode="json")
            assert len(data["daily_feature_coverage_days"]) == 2
            assert str(config.lake_root) not in detail.text
    assert not Path(private).exists()
    assert ledger.get(job.job_id).status == "succeeded"
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_unused_optional_source_keeps_old_spec_and_configuration_default_bytes(
    tmp_path: Path,
) -> None:
    from rquant.factor.run_configuration import (
        FactorRunConfiguration,
        save_factor_run_configuration,
    )
    from rquant.factor.run_plan import compile_factor_run_plan

    root, reference, browser, _, config, _ = _configured(tmp_path, expression="close")
    # Replace the deliberately expanded helper catalog with the original saved definition contract.
    from rquant.factor.registry import (
        FactorDefinitionRegistry,
        FactorHeadRef,
        SaveFactorDefinitionRequest,
    )

    registry = FactorDefinitionRegistry(Path(config.registry_identity.path))
    record = registry.get_head(
        browser.parameters.factor_id, expected_identity=config.registry_identity
    )
    old_def = build_factor_definition(
        **{
            **record.definition.model_dump(exclude={"max_history_window"}),
            "version": 2,
            "feature_catalog": HISTORICAL_DAILY_V1.feature_catalog(),
        }
    )
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="save-old",
            definition=old_def,
            expected_head=browser.parameters.expected_head,
        ),
        expected_identity=config.registry_identity,
    )
    browser = browser.model_copy(
        update={
            "parameters": browser.parameters.model_copy(
                update={
                    "expected_head": FactorHeadRef(version=2, content_sha256=saved.content_sha256)
                }
            )
        }
    )
    no_source = FactorRunConfiguration.model_validate(
        {**config.model_dump(), "daily_feature_source": None}
    )
    old_reference = save_factor_run_configuration(root, no_source)
    old = compile_factor_run_plan(
        root,
        old_reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    same = compile_factor_run_plan(
        root,
        reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    assert old.spec.model_dump(mode="json") == same.spec.model_dump(mode="json")
    assert old.spec.spec_sha256 == same.spec.spec_sha256
    assert "daily_feature_source" not in no_source.model_dump()
    assert "daily_feature_lake_root" not in old.spec.model_dump()
    assert "daily_features" not in old.spec.adapter_request.formula.sources.model_dump()


def test_stored_only_expression_has_no_hidden_bar_dependency_and_returns_missing_stay_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.research_snapshot import FactorStreamReadLease

    def mutate(connection: object) -> None:
        connection.execute("DELETE FROM daily_bar WHERE ts_code='000001.SZ'")

    source, prepared, lake = _source(tmp_path, count=8, days=8, mutate=mutate)
    request = _request(source, prepared, expression="ma5")
    queries = []
    query = FactorStreamReadLease.query_daily_bars

    def track(self: object, argument: object) -> object:
        queries.append(argument)
        return query(self, argument)

    monkeypatch.setattr(FactorStreamReadLease, "query_daily_bars", track)
    batches = []
    result = run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=lake,
        universe_requests=_pools(request),
        batch_observer=batches.append,
    )
    assert all(q.start_date in request.evaluation_days for q in queries)
    for batch in batches:
        assert batch.factor_values[0].value is not None
        assert batch.forward_returns[0].value is None
        assert [v.value for v in batch.factor_values[1:]] == [
            None,
            None,
            None,
            None,
            -1.5,
            0.0,
            None,
        ]
        assert batch.daily_features.counts[0].missing == 1
        assert batch.daily_features.counts[0].null == 1
        assert batch.daily_features.counts[0].non_finite == 3
    assert all(day.coverage.valid_count == 2 for day in result.research.statistics.days)


def test_actual_sse_previous_panel_and_ref_warmup_do_not_jump_missing_observation(
    tmp_path: Path,
) -> None:
    def mutate(connection: object) -> None:
        connection.execute(
            "UPDATE trade_calendar SET is_open=FALSE WHERE cal_date=?", [_FIRST + timedelta(days=2)]
        )
        connection.execute(
            "UPDATE trade_calendar SET pretrade_date=? WHERE cal_date=?",
            [_FIRST + timedelta(days=1), _FIRST + timedelta(days=3)],
        )
        connection.execute(
            "DELETE FROM daily_indicator WHERE ts_code='000002.SZ' AND trade_date=?",
            [_FIRST + timedelta(days=1)],
        )

    source, prepared, lake = _source(tmp_path, count=12, days=8, mutate=mutate)
    request = _request(source, prepared, expression="ma5 + ref(ma5, 1)")
    days = tuple(_FIRST + timedelta(days=i) for i in (1, 3, 4, 5))
    formula = FactorFormulaStreamRequest.model_validate(
        {
            **request.formula.model_dump(),
            "trading_days": days,
            "decision_times": tuple(
                DecisionTime(trade_date=d, decision_at=_at(d, 9, 25)) for d in days
            ),
        }
    )
    request = FactorStreamAdapterRequest.model_validate(
        {**request.model_dump(), "formula": formula, "evaluation_days": days[1:3]}
    )
    batches = []
    result = run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=lake,
        universe_requests=_pools(request),
        batch_observer=batches.append,
    )
    assert batches[0].factor_values[0].value == 12.0
    assert batches[1].factor_values[0].value == 42.0
    assert all(batch.factor_values[1].value is None for batch in batches)
    assert tuple(f.panel_date for f in result.research.adapter_completion.feature_days) == tuple(
        _FIRST + timedelta(days=i) for i in (0, 1, 3, 4)
    )


def test_stored_source_natural_tail_cannot_publish_completed_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureReadLease
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        run_configured_factor_worker,
    )
    from rquant.factor.run_plan import compile_factor_run_plan

    root, reference, browser, _, config, source = _configured(tmp_path)
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    plan = compile_factor_run_plan(
        root,
        reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    job = ledger.submit(browser.command_id, plan.spec)
    query, leases = FactorDailyFeatureReadLease.query, []

    def changed(lease: object, argument: object) -> object:
        batch = query(lease, argument)
        leases.append(lease)
        if argument.trade_date == _FIRST + timedelta(days=2):
            path = config.lake_root / source.tables[0].artifact.relative_path
            path.write_bytes(path.read_bytes() + b"changed at natural tail")
        return batch

    monkeypatch.setattr(FactorDailyFeatureReadLease, "query", changed)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "failed" and ledger.get(job.job_id).completion is None
    assert leases and all(lease.closed and not lease._private_root.exists() for lease in leases)
    assert not list(config.artifact_root.glob("factor-stream-full-*"))
    assert not list(config.artifact_root.glob("factor-stream-display-*"))


def test_rehashed_wrong_stored_journal_value_is_refused_against_sealed_original(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureInput
    from rquant.factor.daily_stream import FactorDailyStreamBatch, evaluate_factor_daily_stream
    from rquant.factor.decay_stream import FactorICDecayStream
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        run_configured_factor_worker,
    )
    from rquant.factor.run_plan import compile_factor_run_plan
    from rquant.factor.stream_job_artifact import (
        FactorStreamJournalDay,
        publish_stream_artifact,
        verify_factor_stream_artifacts,
    )
    from tests.unit.test_factor_stream_job_artifact import _hashed, _replace_full

    root, reference, browser, _, config, _ = _configured(tmp_path)
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    plan = compile_factor_run_plan(
        root,
        reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    ledger.submit(browser.command_id, plan.spec)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "succeeded", result
    full = verify_factor_stream_artifacts(
        plan.spec, result.record.completion, config.artifact_root, config.member_root
    ).full
    batches = [
        FactorDailyStreamBatch.model_validate_json(
            (config.artifact_root / d.artifact.filename).read_bytes()
        )
        for d in full.journal.days
    ]
    original = batches[0].daily_features
    row = original.rows[0]
    row = row.model_copy(
        update={"values": (row.values[0].model_copy(update={"value": 999.0}), *row.values[1:])}
    )
    fields = {**original.model_dump(exclude={"sha256"}), "rows": (row, *original.rows[1:])}
    changed = FactorDailyFeatureInput(**fields, sha256=canonical_sha256(fields))
    batches[0] = FactorDailyStreamBatch.model_validate(
        {**batches[0].model_dump(), "daily_features": changed}
    )
    research = full.result.research.research
    stats = evaluate_factor_daily_stream(research.statistics.request, iter(batches))
    decay = FactorICDecayStream(full.result.research.decay.request)
    try:
        for batch in batches:
            decay.consume(batch)
        decayed = decay.finish(stats)
    finally:
        decay.close()
    adapter = research.adapter_completion
    # The completed feature chain stays original; only the journal metadata and outputs are forged.
    adapter_fields = adapter.model_dump(exclude={"input_sha256", "sha256"})
    adapter_fields["return_days"] = tuple(
        d.model_copy(update={"statistics_batch_sha256": canonical_sha256(b)})
        for d, b in zip(adapter.return_days, batches, strict=True)
    )
    adapter = type(adapter)(
        **adapter_fields,
        input_sha256=canonical_sha256(adapter_fields),
        sha256=canonical_sha256(
            {**adapter_fields, "input_sha256": canonical_sha256(adapter_fields)}
        ),
    )
    research = _hashed(research, adapter_completion=adapter, statistics=stats)
    changed_result = _hashed(
        full.result, research=_hashed(full.result.research, research=research, decay=decayed)
    )
    days = tuple(
        FactorStreamJournalDay(
            trade_date=b.universe.trade_date,
            artifact=publish_stream_artifact(config.artifact_root, "journal-day", b),
            batch_sha256=canonical_sha256(b),
        )
        for b in batches
    )
    forged = _replace_full(
        full, config.artifact_root, result.record.completion, result=changed_result, days=days
    )
    with pytest.raises(ValueError, match="stored daily values"):
        verify_factor_stream_artifacts(plan.spec, forged, config.artifact_root, config.member_root)


def test_stored_day_inputs_release_before_next_adapter_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import weakref

    import rquant.factor.stream_adapter as module
    from rquant.factor.daily_feature_source import FactorDailyFeatureInput
    from rquant.factor.stream_adapter import FactorStreamAdapter

    source, prepared, root = _source(tmp_path, count=12, days=8)
    request = _request(source, prepared, expression="ts_mean(ma5, 2)")
    read, advance = module.read_factor_daily_feature_input, FactorStreamAdapter.__next__
    refs = []

    def observed(*args: object, **kwargs: object) -> FactorDailyFeatureInput:
        assert all(r() is None for r in refs)
        original = read(*args, **kwargs)
        refs.append(weakref.ref(original))
        return original

    def advanced(adapter: object) -> object:
        assert adapter._current_daily_features is None
        return advance(adapter)

    monkeypatch.setattr(module, "read_factor_daily_feature_input", observed)
    monkeypatch.setattr(FactorStreamAdapter, "__next__", advanced)
    run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=root,
        universe_requests=_pools(request),
    )
    assert len(refs) == 4 and all(r() is None for r in refs)
    assert not list(root.glob(".daily-feature-*"))


def test_explicit_offline_cli_seals_and_binds_real_synthetic_tables(tmp_path: Path) -> None:
    import json

    from rquant.factor.run_configuration import (
        FactorRunFileReference,
        open_factor_run_configuration,
    )
    from rquant.factor.run_entry import main

    root, reference, _, prepared, config, _ = _configured(tmp_path)
    path = root / "prepared-input.json"
    path.write_bytes(canonical_json_bytes(prepared.model_dump(mode="json", round_trip=True)))
    path.chmod(0o600)
    from unittest.mock import patch

    with patch("builtins.print") as output:
        assert (
            main(
                [
                    "seal-daily-features",
                    "--root",
                    str(root),
                    "--prepared-source",
                    str(path),
                    "--lake-root",
                    str(config.lake_root),
                    "--reference",
                    reference.model_dump_json(),
                ]
            )
            == 0
        )
    receipt = json.loads(output.call_args.args[0])
    actual = FactorRunFileReference.model_validate_json(
        json.dumps(receipt["configuration_reference"])
    )
    with open_factor_run_configuration(root, actual) as loaded:
        assert loaded.daily_features.prepared_source_sha256 == prepared.sha256
        assert loaded.daily_features.generation == prepared.receipt.generation
        assert (
            loaded.configuration.daily_feature_source.filename
            == receipt["source_reference"]["filename"]
        )


def _tracking_control(tmp_path: Path, *, expression: str, source_present: bool) -> tuple:
    from rquant.factor.run_configuration import save_factor_run_configuration
    from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root, reference, run, _, config, _ = _configured(tmp_path, expression=expression)
    if not source_present:
        reference = save_factor_run_configuration(
            root, config.model_copy(update={"daily_feature_source": None})
        )
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    identity = store.initialize()
    backend = FactorTrackingPageControlBackend(
        root, reference, identity, enabled=True, tracking_users=frozenset({"alice"})
    )
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path,
        log_dir=tmp_path,
        factor_tracking_backend=backend,
        clock=lambda: _AS_OF,
    )
    body = FactorTrackingRequest(
        command_id="stored-start",
        requested_at=run.requested_at,
        serving_generation_id=run.serving_generation_id,
        factor_id=run.parameters.factor_id,
        tracked=True,
        expected_head=run.parameters.expected_head,
    )
    return (
        PageControlService(outbox=outbox, consumer=consumer),
        backend,
        outbox,
        store,
        identity,
        body,
    )


def test_tracking_stored_missing_source_is_refused_before_enqueue_but_existing_state_can_cancel(
    tmp_path: Path,
) -> None:
    for source_present in (False,):
        directory = tmp_path / str(source_present)
        directory.mkdir(mode=0o700)
        service, backend, outbox, store, identity, body = _tracking_control(
            directory, expression="ma5", source_present=source_present
        )
        with pytest.raises(ValueError, match="缺少可核验的库存日线事实"):
            service._submit_trusted_factor_tracking(
                body,
                authenticated_actor_id="alice",
                verified_registry_instance_id=backend.registry_identity.instance_id,
            )
        assert outbox.receipt(body.command_id) is None
        assert store.get(body.factor_id, expected_identity=identity) is None
        # An older accepted state must remain cancellable when its source is absent.
        previous = store.set_tracked(
            body.model_copy(update={"command_id": "prior-state"}),
            actor_id="alice",
            expected_identity=identity,
            registry_identity=backend.registry_identity,
        )
        cancelled = service._submit_trusted_factor_tracking(
            body.model_copy(
                update={
                    "command_id": "stored-cancel",
                    "tracked": False,
                    "expected_tracking_generation": previous.tracking_generation,
                }
            ),
            authenticated_actor_id="alice",
            verified_registry_instance_id=backend.registry_identity.instance_id,
        )
        assert cancelled.status.value == "succeeded" and not cancelled.result["tracked"]


def test_tracking_old_fields_and_context_operators_keep_start_and_cancel_contract(
    tmp_path: Path,
) -> None:
    from rquant.factor.registry import (
        FactorDefinitionRegistry,
        FactorHeadRef,
        SaveFactorDefinitionRequest,
    )

    for number, expression in enumerate(
        ("close", "industry_neutralize(close) + size_neutralize(close)")
    ):
        directory = tmp_path / str(number)
        directory.mkdir(mode=0o700)
        service, backend, _, store, identity, body = _tracking_control(
            directory, expression="close", source_present=False
        )
        if expression != "close":
            registry = FactorDefinitionRegistry(Path(backend.registry_identity.path))
            record = registry.get_head(body.factor_id, expected_identity=backend.registry_identity)
            definition = build_factor_definition(
                **{
                    **record.definition.model_dump(exclude={"max_history_window"}),
                    "version": 2,
                    "expression": expression,
                    "feature_catalog": record.definition.feature_catalog,
                }
            )
            saved = registry.save(
                SaveFactorDefinitionRequest(
                    command_id="context-update",
                    definition=definition,
                    expected_head=body.expected_head,
                ),
                expected_identity=backend.registry_identity,
            )
            body = body.model_copy(
                update={
                    "expected_head": FactorHeadRef(version=2, content_sha256=saved.content_sha256)
                }
            )
        started = service._submit_trusted_factor_tracking(
            body,
            authenticated_actor_id="alice",
            verified_registry_instance_id=backend.registry_identity.instance_id,
        )
        assert started.status.value == "succeeded" and started.result["tracked"]
        cancelled = service._submit_trusted_factor_tracking(
            body.model_copy(
                update={
                    "command_id": "old-cancel",
                    "tracked": False,
                    "expected_tracking_generation": started.result["tracking_generation"],
                }
            ),
            authenticated_actor_id="alice",
            verified_registry_instance_id=backend.registry_identity.instance_id,
        )
        assert cancelled.status.value == "succeeded" and not cancelled.result["tracked"]
        assert store.get(body.factor_id, expected_identity=identity).status == "not_tracked"
