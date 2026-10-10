"""Mixed daily sources use the existing job, replay, and tracking paths."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from tests.unit.test_factor_stock_feature_source import _AS_OF, _FIRST, _module, _prepared, _raw

_EXPRESSION = (
    "ma5 / ma60 + price_position_90d_pct / 100 + ma_alignment + price_percentile_250d "
    "+ ts_mean(accum_ad_flow_20d_pct, 2) / 100"
)


def _configuration(tmp_path: Path, *, expression: str = _EXPRESSION, end: int = 260) -> tuple:
    m = _module()
    from rquant.factor.capability import HISTORICAL_DAILY_V1, historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
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
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.technical_history_source import (
        FactorTechnicalHistoryPrepareRequest,
        prepare_factor_technical_history_source,
    )
    from rquant.factor.time_series import DecisionTime
    from tests.unit.test_factor_member_archive import _private
    from tests.unit.test_factor_member_stream import _archive
    from tests.unit.test_factor_stream_adapter import _at, _pools

    path = _raw(tmp_path, count=12)
    prepared = _prepared(tmp_path, path, count=12, end=end)
    base = prepare_factor_technical_history_source(
        FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    source = m.prepare_factor_stock_feature_source(
        m.FactorStockFeaturePrepareRequest(prepared_source=prepared, base_daily_source=base),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=10),
    )
    caps = historical_daily_capabilities(
        daily_features_available=True,
        technical_history_available=True,
        stock_features_available=True,
        stock_base_daily_available=True,
    )
    catalog = (
        HISTORICAL_DAILY_V1.feature_catalog() if expression == "close" else caps.feature_catalog()
    )
    definition = build_factor_definition(
        factor_id="stock_daily",
        name_zh="日线选股特征",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression=expression,
        feature_catalog=catalog,
    )
    columns = tuple(
        c
        for c in definition.dependency_columns
        if c not in HISTORICAL_DAILY_V1.feature_catalog().columns
    )
    days = prepared.receipt.calendar_open_days[1:]
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
            daily_features=source.select(columns) if columns else None,
        ),
    )
    adapter = FactorStreamAdapterRequest(
        source=prepared.admission_request,
        scope_content_hash=prepared.scope_content_hash,
        formula=formula,
        evaluation_days=days[1:4],
        holding_sessions=1,
        daily_feature_source=source if columns else None,
    )
    members, archive, _ = _archive(tmp_path, adapter, _pools(adapter))
    registry = FactorDefinitionRegistry(tmp_path / "registry.sqlite")
    registry_identity = registry.initialize()
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="save-stock", definition=definition, expected_head=None
        ),
        expected_identity=registry_identity,
    )
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    ledger_identity = ledger.initialize()
    root = _private(tmp_path / "configuration")
    config = FactorRunConfiguration(
        enabled=True,
        factor_run_users=("alice",),
        registry_identity=registry_identity,
        ledger_identity=ledger_identity,
        prepared_source=save_factor_prepared_source(root, prepared),
        lake_root=tmp_path / "lake",
        member_root=members,
        artifact_root=_private(tmp_path / "artifacts"),
        members=(FactorRunMemberBinding(selection="all", archive=archive),),
        code_revision="b" * 40,
        daily_feature_source=save_factor_daily_feature_source(root, source),
    )
    reference = save_factor_run_configuration(root, config)
    browser = FactorRunRequest(
        command_id="00000000-0000-4000-8000-000000000030",
        requested_at=_AS_OF,
        serving_generation_id="c" * 64,
        parameters=FactorRunParameters(
            factor_id=definition.factor_id,
            expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
            selection="all",
            start_date=_FIRST + timedelta(days=257),
            end_date=_FIRST + timedelta(days=259),
            holding_sessions=1,
            extended_statistics=True,
        ),
    )
    return root, reference, browser, prepared, config, source


def _plan(root: Path, reference: object, browser: object, config: object) -> object:
    from rquant.factor.run_plan import compile_factor_run_plan

    return compile_factor_run_plan(
        root,
        reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )


def test_mixed_worker_replay_projection_and_manual_values(tmp_path: Path) -> None:
    import duckdb

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
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from rquant.stock_features import build_daily_stock_feature_result
    from rquant.storage.duckdb import DuckDBStore

    root, reference, browser, prepared, config, source = _configuration(tmp_path)
    caps = FactorRunPageControlBackend(root, reference).capabilities()
    assert caps.version == "daily_stock_v1" and len(caps.fields) == 45
    assert source.base_daily_source.schema_version == 2 and len(source.fields) == 39
    plan = _plan(root, reference, browser, config)
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    ledger.submit(browser.command_id, plan.spec)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "succeeded", result
    verified = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    )
    with (
        duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as raw,
        DuckDBStore(prepared.receipt.request.replica_path, read_only=True) as store,
    ):
        for day in verified.full.journal.days:
            batch = FactorDailyStreamBatch.model_validate_json(
                (config.artifact_root / day.artifact.filename).read_bytes()
            )
            panel = batch.daily_features.panel_date
            f = build_daily_stock_feature_result(store, "000001.SZ", panel).features
            previous = build_daily_stock_feature_result(
                store, "000001.SZ", panel - timedelta(days=1)
            ).features
            closes = [
                v[0]
                for v in raw.execute(
                    "SELECT b.close*a.adj_factor FROM daily_bar b JOIN adj_factor a "
                    "USING(ts_code,trade_date) WHERE ts_code='000001.SZ' AND trade_date<=? "
                    "ORDER BY trade_date",
                    [panel],
                ).fetchall()
            ]
            expected = (
                sum(closes[-5:]) / 5 / (sum(closes[-60:]) / 60)
                + f["price_position_90d_pct"] / 100
                + f["ma_alignment"]
                + f["price_percentile_250d"]
                + (f["accum_ad_flow_20d_pct"] + previous["accum_ad_flow_20d_pct"]) / 2 / 100
            )
            assert batch.factor_values[0].value == pytest.approx(expected, rel=1e-12)
            assert batch.daily_features.sources.technical_history is not None
            assert batch.daily_features.sources.stock_features is not None
            assert any(
                v.diagnostic is not None for r in batch.daily_features.rows for v in r.values
            )
    assert verified.display.daily_features.stock_features == source.stock_features.summary()
    projections = project_factor_result_projections(
        config.ledger_identity, config.artifact_root, available_at=_AS_OF
    )
    validated = validate_factor_result_projections({p.table_name: p for p in projections})
    assert validated.displays[0].daily_features == verified.display.daily_features
    assert any(
        c.stock_reasons for d in verified.display.daily_feature_coverage_days for c in d.counts
    )
    assert not list(config.lake_root.glob(".daily-feature-*"))


def _tracking(tmp_path: Path) -> tuple:
    from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root, reference, browser, _, config, _ = _configuration(tmp_path)
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
    service = PageControlService(outbox=outbox, consumer=consumer)
    body = FactorTrackingRequest(
        command_id="stock-start",
        requested_at=_AS_OF,
        serving_generation_id=browser.serving_generation_id,
        factor_id=browser.parameters.factor_id,
        tracked=True,
        expected_head=browser.parameters.expected_head,
    )
    return service, backend, store, identity, body, config


def test_mixed_tracking_incremental_whole_prefix_and_cancel_are_equal(tmp_path: Path) -> None:
    from rquant.factor.tracking import summarize_factor_tracking
    from rquant.factor.tracking_runner import FactorTrackingRunner, _last_run
    from rquant.factor.tracking_serving import project_factor_tracking_snapshot
    from tests.unit.test_factor_stored_tracking import _submit

    service, backend, store, identity, body, config = _tracking(tmp_path)
    assert _submit(service, backend, body).result["tracked"]
    runner = FactorTrackingRunner(backend.root, backend.reference, identity, clock=lambda: _AS_OF)
    first = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=257))
    assert first.status == "updated" and len(first.evaluation_days) == 1
    initial = store.days(body.factor_id, expected_identity=identity)
    added = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=259))
    assert added.status == "updated" and len(added.evaluation_days) == 2
    days = store.days(body.factor_id, expected_identity=identity)
    assert days[:1] == initial
    state = store.get(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        incremental = _last_run(store, connection, state.segment_id)
    assert (
        runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=259)).status
        == "waiting"
    )
    cancelled = _submit(
        service,
        backend,
        body.model_copy(
            update={
                "command_id": "stock-cancel",
                "tracked": False,
                "expected_tracking_generation": state.generation,
            }
        ),
    )
    assert not cancelled.result["tracked"]
    _submit(
        service,
        backend,
        body.model_copy(
            update={
                "command_id": "stock-rejoin",
                "expected_tracking_generation": cancelled.result["tracking_generation"],
            }
        ),
    )
    whole = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=259))
    assert (
        whole.status == "updated" and store.days(body.factor_id, expected_identity=identity) == days
    )
    state = store.get(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        full = _last_run(store, connection, state.segment_id)
    assert full.prefix == incremental.prefix
    snapshot = project_factor_tracking_snapshot(
        identity, registry_identity=backend.registry_identity, available_at=_AS_OF
    )
    assert snapshot.panels[0].summary == summarize_factor_tracking(days)
    assert not list(config.lake_root.glob(".daily-feature-*"))


def _refresh(
    root: Path, reference: object, tmp_path: Path, *, end: int = 266, policy_change: bool = False
) -> object:
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        save_factor_daily_feature_source,
        save_factor_prepared_source,
        save_factor_run_configuration,
    )
    from rquant.factor.technical_history_source import (
        FactorTechnicalHistoryPrepareRequest,
        prepare_factor_technical_history_source,
    )
    from rquant.runtime_contracts import canonical_sha256
    from tests.unit.test_factor_source_prepare import _sidecar

    m = _module()
    with open_factor_run_configuration(root, reference) as loaded:
        config, path = loaded.configuration, loaded.source.receipt.request.replica_path
    _sidecar(path)
    prepared = _prepared(tmp_path, path, count=12, end=end)
    base = prepare_factor_technical_history_source(
        FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=config.lake_root,
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    source = m.prepare_factor_stock_feature_source(
        m.FactorStockFeaturePrepareRequest(prepared_source=prepared, base_daily_source=base),
        lake_root=config.lake_root,
        now=lambda: _AS_OF + timedelta(minutes=10),
    )
    if policy_change:
        fields = source.model_dump(exclude={"sha256"})
        fields["stock_features"]["policy"]["implementation_sha256"] = "a" * 64
        source = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
    return save_factor_run_configuration(
        root,
        config.model_copy(
            update={
                "prepared_source": save_factor_prepared_source(root, prepared),
                "daily_feature_source": save_factor_daily_feature_source(root, source),
            }
        ),
    )


def test_mixed_prefix_ignores_future_tail_but_binds_window_policy(tmp_path: Path) -> None:
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.tracking_runner import read_factor_tracking_prefix

    root, reference, browser, _, config, _ = _configuration(tmp_path)
    plan = _plan(root, reference, browser, config)
    with open_factor_run_configuration(root, reference) as loaded:
        before, _ = read_factor_tracking_prefix(loaded, plan.spec)
    refreshed = _refresh(root, reference, tmp_path)
    plan = _plan(root, refreshed, browser, config)
    with open_factor_run_configuration(root, refreshed) as loaded:
        after, _ = read_factor_tracking_prefix(loaded, plan.spec)
    assert before == after
    changed = _refresh(root, refreshed, tmp_path, policy_change=True)
    plan = _plan(root, changed, browser, config)
    with open_factor_run_configuration(root, changed) as loaded:
        revised, _ = read_factor_tracking_prefix(loaded, plan.spec)
    assert revised != before


@pytest.mark.parametrize("revision", ["value", "state", "policy"])
def test_stock_consumed_revision_pauses_tracking_without_append(
    tmp_path: Path, revision: str
) -> None:
    import duckdb

    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_stored_tracking import _submit

    service, backend, store, identity, body, _ = _tracking(tmp_path)
    _submit(service, backend, body)
    result = FactorTrackingRunner(
        backend.root, backend.reference, identity, clock=lambda: _AS_OF
    ).run_history(body.factor_id, target_end=_FIRST + timedelta(days=257))
    assert result.status == "updated"
    before = store.days(body.factor_id, expected_identity=identity)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        path = loaded.source.receipt.request.replica_path
    if revision != "policy":
        with duckdb.connect(str(path)) as raw:
            if revision == "state":
                raw.execute(
                    "DELETE FROM adj_factor WHERE ts_code='000001.SZ' AND trade_date=?",
                    [_FIRST + timedelta(days=250)],
                )
            else:
                raw.execute(
                    "UPDATE daily_bar SET high=high+0.01,low=low+0.01,close=close+0.01 "
                    "WHERE ts_code='000001.SZ' AND trade_date=?",
                    [_FIRST + timedelta(days=250)],
                )
    refreshed = _refresh(
        backend.root, backend.reference, tmp_path, policy_change=revision == "policy"
    )
    result = FactorTrackingRunner(
        backend.root, refreshed, identity, clock=lambda: _AS_OF
    ).run_history(body.factor_id, target_end=_FIRST + timedelta(days=259))
    assert result.status == "paused"
    assert store.days(body.factor_id, expected_identity=identity) == before


def test_explicit_cli_composes_and_binds_v3_and_no_base_mode_remains_independent(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    import json

    from rquant.factor.member_archive import _bytes
    from rquant.factor.run_configuration import (
        FactorRunFileReference,
        open_factor_run_configuration,
    )
    from rquant.factor.run_entry import main

    root, reference, _, prepared, config, source = _configuration(tmp_path)
    prepared_path = root / "prepared.json"
    prepared_path.write_bytes(_bytes(prepared))
    prepared_path.chmod(0o600)
    base_path = root / "base.json"
    base_path.write_bytes(_bytes(source.base_daily_source))
    base_path.chmod(0o600)
    args = [
        "seal-stock-features",
        "--root",
        str(root),
        "--prepared-source",
        str(prepared_path),
        "--lake-root",
        str(config.lake_root),
        "--base-daily-source",
        str(base_path),
        "--reference",
        reference.model_dump_json(),
    ]
    assert main(args) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["source_reference"]["kind"] == "factor-daily-feature-source-v3"
    updated = FactorRunFileReference.model_validate_json(
        json.dumps(receipt["configuration_reference"])
    )
    with open_factor_run_configuration(root, updated) as loaded:
        assert len(loaded.daily_features.fields) == 39
        assert loaded.daily_features.base_daily_source == source.base_daily_source
    assert main(args[:-4]) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["source_reference"]["kind"] == "factor-daily-feature-source-v3"


def test_unused_v3_config_keeps_original_six_field_spec_and_prefix_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        save_factor_run_configuration,
    )
    from rquant.strict_json import canonical_json_bytes

    root, reference, browser, _, config, _ = _configuration(tmp_path, expression="close")

    def forbidden(*args: object, **kwargs: object) -> object:
        pytest.fail("unused stock source must not open")

    monkeypatch.setattr(module, "open_factor_daily_feature_source", forbidden)
    plans, prefixes = [], []
    for selected in (
        reference,
        save_factor_run_configuration(
            root, config.model_copy(update={"daily_feature_source": None})
        ),
    ):
        plan = _plan(root, selected, browser, config)
        assert plan.spec.adapter_request.daily_feature_source is None
        plans.append(
            canonical_json_bytes(plan.spec.model_dump(mode="json", exclude_computed_fields=True))
        )
        with open_factor_run_configuration(root, selected) as loaded:
            prefix, _ = module.read_factor_tracking_prefix(loaded, plan.spec)
        prefixes.append(prefix)
    assert plans[0] == plans[1] and prefixes[0] == prefixes[1]
