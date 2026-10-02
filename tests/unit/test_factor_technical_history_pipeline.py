"""Derived values reuse configured jobs, sealed replay and the tracking transaction."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.factor.capability import HISTORICAL_DAILY_V1, historical_daily_capabilities
from rquant.factor.definition import build_factor_definition
from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
from rquant.factor.run_configuration import open_factor_run_configuration
from rquant.factor.run_plan import compile_factor_run_plan
from rquant.factor.stream_adapter import FactorStreamAdapterRequest
from rquant.factor.time_series import DecisionTime
from tests.unit.test_factor_stream_adapter import _at, _pools
from tests.unit.test_factor_technical_history_source import (
    _AS_OF,
    _FIRST,
    _module,
    _oracle,
    _prepared,
    _raw,
)


def _configuration(
    tmp_path: Path, *, expression: str = "ref(ma5, 1) + turnover_rate + close", end: int = 74
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

    raw = _raw(tmp_path, count=12)
    with duckdb.connect(str(raw)) as connection:
        connection.execute(
            "DELETE FROM adj_factor WHERE ts_code='000004.SZ' AND trade_date<?",
            [_FIRST + timedelta(days=78)],
        )
        connection.execute(
            "INSERT INTO adj_factor SELECT ts_code,trade_date,1 FROM daily_bar "
            "WHERE ts_code='000004.SZ' AND trade_date>=? ON CONFLICT DO NOTHING",
            [_FIRST + timedelta(days=78)],
        )
        connection.execute(
            "UPDATE adj_factor SET adj_factor=1 WHERE ts_code='000004.SZ' AND trade_date>=?",
            [_FIRST + timedelta(days=78)],
        )
        connection.execute(
            "UPDATE daily_bar SET close=NULL WHERE ts_code='000006.SZ' AND trade_date=?",
            [_FIRST + timedelta(days=78)],
        )
    from tests.unit.test_factor_source_prepare import _sidecar

    _sidecar(raw)
    prepared = _prepared(tmp_path, raw, start=60, end=end, count=12)
    module = _module()
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    catalog = historical_daily_capabilities(
        daily_features_available=True, technical_history_available=True
    ).feature_catalog()
    if expression == "close":
        catalog = HISTORICAL_DAILY_V1.feature_catalog()
    definition = build_factor_definition(
        factor_id="derived_daily",
        name_zh="历史指标",
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
            command_id="save-derived", definition=definition, expected_head=None
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
        command_id="00000000-0000-4000-8000-000000000020",
        requested_at=_AS_OF,
        serving_generation_id="c" * 64,
        parameters=FactorRunParameters(
            factor_id=definition.factor_id,
            expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
            selection="all",
            start_date=_FIRST + timedelta(days=62),
            end_date=_FIRST + timedelta(days=64),
            holding_sessions=1,
            extended_statistics=True,
        ),
    )
    return root, reference, browser, prepared, config, source


def _plan(root: Path, reference: object, browser: object, config: object) -> object:
    return compile_factor_run_plan(
        root,
        reference,
        browser,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )


def _refresh(
    tmp_path: Path, root: Path, reference: object, *, end: int = 80, policy_change: bool = False
) -> object:
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource
    from rquant.factor.run_configuration import (
        save_factor_daily_feature_source,
        save_factor_prepared_source,
        save_factor_run_configuration,
    )
    from rquant.runtime_contracts import canonical_sha256
    from tests.unit.test_factor_source_prepare import _sidecar

    with open_factor_run_configuration(root, reference) as loaded:
        config, path = loaded.configuration, loaded.source.receipt.request.replica_path
    _sidecar(path)
    prepared = _prepared(tmp_path, path, start=60, end=end, count=12)
    module = _module()
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=config.lake_root,
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    if policy_change:
        fields = source.model_dump(exclude={"sha256"})
        fields["technical_history"]["policy"]["implementation_sha256"] = "a" * 64
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


def test_derived_configured_worker_replay_and_typed_projection_match_raw_manual_values(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.run_configuration import run_configured_factor_worker
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts

    root, reference, browser, prepared, config, source = _configuration(tmp_path)
    caps = FactorRunPageControlBackend(root, reference).capabilities()
    assert caps.version == "daily_derived_v1"
    plan = _plan(root, reference, browser, config)
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    job = ledger.submit(browser.command_id, plan.spec)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "succeeded", result
    verified = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    )
    with duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as raw:
        rows = raw.execute(
            "SELECT b.trade_date,b.high,b.low,b.close,a.adj_factor FROM daily_bar b "
            "JOIN adj_factor a USING(ts_code,trade_date) "
            "WHERE ts_code='000001.SZ' ORDER BY trade_date"
        ).fetchall()
    expected = _oracle(rows)
    for day in verified.full.journal.days:
        batch = FactorDailyStreamBatch.model_validate_json(
            (config.artifact_root / day.artifact.filename).read_bytes()
        )
        panel = day.trade_date - timedelta(days=1)
        assert batch.daily_features.panel_date == panel
        point = batch.factor_values[0]
        assert point.value == pytest.approx(
            expected[panel - timedelta(days=1)]["ma5"]
            + 0.5387
            + next(r[3] for r in rows if r[0] == panel)
        )
        assert batch.daily_features.sources.value_semantics == "history_derived"
    assert verified.display.daily_features.technical_history == source.technical_history.summary()
    assert any(
        count.reasons
        for day in verified.display.daily_feature_coverage_days
        for count in day.counts
    )
    projections = project_factor_result_projections(
        config.ledger_identity, config.artifact_root, available_at=_AS_OF
    )
    projection = validate_factor_result_projections({p.table_name: p for p in projections})
    assert projection.displays[0].daily_features == verified.display.daily_features
    assert ledger.get(job.job_id).status == "succeeded"
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_derived_prefix_ignores_future_initialization_and_break_but_binds_policy(
    tmp_path: Path,
) -> None:
    from rquant.factor.tracking_runner import read_factor_tracking_prefix

    root, reference, browser, _, config, source = _configuration(tmp_path, expression="ma5")
    plan = _plan(root, reference, browser, config)
    with open_factor_run_configuration(root, reference) as loaded:
        before, _ = read_factor_tracking_prefix(loaded, plan.spec)
    refreshed = _refresh(tmp_path, root, reference)
    plan = _plan(root, refreshed, browser, config)
    with open_factor_run_configuration(root, refreshed) as loaded:
        assert loaded.daily_features.technical_history.codes[
            3
        ].first_valid_date > _FIRST + timedelta(days=64)
        assert loaded.daily_features.technical_history.codes[5].break_date > _FIRST + timedelta(
            days=64
        )
        after, _ = read_factor_tracking_prefix(loaded, plan.spec)
    assert after == before
    changed = _refresh(tmp_path, root, refreshed, policy_change=True)
    plan = _plan(root, changed, browser, config)
    with open_factor_run_configuration(root, changed) as loaded:
        revised, _ = read_factor_tracking_prefix(loaded, plan.spec)
    assert revised != before, "derived initialization/algorithm policy must be causal input"


def _tracking(tmp_path: Path, *, expression: str = "ref(ma5, 1) + turnover_rate + close") -> tuple:
    from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root, reference, browser, prepared, config, source = _configuration(
        tmp_path, expression=expression
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
    service = PageControlService(outbox=outbox, consumer=consumer)
    body = FactorTrackingRequest(
        command_id="derived-start",
        requested_at=browser.requested_at,
        serving_generation_id=browser.serving_generation_id,
        factor_id=browser.parameters.factor_id,
        tracked=True,
        expected_head=browser.parameters.expected_head,
    )
    return service, backend, outbox, store, identity, body, config


def test_derived_toggle_incremental_and_full_worker_have_identical_contributions_and_prefix(
    tmp_path: Path,
) -> None:
    from rquant.factor.tracking import summarize_factor_tracking
    from rquant.factor.tracking_runner import FactorTrackingRunner, _last_run
    from rquant.factor.tracking_serving import project_factor_tracking_snapshot
    from tests.unit.test_factor_stored_tracking import _submit

    service, backend, _, store, identity, body, config = _tracking(tmp_path)
    assert _submit(service, backend, body).result["tracked"]
    runner = FactorTrackingRunner(backend.root, backend.reference, identity, clock=lambda: _AS_OF)
    first = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=62))
    assert first.status == "updated" and len(first.evaluation_days) == 1
    initial = store.days(body.factor_id, expected_identity=identity)
    added = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=64))
    assert added.status == "updated" and len(added.evaluation_days) == 2
    days = store.days(body.factor_id, expected_identity=identity)
    assert days[:1] == initial
    state = store.get(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        incremental = _last_run(store, connection, state.segment_id)
    assert (
        runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=64)).status
        == "waiting"
    )
    cancelled = _submit(
        service,
        backend,
        body.model_copy(
            update={
                "command_id": "derived-cancel",
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
                "command_id": "derived-rejoin",
                "expected_tracking_generation": cancelled.result["tracking_generation"],
            }
        ),
    )
    whole = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=64))
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


@pytest.mark.parametrize("revision", ["value", "state", "policy"])
def test_consumed_derived_history_or_policy_revision_pauses_without_append(
    tmp_path: Path, revision: str
) -> None:
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_stored_tracking import _submit

    service, backend, _, store, identity, body, config = _tracking(tmp_path, expression="ma5")
    _submit(service, backend, body)
    assert (
        FactorTrackingRunner(backend.root, backend.reference, identity, clock=lambda: _AS_OF)
        .run_history(body.factor_id, target_end=_FIRST + timedelta(days=62))
        .status
        == "updated"
    )
    before = store.days(body.factor_id, expected_identity=identity)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        path = loaded.source.receipt.request.replica_path
    if revision != "policy":
        with duckdb.connect(str(path)) as raw:
            if revision == "state":
                raw.execute(
                    "DELETE FROM adj_factor WHERE ts_code='000001.SZ' AND trade_date=?",
                    [_FIRST + timedelta(days=60)],
                )
            else:
                raw.execute(
                    "UPDATE daily_bar SET high=high+0.01,low=low+0.01,close=close+0.01 "
                    "WHERE ts_code='000001.SZ' AND trade_date=?",
                    [_FIRST + timedelta(days=60)],
                )
    refreshed = _refresh(
        tmp_path, backend.root, backend.reference, policy_change=revision == "policy"
    )
    result = FactorTrackingRunner(
        backend.root, refreshed, identity, clock=lambda: _AS_OF
    ).run_history(body.factor_id, target_end=_FIRST + timedelta(days=64))
    assert result.status == "paused"
    assert store.days(body.factor_id, expected_identity=identity) == before


def test_new_history_input_witness_change_after_prefix_closes_refuses_tracking_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_stored_tracking import _submit

    service, backend, _, store, identity, body, config = _tracking(tmp_path, expression="ma5")
    _submit(service, backend, body)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        artifact = (
            config.lake_root / loaded.daily_features.technical_history.inputs[0].relative_path
        )
    verify = module.verify_factor_stream_artifacts

    def changed(*args: object, **kwargs: object) -> object:
        result = verify(*args, **kwargs)
        node = artifact.stat()
        os.utime(artifact, ns=(node.st_atime_ns, node.st_mtime_ns + 1_000_000))
        return result

    monkeypatch.setattr(module, "verify_factor_stream_artifacts", changed)
    result = module.FactorTrackingRunner(
        backend.root, backend.reference, identity, clock=lambda: _AS_OF
    ).run_history(body.factor_id, target_end=_FIRST + timedelta(days=62))
    assert result.status != "updated"
    assert store.days(body.factor_id, expected_identity=identity) == ()
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_explicit_technical_cli_prepares_and_binds_v2_without_changing_default_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    import json

    from rquant.factor.member_archive import _bytes
    from rquant.factor.run_configuration import FactorRunFileReference
    from rquant.factor.run_entry import main

    root, reference, _, prepared, config, _ = _configuration(tmp_path)
    input_path = root / "prepared.json"
    input_path.write_bytes(_bytes(prepared))
    input_path.chmod(0o600)
    argv = [
        "seal-technical-history",
        "--root",
        str(root),
        "--prepared-source",
        str(input_path),
        "--lake-root",
        str(config.lake_root),
        "--reference",
        reference.model_dump_json(),
        "--max-input-rows",
        "16000000",
        "--max-code-observations",
        "50000",
        "--max-output-cells",
        "32000000",
    ]
    assert main(argv) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["source_reference"]["kind"] == "factor-daily-feature-source-v2"
    updated = FactorRunFileReference.model_validate_json(
        json.dumps(receipt["configuration_reference"])
    )
    with open_factor_run_configuration(root, updated) as loaded:
        assert loaded.daily_features.schema_version == 2
        assert loaded.daily_features.prepared_source_sha256 == prepared.sha256


@pytest.mark.parametrize("failure", ["missing", "tail"])
def test_derived_input_missing_or_natural_tail_failure_cannot_complete_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureReadLease
    from rquant.factor.run_configuration import run_configured_factor_worker

    root, reference, browser, _, config, source = _configuration(tmp_path)
    plan = _plan(root, reference, browser, config)
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    job = ledger.submit(browser.command_id, plan.spec)
    artifact = config.lake_root / source.technical_history.inputs[0].relative_path
    leases = []
    if failure == "missing":
        artifact.unlink()
    else:
        query = FactorDailyFeatureReadLease.query

        def changed(self: object, argument: object) -> object:
            result = query(self, argument)
            leases.append(self)
            data = artifact.read_bytes()
            artifact.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
            return result

        monkeypatch.setattr(FactorDailyFeatureReadLease, "query", changed)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "failed"
    assert ledger.get(job.job_id).completion is None
    assert all(lease.closed and not lease._private_root.exists() for lease in leases)
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_unused_derived_configuration_keeps_old_spec_and_prefix_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from rquant.factor.run_configuration import save_factor_run_configuration
    from rquant.strict_json import canonical_json_bytes

    root, reference, browser, _, config, _ = _configuration(tmp_path, expression="close")

    def forbidden(*args: object, **kwargs: object) -> object:
        pytest.fail("an unused derived source must not open")

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
        assert "daily_features" not in plan.spec.adapter_request.formula.sources.model_dump()
        plans.append(
            canonical_json_bytes(plan.spec.model_dump(mode="json", exclude_computed_fields=True))
        )
        with open_factor_run_configuration(root, selected) as loaded:
            prefix, _ = module.read_factor_tracking_prefix(loaded, plan.spec)
            prefixes.append(prefix)
    assert plans[0] == plans[1] and prefixes[0] == prefixes[1]
