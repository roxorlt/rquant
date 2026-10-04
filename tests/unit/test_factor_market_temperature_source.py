"""Paired market day facts retain units and are broadcast only while reading."""

from __future__ import annotations

import importlib
import importlib.util
import os
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST, _replica, _request, _sidecar

_COLUMNS = ("market_above_ma20_ratio_pct", "market_high_60d_ratio_pct")


def _module() -> object:
    name = "rquant.factor.market_temperature_source"
    assert importlib.util.find_spec(name) is not None, (
        "paired market temperature producer is missing"
    )
    return importlib.import_module(name)


def _prepared(tmp_path: Path, *, count: int = 12, days: int = 8, mutate: object = None) -> tuple:
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore

    path = _replica(tmp_path, count=count, days=days)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "CREATE TABLE market_sentiment_daily(trade_date DATE PRIMARY KEY,"
            "high_60d_ratio_pct DOUBLE,above_ma20_ratio_pct DOUBLE)"
        )
        raw.execute(
            "INSERT INTO market_sentiment_daily SELECT CAST(? AS DATE)+d::INTEGER,"
            "d::DOUBLE*10,100.-d FROM range(?) days(d)",
            [_FIRST, days],
        )
        if mutate is not None:
            mutate(raw)
    _sidecar(path)
    (tmp_path / "lake").mkdir(mode=0o700)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            _request(path, count=count, days=days),
            metadata_store=metadata,
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF,
        )
    return prepared, path


def _source(tmp_path: Path, **kwargs: object) -> tuple:
    m = _module()
    prepared, path = _prepared(tmp_path, **kwargs)
    source = m.prepare_factor_market_temperature_source(
        m.FactorMarketTemperaturePrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    return source, prepared, path


def _batch(source: object, lease: object, day: object, codes: tuple | None = None) -> object:
    from rquant.factor.daily_feature_source import FactorDailyFeatureQuery

    return lease.query(
        FactorDailyFeatureQuery(
            source_sha256=source.sha256,
            trade_date=day,
            stock_codes=source.scope.stock_codes if codes is None else codes,
            fields=_COLUMNS,
        )
    )


def test_temperature_catalog_requires_actual_source_and_percent_units() -> None:
    _module()
    from rquant.factor.capability import historical_daily_capabilities

    assert not set(_COLUMNS) & set(historical_daily_capabilities().feature_catalog().columns)
    with pytest.raises(ValueError):
        historical_daily_capabilities(market_temperature_available=True)
    caps = historical_daily_capabilities(
        daily_features_available=True, market_temperature_available=True
    )
    fields = {f.column: f for f in caps.fields}
    assert caps.version == "daily_market_temperature_v1"
    assert fields[_COLUMNS[0]].name_zh == "20日均线上方占比"
    assert fields[_COLUMNS[1]].name_zh == "60日新高占比"
    assert all(fields[c].unit == "percent" for c in _COLUMNS)
    assert all(fields[c].tracking_supported is True for c in _COLUMNS)


def test_market_day_is_sealed_once_and_broadcast_without_pool_recalculation(tmp_path: Path) -> None:
    _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureInput,
        FactorDailyFeatureSource,
        open_factor_daily_feature_source,
        read_factor_daily_feature_input,
    )

    source, prepared, _ = _source(tmp_path)
    assert source.schema_version == 5
    assert source.value_semantics == "market_temperature_stored"
    assert source.generation == prepared.receipt.generation
    receipt = source.market_temperature
    assert receipt.row_count == 8
    assert receipt.artifact.primary_key == ("trade_date",)
    with duckdb.connect(":memory:") as check:
        path = tmp_path / "lake" / receipt.artifact.relative_path
        assert check.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone() == (8,)
        assert [
            r[0]
            for r in check.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        ] == ["trade_date", "high_60d_ratio_pct", "above_ma20_ratio_pct"]
    assert FactorDailyFeatureSource.model_validate_json(source.model_dump_json()) == source
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        all_facts = _batch(source, lease, _FIRST).facts
        subset = _batch(source, lease, _FIRST, ("000002.SZ",)).facts
        assert [(f.column, f.value) for f in subset] == [(_COLUMNS[0], 100.0), (_COLUMNS[1], 0.0)]
        assert all(f.value == (100.0 if f.column == _COLUMNS[0] else 0.0) for f in all_facts)
        original = read_factor_daily_feature_input(
            lease,
            source.select(_COLUMNS),
            trade_date=_FIRST + timedelta(days=1),
            panel_date=_FIRST,
            stock_codes=source.scope.stock_codes,
        )
        assert all(not row.values for row in original.rows)
        assert len(original.market_values) == 2
        assert FactorDailyFeatureInput.model_validate_json(original.model_dump_json()) == original
    assert lease.closed and not lease._private_root.exists()


@pytest.mark.parametrize(
    ("raw", "status", "reason", "tag"),
    [
        (None, "null", "market_temperature_null", None),
        (float("nan"), "non_finite", "market_temperature_non_finite", "NaN"),
        (float("inf"), "non_finite", "market_temperature_non_finite", "Infinity"),
        (float("-inf"), "non_finite", "market_temperature_non_finite", "-Infinity"),
        (-0.01, "null", "invalid_market_percentage", None),
        (100.01, "null", "invalid_market_percentage", None),
    ],
)
def test_missing_values_keep_explicit_reason(
    tmp_path: Path, raw: float | None, status: str, reason: str, tag: str | None
) -> None:
    _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    def mutate(connection: object) -> None:
        connection.execute(
            "UPDATE market_sentiment_daily SET high_60d_ratio_pct=? WHERE trade_date=?",
            [raw, _FIRST],
        )

    source, _, _ = _source(tmp_path, mutate=mutate)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        fact = next(f for f in _batch(source, lease, _FIRST).facts if f.column == _COLUMNS[1])
        assert (fact.value, fact.status, fact.reason, fact.non_finite_value) == (
            None,
            status,
            reason,
            tag,
        )


def test_absent_day_has_missing_reason_and_does_not_use_neighbor(tmp_path: Path) -> None:
    _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    def mutate(connection: object) -> None:
        connection.execute("DELETE FROM market_sentiment_daily WHERE trade_date=?", [_FIRST])

    source, _, _ = _source(tmp_path, mutate=mutate)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        facts = _batch(source, lease, _FIRST).facts
        assert all(
            f.value is None and f.status == "missing" and f.reason == "missing_market_temperature"
            for f in facts
        )


def test_duplicate_source_day_is_rejected_before_publishing(tmp_path: Path) -> None:
    m = _module()

    def mutate(connection: object) -> None:
        connection.execute("ALTER TABLE market_sentiment_daily RENAME TO old_temperature")
        connection.execute("CREATE TABLE market_sentiment_daily AS SELECT * FROM old_temperature")
        connection.execute(
            "INSERT INTO market_sentiment_daily SELECT * FROM old_temperature LIMIT 1"
        )

    prepared, _ = _prepared(tmp_path, mutate=mutate)
    with pytest.raises(ValueError, match="duplicate"):
        m.prepare_factor_market_temperature_source(
            m.FactorMarketTemperaturePrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
        )
    assert not list((tmp_path / "lake").glob("tables/market_temperature_daily/versions/*"))


def test_generation_change_is_rejected_and_descriptor_is_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    prepared, path = _prepared(tmp_path)
    opened = []
    original = m.connect_pinned_readonly

    def capture(replica: Path, descriptor: int) -> tuple:
        connection, mode = original(replica, descriptor)
        opened.append((connection, descriptor))
        _sidecar(path)
        return connection, mode

    monkeypatch.setattr(m, "connect_pinned_readonly", capture)
    with pytest.raises(ValueError):
        m.prepare_factor_market_temperature_source(
            m.FactorMarketTemperaturePrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
        )
    connection, descriptor = opened[0]
    with pytest.raises(duckdb.ConnectionException):
        connection.execute("SELECT 1")
    with pytest.raises(OSError):
        os.fstat(descriptor)
    assert not list((tmp_path / "lake").glob(".market-temperature-*"))


def _adapter_request(
    source: object,
    prepared: object,
    *,
    expression: str = "ref(market_high_60d_ratio_pct, 1) + market_above_ma20_ratio_pct + close",
    days: tuple | None = None,
) -> object:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.time_series import DecisionTime
    from tests.unit.test_factor_stream_adapter import _at

    definition = build_factor_definition(
        factor_id="market_temperature",
        name_zh="市场温度",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression=expression,
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True,
            market_temperature_available=True,
            market_temperature_base_daily_available=source.base_daily_source is not None,
        ).feature_catalog(),
    )
    if days is None:
        days = tuple(_FIRST + timedelta(days=i) for i in (1, 2, 3, 4))
    fields = tuple(c for c in definition.dependency_columns if c != "close")
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
            daily_features=source.select(fields),
        ),
    )
    return FactorStreamAdapterRequest(
        source=prepared.admission_request,
        scope_content_hash=prepared.scope_content_hash,
        formula=formula,
        evaluation_days=days[1:],
        holding_sessions=1,
        daily_feature_source=source,
    )


def test_previous_complete_sse_panel_and_mixed_formula_use_raw_percentage(tmp_path: Path) -> None:
    from rquant.factor.run_configuration import PreparedFactorMetadata
    from rquant.factor.stream_runner import run_factor_stream_research_with_decay
    from tests.unit.test_factor_stream_adapter import _pools

    def mutate(connection: object) -> None:
        connection.execute(
            "UPDATE trade_calendar SET is_open=FALSE WHERE cal_date=?", [_FIRST + timedelta(days=2)]
        )
        connection.execute(
            "UPDATE trade_calendar SET pretrade_date=? WHERE cal_date=?",
            [_FIRST + timedelta(days=1), _FIRST + timedelta(days=3)],
        )
        connection.execute(
            "UPDATE market_sentiment_daily SET high_60d_ratio_pct=99,"
            "above_ma20_ratio_pct=99 WHERE trade_date=?",
            [_FIRST + timedelta(days=2)],
        )

    source, prepared, _ = _source(tmp_path, mutate=mutate)
    days = tuple(_FIRST + timedelta(days=i) for i in (1, 3, 4))
    request = _adapter_request(source, prepared, days=days)
    batches = []
    result = run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=tmp_path / "lake",
        universe_requests=_pools(request),
        batch_observer=batches.append,
    )
    assert result.research.formula_completion.processed_days == 3
    assert [b.daily_features.panel_date for b in batches] == [
        _FIRST + timedelta(days=i) for i in (1, 3)
    ]
    for batch in batches:
        offset = (batch.universe.trade_date - _FIRST).days
        panel = offset - 1 if offset != 3 else 1
        previous = 0 if offset == 3 else 1
        for point in batch.factor_values:
            expected = previous * 10 + 100 - panel + 10 + panel + int(point.stock_code[:6]) / 100
            assert point.value == pytest.approx(expected)
        assert len(batch.daily_features.market_values) == 2


def test_constant_temperature_cross_section_keeps_zero_variance_statistics(tmp_path: Path) -> None:
    from rquant.factor.run_configuration import PreparedFactorMetadata
    from rquant.factor.stream_runner import run_factor_stream_research_with_decay
    from tests.unit.test_factor_stream_adapter import _pools

    source, prepared, _ = _source(tmp_path)
    request = _adapter_request(source, prepared, expression="market_high_60d_ratio_pct")
    result = run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=tmp_path / "lake",
        universe_requests=_pools(request),
    )
    assert all(
        day.evaluation.rank_ic.status == "zero_variance" for day in result.research.statistics.days
    )


def _configured(
    tmp_path: Path,
    *,
    expression: str = "ref(market_high_60d_ratio_pct, 1) + market_above_ma20_ratio_pct + close",
    mutate: object = None,
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
    from tests.unit.test_factor_stream_adapter import _pools

    source, prepared, _ = _source(tmp_path, mutate=mutate)
    request = _adapter_request(source, prepared, expression=expression)
    members, archive, request = _archive(tmp_path, request, _pools(request))
    registry = FactorDefinitionRegistry(tmp_path / "registry.sqlite")
    registry_identity = registry.initialize()
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="save-market", definition=request.formula.definition, expected_head=None
        ),
        expected_identity=registry_identity,
    )
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    root = _private(tmp_path / "configuration")
    config = FactorRunConfiguration(
        enabled=True,
        factor_run_users=("alice",),
        registry_identity=registry_identity,
        ledger_identity=ledger.initialize(),
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
        command_id="00000000-0000-4000-8000-000000000017",
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


@pytest.mark.parametrize("missing", [False, True])
def test_configured_worker_journal_replay_result_projection_and_actual_capabilities(
    tmp_path: Path,
    missing: bool,
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

    def mutate(connection: object) -> None:
        if missing:
            connection.execute(
                "UPDATE market_sentiment_daily SET above_ma20_ratio_pct=NULL WHERE trade_date=?",
                [_FIRST + timedelta(days=1)],
            )

    root, reference, browser, _, config, source = _configured(tmp_path, mutate=mutate)
    caps = FactorRunPageControlBackend(root, reference).capabilities()
    assert set(caps.feature_catalog().columns) == {
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
        *_COLUMNS,
    }
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
    ledger.submit(browser.command_id, plan.spec)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "succeeded", result
    verified = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    )
    for day in verified.full.journal.days:
        batch = FactorDailyStreamBatch.model_validate_json(
            (config.artifact_root / day.artifact.filename).read_bytes()
        )
        assert len(batch.daily_features.market_values) == 2
        assert all(not row.values for row in batch.daily_features.rows)
        coverage = next(
            d
            for d in verified.display.daily_feature_coverage_days
            if d.trade_date == day.trade_date
        )
        assert coverage.panel_date == batch.daily_features.panel_date
        assert len(coverage.market_temperature_values) == 2
        for projected, column, original in zip(
            coverage.market_temperature_values,
            _COLUMNS,
            batch.daily_features.market_values,
            strict=True,
        ):
            assert projected.column == column
            assert (projected.value, projected.status, projected.reason) == (
                original.value,
                original.status,
                original.reason,
            )
    assert verified.display.daily_features.market_temperature.policy.unit == "percent"
    if missing:
        assert any(
            v.status == "null" and v.reason == "market_temperature_null"
            for d in verified.display.daily_feature_coverage_days
            for v in d.market_temperature_values
        )
    projection = validate_factor_result_projections(
        {
            p.table_name: p
            for p in project_factor_result_projections(
                config.ledger_identity, config.artifact_root, available_at=_AS_OF
            )
        }
    )
    assert projection.displays[0].daily_features == verified.display.daily_features
    from rquant.web.models.factor_results import FactorStreamResearchDisplay

    public = FactorStreamResearchDisplay.model_validate(
        {
            **{
                key: getattr(verified.display, key)
                for key in FactorStreamResearchDisplay.model_fields
                if hasattr(verified.display, key)
            },
            "basis_label": "历史回顾",
        }
    )
    assert public.daily_feature_coverage_days == verified.display.daily_feature_coverage_days


def test_trusted_cli_seals_v5_and_binds_existing_configuration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.run_entry import FactorDailyFeatureSealReceipt, main
    from rquant.strict_json import canonical_json_bytes

    root, reference, _, prepared, _, original = _configured(tmp_path)
    prepared_path = root / "prepared.json"
    prepared_path.write_bytes(
        canonical_json_bytes(prepared.model_dump(mode="json", round_trip=True))
    )
    os.chmod(prepared_path, 0o600)
    assert (
        main(
            [
                "seal-market-temperature",
                "--root",
                str(root),
                "--prepared-source",
                str(prepared_path),
                "--lake-root",
                str(tmp_path / "lake"),
                "--reference",
                reference.model_dump_json(),
            ]
        )
        == 0
    )
    receipt = FactorDailyFeatureSealReceipt.model_validate_json(capsys.readouterr().out)
    assert receipt.source_reference.kind == "factor-daily-feature-source-v5"
    assert receipt.configuration_reference is not None
    with open_factor_run_configuration(root, receipt.configuration_reference) as loaded:
        assert loaded.daily_features.schema_version == 5
        assert loaded.daily_features.fields == original.fields
        assert loaded.daily_features.market_temperature == original.market_temperature
        loaded.daily_features.require_prepared(prepared)


def test_v5_with_inventory_base_preserves_old_source_wire_and_values(tmp_path: Path) -> None:
    m = _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeaturePrepareRequest,
        FactorDailyFeatureQuery,
        FactorDailyFeatureSource,
        open_factor_daily_feature_source,
        prepare_factor_daily_feature_source,
    )
    from tests.unit.test_factor_daily_feature_source import _prepared as old_prepared

    def mutate(connection: object) -> None:
        connection.execute(
            "CREATE TABLE market_sentiment_daily(trade_date DATE PRIMARY KEY,"
            "high_60d_ratio_pct DOUBLE,above_ma20_ratio_pct DOUBLE)"
        )
        connection.execute(
            "INSERT INTO market_sentiment_daily SELECT DISTINCT trade_date,20.25,75.5 "
            "FROM daily_bar"
        )

    prepared = old_prepared(tmp_path, count=12, days=8, mutate=mutate)
    base = prepare_factor_daily_feature_source(
        FactorDailyFeaturePrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=1),
    )
    wire = base.model_dump_json()
    source = m.prepare_factor_market_temperature_source(
        m.FactorMarketTemperaturePrepareRequest(prepared_source=prepared, base_daily_source=base),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=2),
    )
    decoded = FactorDailyFeatureSource.model_validate_json(source.model_dump_json())
    assert decoded.base_daily_source.model_dump_json() == wire
    assert "market_temperature" not in wire
    with open_factor_daily_feature_source(decoded, lake_root=tmp_path / "lake") as lease:
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST,
                stock_codes=("000001.SZ",),
                fields=("turnover_rate", *_COLUMNS),
            )
        )
        assert {f.column: f.value for f in batch.facts} == {
            "turnover_rate": 0.5387,
            _COLUMNS[0]: 75.5,
            _COLUMNS[1]: 20.25,
        }


@pytest.mark.parametrize("inventory,count,queries_per_day", [(False, 501, 1), (True, 12, 2)])
def test_completion_counts_shared_market_query_and_stock_chunks(
    tmp_path: Path, inventory: bool, count: int, queries_per_day: int
) -> None:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeaturePrepareRequest,
        prepare_factor_daily_feature_source,
    )
    from rquant.factor.run_configuration import PreparedFactorMetadata
    from rquant.factor.stream_runner import run_factor_stream_research_with_decay
    from tests.unit.test_factor_daily_feature_source import _prepared as old_prepared
    from tests.unit.test_factor_stream_adapter import _pools

    if inventory:

        def mutate(connection: object) -> None:
            connection.execute(
                "CREATE TABLE market_sentiment_daily AS SELECT DISTINCT trade_date,"
                "20.25::DOUBLE AS high_60d_ratio_pct,75.5::DOUBLE AS above_ma20_ratio_pct "
                "FROM daily_bar"
            )

        prepared = old_prepared(tmp_path, count=count, days=8, mutate=mutate)
        base = prepare_factor_daily_feature_source(
            FactorDailyFeaturePrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=1),
        )
        m = _module()
        source = m.prepare_factor_market_temperature_source(
            m.FactorMarketTemperaturePrepareRequest(
                prepared_source=prepared, base_daily_source=base
            ),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=2),
        )
    else:
        source, prepared, _ = _source(tmp_path, count=count)
    request = _adapter_request(
        source,
        prepared,
        expression="market_high_60d_ratio_pct + turnover_rate"
        if inventory
        else "market_high_60d_ratio_pct + close",
    )
    result = run_factor_stream_research_with_decay(
        request,
        metadata_store=PreparedFactorMetadata(prepared),
        lake_root=tmp_path / "lake",
        universe_requests=_pools(request),
    )
    receipt = result.research.adapter_completion
    assert (
        receipt.daily_feature_read_query_count
        == len(request.formula.trading_days) * queries_per_day
    )


def _tracking_control(tmp_path: Path, *, source_present: bool = True) -> tuple:
    from rquant.factor.run_configuration import save_factor_run_configuration
    from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root, reference, run, _, config, _ = _configured(tmp_path)
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
        command_id="temperature-start",
        requested_at=run.requested_at,
        serving_generation_id=run.serving_generation_id,
        factor_id=run.parameters.factor_id,
        tracked=True,
        expected_head=run.parameters.expected_head,
    )
    return PageControlService(outbox=outbox, consumer=consumer), backend, store, identity, body


def _toggle(service: object, backend: object, body: object) -> object:
    return service._submit_trusted_factor_tracking(
        body,
        authenticated_actor_id="alice",
        verified_registry_instance_id=backend.registry_identity.instance_id,
    )


def test_tracking_rejects_missing_temperature_before_enqueue(tmp_path: Path) -> None:
    service, backend, _, _, body = _tracking_control(tmp_path, source_present=False)
    with pytest.raises(ValueError, match="来源|事实|温度"):
        _toggle(service, backend, body)


def test_tracking_first_then_continued_equals_whole_and_cancel_preserves_history(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from rquant.factor.tracking import summarize_factor_tracking
    from rquant.factor.tracking_runner import FactorTrackingRunner, _last_run
    from rquant.factor.tracking_serving import project_factor_tracking_snapshot

    service, backend, store, identity, body = _tracking_control(tmp_path)
    assert _toggle(service, backend, body).result["tracked"]
    runner = FactorTrackingRunner(backend.root, backend.reference, identity, clock=lambda: _AS_OF)
    first = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=2))
    assert first.status == "updated" and len(first.evaluation_days) == 1
    before = store.days(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        initial = _last_run(
            store, connection, store.get(body.factor_id, expected_identity=identity).segment_id
        )
    continued = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4))
    assert continued.status == "updated" and len(continued.evaluation_days) == 2
    days = store.days(body.factor_id, expected_identity=identity)
    assert days[:1] == before
    state = store.get(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        incremental = _last_run(store, connection, state.segment_id)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
        config = loaded.configuration

        def values(run: object) -> dict:
            job = ledger.lookup_command(run.plan.request.command_id, run.plan.spec.spec_sha256)
            verified = verify_factor_stream_artifacts(
                job.spec, job.completion, config.artifact_root, config.member_root
            )
            return {
                entry.trade_date: FactorDailyStreamBatch.model_validate_json(
                    (config.artifact_root / entry.artifact.filename).read_bytes()
                ).factor_values
                for entry in verified.full.journal.days
            }

        incremental_values = {**values(initial), **values(incremental)}
        assert (
            runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4)).status
            == "waiting"
        )
        with store._connection(identity) as connection:
            assert connection.execute(
                "SELECT count(*) FROM tracking_days WHERE segment_id=?", [state.segment_id]
            ).fetchone()[0] == len(days)
        cancelled = _toggle(
            service,
            backend,
            body.model_copy(
                update={
                    "command_id": "cancel-temperature",
                    "tracked": False,
                    "expected_tracking_generation": state.generation,
                }
            ),
        )
        assert not cancelled.result["tracked"]
        with store._connection(identity) as connection:
            assert connection.execute(
                "SELECT count(*) FROM tracking_days WHERE segment_id=?", [state.segment_id]
            ).fetchone()[0] == len(days)
        _toggle(
            service,
            backend,
            body.model_copy(
                update={
                    "command_id": "rejoin-temperature",
                    "expected_tracking_generation": cancelled.result["tracking_generation"],
                }
            ),
        )
        whole = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4))
        assert whole.status == "updated"
        assert store.days(body.factor_id, expected_identity=identity) == days
        current = store.get(body.factor_id, expected_identity=identity)
        with store._connection(identity) as connection:
            full = _last_run(store, connection, current.segment_id)
        assert full.prefix == incremental.prefix
        assert values(full) == incremental_values
    snapshot = project_factor_tracking_snapshot(
        identity, registry_identity=backend.registry_identity, available_at=_AS_OF
    )
    assert snapshot.panels[0].summary == summarize_factor_tracking(days)


def test_logical_prefix_ignores_unneeded_future_temperature_tail(tmp_path: Path) -> None:
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.run_plan import compile_factor_run_plan
    from rquant.factor.tracking_runner import read_factor_tracking_prefix

    original, future, changed = (tmp_path / name for name in ("original", "future", "changed"))
    for root in (original, future, changed):
        root.mkdir(mode=0o700)

    def tail(connection: object) -> None:
        connection.execute(
            "UPDATE market_sentiment_daily SET high_60d_ratio_pct=0,"
            "above_ma20_ratio_pct=0 WHERE trade_date>=?",
            [_FIRST + timedelta(days=4)],
        )

    def past(connection: object) -> None:
        connection.execute(
            "UPDATE market_sentiment_daily SET high_60d_ratio_pct=0,"
            "above_ma20_ratio_pct=0 WHERE trade_date=?",
            [_FIRST + timedelta(days=1)],
        )

    prefixes = []
    for root, mutate in ((original, None), (future, tail), (changed, past)):
        config_root, reference, browser, _, config, _ = _configured(root, mutate=mutate)
        plan = compile_factor_run_plan(
            config_root,
            reference,
            browser,
            verified_registry_instance_id=config.registry_identity.instance_id,
            clock=lambda: _AS_OF,
        )
        with open_factor_run_configuration(config_root, reference) as loaded:
            prefix, witness = read_factor_tracking_prefix(loaded, plan.spec)
            witness.recheck()
            prefixes.append(prefix)
    assert prefixes[0] == prefixes[1]
    assert prefixes[0] != prefixes[2]


@pytest.fixture(scope="module")
def descriptor_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_minute_feature_descriptor import descriptor_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


@pytest.fixture(scope="module")
def context_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_minute_feature_descriptor import context_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


def _temperature_shape(base: object) -> object:
    from rquant.data_metadata import DatasetSnapshotArtifact
    from rquant.factor.daily_feature_source import (
        MARKET_TEMPERATURE_FIELDS,
        FactorDailyFeatureSource,
    )
    from rquant.factor.market_temperature_source import FactorMarketTemperatureReceipt
    from rquant.factor.source_prepare import FactorSourceDateCount
    from rquant.runtime_contracts import canonical_sha256

    dates = base.calendar_open_days
    artifact = base.tables[0].artifact.model_dump()
    artifact.update(
        dataset_id="factor_market_temperature",
        table_name="market_temperature_daily",
        primary_key=("trade_date",),
        row_count=len(dates),
        relative_path=f"tables/market_temperature_daily/versions/{artifact['file_hash']}.parquet",
    )
    receipt = FactorMarketTemperatureReceipt(
        artifact=DatasetSnapshotArtifact(**artifact),
        row_count=len(dates),
        date_counts=tuple(FactorSourceDateCount(date=d, count=1) for d in dates),
    )
    fields = base.model_dump(exclude={"sha256"})
    fields.update(
        schema_version=5,
        base_daily_source=base,
        market_temperature=receipt,
        observed_at=base.completed_read_at,
        completed_read_at=base.completed_read_at,
        fields=tuple(sorted(base.fields + MARKET_TEMPERATURE_FIELDS, key=lambda f: f.column)),
        value_semantics="market_temperature_stored",
    )
    return FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))


@pytest.mark.parametrize("count,days", ((7000, 234), (1, 4096)))
def test_maximum_52_fields_original_decoder_and_ledger_with_full_context(
    descriptor_template: tuple, context_template: tuple, tmp_path: Path, count: int, days: int
) -> None:
    import json

    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.job_spec import _definition_sha256
    from rquant.factor.stream_job_spec import FactorStreamJobSpec, decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_minute_feature_descriptor import _minute_shape, _minute_spec
    from tests.unit.test_factor_stock_feature_descriptor import _shape_context, _shape_source
    from tests.unit.test_factor_stock_feature_source import _AS_OF as _STOCK_CUTOFF

    template, original, *_ = descriptor_template
    minute = _minute_shape(_shape_source(template, count, days=days, longest=True))
    source = _temperature_shape(minute)
    context = _shape_context(source, *context_template)
    spec = _minute_spec(original, minute, context)

    def balanced(columns: tuple[str, ...]) -> str:
        if len(columns) == 1:
            return columns[0]
        middle = len(columns) // 2
        return f"({balanced(columns[:middle])}+{balanced(columns[middle:])})"

    fields = spec.adapter_request.formula.definition.model_dump(
        exclude={"max_history_window", "dependency_columns"}
    )
    fields.update(
        expression=balanced(tuple(f.column for f in source.fields)),
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True,
            technical_history_available=True,
            stock_features_available=True,
            stock_base_daily_available=True,
            minute_features_available=True,
            minute_base_daily_available=True,
            market_temperature_available=True,
            market_temperature_base_daily_available=True,
        ).feature_catalog(),
    )
    definition = build_factor_definition(**fields)
    fields = spec.model_dump()
    fields["adapter_request"].update(daily_feature_source=source)
    fields["adapter_request"]["formula"].update(definition=definition)
    fields["adapter_request"]["formula"]["sources"].update(
        daily_features=source.select(definition.dependency_columns)
    )
    fields["definition_content_sha256"] = _definition_sha256(definition)
    shape = FactorStreamJobSpec(**fields)
    data = canonical_json_bytes(shape.model_dump(mode="json", round_trip=True))
    assert len(data) < 2 * 1024 * 1024
    decoded = decode_factor_job_spec_json(data.decode())
    assert decoded == shape
    assert (
        decoded.adapter_request.daily_feature_source.base_daily_source.model_dump_json()
        == minute.model_dump_json()
    )
    ledger = FactorEvaluationJobLedger(
        tmp_path / "capacity-ledger.sqlite", clock=lambda: _STOCK_CUTOFF
    )
    ledger.initialize()
    admitted = ledger.submit("temperature-capacity", shape)
    assert ledger.get(admitted.job_id).spec == shape
    (tmp_path / "actual-wide-spec.json").write_bytes(data)
    record = dict(
        codes=count,
        range_days=days,
        formula_days=len(shape.adapter_request.formula.trading_days),
        fields=52,
        byte_count=len(data),
        budget=2 * 1024 * 1024,
        headroom=2 * 1024 * 1024 - len(data),
        sha256=shape.spec_sha256,
        decoder="pass",
        ledger_readback="pass",
        context="industry_size",
        capacity_shape_only=True,
    )
    (tmp_path / "capacity.json").write_text(json.dumps(record, indent=2) + "\n")
    print("TEMPERATURE_SPEC_CAPACITY", json.dumps(record))


def test_complete_52_field_input_retains_single_query_width_and_coverage_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureReadLease,
        FactorDailyFeatureSource,
        FactorMinuteFeaturePrepareRequest,
        FactorStockFeaturePrepareRequest,
        open_factor_daily_feature_source,
        read_factor_daily_feature_input,
    )
    from rquant.factor.minute_feature_source import prepare_factor_minute_feature_source
    from rquant.factor.stock_feature_source import prepare_factor_stock_feature_source
    from rquant.factor.stream_adapter import FactorStreamFeatureDayReceipt
    from rquant.factor.stream_job_artifact import FactorDailyFeatureCoverageDay
    from rquant.factor.technical_history_source import (
        FactorTechnicalHistoryPrepareRequest,
        prepare_factor_technical_history_source,
    )
    from tests.unit.test_factor_minute_feature_source import _raw
    from tests.unit.test_factor_stock_feature_source import _AS_OF as _STOCK_CUTOFF
    from tests.unit.test_factor_stock_feature_source import _prepared as stock_prepared

    m = _module()
    path = _raw(tmp_path, count=3)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "CREATE TABLE market_sentiment_daily(trade_date DATE PRIMARY KEY,"
            "high_60d_ratio_pct DOUBLE,above_ma20_ratio_pct DOUBLE)"
        )
        raw.execute(
            "INSERT INTO market_sentiment_daily SELECT DISTINCT trade_date,25.75,60.0 "
            "FROM daily_bar"
        )
    _sidecar(path)
    prepared = stock_prepared(tmp_path, path, count=3)
    base = prepare_factor_technical_history_source(
        FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _STOCK_CUTOFF + timedelta(minutes=1),
    )
    base = prepare_factor_stock_feature_source(
        FactorStockFeaturePrepareRequest(prepared_source=prepared, base_daily_source=base),
        lake_root=tmp_path / "lake",
        now=lambda: _STOCK_CUTOFF + timedelta(minutes=2),
    )
    base = prepare_factor_minute_feature_source(
        FactorMinuteFeaturePrepareRequest(prepared_source=prepared, base_daily_source=base),
        lake_root=tmp_path / "lake",
        now=lambda: _STOCK_CUTOFF + timedelta(minutes=3),
    )
    before = base.model_dump_json()
    source = m.prepare_factor_market_temperature_source(
        m.FactorMarketTemperaturePrepareRequest(prepared_source=prepared, base_daily_source=base),
        lake_root=tmp_path / "lake",
        now=lambda: _STOCK_CUTOFF + timedelta(minutes=4),
    )
    restored = FactorDailyFeatureSource.model_validate_json(source.model_dump_json())
    assert restored.base_daily_source.model_dump_json() == before
    queries = []
    query = FactorDailyFeatureReadLease.query

    def observed(lease: object, argument: object) -> object:
        queries.append(argument)
        return query(lease, argument)

    monkeypatch.setattr(FactorDailyFeatureReadLease, "query", observed)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        panel = source.scope.start_date
        original = read_factor_daily_feature_input(
            lease,
            source.select(tuple(f.column for f in source.fields)),
            trade_date=panel + timedelta(days=1),
            panel_date=panel,
            stock_codes=source.scope.stock_codes,
        )
    assert len(original.counts) == 52
    assert [len(q.fields) for q in queries] == [2, 50]
    assert [len(q.stock_codes) for q in queries] == [1, 3]
    FactorStreamFeatureDayReceipt(
        trade_date=original.trade_date,
        panel_date=original.panel_date,
        input_sha256="a" * 64,
        raw_sha256="b" * 64,
        missing_observation_count=0,
        known_null_count=0,
        daily_feature_counts=original.counts,
        daily_feature_input_sha256=original.sha256,
        market_temperature_values=original.market_temperature_values,
    )
    FactorDailyFeatureCoverageDay(
        trade_date=original.trade_date,
        panel_date=original.panel_date,
        computation_stock_count=3,
        counts=original.counts,
        market_temperature_values=original.market_temperature_values,
    )


def test_full_new_catalog_all_null_day_uses_existing_7000_code_contract(tmp_path: Path) -> None:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.formula_stream import (
        FactorFormulaFeaturePoint,
        FactorFormulaStreamBatch,
        FactorFormulaStreamSources,
    )
    from rquant.factor.stream_adapter import FactorStreamFeatureDayReceipt
    from tests.unit.test_factor_stream_adapter import _pools

    source, prepared, _ = _source(tmp_path)
    request = _adapter_request(source, prepared, expression="market_high_60d_ratio_pct")
    pool = next(iter(_pools(request)))
    fields = (
        historical_daily_capabilities(
            daily_features_available=True,
            technical_history_available=True,
            stock_features_available=True,
            stock_base_daily_available=True,
            minute_features_available=True,
            minute_base_daily_available=True,
            market_temperature_available=True,
            market_temperature_base_daily_available=True,
        )
        .feature_catalog()
        .columns
    )
    points = tuple(
        FactorFormulaFeaturePoint(
            stock_code=f"{n:06d}.SZ",
            column=c,
            trade_date=pool.trade_date,
            value=None,
            state="missing_observation",
            first_visible_at=None,
        )
        for n in range(1, 7001)
        for c in fields
    )
    batch = FactorFormulaStreamBatch(
        request_sha256="a" * 64,
        sources=FactorFormulaStreamSources(
            source_mode="historical_retrospective",
            feature_source_id="shape-price",
            feature_source_sha256="b" * 64,
            security_source_id="shape-security",
            security_source_sha256="c" * 64,
        ),
        universe=pool,
        feature_points=points,
    )
    assert len(batch.feature_points) == 7000 * 58
    FactorStreamFeatureDayReceipt(
        trade_date=pool.trade_date,
        panel_date=_FIRST,
        input_sha256="a" * 64,
        raw_sha256="b" * 64,
        missing_observation_count=sum(
            p.state == "missing_observation" for p in batch.feature_points
        ),
        known_null_count=0,
    )
