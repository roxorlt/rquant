"""The original VP window is a sealed, retrospective daily factor source."""

from __future__ import annotations

import importlib
import importlib.util
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from tests.unit.test_factor_market_temperature_source import _configured as _temperature_configured
from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST, _replica, _request, _sidecar

_COLUMNS = tuple(
    sorted(
        "vp90_" + name
        for name in (
            "vwap",
            "poc_price",
            "value_area_low",
            "value_area_high",
            "concentration_top5_pct",
            "below_reference_amount_pct",
            "above_reference_amount_pct",
            "below_reference_volume_pct",
            "above_reference_volume_pct",
            "total_vol",
            "total_amount",
        )
    )
)


def _module() -> object:
    name = "rquant.factor.volume_profile_source"
    assert importlib.util.find_spec(name) is not None, "original VP factor producer is missing"
    return importlib.import_module(name)


def _prepared(tmp_path: Path, *, count: int = 12, days: int = 8, mutate: object = None) -> tuple:
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore

    path = _replica(tmp_path, count=count, days=days)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "CREATE TABLE minute_bar(ts_code VARCHAR,trade_time TIMESTAMP,freq VARCHAR,"
            "open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,vol DOUBLE,amount DOUBLE,"
            "source VARCHAR,PRIMARY KEY(ts_code,trade_time,freq,source))"
        )
        raw.execute(
            "INSERT INTO daily_bar SELECT '000999.SZ',cast(? AS DATE)-i::INTEGER,"
            "10.,12.,9.,10.,1.,1. FROM range(1,101) t(i)",
            [_FIRST],
        )
        raw.execute(
            "INSERT INTO adj_factor SELECT lpad(i::VARCHAR,6,'0')||'.SZ',"
            "cast(? AS DATE)-d::INTEGER,CASE WHEN d>=3 THEN .5 ELSE 1. END "
            "FROM range(1,?) codes(i) CROSS JOIN range(1,101) days(d)",
            [_FIRST, count + 1],
        )
        raw.execute(
            "INSERT INTO minute_bar SELECT lpad(i::VARCHAR,6,'0')||'.SZ',"
            "cast(? AS TIMESTAMP)-d::INTEGER*INTERVAL 1 DAY+INTERVAL '9 hours 30 minutes',"
            "'1min',20.,20.,20.,20.,100.,2000.,'tushare' "
            "FROM range(1,?) codes(i) CROSS JOIN range(1,6) days(d)",
            [_FIRST, count + 1],
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
    module = _module()
    prepared, path = _prepared(tmp_path, **kwargs)
    source = module.prepare_factor_volume_profile_source(
        module.FactorVolumeProfilePrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    return source, prepared, path


def _batch(
    source: object, lease: object, *, day: object = _FIRST, codes: tuple = ("000001.SZ",)
) -> object:
    from rquant.factor.daily_feature_source import FactorDailyFeatureQuery

    return lease.query(
        FactorDailyFeatureQuery(
            source_sha256=source.sha256, trade_date=day, stock_codes=codes, fields=_COLUMNS
        )
    )


def test_vp_capabilities_require_a_verified_source_and_original_units() -> None:
    _module()
    from rquant.factor.capability import historical_daily_capabilities

    assert not set(_COLUMNS) & set(historical_daily_capabilities().feature_catalog().columns)
    with pytest.raises(ValueError):
        historical_daily_capabilities(volume_profile_available=True)
    capabilities = historical_daily_capabilities(
        daily_features_available=True, volume_profile_available=True
    )
    fields = {f.column: f for f in capabilities.fields}
    assert capabilities.version == "daily_volume_profile_v1"
    assert fields["vp90_total_vol"].unit == "shares"
    assert fields["vp90_vwap"].unit == "session_price"
    assert fields["vp90_concentration_top5_pct"].unit == "percent"
    assert all(fields[c].tracking_supported for c in _COLUMNS)


def test_sealed_vp_exactly_reuses_original_profile_and_preserves_sparse_coverage(
    tmp_path: Path,
) -> None:
    _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        open_factor_daily_feature_source,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.volume_profile import calculate_volume_profile_outcome

    source, prepared, path = _source(tmp_path)
    assert source.schema_version == 7
    assert source.generation == prepared.receipt.generation
    assert FactorDailyFeatureSource.model_validate_json(source.model_dump_json()) == source
    with DuckDBStore(path, read_only=True) as original:
        profile = calculate_volume_profile_outcome(
            original, "000001.SZ", reference_date=_FIRST, lookback_days=90
        ).profile
    assert profile is not None
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        facts = _batch(source, lease).facts
        assert {f.column: f.value for f in facts} == {
            c: getattr(profile, c.removeprefix("vp90_")) for c in _COLUMNS
        }
        assert all(f.volume_profile_diagnostic.window_days == 90 for f in facts)
        assert all(f.volume_profile_diagnostic.observed_days == 5 for f in facts)
        assert all(f.volume_profile_diagnostic.reference_date == _FIRST for f in facts)
        assert all(f.volume_profile_diagnostic.minute_rows == 5 for f in facts)
    assert lease.closed and not lease._private_root.exists()


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("DELETE FROM minute_bar", "missing_minute_data"),
        ("DELETE FROM adj_factor WHERE trade_date<?", "missing_required_factor"),
        ("UPDATE adj_factor SET adj_factor=0 WHERE trade_date=?", "non_positive_reference_factor"),
        ("UPDATE minute_bar SET vol=0,amount=0", "non_positive_profile_totals"),
    ],
)
def test_unavailable_vp_keeps_original_reason(tmp_path: Path, sql: str, reason: str) -> None:
    _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, _, _ = _source(
        tmp_path, mutate=lambda c: c.execute(sql, [_FIRST] if "?" in sql else [])
    )
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        assert all(f.value is None and f.reason == reason for f in _batch(source, lease).facts)


def test_vp_input_and_output_budgets_fail_without_shrinking_the_domain(tmp_path: Path) -> None:
    module = _module()
    prepared, _ = _prepared(tmp_path)
    for bounds in ({"max_input_rows": 1}, {"max_code_rows": 2}, {"max_output_cells": 1}):
        with pytest.raises(ValueError, match="budget"):
            module.prepare_factor_volume_profile_source(
                module.FactorVolumeProfilePrepareRequest(prepared_source=prepared, **bounds),
                lake_root=tmp_path / "lake",
                now=lambda: _AS_OF,
            )
    assert not list((tmp_path / "lake").glob(".vp-prepare-*"))


def test_empty_original_date_window_does_not_disclose_future_minute_coverage(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    def mutate(raw: object) -> None:
        raw.execute("DELETE FROM daily_bar WHERE trade_date<?", [_FIRST])
        raw.execute("UPDATE minute_bar SET trade_time=trade_time+INTERVAL 7 DAY")

    source, _, _ = _source(tmp_path, mutate=mutate)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        for fact in _batch(source, lease).facts:
            assert fact.value is None and fact.reason == "no_trading_dates"
            assert fact.volume_profile_diagnostic.window_days == 0
            assert fact.volume_profile_diagnostic.minute_rows == 0
            assert fact.volume_profile_diagnostic.outside_window_days == 0


@pytest.mark.parametrize("column", ("vol", "amount"))
def test_original_internal_null_volume_or_amount_handling_is_unchanged(
    tmp_path: Path, column: str
) -> None:
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source
    from rquant.storage.duckdb import DuckDBStore
    from rquant.volume_profile import calculate_volume_profile_outcome

    source, _, path = _source(
        tmp_path,
        mutate=lambda c: c.execute(
            f"UPDATE minute_bar SET {column}=NULL WHERE cast(trade_time AS DATE)=?",
            [_FIRST - timedelta(days=1)],
        ),
    )
    with DuckDBStore(path, read_only=True) as original:
        profile = calculate_volume_profile_outcome(
            original, "000001.SZ", reference_date=_FIRST, lookback_days=90
        ).profile
    assert profile is not None
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        assert {f.column: f.value for f in _batch(source, lease).facts} == {
            c: getattr(profile, c.removeprefix("vp90_")) for c in _COLUMNS
        }


@pytest.mark.parametrize("column", ("vol", "amount"))
def test_non_finite_original_outputs_are_unavailable_without_zero_fill(
    tmp_path: Path, column: str
) -> None:
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, _, _ = _source(
        tmp_path, mutate=lambda c: c.execute(f"UPDATE minute_bar SET {column}='Infinity'")
    )
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        for fact in _batch(source, lease).facts:
            assert fact.value is None and fact.reason == "volume_profile_non_finite"
            assert fact.status == "non_finite"


def test_cli_explicitly_seals_vp_without_settings(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    _module()
    from rquant.factor.run_entry import main
    from rquant.strict_json import canonical_json_bytes

    prepared, _ = _prepared(tmp_path)
    captured = tmp_path / "prepared.json"
    captured.write_bytes(canonical_json_bytes(prepared.model_dump(mode="json", round_trip=True)))
    captured.chmod(0o600)
    (tmp_path / "files").mkdir(mode=0o700)
    assert (
        main(
            [
                "seal-volume-profile",
                "--root",
                str(tmp_path / "files"),
                "--prepared-source",
                str(captured),
                "--lake-root",
                str(tmp_path / "lake"),
            ]
        )
        == 0
    )
    assert "daily-feature-source-v7" in capsys.readouterr().out


def _adapter_request(
    source: object,
    prepared: object,
    *,
    expression: str = "ref(vp90_vwap, 1) + close",
    days: tuple | None = None,
) -> object:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.time_series import DecisionTime
    from tests.unit.test_factor_stream_adapter import _at

    definition = build_factor_definition(
        factor_id="original_vp",
        name_zh="90日价量分布",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression=expression,
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True, volume_profile_available=True
        ).feature_catalog(),
    )
    days = days or tuple(_FIRST + timedelta(days=i) for i in (1, 2, 3, 4))
    columns = tuple(c for c in definition.dependency_columns if c != "close")
    return FactorStreamAdapterRequest(
        source=prepared.admission_request,
        scope_content_hash=prepared.scope_content_hash,
        formula=FactorFormulaStreamRequest(
            definition=definition,
            computation_stock_codes=prepared.receipt.request.scope.stock_codes,
            trading_days=days,
            decision_times=tuple(
                DecisionTime(trade_date=d, decision_at=_at(d, 9, 25)) for d in days
            ),
            as_of=_AS_OF,
            selection="all",
            sources=FactorFormulaStreamSources(
                source_mode="historical_retrospective",
                feature_source_id=prepared.snapshot.snapshot_id,
                feature_source_sha256=prepared.binding.binding_hash,
                security_source_id="synthetic-security-archive",
                security_source_sha256="b" * 64,
                daily_features=source.select(columns),
            ),
        ),
        evaluation_days=days[1:],
        holding_sessions=1,
        daily_feature_source=source,
    )


def _configured(
    tmp_path: Path, *, expression: str = "ref(vp90_vwap, 1) + close", mutate: object = None
) -> tuple:
    from tests.unit import test_factor_market_temperature_source as helpers

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(helpers, "_source", _source)
        patch.setattr(helpers, "_adapter_request", _adapter_request)
        return _temperature_configured(tmp_path, expression=expression, mutate=mutate)


def test_vp_original_worker_and_display_preserve_exact_values_and_coverage(tmp_path: Path) -> None:
    _module()
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        run_configured_factor_worker,
    )
    from rquant.factor.run_plan import compile_factor_run_plan
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts

    root, reference, browser, _, config, source = _configured(tmp_path)
    assert set(_COLUMNS) <= set(
        FactorRunPageControlBackend(root, reference).capabilities().feature_catalog().columns
    )
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
    display = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    ).display
    assert display.daily_features.volume_profile == source.volume_profile.summary()
    assert all(day.panel_date < day.trade_date for day in display.daily_feature_coverage_days)
    assert all(len(day.volume_profile_values) == 10 for day in display.daily_feature_coverage_days)
    assert all(
        s.diagnostic.observed_days == 5
        for day in display.daily_feature_coverage_days
        for s in day.volume_profile_values
    )


def test_vp_incremental_repeat_and_cancel_preserve_original_history(tmp_path: Path) -> None:
    _module()
    from tests.unit import test_factor_market_temperature_source as helpers

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(helpers, "_configured", _configured)
        helpers.test_tracking_first_then_continued_equals_whole_and_cancel_preserves_history(
            tmp_path
        )


def test_vp_prefix_keeps_used_minute_and_coverage_but_ignores_future_tail(tmp_path: Path) -> None:
    _module()
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.run_plan import compile_factor_run_plan
    from rquant.factor.tracking_runner import read_factor_tracking_prefix

    prefixes = []
    for name in ("original", "future", "changed"):
        folder = tmp_path / name
        folder.mkdir(mode=0o700)

        def mutate(raw: object, name: str = name) -> None:
            if name == "future":
                raw.execute(
                    "INSERT INTO minute_bar VALUES('000001.SZ',?,'1min',"
                    "999.,999.,999.,999.,999.,999999.,'tushare')",
                    [_FIRST + timedelta(days=7)],
                )
            elif name == "changed":
                raw.execute(
                    "DELETE FROM minute_bar WHERE ts_code='000001.SZ' "
                    "AND cast(trade_time AS DATE)=?",
                    [_FIRST - timedelta(days=1)],
                )

        root, reference, browser, _, config, _ = _configured(folder, mutate=mutate)
        plan = compile_factor_run_plan(
            root,
            reference,
            browser,
            verified_registry_instance_id=config.registry_identity.instance_id,
            clock=lambda: _AS_OF,
        )
        with open_factor_run_configuration(root, reference) as loaded:
            prefix, witness = read_factor_tracking_prefix(loaded, plan.spec)
            witness.recheck()
            prefixes.append(prefix)
    assert prefixes[0] == prefixes[1]
    assert prefixes[0] != prefixes[2]
