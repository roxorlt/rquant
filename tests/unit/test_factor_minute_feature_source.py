"""Offline minute observations retain the original kernel and exact target clock."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from pathlib import Path

import pytest


def _raw(tmp_path: Path, *, count: int = 6) -> Path:
    import duckdb

    from tests.unit.test_factor_source_prepare import _sidecar
    from tests.unit.test_factor_stock_feature_source import _FIRST
    from tests.unit.test_factor_stock_feature_source import _raw as stock_raw

    path = stock_raw(tmp_path, count=count)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "CREATE TABLE minute_bar(ts_code VARCHAR,trade_time TIMESTAMP,freq VARCHAR,"
            "open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,vol DOUBLE,amount DOUBLE,"
            "source VARCHAR,PRIMARY KEY(ts_code,trade_time,freq,source))"
        )
        rows = []
        for code_index in range(1, count + 1):
            code = f"{code_index:06d}.SZ"
            for d in range(235, 261):
                if code_index in (3, 6) and d < 255 or code_index == 6:
                    continue
                clocks = (time(9, 30), time(14, 58), time(14, 59), time(15))
                for j, clock in enumerate(clocks):
                    if (
                        code_index == 2
                        and clock == time(15)
                        or code_index == 5
                        and d < 255
                        and clock == time(15)
                    ):
                        continue
                    amount = (
                        0.0
                        if code_index == 4 and d < 255
                        else float((j + 1) * 10 + (100 if d >= 255 else 0))
                    )
                    rows.append(
                        (
                            code,
                            datetime.combine(_FIRST + timedelta(days=d), clock),
                            "1min",
                            10.0,
                            11.0,
                            9.0,
                            10.0,
                            100.0,
                            amount,
                            "tushare",
                        )
                    )
                    if code_index == 1:
                        rows.append((*rows[-1][:-2], 9999.0, "tushare_rt"))
        raw.executemany("INSERT INTO minute_bar VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    _sidecar(path)
    return path


def _source(tmp_path: Path, *, base: bool = False, **limits: int) -> tuple:
    from rquant.factor.daily_feature_source import FactorMinuteFeaturePrepareRequest
    from rquant.factor.minute_feature_source import prepare_factor_minute_feature_source
    from tests.unit.test_factor_stock_feature_source import _AS_OF, _prepared

    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    paired = None
    if base:
        from rquant.factor.daily_feature_source import FactorStockFeaturePrepareRequest
        from rquant.factor.stock_feature_source import prepare_factor_stock_feature_source
        from rquant.factor.technical_history_source import (
            FactorTechnicalHistoryPrepareRequest,
            prepare_factor_technical_history_source,
        )

        technical = prepare_factor_technical_history_source(
            FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=1),
        )
        paired = prepare_factor_stock_feature_source(
            FactorStockFeaturePrepareRequest(prepared_source=prepared, base_daily_source=technical),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=2),
        )
    source = prepare_factor_minute_feature_source(
        FactorMinuteFeaturePrepareRequest(
            prepared_source=prepared, base_daily_source=paired, **limits
        ),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=3),
    )
    return source, prepared, path


def test_pure_frame_entry_matches_store_kernel(tmp_path: Path) -> None:
    import pandas as pd

    from rquant.stock_features import (
        build_intraday_relative_volume_features,
        build_intraday_relative_volume_features_from_history,
    )
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_factor_stock_feature_source import _FIRST

    path = _raw(tmp_path)
    signal = datetime.combine(_FIRST + timedelta(days=255), time(15))
    with DuckDBStore(path, read_only=True) as store:
        previous = tuple(_FIRST + timedelta(days=d) for d in range(235, 255))
        minutes = store.query_minute_bars(
            "000001.SZ",
            datetime.combine(previous[0], time(9, 30)),
            datetime.combine(previous[-1], time(15)),
        )
        args = dict(
            current_minute_amount=140.0,
            current_cum_amount=500.0,
            current_day_amounts=[
                (time(9, 30), 110.0),
                (time(14, 58), 120.0),
                (time(14, 59), 130.0),
                (time(15), 140.0),
            ],
        )
        core = build_intraday_relative_volume_features(store, "000001.SZ", signal, **args)
        assert (
            build_intraday_relative_volume_features_from_history(minutes, previous, signal, **args)
            == core
        )
        assert (
            build_intraday_relative_volume_features_from_history(
                pd.DataFrame(), (), signal, **args
            )["hist_intraday_days_20d"]
            == 0
        )


def test_exact_target_all_fields_missing_and_single_kernel_values(tmp_path: Path) -> None:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        open_factor_daily_feature_source,
    )
    from rquant.factor.minute_feature_source import MINUTE_FEATURE_COLUMNS
    from rquant.stock_features import build_intraday_relative_volume_features
    from rquant.storage.duckdb import DuckDBStore

    source, prepared, path = _source(tmp_path, base=True)
    assert source.schema_version == 4 and len(source.fields) == 50
    source.require_prepared(prepared)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        day = source.scope.start_date
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=day,
                stock_codes=source.scope.stock_codes,
                fields=MINUTE_FEATURE_COLUMNS,
            )
        )
        facts = {(f.stock_code, f.column): f for f in batch.facts}
        for code in ("000002.SZ", "000006.SZ"):
            assert all(
                facts[code, c].value is None and facts[code, c].reason == "missing_target_minute"
                for c in MINUTE_FEATURE_COLUMNS
            )
        with DuckDBStore(path, read_only=True) as store:
            for code in ("000001.SZ", "000003.SZ", "000004.SZ", "000005.SZ"):
                core = build_intraday_relative_volume_features(
                    store,
                    code,
                    datetime.combine(day, time(15)),
                    current_minute_amount=140.0,
                    current_cum_amount=500.0,
                    current_day_amounts=[
                        (time(9, 30), 110.0),
                        (time(14, 58), 120.0),
                        (time(14, 59), 130.0),
                    ],
                )
                assert {c: facts[code, c].value for c in MINUTE_FEATURE_COLUMNS} == core
        assert facts["000003.SZ", "hist_intraday_days_20d"].value == 0
        assert facts["000003.SZ", "signal_rel_cum_amount_asof_20d"].reason == "missing_history"
        assert (
            facts["000004.SZ", "signal_rel_amount_same_minute_20d"].reason
            == "zero_same_minute_baseline"
        )
        assert (
            facts["000005.SZ", "signal_rel_amount_same_minute_20d"].reason
            == "missing_same_minute_history"
        )
        assert facts["000001.SZ", "signal_opening_segment"].value == 0
        assert facts["000001.SZ", "signal_opening_segment_amount"].reason == "not_applicable"
        assert all(f.minute_diagnostic is not None for f in batch.facts)
    assert lease.closed and not lease._private_root.exists()


@pytest.mark.parametrize("limit", ("max_input_rows", "max_code_rows", "max_output_cells"))
def test_bounded_input_and_output_fail_before_publishing(tmp_path: Path, limit: str) -> None:
    with pytest.raises(ValueError, match="budget"):
        _source(tmp_path, **{limit: 1})
    assert not list((tmp_path / "lake").glob("tables/daily_minute_feature/versions/*"))


def test_catalog_and_standalone_source_preserve_original_six_fields(tmp_path: Path) -> None:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.minute_feature_source import MINUTE_FEATURE_COLUMNS

    source, _, _ = _source(tmp_path)
    assert source.base_daily_source is None and len(source.fields) == 11
    assert not set(MINUTE_FEATURE_COLUMNS) & set(
        historical_daily_capabilities().feature_catalog().columns
    )
    with pytest.raises(ValueError):
        historical_daily_capabilities(minute_features_available=True)
    caps = historical_daily_capabilities(
        daily_features_available=True, minute_features_available=True
    )
    assert len(caps.fields) == 17
    assert {f.column: f.unit for f in caps.fields}["signal_minute_amount"] == "CNY"
    assert (
        source.minute_features.codes[0].input_rows
        == 2 * source.minute_features.codes[0].input_observations
    )


def test_hot_reads_keep_first_full_validation_and_check_tampered_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import minute_feature_source as module
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, _, _ = _source(tmp_path)
    original = module.verify_materialized_table_artifact
    calls = []

    def measured(*args: object, **kwargs: object) -> object:
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "verify_materialized_table_artifact", measured)
    module._VALIDATED.clear()
    for _ in range(2):
        with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
            assert not lease.closed
    assert len(calls) == 1
    artifact = source.minute_features.inputs[0]
    path = tmp_path / "lake" / artifact.relative_path
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 1
    path.write_bytes(data)
    with (
        pytest.raises(ValueError, match="hash"),
        open_factor_daily_feature_source(source, lake_root=tmp_path / "lake"),
    ):
        pass
    assert not list((tmp_path / "lake").glob(".daily-feature-reader-*"))


def test_nullable_amount_frame_keeps_original_float64_kernel_semantics(tmp_path: Path) -> None:
    import duckdb

    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        FactorMinuteFeaturePrepareRequest,
        open_factor_daily_feature_source,
    )
    from rquant.factor.minute_feature_source import (
        MINUTE_FEATURE_COLUMNS,
        prepare_factor_minute_feature_source,
    )
    from tests.unit.test_factor_source_prepare import _sidecar
    from tests.unit.test_factor_stock_feature_source import _AS_OF, _prepared

    path = _raw(tmp_path)
    with duckdb.connect(str(path)) as raw:
        raw.execute("UPDATE minute_bar SET amount=NULL WHERE ts_code='000001.SZ'")
    _sidecar(path)
    prepared = _prepared(tmp_path, path)
    source = prepare_factor_minute_feature_source(
        FactorMinuteFeaturePrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=3),
    )
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=source.scope.start_date,
                stock_codes=("000001.SZ",),
                fields=MINUTE_FEATURE_COLUMNS,
            )
        )
    facts = {f.column: f for f in batch.facts}
    assert facts["signal_minute_amount"].reason == "undefined_statistic"
    assert facts["signal_cum_amount_asof"].value == 0
    assert facts["hist_cum_amount_asof_median_20d"].value == 0
    assert facts["signal_rel_cum_amount_asof_20d"].reason == "zero_cumulative_baseline"
    assert facts["hist_intraday_days_20d"].value == 20
    assert facts["signal_opening_segment"].value == 0
    assert facts["signal_minute_amount"].minute_diagnostic.target_present
