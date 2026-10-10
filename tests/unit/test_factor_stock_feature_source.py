"""Stock windows reuse the screening kernel and a paired sealed source."""

from __future__ import annotations

import importlib
import importlib.util
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

_FIRST = date(2026, 1, 1)
_AS_OF = datetime(2026, 10, 2, 8, tzinfo=UTC)
_STOCK_COLUMNS = tuple(
    sorted(
        [
            f"{field}_{n}d{suffix}"
            for n in (90, 120, 250)
            for field, suffix in (
                ("price_window_days", ""),
                ("price_position", "_pct"),
                ("price_rank", "_pct"),
                ("distance_to_high", "_pct"),
                ("distance_to_low", "_pct"),
            )
        ]
        + [
            "accum_window_days_20d",
            "accum_obv_change_20d_pct",
            "accum_ad_flow_20d_pct",
            "accum_up_down_amount_ratio_20d",
            "accum_heavy_no_drop_days_20d",
            "accum_close_position_avg_20d_pct",
            "ma_alignment",
            "price_percentile_250d",
        ]
    )
)


def _module() -> object:
    name = "rquant.factor.stock_feature_source"
    assert importlib.util.find_spec(name) is not None, "paired stock feature producer is missing"
    return importlib.import_module(name)


def _raw(tmp_path: Path, *, count: int = 6, days: int = 280) -> Path:
    import duckdb

    from tests.unit.test_factor_source_prepare import _replica, _sidecar

    path = _replica(tmp_path, count=count, days=days)
    with duckdb.connect(str(path)) as raw:
        raw.execute("UPDATE daily_bar SET trade_date=trade_date-181")
        raw.execute("UPDATE adj_factor SET trade_date=trade_date-181")
        raw.execute(
            "UPDATE trade_calendar SET cal_date=cal_date-181,pretrade_date=pretrade_date-181"
        )
        raw.execute(
            "CREATE TABLE daily_basic(ts_code VARCHAR,trade_date DATE,turnover_rate "
            "DOUBLE,volume_ratio DOUBLE,total_mv DOUBLE,circ_mv DOUBLE,PRIMARY "
            "KEY(ts_code,trade_date))"
        )
        raw.execute(
            "INSERT INTO daily_basic SELECT "
            "ts_code,trade_date,0.5387,1.23,12345.6789,9876.5432 FROM daily_bar"
        )
        raw.execute("ALTER TABLE daily_bar ADD COLUMN pre_close DOUBLE")
        raw.execute("ALTER TABLE daily_bar ADD COLUMN pct_chg DOUBLE")
        raw.execute(
            "UPDATE daily_bar SET open=10+date_diff('day',?,trade_date)*0.03,"
            "close=10+date_diff('day',?,trade_date)*0.03,"
            "high=11+date_diff('day',?,trade_date)*0.03,"
            "low=9+date_diff('day',?,trade_date)*0.03,pre_close=10,"
            "pct_chg=CASE WHEN date_diff('day',?,trade_date)%2=0 THEN 1 ELSE -1 END",
            [_FIRST] * 5,
        )
        raw.execute(
            "UPDATE adj_factor SET adj_factor=2 WHERE trade_date>=?",
            [_FIRST + timedelta(days=245)],
        )
        raw.execute("UPDATE daily_bar SET open=10,high=11,low=9,close=10 WHERE ts_code='000002.SZ'")
        raw.execute("UPDATE adj_factor SET adj_factor=1 WHERE ts_code='000002.SZ'")
        raw.execute(
            "DELETE FROM daily_bar WHERE ts_code='000003.SZ' AND trade_date<?",
            [_FIRST + timedelta(days=249)],
        )
        raw.execute(
            "DELETE FROM adj_factor WHERE ts_code='000004.SZ' AND trade_date=?",
            [_FIRST + timedelta(days=249)],
        )
        raw.execute(
            "DELETE FROM adj_factor WHERE ts_code='000005.SZ' AND trade_date=?",
            [_FIRST + timedelta(days=255)],
        )
        raw.execute("DELETE FROM daily_bar WHERE ts_code='000006.SZ'")
    _sidecar(path)
    return path


def _prepared(
    tmp_path: Path, path: Path, *, count: int = 6, start: int = 255, end: int = 260
) -> object:
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_factor_source_prepare import _request

    request = _request(path, count=count, days=1)
    scope = request.scope.model_copy(
        update={
            "start_date": _FIRST + timedelta(days=start),
            "end_date": _FIRST + timedelta(days=end),
            "as_of_time": _AS_OF,
        }
    )
    request = request.model_copy(update={"scope": scope})
    (tmp_path / "lake").mkdir(mode=0o700, exist_ok=True)
    with DuckDBStore(tmp_path / f"metadata-{start}-{end}.duckdb") as metadata:
        return prepare_factor_stream_source(
            request, metadata_store=metadata, lake_root=tmp_path / "lake", now=lambda: _AS_OF
        )


def _source(tmp_path: Path, **limits: int) -> tuple:
    m = _module()
    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    source = m.prepare_factor_stock_feature_source(
        m.FactorStockFeaturePrepareRequest(prepared_source=prepared, **limits),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    return source, prepared, path


def test_stock_catalog_requires_actual_source_and_has_23_kernel_columns() -> None:
    m = _module()
    from rquant.factor.capability import historical_daily_capabilities

    assert tuple(f.column for f in m.STOCK_FEATURE_FIELDS) == _STOCK_COLUMNS
    assert not set(_STOCK_COLUMNS) & set(historical_daily_capabilities().feature_catalog().columns)
    with pytest.raises(ValueError):
        historical_daily_capabilities(stock_features_available=True)
    caps = historical_daily_capabilities(
        daily_features_available=True, stock_features_available=True
    )
    assert caps.version == "daily_stock_v1"
    assert len(caps.fields) == 29
    fields = {f.column: f for f in caps.fields}
    assert fields["price_percentile_250d"].unit == "ratio"
    assert fields["ma_alignment"].unit == "binary"
    assert fields["accum_window_days_20d"].unit == "observations"


def test_sealed_stock_values_match_single_core_and_keep_window_diagnostics(tmp_path: Path) -> None:
    _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        open_factor_daily_feature_source,
    )
    from rquant.stock_features import build_daily_stock_feature_result
    from rquant.storage.duckdb import DuckDBStore

    source, prepared, path = _source(tmp_path)
    assert source.schema_version == 3 and source.value_semantics == "stock_features_derived"
    assert source.stock_features.policy.max_observations == 250
    assert source.generation == prepared.receipt.generation
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        day = source.scope.start_date
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=day,
                stock_codes=source.scope.stock_codes,
                fields=_STOCK_COLUMNS,
            )
        )
        with DuckDBStore(path, read_only=True) as raw:
            for code in source.scope.stock_codes:
                core = build_daily_stock_feature_result(raw, code, day)
                facts = {f.column: f for f in batch.facts if f.stock_code == code}
                for column in _STOCK_COLUMNS:
                    assert facts[column].value == core.features.get(column)
                    assert facts[column].diagnostic is not None
        facts = {(f.stock_code, f.column): f for f in batch.facts}
        assert facts["000002.SZ", "price_position_90d_pct"].value == 50
        assert facts["000002.SZ", "price_rank_90d_pct"].value == 100
        assert facts["000002.SZ", "ma_alignment"].value == 0
        assert facts["000002.SZ", "price_percentile_250d"].value == 1
        assert facts["000003.SZ", "price_window_days_90d"].value == 7
        assert facts["000003.SZ", "ma_alignment"].reason == "insufficient_history"
        assert facts["000004.SZ", "price_position_90d_pct"].reason == "missing_required_factor"
        assert facts["000004.SZ", "accum_ad_flow_20d_pct"].status == "valid"
        assert facts["000005.SZ", "price_rank_90d_pct"].reason == "missing_reference_factor"
        assert facts["000005.SZ", "accum_ad_flow_20d_pct"].status == "valid"
        assert facts["000006.SZ", "accum_window_days_20d"].reason == "missing_daily_data"
    assert lease.closed and not lease._private_root.exists()
    assert not list((tmp_path / "lake").glob(".stock-feature-*"))


@pytest.mark.parametrize("limit", ["max_input_rows", "max_code_observations", "max_output_cells"])
def test_preparation_refuses_bounded_capacity_before_publishing(tmp_path: Path, limit: str) -> None:
    with pytest.raises(ValueError, match="budget"):
        _source(tmp_path, **{limit: 1})
    assert not list((tmp_path / "lake").glob("tables/daily_stock_feature/versions/*"))


def test_pure_history_prefix_rounding_and_reference_day_exclusion(tmp_path: Path) -> None:
    import duckdb
    import pandas as pd

    from rquant.stock_features import build_daily_stock_feature_result_from_history

    path = _raw(tmp_path)
    day = _FIRST + timedelta(days=255)
    with duckdb.connect(str(path), read_only=True) as raw:
        daily = raw.execute(
            "SELECT * FROM daily_bar WHERE ts_code='000001.SZ' ORDER BY trade_date"
        ).fetchdf()
        factors = {
            d: f
            for d, f in raw.execute(
                "SELECT trade_date,adj_factor FROM adj_factor WHERE ts_code='000001.SZ'"
            ).fetchall()
        }
    result = build_daily_stock_feature_result_from_history(daily, factors, "000001.SZ", day)
    truncated = daily[daily.trade_date.dt.date <= day]
    assert result == build_daily_stock_feature_result_from_history(
        truncated, factors, "000001.SZ", day
    )
    amended = daily.copy()
    amended.loc[amended.trade_date.dt.date == day, ["vol", "amount", "pct_chg"]] = [1e12, 1e12, -99]
    reference = build_daily_stock_feature_result_from_history(amended, factors, "000001.SZ", day)
    assert {c: v for c, v in result.features.items() if c.startswith("accum_")} == {
        c: v for c, v in reference.features.items() if c.startswith("accum_")
    }
    window = truncated.tail(90)
    adjusted = [
        float(r.close) * factors[r.trade_date.date()] / factors[day] for r in window.itertuples()
    ]
    expected = round((adjusted[-1] - min(adjusted)) / (max(adjusted) - min(adjusted)) * 100, 4)
    assert result.features["price_position_90d_pct"] == expected
    assert result.features["price_percentile_250d"] <= 1
    assert result.features["accum_window_days_20d"] == 20
    flat = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": day,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "pre_close": 10.0,
                "vol": 0.0,
                "amount": 0.0,
                "pct_chg": None,
            }
        ]
    )
    empty = build_daily_stock_feature_result_from_history(flat, {day: 1.0}, "000001.SZ", day)
    assert empty.features["accum_window_days_20d"] == 0
    assert empty.features["accum_heavy_no_drop_days_20d"] == 0
    assert empty.features["accum_ad_flow_20d_pct"] is None


@pytest.mark.parametrize("failure", ["missing", "bytes", "identity", "private_bytes"])
def test_reader_checks_original_and_private_input_on_each_access_and_closes(
    tmp_path: Path, failure: str
) -> None:
    import os

    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, _, _ = _source(tmp_path)
    artifact = source.stock_features.inputs[0]
    path = tmp_path / "lake" / artifact.relative_path
    leases = []
    with (
        pytest.raises((OSError, ValueError)),
        open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease,
    ):
        leases.append(lease)
        target = (
            lease._private_root / artifact.relative_path if failure == "private_bytes" else path
        )
        if failure == "missing":
            target.unlink()
        elif failure == "identity":
            node = target.stat()
            os.utime(target, ns=(node.st_atime_ns, node.st_mtime_ns + 1_000_000))
        else:
            data = target.read_bytes()
            target.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
    assert all(reader.closed and not reader._private_root.exists() for reader in leases)
    assert not list((tmp_path / "lake").glob(".daily-feature-reader-*"))


def test_hot_reader_reuses_full_logical_input_validation_but_checks_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, _, _ = _source(tmp_path)
    original = m.verify_materialized_table_artifact
    calls = []

    def counted(*args: object, **kwargs: object) -> object:
        calls.append(args[0].table_name)
        return original(*args, **kwargs)

    monkeypatch.setattr(m, "verify_materialized_table_artifact", counted)
    m._VALIDATED.clear()
    for _ in range(3):
        with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake"):
            pass
    assert calls == ["stock_feature_input"]
