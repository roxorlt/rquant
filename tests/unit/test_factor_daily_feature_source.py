"""Stored indicators and daily facts retain their original units and missing states."""

from __future__ import annotations

import importlib
import importlib.util
import math
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.factor.source_prepare import prepare_factor_stream_source
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST, _replica, _request, _sidecar

_INDICATOR = (
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "rsi6",
    "rsi14",
    "macd",
    "macd_signal",
    "macd_hist",
    "kdj_k",
    "kdj_d",
    "kdj_j",
)
_BASIC = ("turnover_rate", "volume_ratio", "total_mv", "circ_mv")
_FIELDS = tuple(sorted(_INDICATOR + _BASIC))
_READ_AT = _AS_OF + timedelta(minutes=5)


def _module() -> object:
    name = "rquant.factor.daily_feature_source"
    assert importlib.util.find_spec(name) is not None, "stored daily feature source is missing"
    return importlib.import_module(name)


def _prepared(
    tmp_path: Path,
    *,
    count: int = 8,
    days: int = 2,
    schema: str = "valid",
    mutate: Callable | None = None,
) -> object:
    path = _replica(tmp_path, count=count, days=days)
    with duckdb.connect(str(path)) as connection:
        for table, fields in (("daily_indicator", _INDICATOR), ("daily_basic", _BASIC)):
            columns = ", ".join(f"{field} DOUBLE" for field in fields)
            if schema == "missing_column" and table == "daily_indicator":
                columns = columns.replace("ma5 DOUBLE, ", "")
            key = "" if schema == "duplicate_key" else ", PRIMARY KEY(ts_code, trade_date)"
            date_type = "VARCHAR" if schema == "date_type" else "DATE"
            connection.execute(
                f"CREATE TABLE {table}(ts_code VARCHAR, trade_date {date_type}, {columns}{key})"
            )
            if schema == "missing_column":
                continue
            for offset in range(days):
                for number in range(1, count + 1):
                    if count == 8 and number == 8:
                        continue
                    value = number + offset * 10.0
                    if count == 8:
                        value = {
                            2: None,
                            3: float("nan"),
                            4: float("inf"),
                            5: float("-inf"),
                            6: -1.5,
                            7: 0.0,
                        }.get(number, value)
                    values = [value] * len(fields)
                    if number == 1:
                        values = [
                            dict(
                                turnover_rate=0.5387,
                                volume_ratio=1.23,
                                total_mv=12345.6789,
                                circ_mv=9876.5432,
                                macd_hist=-0.75,
                                kdj_j=125.0,
                            ).get(field, value)
                            for field in fields
                        ]
                    connection.execute(
                        f"INSERT INTO {table} VALUES "
                        f"({','.join('?' for _ in range(2 + len(fields)))})",
                        [f"{number:06d}.SZ", _FIRST + timedelta(days=offset), *values],
                    )
            if schema == "duplicate_key":
                connection.execute(f"INSERT INTO {table} SELECT * FROM {table} LIMIT 1")
        if mutate is not None:
            mutate(connection)
    _sidecar(path)
    (tmp_path / "lake").mkdir(mode=0o700)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        return prepare_factor_stream_source(
            _request(path, count=count, days=days),
            metadata_store=metadata,
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF,
        )


def _source(
    tmp_path: Path, *, count: int = 8, days: int = 2, mutate: Callable | None = None
) -> tuple[object, object, Path]:
    m = _module()
    prepared = _prepared(tmp_path, count=count, days=days, mutate=mutate)
    root = tmp_path / "lake"
    source = m.prepare_factor_daily_feature_source(
        m.FactorDailyFeaturePrepareRequest(prepared_source=prepared),
        lake_root=root,
        now=lambda: _READ_AT,
    )
    return source, prepared, root


def test_two_tables_sixteen_fields_match_raw_sql_and_preserve_states_units(tmp_path: Path) -> None:
    m = _module()
    source, prepared, root = _source(tmp_path)
    assert source.prepared_source_sha256 == prepared.sha256
    assert source.prepared_snapshot_id == prepared.snapshot.snapshot_id
    assert source.prepared_binding_hash == prepared.binding.binding_hash
    assert source.scope_content_hash == prepared.scope_content_hash
    assert source.scope == prepared.receipt.request.scope
    assert source.generation == prepared.receipt.generation
    assert source.code_commit == prepared.snapshot.code_commit
    assert source.value_semantics == "stored_not_recomputed"
    assert source.price_basis == source.recursive_initialization == "unverified"
    assert source.source_mode == "historical_retrospective"
    assert source.observed_at == source.completed_read_at == _READ_AT
    assert len(prepared.binding.manifest.artifacts) == 4
    assert tuple(t.table_name for t in prepared.receipt.tables) == (
        "daily_bar",
        "adj_factor",
        "trade_calendar",
    )
    assert m.FactorDailyFeatureSource.model_validate_json(source.model_dump_json()) == source
    with (
        duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as raw,
        m.open_factor_daily_feature_source(source, lake_root=root) as lease,
    ):
        for offset in (1, 0):
            day = _FIRST + timedelta(days=offset)
            batch = lease.query(
                m.FactorDailyFeatureQuery(
                    source_sha256=source.sha256,
                    trade_date=day,
                    stock_codes=source.scope.stock_codes,
                    fields=_FIELDS,
                )
            )
            assert len(batch.facts) == 8 * 16
            for table, fields in (("daily_indicator", _INDICATOR), ("daily_basic", _BASIC)):
                values = {
                    row[0]: dict(zip(fields, row[1:], strict=True))
                    for row in raw.execute(
                        f"SELECT ts_code, {','.join(fields)} FROM {table} WHERE trade_date=?", [day]
                    ).fetchall()
                }
                for fact in (f for f in batch.facts if f.column in fields):
                    if fact.stock_code not in values:
                        assert fact.status == "missing" and fact.value is None
                        continue
                    expected = values[fact.stock_code][fact.column]
                    if expected is None:
                        assert fact.status == "null" and fact.value is None
                    elif not math.isfinite(expected):
                        assert fact.status == "non_finite" and fact.value is None
                        assert fact.non_finite_value == (
                            "NaN"
                            if math.isnan(expected)
                            else "Infinity"
                            if expected > 0
                            else "-Infinity"
                        )
                    else:
                        assert fact.status == "valid" and fact.value == expected
            first = {f.column: f.value for f in batch.facts if f.stock_code == "000001.SZ"}
            assert first["turnover_rate"] == 0.5387
            assert first["macd_hist"] == -0.75 and first["kdj_j"] == 125.0
            assert first["total_mv"] == 12345.6789
            assert (
                m.FactorDailyFeatureDayBatch.model_validate_json(batch.model_dump_json()) == batch
            )
            assert all(
                c.valid == 3 and c.null == 1 and c.non_finite == 3 and c.missing == 1
                for c in batch.counts
            )
    assert lease.closed and not lease._private_root.exists()
    assert not list(root.glob(".daily-feature-*"))


@pytest.mark.parametrize("schema", ["missing_column", "date_type", "duplicate_key"])
def test_schema_or_business_key_mismatch_has_no_source(tmp_path: Path, schema: str) -> None:
    m = _module()
    prepared = _prepared(tmp_path, schema=schema)
    with pytest.raises(ValueError):
        m.prepare_factor_daily_feature_source(
            m.FactorDailyFeaturePrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "features",
            now=lambda: _READ_AT,
        )
    assert not list((tmp_path / "features").rglob("*.parquet"))


def test_changed_replica_generation_refuses_pairing(tmp_path: Path) -> None:
    m = _module()
    prepared = _prepared(tmp_path)
    _sidecar(prepared.receipt.request.replica_path)
    with pytest.raises(ValueError, match="generation"):
        m.prepare_factor_daily_feature_source(
            m.FactorDailyFeaturePrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
        )


def test_prepare_uses_one_ro_transaction_and_no_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    from tests.unit.test_factor_source_prepare import _PRIMARY, _ConnectionProbe

    prepared = _prepared(tmp_path)
    original_stat = Path.stat

    def guard(path: Path, *args: object, **kwargs: object) -> object:
        assert path != _PRIMARY
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", guard)
    connect = m.connect_pinned_readonly
    opened = []

    def track(path: Path, descriptor: int) -> tuple[object, str]:
        connection, mode = connect(path, descriptor)
        probe = _ConnectionProbe(connection)
        opened.append(probe)
        return probe, mode

    monkeypatch.setattr(m, "connect_pinned_readonly", track)
    m.prepare_factor_daily_feature_source(
        m.FactorDailyFeaturePrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _READ_AT,
    )
    assert len(opened) == 1
    assert opened[0].commands.count("BEGIN TRANSACTION") == opened[0].commands.count("COMMIT") == 1
    assert sum(c.lstrip().startswith("COPY") for c in opened[0].commands) == 2
    assert opened[0].closed


def test_query_selects_only_dependencies_and_has_500_code_bound(tmp_path: Path) -> None:
    m = _module()
    source, _, root = _source(tmp_path, count=501)
    with m.open_factor_daily_feature_source(source, lake_root=root) as lease:
        for codes in (source.scope.stock_codes[:500], source.scope.stock_codes[500:]):
            batch = lease.query(
                m.FactorDailyFeatureQuery(
                    source_sha256=source.sha256,
                    trade_date=_FIRST,
                    stock_codes=codes,
                    fields=("ma5", "turnover_rate"),
                )
            )
            assert len(batch.facts) == len(codes) * 2
            assert {f.column for f in batch.facts} == {"ma5", "turnover_rate"}
        assert lease.query_count == 2
        with pytest.raises(ValueError):
            m.FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST,
                stock_codes=source.scope.stock_codes,
                fields=("ma5",),
            )
        with pytest.raises(ValueError):
            lease.query(
                m.FactorDailyFeatureQuery(
                    source_sha256="0" * 64,
                    trade_date=_FIRST,
                    stock_codes=source.scope.stock_codes[:1],
                    fields=("ma5",),
                )
            )
        with pytest.raises(ValueError):
            m.FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST,
                stock_codes=source.scope.stock_codes[:1],
                fields=("close",),
            )
    with pytest.raises(RuntimeError):
        lease.query(batch.query)


def test_private_corruption_or_natural_tail_change_refuses_and_closes(tmp_path: Path) -> None:
    m = _module()
    source, _, root = _source(tmp_path)
    artifact = source.tables[0].artifact
    path = root / artifact.relative_path
    with (
        pytest.raises(ValueError),
        m.open_factor_daily_feature_source(source, lake_root=root) as lease,
    ):
        lease.query(
            m.FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST,
                stock_codes=source.scope.stock_codes,
                fields=("ma5",),
            )
        )
        path.write_bytes(path.read_bytes() + b"changed")
    assert lease.closed and not lease._private_root.exists()
    with pytest.raises(ValueError), m.open_factor_daily_feature_source(source, lake_root=root):
        pytest.fail("corrupt original must not open")


def test_generation_changes_after_second_export_refuse_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    from rquant.replica_generation import replica_generation_path

    prepared = _prepared(tmp_path)
    materialize = m.materialize_table_dependency
    calls = []

    def change(*args: object, **kwargs: object) -> object:
        artifact = materialize(*args, **kwargs)
        calls.append(artifact)
        if len(calls) == 2:
            sidecar = replica_generation_path(prepared.receipt.request.replica_path)
            sidecar.write_bytes(sidecar.read_bytes() + b"\n")
        return artifact

    monkeypatch.setattr(m, "materialize_table_dependency", change)
    with pytest.raises(ValueError, match="generation"):
        m.prepare_factor_daily_feature_source(
            m.FactorDailyFeaturePrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
            now=lambda: _READ_AT,
        )
    assert len(calls) == 2 and not list((tmp_path / "lake").glob(".daily-feature-*"))
