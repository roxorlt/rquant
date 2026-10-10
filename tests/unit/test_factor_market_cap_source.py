"""Market cap keeps the paired RO generation, original units and missing facts."""

from __future__ import annotations

import importlib
import importlib.util
import math
import os
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.factor.source_prepare import prepare_factor_stream_source
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_factor_source_prepare import (
    _AS_OF,
    _FIRST,
    _PRIMARY,
    _replica,
    _request,
    _sidecar,
)

_READ_AT = _AS_OF + timedelta(minutes=5)
_CODES = tuple(f"{i:06d}.SZ" for i in range(1, 9))


def _module() -> object:
    name = "rquant.factor.market_cap_source"
    assert importlib.util.find_spec(name) is not None, "daily market cap source is missing"
    return importlib.import_module(name)


def _prepared(tmp_path: Path, *, schema: str = "valid", count: int = 8) -> object:
    path = _replica(tmp_path, count=count, days=2)
    with duckdb.connect(str(path)) as connection:
        if schema == "missing_column":
            connection.execute(
                "CREATE TABLE daily_basic(ts_code VARCHAR, trade_date DATE, "
                "PRIMARY KEY(ts_code, trade_date))"
            )
        elif schema == "date_type":
            connection.execute(
                "CREATE TABLE daily_basic(ts_code VARCHAR, trade_date VARCHAR, "
                "total_mv DOUBLE, PRIMARY KEY(ts_code, trade_date))"
            )
        elif schema == "duplicate_key":
            connection.execute(
                "CREATE TABLE daily_basic(ts_code VARCHAR, trade_date DATE, total_mv DOUBLE)"
            )
            connection.execute(
                "INSERT INTO daily_basic VALUES ('000001.SZ', ?, 1), ('000001.SZ', ?, 2)",
                [_FIRST, _FIRST],
            )
        else:
            connection.execute(
                "CREATE TABLE daily_basic(ts_code VARCHAR, trade_date DATE, "
                "total_mv DOUBLE, circ_mv DOUBLE, PRIMARY KEY(ts_code, trade_date))"
            )
            if count != 8:
                connection.execute(
                    "INSERT INTO daily_basic SELECT ts_code, trade_date, 1000., 500. FROM daily_bar"
                )
            else:
                values = (
                    (12345.6789, None, 0.0, -11.5, float("inf"), float("nan"), float("-inf")),
                    (24691.3578, 1.5, None, None, 0.0, -2.0, 90000.0, 80000.0),
                )
                for offset, day_values in enumerate(values):
                    for number, value in enumerate(day_values, 1):
                        if offset == 1 and number == 4:
                            continue
                        connection.execute(
                            "INSERT INTO daily_basic VALUES (?, ?, ?, 500.)",
                            [f"{number:06d}.SZ", _FIRST + timedelta(days=offset), value],
                        )
                connection.execute(
                    "INSERT INTO daily_basic VALUES ('999999.SZ', ?, 111., 22.), "
                    "('000001.SZ', ?, 999., 22.)",
                    [_FIRST, _FIRST - timedelta(days=1)],
                )
    _sidecar(path)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        return prepare_factor_stream_source(
            _request(path, count=count, days=2),
            metadata_store=metadata,
            lake_root=tmp_path / "prices",
            now=lambda: _AS_OF,
        )


def _source(tmp_path: Path, *, count: int = 8) -> tuple[object, object, Path]:
    m = _module()
    prepared = _prepared(tmp_path, count=count)
    root = tmp_path / "market-cap"
    source = m.prepare_factor_market_cap_source(
        m.FactorMarketCapPrepareRequest(prepared_source=prepared),
        lake_root=root,
        now=lambda: _READ_AT,
    )
    return source, prepared, root


def _query(
    m: object, source: object, *, offset: int = 0, codes: tuple[str, ...] = _CODES
) -> object:
    return m.FactorMarketCapQuery(
        source_sha256=source.sha256,
        trade_date=_FIRST + timedelta(days=offset),
        stock_codes=codes,
    )


def test_two_days_match_raw_sql_units_binding_and_every_missing_state(tmp_path: Path) -> None:
    m = _module()
    source, prepared, root = _source(tmp_path)
    assert source.prepared_source_sha256 == prepared.sha256
    assert source.prepared_snapshot_id == prepared.snapshot.snapshot_id
    assert source.prepared_binding_hash == prepared.binding.binding_hash
    assert source.scope == prepared.receipt.request.scope
    assert source.generation == prepared.receipt.generation
    assert source.scope_content_hash == prepared.scope_content_hash
    assert source.code_commit == prepared.snapshot.code_commit
    assert source.unit == "CNY_10000" and source.value_field == "total_mv"
    assert source.source_mode == "historical_retrospective"
    assert source.observed_at == source.completed_read_at == _READ_AT
    assert source.artifact.row_count == 14
    assert source.observation.structural_missing_rows == 2
    assert source.observation.null_rows == 2
    assert source.observation.non_positive_rows == 4
    assert source.observation.non_finite_rows == 3
    assert source.observation.valid_rows == 5
    assert len(source.observation.code_counts) == 8
    assert tuple(table.table_name for table in prepared.receipt.tables) == (
        "daily_bar",
        "adj_factor",
        "trade_calendar",
    )
    assert len(prepared.binding.manifest.artifacts) == 4
    restored = m.FactorMarketCapSource.model_validate_json(
        source.model_dump_json(exclude_computed_fields=True)
    )
    assert restored == source
    with (
        duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as original,
        m.open_factor_market_cap_source(source, lake_root=root) as lease,
    ):
        for offset in (1, 0):
            query = _query(m, source, offset=offset)
            batch = lease.query(query)
            raw = dict(
                original.execute(
                    "SELECT ts_code, total_mv FROM daily_basic WHERE trade_date=? "
                    "AND ts_code IN (SELECT unnest(?))",
                    [query.trade_date, list(_CODES)],
                ).fetchall()
            )
            assert batch.unit == "CNY_10000" and batch.source_sha256 == source.sha256
            assert len(batch.facts) == 8 and batch.counts.missing == 1
            assert sum(batch.counts.model_dump().values()) == 8
            for fact in batch.facts:
                assert fact.trade_date == query.trade_date
                if fact.stock_code not in raw:
                    assert fact.status == "missing" and fact.total_mv is None
                    continue
                expected = raw[fact.stock_code]
                if expected is None:
                    assert fact.status == "null" and fact.total_mv is None
                elif not math.isfinite(expected):
                    assert fact.status == "non_finite"
                    assert (
                        math.isnan(expected) and math.isnan(fact.total_mv)
                    ) or fact.total_mv == expected
                else:
                    assert fact.total_mv == expected
                    assert fact.status == ("valid" if expected > 0 else "non_positive")
    assert not list(root.glob(".market-cap-reader-*"))


@pytest.mark.parametrize("schema", ["missing_column", "date_type", "duplicate_key"])
def test_source_schema_or_duplicate_business_key_is_refused(tmp_path: Path, schema: str) -> None:
    m = _module()
    prepared = _prepared(tmp_path, schema=schema)
    root = tmp_path / "market-cap"
    with pytest.raises(ValueError):
        m.prepare_factor_market_cap_source(
            m.FactorMarketCapPrepareRequest(prepared_source=prepared),
            lake_root=root,
            now=lambda: _READ_AT,
        )
    assert not list(root.rglob("*.parquet"))
    assert not list(root.glob(".market-cap-prepare-*"))


def test_another_replica_generation_cannot_pair_with_frozen_prices(tmp_path: Path) -> None:
    m = _module()
    prepared = _prepared(tmp_path)
    _sidecar(prepared.receipt.request.replica_path)
    with pytest.raises(ValueError, match="generation"):
        m.prepare_factor_market_cap_source(
            m.FactorMarketCapPrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "market-cap",
        )


def test_generation_changed_during_export_has_no_success_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    from rquant.replica_generation import replica_generation_path

    prepared = _prepared(tmp_path)
    root = tmp_path / "market-cap"
    materialize = m.materialize_table_dependency

    def change(*args: object, **kwargs: object) -> object:
        artifact = materialize(*args, **kwargs)
        sidecar = replica_generation_path(prepared.receipt.request.replica_path)
        sidecar.write_bytes(sidecar.read_bytes() + b"\n")
        return artifact

    monkeypatch.setattr(m, "materialize_table_dependency", change)
    with pytest.raises(ValueError, match="generation"):
        m.prepare_factor_market_cap_source(
            m.FactorMarketCapPrepareRequest(prepared_source=prepared),
            lake_root=root,
            now=lambda: _READ_AT,
        )
    assert list(root.rglob("*.parquet")), "finished raw artifact is retained as diagnostic"
    assert not list(root.glob(".market-cap-prepare-*"))
    assert not list(root.rglob("*.tmp-*"))


def test_prepare_has_one_ro_transaction_and_never_touches_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    from tests.unit.test_factor_source_prepare import _ConnectionProbe

    prepared = _prepared(tmp_path)
    original_stat, original_lstat, original_resolve = Path.stat, Path.lstat, Path.resolve

    def guard(method: object) -> object:
        def checked(path: Path, *args: object, **kwargs: object) -> object:
            assert path != _PRIMARY, "market cap touched the primary"
            return method(path, *args, **kwargs)

        return checked

    monkeypatch.setattr(Path, "stat", guard(original_stat))
    monkeypatch.setattr(Path, "lstat", guard(original_lstat))
    monkeypatch.setattr(Path, "resolve", guard(original_resolve))
    connect = m.connect_pinned_readonly
    opened = []

    def track(path: Path, descriptor: int) -> tuple[object, str]:
        assert path == prepared.receipt.request.replica_path
        connection, mode = connect(path, descriptor)
        probe = _ConnectionProbe(connection)
        opened.append((probe, descriptor))
        return probe, mode

    monkeypatch.setattr(m, "connect_pinned_readonly", track)
    root = tmp_path / "market-cap"
    m.prepare_factor_market_cap_source(
        m.FactorMarketCapPrepareRequest(prepared_source=prepared),
        lake_root=root,
        now=lambda: _READ_AT,
    )
    assert len(opened) == 1
    probe, descriptor = opened[0]
    assert probe.commands.count("BEGIN TRANSACTION") == probe.commands.count("COMMIT") == 1
    first, last = probe.commands.index("BEGIN TRANSACTION"), probe.commands.index("COMMIT")
    assert sum(command.lstrip().startswith("COPY") for command in probe.commands[first:last]) == 1
    assert probe.closed
    with pytest.raises(OSError):
        os.fstat(descriptor)
    assert not list(root.glob(".market-cap-prepare-*"))


@pytest.mark.parametrize("cancel", [False, True])
def test_export_failure_or_cancel_rolls_back_closes_and_has_no_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    m = _module()
    from tests.unit.test_factor_source_prepare import _ConnectionProbe

    prepared = _prepared(tmp_path)
    connect = m.connect_pinned_readonly
    opened = []

    def track(path: Path, descriptor: int) -> tuple[object, str]:
        connection, mode = connect(path, descriptor)
        probe = _ConnectionProbe(connection)
        opened.append((probe, descriptor))
        return probe, mode

    def fail(*args: object, **kwargs: object) -> object:
        raise KeyboardInterrupt() if cancel else RuntimeError("export failed")

    monkeypatch.setattr(m, "connect_pinned_readonly", track)
    monkeypatch.setattr(m, "materialize_table_dependency", fail)
    root = tmp_path / "market-cap"
    with pytest.raises(KeyboardInterrupt if cancel else RuntimeError):
        m.prepare_factor_market_cap_source(
            m.FactorMarketCapPrepareRequest(prepared_source=prepared), lake_root=root
        )
    probe, descriptor = opened[0]
    assert probe.closed and "ROLLBACK" in probe.commands and "COMMIT" not in probe.commands
    with pytest.raises(OSError):
        os.fstat(descriptor)
    assert not list(root.glob(".market-cap-prepare-*"))


@pytest.mark.parametrize("damage", ["receipt", "artifact"])
def test_package_or_parquet_damage_is_refused_before_reader(tmp_path: Path, damage: str) -> None:
    m = _module()
    source, _, root = _source(tmp_path)
    if damage == "receipt":
        source = source.model_copy(update={"sha256": "f" * 64})
    else:
        (root / source.artifact.relative_path).write_bytes(b"broken parquet")
    with pytest.raises(ValueError), m.open_factor_market_cap_source(source, lake_root=root):
        pytest.fail("damaged source was admitted")
    assert not list(root.glob(".market-cap-reader-*"))


@pytest.mark.parametrize("scope_fault", ["code_limit", "date_limit"])
def test_prepared_scope_limits_are_revalidated(tmp_path: Path, scope_fault: str) -> None:
    m = _module()
    from rquant.research_snapshot import FactorComputationScope

    prepared = _prepared(tmp_path)
    scope = prepared.receipt.request.scope
    bad_scope = scope.model_copy(
        update={"stock_codes": tuple(f"{i:06d}.SZ" for i in range(1, 7002))}
        if scope_fault == "code_limit"
        else {
            "end_date": scope.start_date + timedelta(days=4096),
            "as_of_time": _AS_OF + timedelta(days=5000),
        }
    )
    with pytest.raises(ValueError):
        FactorComputationScope.model_validate(bad_scope)
    broken = prepared.model_copy(
        update={
            "receipt": prepared.receipt.model_copy(
                update={"request": prepared.receipt.request.model_copy(update={"scope": bad_scope})}
            )
        }
    )
    with pytest.raises(ValueError):
        m.FactorMarketCapPrepareRequest(prepared_source=broken)


@pytest.mark.parametrize(
    "fault", ["too_many_codes", "outside_code", "outside_date", "another_source", "duplicate_codes"]
)
def test_daily_query_must_fit_one_day_500_codes_and_bound_source(
    tmp_path: Path, fault: str
) -> None:
    m = _module()
    source, _, root = _source(tmp_path)
    update = {
        "too_many_codes": {"stock_codes": tuple(f"{i:06d}.SZ" for i in range(1, 502))},
        "outside_code": {"stock_codes": ("600001.SH",)},
        "outside_date": {"trade_date": _FIRST - timedelta(days=1)},
        "another_source": {"source_sha256": "f" * 64},
        "duplicate_codes": {"stock_codes": (_CODES[0], _CODES[0])},
    }[fault]
    with (
        m.open_factor_market_cap_source(source, lake_root=root) as lease,
        pytest.raises(ValueError),
    ):
        lease.query(_query(m, source).model_copy(update=update))


def test_scope_larger_than_one_query_is_retained_and_read_in_daily_chunks(tmp_path: Path) -> None:
    m = _module()
    source, _, root = _source(tmp_path, count=501)
    assert len(source.scope.stock_codes) == 501
    with m.open_factor_market_cap_source(source, lake_root=root) as lease:
        batches = [
            lease.query(_query(m, source, codes=source.scope.stock_codes[start : start + 500]))
            for start in (0, 500)
        ]
        assert sum(len(batch.facts) for batch in batches) == 501
        assert all(batch.counts.valid == len(batch.facts) for batch in batches)


@pytest.mark.parametrize("raise_in_body", [False, True])
def test_private_reader_never_reopens_ro_and_closes_after_context_or_error(
    tmp_path: Path, raise_in_body: bool
) -> None:
    m = _module()
    source, prepared, root = _source(tmp_path)
    lease = None
    try:
        with m.open_factor_market_cap_source(source, lake_root=root) as lease:
            first = lease.query(_query(m, source))
            (root / source.artifact.relative_path).write_bytes(b"raw lake changed")
            prepared.receipt.request.replica_path.unlink()
            second = lease.query(_query(m, source))
            assert first.model_dump_json() == second.model_dump_json()
            if raise_in_body:
                raise RuntimeError("consumer stopped")
    except RuntimeError as exc:
        assert raise_in_body and str(exc) == "consumer stopped"
    assert lease is not None
    with pytest.raises(RuntimeError, match="closed"):
        lease.query(_query(m, source))
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        lease._connection.execute("SELECT 1")
    assert not lease._private_root.exists()
    assert not list(root.glob(".market-cap-reader-*"))
