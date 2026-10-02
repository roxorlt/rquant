"""History derivation is checked against an independent scalar recurrence."""

from __future__ import annotations

import importlib
import importlib.util
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.factor.source_prepare import prepare_factor_stream_source
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_factor_source_prepare import _FIRST, _replica, _request, _sidecar

_AS_OF = datetime(2026, 10, 1, 8, tzinfo=UTC)

_TECHNICAL = (
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


def _module() -> object:
    name = "rquant.factor.technical_history_source"
    assert importlib.util.find_spec(name) is not None, "technical history preparation is missing"
    return importlib.import_module(name)


def _raw(tmp_path: Path, *, days: int = 90, count: int = 5) -> Path:
    path = _replica(tmp_path, count=count, days=days)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "CREATE TABLE daily_basic(ts_code VARCHAR,trade_date DATE,turnover_rate DOUBLE,"
            "volume_ratio DOUBLE,total_mv DOUBLE,circ_mv DOUBLE,PRIMARY KEY(ts_code,trade_date))"
        )
        raw.execute(
            "INSERT INTO daily_basic SELECT ts_code,trade_date,0.5387,1.23,12345.6789,9876.5432 "
            "FROM daily_bar"
        )
        for offset in range(days):
            day = _FIRST + timedelta(days=offset)
            close = 10 + (offset % 23) * 0.3 + offset / 3
            raw.execute(
                "UPDATE daily_bar SET open=?,high=?,low=?,close=? WHERE trade_date=?",
                [close, close + 0.1, close - 0.1, close, day],
            )
            raw.execute(
                "UPDATE adj_factor SET adj_factor=? WHERE trade_date=?",
                [1.0 if offset < 40 else 2.0, day],
            )
        raw.execute("UPDATE daily_bar SET high=20,low=20,close=20 WHERE ts_code='000002.SZ'")
        raw.execute("UPDATE adj_factor SET adj_factor=1 WHERE ts_code='000002.SZ'")
        raw.execute(
            "DELETE FROM adj_factor WHERE ts_code='000002.SZ' AND trade_date<?",
            [_FIRST + timedelta(days=10)],
        )
        raw.execute(
            "UPDATE daily_bar SET close=NULL WHERE ts_code='000003.SZ' AND trade_date=?",
            [_FIRST + timedelta(days=50)],
        )
        raw.execute("UPDATE adj_factor SET adj_factor=NULL WHERE ts_code='000004.SZ'")
        raw.execute("DELETE FROM daily_bar WHERE ts_code='000005.SZ'")
    _sidecar(path)
    return path


def _prepared(
    tmp_path: Path, path: Path, *, start: int = 0, end: int = 74, count: int = 5
) -> object:
    (tmp_path / "lake").mkdir(mode=0o700, exist_ok=True)
    request = _request(path, count=count, days=1)
    scope = request.scope.model_copy(
        update={
            "start_date": _FIRST + timedelta(days=start),
            "end_date": _FIRST + timedelta(days=end),
            "as_of_time": _AS_OF,
        }
    )
    request = request.model_copy(update={"scope": scope})
    with DuckDBStore(tmp_path / f"metadata-{start}-{end}.duckdb") as metadata:
        return prepare_factor_stream_source(
            request, metadata_store=metadata, lake_root=tmp_path / "lake", now=lambda: _AS_OF
        )


def _oracle(rows: list[tuple]) -> dict:
    close = [row[3] * row[4] for row in rows]
    high = [row[1] * row[4] for row in rows]
    low = [row[2] * row[4] for row in rows]
    fast, slow = close[0], close[0]
    signal = None
    up = {6: 0.0, 14: 0.0}
    down = {6: 0.0, 14: 0.0}
    k, d = 50.0, 50.0
    result = {}
    for index, row in enumerate(rows):
        factor = row[4]
        values = {
            f"ma{n}": sum(close[index - n + 1 : index + 1]) / n / factor if index + 1 >= n else None
            for n in (5, 10, 20, 60)
        }
        change = 0.0 if index == 0 else close[index] - close[index - 1]
        for n in (6, 14):
            up[n] += (max(change, 0.0) - up[n]) / n
            down[n] += (max(-change, 0.0) - down[n]) / n
            values[f"rsi{n}"] = (
                (100.0 if down[n] == 0 else 100.0 - 100.0 / (1 + up[n] / down[n]))
                if index + 1 >= n
                else None
            )
        if index:
            fast += 2 / 13 * (close[index] - fast)
            slow += 2 / 27 * (close[index] - slow)
        dif = fast - slow if index >= 25 else None
        if dif is not None:
            signal = dif if signal is None else signal + 0.2 * (dif - signal)
        dea = signal if index >= 33 else None
        values.update(
            macd=None if dif is None else dif / factor,
            macd_signal=None if dea is None else dea / factor,
            macd_hist=None if dea is None else (dif - dea) / factor,
        )
        lower, upper = (
            min(low[max(0, index - 8) : index + 1]),
            max(high[max(0, index - 8) : index + 1]),
        )
        if upper != lower:
            rsv = (close[index] - lower) / (upper - lower) * 100
            k = 2 / 3 * k + rsv / 3
            d = 2 / 3 * d + k / 3
        values.update(kdj_k=k, kdj_d=d, kdj_j=3 * k - 2 * d)
        result[row[0]] = values
    return result


def test_history_twelve_fields_match_independent_recurrence_and_daily_factor_scale(
    tmp_path: Path,
) -> None:
    module = _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        open_factor_daily_feature_source,
    )

    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    assert source.schema_version == 2 and source.value_semantics == "history_derived"
    assert source.technical_history.codes[0].first_valid_date == _FIRST
    assert source.technical_history.codes[1].leading_invalid_observations == 10
    assert source.technical_history.codes[2].break_date == _FIRST + timedelta(days=50)
    assert source.technical_history.codes[3].first_valid_date is None
    assert source.technical_history.codes[4].input_observations == 0
    with duckdb.connect(str(path), read_only=True) as raw:
        rows = raw.execute(
            "SELECT b.trade_date,b.high,b.low,b.close,a.adj_factor FROM daily_bar b "
            "JOIN adj_factor a USING(ts_code,trade_date) "
            "WHERE ts_code='000001.SZ' AND b.trade_date<=? ORDER BY b.trade_date",
            [_FIRST + timedelta(days=74)],
        ).fetchall()
    expected = _oracle(rows)
    assert max(values["kdj_j"] for values in expected.values()) > 100
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        for day, values in reversed(list(expected.items())):
            batch = lease.query(
                FactorDailyFeatureQuery(
                    source_sha256=source.sha256,
                    trade_date=day,
                    stock_codes=("000001.SZ",),
                    fields=tuple(sorted(_TECHNICAL)),
                )
            )
            for fact in batch.facts:
                value = values[fact.column]
                if value is None:
                    assert fact.value is None and fact.reason == "insufficient_window"
                else:
                    assert fact.status == "valid" and fact.value == pytest.approx(
                        value, rel=1e-12, abs=1e-12
                    )
        broken = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST + timedelta(days=60),
                stock_codes=("000003.SZ", "000004.SZ", "000005.SZ"),
                fields=("ma5",),
            )
        )
        assert [fact.reason for fact in broken.facts] == [
            "history_break",
            "no_initialization",
            "no_initialization",
        ]
        flat = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST + timedelta(days=74),
                stock_codes=("000002.SZ",),
                fields=tuple(sorted(_TECHNICAL)),
            )
        )
        for fact in flat.facts:
            expected_flat = (
                0.0
                if fact.column.startswith("macd")
                else 20.0
                if fact.column.startswith("ma")
                else 100.0
                if fact.column.startswith("rsi")
                else 50.0
            )
            assert fact.status == "valid" and fact.value == pytest.approx(expected_flat)
    assert lease.closed and not lease._private_root.exists()


def test_short_output_scope_uses_original_seed_and_same_suffix(tmp_path: Path) -> None:
    module = _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        open_factor_daily_feature_source,
    )

    path = _raw(tmp_path)
    long = _prepared(tmp_path, path)
    short = _prepared(tmp_path, path, start=65)
    sources = [
        module.prepare_factor_technical_history_source(
            module.FactorTechnicalHistoryPrepareRequest(prepared_source=p),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=5),
        )
        for p in (long, short)
    ]
    values = []
    for source in sources:
        assert source.technical_history.codes[0].first_valid_date == _FIRST
        with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
            values.append(
                tuple(
                    lease.query(
                        FactorDailyFeatureQuery(
                            source_sha256=source.sha256,
                            trade_date=_FIRST + timedelta(days=day),
                            stock_codes=("000001.SZ",),
                            fields=tuple(sorted(_TECHNICAL)),
                        )
                    ).facts
                    for day in range(65, 75)
                )
            )
    assert values[0] == values[1]


def test_sealed_input_preserves_raw_null_nan_and_absent_factor(tmp_path: Path) -> None:
    module = _module()
    path = _raw(tmp_path)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "UPDATE daily_bar SET close='NaN'::DOUBLE WHERE ts_code='000003.SZ' AND trade_date=?",
            [_FIRST + timedelta(days=50)],
        )
    _sidecar(path)
    prepared = _prepared(tmp_path, path)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    (artifact,) = source.technical_history.inputs
    with duckdb.connect() as verify:
        rows = verify.execute(
            "SELECT ts_code,trade_date,close,adj_factor,factor_present FROM read_parquet(?) "
            "WHERE (ts_code='000003.SZ' AND trade_date=?) "
            "OR (ts_code='000002.SZ' AND trade_date=?) "
            "OR (ts_code='000004.SZ' AND trade_date=?) ORDER BY ts_code",
            [
                str(tmp_path / "lake" / artifact.relative_path),
                _FIRST + timedelta(days=50),
                _FIRST,
                _FIRST,
            ],
        ).fetchall()
    assert rows[0][-2:] == (None, False)
    assert math.isnan(rows[1][2])
    assert rows[2][-2:] == (None, True)


def test_verified_derived_source_selects_capability_and_v2_reference(tmp_path: Path) -> None:
    module = _module()
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.run_configuration import save_factor_daily_feature_source

    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    root = tmp_path / "config"
    root.mkdir(mode=0o700)
    reference = save_factor_daily_feature_source(root, source)
    assert reference.kind == "factor-daily-feature-source-v2"
    caps = historical_daily_capabilities(
        daily_features_available=True, technical_history_available=True
    )
    assert caps.version == "daily_derived_v1"
    fields = {f.column: f for f in caps.fields}
    assert (
        fields["ma5"].unit == "session_price" and fields["ma5"].value_semantics == "history_derived"
    )
    assert fields["total_mv"].value_semantics == "stored_not_recomputed"
    assert source.select(("total_mv",)).technical_history is None


@pytest.mark.parametrize(
    "limits",
    [{"max_input_rows": 1}, {"max_code_observations": 1}, {"max_output_cells": 1}],
    ids=["input", "code", "output"],
)
def test_explicit_history_budgets_reject_before_source_publication(
    tmp_path: Path, limits: dict
) -> None:
    module = _module()
    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    with pytest.raises(ValueError, match="budget"):
        module.prepare_factor_technical_history_source(
            module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared, **limits),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=5),
        )
    assert not list((tmp_path / "lake").glob(".technical-history-*"))


def test_prepare_pins_one_ro_transaction_500_code_batches_and_releases_single_code_frames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import weakref

    module = _module()
    path = _raw(tmp_path, days=6, count=501)
    prepared = _prepared(tmp_path, path, end=5, count=501)
    connect, iterate, compute = (
        module.connect_pinned_readonly,
        module._code_rows,
        module.technical.compute_indicators,
    )
    connections, queries, widths, frames = [], [], [], []

    class Observed:
        def __init__(self, connection: object) -> None:
            self.connection, self.closed = connection, False

        def __getattr__(self, name: str) -> object:
            return getattr(self.connection, name)

        def execute(self, sql: str, args: object = None) -> object:
            queries.append((sql, args))
            return (
                self.connection.execute(sql) if args is None else self.connection.execute(sql, args)
            )

        def close(self) -> None:
            self.closed = True
            self.connection.close()

    def pinned(replica: Path, descriptor: int) -> tuple:
        assert replica == path
        connection, mode = connect(replica, descriptor)
        wrapped = Observed(connection)
        connections.append(wrapped)
        return wrapped, mode

    def bounded(connection: object, codes: tuple, end: object) -> object:
        widths.append(len(codes))
        yield from iterate(connection, codes, end)

    def single(frame: object) -> object:
        assert all(ref() is None for ref in frames)
        assert frame.ts_code.nunique() == 1 and len(frame) <= 6
        frames.append(weakref.ref(frame))
        return compute(frame)

    monkeypatch.setattr(module, "connect_pinned_readonly", pinned)
    monkeypatch.setattr(module, "_code_rows", bounded)
    monkeypatch.setattr(module.technical, "compute_indicators", single)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    assert len(connections) == 1 and connections[0].closed
    assert widths == [500, 1]
    copies = [
        args for sql, args in queries if sql.startswith("INSERT INTO technical_history_input")
    ]
    assert [len(args[0]) for args in copies] == [500, 1]
    assert all(sql.count("UPDATE technical_derived") == 0 for sql, _ in queries)
    assert sum(sql == "BEGIN TRANSACTION" for sql, _ in queries) == 1
    assert sum(sql == "COMMIT" for sql, _ in queries) == 1
    assert all(ref() is None for ref in frames)
    assert len(source.technical_history.codes) == 501
    assert all(not sql.startswith(("UPDATE daily_", "INSERT INTO daily_")) for sql, _ in queries)
    assert not list((tmp_path / "lake").glob(".technical-history-*"))


def test_generation_change_after_input_sealing_refuses_a_completed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module()
    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    materialize = module.materialize_table_dependency

    def changed(*args: object, **kwargs: object) -> object:
        artifact = materialize(*args, **kwargs)
        _sidecar(path)
        return artifact

    monkeypatch.setattr(module, "materialize_table_dependency", changed)
    with pytest.raises(ValueError, match="generation"):
        module.prepare_factor_technical_history_source(
            module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF + timedelta(minutes=5),
        )
    assert not list((tmp_path / "lake").glob(".technical-history-*"))


def test_initialization_receipt_must_match_sealed_original_input(tmp_path: Path) -> None:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        open_factor_daily_feature_source,
    )
    from rquant.runtime_contracts import canonical_sha256

    module = _module()
    path = _raw(tmp_path)
    prepared = _prepared(tmp_path, path)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    fields = source.model_dump(exclude={"sha256"})
    fields["technical_history"]["codes"][0]["first_valid_date"] += timedelta(days=1)
    fields["technical_history"]["codes"][0]["leading_invalid_observations"] = 1
    changed = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
    with (
        pytest.raises(ValueError, match="initialization"),
        open_factor_daily_feature_source(changed, lake_root=tmp_path / "lake"),
    ):
        pytest.fail("an invented seed must not reinterpret sealed history")
    assert not list((tmp_path / "lake").glob(".daily-feature-*"))


def test_absent_bar_is_not_synthetic_observation_or_a_restart(tmp_path: Path) -> None:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        open_factor_daily_feature_source,
    )

    module = _module()
    path = _raw(tmp_path)
    absent = _FIRST + timedelta(days=20)
    with duckdb.connect(str(path)) as raw:
        raw.execute("DELETE FROM daily_bar WHERE ts_code='000001.SZ' AND trade_date=?", [absent])
    _sidecar(path)
    prepared = _prepared(tmp_path, path)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    assert (
        source.technical_history.codes[0].input_observations == 74
        and source.technical_history.codes[0].break_date is None
    )
    with duckdb.connect(str(path), read_only=True) as raw:
        rows = raw.execute(
            "SELECT b.trade_date,b.high,b.low,b.close,a.adj_factor FROM daily_bar b "
            "JOIN adj_factor a USING(ts_code,trade_date) "
            "WHERE ts_code='000001.SZ' ORDER BY trade_date"
        ).fetchall()
    expected = _oracle(rows)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        missing = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=absent,
                stock_codes=("000001.SZ",),
                fields=("ma5",),
            )
        ).facts[0]
        assert missing.status == "missing" and missing.reason == "missing_observation"
        resumed = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=absent + timedelta(days=1),
                stock_codes=("000001.SZ",),
                fields=("ma5",),
            )
        ).facts[0]
        assert resumed.value == pytest.approx(expected[absent + timedelta(days=1)]["ma5"])


def test_derived_overflow_stays_non_finite_in_sealed_output(tmp_path: Path) -> None:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureQuery,
        open_factor_daily_feature_source,
    )

    module = _module()
    path = _raw(tmp_path, days=65, count=1)
    with duckdb.connect(str(path)) as raw:
        raw.execute("UPDATE daily_bar SET high=1e308,low=1e308,close=1e308")
        raw.execute("UPDATE adj_factor SET adj_factor=1")
        raw.execute(
            "UPDATE daily_bar SET high=1,low=1,close=1 WHERE trade_date>=?",
            [_FIRST + timedelta(days=60)],
        )
        raw.execute(
            "UPDATE adj_factor SET adj_factor=0.1 WHERE trade_date>=?",
            [_FIRST + timedelta(days=60)],
        )
    _sidecar(path)
    prepared = _prepared(tmp_path, path, end=64, count=1)
    source = module.prepare_factor_technical_history_source(
        module.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    assert source.technical_history.codes[0].break_date is None
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        fact = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=source.sha256,
                trade_date=_FIRST + timedelta(days=60),
                stock_codes=("000001.SZ",),
                fields=("ma60",),
            )
        ).facts[0]
        # 59 observations at 1e308 cannot be represented after division by a 0.1 session factor.
        assert fact.status == "non_finite" and fact.value is None
        assert fact.non_finite_value in ("NaN", "Infinity") and fact.reason == "derived_non_finite"
