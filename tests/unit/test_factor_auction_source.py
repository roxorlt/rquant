"""The original board auction facts remain paired, bounded, and retrospective."""

from __future__ import annotations

import importlib
import importlib.util
import os
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from tests.unit.test_factor_market_temperature_source import _configured as _temperature_configured
from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST, _replica, _request, _sidecar

_COLUMNS = ("board_auction_amount_ratio", "board_gap_up_ratio", "board_member_count")


def _module() -> object:
    name = "rquant.factor.auction_source"
    assert importlib.util.find_spec(name) is not None, "paired board auction producer is missing"
    return importlib.import_module(name)


def _prepared(tmp_path: Path, *, count: int = 12, days: int = 8, mutate: object = None) -> tuple:
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore

    path = _replica(tmp_path, count=count, days=days)
    with duckdb.connect(str(path)) as raw:
        raw.execute(
            "CREATE TABLE kpl_concept_member_daily(trade_date DATE,board_code VARCHAR,"
            "board_name VARCHAR,con_code VARCHAR,PRIMARY KEY(trade_date,board_code,con_code));"
            "CREATE TABLE auction_bar(ts_code VARCHAR,trade_date DATE,auction_type VARCHAR,"
            "price DOUBLE,amount DOUBLE,source VARCHAR,"
            "PRIMARY KEY(ts_code,trade_date,auction_type,source))"
        )
        for board, members in (("000100.KP", (1, 2, 99)), ("000200.KP", (1, 3))):
            raw.executemany(
                "INSERT INTO kpl_concept_member_daily VALUES(?,?,?,?)",
                [
                    (_FIRST - timedelta(days=1), board, f"题材{board}", f"{i:06d}.SZ")
                    for i in members
                ],
            )
        # The extra member lies outside the requested computation universe.
        raw.execute(
            "INSERT INTO daily_bar SELECT lpad(cast(i AS VARCHAR),6,'0')||'.SZ',"
            "CAST(? AS DATE),10.,12.,9.,10.,100.,1000. FROM unnest([1,2,3,99]) ids(i)",
            [_FIRST - timedelta(days=1)],
        )
        raw.execute(
            "INSERT INTO daily_bar SELECT '000099.SZ',CAST(? AS DATE)+d::INTEGER,"
            "10.,12.,9.,10.+d,100.,1000. FROM range(?) days(d)",
            [_FIRST, days],
        )
        auction_rows = []
        for offset in range(-2, days):
            day = _FIRST + timedelta(days=offset)
            amounts = {
                1: 1000.0,
                2: 500.0 if offset == -2 else 1000.0,
                99: 500.0 if offset == -2 else 1000.0,
                3: 3000.0,
            }
            if offset >= 0:
                amounts = {1: 3000.0 + offset * 100.0, 2: 2000.0, 99: 1000.0, 3: 1000.0}
            auction_rows.extend(
                (
                    f"{i:06d}.SZ",
                    day,
                    "open_realtime",
                    (9.0 if i == 99 else 11.0) + max(0, offset),
                    amount,
                    "tushare",
                )
                for i, amount in amounts.items()
            )
        raw.executemany("INSERT INTO auction_bar VALUES(?,?,?,?,?,?)", auction_rows)
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
    source = module.prepare_factor_auction_source(
        module.FactorAuctionPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    return source, prepared, path


def _batch(
    source: object, lease: object, day: object = _FIRST, codes: tuple | None = None
) -> object:
    from rquant.factor.daily_feature_source import FactorDailyFeatureQuery

    return lease.query(
        FactorDailyFeatureQuery(
            source_sha256=source.sha256,
            trade_date=day,
            stock_codes=source.scope.stock_codes if codes is None else codes,
            fields=_COLUMNS,
        )
    )


def test_auction_capabilities_require_verified_source_and_original_units() -> None:
    _module()
    from rquant.factor.capability import historical_daily_capabilities

    assert not set(_COLUMNS) & set(historical_daily_capabilities().feature_catalog().columns)
    with pytest.raises(ValueError):
        historical_daily_capabilities(auction_available=True)
    capabilities = historical_daily_capabilities(
        daily_features_available=True, auction_available=True
    )
    fields = {f.column: f for f in capabilities.fields}
    assert capabilities.version == "daily_auction_v1"
    assert [fields[c].name_zh for c in _COLUMNS] == [
        "题材竞价金额比",
        "题材竞价高开占比",
        "题材成员数",
    ]
    assert [fields[c].unit for c in _COLUMNS] == ["ratio", "ratio", "observations"]
    assert all(fields[c].tracking_supported for c in _COLUMNS)


def test_sealed_source_reuses_full_board_domain_and_raw_units(tmp_path: Path) -> None:
    _module()
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        open_factor_daily_feature_source,
    )

    source, prepared, _ = _source(tmp_path)
    assert source.schema_version == 6
    assert source.value_semantics == "auction_derived"
    assert source.generation == prepared.receipt.generation
    assert source.auction.policy.signal_clock == "09:30:00"
    assert source.auction.policy.history_mode == "retrospective_no_row_first_observed_time"
    assert FactorDailyFeatureSource.model_validate_json(source.model_dump_json()) == source
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        facts = _batch(source, lease, codes=("000001.SZ",)).facts
        assert [(f.column, f.value) for f in facts] == list(
            zip(_COLUMNS, (2.4, 0.6667, 3.0), strict=True)
        )
        assert all(f.auction_diagnostic.board_code == "000100.KP" for f in facts)
        assert all(
            f.auction_diagnostic.membership_date == _FIRST - timedelta(days=1) for f in facts
        )
        assert all(f.auction_diagnostic.historical_observation_days == 2 for f in facts)
        missing = _batch(source, lease, codes=("000012.SZ",)).facts
        assert all(f.value is None and f.reason == "missing_board_membership" for f in missing)
    assert lease.closed and not lease._private_root.exists()


def test_visibility_preserves_source_priority_and_excludes_same_day_stamp(tmp_path: Path) -> None:
    _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    def mutate(raw: object) -> None:
        raw.execute(
            "INSERT INTO kpl_concept_member_daily VALUES(?,'999999.KP','未来题材','000001.SZ')",
            [_FIRST],
        )
        raw.execute(
            (
                "INSERT INTO auction_bar VALUES('000001.SZ',?,'open_realtime',99.,999"
                "999.,'minute_0930_fallback')"
            ),
            [_FIRST],
        )
        raw.execute("DELETE FROM auction_bar WHERE ts_code='000099.SZ' AND trade_date=?", [_FIRST])
        raw.execute(
            (
                "INSERT INTO auction_bar VALUES('000099.SZ',?,'open_realtime',99.,999"
                "999.,'minute_0930_fallback')"
            ),
            [_FIRST],
        )

    source, _, _ = _source(tmp_path, mutate=mutate)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        facts = _batch(source, lease, codes=("000001.SZ",)).facts
        assert [(f.column, f.value) for f in facts] == list(
            zip(_COLUMNS, (2.0, 1.0, 3.0), strict=True)
        )
        assert facts[0].auction_diagnostic.board_code == "000100.KP"
        assert facts[0].auction_diagnostic.auction_sources == ("tushare",)


@pytest.mark.parametrize(
    ("mutation", "column", "reason"),
    [
        (
            "DELETE FROM auction_bar WHERE trade_date=?",
            "board_gap_up_ratio",
            "missing_board_auction",
        ),
        (
            "DELETE FROM daily_bar WHERE trade_date<?",
            "board_gap_up_ratio",
            "missing_previous_close",
        ),
        (
            "DELETE FROM auction_bar WHERE trade_date<?",
            "board_auction_amount_ratio",
            "missing_auction_history",
        ),
        (
            "UPDATE auction_bar SET price=NULL WHERE trade_date=?",
            "board_gap_up_ratio",
            "auction_null",
        ),
        (
            "UPDATE auction_bar SET amount='Infinity'::DOUBLE WHERE trade_date=?",
            "board_auction_amount_ratio",
            "auction_non_finite",
        ),
    ],
)
def test_missing_and_invalid_values_keep_reason(
    tmp_path: Path, mutation: str, column: str, reason: str
) -> None:
    _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, _, _ = _source(tmp_path, mutate=lambda raw: raw.execute(mutation, [_FIRST]))
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        fact = next(
            f for f in _batch(source, lease, codes=("000001.SZ",)).facts if f.column == column
        )
        assert fact.value is None and fact.reason == reason


def test_zero_gap_is_valid_and_tie_uses_original_member_order(tmp_path: Path) -> None:
    _module()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    def mutate(raw: object) -> None:
        raw.execute("UPDATE auction_bar SET price=1. WHERE trade_date=?", [_FIRST])
        raw.execute(
            "UPDATE auction_bar SET amount=6600. WHERE ts_code='000003.SZ' AND trade_date=?",
            [_FIRST],
        )

    source, _, _ = _source(tmp_path, mutate=mutate)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        fact = next(
            f
            for f in _batch(source, lease, codes=("000001.SZ",)).facts
            if f.column == "board_gap_up_ratio"
        )
        assert (fact.status, fact.value, fact.reason) == ("valid", 0.0, None)
        assert fact.auction_diagnostic.board_code == "000100.KP"


def test_generation_change_closes_source_and_publishes_no_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module()
    prepared, path = _prepared(tmp_path)
    opened = []
    original = module.connect_pinned_readonly

    def capture(replica: Path, descriptor: int) -> tuple:
        connection, mode = original(replica, descriptor)
        opened.append((connection, descriptor))
        _sidecar(path)
        return connection, mode

    monkeypatch.setattr(module, "connect_pinned_readonly", capture)
    with pytest.raises(ValueError):
        module.prepare_factor_auction_source(
            module.FactorAuctionPrepareRequest(prepared_source=prepared),
            lake_root=tmp_path / "lake",
        )
    connection, descriptor = opened[0]
    with pytest.raises(duckdb.ConnectionException):
        connection.execute("SELECT 1")
    with pytest.raises(OSError):
        os.fstat(descriptor)
    assert not list((tmp_path / "lake").glob(".auction-prepare-*"))


def test_cli_seals_explicit_auction_source_without_settings(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
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
                "seal-auction",
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
    assert "daily-feature-source-v6" in capsys.readouterr().out


def test_history_takes_twenty_dates_before_dropping_null_and_uses_global_close(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    def mutate(raw: object) -> None:
        raw.execute("DELETE FROM auction_bar WHERE trade_date<?", [_FIRST])
        raw.execute(
            (
                "INSERT INTO auction_bar SELECT '000001.SZ',CAST(? AS DATE)-d::INTEGE"
                "R,'open_realtime',11.,CASE WHEN d=21 THEN 100000. WHEN d=20 THEN 100"
                "0. ELSE NULL END,'tushare' FROM range(1,22) dates(d)"
            ),
            [_FIRST],
        )

    source, _, _ = _source(tmp_path, mutate=mutate)
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        facts = _batch(source, lease, codes=("000001.SZ",)).facts
        assert facts[0].value == 6.0
        assert facts[0].auction_diagnostic.historical_observation_days == 1
        assert facts[0].auction_diagnostic.previous_close_date == _FIRST - timedelta(days=1)


def _adapter_request(
    source: object,
    prepared: object,
    *,
    expression: str = "ref(board_auction_amount_ratio, 1) + board_gap_up_ratio + close",
    days: tuple | None = None,
) -> object:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.time_series import DecisionTime
    from tests.unit.test_factor_stream_adapter import _at

    definition = build_factor_definition(
        factor_id="board_auction",
        name_zh="题材竞价",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression=expression,
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True, auction_available=True
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
    tmp_path: Path,
    *,
    expression: str = "ref(board_auction_amount_ratio, 1) + board_gap_up_ratio + close",
    mutate: object = None,
) -> tuple:
    from tests.unit import test_factor_market_temperature_source as helpers

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(helpers, "_source", _source)
        patch.setattr(helpers, "_adapter_request", _adapter_request)
        return _temperature_configured(tmp_path, expression=expression, mutate=mutate)


def test_previous_complete_sse_day_and_worker_artifacts_keep_auction_witnesses(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch
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
    assert verified.display.daily_features.auction == source.auction.summary()
    assert all(
        len(day.auction_values) == 10 for day in verified.display.daily_feature_coverage_days
    )
    for day in verified.full.journal.days:
        batch = FactorDailyStreamBatch.model_validate_json(
            (config.artifact_root / day.artifact.filename).read_bytes()
        )
        assert batch.daily_features.panel_date < day.trade_date
        assert all(
            v.auction_diagnostic is not None
            for row in batch.daily_features.rows
            for v in row.values
        )


def test_incremental_tracking_and_cancel_use_original_worker(tmp_path: Path) -> None:
    from tests.unit import test_factor_market_temperature_source as helpers

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(helpers, "_configured", _configured)
        helpers.test_tracking_first_then_continued_equals_whole_and_cancel_preserves_history(
            tmp_path
        )


def test_causal_prefix_ignores_future_tail_and_keeps_board_identity(tmp_path: Path) -> None:
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
                    "UPDATE auction_bar SET amount=999999. WHERE trade_date=?",
                    [_FIRST + timedelta(days=7)],
                )
            elif name == "changed":
                raw.execute(
                    "UPDATE kpl_concept_member_daily SET board_code='000101.KP' WHERE boa"
                    "rd_code='000100.KP'"
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
