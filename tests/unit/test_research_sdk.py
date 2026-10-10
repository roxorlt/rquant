"""Notebook clients use the same bounded readers and factor library as production."""

from __future__ import annotations

import importlib
import importlib.util
import os
import subprocess
import sys
import threading
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from tests.unit.test_factor_daily_feature_source import _source as _stored_source
from tests.unit.test_factor_result_artifact import _CODE_REVISION, _research
from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST

_ROOT = Path(__file__).resolve().parents[2]
_EXAMPLE = _ROOT / "docs/examples/research_sdk.py"
_MARKET = ("market_above_ma20_ratio_pct", "market_high_60d_ratio_pct")


def _sdk() -> object:
    name = "rquant.research_sdk"
    assert importlib.util.find_spec(name) is not None, "shared research SDK is missing"
    return importlib.import_module(name)


def _market_days(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        "CREATE TABLE market_sentiment_daily(trade_date DATE PRIMARY KEY,"
        "high_60d_ratio_pct DOUBLE,above_ma20_ratio_pct DOUBLE)"
    )
    connection.execute(
        "INSERT INTO market_sentiment_daily VALUES (?,0.,100.),(?,NULL,?)",
        [_FIRST, _FIRST + timedelta(days=1), float("nan")],
    )


def _demo_files(tmp_path: Path) -> tuple[object, Path, Path, object, object]:
    from rquant.factor.daily_feature_source import FactorMarketTemperaturePrepareRequest
    from rquant.factor.display_artifact import publish_factor_display_artifact
    from rquant.factor.market_temperature_source import prepare_factor_market_temperature_source
    from rquant.factor.result_artifact import (
        load_factor_research_artifact,
        publish_factor_research_artifact,
    )

    inventory, prepared, lake_root = _stored_source(tmp_path, days=3, mutate=_market_days)
    source = prepare_factor_market_temperature_source(
        FactorMarketTemperaturePrepareRequest(
            prepared_source=prepared, base_daily_source=inventory
        ),
        lake_root=lake_root,
        now=lambda: _AS_OF + timedelta(minutes=10),
    )
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir(mode=0o700)
    research = publish_factor_research_artifact(_research(), _CODE_REVISION, artifact_root)
    full = load_factor_research_artifact(artifact_root, research.sha256)
    display = publish_factor_display_artifact(full, artifact_root)
    return source, lake_root, artifact_root, research, display


def _resources() -> tuple[int, int]:
    return len(os.listdir("/dev/fd")), len(threading.enumerate())


def _files(root: Path) -> tuple[str, ...]:
    return tuple(sorted(str(path.relative_to(root)) for path in root.rglob("*")))


def test_notebook_entry_reads_sealed_values_and_existing_research(tmp_path: Path) -> None:
    sdk = _sdk()
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    source, lake_root, artifact_root, research, display = _demo_files(tmp_path)
    source_file = tmp_path / "source.json"
    source_file.write_text(source.model_dump_json(), encoding="utf-8")
    files_before, resources_before = _files(lake_root), _resources()
    fields = tuple(field.column for field in source.fields)
    units = {field.column: field.unit for field in source.fields}
    assert units["turnover_rate"] == units[_MARKET[0]] == units[_MARKET[1]] == "percent"
    assert units["total_mv"] == "CNY_10000"
    with sdk.open_factor_daily_feature_source(source, lake_root=lake_root) as reader:
        with open_factor_daily_feature_source(source, lake_root=lake_root) as original:
            for offset in (2, 1, 0):
                query = sdk.FactorDailyFeatureQuery(
                    source_sha256=source.sha256,
                    trade_date=_FIRST + timedelta(days=offset),
                    stock_codes=source.scope.stock_codes,
                    fields=fields,
                )
                actual = reader.query(query)
                assert actual == original.query(query)
                by_key = {(fact.stock_code, fact.column): fact for fact in actual.facts}
                assert by_key["000007.SZ", "ma5"].value == 0.0
                assert by_key["000008.SZ", "ma5"].status == "missing"
                assert by_key["000002.SZ", "ma5"].status == "null"
                assert by_key["000003.SZ", "ma5"].non_finite_value == "NaN"
                if offset == 2:
                    assert by_key["000001.SZ", _MARKET[0]].reason == ("missing_market_temperature")
                elif offset == 1:
                    assert by_key["000001.SZ", _MARKET[0]].reason == (
                        "market_temperature_non_finite"
                    )
                    assert by_key["000001.SZ", _MARKET[1]].reason == "market_temperature_null"
                else:
                    for code in source.scope.stock_codes:
                        assert by_key[code, _MARKET[0]].value == 100.0
                        assert by_key[code, _MARKET[1]].value == 0.0
        assert original.closed and not original._private_root.exists()
        full = sdk.load_factor_research_artifact(artifact_root, research.sha256)
        result = sdk.assemble_factor_research_result(full.research.request)
        assert result == full.research.result
        projection = sdk.load_factor_display_artifact(artifact_root, display.sha256)
        assert projection.full_artifact_sha256 == full.content_sha256
    assert reader.closed and not reader._private_root.exists()
    assert _files(lake_root) == files_before
    assert _resources() == resources_before
    with duckdb.connect(":memory:") as check:
        path = lake_root / source.market_temperature.artifact.relative_path
        assert check.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone() == (2,)
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            "import runpy,sys; sys.path.insert(0,sys.argv[1]); sys.argv=sys.argv[2:]; "
            "runpy.run_path(sys.argv[0], run_name='__main__')",
            str(_ROOT / "src"),
            str(_EXAMPLE),
            "--source",
            str(source_file),
            "--lake-root",
            str(lake_root),
            "--date",
            (_FIRST + timedelta(days=2)).isoformat(),
            "--codes",
            "000001.SZ",
            "000007.SZ",
            "--fields",
            "ma5",
            *_MARKET,
            "--artifact-root",
            str(artifact_root),
            "--research-sha256",
            research.sha256,
            "--display-sha256",
            display.sha256,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "missing_market_temperature" in completed.stdout
    assert result.sha256 in completed.stdout
    assert _files(lake_root) == files_before


def test_invalid_feature_requests_and_corrupt_artifacts_leave_no_reader_state(
    tmp_path: Path,
) -> None:
    sdk = _sdk()
    source, lake_root, artifact_root, research, _display = _demo_files(tmp_path)
    files_before, resources_before = _files(lake_root), _resources()
    valid = dict(
        source_sha256=source.sha256,
        trade_date=_FIRST,
        stock_codes=("000001.SZ",),
        fields=("ma5",),
    )
    invalid = (
        dict(source_sha256="f" * 64),
        dict(trade_date=source.scope.end_date + timedelta(days=1)),
        dict(trade_date=source.scope.start_date - timedelta(days=1)),
        dict(stock_codes=("999999.SZ",)),
        dict(fields=("signal_minute_amount",)),
        dict(fields=("unknown_feature",)),
        dict(stock_codes=tuple(f"{index:06d}.SZ" for index in range(501))),
        dict(fields=("ma5", "ma5")),
        dict(stock_codes=("000001.SZ", "000001.SZ")),
        dict(unexpected=True),
    )
    for changes in invalid:
        with (
            pytest.raises((ValidationError, ValueError)),
            sdk.open_factor_daily_feature_source(source, lake_root=lake_root) as reader,
        ):
            reader.query(sdk.FactorDailyFeatureQuery(**(valid | changes)))
        assert reader.closed and not reader._private_root.exists()
        assert _files(lake_root) == files_before
    target = artifact_root / research.filename
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(ValueError):
        sdk.load_factor_research_artifact(artifact_root, research.sha256)
    assert _resources() == resources_before


def test_sdk_import_has_no_settings_database_or_background_service(tmp_path: Path) -> None:
    _sdk()
    code = """
import builtins
import sys
import threading
import duckdb
sys.path.insert(0, sys.argv[1])
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name in ('rquant.config', 'dotenv'):
        raise AssertionError('SDK requested application configuration')
    return original_import(name, *args, **kwargs)
def forbidden_connect(*args, **kwargs):
    raise AssertionError('SDK opened a database while importing')
builtins.__import__ = guarded_import
duckdb.connect = forbidden_connect
before = len(threading.enumerate())
import rquant.research_sdk as sdk
assert sdk.__all__
assert len(threading.enumerate()) == before
print('explicit-source-only import')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-B", "-c", code, str(_ROOT / "src")],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert list(tmp_path.iterdir()) == []
