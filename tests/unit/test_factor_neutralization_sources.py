"""Synthetic paired sources exercise private readers, not real provider data."""

from __future__ import annotations

import importlib
import math
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.factor.industry_source import (
    FactorIndustryPrepareRequest,
    IndustryCaptureRequest,
    collect_industry_sources,
    make_industry_capture,
    prepare_factor_industry_source,
)
from rquant.factor.market_cap_source import (
    FactorMarketCapPrepareRequest,
    prepare_factor_market_cap_source,
)
from rquant.factor.run_configuration import open_factor_run_configuration
from tests.unit import test_factor_run_configuration as fixtures
from tests.unit.test_factor_industry_source import _CAPTURED, _capture, _member, _module, _receipt
from tests.unit.test_factor_source_prepare import _sidecar


def _context_module() -> object:
    return importlib.import_module("rquant.factor.neutralization_context")


def _configured_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple:
    original = fixtures._replica

    def replica(root: Path, **kwargs: object) -> Path:
        path = original(root, **kwargs)
        with duckdb.connect(str(path)) as connection:
            connection.execute(
                "CREATE TABLE daily_basic(ts_code VARCHAR, trade_date DATE, "
                "total_mv DOUBLE, PRIMARY KEY(ts_code, trade_date))"
            )
            rows = connection.execute(
                "SELECT ts_code,trade_date FROM daily_bar ORDER BY ts_code,trade_date"
            ).fetchall()
            connection.executemany(
                "INSERT INTO daily_basic VALUES (?,?,?)",
                [
                    (code, day, math.exp(int(code[:6]) % 4 + 5 * (int(code[:6]) % 3)))
                    for code, day in rows
                ],
            )
            from tests.unit.test_factor_source_prepare import _FIRST

            previous = _FIRST - timedelta(days=1)
            for (day,) in connection.execute(
                "SELECT cal_date FROM trade_calendar ORDER BY cal_date"
            ).fetchall():
                opened = day.weekday() < 5
                connection.execute(
                    "UPDATE trade_calendar SET is_open=?, pretrade_date=? WHERE cal_date=?",
                    [opened, previous, day],
                )
                if opened:
                    previous = day
        _sidecar(path)
        return path

    with monkeypatch.context() as patch:
        patch.setattr(fixtures, "_replica", replica)
        root, reference, request = fixtures._configured(tmp_path)
    with open_factor_run_configuration(root, reference) as loaded:
        prepared, config = loaded.source, loaded.configuration
    config.lake_root.chmod(0o700)
    collection = tmp_path / "capture"

    def capture(query: object) -> object:
        if query.api_name == "index_classify":
            return _capture(_module(), query, industries=31)
        group = int(query.l1_code[:6]) - 801010
        rows = (
            [
                dict(_member(int(code[:6]), "20000101"), l1_code=query.l1_code, is_new=query.is_new)
                for code in prepared.receipt.request.scope.stock_codes
                if (
                    0 if len(prepared.receipt.request.scope.stock_codes) == 3 else int(code[:6]) % 3
                )
                == group
            ]
            if query.is_new == "Y"
            else []
        )
        return make_industry_capture(
            query,
            pd.DataFrame(rows, columns=query.fields),
            requested_at=_CAPTURED - timedelta(seconds=1),
            observed_at=_CAPTURED,
            transport_receipt=_receipt(query),
        )

    collect_industry_sources(IndustryCaptureRequest(root=collection), fetch=capture)
    industry = prepare_factor_industry_source(
        FactorIndustryPrepareRequest(prepared_source=prepared, collection_root=collection),
        lake_root=config.lake_root,
        now=lambda: prepared.receipt.request.scope.as_of_time,
    )
    cap = prepare_factor_market_cap_source(
        FactorMarketCapPrepareRequest(prepared_source=prepared),
        lake_root=config.lake_root,
        now=lambda: prepared.receipt.request.scope.as_of_time,
    )
    return root, reference, request, prepared, config, industry, cap


def test_context_binds_all_source_fields_and_rejects_another_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _context_module()
    _, _, _, prepared, _, industry, cap = _configured_context(tmp_path, monkeypatch)
    context = m.bind_factor_neutralization_context(prepared, industry=industry, market_cap=cap)
    assert context.sources.industry.source_sha256 == industry.sha256
    assert context.sources.market_cap.source_sha256 == cap.sha256
    assert context.sources.prepared_source_sha256 == prepared.sha256
    assert context.sources.industry.source_read_boundary == "captured_api_responses"
    for field in (
        "prepared_source_sha256",
        "snapshot_id",
        "binding_hash",
        "scope_content_hash",
        "code_commit",
    ):
        with pytest.raises(ValueError, match="prepared price source"):
            type(context).model_validate(
                {**context.model_dump(), field: "0" * (40 if field == "code_commit" else 64)}
            )
    with pytest.raises(ValueError, match="another RO generation"):
        type(context).model_validate(
            {
                **context.model_dump(),
                "generation": context.generation.model_copy(update={"sidecar_sha256": "f" * 64}),
            }
        )
    with pytest.raises((ValueError, ValidationError)):
        m.bind_factor_neutralization_context(
            prepared,
            industry=industry.model_copy(update={"prepared_source_sha256": "f" * 64}),
            market_cap=cap,
        )
    bad_generation = cap.generation.model_copy(update={"sidecar_sha256": "f" * 64})
    with pytest.raises((ValueError, ValidationError)):
        m.bind_factor_neutralization_context(
            prepared, market_cap=cap.model_copy(update={"generation": bad_generation})
        )


def test_context_private_queries_preserve_panel_dates_and_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _context_module()
    _, _, _, prepared, config, industry, cap = _configured_context(tmp_path, monkeypatch)
    context = m.bind_factor_neutralization_context(prepared, industry=industry, market_cap=cap)
    day = prepared.receipt.calendar_open_days[6]
    panel = prepared.receipt.calendar_open_days[5]
    from rquant.factor.historical_adapter import _market_time

    with m.open_factor_neutralization_context(context, lake_root=config.lake_root) as lease:
        batch = lease.query(
            trade_date=day,
            panel_date=panel,
            stock_codes=context.scope.stock_codes,
            assumed_visible_at=_market_time(day, 9, 25),
        )
        assert batch.trade_date == day and batch.panel_date == panel
        assert {fact.trade_date for fact in batch.industry_facts} == {panel}
        assert {fact.trade_date for fact in batch.market_cap_facts} == {panel}
        assert len(batch.industry_facts) == len(context.scope.stock_codes)
        assert lease.read_query_count == 2
        assert all(fact.status == "valid" for fact in batch.industry_facts)
    assert lease.closed
    assert not list(config.lake_root.glob(".industry-reader-*"))
    assert not list(config.lake_root.glob(".market-cap-reader-*"))


def test_context_queries_chunk_501_codes_at_original_reader_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.historical_adapter import _market_time
    from rquant.factor.market_cap_source import FactorMarketCapReadLease
    from tests.unit.test_factor_market_cap_source import _prepared

    prepared = _prepared(tmp_path, count=501)
    lake = tmp_path / "prices"
    lake.chmod(0o700)
    cap = prepare_factor_market_cap_source(
        FactorMarketCapPrepareRequest(prepared_source=prepared),
        lake_root=lake,
        now=lambda: prepared.snapshot.as_of_time,
    )
    m = _context_module()
    context = m.bind_factor_neutralization_context(prepared, market_cap=cap)
    queries, original = [], FactorMarketCapReadLease.query

    def query(lease: object, argument: object) -> object:
        queries.append(argument)
        assert len(argument.stock_codes) <= 500
        return original(lease, argument)

    monkeypatch.setattr(FactorMarketCapReadLease, "query", query)
    panel, day = prepared.receipt.calendar_open_days
    with m.open_factor_neutralization_context(context, lake_root=lake) as lease:
        batch = lease.query(
            trade_date=day,
            panel_date=panel,
            stock_codes=context.scope.stock_codes,
            assumed_visible_at=_market_time(day, 9, 25),
        )
        assert len(batch.market_cap_facts) == 501 and all(
            f.status == "valid" for f in batch.market_cap_facts
        )
        assert lease.read_query_count == 2
    assert [len(q.stock_codes) for q in queries] == [500, 1]
    assert lease.closed and not list(lake.glob(".market-cap-reader-*"))


def test_explicit_offline_cli_seals_context_and_attaches_existing_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json
    import subprocess
    import sys

    from rquant.factor.member_archive import _bytes
    from rquant.factor.run_configuration import FactorRunFileReference

    root, reference, _, prepared, config, industry, cap = _configured_context(tmp_path, monkeypatch)
    industry_path, cap_path = root / "industry.json", root / "cap.json"
    industry_path.write_bytes(_bytes(industry))
    cap_path.write_bytes(_bytes(cap))
    industry_path.chmod(0o600)
    cap_path.chmod(0o600)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.run_entry",
            "seal-context",
            "--root",
            str(root),
            "--prepared-source",
            str(root / config.prepared_source.filename),
            "--industry-source",
            str(industry_path),
            "--market-cap-source",
            str(cap_path),
            "--lake-root",
            str(config.lake_root),
            "--reference",
            reference.model_dump_json(),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    receipt = json.loads(result.stdout)
    attached = FactorRunFileReference.model_validate(receipt["configuration_reference"])
    with open_factor_run_configuration(root, attached) as loaded:
        loaded.context.require_prepared(prepared)
        assert (
            loaded.configuration.neutralization_context.model_dump(mode="json")
            == receipt["context_reference"]
        )
    assert not list(config.lake_root.glob(".industry-reader-*")) and not list(
        config.lake_root.glob(".market-cap-reader-*")
    )
