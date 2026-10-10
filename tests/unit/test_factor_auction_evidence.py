"""Named original files and unchanged bounded task contracts for auction facts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from tests.unit.test_factor_auction_source import _batch, _module, _prepared
from tests.unit.test_factor_source_prepare import _AS_OF


def _lake(tmp_path: Path, replica: Path) -> object:
    from rquant.factor.auction_source import FactorAuctionFileEvidence, FactorAuctionLakeInput
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_ingest import ResearchDailyIngestResult, ResearchDatasetIngestAudit
    from rquant.research_lake import ResearchExportSummary, export_research_dataset
    from rquant.research_migration import ResearchAuthorityCandidate
    from rquant.research_snapshot import SnapshotArtifactResolver
    from rquant.runtime_contracts import canonical_sha256

    original = tmp_path / "originals"
    original.mkdir(mode=0o700)
    lake_root = original / "lake"
    catalog_path = original / "catalog.duckdb"
    catalog = ResearchCatalog(catalog_path)
    with duckdb.connect(":memory:") as raw:
        raw.execute(
            "CREATE TABLE auction_bar(ts_code VARCHAR,trade_date DATE,auction_typ"
            "e VARCHAR,price DOUBLE,vol DOUBLE,amount DOUBLE,turnover_rate DOUBLE"
            ",volume_ratio DOUBLE,source VARCHAR,created_at TIMESTAMP,PRIMARY KEY"
            "(ts_code,trade_date,auction_type,source))"
        )
        with duckdb.connect(str(replica), read_only=True) as source:
            rows = source.execute(
                "SELECT ts_code,trade_date,auction_type,price,1.,amount,0.,0.,source,"
                "TIMESTAMP '2026-07-10 00:00:00' FROM auction_bar"
            ).fetchall()
        raw.executemany("INSERT INTO auction_bar VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
        start, end = raw.execute(
            "SELECT min(trade_date),max(trade_date) FROM auction_bar"
        ).fetchone()
        raw.execute(
            "CREATE TABLE trade_calendar(exchange VARCHAR,cal_date DATE,is_open B"
            "OOLEAN,PRIMARY KEY(exchange,cal_date))"
        )
        raw.execute(
            (
                "INSERT INTO trade_calendar SELECT 'SSE',CAST(day AS DATE),TRUE FROM "
                "generate_series(CAST(? AS DATE),CAST(? AS DATE),INTERVAL 1 DAY) date"
                "s(day)"
            ),
            [start, end],
        )
        exported = export_research_dataset(
            raw,
            catalog=catalog,
            lake_root=lake_root,
            dataset="auction_bar",
            start_date=start,
            end_date=end,
            code_commit="a" * 40,
            now=lambda: _AS_OF,
        )
    artifacts = SnapshotArtifactResolver(
        catalog=catalog, lake_root=lake_root
    ).resolve_lake_partitions(
        dataset="auction_bar", start_date=start, end_date=end, as_of_time=_AS_OF
    )
    digest = hashlib.sha256(catalog_path.read_bytes()).hexdigest()
    candidate = ResearchAuthorityCandidate(
        snapshot_id="research-20260710T000000Z-12345678",
        code_commit="a" * 40,
        published_at=_AS_OF,
        bundle_manifest_sha256="b" * 64,
        source_snapshot_sha256="c" * 64,
        catalog_sha256="d" * 64,
        partition_count=10,
        row_count=len(rows),
        auxiliary_table_count=1,
        artifact_file_count=10,
    )
    empty = ResearchExportSummary(
        dataset="minute_bar",
        start_date=start,
        end_date=end,
        status="completed",
        partition_count=0,
        row_count=0,
        exported_count=0,
        unchanged_count=0,
        replaced_count=0,
        partitions=(),
    )
    current = ResearchDailyIngestResult(
        status="degraded",
        observation_id="synthetic-observation",
        bootstrap_snapshot_id=candidate.snapshot_id,
        trade_date=end,
        generated_at=_AS_OF,
        code_commit="a" * 40,
        catalog_sha256=digest,
        readonly_catalog_sha256=digest,
        stable_trading_days=0,
        minute=ResearchDatasetIngestAudit(
            dataset="minute_bar",
            export=empty,
            expected_code_count=0,
            observed_code_count=0,
            complete_code_count=0,
        ),
        auction=ResearchDatasetIngestAudit(
            dataset="auction_bar",
            export=exported,
            expected_code_count=4,
            observed_code_count=4,
            complete_code_count=4,
        ),
        issues=("auction_observed_precision_below_98pct",),
    )
    current_path, candidate_path = original / "current.json", original / "candidate.json"
    current_path.write_text(current.model_dump_json() + "\n")
    candidate_path.write_text(candidate.model_dump_json() + "\n")

    def evidence(path: Path) -> FactorAuctionFileEvidence:
        return FactorAuctionFileEvidence(
            path=path,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            byte_count=path.stat().st_size,
        )

    return FactorAuctionLakeInput(
        lake_root=lake_root,
        catalog_file=evidence(catalog_path),
        current_marker=evidence(current_path),
        candidate_marker=evidence(candidate_path),
        artifacts=artifacts,
        manifest_sha256=canonical_sha256(artifacts),
    )


def test_named_lake_keeps_degraded_original_identity_and_distinct_base(tmp_path: Path) -> None:
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source

    module = _module()
    prepared, replica = _prepared(tmp_path)
    lake = _lake(tmp_path, replica)
    # The replica has no auction history; the named lake is the actual independent input.
    with duckdb.connect(str(replica)) as raw:
        raw.execute("DELETE FROM auction_bar")
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_factor_source_prepare import _request, _sidecar

    _sidecar(replica)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            _request(replica, count=12, days=8),
            metadata_store=metadata,
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF,
        )
    source = module.prepare_factor_auction_source(
        module.FactorAuctionPrepareRequest(prepared_source=prepared, lake_input=lake),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF,
    )
    assert source.auction.lake.catalog_status == "degraded"
    assert source.auction.lake.catalog_issues == ("auction_observed_precision_below_98pct",)
    assert source.auction.lake.catalog_file.identity.inode == lake.catalog_file.path.stat().st_ino
    assert source.auction.lake.candidate_marker.sha256 == lake.candidate_marker.sha256
    assert (
        source.auction.lake.catalog_sha256
        != json.loads(lake.candidate_marker.path.read_text())["catalog_sha256"]
    )
    assert (
        source.auction.lake.verification_scope
        == "named_partitions_current_observation_not_full_authority_chain_or_pit"
    )
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        assert _batch(source, lease, codes=("000001.SZ",)).facts[0].value == 2.4


@pytest.mark.parametrize("changed", ("catalog", "marker", "lineage", "partition"))
def test_changed_original_file_or_marker_binding_is_rejected(tmp_path: Path, changed: str) -> None:
    module = _module()
    prepared, replica = _prepared(tmp_path)
    lake = _lake(tmp_path, replica)
    if changed in ("marker", "lineage"):
        current = json.loads(lake.current_marker.path.read_text())
        current["readonly_catalog_sha256" if changed == "marker" else "bootstrap_snapshot_id"] = (
            "f" * 64
        )
        lake.current_marker.path.write_text(json.dumps(current))
        evidence = lake.current_marker.model_dump()
        evidence.update(
            sha256=hashlib.sha256(lake.current_marker.path.read_bytes()).hexdigest(),
            byte_count=lake.current_marker.path.stat().st_size,
        )
        fields = lake.model_dump()
        fields["current_marker"] = module.FactorAuctionFileEvidence(**evidence)
        lake = module.FactorAuctionLakeInput(**fields)
    else:
        path = (
            lake.catalog_file.path
            if changed == "catalog"
            else lake.lake_root / lake.artifacts[0].relative_path
        )
        with path.open("ab") as handle:
            handle.write(b"changed")
    with pytest.raises((ValueError, OSError)):
        module.prepare_factor_auction_source(
            module.FactorAuctionPrepareRequest(prepared_source=prepared, lake_input=lake),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF,
        )
    assert not list((tmp_path / "lake").glob(".auction-prepare-*"))


@pytest.fixture(scope="module")
def descriptor_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_minute_feature_descriptor import descriptor_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


@pytest.fixture(scope="module")
def context_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_minute_feature_descriptor import context_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


def _auction_shape(base: object) -> object:
    from rquant.data_metadata import DatasetSnapshotArtifact
    from rquant.factor.auction_source import FactorAuctionPolicy, FactorAuctionReceipt
    from rquant.factor.daily_feature_source import AUCTION_FIELDS, FactorDailyFeatureSource
    from rquant.runtime_contracts import canonical_sha256

    artifact = base.tables[0].artifact.model_dump()
    inputs = []
    for name, pk in (
        ("auction_membership_input", ("trade_date", "board_code", "con_code")),
        ("auction_close_input", ("ts_code", "trade_date")),
        ("auction_session_input", ("trade_date",)),
        ("auction_bar", ("ts_code", "trade_date", "auction_type", "source")),
    ):
        value = dict(
            artifact,
            dataset_id="factor_auction_input",
            table_name=name,
            primary_key=pk,
            row_count=1,
            relative_path=f"tables/{name}/versions/{artifact['file_hash']}.parquet",
        )
        inputs.append(DatasetSnapshotArtifact(**value))
    rows = len(base.scope.stock_codes) * len(base.calendar_open_days)
    output = DatasetSnapshotArtifact(
        **dict(
            artifact,
            dataset_id="factor_auction_features",
            table_name="daily_auction_feature",
            primary_key=("ts_code", "trade_date"),
            row_count=rows,
            relative_path=f"tables/daily_auction_feature/versions/{artifact['file_hash']}.parquet",
        )
    )
    receipt = FactorAuctionReceipt(
        policy=FactorAuctionPolicy(implementation_sha256="a" * 64),
        inputs=tuple(inputs),
        artifact=output,
        input_rows=4,
        output_rows=rows,
        board_evaluations=1,
        max_input_rows=16_000_000,
        max_output_cells=128_000_000,
    )
    fields = base.model_dump(exclude={"sha256"})
    fields.update(
        schema_version=6,
        base_daily_source=base,
        auction=receipt,
        observed_at=base.completed_read_at,
        completed_read_at=base.completed_read_at,
        fields=tuple(sorted(base.fields + AUCTION_FIELDS, key=lambda f: f.column)),
        value_semantics="auction_derived",
    )
    return FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))


@pytest.mark.parametrize("count,days", ((7000, 234), (1, 4096)))
def test_maximum_stock_plus_market_spec_decoder_and_ledger(
    descriptor_template: tuple, context_template: tuple, tmp_path: Path, count: int, days: int
) -> None:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.job_spec import _definition_sha256
    from rquant.factor.stream_job_spec import FactorStreamJobSpec, decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_market_temperature_source import _temperature_shape
    from tests.unit.test_factor_minute_feature_descriptor import _minute_shape, _minute_spec
    from tests.unit.test_factor_stock_feature_descriptor import _shape_context, _shape_source

    template, original, *_ = descriptor_template
    minute = _minute_shape(_shape_source(template, count, days=days, longest=True))
    market = _temperature_shape(minute)
    source = _auction_shape(market)
    context = _shape_context(source, *context_template)
    spec = _minute_spec(original, minute, context)
    from rquant.factor.daily_feature_source import MARKET_TEMPERATURE_COLUMNS

    columns = (
        tuple(f.column for f in source.fields if f.column.startswith("board_"))
        + tuple(f.column for f in market.fields if f.column not in MARKET_TEMPERATURE_COLUMNS)[:47]
        + MARKET_TEMPERATURE_COLUMNS
    )
    assert len(columns) == 52

    def balanced(items: tuple[str, ...]) -> str:
        if len(items) == 1:
            return items[0]
        mid = len(items) // 2
        return f"({balanced(items[:mid])}+{balanced(items[mid:])})"

    fields = spec.adapter_request.formula.definition.model_dump(
        exclude={"max_history_window", "dependency_columns"}
    )
    fields.update(
        expression=balanced(columns),
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True,
            technical_history_available=True,
            stock_features_available=True,
            stock_base_daily_available=True,
            minute_features_available=True,
            minute_base_daily_available=True,
            market_temperature_available=True,
            market_temperature_base_daily_available=True,
            auction_available=True,
            auction_base_daily_available=True,
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
    assert decode_factor_job_spec_json(data.decode()) == shape
    assert (
        shape.adapter_request.daily_feature_source.base_daily_source.model_dump_json()
        == market.model_dump_json()
    )
    ledger = FactorEvaluationJobLedger(tmp_path / "capacity.sqlite", clock=lambda: _AS_OF)
    ledger.initialize()
    admitted = ledger.submit("auction-capacity", shape)
    assert ledger.get(admitted.job_id).spec == shape
    print(
        "AUCTION_SPEC_CAPACITY",
        json.dumps(
            dict(
                codes=count,
                range_days=days,
                formula_days=len(shape.adapter_request.formula.trading_days),
                fields=52,
                bytes=len(data),
                decoder="pass",
                ledger_readback="pass",
                context="industry_size",
                capacity_shape_only=True,
            )
        ),
    )
