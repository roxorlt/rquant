"""Named minute evidence and the unchanged 2 MiB maximum task boundary."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from tests.unit.test_factor_source_prepare import _AS_OF, _request, _sidecar
from tests.unit.test_factor_volume_profile_source import _batch, _module, _prepared


def _lake(tmp_path: Path, replica: Path) -> object:
    from rquant.factor.named_lake import NamedLakeFileEvidence
    from rquant.factor.volume_profile_source import FactorVolumeProfileLakeInput
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_ingest import ResearchDailyIngestResult, ResearchDatasetIngestAudit
    from rquant.research_lake import ResearchExportSummary, export_research_dataset
    from rquant.research_migration import ResearchAuthorityCandidate
    from rquant.research_snapshot import SnapshotArtifactResolver
    from rquant.runtime_contracts import canonical_sha256

    folder = tmp_path / "originals"
    folder.mkdir(mode=0o700)
    lake_root, catalog_path = folder / "lake", folder / "catalog.duckdb"
    catalog = ResearchCatalog(catalog_path)
    with duckdb.connect(str(replica), read_only=True) as source:
        rows = source.execute("SELECT *,TIMESTAMP '2026-07-10 00:00:00' FROM minute_bar").fetchall()
    with duckdb.connect(":memory:") as raw:
        raw.execute(
            "CREATE TABLE minute_bar(ts_code VARCHAR,trade_time TIMESTAMP,freq VARCHAR,"
            "open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,vol DOUBLE,amount DOUBLE,"
            "source VARCHAR,created_at TIMESTAMP,PRIMARY KEY(ts_code,trade_time,freq,source))"
        )
        raw.executemany("INSERT INTO minute_bar VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
        first, last = raw.execute(
            "SELECT min(cast(trade_time AS DATE)),max(cast(trade_time AS DATE)) FROM minute_bar"
        ).fetchone()
        raw.execute(
            "CREATE TABLE trade_calendar(exchange VARCHAR,cal_date DATE,is_open BOOLEAN,"
            "PRIMARY KEY(exchange,cal_date))"
        )
        raw.execute(
            "INSERT INTO trade_calendar SELECT 'SSE',cast(day AS DATE),TRUE "
            "FROM generate_series(cast(? AS DATE),cast(? AS DATE),INTERVAL 1 DAY) t(day)",
            [first, last],
        )
        exported = export_research_dataset(
            raw,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=first,
            end_date=last,
            code_commit="a" * 40,
            now=lambda: _AS_OF,
        )
    artifacts = SnapshotArtifactResolver(
        catalog=catalog, lake_root=lake_root
    ).resolve_lake_partitions(
        dataset="minute_bar", start_date=first, end_date=last, as_of_time=_AS_OF
    )
    digest = hashlib.sha256(catalog_path.read_bytes()).hexdigest()
    candidate = ResearchAuthorityCandidate(
        snapshot_id="research-20260710T000000Z-12345678",
        code_commit="a" * 40,
        published_at=_AS_OF,
        bundle_manifest_sha256="b" * 64,
        source_snapshot_sha256="c" * 64,
        catalog_sha256="d" * 64,
        partition_count=len(artifacts),
        row_count=len(rows),
        auxiliary_table_count=1,
        artifact_file_count=len(artifacts),
    )
    empty = ResearchExportSummary(
        dataset="auction_bar",
        start_date=first,
        end_date=last,
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
        observation_id="synthetic-minute-observation",
        bootstrap_snapshot_id=candidate.snapshot_id,
        trade_date=last,
        generated_at=_AS_OF,
        code_commit="a" * 40,
        catalog_sha256=digest,
        readonly_catalog_sha256=digest,
        stable_trading_days=0,
        minute=ResearchDatasetIngestAudit(
            dataset="minute_bar",
            export=exported,
            expected_code_count=12,
            observed_code_count=12,
            complete_code_count=0,
        ),
        auction=ResearchDatasetIngestAudit(
            dataset="auction_bar",
            export=empty,
            expected_code_count=0,
            observed_code_count=0,
            complete_code_count=0,
        ),
        issues=("minute_observed_precision_below_98pct",),
    )
    current_path, candidate_path = folder / "current.json", folder / "candidate.json"
    current_path.write_text(current.model_dump_json() + "\n")
    candidate_path.write_text(candidate.model_dump_json() + "\n")

    def evidence(path: Path) -> NamedLakeFileEvidence:
        return NamedLakeFileEvidence(
            path=path,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            byte_count=path.stat().st_size,
        )

    return FactorVolumeProfileLakeInput(
        lake_root=lake_root,
        catalog_file=evidence(catalog_path),
        current_marker=evidence(current_path),
        candidate_marker=evidence(candidate_path),
        artifacts=artifacts,
        manifest_sha256=canonical_sha256(artifacts),
    )


def test_named_minutes_preserve_degraded_provenance_without_replica_fallback(
    tmp_path: Path,
) -> None:
    m = _module()
    prepared, path = _prepared(tmp_path)
    lake = _lake(tmp_path, path)
    with duckdb.connect(str(path)) as raw:
        raw.execute("DELETE FROM minute_bar")
    from rquant.factor.daily_feature_source import open_factor_daily_feature_source
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore

    _sidecar(path)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            _request(path, count=12, days=8),
            metadata_store=metadata,
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF,
        )
    source = m.prepare_factor_volume_profile_source(
        m.FactorVolumeProfilePrepareRequest(prepared_source=prepared, lake_input=lake),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF,
    )
    assert source.volume_profile.lake.catalog_status == "degraded"
    assert (
        source.volume_profile.lake.catalog_file.identity.inode
        == lake.catalog_file.path.stat().st_ino
    )
    assert (
        source.volume_profile.lake.catalog_sha256
        != json.loads(lake.candidate_marker.path.read_text())["catalog_sha256"]
    )
    assert (
        source.volume_profile.lake.verification_scope
        == "named_partitions_current_observation_not_full_authority_chain_or_pit"
    )
    with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
        facts = _batch(source, lease).facts
        assert next(f for f in facts if f.column == "vp90_total_amount").value == 10000.0
        assert all(f.volume_profile_diagnostic.observed_days == 5 for f in facts)


@pytest.mark.parametrize("changed", ("catalog", "marker", "lineage", "partition"))
def test_named_minute_changes_are_rejected_and_own_scratch_is_cleaned(
    tmp_path: Path, changed: str
) -> None:
    m = _module()
    prepared, path = _prepared(tmp_path)
    lake = _lake(tmp_path, path)
    if changed in ("marker", "lineage"):
        from rquant.factor.named_lake import NamedLakeFileEvidence

        current = json.loads(lake.current_marker.path.read_text())
        current["readonly_catalog_sha256" if changed == "marker" else "bootstrap_snapshot_id"] = (
            "f" * 64
        )
        lake.current_marker.path.write_text(json.dumps(current))
        fields = lake.model_dump()
        fields["current_marker"] = NamedLakeFileEvidence(
            path=lake.current_marker.path,
            sha256=hashlib.sha256(lake.current_marker.path.read_bytes()).hexdigest(),
            byte_count=lake.current_marker.path.stat().st_size,
        )
        lake = m.FactorVolumeProfileLakeInput(**fields)
    else:
        target = (
            lake.catalog_file.path
            if changed == "catalog"
            else lake.lake_root / lake.artifacts[0].relative_path
        )
        with target.open("ab") as output:
            output.write(b"changed")
    with pytest.raises((ValueError, OSError)):
        m.prepare_factor_volume_profile_source(
            m.FactorVolumeProfilePrepareRequest(prepared_source=prepared, lake_input=lake),
            lake_root=tmp_path / "lake",
            now=lambda: _AS_OF,
        )
    assert not list((tmp_path / "lake").glob(".vp-prepare-*"))


@pytest.fixture(scope="module")
def descriptor_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_minute_feature_descriptor import descriptor_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


@pytest.fixture(scope="module")
def context_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_minute_feature_descriptor import context_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


def _vp_shape(base: object) -> object:
    from rquant.data_metadata import DatasetSnapshotArtifact
    from rquant.factor.daily_feature_source import VOLUME_PROFILE_FIELDS, FactorDailyFeatureSource
    from rquant.factor.volume_profile_source import (
        FactorVolumeProfilePolicy,
        FactorVolumeProfileReceipt,
    )
    from rquant.runtime_contracts import canonical_sha256

    template = base.tables[0].artifact
    inputs = []
    for table in ("vp_date_input", "vp_close_input", "vp_adjustment_input", "vp_minute_input"):
        artifact = template.model_dump()
        artifact.update(
            dataset_id="factor_volume_profile_input",
            table_name=table,
            relative_path=f"tables/{table}/versions/{template.file_hash}.parquet",
            row_count=0,
        )
        inputs.append(DatasetSnapshotArtifact(**artifact))
    artifact = template.model_dump()
    artifact.update(
        dataset_id="factor_volume_profile_feature",
        table_name="daily_volume_profile_feature",
        primary_key=("ts_code", "trade_date"),
        row_count=len(base.scope.stock_codes) * len(base.calendar_open_days),
        relative_path=f"tables/daily_volume_profile_feature/versions/{template.file_hash}.parquet",
        earliest_time=base.calendar_open_days[0].isoformat(),
        latest_time=base.calendar_open_days[-1].isoformat(),
    )
    receipt = FactorVolumeProfileReceipt(
        policy=FactorVolumeProfilePolicy(implementation_sha256="f" * 64),
        inputs=tuple(inputs),
        artifact=DatasetSnapshotArtifact(**artifact),
        input_rows=0,
        output_rows=artifact["row_count"],
        profile_evaluations=artifact["row_count"],
        max_input_rows=128_000_000,
        max_code_rows=300_000,
        max_output_cells=128_000_000,
    )
    fields = base.model_dump(exclude={"sha256"})
    fields.update(
        schema_version=7,
        base_daily_source=base,
        volume_profile=receipt,
        observed_at=base.completed_read_at,
        completed_read_at=base.completed_read_at,
        fields=tuple(sorted(base.fields + VOLUME_PROFILE_FIELDS, key=lambda f: f.column)),
        value_semantics="volume_profile_derived",
    )
    return FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))


def test_vp_only_expression_from_mixed_catalog_uses_only_selected_dependencies(
    descriptor_template: tuple,
) -> None:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.formula_stream import FactorFormulaStreamRequest

    base, original, *_ = descriptor_template
    source = _vp_shape(base)
    fields = original.adapter_request.formula.definition.model_dump(
        exclude={"max_history_window", "dependency_columns"}
    )
    fields.update(
        expression="vp90_vwap + close",
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True,
            technical_history_available=True,
            stock_features_available=True,
            stock_base_daily_available=True,
            volume_profile_available=True,
            volume_profile_base_daily_available=True,
        ).feature_catalog(),
    )
    payload = original.adapter_request.formula.model_dump()
    payload["definition"] = build_factor_definition(**fields)
    payload["sources"]["daily_features"] = source.select(("vp90_vwap",))
    request = FactorFormulaStreamRequest(**payload)
    assert tuple(f.column for f in request.sources.daily_features.fields) == ("vp90_vwap",)


@pytest.mark.parametrize("count,days", ((7000, 234), (1, 4096)))
def test_v7_maximum_legal_52_field_spec_decodes_and_reads_back_from_ledger(
    descriptor_template: tuple, context_template: tuple, tmp_path: Path, count: int, days: int
) -> None:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.job_spec import _definition_sha256
    from rquant.factor.stream_job_spec import FactorStreamJobSpec, decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_auction_evidence import _auction_shape
    from tests.unit.test_factor_market_temperature_source import _temperature_shape
    from tests.unit.test_factor_minute_feature_descriptor import _minute_shape, _minute_spec
    from tests.unit.test_factor_stock_feature_descriptor import _shape_context, _shape_source
    from tests.unit.test_factor_stock_feature_source import _AS_OF as _CUTOFF

    template, original, *_ = descriptor_template
    minute = _minute_shape(_shape_source(template, count, days=days, longest=True))
    base = _auction_shape(_temperature_shape(minute))
    prior = base.model_dump_json()
    source = _vp_shape(base)
    context = _shape_context(source, *context_template)
    spec = _minute_spec(original, minute, context)
    fields = spec.adapter_request.formula.definition.model_dump(
        exclude={"max_history_window", "dependency_columns"}
    )
    stock = tuple(
        f.column
        for f in source.fields
        if f.column not in ("market_high_60d_ratio_pct", "market_above_ma20_ratio_pct")
    )
    selected = tuple(
        sorted(
            tuple(c for c in stock if c.startswith("vp90_"))
            + tuple(c for c in stock if not c.startswith("vp90_"))[:39]
            + ("market_high_60d_ratio_pct", "market_above_ma20_ratio_pct")
        )
    )
    assert len(selected) == 52

    def balanced(columns: tuple[str, ...]) -> str:
        if len(columns) == 1:
            return columns[0]
        middle = len(columns) // 2
        return f"({balanced(columns[:middle])}+{balanced(columns[middle:])})"

    raw_columns = ("open", "high", "low", "close", "vol", "amount")
    fields.update(
        expression=balanced(selected + raw_columns),
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
            volume_profile_available=True,
            volume_profile_base_daily_available=True,
        ).feature_catalog(),
    )
    definition = build_factor_definition(**fields)
    payload = spec.model_dump()
    payload["adapter_request"].update(daily_feature_source=source)
    payload["adapter_request"]["formula"].update(definition=definition)
    payload["adapter_request"]["formula"]["sources"].update(daily_features=source.select(selected))
    payload["definition_content_sha256"] = _definition_sha256(definition)
    shape = FactorStreamJobSpec(**payload)
    data = canonical_json_bytes(shape.model_dump(mode="json", round_trip=True))
    assert len(data) < 2 * 1024 * 1024
    decoded = decode_factor_job_spec_json(data.decode())
    assert decoded == shape
    assert decoded.adapter_request.daily_feature_source.base_daily_source.model_dump_json() == prior
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _CUTOFF)
    ledger.initialize()
    admitted = ledger.submit("vp-capacity", shape)
    assert ledger.get(admitted.job_id).spec == shape
    (tmp_path / "actual-wide-spec.json").write_bytes(data)
    print(
        "VP_SPEC_CAPACITY",
        json.dumps(
            dict(
                codes=count,
                range_days=days,
                formula_days=len(shape.adapter_request.formula.trading_days),
                fields=52,
                raw_fields=6,
                byte_count=len(data),
                budget=2 * 1024 * 1024,
                decoder="pass",
                ledger_readback="pass",
                capacity_shape_only=True,
            )
        ),
    )
