"""Offline typed capacity shapes and lossless v3 persistence boundaries."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def descriptor_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_stock_feature_pipeline import _configuration, _plan

    root, reference, browser, prepared, config, source = _configuration(
        tmp_path_factory.mktemp("stock-descriptor-template")
    )
    return source, _plan(root, reference, browser, config).spec, prepared, config


def _shape_source(
    source: object, count: int, *, mixed: bool = True, days: int = 6, longest: bool = False
) -> object:
    from rquant.factor.daily_feature_source import (
        STOCK_FEATURE_FIELDS,
        FactorDailyFeatureSource,
    )
    from rquant.research_snapshot import FactorComputationScope
    from rquant.runtime_contracts import canonical_sha256

    codes = tuple(f"{i:06d}.SZ" for i in range(1, count + 1))
    scope_fields = source.scope.model_dump()
    scope_fields.update(
        stock_codes=codes, start_date=source.scope.end_date - timedelta(days=days - 1)
    )
    scope = FactorComputationScope(**scope_fields)
    dates = tuple(scope.start_date + timedelta(days=i) for i in range(days))
    tables = []
    for table in source.tables if mixed else source.tables[-1:]:
        fields = table.model_dump()
        rows = count * days
        fields.update(
            row_count=rows,
            code_counts=tuple({"code": c, "count": days} for c in codes),
            date_counts=tuple({"date": d, "count": count} for d in dates),
            counts=tuple(
                {"column": c.column, "valid": rows, "missing": 0, "null": 0, "non_finite": 0}
                for c in table.counts
            ),
            structural_missing_rows=0,
            rows_on_closed_dates=0,
        )
        fields["artifact"].update(
            row_count=rows, earliest_time=dates[0].isoformat(), latest_time=dates[-1].isoformat()
        )
        tables.append(type(table)(**fields))
    fields = source.model_dump(exclude={"sha256"})
    fields.update(
        scope=scope,
        scope_content_hash=canonical_sha256(scope),
        calendar_open_days=dates,
        tables=tuple(tables),
    )
    for name, rows_key in (
        ("stock_features", "input_rows"),
        ("technical_history", "input_observations"),
    ):
        receipt = getattr(source, name)
        data = receipt.model_dump()
        code = receipt.codes[0].model_dump()
        if longest:
            observations = min(50_000, 16_000_000 // count)
            code.update(raw_start_date=scope.start_date - timedelta(days=observations))
            if name == "technical_history":
                code.update(
                    input_observations=observations,
                    first_valid_date=scope.start_date,
                    reliable_end_date=scope.end_date - timedelta(days=1),
                    leading_invalid_observations=1,
                    break_date=scope.end_date,
                    break_reason="non_finite_adjusted_price",
                )
            else:
                code.update(
                    input_rows=observations,
                    input_observations=observations,
                    raw_end_date=scope.end_date,
                )
        data.update(
            codes=tuple({**code, "stock_code": c} for c in codes),
            input_rows=code[rows_key] * count,
            max_output_cells=64_000_000,
        )
        data["inputs"][0]["row_count"] = data["input_rows"]
        fields[name] = type(receipt)(**data)
    if mixed:
        base = source.base_daily_source.model_dump(exclude={"sha256"})
        base.update(
            scope=scope,
            scope_content_hash=fields["scope_content_hash"],
            calendar_open_days=dates,
            tables=tuple(tables[:-1]),
            technical_history=fields["technical_history"],
        )
        fields["base_daily_source"] = FactorDailyFeatureSource(
            **base, sha256=canonical_sha256(base)
        )
    else:
        fields.pop("technical_history")
        fields.pop("base_daily_source")
        fields["fields"] = STOCK_FEATURE_FIELDS
    return FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))


def _shape_spec(spec: object, source: object, *, longest: bool = False) -> object:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.job_spec import _definition_sha256
    from rquant.factor.stream_job_spec import FactorStreamJobSpec
    from rquant.factor.time_series import MAX_TRADE_DAYS, DecisionTime
    from tests.unit.test_factor_stream_adapter import _at

    fields = spec.model_dump()
    adapter = fields["adapter_request"]
    adapter["source"]["scope"] = source.scope
    adapter["scope_content_hash"] = source.scope_content_hash
    adapter["formula"]["computation_stock_codes"] = source.scope.stock_codes
    if longest:
        days = source.calendar_open_days[-min(MAX_TRADE_DAYS, len(source.calendar_open_days) - 1) :]
        adapter["formula"].update(
            trading_days=days,
            decision_times=tuple(
                DecisionTime(trade_date=d, decision_at=_at(d, 9, 25)) for d in days
            ),
        )
        adapter["evaluation_days"] = days[1:-1]
    definition = spec.adapter_request.formula.definition
    if source.base_daily_source is None:
        definition = build_factor_definition(
            factor_id=definition.factor_id,
            name_zh=definition.name_zh,
            category=definition.category,
            direction=definition.direction,
            version=definition.version,
            earliest_available_date=None,
            expression="price_position_90d_pct / 100 + ma_alignment + price_percentile_250d",
            feature_catalog=historical_daily_capabilities(
                daily_features_available=True, stock_features_available=True
            ).feature_catalog(),
        )
    adapter["formula"]["definition"] = definition
    adapter["formula"]["sources"]["daily_features"] = source.select(definition.dependency_columns)
    adapter["daily_feature_source"] = source
    fields["definition_content_sha256"] = _definition_sha256(definition)
    return FactorStreamJobSpec(**fields)


@pytest.fixture(scope="module")
def context_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_neutralization_sources import _configured_context

    with pytest.MonkeyPatch.context() as patch:
        *_, industry, cap = _configured_context(
            tmp_path_factory.mktemp("stock-descriptor-context"), patch
        )
    return industry, cap


def _shape_context(source: object, industry: object, cap: object) -> object:
    from rquant.factor.neutralization_context import FactorNeutralizationContext
    from rquant.runtime_contracts import canonical_sha256

    shared = {
        key: getattr(source, key)
        for key in (
            "prepared_source_sha256",
            "prepared_snapshot_id",
            "prepared_binding_hash",
            "scope_content_hash",
            "scope",
            "code_commit",
        )
    }
    fields = industry.model_dump(exclude={"sha256"})
    fields.update(shared)
    industry = type(industry)(**fields, sha256=canonical_sha256(fields))
    fields = cap.model_dump(exclude={"sha256"})
    fields.update(
        shared, generation=source.generation, calendar_open_days=source.calendar_open_days
    )
    days = source.calendar_open_days
    rows = len(source.scope.stock_codes) * len(days)
    fields["artifact"].update(
        row_count=rows,
        earliest_time=source.scope.start_date.isoformat(),
        latest_time=source.scope.end_date.isoformat(),
    )
    fields["observation"].update(
        row_count=rows,
        valid_rows=rows,
        null_rows=0,
        non_positive_rows=0,
        non_finite_rows=0,
        structural_missing_rows=0,
        rows_on_closed_dates=0,
        code_counts=tuple({"code": c, "count": len(days)} for c in source.scope.stock_codes),
        date_counts=tuple({"date": d, "count": len(source.scope.stock_codes)} for d in days),
    )
    cap = type(cap)(**fields, sha256=canonical_sha256(fields))
    return FactorNeutralizationContext(
        prepared_source_sha256=source.prepared_source_sha256,
        snapshot_id=source.prepared_snapshot_id,
        binding_hash=source.prepared_binding_hash,
        scope_content_hash=source.scope_content_hash,
        scope=source.scope,
        generation=source.generation,
        code_commit=source.code_commit,
        industry=industry,
        market_cap=cap,
    )


@pytest.mark.parametrize("count,days", ((7000, 6), (7000, 234), (1, 4096)))
def test_maximum_spec_with_unchanged_industry_size_context_enters_ledger(
    descriptor_template: tuple, context_template: tuple, tmp_path: Path, count: int, days: int
) -> None:
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.stream_job_spec import FactorStreamJobSpec, decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_stock_feature_source import _AS_OF

    template, spec, _, _ = descriptor_template
    source = _shape_source(template, count, days=days, longest=True)
    shape = _shape_spec(spec, source, longest=True)
    context = _shape_context(source, *context_template)
    fields = shape.model_dump()
    fields["adapter_request"]["context"] = context
    fields["adapter_request"]["formula"]["neutralization"] = "industry_size"
    fields["adapter_request"]["formula"]["sources"]["context"] = context.sources
    shape = FactorStreamJobSpec(**fields)
    data = canonical_json_bytes(shape.model_dump(mode="json", round_trip=True))
    assert len(data) < 2 * 1024 * 1024 - 150_000
    assert decode_factor_job_spec_json(data.decode()) == shape
    assert shape.adapter_request.context == context
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    ledger.initialize()
    admitted = ledger.submit("stock-context-capacity", shape)
    assert admitted.status == "queued"
    assert ledger.get(admitted.job_id).spec == shape


@pytest.mark.parametrize("count", (5571, 7000))
@pytest.mark.parametrize("mixed", (False, True), ids=("standalone", "mixed"))
def test_market_size_spec_enters_original_decoder_and_persisted_ledger(
    descriptor_template: tuple, tmp_path: Path, count: int, mixed: bool
) -> None:
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.stream_job_spec import decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_stock_feature_source import _AS_OF

    template, spec, _, _ = descriptor_template
    source = _shape_source(template, count, mixed=mixed)
    shape = _shape_spec(spec, source)
    data = canonical_json_bytes(shape.model_dump(mode="json", round_trip=True))
    assert len(data) < 1_800_000
    assert decode_factor_job_spec_json(data.decode()) == shape
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    ledger.initialize()
    admitted = ledger.submit("stock-capacity", shape)
    assert admitted.status == "queued"
    assert ledger.get(admitted.job_id).spec == shape


@pytest.mark.parametrize("count,days", ((7000, 234), (1, 4096)))
def test_longest_allowed_calendar_and_counts_fit_without_witness_loss(
    descriptor_template: tuple, count: int, days: int
) -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource
    from rquant.factor.stream_job_spec import decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes

    template, spec, _, _ = descriptor_template
    source = _shape_source(template, count, days=days, longest=True)
    shape = _shape_spec(spec, source, longest=True)
    data = canonical_json_bytes(shape.model_dump(mode="json", round_trip=True))
    assert len(data) < 1_800_000
    assert decode_factor_job_spec_json(data.decode()) == shape
    assert FactorDailyFeatureSource.model_validate_json(source.model_dump_json()) == source


@pytest.mark.parametrize("version", (1, 2))
def test_compact_base_preserves_original_defaults_bytes_digest_and_clock(
    descriptor_template: tuple, version: int
) -> None:
    from pydantic import TypeAdapter

    from rquant.factor.daily_feature_source import (
        STOCK_FEATURE_FIELDS,
        STORED_DAILY_FIELDS,
        FactorDailyFeatureSource,
    )
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strict_json import canonical_json_bytes

    source, _, _, _ = descriptor_template
    base = source.base_daily_source
    if version == 1:
        fields = base.model_dump(exclude={"sha256", "technical_history"})
        fields.update(
            schema_version=1,
            fields=STORED_DAILY_FIELDS,
            value_semantics="stored_not_recomputed",
            price_basis="unverified",
            recursive_initialization="unverified",
        )
        base = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
        fields = source.model_dump(exclude={"sha256", "technical_history"})
        fields.update(
            base_daily_source=base,
            fields=tuple(sorted(base.fields + STOCK_FEATURE_FIELDS, key=lambda f: f.column)),
        )
        source = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
    old_json = canonical_json_bytes(
        TypeAdapter(object).dump_python(base.model_dump(mode="python"), mode="json")
    )
    assert canonical_json_bytes(base.model_dump(mode="json")) == old_json
    assert b"ordered_rows_v1" not in old_json and b"shared_parent_v1" not in old_json
    assert canonical_sha256(base.model_dump(exclude={"sha256"})) == base.sha256
    compact = source.model_dump(mode="json")
    assert set(compact["base_daily_source"]) == {
        "representation",
        "schema_version",
        "sha256",
        "read_mode",
        "observed_at",
        "completed_read_at",
    }
    restored = FactorDailyFeatureSource.model_validate_json(canonical_json_bytes(compact))
    assert restored == source and restored.base_daily_source == base
    assert restored.base_daily_source.observed_at != restored.observed_at
    assert canonical_json_bytes(restored.base_daily_source.model_dump(mode="json")) == old_json


@pytest.mark.parametrize(
    "family,change",
    (
        ("technical_history", "short"),
        ("technical_history", "long"),
        ("technical_history", "type"),
        ("technical_history", "swapped"),
        ("technical_history", "reordered"),
        ("technical_history", "duplicate"),
        ("stock_features", "short"),
        ("stock_features", "type"),
        ("stock_features", "missing"),
        ("table", "short"),
        ("table", "duplicate"),
        ("table", "count"),
        ("base", "hash"),
        ("base", "clock"),
        ("parent", "generation"),
        ("parent", "missing"),
    ),
)
def test_compact_wire_rejects_malformed_rows_or_unpaired_witnesses(
    descriptor_template: tuple, family: str, change: str
) -> None:
    from pydantic import ValidationError

    from rquant.factor.daily_feature_source import FactorDailyFeatureSource
    from rquant.strict_json import canonical_json_bytes

    source, _, _, _ = descriptor_template
    wire = source.model_dump(mode="json")
    if family in ("technical_history", "stock_features", "table"):
        rows = wire["tables"][0]["code_counts"] if family == "table" else wire[family]["codes"]
        if change == "short":
            rows[0].pop()
        elif change == "long":
            rows[0].append(None)
        elif change == "type":
            rows[0][0] = str(rows[0][0])
        elif change == "swapped":
            rows[0][0], rows[0][1] = rows[0][1], rows[0][0]
        elif change == "reordered":
            rows[0], rows[2] = rows[2], rows[0]
        elif change == "duplicate":
            rows.insert(1, list(rows[0]))
        elif change == "missing":
            rows.pop()
        elif change == "count":
            rows[0][0] += 1
    elif family == "base":
        wire["base_daily_source"]["sha256" if change == "hash" else "observed_at"] = (
            "0" * 64 if change == "hash" else wire["observed_at"]
        )
    elif change == "generation":
        wire["generation"]["sidecar_sha256"] = "0" * 64
    else:
        wire.pop("scope")
    with pytest.raises(ValidationError):
        FactorDailyFeatureSource.model_validate_json(canonical_json_bytes(wire))


def test_original_job_spec_byte_budget_still_rejects_oversized_json() -> None:
    from rquant.factor.stream_job_spec import decode_factor_job_spec_json

    with pytest.raises(ValueError, match="exceeds byte budget"):
        decode_factor_job_spec_json(" " * (2 * 1024 * 1024 + 1))


def test_compact_serialization_schema_has_explicit_scope_order_and_fixed_columns() -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource

    definitions = FactorDailyFeatureSource.model_json_schema(mode="serialization")["$defs"]
    for name, field, columns in (
        ("_V3TechnicalReceipt", "codes", 7),
        ("_V3StockReceipt", "codes", 4),
        ("_V3Table", "code_counts", 1),
    ):
        properties = definitions[name]["properties"]
        marker = properties["code_format" if field == "codes" else "code_counts_format"]
        assert marker["const"] == "scope_ordered_rows_v1"
        row = properties[field]["items"]
        assert row["minItems"] == row["maxItems"] == len(row["prefixItems"]) == columns
