"""Lossless minute descriptors at the unchanged job admission boundary."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def descriptor_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_stock_feature_descriptor import descriptor_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


@pytest.fixture(scope="module")
def context_template(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_stock_feature_descriptor import context_template as fixture

    return fixture.__wrapped__(tmp_path_factory)


def _minute_shape(base: object) -> object:
    from rquant.data_metadata import DatasetSnapshotArtifact
    from rquant.factor.daily_feature_source import (
        MINUTE_FEATURE_FIELDS,
        FactorDailyFeatureCounts,
        FactorDailyFeatureSource,
        FactorDailyFeatureTable,
    )
    from rquant.factor.minute_feature_source import (
        FactorMinuteFeatureCode,
        FactorMinuteFeaturePolicy,
        FactorMinuteFeatureReceipt,
    )
    from rquant.runtime_contracts import canonical_sha256

    n = len(base.scope.stock_codes)
    days = len(base.calendar_open_days)
    rows = n * days
    template = base.stock_features.inputs[0].model_dump()
    template.update(
        dataset_id="factor_minute_feature_input",
        table_name="minute_feature_input",
        primary_key=("ts_code", "trade_time", "freq", "source"),
        row_count=n * 6000,
    )
    receipt = FactorMinuteFeatureReceipt(
        policy=FactorMinuteFeaturePolicy(implementation_sha256="a" * 64),
        inputs=(DatasetSnapshotArtifact(**template),),
        codes=tuple(
            FactorMinuteFeatureCode(
                stock_code=c,
                input_rows=6000,
                input_observations=6000,
                raw_start_date=base.scope.start_date,
                raw_end_date=base.scope.end_date,
            )
            for c in base.scope.stock_codes
        ),
        input_rows=n * 6000,
        max_input_rows=64_000_000,
        max_code_rows=300_000,
        max_output_cells=128_000_000,
    )
    table = base.tables[-1].model_dump()
    table.update(
        table_name="daily_minute_feature",
        counts=tuple(
            FactorDailyFeatureCounts(column=f.column, valid=rows, missing=0, null=0, non_finite=0)
            for f in MINUTE_FEATURE_FIELDS
        ),
    )
    table["artifact"].update(
        table_name="daily_minute_feature",
        relative_path=f"tables/daily_minute_feature/versions/{table['artifact']['file_hash']}.parquet",
    )
    fields = base.model_dump(exclude={"sha256"})
    fields.update(
        schema_version=4,
        base_daily_source=base,
        minute_features=receipt,
        observed_at=base.completed_read_at,
        completed_read_at=base.completed_read_at,
        fields=tuple(sorted(base.fields + MINUTE_FEATURE_FIELDS, key=lambda f: f.column)),
        tables=base.tables + (FactorDailyFeatureTable(**table),),
        value_semantics="minute_features_derived",
    )
    return FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))


def _minute_spec(spec: object, source: object, context: object) -> object:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.job_spec import _definition_sha256
    from rquant.factor.stream_job_spec import FactorStreamJobSpec
    from tests.unit.test_factor_stock_feature_descriptor import _shape_spec

    shaped = _shape_spec(spec, source.base_daily_source, longest=True)
    original = spec.adapter_request.formula.definition

    def balanced(columns: tuple[str, ...]) -> str:
        if len(columns) == 1:
            return columns[0]
        middle = len(columns) // 2
        return f"({balanced(columns[:middle])} + {balanced(columns[middle:])})"

    definition = build_factor_definition(
        factor_id=original.factor_id,
        name_zh=original.name_zh,
        category=original.category,
        direction=original.direction,
        version=original.version,
        earliest_available_date=None,
        expression=balanced(tuple(f.column for f in source.fields)),
        feature_catalog=historical_daily_capabilities(
            daily_features_available=True,
            technical_history_available=True,
            stock_features_available=True,
            stock_base_daily_available=True,
            minute_features_available=True,
            minute_base_daily_available=True,
        ).feature_catalog(),
    )
    fields = shaped.model_dump()
    adapter = fields["adapter_request"]
    adapter.update(daily_feature_source=source, context=context)
    adapter["formula"].update(definition=definition, neutralization="industry_size")
    adapter["formula"]["sources"].update(
        daily_features=source.select(definition.dependency_columns), context=context.sources
    )
    fields["definition_content_sha256"] = _definition_sha256(definition)
    return FactorStreamJobSpec(**fields)


@pytest.mark.parametrize("count,days", ((7000, 234), (1, 4096)))
def test_maximum_50_fields_with_context_original_decoder_and_ledger(
    descriptor_template: tuple, context_template: tuple, tmp_path: Path, count: int, days: int
) -> None:
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.stream_job_spec import decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_stock_feature_descriptor import _shape_context, _shape_source
    from tests.unit.test_factor_stock_feature_source import _AS_OF

    template, spec, *_ = descriptor_template
    base = _shape_source(template, count, days=days, longest=True)
    source = _minute_shape(base)
    context = _shape_context(source, *context_template)
    shape = _minute_spec(spec, source, context)
    data = canonical_json_bytes(shape.model_dump(mode="json", round_trip=True))
    assert len(data) < 2 * 1024 * 1024 - 100_000
    decoded = decode_factor_job_spec_json(data.decode())
    assert decoded == shape
    assert decoded.adapter_request.daily_feature_source.base_daily_source == base
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    ledger.initialize()
    admitted = ledger.submit("minute-capacity", shape)
    assert ledger.get(admitted.job_id).spec == shape


@pytest.mark.parametrize(
    "change",
    (
        "short",
        "long",
        "row_width",
        "type",
        "date_index",
        "date_duplicate",
        "tamper",
        "base_clock",
        "base_hash",
    ),
)
def test_v4_rejects_malformed_or_unpaired_complete_witnesses(
    descriptor_template: tuple, change: str
) -> None:
    from pydantic import ValidationError

    from rquant.factor.daily_feature_source import FactorDailyFeatureSource
    from rquant.strict_json import canonical_json_bytes

    source = _minute_shape(descriptor_template[0])
    wire = source.model_dump(mode="json")
    rows = wire["minute_features"]["codes"]
    if change == "short":
        rows.pop()
    elif change == "long":
        rows.append(rows[0])
    elif change == "row_width":
        rows[0].pop()
    elif change == "type":
        rows[0][0] = True
    elif change == "date_index":
        rows[0][2] = len(wire["minute_features"]["dates"])
    elif change == "date_duplicate":
        wire["minute_features"]["dates"].append(wire["minute_features"]["dates"][0])
    elif change == "tamper":
        wire["technical_history"]["codes"][0], wire["technical_history"]["codes"][2] = (
            wire["technical_history"]["codes"][2],
            wire["technical_history"]["codes"][0],
        )
    elif change == "base_clock":
        wire["base_daily_source"]["observed_at"] = "2026-10-02T08:00:00Z"
    else:
        wire["base_daily_source"]["base_daily_source"]["sha256"] = "0" * 64
    with pytest.raises(ValidationError):
        FactorDailyFeatureSource.model_validate_json(canonical_json_bytes(wire))


@pytest.mark.parametrize("version", (None, 1, 2, 3))
def test_v4_optional_base_restores_original_bytes_and_independent_clock(
    descriptor_template: tuple, version: int | None
) -> None:
    from rquant.factor.daily_feature_source import (
        MINUTE_FEATURE_FIELDS,
        STORED_DAILY_FIELDS,
        FactorDailyFeatureSource,
    )
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strict_json import canonical_json_bytes

    original = descriptor_template[0]
    source = _minute_shape(original)
    base = None if version is None else original if version == 3 else original.base_daily_source
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
    before = None if base is None else canonical_json_bytes(base.model_dump(mode="json"))
    fields = source.model_dump(
        exclude={"sha256", "base_daily_source", "technical_history", "stock_features"}
    )
    fields.update(
        fields=tuple(
            sorted(
                (() if base is None else base.fields) + MINUTE_FEATURE_FIELDS,
                key=lambda f: f.column,
            )
        ),
        tables=(() if base is None else base.tables) + source.tables[-1:],
    )
    if base is not None:
        fields["base_daily_source"] = base
        if base.technical_history is not None:
            fields["technical_history"] = base.technical_history
        if base.stock_features is not None:
            fields["stock_features"] = base.stock_features
    source = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
    restored = FactorDailyFeatureSource.model_validate_json(source.model_dump_json())
    assert restored == source
    if base is not None:
        assert restored.base_daily_source.sha256 == base.sha256
        assert canonical_json_bytes(restored.base_daily_source.model_dump(mode="json")) == before


def test_v4_serialization_fixed_row_schema_and_marker() -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource

    definitions = FactorDailyFeatureSource.model_json_schema(mode="serialization")["$defs"]
    for name, width in (
        ("_V4TechnicalReceipt", 7),
        ("_V4StockReceipt", 4),
        ("_V4MinuteReceipt", 4),
    ):
        properties = definitions[name]["properties"]
        assert properties["code_format"]["const"] == "scope_ordered_date_indices_v1"
        row = properties["codes"]["items"]
        assert row["minItems"] == row["maxItems"] == len(row["prefixItems"]) == width
