"""Synthetic stock-feature browser fixtures retain the canonical Web wire contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rquant.factor.daily_feature_source import STOCK_FEATURE_FIELDS
from rquant.web.models.factor_results import FactorStreamResearchDisplay
from rquant.web.models.factors import FactorCapabilitiesData

_FIXTURE = (
    Path(__file__).resolve().parents[2] / "web/src/pages/factors/factorStockFeature.fixture.json"
)


@pytest.mark.parametrize("mode", ("standalone", "mixed", "mixed_full39"))
def test_stock_browser_fixture_retains_actual_catalog_and_result_sources(mode: str) -> None:
    wire = json.loads(_FIXTURE.read_text(encoding="utf-8"))[mode]
    capability = FactorCapabilitiesData.model_validate_json(json.dumps(wire["capability"]))
    research = FactorStreamResearchDisplay.model_validate_json(json.dumps(wire["research"]))
    source = research.daily_features
    assert source is not None and source.stock_features is not None
    assert capability.version == "daily_stock_v1"
    assert len(capability.fields) == (29 if mode == "standalone" else 45)
    assert source.value_semantics == "stock_features_derived"
    assert source.price_basis == source.recursive_initialization == "field_specific"
    stock = tuple(field for field in source.fields if field.table == "daily_stock_feature")
    assert stock == STOCK_FEATURE_FIELDS
    assert len(source.fields) == {"standalone": 23, "mixed": 25, "mixed_full39": 39}[mode]
    assert (source.technical_history is not None) == (mode != "standalone")
    assert source.stock_features.policy.price_windows == (90, 120, 250)
    catalog = {field.column: field for field in capability.fields}
    for field in stock:
        assert catalog[field.column].name_zh == field.name_zh
        assert catalog[field.column].description_zh == field.description_zh
        assert catalog[field.column].unit == field.unit
        assert catalog[field.column].tracking_supported
    assert catalog["ma_alignment"].unit == "binary"
    assert catalog["price_window_days_90d"].unit == "observations"
    assert catalog["price_percentile_250d"].unit == "ratio"


@pytest.mark.parametrize("mode", ("standalone", "mixed", "mixed_full39"))
def test_stock_browser_fixture_keeps_null_grid_and_separate_missing_reason_families(
    mode: str,
) -> None:
    wire = json.loads(_FIXTURE.read_text(encoding="utf-8"))[mode]
    research = FactorStreamResearchDisplay.model_validate_json(json.dumps(wire["research"]))
    assert research.daily_feature_coverage_days
    for day in research.daily_feature_coverage_days:
        counts = {item.column: item for item in day.counts}
        assert day.computation_stock_count == 12
        assert len(counts) == {"standalone": 23, "mixed": 25, "mixed_full39": 39}[mode]
        position = counts["price_position_90d_pct"]
        observations = counts["price_window_days_90d"]
        assert (position.valid, position.null, position.missing) == (9, 3, 0)
        assert (observations.valid, observations.null, observations.missing) == (11, 1, 0)
        assert {item.reason: item.count for item in position.stock_reasons} == {
            "missing_daily_data": 1,
            "missing_required_factor": 2,
        }
        assert not position.reasons
        assert counts["ma_alignment"].null == 4
        assert any(
            item.reason == "insufficient_history" for item in counts["ma_alignment"].stock_reasons
        )
        if mode != "standalone":
            assert counts["ma5"].reasons
            assert not counts["ma5"].stock_reasons
