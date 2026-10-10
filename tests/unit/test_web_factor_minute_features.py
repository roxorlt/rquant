"""Synthetic minute browser fixtures retain the generated Web contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rquant.factor.daily_feature_source import MINUTE_FEATURE_FIELDS
from rquant.web.models.factor_results import FactorStreamResearchDisplay
from rquant.web.models.factors import FactorCapabilitiesData

_FIXTURE = (
    Path(__file__).resolve().parents[2] / "web/src/pages/factors/factorMinuteFeature.fixture.json"
)


@pytest.mark.parametrize("mode", ("standalone", "mixed"))
def test_minute_browser_fixture_keeps_actual_catalog_and_consumed_width(mode: str) -> None:
    wire = json.loads(_FIXTURE.read_text())[mode]
    capability = FactorCapabilitiesData.model_validate_json(json.dumps(wire["capability"]))
    research = FactorStreamResearchDisplay.model_validate_json(json.dumps(wire["research"]))
    source = research.daily_features
    assert source is not None and source.minute_features is not None
    assert source.value_semantics == "minute_features_derived"
    assert len(source.fields) == (11 if mode == "standalone" else 50)
    assert len(capability.fields) == (17 if mode == "standalone" else 56)
    assert capability.version == "daily_minute_v1"
    minute_fields = tuple(f for f in source.fields if f.table == "daily_minute_feature")
    assert minute_fields == MINUTE_FEATURE_FIELDS
    assert (source.stock_features is not None) == (mode == "mixed")
    assert (source.technical_history is not None) == (mode == "mixed")
    assert source.minute_features.policy.panel_clock == "15:00:00"
    assert source.minute_features.policy.evaluation_clock == "next_sse_day_09:25"
    assert source.minute_features.policy.lookback_days == 20
    catalog = {f.column: f for f in capability.fields}
    for field in MINUTE_FEATURE_FIELDS:
        assert catalog[field.column].unit == field.unit
        assert catalog[field.column].name_zh == field.name_zh
        assert catalog[field.column].description_zh == field.description_zh
        assert catalog[field.column].tracking_supported


@pytest.mark.parametrize("mode", ("standalone", "mixed"))
def test_minute_browser_fixture_keeps_target_and_not_applicable_reason_families(mode: str) -> None:
    wire = json.loads(_FIXTURE.read_text())[mode]
    research = FactorStreamResearchDisplay.model_validate_json(json.dumps(wire["research"]))
    assert research.daily_feature_coverage_days
    for day in research.daily_feature_coverage_days:
        counts = {item.column: item for item in day.counts}
        assert day.computation_stock_count == 12
        assert len(counts) == (11 if mode == "standalone" else 50)
        for column in ("hist_intraday_days_20d", "signal_opening_segment", "signal_minute_amount"):
            count = counts[column]
            assert (count.valid, count.null, count.missing) == (10, 2, 0)
            assert not count.reasons and not count.stock_reasons
            assert {r.reason: r.count for r in count.minute_reasons} == {"missing_target_minute": 2}
        opening = counts["signal_opening_segment_amount"]
        assert (opening.valid, opening.null, opening.missing) == (0, 12, 0)
        assert {r.reason: r.count for r in opening.minute_reasons} == {
            "missing_target_minute": 2,
            "not_applicable": 10,
        }
        if mode == "mixed":
            assert counts["ma5"].reasons and not counts["ma5"].minute_reasons
            assert counts["price_position_90d_pct"].stock_reasons
            assert not counts["price_position_90d_pct"].minute_reasons
