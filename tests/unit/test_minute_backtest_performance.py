from __future__ import annotations

import json
from decimal import Decimal

import pytest

from rquant.minute_backtest_contracts import FrozenMinuteRuntimeInput
from rquant.minute_backtest_performance import build_minute_performance
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
from tests.unit.test_minute_backtest_producer import original_fixture


def inputs() -> tuple[FrozenMinuteRuntimeInput, MinuteRuntimeReplayResult]:
    original = original_fixture()
    return (FrozenMinuteRuntimeInput.model_validate_json(json.dumps(original["frozen_input"])),
        MinuteRuntimeReplayResult.model_validate_json(json.dumps(original["minute_replay"])))


def test_original_complete_daily_nav_and_v3_fees_feed_original_perf_and_fifo() -> None:
    source, result = inputs()
    value = build_minute_performance(result, runtime=source)
    assert value.status == "complete" and value.metrics is not None
    assert value.basis == "pit_asof_15:00"
    assert value.input_hash == result.input_hash and value.profile_hash == result.profile_hash
    assert [point.daily_return for point in value.daily] == [
        Decimal("100092.899999999999000") / Decimal("100000") - 1,
        Decimal("99256.5100") / Decimal("100092.899999999999000") - 1,
    ]
    assert value.metrics.summary.total_return == pytest.approx(-0.0074349)
    assert len(value.metrics.round_trips) == 1
    assert value.metrics.round_trips[0].net_pnl == pytest.approx(-743.49)
    assert value.metrics.round_trips[0].entry_fee == pytest.approx(5.10)
    assert value.metrics.round_trips[0].exit_fee == pytest.approx(14.49)
    assert value.metrics.benchmark_summary is None and value.metrics.relative is None
    assert value.metrics.overfit_state == "not_evaluated"
    assert value.benchmark_unavailable == "minute_source_has_no_benchmark_series"
    assert value.open_quantity == {}
    july, august = [x for x in value.monthly if x.daily_observations]
    assert (july.year, july.month, august.month) == (2026, 7, 8)
    assert all(x.return_value is None for x in value.monthly if not x.daily_observations)


def test_an_unavailable_day_cannot_be_filled_or_removed_from_complete_performance() -> None:
    source, original = inputs()
    value = original.model_dump(mode="python")
    value["status"] = "incomplete"
    value["daily_status"] = "unavailable"
    value["daily_valuations"][1]["status"] = "unavailable"
    value["daily_valuations"][1]["account"] = None
    value["daily_valuations"][1]["price_proofs"] = ()
    value["daily_valuations"][1]["unavailable_reasons"] = ("test_missing_original_pit_quote",)
    replay = MinuteRuntimeReplayResult.model_validate(value)
    performance = build_minute_performance(replay, runtime=source)
    assert performance.status == "unavailable" and performance.metrics is None
    assert len(performance.daily) == 2
    assert performance.daily[1].nav is None and performance.daily[1].daily_return is None
    assert "daily_nav_unavailable" in performance.unavailable_reasons
    assert performance.monthly == ()


def test_original_incomplete_execution_cannot_claim_complete_performance() -> None:
    source, original = inputs()
    replay = MinuteRuntimeReplayResult.model_validate(original.model_dump(mode="python") | {
        "status": "incomplete", "incomplete_reasons": ("missing_input_prefix",)})
    value = build_minute_performance(replay, runtime=source)
    assert value.metrics is None and value.status == "unavailable"
    assert "execution_incomplete" in value.unavailable_reasons


def test_performance_rejects_a_different_frozen_source_identity() -> None:
    source, result = inputs()
    other = FrozenMinuteRuntimeInput.model_validate(source.model_dump(mode="python") | {"source_version": 2})
    with pytest.raises(ValueError, match="source"):
        build_minute_performance(result, runtime=other)
