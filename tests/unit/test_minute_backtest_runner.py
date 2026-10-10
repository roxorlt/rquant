from __future__ import annotations

import base64
import hashlib

import pytest
from pydantic import ValidationError

from rquant.minute_backtest_contracts import (
    MAX_INPUT_BYTES,
    MAX_WORK_UNITS,
    MinuteReplayMaterial,
    MinuteReplayWork,
)


def test_material_is_pathless_and_content_bound() -> None:
    payload = b"original immutable material"
    value = MinuteReplayMaterial(
        relative_path="warmup.parquet",
        content_base64=base64.b64encode(payload).decode("ascii"),
        content_sha256=hashlib.sha256(payload).hexdigest(),
    )
    assert value.payload() == payload
    for path in ("/tmp/source", "../broker.sqlite3", "market/../../source", ".env", "market/cursors/x"):
        with pytest.raises(ValueError):
            MinuteReplayMaterial(**(value.model_dump() | {"relative_path": path}))
    with pytest.raises(ValueError, match="hash"):
        MinuteReplayMaterial(**(value.model_dump() | {"content_sha256": "a" * 64}))
    with pytest.raises(ValidationError):
        MinuteReplayMaterial(**(value.model_dump() | {"path": "/tmp"}))


def test_work_estimate_counts_physical_rows_and_cartesian_bound() -> None:
    work = MinuteReplayWork(raw_rows=17, warmup_rows=25, static_rows=3, market_batches=4, union_codes=2, daily_observations=2)
    assert work.candidate_bound == 8
    assert work.paper_bound == 8
    assert work.daily_price_bound == 4
    assert work.work_units == 71
    assert work.static_duration_ms == 71_000
    assert MAX_INPUT_BYTES == 16_777_216
    assert MAX_WORK_UNITS == 20_000
    with pytest.raises(ValueError, match="work"):
        MinuteReplayWork(raw_rows=20_000, warmup_rows=0, static_rows=1, market_batches=1, union_codes=1, daily_observations=1)
    with pytest.raises(ValidationError):
        MinuteReplayWork(raw_rows=True, warmup_rows=0, static_rows=0, market_batches=1, union_codes=1, daily_observations=1)
    boundary = MinuteReplayWork(raw_rows=19_995, warmup_rows=0, static_rows=0, market_batches=1, union_codes=1, daily_observations=1)
    assert boundary.work_units == 20_000 and boundary.static_duration_ms == 20_000_000
    with pytest.raises(ValidationError):
        MinuteReplayWork(raw_rows=0, warmup_rows=0, static_rows=0, market_batches=1, union_codes=501, daily_observations=1)


def test_original_paper_policy_rejects_zero_lag_and_non_lot_quantity() -> None:
    from datetime import timedelta
    from rquant.paper_signal_worker import PaperSignalPolicy
    from rquant.signal_contracts import SignalAction

    for quantity, lag in ((99, timedelta(minutes=1)), (100, timedelta(0))):
        with pytest.raises(ValueError):
            PaperSignalPolicy(account_id="paper", execution_lag=lag, action_quantities={SignalAction.B_INTENT: quantity,
                SignalAction.REDUCE: 100, SignalAction.S_INTENT: 100}, producer_commit="a" * 40)


def test_adapter_is_additive_and_requires_independent_source_receipt() -> None:
    from rquant.minute_backtest_adapter import MinuteRuntimeReplayAdapter
    from rquant.research_run_spec import ResearchJobType

    assert MinuteRuntimeReplayAdapter.adapter_id == "minute-runtime-replay"
    assert MinuteRuntimeReplayAdapter.adapter_version == "1"
    assert MinuteRuntimeReplayAdapter.strategy_name == "minute_runtime_replay"
    assert MinuteRuntimeReplayAdapter.job_type is ResearchJobType.STRATEGY_REPLAY
    with pytest.raises(TypeError):
        MinuteRuntimeReplayAdapter()


def test_admission_parameters_must_match_independent_physical_work() -> None:
    from datetime import date
    from rquant.minute_backtest_adapter import MinuteRuntimeReplayAdapter, MinuteRuntimeReplayParameters
    from rquant.minute_backtest_contracts import MinuteRuntimeSourceReceipt

    work = MinuteReplayWork(raw_rows=17, warmup_rows=25, static_rows=3, market_batches=4, union_codes=2, daily_observations=2)
    expected = MinuteRuntimeSourceReceipt(source_key="trusted-input", source_version=1, owner_id="trusted-owner",
        input_hash="a" * 64, producer_commit="b" * 40, start_date=date(2026, 7, 31), end_date=date(2026, 8, 3),
        audit_run_id="independent-producer-audit", dataset_snapshot_id="c" * 64, work=work, profile_hash="d" * 64,
        strategy_id="n_shape", strategy_version=1)
    adapter = MinuteRuntimeReplayAdapter(expected=expected)
    parameters = MinuteRuntimeReplayParameters(input_hash=expected.input_hash, profile_hash=expected.profile_hash,
        source_key=expected.source_key, source_version=expected.source_version, strategy_id=expected.strategy_id,
        strategy_version=1, work_units=work.work_units)
    assert adapter.bound_parameters(parameters) == parameters
    for change in ({"work_units": 1}, {"profile_hash": "e" * 64}, {"strategy_id": "auction_gap"}):
        with pytest.raises(ValueError, match="independent.*physical work"):
            adapter.bound_parameters(parameters.model_copy(update=change))


def test_daily_result_tables_cannot_expand_original_artifact_capacity() -> None:
    from rquant.minute_backtest_contracts import MinuteReplayResultBudget
    budget = MinuteReplayResultBudget()
    assert (budget.table_count, budget.table_bytes, budget.total_bytes, budget.wire_bytes) == (8, 33_554_432, 62_128_104, 83_886_080)
    for name, increased in (("table_count", 9), ("table_bytes", 33_554_433), ("total_bytes", 62_128_105), ("wire_bytes", 83_886_081)):
        with pytest.raises(ValueError):
            MinuteReplayResultBudget.model_validate(budget.model_dump() | {name: increased})
