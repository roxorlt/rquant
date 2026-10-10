"""Explicit complete synthetic bars on the original parameter execution chain."""

from pathlib import Path

import pytest

from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.minute_backtest_parameter_runner import run_minute_parameter_replay
from rquant.paper_signal_worker import PaperSignalQueueStatus
from rquant.signal_contracts import SignalAction
from tests.support.minute_parameter_runtime_fixture import parameter_runtime_fixture


@pytest.mark.parametrize("frequency", ("5min", "15min", "30min", "60min"))
def test_physical_frequency_bars_execute_original_nonzero_broker_and_daily_nav(tmp_path: Path, frequency: str) -> None:
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(freq=frequency,
        paper={"stop_loss_pct": 0.012345, "entry_slippage_pct": 0.0002}))
    value, receipt = parameter_runtime_fixture(tmp_path / "source", parameters)
    assert value.source_frequency == frequency
    replay = run_minute_parameter_replay(value, expected=receipt, research_root=tmp_path / "actual-run")
    assert replay.parameters == parameters
    assert replay.status == "complete" and replay.daily_status == "complete"
    assert any(signal.action is SignalAction.B_INTENT for signal in replay.signals)
    assert replay.fills and all(fill.total_fees > 0 for fill in replay.fills)
    assert all(day.account is not None for day in replay.daily_valuations)
    (tmp_path / "actual-frequency-replay.json").write_text(replay.model_dump_json(exclude_computed_fields=True))


def test_next_five_minute_bar_after_bound_expiry_remains_expired(tmp_path: Path) -> None:
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(freq="5min",
        paper={"entry_slippage_pct": 0.0002}))
    value, receipt = parameter_runtime_fixture(tmp_path / "source", parameters, delayed_next_bar=True)
    replay = run_minute_parameter_replay(value, expected=receipt, research_root=tmp_path / "actual-run")
    entry = next(signal for signal in replay.signals if signal.action is SignalAction.B_INTENT)
    queued = next(record for record in replay.queue_records if record.signal.signal_id == entry.signal_id)
    assert queued.status is PaperSignalQueueStatus.EXPIRED
    assert not replay.fills
    assert (entry.expires_at-entry.available_at).total_seconds() == 420
