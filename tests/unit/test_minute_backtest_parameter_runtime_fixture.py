"""Explicit synthetic publications retain the selected family's actual security."""

from __future__ import annotations

import base64
import io
from pathlib import Path

import pandas as pd
import pytest

from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
from rquant.minute_backtest_parameters import MinuteGrowthParameters, MinuteNShapeParameters, MinuteParameterSet
from tests.support.minute_parameter_runtime_fixture import parameter_runtime_fixture


@pytest.mark.parametrize("family,frequency,code", [
    ("growth_board_surge", "5min", "300001.SZ"),
    ("growth_board_surge", "60min", "300001.SZ"),
    ("n_shape", "60min", "600000.SH"),
])
def test_complete_synthetic_frequency_matches_candidate_warmup_and_market(
    tmp_path: Path, family: str, frequency: str, code: str,
) -> None:
    recipe = MinuteParameterSet(parameters=(MinuteGrowthParameters(freq=frequency)
        if family == "growth_board_surge" else MinuteNShapeParameters(freq=frequency)))
    with minute_parameter_validation_scope():
        frozen, receipt = parameter_runtime_fixture(tmp_path / "explicit-synthetic", recipe)
        assert receipt.frozen == frozen
        assert {fact.ts_code for fact in frozen.session_facts} == {code}
        warmup, = (item for item in frozen.materials if item.relative_path == "warmup.parquet")
        history = pd.read_parquet(io.BytesIO(base64.b64decode(warmup.content_base64)))
        assert set(history.ts_code) == {code}
        spool = LiveBatchSpool(tmp_path / "explicit-synthetic/physical-frequency-synthetic-market", read_only=True)
        records = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
        assert records and len(records) == frozen.parameter_work.runtime_work.market_batches
        for record in records:
            frame = MarketMinuteGateway.decode_payload(spool.read_payload(record))
            assert set(frame.ts_code) == {code}
