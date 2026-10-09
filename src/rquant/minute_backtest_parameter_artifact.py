"""Complete parameter minute results through the original sealed Lab reader."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from rquant.minute_backtest_artifact import MinuteSealedReplayReader, MinuteSealedReplayResult
from rquant.minute_backtest_parameter_adapter import (
    MinuteParameterFormalParameters,
    MinuteParameterFormalReplayAdapter,
    MinuteParameterFormalReplayResult,
)
from rquant.minute_backtest_parameter_producer import MinuteParameterReplayCatalog
from rquant.minute_backtest_parameter_runner import (
    MinuteParameterReplayResult,
    minute_parameter_result_tables,
)
from rquant.strategy_job_adapters import StrategyJobAdapterRegistry


class MinuteParameterSealedReplayResult(MinuteSealedReplayResult):
    kind: Literal["minute_parameter_replay"] = "minute_parameter_replay"
    result: MinuteParameterFormalReplayResult


class MinuteParameterSealedReplayReader(MinuteSealedReplayReader):
    catalog: MinuteParameterReplayCatalog

    @staticmethod
    def _catalog_model() -> type[MinuteParameterReplayCatalog]:
        return MinuteParameterReplayCatalog

    @staticmethod
    def _parameter_model() -> type[MinuteParameterFormalParameters]:
        return MinuteParameterFormalParameters

    @staticmethod
    def _formal_result_model() -> type[MinuteParameterFormalReplayResult]:
        return MinuteParameterFormalReplayResult

    @staticmethod
    def _sealed_result_model() -> type[MinuteParameterSealedReplayResult]:
        return MinuteParameterSealedReplayResult

    @staticmethod
    def _result_tables(replay: MinuteParameterReplayResult) -> dict[str, pd.DataFrame]:
        return minute_parameter_result_tables(replay)

    def _adapter(self) -> MinuteParameterFormalReplayAdapter:
        return MinuteParameterFormalReplayAdapter(self.catalog)

    def _registry(self) -> StrategyJobAdapterRegistry:
        return StrategyJobAdapterRegistry((self._adapter(),))
