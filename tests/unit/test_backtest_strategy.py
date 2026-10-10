from __future__ import annotations

import pytest

from rquant.backtest import BacktestConfig
from rquant.backtest.strategy import StrategySpec, list_strategies, resolve, save_version
from rquant.portfolio import PortfolioWeightRule


def test_versions_are_immutable_and_deduplicated(tmp_path) -> None:
    v1 = save_version(StrategySpec(slug="breakout", title="突破", preset="breakout"), tmp_path)
    same = save_version(StrategySpec(slug="breakout", title="改名不算新版本",
                                     preset="breakout"), tmp_path)
    v2 = save_version(StrategySpec(
        slug="breakout", title="突破", preset="breakout",
        config=BacktestConfig(weights=PortfolioWeightRule(max_positions=5))), tmp_path)
    assert (v1.version, same.version, v2.version) == (1, 1, 2)
    assert resolve("breakout", tmp_path).version == 2
    assert resolve("breakout@1", tmp_path).spec.config.weights.max_positions == 10
    assert list(list_strategies(tmp_path)) == ["breakout"]
    with pytest.raises(LookupError):
        resolve("breakout@9", tmp_path)
    with pytest.raises(ValueError):
        StrategySpec(slug="../x", title="t", preset="p")
