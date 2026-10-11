import numpy as np
import pandas as pd

from rquant.factor.chips import chip_fields
from rquant.factor.expr import validate


def _panel(n_days: int = 120) -> dict[str, pd.DataFrame]:
    idx = pd.bdate_range("2025-01-01", periods=n_days)
    rng = np.random.default_rng(1)
    up = 10 * np.cumprod(1 + 0.004 + rng.normal(0, 0.01, n_days))  # steady riser
    down = 10 * np.cumprod(1 - 0.004 + rng.normal(0, 0.01, n_days))  # steady faller
    close = pd.DataFrame({"UP": up, "DN": down}, index=idx)
    return {"close": close, "high": close * 1.01, "low": close * 0.99,
            "turnover": pd.DataFrame(3.0, index=idx, columns=close.columns)}


def test_chip_fields_order_winners_and_losers() -> None:
    f = chip_fields(_panel())
    last = {k: v.iloc[-1] for k, v in f.items()}
    assert last["chip_profit"]["UP"] > 0.8 > 0.2 > last["chip_profit"]["DN"]
    assert last["chip_cost_dev"]["UP"] > 0 > last["chip_cost_dev"]["DN"]
    assert 0 < last["chip_conc90"]["UP"] < 0.5


def test_chip_fields_validate_in_expressions() -> None:
    validate("cs_rank(chip_profit) + cs_rank(0 - chip_conc90)")
