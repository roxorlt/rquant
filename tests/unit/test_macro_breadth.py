import numpy as np
import pandas as pd

from rquant.macro import breadth as bv


def test_variants_evaluate_select_one_per_group() -> None:
    rng = np.random.default_rng(0)
    days = [d.strftime("%Y%m%d") for d in pd.bdate_range("2023-01-02", "2025-06-30")]
    rows = []
    for k in range(120):
        r = rng.normal(0, 0.02, len(days))
        px = 10 * np.exp(np.cumsum(r))
        rows.append(
            pd.DataFrame(
                {"ts_code": f"{600000 + k}.SH", "trade_date": days, "close": px, "pct_chg": r * 100}
            )
        )
    bars = pd.concat(rows)
    panel, groups = bv.variants(bars)
    assert set(groups.values()) == {"累计涨跌", "涨跌比率", "均线参与度", "新高新低"}
    close = pd.Series(np.exp(np.cumsum(rng.normal(0, 0.01, len(days)))), index=days)
    scores, events = bv.evaluate(panel, groups, close)
    assert {"train", "test"} <= set(scores.split)
    sel = bv.select(scores)
    assert sorted(sel.group) == sorted(set(groups.values()))
    assert set(events.split) <= {"train", "test"}
