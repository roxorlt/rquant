from pathlib import Path

import pandas as pd

from rquant.web.pool_live import load_pool_live


def test_pool_live_pct_and_spark(tmp_path: Path) -> None:
    q = tmp_path / "snapshots" / "2026-10-12" / "quotes"
    q.mkdir(parents=True)
    for i, (t, p) in enumerate([("09:25:00", 10.2), ("09:30:03", 10.5), ("09:30:40", 10.6),
                                ("09:31:10", 11.0)]):
        pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "time": [t, t], "price": [p, 5.0],
                      "pre_close": [10.0, 5.0]}).to_parquet(q / f"part-{i:03d}.parquet")
    d = load_pool_live(tmp_path, "2026-10-12", ["000001.SZ", "000003.SZ"])
    a, b = d.rows
    assert a.pct == 10.0 and a.time == "09:31:10"
    assert a.spark == [10.6, 11.0] and a.spark_t == ["09:30", "09:31"]
    assert b.pct is None and b.spark == []
