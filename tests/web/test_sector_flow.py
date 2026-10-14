from pathlib import Path

import duckdb
import pandas as pd

from rquant.web import sector_flow as sf


def _row(code, t, price, amount, bid, ask, pre=10.0, name="A"):
    return dict(ts_code=code, name=name, time=t, price=price, pre_close=pre, amount=amount,
                b1_p=bid, a1_p=ask)


def test_tick_rule_and_auction(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "market.duckdb"))
    con.execute("CREATE TABLE stock_basic (ts_code VARCHAR, industry VARCHAR)")
    con.execute("INSERT INTO stock_basic VALUES ('000001.SZ','银行'), ('000002.SZ','地产')")
    con.execute("CREATE TABLE daily_basic (ts_code VARCHAR, trade_date DATE, circ_mv DOUBLE)")
    con.execute("INSERT INTO daily_basic VALUES ('000001.SZ', DATE '2026-10-13', 5e5),"
                "('000002.SZ', DATE '2026-10-13', 5e5)")
    con.close()
    q = tmp_path / "snapshots" / "2026-10-14" / "quotes"
    q.mkdir(parents=True)
    pd.DataFrame([
        _row("000001.SZ", "09:20:00", 0.0, 0.0, 10.1, 10.1),
        _row("000001.SZ", "09:25:03", 10.1, 1e8, 10.1, 10.1),   # auction, above pre -> +1e8
        _row("000002.SZ", "09:25:03", 9.9, 2e8, 9.9, 9.9),     # auction, below pre -> -2e8
    ]).to_parquet(q / "part-1.parquet")
    pd.DataFrame([
        _row("000001.SZ", "09:30:03", 10.2, 1.5e8, 10.1, 10.2),  # >= prev ask 10.1 -> buy
        _row("000001.SZ", "09:30:06", 10.1, 1.7e8, 10.1, 10.2),  # <= prev bid 10.1 -> sell
        _row("000002.SZ", "09:31:00", 9.9, 2e8, 9.8, 9.9),       # no turnover change
    ]).to_parquet(q / "part-2.parquet")
    out = sf.load_sector_flow(tmp_path, "2026-10-14", [], wait=True)
    nets = {s.name: s.net for s in out.sectors}
    assert nets["银行"] == 1e8 + 0.5e8 - 0.2e8
    assert nets["地产"] == -2e8
    assert out.t[0] == "09:25" and out.t[-1] == "09:31"
    assert out.sectors[0].series[0] == 1.0  # 亿, the auction point
    assert out.summary is not None and out.summary.amount == 1.7e8 + 2e8
    again = sf.load_sector_flow(tmp_path, "2026-10-14", [], known=out.version)
    assert again.unchanged
