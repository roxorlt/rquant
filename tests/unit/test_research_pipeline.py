from datetime import date

import numpy as np
import pandas as pd

from rquant.backtest.store import list_runs
from rquant.backtest.strategy import list_strategies
from rquant.factor.store import list_factors
from rquant.research import daily, ingest, screens
from rquant.research.tdx_strategy import TDX_STRATEGIES


def _seed(root, days=420, n=40):
    con = ingest.connect(root)
    idx = pd.bdate_range("2024-01-01", periods=days)
    rng = np.random.default_rng(1)
    close = 10 * np.exp(np.cumsum(rng.normal(0, 0.02, (days, n)), axis=0))
    codes = [f"{600000 + i}.SH" for i in range(n)]
    rows = []
    for t, d in enumerate(idx):
        for j, c in enumerate(codes):
            pre = close[t - 1, j] if t else close[t, j]
            rows.append((c, d.date(), pre, close[t, j] * 1.01, close[t, j] * 0.99, close[t, j], pre,
                         (close[t, j] / pre - 1) * 100, 1e5, 1e6 * (j + 1)))
    bar = pd.DataFrame(rows, columns=["ts_code", "trade_date", "open", "high", "low", "close",
                                      "pre_close", "pct_chg", "vol", "amount"])
    ingest._upsert(con, "daily_bar", bar)
    ingest._upsert(con, "adj_factor", bar[["ts_code", "trade_date"]].assign(adj_factor=1.0))
    ingest._upsert(con, "daily_basic", bar[["ts_code", "trade_date"]].assign(
        turnover_rate=1.0, pe_ttm=10.0, pb=bar["close"] / 10, total_mv=1e6, circ_mv=bar["amount"]))
    ingest._upsert(con, "daily_state", bar[["ts_code", "trade_date"]].assign(limit_pct=0.1))
    ingest._upsert(con, "stock_basic", pd.DataFrame({
        "ts_code": codes, "name": [f"股{i}" for i in range(n)], "industry": "测试",
        "market": "主板", "list_date": date(2020, 1, 1)}))
    ingest._upsert(con, "trade_calendar", pd.DataFrame({"cal_date": idx.date, "is_open": True}))
    con.close()
    return idx


def test_screens_and_daily_job_populate_research_root(tmp_path, monkeypatch):
    monkeypatch.setenv("RQUANT_RESEARCH_ROOT", str(tmp_path))
    idx = _seed(tmp_path)
    con = ingest.connect(tmp_path)
    assert screens.build(con, idx[150].date(), idx[-1].date()) > 0
    assert ingest.missing_days(con, "daily_bar", idx[0].date(), idx[-1].date()) == []
    con.close()
    daily.run(ingest=False)
    assert len(list_runs(tmp_path)) == len(daily.STRATEGIES) + len(TDX_STRATEGIES)
    assert "mom20-top10" in {getattr(s, "slug", s) for s in list_strategies(tmp_path)}
    assert len(list_factors(tmp_path)) == len(daily.FACTORS)
    assert (tmp_path / "data_audit" / "latest.json").exists()
    daily.run(ingest=False)  # idempotent
    assert len(list_factors(tmp_path)) == len(daily.FACTORS)
    from rquant.factor.condition import list_conditions
    from rquant.research import paper

    assert len(list_conditions(tmp_path)) == len(daily.CONDITIONS)
    assert {a.account_id for a in paper.read(tmp_path)} >= {"sim-mom20-top10"}
    from rquant.data_catalog.audit import read_report

    ids = {d.dataset_id for d in read_report(tmp_path).datasets}
    assert {"daily_bar", "daily_basic", "index_daily"} <= ids
    for _ in range(6):
        daily.prune_backtests(tmp_path, keep=1)
    assert len(list_runs(tmp_path)) == len(daily.STRATEGIES) + len(TDX_STRATEGIES)


def test_screen_api_falls_back_to_research_presets(tmp_path):
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource

    idx = _seed(tmp_path)
    con = ingest.connect(tmp_path)
    screens.build(con, idx[150].date(), idx[-1].date())
    con.close()
    src = FixtureSource()
    src.con.execute("DELETE FROM screen_result")
    body = TestClient(create_app(src, research_root=tmp_path)).get("/api/v1/screen").json()
    assert any(p.startswith("research_") for p in body["data"]["presets"])


def test_tdx_cross_translates() -> None:
    from rquant.factor.tdx import translate
    from rquant.research.tdx_strategy import HJK

    expr = translate(HJK)
    assert "delay(ts_mean(close, 5), 1) <= delay(ts_mean(close, 10), 1)" in expr


def test_moneyflow_ingest_and_float_turnover_backfill(tmp_path) -> None:
    from datetime import date

    con = ingest.connect(tmp_path)
    d = date(2026, 1, 5)
    ingest._upsert(con, "trade_calendar", pd.DataFrame({"cal_date": [d], "is_open": [True]}))
    ingest._upsert(con, "daily_basic", pd.DataFrame({"ts_code": ["600000.SH"], "trade_date": [d],
                                                     "turnover_rate": [1.0]}))

    class FakePro:
        def __call__(self, api, **kw):
            if api == "moneyflow":
                row = {c: 1.0 for c in ingest.MF_COLS}
                return pd.DataFrame([{**row, "ts_code": "600000.SH", "trade_date": "20260105"}])
            if api == "daily_basic":
                return pd.DataFrame({"ts_code": ["600000.SH"], "turnover_rate_f": [2.5]})
            raise AssertionError(api)

    assert ingest.ingest_moneyflow(con, FakePro(), d, d) == 1
    assert ingest.ingest_moneyflow(con, FakePro(), d, d) == 0  # nothing missing now
    assert ingest.backfill_float_turnover(con, FakePro(), d, d) == 1
    assert con.execute("SELECT turnover_rate_f FROM daily_basic").fetchone()[0] == 2.5
