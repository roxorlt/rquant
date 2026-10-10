"""Loader + file store round trip on a tiny replica."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from rquant.backtest import BacktestConfig
from rquant.backtest.store import list_runs, read_run, run_and_save
from rquant.portfolio import PortfolioWeightRule


def test_run_from_replica_and_read_back(tmp_path: Path) -> None:
    db = tmp_path / "ro.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE screen_result(trade_date DATE, preset_name VARCHAR, "
                "ts_code VARCHAR, extra VARCHAR)")
    con.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, open DOUBLE, "
                "close DOUBLE, pre_close DOUBLE)")
    con.execute("CREATE TABLE daily_state(ts_code VARCHAR, trade_date DATE, limit_pct DOUBLE)")
    con.execute("INSERT INTO screen_result VALUES ('2026-01-05', 'p', '600000.SH', "
                "'{\"score\": 3}')")
    con.execute("INSERT INTO daily_bar VALUES ('600000.SH', '2026-01-05', 10, 10, 10), "
                "('600000.SH', '2026-01-06', 10, 10.5, 10), "
                "('600000.SH', '2026-01-07', 10.5, 11, 10.5)")
    con.close()

    config = BacktestConfig(capital=100_000, weights=PortfolioWeightRule(max_positions=5))
    run = run_and_save(db, "p", date(2026, 1, 5), date(2026, 1, 7), config, root=tmp_path)

    assert [o.status for o in run.result.orders] == ["filled"]
    assert run.result.days[-1].nav > 1
    assert [r.run_id for r in list_runs(tmp_path)] == [run.run_id]
    assert read_run(run.run_id, tmp_path) == run
    assert read_run("../x", tmp_path) is None


def test_cli_runs_a_saved_strategy_version(tmp_path, monkeypatch) -> None:
    from rquant.backtest.store import _main

    test_run_from_replica_and_read_back(tmp_path)  # builds ro.duckdb
    monkeypatch.setenv("RQUANT_RESEARCH_ROOT", str(tmp_path / "r"))
    assert _main(["strategy", "save", "--slug", "s1", "--title", "T", "--preset", "p",
                  "--max-positions", "3"]) == 0
    assert _main(["--db", str(tmp_path / "ro.duckdb"), "--strategy", "s1",
                  "--start", "2026-01-05", "--end", "2026-01-07"]) == 0
    (run,) = list_runs(tmp_path / "r")
    assert run.strategy == "s1@1" and run.result.config.weights.max_positions == 3
