"""Where the web API reads from.

Every read is one bounded SELECT against the published Serving generation
(the same immutable DuckDB snapshot the Streamlit pages read). Tests and the
e2e smoke use ``FixtureSource``: an in-memory DuckDB with the same table names.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import duckdb


class SourceUnavailableError(RuntimeError):
    """The Serving generation cannot be read right now."""


class Source(Protocol):
    def generation(self) -> tuple[str | None, datetime | None]: ...

    def query(self, sql: str, params: Sequence[object] = ()) -> list[dict[str, Any]]: ...


class ServingSource:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _open(self):  # noqa: ANN202 - main's context type
        from rquant.dashboard.serving_only_page_data import ServingOnlyRenderContext

        try:
            return ServingOnlyRenderContext.open(self.root, stale_after=timedelta(days=3650))
        except Exception as exc:  # noqa: BLE001 - surfaced as 503
            raise SourceUnavailableError(f"{type(exc).__name__}: {exc}") from exc

    def generation(self) -> tuple[str | None, datetime | None]:
        with self._open() as ctx:
            return ctx.generation_id, ctx._lease.manifest.built_at  # noqa: SLF001

    def query(self, sql: str, params: Sequence[object] = ()) -> list[dict[str, Any]]:
        from rquant.dashboard.serving_only_page_data import ServingFrameState

        with self._open() as ctx:
            result = ctx.query(sql, params)
        if result.state is ServingFrameState.UNAVAILABLE:
            raise SourceUnavailableError(result.detail)
        return [dict(zip(result.columns, row, strict=True)) for row in result.rows]


_FIXTURE_DDL = """
CREATE TABLE stock_basic(ts_code VARCHAR, name VARCHAR, industry VARCHAR);
CREATE TABLE screen_result(trade_date DATE, ts_code VARCHAR, preset_name VARCHAR,
  name VARCHAR, close DOUBLE, pct_chg DOUBLE);
CREATE TABLE canvas_definition(name VARCHAR, description VARCHAR, pool_refs_json VARCHAR,
  created_at TIMESTAMP, updated_at TIMESTAMP, source VARCHAR);
CREATE TABLE canvas_hit(trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, row_json VARCHAR);
CREATE TABLE monitor_event(trade_date DATE, trigger_time TIMESTAMP, ts_code VARCHAR,
  level VARCHAR, trigger_price DOUBLE, level_price DOUBLE, trigger_type VARCHAR, pool VARCHAR);
CREATE TABLE strategy_summary(run_id VARCHAR, computed_at TIMESTAMP, start_date DATE,
  end_date DATE, max_hold_days INTEGER, entry_mode VARCHAR, profile_variant VARCHAR,
  candidates INTEGER, trades INTEGER, trigger_rate_pct DOUBLE, mean_ret_pct DOUBLE,
  median_ret_pct DOUBLE, win_rate_pct DOUBLE, best_ret_pct DOUBLE, worst_ret_pct DOUBLE,
  gap_stop_rate_pct DOUBLE);
CREATE TABLE benchmark_daily(ts_code VARCHAR, trade_date DATE, close DOUBLE,
  pct_chg DOUBLE);
CREATE TABLE strategy_trade(run_id VARCHAR, trade_id VARCHAR, entry_mode VARCHAR,
  profile_variant VARCHAR, signal_date DATE, ts_code VARCHAR, name VARCHAR,
  entry_time TIMESTAMP, entry_price DOUBLE, exit_time TIMESTAMP, exit_price DOUBLE,
  exit_reason VARCHAR, ret_pct DOUBLE);
CREATE TABLE market_overview(as_of TIMESTAMP, system VARCHAR, board_code VARCHAR,
  board_name VARCHAR, amount DOUBLE, main_net_amount DOUBLE, main_net_rate DOUBLE,
  pct_chg_median DOUBLE, limit_up_count INTEGER, broken_count INTEGER, stock_count INTEGER,
  limit_up_ratio_pct DOUBLE, leading_stock VARCHAR);
CREATE TABLE market_snapshot(as_of TIMESTAMP, ts_code VARCHAR, name VARCHAR, price DOUBLE,
  open DOUBLE, high DOUBLE, low DOUBLE, pre_close DOUBLE, pct_chg DOUBLE, volume DOUBLE,
  amount DOUBLE);
CREATE TABLE runtime_services(service_id VARCHAR, plane VARCHAR, status VARCHAR,
  stale BOOLEAN, observed_at TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ, backlog_count INTEGER,
  consecutive_failures INTEGER, last_error VARCHAR);
CREATE TABLE dashboard_summary(snapshot_key VARCHAR, latest_daily_bar DATE,
  latest_screen DATE, daily_bar_rows BIGINT, monitor_event_rows BIGINT, minute_bar_rows BIGINT);
CREATE TABLE signals(global_sequence BIGINT, signal_id VARCHAR, strategy_id VARCHAR,
  candidate_id VARCHAR, action VARCHAR, available_at TIMESTAMPTZ, reason_codes_json VARCHAR);
CREATE TABLE paper_accounts(account_id VARCHAR, as_of_time TIMESTAMPTZ, cash DECIMAL(18,2),
  available_cash DECIMAL(18,2), nav DECIMAL(18,2), unrealized_pnl DECIMAL(18,2),
  realized_pnl DECIMAL(18,2));
CREATE TABLE alert_ack(alert_id VARCHAR, acked_at TIMESTAMPTZ, actor_id VARCHAR,
  command_id VARCHAR);
CREATE TABLE paper_holdings(account_id VARCHAR, ts_code VARCHAR, quantity DECIMAL(18,2),
  available_quantity DECIMAL(18,2), average_cost DECIMAL(18,4), market_price DECIMAL(18,4),
  market_value DECIMAL(18,2), unrealized_pnl DECIMAL(18,2), as_of_time TIMESTAMPTZ);
"""


def _seed(con: duckdb.DuckDBPyConnection, today: date) -> None:
    d = today.isoformat()
    snap = datetime.combine(today, datetime.min.time()).replace(hour=14, minute=30)
    ins = con.execute
    stocks = [("600519.SH", "贵州茅台", "白酒"), ("000001.SZ", "平安银行", "银行"),
              ("300750.SZ", "宁德时代", "电池"), ("601318.SH", "中国平安", "保险")]
    con.executemany("INSERT INTO stock_basic VALUES (?,?,?)", stocks)
    con.executemany(
        "INSERT INTO screen_result VALUES (?,?,?,?,?,?)",
        [(d, c, "breakout", n, 10.0 + i, 1.5 * (i - 1)) for i, (c, n, _) in enumerate(stocks)],
    )
    ins("INSERT INTO canvas_definition VALUES ('核心观察', '演示池', '[\"breakout\"]', "
        "now(), now(), 'fixture')")
    con.executemany(
        "INSERT INTO canvas_hit VALUES (?,?,?,?)",
        [(d, "breakout", c, json.dumps({"score": 90 - i})) for i, (c, _, _) in enumerate(stocks)],
    )
    con.executemany(
        "INSERT INTO monitor_event VALUES (?, ?::TIMESTAMP, ?, ?, ?, ?, ?, ?)",
        [(d, f"{d} 09:3{i}:00", c, "L1", 10.0 + i, 9.8 + i, "breakout", "pool2")
         for i, (c, _, _) in enumerate(stocks[:3])],
    )
    ins("INSERT INTO strategy_summary VALUES ('run-demo', now(), DATE '2026-01-05', ?, 5, "
        "'open', 'base', 120, 48, 40.0, 1.2, 0.8, 54.0, 9.5, -6.1, 3.0)", [d])
    con.executemany(
        "INSERT INTO strategy_trade VALUES ('run-demo', ?, 'open', 'base', ?, ?, ?, "
        "?::TIMESTAMP, 10, ?::TIMESTAMP, ?, 'take_profit', ?)",
        [(f"t{i}", d, c, n, f"{d} 09:35:00", f"{today - timedelta(days=3 - i)} 14:55:00",
          10 + r / 10, r)
         for i, ((c, n, _), r) in enumerate(zip(stocks, [3.0, -1.5, 2.2, 0.4], strict=True))],
    )
    con.executemany(
        "INSERT INTO benchmark_daily VALUES ('000300.SH', ?, ?, NULL)",
        [(today - timedelta(days=k), 4000 + 10 * (5 - k)) for k in range(5, -1, -1)],
    )
    con.executemany(
        "INSERT INTO market_overview VALUES (?, 'dc_industry', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(snap, "BK01", "白酒", 2.1e10, 3e8, 1.4, 1.2, 3, 1, 20, 15.0, "贵州茅台"),
         (snap, "BK02", "银行", 1.5e10, -1e8, -0.6, -0.3, 0, 0, 42, 0.0, "平安银行")],
    )
    con.executemany(
        "INSERT INTO market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(snap, c, n, 10.0 + i, 10, 11 + i, 9, 10, 1.5 * (i - 1), 1e6, 1e8)
         for i, (c, n, _) in enumerate(stocks)],
    )
    con.executemany(
        "INSERT INTO runtime_services VALUES (?, ?, ?, false, now(), now(), 0, 0, NULL)",
        [("rquant-monitor", "live", "running"), ("rquant-daily", "batch", "idle"),
         ("rquant-notifier", "live", "running")],
    )
    ins("INSERT INTO dashboard_summary VALUES ('latest', ?, ?, 1000000, 120, 5000000)", [d, d])
    con.executemany(
        "INSERT INTO signals VALUES (?, ?, 'breakout', ?, 'buy', now(), '[\"volume_surge\"]')",
        [(i + 1, f"sig-{i}", c) for i, (c, _, _) in enumerate(stocks[:2])],
    )
    ins("INSERT INTO paper_accounts VALUES ('paper-main', now(), 50000, 50000, 110000, 2500, 800)")
    ins("INSERT INTO paper_holdings VALUES ('paper-main', '600519.SH', 100, 100, 1500, 1525, "
        "152500, 2500, now())")


def table_missing(exc: Exception) -> bool:
    """A projection that was never published (e.g. no ack yet) reads as empty."""
    text = str(exc).lower()
    return "does not exist" in text or "not published" in text or "catalog error" in text


class FixtureSource:
    """Small invented dataset with Serving's table names. Never production data."""

    def __init__(self, today: date | None = None) -> None:
        self.con = duckdb.connect(":memory:")
        self.con.execute(_FIXTURE_DDL)
        _seed(self.con, today or datetime.now(UTC).date())
        self.built_at = datetime.now(UTC)

    def record_ack(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Fixture stand-in for page control + Serving republish of ``ack_alert``."""
        self.con.execute(
            "INSERT INTO alert_ack VALUES (?, now(), ?, ?)",
            [payload["alert_id"], payload["actor_id"], payload["command_id"]],
        )
        return {"command_id": payload["command_id"], "status": "succeeded"}

    def generation(self) -> tuple[str | None, datetime | None]:
        return "fixture", self.built_at

    def query(self, sql: str, params: Sequence[object] = ()) -> list[dict[str, Any]]:
        cur = self.con.cursor().execute(sql, list(params))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def write_demo_research(root: Path) -> None:
    """A tiny portfolio backtest result for fixture mode / e2e (three days, two stocks)."""
    from rquant.backtest import BacktestConfig, Bar, run_backtest
    from rquant.backtest.store import save
    from rquant.portfolio import PortfolioCandidate, PortfolioWeightRule

    today = date.today()
    days = [today - timedelta(days=k) for k in (4, 3, 2, 1)]
    bars = {
        d: {"600519.SH": Bar(open=1500 + 5 * i, close=1505 + 5 * i, pre_close=1500 + 5 * i),
            "000001.SZ": Bar(open=10 + 0.1 * i, close=10.05 + 0.1 * i, pre_close=10 + 0.1 * i)}
        for i, d in enumerate(days)
    }
    signals = {days[0]: [PortfolioCandidate(ts_code="600519.SH"),
                         PortfolioCandidate(ts_code="000001.SZ")]}
    config = BacktestConfig(weights=PortfolioWeightRule(max_positions=2))
    save(run_backtest(days, bars, signals, config), preset="breakout", start=days[0],
         end=days[-1], root=root, title="演示组合回测",
         industries={"600519.SH": "白酒", "000001.SZ": "银行"},
         pool_last=["600519.SH", "000001.SZ"])
    from rquant.data_catalog.audit import AuditReport, DatasetCoverage, write_report

    write_report(AuditReport(
        generated_at=datetime.now(UTC), window_start=days[0], window_end=days[-1], open_days=4,
        datasets=[DatasetCoverage(dataset_id="daily_bar", table_name="daily_bar",
                                  latest_date=days[-2], rows_in_window=15000,
                                  missing_open_days=[days[-1]], required_on_open_day=True)],
    ), root)

    import numpy as np
    import pandas as pd

    from rquant.factor.store import run_factor
    from rquant.factor.store import save as save_factor

    rng = np.random.default_rng(7)
    idx = pd.bdate_range(end=pd.Timestamp(today), periods=90)
    close = pd.DataFrame(10 * np.exp(np.cumsum(rng.normal(0, 0.02, (len(idx), 40)), axis=0)),
                         index=idx, columns=[f"{600000 + i}.SH" for i in range(40)])
    panel = {f: close for f in ("open", "high", "low", "close", "pre_close")}
    panel |= {"pct_chg": close.pct_change() * 100, "vol": close * 0 + 1, "amount": close * 0 + 1}
    expr = "cs_rank(ts_delta(close, 5))"
    start = idx[30].date()
    demo = save_factor("演示动量因子", expr, start, today,
                       run_factor(panel, expr, start, today), root=root)
    from rquant.factor.store import save_tracking, track

    save_tracking(track(demo, panel, today, lookback_days=60), root)
