"""Where the web API reads from.

Every read is one bounded SELECT against the published Serving generation
(the same immutable DuckDB snapshot the Streamlit pages read). Tests and the
e2e smoke use ``FixtureSource``: an in-memory DuckDB with the same table names.
"""

from __future__ import annotations

import contextlib
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


def _sql_quote(path: Path) -> str:
    return str(path).replace("'", "''")


def _same_file(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return False


_PRESET_TITLES: dict[str, tuple[str, str]] = {
    "pool1": ("N字初筛", "每个交易日盘后自动筛出的候选池"),
    "pool2": ("N字精选", "从初筛中持续跟踪的观察池"),
}


class ResearchSource:
    """Live read-only source over ``$RQUANT_RESEARCH_ROOT/market.duckdb`` and optional
    ``rquant_ro.duckdb`` replica. Never opens the primary writable ``rquant.duckdb``.
    """

    def __init__(
        self,
        research_root: str | Path,
        readonly_db: str | Path | None = None,
        commands_dir: str | Path | None = None,
    ) -> None:
        self.research_root = Path(research_root)
        self.readonly_db = Path(readonly_db) if readonly_db is not None else None
        self.commands_dir = (
            Path(commands_dir)
            if commands_dir is not None
            else self.research_root / "web-commands"
        )

    def _primary_db_path(self) -> Path | None:
        import os

        raw = os.environ.get("DUCKDB_PATH")
        if raw:
            return Path(raw)
        try:
            from rquant.config import get_settings

            return Path(get_settings().duckdb_path)
        except Exception:  # noqa: BLE001 - standalone env without .env
            return None

    def _resolve_readonly_db(self) -> Path | None:
        import os

        candidate = self.readonly_db
        if candidate is None:
            raw = os.environ.get("DUCKDB_READONLY_PATH")
            if raw:
                candidate = Path(raw)
            else:
                try:
                    from rquant.config import get_settings

                    candidate = Path(get_settings().duckdb_readonly_path)
                except Exception:  # noqa: BLE001
                    candidate = None
        if candidate is None or not candidate.is_file():
            return None
        primary = self._primary_db_path()
        if primary is not None and _same_file(candidate, primary):
            return None
        if _same_file(candidate, self.research_root / "market.duckdb"):
            return None
        return candidate

    def _market_db_path(self) -> Path:
        mkt = self.research_root / "market.duckdb"
        if not mkt.is_file():
            raise SourceUnavailableError(f"market.duckdb not found in {self.research_root}")
        primary = self._primary_db_path()
        if primary is not None and _same_file(mkt, primary):
            raise SourceUnavailableError("refusing to open primary writable duckdb_path")
        return mkt

    def generation(self) -> tuple[str | None, datetime | None]:
        mkt = self._market_db_path()
        try:
            probe = duckdb.connect(str(mkt), read_only=True)
            probe.close()
            mtime = mkt.stat().st_mtime
        except (duckdb.Error, OSError) as exc:
            raise SourceUnavailableError(f"{type(exc).__name__}: {exc}") from exc
        ro = self._resolve_readonly_db()
        if ro is not None:
            with contextlib.suppress(OSError):
                mtime = max(mtime, ro.stat().st_mtime)
        built_at = datetime.fromtimestamp(mtime, UTC)
        return f"research-{built_at.strftime('%Y%m%dT%H%M%SZ')}", built_at

    @staticmethod
    def _catalog_columns(con: duckdb.DuckDBPyConnection, catalog: str) -> dict[str, set[str]]:
        out: dict[str, set[str]] = {}
        rows = con.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = 'main'",
            [catalog],
        ).fetchall()
        for tbl, col in rows:
            out.setdefault(str(tbl), set()).add(str(col))
        return out

    def _open_connection(self) -> duckdb.DuckDBPyConnection:
        mkt = self._market_db_path()
        con = duckdb.connect(":memory:")
        try:
            try:
                con.execute(f"ATTACH '{_sql_quote(mkt)}' AS mkt (READ_ONLY)")
            except duckdb.Error as exc:
                raise SourceUnavailableError(f"{type(exc).__name__}: {exc}") from exc
            mkt_cols = self._catalog_columns(con, "mkt")

            ro_cols: dict[str, set[str]] = {}
            ro = self._resolve_readonly_db()
            if ro is not None:
                try:
                    con.execute(f"ATTACH '{_sql_quote(ro)}' AS ro (READ_ONLY)")
                    ro_cols = self._catalog_columns(con, "ro")
                except duckdb.Error:
                    ro_cols = {}

            self._install_projections(con, mkt_cols, ro_cols)
            return con
        except Exception:
            con.close()
            raise

    def _install_projections(
        self,
        con: duckdb.DuckDBPyConnection,
        mkt_cols: dict[str, set[str]],
        ro_cols: dict[str, set[str]],
    ) -> None:
        if "stock_basic" in mkt_cols:
            con.execute(
                "CREATE VIEW stock_basic AS SELECT ts_code, name, industry FROM mkt.stock_basic"
            )
        elif "stock_basic" in ro_cols:
            con.execute(
                "CREATE VIEW stock_basic AS SELECT ts_code, name, industry FROM ro.stock_basic"
            )
        else:
            con.execute("CREATE TABLE stock_basic(ts_code VARCHAR, name VARCHAR, industry VARCHAR)")

        has_mkt_sr = "screen_result" in mkt_cols
        has_ro_sr = "screen_result" in ro_cols
        mkt_extra = (
            "COALESCE(CAST(extra AS VARCHAR), '{}')"
            if has_mkt_sr and "extra" in mkt_cols["screen_result"]
            else "'{}'"
        )
        ro_extra = (
            "COALESCE(CAST(extra AS VARCHAR), '{}')"
            if has_ro_sr and "extra" in ro_cols["screen_result"]
            else "'{}'"
        )
        if has_mkt_sr and has_ro_sr:
            con.execute(
                "CREATE VIEW screen_result AS "
                "SELECT trade_date, ts_code, preset_name, name, close, pct_chg "
                "FROM mkt.screen_result "
                "UNION ALL "
                "SELECT s.trade_date, s.ts_code, s.preset_name, s.name, s.close, s.pct_chg "
                "FROM ro.screen_result s "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM mkt.screen_result m "
                "  WHERE m.trade_date = s.trade_date "
                "    AND m.preset_name = s.preset_name "
                "    AND m.ts_code = s.ts_code"
                ")"
            )
            ro_row_json = ro_extra.replace("extra", "s.extra")
            con.execute(
                "CREATE VIEW canvas_hit AS "
                f"SELECT trade_date, preset_name, ts_code, {mkt_extra} AS row_json "
                "FROM mkt.screen_result "
                "UNION ALL "
                f"SELECT s.trade_date, s.preset_name, s.ts_code, {ro_row_json} AS row_json "
                "FROM ro.screen_result s "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM mkt.screen_result m "
                "  WHERE m.trade_date = s.trade_date "
                "    AND m.preset_name = s.preset_name "
                "    AND m.ts_code = s.ts_code"
                ")"
            )
        elif has_mkt_sr:
            con.execute(
                "CREATE VIEW screen_result AS "
                "SELECT trade_date, ts_code, preset_name, name, close, pct_chg "
                "FROM mkt.screen_result"
            )
            con.execute(
                "CREATE VIEW canvas_hit AS "
                f"SELECT trade_date, preset_name, ts_code, {mkt_extra} AS row_json "
                "FROM mkt.screen_result"
            )
        elif has_ro_sr:
            con.execute(
                "CREATE VIEW screen_result AS "
                "SELECT trade_date, ts_code, preset_name, name, close, pct_chg "
                "FROM ro.screen_result"
            )
            con.execute(
                "CREATE VIEW canvas_hit AS "
                f"SELECT trade_date, preset_name, ts_code, {ro_extra} AS row_json "
                "FROM ro.screen_result"
            )
        else:
            con.execute(
                "CREATE TABLE screen_result(trade_date DATE, ts_code VARCHAR, "
                "preset_name VARCHAR, name VARCHAR, close DOUBLE, pct_chg DOUBLE)"
            )
            con.execute(
                "CREATE TABLE canvas_hit(trade_date DATE, preset_name VARCHAR, "
                "ts_code VARCHAR, row_json VARCHAR)"
            )

        con.execute(
            "CREATE TABLE canvas_definition(name VARCHAR, description VARCHAR, "
            "pool_refs_json VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, source VARCHAR)"
        )
        from rquant.research.screens import PRESETS

        preset_rows = con.execute(
            "SELECT preset_name, max(trade_date) FROM screen_result "
            "WHERE trade_date = (SELECT max(trade_date) FROM screen_result) "
            "GROUP BY preset_name ORDER BY preset_name"
        ).fetchall()
        for preset_name, max_day in preset_rows:
            pname = str(preset_name)
            if pname in PRESETS:
                title, desc = PRESETS[pname][0], "研究预设候选池"
            elif pname in _PRESET_TITLES:
                title, desc = _PRESET_TITLES[pname]
            else:
                title, desc = pname, "选股候选池"
            ts = (
                datetime.combine(max_day, datetime.min.time()).replace(hour=16)
                if isinstance(max_day, date)
                else datetime.now(UTC).replace(tzinfo=None)
            )
            con.execute(
                "INSERT INTO canvas_definition VALUES (?, ?, ?, ?, ?, 'research')",
                [title, desc, json.dumps([pname]), ts, ts],
            )

        if "index_daily" in mkt_cols:
            con.execute(
                "CREATE VIEW benchmark_daily AS "
                "SELECT ts_code, trade_date, close, pct_chg FROM mkt.index_daily"
            )
        elif "index_daily_bar" in ro_cols:
            con.execute(
                "CREATE VIEW benchmark_daily AS "
                "SELECT ts_code, trade_date, close, pct_chg FROM ro.index_daily_bar"
            )
        else:
            con.execute(
                "CREATE TABLE benchmark_daily(ts_code VARCHAR, trade_date DATE, "
                "close DOUBLE, pct_chg DOUBLE)"
            )

        if "monitor_event" in ro_cols:
            con.execute(
                "CREATE VIEW monitor_event AS "
                "SELECT trade_date, trigger_time, ts_code, level, trigger_price, "
                "level_price, trigger_type, pool FROM ro.monitor_event"
            )
        else:
            con.execute(
                "CREATE TABLE monitor_event(trade_date DATE, trigger_time TIMESTAMP, "
                "ts_code VARCHAR, level VARCHAR, trigger_price DOUBLE, level_price DOUBLE, "
                "trigger_type VARCHAR, pool VARCHAR)"
            )

        if "daily_bar" in mkt_cols and "stock_basic" in mkt_cols:
            con.execute(
                "CREATE VIEW market_snapshot AS "
                "SELECT CAST(b.trade_date AS TIMESTAMP) + INTERVAL 15 HOUR AS as_of, "
                "b.ts_code, s.name, b.close AS price, b.open, b.high, b.low, "
                "b.pre_close, b.pct_chg, b.vol AS volume, b.amount "
                "FROM mkt.daily_bar b LEFT JOIN mkt.stock_basic s USING (ts_code) "
                "WHERE b.trade_date = (SELECT max(trade_date) FROM mkt.daily_bar)"
            )
            if "moneyflow" in mkt_cols:
                con.execute(
                    "CREATE VIEW market_overview AS "
                    "SELECT CAST(b.trade_date AS TIMESTAMP) + INTERVAL 15 HOUR AS as_of, "
                    "'dc_industry' AS system, 'IND:' || s.industry AS board_code, "
                    "s.industry AS board_name, "
                    "SUM(b.amount) * 1000.0 AS amount, "
                    "SUM(mf.buy_lg_amount + mf.buy_elg_amount - mf.sell_lg_amount "
                    "    - mf.sell_elg_amount) * 10000.0 AS main_net_amount, "
                    "CASE WHEN SUM(b.amount) > 0 THEN "
                    "  SUM(mf.buy_lg_amount + mf.buy_elg_amount - mf.sell_lg_amount "
                    "      - mf.sell_elg_amount) * 1000.0 / SUM(b.amount) "
                    "ELSE NULL END AS main_net_rate, "
                    "median(b.pct_chg) AS pct_chg_median, "
                    "CAST(count(*) FILTER (WHERE b.pct_chg >= 9.8) AS INTEGER) AS limit_up_count, "
                    "0::INTEGER AS broken_count, "
                    "CAST(count(*) AS INTEGER) AS stock_count, "
                    "CAST(count(*) FILTER (WHERE b.pct_chg >= 9.8) * 100.0 / count(*) AS DOUBLE) "
                    "  AS limit_up_ratio_pct, "
                    "arg_max(s.name, b.pct_chg) AS leading_stock "
                    "FROM mkt.daily_bar b "
                    "JOIN mkt.stock_basic s USING (ts_code) "
                    "LEFT JOIN mkt.moneyflow mf USING (ts_code, trade_date) "
                    "WHERE b.trade_date = (SELECT max(trade_date) FROM mkt.daily_bar) "
                    "  AND s.industry IS NOT NULL AND length(trim(s.industry)) > 0 "
                    "GROUP BY b.trade_date, s.industry"
                )
            else:
                con.execute(
                    "CREATE VIEW market_overview AS "
                    "SELECT CAST(b.trade_date AS TIMESTAMP) + INTERVAL 15 HOUR AS as_of, "
                    "'dc_industry' AS system, 'IND:' || s.industry AS board_code, "
                    "s.industry AS board_name, "
                    "SUM(b.amount) * 1000.0 AS amount, "
                    "NULL::DOUBLE AS main_net_amount, NULL::DOUBLE AS main_net_rate, "
                    "median(b.pct_chg) AS pct_chg_median, "
                    "CAST(count(*) FILTER (WHERE b.pct_chg >= 9.8) AS INTEGER) AS limit_up_count, "
                    "0::INTEGER AS broken_count, "
                    "CAST(count(*) AS INTEGER) AS stock_count, "
                    "CAST(count(*) FILTER (WHERE b.pct_chg >= 9.8) * 100.0 / count(*) AS DOUBLE) "
                    "  AS limit_up_ratio_pct, "
                    "arg_max(s.name, b.pct_chg) AS leading_stock "
                    "FROM mkt.daily_bar b JOIN mkt.stock_basic s USING (ts_code) "
                    "WHERE b.trade_date = (SELECT max(trade_date) FROM mkt.daily_bar) "
                    "  AND s.industry IS NOT NULL AND length(trim(s.industry)) > 0 "
                    "GROUP BY b.trade_date, s.industry"
                )
        else:
            con.execute(
                "CREATE TABLE market_snapshot(as_of TIMESTAMP, ts_code VARCHAR, name VARCHAR, "
                "price DOUBLE, open DOUBLE, high DOUBLE, low DOUBLE, pre_close DOUBLE, "
                "pct_chg DOUBLE, volume DOUBLE, amount DOUBLE)"
            )
            con.execute(
                "CREATE TABLE market_overview(as_of TIMESTAMP, system VARCHAR, board_code VARCHAR, "
                "board_name VARCHAR, amount DOUBLE, main_net_amount DOUBLE, main_net_rate DOUBLE, "
                "pct_chg_median DOUBLE, limit_up_count INTEGER, broken_count INTEGER, "
                "stock_count INTEGER, limit_up_ratio_pct DOUBLE, leading_stock VARCHAR)"
            )

        daily_bar_latest = (
            "(SELECT max(trade_date) FROM mkt.daily_bar)"
            if "daily_bar" in mkt_cols
            else "NULL::DATE"
        )
        daily_bar_rows = (
            "(SELECT count(*) FROM mkt.daily_bar)"
            if "daily_bar" in mkt_cols
            else "0::BIGINT"
        )
        minute_bar_rows = (
            "(SELECT count(*) FROM mkt.minute_bar)"
            if "minute_bar" in mkt_cols
            else "0::BIGINT"
        )
        con.execute(
            "CREATE VIEW dashboard_summary AS "
            f"SELECT 'live' AS snapshot_key, {daily_bar_latest} AS latest_daily_bar, "
            "(SELECT max(trade_date) FROM screen_result) AS latest_screen, "
            f"{daily_bar_rows} AS daily_bar_rows, "
            "(SELECT count(*) FROM monitor_event) AS monitor_event_rows, "
            f"{minute_bar_rows} AS minute_bar_rows"
        )

        con.execute(
            "CREATE VIEW signals AS "
            "SELECT CAST(row_number() OVER (ORDER BY trade_date DESC, preset_name, ts_code) "
            "            AS BIGINT) AS global_sequence, "
            "preset_name || ':' || CAST(trade_date AS VARCHAR) || ':' || ts_code AS signal_id, "
            "preset_name AS strategy_id, ts_code AS candidate_id, 'watch' AS action, "
            "CAST(trade_date AS TIMESTAMPTZ) AS available_at, '[]' AS reason_codes_json "
            "FROM screen_result "
            "WHERE trade_date = (SELECT max(trade_date) FROM screen_result)"
        )

        con.execute(
            "CREATE TABLE runtime_services(service_id VARCHAR, plane VARCHAR, status VARCHAR, "
            "stale BOOLEAN, observed_at TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ, "
            "backlog_count INTEGER, consecutive_failures INTEGER, last_error VARCHAR)"
        )
        con.execute(
            "CREATE TABLE paper_accounts(account_id VARCHAR, as_of_time TIMESTAMPTZ, "
            "cash DECIMAL(18,2), available_cash DECIMAL(18,2), nav DECIMAL(18,2), "
            "unrealized_pnl DECIMAL(18,2), realized_pnl DECIMAL(18,2))"
        )
        con.execute(
            "CREATE TABLE paper_holdings(account_id VARCHAR, ts_code VARCHAR, "
            "quantity DECIMAL(18,2), available_quantity DECIMAL(18,2), "
            "average_cost DECIMAL(18,4), market_price DECIMAL(18,4), "
            "market_value DECIMAL(18,2), unrealized_pnl DECIMAL(18,2), as_of_time TIMESTAMPTZ)"
        )
        if "strategy_summary" in ro_cols:
            con.execute("CREATE VIEW strategy_summary AS SELECT * FROM ro.strategy_summary")
        else:
            con.execute(
                "CREATE TABLE strategy_summary(run_id VARCHAR, computed_at TIMESTAMP, "
                "start_date DATE, end_date DATE, max_hold_days INTEGER, entry_mode VARCHAR, "
                "profile_variant VARCHAR, candidates INTEGER, trades INTEGER, "
                "trigger_rate_pct DOUBLE, mean_ret_pct DOUBLE, median_ret_pct DOUBLE, "
                "win_rate_pct DOUBLE, best_ret_pct DOUBLE, worst_ret_pct DOUBLE, "
                "gap_stop_rate_pct DOUBLE)"
            )
        if "strategy_trade" in ro_cols:
            con.execute("CREATE VIEW strategy_trade AS SELECT * FROM ro.strategy_trade")
        else:
            con.execute(
                "CREATE TABLE strategy_trade(run_id VARCHAR, trade_id VARCHAR, "
                "entry_mode VARCHAR, profile_variant VARCHAR, signal_date DATE, ts_code VARCHAR, "
                "name VARCHAR, entry_time TIMESTAMP, entry_price DOUBLE, exit_time TIMESTAMP, "
                "exit_price DOUBLE, exit_reason VARCHAR, ret_pct DOUBLE)"
            )

        con.execute(
            "CREATE TABLE alert_rule(rule_id VARCHAR, title VARCHAR, enabled BOOLEAN, "
            "pools VARCHAR, levels VARCHAR, cooldown_minutes INTEGER, saved_at TIMESTAMPTZ, "
            "command_id VARCHAR)"
        )
        con.execute(
            "CREATE TABLE manual_watchlist(ts_code VARCHAR, note VARCHAR, added_at TIMESTAMPTZ, "
            "command_id VARCHAR)"
        )
        if self.commands_dir.is_dir():
            from rquant.web.commands_service import (
                read_alert_rules,
                read_watchlist_items,
            )

            rule_rows = [
                (
                    r.rule_id,
                    r.title,
                    r.enabled,
                    ",".join(r.pools),
                    ",".join(r.levels),
                    r.cooldown_minutes,
                    r.saved_at,
                    r.command_id,
                )
                for r in read_alert_rules(self.commands_dir).values()
            ]
            if rule_rows:
                con.executemany(
                    "INSERT INTO alert_rule VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rule_rows
                )
            watch_rows = [
                (w.code, w.note, w.added_at, w.command_id)
                for w in read_watchlist_items(self.commands_dir)
            ]
            if watch_rows:
                con.executemany("INSERT INTO manual_watchlist VALUES (?, ?, ?, ?)", watch_rows)

    def query(self, sql: str, params: Sequence[object] = ()) -> list[dict[str, Any]]:
        con = self._open_connection()
        try:
            cur = con.execute(sql, list(params))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
        finally:
            con.close()


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
CREATE TABLE alert_rule(rule_id VARCHAR, title VARCHAR, enabled BOOLEAN, pools VARCHAR,
  levels VARCHAR, cooldown_minutes INTEGER, saved_at TIMESTAMPTZ, command_id VARCHAR);
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
        "INSERT INTO signals VALUES (?, ?, 'breakout', ?, ?, now(), '[\"volume_surge\"]')",
        [
            (1, "sig-1", "600519.SH", "watch"),
            (2, "sig-2", "000001.SZ", "b_intent"),
            (3, "sig-3", "300750.SZ", "reduce"),
            (4, "sig-4", "601318.SH", "s_intent"),
            (5, "sig-5", "600519.SH", "cancel"),
            (6, "sig-6", f"daily-summary:{d}", "watch"),
        ],
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

    def record_rule(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Fixture stand-in for ``save_alert_rule`` + Serving republish (latest wins)."""
        rule = payload["rule"]
        self.con.execute("DELETE FROM alert_rule WHERE rule_id = ?", [rule["rule_id"]])
        self.con.execute(
            "INSERT INTO alert_rule VALUES (?, ?, ?, ?, ?, ?, now(), ?)",
            [rule["rule_id"], rule["title"], rule["enabled"], ",".join(rule["pools"]),
             ",".join(rule["levels"]), rule["cooldown_minutes"], payload["command_id"]],
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
    days = [today - timedelta(days=k) for k in range(10, 0, -1)]
    bars = {
        d: {"600519.SH": Bar(open=1500 + 5 * i, close=1505 + 5 * i, pre_close=1500 + 5 * i),
            "000001.SZ": Bar(open=10 + 0.1 * i, close=10.05 + 0.1 * i, pre_close=10 + 0.1 * i)}
        for i, d in enumerate(days)
    }
    signals = {days[0]: [PortfolioCandidate(ts_code="600519.SH"),
                         PortfolioCandidate(ts_code="000001.SZ")]}
    from rquant.backtest.strategy import StrategySpec, save_version

    config = BacktestConfig(weights=PortfolioWeightRule(max_positions=2))
    version = save_version(StrategySpec(slug="demo-breakout", title="演示突破策略",
                                        preset="breakout", config=config), root)
    save(run_backtest(days, bars, signals, config), preset="breakout", start=days[0],
         end=days[-1], root=root, title="演示组合回测", strategy=f"demo-breakout@{version.version}",
         industries={"600519.SH": "白酒", "000001.SZ": "银行"},
         pool_last=["600519.SH", "000001.SZ"])
    from rquant.research.paper import _nav_series
    from rquant.research.paper import save as save_paper
    from rquant.web.models import HoldingItem, PaperAccount

    sim = run_backtest(days, bars, signals, config)
    save_paper(root, [PaperAccount(
        account_id="sim-demo-breakout", as_of=datetime.combine(days[-1], datetime.min.time(), UTC),
        nav=sim.days[-1].nav, cash=sim.days[-1].cash, unrealized_pnl=0.0, realized_pnl=0.0,
        holdings=[HoldingItem(code=c, name=None, quantity=q, average_cost=0.0,
                              market_price=round(sim.days[-1].values.get(c, 0.0) / q, 3),
                              market_value=sim.days[-1].values.get(c, 0.0), unrealized_pnl=0.0,
                              industry={"600519.SH": "白酒", "000001.SZ": "银行"}.get(c))
                  for c, q in sim.days[-1].positions.items()],
        nav_series=_nav_series(sim.days))])
    from rquant.web.stocks import write_demo_market

    write_demo_market(root, today)
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
    from rquant.factor.store import save_latest

    save_latest(demo, {k: v.rename(columns=dict(zip(v.columns[:4], ["600519.SH", "000001.SZ",
                                                                      "300750.SZ", "601318.SH"],
                                                     strict=True)))
                       for k, v in panel.items()}, root)

    from rquant.factor.condition import save as save_condition
    from rquant.factor.condition import screen
    from rquant.factor.tdx import translate

    tdx = "C>MA(C,5) AND C>REF(C,1)"
    save_condition("演示条件：站上5日线且上涨", translate(tdx), tdx,
                   screen(panel, translate(tdx)), root=root)

    from rquant.fundamentals import FundamentalRow
    from rquant.fundamentals import save as save_fundamentals

    save_fundamentals(root, [
        FundamentalRow(code="600519.SH", end_date="20250630", ann_date="20250809",
                       values={"roe": 18.9, "grossprofit_margin": 91.3, "netprofit_margin": 52.6,
                               "netprofit_yoy": 8.9, "or_yoy": 9.2, "debt_to_assets": 15.4,
                               "eps": 36.18, "bps": 186.2}),
        FundamentalRow(code="000001.SZ", end_date="20250630", ann_date="20250822",
                       values={"roe": 5.2, "grossprofit_margin": None, "netprofit_margin": 33.1,
                               "netprofit_yoy": -3.9, "or_yoy": -10.0, "debt_to_assets": 91.2,
                               "eps": 1.18, "bps": 23.1}),
    ])
