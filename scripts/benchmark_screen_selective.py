"""Reproducible synthetic-schema RSS benchmark for the 8,000-stock screen.

Build and query are separate commands so query peak RSS excludes fixture creation.
Run with a Python environment containing the repository's locked dependencies::

    python scripts/benchmark_screen_selective.py prepare /private/tmp/screen-bench
    python scripts/benchmark_screen_selective.py query /private/tmp/screen-bench full
    python scripts/benchmark_screen_selective.py query /private/tmp/screen-bench selective
    python scripts/benchmark_screen_selective.py query /private/tmp/screen-bench full-aggregate
    python scripts/benchmark_screen_selective.py query /private/tmp/screen-bench selective-aggregate
"""

from __future__ import annotations

import json
import os
import platform
import resource
import shutil
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
os.environ["RQUANT_DISABLE_DOTENV"] = "1"

from rquant.replica_generation import (  # noqa: E402
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.screen.loader import load_universe  # noqa: E402
from rquant.screen.replica_source import VerifiedReplicaScreenSource  # noqa: E402
from rquant.screen.rules import (  # noqa: E402
    has_lower_shadow,  # noqa: E402
    has_prior_limit_up,
    volume_ratio_gte,
)
from rquant.storage.duckdb import DuckDBStore  # noqa: E402

TRADE_DATE = "2026-09-25"


def prepare(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    primary = root / "primary.duckdb"
    replica = root / "replica.duckdb"
    if primary.exists() or replica.exists():
        raise ValueError("benchmark fixture directory must be empty")
    started = time.perf_counter()
    with DuckDBStore(primary) as store:
        conn = store._conn
        conn.execute(
            """
            INSERT INTO trade_calendar
                (exchange, cal_date, is_open, source, updated_at)
            SELECT 'SSE', day::DATE, dayofweek(day) BETWEEN 1 AND 5,
                   'synthetic', CURRENT_TIMESTAMP
            FROM generate_series(DATE '2024-10-01', DATE '2026-09-25',
                                 INTERVAL '1 day') AS days(day)
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE benchmark_dates AS
            SELECT cal_date AS trade_date
            FROM trade_calendar
            WHERE exchange = 'SSE' AND is_open
            ORDER BY cal_date DESC LIMIT 91
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE benchmark_codes AS
            SELECT lpad(i::VARCHAR, 6, '0') || '.SH' AS ts_code
            FROM range(1, 8001) AS codes(i)
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE benchmark_aggregate_dates AS
            SELECT cal_date AS trade_date
            FROM trade_calendar
            WHERE exchange = 'SSE' AND is_open
            ORDER BY cal_date DESC LIMIT 500
            """
        )
        assert conn.execute("SELECT COUNT(*) FROM benchmark_dates").fetchone()[0] == 91
        assert conn.execute("SELECT COUNT(*) FROM benchmark_aggregate_dates").fetchone()[0] == 500
        conn.execute(
            """
            INSERT INTO daily_bar
                (ts_code, trade_date, open, high, low, close, pre_close,
                 pct_chg, vol, amount)
            SELECT code.ts_code, day.trade_date, 10, 11, 9, 10.5, 10,
                   5, 100, 1050
            FROM benchmark_codes AS code CROSS JOIN benchmark_dates AS day
            """
        )
        conn.execute(
            """
            INSERT INTO daily_indicator
                (ts_code, trade_date, ma5, ma10, ma20, ma60,
                 rsi6, rsi14, macd, macd_signal, macd_hist,
                 kdj_k, kdj_d, kdj_j)
            SELECT code.ts_code, day.trade_date, 10, 10, 10, 10,
                   50, 50, 0, 0, 0, 50, 50, 50
            FROM benchmark_codes AS code CROSS JOIN benchmark_dates AS day
            """
        )
        conn.execute(
            """
            INSERT INTO daily_state
                (ts_code, trade_date, is_st, is_bj, board_type,
                 is_limit_up, is_limit_down, is_first_limit_up,
                 is_yiziban, consecutive_limit_ups, body_upper, body_lower)
            SELECT code.ts_code, day.trade_date, FALSE, FALSE, 'main',
                   FALSE, FALSE, FALSE, FALSE, 0, 10.5, 10
            FROM benchmark_codes AS code CROSS JOIN benchmark_aggregate_dates AS day
            """
        )
        conn.execute(
            """
            INSERT INTO daily_basic
                (ts_code, trade_date, turnover_rate, volume_ratio,
                 total_mv, circ_mv)
            SELECT code.ts_code, day.trade_date, 2, 1, 100000, 80000
            FROM benchmark_codes AS code CROSS JOIN benchmark_dates AS day
            """
        )
        conn.execute(
            """
            INSERT INTO stock_status_daily
                (ts_code, trade_date, name, is_st, name_source, st_source,
                 available_at, ingested_at)
            SELECT code.ts_code, day.trade_date, '合成股票', FALSE,
                   'synthetic', 'synthetic',
                   day.trade_date::TIMESTAMPTZ, CURRENT_TIMESTAMP
            FROM benchmark_codes AS code CROSS JOIN benchmark_aggregate_dates AS day
            """
        )
        conn.execute("CHECKPOINT")
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    print(json.dumps({"phase": "prepare", "seconds": round(time.perf_counter() - started, 3),
                      "stocks": 8000, "wide_days": 91, "aggregate_days": 500,
                      "schema": "DuckDBStore/formal"}))


def query(root: Path, mode: str) -> None:
    primary = root / "primary.duckdb"
    replica = root / "replica.duckdb"
    rule = volume_ratio_gte(2, offset=30, window=60)
    rules = [rule]
    include_columns: list[str] = []
    started = time.perf_counter()
    if mode in {"full", "full-aggregate"}:
        aggregate_requests = (
            has_prior_limit_up(window=500).aggregate_requests
            if mode == "full-aggregate" else None
        )
        with DuckDBStore(replica, read_only=True) as store:
            frame = load_universe(
                TRADE_DATE, lookback=90, store=store,
                aggregate_requests=aggregate_requests,
            )
    elif mode in {"selective", "selective-aggregate", "bounded-max", "bounded-max-aggregate"}:
        if mode.startswith("bounded-max"):
            rules = [volume_ratio_gte(2, offset=0, window=60), rule]
            rules.extend(has_lower_shadow(offset=offset) for offset in range(6))
            include_columns = ["CIRC_MV[0]", "TURNOVER_RATE[0]"]
        if mode.endswith("aggregate"):
            rules.append(has_prior_limit_up(window=500))
        frame = VerifiedReplicaScreenSource(
            primary_path=primary, replica_path=replica
        ).load(
            date.fromisoformat(TRADE_DATE), rules, include_columns=include_columns
        ).frame
    else:
        raise ValueError("unsupported benchmark mode")
    elapsed = time.perf_counter() - started
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_bytes = rss if platform.system() == "Darwin" else rss * 1024
    measurement = {
        "phase": "query", "mode": mode, "seconds": round(elapsed, 3),
        "peak_rss_mib": round(rss_bytes / 1048576, 1),
        "stocks": len(frame), "columns": len(frame.columns),
        "matched": int(rule(frame).fillna(False).sum()),
    }
    if mode.endswith("aggregate"):
        values = frame["count_limit_up_500d_ex1"]
        measurement["aggregate_known"] = int(values.notna().sum())
        measurement["aggregate_positive"] = int(values.fillna(0).gt(0).sum())
    print(json.dumps(measurement))


def main() -> None:
    if len(sys.argv) not in {3, 4} or sys.argv[1] not in {"prepare", "query"}:
        raise SystemExit("usage: benchmark_screen_selective.py prepare|query ROOT [full|selective]")
    root = Path(sys.argv[2]).resolve()
    if sys.argv[1] == "prepare" and len(sys.argv) == 3:
        prepare(root)
    elif sys.argv[1] == "query" and len(sys.argv) == 4:
        query(root, sys.argv[3])
    else:
        raise SystemExit("usage: benchmark_screen_selective.py prepare|query ROOT [full|selective]")


if __name__ == "__main__":
    main()
