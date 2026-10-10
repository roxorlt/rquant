"""Coverage audit over the read replica (roadmap module 7).

For each catalog dataset with an event date: latest date, rows in the window,
and open trading days in the window with no rows. Written to
``$RQUANT_RESEARCH_ROOT/data_audit/latest.json``; the web API only reads it.

    python -m rquant.data_catalog.audit [--db PATH] [--days 30]
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
from pydantic import BaseModel

from rquant.backtest.store import research_root
from rquant.data_catalog.build import CATALOG_CONTRACTS


class DatasetCoverage(BaseModel):
    dataset_id: str
    table_name: str
    latest_date: date | None
    rows_in_window: int
    missing_open_days: list[date]
    required_on_open_day: bool
    error: str | None = None


class AuditReport(BaseModel):
    generated_at: datetime
    window_start: date
    window_end: date
    open_days: int
    datasets: list[DatasetCoverage]


def audit(con: duckdb.DuckDBPyConnection, end: date, days: int = 30) -> AuditReport:
    start = end - timedelta(days=days)
    open_days = [r[0] for r in con.execute(
        "SELECT DISTINCT cal_date FROM trade_calendar WHERE is_open AND cal_date BETWEEN ? AND ? "
        "ORDER BY cal_date", [start, end]).fetchall()]
    out: list[DatasetCoverage] = []
    for contract in CATALOG_CONTRACTS:
        column = contract.event_date_column
        if column is None:
            continue
        required = contract.freshness.required_on_open_day
        try:
            latest = con.execute(f'SELECT MAX("{column}") FROM "{contract.table_name}"').fetchone()
            per_day = dict(con.execute(
                f'SELECT CAST("{column}" AS DATE), COUNT(*) FROM "{contract.table_name}" '
                f'WHERE "{column}" BETWEEN ? AND ? GROUP BY 1', [start, end]).fetchall())
        except duckdb.Error as exc:
            out.append(DatasetCoverage(dataset_id=contract.dataset_id,
                                       table_name=contract.table_name, latest_date=None,
                                       rows_in_window=0, missing_open_days=[],
                                       required_on_open_day=required,
                                       error=str(exc).splitlines()[0][:200]))
            continue
        latest_date = latest[0] if latest else None
        if isinstance(latest_date, datetime):
            latest_date = latest_date.date()
        out.append(DatasetCoverage(
            dataset_id=contract.dataset_id, table_name=contract.table_name,
            latest_date=latest_date, rows_in_window=int(sum(per_day.values())),
            missing_open_days=[d for d in open_days if d not in per_day] if required else [],
            required_on_open_day=required,
        ))
    return AuditReport(generated_at=datetime.now(UTC), window_start=start, window_end=end,
                       open_days=len(open_days), datasets=out)


def report_path(root: Path | None = None) -> Path:
    return (root or research_root()) / "data_audit" / "latest.json"


def read_report(root: Path | None = None) -> AuditReport | None:
    path = report_path(root)
    return AuditReport.model_validate_json(path.read_text()) if path.is_file() else None


def write_report(report: AuditReport, root: Path | None = None) -> Path:
    path = report_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(report.model_dump_json())
    tmp.replace(path)
    return path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m rquant.data_catalog.audit")
    p.add_argument("--db", type=Path, default=Path(os.environ.get("DUCKDB_READONLY_PATH", "")))
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--end", type=date.fromisoformat, default=date.today())
    a = p.parse_args(argv)
    con = duckdb.connect(str(a.db), read_only=True)
    try:
        report = audit(con, a.end, a.days)
    finally:
        con.close()
    print(write_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
