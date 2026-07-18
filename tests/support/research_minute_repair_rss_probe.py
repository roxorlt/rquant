"""Measure peak RSS while staging a prepared sequence of minute-repair days."""

from __future__ import annotations

import json
import os
import resource
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb

import rquant.research_minute_repair as repair_module
from rquant.research_catalog import ResearchCatalog
from rquant.research_ingest import ResearchIngestPaths
from rquant.research_minute_repair import (
    MinuteRepairSession,
    ResearchMinuteRepairDayPlan,
)


def _peak_rss_bytes() -> int:
    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return peak if sys.platform == "darwin" else peak * 1024


def _load_probe(root: Path) -> tuple[
    str,
    ResearchMinuteRepairDayPlan,
    tuple[ResearchMinuteRepairDayPlan, ...],
    dict[date, tuple[MinuteRepairSession, ...]],
]:
    payload: dict[str, Any] = json.loads(
        (root / "probe.json").read_text(encoding="utf-8")
    )
    days = tuple(
        ResearchMinuteRepairDayPlan.model_validate(item)
        for item in payload["days"]
    )
    warmup_day = ResearchMinuteRepairDayPlan.model_validate(
        payload["warmup_day"]
    )
    targets = {
        date.fromisoformat(trade_date): tuple(
            MinuteRepairSession.model_validate(item)
            for item in items
        )
        for trade_date, items in payload["targets_by_date"].items()
    }
    return str(payload["code_commit"]), warmup_day, days, targets


def main() -> int:
    root = Path(sys.argv[1]).resolve()
    code_commit, warmup_day, days, targets = _load_probe(root)
    live_root = root / "live"
    staged_root = root / "staged"
    paths = ResearchIngestPaths(
        state_dir=live_root,
        catalog_path=live_root / "research.duckdb",
        readonly_catalog_path=live_root / "research_ro.duckdb",
        lake_root=live_root / "lake",
        staging_root=live_root / "staging",
    )
    staged_catalog = staged_root / "research.duckdb"
    staged_lake = staged_root / "lake"
    with ResearchCatalog(paths.catalog_path)._connection():
        pass
    with ResearchCatalog(staged_catalog)._connection():
        pass
    prepared = SimpleNamespace(
        target_sessions_by_date=targets,
        plan=SimpleNamespace(code_commit=code_commit),
    )
    expected_days = {
        day.trade_date: day for day in (warmup_day, *days)
    }

    def build_probe_day_plan(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
        # Row-hash equivalence has dedicated tests; this probe isolates the
        # cross-day DataFrame, DuckDB, and Parquet staging memory lifecycle.
        merged = repair_module.merge_minute_partition(
            kwargs["existing"],
            kwargs["operational"],
            trade_date=kwargs["trade_date"],
            target_sessions=kwargs["target_sessions"],
        )
        return expected_days[kwargs["trade_date"]], merged

    repair_module.build_minute_repair_day_plan = build_probe_day_plan
    generated_at = datetime(2026, 7, 18, 8, 0, tzinfo=UTC)
    peak_rss_by_day: list[int] = []
    with duckdb.connect(
        str(root / "operational.duckdb"),
        read_only=True,
        config={"temp_directory": ""},
    ) as source:
        repair_module._stage_repair_day(
            paths,
            source=source,
            prepared=prepared,
            day=warmup_day,
            staged_catalog=staged_catalog,
            staged_lake=staged_lake,
            generated_at=generated_at,
            as_of_date=max(item.trade_date for item in days),
        )
        warmup_peak_rss_bytes = _peak_rss_bytes()
        for day in days:
            repair_module._stage_repair_day(
                paths,
                source=source,
                prepared=prepared,
                day=day,
                staged_catalog=staged_catalog,
                staged_lake=staged_lake,
                generated_at=generated_at,
                as_of_date=max(item.trade_date for item in days),
            )
            peak_rss_by_day.append(_peak_rss_bytes())
            if os.getenv("RQUANT_RSS_PROBE_TRACE") == "1":
                print(
                    f"{day.trade_date.isoformat()} {peak_rss_by_day[-1]}",
                    file=sys.stderr,
                    flush=True,
                )
    print(json.dumps({
        "day_count": len(days),
        "total_rows": sum(day.source_row_count for day in days),
        "max_day_rows": max(day.source_row_count for day in days),
        "peak_rss_bytes": _peak_rss_bytes(),
        "warmup_peak_rss_bytes": warmup_peak_rss_bytes,
        "peak_rss_by_day": peak_rss_by_day,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
