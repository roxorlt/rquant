"""盯盘读取网页手动自选：Serving manual_watchlist → build_watchlist(pool='manual')."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.monitor import build_watchlist, load_manual_watchlist_codes
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingTableSpec
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path: Path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _first_limit_up(store: DuckDBStore, code: str) -> None:
    store._conn.execute(
        f"""
        INSERT INTO daily_state VALUES
        ('{code}', '2026-04-20', false, false, 'main', 0.10,
         18.0, 12.0, true, false, true, false, 1, 16.50, 14.80)
        """
    )


def test_manual_codes_join_watchlist_with_levels(store: DuckDBStore) -> None:
    _first_limit_up(store, "600519.SH")
    items = build_watchlist(store, manual_codes=["600519.SH", "000001.SZ"])
    assert [(i.ts_code, i.pool) for i in items] == [("600519.SH", "manual")]
    assert items[0].level_40 == pytest.approx(14.80 + (16.50 - 14.80) * 0.4)


def test_pool_membership_wins_over_manual(store: DuckDBStore) -> None:
    store.upsert_pool2_watch(pd.DataFrame([{
        "ts_code": "002415.SZ", "entry_date": "2026-04-18", "limit_up_date": "2026-04-17",
        "body_upper": 13.2, "body_lower": 11.8, "level_40": 12.36, "level_30": 12.22,
        "level_20": 12.08, "stop_strong": 11.8, "stop_weak": 11.52, "status": "active"}]))
    items = build_watchlist(store, manual_codes=["002415.SZ"])
    assert [i.pool for i in items] == ["pool2"]


def test_load_codes_from_serving(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    assert load_manual_watchlist_codes(root) == []  # no generation: monitor keeps running
    built = datetime(2026, 10, 9, 7, 0, tzinfo=UTC)
    ServingPublisher(
        root, producer_commit="a" * 40,
        table_specs={"manual_watchlist": ServingTableSpec(sort_keys=("ts_code",))},
    ).publish(
        {"manual_watchlist": pd.DataFrame({
            "ts_code": ["600519.SH"], "note": [""], "added_at": [built], "command_id": ["c"]})},
        watermarks=(ServingDatasetWatermark(
            dataset_id="manual_watchlist", generation_id="s", event_time=built - timedelta(seconds=1),
            published_at=built, sequence=1, status=FreshnessStatus.FRESH),),
        source_generations={"manual_watchlist": "s"},
        built_at=built,
    )
    assert load_manual_watchlist_codes(root) == ["600519.SH"]
