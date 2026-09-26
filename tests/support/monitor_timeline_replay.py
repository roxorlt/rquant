"""Build a local Serving replay from legacy-shaped, invented event sources."""

from __future__ import annotations

import argparse
import json
import os
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from rquant.notification_state import NotificationStateStore
from rquant.serving_contracts import ServingGenerationManifest
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    SignalPageProjectionProducer,
)
from rquant.surge_watch import SurgeConfirmed
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def build_monitor_timeline_replay(root: Path) -> ServingGenerationManifest:
    """Exercise operational file readers and notification publication before Serving."""

    with TemporaryDirectory(prefix="rquant-monitor-replay-") as directory:
        source_root = Path(os.path.realpath(directory))
        database = source_root / "rquant_ro.duckdb"
        with duckdb.connect(str(database)) as connection:
            connection.execute(
                """
                CREATE TABLE screen_result (
                    trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, name VARCHAR,
                    close DOUBLE, pct_chg DOUBLE, extra JSON, created_at TIMESTAMP
                )
                """
            )
            connection.execute(
                "INSERT INTO screen_result VALUES "
                "('2026-09-24', 'sample', '600001.SH', '样本01', 12.34, 2.5, '{}', "
                "'2026-09-24 09:35:00')"
            )
            connection.execute(
                """
                CREATE TABLE minute_bar (
                    ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, open DOUBLE,
                    high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE,
                    source VARCHAR, created_at TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE monitor_event (
                    trade_date DATE, ts_code VARCHAR, level VARCHAR,
                    trigger_price DOUBLE, level_price DOUBLE, trigger_time TIMESTAMP,
                    trigger_type VARCHAR, pool VARCHAR
                )
                """
            )
            connection.execute(
                "INSERT INTO monitor_event VALUES "
                "('2026-09-24', '600005.SH', 'attack_break_high', 12.34, 12.00, "
                "'2026-09-24 10:05:00', 'attack', 'pool2')"
            )
        live_root = source_root / "surge_live"
        live_root.mkdir()
        surge_path = live_root / "events-2026-09-24.jsonl"
        surge_path.write_text(
            json.dumps(
                SurgeConfirmed(
                    ts_code="600006.SH",
                    name="样本06",
                    confirmed_at="09:52",
                    price=11.25,
                    pct_chg=3.15,
                    status="confirmed",
                ).model_dump(),
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        source_stamp = (FIXTURE_BUILT_AT - timedelta(seconds=10)).timestamp()
        os.utime(database, (source_stamp, source_stamp))
        os.utime(surge_path, (source_stamp, source_stamp))

        store = NotificationStateStore(source_root / "notification.sqlite3")
        SignalPageProjectionProducer(
            source=DuckDBSignalPageProjectionSource(database, surge_live_root=live_root),
            store=store,
        ).publish(FIXTURE_BUILT_AT)
        published = store.serving_snapshot(observed_at=FIXTURE_BUILT_AT, history_limit=1)
        events = tuple(
            item
            for item in published.payload.projections
            if item.table_name in {"monitor_event", "surge_event"}
        )
        return build_web_fixture(root, "panorama", event_projections=events)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    manifest = build_monitor_timeline_replay(args.out)
    print(manifest.generation_id)


if __name__ == "__main__":
    main()
