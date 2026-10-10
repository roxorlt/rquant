"""page control's ack log → alert_ack Serving projection (latest ack per alert)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from rquant.serving_page_projection_source import (
    SignalPageProjectionSnapshot,
    read_alert_ack_projection_source,
)

NOW = datetime(2026, 10, 9, 7, 0, tzinfo=UTC)


def _write(path: Path, *rows: tuple[str, str, str]) -> None:
    path.parent.mkdir(parents=True)
    path.write_text("".join(
        json.dumps({"alert_id": a, "ts": ts, "actor_id": who, "command_id": f"{a[0]}{ts}",
                    "generation_id": "g"}) + "\n" for a, ts, who in rows))


def test_missing_log_publishes_nothing(tmp_path: Path) -> None:
    assert read_alert_ack_projection_source(tmp_path / "none.jsonl", observed=NOW) is None
    assert read_alert_ack_projection_source(None, observed=NOW) is None


def test_latest_ack_per_alert_and_future_rows_wait(tmp_path: Path) -> None:
    log = tmp_path / "alert_acks" / "acks.jsonl"
    _write(log, ("a" * 64, "2026-10-09T01:00:00+00:00", "alice"),
           ("a" * 64, "2026-10-09T02:00:00+00:00", "bob"),
           ("b" * 64, "2026-10-09T08:00:00+00:00", "future"))
    source = read_alert_ack_projection_source(log, observed=NOW)
    assert source is not None
    assert [(r.alert_id[0], r.actor_id) for r in source.rows] == [("a", "bob")]

    snapshot = SignalPageProjectionSnapshot.create(available_at=NOW, alert_acks=source)
    table = {p.table_name: p for p in snapshot.projections}["alert_ack"]
    assert table.rows[0]["actor_id"] == "bob"


def test_manual_watchlist_latest_per_code(tmp_path: Path) -> None:
    from rquant.serving_page_projection_source import read_manual_watchlist_projection_source

    log = tmp_path / "watchlist" / "items.jsonl"
    log.parent.mkdir(parents=True)
    log.write_text("".join(json.dumps(r) + "\n" for r in [
        {"ts_code": "600519.SH", "note": "a", "ts": "2026-10-09T01:00:00+00:00", "command_id": "1"},
        {"ts_code": "600519.SH", "note": "b", "ts": "2026-10-09T02:00:00+00:00", "command_id": "2"},
        {"ts_code": "000001.SZ", "note": "", "ts": "2026-10-09T09:00:00+00:00", "command_id": "3"},
    ]))
    source = read_manual_watchlist_projection_source(log, observed=NOW)
    assert source is not None
    assert [(r.ts_code, r.note) for r in source.rows] == [("600519.SH", "b")]
    snapshot = SignalPageProjectionSnapshot.create(available_at=NOW, manual_watchlist=source)
    table = {p.table_name: p for p in snapshot.projections}["manual_watchlist"]
    assert table.rows[0]["ts_code"] == "600519.SH"
    assert read_manual_watchlist_projection_source(tmp_path / "x", observed=NOW) is None
