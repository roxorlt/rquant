"""ack_alert / add_watchlist_item: lean, append-only, idempotent page-control effects."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.page_control import (
    AckAlert,
    AddWatchlistItem,
    PageControlOutbox,
    PageControlStatus,
    WatchlistItem,
    parse_page_control_command,
)
from tests.unit.test_page_control import NOW, _claim_as_crashed, _service_for

ALERT = "a" * 64


def _ack(command_id: str = "ack-1") -> AckAlert:
    return AckAlert(
        command_id=command_id, requested_at=NOW, alert_id=ALERT, generation_id="g1",
        actor_id="owner",
    )


def _rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_ack_and_watch_append_once(tmp_path: Path) -> None:
    service = _service_for(outbox=PageControlOutbox(tmp_path / "c.sqlite3"), tmp_path=tmp_path)
    assert service.submit(_ack()).status is PageControlStatus.SUCCEEDED
    assert service.submit(_ack()).status is PageControlStatus.SUCCEEDED  # same id: no-op
    watch = AddWatchlistItem(
        command_id="w-1", requested_at=NOW, item=WatchlistItem(ts_code="600519.SH", note="x"))
    assert service.submit(watch).status is PageControlStatus.SUCCEEDED

    acks = _rows(tmp_path / "data" / "alert_acks" / "acks.jsonl")
    assert [(r["alert_id"], r["actor_id"], r["command_id"]) for r in acks] == [
        (ALERT, "owner", "ack-1")]
    items = _rows(tmp_path / "data" / "watchlist" / "items.jsonl")
    assert [(r["ts_code"], r["note"]) for r in items] == [("600519.SH", "x")]


def test_crashed_ack_replays_once_after_restart(tmp_path: Path) -> None:
    outbox_path = tmp_path / "c.sqlite3"
    command = _ack("crashed")
    _claim_as_crashed(PageControlOutbox(outbox_path), command)
    restarted = _service_for(
        outbox=PageControlOutbox(outbox_path), tmp_path=tmp_path, now=NOW + timedelta(seconds=2))
    first, duplicate = restarted.submit(command), restarted.submit(command)
    assert first.status == duplicate.status == "succeeded"
    ids = [r["command_id"] for r in _rows(tmp_path / "data" / "alert_acks" / "acks.jsonl")]
    assert ids == ["crashed"]


def test_wire_format_matches_web_forwarder() -> None:
    command = parse_page_control_command({
        "kind": "add_watchlist_item", "command_id": "c", "requested_at": NOW.isoformat(),
        "item": {"ts_code": "000001.SZ", "note": ""}})
    assert isinstance(command, AddWatchlistItem)
    with pytest.raises(ValidationError):
        parse_page_control_command({
            "kind": "ack_alert", "command_id": "c", "requested_at": NOW.isoformat(),
            "alert_id": "not-hex", "generation_id": "g", "actor_id": "o"})
