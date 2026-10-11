"""rquant-web-commands appends web commands in page control's on-disk layout."""

from __future__ import annotations

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer

from rquant.web import page_control
from rquant.web.commands_service import handle, make_handler


def _payload(kind: str, **fields: object) -> dict:
    return {"kind": kind, "command_id": "c" * 32,
            "requested_at": "2026-10-11T00:00:00+00:00", **fields}


def test_each_kind_lands_in_its_log_and_is_idempotent(tmp_path):
    cases = [
        (_payload("ack_alert", alert_id="a" * 64, generation_id="g", actor_id="roxor"),
         "alert_acks/acks.jsonl"),
        (_payload("add_watchlist_item", item={"ts_code": "600519.SH", "note": ""}),
         "watchlist/items.jsonl"),
        (_payload("save_alert_rule", rule={"rule_id": "r1", "title": "t", "enabled": False,
                                           "pools": ["manual"], "levels": [],
                                           "cooldown_minutes": 5}),
         "alert_rules/rules.jsonl"),
        (_payload("save_canvas", name="pool-a", description="", pool_refs=["x"]),
         "canvas_saves/saves.jsonl"),
    ]
    for payload, rel in cases:
        assert handle(tmp_path, payload)[0] == 200
        assert handle(tmp_path, payload)[0] == 200  # same command_id: one line
        lines = (tmp_path / rel).read_text().splitlines()
        assert len(lines) == 1 and json.loads(lines[0])["command_id"] == "c" * 32


def test_rejects_invalid_and_unknown(tmp_path):
    assert handle(tmp_path, _payload("ack_alert", alert_id="nothex"))[0] == 400
    assert handle(tmp_path, _payload("delete_everything"))[0] == 400
    assert not any(tmp_path.iterdir())


def test_http_roundtrip_with_web_forwarder(tmp_path, monkeypatch):
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(tmp_path))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        port = server.server_address[1]
        monkeypatch.setenv("RQUANT_PAGE_CONTROL_URL", f"http://127.0.0.1:{port}/v1/commands")
        receipt = page_control.forward(
            "add_watchlist_item", {"item": {"ts_code": "000001.SZ", "note": "n"}})
        assert receipt["status"] == "succeeded"
        assert "000001.SZ" in (tmp_path / "watchlist/items.jsonl").read_text()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz") as r:  # noqa: S310
            assert json.loads(r.read())["status"] == "ok"
    finally:
        server.shutdown()
