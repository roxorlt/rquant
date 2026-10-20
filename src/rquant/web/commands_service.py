"""rquant-web-commands: the tiny write sink for the web app's commands.

Accepts ``POST /v1/commands`` with the same payload ``rquant.web.page_control`` sends
(``ack_alert``, ``unack_alert``, ``add_watchlist_item``, ``save_alert_rule``, ``save_canvas``), validates it
with page control's own command models, and appends one JSON line (idempotent by
``command_id``) under a fixed data directory, in page control's on-disk layout:

    <dir>/alert_acks/acks.jsonl      ack_alert            (page_control.ALERT_ACK_LOG)
    <dir>/watchlist/items.jsonl      add_watchlist_item   (page_control.WATCHLIST_LOG)
    <dir>/alert_rules/rules.jsonl    save_alert_rule      (page_control.ALERT_RULE_LOG)
    <dir>/canvas_saves/saves.jsonl   save_canvas          (pool saves; not the signed catalog)

No credentials, no runtime pyz, no database: stdlib HTTP server on 127.0.0.1.
Readers (notifier projection, monitor) are expected to read these files; see
docs/deploy/web-commands-service.md for what is wired today.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
from datetime import UTC
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, TypeAdapter, ValidationError

from rquant.page_control import (
    ALERT_ACK_LOG,
    ALERT_RULE_LOG,
    WATCHLIST_LOG,
    AckAlert,
    AddWatchlistItem,
    SaveAlertRule,
    SaveCanvas,
    UnackAlert,
    _append_managed_jsonl,
)

CANVAS_SAVE_LOG = Path("canvas_saves") / "saves.jsonl"
MAX_BODY = 64 * 1024
WebCommand = Annotated[
    AckAlert | UnackAlert | AddWatchlistItem | SaveAlertRule | SaveCanvas,
    Field(discriminator="kind"),
]
_ADAPTER: TypeAdapter[Any] = TypeAdapter(WebCommand)
_LOCK = threading.Lock()
log = logging.getLogger("rquant-web-commands")


def record_for(command: Any) -> tuple[Path, dict[str, object]]:
    record: dict[str, object] = {
        "command_id": command.command_id,
        "ts": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
    }
    if isinstance(command, (AckAlert, UnackAlert)):
        record.update(alert_id=command.alert_id, generation_id=command.generation_id,
                      actor_id=command.actor_id)
        if isinstance(command, UnackAlert):
            record["action"] = "unack"
        return ALERT_ACK_LOG, record
    if isinstance(command, AddWatchlistItem):
        record.update(ts_code=command.item.ts_code, note=command.item.note)
        return WATCHLIST_LOG, record
    if isinstance(command, SaveAlertRule):
        record.update(command.rule.model_dump(mode="json"))
        return ALERT_RULE_LOG, record
    record.update(name=command.name, description=command.description,
                  pool_refs=list(command.pool_refs))
    return CANVAS_SAVE_LOG, record


def handle(data_dir: Path, payload: object) -> tuple[int, dict[str, object]]:
    command_id = payload.get("command_id") if isinstance(payload, dict) else None
    try:
        command = _ADAPTER.validate_python(payload)
    except ValidationError as exc:
        return 400, {"command_id": command_id, "status": "rejected",
                     "error": exc.errors(include_url=False, include_input=False)[:5].__repr__()}
    rel, record = record_for(command)
    path = data_dir / rel
    with _LOCK:
        _append_managed_jsonl(path, record, command_id=command.command_id)
    log.info("recorded %s %s -> %s", command.kind, command.command_id, rel)
    return 200, {"command_id": command.command_id, "status": "succeeded",
                 "detail": f"recorded in {rel}"}


def make_handler(data_dir: Path) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "rquant-web-commands"

        def _reply(self, code: int, body: dict[str, object]) -> None:
            raw = json.dumps(body, ensure_ascii=True).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802 - http.server API
            if self.path == "/healthz":
                self._reply(200, {"status": "ok"})
            else:
                self._reply(404, {"status": "not_found"})

        def do_POST(self) -> None:  # noqa: N802 - http.server API
            if self.path != "/v1/commands":
                self._reply(404, {"status": "not_found"})
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY:
                self._reply(413, {"status": "rejected", "error": "body size"})
                return
            try:
                payload = json.loads(self.rfile.read(length))
            except ValueError:
                self._reply(400, {"status": "rejected", "error": "invalid json"})
                return
            try:
                code, body = handle(data_dir, payload)
            except Exception as exc:  # noqa: BLE001 - surfaced to the caller as 500
                log.exception("append failed")
                code, body = 500, {"status": "failed", "error": type(exc).__name__}
            self._reply(code, body)

        def log_message(self, fmt: str, *args: object) -> None:
            log.debug(fmt, *args)

    return Handler


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="rquant-web-commands")
    ap.add_argument("--dir", required=True, type=Path)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8769)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    data_dir = args.dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(data_dir))
    log.info("listening on %s:%s, data dir %s", args.host, args.port, data_dir)
    server.serve_forever()


if __name__ == "__main__":
    main()
