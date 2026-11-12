"""rquant-web-commands: append-only JSONL write sink and read projections for web commands.

Accepts ``POST /v1/commands`` with the payload ``rquant.web.page_control`` sends
(``ack_alert``, ``unack_alert``, ``add_watchlist_item``, ``remove_watchlist_item``,
``save_alert_rule``, ``save_canvas``), validates it with local Pydantic command models
(zero dependency on legacy ``rquant.page_control``), and appends one JSON line
(idempotent by ``command_id``) under a fixed data directory:

    <dir>/alert_acks/acks.jsonl      ack_alert / unack_alert
    <dir>/watchlist/items.jsonl      add_watchlist_item / remove_watchlist_item
    <dir>/alert_rules/rules.jsonl    save_alert_rule
    <dir>/canvas_saves/saves.jsonl   save_canvas
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
)

ALERT_ACK_LOG = Path("alert_acks") / "acks.jsonl"
WATCHLIST_LOG = Path("watchlist") / "items.jsonl"
ALERT_RULE_LOG = Path("alert_rules") / "rules.jsonl"
CANVAS_SAVE_LOG = Path("canvas_saves") / "saves.jsonl"
MAX_BODY = 64 * 1024
_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
_POOL_NAME_RE = re.compile(r"^[\w\u4e00-\u9fff][\w\u4e00-\u9fff ·._-]{0,63}$")


class WebCommandBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareDatetime


class AckAlert(WebCommandBase):
    kind: Literal["ack_alert"] = "ack_alert"
    alert_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)


class UnackAlert(WebCommandBase):
    kind: Literal["unack_alert"] = "unack_alert"
    alert_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)


class WatchlistItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts_code: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    note: str = Field(default="", max_length=200)


class AddWatchlistItem(WebCommandBase):
    kind: Literal["add_watchlist_item"] = "add_watchlist_item"
    item: WatchlistItem


class RemoveWatchlistItem(WebCommandBase):
    kind: Literal["remove_watchlist_item"] = "remove_watchlist_item"
    ts_code: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")


class AlertRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule_id: str = Field(pattern=r"^[a-z0-9_-]{1,40}$")
    title: str = Field(min_length=1, max_length=60)
    enabled: bool = True
    pools: tuple[Literal["pool1", "pool2", "manual"], ...] = ()
    levels: tuple[str, ...] = Field(default=(), max_length=10)
    cooldown_minutes: int = Field(default=0, ge=0, le=24 * 60)


class SaveAlertRule(WebCommandBase):
    kind: Literal["save_alert_rule"] = "save_alert_rule"
    rule: AlertRule


class SaveCanvas(WebCommandBase):
    kind: Literal["save_canvas"] = "save_canvas"
    name: str = Field(min_length=1, max_length=64)
    description: str = Field(default="", max_length=300)
    pool_refs: tuple[str, ...] = ()
    codes: tuple[str, ...] = Field(default=(), max_length=5000)
    mode: Literal["new", "existing"] = "new"
    source: str = "page_control"

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not _POOL_NAME_RE.match(cleaned) or ".." in cleaned:
            raise ValueError("invalid pool name")
        return cleaned

    @field_validator("codes")
    @classmethod
    def _validate_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in values:
            code = raw.strip().upper()
            if not _CODE_RE.match(code):
                raise ValueError(f"invalid stock code: {raw}")
            if code not in seen:
                seen.add(code)
                out.append(code)
        return tuple(out)


WebCommand = Annotated[
    AckAlert
    | UnackAlert
    | AddWatchlistItem
    | RemoveWatchlistItem
    | SaveAlertRule
    | SaveCanvas,
    Field(discriminator="kind"),
]
_ADAPTER: TypeAdapter[WebCommand] = TypeAdapter(WebCommand)
_LOCK = threading.Lock()
log = logging.getLogger("rquant-web-commands")


class AckStateRecord(BaseModel):
    alert_id: str
    acked_at: datetime | None = None
    actor_id: str | None = None
    command_id: str


class WatchlistRecord(BaseModel):
    code: str
    note: str = ""
    added_at: datetime | None = None
    command_id: str


class AlertRuleRecord(BaseModel):
    rule_id: str
    title: str
    enabled: bool = True
    pools: list[Literal["pool1", "pool2", "manual"]] = Field(default_factory=list)
    levels: list[str] = Field(default_factory=list)
    cooldown_minutes: int = 0
    saved_at: datetime | None = None
    command_id: str


class SavedPoolRecord(BaseModel):
    name: str
    description: str = ""
    pool_refs: list[str] = Field(default_factory=list)
    codes: list[str] = Field(default_factory=list)
    updated_at: datetime | None = None
    command_id: str


class RecordedOperation(BaseModel):
    at: datetime | None = None
    kind: Literal[
        "ack_alert",
        "unack_alert",
        "add_watchlist_item",
        "remove_watchlist_item",
        "save_alert_rule",
        "save_canvas",
    ]
    summary: str
    actor: str | None = None
    command_id: str


def _append_jsonl_idempotent(path: Path, record: dict[str, object], command_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        for line in handle:
            try:
                existing = json.loads(line)
            except ValueError:
                continue
            if isinstance(existing, dict) and existing.get("command_id") == command_id:
                return
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def record_for(command: WebCommand) -> tuple[Path, dict[str, object]]:
    record: dict[str, object] = {
        "command_id": command.command_id,
        "ts": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
    }
    if isinstance(command, AckAlert | UnackAlert):
        record.update(
            alert_id=command.alert_id,
            generation_id=command.generation_id,
            actor_id=command.actor_id,
        )
        if isinstance(command, UnackAlert):
            record["action"] = "unack"
        return ALERT_ACK_LOG, record
    if isinstance(command, AddWatchlistItem):
        record.update(ts_code=command.item.ts_code, note=command.item.note)
        return WATCHLIST_LOG, record
    if isinstance(command, RemoveWatchlistItem):
        record.update(ts_code=command.ts_code, action="remove")
        return WATCHLIST_LOG, record
    if isinstance(command, SaveAlertRule):
        record.update(command.rule.model_dump(mode="json"))
        return ALERT_RULE_LOG, record
    record.update(
        name=command.name,
        description=command.description,
        pool_refs=list(command.pool_refs),
    )
    if command.codes:
        record["codes"] = list(command.codes)
    if command.mode != "new":
        record["mode"] = command.mode
    return CANVAS_SAVE_LOG, record


def handle(data_dir: Path, payload: object) -> tuple[int, dict[str, object]]:
    command_id = payload.get("command_id") if isinstance(payload, dict) else None
    try:
        command = _ADAPTER.validate_python(payload)
    except ValidationError as exc:
        return 400, {
            "command_id": command_id,
            "status": "rejected",
            "error": exc.errors(include_url=False, include_input=False)[:5].__repr__(),
        }
    rel, record = record_for(command)
    path = data_dir / rel
    with _LOCK:
        _append_jsonl_idempotent(path, record, command_id=command.command_id)
    log.info("recorded %s %s -> %s", command.kind, command.command_id, rel)
    return 200, {
        "command_id": command.command_id,
        "status": "succeeded",
        "detail": f"recorded in {rel}",
    }


def _parse_ts(raw: object) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def read_ack_states(data_dir: Path) -> dict[str, AckStateRecord | None]:
    """Latest ack state per alert_id; ``None`` means explicitly unacked."""
    states: dict[str, AckStateRecord | None] = {}
    for row in _iter_jsonl(data_dir / ALERT_ACK_LOG):
        aid = str(row.get("alert_id") or "")
        cid = str(row.get("command_id") or "")
        if not aid or not cid:
            continue
        if row.get("action") == "unack":
            states[aid] = None
        else:
            states[aid] = AckStateRecord(
                alert_id=aid,
                acked_at=_parse_ts(row.get("ts")),
                actor_id=str(row["actor_id"]) if row.get("actor_id") else None,
                command_id=cid,
            )
    return states


def read_watchlist_items(data_dir: Path) -> list[WatchlistRecord]:
    """Active manual watchlist items, newest first, respecting ``remove`` entries."""
    by_code: dict[str, WatchlistRecord] = {}
    for row in _iter_jsonl(data_dir / WATCHLIST_LOG):
        raw_code = row.get("ts_code") or (row.get("item") or {}).get("ts_code")
        code = str(raw_code or "").strip().upper()
        cid = str(row.get("command_id") or "")
        if not _CODE_RE.match(code):
            continue
        if row.get("action") == "remove":
            by_code.pop(code, None)
            continue
        by_code.pop(code, None)
        by_code[code] = WatchlistRecord(
            code=code,
            note=str(row.get("note") or ""),
            added_at=_parse_ts(row.get("ts")),
            command_id=cid or code,
        )
    return list(reversed(list(by_code.values())))


def read_alert_rules(data_dir: Path) -> dict[str, AlertRuleRecord]:
    """Latest alert rule per ``rule_id``."""
    rules: dict[str, AlertRuleRecord] = {}
    valid_pools = {"pool1", "pool2", "manual"}
    for row in _iter_jsonl(data_dir / ALERT_RULE_LOG):
        rid = str(row.get("rule_id") or "")
        title = str(row.get("title") or "")
        cid = str(row.get("command_id") or "")
        if not rid or not title:
            continue
        raw_pools = row.get("pools") or []
        pools: list[Literal["pool1", "pool2", "manual"]] = [
            p for p in raw_pools if p in valid_pools
        ]
        raw_levels = row.get("levels") or []
        levels = [str(x) for x in raw_levels if str(x)]
        rules[rid] = AlertRuleRecord(
            rule_id=rid,
            title=title,
            enabled=bool(row.get("enabled", True)),
            pools=pools,
            levels=levels,
            cooldown_minutes=int(row.get("cooldown_minutes") or 0),
            saved_at=_parse_ts(row.get("ts")),
            command_id=cid or rid,
        )
    return rules


def read_saved_pools(data_dir: Path) -> list[SavedPoolRecord]:
    """Saved pools from ``canvas_saves/saves.jsonl`` (latest per name; ``existing`` merges)."""
    by_name: dict[str, SavedPoolRecord] = {}
    for row in _iter_jsonl(data_dir / CANVAS_SAVE_LOG):
        name = str(row.get("name") or "").strip()
        cid = str(row.get("command_id") or "")
        if not name:
            continue
        refs = [str(r) for r in (row.get("pool_refs") or []) if str(r)]
        codes = [
            s
            for c in (row.get("codes") or [])
            if _CODE_RE.match(s := str(c or "").strip().upper())
        ]
        prev = by_name.get(name)
        if row.get("mode") == "existing" and prev is not None:
            merged_refs = list(dict.fromkeys([*prev.pool_refs, *refs]))
            merged_codes = list(dict.fromkeys([*prev.codes, *codes]))
            desc = str(row.get("description") or prev.description)
            by_name[name] = SavedPoolRecord(
                name=name,
                description=desc,
                pool_refs=merged_refs,
                codes=merged_codes,
                updated_at=_parse_ts(row.get("ts")) or prev.updated_at,
                command_id=cid or prev.command_id,
            )
        else:
            by_name[name] = SavedPoolRecord(
                name=name,
                description=str(row.get("description") or ""),
                pool_refs=refs,
                codes=codes,
                updated_at=_parse_ts(row.get("ts")),
                command_id=cid or name,
            )
    return list(reversed(list(by_name.values())))


def read_operations(data_dir: Path) -> list[RecordedOperation]:
    """Audit trail across all four JSONL command logs, newest first."""
    ops: list[RecordedOperation] = []
    for row in _iter_jsonl(data_dir / ALERT_ACK_LOG):
        aid = str(row.get("alert_id") or "")
        cid = str(row.get("command_id") or "")
        if not aid or not cid:
            continue
        unack = row.get("action") == "unack"
        ops.append(
            RecordedOperation(
                at=_parse_ts(row.get("ts")),
                kind="unack_alert" if unack else "ack_alert",
                summary=f"{'撤销确认' if unack else '确认告警'} {aid[:12]}",
                actor=str(row["actor_id"]) if row.get("actor_id") else None,
                command_id=cid,
            )
        )
    for row in _iter_jsonl(data_dir / WATCHLIST_LOG):
        code = str(row.get("ts_code") or (row.get("item") or {}).get("ts_code") or "").upper()
        cid = str(row.get("command_id") or "")
        if not code or not cid:
            continue
        removed = row.get("action") == "remove"
        note = str(row.get("note") or "")
        ops.append(
            RecordedOperation(
                at=_parse_ts(row.get("ts")),
                kind="remove_watchlist_item" if removed else "add_watchlist_item",
                summary=f"移出自选 {code}" if removed else f"加自选 {code} {note}".strip(),
                actor=None,
                command_id=cid,
            )
        )
    for row in _iter_jsonl(data_dir / ALERT_RULE_LOG):
        title = str(row.get("title") or "")
        cid = str(row.get("command_id") or "")
        if not title or not cid:
            continue
        enabled = bool(row.get("enabled", True))
        ops.append(
            RecordedOperation(
                at=_parse_ts(row.get("ts")),
                kind="save_alert_rule",
                summary=f"{'启用' if enabled else '停用'}告警规则 {title}",
                actor=None,
                command_id=cid,
            )
        )
    for row in _iter_jsonl(data_dir / CANVAS_SAVE_LOG):
        name = str(row.get("name") or "").strip()
        cid = str(row.get("command_id") or "")
        if not name or not cid:
            continue
        ops.append(
            RecordedOperation(
                at=_parse_ts(row.get("ts")),
                kind="save_canvas",
                summary=f"保存池子 {name}",
                actor=None,
                command_id=cid,
            )
        )
    ops.sort(key=lambda i: i.at.timestamp() if i.at else 0, reverse=True)
    return ops


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
