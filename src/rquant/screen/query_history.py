"""Private screening facts in the existing PageControl transaction authority."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import stat
import unicodedata
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

from rquant.manual_watchlist import OwnerId
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.strict_json import strict_json_loads
from rquant.web.models.screen import ScreenRunData

from .query_contracts import (
    ExecuteScreenQuery, ScreenExecutionResults, ScreenHistoryPage, ScreenPresetDefinition,
    ScreenQueryExecution, ScreenQueryPreset, _OwnedExecuteScreenQuery,
)

if TYPE_CHECKING:
    from rquant.page_control import PageControlClaim, PageControlOutbox, PageControlReceipt

MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_RESULT_ROWS = 8000


def install_screen_query_tables(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS screen_query_execution (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id TEXT NOT NULL,
            execution_id TEXT NOT NULL UNIQUE,
            command_hash TEXT NOT NULL,
            definition_json TEXT NOT NULL,
            original_command_json TEXT NOT NULL,
            started_at TEXT,
            facts_json TEXT,
            log_json TEXT,
            FOREIGN KEY(execution_id) REFERENCES page_control_command(command_id)
        );
        CREATE INDEX IF NOT EXISTS screen_query_history_owner_idx
            ON screen_query_execution(owner_id,sequence DESC);
        CREATE TABLE IF NOT EXISTS screen_query_preset (
            owner_id TEXT NOT NULL,
            preset_id TEXT NOT NULL,
            name_key TEXT NOT NULL,
            body_json TEXT NOT NULL,
            PRIMARY KEY(owner_id,preset_id),
            UNIQUE(owner_id,name_key)
        );
    """)
    from rquant.screen.alert_draft import install_screen_alert_draft_table
    install_screen_alert_draft_table(connection)


def register_screen_execution(connection: sqlite3.Connection, command: _OwnedExecuteScreenQuery) -> None:
    from rquant.page_control import _command_hash

    connection.execute(
        "INSERT INTO screen_query_execution(owner_id,execution_id,command_hash,definition_json,original_command_json) VALUES (?,?,?,?,?)",
        (command.owner_id, command.command_id, _command_hash(command), command.definition.model_dump_json(), ExecuteScreenQuery.model_validate(command.model_dump(exclude={"owner_id"})).model_dump_json()),
    )


def prepare_private_screen_outbox(path: Path) -> None:
    from rquant.page_control import _bind_managed_directory
    target=Path(path)
    bound=_bind_managed_directory(target.parent,create=True)
    try:
        fd=os.open(target.name,os.O_WRONLY|os.O_CREAT|os.O_CLOEXEC|os.O_NOFOLLOW,0o600,dir_fd=bound.descriptor)
        try:
            node=os.fstat(fd)
            if not stat.S_ISREG(node.st_mode) or node.st_uid!=os.geteuid() or stat.S_IMODE(node.st_mode)!=0o600 or node.st_nlink!=1:
                raise ValueError("private screen outbox permissions are invalid")
            bound.verify()
        finally: os.close(fd)
    finally: bound.close()


class ScreenQueryHistory:
    def __init__(self, outbox: PageControlOutbox, *, cursor_key: bytes) -> None:
        if len(cursor_key) < 32:
            raise ValueError("private screen cursor key is too short")
        self.outbox, self.cursor_key = outbox, bytes(cursor_key)
        self.artifact_root = outbox.path.parent / "screen-query-results"
        self._assert_private_database()
        from rquant.page_control import _bind_managed_directory

        bound = _bind_managed_directory(self.artifact_root, create=True)
        bound.close()

    def _assert_private_database(self) -> None:
        for path, mode, directory in [(self.outbox.path.parent,0o700,True),(self.outbox.path,0o600,False)]:
            node=path.lstat()
            if node.st_uid != os.geteuid() or stat.S_IMODE(node.st_mode)!=mode or stat.S_ISLNK(node.st_mode) or (stat.S_ISDIR(node.st_mode) if directory else stat.S_ISREG(node.st_mode)) is False:
                raise ValueError("screen history requires private PageControl storage")
            if not directory and node.st_nlink != 1:
                raise ValueError("screen history database must not be hard-linked")

    def scope_tag(self, owner_id: str) -> str:
        owner=TypeAdapter(OwnerId).validate_python(owner_id)
        return hmac.new(self.cursor_key, ("screen-owner/v1:"+owner).encode(),hashlib.sha256).hexdigest()

    def ensure_capacity(self) -> None:
        self._assert_private_database()
        usage=os.statvfs(self.artifact_root)
        if usage.f_bavail*usage.f_frsize < MAX_ARTIFACT_BYTES + 128*1024:
            raise ValueError("screen history capacity unavailable")

    def _encode(self, value: dict[str, object]) -> str:
        payload=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode()
        signed=hmac.new(self.cursor_key,b"screen-cursor/v1:"+payload,hashlib.sha256).digest()
        return base64.urlsafe_b64encode(signed+payload).decode()

    def _decode(self, cursor: str, expected: dict[str, object]) -> dict[str, object]:
        try:
            if len(cursor)>1024: raise ValueError("cursor too long")
            encoded=base64.b64decode(cursor,altchars=b"-_",validate=True)
            signature,payload=encoded[:32],encoded[32:]
            if not hmac.compare_digest(signature,hmac.new(self.cursor_key,b"screen-cursor/v1:"+payload,hashlib.sha256).digest()):
                raise ValueError("cursor signature changed")
            value=strict_json_loads(payload)
            if not isinstance(value,dict) or any(value.get(k)!=v for k,v in expected.items()):
                raise ValueError("cursor scope changed")
            return value
        except Exception as error:
            raise ValueError("private screen cursor is invalid") from error

    def _execution(self, row: sqlite3.Row) -> ScreenQueryExecution:
        from .query_contracts import ScreenQueryDefinition

        if row["facts_json"] is not None:
            return ScreenQueryExecution.model_validate({**strict_json_loads(row["facts_json"]),"owner_id":row["owner_id"]})
        return ScreenQueryExecution(
            owner_id=row["owner_id"],execution_id=row["execution_id"],sequence=row["sequence"],
            command_hash=row["command_hash"],plan_hash=ScreenQueryDefinition.model_validate_json(row["definition_json"]).normalized_plan_sha256,
            definition=ScreenQueryDefinition.model_validate_json(row["definition_json"]),started_at=row["started_at"],status=row["command_status"],
            original_command=ExecuteScreenQuery.model_validate_json(row["original_command_json"]),
        )

    def mark_started(self, claim: PageControlClaim, *, now: datetime) -> None:
        from rquant.page_control import _COMMAND_ADAPTER, _command_hash

        if type(claim.command) is not _OwnedExecuteScreenQuery:
            raise TypeError("screen start requires an exact trusted execution")
        self._assert_private_database()
        stamp = normalize_aware_utc(now).isoformat(timespec="microseconds")
        with closing(self.outbox._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM page_control_command WHERE command_id=?", (claim.command.command_id,)).fetchone()
            if row is None or row["command_hash"] != _command_hash(claim.command) or _COMMAND_ADAPTER.validate_json(row["payload_json"]) != claim.command:
                raise ValueError("screen command changed before execution")
            if row["status"] != "processing" or row["processing_owner"] != claim.owner_id or row["claim_token"] != claim.claim_token or row["lease_expires_at"] is None or row["lease_expires_at"] <= stamp:
                raise RuntimeError("stale screen claim cannot start")
            if connection.execute("UPDATE screen_query_execution SET started_at=coalesce(started_at,?) WHERE owner_id=? AND execution_id=?", (stamp, claim.command.owner_id, claim.command.command_id)).rowcount != 1:
                raise ValueError("screen execution was not registered")

    def history(self, owner_id: str, *, limit: int=20, cursor: str | None=None) -> ScreenHistoryPage:
        if type(limit) is not int or not 1<=limit<=100: raise ValueError("invalid history page size")
        tag=self.scope_tag(owner_id);self._assert_private_database()
        binding={"schema":1,"kind":"history","owner":tag,"filter":canonical_sha256({})}
        with closing(self.outbox._connect()) as connection:
            high=connection.execute("SELECT coalesce(max(sequence),0) FROM screen_query_execution WHERE owner_id=?",(owner_id,)).fetchone()[0]
            before=high+1
            if cursor:
                value=self._decode(cursor,binding);high=value.get("high");before=value.get("before")
                if type(high) is not int or type(before) is not int or not 0<before<=high+1: raise ValueError("invalid history cursor sequence")
            rows=connection.execute("SELECT e.*,c.status AS command_status FROM screen_query_execution e JOIN page_control_command c ON c.command_id=e.execution_id WHERE e.owner_id=? AND e.sequence<=? AND e.sequence<? ORDER BY e.sequence DESC LIMIT ?",(owner_id,high,before,limit+1)).fetchall()
        items=tuple(self._execution(row) for row in rows[:limit])
        next_cursor=self._encode({**binding,"high":high,"before":items[-1].sequence}) if len(rows)>limit else None
        return ScreenHistoryPage(owner_scope_tag=tag,items=items,next_cursor=next_cursor)

    def detail(self, owner_id: str, execution_id: str) -> ScreenQueryExecution | None:
        self.scope_tag(owner_id);self._assert_private_database()
        with closing(self.outbox._connect()) as connection:
            row=connection.execute("SELECT e.*,c.status AS command_status FROM screen_query_execution e JOIN page_control_command c ON c.command_id=e.execution_id WHERE e.owner_id=? AND e.execution_id=?",(owner_id,execution_id)).fetchone()
        return None if row is None else self._execution(row)

    def _read_artifact(self, digest: str) -> bytes:
        from rquant.page_control import _bind_managed_directory

        if len(digest)!=64 or any(c not in "0123456789abcdef" for c in digest): raise ValueError("invalid artifact identity")
        bound=_bind_managed_directory(self.artifact_root,create=False)
        try:
            fd=os.open(digest+".json",os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW,dir_fd=bound.descriptor)
            try:
                node=os.fstat(fd)
                if not stat.S_ISREG(node.st_mode) or node.st_uid!=os.geteuid() or stat.S_IMODE(node.st_mode)!=0o600 or node.st_nlink!=1 or node.st_size>MAX_ARTIFACT_BYTES:
                    raise ValueError("invalid private result artifact")
                chunks=[];size=0
                while chunk:=os.read(fd,1024*1024):
                    size+=len(chunk)
                    if size>MAX_ARTIFACT_BYTES: raise ValueError("result artifact too large")
                    chunks.append(chunk)
                after=os.fstat(fd)
                if (node.st_dev,node.st_ino,node.st_size,node.st_mtime_ns,node.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns): raise ValueError("result artifact changed")
                bound.verify();payload=b"".join(chunks)
                if hashlib.sha256(payload).hexdigest()!=digest: raise ValueError("result artifact hash changed")
                return payload
            finally: os.close(fd)
        finally: bound.close()

    def _persist_artifact(self, data: ScreenRunData) -> str:
        from rquant.page_control import _bind_managed_directory

        payload = data.model_dump_json().encode()
        if len(payload) > MAX_ARTIFACT_BYTES or len(data.rows) > MAX_RESULT_ROWS:
            raise ValueError("complete result exceeds artifact budget")
        digest = hashlib.sha256(payload).hexdigest()
        try:
            original = self._read_artifact(digest)
        except FileNotFoundError:
            self.ensure_capacity()
        else:
            if original != payload:
                raise ValueError("result artifact conflicts")
            return digest
        bound = _bind_managed_directory(self.artifact_root, create=False)
        try:
            try:
                fd = os.open(
                    digest + ".json",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=bound.descriptor,
                )
            except FileExistsError:
                if self._read_artifact(digest) != payload:
                    raise ValueError("result artifact conflicts")
                return digest
            try:
                remaining = memoryview(payload)
                while remaining:
                    written = os.write(fd, remaining)
                    if written <= 0:
                        raise OSError("incomplete result artifact write")
                    remaining = remaining[written:]
                os.fsync(fd)
                bound.verify()
                os.fsync(bound.descriptor)
            finally:
                os.close(fd)
        finally:
            bound.close()
        self._read_artifact(digest)
        return digest

    def results(self, owner_id: str, execution_id: str, *, limit: int=20, cursor: str | None=None) -> ScreenExecutionResults | None:
        if type(limit) is not int or not 1<=limit<=100: raise ValueError("invalid result page size")
        facts=self.detail(owner_id,execution_id)
        if facts is None or facts.status!="succeeded" or facts.artifact_sha256 is None: return None
        data=ScreenRunData.model_validate_json(self._read_artifact(facts.artifact_sha256))
        binding={"schema":1,"kind":"results","owner":self.scope_tag(owner_id),"execution":execution_id,"artifact":facts.artifact_sha256}
        offset=0 if cursor is None else self._decode(cursor,binding).get("offset")
        if type(offset) is not int or not 0<=offset<=len(data.rows): raise ValueError("invalid result offset")
        next_offset=offset+limit
        return ScreenExecutionResults(execution_id=execution_id,artifact_sha256=facts.artifact_sha256,rows=tuple(data.rows[offset:next_offset]),next_cursor=self._encode({**binding,"offset":next_offset}) if next_offset<len(data.rows) else None)

    def presets(self, owner_id: str) -> tuple[ScreenQueryPreset,...]:
        self.scope_tag(owner_id);self._assert_private_database()
        with closing(self.outbox._connect()) as connection:
            rows=connection.execute("SELECT body_json FROM screen_query_preset WHERE owner_id=? ORDER BY name_key,preset_id",(owner_id,)).fetchall()
        return tuple(ScreenQueryPreset.model_validate_json(row[0]) for row in rows)

    def complete(self, claim: PageControlClaim, *, now: datetime, data: ScreenRunData | None=None, failure_code: str | None=None) -> PageControlReceipt:
        from rquant.page_control import _COMMAND_ADAPTER, _OwnedSaveNlPreset, _OwnedAppendNlQueryLog, _command_hash, PageControlStatus

        command=claim.command
        if type(command) not in (_OwnedExecuteScreenQuery,_OwnedSaveNlPreset): raise TypeError("screen completion requires an exact trusted claim")
        self._assert_private_database();observed=normalize_aware_utc(now);stamp=observed.isoformat(timespec="microseconds");digest=_command_hash(command)
        result_digest=None
        if type(command) is _OwnedExecuteScreenQuery and data is not None and data.status=="ready" and failure_code is None:
            expected=data.ranked_count if command.definition.ranking else data.total
            if data.source is None or data.source.identity!=command.definition.source_identity or data.trade_date!=command.definition.trade_date or data.source.updated_at>observed or data.next_cursor is not None or len(data.rows)!=expected or len({r.ts_code for r in data.rows})!=len(data.rows):
                raise ValueError("complete screen result/source does not match command")
            result_digest=self._persist_artifact(data)
        with closing(self.outbox._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row=connection.execute("SELECT * FROM page_control_command WHERE command_id=?",(command.command_id,)).fetchone()
            if row is None or row["command_hash"]!=digest or row["command_kind"]!=command.kind or _COMMAND_ADAPTER.validate_json(row["payload_json"])!=command: raise ValueError("screen command content changed")
            if row["status"]!="processing" or row["processing_owner"]!=claim.owner_id or row["claim_token"]!=claim.claim_token or row["lease_expires_at"] is None or row["lease_expires_at"]<=stamp: raise RuntimeError("stale screen claim cannot complete")
            code=failure_code
            if command.requested_at>observed+timedelta(minutes=5): code="future_request"
            if type(command) is _OwnedExecuteScreenQuery:
                registered=connection.execute("SELECT * FROM screen_query_execution WHERE owner_id=? AND execution_id=?",(command.owner_id,command.command_id)).fetchone()
                if registered is None or registered["command_hash"]!=digest: raise ValueError("screen execution was not registered")
                if result_digest is None and code is None: code="source_unavailable"
                successful=code is None
                entry=ScreenQueryExecution(owner_id=command.owner_id,execution_id=command.command_id,sequence=registered["sequence"],command_hash=digest,plan_hash=command.definition.normalized_plan_sha256,definition=command.definition,original_command=ExecuteScreenQuery.model_validate_json(registered["original_command_json"]),source=data.source if successful else None,started_at=registered["started_at"],completed_at=observed,status="succeeded" if successful else ("source_expired" if code=="source_expired" else "failed"),base_count=data.base_count if successful else None,total=data.total if successful else None,unknown_count=data.unknown_count if successful else None,ranked_count=data.ranked_count if successful else None,steps=tuple(data.steps) if successful else (),artifact_sha256=result_digest if successful else None,member_rank_sha256=canonical_sha256(data.rows) if successful else None,failure_code=code)
                # The legacy-shaped log is generated from the actual result, never browser counts.
                log=_OwnedAppendNlQueryLog(command_id=command.command_id,requested_at=command.requested_at,owner_id=command.owner_id,query=command.definition.description or "条件选股",plan=command.definition.model_dump(mode="json"),outcome="success" if successful else "error",error=code)
                connection.execute("UPDATE screen_query_execution SET facts_json=?,log_json=? WHERE owner_id=? AND execution_id=?",(entry.model_dump_json(),log.model_dump_json(),command.owner_id,command.command_id))
                result={"execution_id":command.command_id,"code":code or "executed","artifact_sha256":entry.artifact_sha256,"member_rank_sha256":entry.member_rank_sha256,"log_sha256":canonical_sha256(log)}
            else:
                definition=command.definition
                existing=connection.execute("SELECT body_json FROM screen_query_preset WHERE owner_id=? AND preset_id=?",(command.owner_id,definition.preset_id)).fetchone()
                current=None if existing is None else ScreenQueryPreset.model_validate_json(existing[0])
                name_key=unicodedata.normalize("NFC",definition.name).strip().casefold()
                conflict=connection.execute("SELECT preset_id FROM screen_query_preset WHERE owner_id=? AND name_key=?",(command.owner_id,name_key)).fetchone()
                if (None if current is None else current.version)!=command.expected_version or (conflict and conflict[0]!=definition.preset_id): code="version_conflict"
                version=1 if current is None else current.version+1
                if code is None:
                    saved=ScreenQueryPreset(**definition.model_dump(),version=version,updated_at=observed,command_hash=digest)
                    connection.execute("INSERT INTO screen_query_preset(owner_id,preset_id,name_key,body_json) VALUES (?,?,?,?) ON CONFLICT(owner_id,preset_id) DO UPDATE SET name_key=excluded.name_key,body_json=excluded.body_json",(command.owner_id,definition.preset_id,name_key,saved.model_dump_json()))
                result={"preset_id":definition.preset_id,"version":version if code is None else None,"code":code or "saved"}
            status=PageControlStatus.SUCCEEDED if code is None else PageControlStatus.FAILED
            result_json=json.dumps(result,ensure_ascii=True)
            connection.execute("INSERT INTO page_control_effect(command_id,command_hash,effect_kind,status,owner_id,claim_token,started_at,completed_at,result_json,error) VALUES (?,?,?,?,?,?,?,?,?,?)",(command.command_id,digest,command.kind,status.value,claim.owner_id,claim.claim_token,stamp,stamp,result_json,code))
            changed=connection.execute("UPDATE page_control_command SET status=?,completed_at=?,result_json=?,error=?,processing_owner=NULL,lease_expires_at=NULL,claim_token=NULL WHERE command_id=? AND status='processing' AND processing_owner=? AND claim_token=? AND lease_expires_at>?",(status.value,stamp,result_json,code,command.command_id,claim.owner_id,claim.claim_token,stamp)).rowcount
            if changed!=1: raise RuntimeError("screen claim changed during completion")
            completed=connection.execute("SELECT * FROM page_control_command WHERE command_id=?",(command.command_id,)).fetchone()
            return self.outbox._receipt(completed)
