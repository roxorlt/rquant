"""Peer-verified bounded private endpoints for computation or PageControl saves."""

from __future__ import annotations

import json
import os
import socket
import socketserver
import stat
import threading
from collections.abc import Callable
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Literal

from pydantic import Field

from rquant.factor_definition_admission import (
    _assert_endpoint,
    _peer_uid,
    _socket_identity,
    _UnixHTTPConnection,
    _unlink_matching_socket,
    _validate_socket_path,
)
from rquant.page_control import (
    PageControlCommandConflictError,
    PageControlReceipt,
    PageControlService,
    SaveResearchQuery,
)
from rquant.strict_json import strict_json_loads

from .contracts import MAX_RESULT_BYTES, QueryColumn, QueryModel, QueryRequest, QueryResult
from .executor import QueryExecutor
from .saved import SavedResearchQuery
from .snapshot import PUBLIC_SCHEMA

MAX_REQUEST_BYTES = 256 * 1024


class QueryAdmissionRejectedError(ValueError):
    pass


class QueryAdmissionUnavailableError(RuntimeError):
    pass


class QueryCatalogColumn(QueryColumn):
    description: str


_COLUMN_DESCRIPTIONS = {
    "ts_code": "股票代码，如 600001.SH",
    "trade_date": "行情交易日",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "vol": "成交量，沿用来源单位",
    "amount": "成交额，沿用来源单位",
    "adj_factor": "当日复权因子",
    "exchange": "交易所，如 SSE、SZSE",
    "cal_date": "日历日期",
    "is_open": "是否为交易日，true 为开市",
    "pretrade_date": "上一个交易日",
}


class QueryTableInfo(QueryModel):
    name: str
    label: str
    columns: tuple[QueryCatalogColumn, ...]
    row_count: int
    earliest_date: str | None = None
    latest_date: str | None = None


class QueryCatalogData(QueryModel):
    available: bool
    source_at: datetime | None = None
    tables: tuple[QueryTableInfo, ...] = ()
    save_enabled: bool = False
    message: str = ""


class QuerySavedList(QueryModel):
    available: bool
    items: tuple[SavedResearchQuery, ...] = ()
    message: str = ""


class QuerySaveData(QueryModel):
    receipt: PageControlReceipt | None = None
    message: str = ""


class _PrivateRequest(QueryModel):
    authenticated_actor_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    action: Literal["catalog", "query", "saved", "save", "resume"]
    query: QueryRequest | None = None
    command: SaveResearchQuery | None = None


class QueryPrivateServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = False

    def __init__(
        self,
        socket_path: Path,
        *,
        allowed_users: frozenset[str],
        trusted_web_uid: int,
        shared_gid: int,
        executor: QueryExecutor | None = None,
        control: PageControlService | None = None,
    ) -> None:
        if (
            (executor is None) == (control is None)
            or not allowed_users
            or len(allowed_users) > 16
            or trusted_web_uid < 0
            or shared_gid < 0
        ):
            raise ValueError("one explicit query or save role and trusted users are required")
        _validate_socket_path(socket_path)
        parent = socket_path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or parent.st_gid != shared_gid
            or stat.S_IMODE(parent.st_mode) != 0o710
        ):
            raise ValueError("private query directory permissions are invalid")
        self.socket_path = socket_path
        self.allowed_users = allowed_users
        self.trusted_web_uid = trusted_web_uid
        self.executor, self.control = executor, control
        self._handler_slots = threading.BoundedSemaphore(10)
        self._bound_identity = None
        super().__init__(str(socket_path), _Handler, bind_and_activate=False)
        try:
            self.server_bind()
            self._bound_identity = _socket_identity(socket_path)
            os.chown(socket_path, os.geteuid(), shared_gid, follow_symlinks=False)
            os.chmod(socket_path, 0o660, follow_symlinks=False)
            if _assert_endpoint(socket_path, os.geteuid(), shared_gid)[2:] != self._bound_identity:
                raise ValueError("private endpoint changed during bind")
            self.server_activate()
        except BaseException:
            self.server_close()
            raise

    def verify_request(self, request: socket.socket, client_address: object) -> bool:
        try:
            return _peer_uid(request) == self.trusted_web_uid
        except Exception:
            return False

    def process_request(self, request: socket.socket, client_address: object) -> None:
        if not self._handler_slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.0 503 Busy\r\nContent-Length: 2\r\n\r\n{}")
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._handler_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: object) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._handler_slots.release()

    def server_close(self) -> None:
        super().server_close()
        _unlink_matching_socket(self.socket_path, self._bound_identity)

    def dispatch(self, message: _PrivateRequest) -> QueryModel:
        if message.authenticated_actor_id not in self.allowed_users:
            raise QueryAdmissionRejectedError("你没有查询权限。")
        if self.executor is not None:
            if message.command is not None:
                raise QueryAdmissionRejectedError("请求不属于查询服务。")
            if message.action == "query" and message.query is not None:
                return self.executor.execute(message.query)
            if message.action == "catalog" and message.query is None:
                self.executor.snapshot.verify_current()
                manifest = self.executor.snapshot.manifest
                labels = {
                    "daily_bar": "日线行情",
                    "adj_factor": "复权因子",
                    "trade_calendar": "交易日历",
                }
                return QueryCatalogData(
                    available=True,
                    source_at=manifest.source_at,
                    tables=tuple(
                        QueryTableInfo(
                            name=table,
                            label=labels[table],
                            columns=tuple(
                                QueryCatalogColumn(
                                    name=name,
                                    data_type=kind,
                                    description=_COLUMN_DESCRIPTIONS[name],
                                )
                                for name, kind in columns
                            ),
                            row_count=manifest.row_counts[table],
                            earliest_date=manifest.date_ranges[table][0],
                            latest_date=manifest.date_ranges[table][1],
                        )
                        for table, columns in PUBLIC_SCHEMA.items()
                    ),
                )
        if self.control is not None and message.query is None:
            if message.action == "saved" and message.command is None:
                return QuerySavedList(
                    available=True,
                    items=self.control.outbox.list_research_queries(message.authenticated_actor_id),
                )
            if message.action in {"save", "resume"} and message.command is not None:
                operation = (
                    self.control._submit_trusted_research_query
                    if message.action == "save"
                    else self.control._resume_trusted_research_query
                )
                receipt = operation(
                    message.command, authenticated_actor_id=message.authenticated_actor_id
                )
                return QuerySaveData(
                    receipt=receipt, message="未找到原保存命令。" if receipt is None else ""
                )
        raise QueryAdmissionRejectedError("请求不属于此服务。")


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.connection.settimeout(35)
        lengths = self.headers.get_all("Content-Length", [])
        if (
            self.path != "/v1/research"
            or self.headers.get("Transfer-Encoding")
            or self.headers.get("Content-Type") != "application/json"
            or len(lengths) != 1
            or not lengths[0].isdigit()
            or not 1 <= int(lengths[0]) <= MAX_REQUEST_BYTES
        ):
            self._json(400, {"error": "请求格式有误。"})
            return
        try:
            message = _PrivateRequest.model_validate(
                strict_json_loads(self.rfile.read(int(lengths[0])))
            )
            result = self.server.dispatch(message)
            self._json(200, {"data": result.model_dump(mode="json")})
        except (ValueError, PageControlCommandConflictError):
            self._json(403, {"error": "查询身份或原命令不匹配，请检查后重试。"})
        except Exception:
            self._json(503, {"error": "查询服务暂时不可用，请稍后重试。"})

    def _json(self, status: int, value: object) -> None:
        body = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
            "utf-8"
        )
        if len(body) > MAX_RESULT_BYTES:
            status, body = 503, b'{"error":"result limit"}'
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class QueryPrivateClient:
    def __init__(
        self,
        socket_path: Path,
        *,
        expected_service_uid: int,
        shared_gid: int,
        client_uid: Callable[[], int] = os.geteuid,
    ) -> None:
        _validate_socket_path(socket_path)
        if expected_service_uid < 0 or shared_gid < 0 or expected_service_uid == client_uid():
            raise ValueError("query service must have a distinct private identity")
        self.socket_path, self.service_uid, self.shared_gid, self.client_uid = (
            socket_path,
            expected_service_uid,
            shared_gid,
            client_uid,
        )

    def _request(self, message: _PrivateRequest, model: type[QueryModel]) -> QueryModel:
        body = message.model_dump_json(exclude_none=True).encode("utf-8")
        if len(body) > MAX_REQUEST_BYTES:
            raise QueryAdmissionRejectedError("查询内容过长。")
        connection = _UnixHTTPConnection(
            self.socket_path,
            timeout_seconds=35,
            expected_service_uid=self.service_uid,
            shared_gid=self.shared_gid,
            client_uid=self.client_uid,
        )
        try:
            connection.request(
                "POST", "/v1/research", body, headers={"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            sizes = response.headers.get_all("Content-Length", [])
            if (
                len(sizes) != 1
                or not sizes[0].isdigit()
                or not 1 <= int(sizes[0]) <= MAX_RESULT_BYTES
            ):
                raise QueryAdmissionUnavailableError("查询响应未通过核验。")
            raw = response.read(int(sizes[0]) + 1)
            if len(raw) != int(sizes[0]):
                raise QueryAdmissionUnavailableError("查询响应未通过核验。")
            if response.status == 403:
                raise QueryAdmissionRejectedError("查询身份或原命令不匹配。")
            if response.status != 200:
                raise QueryAdmissionUnavailableError("查询服务暂时不可用。")
            payload = strict_json_loads(raw)
            if not isinstance(payload, dict) or set(payload) != {"data"}:
                raise QueryAdmissionUnavailableError("查询响应未通过核验。")
            return model.model_validate(payload["data"])
        except QueryAdmissionRejectedError:
            raise
        except Exception as exc:
            raise QueryAdmissionUnavailableError("查询服务暂时不可用。") from exc
        finally:
            connection.close()

    def catalog(self, *, authenticated_actor_id: str) -> QueryCatalogData:
        return self._request(
            _PrivateRequest(authenticated_actor_id=authenticated_actor_id, action="catalog"),
            QueryCatalogData,
        )

    def execute(self, query: QueryRequest, *, authenticated_actor_id: str) -> QueryResult:
        return self._request(
            _PrivateRequest(
                authenticated_actor_id=authenticated_actor_id, action="query", query=query
            ),
            QueryResult,
        )

    def saved(self, *, authenticated_actor_id: str) -> QuerySavedList:
        return self._request(
            _PrivateRequest(authenticated_actor_id=authenticated_actor_id, action="saved"),
            QuerySavedList,
        )

    def save(
        self, command: SaveResearchQuery, *, authenticated_actor_id: str, resume: bool = False
    ) -> QuerySaveData:
        return self._request(
            _PrivateRequest(
                authenticated_actor_id=authenticated_actor_id,
                action="resume" if resume else "save",
                command=command,
            ),
            QuerySaveData,
        )
