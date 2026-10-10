"""Private screening protocol using the existing peer and endpoint boundary."""

from __future__ import annotations

import json
import os
import secrets
import stat
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from pydantic import Field, StrictInt, field_validator, model_validator

from rquant.factor_definition_admission import _UnixHTTPConnection, _validate_socket_path
from rquant.manual_watchlist import OwnerId
from rquant.page_control import PageControlCommandConflictError, PageControlService
from rquant.pool_result_receipt import DailyWriterCapability, PublishedDailyScreenEvidence
from rquant.research_query.service import QueryPrivateServer
from rquant.runtime_contracts import RuntimeContractModel
from rquant.screen.query_contracts import ScreenQueryDefinition
from rquant.web.models.screen import ScreenRunData
from rquant.strict_json import strict_json_loads
from rquant.web.models.screen_history import (
    ScreenExecutionAction, ScreenExecutionView, ScreenExecuteAction, ScreenHistoryAction,
    ScreenHistoryView, ScreenLookupAction, ScreenPresetSaveAction, ScreenPresetsAction,
    ScreenQueryAction, ScreenQueryReadData, ScreenResultsAction, ScreenResumeAction,
)

MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class ScreenQueryPrivateConfig(RuntimeContractModel):
    socket_path: Path
    trusted_web_uid: StrictInt = Field(ge=0)
    shared_gid: StrictInt = Field(ge=0)
    allowed_users: frozenset[OwnerId] = Field(min_length=1, max_length=16)
    serving_root: Path
    primary_path: Path | None = None
    replica_path: Path | None = None
    history_root: Path | None = None
    rsi_root: Path | None = None
    stale_after_seconds: float = Field(default=600, gt=0, allow_inf_nan=False)

    @field_validator("socket_path", "serving_root", "primary_path", "replica_path", "history_root", "rsi_root")
    @classmethod
    def canonical_path(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or Path(os.path.abspath(value))!=value):
            raise ValueError("screen paths must be canonical and absolute")
        return value

    @model_validator(mode="after")
    def verify_role(self) -> ScreenQueryPrivateConfig:
        _validate_socket_path(self.socket_path)
        if self.trusted_web_uid==os.geteuid():
            raise ValueError("private screening requires a distinct Web UID")
        if (self.primary_path is None)!=(self.replica_path is None) or (self.rsi_root is not None and self.replica_path is None):
            raise ValueError("screen source descriptors are incomplete")
        return self


def load_screen_query_private_config(path: Path) -> ScreenQueryPrivateConfig:
    from rquant.page_control import _read_managed_file

    node=path.lstat()
    if node.st_uid!=os.geteuid() or stat.S_IMODE(node.st_mode)!=0o600 or not stat.S_ISREG(node.st_mode) or node.st_nlink!=1:
        raise ValueError("screen private config permissions are invalid")
    body=_read_managed_file(path)
    if len(body)>MAX_REQUEST_BYTES: raise ValueError("screen private config exceeds its budget")
    return ScreenQueryPrivateConfig.model_validate(strict_json_loads(body))


class ScreenQueryExecutor:
    """Hold the original source and call the existing application's complete path."""

    def __init__(self, config: ScreenQueryPrivateConfig, *, clock: Callable[[],datetime] | None=None) -> None:
        from rquant.screen.dynamic_rsi import VerifiedDynamicRsiProjection
        from rquant.screen.formula_history_projection import VerifiedFormulaHistoryProjection
        from rquant.screen.replica_source import VerifiedReplicaScreenSource
        from rquant.web.screen_service import ScreenApplicationService
        from rquant.web.serving import GenerationTracker

        self.clock=clock or (lambda:datetime.now(UTC))
        self.stale_after=timedelta(seconds=config.stale_after_seconds)
        self.tracker=GenerationTracker(config.serving_root)
        self.service=ScreenApplicationService(cursor_key=secrets.token_bytes(32),replica=None if config.replica_path is None else VerifiedReplicaScreenSource(primary_path=config.primary_path,replica_path=config.replica_path),history=None if config.history_root is None else VerifiedFormulaHistoryProjection(config.history_root),rsi=None if config.rsi_root is None else VerifiedDynamicRsiProjection(config.rsi_root),clock=self.clock)

    @property
    def cursor_key(self) -> bytes:
        return self.service.cursor_key

    def __call__(self, definition: ScreenQueryDefinition) -> ScreenRunData:
        from rquant.web.serving import serving_meta

        with self.tracker.borrow() as borrowed:
            meta=serving_meta(borrowed,now=self.clock(),stale_after=self.stale_after,failure=self.tracker.failure)
            return self.service.run_complete(definition,borrowed=borrowed,serving_unavailable=meta.state=="unavailable")

    def daily_run_evidence(self) -> tuple[PublishedDailyScreenEvidence, ...]:
        from rquant.web.serving import serving_meta
        from rquant.web.readers import table_states
        columns = tuple(PublishedDailyScreenEvidence.model_fields)
        with self.tracker.borrow() as borrowed:
            meta = serving_meta(borrowed,now=self.clock(),stale_after=self.stale_after,failure=self.tracker.failure)
            if meta.state != "ready" or borrowed.cursor is None:
                return ()
            status = table_states(borrowed.cursor).get("screen_run_evidence")
            if status is None or not status.available:
                return ()
            rows = borrowed.cursor.execute(
                f"SELECT {','.join(columns)} FROM screen_run_evidence ORDER BY completed_at DESC,preset_name LIMIT 513"
            ).fetchall()
            if len(rows)>512:
                raise ValueError("daily result evidence exceeds bound")
            proofs = tuple(PublishedDailyScreenEvidence.model_validate(dict(zip(columns,row,strict=True))) for row in rows)
            if any(item.completed_at>self.clock() or item.decision_at>item.completed_at for item in proofs):
                raise ValueError("daily result evidence is not visible")
            return proofs

    def daily_writer_capability(self) -> DailyWriterCapability | None:
        from rquant.screen.daily_inputs import daily_writer_contract_fingerprint
        from rquant.web.serving import serving_meta
        expected = daily_writer_contract_fingerprint()
        proofs = self.daily_run_evidence()
        matching = next((item for item in proofs if item.writer_contract_fingerprint==expected
            and item.canonical_receipt_id is not None and item.canonical_generation_id is not None
            and item.source_generation_id is not None),None)
        if matching is None:
            return None
        with self.tracker.borrow() as borrowed:
            meta=serving_meta(borrowed,now=self.clock(),stale_after=self.stale_after,failure=self.tracker.failure)
            if meta.state != "ready" or meta.generation_id is None:
                return None
            # The second borrow must still contain the exact published writer proof.
            row = borrowed.cursor.execute("SELECT evidence_version FROM screen_run_evidence WHERE preset_name=? AND result_version=?",[matching.preset_name,matching.result_version]).fetchone()
            if row != (matching.evidence_version,):
                return None
            return DailyWriterCapability(serving_generation_id=meta.generation_id,writer_contract_fingerprint=expected,
                verified_result_version=matching.result_version,verified_evidence_version=matching.evidence_version,completed_at=matching.completed_at,
                canonical_receipt_id=matching.canonical_receipt_id,canonical_generation_id=matching.canonical_generation_id,
                source_generation_id=matching.source_generation_id)


class ScreenQueryAdmissionRejectedError(ValueError):
    pass


class ScreenQueryAdmissionUnavailableError(RuntimeError):
    pass


class _PrivateRequest(RuntimeContractModel):
    authenticated_actor_id: OwnerId
    request: ScreenQueryAction


def dispatch_screen_query_action(control: PageControlService, *, authenticated_actor_id: str, allowed_users: frozenset[str], action: ScreenQueryAction) -> ScreenQueryReadData:
    if authenticated_actor_id not in allowed_users:
        raise ScreenQueryAdmissionRejectedError("你没有选股权限。")
    history=control.consumer.screen_query_history
    if history is None:
        raise ScreenQueryAdmissionUnavailableError("选股历史暂不可用。")
    fields={"owner_scope_tag":history.scope_tag(authenticated_actor_id)}
    from rquant.screen.alert_draft import create_screen_alert_draft,read_screen_alert_draft
    from rquant.web.models.screen_alert_draft import ScreenAlertDraftCreateAction,ScreenAlertDraftReadAction
    if type(action) is ScreenAlertDraftCreateAction:
        try:
            draft=create_screen_alert_draft(history,owner_id=authenticated_actor_id,request=action.request,now=control.consumer.clock())
        except ValueError as error:
            raise ScreenQueryAdmissionRejectedError("原结果或草稿请求尚未确认。") from error
        return ScreenQueryReadData(**fields,alert_draft=draft)
    if type(action) is ScreenAlertDraftReadAction:
        return ScreenQueryReadData(**fields,alert_draft=read_screen_alert_draft(history,owner_id=authenticated_actor_id,draft_id=action.draft_id,now=control.consumer.clock()))
    if type(action) is ScreenHistoryAction:
        page=history.history(authenticated_actor_id,limit=action.limit,cursor=action.cursor)
        capability=None
        daily=()
        try:
            capability=control.consumer.trusted_daily_writer_capability()
            if control.consumer.daily_run_evidence is not None:
                daily=control.consumer.daily_run_evidence()
        except (OSError,ValueError,RuntimeError):
            capability=None
            daily=()
        return ScreenQueryReadData(**fields,history=ScreenHistoryView.model_validate(page.model_dump()),
            daily_writer_capability=capability,daily_run_evidence=daily)
    if type(action) is ScreenPresetsAction:
        return ScreenQueryReadData(**fields,presets=history.presets(authenticated_actor_id))
    if type(action) is ScreenExecutionAction:
        execution=history.detail(authenticated_actor_id,action.execution_id)
        return ScreenQueryReadData(**fields,execution=None if execution is None else ScreenExecutionView.model_validate(execution.model_dump()))
    if type(action) is ScreenResultsAction:
        return ScreenQueryReadData(**fields,results=history.results(authenticated_actor_id,action.execution_id,limit=action.limit,cursor=action.cursor))
    original=action.original if type(action) in (ScreenLookupAction,ScreenResumeAction) else action
    context={"authenticated_actor_id":authenticated_actor_id}
    if type(original) is ScreenExecuteAction:
        command=original.command
    elif type(original) is ScreenPresetSaveAction:
        command=original.request.legacy_command()
        context.update(definition=original.request.preset,expected_version=original.request.expected_version)
    else:
        raise ScreenQueryAdmissionRejectedError("选股请求有误。")
    operation=control._lookup_trusted_screen_query if type(action) is ScreenLookupAction else control._resume_trusted_screen_query if type(action) is ScreenResumeAction else control._submit_trusted_screen_query
    return ScreenQueryReadData(**fields,receipt=operation(command,**context))


class ScreenQueryPrivateServer(QueryPrivateServer):
    def __init__(self, socket_path: Path, *, allowed_users: frozenset[str], trusted_web_uid: int, shared_gid: int, control: PageControlService) -> None:
        if trusted_web_uid == os.geteuid():
            raise ValueError("screen service requires a distinct trusted Web UID")
        if control is None or control.consumer.screen_query_history is None:
            raise ValueError("private screen authority is not configured")
        super().__init__(socket_path,allowed_users=allowed_users,trusted_web_uid=trusted_web_uid,shared_gid=shared_gid,control=control)
        self.RequestHandlerClass=_Handler

    def dispatch(self, message: _PrivateRequest) -> ScreenQueryReadData:
        return dispatch_screen_query_action(self.control,authenticated_actor_id=message.authenticated_actor_id,allowed_users=self.allowed_users,action=message.request)


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.connection.settimeout(35)
        lengths=self.headers.get_all("Content-Length",[])
        if self.path!="/v1/screen-query" or self.headers.get("Transfer-Encoding") or self.headers.get("Content-Type")!="application/json" or len(lengths)!=1 or not lengths[0].isdigit() or not 1<=int(lengths[0])<=MAX_REQUEST_BYTES:
            self._json(422,{"error":"选股请求有误。"});return
        try:
            message=_PrivateRequest.model_validate(strict_json_loads(self.rfile.read(int(lengths[0]))))
            data=self.server.dispatch(message)
            self._json(200,{"data":data.model_dump(mode="json")})
        except PageControlCommandConflictError:
            self._json(409,{"error":"原请求已改变，请重新运行。"})
        except ScreenQueryAdmissionRejectedError:
            self._json(403,{"error":"你没有选股权限。"})
        except (ValueError,TypeError):
            self._json(422,{"error":"选股请求有误。"})
        except Exception:
            self._json(503,{"error":"选股历史暂不可用。"})

    def _json(self, status: int, value: object) -> None:
        body=json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(",",":")).encode()
        if len(body)>MAX_RESPONSE_BYTES: status,body=503,b'{"error":"unavailable"}'
        self.send_response(status);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(body)));self.send_header("Connection","close");self.end_headers();self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class ScreenQueryPrivateClient:
    def __init__(self, socket_path: Path, *, expected_service_uid: int, shared_gid: int, client_uid: Callable[[],int]=os.geteuid) -> None:
        _validate_socket_path(socket_path)
        if expected_service_uid<0 or shared_gid<0 or expected_service_uid==client_uid():
            raise ValueError("screen service requires a distinct private identity")
        self.socket_path,self.service_uid,self.shared_gid,self.client_uid=socket_path,expected_service_uid,shared_gid,client_uid

    def request(self, action: ScreenQueryAction, *, authenticated_actor_id: str) -> ScreenQueryReadData:
        message=_PrivateRequest(authenticated_actor_id=authenticated_actor_id,request=action)
        body=message.model_dump_json().encode()
        if len(body)>MAX_REQUEST_BYTES: raise ScreenQueryAdmissionRejectedError("条件太多，请减少后重试。")
        connection=_UnixHTTPConnection(self.socket_path,timeout_seconds=35,expected_service_uid=self.service_uid,shared_gid=self.shared_gid,client_uid=self.client_uid)
        try:
            connection.request("POST","/v1/screen-query",body,headers={"Content-Type":"application/json"})
            response=connection.getresponse();sizes=response.headers.get_all("Content-Length",[])
            if len(sizes)!=1 or not sizes[0].isdigit() or not 1<=int(sizes[0])<=MAX_RESPONSE_BYTES:
                raise ScreenQueryAdmissionUnavailableError("选股响应未通过核验。")
            raw=response.read(int(sizes[0])+1)
            if len(raw)!=int(sizes[0]): raise ScreenQueryAdmissionUnavailableError("选股响应未通过核验。")
            if response.status==409: raise PageControlCommandConflictError("原请求已改变，请重新运行。")
            if response.status in (403,422): raise ScreenQueryAdmissionRejectedError("选股请求或身份不匹配。")
            if response.status!=200: raise ScreenQueryAdmissionUnavailableError("选股历史暂不可用。")
            payload=strict_json_loads(raw)
            if not isinstance(payload,dict) or set(payload)!={"data"}: raise ScreenQueryAdmissionUnavailableError("选股响应未通过核验。")
            return ScreenQueryReadData.model_validate(payload["data"])
        except (ScreenQueryAdmissionRejectedError,PageControlCommandConflictError):
            raise
        except Exception as error:
            raise ScreenQueryAdmissionUnavailableError("选股历史暂不可用。") from error
        finally:
            connection.close()
