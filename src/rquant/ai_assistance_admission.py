"""AI is an original PageControl-owned private endpoint with the existing peer checks."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Callable
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Self, TYPE_CHECKING

from pydantic import Field, SecretStr, StrictInt, field_validator, model_validator

from rquant.ai_assistance import AIAccountConfig, AIAssistanceOwner
from rquant.ai_usage import AIBudgetExceeded, AIRequestConflict, AIRequestNotFound
from rquant.factor_definition_admission import _UnixHTTPConnection, _validate_socket_path
from rquant.manual_watchlist import OwnerId
from rquant.page_control import PageControlService, _read_managed_file
from rquant.research_query.service import QueryPrivateServer
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import strict_json_loads
from rquant.web.ai_assistance_gateway import AIAdmissionRejected, AIAdmissionUnavailable
from rquant.web.models.ai_assistance import AIAction, AIReadData

if TYPE_CHECKING:
    from rquant.ai_assistance import AIModelProvider, AIAssistanceContexts
    from rquant.page_control import PageControlOutbox
    from rquant.screen.query_admission import ScreenQueryExecutor

MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024


class AIPrivateConfig(AIAccountConfig):
    socket_path: Path
    trusted_web_uid: StrictInt = Field(ge=0)
    shared_gid: StrictInt = Field(ge=0)
    allowed_users: frozenset[OwnerId] = Field(min_length=1, max_length=16)
    api_key_file: Path | None = None
    news_artifact_root: Path | None = None
    result_jobs_path: Path | None = None
    result_artifact_root: Path | None = None
    experiment_registry_path: Path | None = None
    historical_profile_file: Path | None = None
    news_profile_file: Path | None = None

    @field_validator("socket_path", "api_key_file", "news_artifact_root", "result_jobs_path", "result_artifact_root", "experiment_registry_path", "historical_profile_file", "news_profile_file")
    @classmethod
    def exact_path(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or Path(os.path.abspath(value)) != value or value.name.startswith(".env")):
            raise ValueError("AI paths must be explicit canonical private paths")
        return value

    @model_validator(mode="after")
    def exact_role(self) -> Self:
        _validate_socket_path(self.socket_path)
        if self.trusted_web_uid == os.geteuid():
            raise ValueError("AI requires the original distinct trusted Web UID")
        if self.daily_limit > 0 and self.api_key_file is None:
            raise ValueError("enabled AI needs an explicit owner-private key file")
        if (self.result_jobs_path is None) != (self.result_artifact_root is None):
            raise ValueError("AI original result descriptors must be paired")
        if self.experiment_registry_path is not None and self.result_jobs_path is None:
            raise ValueError("AI private results require the original result reader")
        return self


def read_private_config(path: Path) -> AIPrivateConfig:
    node = path.lstat()
    if not stat.S_ISREG(node.st_mode) or node.st_uid != os.geteuid() or stat.S_IMODE(node.st_mode) != 0o600 or node.st_nlink != 1 or path.name.startswith(".env"):
        raise ValueError("AI config must be a private original-owner regular file")
    raw = _read_managed_file(path)
    if len(raw) > MAX_REQUEST_BYTES:
        raise ValueError("AI config exceeds its budget")
    return AIPrivateConfig.model_validate(strict_json_loads(raw))


def private_model_key(config: AIPrivateConfig) -> SecretStr | None:
    if config.daily_limit == 0:
        return None
    path = config.api_key_file
    node = path.lstat()
    if not stat.S_ISREG(node.st_mode) or node.st_uid != os.geteuid() or stat.S_IMODE(node.st_mode) != 0o600 or node.st_nlink != 1:
        raise ValueError("AI key must remain owner-private")
    raw = _read_managed_file(path)
    if not 1 <= len(raw) <= 4096:
        raise ValueError("AI key exceeds its budget")
    key = raw.decode().strip()
    if not key or any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError("AI key format is invalid")
    return SecretStr(key)


def build_ai_owner(config: AIPrivateConfig, *, outbox: PageControlOutbox,
                   screen: ScreenQueryExecutor, provider: AIModelProvider | None = None,
                   contexts: AIAssistanceContexts | None = None,
                   clock: Callable[[], datetime] | None = None) -> AIAssistanceOwner:
    from rquant.ai_assistance import AIAssistanceContexts
    from rquant.web.nl_parser import OpenAiScreenPlanParser
    if screen is None:
        raise ValueError("AI requires the installed original screen source")
    if contexts is None:
        portfolio, templates, private_results, news = None, None, None, None
        if config.result_jobs_path is not None:
            from rquant.lab_jobs import LabJobReader
            from rquant.portfolio_backtest_artifact import PortfolioResultReader
            from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
            from rquant.lab_artifact_preview import ArtifactPreviewReader
            reader = LabJobReader(config.result_jobs_path)
            portfolio = PortfolioResultReader(reader=reader, artifact_root=config.result_artifact_root)
            templates = StrategyTemplateSealedResultReader(reader=reader, artifact_reader=ArtifactPreviewReader(reader=reader, artifact_root=config.result_artifact_root))
            if config.experiment_registry_path is not None:
                from rquant.experiment_registry import ExperimentRegistryReadonlyReader
                from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
                private_results = ExperimentPrivateResultAuthority(ExperimentRegistryReadonlyReader(config.experiment_registry_path))
        if config.news_artifact_root is not None:
            from rquant.stock_news_sources import StockNewsArtifactStore
            news = StockNewsArtifactStore(config.news_artifact_root, outbox=outbox)
        contexts = AIAssistanceContexts(screen=screen, portfolio=portfolio, templates=templates,
            private_results=private_results, news=news, portfolio_owner=outbox.authorize_ai_portfolio_result)
    elif type(contexts) is not AIAssistanceContexts or contexts.screen is not screen:
        raise ValueError("AI context differs from the installed original source")
    if provider is None:
        key = private_model_key(config)
        if key is not None:
            provider = OpenAiScreenPlanParser(api_key=key, model=config.model_id)
    return AIAssistanceOwner(outbox=outbox, account=AIAccountConfig.model_validate(config.model_dump(include=set(AIAccountConfig.model_fields))), provider=provider, contexts=contexts, clock=clock)


class _PrivateRequest(RuntimeContractModel):
    authenticated_actor_id: OwnerId
    action: AIAction


class AIAssistanceAdmission:
    def __init__(self, *, owner: AIAssistanceOwner, allowed_users: frozenset[str]) -> None:
        if type(owner) is not AIAssistanceOwner or not allowed_users or len(allowed_users) > 16:
            raise ValueError("AI requires the original installed owner and exact users")
        self.owner, self.allowed_users = owner, allowed_users

    def dispatch(self, action: AIAction, *, authenticated_actor_id: str) -> AIReadData:
        if authenticated_actor_id not in self.allowed_users:
            raise AIAdmissionRejected("当前账号不能使用助手。")
        try:
            self.owner.require_current_role(authenticated_actor_id,
                write=action.operation in {"generate", "prepare_backtest", "confirm_backtest"})
        except PermissionError as exc:
            raise AIAdmissionRejected("当前角色不能执行此操作。") from exc
        if action.operation == "generate":
            return AIReadData(request=self.owner.generate(authenticated_actor_id, action.request))
        if action.operation == "lookup":
            return AIReadData(request=self.owner.lookup(authenticated_actor_id, action.original))
        if action.operation == "capabilities":
            return AIReadData(capabilities=self.owner.capabilities(authenticated_actor_id))
        if action.operation == "usage":
            return AIReadData(usage=self.owner.usage(authenticated_actor_id, action.start_date, action.end_date))
        if action.operation == 'stock_news':
            return AIReadData(stock_news=self.owner.stock_news(authenticated_actor_id,action.stock_code))
        if action.operation == 'read_interpretation':
            return AIReadData(interpretation=self.owner.interpretation(authenticated_actor_id,action.original))
        if action.operation in {"prepare_backtest", "lookup_backtest", "confirm_backtest"}:
            pipeline = self.owner.backtests
            if pipeline is None:
                raise AIAdmissionUnavailable("完整回测来源尚未准备。")
            if action.operation == "prepare_backtest":
                return AIReadData(preparation=pipeline.prepare(authenticated_actor_id, action.request))
            if action.operation == "lookup_backtest":
                return AIReadData(preparation=pipeline.lookup(authenticated_actor_id, action.original))
            if self.owner.control is None or self.owner.control.consumer.portfolio_backend is None:
                raise AIAdmissionUnavailable("回测提交暂不可用。")
            return AIReadData(confirmation=pipeline.confirm(authenticated_actor_id, action.request, control=self.owner.control))
        raise AIAdmissionRejected("助手请求有误。")


class AIAssistancePrivateServer(QueryPrivateServer):
    def __init__(self, config: AIPrivateConfig, *, control: PageControlService) -> None:
        if control.ai_assistance is None or control.ai_assistance.outbox is not control.outbox:
            raise ValueError("AI must use the same original PageControl outbox")
        self.admission = AIAssistanceAdmission(owner=control.ai_assistance, allowed_users=config.allowed_users)
        super().__init__(config.socket_path, allowed_users=config.allowed_users, trusted_web_uid=config.trusted_web_uid, shared_gid=config.shared_gid, control=control)
        self.RequestHandlerClass = _Handler

    def dispatch(self, message: _PrivateRequest) -> AIReadData:
        return self.admission.dispatch(message.action, authenticated_actor_id=message.authenticated_actor_id)


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        self.connection.settimeout(35)
        lengths = self.headers.get_all("Content-Length", [])
        if self.path != "/v1/ai-assistance" or self.headers.get("Transfer-Encoding") or self.headers.get("Content-Type") != "application/json" or len(lengths) != 1 or not lengths[0].isdigit() or not 1 <= int(lengths[0]) <= MAX_REQUEST_BYTES:
            self._json(422, {"error": "助手请求有误。"})
            return
        try:
            message = _PrivateRequest.model_validate(strict_json_loads(self.rfile.read(int(lengths[0]))))
            data = self.server.dispatch(message)
            self._json(200, {"data": data.model_dump(mode="json")})
        except AIRequestConflict:
            self._json(409, {"error": "原请求已改变，请新建请求。"})
        except AIRequestNotFound:
            self._json(404, {"error": "找不到原请求。"})
        except AIBudgetExceeded:
            self._json(429, {"error": "今日调用次数已用完，或调用尚未启用。"})
        except AIAdmissionRejected:
            self._json(403, {"error": "当前账号不能使用助手。"})
        except (ValueError, TypeError):
            self._json(409, {"error": "数据已更新或请求有误，请刷新后重试。"})
        except Exception:
            self._json(503, {"error": "助手暂不可用，请稍后查看原请求。"})

    def _json(self, status: int, value: object) -> None:
        body = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
        if len(body) > MAX_RESPONSE_BYTES:
            status, body = 503, b'{"error":"unavailable"}'
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class AIAssistancePrivateClient:
    def __init__(self, socket_path: Path, *, expected_service_uid: int, shared_gid: int) -> None:
        _validate_socket_path(socket_path)
        if expected_service_uid < 0 or shared_gid < 0 or expected_service_uid == os.geteuid():
            raise ValueError("AI requires a distinct private service identity")
        self.socket_path, self.service_uid, self.shared_gid = socket_path, expected_service_uid, shared_gid

    def request(self, action: AIAction, *, authenticated_actor_id: str) -> AIReadData:
        message = _PrivateRequest(authenticated_actor_id=authenticated_actor_id, action=action)
        body = message.model_dump_json().encode()
        if len(body) > MAX_REQUEST_BYTES:
            raise AIAdmissionRejected("助手请求过长，请减少条件。")
        connection = _UnixHTTPConnection(self.socket_path, timeout_seconds=35, expected_service_uid=self.service_uid, shared_gid=self.shared_gid)
        try:
            connection.request("POST", "/v1/ai-assistance", body, headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            sizes = response.headers.get_all("Content-Length", [])
            if len(sizes) != 1 or not sizes[0].isdigit() or not 1 <= int(sizes[0]) <= MAX_RESPONSE_BYTES:
                raise AIAdmissionUnavailable("助手响应未通过核验。")
            raw = response.read(int(sizes[0]) + 1)
            if len(raw) != int(sizes[0]):
                raise AIAdmissionUnavailable("助手响应未通过核验。")
            if response.status == 409:
                raise AIRequestConflict("原请求或数据已改变，请刷新后新建请求。")
            if response.status == 404:
                raise AIRequestNotFound("找不到原请求。")
            if response.status == 429:
                raise AIBudgetExceeded("今日调用次数已用完，或调用尚未启用。")
            if response.status in (403, 422):
                raise AIAdmissionRejected("助手请求或身份不匹配。")
            if response.status != 200:
                raise AIAdmissionUnavailable("助手暂不可用，请稍后查看原请求。")
            value = strict_json_loads(raw)
            if not isinstance(value, dict) or set(value) != {"data"}:
                raise AIAdmissionUnavailable("助手响应未通过核验。")
            return AIReadData.model_validate(value["data"])
        except (AIRequestConflict, AIRequestNotFound, AIBudgetExceeded, AIAdmissionRejected):
            raise
        except Exception:
            raise AIAdmissionUnavailable("助手暂不可用，请稍后查看原请求。") from None
        finally:
            connection.close()
