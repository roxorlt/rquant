"""AI suggestions are private drafts; original domain services retain authority."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Protocol
from zoneinfo import ZoneInfo

from pydantic import Field, JsonValue

from rquant.ai_assistance_contracts import AIMeasuredUsage
from rquant.runtime_contracts import RuntimeContractModel

if TYPE_CHECKING:
    from rquant.page_control import PageControlOutbox
    from rquant.screen.query_admission import ScreenQueryExecutor
    from rquant.portfolio_backtest_artifact import PortfolioResultReader
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
    from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
    from rquant.stock_news_sources import StockNewsArtifactStore
    from rquant.web.models.ai_assistance import AIGenerateRequest, AIContent, AIRequestView, AICapabilities, AIUsageView
    from rquant.web.models.screen import ScreenCatalogData
    from rquant.ai_screen_backtest_source import AIScreenBacktestPipeline
    from rquant.page_control import PageControlService
    from rquant.portfolio_backtest_commands import SubmitPortfolioBacktest
    from rquant.ai_assistance_contracts import AIInterpretationBinding, SealedInterpretationFacts
    from rquant.ai_interpretation import ValidatedInterpretation
    from rquant.ai_usage import AIUsageRecord

MAX_MODEL_INPUT_BYTES = 64 * 1024
MAX_MODEL_RESPONSE_BYTES = 256 * 1024
MAX_TOOL_ARGUMENT_BYTES = 16 * 1024


class AIInputTooLarge(ValueError):
    """The complete encoded request cannot enter the provider."""


class AIResponseUnknown(RuntimeError):
    """Dispatch may have reached the provider; the original request must not resend."""


class AIModelPrompt(RuntimeContractModel):
    model_id: str = Field(min_length=1, max_length=128)
    system: str = Field(min_length=1)
    instruction: str = Field(min_length=1)
    tool_name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    tool_schema: dict[str, JsonValue]

    def encoded_request(self) -> bytes:
        payload = {
            "model": self.model_id,
            "messages": [{"role": "system", "content": self.system}, {"role": "user", "content": self.instruction}],
            "tools": [{"type": "function", "function": {"name": self.tool_name, "description": "返回待校验的草稿", "parameters": self.tool_schema}}],
            "tool_choice": {"type": "function", "function": {"name": self.tool_name}},
            "temperature": 0.0,
            "max_completion_tokens": 4096,
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        if len(encoded) > MAX_MODEL_INPUT_BYTES:
            raise AIInputTooLarge("model input exceeds 64 KiB")
        return encoded


class AIModelReply(RuntimeContractModel):
    draft: dict[str, JsonValue] | None = None
    usage: AIMeasuredUsage = AIMeasuredUsage()
    error_code: str | None = Field(default=None, pattern=r"^[a-z_]{1,64}$")


class AIModelProvider(Protocol):
    def prepare(self, prompt: AIModelPrompt) -> bytes: ...

    def generate(self, prompt: AIModelPrompt) -> AIModelReply: ...


def measured_provider_usage(raw: object) -> AIMeasuredUsage:
    """Only transport response counters are evidence; a draft's counters are ignored."""
    if not isinstance(raw, dict):
        return AIMeasuredUsage()
    numbers = (raw.get("prompt_tokens"), raw.get("completion_tokens"))
    if any(type(value) is not int or not 0 <= value < 2**63 for value in numbers):
        return AIMeasuredUsage()
    total = raw.get("total_tokens")
    if total is not None and (type(total) is not int or total != sum(numbers)):
        return AIMeasuredUsage()
    return AIMeasuredUsage(input_tokens=numbers[0], output_tokens=numbers[1])


class AIAccountConfig(RuntimeContractModel):
    account_id: str = Field(min_length=1, max_length=128)
    model_id: str = Field(min_length=1, max_length=128)
    template_version: str = "ai-assistance/v1"
    daily_limit: int = Field(default=0, strict=True, ge=0, le=2**63 - 1)


class AIAssistanceContexts:
    """Original installed readers determine facts; a browser supplies no trusted context."""

    def __init__(self, *, screen: ScreenQueryExecutor,
                 portfolio: PortfolioResultReader | None = None,
                 templates: StrategyTemplateSealedResultReader | None = None,
                 private_results: ExperimentPrivateResultAuthority | None = None,
                 news: StockNewsArtifactStore | None = None,
                 portfolio_owner: Callable[[str, object], SubmitPortfolioBacktest] | None = None) -> None:
        from rquant.screen.query_admission import ScreenQueryExecutor
        if type(screen) is not ScreenQueryExecutor:
            raise TypeError("AI contexts require the original complete screen executor")
        self.screen, self.portfolio, self.templates = screen, portfolio, templates
        self.private_results, self.news, self.portfolio_owner = private_results, news, portfolio_owner

    def screen_catalog(self) -> ScreenCatalogData:
        from rquant.web.serving import serving_meta
        self.screen.tracker.refresh()
        with self.screen.tracker.borrow() as borrowed:
            meta = serving_meta(borrowed, now=self.screen.clock(), stale_after=self.screen.stale_after, failure=self.screen.tracker.failure)
            catalog = self.screen.service.catalog(borrowed)
            if not catalog.available or catalog.source is None or (catalog.source_kind == "serving" and meta.state != "ready"):
                raise ValueError("筛选数据暂不可用，请稍后刷新。")
            return catalog

    def _screen_context(self, request: AIGenerateRequest) -> ScreenCatalogData:
        catalog = self.screen_catalog()
        if (catalog.source_kind != request.source_kind or catalog.source.identity != request.source_identity or request.trade_date not in catalog.dates):
            raise ValueError("筛选数据已更新，请刷新后重试。")
        return catalog

    def _pool_context(self, request: AIGenerateRequest):
        from rquant.web.pool_editor_read import read_pool_editor
        from rquant.web.pool_nl_preview import _UNPUBLISHED_POOL_COLUMNS
        from rquant.web.serving import serving_meta
        self.screen.tracker.refresh()
        with self.screen.tracker.borrow() as borrowed:
            meta = serving_meta(borrowed, now=self.screen.clock(), stale_after=self.screen.stale_after, failure=self.screen.tracker.failure)
            snapshot = read_pool_editor(borrowed)
            if meta.state != "ready" or meta.generation_id != request.generation_id or snapshot.data.state != "ready":
                raise ValueError("池子数据已更新，请刷新后重试。")
            base = next((pool for pool in snapshot.data.pools if pool.key == request.pool_key), None)
            if base is None or base.version != request.expected_version:
                raise ValueError("池子已变化，请重新生成建议。")
            if any(column in _UNPUBLISHED_POOL_COLUMNS for column in base.include_columns):
                raise ValueError("这份池子含暂不可用的数据项，请手动编辑。")
            if len(base.model_dump_json().encode()) > 32 * 1024:
                raise ValueError("池子条件过多，请手动编辑。")
            return base

    def _interpretation_context(self, owner: str, request: AIGenerateRequest):
        from rquant.ai_interpretation import facts_from_portfolio, facts_from_template
        from rquant.ai_assistance_contracts import AISealedResultBinding
        if request.source_kind == "portfolio":
            if self.portfolio is None or self.portfolio_owner is None:
                raise ValueError("回测原始结果暂不可用。")
            from rquant.portfolio_backtest_artifact import is_private_portfolio_job
            authority = self.portfolio.reader.get_artifact_preview_authority(request.job_id)
            if authority is None:
                raise ValueError("回测结果尚未封存。")
            if not is_private_portfolio_job(authority.job):
                command=self.portfolio_owner(owner, request.job_id)
            else:
                command=None
            read = self.portfolio.read(request.job_id, expected_result_hash=request.result_sha256, private_owner=owner, private_authority=self.private_results)
            if command is not None and read.bundle.frozen.config != command.config:
                raise PermissionError('sealed portfolio config differs from the original owner command')
            facts = facts_from_portfolio(read, owner_uid=owner)
        else:
            if self.templates is None or self.private_results is None:
                raise ValueError("策略原始结果暂不可用。")
            read = self.templates.read_private(request.job_id, private_owner=owner, private_authority=self.private_results, expected_result_hash=request.result_sha256)
            facts = facts_from_template(read, owner_uid=owner)
        expected = (AISealedResultBinding(owner_uid=owner, **request.model_dump(exclude={"purpose", "request_id"}))
                    if hasattr(request, 'spec_sha256') else facts.binding)
        if (facts.binding != expected or facts.binding.result_sha256 != request.result_sha256
                or facts.binding.source_kind != request.source_kind or facts.binding.job_id != request.job_id):
            raise ValueError("回测结果已变化，请重新选择。")
        return facts

    def _news_context(self, owner: str, request: AIGenerateRequest):
        if self.news is None:
            raise ValueError("摘要原文暂不可用。")
        return self.news.read_facts(owner, request.stock_code, expected_context_sha256=request.context_sha256)

    def prepare(self, owner: str, request: AIGenerateRequest, account: AIAccountConfig):
        from rquant.ai_assistance_contracts import AIInterpretationBinding
        from rquant.ai_interpretation import InterpretationDraft
        from rquant.llm.prompts import build_system_prompt, build_edit_system_prompt
        from rquant.llm.schema_export import to_openai_tools
        from rquant.runtime_contracts import canonical_sha256
        from rquant.stock_news_digest import StockNewsDigestDraft
        from rquant.web.models.screen import ScreenRankingPlan
        if request.purpose == "screen":
            value = self._screen_context(request)
            material = {"source": value.source, "blocks": value.blocks, "ranking_metrics": value.ranking_metrics, "trade_date": request.trade_date}
            schema = to_openai_tools()[0]["function"]["parameters"]
            schema["properties"].pop("include_columns", None)
            if request.include_ranking:
                rank_schema = ScreenRankingPlan.model_json_schema()
                schema["$defs"] = {**schema.get("$defs", {}), **rank_schema.pop("$defs", {})}
                schema["properties"]["ranking"] = rank_schema
                schema["required"] = [*schema.get("required", []), "ranking"]
            system = build_system_prompt() + "\n只使用当前目录的条件。不要生成日期、来源、身份、路径或执行指令。"
            system += "\n排名必须来自当前目录。总权重大于零；个别权重可为零。" if request.include_ranking else "\n只生成条件，不生成排名。"
            system += "\n当前允许目录：" + json.dumps({"blocks": [b.model_dump(mode="json") for b in value.blocks], "ranking_metrics": [m.model_dump(mode="json") for m in value.ranking_metrics]}, ensure_ascii=False, separators=(",", ":"))
            tool = "build_screen"
        elif request.purpose == "pool_edit":
            value = self._pool_context(request)
            material = {"generation": request.generation_id, "pool": value}
            schema = to_openai_tools()[0]["function"]["parameters"]
            schema["properties"].pop("include_columns", None)
            system = build_edit_system_prompt(value.rule_calls) + "\n只修改选股条件，返回完整条件；不改名称、展示列、父池、延迟、排名或执行指令。"
            tool = "build_screen"
        elif request.purpose == "interpretation":
            value = self._interpretation_context(owner, request)
            material = value
            binding = AIInterpretationBinding(result=value.binding, facts_sha256=value.facts_sha256, model_id=account.model_id, template_version=account.template_version)
            schema = InterpretationDraft.model_json_schema()
            system = "按 overview、annual、risk、suggestions 四节解读。所有数值只用 {{fact_id}}，逐段引用 citations。不得写自由数字、日期或无依据结论。过拟合未评估时保持未知。\ncontext_sha256=" + binding.cache_key + "\n封存事实=" + value.model_dump_json()
            tool = "interpret_result"
        else:
            value = self._news_context(owner, request)
            material = value
            schema = StockNewsDigestDraft.model_json_schema()
            inventory = [{"document_id": d.document_id, "title": d.title, "source_kind": d.source_kind, "facts": [f.model_dump(mode="json") for f in d.facts], "published_date": None if d.published_date is None else d.published_date.isoformat(), "body_date": None if d.body_date is None else d.body_date.isoformat(), "first_collected_at": d.first_collected_at.isoformat()} for d in value.documents]
            system = "仅摘要已采集原文事实。事实与预测分开。每句引用 citations，数字只用 {{fact_id}}。不生成任何地址。\ncontext_sha256=" + value.context_sha256 + "\n已封存原文事实=" + json.dumps(inventory, ensure_ascii=False, separators=(",", ":"))
            tool = "summarize_stock"
        prompt = AIModelPrompt(model_id=account.model_id, system=system, instruction=getattr(request, "instruction", "给出简短解读。"), tool_name=tool, tool_schema=schema)
        return prompt, canonical_sha256(material), value

    def validate(self, owner: str, request: AIGenerateRequest, account: AIAccountConfig, context_sha: str, value: object, raw: dict[str, JsonValue]) -> AIContent:
        from rquant.ai_assistance_contracts import AIInterpretationBinding
        from rquant.ai_interpretation import InterpretationDraft, validate_interpretation
        from rquant.llm.schemas import RuleCall
        from rquant.screen.query_contracts import ScreenQueryDefinition
        from rquant.stock_news_digest import StockNewsDigestDraft, validate_stock_news_digest
        from rquant.web.models.ai_assistance import AIScreenDraft, AIPoolDraft, AIInterpretationContent, AINewsContent, AINewsSource
        from rquant.web.models.screen import ScreenRankingPlan
        from rquant.web.screen_nl_preview import validate_screen_draft
        from rquant.web.pool_nl_preview import validate_pool_draft
        # The source remains the same original generation/version after a paid call.
        _, current_hash, _ = self.prepare(owner, request, account)
        if current_hash != context_sha:
            raise ValueError("original AI context changed during generation")
        if request.purpose == "screen":
            remaining = dict(raw)
            rank_raw = remaining.pop("ranking", None)
            conditions = validate_screen_draft(remaining, value, request.trade_date)
            ranking = None
            if request.include_ranking:
                ranking = ScreenRankingPlan.model_validate(rank_raw, strict=True)
                metrics = {item.value for item in value.ranking_metrics}
                if any(item.metric not in metrics for item in ranking.conditions):
                    raise ValueError("ranking metric is outside the current original catalog")
            elif rank_raw is not None:
                raise ValueError("legacy condition draft cannot choose ranking")
            return AIScreenDraft(definition=ScreenQueryDefinition(description=request.instruction, trade_date=request.trade_date, source_kind=request.source_kind, source_identity=request.source_identity, conditions=tuple(RuleCall(name=c.key, args=c.args) for c in conditions), ranking=ranking))
        if request.purpose == "pool_edit":
            calls, changes = validate_pool_draft(raw, value)
            return AIPoolDraft(base_generation_id=request.generation_id, base=value, rule_calls=tuple(calls), changes=tuple(changes))
        if request.purpose == "interpretation":
            binding = AIInterpretationBinding(result=value.binding, facts_sha256=value.facts_sha256, model_id=account.model_id, template_version=account.template_version)
            return AIInterpretationContent(interpretation=validate_interpretation(value, binding=binding, draft=InterpretationDraft.model_validate(raw)))
        validated = validate_stock_news_digest(value, draft=StockNewsDigestDraft.model_validate(raw))
        from rquant.stock_news_sources import news_content
        return news_content(value, validated)


class AIAssistanceOwner:
    def __init__(self, *, outbox: PageControlOutbox, account: AIAccountConfig,
                 provider: AIModelProvider | None, contexts: AIAssistanceContexts,
                 backtests: AIScreenBacktestPipeline | None = None,
                 clock: Callable[[], datetime] | None = None) -> None:
        from rquant.page_control import PageControlOutbox
        if type(outbox) is not PageControlOutbox:
            raise TypeError("AI budget requires the original PageControl SQLite owner")
        self.outbox, self.account, self.provider, self.contexts = outbox, account, provider, contexts
        self.backtests = backtests
        self.control: PageControlService | None = None
        self.nightly: 'AINightlyNewsRunner | None' = None
        self.clock = clock or (lambda: datetime.now(UTC))
        self.interpretations = AIInterpretationCache(outbox)
        self.outbox.ai_usage_recover_dispatches(now=self.clock())

    @staticmethod
    def _view(record) -> AIRequestView:
        from rquant.web.models.ai_assistance import AIRequestView, AI_CONTENT_ADAPTER
        messages = {"unknown": "调用结果未知。继续查看原请求，不会重复调用。", "dispatched": "正在生成，请稍候。", "reserved": "调用尚未发出，可以继续原请求。", "not_dispatched": "调用未发出，可以新建请求。"}
        result = None if record.result is None else AI_CONTENT_ADAPTER.validate_python(record.result)
        return AIRequestView(request_id=record.binding.request_id, purpose=record.binding.purpose, state=record.state, created_at=record.binding.reserved_at, completed_at=record.completed_at, result=result, message=("建议未通过检查，请调整描述后新建请求。" if record.error_code and record.state == "completed" else messages.get(record.state)))

    def lookup(self, owner: str, request: AIGenerateRequest) -> AIRequestView:
        self.require_current_role(owner)
        from rquant.runtime_contracts import canonical_sha256
        record = self.outbox.ai_usage_lookup(owner, request.request_id, canonical_sha256(request))
        return self._original_view(owner, request, record)

    def _original_view(self, owner: str, request: AIGenerateRequest, record: AIUsageRecord) -> AIRequestView:
        if record.state != "not_dispatched" or record.error_code != "cache_reused":
            return self._view(record)
        from rquant.ai_assistance_contracts import AIInterpretationBinding
        from rquant.ai_usage import AIRequestConflict
        from rquant.runtime_contracts import canonical_sha256
        from rquant.web.models.ai_assistance import AIInterpretationContent, AIRequestView
        if request.purpose != "interpretation":
            raise AIRequestConflict("cached original request is not an interpretation")
        facts = self.contexts._interpretation_context(owner, request)
        if canonical_sha256(facts) != record.binding.context_sha256:
            raise AIRequestConflict("cached original complete facts changed")
        binding = AIInterpretationBinding(
            result=facts.binding, facts_sha256=facts.facts_sha256,
            model_id=record.binding.model_id, template_version=record.binding.template_version,
        )
        try:
            cached = self.interpretations.read(facts, binding)
        except LookupError as exc:
            raise AIRequestConflict("cached original interpretation is unavailable") from exc
        self.require_current_role(owner)
        return AIRequestView(
            request_id=record.binding.request_id, purpose="interpretation", state="completed",
            created_at=record.binding.reserved_at, completed_at=record.completed_at,
            result=AIInterpretationContent(interpretation=cached),
            message="已读取现有解读，未新增调用。",
        )

    def _release_unsent(self, owner: str, request: AIGenerateRequest, body_sha: str) -> AIRequestView:
        from rquant.ai_usage import AIRequestConflict
        try:
            with self.write_fence(owner):
                record = self.outbox.ai_usage_release(
                    owner, request.request_id, body_sha, now=self.clock(), reason="not_dispatched",
                )
        except AIRequestConflict:
            # Another continuation may have persisted its dispatch in the meantime.
            record = self.outbox.ai_usage_lookup(owner, request.request_id, body_sha)
        self.require_current_role(owner)
        return self._original_view(owner, request, record)

    def generate(self, owner: str, request: AIGenerateRequest, *, submission_gate: Callable[[], None] | None = None) -> AIRequestView:
        from rquant.ai_usage import AIRequestNotFound, AIBudgetExceeded
        from rquant.ai_assistance_contracts import AIRequestBinding
        from rquant.runtime_contracts import canonical_sha256
        body_sha = canonical_sha256(request)
        self.require_current_role(owner)
        original = None
        try:
            original = self.outbox.ai_usage_lookup(owner, request.request_id, body_sha)
        except AIRequestNotFound:
            if self.outbox.ai_usage_request_exists(request.request_id):
                raise AIRequestNotFound("original request is unavailable") from None
        if original is not None and original.state != "reserved":
            return self._original_view(owner, request, original)
        self.require_current_role(owner, write=True)
        if original is not None and (
            original.binding.account_id, original.binding.model_id, original.binding.template_version
        ) != (self.account.account_id, self.account.model_id, self.account.template_version):
            return self._release_unsent(owner, request, body_sha)
        if submission_gate is not None:
            submission_gate()
        try:
            prompt, context_sha, value = self.contexts.prepare(owner, request, self.account)
        except (ValueError, LookupError, OSError):
            if original is not None:
                return self._release_unsent(owner, request, body_sha)
            raise
        self.require_current_role(owner, write=True)
        if original is not None and original.binding.context_sha256 != context_sha:
            return self._release_unsent(owner, request, body_sha)
        now = self.clock()
        binding = original.binding if original is not None else AIRequestBinding(owner_uid=owner, request_id=request.request_id, request_body_sha256=body_sha, purpose=request.purpose, account_id=self.account.account_id, model_id=self.account.model_id, template_version=self.account.template_version, context_sha256=context_sha, reserved_at=now, budget_date=now.astimezone(ZoneInfo("Asia/Shanghai")).date())
        if request.purpose == "interpretation":
            from contextlib import closing
            from rquant.ai_assistance_contracts import AIInterpretationBinding
            from rquant.ai_usage import AIUsageRepository
            cache_binding = AIInterpretationBinding(
                result=value.binding, facts_sha256=value.facts_sha256,
                model_id=binding.model_id, template_version=binding.template_version,
            )
            try:
                self.interpretations.read(value, cache_binding)
            except LookupError:
                pass
            else:
                with self.write_fence(owner), closing(self.outbox._connect()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    self.interpretations.read(value, cache_binding)
                    cached_record = AIUsageRepository(connection).reuse_interpretation_cache(binding, now=self.clock())
                    connection.commit()
                return self._original_view(owner, request, cached_record)
        provider = self.provider
        if self.account.daily_limit == 0 or provider is None:
            if original is not None:
                return self._release_unsent(owner, request, body_sha)
            raise AIBudgetExceeded("AI 调用尚未启用，可继续手动编辑。")
        try:
            provider.prepare(prompt)
        except Exception:
            if original is not None:
                return self._release_unsent(owner, request, body_sha)
            raise
        with self.write_fence(owner):
            original = self.outbox.ai_usage_reserve(binding, daily_limit=self.account.daily_limit)
        if original.state != "reserved":
            return self._original_view(owner, request, original)
        if submission_gate is not None:
            submission_gate()
        if (binding.account_id, binding.model_id, binding.template_version) != (
            self.account.account_id, self.account.model_id, self.account.template_version
        ) or self.account.daily_limit == 0 or self.provider is not provider:
            return self._release_unsent(owner, request, body_sha)
        try:
            _, current_context_sha, _ = self.contexts.prepare(owner, request, self.account)
        except (ValueError, LookupError, OSError):
            return self._release_unsent(owner, request, body_sha)
        if current_context_sha != binding.context_sha256:
            return self._release_unsent(owner, request, body_sha)
        with self.write_fence(owner):
            dispatch = self.outbox.ai_usage_dispatch(owner, request.request_id, body_sha, now=self.clock())
        if not dispatch.claimed:
            return self._original_view(owner, request, dispatch.record)
        try:
            reply = provider.generate(prompt)
        except Exception:
            unknown = self.outbox.ai_usage_unknown(owner, request.request_id, body_sha, dispatch_token=dispatch.token, now=self.clock())
            self.require_current_role(owner)
            return self._view(unknown)
        content = None
        error = reply.error_code
        try:
            self.require_current_role(owner, write=True)
        except PermissionError:
            error = "permission_changed"
        if reply.draft is not None and error is None:
            try:
                content = self.contexts.validate(owner, request, self.account, context_sha, value, reply.draft).model_dump(mode="json")
                from rquant.ai_usage import MAX_RESULT_BYTES
                from rquant.strict_json import canonical_json_bytes
                if len(canonical_json_bytes(content)) > MAX_RESULT_BYTES - 16 * 1024:
                    raise ValueError("validated content exceeds the owner result budget")
            except Exception:
                content = None
                error = "invalid_domain_draft"
        else:
            error = error or "invalid_model_output"
        # Actual incurred usage must remain journaled even if access changed during
        # the paid call. A withdrawn actor gets no new persisted domain content.
        try:
            with self.write_fence(owner):
                if content is not None:
                    try:
                        if request.purpose == "news_digest":
                            from rquant.web.models.ai_assistance import AINewsContent
                            self.contexts.news.put_digest(value, AINewsContent.model_validate(content).digest,
                                model_id=self.account.model_id, template_version=self.account.template_version)
                        elif request.purpose == "interpretation":
                            from rquant.web.models.ai_assistance import AIInterpretationContent
                            self.interpretations.put(value, AIInterpretationContent.model_validate(content).interpretation)
                    except Exception:
                        content, error = None, "invalid_domain_draft"
                finished = self.outbox.ai_usage_finish(owner, request.request_id, body_sha, dispatch_token=dispatch.token, now=self.clock(), usage=reply.usage, result=content, error_code=error)
        except PermissionError:
            finished = self.outbox.ai_usage_finish(owner, request.request_id, body_sha, dispatch_token=dispatch.token,
                now=self.clock(), usage=reply.usage, result=None, error_code="permission_changed")
        self.require_current_role(owner)
        return self._view(finished)

    def require_current_role(self, owner: str, *, write: bool = False) -> None:
        authority = self.outbox.collaboration
        if authority is None or authority.mode == "legacy":
            return
        authority.require_outbox_path(self.outbox.path)
        if write:
            authority.require_operation(owner, "POST", "/api/v1/ai/requests")
        else:
            authority.current_role(owner)

    @contextmanager
    def write_fence(self, owner: str) -> Iterator[None]:
        authority = self.outbox.collaboration
        if authority is None or authority.mode == "legacy":
            yield
            return
        with authority.locked():
            self.require_current_role(owner, write=True)
            yield

    def capabilities(self, owner: str) -> AICapabilities:
        self.require_current_role(owner)
        from rquant.web.models.ai_assistance import AICapabilities
        day = self.clock().astimezone(ZoneInfo("Asia/Shanghai")).date()
        calls = self.outbox.ai_usage_account_calls(self.account.account_id, day)
        current = self.outbox.collaboration
        role_can_write = current is None or current.mode == "legacy" or current.current_role(owner) in {"admin", "researcher"}
        enabled = self.account.daily_limit > 0 and self.provider is not None and role_can_write
        return AICapabilities(available=True, can_generate=enabled and calls < self.account.daily_limit, daily_limit=self.account.daily_limit, remaining_calls=max(0, self.account.daily_limit - calls), message=None if enabled else "AI 调用尚未启用，可继续手动编辑。", can_prepare_backtest=role_can_write and self.backtests is not None and self.control is not None and self.control.consumer.portfolio_backend is not None)

    def usage(self, owner: str, start_date: date, end_date: date) -> AIUsageView:
        self.require_current_role(owner)
        from rquant.web.models.ai_assistance import AIUsageView
        cap = self.capabilities(owner)
        return AIUsageView(available=True, summary=self.outbox.ai_usage_summary(owner, self.account.account_id, start_date=start_date, end_date=end_date), remaining_calls=cap.remaining_calls, daily_limit=cap.daily_limit)

    def stock_news(self, owner: str, stock_code: str) -> 'AIStockNewsView':
        self.require_current_role(owner)
        from rquant.web.models.ai_assistance import AIStockNewsView
        progress = None if self.nightly is None else self.nightly.journal.latest_view(owner)
        enabled = self.nightly is not None and self.nightly.enabled
        if self.contexts.news is None:
            return AIStockNewsView(stock_code=stock_code,state='missing',message='原文尚未采集。',nightly_enabled=enabled,progress=progress)
        try:
            facts=self.contexts.news.read_facts(owner,stock_code)
        except LookupError:
            return AIStockNewsView(stock_code=stock_code,state='missing',message='原文尚未采集。',nightly_enabled=enabled,progress=progress)
        try:
            content=self.contexts.news.read_digest(owner,stock_code,model_id=self.account.model_id,template_version=self.account.template_version)
        except LookupError:
            content=None
        self.require_current_role(owner)
        return AIStockNewsView(stock_code=stock_code,state='ready' if content is not None else 'collected',context_sha256=facts.context_sha256,content=content,coverage=facts.coverage,message=None if content is not None else '原文已采集，摘要尚未生成。',nightly_enabled=enabled,progress=progress)

    def interpretation(self, owner: str, request: 'AIInterpretationRequest') -> 'AIInterpretationView':
        self.require_current_role(owner)
        from rquant.ai_assistance_contracts import AIInterpretationBinding
        from rquant.web.models.ai_assistance import AIInterpretationView
        facts=self.contexts._interpretation_context(owner,request)
        binding=AIInterpretationBinding(result=facts.binding,facts_sha256=facts.facts_sha256,model_id=self.account.model_id,template_version=self.account.template_version)
        try:
            content=self.interpretations.read(facts,binding)
        except LookupError:
            content=None
        self.require_current_role(owner)
        return AIInterpretationView(binding=facts.binding,content=content,facts=facts.facts,cache_key=binding.cache_key,message=None if content is not None else '结果已封存，可以生成解读。')


class AIInterpretationCache:
    """Immutable validated content indexed in the existing private owner journal."""
    def __init__(self,outbox:PageControlOutbox) -> None:
        from contextlib import closing
        from rquant.page_control import PageControlOutbox
        if type(outbox) is not PageControlOutbox:
            raise TypeError('interpretations require the original PageControl journal')
        self.outbox=outbox
        with closing(outbox._connect()) as connection:
            connection.execute('CREATE TABLE IF NOT EXISTS ai_interpretation_cache (owner_uid TEXT NOT NULL, cache_key TEXT NOT NULL, content_json TEXT NOT NULL, PRIMARY KEY(owner_uid,cache_key))')
            connection.commit()

    def put(self,facts:'SealedInterpretationFacts',content:'ValidatedInterpretation') -> None:
        from contextlib import closing
        from rquant.ai_interpretation import ValidatedInterpretation
        checked=ValidatedInterpretation.model_validate(content.model_dump(mode='python'))
        if checked.binding.result!=facts.binding or checked.binding.facts_sha256!=facts.facts_sha256:
            raise ValueError('interpretation cache differs from complete original facts')
        raw=checked.model_dump_json()
        authority = self.outbox.collaboration
        with (nullcontext() if authority is None or authority.mode == "legacy" else authority.locked()):
            if authority is not None and authority.mode == "enforced":
                authority.require_outbox_path(self.outbox.path)
                authority.require_operation(facts.binding.owner_uid, "POST", "/api/v1/ai/requests")
            with closing(self.outbox._connect()) as connection:
                connection.execute('BEGIN IMMEDIATE')
                old=connection.execute('SELECT content_json FROM ai_interpretation_cache WHERE owner_uid=? AND cache_key=?',(facts.binding.owner_uid,checked.binding.cache_key)).fetchone()
                if old is not None and old[0]!=raw:
                    raise ValueError('immutable interpretation already differs')
                connection.execute('INSERT OR IGNORE INTO ai_interpretation_cache VALUES (?,?,?)',(facts.binding.owner_uid,checked.binding.cache_key,raw))
                connection.commit()

    def read(self,facts:'SealedInterpretationFacts',binding:'AIInterpretationBinding') -> 'ValidatedInterpretation':
        from contextlib import closing
        from rquant.ai_interpretation import ValidatedInterpretation
        if binding.result!=facts.binding or binding.facts_sha256!=facts.facts_sha256:
            raise ValueError('interpretation differs from current sealed original context')
        with closing(self.outbox._connect()) as connection:
            row=connection.execute('SELECT content_json FROM ai_interpretation_cache WHERE owner_uid=? AND cache_key=?',(facts.binding.owner_uid,binding.cache_key)).fetchone()
        if row is None:
            raise LookupError('original interpretation is unavailable')
        value=ValidatedInterpretation.model_validate_json(row[0])
        if value.binding!=binding:
            raise ValueError('immutable interpretation binding changed')
        return value


def build_ai_page_projections(connection:'sqlite3.Connection',observed_at:datetime) -> tuple['ServingProjectionPayload',...]:
    from datetime import timedelta
    import hashlib
    from rquant.ai_usage import AIUsageRecord,AIUsageDay,_UsageTotals
    from rquant.ai_interpretation import ValidatedInterpretation
    from rquant.web.models.ai_assistance import AINewsContent
    from rquant.serving_read_models import ServingProjectionPayload
    names={row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ai_request','ai_interpretation_cache','stock_news_digest','stock_news_head')")}
    if names!={'ai_request','ai_interpretation_cache','stock_news_digest','stock_news_head'}:
        return ()
    rows=connection.execute('SELECT d.owner_uid,d.stock_code,d.model_id,d.template_version,d.context_sha256,d.artifact_sha256,CASE WHEN length(CAST(d.content_json AS BLOB))<=65536 THEN d.content_json END,h.collected_at FROM stock_news_digest d JOIN stock_news_head h ON h.owner_uid=d.owner_uid AND h.stock_code=d.stock_code AND h.context_sha256=d.context_sha256 ORDER BY d.owner_uid,d.stock_code,d.model_id,d.template_version LIMIT 8193').fetchall()
    if len(rows)>8192:raise ValueError('full news projection exceeds original row capacity')
    news=[]
    for row in rows:
        if row[6] is None or hashlib.sha256(row[6].encode()).hexdigest()!=row[5]:
            raise ValueError('complete original news projection cell is unavailable')
        content=AINewsContent.model_validate_json(row[6])
        if (content.digest.owner_uid,content.digest.stock_code,content.digest.context_sha256)!=tuple(row[:2])+(row[4],):
            raise ValueError('news projection original owner/facts differ')
        collected=datetime.fromisoformat(row[7])
        if collected>observed_at:raise ValueError('news projection contains future knowledge')
        news.append(dict(zip(('owner_uid','stock_code','model_id','template_version','context_sha256','content_sha256','payload_json','collected_at'),(*row[:5],content.digest.content_sha256,row[6],row[7]),strict=True)))
    rows=connection.execute('SELECT owner_uid,cache_key,CASE WHEN length(CAST(content_json AS BLOB))<=65536 THEN content_json END FROM ai_interpretation_cache ORDER BY owner_uid,cache_key LIMIT 257').fetchall()
    if len(rows)>256:raise ValueError('full interpretation projection exceeds row capacity')
    interpretations=[]
    for owner,key,raw in rows:
        if raw is None:raise ValueError('complete interpretation cell exceeds original byte capacity')
        content=ValidatedInterpretation.model_validate_json(raw)
        if content.binding.result.owner_uid!=owner or content.binding.cache_key!=key:
            raise ValueError('interpretation projection original identity differs')
        interpretations.append({'owner_uid':owner,'job_id':str(content.binding.result.job_id),'cache_key':key,'source_kind':content.binding.result.source_kind,'result_sha256':content.binding.result.result_sha256,'payload_json':raw})
    day=observed_at.astimezone(ZoneInfo('Asia/Shanghai')).date()
    groups:dict[tuple[str,str,date],_UsageTotals]={}
    count=0
    for owner,account,day_text,raw in connection.execute("SELECT owner_uid,account_id,budget_date,CASE WHEN length(CAST(record_json AS BLOB))<=1064960 THEN record_json END FROM ai_request WHERE budget_date>=? AND budget_date<=? AND state!='not_dispatched' ORDER BY owner_uid,account_id,budget_date LIMIT 10001",((day-timedelta(days=366)).isoformat(),day.isoformat())):
        count+=1
        if raw is None or count>10000:raise ValueError('complete AI usage projection exceeds original capacity')
        record=AIUsageRecord.model_validate_json(raw)
        if (record.binding.owner_uid,record.binding.account_id,record.binding.budget_date.isoformat())!=(owner,account,day_text) or record.binding.reserved_at>observed_at:
            raise ValueError('usage projection original binding or time differs')
        groups.setdefault((owner,account,date.fromisoformat(day_text)),_UsageTotals()).add(record.usage)
    usage=tuple({'owner_uid':owner,'account_id':account,'day':date_value.isoformat(),'payload_json':AIUsageDay(day=date_value,**total.values()).model_dump_json()} for (owner,account,date_value),total in sorted(groups.items()))
    return tuple(ServingProjectionPayload(table_name=name,available_at=observed_at,rows=tuple(values)) for name,values in (('ai_news_digest',news),('ai_interpretation',interpretations),('ai_usage_day',usage)))
